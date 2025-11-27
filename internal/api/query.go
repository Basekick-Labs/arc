package api

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Regex patterns for SHOW commands
var (
	showDatabasesPattern = regexp.MustCompile(`(?i)^\s*SHOW\s+DATABASES\s*;?\s*$`)
	showTablesPattern    = regexp.MustCompile(`(?i)^\s*SHOW\s+(?:TABLES|MEASUREMENTS)(?:\s+FROM\s+([\w-]+))?\s*;?\s*$`)
)

// Pre-compiled regex patterns for SQL-to-storage-path conversion
// These are compiled once at package init rather than on every query
var (
	// Pattern for database.table references (e.g., FROM mydb.mytable)
	patternDBTable = regexp.MustCompile(`(?i)\bFROM\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b`)
	// Pattern for simple table references (FROM table_name)
	patternSimpleTable = regexp.MustCompile(`(?i)\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b`)
)

// QueryHandler handles SQL query endpoints
type QueryHandler struct {
	db      *database.DuckDB
	storage storage.Backend
	pruner  *pruning.PartitionPruner
	logger  zerolog.Logger
}

// QueryRequest represents a SQL query request
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse represents a SQL query response
type QueryResponse struct {
	Success         bool            `json:"success"`
	Columns         []string        `json:"columns"`
	Data            [][]interface{} `json:"data"`
	RowCount        int             `json:"row_count"`
	ExecutionTimeMs float64         `json:"execution_time_ms"`
	Timestamp       string          `json:"timestamp"`
	Error           string          `json:"error,omitempty"`
}

// Dangerous SQL patterns (with word boundaries to avoid false positives)
var dangerousSQLPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\bDROP\s+(?:TABLE|DATABASE|INDEX|VIEW)\b`),
	regexp.MustCompile(`(?i)\bDELETE\s+FROM\b`),
	regexp.MustCompile(`(?i)\bTRUNCATE\s+TABLE\b`),
	regexp.MustCompile(`(?i)\bALTER\s+TABLE\b`),
	regexp.MustCompile(`(?i)\bCREATE\s+(?:TABLE|DATABASE|INDEX)\b`),
	regexp.MustCompile(`(?i)\bINSERT\s+INTO\b`),
	regexp.MustCompile(`(?i)\bUPDATE\s+\w+\s+SET\b`),
}

// Patterns for SQL injection prevention in queryMeasurement endpoint
var (
	// Valid identifier pattern: alphanumeric, underscore, hyphen (common for database/measurement names)
	validIdentifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_-]*$`)

	// Valid ORDER BY pattern: column name with optional ASC/DESC
	validOrderByPattern = regexp.MustCompile(`(?i)^[a-zA-Z_][a-zA-Z0-9_]*(\s+(ASC|DESC))?(,\s*[a-zA-Z_][a-zA-Z0-9_]*(\s+(ASC|DESC))?)*$`)

	// Dangerous patterns for WHERE clause
	dangerousQueryPatterns = []string{
		";",      // Statement terminator
		"--",     // SQL comment
		"/*",     // Multi-line comment start
		"*/",     // Multi-line comment end
		"DROP",   // DDL
		"DELETE", // DML (in WHERE context means injection attempt)
		"INSERT",
		"UPDATE",
		"TRUNCATE",
		"ALTER",
		"CREATE",
		"EXEC",
		"EXECUTE",
		"xp_",
		"sp_",
		"UNION",
	}
)

// validateIdentifier validates database and measurement names to prevent SQL injection
func validateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	if len(name) > 128 {
		return fmt.Errorf("name too long (max 128 characters)")
	}
	if !validIdentifierPattern.MatchString(name) {
		return fmt.Errorf("name contains invalid characters (allowed: alphanumeric, underscore, hyphen)")
	}
	return nil
}

// validateOrderByClause validates ORDER BY clause to prevent SQL injection
func validateOrderByClause(orderBy string) error {
	if orderBy == "" {
		return fmt.Errorf("order_by cannot be empty")
	}
	if len(orderBy) > 256 {
		return fmt.Errorf("order_by too long (max 256 characters)")
	}
	if !validOrderByPattern.MatchString(orderBy) {
		return fmt.Errorf("order_by contains invalid characters or format")
	}
	return nil
}

// validateWhereClauseQuery validates WHERE clause to prevent SQL injection
func validateWhereClauseQuery(where string) error {
	if len(where) > 4096 {
		return fmt.Errorf("where clause too long (max 4096 characters)")
	}

	whereUpper := strings.ToUpper(where)

	// Check for dangerous patterns
	for _, pattern := range dangerousQueryPatterns {
		if strings.Contains(whereUpper, pattern) {
			return fmt.Errorf("where clause contains forbidden pattern: %s", pattern)
		}
	}

	// Check for unmatched quotes
	if strings.Count(where, "'")%2 != 0 {
		return fmt.Errorf("where clause has unmatched single quotes")
	}
	if strings.Count(where, "\"")%2 != 0 {
		return fmt.Errorf("where clause has unmatched double quotes")
	}

	// Check for unmatched parentheses
	if strings.Count(where, "(") != strings.Count(where, ")") {
		return fmt.Errorf("where clause has unmatched parentheses")
	}

	return nil
}

// NewQueryHandler creates a new query handler
func NewQueryHandler(db *database.DuckDB, storage storage.Backend, logger zerolog.Logger) *QueryHandler {
	return &QueryHandler{
		db:      db,
		storage: storage,
		pruner:  pruning.NewPartitionPruner(logger),
		logger:  logger.With().Str("component", "query-handler").Logger(),
	}
}

// RegisterRoutes registers query endpoints
func (h *QueryHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/query", h.executeQuery)
	app.Post("/api/v1/query/estimate", h.estimateQuery)
	app.Get("/api/v1/measurements", h.listMeasurements)
	app.Get("/api/v1/query/:measurement", h.queryMeasurement)
	h.registerArrowRoutes(app)
}

// executeQuery handles POST /api/v1/query - returns JSON response
func (h *QueryHandler) executeQuery(c *fiber.Ctx) error {
	start := time.Now()

	// Parse request body
	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid request body: " + err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Validate SQL
	if strings.TrimSpace(req.SQL) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "SQL query is required",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	if len(req.SQL) > 10000 {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "SQL query exceeds maximum length (10000 characters)",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Check for dangerous SQL patterns
	if err := h.validateSQL(req.SQL); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Handle SHOW DATABASES command
	if showDatabasesPattern.MatchString(req.SQL) {
		return h.handleShowDatabases(c, start)
	}

	// Handle SHOW TABLES/MEASUREMENTS command
	if matches := showTablesPattern.FindStringSubmatch(req.SQL); matches != nil {
		database := "default"
		if len(matches) > 1 && matches[1] != "" {
			database = matches[1]
		}
		return h.handleShowTables(c, start, database)
	}

	// Convert SQL to storage paths
	convertedSQL := h.convertSQLToStoragePaths(req.SQL)

	h.logger.Debug().
		Str("original_sql", req.SQL).
		Str("converted_sql", convertedSQL).
		Msg("Executing query")

	// Execute query
	rows, err := h.db.Query(convertedSQL)
	if err != nil {
		h.logger.Error().Err(err).Str("sql", req.SQL).Msg("Query execution failed")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           "Query execution failed", // Don't expose database error details
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get column names")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           "Query execution failed", // Don't expose database error details
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Fetch results
	data := make([][]interface{}, 0)
	rowCount := 0

	for rows.Next() {
		// Create slice of interface{} pointers for scanning
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			h.logger.Error().Err(err).Msg("Failed to scan row")
			continue
		}

		// Convert values to JSON-serializable types
		row := make([]interface{}, len(values))
		for i, v := range values {
			row[i] = h.convertValue(v)
		}

		data = append(data, row)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		h.logger.Error().Err(err).Msg("Error iterating rows")
	}

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Int("row_count", rowCount).
		Float64("execution_time_ms", executionTime).
		Msg("Query completed")

	return c.JSON(QueryResponse{
		Success:         true,
		Columns:         columns,
		Data:            data,
		RowCount:        rowCount,
		ExecutionTimeMs: executionTime,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	})
}

// validateSQL checks for dangerous SQL patterns
func (h *QueryHandler) validateSQL(sql string) error {
	for _, pattern := range dangerousSQLPatterns {
		if pattern.MatchString(sql) {
			return fiber.NewError(fiber.StatusBadRequest, "Dangerous SQL operation not allowed")
		}
	}
	return nil
}

// convertSQLToStoragePaths converts table references to storage paths
// Converts: FROM database.measurement -> FROM read_parquet('path/**/*.parquet')
// Converts: FROM measurement -> FROM read_parquet('path/**/*.parquet')
func (h *QueryHandler) convertSQLToStoragePaths(sql string) string {
	originalSQL := sql

	sql = patternDBTable.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternDBTable.FindStringSubmatch(match)
		if len(parts) < 3 {
			return match
		}
		database := parts[1]
		table := parts[2]

		// Generate storage path based on backend type
		path := h.getStoragePath(database, table)

		// Apply partition pruning
		optimizedPath, wasOptimized := h.pruner.OptimizeTablePath(path, originalSQL)

		if wasOptimized {
			// Check if it's a list of paths or a single path
			if pathList, ok := optimizedPath.([]string); ok {
				// Multiple paths - use DuckDB array syntax
				pathsStr := "["
				for i, p := range pathList {
					if i > 0 {
						pathsStr += ", "
					}
					pathsStr += "'" + p + "'"
				}
				pathsStr += "]"
				h.logger.Info().Int("partition_count", len(pathList)).Msg("Partition pruning: Using targeted paths")
				return "FROM read_parquet(" + pathsStr + ", union_by_name=true)"
			} else if pathStr, ok := optimizedPath.(string); ok {
				h.logger.Info().Str("optimized_path", pathStr).Msg("Partition pruning: Using optimized path")
				return "FROM read_parquet('" + pathStr + "', union_by_name=true)"
			}
		}

		return "FROM read_parquet('" + path + "', union_by_name=true)"
	})

	sql = patternSimpleTable.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternSimpleTable.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		table := strings.ToLower(parts[1])

		// Skip already converted read_parquet, system tables, etc.
		skipPrefixes := []string{"read_parquet", "information_schema", "pg_", "duckdb_"}
		for _, prefix := range skipPrefixes {
			if strings.HasPrefix(table, prefix) {
				return match
			}
		}

		// Check if followed by a dot (database.table already handled) or parenthesis (function call)
		// We look at the original match position in the modified sql
		idx := strings.Index(strings.ToLower(sql), strings.ToLower(match))
		if idx >= 0 {
			afterMatch := sql[idx+len(match):]
			afterMatch = strings.TrimLeft(afterMatch, " \t")
			if len(afterMatch) > 0 && (afterMatch[0] == '.' || afterMatch[0] == '(') {
				return match
			}
		}

		// Use default database
		path := h.getStoragePath("default", parts[1])

		// Apply partition pruning
		optimizedPath, wasOptimized := h.pruner.OptimizeTablePath(path, originalSQL)

		if wasOptimized {
			// Check if it's a list of paths or a single path
			if pathList, ok := optimizedPath.([]string); ok {
				// Multiple paths - use DuckDB array syntax
				pathsStr := "["
				for i, p := range pathList {
					if i > 0 {
						pathsStr += ", "
					}
					pathsStr += "'" + p + "'"
				}
				pathsStr += "]"
				h.logger.Info().Int("partition_count", len(pathList)).Msg("Partition pruning: Using targeted paths")
				return "FROM read_parquet(" + pathsStr + ", union_by_name=true)"
			} else if pathStr, ok := optimizedPath.(string); ok {
				h.logger.Info().Str("optimized_path", pathStr).Msg("Partition pruning: Using optimized path")
				return "FROM read_parquet('" + pathStr + "', union_by_name=true)"
			}
		}

		return "FROM read_parquet('" + path + "', union_by_name=true)"
	})

	return sql
}

// getStoragePath returns the storage path for a database.table
func (h *QueryHandler) getStoragePath(database, table string) string {
	// Check backend type and generate appropriate path
	switch backend := h.storage.(type) {
	case *storage.S3Backend:
		return "s3://" + backend.GetBucket() + "/" + database + "/" + table + "/**/*.parquet"
	case *storage.LocalBackend:
		return backend.GetBasePath() + "/" + database + "/" + table + "/**/*.parquet"
	default:
		// Fallback to local path
		return "./data/" + database + "/" + table + "/**/*.parquet"
	}
}

// convertValue converts database values to JSON-serializable types
func (h *QueryHandler) convertValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339Nano)
	case []byte:
		return string(val)
	case sql.NullString:
		if val.Valid {
			return val.String
		}
		return nil
	case sql.NullInt64:
		if val.Valid {
			return val.Int64
		}
		return nil
	case sql.NullFloat64:
		if val.Valid {
			return val.Float64
		}
		return nil
	case sql.NullBool:
		if val.Valid {
			return val.Bool
		}
		return nil
	default:
		return val
	}
}

// handleShowDatabases handles SHOW DATABASES command by scanning storage
func (h *QueryHandler) handleShowDatabases(c *fiber.Ctx, start time.Time) error {
	h.logger.Debug().Msg("Handling SHOW DATABASES")

	columns := []string{"database"}
	data := make([][]interface{}, 0)

	// Get base path from storage backend
	basePath := h.getStorageBasePath()
	if basePath == "" {
		return c.JSON(QueryResponse{
			Success:         true,
			Columns:         columns,
			Data:            data,
			RowCount:        0,
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Scan for database directories
	entries, err := os.ReadDir(basePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No data directory yet, return empty result
			return c.JSON(QueryResponse{
				Success:         true,
				Columns:         columns,
				Data:            data,
				RowCount:        0,
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       time.Now().UTC().Format(time.RFC3339),
			})
		}
		h.logger.Error().Err(err).Str("path", basePath).Msg("Failed to read storage directory")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           "Failed to read storage: " + err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}

	databases := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") && !strings.HasPrefix(entry.Name(), "_") {
			databases = append(databases, entry.Name())
		}
	}

	// Sort alphabetically
	sort.Strings(databases)

	for _, db := range databases {
		data = append(data, []interface{}{db})
	}

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Int("database_count", len(databases)).
		Float64("execution_time_ms", executionTime).
		Msg("SHOW DATABASES completed")

	return c.JSON(QueryResponse{
		Success:         true,
		Columns:         columns,
		Data:            data,
		RowCount:        len(data),
		ExecutionTimeMs: executionTime,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	})
}

// handleShowTables handles SHOW TABLES/MEASUREMENTS command by scanning storage
func (h *QueryHandler) handleShowTables(c *fiber.Ctx, start time.Time, database string) error {
	h.logger.Debug().Str("database", database).Msg("Handling SHOW TABLES")

	columns := []string{"database", "table_name", "storage_path", "file_count", "total_size_mb"}
	data := make([][]interface{}, 0)

	// Get base path from storage backend
	basePath := h.getStorageBasePath()
	if basePath == "" {
		return c.JSON(QueryResponse{
			Success:         true,
			Columns:         columns,
			Data:            data,
			RowCount:        0,
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}

	dbPath := filepath.Join(basePath, database)

	// Scan for table/measurement directories
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Database doesn't exist yet, return empty result
			return c.JSON(QueryResponse{
				Success:         true,
				Columns:         columns,
				Data:            data,
				RowCount:        0,
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       time.Now().UTC().Format(time.RFC3339),
			})
		}
		h.logger.Error().Err(err).Str("path", dbPath).Msg("Failed to read database directory")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           "Failed to read database: " + err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}

	tables := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") && !strings.HasPrefix(entry.Name(), "_") {
			tables = append(tables, entry.Name())
		}
	}

	// Sort alphabetically
	sort.Strings(tables)

	for _, table := range tables {
		tablePath := filepath.Join(dbPath, table)
		fileCount, totalSize := h.getTableStats(tablePath)

		// Format storage path for display
		storagePath := h.getStoragePath(database, table)

		data = append(data, []interface{}{
			database,
			table,
			storagePath,
			fileCount,
			float64(totalSize) / (1024 * 1024), // Convert to MB
		})
	}

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Str("database", database).
		Int("table_count", len(tables)).
		Float64("execution_time_ms", executionTime).
		Msg("SHOW TABLES completed")

	return c.JSON(QueryResponse{
		Success:         true,
		Columns:         columns,
		Data:            data,
		RowCount:        len(data),
		ExecutionTimeMs: executionTime,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	})
}

// getStorageBasePath returns the base path for storage
func (h *QueryHandler) getStorageBasePath() string {
	switch backend := h.storage.(type) {
	case *storage.LocalBackend:
		return backend.GetBasePath()
	case *storage.S3Backend:
		// For S3, we can't easily scan directories
		// Return empty to indicate we need different handling
		// TODO: Implement S3 listing via the backend
		h.logger.Warn().Msg("SHOW commands not fully supported for S3 backend yet")
		return ""
	default:
		return "./data/arc"
	}
}

// getTableStats returns file count and total size for a table directory
func (h *QueryHandler) getTableStats(tablePath string) (int, int64) {
	var fileCount int
	var totalSize int64

	_ = filepath.WalkDir(tablePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // Continue on error
		}
		if !d.IsDir() && strings.HasSuffix(strings.ToLower(d.Name()), ".parquet") {
			fileCount++
			if info, err := d.Info(); err == nil {
				totalSize += info.Size()
			}
		}
		return nil
	})

	return fileCount, totalSize
}

// EstimateResponse represents the response for query estimation
type EstimateResponse struct {
	Success         bool    `json:"success"`
	EstimatedRows   *int64  `json:"estimated_rows"`
	WarningLevel    string  `json:"warning_level"`
	WarningMessage  string  `json:"warning_message,omitempty"`
	ExecutionTimeMs float64 `json:"execution_time_ms"`
	Error           string  `json:"error,omitempty"`
}

// estimateQuery handles POST /api/v1/query/estimate - returns row count estimate
func (h *QueryHandler) estimateQuery(c *fiber.Ctx) error {
	start := time.Now()

	// Parse request body
	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
			Success:      false,
			Error:        "Invalid request body: " + err.Error(),
			WarningLevel: "error",
		})
	}

	// Validate SQL
	if strings.TrimSpace(req.SQL) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
			Success:      false,
			Error:        "SQL query is required",
			WarningLevel: "error",
		})
	}

	// Check for dangerous SQL patterns
	if err := h.validateSQL(req.SQL); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
			Success:      false,
			Error:        err.Error(),
			WarningLevel: "error",
		})
	}

	// Convert SQL to storage paths
	convertedSQL := h.convertSQLToStoragePaths(req.SQL)

	// Create a COUNT(*) version of the query
	countSQL := "SELECT COUNT(*) FROM (" + convertedSQL + ") AS t"

	h.logger.Debug().
		Str("original_sql", req.SQL).
		Str("count_sql", countSQL).
		Msg("Estimating query")

	// Execute count query
	rows, err := h.db.Query(countSQL)
	if err != nil {
		h.logger.Error().Err(err).Str("sql", countSQL).Msg("Estimate query failed")
		return c.JSON(EstimateResponse{
			Success:         false,
			Error:           "Cannot estimate query: " + err.Error(),
			WarningLevel:    "error",
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
		})
	}
	defer rows.Close()

	var estimatedRows int64
	if rows.Next() {
		if err := rows.Scan(&estimatedRows); err != nil {
			h.logger.Error().Err(err).Msg("Failed to scan count result")
			return c.JSON(EstimateResponse{
				Success:         false,
				Error:           "Failed to get row count: " + err.Error(),
				WarningLevel:    "error",
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			})
		}
	}

	// Determine warning level and message
	var warningLevel, warningMessage string

	switch {
	case estimatedRows > 1000000:
		warningLevel = "high"
		warningMessage = formatRowMessage(estimatedRows, "⚠️ Large query", "This may take several minutes and use significant memory.")
	case estimatedRows > 100000:
		warningLevel = "medium"
		warningMessage = formatRowMessage(estimatedRows, "⚠️ Medium query", "This may take 30-60 seconds.")
	case estimatedRows > 10000:
		warningLevel = "low"
		warningMessage = formatRowMessage(estimatedRows, "📊", "Should complete quickly.")
	default:
		warningLevel = "none"
		warningMessage = formatRowMessage(estimatedRows, "✅ Small query", "")
	}

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Int64("estimated_rows", estimatedRows).
		Str("warning_level", warningLevel).
		Float64("execution_time_ms", executionTime).
		Msg("Query estimation completed")

	return c.JSON(EstimateResponse{
		Success:         true,
		EstimatedRows:   &estimatedRows,
		WarningLevel:    warningLevel,
		WarningMessage:  warningMessage,
		ExecutionTimeMs: executionTime,
	})
}

// formatRowMessage formats a message with row count
func formatRowMessage(rows int64, prefix, suffix string) string {
	// Format number with commas
	formatted := formatNumber(rows)
	if suffix != "" {
		return prefix + ": " + formatted + " rows. " + suffix
	}
	return prefix + ": " + formatted + " rows."
}

// formatNumber formats a number with comma separators
func formatNumber(n int64) string {
	if n < 0 {
		return "-" + formatNumber(-n)
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	s := fmt.Sprintf("%d", n)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

// MeasurementInfo represents information about a measurement
type MeasurementInfo struct {
	Database    string  `json:"database"`
	Measurement string  `json:"measurement"`
	FileCount   int     `json:"file_count"`
	TotalSizeMB float64 `json:"total_size_mb"`
	StoragePath string  `json:"storage_path"`
}

// listMeasurements handles GET /api/v1/measurements - lists all measurements across all databases
func (h *QueryHandler) listMeasurements(c *fiber.Ctx) error {
	start := time.Now()

	// Optional database filter
	dbFilter := c.Query("database", "")

	basePath := h.getStorageBasePath()
	if basePath == "" {
		return c.JSON(fiber.Map{
			"success":           true,
			"measurements":      []MeasurementInfo{},
			"count":             0,
			"execution_time_ms": float64(time.Since(start).Milliseconds()),
		})
	}

	measurements := make([]MeasurementInfo, 0)

	// Scan for database directories
	dbEntries, err := os.ReadDir(basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return c.JSON(fiber.Map{
				"success":           true,
				"measurements":      measurements,
				"count":             0,
				"execution_time_ms": float64(time.Since(start).Milliseconds()),
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to read storage: " + err.Error(),
		})
	}

	for _, dbEntry := range dbEntries {
		if !dbEntry.IsDir() || strings.HasPrefix(dbEntry.Name(), ".") || strings.HasPrefix(dbEntry.Name(), "_") {
			continue
		}

		dbName := dbEntry.Name()

		// Apply database filter if specified
		if dbFilter != "" && dbName != dbFilter {
			continue
		}

		dbPath := filepath.Join(basePath, dbName)

		// Scan for measurement directories
		measurementEntries, err := os.ReadDir(dbPath)
		if err != nil {
			continue
		}

		for _, measurementEntry := range measurementEntries {
			if !measurementEntry.IsDir() || strings.HasPrefix(measurementEntry.Name(), ".") || strings.HasPrefix(measurementEntry.Name(), "_") {
				continue
			}

			measurementName := measurementEntry.Name()
			measurementPath := filepath.Join(dbPath, measurementName)
			fileCount, totalSize := h.getTableStats(measurementPath)

			measurements = append(measurements, MeasurementInfo{
				Database:    dbName,
				Measurement: measurementName,
				FileCount:   fileCount,
				TotalSizeMB: float64(totalSize) / (1024 * 1024),
				StoragePath: h.getStoragePath(dbName, measurementName),
			})
		}
	}

	// Sort by database, then measurement
	sort.Slice(measurements, func(i, j int) bool {
		if measurements[i].Database != measurements[j].Database {
			return measurements[i].Database < measurements[j].Database
		}
		return measurements[i].Measurement < measurements[j].Measurement
	})

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Int("measurement_count", len(measurements)).
		Float64("execution_time_ms", executionTime).
		Msg("List measurements completed")

	return c.JSON(fiber.Map{
		"success":           true,
		"measurements":      measurements,
		"count":             len(measurements),
		"execution_time_ms": executionTime,
	})
}

// queryMeasurement handles GET /api/v1/query/:measurement - query a specific measurement
func (h *QueryHandler) queryMeasurement(c *fiber.Ctx) error {
	start := time.Now()

	measurement := c.Params("measurement")
	if measurement == "" {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Measurement name is required",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Get query parameters
	database := c.Query("database", "default")
	limitStr := c.Query("limit", "100")
	offsetStr := c.Query("offset", "0")
	orderBy := c.Query("order_by", "time DESC")
	where := c.Query("where", "")

	// Validate database and measurement names (prevent SQL injection via identifiers)
	if err := validateIdentifier(database); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid database name: " + err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}
	if err := validateIdentifier(measurement); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid measurement name: " + err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Validate limit and offset as integers
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 0 || limit > 1000000 {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid limit: must be a positive integer up to 1000000",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}
	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid offset: must be a non-negative integer",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Validate ORDER BY clause
	if err := validateOrderByClause(orderBy); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid order_by: " + err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Validate WHERE clause if provided
	if where != "" {
		if err := validateWhereClauseQuery(where); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
				Success:   false,
				Error:     "Invalid where clause: " + err.Error(),
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			})
		}
	}

	// Build SQL query with validated parameters
	sql := fmt.Sprintf("SELECT * FROM %s.%s", database, measurement)
	if where != "" {
		sql += " WHERE " + where
	}
	sql += " ORDER BY " + orderBy
	sql += fmt.Sprintf(" LIMIT %d", limit)
	sql += fmt.Sprintf(" OFFSET %d", offset)

	// Convert SQL to storage paths
	convertedSQL := h.convertSQLToStoragePaths(sql)

	h.logger.Debug().
		Str("measurement", measurement).
		Str("database", database).
		Str("sql", convertedSQL).
		Msg("Querying measurement")

	// Execute query
	rows, err := h.db.Query(convertedSQL)
	if err != nil {
		h.logger.Error().Err(err).Str("sql", sql).Msg("Measurement query failed")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           "Query execution failed", // Don't expose database error details
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get column names in measurement query")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           "Query execution failed", // Don't expose database error details
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Fetch results
	data := make([][]interface{}, 0)
	rowCount := 0

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			h.logger.Error().Err(err).Msg("Failed to scan row")
			continue
		}

		row := make([]interface{}, len(values))
		for i, v := range values {
			row[i] = h.convertValue(v)
		}

		data = append(data, row)
		rowCount++
	}

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Str("measurement", measurement).
		Int("row_count", rowCount).
		Float64("execution_time_ms", executionTime).
		Msg("Measurement query completed")

	return c.JSON(QueryResponse{
		Success:         true,
		Columns:         columns,
		Data:            data,
		RowCount:        rowCount,
		ExecutionTimeMs: executionTime,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	})
}

