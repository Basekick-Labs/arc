package api

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// ContinuousQueryHandler handles continuous query operations
type ContinuousQueryHandler struct {
	db          *database.DuckDB
	storage     storage.Backend
	arrowBuffer *ingest.ArrowBuffer
	config      *config.ContinuousQueryConfig
	sqliteDB    *sql.DB
	logger      zerolog.Logger
}

// ContinuousQuery represents a continuous query definition
type ContinuousQuery struct {
	ID                     int64   `json:"id"`
	Name                   string  `json:"name"`
	Description            *string `json:"description"`
	Database               string  `json:"database"`
	SourceMeasurement      string  `json:"source_measurement"`
	DestinationMeasurement string  `json:"destination_measurement"`
	Query                  string  `json:"query"`
	Interval               string  `json:"interval"`
	RetentionDays          *int    `json:"retention_days"`
	DeleteSourceAfterDays  *int    `json:"delete_source_after_days"`
	IsActive               bool    `json:"is_active"`
	LastExecutionTime      *string `json:"last_execution_time"`
	LastExecutionStatus    *string `json:"last_execution_status"`
	LastProcessedTime      *string `json:"last_processed_time"`
	LastRecordsWritten     *int64  `json:"last_records_written"`
	CreatedAt              string  `json:"created_at"`
	UpdatedAt              string  `json:"updated_at"`
}

// ContinuousQueryRequest represents a request to create/update a CQ
type ContinuousQueryRequest struct {
	Name                   string  `json:"name"`
	Description            *string `json:"description"`
	Database               string  `json:"database"`
	SourceMeasurement      string  `json:"source_measurement"`
	DestinationMeasurement string  `json:"destination_measurement"`
	Query                  string  `json:"query"`
	Interval               string  `json:"interval"`
	RetentionDays          *int    `json:"retention_days"`
	DeleteSourceAfterDays  *int    `json:"delete_source_after_days"`
	IsActive               bool    `json:"is_active"`
}

// ExecuteCQRequest represents a request to execute a CQ
type ExecuteCQRequest struct {
	StartTime *string `json:"start_time"`
	EndTime   *string `json:"end_time"`
	DryRun    bool    `json:"dry_run"`
}

// ExecuteCQResponse represents the result of executing a CQ
type ExecuteCQResponse struct {
	QueryID                int64   `json:"query_id"`
	QueryName              string  `json:"query_name"`
	ExecutionID            string  `json:"execution_id"`
	Status                 string  `json:"status"`
	StartTime              string  `json:"start_time"`
	EndTime                string  `json:"end_time"`
	RecordsRead            *int64  `json:"records_read"`
	RecordsWritten         int64   `json:"records_written"`
	ExecutionTimeSeconds   float64 `json:"execution_time_seconds"`
	DestinationMeasurement string  `json:"destination_measurement"`
	DryRun                 bool    `json:"dry_run"`
	ExecutedAt             string  `json:"executed_at"`
	ExecutedQuery          string  `json:"executed_query,omitempty"`
}

// CQExecution represents an execution history record
type CQExecution struct {
	ID                       int64   `json:"id"`
	QueryID                  int64   `json:"query_id"`
	ExecutionID              string  `json:"execution_id"`
	ExecutionTime            string  `json:"execution_time"`
	Status                   string  `json:"status"`
	StartTime                string  `json:"start_time"`
	EndTime                  string  `json:"end_time"`
	RecordsRead              *int64  `json:"records_read"`
	RecordsWritten           int64   `json:"records_written"`
	ExecutionDurationSeconds float64 `json:"execution_duration_seconds"`
	ErrorMessage             *string `json:"error_message"`
}

// NewContinuousQueryHandler creates a new continuous query handler
func NewContinuousQueryHandler(db *database.DuckDB, storage storage.Backend, arrowBuffer *ingest.ArrowBuffer, cfg *config.ContinuousQueryConfig, logger zerolog.Logger) (*ContinuousQueryHandler, error) {
	// Ensure directory exists
	dir := filepath.Dir(cfg.DBPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for CQ DB: %w", err)
	}

	// Open SQLite database
	sqliteDB, err := sql.Open("sqlite3", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CQ database: %w", err)
	}

	h := &ContinuousQueryHandler{
		db:          db,
		storage:     storage,
		arrowBuffer: arrowBuffer,
		config:      cfg,
		sqliteDB:    sqliteDB,
		logger:      logger.With().Str("component", "cq-handler").Logger(),
	}

	// Initialize tables
	if err := h.initTables(); err != nil {
		sqliteDB.Close()
		return nil, fmt.Errorf("failed to initialize CQ tables: %w", err)
	}

	return h, nil
}

// initTables creates the continuous query tables
func (h *ContinuousQueryHandler) initTables() error {
	// Continuous queries table
	_, err := h.sqliteDB.Exec(`
		CREATE TABLE IF NOT EXISTS continuous_queries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			description TEXT,
			database TEXT NOT NULL,
			source_measurement TEXT NOT NULL,
			destination_measurement TEXT NOT NULL,
			query TEXT NOT NULL,
			interval TEXT NOT NULL,
			retention_days INTEGER,
			delete_source_after_days INTEGER,
			is_active BOOLEAN DEFAULT TRUE,
			last_processed_time TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create continuous_queries table: %w", err)
	}

	// Execution history table
	_, err = h.sqliteDB.Exec(`
		CREATE TABLE IF NOT EXISTS continuous_query_executions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			query_id INTEGER NOT NULL,
			execution_id TEXT NOT NULL,
			execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			status TEXT NOT NULL,
			start_time TIMESTAMP NOT NULL,
			end_time TIMESTAMP NOT NULL,
			records_read INTEGER,
			records_written INTEGER DEFAULT 0,
			execution_duration_seconds FLOAT,
			error_message TEXT,
			FOREIGN KEY (query_id) REFERENCES continuous_queries (id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create continuous_query_executions table: %w", err)
	}

	h.logger.Info().Msg("Continuous query tables initialized")
	return nil
}

// Close closes the database connection
func (h *ContinuousQueryHandler) Close() error {
	return h.sqliteDB.Close()
}

// RegisterRoutes registers continuous query endpoints
func (h *ContinuousQueryHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/continuous_queries", h.handleCreate)
	app.Get("/api/v1/continuous_queries", h.handleList)
	app.Get("/api/v1/continuous_queries/:id", h.handleGet)
	app.Put("/api/v1/continuous_queries/:id", h.handleUpdate)
	app.Delete("/api/v1/continuous_queries/:id", h.handleDelete)
	app.Post("/api/v1/continuous_queries/:id/execute", h.handleExecute)
	app.Get("/api/v1/continuous_queries/:id/executions", h.handleGetExecutions)
}

// handleCreate creates a new continuous query
func (h *ContinuousQueryHandler) handleCreate(c *fiber.Ctx) error {
	if !h.config.Enabled {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "Continuous queries are disabled",
		})
	}

	var req ContinuousQueryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	// Validate
	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "name is required"})
	}
	if req.Database == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "database is required"})
	}
	if req.SourceMeasurement == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "source_measurement is required"})
	}
	if req.DestinationMeasurement == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "destination_measurement is required"})
	}
	if req.Query == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "query is required"})
	}
	if req.Interval == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "interval is required"})
	}

	// Validate query has required placeholders
	if !strings.Contains(req.Query, "{start_time}") || !strings.Contains(req.Query, "{end_time}") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "query must contain {start_time} and {end_time} placeholders",
		})
	}

	// Insert query
	result, err := h.sqliteDB.Exec(`
		INSERT INTO continuous_queries
		(name, description, database, source_measurement, destination_measurement, query, interval, retention_days, delete_source_after_days, is_active)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, req.Name, req.Description, req.Database, req.SourceMeasurement, req.DestinationMeasurement, req.Query, req.Interval, req.RetentionDays, req.DeleteSourceAfterDays, req.IsActive)

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Continuous query with name '%s' already exists", req.Name),
			})
		}
		h.logger.Error().Err(err).Msg("Failed to create continuous query")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create continuous query",
		})
	}

	queryID, _ := result.LastInsertId()
	h.logger.Info().Int64("query_id", queryID).Str("name", req.Name).Msg("Created continuous query")

	// Return created query
	cq, err := h.getQuery(queryID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to retrieve created query",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(cq)
}

// handleList returns all continuous queries
func (h *ContinuousQueryHandler) handleList(c *fiber.Ctx) error {
	database := c.Query("database")
	isActiveStr := c.Query("is_active")

	queries, err := h.getQueries(database, isActiveStr)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list continuous queries")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list continuous queries",
		})
	}

	return c.JSON(queries)
}

// handleGet returns a single continuous query
func (h *ContinuousQueryHandler) handleGet(c *fiber.Ctx) error {
	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	cq, err := h.getQuery(int64(queryID))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}

	return c.JSON(cq)
}

// handleUpdate updates an existing continuous query
func (h *ContinuousQueryHandler) handleUpdate(c *fiber.Ctx) error {
	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	var req ContinuousQueryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	result, err := h.sqliteDB.Exec(`
		UPDATE continuous_queries SET
			name = ?, description = ?, database = ?, source_measurement = ?,
			destination_measurement = ?, query = ?, interval = ?, retention_days = ?,
			delete_source_after_days = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, req.Name, req.Description, req.Database, req.SourceMeasurement, req.DestinationMeasurement, req.Query, req.Interval, req.RetentionDays, req.DeleteSourceAfterDays, req.IsActive, queryID)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to update continuous query")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to update continuous query",
		})
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}

	h.logger.Info().Int("query_id", queryID).Msg("Updated continuous query")

	cq, _ := h.getQuery(int64(queryID))
	return c.JSON(cq)
}

// handleDelete deletes a continuous query
func (h *ContinuousQueryHandler) handleDelete(c *fiber.Ctx) error {
	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	// Delete execution history first
	_, _ = h.sqliteDB.Exec("DELETE FROM continuous_query_executions WHERE query_id = ?", queryID)

	// Delete query
	result, err := h.sqliteDB.Exec("DELETE FROM continuous_queries WHERE id = ?", queryID)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to delete continuous query")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to delete continuous query",
		})
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}

	h.logger.Info().Int("query_id", queryID).Msg("Deleted continuous query")

	return c.JSON(fiber.Map{"message": "Continuous query deleted successfully"})
}

// handleExecute executes a continuous query
func (h *ContinuousQueryHandler) handleExecute(c *fiber.Ctx) error {
	start := time.Now()

	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	var req ExecuteCQRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	// Get query definition
	cq, err := h.getQuery(int64(queryID))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}

	if !cq.IsActive {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Continuous query is not active"})
	}

	// Determine time range
	var startTime, endTime time.Time

	if req.StartTime != nil && *req.StartTime != "" {
		startTime, err = time.Parse(time.RFC3339, *req.StartTime)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid start_time format"})
		}
		startTime = startTime.UTC()
	} else if cq.LastProcessedTime != nil {
		startTime, _ = time.Parse(time.RFC3339, *cq.LastProcessedTime)
		startTime = startTime.UTC()
	} else {
		startTime = time.Now().UTC().Add(-1 * time.Hour)
	}

	if req.EndTime != nil && *req.EndTime != "" {
		endTime, err = time.Parse(time.RFC3339, *req.EndTime)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid end_time format"})
		}
		endTime = endTime.UTC()
	} else {
		endTime = time.Now().UTC()
	}

	if !startTime.Before(endTime) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "start_time must be before end_time"})
	}

	// Replace placeholders in query
	executedQuery := strings.ReplaceAll(cq.Query, "{start_time}", fmt.Sprintf("'%s'", startTime.Format(time.RFC3339)))
	executedQuery = strings.ReplaceAll(executedQuery, "{end_time}", fmt.Sprintf("'%s'", endTime.Format(time.RFC3339)))

	executionID := fmt.Sprintf("cq-exec-%s", uuid.New().String()[:8])

	h.logger.Info().
		Str("query_name", cq.Name).
		Str("execution_id", executionID).
		Time("start_time", startTime).
		Time("end_time", endTime).
		Bool("dry_run", req.DryRun).
		Msg("Executing continuous query")

	// Dry run - just return the query
	if req.DryRun {
		executionDuration := time.Since(start).Seconds()

		return c.JSON(ExecuteCQResponse{
			QueryID:                int64(queryID),
			QueryName:              cq.Name,
			ExecutionID:            executionID,
			Status:                 "dry_run",
			StartTime:              startTime.Format(time.RFC3339),
			EndTime:                endTime.Format(time.RFC3339),
			RecordsRead:            nil,
			RecordsWritten:         0,
			ExecutionTimeSeconds:   executionDuration,
			DestinationMeasurement: cq.DestinationMeasurement,
			DryRun:                 true,
			ExecutedAt:             time.Now().UTC().Format(time.RFC3339),
			ExecutedQuery:          executedQuery,
		})
	}

	// Execute the aggregation query using DuckDB
	ctx := context.Background()
	recordsWritten, err := h.executeAggregation(ctx, cq, executedQuery, startTime, endTime)
	if err != nil {
		executionDuration := time.Since(start).Seconds()

		// Record failed execution
		h.recordExecution(int64(queryID), executionID, "failed", startTime, endTime, nil, 0, executionDuration, err.Error())

		h.logger.Error().Err(err).Str("query_name", cq.Name).Msg("Continuous query execution failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Execution failed: " + err.Error(),
		})
	}

	executionDuration := time.Since(start).Seconds()

	// Record successful execution
	h.recordExecution(int64(queryID), executionID, "completed", startTime, endTime, nil, recordsWritten, executionDuration, "")

	// Update last processed time
	h.updateLastProcessedTime(int64(queryID), endTime)

	h.logger.Info().
		Str("query_name", cq.Name).
		Int64("records_written", recordsWritten).
		Float64("duration_seconds", executionDuration).
		Msg("Continuous query completed")

	return c.JSON(ExecuteCQResponse{
		QueryID:                int64(queryID),
		QueryName:              cq.Name,
		ExecutionID:            executionID,
		Status:                 "completed",
		StartTime:              startTime.Format(time.RFC3339),
		EndTime:                endTime.Format(time.RFC3339),
		RecordsRead:            nil,
		RecordsWritten:         recordsWritten,
		ExecutionTimeSeconds:   executionDuration,
		DestinationMeasurement: cq.DestinationMeasurement,
		DryRun:                 false,
		ExecutedAt:             time.Now().UTC().Format(time.RFC3339),
	})
}

// executeAggregation runs the aggregation query and writes results
func (h *ContinuousQueryHandler) executeAggregation(ctx context.Context, cq *ContinuousQuery, query string, startTime, endTime time.Time) (int64, error) {
	// Get storage base path for reading source data
	basePath := h.getStorageBasePath()
	if basePath == "" {
		return 0, fmt.Errorf("unable to determine storage base path")
	}

	// Build path pattern for source measurement
	measurementPath := filepath.Join(basePath, cq.Database, cq.SourceMeasurement, "**", "*.parquet")

	// Extract CTE names to avoid replacing them with read_parquet paths
	cteNames := extractCTENames(query)

	// Wrap query to read from parquet files
	// Replace measurement name in query with read_parquet
	// Use word boundary regex to avoid replacing partial matches (e.g., "cpu" in "cpu_user")
	// Skip if the source measurement name matches a CTE name (it's a virtual table reference)
	wrappedQuery := query
	if !cteNames[strings.ToLower(cq.SourceMeasurement)] {
		readParquetExpr := fmt.Sprintf("read_parquet('%s', union_by_name=true)", measurementPath)
		measurementPattern := regexp.MustCompile(`\bFROM\s+` + regexp.QuoteMeta(cq.SourceMeasurement) + `\b`)
		wrappedQuery = measurementPattern.ReplaceAllString(query, "FROM "+readParquetExpr)
	}

	h.logger.Debug().Str("query", wrappedQuery).Msg("Executing wrapped query")

	// Execute query using DuckDB
	rows, err := h.db.Query(wrappedQuery)
	if err != nil {
		return 0, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to get columns: %w", err)
	}

	// Read all results
	var records []map[string]interface{}
	for rows.Next() {
		// Create slice of interface{} to scan into
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return 0, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to map
		record := make(map[string]interface{})
		for i, col := range columns {
			record[col] = values[i]
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %w", err)
	}

	h.logger.Info().Int("record_count", len(records)).Msg("Aggregation query returned records")

	if len(records) == 0 {
		return 0, nil
	}

	// Write aggregated results to destination measurement using arrow buffer
	if h.arrowBuffer == nil {
		return 0, fmt.Errorf("arrow buffer not initialized")
	}

	// Convert row-based records to columnar format for WriteColumnarDirect
	// First, collect all column names
	columnSet := make(map[string]bool)
	for _, record := range records {
		for col := range record {
			columnSet[col] = true
		}
	}

	// Initialize columnar data structure
	columnarData := make(map[string][]interface{})
	for col := range columnSet {
		columnarData[col] = make([]interface{}, 0, len(records))
	}

	// Fill columnar data from records
	for _, record := range records {
		for col := range columnSet {
			val, ok := record[col]
			if !ok {
				val = nil
			}

			// Handle time field - convert to int64 microseconds (required by ArrowBuffer)
			if col == "time" && val != nil {
				switch t := val.(type) {
				case int64:
					// Already microseconds, keep as int64
					val = t
				case float64:
					val = int64(t)
				case time.Time:
					val = t.UnixMicro()
				case string:
					if parsed, err := time.Parse(time.RFC3339, t); err == nil {
						val = parsed.UTC().UnixMicro()
					}
				}
			}

			columnarData[col] = append(columnarData[col], val)
		}
	}

	// Ensure time column exists (as int64 microseconds)
	if _, hasTime := columnarData["time"]; !hasTime {
		nowMicro := time.Now().UTC().UnixMicro()
		times := make([]interface{}, len(records))
		for i := range times {
			times[i] = nowMicro
		}
		columnarData["time"] = times
	}

	// Write using WriteColumnarDirect
	if err := h.arrowBuffer.WriteColumnarDirect(ctx, cq.Database, cq.DestinationMeasurement, columnarData); err != nil {
		return 0, fmt.Errorf("failed to write records: %w", err)
	}

	return int64(len(records)), nil
}

// handleGetExecutions returns execution history for a query
func (h *ContinuousQueryHandler) handleGetExecutions(c *fiber.Ctx) error {
	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	limit := c.QueryInt("limit", 50)

	rows, err := h.sqliteDB.Query(`
		SELECT id, query_id, execution_id, execution_time, status, start_time, end_time, records_read, records_written, execution_duration_seconds, error_message
		FROM continuous_query_executions
		WHERE query_id = ?
		ORDER BY execution_time DESC
		LIMIT ?
	`, queryID, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get executions",
		})
	}
	defer rows.Close()

	var executions []CQExecution
	for rows.Next() {
		var ex CQExecution
		if err := rows.Scan(&ex.ID, &ex.QueryID, &ex.ExecutionID, &ex.ExecutionTime, &ex.Status, &ex.StartTime, &ex.EndTime, &ex.RecordsRead, &ex.RecordsWritten, &ex.ExecutionDurationSeconds, &ex.ErrorMessage); err != nil {
			continue
		}
		executions = append(executions, ex)
	}

	return c.JSON(fiber.Map{
		"query_id":   queryID,
		"executions": executions,
	})
}

// getQuery retrieves a single query by ID
func (h *ContinuousQueryHandler) getQuery(queryID int64) (*ContinuousQuery, error) {
	row := h.sqliteDB.QueryRow(`
		SELECT
			cq.id, cq.name, cq.description, cq.database, cq.source_measurement, cq.destination_measurement,
			cq.query, cq.interval, cq.retention_days, cq.delete_source_after_days, cq.is_active,
			cq.last_processed_time, cq.created_at, cq.updated_at,
			cqe.execution_time, cqe.status, cqe.records_written
		FROM continuous_queries cq
		LEFT JOIN (
			SELECT query_id, execution_time, status, records_written
			FROM continuous_query_executions
			WHERE id IN (SELECT MAX(id) FROM continuous_query_executions GROUP BY query_id)
		) cqe ON cq.id = cqe.query_id
		WHERE cq.id = ?
	`, queryID)

	var q ContinuousQuery
	err := row.Scan(
		&q.ID, &q.Name, &q.Description, &q.Database, &q.SourceMeasurement, &q.DestinationMeasurement,
		&q.Query, &q.Interval, &q.RetentionDays, &q.DeleteSourceAfterDays, &q.IsActive,
		&q.LastProcessedTime, &q.CreatedAt, &q.UpdatedAt,
		&q.LastExecutionTime, &q.LastExecutionStatus, &q.LastRecordsWritten,
	)
	if err != nil {
		return nil, err
	}

	return &q, nil
}

// getQueries retrieves all queries with optional filters
func (h *ContinuousQueryHandler) getQueries(database, isActiveStr string) ([]ContinuousQuery, error) {
	query := `
		SELECT
			cq.id, cq.name, cq.description, cq.database, cq.source_measurement, cq.destination_measurement,
			cq.query, cq.interval, cq.retention_days, cq.delete_source_after_days, cq.is_active,
			cq.last_processed_time, cq.created_at, cq.updated_at,
			cqe.execution_time, cqe.status, cqe.records_written
		FROM continuous_queries cq
		LEFT JOIN (
			SELECT query_id, execution_time, status, records_written
			FROM continuous_query_executions
			WHERE id IN (SELECT MAX(id) FROM continuous_query_executions GROUP BY query_id)
		) cqe ON cq.id = cqe.query_id
	`

	var whereClauses []string
	var args []interface{}

	if database != "" {
		whereClauses = append(whereClauses, "cq.database = ?")
		args = append(args, database)
	}

	if isActiveStr != "" {
		isActive := isActiveStr == "true"
		whereClauses = append(whereClauses, "cq.is_active = ?")
		args = append(args, isActive)
	}

	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	query += " ORDER BY cq.created_at DESC"

	rows, err := h.sqliteDB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queries []ContinuousQuery
	for rows.Next() {
		var q ContinuousQuery
		if err := rows.Scan(
			&q.ID, &q.Name, &q.Description, &q.Database, &q.SourceMeasurement, &q.DestinationMeasurement,
			&q.Query, &q.Interval, &q.RetentionDays, &q.DeleteSourceAfterDays, &q.IsActive,
			&q.LastProcessedTime, &q.CreatedAt, &q.UpdatedAt,
			&q.LastExecutionTime, &q.LastExecutionStatus, &q.LastRecordsWritten,
		); err != nil {
			continue
		}
		queries = append(queries, q)
	}

	return queries, nil
}

// recordExecution records an execution in the database
func (h *ContinuousQueryHandler) recordExecution(queryID int64, executionID, status string, startTime, endTime time.Time, recordsRead *int64, recordsWritten int64, duration float64, errorMessage string) {
	var errMsg *string
	if errorMessage != "" {
		errMsg = &errorMessage
	}

	_, err := h.sqliteDB.Exec(`
		INSERT INTO continuous_query_executions
		(query_id, execution_id, status, start_time, end_time, records_read, records_written, execution_duration_seconds, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, queryID, executionID, status, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), recordsRead, recordsWritten, duration, errMsg)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to record execution")
	}
}

// updateLastProcessedTime updates the last processed time for a query
func (h *ContinuousQueryHandler) updateLastProcessedTime(queryID int64, processedTime time.Time) {
	_, err := h.sqliteDB.Exec(`
		UPDATE continuous_queries SET last_processed_time = ? WHERE id = ?
	`, processedTime.Format(time.RFC3339), queryID)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to update last processed time")
	}
}

// getStorageBasePath returns the base path for storage
func (h *ContinuousQueryHandler) getStorageBasePath() string {
	switch backend := h.storage.(type) {
	case *storage.LocalBackend:
		return backend.GetBasePath()
	case *storage.S3Backend:
		h.logger.Warn().Msg("Continuous queries not fully supported for S3 backend yet")
		return ""
	default:
		return "./data/arc"
	}
}
