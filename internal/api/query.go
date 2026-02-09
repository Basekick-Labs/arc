package api

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/query"
	"github.com/basekick-labs/arc/internal/queryregistry"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// AuthManager interface for getting token info from context
type AuthManager interface {
	HasPermission(tokenInfo *auth.TokenInfo, permission string) bool
}

// RBACChecker interface for RBAC permission checking
type RBACChecker interface {
	IsRBACEnabled() bool
	CheckPermission(req *auth.PermissionCheckRequest) *auth.PermissionCheckResult
	CheckPermissionsBatch(reqs []*auth.PermissionCheckRequest) []*auth.PermissionCheckResult
}

// TableReference represents a database.measurement reference extracted from SQL
type TableReference struct {
	Database    string
	Measurement string
}

// Regex patterns for SHOW commands
var (
	showDatabasesPattern = regexp.MustCompile(`(?i)^\s*SHOW\s+DATABASES\s*;?\s*$`)
	showTablesPattern    = regexp.MustCompile(`(?i)^\s*SHOW\s+(?:TABLES|MEASUREMENTS)(?:\s+FROM\s+([\w-]+))?\s*;?\s*$`)
)

// Pre-compiled regex patterns for SQL-to-storage-path conversion
// These are compiled once at package init rather than on every query
// Note: We use 4 separate patterns instead of combined patterns because
// benchmarks showed simpler patterns execute faster despite more passes.
var (
	// Pattern for database.table references (e.g., FROM mydb.mytable)
	patternDBTable = regexp.MustCompile(`(?i)\bFROM\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b`)
	// Pattern for simple table references (FROM table_name)
	patternSimpleTable = regexp.MustCompile(`(?i)\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b`)
	// Pattern for database.table in JOIN clauses (e.g., JOIN mydb.mytable)
	// Includes LATERAL JOIN support: "LATERAL JOIN", "JOIN LATERAL", "CROSS JOIN LATERAL"
	patternJoinDBTable = regexp.MustCompile(`(?i)\b(?:(?:LEFT|RIGHT|INNER|OUTER|CROSS|NATURAL)?\s*)?(?:LATERAL\s+)?JOIN\s+(?:LATERAL\s+)?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b`)
	// Pattern for simple table in JOIN clauses (JOIN table_name)
	// Includes LATERAL JOIN support: "LATERAL JOIN", "JOIN LATERAL", "CROSS JOIN LATERAL"
	patternJoinSimpleTable = regexp.MustCompile(`(?i)\b(?:(?:LEFT|RIGHT|INNER|OUTER|CROSS|NATURAL)?\s*)?(?:LATERAL\s+)?JOIN\s+(?:LATERAL\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\b`)
	// Pattern to extract CTE names from WITH clauses
	// Matches: WITH name AS, WITH RECURSIVE name AS, and comma-separated CTEs
	patternCTENames = regexp.MustCompile(`(?i)\bWITH\s+(?:RECURSIVE\s+)?(\w+)\s+AS\s*\(|,\s*(\w+)\s+AS\s*\(`)

	// skipPrefixes are table name prefixes that should not be converted to storage paths
	skipPrefixes = []string{"read_parquet", "information_schema", "pg_", "duckdb_"}

	// Pattern for time_bucket function calls (2-argument form)
	// Matches: time_bucket(INTERVAL '1 hour', time_column) or time_bucket('1 hour', time_column)
	patternTimeBucket2Args = regexp.MustCompile(`(?i)\btime_bucket\s*\(\s*(?:INTERVAL\s*)?'(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)'\s*,\s*([^,)]+)\)`)

	// Pattern for time_bucket function calls (3-argument form with origin)
	// Matches: time_bucket(INTERVAL '1 hour', time_column, TIMESTAMP '2024-01-01')
	patternTimeBucket3Args = regexp.MustCompile(`(?i)\btime_bucket\s*\(\s*(?:INTERVAL\s*)?'(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)'\s*,\s*([^,]+)\s*,\s*(?:TIMESTAMP\s*)?'([^']+)'\s*\)`)

	// Pattern for date_trunc function calls
	// Matches: date_trunc('hour', time_column) or date_trunc('day', column_expr)
	patternDateTrunc = regexp.MustCompile(`(?i)\bdate_trunc\s*\(\s*'(second|minute|hour|day|week|month)'\s*,\s*([^)]+)\)`)

	// Pattern for extracting LIMIT clause value for result pre-allocation
	patternLimit = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)\b`)

)

// isIdentChar returns true if c is a valid SQL identifier character (a-z, A-Z, 0-9, _)
func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

// hasCrossDatabaseSyntax checks if SQL contains db.table patterns without using regex.
// This is faster than regex-based detection for simple pattern matching.
// Used to reject queries that use db.table syntax when x-arc-database header is set.
func hasCrossDatabaseSyntax(sql string) bool {
	sqlLower := strings.ToLower(sql)

	// Check for "FROM identifier.identifier" or "JOIN identifier.identifier" patterns
	for _, keyword := range []string{"from ", "join "} {
		pos := 0
		for {
			idx := strings.Index(sqlLower[pos:], keyword)
			if idx < 0 {
				break
			}
			idx += pos + len(keyword)
			pos = idx

			// Skip whitespace after keyword
			for idx < len(sql) && (sql[idx] == ' ' || sql[idx] == '\t' || sql[idx] == '\n') {
				idx++
			}

			// Find first identifier (database name)
			start := idx
			for idx < len(sql) && isIdentChar(sql[idx]) {
				idx++
			}
			if idx == start || idx >= len(sql) {
				continue
			}

			// Check for dot followed by another identifier (table name)
			if sql[idx] == '.' && idx+1 < len(sql) && isIdentChar(sql[idx+1]) {
				return true
			}
		}
	}
	return false
}

// isSingleTableQuery returns true if query has exactly one FROM and no JOINs.
// These queries can use a faster transformation path.
func isSingleTableQuery(sqlLower string) bool {
	fromCount := strings.Count(sqlLower, "from ")
	if fromCount != 1 {
		return false
	}
	// Check for any JOIN type
	if strings.Contains(sqlLower, " join ") {
		return false
	}
	// Check for subquery (FROM followed by parenthesis)
	idx := strings.Index(sqlLower, "from ")
	if idx >= 0 {
		rest := strings.TrimLeft(sqlLower[idx+5:], " \t\n")
		if len(rest) > 0 && rest[0] == '(' {
			return false
		}
	}
	return true
}

// Pools for reusing scan buffers to reduce allocations in row fetching.
// Each query reuses buffers from these pools instead of allocating new slices per row.
var (
	// scanBufferPool holds reusable scanBuffer instances
	scanBufferPool = sync.Pool{
		New: func() interface{} {
			return &scanBuffer{
				values:    make([]interface{}, 0, 32),
				valuePtrs: make([]interface{}, 0, 32),
			}
		},
	}
)

// scanBuffer holds reusable slices for row scanning
type scanBuffer struct {
	values    []interface{}
	valuePtrs []interface{}
}

// reset prepares the buffer for reuse with a specific column count
func (b *scanBuffer) reset(numCols int) {
	// Ensure capacity
	if cap(b.values) < numCols {
		b.values = make([]interface{}, numCols)
		b.valuePtrs = make([]interface{}, numCols)
	} else {
		b.values = b.values[:numCols]
		b.valuePtrs = b.valuePtrs[:numCols]
	}
	// Set up pointers
	for i := range b.values {
		b.values[i] = nil // Clear previous values
		b.valuePtrs[i] = &b.values[i]
	}
}

// extractLimit extracts the LIMIT value from SQL for result pre-allocation.
// Returns defaultLimit if no LIMIT clause is found.
func extractLimit(sql string, defaultLimit int) int {
	if match := patternLimit.FindStringSubmatch(sql); match != nil {
		if limit, err := strconv.Atoi(match[1]); err == nil && limit > 0 {
			// Cap at 100k to avoid excessive pre-allocation
			if limit > 100000 {
				return 100000
			}
			return limit
		}
	}
	return defaultLimit
}

// extractCTENames extracts CTE names from a SQL query's WITH clause.
// Returns a set of lowercase CTE names for efficient lookup.
func extractCTENames(sql string) map[string]bool {
	cteNames := make(map[string]bool)
	matches := patternCTENames.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		// match[1] is from "WITH name AS" or "WITH RECURSIVE name AS"
		// match[2] is from ", name AS"
		if match[1] != "" {
			cteNames[strings.ToLower(match[1])] = true
		}
		if match[2] != "" {
			cteNames[strings.ToLower(match[2])] = true
		}
	}
	return cteNames
}

// rewriteTimeBucket converts time_bucket() to epoch-based arithmetic.
// All intervals are converted to epoch-based arithmetic for consistent ~2.5x performance improvement.
// Handles both 2-arg and 3-arg (with origin) forms.
func rewriteTimeBucket(sql string) string {
	// Fast path: skip regex if keyword not present (avoids 16 allocs, 41KB per call)
	if !strings.Contains(strings.ToLower(sql), "time_bucket") {
		return sql
	}

	// First handle 3-argument form (with origin) - must come first to avoid partial matches
	sql = patternTimeBucket3Args.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternTimeBucket3Args.FindStringSubmatch(match)
		if len(parts) < 5 {
			return match
		}

		amount := parts[1]
		unit := strings.ToLower(strings.TrimSuffix(parts[2], "s"))
		column := strings.TrimSpace(parts[3])
		origin := parts[4] // e.g., "2024-01-01 00:30:00"

		// Parse origin timestamp
		originTime, err := parseTimeBucketOrigin(origin)
		if err != nil {
			return match // Keep original if can't parse origin
		}

		// Calculate interval in seconds
		seconds := intervalToSeconds(amount, unit)
		if seconds == 0 {
			return match // Keep original for months or invalid units
		}

		// Calculate origin offset (seconds since epoch)
		originEpoch := originTime.Unix()

		// Formula: origin + floor((epoch(col) - origin_epoch) / interval) * interval
		return fmt.Sprintf("to_timestamp(%d + ((epoch(%s)::BIGINT - %d) / %d) * %d)",
			originEpoch, column, originEpoch, seconds, seconds)
	})

	// Then handle 2-argument form (no origin)
	sql = patternTimeBucket2Args.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternTimeBucket2Args.FindStringSubmatch(match)
		if len(parts) < 4 {
			return match
		}

		amount := parts[1]
		unit := strings.ToLower(strings.TrimSuffix(parts[2], "s"))
		column := strings.TrimSpace(parts[3])

		// Use epoch-based arithmetic for all intervals (2.5x faster than date_trunc)
		seconds := intervalToSeconds(amount, unit)
		if seconds == 0 {
			return match // Keep original for months (variable length)
		}

		return fmt.Sprintf("to_timestamp((epoch(%s)::BIGINT / %d) * %d)", column, seconds, seconds)
	})

	return sql
}

// rewriteDateTrunc converts date_trunc() to epoch-based arithmetic for 2.5x performance improvement.
// Handles: date_trunc('hour', time), date_trunc('day', time), etc.
// Months are not converted because they have variable length.
func rewriteDateTrunc(sql string) string {
	// Fast path: skip regex if keyword not present (avoids 5 allocs, 2.3KB per call)
	if !strings.Contains(strings.ToLower(sql), "date_trunc") {
		return sql
	}

	return patternDateTrunc.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternDateTrunc.FindStringSubmatch(match)
		if len(parts) < 3 {
			return match
		}

		unit := strings.ToLower(parts[1])
		column := strings.TrimSpace(parts[2])

		// Get interval in seconds
		seconds := intervalToSeconds("1", unit)
		if seconds == 0 {
			return match // Keep original for months (variable length)
		}

		// Convert to epoch-based arithmetic: to_timestamp((epoch(col) / interval) * interval)
		return fmt.Sprintf("to_timestamp((epoch(%s)::BIGINT / %d) * %d)", column, seconds, seconds)
	})
}

// intervalToSeconds converts an interval amount and unit to seconds.
// Returns 0 for variable-length intervals (months) or invalid units.
func intervalToSeconds(amount, unit string) int {
	n, err := strconv.Atoi(amount)
	if err != nil {
		return 0
	}

	switch unit {
	case "second":
		return n
	case "minute":
		return n * 60
	case "hour":
		return n * 3600
	case "day":
		return n * 86400
	case "week":
		return n * 604800
	default:
		return 0 // months have variable length
	}
}

// parseTimeBucketOrigin parses common timestamp formats used in time_bucket origin parameter.
func parseTimeBucketOrigin(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp: %s", s)
}

// sqlFeatures contains flags for what features are present in a SQL string
type sqlFeatures struct {
	hasQuotes      bool // single or double quotes
	hasDashComment bool // -- comment
	hasBlockComment bool // /* comment */
}

// scanSQLFeatures scans SQL once to detect presence of quotes and comments.
// This avoids multiple strings.Contains calls.
func scanSQLFeatures(sql string) sqlFeatures {
	var f sqlFeatures
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' || ch == '"' {
			f.hasQuotes = true
		} else if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			f.hasDashComment = true
		} else if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			f.hasBlockComment = true
		}
		// Early exit if we found everything
		if f.hasQuotes && f.hasDashComment && f.hasBlockComment {
			break
		}
	}
	return f
}


// stripSQLComments removes SQL comments from the query.
// Handles: -- single line comments and /* multi-line comments */
// This must be called AFTER string masking to avoid stripping comments inside strings.
func stripSQLComments(sql string, hasComments bool) string {
	// Fast path: if no comment markers, return original string (avoids allocation)
	if !hasComments {
		return sql
	}

	var result strings.Builder
	result.Grow(len(sql))

	i := 0
	for i < len(sql) {
		// Check for single-line comment (--)
		if i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-' {
			// Skip until end of line or end of string
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			// Keep the newline if present
			if i < len(sql) {
				result.WriteByte('\n')
				i++
			}
			continue
		}

		// Check for multi-line comment (/* ... */)
		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			i += 2 // Skip /*
			// Skip until closing */
			for i+1 < len(sql) {
				if sql[i] == '*' && sql[i+1] == '/' {
					i += 2 // Skip */
					break
				}
				i++
			}
			// If we reached end without finding */, skip remaining
			if i+1 >= len(sql) {
				i = len(sql)
			}
			// Add a space to prevent token concatenation
			result.WriteByte(' ')
			continue
		}

		result.WriteByte(sql[i])
		i++
	}

	return result.String()
}

// QueryHandler handles SQL query endpoints
type QueryHandler struct {
	db               *database.DuckDB
	storage          storage.Backend
	pruner           *pruning.PartitionPruner
	queryCache       *database.QueryCache
	logger           zerolog.Logger
	authManager      AuthManager
	rbacManager      RBACChecker
	debugEnabled     bool // Cached check for debug logging to avoid repeated level checks
	parallelExecutor *query.ParallelExecutor
	queryTimeout     time.Duration // Query timeout (0 = no timeout)

	// Cluster routing support
	router *cluster.Router

	// Tiering support for multi-tier query routing (hot/cold)
	tieringManager *tiering.Manager

	// Query governance (Enterprise feature - rate limiting and quotas)
	governanceManager *governance.Manager
	licenseClient     *license.Client

	// Query management (Enterprise feature - active query tracking and cancellation)
	queryRegistry *queryregistry.Registry
}

// isDebugEnabled returns true if debug logging is enabled.
// This is cached at handler creation to avoid repeated level checks in hot paths.
func (h *QueryHandler) isDebugEnabled() bool {
	return h.debugEnabled
}

// QueryRequest represents a SQL query request
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse represents a SQL query response
type QueryResponse struct {
	Success         bool                   `json:"success"`
	Columns         []string               `json:"columns"`
	Data            [][]interface{}        `json:"data"`
	RowCount        int                    `json:"row_count"`
	ExecutionTimeMs float64                `json:"execution_time_ms"`
	Timestamp       string                 `json:"timestamp"`
	Error           string                 `json:"error,omitempty"`
	Profile         *database.QueryProfile `json:"profile,omitempty"`
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
// queryTimeoutSeconds: timeout for query execution in seconds (0 = no timeout)
func NewQueryHandler(db *database.DuckDB, storage storage.Backend, logger zerolog.Logger, queryTimeoutSeconds int) *QueryHandler {
	handlerLogger := logger.With().Str("component", "query-handler").Logger()
	pruner := pruning.NewPartitionPruner(logger)
	pruner.SetStorageBackend(storage) // Enable S3/Azure partition filtering

	var queryTimeout time.Duration
	if queryTimeoutSeconds > 0 {
		queryTimeout = time.Duration(queryTimeoutSeconds) * time.Second
		handlerLogger.Info().Int("timeout_seconds", queryTimeoutSeconds).Msg("Query timeout configured")
	}

	return &QueryHandler{
		db:               db,
		storage:          storage,
		pruner:           pruner,
		queryCache:       database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		logger:           handlerLogger,
		authManager:      nil,
		rbacManager:      nil,
		debugEnabled:     handlerLogger.GetLevel() <= zerolog.DebugLevel,
		parallelExecutor: query.NewParallelExecutor(db.DB(), query.DefaultParallelConfig(), handlerLogger),
		queryTimeout:     queryTimeout,
	}
}

// SetAuthAndRBAC sets the auth and RBAC managers for permission checking
func (h *QueryHandler) SetAuthAndRBAC(am AuthManager, rm RBACChecker) {
	h.authManager = am
	h.rbacManager = rm
}

// SetRouter sets the cluster router for request forwarding.
// When set, query requests from nodes that cannot handle queries will be forwarded.
// Note: Currently writers always process queries locally (CanQuery=true).
// This is provided for future extensibility (e.g., prefer_readers mode).
func (h *QueryHandler) SetRouter(router *cluster.Router) {
	h.router = router
}

// SetTieringManager sets the tiering manager for multi-tier query routing.
// When set, queries will check both hot and cold tiers for data.
func (h *QueryHandler) SetTieringManager(manager *tiering.Manager) {
	h.tieringManager = manager
}

// SetGovernance sets the governance manager and license client for query rate limiting and quotas.
func (h *QueryHandler) SetGovernance(manager *governance.Manager, lc *license.Client) {
	h.governanceManager = manager
	h.licenseClient = lc
}

// SetQueryRegistry sets the query registry for long-running query management.
func (h *QueryHandler) SetQueryRegistry(registry *queryregistry.Registry) {
	h.queryRegistry = registry
}

// extractTableReferences extracts all database.measurement references from SQL
// Returns a slice of TableReference structs for permission checking
func extractTableReferences(sql string) []TableReference {
	var refs []TableReference
	seen := make(map[string]bool)

	// Extract database.table references (FROM database.table, JOIN database.table)
	// These take priority - we track their positions to avoid double-counting
	dbTableMatches := patternDBTable.FindAllStringSubmatch(sql, -1)
	for _, match := range dbTableMatches {
		if len(match) >= 3 {
			key := match[1] + "." + match[2]
			if !seen[key] {
				seen[key] = true
				refs = append(refs, TableReference{
					Database:    match[1],
					Measurement: match[2],
				})
			}
		}
	}

	// Also check JOIN patterns for database.table
	joinDBMatches := patternJoinDBTable.FindAllStringSubmatch(sql, -1)
	for _, match := range joinDBMatches {
		if len(match) >= 3 {
			key := match[1] + "." + match[2]
			if !seen[key] {
				seen[key] = true
				refs = append(refs, TableReference{
					Database:    match[1],
					Measurement: match[2],
				})
			}
		}
	}

	// Extract simple table references (defaults to "default" database)
	// BUT skip tables that are part of database.table patterns we already found
	simpleMatches := patternSimpleTable.FindAllStringSubmatchIndex(sql, -1)
	for _, matchIdx := range simpleMatches {
		if len(matchIdx) >= 4 {
			tableName := sql[matchIdx[2]:matchIdx[3]]
			table := strings.ToLower(tableName)

			// Skip system tables and already-converted paths
			if shouldSkipTableConversion(table) {
				continue
			}

			// Check if this table name is followed by a dot (meaning it's a database name, not a table)
			endIdx := matchIdx[3]
			if endIdx < len(sql) && sql[endIdx] == '.' {
				// This is actually a database name in "database.table", skip it
				continue
			}

			key := "default." + table
			if !seen[key] {
				seen[key] = true
				refs = append(refs, TableReference{
					Database:    "default",
					Measurement: tableName,
				})
			}
		}
	}

	// Also check JOIN simple patterns
	joinSimpleMatches := patternJoinSimpleTable.FindAllStringSubmatchIndex(sql, -1)
	for _, matchIdx := range joinSimpleMatches {
		if len(matchIdx) >= 4 {
			tableName := sql[matchIdx[2]:matchIdx[3]]
			table := strings.ToLower(tableName)

			if shouldSkipTableConversion(table) {
				continue
			}

			// Check if this table name is followed by a dot
			endIdx := matchIdx[3]
			if endIdx < len(sql) && sql[endIdx] == '.' {
				continue
			}

			key := "default." + table
			if !seen[key] {
				seen[key] = true
				refs = append(refs, TableReference{
					Database:    "default",
					Measurement: tableName,
				})
			}
		}
	}

	return refs
}

// checkQueryPermissions checks RBAC permissions for all tables referenced in a query
// Returns nil if access is allowed, or an error describing what access was denied
// Uses batch permission checking for efficiency when multiple tables are referenced
func (h *QueryHandler) checkQueryPermissions(c *fiber.Ctx, sql, permission string) error {
	// If no RBAC manager, skip permission check (handled by basic auth middleware)
	if h.rbacManager == nil || !h.rbacManager.IsRBACEnabled() {
		return nil
	}

	// Get token info from context
	tokenInfo := auth.GetTokenInfo(c)
	if tokenInfo == nil {
		// No token info means basic auth middleware allowed it (or auth disabled)
		return nil
	}

	// Extract table references from SQL
	tableRefs := extractTableReferences(sql)
	if len(tableRefs) == 0 {
		// No tables referenced (e.g., SELECT 1+1)
		return nil
	}

	if h.debugEnabled {
		h.logger.Debug().
			Str("sql", sql).
			Int("table_count", len(tableRefs)).
			Msg("Checking RBAC permissions for query")
	}

	// Build batch request for all table references
	reqs := make([]*auth.PermissionCheckRequest, len(tableRefs))
	for i, ref := range tableRefs {
		reqs[i] = &auth.PermissionCheckRequest{
			TokenInfo:   tokenInfo,
			Database:    ref.Database,
			Measurement: ref.Measurement,
			Permission:  permission,
		}
	}

	// Batch check all permissions (single RBAC data load for same token)
	results := h.rbacManager.CheckPermissionsBatch(reqs)

	// Check for any denials
	for i, result := range results {
		if !result.Allowed {
			ref := tableRefs[i]
			h.logger.Warn().
				Str("database", ref.Database).
				Str("measurement", ref.Measurement).
				Str("permission", permission).
				Str("reason", result.Reason).
				Int64("token_id", tokenInfo.ID).
				Msg("RBAC permission denied")
			return fmt.Errorf("access denied: no %s permission for %s.%s", permission, ref.Database, ref.Measurement)
		}

		if h.debugEnabled {
			h.logger.Debug().
				Str("database", tableRefs[i].Database).
				Str("measurement", tableRefs[i].Measurement).
				Str("permission", permission).
				Str("source", result.Source).
				Msg("RBAC permission granted")
		}
	}

	return nil
}

// checkMeasurementPermission checks RBAC permission for a specific database/measurement
// This is a simpler version for endpoints where database/measurement are known directly
func (h *QueryHandler) checkMeasurementPermission(c *fiber.Ctx, database, measurement, permission string) error {
	// If no RBAC manager, skip permission check (handled by basic auth middleware)
	if h.rbacManager == nil || !h.rbacManager.IsRBACEnabled() {
		return nil
	}

	// Get token info from context
	tokenInfo := auth.GetTokenInfo(c)
	if tokenInfo == nil {
		// No token info means basic auth middleware allowed it (or auth disabled)
		return nil
	}

	result := h.rbacManager.CheckPermission(&auth.PermissionCheckRequest{
		TokenInfo:   tokenInfo,
		Database:    database,
		Measurement: measurement,
		Permission:  permission,
	})

	if !result.Allowed {
		h.logger.Warn().
			Str("database", database).
			Str("measurement", measurement).
			Str("permission", permission).
			Str("reason", result.Reason).
			Int64("token_id", tokenInfo.ID).
			Msg("RBAC permission denied")
		return fmt.Errorf("access denied: no %s permission for %s.%s", permission, database, measurement)
	}

	h.logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Str("permission", permission).
		Str("source", result.Source).
		Msg("RBAC permission granted")

	return nil
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
	// Cache timestamp format once per request to avoid repeated formatting
	timestamp := start.UTC().Format(time.RFC3339)
	m := metrics.Get()
	m.IncQueryRequests()

	// Check if this request should be forwarded to a reader/writer node
	// Compactor nodes cannot process queries locally, so they forward to readers/writers
	if ShouldForwardQuery(h.router, c) {
		h.logger.Debug().Msg("Forwarding query request to reader/writer node")

		httpReq, err := BuildHTTPRequest(c)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to build HTTP request for forwarding")
			m.IncQueryErrors()
			return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
				Success:   false,
				Error:     "Failed to prepare request for forwarding",
				Timestamp: timestamp,
			})
		}

		resp, err := h.router.RouteQuery(c.Context(), httpReq)
		if err == cluster.ErrLocalNodeCanHandle {
			// Fall through to local processing
			goto localProcessing
		}
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to route query request")
			m.IncQueryErrors()
			return HandleRoutingError(c, err)
		}

		return CopyResponse(c, resp)
	}

localProcessing:

	// Query governance enforcement (Enterprise feature - rate limiting and quotas)
	var governanceMaxRows int
	var governanceTimeout time.Duration
	if h.governanceManager != nil && h.licenseClient != nil && h.licenseClient.CanUseQueryGovernance() {
		if tokenInfo := auth.GetTokenInfo(c); tokenInfo != nil {
			if result := h.governanceManager.CheckRateLimit(tokenInfo.ID); !result.Allowed {
				c.Set("Retry-After", strconv.Itoa(result.RetryAfterSec))
				m.IncQueryErrors()
				metrics.Get().IncGovernanceRateLimited()
				h.logger.Warn().
					Int64("token_id", tokenInfo.ID).
					Str("token_name", tokenInfo.Name).
					Str("reason", result.Reason).
					Int("retry_after_sec", result.RetryAfterSec).
					Msg("Query rejected: rate limit exceeded")
				return c.Status(fiber.StatusTooManyRequests).JSON(QueryResponse{
					Success:   false,
					Error:     result.Reason,
					Timestamp: timestamp,
				})
			}
			if result := h.governanceManager.CheckQuota(tokenInfo.ID); !result.Allowed {
				m.IncQueryErrors()
				metrics.Get().IncGovernanceQuotaExhausted()
				h.logger.Warn().
					Int64("token_id", tokenInfo.ID).
					Str("token_name", tokenInfo.Name).
					Str("reason", result.Reason).
					Msg("Query rejected: quota exhausted")
				return c.Status(fiber.StatusTooManyRequests).JSON(QueryResponse{
					Success:   false,
					Error:     result.Reason,
					Timestamp: timestamp,
				})
			} else {
				governanceMaxRows = result.MaxRows
				governanceTimeout = result.MaxDuration
			}
		}
	}

	// Parse request body
	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid request body: " + err.Error(),
			Timestamp: timestamp,
		})
	}

	// Validate SQL (empty, max length, dangerous patterns)
	if err := ValidateSQLRequest(req.SQL); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     err.Error(),
			Timestamp: timestamp,
		})
	}

	// Extract x-arc-database header for optimized query path
	headerDB := c.Get("x-arc-database")

	// If header is set, reject cross-database syntax (db.table not allowed)
	if headerDB != "" && hasCrossDatabaseSyntax(req.SQL) {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Cross-database queries (db.table syntax) not allowed when x-arc-database header is set",
			Timestamp: timestamp,
		})
	}

	// Handle SHOW DATABASES command
	if showDatabasesPattern.MatchString(req.SQL) {
		// Check RBAC - user needs at least some read permission to see databases
		if err := h.checkMeasurementPermission(c, "*", "*", "read"); err != nil {
			m.IncQueryErrors()
			return c.Status(fiber.StatusForbidden).JSON(QueryResponse{
				Success:   false,
				Error:     "access denied: no read permission to list databases",
				Timestamp: timestamp,
			})
		}
		return h.handleShowDatabases(c, start)
	}

	// Handle SHOW TABLES/MEASUREMENTS command
	if matches := showTablesPattern.FindStringSubmatch(req.SQL); matches != nil {
		database := "default"
		if len(matches) > 1 && matches[1] != "" {
			database = matches[1]
		}
		// Check RBAC - user needs read permission on the specific database
		if err := h.checkMeasurementPermission(c, database, "*", "read"); err != nil {
			m.IncQueryErrors()
			return c.Status(fiber.StatusForbidden).JSON(QueryResponse{
				Success:   false,
				Error:     fmt.Sprintf("access denied: no read permission for database '%s'", database),
				Timestamp: timestamp,
			})
		}
		return h.handleShowTables(c, start, database)
	}

	// Check RBAC permissions for all tables referenced in the query
	if err := h.checkQueryPermissions(c, req.SQL, "read"); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusForbidden).JSON(QueryResponse{
			Success:   false,
			Error:     err.Error(),
			Timestamp: timestamp,
		})
	}

	// Convert SQL to storage paths and check for parallel execution opportunity
	convertedSQL, parallelInfo, cached := h.getTransformedSQLForParallel(req.SQL, headerDB)

	if h.debugEnabled {
		h.logger.Debug().
			Str("original_sql", req.SQL).
			Str("converted_sql", convertedSQL).
			Bool("cache_hit", cached).
			Bool("parallel", parallelInfo != nil).
			Str("header_db", headerDB).
			Msg("Executing query")
	}

	var columns []string
	var data [][]interface{}
	var rowCount int
	var profile *database.QueryProfile

	// Register query with management registry if available
	var queryID string
	var queryCtx context.Context
	if h.queryRegistry != nil {
		var tokenID int64
		var tokenName string
		if tokenInfo := auth.GetTokenInfo(c); tokenInfo != nil {
			tokenID = tokenInfo.ID
			tokenName = tokenInfo.Name
		}
		isParallel := parallelInfo != nil && h.parallelExecutor != nil
		partCount := 0
		if isParallel {
			partCount = len(parallelInfo.Paths)
		}
		queryID, queryCtx = h.queryRegistry.Register(
			c.UserContext(), req.SQL, tokenID, tokenName, c.IP(), isParallel, partCount,
		)
		c.Set("X-Arc-Query-ID", queryID)
	}

	// Compute effective timeout for both parallel and standard paths
	effectiveTimeout := h.queryTimeout
	if governanceTimeout > 0 {
		effectiveTimeout = governanceTimeout
	}

	// Check for profiling mode
	profileMode := c.Get("x-arc-profile") == "true"

	// Execute query - use parallel path if available
	if parallelInfo != nil && h.parallelExecutor != nil {
		// Parallel partition execution — use registry context if available
		execCtx := c.UserContext()
		if queryCtx != nil {
			execCtx = queryCtx
		}
		var cancelTimeout context.CancelFunc
		if effectiveTimeout > 0 {
			execCtx, cancelTimeout = context.WithTimeout(execCtx, effectiveTimeout)
			defer cancelTimeout()
		}
		results, err := h.parallelExecutor.ExecutePartitioned(
			execCtx,
			parallelInfo.Paths,
			parallelInfo.QueryTemplate,
			parallelInfo.ReadParquetOptions,
		)
		if err != nil {
			m.IncQueryErrors()
			if h.queryRegistry != nil && queryID != "" {
				if execCtx.Err() == context.DeadlineExceeded {
					h.queryRegistry.TimedOut(queryID)
				} else if execCtx.Err() == context.Canceled {
					// Already marked as cancelled by Cancel() — no-op
				} else {
					h.queryRegistry.Fail(queryID, "Parallel query execution failed")
				}
			}
			h.logger.Error().Err(err).Str("sql", req.SQL).Msg("Parallel query execution failed")
			return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
				Success:         false,
				Error:           "Query execution failed",
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
		}

		// Create merged iterator
		iter, err := query.NewMergedRowIterator(results, h.logger)
		if err != nil {
			// Close all results on error
			for _, r := range results {
				if r.Rows != nil {
					r.Rows.Close()
				}
			}
			m.IncQueryErrors()
			if h.queryRegistry != nil && queryID != "" {
				h.queryRegistry.Fail(queryID, "Failed to create merged iterator")
			}
			h.logger.Error().Err(err).Msg("Failed to create merged iterator")
			return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
				Success:         false,
				Error:           "Query execution failed",
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
		}
		defer iter.Close()

		columns = iter.Columns()
		numCols := len(columns)
		estimatedRows := extractLimit(req.SQL, 1000)
		data = make([][]interface{}, 0, estimatedRows)

		for iter.Next() {
			values, err := iter.ScanBuffer()
			if err != nil {
				h.logger.Error().Err(err).Msg("Failed to scan row")
				continue
			}

			row := make([]interface{}, numCols)
			for i, v := range values {
				row[i] = h.convertValue(v)
			}
			data = append(data, row)
			rowCount++

			if governanceMaxRows > 0 && rowCount >= governanceMaxRows {
				break
			}
		}

		if err := iter.Err(); err != nil {
			h.logger.Error().Err(err).Msg("Error iterating merged rows")
		}
	} else {
		// Standard single-query execution
		var rows *sql.Rows
		var err error

		// Create context with timeout if configured (0 = no timeout)
		ctx := c.UserContext()
		if queryCtx != nil {
			ctx = queryCtx
		}
		var cancel context.CancelFunc
		if effectiveTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, effectiveTimeout)
			defer cancel()
		}

		if profileMode {
			// Use profiled query to capture timing breakdown (with timeout support)
			rows, profile, err = h.db.QueryWithProfileContext(ctx, convertedSQL)
		} else {
			rows, err = h.db.QueryContext(ctx, convertedSQL)
		}

		if err != nil {
			m.IncQueryErrors()
			// Check if it was a timeout
			if effectiveTimeout > 0 && ctx.Err() == context.DeadlineExceeded {
				m.IncQueryTimeouts()
				if h.queryRegistry != nil && queryID != "" {
					h.queryRegistry.TimedOut(queryID)
				}
				h.logger.Error().Err(err).Str("sql", req.SQL).Dur("timeout", effectiveTimeout).Msg("Query timed out")
				return c.Status(fiber.StatusGatewayTimeout).JSON(QueryResponse{
					Success:         false,
					Error:           "Query timed out",
					ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
					Timestamp:       timestamp,
				})
			}
			if h.queryRegistry != nil && queryID != "" {
				if ctx.Err() == context.Canceled {
					// Already marked as cancelled by Cancel() — no-op
				} else {
					h.queryRegistry.Fail(queryID, "Query execution failed")
				}
			}
			h.logger.Error().Err(err).Str("sql", req.SQL).Msg("Query execution failed")
			return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
				Success:         false,
				Error:           "Query execution failed",
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
		}
		defer rows.Close()

		// Get column names
		columns, err = rows.Columns()
		if err != nil {
			m.IncQueryErrors()
			h.logger.Error().Err(err).Msg("Failed to get column names")
			return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
				Success:         false,
				Error:           "Query execution failed",
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
		}

		// Fetch results with optimized memory allocation
		estimatedRows := extractLimit(req.SQL, 1000)
		data = make([][]interface{}, 0, estimatedRows)
		numCols := len(columns)

		// Get scan buffer from pool to avoid allocating per row
		buf := scanBufferPool.Get().(*scanBuffer)
		buf.reset(numCols)
		defer func() {
			for i := range buf.values {
				buf.values[i] = nil
			}
			scanBufferPool.Put(buf)
		}()

		for rows.Next() {
			if err := rows.Scan(buf.valuePtrs...); err != nil {
				h.logger.Error().Err(err).Msg("Failed to scan row")
				continue
			}

			row := make([]interface{}, numCols)
			for i, v := range buf.values {
				row[i] = h.convertValue(v)
			}
			data = append(data, row)
			rowCount++

			if governanceMaxRows > 0 && rowCount >= governanceMaxRows {
				break
			}
		}

		if err := rows.Err(); err != nil {
			h.logger.Error().Err(err).Msg("Error iterating rows")
		}
	}

	executionTime := float64(time.Since(start).Milliseconds())

	// Record query completion in registry
	if h.queryRegistry != nil && queryID != "" {
		h.queryRegistry.Complete(queryID, rowCount)
	}

	// Record success metrics
	m.IncQuerySuccess()
	m.IncQueryRows(int64(rowCount))
	m.RecordQueryLatency(time.Since(start).Microseconds())

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
		Timestamp:       timestamp,
		Profile:         profile,
	})
}

// SQLValidationError represents an error from SQL validation
type SQLValidationError struct {
	Message string
}

func (e *SQLValidationError) Error() string {
	return e.Message
}

// ValidateSQLRequest validates an SQL query for common issues.
// Returns nil if valid, or an error with appropriate message.
// This is a shared function used by multiple query endpoints.
func ValidateSQLRequest(sql string) error {
	if strings.TrimSpace(sql) == "" {
		return &SQLValidationError{Message: "SQL query is required"}
	}

	if len(sql) > 10000 {
		return &SQLValidationError{Message: "SQL query exceeds maximum length (10000 characters)"}
	}

	// Check for dangerous SQL patterns
	for _, pattern := range dangerousSQLPatterns {
		if pattern.MatchString(sql) {
			return &SQLValidationError{Message: "Dangerous SQL operation not allowed"}
		}
	}

	return nil
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

// getTransformedSQL returns the transformed SQL with caching.
// If headerDB is non-empty, uses the optimized path with that database for all tables.
// Returns the transformed SQL and whether it was a cache hit.
func (h *QueryHandler) getTransformedSQL(sql string, headerDB string) (string, bool) {
	// Fast path: queries already using read_parquet don't need transformation
	sqlLower := strings.ToLower(sql)
	if strings.Contains(sqlLower, "read_parquet") {
		return sql, true // Return as "hit" since no work needed
	}

	// Fast path: queries without FROM or JOIN don't need table transformation
	// (e.g., SELECT 1+1, SELECT NOW(), SHOW commands handled elsewhere)
	if !strings.Contains(sqlLower, "from") && !strings.Contains(sqlLower, "join") {
		return sql, true
	}

	// Build cache key - include header database if provided
	cacheKey := sql
	if headerDB != "" {
		cacheKey = headerDB + ":" + sql
	}

	// Check cache
	if transformed, ok := h.queryCache.Get(cacheKey); ok {
		return transformed, true
	}

	// Transform using appropriate method
	var transformed string
	if headerDB != "" {
		transformed = h.convertSQLToStoragePathsWithHeaderDB(sql, headerDB)
	} else {
		transformed = h.convertSQLToStoragePaths(sql)
	}

	h.queryCache.Set(cacheKey, transformed)
	return transformed, false
}

// getTransformedSQLForParallel returns the transformed SQL and parallel execution info.
// This variant checks if the query can benefit from parallel partition scanning.
// Only simple single-table queries with header DB can use parallel execution.
// Returns (sql, parallel_info, cache_hit).
func (h *QueryHandler) getTransformedSQLForParallel(sql string, headerDB string) (string, *ParallelQueryInfo, bool) {
	sqlLower := strings.ToLower(sql)

	// Fast paths that don't support parallel execution
	if strings.Contains(sqlLower, "read_parquet") {
		return sql, nil, true
	}
	if !strings.Contains(sqlLower, "from") && !strings.Contains(sqlLower, "join") {
		return sql, nil, true
	}

	// Parallel execution only supported for simple single-table queries with header DB
	// Complex queries (JOINs, subqueries, CTEs) fall back to standard execution
	if headerDB == "" || !isSingleTableQuery(sqlLower) || strings.Contains(sqlLower, "with ") {
		transformed, cached := h.getTransformedSQL(sql, headerDB)
		return transformed, nil, cached
	}

	// Check for features that prevent fast path
	features := scanSQLFeatures(sql)
	if features.hasQuotes || features.hasDashComment || features.hasBlockComment {
		transformed, cached := h.getTransformedSQL(sql, headerDB)
		return transformed, nil, cached
	}

	// Rewrite time functions if present
	if strings.Contains(sqlLower, "time_bucket") || strings.Contains(sqlLower, "date_trunc") {
		sql = rewriteTimeBucket(sql)
		sql = rewriteDateTrunc(sql)
		sqlLower = strings.ToLower(sql)
	}

	// Use parallel-aware conversion
	convertedSQL, parallelInfo := h.convertSingleTableQueryForParallel(sql, sqlLower, headerDB)
	return convertedSQL, parallelInfo, false
}

// convertSQLToStoragePaths converts table references to storage paths
// Converts: FROM database.measurement -> FROM read_parquet('path/**/*.parquet')
// Converts: FROM measurement -> FROM read_parquet('path/**/*.parquet')
// Converts: JOIN database.measurement -> JOIN read_parquet('path/**/*.parquet')
// Converts: JOIN measurement -> JOIN read_parquet('path/**/*.parquet')
// CTE names are extracted and excluded from conversion to avoid replacing virtual table references.
// String literals and comments are protected from regex matching.
func (h *QueryHandler) convertSQLToStoragePaths(sql string) string {
	originalSQL := sql

	// Phase 0a: Rewrite regex functions to faster string functions BEFORE masking
	// This rewrites patterns like REGEXP_REPLACE(col, 'url_pattern', '\1') to CASE expressions
	// Must happen before masking since the regex patterns contain string literals
	sql, _ = RewriteRegexToStringFuncs(sql)

	// Phase 0b: Rewrite time functions to faster epoch-based alternatives BEFORE masking
	// This must happen first because these functions contain string literals
	// that would be masked, preventing our regex from matching
	sql = rewriteTimeBucket(sql)
	sql = rewriteDateTrunc(sql)

	// Phase 0c: Optimize LIKE patterns by reordering WHERE clause predicates
	// Moves cheap operations (empty string checks) before expensive operations (LIKE scans)
	// This allows DuckDB to short-circuit rows early, reducing LIKE evaluations
	sql, _ = OptimizeLikePatterns(sql)

	// Single pass to detect features (replaces 3 separate strings.Contains calls)
	features := scanSQLFeatures(sql)

	// Phase 1: Mask string literals to prevent regex from matching inside them
	// e.g., WHERE msg = 'SELECT * FROM mydb.cpu' should not convert the string content
	sql, masks := sqlutil.MaskStringLiterals(sql, features.hasQuotes)

	// Phase 2: Strip SQL comments (after masking to preserve comments inside strings)
	// e.g., "-- FROM mydb.cpu" should not be converted
	sql = stripSQLComments(sql, features.hasDashComment || features.hasBlockComment)

	// Extract CTE names to avoid converting them to storage paths
	cteNames := extractCTENames(sql)

	// Handle FROM database.table references
	sql = patternDBTable.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternDBTable.FindStringSubmatch(match)
		if len(parts) < 3 {
			return match
		}
		path := h.getStoragePath(parts[1], parts[2])
		return h.buildReadParquetExpr(path, originalSQL, "FROM")
	})

	// Handle JOIN database.table references (includes LATERAL JOIN)
	sql = patternJoinDBTable.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternJoinDBTable.FindStringSubmatch(match)
		if len(parts) < 3 {
			return match
		}
		path := h.getStoragePath(parts[1], parts[2])
		return h.buildReadParquetExpr(path, originalSQL, "JOIN")
	})

	// Handle FROM simple_table references
	sql = patternSimpleTable.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternSimpleTable.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		table := strings.ToLower(parts[1])

		// Skip if this is a CTE name - CTEs are virtual tables, not physical storage
		if cteNames[table] {
			return match
		}

		// Skip already converted read_parquet, system tables, etc.
		if shouldSkipTableConversion(table) {
			return match
		}

		// Check if followed by a dot (database.table already handled) or parenthesis (function call)
		idx := strings.Index(strings.ToLower(sql), strings.ToLower(match))
		if idx >= 0 {
			afterMatch := sql[idx+len(match):]
			afterMatch = strings.TrimLeft(afterMatch, " \t")
			if len(afterMatch) > 0 && (afterMatch[0] == '.' || afterMatch[0] == '(') {
				return match
			}
		}

		path := h.getStoragePath("default", parts[1])
		return h.buildReadParquetExpr(path, originalSQL, "FROM")
	})

	// Handle JOIN simple_table references (includes LATERAL JOIN)
	sql = patternJoinSimpleTable.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternJoinSimpleTable.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		table := strings.ToLower(parts[1])

		// Skip if this is a CTE name - CTEs are virtual tables, not physical storage
		if cteNames[table] {
			return match
		}

		// Skip already converted read_parquet, system tables, etc.
		if shouldSkipTableConversion(table) {
			return match
		}

		// Check if followed by a dot or parenthesis
		idx := strings.Index(strings.ToLower(sql), strings.ToLower(match))
		if idx >= 0 {
			afterMatch := sql[idx+len(match):]
			afterMatch = strings.TrimLeft(afterMatch, " \t")
			if len(afterMatch) > 0 && (afterMatch[0] == '.' || afterMatch[0] == '(') {
				return match
			}
		}

		path := h.getStoragePath("default", parts[1])
		return h.buildReadParquetExpr(path, originalSQL, "JOIN")
	})

	// Restore original string literals
	sql = sqlutil.UnmaskStringLiterals(sql, masks)

	return sql
}

// getStoragePath returns the storage path for a database.table
func (h *QueryHandler) getStoragePath(database, table string) string {
	return storage.GetStoragePath(h.storage, database, table)
}

// buildReadParquetOptions builds the read_parquet options string.
// Returns options like "union_by_name=true"
// Note: column pruning via 'columns' parameter is not supported in current DuckDB version.
// DuckDB handles projection pushdown internally when it sees which columns are actually used.
func buildReadParquetOptions() string {
	return "union_by_name=true"
}

// ParallelQueryInfo contains information for parallel partition execution.
// When set, the query should be executed using the parallel executor.
type ParallelQueryInfo struct {
	// Paths contains the partition paths to execute in parallel
	Paths []string
	// QueryTemplate is the SQL with {PARTITION_PATH} placeholder
	QueryTemplate string
	// ReadParquetOptions are the options to pass to read_parquet
	ReadParquetOptions string
}

// buildReadParquetExpr builds a read_parquet expression with optional partition pruning.
// keyword is "FROM" or "JOIN" to prepend to the result.
// If tiering is enabled and cold tier has data, builds a UNION ALL query across tiers.
func (h *QueryHandler) buildReadParquetExpr(path, originalSQL, keyword string) string {
	// Check if tiering is enabled and cold tier is configured
	if h.tieringManager != nil {
		router := h.tieringManager.GetRouter()
		if router != nil {
			// Extract database/measurement from the path
			database, measurement := h.extractDBMeasurementFromPath(path)
			if database != "" && measurement != "" {
				// Get glob paths for both tiers
				tieredPaths := router.GetGlobPathsForQuery(database, measurement)

				// If cold tier is configured and enabled, build multi-tier query
				if _, hasCold := tieredPaths[tiering.TierCold]; hasCold {
					h.logger.Debug().
						Str("database", database).
						Str("measurement", measurement).
						Msg("Tiering enabled: building multi-tier query")
					return h.buildMultiTierReadParquet(database, measurement, tieredPaths, keyword)
				}
			}
		}
	}

	// Fall back to single-tier behavior (original logic)
	options := buildReadParquetOptions()

	// Apply partition pruning
	optimizedPath, wasOptimized := h.pruner.OptimizeTablePath(path, originalSQL)

	if wasOptimized {
		// Check if it's a list of paths or a single path
		if pathList, ok := optimizedPath.([]string); ok {
			// Multiple paths - use DuckDB array syntax
			var pathsStr strings.Builder
			pathsStr.WriteString("[")
			for i, p := range pathList {
				if i > 0 {
					pathsStr.WriteString(", ")
				}
				pathsStr.WriteString("'")
				pathsStr.WriteString(p)
				pathsStr.WriteString("'")
			}
			pathsStr.WriteString("]")
			h.logger.Info().Int("partition_count", len(pathList)).Str("keyword", keyword).Msg("Partition pruning: Using targeted paths")
			return keyword + " read_parquet(" + pathsStr.String() + ", " + options + ")"
		} else if pathStr, ok := optimizedPath.(string); ok {
			h.logger.Info().Str("optimized_path", pathStr).Str("keyword", keyword).Msg("Partition pruning: Using optimized path")
			return keyword + " read_parquet('" + pathStr + "', " + options + ")"
		}
	}

	return keyword + " read_parquet('" + path + "', " + options + ")"
}

// buildReadParquetExprForMeasurement builds a read_parquet expression for a database/measurement pair.
// This is the tiering-aware version used by the fast path that takes database and measurement
// separately instead of a pre-constructed path, allowing proper tiering metadata lookup.
func (h *QueryHandler) buildReadParquetExprForMeasurement(database, measurement, originalSQL, keyword string) string {
	// Check if tiering is enabled and cold tier is configured
	if h.tieringManager != nil {
		router := h.tieringManager.GetRouter()
		if router != nil {
			// Get glob paths for both tiers
			tieredPaths := router.GetGlobPathsForQuery(database, measurement)

			// If cold tier is configured and enabled, build multi-tier query
			if _, hasCold := tieredPaths[tiering.TierCold]; hasCold {
				h.logger.Debug().
					Str("database", database).
					Str("measurement", measurement).
					Msg("Tiering enabled: building multi-tier query (fast path)")
				return h.buildMultiTierReadParquet(database, measurement, tieredPaths, keyword)
			}
		}
	}

	// Fall back to single-tier behavior (hot tier only)
	path := h.getStoragePath(database, measurement)
	return h.buildReadParquetExpr(path, originalSQL, keyword)
}

// buildReadParquetExprForParallel builds a read_parquet expression and returns
// parallel execution info if the query can benefit from parallel partition scanning.
// Returns (sql_expression, parallel_info) where parallel_info is non-nil if parallel is recommended.
func (h *QueryHandler) buildReadParquetExprForParallel(path, originalSQL, keyword string) (string, *ParallelQueryInfo) {
	options := buildReadParquetOptions()

	// Apply partition pruning
	optimizedPath, wasOptimized := h.pruner.OptimizeTablePath(path, originalSQL)

	if wasOptimized {
		if pathList, ok := optimizedPath.([]string); ok {
			// Check if parallel execution is recommended
			if h.parallelExecutor != nil && h.parallelExecutor.ShouldUseParallel(len(pathList)) {
				h.logger.Info().
					Int("partition_count", len(pathList)).
					Str("keyword", keyword).
					Msg("Partition pruning: Using parallel execution")

				// Return placeholder for template and parallel info
				return keyword + " {PARTITION_PATH}", &ParallelQueryInfo{
					Paths:              pathList,
					ReadParquetOptions: options,
				}
			}

			// Fall back to standard array syntax if parallel not recommended
			var pathsStr strings.Builder
			pathsStr.WriteString("[")
			for i, p := range pathList {
				if i > 0 {
					pathsStr.WriteString(", ")
				}
				pathsStr.WriteString("'")
				pathsStr.WriteString(p)
				pathsStr.WriteString("'")
			}
			pathsStr.WriteString("]")
			h.logger.Info().Int("partition_count", len(pathList)).Str("keyword", keyword).Msg("Partition pruning: Using targeted paths")
			return keyword + " read_parquet(" + pathsStr.String() + ", " + options + ")", nil
		} else if pathStr, ok := optimizedPath.(string); ok {
			h.logger.Info().Str("optimized_path", pathStr).Str("keyword", keyword).Msg("Partition pruning: Using optimized path")
			return keyword + " read_parquet('" + pathStr + "', " + options + ")", nil
		}
	}

	return keyword + " read_parquet('" + path + "', " + options + ")", nil
}

// shouldSkipTableConversion returns true if the table name should not be converted to a storage path
func shouldSkipTableConversion(table string) bool {
	for _, prefix := range skipPrefixes {
		if strings.HasPrefix(table, prefix) {
			return true
		}
	}
	return false
}

// extractDBMeasurementFromPath extracts database and measurement from a storage path.
// Path format: /some/base/path/{database}/{measurement}/**/*.parquet
// or: s3://bucket/{database}/{measurement}/**/*.parquet
// or: {database}/{measurement}/**/*.parquet (relative path)
// The key insight: database/measurement are always followed by year directories (4-digit numbers)
func (h *QueryHandler) extractDBMeasurementFromPath(path string) (database, measurement string) {
	// Normalize path separators
	path = strings.ReplaceAll(path, "\\", "/")

	// Remove any s3:// or azure:// prefix and bucket name
	if strings.Contains(path, "://") {
		parts := strings.SplitN(path, "://", 2)
		if len(parts) == 2 {
			// Remove bucket/container name
			path = parts[1]
			if idx := strings.Index(path, "/"); idx >= 0 {
				path = path[idx+1:]
			}
		}
	}

	// Remove glob pattern suffix (**/*.parquet)
	if idx := strings.Index(path, "**"); idx > 0 {
		path = path[:idx]
	}
	path = strings.TrimSuffix(path, "/")

	parts := strings.Split(path, "/")

	// Find database/measurement by looking for the pattern where:
	// - database is a non-numeric directory name
	// - measurement is a non-numeric directory name
	// - followed by year (4-digit number like 2024, 2025, 2026)
	// Scan from the end to find the measurement (just before the year)
	for i := len(parts) - 1; i >= 2; i-- {
		// Check if this part looks like a year (4 digits starting with 20)
		if len(parts[i]) == 4 && strings.HasPrefix(parts[i], "20") {
			if _, err := strconv.Atoi(parts[i]); err == nil {
				// parts[i] is the year, parts[i-1] is measurement, parts[i-2] is database
				if i >= 2 {
					return parts[i-2], parts[i-1]
				}
			}
		}
	}

	// Fallback: if path doesn't have year structure, take last two non-empty parts
	// This handles paths like: production/cpu/**/*.parquet
	nonEmpty := make([]string, 0)
	for _, p := range parts {
		if p != "" && p != "**" && !strings.Contains(p, "*") {
			nonEmpty = append(nonEmpty, p)
		}
	}
	if len(nonEmpty) >= 2 {
		return nonEmpty[len(nonEmpty)-2], nonEmpty[len(nonEmpty)-1]
	}

	return "", ""
}

// buildMultiTierReadParquet builds a read_parquet expression that queries tiers with actual data.
// Queries the tiering metadata to determine which tiers have files for this database/measurement,
// then only includes paths for tiers that actually have data.
func (h *QueryHandler) buildMultiTierReadParquet(database, measurement string, tieredPaths map[tiering.Tier]string, keyword string) string {
	options := buildReadParquetOptions()

	// Query metadata to find which tiers actually have data for this measurement
	ctx := context.Background()
	actualTiers, err := h.tieringManager.GetMetadata().GetTiersForMeasurement(ctx, database, measurement)
	if err != nil {
		h.logger.Warn().Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Msg("Failed to query tier metadata, falling back to hot tier only")
		// Fall back to hot tier only on error
		return keyword + fmt.Sprintf(" read_parquet('%s', %s)", h.getStoragePath(database, measurement), options)
	}

	// If no metadata found, fall back to hot tier (data might not be registered yet)
	if len(actualTiers) == 0 {
		h.logger.Debug().
			Str("database", database).
			Str("measurement", measurement).
			Msg("No tier metadata found, using hot tier")
		return keyword + fmt.Sprintf(" read_parquet('%s', %s)", h.getStoragePath(database, measurement), options)
	}

	// Collect paths only for tiers that actually have data
	var paths []string

	// Hot tier (local) - only if metadata says there's hot data
	if actualTiers[tiering.TierHot] {
		if _, ok := tieredPaths[tiering.TierHot]; ok {
			fullHotPath := h.getStoragePath(database, measurement)
			paths = append(paths, fullHotPath)
		}
	}

	// Cold tier (S3/Azure) - only if metadata says there's cold data
	if actualTiers[tiering.TierCold] {
		if _, ok := tieredPaths[tiering.TierCold]; ok {
			coldBackend := h.tieringManager.GetBackendForTier(tiering.TierCold)
			if coldBackend != nil {
				coldPath := storage.GetStoragePath(coldBackend, database, measurement)
				paths = append(paths, coldPath)
			}
		}
	}

	if len(paths) == 0 {
		// No paths found, return empty result
		h.logger.Warn().
			Str("database", database).
			Str("measurement", measurement).
			Msg("No tier paths found despite having metadata")
		return keyword + " (SELECT * WHERE 1=0)"
	}

	if len(paths) == 1 {
		// Single tier - use standard read_parquet
		h.logger.Debug().
			Str("database", database).
			Str("measurement", measurement).
			Str("path", paths[0]).
			Msg("Single-tier query")
		return keyword + fmt.Sprintf(" read_parquet('%s', %s)", paths[0], options)
	}

	// Multiple tiers: use read_parquet with a list of paths
	h.logger.Info().
		Str("database", database).
		Str("measurement", measurement).
		Int("tier_count", len(paths)).
		Strs("paths", paths).
		Msg("Building multi-tier query")

	// Build path list: ['path1', 'path2']
	var pathList strings.Builder
	pathList.WriteString("[")
	for i, p := range paths {
		if i > 0 {
			pathList.WriteString(", ")
		}
		pathList.WriteString("'")
		pathList.WriteString(p)
		pathList.WriteString("'")
	}
	pathList.WriteString("]")

	return keyword + fmt.Sprintf(" read_parquet(%s, %s)", pathList.String(), options)
}

// convertSingleTableQuery is a fast path for simple single-table queries.
// It avoids regex entirely by using simple string manipulation.
func (h *QueryHandler) convertSingleTableQuery(sql, sqlLower, database string) string {
	// Find "FROM table" position
	idx := strings.Index(sqlLower, "from ")
	if idx < 0 {
		return sql
	}

	start := idx + 5
	// Skip whitespace after FROM
	for start < len(sql) && (sql[start] == ' ' || sql[start] == '\t' || sql[start] == '\n') {
		start++
	}

	// Find table name end
	end := start
	for end < len(sql) && isIdentChar(sql[end]) {
		end++
	}

	if end == start {
		return sql // No table found, return original
	}

	tableName := sql[start:end]
	tableLower := strings.ToLower(tableName)

	// Skip system tables
	if shouldSkipTableConversion(tableLower) {
		return sql
	}

	// Build replacement - use tiering-aware method that checks both hot and cold tiers
	replacement := h.buildReadParquetExprForMeasurement(database, tableName, sql, "FROM")

	return sql[:idx] + replacement + sql[end:]
}

// convertSingleTableQueryForParallel is a variant that returns parallel execution info.
// Returns (converted_sql, parallel_info) where parallel_info is non-nil if parallel execution is recommended.
func (h *QueryHandler) convertSingleTableQueryForParallel(sql, sqlLower, database string) (string, *ParallelQueryInfo) {
	// Find "FROM table" position
	idx := strings.Index(sqlLower, "from ")
	if idx < 0 {
		return sql, nil
	}

	start := idx + 5
	// Skip whitespace after FROM
	for start < len(sql) && (sql[start] == ' ' || sql[start] == '\t' || sql[start] == '\n') {
		start++
	}

	// Find table name end
	end := start
	for end < len(sql) && isIdentChar(sql[end]) {
		end++
	}

	if end == start {
		return sql, nil // No table found, return original
	}

	tableName := sql[start:end]
	tableLower := strings.ToLower(tableName)

	// Skip system tables
	if shouldSkipTableConversion(tableLower) {
		return sql, nil
	}

	// Check tiering first - if cold tier exists, use tiering-aware method (no parallel for multi-tier)
	if h.tieringManager != nil {
		router := h.tieringManager.GetRouter()
		if router != nil {
			tieredPaths := router.GetGlobPathsForQuery(database, tableName)
			if _, hasCold := tieredPaths[tiering.TierCold]; hasCold {
				// Use tiering-aware method - parallel not supported for multi-tier queries
				replacement := h.buildMultiTierReadParquet(database, tableName, tieredPaths, "FROM")
				return sql[:idx] + replacement + sql[end:], nil
			}
		}
	}

	// Build replacement with parallel info (hot tier only)
	path := h.getStoragePath(database, tableName)
	replacement, parallelInfo := h.buildReadParquetExprForParallel(path, sql, "FROM")

	convertedSQL := sql[:idx] + replacement + sql[end:]

	// Store the template in parallel info if parallel execution is needed
	if parallelInfo != nil {
		parallelInfo.QueryTemplate = convertedSQL
	}

	return convertedSQL, parallelInfo
}

// convertSQLToStoragePathsWithHeaderDB converts table references to storage paths using
// the database specified in the x-arc-database header. This is an optimized path that
// skips the database.table regex patterns since all tables use the header-specified database.
// This provides ~50% reduction in regex operations compared to convertSQLToStoragePaths.
func (h *QueryHandler) convertSQLToStoragePathsWithHeaderDB(sql string, database string) string {
	originalSQL := sql
	sqlLower := strings.ToLower(sql)

	// Phase 0a: Rewrite regex functions to faster string functions BEFORE any other processing
	sql, _ = RewriteRegexToStringFuncs(sql)
	if sql != originalSQL {
		sqlLower = strings.ToLower(sql)
	}

	// FAST PATH: For simple single-table queries without special SQL features,
	// skip all the regex machinery and use direct string manipulation
	if isSingleTableQuery(sqlLower) && !strings.Contains(sqlLower, "with ") {
		features := scanSQLFeatures(sql)
		if !features.hasQuotes && !features.hasDashComment && !features.hasBlockComment {
			// Also need to rewrite time functions if present
			if strings.Contains(sqlLower, "time_bucket") || strings.Contains(sqlLower, "date_trunc") {
				sql = rewriteTimeBucket(sql)
				sql = rewriteDateTrunc(sql)
				sqlLower = strings.ToLower(sql)
			}
			return h.convertSingleTableQuery(sql, sqlLower, database)
		}
	}

	// Phase 0b: Rewrite time functions to faster epoch-based alternatives BEFORE masking
	sql = rewriteTimeBucket(sql)
	sql = rewriteDateTrunc(sql)

	// Phase 0c: Optimize LIKE patterns by reordering WHERE clause predicates
	sql, _ = OptimizeLikePatterns(sql)

	// Single pass to detect features
	features := scanSQLFeatures(sql)

	// Phase 1: Mask string literals to prevent regex from matching inside them
	sql, masks := sqlutil.MaskStringLiterals(sql, features.hasQuotes)

	// Phase 2: Strip SQL comments
	sql = stripSQLComments(sql, features.hasDashComment || features.hasBlockComment)

	// Extract CTE names only if query has WITH clause (fast path for majority of queries)
	var cteNames map[string]bool
	if strings.Contains(strings.ToLower(sql), "with ") {
		cteNames = extractCTENames(sql)
	}

	// OPTIMIZATION: Skip patternDBTable and patternJoinDBTable entirely
	// since we know all tables use the header-specified database

	// Handle FROM simple_table references - apply header database
	sql = patternSimpleTable.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternSimpleTable.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		table := strings.ToLower(parts[1])

		// Skip if this is a CTE name
		if cteNames[table] {
			return match
		}

		// Skip already converted read_parquet, system tables, etc.
		if shouldSkipTableConversion(table) {
			return match
		}

		// Check if followed by a dot (function call like db.func()) or parenthesis
		idx := strings.Index(strings.ToLower(sql), strings.ToLower(match))
		if idx >= 0 {
			afterMatch := sql[idx+len(match):]
			afterMatch = strings.TrimLeft(afterMatch, " \t")
			if len(afterMatch) > 0 && (afterMatch[0] == '.' || afterMatch[0] == '(') {
				return match
			}
		}

		// Use header database instead of "default"
		path := h.getStoragePath(database, parts[1])
		return h.buildReadParquetExpr(path, originalSQL, "FROM")
	})

	// Handle JOIN simple_table references - apply header database
	sql = patternJoinSimpleTable.ReplaceAllStringFunc(sql, func(match string) string {
		parts := patternJoinSimpleTable.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		table := strings.ToLower(parts[1])

		// Skip if this is a CTE name
		if cteNames[table] {
			return match
		}

		// Skip already converted read_parquet, system tables, etc.
		if shouldSkipTableConversion(table) {
			return match
		}

		// Check if followed by a dot or parenthesis
		idx := strings.Index(strings.ToLower(sql), strings.ToLower(match))
		if idx >= 0 {
			afterMatch := sql[idx+len(match):]
			afterMatch = strings.TrimLeft(afterMatch, " \t")
			if len(afterMatch) > 0 && (afterMatch[0] == '.' || afterMatch[0] == '(') {
				return match
			}
		}

		// Use header database instead of "default"
		path := h.getStoragePath(database, parts[1])
		return h.buildReadParquetExpr(path, originalSQL, "JOIN")
	})

	// Restore original string literals
	sql = sqlutil.UnmaskStringLiterals(sql, masks)

	return sql
}

// convertValue converts database values to JSON-serializable types.
// Type cases are ordered by frequency for time-series workloads:
// 1. Numeric types (float64, int64) - metrics values
// 2. string - tags, labels
// 3. time.Time - timestamps
// 4. sql.Null* types - sparse data
// 5. []byte - binary data (rare)
func (h *QueryHandler) convertValue(v interface{}) interface{} {
	// Fast path for nil (very common in sparse data)
	if v == nil {
		return nil
	}

	// Type switch ordered by frequency for time-series workloads
	switch val := v.(type) {
	// Most common: numeric types are already JSON-serializable
	case float64:
		return val
	case int64:
		return val
	case float32:
		return val
	case int32:
		return val
	case int:
		return val
	case uint64:
		return val
	case uint32:
		return val
	case uint:
		return val
	// Second most common: strings
	case string:
		return val
	// Timestamps need formatting - always normalize to UTC for consistency
	// Data is stored in UTC, so ensure output is always UTC regardless of server timezone
	case time.Time:
		return val.UTC().Format(time.RFC3339Nano)
	// Nullable types for sparse data
	case sql.NullFloat64:
		if val.Valid {
			return val.Float64
		}
		return nil
	case sql.NullInt64:
		if val.Valid {
			return val.Int64
		}
		return nil
	case sql.NullString:
		if val.Valid {
			return val.String
		}
		return nil
	case sql.NullBool:
		if val.Valid {
			return val.Bool
		}
		return nil
	// Binary data (rare)
	case []byte:
		return string(val)
	// Default: already JSON-serializable (bool, etc.)
	default:
		return val
	}
}

// handleShowDatabases handles SHOW DATABASES command by scanning storage
func (h *QueryHandler) handleShowDatabases(c *fiber.Ctx, start time.Time) error {
	h.logger.Debug().Msg("Handling SHOW DATABASES")

	// Include tier column if tiering is enabled
	var columns []string
	hasTiering := h.tieringManager != nil
	if hasTiering {
		columns = []string{"database", "tier"}
	} else {
		columns = []string{"database"}
	}
	data := make([][]interface{}, 0)

	ctx := context.Background()

	// Use DirectoryLister interface if available, otherwise fall back to List
	var databases []string
	var err error

	if lister, ok := h.storage.(storage.DirectoryLister); ok {
		databases, err = lister.ListDirectories(ctx, "")
	} else {
		// Fall back to List and extract unique top-level directories
		files, listErr := h.storage.List(ctx, "")
		if listErr != nil {
			err = listErr
		} else {
			databases = h.extractTopLevelDirs(files)
		}
	}

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list databases")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           "Failed to read storage: " + err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Also get databases from tiering metadata (for cold-only databases)
	if h.tieringManager != nil {
		metadata := h.tieringManager.GetMetadata()
		if metadata != nil {
			coldDatabases, coldErr := metadata.GetAllDatabases(ctx)
			if coldErr != nil {
				h.logger.Warn().Err(coldErr).Msg("Failed to get databases from tiering metadata")
			} else {
				// Merge into a set to deduplicate
				dbSet := make(map[string]bool)
				for _, db := range databases {
					dbSet[db] = true
				}
				for _, db := range coldDatabases {
					dbSet[db] = true
				}
				// Convert back to slice
				databases = make([]string, 0, len(dbSet))
				for db := range dbSet {
					databases = append(databases, db)
				}
			}
		}
	}

	// Filter out hidden directories
	filtered := make([]string, 0)
	for _, db := range databases {
		if !strings.HasPrefix(db, ".") && !strings.HasPrefix(db, "_") {
			filtered = append(filtered, db)
		}
	}

	// Sort alphabetically
	sort.Strings(filtered)

	for _, db := range filtered {
		if hasTiering {
			// Get tier info for this database
			tierStr := "local" // Default for databases not in tiering metadata
			metadata := h.tieringManager.GetMetadata()
			if metadata != nil {
				tiers, err := metadata.GetTiersForDatabase(ctx, db)
				if err != nil {
					h.logger.Warn().Err(err).Str("database", db).Msg("Failed to get tiers for database")
				} else if len(tiers) > 0 {
					tierStr = strings.Join(tiers, ",")
				}
			}
			data = append(data, []interface{}{db, tierStr})
		} else {
			data = append(data, []interface{}{db})
		}
	}

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Int("database_count", len(filtered)).
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

	ctx := context.Background()

	// Use DirectoryLister interface if available, otherwise fall back to List
	var tables []string
	var err error

	prefix := database + "/"
	if lister, ok := h.storage.(storage.DirectoryLister); ok {
		tables, err = lister.ListDirectories(ctx, prefix)
	} else {
		// Fall back to List and extract table names
		files, listErr := h.storage.List(ctx, prefix)
		if listErr != nil {
			err = listErr
		} else {
			tables = h.extractTableNames(files, database)
		}
	}

	if err != nil {
		h.logger.Error().Err(err).Str("database", database).Msg("Failed to list tables")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           "Failed to read database: " + err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Also get tables from tiering metadata (for cold-only tables)
	if h.tieringManager != nil {
		metadata := h.tieringManager.GetMetadata()
		if metadata != nil {
			coldTables, coldErr := metadata.GetMeasurementsByDatabase(ctx, database)
			if coldErr != nil {
				h.logger.Warn().Err(coldErr).Str("database", database).Msg("Failed to get tables from tiering metadata")
			} else {
				// Merge into a set to deduplicate
				tableSet := make(map[string]bool)
				for _, t := range tables {
					tableSet[t] = true
				}
				for _, t := range coldTables {
					tableSet[t] = true
				}
				// Convert back to slice
				tables = make([]string, 0, len(tableSet))
				for t := range tableSet {
					tables = append(tables, t)
				}
			}
		}
	}

	// Filter out hidden tables
	filtered := make([]string, 0)
	for _, table := range tables {
		if !strings.HasPrefix(table, ".") && !strings.HasPrefix(table, "_") {
			filtered = append(filtered, table)
		}
	}

	// Sort alphabetically
	sort.Strings(filtered)

	for _, table := range filtered {
		// Get table stats - for S3 use prefix path, for local use filesystem path
		var tablePath string
		if basePath := h.getStorageBasePath(); basePath != "" {
			tablePath = filepath.Join(basePath, database, table)
		} else {
			tablePath = database + "/" + table + "/"
		}
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
		Int("table_count", len(filtered)).
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

// extractTableNames extracts unique table names from file paths within a database
func (h *QueryHandler) extractTableNames(files []string, database string) []string {
	seen := make(map[string]bool)
	var tables []string
	prefix := database + "/"

	for _, f := range files {
		if !strings.HasPrefix(f, prefix) {
			continue
		}
		// Remove database prefix and get table name
		remaining := strings.TrimPrefix(f, prefix)
		parts := strings.SplitN(remaining, "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			table := parts[0]
			if !seen[table] {
				seen[table] = true
				tables = append(tables, table)
			}
		}
	}

	return tables
}

// getStorageBasePath returns the base path for local storage (empty for cloud backends)
func (h *QueryHandler) getStorageBasePath() string {
	return storage.GetLocalBasePath(h.storage, nil, "", "")
}

// extractTopLevelDirs extracts unique top-level directory names from file paths
func (h *QueryHandler) extractTopLevelDirs(files []string) []string {
	seen := make(map[string]bool)
	var dirs []string

	for _, f := range files {
		parts := strings.SplitN(f, "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			dir := parts[0]
			if !seen[dir] {
				seen[dir] = true
				dirs = append(dirs, dir)
			}
		}
	}

	return dirs
}

// getTableStats returns file count and total size for a table
// Works with both local filesystem and S3 backends
func (h *QueryHandler) getTableStats(tablePath string) (int, int64) {
	var fileCount int
	var totalSize int64

	// For local backend, use filesystem walk
	if basePath := h.getStorageBasePath(); basePath != "" {
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

	// For S3 and other backends, use ObjectLister if available
	if lister, ok := h.storage.(storage.ObjectLister); ok {
		ctx := context.Background()
		objects, err := lister.ListObjects(ctx, tablePath)
		if err != nil {
			h.logger.Warn().Err(err).Str("path", tablePath).Msg("Failed to list objects for stats")
			return 0, 0
		}

		for _, obj := range objects {
			if strings.HasSuffix(strings.ToLower(obj.Path), ".parquet") {
				fileCount++
				totalSize += obj.Size
			}
		}
	}

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

	// Validate SQL (empty, max length, dangerous patterns)
	if err := ValidateSQLRequest(req.SQL); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
			Success:      false,
			Error:        err.Error(),
			WarningLevel: "error",
		})
	}

	// Extract x-arc-database header for optimized query path
	headerDB := c.Get("x-arc-database")

	// Convert SQL to storage paths (with caching)
	convertedSQL, _ := h.getTransformedSQL(req.SQL, headerDB)

	// Create a COUNT(*) version of the query
	countSQL := "SELECT COUNT(*) FROM (" + convertedSQL + ") AS t"

	h.logger.Debug().
		Str("original_sql", req.SQL).
		Str("count_sql", countSQL).
		Msg("Estimating query")

	m := metrics.Get()

	// Create context with timeout if configured
	ctx := c.UserContext()
	var cancel context.CancelFunc
	if h.queryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, h.queryTimeout)
		defer cancel()
	}

	// Execute count query with timeout support
	rows, err := h.db.QueryContext(ctx, countSQL)
	if err != nil {
		// Check if it was a timeout
		if h.queryTimeout > 0 && ctx.Err() == context.DeadlineExceeded {
			m.IncQueryTimeouts()
			h.logger.Error().Err(err).Str("sql", countSQL).Dur("timeout", h.queryTimeout).Msg("Estimate query timed out")
			return c.Status(fiber.StatusGatewayTimeout).JSON(EstimateResponse{
				Success:         false,
				Error:           "Query timed out",
				WarningLevel:    "error",
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			})
		}
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
	m := metrics.Get()
	m.IncQueryRequests()

	measurement := c.Params("measurement")
	if measurement == "" {
		m.IncQueryErrors()
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
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid database name: " + err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}
	if err := validateIdentifier(measurement); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid measurement name: " + err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Validate limit and offset as integers
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 0 || limit > 1000000 {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid limit: must be a positive integer up to 1000000",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}
	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid offset: must be a non-negative integer",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Validate ORDER BY clause
	if err := validateOrderByClause(orderBy); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
			Success:   false,
			Error:     "Invalid order_by: " + err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Validate WHERE clause if provided
	if where != "" {
		if err := validateWhereClauseQuery(where); err != nil {
			m.IncQueryErrors()
			return c.Status(fiber.StatusBadRequest).JSON(QueryResponse{
				Success:   false,
				Error:     "Invalid where clause: " + err.Error(),
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			})
		}
	}

	// Check RBAC permissions for this database/measurement
	if err := h.checkMeasurementPermission(c, database, measurement, "read"); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusForbidden).JSON(QueryResponse{
			Success:   false,
			Error:     err.Error(),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Build SQL query with validated parameters
	sql := fmt.Sprintf("SELECT * FROM %s.%s", database, measurement)
	if where != "" {
		sql += " WHERE " + where
	}
	sql += " ORDER BY " + orderBy
	sql += fmt.Sprintf(" LIMIT %d", limit)
	sql += fmt.Sprintf(" OFFSET %d", offset)

	// Convert SQL to storage paths (with caching)
	// Note: This endpoint builds its own db.measurement SQL, so no header optimization
	convertedSQL, _ := h.getTransformedSQL(sql, "")

	h.logger.Debug().
		Str("measurement", measurement).
		Str("database", database).
		Str("sql", convertedSQL).
		Msg("Querying measurement")

	// Execute query
	rows, err := h.db.Query(convertedSQL)
	if err != nil {
		m.IncQueryErrors()
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
		m.IncQueryErrors()
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

	// Record success metrics
	m.IncQuerySuccess()
	m.IncQueryRows(int64(rowCount))
	m.RecordQueryLatency(time.Since(start).Microseconds())

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

