package api

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	// The database name in `SHOW TABLES FROM <db>` may be quoted ("db", 'db', or
	// `db`). The capture excludes the quotes so the RBAC check and the listing
	// use the real database name. The surrounding quotes are matched
	// independently (RE2 has no backreferences) — that is acceptable here: the
	// goal is to RECOGNIZE the SHOW so it is RBAC-gated, and over-recognizing is
	// safe; only a SHOW that fails to match would slip past the gate. Not a raw
	// string literal because the pattern itself contains a backtick.
	showTablesPattern = regexp.MustCompile("(?i)^\\s*SHOW\\s+(?:TABLES|MEASUREMENTS)(?:\\s+FROM\\s+[\"'`]?([\\w.-]+)[\"'`]?)?\\s*;?\\s*$")
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
	// CTE name extraction. DuckDB allows `WITH foo AS (...)` and
	// `WITH foo(col1, col2) AS (...)` (parenthesized column lists);
	// both forms must be recognised so the table-resolver doesn't
	// treat the CTE name as a real measurement and either (a) try to
	// rewrite `FROM foo` to a non-existent storage path, or (b) leak
	// the CTE name through RBAC as if it were a real table. The
	// optional `(...)` between the name and AS is matched non-greedily
	// to avoid swallowing too much. See review/query-path-criticals C3.
	patternCTENames = regexp.MustCompile(`(?i)\bWITH\s+(?:RECURSIVE\s+)?(\w+)(?:\s*\([^)]*\))?\s+AS\s*\(|,\s*(\w+)(?:\s*\([^)]*\))?\s+AS\s*\(`)

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

// arrowJSONQueryFunc is set by query_arrow_json.go init() when compiled with duckdb_arrow tag.
// It executes a query via DuckDB's native Arrow API and streams the JSON response.
// Returns (rowCount, handled). If handled is false, the caller falls back to database/sql.
var arrowJSONQueryFunc func(h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc, convertedSQL string, profileMode bool, governanceMaxRows int, start time.Time, timestamp string, onComplete func(int), onFail func(string), onTimeout func()) (int, bool)

// arrowMsgPackQueryFunc is set by query_msgpack.go init() when compiled with duckdb_arrow tag.
// It executes a query via DuckDB's native Arrow API and streams a MessagePack response.
// Returns (rowCount, handled). If handled is false, the caller MUST treat the request as
// unsupported and return 501 — the msgpack endpoint has no database/sql fallback because
// the Arrow path is the entire reason for its existence.
var arrowMsgPackQueryFunc func(h *QueryHandler, c *fiber.Ctx, ctx context.Context, cancel context.CancelFunc, convertedSQL string, profileMode bool, governanceMaxRows int, start time.Time, timestamp string, onComplete func(int), onFail func(string), onTimeout func()) (int, bool)

// errClientDisconnected is wrapped into streamErr by the streaming query
// handlers when bufio.Writer.Write or Flush fails mid-stream — the canonical
// signal in fasthttp's streaming model that the underlying TCP connection
// has been closed by the client. Callers use errors.Is to disambiguate
// client-side disconnect (operational noise, log at Warn) from server-side
// stream failures (genuine bug, log at Error).
var errClientDisconnected = errors.New("client disconnected mid-stream")

// isClientError reports whether err originated from the client side — either
// the connection was closed mid-stream, the request's deadline elapsed, or
// the request context was cancelled. These are expected operational events
// and should be logged at Warn, not Error.
func isClientError(err error) bool {
	return errors.Is(err, errClientDisconnected) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled)
}

// streamErrEvent picks the right zerolog level for a stream-truncation
// error: Warn for client-side events, Error for server-side failures.
func (h *QueryHandler) streamErrEvent(err error) *zerolog.Event {
	if isClientError(err) {
		return h.logger.Warn()
	}
	return h.logger.Error()
}

// isIdentChar returns true if c is a valid SQL identifier character (a-z, A-Z, 0-9, _)
func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

// hasCrossDatabaseSyntax checks if SQL contains db.table patterns without using regex.
// This is faster than regex-based detection for simple pattern matching.
// Used to reject queries that use db.table syntax when x-arc-database header is set.
func hasCrossDatabaseSyntax(sql string) bool {
	// Normalise before scanning, matching checkQueryPermissions /
	// convertSQLToStoragePaths exactly:
	//   - mask string literals so a db.table inside a literal isn't scanned;
	//   - mask FROM inside function bodies so `EXTRACT(EPOCH FROM t.ts)` isn't
	//     misread as a cross-database `t.ts` reference (false 400 on a legit
	//     query whose `t` is just a table alias);
	//   - strip comments so a db.table hidden in a comment can't bypass the scan
	//     (e.g. `FROM /* x */ otherdb.cpu`).
	features := scanSQLFeatures(sql)
	sql, _ = sqlutil.MaskStringLiterals(sql, features.hasQuotes)
	sql, _ = sqlutil.MaskFromKeywordsInFunctionBodies(sql)
	sql = stripSQLComments(sql, features.hasDashComment || features.hasBlockComment)

	sqlLower := strings.ToLower(sql)

	// Check for "FROM identifier.identifier" or "JOIN identifier.identifier" patterns.
	// Search for the bare keyword and verify it's followed by whitespace (not part of
	// a longer identifier like "fromage"), then scan past any whitespace before
	// checking for the db.table dot pattern.
	for _, keyword := range []string{"from", "join"} {
		pos := 0
		for {
			idx := strings.Index(sqlLower[pos:], keyword)
			if idx < 0 {
				break
			}
			idx += pos + len(keyword)
			pos = idx

			// All indexing below uses sqlLower, since idx is derived from
			// strings.Index(sqlLower, ...). Indexing the original sql with this
			// offset would be incorrect (and could panic) when ToLower changes
			// the byte length on non-ASCII input. The patterns we match (ASCII
			// whitespace, identifier chars, '.') are unaffected by lowercasing.

			// Verify keyword boundary on both sides: the preceding char must not
			// be an identifier char (else this is a suffix like "select_from"),
			// and the next char must be whitespace or end-of-string (else this is
			// a prefix like "fromage"/"joiner").
			startOfKeyword := idx - len(keyword)
			if startOfKeyword > 0 && isIdentChar(sqlLower[startOfKeyword-1]) {
				continue
			}
			if idx < len(sqlLower) && !isWhitespace(sqlLower[idx]) {
				continue
			}

			// Skip whitespace after keyword (spaces, tabs, newlines)
			for idx < len(sqlLower) && isWhitespace(sqlLower[idx]) {
				idx++
			}

			// Find first identifier (database name)
			start := idx
			for idx < len(sqlLower) && isIdentChar(sqlLower[idx]) {
				idx++
			}
			if idx == start || idx >= len(sqlLower) {
				continue
			}

			// Check for dot followed by another identifier (table name)
			if sqlLower[idx] == '.' && idx+1 < len(sqlLower) && isIdentChar(sqlLower[idx+1]) {
				return true
			}
		}
	}
	return false
}

// isWhitespace returns true if b is a whitespace byte.
func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// isFunctionCallAt reports whether the identifier ending at index `pos` is a
// function call — i.e. the next non-whitespace byte is '('. SQL permits
// whitespace between a function name and its opening paren (`generate_series
// (1, 10)`), so a bare `sql[pos] == '('` check misses the spaced form and
// would extract the function name as a spurious table reference.
func isFunctionCallAt(sql string, pos int) bool {
	for pos < len(sql) && isWhitespace(sql[pos]) {
		pos++
	}
	return pos < len(sql) && sql[pos] == '('
}

// maskedQuoteForBacktick lets MaskStringLiterals (which only recognises ' and ")
// also protect backtick-quoted identifiers: we map ` to " before masking so a
// comment marker, semicolon, or keyword inside a backtick-quoted name does not
// leak into comment-stripping / statement-splitting / denylist scanning. The
// content inside is what matters for those scans; the exact quote character is
// irrelevant downstream (the SHOW regex's capture excludes quotes either way).
func backticksToDoubleQuotes(sql string) string {
	if !strings.ContainsRune(sql, '`') {
		return sql
	}
	return strings.ReplaceAll(sql, "`", "\"")
}

// normalizeSQLForShow prepares SQL for matching against the anchored SHOW
// patterns. It masks string literals BEFORE stripping comments, then unmasks —
// so a comment marker inside a quoted identifier (e.g. SHOW TABLES FROM
// "my--db" or `my--db`) is not mistaken for a real comment and truncated.
// Stripping comments on raw SQL first would turn that query into
// `SHOW TABLES FROM "my`, causing the gate to authorise database `my` while
// DuckDB executes against `my--db` — an RBAC bypass. Backticks are mapped to
// double quotes first so backtick-quoted names are masked too (MaskStringLiterals
// only knows ' and "). The captured database name excludes the surrounding
// quotes, so the quote-character swap does not change the resolved db.
func normalizeSQLForShow(sql string) string {
	sql = backticksToDoubleQuotes(sql)
	features := scanSQLFeatures(sql)
	masked, masks := sqlutil.MaskStringLiterals(sql, features.hasQuotes)
	stripped := stripSQLComments(masked, features.hasDashComment || features.hasBlockComment)
	return strings.TrimSpace(sqlutil.UnmaskStringLiterals(stripped, masks))
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
		return fmt.Sprintf("to_timestamp(%d + ((epoch(%s)::BIGINT - %d) // %d) * %d)",
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

		return fmt.Sprintf("to_timestamp((epoch(%s)::BIGINT // %d) * %d)", column, seconds, seconds)
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

		// Convert to epoch-based arithmetic: to_timestamp((epoch(col) // interval) * interval)
		return fmt.Sprintf("to_timestamp((epoch(%s)::BIGINT // %d) * %d)", column, seconds, seconds)
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
	hasQuotes       bool // single or double quotes
	hasDashComment  bool // -- comment
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
	db                 *database.DuckDB
	storage            storage.Backend
	pruner             *pruning.PartitionPruner
	queryCache         *database.QueryCache
	logger             zerolog.Logger
	authManager        AuthManager
	rbacManager        RBACChecker
	debugEnabled       bool // Cached check for debug logging to avoid repeated level checks
	parallelExecutor   *query.ParallelExecutor
	queryTimeout       time.Duration // Query timeout (0 = no timeout)
	slowQueryThreshold time.Duration // Slow query WARN threshold (0 = disabled)

	// Cluster routing support
	router *cluster.Router

	// Cluster catch-up gating (#392). nil/false in OSS / standalone — gate is
	// a no-op. The interface keeps the gate testable without a real
	// coordinator stack.
	coordinator        replicationReadiness
	queryGateOnCatchup bool

	// gate503LogLastNano rate-limits the "gate fired" Warn to at most one per
	// second under sustained 503 storms. Without sampling a high-RPS catch-up
	// would flood operator logs.
	gate503LogLastNano atomic.Int64
	// gate503Total counts every gated 503 since startup. Exposed via the
	// query handler's Stats endpoint and as a Prometheus counter so operators
	// can alert on a non-zero rate without inferring from HTTP error logs.
	gate503Total atomic.Int64

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

// dangerousSQLPattern is a single combined regex for all dangerous SQL
// operations. Using one pattern instead of N separate ones reduces regex
// matching to a single pass. Run AFTER comment-stripping and string-
// literal masking via ValidateSQLRequest — `DROP /* */ TABLE` would slip
// past a `\b...\b` regex without comment removal, and `SELECT 'DROP
// TABLE x'` would false-positive without literal masking.
//
// Beyond classic DDL/DML, the following RCE-class operations are
// blocked because the user-facing query API is read-only:
//   - ATTACH/DETACH: mount remote DBs / extensions; can be used to
//     pivot to attacker-controlled storage.
//   - COPY (... TO ...): write query result to an arbitrary
//     filesystem path. Direct exfiltration vector.
//   - EXPORT/IMPORT DATABASE: bulk-export the catalog or pull
//     attacker-controlled DDL/DML.
//   - PRAGMA: many DuckDB pragmas are state-changing (memory_limit,
//     extension_directory, profile_output, etc.).
//   - SET (any session var): includes enable_external_access,
//     extension_directory, custom credentials.
//   - LOAD/INSTALL: load extensions (e.g. shellfs, httpfs) which
//     introduce arbitrary RCE.
//   - CALL: invoke extension procedures.
//
// Arc's internal startup code (configureDatabase, configureS3Access,
// configureAzureAccess in internal/database/duckdb.go) issues these
// commands directly via db.Exec without going through ValidateSQLRequest,
// so the denylist does not block legitimate Arc operations. Only user-
// supplied SQL via the /api/v1/query* endpoints is gated.
// The pattern is split so each alternative is anchored to its own
// boundary. Original DDL/DML keywords keep their trailing `\b` (they
// end at an identifier word). The RCE-class additions are anchored at
// the START with `\b` and terminate naturally because they are
// followed by mandatory whitespace/parentheses/equals — using a
// trailing `\b` there fails for `COPY (SELECT...)` because `\s` →
// `(` is a non-word→non-word transition.
var dangerousSQLPattern = regexp.MustCompile(`(?i)(?:` +
	// Classic DDL/DML — keyword followed by another word.
	`\b(?:DROP\s+(?:TABLE|DATABASE|INDEX|VIEW)` +
	`|DELETE\s+FROM` +
	`|TRUNCATE\s+TABLE` +
	`|ALTER\s+TABLE` +
	`|CREATE\s+(?:TABLE|DATABASE|INDEX)` +
	`|INSERT\s+INTO` +
	`|UPDATE\s+\w+\s+SET)\b` +
	// RCE / file-system / extension-loading class. Anchored at start
	// with \b; the terminator is whatever the keyword's argument shape
	// requires (quote, paren, equals, or whitespace+identifier).
	`|\bATTACH\b` +
	`|\bDETACH\b` +
	`|\bCOPY\b` +
	`|\bEXPORT\s+DATABASE\b` +
	`|\bIMPORT\s+DATABASE\b` +
	`|\bPRAGMA\b` +
	// SET / RESET match the bare keyword (any session-state mutation
	// is forbidden in the read-only API). The previous "\s+\w+\s*=" form
	// was bypassable via DuckDB's `SET x TO 1`, `SET VARIABLE x = 1`,
	// and `RESET x` syntax — gemini round 1.
	`|\bSET\b` +
	`|\bRESET\b` +
	`|\bLOAD\b` +
	`|\bINSTALL\b` +
	// CALL matches the bare keyword. The previous "\s+\w+" form was
	// bypassable via `CALL(proc)` (no whitespace before paren) — gemini
	// round 1.
	`|\bCALL\b` +
	`)`)

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

// validateHeaderDatabase validates the value of the x-arc-database HTTP
// header before it is interpolated into storage paths. The header
// content lands in `read_parquet('<base>/<database>/.../*.parquet')`
// — without validation, an attacker controls the path and can inject
// SQL via single-quote breakout, control characters, or path
// traversal. Empty header is allowed (handlers fall back to default-
// database behaviour).
//
// SECURITY: Defense-in-depth — the read_parquet interpolation sites
// also escape via sql.EscapeStringLiteral, but rejecting bad headers
// at the entry point produces a clear 400 instead of an obscure
// downstream SQL error. See review/query-path-criticals C2.
func validateHeaderDatabase(name string) error {
	if name == "" {
		return nil // empty header is allowed
	}
	return validateIdentifier(name)
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
// slowQueryThresholdMs: threshold in milliseconds for slow query WARN logging (0 = disabled)
func NewQueryHandler(db *database.DuckDB, storage storage.Backend, logger zerolog.Logger, queryTimeoutSeconds int, slowQueryThresholdMs int) *QueryHandler {
	handlerLogger := logger.With().Str("component", "query-handler").Logger()
	pruner := pruning.NewPartitionPruner(logger)
	pruner.SetStorageBackend(storage) // Enable S3/Azure partition filtering

	var queryTimeout time.Duration
	if queryTimeoutSeconds > 0 {
		queryTimeout = time.Duration(queryTimeoutSeconds) * time.Second
		handlerLogger.Info().Int("timeout_seconds", queryTimeoutSeconds).Msg("Query timeout configured")
	}

	var slowQueryThreshold time.Duration
	if slowQueryThresholdMs > 0 {
		slowQueryThreshold = time.Duration(slowQueryThresholdMs) * time.Millisecond
		handlerLogger.Info().Int("threshold_ms", slowQueryThresholdMs).Msg("Slow query logging enabled")
	}

	return &QueryHandler{
		db:                 db,
		storage:            storage,
		pruner:             pruner,
		queryCache:         database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		logger:             handlerLogger,
		authManager:        nil,
		rbacManager:        nil,
		debugEnabled:       handlerLogger.GetLevel() <= zerolog.DebugLevel,
		parallelExecutor:   query.NewParallelExecutor(db.DB(), query.DefaultParallelConfig(), handlerLogger),
		queryTimeout:       queryTimeout,
		slowQueryThreshold: slowQueryThreshold,
	}
}

// logSlowQuery emits a WARN log and increments the slow query counter if
// the query duration exceeds the configured threshold.
func (h *QueryHandler) logSlowQuery(sql string, start time.Time, rowCount int, tokenName string) {
	if h.slowQueryThreshold <= 0 {
		return
	}
	elapsed := time.Since(start)
	if elapsed < h.slowQueryThreshold {
		return
	}
	metrics.Get().IncSlowQueries()
	maskedSQL, _ := sqlutil.MaskStringLiterals(sql, sqlutil.HasQuotes(sql))
	h.logger.Warn().
		Str("sql", maskedSQL).
		Float64("execution_time_ms", float64(elapsed.Milliseconds())).
		Int("row_count", rowCount).
		Str("token_name", tokenName).
		Msg("Slow query detected")
}

// getTokenName extracts the token name from the Fiber context, or returns
// empty string if auth is not configured or no token is present.
func getTokenName(c *fiber.Ctx) string {
	if ti := auth.GetTokenInfo(c); ti != nil {
		return ti.Name
	}
	return ""
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

// replicationReadiness is the subset of cluster.Coordinator the query gate
// consumes. Defining the interface here (rather than depending on the
// concrete *cluster.Coordinator) keeps the gate independently testable —
// query_gate_test.go injects a fake without spinning up a real coordinator.
// The concrete *cluster.Coordinator satisfies this interface implicitly.
type replicationReadiness interface {
	ReplicationReady() bool
	ReplicationCatchUpStatus() map[string]int64
}

// SetCluster wires the cluster coordinator and the query-gate-on-catchup
// behavior flag into the query handler. Consulted by checkReplicationReady
// to short-circuit queries with 503 while peer file replication is still
// draining (#392). Both arguments are optional — nil coordinator OR
// gate=false makes the gate a no-op.
//
// Initialization-only: must be called once during process startup before
// the HTTP server starts accepting requests. Order relative to
// RegisterRoutes does not matter (Fiber resolves field reads at request
// time, and request handling does not begin until server.Start). Runtime
// re-wiring is not supported — the fields are read lock-free per request.
//
// The explicit nil branch protects against the typed-nil interface trap:
// assigning a nil *cluster.Coordinator to an interface field yields a
// non-nil interface that panics on method call. The current caller in
// main.go is gated on clusterCoordinator != nil so the branch is dead
// today, but we keep it so a future caller passing nil literally can't
// break the gate.
func (h *QueryHandler) SetCluster(coordinator *cluster.Coordinator, queryGateOnCatchup bool) {
	if coordinator == nil {
		h.coordinator = nil
	} else {
		h.coordinator = coordinator
	}
	h.queryGateOnCatchup = queryGateOnCatchup
}

// gate503RetryAfterSeconds is the Retry-After header value returned with
// 503s from the catch-up gate. Conservative; load balancers honor this
// automatically and well-behaved clients back off before retrying.
const gate503RetryAfterSeconds = "5"

// gate503LogIntervalNanos rate-limits the "gate fired" Warn to one per
// second to prevent log floods during sustained catch-up.
const gate503LogIntervalNanos = int64(time.Second)

// checkReplicationReady is Fiber middleware that short-circuits user-facing
// read endpoints with 503 when peer file replication is still draining and
// the operator has opted into the gate via cluster.query_gate_on_catchup.
//
// Returns c.Next() (no-op pass-through) when:
//   - the gate is disabled (queryGateOnCatchup=false), OR
//   - the coordinator is nil (OSS / standalone), OR
//   - Coordinator.ReplicationReady() reports the puller is fully drained.
//
// Returns 503 with a structured body and Retry-After header otherwise. The
// body includes the puller's catch-up status so clients can implement
// bounded retry without scraping logs or polling /api/v1/cluster separately.
// Each gated 503 increments gate503Total and emits at most one sampled Warn
// per second so operators can alert on a non-zero rate. See #392.
func (h *QueryHandler) checkReplicationReady(c *fiber.Ctx) error {
	if !h.queryGateOnCatchup || h.coordinator == nil {
		return c.Next()
	}
	if h.coordinator.ReplicationReady() {
		return c.Next()
	}

	// Track + log the gate fire.
	total := h.gate503Total.Add(1)
	now := time.Now().UnixNano()
	last := h.gate503LogLastNano.Load()
	if now-last >= gate503LogIntervalNanos && h.gate503LogLastNano.CompareAndSwap(last, now) {
		h.logger.Warn().
			Int64("gate_503_total", total).
			Str("path", c.Path()).
			Msg("Query gate fired: peer replication still draining (sampled at 1Hz)")
	}

	body := fiber.Map{
		"success": false,
		"error":   "replication_catch_up_in_progress",
		"message": "Reader is still catching up on replicated files. Retry shortly or check /api/v1/cluster for catch-up progress.",
	}
	if status := h.coordinator.ReplicationCatchUpStatus(); status != nil {
		body["catchup_status"] = status
	}
	c.Set("Retry-After", gate503RetryAfterSeconds)
	return c.Status(fiber.StatusServiceUnavailable).JSON(body)
}

// QueryGate503Total returns the total number of times the catch-up gate has
// fired since process start. Zero when the gate is disabled or has never
// fired. Exposed for Prometheus / metrics dashboards to alert on a non-zero
// rate without inferring from HTTP error logs.
func (h *QueryHandler) QueryGate503Total() int64 {
	return h.gate503Total.Load()
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

// InvalidateCaches clears all internal caches (partition pruner and SQL transform cache).
// This should be called after compaction to prevent stale file references.
func (h *QueryHandler) InvalidateCaches() {
	h.pruner.InvalidateAllCaches()
	h.queryCache.Invalidate()
	h.logger.Info().Msg("Query caches invalidated after compaction")
}

// StartBackgroundWorkers spawns the long-lived goroutines the handler
// needs: today this is the partition pruner cache janitor, which
// sweeps expired entries from globCache + partitionCache so they don't
// accumulate over the process lifetime. (Both caches are TTL-only with
// no max-size cap and no read-side eviction — get() returns "expired"
// as a miss but leaves the stale entry in the map.)
//
// The workers stop when ctx is cancelled. The handler itself remains
// usable after that, and InvalidateCaches() (called post-compaction)
// continues to reset both maps, but expired entries are no longer
// swept on a schedule — they accumulate until the next compaction
// flushes everything or the process exits. Production callers should
// pass a context tied to process lifetime via the shutdown coordinator.
func (h *QueryHandler) StartBackgroundWorkers(ctx context.Context) {
	h.pruner.StartCleanup(ctx, 0) // 0 → DefaultCleanupInterval
}

// extractTableReferences extracts all database.measurement references from SQL
// Returns a slice of TableReference structs for permission checking
func extractTableReferences(sql string) []TableReference {
	var refs []TableReference
	seen := make(map[string]bool)

	// CTE names (WITH t AS (...)) are virtual, not real measurements. The query
	// transform (convertSQLToStoragePaths) skips them, so the permission check
	// must too — otherwise a query like `WITH t AS (...) SELECT * FROM t` would
	// demand a spurious default.t:read grant (false denial).
	cteNames := extractCTENames(sql)

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

			// Skip CTE names — they are virtual, not real measurements.
			if cteNames[table] {
				continue
			}

			// Check if this table name is followed by a dot (meaning it's a database name, not a table)
			endIdx := matchIdx[3]
			if endIdx < len(sql) && sql[endIdx] == '.' {
				// This is actually a database name in "database.table", skip it
				continue
			}

			// Skip table-valued function calls (e.g. generate_series(...),
			// read_csv(...)) — the name is followed by '(', not a real table.
			// SQL allows whitespace before the paren (`generate_series  (…)`),
			// and the query transform treats it as a function regardless, so the
			// extractor must skip whitespace too to stay in parity (else a false
			// default.<fn> ref → 403).
			if isFunctionCallAt(sql, endIdx) {
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

			// Skip CTE names — they are virtual, not real measurements.
			if cteNames[table] {
				continue
			}

			// Check if this table name is followed by a dot
			endIdx := matchIdx[3]
			if endIdx < len(sql) && sql[endIdx] == '.' {
				continue
			}

			// Skip table-valued function calls (name followed by '(', possibly
			// after whitespace) — see the simple-table loop above.
			if isFunctionCallAt(sql, endIdx) {
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

	// Normalise SQL before extracting table references. This MUST match the
	// normalisation that convertSQLToStoragePaths applies downstream, or the
	// permission check and the executed query disagree on which tables are
	// referenced:
	//   - mask string literals so keywords inside them don't false-positive;
	//   - mask FROM inside function bodies (e.g. EXTRACT(YEAR FROM time)) so the
	//     extractor doesn't treat the field as a default.<field> table ref —
	//     that would cause a false-positive denial;
	//   - strip comments so a comment interleaved between FROM/JOIN and the
	//     table name can't hide a reference (SECURITY: RBAC bypass — the query
	//     `SELECT * FROM /* x */ secret.cpu` would otherwise yield zero refs
	//     here yet still execute against secret.cpu after the transform).
	features := scanSQLFeatures(sql)
	normalisedSQL, _ := sqlutil.MaskStringLiterals(sql, features.hasQuotes)
	normalisedSQL, _ = sqlutil.MaskFromKeywordsInFunctionBodies(normalisedSQL)
	normalisedSQL = stripSQLComments(normalisedSQL, features.hasDashComment || features.hasBlockComment)

	// Extract table references from the normalised SQL
	tableRefs := extractTableReferences(normalisedSQL)
	if len(tableRefs) == 0 {
		// No tables referenced (e.g., SELECT 1+1)
		return nil
	}

	// Override "default" database with the x-arc-database header value when
	// present. Without this, a user with default.cpu:read can bypass RBAC
	// and query sensitive_db.cpu by setting x-arc-database: sensitive_db —
	// the permission check would use "default" while the query transform
	// resolves paths against the header-specified database.
	if headerDB := c.Get("x-arc-database"); headerDB != "" {
		for i := range tableRefs {
			if tableRefs[i].Database == "default" {
				tableRefs[i].Database = headerDB
			}
		}
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
	// User-facing read endpoints get the read-auth + catch-up gate (#392) as
	// route-level middleware. The read-auth middleware enforces that the token
	// has read-level permission (not just any valid token). The catch-up gate
	// is a no-op unless cluster.query_gate_on_catchup is true AND the
	// coordinator reports peer file replication is still drifting.
	//
	// The auth middleware is resolved lazily so it is independent of
	// whether SetAuthAndRBAC runs before or after RegisterRoutes. Uses
	// atomic.Value + double-checked locking (not sync.Once) because if
	// h.authManager is nil at init time (early request before
	// SetAuthAndRBAC), the middleware resolves as a passthrough for that
	// single request but is NOT permanently cached — the next request
	// retries and eventually caches the real middleware once the auth
	// manager is configured. sync.Once would permanently cache the
	// passthrough, creating a silent auth bypass.
	//
	// On the fast path (cached) this is a single atomic load — zero
	// measurable overhead.
	//
	// QueryHandler holds the AuthManager interface for testability; at
	// runtime it is always *auth.AuthManager. If the type assertion
	// fails (non-nil but wrong concrete type), fail closed rather than
	// silently disabling auth on all query routes.
	var (
		cachedReadAuth atomic.Value
		initMu         sync.Mutex
	)
	readAuth := func(c *fiber.Ctx) error {
		if handler, ok := cachedReadAuth.Load().(fiber.Handler); ok {
			return handler(c)
		}

		initMu.Lock()
		defer initMu.Unlock()

		// Double-check after acquiring lock.
		if handler, ok := cachedReadAuth.Load().(fiber.Handler); ok {
			return handler(c)
		}

		// authManager is nil before SetAuthAndRBAC is called (auth
		// disabled, or early-startup request). Return passthrough for
		// THIS request only — do NOT cache, so the next request retries.
		if h.authManager == nil {
			return c.Next()
		}

		var handler fiber.Handler
		// Tests may pass a mock implementing an optional RequireRead()
		// interface to provide their own middleware.
		if provider, ok := h.authManager.(interface{ RequireRead() fiber.Handler }); ok {
			handler = provider.RequireRead()
		} else if concrete, ok := h.authManager.(*auth.AuthManager); ok {
			handler = auth.RequireRead(concrete)
		} else {
			h.logger.Error().Msg("AuthManager type assertion failed — denying all query traffic (fail-closed)")
			handler = func(c *fiber.Ctx) error {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"success": false,
					"error":   "internal_auth_error",
				})
			}
		}

		cachedReadAuth.Store(handler)
		return handler(c)
	}
	app.Post("/api/v1/query", readAuth, h.checkReplicationReady, h.executeQuery)
	// Experimental: same execution pipeline as /api/v1/query, but the
	// response is streamed as MessagePack instead of JSON. Gated by the
	// duckdb_arrow build tag (no database/sql fallback — see the
	// wire-format dispatch in executeQuery).
	app.Post("/api/v1/query/msgpack", readAuth, h.checkReplicationReady, h.executeQueryMsgPack)
	app.Post("/api/v1/query/estimate", readAuth, h.checkReplicationReady, h.estimateQuery)
	app.Get("/api/v1/measurements", readAuth, h.checkReplicationReady, h.listMeasurements)
	app.Get("/api/v1/query/:measurement", readAuth, h.checkReplicationReady, h.queryMeasurement)
	h.registerArrowRoutes(app, readAuth)

	// The distributed cache-invalidate endpoint
	// (POST CacheInvalidatePath) is wired separately in cmd/arc/main.go,
	// conditionally on cluster.shared_secret being configured. It lives
	// in its own file (cache_invalidate.go) because its auth model is
	// HMAC-only, distinct from the user-token auth applied here.
}

// executeQueryMsgPack handles POST /api/v1/query/msgpack. It is a thin
// wrapper that sets the "wire_format" request-local to "msgpack" and
// delegates to executeQuery — every step (auth, RBAC, governance,
// forwarding, transform, timeout, registry, slow-query logging) is
// identical to the JSON path. The wire-format selector is consumed at
// the Arrow dispatch site to route the response stream through the
// msgpack encoder instead of JSON.
//
// Experimental. The endpoint may move or be removed based on benchmark
// results; clients should not hard-code against it for production
// traffic yet.
func (h *QueryHandler) executeQueryMsgPack(c *fiber.Ctx) error {
	c.Locals(wireFormatLocalsKey, wireFormatMsgPack)
	return h.executeQuery(c)
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
			return respondError(c, fiber.StatusInternalServerError, "Failed to prepare request for forwarding", timestamp, start)
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
				return respondError(c, fiber.StatusTooManyRequests, result.Reason, timestamp, start)
			}
			if result := h.governanceManager.CheckQuota(tokenInfo.ID); !result.Allowed {
				m.IncQueryErrors()
				metrics.Get().IncGovernanceQuotaExhausted()
				h.logger.Warn().
					Int64("token_id", tokenInfo.ID).
					Str("token_name", tokenInfo.Name).
					Str("reason", result.Reason).
					Msg("Query rejected: quota exhausted")
				return respondError(c, fiber.StatusTooManyRequests, result.Reason, timestamp, start)
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
		return respondError(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error(), timestamp, start)
	}

	// Validate SQL (empty, max length, dangerous patterns)
	if err := ValidateSQLRequest(req.SQL); err != nil {
		m.IncQueryErrors()
		return respondError(c, fiber.StatusBadRequest, err.Error(), timestamp, start)
	}

	// Extract x-arc-database header for optimized query path
	headerDB := c.Get("x-arc-database")
	if err := validateHeaderDatabase(headerDB); err != nil {
		m.IncQueryErrors()
		return respondError(c, fiber.StatusBadRequest, "invalid x-arc-database header: "+err.Error(), timestamp, start)
	}

	// If header is set, reject cross-database syntax (db.table not allowed)
	if headerDB != "" && hasCrossDatabaseSyntax(req.SQL) {
		m.IncQueryErrors()
		return respondError(c, fiber.StatusBadRequest, "Cross-database queries (db.table syntax) not allowed when x-arc-database header is set", timestamp, start)
	}

	// Normalise before matching SHOW: strip comments and trim whitespace so a
	// comment cannot hide a SHOW command from the anchored regex. Without this,
	// `/* x */ SHOW DATABASES` fails the raw match, falls through to
	// checkQueryPermissions (zero table refs → allowed), and DuckDB strips the
	// comment and executes it — returning the database/table list to a caller
	// the SHOW RBAC gate would have denied. SECURITY: RBAC bypass.
	showNormalised := normalizeSQLForShow(req.SQL)

	// Handle SHOW DATABASES command
	if showDatabasesPattern.MatchString(showNormalised) {
		// Check RBAC - user needs at least some read permission to see databases
		if err := h.checkMeasurementPermission(c, "*", "*", "read"); err != nil {
			m.IncQueryErrors()
			return respondError(c, fiber.StatusForbidden, "access denied: no read permission to list databases", timestamp, start)
		}
		return h.handleShowDatabases(c, start)
	}

	// Handle SHOW TABLES/MEASUREMENTS command
	if matches := showTablesPattern.FindStringSubmatch(showNormalised); matches != nil {
		// Resolve the target database the same way the command does: explicit
		// `FROM db` wins, else the x-arc-database header is the implicit target,
		// else "default". Checking a different database than the listing targets
		// would be an RBAC bypass (handleShowTables lists `database` below).
		database := "default"
		if len(matches) > 1 && matches[1] != "" {
			database = matches[1]
		} else if headerDB != "" {
			database = headerDB
		}
		// Validate the resolved database name before it reaches storage. The
		// SHOW regex permits a quoted/dotted token, so `SHOW TABLES FROM ..`
		// would otherwise traverse out of the storage root when RBAC is
		// disabled (handleShowTables lists `database + "/"`). validateIdentifier
		// rejects anything but alphanumeric/underscore/hyphen.
		if err := validateIdentifier(database); err != nil {
			m.IncQueryErrors()
			return respondError(c, fiber.StatusBadRequest, "invalid database name: "+err.Error(), timestamp, start)
		}
		// Check RBAC - user needs read permission on the specific database
		if err := h.checkMeasurementPermission(c, database, "*", "read"); err != nil {
			m.IncQueryErrors()
			return respondError(c, fiber.StatusForbidden, fmt.Sprintf("access denied: no read permission for database '%s'", database), timestamp, start)
		}
		return h.handleShowTables(c, start, database)
	}

	// Check RBAC permissions for all tables referenced in the query
	if err := h.checkQueryPermissions(c, req.SQL, "read"); err != nil {
		m.IncQueryErrors()
		return respondError(c, fiber.StatusForbidden, err.Error(), timestamp, start)
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
	// (msgpack endpoint deliberately bypasses the parallel-partition
	// executor: its response shape and per-partition merging is wired
	// for JSON streaming. The msgpack experiment routes only through
	// the standard Arrow dispatch below.)
	if parallelInfo != nil && h.parallelExecutor != nil && !isMsgPackWire(c) {
		// Parallel partition execution — use registry context if available
		execCtx := c.UserContext()
		if queryCtx != nil {
			execCtx = queryCtx
		}
		var cancelTimeout context.CancelFunc
		if effectiveTimeout > 0 {
			execCtx, cancelTimeout = context.WithTimeout(execCtx, effectiveTimeout)
			// Note: cancelTimeout is called inside the stream writer callback, not deferred here,
			// because SetBodyStreamWriter runs asynchronously after this function returns.
		}
		results, err := h.parallelExecutor.ExecutePartitioned(
			execCtx,
			parallelInfo.Paths,
			parallelInfo.QueryTemplate,
			parallelInfo.ReadParquetOptions,
		)
		if err != nil {
			if cancelTimeout != nil {
				cancelTimeout()
			}
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
				Error:           err.Error(),
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
		}

		// CRITICAL: per-partition error inspection. ExecutePartitioned
		// returns successfully even if individual partitions errored
		// (PartitionResult.Error != nil). NewMergedRowIterator below
		// would happily ignore errored partitions and return rows from
		// the surviving ones, producing a 200 with `success:true` and
		// silently dropping a fraction of the result. That is exactly
		// the silent-data-loss class CLAUDE.md prohibits — fail loudly
		// instead. See review/query-path-criticals C4.
		var erroredPaths []string
		var firstPartitionErr error
		for _, r := range results {
			if r.Error != nil {
				erroredPaths = append(erroredPaths, r.Path)
				if firstPartitionErr == nil {
					firstPartitionErr = r.Error
				}
			}
		}
		if len(erroredPaths) > 0 {
			// Close any rows that DID succeed before bailing.
			for _, r := range results {
				if r.Rows != nil {
					r.Rows.Close()
				}
			}
			if cancelTimeout != nil {
				cancelTimeout()
			}
			m.IncQueryErrors()
			if h.queryRegistry != nil && queryID != "" {
				if execCtx.Err() == context.DeadlineExceeded {
					h.queryRegistry.TimedOut(queryID)
				} else {
					h.queryRegistry.Fail(queryID, fmt.Sprintf("%d/%d partitions failed: %v",
						len(erroredPaths), len(results), firstPartitionErr))
				}
			}
			// Sample paths at log level — full list could be hundreds.
			samplePaths := erroredPaths
			if len(samplePaths) > 5 {
				samplePaths = samplePaths[:5]
			}
			h.logger.Error().
				Err(firstPartitionErr).
				Int("errored_partitions", len(erroredPaths)).
				Int("total_partitions", len(results)).
				Strs("sample_paths", samplePaths).
				Str("sql", req.SQL).
				Msg("Parallel query: partition error(s) — failing whole request to avoid silent partial result")
			return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
				Success: false,
				Error: fmt.Sprintf("parallel query: %d of %d partitions failed (first error: %v)",
					len(erroredPaths), len(results), firstPartitionErr),
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
			if cancelTimeout != nil {
				cancelTimeout()
			}
			m.IncQueryErrors()
			if h.queryRegistry != nil && queryID != "" {
				h.queryRegistry.Fail(queryID, "Failed to create merged iterator")
			}
			h.logger.Error().Err(err).Msg("Failed to create merged iterator")
			return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
				Success:         false,
				Error:           err.Error(),
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
		}

		columns = iter.Columns()

		// Get column types from first successful partition for typed JSON serialization
		var colTypes []colType
		for _, r := range results {
			if r.Error == nil && r.Rows != nil {
				if ct, err := r.Rows.ColumnTypes(); err == nil {
					colTypes = mapColumnTypes(ct)
					break
				}
			}
		}
		if colTypes == nil {
			// Fallback: treat all columns as strings
			colTypes = make([]colType, len(columns))
		}

		// Capture token name before async callback (Fiber context not safe in callbacks)
		tokenName := getTokenName(c)

		// Stream typed JSON response directly to HTTP — no full-response buffering.
		// streamCtx captures the timeout-aware context for per-row cancellation
		// inside the streaming callback. The callback runs async after this
		// handler returns; closing over execCtx is fine because cancelTimeout
		// is fired inside the callback after streaming completes.
		streamCtx := execCtx
		c.Set("Content-Type", "application/json")
		c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
			rowCount, streamErr := streamTypedJSON(streamCtx, w, columns, colTypes, iter, governanceMaxRows, profile, start, timestamp)
			w.Flush()

			iter.Close()
			if cancelTimeout != nil {
				cancelTimeout()
			}

			// Record metrics after streaming completes. If the stream
			// terminated mid-flight (Scan / Err / ctx cancel), record as
			// a failure even though the JSON envelope was already sent —
			// operators must see the partial-result signal. See C5.
			if streamErr != nil {
				m.IncQueryErrors()
				if h.queryRegistry != nil && queryID != "" {
					if errors.Is(streamErr, context.DeadlineExceeded) {
						h.queryRegistry.TimedOut(queryID)
					} else {
						h.queryRegistry.Fail(queryID, streamErr.Error())
					}
				}
				// Per-handler client-disconnect counter (#426).
				if isClientError(streamErr) {
					m.IncQueryClientDisconnect(metrics.DisconnectPathSQLJSON)
				}
				// Warn for client-disconnect / context expiry (headers already
				// committed, partial result was delivered). Error for genuine
				// server-side failures (scanner, db iteration).
				h.streamErrEvent(streamErr).Err(streamErr).
					Int("rows_sent", rowCount).
					Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
					Msg("Query stream truncated after headers committed; client received partial result")
				return
			}
			if h.queryRegistry != nil && queryID != "" {
				h.queryRegistry.Complete(queryID, rowCount)
			}
			m.IncQuerySuccess()
			m.IncQueryRows(int64(rowCount))
			m.RecordQueryLatency(time.Since(start).Microseconds())

			h.logger.Info().
				Int("row_count", rowCount).
				Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
				Msg("Query completed")
			h.logSlowQuery(convertedSQL, start, rowCount, tokenName)
		})
		return nil
	} else {
		// Standard single-query execution

		// Create context with timeout if configured (0 = no timeout)
		ctx := c.UserContext()
		if queryCtx != nil {
			ctx = queryCtx
		}
		var cancel context.CancelFunc
		if effectiveTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, effectiveTimeout)
			// Note: cancel is called inside the stream writer callback, not deferred here,
			// because SetBodyStreamWriter runs asynchronously after this function returns.
		}

		// Arrow-native path: bypasses database/sql row scanning entirely — reads typed
		// values directly from DuckDB's internal Arrow columnar chunks.
		//
		// Wire-format selector: the experimental msgpack endpoint
		// (POST /api/v1/query/msgpack) sets c.Locals("wire_format", "msgpack")
		// in its wrapper handler so this dispatch can route to the msgpack
		// streamer instead of JSON. Unknown / missing values default to JSON.
		// When the msgpack dispatch fails ("handled=false", e.g. driver
		// doesn't implement Arrow), we return 501 instead of falling back
		// to database/sql — the entire reason for the msgpack endpoint is
		// the typed Arrow encode, and a Scan-based fallback would silently
		// defeat that contract.
		arrowDispatch := arrowJSONQueryFunc
		isMsgPack := isMsgPackWire(c)
		if isMsgPack {
			arrowDispatch = arrowMsgPackQueryFunc
		}
		// When built without -tags=duckdb_arrow, arrowMsgPackQueryFunc
		// is nil; the JSON path would silently fall through to the
		// database/sql route, but a msgpack client must NOT receive
		// JSON. Short-circuit to 501 here so the wire-format contract
		// is preserved end-to-end.
		if isMsgPack && arrowDispatch == nil {
			if cancel != nil {
				cancel()
			}
			return respondError(c, fiber.StatusNotImplemented, "msgpack query path requires the duckdb_arrow build tag", timestamp, start)
		}
		if arrowDispatch != nil {
			var onComplete func(int)
			var onFail func(string)
			var onTimeout func()
			if h.queryRegistry != nil && queryID != "" {
				onComplete = func(rc int) { h.queryRegistry.Complete(queryID, rc) }
				onFail = func(msg string) { h.queryRegistry.Fail(queryID, msg) }
				onTimeout = func() { h.queryRegistry.TimedOut(queryID) }
			}
			_, handled := arrowDispatch(h, c, ctx, cancel, convertedSQL, profileMode, governanceMaxRows, start, timestamp, onComplete, onFail, onTimeout)
			if handled {
				// Arrow path handled the response — registry callbacks are invoked
				// inside executeArrowJSONQuery / executeArrowMsgPackQuery (either
				// directly for errors, or via the async stream writer callback
				// for success).
				return nil
			}
			if isMsgPack {
				// No database/sql fallback for the msgpack endpoint — return 501.
				if cancel != nil {
					cancel()
				}
				return respondError(c, fiber.StatusNotImplemented, "msgpack query path requires the duckdb_arrow build tag", timestamp, start)
			}
			// handled=false means Arrow path declined (e.g., driver issue).
			// Fall through to database/sql path.
		}

		var rows *sql.Rows
		var profileConn *sql.Conn // pinned connection for profiled queries — caller must close
		var err error

		if profileMode {
			// Use profiled query to capture timing breakdown (with timeout support)
			rows, profileConn, profile, err = h.db.QueryWithProfileContext(ctx, convertedSQL)
		} else {
			rows, err = h.db.QueryContext(ctx, convertedSQL)
		}

		if err != nil {
			if cancel != nil {
				cancel()
			}
			// Check if this is a "no files found" error — treat as empty result, not an error.
			// This happens when querying a measurement that has no data on storage yet
			// (e.g., new measurement, or DuckDB's httpfs cache is stale).
			if isNoFilesFoundError(err) {
				h.logger.Info().Str("sql", req.SQL).Msg("No files found for measurement, returning empty result")
				m.IncQuerySuccess()
				if h.queryRegistry != nil && queryID != "" {
					h.queryRegistry.Complete(queryID, 0)
				}
				return c.JSON(QueryResponse{
					Success:         true,
					Columns:         []string{},
					Data:            [][]interface{}{},
					RowCount:        0,
					ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
					Timestamp:       timestamp,
				})
			}

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
				Error:           err.Error(),
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
		}

		// Get column names
		columns, err = rows.Columns()
		if err != nil {
			rows.Close()
			if profileConn != nil {
				profileConn.Close()
			}
			if cancel != nil {
				cancel()
			}
			m.IncQueryErrors()
			h.logger.Error().Err(err).Msg("Failed to get column names")
			return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
				Success:         false,
				Error:           err.Error(),
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
		}

		// Get column types for typed JSON serialization
		columnTypes, err := rows.ColumnTypes()
		var colTypes []colType
		if err == nil {
			colTypes = mapColumnTypes(columnTypes)
		} else {
			// Fallback: treat all columns as strings
			colTypes = make([]colType, len(columns))
		}

		// Capture token name before async callback (Fiber context not safe in callbacks)
		tokenName := getTokenName(c)

		// Stream typed JSON response directly to HTTP — no full-response buffering.
		// streamCtx captures the timeout-aware context so per-row cancellation
		// can fire inside the streaming callback. See C5.
		streamCtx := ctx
		c.Set("Content-Type", "application/json")
		c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
			rowCount, streamErr := streamTypedJSON(streamCtx, w, columns, colTypes, rows, governanceMaxRows, profile, start, timestamp)
			w.Flush()

			rows.Close()
			if profileConn != nil {
				profileConn.Close()
			}
			if cancel != nil {
				cancel()
			}

			// If the stream terminated mid-flight (Scan / Err / ctx
			// cancel) record as failure even though the JSON envelope
			// was already sent — operators must see the partial-result
			// signal. See review/query-path-criticals C5.
			if streamErr != nil {
				m.IncQueryErrors()
				if h.queryRegistry != nil && queryID != "" {
					if errors.Is(streamErr, context.DeadlineExceeded) {
						h.queryRegistry.TimedOut(queryID)
					} else {
						h.queryRegistry.Fail(queryID, streamErr.Error())
					}
				}
				// Per-handler client-disconnect counter (#426).
				if isClientError(streamErr) {
					m.IncQueryClientDisconnect(metrics.DisconnectPathSQLJSON)
				}
				// Warn for client-disconnect / context expiry (headers already
				// committed, partial result was delivered). Error for genuine
				// server-side failures (scanner, db iteration).
				h.streamErrEvent(streamErr).Err(streamErr).
					Int("rows_sent", rowCount).
					Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
					Msg("Query stream truncated after headers committed; client received partial result")
				return
			}

			if h.queryRegistry != nil && queryID != "" {
				h.queryRegistry.Complete(queryID, rowCount)
			}
			m.IncQuerySuccess()
			m.IncQueryRows(int64(rowCount))
			m.RecordQueryLatency(time.Since(start).Microseconds())

			h.logger.Info().
				Int("row_count", rowCount).
				Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
				Msg("Query completed")
			h.logSlowQuery(convertedSQL, start, rowCount, tokenName)
		})
		return nil
	}
}

// isNoFilesFoundError checks if a DuckDB error is the "No files found" IO error.
// This occurs when read_parquet glob pattern matches zero files on S3/Azure/local storage.
// This is NOT a real error — it means the measurement has no data yet (or DuckDB's
// httpfs directory cache is stale). We treat it as an empty result.
func isNoFilesFoundError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "No files found that match the pattern")
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
//
// SECURITY: The denylist regex must run on a normalised version of the
// SQL — comments stripped (so `DROP /* */ TABLE x` does not bypass the
// `\b...\b` token boundary) and string literals masked (so `SELECT
// 'DROP TABLE x'` does not false-positive). The cost is one full pass
// over the SQL; for the 10KB cap that's microseconds.
func ValidateSQLRequest(sql string) error {
	if strings.TrimSpace(sql) == "" {
		return &SQLValidationError{Message: "SQL query is required"}
	}

	if len(sql) > 10000 {
		return &SQLValidationError{Message: "SQL query exceeds maximum length (10000 characters)"}
	}

	// Normalise before denylist check: mask string literals so keywords
	// inside literals don't false-positive, then strip comments so
	// keywords interleaved with comments don't slip past token
	// boundaries (`DROP /* */ TABLE x`). Map backticks to double quotes first
	// so backtick-quoted identifiers are masked too — otherwise a semicolon
	// inside `a;b` would false-trip the multi-statement check below, and a
	// keyword inside `select` could be scanned. `normalised` is only ever read
	// (never reconstructed into executable SQL), so the swap is safe here.
	maskInput := backticksToDoubleQuotes(sql)
	features := scanSQLFeatures(maskInput)
	normalised, _ := sqlutil.MaskStringLiterals(maskInput, features.hasQuotes)
	normalised = stripSQLComments(normalised, features.hasDashComment || features.hasBlockComment)

	// SECURITY: reject multi-statement queries. A second statement smuggled
	// behind a semicolon (`SHOW DATABASES; SELECT 1`) bypasses the anchored
	// SHOW-command regexes (which require the SHOW to be the whole query),
	// falls through checkQueryPermissions (a SHOW has no FROM/JOIN table refs,
	// so zero are extracted → allowed), and DuckDB then executes the statement
	// list. The dangerous-keyword denylist already blocks the destructive
	// second statements, but rejecting multiple statements outright closes the
	// SHOW-smuggling class for every endpoint that calls this (query, msgpack,
	// arrow, estimate) in one place. A single trailing `;` is allowed; any
	// semicolon before the final non-space character means >1 statement.
	// Checked on the comment-stripped, literal-masked form so a `;` inside a
	// string or comment doesn't false-positive.
	if strings.Contains(strings.TrimRight(normalised, " \t\n\r;"), ";") {
		return &SQLValidationError{Message: "Multiple SQL statements are not allowed"}
	}

	if dangerousSQLPattern.MatchString(normalised) {
		return &SQLValidationError{Message: "Dangerous SQL operation not allowed"}
	}

	// SECURITY: reject the DuckDB filesystem-I/O table-function family in
	// user SQL.
	//
	// Background: an earlier fix (CVE-2026-47735) blocked only the literal
	// spellings `read_parquet(` and `arc_partition_agg(`. That missed the
	// documented alias `parquet_scan(` and the rest of the I/O family
	// (`glob`, `read_blob`, `read_csv*`, `read_json*`, `read_text`,
	// `parquet_metadata`, `parquet_schema`, `delta_scan`, `iceberg_scan`,
	// …). The DuckDB sandbox allowlists the *entire* storage root and Arc's
	// RBAC layer (extractTableReferences) skips anything that looks like a
	// function call, so any of these in user SQL reads across the RBAC
	// boundary — a user scoped to db1 could read /data/db2/secrets via
	// `parquet_scan(...)` or enumerate every database via `glob(...)`.
	// (GHSA-93cm-2v4m-c56c — incomplete fix of CVE-2026-47735.)
	//
	// The match is on the function NAME anywhere in the normalised
	// (literal-masked, comment-stripped) SQL, NOT anchored to a FROM/JOIN
	// position. Position-anchoring is unsafe: DuckDB supports comma
	// cross-joins (`FROM cpu, parquet_scan(...)`) and these readers can sit
	// in subqueries, IN-lists, and lateral joins — none of which the
	// FROM/JOIN patterns reach. Whole-string name matching catches every
	// position. Only Arc's own transformation layer (convertSQLToStoragePaths,
	// which runs AFTER this validation and carries the caller's identity)
	// may emit read_parquet; user input never legitimately contains any of
	// these. A false positive on a string literally containing e.g.
	// 'read_csv' is avoided because single-quoted string literals are masked
	// before this runs (see ioDenylistNormalise).
	//
	// IMPORTANT: this uses a DIFFERENT normalisation than `normalised` above.
	// The shared `normalised` masks double-quoted/backtick identifiers, but
	// DuckDB executes `"parquet_scan"(...)` / `` `read_parquet`(...) ``
	// identically to the unquoted call — so matching the denylist against
	// `normalised` lets a quoted spelling slip past (the name is hidden inside
	// a `__STR__` placeholder). Confirmed live: `SELECT * FROM
	// "parquet_scan"('…/other-db/…')` executed and returned cross-tenant rows
	// (GHSA-93cm-2v4m-c56c review round 2). ioDenylistNormalise strips
	// identifier quoting so quoted function names are exposed to the regex,
	// while still masking single-quoted string literals.
	ioCheckNormalised := ioDenylistNormalise(sql)
	if m := ioTableFunctionPattern.FindStringSubmatch(ioCheckNormalised); m != nil {
		return &SQLValidationError{Message: "File I/O function not allowed in user SQL: " + m[1] + "()"}
	}

	return nil
}

// ioDenylistNormalise produces the form of the SQL the I/O-function denylist is
// matched against. Unlike the general ValidateSQLRequest normalisation, it
// strips identifier quoting (`"` and backtick) BEFORE masking so that a
// quoted-identifier function call — `"parquet_scan"(...)`, “ `read_parquet`(...) “,
// which DuckDB executes identically to the unquoted form — is exposed to the
// regex rather than hidden inside a masked string. Single-quoted string literals
// are still masked (so a literal value such as 'read_csv failed' is not matched)
// and comments are stripped (so a name interleaved with a comment cannot hide).
//
// Stripping `"`/backtick can only ever expose an identifier (DuckDB uses `'`
// for strings and `"`/backtick for identifiers), never the body of a string
// literal, so this cannot unmask a genuine string. In the pathological case of
// a `'` inside a `"..."` identifier the subsequent literal-masking may mis-pair
// quotes, but that errs toward showing MORE text to the denylist (fail-closed),
// never less.
func ioDenylistNormalise(sql string) string {
	stripped := strings.NewReplacer(`"`, "", "`", "").Replace(sql)
	features := scanSQLFeatures(stripped)
	masked, _ := sqlutil.MaskStringLiterals(stripped, features.hasQuotes)
	masked = stripSQLComments(masked, features.hasDashComment || features.hasBlockComment)
	return masked
}

// ioTableFunctionPattern matches a call to any DuckDB function that reads from
// the filesystem (or an attached storage scanner), by name, in any position of
// the masked/comment-stripped SQL. This is an intentionally comprehensive
// denylist of the I/O family: each entry takes a path/glob and would let user
// SQL read across the RBAC boundary inside the sandbox's allowlisted storage
// root. The trailing `\s*\(` ensures we match the function-call form, not a
// bare identifier. Anchored matching (`\b`) avoids matching these as a
// substring of a longer identifier.
//
// Maintenance: when DuckDB adds a new path-taking table function (or Arc loads
// an extension that exposes one), add it here. TestIOTableFunctionPattern_Family
// documents the set we verified against the pinned DuckDB release.
var ioTableFunctionPattern = regexp.MustCompile(`(?i)\b(` + strings.Join([]string{
	"read_parquet",
	"parquet_scan",
	"parquet_metadata",
	"parquet_schema",
	"parquet_file_metadata",
	"parquet_kv_metadata",
	"parquet_bloom_probe",
	"read_csv",
	"read_csv_auto",
	"sniff_csv",
	"read_json",
	"read_json_auto",
	"read_json_objects",
	"read_json_objects_auto",
	"read_ndjson",
	"read_ndjson_auto",
	"read_ndjson_objects",
	"read_text",
	"read_blob",
	"read_xlsx",
	"glob",
	"delta_scan",
	"iceberg_scan",
	"iceberg_metadata",
	"iceberg_snapshots",
	"arc_partition_agg",
}, "|") + `)\s*\(`)

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

	// Bail to slow path for features the fast path can't handle. EXTRACT/
	// SUBSTRING/TRIM/OVERLAY need the slow path's FROM-keyword mask.
	features := scanSQLFeatures(sql)
	if features.hasQuotes || features.hasDashComment || features.hasBlockComment || sqlutil.ContainsFromKeywordFunction(sql) {
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

	// Phase 1b: Mask bare FROM inside EXTRACT/SUBSTRING/TRIM/OVERLAY so the
	// table-rewriter regex below does not treat e.g. `time` in
	// `EXTRACT(YEAR FROM time)` as a measurement.
	sql, fromMasks := sqlutil.MaskFromKeywordsInFunctionBodies(sql)

	// Phase 2: Strip SQL comments (after masking to preserve comments inside strings)
	// e.g., "-- FROM mydb.cpu" should not be converted
	sql = stripSQLComments(sql, features.hasDashComment || features.hasBlockComment)

	// Compute sqlLower once after all pre-processing mutations
	sqlLower := strings.ToLower(sql)

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
		matchLower := strings.ToLower(match)
		idx := strings.Index(sqlLower, matchLower)
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
		matchLower := strings.ToLower(match)
		idx := strings.Index(sqlLower, matchLower)
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

	// Restore masked FROM keywords and string literals. Both use content-
	// addressed placeholders, so the intermediate length-changing regex
	// rewrites above are safe.
	sql = sqlutil.UnmaskFromKeywordsInFunctionBodies(sql, fromMasks)
	sql = sqlutil.UnmaskStringLiterals(sql, masks)

	return sql
}

// getStoragePath returns the storage path for a database.table
func (h *QueryHandler) getStoragePath(database, table string) string {
	return storage.GetStoragePath(h.storage, database, table)
}

// quotePath returns a safe single-quoted DuckDB string literal for use
// inside read_parquet('PATH', ...) interpolations. Single quotes inside
// the path are doubled per DuckDB's literal-escape rule. This is the
// single source of truth for path interpolation in the query layer —
// every `read_parquet('` site goes through this helper to close the
// SQL-injection vector via the x-arc-database header / measurement
// names / manifest entries (review/query-path-criticals C2).
func quotePath(path string) string {
	return "'" + sqlutil.EscapeStringLiteral(path) + "'"
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
				pathsStr.WriteString(quotePath(p))
			}
			pathsStr.WriteString("]")
			h.logger.Info().Int("partition_count", len(pathList)).Str("keyword", keyword).Msg("Partition pruning: Using targeted paths")
			return keyword + " read_parquet(" + pathsStr.String() + ", " + options + ")"
		} else if pathStr, ok := optimizedPath.(string); ok {
			h.logger.Info().Str("optimized_path", pathStr).Str("keyword", keyword).Msg("Partition pruning: Using optimized path")
			return keyword + " read_parquet(" + quotePath(pathStr) + ", " + options + ")"
		}
	}

	return keyword + " read_parquet(" + quotePath(path) + ", " + options + ")"
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
				pathsStr.WriteString(quotePath(p))
			}
			pathsStr.WriteString("]")
			h.logger.Info().Int("partition_count", len(pathList)).Str("keyword", keyword).Msg("Partition pruning: Using targeted paths")
			return keyword + " read_parquet(" + pathsStr.String() + ", " + options + ")", nil
		} else if pathStr, ok := optimizedPath.(string); ok {
			h.logger.Info().Str("optimized_path", pathStr).Str("keyword", keyword).Msg("Partition pruning: Using optimized path")
			return keyword + " read_parquet(" + quotePath(pathStr) + ", " + options + ")", nil
		}
	}

	return keyword + " read_parquet(" + quotePath(path) + ", " + options + ")", nil
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
		return keyword + " read_parquet(" + quotePath(h.getStoragePath(database, measurement)) + ", " + options + ")"
	}

	// If no metadata found, fall back to hot tier (data might not be registered yet)
	if len(actualTiers) == 0 {
		h.logger.Debug().
			Str("database", database).
			Str("measurement", measurement).
			Msg("No tier metadata found, using hot tier")
		return keyword + " read_parquet(" + quotePath(h.getStoragePath(database, measurement)) + ", " + options + ")"
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
		return keyword + " read_parquet(" + quotePath(paths[0]) + ", " + options + ")"
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
		pathList.WriteString(quotePath(p))
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

	// FAST PATH: skip all regex machinery for simple single-table queries.
	// Bail when the SQL has a bare FROM inside EXTRACT/SUBSTRING/TRIM/OVERLAY
	// — `SELECT EXTRACT(YEAR FROM CURRENT_DATE)` slips past isSingleTableQuery
	// with fromCount==1, so the slow path's mask helper must run.
	if isSingleTableQuery(sqlLower) && !strings.Contains(sqlLower, "with ") && !sqlutil.ContainsFromKeywordFunction(sql) {
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

	// Phase 1b: see convertSQLToStoragePaths.
	sql, fromMasks := sqlutil.MaskFromKeywordsInFunctionBodies(sql)

	// Phase 2: Strip SQL comments
	sql = stripSQLComments(sql, features.hasDashComment || features.hasBlockComment)

	// Compute sqlLower once after all pre-processing mutations
	sqlLower = strings.ToLower(sql)

	// Extract CTE names only if query has WITH clause (fast path for majority of queries)
	var cteNames map[string]bool
	if strings.Contains(sqlLower, "with ") {
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
		matchLower := strings.ToLower(match)
		idx := strings.Index(sqlLower, matchLower)
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
		matchLower := strings.ToLower(match)
		idx := strings.Index(sqlLower, matchLower)
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

	// Restore masked FROM keywords and original string literals.
	sql = sqlutil.UnmaskFromKeywordsInFunctionBodies(sql, fromMasks)
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

	// Include tier column if tiering is enabled. The types slice is
	// parallel to columns and feeds the msgpack envelope's "types"
	// field — SHOW results are schema-known by construction, so we
	// declare types explicitly rather than infer them from cell
	// content (which is brittle when leading rows are nil).
	var columns []string
	var types []string
	hasTiering := h.tieringManager != nil
	if hasTiering {
		columns = []string{"database", "tier"}
		types = []string{"utf8", "utf8"}
	} else {
		columns = []string{"database"}
		types = []string{"utf8"}
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
		return respondError(c, fiber.StatusInternalServerError, "Failed to read storage: "+err.Error(), time.Now().UTC().Format(time.RFC3339), start)
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

	return respondSuccessRows(c, columns, types, data, time.Now().UTC().Format(time.RFC3339), start)
}

// handleShowTables handles SHOW TABLES/MEASUREMENTS command by scanning storage
func (h *QueryHandler) handleShowTables(c *fiber.Ctx, start time.Time, database string) error {
	h.logger.Debug().Str("database", database).Msg("Handling SHOW TABLES")

	columns := []string{"database", "table_name", "storage_path", "file_count", "total_size_mb"}
	// types parallel to columns: SHOW TABLES emits two text columns,
	// a text path, an int file count, and a float size. Declared
	// explicitly rather than inferred from row content.
	types := []string{"utf8", "utf8", "utf8", "int64", "float64"}
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
		return respondError(c, fiber.StatusInternalServerError, "Failed to read database: "+err.Error(), time.Now().UTC().Format(time.RFC3339), start)
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

	return respondSuccessRows(c, columns, types, data, time.Now().UTC().Format(time.RFC3339), start)
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
	if err := validateHeaderDatabase(headerDB); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
			Success:      false,
			Error:        "invalid x-arc-database header: " + err.Error(),
			WarningLevel: "error",
		})
	}

	// RBAC-gate SHOW commands — mirror executeQuery's permission checks.
	// SHOW DATABASES / SHOW TABLES extract zero table references, so
	// checkQueryPermissions (which extracts FROM db.table refs) would pass them
	// through without a permission check. Reject them here instead; the estimate
	// endpoint has no legitimate use for metadata commands.
	//
	// Normalize the SQL before matching: strip comments and trim whitespace so
	// that a comment (e.g. /* x */ SHOW DATABASES) cannot hide a SHOW command
	// from the anchored regex. The same normalization is applied by
	// checkQueryPermissions below; without it, the comment bypass would let
	// SHOW reach DuckDB unchecked (the regex would not match the raw string,
	// checkQueryPermissions would find zero table refs, and DuckDB would strip
	// the comment and execute the SHOW).
	normalised := normalizeSQLForShow(req.SQL)

	if showDatabasesPattern.MatchString(normalised) {
		if err := h.checkMeasurementPermission(c, "*", "*", "read"); err != nil {
			metrics.Get().IncQueryErrors()
			return c.Status(fiber.StatusForbidden).JSON(EstimateResponse{
				Success:      false,
				Error:        "access denied: no read permission to list databases",
				WarningLevel: "error",
			})
		}
		return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
			Success:      false,
			Error:        "SHOW DATABASES is not supported on the estimate endpoint; use /api/v1/query instead",
			WarningLevel: "error",
		})
	}
	if matches := showTablesPattern.FindStringSubmatch(normalised); matches != nil {
		// Resolve the target database the same way the command itself does:
		// an explicit `SHOW TABLES FROM db` wins, otherwise the x-arc-database
		// header is the implicit target, falling back to "default" only when
		// neither is set. Checking a different database than the command targets
		// would either deny a legitimate request or check the wrong scope.
		database := "default"
		if len(matches) > 1 && matches[1] != "" {
			database = matches[1]
		} else if headerDB != "" {
			database = headerDB
		}
		// Validate the resolved database name (defense-in-depth, matching
		// executeQuery): the SHOW regex permits a quoted/dotted token, so reject
		// path-traversal like `SHOW TABLES FROM ..` before any storage access.
		if err := validateIdentifier(database); err != nil {
			metrics.Get().IncQueryErrors()
			return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
				Success:      false,
				Error:        "invalid database name: " + err.Error(),
				WarningLevel: "error",
			})
		}
		if err := h.checkMeasurementPermission(c, database, "*", "read"); err != nil {
			metrics.Get().IncQueryErrors()
			return c.Status(fiber.StatusForbidden).JSON(EstimateResponse{
				Success:      false,
				Error:        fmt.Sprintf("access denied: no read permission for database '%s'", database),
				WarningLevel: "error",
			})
		}
		return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
			Success:      false,
			Error:        "SHOW TABLES/MEASUREMENTS is not supported on the estimate endpoint; use /api/v1/query instead",
			WarningLevel: "error",
		})
	}

	// If header is set, reject cross-database syntax (db.table not allowed),
	// matching the validation in executeQuery.
	if headerDB != "" && hasCrossDatabaseSyntax(req.SQL) {
		metrics.Get().IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(EstimateResponse{
			Success:      false,
			Error:        "Cross-database queries (db.table syntax) not allowed when x-arc-database header is set",
			WarningLevel: "error",
		})
	}

	// RBAC permission check for all tables referenced in the query
	if err := h.checkQueryPermissions(c, req.SQL, "read"); err != nil {
		metrics.Get().IncQueryErrors()
		return c.Status(fiber.StatusForbidden).JSON(EstimateResponse{
			Success:      false,
			Error:        err.Error(),
			WarningLevel: "error",
		})
	}

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

	// Validate the database filter parameter if provided
	if dbFilter != "" {
		if err := validateIdentifier(dbFilter); err != nil {
			metrics.Get().IncQueryErrors()
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"success": false,
				"error":   "invalid database parameter: " + err.Error(),
			})
		}
	}

	// RBAC permission check - user needs at least some read permission to list measurements.
	// When a database filter is specified, check against that specific database instead of
	// requiring wildcard access — users with single-database permissions should be able to
	// list measurements scoped to that database.
	rbacDB := "*"
	if dbFilter != "" {
		rbacDB = dbFilter
	}
	if err := h.checkMeasurementPermission(c, rbacDB, "*", "read"); err != nil {
		metrics.Get().IncQueryErrors()
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

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

	timestamp := time.Now().UTC().Format(time.RFC3339)

	// Arrow-native path: bypasses database/sql row scanning entirely.
	if arrowJSONQueryFunc != nil {
		ctx := context.Background()
		_, handled := arrowJSONQueryFunc(h, c, ctx, nil, convertedSQL, false, 0, start, timestamp, nil, nil, nil)
		if handled {
			// Metrics are recorded inside the async stream callback — not here.
			return nil
		}
	}

	// Fallback: database/sql path
	rows, err := h.db.Query(convertedSQL)
	if err != nil {
		m.IncQueryErrors()
		h.logger.Error().Err(err).Str("sql", sql).Msg("Measurement query failed")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       timestamp,
		})
	}

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		m.IncQueryErrors()
		h.logger.Error().Err(err).Msg("Failed to get column names in measurement query")
		return c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       timestamp,
		})
	}

	// Get column types for typed JSON serialization
	columnTypes, err := rows.ColumnTypes()
	var colTypes []colType
	if err == nil {
		colTypes = mapColumnTypes(columnTypes)
	} else {
		colTypes = make([]colType, len(columns))
	}

	// Capture token name before async callback (Fiber context not safe in callbacks)
	tokenName := getTokenName(c)

	// Stream typed JSON response directly to HTTP. Use c.UserContext()
	// so client disconnects propagate to per-row cancellation — fasthttp
	// keeps c.Context() alive across the SetBodyStreamWriter boundary,
	// per gemini r1. queryMeasurement has no server-side timeout today
	// (#308 follow-up); when #308 adds one, derive from this ctx.
	streamCtx := c.UserContext()
	c.Set("Content-Type", "application/json")
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		rowCount, streamErr := streamTypedJSON(streamCtx, w, columns, colTypes, rows, 0, nil, start, timestamp)
		w.Flush()

		rows.Close()

		if streamErr != nil {
			m.IncQueryErrors()
			// Per-handler client-disconnect counter (#426). queryMeasurement
			// uses the pure database/sql streaming JSON path same as the
			// other sites above, so it shares the sql_json label.
			if isClientError(streamErr) {
				m.IncQueryClientDisconnect(metrics.DisconnectPathSQLJSON)
			}
			// Warn for client-disconnect / context expiry (headers already
			// committed, partial result was delivered). Error for genuine
			// server-side failures.
			h.streamErrEvent(streamErr).Err(streamErr).
				Str("measurement", measurement).
				Int("rows_sent", rowCount).
				Msg("queryMeasurement stream truncated after headers committed")
			return
		}

		// Record success metrics
		m.IncQuerySuccess()
		m.IncQueryRows(int64(rowCount))
		m.RecordQueryLatency(time.Since(start).Microseconds())

		h.logger.Info().
			Str("measurement", measurement).
			Int("row_count", rowCount).
			Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
			Msg("Measurement query completed")
		h.logSlowQuery(convertedSQL, start, rowCount, tokenName)
	})
	return nil
}
