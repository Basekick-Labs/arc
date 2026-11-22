package audit

import (
	pathpkg "path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
)

// compactionPrefix is matched at a segment boundary, so a sibling path that
// merely starts with the same characters (/api/v1/compactionfoo) is not a
// compaction action. Declared const so prefix+"/" folds at compile time
// instead of concatenating on every audited request.
const compactionPrefix = "/api/v1/compaction"

// maxRecordedField bounds every request-supplied string an event records
// (path, database, measurement, user agent): the writer stores them as sent,
// and a client controls their length.
const maxRecordedField = 256

// excludedPaths are never audited
var excludedPaths = map[string]bool{
	"/health":       true,
	"/healthz":      true,
	"/metrics":      true,
	"/api/v1/logs":  true,
	"/api/v1/ready": true,
	// Public (internal/auth middleware PublicRoutes): authentication does not
	// run before it, so an event could never name the caller, and clients
	// poll it. Registered after the audit middleware since #1630.
	"/api/v1/auth/verify": true,
}

// isMetricsPath reports the JSON metrics family, which monitoring dashboards
// poll every few seconds: like /metrics, never audited, or include_reads
// would record every poll.
func isMetricsPath(p string) bool {
	return p == "/api/v1/metrics" || strings.HasPrefix(p, "/api/v1/metrics/")
}

// isRead reports the methods include_reads governs. HEAD is a GET without a
// body (Fiber registers it beside every GET) and OPTIONS a CORS preflight;
// neither changes anything, so both are reads (#1630 review).
func isRead(method string) bool {
	return method == fiber.MethodGet || method == fiber.MethodHead || method == fiber.MethodOptions
}

// DetailLocalsKey is the Fiber locals key used by handlers to supply custom
// key-value metadata to be attached to AuditEvent.Detail.
const DetailLocalsKey = "audit_detail"

// Middleware returns a Fiber middleware that logs auditable requests
func Middleware(logger *Logger, includeReads bool) fiber.Handler {
	return func(c *fiber.Ctx) error {
		path := c.Path()

		// Skip excluded paths, matched on the cleaned path as the auth
		// middleware matches public routes, so /api/v1/auth/verify/ is
		// excluded too.
		clean := pathpkg.Clean(path)
		if excludedPaths[clean] || isMetricsPath(clean) {
			return c.Next()
		}

		// Skip reads (GET, HEAD, OPTIONS) unless includeReads is enabled.
		// Compare the borrowed string first so a skipped request allocates
		// nothing.
		method := c.Method()
		if !includeReads && isRead(method) {
			return c.Next()
		}

		// Fiber runs with Immutable=false, so every accessor returns a string
		// aliasing a pooled request buffer that the next request overwrites.
		// The event outlives the request (a background writer serialises it),
		// so copy each retained string. The path is copied before the handler
		// runs so a handler that rewrites it does not change the audited one.
		//
		// The event is classified on the full path and only the stored copy is
		// truncated: a route ID may carry leading zeros (strconv.ParseInt
		// accepts them), so a padded /api/v1/auth/tokens/000…5/rotate cut to
		// 256 bytes would lose its /rotate and be filed as api.post (#1630
		// review).
		method = utils.CopyString(method)
		path = utils.CopyString(path)
		start := time.Now()

		// Execute the handler
		err := c.Next()

		// Build audit event
		duration := time.Since(start)
		statusCode := responseStatus(c, err)

		event := &AuditEvent{
			Timestamp:  start.UTC(),
			EventType:  classifyEvent(method, path, statusCode),
			Method:     method,
			Path:       truncate(path, maxRecordedField),
			StatusCode: statusCode,
			IPAddress:  utils.CopyString(c.IP()),
			UserAgent:  utils.CopyString(truncate(c.Get("User-Agent"), maxRecordedField)),
			DurationMs: duration.Milliseconds(),
		}

		// Extract actor from token info
		if tokenInfo := auth.GetTokenInfo(c); tokenInfo != nil {
			event.Actor = tokenInfo.Name
		} else {
			event.Actor = "anonymous"
		}

		// Extract database and measurement from headers or params
		event.Database = c.Get("x-arc-database")
		if event.Database == "" {
			event.Database = c.Params("database")
		}
		if event.Database == "" {
			event.Database = c.Query("db")
		}

		event.Database = utils.CopyString(truncate(event.Database, maxRecordedField))

		event.Measurement = c.Get("x-arc-measurement")
		if event.Measurement == "" {
			event.Measurement = c.Params("measurement")
		}

		event.Measurement = utils.CopyString(truncate(event.Measurement, maxRecordedField))

		// Extract handler-supplied audit detail from Fiber locals if present
		if detail, ok := c.Locals(DetailLocalsKey).(map[string]string); ok && len(detail) > 0 {
			event.Detail = make(map[string]string, len(detail))
			for key, value := range detail {
				event.Detail[utils.CopyString(key)] = utils.CopyString(value)
			}
		}

		logger.LogEvent(event)

		return err
	}
}

// classifyEvent determines the event type from method, path, and status
func classifyEvent(method, path string, statusCode int) string {
	// Auth failures
	if statusCode == 401 || statusCode == 403 {
		return "auth.failed"
	}

	// Token management
	if strings.HasPrefix(path, "/api/v1/auth/tokens") {
		if name := classifyTokenEvent(method, path); name != "" {
			return name
		}
	}

	// RBAC management
	if strings.HasPrefix(path, "/api/v1/rbac/") {
		resource := extractRBACResource(path)
		switch method {
		case "POST":
			return "rbac." + resource + ".created"
		case "PUT", "PATCH":
			// The RBAC routes update with PATCH (internal/api/rbac_routes.go);
			// PUT is kept for any client that sends it.
			return "rbac." + resource + ".updated"
		case "DELETE":
			return "rbac." + resource + ".deleted"
		default:
			return "rbac." + resource + ".read"
		}
	}

	// Data operations
	if strings.HasPrefix(path, "/api/v1/query") || strings.HasPrefix(path, "/api/v1/sql") {
		return "data.query"
	}

	if path == "/write" || path == "/api/v2/write" || strings.HasPrefix(path, "/api/v1/write") {
		return "data.write"
	}

	if strings.HasPrefix(path, "/api/v1/import") {
		return "data.import"
	}

	if path == "/api/v1/delete" {
		return "data.delete"
	}

	// Database management
	if strings.HasPrefix(path, "/api/v1/databases") {
		switch method {
		case "POST":
			return "database.created"
		case "DELETE":
			return "database.deleted"
		}
	}

	// MQTT management
	if strings.HasPrefix(path, "/api/v1/mqtt") {
		return "mqtt." + strings.ToLower(method)
	}

	// Compaction. Every method used to classify as "compaction.triggered", so
	// an auditor filtering for who triggered a compaction also got everyone
	// who polled the status page -- and with HEAD bypassing the include_reads
	// skip below, that happened at the default configuration (#1168).
	//
	// POST /trigger keeps the shipped name because audit consumers filter on
	// "compaction.triggered". A new POST sibling does not inherit it: it gets
	// "compaction.post" until someone names it deliberately, which is the
	// safer default -- inheriting "triggered" is the mislabelling fixed here.
	//
	// Matched at a segment boundary, so /api/v1/compactionfoo is not a
	// compaction action. The sibling prefixes below still match mid-segment.
	if path == compactionPrefix || strings.HasPrefix(path, compactionPrefix+"/") {
		// Keyed on the trigger PATH, not merely on POST. Fiber answers 405 for
		// a POST to a read route, and those reached the classifier too -- so
		// method-keying alone would still file them under the trigger action,
		// the same mislabelling in a rarer shape.
		//
		// The trailing slash is trimmed because the router does not require
		// it to match: StrictRouting is left at its default (false), so
		// POST /api/v1/compaction/trigger/ reaches the trigger handler and
		// must not be filed as an ordinary POST. Verified against the router,
		// not assumed.
		if method == "POST" && strings.TrimSuffix(path, "/") == compactionPrefix+"/trigger" {
			return "compaction.triggered"
		}
		return "compaction." + strings.ToLower(method)
	}

	// Tiering
	if strings.HasPrefix(path, "/api/v1/tiering") {
		return "tiering." + strings.ToLower(method)
	}

	// Default
	return "api." + strings.ToLower(method)
}

// classifyTokenEvent names a write under /api/v1/auth/tokens, or returns ""
// for a request that is not one (a read). The shipped names token.created,
// token.rotated and token.deleted keep their meaning; a revoke and a team
// addition used to be filed as token.created, a team removal as
// token.deleted and an update as api.patch (#1630). Matched on segments, with
// a trailing slash ignored as the router ignores it.
func classifyTokenEvent(method, path string) string {
	rest := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v1/auth/tokens"), "/")
	if rest != "" && !strings.HasPrefix(rest, "/") {
		return "" // a sibling such as /api/v1/auth/tokensfoo
	}
	var seg []string
	if rest != "" {
		seg = strings.Split(rest[1:], "/")
	}
	switch {
	case len(seg) == 0 && method == "POST":
		return "token.created"
	case len(seg) == 1 && method == "DELETE":
		return "token.deleted"
	case len(seg) == 1 && method == "PATCH":
		return "token.updated"
	case len(seg) == 2 && method == "POST" && seg[1] == "rotate":
		return "token.rotated"
	case len(seg) == 2 && method == "POST" && seg[1] == "revoke":
		return "token.revoked"
	case len(seg) == 2 && method == "POST" && seg[1] == "teams":
		return "token.team_added"
	case len(seg) == 3 && method == "DELETE" && seg[1] == "teams":
		return "token.team_removed"
	}
	return ""
}

// rbacResources names the static path segments of the RBAC routes
// (internal/api/rbac_routes.go) as the resource an event is about.
var rbacResources = map[string]string{
	"organizations":           "org",
	"teams":                   "team",
	"roles":                   "role",
	"measurements":            "measurement_permission",
	"measurement-permissions": "measurement_permission",
	"memberships":             "membership",
}

// extractRBACResource names the resource of an RBAC route by the LAST static
// segment of its path, not the first: POST /organizations/:org_id/teams
// creates a team and POST /roles/:role_id/measurements a measurement
// permission (#1630). An ID segment is never a resource name. A path with no
// known segment is "unknown".
func extractRBACResource(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/api/v1/rbac/"), "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if r, ok := rbacResources[parts[i]]; ok {
			return r
		}
	}
	return "unknown"
}

// responseStatus is the status the client receives. A handler that returns an
// error has not set it yet: the app's error handler does, after this
// middleware returns. It uses the same check as customErrorHandler
// (internal/api/server.go): a plain type assertion, so a *fiber.Error wrapped
// in another error is a 500 there and here. A 404 or 405 from the router
// arrives the same way.
func responseStatus(c *fiber.Ctx, err error) int {
	if err == nil {
		return c.Response().StatusCode()
	}
	if e, ok := err.(*fiber.Error); ok {
		return e.Code
	}
	return fiber.StatusInternalServerError
}

// truncate cuts s to at most maxLen bytes without splitting a UTF-8
// sequence.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	cut := maxLen
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
