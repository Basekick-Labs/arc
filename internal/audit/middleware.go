package audit

import (
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
)

// excludedPaths are never audited
var excludedPaths = map[string]bool{
	"/health":       true,
	"/healthz":      true,
	"/metrics":      true,
	"/api/v1/logs":  true,
	"/api/v1/ready": true,
}

// Middleware returns a Fiber middleware that logs auditable requests
func Middleware(logger *Logger, includeReads bool) fiber.Handler {
	return func(c *fiber.Ctx) error {
		path := c.Path()

		// Skip excluded paths
		if excludedPaths[path] {
			return c.Next()
		}

		// Skip GET requests unless includeReads is enabled
		method := c.Method()
		if !includeReads && method == "GET" {
			return c.Next()
		}

		start := time.Now()

		// Execute the handler
		err := c.Next()

		// Build audit event
		duration := time.Since(start)
		statusCode := c.Response().StatusCode()

		event := &AuditEvent{
			Timestamp:  start.UTC(),
			EventType:  classifyEvent(method, path, statusCode),
			Method:     method,
			Path:       path,
			StatusCode: statusCode,
			IPAddress:  c.IP(),
			UserAgent:  truncate(c.Get("User-Agent"), 256),
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

		event.Measurement = c.Get("x-arc-measurement")
		if event.Measurement == "" {
			event.Measurement = c.Params("measurement")
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
		switch method {
		case "POST":
			if strings.HasSuffix(path, "/rotate") {
				return "token.rotated"
			}
			return "token.created"
		case "DELETE":
			return "token.deleted"
		}
	}

	// RBAC management
	if strings.HasPrefix(path, "/api/v1/rbac/") {
		resource := extractRBACResource(path)
		switch method {
		case "POST":
			return "rbac." + resource + ".created"
		case "PUT":
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

	// Compaction triggers
	if strings.HasPrefix(path, "/api/v1/compaction") {
		return "compaction.triggered"
	}

	// Tiering
	if strings.HasPrefix(path, "/api/v1/tiering") {
		return "tiering." + strings.ToLower(method)
	}

	// Default
	return "api." + strings.ToLower(method)
}

// extractRBACResource extracts the RBAC resource name from a path like /api/v1/rbac/organizations/...
func extractRBACResource(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/api/v1/rbac/"), "/")
	if len(parts) > 0 && parts[0] != "" {
		// Singularize: organizations -> org, teams -> team, roles -> role
		r := parts[0]
		switch r {
		case "organizations":
			return "org"
		case "teams":
			return "team"
		case "roles":
			return "role"
		case "memberships":
			return "membership"
		default:
			return r
		}
	}
	return "unknown"
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
