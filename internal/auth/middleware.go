package auth

import (
	"strings"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
)

// Middleware configuration
type MiddlewareConfig struct {
	// AuthManager instance
	AuthManager *AuthManager

	// Routes that don't require authentication
	PublicRoutes []string

	// Route prefixes that don't require authentication
	PublicPrefixes []string

	// Required permission for protected routes (empty means any valid token)
	RequiredPermission string

	// Skip authentication entirely (for development/testing)
	Skip bool
}

// DefaultMiddlewareConfig returns default middleware config
func DefaultMiddlewareConfig() MiddlewareConfig {
	return MiddlewareConfig{
		PublicRoutes: []string{
			"/health",
			"/ready",
			"/api/v1/auth/verify",
		},
		PublicPrefixes: []string{
			"/metrics",
		},
		Skip: false,
	}
}

// NewMiddleware creates authentication middleware for Fiber
func NewMiddleware(config MiddlewareConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Skip authentication if disabled
		if config.Skip {
			return c.Next()
		}

		// Check if route is public
		path := c.Path()
		for _, route := range config.PublicRoutes {
			if path == route {
				return c.Next()
			}
		}
		for _, prefix := range config.PublicPrefixes {
			if strings.HasPrefix(path, prefix) {
				return c.Next()
			}
		}

		// No auth manager configured
		if config.AuthManager == nil {
			return c.Next()
		}

		// Track auth request
		metrics.Get().IncAuthRequests()

		// Extract token from request
		token := ExtractTokenFromRequest(c)

		// No token provided
		if token == "" {
			metrics.Get().IncAuthFailures()
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"success": false,
				"error":   "Authentication required",
			})
		}

		// Verify token
		tokenInfo := config.AuthManager.VerifyToken(token)
		if tokenInfo == nil {
			metrics.Get().IncAuthFailures()
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"success": false,
				"error":   "Invalid or expired token",
			})
		}

		// Check permission if required
		if config.RequiredPermission != "" {
			if !config.AuthManager.HasPermission(tokenInfo, config.RequiredPermission) {
				return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
					"success": false,
					"error":   "Insufficient permissions",
				})
			}
		}

		// Store token info in context for handlers
		c.Locals("token_info", tokenInfo)

		return c.Next()
	}
}

// RequirePermission creates middleware that requires a specific permission
func RequirePermission(am *AuthManager, permission string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenInfo, ok := c.Locals("token_info").(*TokenInfo)
		if !ok || tokenInfo == nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"success": false,
				"error":   "Authentication required",
			})
		}

		if !am.HasPermission(tokenInfo, permission) {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"success": false,
				"error":   "Permission denied: " + permission + " required",
			})
		}

		return c.Next()
	}
}

// RequireRead creates middleware requiring read permission
func RequireRead(am *AuthManager) fiber.Handler {
	return RequirePermission(am, "read")
}

// RequireWrite creates middleware requiring write permission
func RequireWrite(am *AuthManager) fiber.Handler {
	return RequirePermission(am, "write")
}

// RequireDelete creates middleware requiring delete permission
func RequireDelete(am *AuthManager) fiber.Handler {
	return RequirePermission(am, "delete")
}

// RequireAdmin creates middleware requiring admin permission
func RequireAdmin(am *AuthManager) fiber.Handler {
	return RequirePermission(am, "admin")
}

// GetTokenInfo retrieves token info from Fiber context
func GetTokenInfo(c *fiber.Ctx) *TokenInfo {
	if info, ok := c.Locals("token_info").(*TokenInfo); ok {
		return info
	}
	return nil
}

// ExtractTokenFromRequest extracts auth token from Fiber request headers.
// Checks in order: Authorization Bearer, Authorization plain, x-api-key.
func ExtractTokenFromRequest(c *fiber.Ctx) string {
	authHeader := c.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		return strings.TrimPrefix(authHeader, "Bearer ")
	}
	if authHeader != "" {
		return authHeader
	}
	return c.Get("x-api-key")
}

// RequireResourcePermission creates middleware that checks resource-scoped permissions
// using RBAC when enabled, with fallback to OSS token permissions.
// The database and measurement are extracted from request headers or path.
func RequireResourcePermission(am *AuthManager, rm *RBACManager, permission string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tokenInfo := GetTokenInfo(c)
		if tokenInfo == nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"success": false,
				"error":   "Authentication required",
			})
		}

		// Extract database from request
		database := extractDatabase(c)
		measurement := extractMeasurement(c)

		// If no RBAC manager or RBAC not enabled, use OSS permissions
		if rm == nil || !rm.IsRBACEnabled() {
			if !am.HasPermission(tokenInfo, permission) {
				return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
					"success": false,
					"error":   "Permission denied: " + permission + " required",
				})
			}
			return c.Next()
		}

		// Use RBAC permission checking
		result := rm.CheckPermission(&PermissionCheckRequest{
			TokenInfo:   tokenInfo,
			Database:    database,
			Measurement: measurement,
			Permission:  permission,
		})

		if !result.Allowed {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"success": false,
				"error":   result.Reason,
			})
		}

		// Store permission result in context for potential audit logging
		c.Locals("permission_result", result)
		return c.Next()
	}
}

// extractDatabase extracts the target database from the request.
// Checks: x-arc-database header, path parameter, query parameter
func extractDatabase(c *fiber.Ctx) string {
	// Check header first
	if db := c.Get("x-arc-database"); db != "" {
		return db
	}

	// Check path parameter
	if db := c.Params("database"); db != "" {
		return db
	}

	// Check query parameter
	if db := c.Query("database"); db != "" {
		return db
	}

	// Default to empty (will be checked against wildcard patterns)
	return ""
}

// extractMeasurement extracts the target measurement from the request.
// Checks: x-arc-measurement header, path parameter, query parameter
func extractMeasurement(c *fiber.Ctx) string {
	// Check header first
	if m := c.Get("x-arc-measurement"); m != "" {
		return m
	}

	// Check path parameter
	if m := c.Params("measurement"); m != "" {
		return m
	}

	// Check query parameter
	if m := c.Query("measurement"); m != "" {
		return m
	}

	// Default to empty (will match against role's database-level permissions)
	return ""
}

// RequireResourceRead creates middleware requiring read permission with resource context
func RequireResourceRead(am *AuthManager, rm *RBACManager) fiber.Handler {
	return RequireResourcePermission(am, rm, "read")
}

// RequireResourceWrite creates middleware requiring write permission with resource context
func RequireResourceWrite(am *AuthManager, rm *RBACManager) fiber.Handler {
	return RequireResourcePermission(am, rm, "write")
}

// RequireResourceDelete creates middleware requiring delete permission with resource context
func RequireResourceDelete(am *AuthManager, rm *RBACManager) fiber.Handler {
	return RequireResourcePermission(am, rm, "delete")
}
