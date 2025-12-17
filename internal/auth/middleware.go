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

		// Extract token from headers (in order of precedence)
		// 1. Authorization: Bearer <token>
		// 2. Authorization: <token> (plain token)
		// 3. x-api-key: <token> (Telegraf Arc plugin compatibility)
		var token string

		authHeader := c.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token = strings.TrimPrefix(authHeader, "Bearer ")
		} else if authHeader != "" {
			// Also accept plain token (for curl convenience)
			token = authHeader
		}

		// Fallback to x-api-key header (Telegraf Arc plugin uses this)
		if token == "" {
			token = c.Get("x-api-key")
		}

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
