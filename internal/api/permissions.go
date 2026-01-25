package api

import (
	"fmt"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// CheckWritePermissions checks if the token has write permission for the database and measurements.
// This is a shared implementation used by both LineProtocol and MsgPack handlers.
func CheckWritePermissions(c *fiber.Ctx, rbacManager RBACChecker, logger zerolog.Logger, database string, measurements []string) error {
	// Skip if RBAC is not configured or not enabled
	if rbacManager == nil || !rbacManager.IsRBACEnabled() {
		return nil
	}

	// Get token info from context
	tokenInfo, ok := c.Locals("token").(*auth.TokenInfo)
	if !ok || tokenInfo == nil {
		return nil // No token info, let other middleware handle auth
	}

	// Check permission for each measurement
	for _, measurement := range measurements {
		req := &auth.PermissionCheckRequest{
			TokenInfo:   tokenInfo,
			Database:    database,
			Measurement: measurement,
			Permission:  "write",
		}

		result := rbacManager.CheckPermission(req)
		if !result.Allowed {
			logger.Warn().
				Int64("token_id", tokenInfo.ID).
				Str("database", database).
				Str("measurement", measurement).
				Str("reason", result.Reason).
				Msg("RBAC denied write access")
			return fmt.Errorf("access denied: no write permission for %s.%s", database, measurement)
		}
	}

	return nil
}
