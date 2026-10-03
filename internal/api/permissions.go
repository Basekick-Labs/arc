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
	// Gated on the checker being WIRED, not on the license: enforcement must
	// survive a lapsed or revoked license, or every tenant token would widen
	// to full write the moment one expired. See the RBAC ENFORCEMENT MODEL
	// note in internal/auth/rbac_manager.go. CheckPermission resolves admin
	// break-glass, memberships-are-authoritative and no-memberships-means-
	// coarse itself, so a deployment without RBAC configured is unaffected.
	if rbacManager == nil {
		return nil
	}

	// Get token info from context
	tokenInfo := auth.GetTokenInfo(c)
	if tokenInfo == nil {
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
