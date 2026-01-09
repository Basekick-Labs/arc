package api

import (
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// AuthHandler handles authentication-related endpoints
type AuthHandler struct {
	authManager *auth.AuthManager
	rbacManager *auth.RBACManager
	logger      zerolog.Logger
}

// NewAuthHandler creates a new auth handler
func NewAuthHandler(authManager *auth.AuthManager, logger zerolog.Logger) *AuthHandler {
	return &AuthHandler{
		authManager: authManager,
		logger:      logger.With().Str("component", "auth-handler").Logger(),
	}
}

// SetRBACManager sets the RBAC manager for token membership endpoints
func (h *AuthHandler) SetRBACManager(rbacManager *auth.RBACManager) {
	h.rbacManager = rbacManager
}

// RegisterRoutes registers auth-related endpoints
func (h *AuthHandler) RegisterRoutes(app *fiber.App) {
	authGroup := app.Group("/api/v1/auth")

	// Public endpoint - verify token
	authGroup.Get("/verify", h.verifyToken)

	// Protected endpoints - require admin permission
	authGroup.Get("/tokens", auth.RequireAdmin(h.authManager), h.listTokens)
	authGroup.Post("/tokens", auth.RequireAdmin(h.authManager), h.createToken)
	authGroup.Get("/tokens/:id", auth.RequireAdmin(h.authManager), h.getToken)
	authGroup.Patch("/tokens/:id", auth.RequireAdmin(h.authManager), h.updateToken)
	authGroup.Delete("/tokens/:id", auth.RequireAdmin(h.authManager), h.deleteToken)
	authGroup.Post("/tokens/:id/rotate", auth.RequireAdmin(h.authManager), h.rotateToken)
	authGroup.Post("/tokens/:id/revoke", auth.RequireAdmin(h.authManager), h.revokeToken)

	// Cache management
	authGroup.Get("/cache/stats", auth.RequireAdmin(h.authManager), h.getCacheStats)
	authGroup.Post("/cache/invalidate", auth.RequireAdmin(h.authManager), h.invalidateCache)
}

// verifyToken handles GET /api/v1/auth/verify
func (h *AuthHandler) verifyToken(c *fiber.Ctx) error {
	// Extract token from request headers
	token := auth.ExtractTokenFromRequest(c)

	if token == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"valid": false,
			"error": "No token provided",
		})
	}

	// Verify the token
	tokenInfo := h.authManager.VerifyToken(token)
	if tokenInfo == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"valid": false,
			"error": "Invalid or expired token",
		})
	}

	return c.JSON(fiber.Map{
		"valid":       true,
		"token_info":  tokenInfo,
		"permissions": tokenInfo.Permissions,
	})
}

// CreateTokenRequest represents a token creation request
type CreateTokenRequest struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Permissions *[]string `json:"permissions,omitempty"` // nil = default (read,write), empty array = no permissions (RBAC-only)
	ExpiresIn   string   `json:"expires_in,omitempty"` // e.g., "24h", "7d", "30d"
}

// createToken handles POST /api/v1/auth/tokens
func (h *AuthHandler) createToken(c *fiber.Ctx) error {
	var req CreateTokenRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Token name is required",
		})
	}

	// Parse permissions
	// nil = use defaults (read,write), empty array = no permissions (RBAC-only token)
	permissions := "read,write"
	if req.Permissions != nil {
		if len(*req.Permissions) == 0 {
			permissions = "" // RBAC-only token with no OSS permissions
		} else {
			permissions = ""
			for i, p := range *req.Permissions {
				if i > 0 {
					permissions += ","
				}
				permissions += p
			}
		}
	}

	// Parse expiration
	var expiresAt *time.Time
	if req.ExpiresIn != "" {
		duration, err := time.ParseDuration(req.ExpiresIn)
		if err != nil {
			// Try parsing as days (e.g., "7d")
			if len(req.ExpiresIn) > 1 && req.ExpiresIn[len(req.ExpiresIn)-1] == 'd' {
				days, err := strconv.Atoi(req.ExpiresIn[:len(req.ExpiresIn)-1])
				if err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
						"success": false,
						"error":   "Invalid expires_in format. Use duration like '24h' or '7d'",
					})
				}
				duration = time.Duration(days) * 24 * time.Hour
			} else {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"success": false,
					"error":   "Invalid expires_in format. Use duration like '24h' or '7d'",
				})
			}
		}
		t := time.Now().Add(duration)
		expiresAt = &t
	}

	token, err := h.authManager.CreateToken(req.Name, req.Description, permissions, expiresAt)
	if err != nil {
		h.logger.Error().Err(err).Str("name", req.Name).Msg("Failed to create token")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	h.logger.Info().Str("name", req.Name).Msg("Created new API token")

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success": true,
		"token":   token,
		"message": "Token created successfully. Store this token securely - it cannot be retrieved again.",
	})
}

// listTokens handles GET /api/v1/auth/tokens
func (h *AuthHandler) listTokens(c *fiber.Ctx) error {
	tokens, err := h.authManager.ListTokens()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list tokens")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to list tokens: " + err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"tokens":  tokens,
		"count":   len(tokens),
	})
}

// getToken handles GET /api/v1/auth/tokens/:id
func (h *AuthHandler) getToken(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	tokenInfo, err := h.authManager.GetTokenByID(id)
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", id).Msg("Failed to get token")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get token: " + err.Error(),
		})
	}

	if tokenInfo == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Token not found",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"token":   tokenInfo,
	})
}

// UpdateTokenRequest represents a token update request
type UpdateTokenRequest struct {
	Name        *string   `json:"name,omitempty"`
	Description *string   `json:"description,omitempty"`
	Permissions *[]string `json:"permissions,omitempty"` // nil = don't change, empty array = clear permissions (RBAC-only)
	ExpiresIn   *string   `json:"expires_in,omitempty"`
}

// updateToken handles PATCH /api/v1/auth/tokens/:id
func (h *AuthHandler) updateToken(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	var req UpdateTokenRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	// Convert permissions array to string if provided
	// nil = don't change, empty array = clear permissions (RBAC-only token)
	var permissions *string
	if req.Permissions != nil {
		permStr := ""
		if len(*req.Permissions) > 0 {
			for i, p := range *req.Permissions {
				if i > 0 {
					permStr += ","
				}
				permStr += p
			}
		}
		permissions = &permStr
	}

	// Parse expiration if provided
	var expiresAt *time.Time
	if req.ExpiresIn != nil {
		duration, err := time.ParseDuration(*req.ExpiresIn)
		if err != nil {
			if len(*req.ExpiresIn) > 1 && (*req.ExpiresIn)[len(*req.ExpiresIn)-1] == 'd' {
				days, err := strconv.Atoi((*req.ExpiresIn)[:len(*req.ExpiresIn)-1])
				if err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
						"success": false,
						"error":   "Invalid expires_in format",
					})
				}
				duration = time.Duration(days) * 24 * time.Hour
			} else {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"success": false,
					"error":   "Invalid expires_in format",
				})
			}
		}
		t := time.Now().Add(duration)
		expiresAt = &t
	}

	err = h.authManager.UpdateToken(id, req.Name, req.Description, permissions, expiresAt)
	if err != nil {
		if err.Error() == "token not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Token not found",
			})
		}
		h.logger.Error().Err(err).Int64("token_id", id).Msg("Failed to update token")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to update token: " + err.Error(),
		})
	}

	h.logger.Info().Int64("token_id", id).Msg("Updated API token")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Token updated successfully",
	})
}

// deleteToken handles DELETE /api/v1/auth/tokens/:id
func (h *AuthHandler) deleteToken(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	err = h.authManager.DeleteToken(id)
	if err != nil {
		if err.Error() == "token not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Token not found",
			})
		}
		h.logger.Error().Err(err).Int64("token_id", id).Msg("Failed to delete token")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to delete token: " + err.Error(),
		})
	}

	h.logger.Info().Int64("token_id", id).Msg("Deleted API token")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Token deleted successfully",
	})
}

// rotateToken handles POST /api/v1/auth/tokens/:id/rotate
func (h *AuthHandler) rotateToken(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	newToken, err := h.authManager.RotateToken(id)
	if err != nil {
		if err.Error() == "token not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Token not found",
			})
		}
		h.logger.Error().Err(err).Int64("token_id", id).Msg("Failed to rotate token")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to rotate token: " + err.Error(),
		})
	}

	h.logger.Info().Int64("token_id", id).Msg("Rotated API token")

	return c.JSON(fiber.Map{
		"success":   true,
		"new_token": newToken,
		"message":   "Token rotated successfully. Store this new token securely - it cannot be retrieved again.",
	})
}

// revokeToken handles POST /api/v1/auth/tokens/:id/revoke
func (h *AuthHandler) revokeToken(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	err = h.authManager.RevokeToken(id)
	if err != nil {
		if err.Error() == "token not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Token not found",
			})
		}
		h.logger.Error().Err(err).Int64("token_id", id).Msg("Failed to revoke token")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to revoke token: " + err.Error(),
		})
	}

	h.logger.Info().Int64("token_id", id).Msg("Revoked API token")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Token revoked successfully",
	})
}

// getCacheStats handles GET /api/v1/auth/cache/stats
func (h *AuthHandler) getCacheStats(c *fiber.Ctx) error {
	stats := h.authManager.GetCacheStats()
	return c.JSON(fiber.Map{
		"success": true,
		"stats":   stats,
	})
}

// invalidateCache handles POST /api/v1/auth/cache/invalidate
func (h *AuthHandler) invalidateCache(c *fiber.Ctx) error {
	h.authManager.InvalidateCache()
	return c.JSON(fiber.Map{
		"success": true,
		"message": "Cache invalidated successfully",
	})
}

// RegisterTokenMembershipRoutes registers token membership endpoints (requires RBAC manager)
func (h *AuthHandler) RegisterTokenMembershipRoutes(app *fiber.App) {
	if h.rbacManager == nil {
		h.logger.Warn().Msg("RBAC manager not set, skipping token membership routes")
		return
	}

	authGroup := app.Group("/api/v1/auth")

	// Token membership endpoints - require admin permission
	authGroup.Get("/tokens/:id/teams", auth.RequireAdmin(h.authManager), h.getTokenTeams)
	authGroup.Post("/tokens/:id/teams", auth.RequireAdmin(h.authManager), h.addTokenToTeam)
	authGroup.Delete("/tokens/:id/teams/:team_id", auth.RequireAdmin(h.authManager), h.removeTokenFromTeam)
	authGroup.Get("/tokens/:id/permissions", auth.RequireAdmin(h.authManager), h.getEffectivePermissions)
}

// getTokenTeams handles GET /api/v1/auth/tokens/:id/teams
func (h *AuthHandler) getTokenTeams(c *fiber.Ctx) error {
	if h.rbacManager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"success": false,
			"error":   "RBAC is not enabled",
		})
	}

	tokenID, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	// Verify token exists
	tokenInfo, err := h.authManager.GetTokenByID(tokenID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get token: " + err.Error(),
		})
	}
	if tokenInfo == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Token not found",
		})
	}

	teams, err := h.rbacManager.GetTokenTeams(tokenID)
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to get token teams")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get token teams: " + err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"teams":   teams,
		"count":   len(teams),
	})
}

// addTokenToTeam handles POST /api/v1/auth/tokens/:id/teams
func (h *AuthHandler) addTokenToTeam(c *fiber.Ctx) error {
	if h.rbacManager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"success": false,
			"error":   "RBAC is not enabled",
		})
	}

	if !h.rbacManager.IsRBACEnabled() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "RBAC requires an enterprise license with the 'rbac' feature enabled",
		})
	}

	tokenID, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	var req auth.AddTokenToTeamRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	// Verify token exists
	tokenInfo, err := h.authManager.GetTokenByID(tokenID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get token: " + err.Error(),
		})
	}
	if tokenInfo == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Token not found",
		})
	}

	membership, err := h.rbacManager.AddTokenToTeam(tokenID, req.TeamID)
	if err != nil {
		status := fiber.StatusInternalServerError
		if err.Error() == "team not found" || err.Error() == "token is already a member of this team" {
			status = fiber.StatusBadRequest
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success":    true,
		"membership": membership,
	})
}

// removeTokenFromTeam handles DELETE /api/v1/auth/tokens/:id/teams/:team_id
func (h *AuthHandler) removeTokenFromTeam(c *fiber.Ctx) error {
	if h.rbacManager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"success": false,
			"error":   "RBAC is not enabled",
		})
	}

	if !h.rbacManager.IsRBACEnabled() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "RBAC requires an enterprise license with the 'rbac' feature enabled",
		})
	}

	tokenID, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	teamID, err := strconv.ParseInt(c.Params("team_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid team ID",
		})
	}

	err = h.rbacManager.RemoveTokenFromTeam(tokenID, teamID)
	if err != nil {
		if err.Error() == "token membership not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Token is not a member of this team",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Token removed from team successfully",
	})
}

// getEffectivePermissions handles GET /api/v1/auth/tokens/:id/permissions
func (h *AuthHandler) getEffectivePermissions(c *fiber.Ctx) error {
	if h.rbacManager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"success": false,
			"error":   "RBAC is not enabled",
		})
	}

	tokenID, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token ID",
		})
	}

	// Get token info
	tokenInfo, err := h.authManager.GetTokenByID(tokenID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get token: " + err.Error(),
		})
	}
	if tokenInfo == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Token not found",
		})
	}

	perms, err := h.rbacManager.GetEffectivePermissions(tokenID, tokenInfo)
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to get effective permissions")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get effective permissions: " + err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success":     true,
		"permissions": perms,
		"rbac_enabled": h.rbacManager.IsRBACEnabled(),
	})
}
