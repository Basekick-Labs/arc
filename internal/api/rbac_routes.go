package api

import (
	"strconv"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// RBACHandler handles RBAC-related endpoints
type RBACHandler struct {
	authManager *auth.AuthManager
	rbacManager *auth.RBACManager
	logger      zerolog.Logger
}

// NewRBACHandler creates a new RBAC handler
func NewRBACHandler(authManager *auth.AuthManager, rbacManager *auth.RBACManager, logger zerolog.Logger) *RBACHandler {
	return &RBACHandler{
		authManager: authManager,
		rbacManager: rbacManager,
		logger:      logger.With().Str("component", "rbac-handler").Logger(),
	}
}

// requireRBACLicense is a middleware that checks if RBAC feature is licensed
func (h *RBACHandler) requireRBACLicense(c *fiber.Ctx) error {
	if !h.rbacManager.IsRBACEnabled() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "RBAC requires an enterprise license with the 'rbac' feature enabled",
		})
	}
	return c.Next()
}

// RegisterRoutes registers RBAC-related endpoints
func (h *RBACHandler) RegisterRoutes(app *fiber.App) {
	rbacGroup := app.Group("/api/v1/rbac")

	// All RBAC routes require admin permission and valid license
	rbacGroup.Use(auth.RequireAdmin(h.authManager))
	rbacGroup.Use(h.requireRBACLicense)

	// Organizations
	rbacGroup.Get("/organizations", h.listOrganizations)
	rbacGroup.Post("/organizations", h.createOrganization)
	rbacGroup.Get("/organizations/:id", h.getOrganization)
	rbacGroup.Patch("/organizations/:id", h.updateOrganization)
	rbacGroup.Delete("/organizations/:id", h.deleteOrganization)

	// Teams (nested under organizations)
	rbacGroup.Get("/organizations/:org_id/teams", h.listTeams)
	rbacGroup.Post("/organizations/:org_id/teams", h.createTeam)

	// Teams (direct access)
	rbacGroup.Get("/teams/:id", h.getTeam)
	rbacGroup.Patch("/teams/:id", h.updateTeam)
	rbacGroup.Delete("/teams/:id", h.deleteTeam)

	// Roles (nested under teams)
	rbacGroup.Get("/teams/:team_id/roles", h.listRoles)
	rbacGroup.Post("/teams/:team_id/roles", h.createRole)

	// Roles (direct access)
	rbacGroup.Get("/roles/:id", h.getRole)
	rbacGroup.Patch("/roles/:id", h.updateRole)
	rbacGroup.Delete("/roles/:id", h.deleteRole)

	// Measurement permissions (nested under roles)
	rbacGroup.Get("/roles/:role_id/measurements", h.listMeasurementPermissions)
	rbacGroup.Post("/roles/:role_id/measurements", h.createMeasurementPermission)

	// Measurement permissions (direct access for delete)
	rbacGroup.Delete("/measurement-permissions/:id", h.deleteMeasurementPermission)
}

// =============================================================================
// Organizations
// =============================================================================

// listOrganizations handles GET /api/v1/rbac/organizations
func (h *RBACHandler) listOrganizations(c *fiber.Ctx) error {
	orgs, err := h.rbacManager.ListOrganizations()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list organizations")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Internal server error",
		})
	}

	return c.JSON(fiber.Map{
		"success":       true,
		"organizations": orgs,
		"count":         len(orgs),
	})
}

// createOrganization handles POST /api/v1/rbac/organizations
func (h *RBACHandler) createOrganization(c *fiber.Ctx) error {
	var req auth.CreateOrganizationRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	org, err := h.rbacManager.CreateOrganization(&req)
	if err != nil {
		status := fiber.StatusInternalServerError
		if err.Error() == "organization name is required" {
			status = fiber.StatusBadRequest
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success":      true,
		"organization": org,
	})
}

// getOrganization handles GET /api/v1/rbac/organizations/:id
func (h *RBACHandler) getOrganization(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid organization ID",
		})
	}

	org, err := h.rbacManager.GetOrganization(id)
	if err != nil {
		h.logger.Error().Err(err).Int64("id", id).Msg("Failed to get organization")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Internal server error",
		})
	}

	if org == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Organization not found",
		})
	}

	// Load teams for this organization
	teams, err := h.rbacManager.ListTeamsByOrganization(id)
	if err != nil {
		h.logger.Warn().Err(err).Int64("id", id).Msg("Failed to load teams for organization")
	} else {
		org.Teams = teams
	}

	return c.JSON(fiber.Map{
		"success":      true,
		"organization": org,
	})
}

// updateOrganization handles PATCH /api/v1/rbac/organizations/:id
func (h *RBACHandler) updateOrganization(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid organization ID",
		})
	}

	var req auth.UpdateOrganizationRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	err = h.rbacManager.UpdateOrganization(id, &req)
	if err != nil {
		if err.Error() == "organization not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Organization not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Organization updated successfully",
	})
}

// deleteOrganization handles DELETE /api/v1/rbac/organizations/:id
func (h *RBACHandler) deleteOrganization(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid organization ID",
		})
	}

	err = h.rbacManager.DeleteOrganization(id)
	if err != nil {
		if err.Error() == "organization not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Organization not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Organization deleted successfully",
	})
}

// =============================================================================
// Teams
// =============================================================================

// listTeams handles GET /api/v1/rbac/organizations/:org_id/teams
func (h *RBACHandler) listTeams(c *fiber.Ctx) error {
	orgID, err := strconv.ParseInt(c.Params("org_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid organization ID",
		})
	}

	teams, err := h.rbacManager.ListTeamsByOrganization(orgID)
	if err != nil {
		h.logger.Error().Err(err).Int64("org_id", orgID).Msg("Failed to list teams")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Internal server error",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"teams":   teams,
		"count":   len(teams),
	})
}

// createTeam handles POST /api/v1/rbac/organizations/:org_id/teams
func (h *RBACHandler) createTeam(c *fiber.Ctx) error {
	orgID, err := strconv.ParseInt(c.Params("org_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid organization ID",
		})
	}

	var req auth.CreateTeamRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	team, err := h.rbacManager.CreateTeam(orgID, &req)
	if err != nil {
		status := fiber.StatusInternalServerError
		if err.Error() == "team name is required" || err.Error() == "organization not found" {
			status = fiber.StatusBadRequest
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success": true,
		"team":    team,
	})
}

// getTeam handles GET /api/v1/rbac/teams/:id
func (h *RBACHandler) getTeam(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid team ID",
		})
	}

	team, err := h.rbacManager.GetTeam(id)
	if err != nil {
		h.logger.Error().Err(err).Int64("id", id).Msg("Failed to get team")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Internal server error",
		})
	}

	if team == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Team not found",
		})
	}

	// Load roles for this team
	roles, err := h.rbacManager.ListRolesByTeam(id)
	if err != nil {
		h.logger.Warn().Err(err).Int64("id", id).Msg("Failed to load roles for team")
	} else {
		team.Roles = roles
	}

	return c.JSON(fiber.Map{
		"success": true,
		"team":    team,
	})
}

// updateTeam handles PATCH /api/v1/rbac/teams/:id
func (h *RBACHandler) updateTeam(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid team ID",
		})
	}

	var req auth.UpdateTeamRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	err = h.rbacManager.UpdateTeam(id, &req)
	if err != nil {
		if err.Error() == "team not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Team not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Team updated successfully",
	})
}

// deleteTeam handles DELETE /api/v1/rbac/teams/:id
func (h *RBACHandler) deleteTeam(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid team ID",
		})
	}

	err = h.rbacManager.DeleteTeam(id)
	if err != nil {
		if err.Error() == "team not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Team not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Team deleted successfully",
	})
}

// =============================================================================
// Roles
// =============================================================================

// listRoles handles GET /api/v1/rbac/teams/:team_id/roles
func (h *RBACHandler) listRoles(c *fiber.Ctx) error {
	teamID, err := strconv.ParseInt(c.Params("team_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid team ID",
		})
	}

	roles, err := h.rbacManager.ListRolesByTeam(teamID)
	if err != nil {
		h.logger.Error().Err(err).Int64("team_id", teamID).Msg("Failed to list roles")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Internal server error",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"roles":   roles,
		"count":   len(roles),
	})
}

// createRole handles POST /api/v1/rbac/teams/:team_id/roles
func (h *RBACHandler) createRole(c *fiber.Ctx) error {
	teamID, err := strconv.ParseInt(c.Params("team_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid team ID",
		})
	}

	var req auth.CreateRoleRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	role, err := h.rbacManager.CreateRole(teamID, &req)
	if err != nil {
		status := fiber.StatusInternalServerError
		errMsg := err.Error()
		if errMsg == "database pattern is required" ||
			errMsg == "at least one permission is required" ||
			errMsg == "team not found" ||
			len(errMsg) > 20 && errMsg[:20] == "invalid permission: " {
			status = fiber.StatusBadRequest
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   errMsg,
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success": true,
		"role":    role,
	})
}

// getRole handles GET /api/v1/rbac/roles/:id
func (h *RBACHandler) getRole(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid role ID",
		})
	}

	role, err := h.rbacManager.GetRole(id)
	if err != nil {
		h.logger.Error().Err(err).Int64("id", id).Msg("Failed to get role")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Internal server error",
		})
	}

	if role == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Role not found",
		})
	}

	// Load measurement permissions for this role
	measPerms, err := h.rbacManager.ListMeasurementPermissionsByRole(id)
	if err != nil {
		h.logger.Warn().Err(err).Int64("id", id).Msg("Failed to load measurement permissions for role")
	} else {
		role.MeasurementPermissions = measPerms
	}

	return c.JSON(fiber.Map{
		"success": true,
		"role":    role,
	})
}

// updateRole handles PATCH /api/v1/rbac/roles/:id
func (h *RBACHandler) updateRole(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid role ID",
		})
	}

	var req auth.UpdateRoleRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	err = h.rbacManager.UpdateRole(id, &req)
	if err != nil {
		if err.Error() == "role not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Role not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Role updated successfully",
	})
}

// deleteRole handles DELETE /api/v1/rbac/roles/:id
func (h *RBACHandler) deleteRole(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid role ID",
		})
	}

	err = h.rbacManager.DeleteRole(id)
	if err != nil {
		if err.Error() == "role not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Role not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Role deleted successfully",
	})
}

// =============================================================================
// Measurement Permissions
// =============================================================================

// listMeasurementPermissions handles GET /api/v1/rbac/roles/:role_id/measurements
func (h *RBACHandler) listMeasurementPermissions(c *fiber.Ctx) error {
	roleID, err := strconv.ParseInt(c.Params("role_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid role ID",
		})
	}

	perms, err := h.rbacManager.ListMeasurementPermissionsByRole(roleID)
	if err != nil {
		h.logger.Error().Err(err).Int64("role_id", roleID).Msg("Failed to list measurement permissions")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Internal server error",
		})
	}

	return c.JSON(fiber.Map{
		"success":                 true,
		"measurement_permissions": perms,
		"count":                   len(perms),
	})
}

// createMeasurementPermission handles POST /api/v1/rbac/roles/:role_id/measurements
func (h *RBACHandler) createMeasurementPermission(c *fiber.Ctx) error {
	roleID, err := strconv.ParseInt(c.Params("role_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid role ID",
		})
	}

	var req auth.CreateMeasurementPermissionRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	perm, err := h.rbacManager.CreateMeasurementPermission(roleID, &req)
	if err != nil {
		status := fiber.StatusInternalServerError
		errMsg := err.Error()
		if errMsg == "measurement pattern is required" ||
			errMsg == "at least one permission is required" ||
			errMsg == "role not found" ||
			len(errMsg) > 20 && errMsg[:20] == "invalid permission: " {
			status = fiber.StatusBadRequest
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   errMsg,
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success":                true,
		"measurement_permission": perm,
	})
}

// deleteMeasurementPermission handles DELETE /api/v1/rbac/measurement-permissions/:id
func (h *RBACHandler) deleteMeasurementPermission(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid measurement permission ID",
		})
	}

	err = h.rbacManager.DeleteMeasurementPermission(id)
	if err != nil {
		if err.Error() == "measurement permission not found" {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Measurement permission not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Measurement permission deleted successfully",
	})
}
