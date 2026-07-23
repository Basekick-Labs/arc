package api

import (
	"errors"
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

// rbacErrorStatus maps an RBACManager error to an HTTP status code.
//
// Client-supplied bad input must not surface as 5xx: a 500 is what drives
// client retries, alerting, and error budgets, and it gives the caller no
// way to tell "the server broke" from "pick a different name". Detection
// uses errors.Is against the exported sentinels, so rewording any error
// these sentinels tag cannot change a status code (issue #549).
//
// Scope: this helper covers the conditions whose status code is the same
// at every call site — name collisions, name validation, and role input.
//
// ErrNotFound and ErrMissingField are deliberately NOT handled here.
// ErrMissingField only arises on create paths, and ErrNotFound is
// genuinely ambiguous: the same error means 404 when the missing entity
// is the request's target (PATCH /orgs/:id) but 400 when it is the parent
// of a create (POST /orgs/:org_id/teams). Centralising it would have to
// pick one and silently break the other, so each handler matches those
// two sentinels itself. See TestRBACNotFoundStatusContract, which pins
// the asymmetry.
//
// Returns 0 when the error is not a recognised client error, leaving the
// caller to apply its own default (500, or a handler-specific 404).
func rbacErrorStatus(err error) int {
	switch {
	case errors.Is(err, auth.ErrInvalidName), errors.Is(err, auth.ErrInvalidRoleInput):
		return fiber.StatusBadRequest
	case errors.Is(err, auth.ErrNameConflict):
		return fiber.StatusConflict
	case errors.Is(err, auth.ErrCascadeCapExceeded):
		// Not reachable from the current callers — the cascade cap is
		// only produced by the delete handlers, which match it directly.
		// Kept so the mapping stays in one place if a delete path is
		// ever routed through this helper.
		return fiber.StatusConflict
	}
	return 0
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

	org, err := h.rbacManager.CreateOrganization(c.UserContext(), &req)
	if err != nil {
		status := fiber.StatusInternalServerError
		if s := rbacErrorStatus(err); s != 0 {
			status = s
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

	err = h.rbacManager.UpdateOrganization(c.UserContext(), id, &req)
	if err != nil {
		if errors.Is(err, auth.ErrNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Organization not found",
			})
		}
		if s := rbacErrorStatus(err); s != 0 {
			return c.Status(s).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
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

	err = h.rbacManager.DeleteOrganization(c.UserContext(), id)
	if err != nil {
		if errors.Is(err, auth.ErrNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Organization not found",
			})
		}
		// Phase A.2 Item 2: cascade-on-delete soft cap. The proposer
		// refused because the descendant count exceeds
		// cluster.rbac.max_cascade_descendants. 409 Conflict is the
		// right HTTP code: the request conflicts with system state
		// (too many descendants for an atomic Raft cascade); operator
		// action required (delete sub-resources first, or raise the
		// cap if their tenant size justifies it).
		if errors.Is(err, auth.ErrCascadeCapExceeded) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
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

	team, err := h.rbacManager.CreateTeam(c.UserContext(), orgID, &req)
	if err != nil {
		status := fiber.StatusInternalServerError
		if errors.Is(err, auth.ErrNotFound) {
			// The missing entity is this create's PARENT, not its
			// target, and Arc has always reported that as 400. 404 is
			// arguably more correct, but changing it would break
			// existing clients — out of scope here. This is why
			// ErrNotFound is not mapped centrally in rbacErrorStatus.
			status = fiber.StatusBadRequest
		}
		if s := rbacErrorStatus(err); s != 0 {
			status = s
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

	err = h.rbacManager.UpdateTeam(c.UserContext(), id, &req)
	if err != nil {
		if errors.Is(err, auth.ErrNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Team not found",
			})
		}
		if s := rbacErrorStatus(err); s != 0 {
			return c.Status(s).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
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

	err = h.rbacManager.DeleteTeam(c.UserContext(), id)
	if err != nil {
		if errors.Is(err, auth.ErrNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Team not found",
			})
		}
		// Phase A.2 Item 2: cascade-on-delete soft cap (see deleteOrganization).
		if errors.Is(err, auth.ErrCascadeCapExceeded) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
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

	role, err := h.rbacManager.CreateRole(c.UserContext(), teamID, &req)
	if err != nil {
		status := fiber.StatusInternalServerError
		// ErrNotFound here is the parent team from the URL path, so this
		// is 400 rather than 404 — see createTeam for the same shape.
		if errors.Is(err, auth.ErrMissingField) || errors.Is(err, auth.ErrNotFound) {
			status = fiber.StatusBadRequest
		}
		if s := rbacErrorStatus(err); s != 0 {
			status = s
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
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

	err = h.rbacManager.UpdateRole(c.UserContext(), id, &req)
	if err != nil {
		if errors.Is(err, auth.ErrNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"success": false,
				"error":   "Role not found",
			})
		}
		if s := rbacErrorStatus(err); s != 0 {
			return c.Status(s).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
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

	err = h.rbacManager.DeleteRole(c.UserContext(), id)
	if err != nil {
		if errors.Is(err, auth.ErrNotFound) {
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

	perm, err := h.rbacManager.CreateMeasurementPermission(c.UserContext(), roleID, &req)
	if err != nil {
		status := fiber.StatusInternalServerError
		// ErrNotFound here is the parent role from the URL path, so this
		// is 400 rather than 404 — see createTeam for the same shape.
		if errors.Is(err, auth.ErrMissingField) || errors.Is(err, auth.ErrNotFound) {
			status = fiber.StatusBadRequest
		}
		if s := rbacErrorStatus(err); s != 0 {
			status = s
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
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

	err = h.rbacManager.DeleteMeasurementPermission(c.UserContext(), id)
	if err != nil {
		if errors.Is(err, auth.ErrNotFound) {
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
