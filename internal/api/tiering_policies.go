package api

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TieringPoliciesHandler handles tiering policy API operations
type TieringPoliciesHandler struct {
	manager       *tiering.Manager
	authManager   *auth.AuthManager
	licenseClient *license.Client
	logger        zerolog.Logger
}

// NewTieringPoliciesHandler creates a new tiering policies handler
func NewTieringPoliciesHandler(manager *tiering.Manager, authManager *auth.AuthManager, licenseClient *license.Client, logger zerolog.Logger) *TieringPoliciesHandler {
	return &TieringPoliciesHandler{
		manager:       manager,
		authManager:   authManager,
		licenseClient: licenseClient,
		logger:        logger.With().Str("component", "tiering-policies-api").Logger(),
	}
}

// PolicyRequest represents a request to create/update a tiering policy
// 2-tier system: data older than HotMaxAgeDays moves from hot to cold
type PolicyRequest struct {
	HotOnly       bool `json:"hot_only,omitempty"`         // Exclude from tiering entirely
	HotMaxAgeDays *int `json:"hot_max_age_days,omitempty"` // nil = use global default
}

// ListPolicies returns all custom tiering policies
// GET /api/v1/tiering/policies
func (h *TieringPoliciesHandler) ListPolicies(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	policies, err := h.manager.GetPolicies().List(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list tiering policies")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list tiering policies",
		})
	}

	return c.JSON(fiber.Map{
		"policies": policies,
		"count":    len(policies),
	})
}

// GetPolicy returns the policy for a specific database
// GET /api/v1/tiering/policies/:database
func (h *TieringPoliciesHandler) GetPolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	database := c.Params("database")
	if database == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name is required",
		})
	}

	policy, err := h.manager.GetPolicies().Get(ctx, database)
	if err != nil {
		h.logger.Error().Err(err).Str("database", database).Msg("Failed to get tiering policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get tiering policy",
		})
	}

	if policy == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error":   "No custom policy for this database",
			"message": "Using global defaults",
		})
	}

	return c.JSON(policy)
}

// SetPolicy creates or updates the policy for a database
// PUT /api/v1/tiering/policies/:database
func (h *TieringPoliciesHandler) SetPolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	database := c.Params("database")
	if database == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name is required",
		})
	}

	var req PolicyRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	policy := &tiering.DatabasePolicy{
		Database:      database,
		HotOnly:       req.HotOnly,
		HotMaxAgeDays: req.HotMaxAgeDays,
	}

	if err := h.manager.GetPolicies().Set(ctx, policy); err != nil {
		h.logger.Error().Err(err).Str("database", database).Msg("Failed to set tiering policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to set tiering policy",
		})
	}

	h.logger.Info().
		Str("database", database).
		Bool("hot_only", req.HotOnly).
		Msg("Tiering policy updated")

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message":  "Policy updated successfully",
		"database": database,
		"policy":   policy,
	})
}

// DeletePolicy removes the custom policy for a database (reverts to global defaults)
// DELETE /api/v1/tiering/policies/:database
func (h *TieringPoliciesHandler) DeletePolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	database := c.Params("database")
	if database == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name is required",
		})
	}

	if err := h.manager.GetPolicies().Delete(ctx, database); err != nil {
		h.logger.Error().Err(err).Str("database", database).Msg("Failed to delete tiering policy")
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	h.logger.Info().
		Str("database", database).
		Msg("Tiering policy deleted, reverting to global defaults")

	return c.JSON(fiber.Map{
		"message":  "Policy deleted, database now uses global defaults",
		"database": database,
	})
}

// GetEffectivePolicy returns the effective policy for a database (resolved with global defaults)
// GET /api/v1/tiering/policies/:database/effective
func (h *TieringPoliciesHandler) GetEffectivePolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	database := c.Params("database")
	if database == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name is required",
		})
	}

	effective := h.manager.GetEffectivePolicy(ctx, database)
	return c.JSON(effective)
}

// requireTieringLicense checks that the license includes the tiering feature.
func (h *TieringPoliciesHandler) requireTieringLicense(c *fiber.Ctx) error {
	if h.licenseClient == nil || !h.licenseClient.CanUseTieredStorage() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "Tiered storage requires an enterprise license with the 'tiering' feature enabled",
		})
	}
	return c.Next()
}

// RegisterRoutes registers tiering policy API routes
func (h *TieringPoliciesHandler) RegisterRoutes(app fiber.Router) {
	policies := app.Group("/api/v1/tiering/policies")
	if h.authManager != nil {
		policies.Use(auth.RequireAdmin(h.authManager))
	}
	policies.Use(h.requireTieringLicense)

	policies.Get("/", h.ListPolicies)
	policies.Get("/:database", h.GetPolicy)
	policies.Put("/:database", h.SetPolicy)
	policies.Delete("/:database", h.DeletePolicy)
	policies.Get("/:database/effective", h.GetEffectivePolicy)
}
