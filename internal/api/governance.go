package api

import (
	"context"
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// GovernanceHandler handles governance policy API operations.
type GovernanceHandler struct {
	manager       *governance.Manager
	authManager   *auth.AuthManager
	licenseClient *license.Client
	logger        zerolog.Logger
}

// NewGovernanceHandler creates a new governance handler.
func NewGovernanceHandler(manager *governance.Manager, authManager *auth.AuthManager, licenseClient *license.Client, logger zerolog.Logger) *GovernanceHandler {
	return &GovernanceHandler{
		manager:       manager,
		authManager:   authManager,
		licenseClient: licenseClient,
		logger:        logger.With().Str("component", "governance-api").Logger(),
	}
}

// RegisterRoutes registers governance API routes.
func (h *GovernanceHandler) RegisterRoutes(app fiber.Router) {
	group := app.Group("/api/v1/governance")
	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}
	group.Use(h.requireGovernanceLicense)

	group.Post("/policies", h.CreatePolicy)
	group.Get("/policies", h.ListPolicies)
	group.Get("/policies/:token_id", h.GetPolicy)
	group.Put("/policies/:token_id", h.UpdatePolicy)
	group.Delete("/policies/:token_id", h.DeletePolicy)
	group.Get("/usage/:token_id", h.GetUsage)
}

// requireGovernanceLicense checks that the license includes the query governance feature.
func (h *GovernanceHandler) requireGovernanceLicense(c *fiber.Ctx) error {
	if h.licenseClient == nil || !h.licenseClient.CanUseQueryGovernance() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "Query governance requires an enterprise license with the 'query_governance' feature enabled",
		})
	}
	return c.Next()
}

// CreatePolicy creates a governance policy for a token.
// POST /api/v1/governance/policies
func (h *GovernanceHandler) CreatePolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	var req governance.Policy
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	if req.TokenID <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "token_id is required and must be positive",
		})
	}

	if req.RateLimitPerMinute < 0 || req.RateLimitPerHour < 0 ||
		req.MaxQueriesPerHour < 0 || req.MaxQueriesPerDay < 0 ||
		req.MaxRowsPerQuery < 0 || req.MaxScanDurationSec < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Limit values must be non-negative (0 = unlimited)",
		})
	}

	policy, err := h.manager.CreatePolicy(ctx, &req)
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", req.TokenID).Msg("Failed to create governance policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to create policy",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success": true,
		"policy":  policy,
	})
}

// ListPolicies returns all governance policies.
// GET /api/v1/governance/policies
func (h *GovernanceHandler) ListPolicies(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	policies, err := h.manager.ListPolicies(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list governance policies")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to list policies",
		})
	}

	return c.JSON(fiber.Map{
		"success":  true,
		"policies": policies,
		"count":    len(policies),
	})
}

// GetPolicy returns the governance policy for a specific token.
// GET /api/v1/governance/policies/:token_id
func (h *GovernanceHandler) GetPolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	tokenID, err := strconv.ParseInt(c.Params("token_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token_id",
		})
	}

	policy, err := h.manager.GetPolicy(ctx, tokenID)
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to get governance policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get policy",
		})
	}

	if policy == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "No policy found for this token",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"policy":  policy,
	})
}

// UpdatePolicy updates the governance policy for a specific token.
// PUT /api/v1/governance/policies/:token_id
func (h *GovernanceHandler) UpdatePolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	tokenID, err := strconv.ParseInt(c.Params("token_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token_id",
		})
	}

	// Verify policy exists
	existing, err := h.manager.GetPolicy(ctx, tokenID)
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to get governance policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get policy",
		})
	}
	if existing == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "No policy found for this token",
		})
	}

	var req governance.Policy
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	if req.RateLimitPerMinute < 0 || req.RateLimitPerHour < 0 ||
		req.MaxQueriesPerHour < 0 || req.MaxQueriesPerDay < 0 ||
		req.MaxRowsPerQuery < 0 || req.MaxScanDurationSec < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Limit values must be non-negative (0 = unlimited)",
		})
	}

	req.TokenID = tokenID
	req.ID = existing.ID
	req.CreatedAt = existing.CreatedAt

	policy, err := h.manager.UpdatePolicy(ctx, &req)
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to update governance policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to update policy",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"policy":  policy,
	})
}

// DeletePolicy removes the governance policy for a specific token.
// DELETE /api/v1/governance/policies/:token_id
func (h *GovernanceHandler) DeletePolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	tokenID, err := strconv.ParseInt(c.Params("token_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token_id",
		})
	}

	if err := h.manager.DeletePolicy(ctx, tokenID); err != nil {
		h.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to delete governance policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to delete policy",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Policy deleted",
	})
}

// GetUsage returns current usage statistics for a specific token.
// GET /api/v1/governance/usage/:token_id
func (h *GovernanceHandler) GetUsage(c *fiber.Ctx) error {
	tokenID, err := strconv.ParseInt(c.Params("token_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token_id",
		})
	}

	usage := h.manager.GetTokenUsage(tokenID)
	policy := h.manager.GetCachedPolicy(tokenID)

	return c.JSON(fiber.Map{
		"success": true,
		"usage":   usage,
		"policy":  policy,
	})
}
