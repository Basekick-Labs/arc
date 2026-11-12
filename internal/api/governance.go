package api

import (
	"context"
	"errors"
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
	ctx, cancel := context.WithTimeout(c.UserContext(), 30*time.Second)
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

	policy, err := h.manager.CreatePolicyFor(ctx, &req, h.tokenFP(req.TokenID))
	if errors.Is(err, governance.ErrPolicyNotFound) {
		// Created in the cluster, and deleted again before it could be read
		// back here.
		return policyNotFound(c)
	}
	if errors.Is(err, governance.ErrPolicyExists) {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"success": false,
			"error":   "A policy already exists for this token",
		})
	}
	if handled, resp := h.clusterError(c, err, req.TokenID); handled {
		return resp
	}
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
	ctx, cancel := context.WithTimeout(c.UserContext(), 30*time.Second)
	defer cancel()

	policies, err := h.manager.ListPolicies(ctx)
	if handled, resp := h.clusterError(c, err, 0); handled {
		return resp
	}
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
	ctx, cancel := context.WithTimeout(c.UserContext(), 30*time.Second)
	defer cancel()

	tokenID, err := strconv.ParseInt(c.Params("token_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token_id",
		})
	}

	policy, err := h.manager.GetPolicyFor(ctx, tokenID, h.tokenFP(tokenID))
	if handled, resp := h.clusterError(c, err, tokenID); handled {
		return resp
	}
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to get governance policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get policy",
		})
	}

	if policy == nil {
		return policyNotFound(c)
	}

	return c.JSON(fiber.Map{
		"success": true,
		"policy":  policy,
	})
}

// UpdatePolicy updates the governance policy for a specific token.
// PUT /api/v1/governance/policies/:token_id
func (h *GovernanceHandler) UpdatePolicy(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.UserContext(), 30*time.Second)
	defer cancel()

	tokenID, err := strconv.ParseInt(c.Params("token_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token_id",
		})
	}

	// Verify policy exists
	tokenFP := h.tokenFP(tokenID)
	existing, err := h.manager.GetPolicyFor(ctx, tokenID, tokenFP)
	if handled, resp := h.clusterError(c, err, tokenID); handled {
		return resp
	}
	if err != nil {
		h.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to get governance policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get policy",
		})
	}
	if existing == nil {
		return policyNotFound(c)
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

	policy, err := h.manager.UpdatePolicyFor(ctx, &req, tokenFP)
	if errors.Is(err, governance.ErrPolicyNotFound) {
		return policyNotFound(c)
	}
	if handled, resp := h.clusterError(c, err, tokenID); handled {
		return resp
	}
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
	ctx, cancel := context.WithTimeout(c.UserContext(), 30*time.Second)
	defer cancel()

	tokenID, err := strconv.ParseInt(c.Params("token_id"), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid token_id",
		})
	}

	if err := h.manager.DeletePolicyFor(ctx, tokenID, h.tokenFP(tokenID)); err != nil {
		if errors.Is(err, governance.ErrPolicyNotFound) {
			return policyNotFound(c)
		}
		if handled, resp := h.clusterError(c, err, tokenID); handled {
			return resp
		}
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

// tokenFP is the fingerprint of token id's stored hash on this node, which
// names the token to the cluster's policies (#1627): an ID alone can be a
// pre-cluster local token sharing a cluster token's ID. "" outside a cluster,
// without auth, or for a token this node does not hold.
func (h *GovernanceHandler) tokenFP(id int64) string {
	if h.authManager == nil || !h.manager.ClusterMode() {
		return ""
	}
	info, err := h.authManager.GetTokenByID(id)
	if err != nil || info == nil {
		return ""
	}
	return info.Fingerprint
}

// clusterError answers the errors of the cluster-wide policies (#1627);
// handled is false for any other error.
func (h *GovernanceHandler) clusterError(c *fiber.Ctx, err error, tokenID int64) (handled bool, resp error) {
	var status int
	msg := ""
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, governance.ErrPoliciesBeingAdopted), errors.Is(err, governance.ErrNotCaughtUp):
		status, msg = fiber.StatusServiceUnavailable, err.Error()
	case errors.Is(err, governance.ErrClusterUnavailable):
		h.logger.Warn().Err(err).Int64("token_id", tokenID).Msg("The cluster did not take the governance policy write")
		status, msg = fiber.StatusServiceUnavailable, governance.ErrClusterUnavailable.Error()
	case errors.Is(err, governance.ErrTokenNotCluster):
		status, msg = fiber.StatusNotFound, "token "+strconv.FormatInt(tokenID, 10)+" is not a cluster token; governance policies apply to cluster tokens only"
	case errors.Is(err, governance.ErrInvalidPolicy):
		status, msg = fiber.StatusBadRequest, err.Error()
	default:
		return false, nil
	}
	if status == fiber.StatusServiceUnavailable {
		c.Set("Retry-After", "2")
	}
	return true, c.Status(status).JSON(fiber.Map{"success": false, "error": msg})
}

// policyNotFound answers a route naming a token that has no policy.
func policyNotFound(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
		"success": false,
		"error":   "No policy found for this token",
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

	tokenFP := h.tokenFP(tokenID)
	usage := h.manager.GetTokenUsageFor(tokenID, tokenFP)
	policy := h.manager.GetCachedPolicyFor(tokenID, tokenFP)

	return c.JSON(fiber.Map{
		"success": true,
		"usage":   usage,
		"policy":  policy,
	})
}
