package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/gofiber/fiber/v2"
)

// adoptPoliciesRequest is the body of POST /api/v1/cluster/policies/adopt.
type adoptPoliciesRequest struct {
	Kind string `json:"kind"`
	Node string `json:"node"`
	Hash string `json:"hash"`
}

// clusterPoliciesError answers the leader-only policy endpoints' errors.
func (h *ClusterHandler) clusterPoliciesError(c *fiber.Ctx, err error) error {
	status := fiber.StatusServiceUnavailable
	msg := err.Error()
	switch {
	case errors.Is(err, cluster.ErrNotLeaderForTopology):
		status = fiber.StatusConflict
		msg = "run this on the Raft leader"
	case errors.Is(err, cluster.ErrPolicyAdoptionNotNeeded), errors.Is(err, cluster.ErrRetentionPoliciesNotEnabled), errors.Is(err, cluster.ErrCQPoliciesNotEnabled):
		status = fiber.StatusConflict
	case errors.Is(err, cluster.ErrUnknownPolicyNode):
		status = fiber.StatusBadRequest
	}
	return c.Status(status).JSON(fiber.Map{"success": false, "error": msg, "leader": h.coordinator.LeaderID()})
}

// handleGetPolicyCandidates returns each node's own retention policies, read
// live, with the hash POST /adopt names. Until the cluster adopts them each
// node runs its own (the primary's), as before.
func (h *ClusterHandler) handleGetPolicyCandidates(c *fiber.Ctx) error {
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}
	// ?kind= narrows it to one kind; by default both are read.
	kind := c.Query("kind")
	if kind != "" && kind != "retention" && kind != "cq" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"success": false, "error": `kind must be "retention" or "cq"`})
	}
	resp := fiber.Map{"success": true}
	if kind == "" || kind == "retention" {
		out, err := h.coordinator.RetentionCandidates(c.UserContext())
		if err != nil && (kind != "" || !errors.Is(err, cluster.ErrRetentionPoliciesNotEnabled)) {
			return h.clusterPoliciesError(c, err)
		}
		if err == nil {
			resp["retention"] = out
		}
	}
	if kind == "" || kind == "cq" {
		out, err := h.coordinator.CQCandidates(c.UserContext())
		if err != nil && (kind != "" || !errors.Is(err, cluster.ErrCQPoliciesNotEnabled)) {
			return h.clusterPoliciesError(c, err)
		}
		if err == nil {
			resp["cq"] = out
		}
	}
	if resp["retention"] == nil && resp["cq"] == nil {
		return h.clusterPoliciesError(c, cluster.ErrRetentionPoliciesNotEnabled)
	}
	return c.JSON(resp)
}

// handleAdoptPolicies chooses the node whose retention policies the cluster
// adopts. The hash is the one GET returned for that node: the leader adopts
// only if the node still holds exactly those policies. Every other node's
// own policies are kept as dormant rows and never run.
func (h *ClusterHandler) handleAdoptPolicies(c *fiber.Ctx) error {
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}
	var req adoptPoliciesRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"success": false, "error": "Invalid request body: " + err.Error()})
	}
	if req.Kind != "retention" && req.Kind != "cq" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"success": false, "error": `kind must be "retention" or "cq"`})
	}
	if req.Node == "" || strings.TrimSpace(req.Hash) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"success": false, "error": "node and hash are required (GET /api/v1/cluster/policies lists them)"})
	}
	by := ""
	if ti := auth.GetTokenInfo(c); ti != nil {
		by = fmt.Sprintf("token %d (%s)", ti.ID, ti.Name)
	}
	if err := h.coordinator.ChoosePolicyAdoption(req.Kind, req.Node, req.Hash, by); err != nil {
		return h.clusterPoliciesError(c, err)
	}
	h.logger.Info().Str("kind", req.Kind).Str("node", req.Node).Str("by", by).Msg("Policy adoption chosen")
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"success": true,
		"message": "the leader adopts the node's " + req.Kind + " policies if it still holds exactly these; every other node's own policies of that kind then stop running and are kept as dormant rows. GET /api/v1/cluster shows the outcome under cluster_policies",
	})
}
