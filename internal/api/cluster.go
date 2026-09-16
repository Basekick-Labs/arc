package api

import (
	"errors"
	"sort"
	"strconv"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Input validation constants
const (
	maxNodeIDLength = 256
)

// validRoles defines the valid role filter values.
var validRoles = map[string]bool{
	"writer":     true,
	"reader":     true,
	"compactor":  true,
	"standalone": true,
}

// validStates defines the valid state filter values.
var validStates = map[string]bool{
	"healthy":   true,
	"unhealthy": true,
	"dead":      true,
	"unknown":   true,
	"joining":   true,
	"leaving":   true,
}

// ClusterHandler handles cluster management API endpoints.
type ClusterHandler struct {
	coordinator   *cluster.Coordinator
	authManager   *auth.AuthManager
	licenseClient *license.Client
	logger        zerolog.Logger
}

// NewClusterHandler creates a new cluster handler.
// The coordinator can be nil if clustering is not enabled.
func NewClusterHandler(
	coordinator *cluster.Coordinator,
	authManager *auth.AuthManager,
	licenseClient *license.Client,
	logger zerolog.Logger,
) *ClusterHandler {
	return &ClusterHandler{
		coordinator:   coordinator,
		authManager:   authManager,
		licenseClient: licenseClient,
		logger:        logger.With().Str("component", "cluster-handler").Logger(),
	}
}

// RegisterRoutes registers cluster API routes.
func (h *ClusterHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/api/v1/cluster", h.handleGetStatus)
	app.Get("/api/v1/cluster/nodes", h.handleGetNodes)
	app.Get("/api/v1/cluster/nodes/:id", h.handleGetNode)
	app.Get("/api/v1/cluster/local", h.handleGetLocalNode)
	app.Get("/api/v1/cluster/health", h.handleGetHealth)

	// Admin-only: file manifest exposes database schema + file paths
	// and destructive node removal
	filesGroup := app.Group("/api/v1/cluster/files")
	if h.authManager != nil {
		filesGroup.Use(auth.RequireAdmin(h.authManager))
	}
	filesGroup.Get("", h.handleGetFiles)

	removeGroup := app.Group("/api/v1/cluster/nodes/:id")
	if h.authManager != nil {
		removeGroup.Use(auth.RequireAdmin(h.authManager))
	}
	removeGroup.Delete("", h.handleRemoveNode)

	// Admin-only: hands the primary-writer role off a node so the cluster
	// elects a new one. Deliberately NOT gated on the writer_failover licence
	// — that feature is AUTOMATIC failover, and this is the manual recovery a
	// cluster without it needs (#872).
	writerGroup := app.Group("/api/v1/cluster/writers/:id")
	if h.authManager != nil {
		writerGroup.Use(auth.RequireAdmin(h.authManager))
	}
	writerGroup.Post("/demote", h.handleDemoteWriter)

	// Admin-only: hands the compactor lease to a named node. Unlike the
	// writer hand-over above, the target is explicit and required — the
	// automatic choice landing in the wrong place is exactly what this
	// endpoint exists to override (#876).
	compactorGroup := app.Group("/api/v1/cluster/compactor")
	if h.authManager != nil {
		compactorGroup.Use(auth.RequireAdmin(h.authManager))
	}
	compactorGroup.Post("/assign", h.handleAssignCompactor)
}

// assignCompactorRequest is the body of POST /api/v1/cluster/compactor/assign.
type assignCompactorRequest struct {
	NodeID string `json:"node_id"`
}

// handleAssignCompactor hands the compactor lease to a named node.
//
// This is the operator lever #876 found missing: the lease is only ever moved
// automatically, and only when its holder is unhealthy or a dedicated
// compactor has been idle for long enough to preempt it. Neither covers "move
// compaction off this node right now".
func (h *ClusterHandler) handleAssignCompactor(c *fiber.Ctx) error {
	var req assignCompactorRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "request body must be JSON with a node_id field",
		})
	}
	if len(req.NodeID) == 0 || len(req.NodeID) > maxNodeIDLength {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "invalid node ID",
		})
	}

	// Deliberately NOT respondNotEnabled, which answers 200 with
	// enabled=false. That is right for "describe yourself" and wrong for
	// "do this" — a caller checking the status code would read it as an
	// assignment that happened.
	if h.coordinator == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"success": false,
			"error":   "clustering is not enabled on this node, so there is no compactor lease to assign",
		})
	}

	oldCompactor, err := h.coordinator.AssignCompactorViaRaft(req.NodeID)
	if err != nil {
		status := fiber.StatusInternalServerError
		switch {
		case errors.Is(err, cluster.ErrNodeNotFound):
			status = fiber.StatusNotFound
		case errors.Is(err, cluster.ErrNotLeaderForTopology),
			errors.Is(err, cluster.ErrClusterRaftNotConfigured),
			errors.Is(err, cluster.ErrCompactorFailoverInProgress),
			errors.Is(err, cluster.ErrCompactorLeaseNotManaged),
			errors.Is(err, cluster.ErrAlreadyCompactorLeaseHolder),
			errors.Is(err, cluster.ErrCannotHoldCompactorLease),
			errors.Is(err, cluster.ErrNodeNotHealthy):
			status = fiber.StatusConflict
		}
		if status == fiber.StatusInternalServerError {
			h.logger.Error().Err(err).Str("node_id", req.NodeID).Msg("Failed to assign the compactor lease")
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	h.logger.Info().
		Str("node_id", req.NodeID).
		Str("old_compactor", oldCompactor).
		Msg("Compactor lease assigned via API")

	resp := fiber.Map{
		"success":       true,
		"node_id":       req.NodeID,
		"old_compactor": oldCompactor,
		"message":       "compactor lease assigned",
	}
	// Say how long the choice survives. Automatic preemption hands the lease
	// to a dedicated compactor, so an operator who deliberately put it on a
	// writer needs to know this is an override with an expiry rather than a
	// permanent setting — and that changing the node's role is the permanent
	// version.
	if d := h.coordinator.CompactorPreemptCooldown(); d > 0 {
		resp["preemption_suppressed_seconds"] = int(d.Seconds())
	}
	return c.JSON(resp)
}

// handleDemoteWriter hands the primary-writer role off the named node.
//
// The caller does not choose a successor, and should not: clearing the
// designation is what lets the cluster's own election run, and it already
// applies the selection rules. On a cluster with automatic failover this is a
// way to drain a writer deliberately; on one without, it is the only way to
// recover after the primary is gone for good.
func (h *ClusterHandler) handleDemoteWriter(c *fiber.Ctx) error {
	// Validate the input before anything else, so a malformed request is
	// rejected the same way whether or not this node is clustered.
	nodeID := c.Params("id")
	if len(nodeID) == 0 || len(nodeID) > maxNodeIDLength {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "invalid node ID",
		})
	}

	// Deliberately NOT respondNotEnabled, which the read endpoints use: it
	// answers 200 with enabled=false, which is right for "describe yourself"
	// and wrong for "do this". A caller checking the status code would read it
	// as a hand-over that happened.
	if h.coordinator == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"success": false,
			"error":   "clustering is not enabled on this node, so there is no primary writer to hand over",
		})
	}

	newPrimary, err := h.coordinator.DemoteWriterViaRaft(nodeID)
	if err != nil {
		// A wrong target is the operator's mistake to see and correct, not a
		// server fault: it names which node the cluster actually has on
		// record. Not being the leader is a 409 for the same reason — retry
		// against the leader.
		status := fiber.StatusInternalServerError
		switch {
		case errors.Is(err, cluster.ErrNotPrimaryWriter):
			status = fiber.StatusConflict
		case errors.Is(err, cluster.ErrNotLeaderForTopology):
			status = fiber.StatusConflict
		}
		if status == fiber.StatusInternalServerError {
			h.logger.Error().Err(err).Str("node_id", nodeID).Msg("Failed to hand over the primary writer")
		}
		return c.Status(status).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	h.logger.Info().
		Str("node_id", nodeID).
		Str("new_primary", newPrimary).
		Msg("Primary writer handed over via API")

	// Report who took it, so the operator does not have to go looking — and
	// say plainly when nobody did, which is the single-writer case.
	if newPrimary == "" {
		return c.JSON(fiber.Map{
			"success":     true,
			"node_id":     nodeID,
			"new_primary": nil,
			"message":     "primary writer designation released, but no other writer was available to take it — this cluster has no primary until one is",
		})
	}
	return c.JSON(fiber.Map{
		"success":     true,
		"node_id":     nodeID,
		"new_primary": newPrimary,
		"message":     "primary writer handed over",
	})
}

// handleGetStatus returns the overall cluster status.
func (h *ClusterHandler) handleGetStatus(c *fiber.Ctx) error {
	// Check if clustering is enabled and licensed
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}

	status := h.coordinator.Status()
	status["enabled"] = true
	status["mode"] = "cluster"

	// Add license info
	if h.licenseClient != nil {
		lic := h.licenseClient.GetLicense()
		if lic != nil {
			status["license"] = map[string]interface{}{
				"valid":    lic.IsValid(),
				"tier":     lic.Tier,
				"features": lic.Features,
			}
		}
	}

	return c.JSON(status)
}

// handleGetNodes returns all cluster nodes.
func (h *ClusterHandler) handleGetNodes(c *fiber.Ctx) error {
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}

	// Validate query parameters
	roleFilter := c.Query("role")
	if roleFilter != "" && !validRoles[roleFilter] {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid role filter. Valid values: writer, reader, compactor, standalone",
		})
	}

	stateFilter := c.Query("state")
	if stateFilter != "" && !validStates[stateFilter] {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid state filter. Valid values: healthy, unhealthy, dead, unknown, joining, leaving",
		})
	}

	registry := h.coordinator.GetRegistry()
	nodes := registry.GetAll()

	// Read once for the whole list, not once per node.
	activeCompactor := h.activeCompactorID()

	nodeList := make([]map[string]interface{}, 0, len(nodes))
	for _, node := range nodes {
		// Filter by role if specified
		if roleFilter != "" && string(node.Role) != roleFilter {
			continue
		}

		// Filter by state if specified
		if stateFilter != "" && string(node.GetState()) != stateFilter {
			continue
		}

		nodeList = append(nodeList, h.nodeToMapWithLease(node, activeCompactor))
	}

	return c.JSON(fiber.Map{
		"nodes": nodeList,
		"total": len(nodeList),
	})
}

// handleGetNode returns a specific node by ID.
func (h *ClusterHandler) handleGetNode(c *fiber.Ctx) error {
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}

	nodeID := c.Params("id")

	// Validate node ID
	if len(nodeID) == 0 || len(nodeID) > maxNodeIDLength {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid node ID",
		})
	}

	registry := h.coordinator.GetRegistry()

	node, exists := registry.Get(nodeID)
	if !exists {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Node not found",
		})
	}

	return c.JSON(h.nodeToMap(node))
}

// handleGetLocalNode returns the local node info with its capabilities.
func (h *ClusterHandler) handleGetLocalNode(c *fiber.Ctx) error {
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}

	node := h.coordinator.GetLocalNode()
	capabilities := node.GetCapabilities()

	response := h.nodeToMap(node)
	response["capabilities"] = map[string]bool{
		"can_ingest":     capabilities.CanIngest,
		"can_query":      capabilities.CanQuery,
		"can_compact":    capabilities.CanCompact,
		"can_coordinate": capabilities.CanCoordinate,
	}
	response["is_local"] = true

	return c.JSON(response)
}

// handleGetHealth returns cluster health information.
func (h *ClusterHandler) handleGetHealth(c *fiber.Ctx) error {
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}

	registry := h.coordinator.GetRegistry()
	healthChecker := h.coordinator.GetHealthChecker()

	summary := registry.Summary()

	return c.JSON(fiber.Map{
		"healthy":        summary["healthy"],
		"unhealthy":      summary["unhealthy"],
		"total":          summary["total"],
		"health_checker": healthChecker.Status(),
	})
}

// respondNotEnabled returns a response indicating clustering is not enabled.
func (h *ClusterHandler) respondNotEnabled(c *fiber.Ctx) error {
	response := fiber.Map{
		"enabled": false,
		"mode":    "standalone",
	}

	// Determine the reason
	if h.licenseClient == nil {
		response["reason"] = "Enterprise license not configured"
	} else {
		lic := h.licenseClient.GetLicense()
		if lic == nil || !lic.IsValid() {
			response["reason"] = "Enterprise license not valid"
		} else if !lic.HasFeature(license.FeatureClustering) {
			response["reason"] = "License does not include clustering feature"
		} else {
			response["reason"] = "Clustering not enabled in configuration (cluster.enabled=false)"
		}
	}

	return c.JSON(response)
}

// nodeToMap converts a Node to a map for JSON serialization.
func (h *ClusterHandler) nodeToMap(node *cluster.Node) map[string]interface{} {
	return h.nodeToMapWithLease(node, h.activeCompactorID())
}

// activeCompactorID reads the compactor lease once, for callers that map more
// than one node. GetActiveCompactorID takes the FSM lock, so re-reading it
// per node in a loop would take it once per cluster member.
func (h *ClusterHandler) activeCompactorID() string {
	if h.coordinator == nil {
		return ""
	}
	return h.coordinator.GetActiveCompactorID()
}

func (h *ClusterHandler) nodeToMapWithLease(node *cluster.Node, activeCompactorID string) map[string]interface{} {
	return map[string]interface{}{
		"id":    node.ID,
		"name":  node.Name,
		"role":  node.Role,
		"state": node.GetState(),
		// Which writer is the primary is what gates every singleton task
		// (retention, CQ, delete). It was absent here, so an operator had no
		// way to see that no node held it — which is how #850 stayed hidden.
		// Empty for readers, compactors, and for writers in shared-storage
		// mode, where there is no primary/standby distinction.
		"writer_state":   node.GetWriterState(),
		"address":        node.Address,
		"api_address":    node.APIAddress,
		"cluster_name":   node.ClusterName,
		"version":        node.Version,
		"started_at":     node.StartedAt,
		"joined_at":      node.JoinedAt,
		"last_heartbeat": node.GetLastHeartbeat(),
		"failed_checks":  node.GetFailedChecks(),
		"stats":          node.GetStats(),
		// Who holds the compactor lease was previously observable only in a
		// log line written once, at assignment time, so "my dedicated
		// compactor is idle" had no answer in the API (#876).
		"is_active_compactor": activeCompactorID != "" && node.ID == activeCompactorID,
	}
}

// handleRemoveNode removes a dead or unresponsive node from the cluster.
// This is an admin-only destructive operation — it removes the node from
// Raft voting, the cluster FSM state, and the local registry.
func (h *ClusterHandler) handleRemoveNode(c *fiber.Ctx) error {
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}

	nodeID := c.Params("id")
	if len(nodeID) == 0 || len(nodeID) > maxNodeIDLength {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "invalid node ID",
		})
	}

	// Prevent self-removal
	localNode := h.coordinator.GetLocalNode()
	if localNode != nil && localNode.ID == nodeID {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "cannot remove self from cluster — use graceful shutdown instead",
		})
	}

	if err := h.coordinator.RemoveNodeViaRaft(nodeID); err != nil {
		h.logger.Error().Err(err).Str("node_id", nodeID).Msg("Failed to remove node from cluster")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	h.logger.Info().Str("node_id", nodeID).Msg("Node removed from cluster via API")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "node removed from cluster",
		"node_id": nodeID,
	})
}

// handleGetFiles returns the cluster-wide file manifest from the Raft FSM.
// Supports optional `database` query parameter to filter by database.
// Supports optional `cursor` and `limit` query parameters for pagination.
// Without cursor/limit, returns all files (backward-compatible but O(N)).
// This is the authoritative view of all files known to the cluster — used
// by the peer replication system to determine what to pull from other nodes.
func (h *ClusterHandler) handleGetFiles(c *fiber.Ctx) error {
	if h.coordinator == nil {
		return h.respondNotEnabled(c)
	}

	database := c.Query("database")
	cursor := c.Query("cursor")
	limitStr := c.Query("limit")

	// Paginated path
	if cursor != "" || limitStr != "" {
		limit := 1000
		if limitStr != "" {
			if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 && parsed <= 10000 {
				limit = parsed
			}
		}

		var files []*clusterraft.FileEntry
		var nextCursor string
		var err error

		if database != "" {
			// Database filtering + pagination: get all for DB, then slice
			allFiles := h.coordinator.GetFileManifestByDatabase(database)
			files, nextCursor = paginateSlice(allFiles, cursor, limit)
		} else {
			files, nextCursor, err = h.coordinator.GetFileManifestPaginated(cursor, limit)
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": err.Error(),
				})
			}
		}

		return c.JSON(fiber.Map{
			"files":       files,
			"total":       len(files),
			"next_cursor": nextCursor,
		})
	}

	// Backward-compatible: return all files (no pagination)
	var files []*clusterraft.FileEntry
	if database != "" {
		files = h.coordinator.GetFileManifestByDatabase(database)
	} else {
		files = h.coordinator.GetFileManifest()
	}

	return c.JSON(fiber.Map{
		"files": files,
		"total": len(files),
	})
}

// paginateSlice applies cursor-based pagination to an in-memory slice.
// Used for database-filtered results where the FSM's paginated API doesn't
// natively support database filtering yet. Files are sorted by path first
// (source is an unordered map, so iteration order is non-deterministic).
// nextCursor is the last path in this page; the next call will resume after it.
//
// NOTE: This still allocates the full filtered set (O(k) where k = files in
// the database). The unfiltered path via GetFileManifestPaginated releases
// the RLock between pages; this path does not. For large databases, prefer
// the unfiltered paginated endpoint.
func paginateSlice(files []*clusterraft.FileEntry, cursor string, limit int) ([]*clusterraft.FileEntry, string) {
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })

	start := 0
	if cursor != "" {
		// Binary search for cursor position
		idx := sort.Search(len(files), func(i int) bool { return files[i].Path >= cursor })
		if idx < len(files) && files[idx].Path == cursor {
			start = idx + 1
		} else {
			start = idx
		}
	}
	if start >= len(files) {
		return nil, ""
	}
	end := start + limit
	if end > len(files) {
		end = len(files)
	}
	nextCursor := ""
	if end < len(files) {
		nextCursor = files[end-1].Path
	}
	return files[start:end], nextCursor
}
