package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Input validation constants
const (
	maxNodeIDLength    = 256
	maxDeletePathLen   = 4096
	maxDeleteReasonLen = 256
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

// clusterFilesCoordinator is the minimal coordinator interface required for
// cluster file management operations (lookup and deletion).
type clusterFilesCoordinator interface {
	GetFileEntry(path string) (*clusterraft.FileEntry, bool)
	DeleteFileFromManifest(ctx context.Context, path, reason string) error
}

// ClusterHandler handles cluster management API endpoints.
type ClusterHandler struct {
	coordinator      *cluster.Coordinator
	filesCoordinator clusterFilesCoordinator
	authManager      *auth.AuthManager
	licenseClient    *license.Client
	logger           zerolog.Logger
}

// NewClusterHandler creates a new cluster handler.
// The coordinator can be nil if clustering is not enabled.
func NewClusterHandler(
	coordinator *cluster.Coordinator,
	authManager *auth.AuthManager,
	licenseClient *license.Client,
	logger zerolog.Logger,
) *ClusterHandler {
	var fc clusterFilesCoordinator
	if coordinator != nil {
		fc = coordinator
	}
	return &ClusterHandler{
		coordinator:      coordinator,
		filesCoordinator: fc,
		authManager:      authManager,
		licenseClient:    licenseClient,
		logger:           logger.With().Str("component", "cluster-handler").Logger(),
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
	filesGroup.Delete("", h.handleDeleteFile)

	removeGroup := app.Group("/api/v1/cluster/nodes/:id")
	if h.authManager != nil {
		removeGroup.Use(auth.RequireAdmin(h.authManager))
	}
	removeGroup.Delete("", h.handleRemoveNode)
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

		nodeList = append(nodeList, h.nodeToMap(node))
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
		"success": false,
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
	return map[string]interface{}{
		"id":             node.ID,
		"name":           node.Name,
		"role":           node.Role,
		"state":          node.GetState(),
		"address":        node.Address,
		"api_address":    node.APIAddress,
		"cluster_name":   node.ClusterName,
		"version":        node.Version,
		"started_at":     node.StartedAt,
		"joined_at":      node.JoinedAt,
		"last_heartbeat": node.GetLastHeartbeat(),
		"failed_checks":  node.GetFailedChecks(),
		"stats":          node.GetStats(),
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

// handleDeleteFile removes a file entry from the cluster-wide manifest via Raft consensus
// and triggers cluster-wide deletion.
//
// WARNING: On nodes with local storage, removing the manifest entry enqueues a physical
// deletion (via the delete worker pool) that unlinks the file from disk on every node.
// On shared storage backends (S3, Azure), the physical object is left orphaned until
// cleaned up by the reconciliation sweeper.
//
// Query parameters:
//   - path (required, max 4096 bytes): relative storage path of the file entry in the manifest.
//   - confirm (required): must be "true" to confirm this destructive operation.
//   - reason (optional, default "operator", max 256 chars): reason recorded in audit logs and Raft payloads.
func (h *ClusterHandler) handleDeleteFile(c *fiber.Ctx) error {
	if h.filesCoordinator == nil {
		return h.respondNotEnabled(c)
	}

	path := c.Query("path")
	if path == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "path query parameter is required",
		})
	}
	if len(path) > maxDeletePathLen {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   fmt.Sprintf("path exceeds maximum length of %d characters", maxDeletePathLen),
		})
	}

	if c.Query("confirm") != "true" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "confirmation required: add ?confirm=true to delete the file from cluster manifest and storage",
		})
	}

	reason := c.Query("reason")
	if reason == "" {
		reason = "operator"
	} else if len(reason) > maxDeleteReasonLen {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   fmt.Sprintf("reason exceeds maximum length of %d characters", maxDeleteReasonLen),
		})
	}

	// Look up by exact string in cluster manifest; 404 if absent.
	// NOTE: We deliberately do not validate the path format upfront (#794 item 5 correction).
	// This allows operators to remove corrupted or unaddressable keys (e.g. malformed paths
	// from older Arc releases) that retention and reconciliation cannot delete.
	if _, exists := h.filesCoordinator.GetFileEntry(path); !exists {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "file not found in cluster manifest",
			"path":    path,
		})
	}

	// Classify unaddressable keys for audit details and warning logs
	isUnaddressable := clusterraft.ValidateManifestPath(path) != nil

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	if err := h.filesCoordinator.DeleteFileFromManifest(ctx, path, reason); err != nil {
		if errors.Is(err, clusterraft.ErrManifestApply) {
			c.Set(fiber.HeaderRetryAfter, "1")
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"success": false,
				"error":   "cluster manifest update unavailable; retry later",
			})
		}
		h.logger.Error().Err(err).Str("path", path).Msg("Failed to delete file from cluster manifest")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Record audit detail in locals for audit middleware to persist
	auditDetail := map[string]string{
		"path":   path,
		"reason": reason,
	}
	if isUnaddressable {
		auditDetail["unaddressable_key"] = "true"
	}
	c.Locals(audit.DetailLocalsKey, auditDetail)

	var nodeID string
	if h.coordinator != nil {
		if node := h.coordinator.GetLocalNode(); node != nil {
			nodeID = node.ID
		}
	}

	logEvent := h.logger.Warn().
		Str("path", path).
		Str("reason", reason)
	if nodeID != "" {
		logEvent = logEvent.Str("node_id", nodeID)
	}
	if isUnaddressable {
		logEvent = logEvent.Bool("unaddressable_key", true)
	}
	logEvent.Msg("File removed from cluster manifest and deleted cluster-wide")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "file removed from cluster manifest and deleted cluster-wide",
		"path":    path,
	})
}
