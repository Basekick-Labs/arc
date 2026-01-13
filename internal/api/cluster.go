package api

import (
	"github.com/basekick-labs/arc/internal/cluster"
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
	licenseClient *license.Client
	logger        zerolog.Logger
}

// NewClusterHandler creates a new cluster handler.
// The coordinator can be nil if clustering is not enabled.
func NewClusterHandler(
	coordinator *cluster.Coordinator,
	licenseClient *license.Client,
	logger zerolog.Logger,
) *ClusterHandler {
	return &ClusterHandler{
		coordinator:   coordinator,
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
