package cluster

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/rs/zerolog"
)

// Coordinator manages cluster membership and coordination.
// It is responsible for:
// - Maintaining the local node's identity and state
// - Tracking other nodes in the cluster via the registry
// - Running health checks on cluster members
// - Managing the node lifecycle (join, leave, fail)
type Coordinator struct {
	cfg           *config.ClusterConfig
	licenseClient *license.Client
	registry      *Registry
	localNode     *Node
	healthChecker *HealthChecker

	// Network
	listener net.Listener

	// State
	running bool
	stopCh  chan struct{}
	mu      sync.RWMutex

	logger zerolog.Logger
}

// CoordinatorConfig holds configuration for the coordinator.
type CoordinatorConfig struct {
	Config        *config.ClusterConfig
	LicenseClient *license.Client
	Version       string // Arc version
	APIAddress    string // HTTP API address for this node
	Logger        zerolog.Logger
}

// NewCoordinator creates a new cluster coordinator.
// Returns an error if the license is invalid or missing the clustering feature.
func NewCoordinator(cfg *CoordinatorConfig) (*Coordinator, error) {
	// Validate license - clustering requires enterprise license
	if err := validateClusteringLicense(cfg.LicenseClient); err != nil {
		return nil, err
	}

	// Validate role
	role := ParseRole(cfg.Config.Role)
	if !role.IsValid() {
		return nil, fmt.Errorf("%w: %s", ErrInvalidRole, cfg.Config.Role)
	}

	// Generate node ID if not provided
	nodeID := cfg.Config.NodeID
	if nodeID == "" {
		nodeID = generateNodeID()
	}

	// Create local node
	localNode := NewNode(nodeID, nodeID, role, cfg.Config.ClusterName)
	localNode.SetVersion(cfg.Version)
	localNode.SetAddresses(cfg.Config.AdvertiseAddr, cfg.APIAddress)
	localNode.UpdateState(StateJoining)

	// Create registry
	registry := NewRegistry(&RegistryConfig{
		LocalNode: localNode,
		Logger:    cfg.Logger,
	})

	c := &Coordinator{
		cfg:           cfg.Config,
		licenseClient: cfg.LicenseClient,
		registry:      registry,
		localNode:     localNode,
		stopCh:        make(chan struct{}),
		logger:        cfg.Logger.With().Str("component", "cluster-coordinator").Logger(),
	}

	// Create health checker
	c.healthChecker = NewHealthChecker(&HealthCheckerConfig{
		Registry:           registry,
		CheckInterval:      time.Duration(cfg.Config.HealthCheckInterval) * time.Second,
		CheckTimeout:       time.Duration(cfg.Config.HealthCheckTimeout) * time.Second,
		UnhealthyThreshold: cfg.Config.UnhealthyThreshold,
		Logger:             cfg.Logger,
	})

	c.logger.Info().
		Str("node_id", nodeID).
		Str("role", string(role)).
		Str("cluster", cfg.Config.ClusterName).
		Msg("Cluster coordinator initialized")

	return c, nil
}

// Start starts the cluster coordinator.
func (c *Coordinator) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return ErrAlreadyRunning
	}

	// Validate license on each start (license may have expired)
	if err := validateClusteringLicense(c.licenseClient); err != nil {
		return err
	}

	// Start listening for peer connections if we have a coordinator address
	if c.cfg.CoordinatorAddr != "" {
		listener, err := net.Listen("tcp", c.cfg.CoordinatorAddr)
		if err != nil {
			return fmt.Errorf("failed to start coordinator listener: %w", err)
		}
		c.listener = listener

		// Start peer listener
		go c.acceptLoop()
	}

	// Start health checker
	c.healthChecker.Start()

	// Start peer discovery if we have seeds
	if len(c.cfg.Seeds) > 0 {
		go c.discoveryLoop()
	}

	c.running = true
	c.localNode.MarkJoined()

	c.logger.Info().
		Str("coordinator_addr", c.cfg.CoordinatorAddr).
		Int("seed_count", len(c.cfg.Seeds)).
		Str("role", string(c.localNode.Role)).
		Msg("Cluster coordinator started")

	return nil
}

// Stop stops the cluster coordinator gracefully.
func (c *Coordinator) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	c.logger.Info().Msg("Stopping cluster coordinator...")

	// Signal all goroutines to stop
	close(c.stopCh)

	// Stop health checker
	c.healthChecker.Stop()

	// Close listener
	if c.listener != nil {
		c.listener.Close()
	}

	// Mark local node as leaving
	c.localNode.UpdateState(StateLeaving)

	c.running = false

	c.logger.Info().Msg("Cluster coordinator stopped")
	return nil
}

// Close implements the shutdown.Shutdownable interface.
func (c *Coordinator) Close() error {
	return c.Stop()
}

// discoveryLoop periodically discovers and connects to peer nodes.
func (c *Coordinator) discoveryLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Initial discovery
	c.discoverPeers()

	for {
		select {
		case <-ticker.C:
			c.discoverPeers()
		case <-c.stopCh:
			return
		}
	}
}

// discoverPeers attempts to discover peer nodes from seeds.
// Note: Full peer discovery protocol is implemented in Phase 3.
// For now, this is a placeholder that logs discovery attempts.
func (c *Coordinator) discoverPeers() {
	for _, seed := range c.cfg.Seeds {
		// Skip self
		if seed == c.cfg.AdvertiseAddr {
			continue
		}

		// TODO (Phase 3): Implement peer discovery protocol
		// - Connect to seed node
		// - Exchange node info via protocol messages
		// - Register discovered nodes in registry
		c.logger.Debug().
			Str("seed", seed).
			Msg("Peer discovery placeholder - full implementation in Phase 3")
	}
}

// acceptLoop accepts incoming peer connections.
func (c *Coordinator) acceptLoop() {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			select {
			case <-c.stopCh:
				return
			default:
				c.logger.Error().Err(err).Msg("Failed to accept peer connection")
				continue
			}
		}

		go c.handlePeerConnection(conn)
	}
}

// handlePeerConnection handles an incoming peer connection.
// Note: Full peer protocol is implemented in Phase 3.
func (c *Coordinator) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	// TODO (Phase 3): Implement peer protocol
	// - Read message header
	// - Parse protocol message
	// - Handle join, leave, heartbeat, etc.
	c.logger.Debug().
		Str("peer", conn.RemoteAddr().String()).
		Msg("Peer connection received - full protocol in Phase 3")
}

// GetRegistry returns the node registry.
func (c *Coordinator) GetRegistry() *Registry {
	return c.registry
}

// GetLocalNode returns the local node.
func (c *Coordinator) GetLocalNode() *Node {
	return c.localNode
}

// GetHealthChecker returns the health checker.
func (c *Coordinator) GetHealthChecker() *HealthChecker {
	return c.healthChecker
}

// IsRunning returns true if the coordinator is running.
func (c *Coordinator) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

// GetRole returns the role of the local node.
func (c *Coordinator) GetRole() NodeRole {
	return c.localNode.Role
}

// GetCapabilities returns the capabilities of the local node.
func (c *Coordinator) GetCapabilities() RoleCapabilities {
	return c.localNode.GetCapabilities()
}

// Status returns the cluster status as a map for JSON serialization.
func (c *Coordinator) Status() map[string]interface{} {
	c.mu.RLock()
	running := c.running
	c.mu.RUnlock()

	nodes := c.registry.GetAll()
	nodeList := make([]map[string]interface{}, 0, len(nodes))
	for _, node := range nodes {
		nodeList = append(nodeList, map[string]interface{}{
			"id":             node.ID,
			"name":           node.Name,
			"role":           node.Role,
			"state":          node.State,
			"address":        node.Address,
			"api_address":    node.APIAddress,
			"version":        node.Version,
			"last_heartbeat": node.GetLastHeartbeat(),
			"stats":          node.GetStats(),
		})
	}

	summary := c.registry.Summary()

	return map[string]interface{}{
		"running":       running,
		"cluster_name":  c.cfg.ClusterName,
		"local_node_id": c.localNode.ID,
		"local_role":    c.localNode.Role,
		"node_count":    summary["total"],
		"healthy_count": summary["healthy"],
		"nodes":         nodeList,
		"writers":       summary["writers"],
		"readers":       summary["readers"],
		"compactors":    summary["compactors"],
	}
}

// generateNodeID generates a unique node ID with sufficient entropy.
// Uses 8 bytes (64 bits) of randomness plus timestamp for collision resistance.
func generateNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// Generate random suffix with 64 bits of entropy for collision resistance
	suffix := make([]byte, 8)
	rand.Read(suffix)

	return fmt.Sprintf("%s-%d-%x", hostname, time.Now().UnixNano(), suffix)
}

// validateClusteringLicense validates that the license allows clustering.
func validateClusteringLicense(client *license.Client) error {
	if client == nil {
		return ErrLicenseRequired
	}
	lic := client.GetLicense()
	if lic == nil {
		return ErrLicenseRequired
	}
	if !lic.HasFeature(license.FeatureClustering) {
		return ErrClusteringFeatureRequired
	}
	return nil
}
