package cluster

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/replication"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// Coordinator manages cluster membership and coordination.
// It is responsible for:
// - Maintaining the local node's identity and state
// - Tracking other nodes in the cluster via the registry
// - Running health checks on cluster members
// - Managing the node lifecycle (join, leave, fail)
// - Leader election via Raft consensus (Phase 3)
// - Request routing to appropriate nodes (Phase 3)
type Coordinator struct {
	cfg           *config.ClusterConfig
	licenseClient *license.Client
	registry      *Registry
	localNode     *Node
	healthChecker *HealthChecker

	// Raft consensus (Phase 3)
	raftNode *raft.Node
	raftFSM  *raft.ClusterFSM

	// Request routing (Phase 3)
	router *Router

	// Writer failover (Phase 3)
	writerFailoverMgr *WriterFailoverManager

	// WAL Replication (Phase 3.3)
	replicationSender   *replication.Sender   // Writer only: sends entries to readers
	replicationReceiver *replication.Receiver // Reader only: receives entries from writer
	walWriter           *wal.Writer           // Reference to local WAL for replication hook

	// Network
	listener net.Listener

	// State
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
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

	// Initialize Raft FSM and node (Phase 3)
	if cfg.Config.RaftDataDir != "" {
		c.raftFSM = raft.NewClusterFSM(cfg.Logger)

		// Set up FSM callbacks to sync with local registry
		c.raftFSM.SetCallbacks(
			func(n *raft.NodeInfo) { c.onRaftNodeAdded(n) },
			func(id string) { c.onRaftNodeRemoved(id) },
			func(n *raft.NodeInfo) { c.onRaftNodeUpdated(n) },
		)

		raftCfg := &raft.NodeConfig{
			NodeID:            nodeID,
			DataDir:           cfg.Config.RaftDataDir,
			BindAddr:          cfg.Config.RaftBindAddr,
			AdvertiseAddr:     cfg.Config.RaftAdvertiseAddr,
			Bootstrap:         cfg.Config.RaftBootstrap,
			ElectionTimeout:   time.Duration(cfg.Config.RaftElectionTimeout) * time.Millisecond,
			HeartbeatTimeout:  time.Duration(cfg.Config.RaftHeartbeatTimeout) * time.Millisecond,
			SnapshotInterval:  time.Duration(cfg.Config.RaftSnapshotInterval) * time.Second,
			SnapshotThreshold: uint64(cfg.Config.RaftSnapshotThreshold),
			Logger:            cfg.Logger,
		}

		var err error
		c.raftNode, err = raft.NewNode(raftCfg, c.raftFSM)
		if err != nil {
			return nil, fmt.Errorf("failed to create raft node: %w", err)
		}

		c.logger.Info().
			Str("raft_data_dir", cfg.Config.RaftDataDir).
			Str("raft_bind_addr", cfg.Config.RaftBindAddr).
			Bool("raft_bootstrap", cfg.Config.RaftBootstrap).
			Msg("Raft consensus initialized")
	}

	// Initialize request router (Phase 3)
	routeTimeout := time.Duration(cfg.Config.RouteTimeout) * time.Millisecond
	if routeTimeout == 0 {
		routeTimeout = 5 * time.Second
	}
	c.router = NewRouter(&RouterConfig{
		Timeout:   routeTimeout,
		Retries:   cfg.Config.RouteRetries,
		Strategy:  LoadBalanceRoundRobin,
		Registry:  registry,
		LocalNode: localNode,
		Logger:    cfg.Logger,
	})

	// Initialize writer failover manager (Phase 3) — requires license and Raft
	if cfg.Config.FailoverEnabled && c.raftNode != nil {
		if cfg.LicenseClient == nil || !cfg.LicenseClient.CanUseWriterFailover() {
			c.logger.Warn().Msg("Writer failover enabled but license does not include writer_failover feature — failover disabled")
		} else {
			c.writerFailoverMgr = NewWriterFailoverManager(&WriterFailoverConfig{
				Registry:           registry,
				RaftNode:           c.raftNode,
				FailoverTimeout:    time.Duration(cfg.Config.FailoverTimeoutSeconds) * time.Second,
				CooldownPeriod:     time.Duration(cfg.Config.FailoverCooldownSeconds) * time.Second,
				Logger:             cfg.Logger,
			})

			// Wire FSM writer promotion callback to update registry
			c.raftFSM.SetWriterPromotedCallback(func(newPrimaryID, oldPrimaryID string) {
				c.onWriterPromoted(newPrimaryID, oldPrimaryID)
			})

			c.logger.Info().Msg("Writer failover manager initialized")
		}
	}

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

	// Start Raft node if configured (Phase 3)
	if c.raftNode != nil {
		if err := c.raftNode.Start(); err != nil {
			return fmt.Errorf("failed to start raft node: %w", err)
		}
		c.logger.Info().Msg("Raft consensus started")

		// If we're bootstrapping, register ourselves in the FSM once we become leader
		// This ensures the leader is known to all nodes that join later
		if c.cfg.RaftBootstrap {
			go c.registerSelfInFSMWhenLeader()
		}
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

	// Start writer failover manager if configured
	if c.writerFailoverMgr != nil {
		// Wire unhealthy callback to failover manager
		c.registry.SetCallbacks(nil, nil, nil, func(node *Node) {
			c.writerFailoverMgr.HandleWriterUnhealthy(node)
		})
		if err := c.writerFailoverMgr.Start(context.Background()); err != nil {
			c.logger.Error().Err(err).Msg("Failed to start writer failover manager")
		}
	}

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

	// Stop writer failover manager
	if c.writerFailoverMgr != nil {
		if err := c.writerFailoverMgr.Stop(); err != nil {
			c.logger.Error().Err(err).Msg("Error stopping writer failover manager")
		}
	}

	// Stop Raft node if running (Phase 3)
	if c.raftNode != nil {
		if err := c.raftNode.Stop(); err != nil {
			c.logger.Error().Err(err).Msg("Error stopping Raft node")
		}
	}

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

// discoverPeers attempts to discover peer nodes from seeds and join the cluster.
func (c *Coordinator) discoverPeers() {
	// If we're already part of a Raft cluster with a leader, skip discovery
	if c.raftNode != nil && c.raftNode.LeaderAddr() != "" {
		return
	}

	c.logger.Info().
		Int("seed_count", len(c.cfg.Seeds)).
		Strs("seeds", c.cfg.Seeds).
		Msg("Starting peer discovery")

	for _, seed := range c.cfg.Seeds {
		// Skip self
		if seed == c.cfg.AdvertiseAddr || seed == c.cfg.CoordinatorAddr {
			c.logger.Debug().Str("seed", seed).Msg("Skipping self in seeds")
			continue
		}

		c.logger.Info().
			Str("seed", seed).
			Msg("Attempting to connect to seed node")

		if err := c.tryJoinViaSeed(seed); err != nil {
			c.logger.Warn().
				Err(err).
				Str("seed", seed).
				Msg("Failed to join via seed")
			continue
		}

		// Successfully joined
		c.logger.Info().
			Str("seed", seed).
			Msg("Successfully joined cluster via seed")
		return
	}
}

// tryJoinViaSeed attempts to join the cluster via a seed node.
func (c *Coordinator) tryJoinViaSeed(seedAddr string) error {
	conn, err := net.DialTimeout("tcp", seedAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to seed: %w", err)
	}
	defer conn.Close()

	// Build join request
	req := &protocol.JoinRequest{
		NodeID:      c.localNode.ID,
		NodeName:    c.localNode.Name,
		Role:        string(c.localNode.Role),
		ClusterName: c.cfg.ClusterName,
		RaftAddr:    c.cfg.RaftAdvertiseAddr,
		APIAddr:     c.localNode.APIAddress,
		CoordAddr:   c.cfg.AdvertiseAddr,
		Version:     c.localNode.Version,
	}

	// If RaftAdvertiseAddr is empty, use RaftBindAddr
	if req.RaftAddr == "" {
		req.RaftAddr = c.cfg.RaftBindAddr
	}

	// Send join request
	msg := protocol.NewJoinRequest(req)
	if err := protocol.SendMessage(conn, msg, 5*time.Second); err != nil {
		return fmt.Errorf("failed to send join request: %w", err)
	}

	// Receive response
	resp, err := protocol.ReceiveMessage(conn, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to receive response: %w", err)
	}

	return c.handleJoinResponse(resp, seedAddr)
}

// handleJoinResponse processes a response to a join request.
func (c *Coordinator) handleJoinResponse(msg *protocol.Message, seedAddr string) error {
	switch msg.Type {
	case protocol.MsgJoinResponse:
		resp := msg.Payload.(*protocol.JoinResponse)
		if !resp.Success {
			return fmt.Errorf("join rejected: %s", resp.Error)
		}

		c.logger.Info().
			Str("leader_id", resp.LeaderID).
			Int("cluster_size", len(resp.Nodes)).
			Msg("Join accepted by leader")

		// Register all nodes from the response in our local registry
		for _, nodeInfo := range resp.Nodes {
			node := NewNode(nodeInfo.ID, nodeInfo.Name, ParseRole(nodeInfo.Role), c.cfg.ClusterName)
			node.SetAddresses(nodeInfo.CoordAddr, nodeInfo.APIAddr)
			node.UpdateState(NodeState(nodeInfo.State))
			if err := c.registry.Register(node); err != nil {
				c.logger.Warn().Err(err).Str("node_id", nodeInfo.ID).Msg("Failed to register peer node")
			}
		}

		// Mark ourselves as healthy now that we've joined
		c.localNode.UpdateState(StateHealthy)

		return nil

	case protocol.MsgLeaderInfo:
		// Redirect to leader
		info := msg.Payload.(*protocol.LeaderInfo)
		c.logger.Debug().
			Str("leader_id", info.LeaderID).
			Str("leader_addr", info.LeaderCoordAddr).
			Msg("Redirected to leader")

		// Try to join via the leader directly
		if info.LeaderCoordAddr != "" && info.LeaderCoordAddr != seedAddr {
			return c.tryJoinViaSeed(info.LeaderCoordAddr)
		}
		return fmt.Errorf("redirect to leader failed: no valid leader address")

	default:
		return fmt.Errorf("unexpected response type: %v", msg.Type)
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
func (c *Coordinator) handlePeerConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()

	// Read the incoming message
	msg, err := protocol.ReceiveMessage(conn, 10*time.Second)
	if err != nil {
		c.logger.Debug().
			Err(err).
			Str("peer", remoteAddr).
			Msg("Failed to read peer message")
		conn.Close()
		return
	}

	c.logger.Debug().
		Str("peer", remoteAddr).
		Str("msg_type", msg.Type.String()).
		Msg("Received peer message")

	// For most message types, we close the connection after handling.
	// Exception: MsgReplicateSync - the sender takes ownership of the connection.
	closeConn := true

	switch msg.Type {
	case protocol.MsgJoinRequest:
		c.handleJoinRequest(conn, msg.Payload.(*protocol.JoinRequest))

	case protocol.MsgHeartbeat:
		c.handleHeartbeat(conn, msg.Payload.(*protocol.Heartbeat))

	case protocol.MsgLeaveNotify:
		c.handleLeaveNotify(msg.Payload.(*protocol.LeaveNotify))

	case protocol.MsgReplicateSync:
		c.handleReplicateSync(conn, msg.Payload.(*protocol.ReplicateSync))
		closeConn = false // Sender takes ownership

	default:
		c.logger.Warn().
			Str("peer", remoteAddr).
			Str("msg_type", msg.Type.String()).
			Msg("Unknown message type from peer")
	}

	if closeConn {
		conn.Close()
	}
}

// handleJoinRequest processes a join request from a new node.
func (c *Coordinator) handleJoinRequest(conn net.Conn, req *protocol.JoinRequest) {
	c.logger.Info().
		Str("node_id", req.NodeID).
		Str("role", req.Role).
		Str("cluster", req.ClusterName).
		Msg("Received join request")

	// Validate cluster name
	if req.ClusterName != c.cfg.ClusterName {
		c.sendJoinError(conn, fmt.Sprintf("cluster name mismatch: expected %s, got %s", c.cfg.ClusterName, req.ClusterName))
		return
	}

	// Check if we're the leader
	if c.raftNode != nil && !c.raftNode.IsLeader() {
		// Redirect to leader
		c.sendLeaderRedirect(conn)
		return
	}

	// We are the leader (or no Raft configured) - process the join
	// Create node from request
	node := NewNode(req.NodeID, req.NodeName, ParseRole(req.Role), req.ClusterName)
	node.SetAddresses(req.CoordAddr, req.APIAddr)
	node.SetVersion(req.Version)
	node.UpdateState(StateHealthy)

	// Add to Raft cluster if configured
	if c.raftNode != nil {
		// First add as a Raft voter
		if err := c.raftNode.AddVoter(req.NodeID, req.RaftAddr, 10*time.Second); err != nil {
			c.logger.Error().Err(err).Str("node_id", req.NodeID).Msg("Failed to add voter to Raft")
			c.sendJoinError(conn, fmt.Sprintf("failed to add to Raft cluster: %v", err))
			return
		}

		// Then add node info to FSM
		nodeInfo := &raft.NodeInfo{
			ID:          req.NodeID,
			Name:        req.NodeName,
			Role:        req.Role,
			ClusterName: req.ClusterName,
			Address:     req.CoordAddr,
			APIAddress:  req.APIAddr,
			State:       string(StateHealthy),
			Version:     req.Version,
		}
		if err := c.raftNode.AddNode(nodeInfo, 5*time.Second); err != nil {
			c.logger.Error().Err(err).Str("node_id", req.NodeID).Msg("Failed to add node to FSM")
			// Node was added to Raft but not FSM - this is ok, FSM will sync eventually
		}
	} else {
		// No Raft, just register locally
		if err := c.registry.Register(node); err != nil {
			c.sendJoinError(conn, fmt.Sprintf("failed to register node: %v", err))
			return
		}
	}

	c.logger.Info().
		Str("node_id", req.NodeID).
		Str("role", req.Role).
		Msg("Node successfully joined cluster")

	// Send success response with cluster info
	c.sendJoinSuccess(conn)
}

// sendJoinError sends a join failure response.
func (c *Coordinator) sendJoinError(conn net.Conn, errMsg string) {
	resp := protocol.NewJoinResponse(&protocol.JoinResponse{
		Success: false,
		Error:   errMsg,
	})
	if err := protocol.SendMessage(conn, resp, 5*time.Second); err != nil {
		c.logger.Debug().Err(err).Msg("Failed to send join error response")
	}
}

// sendLeaderRedirect sends a redirect to the current leader.
func (c *Coordinator) sendLeaderRedirect(conn net.Conn) {
	leaderID := c.raftNode.LeaderID()
	leaderRaftAddr := c.raftNode.LeaderAddr()

	// Try to find the leader's coordinator address from registry
	leaderCoordAddr := ""
	if leaderNode, exists := c.registry.Get(leaderID); exists {
		leaderCoordAddr = leaderNode.Address
	}

	info := protocol.NewLeaderInfo(&protocol.LeaderInfo{
		LeaderID:        leaderID,
		LeaderCoordAddr: leaderCoordAddr,
		LeaderRaftAddr:  leaderRaftAddr,
	})

	if err := protocol.SendMessage(conn, info, 5*time.Second); err != nil {
		c.logger.Debug().Err(err).Msg("Failed to send leader redirect")
	}
}

// sendJoinSuccess sends a successful join response with cluster info.
func (c *Coordinator) sendJoinSuccess(conn net.Conn) {
	// Gather all nodes in the cluster
	nodes := c.registry.GetAll()
	nodeInfos := make([]protocol.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		nodeInfos = append(nodeInfos, protocol.NodeInfo{
			ID:        n.ID,
			Name:      n.Name,
			Role:      string(n.Role),
			State:     string(n.GetState()),
			RaftAddr:  "", // TODO: store Raft addr in node
			APIAddr:   n.APIAddress,
			CoordAddr: n.Address,
		})
	}

	leaderID := c.localNode.ID
	leaderRaftAddr := ""
	if c.raftNode != nil {
		leaderID = c.raftNode.LeaderID()
		leaderRaftAddr = c.raftNode.LeaderAddr()
	}

	resp := protocol.NewJoinResponse(&protocol.JoinResponse{
		Success:    true,
		LeaderID:   leaderID,
		LeaderAddr: c.cfg.AdvertiseAddr,
		RaftLeader: leaderRaftAddr,
		Nodes:      nodeInfos,
	})

	if err := protocol.SendMessage(conn, resp, 5*time.Second); err != nil {
		c.logger.Debug().Err(err).Msg("Failed to send join success response")
	}
}

// handleHeartbeat processes a heartbeat from a peer.
func (c *Coordinator) handleHeartbeat(conn net.Conn, hb *protocol.Heartbeat) {
	// Update the node's last heartbeat time
	if node, exists := c.registry.Get(hb.NodeID); exists {
		// Record heartbeat with empty stats (stats could be added to heartbeat message later)
		node.RecordHeartbeat(NodeStats{})
		node.UpdateState(NodeState(hb.State))
	}

	// Send acknowledgment
	ack := protocol.NewHeartbeatAck(&protocol.HeartbeatAck{
		NodeID:    c.localNode.ID,
		Timestamp: time.Now(),
	})
	protocol.SendMessage(conn, ack, 5*time.Second)
}

// handleLeaveNotify processes a leave notification from a peer.
func (c *Coordinator) handleLeaveNotify(leave *protocol.LeaveNotify) {
	c.logger.Info().
		Str("node_id", leave.NodeID).
		Str("reason", leave.Reason).
		Msg("Node leaving cluster")

	// If we're the leader, remove from Raft
	if c.raftNode != nil && c.raftNode.IsLeader() {
		if err := c.raftNode.RemoveServer(leave.NodeID, 5*time.Second); err != nil {
			c.logger.Error().Err(err).Str("node_id", leave.NodeID).Msg("Failed to remove node from Raft")
		}
		if err := c.raftNode.RemoveNode(leave.NodeID, 5*time.Second); err != nil {
			c.logger.Error().Err(err).Str("node_id", leave.NodeID).Msg("Failed to remove node from FSM")
		}
	}

	// Remove from local registry
	c.registry.Unregister(leave.NodeID)
}

// handleReplicateSync handles a replication sync request from a reader node.
// This is called when a reader connects to start receiving WAL entries.
// NOTE: We don't close the connection here - the sender takes ownership.
func (c *Coordinator) handleReplicateSync(conn net.Conn, syncReq *protocol.ReplicateSync) {
	c.logger.Info().
		Str("reader_id", syncReq.ReaderID).
		Uint64("last_known_seq", syncReq.LastKnownSequence).
		Msg("Received replication sync request")

	// Check if we have a replication sender (we're a writer with replication enabled)
	c.mu.RLock()
	sender := c.replicationSender
	c.mu.RUnlock()

	if sender == nil {
		// Not a writer or replication not enabled
		c.logger.Warn().
			Str("reader_id", syncReq.ReaderID).
			Msg("Replication sync rejected: not a writer or replication not enabled")

		syncAck := &protocol.ReplicateSyncAck{
			CurrentSequence: 0,
			CanResume:       false,
			Error:           "this node is not configured as a writer with replication enabled",
		}
		protocol.SendMessage(conn, &protocol.Message{
			Type:    protocol.MsgReplicateSyncAck,
			Payload: syncAck,
		}, 5*time.Second)
		return
	}

	// Convert protocol types to replication types and accept the reader
	replSyncReq := &replication.ReplicateSync{
		ReaderID:          syncReq.ReaderID,
		LastKnownSequence: syncReq.LastKnownSequence,
	}

	if err := c.AcceptReplicationConnection(conn, replSyncReq); err != nil {
		c.logger.Error().
			Err(err).
			Str("reader_id", syncReq.ReaderID).
			Msg("Failed to accept replication connection")
		// Connection was already handled by AcceptReplicationConnection
	}
	// NOTE: Connection is now owned by the sender, don't close it
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

	status := map[string]interface{}{
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

	// Add Raft status if configured (Phase 3)
	if c.raftNode != nil {
		raftStats := c.raftNode.Stats()
		status["raft"] = map[string]interface{}{
			"enabled":     true,
			"is_leader":   c.raftNode.IsLeader(),
			"leader_addr": c.raftNode.LeaderAddr(),
			"leader_id":   c.raftNode.LeaderID(),
			"state":       c.raftNode.State().String(),
			"stats":       raftStats,
		}
	} else {
		status["raft"] = map[string]interface{}{
			"enabled": false,
		}
	}

	// Add router stats (Phase 3)
	if c.router != nil {
		status["router"] = c.router.Stats()
	}

	return status
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

// Phase 3: Raft and routing methods

// onRaftNodeAdded is called when a node is added via Raft consensus.
// It syncs the Raft FSM state to the local registry.
// registerSelfInFSMWhenLeader waits for this node to become Raft leader,
// then registers itself in the FSM so that joining nodes will receive the leader's info.
func (c *Coordinator) registerSelfInFSMWhenLeader() {
	// Wait for leader election (check every 100ms for up to 30 seconds)
	for i := 0; i < 300; i++ {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if c.raftNode.IsLeader() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !c.raftNode.IsLeader() {
		c.logger.Debug().Msg("Not leader after bootstrap, skipping self-registration in FSM")
		return
	}

	// Check if we're already in the FSM (e.g., from a previous run with snapshot)
	if _, exists := c.raftFSM.GetNode(c.localNode.ID); exists {
		c.logger.Debug().Msg("Already registered in FSM")
		return
	}

	// Register ourselves in the FSM
	nodeInfo := &raft.NodeInfo{
		ID:          c.localNode.ID,
		Name:        c.localNode.Name,
		Role:        string(c.localNode.Role),
		ClusterName: c.cfg.ClusterName,
		Address:     c.cfg.AdvertiseAddr,
		APIAddress:  c.localNode.APIAddress,
		State:       string(StateHealthy),
		Version:     c.localNode.Version,
	}

	if err := c.raftNode.AddNode(nodeInfo, 5*time.Second); err != nil {
		c.logger.Error().Err(err).Msg("Failed to register self in FSM")
		return
	}

	c.logger.Info().
		Str("node_id", c.localNode.ID).
		Msg("Bootstrap leader registered self in FSM")
}

func (c *Coordinator) onRaftNodeAdded(n *raft.NodeInfo) {
	node := NewNode(n.ID, n.Name, ParseRole(n.Role), n.ClusterName)
	node.SetAddresses(n.Address, n.APIAddress)
	node.SetVersion(n.Version)
	node.UpdateState(NodeState(n.State))

	if err := c.registry.Register(node); err != nil {
		c.logger.Error().Err(err).Str("node_id", n.ID).Msg("Failed to register node from Raft")
	}
}

// onRaftNodeRemoved is called when a node is removed via Raft consensus.
func (c *Coordinator) onRaftNodeRemoved(nodeID string) {
	c.registry.Unregister(nodeID)
}

// onRaftNodeUpdated is called when a node is updated via Raft consensus.
func (c *Coordinator) onRaftNodeUpdated(n *raft.NodeInfo) {
	node, exists := c.registry.Get(n.ID)
	if !exists {
		// Node doesn't exist locally, add it
		c.onRaftNodeAdded(n)
		return
	}

	// Update the existing node's state
	node.UpdateState(NodeState(n.State))
	c.registry.Register(node)
}

// onWriterPromoted is called when the FSM promotes a new primary writer.
// It updates the local registry to reflect the new writer states.
func (c *Coordinator) onWriterPromoted(newPrimaryID, oldPrimaryID string) {
	// Demote old primary in registry
	if oldPrimaryID != "" {
		if oldNode, exists := c.registry.Get(oldPrimaryID); exists {
			oldNode.SetWriterState(WriterStateStandby)
			c.registry.Register(oldNode)
		}
	}

	// Promote new primary in registry
	if newNode, exists := c.registry.Get(newPrimaryID); exists {
		newNode.SetWriterState(WriterStatePrimary)
		c.registry.Register(newNode)
	}

	c.logger.Info().
		Str("new_primary", newPrimaryID).
		Str("old_primary", oldPrimaryID).
		Msg("Writer promotion applied to registry")
}

// GetRouter returns the request router.
func (c *Coordinator) GetRouter() *Router {
	return c.router
}

// SetWAL sets the WAL writer reference for replication.
// This should be called after the WAL is created but before Start().
func (c *Coordinator) SetWAL(walWriter *wal.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.walWriter = walWriter
}

// StartReplication starts WAL replication based on node role.
// - Writers start a Sender to stream entries to readers
// - Readers start a Receiver to receive entries from the writer
// This should be called after Start() and SetWAL().
func (c *Coordinator) StartReplication() error {
	if !c.cfg.ReplicationEnabled {
		c.logger.Debug().Msg("WAL replication is disabled")
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Create context for replication
	c.ctx, c.cancel = context.WithCancel(context.Background())

	if c.localNode.Role == RoleWriter || c.localNode.Role == RoleStandalone {
		// Writer: start sender to stream entries to readers
		c.replicationSender = replication.NewSender(&replication.SenderConfig{
			BufferSize:   c.cfg.ReplicationBufferSize,
			WriteTimeout: 5 * time.Second,
			Logger:       c.logger,
		})

		if err := c.replicationSender.Start(c.ctx); err != nil {
			return fmt.Errorf("failed to start replication sender: %w", err)
		}

		// Hook into WAL if available
		if c.walWriter != nil {
			c.walWriter.SetReplicationHook(func(entry *wal.ReplicationEntry) {
				c.replicationSender.Replicate(&replication.ReplicateEntry{
					Sequence:    entry.Sequence,
					TimestampUS: entry.TimestampUS,
					Payload:     entry.Payload,
				})
			})
			c.logger.Info().Msg("WAL replication hook installed")
		}

		c.logger.Info().
			Int("buffer_size", c.cfg.ReplicationBufferSize).
			Msg("Replication sender started (writer mode)")

	} else {
		// Reader: start receiver to get entries from writer
		writerAddr := c.findWriterAddr()
		if writerAddr == "" {
			c.logger.Warn().Msg("No writer found for replication, will retry when writer joins")
			// Start a goroutine to wait for writer and then start receiver
			go c.waitForWriterAndStartReceiver()
			return nil
		}

		if err := c.startReceiverWithAddr(writerAddr); err != nil {
			return err
		}
	}

	return nil
}

// findWriterAddr finds a healthy writer node's coordinator address.
func (c *Coordinator) findWriterAddr() string {
	nodes := c.registry.GetByRole(RoleWriter)
	for _, node := range nodes {
		if node.State == StateHealthy && node.ID != c.localNode.ID {
			return node.Address
		}
	}
	return ""
}

// waitForWriterAndStartReceiver waits for a writer node to appear and starts the receiver.
func (c *Coordinator) waitForWriterAndStartReceiver() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			writerAddr := c.findWriterAddr()
			if writerAddr != "" {
				c.mu.Lock()
				if c.replicationReceiver == nil {
					if err := c.startReceiverWithAddr(writerAddr); err != nil {
						c.logger.Error().Err(err).Msg("Failed to start replication receiver")
					}
				}
				c.mu.Unlock()
				return
			}
		}
	}
}

// startReceiverWithAddr starts the replication receiver with the given writer address.
// Caller must hold c.mu lock.
func (c *Coordinator) startReceiverWithAddr(writerAddr string) error {
	c.replicationReceiver = replication.NewReceiver(&replication.ReceiverConfig{
		ReaderID:          c.localNode.ID,
		WriterAddr:        writerAddr,
		LocalWAL:          c.walWriter,
		ReconnectInterval: 5 * time.Second,
		AckInterval:       time.Duration(c.cfg.ReplicationAckInterval) * time.Millisecond,
		Logger:            c.logger,
	})

	if err := c.replicationReceiver.Start(c.ctx); err != nil {
		return fmt.Errorf("failed to start replication receiver: %w", err)
	}

	c.logger.Info().
		Str("writer_addr", writerAddr).
		Msg("Replication receiver started (reader mode)")

	return nil
}

// StopReplication stops WAL replication.
func (c *Coordinator) StopReplication() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	if c.replicationSender != nil {
		c.replicationSender.Stop()
		c.replicationSender = nil
	}

	if c.replicationReceiver != nil {
		c.replicationReceiver.Stop()
		c.replicationReceiver = nil
	}
}

// GetReplicationStats returns replication statistics.
func (c *Coordinator) GetReplicationStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := map[string]interface{}{
		"enabled": c.cfg.ReplicationEnabled,
		"role":    string(c.localNode.Role),
	}

	if c.replicationSender != nil {
		stats["sender"] = c.replicationSender.Stats()
	}

	if c.replicationReceiver != nil {
		stats["receiver"] = c.replicationReceiver.Stats()
	}

	return stats
}

// AcceptReplicationConnection handles a replication connection from a reader.
// This is called when a reader sends a MsgReplicateSync message.
func (c *Coordinator) AcceptReplicationConnection(conn net.Conn, syncReq *replication.ReplicateSync) error {
	c.mu.RLock()
	sender := c.replicationSender
	c.mu.RUnlock()

	if sender == nil {
		// Not a writer or replication not enabled
		errMsg := &replication.ReplicateError{
			Code:    replication.ErrCodeNotWriter,
			Message: "This node is not configured as a writer",
		}
		replication.WriteError(conn, errMsg)
		return fmt.Errorf("replication connection rejected: not a writer")
	}

	return sender.AcceptReader(conn, syncReq.ReaderID, syncReq.LastKnownSequence)
}

// GetRaftNode returns the Raft node (may be nil if Raft is not configured).
func (c *Coordinator) GetRaftNode() *raft.Node {
	return c.raftNode
}

// IsLeader returns true if this node is the Raft leader.
// Always returns true if Raft is not configured (standalone mode).
func (c *Coordinator) IsLeader() bool {
	if c.raftNode == nil {
		return true // Standalone mode - this node is always the "leader"
	}
	return c.raftNode.IsLeader()
}

// LeaderAddr returns the address of the current Raft leader.
// Returns empty string if Raft is not configured.
func (c *Coordinator) LeaderAddr() string {
	if c.raftNode == nil {
		return ""
	}
	return c.raftNode.LeaderAddr()
}

// AddNodeViaRaft adds a node to the cluster via Raft consensus.
// Must be called on the leader.
func (c *Coordinator) AddNodeViaRaft(node *Node) error {
	if c.raftNode == nil {
		// No Raft, just register locally
		return c.registry.Register(node)
	}

	if !c.raftNode.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	nodeInfo := &raft.NodeInfo{
		ID:          node.ID,
		Name:        node.Name,
		Role:        string(node.Role),
		ClusterName: node.ClusterName,
		Address:     node.Address,
		APIAddress:  node.APIAddress,
		State:       string(node.GetState()),
		Version:     node.Version,
	}

	return c.raftNode.AddNode(nodeInfo, 5*time.Second)
}

// RemoveNodeViaRaft removes a node from the cluster via Raft consensus.
// Must be called on the leader.
func (c *Coordinator) RemoveNodeViaRaft(nodeID string) error {
	if c.raftNode == nil {
		// No Raft, just unregister locally
		c.registry.Unregister(nodeID)
		return nil
	}

	if !c.raftNode.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	return c.raftNode.RemoveNode(nodeID, 5*time.Second)
}

// UpdateNodeStateViaRaft updates a node's state via Raft consensus.
// Must be called on the leader.
func (c *Coordinator) UpdateNodeStateViaRaft(nodeID string, state NodeState) error {
	if c.raftNode == nil {
		// No Raft, just update locally
		node, exists := c.registry.Get(nodeID)
		if !exists {
			return ErrNodeNotFound
		}
		node.UpdateState(state)
		return c.registry.Register(node)
	}

	if !c.raftNode.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	return c.raftNode.UpdateNodeState(nodeID, string(state), 5*time.Second)
}
