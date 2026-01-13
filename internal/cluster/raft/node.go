package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rs/zerolog"
)

// NodeConfig holds configuration for the Raft node.
type NodeConfig struct {
	// NodeID is the unique identifier for this node in the Raft cluster.
	NodeID string
	// DataDir is the directory where Raft data is stored.
	DataDir string
	// BindAddr is the address to bind the Raft transport to.
	BindAddr string
	// AdvertiseAddr is the address advertised to other nodes.
	AdvertiseAddr string
	// Bootstrap indicates if this node should bootstrap a new cluster.
	Bootstrap bool
	// Peers is the list of peer addresses for joining an existing cluster.
	Peers []string

	// Timeouts
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout    time.Duration

	// Snapshots
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	TrailingLogs      uint64

	Logger zerolog.Logger
}

// DefaultNodeConfig returns a NodeConfig with sensible defaults.
func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		ElectionTimeout:    1 * time.Second,
		HeartbeatTimeout:   500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		SnapshotInterval:   5 * time.Minute,
		SnapshotThreshold:  10000,
		TrailingLogs:       10000,
	}
}

// Node wraps hashicorp/raft and provides a higher-level API.
type Node struct {
	cfg       *NodeConfig
	raft      *raft.Raft
	fsm       *ClusterFSM
	transport *raft.NetworkTransport
	logStore  *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
	snapStore raft.SnapshotStore

	mu      sync.RWMutex
	running bool

	logger zerolog.Logger
}

// NewNode creates a new Raft node.
func NewNode(cfg *NodeConfig, fsm *ClusterFSM) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("bind address is required")
	}

	// Set defaults
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 1 * time.Second
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 500 * time.Millisecond
	}
	if cfg.LeaderLeaseTimeout == 0 {
		cfg.LeaderLeaseTimeout = 500 * time.Millisecond
	}
	if cfg.CommitTimeout == 0 {
		cfg.CommitTimeout = 50 * time.Millisecond
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 10000
	}

	return &Node{
		cfg:    cfg,
		fsm:    fsm,
		logger: cfg.Logger.With().Str("component", "raft-node").Logger(),
	}, nil
}

// Start starts the Raft node.
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return fmt.Errorf("raft node already running")
	}

	// Ensure data directory exists
	if err := os.MkdirAll(n.cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(n.cfg.NodeID)

	// Use zerolog adapter for raft's internal logging
	raftConfig.Logger = newZerologAdapter(n.logger, "raft")

	// Only override non-zero values to preserve sensible defaults
	if n.cfg.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = n.cfg.ElectionTimeout
	}
	if n.cfg.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = n.cfg.HeartbeatTimeout
	}
	if n.cfg.LeaderLeaseTimeout > 0 {
		raftConfig.LeaderLeaseTimeout = n.cfg.LeaderLeaseTimeout
	}
	if n.cfg.CommitTimeout > 0 {
		raftConfig.CommitTimeout = n.cfg.CommitTimeout
	}
	if n.cfg.SnapshotInterval > 0 {
		raftConfig.SnapshotInterval = n.cfg.SnapshotInterval
	}
	if n.cfg.SnapshotThreshold > 0 {
		raftConfig.SnapshotThreshold = n.cfg.SnapshotThreshold
	}
	if n.cfg.TrailingLogs > 0 {
		raftConfig.TrailingLogs = n.cfg.TrailingLogs
	}

	// Create transport
	advertiseAddr := n.cfg.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = n.cfg.BindAddr
	}

	addr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve advertise address: %w", err)
	}

	transport, err := raft.NewTCPTransport(n.cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create transport: %w", err)
	}
	n.transport = transport

	// Create log store
	logStorePath := filepath.Join(n.cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return fmt.Errorf("failed to create log store: %w", err)
	}
	n.logStore = logStore

	// Create stable store
	stableStorePath := filepath.Join(n.cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		return fmt.Errorf("failed to create stable store: %w", err)
	}
	n.stableStore = stableStore

	// Create snapshot store
	snapStore, err := raft.NewFileSnapshotStore(n.cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}
	n.snapStore = snapStore

	// Create Raft instance
	ra, err := raft.NewRaft(raftConfig, n.fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return fmt.Errorf("failed to create raft instance: %w", err)
	}
	n.raft = ra

	// Bootstrap if requested and no existing state
	if n.cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
		if err != nil {
			return fmt.Errorf("failed to check existing state: %w", err)
		}

		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(n.cfg.NodeID),
						Address: raft.ServerAddress(advertiseAddr),
					},
				},
			}

			future := ra.BootstrapCluster(configuration)
			if err := future.Error(); err != nil {
				return fmt.Errorf("failed to bootstrap cluster: %w", err)
			}

			n.logger.Info().
				Str("node_id", n.cfg.NodeID).
				Str("address", advertiseAddr).
				Msg("Bootstrapped new Raft cluster")
		}
	}

	n.running = true

	n.logger.Info().
		Str("node_id", n.cfg.NodeID).
		Str("bind_addr", n.cfg.BindAddr).
		Bool("bootstrap", n.cfg.Bootstrap).
		Msg("Raft node started")

	return nil
}

// Stop stops the Raft node.
func (n *Node) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return nil
	}

	// Shutdown Raft
	future := n.raft.Shutdown()
	if err := future.Error(); err != nil {
		n.logger.Error().Err(err).Msg("Error shutting down Raft")
	}

	// Close stores
	if n.logStore != nil {
		n.logStore.Close()
	}
	if n.stableStore != nil {
		n.stableStore.Close()
	}
	if n.transport != nil {
		n.transport.Close()
	}

	n.running = false

	n.logger.Info().Msg("Raft node stopped")
	return nil
}

// IsLeader returns true if this node is the Raft leader.
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.raft == nil {
		return false
	}
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (n *Node) LeaderAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.raft == nil {
		return ""
	}
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the ID of the current leader.
func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.raft == nil {
		return ""
	}
	_, id := n.raft.LeaderWithID()
	return string(id)
}

// State returns the current Raft state.
func (n *Node) State() raft.RaftState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.raft == nil {
		return raft.Shutdown
	}
	return n.raft.State()
}

// Apply applies a command to the Raft cluster.
// This can only be called on the leader.
func (n *Node) Apply(cmd *Command, timeout time.Duration) error {
	n.mu.RLock()
	ra := n.raft
	n.mu.RUnlock()

	if ra == nil {
		return fmt.Errorf("raft not initialized")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	future := ra.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}

	// Check for application error
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}

	return nil
}

// AddVoter adds a voting member to the cluster.
func (n *Node) AddVoter(nodeID, addr string, timeout time.Duration) error {
	n.mu.RLock()
	ra := n.raft
	n.mu.RUnlock()

	if ra == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := ra.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, timeout)
	return future.Error()
}

// RemoveServer removes a member from the cluster.
func (n *Node) RemoveServer(nodeID string, timeout time.Duration) error {
	n.mu.RLock()
	ra := n.raft
	n.mu.RUnlock()

	if ra == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := ra.RemoveServer(raft.ServerID(nodeID), 0, timeout)
	return future.Error()
}

// GetConfiguration returns the current Raft configuration.
func (n *Node) GetConfiguration() (raft.Configuration, error) {
	n.mu.RLock()
	ra := n.raft
	n.mu.RUnlock()

	if ra == nil {
		return raft.Configuration{}, fmt.Errorf("raft not initialized")
	}

	future := ra.GetConfiguration()
	if err := future.Error(); err != nil {
		return raft.Configuration{}, err
	}
	return future.Configuration(), nil
}

// Stats returns Raft statistics.
func (n *Node) Stats() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.raft == nil {
		return nil
	}
	return n.raft.Stats()
}

// FSM returns the FSM.
func (n *Node) FSM() *ClusterFSM {
	return n.fsm
}

// WaitForLeader blocks until a leader is elected or timeout.
func (n *Node) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if n.LeaderAddr() != "" {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for leader")
		}
	}
}

// AddNode adds a node to the cluster state via Raft.
func (n *Node) AddNode(node *NodeInfo, timeout time.Duration) error {
	payload, err := json.Marshal(AddNodePayload{Node: *node})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := &Command{
		Type:    CommandAddNode,
		Payload: payload,
	}

	return n.Apply(cmd, timeout)
}

// RemoveNode removes a node from the cluster state via Raft.
func (n *Node) RemoveNode(nodeID string, timeout time.Duration) error {
	payload, err := json.Marshal(RemoveNodePayload{NodeID: nodeID})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := &Command{
		Type:    CommandRemoveNode,
		Payload: payload,
	}

	return n.Apply(cmd, timeout)
}

// UpdateNodeState updates a node's state via Raft.
func (n *Node) UpdateNodeState(nodeID, newState string, timeout time.Duration) error {
	payload, err := json.Marshal(UpdateNodeStatePayload{NodeID: nodeID, NewState: newState})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := &Command{
		Type:    CommandUpdateNodeState,
		Payload: payload,
	}

	return n.Apply(cmd, timeout)
}
