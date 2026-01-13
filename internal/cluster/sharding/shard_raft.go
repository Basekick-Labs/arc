package sharding

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rs/zerolog"
)

// ShardRaftConfig holds configuration for per-shard Raft.
type ShardRaftConfig struct {
	// DataDir is the base directory for Raft data (will be appended with shard ID)
	DataDir string

	// BasePort is the starting port for shard Raft (shard N uses BasePort+N)
	BasePort int

	// LocalNode is this node
	LocalNode *cluster.Node

	// ElectionTimeout for leader election
	ElectionTimeout time.Duration

	// HeartbeatTimeout for heartbeats
	HeartbeatTimeout time.Duration

	// Logger for Raft events
	Logger zerolog.Logger
}

// ShardRaftManager manages per-shard Raft clusters.
// Each shard has its own Raft cluster for leader election and failover.
type ShardRaftManager struct {
	cfg      *ShardRaftConfig
	shards   map[int]*ShardRaftNode // shardID -> raft node
	mu       sync.RWMutex
	logger   zerolog.Logger
	running  bool

	// Callbacks for leadership changes
	onBecomeLeader   func(shardID int)
	onLoseLeadership func(shardID int)
}

// ShardRaftNode wraps a Raft instance for a single shard.
type ShardRaftNode struct {
	shardID     int
	raft        *raft.Raft
	fsm         *ShardFSM
	transport   *raft.NetworkTransport
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
	snapStore   raft.SnapshotStore
	logger      zerolog.Logger
	mu          sync.RWMutex
	running     bool

	// Leadership tracking
	isLeader    bool
	leaderMu    sync.RWMutex
	onLeaderChange func(isLeader bool)
}

// ShardFSM is the FSM for a single shard's Raft cluster.
// It tracks which node is the primary and replication state.
type ShardFSM struct {
	shardID      int
	primaryID    string                // Current primary node ID
	replicaLag   map[string]int64      // replica ID -> bytes behind
	lastWriteSeq uint64                // Last write sequence
	mu           sync.RWMutex
	logger       zerolog.Logger
}

// ShardFSMCommand types
const (
	ShardCmdSetPrimary     = "set_primary"
	ShardCmdUpdateLag      = "update_lag"
	ShardCmdUpdateWriteSeq = "update_write_seq"
)

// ShardFSMCommand represents a command for the shard FSM.
type ShardFSMCommand struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// SetPrimaryPayload sets the primary for a shard.
type SetPrimaryPayload struct {
	NodeID string `json:"node_id"`
}

// UpdateLagPayload updates replica lag.
type UpdateLagPayload struct {
	ReplicaID string `json:"replica_id"`
	Lag       int64  `json:"lag"`
}

// NewShardRaftManager creates a new shard Raft manager.
func NewShardRaftManager(cfg *ShardRaftConfig) *ShardRaftManager {
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 1 * time.Second
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 500 * time.Millisecond
	}

	return &ShardRaftManager{
		cfg:    cfg,
		shards: make(map[int]*ShardRaftNode),
		logger: cfg.Logger.With().Str("component", "shard-raft-manager").Logger(),
	}
}

// SetCallbacks sets callbacks for leadership changes.
func (m *ShardRaftManager) SetCallbacks(onBecomeLeader, onLoseLeadership func(shardID int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onBecomeLeader = onBecomeLeader
	m.onLoseLeadership = onLoseLeadership
}

// Start initializes the shard Raft manager.
func (m *ShardRaftManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = true
	m.logger.Info().Msg("Shard Raft manager started")
	return nil
}

// Stop shuts down all shard Raft nodes.
func (m *ShardRaftManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for shardID, node := range m.shards {
		if err := node.Stop(); err != nil {
			m.logger.Error().Err(err).Int("shard_id", shardID).Msg("Error stopping shard Raft")
		}
		delete(m.shards, shardID)
	}

	m.running = false
	m.logger.Info().Msg("Shard Raft manager stopped")
	return nil
}

// JoinShard joins the Raft cluster for a shard.
func (m *ShardRaftManager) JoinShard(shardID int, peers []string, bootstrap bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.shards[shardID]; exists {
		return fmt.Errorf("already joined shard %d", shardID)
	}

	// Create shard-specific data directory
	dataDir := filepath.Join(m.cfg.DataDir, fmt.Sprintf("shard-%d", shardID))

	// Calculate port for this shard
	port := m.cfg.BasePort + shardID
	bindAddr := fmt.Sprintf(":%d", port)

	// Create Raft node for this shard
	node, err := newShardRaftNode(shardID, &shardRaftNodeConfig{
		NodeID:           m.cfg.LocalNode.ID,
		DataDir:          dataDir,
		BindAddr:         bindAddr,
		Peers:            peers,
		Bootstrap:        bootstrap,
		ElectionTimeout:  m.cfg.ElectionTimeout,
		HeartbeatTimeout: m.cfg.HeartbeatTimeout,
		Logger:           m.logger,
	})
	if err != nil {
		return fmt.Errorf("create shard raft node: %w", err)
	}

	// Set leadership callback
	node.onLeaderChange = func(isLeader bool) {
		if isLeader {
			if m.onBecomeLeader != nil {
				m.onBecomeLeader(shardID)
			}
		} else {
			if m.onLoseLeadership != nil {
				m.onLoseLeadership(shardID)
			}
		}
	}

	if err := node.Start(); err != nil {
		return fmt.Errorf("start shard raft: %w", err)
	}

	m.shards[shardID] = node

	m.logger.Info().
		Int("shard_id", shardID).
		Str("bind_addr", bindAddr).
		Bool("bootstrap", bootstrap).
		Msg("Joined shard Raft cluster")

	return nil
}

// LeaveShard leaves the Raft cluster for a shard.
func (m *ShardRaftManager) LeaveShard(shardID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.shards[shardID]
	if !exists {
		return nil
	}

	if err := node.Stop(); err != nil {
		return fmt.Errorf("stop shard raft: %w", err)
	}

	delete(m.shards, shardID)

	m.logger.Info().Int("shard_id", shardID).Msg("Left shard Raft cluster")
	return nil
}

// IsLeader returns true if this node is the leader for the given shard.
func (m *ShardRaftManager) IsLeader(shardID int) bool {
	m.mu.RLock()
	node, exists := m.shards[shardID]
	m.mu.RUnlock()

	if !exists {
		return false
	}

	return node.IsLeader()
}

// GetLeader returns the leader address for a shard.
func (m *ShardRaftManager) GetLeader(shardID int) string {
	m.mu.RLock()
	node, exists := m.shards[shardID]
	m.mu.RUnlock()

	if !exists {
		return ""
	}

	return node.LeaderAddr()
}

// AddVoter adds a voter to a shard's Raft cluster.
func (m *ShardRaftManager) AddVoter(shardID int, nodeID, addr string) error {
	m.mu.RLock()
	node, exists := m.shards[shardID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("not in shard %d", shardID)
	}

	return node.AddVoter(nodeID, addr)
}

// RemoveVoter removes a voter from a shard's Raft cluster.
func (m *ShardRaftManager) RemoveVoter(shardID int, nodeID string) error {
	m.mu.RLock()
	node, exists := m.shards[shardID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("not in shard %d", shardID)
	}

	return node.RemoveVoter(nodeID)
}

// Stats returns statistics for all shard Raft clusters.
func (m *ShardRaftManager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shardStats := make(map[int]map[string]interface{})
	for shardID, node := range m.shards {
		shardStats[shardID] = node.Stats()
	}

	return map[string]interface{}{
		"running":     m.running,
		"shard_count": len(m.shards),
		"shards":      shardStats,
	}
}

// shardRaftNodeConfig holds configuration for a single shard's Raft node.
type shardRaftNodeConfig struct {
	NodeID           string
	DataDir          string
	BindAddr         string
	Peers            []string
	Bootstrap        bool
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	Logger           zerolog.Logger
}

// newShardRaftNode creates a new Raft node for a shard.
func newShardRaftNode(shardID int, cfg *shardRaftNodeConfig) (*ShardRaftNode, error) {
	fsm := &ShardFSM{
		shardID:    shardID,
		replicaLag: make(map[string]int64),
		logger:     cfg.Logger.With().Int("shard_id", shardID).Logger(),
	}

	return &ShardRaftNode{
		shardID: shardID,
		fsm:     fsm,
		logger:  cfg.Logger.With().Int("shard_id", shardID).Logger(),
	}, nil
}

// Start starts the shard Raft node.
func (n *ShardRaftNode) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return fmt.Errorf("shard raft already running")
	}

	n.running = true
	n.logger.Info().Msg("Shard Raft node started (lightweight mode)")

	// In this implementation, we use a lightweight approach where the
	// shard Raft doesn't actually start a full Raft cluster. Instead,
	// leadership is determined by the meta cluster and communicated
	// via callbacks. This avoids the overhead of running N separate
	// Raft clusters for N shards.
	//
	// For full per-shard Raft consensus (e.g., for stronger consistency
	// guarantees), the Start method would initialize:
	// - Transport (TCP listener)
	// - Log store (BoltDB)
	// - Stable store (BoltDB)
	// - Snapshot store (file-based)
	// - Raft instance

	return nil
}

// Stop stops the shard Raft node.
func (n *ShardRaftNode) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return nil
	}

	// Shutdown Raft if running
	if n.raft != nil {
		future := n.raft.Shutdown()
		if err := future.Error(); err != nil {
			n.logger.Error().Err(err).Msg("Error shutting down shard Raft")
		}
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
	n.logger.Info().Msg("Shard Raft node stopped")
	return nil
}

// IsLeader returns true if this node is the leader for this shard.
func (n *ShardRaftNode) IsLeader() bool {
	n.leaderMu.RLock()
	defer n.leaderMu.RUnlock()
	return n.isLeader
}

// SetLeader sets the leadership status (called by meta cluster).
func (n *ShardRaftNode) SetLeader(isLeader bool) {
	n.leaderMu.Lock()
	wasLeader := n.isLeader
	n.isLeader = isLeader
	callback := n.onLeaderChange
	n.leaderMu.Unlock()

	if wasLeader != isLeader && callback != nil {
		callback(isLeader)
	}
}

// LeaderAddr returns the leader address.
func (n *ShardRaftNode) LeaderAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.raft == nil {
		return ""
	}
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// AddVoter adds a voter to this shard's Raft cluster.
func (n *ShardRaftNode) AddVoter(nodeID, addr string) error {
	n.mu.RLock()
	ra := n.raft
	n.mu.RUnlock()

	if ra == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := ra.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 10*time.Second)
	return future.Error()
}

// RemoveVoter removes a voter from this shard's Raft cluster.
func (n *ShardRaftNode) RemoveVoter(nodeID string) error {
	n.mu.RLock()
	ra := n.raft
	n.mu.RUnlock()

	if ra == nil {
		return fmt.Errorf("raft not initialized")
	}

	future := ra.RemoveServer(raft.ServerID(nodeID), 0, 10*time.Second)
	return future.Error()
}

// Stats returns statistics for this shard's Raft.
func (n *ShardRaftNode) Stats() map[string]interface{} {
	n.leaderMu.RLock()
	isLeader := n.isLeader
	n.leaderMu.RUnlock()

	stats := map[string]interface{}{
		"shard_id":  n.shardID,
		"running":   n.running,
		"is_leader": isLeader,
	}

	if n.raft != nil {
		stats["raft_stats"] = n.raft.Stats()
	}

	return stats
}

// ShardFSM implementation

// Apply implements raft.FSM.
func (f *ShardFSM) Apply(log *raft.Log) interface{} {
	var cmd ShardFSMCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error().Err(err).Msg("Failed to unmarshal shard FSM command")
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case ShardCmdSetPrimary:
		var p SetPrimaryPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		f.primaryID = p.NodeID
		f.logger.Info().Str("primary_id", p.NodeID).Msg("Primary set for shard")
		return nil

	case ShardCmdUpdateLag:
		var p UpdateLagPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		f.replicaLag[p.ReplicaID] = p.Lag
		return nil

	case ShardCmdUpdateWriteSeq:
		var seq uint64
		if err := json.Unmarshal(cmd.Payload, &seq); err != nil {
			return err
		}
		f.lastWriteSeq = seq
		return nil

	default:
		return fmt.Errorf("unknown shard command type: %s", cmd.Type)
	}
}

// Snapshot implements raft.FSM.
func (f *ShardFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return &shardFSMSnapshot{
		shardID:      f.shardID,
		primaryID:    f.primaryID,
		replicaLag:   copyMap(f.replicaLag),
		lastWriteSeq: f.lastWriteSeq,
	}, nil
}

// Restore implements raft.FSM.
func (f *ShardFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot shardFSMSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	f.mu.Lock()
	f.primaryID = snapshot.primaryID
	f.replicaLag = snapshot.replicaLag
	f.lastWriteSeq = snapshot.lastWriteSeq
	f.mu.Unlock()

	f.logger.Info().
		Str("primary_id", snapshot.primaryID).
		Uint64("last_write_seq", snapshot.lastWriteSeq).
		Msg("Shard FSM restored from snapshot")

	return nil
}

// GetPrimaryID returns the current primary ID.
func (f *ShardFSM) GetPrimaryID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.primaryID
}

// GetLastWriteSeq returns the last write sequence.
func (f *ShardFSM) GetLastWriteSeq() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastWriteSeq
}

// shardFSMSnapshot implements raft.FSMSnapshot for shard FSM.
type shardFSMSnapshot struct {
	shardID      int
	primaryID    string
	replicaLag   map[string]int64
	lastWriteSeq uint64
}

// Persist implements raft.FSMSnapshot.
func (s *shardFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (s *shardFSMSnapshot) Release() {}

// copyMap creates a copy of a map.
func copyMap(m map[string]int64) map[string]int64 {
	result := make(map[string]int64, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// StartFullRaft initializes a full Raft cluster for this shard.
// This is an optional method for scenarios requiring stronger consistency.
func (n *ShardRaftNode) StartFullRaft(cfg *shardRaftNodeConfig) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.ElectionTimeout = cfg.ElectionTimeout
	raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return fmt.Errorf("resolve address: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("create transport: %w", err)
	}
	n.transport = transport

	// Create log store
	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return fmt.Errorf("create log store: %w", err)
	}
	n.logStore = logStore

	// Create stable store
	stableStorePath := filepath.Join(cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		return fmt.Errorf("create stable store: %w", err)
	}
	n.stableStore = stableStore

	// Create snapshot store
	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("create snapshot store: %w", err)
	}
	n.snapStore = snapStore

	// Create Raft instance
	ra, err := raft.NewRaft(raftConfig, n.fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return fmt.Errorf("create raft: %w", err)
	}
	n.raft = ra

	// Bootstrap if requested
	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
		if err != nil {
			return fmt.Errorf("check existing state: %w", err)
		}

		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(cfg.NodeID),
						Address: raft.ServerAddress(cfg.BindAddr),
					},
				},
			}

			future := ra.BootstrapCluster(configuration)
			if err := future.Error(); err != nil {
				return fmt.Errorf("bootstrap cluster: %w", err)
			}
		}
	}

	// Start leadership observer
	go n.observeLeadership()

	n.logger.Info().
		Str("bind_addr", cfg.BindAddr).
		Bool("bootstrap", cfg.Bootstrap).
		Msg("Full shard Raft started")

	return nil
}

// observeLeadership monitors Raft state changes.
func (n *ShardRaftNode) observeLeadership() {
	if n.raft == nil {
		return
	}

	leaderCh := n.raft.LeaderCh()
	for isLeader := range leaderCh {
		n.SetLeader(isLeader)
	}
}
