package sharding

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/replication"
	"github.com/rs/zerolog"
)

// ShardReplicationConfig holds configuration for per-shard replication.
type ShardReplicationConfig struct {
	// ShardMap provides shard-to-node mappings
	ShardMap *ShardMap

	// LocalNode is the local node
	LocalNode *cluster.Node

	// BufferSize is the capacity of the entry buffer per shard (default: 10000)
	BufferSize int

	// WriteTimeout for sending entries to replicas
	WriteTimeout time.Duration

	// Logger for replication events
	Logger zerolog.Logger
}

// ShardReplicationManager manages WAL replication for shards where this node is primary.
// Each shard has independent replication streams to its replicas.
type ShardReplicationManager struct {
	cfg       *ShardReplicationConfig
	shards    map[int]*ShardSender // shardID -> sender (only for shards we're primary for)
	mu        sync.RWMutex
	logger    zerolog.Logger
	ctx       context.Context
	cancelFn  context.CancelFunc
	running   atomic.Bool
	wg        sync.WaitGroup
}

// ShardSender handles replication for a single shard from primary to replicas.
type ShardSender struct {
	shardID    int
	replicas   map[string]*ShardReplicaConn // nodeID -> connection
	entryChan  chan *replication.ReplicateEntry
	sequence   atomic.Uint64
	mu         sync.RWMutex
	logger     zerolog.Logger
	cfg        *ShardReplicationConfig
	ctx        context.Context
	cancelFn   context.CancelFunc
	wg         sync.WaitGroup
	running    atomic.Bool

	// Stats
	totalEntriesReceived atomic.Int64
	totalEntriesDropped  atomic.Int64
	totalEntriesSent     atomic.Int64
}

// ShardReplicaConn represents a connection to a replica for a specific shard.
type ShardReplicaConn struct {
	nodeID       string
	node         *cluster.Node
	conn         net.Conn
	writeMu      sync.Mutex
	lastAck      atomic.Uint64
	lastSendTime atomic.Int64
	entriesSent  atomic.Int64
	bytesSent    atomic.Int64
	errors       atomic.Int64
	ctx          context.Context
	cancelFn     context.CancelFunc
}

// NewShardReplicationManager creates a new per-shard replication manager.
func NewShardReplicationManager(cfg *ShardReplicationConfig) *ShardReplicationManager {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 10000
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}

	return &ShardReplicationManager{
		cfg:    cfg,
		shards: make(map[int]*ShardSender),
		logger: cfg.Logger.With().Str("component", "shard-replication-manager").Logger(),
	}
}

// Start begins the replication manager.
func (m *ShardReplicationManager) Start(ctx context.Context) error {
	m.ctx, m.cancelFn = context.WithCancel(ctx)
	m.running.Store(true)

	m.logger.Info().Msg("Shard replication manager started")
	return nil
}

// Stop gracefully shuts down the replication manager.
func (m *ShardReplicationManager) Stop() error {
	if !m.running.Load() {
		return nil
	}

	m.running.Store(false)
	m.cancelFn()

	// Stop all shard senders
	m.mu.Lock()
	for shardID, sender := range m.shards {
		sender.Stop()
		delete(m.shards, shardID)
	}
	m.mu.Unlock()

	m.wg.Wait()
	m.logger.Info().Msg("Shard replication manager stopped")
	return nil
}

// BecomePrimary is called when this node becomes primary for a shard.
// It starts replication to the shard's replicas.
func (m *ShardReplicationManager) BecomePrimary(shardID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already primary
	if _, exists := m.shards[shardID]; exists {
		return nil
	}

	// Get shard group to find replicas
	group := m.cfg.ShardMap.GetShardGroup(shardID)
	if group == nil {
		return fmt.Errorf("shard %d not found", shardID)
	}

	// Create sender for this shard
	sender := newShardSender(shardID, m.cfg, m.logger)
	if err := sender.Start(m.ctx); err != nil {
		return fmt.Errorf("start shard sender: %w", err)
	}

	// Connect to replicas
	for _, replica := range group.Replicas {
		if replica.ID != m.cfg.LocalNode.ID {
			sender.AddReplica(replica)
		}
	}

	m.shards[shardID] = sender

	m.logger.Info().
		Int("shard_id", shardID).
		Int("replica_count", len(group.Replicas)).
		Msg("Started replication as primary for shard")

	return nil
}

// StopPrimary is called when this node stops being primary for a shard.
func (m *ShardReplicationManager) StopPrimary(shardID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sender, exists := m.shards[shardID]; exists {
		sender.Stop()
		delete(m.shards, shardID)
		m.logger.Info().Int("shard_id", shardID).Msg("Stopped replication for shard")
	}
}

// Replicate sends an entry to all replicas for a shard.
// Called when new data is written to the shard.
func (m *ShardReplicationManager) Replicate(shardID int, entry *replication.ReplicateEntry) {
	m.mu.RLock()
	sender, exists := m.shards[shardID]
	m.mu.RUnlock()

	if !exists {
		return // Not primary for this shard
	}

	sender.Replicate(entry)
}

// AddReplica adds a replica connection for a shard.
func (m *ShardReplicationManager) AddReplica(shardID int, replica *cluster.Node) error {
	m.mu.RLock()
	sender, exists := m.shards[shardID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("not primary for shard %d", shardID)
	}

	return sender.AddReplica(replica)
}

// RemoveReplica removes a replica connection for a shard.
func (m *ShardReplicationManager) RemoveReplica(shardID int, nodeID string) {
	m.mu.RLock()
	sender, exists := m.shards[shardID]
	m.mu.RUnlock()

	if exists {
		sender.RemoveReplica(nodeID)
	}
}

// GetShardStats returns statistics for a specific shard's replication.
func (m *ShardReplicationManager) GetShardStats(shardID int) map[string]interface{} {
	m.mu.RLock()
	sender, exists := m.shards[shardID]
	m.mu.RUnlock()

	if !exists {
		return nil
	}

	return sender.Stats()
}

// Stats returns overall replication statistics.
func (m *ShardReplicationManager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shardStats := make(map[int]map[string]interface{})
	for shardID, sender := range m.shards {
		shardStats[shardID] = sender.Stats()
	}

	return map[string]interface{}{
		"running":           m.running.Load(),
		"shards_as_primary": len(m.shards),
		"shard_stats":       shardStats,
	}
}

// newShardSender creates a new sender for a specific shard.
func newShardSender(shardID int, cfg *ShardReplicationConfig, logger zerolog.Logger) *ShardSender {
	return &ShardSender{
		shardID:   shardID,
		replicas:  make(map[string]*ShardReplicaConn),
		entryChan: make(chan *replication.ReplicateEntry, cfg.BufferSize),
		cfg:       cfg,
		logger:    logger.With().Int("shard_id", shardID).Logger(),
	}
}

// Start begins the shard sender's distribution loop.
func (s *ShardSender) Start(ctx context.Context) error {
	s.ctx, s.cancelFn = context.WithCancel(ctx)
	s.running.Store(true)

	s.wg.Add(1)
	go s.distributionLoop()

	s.logger.Debug().Msg("Shard sender started")
	return nil
}

// Stop gracefully shuts down the shard sender.
func (s *ShardSender) Stop() {
	if !s.running.Load() {
		return
	}

	s.running.Store(false)
	s.cancelFn()

	// Close all replica connections
	s.mu.Lock()
	for nodeID, replica := range s.replicas {
		replica.cancelFn()
		if replica.conn != nil {
			replica.conn.Close()
		}
		delete(s.replicas, nodeID)
	}
	s.mu.Unlock()

	s.wg.Wait()
	s.logger.Debug().Msg("Shard sender stopped")
}

// AddReplica adds a replica connection.
func (s *ShardSender) AddReplica(node *cluster.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already connected
	if _, exists := s.replicas[node.ID]; exists {
		return nil
	}

	replicaCtx, cancel := context.WithCancel(s.ctx)
	replica := &ShardReplicaConn{
		nodeID:   node.ID,
		node:     node,
		ctx:      replicaCtx,
		cancelFn: cancel,
	}
	replica.lastSendTime.Store(time.Now().UnixNano())

	s.replicas[node.ID] = replica

	// Start connection goroutine
	s.wg.Add(1)
	go s.maintainConnection(replica)

	s.logger.Info().
		Str("replica_id", node.ID).
		Str("replica_addr", node.Address).
		Msg("Added replica for shard replication")

	return nil
}

// RemoveReplica removes a replica connection.
func (s *ShardSender) RemoveReplica(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if replica, exists := s.replicas[nodeID]; exists {
		replica.cancelFn()
		if replica.conn != nil {
			replica.conn.Close()
		}
		delete(s.replicas, nodeID)
		s.logger.Info().Str("replica_id", nodeID).Msg("Removed replica from shard replication")
	}
}

// Replicate queues an entry for replication.
func (s *ShardSender) Replicate(entry *replication.ReplicateEntry) {
	if !s.running.Load() {
		return
	}

	// Assign sequence number
	entry.Sequence = s.sequence.Add(1)
	s.totalEntriesReceived.Add(1)

	// Non-blocking send
	select {
	case s.entryChan <- entry:
		// Entry queued
	default:
		// Buffer full
		s.totalEntriesDropped.Add(1)
		s.logger.Warn().
			Uint64("sequence", entry.Sequence).
			Msg("Shard replication buffer full, entry dropped")
	}
}

// distributionLoop sends entries to all replicas.
func (s *ShardSender) distributionLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case entry := <-s.entryChan:
			s.broadcastEntry(entry)
		}
	}
}

// broadcastEntry sends an entry to all connected replicas.
func (s *ShardSender) broadcastEntry(entry *replication.ReplicateEntry) {
	s.mu.RLock()
	replicas := make([]*ShardReplicaConn, 0, len(s.replicas))
	for _, r := range s.replicas {
		replicas = append(replicas, r)
	}
	s.mu.RUnlock()

	for _, replica := range replicas {
		if err := s.sendToReplica(replica, entry); err != nil {
			replica.errors.Add(1)
			s.logger.Debug().
				Err(err).
				Str("replica_id", replica.nodeID).
				Uint64("sequence", entry.Sequence).
				Msg("Failed to send entry to replica")
			// Don't remove replica - maintainConnection will handle reconnection
		}
	}
}

// sendToReplica sends an entry to a specific replica.
func (s *ShardSender) sendToReplica(replica *ShardReplicaConn, entry *replication.ReplicateEntry) error {
	replica.writeMu.Lock()
	defer replica.writeMu.Unlock()

	// Check if replica is still active
	select {
	case <-replica.ctx.Done():
		return context.Canceled
	default:
	}

	if replica.conn == nil {
		return fmt.Errorf("not connected")
	}

	// Set write deadline
	if err := replica.conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
		return err
	}

	// Write entry
	if err := replication.WriteEntry(replica.conn, entry); err != nil {
		return err
	}

	// Update stats
	replica.entriesSent.Add(1)
	replica.bytesSent.Add(int64(len(entry.Payload)))
	replica.lastSendTime.Store(time.Now().UnixNano())
	s.totalEntriesSent.Add(1)

	return nil
}

// maintainConnection keeps a connection to a replica alive.
func (s *ShardSender) maintainConnection(replica *ShardReplicaConn) {
	defer s.wg.Done()

	reconnectInterval := 5 * time.Second

	for {
		select {
		case <-replica.ctx.Done():
			return
		default:
		}

		// Try to connect
		if err := s.connectToReplica(replica); err != nil {
			s.logger.Debug().
				Err(err).
				Str("replica_id", replica.nodeID).
				Dur("retry_in", reconnectInterval).
				Msg("Failed to connect to replica")

			select {
			case <-replica.ctx.Done():
				return
			case <-time.After(reconnectInterval):
				continue
			}
		}

		// Connected - run receive loop for acks
		s.receiveLoop(replica)

		// Disconnected
		replica.writeMu.Lock()
		if replica.conn != nil {
			replica.conn.Close()
			replica.conn = nil
		}
		replica.writeMu.Unlock()

		select {
		case <-replica.ctx.Done():
			return
		case <-time.After(reconnectInterval):
		}
	}
}

// connectToReplica establishes connection to a replica.
func (s *ShardSender) connectToReplica(replica *ShardReplicaConn) error {
	addr := replica.node.Address
	if addr == "" {
		return fmt.Errorf("replica has no address")
	}

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	// Send sync request
	syncReq := &replication.ReplicateSync{
		ReaderID:          s.cfg.LocalNode.ID + "-shard-" + fmt.Sprintf("%d", s.shardID),
		LastKnownSequence: replica.lastAck.Load(),
	}

	if err := replication.WriteSync(conn, syncReq); err != nil {
		conn.Close()
		return fmt.Errorf("write sync: %w", err)
	}

	// Read sync ack
	msgType, payload, err := replication.ReadMessage(conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("read sync ack: %w", err)
	}

	if msgType != replication.MsgReplicateSyncAck {
		conn.Close()
		return fmt.Errorf("unexpected message type: %d", msgType)
	}

	syncAck, err := replication.ParseSyncAck(payload)
	if err != nil {
		conn.Close()
		return fmt.Errorf("parse sync ack: %w", err)
	}

	if syncAck.Error != "" {
		conn.Close()
		return fmt.Errorf("sync rejected: %s", syncAck.Error)
	}

	// Store connection
	replica.writeMu.Lock()
	replica.conn = conn
	replica.writeMu.Unlock()

	s.logger.Info().
		Str("replica_id", replica.nodeID).
		Uint64("remote_seq", syncAck.CurrentSequence).
		Bool("can_resume", syncAck.CanResume).
		Msg("Connected to replica for shard replication")

	return nil
}

// receiveLoop handles incoming acks from a replica.
func (s *ShardSender) receiveLoop(replica *ShardReplicaConn) {
	for {
		select {
		case <-replica.ctx.Done():
			return
		default:
		}

		replica.writeMu.Lock()
		conn := replica.conn
		replica.writeMu.Unlock()

		if conn == nil {
			return
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		msgType, payload, err := replication.ReadMessage(conn)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			s.logger.Debug().
				Err(err).
				Str("replica_id", replica.nodeID).
				Msg("Replica connection closed")
			return
		}

		switch msgType {
		case replication.MsgReplicateAck:
			ack, err := replication.ParseAck(payload)
			if err != nil {
				s.logger.Error().Err(err).Str("replica_id", replica.nodeID).Msg("Failed to parse ack")
				continue
			}
			replica.lastAck.Store(ack.LastSequence)
			s.logger.Debug().
				Str("replica_id", replica.nodeID).
				Uint64("last_seq", ack.LastSequence).
				Msg("Received ack from replica")

		default:
			s.logger.Warn().
				Str("replica_id", replica.nodeID).
				Uint8("msg_type", msgType).
				Msg("Unexpected message type from replica")
		}
	}
}

// Stats returns statistics for this shard sender.
func (s *ShardSender) Stats() map[string]interface{} {
	s.mu.RLock()
	replicaStats := make([]map[string]interface{}, 0, len(s.replicas))
	for id, replica := range s.replicas {
		lastSend := time.Unix(0, replica.lastSendTime.Load())
		connected := replica.conn != nil
		replicaStats = append(replicaStats, map[string]interface{}{
			"replica_id":     id,
			"connected":      connected,
			"last_ack_seq":   replica.lastAck.Load(),
			"entries_sent":   replica.entriesSent.Load(),
			"bytes_sent":     replica.bytesSent.Load(),
			"last_send_time": lastSend.Format(time.RFC3339),
			"errors":         replica.errors.Load(),
			"lag":            s.sequence.Load() - replica.lastAck.Load(),
		})
	}
	s.mu.RUnlock()

	return map[string]interface{}{
		"shard_id":               s.shardID,
		"running":                s.running.Load(),
		"current_sequence":       s.sequence.Load(),
		"buffer_used":            len(s.entryChan),
		"replica_count":          len(replicaStats),
		"replicas":               replicaStats,
		"total_entries_received": s.totalEntriesReceived.Load(),
		"total_entries_dropped":  s.totalEntriesDropped.Load(),
		"total_entries_sent":     s.totalEntriesSent.Load(),
	}
}
