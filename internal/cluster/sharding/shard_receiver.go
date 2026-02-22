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
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// ShardReceiverConfig holds configuration for the shard receiver.
type ShardReceiverConfig struct {
	// ShardMap provides shard-to-node mappings
	ShardMap *ShardMap

	// LocalNode is this node
	LocalNode *cluster.Node

	// LocalWAL writes received entries (optional, for durability)
	LocalWAL replication.WALWriter

	// IngestHandler applies entries to local buffer (optional)
	IngestHandler replication.IngestHandler

	// ReconnectInterval for reconnecting to primaries
	ReconnectInterval time.Duration

	// AckInterval for sending acknowledgments
	AckInterval time.Duration

	// Logger for receiver events
	Logger zerolog.Logger
}

// ShardReceiverManager manages receiving replication data for shards where this node is a replica.
// Unlike the simple Receiver, this handles multiple incoming streams (one per shard primary).
type ShardReceiverManager struct {
	cfg      *ShardReceiverConfig
	shards   map[int]*ShardReceiver // shardID -> receiver
	mu       sync.RWMutex
	logger   zerolog.Logger
	ctx      context.Context
	cancelFn context.CancelFunc
	running  atomic.Bool
	wg       sync.WaitGroup
}

// ShardReceiver handles replication for a single shard from its primary.
type ShardReceiver struct {
	shardID       int
	primaryNode   *cluster.Node
	conn          net.Conn
	lastSeq       atomic.Uint64
	mu            sync.Mutex
	logger        zerolog.Logger
	cfg           *ShardReceiverConfig
	ctx           context.Context
	cancelFn      context.CancelFunc
	wg            sync.WaitGroup
	running       atomic.Bool
	connected     atomic.Bool

	// Stats
	totalEntriesReceived atomic.Int64
	totalBytesReceived   atomic.Int64
	totalEntriesApplied  atomic.Int64
	totalErrors          atomic.Int64
	lastReceiveTime      atomic.Int64
	connectTime          atomic.Int64
}

// NewShardReceiverManager creates a new per-shard receiver manager.
func NewShardReceiverManager(cfg *ShardReceiverConfig) *ShardReceiverManager {
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = 5 * time.Second
	}
	if cfg.AckInterval <= 0 {
		cfg.AckInterval = 100 * time.Millisecond
	}

	return &ShardReceiverManager{
		cfg:    cfg,
		shards: make(map[int]*ShardReceiver),
		logger: cfg.Logger.With().Str("component", "shard-receiver-manager").Logger(),
	}
}

// Start begins the receiver manager.
func (m *ShardReceiverManager) Start(ctx context.Context) error {
	m.ctx, m.cancelFn = context.WithCancel(ctx)
	m.running.Store(true)

	m.logger.Info().Msg("Shard receiver manager started")
	return nil
}

// Stop gracefully shuts down the receiver manager.
func (m *ShardReceiverManager) Stop() error {
	if !m.running.Load() {
		return nil
	}

	m.running.Store(false)
	m.cancelFn()

	// Stop all receivers
	m.mu.Lock()
	for shardID, receiver := range m.shards {
		receiver.Stop()
		delete(m.shards, shardID)
	}
	m.mu.Unlock()

	m.wg.Wait()
	m.logger.Info().Msg("Shard receiver manager stopped")
	return nil
}

// BecomeReplica is called when this node becomes a replica for a shard.
// It starts receiving replication from the shard's primary.
func (m *ShardReceiverManager) BecomeReplica(shardID int, primary *cluster.Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already receiving for this shard
	if existing, exists := m.shards[shardID]; exists {
		// Check if primary changed
		if existing.primaryNode.ID == primary.ID {
			return nil // Same primary, already connected
		}
		// Different primary - stop old receiver
		existing.Stop()
		delete(m.shards, shardID)
	}

	// Create receiver for this shard
	receiver := newShardReceiver(shardID, primary, m.cfg, m.logger)
	if err := receiver.Start(m.ctx); err != nil {
		return fmt.Errorf("start shard receiver: %w", err)
	}

	m.shards[shardID] = receiver

	m.logger.Info().
		Int("shard_id", shardID).
		Str("primary_id", primary.ID).
		Str("primary_addr", primary.Address).
		Msg("Started receiving replication as replica for shard")

	return nil
}

// StopReplica is called when this node stops being a replica for a shard.
func (m *ShardReceiverManager) StopReplica(shardID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if receiver, exists := m.shards[shardID]; exists {
		receiver.Stop()
		delete(m.shards, shardID)
		m.logger.Info().Int("shard_id", shardID).Msg("Stopped receiving replication for shard")
	}
}

// UpdatePrimary updates the primary node for a shard.
// Called when the shard's primary changes (failover).
func (m *ShardReceiverManager) UpdatePrimary(shardID int, newPrimary *cluster.Node) error {
	return m.BecomeReplica(shardID, newPrimary)
}

// GetShardStats returns statistics for a specific shard's receiver.
func (m *ShardReceiverManager) GetShardStats(shardID int) map[string]interface{} {
	m.mu.RLock()
	receiver, exists := m.shards[shardID]
	m.mu.RUnlock()

	if !exists {
		return nil
	}

	return receiver.Stats()
}

// Stats returns overall receiver statistics.
func (m *ShardReceiverManager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shardStats := make(map[int]map[string]interface{})
	for shardID, receiver := range m.shards {
		shardStats[shardID] = receiver.Stats()
	}

	return map[string]interface{}{
		"running":           m.running.Load(),
		"shards_as_replica": len(m.shards),
		"shard_stats":       shardStats,
	}
}

// newShardReceiver creates a new receiver for a specific shard.
func newShardReceiver(shardID int, primary *cluster.Node, cfg *ShardReceiverConfig, logger zerolog.Logger) *ShardReceiver {
	return &ShardReceiver{
		shardID:     shardID,
		primaryNode: primary,
		cfg:         cfg,
		logger:      logger.With().Int("shard_id", shardID).Str("primary_id", primary.ID).Logger(),
	}
}

// Start begins the shard receiver's connection loop.
func (r *ShardReceiver) Start(ctx context.Context) error {
	r.ctx, r.cancelFn = context.WithCancel(ctx)
	r.running.Store(true)

	r.wg.Add(1)
	go r.connectionLoop()

	r.logger.Debug().Msg("Shard receiver started")
	return nil
}

// Stop gracefully shuts down the shard receiver.
func (r *ShardReceiver) Stop() {
	if !r.running.Load() {
		return
	}

	r.running.Store(false)
	r.cancelFn()

	r.mu.Lock()
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
	r.mu.Unlock()

	r.wg.Wait()
	r.logger.Debug().Msg("Shard receiver stopped")
}

// connectionLoop maintains connection to the primary.
func (r *ShardReceiver) connectionLoop() {
	defer r.wg.Done()

	for r.running.Load() {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		// Try to connect
		if err := r.connect(); err != nil {
			r.logger.Debug().
				Err(err).
				Str("primary_addr", r.primaryNode.Address).
				Dur("retry_in", r.cfg.ReconnectInterval).
				Msg("Failed to connect to primary")

			select {
			case <-r.ctx.Done():
				return
			case <-time.After(r.cfg.ReconnectInterval):
				continue
			}
		}

		// Connected, receive entries
		r.receiveLoop()

		// Disconnected
		r.connected.Store(false)
		r.mu.Lock()
		if r.conn != nil {
			r.conn.Close()
			r.conn = nil
		}
		r.mu.Unlock()

		r.logger.Info().
			Dur("retry_in", r.cfg.ReconnectInterval).
			Msg("Disconnected from primary, will reconnect")

		select {
		case <-r.ctx.Done():
			return
		case <-time.After(r.cfg.ReconnectInterval):
		}
	}
}

// connect establishes connection to the primary.
func (r *ShardReceiver) connect() error {
	addr := r.primaryNode.Address
	if addr == "" {
		return fmt.Errorf("primary has no address")
	}

	r.logger.Debug().Str("primary_addr", addr).Msg("Connecting to primary")

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	// Send sync request (identify as shard receiver)
	syncReq := &replication.ReplicateSync{
		ReaderID:          r.cfg.LocalNode.ID + "-shard-" + fmt.Sprintf("%d", r.shardID),
		LastKnownSequence: r.lastSeq.Load(),
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
	r.mu.Lock()
	r.conn = conn
	r.mu.Unlock()

	r.connected.Store(true)
	r.connectTime.Store(time.Now().UnixNano())

	r.logger.Info().
		Str("primary_addr", addr).
		Uint64("current_seq", syncAck.CurrentSequence).
		Uint64("last_known_seq", r.lastSeq.Load()).
		Bool("can_resume", syncAck.CanResume).
		Msg("Connected to primary for shard replication")

	return nil
}

// receiveLoop receives and processes entries from the primary.
func (r *ShardReceiver) receiveLoop() {
	// Start ack sender
	ackCtx, ackCancel := context.WithCancel(r.ctx)
	defer ackCancel()

	r.wg.Add(1)
	go r.ackLoop(ackCtx)

	for r.running.Load() {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		r.mu.Lock()
		conn := r.conn
		r.mu.Unlock()

		if conn == nil {
			return
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		msgType, payload, err := replication.ReadMessage(conn)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			r.logger.Debug().Err(err).Msg("Connection to primary closed")
			return
		}

		switch msgType {
		case replication.MsgReplicateEntry:
			entry, err := replication.ParseEntry(payload)
			if err != nil {
				r.totalErrors.Add(1)
				r.logger.Error().Err(err).Msg("Failed to parse entry")
				continue
			}

			if err := r.applyEntry(entry); err != nil {
				r.totalErrors.Add(1)
				r.logger.Error().
					Err(err).
					Uint64("sequence", entry.Sequence).
					Msg("Failed to apply entry")
				continue
			}

			// Detect sequence gaps
			prevSeq := r.lastSeq.Load()
			if prevSeq > 0 && entry.Sequence > prevSeq+1 {
				gap := entry.Sequence - prevSeq - 1
				metrics.Get().IncReplicationSequenceGaps(int64(gap))
				r.logger.Warn().
					Uint64("expected", prevSeq+1).
					Uint64("received", entry.Sequence).
					Uint64("gap", gap).
					Msg("Sequence gap detected in replication stream")
			}

			if entry.Sequence > prevSeq {
				r.lastSeq.Store(entry.Sequence)
			}
			r.totalEntriesReceived.Add(1)
			r.totalBytesReceived.Add(int64(len(entry.Payload)))
			r.lastReceiveTime.Store(time.Now().UnixNano())

		case replication.MsgReplicateError:
			errMsg, _ := replication.ParseError(payload)
			r.logger.Error().
				Str("code", errMsg.Code).
				Str("message", errMsg.Message).
				Msg("Error from primary")
			r.totalErrors.Add(1)

		default:
			r.logger.Warn().Uint8("msg_type", msgType).Msg("Unexpected message type")
		}
	}
}

// applyEntry applies a received entry to local storage.
func (r *ShardReceiver) applyEntry(entry *replication.ReplicateEntry) error {
	// Write to local WAL first (if configured)
	if r.cfg.LocalWAL != nil {
		if err := r.cfg.LocalWAL.AppendRaw(entry.Payload); err != nil {
			return fmt.Errorf("write to local WAL: %w", err)
		}
	}

	// Apply to ingest handler (if configured)
	if r.cfg.IngestHandler != nil {
		if err := r.cfg.IngestHandler.ApplyReplicatedEntry(r.ctx, entry.Payload); err != nil {
			return fmt.Errorf("apply to ingest: %w", err)
		}
	}

	r.totalEntriesApplied.Add(1)
	return nil
}

// ackLoop periodically sends acknowledgments to the primary.
func (r *ShardReceiver) ackLoop(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.cfg.AckInterval)
	defer ticker.Stop()

	var lastAckedSeq uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentSeq := r.lastSeq.Load()
			if currentSeq == lastAckedSeq {
				continue // No new entries to ack
			}

			r.mu.Lock()
			conn := r.conn
			r.mu.Unlock()

			if conn == nil {
				return
			}

			ack := &replication.ReplicateAck{
				LastSequence: currentSeq,
				ReaderID:     r.cfg.LocalNode.ID + "-shard-" + fmt.Sprintf("%d", r.shardID),
			}

			if err := replication.WriteAck(conn, ack); err != nil {
				r.logger.Debug().Err(err).Msg("Failed to send ack")
				return
			}

			lastAckedSeq = currentSeq
		}
	}
}

// IsConnected returns whether this receiver is connected to its primary.
func (r *ShardReceiver) IsConnected() bool {
	return r.connected.Load()
}

// LastSequence returns the last received sequence number.
func (r *ShardReceiver) LastSequence() uint64 {
	return r.lastSeq.Load()
}

// Stats returns statistics for this shard receiver.
func (r *ShardReceiver) Stats() map[string]interface{} {
	lastReceive := time.Unix(0, r.lastReceiveTime.Load())
	connectTime := time.Unix(0, r.connectTime.Load())

	return map[string]interface{}{
		"shard_id":                r.shardID,
		"running":                 r.running.Load(),
		"connected":               r.connected.Load(),
		"primary_id":              r.primaryNode.ID,
		"primary_addr":            r.primaryNode.Address,
		"last_sequence":           r.lastSeq.Load(),
		"total_entries_received":  r.totalEntriesReceived.Load(),
		"total_bytes_received":    r.totalBytesReceived.Load(),
		"total_entries_applied":   r.totalEntriesApplied.Load(),
		"total_errors":            r.totalErrors.Load(),
		"last_receive_time":       lastReceive.Format(time.RFC3339),
		"connected_since":         connectTime.Format(time.RFC3339),
		"connection_duration_sec": time.Since(connectTime).Seconds(),
	}
}
