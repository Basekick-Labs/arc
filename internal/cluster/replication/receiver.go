package replication

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

// WALWriter interface for writing to local WAL
type WALWriter interface {
	AppendRaw(payload []byte) error
}

// IngestHandler interface for applying entries to the ingest buffer
type IngestHandler interface {
	// ApplyReplicatedEntry applies a replicated entry to the local buffer
	// The entry contains msgpack-encoded records that need to be ingested
	ApplyReplicatedEntry(ctx context.Context, payload []byte) error
}

// ReceiverConfig holds configuration for the replication receiver.
type ReceiverConfig struct {
	// ReaderID is this reader's unique identifier
	ReaderID string

	// WriterAddr is the writer's coordinator address to connect to
	WriterAddr string

	// LocalWAL is the local WAL to write received entries (optional, for durability)
	LocalWAL WALWriter

	// IngestHandler applies entries to local buffer (optional)
	IngestHandler IngestHandler

	// ReconnectInterval is how long to wait before reconnecting after disconnect
	ReconnectInterval time.Duration

	// AckInterval is how often to send acknowledgments to the writer
	AckInterval time.Duration

	// Logger for receiver events
	Logger zerolog.Logger
}

// Receiver receives WAL entries from the writer node and applies them locally.
// It runs on reader nodes to maintain data consistency with the writer.
type Receiver struct {
	cfg        *ReceiverConfig
	conn       net.Conn
	lastSeq    atomic.Uint64 // Last received sequence
	mu         sync.Mutex
	logger     zerolog.Logger

	// Lifecycle
	ctx        context.Context
	cancelFunc context.CancelFunc
	running    atomic.Bool
	wg         sync.WaitGroup
	connected  atomic.Bool

	// Stats
	totalEntriesReceived atomic.Int64
	totalBytesReceived   atomic.Int64
	totalEntriesApplied  atomic.Int64
	totalErrors          atomic.Int64
	lastReceiveTime      atomic.Int64 // Unix nano
	connectTime          atomic.Int64 // Unix nano
}

// NewReceiver creates a new replication receiver.
func NewReceiver(cfg *ReceiverConfig) *Receiver {
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = 5 * time.Second
	}
	if cfg.AckInterval <= 0 {
		cfg.AckInterval = 100 * time.Millisecond
	}

	return &Receiver{
		cfg:    cfg,
		logger: cfg.Logger.With().Str("component", "replication-receiver").Logger(),
	}
}

// Start begins the receiver's connection and processing loop.
func (r *Receiver) Start(ctx context.Context) error {
	r.ctx, r.cancelFunc = context.WithCancel(ctx)
	r.running.Store(true)

	// Start connection loop
	r.wg.Add(1)
	go r.connectionLoop()

	r.logger.Info().
		Str("writer_addr", r.cfg.WriterAddr).
		Str("reader_id", r.cfg.ReaderID).
		Msg("Replication receiver started")

	return nil
}

// Stop gracefully shuts down the receiver.
func (r *Receiver) Stop() error {
	if !r.running.Load() {
		return nil
	}

	r.running.Store(false)
	r.cancelFunc()

	// Close connection if any
	r.mu.Lock()
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
	r.mu.Unlock()

	// Wait for goroutines
	r.wg.Wait()

	r.logger.Info().Msg("Replication receiver stopped")
	return nil
}

// connectionLoop manages the connection to the writer, with automatic reconnection.
func (r *Receiver) connectionLoop() {
	defer r.wg.Done()

	for r.running.Load() {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		// Try to connect
		if err := r.connect(); err != nil {
			r.logger.Warn().
				Err(err).
				Str("writer_addr", r.cfg.WriterAddr).
				Dur("retry_in", r.cfg.ReconnectInterval).
				Msg("Failed to connect to writer, will retry")

			// Wait before retrying
			select {
			case <-r.ctx.Done():
				return
			case <-time.After(r.cfg.ReconnectInterval):
				continue
			}
		}

		// Connected, start receiving
		r.receiveLoop()

		// Disconnected, wait before reconnecting
		r.connected.Store(false)
		r.logger.Info().
			Dur("retry_in", r.cfg.ReconnectInterval).
			Msg("Disconnected from writer, will reconnect")

		select {
		case <-r.ctx.Done():
			return
		case <-time.After(r.cfg.ReconnectInterval):
		}
	}
}

// connect establishes connection to the writer and performs handshake.
func (r *Receiver) connect() error {
	r.logger.Debug().Str("writer_addr", r.cfg.WriterAddr).Msg("Connecting to writer")

	// Connect with timeout
	conn, err := net.DialTimeout("tcp", r.cfg.WriterAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	// Send sync request using cluster protocol (same format as peer messages)
	syncReq := &protocol.ReplicateSync{
		ReaderID:          r.cfg.ReaderID,
		LastKnownSequence: r.lastSeq.Load(),
	}

	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgReplicateSync,
		Payload: syncReq,
	}, 10*time.Second); err != nil {
		conn.Close()
		return fmt.Errorf("send sync: %w", err)
	}

	// Read sync response using cluster protocol
	msg, err := protocol.ReceiveMessage(conn, 10*time.Second)
	if err != nil {
		conn.Close()
		return fmt.Errorf("read sync response: %w", err)
	}

	if msg.Type != protocol.MsgReplicateSyncAck {
		conn.Close()
		return fmt.Errorf("unexpected message type: %s", msg.Type.String())
	}

	syncAck, ok := msg.Payload.(*protocol.ReplicateSyncAck)
	if !ok {
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
		Str("writer_addr", r.cfg.WriterAddr).
		Uint64("current_seq", syncAck.CurrentSequence).
		Uint64("last_known_seq", r.lastSeq.Load()).
		Bool("can_resume", syncAck.CanResume).
		Msg("Connected to writer for replication")

	return nil
}

// receiveLoop receives and processes entries from the writer.
func (r *Receiver) receiveLoop() {
	// Start ack sender goroutine
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

		// Read with timeout
		r.mu.Lock()
		conn := r.conn
		r.mu.Unlock()

		if conn == nil {
			return
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		msgType, payload, err := ReadMessage(conn)
		if err != nil {
			// Check for timeout (expected during low activity)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			r.logger.Debug().Err(err).Msg("Connection closed")
			return
		}

		switch msgType {
		case MsgReplicateEntry:
			entry, err := ParseEntry(payload)
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

			r.lastSeq.Store(entry.Sequence)
			r.totalEntriesReceived.Add(1)
			r.totalBytesReceived.Add(int64(len(entry.Payload)))
			r.lastReceiveTime.Store(time.Now().UnixNano())

		case MsgReplicateError:
			errMsg, _ := ParseError(payload)
			r.logger.Error().
				Str("code", errMsg.Code).
				Str("message", errMsg.Message).
				Msg("Error from writer")
			r.totalErrors.Add(1)

		default:
			r.logger.Warn().Uint8("msg_type", msgType).Msg("Unexpected message type")
		}
	}
}

// applyEntry applies a received entry to local storage.
func (r *Receiver) applyEntry(entry *ReplicateEntry) error {
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

// ackLoop periodically sends acknowledgments to the writer.
func (r *Receiver) ackLoop(ctx context.Context) {
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

			ack := &ReplicateAck{
				LastSequence: currentSeq,
				ReaderID:     r.cfg.ReaderID,
			}

			if err := WriteAck(conn, ack); err != nil {
				r.logger.Debug().Err(err).Msg("Failed to send ack")
				return
			}

			lastAckedSeq = currentSeq
		}
	}
}

// IsConnected returns whether the receiver is connected to the writer.
func (r *Receiver) IsConnected() bool {
	return r.connected.Load()
}

// LastSequence returns the last received sequence number.
func (r *Receiver) LastSequence() uint64 {
	return r.lastSeq.Load()
}

// Stats returns receiver statistics.
func (r *Receiver) Stats() map[string]interface{} {
	lastReceive := time.Unix(0, r.lastReceiveTime.Load())
	connectTime := time.Unix(0, r.connectTime.Load())

	return map[string]interface{}{
		"running":                 r.running.Load(),
		"connected":               r.connected.Load(),
		"writer_addr":             r.cfg.WriterAddr,
		"reader_id":               r.cfg.ReaderID,
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

// SimpleIngestHandler is a basic implementation of IngestHandler that decodes
// msgpack records and calls a provided function.
type SimpleIngestHandler struct {
	HandleFunc func(ctx context.Context, records []map[string]interface{}) error
	Logger     zerolog.Logger
}

// ApplyReplicatedEntry decodes the msgpack payload and calls the handler function.
func (h *SimpleIngestHandler) ApplyReplicatedEntry(ctx context.Context, payload []byte) error {
	var records []map[string]interface{}
	if err := msgpack.Unmarshal(payload, &records); err != nil {
		return fmt.Errorf("unmarshal records: %w", err)
	}

	if len(records) == 0 {
		return nil
	}

	return h.HandleFunc(ctx, records)
}
