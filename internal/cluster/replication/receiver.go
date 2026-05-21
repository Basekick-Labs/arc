package replication

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
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

// IngestHandlerFunc is an adapter to allow the use of ordinary functions as IngestHandler.
type IngestHandlerFunc func(ctx context.Context, payload []byte) error

func (f IngestHandlerFunc) ApplyReplicatedEntry(ctx context.Context, payload []byte) error {
	return f(ctx, payload)
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

	// TLSConfig for encrypted inter-node communication (nil = plain TCP)
	TLSConfig *tls.Config

	// SharedSecret is the cluster shared secret used to authenticate the
	// replication handshake (MsgReplicateSync) and derive the per-
	// connection session key for stream MAC tags. Required for
	// authenticated replication; without it, the writer will refuse
	// every connection. See GHSA-wfgr-8x84-22q7 / CVE-2026-48106.
	SharedSecret string

	// ClusterName is bound into the handshake HMAC so a leaked MAC from
	// cluster A cannot be replayed against cluster B sharing the same
	// network. Must match the writer's cluster name.
	ClusterName string
}

// Receiver receives WAL entries from the writer node and applies them locally.
// It runs on reader nodes to maintain data consistency with the writer.
type Receiver struct {
	cfg     *ReceiverConfig
	conn    net.Conn
	lastSeq atomic.Uint64 // Last received sequence
	mu      sync.Mutex
	logger  zerolog.Logger

	// sessionKey is the per-connection HKDF-derived key used to verify
	// per-entry MAC tags and periodic checkpoint HMACs. Set inside
	// connect() once the handshake completes; cleared on disconnect.
	// Protected by mu — receiveLoop reads it under the same lock that
	// guards conn so it observes a consistent (conn, sessionKey) pair.
	sessionKey []byte

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
	totalLocalWALDropped atomic.Int64 // Count of entries where the follower's
	// LocalWAL dropped on backpressure but the entry was still applied to
	// the in-memory ingest buffer. Treated as non-fatal — the primary
	// retains the data and follower-side durability is restored on next
	// flush via Parquet replication. See applyEntry for full rationale.
	walDropLastLogNano atomic.Int64 // Sampled-Warn timestamp for WAL drops:
	// sustained follower-side backpressure can produce a dropped entry
	// per record; without sampling that becomes a Warn flood. Operators
	// get the rate from totalLocalWALDropped — the log line just signals
	// "the degraded state is in effect, look at the counter." Mirrors
	// ArrowBuffer.walDropLastLogNano.
	lastReceiveTime atomic.Int64 // Unix nano
	connectTime     atomic.Int64 // Unix nano
}

// walDropLogIntervalNano caps the WAL-drop Warn rate to one line per
// second per receiver. Same semantics as ArrowBuffer's identically
// named const.
const walDropLogIntervalNano = int64(time.Second)

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
	r.sessionKey = nil
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

		// Disconnected. Close the socket and clear the session key
		// so the next connect() opens a fresh conn and derives a
		// fresh key — never reuse a session key across reconnects
		// (each handshake nonce is single-use; reusing the key would
		// defeat replay protection).
		//
		// receiveLoop returns on read error (Connection closed,
		// HMAC fail, malformed entry, etc.) without closing r.conn
		// itself. Without this close, the next connect() would
		// overwrite r.conn and leak the previous socket — flagged
		// by Gemini round 1 on PR #449.
		r.connected.Store(false)
		r.mu.Lock()
		if r.conn != nil {
			r.conn.Close()
			r.conn = nil
		}
		r.sessionKey = nil
		r.mu.Unlock()
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

	// Refuse-when-unconfigured: don't even dial if no shared secret is
	// configured. The writer would reject every handshake anyway (see
	// GHSA-wfgr-8x84-22q7 / coordinator.handleReplicateSync); failing
	// here avoids burning a TCP connection and surfaces the
	// misconfiguration on the reader's logs with the actual cause.
	if r.cfg.SharedSecret == "" {
		return fmt.Errorf("replication requires cluster.shared_secret to be configured")
	}

	// Connect with timeout (TLS if configured)
	conn, err := security.Dial("tcp", r.cfg.WriterAddr, 10*time.Second, r.cfg.TLSConfig)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	// Build the authenticated sync request (GHSA-wfgr-8x84-22q7).
	// Nonce binds this handshake to a single attempt and seeds the
	// per-connection session key via HKDF on both ends.
	nonce, err := security.GenerateNonce()
	if err != nil {
		conn.Close()
		return fmt.Errorf("generate handshake nonce: %w", err)
	}
	lastKnownSeq := r.lastSeq.Load()
	timestamp := time.Now().Unix()
	syncReq := &protocol.ReplicateSync{
		ReaderID:          r.cfg.ReaderID,
		LastKnownSequence: lastKnownSeq,
		Nonce:             nonce,
		ClusterName:       r.cfg.ClusterName,
		Timestamp:         timestamp,
		HMAC: security.ComputeReplicateSyncHMAC(
			r.cfg.SharedSecret, nonce, r.cfg.ReaderID, r.cfg.ClusterName,
			lastKnownSeq, timestamp,
		),
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

	// Handshake accepted — derive the per-connection session key from
	// the same nonce both sides agreed on. Used to verify per-entry
	// MAC tags and periodic checkpoint HMACs.
	sessionKey, err := security.DeriveReplicationSessionKey(r.cfg.SharedSecret, nonce)
	if err != nil {
		conn.Close()
		return fmt.Errorf("derive session key: %w", err)
	}

	// Store connection + session key under the same lock so receiveLoop
	// observes a consistent (conn, sessionKey) pair.
	r.mu.Lock()
	r.conn = conn
	r.sessionKey = sessionKey
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
//
// Stream authentication state (GHSA-wfgr-8x84-22q7) is held locally on
// the goroutine stack so it is reset on every reconnect:
//
//   - sessionKey is snapshotted from r.sessionKey once at the top
//     (set by connect() under r.mu).
//   - cumulativeHash is a fresh sha256.New() per connection. It mirrors
//     the sender's running hash; every entry payload feeds both ends.
//     A checkpoint message proves both sides see the same hash; a
//     mismatch tears down the connection.
func (r *Receiver) receiveLoop() {
	// Start ack sender goroutine
	ackCtx, ackCancel := context.WithCancel(r.ctx)
	defer ackCancel()

	r.wg.Add(1)
	go r.ackLoop(ackCtx)

	r.mu.Lock()
	sessionKey := r.sessionKey
	conn0 := r.conn
	r.mu.Unlock()
	if len(sessionKey) == 0 {
		// connect() guarantees sessionKey is set before returning; this
		// is a defensive check so a future refactor can't silently
		// degrade the stream to unauthenticated.
		r.logger.Error().Msg("Replication receive loop started without session key, refusing to process stream")
		return
	}
	// Snapshot remoteAddr once for the lifetime of this connection so
	// every error log carries enough context for an operator to
	// triage which peer is misbehaving (Gemini round 1 / PR #449).
	// Cheap: RemoteAddr() reads a cached value on the conn.
	var remoteAddr string
	if conn0 != nil && conn0.RemoteAddr() != nil {
		remoteAddr = conn0.RemoteAddr().String()
	}
	llog := r.logger.With().Str("remote_addr", remoteAddr).Logger()
	cumulativeHash := sha256.New()

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
			// Check for timeout (expected during low activity).
			// ReadMessage wraps the underlying net error with
			// fmt.Errorf("...: %w", err), so a plain type
			// assertion would miss it — use errors.As to unwrap.
			// Pre-X1 this incorrectly tripped a full reconnect
			// every 30s on an idle stream.
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			llog.Debug().Err(err).Msg("Connection closed")
			return
		}

		switch msgType {
		case MsgReplicateEntry:
			entry, err := ParseEntry(payload)
			if err != nil {
				r.totalErrors.Add(1)
				llog.Error().Err(err).Msg("Failed to parse entry")
				return
			}

			// Stream-auth: verify the per-entry truncated MAC tag
			// under the session key. A missing or mismatched tag is
			// fatal — drop the connection rather than continue with
			// a poisoned stream. See GHSA-wfgr-8x84-22q7.
			if entry.Tag == "" {
				r.totalErrors.Add(1)
				llog.Error().
					Uint64("sequence", entry.Sequence).
					Msg("Replication entry missing MAC tag; dropping connection")
				return
			}
			// Length-check the hex string before decoding so an
			// attacker who sends an arbitrarily long Tag can't make
			// us allocate a multi-MB byte slice in DecodeString just
			// to throw it away. The expected hex length is
			// 2*ReplicationEntryTagLen since each byte encodes to
			// two hex chars.
			if len(entry.Tag) != security.ReplicationEntryTagLen*2 {
				r.totalErrors.Add(1)
				llog.Error().
					Int("got_len", len(entry.Tag)).
					Uint64("sequence", entry.Sequence).
					Msg("Replication entry tag length mismatch; dropping connection")
				return
			}
			// Stack-allocate the decoded tag — hex.DecodeString
			// would heap-allocate twice per entry (source + dest)
			// at 200k+ entries/sec; hex.Decode into a fixed-size
			// array eliminates the destination allocation, and
			// Go optimises the []byte(entry.Tag) source conversion
			// here. Gemini round 5 / PR #449.
			var tagBytes [security.ReplicationEntryTagLen]byte
			if _, err := hex.Decode(tagBytes[:], []byte(entry.Tag)); err != nil {
				r.totalErrors.Add(1)
				llog.Error().
					Err(err).
					Uint64("sequence", entry.Sequence).
					Msg("Replication entry tag malformed; dropping connection")
				return
			}
			if err := security.ValidateReplicationEntryTag(sessionKey, entry.Sequence, entry.Payload, tagBytes[:]); err != nil {
				r.totalErrors.Add(1)
				llog.Error().
					Err(err).
					Uint64("sequence", entry.Sequence).
					Msg("Replication entry MAC tag verification failed; dropping connection")
				return
			}

			// Sequence-based replay protection: every entry must
			// strictly advance lastSeq. A repeated or backwards
			// sequence indicates either a misbehaving writer or a
			// tampered stream — drop the connection.
			if entry.Sequence <= r.lastSeq.Load() {
				r.totalErrors.Add(1)
				llog.Error().
					Uint64("sequence", entry.Sequence).
					Uint64("last_seq", r.lastSeq.Load()).
					Msg("Replication entry sequence did not advance; dropping connection")
				return
			}

			// Both ends feed the running hash with the verified
			// payload. The next checkpoint must agree.
			cumulativeHash.Write(entry.Payload)

			if err := r.applyEntry(entry); err != nil {
				r.totalErrors.Add(1)
				llog.Error().
					Err(err).
					Uint64("sequence", entry.Sequence).
					Msg("Failed to apply entry")
				continue
			}

			r.lastSeq.Store(entry.Sequence)
			r.totalEntriesReceived.Add(1)
			r.totalBytesReceived.Add(int64(len(entry.Payload)))
			r.lastReceiveTime.Store(time.Now().UnixNano())

		case MsgReplicateCheckpoint:
			cp, err := ParseCheckpoint(payload)
			if err != nil {
				r.totalErrors.Add(1)
				llog.Error().Err(err).Msg("Replication checkpoint parse failed; dropping connection")
				return
			}
			// Cluster name mismatch is a fast fail before HMAC compute.
			if cp.ClusterName != r.cfg.ClusterName {
				r.totalErrors.Add(1)
				llog.Error().
					Str("their_cluster", cp.ClusterName).
					Msg("Replication checkpoint cluster name mismatch; dropping connection")
				return
			}
			// Sequence sanity: the checkpoint follows the Nth entry
			// immediately on a single TCP connection, so its
			// LastSequence must EXACTLY equal what the receiver has
			// observed. Greater = checkpoint claims an unseen entry
			// (impossible on an in-order TCP stream). Less = the
			// receiver advanced past the checkpoint's coverage
			// (protocol desync or out-of-order delivery, also
			// impossible). Both are fatal. Hard-fail on inequality
			// so a desync surfaces in the log with a clear message
			// rather than via the downstream hash mismatch.
			// (Gemini round 4 / PR #449.)
			if cp.LastSequence != r.lastSeq.Load() {
				r.totalErrors.Add(1)
				llog.Error().
					Uint64("checkpoint_seq", cp.LastSequence).
					Uint64("our_last_seq", r.lastSeq.Load()).
					Msg("Replication checkpoint sequence mismatch; dropping connection")
				return
			}
			// Validate the hash matches what we've computed.
			// Length-check the hex string before decoding (same
			// rationale as the per-entry Tag check above).
			if len(cp.CumulativePayloadHashHex) != sha256.Size*2 {
				r.totalErrors.Add(1)
				llog.Error().
					Int("got_len", len(cp.CumulativePayloadHashHex)).
					Msg("Replication checkpoint hash length mismatch; dropping connection")
				return
			}
			// Stack-allocate both hashes — hash.Hash.Sum(nil) and
			// hex.DecodeString each heap-allocate; Sum(buf[:0])
			// and hex.Decode reuse the stack arrays. (Gemini
			// round 5 / PR #449.) The cumulativeHash itself
			// keeps accumulating across checkpoints, so
			// snapshotting via Sum doesn't disturb its state.
			var ourHashBuf [sha256.Size]byte
			ourHash := cumulativeHash.Sum(ourHashBuf[:0])
			var theirHash [sha256.Size]byte
			if _, err := hex.Decode(theirHash[:], []byte(cp.CumulativePayloadHashHex)); err != nil {
				r.totalErrors.Add(1)
				llog.Error().Err(err).Msg("Replication checkpoint hash malformed; dropping connection")
				return
			}
			if subtle.ConstantTimeCompare(ourHash, theirHash[:]) != 1 {
				r.totalErrors.Add(1)
				llog.Error().Msg("Replication checkpoint hash mismatch; dropping connection")
				return
			}
			// Full HMAC validation against the cluster shared
			// secret. Validates the timestamp window and the MAC.
			//
			// Note: no nonce-cache replay check here. A replayed
			// checkpoint carries a hash captured from a past window;
			// our local running hash has since advanced, so the
			// subtle.ConstantTimeCompare(ourHash, theirHash) check
			// just above rejects it first.
			// A NonceCache pass would only matter if an attacker
			// could MITM TLS (already cluster-CA protected) AND
			// reorder the stream such that a replay arrives at the
			// exact moment our hash matches — outside the threat
			// model we're solving here.
			if err := security.ValidateReplicationCheckpointHMAC(
				r.cfg.SharedSecret, cp.Nonce, cp.SenderNodeID, cp.ClusterName,
				theirHash, cp.LastSequence, cp.Timestamp, cp.HMAC, security.HMACTimestampTolerance,
			); err != nil {
				r.totalErrors.Add(1)
				llog.Error().Err(err).Msg("Replication checkpoint HMAC validation failed; dropping connection")
				return
			}
			llog.Debug().
				Uint64("last_seq", cp.LastSequence).
				Msg("Replication checkpoint verified")

		case MsgReplicateError:
			errMsg, _ := ParseError(payload)
			llog.Error().
				Str("code", errMsg.Code).
				Str("message", errMsg.Message).
				Msg("Error from writer")
			r.totalErrors.Add(1)

		default:
			llog.Warn().Uint8("msg_type", msgType).Msg("Unexpected message type")
		}
	}
}

// applyEntry applies a received entry to local storage.
//
// LocalWAL.AppendRaw can return wal.ErrWALDropped when the follower's
// async WAL channel is saturated. We treat that as NON-FATAL and
// continue to apply to the ingest handler:
//
//  1. The primary still has the entry — it didn't fail there.
//  2. The follower's in-memory ingest buffer accepts the data and
//     will eventually flush it to Parquet, which is replicated to
//     peers via Phase 2 file replication.
//  3. The brief gap is "if the follower crashes before its next
//     Parquet flush, this entry is lost from the follower's WAL but
//     survives on the primary." On restart the follower's
//     Phase 3 catch-up reconciles the manifest with peer storage.
//
// Treating the drop as fatal would either stall the receive loop on
// the same entry (no retry mechanism today) or cause silent
// divergence — primary advances, follower lastSeq stays behind, the
// failed entry never reapplies. Both are worse than the brief
// durability gap we accept here.
func (r *Receiver) applyEntry(entry *ReplicateEntry) error {
	// Write to local WAL first (if configured). A drop on backpressure
	// is non-fatal — see function doc.
	if r.cfg.LocalWAL != nil {
		if err := r.cfg.LocalWAL.AppendRaw(entry.Payload); err != nil {
			if errors.Is(err, wal.ErrWALDropped) {
				r.totalLocalWALDropped.Add(1)
				// Sampled Warn — at most one line per walDropLogIntervalNano.
				// Sustained backpressure can drop one entry per replicated
				// record; an unsampled Warn drowns operator dashboards.
				now := time.Now().UnixNano()
				last := r.walDropLastLogNano.Load()
				if now-last >= walDropLogIntervalNano && r.walDropLastLogNano.CompareAndSwap(last, now) {
					r.logger.Warn().
						Uint64("sequence", entry.Sequence).
						Int("payload_size", len(entry.Payload)).
						Int64("total_dropped", r.totalLocalWALDropped.Load()).
						Msg("Replication: follower LocalWAL dropped entry on backpressure; applying to ingest buffer anyway (durability via primary + peer Parquet replication)")
				}
			} else {
				return fmt.Errorf("write to local WAL: %w", err)
			}
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
		"total_local_wal_dropped": r.totalLocalWALDropped.Load(),
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
