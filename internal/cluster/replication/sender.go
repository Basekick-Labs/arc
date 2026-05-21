package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// Sentinel errors for sender refusal paths. Exposed as package-level
// vars so callers (coordinator, tests) can errors.Is() on them.
var (
	// errSharedSecretRequired is returned by AcceptReader when the
	// sender has no shared secret configured. Refuse-when-unconfigured.
	errSharedSecretRequired = errors.New("replication sender refuses connection: cluster.shared_secret not configured")
	// errHandshakeNonceRequired is returned when the handshake nonce is
	// empty. Should never happen if the coordinator validated the
	// MsgReplicateSync before calling AcceptReader; defensive.
	errHandshakeNonceRequired = errors.New("replication sender refuses connection: missing handshake nonce")
	// errReaderUnknown is returned by DoWithReaderWriteLock when the
	// readerID is no longer in s.readers (the reader was removed
	// between AcceptReader and the ack write — e.g. concurrent
	// RemoveReader from a broadcast failure). The caller should
	// treat this as a tear-down signal, same as a write error.
	errReaderUnknown = errors.New("replication sender: reader no longer registered")
)

// defaultCheckpointInterval is how many entries the sender streams
// between full-HMAC checkpoint messages. 1024 keeps the checkpoint
// itself ≤ 0.1% of the message volume at 19.9M records/sec while
// bounding the window an attacker would need to forge truncated tags
// within. Operators can override via SenderConfig.CheckpointInterval.
const defaultCheckpointInterval = 1024

// SenderConfig holds configuration for the replication sender.
type SenderConfig struct {
	// BufferSize is the capacity of the entry buffer (default: 10000)
	BufferSize int

	// WriteTimeout is the timeout for writing to a reader connection
	WriteTimeout time.Duration

	// Logger for sender events
	Logger zerolog.Logger

	// SharedSecret is the cluster shared secret. Required to authenticate
	// the replication stream (GHSA-wfgr-8x84-22q7); when empty the sender
	// refuses every reader at AcceptReader time (refuse-when-unconfigured,
	// matching coordinator.handleReplicateSync).
	SharedSecret string

	// ClusterName is bound into the checkpoint HMAC so a leaked MAC from
	// cluster A cannot be replayed against cluster B.
	ClusterName string

	// LocalNodeID is the sender's node ID, bound into the checkpoint HMAC
	// so receivers can pin checkpoints to a specific peer if needed.
	LocalNodeID string

	// CheckpointInterval is the number of entries between full-HMAC
	// checkpoint messages. Defaults to defaultCheckpointInterval (1024).
	// Operators may shorten it to detect a forged-tag stream sooner at
	// the cost of slightly higher overhead; never shorter than 1.
	CheckpointInterval int
}

// ReaderConnection represents a connected reader receiving replication data.
type ReaderConnection struct {
	id         string
	conn       net.Conn
	lastAck    atomic.Uint64 // Last acknowledged sequence
	writeMu    sync.Mutex    // Serialize writes to connection
	cancelFunc context.CancelFunc
	ctx        context.Context

	// Stream authentication state (GHSA-wfgr-8x84-22q7). All three
	// fields are protected by writeMu — every send path that touches
	// them already holds it. sessionKey is the HKDF-derived per-
	// connection key, cumulativeHash is the running SHA-256 over
	// every payload streamed since the handshake (matches the
	// receiver's running hash), and entriesSinceCheckpoint is the
	// counter that triggers the next checkpoint emission.
	sessionKey             []byte
	cumulativeHash         hash.Hash
	entriesSinceCheckpoint int

	// Stats
	entriesSent  atomic.Int64
	bytesSent    atomic.Int64
	lastSendTime atomic.Int64 // Unix nano
	errors       atomic.Int64
}

// Sender streams WAL entries to connected reader nodes.
// It runs on the writer node and pushes entries as they arrive.
type Sender struct {
	cfg       *SenderConfig
	readers   map[string]*ReaderConnection // reader ID -> connection
	entryChan chan *ReplicateEntry         // Buffered entry queue
	sequence  atomic.Uint64                // Global sequence counter
	mu        sync.RWMutex                 // Protects readers map
	logger    zerolog.Logger

	// Lifecycle
	ctx        context.Context
	cancelFunc context.CancelFunc
	running    atomic.Bool
	wg         sync.WaitGroup

	// Stats
	totalEntriesReceived atomic.Int64
	totalEntriesDropped  atomic.Int64
	totalEntriesSent     atomic.Int64
	totalBytesSent       atomic.Int64
}

// NewSender creates a new replication sender.
func NewSender(cfg *SenderConfig) *Sender {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 10000
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = defaultCheckpointInterval
	}

	return &Sender{
		cfg:       cfg,
		readers:   make(map[string]*ReaderConnection),
		entryChan: make(chan *ReplicateEntry, cfg.BufferSize),
		logger:    cfg.Logger.With().Str("component", "replication-sender").Logger(),
	}
}

// Start begins the sender's background processing.
func (s *Sender) Start(ctx context.Context) error {
	s.ctx, s.cancelFunc = context.WithCancel(ctx)
	s.running.Store(true)

	// Start entry distribution goroutine
	s.wg.Add(1)
	go s.distributionLoop()

	s.logger.Info().
		Int("buffer_size", s.cfg.BufferSize).
		Msg("Replication sender started")

	return nil
}

// Stop gracefully shuts down the sender.
func (s *Sender) Stop() error {
	if !s.running.Load() {
		return nil
	}

	s.running.Store(false)
	s.cancelFunc()

	// Close all reader connections
	s.mu.Lock()
	for id, reader := range s.readers {
		reader.cancelFunc()
		reader.conn.Close()
		delete(s.readers, id)
	}
	s.mu.Unlock()

	// Wait for goroutines to finish
	s.wg.Wait()

	s.logger.Info().Msg("Replication sender stopped")
	return nil
}

// AcceptReader registers a new reader connection for replication.
// Called when a reader connects and sends a sync request. handshakeNonce
// is the nonce the reader sent in MsgReplicateSync — both ends use it to
// derive the same per-connection session key via HKDF
// (GHSA-wfgr-8x84-22q7).
func (s *Sender) AcceptReader(conn net.Conn, readerID, handshakeNonce string, lastKnownSeq uint64) error {
	// Refuse-when-unconfigured: an unauthenticated sender is a footgun
	// (anyone who reaches AcceptReader bypassed the coordinator's auth
	// check, but defense in depth — we won't stream entries without a
	// shared secret to MAC them with). Matches the coordinator-level
	// refusal in handleReplicateSync.
	if s.cfg.SharedSecret == "" {
		conn.Close()
		return errSharedSecretRequired
	}
	if handshakeNonce == "" {
		conn.Close()
		return errHandshakeNonceRequired
	}

	// Derive the per-connection session key from the handshake nonce.
	// The receiver derives the identical key from the same nonce.
	sessionKey, err := security.DeriveReplicationSessionKey(s.cfg.SharedSecret, handshakeNonce)
	if err != nil {
		conn.Close()
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if this reader is already connected
	if existing, ok := s.readers[readerID]; ok {
		existing.cancelFunc()
		existing.conn.Close()
		s.logger.Warn().Str("reader_id", readerID).Msg("Reader reconnected, closing old connection")
	}

	// Create reader context
	readerCtx, cancel := context.WithCancel(s.ctx)

	reader := &ReaderConnection{
		id:             readerID,
		conn:           conn,
		cancelFunc:     cancel,
		ctx:            readerCtx,
		sessionKey:     sessionKey,
		cumulativeHash: sha256.New(),
	}
	reader.lastAck.Store(lastKnownSeq)
	reader.lastSendTime.Store(time.Now().UnixNano())

	s.readers[readerID] = reader

	// Start reader's receive loop (for acks)
	s.wg.Add(1)
	go s.receiveLoop(reader)

	// Note: the success-ack to the reader is sent by the caller
	// (Coordinator.handleReplicateSync) via the cluster-protocol
	// wire format, NOT here. AcceptReader used to write the ack
	// via replication.WriteSyncAck (replication-package wire format,
	// 0x13) but the receiver reads with protocol.ReceiveMessage
	// (cluster-protocol wire format), so the two-byte-type mismatch
	// silently broke handshake completion. The coordinator now
	// owns the wire framing for both error AND success acks.

	s.logger.Info().
		Str("reader_id", readerID).
		Uint64("last_known_seq", lastKnownSeq).
		Uint64("current_seq", s.sequence.Load()).
		Msg("Reader connected for replication")

	return nil
}

// CurrentSequenceAndCanResume returns the writer's current sequence
// and whether the reader's lastKnownSeq is within the buffer window
// for resume. The coordinator calls this after a successful
// AcceptReader to build the protocol-level success ack. Exposed
// because the framing now lives in the coordinator (see
// AcceptReader comment).
func (s *Sender) CurrentSequenceAndCanResume(lastKnownSeq uint64) (uint64, bool) {
	currentSeq := s.sequence.Load()
	canResume := lastKnownSeq == 0 || lastKnownSeq >= currentSeq-uint64(s.cfg.BufferSize)
	return currentSeq, canResume
}

// DoWithReaderWriteLock runs fn while holding the per-reader writeMu —
// the same lock sendToReader takes for entry framing. The coordinator
// uses this to write the post-AcceptReader success ack onto the same
// conn that the distributionLoop's broadcast can ALSO start writing to
// the instant AcceptReader returns (the reader is published to
// s.readers under s.mu before AcceptReader returns, so the broadcast
// path can race the unlocked ack write at the byte level — see the
// final-review finding on PR #449). Holding writeMu for the ack write
// serialises it against any concurrent sendToReader on the same conn.
//
// Returns the error fn returns, or sender.errReaderUnknown if the
// readerID is no longer in the sender's map (concurrent RemoveReader
// or a stale call after disconnect).
func (s *Sender) DoWithReaderWriteLock(readerID string, fn func(net.Conn) error) error {
	s.mu.RLock()
	reader, ok := s.readers[readerID]
	s.mu.RUnlock()
	if !ok {
		return errReaderUnknown
	}
	reader.writeMu.Lock()
	defer reader.writeMu.Unlock()
	return fn(reader.conn)
}

// RemoveReader disconnects a reader from replication.
func (s *Sender) RemoveReader(readerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if reader, ok := s.readers[readerID]; ok {
		reader.cancelFunc()
		reader.conn.Close()
		delete(s.readers, readerID)
		s.logger.Info().Str("reader_id", readerID).Msg("Reader removed from replication")
	}
}

// Replicate queues a WAL entry for replication to all readers.
// This is called by the WAL writer via the replication hook.
// It is non-blocking - entries are dropped if the buffer is full.
func (s *Sender) Replicate(entry *ReplicateEntry) {
	if !s.running.Load() {
		return
	}

	// Assign sequence number
	entry.Sequence = s.sequence.Add(1)
	s.totalEntriesReceived.Add(1)

	// Non-blocking send to entry channel
	select {
	case s.entryChan <- entry:
		// Entry queued successfully
	default:
		// Buffer full, drop entry
		s.totalEntriesDropped.Add(1)
		metrics.Get().IncReplicationEntriesDropped()
		s.logger.Warn().
			Uint64("sequence", entry.Sequence).
			Int("buffer_size", s.cfg.BufferSize).
			Msg("Replication buffer full, entry dropped")
	}
}

// distributionLoop reads entries from the channel and sends to all readers.
func (s *Sender) distributionLoop() {
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

// broadcastEntry sends an entry to all connected readers.
func (s *Sender) broadcastEntry(entry *ReplicateEntry) {
	s.mu.RLock()
	readers := make([]*ReaderConnection, 0, len(s.readers))
	for _, r := range s.readers {
		readers = append(readers, r)
	}
	s.mu.RUnlock()

	// Hash the payload once for the whole broadcast — every reader's
	// per-entry tag uses the same SHA-256 over the payload. Without
	// this we'd pay sha256(payload) per reader; with it we pay once
	// per entry regardless of reader count. (Gemini round 1 / PR #449.)
	payloadHash := sha256.Sum256(entry.Payload)

	for _, reader := range readers {
		if err := s.sendToReader(reader, entry, payloadHash); err != nil {
			reader.errors.Add(1)
			s.logger.Error().
				Err(err).
				Str("reader_id", reader.id).
				Uint64("sequence", entry.Sequence).
				Msg("Failed to send entry to reader")

			// Remove failed reader
			s.RemoveReader(reader.id)
		}
	}
}

// sendToReader sends an entry to a specific reader.
//
// payloadHash is the SHA-256 over entry.Payload, pre-computed once per
// broadcast by broadcastEntry and shared across every reader (the hash
// is independent of the per-connection session key, so reusing it is
// safe). See ComputeReplicationEntryTagFromHash for the rationale.
//
// GHSA-wfgr-8x84-22q7: the entry is stamped with a per-entry truncated
// MAC tag using the per-connection session key, and every payload feeds
// the running SHA-256 used by the periodic checkpoint message. After
// CheckpointInterval entries the sender emits a MsgReplicateCheckpoint
// signed with the full cluster shared secret, anchoring the truncated
// tags against a full-strength HMAC.
func (s *Sender) sendToReader(reader *ReaderConnection, entry *ReplicateEntry, payloadHash [sha256.Size]byte) error {
	reader.writeMu.Lock()
	defer reader.writeMu.Unlock()

	// Check if reader is still connected
	select {
	case <-reader.ctx.Done():
		return context.Canceled
	default:
	}

	// Stamp the per-entry MAC tag under the session key, using the
	// hash broadcastEntry already computed once for the whole fan-out.
	//
	// We mutate Tag on a SHALLOW COPY of *entry (not the shared
	// pointer) so a future parallel broadcastEntry can race-free
	// stamp distinct tags into distinct ReplicateEntry values on
	// different goroutines. The shallow copy is cheap — three
	// uint64-sized fields plus the Payload slice header (16 bytes);
	// the payload bytes themselves are NOT copied (and don't need
	// to be — sender never mutates them).
	//
	// Gemini round 2 on PR #449 flagged the previous shared-pointer
	// mutation as a latent race under parallelization.
	entryCopy := *entry
	tag := security.ComputeReplicationEntryTagFromHash(reader.sessionKey, entryCopy.Sequence, payloadHash)
	entryCopy.Tag = hex.EncodeToString(tag)

	// Feed the running cumulative hash that the checkpoint will sign.
	// We hash the payload (not the tag) so both ends compute the same
	// hash without having to agree on tag encoding.
	reader.cumulativeHash.Write(entryCopy.Payload)
	reader.entriesSinceCheckpoint++

	// Set write deadline
	if err := reader.conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
		return err
	}

	// Write entry
	if err := WriteEntry(reader.conn, &entryCopy); err != nil {
		return err
	}

	// Update stats
	reader.entriesSent.Add(1)
	reader.bytesSent.Add(int64(len(entry.Payload)))
	reader.lastSendTime.Store(time.Now().UnixNano())
	s.totalEntriesSent.Add(1)
	s.totalBytesSent.Add(int64(len(entry.Payload)))

	// Emit checkpoint if interval reached. Same writeMu held, same
	// connection, no separate deadline — the checkpoint piggybacks
	// the entry's WriteTimeout budget. A checkpoint failure tears
	// down the connection just like an entry failure would.
	if reader.entriesSinceCheckpoint >= s.cfg.CheckpointInterval {
		if err := s.emitCheckpointLocked(reader, entry.Sequence); err != nil {
			return err
		}
	}

	return nil
}

// emitCheckpointLocked sends a full-HMAC checkpoint covering every entry
// streamed since the last checkpoint (or since the handshake for the
// first checkpoint). Caller must hold reader.writeMu.
//
// Resets entriesSinceCheckpoint to 0 on successful send but does NOT
// reset cumulativeHash — the receiver verifies against the running
// hash from the start of the connection, not a per-checkpoint window.
// Resetting the hash here would force the receiver to also reset and
// open a 1-entry desync window if the checkpoint is dropped after the
// sender reset but before the receiver acknowledged.
func (s *Sender) emitCheckpointLocked(reader *ReaderConnection, lastSeq uint64) error {
	if s.cfg.SharedSecret == "" {
		// Refuse-when-unconfigured should have caught this at
		// AcceptReader, but defensive — never sign with an empty key.
		return errSharedSecretRequired
	}
	nonce, err := security.GenerateNonce()
	if err != nil {
		return err
	}
	// Snapshot the running hash. sha256's Sum(nil) returns a new slice
	// without mutating the internal state, so the cumulative hash keeps
	// accumulating across checkpoints.
	var hashSnap [32]byte
	copy(hashSnap[:], reader.cumulativeHash.Sum(nil))
	timestamp := time.Now().Unix()
	cp := &ReplicateCheckpoint{
		CumulativePayloadHashHex: hex.EncodeToString(hashSnap[:]),
		LastSequence:             lastSeq,
		Nonce:                    nonce,
		SenderNodeID:             s.cfg.LocalNodeID,
		ClusterName:              s.cfg.ClusterName,
		Timestamp:                timestamp,
		HMAC: security.ComputeReplicationCheckpointHMAC(
			s.cfg.SharedSecret, nonce, s.cfg.LocalNodeID, s.cfg.ClusterName,
			hashSnap, lastSeq, timestamp,
		),
	}
	if err := reader.conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
		return err
	}
	if err := WriteCheckpoint(reader.conn, cp); err != nil {
		return err
	}
	reader.entriesSinceCheckpoint = 0
	return nil
}

// receiveLoop handles incoming messages from a reader (mainly acks).
func (s *Sender) receiveLoop(reader *ReaderConnection) {
	defer s.wg.Done()
	defer s.RemoveReader(reader.id)

	for {
		select {
		case <-reader.ctx.Done():
			return
		default:
		}

		// Read message with timeout
		reader.conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		msgType, payload, err := ReadMessage(reader.conn)
		if err != nil {
			// Check if it's just a timeout (expected during low
			// activity). ReadMessage wraps the underlying net
			// error with fmt.Errorf("...: %w", err), so a plain
			// type assertion misses it — use errors.As to unwrap.
			// Pre-X1 this dropped every reader connection every
			// 30s on an idle ack channel.
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			s.logger.Debug().
				Err(err).
				Str("reader_id", reader.id).
				Msg("Reader connection closed")
			return
		}

		switch msgType {
		case MsgReplicateAck:
			ack, err := ParseAck(payload)
			if err != nil {
				s.logger.Error().Err(err).Str("reader_id", reader.id).Msg("Failed to parse ack")
				continue
			}
			reader.lastAck.Store(ack.LastSequence)
			s.logger.Debug().
				Str("reader_id", reader.id).
				Uint64("last_seq", ack.LastSequence).
				Msg("Received ack from reader")

		default:
			s.logger.Warn().
				Str("reader_id", reader.id).
				Uint8("msg_type", msgType).
				Msg("Unexpected message type from reader")
		}
	}
}

// ReaderCount returns the number of connected readers.
func (s *Sender) ReaderCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.readers)
}

// CurrentSequence returns the current sequence number.
func (s *Sender) CurrentSequence() uint64 {
	return s.sequence.Load()
}

// Stats returns sender statistics.
func (s *Sender) Stats() map[string]interface{} {
	s.mu.RLock()
	readerStats := make([]map[string]interface{}, 0, len(s.readers))
	for id, reader := range s.readers {
		lastSend := time.Unix(0, reader.lastSendTime.Load())
		readerStats = append(readerStats, map[string]interface{}{
			"reader_id":      id,
			"last_ack_seq":   reader.lastAck.Load(),
			"entries_sent":   reader.entriesSent.Load(),
			"bytes_sent":     reader.bytesSent.Load(),
			"last_send_time": lastSend.Format(time.RFC3339),
			"errors":         reader.errors.Load(),
			"lag":            s.sequence.Load() - reader.lastAck.Load(),
		})
	}
	s.mu.RUnlock()

	return map[string]interface{}{
		"running":                s.running.Load(),
		"current_sequence":       s.sequence.Load(),
		"buffer_size":            s.cfg.BufferSize,
		"buffer_used":            len(s.entryChan),
		"reader_count":           len(readerStats),
		"readers":                readerStats,
		"total_entries_received": s.totalEntriesReceived.Load(),
		"total_entries_dropped":  s.totalEntriesDropped.Load(),
		"total_entries_sent":     s.totalEntriesSent.Load(),
		"total_bytes_sent":       s.totalBytesSent.Load(),
	}
}
