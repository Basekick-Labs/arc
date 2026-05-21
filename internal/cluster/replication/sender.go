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
	// MsgReplicateSync before calling PrepareReader; defensive.
	errHandshakeNonceRequired = errors.New("replication sender refuses connection: missing handshake nonce")
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

	// Stream authentication state (GHSA-wfgr-8x84-22q7). All four
	// fields are protected by writeMu — every send path that touches
	// them already holds it. sessionKey is the HKDF-derived per-
	// connection key, entryMAC is a long-lived hmac.Hash keyed by
	// sessionKey that sendToReader reuses (via Reset) for every entry
	// — at 200k+ entries/sec this saves the per-entry hmac.New
	// allocation. cumulativeHash is the running SHA-256 over every
	// payload streamed since the handshake (matches the receiver's
	// running hash). entriesSinceCheckpoint is the counter that
	// triggers the next checkpoint emission.
	sessionKey             []byte
	entryMAC               hash.Hash
	cumulativeHash         hash.Hash
	entriesSinceCheckpoint int

	// Stats
	entriesSent  atomic.Int64
	bytesSent    atomic.Int64
	lastSendTime atomic.Int64 // Unix nano
	errors       atomic.Int64
}

// Discard tears down a prepared-but-not-yet-activated reader. The
// coordinator calls this when the handshake ack write fails — the
// reader was built by PrepareReader but never published to the
// broadcast map or had its receiveLoop started, so all we need to do
// is cancel its context (in case anything stashed it) and close the
// underlying conn. Safe to call exactly once on a reader that has
// NOT been passed to ActivateReader; calling it on an active reader
// would leak the entry from s.readers — use RemoveReader for that.
func (r *ReaderConnection) Discard() {
	r.cancelFunc()
	if r.conn != nil {
		r.conn.Close()
	}
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

// PrepareReader validates and builds a ReaderConnection but does NOT
// add it to the sender's active map or start its receive loop. The
// caller writes the handshake success ack on the returned conn, then
// calls ActivateReader to publish it.
//
// Two-phase accept (Prepare → coordinator writes ack → Activate) is
// the structural fix for two races on PR #449:
//   - Write race: pre-fix, AcceptReader published the reader to
//     s.readers before returning, so distributionLoop could start
//     writing entry frames to the same conn concurrently with the
//     coordinator's ack write — byte-level interleave on net.Conn.
//   - Delivery-order race: even with writeMu serialisation, an entry
//     frame could land BEFORE the ack frame (the broadcast path
//     happened to acquire writeMu first), violating the protocol
//     contract that the receiver reads MsgReplicateSyncAck as the
//     first message. Receiver would log "unexpected message type"
//     and tear down the handshake. Gemini round 4 finding.
//
// PrepareReader is safe to call without external locking; it does its
// own validation and returns a fully-constructed ReaderConnection
// that is invisible to broadcastEntry until ActivateReader runs.
//
// On error the conn is closed by this method, so the caller never
// needs to clean it up.
func (s *Sender) PrepareReader(conn net.Conn, readerID, handshakeNonce string, lastKnownSeq uint64) (*ReaderConnection, error) {
	// Refuse-when-unconfigured: an unauthenticated sender is a footgun
	// (anyone who reaches PrepareReader bypassed the coordinator's
	// auth check, but defense in depth — we won't stream entries
	// without a shared secret to MAC them with).
	if s.cfg.SharedSecret == "" {
		conn.Close()
		return nil, errSharedSecretRequired
	}
	if handshakeNonce == "" {
		conn.Close()
		return nil, errHandshakeNonceRequired
	}

	// Derive the per-connection session key from the handshake nonce.
	// The receiver derives the identical key from the same nonce.
	sessionKey, err := security.DeriveReplicationSessionKey(s.cfg.SharedSecret, handshakeNonce)
	if err != nil {
		conn.Close()
		return nil, err
	}

	readerCtx, cancel := context.WithCancel(s.ctx)
	reader := &ReaderConnection{
		id:             readerID,
		conn:           conn,
		cancelFunc:     cancel,
		ctx:            readerCtx,
		sessionKey:     sessionKey,
		entryMAC:       security.NewReplicationEntryHMAC(sessionKey),
		cumulativeHash: sha256.New(),
	}
	reader.lastAck.Store(lastKnownSeq)
	reader.lastSendTime.Store(time.Now().UnixNano())
	return reader, nil
}

// ActivateReader publishes a prepared reader to the sender's active
// map and starts its receive loop. Called by the coordinator AFTER
// the handshake success ack has been written to reader.conn — so the
// first message the receiver reads is always MsgReplicateSyncAck,
// not a racing MsgReplicateEntry from the broadcast path. See
// PrepareReader for the full ordering invariant.
//
// If a reader with the same ID is already active, its old connection
// is closed and replaced with this one (matches the pre-two-phase
// reconnect-replace behaviour).
func (s *Sender) ActivateReader(reader *ReaderConnection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.readers[reader.id]; ok {
		existing.cancelFunc()
		existing.conn.Close()
		s.logger.Warn().Str("reader_id", reader.id).Msg("Reader reconnected, closing old connection")
	}
	s.readers[reader.id] = reader

	s.wg.Add(1)
	go s.receiveLoop(reader)

	s.logger.Info().
		Str("reader_id", reader.id).
		Uint64("last_known_seq", reader.lastAck.Load()).
		Uint64("current_seq", s.sequence.Load()).
		Msg("Reader connected for replication")
}

// AcceptReader is the legacy single-call shape: PrepareReader +
// (caller writes ack) + ActivateReader. Kept for test code that
// doesn't care about the handshake ack ordering. Production callers
// (coordinator.AcceptReplicationConnection) MUST use the two-phase
// API to avoid the delivery-order race documented on PrepareReader.
func (s *Sender) AcceptReader(conn net.Conn, readerID, handshakeNonce string, lastKnownSeq uint64) error {
	reader, err := s.PrepareReader(conn, readerID, handshakeNonce, lastKnownSeq)
	if err != nil {
		return err
	}
	s.ActivateReader(reader)
	return nil
}

// CurrentSequenceAndCanResume returns the writer's current sequence
// and whether the reader's lastKnownSeq is within the buffer window
// for resume. The coordinator calls this after PrepareReader to build
// the protocol-level success ack. Exposed because the framing lives
// in the coordinator (see PrepareReader doc).
func (s *Sender) CurrentSequenceAndCanResume(lastKnownSeq uint64) (uint64, bool) {
	currentSeq := s.sequence.Load()
	canResume := lastKnownSeq == 0 || lastKnownSeq >= currentSeq-uint64(s.cfg.BufferSize)
	return currentSeq, canResume
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
	//
	// We reuse reader.entryMAC (allocated once at PrepareReader
	// time) instead of building a fresh hmac.Hash per entry —
	// hmac.New is one of the dominant allocations on the hot path
	// at 200k+ entries/sec. The reuse is safe because writeMu
	// serialises every sendToReader call for the same reader.
	// (Gemini round 6 / PR #449.)
	entryCopy := *entry
	var tag [security.ReplicationEntryTagLen]byte
	security.ComputeReplicationEntryTagWithMAC(reader.entryMAC, entryCopy.Sequence, payloadHash, tag[:])
	entryCopy.Tag = hex.EncodeToString(tag[:])

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
	// Snapshot the running hash into a stack-allocated array.
	// Sum(buf[:0]) appends to the buffer without allocating when the
	// capacity is sufficient — vs Sum(nil) + copy which would heap-
	// allocate the intermediate slice. sha256's Sum doesn't mutate
	// internal state, so the cumulative hash keeps accumulating
	// across checkpoints. (Gemini round 5 / PR #449.)
	var hashSnap [sha256.Size]byte
	reader.cumulativeHash.Sum(hashSnap[:0])
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
