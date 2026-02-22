package replication

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// SenderConfig holds configuration for the replication sender.
type SenderConfig struct {
	// BufferSize is the capacity of the entry buffer (default: 10000)
	BufferSize int

	// WriteTimeout is the timeout for writing to a reader connection
	WriteTimeout time.Duration

	// Logger for sender events
	Logger zerolog.Logger
}

// ReaderConnection represents a connected reader receiving replication data.
type ReaderConnection struct {
	id         string
	conn       net.Conn
	lastAck    atomic.Uint64  // Last acknowledged sequence
	writeMu    sync.Mutex     // Serialize writes to connection
	cancelFunc context.CancelFunc
	ctx        context.Context

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
// Called when a reader connects and sends a sync request.
func (s *Sender) AcceptReader(conn net.Conn, readerID string, lastKnownSeq uint64) error {
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
		id:         readerID,
		conn:       conn,
		cancelFunc: cancel,
		ctx:        readerCtx,
	}
	reader.lastAck.Store(lastKnownSeq)
	reader.lastSendTime.Store(time.Now().UnixNano())

	s.readers[readerID] = reader

	// Start reader's receive loop (for acks)
	s.wg.Add(1)
	go s.receiveLoop(reader)

	// Send sync ack to reader
	currentSeq := s.sequence.Load()
	canResume := lastKnownSeq == 0 || lastKnownSeq >= currentSeq-uint64(s.cfg.BufferSize)

	syncAck := &ReplicateSyncAck{
		CurrentSequence: currentSeq,
		CanResume:       canResume,
	}

	if err := WriteSyncAck(conn, syncAck); err != nil {
		s.logger.Error().Err(err).Str("reader_id", readerID).Msg("Failed to send sync ack")
		reader.cancelFunc()
		conn.Close()
		delete(s.readers, readerID)
		return err
	}

	s.logger.Info().
		Str("reader_id", readerID).
		Uint64("last_known_seq", lastKnownSeq).
		Uint64("current_seq", currentSeq).
		Bool("can_resume", canResume).
		Msg("Reader connected for replication")

	return nil
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

	for _, reader := range readers {
		if err := s.sendToReader(reader, entry); err != nil {
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
func (s *Sender) sendToReader(reader *ReaderConnection, entry *ReplicateEntry) error {
	reader.writeMu.Lock()
	defer reader.writeMu.Unlock()

	// Check if reader is still connected
	select {
	case <-reader.ctx.Done():
		return context.Canceled
	default:
	}

	// Set write deadline
	if err := reader.conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
		return err
	}

	// Write entry
	if err := WriteEntry(reader.conn, entry); err != nil {
		return err
	}

	// Update stats
	reader.entriesSent.Add(1)
	reader.bytesSent.Add(int64(len(entry.Payload)))
	reader.lastSendTime.Store(time.Now().UnixNano())
	s.totalEntriesSent.Add(1)
	s.totalBytesSent.Add(int64(len(entry.Payload)))

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
			// Check if it's just a timeout (expected)
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
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
