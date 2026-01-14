package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

// WAL file format constants
var (
	WALMagic   = []byte{'A', 'R', 'C', 'W'} // Magic bytes
	WALVersion = uint16(0x0001)             // Version 1
)

const (
	WALChecksumCRC32 = 0x01 // CRC32 checksum type

	// Entry format: [Length: 4 bytes] [Timestamp: 8 bytes] [Checksum: 4 bytes] [Payload: N bytes]
	WALEntryHeaderSize = 16
	WALFileHeaderSize  = 7 // Magic(4) + Version(2) + ChecksumType(1)

	// MaxWALPayloadSize is the maximum allowed payload size for a single WAL entry.
	// This limit prevents integer overflow during buffer allocation (CWE-190) and
	// aligns with the replication protocol limit (100MB).
	MaxWALPayloadSize = 100 * 1024 * 1024 // 100MB
)

// SyncMode defines how WAL syncs to disk
type SyncMode string

const (
	SyncModeFsync     SyncMode = "fsync"     // Full sync: data + metadata (safest)
	SyncModeFdatasync SyncMode = "fdatasync" // Data sync only (balanced, default)
	SyncModeAsync     SyncMode = "async"     // No explicit sync (fastest, least safe)
)

// ErrPayloadTooLarge indicates the payload exceeds MaxWALPayloadSize.
var ErrPayloadTooLarge = errors.New("WAL payload exceeds maximum allowed size")

// walEntry is a pre-serialized WAL entry ready for writing
type walEntry struct {
	data []byte // Complete entry: header + payload
}

// WriterConfig holds configuration for WAL writer
type WriterConfig struct {
	WALDir        string        // Directory for WAL files
	SyncMode      SyncMode      // Sync mode: fsync, fdatasync, async
	MaxSizeBytes  int64         // Rotate WAL when it reaches this size (default: 100MB)
	MaxAge        time.Duration // Rotate WAL after this duration (default: 1 hour)
	SyncInterval  time.Duration // Sync at most this often (default: 100ms, 0 = sync every write)
	SyncBytes     int64         // Sync after this many bytes written (default: 1MB, 0 = no byte threshold)
	BufferSize    int           // Size of async write buffer (default: 10000)
	Logger        zerolog.Logger
}

// ReplicationEntry represents a WAL entry for replication.
// This is passed to the replication hook for streaming to readers.
type ReplicationEntry struct {
	// Sequence is a monotonically increasing number for ordering
	Sequence uint64

	// TimestampUS is the entry timestamp in microseconds since epoch
	TimestampUS uint64

	// Payload is the raw msgpack data
	Payload []byte
}

// ReplicationHook is called for each WAL entry before it's written locally.
// This enables real-time streaming of entries to reader nodes.
type ReplicationHook func(entry *ReplicationEntry)

// Writer is a Write-Ahead Log writer with configurable durability
type Writer struct {
	config WriterConfig
	logger zerolog.Logger

	// Current WAL file
	currentFile *os.File
	currentPath string
	currentSize int64
	startTime   time.Time

	// Batched sync tracking
	lastSyncTime   time.Time // Last time we synced
	bytesSinceSync int64     // Bytes written since last sync

	// Async write buffer
	entryChan chan walEntry
	done      chan struct{}
	wg        sync.WaitGroup

	// Replication hook for streaming entries to readers
	replicationHook ReplicationHook
	sequence        uint64 // Monotonic sequence counter for replication

	// Metrics (atomic for lock-free reads)
	TotalEntries   int64
	TotalBytes     int64
	TotalSyncs     int64
	TotalRotations int64
	DroppedEntries int64 // Entries dropped due to full buffer

	mu sync.Mutex
}

// NewWriter creates a new WAL writer
func NewWriter(cfg *WriterConfig) (*Writer, error) {
	// Set defaults
	if cfg.SyncMode == "" {
		cfg.SyncMode = SyncModeFdatasync
	}
	if cfg.MaxSizeBytes == 0 {
		cfg.MaxSizeBytes = 100 * 1024 * 1024 // 100MB
	}
	if cfg.MaxAge == 0 {
		cfg.MaxAge = time.Hour
	}
	// Default batched sync: every 100ms OR every 1MB, whichever comes first
	// This significantly reduces fsync overhead while maintaining reasonable durability
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 100 * time.Millisecond
	}
	if cfg.SyncBytes == 0 {
		cfg.SyncBytes = 1024 * 1024 // 1MB
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 10000 // Default buffer size
	}

	// Create WAL directory with owner-only permissions (WAL contains sensitive data)
	if err := os.MkdirAll(cfg.WALDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	w := &Writer{
		config:       *cfg,
		logger:       cfg.Logger.With().Str("component", "wal-writer").Logger(),
		lastSyncTime: time.Now(),
		entryChan:    make(chan walEntry, cfg.BufferSize),
		done:         make(chan struct{}),
	}

	// Initialize first WAL file
	if err := w.rotate(); err != nil {
		return nil, fmt.Errorf("failed to create initial WAL file: %w", err)
	}

	// Start async writer goroutine
	w.wg.Add(1)
	go w.writerLoop()

	w.logger.Info().
		Str("dir", cfg.WALDir).
		Str("sync_mode", string(cfg.SyncMode)).
		Int64("max_size_mb", cfg.MaxSizeBytes/1024/1024).
		Dur("max_age", cfg.MaxAge).
		Dur("sync_interval", cfg.SyncInterval).
		Int64("sync_bytes", cfg.SyncBytes).
		Int("buffer_size", cfg.BufferSize).
		Msg("WAL writer initialized (async mode)")

	return w, nil
}

// writerLoop is the background goroutine that writes entries to disk
func (w *Writer) writerLoop() {
	defer w.wg.Done()

	syncTicker := time.NewTicker(w.config.SyncInterval)
	defer syncTicker.Stop()

	for {
		select {
		case entry := <-w.entryChan:
			w.writeEntry(entry)

		case <-syncTicker.C:
			// Periodic sync
			w.mu.Lock()
			if w.bytesSinceSync > 0 {
				w.sync()
				w.lastSyncTime = time.Now()
				w.bytesSinceSync = 0
				w.TotalSyncs++
			}
			w.mu.Unlock()

		case <-w.done:
			// Drain remaining entries before shutdown
			for {
				select {
				case entry := <-w.entryChan:
					w.writeEntry(entry)
				default:
					// No more entries, final sync and exit
					w.mu.Lock()
					if w.bytesSinceSync > 0 {
						w.sync()
						w.TotalSyncs++
					}
					w.mu.Unlock()
					return
				}
			}
		}
	}
}

// writeEntry writes a single entry to the WAL file (called from writerLoop)
func (w *Writer) writeEntry(entry walEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write entry
	n, err := w.currentFile.Write(entry.data)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to write WAL entry")
		return
	}

	bytesWritten := int64(n)
	w.currentSize += bytesWritten
	w.bytesSinceSync += bytesWritten

	// Update metrics
	w.TotalEntries++
	w.TotalBytes += bytesWritten

	// Sync if byte threshold exceeded
	if w.bytesSinceSync >= w.config.SyncBytes {
		w.sync()
		w.lastSyncTime = time.Now()
		w.bytesSinceSync = 0
		w.TotalSyncs++
	}

	// Check if rotation needed
	age := time.Since(w.startTime)
	if w.currentSize >= w.config.MaxSizeBytes || age >= w.config.MaxAge {
		if err := w.rotate(); err != nil {
			w.logger.Error().Err(err).Msg("Failed to rotate WAL")
		}
	}
}

// rotate creates a new WAL file
func (w *Writer) rotate() error {
	// Close current file (sync any pending data first)
	if w.currentFile != nil {
		if w.bytesSinceSync > 0 {
			w.sync()
			w.TotalSyncs++
		}
		w.currentFile.Close()
	}

	// Generate new filename
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("arc-%s.wal", timestamp)
	w.currentPath = filepath.Join(w.config.WALDir, filename)

	// Open new file with owner-only permissions (WAL contains sensitive data)
	var err error
	w.currentFile, err = os.OpenFile(w.currentPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}

	w.currentSize = 0
	w.startTime = time.Now()
	w.lastSyncTime = time.Now()
	w.bytesSinceSync = 0
	w.TotalRotations++

	// Write WAL header
	header := make([]byte, WALFileHeaderSize)
	copy(header[0:4], WALMagic)
	binary.BigEndian.PutUint16(header[4:6], WALVersion)
	header[6] = WALChecksumCRC32

	n, err := w.currentFile.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}
	w.currentSize += int64(n)

	w.logger.Info().Str("file", filename).Msg("WAL rotated")
	return nil
}

// Append writes records to the WAL asynchronously (non-blocking)
func (w *Writer) Append(records []map[string]interface{}) error {
	// Serialize records with MessagePack
	payload, err := msgpack.Marshal(records)
	if err != nil {
		return fmt.Errorf("failed to serialize records: %w", err)
	}

	return w.AppendRaw(payload)
}

// AppendRaw writes raw (already serialized) msgpack bytes to the WAL asynchronously
// This is a zero-copy optimization - use this when you already have msgpack bytes
func (w *Writer) AppendRaw(payload []byte) error {
	// Validate payload size to prevent integer overflow during allocation (CWE-190)
	if len(payload) > MaxWALPayloadSize {
		return fmt.Errorf("%w: size %d exceeds limit %d", ErrPayloadTooLarge, len(payload), MaxWALPayloadSize)
	}

	// Calculate checksum (CRC32)
	checksum := crc32.ChecksumIEEE(payload)

	// Get current timestamp (microseconds since epoch)
	timestampUS := uint64(time.Now().UnixMicro())

	// Call replication hook before local write (if set)
	// This enables real-time streaming to reader nodes
	if w.replicationHook != nil {
		w.mu.Lock()
		w.sequence++
		seq := w.sequence
		hook := w.replicationHook
		w.mu.Unlock()

		hook(&ReplicationEntry{
			Sequence:    seq,
			TimestampUS: timestampUS,
			Payload:     payload,
		})
	}

	// Build complete entry: header + payload
	entryData := make([]byte, WALEntryHeaderSize+len(payload))
	binary.BigEndian.PutUint32(entryData[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
	binary.BigEndian.PutUint32(entryData[12:16], checksum)
	copy(entryData[WALEntryHeaderSize:], payload)

	// Non-blocking send to channel
	select {
	case w.entryChan <- walEntry{data: entryData}:
		// Successfully queued
		return nil
	default:
		// Buffer full - drop entry (trade durability for throughput)
		atomic.AddInt64(&w.DroppedEntries, 1)
		return nil // Don't return error to avoid slowing down the caller
	}
}

// sync syncs the WAL file to disk based on sync mode
func (w *Writer) sync() {
	if w.currentFile == nil {
		return
	}

	switch w.config.SyncMode {
	case SyncModeFsync:
		// Full sync: data + metadata
		w.currentFile.Sync()
	case SyncModeFdatasync:
		// Data sync only (use Sync on systems without fdatasync)
		w.currentFile.Sync()
	case SyncModeAsync:
		// No explicit sync, rely on OS buffer cache
	}
}

// Close closes the WAL writer
func (w *Writer) Close() error {
	// Signal shutdown
	close(w.done)

	// Wait for writer goroutine to finish
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFile != nil {
		err := w.currentFile.Close()
		w.currentFile = nil
		w.logger.Info().
			Str("file", w.currentPath).
			Int64("dropped_entries", atomic.LoadInt64(&w.DroppedEntries)).
			Msg("WAL closed")
		return err
	}
	return nil
}

// Stats returns WAL statistics
func (w *Writer) Stats() map[string]interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	age := time.Since(w.startTime)
	return map[string]interface{}{
		"current_file":        w.currentPath,
		"current_size_mb":     float64(w.currentSize) / 1024 / 1024,
		"current_age_seconds": age.Seconds(),
		"sync_mode":           string(w.config.SyncMode),
		"total_entries":       w.TotalEntries,
		"total_bytes":         w.TotalBytes,
		"total_syncs":         w.TotalSyncs,
		"total_rotations":     w.TotalRotations,
		"dropped_entries":     atomic.LoadInt64(&w.DroppedEntries),
		"buffer_size":         w.config.BufferSize,
		"buffer_used":         len(w.entryChan),
	}
}

// CurrentFile returns the current WAL file path
func (w *Writer) CurrentFile() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentPath
}

// SetReplicationHook sets the hook function called for each WAL entry.
// This enables cluster replication by streaming entries to reader nodes.
// The hook is called synchronously before the entry is written locally.
func (w *Writer) SetReplicationHook(hook ReplicationHook) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.replicationHook = hook
	w.logger.Info().Msg("Replication hook set")
}

// CurrentSequence returns the current replication sequence number.
func (w *Writer) CurrentSequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}
