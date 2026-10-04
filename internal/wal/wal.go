package wal

import (
	"bytes"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/Basekick-Labs/msgpack/v6/msgpcode"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
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
	MaxWALPayloadSize      = 100 * 1024 * 1024 // 100MB
	walTrackedHeaderSize   = 1 + 16
	walCheckpointBatchSize = 100_000

	// walChunkTarget is the payload size an oversized payload is chunked down
	// to (#677). It sits well below MaxWALPayloadSize so a chunk still clears
	// the single-entry cap after its msgpack container headers, and stays under
	// the replication protocol's own 100MB message limit once the JSON + base64
	// envelope (bytes expand 4/3) is accounted for.
	walChunkTarget = 32 * 1024 * 1024 // 32MB

	// WALEnvelopeMarker is the first byte of an enveloped WAL payload.
	// Enveloped format: [0x01][2-byte db name length][db name][original msgpack]
	// Since msgpack maps/arrays always start with bytes >= 0x80, 0x01 is unambiguous.
	WALEnvelopeMarker   = 0x01
	WALCheckpointMarker = 0x02
	WALTrackedMarker    = 0x03
)

// ParseEnvelope extracts the database name and msgpack payload from a WAL entry.
// If the payload uses the envelope format [0x01][2-byte dbLen][dbName][msgpack],
// it returns the database name and the inner msgpack bytes. Otherwise, it returns
// defaultDB and the original payload unchanged.
func ParseEnvelope(payload []byte, defaultDB string) (database string, msgpackData []byte) {
	if len(payload) > 3 && payload[0] == WALEnvelopeMarker {
		dbLen := binary.BigEndian.Uint16(payload[1:3])
		if int(3+dbLen) <= len(payload) {
			return string(payload[3 : 3+dbLen]), payload[3+dbLen:]
		}
	}
	return defaultDB, payload
}

// splitOversizedPayload divides a payload larger than MaxWALPayloadSize into
// chunks that each fit the cap (#677). It never operates on arbitrary byte
// ranges: WAL payloads are replayed as whole msgpack values, and a bare byte
// tail of an array is not one — it decodes as the first value only and
// silently drops the rest. Chunks are re-emitted as self-contained msgpack
// values instead:
//
//   - a top-level array (the row format Append marshals) is split between
//     elements; each chunk is a shorter array holding a contiguous run of
//     the same records
//   - a top-level map whose "columns" value is a map of arrays (the columnar
//     format the zero-copy path stores) is split by row range: every other
//     key carries over unchanged and each column array is sliced to the same
//     [lo, hi) rows
//
// A payload whose top-level shape cannot be split without inventing a new
// record format (including a map without "columns", or a single element
// larger than the cap) is returned as the sole chunk for the caller to
// reject. A crash between chunk writes replays the completed prefix, which is
// safe because rows are independent.
func splitOversizedPayload(payload []byte) ([][]byte, error) {
	reader := bytes.NewReader(payload)
	dec := msgpack.NewDecoder(reader)
	code, err := dec.PeekCode()
	if err != nil {
		return nil, fmt.Errorf("failed to read msgpack header: %w", err)
	}

	if msgpcode.IsFixedArray(code) || code == msgpcode.Array16 || code == msgpcode.Array32 {
		n, err := dec.DecodeArrayLen()
		if err != nil {
			return nil, fmt.Errorf("failed to read array header: %w", err)
		}
		var chunks [][]byte
		var elem bytes.Buffer
		count := 0
		flush := func() error {
			if count == 0 {
				return nil
			}
			var out bytes.Buffer
			enc := msgpack.NewEncoder(&out)
			if err := enc.EncodeArrayLen(count); err != nil {
				return err
			}
			out.Write(elem.Bytes())
			chunks = append(chunks, out.Bytes())
			elem.Reset()
			count = 0
			return nil
		}
		for i := 0; i < n; i++ {
			raw, err := dec.DecodeRaw()
			if err != nil {
				return nil, fmt.Errorf("failed to read element %d: %w", i, err)
			}
			elem.Write(raw)
			count++
			if elem.Len() >= walChunkTarget {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
		if err := flush(); err != nil {
			return nil, err
		}
		if len(chunks) == 0 {
			// An empty array cannot be oversized; return it unchanged.
			chunks = append(chunks, payload)
		}
		return chunks, nil
	}

	if msgpcode.IsFixedMap(code) || code == msgpcode.Map16 || code == msgpcode.Map32 {
		n, err := dec.DecodeMapLen()
		if err != nil {
			return nil, fmt.Errorf("failed to read map header: %w", err)
		}
		fields := make([]rawField, 0, n)
		colIdx := -1
		for i := 0; i < n; i++ {
			key := rawSpan{start: len(payload) - reader.Len()}
			if err := dec.Skip(); err != nil {
				return nil, fmt.Errorf("failed to read map key: %w", err)
			}
			key.end = len(payload) - reader.Len()
			value := rawSpan{start: len(payload) - reader.Len()}
			if err := dec.Skip(); err != nil {
				return nil, fmt.Errorf("failed to read map value: %w", err)
			}
			value.end = len(payload) - reader.Len()
			fields = append(fields, rawField{key: key, value: value})
			var name string
			if err := msgpack.Unmarshal(payload[key.start:key.end], &name); err == nil && name == "columns" {
				colIdx = i
			}
		}
		if colIdx < 0 {
			return [][]byte{payload}, nil
		}

		columns, rows, err := decodeColumnarColumns(payload, fields[colIdx].value)
		if err != nil {
			return nil, err
		}
		if len(columns) == 0 {
			return [][]byte{payload}, nil
		}
		if rows == 0 {
			return [][]byte{payload}, nil
		}

		// Slice columns by row range, sized so a chunk's columns stay near
		// walChunkTarget. Sizes vary per column; the target only picks the
		// range length, and the hard cap check on the final entry rejects a
		// range that still does not fit.
		bytesPerRow := fields[colIdx].value.end - fields[colIdx].value.start
		bytesPerRow /= rows
		if bytesPerRow < 1 {
			bytesPerRow = 1
		}
		rowsPerChunk := walChunkTarget / bytesPerRow
		if rowsPerChunk < 1 {
			rowsPerChunk = 1
		}
		if err := recordColumnOffsets(payload, columns, rows, rowsPerChunk); err != nil {
			return nil, err
		}

		var chunks [][]byte
		for lo := 0; lo < rows; lo += rowsPerChunk {
			hi := lo + rowsPerChunk
			if hi > rows {
				hi = rows
			}
			out, err := encodeColumnarRange(payload, fields, colIdx, columns, lo/rowsPerChunk, hi-lo)
			if err != nil {
				return nil, err
			}
			chunks = append(chunks, out)
		}
		return chunks, nil
	}

	return [][]byte{payload}, nil
}

type rawSpan struct {
	start int
	end   int
}

type rawField struct {
	key   rawSpan
	value rawSpan
}

type rawColumn struct {
	name    string
	key     rawSpan
	values  rawSpan
	offsets []int
}

func decodeColumnarColumns(payload []byte, raw rawSpan) ([]rawColumn, int, error) {
	reader := bytes.NewReader(payload[raw.start:raw.end])
	dec := msgpack.NewDecoder(reader)
	rawLen := raw.end - raw.start
	n, err := dec.DecodeMapLen()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read columns map: %w", err)
	}
	columns := make([]rawColumn, 0, n)
	rows := -1
	for i := 0; i < n; i++ {
		key := rawSpan{start: raw.start + rawLen - reader.Len()}
		if err := dec.Skip(); err != nil {
			return nil, 0, fmt.Errorf("failed to read column key: %w", err)
		}
		key.end = raw.start + rawLen - reader.Len()
		var name string
		if err := msgpack.Unmarshal(payload[key.start:key.end], &name); err != nil {
			return nil, 0, fmt.Errorf("failed to decode column name: %w", err)
		}
		values := rawSpan{start: raw.start + rawLen - reader.Len()}
		valueLen, err := dec.DecodeArrayLen()
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read column %q array: %w", name, err)
		}
		if valueLen < 0 {
			return nil, 0, fmt.Errorf("column %q is nil, want an array", name)
		}
		for j := 0; j < valueLen; j++ {
			if err := dec.Skip(); err != nil {
				return nil, 0, fmt.Errorf("failed to read column %q value %d: %w", name, j, err)
			}
		}
		values.end = raw.start + rawLen - reader.Len()
		if rows == -1 {
			rows = valueLen
		} else if valueLen != rows {
			return nil, 0, fmt.Errorf("column %q has %d values, want %d", name, valueLen, rows)
		}
		columns = append(columns, rawColumn{name: name, key: key, values: values})
	}
	return columns, rows, nil
}

func recordColumnOffsets(payload []byte, columns []rawColumn, rows, rowsPerChunk int) error {
	for i := range columns {
		column := &columns[i]
		reader := bytes.NewReader(payload[column.values.start:column.values.end])
		dec := msgpack.NewDecoder(reader)
		valueLen, err := dec.DecodeArrayLen()
		if err != nil {
			return fmt.Errorf("failed to reread column %q array: %w", column.name, err)
		}
		if valueLen != rows {
			return fmt.Errorf("column %q has %d values, want %d", column.name, valueLen, rows)
		}
		column.offsets = make([]int, 0, (rows+rowsPerChunk-1)/rowsPerChunk+1)
		column.offsets = append(column.offsets, column.values.start+len(payload[column.values.start:column.values.end])-reader.Len())
		for lo := 0; lo < rows; lo += rowsPerChunk {
			hi := lo + rowsPerChunk
			if hi > rows {
				hi = rows
			}
			for row := lo; row < hi; row++ {
				if err := dec.Skip(); err != nil {
					return fmt.Errorf("failed to read column %q value %d: %w", column.name, row, err)
				}
			}
			column.offsets = append(column.offsets, column.values.start+len(payload[column.values.start:column.values.end])-reader.Len())
		}
	}
	return nil
}

// encodeColumnarRange re-emits the columnar record with every column sliced
// to rows [lo, hi). Keys other than "columns" (e.g. "m") carry over unchanged
// through their raw bytes.
func encodeColumnarRange(payload []byte, fields []rawField, colIdx int, columns []rawColumn, chunk, rowCount int) ([]byte, error) {
	var cols bytes.Buffer
	cenc := msgpack.NewEncoder(&cols)
	if err := cenc.EncodeMapLen(len(columns)); err != nil {
		return nil, err
	}
	for _, column := range columns {
		cols.Write(payload[column.key.start:column.key.end])
		if err := cenc.EncodeArrayLen(rowCount); err != nil {
			return nil, err
		}
		cols.Write(payload[column.offsets[chunk]:column.offsets[chunk+1]])
	}

	var out bytes.Buffer
	enc := msgpack.NewEncoder(&out)
	if err := enc.EncodeMapLen(len(fields)); err != nil {
		return nil, err
	}
	for i := range fields {
		out.Write(payload[fields[i].key.start:fields[i].key.end])
		if i == colIdx {
			out.Write(cols.Bytes())
		} else {
			out.Write(payload[fields[i].value.start:fields[i].value.end])
		}
	}
	return out.Bytes(), nil
}

// SyncMode defines how WAL syncs to disk
type SyncMode string

const (
	SyncModeFsync     SyncMode = "fsync"     // Full sync: data + metadata (safest)
	SyncModeFdatasync SyncMode = "fdatasync" // Data sync only (balanced, default)
	SyncModeAsync     SyncMode = "async"     // No explicit sync (fastest, least safe)
)

// ErrPayloadTooLarge indicates the payload exceeds MaxWALPayloadSize.
var ErrPayloadTooLarge = errors.New("WAL payload exceeds maximum allowed size")

func oversizedPayloadError(err error) error {
	metrics.Get().IncWALOversizedPayloads()
	return fmt.Errorf("%w: %v", ErrPayloadTooLarge, err)
}

// ErrWALDropped is returned by Append/AppendRaw/AppendRawWithMeta when the
// async entry channel is full and the entry is dropped. Previous behavior
// returned nil and silently incremented DroppedEntries — callers logging
// "data preserved in WAL for recovery" downstream were reporting durability
// they did not actually have. Returning a sentinel lets callers (the
// ingestion buffer in particular) increment their own error counters and
// surface accurate operator-facing messages. Use errors.Is to detect.
var ErrWALDropped = errors.New("WAL entry dropped: async buffer full")

// walEntry is a pre-serialized WAL entry ready for writing
type walEntry struct {
	data    []byte // Complete entry: header + payload
	durable bool   // Force a sync before acknowledging this entry
	done    chan error

	// seq is the tracked sequence this entry carries, or 0 for an entry that
	// carries none (an untracked data append, or a flush checkpoint). The
	// writer loop records it against the file the write actually SUCCEEDED
	// into, which is not necessarily the file that was current when the entry
	// was enqueued: a failed write rotates and retries, and rotation can land
	// between the append and the write regardless.
	seq uint64
	// rotate makes this a command rather than a payload: the writer loop
	// rotates when it dequeues it, which is how Rotate() orders itself behind
	// entries already queued. data is nil for a command.
	rotate bool
	// untrackedData marks a DATA entry with no tracked sequence. A file holding
	// one can never be purged by sequence, because nothing will ever report it
	// flushed. Checkpoint entries also have seq 0 but must NOT set this, or
	// every file would hold one and none would ever be reclaimable.
	untrackedData bool
}

// fileSeqState is what the purge needs to know about one WAL file this process
// created. Keyed by path in w.fileSeqs, ordered by w.fileOrder.
type fileSeqState struct {
	// maxSeq is the highest tracked sequence written to this file. It is an
	// upper bound, not a description: sequences are assigned before the entries
	// are enqueued, so interleaved appends scatter them across files and one
	// file can hold a non-contiguous set. The bound is all the purge rule needs
	// — see Writer.PurgeFlushed.
	maxSeq uint64
	// hasUntrackedData is set when an untracked data entry lands here.
	hasUntrackedData bool
}

// WriterConfig holds configuration for WAL writer
type WriterConfig struct {
	WALDir       string        // Directory for WAL files
	SyncMode     SyncMode      // Sync mode: fsync, fdatasync, async
	MaxSizeBytes int64         // Rotate WAL when it reaches this size (default: 100MB)
	MaxAge       time.Duration // Rotate WAL after this duration (default: 1 hour)
	SyncInterval time.Duration // Sync at most this often (default: 100ms, 0 = sync every write)
	SyncBytes    int64         // Sync after this many bytes written (default: 1MB, 0 = no byte threshold)
	BufferSize   int           // Size of async write buffer (default: 10000)
	Logger       zerolog.Logger
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
	trackedInstance uint64

	// fileSeqs and fileOrder track what this process wrote to each WAL file, so
	// the purge can decide by what has been flushed rather than by mtime
	// (#1009). Both are guarded by w.mu. fileOrder is rotation order, which is
	// the ordering the purge needs — filename timestamps and mtimes both move
	// backwards under a clock step, rotation order cannot. Files created by a
	// PREVIOUS process appear in neither, which is deliberate: nothing here
	// knows whether their data was flushed, so only recovery may delete them.
	fileSeqs        map[string]*fileSeqState
	fileOrder       []string
	trackedSequence uint64
	// pendingTracked maps this process's tracked identity token to its
	// monotonic tracked sequence until a durable flush checkpoint is written.
	// It is the source of truth for MinUnflushedSequence and therefore for
	// clock-free WAL purging (#1009).
	pendingTracked map[string]uint64
	closed         bool

	// Metrics (atomic for lock-free reads)
	TotalEntries   int64
	TotalBytes     int64
	TotalSyncs     int64
	TotalRotations int64
	DroppedEntries int64 // Entries dropped due to full buffer
	FailedWrites   int64 // Write failures to current WAL file

	mu sync.Mutex
}

// NewWriter creates a new WAL writer
func NewWriter(cfg *WriterConfig) (*Writer, error) {
	var instanceBytes [8]byte
	for {
		if _, err := cryptorand.Read(instanceBytes[:]); err != nil {
			return nil, fmt.Errorf("failed to initialize WAL tracked identity: %w", err)
		}
		if binary.BigEndian.Uint64(instanceBytes[:]) != 0 {
			break
		}
	}
	trackedInstance := binary.BigEndian.Uint64(instanceBytes[:])

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
	if cfg.BufferSize < 1 {
		cfg.BufferSize = 10000 // Default buffer size
	} else if cfg.BufferSize > 1000000 {
		cfg.BufferSize = 1000000 // Cap to prevent excessive memory allocation
	}

	// Create WAL directory with owner-only permissions (WAL contains sensitive data)
	if err := os.MkdirAll(cfg.WALDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	w := &Writer{
		config:          *cfg,
		logger:          cfg.Logger.With().Str("component", "wal-writer").Logger(),
		lastSyncTime:    time.Now(),
		entryChan:       make(chan walEntry, cfg.BufferSize),
		fileSeqs:        make(map[string]*fileSeqState),
		pendingTracked:  make(map[string]uint64),
		done:            make(chan struct{}),
		trackedInstance: trackedInstance,
	}

	// Initialize first WAL file
	if err := w.rotate(); err != nil {
		return nil, fmt.Errorf("failed to create initial WAL file: %w", err)
	}

	// Start async writer goroutine
	w.wg.Add(1)
	go w.writerLoop()

	// Say so when the selected mode cannot be honored, rather than reporting
	// "fdatasync" while performing a full fsync.
	if cfg.SyncMode == SyncModeFdatasync && !dataSyncSupported {
		w.logger.Info().
			Str("sync_mode", string(cfg.SyncMode)).
			Msg("fdatasync is unavailable on this platform; using full fsync instead")
	}

	w.logger.Info().
		Str("dir", cfg.WALDir).
		Str("sync_mode", string(cfg.SyncMode)).
		Bool("fdatasync_supported", dataSyncSupported).
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
			w.processEntry(entry)

		case <-syncTicker.C:
			// Periodic sync
			w.mu.Lock()
			if w.bytesSinceSync > 0 {
				w.sync()
				w.lastSyncTime = time.Now()
				w.bytesSinceSync = 0
				atomic.AddInt64(&w.TotalSyncs, 1)
			}
			w.mu.Unlock()

		case <-w.done:
			// Drain remaining entries before shutdown
			for {
				select {
				case entry := <-w.entryChan:
					w.processEntry(entry)
				default:
					// No more entries, final sync and exit
					w.mu.Lock()
					if w.bytesSinceSync > 0 {
						w.sync()
						atomic.AddInt64(&w.TotalSyncs, 1)
					}
					w.mu.Unlock()
					return
				}
			}
		}
	}
}

func (w *Writer) processEntry(entry walEntry) {
	err := w.writeEntry(entry)
	if entry.done != nil {
		entry.done <- err
		close(entry.done)
	}
}

// writeEntry writes a single entry to the WAL file (called from writerLoop)
func (w *Writer) writeEntry(entry walEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entry.rotate {
		return w.rotate()
	}

	// Write entry
	n, err := w.currentFile.Write(entry.data)
	if err != nil {
		atomic.AddInt64(&w.FailedWrites, 1)
		metrics.Get().IncWALFailedWrites()
		w.logger.Error().Err(err).Msg("Failed to write WAL entry, attempting rotation")

		// Attempt rotation — the current file handle may be bad (disk full,
		// permission change, file deleted). A new file might succeed.
		if rotErr := w.rotate(); rotErr != nil {
			w.logger.Error().Err(rotErr).Msg("Rotation after write failure also failed, entry lost")
			return rotErr
		}

		// Retry write on the new file. If the original write failed partway
		// (e.g. disk filled mid-write), a truncated entry is left at the tail
		// of the old file. That is safe: the reader treats a partial trailing
		// entry as clean EOF (truncated header) or a skipped corrupted entry
		// (truncated payload / checksum mismatch), never a fatal error — see
		// Reader.readEntry. The full entry is re-written here on the new file.
		n, err = w.currentFile.Write(entry.data)
		if err != nil {
			atomic.AddInt64(&w.FailedWrites, 1)
			metrics.Get().IncWALFailedWrites()
			w.logger.Error().Err(err).Msg("Retry write after rotation also failed, entry lost")
			return err
		}
	}

	// Record what this entry contributed to the file it actually landed in.
	// This must be here and not before the write: the failure path above
	// rotates and retries, so an entry can be durable in a DIFFERENT file than
	// the one that was current when it was dequeued. Attributing it to the old
	// file would let the new file's sequence bound fall below the unflushed
	// floor while this entry is still unflushed, and the purge would then
	// delete the only copy (#1009).
	w.noteWrittenLocked(entry)

	bytesWritten := int64(n)
	w.currentSize += bytesWritten
	w.bytesSinceSync += bytesWritten

	// Update metrics
	atomic.AddInt64(&w.TotalEntries, 1)
	atomic.AddInt64(&w.TotalBytes, bytesWritten)

	if entry.durable {
		if err := dataSync(w.currentFile); err != nil {
			w.logger.Error().Err(err).Msg("WAL checkpoint sync failed")
			return err
		}
		w.lastSyncTime = time.Now()
		w.bytesSinceSync = 0
		atomic.AddInt64(&w.TotalSyncs, 1)
	} else if w.bytesSinceSync >= w.config.SyncBytes {
		w.sync()
		w.lastSyncTime = time.Now()
		w.bytesSinceSync = 0
		atomic.AddInt64(&w.TotalSyncs, 1)
	}

	// Check if rotation needed
	age := time.Since(w.startTime)
	if w.currentSize >= w.config.MaxSizeBytes || age >= w.config.MaxAge {
		if err := w.rotate(); err != nil {
			w.logger.Error().Err(err).Msg("Failed to rotate WAL")
		}
	}
	return nil
}

// Rotate closes the active WAL file and starts a new one, and does not return
// until every entry enqueued before the call has been written to the OLD file.
//
// That ordering is the whole point. A forced rotation exists so recovery can
// reach data that is only in the active file (#1009): the file is rotated away
// and then replayed. A rotation that simply took w.mu and swapped the handle
// would leave every entry already sitting in the queue — up to BufferSize of
// them — to be written to the NEW file, where the pass that rotated would not
// look. Going through the writer loop puts the rotation behind those entries.
func (w *Writer) Rotate() error {
	done := make(chan error, 1)
	if err := w.tryEnqueueEntry(walEntry{rotate: true, durable: true, done: done}); err != nil {
		return err
	}
	return <-done
}

// rotate creates a new WAL file.
// The new file is opened and its header written BEFORE the old file is closed,
// so a failure to create or initialize the new file leaves the old file intact
// (no nil w.currentFile that would panic on the next write).
func (w *Writer) rotate() error {
	// Generate new filename
	timestamp := time.Now().UTC().Format("20060102_150405.000000000")
	filename := fmt.Sprintf("arc-%s.wal", timestamp)
	newPath := filepath.Join(w.config.WALDir, filename)

	// Open new file first — if this fails, old file is still valid
	newFile, err := os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}

	// Write WAL header to new file
	var header [WALFileHeaderSize]byte
	copy(header[0:4], WALMagic)
	binary.BigEndian.PutUint16(header[4:6], WALVersion)
	header[6] = WALChecksumCRC32

	n, err := newFile.Write(header[:])
	if err != nil {
		newFile.Close()
		return fmt.Errorf("failed to write WAL header: %w", err)
	}

	// New file is ready — now close the old one (sync any pending data first)
	if w.currentFile != nil {
		if w.bytesSinceSync > 0 {
			w.sync()
			atomic.AddInt64(&w.TotalSyncs, 1)
		}
		w.currentFile.Close()
	}

	// Swap to new file
	w.currentFile = newFile
	w.currentPath = newPath
	w.currentSize = int64(n)
	w.startTime = time.Now()
	w.lastSyncTime = time.Now()
	w.bytesSinceSync = 0
	atomic.AddInt64(&w.TotalRotations, 1)

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

// AppendRawWithMeta writes raw msgpack bytes with database metadata envelope.
// Format: [0x01 marker][2-byte db name length][db name][original msgpack]
// This preserves the database name for correct recovery routing.
//
// Unlike calling AppendRaw with a pre-built envelope, this method builds the
// WAL entry in a single allocation to avoid copying the payload twice.
//
// A payload whose logical entry exceeds MaxWALPayloadSize is split into
// multiple entries, each carrying the same database envelope (#677) — a wide
// ingest request previously landed here as a wholesale ErrPayloadTooLarge
// rejection, so the WAL silently recorded nothing while ingest reported
// healthy.
func (w *Writer) AppendRawWithMeta(database string, payload []byte) error {
	dbBytes := []byte(database)
	envelopeHeaderLen := 1 + 2 + len(dbBytes) // marker + dbLen + dbName
	totalPayloadLen := envelopeHeaderLen + len(payload)

	if totalPayloadLen <= MaxWALPayloadSize {
		return w.appendEnvelopedEntry(dbBytes, envelopeHeaderLen, payload, totalPayloadLen)
	}

	// Oversized: split the msgpack payload into size-valid chunks and write
	// each as its own enveloped entry. A chunk that still exceeds the cap (a
	// single record or row larger than the limit) keeps the loud rejection.
	chunks, err := splitOversizedPayload(payload)
	if err != nil {
		return oversizedPayloadError(err)
	}
	for _, chunk := range chunks {
		if chunkLen := envelopeHeaderLen + len(chunk); chunkLen > MaxWALPayloadSize {
			return oversizedPayloadError(fmt.Errorf("size %d exceeds limit %d", chunkLen, MaxWALPayloadSize))
		}
	}
	for _, chunk := range chunks {
		if err := w.appendEnvelopedEntry(dbBytes, envelopeHeaderLen, chunk, envelopeHeaderLen+len(chunk)); err != nil {
			return err
		}
	}
	return nil
}

// AppendTracked writes a row-format entry and returns its payload identity.
// The identity is used by flush checkpoints to avoid replaying data that was
// already durably written to storage.
func (w *Writer) AppendTracked(records []map[string]interface{}) ([]string, error) {
	payload, err := msgpack.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize records: %w", err)
	}
	chunks := [][]byte{payload}
	if !trackedPayloadFits(len(payload), 0) {
		chunks, err = splitOversizedPayload(payload)
		if err != nil {
			return nil, oversizedPayloadError(err)
		}
	}
	hashes := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		if !trackedPayloadFits(len(chunk), 0) {
			return nil, oversizedPayloadError(fmt.Errorf("tracked size %d exceeds limit %d", len(chunk)+walTrackedHeaderSize, MaxWALPayloadSize))
		}
		token, err := w.appendTrackedEntry(chunk)
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, token)
	}
	return hashes, nil
}

// AppendRawWithMetaTracked is the tracked counterpart to AppendRawWithMeta.
func (w *Writer) AppendRawWithMetaTracked(database string, payload []byte) ([]string, error) {
	if len(database) > 255 {
		return nil, fmt.Errorf("database name too long: %d bytes", len(database))
	}
	dbBytes := []byte(database)
	envelopeHeaderLen := 3 + len(dbBytes)
	chunks := [][]byte{payload}
	if !trackedPayloadFits(len(payload), envelopeHeaderLen) {
		var err error
		chunks, err = splitOversizedPayload(payload)
		if err != nil {
			return nil, oversizedPayloadError(err)
		}
	}
	var hashes []string
	for _, chunk := range chunks {
		if !trackedPayloadFits(len(chunk), envelopeHeaderLen) {
			return nil, oversizedPayloadError(fmt.Errorf("tracked size %d exceeds limit %d", envelopeHeaderLen+len(chunk)+walTrackedHeaderSize, MaxWALPayloadSize))
		}
		token, err := w.appendTrackedEntry(envelopePayload(dbBytes, chunk))
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, token)
	}
	return hashes, nil
}

// MarkFlushed appends a checkpoint after the corresponding data entries have
// reached durable storage. A failed checkpoint is safe: it can only cause a
// replay duplicate, never data loss.
func (w *Writer) MarkFlushed(hashes []string) error {
	if len(hashes) == 0 {
		return nil
	}
	for start := 0; start < len(hashes); start += walCheckpointBatchSize {
		end := start + walCheckpointBatchSize
		if end > len(hashes) {
			end = len(hashes)
		}
		payload, err := msgpack.Marshal(hashes[start:end])
		if err != nil {
			return err
		}
		checkpoint := append([]byte{WALCheckpointMarker}, payload...)
		checksum := crc32.ChecksumIEEE(checkpoint)
		timestampUS := uint64(time.Now().UnixMicro())
		entryData := make([]byte, WALEntryHeaderSize+len(checkpoint))
		binary.BigEndian.PutUint32(entryData[0:4], uint32(len(checkpoint)))
		binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
		binary.BigEndian.PutUint32(entryData[12:16], checksum)
		copy(entryData[WALEntryHeaderSize:], checkpoint)
		done := make(chan error, 1)
		if err := w.tryEnqueueEntry(walEntry{data: entryData, durable: true, done: done}); err != nil {
			return err
		}
		if err := <-done; err != nil {
			return fmt.Errorf("failed to persist WAL flush checkpoint: %w", err)
		}
		w.mu.Lock()
		for _, token := range hashes[start:end] {
			delete(w.pendingTracked, token)
		}
		w.mu.Unlock()
	}
	return nil
}

func envelopePayload(dbBytes, payload []byte) []byte {
	out := make([]byte, 3+len(dbBytes)+len(payload))
	out[0] = WALEnvelopeMarker
	binary.BigEndian.PutUint16(out[1:3], uint16(len(dbBytes)))
	copy(out[3:], dbBytes)
	copy(out[3+len(dbBytes):], payload)
	return out
}

func payloadHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:])
}

func trackedPayloadFits(payloadLen, envelopeHeaderLen int) bool {
	return payloadLen <= MaxWALPayloadSize-walTrackedHeaderSize-envelopeHeaderLen
}

func (w *Writer) appendTrackedEntry(logicalPayload []byte) (string, error) {
	seq := atomic.AddUint64(&w.trackedSequence, 1)
	token := fmt.Sprintf("%016x%016x", w.trackedInstance, seq)
	trackedPayload := make([]byte, walTrackedHeaderSize+len(logicalPayload))
	trackedPayload[0] = WALTrackedMarker
	binary.BigEndian.PutUint64(trackedPayload[1:9], w.trackedInstance)
	binary.BigEndian.PutUint64(trackedPayload[9:17], seq)
	copy(trackedPayload[17:], logicalPayload)
	checksum := crc32.ChecksumIEEE(trackedPayload)
	timestampUS := uint64(time.Now().UnixMicro())
	if w.replicationHook != nil {
		w.mu.Lock()
		w.sequence++
		replicationSequence := w.sequence
		hook := w.replicationHook
		w.mu.Unlock()
		hook(&ReplicationEntry{Sequence: replicationSequence, TimestampUS: timestampUS, Payload: logicalPayload})
	}
	entryData := make([]byte, WALEntryHeaderSize+len(trackedPayload))
	binary.BigEndian.PutUint32(entryData[0:4], uint32(len(trackedPayload)))
	binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
	binary.BigEndian.PutUint32(entryData[12:16], checksum)
	copy(entryData[WALEntryHeaderSize:], trackedPayload)
	// Record the pending identity BEFORE enqueueing. The writer goroutine may
	// consume the entry immediately; publishing it first closes the race where
	// maintenance could otherwise observe an empty floor and purge its file.
	w.mu.Lock()
	w.pendingTracked[token] = seq
	w.mu.Unlock()

	// Carry the sequence so the writer loop can record it against the file the
	// write actually lands in (#1009).
	if err := w.tryEnqueueEntry(walEntry{data: entryData, seq: seq}); err != nil {
		w.mu.Lock()
		delete(w.pendingTracked, token)
		w.mu.Unlock()
		return "", err
	}
	return token, nil
}

// appendEnvelopedEntry is AppendRawWithMeta's single-entry fast path: CRC,
// replication hook, and entry assembly for one size-validated payload.
func (w *Writer) appendEnvelopedEntry(dbBytes []byte, envelopeHeaderLen int, payload []byte, totalPayloadLen int) error {
	// Compute CRC32 over the logical payload (envelope header + msgpack) without copying
	crc := crc32.NewIEEE()
	var envHeader [1 + 2 + 255]byte // envelope header (db name max 255 bytes)
	envHeader[0] = WALEnvelopeMarker
	binary.BigEndian.PutUint16(envHeader[1:3], uint16(len(dbBytes)))
	copy(envHeader[3:], dbBytes)
	crc.Write(envHeader[:envelopeHeaderLen])
	crc.Write(payload)
	checksum := crc.Sum32()

	timestampUS := uint64(time.Now().UnixMicro())

	// Replication hook
	if w.replicationHook != nil {
		w.mu.Lock()
		w.sequence++
		seq := w.sequence
		hook := w.replicationHook
		w.mu.Unlock()

		// Build envelope for replication (unavoidable copy for hook consumers)
		repPayload := make([]byte, totalPayloadLen)
		copy(repPayload, envHeader[:envelopeHeaderLen])
		copy(repPayload[envelopeHeaderLen:], payload)
		hook(&ReplicationEntry{
			Sequence:    seq,
			TimestampUS: timestampUS,
			Payload:     repPayload,
		})
	}

	// Build complete WAL entry in one allocation: header + envelope header + payload
	entryData := make([]byte, WALEntryHeaderSize+totalPayloadLen)
	binary.BigEndian.PutUint32(entryData[0:4], uint32(totalPayloadLen))
	binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
	binary.BigEndian.PutUint32(entryData[12:16], checksum)
	copy(entryData[WALEntryHeaderSize:], envHeader[:envelopeHeaderLen])
	copy(entryData[WALEntryHeaderSize+envelopeHeaderLen:], payload)

	return w.tryEnqueue(entryData)
}

// tryEnqueue is the shared non-blocking send into entryChan used by
// every Append variant. On channel-full it bumps the dropped counter
// (both on the Writer and the global metrics package) and returns
// ErrWALDropped — callers use errors.Is to differentiate from real
// I/O errors. Centralized so the drop accounting is impossible to
// drift across the multiple append paths.
// Every caller that reaches tryEnqueue appends a payload with NO tracked
// sequence, so the file it lands in can never be purged by sequence (#1009).
// Checkpoints do not come through here — they build their own walEntry — which
// is what keeps a file holding only checkpoints reclaimable.
func (w *Writer) tryEnqueue(entryData []byte) error {
	return w.tryEnqueueEntry(walEntry{data: entryData, untrackedData: true})
}

func (w *Writer) tryEnqueueEntry(entry walEntry) error {
	if entry.durable {
		// Coordinate shutdown only for acknowledged durable barriers. Keep the
		// per-record append path lock-free.
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.closed {
			return errors.New("WAL writer is closed")
		}
	}
	select {
	case w.entryChan <- entry:
		return nil
	default:
		atomic.AddInt64(&w.DroppedEntries, 1)
		metrics.Get().IncWALDroppedEntries()
		return ErrWALDropped
	}
}

// AppendRaw writes raw (already serialized) msgpack bytes to the WAL asynchronously
// This is a zero-copy optimization - use this when you already have msgpack bytes
func (w *Writer) AppendRaw(payload []byte) error {
	// Validate payload size to prevent integer overflow during allocation (CWE-190).
	// An oversized payload is split into size-valid entries (#677) instead of
	// being rejected wholesale; a payload that cannot be split (a single
	// element larger than the limit) keeps the loud rejection.
	if len(payload) > MaxWALPayloadSize {
		chunks, err := splitOversizedPayload(payload)
		if err != nil {
			return oversizedPayloadError(err)
		}
		for _, chunk := range chunks {
			if len(chunk) > MaxWALPayloadSize {
				return oversizedPayloadError(fmt.Errorf("size %d exceeds limit %d", len(chunk), MaxWALPayloadSize))
			}
		}
		for _, chunk := range chunks {
			if err := w.appendRawEntry(chunk); err != nil {
				return err
			}
		}
		return nil
	}

	return w.appendRawEntry(payload)
}

// appendRawEntry is AppendRaw's single-entry path: checksum, replication
// hook, and entry assembly for one size-validated payload.
func (w *Writer) appendRawEntry(payload []byte) error {
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

	return w.tryEnqueue(entryData)
}

// sync syncs the WAL file to disk based on sync mode
func (w *Writer) sync() {
	if w.currentFile == nil {
		return
	}

	var syncErr error
	switch w.config.SyncMode {
	case SyncModeFsync:
		// Full sync: data + metadata
		syncErr = w.currentFile.Sync()
	case SyncModeFdatasync:
		// Data sync only. Real fdatasync(2) on Linux; a full Sync elsewhere,
		// where Go does not expose it (see datasync_other.go).
		syncErr = dataSync(w.currentFile)
	case SyncModeAsync:
		// No explicit sync, rely on OS buffer cache
		return
	}

	if syncErr != nil {
		w.logger.Error().Err(syncErr).Msg("WAL sync failed")
	}
}

// Close closes the WAL writer
func (w *Writer) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()

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

// purgeWALFiles deletes WAL files matching the given filter function.
// Returns the count of deleted files.
func (w *Writer) purgeWALFiles(shouldDelete func(path string) bool) (int, error) {
	pattern := filepath.Join(w.config.WALDir, "*.wal")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, f := range files {
		if shouldDelete(f) {
			if err := os.Remove(f); err != nil {
				w.logger.Error().Err(err).Str("file", f).Msg("Failed to purge WAL file")
			} else {
				deleted++
				// Keep the sequence bookkeeping in step. A stale entry would
				// leave PurgeFlushed walking to a path that no longer exists,
				// where the delete fails and — because it stops rather than
				// skips — nothing after it is ever reclaimed.
				w.mu.Lock()
				w.forgetFileLocked(f)
				w.mu.Unlock()
			}
		}
	}
	return deleted, nil
}

// PurgeAll deletes all WAL files in the directory.
// Call this after a clean shutdown where all data has been flushed to storage,
// so that recovery on next startup doesn't replay already-persisted data.
func (w *Writer) PurgeAll() (int, error) {
	deleted, err := w.purgeWALFiles(func(_ string) bool { return true })
	if deleted > 0 {
		w.logger.Info().Int("deleted", deleted).Msg("Purged WAL files after clean shutdown")
	}
	return deleted, err
}

// PurgeInactive deletes all WAL files except the currently active one.
// Use this during normal operation (unlike PurgeAll which is for shutdown)
// to clean up rotated WAL files after their data has been flushed to storage.
func (w *Writer) PurgeInactive() (int, error) {
	w.mu.Lock()
	activePath := w.currentPath
	w.mu.Unlock()

	deleted, err := w.purgeWALFiles(func(path string) bool {
		return path != activePath
	})
	if deleted > 0 {
		w.logger.Info().Int("deleted", deleted).Msg("Purged inactive WAL files")
	}
	return deleted, err
}

// noteWrittenLocked records an entry against w.currentPath. Caller holds w.mu.
func (w *Writer) noteWrittenLocked(entry walEntry) {
	if w.currentPath == "" {
		return
	}
	state, ok := w.fileSeqs[w.currentPath]
	if !ok {
		state = &fileSeqState{}
		w.fileSeqs[w.currentPath] = state
		// Rotation order, which is the order the purge walks. Appending here
		// rather than in rotate() keeps the two structures in step even for a
		// file created by the write-failure retry path.
		w.fileOrder = append(w.fileOrder, w.currentPath)
	}
	if entry.untrackedData {
		state.hasUntrackedData = true
		return
	}
	if entry.seq > state.maxSeq {
		state.maxSeq = entry.seq
	}
}

// forgetFileLocked drops a deleted file from the purge bookkeeping. Caller
// holds w.mu.
func (w *Writer) forgetFileLocked(path string) {
	if _, ok := w.fileSeqs[path]; !ok {
		return
	}
	delete(w.fileSeqs, path)
	for i, p := range w.fileOrder {
		if p == path {
			w.fileOrder = append(w.fileOrder[:i], w.fileOrder[i+1:]...)
			break
		}
	}
}

// MinUnflushedSequence returns the lowest tracked sequence that has not yet
// been covered by a durable flush checkpoint. When every tracked entry is
// checkpointed it returns MaxUint64, which makes every eligible rotated file
// reclaimable. Entries are tracked by identity token so duplicate payloads do
// not collide and old-process checkpoints are harmless.
func (w *Writer) MinUnflushedSequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	min := ^uint64(0)
	for _, seq := range w.pendingTracked {
		if seq < min {
			min = seq
		}
	}
	return min
}

// PurgeFlushed deletes rotated WAL files whose data is known to have reached
// storage, where "known" means every sequence they hold is below minUnflushedSeq
// — the lowest sequence that is still only in memory.
//
// This replaces purging by modification time, which assumed flush latency was
// at most a fixed age and deleted acknowledged-but-unflushed data whenever that
// assumption broke (#966, #1009; PR #997 measured 48,500 records lost this way).
// It is clock-free: no mtime granularity, no NTP steps.
//
// Three rules, each of which is a data-loss or duplicate bug if dropped:
//
//   - Only files THIS process created are candidates. A file from a previous
//     process is absent from w.fileSeqs, and nothing here knows whether its
//     data was flushed; only recovery may delete those.
//   - A file holding an untracked data entry is never a candidate. Nothing will
//     ever report that entry flushed, so its sequence bound means nothing.
//   - Walk in rotation order and STOP at the first file that must be retained.
//     A flush checkpoint is appended like any other entry, so a checkpoint
//     covering file R's entries can live in R or in any later file. Deleting a
//     later file while R is retained can therefore destroy the only record that
//     R's entries were flushed, and recovery would replay them — permanent
//     duplicate rows for a measurement without tags.
//
// Returns the number of files deleted.
func (w *Writer) PurgeFlushed(minUnflushedSeq uint64) (int, error) {
	w.mu.Lock()
	activePath := w.currentPath
	candidates := make([]string, 0, len(w.fileOrder))
	for _, path := range w.fileOrder {
		if path == activePath {
			// The active file is still being appended to; its bound is not final.
			break
		}
		state := w.fileSeqs[path]
		if state == nil || state.hasUntrackedData {
			break
		}
		// maxSeq == 0 means the file holds no tracked data at all (checkpoints
		// only, or just a header), so there is nothing in it to protect.
		if state.maxSeq != 0 && state.maxSeq >= minUnflushedSeq {
			break
		}
		candidates = append(candidates, path)
	}
	w.mu.Unlock()

	deleted := 0
	var firstErr error
	for _, path := range candidates {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			if firstErr == nil {
				firstErr = err
			}
			// Stop rather than skip: continuing past a file that could not be
			// deleted would delete a LATER file, which is the checkpoint-ordering
			// hazard above.
			break
		}
		deleted++
		w.mu.Lock()
		w.forgetFileLocked(path)
		w.mu.Unlock()
	}
	if deleted > 0 {
		w.logger.Info().
			Int("deleted", deleted).
			Uint64("min_unflushed_seq", minUnflushedSeq).
			Msg("Purged WAL files whose data is flushed")
	}
	return deleted, firstErr
}

// PurgeOlderThan deletes inactive WAL files whose modification time is older
// than the given threshold. The active WAL file is never deleted.
// Use this during normal operation to safely purge rotated WAL files whose
// data has been flushed to parquet by the normal buffer flush cycle.
func (w *Writer) PurgeOlderThan(minAge time.Duration) (int, error) {
	w.mu.Lock()
	activePath := w.currentPath
	w.mu.Unlock()

	now := time.Now()
	deleted, err := w.purgeWALFiles(func(path string) bool {
		if path == activePath {
			return false
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			return false
		}
		return now.Sub(info.ModTime()) > minAge
	})
	if deleted > 0 {
		w.logger.Info().Int("deleted", deleted).Dur("min_age", minAge).Msg("Purged old WAL files")
	}
	return deleted, err
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
		"total_entries":       atomic.LoadInt64(&w.TotalEntries),
		"total_bytes":         atomic.LoadInt64(&w.TotalBytes),
		"total_syncs":         atomic.LoadInt64(&w.TotalSyncs),
		"total_rotations":     atomic.LoadInt64(&w.TotalRotations),
		"dropped_entries":     atomic.LoadInt64(&w.DroppedEntries),
		"failed_writes":       atomic.LoadInt64(&w.FailedWrites),
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

// CurrentCheckpointHashes returns flush checkpoints in the active WAL file.
// The writer lock keeps the file stable while the reader scans it.
func (w *Writer) CurrentCheckpointHashes() ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentPath == "" {
		return nil, nil
	}
	return NewReader(w.currentPath, w.logger).ReadCheckpointHashes()
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
