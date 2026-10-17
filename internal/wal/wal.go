package wal

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	// WALCoveredMarker is a tracked entry that also carries the ingest batch's
	// coverage id in a trailer (#1207); see coverage.go for the layout.
	WALCoveredMarker = 0x04
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
	// foreignProof marks a CHECKPOINT entry that covers at least one identity
	// this process did not mint: a row-range identity, or a whole-entry token
	// from an earlier writer instance. Either can describe entries in a file
	// an earlier process wrote and recovery retained, which is absent from
	// w.fileOrder — so the purge walk cannot see it and cannot stop at it.
	// The destination file is therefore pinned while such a file exists; see
	// Writer.PurgeFlushed.
	foreignProof bool
	// covKey and covSeq are the buffer key and coverage sequence of a covered
	// data entry (#1207), recorded against the file it lands in so the purge
	// can keep a file whose batches are not registered yet.
	covKey string
	covSeq uint64
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
	// hasForeignProof is set when a checkpoint covering an identity this
	// process did not mint lands here. While a WAL file an earlier process
	// wrote still exists, this file can be the only record that its entries
	// were flushed, and that file is not in w.fileOrder, so no ordering rule
	// protects it.
	hasForeignProof bool
	// hasUntrackedData is set when an untracked data entry lands here.
	hasUntrackedData bool
	// cov is the coverage of the covered entries written here (#1207 S4c).
	cov coverageSet
}

// WriterConfig holds configuration for WAL writer
type WriterConfig struct {
	WALDir       string        // Directory for WAL files
	SyncMode     SyncMode      // Sync mode: fsync, fdatasync, async
	MaxSizeBytes int64         // Rotate WAL when it reaches this size (default: 100MB)
	MaxAge       time.Duration // Rotate WAL after this duration (default: 1 hour)
	SyncInterval time.Duration // Request a sync at least this often while bytes are unsynced (default: 100ms; 0 = default)
	SyncBytes    int64         // Request a sync once this many bytes are unsynced (default: 1MB; 0 = default)
	BufferSize   int           // Size of async write buffer (default: 10000)
	Logger       zerolog.Logger
}

// ReplicationEntry is a WAL entry as the replication hook receives it: the
// origin stream (#1207) sends it to standby writers.
type ReplicationEntry struct {
	// TimestampUS is the entry timestamp in microseconds since epoch
	TimestampUS uint64

	// Payload is the raw msgpack data
	Payload []byte

	// Coverage identifies the ingest batch the entry belongs to, for entries
	// appended through PrepareCovered (#1207). Nil otherwise.
	Coverage *Coverage
}

// ReplicationHook is called for each WAL entry before it's written locally:
// the origin stream's, on a primary writer with WAL coverage (#1207).
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

	// Group commit (#1246). The writer loop never waits for a sync: it writes,
	// publishes how far it has written, and nudges the syncer goroutine, which
	// runs fdatasync beside it. A sync covers every byte written before it
	// started, so one sync answers every durable entry written by then. The
	// fields below are guarded by syncMu, which is never held across I/O and is
	// always taken AFTER w.mu when both are held. The syncer never takes w.mu:
	// w.mu is held across full-file scans (CurrentCheckpointHashes).
	syncMu sync.Mutex
	// syncTarget is the file the published bytes are in, and syncGen counts
	// rotations: a sync whose file was rotated away while it ran must not
	// advance anything, because the rotation synced and answered that file.
	syncTarget *os.File
	syncGen    uint64
	// writtenBytes counts every byte written since the writer started, across
	// files; syncedBytes is the writtenBytes value the last completed sync
	// covered. Unsynced bytes are writtenBytes - syncedBytes.
	writtenBytes uint64
	syncedBytes  uint64
	// syncWaiters are durable entries (flush checkpoints) written but not yet
	// covered by a sync, in write order. Each is answered exactly once: by the
	// syncer, by a rotation, or by the shutdown drain.
	syncWaiters []syncWaiter
	// syncReq nudges the syncer; capacity 1, so a nudge never blocks and
	// nudges coalesce. requestedBytes is the writtenBytes value of the last
	// byte-threshold nudge; writer loop only, no lock.
	syncReq        chan struct{}
	requestedBytes uint64
	// syncFailed asks the writer loop to rotate: Linux reports a writeback
	// error once, so a later fdatasync on the same file can succeed over pages
	// that were lost, and a checkpoint appended there could prove nothing.
	syncFailed atomic.Bool
	// syncErr is sticky for the current file: once a sync of it fails, every
	// later completion for it answers its waiters with this error, because a
	// later sync can succeed over pages whose loss was already reported.
	// Cleared when the writer rotates to a new file. Guarded by syncMu.
	syncErr error
	// syncIOMu serialises the syncs themselves (the syncer's and the inline
	// ones at rotation and shutdown), and is held until the result is
	// recorded, so a failure is recorded before any other sync of the file
	// can start. Lock order: w.mu, then syncIOMu, then syncMu; the syncer
	// takes only the last two.
	syncIOMu sync.Mutex
	// rotateRetryAt delays the next rotation attempt after one failed, so a
	// full disk is not retried on every entry. Writer loop only.
	rotateRetryAt time.Time
	// tailTorn is set when a failed write left bytes at the tail of the
	// current file that could not be truncated away (#676). Nothing may be
	// appended after them — the reader would read the next entry at a
	// misaligned offset — so every later write first retries the truncate,
	// then a rotation, and is dropped while neither succeeds. Cleared by a
	// successful truncate or rotation. Guarded by w.mu.
	tailTorn bool
	// tornErr is the write error that left the torn tail, so the entries
	// dropped while it stays report that cause (a full disk), not the
	// truncate's own error. Guarded by w.mu.
	tornErr error
	// writeErrLogNano samples the write-failure log: on a full disk every
	// entry fails, and one line per entry buries the signal.
	writeErrLogNano atomic.Int64

	// purgeHint is signalled whenever a purge could now delete something: a
	// rotation finished a file, or the purge floor or a pin moved (#676).
	// Capacity 1 and never a blocking send, so hints coalesce and a sender
	// holding w.mu or pendingMu never waits. See PurgeHint.
	purgeHint chan struct{}
	// diskFull is set by a write or rotation that failed with ENOSPC or
	// EDQUOT and cleared by the next successful entry write (#676). See
	// DiskFullObserved.
	diskFull atomic.Bool
	// diskFullEvents counts every write or rotation that failed with ENOSPC or
	// EDQUOT, so the disk pressure monitor sees each new one even while
	// diskFull stays set (#676).
	diskFullEvents atomic.Uint64
	// diskHint wakes the disk pressure monitor (DiskMonitor) at once: a
	// rotation finished, or a write found the disk full. Capacity 1, never a
	// blocking send.
	diskHint chan struct{}
	// enqueuedBytes counts the bytes of every entry admitted to the write
	// queue, and processedBytes those the writer loop has finished with
	// (written or lost). The disk pressure gate charges enqueued bytes against
	// the free space of its last sample, so it is exact between samples, and
	// enqueued - processed is what a sample's free space does not show yet.
	enqueuedBytes  atomic.Uint64
	processedBytes atomic.Uint64
	// recoveredLogNano samples the log of a failed write the retry on a new
	// file recovered.
	recoveredLogNano atomic.Int64
	// lostCov records the covered entries (#1207) lost to a failed write, by
	// buffer key and coverage sequence, so a flush that later fails can tell
	// recovery will never replay them (CoverageLost). Guarded by lostMu, which
	// is taken after w.mu when both are held.
	lostMu  sync.Mutex
	lostCov coverageSet
	// purgedTotal and purgeLogNano sample the PurgeFlushed log: with the
	// purge running on every hint, one Info line per pass is one per second.
	purgedTotal  atomic.Int64
	purgeLogNano atomic.Int64

	// Async write buffer
	entryChan chan walEntry
	done      chan struct{}
	wg        sync.WaitGroup

	// Replication hook for streaming entries to readers. Atomic, not under
	// w.mu: the writer loop holds w.mu across fsync and rotation, and a covered
	// append fires the hook while its caller holds an ingest shard lock (#1207).
	replicationHook atomic.Pointer[ReplicationHook]
	trackedInstance uint64

	// fileSeqs and fileOrder track what this process wrote to each WAL file, so
	// the purge can decide by what has been flushed rather than by mtime
	// (#1009). Both are guarded by w.mu. fileOrder is rotation order, which is
	// the ordering the purge needs — filename timestamps and mtimes both move
	// backwards under a clock step, rotation order cannot. Files created by a
	// PREVIOUS process appear in neither, which is deliberate: nothing here
	// knows whether their data was flushed, so only recovery may delete them.
	fileSeqs  map[string]*fileSeqState
	fileOrder []string
	// created holds every WAL file this process rotated in, including ones
	// that rotated out empty and so never reached fileSeqs. A *.wal file in
	// the directory that is not in it was written by an earlier process (or
	// put there by an operator), and only recovery may delete it. Guarded by
	// w.mu.
	created map[string]struct{}
	// coverageHold is the source of the registration hold (#1207 S4c); adopted
	// holds the earlier-process files recovery retained for it. Guarded by w.mu.
	coverageHold func() CoverageHold
	adopted      map[string]RetainedFile
	// pinnedOn is the earlier-process file the last purge pass found keeping
	// a foreign-proof pin, for logging only on change. Guarded by w.mu.
	pinnedOn        string
	trackedSequence uint64
	// pendingSeqs holds the tracked sequences this process has appended that no
	// durable flush checkpoint covers yet. Its minimum is the floor that makes
	// WAL purging clock-free (#1009).
	//
	// Keyed by sequence, not by identity token: the token encodes the
	// sequence (see appendTrackedEntry), so a release can recover it without
	// a second map. Guarded by pendingMu rather than w.mu, deliberately — the
	// per-record append path must stay off the writer lock, which is held
	// across file writes, rotation (which syncs the old file), and the
	// full-file scan in CurrentCheckpointHashes.
	pendingMu   sync.Mutex
	pendingSeqs map[uint64]struct{}
	closed      bool

	// Metrics (atomic for lock-free reads)
	TotalEntries   int64
	TotalBytes     int64
	TotalSyncs     int64
	FailedSyncs    int64 // Syncs that returned an error
	TotalRotations int64
	DroppedEntries int64 // Entries dropped due to full buffer
	FailedWrites   int64 // Entries the writer loop could not write (lost)
	DiskFullWrites int64 // FailedWrites whose error was ENOSPC or EDQUOT
	// RecoveredWrites counts failed entry writes the retry on a new WAL file
	// recovered: nothing was lost, but the WAL file or its device failed.
	RecoveredWrites int64

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
	if cfg.SyncBytes < 0 {
		return nil, fmt.Errorf("wal.sync_bytes must not be negative, got %d", cfg.SyncBytes)
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
		entryChan:       make(chan walEntry, cfg.BufferSize),
		syncReq:         make(chan struct{}, 1),
		purgeHint:       make(chan struct{}, 1),
		diskHint:        make(chan struct{}, 1),
		fileSeqs:        make(map[string]*fileSeqState),
		created:         make(map[string]struct{}),
		pendingSeqs:     make(map[uint64]struct{}),
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
		Msg("WAL writer initialized (async append, group commit)")

	return w, nil
}

// syncWaiter is a durable entry waiting for a sync that covers it: target is
// writtenBytes just after the entry was written.
type syncWaiter struct {
	target uint64
	done   chan error
}

// writerLoop is the background goroutine that writes entries to disk. It owns
// the syncer goroutine for its lifetime, so a test that pauses and restarts the
// loop restarts the syncer with it.
func (w *Writer) writerLoop() {
	defer w.wg.Done()

	stopSyncer := make(chan struct{})
	syncerDone := make(chan struct{})
	go w.syncerLoop(stopSyncer, syncerDone)

	syncTicker := time.NewTicker(w.config.SyncInterval)
	defer syncTicker.Stop()

	for {
		select {
		case entry := <-w.entryChan:
			w.processEntry(entry)
			w.processedBytes.Add(uint64(len(entry.data)))
			w.rotateIfSyncFailed()

		case <-syncTicker.C:
			// Bounds how long written bytes stay unsynced when the byte
			// threshold is not reached.
			if w.config.SyncMode != SyncModeAsync && w.unsyncedBytes() > 0 {
				w.nudgeSyncer()
			}
			w.rotateIfSyncFailed()

		case <-w.done:
			// Drain remaining entries before shutdown. The syncer keeps
			// running meanwhile, so checkpoints written here are still
			// answered by it.
			for {
				select {
				case entry := <-w.entryChan:
					w.processEntry(entry)
					w.processedBytes.Add(uint64(len(entry.data)))
				default:
					// Stop the syncer first, so the final sync below is the only
					// sync left, then answer every waiter still pending.
					close(stopSyncer)
					<-syncerDone
					w.mu.Lock()
					w.syncCurrentLocked()
					w.mu.Unlock()
					return
				}
			}
		}
	}
}

// syncerLoop runs every sync the writer loop requests, beside it (#1246). It
// never takes w.mu.
func (w *Writer) syncerLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-w.syncReq:
		case <-stop:
			return
		}
		w.syncIOMu.Lock()
		w.syncMu.Lock()
		f, gen, target := w.syncTarget, w.syncGen, w.writtenBytes
		forceData := len(w.syncWaiters) > 0
		skip := f == nil || target <= w.syncedBytes ||
			(!forceData && w.config.SyncMode == SyncModeAsync)
		w.syncMu.Unlock()
		if skip {
			w.syncIOMu.Unlock()
			continue
		}
		// target was read after the writes it counts had returned, and a sync
		// covers every write completed before it starts, so a successful sync
		// covers target.
		err := w.syncFile(f, forceData)
		w.syncMu.Lock()
		if gen == w.syncGen {
			w.completeSyncLocked(target, err, true)
		}
		// Otherwise the file was rotated away before this sync ran: the
		// rotation synced it, answered its waiters and closed it, so this
		// result (often os.ErrClosed) describes nothing still pending.
		w.syncMu.Unlock()
		w.syncIOMu.Unlock()
	}
}

// completeSyncLocked records a sync covering target and answers the waiters it
// covers. Caller holds syncMu. A failed sync still advances syncedBytes: the
// waiters it covered get the error, and retrying cannot recover pages the
// kernel already reported lost. The error then sticks to the file, and
// requestRotate asks the writer loop to move to a new one (the rotation path
// passes false: it is already rotating).
func (w *Writer) completeSyncLocked(target uint64, err error, requestRotate bool) {
	if target > w.syncedBytes {
		w.syncedBytes = target
	}
	if err != nil {
		atomic.AddInt64(&w.FailedSyncs, 1)
		metrics.Get().IncWALFailedSyncs()
		if w.syncErr == nil {
			w.syncErr = err
		}
		if requestRotate {
			w.syncFailed.Store(true)
		}
		w.logger.Error().Err(err).Msg("WAL sync failed; flush checkpoints in this WAL file fail until it is rotated")
	}
	atomic.AddInt64(&w.TotalSyncs, 1)
	answer := w.syncErr
	n := 0
	for _, wt := range w.syncWaiters {
		if wt.target > target {
			break
		}
		wt.done <- answer
		close(wt.done)
		n++
	}
	w.syncWaiters = w.syncWaiters[n:]
}

// syncCurrentLocked syncs the current file inline and answers every waiter.
// Rotation and shutdown use it; caller holds w.mu. It waits for a sync already
// running on the syncer, so their results cannot interleave.
func (w *Writer) syncCurrentLocked() {
	w.syncIOMu.Lock()
	defer w.syncIOMu.Unlock()
	w.syncMu.Lock()
	f, target := w.syncTarget, w.writtenBytes
	forceData := len(w.syncWaiters) > 0
	skip := f == nil || (target <= w.syncedBytes && !forceData) ||
		(!forceData && w.config.SyncMode == SyncModeAsync)
	w.syncMu.Unlock()
	if skip {
		return
	}
	err := w.syncFile(f, forceData)
	w.syncMu.Lock()
	w.completeSyncLocked(target, err, false)
	w.syncMu.Unlock()
}

// unsyncedBytes reports written bytes no completed sync covers yet.
func (w *Writer) unsyncedBytes() uint64 {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	return w.writtenBytes - w.syncedBytes
}

// nudgeSyncer requests a sync without blocking; nudges coalesce.
func (w *Writer) nudgeSyncer() {
	select {
	case w.syncReq <- struct{}{}:
	default:
	}
}

// rotateIfSyncFailed rotates after a failed sync, on the writer loop. A failed
// rotation (a full disk, a permission change) is retried at most once a second
// rather than on every entry.
func (w *Writer) rotateIfSyncFailed() {
	if !w.syncFailed.Load() || time.Now().Before(w.rotateRetryAt) {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotate(); err != nil {
		w.rotateRetryAt = time.Now().Add(time.Second)
		w.logger.Error().Err(err).Msg("Failed to rotate WAL after a sync failure; will retry")
		return
	}
	w.syncFailed.Store(false)
}

func (w *Writer) processEntry(entry walEntry) {
	deferred, err := w.writeEntry(entry)
	if deferred {
		return // answered by the sync that covers it
	}
	if entry.done != nil {
		entry.done <- err
		close(entry.done)
	}
}

// writeEntry writes a single entry to the WAL file (called from writerLoop).
// deferred reports that the entry's reply was handed to the syncer: a durable
// entry is answered once a sync covering it completes, not here.
func (w *Writer) writeEntry(entry walEntry) (deferred bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entry.rotate {
		// An explicit rotation is attempted whatever the backoff: it is one
		// command, not a per-entry retry, and recovery waits on it. Its
		// caller is answered with the error either way.
		return false, w.rotate()
	}

	n, err := w.appendLocked(entry.data)
	if err != nil {
		firstErr := err
		n, err = w.retryFailedWriteLocked(entry, err)
		if err == nil {
			w.noteRecoveredWrite(firstErr)
		}
	}
	if err != nil {
		// The entry is lost. Its caller is answered with the error by
		// processEntry (a checkpoint's markFlushed and RotateContext wait on
		// that answer).
		atomic.AddInt64(&w.FailedWrites, 1)
		metrics.Get().IncWALFailedWrites()
		if isDiskFull(err) {
			atomic.AddInt64(&w.DiskFullWrites, 1)
			metrics.Get().IncWALDiskFullWrites()
		}
		w.forgetLostEntryLocked(entry)
		w.logWriteFailure(entry, err)
		return false, err
	}
	w.diskFull.Store(false)

	// Record what this entry contributed to the file it actually landed in.
	// This must be here and not before the write: the failure path above
	// can rotate and retry, so an entry can be durable in a DIFFERENT file
	// than the one that was current when it was dequeued. Attributing it to
	// the old file would let the new file's sequence bound fall below the
	// unflushed floor while this entry is still unflushed, and the purge
	// would then delete the only copy (#1009).
	w.noteWrittenLocked(entry)

	bytesWritten := int64(n)
	w.currentSize += bytesWritten

	// Update metrics
	atomic.AddInt64(&w.TotalEntries, 1)
	atomic.AddInt64(&w.TotalBytes, bytesWritten)

	// Publish the write, and hand a durable entry's reply to the syncer. The
	// writer does not wait for the sync (#1246): a durable entry is answered
	// once a sync that started after this write completes, and the entries
	// behind it are written meanwhile.
	w.syncMu.Lock()
	w.writtenBytes += uint64(bytesWritten)
	if entry.durable && entry.done != nil {
		w.syncWaiters = append(w.syncWaiters, syncWaiter{target: w.writtenBytes, done: entry.done})
		deferred = true
	}
	written := w.writtenBytes
	w.syncMu.Unlock()
	if entry.durable {
		w.nudgeSyncer()
	} else if w.config.SyncMode != SyncModeAsync && written-w.requestedBytes >= uint64(w.config.SyncBytes) {
		w.requestedBytes = written
		w.nudgeSyncer()
	}

	// Check if rotation needed. A rotation syncs the old file and answers
	// every waiter, including this entry's. A failed one (a full disk) is
	// retried at most once a second, not after every entry: each attempt
	// creates a file and removes it again.
	age := time.Since(w.startTime)
	if (w.currentSize >= w.config.MaxSizeBytes || age >= w.config.MaxAge) && !time.Now().Before(w.rotateRetryAt) {
		if err := w.rotate(); err != nil {
			w.rotateRetryAt = time.Now().Add(rotateRetryBackoff)
			w.logger.Error().Err(err).Msg("Failed to rotate WAL; will retry")
		}
	}
	return deferred, nil
}

// forgetLostEntryLocked releases what a lost entry held. Its sequence leaves
// the purge floor at once: the entry is in no WAL file, so recovery can never
// checkpoint it, and if its buffer's flush fails nothing else releases it —
// the floor would stop every later purge until a restart (#676). Releasing
// it is floor-safe for the same reason: the floor protects files, and no
// file holds this entry. A covered entry's coverage is recorded as lost, so a
// failed flush settles its batch instead of waiting for a replay that cannot
// come (see CoverageLost). Caller holds w.mu.
func (w *Writer) forgetLostEntryLocked(entry walEntry) {
	if entry.seq != 0 {
		w.pendingMu.Lock()
		_, pending := w.pendingSeqs[entry.seq]
		delete(w.pendingSeqs, entry.seq)
		w.pendingMu.Unlock()
		if pending {
			w.signalPurge()
		}
	}
	if entry.covSeq != 0 {
		w.lostMu.Lock()
		if w.lostCov == nil {
			w.lostCov = make(coverageSet)
		}
		w.lostCov.add(w.trackedInstance, entry.covKey, entry.covSeq)
		w.lostMu.Unlock()
	}
}

// CoverageLost reports whether a covered entry of this writer's batch cseq
// under buffer key was lost to a failed write (#676): the WAL holds no
// complete copy of the batch, so recovery will never replay it.
func (w *Writer) CoverageLost(key string, cseq uint64) bool {
	w.lostMu.Lock()
	defer w.lostMu.Unlock()
	return w.lostCov.contains(w.trackedInstance, key, cseq)
}

// rotateRetryBackoff is how long a failed rotation waits before the writer
// loop tries again.
const rotateRetryBackoff = time.Second

// writeFileFn and truncateFileFn perform the entry write and the tail
// truncate. Variables so tests can fail a write part-way (#676).
var (
	writeFileFn    = func(f *os.File, b []byte) (int, error) { return f.Write(b) }
	truncateFileFn = func(f *os.File, size int64) error { return f.Truncate(size) }
)

// isDiskFull reports a write error that means the volume (or the user's
// quota) has no room left.
func isDiskFull(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}

// appendLocked writes data at the tail of the current file. A write that
// fails part-way leaves a torn entry there, and the file is O_APPEND, so the
// next entry would land after it and the reader would read it at a
// misaligned offset — losing it and everything behind it in the file. So the
// tail is cut back to w.currentSize (the end of the last whole entry; it
// advances only when an entry write succeeds) before anything else is
// appended. If that truncate fails, tailTorn keeps any later write off the
// file until a truncate or a rotation succeeds. Caller holds w.mu.
func (w *Writer) appendLocked(data []byte) (int, error) {
	if w.tailTorn {
		if err := truncateFileFn(w.currentFile, w.currentSize); err != nil {
			// Report the cause of the torn tail (a full disk), which is why
			// this entry cannot be written, not the truncate's error.
			return 0, fmt.Errorf("WAL file ends in a torn write that cannot be truncated (%v): %w", err, w.tornErr)
		}
		w.tailTorn, w.tornErr = false, nil
	}
	n, err := writeFileFn(w.currentFile, data)
	if err == nil {
		return n, nil
	}
	if isDiskFull(err) {
		w.noteDiskFull()
	}
	if n > 0 {
		if terr := truncateFileFn(w.currentFile, w.currentSize); terr != nil {
			w.tailTorn, w.tornErr = true, err
			return 0, fmt.Errorf("%w (and truncating the torn write failed: %v)", err, terr)
		}
	}
	return 0, err
}

// retryFailedWriteLocked decides what a failed entry write does next. A file
// whose tail could not be restored must be left; so must a handle that fails
// for a reason other than a full disk (a bad descriptor, an I/O error), and
// the entry is retried once on the new file. A full disk is not escaped by a
// new file on the same volume, so with the tail restored the entry is simply
// lost: rotating there created and removed a file per entry (#676). A
// rotation for a failed write, successful or not, is not tried again for
// rotateRetryBackoff: an error that follows the new file too (a failing
// device) would otherwise still rotate once per entry. Caller holds w.mu.
func (w *Writer) retryFailedWriteLocked(entry walEntry, writeErr error) (int, error) {
	if !w.tailTorn && isDiskFull(writeErr) {
		return 0, writeErr
	}
	if time.Now().Before(w.rotateRetryAt) {
		return 0, writeErr
	}
	w.rotateRetryAt = time.Now().Add(rotateRetryBackoff)
	if err := w.rotate(); err != nil {
		return 0, fmt.Errorf("%w (rotating to a new WAL file also failed: %v)", writeErr, err)
	}
	return w.appendLocked(entry.data)
}

// logWriteFailure logs a lost entry, at most once a second.
func (w *Writer) logWriteFailure(entry walEntry, err error) {
	now := time.Now().UnixNano()
	last := w.writeErrLogNano.Load()
	if now-last < int64(time.Second) || !w.writeErrLogNano.CompareAndSwap(last, now) {
		return
	}
	msg := "WAL data entry could not be written and is lost from the WAL: its rows reach storage only if the flush of the ingest buffer holding them succeeds; a crash or a failed flush before that loses them"
	if entry.durable {
		msg = "WAL flush checkpoint could not be written: its data is already in storage, but recovery may replay those entries again (a duplicate, not a loss)"
	}
	w.logger.Error().Err(err).
		Bool("disk_full", isDiskFull(err)).
		Int64("failed_writes", atomic.LoadInt64(&w.FailedWrites)).
		Msg(msg + " (sampled: at most one line per second, see failed_writes)")
}

// noteRecoveredWrite counts and logs (at most once a second) a failed entry
// write the retry on a new WAL file recovered.
func (w *Writer) noteRecoveredWrite(err error) {
	atomic.AddInt64(&w.RecoveredWrites, 1)
	metrics.Get().IncWALRecoveredWrites()
	now := time.Now().UnixNano()
	last := w.recoveredLogNano.Load()
	if now-last < int64(time.Second) || !w.recoveredLogNano.CompareAndSwap(last, now) {
		return
	}
	w.logger.Warn().Err(err).
		Int64("recovered_writes", atomic.LoadInt64(&w.RecoveredWrites)).
		Msg("WAL write failed and succeeded on a new WAL file; nothing was lost, but the WAL file or its device is failing (sampled: at most one line per second)")
}

// PurgeHint is signalled when a purge could now delete something: a rotation
// finished a file, or the purge floor or a pin moved (#676). Hints coalesce
// (capacity 1); the receiver runs PurgeFlushed and ReleaseRetained, rate
// limited — the WAL directory otherwise grows until the periodic pass.
func (w *Writer) PurgeHint() <-chan struct{} {
	return w.purgeHint
}

// signalPurge never blocks, so it is safe under w.mu and pendingMu.
func (w *Writer) signalPurge() {
	select {
	case w.purgeHint <- struct{}{}:
	default:
	}
}

// ActiveFileBytes is the size of the WAL file being written, header
// included (#676).
func (w *Writer) ActiveFileBytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentSize
}

// noteDiskFull records a write or rotation that failed because the volume or
// the quota was full, and wakes the disk pressure monitor.
func (w *Writer) noteDiskFull() {
	w.diskFull.Store(true)
	w.diskFullEvents.Add(1)
	w.signalDisk()
}

// signalDisk never blocks, so it is safe under w.mu.
func (w *Writer) signalDisk() {
	select {
	case w.diskHint <- struct{}{}:
	default:
	}
}

// DiskFullObserved reports that the most recent entry write or rotation
// failed because the volume or the quota was full (ENOSPC, EDQUOT), and no
// entry has been written since (#676).
func (w *Writer) DiskFullObserved() bool {
	return w.diskFull.Load()
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
	return w.RotateContext(context.Background())
}

// RotateContext waits for queue capacity instead of dropping a maintenance
// command when writers fill the queue. The command uses the same FIFO as data,
// so all preceding accepted entries are processed before the rotation.
//
// Cancellation bounds both admission and completion waits. Once admitted, the
// command remains owned by the writer loop and can complete after cancellation;
// callers must not infer a rotation boundary from an error. The buffered reply
// lets the writer finish even after its caller has stopped waiting.
func (w *Writer) RotateContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-w.done:
		return errors.New("WAL writer is closed")
	default:
	}
	done := make(chan error, 1)
	// Do not hold w.mu while waiting for capacity: the consumer needs it to
	// write the entries that free that capacity. Unlike a data checkpoint, an
	// unprocessed rotation racing the final shutdown drain carries no data or
	// durability proof; the shutdown arm below reports failure in that case.
	select {
	case w.entryChan <- walEntry{rotate: true, durable: true, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return errors.New("WAL writer is closed")
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return errors.New("WAL writer is closed")
	}
}

// walNow names WAL files; a variable so a test can freeze it.
var walNow = time.Now

// rotate creates a new WAL file.
// The new file is opened and its header written BEFORE the old file is closed,
// so a failure to create or initialize the new file leaves the old file intact
// (no nil w.currentFile that would panic on the next write).
func (w *Writer) rotate() error {
	// Open new file first — if this fails, old file is still valid. O_EXCL:
	// two rotations in one clock tick (or a second writer on the directory
	// just after a restart) would otherwise append to the SAME file, and a
	// file another process wrote would be taken for this one's. On a clash
	// the name moves a microsecond later, which keeps name order = rotation
	// order.
	now := walNow().UTC()
	var newPath, filename string
	var newFile *os.File
	var err error
	for attempt := 0; ; attempt++ {
		filename = fmt.Sprintf("arc-%s.wal", now.Format("20060102_150405.000000000"))
		newPath = filepath.Join(w.config.WALDir, filename)
		newFile, err = os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0600)
		if err == nil {
			break
		}
		if !os.IsExist(err) || attempt >= 1000 {
			if isDiskFull(err) {
				w.noteDiskFull()
			}
			return fmt.Errorf("failed to create WAL file: %w", err)
		}
		now = now.Add(time.Microsecond)
	}

	// Write WAL header to new file
	var header [WALFileHeaderSize]byte
	copy(header[0:4], WALMagic)
	binary.BigEndian.PutUint16(header[4:6], WALVersion)
	header[6] = WALChecksumCRC32

	n, err := writeFileFn(newFile, header[:])
	if err != nil {
		if isDiskFull(err) {
			w.noteDiskFull()
		}
		newFile.Close()
		// Remove the headerless file: nothing else will, and a *.wal this
		// writer did not record pins the purge as an earlier process file.
		_ = os.Remove(newPath)
		return fmt.Errorf("failed to write WAL header: %w", err)
	}

	// New file is ready — now close the old one. Sync it first and answer
	// every waiting durable entry: all of them were written to it. This runs
	// whatever the sync mode when a durable entry waits, so an async-mode
	// checkpoint is still synced before it is answered.
	oldFile := w.currentFile
	if oldFile != nil {
		w.syncCurrentLocked()
	}

	// Swap to the new file BEFORE closing the old one. The generation moves
	// under syncMu, so a sync the syncer starts on the old handle afterwards
	// (and finds closed) is recognised as stale instead of being counted as a
	// failure of the new file. The sticky error belonged to the old file.
	w.currentFile = newFile
	w.currentPath = newPath
	w.created[newPath] = struct{}{}
	w.currentSize = int64(n)
	w.startTime = time.Now()
	w.syncMu.Lock()
	w.syncTarget = newFile
	w.syncGen++
	w.syncErr = nil
	w.writtenBytes += uint64(n)
	w.syncedBytes = w.writtenBytes
	w.syncMu.Unlock()
	if oldFile != nil {
		oldFile.Close()
	}
	// The new file holds nothing but its header: whatever torn write the old
	// one ended in stays behind with it, where the reader stops at it.
	w.tailTorn, w.tornErr = false, nil
	atomic.AddInt64(&w.TotalRotations, 1)

	w.logger.Info().Str("file", filename).Msg("WAL rotated")
	// The file just rotated out may already be purgeable (#676).
	if oldFile != nil {
		w.signalPurge()
	}
	w.signalDisk()
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
			// The chunks that already landed are pending in the floor, and
			// their tokens go out of scope with this slice — the caller gets
			// nil and cannot release them. Unreleased, each pins the floor for
			// the life of the process, and because PurgeFlushed stops at the
			// first retained file the WAL never reclaims anything again (#676).
			w.releasePending(hashes)
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
			// The chunks that already landed are pending in the floor, and
			// their tokens go out of scope with this slice — the caller gets
			// nil and cannot release them. Unreleased, each pins the floor for
			// the life of the process, and because PurgeFlushed stops at the
			// first retained file the WAL never reclaims anything again (#676).
			w.releasePending(hashes)
			return nil, err
		}
		hashes = append(hashes, token)
	}
	return hashes, nil
}

// MarkFlushed appends a checkpoint after the corresponding data entries have
// reached durable storage. A failed checkpoint is safe: it can only cause a
// replay duplicate, never data loss.
//
// Admission is non-blocking — a full queue returns ErrWALDropped — because this
// is the production ingest flush path (ArrowBuffer.markWALFlushed). Completion
// already blocks on the writer loop's reply. Use MarkFlushedContext from
// recovery, which needs the opposite trade.
func (w *Writer) MarkFlushed(hashes []string) error {
	return w.markFlushed(context.Background(), hashes, false, true)
}

// MarkFlushedRetained writes the same durable checkpoint as MarkFlushed — so
// recovery does not replay the entries — but keeps their sequences pending in
// the purge floor. A covered flush (#1207) holds its WAL entries until the
// files they became are registered; the caller releases them with
// ReleaseRegistered once that happens.
func (w *Writer) MarkFlushedRetained(hashes []string) error {
	return w.markFlushed(context.Background(), hashes, false, false)
}

// ReleaseRegistered drops identities checkpointed with MarkFlushedRetained
// from the purge floor once their files are registered (#1207). Also the
// release for a checkpoint that failed, exactly as ForgetTracked is today.
func (w *Writer) ReleaseRegistered(hashes []string) {
	if len(hashes) == 0 {
		return
	}
	w.releasePending(hashes)
}

// MarkFlushedContext is MarkFlushed with a cancellable, blocking admission wait
// instead of the drop-on-full enqueue.
//
// Recovery's parent finalization runs under the same sustained queue pressure
// that made the forced maintenance rotation unreliable (#1009) — same pass, same
// queue — but was left on the dropping path when rotation moved off it. A
// dropped checkpoint retains the replayed file for another pass, which is safe
// but is one of the ways a file stays retained long enough to matter.
//
// ctx bounds ADMISSION only. Once an entry is admitted the checkpoint will be
// written, so the reply is waited for unconditionally — see markFlushed. On a
// cancelled admission or a closed writer this returns an error, so the caller
// retains the WAL and replays later: at-least-once, never at-most-once.
func (w *Writer) MarkFlushedContext(ctx context.Context, hashes []string) error {
	return w.markFlushed(ctx, hashes, true, true)
}

func (w *Writer) markFlushed(ctx context.Context, hashes []string, blocking, release bool) error {
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
		entry := walEntry{
			data:         entryData,
			durable:      true,
			done:         done,
			foreignProof: w.coversForeignIdentity(hashes[start:end]),
		}
		if blocking {
			if err := w.enqueueEntryBlocking(ctx, entry); err != nil {
				return err
			}
			// Admission was the only unbounded step and it is now behind us,
			// so the reply is NOT waited on with ctx.
			//
			// Abandoning it here would be a WAL-growth bug, not a timeout: the
			// writer loop still writes this checkpoint durably, but the
			// releasePending below would be skipped, so the parent's sequence
			// stays in pendingSeqs, MinUnflushedSequence holds the purge floor
			// down, and PurgeFlushed stops at the first retained file — the WAL
			// never reclaims anything again for the life of the process. That
			// is the hazard ForgetTracked's own comment describes (#676), and
			// it does NOT self-heal: a later pass SKIPS the entry because its
			// checkpoint is durable, so CheckpointRecovered is never called for
			// that parent a second time.
			select {
			case err := <-done:
				if err != nil {
					return fmt.Errorf("failed to persist WAL flush checkpoint: %w", err)
				}
			case <-w.done:
				// Shutdown. The release is skipped, but pendingSeqs does not
				// outlive the process and the caller retains its file.
				return errors.New("WAL writer is closed")
			}
		} else {
			if err := w.tryEnqueueEntry(entry); err != nil {
				return err
			}
			if err := <-done; err != nil {
				return fmt.Errorf("failed to persist WAL flush checkpoint: %w", err)
			}
		}
		if release {
			w.releasePending(hashes[start:end])
		}
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
	// Allocating the sequence and publishing it as pending must be one step.
	// Everything after this — the Sprintf, the CRC, and above all the
	// synchronous replication hook below — sits between them otherwise, and a
	// sequence counted in trackedSequence but absent from pendingSeqs is a
	// sequence the floor does not protect. With a replication hook set (the
	// origin stream, #1207) that window spans its hook call.
	w.pendingMu.Lock()
	seq := atomic.AddUint64(&w.trackedSequence, 1)
	w.pendingSeqs[seq] = struct{}{}
	w.pendingMu.Unlock()
	token := fmt.Sprintf("%016x%016x", w.trackedInstance, seq)
	trackedPayload := make([]byte, walTrackedHeaderSize+len(logicalPayload))
	trackedPayload[0] = WALTrackedMarker
	binary.BigEndian.PutUint64(trackedPayload[1:9], w.trackedInstance)
	binary.BigEndian.PutUint64(trackedPayload[9:17], seq)
	copy(trackedPayload[17:], logicalPayload)
	checksum := crc32.ChecksumIEEE(trackedPayload)
	timestampUS := uint64(time.Now().UnixMicro())
	w.replicate(&ReplicationEntry{TimestampUS: timestampUS, Payload: logicalPayload})
	entryData := make([]byte, WALEntryHeaderSize+len(trackedPayload))
	binary.BigEndian.PutUint32(entryData[0:4], uint32(len(trackedPayload)))
	binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
	binary.BigEndian.PutUint32(entryData[12:16], checksum)
	copy(entryData[WALEntryHeaderSize:], trackedPayload)
	// Carry the sequence so the writer loop can record it against the file the
	// write actually lands in (#1009).
	if err := w.tryEnqueueEntry(walEntry{data: entryData, seq: seq}); err != nil {
		w.pendingMu.Lock()
		delete(w.pendingSeqs, seq)
		w.pendingMu.Unlock()
		w.signalPurge() // the floor may have moved (#676)
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
	if w.replicationHook.Load() != nil {
		// Build envelope for replication (unavoidable copy for hook consumers)
		repPayload := make([]byte, totalPayloadLen)
		copy(repPayload, envHeader[:envelopeHeaderLen])
		copy(repPayload[envelopeHeaderLen:], payload)
		w.replicate(&ReplicationEntry{TimestampUS: timestampUS, Payload: repPayload})
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

// enqueueEntryBlocking waits for queue capacity instead of dropping the entry,
// using the same FIFO as data so every preceding accepted entry is written
// first.
//
// w.mu is taken only to read w.closed and is released BEFORE the wait: the
// writer loop needs it to write the entries that free the capacity this wait is
// for, which is the constraint RotateContext documents. A Close racing the wait
// is caught by the w.done arm, and that error retains the caller's file rather
// than claiming a durability proof that was never written.
func (w *Writer) enqueueEntryBlocking(ctx context.Context, entry walEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()
	if closed {
		return errors.New("WAL writer is closed")
	}
	select {
	case w.entryChan <- entry:
		w.enqueuedBytes.Add(uint64(len(entry.data)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return errors.New("WAL writer is closed")
	}
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
		// Only an admitted entry reaches the disk: a dropped one must not
		// count against the disk pressure budget (#676).
		w.enqueuedBytes.Add(uint64(len(entry.data)))
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

// AppendRawTracked writes a raw msgpack payload with a process-local sequence
// identity and returns the identities the eventual flush must checkpoint. It
// is used by replication followers so their local WAL participates in the
// same sequence-floor purge as primary ingest.
func (w *Writer) AppendRawTracked(payload []byte) ([]string, error) {
	chunks := [][]byte{payload}
	if len(payload) > MaxWALPayloadSize {
		var err error
		chunks, err = splitOversizedPayload(payload)
		if err != nil {
			return nil, oversizedPayloadError(err)
		}
	}

	hashes := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		if len(chunk) > MaxWALPayloadSize-walTrackedHeaderSize {
			w.releasePending(hashes)
			return nil, oversizedPayloadError(fmt.Errorf("tracked size %d exceeds limit %d", len(chunk)+walTrackedHeaderSize, MaxWALPayloadSize))
		}
		hash, err := w.appendTrackedEntry(chunk)
		if err != nil {
			w.releasePending(hashes)
			return nil, err
		}
		hashes = append(hashes, hash)
	}
	return hashes, nil
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
	w.replicate(&ReplicationEntry{TimestampUS: timestampUS, Payload: payload})

	// Build complete entry: header + payload
	entryData := make([]byte, WALEntryHeaderSize+len(payload))
	binary.BigEndian.PutUint32(entryData[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
	binary.BigEndian.PutUint32(entryData[12:16], checksum)
	copy(entryData[WALEntryHeaderSize:], payload)

	return w.tryEnqueue(entryData)
}

// sync syncs the WAL file to disk based on sync mode
// syncFile syncs f according to the sync mode. forceData syncs file data even
// in async mode: a durable entry is never answered without a sync.
func (w *Writer) syncFile(f *os.File, forceData bool) error {
	switch {
	case w.config.SyncMode == SyncModeFsync:
		// Full sync: data + metadata
		return syncFileFn(f, true)
	case w.config.SyncMode == SyncModeFdatasync || forceData:
		// Data sync only. Real fdatasync(2) on Linux; a full Sync elsewhere,
		// where Go does not expose it (see datasync_other.go).
		return syncFileFn(f, false)
	default:
		// Async: no explicit sync, rely on the OS buffer cache.
		return nil
	}
}

// syncFileFn performs the sync; full selects fsync over fdatasync. A variable
// so tests can hold a sync in flight.
var syncFileFn = func(f *os.File, full bool) error {
	if full {
		return f.Sync()
	}
	return dataSync(f)
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
	if entry.foreignProof {
		state.hasForeignProof = true
		return
	}
	if entry.untrackedData {
		state.hasUntrackedData = true
		return
	}
	if entry.seq > state.maxSeq {
		state.maxSeq = entry.seq
	}
	if entry.covSeq != 0 {
		if state.cov == nil {
			state.cov = make(coverageSet)
		}
		state.cov.add(w.trackedInstance, entry.covKey, entry.covSeq)
	}
}

// listWALFilesFn lists the *.wal files in dir. A variable so tests can make
// the listing fail.
var listWALFilesFn = listWALFiles

// listWALFiles lists with ReadDir rather than filepath.Glob, which reports no
// I/O error: an unreadable directory would read as an empty one.
func listWALFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.Type().IsRegular() && strings.HasSuffix(e.Name(), ".wal") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out, nil
}

// earlierFileLocked reports whether a foreign-proof file must stay pinned: a
// *.wal file in the directory that this process did not create still exists
// (returned), or the directory could not be listed — the pin fails closed.
// Caller holds w.mu.
func (w *Writer) earlierFileLocked(listed []string, listErr error) (string, bool) {
	if listErr != nil {
		w.logger.Warn().Err(listErr).Msg("Could not list the WAL directory; WAL files holding flush proof for earlier process files stay pinned")
		return "", true
	}
	for _, path := range listed {
		if _, ok := w.created[path]; !ok {
			return path, true
		}
	}
	if w.pinnedOn != "" {
		w.logger.Info().Str("file", filepath.Base(w.pinnedOn)).
			Msg("Earlier process WAL file is gone; WAL purge no longer pinned by its flush proof")
		w.pinnedOn = ""
	}
	return "", false
}

// forgetFileLocked drops a deleted file from the purge bookkeeping. Caller
// holds w.mu.
func (w *Writer) forgetFileLocked(path string) {
	delete(w.created, path)
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

// Quarantined tells the writer that recovery moved path to quarantinedAs
// after repeated replay failures (#1225). If this process wrote it, entries in
// it may still be waiting for a flush: recovery can no longer replay them
// from it, so keeping them pending would hold the purge floor at this file for
// the life of the process, and no WAL file would be reclaimed again (#676).
// Their sequences are released. Returns how many were.
//
// Every pending entry of the file is released, including any this pass
// replayed into a buffer: if that flush fails too, nothing can replay it
// again. Nothing else owns them: only the periodic recovery pass quarantines
// a file this process wrote, after a rotation and a flush of every buffer.
//
// The sequences come from the quarantined file itself. An entry whose bytes
// are damaged has no readable identity; its sequence stays pending until a
// restart.
func (w *Writer) Quarantined(path, quarantinedAs string) int {
	w.mu.Lock()
	_, own := w.created[path]
	w.forgetFileLocked(path)
	w.mu.Unlock()
	if !own {
		return 0
	}
	// The file left the purge walk, and its pending entries are released
	// below: either can let the purge continue past where it stopped (#676).
	defer w.signalPurge()
	reader := NewReader(quarantinedAs, w.logger)
	ids, err := reader.ReadTrackedIdentities()
	if err != nil {
		w.logger.Warn().Err(err).Str("file", filepath.Base(quarantinedAs)).
			Msg("Could not read a quarantined WAL file; its pending entries hold the WAL purge until a restart")
	}
	released := 0
	w.pendingMu.Lock()
	for _, id := range ids {
		if seq, ok := w.trackedTokenSeq(id); ok {
			if _, pending := w.pendingSeqs[seq]; pending {
				delete(w.pendingSeqs, seq)
				released++
			}
		}
	}
	w.pendingMu.Unlock()
	if released > 0 {
		metrics.Get().AddWALQuarantineReleased(int64(released))
		// Warn, not Error: recovery already logged the failure at Error.
		w.logger.Warn().Str("file", filepath.Base(quarantinedAs)).Int("entries", released).
			Msg("Released the pending WAL entries of a quarantined WAL file so the WAL purge can continue. Keep the quarantined file: it holds those entries. Do not rename it back to .wal: the flush proof for its other entries may be gone, and they would be written again")
	}
	if reader.CorruptedEntries > 0 {
		w.logger.Warn().Str("file", filepath.Base(quarantinedAs)).Int64("damaged", reader.CorruptedEntries).
			Msg("Quarantined WAL file has damaged entries; any of them still pending holds the WAL purge until a restart")
	}
	return released
}

// trackedTokenSeq recovers the sequence from an identity token minted by this
// process. Tokens are "%016x%016x" of the writer instance and the sequence
// (appendTrackedEntry), so a foreign token — one inherited from a replayed
// entry written by an earlier process — is reported as not ours. Its sequence
// belongs to another numbering domain and means nothing against this
// process's floor.
func (w *Writer) trackedTokenSeq(token string) (uint64, bool) {
	if len(token) != 32 {
		return 0, false
	}
	instance, err := strconv.ParseUint(token[:16], 16, 64)
	if err != nil || instance != w.trackedInstance {
		return 0, false
	}
	seq, err := strconv.ParseUint(token[16:], 16, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// coversForeignIdentity reports whether any identity in a checkpoint batch
// describes an entry this process did not write: a row-range identity (which by
// construction covers part of an entry recovery replayed), or a whole-entry
// token minted by an earlier writer instance.
//
// The file such a checkpoint lands in must not be purged by sequence while an
// earlier process's WAL file remains — the entries it vouches for can live in
// such a file, absent from w.fileOrder, so the walk in PurgeFlushed cannot
// reach them to stop there.
func (w *Writer) coversForeignIdentity(hashes []string) bool {
	for _, h := range hashes {
		if strings.HasPrefix(h, recoveryRowPrefix) {
			return true
		}
		if len(h) == 32 {
			if _, ours := w.trackedTokenSeq(h); !ours {
				return true
			}
		}
	}
	return false
}

// releasePending drops identities from the unflushed set. Every release of
// the purge floor goes through here (MarkFlushed, ReleaseRegistered,
// ForgetTracked), so it is where the purge is told the floor may have moved.
func (w *Writer) releasePending(tokens []string) {
	released := false
	w.pendingMu.Lock()
	for _, token := range tokens {
		if seq, ok := w.trackedTokenSeq(token); ok {
			if _, pending := w.pendingSeqs[seq]; pending {
				delete(w.pendingSeqs, seq)
				released = true
			}
		}
	}
	w.pendingMu.Unlock()
	if released {
		w.signalPurge()
	}
}

// ForgetTracked drops identities whose data will never be flushed, because the
// write that produced them was abandoned after its WAL append.
//
// This is not the same as MarkFlushed: no checkpoint is written, because
// nothing reached storage. It only stops the floor waiting for data no buffer
// holds. Without it a single rejected write — a type-mismatched column, a
// client that disconnects during a schema-change flush, the schema-churn
// guard, a closing shard — would pin the floor for the life of the process,
// and because PurgeFlushed stops at the first retained file, the WAL would
// never reclaim anything again (#676).
//
// Callers must only pass identities they are certain no buffer and no flush
// in flight still owns.
func (w *Writer) ForgetTracked(hashes []string) {
	if len(hashes) == 0 {
		return
	}
	w.releasePending(hashes)
}

// MinUnflushedSequence returns the lowest tracked sequence this process has
// appended that no durable flush checkpoint covers. With nothing pending it
// returns one past the highest sequence issued.
//
// Deliberately NOT one past infinity. The caller reads the floor and then
// calls PurgeFlushed with it, under a different lock acquisition, so an append
// and a rotation can land in between; a MaxUint64 floor is above every
// sequence the writer can ever issue, and would purge that file. highest+1 is
// above everything pending and below anything issued later, which closes that
// window at no cost.
//
// Deliberately a plain scan of the pending set, too. An earlier revision
// walked a forward-only cursor over the sequence space to avoid it, which was
// both unsound and slower: unsound because a sequence allocated but not yet
// published let the cursor park above it and never return to it, and slower
// because every pending sequence lies in [min, highest], so the range the
// cursor walks is always at least as large as the set it is scanning.
// PendingUnflushedCount reports how many tracked sequences are awaiting a
// durable flush checkpoint. It is the floor's working set, and it grows for as
// long as flushes keep failing — during a prolonged object-store outage that
// is one entry per write request, which is worth being able to see rather than
// infer. Detect and report rather than cap, as with the ingest buffers.
func (w *Writer) PendingUnflushedCount() int {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()
	return len(w.pendingSeqs)
}

func (w *Writer) MinUnflushedSequence() uint64 {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()

	// Read inside the lock: a sequence is allocated and published together, so
	// this observes no sequence that is issued but not yet pending.
	highest := atomic.LoadUint64(&w.trackedSequence)
	if len(w.pendingSeqs) == 0 {
		return highest + 1
	}
	lowest := ^uint64(0)
	for seq := range w.pendingSeqs {
		if seq < lowest {
			lowest = seq
		}
	}
	return lowest
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
	// Listed outside w.mu: the writer loop holds it across rotation, which
	// syncs the old file.
	listed, listErr := listWALFilesFn(w.config.WALDir)
	hold := w.currentHold()
	w.mu.Lock()
	earlier, pinned := w.earlierFileLocked(listed, listErr)
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
		// A file holding proof for a retained file an earlier process wrote is
		// the only record that those entries were flushed, and that file is not
		// in w.fileOrder, so the ordering rule above cannot protect it.
		// Deleting this one makes recovery replay rows that are already in
		// storage — so it is pinned while any file an earlier process wrote
		// remains. Once recovery has deleted or quarantined them all, the
		// proof vouches for nothing: every crash replay writes such a
		// checkpoint, and a pin for the life of the process stopped this walk
		// for good (#1222).
		//
		// A row-range checkpoint for an entry in one of this process's own
		// files needs no pin: that parent's sequence stays pending until
		// recovery checkpoints it, so its file precedes this one in the walk
		// and stops it.
		if state.hasForeignProof && pinned {
			if earlier != "" && w.pinnedOn != earlier {
				w.pinnedOn = earlier
				w.logger.Info().Str("file", filepath.Base(path)).Str("waiting_on", filepath.Base(earlier)).
					Msg("WAL purge stopped at a file holding flush proof for an earlier process WAL file; it resumes once recovery has removed that file")
			}
			break
		}
		// A batch whose file is not registered yet keeps its WAL entries
		// (#1207 S4c), whatever the floor says: the floor is released by
		// recovery checkpoints and by an abandoned outbox, the registration
		// hold only by the registration itself.
		if state.cov.held(hold) {
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
		w.logPurged(deleted, minUnflushedSeq)
	}
	return deleted, firstErr
}

// purgeLogInterval is the least time between two Info lines from
// PurgeFlushed. The purge runs on every rotation and floor release (#676), up
// to once a second.
const purgeLogInterval = time.Minute

// logPurged reports a purge pass that deleted files: at Info at most once per
// purgeLogInterval, with the running total so the sampling drops lines, not
// information; at Debug otherwise.
func (w *Writer) logPurged(deleted int, minUnflushedSeq uint64) {
	total := w.purgedTotal.Add(int64(deleted))
	now := time.Now().UnixNano()
	last := w.purgeLogNano.Load()
	ev := w.logger.Debug()
	if now-last >= int64(purgeLogInterval) && w.purgeLogNano.CompareAndSwap(last, now) {
		ev = w.logger.Info()
	}
	ev.Int("deleted", deleted).
		Int64("deleted_total", total).
		Uint64("min_unflushed_seq", minUnflushedSeq).
		Msg("Purged WAL files whose data is flushed")
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
		"failed_syncs":        atomic.LoadInt64(&w.FailedSyncs),
		"total_rotations":     atomic.LoadInt64(&w.TotalRotations),
		"dropped_entries":     atomic.LoadInt64(&w.DroppedEntries),
		// The purge floor's working set. Climbs while flushes fail and does
		// not come back down until they succeed, so a steadily rising value
		// is an object-store problem, not a WAL one.
		"pending_unflushed": w.PendingUnflushedCount(),
		"failed_writes":     atomic.LoadInt64(&w.FailedWrites),
		"disk_full_writes":  atomic.LoadInt64(&w.DiskFullWrites),
		"recovered_writes":  atomic.LoadInt64(&w.RecoveredWrites),
		"disk_full":         w.diskFull.Load(),
		"purged_files":      w.purgedTotal.Load(),
		"buffer_size":       w.config.BufferSize,
		"buffer_used":       len(w.entryChan),
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

// SetReplicationHook sets the hook function called for each WAL entry (the
// origin stream's, #1207). The hook is called synchronously before the entry
// is written locally.
func (w *Writer) SetReplicationHook(hook ReplicationHook) {
	if hook == nil {
		w.replicationHook.Store(nil)
	} else {
		w.replicationHook.Store(&hook)
	}
	w.logger.Info().Msg("Replication hook set")
}

// replicate hands entry to the replication hook, if one is set. Lock-free;
// the hook must not block.
func (w *Writer) replicate(entry *ReplicationEntry) {
	hook := w.replicationHook.Load()
	if hook == nil {
		return
	}
	(*hook)(entry)
}
