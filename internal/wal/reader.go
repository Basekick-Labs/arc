package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/rs/zerolog"
)

// Reader reads WAL files for recovery operations
type Reader struct {
	filePath string
	logger   zerolog.Logger

	// Metrics
	TotalEntries     int64
	TotalBytes       int64
	CorruptedEntries int64
	// TruncatedEntries counts incomplete entries at EOF after an interrupted
	// append. These are not corrupt complete entries and cannot be replayed.
	TruncatedEntries int64
}

// NewReader creates a new WAL reader
func NewReader(filePath string, logger zerolog.Logger) *Reader {
	return &Reader{
		filePath: filePath,
		logger:   logger.With().Str("component", "wal-reader").Logger(),
	}
}

// Entry represents a single WAL entry
type Entry struct {
	// ReplicationPayload is populated only for received entries. Keeping the
	// originating tracked envelope lets one replica-specific callback handle
	// row and columnar formats identically during live apply and recovery.
	// Records/ColumnarData are deliberately empty for this provenance.
	ReplicationPayload []byte
	TimestampUS        uint64                   // Microseconds since epoch
	PayloadHash        string                   // Entry identity: the writer-assigned "instanceSeq" (32 hex) for a tracked entry, else a SHA-256 of the logical payload (64 hex)
	CheckpointHashes   []string                 // Flush checkpoint identities, when present
	Records            []map[string]interface{} // Row format (from Append path)
	ColumnarData       *ColumnarEntry           // Columnar format (from AppendRaw path)
}

// ColumnarEntry represents a columnar WAL entry written via the zero-copy path
type ColumnarEntry struct {
	Database    string // From envelope metadata (empty = "default")
	Measurement string
	Columns     map[string][]interface{}
}

// ReadAll reads all entries from the WAL file
func (r *Reader) ReadAll() ([]Entry, error) {
	var entries []Entry

	f, err := os.Open(r.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer f.Close()

	// Read and verify header
	var header [WALFileHeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			r.logger.Warn().Str("file", r.filePath).Msg("WAL file too short")
			return entries, nil
		}
		return nil, fmt.Errorf("failed to read WAL file header: %w", err)
	}

	// Verify magic bytes
	if !bytes.Equal(header[0:4], WALMagic) {
		return nil, fmt.Errorf("invalid WAL magic bytes")
	}

	// Check version
	version := binary.BigEndian.Uint16(header[4:6])
	if version != WALVersion {
		r.logger.Warn().
			Uint16("file_version", version).
			Uint16("expected_version", WALVersion).
			Msg("WAL version mismatch")
	}

	// Read entries
	for {
		entry, err := r.readEntry(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			r.logger.Warn().Err(err).Msg("Skipping incomplete WAL entry")
			r.CorruptedEntries++
			continue
		}

		entries = append(entries, *entry)
		r.TotalEntries++
	}

	r.logger.Info().
		Str("file", r.filePath).
		Int64("entries", r.TotalEntries).
		Int64("bytes", r.TotalBytes).
		Int64("corrupted", r.CorruptedEntries).
		Int64("truncated", r.TruncatedEntries).
		Msg("WAL read complete")

	return entries, nil
}

// ReadCheckpointHashes scans a WAL file for flush checkpoints without
// materializing or decoding ordinary data entries. Non-checkpoint payloads are
// skipped by seeking over their validated on-disk lengths; the recovery pass
// still reads and checksum-validates entries before replaying them.
func (r *Reader) ReadCheckpointHashes() ([]string, error) {
	var hashes []string
	f, err := os.Open(r.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer f.Close()

	var header [WALFileHeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			r.logger.Warn().Str("file", r.filePath).Msg("WAL file too short")
			return hashes, nil
		}
		return nil, fmt.Errorf("failed to read WAL file header: %w", err)
	}
	if !bytes.Equal(header[0:4], WALMagic) {
		return nil, fmt.Errorf("invalid WAL magic bytes")
	}
	version := binary.BigEndian.Uint16(header[4:6])
	if version != WALVersion {
		r.logger.Warn().Uint16("file_version", version).Uint16("expected_version", WALVersion).
			Msg("WAL version mismatch")
	}
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}

	for {
		var entryHeader [WALEntryHeaderSize]byte
		if _, err := io.ReadFull(f, entryHeader[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if err == io.ErrUnexpectedEOF {
					r.TruncatedEntries++
				}
				break
			}
			return hashes, fmt.Errorf("failed to read WAL entry header: %w", err)
		}

		payloadLen := binary.BigEndian.Uint32(entryHeader[0:4])
		timestampUS := binary.BigEndian.Uint64(entryHeader[4:12])
		expectedChecksum := binary.BigEndian.Uint32(entryHeader[12:16])
		if payloadLen > MaxWALPayloadSize {
			r.CorruptedEntries++
			return hashes, fmt.Errorf("%w: size %d exceeds limit %d", ErrPayloadTooLarge, payloadLen, MaxWALPayloadSize)
		}
		payloadOffset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return hashes, fmt.Errorf("failed to locate WAL payload: %w", err)
		}
		if payloadOffset+int64(payloadLen) > fileInfo.Size() {
			r.TruncatedEntries++
			r.logger.Warn().Str("file", r.filePath).Uint64("timestamp_us", timestampUS).
				Msg("Skipping incomplete WAL entry while scanning checkpoints")
			break
		}
		r.TotalEntries++
		r.TotalBytes += int64(WALEntryHeaderSize) + int64(payloadLen)
		if payloadLen == 0 {
			continue
		}

		var marker [1]byte
		if _, err := io.ReadFull(f, marker[:]); err != nil {
			r.CorruptedEntries++
			break
		}
		if marker[0] != WALCheckpointMarker {
			if _, err := f.Seek(int64(payloadLen)-1, io.SeekCurrent); err != nil {
				return hashes, fmt.Errorf("failed to skip WAL data payload: %w", err)
			}
			continue
		}

		payload := make([]byte, int(payloadLen))
		payload[0] = marker[0]
		if _, err := io.ReadFull(f, payload[1:]); err != nil {
			r.CorruptedEntries++
			break
		}
		if actualChecksum := crc32.ChecksumIEEE(payload); actualChecksum != expectedChecksum {
			r.CorruptedEntries++
			r.logger.Warn().Str("file", r.filePath).Uint64("timestamp_us", timestampUS).
				Msg("Skipping WAL checkpoint with checksum mismatch")
			continue
		}
		var checkpointHashes []string
		if err := msgpack.Unmarshal(payload[1:], &checkpointHashes); err != nil {
			r.CorruptedEntries++
			r.logger.Warn().Err(err).Str("file", r.filePath).Uint64("timestamp_us", timestampUS).
				Msg("Skipping malformed WAL checkpoint")
			continue
		}
		hashes = append(hashes, checkpointHashes...)
	}

	r.logger.Debug().Str("file", r.filePath).Int64("entries", r.TotalEntries).
		Int64("bytes", r.TotalBytes).Int64("corrupted", r.CorruptedEntries).
		Int("checkpoint_hashes", len(hashes)).Msg("WAL checkpoint scan complete")
	return hashes, nil
}

// readEntry reads a single entry from the file
func (r *Reader) readEntry(f *os.File) (*Entry, error) {
	// Read entry header
	var header [WALEntryHeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		// io.EOF: 0 bytes read at clean end of WAL file.
		// io.ErrUnexpectedEOF: 1-15 bytes read then EOF (crash during write,
		// truncated entry header). Must stop here — the file offset is
		// mid-header; continuing would cascade misaligned reads.
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if err == io.ErrUnexpectedEOF {
				r.TruncatedEntries++
			}
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read entry header: %w", err)
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	timestampUS := binary.BigEndian.Uint64(header[4:12])
	expectedChecksum := binary.BigEndian.Uint32(header[12:16])

	// Validate payload length to prevent OOM on corrupt WAL files
	if payloadLen > MaxWALPayloadSize {
		return nil, fmt.Errorf("%w: size %d exceeds limit %d", ErrPayloadTooLarge, payloadLen, MaxWALPayloadSize)
	}

	// Read payload
	payload := make([]byte, payloadLen)
	n, err := io.ReadFull(f, payload)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// A complete header followed by an incomplete payload is the same
			// crash suffix as a torn header. Earlier complete entries may still
			// be recovered and fenced; checksum/decode failures remain errors.
			r.TruncatedEntries++
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read payload: %w", err)
	}

	r.TotalBytes += int64(WALEntryHeaderSize + n)

	// Verify checksum
	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %d, got %d", expectedChecksum, actualChecksum)
	}
	if len(payload) > 0 && payload[0] == WALCheckpointMarker {
		var hashes []string
		if err := msgpack.Unmarshal(payload[1:], &hashes); err != nil {
			return nil, fmt.Errorf("failed to deserialize WAL checkpoint: %w", err)
		}
		return &Entry{TimestampUS: timestampUS, CheckpointHashes: hashes}, nil
	}
	if len(payload) > 0 && payload[0] == WALReplicatedMarker {
		identity, _, err := TrackedPayload(payload)
		if err != nil {
			return nil, err
		}
		// This buffer is owned by readEntry. Restore the wire marker for the
		// replica apply callback while retaining provenance in the Entry type.
		payload[0] = WALTrackedMarker
		return &Entry{TimestampUS: timestampUS, PayloadHash: identity, ReplicationPayload: payload}, nil
	}
	payloadHashValue := payloadHash(payload)
	logicalPayload := payload
	if len(payload) >= 17 && payload[0] == WALTrackedMarker {
		payloadHashValue = fmt.Sprintf("%016x%016x", binary.BigEndian.Uint64(payload[1:9]), binary.BigEndian.Uint64(payload[9:17]))
		logicalPayload = payload[17:]
	}

	// Parse envelope to extract database name and inner msgpack payload
	database, msgpackData := ParseEnvelope(logicalPayload, "")

	// Try row format first (array of maps from Append path)
	var records []map[string]interface{}
	if err := msgpack.Unmarshal(msgpackData, &records); err == nil {
		return &Entry{
			TimestampUS: timestampUS,
			PayloadHash: payloadHashValue,
			Records:     records,
		}, nil
	}

	// Try columnar format (map with m + columns from AppendRaw path)
	var rawMap map[string]interface{}
	if err := msgpack.Unmarshal(msgpackData, &rawMap); err == nil {
		if colEntry := parseColumnarEntry(rawMap); colEntry != nil {
			colEntry.Database = database
			return &Entry{
				TimestampUS:  timestampUS,
				PayloadHash:  payloadHashValue,
				ColumnarData: colEntry,
			}, nil
		}
	}

	return nil, fmt.Errorf("failed to deserialize: unrecognized WAL entry format")
}

// parseColumnarEntry extracts measurement and columns from a raw msgpack map
func parseColumnarEntry(rawMap map[string]interface{}) *ColumnarEntry {
	m, ok := rawMap["m"].(string)
	if !ok {
		return nil
	}

	colsRaw, ok := rawMap["columns"]
	if !ok {
		return nil
	}

	colsMap, ok := colsRaw.(map[string]interface{})
	if !ok {
		return nil
	}

	columns := make(map[string][]interface{}, len(colsMap))
	for k, v := range colsMap {
		arr, ok := v.([]interface{})
		if !ok {
			continue
		}
		columns[k] = arr
	}

	return &ColumnarEntry{
		Measurement: m,
		Columns:     columns,
	}
}
