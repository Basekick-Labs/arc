package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	// ChecksumMismatches is the part of CorruptedEntries read whole whose
	// checksum did not match: bytes on disk, where the other damage (a read
	// error, a length past the limit, a trailer or checkpoint that does not
	// parse) may be transient or a format this build cannot read (#1634).
	ChecksumMismatches int64
	// TruncatedEntries counts incomplete entries at EOF after an interrupted
	// append. These are not corrupt complete entries and cannot be replayed.
	TruncatedEntries int64
	// UndecodableEntries counts intact entries whose payload this build
	// cannot decode. They are returned with DecodeErr set, and are included
	// in TotalEntries.
	UndecodableEntries int64
	// RepairedLengths counts frames whose length field was damaged and whose
	// true length was found by the payload checksum and confirmed by what
	// follows (#1641, length_repair.go). They are read like intact frames.
	RepairedLengths int64

	// fileSize is the size of the file being read, for length repair; 0
	// disables it. pos is readFrame's offset in the file. repairTried: a
	// file gets one repair attempt (#1641), so a walk misaligned by other
	// damage does not search the rest of the file at every step.
	fileSize    int64
	pos         int64
	repairTried bool
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
	TimestampUS      uint64                   // Microseconds since epoch
	PayloadHash      string                   // Entry identity: the writer-assigned "instanceSeq" (32 hex) for a tracked entry, else a SHA-256 of the logical payload (64 hex)
	CheckpointHashes []string                 // Flush checkpoint identities, when present
	Records          []map[string]interface{} // Row format (from Append path)
	ColumnarData     *ColumnarEntry           // Columnar format (from AppendRaw path)
	Coverage         *Coverage                // Ingest batch coverage, for a covered (0x04) entry (#1207)
	// DecodeErr is set for an entry whose bytes are intact (checksum valid)
	// but whose payload this build cannot decode. Its identity is still known,
	// so recovery can tell whether it was already flushed (#1225).
	DecodeErr error
}

// ColumnarEntry represents a columnar WAL entry written via the zero-copy path
type ColumnarEntry struct {
	Database    string // From envelope metadata (empty = "default")
	Measurement string
	Columns     map[string][]interface{}
	TagColumns  []string
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
		return nil, errBadWALMagic
	}
	if info, err := f.Stat(); err == nil {
		r.fileSize = info.Size() // for length repair (#1641)
	}
	r.pos = WALFileHeaderSize

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
			r.logger.Warn().Err(err).Msg("Skipping unreadable WAL entry")
			r.CorruptedEntries++
			if errors.Is(err, errChecksumMismatch) {
				r.ChecksumMismatches++
			}
			continue
		}

		if entry.DecodeErr != nil {
			r.UndecodableEntries++
			// Debug: recovery skips it when it was flushed, and logs it
			// at Error when it was not.
			r.logger.Debug().Err(entry.DecodeErr).Str("payload_hash", entry.PayloadHash).Msg("WAL entry payload cannot be decoded")
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
		Int64("repaired_lengths", r.RepairedLengths).
		Msg("WAL read complete")

	return entries, nil
}

// errChecksumMismatch: an entry read whole whose checksum does not match;
// the next entry starts right after it.
var errChecksumMismatch = errors.New("checksum mismatch")

// errBadWALMagic: the file does not start with the WAL magic bytes.
var errBadWALMagic = errors.New("invalid WAL magic bytes")

// ReadTrackedIdentities returns the identity tokens of the intact tracked
// (0x03) and covered (0x04) entries of a WAL file, without decoding their
// payloads (#1225). A damaged entry is counted in CorruptedEntries and
// skipped; a torn tail ends the scan.
func (r *Reader) ReadTrackedIdentities() ([]string, error) {
	f, _, err := r.openFrames()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ids []string
	for {
		_, payload, err := r.readFrame(f)
		if err == io.EOF {
			return ids, nil
		}
		if errors.Is(err, errChecksumMismatch) {
			r.CorruptedEntries++
			continue
		}
		if err != nil {
			// An I/O error, or a length that cannot be trusted or repaired:
			// nothing after it can be found.
			r.CorruptedEntries++
			if errors.Is(err, ErrPayloadTooLarge) {
				return ids, nil
			}
			return ids, err
		}
		switch {
		case len(payload) >= 17 && payload[0] == WALTrackedMarker:
			ids = append(ids, fmt.Sprintf("%016x%016x", binary.BigEndian.Uint64(payload[1:9]), binary.BigEndian.Uint64(payload[9:17])))
		case len(payload) > 0 && payload[0] == WALCoveredMarker:
			if token, _, _, err := parseCoveredPayload(payload); err == nil {
				ids = append(ids, token)
			} else {
				r.CorruptedEntries++
			}
		}
	}
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
		return nil, errBadWALMagic
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
	r.fileSize = fileInfo.Size()

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
		payloadOffset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return hashes, fmt.Errorf("failed to locate WAL payload: %w", err)
		}
		if payloadLen > MaxWALPayloadSize || payloadOffset+int64(payloadLen) > fileInfo.Size() {
			// Possibly a damaged length field rather than a torn tail
			// (#1641): the same repair ReadAll applies, so both scans see
			// the same frames. A repaired frame is a checkpoint or skipped.
			if payload, ok := r.repairLength(f, payloadOffset, expectedChecksum); ok {
				r.TotalEntries++
				if len(payload) > 0 && payload[0] == WALCheckpointMarker {
					var checkpointHashes []string
					if err := msgpack.Unmarshal(payload[1:], &checkpointHashes); err != nil {
						r.CorruptedEntries++
						continue
					}
					hashes = append(hashes, checkpointHashes...)
				}
				continue
			}
		}
		if payloadLen > MaxWALPayloadSize {
			r.CorruptedEntries++
			return hashes, fmt.Errorf("%w: size %d exceeds limit %d", ErrPayloadTooLarge, payloadLen, MaxWALPayloadSize)
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

// readFrame reads one entry frame (header and payload) and verifies its
// checksum. io.EOF ends the file (a torn tail included); any other error
// is one damaged entry, already consumed.
func (r *Reader) readFrame(f *os.File) (uint64, []byte, error) {
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
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("failed to read entry header: %w", err)
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	timestampUS := binary.BigEndian.Uint64(header[4:12])
	expectedChecksum := binary.BigEndian.Uint32(header[12:16])
	r.pos += WALEntryHeaderSize
	payloadStart := r.pos

	// A length past the limit or past the end of the file may be a damaged
	// length field rather than a torn tail (#1641): read what is there and
	// look for the frame's true end before giving up on the rest of the file.
	if payloadLen > MaxWALPayloadSize || (r.fileSize > 0 && int64(payloadLen) > r.fileSize-payloadStart) {
		if payload, ok := r.repairLength(f, payloadStart, expectedChecksum); ok {
			return timestampUS, payload, nil
		}
		if payloadLen > MaxWALPayloadSize {
			return 0, nil, fmt.Errorf("%w: size %d exceeds limit %d", ErrPayloadTooLarge, payloadLen, MaxWALPayloadSize)
		}
		// A complete header followed by an incomplete payload is the same
		// crash suffix as a torn header. Earlier complete entries may still
		// be recovered and fenced; checksum/decode failures remain errors.
		r.TruncatedEntries++
		return 0, nil, io.EOF
	}

	// Read payload
	payload := make([]byte, payloadLen)
	n, err := io.ReadFull(f, payload)
	r.pos += int64(n)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			r.TruncatedEntries++
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("failed to read payload: %w", err)
	}

	r.TotalBytes += int64(WALEntryHeaderSize + n)

	// Verify checksum
	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return 0, nil, fmt.Errorf("%w: expected %d, got %d", errChecksumMismatch, expectedChecksum, actualChecksum)
	}
	return timestampUS, payload, nil
}

// repairLength reads the rest of the file from payloadStart (at most
// MaxWALPayloadSize bytes) and looks for the frame's true end (#1641). On
// success the file is positioned after the frame.
func (r *Reader) repairLength(f *os.File, payloadStart int64, expected uint32) ([]byte, bool) {
	if r.repairTried || r.fileSize <= payloadStart {
		return nil, false
	}
	r.repairTried = true
	size := r.fileSize - payloadStart
	if size > MaxWALPayloadSize {
		size = MaxWALPayloadSize
	}
	have := make([]byte, size)
	n, err := f.ReadAt(have, payloadStart)
	if err != nil && err != io.EOF {
		return nil, false
	}
	l := repairFrameLength(f, r.fileSize, payloadStart, expected, have[:n])
	if l == 0 {
		return nil, false
	}
	if _, err := f.Seek(payloadStart+int64(l), io.SeekStart); err != nil {
		return nil, false
	}
	r.pos = payloadStart + int64(l)
	r.RepairedLengths++
	r.TotalBytes += int64(WALEntryHeaderSize + l)
	return have[:l], true
}

// openFrames opens the file, checks its header and records its size for
// length repair.
func (r *Reader) openFrames() (*os.File, []byte, error) {
	f, err := os.Open(r.filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	header := make([]byte, WALFileHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		f.Close()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, nil, io.EOF
		}
		return nil, nil, fmt.Errorf("failed to read WAL file header: %w", err)
	}
	if !bytes.Equal(header[0:4], WALMagic) {
		f.Close()
		return nil, nil, errBadWALMagic
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}
	r.fileSize = info.Size()
	r.pos = WALFileHeaderSize
	return f, header, nil
}

// readEntry reads a single entry from the file
func (r *Reader) readEntry(f *os.File) (*Entry, error) {
	timestampUS, payload, err := r.readFrame(f)
	if err != nil {
		return nil, err
	}
	if len(payload) > 0 && payload[0] == WALCheckpointMarker {
		var hashes []string
		if err := msgpack.Unmarshal(payload[1:], &hashes); err != nil {
			return nil, fmt.Errorf("failed to deserialize WAL checkpoint: %w", err)
		}
		return &Entry{TimestampUS: timestampUS, CheckpointHashes: hashes}, nil
	}
	payloadHashValue := payloadHash(payload)
	logicalPayload := payload
	var coverage *Coverage
	if len(payload) >= 17 && payload[0] == WALTrackedMarker {
		payloadHashValue = fmt.Sprintf("%016x%016x", binary.BigEndian.Uint64(payload[1:9]), binary.BigEndian.Uint64(payload[9:17]))
		logicalPayload = payload[17:]
	} else if len(payload) > 0 && payload[0] == WALCoveredMarker {
		token, logical, cov, err := parseCoveredPayload(payload)
		if err != nil {
			return nil, err
		}
		payloadHashValue, logicalPayload, coverage = token, logical, cov
	}

	e := DecodeLogical(logicalPayload)
	e.TimestampUS, e.PayloadHash, e.Coverage = timestampUS, payloadHashValue, coverage
	return e, nil
}

// DecodeLogical decodes a logical WAL payload ([0x01][dbLen][db][msgpack] or
// bare msgpack): row form into Records, columnar form into ColumnarData, and
// anything else into an entry carrying DecodeErr. The standby store keeps
// these payloads as the origin wrote them (#1207).
func DecodeLogical(logicalPayload []byte) *Entry {
	// Parse envelope to extract database name and inner msgpack payload
	database, msgpackData := ParseEnvelope(logicalPayload, "")

	// Try row format first (array of maps from Append path)
	var records []map[string]interface{}
	if err := msgpack.Unmarshal(msgpackData, &records); err == nil {
		return &Entry{Records: records}
	}

	// Try columnar format (map with m + columns from AppendRaw path)
	var rawMap map[string]interface{}
	if err := msgpack.Unmarshal(msgpackData, &rawMap); err == nil {
		if colEntry := parseColumnarEntry(rawMap); colEntry != nil {
			colEntry.Database = database
			return &Entry{ColumnarData: colEntry}
		}
	}

	return &Entry{DecodeErr: errors.New("unrecognized WAL entry format")}
}

// MeasurementName reads the "m" of a columnar payload the way ingest does
// (ingest.extractMeasurement): a string as is, an integer as
// "measurement_<n>". The WAL keeps the payload the client sent, so a reader
// that accepted only a string failed every entry written with an integer
// measurement (#1225).
func MeasurementName(m interface{}) (string, bool) {
	switch v := m.(type) {
	case string:
		return v, true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("measurement_%v", v), true
	default:
		return "", false
	}
}

// parseColumnarEntry extracts measurement and columns from a raw msgpack map
func parseColumnarEntry(rawMap map[string]interface{}) *ColumnarEntry {
	m, ok := MeasurementName(rawMap["m"])
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
	tagColumns := make([]string, 0)
	if rawTagColumns, ok := rawMap["tag_keys"].([]interface{}); ok {
		for _, rawTag := range rawTagColumns {
			if tag, ok := rawTag.(string); ok {
				tagColumns = append(tagColumns, tag)
			}
		}
	}

	return &ColumnarEntry{
		Measurement: m,
		Columns:     columns,
		TagColumns:  tagColumns,
	}
}

// ScanCovered streams the intact covered (0x04) entries of the file whose
// coverage match accepts, in file order; only those payloads are decoded
// (#1207 S4b1c2b: an origin republishing parked batches reads them back).
// A damaged entry is counted and skipped; a torn tail ends the scan. An
// error from fn stops it and is returned.
func (r *Reader) ScanCovered(match func(Coverage) bool, fn func(*Entry) error) error {
	f, err := os.Open(r.filePath)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer f.Close()
	var header [WALFileHeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		return fmt.Errorf("failed to read WAL file header: %w", err)
	}
	if !bytes.Equal(header[0:4], WALMagic) {
		return errBadWALMagic
	}
	if info, err := f.Stat(); err == nil {
		r.fileSize = info.Size() // for length repair (#1641)
	}
	r.pos = WALFileHeaderSize
	for {
		ts, payload, err := r.readFrame(f)
		if err == io.EOF {
			return nil
		}
		if errors.Is(err, errChecksumMismatch) {
			r.CorruptedEntries++
			continue
		}
		if err != nil {
			// An I/O error, or a length that cannot be trusted: the next
			// entry's position is unknown.
			r.CorruptedEntries++
			return err
		}
		if len(payload) == 0 || payload[0] != WALCoveredMarker {
			continue
		}
		token, logical, cov, err := parseCoveredPayload(payload)
		if err != nil {
			r.CorruptedEntries++
			continue
		}
		if !match(*cov) {
			continue
		}
		e := DecodeLogical(logical)
		e.TimestampUS, e.PayloadHash, e.Coverage = ts, token, cov
		r.TotalEntries++
		if err := fn(e); err != nil {
			return err
		}
	}
}
