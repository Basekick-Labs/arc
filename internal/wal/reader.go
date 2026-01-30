package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

// Reader reads WAL files for recovery operations
type Reader struct {
	filePath string
	logger   zerolog.Logger

	// Metrics
	TotalEntries     int64
	TotalBytes       int64
	CorruptedEntries int64
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
	TimestampUS  uint64                   // Microseconds since epoch
	Records      []map[string]interface{} // Row format (from Append path)
	ColumnarData *ColumnarEntry           // Columnar format (from AppendRaw path)
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
	header := make([]byte, WALFileHeaderSize)
	n, err := f.Read(header)
	if err != nil || n < WALFileHeaderSize {
		r.logger.Warn().Str("file", r.filePath).Msg("WAL file too short")
		return entries, nil
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
			r.logger.Error().Err(err).Msg("Error reading WAL entry")
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
		Msg("WAL read complete")

	return entries, nil
}

// readEntry reads a single entry from the file
func (r *Reader) readEntry(f *os.File) (*Entry, error) {
	// Read entry header
	header := make([]byte, WALEntryHeaderSize)
	n, err := f.Read(header)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil || n < WALEntryHeaderSize {
		return nil, fmt.Errorf("failed to read entry header: %w", err)
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	timestampUS := binary.BigEndian.Uint64(header[4:12])
	expectedChecksum := binary.BigEndian.Uint32(header[12:16])

	// Read payload
	payload := make([]byte, payloadLen)
	n, err = io.ReadFull(f, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload: %w", err)
	}

	r.TotalBytes += int64(WALEntryHeaderSize + n)

	// Verify checksum
	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %d, got %d", expectedChecksum, actualChecksum)
	}

	// Check for envelope format: [0x01 marker][2-byte dbLen][dbName][msgpack]
	var database string
	msgpackData := payload
	if len(payload) > 3 && payload[0] == WALEnvelopeMarker {
		dbLen := binary.BigEndian.Uint16(payload[1:3])
		if int(3+dbLen) <= len(payload) {
			database = string(payload[3 : 3+dbLen])
			msgpackData = payload[3+dbLen:]
		}
	}

	// Try row format first (array of maps from Append path)
	var records []map[string]interface{}
	if err := msgpack.Unmarshal(msgpackData, &records); err == nil {
		return &Entry{
			TimestampUS: timestampUS,
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
