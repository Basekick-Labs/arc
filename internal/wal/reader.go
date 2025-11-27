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
	TimestampUS uint64                   // Microseconds since epoch
	Records     []map[string]interface{} // Deserialized records
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

	// Deserialize records
	var records []map[string]interface{}
	if err := msgpack.Unmarshal(payload, &records); err != nil {
		return nil, fmt.Errorf("failed to deserialize records: %w", err)
	}

	return &Entry{
		TimestampUS: timestampUS,
		Records:     records,
	}, nil
}
