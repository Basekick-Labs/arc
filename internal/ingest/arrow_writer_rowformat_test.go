package ingest

// End-to-end tests for row-format MessagePack ingest path.
//
// Regression guard for issue #401 — row-format writes to /api/v1/write/msgpack
// were silently dropped at flush with "no time data in batch" error.
//
// These tests exercise two flows:
//   1. Direct: buffer.Write(ctx, db, []interface{}{*models.Record}) → flush
//   2. Full:   raw msgpack bytes → decoder → buffer → flush
//
// The bug was not caught by existing rowsToColumnar unit tests because those
// tests stopped at the ColumnarRecord produced by rowsToColumnar; they never
// exercised the full buffer → flush pipeline or the decoder → buffer handoff.

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/Basekick-Labs/msgpack/v6"
)

// capturingStorageBackend records every Parquet write so tests can verify
// that data made it through the flush pipeline.
type capturingStorageBackend struct {
	mu     sync.Mutex
	writes [][]byte
	paths  []string
}

func (s *capturingStorageBackend) Write(ctx context.Context, path string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paths = append(s.paths, path)
	cp := make([]byte, len(data))
	copy(cp, data)
	s.writes = append(s.writes, cp)
	return nil
}

func (s *capturingStorageBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	buf := bytes.NewBuffer(make([]byte, 0, size))
	if _, err := io.Copy(buf, reader); err != nil {
		return err
	}
	return s.Write(ctx, path, buf.Bytes())
}

func (s *capturingStorageBackend) Read(ctx context.Context, path string) ([]byte, error) {
	return nil, nil
}
func (s *capturingStorageBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	return nil
}
func (s *capturingStorageBackend) Delete(ctx context.Context, path string) error { return nil }
func (s *capturingStorageBackend) Exists(ctx context.Context, path string) (bool, error) {
	return false, nil
}
func (s *capturingStorageBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (s *capturingStorageBackend) Close() error       { return nil }
func (s *capturingStorageBackend) Type() string       { return "mock-capturing" }
func (s *capturingStorageBackend) ConfigJSON() string { return "{}" }

func (s *capturingStorageBackend) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.writes)
}

func createE2EArrowBuffer(t *testing.T) (*ArrowBuffer, *capturingStorageBackend) {
	t.Helper()
	logger := zerolog.New(os.Stderr).Level(zerolog.DebugLevel)
	cfg := &config.IngestConfig{
		MaxBufferSize:  10,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   1,
		FlushQueueSize: 16,
	}
	storage := &capturingStorageBackend{}
	buf := NewArrowBuffer(cfg, storage, logger)
	t.Cleanup(func() { buf.Close() })
	return buf, storage
}

// TestArrowBuffer_WriteRowFormat_DirectRecord — direct path: construct
// *models.Record manually, send through Write. This mirrors what decodeRow
// produces.
func TestArrowBuffer_WriteRowFormat_DirectRecord(t *testing.T) {
	buf, storage := createE2EArrowBuffer(t)

	now := time.Now().UTC()
	records := []interface{}{
		&models.Record{
			Measurement: "row_test",
			Time:        now,
			Fields: map[string]interface{}{
				"sensor": "temp-1",
				"value":  22.5,
			},
			Tags: map[string]string{},
		},
	}

	for i := 0; i < 15; i++ {
		if err := buf.Write(context.Background(), "default", records); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && storage.writeCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if storage.writeCount() == 0 {
		t.Fatal("No Parquet files written — direct path failed")
	}
}

// TestArrowBuffer_WriteRowFormat_ViaDecoder — full path mirroring the HTTP
// handler: raw msgpack → MessagePackDecoder.Decode → ArrowBuffer.Write →
// flush. This is what the HTTP handler at /api/v1/write/msgpack does.
// This is the actual issue #401 reproduction.
func TestArrowBuffer_WriteRowFormat_ViaDecoder(t *testing.T) {
	buf, storage := createE2EArrowBuffer(t)
	logger := zerolog.New(os.Stderr).Level(zerolog.DebugLevel)
	decoder := NewMessagePackDecoder(logger)

	// Construct the exact payload from issue #401: single bare row map.
	now := time.Now().UnixMicro()
	payload := map[string]any{
		"m": "row_test",
		"t": now,
		"fields": map[string]any{
			"sensor": "temp-1",
			"value":  22.5,
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Decode exactly like the HTTP handler does.
	records, err := decoder.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	t.Logf("Decoded records type: %T", records)
	if recList, ok := records.([]interface{}); ok {
		t.Logf("Records count: %d", len(recList))
		for i, r := range recList {
			t.Logf("  Record[%d] type: %T, value: %+v", i, r, r)
		}
	}

	// Write enough to trigger flush.
	for i := 0; i < 15; i++ {
		if err := buf.Write(context.Background(), "default", records); err != nil {
			t.Fatalf("Write iter %d: %v", i, err)
		}
	}

	// Wait for async flush.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && storage.writeCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	if storage.writeCount() == 0 {
		t.Fatal("No Parquet files written — decoder→buffer→flush path failed (issue #401)")
	}
	t.Logf("Parquet files written: %d", storage.writeCount())
}

// TestArrowBuffer_WriteRowFormat_ViaDecoder_SingleWrite — single write (no
// size-based flush), rely on buffer-age flush.
func TestArrowBuffer_WriteRowFormat_ViaDecoder_SingleWrite(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.DebugLevel)
	cfg := &config.IngestConfig{
		MaxBufferSize:  1000,
		MaxBufferAgeMS: 200, // flush after 200ms
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   1,
		FlushQueueSize: 16,
	}
	storage := &capturingStorageBackend{}
	buf := NewArrowBuffer(cfg, storage, logger)
	defer buf.Close()

	decoder := NewMessagePackDecoder(logger)

	now := time.Now().UnixMicro()
	payload := map[string]any{
		"m": "row_test",
		"t": now,
		"fields": map[string]any{
			"sensor": "temp-1",
			"value":  22.5,
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	records, err := decoder.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if err := buf.Write(context.Background(), "default", records); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Wait for age-based flush (200ms + grace).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && storage.writeCount() == 0 {
		time.Sleep(20 * time.Millisecond)
	}

	if storage.writeCount() == 0 {
		t.Fatal("No Parquet files written — age-based flush failed for single row (issue #401)")
	}
}
