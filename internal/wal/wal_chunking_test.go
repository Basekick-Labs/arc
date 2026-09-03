package wal

import (
	"errors"
	"strings"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

func newChunkTestWriter(t *testing.T) *Writer {
	t.Helper()
	writer, err := NewWriter(&WriterConfig{
		WALDir:       t.TempDir(),
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 1024 * 1024 * 1024, // no rotation mid-test
		BufferSize:   100000,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return writer
}

func TestWriter_AppendRaw_ChunksOversizedRowPayload(t *testing.T) {
	writer := newChunkTestWriter(t)
	record := map[string]interface{}{"_measurement": "cpu", "time": int64(1), "field": strings.Repeat("r", 64*1024)}
	records := make([]interface{}, 2000)
	for i := range records {
		records[i] = record
	}
	payload, err := msgpack.Marshal(records)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(payload) <= MaxWALPayloadSize {
		t.Fatalf("test bug: payload %d not above cap", len(payload))
	}
	if err := writer.AppendRaw(payload); err != nil {
		t.Fatalf("AppendRaw: %v", err)
	}
	path := writer.CurrentFile()
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entries, err := NewReader(path, zerolog.Nop()).ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected multiple chunk entries, got %d", len(entries))
	}
	total := 0
	for i, e := range entries {
		if e.Records == nil {
			t.Fatalf("entry %d not row format", i)
		}
		total += len(e.Records)
	}
	if total != 2000 {
		t.Fatalf("record union mismatch: got %d, want 2000", total)
	}
}

func TestWriter_AppendRaw_SingleOversizedElementStillRejected(t *testing.T) {
	writer := newChunkTestWriter(t)
	defer writer.Close()
	payload, err := msgpack.Marshal(strings.Repeat("x", MaxWALPayloadSize+1000))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	before := metrics.Get().Snapshot()["wal_oversized_payloads"].(int64)
	err = writer.AppendRaw(payload)
	if err == nil || !strings.Contains(err.Error(), "104857600") {
		t.Fatalf("want ErrPayloadTooLarge naming the limit, got: %v", err)
	}
	if after := metrics.Get().Snapshot()["wal_oversized_payloads"].(int64); after != before+1 {
		t.Fatalf("oversized metric should bump by 1, got %d -> %d", before, after)
	}
}

func TestWriter_AppendRaw_RejectsOversizedRaggedColumns(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]interface{}{"columns": map[string]interface{}{"a": []int{1, 2}, "b": []int{1}}, "padding": strings.Repeat("p", MaxWALPayloadSize+1)})
	if err != nil {
		t.Fatal(err)
	}
	writer := newChunkTestWriter(t)
	defer writer.Close()
	before := metrics.Get().Snapshot()["wal_oversized_payloads"].(int64)
	err = writer.AppendRaw(payload)
	if !errors.Is(err, ErrPayloadTooLarge) || !strings.Contains(err.Error(), "column") {
		t.Fatalf("AppendRaw error = %v, want ErrPayloadTooLarge", err)
	}
	if after := metrics.Get().Snapshot()["wal_oversized_payloads"].(int64); after != before+1 {
		t.Fatalf("oversized metric should bump by 1, got %d -> %d", before, after)
	}
}
