package ingest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// newReplayTestBuffer returns a buffer flushing to tmpDir.
func newReplayTestBuffer(t *testing.T, tmpDir string) *ArrowBuffer {
	t.Helper()
	cfg := &config.IngestConfig{
		MaxBufferSize:  1000000,
		MaxBufferAgeMS: 60000,
		FlushWorkers:   2,
		FlushQueueSize: 10,
		ShardCount:     4,
		Compression:    "snappy",
	}
	localStorage, err := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	t.Cleanup(func() { localStorage.Close() })
	return NewArrowBuffer(cfg, localStorage, zerolog.Nop())
}

func parquetPaths(t *testing.T, tmpDir string) []string {
	t.Helper()
	var paths []string
	_ = filepath.Walk(tmpDir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(p, ".parquet") {
			rel, _ := filepath.Rel(tmpDir, p)
			paths = append(paths, rel)
		}
		return nil
	})
	return paths
}

// TestNoWALReplayNormalizesSecondTimestamps is the #590 repro: WAL replay and
// cluster replication feed RAW client payloads into WriteColumnarDirectNoWAL,
// which historically skipped the live decode path's timestamp normalization.
// A client legitimately sending SECOND-precision timestamps (accepted and
// normalized on the live path) would replay into 1970-era partitions
// (seconds interpreted as microseconds by groupByHour).
//
// 1700000000 seconds = 2023-11-14. The partition path is the assertion.
func TestNoWALReplayNormalizesSecondTimestamps(t *testing.T) {
	tmpDir := t.TempDir()
	buf := newReplayTestBuffer(t, tmpDir)

	columns := map[string][]interface{}{
		"time": {int64(1700000000), int64(1700000001)}, // seconds precision
		"v":    {1.5, 2.5},
	}
	if err := buf.WriteColumnarDirectNoWAL(context.Background(), "replaydb", "m1", columns); err != nil {
		t.Fatalf("WriteColumnarDirectNoWAL: %v", err)
	}
	if err := buf.Close(); err != nil { // Close flushes pending buffers
		t.Fatalf("close: %v", err)
	}

	paths := parquetPaths(t, tmpDir)
	if len(paths) == 0 {
		t.Fatal("no parquet files written")
	}
	for _, p := range paths {
		if strings.Contains(p, "1970") {
			t.Fatalf("replayed data landed in 1970 partition (#590): %s", p)
		}
		if !strings.Contains(p, string(filepath.Separator)+"2023"+string(filepath.Separator)) {
			t.Fatalf("expected 2023 partition for 1700000000s, got: %s", p)
		}
	}
}

// TestNoWALReplayGeneratesMissingTime: a raw payload whose client omitted
// the time column (the live path generated timestamps at ingest, but the WAL
// stores the ORIGINAL bytes without them) must not break replay — timestamps
// are generated like the live path does.
func TestNoWALReplayGeneratesMissingTime(t *testing.T) {
	tmpDir := t.TempDir()
	buf := newReplayTestBuffer(t, tmpDir)

	columns := map[string][]interface{}{
		"v":    {int64(1), int64(2), int64(3)},
		"host": {"a", "b", "c"},
	}
	if err := buf.WriteColumnarDirectNoWAL(context.Background(), "replaydb", "m2", columns); err != nil {
		t.Fatalf("WriteColumnarDirectNoWAL: %v", err)
	}
	if err := buf.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	paths := parquetPaths(t, tmpDir)
	if len(paths) == 0 {
		t.Fatal("no parquet files written for missing-time payload")
	}
	for _, p := range paths {
		if strings.Contains(p, "1970") {
			t.Fatalf("missing-time payload landed in 1970 partition: %s", p)
		}
	}
}

// TestNoWALReplaySanitizesStrings: replayed raw payloads must get the same
// UTF-8 sanitization the live path applies (invalid UTF-8 in parquet breaks
// DuckDB queries).
func TestNoWALReplaySanitizesStrings(t *testing.T) {
	tmpDir := t.TempDir()
	buf := newReplayTestBuffer(t, tmpDir)

	columns := map[string][]interface{}{
		"time": {int64(1700000000000000)},
		"host": {"bad\xff\xfeutf8"},
	}
	if err := buf.WriteColumnarDirectNoWAL(context.Background(), "replaydb", "m3", columns); err != nil {
		t.Fatalf("WriteColumnarDirectNoWAL: %v", err)
	}
	// The write mutates columns in place exactly like the live path;
	// sanitized means valid UTF-8 now.
	s := columns["host"][0].(string)
	if strings.Contains(s, "\xff") {
		t.Fatalf("string column not sanitized on replay path: %q", s)
	}
	_ = buf.Close()
}
