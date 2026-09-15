package ingest

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// bufferMetricsConfig keeps records in the buffer: the size threshold is high
// enough that the test controls when a flush happens, and the age threshold is
// long enough that it never fires on its own.
func bufferMetricsConfig() *config.IngestConfig {
	return &config.IngestConfig{
		MaxBufferSize:       10,
		MaxBufferAgeMS:      600_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      16,
		FlushTimeoutSeconds: 5,
	}
}

func bufferMetricsRecord() *models.Record {
	return &models.Record{
		Measurement: "buffer_metrics",
		Time:        time.Now().UTC(),
		Fields:      map[string]interface{}{"value": 1.0},
		Tags:        map[string]string{},
	}
}

func snapshotInt(t *testing.T, key string) int64 {
	t.Helper()
	v, ok := metrics.Get().Snapshot()[key]
	if !ok {
		t.Fatalf("metric %q missing from snapshot", key)
	}
	n, ok := v.(int64)
	if !ok {
		t.Fatalf("metric %q is %T, want int64", key, v)
	}
	return n
}

// TestBufferMetrics_RecordsBufferedTracksUnflushedData pins the ingest
// backpressure gauge.
//
// Regression test for #802: arc_buffer_records_buffered was exported but never
// set, so it read 0 forever — indistinguishable from an idle node, and useless
// as the "records accepted but not yet durable" signal it names. Publishing it
// only on flush is equally useless, because a flush is what empties the
// buffer, so a sampler refreshes it independently.
func TestBufferMetrics_RecordsBufferedTracksUnflushedData(t *testing.T) {
	buf := NewArrowBuffer(bufferMetricsConfig(), &mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = buf.Close() })

	// Stay below MaxBufferSize so nothing flushes.
	const buffered = 6
	for i := 0; i < buffered; i++ {
		if err := buf.Write(context.Background(), "default", []interface{}{bufferMetricsRecord()}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	// The sampler publishes on a 1s tick; poll rather than sleeping a fixed
	// interval so the test is not timing-fragile.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snapshotInt(t, "buffer_records_buffered") == buffered {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("buffer_records_buffered = %d, want %d: the ingest backpressure gauge does not reflect unflushed records (#802)",
		snapshotInt(t, "buffer_records_buffered"), buffered)
}

// TestBufferMetrics_FlushCountersMove pins that the flush counters are
// published at all. Before #802 they were exported and never set.
//
// The published value is this buffer's own lifetime count, not a process-wide
// running total: SetBufferFlushes stores rather than adds, which is correct
// because cmd/arc/main.go:839 constructs exactly one ArrowBuffer. So the
// assertion compares against the buffer's internal counters rather than
// against a snapshot taken before it was created. An earlier test in this
// package leaves its own (higher) count in the metrics singleton, so a
// before/after delta on the global would be testing test-execution order.
func TestBufferMetrics_FlushCountersMove(t *testing.T) {
	buf := NewArrowBuffer(bufferMetricsConfig(), &mockStorageBackend{}, zerolog.New(io.Discard))

	// Cross MaxBufferSize so a flush fires.
	for i := 0; i < 12; i++ {
		if err := buf.Write(context.Background(), "default", []interface{}{bufferMetricsRecord()}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The buffer actually flushed, so there is something to publish.
	wantFlushes := buf.totalFlushes.Load()
	wantWritten := buf.totalRecordsWritten.Load()
	if wantFlushes == 0 {
		t.Fatal("precondition: the buffer recorded no flushes, so the test cannot observe publication")
	}
	if wantWritten == 0 {
		t.Fatal("precondition: the buffer wrote no records, so the test cannot observe publication")
	}

	if got := snapshotInt(t, "buffer_flushes_total"); got != wantFlushes {
		t.Fatalf("buffer_flushes_total = %d, want %d: the buffer's flush count is not published (#802)", got, wantFlushes)
	}
	if got := snapshotInt(t, "buffer_records_written"); got != wantWritten {
		t.Fatalf("buffer_records_written = %d, want %d: the buffer's written count is not published (#802)", got, wantWritten)
	}
}
