package ingest

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
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

func snapshotFloat(t *testing.T, key string) float64 {
	t.Helper()
	v, ok := metrics.Get().Snapshot()[key]
	if !ok {
		t.Fatalf("metric %q missing from snapshot", key)
	}
	n, ok := v.(float64)
	if !ok {
		t.Fatalf("metric %q is %T, want float64", key, v)
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

type blockingBufferMetricsBackend struct {
	mockStorageBackend
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingBufferMetricsBackend) Write(ctx context.Context, _ string, _ []byte) error {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestBufferMetricsIncludeInFlightFlush(t *testing.T) {
	release := make(chan struct{})
	backend := &blockingBufferMetricsBackend{entered: make(chan struct{}), release: release}
	cfg := bufferMetricsConfig()
	cfg.MaxBufferSize = 1
	buf := NewArrowBuffer(cfg, backend, zerolog.New(io.Discard))
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = buf.Close()
	})

	if err := buf.Write(context.Background(), "default", []interface{}{bufferMetricsRecord()}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	select {
	case <-backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush worker did not reach storage")
	}

	// The batch has left the shard but is still retained by the blocked worker.
	if got := buf.currentBufferedRecords(); got != 0 {
		t.Fatalf("currentBufferedRecords() = %d, want 0 after handoff", got)
	}
	buf.publishBufferMemoryMetrics()

	if got := snapshotInt(t, "buffer_bytes_buffered"); got <= 0 {
		t.Fatalf("buffer_bytes_buffered = %d while worker retains a batch, want > 0", got)
	}
	if got := snapshotFloat(t, "buffer_oldest_unflushed_seconds"); got <= 0 {
		t.Fatalf("buffer_oldest_unflushed_seconds = %f while flush is in flight, want > 0", got)
	}
	prometheus := metrics.Get().PrometheusFormat()
	if !strings.Contains(prometheus, "arc_buffer_bytes_buffered ") || !strings.Contains(prometheus, "arc_buffer_oldest_unflushed_seconds ") || !strings.Contains(prometheus, "arc_buffer_memory_limit_bytes ") || !strings.Contains(prometheus, "arc_buffer_memory_pressure ") {
		t.Fatalf("Prometheus output missing buffer memory gauges:\n%s", prometheus)
	}

	releaseOnce.Do(func() { close(release) })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && buf.bufferBytesBuffered.Load() != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := buf.bufferBytesBuffered.Load(); got != 0 {
		t.Fatalf("bufferBytesBuffered = %d after completed flush, want 0", got)
	}
	buf.publishBufferMemoryMetrics()
	if got := snapshotInt(t, "buffer_bytes_buffered"); got != 0 {
		t.Fatalf("buffer_bytes_buffered = %d after completed flush, want 0", got)
	}
	if got := snapshotFloat(t, "buffer_oldest_unflushed_seconds"); got != 0 {
		t.Fatalf("buffer_oldest_unflushed_seconds = %f after completed flush, want 0", got)
	}
}

func TestBufferMetricsWarnAtMemoryThreshold(t *testing.T) {
	var logs bytes.Buffer
	buf := &ArrowBuffer{
		config:           &config.IngestConfig{MaxBufferSize: 100, MaxBufferAgeMS: 100},
		memoryLimitBytes: 100,
		logger:           zerolog.New(&logs),
	}
	buf.bufferBytesBuffered.Store(50)
	buf.publishBufferMemoryMetrics()
	buf.publishBufferMemoryMetrics()
	if got := snapshotInt(t, "buffer_memory_pressure"); got != 1 {
		t.Fatalf("buffer_memory_pressure = %d at 50%%, want 1", got)
	}
	if got := snapshotInt(t, "buffer_memory_limit_bytes"); got != 100 {
		t.Fatalf("buffer_memory_limit_bytes = %d, want 100", got)
	}
	if got := strings.Count(logs.String(), "Ingest buffers are using a large share"); got != 1 {
		t.Fatalf("memory pressure warning logged %d times while continuously above threshold, want 1", got)
	}

	buf.bufferBytesBuffered.Store(49)
	buf.publishBufferMemoryMetrics()
	if got := snapshotInt(t, "buffer_memory_pressure"); got != 0 {
		t.Fatalf("buffer_memory_pressure = %d below threshold, want 0", got)
	}
	buf.bufferBytesBuffered.Store(50)
	buf.publishBufferMemoryMetrics()
	if got := strings.Count(logs.String(), "Ingest buffers are using a large share"); got != 2 {
		t.Fatalf("memory pressure warning logged %d times after crossing threshold twice, want 2", got)
	}
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
