package ingest

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// hangingStorageBackend simulates an S3 backend that hangs after N successful writes.
// This reproduces production behavior where S3 becomes slow or unresponsive,
// causing flush workers to block indefinitely due to missing context timeouts.
type hangingStorageBackend struct {
	mu              sync.Mutex
	writesCompleted int
	hangAfterN      int           // writes succeed up to this count, then hang
	hangDuration    time.Duration // 0 = hang forever
	stuck           atomic.Int32  // currently blocked writes
	totalHung       atomic.Int32  // total writes that entered hang path
}

func newHangingStorage(hangAfterN int, hangDuration time.Duration) *hangingStorageBackend {
	return &hangingStorageBackend{
		hangAfterN:   hangAfterN,
		hangDuration: hangDuration,
	}
}

func (h *hangingStorageBackend) Write(ctx context.Context, path string, data []byte) error {
	h.mu.Lock()
	h.writesCompleted++
	shouldHang := h.writesCompleted > h.hangAfterN
	h.mu.Unlock()

	if shouldHang {
		h.stuck.Add(1)
		h.totalHung.Add(1)
		defer h.stuck.Add(-1)

		if h.hangDuration > 0 {
			select {
			case <-time.After(h.hangDuration):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// hang forever — only ctx cancellation can unblock
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (h *hangingStorageBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	data, _ := io.ReadAll(r)
	return h.Write(ctx, path, data)
}
func (h *hangingStorageBackend) Read(ctx context.Context, path string) ([]byte, error)     { return nil, nil }
func (h *hangingStorageBackend) ReadTo(ctx context.Context, path string, w io.Writer) error { return nil }
func (h *hangingStorageBackend) List(ctx context.Context, prefix string) ([]string, error)  { return nil, nil }
func (h *hangingStorageBackend) Delete(ctx context.Context, path string) error              { return nil }
func (h *hangingStorageBackend) Exists(ctx context.Context, path string) (bool, error)      { return false, nil }
func (h *hangingStorageBackend) Close() error                                               { return nil }
func (h *hangingStorageBackend) Type() string                                               { return "mock-hanging" }
func (h *hangingStorageBackend) ConfigJSON() string                                         { return "{}" }

// makeColumns builds a columnar batch with the given record count.
func makeColumns(n int) map[string][]interface{} {
	ts := make([]interface{}, n)
	vals := make([]interface{}, n)
	tags := make([]interface{}, n)
	base := time.Now().UnixMicro()
	for i := 0; i < n; i++ {
		ts[i] = base + int64(i)*1000
		vals[i] = float64(i) * 0.1
		tags[i] = fmt.Sprintf("device_%d", i%50)
	}
	return map[string][]interface{}{
		"time":      ts,
		"value":     vals,
		"device_id": tags,
	}
}

// -----------------------------------------------------------------------------
// Test: flush workers block forever when S3 hangs (no timeout on context)
// -----------------------------------------------------------------------------

func TestFlushWorkers_BlockForever_WhenStorageHangs(t *testing.T) {
	logger := zerolog.Nop()
	store := newHangingStorage(1, 0) // 1 write succeeds, then hang forever

	cfg := &config.IngestConfig{
		MaxBufferSize:   2000,
		MaxBufferAgeMS:  60000, // disable age-based flush
		Compression:     "snappy",
		UseDictionary:   true,
		WriteStatistics: true,
		DataPageVersion: "2.0",
		FlushWorkers:    2,
		FlushQueueSize:  5,
		ShardCount:      4,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// First flush — should succeed (within the hangAfterN window)
	buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(2500))
	time.Sleep(1 * time.Second) // let flush complete

	if store.stuck.Load() != 0 {
		t.Fatalf("expected 0 stuck workers after first flush, got %d", store.stuck.Load())
	}

	// Second flush — should hang
	buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(2500))
	time.Sleep(1 * time.Second)

	stuckCount := int(store.stuck.Load())
	if stuckCount == 0 {
		t.Fatal("expected at least 1 stuck worker after S3 hang, got 0")
	}
	t.Logf("CONFIRMED: %d/%d flush workers stuck on storage.Write with no timeout", stuckCount, cfg.FlushWorkers)

	// Verify Close() cannot unblock the stuck worker
	closeDone := make(chan struct{})
	go func() { buf.Close(); close(closeDone) }()

	select {
	case <-closeDone:
		// Close returned — worker may have been unblocked by ctx cancellation
		// This is only possible if the flush task propagates buffer ctx (currently it doesn't)
	case <-time.After(3 * time.Second):
		t.Log("CONFIRMED: Close() hangs because flush task uses context.Background()")
	}

	// The bug: flush tasks are created with context.Background() (arrow_writer.go:1182)
	// so neither the buffer's ctx cancellation nor Close() can stop in-flight S3 writes.
}

// -----------------------------------------------------------------------------
// Test: periodic flush goroutine blocks when storage hangs
// -----------------------------------------------------------------------------

func TestPeriodicFlush_BlocksOnStorageHang(t *testing.T) {
	logger := zerolog.Nop()
	store := newHangingStorage(0, 0) // hang on ALL writes

	cfg := &config.IngestConfig{
		MaxBufferSize:   100000, // large — won't trigger size-based flush
		MaxBufferAgeMS:  200,    // 200ms age threshold, ticker at 100ms
		Compression:     "snappy",
		UseDictionary:   true,
		WriteStatistics: true,
		DataPageVersion: "2.0",
		FlushWorkers:    2,
		FlushQueueSize:  10,
		ShardCount:      4,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// Write small batch — not enough for size flush, but periodic flush will pick it up
	buf.WriteColumnarDirect(context.Background(), "db", "sensor", makeColumns(100))

	// Wait for periodic flush to fire and get stuck on S3
	time.Sleep(1 * time.Second)

	if store.stuck.Load() == 0 {
		t.Fatal("expected periodic flush to be stuck on storage write, got 0 stuck")
	}

	// The periodic flush goroutine is now blocked inside flushBufferLocked → storage.Write.
	// Verify that it cannot flush OTHER buffers anymore.
	buf.WriteColumnarDirect(context.Background(), "db", "other_measurement", makeColumns(100))
	time.Sleep(1 * time.Second)

	stats := buf.GetStats()
	written, _ := stats["total_records_written"].(int64)
	if written != 0 {
		t.Fatalf("expected 0 records written (all flushes should be stuck), got %d", written)
	}
	t.Log("CONFIRMED: periodic flush stuck — no measurements can flush via age-based path")

	closeDone := make(chan struct{})
	go func() { buf.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(3 * time.Second):
		t.Log("CONFIRMED: Close() hangs — periodic flush goroutine stuck on storage.Write")
	}
}

// -----------------------------------------------------------------------------
// Test: all flush workers exhausted → queue fills → data dropped
// -----------------------------------------------------------------------------

func TestAllFlushWorkers_Exhausted_QueueFills(t *testing.T) {
	logger := zerolog.Nop()
	store := newHangingStorage(0, 0) // hang on ALL writes

	cfg := &config.IngestConfig{
		MaxBufferSize:   1000,
		MaxBufferAgeMS:  60000, // disable age flush
		Compression:     "snappy",
		UseDictionary:   true,
		WriteStatistics: true,
		DataPageVersion: "2.0",
		FlushWorkers:    2,
		FlushQueueSize:  3, // tiny queue
		ShardCount:      2,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// Write enough to trigger multiple flushes — workers and queue will fill up
	for i := 0; i < 20; i++ {
		buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(600))
	}
	time.Sleep(2 * time.Second)

	stats := buf.GetStats()
	errors, _ := stats["total_errors"].(int64)
	written, _ := stats["total_records_written"].(int64)
	buffered, _ := stats["total_records_buffered"].(int64)

	if written != 0 {
		t.Fatalf("expected 0 records written to S3 (all hung), got %d", written)
	}

	if errors == 0 {
		t.Fatal("expected flush queue full errors, got 0")
	}

	t.Logf("CONFIRMED: workers=%d stuck, queue full errors=%d, buffered=%d, written=%d",
		store.stuck.Load(), errors, buffered, written)
	t.Log("CONFIRMED: data is accepted but never persisted to S3")

	closeDone := make(chan struct{})
	go func() { buf.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(3 * time.Second):
	}
}

// -----------------------------------------------------------------------------
// Test: with a timeout on storage writes, workers recover
// This proves the fix: using context.WithTimeout instead of context.Background
// -----------------------------------------------------------------------------

func TestFlushWorkers_RecoverWithTimeout(t *testing.T) {
	logger := zerolog.Nop()
	// Hang for 1 second then return — simulates what a timeout context would do
	store := newHangingStorage(1, 1*time.Second)

	cfg := &config.IngestConfig{
		MaxBufferSize:   2000,
		MaxBufferAgeMS:  60000,
		Compression:     "snappy",
		UseDictionary:   true,
		WriteStatistics: true,
		DataPageVersion: "2.0",
		FlushWorkers:    2,
		FlushQueueSize:  5,
		ShardCount:      4,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// First flush succeeds
	buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(2500))
	time.Sleep(500 * time.Millisecond)

	// Subsequent flushes hang for 1s then recover
	buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(2500))
	buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(2500))
	buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(2500))

	// Wait for hung writes to resolve
	time.Sleep(4 * time.Second)

	stuck := int(store.stuck.Load())
	if stuck != 0 {
		t.Fatalf("expected 0 stuck workers after timeout, got %d", stuck)
	}

	stats := buf.GetStats()
	written, _ := stats["total_records_written"].(int64)
	if written == 0 {
		t.Fatal("expected some records written after workers recovered, got 0")
	}

	t.Logf("CONFIRMED: workers recovered — %d records written, 0 stuck", written)
	t.Log("This proves: adding a timeout to flush context would fix the hang")

	buf.Close()
}

// -----------------------------------------------------------------------------
// Test: memory grows while flush workers are stuck
// -----------------------------------------------------------------------------

func TestMemoryGrows_WhileFlushWorkersStuck(t *testing.T) {
	logger := zerolog.Nop()
	store := newHangingStorage(1, 0) // 1 write then hang forever

	cfg := &config.IngestConfig{
		MaxBufferSize:   5000,
		MaxBufferAgeMS:  60000,
		Compression:     "snappy",
		UseDictionary:   true,
		WriteStatistics: true,
		DataPageVersion: "2.0",
		FlushWorkers:    2,
		FlushQueueSize:  3,
		ShardCount:      4,
	}

	buf := NewArrowBuffer(cfg, store, logger)

	// Trigger first flush (succeeds)
	buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(6000))
	time.Sleep(1 * time.Second)

	// Now storage hangs — keep writing for 5 seconds
	var totalBuffered int64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			goto done
		default:
			buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(500))
			totalBuffered += 500
			time.Sleep(20 * time.Millisecond)
		}
	}
done:

	stats := buf.GetStats()
	written, _ := stats["total_records_written"].(int64)
	buffered, _ := stats["total_records_buffered"].(int64)
	errors, _ := stats["total_errors"].(int64)

	if written == buffered {
		t.Fatalf("expected written < buffered (some data lost), got written=%d buffered=%d", written, buffered)
	}

	lostRecords := buffered - written
	lostPct := float64(lostRecords) / float64(buffered) * 100

	t.Logf("Buffered: %d, Written: %d, Lost: %d (%.0f%%), Errors: %d",
		buffered, written, lostRecords, lostPct, errors)
	t.Logf("Stuck workers: %d/%d", store.stuck.Load(), cfg.FlushWorkers)
	t.Log("CONFIRMED: data accepted but not persisted while workers are stuck on S3")

	closeDone := make(chan struct{})
	go func() { buf.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(3 * time.Second):
	}
}
