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
func (h *hangingStorageBackend) Read(ctx context.Context, path string) ([]byte, error) {
	return nil, nil
}
func (h *hangingStorageBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	return nil
}
func (h *hangingStorageBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (h *hangingStorageBackend) Delete(ctx context.Context, path string) error { return nil }
func (h *hangingStorageBackend) Exists(ctx context.Context, path string) (bool, error) {
	return false, nil
}
func (h *hangingStorageBackend) Close() error       { return nil }
func (h *hangingStorageBackend) Type() string       { return "mock-hanging" }
func (h *hangingStorageBackend) ConfigJSON() string { return "{}" }
func (h *hangingStorageBackend) ReadToAt(_ context.Context, _ string, _ io.Writer, _ int64) error {
	return nil
}
func (h *hangingStorageBackend) StatFile(_ context.Context, _ string) (int64, error) {
	return -1, nil
}
func (h *hangingStorageBackend) AppendReader(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}

type gatedStorageBackend struct {
	*hangingStorageBackend
	release   <-chan struct{}
	started   chan struct{}
	completed chan struct{}
	active    atomic.Int32
}

func (g *gatedStorageBackend) Write(_ context.Context, _ string, _ []byte) error {
	g.active.Add(1)
	g.started <- struct{}{}
	<-g.release
	g.active.Add(-1)
	g.completed <- struct{}{}
	return nil
}

func (g *gatedStorageBackend) WriteReader(ctx context.Context, path string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return g.Write(ctx, path, data)
}

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
		// Bounded explicitly. These tests deliberately leave a flush stuck in the
		// backend and then call Close; Close now WAITS for flushes in progress
		// instead of cancelling them (#1007), so an unset timeout would default to
		// 30s and leave a Close goroutine parked for that long after the test
		// returned.
		FlushTimeoutSeconds: 2,
		FlushWorkers:        2,
		FlushQueueSize:      5,
		ShardCount:          4,
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
		t.Log("CONFIRMED: the worker is stuck in the backend and Close cannot finish instantly")
	}

	// Flush tasks now carry no context at all; the worker builds one from flushParent at dequeue, bounded by FlushTimeoutSeconds (#1006). Close waits for an in-flight write rather than cancelling it, bounded by its own budget (#1007)
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
		// Bounded explicitly. These tests deliberately leave a flush stuck in the
		// backend and then call Close; Close now WAITS for flushes in progress
		// instead of cancelling them (#1007), so an unset timeout would default to
		// 30s and leave a Close goroutine parked for that long after the test
		// returned.
		FlushTimeoutSeconds: 2,
		FlushWorkers:        2,
		FlushQueueSize:      10,
		ShardCount:          4,
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
// Test: when all workers and queued slots are occupied, writes remain buffered
// and Close persists them after the storage backend recovers.
// -----------------------------------------------------------------------------

func TestAllFlushWorkers_Exhausted_QueueFills_DataRemainsBuffered(t *testing.T) {
	for _, typed := range []bool{false, true} {
		name := "columnar"
		if typed {
			name = "typed-columnar"
		}
		t.Run(name, func(t *testing.T) {
			testFlushQueueSaturationRetainsData(t, typed)
		})
	}
}

func testFlushQueueSaturationRetainsData(t *testing.T, typed bool) {
	logger := zerolog.Nop()
	release := make(chan struct{})
	store := &gatedStorageBackend{
		hangingStorageBackend: newHangingStorage(0, 0),
		release:               release,
		started:               make(chan struct{}, 32),
		completed:             make(chan struct{}, 32),
	}

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

	const batchSize = 600
	const batchCount = 20
	for i := 0; i < batchCount; i++ {
		var err error
		if typed {
			batch := &TypedColumnBatch{Data: map[string]interface{}{
				"time": make([]int64, batchSize), "value": make([]float64, batchSize),
				"device_id": make([]string, batchSize),
			}}
			err = buf.WriteTypedColumnarDirect(context.Background(), "db", "m1", batch, batchSize)
		} else {
			err = buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(batchSize))
		}
		if err != nil {
			t.Fatalf("write %d was rejected instead of retained: %v", i, err)
		}
	}
	for i := 0; i < cfg.FlushWorkers; i++ {
		select {
		case <-store.started:
		case <-time.After(5 * time.Second):
			t.Fatal("flush worker did not reach gated storage")
		}
	}

	stats := buf.GetStats()
	errors, _ := stats["total_errors"].(int64)
	written, _ := stats["total_records_written"].(int64)
	buffered, _ := stats["total_records_buffered"].(int64)
	if written != 0 {
		t.Fatalf("expected writes to remain gated, got %d records written", written)
	}
	if errors != 0 {
		t.Fatalf("queue saturation must not count as a write failure, got %d", errors)
	}
	if buffered != batchSize*batchCount {
		t.Fatalf("buffered records = %d, want %d", buffered, batchSize*batchCount)
	}
	if got := buf.totalFlushDeferred.Load(); got == 0 {
		t.Fatal("expected full queue to defer at least one flush")
	}
	// Deliberately NOT asserting len(buf.flushQueue) == cap(buf.flushQueue).
	// The gate only holds FlushWorkers tasks inside the backend; a worker is free
	// to dequeue the next task the moment it finishes handing one over, so the
	// observed depth legitimately sits anywhere in [1, cap]. Pinning it to cap
	// failed 10 times in 25 runs under -race, which is how CI runs the suite.
	//
	// totalFlushDeferred > 0 above already proves what the capacity check was
	// reaching for: a deferral can only happen when the send found no room.
	if got := len(buf.flushQueue); got == 0 {
		t.Fatal("flush queue is empty, so the deferrals above cannot have come from a full queue")
	}

	// Let the already-enqueued tasks drain before Close, then Close flushes the
	// retained batches synchronously.
	queuedAndActive := int(buf.queueDepth.Load()) + int(store.active.Load())
	close(release)
	for i := 0; i < queuedAndActive; i++ {
		select {
		case <-store.completed:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d queued/active flushes completed", i, queuedAndActive)
		}
	}
	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	final := buf.GetStats()
	if got, _ := final["total_records_written"].(int64); got != batchSize*batchCount {
		t.Fatalf("records written after Close = %d, want %d", got, batchSize*batchCount)
	}
	if !buf.CloseFlushedCleanly() {
		t.Fatal("Close did not report a clean flush")
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
		// Bounded explicitly. These tests deliberately leave a flush stuck in the
		// backend and then call Close; Close now WAITS for flushes in progress
		// instead of cancelling them (#1007), so an unset timeout would default to
		// 30s and leave a Close goroutine parked for that long after the test
		// returned.
		FlushTimeoutSeconds: 2,
		FlushWorkers:        2,
		FlushQueueSize:      5,
		ShardCount:          4,
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
		// Bounded explicitly. These tests deliberately leave a flush stuck in the
		// backend and then call Close; Close now WAITS for flushes in progress
		// instead of cancelling them (#1007), so an unset timeout would default to
		// 30s and leave a Close goroutine parked for that long after the test
		// returned.
		FlushTimeoutSeconds: 2,
		FlushWorkers:        2,
		FlushQueueSize:      3,
		ShardCount:          4,
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
