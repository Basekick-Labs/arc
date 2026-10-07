package ingest

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// releasableBackend blocks every write until released, counting how many writes
// have begun. It is the gate that lets a test hold the flush queue full.
type releasableBackend struct {
	release   chan struct{}
	once      sync.Once
	started   atomic.Int64
	completed atomic.Int64
}

// releaseAll is idempotent so a test can release mid-body and still defer it.
func (r *releasableBackend) releaseAll() { r.once.Do(func() { close(r.release) }) }

func newReleasableBackend() *releasableBackend {
	return &releasableBackend{release: make(chan struct{})}
}

func (r *releasableBackend) Write(ctx context.Context, _ string, _ []byte) error {
	r.started.Add(1)
	select {
	case <-r.release:
		r.completed.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *releasableBackend) WriteReader(ctx context.Context, path string, rd io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, rd)
	return r.Write(ctx, path, nil)
}
func (r *releasableBackend) Read(context.Context, string) ([]byte, error)    { return nil, nil }
func (r *releasableBackend) ReadTo(context.Context, string, io.Writer) error { return nil }
func (r *releasableBackend) List(context.Context, string) ([]string, error)  { return nil, nil }
func (r *releasableBackend) Delete(context.Context, string) error            { return nil }
func (r *releasableBackend) Exists(context.Context, string) (bool, error)    { return false, nil }
func (r *releasableBackend) Close() error                                    { return nil }
func (r *releasableBackend) Type() string                                    { return "mock-releasable" }
func (r *releasableBackend) ConfigJSON() string                              { return "{}" }
func (r *releasableBackend) ReadToAt(context.Context, string, io.Writer, int64) error {
	return nil
}
func (r *releasableBackend) StatFile(context.Context, string) (int64, error) { return -1, nil }
func (r *releasableBackend) AppendReader(context.Context, string, io.Reader, int64) error {
	return nil
}

// drainerConfig: the size trigger is the only one that fires, so a deferral can
// only be resolved by the drainer or by Close — never by the age sweep, which
// would otherwise mask what these tests measure.
func drainerConfig() *config.IngestConfig {
	return &config.IngestConfig{
		MaxBufferSize:       100,
		MaxBufferAgeMS:      3_600_000, // age sweep must NOT rescue a deferred buffer
		Compression:         "snappy",
		ShardCount:          4,
		FlushWorkers:        1,
		FlushQueueSize:      2,
		FlushTimeoutSeconds: 30,
		DataPageVersion:     "2.0",
	}
}

// TestDrainer_EnqueuesDeferredBufferWhenASlotFrees is the #1008 regression.
//
// A buffer that crossed MaxBufferSize while the queue was full keeps its records
// and is marked deferred. Nothing re-enqueues it: not a later write (ingest has
// moved on to other measurements), not the age sweep (disabled here, and 100ms
// in the shipped config only by luck of configuration), not Close (the process
// is still running). Pre-drainer it sits there while the flush worker goes idle.
//
// The drainer must pick it up as soon as a worker frees a queue slot.
func TestDrainer_EnqueuesDeferredBufferWhenASlotFrees(t *testing.T) {
	store := newReleasableBackend()
	buf := NewArrowBuffer(drainerConfig(), store, zerolog.New(io.Discard))
	buf.SetCloseBudget(30 * time.Second)
	defer func() { store.releaseAll(); _ = buf.Close() }()

	// Fill the worker and both queue slots, then defer several more. Distinct
	// measurements so no later write to the same key can rescue them.
	for i := 0; i < 8; i++ {
		meas := "m" + string(rune('a'+i))
		if err := buf.WriteColumnarDirect(context.Background(), "db", meas, makeColumns(100)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for buf.countDeferredBuffers() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	deferredBefore := buf.countDeferredBuffers()
	if deferredBefore == 0 {
		t.Fatal("no buffer was deferred; the fixture did not saturate the queue")
	}

	// Free the workers. No further writes arrive, so only the drainer can move
	// these buffers.
	store.releaseAll()

	deadline = time.Now().Add(10 * time.Second)
	for buf.countDeferredBuffers() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := buf.countDeferredBuffers(); got > 0 {
		t.Fatalf("%d buffer(s) still deferred %v after the workers were freed (started with %d); nothing re-enqueues a deferred buffer, so it waits for the age sweep or Close (#1008)",
			got, 10*time.Second, deferredBefore)
	}
}

// TestDrainer_OneWakeDoesBoundedWork is the B1 regression, narrowed to the part
// that can be pinned deterministically.
//
// enqueueOrDeferLocked returns (false, _) for four different reasons — queue
// full, closing, ctx cancelled, and losing the last slot to another writer — and
// the caller cannot tell them apart. A drainer that retries a key on (false, _)
// never finishes its pass. Two consequences: it burns a core and increments
// arc_ingest_flush_deferred_total at CPU speed, and if it is inside that loop
// while Close is waiting on b.wg it keeps Close from returning at all — the
// coordinator then skips the wal-purge and wal components, so the WAL writer's
// final drain-and-sync never runs and the retained WAL is missing its newest
// entries.
//
// What this pins, precisely: drainDeferredOnce RETURNS when handed far more
// deferred buffers than the flush queue can hold. It terminates via the
// queue-full check, which is the reachable path.
//
// What it does NOT pin, stated rather than implied: the Close-hang half of B1.
// That needs queue room AND outstanding deferred keys at the exact moment Close
// waits on b.wg, and every other exit (queue full, no work, ctx cancelled,
// vanished buffer) fires first in any fixture simple enough to be worth having.
// The per-iteration ctx re-check and the no-retry rule prevent it by
// construction; neither is covered by a test, and the attempt budget in
// drainDeferredOnce is unreachable in all current paths.
func TestDrainer_OneWakeDoesBoundedWork(t *testing.T) {
	store := newReleasableBackend()
	buf := NewArrowBuffer(drainerConfig(), store, zerolog.New(io.Discard))
	buf.SetCloseBudget(5 * time.Second)
	defer func() { store.releaseAll(); _ = buf.Close() }()

	// Far more deferred buffers than cap(flushQueue), each with a live buffer so
	// the existence check cannot short-circuit the loop for us.
	const keys = 50
	for i := 0; i < keys; i++ {
		key := "db/bounded" + string(rune('A'+i%26)) + string(rune('a'+i/26))
		// A REAL batch, not an empty slice. With `[]interface{}{}` the two tasks
		// the drainer does enqueue carry no records, so mergeBatches fails with
		// "no batches to merge" and markFlushFailure fires — the test would pass
		// while deterministically producing the very pathology
		// TestDrainer_SurvivesAVanishedBuffer exists to prevent. It would also
		// break the moment enqueueOrDeferLocked learns to skip empty buffers.
		batch, _, err := buf.convertColumnsToTyped("bounded", makeColumns(10))
		if err != nil {
			t.Fatalf("build batch: %v", err)
		}
		shard := buf.getShard(key)
		shard.mu.Lock()
		shard.buffers[key] = []interface{}{batch}
		shard.bufferStartTimes[key] = time.Now().UTC().Add(-time.Duration(i) * time.Second)
		shard.bufferRecordCounts[key] = 10
		shard.deferredKeys[key] = struct{}{}
		shard.mu.Unlock()
	}

	done := make(chan struct{})
	go func() {
		buf.drainDeferredOnce()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("drainDeferredOnce did not return with more deferred buffers than the queue can hold: it is retrying a key that enqueueOrDeferLocked keeps refusing, which burns a core and can keep Close from returning (#1008 B1)")
	}

	// One pass must not have emptied the whole backlog: it is bounded, and the
	// next freed slot is what continues it.
	if remaining := buf.countDeferredBuffers(); remaining == 0 {
		t.Fatal("one drain pass cleared all 50 deferred buffers; the pass is meant to be bounded by the queue capacity")
	}
	if buf.HasFlushFailure() {
		t.Fatal("the drain pass recorded a flush failure; the enqueued tasks are carrying no records")
	}
}

// TestDrainer_SurvivesAVanishedBuffer is the B2 regression.
//
// The drainer chooses a key from a snapshot taken under a read lock, then takes
// the write lock to act. In between, the buffer can be flushed by the age sweep,
// a schema-change flush, FlushAll, or an ordinary write. enqueueOrDeferLocked
// built its task as `records: shard.buffers[bufferKey]` with no existence check,
// so a vanished key produced a task with nil records and a stale count:
// mergeBatches fails with "no batches to merge", markFlushFailure fires, and on
// a perfectly healthy system that triggers a full periodic WAL recovery pass and
// leaves CloseFlushedCleanly false for the rest of the process lifetime.
//
// Driven directly, because the window is too narrow to race reliably.
func TestDrainer_SurvivesAVanishedBuffer(t *testing.T) {
	store := newReleasableBackend()
	store.releaseAll() // writes complete immediately
	buf := NewArrowBuffer(drainerConfig(), store, zerolog.New(io.Discard))
	buf.SetCloseBudget(30 * time.Second)
	defer func() { _ = buf.Close() }()

	const key = "db/ghost"
	shard := buf.getShard(key)

	// A deferred marker whose buffer does not exist — exactly what the drainer
	// sees when the buffer is flushed between its scan and its act.
	shard.mu.Lock()
	shard.deferredKeys[key] = struct{}{}
	shard.bufferStartTimes[key] = time.Now().UTC()
	shard.bufferRecordCounts[key] = 500
	queued, deferred := buf.enqueueOrDeferLocked(shard, key, "db", "ghost", 500)
	_, stillMarked := shard.deferredKeys[key]
	shard.mu.Unlock()

	if queued {
		t.Fatal("enqueueOrDeferLocked queued a task for a buffer that does not exist; it would carry nil records into mergeBatches and trip markFlushFailure on a healthy system (#1008 B2)")
	}
	if deferred {
		t.Fatal("a vanished buffer was reported as deferred, so the drainer would keep coming back for it forever")
	}
	if stillMarked {
		t.Fatal("the deferred marker for a vanished buffer was not cleared")
	}
	if buf.HasFlushFailure() {
		t.Fatal("HasFlushFailure is set after the drainer met a vanished buffer; this would trigger a periodic WAL recovery pass and keep CloseFlushedCleanly false for the process lifetime (#1008 B2)")
	}
}

// TestRefusedWriteLeavesNoPhantomBufferEntry covers the map leak the drainer
// would otherwise walk over.
//
// #1017's swept check sat AFTER the buffer-initialisation block, so a write
// refused post-Close had already created bufferStartTimes, bufferRecordCounts
// and bufferSchemas entries with no matching shard.buffers entry. The age sweep
// and the next-deadline computation both iterate bufferStartTimes.
func TestRefusedWriteLeavesNoPhantomBufferEntry(t *testing.T) {
	store := newReleasableBackend()
	store.releaseAll()
	cfg := drainerConfig()
	cfg.ShardCount = 1
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	buf.SetCloseBudget(30 * time.Second)

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Every shard is swept, so this write is refused.
	_ = buf.WriteColumnarDirect(context.Background(), "db", "phantom", makeColumns(4))

	shard := buf.getShard("db/phantom")
	shard.mu.RLock()
	_, hasBuffer := shard.buffers["db/phantom"]
	_, hasStart := shard.bufferStartTimes["db/phantom"]
	_, hasCount := shard.bufferRecordCounts["db/phantom"]
	_, hasSchema := shard.bufferSchemas["db/phantom"]
	shard.mu.RUnlock()

	if hasBuffer {
		t.Fatal("a refused write appended to a swept shard")
	}
	if hasStart || hasCount || hasSchema {
		t.Fatalf("a refused write left phantom map entries (start=%v count=%v schema=%v) with no buffer; the age sweep and the next-flush-deadline computation both walk bufferStartTimes",
			hasStart, hasCount, hasSchema)
	}
}

// TestDrainer_VanishedPickDoesNotStarveTheRest is the H1(ii) regression, and it
// is deterministic rather than a race.
//
// oldestDeferred picks a key under a read lock; drainDeferredOnce then takes the
// write lock to act on it. In between, that buffer can be removed by the age
// sweep, a schema-change flush, or FlushAll. enqueueOrDeferLocked then reports
// "not queued" — and if the pass ENDS there, every other deferred buffer stays
// deferred, because a buffer removed by one of those paths never touches
// flushQueue and so produces no dequeue signal to wake the drainer again. The
// drain silently reverts to pre-#1008 behaviour: wait for the age sweep or Close.
//
// Set up directly: the oldest deferred key has no buffer, a younger one does.
// Draining must skip the first and still flush the second.
func TestDrainer_VanishedPickDoesNotStarveTheRest(t *testing.T) {
	store := newReleasableBackend()
	store.releaseAll() // writes complete immediately
	cfg := drainerConfig()
	cfg.ShardCount = 1 // one shard, so ordering is unambiguous
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	buf.SetCloseBudget(30 * time.Second)
	defer func() { _ = buf.Close() }()

	const ghost, real = "db/ghost", "db/real"
	shard := buf.getShard(ghost)
	if buf.getShard(real) != shard {
		t.Skip("keys hashed to different shards; this test needs them together")
	}

	batch, _, err := buf.convertColumnsToTyped("real", makeColumns(10))
	if err != nil {
		t.Fatalf("build batch: %v", err)
	}

	shard.mu.Lock()
	// The OLDEST deferred key, with no buffer — the vanished pick.
	shard.deferredKeys[ghost] = struct{}{}
	shard.bufferStartTimes[ghost] = time.Now().UTC().Add(-time.Hour)
	shard.bufferRecordCounts[ghost] = 500
	// A younger deferred key that is real and must still be flushed.
	shard.deferredKeys[real] = struct{}{}
	shard.buffers[real] = []interface{}{batch}
	shard.bufferStartTimes[real] = time.Now().UTC().Add(-time.Minute)
	shard.bufferRecordCounts[real] = 10
	shard.mu.Unlock()

	buf.drainDeferredOnce()

	shard.mu.RLock()
	_, ghostStillDeferred := shard.deferredKeys[ghost]
	_, realStillDeferred := shard.deferredKeys[real]
	shard.mu.RUnlock()

	if ghostStillDeferred {
		t.Fatal("the vanished key is still marked deferred; the drainer would keep selecting it")
	}
	if realStillDeferred {
		t.Fatal("the younger deferred buffer was never enqueued: the pass ended on the vanished pick instead of moving past it, so every other deferred buffer is stranded until the age sweep or Close (#1008 H1ii)")
	}
	if buf.HasFlushFailure() {
		t.Fatal("a flush failure was recorded; the vanished key produced a task with no records")
	}
}
