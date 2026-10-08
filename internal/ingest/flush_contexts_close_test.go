package ingest

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// -----------------------------------------------------------------------------
// Backends
// -----------------------------------------------------------------------------

// ctxHonouringBackend sleeps for `delay` on every write but returns early with
// ctx.Err() if its context is cancelled first. It records how many writes were
// cut short, which is what distinguishes "the flush was aborted" from "the
// flush completed".
//
// This matters because LocalBackend.Write ignores its context entirely, so a
// test built on the local backend cannot observe cancellation at all.
type ctxHonouringBackend struct {
	delay     time.Duration
	completed atomic.Int64
	cancelled atomic.Int64
	started   atomic.Int64
}

func (c *ctxHonouringBackend) Write(ctx context.Context, _ string, _ []byte) error {
	c.started.Add(1)
	select {
	case <-time.After(c.delay):
		c.completed.Add(1)
		return nil
	case <-ctx.Done():
		c.cancelled.Add(1)
		return ctx.Err()
	}
}

func (c *ctxHonouringBackend) WriteReader(ctx context.Context, path string, r io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, r)
	return c.Write(ctx, path, nil)
}
func (c *ctxHonouringBackend) Read(context.Context, string) ([]byte, error) { return nil, nil }
func (c *ctxHonouringBackend) ReadTo(context.Context, string, io.Writer) error {
	return nil
}
func (c *ctxHonouringBackend) List(context.Context, string) ([]string, error) { return nil, nil }
func (c *ctxHonouringBackend) Delete(context.Context, string) error           { return nil }
func (c *ctxHonouringBackend) Exists(context.Context, string) (bool, error)   { return false, nil }
func (c *ctxHonouringBackend) Close() error                                   { return nil }
func (c *ctxHonouringBackend) Type() string                                   { return "mock-ctx-honouring" }
func (c *ctxHonouringBackend) ConfigJSON() string                             { return "{}" }
func (c *ctxHonouringBackend) ReadToAt(context.Context, string, io.Writer, int64) error {
	return nil
}
func (c *ctxHonouringBackend) StatFile(context.Context, string) (int64, error) { return -1, nil }
func (c *ctxHonouringBackend) AppendReader(context.Context, string, io.Reader, int64) error {
	return nil
}

// countingBackend records every successful write's byte count and never fails.
type countingBackend struct {
	delay  time.Duration
	writes atomic.Int64
}

type gatedBackend struct {
	countingBackend
	started chan struct{}
	release chan struct{}
}

func (g *gatedBackend) Write(ctx context.Context, path string, data []byte) error {
	g.started <- struct{}{}
	select {
	case <-g.release:
		g.writes.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gatedBackend) WriteReader(ctx context.Context, path string, r io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, r)
	return g.Write(ctx, path, nil)
}

func (c *countingBackend) Write(context.Context, string, []byte) error {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.writes.Add(1)
	return nil
}
func (c *countingBackend) WriteReader(ctx context.Context, path string, r io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, r)
	return c.Write(ctx, path, nil)
}
func (c *countingBackend) Read(context.Context, string) ([]byte, error)    { return nil, nil }
func (c *countingBackend) ReadTo(context.Context, string, io.Writer) error { return nil }
func (c *countingBackend) List(context.Context, string) ([]string, error)  { return nil, nil }
func (c *countingBackend) Delete(context.Context, string) error            { return nil }
func (c *countingBackend) Exists(context.Context, string) (bool, error)    { return false, nil }
func (c *countingBackend) Close() error                                    { return nil }
func (c *countingBackend) Type() string                                    { return "mock-counting" }
func (c *countingBackend) ConfigJSON() string                              { return "{}" }
func (c *countingBackend) ReadToAt(context.Context, string, io.Writer, int64) error {
	return nil
}
func (c *countingBackend) StatFile(context.Context, string) (int64, error) { return -1, nil }
func (c *countingBackend) AppendReader(context.Context, string, io.Reader, int64) error {
	return nil
}

// -----------------------------------------------------------------------------
// #1006 — the flush timeout must start at dequeue, not at enqueue
// -----------------------------------------------------------------------------

// TestFlushTimeout_StartsAtDequeue pins #1006.
//
// With one worker, a per-flush time of `delay` and a queue several tasks deep,
// a timeout created when the task was ENQUEUED is consumed while the task waits
// its turn. The task at the back of the queue then reaches the worker with
// almost none of flushTimeout left, the storage write fails with
// "context deadline exceeded", and flushRecordsAsync drops the batch as though
// storage had failed — which with the WAL disabled is loss.
//
// Pre-fix: the later tasks are cancelled (b.cancelled > 0) and the buffer
// records flush failures. Post-fix: every task gets the full flushTimeout
// measured from the moment a worker picked it up, so all of them complete.
func TestFlushTimeout_StartsAtDequeue(t *testing.T) {
	const (
		batches     = 6
		perBatch    = 10
		flushDelay  = 400 * time.Millisecond
		flushBudget = 1 // second — 6 x 400ms = 2.4s of queue wait, well past it
	)

	store := &ctxHonouringBackend{delay: flushDelay}
	cfg := &config.IngestConfig{
		MaxBufferSize:       perBatch,
		MaxBufferAgeMS:      3_600_000, // never age-flush
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      batches + 2,
		FlushTimeoutSeconds: flushBudget,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	// The close budget must comfortably exceed the time the whole backlog needs
	// (batches x flushDelay = 2.4s). Without this the budget defaults to
	// flushTimeout — 1s here, chosen to be shorter than the queue wait so the
	// pre-fix failure is unambiguous — and Close would cut the drain off, making
	// this test flaky for a reason unrelated to what it measures.
	buf.SetCloseBudget(30 * time.Second)

	for i := 0; i < batches; i++ {
		if err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(perBatch)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := store.cancelled.Load(); got != 0 {
		t.Fatalf("%d flushes were cancelled by an expired context; the timeout is being consumed while the task waits in the queue (#1006)", got)
	}
	if got := store.completed.Load(); got < batches {
		t.Fatalf("only %d of %d flushes completed; queued tasks are expiring before a worker reaches them (#1006)", got, batches)
	}
	if buf.HasFlushFailure() {
		t.Fatal("a flush failure was recorded although storage never failed; a queueing delay is being reported as a storage failure (#1006)")
	}
}

// -----------------------------------------------------------------------------
// #1007 — Close must not abandon queued or in-flight work
// -----------------------------------------------------------------------------

// TestClose_FlushesQueuedTasks pins the #1007 headline.
//
// Close() used to cancel the workers and then DISCARD whatever was still in the
// flush queue, "in favour of WAL replay". Those records were already removed
// from shard.buffers at enqueue time, so nothing else would ever write them;
// with the shipped wal.enabled=false they were simply lost on every graceful
// stop under load.
//
// This replaces TestArrowBuffer_CloseNotCleanWhenQueuedFlushesAbandoned, which
// pinned the old contract (abandoned => unclean). The #803 guarantee that test
// protected — a Close that loses data must report unclean — is now pinned by
// TestClose_DrainedTaskFailureMarksUnclean below.
func TestClose_FlushesQueuedTasks(t *testing.T) {
	const (
		batches  = 8
		perBatch = 5
	)

	store := &countingBackend{delay: 150 * time.Millisecond}
	cfg := &config.IngestConfig{
		MaxBufferSize:       perBatch,
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      batches + 2,
		FlushTimeoutSeconds: 10,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))

	for i := 0; i < batches; i++ {
		if err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(perBatch)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Let the queue build up behind the slow backend.
	time.Sleep(100 * time.Millisecond)

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got, want := buf.totalRecordsWritten.Load(), int64(batches*perBatch); got != want {
		t.Fatalf("Close wrote %d of %d records; queued flush tasks were abandoned instead of flushed (#1007)", got, want)
	}
	if !buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is false although every record reached storage; the WAL would be retained and replayed on every start")
	}
}

func TestClose_DrainsConcurrentFlushRecordMetrics(t *testing.T) {
	const (
		workers   = 4
		batches   = 8
		perBatch  = 5
		records   = int64(perBatch)
		waitLimit = 2 * time.Second
	)
	store := &gatedBackend{
		started: make(chan struct{}, batches),
		release: make(chan struct{}, batches),
	}
	cfg := &config.IngestConfig{
		MaxBufferSize:       perBatch,
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        workers,
		FlushQueueSize:      batches + 2,
		FlushTimeoutSeconds: 10,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	buf.SetCloseBudget(10 * time.Second)
	for i := 0; i < batches; i++ {
		if err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(perBatch)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	deadline := time.After(waitLimit)
	for i := 0; i < workers; i++ {
		select {
		case <-store.started:
		case <-deadline:
			t.Fatalf("only %d flush workers entered storage", i)
		}
	}
	queued := records * int64(batches-workers)
	inflight := records * workers
	if got := metrics.Get().Snapshot()["buffer_records_queued"]; got != queued {
		t.Fatalf("queued gauge = %v, want %d while workers are blocked", got, queued)
	}
	if got := metrics.Get().Snapshot()["buffer_records_inflight"]; got != inflight {
		t.Fatalf("in-flight gauge = %v, want %d while workers are blocked", got, inflight)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- buf.Close() }()
	for !buf.closing.Load() {
		select {
		case <-deadline:
			t.Fatal("Close did not begin draining")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	for i := 0; i < batches; i++ {
		store.release <- struct{}{}
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(waitLimit):
		t.Fatal("Close did not finish draining gated flushes")
	}
	if got := buf.queuedRecords.Load(); got != 0 {
		t.Fatalf("queued counter = %d after Close, want 0", got)
	}
	if got := buf.inflightRecords.Load(); got != 0 {
		t.Fatalf("in-flight counter = %d after Close, want 0", got)
	}
	snapshot := metrics.Get().Snapshot()
	if got := snapshot["buffer_records_queued"]; got != int64(0) {
		t.Fatalf("queued gauge = %v after Close, want 0", got)
	}
	if got := snapshot["buffer_records_inflight"]; got != int64(0) {
		t.Fatalf("in-flight gauge = %v after Close, want 0", got)
	}
	if !buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is false after all gated flushes completed")
	}
}

// TestClose_DrainedTaskFailureMarksUnclean keeps the #803 guarantee against the
// new code path: Close now flushes drained tasks, so a drained task whose flush
// FAILS must still mark the close unclean. Otherwise the shutdown WAL purge
// deletes the only remaining copy.
func TestClose_DrainedTaskFailureMarksUnclean(t *testing.T) {
	const perBatch = 5

	store := &failingStorageBackend{err: errors.New("storage unavailable")}
	cfg := &config.IngestConfig{
		MaxBufferSize:       perBatch,
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      8,
		FlushTimeoutSeconds: 5,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))

	for i := 0; i < 4; i++ {
		_ = buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(perBatch))
	}

	_ = buf.Close()

	if buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is true although every flush failed; the shutdown WAL purge would delete the only copy of those records (#803)")
	}
}

// TestClose_DoesNotCancelInFlightFlush pins that a storage write already in
// progress survives Close.
//
// Every flush context used to derive from b.ctx, which Close cancels. The
// records had already been removed from the buffer by then, so cancelling the
// write lost them. Flush I/O now runs on flushParent, which b.cancel() cannot
// reach; Close waits for the write instead of killing it.
func TestClose_DoesNotCancelInFlightFlush(t *testing.T) {
	const perBatch = 10

	store := &ctxHonouringBackend{delay: 600 * time.Millisecond}
	cfg := &config.IngestConfig{
		MaxBufferSize:       perBatch,
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      4,
		FlushTimeoutSeconds: 10,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))

	if err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(perBatch)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Wait until the worker is actually inside storage.Write, then Close.
	deadline := time.Now().Add(2 * time.Second)
	for store.started.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if store.started.Load() == 0 {
		t.Fatal("flush never reached the storage backend")
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := store.cancelled.Load(); got != 0 {
		t.Fatalf("%d in-flight flush(es) were cancelled by Close; those records were already out of the buffer, so cancelling loses them (#1007)", got)
	}
	if got := store.completed.Load(); got == 0 {
		t.Fatal("no flush completed; Close aborted the write instead of waiting for it (#1007)")
	}
}

// TestClose_ConcurrentWritesLoseNothing is the -race shape of the stranded-
// writer race: writers run concurrently with Close, and every write must either
// be refused with ErrBufferClosing or have its rows accounted for. A write that
// returns nil while its rows reach neither storage nor the WAL-only count is the
// bug (#803, #1007).
func TestClose_ConcurrentWritesLoseNothing(t *testing.T) {
	const (
		writers  = 8
		perWrite = 3
		rounds   = 12
	)

	store := &countingBackend{}
	cfg := &config.IngestConfig{
		MaxBufferSize:       perWrite * 2,
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          4,
		FlushWorkers:        2,
		FlushQueueSize:      32,
		FlushTimeoutSeconds: 5,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))

	var (
		wg       sync.WaitGroup
		accepted atomic.Int64
		refused  atomic.Int64
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(perWrite))
				switch {
				case err == nil:
					accepted.Add(perWrite)
				case errors.Is(err, ErrBufferClosing):
					refused.Add(perWrite)
				default:
					t.Errorf("unexpected write error: %v", err)
					return
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	closeErr := buf.Close()
	wg.Wait()

	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}

	// Every accepted record must be accounted for: written to storage, or
	// counted as WAL-only. Nothing may simply vanish.
	written := buf.totalRecordsWritten.Load()
	// Refused writes also bump walOnlyRecords, and their records were never in
	// `accepted`. Subtract them, or the assertion carries slack equal to the
	// refusal count and one refused write would mask one lost accepted write.
	walOnly := buf.walOnlyRecords.Load() - refused.Load()
	if walOnly < 0 {
		walOnly = 0
	}
	if written+walOnly < accepted.Load() {
		t.Fatalf("accepted %d records but only %d written + %d WAL-only (excluding %d refused) = %d accounted for; %d vanished",
			accepted.Load(), written, walOnly, refused.Load(), written+walOnly, accepted.Load()-(written+walOnly))
	}
	if refused.Load() > 0 && buf.CloseFlushedCleanly() {
		t.Fatal("writes were refused after their shard was swept, yet the close reports clean; those records exist only in the WAL, which the purge would then delete")
	}
}

// TestSweptShardRefusalMarksUnclean is the narrow regression for the same
// accounting rule, without the concurrency: a refused write's record is already
// in the WAL and is NOT in the buffer, so the close must not report clean.
func TestSweptShardRefusalMarksUnclean(t *testing.T) {
	store := &countingBackend{}
	cfg := closeTestConfig()
	cfg.ShardCount = 1
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Every shard is swept now, so this write must be refused.
	err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(4))
	if !errors.Is(err, ErrBufferClosing) {
		t.Fatalf("write after Close returned %v; want ErrBufferClosing so the caller learns the record was not accepted", err)
	}
	if buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is true after a refused write; the record exists only in the WAL and the shutdown purge would delete it")
	}
}

// TestSchemaChangeFlush_SurvivesRequestCancel pins that a client disconnecting
// does not abort a schema-change flush.
//
// The schema-evolution flush used to run on the REQUEST context with no
// timeout, so one client going away aborted a flush of rows other clients had
// already been acknowledged for. The I/O now runs on flushParent; the request
// context still governs the loop, so cancellation is observed between
// iterations rather than mid-write.
func TestSchemaChangeFlush_SurvivesRequestCancel(t *testing.T) {
	store := &ctxHonouringBackend{delay: 300 * time.Millisecond}
	cfg := &config.IngestConfig{
		MaxBufferSize:       1_000_000, // never size-flush
		MaxBufferAgeMS:      3_600_000, // never age-flush
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      4,
		FlushTimeoutSeconds: 10,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	defer func() { _ = buf.Close() }()

	// First schema.
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(3)); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Second write with a DIFFERENT column set triggers the schema-change
	// flush of the first batch. Its request context is cancelled while that
	// flush is in progress.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel once the flush has entered the backend.
		deadline := time.Now().Add(2 * time.Second)
		for store.started.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()

	cols := makeColumns(3)
	cols["extra_field"] = []interface{}{"a", "b", "c"}
	_ = buf.WriteColumnarDirect(ctx, "db", "m1", cols)

	if got := store.cancelled.Load(); got != 0 {
		t.Fatalf("%d schema-change flush(es) were aborted by the requesting client's cancellation; those rows belong to earlier, already-acknowledged writes (#1007)", got)
	}
	if got := store.completed.Load(); got == 0 {
		t.Fatal("the schema-change flush never completed")
	}
}

// TestAgedSweep_StopsOnClosing_ReleasesShardLock is the deadlock regression for
// the aged sweep's early exit.
//
// flushAgedBuffers holds shard.mu across its whole loop (released only inside
// flushBufferLocked, around the I/O). Stopping the sweep on b.closing with a
// bare `return` would leak that lock, and Close()'s own shard loop — plus every
// later write to that shard — would block on it forever. The test fails by
// timing out if the unlock is missing.
func TestAgedSweep_StopsOnClosing_ReleasesShardLock(t *testing.T) {
	store := &countingBackend{delay: 80 * time.Millisecond}
	cfg := &config.IngestConfig{
		MaxBufferSize:       1_000_000, // only the age trigger fires
		MaxBufferAgeMS:      50,
		Compression:         "snappy",
		ShardCount:          8,
		FlushWorkers:        1,
		FlushQueueSize:      8,
		FlushTimeoutSeconds: 5,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))

	// Spread buffers across shards so the sweep has several to walk.
	for i := 0; i < 16; i++ {
		meas := "m" + string(rune('a'+i))
		if err := buf.WriteColumnarDirect(context.Background(), "db", meas, makeColumns(2)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Let the buffers age so the sweep is running when Close arrives.
	time.Sleep(120 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- buf.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Close did not return within 20s: the aged sweep stopped on b.closing without releasing shard.mu, so Close's shard loop is deadlocked on it")
	}
}

// TestCloseBudget_BoundsTheWaitForWorkers pins the Blocker the plan review
// found: the close budget has to cover the wait for in-flight flushes, not just
// the flush steps after it.
//
// Flush I/O no longer derives from b.ctx, so b.cancel() cannot stop a write in
// progress. If Close then waits for it unbounded, it can exceed the shutdown
// budget — and the coordinator checks its deadline only BETWEEN components, so
// the wal-purge and wal components are skipped. wal.Writer.Close is what
// performs the WAL's final drain-and-sync, so a hang here retains a WAL that is
// missing its newest entries: worse than the loss being fixed.
//
// Close must therefore return within its budget, cancelling flushParent as a
// last resort, and report the close UNCLEAN because it could not confirm the
// records landed.
func TestCloseBudget_BoundsTheWaitForWorkers(t *testing.T) {
	const budget = 500 * time.Millisecond

	// Backend far slower than the budget, but context-aware so the last-resort
	// cancel can actually release it.
	store := &ctxHonouringBackend{delay: 30 * time.Second}
	cfg := &config.IngestConfig{
		MaxBufferSize:       5,
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      4,
		FlushTimeoutSeconds: 120, // deliberately >> budget
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	buf.SetCloseBudget(budget)

	if err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(5)); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for store.started.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	done := make(chan struct{})
	go func() { _ = buf.Close(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not respect its budget and is still waiting for an in-flight flush; the coordinator would skip the wal-purge and wal components, retaining a WAL missing its newest entries")
	}

	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("Close took %v with a %v budget; the budget must cover the wait for in-flight flushes", elapsed, budget)
	}
	if buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is true although the budget expired with a flush unconfirmed; the WAL must be retained")
	}
}

// TestSetCloseBudget_IgnoresNonPositive covers the config edge: server.
// shutdown_timeout can be zero or negative, and the fallback must be a real
// duration rather than an instantly-expired deadline.
func TestSetCloseBudget_IgnoresNonPositive(t *testing.T) {
	cfg := closeTestConfig()
	buf := NewArrowBuffer(cfg, &countingBackend{}, zerolog.New(io.Discard))
	defer func() { _ = buf.Close() }()

	for _, d := range []time.Duration{0, -5 * time.Second} {
		buf.SetCloseBudget(d)
		if got := buf.closeDeadlineBudget(); got <= 0 {
			t.Fatalf("SetCloseBudget(%v) left a non-positive budget %v; Close would start already expired", d, got)
		}
	}

	buf.SetCloseBudget(7 * time.Second)
	if got := buf.closeDeadlineBudget(); got != 7*time.Second {
		t.Fatalf("closeDeadlineBudget() = %v, want 7s", got)
	}
}

// blockFirstBackend blocks the first write until its context is done, then
// serves every later write instantly. It lets a test force the close budget to
// expire on one stuck flush while still asking whether the OTHER buffers got
// written afterwards.
type blockFirstBackend struct {
	first     sync.Once
	entered   atomic.Int64
	completed atomic.Int64
	cancelled atomic.Int64
}

func (b *blockFirstBackend) Write(ctx context.Context, _ string, _ []byte) error {
	b.entered.Add(1)
	blocked := false
	b.first.Do(func() { blocked = true })
	if blocked {
		<-ctx.Done()
		return ctx.Err()
	}
	// The ctx check is what makes this backend able to DETECT a context that
	// arrived already cancelled. Without it the fast path would succeed
	// regardless and the test would pass even with the bug present — the
	// parquet encode upstream does not consult its context either, so this is
	// the only place a born-cancelled flush context is observable.
	if err := ctx.Err(); err != nil {
		b.cancelled.Add(1)
		return err
	}
	b.completed.Add(1)
	return nil
}
func (b *blockFirstBackend) WriteReader(ctx context.Context, path string, r io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, r)
	return b.Write(ctx, path, nil)
}
func (b *blockFirstBackend) Read(context.Context, string) ([]byte, error)    { return nil, nil }
func (b *blockFirstBackend) ReadTo(context.Context, string, io.Writer) error { return nil }
func (b *blockFirstBackend) List(context.Context, string) ([]string, error)  { return nil, nil }
func (b *blockFirstBackend) Delete(context.Context, string) error            { return nil }
func (b *blockFirstBackend) Exists(context.Context, string) (bool, error)    { return false, nil }
func (b *blockFirstBackend) Close() error                                    { return nil }
func (b *blockFirstBackend) Type() string                                    { return "mock-block-first" }
func (b *blockFirstBackend) ConfigJSON() string                              { return "{}" }
func (b *blockFirstBackend) ReadToAt(context.Context, string, io.Writer, int64) error {
	return nil
}
func (b *blockFirstBackend) StatFile(context.Context, string) (int64, error) { return -1, nil }
func (b *blockFirstBackend) AppendReader(context.Context, string, io.Reader, int64) error {
	return nil
}

// TestClose_BudgetExpiryStillFlushesRemainingBuffers pins the subtler half of
// the close budget.
//
// When the budget expires, Close cancels flushParent to unstick a worker blocked
// in a storage write. Close's own remaining work must NOT be collateral damage:
// if the buffers it still has to flush derive their contexts from that cancelled
// parent, every one of them starts already Done and Close writes *nothing* —
// worse than before this change, where the close loop used context.Background()
// and b.cancel() could not reach it. With the WAL disabled, which is the shipped
// default, that is loss of everything still buffered.
//
// Setup: one flush is stuck in the backend so the budget expires on the wait for
// workers; a second measurement sits in memory and is fast to write. After the
// cancel, that second buffer must still reach storage.
func TestClose_BudgetExpiryStillFlushesRemainingBuffers(t *testing.T) {
	const perBatch = 5

	store := &blockFirstBackend{}
	cfg := &config.IngestConfig{
		MaxBufferSize:       perBatch,
		MaxBufferAgeMS:      3_600_000, // only the size trigger and Close flush
		Compression:         "snappy",
		ShardCount:          8,
		FlushWorkers:        1,
		FlushQueueSize:      4,
		FlushTimeoutSeconds: 120, // >> budget, so the budget is what bounds Close
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	buf.SetCloseBudget(400 * time.Millisecond)

	// m_stuck crosses MaxBufferSize, so it is extracted and handed to the worker,
	// which then blocks in the backend.
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m_stuck", makeColumns(perBatch)); err != nil {
		t.Fatalf("write m_stuck: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for store.entered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if store.entered.Load() == 0 {
		t.Fatal("the stuck flush never reached the backend")
	}

	// m_buffered stays under MaxBufferSize, so it is still in shard.buffers and
	// only Close will flush it.
	if err := buf.WriteColumnarDirect(context.Background(), "db", "m_buffered", makeColumns(perBatch-2)); err != nil {
		t.Fatalf("write m_buffered: %v", err)
	}

	_ = buf.Close()

	if store.completed.Load() == 0 {
		t.Fatalf("Close wrote nothing after its budget expired (%d flush(es) arrived already cancelled): the remaining buffers inherited the cancelled flush parent, so every close-time flush started already cancelled. With the WAL off those records are lost (#1007)", store.cancelled.Load())
	}
	if got := buf.totalRecordsWritten.Load(); got < int64(perBatch-2) {
		t.Fatalf("Close wrote %d records; the buffered measurement (%d records) should still have been flushed after the budget expired", got, perBatch-2)
	}
}

// TestAgedFlush_SurvivesClose pins the fourth seam named in #1007.
//
// The age-triggered flush built its context from b.ctx, which Close cancels. By
// the time the storage write is running, flushBufferLocked has already deleted
// the buffer entry, so cancelling it loses those rows outright — and with the
// WAL disabled, which is the shipped default, there is no second copy.
//
// The sweep now runs on flushParent, so Close waits for an aged flush in
// progress instead of killing it.
func TestAgedFlush_SurvivesClose(t *testing.T) {
	store := &ctxHonouringBackend{delay: 500 * time.Millisecond}
	cfg := &config.IngestConfig{
		MaxBufferSize:       1_000_000, // never size-flush: the age trigger is the only one
		MaxBufferAgeMS:      60,
		Compression:         "snappy",
		ShardCount:          1,
		FlushWorkers:        1,
		FlushQueueSize:      4,
		FlushTimeoutSeconds: 30,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	buf.SetCloseBudget(30 * time.Second)

	if err := buf.WriteColumnarDirect(context.Background(), "db", "aged", makeColumns(8)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Wait until the aged sweep is inside the storage write, then Close.
	deadline := time.Now().Add(3 * time.Second)
	for store.started.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if store.started.Load() == 0 {
		t.Fatal("the aged flush never reached the storage backend")
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := store.cancelled.Load(); got != 0 {
		t.Fatalf("%d aged flush(es) were cancelled by Close; flushBufferLocked had already deleted the buffer entry, so those rows are lost (#1007)", got)
	}
	if got := store.completed.Load(); got == 0 {
		t.Fatal("the aged flush never completed; Close aborted it instead of waiting")
	}
	if !buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is false although the aged flush completed")
	}
}

// TestClose_NoHandoffWindow pins the third seam in #1007 as it now stands.
//
// The seam was: a writer between shard.mu.Unlock() and the flush-queue send
// holds records that are in no buffer, no queue and no flush, so a Close that
// drains the queue in that window strands the writer's subsequent send — the
// records are counted nowhere and the close still reports clean.
//
// #1017 closed that with a pendingEnqueueRecords counter Close waited on. #966
// route 1 then moved the send UNDER shard.mu, which removes the window itself:
// a writer either completes its enqueue before Close's shard loop takes that
// shard's lock, or blocks on the lock and then sees shard.swept and is refused
// with ErrBufferClosing. There is no third state, so the counter was removed.
//
// This asserts the invariant that replaced it: under concurrent writers racing
// Close, nothing is left in the queue and every accepted record is accounted
// for. It is not a race-provoking test — it cannot be, now that the window is
// gone — it is a guard against the send escaping shard.mu again.
func TestClose_NoHandoffWindow(t *testing.T) {
	const (
		writers  = 12
		perWrite = 4
		rounds   = 25
	)

	store := &countingBackend{}
	cfg := &config.IngestConfig{
		MaxBufferSize:       perWrite, // every write triggers a flush → maximum handoff traffic
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          4,
		FlushWorkers:        2,
		FlushQueueSize:      64,
		FlushTimeoutSeconds: 10,
		DataPageVersion:     "2.0",
	}
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	buf.SetCloseBudget(30 * time.Second)

	var (
		wg       sync.WaitGroup
		accepted atomic.Int64
		refused  atomic.Int64
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				err := buf.WriteColumnarDirect(context.Background(), "db", "m1", makeColumns(perWrite))
				switch {
				case err == nil:
					accepted.Add(perWrite)
				case errors.Is(err, ErrBufferClosing):
					refused.Add(perWrite)
				default:
					t.Errorf("unexpected write error: %v", err)
					return
				}
			}
		}()
	}

	time.Sleep(15 * time.Millisecond)
	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()

	if n := len(buf.flushQueue); n != 0 {
		t.Fatalf("%d flush task(s) left in the queue after Close; a send escaped shard.mu, so Close's shard loop is no longer the barrier it relies on (#1007)", n)
	}
	written := buf.totalRecordsWritten.Load()
	walOnly := buf.walOnlyRecords.Load() - refused.Load()
	if walOnly < 0 {
		walOnly = 0
	}
	if written+walOnly < accepted.Load() {
		t.Fatalf("accepted %d records but only %d written + %d WAL-only = %d accounted for; %d vanished",
			accepted.Load(), written, walOnly, written+walOnly, accepted.Load()-(written+walOnly))
	}
}
