package ingest

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// #1760 (a1): the force primitive. The ledger is unwired, so every test here
// drives ArrowBuffer.ForceFlushKey directly.

// gateBackend routes each write by the path's measurement: "blocker*" blocks
// until released, "failer*" fails, "panicker*" panics, anything else succeeds
// at once. The gate lets a test hold a worker busy while it fills the queue.
type gateBackend struct {
	release  chan struct{}
	once     sync.Once
	blocked  atomic.Int64
	mu       sync.Mutex
	written  map[string]int // measurement -> files written
	writeErr error
}

func newGateBackend() *gateBackend {
	return &gateBackend{release: make(chan struct{}), written: map[string]int{}, writeErr: errors.New("injected write failure")}
}

func (g *gateBackend) releaseAll() { g.once.Do(func() { close(g.release) }) }

func (g *gateBackend) files(measurement string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.written[measurement]
}

func (g *gateBackend) Write(ctx context.Context, path string, _ []byte) error {
	parts := strings.Split(path, "/")
	meas := ""
	if len(parts) > 1 {
		meas = parts[1]
	}
	switch {
	case strings.HasPrefix(meas, "blocker"):
		g.blocked.Add(1)
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	case strings.HasPrefix(meas, "failer"):
		return g.writeErr
	case strings.HasPrefix(meas, "panicker"):
		panic("injected backend panic")
	}
	g.mu.Lock()
	g.written[meas]++
	g.mu.Unlock()
	return nil
}
func (g *gateBackend) WriteReader(ctx context.Context, path string, rd io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, rd)
	return g.Write(ctx, path, nil)
}
func (g *gateBackend) Read(context.Context, string) ([]byte, error)    { return nil, nil }
func (g *gateBackend) ReadTo(context.Context, string, io.Writer) error { return nil }
func (g *gateBackend) List(context.Context, string) ([]string, error)  { return nil, nil }
func (g *gateBackend) Delete(context.Context, string) error            { return nil }
func (g *gateBackend) Exists(context.Context, string) (bool, error)    { return false, nil }
func (g *gateBackend) Close() error                                    { return nil }
func (g *gateBackend) Type() string                                    { return "mock-gate" }
func (g *gateBackend) ConfigJSON() string                              { return "{}" }
func (g *gateBackend) ReadToAt(context.Context, string, io.Writer, int64) error {
	return nil
}
func (g *gateBackend) StatFile(context.Context, string) (int64, error) { return -1, nil }
func (g *gateBackend) AppendReader(context.Context, string, io.Reader, int64) error {
	return nil
}

// ledgerClock is the ledger's injected clock.
type ledgerClock struct{ ns atomic.Int64 }

func newLedgerClock() *ledgerClock {
	c := &ledgerClock{}
	c.ns.Store(time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}
func (c *ledgerClock) Now() time.Time               { return time.Unix(0, c.ns.Load()) }
func (c *ledgerClock) Advance(d time.Duration)      { c.ns.Add(int64(d)) }
func (c *ledgerClock) At(d time.Duration) time.Time { return c.Now().Add(d) }

// forceConfig: only the size trigger (100 rows) and explicit calls flush; the
// age sweep is an hour away.
func forceConfig(workers, queue int) *config.IngestConfig {
	return &config.IngestConfig{
		MaxBufferSize:       100,
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          4,
		FlushWorkers:        workers,
		FlushQueueSize:      queue,
		FlushTimeoutSeconds: 30,
		DataPageVersion:     "2.0",
	}
}

type forceFixture struct {
	t     *testing.T
	buf   *ArrowBuffer
	store *gateBackend
	clock *ledgerClock
}

// newForceFixture builds a buffer with the ledger on and its clock injected
// before the first write. budget nil leaves the ledger off.
func newForceFixture(t *testing.T, cfg *config.IngestConfig, budget *ForceBudget) *forceFixture {
	t.Helper()
	store := newGateBackend()
	buf := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	buf.SetCloseBudget(10 * time.Second)
	f := &forceFixture{t: t, buf: buf, store: store, clock: newLedgerClock()}
	if budget != nil {
		buf.EnableKeyFlushLedger(*budget)
		buf.ledger.Load().now = f.clock.Now
	}
	t.Cleanup(func() {
		forceAfterScanHook = nil
		forceBeforeSendHook = nil
		store.releaseAll()
		_ = buf.Close()
	})
	return f
}

func (f *forceFixture) write(meas string, rows int) {
	f.t.Helper()
	if err := f.buf.WriteColumnarDirect(context.Background(), "db", meas, makeColumns(rows)); err != nil {
		f.t.Fatalf("write %s: %v", meas, err)
	}
}

func (f *forceFixture) buffered(key string) int {
	shard := f.buf.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return len(shard.buffers[key])
}

func (f *forceFixture) inFlight() int64 { return f.buf.ledger.Load().inFlight.Load() }

func (f *forceFixture) waitFor(what string, cond func() bool) {
	f.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			f.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// occupyWorker sends a size-triggered flush of db/blocker0 and waits until a
// worker is blocked in its write.
func (f *forceFixture) occupyWorker() {
	f.t.Helper()
	f.write("blocker0", 100)
	f.waitFor("a worker blocked on blocker0", func() bool { return f.store.blocked.Load() >= 1 })
}

func (f *forceFixture) force(key string, want func([]interface{}) bool) (ForceResult, time.Time) {
	return f.buf.ForceFlushKey(key, 5*time.Second, ForceRead, want)
}

func (f *forceFixture) expect(key string, want ForceResult) {
	f.t.Helper()
	if got, _ := f.force(key, nil); got != want {
		f.t.Fatalf("ForceFlushKey(%s) = %v, want %v", key, got, want)
	}
}

// assertNotDeferred: an attempt that did not force left no deferral behind
// and gave its slot back.
func (f *forceFixture) assertNotDeferred(deferredBefore int64, slotsBefore int64) {
	f.t.Helper()
	if n := f.buf.countDeferredBuffers(); n != 0 {
		f.t.Fatalf("%d buffer(s) marked deferred by a forced attempt; a forced attempt must never defer (R-L4)", n)
	}
	if got := f.buf.totalFlushDeferred.Load(); got != deferredBefore {
		f.t.Fatalf("totalFlushDeferred moved %d -> %d during a forced attempt", deferredBefore, got)
	}
	if got := f.inFlight(); got != slotsBefore {
		f.t.Fatalf("forced slots in flight = %d, want %d: a non-forced exit kept its slot", got, slotsBefore)
	}
}

func TestForceFlushKey_LedgerOffAndNonPositiveInterval(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 4), nil)
	f.write("m", 10)
	if got, _ := f.force("db/m", nil); got != LedgerOff {
		t.Fatalf("ledger never enabled: got %v, want ledger_off", got)
	}

	f.buf.EnableKeyFlushLedger(ForceBudget{})
	for _, interval := range []time.Duration{0, -time.Second} {
		if got, _ := f.buf.ForceFlushKey("db/m", interval, ForceWrite, nil); got != LedgerOff {
			t.Fatalf("interval %v: got %v, want ledger_off (no forcing without a positive interval)", interval, got)
		}
	}
	if f.buffered("db/m") == 0 {
		t.Fatal("a refused force flushed the key")
	}
}

// I23 (L-a): a plain call on a buffered, due key returns, and the forced
// flush stores the key's file and frees its slot.
func TestForceFlushKey_ForcesBufferedKey(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{})
	f.write("m", 10)

	type res struct {
		r   ForceResult
		due time.Time
	}
	done := make(chan res, 1)
	go func() {
		r, due := f.force("db/m", nil)
		done <- res{r, due}
	}()
	select {
	case got := <-done:
		if got.r != Forced || !got.due.IsZero() {
			t.Fatalf("got (%v, %v), want (forced, zero)", got.r, got.due)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ForceFlushKey did not return within 5 s: a lock taken under shard.mu deadlocked (countDeferredBuffers takes every shard's RLock)")
	}
	if f.buffered("db/m") != 0 {
		t.Fatal("the key is still buffered after Forced")
	}
	f.waitFor("the forced file", func() bool { return f.store.files("m") == 1 })
	f.waitFor("the forced slot to free", func() bool { return f.inFlight() == 0 })
	if d, ok := f.buf.ledger.Load().lastDetach("db/m"); !ok || d.id == 0 || !d.at.Equal(f.clock.Now()) {
		t.Fatalf("the forced detach was not recorded: %+v %v", d, ok)
	}
}

// I8: the interval runs from the key's last detach of ANY source, here the
// size trigger (D1) and FlushAll (D2), not only from forced ones.
func TestForceFlushKey_NaturalDetachStartsTheInterval(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{})

	// D1: the size trigger at t0.
	t0 := f.clock.Now()
	f.write("m", 100)
	f.waitFor("the size-triggered file", func() bool { return f.store.files("m") == 1 })
	f.write("m", 10)
	f.clock.Advance(time.Second)
	r, due := f.force("db/m", nil)
	if r != NotDue || !due.Equal(t0.Add(5*time.Second)) {
		t.Fatalf("1 s after a size-triggered detach: got (%v, %v), want (not_due, t0+5s)", r, due)
	}
	f.clock.Advance(4 * time.Second)
	f.expect("db/m", Forced)

	// D2: FlushAll at t1.
	f.waitFor("the forced file", func() bool { return f.store.files("m") == 2 })
	f.clock.Advance(time.Minute)
	t1 := f.clock.Now()
	f.write("m", 10)
	if err := f.buf.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	f.write("m", 10)
	f.clock.Advance(time.Second)
	r, due = f.force("db/m", nil)
	if r != NotDue || !due.Equal(t1.Add(5*time.Second)) {
		t.Fatalf("1 s after a FlushAll detach: got (%v, %v), want (not_due, t1+5s)", r, due)
	}
}

// I12: the empty-cleanup branch of extractBufferLocked detaches nothing, so it
// records nothing.
func TestKeyFlushLedger_EmptyCleanupRecordsNothing(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 4), &ForceBudget{})
	key := "db/empty"
	shard := f.buf.getShard(key)
	shard.mu.Lock()
	shard.buffers[key] = []interface{}{}
	e := f.buf.extractBufferLocked(shard, key)
	shard.mu.Unlock()
	if e != nil {
		t.Fatal("extractBufferLocked returned a flush for an empty buffer")
	}
	if d, ok := f.buf.ledger.Load().lastDetach(key); ok {
		t.Fatalf("the empty cleanup recorded a detach: %+v", d)
	}
}

// An existing but empty slice with a nil want: nothing to force, and above all
// no empty task ("no batches to merge" would set the flush-failure latch).
func TestForceFlushKey_EmptySliceIsNothingToForce(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 4), &ForceBudget{})
	key := "db/empty"
	shard := f.buf.getShard(key)
	shard.mu.Lock()
	shard.buffers[key] = []interface{}{}
	shard.bufferStartTimes[key] = time.Now()
	shard.mu.Unlock()

	f.expect(key, NothingToForce)
	if f.buf.HasFlushFailure() {
		t.Fatal("forcing an empty buffer recorded a flush failure")
	}
	if got := f.inFlight(); got != 0 {
		t.Fatalf("slot kept: %d", got)
	}
}

func TestForceFlushKey_NothingAndNotWanted(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{})
	never := func([]interface{}) bool { return false }

	f.expect("db/absent", NothingToForce)
	if r, _ := f.force("db/absent", never); r != NothingToForce {
		t.Fatalf("unbuffered key with a want: got %v, want nothing", r)
	}

	f.write("m", 10)
	var seen int
	r, _ := f.force("db/m", func(bs []interface{}) bool { seen = len(bs); return false })
	if r != NotWanted {
		t.Fatalf("want false: got %v, want not_wanted", r)
	}
	if seen == 0 {
		t.Fatal("want was not given the buffered batches")
	}
	if f.buffered("db/m") == 0 || f.inFlight() != 0 {
		t.Fatal("a not-wanted key was flushed, or its slot kept")
	}
	if r, _ := f.force("db/m", func([]interface{}) bool { return true }); r != Forced {
		t.Fatalf("want true: got %v, want forced", r)
	}
}

// I24, second half: want runs under the shard READ lock (writers excluded,
// other readers not).
func TestForceFlushKey_WantRunsUnderTheReadLock(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{})
	f.write("m", 10)
	shard := f.buf.getShard("db/m")
	var writeExcluded, readAllowed bool
	r, _ := f.force("db/m", func([]interface{}) bool {
		writeExcluded = !shard.mu.TryLock()
		if !writeExcluded {
			shard.mu.Unlock()
		}
		if shard.mu.TryRLock() {
			readAllowed = true
			shard.mu.RUnlock()
		}
		return true
	})
	if r != Forced {
		t.Fatalf("got %v, want forced", r)
	}
	if !writeExcluded {
		t.Fatal("want ran with no shard lock held")
	}
	if !readAllowed {
		t.Fatal("want ran under the shard WRITE lock; it must take only the read lock (V-M3)")
	}
}

// I24, first half: two callers that both pass the due check cannot both send.
// The second is held between its scan and its write lock while the first
// forces and a new write lands; its O(1) re-check sees the new detach id.
func TestForceFlushKey_ConcurrentCallersSendOnce(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{MaxInFlight: 4})
	f.write("m", 10)

	inHook := make(chan struct{})
	proceed := make(chan struct{})
	var first atomic.Bool
	forceAfterScanHook = func(string) {
		if first.CompareAndSwap(false, true) {
			close(inHook)
			<-proceed
		}
	}
	held := make(chan ForceResult, 1)
	go func() {
		r, _ := f.force("db/m", func([]interface{}) bool { return true })
		held <- r
	}()
	<-inHook

	if r, _ := f.force("db/m", func([]interface{}) bool { return true }); r != Forced {
		t.Fatalf("the free caller: got %v, want forced", r)
	}
	f.write("m", 10) // the key is buffered again, after a detach
	close(proceed)
	if r := <-held; r != NothingToForce {
		t.Fatalf("the held caller: got %v, want nothing (a detach happened after its due check)", r)
	}
	if f.buffered("db/m") == 0 {
		t.Fatal("the held caller sent the new write: two forced sends for one due check")
	}
}

// I19: each budget condition alone refuses, refusing marks nothing deferred,
// and the slot is given back.
func TestForceFlushKey_BudgetQueueHalfFull(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10})
	f.occupyWorker()
	f.write("blocker1", 100) // queued: len 1 < 4/2
	f.write("k1", 10)
	f.write("k2", 10)
	d0 := f.buf.totalFlushDeferred.Load()

	f.expect("db/k1", Forced) // len 1 -> 2
	f.expect("db/k2", OverBudget)
	f.assertNotDeferred(d0, 1)
	if f.buffered("db/k2") == 0 {
		t.Fatal("over budget, yet the key was flushed")
	}
}

func TestForceFlushKey_BudgetQueueOfOneForcesOnlyWhenEmpty(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 1), &ForceBudget{MaxInFlight: 10})
	f.occupyWorker()
	f.write("k1", 10)
	f.write("k2", 10)
	d0 := f.buf.totalFlushDeferred.Load()

	f.expect("db/k1", Forced) // queue empty -> 1
	f.expect("db/k2", OverBudget)
	f.assertNotDeferred(d0, 1)
}

func TestForceFlushKey_BudgetRecentDeferral(t *testing.T) {
	cfg := forceConfig(4, 8)
	f := newForceFixture(t, cfg, &ForceBudget{})
	f.write("k", 10)

	// A real deferral, through markDeferredLocked, of a key that is then
	// unmarked so that only the timestamp remains.
	ghost := "db/ghost"
	shard := f.buf.getShard(ghost)
	shard.mu.Lock()
	f.buf.markDeferredLocked(shard, ghost)
	delete(shard.deferredKeys, ghost)
	shard.mu.Unlock()
	d0 := f.buf.totalFlushDeferred.Load()

	f.clock.Advance(time.Duration(cfg.MaxBufferAgeMS)*time.Millisecond - time.Millisecond)
	f.expect("db/k", OverBudget)
	f.assertNotDeferred(d0, 0)
	f.clock.Advance(time.Millisecond)
	f.expect("db/k", Forced)
}

func TestForceFlushKey_BudgetBufferedBytes(t *testing.T) {
	cfg := forceConfig(4, 8)
	cfg.MaxBufferedBytes = 1 << 40
	f := newForceFixture(t, cfg, &ForceBudget{})
	f.write("k", 10)
	d0 := f.buf.totalFlushDeferred.Load()

	pad := int64(1<<39) - f.buf.bufferBytesBuffered.Load()
	f.buf.bufferBytesBuffered.Add(pad) // exactly half
	f.expect("db/k", OverBudget)
	f.assertNotDeferred(d0, 0)
	f.buf.bufferBytesBuffered.Add(-pad - 1)
	f.expect("db/k", Forced)
}

func TestForceFlushKey_BudgetForcedTasksInFlight(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{}) // max(1, 4/4) = 1
	f.write("blocker0", 10)
	f.write("k", 10)
	d0 := f.buf.totalFlushDeferred.Load()

	f.expect("db/blocker0", Forced)
	f.waitFor("the forced task to block", func() bool { return f.store.blocked.Load() == 1 })
	f.expect("db/k", OverBudget)
	f.assertNotDeferred(d0, 1)

	f.store.releaseAll()
	f.waitFor("the forced slot to free", func() bool { return f.inFlight() == 0 })
	f.expect("db/k", Forced)
}

func TestEnableKeyFlushLedger_DefaultSlotCap(t *testing.T) {
	for _, tc := range []struct{ workers, want int }{{1, 1}, {3, 1}, {4, 1}, {7, 1}, {8, 2}, {16, 4}} {
		buf := NewArrowBuffer(forceConfig(tc.workers, 8), newGateBackend(), zerolog.New(io.Discard))
		buf.EnableKeyFlushLedger(ForceBudget{})
		if got := buf.ledger.Load().maxInFlight; got != int64(tc.want) {
			t.Errorf("flush_workers=%d: maxInFlight %d, want %d", tc.workers, got, tc.want)
		}
		_ = buf.Close()
	}
}

// I19, the send's lost-slot branch: the queue fills between the budget check
// and the send. Over budget, not deferred, slot back, buffer intact.
func TestForceFlushKey_LostSlotIsNotDeferred(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10})
	f.occupyWorker()
	f.write("k", 10)
	d0 := f.buf.totalFlushDeferred.Load()

	filled := 0
	forceBeforeSendHook = func(string) {
		for len(f.buf.flushQueue) < cap(f.buf.flushQueue) {
			f.buf.flushQueue <- flushTask{}
			filled++
		}
	}
	f.expect("db/k", OverBudget)
	forceBeforeSendHook = nil
	for ; filled > 0; filled-- {
		<-f.buf.flushQueue
	}
	f.assertNotDeferred(d0, 0)
	if f.buffered("db/k") == 0 {
		t.Fatal("the lost-slot branch dropped the buffer")
	}
	if _, ok := f.buf.ledger.Load().lastDetach("db/k"); ok {
		t.Fatal("a failed send recorded a detach")
	}
}

func TestForceFlushKey_Closing(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{})
	f.write("k", 10)

	// Close starting between the scan and the write lock.
	forceAfterScanHook = func(string) { f.buf.closing.Store(true) }
	if r, _ := f.force("db/k", nil); r != Closing {
		t.Fatalf("closing set before the write lock: got %v, want closing", r)
	}
	forceAfterScanHook = nil
	if f.buffered("db/k") == 0 || f.inFlight() != 0 {
		t.Fatal("a closing force sent the key or kept its slot")
	}

	_ = f.buf.Close()
	if r, _ := f.force("db/k", nil); r != Closing {
		t.Fatalf("after Close: got %v, want closing", r)
	}
}

// I20: the forced slot comes back on every exit of a forced task.
func TestForcedSlot_ReleasedAfterAFailedFlush(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{})
	f.write("failer", 10)
	f.expect("db/failer", Forced)
	f.waitFor("the forced slot to free after a failed flush", func() bool { return f.inFlight() == 0 })
	if !f.buf.HasFlushFailure() {
		t.Fatal("fixture: the flush did not fail")
	}
}

func TestForcedSlot_ReleasedByCloseDrain(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline time.Duration
		files    int
	}{
		{"abandon", -time.Second, 0},
		{"flush", 5 * time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newForceFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10})
			f.occupyWorker()
			f.write("k", 10)
			f.expect("db/k", Forced)
			if got := f.inFlight(); got != 1 {
				t.Fatalf("queued forced task holds %d slots, want 1", got)
			}
			// The worker is blocked in a write, so it receives nothing: the
			// drain is the only consumer, as in Close.
			f.buf.drainAndFlushQueue(time.Now().Add(tc.deadline))
			if got := f.inFlight(); got != 0 {
				t.Fatalf("after Close's drain (%s) the forced slot is still held: %d", tc.name, got)
			}
			if got := f.store.files("k"); got != tc.files {
				t.Fatalf("files for k = %d, want %d", got, tc.files)
			}
		})
	}
}

func TestForcedSlot_ReleasedWhenTheWorkerPanics(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 4), &ForceBudget{MaxInFlight: 10})
	f.occupyWorker()
	f.write("panicker", 10)
	f.expect("db/panicker", Forced)
	task := <-f.buf.flushQueue // this goroutine is the worker now

	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		f.buf.runQueuedFlushTask(-1, task)
		return false
	}()
	if !panicked {
		t.Fatal("fixture: the backend did not panic")
	}
	if got := f.inFlight(); got != 0 {
		t.Fatalf("a panicking flush kept its forced slot: %d (a leaked slot disables forcing for good)", got)
	}
}

func TestForcedSlot_ReleasedWhenWantPanics(t *testing.T) {
	f := newForceFixture(t, forceConfig(4, 8), &ForceBudget{})
	f.write("k", 10)
	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		f.force("db/k", func([]interface{}) bool { panic("want") })
		return false
	}()
	if !panicked {
		t.Fatal("fixture: want did not panic")
	}
	if got := f.inFlight(); got != 0 {
		t.Fatalf("a panic in want kept the slot: %d", got)
	}
	shard := f.buf.getShard("db/k")
	if !shard.mu.TryLock() {
		t.Fatal("a panic in want left the shard locked")
	}
	shard.mu.Unlock()
}

// I11: with the ledger off no state is built, whatever detaches and defers.
// With it on, a detach of a known key allocates nothing.
func TestKeyFlushLedger_OffBuildsNothing(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 1), nil)
	f.occupyWorker()
	f.write("blocker1", 100) // fills the queue
	f.write("blocker2", 100) // deferred
	f.write("m", 10)
	if f.buf.countDeferredBuffers() == 0 {
		t.Fatal("fixture: nothing was deferred")
	}
	f.store.releaseAll()
	if err := f.buf.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if f.buf.ledger.Load() != nil {
		t.Fatal("the ledger was built with forcing never enabled")
	}
}

func TestKeyFlushLedger_RecordDetachOfAKnownKeyDoesNotAllocate(t *testing.T) {
	f := newForceFixture(t, forceConfig(1, 4), &ForceBudget{})
	l := f.buf.ledger.Load()
	l.recordDetach("db/m", 1)
	var id uint64 = 1
	if allocs := testing.AllocsPerRun(1000, func() {
		id++
		l.recordDetach("db/m", id)
	}); allocs != 0 {
		t.Fatalf("recordDetach of a known key allocates %.1f per call; the per-detach cost with the ledger on must be a leaf lock and a map write", allocs)
	}
}
