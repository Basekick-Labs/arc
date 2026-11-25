package ingest

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
)

// #1759 (b): a visible write flushes the keys it wrote early (VisibleWait.
// Forcer), through the #1760 (a1) primitive ForceFlushKey.

// countingForcer wraps a KeyForcer: it counts calls, results and want scans
// per key. next nil: every call answers NotDue an hour away.
type countingForcer struct {
	next KeyForcer
	mu   sync.Mutex
	// calls and scans per key; results in call order.
	calls, scans map[string]int
	results      []ForceResult
	// onCall, when set, runs before each call is passed on.
	onCall func(key string)
}

func newCountingForcer(next KeyForcer) *countingForcer {
	return &countingForcer{next: next, calls: map[string]int{}, scans: map[string]int{}}
}

func (c *countingForcer) ForceFlushKey(key string, interval time.Duration, src ForceSource,
	want func([]interface{}) bool) (ForceResult, time.Time) {
	if c.onCall != nil {
		c.onCall(key)
	}
	c.mu.Lock()
	c.calls[key]++
	c.mu.Unlock()
	if src != ForceWrite {
		panic("a visible write must force as ForceWrite")
	}
	counted := want
	if want != nil {
		counted = func(b []interface{}) bool {
			c.mu.Lock()
			c.scans[key]++
			c.mu.Unlock()
			return want(b)
		}
	}
	r, due := NotDue, time.Now().Add(time.Hour)
	if c.next != nil {
		r, due = c.next.ForceFlushKey(key, interval, src, counted)
	}
	c.mu.Lock()
	c.results = append(c.results, r)
	c.mu.Unlock()
	return r, due
}

func (c *countingForcer) callsOf(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[key]
}

func (c *countingForcer) scansOf(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scans[key]
}

func (c *countingForcer) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.results)
}

func (c *countingForcer) count(r ForceResult) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, x := range c.results {
		if x == r {
			n++
		}
	}
	return n
}

// newVisibleForceFixture is a force fixture on the real clock (WaitVisible's
// timers run on it) with the ledger on.
func newVisibleForceFixture(t *testing.T, cfg *config.IngestConfig, budget ForceBudget) *forceFixture {
	t.Helper()
	f := newForceFixture(t, cfg, nil)
	f.buf.EnableKeyFlushLedger(budget)
	return f
}

// visibleWrite writes rows rows to db/meas under a new visible outcome.
func (f *forceFixture) visibleWrite(meas string, rows int) *FlushOutcome {
	f.t.Helper()
	ctx, o := visibleCtx()
	if err := f.buf.WriteColumnarDirect(ctx, "db", meas, makeColumns(rows)); err != nil {
		f.t.Fatalf("visible write %s: %v", meas, err)
	}
	return o
}

func (f *forceFixture) flushAll() {
	f.t.Helper()
	if err := f.buf.FlushAll(context.Background()); err != nil {
		f.t.Fatalf("FlushAll: %v", err)
	}
}

func forceWait(o *FlushOutcome, rows int64, forcer KeyForcer, interval, budget time.Duration) VisibleVerdict {
	return WaitVisible(context.Background(), o, VisibleWait{
		WrittenRows: rows, WrittenRowsKnown: true, Budget: budget, StorageConfirmable: true,
		Forcer: forcer, ForceInterval: interval,
	})
}

// B1: a write to a key detached a moment ago is answered once the interval
// allows the force, not at the age (an hour here).
func TestVisibleForceShortensTheWait1759(t *testing.T) {
	f := newVisibleForceFixture(t, forceConfig(2, 100), ForceBudget{})
	f.write("m", 5)
	f.flushAll() // the key's last detach: now
	o := f.visibleWrite("m", 5)

	const interval = 300 * time.Millisecond
	v := forceWait(o, 5, f.buf, interval, 5*time.Second)
	if !v.Visible {
		t.Fatalf("verdict %+v: want visible, through a forced flush", v)
	}
	if v.Waited < interval/2 || v.Waited > 2*time.Second {
		t.Fatalf("waited %v: want about the interval (%v) after the last detach, not the age", v.Waited, interval)
	}
	if n := f.store.files("m"); n != 2 {
		t.Fatalf("files of m = %d, want 2 (the FlushAll, then the forced flush)", n)
	}

	// The same write with no forcer waits for the age: here, its budget.
	o2 := f.visibleWrite("m", 5)
	if v := forceWait(o2, 5, nil, interval, 400*time.Millisecond); v.Visible || v.Reason != VisibleReasonTimeout {
		t.Fatalf("no forcer: verdict %+v, want timeout", v)
	}
}

// B2: two writes buffered before a detach share it; a write appended after
// it waits for its own force, the interval later.
func TestVisibleForceSharesADetachOnlyWithBatchesBeforeIt1759(t *testing.T) {
	f := newVisibleForceFixture(t, forceConfig(2, 100), ForceBudget{})
	const interval = 300 * time.Millisecond
	a := f.visibleWrite("m", 5)
	b := f.visibleWrite("m", 5)
	if v := forceWait(a, 5, f.buf, interval, 5*time.Second); !v.Visible {
		t.Fatalf("a: %+v", v)
	}
	if v := forceWait(b, 5, f.buf, interval, 5*time.Second); !v.Visible || v.Waited > 100*time.Millisecond {
		t.Fatalf("b, appended before a's force: %+v, want visible at once", v)
	}
	if n := f.store.files("m"); n != 1 {
		t.Fatalf("files of m = %d after a and b, want 1 (one forced flush for both)", n)
	}

	c := f.visibleWrite("m", 5)
	v := forceWait(c, 5, f.buf, interval, 3*time.Second)
	if !v.Visible {
		t.Fatalf("c, appended after the detach: %+v, want visible through its own force", v)
	}
	if v.Waited < interval/2 {
		t.Fatalf("c waited %v: it cannot have been served by a's detach, and the key was not due for %v", v.Waited, interval)
	}
	if n := f.store.files("m"); n != 2 {
		t.Fatalf("files of m = %d after c, want 2", n)
	}
}

// B3 and B9: a key whose batches of this write were flushed by another
// detach is not forced (want false) even though others' rows sit in it, and
// it is scanned once, never again in that wait.
func TestVisibleForceOnlyWhereItsBatchesAre1759(t *testing.T) {
	// Room for m's force after blocker0's: m is refused by want, not budget.
	f := newVisibleForceFixture(t, forceConfig(2, 100), ForceBudget{MaxInFlight: 4})
	ctx, o := visibleCtx()
	for _, m := range []string{"m", "blocker0"} {
		if err := f.buf.WriteColumnarDirect(ctx, "db", m, makeColumns(5)); err != nil {
			t.Fatal(err)
		}
	}
	// o's batch of m leaves with a flush of m alone; another writer's rows
	// then sit in m.
	if err := f.buf.FlushMeasurements(context.Background(), "db", []string{"m"}); err != nil {
		t.Fatal(err)
	}
	f.write("m", 5)

	cf := newCountingForcer(f.buf)
	done := make(chan VisibleVerdict, 1)
	go func() { done <- forceWait(o, 10, cf, time.Millisecond, 600*time.Millisecond) }()
	// Extra rounds: each token is a round.
	for i := 0; i < 5; i++ {
		time.Sleep(40 * time.Millisecond)
		o.vis.signal()
	}
	v := <-done
	if v.Visible || v.Reason != VisibleReasonTimeout {
		t.Fatalf("verdict %+v: want timeout (blocker0's flush is held)", v)
	}
	if n := f.store.files("m"); n != 1 {
		t.Fatalf("files of m = %d, want 1: m held none of this write's batches, so it must not be forced", n)
	}
	if n := f.buffered("db/m"); n != 1 {
		t.Fatalf("buffered batches of m = %d, want 1 (the other writer's)", n)
	}
	if n := cf.scansOf("db/m"); n != 1 {
		t.Fatalf("m scanned %d times, want once: a key with none of this write's batches is dropped from the round (V21-L3)", n)
	}
	if n := cf.callsOf("db/blocker0"); n != 1 {
		t.Fatalf("blocker0 asked %d times, want once: a forced key is dropped", n)
	}
}

// B4: write forcing advances the shared last detach, so a read inside the
// interval is not due; and the forced flush is counted under source=write.
func TestVisibleForceSharesTheReadLimiter1759(t *testing.T) {
	f := newVisibleForceFixture(t, forceConfig(2, 100), ForceBudget{})
	before := forcedFlushCount(t, "write", "forced")
	o := f.visibleWrite("m", 5)
	if v := forceWait(o, 5, f.buf, 5*time.Second, 5*time.Second); !v.Visible {
		t.Fatalf("%+v", v)
	}
	if got := forcedFlushCount(t, "write", "forced"); got != before+1 {
		t.Fatalf("arc_ingest_forced_flushes_total{source=write,result=forced} = %d, want %d", got, before+1)
	}
	f.write("m", 5)
	if r, due := f.buf.ForceFlushKey("db/m", 5*time.Second, ForceRead, nil); r != NotDue || time.Until(due) < 4*time.Second {
		t.Fatalf("a read right after a write's force: %v (due in %v), want not_due about 5 s away", r, time.Until(due))
	}
}

// B5: over budget, the wait falls back to the age flush, asks again only at
// later rounds (never in a loop) and marks nothing deferred.
func TestVisibleForceOverBudgetWaitsForTheAgeFlush1759(t *testing.T) {
	cfg := forceConfig(2, 100) // one forced slot
	cfg.MaxBufferAgeMS = 600
	f := newVisibleForceFixture(t, cfg, ForceBudget{})
	f.write("blocker0", 5)
	if r, _ := f.buf.ForceFlushKey("db/blocker0", time.Second, ForceRead, nil); r != Forced {
		t.Fatalf("taking the only forced slot: %v", r)
	}
	f.waitFor("blocker0 held in a worker", func() bool { return f.store.blocked.Load() >= 1 })

	o := f.visibleWrite("m", 5)
	cf := newCountingForcer(f.buf)
	v := forceWait(o, 5, cf, time.Second, 5*time.Second)
	if !v.Visible {
		t.Fatalf("%+v: want visible through the age flush", v)
	}
	if v.Waited < 300*time.Millisecond {
		t.Fatalf("waited %v: over budget, the write cannot be forced and waits for the age (600 ms)", v.Waited)
	}
	if n := cf.count(Forced); n != 0 {
		t.Fatalf("%d forced while the only slot was held", n)
	}
	if n := cf.callsOf("db/m"); n > 2 {
		t.Fatalf("m asked %d times during one wait: an over-budget key is asked again only at a later round, never in a loop", n)
	}
	if n := f.buf.countDeferredBuffers(); n != 0 {
		t.Fatalf("%d buffers deferred by forcing", n)
	}
}

// B6: a wait that forces keeps all four exits and the re-check of an outcome
// resolved as the wait ends.
func TestVisibleForcingKeepsEveryExit1759(t *testing.T) {
	// marked is an outcome with one marked, unresolved batch of 3 rows
	// under db/k.
	marked := func() *FlushOutcome {
		o := NewVisibleOutcome(time.Now(), false, 0)
		visibleBatch(o, 3, true)
		noteVisibleKey(o, "db/k")
		return o
	}
	settle := func(o *FlushOutcome) {
		o.vis.settledRows.Add(3)
		o.vis.settledBatches.Add(1)
		o.vis.signal()
	}
	run := func(o *FlushOutcome, w VisibleWait, after func()) (VisibleVerdict, *countingForcer) {
		cf := newCountingForcer(nil) // not due for an hour: the wait keeps forcing
		w.WrittenRows, w.WrittenRowsKnown, w.StorageConfirmable = 3, true, true
		w.Forcer, w.ForceInterval = cf, time.Second
		if w.Budget == 0 {
			w.Budget = 10 * time.Second
		}
		if after != nil {
			time.AfterFunc(50*time.Millisecond, after)
		}
		return WaitVisible(context.Background(), o, w), cf
	}
	check := func(name string, v VisibleVerdict, cf *countingForcer, visible bool, reason VisibleReason) {
		t.Helper()
		if v.Visible != visible || (!visible && v.Reason != reason) {
			t.Fatalf("%s: verdict %+v, want visible=%v reason=%q", name, v, visible, reason)
		}
		if v.Waited > 2*time.Second {
			t.Fatalf("%s: waited %v; the exit must end a forcing wait at once", name, v.Waited)
		}
		if cf.callsOf("db/k") == 0 {
			t.Fatalf("%s: no forcing round ran", name)
		}
	}

	v, cf := run(marked(), VisibleWait{Budget: 100 * time.Millisecond}, nil)
	check("budget", v, cf, false, VisibleReasonTimeout)

	done := make(chan struct{})
	v, cf = run(marked(), VisibleWait{Done: done}, func() { close(done) })
	check("shutdown", v, cf, false, VisibleReasonShutdown)

	ctx, cancel := context.WithCancel(context.Background())
	o := marked()
	cf = newCountingForcer(nil)
	time.AfterFunc(50*time.Millisecond, cancel)
	v = WaitVisible(ctx, o, VisibleWait{WrittenRows: 3, WrittenRowsKnown: true, StorageConfirmable: true, Budget: 10 * time.Second, Forcer: cf, ForceInterval: time.Second})
	check("ctx", v, cf, false, VisibleReasonShutdown)

	o = marked()
	v, cf = run(o, VisibleWait{}, func() { o.vis.overResolved.Store(true); o.vis.signal() })
	check("over-resolved", v, cf, false, VisibleReasonNotConfirmed)

	o = marked()
	v, cf = run(o, VisibleWait{}, func() { settle(o) })
	check("settled while forcing", v, cf, true, "")

	// Resolved by the round itself as the budget ends: both the token and
	// the budget are ready; either way the answer is visible.
	for i := 0; i < 20; i++ {
		o := marked()
		cf := newCountingForcer(nil)
		cf.onCall = func(string) { settle(o); time.Sleep(2 * time.Millisecond) }
		v := WaitVisible(context.Background(), o, VisibleWait{WrittenRows: 3, WrittenRowsKnown: true, StorageConfirmable: true,
			Budget: time.Millisecond, Forcer: cf, ForceInterval: time.Second})
		if !v.Visible {
			t.Fatalf("resolved as the wait ended (run %d): %+v, want visible", i, v)
		}
	}
}

// B7: a write of many units to one measurement records one key; interleaved
// measurements each once, in first-mark order.
func TestVisibleKeysDeduplicated1759(t *testing.T) {
	cfg := forceConfig(2, 100)
	cfg.MaxBufferSize = 1_000_000
	f := newVisibleForceFixture(t, cfg, ForceBudget{})
	ctx, o := visibleCtx()
	for i := 0; i < 10_000; i++ {
		if err := f.buf.WriteColumnarDirect(ctx, "db", "m", makeColumns(1)); err != nil {
			t.Fatal(err)
		}
	}
	if got := o.vis.markedKeys(); len(got) != 1 || got[0] != "db/m" {
		t.Fatalf("keys after 10 000 units to one measurement = %d %v, want [db/m]", len(got), got[:min(len(got), 3)])
	}
	ctx, o = visibleCtx()
	for _, m := range []string{"a", "b", "a", "b", "c", "a"} {
		if err := f.buf.WriteColumnarDirect(ctx, "db", m, makeColumns(1)); err != nil {
			t.Fatal(err)
		}
	}
	if got := fmt.Sprint(o.vis.markedKeys()); got != "[db/a db/b db/c]" {
		t.Fatalf("interleaved keys = %s, want [db/a db/b db/c]", got)
	}
	// An import's outcome records none.
	imp := &FlushOutcome{}
	noteVisibleKey(imp, "db/m")
	noteVisibleKey(nil, "db/m")
}

// Both append sites record the key: the typed path and the record path.
func TestVisibleBothAppendSitesRecordTheKey1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	ctx, o := visibleCtx()
	if err := buf.WriteColumnarRecord(ctx, "db", columnarVisibleRecord("msites", 3)); err != nil {
		t.Fatal(err)
	}
	writeOutcomeBatch(t, buf, ctx, "msites2", 2)
	writeOutcomeBatch(t, buf, context.Background(), "mlive", 7) // live: not this outcome's
	if got := fmt.Sprint(o.vis.markedKeys()); got != "[db/msites db/msites2]" {
		t.Fatalf("keys = %s, want [db/msites db/msites2]", got)
	}
}

// B8: 64 visible waiters on one hot key, none due: a round takes no shard
// lock (each answers not_due while the test holds the key's shard lock).
func TestVisibleForceNotDueTakesNoShardLock1759(t *testing.T) {
	f := newVisibleForceFixture(t, forceConfig(2, 100), ForceBudget{})
	f.write("hot", 5)
	f.flushAll()
	const n = 64
	outcomes := make([]*FlushOutcome, n)
	for i := range outcomes {
		outcomes[i] = f.visibleWrite("hot", 1)
	}
	cf := newCountingForcer(f.buf)
	shard := f.buf.getShard("db/hot")
	shard.mu.Lock()
	locked := true
	defer func() {
		if locked {
			shard.mu.Unlock()
		}
	}()
	var wg sync.WaitGroup
	var visible atomic.Int64
	for _, o := range outcomes {
		wg.Add(1)
		go func(o *FlushOutcome) {
			defer wg.Done()
			if v := forceWait(o, 1, cf, 10*time.Second, 5*time.Second); v.Visible {
				visible.Add(1)
			}
		}(o)
	}
	deadline := time.Now().Add(2 * time.Second)
	for cf.count(NotDue) < n {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d waiters finished a not-due round while the key's shard lock was held: a round of a key that is not due must take no shard lock", cf.count(NotDue), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
	shard.mu.Unlock()
	locked = false
	f.flushAll()
	wg.Wait()
	if visible.Load() != n {
		t.Fatalf("%d of %d visible after the flush", visible.Load(), n)
	}
}

// B10: with no forcer, or a forcer and no positive interval, the wait never
// asks: today's behaviour (the #1759 tests run with no forcer, unchanged).
func TestVisibleNoForcerOrNoIntervalNeverForces1759(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		o := NewVisibleOutcome(time.Now(), false, 0)
		visibleBatch(o, 3, true)
		noteVisibleKey(o, "db/k")
		cf := newCountingForcer(nil)
		if v := forceWait(o, 3, cf, interval, 50*time.Millisecond); v.Reason != VisibleReasonTimeout {
			t.Fatalf("interval %v: %+v, want timeout", interval, v)
		}
		if cf.total() != 0 {
			t.Fatalf("interval %v: the forcer was asked %d times; ForceInterval <= 0 must never force", interval, cf.total())
		}
	}
	o := NewVisibleOutcome(time.Now(), false, 0)
	visibleBatch(o, 3, true)
	noteVisibleKey(o, "db/k")
	if v := forceWait(o, 3, nil, time.Second, 50*time.Millisecond); v.Reason != VisibleReasonTimeout {
		t.Fatalf("nil forcer: %+v, want timeout", v)
	}
}

// fixedForcer answers every call with one result.
type fixedForcer ForceResult

func (r fixedForcer) ForceFlushKey(string, time.Duration, ForceSource, func([]interface{}) bool) (ForceResult, time.Time) {
	return ForceResult(r), time.Time{}
}

// LedgerOff and Closing end forcing for the rest of the wait: one call.
func TestVisibleForceStopsOnLedgerOffAndClosing1759(t *testing.T) {
	o := NewVisibleOutcome(time.Now(), false, 0)
	visibleBatch(o, 3, true)
	noteVisibleKey(o, "db/k")
	cf := newCountingForcer(fixedForcer(Closing))
	go func() {
		for i := 0; i < 3; i++ {
			time.Sleep(30 * time.Millisecond)
			o.vis.signal()
		}
	}()
	if v := forceWait(o, 3, cf, time.Second, 300*time.Millisecond); v.Reason != VisibleReasonTimeout {
		t.Fatalf("closing: %+v", v)
	}
	if n := cf.callsOf("db/k"); n != 1 {
		t.Fatalf("closing: asked %d times, want once", n)
	}

	f := newForceFixture(t, forceConfig(2, 100), nil) // ledger never enabled
	o = f.visibleWrite("m", 5)
	cf = newCountingForcer(f.buf)
	done := make(chan VisibleVerdict, 1)
	go func() { done <- forceWait(o, 5, cf, time.Second, 300*time.Millisecond) }()
	for i := 0; i < 3; i++ {
		time.Sleep(30 * time.Millisecond)
		o.vis.signal()
	}
	if v := <-done; v.Reason != VisibleReasonTimeout {
		t.Fatalf("%+v", v)
	}
	if n := cf.callsOf("db/m"); n != 1 || cf.count(LedgerOff) != 1 {
		t.Fatalf("ledger off: asked %d times (results %v), want once", n, cf.results)
	}
}

// Pruning: an entry older than the retention is deleted, a newer one kept,
// and the walk runs at most once per pruneEvery.
func TestKeyFlushLedgerPrune1759(t *testing.T) {
	f := newForceFixture(t, forceConfig(2, 100), &ForceBudget{Retain: 10 * time.Second})
	l := f.buf.ledger.Load()
	if l.pruneEvery != 10*time.Second {
		t.Fatalf("pruneEvery = %v, want the retention (10 s) when under a minute", l.pruneEvery)
	}
	l.recordDetach("db/old", 1)
	f.clock.Advance(5 * time.Second)
	l.recordDetach("db/new", 2)
	f.clock.Advance(6 * time.Second) // old: 11 s, new: 6 s
	if n := l.maybePrune(); n != 1 {
		t.Fatalf("pruned %d, want 1", n)
	}
	if _, ok := l.lastDetach("db/old"); ok {
		t.Fatal("an entry older than the retention was kept")
	}
	if _, ok := l.lastDetach("db/new"); !ok {
		t.Fatal("an entry within the retention was pruned")
	}
	f.clock.Advance(5 * time.Second) // new: 11 s, last walk 5 s ago
	if n := l.maybePrune(); n != 0 {
		t.Fatalf("pruned %d within pruneEvery of the last walk, want 0", n)
	}
	f.clock.Advance(5 * time.Second)
	if n := l.maybePrune(); n != 1 {
		t.Fatalf("pruned %d once pruneEvery passed, want 1", n)
	}
	// Defaults: an hour's retention, walked once a minute.
	g := newForceFixture(t, forceConfig(2, 100), &ForceBudget{})
	if gl := g.buf.ledger.Load(); gl.retain != MaxForceInterval || gl.pruneEvery != time.Minute {
		t.Fatalf("default retain %v / pruneEvery %v, want %v / 1m", gl.retain, gl.pruneEvery, MaxForceInterval)
	}
	if want := time.Duration(config.MaxWaitVisibleMinIntervalMS) * time.Millisecond; want != MaxForceInterval {
		t.Fatalf("config ceiling %v != MaxForceInterval %v: an interval above the retention would not hold", want, MaxForceInterval)
	}
}

// The periodic sweep prunes the ledger after the aged flush.
func TestPeriodicFlushPrunesTheLedger1759(t *testing.T) {
	cfg := forceConfig(2, 100)
	cfg.MaxBufferAgeMS = 50
	f := newForceFixture(t, cfg, &ForceBudget{Retain: time.Second})
	f.write("m", 5)
	f.flushAll()
	l := f.buf.ledger.Load()
	if _, ok := l.lastDetach("db/m"); !ok {
		t.Fatal("no entry after the flush")
	}
	f.clock.Advance(2 * time.Second)
	f.waitFor("the periodic sweep to prune db/m", func() bool {
		_, ok := l.lastDetach("db/m")
		return !ok
	})
}

// A prune between the due check and the write-lock re-check is not a detach:
// the force goes ahead.
func TestForceFlushKeyPrunedEntryIsNotADetach1759(t *testing.T) {
	f := newForceFixture(t, forceConfig(2, 100), &ForceBudget{Retain: 2 * time.Second})
	l := f.buf.ledger.Load()
	f.write("m", 5)
	f.flushAll()
	f.clock.Advance(3 * time.Second)
	f.write("m", 5)
	forceAfterScanHook = func(string) {
		l.lastPrune = time.Time{}
		if n := l.maybePrune(); n != 1 {
			t.Errorf("hook pruned %d, want 1", n)
		}
	}
	if r, _ := f.buf.ForceFlushKey("db/m", time.Second, ForceWrite, nil); r != Forced {
		t.Fatalf("ForceFlushKey after a prune of its entry = %v, want forced: a pruned entry is not a detach", r)
	}
}

// Every result but ledger_off is counted, under its source.
func TestForceFlushKeyCountsResults1759(t *testing.T) {
	off := newForceFixture(t, forceConfig(2, 100), nil)
	sum := func() (n int64) {
		for _, r := range []string{"forced", "not_due", "over_budget", "nothing", "not_wanted", "closing"} {
			n += forcedFlushCount(t, "read", r)
		}
		return n
	}
	before := sum()
	off.write("m", 5)
	if r, _ := off.buf.ForceFlushKey("db/m", time.Second, ForceRead, nil); r != LedgerOff {
		t.Fatal(r)
	}
	if sum() != before {
		t.Fatal("ledger_off was counted")
	}

	f := newForceFixture(t, forceConfig(2, 100), &ForceBudget{})
	f.write("m", 5)
	for _, c := range []struct {
		src    ForceSource
		source string
		result string
	}{{ForceWrite, "write", "forced"}, {ForceWrite, "write", "not_due"}, {ForceRead, "read", "not_due"}} {
		before := forcedFlushCount(t, c.source, c.result)
		f.buf.ForceFlushKey("db/m", time.Second, c.src, nil)
		if got := forcedFlushCount(t, c.source, c.result); got != before+1 {
			t.Fatalf("{source=%s,result=%s} = %d, want %d", c.source, c.result, got, before+1)
		}
	}
}

func forcedFlushCount(t *testing.T, source, result string) int64 {
	t.Helper()
	re := regexp.MustCompile(`arc_ingest_forced_flushes_total\{source="` + source + `",result="` + result + `"\} (\d+)`)
	m := re.FindStringSubmatch(metrics.Get().PrometheusFormat())
	if m == nil {
		t.Fatalf("no arc_ingest_forced_flushes_total{source=%q,result=%q} series", source, result)
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n
}
