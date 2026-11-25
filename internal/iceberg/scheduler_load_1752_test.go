package iceberg

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// #1752: a REST catalog load waits, briefly, for the files reported for its
// table before it, by pulling the table's queued ingest export forward; it
// never commits itself, and never makes a table that needs a pass run one.

// startLoadRig starts a scheduler on the rig with the ingest interval I, the
// load interval F and the append path on or off, and waits for the start-up
// pass to publish the rig's first file.
func startLoadRig(t *testing.T, ingest, load time.Duration, appendOn bool, gate WriterGate) *appendRig {
	t.Helper()
	r := newAppendRig(t)
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: r.src, Interval: time.Hour, Logger: r.sched.logger,
		IngestInterval: ingest, IngestAppend: appendOn, LoadInterval: load, Gate: gate})
	r.sched.started = time.Now().Add(-time.Second)
	r.sched.gatePoll = 0
	r.newFile()
	r.sched.Start()
	t.Cleanup(r.sched.Stop)
	waitUntil(t, "the start-up pass did not publish the file", func() bool { return r.tableFiles() >= 1 })
	// Until the pass has returned: a load during a timed pass does not wait.
	waitUntil(t, "the start-up pass did not end", func() bool { return !r.sched.inTimedPass.Load() })
	return r
}

// awaitTimed runs AwaitFreshForRead and returns how long it took.
func awaitTimed(r *appendRig) time.Duration {
	began := time.Now()
	r.sched.AwaitFreshForRead(context.Background(), "mydb", "cpu")
	return time.Since(began)
}

func TestLoadServesAFileReportedBeforeIt(t *testing.T) {
	// I is an hour: without the load the new file would wait for it.
	r := startLoadRig(t, time.Hour, 10*time.Millisecond, true, nil)
	listed := r.listings()
	f := r.newFile()
	r.report(f)
	if took := awaitTimed(r); took >= loadFreshBudget {
		t.Fatalf("the load waited the whole budget (%v)", took)
	}
	if got := r.tableFiles(); got != 2 {
		t.Fatalf("table has %d files after the load, want 2 (the file reported before it)", got)
	}
	if got := r.listings() - listed; got != 0 {
		t.Fatalf("the load's export listed the measurement %d times, want 0 (an append)", got)
	}
	assertNoLoadWaiters(t, r.sched)
}

func TestLoadWithNothingReportedReturnsAtOnce(t *testing.T) {
	r := startLoadRig(t, time.Hour, 10*time.Millisecond, true, nil)
	if took := awaitTimed(r); took > 50*time.Millisecond {
		t.Fatalf("a load with nothing reported waited %v", took)
	}
}

// A table whose ingest export would be a pass (here the append path is off)
// is not pulled forward: the load reads the published version at once, and
// the request keeps its due time.
func TestLoadDoesNotPullAPassForward(t *testing.T) {
	r := startLoadRig(t, time.Hour, 10*time.Millisecond, false, nil)
	listed := r.listings()
	r.report(r.newFile())
	if took := awaitTimed(r); took > 500*time.Millisecond {
		t.Fatalf("the load waited %v for a table that needs a pass", took)
	}
	time.Sleep(100 * time.Millisecond)
	if got := r.listings() - listed; got != 0 {
		t.Fatalf("a load made the table run %d passes", got)
	}
	if got := r.tableFiles(); got != 1 {
		t.Fatalf("table has %d files, want 1 (the new file waits for its own due time)", got)
	}
}

// F counts from the table's own spacing stamp: a load right after an export
// does not export again sooner than F after it.
func TestLoadKeepsTheLoadInterval(t *testing.T) {
	r := startLoadRig(t, time.Hour, time.Hour, true, nil) // F an hour too: never within the budget
	r.report(r.newFile())
	if took := awaitTimed(r); took > 500*time.Millisecond {
		t.Fatalf("the load waited %v though F had not passed", took)
	}
	time.Sleep(100 * time.Millisecond)
	if got := r.tableFiles(); got != 1 {
		t.Fatalf("table has %d files, want 1: the load exported inside F", got)
	}
}

// On a node that does not export, the loop drops the reports; the load goes
// at once instead of waiting out the budget, and never asks the gate itself.
func TestLoadOnAClosedGateReturnsAtOnce(t *testing.T) {
	gate := &atomicGate{}
	gate.on.Store(true)
	r := startLoadRig(t, time.Hour, 10*time.Millisecond, true, gate)
	gate.on.Store(false)
	r.report(r.newFile())
	if took := awaitTimed(r); took > 500*time.Millisecond {
		t.Fatalf("the load waited %v on a node that does not export", took)
	}
}

// Concurrent loads share one export (the issue's coalescing acceptance).
func TestConcurrentLoadsShareOneExport(t *testing.T) {
	r := startLoadRig(t, time.Hour, 10*time.Millisecond, true, nil)
	before := r.snapshotCount()
	r.report(r.newFile())
	done := make(chan struct{})
	for range 8 {
		go func() { r.sched.AwaitFreshForRead(context.Background(), "mydb", "cpu"); done <- struct{}{} }()
	}
	for range 8 {
		<-done
	}
	if got := r.tableFiles(); got != 2 {
		t.Fatalf("table has %d files, want 2", got)
	}
	if got := r.snapshotCount() - before; got != 1 {
		t.Fatalf("8 loads made %d commits, want 1", got)
	}
	assertNoLoadWaiters(t, r.sched)
}

// Every load removes its waiter: a leak would skip every load past the cap.
func assertNoLoadWaiters(t *testing.T, s *Scheduler) {
	t.Helper()
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	if s.loadWaiterN != 0 || len(s.loadWaiters) != 0 {
		t.Fatalf("%d waiters counted, %d tables listed after every load returned", s.loadWaiterN, len(s.loadWaiters))
	}
}

// pullForLoadLocked bounds what loads pull into the budget's window by the
// ingest concurrency, and never moves a due time later.
func TestPullForLoadBoundsAndOnlyLowers(t *testing.T) {
	s := NewScheduler(SchedulerConfig{IngestInterval: time.Hour, LoadInterval: time.Second, IngestConcurrency: 1})
	now := time.Now()
	horizon := now.Add(loadFreshBudget)
	key := "db\x00m"
	s.ingestDue[key] = ingestRequest{m: Measurement{Database: "db", Measurement: "m"}, due: now.Add(time.Hour)}
	full := 1
	if s.pullForLoadLocked(key, true, now, horizon, &full) {
		t.Fatal("pulled a request into a window already holding ingestConcurrency due requests")
	}
	none := 0
	if !s.pullForLoadLocked(key, true, now, horizon, &none) || s.ingestDue[key].due.After(now) {
		t.Fatalf("not pulled to now: due %v", s.ingestDue[key].due)
	}
	early := now.Add(-time.Minute)
	s.ingestDue[key] = ingestRequest{m: Measurement{Database: "db", Measurement: "m"}, due: early}
	s.ingestNext[key] = now.Add(time.Hour)
	s.loadInterval = 10 * time.Second // target = ingestNext - (I - F) = now + 10 s, beyond the window
	if s.pullForLoadLocked(key, true, now, horizon, &none) {
		t.Fatal("pulled though F had not passed since the last export")
	}
	if !s.ingestDue[key].due.Equal(early) {
		t.Fatal("a refused pull changed the due time")
	}
	s.loadUnsettled[key] = true
	delete(s.ingestNext, key)
	if s.pullForLoadLocked(key, true, now, horizon, &none) {
		t.Fatal("pulled a table whose last export did not settle")
	}
}

// A load for another table that arrives between serviceLoadWaiters' two
// locked sections is left for the next turn, not skipped.
func TestLoadArrivingMidServiceIsNotSkipped(t *testing.T) {
	s := NewScheduler(SchedulerConfig{IngestInterval: time.Hour, LoadInterval: time.Second})
	first, late := "db\x00a", "db\x00b"
	s.ingestPending[first] = ingestReports{first: time.Now().Add(-time.Second)}
	s.ingestPending[late] = ingestReports{first: time.Now().Add(-time.Second)}
	s.loadWaiters[first] = []*loadWaiter{{loadAt: time.Now(), done: make(chan struct{})}}
	w := &loadWaiter{loadAt: time.Now(), done: make(chan struct{})}
	loadAfterSnapshot = func() {
		s.reqMu.Lock()
		s.loadWaiters[late] = append(s.loadWaiters[late], w)
		s.reqMu.Unlock()
	}
	t.Cleanup(func() { loadAfterSnapshot = nil })
	s.serviceLoadWaiters()
	if w.closed {
		t.Fatalf("a load that arrived mid-service was closed (outcome %d), want it left for the next turn", w.outcome)
	}
	if !s.loadWaiters[first][0].closed {
		t.Fatal("the load in the snapshot was not decided (its table is not servable: no source)")
	}
}

func newLoadScheduler(t *testing.T) (*Scheduler, string) {
	t.Helper()
	s := NewScheduler(SchedulerConfig{IngestInterval: time.Hour, LoadInterval: time.Second, IngestConcurrency: 2})
	key := "db\x00m"
	s.ingestPending[key] = ingestReports{first: time.Now().Add(-time.Minute), last: time.Now().Add(-time.Minute)}
	return s, key
}

func addWaiter(s *Scheduler, key string, decided bool) *loadWaiter {
	w := &loadWaiter{loadAt: time.Now(), done: make(chan struct{}), decided: decided}
	s.loadWaiters[key] = append(s.loadWaiters[key], w)
	s.loadWaiterN++
	return w
}

// A pull never moves a due time later: a request already due sooner than F
// allows keeps its time.
func TestPullForLoadNeverDelays(t *testing.T) {
	s, key := newLoadScheduler(t)
	now := time.Now()
	early := now.Add(-time.Minute)
	s.ingestDue[key] = ingestRequest{m: Measurement{Database: "db", Measurement: "m"}, due: early}
	s.ingestNext[key] = now.Add(time.Hour) // target = now + F (1 s), inside the window, after due
	n := 0
	if !s.pullForLoadLocked(key, true, now, now.Add(loadFreshBudget), &n) {
		t.Fatal("a request already due was refused")
	}
	if !s.ingestDue[key].due.Equal(early) {
		t.Fatalf("due moved from %v to %v", early, s.ingestDue[key].due)
	}
}

// Loads pull at most ingestConcurrency exports per budget over all tables.
func TestPullForLoadIsCappedOverAllTables(t *testing.T) {
	s, _ := newLoadScheduler(t)
	now := time.Now()
	for i := range 3 {
		key := "db\x00m" + string(rune('a'+i))
		s.ingestDue[key] = ingestRequest{m: Measurement{Database: "db", Measurement: key}, due: now.Add(time.Hour)}
		n := 0 // each its own service turn: only the cap over all tables applies
		got := s.pullForLoadLocked(key, true, now, now.Add(loadFreshBudget), &n)
		if want := i < 2; got != want { // IngestConcurrency 2
			t.Fatalf("table %d: pulled %v, want %v", i, got, want)
		}
	}
}

// An export that covers a load's files lets it go at once, wherever the
// loop is; one that does not settle lets go the loads it was pulled for.
func TestCoveringExportReleasesLoadsAtOnce(t *testing.T) {
	s, key := newLoadScheduler(t)
	s.state[key] = measurementState{}
	covered := addWaiter(s, key, true)
	s.noteIngestCovered(key, time.Now(), outcomeReconciled, true, nil)
	if !covered.closed || covered.outcome != metrics.IcebergLoadFreshCovered {
		t.Fatalf("covering export: closed %v outcome %d", covered.closed, covered.outcome)
	}

	s, key = newLoadScheduler(t)
	pulled, undecided := addWaiter(s, key, true), addWaiter(s, key, false)
	s.noteIngestCovered(key, time.Now(), outcomeUnchanged, false, errors.New("commit failed"))
	if !pulled.closed || pulled.outcome != metrics.IcebergLoadFreshSkipped {
		t.Fatalf("failed export, pulled load: closed %v outcome %d", pulled.closed, pulled.outcome)
	}
	if undecided.closed {
		t.Fatal("a failed export let go a load it was not pulled for")
	}
	if !s.loadUnsettled[key] {
		t.Fatal("the table is not marked unsettled")
	}
}

// Dropping what is reported (another node exports) lets loads go as skipped,
// not as covered.
func TestDroppedReportsSkipLoads(t *testing.T) {
	s, key := newLoadScheduler(t)
	w := addWaiter(s, key, false)
	s.dropIngest()
	if !w.closed || w.outcome != metrics.IcebergLoadFreshSkipped {
		t.Fatalf("closed %v outcome %d, want skipped", w.closed, w.outcome)
	}
}

// During a timed pass, past the caps, or after an unsettled export, a load
// does not wait.
func TestLoadDoesNotWaitWhenItCannotBeServed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(s *Scheduler, key string)
	}{
		{"timed pass", func(s *Scheduler, _ string) { s.inTimedPass.Store(true) }},
		{"unsettled", func(s *Scheduler, key string) { s.loadUnsettled[key] = true }},
		{"per-table cap", func(s *Scheduler, key string) {
			for range loadMaxWaitersPerTable {
				addWaiter(s, key, false)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, key := newLoadScheduler(t)
			tc.setup(s, key)
			before := len(s.loadWaiters[key])
			began := time.Now()
			s.AwaitFreshForRead(context.Background(), "db", "m")
			if took := time.Since(began); took > 100*time.Millisecond {
				t.Fatalf("waited %v", took)
			}
			if len(s.loadWaiters[key]) != before {
				t.Fatal("the load registered a waiter")
			}
		})
	}
}

// A load that registers next to a waiter the loop has closed but its load
// has not yet removed still gets a loop turn (it is the first open one).
func TestLoadNextToAClosedWaiterIsServed(t *testing.T) {
	r := startLoadRig(t, time.Hour, 10*time.Millisecond, false, nil) // append off: not servable, skipped by the loop
	r.report(r.newFile())
	// Let the report's own wake be served, so only a load's wake can bring
	// the next loop turn.
	waitUntil(t, "the loop did not take the report's wake", func() bool { return len(r.sched.wake) == 0 })
	time.Sleep(50 * time.Millisecond)
	r.sched.reqMu.Lock()
	stale := &loadWaiter{loadAt: time.Now(), done: make(chan struct{})}
	stale.finishLocked(0)
	r.sched.loadWaiters["mydb\x00cpu"] = append(r.sched.loadWaiters["mydb\x00cpu"], stale)
	r.sched.loadWaiterN++
	r.sched.reqMu.Unlock()
	done := make(chan time.Duration, 16)
	for range 16 {
		go func() { done <- awaitTimed(r) }()
	}
	for range 16 {
		if took := <-done; took > 500*time.Millisecond {
			t.Fatalf("a load waited %v next to a closed waiter", took)
		}
	}
}

// A table whose last append fell back to a pass is not pulled by a load
// until an append settles again.
func TestLoadDoesNotPullATableThatFellBack(t *testing.T) {
	r := newAppendRig(t)
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: r.src, Interval: time.Hour, Logger: r.sched.logger,
		IngestInterval: time.Millisecond, IngestAppend: true, LoadInterval: time.Millisecond})
	r.sched.started = time.Now().Add(-time.Second)
	r.settled()
	key := "mydb\x00cpu"
	if !r.sched.loadServableSoon(key) {
		t.Fatal("a settled table on the append path is not servable")
	}
	job := &ingestJob{m: Measurement{Database: "mydb", Measurement: "cpu"}, key: key, reason: "a new column"}
	r.sched.noteAppendFallback(job)
	r.sched.appendOK[key] = true // as if the pass after it settled
	if r.sched.loadServableSoon(key) {
		t.Fatal("a table whose append fell back is pulled by loads")
	}
	r.sched.finishAppend(&ingestJob{m: job.m, key: key})
	if !r.sched.loadServableSoon(key) {
		t.Fatal("a settled append did not make the table servable again")
	}
}

// Past the cap over all tables, a load does not wait.
func TestLoadOverTheGlobalCapDoesNotWait(t *testing.T) {
	s, key := newLoadScheduler(t)
	s.loadWaiterN = loadMaxWaiters
	began := time.Now()
	s.AwaitFreshForRead(context.Background(), "db", "m")
	if took := time.Since(began); took > 100*time.Millisecond || len(s.loadWaiters[key]) != 0 {
		t.Fatalf("waited %v, registered %d", took, len(s.loadWaiters[key]))
	}
}

// timedPass sets inTimedPass for the whole pass, and clears it after.
type passFlagSource struct {
	*recordingSource
	s    **Scheduler
	seen atomic.Bool
}

func (p *passFlagSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	if (*p.s).inTimedPass.Load() {
		p.seen.Store(true)
	}
	return p.recordingSource.FilesAndLocal(ctx, m)
}

func TestTimedPassMarksItself(t *testing.T) {
	r := newAppendRig(t)
	src := &passFlagSource{recordingSource: r.src}
	src.s = &r.sched
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: src, Interval: time.Hour, Logger: r.sched.logger,
		IngestInterval: time.Hour, IngestAppend: true, LoadInterval: time.Second})
	r.sched.started = time.Now().Add(-time.Second)
	r.newFile()
	r.sched.timedPass(context.Background())
	if !src.seen.Load() {
		t.Fatal("inTimedPass was not set while the pass listed the measurement")
	}
	if r.sched.inTimedPass.Load() {
		t.Fatal("inTimedPass still set after the pass")
	}
}

// A dropped database lets its tables' loads go as skipped.
func TestDroppedDatabaseSkipsLoads(t *testing.T) {
	s, key := newLoadScheduler(t)
	w := addWaiter(s, key, false)
	s.forgetDatabase("db")
	if !w.closed || w.outcome != metrics.IcebergLoadFreshSkipped {
		t.Fatalf("closed %v outcome %d, want skipped", w.closed, w.outcome)
	}
}
