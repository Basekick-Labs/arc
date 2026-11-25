package iceberg

import (
	"context"
	"testing"
	"time"
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
