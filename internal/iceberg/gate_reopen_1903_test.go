package iceberg

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// #1903: a node that loses the export role and gets it back without a
// restart must not append onto the table as it left it. While another node
// exported, this node's ingest reports were dropped and files it had exported
// went away (compaction inputs, retention, DELETE); appending only the new
// file would publish a table that names the gone files and misses the ones
// written in between. The first export after the role returns must be a pass.
// Both ingest paths: serial (concurrency 1) and batched (the default, 8).
func TestGateReopenDoesNotAppendOntoAStaleTable(t *testing.T) {
	for _, conc := range []int{1, 8} {
		t.Run("concurrency="+strconv.Itoa(conc), func(t *testing.T) {
			r := newAppendRig(t)
			gate := &flagGate{on: true}
			r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: r.src, Interval: time.Hour, Logger: r.sched.logger,
				IngestInterval: time.Millisecond, IngestAppend: true, Gate: gate, IngestConcurrency: conc})
			r.sched.started = time.Now().Add(-time.Second)
			first := r.settled()

			// Another node exports: this node's report is dropped, and a
			// file its table names goes away meanwhile.
			gate.on = false
			gap := r.newFile()
			r.report(gap)
			r.serve()
			if err := os.Remove(filepath.Join(r.root, filepath.FromSlash(appendDir+first))); err != nil {
				t.Fatal(err)
			}

			// The role comes back to this node, which has not restarted.
			gate.on = true
			fresh := r.newFile()
			before := r.listings()
			r.report(fresh)
			r.serve()
			r.wantOnce(gap, fresh)
			if got := r.listings() - before; got != 1 {
				t.Fatalf("the first export after the role returned listed the measurement %d times, want 1 (a pass)", got)
			}
		})
	}
}

// atomicGate is a gate the test flips while the loop runs; calls counts the
// checks, so a test can wait until the loop has seen a change.
type atomicGate struct {
	on    atomic.Bool
	calls atomic.Int64
}

func (g *atomicGate) CanRun() bool { g.calls.Add(1); return g.on.Load() }

// gateFunc adapts a func to WriterGate.
type gateFunc func() bool

func (f gateFunc) CanRun() bool { return f() }

// startGated starts a scheduler on the rig with gate g, a poll of poll and the
// timed interval given, and waits for its start-up pass.
func startGated(t *testing.T, r *appendRig, src FileSetSourceWithSchema, g WriterGate, interval, poll time.Duration) {
	t.Helper()
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: src, Interval: interval, Logger: r.sched.logger,
		IngestInterval: time.Millisecond, IngestAppend: true, Gate: g})
	r.sched.started = time.Now().Add(-time.Second)
	r.sched.gatePoll = poll
	r.newFile()
	r.sched.Start()
	t.Cleanup(r.sched.Stop)
	// Until the start-up pass has published the file, not just listed it:
	// a pass still running would answer the tests' gate checks itself.
	waitUntil(t, "the start-up pass did not publish the file", func() bool { return r.tableFiles() >= 1 })
}

// tableFiles is how many data files the published table holds, 0 while it
// does not exist yet (no t.Fatal: polled while the loop runs).
func (r *appendRig) tableFiles() int {
	tbl, err := r.exp.catalog.LoadTable(context.Background(), r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		return 0
	}
	tasks, err := tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		return 0
	}
	return len(tasks)
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// #1903: with no traffic at all, the loop still sees the role leave and come
// back, and runs a pass at once rather than at the next tick (an hour here),
// so the table the REST catalog serves again is brought in line promptly.
func TestGateReopenRunsAPassWithoutWaitingForTheTimer(t *testing.T) {
	r := newAppendRig(t)
	gate := &atomicGate{}
	gate.on.Store(true)
	startGated(t, r, r.src, gate, time.Hour, 10*time.Millisecond)
	afterStart := r.listings()

	gate.on.Store(false)
	// Ten polls' worth: the gate's call count cannot tell a poll from the
	// canRun checks of work still running from the start-up pass, and a
	// close that no gateOpen saw is, correctly, no reopen.
	time.Sleep(100 * time.Millisecond)
	if got := r.listings(); got != afterStart {
		t.Fatalf("listed %d times while another node exported", got-afterStart)
	}
	gate.on.Store(true)
	waitUntil(t, "no pass after the role came back (the next tick is an hour away)", func() bool { return r.listings() > afterStart })
	time.Sleep(100 * time.Millisecond)
	if got := r.listings() - afterStart; got != 1 {
		t.Fatalf("%d passes after one reopen, want 1", got)
	}
}

// A reopen first seen by a timed pass's own gate check is answered by that
// pass: no second pass right after it.
func TestGateReopenSeenByATimedPassRunsOnePass(t *testing.T) {
	r := newAppendRig(t)
	gate := &atomicGate{}
	gate.on.Store(true)
	startGated(t, r, r.src, gate, 300*time.Millisecond, 0) // only timed passes ask the gate
	gate.on.Store(false)
	time.Sleep(400 * time.Millisecond) // past the 300 ms interval: a timed pass saw it closed
	base := r.listings()
	gate.on.Store(true)
	waitUntil(t, "no pass after the reopen", func() bool { return r.listings() > base })
	time.Sleep(100 * time.Millisecond) // well inside the 300 ms interval
	if got := r.listings() - base; got != 1 {
		t.Fatalf("%d passes right after a reopen seen by the timed pass, want 1", got)
	}
}

// A reopen whose prompt pass finds the gate closed again must not make the
// loop ask for a pass at once, forever.
func TestGateReopenThenClosedAgainDoesNotSpin(t *testing.T) {
	r := newAppendRig(t)
	var mode atomic.Int32 // 0 open, 1 closed, 2 open for one check then closed
	var calls atomic.Int64
	gate := gateFunc(func() bool {
		calls.Add(1)
		switch mode.Load() {
		case 0:
			return true
		case 2:
			return mode.CompareAndSwap(2, 1)
		}
		return false
	})
	startGated(t, r, r.src, gate, time.Hour, 10*time.Millisecond)
	mode.Store(1)
	time.Sleep(50 * time.Millisecond)
	mode.Store(2) // a poll sees it open once; the prompt pass finds it closed
	waitUntil(t, "the reopen was never seen", func() bool { return mode.Load() == 1 })
	before := calls.Load()
	time.Sleep(200 * time.Millisecond)
	if got := calls.Load() - before; got > 100 {
		t.Fatalf("gate asked %d times in 200 ms: the loop spins", got)
	}
}

// A reopen seen while a restore holds the export paused runs the paused
// runPass once, not at once forever.
func TestGateReopenUnderRestorePauseDoesNotSpin(t *testing.T) {
	r := newAppendRig(t)
	gate := &atomicGate{}
	gate.on.Store(true)
	startGated(t, r, r.src, gate, time.Hour, 10*time.Millisecond)
	resume, err := r.exp.PauseExportForRestore(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(resume)
	gate.on.Store(false)
	time.Sleep(50 * time.Millisecond)
	gate.on.Store(true)
	time.Sleep(50 * time.Millisecond)
	before := lagGeneration(r.exp)
	time.Sleep(200 * time.Millisecond)
	if got := lagGeneration(r.exp) - before; got > 5 {
		t.Fatalf("the paused pass ran %d times in 200 ms: the loop spins", got)
	}
}

// lagGeneration counts clearLastLag calls, which a paused runPass makes.
func lagGeneration(e *Exporter) uint64 {
	e.lag.mu.Lock()
	defer e.lag.mu.Unlock()
	return uint64(e.lag.gen)
}

// armingSource makes the gate answer closed once, at the pass's mid-pass check,
// when armed: the pass lists measurements, then checks the gate per batch.
type armingSource struct {
	*recordingSource
	failNext *atomic.Bool
	arm      atomic.Bool
}

func (a *armingSource) Measurements(ctx context.Context) ([]Measurement, error) {
	if a.arm.CompareAndSwap(true, false) {
		a.failNext.Store(true)
	}
	return a.recordingSource.Measurements(ctx)
}

// The prompt pass is stopped by the mid-pass gate check, and the lease is back
// before any poll or request saw it gone: a pass must still follow, or idle
// tables stay as this node left them until the next tick.
func TestGateReopenAbortedPromptPassIsRetried(t *testing.T) {
	r := newAppendRig(t)
	var on, failNext atomic.Bool
	on.Store(true)
	src := &armingSource{recordingSource: r.src, failNext: &failNext}
	gate := gateFunc(func() bool { return on.Load() && !failNext.CompareAndSwap(true, false) })
	startGated(t, r, src, gate, time.Hour, 10*time.Millisecond)
	base := r.listings()
	on.Store(false)
	time.Sleep(60 * time.Millisecond)
	src.arm.Store(true)
	on.Store(true)
	waitUntil(t, "the prompt pass never listed measurements", func() bool { return !src.arm.Load() })
	time.Sleep(300 * time.Millisecond)
	if got := r.listings() - base; got < 1 {
		t.Fatalf("no table pass after the aborted prompt pass (listings +%d)", got)
	}
}

// #1903 with #1886's shutdown drain: a role that came back while the loop was
// already stopping is first seen by DrainStopped's gate check. The drain must
// take that table off the append path too, or it appends the last flushed
// file onto the table as this node left it.
func TestGateReopenFirstSeenByTheShutdownDrainDoesNotAppend(t *testing.T) {
	r := newAppendRig(t)
	gate := &atomicGate{}
	gate.on.Store(true)
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: r.src, Interval: time.Hour, Logger: r.sched.logger,
		IngestInterval: time.Millisecond, IngestAppend: true, Gate: gate})
	r.sched.started = time.Now().Add(-time.Second)
	first := r.settled()

	// Another node exports: the report is dropped and a file goes away.
	gate.on.Store(false)
	gap := r.newFile()
	r.report(gap)
	r.serve()
	if err := os.Remove(filepath.Join(r.root, filepath.FromSlash(appendDir+first))); err != nil {
		t.Fatal(err)
	}

	// Shutdown starts while the gate is still closed: the loop ends having
	// exported nothing.
	r.sched.Start()
	if !r.sched.StopWithin(5 * time.Second) {
		t.Fatal("the loop did not stop")
	}

	// The role comes back, and the final flush reports one more file.
	gate.on.Store(true)
	fresh := r.newFile()
	before := r.listings()
	r.report(fresh)
	if n := r.sched.DrainStopped(context.Background()); n != 1 {
		t.Fatalf("drain exported %d tables, want 1", n)
	}
	r.wantOnce(gap, fresh)
	if got := r.listings() - before; got != 1 {
		t.Fatalf("the drain's export after the role returned listed the measurement %d times, want 1 (a pass)", got)
	}
}
