package iceberg

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// newDrainRig is an ingest rig whose requests are spaced by an hour, so a
// second request for a table is never due before the process "exits".
func newDrainRig(t *testing.T, gate WriterGate) (*exportRig, *recordingSource) {
	t.Helper()
	r := newExportRig(t, time.Hour)
	src := &recordingSource{StorageWalkSource: r.src, fail: map[string]bool{}}
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: src, Interval: time.Hour, Logger: r.sched.logger,
		IngestInterval: time.Hour, Gate: gate})
	t.Cleanup(func() { metrics.Get().SetIcebergOldestPendingExport(time.Time{}) })
	return r, src
}

// #1886: a request that is queued but not yet due when the loop stops (the
// table was exported less than an interval ago) is served by the drain.
// Without the drain it waited for a process that was exiting.
func TestDrainStopped_ExportsARequestNotYetDue(t *testing.T) {
	r, _ := newDrainRig(t, nil)
	r.sched.Start()
	first := ingestName("cpu", time.Now())
	r.writeIn(t, "cpu", first)
	r.sched.RequestIngestReconcile("mydb", "cpu")
	waitLive(t, r, first)

	second := ingestName("cpu", time.Now().Add(time.Millisecond))
	r.writeIn(t, "cpu", second)
	r.sched.RequestIngestReconcile("mydb", "cpu") // due an hour from now
	if !r.sched.StopWithin(5 * time.Second) {
		t.Fatal("loop did not stop")
	}
	if got := strings.Join(r.live(t), ","); strings.Contains(got, second) {
		t.Fatalf("second file exported before the drain (%s): the test does not exercise the spacing", got)
	}
	if n := r.sched.DrainStopped(context.Background()); n != 1 {
		t.Fatalf("drain exported %d tables, want 1", n)
	}
	want := []string{first, second}
	if second < first {
		want = []string{second, first}
	}
	if got := strings.Join(r.live(t), ","); got != strings.Join(want, ",") {
		t.Fatalf("table = %s, want %s", got, strings.Join(want, ","))
	}
}

// #1886: the buffer's final flush reports its file after the loop stopped.
// The drain exports it.
func TestDrainStopped_ExportsAFileReportedAfterTheLoopStopped(t *testing.T) {
	r, _ := newDrainRig(t, nil)
	r.sched.Start()
	if !r.sched.StopWithin(5 * time.Second) {
		t.Fatal("loop did not stop")
	}
	name := ingestName("cpu", time.Now())
	r.writeIn(t, "cpu", name)
	r.sched.RequestIngestReconcile("mydb", "cpu")
	if n := r.sched.DrainStopped(context.Background()); n != 1 {
		t.Fatalf("drain exported %d tables, want 1", n)
	}
	if got := strings.Join(r.live(t), ","); got != name {
		t.Fatalf("table = %q, want %s", got, name)
	}
}

// A table whose export fails is tried once per drain, not back to back until
// the deadline, and the other tables are still exported.
func TestDrainStopped_TriesAFailingTableOnce(t *testing.T) {
	r, src := newDrainRig(t, nil)
	r.sched.Start()
	if !r.sched.StopWithin(5 * time.Second) {
		t.Fatal("loop did not stop")
	}
	src.fail["mem"] = true
	r.writeIn(t, "mem", ingestName("mem", time.Now()))
	r.sched.RequestIngestReconcile("mydb", "mem")
	name := ingestName("cpu", time.Now())
	r.writeIn(t, "cpu", name)
	r.sched.RequestIngestReconcile("mydb", "cpu")

	// A pass reads footers only with more than the commit reserve (20 s) to
	// its deadline: give the drain a minute, as a long shutdown would.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	start := time.Now()
	if n := r.sched.DrainStopped(ctx); n != 1 {
		t.Fatalf("drain exported %d tables, want 1 (cpu)", n)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("drain took %v: it retried the failing table", took)
	}
	memListings := strings.Count(src.order(), "mem")
	if memListings != 1 {
		t.Fatalf("failing table listed %d times (%s), want once", memListings, src.order())
	}
	if got := strings.Join(r.live(t), ","); got != name {
		t.Fatalf("cpu table = %q, want %s", got, name)
	}
}

// Where another node exports, the drain exports nothing and forgets the
// requests, as the loop does.
func TestDrainStopped_ClosedGateExportsNothing(t *testing.T) {
	r, src := newDrainRig(t, &flagGate{on: false})
	r.sched.Start()
	if !r.sched.StopWithin(5 * time.Second) {
		t.Fatal("loop did not stop")
	}
	r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
	r.sched.RequestIngestReconcile("mydb", "cpu")
	if n := r.sched.DrainStopped(context.Background()); n != 0 {
		t.Fatalf("drain exported %d tables on a closed gate", n)
	}
	if got := src.order(); got != "" {
		t.Fatalf("listed %q on a closed gate, want nothing", got)
	}
}

// A scheduler that never started (the export was never turned on), or whose
// loop is still running, is not drained.
func TestDrainStopped_OnlyAfterTheLoopEnded(t *testing.T) {
	t.Run("never started", func(t *testing.T) {
		r, src := newDrainRig(t, nil)
		r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
		r.sched.RequestIngestReconcile("mydb", "cpu")
		if !r.sched.StopWithin(time.Second) {
			t.Fatal("StopWithin on a scheduler that never started reported a running loop")
		}
		if n := r.sched.DrainStopped(context.Background()); n != 0 || src.order() != "" {
			t.Fatalf("drained %d tables (listed %q) of a scheduler that never started", n, src.order())
		}
	})
	t.Run("still running", func(t *testing.T) {
		r, _ := newDrainRig(t, nil)
		r.sched.Start()
		t.Cleanup(r.sched.Stop)
		if n := r.sched.DrainStopped(context.Background()); n != 0 {
			t.Fatalf("drained %d tables while the loop still ran", n)
		}
	})
}

// A drain whose deadline has passed exports nothing: the request stays
// queued and the next start's pass reconciles the table.
func TestDrainStopped_ExpiredContext(t *testing.T) {
	r, src := newDrainRig(t, nil)
	r.sched.Start()
	if !r.sched.StopWithin(5 * time.Second) {
		t.Fatal("loop did not stop")
	}
	r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
	r.sched.RequestIngestReconcile("mydb", "cpu")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n := r.sched.DrainStopped(ctx); n != 0 || src.order() != "" {
		t.Fatalf("drained %d tables (listed %q) on an expired context", n, src.order())
	}
}
