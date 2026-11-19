package iceberg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// ingestName is an ingest file's name as the flush builds it, stamped at.
func ingestName(measurement string, at time.Time) string {
	at = at.UTC()
	return fmt.Sprintf("%s_%s_%09d.parquet", measurement, at.Format("20060102_150405"), at.Nanosecond())
}

// recordingSource notes which measurements are listed, in order, and can
// fail the listing of some.
type recordingSource struct {
	*StorageWalkSource
	mu     sync.Mutex
	listed []string
	fail   map[string]bool
}

func (r *recordingSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	r.mu.Lock()
	r.listed = append(r.listed, m.Measurement)
	fail := r.fail[m.Measurement]
	r.mu.Unlock()
	if fail {
		return nil, nil, errors.New("injected listing failure")
	}
	return r.StorageWalkSource.FilesAndLocal(ctx, m)
}

func (r *recordingSource) order() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.listed, ",")
}

// newIngestRig is an export rig whose scheduler takes ingest requests spaced
// by interval, listing through a recording source. The loop is not started:
// the tests drive the scheduler's steps themselves.
func newIngestRig(t *testing.T, interval time.Duration) (*exportRig, *recordingSource) {
	t.Helper()
	r := newExportRig(t, time.Hour)
	src := &recordingSource{StorageWalkSource: r.src, fail: map[string]bool{}}
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: src, Interval: time.Hour, Logger: r.sched.logger, IngestInterval: interval})
	t.Cleanup(func() { metrics.Get().SetIcebergOldestPendingExport(time.Time{}) })
	return r, src
}

func (r *exportRig) writeIn(t *testing.T, measurement, name string) {
	t.Helper()
	writeArcStyleParquet(t, filepath.Join(r.root, "mydb", measurement, "2026", "07", "14", "15", name), 1_700_000_000_000_000, 5)
}

func snapshotInt(t *testing.T, key string) int64 {
	t.Helper()
	v, ok := metrics.Get().Snapshot()[key].(int64)
	if !ok {
		t.Fatalf("metric %q missing or not an integer", key)
	}
	return v
}

func snapshotFloat(t *testing.T, key string) float64 {
	t.Helper()
	v, ok := metrics.Get().Snapshot()[key].(float64)
	if !ok {
		t.Fatalf("metric %q missing or not a float", key)
	}
	return v
}

// An ingest flush's request exports the file with the timer an hour away,
// through the running loop. With the trigger off the request does nothing
// and the file waits for a pass.
func TestScheduler_IngestRequestExportsWithoutTheTimer(t *testing.T) {
	t.Run("on", func(t *testing.T) {
		r := newExportRig(t, time.Hour)
		r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: r.src, Interval: time.Hour, Logger: r.sched.logger, IngestInterval: 20 * time.Millisecond})
		t.Cleanup(func() { metrics.Get().SetIcebergOldestPendingExport(time.Time{}) })
		r.sched.Start()
		t.Cleanup(r.sched.Stop)
		name := ingestName("cpu", time.Now())
		r.writeIn(t, "cpu", name)
		r.sched.RequestIngestReconcile("mydb", "cpu")
		waitLive(t, r, name)
		// A second file after the first export: the request waits out the
		// spacing on the loop's timer and is then served, with no other wake.
		second := ingestName("cpu", time.Now().Add(time.Millisecond))
		r.writeIn(t, "cpu", second)
		r.sched.RequestIngestReconcile("mydb", "cpu")
		want := []string{name, second}
		if second < name {
			want = []string{second, name}
		}
		waitLive(t, r, strings.Join(want, ","))
	})
	t.Run("off", func(t *testing.T) {
		r, src := newIngestRig(t, 0)
		r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
		r.sched.RequestIngestReconcile("mydb", "cpu")
		r.sched.serviceIngest(context.Background(), ingestBatch)
		if got := src.order(); got != "" {
			t.Errorf("listed %q with iceberg.ingest_export_interval = 0, want nothing", got)
		}
		if got := snapshotFloat(t, "iceberg_export_pending_seconds"); got != 0 {
			t.Errorf("pending = %v s with the trigger off, want 0", got)
		}
	})
}

// The spacing counts from the last attempt an ingest request caused, not
// from the last time the table changed: a table whose reconcile fails must
// not be listed again at every flush.
func TestScheduler_IngestRequestsAreSpacedFromTheAttempt(t *testing.T) {
	const interval = 250 * time.Millisecond
	for name, fail := range map[string]bool{"a table that reconciles": false, "a table whose listing fails": true} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			r, src := newIngestRig(t, interval)
			src.fail["cpu"] = fail
			r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
			for i := 0; i < 10; i++ { // ten flushes inside one interval
				r.sched.RequestIngestReconcile("mydb", "cpu")
				r.sched.serviceIngest(ctx, ingestBatch)
			}
			if got := src.order(); got != "cpu" {
				t.Fatalf("listed %q for ten requests inside the interval, want one listing", got)
			}
			if due, ok := r.sched.nextIngestDue(); fail && (!ok || time.Until(due) <= 0 || time.Until(due) > interval) {
				t.Errorf("the failed table's next request is due in %v (queued=%v), want within the interval", time.Until(due), ok)
			}
			time.Sleep(interval + 30*time.Millisecond)
			r.sched.RequestIngestReconcile("mydb", "cpu")
			r.sched.serviceIngest(ctx, ingestBatch)
			if got := src.order(); got != "cpu,cpu" {
				t.Errorf("listed %q after the interval, want a second listing", got)
			}
		})
	}
}

// Requests of the other kinds are served before an ingest request, and a
// capped call serves no more ingest requests than its cap.
func TestScheduler_IngestRequestsGoLast(t *testing.T) {
	ctx := context.Background()
	r, src := newIngestRig(t, time.Minute)
	for _, m := range []string{"aaa", "bbb", "cpu", "zzz"} {
		r.writeIn(t, m, ingestName(m, time.Now()))
	}
	// Ingest requests first, in an order the names would not give...
	r.sched.RequestIngestReconcile("mydb", "cpu")
	time.Sleep(2 * time.Millisecond)
	r.sched.RequestIngestReconcile("mydb", "aaa")
	time.Sleep(2 * time.Millisecond)
	r.sched.RequestIngestReconcile("mydb", "bbb")
	// ...then a compaction's request, which must still be served first.
	r.sched.RequestReconcile("mydb", "zzz")

	r.sched.serviceIngest(ctx, 1)
	if got := src.order(); got != "zzz,cpu" {
		t.Fatalf("served %q, want the compaction's table, then the one ingest request the cap allows (the one waiting longest)", got)
	}
	r.sched.serviceIngest(ctx, ingestBatch)
	if got := src.order(); got != "zzz,cpu,aaa,bbb" {
		t.Errorf("served %q, want the remaining ingest requests in the order they became due", got)
	}
}

// The pending gauge is how long the oldest reported file has waited for a
// reconcile that covers it. A reconcile that started before the report does
// not cover it, nor does one that failed or was busy; on a node that does not
// export nothing waits.
func TestScheduler_IngestPendingTracksUncoveredReports(t *testing.T) {
	pending := func() float64 { return snapshotFloat(t, "iceberg_export_pending_seconds") }
	const key = "mydb\x00cpu"

	r, _ := newIngestRig(t, time.Minute)
	if got := pending(); got != 0 {
		t.Fatalf("pending = %v before any report", got)
	}
	r.sched.RequestIngestReconcile("mydb", "cpu")
	time.Sleep(15 * time.Millisecond)
	if got := pending(); got < 0.010 {
		t.Fatalf("pending = %v s after a report 15 ms ago, want it counting", got)
	}
	r.sched.noteIngestCovered(key, time.Now().Add(-time.Hour), outcomeReconciled, nil) // started before the report
	r.sched.noteIngestCovered(key, time.Now(), outcomeReconciled, errors.New("failed"))
	r.sched.noteIngestCovered(key, time.Now(), outcomeBusy, nil)
	r.sched.noteIngestCovered(key, time.Now(), outcomeInProgress, nil)
	if got := pending(); got == 0 {
		t.Fatal("a reconcile that started before the report, failed, was busy or left files behind cleared the pending report")
	} else if got > 5 {
		t.Fatalf("pending = %v s for a report made a moment ago: a reconcile that started before it moved its time back", got)
	}
	r.sched.noteIngestCovered(key, time.Now(), outcomeUnchanged, nil)
	if got := pending(); got != 0 {
		t.Errorf("pending = %v s after a reconcile that started after the report went through, want 0", got)
	}

	// A file reported while a reconcile runs may have missed its listing: the
	// report and its request outlive that reconcile, counted from its start.
	r.sched.RequestIngestReconcile("mydb", "cpu") // before it started
	_, _, _ = r.sched.takeDueIngest(time.Now())   // the reconcile takes the request...
	time.Sleep(5 * time.Millisecond)
	started := time.Now()
	time.Sleep(5 * time.Millisecond)
	r.sched.RequestIngestReconcile("mydb", "cpu") // ...and a flush reports another file while it runs
	r.sched.noteIngestCovered(key, started, outcomeReconciled, nil)
	if _, queued := r.sched.nextIngestDue(); !queued {
		t.Fatal("the request for a file reported during a reconcile was dropped when that reconcile finished")
	}
	if got := pending(); got == 0 {
		t.Fatal("a file reported during a reconcile no longer counts as pending")
	}
	r.sched.noteIngestCovered(key, time.Now(), outcomeReconciled, nil)
	if _, queued := r.sched.nextIngestDue(); queued || pending() != 0 {
		t.Fatal("a reconcile that started after every report left a request or a pending report")
	}

	// The oldest of several tables is what is reported.
	r.sched.RequestIngestReconcile("mydb", "aaa")
	time.Sleep(20 * time.Millisecond)
	r.sched.RequestIngestReconcile("mydb", "bbb")
	r.sched.noteIngestCovered("mydb\x00aaa", time.Now(), outcomeReconciled, nil)
	if got := pending(); got >= 0.020 {
		t.Errorf("pending = %v s after the older table was covered, want the newer table's wait", got)
	}

	// A node that does not export drops requests and reports alike.
	g := &flagGate{on: false}
	r.sched.gate = g
	r.sched.serviceIngest(context.Background(), ingestBatch)
	if got := pending(); got != 0 {
		t.Errorf("pending = %v s on a node the gate keeps from exporting, want 0", got)
	}
	if _, queued := r.sched.nextIngestDue(); queued {
		t.Error("ingest requests are still queued on a node that does not export")
	}
}

// The lag of an ingest file is observed when the table version that holds it
// is published: now minus the time in its name. Compacted files, DELETE
// rewrites and files written before the process started are not observed;
// the last are counted as backlog.
func TestExportLagObservedForIngestFilesAtPublish(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	count, sum, backlog := snapshotInt(t, "iceberg_export_lag_count"), snapshotFloat(t, "iceberg_export_lag_sum_seconds"), snapshotInt(t, "iceberg_export_backlog_files")

	// A file counts as fresh when it was written after the exporter started:
	// move that start back, so a file written 3 s ago is fresh.
	r.exp.lag.started = time.Now().Add(-time.Minute)
	fresh := ingestName("cpu", time.Now().Add(-3*time.Second))
	old := ingestName("cpu", time.Now().Add(-24*time.Hour))
	r.writeIn(t, "cpu", fresh)
	r.writeIn(t, "cpu", old)
	r.writeIn(t, "cpu", strings.TrimSuffix(ingestName("cpu", time.Now()), ".parquet")+"_rewrite_1700000000000000000.parquet")
	r.writeIn(t, "cpu", "cpu_20260714_150000_000000001_b0_compacted.parquet")
	r.writeIn(t, "cpu", "hand-placed.parquet")
	r.sched.runPass(ctx)
	if got := len(r.live(t)); got != 5 {
		t.Fatalf("%d files in the table, want 5", got)
	}
	if got := snapshotInt(t, "iceberg_export_lag_count") - count; got != 1 {
		t.Errorf("%d lag observations, want 1: only the fresh ingest file", got)
	}
	if got := snapshotFloat(t, "iceberg_export_lag_sum_seconds") - sum; got < 3 || got > 30 {
		t.Errorf("observed %.2f s for a file written 3 s ago, want a little over 3", got)
	}
	if got := snapshotInt(t, "iceberg_export_backlog_files") - backlog; got != 1 {
		t.Errorf("%d backlog files, want 1: the ingest file written before the exporter started", got)
	}
}

// A file committed by a pass whose publish failed is observed when a later
// pass publishes it, once, with the whole wait.
func TestExportLagWaitsForAPublishThatLands(t *testing.T) {
	if os.Geteuid() == 0 || runtime.GOOS == "windows" {
		t.Skip("needs a hint write that fails")
	}
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
	r.sched.runPass(ctx)
	count, sum := snapshotInt(t, "iceberg_export_lag_count"), snapshotFloat(t, "iceberg_export_lag_sum_seconds")

	// The hint cannot be written: a directory stands where it goes. Commits
	// and the v<N> copy still land.
	hint := filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata", "version-hint.text")
	if err := os.Remove(hint); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(hint, 0o755); err != nil {
		t.Fatal(err)
	}
	second := ingestName("cpu", time.Now().Add(time.Millisecond))
	r.writeIn(t, "cpu", second)
	r.sched.runPass(ctx)
	if got := len(r.live(t)); got != 2 {
		t.Fatalf("%d files in the table, want the second file committed", got)
	}
	if got := snapshotInt(t, "iceberg_export_lag_count") - count; got != 0 {
		t.Fatalf("%d lag observations for a file whose table version was not published, want 0", got)
	}

	time.Sleep(300 * time.Millisecond)
	if err := os.Remove(hint); err != nil {
		t.Fatal(err)
	}
	r.sched.runPass(ctx) // commits nothing: it publishes what the last pass committed
	if got := snapshotInt(t, "iceberg_export_lag_count") - count; got != 1 {
		t.Fatalf("%d lag observations once the publish landed, want 1", got)
	}
	if got := snapshotFloat(t, "iceberg_export_lag_sum_seconds") - sum; got < 0.3 {
		t.Errorf("observed %.3f s, want at least the 0.3 s the file waited for a publish that landed", got)
	}
	r.sched.runPass(ctx)
	if got := snapshotInt(t, "iceberg_export_lag_count") - count; got != 1 {
		t.Errorf("%d lag observations after another pass, want still 1", got)
	}
}
