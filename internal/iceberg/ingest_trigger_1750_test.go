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
	slow   time.Duration // how long a failing listing takes
}

func (r *recordingSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	r.mu.Lock()
	r.listed = append(r.listed, m.Measurement)
	fail, slow := r.fail[m.Measurement], r.slow
	r.mu.Unlock()
	if fail {
		time.Sleep(slow)
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
	// "Settled" is the loop's cached state for the measurement: these tables
	// have it, as after a pass that listed, registered and published.
	for _, k := range []string{key, "mydb\x00aaa", "mydb\x00bbb"} {
		r.sched.state[k] = measurementState{}
	}
	r.sched.RequestIngestReconcile("mydb", "cpu")
	time.Sleep(15 * time.Millisecond)
	if got := pending(); got < 0.010 {
		t.Fatalf("pending = %v s after a report 15 ms ago, want it counting", got)
	}
	r.sched.noteIngestCovered(key, time.Now().Add(-time.Hour), outcomeReconciled, true, nil) // started before the report
	r.sched.noteIngestCovered(key, time.Now(), outcomeReconciled, true, errors.New("failed"))
	r.sched.noteIngestCovered(key, time.Now(), outcomeBusy, false, nil)
	r.sched.noteIngestCovered(key, time.Now(), outcomeInProgress, false, nil)
	// One that committed and could not publish (or lost the writer role)
	// returns "reconciled" too, and leaves no cached state: the file is not
	// readable yet.
	delete(r.sched.state, key)
	r.sched.noteIngestCovered(key, time.Now(), outcomeReconciled, true, nil)
	r.sched.state[key] = measurementState{}
	if got := pending(); got == 0 {
		t.Fatal("a reconcile that started before the report, failed, was busy, left files behind or did not publish cleared the pending report")
	} else if got > 5 {
		t.Fatalf("pending = %v s for a report made a moment ago: a reconcile that started before it moved its time back", got)
	}
	r.sched.noteIngestCovered(key, time.Now(), outcomeUnchanged, false, nil)
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
	r.sched.noteIngestCovered(key, started, outcomeReconciled, true, nil)
	if _, queued := r.sched.nextIngestDue(); !queued {
		t.Fatal("the request for a file reported during a reconcile was dropped when that reconcile finished")
	}
	if got := pending(); got == 0 {
		t.Fatal("a file reported during a reconcile no longer counts as pending")
	}
	r.sched.noteIngestCovered(key, time.Now(), outcomeReconciled, true, nil)
	if _, queued := r.sched.nextIngestDue(); queued || pending() != 0 {
		t.Fatal("a reconcile that started after every report left a request or a pending report")
	}

	// The oldest of several tables is what is reported.
	r.sched.RequestIngestReconcile("mydb", "aaa")
	time.Sleep(300 * time.Millisecond)
	r.sched.RequestIngestReconcile("mydb", "bbb")
	if got := pending(); got < 0.3 {
		t.Errorf("pending = %v s with a table waiting 300 ms, want the older table's wait", got)
	}
	r.sched.noteIngestCovered("mydb\x00aaa", time.Now(), outcomeReconciled, true, nil)
	if got := pending(); got == 0 || got >= 0.3 {
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

// A reconcile of any kind that changed the table restarts the spacing for a
// request that is already queued: an ingest request must not publish a
// version right behind a compaction's or a timed pass's.
func TestScheduler_AnIngestRequestWaitsBehindAnotherReconcilesPublish(t *testing.T) {
	const interval = time.Minute
	const key = "mydb\x00cpu"
	r, _ := newIngestRig(t, interval)
	r.sched.state[key] = measurementState{}
	started := time.Now()
	time.Sleep(2 * time.Millisecond)
	r.sched.RequestIngestReconcile("mydb", "cpu") // a flush, while another reconcile of the table runs
	if due, ok := r.sched.nextIngestDue(); !ok || time.Until(due) > time.Second {
		t.Fatalf("before the other reconcile ends the request is due in %v (queued=%v), want now", time.Until(due), ok)
	}
	r.sched.noteIngestCovered(key, started, outcomeReconciled, true, nil) // the other reconcile ends, having changed the table
	due, ok := r.sched.nextIngestDue()
	if !ok {
		t.Fatal("the request was dropped: its file was reported after the reconcile started and may have missed it")
	}
	if wait := time.Until(due); wait < interval-5*time.Second || wait > interval {
		t.Errorf("the request is due in %v, want a full interval after the reconcile that changed the table", wait)
	}
	// One that changed nothing publishes nothing: the request stays due.
	r2, _ := newIngestRig(t, interval)
	r2.sched.state[key] = measurementState{}
	started = time.Now()
	time.Sleep(2 * time.Millisecond)
	r2.sched.RequestIngestReconcile("mydb", "cpu")
	r2.sched.noteIngestCovered(key, started, outcomeUnchanged, false, nil)
	if due, ok := r2.sched.nextIngestDue(); !ok || time.Until(due) > time.Second {
		t.Errorf("after a reconcile that changed nothing the request is due in %v (queued=%v), want now", time.Until(due), ok)
	}
}

// A file committed by a pass that could not publish is not exported yet: its
// report keeps waiting, the request comes back on its own at the next
// allowed attempt, and the lag is observed when a publish lands.
func TestScheduler_IngestRequestSurvivesAPublishThatFails(t *testing.T) {
	if os.Geteuid() == 0 || runtime.GOOS == "windows" {
		t.Skip("needs a hint write that fails")
	}
	const interval = 200 * time.Millisecond
	ctx := context.Background()
	r, src := newIngestRig(t, interval)
	first := ingestName("cpu", time.Now())
	r.writeIn(t, "cpu", first)
	r.sched.RequestIngestReconcile("mydb", "cpu")
	r.sched.serviceIngest(ctx, ingestBatch)
	if got := snapshotFloat(t, "iceberg_export_pending_seconds"); got != 0 {
		t.Fatalf("pending = %v s after a settled export, want 0", got)
	}
	count := snapshotInt(t, "iceberg_export_lag_count")

	hint := filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata", "version-hint.text")
	if err := os.Remove(hint); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(hint, 0o755); err != nil { // a directory where the hint goes: the write fails
		t.Fatal(err)
	}
	time.Sleep(interval + 20*time.Millisecond)
	r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
	r.sched.RequestIngestReconcile("mydb", "cpu")
	r.sched.serviceIngest(ctx, ingestBatch)
	if got := len(r.live(t)); got != 2 {
		t.Fatalf("%d files committed, want 2", got)
	}
	if got := snapshotFloat(t, "iceberg_export_pending_seconds"); got == 0 {
		t.Error("pending = 0 for a file whose table version was not published")
	}
	if due, ok := r.sched.nextIngestDue(); !ok || time.Until(due) > interval {
		t.Fatalf("after a publish that failed the request is due in %v (queued=%v), want it queued for the next attempt with no further flush", time.Until(due), ok)
	}
	if got := snapshotInt(t, "iceberg_export_lag_count") - count; got != 0 {
		t.Errorf("%d lag observations before the publish landed, want 0", got)
	}

	if err := os.Remove(hint); err != nil {
		t.Fatal(err)
	}
	time.Sleep(interval + 20*time.Millisecond)
	listed := len(strings.Split(src.order(), ","))
	r.sched.serviceIngest(ctx, ingestBatch) // no new flush: the requeued request alone
	if got := len(strings.Split(src.order(), ",")); got != listed+1 {
		t.Fatalf("the requeued request was not served (%d listings, then %d)", listed, got)
	}
	if got := snapshotFloat(t, "iceberg_export_pending_seconds"); got != 0 {
		t.Errorf("pending = %v s once the publish landed, want 0", got)
	}
	if got := snapshotInt(t, "iceberg_export_lag_count") - count; got != 1 {
		t.Errorf("%d lag observations once the publish landed, want 1", got)
	}
	if _, queued := r.sched.nextIngestDue(); queued {
		t.Error("a request is still queued after the export settled")
	}
}

// A failed reconcile comes back at the next allowed attempt with no further
// flush, and a dropped database's unpublished files are not kept for a lag
// that will never be observed.
func TestScheduler_FailedIngestReconcileIsRequeued(t *testing.T) {
	const interval = 200 * time.Millisecond
	ctx := context.Background()
	r, src := newIngestRig(t, interval)
	src.fail["cpu"] = true
	r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
	r.sched.RequestIngestReconcile("mydb", "cpu")
	r.sched.serviceIngest(ctx, ingestBatch)
	due, ok := r.sched.nextIngestDue()
	if !ok || time.Until(due) <= 0 || time.Until(due) > interval {
		t.Fatalf("after a failed reconcile the request is due in %v (queued=%v), want within the interval", time.Until(due), ok)
	}
	src.fail["cpu"] = false
	time.Sleep(interval + 20*time.Millisecond)
	r.sched.serviceIngest(ctx, ingestBatch)
	if got := len(r.live(t)); got != 1 {
		t.Errorf("%d files in the table after the retry, want 1", got)
	}
}

func TestExportLagForgetsADroppedDatabase(t *testing.T) {
	r := newExportRig(t, time.Hour)
	mine, other := r.exp.tableIdent("mydb", "cpu"), r.exp.tableIdent("otherdb", "cpu")
	r.exp.lag.pending = map[string][]time.Time{tableLagKey(mine): {time.Now()}, tableLagKey(other): {time.Now()}}
	if err := r.exp.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatal(err)
	}
	if _, kept := r.exp.lag.pending[tableLagKey(mine)]; kept {
		t.Error("the dropped database's unpublished files are still waiting for a lag observation")
	}
	if _, kept := r.exp.lag.pending[tableLagKey(other)]; !kept {
		t.Error("another database's unpublished files were dropped")
	}
	r.exp.forgetExportLag(other)
	if len(r.exp.lag.pending) != 0 {
		t.Errorf("pending = %v after forgetting the last table", r.exp.lag.pending)
	}
}

// An attempt that fails after longer than the interval is retried one
// interval after it ENDED, not the moment it ends: a table that keeps failing
// slowly must not be reconciled back to back.
func TestScheduler_ASlowFailingIngestReconcileIsNotRetriedAtOnce(t *testing.T) {
	const interval = 100 * time.Millisecond
	ctx := context.Background()
	r, src := newIngestRig(t, interval)
	src.fail["cpu"] = true
	src.slow = 3 * interval
	r.writeIn(t, "cpu", ingestName("cpu", time.Now()))
	r.sched.RequestIngestReconcile("mydb", "cpu")
	r.sched.serviceIngest(ctx, ingestBatch) // the batch that would otherwise retry it up to ingestBatch times
	if got := src.order(); got != "cpu" {
		t.Fatalf("listed %q in one turn, want one attempt: the retry was due as soon as the slow attempt ended", got)
	}
	if due, ok := r.sched.nextIngestDue(); !ok || time.Until(due) <= interval/2 || time.Until(due) > interval {
		t.Errorf("the retry is due in %v (queued=%v), want about one interval after the attempt ended", time.Until(due), ok)
	}
}

// A table the timed passes have escalated as busy for too long is not
// retried by ingest requests either.
func TestScheduler_AnEscalatedBusyTableIsLeftToThePasses(t *testing.T) {
	ctx := context.Background()
	r, _ := newIngestRig(t, 50*time.Millisecond)
	busy := &busySource{StorageWalkSource: r.src}
	r.sched.source = busy
	r.writeIn(t, "cpu", ingestName("cpu", time.Now()))

	r.sched.RequestIngestReconcile("mydb", "cpu")
	r.sched.serviceIngest(ctx, ingestBatch)
	if _, queued := r.sched.nextIngestDue(); !queued {
		t.Fatal("a busy table was not requeued")
	}
	// Busy since longer than the escalation: the passes own it.
	r.sched.busySince["mydb\x00cpu"] = time.Now().Add(-busyEscalateAfter - time.Minute)
	time.Sleep(70 * time.Millisecond)
	r.sched.serviceIngest(ctx, ingestBatch)
	if _, queued := r.sched.nextIngestDue(); queued {
		t.Error("an escalated busy table was requeued by an ingest request")
	}
}

// busySource reports the listing as never holding still.
type busySource struct{ *StorageWalkSource }

func (busySource) FilesAndLocal(context.Context, Measurement) ([]FileRef, []string, error) {
	return nil, nil, errSourcesMoving
}

// A pass that ends on a node that is no longer the writer, or fenced, leaves
// its commits for another node to publish: their write times are not kept.
func TestExportLagForgottenWhenThisNodeWillNotPublish(t *testing.T) {
	r := newExportRig(t, time.Hour)
	ident := r.exp.tableIdent("mydb", "cpu")
	seed := func() { r.exp.lag.pending = map[string][]time.Time{tableLagKey(ident): {time.Now()}} }
	kept := func() bool { _, ok := r.exp.lag.pending[tableLagKey(ident)]; return ok }

	seed()
	r.exp.forgetExportLagIfNotPublisher("mydb", "cpu") // still the writer, not fenced
	if !kept() {
		t.Fatal("a pass that may still publish dropped its files' write times")
	}
	r.exp.setPassGate(func() bool { return false }) // the writer role moved
	r.exp.forgetExportLagIfNotPublisher("mydb", "cpu")
	r.exp.setPassGate(nil)
	if kept() {
		t.Error("write times kept on a node that is no longer the writer")
	}
	seed()
	r.exp.hintFailMu.Lock()
	r.exp.fenced = errFenced // another node published this version first
	r.exp.hintFailMu.Unlock()
	r.exp.forgetExportLagIfNotPublisher("mydb", "cpu")
	if kept() {
		t.Error("write times kept after the pass was fenced")
	}
}

// A request names its measurement directly, so it is checked against what a
// pass would list: Arc's reserved directories and the exporter's own
// warehouse never get a table.
func TestScheduler_IngestRequestForANameAPassSkipsIsDropped(t *testing.T) {
	ctx := context.Background()
	r, src := newIngestRig(t, time.Minute)
	for _, db := range []string{"_schema", "_compaction_state", "arc_mydb.db"} {
		r.sched.RequestIngestReconcile(db, "cpu")
	}
	r.sched.serviceIngest(ctx, ingestBatch)
	if got := src.order(); got != "" {
		t.Errorf("listed %q for names a pass never exports", got)
	}
	if _, queued := r.sched.nextIngestDue(); queued {
		t.Error("a request for a name a pass never exports is still queued")
	}
	if got := snapshotFloat(t, "iceberg_export_pending_seconds"); got != 0 {
		t.Errorf("pending = %v s for names that will never be exported, want 0", got)
	}
	if !r.src.Exportable(Measurement{Database: "mydb", Measurement: "cpu"}) || r.src.Exportable(Measurement{Database: "mydb"}) {
		t.Error("Exportable: an ordinary measurement must pass, an empty name must not")
	}
}
