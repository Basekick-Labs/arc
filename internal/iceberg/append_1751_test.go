package iceberg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"

	"github.com/basekick-labs/arc/internal/metrics"
)

// appendRig drives the scheduler's ingest service by hand, with the append
// path on. Requests are spaced a millisecond apart, and serve waits that out.
type appendRig struct {
	*exportRig
	src *recordingSource
	t   *testing.T
	seq int
}

func newAppendRig(t *testing.T) *appendRig {
	t.Helper()
	r := newExportRig(t, time.Hour)
	src := &recordingSource{StorageWalkSource: r.src, fail: map[string]bool{}}
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: src, Interval: time.Hour, Logger: r.sched.logger,
		IngestInterval: time.Millisecond, IngestAppend: true})
	// The rig's files are named just after "now"; a scheduler built in the
	// same instant must still see them as written by this process.
	r.sched.started = time.Now().Add(-time.Second)
	t.Cleanup(func() { metrics.Get().SetIcebergOldestPendingExport(time.Time{}) })
	return &appendRig{exportRig: r, src: src, t: t}
}

const appendDir = "mydb/cpu/2026/07/14/15/"

// newFile writes an ingest file of the measurement and returns its name.
func (r *appendRig) newFile() string {
	r.t.Helper()
	r.seq++
	name := ingestName("cpu", time.Now().Add(time.Duration(r.seq)*time.Microsecond))
	r.writeIn(r.t, "cpu", name)
	return name
}

func (r *appendRig) report(names ...string) {
	for _, n := range names {
		r.sched.RequestIngestAppend("mydb", "cpu", appendDir+n)
	}
}

// serve runs the ingest service once the spacing has passed.
func (r *appendRig) serve() {
	time.Sleep(5 * time.Millisecond)
	r.sched.serviceIngest(context.Background(), ingestBatch)
}

// pass runs a reconcile pass of the measurement, as the timer does.
func (r *appendRig) pass() (reconcileOutcome, error) {
	return r.sched.reconcileOneRecovered(context.Background(), Measurement{Database: "mydb", Measurement: "cpu"}, "mydb\x00cpu")
}

func (r *appendRig) listings() int {
	if o := r.src.order(); o != "" {
		return strings.Count(o, ",") + 1
	}
	return 0
}

// entries is every live entry of the table by file name, WITH repeats: a
// file added twice is two entries, which a set of paths would hide.
func (r *appendRig) entries() map[string]int {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		r.t.Fatal(err)
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	out := map[string]int{}
	for _, task := range tasks {
		out[filepath.Base(task.File.FilePath())]++
	}
	return out
}

func (r *appendRig) wantOnce(names ...string) {
	r.t.Helper()
	got := r.entries()
	if len(got) != len(names) {
		r.t.Fatalf("table has %d files %v, want %d", len(got), got, len(names))
	}
	for _, n := range names {
		if got[n] != 1 {
			r.t.Fatalf("file %s is in the table %d times, want once (table: %v)", n, got[n], got)
		}
	}
}

// settled gets the table to the state appends need: one file, exported by a
// pass that settled.
func (r *appendRig) settled() string {
	r.t.Helper()
	first := r.newFile()
	r.report(first)
	r.serve()
	r.wantOnce(first)
	if !r.sched.appendOK["mydb\x00cpu"] {
		r.t.Fatal("the table does not take appends after a pass that settled")
	}
	return first
}

func (r *appendRig) manifests() []iceberg.ManifestFile {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		r.t.Fatal(err)
	}
	fio, err := tbl.FS(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	ms, err := tbl.CurrentSnapshot().Manifests(fio)
	if err != nil {
		r.t.Fatal(err)
	}
	return ms
}

// The point of #1751: a reported file is added with no listing of the
// measurement, the parent snapshot's manifests carried as they are, and the
// pass that follows finds nothing to add.
func TestAppend_AddsTheReportedFileWithoutListing(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	if got := r.listings(); got != 1 {
		t.Fatalf("%d listings for the first export, want 1 (a pass)", got)
	}
	before := r.manifests()
	appends, files := snapshotInt(t, "iceberg_ingest_appends"), snapshotInt(t, "iceberg_ingest_append_files")
	lag := snapshotInt(t, "iceberg_export_lag_count")

	second := r.newFile()
	r.report(second)
	r.serve()

	r.wantOnce(first, second)
	if got := r.listings(); got != 1 {
		t.Errorf("%d listings after an append, want still 1: the append listed the measurement", got)
	}
	if got := snapshotInt(t, "iceberg_ingest_appends") - appends; got != 1 {
		t.Errorf("%d appends counted, want 1", got)
	}
	if got := snapshotInt(t, "iceberg_ingest_append_files") - files; got != 1 {
		t.Errorf("%d appended files counted, want 1", got)
	}
	if got := snapshotInt(t, "iceberg_export_lag_count") - lag; got != 1 {
		t.Errorf("%d export-lag observations for the appended file, want 1", got)
	}
	if got := snapshotFloat(t, "iceberg_export_pending_seconds"); got != 0 {
		t.Errorf("pending = %v s after the append covered the report, want 0", got)
	}
	after := map[string]bool{}
	for _, m := range r.manifests() {
		after[m.FilePath()] = true
	}
	for _, m := range before {
		if !after[m.FilePath()] {
			t.Errorf("manifest %s of the parent snapshot was rewritten by an append", filepath.Base(m.FilePath()))
		}
	}
	if len(after) != len(before)+1 {
		t.Errorf("%d manifests after one append over %d, want one more", len(after), len(before))
	}

	// The next pass runs (the cached listing is not the table any more) and
	// adds nothing a second time.
	if st := r.sched.state["mydb\x00cpu"]; st.fingerprint != appendedFingerprint {
		t.Fatalf("fingerprint after an append = %q, want the one no listing produces", st.fingerprint)
	}
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := r.listings(); got != 2 {
		t.Errorf("%d listings after the pass, want 2", got)
	}
	r.wantOnce(first, second)
	if st := r.sched.state["mydb\x00cpu"]; st.fingerprint == appendedFingerprint || st.fingerprint == "" {
		t.Errorf("fingerprint after the pass = %q, want the listing's", st.fingerprint)
	}
}

// A pass can list a file between its write and its registration (an hour
// file that waits for the rest of its flush). The report that then arrives
// must not add it a second time: the duplicate check is off.
func TestAppend_AFileAPassAlreadyAddedIsNotAddedAgain(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	late := r.newFile() // stored, not registered yet
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	r.wantOnce(first, late)
	listed := r.listings()

	r.report(late) // the flush registers it now
	r.serve()
	r.wantOnce(first, late)
	if got := r.listings(); got != listed {
		t.Errorf("%d listings, want %d: a report of a file already in the table needs no pass", got, listed)
	}
}

// The same file reported twice, in one batch and across two, is one entry.
func TestAppend_AFileReportedTwiceIsAddedOnce(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	a := r.newFile()
	r.report(a, a)
	r.serve()
	r.wantOnce(first, a)
	r.report(a)
	r.serve()
	r.wantOnce(first, a)
}

// A pass drops the files reported before it started (it listed after they
// were stored) and keeps the ones reported after, which are appended.
func TestAppend_APassCoversWhatWasReportedBeforeIt(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	a := r.newFile()
	r.report(a)
	if _, err := r.pass(); err != nil { // lists a, adds it
		t.Fatal(err)
	}
	r.sched.reqMu.Lock()
	left := len(r.sched.appendPending["mydb\x00cpu"].files)
	r.sched.reqMu.Unlock()
	if left != 0 {
		t.Fatalf("%d reported files still remembered after a pass that started after them, want 0", left)
	}
	b := r.newFile()
	r.report(b)
	listed := r.listings()
	r.serve()
	r.wantOnce(first, a, b)
	if got := r.listings(); got != listed {
		t.Errorf("%d listings, want %d: the file reported after the pass is an append", got, listed)
	}
}

// An appended file that then leaves storage is taken out of the table by the
// next pass. The listing is back to the one the cache describes, so only the
// replaced fingerprint makes that pass run.
func TestAppend_AnAppendedFileThatIsDeletedLeavesTheTable(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	a := r.newFile()
	r.report(a)
	r.serve()
	r.wantOnce(first, a)
	if err := os.Remove(filepath.Join(r.root, filepath.FromSlash(appendDir+a))); err != nil {
		t.Fatal(err)
	}
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	r.wantOnce(first)
}

// What an append cannot do runs the pass instead, and the table ends right.
func TestAppend_FallsBackToAPass(t *testing.T) {
	t.Run("a new column", func(t *testing.T) {
		r := newAppendRig(t)
		first := r.settled()
		fallbacks := snapshotInt(t, "iceberg_ingest_append_fallbacks")
		wide := ingestName("cpu", time.Now().Add(time.Millisecond))
		writeArcStyleParquet4Col(t, filepath.Join(r.root, filepath.FromSlash(appendDir+wide)), 1_700_000_000_000_000, 1)
		// The append itself says why, before anything is read from the file
		// to build its entry.
		refs, _, err := r.src.AppendRefs(context.Background(), Measurement{Database: "mydb", Measurement: "cpu"}, []string{appendDir + wide})
		if err != nil || len(refs) != 1 {
			t.Fatalf("AppendRefs: %v, %d refs", err, len(refs))
		}
		if _, err := r.exp.appendFiles(context.Background(), "mydb", "cpu", refs, r.sched.unionFileSchemas, nil); !errors.Is(err, errNeedsFullPass) {
			t.Fatalf("appendFiles for a file with a new column: %v, want errNeedsFullPass", err)
		}
		r.wantOnce(first)
		r.report(wide)
		r.serve()
		r.wantOnce(first, wide)
		if got := r.listings(); got != 2 {
			t.Errorf("%d listings, want 2: a file with a new column needs a pass", got)
		}
		if got := snapshotInt(t, "iceberg_ingest_append_fallbacks") - fallbacks; got != 1 {
			t.Errorf("%d fallbacks counted, want 1", got)
		}
		if got := r.sched.appendFallback["mydb\x00cpu"]; got != "" {
			t.Errorf("fallback reason %q still remembered after the pass settled, want it cleared", got)
		}
		// The pass settled, so the table takes appends again.
		next := r.newFile()
		r.report(next)
		r.serve()
		r.wantOnce(first, wide, next)
		if got := r.listings(); got != 2 {
			t.Errorf("%d listings, want still 2", got)
		}
	})
	t.Run("a file written before this process started", func(t *testing.T) {
		r := newAppendRig(t)
		first := r.settled()
		old := r.newFile()
		r.sched.started = time.Now().Add(time.Hour)
		r.report(old)
		r.serve()
		r.wantOnce(first, old)
		if got := r.listings(); got != 2 {
			t.Errorf("%d listings, want 2", got)
		}
	})
	t.Run("a file that is not an ingest file", func(t *testing.T) {
		r := newAppendRig(t)
		first := r.settled()
		// A DELETE's rewrite of a fresh ingest file: named after it, with its
		// time, and not a file ingest wrote.
		rewrite := strings.TrimSuffix(ingestName("cpu", time.Now().Add(time.Millisecond)), ".parquet") + "_rewrite_1700000000000000000.parquet"
		r.writeIn(t, "cpu", rewrite)
		r.report(rewrite)
		r.serve()
		r.wantOnce(first, rewrite)
		if got := r.listings(); got != 2 {
			t.Errorf("%d listings, want 2", got)
		}
	})
	t.Run("more reported than remembered", func(t *testing.T) {
		r := newAppendRig(t)
		first := r.settled()
		a := r.newFile()
		r.report(a)
		r.sched.reqMu.Lock()
		p := r.sched.appendPending["mydb\x00cpu"]
		p.overflow = true
		r.sched.appendPending["mydb\x00cpu"] = p
		r.sched.reqMu.Unlock()
		r.serve()
		r.wantOnce(first, a)
		if got := r.listings(); got != 2 {
			t.Errorf("%d listings, want 2", got)
		}
	})
	t.Run("a table whose last pass failed", func(t *testing.T) {
		r := newAppendRig(t)
		first := r.settled()
		r.src.mu.Lock()
		r.src.fail["cpu"] = true
		r.src.mu.Unlock()
		if _, err := r.pass(); err == nil {
			t.Fatal("the pass did not fail")
		}
		r.src.mu.Lock()
		r.src.fail["cpu"] = false
		r.src.mu.Unlock()
		a := r.newFile()
		r.report(a)
		listed := r.listings()
		r.serve()
		r.wantOnce(first, a)
		if got := r.listings(); got != listed+1 {
			t.Errorf("%d listings, want %d: after a failed pass the table takes a pass, not an append", got, listed+1)
		}
	})
}

// A reported file that is gone, or that Arc's reads leave out, is not added
// and needs no pass: whatever replaced it reports for itself.
func TestAppend_LeavesOutFilesThatAreGoneOrReplaced(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	gone := ingestName("cpu", time.Now().Add(time.Millisecond)) // never written
	replaced := r.newFile()
	kept := r.newFile()
	r.src.StorageWalkSource.SetExclusions(&replacingExclusions{keys: []string{appendDir + replaced}})
	r.report(gone, replaced, kept)
	r.serve()
	r.wantOnce(first, kept)
	if got := r.listings(); got != 1 {
		t.Errorf("%d listings, want 1", got)
	}
}

// Appends expire no snapshot; the pass after them does, though its own diff
// is empty.
func TestAppend_ThePassAfterAppendsExpiresTheirSnapshots(t *testing.T) {
	r := newAppendRig(t)
	r.exp.retain = 2
	r.settled()
	for i := 0; i < 5; i++ {
		r.report(r.newFile())
		r.serve()
	}
	snapshots := func() int {
		tbl, err := r.exp.catalog.LoadTable(context.Background(), r.exp.tableIdent("mydb", "cpu"))
		if err != nil {
			t.Fatal(err)
		}
		return len(tbl.Metadata().Snapshots())
	}
	if got := snapshots(); got < 6 {
		t.Fatalf("%d snapshots after 5 appends, want them all still there (an append expires nothing)", got)
	}
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := snapshots(); got != 2 {
		t.Errorf("%d snapshots after the pass, want retain_snapshots = 2", got)
	}
}

// Every append adds a manifest; they are merged before they pile up, and no
// file is lost or repeated by a merge.
func TestAppend_MergesTheSmallManifests(t *testing.T) {
	r := newAppendRig(t)
	names := []string{r.settled()}
	most := 0
	for i := 0; i < 3*appendMergeMinCount; i++ {
		n := r.newFile()
		names = append(names, n)
		r.report(n)
		r.serve()
		if got := len(r.manifests()); got > most {
			most = got
		}
	}
	if most > appendMergeMinCount {
		t.Errorf("up to %d manifests during %d appends, want at most %d", most, 3*appendMergeMinCount, appendMergeMinCount)
	}
	r.wantOnce(names...)
	if got := r.listings(); got != 1 {
		t.Errorf("%d listings, want 1", got)
	}
}

// A database drop forgets the append state: nothing may be appended to a
// table that is gone.
func TestAppend_ADroppedDatabaseForgetsItsFiles(t *testing.T) {
	r := newAppendRig(t)
	r.settled()
	r.report(r.newFile())
	r.sched.forgetDatabase("mydb")
	if r.sched.appendOK["mydb\x00cpu"] || len(r.sched.recentAdds["mydb\x00cpu"]) != 0 {
		t.Error("append state survives a database drop")
	}
	r.sched.reqMu.Lock()
	_, pending := r.sched.appendPending["mydb\x00cpu"]
	r.sched.reqMu.Unlock()
	if pending {
		t.Error("reported files survive a database drop")
	}
}

// With the append path off, a report is the request it was before #1751.
func TestAppend_OffEveryIngestExportIsAPass(t *testing.T) {
	r := newAppendRig(t)
	r.sched.appendEnabled = false
	a := r.newFile()
	r.report(a)
	r.serve()
	b := r.newFile()
	r.report(b)
	r.serve()
	r.wantOnce(a, b)
	if got := r.listings(); got != 2 {
		t.Errorf("%d listings with the append path off, want 2", got)
	}
}

// A published version is kept until min_version_age after the NEXT one was
// published, on top of the newest retain_snapshots+1. Its own age does not
// count: a version that was current for an hour is still the one readers were
// handed a moment before it was superseded.
func TestPrune_KeepsAVersionUntilItsSuccessorIsOldEnough(t *testing.T) {
	r := newHealRig(t, 1)
	r.exp.SetVersionRetention(time.Hour, 0)
	files, sc := r.history(5)
	all := r.vCopies()
	if len(all) < 5 {
		t.Fatalf("%v copies after 5 passes with min_version_age an hour, want every version kept", all)
	}
	metaDir := filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata")
	vPath := func(n int) string { return filepath.Join(metaDir, "v"+strconv.Itoa(n)+".metadata.json") }
	old := time.Now().Add(-2 * time.Hour)

	// The oldest copy is itself two hours old; its successor is new. Kept.
	if err := os.Chtimes(vPath(all[0]), old, old); err != nil {
		t.Fatal(err)
	}
	r.reconcileConverged(files, sc)
	if got := r.vCopies(); got[0] != all[0] {
		t.Fatalf("copies %v: v%d was deleted though the version after it was published a moment ago", got, all[0])
	}
	// Its successor published two hours ago: now it goes, and only it.
	if err := os.Chtimes(vPath(all[1]), old, old); err != nil {
		t.Fatal(err)
	}
	r.reconcileConverged(files, sc)
	got := r.vCopies()
	if got[0] != all[1] || len(got) != len(all)-1 {
		t.Fatalf("copies %v, want %v without v%d", got, all, all[0])
	}
	// With no minimum age, the count alone decides, as before.
	r.exp.SetVersionRetention(0, 0)
	r.reconcileConverged(files, sc)
	if got := r.vCopies(); len(got) != 2 {
		t.Fatalf("copies %v with min_version_age 0 at retain 1, want 2", got)
	}
}

func (r *rig1686) reconcileConverged(files []FileRef, sc ArcSchema) {
	r.t.Helper()
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files); err != nil {
		r.t.Fatal(err)
	}
}

// iceberg-go keeps its own metadata files by count; the count covers
// min_version_age at the rate ingest commits, and never less than retain.
func TestPreviousVersionsMaxCoversTheMinimumAge(t *testing.T) {
	for _, c := range []struct {
		retain        int
		age, interval time.Duration
		want          int
	}{
		{10, 300 * time.Second, 60 * time.Second, 10},
		{10, 300 * time.Second, 5 * time.Second, 60},
		{1, 120 * time.Second, 7 * time.Second, 18}, // rounded up
		{10, 300 * time.Second, 0, 10},              // ingest triggers nothing
		{10, 0, 5 * time.Second, 10},
	} {
		e := &Exporter{retain: c.retain}
		e.SetVersionRetention(c.age, c.interval)
		if got := e.previousVersionsMax(); got != c.want {
			t.Errorf("retain %d, age %v, interval %v: %d, want %d", c.retain, c.age, c.interval, got, c.want)
		}
	}
}
