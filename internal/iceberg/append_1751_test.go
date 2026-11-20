package iceberg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/rs/zerolog"

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
		r.sched.RequestIngestAppend("mydb", "cpu", appendDir+n, 0)
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
		refs, _, err := r.src.AppendRefs(context.Background(), Measurement{Database: "mydb", Measurement: "cpu"}, []string{appendDir + wide}, nil)
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

// A file written longer ago than appendMaxFileAge is the pass's, not an
// append's: recentAdds is only kept for so long, and past it nothing says
// whether a pass already added the file.
func TestAppend_AnOldFileTakesAPass(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	r.sched.started = time.Now().Add(-2 * time.Hour)
	old := ingestName("cpu", time.Now().Add(-appendMaxFileAge-time.Minute))
	r.writeIn(t, "cpu", old)
	r.report(old)
	r.serve()
	r.wantOnce(first, old)
	if got := r.listings(); got != 2 {
		t.Errorf("%d listings, want 2: a file written %v ago is not appended", got, appendMaxFileAge+time.Minute)
	}
}

// An append that fails can still have committed (here: committed, and the
// publish failed). The pass that follows may fail too, at its listing, which
// keeps the cached listing. That cache must not match a later listing, or the
// committed file, once deleted, would stay in the table for good.
func TestAppend_AFailedAppendDoesNotLeaveTheOldFingerprint(t *testing.T) {
	if os.Geteuid() == 0 || runtime.GOOS == "windows" {
		t.Skip("needs a hint write that fails")
	}
	r := newAppendRig(t)
	first := r.settled()
	hint := filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata", "version-hint.text")
	if err := os.Remove(hint); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(hint, 0o755); err != nil { // a directory where the hint goes: the publish fails
		t.Fatal(err)
	}
	r.src.mu.Lock()
	r.src.fail["cpu"] = true // and the pass that follows fails at its listing
	r.src.mu.Unlock()
	a := r.newFile()
	r.report(a)
	r.serve()
	if got := r.entries(); got[a] != 1 {
		t.Fatalf("table %v: the append was meant to commit before its publish failed", got)
	}
	if st, ok := r.sched.state["mydb\x00cpu"]; ok && st.fingerprint != appendedFingerprint {
		t.Fatalf("fingerprint %q kept after an append that committed and failed: a listing without the file would match it", st.fingerprint)
	}
	if r.sched.appendOK["mydb\x00cpu"] {
		t.Error("the table still takes appends after one failed")
	}
	// The file leaves storage; with the listing working again, a pass takes
	// it out of the table.
	if err := os.Remove(hint); err != nil {
		t.Fatal(err)
	}
	r.src.mu.Lock()
	r.src.fail["cpu"] = false
	r.src.mu.Unlock()
	if err := os.Remove(filepath.Join(r.root, filepath.FromSlash(appendDir+a))); err != nil {
		t.Fatal(err)
	}
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	r.wantOnce(first)
}

// More files reported than are remembered: the ones left out are known only
// to a listing. A pass that started before they were reported did not list
// them, so the table still takes a pass afterwards, not an append of the
// files that were remembered.
func TestAppend_AnOverflowAfterAPassStartedStillTakesAPass(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	key := "mydb\x00cpu"
	before := time.Now()
	time.Sleep(2 * time.Millisecond)
	a, b := r.newFile(), r.newFile()
	r.report(a)
	r.sched.reqMu.Lock()
	p := r.sched.appendPending[key]
	p.overflow, p.overflowAt = true, time.Now() // b was reported and not remembered
	r.sched.appendPending[key] = p
	r.sched.reqMu.Unlock()

	r.sched.dropAppendPendingBefore(key, before) // a pass that started before both
	r.sched.reqMu.Lock()
	p = r.sched.appendPending[key]
	r.sched.reqMu.Unlock()
	if !p.overflow || len(p.files) != 1 {
		t.Fatalf("after a pass that started before the reports: overflow %v, %d files; want the overflow and the file kept", p.overflow, len(p.files))
	}
	r.serve()
	r.wantOnce(first, a, b)
	if got := r.listings(); got != 2 {
		t.Errorf("%d listings, want 2: only a pass finds the file that was not remembered", got)
	}

	// An overflow from before the pass started is covered by it.
	r.sched.reqMu.Lock()
	r.sched.appendPending[key] = pendingFiles{overflow: true, overflowAt: time.Now()}
	r.sched.reqMu.Unlock()
	time.Sleep(2 * time.Millisecond)
	r.sched.dropAppendPendingBefore(key, time.Now())
	r.sched.reqMu.Lock()
	_, left := r.sched.appendPending[key]
	r.sched.reqMu.Unlock()
	if left {
		t.Error("an overflow from before the pass is still remembered after it")
	}
}

// Appends expire, prune and sweep nothing, so after appendMaxBetweenPasses of
// them a table takes a pass, however far away the timed one is.
func TestAppend_ATableTakesAPassAfterManyAppends(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	a := r.newFile()
	r.report(a)
	r.serve()
	if got := r.sched.appendsSince["mydb\x00cpu"]; got != 1 {
		t.Fatalf("%d appends counted since the last pass, want 1", got)
	}
	r.sched.appendsSince["mydb\x00cpu"] = appendMaxBetweenPasses
	b := r.newFile()
	r.report(b)
	r.serve()
	r.wantOnce(first, a, b)
	if got := r.listings(); got != 2 {
		t.Errorf("%d listings, want 2: the table was due a pass", got)
	}
	if got := r.sched.appendsSince["mydb\x00cpu"]; got != 0 {
		t.Errorf("%d appends counted after the pass, want 0", got)
	}
	c := r.newFile()
	r.report(c)
	r.serve()
	if got := r.listings(); got != 2 {
		t.Errorf("%d listings, want still 2: after the pass the table takes appends again", got)
	}
}

// A measurement that is gone from the store is forgotten by the timed pass,
// its append state included.
func TestAppend_ATimedPassForgetsMeasurementsThatAreGone(t *testing.T) {
	r := newAppendRig(t)
	r.settled()
	r.report(r.newFile())
	r.serve()
	if err := os.RemoveAll(filepath.Join(r.root, "mydb")); err != nil {
		t.Fatal(err)
	}
	r.sched.runPass(context.Background())
	key := "mydb\x00cpu"
	if r.sched.appendOK[key] || len(r.sched.recentAdds[key]) != 0 || r.sched.appendsSince[key] != 0 {
		t.Errorf("append state survives the measurement: ok %v, %d recent, %d appends", r.sched.appendOK[key], len(r.sched.recentAdds[key]), r.sched.appendsSince[key])
	}
}

// A process that restarts finds the snapshots its appends left: the flag that
// asks a pass to expire them is not kept across a restart, so a table's first
// pass in a process expires, even when it commits nothing.
func TestAppend_TheFirstPassOfAProcessExpiresWhatTheLastOneLeft(t *testing.T) {
	r := newAppendRig(t)
	r.exp.retain = 2
	r.settled()
	for i := 0; i < 4; i++ {
		r.report(r.newFile())
		r.serve()
	}
	// "Restart": the exporter forgets what it owes and which tables it saw,
	// and the scheduler its cache.
	r.exp.chunkMu.Lock()
	r.exp.expireOwed, r.exp.expirySeen = nil, nil
	r.exp.chunkMu.Unlock()
	delete(r.sched.state, "mydb\x00cpu")
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	tbl, err := r.exp.catalog.LoadTable(context.Background(), r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(tbl.Metadata().Snapshots()); got != 2 {
		t.Errorf("%d snapshots after the first pass of a new process, want retain_snapshots = 2", got)
	}
}

// The permissions walk runs on every publish, and with ingest publishing
// every few seconds the directory holds thousands of files: only entries it
// has not tightened before are looked at. A new file is still tightened, and
// a name that left the directory is looked at again if it comes back.
func TestHardenLocalMetadataPerms_LooksAtEachEntryOnce(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits")
	}
	base := t.TempDir()
	metaDir := filepath.Join(base, "wh", "arc_db.db", "cpu", "metadata")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name string) string {
		p := filepath.Join(metaDir, name)
		if err := os.WriteFile(p, []byte("m"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, 0o644); err != nil { // whatever the umask is
			t.Fatal(err)
		}
		return p
	}
	perm := func(p string) os.FileMode {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return info.Mode().Perm()
	}
	e := &Exporter{logger: zerolog.Nop(), warehouse: localFileURI(filepath.Join(base, "wh"))}
	first := write("00001-a.metadata.json")
	loc := "file://" + filepath.ToSlash(first)
	e.hardenLocalMetadataPerms(loc)
	if got := perm(first); got != 0o600 {
		t.Fatalf("first file %v, want 0600", got)
	}
	// A file added later is tightened; the one already done is not looked at
	// again (loosened here by hand to show it).
	second := write("00002-b.metadata.json")
	if err := os.Chmod(first, 0o644); err != nil {
		t.Fatal(err)
	}
	e.hardenLocalMetadataPerms(loc)
	if got := perm(second); got != 0o600 {
		t.Errorf("a file added after the first walk is %v, want 0600", got)
	}
	if got := perm(first); got != 0o644 {
		t.Errorf("the file already tightened was looked at again (%v)", got)
	}
	// A name that left the directory and came back is a new file.
	if err := os.Remove(second); err != nil {
		t.Fatal(err)
	}
	e.hardenLocalMetadataPerms(loc)
	write("00002-b.metadata.json")
	e.hardenLocalMetadataPerms(loc)
	if got := perm(second); got != 0o600 {
		t.Errorf("a name that came back is %v, want 0600", got)
	}
}

// The pass that follows appends always runs, and usually finds nothing to
// commit. It must not restart the table's export spacing: the table's next
// flush would then wait up to a whole interval for a pass that published
// nothing. A pass that does commit still restarts it.
func TestAppend_APassThatCommitsNothingDoesNotDelayTheNextExport(t *testing.T) {
	r := newAppendRig(t)
	r.settled()
	r.report(r.newFile())
	r.serve()
	const interval = time.Minute
	r.sched.ingestInterval = interval
	key := "mydb\x00cpu"
	due := func() time.Duration {
		r.sched.reqMu.Lock()
		defer r.sched.reqMu.Unlock()
		return time.Until(r.sched.ingestNext[key])
	}
	if _, err := r.pass(); err != nil { // after an append: runs, commits nothing
		t.Fatal(err)
	}
	if got := due(); got > time.Second {
		t.Fatalf("the next ingest export is held back %v by a pass that committed nothing", got)
	}
	r.newFile() // not reported: the pass finds and commits it
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := due(); got < interval/2 {
		t.Errorf("after a pass that committed, the next ingest export is due in %v, want about %v", got, interval)
	}
}

// A table that goes quiet still has its old published versions pruned: a
// pass that leaves versions on disk only because of iceberg.min_version_age
// makes the table's next pass run whatever its listing is. Without that an
// unchanged table's pass is skipped, and the versions would stay for good.
func TestAppend_AQuietTableStillHasItsOldVersionsPruned(t *testing.T) {
	r := newAppendRig(t)
	r.exp.retain = 1
	r.exp.SetVersionRetention(time.Hour, 0)
	r.settled()
	for i := 0; i < 4; i++ {
		r.report(r.newFile())
		r.serve()
	}
	copies := func() int {
		entries, err := os.ReadDir(filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata"))
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "v") && strings.HasSuffix(e.Name(), ".metadata.json") {
				n++
			}
		}
		return n
	}
	key := "mydb\x00cpu"
	if _, err := r.pass(); err != nil { // the pass after the appends: everything is too young to prune
		t.Fatal(err)
	}
	held := copies()
	if held <= 2 {
		t.Fatalf("%d copies inside the minimum age, want more than retain_snapshots+1 = 2 kept", held)
	}
	if st := r.sched.state[key]; st.fingerprint != appendedFingerprint {
		t.Fatalf("fingerprint %q after a pass that left versions held by age: the next pass of an unchanged table would be skipped", st.fingerprint)
	}
	// Nothing changes in storage; the versions are old enough now.
	r.exp.SetVersionRetention(time.Millisecond, 0)
	time.Sleep(20 * time.Millisecond)
	listed := r.listings()
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := r.listings(); got != listed+1 {
		t.Fatalf("the pass of the unchanged table was skipped (%d listings, then %d)", listed, got)
	}
	if got := copies(); got >= held || got > 3 {
		t.Errorf("%d copies after the minimum age passed (was %d), want them pruned to about retain_snapshots+1", got, held)
	}
	// Nothing is held any more: the table's passes are skipped again.
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if st := r.sched.state[key]; st.fingerprint == appendedFingerprint {
		t.Error("the table is still marked for a pass with nothing left to prune")
	}
	listed = r.listings()
	if outcome, _ := r.pass(); outcome != outcomeUnchanged {
		t.Errorf("a pass of the settled, unchanged table: outcome %v after %d listings, want unchanged", outcome, listed)
	}
}
