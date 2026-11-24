package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1825: a re-home job replaces a misfiled file with files in the directories
// its rows belong to. The table must never be published with the old file's
// removal and without those files.

// swapRig1825 has a table holding keep (day 0) and old: a file in day 0's
// directory whose rows are all of day 1, registered under day 1. The job's
// new file for old's rows is in day 1's directory, and three unrelated new
// files are in day 3.
type swapRig1825 struct {
	*rig1686
	sc             ArcSchema
	keep, old, out FileRef
	extras         []FileRef
}

func newSwapRig1825(t *testing.T) *swapRig1825 {
	t.Helper()
	r := &swapRig1825{rig1686: newRig1686(t, 3, 1)}
	r.keep = r.write(0, "keep")
	// old: day 0's directory, day 1's rows.
	tm := time.UnixMicro(day14).UTC()
	rel := fmt.Sprintf("mydb/cpu/%04d/%02d/%02d/22/old.parquet", tm.Year(), tm.Month(), tm.Day())
	p := filepath.Join(r.root, rel)
	writeArcStyleParquet(t, p, day14+dayMicros, 5)
	r.old = refOf(t, p)
	r.old.Key, r.old.LocalPath = rel, p
	r.sc = r.schema(r.keep)
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", r.sc, []FileRef{r.keep, r.old}); err != nil {
		t.Fatal(err)
	}
	if len(r.live()) != 2 {
		t.Fatalf("the table holds %v, want keep and old", keys(r.live()))
	}
	r.out = r.write(1, "out")
	for i := 0; i < 3; i++ {
		r.extras = append(r.extras, r.write(3, fmt.Sprintf("extra%d", i)))
	}
	return r
}

// pass reconciles the listing a pass takes once the job's reads use the new
// file: old is left out.
func (r *swapRig1825) pass(canCommit func() bool, current ...FileRef) (reconcileResult, error) {
	unread := []FileRef{}
	for _, f := range current {
		if f.Key != r.keep.Key {
			unread = append(unread, f)
		}
	}
	return r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: r.sc, current: current,
		unread: unread, readSchemas: readLocal, exists: existsLocal, canCommit: canCommit,
	})
}

func (r *swapRig1825) holds(t *testing.T, want ...FileRef) {
	t.Helper()
	live := r.live()
	ok := len(live) == len(want)
	for _, f := range want {
		if _, in := live[f.PhysicalPath]; !in {
			ok = false
		}
	}
	if !ok {
		names := make([]string, len(want))
		for i, f := range want {
			names[i] = filepath.Base(f.Key)
		}
		t.Fatalf("the table holds %v; want %v", keys(live), names)
	}
}

func TestRehomeSwap1825_TheOldFileLeavesWithTheNewOnesInOneCommit(t *testing.T) {
	r := newSwapRig1825(t)
	r.exp.SetReplaced(func(database, measurement string) map[string][]string {
		if database != "mydb" || measurement != "cpu" {
			t.Fatalf("asked about %s.%s", database, measurement)
		}
		return map[string][]string{r.old.Key: {r.out.Key}}
	})
	// The pass may commit once: whatever that commit holds is what a reader
	// has if the pass stops there.
	commits := 0
	res, err := r.pass(func() bool { commits++; return commits == 1 }, append([]FileRef{r.keep, r.out}, r.extras...)...)
	if err != nil {
		t.Fatal(err)
	}
	if res.commits != 1 {
		t.Fatalf("the pass made %d commits, want 1", res.commits)
	}
	// The job's new file, and not the unrelated ones: the first commit is
	// not chunked, and must not carry a backlog.
	r.holds(t, r.keep, r.out)

	// The next pass registers the rest a chunk at a time.
	res, err = r.pass(nil, append([]FileRef{r.keep, r.out}, r.extras...)...)
	if err != nil || res.commits != len(r.extras) {
		t.Fatalf("the next pass: %+v, err %v; want one commit per unrelated file (chunks of 1)", res, err)
	}
	r.holds(t, append([]FileRef{r.keep, r.out}, r.extras...)...)
}

// Without a job, the same listing is the case the rule exists for: the first
// commit removes old, and its rows come in a later one. The test pins that the
// rig is that case, so the test above is not passing for another reason.
func TestRehomeSwap1825_TheRigIsTheSplitCase(t *testing.T) {
	r := newSwapRig1825(t)
	commits := 0
	if _, err := r.pass(func() bool { commits++; return commits == 1 }, append([]FileRef{r.keep, r.out}, r.extras...)...); err != nil {
		t.Fatal(err)
	}
	r.holds(t, r.keep)
}

func TestRehomeSwap1825_AFileGoneDuringThePassDefersTheWholeSwap(t *testing.T) {
	r := newSwapRig1825(t)
	r.exp.SetReplaced(func(string, string) map[string][]string {
		return map[string][]string{r.old.Key: {r.out.Key}}
	})
	// A new file in the old file's directory day is gone by the time it is
	// read. Deferring that day alone would defer the old file's removal and
	// still commit the job's new file, in another day: its rows twice.
	gone := r.write(0, "gone")
	removeFile(t, gone)
	res, err := r.pass(nil, r.keep, r.out, gone)
	if err != nil {
		t.Fatalf("a vanished file failed the pass: %v", err)
	}
	if !res.moved || res.settled {
		t.Fatalf("result %+v, want moved and not settled", res)
	}
	r.holds(t, r.keep, r.old)
}

// The job's own new file gone: nothing of the swap commits.
func TestRehomeSwap1825_TheNewFileGoneDefersTheWholeSwap(t *testing.T) {
	r := newSwapRig1825(t)
	r.exp.SetReplaced(func(string, string) map[string][]string {
		return map[string][]string{r.old.Key: {r.out.Key}}
	})
	removeFile(t, r.out)
	if _, err := r.pass(nil, r.keep, r.out); err != nil {
		t.Fatalf("a vanished file failed the pass: %v", err)
	}
	r.holds(t, r.keep, r.old)
}

func TestRehomeSwap1825_ANewFileThatCannotBeRegisteredKeepsTheOldOne(t *testing.T) {
	r := newSwapRig1825(t)
	// The job's new file holds rows of two days: no partition can take it.
	removeFile(t, r.out)
	writeStraddlingParquet(t, r.out.LocalPath, day14+dayMicros)
	out := refOf(t, r.out.LocalPath)
	out.Key, out.LocalPath = r.out.Key, r.out.LocalPath
	r.exp.SetReplaced(func(string, string) map[string][]string {
		return map[string][]string{r.old.Key: {out.Key}}
	})
	_, err := r.pass(nil, r.keep, out)
	if err == nil || !strings.Contains(err.Error(), "cannot be registered") {
		t.Fatalf("err = %v, want the pass to fail on the new file", err)
	}
	r.holds(t, r.keep, r.old)
}

func TestPathIsKey1825(t *testing.T) {
	for _, tc := range []struct {
		p, key string
		want   bool
	}{
		{"file:///data/arc/db/cpu/2026/11/18/23/x.parquet", "db/cpu/2026/11/18/23/x.parquet", true},
		{"db/cpu/2026/11/18/23/x.parquet", "db/cpu/2026/11/18/23/x.parquet", true},
		// Another database whose name ends with this one's.
		{"file:///data/arc/otherdb/cpu/2026/11/18/23/x.parquet", "db/cpu/2026/11/18/23/x.parquet", false},
		{"file:///data/arc/db/cpu/2026/11/18/23/x.parquet", "cpu/2026/11/18/23/x.parquet", true},
		// A file URI encodes what a key does not.
		{"file:///data/arc/my%20db/cpu/2026/11/18/23/x.parquet", "my db/cpu/2026/11/18/23/x.parquet", true},
		{"file:///data/arc/caf%C3%A9/cpu/2026/11/18/23/x.parquet", "café/cpu/2026/11/18/23/x.parquet", true},
		{"file:///data/arc/my%20db/cpu/2026/11/18/23/x.parquet", "db/cpu/2026/11/18/23/x.parquet", false},
	} {
		if got := pathIsKey(tc.p, tc.key); got != tc.want {
			t.Errorf("pathIsKey(%q, %q) = %v", tc.p, tc.key, got)
		}
	}
}

// confirmRig1825 runs a scheduler over one measurement with two files.
func confirmRig1825(t *testing.T) (root string, exp *Exporter, sched *Scheduler) {
	t.Helper()
	root = t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		writeArcStyleParquet(t, filepath.Join(root, fmt.Sprintf("mydb/cpu/2023/11/14/22/f%d.parquet", i)), day14+int64(i), 5)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err = NewExporter(db, backend, "file://"+root, "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sched = NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Interval: time.Hour, Logger: zerolog.Nop()})
	sched.Start()
	t.Cleanup(sched.Stop)
	return root, exp, sched
}

func TestReconcileAndConfirm1825(t *testing.T) {
	root, exp, sched := confirmRig1825(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const f0 = "mydb/cpu/2023/11/14/22/f0.parquet"

	// The pass it runs creates the table and publishes it; the file is named.
	a, err := sched.ReconcileAndConfirm(ctx, "mydb", "cpu", f0)
	if err != nil || a.Blocked != "" || a.Err != nil || !a.Published || !a.Named || a.Clear() {
		t.Fatalf("with the file in storage: %+v, err %v", a, err)
	}
	// No file asked about: the answer is about the pass alone.
	if a, err = sched.ReconcileAndConfirm(ctx, "mydb", "cpu", ""); err != nil || !a.Clear() {
		t.Fatalf("with no file asked about: %+v, err %v", a, err)
	}
	// The file leaves storage: the pass this call runs removes it from the
	// table, and nothing else would have (the timer is an hour away).
	if err := os.Remove(filepath.Join(root, f0)); err != nil {
		t.Fatal(err)
	}
	if a, err = sched.ReconcileAndConfirm(ctx, "mydb", "cpu", f0); err != nil || !a.Clear() || a.Named {
		t.Fatalf("with the file gone: %+v, err %v", a, err)
	}
	if got := len(liveEntries(ctx, t, exp, "mydb", "cpu")); got != 1 {
		t.Fatalf("the table holds %d files, want 1", got)
	}
	// A name the export gives no table.
	if a, err = sched.ReconcileAndConfirm(ctx, "_rehome", "cpu", "x"); err != nil || !a.NotExported || !a.Clear() {
		t.Fatalf("a reserved directory: %+v, err %v", a, err)
	}
	// A measurement with no files and no table: nothing names anything.
	if a, err = sched.ReconcileAndConfirm(ctx, "mydb", "nosuch", "mydb/nosuch/2023/11/14/22/x.parquet"); err != nil || !a.Clear() {
		t.Fatalf("a measurement with no table: %+v, err %v", a, err)
	}
}

func TestReconcileAndConfirm1825_APausedExportSaysSo(t *testing.T) {
	_, exp, sched := confirmRig1825(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resume, err := exp.PauseExportForRestore(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer resume()
	a, err := sched.ReconcileAndConfirm(ctx, "mydb", "cpu", "mydb/cpu/2023/11/14/22/f0.parquet")
	// Blocked, or, when the pause landed while the question was being
	// served, a pass that failed on the pause: never clear, and with a reason.
	if err != nil || a.Clear() || (a.Blocked == "" && (a.Err == nil || !strings.Contains(a.Err.Error(), "paused"))) {
		t.Fatalf("with the export paused: %+v, err %v", a, err)
	}
}

func TestReconcileAndConfirm1825_AnAskerThatGivesUpLeavesNothingBehind(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// Not started: nothing serves the question.
	sched := NewScheduler(SchedulerConfig{Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Interval: time.Hour, Logger: zerolog.Nop()})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := sched.ReconcileAndConfirm(ctx, "mydb", "cpu", "x"); err == nil {
		t.Fatal("an unanswered question returned no error")
	}
	if n := len(sched.takeConfirms()); n != 0 {
		t.Fatalf("%d question(s) left queued", n)
	}
}

// The answer is about what a reader of the table resolves: a table whose
// discovery files name an older version than the catalog's is not published.
func TestPublishedNames1825_AHintBehindTheCatalogIsNotPublished(t *testing.T) {
	root, exp, sched := confirmRig1825(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const f0 = "mydb/cpu/2023/11/14/22/f0.parquet"
	if a, err := sched.ReconcileAndConfirm(ctx, "mydb", "cpu", f0); err != nil || !a.Published {
		t.Fatalf("%+v %v", a, err)
	}
	var hint string
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.Name() == "version-hint.text" {
			hint = p
		}
		return nil
	})
	before, err := os.ReadFile(hint)
	if err != nil {
		t.Fatalf("no version hint under %s: %v", root, err)
	}
	if err := os.Remove(filepath.Join(root, f0)); err != nil {
		t.Fatal(err)
	}
	if a, err := sched.ReconcileAndConfirm(ctx, "mydb", "cpu", f0); err != nil || !a.Clear() {
		t.Fatalf("%+v %v", a, err)
	}
	sched.Stop() // publishedNames runs where passes run; none does now
	if _, published, named, err := exp.publishedNames(ctx, "mydb", "cpu", f0); err != nil || !published || named {
		t.Fatalf("published %v, named %v, err %v", published, named, err)
	}
	// The hint as it was before the file's removal was committed: readers
	// resolve a version that still names the file.
	if err := os.WriteFile(hint, before, 0o600); err != nil {
		t.Fatal(err)
	}
	exists, published, _, err := exp.publishedNames(ctx, "mydb", "cpu", f0)
	if err != nil || !exists || published {
		t.Fatalf("with the hint behind the catalog: exists %v, published %v, err %v", exists, published, err)
	}
}

// At a small retain_snapshots, passes of a table are spaced apart: a question
// that comes too soon after the last pass is answered once the spacing has
// passed, not dropped.
func TestReconcileAndConfirm1825_SpacedPassesStillAnswer(t *testing.T) {
	old := requestMinSpacing
	requestMinSpacing = 300 * time.Millisecond
	t.Cleanup(func() { requestMinSpacing = old })
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/f0.parquet"), day14, 5)
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/f1.parquet"), day14+1, 5)
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 1, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Interval: time.Hour, Logger: zerolog.Nop()})
	if sched.requestSpacing() == 0 {
		t.Fatal("the rig has no spacing between passes")
	}
	sched.Start()
	t.Cleanup(sched.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const f0 = "mydb/cpu/2023/11/14/22/f0.parquet"
	if a, err := sched.ReconcileAndConfirm(ctx, "mydb", "cpu", f0); err != nil || !a.Named {
		t.Fatalf("%+v %v", a, err)
	}
	if err := os.Remove(filepath.Join(root, f0)); err != nil {
		t.Fatal(err)
	}
	// Straight after a pass that committed: inside the spacing.
	start := time.Now()
	a, err := sched.ReconcileAndConfirm(ctx, "mydb", "cpu", f0)
	if err != nil || !a.Clear() {
		t.Fatalf("a question inside the spacing: %+v, err %v", a, err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatalf("answered after %s: the spacing was not kept", time.Since(start))
	}
}

func TestPassAnswerVerdict1825(t *testing.T) {
	failed := errors.New("schema conflict")
	for _, tc := range []struct {
		name  string
		a     PassAnswer
		asked bool
		clear bool
		why   string
	}{
		{"published without the file", PassAnswer{Published: true}, true, true, ""},
		{"published, still naming the file", PassAnswer{Published: true, Named: true, Outcome: "unchanged"}, true, false, "still names the file"},
		{"not published", PassAnswer{Outcome: "reconciled"}, true, false, "did not publish"},
		{"blocked", PassAnswer{Blocked: "paused"}, true, false, "paused"},
		{"not exported", PassAnswer{NotExported: true}, false, true, ""},
		// The file is out of the published table and the pass failed on
		// something else: the old file can go.
		{"clear with a failed pass, about a file", PassAnswer{Published: true, Err: failed}, true, true, ""},
		// Before the job: a table published by an earlier pass is not
		// enough when this one failed.
		{"before the job, the pass failed", PassAnswer{Published: true, Err: failed}, false, false, "schema conflict"},
		{"before the job, the pass went through", PassAnswer{Published: true}, false, true, ""},
		{"a failed pass that did not publish", PassAnswer{Err: failed}, true, false, "schema conflict"},
	} {
		clear, why := tc.a.Verdict(tc.asked)
		if clear != tc.clear || !strings.Contains(why, tc.why) || (clear && why != "") {
			t.Errorf("%s: clear %v, why %q", tc.name, clear, why)
		}
	}
}

// A file on the cold tier that starts being left out while a listing runs
// (a re-home job switched its reads to the files replacing it): the listing
// is taken again. The hot listing cannot show a cold file, so the check that
// catches this for a hot file never saw it, and the pass would have published
// the cold file beside its replacements: its rows twice.
func TestFilesAndLocal1825_AColdFileLeftOutDuringTheListingListsAgain(t *testing.T) {
	ctx := context.Background()
	r := newTierRig(t, time.Hour)
	r.writeHot(0)
	r.migrate(0, false, true) // the old file: in cold storage only
	ex := &fakeExclusions{}
	r.src.SetExclusions(ex)
	fired := false
	// Between the hot listing and the second read of the exclusions: the job
	// puts its new file in place and switches reads to it.
	r.src.SetPendingOutputs(func(context.Context, string) (map[string]struct{}, error) {
		if !fired {
			fired = true
			r.writeHot(1)
			ex.set(r.key(0))
		}
		return nil, nil
	})
	files, _, err := r.src.FilesAndLocal(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("the job never switched during the listing: the test did not run its case")
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.Key] = true
	}
	if got[r.key(0)] || !got[r.key(1)] || len(got) != 1 {
		t.Fatalf("files = %v; want the new file only, not the cold file beside it", got)
	}
	if ex.calls != 4 {
		t.Fatalf("exclusions read %d times, want 4 (twice per listing, two listings)", ex.calls)
	}
}

// A cold file left out from before the listing is simply left out: nothing
// moved, nothing is listed again.
func TestFilesAndLocal1825_AColdFileLeftOutBeforeTheListing(t *testing.T) {
	ctx := context.Background()
	r := newTierRig(t, time.Hour)
	r.writeHot(0)
	r.migrate(0, false, true)
	r.writeHot(1)
	ex := &fakeExclusions{}
	ex.set(r.key(0))
	r.src.SetExclusions(ex)
	files, _, err := r.src.FilesAndLocal(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Key != r.key(1) || ex.calls != 2 {
		t.Fatalf("files %v, exclusions read %d times; want the hot file and one listing", files, ex.calls)
	}
}
