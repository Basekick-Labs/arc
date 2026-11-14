package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"gocloud.dev/blob/memblob"

	"github.com/basekick-labs/arc/internal/storage"
)

// Source files deleted by another Arc writer path (compaction, DELETE rewrite, retention) while an
// export pass runs: the pass leaves what they touched for the next one instead of failing
// (vanished.go).

// existsLocal is the source's FileExists over local fixture files.
func existsLocal(_ context.Context, f FileRef) (bool, error) {
	_, err := os.Stat(f.LocalPath)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func removeFile(t *testing.T, f FileRef) {
	t.Helper()
	if err := os.Remove(f.LocalPath); err != nil {
		t.Fatal(err)
	}
}

// A new file deleted after the listing, before its footer is read: its whole day is left for the
// next pass, the other days of the chunk commit, and the pass is not settled.
func TestVanished_FreeChunkLeavesTheDayAndCommitsTheRest(t *testing.T) {
	r := newRig1686(t, 3, 16)
	g1 := r.write(0, "g1")
	sc := r.schema(g1)
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, []FileRef{g1}); err != nil {
		t.Fatal(err)
	}
	a1, a2, b1, b2 := r.write(1, "a1"), r.write(1, "a2"), r.write(2, "b1"), r.write(2, "b2")
	removeFile(t, a1) // compacted away after the listing
	current := []FileRef{g1, a1, a2, b1, b2}

	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: current,
		unread: []FileRef{a1, a2, b1, b2}, readSchemas: readLocal, exists: existsLocal,
	})
	if err != nil {
		t.Fatalf("a vanished source failed the pass: %v", err)
	}
	if !res.moved || res.settled || res.commits != 1 {
		t.Fatalf("result %+v, want moved, not settled, one commit", res)
	}
	live := r.live()
	for _, f := range []FileRef{g1, b1, b2} {
		if _, ok := live[f.PhysicalPath]; !ok {
			t.Fatalf("%s missing from the table: %v", f.Key, keys(live))
		}
	}
	for _, f := range []FileRef{a1, a2} {
		if _, ok := live[f.PhysicalPath]; ok {
			t.Fatalf("%s registered although its day had a vanished file", f.Key)
		}
	}
	if res.remaining != 2 {
		t.Fatalf("remaining %d, want the vanished day's 2 files", res.remaining)
	}
}

// A file of commit 0 vanished: its day is deferred whole (removals included), the other day's
// compaction commits atomically.
func TestVanished_Commit0DefersOnlyTheVanishedDay(t *testing.T) {
	r := newRig1686(t, 3, 16)
	f1, f2, h1, h2 := r.write(0, "f1"), r.write(0, "f2"), r.write(1, "h1"), r.write(1, "h2")
	sc := r.schema(f1)
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, []FileRef{f1, f2, h1, h2}); err != nil {
		t.Fatal(err)
	}
	// Both days compacted; day 1's output is itself gone by the time the pass reads it.
	for _, f := range []FileRef{f1, f2, h1, h2} {
		removeFile(t, f)
	}
	c0, c1 := r.write(0, "c0"), r.write(1, "c1")
	removeFile(t, c1)

	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: []FileRef{c0, c1},
		unread: []FileRef{c0, c1}, readSchemas: readLocal, exists: existsLocal,
	})
	if err != nil {
		t.Fatalf("a vanished compaction output failed the pass: %v", err)
	}
	if !res.moved || res.settled {
		t.Fatalf("result %+v, want moved and not settled", res)
	}
	live := r.live()
	want := map[string]bool{c0.PhysicalPath: true, h1.PhysicalPath: true, h2.PhysicalPath: true}
	if len(live) != len(want) {
		t.Fatalf("table holds %v; want day 0 compacted (c0) and day 1 untouched (h1, h2)", keys(live))
	}
	for p := range want {
		if _, ok := live[p]; !ok {
			t.Fatalf("table holds %v; missing %s", keys(live), p)
		}
	}
}

// No table yet: a vanished file in the first read leaves its day behind, and the table is created
// from the rest. If every file vanished, nothing is created and the pass is busy, not failed.
func TestVanished_NewTableFirstRead(t *testing.T) {
	r := newRig1686(t, 3, 16)
	a1, a2, b1 := r.write(0, "a1"), r.write(0, "a2"), r.write(1, "b1")
	removeFile(t, a1)
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", current: []FileRef{a1, a2, b1},
		unread: []FileRef{a1, a2, b1}, readSchemas: readLocal, exists: existsLocal,
	})
	if err != nil {
		t.Fatalf("first read with a vanished file: %v", err)
	}
	live := r.live()
	if _, ok := live[b1.PhysicalPath]; !ok || len(live) != 1 || !res.moved || res.settled {
		t.Fatalf("table %v, result %+v; want only b1 and a moved, unsettled pass", keys(live), res)
	}

	r2 := newRig1686(t, 3, 16)
	x := r2.write(0, "x")
	removeFile(t, x)
	res, err = r2.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", current: []FileRef{x},
		unread: []FileRef{x}, readSchemas: readLocal, exists: existsLocal,
	})
	if err != nil || !res.moved || res.commits != 0 {
		t.Fatalf("every file vanished: result %+v err %v; want moved, nothing committed, no error", res, err)
	}
	if exists, err := r2.exp.tableExists(context.Background(), "mydb", "cpu"); err != nil || exists {
		t.Fatalf("table exists %v (err %v) with no footer read", exists, err)
	}
}

// Only a file its own backend says is gone is treated as vanished. A not-found on a file that is
// still there, any other read error, and a caller with no existence check keep failing the pass.
func TestVanished_OnlyConfirmedAbsenceIsForgiven(t *testing.T) {
	r := newRig1686(t, 3, 16)
	f := r.write(0, "f")
	notFound := func(context.Context, []FileRef) (ArcSchema, error) {
		return ArcSchema{}, fmt.Errorf("read footer: %w", fs.ErrNotExist)
	}
	other := errors.New("permission denied")
	var probes atomic.Int32
	counting := func(ctx context.Context, f FileRef) (bool, error) {
		probes.Add(1)
		return existsLocal(ctx, f)
	}
	cases := []struct {
		name   string
		read   func(context.Context, []FileRef) (ArcSchema, error)
		exists func(context.Context, FileRef) (bool, error)
		probes int32
	}{
		{"not-found on a file that exists", notFound, counting, 1},
		{"another read error is not probed", func(context.Context, []FileRef) (ArcSchema, error) { return ArcSchema{}, other }, counting, 0},
		{"no existence check", notFound, nil, 0},
	}
	for _, c := range cases {
		probes.Store(0)
		res, err := r.exp.reconcile(context.Background(), reconcileRequest{
			database: "mydb", measurement: "cpu", current: []FileRef{f},
			unread: []FileRef{f}, readSchemas: c.read, exists: c.exists,
		})
		if err == nil || res.moved {
			t.Fatalf("%s: result %+v err %v; want the read error", c.name, res, err)
		}
		if got := probes.Load(); got != c.probes {
			t.Fatalf("%s: %d existence checks, want %d", c.name, got, c.probes)
		}
	}
}

// movingSource is a source whose listing never holds still.
type movingSource struct{}

func (movingSource) Measurements(context.Context) ([]Measurement, error) {
	return []Measurement{{Database: "mydb", Measurement: "cpu"}}, nil
}
func (movingSource) Files(context.Context, Measurement) ([]FileRef, error) { return nil, nil }
func (movingSource) LocalFiles(context.Context, Measurement) ([]string, error) {
	return nil, nil
}
func (movingSource) FilesAndLocal(context.Context, Measurement) ([]FileRef, []string, error) {
	return nil, nil, fmt.Errorf("file set of mydb/cpu kept changing: %w", errSourcesMoving)
}
func (movingSource) FileSchema(context.Context, FileRef) (ArcSchema, error) {
	return ArcSchema{}, nil
}

// A busy measurement brings the next pass forward and is not a failure; after busyEscalateAfter
// without progress it is one, and stops bringing passes forward.
func TestScheduler_BusyThenEscalates(t *testing.T) {
	r := newRig1686(t, 3, 16)
	sched := NewScheduler(SchedulerConfig{Exporter: r.exp, Source: movingSource{}, Logger: zerolog.Nop()})
	key := "mydb\x00cpu"
	sched.state[key] = measurementState{fingerprint: "x"}
	if !sched.runPass(context.Background()) {
		t.Fatal("a busy measurement did not bring the next pass forward")
	}
	if _, ok := sched.busySince[key]; !ok {
		t.Fatal("busy clock not started")
	}
	if _, ok := sched.state[key]; !ok {
		t.Fatal("a listing that never held still dropped the cached state; nothing was committed")
	}
	sched.busySince[key] = time.Now().Add(-busyEscalateAfter - time.Minute)
	if sched.runPass(context.Background()) {
		t.Fatal("an escalated measurement still brings passes forward")
	}
}

// The busy clock only runs while nothing progresses, and leaves with the measurement.
func TestScheduler_BusyClockResetsAndIsPruned(t *testing.T) {
	r := newRig1686(t, 3, 16)
	sched := NewScheduler(SchedulerConfig{Exporter: r.exp, Source: movingSource{}, Logger: zerolog.Nop()})
	sched.busySince["gone\x00m"] = time.Now()
	sched.runPass(context.Background())
	if _, ok := sched.busySince["gone\x00m"]; ok {
		t.Fatal("busy clock kept for a measurement no longer listed")
	}
	gated := NewScheduler(SchedulerConfig{Exporter: r.exp, Source: movingSource{}, Gate: &countingGate{allow: 0}, Logger: zerolog.Nop()})
	gated.busySince["mydb\x00cpu"] = time.Now()
	gated.runPass(context.Background())
	if len(gated.busySince) != 0 {
		t.Fatal("busy clock kept on a node that is not the writer")
	}
}

func TestDropVanished(t *testing.T) {
	p := func(day, name string) string { return "s3://b/mydb/cpu/2023/11/" + day + "/22/" + name + ".parquet" }
	paths := []string{p("14", "a"), p("14", "b"), p("15", "c"), "s3://b/odd/x.parquet"}
	kept, dropped := dropVanished(paths, []string{p("14", "a")}, "mydb", "cpu")
	if len(kept) != 2 || len(dropped) != 2 {
		t.Fatalf("kept %v dropped %v; want day 14 dropped whole", kept, dropped)
	}
	kept, dropped = dropVanished(paths, []string{"s3://b/odd/x.parquet"}, "mydb", "cpu")
	if len(kept) != 3 || len(dropped) != 1 {
		t.Fatalf("kept %v dropped %v; want only the unreadable-day file dropped", kept, dropped)
	}
}

// FileExists asks the backend that holds the file: a cold file is present on cold even with no
// hot copy, and a cold HEAD that fails is an error, never "gone".
func TestFileExists_RoutesColdToTheColdTier(t *testing.T) {
	r := newTierRig(t, time.Hour)
	r.writeHot(1)
	r.migrate(1, false, true)
	ctx := context.Background()
	if ok, err := r.src.FileExists(ctx, FileRef{Key: r.key(1), Cold: true}); err != nil || !ok {
		t.Fatalf("cold file: exists %v err %v; want present", ok, err)
	}
	if ok, err := r.src.FileExists(ctx, FileRef{Key: r.key(1)}); err != nil || ok {
		t.Fatalf("hot copy deleted: exists %v err %v; want absent", ok, err)
	}

	f := newTierRig(t, time.Hour, true)
	f.coldStatErr = errors.New("throttled")
	if _, err := f.src.FileExists(ctx, FileRef{Key: f.key(1), Cold: true}); err == nil {
		t.Fatal("a failing cold HEAD was reported as an answer")
	}
}

// compactingSource deletes one listed file right after the listing, as a compaction finishing
// between the listing and the footer reads does.
type compactingSource struct {
	*StorageWalkSource
	victim string // local path deleted after the next listing
}

func (c *compactingSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	files, local, err := c.StorageWalkSource.FilesAndLocal(ctx, m)
	if c.victim != "" {
		if rerr := os.Remove(c.victim); rerr != nil {
			return nil, nil, rerr
		}
		c.victim = ""
	}
	return files, local, err
}

// Through the scheduler and the real source: a file deleted after the listing makes the pass busy
// (nothing else to commit) and brings the next one forward; the next pass settles.
func TestScheduler_VanishedAfterListingIsBusyThenSettles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet")
	writeArcStyleParquet(t, victim, day14, 5)
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/15/22/b.parquet"), day14+dayMicros, 5)
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := &compactingSource{StorageWalkSource: NewStorageWalkSource(backend, "arc", zerolog.Nop()), victim: victim}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})

	if !sched.runPass(ctx) {
		t.Fatal("a pass whose source vanished did not bring the next one forward")
	}
	if got := len(liveEntries(ctx, t, exp, "mydb", "cpu")); got != 1 {
		t.Fatalf("after the first pass the table holds %d files, want b only", got)
	}
	if sched.runPass(ctx) {
		t.Fatal("second pass still busy")
	}
	if _, ok := sched.state["mydb\x00cpu"]; !ok {
		t.Fatal("the second pass did not settle")
	}
	if len(sched.busySince) != 0 {
		t.Fatal("busy clock kept after progress")
	}
}

// iceberg-go's S3 FileIO reports a 404 on a range read (after a successful open) as a gocloud
// error, not fs.ErrNotExist: it must still count as a not-found read, wrapped or not.
func TestIsReadNotFound_GocloudRangeRead(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer b.Close()
	_, err := b.NewRangeReader(ctx, "missing.parquet", 0, 8, nil)
	if err == nil {
		t.Fatal("range read of a missing object succeeded")
	}
	if !isReadNotFound(fmt.Errorf("build data file for x: %w", err)) {
		t.Fatalf("gocloud not-found not recognised: %v", err)
	}
	if isReadNotFound(errors.New("permission denied")) {
		t.Fatal("an unrelated error recognised as not-found")
	}
}

// A commit-0 file whose day cannot be read from its key is anchored to every day: when another
// file of commit 0 vanishes, it must not commit alone while its day's removals wait.
func TestVanished_Commit0UnreadableDayDefersEverything(t *testing.T) {
	r := newRig1686(t, 3, 16)
	f1, f2 := r.write(0, "f1"), r.write(0, "f2")
	sc := r.schema(f1)
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, []FileRef{f1, f2}); err != nil {
		t.Fatal(err)
	}
	removeFile(t, f1)
	removeFile(t, f2)
	c0 := r.write(0, "c0")
	removeFile(t, c0)
	oddRel := "mydb/cpu/misc/odd.parquet"
	oddPath := filepath.Join(r.root, oddRel)
	writeArcStyleParquet(t, oddPath, day14, 5)
	odd := refOf(t, oddPath)
	odd.Key, odd.LocalPath = oddRel, oddPath

	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: []FileRef{c0, odd},
		unread: []FileRef{c0, odd}, readSchemas: readLocal, exists: existsLocal,
	})
	if err != nil || !res.moved || res.commits != 0 {
		t.Fatalf("result %+v err %v; want moved with nothing committed", res, err)
	}
	live := r.live()
	if _, ok := live[odd.PhysicalPath]; ok || len(live) != 2 {
		t.Fatalf("table holds %v; want f1 and f2 untouched and odd not committed alone", keys(live))
	}
}
