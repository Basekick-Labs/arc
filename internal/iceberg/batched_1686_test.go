package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1686: a first registration with more new files than fit in one measurement deadline must
// commit what it gets through, and the next pass must continue from the table.

const day14 = int64(1_700_000_000_000_000) // 2023-11-14T22:13:20Z
const dayMicros = int64(86_400_000_000)

// rig1686 writes Arc-layout files under one root and reconciles them with a test exporter.
type rig1686 struct {
	t    *testing.T
	root string
	exp  *Exporter
}

func newRig1686(t *testing.T, retain, chunkMax int) *rig1686 {
	t.Helper()
	dir := t.TempDir()
	exp := newTestExporter(t, dir, retain)
	exp.chunkMax = chunkMax
	return &rig1686{t: t, root: dir, exp: exp}
}

// write creates mydb/cpu/<day dir>/<name>.parquet with rows in that day and returns its ref.
func (r *rig1686) write(dayOffset int, name string) FileRef {
	r.t.Helper()
	ts := day14 + int64(dayOffset)*dayMicros
	tm := time.UnixMicro(ts).UTC()
	rel := fmt.Sprintf("mydb/cpu/%04d/%02d/%02d/22/%s.parquet", tm.Year(), tm.Month(), tm.Day(), name)
	p := filepath.Join(r.root, rel)
	writeArcStyleParquet(r.t, p, ts, 5)
	ref := refOf(r.t, p)
	ref.Key = rel
	ref.LocalPath = p
	return ref
}

func (r *rig1686) schema(f FileRef) ArcSchema {
	r.t.Helper()
	sc, err := SchemaFromParquet(f.LocalPath)
	if err != nil {
		r.t.Fatal(err)
	}
	return sc
}

func (r *rig1686) live() map[string]liveEntry {
	return liveEntries(context.Background(), r.t, r.exp, "mydb", "cpu")
}

func (r *rig1686) snapshots() int {
	r.t.Helper()
	tbl, err := r.exp.EnsureTable(context.Background(), "mydb", "cpu", ArcSchema{})
	if err != nil {
		r.t.Fatal(err)
	}
	return len(tbl.Metadata().Snapshots())
}

// readLocal is the scheduler's readSchemas over local fixture files.
func readLocal(ctx context.Context, files []FileRef) (ArcSchema, error) {
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.LocalPath
	}
	return UnionSchema(ctx, paths)
}

func TestReconcile1686_NewFilesCommitInChunks(t *testing.T) {
	r := newRig1686(t, 0, 2) // retain 0: keep every snapshot, so commits can be counted
	var files []FileRef
	for i := 0; i < 5; i++ {
		files = append(files, r.write(0, fmt.Sprintf("f%d", i)))
	}
	settled, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", r.schema(files[0]), files)
	if err != nil || !settled {
		t.Fatalf("settled %v, err %v", settled, err)
	}
	if got := len(r.live()); got != 5 {
		t.Fatalf("live files %d, want 5", got)
	}
	if got := r.snapshots(); got != 3 {
		t.Fatalf("snapshots %d, want 3 (5 files in chunks of 2)", got)
	}
}

// A pass that runs out of time keeps what it committed, reports partial rather than an error,
// and the next pass registers the rest.
func TestReconcile1686_PartialPassKeepsProgressAndResumes(t *testing.T) {
	r := newRig1686(t, 3, 2)
	r.exp.commitReserve = time.Second
	var files []FileRef
	for i := 0; i < 8; i++ {
		files = append(files, r.write(0, fmt.Sprintf("f%d", i)))
	}
	slow := func(ctx context.Context, fs []FileRef) (ArcSchema, error) {
		select {
		case <-time.After(700 * time.Millisecond):
		case <-ctx.Done():
			return ArcSchema{}, ctx.Err()
		}
		return readLocal(ctx, fs)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := r.exp.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", current: files, unread: files, readSchemas: slow})
	if err != nil {
		t.Fatalf("partial pass returned an error: %v", err)
	}
	if !res.partial || res.settled || res.registered == 0 || res.registered+res.remaining != len(files) {
		t.Fatalf("result %+v, want partial with progress", res)
	}
	if got := len(r.live()); got != res.registered {
		t.Fatalf("table holds %d files, the pass reported %d registered", got, res.registered)
	}

	// The next pass: what the table holds is known, only the rest is read.
	seed, tableFiles, ok, err := r.exp.tableSeed(context.Background(), "mydb", "cpu")
	if err != nil || !ok {
		t.Fatalf("tableSeed: %v %v", ok, err)
	}
	var unread []FileRef
	for _, f := range files {
		if _, in := tableFiles[f.PhysicalPath]; !in {
			unread = append(unread, f)
		}
	}
	if len(unread) != res.remaining {
		t.Fatalf("%d files unregistered after the partial pass, it reported %d remaining", len(unread), res.remaining)
	}
	res, err = r.exp.reconcile(context.Background(), reconcileRequest{database: "mydb", measurement: "cpu", schema: seed, current: files, unread: unread, readSchemas: readLocal})
	if err != nil || !res.settled || res.partial {
		t.Fatalf("second pass: %+v, err %v", res, err)
	}
	if got := len(r.live()); got != len(files) {
		t.Fatalf("live %d after the second pass, want %d", got, len(files))
	}
}

// A chunk that does not fit in the time left commits nothing, is not an error, and shrinks the
// next chunk, so a store too slow for a full chunk still converges.
func TestReconcile1686_ChunkThatDoesNotFitShrinks(t *testing.T) {
	r := newRig1686(t, 3, 16)
	r.exp.commitReserve = 500 * time.Millisecond
	var files []FileRef
	for i := 0; i < 20; i++ {
		files = append(files, r.write(0, fmt.Sprintf("f%02d", i)))
	}
	var calls atomic.Int32
	stuck := func(ctx context.Context, fs []FileRef) (ArcSchema, error) {
		if calls.Add(1) == 1 {
			return readLocal(ctx, fs) // the first chunk, read before the table is created
		}
		<-ctx.Done()
		return ArcSchema{}, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := r.exp.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", current: files, unread: files, readSchemas: stuck})
	if err != nil {
		t.Fatalf("a chunk out of time failed the pass: %v", err)
	}
	if !res.partial || res.registered != 16 || res.remaining != 4 {
		t.Fatalf("result %+v, want 16 registered and 4 remaining", res)
	}
	if got := r.exp.chunkSizeFor("mydb\x00cpu"); got != 8 {
		t.Fatalf("chunk size after a chunk that did not fit: %d, want 8", got)
	}
}

// Compaction (inputs out, one output in, the same day) is one commit, whatever else is waiting:
// a reader must never see the day's inputs gone and the output not yet there. And a file
// already in the table is never removed because the pass did not get to the new ones.
func TestReconcile1686_CompactionStaysAtomicAndDeferredFilesRemoveNothing(t *testing.T) {
	r := newRig1686(t, 3, 1)
	f1, f2, g1 := r.write(0, "f1"), r.write(0, "f2"), r.write(1, "g1")
	sc := r.schema(f1)
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, []FileRef{f1, g1, f2}); err != nil {
		t.Fatal(err)
	}
	// Compact day 14 (f1+f2 -> c) and add three new files in day 16.
	for _, f := range []FileRef{f1, f2} {
		if err := os.Remove(f.LocalPath); err != nil {
			t.Fatal(err)
		}
	}
	c := r.write(0, "c")
	n1, n2, n3 := r.write(2, "n1"), r.write(2, "n2"), r.write(2, "n3")
	current := []FileRef{g1, c, n1, n2, n3}

	var commits atomic.Int32
	oneCommit := func() bool { return commits.Add(1) <= 1 }
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: current,
		unread: []FileRef{c, n1, n2, n3}, readSchemas: readLocal, canCommit: oneCommit,
	})
	if err != nil {
		t.Fatal(err)
	}
	live := r.live()
	if _, ok := live[c.PhysicalPath]; !ok || len(live) != 2 {
		t.Fatalf("after one commit the table holds %v; want exactly the compacted output and g1", keys(live))
	}
	if _, ok := live[g1.PhysicalPath]; !ok {
		t.Fatal("g1, already registered, was removed by a pass that deferred other files")
	}
	if !res.partial || res.remaining != 3 {
		t.Fatalf("result %+v, want partial with the three day-16 files remaining", res)
	}
}

// A table that does not exist is never created from a schema no footer was read into: its
// day(time) partition spec comes from that schema and cannot be added later.
func TestReconcile1686_NoTableFromAnUnreadSchema(t *testing.T) {
	r := newRig1686(t, 3, 4)
	f := r.write(0, "f")
	boom := errors.New("footer read timed out")
	_, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", current: []FileRef{f}, unread: []FileRef{f},
		readSchemas: func(context.Context, []FileRef) (ArcSchema, error) { return ArcSchema{}, boom },
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err %v, want the read failure", err)
	}
	exists, err := r.exp.tableExists(context.Background(), "mydb", "cpu")
	if err != nil || exists {
		t.Fatalf("table exists %v (err %v) after a pass that read no schema", exists, err)
	}
}

func TestDayOf1686(t *testing.T) {
	for _, tc := range []struct{ p, want string }{
		{"file:///data/mydb/cpu/2023/11/14/22/a.parquet", "2023/11/14"},
		{"/data/mydb/cpu/2023/11/14/a.parquet", "2023/11/14"},                 // daily-compacted: no hour dir
		{"s3://bucket/prefix/mydb/cpu/2023/11/14/22/a.parquet", "2023/11/14"}, // cold tier
		{"mydb/cpu/2023/11/14/22/a.parquet", "2023/11/14"},                    // a bare key
		{"file:///x/mydb/cpu/mydb/cpu/2023/11/14/22/a.parquet", "2023/11/14"}, // the last match wins
		{"file:///data/otherdb/cpu/2023/11/14/22/a.parquet", ""},              // another measurement
		{"file:///data/mydb/cpu/a.parquet", ""},                               // no date dirs
		{"file:///data/mydb/cpux/2023/11/14/22/a.parquet", ""},                // sibling name
	} {
		if got := dayOf(tc.p, "mydb", "cpu"); got != tc.want {
			t.Errorf("dayOf(%q) = %q, want %q", tc.p, got, tc.want)
		}
	}
}

func TestSplitAnchored1686(t *testing.T) {
	p := func(day, f string) string { return "file:///d/mydb/cpu/2023/11/" + day + "/22/" + f }
	build := []string{p("14", "c"), p("15", "n1"), p("16", "n2")}

	if a, f := splitAnchored(build, nil, "mydb", "cpu"); len(a) != 0 || len(f) != 3 {
		t.Fatalf("no removals: anchored %v free %v; want everything free", a, f)
	}
	a, f := splitAnchored(build, []string{p("14", "old")}, "mydb", "cpu")
	if len(a) != 1 || a[0] != p("14", "c") || len(f) != 2 {
		t.Fatalf("removal in day 14: anchored %v free %v; want only the day-14 file anchored", a, f)
	}
	// A removal whose day cannot be read anchors everything: the pre-#1686 single commit.
	if a, f := splitAnchored(build, []string{"file:///elsewhere/x.parquet"}, "mydb", "cpu"); len(a) != 3 || len(f) != 0 {
		t.Fatalf("unknown removal day: anchored %v free %v; want everything anchored", a, f)
	}
	// A new file whose day cannot be read is anchored when anything is removed.
	odd := "file:///d/mydb/cpu/x.parquet"
	if a, _ := splitAnchored([]string{odd}, []string{p("14", "old")}, "mydb", "cpu"); len(a) != 1 {
		t.Fatalf("unknown add day: anchored %v; want it anchored", a)
	}
}

// The scheduler path: a pass that loses the writer role after its first chunk reports the
// measurement in progress (not failed), and the next pass, seeded cold from the table,
// registers the rest and settles.
func TestScheduler1686_InProgressThenSettles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		writeArcStyleParquet(t, filepath.Join(root, fmt.Sprintf("mydb/cpu/2023/11/14/22/f%d.parquet", i)), day14+int64(i), 5)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	exp.chunkMax = 2
	gate := &countingGate{allow: 3} // pass start, the measurement, the first chunk's commit
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Gate: gate, Logger: zerolog.Nop()})

	if !sched.runPass(ctx) {
		t.Fatal("first pass did not report a partly registered measurement")
	}
	if got := len(liveEntries(ctx, t, exp, "mydb", "cpu")); got != 2 {
		t.Fatalf("after the first pass the table holds %d files, want the first chunk's 2", got)
	}
	if len(sched.state) != 0 {
		t.Fatal("a partial pass cached the measurement's state")
	}
	gate.allow = 1 << 30
	if sched.runPass(ctx) {
		t.Fatal("second pass still reports partial")
	}
	if got := len(liveEntries(ctx, t, exp, "mydb", "cpu")); got != 5 {
		t.Fatalf("after the second pass the table holds %d files, want 5", got)
	}
	if _, ok := sched.state["mydb\x00cpu"]; !ok {
		t.Fatal("a settled pass did not cache the measurement")
	}
}

type countingGate struct {
	allow int
	calls int
}

func (g *countingGate) CanRun() bool {
	g.calls++
	return g.calls <= g.allow
}

func keys(m map[string]liveEntry) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// A node that loses the writer role between chunks commits nothing more, not even the schema
// change the next chunk would need.
func TestReconcile1686_WriterLostBeforeASchemaChangeCommitsNothing(t *testing.T) {
	r := newRig1686(t, 3, 1)
	old := r.write(0, "a")
	sc := r.schema(old)
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, []FileRef{old}); err != nil {
		t.Fatal(err)
	}
	wide := r.write(1, "b") // older of the two new files: registered second
	writeArcStyleParquet4Col(t, wide.LocalPath, day14+dayMicros, 5)
	wide = refOf(t, wide.LocalPath)
	wide.Key, wide.LocalPath = "mydb/cpu/2023/11/15/22/b.parquet", filepath.Join(r.root, "mydb/cpu/2023/11/15/22/b.parquet")
	narrow := r.write(2, "c")

	var calls atomic.Int32
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: []FileRef{old, wide, narrow},
		unread: []FileRef{wide, narrow}, readSchemas: readLocal,
		canCommit: func() bool { return calls.Add(1) <= 1 },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.partial || res.registered != 1 {
		t.Fatalf("result %+v, want one chunk registered and the pass stopped", res)
	}
	tbl, err := r.exp.EnsureTable(context.Background(), "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.Schema().FindFieldByName("cpu_idle"); ok {
		t.Fatal("a node that lost the writer role committed a schema change")
	}
}

// A pass of several commits claims each intermediate version, so a version another writer
// already published stops it at that commit (adopt.go's fence), not only at the last one.
func TestReconcile1686_MidPassVersionsAreClaimedAndFenced(t *testing.T) {
	r := newRig1686(t, 3, 1)
	// Discovery files need a backend: an exporter over the rig's root, warehouse at the root.
	backend, err := storage.NewLocalBackend(r.root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if r.exp, err = NewExporter(db, backend, DefaultWarehouse(backend), "arc", 3, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	r.exp.chunkMax = 1
	a := r.write(0, "a")
	sc := r.schema(a)
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, []FileRef{a}); err != nil {
		t.Fatal(err)
	}
	tbl, err := r.exp.EnsureTable(context.Background(), "mydb", "cpu", sc)
	if err != nil {
		t.Fatal(err)
	}
	cur := r.exp.metadataVersion(tbl)
	_, dirKey, _ := r.exp.parseVersionAndMetaDir(tbl.MetadataLocation())
	// Another writer's v<cur+2>: the second commit of the next pass lands on it, and the third
	// commit's claim of it must collide.
	foreign := filepath.Join(r.root, dirKey, fmt.Sprintf("v%d.metadata.json", cur+2))
	if err := os.WriteFile(foreign, []byte(`{"another":"writer"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	files := []FileRef{a, r.write(1, "b"), r.write(2, "c"), r.write(3, "d")}
	_, err = r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files)
	if !errors.Is(err, errFenced) {
		t.Fatalf("err %v, want errFenced from the mid-pass claim", err)
	}
	if got := len(r.live()); got != 3 {
		t.Fatalf("live %d, want the 3 files committed before the fence (a and two chunks)", got)
	}
	hint, err := os.ReadFile(filepath.Join(r.root, dirKey, "version-hint.text"))
	if err != nil {
		t.Fatal(err)
	}
	if string(hint) != fmt.Sprint(cur) {
		t.Fatalf("version-hint %s after a fenced pass, want it left at %d", hint, cur)
	}
}

// A pass with no time for even one chunk is a failure, not progress: in_progress would bring
// every later pass forward.
func TestReconcile1686_NoChunkFitsIsAFailureNotProgress(t *testing.T) {
	r := newRig1686(t, 3, 4)
	a := r.write(0, "a")
	sc := r.schema(a)
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, []FileRef{a}); err != nil {
		t.Fatal(err)
	}
	r.exp.commitReserve = time.Hour // more than the deadline below leaves
	b := r.write(1, "b")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res, err := r.exp.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: []FileRef{a, b}, unread: []FileRef{b}, readSchemas: readLocal})
	if err == nil || res.partial {
		t.Fatalf("result %+v, err %v; want an error and not partial", res, err)
	}
}
