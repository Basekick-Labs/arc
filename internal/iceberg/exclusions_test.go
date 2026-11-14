package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// The exporter leaves out the files Arc's own reads leave out (#939 A0).

type fakeExclusions struct {
	mu    sync.Mutex
	keys  []string
	err   error
	calls int
}

func (f *fakeExclusions) set(keys ...string) {
	f.mu.Lock()
	f.keys = keys
	f.mu.Unlock()
}

func (f *fakeExclusions) ExcludedForStrict(_ context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	var out []string
	for _, k := range f.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func localRig(t *testing.T, keys ...string) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if err := b.Write(context.Background(), k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

var cpu = Measurement{Database: "mydb", Measurement: "cpu"}

const (
	hourDir = "mydb/cpu/2026/07/14/15/"
	oldKey  = hourDir + "a.parquet"
	newKey  = hourDir + "a_rewrite_1.parquet"
)

// A file Arc's reads leave out (a DELETE window: the old file next to its
// rewrite) is left out of both slices; an exclusion error fails the
// measurement rather than export a set that may count rows twice.
func TestFilesAndLocal_LeavesOutWhatReadsLeaveOut(t *testing.T) {
	ctx := context.Background()
	b := localRig(t, oldKey, newKey, hourDir+"b.parquet")
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	ex := &fakeExclusions{}
	src.SetExclusions(ex)
	ex.set(oldKey, "mydb/mem/2026/07/14/15/x.parquet")
	files, local, err := src.FilesAndLocal(ctx, cpu)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(files); len(got) != 2 || got[0] != "a_rewrite_1.parquet" || got[1] != "b.parquet" || len(local) != 2 {
		t.Fatalf("files = %v (local %d), want the rewrite and b only", got, len(local))
	}
	ex.err = errors.New("stat throttled")
	if _, _, err := src.FilesAndLocal(ctx, cpu); err == nil {
		t.Fatal("an exclusion error must fail the measurement")
	}
}

// The ordering: the exclusions are read before the listing, so a key left
// out has its replacement in the listing. A DELETE whose rewrite lands after
// the listing and is recorded before the second read must not drop the old
// file's rows (left out, its rewrite not listed) nor count them twice: the
// pass lists again and exports the rewrite only.
func TestFilesAndLocal_ExclusionAfterListingListsAgain(t *testing.T) {
	ctx := context.Background()
	b := localRig(t, oldKey)
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	ex := &fakeExclusions{}
	src.SetExclusions(ex)
	fired := false
	// pendingOutputs runs between the listing and the second exclusions
	// read: the DELETE publishes its rewrite and records the old file here.
	src.SetPendingOutputs(func(ctx context.Context, _ string) (map[string]struct{}, error) {
		if !fired {
			fired = true
			if err := b.Write(ctx, newKey, []byte("PAR1")); err != nil {
				t.Fatal(err)
			}
			ex.set(oldKey)
		}
		return nil, nil
	})
	files, _, err := src.FilesAndLocal(ctx, cpu)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(files); len(got) != 1 || got[0] != "a_rewrite_1.parquet" {
		t.Fatalf("files = %v, want the rewrite only", got)
	}
	if ex.calls != 4 {
		t.Fatalf("exclusions read %d times, want 4 (twice per listing, two listings)", ex.calls)
	}
}

// A file removed under the listing: the pass lists again, and exports the
// set the second listing sees.
func TestFilesAndLocal_RemovalUnderTheListingListsAgain(t *testing.T) {
	ctx := context.Background()
	out := hourDir + "cpu_x_b1_compacted.parquet"
	b := localRig(t, oldKey, hourDir+"b.parquet")
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	n := 0
	src.SetPendingOutputs(func(ctx context.Context, _ string) (map[string]struct{}, error) {
		n++
		if n == 1 {
			// A compaction completes after the listing: its output lands,
			// its inputs go. This listing has the inputs and not the output.
			if err := b.Write(ctx, out, []byte("PAR1")); err != nil {
				t.Fatal(err)
			}
			for _, k := range []string{oldKey, hourDir + "b.parquet"} {
				if err := b.Delete(ctx, k); err != nil {
					t.Fatal(err)
				}
			}
		}
		return nil, nil
	})
	files, _, err := src.FilesAndLocal(ctx, cpu)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(files); len(got) != 1 || got[0] != filepath.Base(out) {
		t.Fatalf("files = %v, want the compacted output only", got)
	}
}

// Files removed under every listing: after vanishRetries listings the
// measurement fails, keeping the previous snapshot, instead of exporting a
// set missing rows.
func TestFilesAndLocal_ChurnFailsTheMeasurement(t *testing.T) {
	ctx := context.Background()
	var keys []string
	for i := 0; i < vanishRetries+1; i++ {
		keys = append(keys, hourDir+string(rune('a'+i))+".parquet")
	}
	b := localRig(t, keys...)
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	n := 0
	src.SetPendingOutputs(func(ctx context.Context, _ string) (map[string]struct{}, error) {
		if err := b.Delete(ctx, keys[n]); err != nil {
			t.Fatal(err)
		}
		n++
		return nil, nil
	})
	if _, _, err := src.FilesAndLocal(ctx, cpu); err == nil || !strings.Contains(err.Error(), "kept changing") || !errors.Is(err, errSourcesMoving) {
		t.Fatalf("err = %v, want the churn error, marked as sources moving (reported busy)", err)
	}
	if n != vanishRetries {
		t.Fatalf("listed %d times, want %d", n, vanishRetries)
	}
}

// End to end (the live defect A0 fixes): across a row DELETE on a
// standalone node — old file only; rewrite published with the old file not
// yet removed (or its removal failed); old file gone — no committed snapshot
// holds both the old file and its rewrite.
func TestScheduler_DeleteWindowNeverDuplicates(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1_700_000_000_000_000)
	old := "mydb/cpu/2023/11/14/22/cpu_a.parquet"
	rw := "mydb/cpu/2023/11/14/22/cpu_a_rewrite_1.parquet"
	writeArcStyleParquet(t, filepath.Join(root, old), base, 50)

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ex := &fakeExclusions{}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	src.SetExclusions(ex)
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})

	live := func() []string {
		t.Helper()
		lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
		if err != nil {
			t.Fatal(err)
		}
		have, err := exp.tableDataFiles(ctx, lt)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for p := range have {
			names = append(names, filepath.Base(p))
		}
		sort.Strings(names)
		return names
	}

	sched.runPass(ctx)
	if got := live(); len(got) != 1 || got[0] != "cpu_a.parquet" {
		t.Fatalf("pass 1 live = %v, want the old file", got)
	}
	// The window: the rewrite is in storage and recorded, the old file not
	// removed yet (or its removal failed).
	writeArcStyleParquet(t, filepath.Join(root, rw), base, 30)
	ex.set(old)
	sched.runPass(ctx)
	if got := live(); len(got) != 1 || got[0] != "cpu_a_rewrite_1.parquet" {
		t.Fatalf("pass 2 (DELETE window) live = %v, want the rewrite only", got)
	}
	if err := backend.Delete(ctx, old); err != nil {
		t.Fatal(err)
	}
	ex.set()
	sched.runPass(ctx)
	if got := live(); len(got) != 1 || got[0] != "cpu_a_rewrite_1.parquet" {
		t.Fatalf("pass 3 live = %v, want the rewrite only", got)
	}
	lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatal(err)
	}
	for _, snap := range lt.Metadata().Snapshots() {
		tasks, err := lt.Scan(icetable.WithSnapshotID(snap.SnapshotID)).PlanFiles(ctx)
		if err != nil {
			t.Fatalf("snapshot %d: %v", snap.SnapshotID, err)
		}
		hasOld, hasNew := false, false
		for _, task := range tasks {
			switch filepath.Base(task.File.FilePath()) {
			case "cpu_a.parquet":
				hasOld = true
			case "cpu_a_rewrite_1.parquet":
				hasNew = true
			}
		}
		if hasOld && hasNew {
			t.Fatalf("snapshot %d holds a file and its DELETE rewrite: external readers would double count", snap.SnapshotID)
		}
	}
}

// flapExclusions names its key only on every second read: each listing sees
// it start being left out after the listing ran.
type flapExclusions struct {
	key   string
	calls int
}

func (f *flapExclusions) ExcludedForStrict(context.Context, string) ([]string, error) {
	f.calls++
	if f.calls%2 == 0 {
		return []string{f.key}, nil
	}
	return nil, nil
}

// A listing whose exclusions moved has no usable set, however often the
// same key moves: the measurement fails rather than export it (an empty
// set would empty the table).
func TestFilesAndLocal_RepeatedlyMovingExclusionsFail(t *testing.T) {
	b := localRig(t, oldKey, newKey)
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	src.SetExclusions(&flapExclusions{key: oldKey})
	files, _, err := src.FilesAndLocal(context.Background(), cpu)
	if err == nil {
		t.Fatalf("moving exclusions exported %v, want an error", keysOf(files))
	}
}

// Shared storage: a listed file the replicated manifest does not know means this node's manifest
// (where the exclusions come from) lags a commit; the source waits for it once and lists again,
// and does not wait when every listed file is known.
func TestFilesAndLocal_ManifestBarrierOnlyForUnknownFiles(t *testing.T) {
	ctx := context.Background()
	b := localRig(t, oldKey, newKey)
	src := NewStorageWalkSource(b, "arc", zerolog.Nop())
	ex := &fakeExclusions{}
	src.SetExclusions(ex)
	known := map[string]bool{oldKey: true}
	syncs := 0
	src.SetManifestBarrier(func(p string) bool { return known[p] }, func(context.Context) error {
		// The manifest catches up: the rewrite is registered and the old file left out.
		syncs++
		known[newKey] = true
		ex.set(oldKey)
		return nil
	})
	files, _, err := src.FilesAndLocal(ctx, cpu)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(files); len(got) != 1 || got[0] != "a_rewrite_1.parquet" || syncs != 1 {
		t.Fatalf("files = %v after %d syncs, want the rewrite only after 1", got, syncs)
	}
	if _, _, err := src.FilesAndLocal(ctx, cpu); err != nil || syncs != 1 {
		t.Fatalf("every file known: %d syncs (err %v), want no further one", syncs, err)
	}
	// A file the manifest never learns of costs one barrier, not one per retry,
	// and not one per pass (#1609): after a barrier it is remembered as
	// unmanaged.
	delete(known, newKey)
	src.SetManifestBarrier(func(p string) bool { return known[p] }, func(context.Context) error { syncs++; return nil })
	if _, _, err := src.FilesAndLocal(ctx, cpu); err != nil || syncs != 2 {
		t.Fatalf("an unknown file: %d syncs (err %v), want exactly one more", syncs, err)
	}
	for pass := 0; pass < 3; pass++ {
		if _, _, err := src.FilesAndLocal(ctx, cpu); err != nil || syncs != 2 {
			t.Fatalf("pass %d over the same unmanaged file: %d syncs (err %v), want no further one", pass, syncs, err)
		}
	}
	// Once the manifest learns of it, it is forgotten: a later unknown state
	// waits again.
	known[newKey] = true
	if _, _, err := src.FilesAndLocal(ctx, cpu); err != nil || syncs != 2 {
		t.Fatalf("known file: %d syncs (err %v)", syncs, err)
	}
	delete(known, newKey)
	if _, _, err := src.FilesAndLocal(ctx, cpu); err != nil || syncs != 3 {
		t.Fatalf("unknown again after being known: %d syncs (err %v), want one more", syncs, err)
	}

	fresh := NewStorageWalkSource(b, "arc", zerolog.Nop())
	fresh.SetManifestBarrier(func(string) bool { return false }, func(context.Context) error { return errors.New("no leader") })
	if _, _, err := fresh.FilesAndLocal(ctx, cpu); err == nil {
		t.Fatal("a failed barrier must fail the measurement")
	}
}
