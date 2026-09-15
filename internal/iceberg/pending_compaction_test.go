package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
)

func keysOf(files []FileRef) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, filepath.Base(f.PhysicalPath))
	}
	sort.Strings(out)
	return out
}

// TestFilesAndLocal_ExcludesPendingCompactionOutputs (regression, #638): a key
// named by the pending-output lookup is left out of both slices; nil lookup and
// an empty set change nothing; a lookup error fails the measurement.
func TestFilesAndLocal_ExcludesPendingCompactionOutputs(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"mydb/cpu/2026/07/14/15/a.parquet", "mydb/cpu/2026/07/14/15/b.parquet", "mydb/cpu/2026/07/14/15/cpu_x_b1_compacted.parquet"} {
		if err := backend.Write(ctx, k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	m := Measurement{Database: "mydb", Measurement: "cpu"}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	files, local, err := src.FilesAndLocal(ctx, m)
	if err != nil || len(files) != 3 || len(local) != 3 {
		t.Fatalf("no lookup: files=%v local=%d err=%v", keysOf(files), len(local), err)
	}
	var gotPrefix string
	src.SetPendingOutputs(func(_ context.Context, prefix string) (map[string]struct{}, error) {
		gotPrefix = prefix
		return map[string]struct{}{"mydb/cpu/2026/07/14/15/cpu_x_b1_compacted.parquet": {}}, nil
	})
	files, local, err = src.FilesAndLocal(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if gotPrefix != "mydb/cpu/" {
		t.Errorf("lookup prefix = %q, want mydb/cpu/", gotPrefix)
	}
	if got := keysOf(files); len(got) != 2 || got[0] != "a.parquet" || got[1] != "b.parquet" || len(local) != 2 {
		t.Fatalf("pending output not excluded: files=%v local=%d", got, len(local))
	}
	src.SetPendingOutputs(func(context.Context, string) (map[string]struct{}, error) { return nil, errors.New("boom") })
	if _, _, err := src.FilesAndLocal(ctx, m); err == nil {
		t.Fatal("a failing lookup must fail the measurement, not export a possibly duplicated set")
	}
}

// TestFilesAndLocal_ReadsCompactionStateBeforeStat (regression, #638 ordering):
// the lookup runs after the listing and before the files are stat'ed. A
// compaction that commits in between deletes its sources before its manifest,
// so the lookup returns nothing and the stat loop drops the vanished sources:
// the set holds only the output. Reading the state after the stats would keep
// the (already registered) sources next to the output.
func TestFilesAndLocal_ReadsCompactionStateBeforeStat(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	raws := []string{"mydb/cpu/2026/07/14/15/a.parquet", "mydb/cpu/2026/07/14/15/b.parquet"}
	out := "mydb/cpu/2026/07/14/15/cpu_x_b1_compacted.parquet"
	for _, k := range append(raws, out) {
		if err := backend.Write(ctx, k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	src.SetPendingOutputs(func(ctx context.Context, _ string) (map[string]struct{}, error) {
		// The compaction commits right here: sources gone, manifest gone.
		for _, k := range raws {
			if err := backend.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
		return nil, nil
	})
	files, _, err := src.FilesAndLocal(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(files); len(got) != 1 || got[0] != filepath.Base(out) {
		t.Fatalf("files after a commit between listing and stat = %v, want only the compacted output", got)
	}
}

// TestFilesAndLocal_ReadsCompactionStateAfterListing (regression, #638
// ordering, other half): the lookup runs after the listing. An output uploaded
// while the lookup runs is not in the listing and cannot be registered this
// pass; a lookup that ran before the listing would list it.
func TestFilesAndLocal_ReadsCompactionStateAfterListing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"mydb/cpu/2026/07/14/15/a.parquet", "mydb/cpu/2026/07/14/15/b.parquet"} {
		if err := backend.Write(ctx, k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	src.SetPendingOutputs(func(ctx context.Context, _ string) (map[string]struct{}, error) {
		// The upload lands right here, after the listing: manifest written
		// earlier would have been read now, but this probe returns nothing to
		// show the listing alone keeps the output out of this pass.
		if err := backend.Write(ctx, "mydb/cpu/2026/07/14/15/cpu_x_b1_compacted.parquet", []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
		return nil, nil
	})
	files, _, err := src.FilesAndLocal(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(files); len(got) != 2 {
		t.Fatalf("files = %v, want the two raws only: the compaction state must be read after the listing", got)
	}
}

// TestCompactionStateDirMatchesManifestBasePath pins the literal this package
// keeps so it does not import internal/compaction on the production path.
func TestCompactionStateDirMatchesManifestBasePath(t *testing.T) {
	if compactionStateDir != compaction.ManifestBasePath {
		t.Fatalf("compactionStateDir = %q, compaction.ManifestBasePath = %q", compactionStateDir, compaction.ManifestBasePath)
	}
}

// TestScheduler_CompactionWindowNeverDuplicates (regression, #638): across a
// simulated compaction — raws only; output uploaded while its manifest is
// pending; sources deleted and manifest gone — no committed snapshot ever holds
// both a raw and the compacted output. On main the middle pass registers both.
func TestScheduler_CompactionWindowNeverDuplicates(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1_700_000_000_000_000)
	rawA := "mydb/cpu/2023/11/14/22/cpu_a.parquet"
	rawB := "mydb/cpu/2023/11/14/22/cpu_b.parquet"
	out := "mydb/cpu/2023/11/14/22/cpu_20231114_220000_1_b1_compacted.parquet"
	writeArcStyleParquet(t, filepath.Join(root, rawA), base, 50)
	writeArcStyleParquet(t, filepath.Join(root, rawB), base+60_000_000, 50)

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	pending := map[string]struct{}{}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	src.SetPendingOutputs(func(context.Context, string) (map[string]struct{}, error) { return pending, nil })
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})

	liveNames := func() []string {
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

	sched.runPass(ctx) // 1: raws only
	if got := liveNames(); len(got) != 2 {
		t.Fatalf("pass 1 live = %v, want the two raws", got)
	}
	// 2: the window — output uploaded, manifest pending, sources still there.
	writeArcStyleParquet(t, filepath.Join(root, out), base, 100)
	pending[out] = struct{}{}
	sched.runPass(ctx)
	if got := liveNames(); len(got) != 2 || got[0] != "cpu_a.parquet" {
		t.Fatalf("pass 2 (mid-compaction) live = %v, want the two raws only", got)
	}
	// 3: committed — sources deleted, manifest gone.
	for _, k := range []string{rawA, rawB} {
		if err := os.Remove(filepath.Join(root, k)); err != nil {
			t.Fatal(err)
		}
	}
	delete(pending, out)
	sched.runPass(ctx)
	if got := liveNames(); len(got) != 1 || got[0] != filepath.Base(out) {
		t.Fatalf("pass 3 live = %v, want the compacted output only", got)
	}
	// No snapshot in the table's history may hold a raw and the output together.
	lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatal(err)
	}
	for _, snap := range lt.Metadata().Snapshots() {
		tasks, err := lt.Scan(icetable.WithSnapshotID(snap.SnapshotID)).PlanFiles(ctx)
		if err != nil {
			t.Fatalf("snapshot %d: %v", snap.SnapshotID, err)
		}
		hasRaw, hasOut := false, false
		for _, task := range tasks {
			switch filepath.Base(task.File.FilePath()) {
			case "cpu_a.parquet", "cpu_b.parquet":
				hasRaw = true
			case filepath.Base(out):
				hasOut = true
			}
		}
		if hasRaw && hasOut {
			t.Fatalf("snapshot %d holds both a raw and the compacted output: external readers would double count", snap.SnapshotID)
		}
	}
}
