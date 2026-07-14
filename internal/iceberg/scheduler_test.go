package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// TestStorageWalkSourceAndReconcile is the Phase-2 integration proof: lay out Arc-style
// Parquet files under a real LocalBackend, then drive the reconciler through the
// FileSetSource (storage walk) — no tiering, no cluster — and assert the Iceberg table
// converges to the on-disk file set, then re-converges after a file is added/removed.
func TestStorageWalkSourceAndReconcile(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	// Arc layout: {db}/{measurement}/{Y}/{M}/{D}/{H}/{file}.parquet, all within one UTC day.
	base := int64(1_700_000_000_000_000)
	relH14 := "mydb/cpu/2023/11/14/22/cpu_a.parquet"
	relH15 := "mydb/cpu/2023/11/14/23/cpu_b.parquet"
	writeArcStyleParquet(t, filepath.Join(root, relH14), base, 100)
	writeArcStyleParquet(t, filepath.Join(root, relH15), base+3600_000_000, 100)

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	exp, err := NewExporter(db, backend, "file://"+root, "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := NewStorageWalkSource(backend, "arc")
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})

	// Enumerate: should find exactly mydb/cpu.
	ms, err := src.Measurements(ctx)
	if err != nil {
		t.Fatalf("Measurements: %v", err)
	}
	if len(ms) != 1 || ms[0].Database != "mydb" || ms[0].Measurement != "cpu" {
		t.Fatalf("Measurements = %+v, want [mydb/cpu]", ms)
	}

	// One reconcile pass (gate nil = runs).
	sched.runPass(ctx)

	assertTableFiles := func(want ...string) {
		t.Helper()
		lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
		if err != nil {
			t.Fatalf("load table: %v", err)
		}
		have, err := exp.tableDataFiles(ctx, lt)
		if err != nil {
			t.Fatalf("tableDataFiles: %v", err)
		}
		if len(have) != len(want) {
			t.Fatalf("table has %d files, want %d: %v", len(have), len(want), have)
		}
		for _, w := range want {
			if _, ok := have["file://"+filepath.Join(root, w)]; !ok {
				t.Errorf("table missing expected file %q; have %v", w, have)
			}
		}
	}
	assertTableFiles(relH14, relH15)

	// Add a third file, reconcile → table grows to 3.
	relH16 := "mydb/cpu/2023/11/14/21/cpu_c.parquet"
	writeArcStyleParquet(t, filepath.Join(root, relH16), base+7200_000_000, 50)
	sched.runPass(ctx)
	assertTableFiles(relH14, relH15, relH16)

	// Remove one file (simulate retention/compaction), reconcile → table shrinks to 2.
	if err := os.Remove(filepath.Join(root, relH14)); err != nil {
		t.Fatal(err)
	}
	sched.runPass(ctx)
	assertTableFiles(relH15, relH16)

	// version-hint.text must exist in the table's metadata dir with the current version int,
	// so directory-based readers (Spark hadoop-format load) can discover current metadata.
	hint := filepath.Join(root, "arc_mydb.db", "cpu", "metadata", "version-hint.text")
	data, err := os.ReadFile(hint)
	if err != nil {
		t.Fatalf("version-hint.text not written: %v", err)
	}
	v := strings.TrimSpace(string(data))
	if n, err := strconv.Atoi(v); err != nil || n < 1 {
		t.Fatalf("version-hint.text content = %q, want a positive integer", v)
	}
	// It must match the current metadata file's version prefix.
	lt, _ := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	wantPrefix := fmt.Sprintf("%05d-", mustAtoi(t, v))
	if base := filepath.Base(strings.TrimPrefix(lt.MetadataLocation(), "file://")); !strings.HasPrefix(base, wantPrefix) {
		t.Errorf("version-hint %q does not match current metadata %q", v, base)
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestScheduler_IncrementalSkip verifies the deferred optimization: a second pass over an
// UNCHANGED file set does no work — no new Iceberg snapshot — because the fingerprint matches.
func TestScheduler_IncrementalSkip(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 50)

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc"), Logger: zerolog.Nop()})

	snapOf := func() int64 {
		lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if snap := lt.CurrentSnapshot(); snap != nil {
			return snap.SnapshotID
		}
		return 0
	}

	sched.runPass(ctx) // first pass: creates + reconciles
	s1 := snapOf()
	if s1 == 0 {
		t.Fatal("expected a snapshot after first pass")
	}

	// Second pass, nothing changed on disk → fingerprint hit → NO new snapshot.
	sched.runPass(ctx)
	if s2 := snapOf(); s2 != s1 {
		t.Errorf("unchanged pass created a new snapshot (%d -> %d) — incremental skip failed", s1, s2)
	}

	// Add a file → fingerprint changes → a reconcile happens (new snapshot).
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/23/b.parquet"), 1_700_000_003_600_000, 50)
	sched.runPass(ctx)
	if s3 := snapOf(); s3 == s1 {
		t.Errorf("adding a file did not produce a new snapshot (still %d) — change not detected", s1)
	}
}

// TestScheduler_IncrementalSchemaEvolution proves the incremental schema-derivation path picks
// up a column that appears only in a newly-added file. The first pass caches the narrow (3-col)
// schema; the second pass adds a wide (4-col) file, so reconcileOne reads ONLY the new file's
// footer and merges it — the table schema must gain the new column without a full re-derivation.
func TestScheduler_IncrementalSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1_700_000_000_000_000)
	// Narrow file first: time, host, value.
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), base, 20)

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc"), Logger: zerolog.Nop()})

	sched.runPass(ctx) // caches narrow schema
	lt, _ := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if _, ok := lt.Schema().FindFieldByName("cpu_idle"); ok {
		t.Fatal("cpu_idle present before the wide file was added")
	}

	// Add a WIDE file (has the extra cpu_idle column). Incremental path reads only this footer.
	writeArcStyleParquet4Col(t, filepath.Join(root, "mydb/cpu/2023/11/14/23/b.parquet"), base+3_600_000_000, 20)
	sched.runPass(ctx)

	lt, _ = exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if _, ok := lt.Schema().FindFieldByName("cpu_idle"); !ok {
		t.Fatalf("incremental schema derivation missed the new column cpu_idle: %v", lt.Schema())
	}
	files, _ := exp.tableDataFiles(ctx, lt)
	if len(files) != 2 {
		t.Fatalf("want 2 files after evolution, got %d", len(files))
	}
}

// TestUnionSchema_ParallelDeterministic proves the parallelized UnionSchema still folds columns
// in first-seen order regardless of which footer read finishes first, and surfaces a per-file
// error. Uses a mix of narrow (3-col) and wide (4-col) files so cpu_idle must land last.
func TestUnionSchema_ParallelDeterministic(t *testing.T) {
	dir := t.TempDir()
	base := int64(1_700_000_000_000_000)
	var paths []string
	// Interleave narrow/wide so the extra column comes from files at varying positions.
	for i := 0; i < 12; i++ {
		p := filepath.Join(dir, fmt.Sprintf("f%02d.parquet", i))
		if i%3 == 0 {
			writeArcStyleParquet4Col(t, p, base+int64(i)*1000, 5) // time, host, value, cpu_idle
		} else {
			writeArcStyleParquet(t, p, base+int64(i)*1000, 5) // time, host, value
		}
		paths = append(paths, p)
	}

	sc, err := UnionSchema(paths)
	if err != nil {
		t.Fatalf("UnionSchema: %v", err)
	}
	got := make([]string, len(sc.Fields))
	for i, f := range sc.Fields {
		got[i] = f.Name
	}
	// First-seen order: the first file (i=0) is wide (time, host, value, cpu_idle).
	want := []string{"time", "host", "value", "cpu_idle"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("union column order = %v, want %v", got, want)
	}

	// A missing/unreadable file must produce an error, not a silent partial schema.
	if _, err := UnionSchema(append(paths, filepath.Join(dir, "does-not-exist.parquet"))); err == nil {
		t.Error("expected an error for an unreadable file, got nil")
	}
}

func TestMergeSchemas(t *testing.T) {
	long := iceberg.PrimitiveTypes.Int64
	dbl := iceberg.PrimitiveTypes.Float64
	str := iceberg.PrimitiveTypes.String

	base := ArcSchema{Fields: []ArcField{{Name: "time", Type: long}, {Name: "host", Type: str}}}
	add := ArcSchema{Fields: []ArcField{{Name: "host", Type: str}, {Name: "value", Type: dbl}}}

	merged, err := MergeSchemas(base, add)
	if err != nil {
		t.Fatalf("MergeSchemas: %v", err)
	}
	// Order: base first-seen, then new names from add.
	gotNames := make([]string, len(merged.Fields))
	for i, f := range merged.Fields {
		gotNames[i] = f.Name
	}
	want := []string{"time", "host", "value"}
	if strings.Join(gotNames, ",") != strings.Join(want, ",") {
		t.Errorf("merged order = %v, want %v", gotNames, want)
	}

	// Conflicting type for the same name must error, not silently pick one.
	conflict := ArcSchema{Fields: []ArcField{{Name: "host", Type: long}}}
	if _, err := MergeSchemas(base, conflict); err == nil {
		t.Error("expected an error for a conflicting type on column 'host', got nil")
	}
}
