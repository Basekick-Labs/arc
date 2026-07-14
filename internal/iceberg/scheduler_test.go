package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
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

// TestScheduler_AllFilesDeletedEmptiesTable is the regression test for the orphaned-reference
// bug: when retention/compaction deletes EVERY file of a measurement, Arc's Delete only removes
// the file (os.Remove) — the {db}/{measurement}/… directory tree survives, so Measurements()
// keeps yielding the measurement and reconcileOne sees files=[] and localFiles=[]. Returning
// early there left the Iceberg table pointing at deleted files forever, and external engines
// fail on the missing paths. The table must be reconciled to EMPTY instead.
func TestScheduler_AllFilesDeletedEmptiesTable(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	rel := "mydb/cpu/2023/11/14/22/a.parquet"
	writeArcStyleParquet(t, filepath.Join(root, rel), 1_700_000_000_000_000, 20)

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

	sched.runPass(ctx)
	lt, _ := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if f, _ := exp.tableDataFiles(ctx, lt); len(f) != 1 {
		t.Fatalf("setup: want 1 file in table, got %d", len(f))
	}

	// Delete ALL data files, leaving the directory tree (what retention actually does).
	if err := os.Remove(filepath.Join(root, rel)); err != nil {
		t.Fatal(err)
	}
	// Precondition for the bug: the measurement is still enumerated.
	if ms, _ := NewStorageWalkSource(backend, "arc").Measurements(ctx); len(ms) != 1 {
		t.Fatalf("precondition: measurement should still be enumerated, got %v", ms)
	}

	sched.runPass(ctx)
	lt2, _ := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	files, err := exp.tableDataFiles(ctx, lt2)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("table still references %d deleted file(s) — orphaned refs; want 0: %v", len(files), files)
	}

	// The empty state must be fingerprint-cached: a further pass over the still-empty
	// measurement does no work (no repeated catalog commit / log every tick).
	emptied := lt2.CurrentSnapshot().SnapshotID
	sched.runPass(ctx)
	lt3, _ := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if got := lt3.CurrentSnapshot().SnapshotID; got != emptied {
		t.Errorf("a second pass over an already-empty measurement committed again (%d -> %d)", emptied, got)
	}

	// Re-ingest must recover: the cached empty state has no schema, so this falls through to a
	// full re-derivation and the file lands back in the table.
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/23/b.parquet"), 1_700_000_003_600_000, 20)
	sched.runPass(ctx)
	lt4, _ := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if f, _ := exp.tableDataFiles(ctx, lt4); len(f) != 1 {
		t.Errorf("re-ingest after empty-out: want 1 file back in the table, got %d", len(f))
	}
}

// TestScheduler_EmptyDirNeverCreatesTable guards the other half of the empty-out fix: a stray
// empty measurement directory that never held data must NOT mint a zero-column Iceberg table
// (EnsureTable with an empty schema would create one with no fields and no partition spec).
func TestScheduler_EmptyDirNeverCreatesTable(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// A measurement directory with no data files at all.
	if err := os.MkdirAll(filepath.Join(root, "mydb/ghost/2023/11/14/22"), 0o755); err != nil {
		t.Fatal(err)
	}

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

	sched.runPass(ctx)

	if exp.TableExists(ctx, "mydb", "ghost") {
		t.Error("an empty measurement directory created an Iceberg table; want none")
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

	sc, err := UnionSchema(context.Background(), paths)
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
	if _, err := UnionSchema(context.Background(), append(paths, filepath.Join(dir, "does-not-exist.parquet"))); err == nil {
		t.Error("expected an error for an unreadable file, got nil")
	}
}

// TestUnionSchema_ContextCancelled verifies the parallel footer reads honour cancellation
// rather than grinding through every remaining file after the caller has given up (the
// reconciler bounds each measurement with measurementTimeout).
func TestUnionSchema_ContextCancelled(t *testing.T) {
	dir := t.TempDir()
	base := int64(1_700_000_000_000_000)
	var paths []string
	for i := 0; i < 8; i++ {
		p := filepath.Join(dir, fmt.Sprintf("c%02d.parquet", i))
		writeArcStyleParquet(t, p, base+int64(i)*1000, 5)
		paths = append(paths, p)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the call
	if _, err := UnionSchema(ctx, paths); !errors.Is(err, context.Canceled) {
		t.Errorf("UnionSchema with a cancelled context: got err=%v, want context.Canceled", err)
	}
}

// TestEvolveSchema_TypeMismatchRejected guards against silent table corruption: a column whose
// type CHANGES for an existing table must fail the measurement, not register incompatible files.
// Neither schema-derivation path catches this — UnionSchema only compares the current pass's
// files against each other, and MergeSchemas compares against the in-memory cache (empty after a
// restart or an empty-out) — so evolveSchema, which sees the durable table schema, is the guard.
func TestEvolveSchema_TypeMismatchRejected(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := sql.Open("sqlite3", filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, nil, "file://"+dir+"/wh", "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	// Create the table with `value` as a long.
	longSc := ArcSchema{Fields: []ArcField{
		{Name: "time", Type: iceberg.PrimitiveTypes.TimestampTz},
		{Name: "value", Type: iceberg.PrimitiveTypes.Int64},
	}}
	if _, err := exp.EnsureTable(ctx, "mydb", "cpu", longSc); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Same column now arrives as a double (what a restart + re-ingest with changed types looks
	// like). Must be rejected rather than silently skipped.
	doubleSc := ArcSchema{Fields: []ArcField{
		{Name: "time", Type: iceberg.PrimitiveTypes.TimestampTz},
		{Name: "value", Type: iceberg.PrimitiveTypes.Float64},
	}}
	_, err = exp.EnsureTable(ctx, "mydb", "cpu", doubleSc)
	if err == nil {
		t.Fatal("expected a type-mismatch error when a column's type changes, got nil")
	}
	if !strings.Contains(err.Error(), "type mismatch") {
		t.Errorf("error should name the type mismatch, got: %v", err)
	}

	// An unchanged schema must still be a clean no-op (no false positives).
	if _, err := exp.EnsureTable(ctx, "mydb", "cpu", longSc); err != nil {
		t.Errorf("unchanged schema should be a no-op, got: %v", err)
	}
}

// TestArrowToIceberg covers the Arrow→Iceberg type mapping. Arc's own ingest only emits
// Int64/Float64/String/Boolean/Timestamp_us, but Parquet can also arrive via the bulk import
// path carrying externally-produced types (int32/float32/decimal), so those must map too
// rather than failing the measurement's whole export.
func TestArrowToIceberg(t *testing.T) {
	tests := []struct {
		name string
		in   arrow.DataType
		want iceberg.Type
	}{
		{"int64", arrow.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int64},
		{"int32", arrow.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int32},
		{"float64", arrow.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Float64},
		{"float32", arrow.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float32},
		{"string", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String},
		{"bool", arrow.FixedWidthTypes.Boolean, iceberg.PrimitiveTypes.Bool},
		// Arc's `time`: UTC-adjusted timestamp MUST become timestamptz, not timestamp.
		{"timestamp_utc", arrow.FixedWidthTypes.Timestamp_us, iceberg.PrimitiveTypes.TimestampTz},
		{"timestamp_naive", &arrow.TimestampType{Unit: arrow.Microsecond}, iceberg.PrimitiveTypes.Timestamp},
	}
	for _, tt := range tests {
		got, err := arrowToIceberg(tt.in)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tt.name, err)
			continue
		}
		if !got.Equals(tt.want) {
			t.Errorf("%s: got %s, want %s", tt.name, got, tt.want)
		}
	}

	// An genuinely unsupported type must error, not map to something wrong.
	if _, err := arrowToIceberg(arrow.ListOf(arrow.PrimitiveTypes.Int64)); err == nil {
		t.Error("expected an error for an unsupported Arrow type (list), got nil")
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

// TestCustomWarehouseSubdir covers iceberg.warehouse pointed at a SUBDIRECTORY of the storage
// root (a supported config: the key defaults to the storage root but operators can override it).
// backend Read/Write take keys relative to the STORAGE ROOT, not the warehouse — trimming the
// warehouse instead dropped its own path segment, so version-hint.text and the v<N> copies
// landed outside the warehouse and directory-based readers (DuckDB/Spark) could not resolve the
// current snapshot.
func TestCustomWarehouseSubdir(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 20)

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Warehouse is <root>/warehouse, NOT <root>. Built via localFileURI so it matches what
	// DefaultWarehouse produces (absolute + forward slashes) on every platform.
	exp, err := NewExporter(db, backend, localFileURI(filepath.Join(root, "warehouse")), "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc"), Logger: zerolog.Nop()})
	sched.runPass(ctx)

	// version-hint.text must live INSIDE the configured warehouse.
	hint := filepath.Join(root, "warehouse", "arc_mydb.db", "cpu", "metadata", "version-hint.text")
	if _, err := os.Stat(hint); err != nil {
		t.Fatalf("version-hint.text not inside the configured warehouse: %v", err)
	}
	// ...and must NOT have leaked to the storage root (the pre-fix behaviour).
	if _, err := os.Stat(filepath.Join(root, "arc_mydb.db", "cpu", "metadata", "version-hint.text")); err == nil {
		t.Error("version-hint.text leaked to the storage root, outside the configured warehouse")
	}

	// The table must still be readable/registered (the file made it into the table).
	lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatal(err)
	}
	if f, _ := exp.tableDataFiles(ctx, lt); len(f) != 1 {
		t.Errorf("want 1 file registered with a custom warehouse, got %d", len(f))
	}
}

// TestIsUnderDir pins the path-boundary matching that warehouseRelKey's gate depends on. A bare
// strings.HasPrefix accepts a sibling directory whose name merely starts with the warehouse's
// ("file:///data/wh" vs "file:///data/wh-other"), which would let another warehouse's metadata
// be treated as ours — and pruneOldVersionFiles deletes files under the derived key.
func TestIsUnderDir(t *testing.T) {
	const dir = "file:///data/wh"
	tests := []struct {
		p    string
		want bool
	}{
		{"file:///data/wh", true}, // the dir itself
		{"file:///data/wh/arc_db.db/cpu/metadata/v1.metadata.json", true}, // beneath it
		{"file:///data/wh/", true},                                        // trailing slash
		// Siblings that share a name prefix must NOT match.
		{"file:///data/wh-other/arc_db.db/cpu/metadata/v1.metadata.json", false},
		{"file:///data/wharf/x.json", false},
		{"file:///data/whx", false},
		// Unrelated / parent paths.
		{"file:///data/other/x.json", false},
		{"file:///data", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isUnderDir(tt.p, dir); got != tt.want {
			t.Errorf("isUnderDir(%q, %q) = %v, want %v", tt.p, dir, got, tt.want)
		}
	}
	// A dir configured WITH a trailing slash must behave identically.
	if !isUnderDir("file:///data/wh/a.json", "file:///data/wh/") {
		t.Error("isUnderDir should normalize a trailing slash on dir")
	}
}
