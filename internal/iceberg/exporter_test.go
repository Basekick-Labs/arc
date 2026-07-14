package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// writeArcStyleParquet writes a Parquet file matching Arc's ingest output: time as
// Timestamp_us(UTC), a string tag, a float64 metric — NO PARQUET:field_id (like Arc).
func writeArcStyleParquet(t *testing.T, path string, baseTS int64, n int) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	tsb := b.Field(0).(*array.TimestampBuilder)
	hb := b.Field(1).(*array.StringBuilder)
	vb := b.Field(2).(*array.Float64Builder)
	for i := 0; i < n; i++ {
		tsb.Append(arrow.Timestamp(baseTS + int64(i)*1_000_000)) // 1s apart, within one day
		hb.Append("host-1")
		vb.Append(float64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := pqarrow.NewFileWriter(schema, f,
		parquet.NewWriterProperties(parquet.WithCompression(0)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func fileURI(p string) string { return "file://" + p }

// TestReconcile_AddIdempotentSupersede exercises the core reconciler contract end-to-end
// against a real SQLite catalog + real Parquet fixtures:
//  1. reconcile [f1] -> table has f1
//  2. reconcile [f1] again -> NO new snapshot (idempotent)
//  3. reconcile [f2] (f1 gone, f2 new, i.e. hourly->daily supersession) -> table has only f2
func TestReconcile_AddIdempotentSupersede(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	f1 := filepath.Join(dir, "cpu_h1.parquet")
	f2 := filepath.Join(dir, "cpu_daily.parquet")
	baseTS := int64(1_700_000_000_000_000) // 2023-11-14, within one UTC day
	writeArcStyleParquet(t, f1, baseTS, 100)
	writeArcStyleParquet(t, f2, baseTS, 500)

	db, err := sql.Open("sqlite3", filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	exp, err := NewExporter(db, nil, "file://"+dir+"/warehouse", "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}

	sc, err := SchemaFromParquet(f1)
	if err != nil {
		t.Fatalf("SchemaFromParquet: %v", err)
	}
	// Sanity: time must have mapped to timestamptz (the Phase-0b gotcha).
	if got := sc.Fields[0]; got.Name != "time" || got.Type.String() != "timestamptz" {
		t.Fatalf("time field = %+v, want timestamptz", got)
	}

	tbl := func() map[string]struct{} {
		lt, err := exp.EnsureTable(ctx, "mydb", "cpu", sc)
		if err != nil {
			t.Fatalf("EnsureTable: %v", err)
		}
		files, err := exp.tableDataFiles(ctx, lt)
		if err != nil {
			t.Fatalf("tableDataFiles: %v", err)
		}
		return files
	}

	// 1. add f1
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{{PhysicalPath: fileURI(f1)}}); err != nil {
		t.Fatalf("reconcile add f1: %v", err)
	}
	files := tbl()
	if _, ok := files[fileURI(f1)]; !ok || len(files) != 1 {
		t.Fatalf("after add f1: files=%v, want just f1", files)
	}
	snapAfterAdd := currentSnapshotID(ctx, t, exp)

	// 2. reconcile same set -> no-op, no new snapshot
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{{PhysicalPath: fileURI(f1)}}); err != nil {
		t.Fatalf("reconcile idempotent: %v", err)
	}
	if got := currentSnapshotID(ctx, t, exp); got != snapAfterAdd {
		t.Errorf("idempotent reconcile created a new snapshot (%d -> %d)", snapAfterAdd, got)
	}

	// 3. supersede: f1 gone, f2 present (hourly -> daily)
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{{PhysicalPath: fileURI(f2)}}); err != nil {
		t.Fatalf("reconcile supersede: %v", err)
	}
	files = tbl()
	if _, ok := files[fileURI(f2)]; !ok {
		t.Fatalf("after supersede: f2 missing, files=%v", files)
	}
	if _, ok := files[fileURI(f1)]; ok {
		t.Fatalf("after supersede: f1 still present, files=%v", files)
	}
	if len(files) != 1 {
		t.Fatalf("after supersede: want exactly 1 file, got %v", files)
	}
}

// writeArcStyleParquet4Col writes a file with an extra column (cpu_idle) to exercise schema
// evolution — matches what Arc's schema-flexible ingest produces when a metric is added.
func writeArcStyleParquet4Col(t *testing.T, path string, baseTS int64, n int) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "cpu_idle", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(baseTS))
	b.Field(1).(*array.StringBuilder).Append("h")
	b.Field(2).(*array.Float64Builder).Append(1)
	b.Field(3).(*array.Float64Builder).Append(2)
	rec := b.NewRecord()
	defer rec.Release()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Create(path)
	defer f.Close()
	w, _ := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithCompression(0)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	w.Close()
}

// TestReconcile_SchemaEvolution reproduces the bug the running binary caught: a narrow file
// (3 cols) creates the table, then a wider file (4 cols, +cpu_idle) is reconciled. Without
// schema evolution, AddFiles fails "field missing from name mapping: cpu_idle". With union
// schema + evolveSchema, the table widens and both files register.
func TestReconcile_SchemaEvolution(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	narrow := filepath.Join(dir, "cpu_narrow.parquet")
	wide := filepath.Join(dir, "cpu_wide.parquet")
	base := int64(1_700_000_000_000_000)
	writeArcStyleParquet(t, narrow, base, 10)        // time, host, value
	writeArcStyleParquet4Col(t, wide, base+1000, 10) // + cpu_idle

	db, err := sql.Open("sqlite3", filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, nil, "file://"+dir+"/wh", "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	// 1. Create the table from the NARROW schema (first file only).
	narrowSc, _ := SchemaFromParquet(narrow)
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", narrowSc, []FileRef{{PhysicalPath: fileURI(narrow)}}); err != nil {
		t.Fatalf("reconcile narrow: %v", err)
	}

	// 2. Now reconcile with the UNION schema + BOTH files (the wide one has cpu_idle).
	unionSc, err := UnionSchema([]string{narrow, wide})
	if err != nil {
		t.Fatalf("UnionSchema: %v", err)
	}
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", unionSc,
		[]FileRef{{PhysicalPath: fileURI(narrow)}, {PhysicalPath: fileURI(wide)}}); err != nil {
		t.Fatalf("reconcile wide (schema evolution): %v", err)
	}

	// Both files must be in the table, and the schema must now include cpu_idle.
	lt, _ := exp.EnsureTable(ctx, "mydb", "cpu", unionSc)
	files, _ := exp.tableDataFiles(ctx, lt)
	if len(files) != 2 {
		t.Fatalf("want 2 files after evolution, got %d: %v", len(files), files)
	}
	if _, ok := lt.Schema().FindFieldByName("cpu_idle"); !ok {
		t.Fatalf("table schema missing cpu_idle after evolution: %v", lt.Schema())
	}
}

// TestExpireSnapshotsAndPruneVersions verifies that with retain=N, snapshot history is capped
// and our v<M>.metadata.json copies below the retained window are pruned.
func TestExpireSnapshotsAndPruneVersions(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const retain = 3
	exp, err := NewExporter(db, backend, "file://"+root, "arc", retain, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	base := int64(1_700_000_000_000_000)

	// Create 8 distinct single-file snapshots. Derive the schema the SAME way the reconciler
	// does (from a Parquet file) so no spurious schema-evolution commit occurs on load.
	var sc ArcSchema
	for i := 0; i < 8; i++ {
		f := filepath.Join(root, "db", "cpu", "2023", "11", "14", "22", fmt.Sprintf("f%d.parquet", i))
		writeArcStyleParquet(t, f, base+int64(i)*1000, 5)
		if i == 0 {
			sc, _ = SchemaFromParquet(f)
		}
		if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc, []FileRef{{PhysicalPath: fileURI(f)}}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	// Snapshot count must be capped at retain, not 8+.
	lt, _ := exp.EnsureTable(ctx, "db", "cpu", sc)
	if n := len(lt.Metadata().Snapshots()); n > retain+1 {
		t.Errorf("snapshot count = %d, want <= %d (retain=%d)", n, retain+1, retain)
	}

	// Old v<M>.metadata.json copies must be pruned to the newest `retain`.
	metaDir := filepath.Join(root, "arc_db.db", "cpu", "metadata")
	entries, _ := os.ReadDir(metaDir)
	var vFiles []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "v") && strings.HasSuffix(e.Name(), ".metadata.json") {
			vFiles = append(vFiles, e.Name())
		}
	}
	if len(vFiles) > retain {
		t.Errorf("v<N>.metadata.json count = %d (%v), want <= %d after pruning", len(vFiles), vFiles, retain)
	}
	// version-hint.text must point at an existing v<N>.metadata.json (readers rely on it).
	hint, err := os.ReadFile(filepath.Join(metaDir, "version-hint.text"))
	if err != nil {
		t.Fatalf("version-hint.text missing: %v", err)
	}
	hv := strings.TrimSpace(string(hint))
	if _, err := os.Stat(filepath.Join(metaDir, "v"+hv+".metadata.json")); err != nil {
		t.Errorf("version-hint points at v%s but v%s.metadata.json missing: %v (have %v)", hv, hv, err, vFiles)
	}
}

func currentSnapshotID(ctx context.Context, t *testing.T, exp *Exporter) int64 {
	t.Helper()
	lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatalf("load for snapshot id: %v", err)
	}
	snap := lt.CurrentSnapshot()
	if snap == nil {
		return 0
	}
	return snap.SnapshotID
}
