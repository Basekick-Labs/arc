package iceberg

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"

	"database/sql"
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

	exp, err := NewExporter(db, "file://"+dir+"/warehouse", "arc", zerolog.Nop())
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
