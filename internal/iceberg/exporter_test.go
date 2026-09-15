package iceberg

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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

	tbl := func() map[string]int64 {
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
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, f1)}); err != nil {
		t.Fatalf("reconcile add f1: %v", err)
	}
	files := tbl()
	if _, ok := files[fileURI(f1)]; !ok || len(files) != 1 {
		t.Fatalf("after add f1: files=%v, want just f1", files)
	}
	snapAfterAdd := currentSnapshotID(ctx, t, exp)

	// 2. reconcile same set -> no-op, no new snapshot
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, f1)}); err != nil {
		t.Fatalf("reconcile idempotent: %v", err)
	}
	if got := currentSnapshotID(ctx, t, exp); got != snapAfterAdd {
		t.Errorf("idempotent reconcile created a new snapshot (%d -> %d)", snapAfterAdd, got)
	}

	// 3. supersede: f1 gone, f2 present (hourly -> daily)
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, f2)}); err != nil {
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

// writeStraddlingParquet writes a file whose time values span MORE than one UTC day (first
// row at baseTS, last row +2 days), so iceberg-go's day() partition inference cannot map it to
// a single partition value. Exercises the B2 resilient path.
func writeStraddlingParquet(t *testing.T, path string, baseTS int64) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	const twoDaysUs = int64(2 * 24 * 60 * 60 * 1_000_000)
	b.Field(0).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{arrow.Timestamp(baseTS), arrow.Timestamp(baseTS + twoDaysUs)}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"h", "h"}, nil)
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1, 2}, nil)
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

// TestReconcile_DayStraddlingFileSkipped verifies B2: a file whose time range spans >1 day
// cannot be day()-partitioned and would otherwise wedge the measurement forever. The resilient
// path must skip ONLY the straddling file and still export the good one.
func TestReconcile_DayStraddlingFileSkipped(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	good := filepath.Join(dir, "good.parquet")
	bad := filepath.Join(dir, "straddle.parquet")
	base := int64(1_700_000_000_000_000)
	writeArcStyleParquet(t, good, base, 10) // single-day
	writeStraddlingParquet(t, bad, base)    // spans 2 days

	db, err := sql.Open("sqlite3", filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, nil, "file://"+dir+"/wh", "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sc, _ := SchemaFromParquet(good)

	// Reconcile both files. The straddling one must be skipped, the good one exported —
	// and crucially this must NOT return an error (the measurement is not wedged).
	if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc,
		[]FileRef{refOf(t, good), refOf(t, bad)}); err != nil {
		t.Fatalf("reconcile with straddling file returned error (should skip, not fail): %v", err)
	}
	lt, _ := exp.EnsureTable(ctx, "db", "cpu", sc)
	files, _ := exp.tableDataFiles(ctx, lt)
	if _, ok := files[fileURI(good)]; !ok {
		t.Errorf("good file not exported: %v", files)
	}
	if _, ok := files[fileURI(bad)]; ok {
		t.Errorf("straddling file should have been skipped but is in the table: %v", files)
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
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", narrowSc, []FileRef{refOf(t, narrow)}); err != nil {
		t.Fatalf("reconcile narrow: %v", err)
	}

	// 2. Now reconcile with the UNION schema + BOTH files (the wide one has cpu_idle).
	unionSc, err := UnionSchema(ctx, []string{narrow, wide})
	if err != nil {
		t.Fatalf("UnionSchema: %v", err)
	}
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", unionSc,
		[]FileRef{refOf(t, narrow), refOf(t, wide)}); err != nil {
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
		if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc, []FileRef{refOf(t, f)}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	// Snapshot count must be capped at retain, not 8+.
	lt, _ := exp.EnsureTable(ctx, "db", "cpu", sc)
	if n := len(lt.Metadata().Snapshots()); n > retain+1 {
		t.Errorf("snapshot count = %d, want <= %d (retain=%d)", n, retain+1, retain)
	}

	// Old v<M>.metadata.json copies must be pruned. The bound is retain+1, not
	// retain: one extra version is kept so a directory reader that resolved
	// version-hint.text just before a commit can still open the version it read
	// (see pruneOldVersionFiles).
	metaDir := filepath.Join(root, "arc_db.db", "cpu", "metadata")
	entries, _ := os.ReadDir(metaDir)
	var vFiles []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "v") && strings.HasSuffix(e.Name(), ".metadata.json") {
			vFiles = append(vFiles, e.Name())
		}
	}
	if len(vFiles) > retain+1 {
		t.Errorf("v<N>.metadata.json count = %d (%v), want <= %d after pruning", len(vFiles), vFiles, retain+1)
	}
	// The bound must still be a bound — 8 writes must not leave 8 copies.
	if len(vFiles) >= 8 {
		t.Errorf("v<N>.metadata.json count = %d (%v): pruning is not bounding growth", len(vFiles), vFiles)
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

// TestExpireSnapshotsAfterArcDeletedSupersededFiles reproduces the production
// ordering that #632 exposed: Arc deletes a superseded Parquet file from disk
// (compaction, retention, the delete API) and only afterwards does the exporter
// expire the snapshots that still reference it.
//
// iceberg-go's ExpireSnapshots runs orphan deletion as a POST-COMMIT hook, so
// the catalog commit lands first and the hook then tries to os.Remove files Arc
// has already removed. With WithPostCommit at its default (true), every one of
// those ENOENTs is joined into the error returned by Commit — from a commit
// that actually succeeded. The exporter read that as failure, logged "snapshot
// history grows until it recovers", and returned the PRE-expire table, so
// version-hint.text published metadata listing snapshots whose manifest-list
// files the hook had just deleted.
//
// TestExpireSnapshotsAndPruneVersions above cannot catch this: it leaves every
// file on disk, so the hook always succeeds. The deletion below is the whole
// point of this test.
func TestExpireSnapshotsAfterArcDeletedSupersededFiles(t *testing.T) {
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

	const retain = 2
	// Capture exporter logs: the false "commit failed" ERROR is the
	// operator-visible half of #632, and it fires forever once history churns.
	var logBuf bytes.Buffer
	exp, err := NewExporter(db, backend, "file://"+root, "arc", retain, zerolog.New(&logBuf))
	if err != nil {
		t.Fatal(err)
	}

	base := int64(1_700_000_000_000_000)
	var sc ArcSchema

	// Each pass registers a fresh file as the table's only content, which
	// supersedes the previous one, and then deletes the superseded file from
	// disk exactly as compaction would.
	var prev string
	for i := 0; i < 6; i++ {
		f := filepath.Join(root, "db", "cpu", "2023", "11", "14", "22", fmt.Sprintf("f%d.parquet", i))
		writeArcStyleParquet(t, f, base+int64(i)*1000, 5)
		if i == 0 {
			sc, _ = SchemaFromParquet(f)
		}
		if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc, []FileRef{refOf(t, f)}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
		// Arc owns the data-file lifecycle: the superseded file is gone from
		// storage before the next pass expires the snapshot that referenced it.
		if prev != "" {
			if err := os.Remove(prev); err != nil {
				t.Fatalf("removing superseded file %s: %v", prev, err)
			}
		}
		prev = f
	}

	// A commit that succeeded must not be reported as failed. Before the fix
	// this fired on every pass after history exceeded `retain`, masking genuine
	// expiry failures behind permanent noise.
	if logs := logBuf.String(); strings.Contains(logs, "ExpireSnapshots commit failed") {
		t.Errorf("expiry logged a false commit failure (#632):\n%s", logs)
	}

	lt, err := exp.EnsureTable(ctx, "db", "cpu", sc)
	if err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}

	// Expiry must actually cap history. Before the fix the exporter returned
	// the pre-expire table on every pass, so this grew without bound.
	if n := len(lt.Metadata().Snapshots()); n > retain+1 {
		t.Errorf("snapshot count = %d, want <= %d: expiry is not being applied (#632)", n, retain+1)
	}

	// version-hint.text must name a metadata file that exists, and that
	// metadata must not reference a manifest list the post-commit hook deleted.
	metaDir := filepath.Join(root, "arc_db.db", "cpu", "metadata")
	hint, err := os.ReadFile(filepath.Join(metaDir, "version-hint.text"))
	if err != nil {
		t.Fatalf("version-hint.text missing: %v", err)
	}
	hv := strings.TrimSpace(string(hint))
	hintedMeta := filepath.Join(metaDir, "v"+hv+".metadata.json")
	if _, err := os.Stat(hintedMeta); err != nil {
		t.Fatalf("version-hint points at v%s but that metadata is missing: %v", hv, err)
	}

	// Every snapshot the published metadata still advertises must have its
	// manifest list on disk. A dangling entry here is the reader-visible
	// symptom of #632: directory readers doing snapshot listing or time travel
	// resolve the hint, then fail on a missing manifest list.
	raw, err := os.ReadFile(hintedMeta)
	if err != nil {
		t.Fatalf("reading hinted metadata: %v", err)
	}
	var meta struct {
		Snapshots []struct {
			SnapshotID   int64  `json:"snapshot-id"`
			ManifestList string `json:"manifest-list"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("parsing hinted metadata: %v", err)
	}
	for _, sn := range meta.Snapshots {
		p := strings.TrimPrefix(sn.ManifestList, "file://")
		if _, err := os.Stat(p); err != nil {
			t.Errorf("published metadata advertises snapshot %d whose manifest list is missing: %s (#632)", sn.SnapshotID, p)
		}
	}
}
