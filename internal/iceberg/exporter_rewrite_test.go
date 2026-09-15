package iceberg

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"
)

// refOf builds the FileRef the storage-walk source would produce for a local file: URI plus
// on-disk size. Tests that reconcile fixtures directly must carry the size too, or the
// size-aware diff sees 0 ≠ manifest size and re-registers the file on every pass.
func refOf(t *testing.T, path string) FileRef {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return FileRef{PhysicalPath: fileURI(path), SizeBytes: st.Size()}
}

// rewriteInPlace mirrors the delete API's partial-match branch (internal/api/delete.go
// rewriteLocalFile): the surviving rows go to <dir>/.tmp/<name>.new, then os.Rename over the
// original path. Same path, fewer rows, new size.
func rewriteInPlace(t *testing.T, path string, baseTS int64, n int) {
	t.Helper()
	tmp := filepath.Join(filepath.Dir(path), ".tmp", filepath.Base(path)+".new")
	writeArcStyleParquet(t, tmp, baseTS, n)
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

type liveEntry struct {
	count, size int64
}

// liveEntries returns what an external engine sees in the table's current snapshot: the manifest
// entry's record_count and file_size_in_bytes per live data-file path.
func liveEntries(ctx context.Context, t *testing.T, exp *Exporter, database, measurement string) map[string]liveEntry {
	t.Helper()
	lt, err := exp.EnsureTable(ctx, database, measurement, ArcSchema{})
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	out := map[string]liveEntry{}
	if lt.CurrentSnapshot() == nil {
		return out
	}
	tasks, err := lt.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatalf("PlanFiles: %v", err)
	}
	for _, task := range tasks {
		out[task.File.FilePath()] = liveEntry{count: task.File.Count(), size: task.File.FileSizeBytes()}
	}
	return out
}

func newTestExporter(t *testing.T, dir string, retain int) *Exporter {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, nil, "file://"+dir+"/warehouse", "arc", retain, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	return exp
}

// TestReconcile_InPlaceRewriteReregisters is the #633 contract: a file rewritten in place
// (same path, fewer rows) must be re-registered so the manifest's record_count and
// file_size_in_bytes follow the rewrite, in one commit, and the table must still be able
// to drop that path later (the re-add must not wedge subsequent removals).
func TestReconcile_InPlaceRewriteReregisters(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	f1 := filepath.Join(dir, "data", "cpu_h1.parquet")
	baseTS := int64(1_700_000_000_000_000)
	writeArcStyleParquet(t, f1, baseTS, 100)
	exp := newTestExporter(t, dir, 0)
	sc, err := SchemaFromParquet(f1)
	if err != nil {
		t.Fatal(err)
	}

	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, f1)}); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	before := liveEntries(ctx, t, exp, "mydb", "cpu")[fileURI(f1)]
	if before.count != 100 {
		t.Fatalf("initial record_count = %d, want 100", before.count)
	}
	snap1 := currentSnapshotID(ctx, t, exp)

	// The delete API's partial-match branch: same path, 60 rows, smaller file.
	rewriteInPlace(t, f1, baseTS, 60)
	ref := refOf(t, f1)
	if ref.SizeBytes == before.size {
		t.Fatalf("fixture did not change size (%d); the test needs a different byte size", ref.SizeBytes)
	}
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{ref}); err != nil {
		t.Fatalf("reconcile after rewrite: %v", err)
	}
	after := liveEntries(ctx, t, exp, "mydb", "cpu")
	if len(after) != 1 {
		t.Fatalf("after rewrite: %d live entries, want 1: %+v", len(after), after)
	}
	if got := after[fileURI(f1)]; got.count != 60 || got.size != ref.SizeBytes {
		t.Fatalf("after rewrite: entry = %+v, want count=60 size=%d (manifest still describes the old file)", got, ref.SizeBytes)
	}
	snap2 := currentSnapshotID(ctx, t, exp)
	if snap2 == snap1 {
		t.Fatal("rewrite did not produce a new snapshot")
	}

	// Idempotent: same path + same size → no-op, no new snapshot.
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{ref}); err != nil {
		t.Fatalf("idempotent reconcile: %v", err)
	}
	if got := currentSnapshotID(ctx, t, exp); got != snap2 {
		t.Errorf("unchanged reconcile created a new snapshot (%d -> %d)", snap2, got)
	}

	// A second rewrite of the same path, then retention removes it: neither may wedge.
	rewriteInPlace(t, f1, baseTS, 20)
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, f1)}); err != nil {
		t.Fatalf("second rewrite: %v", err)
	}
	if got := liveEntries(ctx, t, exp, "mydb", "cpu")[fileURI(f1)]; got.count != 20 {
		t.Fatalf("second rewrite: record_count = %d, want 20", got.count)
	}
	if err := os.Remove(f1); err != nil {
		t.Fatal(err)
	}
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, nil); err != nil {
		t.Fatalf("removing a re-registered path wedged the table: %v", err)
	}
	if got := liveEntries(ctx, t, exp, "mydb", "cpu"); len(got) != 0 {
		t.Fatalf("after removal: %d live entries, want 0: %+v", len(got), got)
	}
}

// TestReconcile_RewriteWithStraddlerUsesSlowPath: a rewritten file in the same pass as a
// day-straddling file takes the per-file fallback; the straddler is skipped and the rewritten
// path is still re-registered.
func TestReconcile_RewriteWithStraddlerUsesSlowPath(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	good := filepath.Join(dir, "data", "cpu_good.parquet")
	bad := filepath.Join(dir, "data", "cpu_bad.parquet")
	baseTS := int64(1_700_000_000_000_000)
	writeArcStyleParquet(t, good, baseTS, 100)
	writeStraddlingParquet(t, bad, baseTS)
	exp := newTestExporter(t, dir, 0)
	sc, err := SchemaFromParquet(good)
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, good)}); err != nil {
		t.Fatalf("initial: %v", err)
	}
	rewriteInPlace(t, good, baseTS, 60)
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, good), refOf(t, bad)}); err != nil {
		t.Fatalf("rewrite + straddler: %v", err)
	}
	got := liveEntries(ctx, t, exp, "mydb", "cpu")
	if _, ok := got[fileURI(bad)]; ok {
		t.Errorf("straddling file was registered: %+v", got)
	}
	if e := got[fileURI(good)]; e.count != 60 {
		t.Fatalf("rewritten file not re-registered on the slow path: %+v", e)
	}
}

// TestReconcile_ReaddAfterRemove covers the restore shape: a path that left the table in an
// earlier pass comes back later (backup restore writes the same key). iceberg-go refuses to
// re-add a path it still holds a DELETED entry for; manifest merging sheds those entries.
func TestReconcile_ReaddAfterRemove(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	f1 := filepath.Join(dir, "data", "cpu_h1.parquet")
	f2 := filepath.Join(dir, "data", "cpu_h2.parquet")
	baseTS := int64(1_700_000_000_000_000)
	writeArcStyleParquet(t, f1, baseTS, 100)
	writeArcStyleParquet(t, f2, baseTS+3_600_000_000, 40)
	exp := newTestExporter(t, dir, 0)
	sc, err := SchemaFromParquet(f1)
	if err != nil {
		t.Fatal(err)
	}
	steps := [][]FileRef{
		{refOf(t, f1), refOf(t, f2)}, // both present
		{refOf(t, f2)},               // f1 removed
		{refOf(t, f1), refOf(t, f2)}, // f1 restored at the same path
	}
	for i, files := range steps {
		if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, files); err != nil {
			t.Fatalf("step %d: %v", i+1, err)
		}
	}
	got := liveEntries(ctx, t, exp, "mydb", "cpu")
	if len(got) != 2 || got[fileURI(f1)].count != 100 {
		t.Fatalf("restored path not live: %+v", got)
	}
	// And it can leave again.
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, f2)}); err != nil {
		t.Fatalf("removing the restored path: %v", err)
	}
}

// TestReconcile_RewriteTogglesMergePerTransaction pins the metadata shape after a rewrite pass:
// manifest merging was used for that commit only (the property is back to "false" on the
// committed table), the live file list sits in ONE data manifest with no DELETED entries, and
// a plain add afterwards still runs with iceberg-go's duplicate check (a live path is refused).
func TestReconcile_RewriteTogglesMergePerTransaction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	f1 := filepath.Join(dir, "data", "cpu_h1.parquet")
	f2 := filepath.Join(dir, "data", "cpu_h2.parquet")
	baseTS := int64(1_700_000_000_000_000)
	writeArcStyleParquet(t, f1, baseTS, 100)
	writeArcStyleParquet(t, f2, baseTS+3_600_000_000, 40)
	exp := newTestExporter(t, dir, 0)
	sc, err := SchemaFromParquet(f1)
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, f1), refOf(t, f2)}); err != nil {
		t.Fatal(err)
	}
	rewriteInPlace(t, f1, baseTS, 60)
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc, []FileRef{refOf(t, f1), refOf(t, f2)}); err != nil {
		t.Fatalf("rewrite pass: %v", err)
	}
	tbl, err := exp.EnsureTable(ctx, "mydb", "cpu", sc)
	if err != nil {
		t.Fatal(err)
	}
	if got := tbl.Properties()[icetable.ManifestMergeEnabledKey]; got != "false" {
		t.Errorf("manifest merging left %q on the table after the commit, want \"false\"", got)
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 {
		t.Fatalf("after a rewrite pass the table has %d data manifests, want 1 (merged)", len(manifests))
	}
	if m := manifests[0]; m.DeletedDataFiles() != 0 || m.AddedDataFiles()+m.ExistingDataFiles() != 2 {
		t.Fatalf("merged manifest counts added=%d existing=%d deleted=%d, want live=2 deleted=0",
			m.AddedDataFiles(), m.ExistingDataFiles(), m.DeletedDataFiles())
	}
	// The duplicate check is back for ordinary adds: a live path is refused.
	txn := tbl.NewTransaction()
	if err := txn.AddFiles(ctx, []string{fileURI(f2)}, nil, false); !isAlreadyReferencedError(err) {
		t.Fatalf("plain add of a live path after the rewrite pass = %v, want the duplicate refusal", err)
	}
}
