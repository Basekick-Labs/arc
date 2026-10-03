package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestCollectReachableManifestMetadataIssue835(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	var sc ArcSchema
	for i := 0; i < 5; i++ {
		file := filepath.Join(root, "db", "cpu", "2023", "11", "14", "22", fmt.Sprintf("f%d.parquet", i))
		writeArcStyleParquet(t, file, int64(1_700_000_000_000_000)+int64(i)*1000, 5)
		if i == 0 {
			sc, err = SchemaFromParquet(file)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc, []FileRef{refOf(t, file)}); err != nil {
			t.Fatal(err)
		}
	}
	tbl, err := exp.catalog.LoadTable(ctx, exp.tableIdent("db", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	protected, err := exp.collectReachableManifestMetadata(ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	if len(protected) == 0 {
		t.Fatal("empty protection set after snapshot commits")
	}
	for _, snap := range tbl.Metadata().Snapshots() {
		key, ok := exp.warehouseRelKey(snap.ManifestList)
		if !ok {
			t.Fatalf("unaddressable list %q", snap.ManifestList)
		}
		if _, found := protected[key]; !found {
			t.Errorf("live snapshot manifest list not protected: %q", key)
		}
	}

	dir := path.Dir(mustMetadataKeyIssue835(t, exp, tbl.MetadataLocation()))
	if err := backend.Write(ctx, dir+"/v99999.metadata.json", []byte("{corrupt")); err != nil {
		t.Fatal(err)
	}
	if _, err := exp.collectReachableManifestMetadata(ctx, tbl); err == nil {
		t.Fatal("corrupt retained metadata must abort reachability")
	}
	// The collector itself is read-only, including when it encounters corruption.
	for key := range protected {
		exists, err := backend.Exists(ctx, key)
		if err != nil || !exists {
			t.Fatalf("protected object %q lost: %v", key, err)
		}
	}
}

func mustMetadataKeyIssue835(t *testing.T, exp *Exporter, uri string) string {
	t.Helper()
	key, ok := exp.warehouseRelKey(uri)
	if !ok {
		t.Fatalf("unaddressable URI %q", uri)
	}
	return key
}

func TestOldUnreachableManifestCandidatesIssue835(t *testing.T) {
	now := time.Now()
	dir := "arc_db.db/cpu/metadata"
	old := now.Add(-8 * 24 * time.Hour)
	young := now.Add(-time.Hour)
	protected := map[string]struct{}{dir + "/kept.avro": {}}
	objects := []storage.ObjectInfo{
		{Path: dir + "/orphan.avro", LastModified: old},
		{Path: dir + "/orphan.avro", LastModified: old},
		{Path: dir + "/kept.avro", LastModified: old},
		{Path: dir + "/young.avro", LastModified: young},
		{Path: dir + "/unknown.avro"},
		{Path: dir + "/data.parquet", LastModified: old},
		{Path: dir + "/v1.metadata.json", LastModified: old},
		{Path: dir + "/nested/stray.avro", LastModified: old},
		{Path: "another/metadata/stray.avro", LastModified: old},
	}
	got := oldUnreachableManifestCandidates(objects, dir, protected, now)
	if len(got) != 1 || got[0] != dir+"/orphan.avro" {
		t.Fatalf("candidates = %v, want only orphan.avro", got)
	}
}

// TestManifestOrphanSweepIssue835 exercises metadata-only deletion on a
// temporary local backend. It intentionally ages both reachable and orphaned
// Avro files to verify that reachability wins over the grace-period cutoff.
func TestManifestOrphanSweepIssue835(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	var sc ArcSchema
	var latestFile string
	for i := 0; i < 7; i++ {
		latestFile = filepath.Join(root, "db", "cpu", "2023", "11", "14", "22", fmt.Sprintf("gc%d.parquet", i))
		writeArcStyleParquet(t, latestFile, int64(1_700_000_000_000_000)+int64(i)*1000, 5)
		if i == 0 {
			sc, err = SchemaFromParquet(latestFile)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc, []FileRef{refOf(t, latestFile)}); err != nil {
			t.Fatal(err)
		}
	}
	tbl, err := exp.catalog.LoadTable(ctx, exp.tableIdent("db", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	protected, err := exp.collectReachableManifestMetadata(ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	if len(protected) == 0 {
		t.Fatal("no reachable manifest metadata")
	}
	dir := path.Dir(mustMetadataKeyIssue835(t, exp, tbl.MetadataLocation()))
	old := time.Now().Add(-9 * 24 * time.Hour)
	for key := range protected {
		if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(key)), old, old); err != nil {
			t.Fatal(err)
		}
	}

	// Pick an actual Iceberg-generated orphan, not a synthetic Avro file.
	generated, err := backend.ListObjects(ctx, dir+"/")
	if err != nil {
		t.Fatal(err)
	}

	var orphan string
	for _, obj := range generated {
		if path.Dir(obj.Path) != dir ||
			!strings.HasSuffix(obj.Path, ".avro") {
			continue
		}
		if _, reachable := protected[obj.Path]; !reachable {
			orphan = obj.Path
			break
		}
	}
	if orphan == "" {
		t.Fatal("no actual expired Iceberg manifest found")
	}

	original, err := backend.Read(ctx, orphan)
	if err != nil || len(original) == 0 {
		t.Fatalf("read generated orphan: bytes=%d, err=%v",
			len(original), err)
	}

	young := dir + "/young-issue835.avro"
	parquet := dir + "/not-metadata.parquet"
	for _, key := range []string{young, parquet} {
		if err := backend.Write(ctx, key, []byte("fixture")); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{orphan, parquet} {
		if err := os.Chtimes(
			filepath.Join(root, filepath.FromSlash(key)), old, old,
		); err != nil {
			t.Fatal(err)
		}
	}
	exp.sweepOrphanManifestMetadata(ctx, tbl, "db", "cpu")
	if exists, err := backend.Exists(ctx, orphan); err != nil || exists {
		t.Fatalf("old orphan still present (exists=%v, err=%v)", exists, err)
	}
	for key := range protected {
		if exists, err := backend.Exists(ctx, key); err != nil || !exists {
			t.Fatalf("reachable old manifest %q missing (exists=%v, err=%v)", key, exists, err)
		}
	}
	for _, key := range []string{young, parquet} {
		if exists, err := backend.Exists(ctx, key); err != nil || !exists {
			t.Errorf("non-eligible object %q removed (exists=%v, err=%v)", key, exists, err)
		}
	}
	// Protected metadata still supports a later reconcile and catalog load.
	if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc, []FileRef{refOf(t, latestFile)}); err != nil {
		t.Fatalf("subsequent reconcile: %v", err)
	}
	if _, err := exp.catalog.LoadTable(ctx, exp.tableIdent("db", "cpu")); err != nil {
		t.Fatalf("load after sweep: %v", err)
	}
}

func TestManifestOrphanSweepFailsClosedIssue835(t *testing.T) {
	for _, failure := range []string{"corrupt-metadata", "corrupt-manifest-list", "retain-zero"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			backend, err := storage.NewLocalBackend(root, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = backend.Close() })
			db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			retain := 2
			if failure == "retain-zero" {
				retain = 0
			}
			exp, err := NewExporter(db, backend, "file://"+root, "arc", retain, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			data := filepath.Join(root, "db", "cpu", "2023", "11", "14", "22", "source.parquet")
			writeArcStyleParquet(t, data, int64(1_700_000_000_000_000), 5)
			sc, err := SchemaFromParquet(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc, []FileRef{refOf(t, data)}); err != nil {
				t.Fatal(err)
			}
			tbl, err := exp.catalog.LoadTable(ctx, exp.tableIdent("db", "cpu"))
			if err != nil {
				t.Fatal(err)
			}
			dir := path.Dir(mustMetadataKeyIssue835(t, exp, tbl.MetadataLocation()))
			old := time.Now().Add(-9 * 24 * time.Hour)
			orphan := dir + "/orphan-issue835.avro"
			if err := backend.Write(ctx, orphan, []byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(orphan)), old, old); err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "corrupt-metadata":
				if err := backend.Write(ctx, dir+"/v99999.metadata.json", []byte("{bad")); err != nil {
					t.Fatal(err)
				}
			case "corrupt-manifest-list":
				snapshot := tbl.CurrentSnapshot()
				if snapshot == nil {
					t.Fatal("missing snapshot")
				}
				key := mustMetadataKeyIssue835(t, exp, snapshot.ManifestList)
				if err := backend.Write(ctx, key, []byte("invalid manifest list")); err != nil {
					t.Fatal(err)
				}
			}
			exp.sweepOrphanManifestMetadata(ctx, tbl, "db", "cpu")
			if exists, err := backend.Exists(ctx, orphan); err != nil || !exists {
				t.Fatalf("sweep deleted %q in fail-closed case (exists=%v, err=%v)", orphan, exists, err)
			}
		})
	}
}

// TestManifestOrphanSweepQuietTableIssue835 verifies that an orphan which
// becomes old after the last data change is reclaimed by a converged pass.
func TestManifestOrphanSweepQuietTableIssue835(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	exp, err := NewExporter(
		db, backend, "file://"+root, "arc", 2, zerolog.Nop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	data := filepath.Join(
		root, "db", "cpu", "2023", "11", "14", "22", "quiet.parquet",
	)
	writeArcStyleParquet(t, data, int64(1_700_000_000_000_000), 5)

	sc, err := SchemaFromParquet(data)
	if err != nil {
		t.Fatal(err)
	}

	files := []FileRef{refOf(t, data)}
	if err := exp.ReconcileMeasurement(
		ctx, "db", "cpu", sc, files,
	); err != nil {
		t.Fatal(err)
	}

	tbl, err := exp.catalog.LoadTable(
		ctx, exp.tableIdent("db", "cpu"),
	)
	if err != nil {
		t.Fatal(err)
	}

	dir := path.Dir(
		mustMetadataKeyIssue835(t, exp, tbl.MetadataLocation()),
	)
	orphan := dir + "/quiet-orphan-issue835.avro"

	if err := backend.Write(
		ctx, orphan, []byte("unreferenced"),
	); err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-8 * 24 * time.Hour)
	if err := os.Chtimes(
		filepath.Join(root, filepath.FromSlash(orphan)), old, old,
	); err != nil {
		t.Fatal(err)
	}

	// Exactly the same file set: the exporter must use its converged path.
	if err := exp.ReconcileMeasurement(
		ctx, "db", "cpu", sc, files,
	); err != nil {
		t.Fatal(err)
	}

	exists, err := backend.Exists(ctx, orphan)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("quiet reconcile did not reclaim old orphan")
	}

	if _, err := exp.catalog.LoadTable(
		ctx, exp.tableIdent("db", "cpu"),
	); err != nil {
		t.Fatalf("catalog load after quiet sweep: %v", err)
	}
}
