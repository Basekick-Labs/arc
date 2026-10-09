package iceberg

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

func TestDottedNamespaceMigrationDryRunThenApply(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	exp, err := NewExporter(db, backend, localFileURI(root), "arc", 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	database, measurement := "rocket.01/telemetry", "cpu"
	dataPath := filepath.Join(root, "rocket.01", "telemetry", measurement, "2026", "07", "14", "15", "a.parquet")
	writeArcStyleParquet(t, dataPath, 1_752_500_000_000_000, 2)
	schema, err := UnionSchema(ctx, []string{dataPath})
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.ReconcileMeasurement(ctx, database, measurement, schema, []FileRef{refOf(t, dataPath)}); err != nil {
		t.Fatalf("seed dotted spoke table: %v", err)
	}

	identifier := exp.tableIdent(database, measurement)
	newNamespace := catalogNamespaceKey(identifier[:len(identifier)-1])
	oldNamespace := "arc_rocket.01.telemetry"
	oldTableLocation := localFileURI(filepath.Join(root, oldNamespace+".db", measurement))
	if _, _, err := exp.catalog.CommitTable(ctx, identifier, nil,
		[]table.Update{table.NewSetLocationUpdate(oldTableLocation)}); err != nil {
		t.Fatalf("place seeded table at legacy warehouse location: %v", err)
	}
	legacyTable, err := exp.catalog.LoadTable(ctx, identifier)
	if err != nil {
		t.Fatal(err)
	}
	if !metadataHasNamespaceDirectory(legacyTable.MetadataLocation(), oldNamespace, measurement) {
		t.Fatalf("seed table metadata did not move to legacy directory: %s", legacyTable.MetadataLocation())
	}
	if legacyTable.CurrentSnapshot() == nil {
		t.Fatal("seed table has no snapshot")
	}
	legacySnapshotID := legacyTable.CurrentSnapshot().SnapshotID
	legacyFiles, err := exp.tableDataFiles(ctx, legacyTable)
	if err != nil {
		t.Fatal(err)
	}
	targetBeforeDryRun := tableTreeHashes(t, ctx, exp, path.Join(newNamespace+".db", measurement))
	if len(targetBeforeDryRun) == 0 {
		t.Fatal("seeded target directory unexpectedly has no files")
	}

	// Model a pre-upgrade SQL catalog row while retaining a namespace property that the migration
	// must carry forward. The row points to the real legacy table location and snapshot history.
	if _, err := db.ExecContext(ctx,
		`UPDATE iceberg_tables SET table_namespace = ? WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		oldNamespace, "arc", newNamespace, measurement); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (?, ?, ?, ?)`,
		"arc", oldNamespace, "migration-test", "preserve-me"); err != nil {
		t.Fatal(err)
	}

	measurements := []Measurement{{Database: database, Measurement: measurement}}
	blocked, err := exp.MigrateDottedNamespaces(ctx, measurements)
	if err != nil {
		t.Fatalf("dry-run plan: %v", err)
	}
	if _, ok := blocked[database+"\x00"+measurement]; !ok {
		t.Fatal("dry-run did not block reconciliation of the legacy table")
	}
	assertCatalogNamespace(t, ctx, db, oldNamespace, measurement)
	if !metadataHasNamespaceDirectory(legacyTable.MetadataLocation(), oldNamespace, measurement) {
		t.Fatal("dry-run changed the legacy metadata location")
	}
	if targetAfterDryRun := tableTreeHashes(t, ctx, exp, path.Join(newNamespace+".db", measurement)); !reflect.DeepEqual(targetAfterDryRun, targetBeforeDryRun) {
		t.Fatal("dry-run changed the target table tree")
	}

	// Leave an interrupted migration after the target files and catalog namespace are in place,
	// but before the table metadata location moves. The next reconcile must finish it safely.
	exp.ConfigureNamespaceMigrationDryRun(false)
	migration := namespaceMigration{
		database: database, measurement: measurement, identifier: identifier,
		oldNamespace: oldNamespace, newNamespace: newNamespace,
		catalogNamespace: oldNamespace, metadataLocation: legacyTable.MetadataLocation(),
	}
	targetMetadata, _, err := relocatedMetadataLocation(legacyTable.MetadataLocation(), oldNamespace, newNamespace, measurement)
	if err != nil {
		t.Fatal(err)
	}
	oldMetadataKey, ok := exp.warehouseRelKey(legacyTable.MetadataLocation())
	if !ok {
		t.Fatal("legacy metadata location is not addressable")
	}
	newMetadataKey, ok := exp.warehouseRelKey(targetMetadata)
	if !ok {
		t.Fatal("target metadata location is not addressable")
	}
	if err := exp.copyAndVerifyTableTree(ctx, path.Dir(path.Dir(oldMetadataKey)), path.Dir(path.Dir(newMetadataKey))); err != nil {
		t.Fatalf("stage interrupted table tree copy: %v", err)
	}
	if err := exp.rekeyNamespaceTable(ctx, migration); err != nil {
		t.Fatalf("stage interrupted catalog rekey: %v", err)
	}
	assertCatalogNamespace(t, ctx, db, newNamespace, measurement)
	blocked, err = exp.MigrateDottedNamespaces(ctx, measurements)
	if err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	if _, ok := blocked[database+"\x00"+measurement]; ok {
		t.Fatal("successful migration left the measurement blocked")
	}
	assertCatalogNamespace(t, ctx, db, newNamespace, measurement)

	migrated, err := exp.catalog.LoadTable(ctx, identifier)
	if err != nil {
		t.Fatalf("load migrated table: %v", err)
	}
	if !metadataHasNamespaceDirectory(migrated.MetadataLocation(), newNamespace, measurement) {
		t.Fatalf("migrated metadata location is not under target namespace: %s", migrated.MetadataLocation())
	}
	if migrated.CurrentSnapshot() == nil || migrated.CurrentSnapshot().SnapshotID != legacySnapshotID {
		t.Fatalf("migration changed the current snapshot: got %v, want %d", migrated.CurrentSnapshot(), legacySnapshotID)
	}
	migratedFiles, err := exp.tableDataFiles(ctx, migrated)
	if err != nil {
		t.Fatal(err)
	}
	if len(migratedFiles) != len(legacyFiles) {
		t.Fatalf("migrated table has %d data files, legacy table had %d", len(migratedFiles), len(legacyFiles))
	}
	for file, size := range legacyFiles {
		if migratedFiles[file] != size {
			t.Fatalf("migrated table changed file %s size: got %d, want %d", file, migratedFiles[file], size)
		}
	}
	if _, err := os.Stat(filepath.Join(root, oldNamespace+".db", measurement, "metadata")); err != nil {
		t.Fatalf("legacy metadata chain was not retained: %v", err)
	}
	var property string
	if err := db.QueryRowContext(ctx,
		`SELECT property_value FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = ?`,
		"arc", newNamespace, "migration-test").Scan(&property); err != nil {
		t.Fatalf("load migrated namespace property: %v", err)
	}
	if property != "preserve-me" {
		t.Fatalf("migrated namespace property = %q, want preserve-me", property)
	}

	_, ok = exp.warehouseRelKey(migrated.MetadataLocation())
	if !ok {
		t.Fatal("migrated metadata location is not addressable by storage backend")
	}
	version, versionDir, ok := exp.parseVersionAndMetaDir(migrated.MetadataLocation())
	if !ok {
		t.Fatalf("parse migrated metadata location %q", migrated.MetadataLocation())
	}
	versionHint, err := backend.Read(ctx, path.Join(versionDir, "version-hint.text"))
	if err != nil {
		t.Fatalf("read migrated version-hint: %v", err)
	}
	if string(versionHint) != version {
		t.Fatalf("version-hint = %q, want %q", versionHint, version)
	}
	versionedMetadataKey := path.Join(versionDir, "v"+version+".metadata.json")
	if _, err := backend.Read(ctx, versionedMetadataKey); err != nil {
		t.Fatalf("read migrated v<N>.metadata.json: %v", err)
	}

	// A second startup pass must recognize the completed migration and do no further work.
	blocked, err = exp.MigrateDottedNamespaces(ctx, measurements)
	if err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	if len(blocked) != 0 {
		t.Fatalf("completed migration returned blocked measurements: %v", blocked)
	}
}

func tableTreeHashes(t *testing.T, ctx context.Context, exp *Exporter, prefix string) map[string]string {
	t.Helper()
	objects, err := exp.backend.List(ctx, strings.TrimSuffix(prefix, "/")+"/")
	if err != nil {
		t.Fatalf("list table tree %q: %v", prefix, err)
	}
	hashes := make(map[string]string, len(objects))
	for _, object := range objects {
		hash, err := exp.hashObject(ctx, object)
		if err != nil {
			t.Fatalf("hash table object %q: %v", object, err)
		}
		hashes[object] = hash
	}
	return hashes
}

func TestRelativeTableObjectPathRejectsTraversal(t *testing.T) {
	for _, source := range []string{
		"legacy.db/cpu/../other.parquet",
		"legacy.db/cpu//metadata.json",
		"legacy.db/cpu/../../outside",
		"legacy.db/cpu/",
	} {
		if _, err := relativeTableObjectPath(source, "legacy.db/cpu"); err == nil {
			t.Errorf("relativeTableObjectPath(%q) accepted an unsafe key", source)
		}
	}
	got, err := relativeTableObjectPath("legacy.db/cpu/metadata/v2.json", "legacy.db/cpu")
	if err != nil || got != "metadata/v2.json" {
		t.Fatalf("relativeTableObjectPath returned %q, %v; want metadata/v2.json", got, err)
	}
}

func TestRelocatedMetadataLocationRejectsMissingScheme(t *testing.T) {
	if _, _, err := relocatedMetadataLocation("/warehouse/legacy.db/cpu/metadata/v1.metadata.json", "legacy", "target", "cpu"); err == nil {
		t.Fatal("relocatedMetadataLocation accepted a path without a URI scheme")
	}
}

func TestLocalFileURIEncodesReservedPathCharacters(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "arc data?#%", "warehouse")
	uri := localFileURI(localPath)
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parse local file URI %q: %v", uri, err)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		t.Fatalf("local path was parsed as query or fragment: %q", uri)
	}
	wantPath := filepath.ToSlash(localPath)
	if !filepath.IsAbs(localPath) {
		t.Fatal("test path is not absolute")
	}
	if !strings.HasPrefix(wantPath, "/") {
		wantPath = "/" + wantPath
	}
	if parsed.Path != wantPath {
		t.Fatalf("local file URI path = %q, want %q", parsed.Path, wantPath)
	}
}

func assertCatalogNamespace(t *testing.T, ctx context.Context, db *sql.DB, want, measurement string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx,
		`SELECT table_namespace FROM iceberg_tables WHERE catalog_name = ? AND table_name = ?`,
		"arc", measurement).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("catalog namespace = %q, want %q", got, want)
	}
}
