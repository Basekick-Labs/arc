package iceberg

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// A spoke ID WITHOUT a dot must keep the exact catalog key Arc wrote before #1129, because that
// key is the table's identity: if it changed, every existing spoke table would stop being found
// and the exporter would publish a second one beside it -- the orphaning #1129 exists to prevent.
//
// The old scheme is spelled out here rather than called, deliberately. It no longer exists in the
// tree (sanitizeNamespaceDB is gone), so a test that ran current code on both sides would compare
// it against itself and pass whatever either side did.
func TestSpokeCatalogKeyCompatibility(t *testing.T) {
	// The pre-#1129 scheme: one namespace component, separator folded into a dot.
	legacyKey := func(nsPrefix, database string) string {
		return nsPrefix + "_" + strings.ReplaceAll(database, "/", ".")
	}

	for _, tc := range []struct {
		database  string
		unchanged bool // true: same key, no migration; false: key moves, migration required
	}{
		{"rocket01/telemetry", true},
		{"site01/metrics", true},
		{"plant2/db", true},
		{"rocket-01/telemetry", true},
		{"mydb", true},
		{"rocket.01/telemetry", false},
		{"site.a/metrics", false},
	} {
		t.Run(tc.database, func(t *testing.T) {
			namespace, err := namespaceIdentifier("arc", tc.database)
			if err != nil {
				t.Fatalf("namespaceIdentifier(%q): %v", tc.database, err)
			}
			got, legacy := catalogNamespaceKey(namespace), legacyKey("arc", tc.database)
			if tc.unchanged && got != legacy {
				t.Fatalf("catalog key for %q moved: %q != legacy %q; every table under the legacy key would be orphaned",
					tc.database, got, legacy)
			}
			if !tc.unchanged && got == legacy {
				t.Fatalf("catalog key for %q is unchanged (%q); legacyNamespaceCandidates would skip it and the fix would be inert",
					tc.database, got)
			}
		})
	}
}

// The migration rekeys the catalog row and moves NOTHING on disk.
//
// The fixture is built in the order a real pre-upgrade deployment produced: the table is placed at
// the legacy warehouse location FIRST and only then given its snapshot, so the manifest chain is
// written under the legacy tree and is internally consistent there. Seeding the other way round --
// reconciling first and relocating the metadata afterwards -- leaves the manifests under the
// target directory, which is the one state in which a relocating migration appears to work, and
// is why the first version of this test could not see that a copy-and-repoint migration left the
// live chain behind (the copies in the new tree were referenced by nothing).
func TestDottedNamespaceMigrationRekeysWithoutMovingFiles(t *testing.T) {
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
	identifier := exp.tableIdent(database, measurement)
	encodedNamespace := catalogNamespaceKey(identifier[:len(identifier)-1])
	legacyNamespace := "arc_rocket.01.telemetry"

	dataPath := filepath.Join(root, "rocket.01", "telemetry", measurement, "2026", "07", "14", "15", "a.parquet")
	writeArcStyleParquet(t, dataPath, 1_752_500_000_000_000, 2)
	schema, err := UnionSchema(ctx, []string{dataPath})
	if err != nil {
		t.Fatal(err)
	}

	// Place the table at the legacy location BEFORE it has any snapshot.
	if _, err := exp.EnsureTable(ctx, database, measurement, schema); err != nil {
		t.Fatal(err)
	}
	legacyTableURI := localFileURI(filepath.Join(root, legacyNamespace+".db", measurement))
	if _, _, err := exp.catalog.CommitTable(ctx, identifier, nil,
		[]table.Update{table.NewSetLocationUpdate(legacyTableURI)}); err != nil {
		t.Fatal(err)
	}
	// Now give it its data, so the snapshot and its manifests are written under the legacy tree.
	if err := exp.ReconcileMeasurement(ctx, database, measurement, schema, []FileRef{refOf(t, dataPath)}); err != nil {
		t.Fatal(err)
	}
	// Drop the directory EnsureTable made under the encoded key: a pre-upgrade deployment never
	// had one, and leaving it would hide a migration that writes there.
	encodedDir := filepath.Join(root, encodedNamespace+".db")
	if err := os.RemoveAll(encodedDir); err != nil {
		t.Fatal(err)
	}

	legacyTable, err := exp.catalog.LoadTable(ctx, identifier)
	if err != nil {
		t.Fatal(err)
	}
	if legacyTable.CurrentSnapshot() == nil {
		t.Fatal("seed table has no snapshot")
	}
	legacySnapshotID := legacyTable.CurrentSnapshot().SnapshotID
	legacyManifestList := legacyTable.CurrentSnapshot().ManifestList
	legacyMetadataLocation := legacyTable.MetadataLocation()
	if !strings.Contains(legacyManifestList, legacyNamespace+".db") {
		t.Fatalf("fixture is not faithful: manifest list %q is not under the legacy tree", legacyManifestList)
	}
	legacyFiles, err := exp.tableDataFiles(ctx, legacyTable)
	if err != nil {
		t.Fatal(err)
	}

	// Model the pre-upgrade catalog row, plus a namespace property the migration must carry.
	if _, err := db.ExecContext(ctx,
		`UPDATE iceberg_tables SET table_namespace = ? WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		legacyNamespace, "arc", encodedNamespace, measurement); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (?, ?, ?, ?)`,
		"arc", legacyNamespace, "migration-test", "preserve-me"); err != nil {
		t.Fatal(err)
	}

	measurements := []Measurement{{Database: database, Measurement: measurement}}
	key := measurementKey(database, measurement)

	// Dry run: blocks the measurement, changes nothing.
	blocked, err := exp.MigrateDottedNamespaces(ctx, measurements)
	if err != nil {
		t.Fatalf("dry-run plan: %v", err)
	}
	if _, ok := blocked[key]; !ok {
		t.Fatal("dry run did not block reconciliation of the legacy table")
	}
	assertCatalogNamespace(t, ctx, db, legacyNamespace, measurement)

	// Apply.
	exp.ConfigureNamespaceMigrationDryRun(false)
	blocked, err = exp.MigrateDottedNamespaces(ctx, measurements)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, ok := blocked[key]; ok {
		t.Fatal("a successful migration left the measurement blocked")
	}
	assertCatalogNamespace(t, ctx, db, encodedNamespace, measurement)

	// Nothing moved: no directory under the encoded key at all.
	if _, err := os.Stat(encodedDir); !os.IsNotExist(err) {
		t.Errorf("migration created %s; it must rekey the catalog row and move nothing (err=%v)", encodedDir, err)
	}

	migrated, err := exp.catalog.LoadTable(ctx, identifier)
	if err != nil {
		t.Fatalf("load migrated table: %v", err)
	}
	if got := migrated.MetadataLocation(); got != legacyMetadataLocation {
		t.Errorf("metadata location changed: %q != %q", got, legacyMetadataLocation)
	}
	if migrated.CurrentSnapshot() == nil || migrated.CurrentSnapshot().SnapshotID != legacySnapshotID {
		t.Fatalf("migration changed the current snapshot: got %v, want %d", migrated.CurrentSnapshot(), legacySnapshotID)
	}
	if got := migrated.CurrentSnapshot().ManifestList; got != legacyManifestList {
		t.Errorf("manifest list changed: %q != %q", got, legacyManifestList)
	}
	migratedFiles, err := exp.tableDataFiles(ctx, migrated)
	if err != nil {
		t.Fatalf("migrated table is not readable: %v", err)
	}
	if len(migratedFiles) != len(legacyFiles) || len(migratedFiles) == 0 {
		t.Fatalf("migrated table lists %d data files, want %d", len(migratedFiles), len(legacyFiles))
	}

	// The namespace property moved with the row.
	var value string
	if err := db.QueryRowContext(ctx,
		`SELECT property_value FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = ?`,
		"arc", encodedNamespace, "migration-test").Scan(&value); err != nil {
		t.Fatalf("namespace property did not move: %v", err)
	}
	if value != "preserve-me" {
		t.Errorf("namespace property value = %q, want preserve-me", value)
	}

	// Idempotent: a second pass finds nothing to do and blocks nothing.
	blocked, err = exp.MigrateDottedNamespaces(ctx, measurements)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if len(blocked) != 0 {
		t.Errorf("second pass blocked %v, want nothing", blocked)
	}
	assertCatalogNamespace(t, ctx, db, encodedNamespace, measurement)
}

// The rekey refuses a row that changed under it rather than updating a different number of rows
// than it planned for. Without the RowsAffected check an UPDATE matching nothing is silently a
// success, and the measurement is unblocked while its table is still under the legacy key.
func TestRekeyRefusesWhenTheRowDidNotMatch(t *testing.T) {
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
	// Create the catalog schema by touching it once.
	if _, err := exp.namespaceTableRows(ctx); err != nil {
		t.Fatal(err)
	}

	err = exp.rekeyNamespaceTable(ctx, namespaceMigration{
		database:     "rocket.01/telemetry",
		measurement:  "cpu",
		oldNamespace: "arc_rocket.01.telemetry", // no such row
		newNamespace: `__iceberg_namespace_v1__:["arc_rocket.01","telemetry"]`,
	})
	if err == nil {
		t.Fatal("rekey of a row that does not exist reported success")
	}
	if !strings.Contains(err.Error(), "0 rows updated") {
		t.Errorf("error does not name the cause: %v", err)
	}
}

// localFileURI percent-encodes, so an encoded namespace directory survives a URI round trip.
func TestLocalFileURIEncodesReservedPathCharacters(t *testing.T) {
	uri := localFileURI(`/tmp/__iceberg_namespace_v1__:["arc_rocket.01","telemetry"].db/cpu`)
	if !strings.Contains(uri, "%5B") || !strings.Contains(uri, "%22") {
		t.Errorf("localFileURI did not escape reserved characters: %s", uri)
	}
	if got, want := decodedURIPath(uri), `file:///tmp/__iceberg_namespace_v1__:["arc_rocket.01","telemetry"].db/cpu`; got != want {
		t.Errorf("decodedURIPath(localFileURI(p)) = %q, want %q", got, want)
	}
}

func assertCatalogNamespace(t *testing.T, ctx context.Context, db *sql.DB, want, measurement string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx,
		`SELECT table_namespace FROM iceberg_tables WHERE catalog_name = ? AND table_name = ?`,
		"arc", measurement).Scan(&got); err != nil {
		t.Fatalf("read catalog namespace: %v", err)
	}
	if got != want {
		t.Fatalf("catalog namespace = %q, want %q", got, want)
	}
}

// oneMeasurementSource serves a single measurement whose database is a dotted spoke, which the
// storage walk cannot produce on its own (it needs a spoke registry to expand namespaces).
type oneMeasurementSource struct {
	measurement Measurement
	files       []FileRef
	local       []string
}

func (s *oneMeasurementSource) Measurements(context.Context) ([]Measurement, error) {
	return []Measurement{s.measurement}, nil
}
func (s *oneMeasurementSource) Files(context.Context, Measurement) ([]FileRef, error) {
	return s.files, nil
}
func (s *oneMeasurementSource) LocalFiles(context.Context, Measurement) ([]string, error) {
	return s.local, nil
}
func (s *oneMeasurementSource) FilesAndLocal(context.Context, Measurement) ([]FileRef, []string, error) {
	return s.files, s.local, nil
}

// The scheduler must ACT on the blocked set, not merely receive it.
//
// This is the whole point of the mechanism: while a legacy row is still under its dotted key, the
// table is unreachable under the identifier tableIdent builds, so a reconcile would not find it
// and EnsureTable would create a SECOND table beside the readable one -- the orphaning #1129
// exists to prevent. Deleting the gate in runPass leaves every other test in this package green,
// which is why this one exists and asserts on the catalog row count rather than on a log line.
func TestSchedulerSkipsMeasurementsAwaitingNamespaceMigration(t *testing.T) {
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
	identifier := exp.tableIdent(database, measurement)
	encodedNamespace := catalogNamespaceKey(identifier[:len(identifier)-1])
	legacyNamespace := "arc_rocket.01.telemetry"

	dataPath := filepath.Join(root, "rocket.01", "telemetry", measurement, "2026", "07", "14", "15", "a.parquet")
	writeArcStyleParquet(t, dataPath, 1_752_500_000_000_000, 2)
	schema, err := UnionSchema(ctx, []string{dataPath})
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.ReconcileMeasurement(ctx, database, measurement, schema, []FileRef{refOf(t, dataPath)}); err != nil {
		t.Fatal(err)
	}
	// Pre-upgrade catalog row: under the legacy dotted key.
	if _, err := db.ExecContext(ctx,
		`UPDATE iceberg_tables SET table_namespace = ? WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		legacyNamespace, "arc", encodedNamespace, measurement); err != nil {
		t.Fatal(err)
	}

	src := &oneMeasurementSource{
		measurement: Measurement{Database: database, Measurement: measurement},
		files:       []FileRef{refOf(t, dataPath)},
		local:       []string{dataPath},
	}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})

	// Dry-run is the default, so the measurement stays blocked for this pass.
	sched.runPass(ctx)

	var rows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM iceberg_tables WHERE catalog_name = ? AND table_name = ?`,
		"arc", measurement).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("catalog holds %d rows for %s, want 1: the pass reconciled a measurement awaiting migration and created a duplicate table", rows, measurement)
	}
	assertCatalogNamespace(t, ctx, db, legacyNamespace, measurement)
}
