package backup

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	_ "github.com/mattn/go-sqlite3"

	"github.com/basekick-labs/arc/internal/storage"
)

// TestBackupRestore_IcebergMetadata is the Phase-5 data-loss-gap proof: Iceberg warehouse
// metadata (metadata.json / .avro / version-hint.text) must survive backup + restore, or a
// restored deployment keeps the parquet data but loses the Iceberg tables that point at it.
func TestBackupRestore_IcebergMetadata(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	backupDir := t.TempDir()

	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	// Lay out a realistic store: one parquet data file + a full Iceberg table metadata dir.
	parquetKey := "prod/sensors/2026/07/14/15/sensors_1.parquet"
	iceKeys := []string{
		"arc_prod.db/sensors/metadata/00000-uuid.metadata.json",
		"arc_prod.db/sensors/metadata/00001-uuid.metadata.json",
		"arc_prod.db/sensors/metadata/v1.metadata.json",
		"arc_prod.db/sensors/metadata/abcd-m1.avro",
		"arc_prod.db/sensors/metadata/snap-123-abcd.avro",
		"arc_prod.db/sensors/metadata/version-hint.text",
	}
	if err := data.Write(ctx, parquetKey, []byte("PAR1-fake-parquet")); err != nil {
		t.Fatal(err)
	}
	for _, k := range iceKeys {
		if err := data.Write(ctx, k, []byte("iceberg-metadata-"+filepath.Base(k))); err != nil {
			t.Fatal(err)
		}
	}

	mgr, err := NewManager(&ManagerConfig{
		DataStorage: data,
		BackupPath:  backupDir,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// ── Back up ──────────────────────────────────────────────────────────
	res, err := mgr.CreateBackup(ctx, BackupOptions{IncludeMetadata: false, IncludeConfig: false})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	backupID := res.Manifest.BackupID

	// The Iceberg metadata must be present in the backup under data/<key>.
	backupBackend, _ := storage.NewLocalBackend(backupDir, zerolog.Nop())
	for _, k := range iceKeys {
		if _, err := backupBackend.Read(ctx, backupID+"/data/"+k); err != nil {
			t.Errorf("Iceberg metadata %q not in backup: %v", k, err)
		}
	}

	// ── Simulate loss: delete the Iceberg metadata from the data store ───
	for _, k := range iceKeys {
		if err := data.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range iceKeys {
		if ok, _ := data.Exists(ctx, k); ok {
			t.Fatalf("precondition: %q should be gone before restore", k)
		}
	}

	// ── Restore ──────────────────────────────────────────────────────────
	if _, err := mgr.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	// All Iceberg metadata must be back, with original content.
	for _, k := range iceKeys {
		got, err := data.Read(ctx, k)
		if err != nil {
			t.Errorf("Iceberg metadata %q not restored: %v", k, err)
			continue
		}
		if want := "iceberg-metadata-" + filepath.Base(k); string(got) != want {
			t.Errorf("restored %q content = %q, want %q", k, got, want)
		}
	}
	// And the parquet data file too (sanity).
	if _, err := data.Read(ctx, parquetKey); err != nil {
		t.Errorf("parquet data not restored: %v", err)
	}
}

// The Iceberg SQL catalog holds every table's schema and snapshot pointers. When
// an operator moves it out of the shared database via iceberg.catalog_db_path, a
// backup that only copies the shared database restores Parquet and warehouse
// metadata whose tables no longer resolve.
func TestBackup_SeparateIcebergCatalogIsIncluded(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}

	sharedDB := filepath.Join(t.TempDir(), "arc.db")
	catalogDB := filepath.Join(t.TempDir(), "iceberg-catalog.db")
	for _, p := range []string{sharedDB, catalogDB} {
		db, err := sql.Open("sqlite3", p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	backupDir := t.TempDir()
	m, err := NewManager(&ManagerConfig{
		DataStorage:          dataStorage,
		BackupPath:           backupDir,
		SQLiteDBPath:         sharedDB,
		IcebergCatalogDBPath: catalogDB,
		Logger:               logger,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	res, err := m.CreateBackup(ctx, BackupOptions{IncludeMetadata: true})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if !res.Manifest.HasIcebergCatalog {
		t.Error("manifest does not record the separate Iceberg catalog")
	}

	stored := filepath.Join(backupDir, res.Manifest.BackupID, "metadata", icebergCatalogDBName)
	if _, err := os.Stat(stored); err != nil {
		t.Fatalf("Iceberg catalog was not backed up: %v", err)
	}
}

// When the catalog lives in the shared database (the default), it must not be
// copied a second time under its own name.
func TestBackup_SharedIcebergCatalogIsNotDuplicated(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}

	sharedDB := filepath.Join(t.TempDir(), "arc.db")
	db, err := sql.Open("sqlite3", sharedDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	backupDir := t.TempDir()
	m, err := NewManager(&ManagerConfig{
		DataStorage:  dataStorage,
		BackupPath:   backupDir,
		SQLiteDBPath: sharedDB,
		// Same file, spelled the same way — the common default.
		IcebergCatalogDBPath: sharedDB,
		Logger:               logger,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	res, err := m.CreateBackup(ctx, BackupOptions{IncludeMetadata: true})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if res.Manifest.HasIcebergCatalog {
		t.Error("a catalog inside the shared database must not be recorded as separate")
	}
	stored := filepath.Join(backupDir, res.Manifest.BackupID, "metadata", icebergCatalogDBName)
	if _, err := os.Stat(stored); err == nil {
		t.Error("catalog was copied a second time despite living in the shared database")
	}
}

// Restoring a backup that predates the separate-catalog support must skip the
// missing catalog rather than fail. Presence is tested via Exists, so this holds
// regardless of how a backend words its not-found error.
func TestRestore_MissingIcebergCatalogIsSkipped(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}

	sharedDB := filepath.Join(t.TempDir(), "arc.db")
	db, err := sql.Open("sqlite3", sharedDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	backupDir := t.TempDir()

	// Take a backup with NO Iceberg catalog configured — an "old" backup.
	writer, err := NewManager(&ManagerConfig{
		DataStorage:  dataStorage,
		BackupPath:   backupDir,
		SQLiteDBPath: sharedDB,
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	res, err := writer.CreateBackup(ctx, BackupOptions{IncludeMetadata: true})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if res.Manifest.HasIcebergCatalog {
		t.Fatal("setup: this backup should not contain a catalog")
	}

	// Restore it on a deployment that DOES configure a separate catalog.
	catalogDB := filepath.Join(t.TempDir(), "iceberg-catalog.db")
	reader, err := NewManager(&ManagerConfig{
		DataStorage:          dataStorage,
		BackupPath:           backupDir,
		SQLiteDBPath:         sharedDB,
		IcebergCatalogDBPath: catalogDB,
		Logger:               logger,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if err := reader.restoreSQLite(ctx, res.Manifest.BackupID); err != nil {
		t.Fatalf("restoring a backup without a catalog must not fail: %v", err)
	}
	if _, err := os.Stat(catalogDB); err == nil {
		t.Error("no catalog was in the backup, so none should have been written")
	}
}

// A catalog present in the backup must actually be restored to the configured path.
func TestRestore_SeparateIcebergCatalogIsRestored(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}

	sharedDB := filepath.Join(t.TempDir(), "arc.db")
	catalogDB := filepath.Join(t.TempDir(), "iceberg-catalog.db")
	for _, p := range []string{sharedDB, catalogDB} {
		db, err := sql.Open("sqlite3", p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("CREATE TABLE marker (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	backupDir := t.TempDir()
	m, err := NewManager(&ManagerConfig{
		DataStorage:          dataStorage,
		BackupPath:           backupDir,
		SQLiteDBPath:         sharedDB,
		IcebergCatalogDBPath: catalogDB,
		Logger:               logger,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	res, err := m.CreateBackup(ctx, BackupOptions{IncludeMetadata: true})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	// Destroy the live catalog, then restore it from the backup.
	if err := os.Remove(catalogDB); err != nil {
		t.Fatal(err)
	}
	if err := m.restoreSQLite(ctx, res.Manifest.BackupID); err != nil {
		t.Fatalf("restoreSQLite: %v", err)
	}
	// Restores are STAGED (#635); the boot apply performs the swap.
	if _, err := os.Stat(StagePath(catalogDB)); err != nil {
		t.Fatalf("catalog restore not staged: %v", err)
	}
	ApplyPendingRestores(logger, sharedDB, catalogDB)

	db, err := sql.Open("sqlite3", catalogDB)
	if err != nil {
		t.Fatalf("restored catalog is not openable: %v", err)
	}
	defer db.Close()
	var name string
	if err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='marker'`).Scan(&name); err != nil {
		t.Fatalf("restored catalog is missing its contents: %v", err)
	}
}

// Restore semantics (#635, apply-at-boot): the API path STAGES and must not
// touch the live database, its sidecars, or create safety copies; the boot
// apply does the swap. Replaces the #678-era live-swap tests.
func TestRestoreSQLiteFile_StagesWithoutTouchingLive(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	liveDir := t.TempDir()
	dbPath := filepath.Join(liveDir, "arc.db")
	live, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if _, err := live.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t (v INTEGER); INSERT INTO t (v) VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
	preBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   mustLocalBackend(t, t.TempDir(), logger),
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}
	older, err := sql.Open("sqlite3", filepath.Join(backupDir, "older.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := older.Exec("CREATE TABLE t (v INTEGER); INSERT INTO t (v) VALUES (7)"); err != nil {
		t.Fatal(err)
	}
	older.Close()
	olderData, err := os.ReadFile(filepath.Join(backupDir, "older.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.backupStorage.Write(ctx, "backup-1/metadata/arc.db", olderData); err != nil {
		t.Fatal(err)
	}

	if err := m.restoreSQLiteFile(ctx, "backup-1", "arc.db", dbPath); err != nil {
		t.Fatalf("restoreSQLiteFile: %v", err)
	}

	// Live database and sidecars untouched, byte for byte.
	postBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(preBytes, postBytes) {
		t.Fatal("staging modified the live database file")
	}
	if _, err := os.Stat(dbPath + "-wal"); err != nil {
		t.Fatal("staging removed the live WAL sidecar")
	}
	if _, err := os.Stat(dbPath + ".before-restore"); !os.IsNotExist(err) {
		t.Fatal("staging created a safety copy; that belongs to boot apply")
	}

	// The staged file holds exactly the backup bytes.
	staged, err := os.ReadFile(StagePath(dbPath))
	if err != nil {
		t.Fatalf("staged restore missing: %v", err)
	}
	if !bytes.Equal(staged, olderData) {
		t.Fatal("staged restore does not match the backup contents")
	}

	// Boot apply: live becomes the backup, safety copy holds the live
	// database's WAL-resident commits, no sidecars remain anywhere.
	live.Close()
	ApplyPendingRestores(logger, dbPath)

	// Filesystem assertions FIRST: opening the WAL-mode safety copy below
	// recreates its sidecars, so any handle open before these checks would
	// fabricate the very files the apply must have removed.
	for _, sidecar := range []string{dbPath + "-wal", dbPath + "-shm", dbPath + ".before-restore-wal", dbPath + ".before-restore-shm", StagePath(dbPath)} {
		if _, err := os.Stat(sidecar); !os.IsNotExist(err) {
			t.Errorf("%s still present after boot apply", sidecar)
		}
	}

	restored, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var got int
	if err := restored.QueryRow("SELECT count(*) FROM t").Scan(&got); err != nil || got != 1 {
		t.Fatalf("applied database rows = (%d, %v), want the backup's 1", got, err)
	}
	safety, err := sql.Open("sqlite3", dbPath+".before-restore")
	if err != nil {
		t.Fatal(err)
	}
	defer safety.Close()
	if err := safety.QueryRow("SELECT count(*) FROM t").Scan(&got); err != nil || got != 2 {
		t.Fatalf("safety copy rows = (%d, %v), want the live database's 2 (WAL folded in)", got, err)
	}
}
