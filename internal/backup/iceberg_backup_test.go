package backup

import (
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
