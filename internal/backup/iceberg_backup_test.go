package backup

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

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
