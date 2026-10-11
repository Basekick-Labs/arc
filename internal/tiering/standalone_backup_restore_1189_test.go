package tiering

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestStandaloneRestorePreservesColdCopyWhenLegacyBackupHasNoSidecar(t *testing.T) {
	ctx := context.Background()
	tierManager, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	const path = gateDailyA
	const backupBody = "PAR1-from-legacy-backup"
	const coldBody = "PAR1-existing-cold-copy"
	if err := hot.Write(ctx, path, []byte(backupBody)); err != nil {
		t.Fatalf("write hot source: %v", err)
	}

	backupDir := t.TempDir()
	backupManager, err := backup.NewManager(&backup.ManagerConfig{
		DataStorage: hot,
		BackupPath:  backupDir,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("create backup manager: %v", err)
	}
	created, err := backupManager.CreateBackup(ctx, backup.BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	backupStore, err := storage.NewLocalBackend(backupDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("open backup store: %v", err)
	}
	defer backupStore.Close()
	if err := backupStore.Delete(ctx, created.Manifest.BackupID+"/manifest-files.json"); err != nil {
		t.Fatalf("remove sidecar to model a pre-stage-C backup: %v", err)
	}

	if err := hot.Delete(ctx, path); err != nil {
		t.Fatalf("remove hot copy: %v", err)
	}
	if err := cold.Write(ctx, path, []byte(coldBody)); err != nil {
		t.Fatalf("write existing cold copy: %v", err)
	}
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	recordHotRow(t, tierManager, path, partition)

	// A standalone scan rebuilds the cold row before the restore consults it.
	scan, err := tierManager.ScanTiers(ctx)
	if err != nil {
		t.Fatalf("ScanTiers: %v", err)
	}
	if scan.ColdSynced != 1 || scan.ColdSyncFailed {
		t.Fatalf("ScanTiers = %+v, want one cold row synced without failure", scan)
	}
	if got := fileMeta(t, tierManager, path).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold after the standalone scan", got)
	}

	backupManager.SetColdSource(tierManager)
	if _, err := backupManager.RestoreBackup(ctx, backup.RestoreOptions{
		BackupID:    created.Manifest.BackupID,
		RestoreData: true,
	}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	got, err := cold.Read(ctx, path)
	if err != nil {
		t.Fatalf("read cold copy after restore: %v", err)
	}
	if string(got) != coldBody {
		t.Fatalf("cold copy = %q, want it untouched at %q", got, coldBody)
	}
	if exists, err := hot.Exists(ctx, path); err != nil {
		t.Fatalf("check hot copy: %v", err)
	} else if exists {
		t.Fatal("restore recreated a hot copy despite the standalone cold row")
	}
	if got := backupManager.GetProgress().ColdRowsSkippedUnverifiable; got != 1 {
		t.Fatalf("cold_rows_skipped_unverifiable = %d, want 1", got)
	}
}
