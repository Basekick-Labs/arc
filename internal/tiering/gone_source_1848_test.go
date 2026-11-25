package tiering

import (
	"context"
	"testing"
)

// #1848 item 2: a file removed after the scan listed it (a compaction, a
// DELETE, a re-home job) is no failure of the migration: nothing is copied,
// nothing counts as failed, and no cold object appears.
func TestMigrationSkipsASourceRemovedSinceTheScan(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	const key = "db1/cpu/2024/03/15/cpu_20240315_daily.parquet"
	hot.seedRaw(key, []byte("daily"))
	if res, err := m.ScanAndRegisterFiles(ctx); err != nil || res.FilesRegistered != 1 {
		t.Fatalf("scan: %+v %v", res, err)
	}
	candidates, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidates %+v err %v", candidates, err)
	}
	if err := hot.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	migrated, failed := m.migrator.MigrateBatch(ctx, candidates)
	if migrated != 0 || failed != 0 {
		t.Fatalf("migrated %d failed %d; want neither for a file that is gone", migrated, failed)
	}
	if n, _ := cold.StatFile(ctx, key); n >= 0 {
		t.Fatal("a cold object appeared for a file that is gone")
	}
}
