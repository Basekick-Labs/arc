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

// #1848 test gap: a file a re-home job took over after its flip (the job's
// owned filter, not a compaction) loses its cold copy and is hot again.
func TestDropTakenOverForARehomeJob1848(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	t.Cleanup(cleanup)
	cold.seedRaw(missA, []byte("x"))
	m.SetOwnedFilter(func(p string) bool { return p == missA })
	var reported [][]string
	m.SetOnColdRemoved(func(k []string) { reported = append(reported, k) })
	kept := m.migrator.dropTakenOver(context.Background(), []MigrationCandidate{candidateFor(missA), candidateFor(missB)})
	if len(kept) != 1 || kept[0].Path != missB {
		t.Fatalf("kept %v; want only the file no job owns", kept)
	}
	if n, _ := cold.StatFile(context.Background(), missA); n >= 0 {
		t.Fatal("the cold copy of the job's file was kept")
	}
	if len(reported) != 1 || len(reported[0]) != 1 || reported[0][0] != missA {
		t.Fatalf("reported %v", reported)
	}
}
