package tiering

// Receipt-aware spoke cold migration (#687): the hot-file-removal hook must
// run BEFORE any state flips (a marked receipt on an aborted migration is
// harmless; an unmarked receipt on a deleted file re-accepts duplicates), its
// failure must abort with the cold copy rolled back, and wiring it is what
// lifts the spoke-namespace migration gate.

import (
	"context"
	"errors"
	"testing"
)

func TestSpokeGateLiftsWithRemovalHook(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	spokeDaily := "rocket-01/telemetry/engine_temp/2024/03/15/engine_temp_20240315_231459_1_b1_daily.parquet"
	if err := hot.Write(ctx, spokeDaily, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ScanAndRegisterFiles(ctx); err != nil {
		t.Fatal(err)
	}

	candidates, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("without a removal hook, candidates = %+v, want none (gate closed)", candidates)
	}

	m.SetOnHotFilesRemoved(func([]string) error { return nil })
	candidates, err = m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].Path != spokeDaily {
		t.Fatalf("with a removal hook, candidates = %+v, want the spoke daily file", candidates)
	}
}

func TestMigrateFileMarksReceiptsBeforeAnyStateFlip(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	spokeDaily := "rocket-01/telemetry/engine_temp/2024/03/15/engine_temp_20240315_231459_1_b1_daily.parquet"
	if err := hot.Write(ctx, spokeDaily, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ScanAndRegisterFiles(ctx); err != nil {
		t.Fatal(err)
	}

	var sawPaths []string
	m.SetOnHotFilesRemoved(func(paths []string) error {
		sawPaths = append(sawPaths, paths...)
		// Ordering assertions AT CALL TIME: the hot copy must still exist
		// and the metadata must still say hot — the mark precedes UpdateTier
		// and the delete.
		if ok, _ := hot.Exists(ctx, paths[0]); !ok {
			t.Error("hook called after the hot copy was already deleted")
		}
		if meta, err := m.metadata.GetFile(ctx, paths[0]); err != nil || meta.Tier != TierHot {
			t.Errorf("hook called after tier flip: (%+v, %v)", meta, err)
		}
		return nil
	})

	candidates, _ := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if migrated, errs := m.migrator.MigrateBatch(ctx, candidates); migrated != 1 || errs != 0 {
		t.Fatalf("MigrateBatch = (%d, %d), want (1, 0)", migrated, errs)
	}
	if len(sawPaths) != 1 || sawPaths[0] != spokeDaily {
		t.Fatalf("hook saw %v, want exactly the migrated path", sawPaths)
	}
	if ok, _ := hot.Exists(ctx, spokeDaily); ok {
		t.Fatal("hot copy survived a successful migration")
	}
	if ok, _ := cold.Exists(ctx, spokeDaily); !ok {
		t.Fatal("cold copy missing after migration")
	}
}

func TestMarkFailureAbortsMigrationAndRollsBack(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	spokeDaily := "rocket-01/telemetry/engine_temp/2024/03/15/engine_temp_20240315_231459_1_b1_daily.parquet"
	if err := hot.Write(ctx, spokeDaily, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ScanAndRegisterFiles(ctx); err != nil {
		t.Fatal(err)
	}
	m.SetOnHotFilesRemoved(func([]string) error { return errors.New("sync db unavailable") })

	candidates, _ := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if migrated, errs := m.migrator.MigrateBatch(ctx, candidates); migrated != 0 || errs != 1 {
		t.Fatalf("MigrateBatch = (%d, %d), want (0, 1) on mark failure", migrated, errs)
	}
	if ok, _ := hot.Exists(ctx, spokeDaily); !ok {
		t.Fatal("hot copy deleted despite mark failure")
	}
	if ok, _ := cold.Exists(ctx, spokeDaily); ok {
		t.Fatal("cold copy not rolled back after mark failure")
	}
	if meta, err := m.metadata.GetFile(ctx, spokeDaily); err != nil || meta.Tier != TierHot {
		t.Fatalf("row = (%+v, %v), want tier still hot so the candidate retries", meta, err)
	}
}

func TestReconcileMarksBeforeOrphanDelete(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	spokeDaily := "rocket-01/telemetry/engine_temp/2024/03/15/engine_temp_20240315_231459_1_b1_daily.parquet"
	// Simulate a crash after cold copy + tier flip but before the hot delete.
	for _, b := range []*mockBackend{hot, cold} {
		if err := b.Write(ctx, spokeDaily, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.ScanAndRegisterFiles(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.metadata.UpdateTier(ctx, spokeDaily, TierCold); err != nil {
		t.Fatal(err)
	}

	marks := 0
	fail := true
	m.SetOnHotFilesRemoved(func([]string) error {
		marks++
		if fail {
			return errors.New("sync db unavailable")
		}
		return nil
	})

	_, deleted, errs := m.migrator.ReconcileOrphanedFiles(ctx)
	if deleted != 0 || errs != 1 || marks != 1 {
		t.Fatalf("failing mark: (deleted=%d, errs=%d, marks=%d), want (0, 1, 1) — orphan kept", deleted, errs, marks)
	}
	if ok, _ := hot.Exists(ctx, spokeDaily); !ok {
		t.Fatal("orphan deleted despite mark failure")
	}

	fail = false
	_, deleted, errs = m.migrator.ReconcileOrphanedFiles(ctx)
	if deleted != 1 || errs != 0 {
		t.Fatalf("passing mark: (deleted=%d, errs=%d), want (1, 0)", deleted, errs)
	}
	if ok, _ := hot.Exists(ctx, spokeDaily); ok {
		t.Fatal("orphan survived a successful reconcile")
	}
}
