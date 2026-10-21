package tiering

import (
	"context"
	"errors"
	"testing"
	"time"
)

type failedDeleteBackend struct {
	*mockBackend
	deleteErr  error
	deleteErrs int
}

func (b *failedDeleteBackend) Delete(ctx context.Context, path string) error {
	if b.deleteErrs > 0 {
		b.deleteErrs--
		return b.deleteErr
	}
	return b.mockBackend.Delete(ctx, path)
}

func TestRunCycleCleansFailedColdCopyForHotOnlyDatabase(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := gateDailyA
	coldWithRetry := &failedDeleteBackend{mockBackend: cold}
	m.coldBackend = coldWithRetry

	mustWrite(t, hot, path)
	recordHotRow(t, m, path, manifestPartition)
	if _, err := m.metadata.db.Exec(`CREATE TRIGGER fail_tier_flip BEFORE UPDATE OF tier ON tier_files BEGIN SELECT RAISE(ABORT, 'forced tier flip failure'); END`); err != nil {
		t.Fatalf("create tier-flip failure trigger: %v", err)
	}
	originalErr := m.migrator.MigrateFile(ctx, candidateFor(path))
	if originalErr == nil {
		t.Fatal("MigrateFile succeeded despite the forced tier-flip failure")
	}
	if exists, err := cold.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("cold copy exists after failed migration = %v, err = %v; want the failed copy", exists, err)
	}
	if exists, err := hot.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("hot copy exists after failed migration = %v, err = %v; want preserved", exists, err)
	}
	var failures int
	if err := m.metadata.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tier_migrations WHERE file_path = ? AND error != ''`, path).Scan(&failures); err != nil {
		t.Fatalf("count failed migrations: %v", err)
	}
	if failures == 0 {
		t.Fatal("failed migration was not recorded")
	}
	if _, err := m.metadata.db.Exec(`DROP TRIGGER fail_tier_flip`); err != nil {
		t.Fatalf("drop tier-flip failure trigger: %v", err)
	}
	// Cleanup is based on current tier state, so a failover that loses
	// migration history must not strand the redundant cold copy.
	if _, err := m.metadata.db.ExecContext(ctx, `DELETE FROM tier_migrations WHERE file_path = ?`, path); err != nil {
		t.Fatalf("delete migration history: %v", err)
	}
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}
	coldWithRetry.deleteErr = errors.New("temporary cold delete failure")
	coldWithRetry.deleteErrs = 1
	cacheInvalidations := 0
	m.SetOnMigrationComplete(func() { cacheInvalidations++ })

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}
	if result, _ := m.LastScan(); result == nil || result.ColdSyncFailed {
		t.Fatalf("scan after per-object delete failure = %+v, want cycle to continue without a cold sync failure", result)
	}
	if exists, err := cold.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("cold object after failed cleanup = %v, err = %v; want preserved for retry", exists, err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierHot {
		t.Fatalf("tier after failed cleanup = %s, want hot", got)
	}
	if cacheInvalidations != 0 {
		t.Fatalf("query cache invalidations = %d, want none before cold-copy deletion", cacheInvalidations)
	}

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("retry runCycle: %v", err)
	}
	if exists, err := cold.Exists(ctx, path); err != nil || exists {
		t.Fatalf("failed cold copy exists after cleanup = %v, err = %v; want removed", exists, err)
	}
	if exists, err := hot.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("hot copy exists after cleanup = %v, err = %v; want preserved", exists, err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierHot {
		t.Fatalf("tier after failed-copy cleanup = %s, want hot", got)
	}
	if cacheInvalidations != 1 {
		t.Fatalf("query cache invalidations = %d, want 1 after cold-copy deletion", cacheInvalidations)
	}
}

func TestHotOnlyCleanupPreservesColdMetadataRowsAfterFailedFlip(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := gateDailyA
	mustWrite(t, hot, path)
	recordHotRow(t, m, path, manifestPartition)
	if _, err := m.metadata.db.Exec(`CREATE TRIGGER fail_tier_flip BEFORE UPDATE OF tier ON tier_files BEGIN SELECT RAISE(ABORT, 'forced tier flip failure'); END`); err != nil {
		t.Fatalf("create tier-flip failure trigger: %v", err)
	}
	if err := m.migrator.MigrateFile(ctx, candidateFor(path)); err == nil {
		t.Fatal("MigrateFile succeeded despite the forced tier-flip failure")
	}
	if _, err := m.metadata.db.Exec(`DROP TRIGGER fail_tier_flip`); err != nil {
		t.Fatalf("drop tier-flip failure trigger: %v", err)
	}

	coldSize, err := cold.StatFile(ctx, path)
	if err != nil || coldSize < 0 {
		t.Fatalf("cold object after failed migration = size %d, err %v; want present", coldSize, err)
	}
	manifest := &fakeManifest{hot: hot, entries: map[string]int64{path: coldSize}}
	m.manifest = manifest
	if _, _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("sync failed migration before hot-only pin: %v", err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierCold {
		t.Fatalf("tier after pre-pin sync = %s, want cold", got)
	}
	// Model the orphan reconciliation removing the hot manifest entry while
	// the primary's local hot file remains after a failed hot delete/restart.
	delete(manifest.entries, path)
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}
	faultyHot := &hotOnlyExistsErrorBackend{mockBackend: hot}
	m.hotBackend = faultyHot

	if _, _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("syncColdTierMetadata: %v", err)
	}
	if exists, err := cold.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("cold object after sync = %v, err = %v; want preserved for cold metadata row", exists, err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierCold {
		t.Fatalf("metadata tier after sync = %s, want cold", got)
	}
	if size, ok := manifest.ManifestEntry(path); ok || size != 0 {
		t.Fatalf("hot manifest entry after simulated orphan reconciliation = (%d, %v), want absent", size, ok)
	}
	if faultyHot.existsCalls != 0 {
		t.Errorf("hot existence checks = %d, want none for a cold row already in metadata", faultyHot.existsCalls)
	}
}

func TestHotOnlyMissingColdRowWaitsForLocalHotReplica(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := gateDailyA
	// A reader can receive metadata for the hot row before its local replica.
	// Keep the cold object queryable until the hot replica arrives.
	recordHotRow(t, m, path, manifestPartition)
	mustWrite(t, cold, path)
	coldSize, err := cold.StatFile(ctx, path)
	if err != nil {
		t.Fatalf("stat cold object: %v", err)
	}
	manifest := &fakeManifest{hot: hot, entries: map[string]int64{path: coldSize}}
	m.manifest = manifest
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}

	if _, _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("sync while the local hot replica is missing: %v", err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierCold {
		t.Fatalf("tier without local hot replica = %s, want cold so queries can read the object", got)
	}

	if err := cold.Delete(ctx, path); err != nil {
		t.Fatalf("remove cold object: %v", err)
	}
	if _, _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("sync while both local copies are missing: %v", err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierCold {
		t.Fatalf("tier while both local copies are missing = %s, want cold until hot replica arrives", got)
	}
	if _, err := m.metadata.db.ExecContext(ctx, `UPDATE tier_files SET migrated_at = ? WHERE path = ?`, time.Now().UTC().Add(-time.Hour), path); err != nil {
		t.Fatalf("age cold row for manifest reconciliation: %v", err)
	}

	mustWrite(t, hot, path)
	_, rows, err := m.syncColdTierMetadata(ctx)
	if err != nil {
		t.Fatalf("sync after local hot replica arrives: %v", err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierHot {
		t.Fatalf("tier after local hot replica arrives = %s, want hot", got)
	}
	foundHotRow := false
	for _, row := range rows {
		if row.Path == path {
			foundHotRow = row.Tier == TierHot && row.MigratedAt == nil
		}
	}
	if !foundHotRow {
		t.Fatalf("rows returned to manifest reconciliation = %+v; want the restored row marked hot", rows)
	}
	// A listing can lag the direct stat once the hot copy arrives. The stale
	// cold-row snapshot must not unlink the manifest's hot replica.
	mustWrite(t, cold, path)
	removed, err := m.migrator.ReconcileManifest(ctx, rows)
	if err != nil {
		t.Fatalf("reconcile manifest from returned rows: %v", err)
	}
	if removed != 0 || len(manifest.calls) != 0 {
		t.Fatalf("manifest reconciliation removed %d entries with calls %+v; want none", removed, manifest.calls)
	}
}
