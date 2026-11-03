package tiering

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

type listedColdObjects struct {
	*mockBackend
	objects []storage.ObjectInfo
}

func (l *listedColdObjects) ListObjects(context.Context, string) ([]storage.ObjectInfo, error) {
	return l.objects, nil
}

func TestSyncColdTierMetadataBatchesCacheInvalidationAndKeepsObjectTimestamps(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	const batchSize = 1000
	objects := make([]storage.ObjectInfo, 0, batchSize+3)
	firstStamp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := 0; i < batchSize+1; i++ {
		objects = append(objects, storage.ObjectInfo{
			Path:         fmt.Sprintf("db1/cpu/2024/03/15/cpu_%04d_daily.parquet", i),
			Size:         int64(i + 10),
			LastModified: firstStamp.Add(time.Duration(i) * time.Second),
		})
	}
	objects = append(objects,
		storage.ObjectInfo{Path: "db1/mem/2024/03/15/mem_a_daily.parquet", Size: 30, LastModified: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)},
		storage.ObjectInfo{Path: "db2/cpu/2024/03/15/cpu_a_daily.parquet", Size: 40, LastModified: time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)},
	)
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}

	ctx := context.Background()
	for _, scope := range [][2]string{{"db1", "cpu"}, {"db1", "mem"}, {"db2", "cpu"}} {
		if _, err := m.metadata.GetTiersForMeasurement(ctx, scope[0], scope[1]); err != nil {
			t.Fatalf("prime tier cache for %s/%s: %v", scope[0], scope[1], err)
		}
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	synced, err := m.syncColdTierMetadata(ctx)
	if err != nil {
		t.Fatalf("syncColdTierMetadata: %v", err)
	}
	if synced != len(objects) {
		t.Fatalf("synced = %d, want %d", synced, len(objects))
	}

	// One invalidation per database/measurement per committed batch: the
	// first batch is 1000 db1/cpu rows, the second holds the last db1/cpu
	// row plus db1/mem and db2/cpu. Per object would be 1003.
	m.metadata.tierCacheMu.RLock()
	after := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()
	if got := after - before; got != 4 {
		t.Errorf("tier cache generation advanced %d times for %d files in 2 batches; want one invalidation per scope per batch (4)", got, len(objects))
	}

	for _, index := range []int{0, batchSize, len(objects) - 2, len(objects) - 1} {
		object := objects[index]
		got, err := m.metadata.GetFile(ctx, object.Path)
		if err != nil {
			t.Fatalf("GetFile(%q): %v", object.Path, err)
		}
		if got == nil {
			t.Fatalf("GetFile(%q) returned no row", object.Path)
		}
		if got.Tier != TierCold {
			t.Errorf("%s tier = %s, want cold", object.Path, got.Tier)
		}
		if got.MigratedAt == nil || !got.MigratedAt.Equal(object.LastModified) {
			t.Errorf("%s migrated_at = %v, want the object's LastModified %v", object.Path, got.MigratedAt, object.LastModified)
		}
		if !got.CreatedAt.Equal(object.LastModified) {
			t.Errorf("%s created_at = %v, want the object's LastModified %v", object.Path, got.CreatedAt, object.LastModified)
		}
	}
}

func TestSyncColdTierMetadataContinuesAfterABatchWriteErrorAndInvalidatesOncePerScope(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	const rejected = "db1/cpu/2024/03/15/cpu_rejected_daily.parquet"
	objects := make([]storage.ObjectInfo, 0, coldTierMetadataBatchSize+2)
	for i := 0; i < coldTierMetadataBatchSize; i++ {
		path := fmt.Sprintf("db1/cpu/2024/03/15/cpu_%04d_daily.parquet", i)
		if i == coldTierMetadataBatchSize/2 {
			path = rejected
		}
		objects = append(objects, storage.ObjectInfo{
			Path:         path,
			Size:         int64(i + 10),
			LastModified: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Add(time.Duration(i) * time.Second),
		})
	}
	objects = append(objects,
		storage.ObjectInfo{Path: "db1/mem/2024/03/15/mem_after_daily.parquet", Size: 30, LastModified: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)},
		storage.ObjectInfo{Path: "db2/cpu/2024/03/15/cpu_after_daily.parquet", Size: 40, LastModified: time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)},
	)
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}

	for _, scope := range [][2]string{{"db1", "cpu"}, {"db1", "mem"}, {"db2", "cpu"}} {
		if _, err := m.metadata.GetTiersForMeasurement(ctx, scope[0], scope[1]); err != nil {
			t.Fatalf("prime tier cache for %s/%s: %v", scope[0], scope[1], err)
		}
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	if _, err := m.metadata.db.ExecContext(ctx, `
		CREATE TRIGGER reject_cold_sync_row BEFORE INSERT ON tier_files
		WHEN NEW.path = 'db1/cpu/2024/03/15/cpu_rejected_daily.parquet'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatalf("create test trigger: %v", err)
	}

	synced, err := m.syncColdTierMetadata(ctx)
	if err != nil {
		t.Fatalf("syncColdTierMetadata = %v; a failed row is logged and skipped, it does not fail the sync", err)
	}
	if synced != len(objects)-1 {
		t.Fatalf("syncColdTierMetadata synced = %d, want %d valid rows", synced, len(objects)-1)
	}
	for _, path := range []string{objects[0].Path, objects[coldTierMetadataBatchSize-1].Path, objects[coldTierMetadataBatchSize].Path, objects[coldTierMetadataBatchSize+1].Path} {
		if got, err := m.metadata.GetFile(ctx, path); err != nil || got == nil || got.Tier != TierCold {
			t.Errorf("GetFile(%q) = (%+v, %v), want a cold row", path, got, err)
		}
	}
	if got, err := m.metadata.GetFile(ctx, rejected); err != nil || got != nil {
		t.Errorf("GetFile(%q) = (%+v, %v), want no row", rejected, got, err)
	}

	m.metadata.tierCacheMu.RLock()
	after := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()
	if got := after - before; got != 3 {
		t.Errorf("tier cache generation advanced %d times for 3 touched scopes (one per batch each); want 3", got)
	}
}

func TestSyncColdTierMetadataCancellationInvalidatesCommittedAndFallbackWrites(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const rejected = "db2/mem/2024/03/15/mem_rejected_daily.parquet"
	objects := make([]storage.ObjectInfo, 0, coldTierMetadataBatchSize*2)
	for i := 0; i < coldTierMetadataBatchSize; i++ {
		objects = append(objects, storage.ObjectInfo{Path: fmt.Sprintf("db1/cpu/2024/03/15/cpu_%04d_daily.parquet", i)})
	}
	for i := 0; i < coldTierMetadataBatchSize; i++ {
		path := fmt.Sprintf("db2/mem/2024/03/15/mem_%04d_daily.parquet", i)
		if i == coldTierMetadataBatchSize/2 {
			path = rejected
		}
		objects = append(objects, storage.ObjectInfo{Path: path})
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}

	for _, scope := range [][2]string{{"db1", "cpu"}, {"db2", "mem"}} {
		if _, err := m.metadata.GetTiersForMeasurement(context.Background(), scope[0], scope[1]); err != nil {
			t.Fatalf("prime tier cache for %s/%s: %v", scope[0], scope[1], err)
		}
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	if _, err := m.metadata.db.ExecContext(context.Background(), `
		CREATE TRIGGER reject_cold_sync_row_before_cancel BEFORE INSERT ON tier_files
		WHEN NEW.path = 'db2/mem/2024/03/15/mem_rejected_daily.parquet'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatalf("create test trigger: %v", err)
	}

	type syncResult struct {
		synced int
		err    error
	}
	done := make(chan syncResult, 1)
	go func() {
		synced, err := m.syncColdTierMetadata(ctx)
		done <- syncResult{synced: synced, err: err}
	}()

	firstFallbackPath := objects[coldTierMetadataBatchSize].Path
	deadline := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		row, err := m.metadata.GetFile(context.Background(), firstFallbackPath)
		if err != nil {
			t.Fatalf("poll first fallback row: %v", err)
		}
		if row != nil {
			cancel()
			break
		}
		select {
		case result := <-done:
			t.Fatalf("sync finished before the fallback row appeared: synced=%d err=%v", result.synced, result.err)
		case <-deadline.C:
			cancel()
			select {
			case result := <-done:
				t.Fatalf("fallback row was not written before timeout: synced=%d err=%v", result.synced, result.err)
			case <-time.After(5 * time.Second):
				t.Fatal("sync did not stop after cancellation")
			}
		case <-ticker.C:
		}
	}

	result := <-done
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("sync error = %v, want context.Canceled", result.err)
	}
	if row, err := m.metadata.GetFile(context.Background(), firstFallbackPath); err != nil || row == nil || row.Tier != TierCold {
		t.Fatalf("first fallback row = (%+v, %v), want a persisted cold row", row, err)
	}
	if row, err := m.metadata.GetFile(context.Background(), rejected); err != nil || row != nil {
		t.Fatalf("rejected row = (%+v, %v), want no row", row, err)
	}

	m.metadata.tierCacheMu.RLock()
	after := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()
	if got := after - before; got != 2 {
		t.Errorf("tier cache generation advanced %d times for 2 touched scopes before cancellation; want one invalidation per scope", got)
	}
}

// The quarantine guard holds on the batch path: a listed object whose row is
// quarantined is neither written nor counted, an existing cold row keeps its
// migrated_at, and only the scope actually written is invalidated.
func TestSyncColdTierMetadataPreservesQuarantineAndExistingColdTimestamp(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	oldColdAt := time.Date(2024, 3, 15, 12, 30, 0, 0, time.UTC)
	oldCold := &FileMetadata{
		Path:          "db1/cpu/2024/03/15/cpu_existing_daily.parquet",
		Database:      "db1",
		Measurement:   "cpu",
		PartitionTime: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
		Tier:          TierCold,
		CreatedAt:     oldColdAt,
	}
	if wrote, err := m.metadata.RecordColdFile(ctx, oldCold, oldColdAt); err != nil || !wrote {
		t.Fatalf("seed existing cold row = (%t, %v), want written", wrote, err)
	}
	quarantined := &FileMetadata{
		Path:          "db9/exclusive/2024/03/15/exclusive_quarantined_daily.parquet",
		Database:      "db9",
		Measurement:   "exclusive",
		PartitionTime: oldCold.PartitionTime,
		Tier:          TierHot,
		CreatedAt:     oldColdAt,
	}
	if err := m.metadata.RecordFile(ctx, quarantined); err != nil {
		t.Fatal(err)
	}
	if err := m.metadata.QuarantineFile(ctx, quarantined.Path, "test quarantine"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.metadata.GetTiersForMeasurement(ctx, quarantined.Database, quarantined.Measurement); err != nil {
		t.Fatal(err)
	}
	m.metadata.tierCacheMu.RLock()
	cacheGenerationBefore := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()
	newPath := "db1/cpu/2024/03/15/cpu_new_daily.parquet"
	newAt := oldColdAt.Add(48 * time.Hour)
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: []storage.ObjectInfo{
		{Path: oldCold.Path, LastModified: newAt},
		{Path: quarantined.Path, LastModified: newAt},
		{Path: newPath, LastModified: newAt},
	}}

	synced, err := m.syncColdTierMetadata(ctx)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if synced != 1 {
		t.Fatalf("synced = %d, want only the new row (the quarantined one is not written or counted)", synced)
	}
	gotOld, err := m.metadata.GetFile(ctx, oldCold.Path)
	if err != nil {
		t.Fatal(err)
	}
	if gotOld.MigratedAt == nil || !gotOld.MigratedAt.Equal(oldColdAt) {
		t.Errorf("existing cold migrated_at = %v, want preserved %s", gotOld.MigratedAt, oldColdAt)
	}
	if !gotOld.CreatedAt.Equal(oldColdAt) {
		t.Errorf("existing cold created_at = %s, want preserved %s", gotOld.CreatedAt, oldColdAt)
	}
	gotQuarantined, err := m.metadata.GetFile(ctx, quarantined.Path)
	if err != nil {
		t.Fatal(err)
	}
	if gotQuarantined.Tier != TierHot || gotQuarantined.QuarantinedAt == nil || gotQuarantined.QuarantineReason != "test quarantine" {
		t.Errorf("quarantined row = %+v, want unchanged hot tier with quarantine metadata", gotQuarantined)
	}
	gotNew, err := m.metadata.GetFile(ctx, newPath)
	if err != nil {
		t.Fatal(err)
	}
	if gotNew.MigratedAt == nil || !gotNew.MigratedAt.Equal(newAt) {
		t.Errorf("new cold migrated_at = %v, want object LastModified %s", gotNew.MigratedAt, newAt)
	}
	if !gotNew.CreatedAt.Equal(newAt) {
		t.Errorf("new cold created_at = %s, want object LastModified %s", gotNew.CreatedAt, newAt)
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if delta := m.metadata.tierCacheGen - cacheGenerationBefore; delta != 1 {
		t.Errorf("generation advanced %d times, want one invalidation for the written scope", delta)
	}
	if _, ok := m.metadata.tierCache["db9/exclusive"]; !ok {
		t.Error("quarantined-only scope lost its resident cache entry")
	}
}

// A tier-cache fill that read before the sync committed and stores after it
// must be discarded, not installed over the committed rows.
func TestSyncColdTierMetadataDiscardsStaleCacheFill(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	path := "db1/cpu/2024/03/15/cpu_new_daily.parquet"
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: []storage.ObjectInfo{{Path: path}}}
	cacheKey := "db1/cpu"
	m.metadata.tierCacheMu.RLock()
	oldGeneration := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()
	staleTiers, err := m.metadata.queryTierSet(ctx, "db1", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if len(staleTiers) != 0 {
		t.Fatalf("pre-sync tiers = %v, want empty", staleTiers)
	}
	ready := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(ready)
		<-release
		m.metadata.storeTierCacheIfUnchanged(cacheKey, staleTiers, oldGeneration)
	}()
	<-ready

	if _, err := m.syncColdTierMetadata(ctx); err != nil {
		close(release)
		wg.Wait()
		t.Fatal(err)
	}
	close(release)
	wg.Wait()
	m.metadata.tierCacheMu.RLock()
	_, staleInstalled := m.metadata.tierCache[cacheKey]
	m.metadata.tierCacheMu.RUnlock()
	if staleInstalled {
		t.Fatal("stale pre-sync fill was installed after invalidation")
	}

	got, err := m.metadata.GetTiersForMeasurement(ctx, "db1", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if !got[TierCold] {
		t.Fatalf("post-sync tiers = %v, want cold", got)
	}
}

// RecordColdFile still invalidates on an identical upsert: SQLite reports one
// row affected for a DO UPDATE that changes nothing, so a re-run is not
// mistaken for a quarantined row.
func TestRecordColdFileIdenticalUpsertStillInvalidates(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	stamp := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	file := &FileMetadata{
		Path:          "db1/cpu/2024/03/15/cpu_same_daily.parquet",
		Database:      "db1",
		Measurement:   "cpu",
		PartitionTime: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
		Tier:          TierCold,
		SizeBytes:     17,
		CreatedAt:     stamp,
	}
	if wrote, err := m.metadata.RecordColdFile(ctx, file, stamp); err != nil || !wrote {
		t.Fatalf("first RecordColdFile = (%v, %v), want write", wrote, err)
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()
	if wrote, err := m.metadata.RecordColdFile(ctx, file, stamp); err != nil || !wrote {
		t.Fatalf("identical RecordColdFile upsert = (%v, %v), want successful upsert", wrote, err)
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if got := m.metadata.tierCacheGen - before; got != 1 {
		t.Errorf("identical successful upsert advanced generation %d times, want 1", got)
	}
}

// Observed from inside the walk, between two batches: the first batch's
// scope is invalidated as soon as it commits, not when the whole walk ends.
// The hot-only object after the batch boundary makes the walk ask the hot
// backend about it, which is the vantage point.
func TestSyncColdTierMetadataInvalidatesEachBatchAsItCommits(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db9", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}
	// Exactly one batch of plain db1/cpu objects before the probe: none is
	// cleaned, reserved or unparseable, so the flush at the 1000th lands
	// before the walk reaches the probe.
	objects := make([]storage.ObjectInfo, 0, coldTierMetadataBatchSize+1)
	for i := 0; i < coldTierMetadataBatchSize; i++ {
		objects = append(objects, storage.ObjectInfo{Path: fmt.Sprintf("db1/cpu/2024/03/15/cpu_%04d_daily.parquet", i)})
	}
	probe := "db9/pinned/2024/03/15/pinned_daily.parquet"
	objects = append(objects, storage.ObjectInfo{Path: probe})
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}

	if tiers, err := m.metadata.GetTiersForMeasurement(ctx, "db1", "cpu"); err != nil || len(tiers) != 0 {
		t.Fatalf("prime db1/cpu = (%v, %v), want an empty cached tier set", tiers, err)
	}
	observed := false
	m.hotBackend = &lagListingHotBackend{mockBackend: hot, onExists: func(p string) {
		if p != probe {
			return
		}
		observed = true
		tiers, err := m.metadata.GetTiersForMeasurement(ctx, "db1", "cpu")
		if err != nil {
			t.Errorf("GetTiersForMeasurement between batches: %v", err)
			return
		}
		if !tiers[TierCold] {
			t.Errorf("db1/cpu tiers after its batch committed = %v, want cold: the pre-sync cache entry was still served", tiers)
		}
	}}

	if _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if !observed {
		t.Fatal("the walk never checked the hot-only object, so nothing was observed between batches")
	}
}

// The manifest sweep loads its own cold rows, once, on a primary that gets
// as far as the sweep — and nowhere else: not on a gated reader, and not on
// a cycle whose cold sync failed (#1188).
func TestRunCycleLoadsSweepRowsOnlyWhereTheSweepRuns(t *testing.T) {
	for _, tc := range []struct {
		name      string
		primary   bool
		listErr   error
		wantLoads int
	}{
		{"primary", true, nil, 1},
		{"gated reader", false, nil, 0},
		{"primary with a failed cold sync", true, errors.New("cold tier unreachable"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, hot, cold, gate, cleanup := setupGatedTest(t)
			defer cleanup()
			ctx := context.Background()
			gate.primary.Store(tc.primary)
			fake := &fakeManifest{hot: hot, entries: map[string]int64{gateDailyA: 7}}
			m.manifest = fake
			mustWrite(t, cold, gateDailyA)
			if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(gateDailyA), time.Now().Add(-2*time.Hour)); err != nil {
				t.Fatal(err)
			}
			cold.listErr = tc.listErr
			loads := 0
			m.sweepColdRows = func(ctx context.Context) ([]FileMetadata, error) {
				loads++
				return m.metadata.GetFilesInTier(ctx, TierCold)
			}

			_ = m.runCycle(ctx)
			if loads != tc.wantLoads {
				t.Fatalf("sweep row loads = %d, want %d", loads, tc.wantLoads)
			}
			swept := false
			for _, c := range fake.calls {
				if c.reason == manifestReasonSweep {
					swept = true
				}
			}
			if swept != (tc.wantLoads == 1) {
				t.Fatalf("manifest swept = %v, want %v", swept, tc.wantLoads == 1)
			}
		})
	}
}

// A failure to load the sweep's rows skips the sweep only; this cycle's
// migration has already run.
func TestRunCycleSkipsOnlyTheSweepWhenItsRowsCannotBeLoaded(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(true)
	fake := &fakeManifest{hot: hot, entries: map[string]int64{gateDailyA: 7}}
	m.manifest = fake
	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(gateDailyA), time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	m.sweepColdRows = func(context.Context) ([]FileMetadata, error) {
		return nil, errors.New("database is locked")
	}

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}
	for _, c := range fake.calls {
		if c.reason == manifestReasonSweep {
			t.Fatalf("manifest swept without its rows: %+v", fake.calls)
		}
	}
	if _, ok := fake.entries[gateDailyA]; !ok {
		t.Fatal("manifest entry removed although the sweep could not load its rows")
	}
}

// The revert loop flips a missing cold object's row back to hot only for a
// hot-only database. Elsewhere a cold row whose object is gone is reported
// and left as it is (#683), even when a hot copy and a matching manifest
// entry exist.
func TestSyncColdTierMetadataLeavesMissingColdRowOutsideHotOnly(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := gateDailyA
	mustWrite(t, hot, path)
	size, err := hot.StatFile(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	row := coldRowFor(path)
	row.SizeBytes = size
	if _, err := m.metadata.RecordColdFile(ctx, row, time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	m.manifest = &fakeManifest{hot: hot, entries: map[string]int64{path: size}}
	m.coldBackend = &listedColdObjects{mockBackend: cold}

	if _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold: only a hot-only database's row is reverted", got)
	}
}

// The revert loop compares the manifest entry with the row's own size, now
// read from the sync's path -> size set rather than a full row: a manifest
// entry for a different file at the same key keeps the row cold.
func TestSyncColdTierMetadataHotOnlyRevertNeedsTheRowsSize(t *testing.T) {
	for _, tc := range []struct {
		name         string
		manifestSize func(rowSize int64) int64
		want         Tier
	}{
		{"manifest matches the row", func(n int64) int64 { return n }, TierHot},
		{"manifest holds another size", func(n int64) int64 { return n + 1 }, TierCold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, hot, cold, cleanup := setupIntegrationTest(t, true)
			defer cleanup()
			ctx := context.Background()
			path := gateDailyA
			mustWrite(t, hot, path)
			row := coldRowFor(path)
			row.SizeBytes = 41
			if _, err := m.metadata.RecordColdFile(ctx, row, time.Now().Add(-2*time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
				t.Fatalf("set hot-only policy: %v", err)
			}
			m.manifest = &fakeManifest{hot: hot, entries: map[string]int64{path: tc.manifestSize(row.SizeBytes)}}
			m.coldBackend = &listedColdObjects{mockBackend: cold}

			if _, err := m.syncColdTierMetadata(ctx); err != nil {
				t.Fatalf("sync: %v", err)
			}
			if got := fileMeta(t, m, path).Tier; got != tc.want {
				t.Fatalf("tier = %s, want %s", got, tc.want)
			}
		})
	}
}
