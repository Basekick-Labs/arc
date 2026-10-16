package tiering

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

func TestTierBatchCommitNotStartedClassifiesCanceledTransactionStates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()
	for name, err := range map[string]error{
		"context error before driver commit": context.Canceled,
		"automatic rollback completed first": sql.ErrTxDone,
	} {
		t.Run(name, func(t *testing.T) {
			if !tierBatchCommitNotStarted(ctx, err) {
				t.Errorf("tierBatchCommitNotStarted(%v) = false, want true", err)
			}
		})
	}
	if tierBatchCommitNotStarted(context.Background(), context.Canceled) {
		t.Error("uncanceled context with driver cancellation error classified as pre-driver rollback")
	}
}

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
	objects := make([]storage.ObjectInfo, 0, 2*batchSize+3)
	firstStamp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := 0; i < 2*batchSize+1; i++ {
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

	synced, rows, err := m.syncColdTierMetadata(ctx)
	if err != nil {
		t.Fatalf("syncColdTierMetadata: %v", err)
	}
	if synced != len(objects) {
		t.Fatalf("synced = %d, want %d", synced, len(objects))
	}
	if len(rows) != len(objects) {
		t.Fatalf("cold rows = %d, want %d", len(rows), len(objects))
	}

	m.metadata.tierCacheMu.RLock()
	after := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()
	if got := after - before; got != 3 {
		t.Errorf("tier cache generation advanced %d times for %d files in 3 database/measurement pairs; want one invalidation per pair", got, len(objects))
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

func TestSyncColdTierMetadataContinuesAfterABatchWriteError(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	const rejected = "db1/cpu/2024/03/15/cpu_rejected_daily.parquet"
	objects := []storage.ObjectInfo{
		{Path: "db1/cpu/2024/03/15/cpu_before_daily.parquet", Size: 10, LastModified: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
		{Path: rejected, Size: 20, LastModified: time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)},
		{Path: "db1/cpu/2024/03/15/cpu_after_daily.parquet", Size: 30, LastModified: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)},
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}
	if _, err := m.metadata.db.ExecContext(ctx, `
		CREATE TRIGGER reject_cold_sync_row BEFORE INSERT ON tier_files
		WHEN NEW.path = 'db1/cpu/2024/03/15/cpu_rejected_daily.parquet'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatalf("create test trigger: %v", err)
	}

	synced, rows, err := m.syncColdTierMetadata(ctx)
	if err == nil {
		t.Fatal("syncColdTierMetadata returned success after a persistent row failure")
	}
	if synced != 2 || len(rows) != 2 {
		t.Fatalf("syncColdTierMetadata = (%d synced, %d rows), want the two valid rows", synced, len(rows))
	}
	for _, path := range []string{objects[0].Path, objects[2].Path} {
		if got, err := m.metadata.GetFile(ctx, path); err != nil || got == nil || got.Tier != TierCold {
			t.Errorf("GetFile(%q) = (%+v, %v), want a cold row", path, got, err)
		}
	}
	for _, index := range []int{0, 2} {
		got, err := m.metadata.GetFile(ctx, objects[index].Path)
		if err != nil {
			t.Fatal(err)
		}
		if !got.CreatedAt.Equal(objects[index].LastModified) || got.MigratedAt == nil || !got.MigratedAt.Equal(objects[index].LastModified) {
			t.Errorf("fallback timestamps for %q = (created %s, migrated %v), want %s", objects[index].Path, got.CreatedAt, got.MigratedAt, objects[index].LastModified)
		}
	}
	if got, err := m.metadata.GetFile(ctx, rejected); err != nil || got != nil {
		t.Errorf("GetFile(%q) = (%+v, %v), want no row", rejected, got, err)
	}
}

func TestSyncColdTierMetadataFallbackInvalidatesEachWrittenScopeOnce(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	const rejected = "db9/exclusive/2024/03/15/exclusive_rejected_daily.parquet"
	objects := make([]storage.ObjectInfo, 2003)
	for i := range objects {
		path := fmt.Sprintf("db1/cpu/2024/03/15/cpu_%04d_daily.parquet", i)
		if i == 1002 {
			path = rejected
		}
		if i == 1500 {
			path = "db1/mem/2024/03/15/mem_1500_daily.parquet"
		}
		objects[i] = storage.ObjectInfo{Path: path, Size: int64(i + 1), LastModified: time.Date(2026, 1, 1, 0, 0, i%60, 0, time.UTC)}
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}
	if _, err := m.metadata.db.ExecContext(ctx, `
		CREATE TRIGGER reject_cold_sync_row BEFORE INSERT ON tier_files
		WHEN NEW.path = 'db9/exclusive/2024/03/15/exclusive_rejected_daily.parquet'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	for _, scope := range [][2]string{{"db1", "cpu"}, {"db1", "mem"}, {"db9", "exclusive"}} {
		if _, err := m.metadata.GetTiersForMeasurement(ctx, scope[0], scope[1]); err != nil {
			t.Fatalf("prime cache %s/%s: %v", scope[0], scope[1], err)
		}
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	synced, rows, err := m.syncColdTierMetadata(ctx)
	if err == nil {
		t.Fatal("expected partial-sync error for the rejected row")
	}
	if synced != len(objects)-1 || len(rows) != len(objects)-1 {
		t.Fatalf("sync = (%d rows, %d metadata rows), want %d successful rows", synced, len(rows), len(objects)-1)
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if got := m.metadata.tierCacheGen - before; got != 2 {
		t.Errorf("cache generation advanced %d times, want once for each of the two written scopes", got)
	}
	for _, key := range []string{"db1/cpu", "db1/mem"} {
		if _, ok := m.metadata.tierCache[key]; ok {
			t.Errorf("written scope %q retained a stale cache entry", key)
		}
	}
	if _, ok := m.metadata.tierCache["db9/exclusive"]; !ok {
		t.Error("scope containing only the rejected row lost its resident cache entry")
	}
}

func TestSyncColdTierMetadataStagesAtMostOneBatch(t *testing.T) {
	for _, size := range []int{0, 1, coldTierMetadataBatchSize, coldTierMetadataBatchSize + 1} {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			m, _, cold, cleanup := setupIntegrationTest(t, true)
			defer cleanup()
			objects := make([]storage.ObjectInfo, size)
			for i := range objects {
				objects[i] = storage.ObjectInfo{
					Path:         fmt.Sprintf("db1/cpu/2024/03/15/cpu_%04d_daily.parquet", i),
					LastModified: time.Date(2026, 2, 1, 0, 0, i%60, 0, time.UTC),
				}
			}
			m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}
			var batchLengths []int
			var batchCapacities []int
			synced, rows, err := m.syncColdTierMetadataWithHooks(context.Background(), &coldSyncHooks{onBatch: func(length, capacity int) {
				batchLengths = append(batchLengths, length)
				batchCapacities = append(batchCapacities, capacity)
			}})
			if err != nil {
				t.Fatalf("syncColdTierMetadata: %v", err)
			}
			if synced != size || len(rows) != size {
				t.Fatalf("sync returned (%d, %d rows), want %d", synced, len(rows), size)
			}
			wantBatches := 0
			if size > 0 {
				wantBatches = (size + coldTierMetadataBatchSize - 1) / coldTierMetadataBatchSize
			}
			if len(batchLengths) != wantBatches {
				t.Fatalf("flushed %d batches with lengths %v, want %d batches", len(batchLengths), batchLengths, wantBatches)
			}
			for i, length := range batchLengths {
				wantLength := min(coldTierMetadataBatchSize, size-i*coldTierMetadataBatchSize)
				if length != wantLength {
					t.Errorf("batch %d length = %d, want %d", i, length, wantLength)
				}
			}
			for i, length := range batchLengths {
				if length > coldTierMetadataBatchSize {
					t.Errorf("batch %d length = %d, exceeds %d", i, length, coldTierMetadataBatchSize)
				}
				if batchCapacities[i] > coldTierMetadataBatchSize {
					t.Errorf("batch %d buffer capacity = %d, exceeds %d", i, batchCapacities[i], coldTierMetadataBatchSize)
				}
			}
		})
	}
}

func TestSyncColdTierMetadataCancellationAfterCommitInvalidatesBeforeReturn(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := "db1/cpu/2024/03/15/cpu_committed_daily.parquet"
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: []storage.ObjectInfo{{Path: path, LastModified: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)}}}
	if _, err := m.metadata.GetTiersForMeasurement(context.Background(), "db1", "cpu"); err != nil {
		t.Fatal(err)
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	synced, rows, err := m.syncColdTierMetadataWithHooks(ctx, &coldSyncHooks{afterBatchPersist: cancel})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sync error = %v, want context.Canceled", err)
	}
	if synced != 1 || len(rows) != 1 {
		t.Fatalf("sync returned (%d, %d rows), want committed row", synced, len(rows))
	}
	if got, err := m.metadata.GetFile(context.Background(), path); err != nil || got == nil {
		t.Fatalf("committed row = (%+v, %v), want present", got, err)
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if got := m.metadata.tierCacheGen - before; got != 1 {
		t.Errorf("generation advanced %d times, want one invalidation before return", got)
	}
}

func TestSyncColdTierMetadataCancellationAfterCommittedBatchLeavesNextBatchPending(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	objects := make([]storage.ObjectInfo, coldTierMetadataBatchSize+1)
	for i := range objects {
		objects[i] = storage.ObjectInfo{Path: fmt.Sprintf("db1/cpu/2024/03/15/cpu_cancel_%04d_daily.parquet", i)}
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}
	if _, err := m.metadata.GetTiersForMeasurement(context.Background(), "db1", "cpu"); err != nil {
		t.Fatal(err)
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	synced, rows, err := m.syncColdTierMetadataWithHooks(ctx, &coldSyncHooks{afterBatchPersist: cancel})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sync error = %v, want context.Canceled", err)
	}
	if synced != coldTierMetadataBatchSize || len(rows) != coldTierMetadataBatchSize {
		t.Fatalf("sync returned (%d, %d rows), want only the first committed batch", synced, len(rows))
	}
	if got, err := m.metadata.GetFile(context.Background(), objects[0].Path); err != nil || got == nil {
		t.Errorf("first batch row = (%+v, %v), want persisted", got, err)
	}
	if got, err := m.metadata.GetFile(context.Background(), objects[len(objects)-1].Path); err != nil || got != nil {
		t.Errorf("pending next-batch row = (%+v, %v), want absent", got, err)
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if delta := m.metadata.tierCacheGen - before; delta != 1 {
		t.Errorf("generation advanced %d times, want one invalidation for the committed batch", delta)
	}
}

func TestSyncColdTierMetadataUncertainCommitStopsFallbackAndInvalidatesPriorScopes(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	objects := make([]storage.ObjectInfo, coldTierMetadataBatchSize+1)
	for i := 0; i < coldTierMetadataBatchSize; i++ {
		objects[i] = storage.ObjectInfo{Path: fmt.Sprintf("db1/cpu/2024/03/15/cpu_confirmed_%04d_daily.parquet", i)}
	}
	objects[len(objects)-1] = storage.ObjectInfo{Path: "db2/mem/2024/03/15/mem_uncertain_daily.parquet"}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}
	for _, scope := range [][2]string{{"db1", "cpu"}, {"db2", "mem"}} {
		if _, err := m.metadata.GetTiersForMeasurement(context.Background(), scope[0], scope[1]); err != nil {
			t.Fatalf("prime cache %s/%s: %v", scope[0], scope[1], err)
		}
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()
	commitCount := 0
	fallbackWrites := 0

	synced, rows, err := m.syncColdTierMetadataWithHooks(context.Background(), &coldSyncHooks{
		afterBatchCommit: func() error {
			commitCount++
			if commitCount == 2 {
				return errors.New("injected lost commit acknowledgement")
			}
			return nil
		},
		afterFallbackWrite: func() { fallbackWrites++ },
	})
	if err == nil || !strings.Contains(err.Error(), "outcome is unresolved") {
		t.Fatalf("sync error = %v, want unresolved transaction outcome", err)
	}
	if synced != coldTierMetadataBatchSize || len(rows) != coldTierMetadataBatchSize {
		t.Fatalf("sync returned (%d, %d rows), want only first confirmed batch", synced, len(rows))
	}
	if fallbackWrites != 0 {
		t.Errorf("fallback writes = %d, want none after an unresolved commit", fallbackWrites)
	}
	for _, path := range []string{objects[0].Path, objects[len(objects)-1].Path} {
		if got, err := m.metadata.GetFile(context.Background(), path); err != nil || got == nil {
			t.Errorf("row %q = (%+v, %v), want persisted by injected post-commit acknowledgement loss", path, got, err)
		}
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if delta := m.metadata.tierCacheGen - before; delta != 2 {
		t.Errorf("generation advanced %d times, want one invalidation for each confirmed or possibly committed scope", delta)
	}
}

func TestSyncColdTierMetadataCancellationDuringFallbackFlushesSuccessfulWrites(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	objects := []storage.ObjectInfo{
		{Path: "db1/cpu/2024/03/15/cpu_first_daily.parquet"},
		{Path: "db1/cpu/2024/03/15/cpu_rejected_daily.parquet"},
		{Path: "db1/cpu/2024/03/15/cpu_never_attempted_daily.parquet"},
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}
	if _, err := m.metadata.db.ExecContext(context.Background(), `
		CREATE TRIGGER reject_fallback_row BEFORE INSERT ON tier_files
		WHEN NEW.path = 'db1/cpu/2024/03/15/cpu_rejected_daily.parquet'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatal(err)
	}
	var before uint64
	m.metadata.tierCacheMu.RLock()
	before = m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	synced, rows, err := m.syncColdTierMetadataWithHooks(ctx, &coldSyncHooks{afterFallbackWrite: cancel})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sync error = %v, want context.Canceled", err)
	}
	if synced != 1 || len(rows) != 1 || rows[0].Path != objects[0].Path {
		t.Fatalf("sync = (%d, %#v), want only first fallback write", synced, rows)
	}
	if got, err := m.metadata.GetFile(context.Background(), objects[0].Path); err != nil || got == nil {
		t.Errorf("first fallback row = (%+v, %v), want present", got, err)
	}
	for _, path := range []string{objects[1].Path, objects[2].Path} {
		if got, err := m.metadata.GetFile(context.Background(), path); err != nil || got != nil {
			t.Errorf("uncommitted fallback row %q = (%+v, %v), want absent", path, got, err)
		}
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if got := m.metadata.tierCacheGen - before; got != 1 {
		t.Errorf("generation advanced %d times, want one invalidation", got)
	}
}

func TestSyncColdTierMetadataCancellationAfterLastFallbackWriteIsReturned(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := "db1/cpu/2024/03/15/cpu_last_fallback_daily.parquet"
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: []storage.ObjectInfo{{Path: path}}}
	if _, err := m.metadata.GetTiersForMeasurement(context.Background(), "db1", "cpu"); err != nil {
		t.Fatal(err)
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	synced, rows, err := m.syncColdTierMetadataWithHooks(ctx, &coldSyncHooks{
		beforeBatchCommit:  func() error { return errors.New("force batch fallback") },
		afterFallbackWrite: cancel,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sync error = %v, want context.Canceled after the final fallback row", err)
	}
	if synced != 1 || len(rows) != 1 || rows[0].Path != path {
		t.Fatalf("sync = (%d, %#v), want the committed final fallback row", synced, rows)
	}
	if got, err := m.metadata.GetFile(context.Background(), path); err != nil || got == nil {
		t.Fatalf("last fallback row = (%+v, %v), want persisted", got, err)
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if delta := m.metadata.tierCacheGen - before; delta != 1 {
		t.Errorf("generation advanced %d times, want one invalidation", delta)
	}
}

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

	synced, rows, err := m.syncColdTierMetadata(ctx)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if synced != 1 || len(rows) != 2 {
		t.Fatalf("sync = (%d synced, %d rows), want the new row plus existing cold row only", synced, len(rows))
	}
	for _, row := range rows {
		if row.Path == quarantined.Path {
			t.Fatal("quarantined row appeared in cold sync results")
		}
	}
	gotOld, err := m.metadata.GetFile(ctx, oldCold.Path)
	if err != nil {
		t.Fatal(err)
	}
	if gotOld.MigratedAt == nil || !gotOld.MigratedAt.Equal(oldColdAt) {
		t.Errorf("existing cold migrated_at = %v, want preserved %s", gotOld.MigratedAt, oldColdAt)
	}
	gotQuarantined, err := m.metadata.GetFile(ctx, quarantined.Path)
	if err != nil {
		t.Fatal(err)
	}
	if gotQuarantined.Tier != TierHot || gotQuarantined.QuarantinedAt == nil || gotQuarantined.QuarantineReason != "test quarantine" {
		t.Errorf("quarantined row = %+v, want unchanged hot tier with quarantine metadata", gotQuarantined)
	}
	if !gotOld.CreatedAt.Equal(oldColdAt) {
		t.Errorf("existing cold created_at = %s, want preserved %s", gotOld.CreatedAt, oldColdAt)
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

func TestSyncColdTierMetadataBoundsPartialFailureErrorDetails(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	objects := make([]storage.ObjectInfo, 30)
	for i := range objects {
		objects[i] = storage.ObjectInfo{Path: fmt.Sprintf("db1/cpu/2024/03/15/cpu_rejected_%02d_daily.parquet", i)}
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}
	if _, err := m.metadata.db.ExecContext(context.Background(), `
		CREATE TRIGGER reject_partial_error_batch BEFORE INSERT ON tier_files
		WHEN NEW.path LIKE 'db1/cpu/2024/03/15/cpu_rejected_%'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatal(err)
	}

	_, rows, err := m.syncColdTierMetadata(context.Background())
	if err == nil {
		t.Fatal("sync error = nil, want partial fallback error")
	}
	if len(rows) != 0 {
		t.Fatalf("synced rows = %d, want zero", len(rows))
	}
	if !strings.Contains(err.Error(), "failed to record 30 cold metadata rows") || !strings.Contains(err.Error(), objects[0].Path) {
		t.Errorf("error %q does not report count and first failure", err)
	}
	if strings.Contains(err.Error(), objects[len(objects)-1].Path) || len(err.Error()) > 600 {
		t.Errorf("partial failure error retained per-row details (len=%d): %q", len(err.Error()), err)
	}
}

func TestSyncColdTierMetadataCancelsBeforeCommitWithoutFallbackWrites(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	objects := []storage.ObjectInfo{
		{Path: "db1/cpu/2024/03/15/cpu_first_daily.parquet"},
		{Path: "db1/cpu/2024/03/15/cpu_second_daily.parquet"},
	}
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: objects}
	var before uint64
	m.metadata.tierCacheMu.RLock()
	before = m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	synced, rows, err := m.syncColdTierMetadataWithHooks(ctx, &coldSyncHooks{beforeBatchCommit: func() error {
		cancel()
		return ctx.Err()
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sync error = %v, want context.Canceled", err)
	}
	if synced != 0 || len(rows) != 0 {
		t.Fatalf("sync = (%d, %d rows), want no confirmed writes", synced, len(rows))
	}
	for _, object := range objects {
		if got, err := m.metadata.GetFile(context.Background(), object.Path); err != nil || got != nil {
			t.Errorf("pre-commit row %q = (%+v, %v), want absent", object.Path, got, err)
		}
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if delta := m.metadata.tierCacheGen - before; delta != 0 {
		t.Errorf("generation advanced %d times, want no invalidation for confirmed rollback", delta)
	}
}

func TestRecordColdFilesBatchCancellationAtCommitIsConfirmedRollback(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	file := FileMetadata{
		Path:        "db1/cpu/2024/03/15/cpu_commit_cancel_daily.parquet",
		Database:    "db1",
		Measurement: "cpu",
		Tier:        TierCold,
	}
	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	notWritten, scopes, outcomeUnknown, err := m.metadata.recordColdFilesBatch(ctx, []FileMetadata{file}, time.Time{}, func() error {
		cancel()
		return nil // Reach database/sql's Tx.Commit context check.
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("batch error = %v, want context.Canceled", err)
	}
	if outcomeUnknown {
		t.Fatal("batch outcome is unresolved, want confirmed pre-driver rollback")
	}
	if len(notWritten) != 0 || len(scopes) != 0 {
		t.Fatalf("batch returned notWritten=%v scopes=%v, want neither after confirmed rollback", notWritten, scopes)
	}
	if got, err := m.metadata.GetFile(context.Background(), file.Path); err != nil || got != nil {
		t.Fatalf("canceled batch row = (%+v, %v), want absent", got, err)
	}
	m.metadata.tierCacheMu.RLock()
	defer m.metadata.tierCacheMu.RUnlock()
	if delta := m.metadata.tierCacheGen - before; delta != 0 {
		t.Errorf("generation advanced %d times, want no invalidation for confirmed rollback", delta)
	}
}

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

	if _, _, err := m.syncColdTierMetadata(ctx); err != nil {
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
	m.metadata.tierCacheMu.RLock()
	entry := m.metadata.tierCache[cacheKey]
	m.metadata.tierCacheMu.RUnlock()
	if entry == nil || !entry.tiers[TierCold] {
		t.Fatal("post-invalidation fill did not install the committed tier set")
	}
}

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

type observedManifest struct {
	*fakeManifest
	lookups int
}

func (m *observedManifest) ManifestEntry(path string) (int64, bool) {
	m.lookups++
	return m.fakeManifest.ManifestEntry(path)
}

func TestRunCycleSkipsManifestReconciliationAfterPartialColdSync(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	settled := time.Now().Add(-manifestSettle - time.Hour)
	existing := gateDailyA
	mustWrite(t, cold, existing)
	existingRow := &FileMetadata{
		Path:          existing,
		Database:      "db1",
		Measurement:   "cpu",
		PartitionTime: manifestPartition,
		Tier:          TierCold,
		SizeBytes:     4,
	}
	if wrote, err := m.metadata.RecordColdFile(ctx, existingRow, settled); err != nil || !wrote {
		t.Fatalf("seed existing cold row: wrote=%v err=%v", wrote, err)
	}
	const orphan = "db1/cpu/2024/03/15/cpu_orphan_daily.parquet"
	mustWrite(t, cold, orphan)
	mustWrite(t, hot, orphan)
	orphanRow := &FileMetadata{
		Path:          orphan,
		Database:      "db1",
		Measurement:   "cpu",
		PartitionTime: manifestPartition,
		Tier:          TierCold,
		SizeBytes:     5,
	}
	if wrote, err := m.metadata.RecordColdFile(ctx, orphanRow, time.Now().Add(-time.Hour)); err != nil || !wrote {
		t.Fatalf("seed recent cold orphan row: wrote=%v err=%v", wrote, err)
	}
	const migrationCandidate = "db1/cpu/2024/03/15/cpu_migration_candidate_daily.parquet"
	mustWrite(t, hot, migrationCandidate)
	candidateRow := &FileMetadata{
		Path:          migrationCandidate,
		Database:      "db1",
		Measurement:   "cpu",
		PartitionTime: manifestPartition,
		Tier:          TierHot,
		SizeBytes:     6,
	}
	if err := m.metadata.RecordFile(ctx, candidateRow); err != nil {
		t.Fatalf("seed hot migration candidate row: %v", err)
	}
	const rejected = "db1/cpu/2024/03/15/cpu_rejected_daily.parquet"
	listed := &listedColdObjects{mockBackend: cold, objects: []storage.ObjectInfo{
		{Path: existing, Size: 4, LastModified: settled},
		{Path: orphan, Size: 5, LastModified: time.Now().Add(-time.Hour)},
		{Path: rejected, Size: 9, LastModified: settled},
	}}
	m.coldBackend = listed
	if _, err := m.metadata.db.ExecContext(ctx, `
		CREATE TRIGGER reject_partial_sync BEFORE INSERT ON tier_files
		WHEN NEW.path = 'db1/cpu/2024/03/15/cpu_rejected_daily.parquet'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	manifest := &observedManifest{fakeManifest: &fakeManifest{
		hot:     hot,
		entries: map[string]int64{existing: 4},
	}}
	m.manifest = manifest

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}
	if manifest.lookups != 0 || len(manifest.calls) != 0 {
		t.Fatalf("manifest reconciliation ran after partial cold sync: lookups=%d deletes=%d", manifest.lookups, len(manifest.calls))
	}
	if exists, err := hot.Exists(ctx, orphan); err != nil || !exists {
		t.Errorf("orphan hot copy after failed sync = (%v, %v), want retained", exists, err)
	}
	if exists, err := cold.Exists(ctx, migrationCandidate); err != nil || exists {
		t.Errorf("migration candidate cold copy after failed sync = (%v, %v), want no migration", exists, err)
	}
	if got, err := m.metadata.GetFile(ctx, migrationCandidate); err != nil || got == nil || got.Tier != TierHot {
		t.Errorf("migration candidate metadata after failed sync = (%+v, %v), want hot row", got, err)
	}
}
