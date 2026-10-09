package tiering

import (
	"context"
	"fmt"
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
	if err != nil {
		t.Fatalf("syncColdTierMetadata: %v", err)
	}
	if synced != 2 || len(rows) != 2 {
		t.Fatalf("syncColdTierMetadata = (%d synced, %d rows), want the two valid rows", synced, len(rows))
	}
	for _, path := range []string{objects[0].Path, objects[2].Path} {
		if got, err := m.metadata.GetFile(ctx, path); err != nil || got == nil || got.Tier != TierCold {
			t.Errorf("GetFile(%q) = (%+v, %v), want a cold row", path, got, err)
		}
	}
	if got, err := m.metadata.GetFile(ctx, rejected); err != nil || got != nil {
		t.Errorf("GetFile(%q) = (%+v, %v), want no row", rejected, got, err)
	}
}
