package tiering

import (
	"context"
	"errors"
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

func TestAppendColdTierMetadataBatchKeepsBufferBounded(t *testing.T) {
	batch := make([]FileMetadata, 0, coldTierMetadataBatchSize)
	maxBuffered := 0
	flushes := 0

	for i := 0; i < coldTierMetadataBatchSize*2+17; i++ {
		var full bool
		batch, full = appendColdTierMetadataBatch(batch, FileMetadata{Path: fmt.Sprintf("file-%d", i)})
		if len(batch) > maxBuffered {
			maxBuffered = len(batch)
		}
		if full {
			if len(batch) != coldTierMetadataBatchSize {
				t.Fatalf("full batch has %d rows, want %d", len(batch), coldTierMetadataBatchSize)
			}
			flushes++
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		flushes++
	}
	if maxBuffered != coldTierMetadataBatchSize {
		t.Errorf("maximum buffered rows = %d, want %d", maxBuffered, coldTierMetadataBatchSize)
	}
	if flushes != 3 {
		t.Errorf("flushes = %d, want 3", flushes)
	}
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

	synced, rows, err := m.syncColdTierMetadata(ctx)
	if err != nil {
		t.Fatalf("syncColdTierMetadata: %v", err)
	}
	if synced != len(objects)-1 {
		t.Fatalf("syncColdTierMetadata synced = %d, want %d valid rows", synced, len(objects)-1)
	}
	if len(rows) != len(objects)-1 {
		t.Fatalf("cold rows = %d, want %d valid rows", len(rows), len(objects)-1)
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
		t.Errorf("tier cache generation advanced %d times for 3 touched scopes; want one invalidation per scope", got)
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
		rows   []FileMetadata
		err    error
	}
	done := make(chan syncResult, 1)
	go func() {
		synced, rows, err := m.syncColdTierMetadata(ctx)
		done <- syncResult{synced: synced, rows: rows, err: err}
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
	if len(result.rows) == 0 {
		t.Fatal("cancelled sync returned no rows for the manifest sweep")
	}
	foundFallbackRow := false
	for _, row := range result.rows {
		if row.Path == firstFallbackPath {
			foundFallbackRow = true
			break
		}
	}
	if !foundFallbackRow {
		t.Fatalf("cancelled sync manifest rows do not include committed fallback row %q", firstFallbackPath)
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

func TestLoadColdRowsForManifestAfterCancellation(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	file := FileMetadata{
		Path:          "db1/cpu/2024/03/15/cpu_manifest_daily.parquet",
		Database:      "db1",
		Measurement:   "cpu",
		PartitionTime: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
		Tier:          TierCold,
	}
	wrote, err := m.metadata.recordColdFile(context.Background(), &file, time.Time{})
	if err != nil || !wrote {
		t.Fatalf("record cold row = (%t, %v), want a persisted row", wrote, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The ordinary read cannot return rows with a cancelled context. The
	// manifest loader must preserve the rows committed before cancellation.
	cancelledRows, cancelledErr := m.metadata.GetFilesInTier(ctx, TierCold)
	if !errors.Is(cancelledErr, context.Canceled) || len(cancelledRows) != 0 {
		t.Fatalf("GetFilesInTier with cancelled context = (%d rows, %v), want (0 rows, context.Canceled)", len(cancelledRows), cancelledErr)
	}

	rows, err := m.loadColdRowsForManifest(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("loadColdRowsForManifest error = %v, want context.Canceled", err)
	}
	if len(rows) != 1 || rows[0].Path != file.Path {
		t.Fatalf("loadColdRowsForManifest rows = %+v, want the persisted row %q", rows, file.Path)
	}
}
