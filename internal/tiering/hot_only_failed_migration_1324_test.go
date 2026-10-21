package tiering

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// migrationTimestampBackend records the time a committed cold object was
// written, rather than the later time its listing was requested.
type migrationTimestampBackend struct {
	*mockBackend
	mu         sync.Mutex
	modified   map[string]time.Time
	deleteErr  error
	deleteErrs int
}

func newMigrationTimestampBackend(backend *mockBackend) *migrationTimestampBackend {
	return &migrationTimestampBackend{mockBackend: backend, modified: make(map[string]time.Time)}
}

func (b *migrationTimestampBackend) Write(ctx context.Context, path string, data []byte) error {
	if err := b.mockBackend.Write(ctx, path, data); err != nil {
		return err
	}
	b.mu.Lock()
	b.modified[path] = time.Now().UTC()
	b.mu.Unlock()
	return nil
}

func (b *migrationTimestampBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	if err := b.mockBackend.WriteReader(ctx, path, reader, size); err != nil {
		return err
	}
	b.mu.Lock()
	b.modified[path] = time.Now().UTC()
	b.mu.Unlock()
	return nil
}

func (b *migrationTimestampBackend) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objects, err := b.mockBackend.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range objects {
		if modified, ok := b.modified[objects[i].Path]; ok {
			objects[i].LastModified = modified
		}
	}
	return objects, nil
}

func (b *migrationTimestampBackend) Delete(ctx context.Context, path string) error {
	b.mu.Lock()
	if b.deleteErrs > 0 {
		b.deleteErrs--
		err := b.deleteErr
		b.mu.Unlock()
		return err
	}
	b.mu.Unlock()
	if err := b.mockBackend.Delete(ctx, path); err != nil {
		return err
	}
	b.mu.Lock()
	delete(b.modified, path)
	b.mu.Unlock()
	return nil
}

func TestRunCycleCleansFailedColdCopyForHotOnlyDatabase(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := gateDailyA
	timedCold := newMigrationTimestampBackend(cold)
	m.coldBackend = timedCold

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
	failed, err := m.metadata.GetLatestMigrationForFile(ctx, path)
	if err != nil || failed == nil || failed.Error == "" {
		t.Fatalf("latest migration = %+v, err = %v; want a recorded failure", failed, err)
	}
	if _, err := m.metadata.db.Exec(`DROP TRIGGER fail_tier_flip`); err != nil {
		t.Fatalf("drop tier-flip failure trigger: %v", err)
	}
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
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
}

func TestHotOnlyColdSyncPreservesUnattributedColdObject(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := gateDailyA
	timedCold := newMigrationTimestampBackend(cold)
	m.coldBackend = timedCold
	mustWrite(t, hot, path)
	mustWrite(t, timedCold, path) // no failed migration record owns this object
	recordHotRow(t, m, path, manifestPartition)
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}

	if _, _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("syncColdTierMetadata: %v", err)
	}
	if exists, err := cold.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("unattributed cold object exists = %v, err = %v; want preserved", exists, err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierHot {
		t.Fatalf("tier after sync = %s, want hot", got)
	}
}

func TestHotOnlyFailedColdCleanupRetriesAfterDeleteFailure(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	path := gateDailyA
	timedCold := newMigrationTimestampBackend(cold)
	m.coldBackend = timedCold
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
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("set hot-only policy: %v", err)
	}
	timedCold.deleteErr = errors.New("temporary cold delete failure")
	timedCold.deleteErrs = 1

	if _, _, err := m.syncColdTierMetadata(ctx); err == nil {
		t.Fatal("syncColdTierMetadata succeeded despite the cold delete failure")
	}
	if exists, err := cold.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("cold object after failed cleanup = %v, err = %v; want preserved for retry", exists, err)
	}
	if got := fileMeta(t, m, path).Tier; got != TierHot {
		t.Fatalf("tier after failed cleanup = %s, want hot", got)
	}

	if _, _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("retry syncColdTierMetadata: %v", err)
	}
	if exists, err := cold.Exists(ctx, path); err != nil || exists {
		t.Fatalf("cold object after retry = %v, err = %v; want removed", exists, err)
	}
	if exists, err := hot.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("hot object after retry = %v, err = %v; want preserved", exists, err)
	}
}
