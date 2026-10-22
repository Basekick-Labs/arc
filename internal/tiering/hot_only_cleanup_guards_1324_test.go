package tiering

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

func setupHotOnlyCleanupCandidate(t *testing.T, path string) (*Manager, *mockBackend, *mockBackend, func()) {
	t.Helper()
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	ctx := context.Background()
	if err := m.policies.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		cleanup()
		t.Fatalf("set hot-only policy: %v", err)
	}
	mustWrite(t, hot, path)
	mustWrite(t, cold, path)
	recordHotRow(t, m, path, manifestPartition)
	return m, hot, cold, cleanup
}

func TestHotOnlyCleanupSkipsColdRowsWithoutManifest(t *testing.T) {
	m, hot, cold, cleanup := setupHotOnlyCleanupCandidate(t, gateDailyA)
	defer cleanup()
	ctx := context.Background()
	if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(gateDailyA), time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("record cold row: %v", err)
	}
	m.manifest = nil

	cleaned := m.cleanupHotOnlyColdCopies(ctx, []storage.ObjectInfo{{Path: gateDailyA, Size: 7}}, map[string]int64{gateDailyA: 7})
	if len(cleaned) != 0 {
		t.Fatalf("cleaned = %v, want no cleanup for a cold metadata row", cleaned)
	}
	if exists, err := cold.Exists(ctx, gateDailyA); err != nil || !exists {
		t.Fatalf("cold object = %v, err = %v; want preserved", exists, err)
	}
	if exists, err := hot.Exists(ctx, gateDailyA); err != nil || !exists {
		t.Fatalf("hot object = %v, err = %v; want preserved", exists, err)
	}
}

func TestHotOnlyCleanupRequiresMatchingManifestEntry(t *testing.T) {
	for name, entry := range map[string]*int64{
		"missing":       nil,
		"size mismatch": func() *int64 { n := int64(8); return &n }(),
	} {
		t.Run(name, func(t *testing.T) {
			m, _, cold, cleanup := setupHotOnlyCleanupCandidate(t, gateDailyA)
			defer cleanup()
			entries := map[string]int64{}
			if entry != nil {
				entries[gateDailyA] = *entry
			}
			m.manifest = &fakeManifest{hot: m.hotBackend, entries: entries}

			cleaned := m.cleanupHotOnlyColdCopies(context.Background(), []storage.ObjectInfo{{Path: gateDailyA, Size: 7}}, map[string]int64{})
			if len(cleaned) != 0 {
				t.Fatalf("cleaned = %v, want cold object preserved", cleaned)
			}
			if exists, err := cold.Exists(context.Background(), gateDailyA); err != nil || !exists {
				t.Fatalf("cold object = %v, err = %v; want preserved", exists, err)
			}
		})
	}
}

func TestHotOnlyCleanupRequiresMatchingObjectSize(t *testing.T) {
	m, _, cold, cleanup := setupHotOnlyCleanupCandidate(t, gateDailyA)
	defer cleanup()
	m.manifest = nil

	cleaned := m.cleanupHotOnlyColdCopies(context.Background(), []storage.ObjectInfo{{Path: gateDailyA, Size: 8}}, map[string]int64{})
	if len(cleaned) != 0 {
		t.Fatalf("cleaned = %v, want cold object preserved for size mismatch", cleaned)
	}
	if exists, err := cold.Exists(context.Background(), gateDailyA); err != nil || !exists {
		t.Fatalf("cold object = %v, err = %v; want preserved", exists, err)
	}
}

func TestHotOnlyCleanupSkipsRoleGatedNode(t *testing.T) {
	m, _, cold, cleanup := setupHotOnlyCleanupCandidate(t, gateDailyA)
	defer cleanup()
	m.manifest = nil
	m.clusterGate = &mockGate{role: "reader"}

	cleaned := m.cleanupHotOnlyColdCopies(context.Background(), []storage.ObjectInfo{{Path: gateDailyA, Size: 7}}, map[string]int64{})
	if len(cleaned) != 0 {
		t.Fatalf("cleaned = %v, want no cleanup on a reader", cleaned)
	}
	if exists, err := cold.Exists(context.Background(), gateDailyA); err != nil || !exists {
		t.Fatalf("cold object = %v, err = %v; want preserved", exists, err)
	}
}

func TestHotOnlyCleanupContinuesAfterDeleteFailure(t *testing.T) {
	m, hot, cold, cleanup := setupHotOnlyCleanupCandidate(t, gateDailyA)
	defer cleanup()
	ctx := context.Background()
	const second = gateDailyB
	mustWrite(t, hot, second)
	mustWrite(t, cold, second)
	recordHotRow(t, m, second, manifestPartition)
	m.manifest = nil
	coldWithFailure := &failedDeleteBackend{mockBackend: cold, deleteErr: errors.New("temporary delete failure"), deleteErrs: 1}
	m.coldBackend = coldWithFailure
	objects := []storage.ObjectInfo{{Path: gateDailyA, Size: 7}, {Path: second, Size: 7}}

	cleaned := m.cleanupHotOnlyColdCopies(ctx, objects, map[string]int64{})
	if _, ok := cleaned[gateDailyA]; ok {
		t.Fatal("failed first delete was reported as cleaned")
	}
	if _, ok := cleaned[second]; !ok {
		t.Fatalf("second object not cleaned after first delete failed: %v", cleaned)
	}
	if exists, err := cold.Exists(ctx, gateDailyA); err != nil || !exists {
		t.Fatalf("first cold object = %v, err = %v; want preserved for retry", exists, err)
	}
	if exists, err := cold.Exists(ctx, second); err != nil || exists {
		t.Fatalf("second cold object = %v, err = %v; want removed", exists, err)
	}
}

type staleStatBackend struct{ *mockBackend }

func (b *staleStatBackend) StatFile(context.Context, string) (int64, error) {
	return 7, nil
}

func TestHotOnlyCleanupDoesNotCountObjectStillReportedByStat(t *testing.T) {
	m, _, cold, cleanup := setupHotOnlyCleanupCandidate(t, gateDailyA)
	defer cleanup()
	m.manifest = nil
	m.coldBackend = &staleStatBackend{mockBackend: cold}

	cleaned := m.cleanupHotOnlyColdCopies(context.Background(), []storage.ObjectInfo{{Path: gateDailyA, Size: 7}}, map[string]int64{})
	if len(cleaned) != 0 {
		t.Fatalf("cleaned = %v, want object still present in stale stat", cleaned)
	}
	if exists, err := cold.Exists(context.Background(), gateDailyA); err != nil || exists {
		t.Fatalf("mock cold object = %v, err = %v; delete should have been attempted before stale stat", exists, err)
	}
}

func TestHotOnlyColdRowWithoutManifestEntryIsNotReverted(t *testing.T) {
	m, hot, cold, cleanup := setupHotOnlyCleanupCandidate(t, gateDailyA)
	defer cleanup()
	ctx := context.Background()
	if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(gateDailyA), time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("record cold row: %v", err)
	}
	if err := cold.Delete(ctx, gateDailyA); err != nil {
		t.Fatalf("remove cold object: %v", err)
	}
	m.coldBackend = &coldLister{mockBackend: cold, lastModified: time.Now().Add(-time.Hour)}
	m.manifest = &fakeManifest{hot: hot, entries: map[string]int64{}}

	if _, _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatalf("syncColdTierMetadata: %v", err)
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold while manifest entry is absent", got)
	}
}
