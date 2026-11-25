package reconciliation

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The objects a storage sweep deleted are reported (the Iceberg export may
// have published them as objects the manifest does not know, #1728); the ones
// it could not delete are not.
func TestStorageSweepReportsDeletedObjects(t *testing.T) {
	const goodOrphan = "db/m/2026/04/11/14/orphan.parquet"
	const flakyOrphan = "db/m/2026/04/11/14/timeout.parquet"
	store := &flakyBackend{fakeBackend: newFakeBackend(), flakyKey: flakyOrphan}
	store.put(goodOrphan, time.Now().UTC().Add(-24*time.Hour))
	store.put(flakyOrphan, time.Now().UTC().Add(-24*time.Hour))
	r := newReconciler(t, Config{Enabled: true, BackendKind: BackendShared, BatchSize: 100, MaxDeletesPerRun: 100},
		newFakeCoordinator(), store, &fakeGate{scan: true, sweep: true, role: "leader"})
	var got []string
	r.SetOnPathsDeleted(func(paths []string) { got = append(got, paths...) })
	r.applyStorageDeletes(context.Background(), &Run{ID: "r"}, []string{goodOrphan, invalidManifestKey, flakyOrphan}, nil, false)
	if strings.Join(got, ",") != goodOrphan {
		t.Fatalf("reported %v, want only the deleted orphan", got)
	}
}

// The batch-delete branch reports too.
func TestStorageSweepReportsABatchDelete(t *testing.T) {
	const a, b = "db/m/2026/04/11/14/a.parquet", "db/m/2026/04/11/14/b.parquet"
	store := newFakeBackend()
	store.put(a, time.Now().UTC().Add(-24*time.Hour))
	store.put(b, time.Now().UTC().Add(-24*time.Hour))
	r := newReconciler(t, Config{Enabled: true, BackendKind: BackendShared, BatchSize: 100, MaxDeletesPerRun: 100},
		newFakeCoordinator(), store, &fakeGate{scan: true, sweep: true, role: "leader"})
	var got []string
	r.SetOnPathsDeleted(func(paths []string) { got = append(got, paths...) })
	r.applyStorageDeletes(context.Background(), &Run{ID: "r"}, []string{a, b}, store, true)
	if strings.Join(got, ",") != a+","+b {
		t.Fatalf("reported %v, want both", got)
	}
}
