package reconciliation

// #1212: a file this node flushed that the cluster manifest has not confirmed
// yet is unregistered but not an orphan. Deleting it loses the only copy of
// its rows.

import (
	"context"
	"testing"
	"time"
)

func TestReconcile_KeepsAFileStillPendingRegistration(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeBackend()
	const pending = "db/m/2026/04/27/12/pending.parquet"
	const orphan = "db/m/2026/04/27/12/orphan.parquet"
	store.put(pending, now.Add(-48*time.Hour)) // past the grace window: an outage this long
	store.put(orphan, now.Add(-48*time.Hour))

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared, DeletePreManifestOrphans: true},
		newFakeCoordinator(), store, &fakeGate{scan: true, sweep: true})
	r.SetPendingFilter(func(path string) bool { return path == pending })

	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if exists, _ := store.Exists(context.Background(), pending); !exists {
		t.Fatal("deleted a file still pending registration")
	}
	if exists, _ := store.Exists(context.Background(), orphan); exists {
		t.Fatal("the real orphan was kept: the filter is too broad")
	}
	if run.SkippedPending != 1 {
		t.Fatalf("SkippedPending = %d, want 1", run.SkippedPending)
	}
}
