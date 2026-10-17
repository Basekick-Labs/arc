package reconciliation

// #1212: a file this node flushed that the cluster manifest has not confirmed
// yet is unregistered but not an orphan. Deleting it loses the only copy of
// its rows.

import (
	"context"
	"strings"
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

// #1207 d2a2a: a committed compaction output still moving into place here is
// registered and briefly absent, not an orphan entry.
func TestReconcile_KeepsAManifestEntryHeldForAdoption(t *testing.T) {
	now := time.Now().UTC()
	const held = "db/m/2026/04/27/12/held.parquet"
	const missing = "db/m/2026/04/27/12/missing.parquet"
	coord := newFakeCoordinator(fileEntry(held, "node-a"), fileEntry(missing, "node-a"))
	store := newFakeBackend()
	store.put("db/m/2026/04/27/12/a.parquet", now.Add(-2*time.Hour))
	r := newReconciler(t, Config{Enabled: true, BackendKind: BackendShared}, coord, store, &fakeGate{scan: true, sweep: true})
	r.SetHeldFilter(func(path string) bool { return path == held })
	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.SkippedHeld != 1 || run.ManifestDeletes != 1 {
		t.Fatalf("held %d manifest deletes %d", run.SkippedHeld, run.ManifestDeletes)
	}
	for _, call := range coord.batchCalls {
		for _, op := range call {
			if strings.Contains(string(op.Payload), held) {
				t.Fatal("the held entry was deleted")
			}
		}
	}
}

// #1207 d2a2b: an input object of a committed object-store compaction is
// unregistered and still the data until its finisher puts the output in
// place: not an orphan.
func TestReconcile_KeepsAFileACompactionOwns(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeBackend()
	const owned = "db/m/2026/04/27/12/input.parquet"
	const orphan = "db/m/2026/04/27/12/orphan.parquet"
	store.put(owned, now.Add(-48*time.Hour))
	store.put(orphan, now.Add(-48*time.Hour))

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared, DeletePreManifestOrphans: true},
		newFakeCoordinator(), store, &fakeGate{scan: true, sweep: true})
	r.SetCompactionOwnedFilter(func(path string) bool { return path == owned })

	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if exists, _ := store.Exists(context.Background(), owned); !exists {
		t.Fatal("deleted an input a compaction owns")
	}
	if exists, _ := store.Exists(context.Background(), orphan); exists {
		t.Fatal("the real orphan was kept: the filter is too broad")
	}
	if run.SkippedHeld != 1 {
		t.Fatalf("SkippedHeld = %d, want 1", run.SkippedHeld)
	}
}
