package reconciliation

import (
	"context"
	"testing"
	"time"
)

// On per-node storage the gate may withhold the orphan-manifest sweep alone
// while file replication is still converging on this node (#959). That is a
// hold, not a revocation: the run completes, the storage half still runs,
// the counts are still reported, and the run says the sweep was held. Before
// the pre-sweep check the same gate answer hit the sweep's chunk-boundary
// revocation guard and aborted the whole run as a lost lease.
func TestReconcile_HeldManifestSweepStillRunsStorageSweep(t *testing.T) {
	now := time.Now().UTC()
	const mineMissing = "db/m/2026/04/27/12/mine-missing.parquet" // orphan-manifest candidate
	const trueOrphan = "db/m/2026/04/27/12/orphan.parquet"        // orphan-storage candidate
	coord := newFakeCoordinator(fileEntry(mineMissing, "node-a"))
	store := newFakeBackend()
	store.put(trueOrphan, now.Add(-48*time.Hour))

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendLocal, LocalNodeID: "node-a", ManifestOnlyDryRun: false},
		coord, store, &fakeGate{scan: true, sweep: false, role: "writer"})
	run, err := r.Reconcile(context.Background(), false) // act mode
	if err != nil {
		t.Fatalf("Reconcile: %v (a held manifest sweep is not an error)", err)
	}
	if run.Aborted {
		t.Fatalf("run aborted (%s: %s); a held sweep must not abort the run", run.AbortReason, run.AbortMessage)
	}
	if !run.ManifestSweepHeld {
		t.Fatal("ManifestSweepHeld must be set when the gate withholds the manifest sweep")
	}
	if run.OrphanManifestCount != 1 || run.ManifestDeletes != 0 {
		t.Fatalf("orphan-manifest: count=%d deletes=%d, want 1 reported and 0 proposed", run.OrphanManifestCount, run.ManifestDeletes)
	}
	if run.StorageDeletes != 1 {
		t.Fatalf("StorageDeletes=%d, want 1: the storage half must still run while the manifest sweep is held", run.StorageDeletes)
	}
	if _, ok := coord.GetFileEntry(mineMissing); !ok {
		t.Fatal("the held sweep must not have proposed the manifest delete")
	}
	if exists, _ := store.Exists(context.Background(), trueOrphan); exists {
		t.Fatal("the true storage orphan must still have been swept")
	}
}
