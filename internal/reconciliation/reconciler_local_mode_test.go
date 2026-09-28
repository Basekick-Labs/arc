package reconciliation

// Per-node storage (BackendLocal) with file replication: every node holds a
// replica of every file, so a node's disk legitimately contains files whose
// manifest entry names another node as origin. The manifest is the source of
// truth for "does this path exist" — a tracked path is never orphan storage,
// whatever its origin — while the per-node scoping applies only to the
// orphan-manifest direction (#957).

import (
	"context"
	"testing"
	"time"
)

// The #957 regression. Before the fix the origin filter dropped node-b's
// entry from the membership set and the replica came back as orphan storage
// on node-a in dry-run and act mode alike; only the storage sweep's
// per-candidate manifest re-check stood between it and deletion.
func TestReconcile_LocalModeReplicaOfForeignFileIsNotOrphanStorage(t *testing.T) {
	now := time.Now().UTC()
	const replica = "db/m/2026/04/27/12/from-node-b.parquet"

	coord := newFakeCoordinator(fileEntry(replica, "node-b"))
	store := newFakeBackend()
	store.put(replica, now.Add(-48*time.Hour)) // well past the grace window

	for _, tc := range []struct {
		name   string
		cfg    Config
		dryRun bool
	}{
		{"dry_run", Config{Enabled: true, BackendKind: BackendLocal, LocalNodeID: "node-a"}, true},
		{"act_mode", Config{Enabled: true, BackendKind: BackendLocal, LocalNodeID: "node-a", ManifestOnlyDryRun: false}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReconciler(t, tc.cfg, coord, store, &fakeGate{scan: true, sweep: true, role: "writer"})
			run, err := r.Reconcile(context.Background(), tc.dryRun)
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if run.OrphanStorageCount != 0 {
				t.Errorf("a manifest-tracked replica of a node-b file was classified as orphan storage on node-a: OrphanStorageCount=%d sample=%v", run.OrphanStorageCount, run.OrphanStorageSample)
			}
			if run.SkippedRecheck != 0 {
				t.Errorf("SkippedRecheck=%d, want 0: the sweep's manifest re-check must not be what keeps a replica alive", run.SkippedRecheck)
			}
			if run.StorageDeletes != 0 {
				t.Errorf("StorageDeletes=%d, want 0", run.StorageDeletes)
			}
			if run.StorageFileCount != 1 || run.ManifestFileCount != 1 || run.OrphanManifestCount != 0 {
				t.Errorf("counts: storage=%d manifest=%d orphan_manifest=%d, want 1/1/0", run.StorageFileCount, run.ManifestFileCount, run.OrphanManifestCount)
			}
			if exists, _ := store.Exists(context.Background(), replica); !exists {
				t.Fatalf("%q was deleted from node-a's storage", replica)
			}
		})
	}
}

// The direction the origin scoping is for: an entry this node originated and
// no longer has is an orphan-manifest candidate; one another node originated
// and this node does not hold is not — it is the puller's business, or lives
// on that node's disk without replication.
func TestReconcile_LocalModeOwnEntryMissingIsOrphanManifest(t *testing.T) {
	const mine = "db/m/2026/04/27/12/mine.parquet"
	const theirs = "db/m/2026/04/27/12/theirs.parquet"
	coord := newFakeCoordinator(fileEntry(mine, "node-a"), fileEntry(theirs, "node-b"))
	store := newFakeBackend() // neither file on disk

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendLocal, LocalNodeID: "node-a"},
		coord, store, &fakeGate{scan: true, sweep: true, role: "writer"})
	run, err := r.Reconcile(context.Background(), true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.OrphanManifestCount != 1 || len(run.OrphanManifestSample) != 1 || run.OrphanManifestSample[0] != mine {
		t.Fatalf("orphan-manifest: count=%d sample=%v, want exactly the own-origin entry %q", run.OrphanManifestCount, run.OrphanManifestSample, mine)
	}
}

// The filtered set also fed prefix derivation, so a measurement only another
// node writes was never walked on this node: the root walk skips every
// database this node already has an entry in, at the default settings. A
// true orphan under such a measurement was invisible here.
func TestReconcile_LocalModeWalksForeignPrefixes(t *testing.T) {
	now := time.Now().UTC()
	const own = "db/m1/2026/04/27/12/own.parquet"
	const replica = "db/m2/2026/04/27/12/replica.parquet"
	const orphan = "db/m2/2026/04/27/12/orphan.parquet"
	coord := newFakeCoordinator(fileEntry(own, "node-a"), fileEntry(replica, "node-b"))
	store := newFakeBackend()
	store.put(own, now.Add(-2*time.Hour))
	store.put(replica, now.Add(-48*time.Hour))
	store.put(orphan, now.Add(-48*time.Hour))

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendLocal, LocalNodeID: "node-a"}, // default root-walk cap
		coord, store, &fakeGate{scan: true, sweep: true, role: "writer"})
	run, err := r.Reconcile(context.Background(), true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.StorageFileCount != 3 {
		t.Fatalf("StorageFileCount=%d, want 3: db/m2/ (only node-b writes it) was not walked", run.StorageFileCount)
	}
	if run.OrphanStorageCount != 1 || len(run.OrphanStorageSample) != 1 || run.OrphanStorageSample[0] != orphan {
		t.Fatalf("orphan storage: count=%d sample=%v, want exactly %q", run.OrphanStorageCount, run.OrphanStorageSample, orphan)
	}
	if run.OrphanManifestCount != 0 {
		t.Errorf("OrphanManifestCount=%d, want 0", run.OrphanManifestCount)
	}
}
