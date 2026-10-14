package reconciliation

// #1214: on a shared object store the orphan-storage sweep runs on the active
// compactor. Its manifest is its own FSM's, which trails the leader while it
// is cut off, and it has no registration outbox: a file another writer
// flushed and has not registered yet is an unregistered object like any
// orphan. The run syncs with the leader before each sweep, and keeps what the
// writers report pending.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

const (
	remoteOrphan  = "db/m/2026/04/27/12/orphan.parquet"
	remotePending = "db/m/2026/04/27/12/pending.parquet"
)

func remoteRig(t *testing.T) (*fakeCoordinator, *fakeBackend, *Reconciler) {
	t.Helper()
	now := time.Now().UTC()
	store := newFakeBackend()
	store.put(remoteOrphan, now.Add(-48*time.Hour))
	store.put(remotePending, now.Add(-48*time.Hour))
	coord := newFakeCoordinator(fileEntry("db/m/2026/04/27/12/listed.parquet", "w1"))
	store.put("db/m/2026/04/27/12/listed.parquet", now.Add(-48*time.Hour))
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared, DeletePreManifestOrphans: true},
		coord, store, &fakeGate{scan: true, sweep: true})
	return coord, store, r
}

func exists(store *fakeBackend, p string) bool {
	ok, _ := store.Exists(context.Background(), p)
	return ok
}

func TestReconcileAbortsWhenTheManifestCannotBeSynced(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		reason AbortReason
	}{
		{"no leader", errors.New("follower barrier: could not reach the leader before the deadline"), AbortManifestNotSynced},
		{"older leader", fmt.Errorf("follower barrier not accepted by the leader: %w: leader rejected", raft.ErrBarrierUnsupported), AbortLeaderBarrierUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coord, store, r := remoteRig(t)
			// A manifest entry whose file is gone: an orphan-manifest
			// candidate, so neither half may act.
			coord.manifest["db/m/2026/04/27/12/gone.parquet"] = fileEntry("db/m/2026/04/27/12/gone.parquet", "w1")
			coord.syncErr = tc.err
			run, err := r.Reconcile(context.Background(), false)
			if err == nil || !run.Aborted || run.AbortReason != tc.reason {
				t.Fatalf("run %+v err %v, want an abort with %s", run, err, tc.reason)
			}
			if !exists(store, remoteOrphan) || len(coord.batchCalls) != 0 || run.StorageDeletes+run.ManifestDeletes != 0 {
				t.Fatal("a run without a synced manifest deleted something")
			}
		})
	}
}

// A run lasts up to MaxRunDuration: the barrier is taken again before the
// storage sweep, and a failure there deletes no storage.
func TestReconcileSyncsAgainBeforeTheStorageSweep(t *testing.T) {
	coord, store, r := remoteRig(t)
	coord.syncFn = func(n int) error {
		if n >= 2 {
			return errors.New("leader lost")
		}
		return nil
	}
	run, err := r.Reconcile(context.Background(), false)
	if err == nil || run.AbortReason != AbortManifestNotSynced {
		t.Fatalf("run %+v err %v", run, err)
	}
	if coord.syncCalls != 2 || !exists(store, remoteOrphan) {
		t.Fatalf("sync calls %d, orphan kept %v", coord.syncCalls, exists(store, remoteOrphan))
	}
}

func TestReconcileKeepsAFilePendingOnAnotherNode(t *testing.T) {
	coord, store, r := remoteRig(t)
	r.SetRemotePendingSource(func(context.Context) RemotePending {
		return RemotePending{Paths: map[string]struct{}{remotePending: {}}}
	})
	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !exists(store, remotePending) {
		t.Fatal("deleted a file another node reported pending registration")
	}
	if exists(store, remoteOrphan) {
		t.Fatal("the real orphan was kept")
	}
	if run.SkippedPendingRemote != 1 || run.StorageSweepHeld {
		t.Fatalf("skipped %d held %v", run.SkippedPendingRemote, run.StorageSweepHeld)
	}
	if coord.syncCalls != 2 {
		t.Fatalf("barriers %d, want one per sweep", coord.syncCalls)
	}
	if metrics.Get().ReconciliationStorageSweepHeld() {
		t.Fatal("the held gauge is set after a run that swept")
	}
}

func TestReconcileHoldsTheStorageSweepWhenAWriterCannotTell(t *testing.T) {
	_, store, r := remoteRig(t)
	r.SetRemotePendingSource(func(context.Context) RemotePending {
		return RemotePending{Paths: map[string]struct{}{}, Held: []string{"node writer-2: no answer"}}
	})
	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !run.StorageSweepHeld || !strings.Contains(run.StorageSweepHeldReason, "writer-2") || run.Aborted {
		t.Fatalf("run %+v, want the storage sweep held naming writer-2", run)
	}
	if !exists(store, remoteOrphan) || !exists(store, remotePending) || run.StorageDeletes != 0 {
		t.Fatal("a held sweep deleted storage")
	}
	if run.OrphanStorageCount != 2 {
		t.Fatalf("held run still reports its candidates: %d", run.OrphanStorageCount)
	}
	if !metrics.Get().ReconciliationStorageSweepHeld() {
		t.Fatal("the held gauge is not set")
	}
}

// Only on a shared object store: per-node storage keeps its own outbox filter.
func TestReconcileAsksNoOneOnLocalStorage(t *testing.T) {
	store := newFakeBackend()
	store.put(remoteOrphan, time.Now().Add(-48*time.Hour))
	r := newReconciler(t, Config{Enabled: true, BackendKind: BackendLocal, LocalNodeID: "w1", DeletePreManifestOrphans: true},
		newFakeCoordinator(), store, &fakeGate{scan: true, sweep: true})
	asked := false
	r.SetRemotePendingSource(func(context.Context) RemotePending {
		asked = true
		return RemotePending{Held: []string{"x"}}
	})
	if run, err := r.Reconcile(context.Background(), false); err != nil || run.StorageSweepHeld || asked {
		t.Fatalf("run %+v err %v asked %v", run, err, asked)
	}
}

func TestReconcileKeepsAnAdoptableCompactionOutput(t *testing.T) {
	_, store, r := remoteRig(t)
	r.SetAdoptableFilter(func(_ context.Context, p string) bool { return p == remotePending })
	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !exists(store, remotePending) || exists(store, remoteOrphan) || run.SkippedAdoptable != 1 {
		t.Fatalf("adoptable kept %v, orphan kept %v, skipped %d", exists(store, remotePending), exists(store, remoteOrphan), run.SkippedAdoptable)
	}
}

func TestReconcilerGraceWindowFloor(t *testing.T) {
	for _, tc := range []struct {
		set, want time.Duration
	}{
		{0, 24 * time.Hour},
		{time.Minute, MinGraceWindow},
		{2 * time.Hour, 2 * time.Hour},
	} {
		r, err := NewReconciler(Config{Enabled: true, BackendKind: BackendShared, GraceWindow: tc.set}, newFakeCoordinator(), newFakeBackend(), nil, nil, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		if got := r.Config().GraceWindow; got != tc.want {
			t.Fatalf("grace %v became %v, want %v", tc.set, got, tc.want)
		}
	}

	// A file 30 minutes old is inside the floor, whatever was configured.
	store := newFakeBackend()
	store.put(remoteOrphan, time.Now().Add(-30*time.Minute))
	r := newReconciler(t, Config{Enabled: true, BackendKind: BackendShared, DeletePreManifestOrphans: true, GraceWindow: time.Minute},
		newFakeCoordinator(), store, &fakeGate{scan: true, sweep: true})
	if _, err := r.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if !exists(store, remoteOrphan) {
		t.Fatal("a file younger than the grace floor was deleted")
	}
}

// The published pending lists live under a reserved root, as JSON: never a
// database to walk, never a candidate.
func TestReconcileNeverWalksThePublishedPendingLists(t *testing.T) {
	store := newFakeBackend()
	const list = "_registration_pending/writer-1.json"
	store.put(list, time.Now().Add(-48*time.Hour))
	store.put(remoteOrphan, time.Now().Add(-48*time.Hour))
	r := newReconciler(t, Config{Enabled: true, BackendKind: BackendShared, DeletePreManifestOrphans: true, MaxRootWalkDatabases: -1},
		newFakeCoordinator(), store, &fakeGate{scan: true, sweep: true})
	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !exists(store, list) || run.StorageFileCount != 1 || exists(store, remoteOrphan) {
		t.Fatalf("list kept %v, storage files %d, orphan kept %v", exists(store, list), run.StorageFileCount, exists(store, remoteOrphan))
	}
}
