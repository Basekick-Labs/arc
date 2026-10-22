package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// The unlink of a DELETE rewrite's old file waits until the rewrite is on
// this node (rewrite_unlink.go).

type rewriteUnlinkRig struct {
	*stagedRigT
	old, rw string
	rwBody  string
}

// newRewriteUnlinkRig: L on disk (and unlisted, as after the DELETE's
// batch), its rewrite R registered and not on this node yet.
func newRewriteUnlinkRig(t *testing.T) *rewriteUnlinkRig {
	t.Helper()
	r := stagedRig(t)
	// The stale-local journal is what finds a kept copy again after a
	// restart: deferral needs it.
	r.c.cfg.RaftDataDir = t.TempDir()
	seen, err := openSeenLocal(r.c.cfg.RaftDataDir, r.c.cfg.ClusterName)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(seen.close)
	r.c.stale.seen.Store(seen)
	old := stHour + "cpu_1.parquet"
	rw := stHour + "cpu_1_rewrite_5.parquet"
	r.write(old, "all the rows")
	body := "the kept rows"
	sum := sha256.Sum256([]byte(body))
	e := raft.FileEntry{Path: rw, SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body)), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	return &rewriteUnlinkRig{stagedRigT: r, old: old, rw: rw, rwBody: body}
}

func (r *rewriteUnlinkRig) unlink() {
	r.c.unlinkOne(deleteRequest{path: r.old, reason: deleteRewriteReason}, nil)
}

func (r *rewriteUnlinkRig) pending() []string {
	r.c.deletePendingMu.Lock()
	defer r.c.deletePendingMu.Unlock()
	var out []string
	for _, d := range r.c.deletePending {
		out = append(out, d.path)
	}
	return out
}

func TestRewriteUnlinkWaitsForTheRewrite(t *testing.T) {
	r := newRewriteUnlinkRig(t)
	r.unlink()
	if !r.exists(r.old) || !r.c.unlinkDeferred(r.old) {
		t.Fatal("the old file was unlinked before its rewrite arrived")
	}
	// The rewrite lands (the puller's hook): the unlink is queued again,
	// once, and then goes ahead.
	r.write(r.rw, r.rwBody)
	r.c.recordPulledFileInTiering(r.rw, int64(len(r.rwBody)))
	if got := r.pending(); len(got) != 1 || got[0] != r.old || r.c.unlinkDeferred(r.old) {
		t.Fatalf("pending %v deferred %v", got, r.c.unlinkDeferred(r.old))
	}
	r.unlink()
	if r.exists(r.old) {
		t.Fatal("not unlinked once the rewrite is here")
	}
}

// Nothing to wait for: no registered rewrite (it left the manifest), the
// copy is not on this node, the workers are stopping, or the reason is not a
// DELETE rewrite.
func TestRewriteUnlinkDoesNotWaitWithoutCause(t *testing.T) {
	t.Run("no registered rewrite", func(t *testing.T) {
		r := newRewriteUnlinkRig(t)
		if err := r.c.raftNode.DeleteFile(r.rw, "compaction:j9", 5*time.Second); err != nil {
			t.Fatal(err)
		}
		r.unlink()
		if r.exists(r.old) {
			t.Fatal("waited for a rewrite the manifest no longer lists")
		}
	})
	t.Run("not on this node", func(t *testing.T) {
		r := newRewriteUnlinkRig(t)
		_ = os.Remove(filepath.Join(r.root, r.old))
		r.unlink()
		if r.c.unlinkDeferred(r.old) {
			t.Fatal("deferred the unlink of a copy this node does not hold")
		}
	})
	t.Run("stopping", func(t *testing.T) {
		// Not parked, and kept on disk: the next start re-derives it.
		r := newRewriteUnlinkRig(t)
		r.c.deleteStopping.Store(true)
		r.unlink()
		if !r.exists(r.old) || r.c.unlinkDeferred(r.old) {
			t.Fatalf("stopping: exists %v deferred %v", r.exists(r.old), r.c.unlinkDeferred(r.old))
		}
	})
	t.Run("no journal", func(t *testing.T) {
		// Nothing could find a kept copy again after a restart.
		r := newRewriteUnlinkRig(t)
		r.c.stale.seen.Store(nil)
		r.unlink()
		if r.exists(r.old) {
			t.Fatal("deferred without the stale-local journal")
		}
	})
	t.Run("other reason", func(t *testing.T) {
		r := newRewriteUnlinkRig(t)
		r.c.unlinkOne(deleteRequest{path: r.old, reason: "retention:1"}, nil)
		if r.exists(r.old) {
			t.Fatal("deferred a retention removal")
		}
	})
	t.Run("rewrite here at another size", func(t *testing.T) {
		r := newRewriteUnlinkRig(t)
		r.write(r.rw, "a partial")
		r.unlink()
		if !r.exists(r.old) {
			t.Fatal("a rewrite at another size counted as arrived")
		}
	})
}

// The sweep re-queues what became ready without a wake (a missed hook).
func TestRewriteUnlinkSweep(t *testing.T) {
	r := newRewriteUnlinkRig(t)
	prev := deferredUnlinkSweepEvery
	deferredUnlinkSweepEvery = 10 * time.Millisecond
	t.Cleanup(func() { deferredUnlinkSweepEvery = prev })
	r.unlink()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { r.c.sweepDeferredUnlinks(stop); close(done) }()
	defer func() { close(stop); <-done }()
	time.Sleep(50 * time.Millisecond)
	if !r.c.unlinkDeferred(r.old) {
		t.Fatal("the sweep re-queued an unready unlink")
	}
	r.write(r.rw, r.rwBody)
	deadline := time.Now().Add(5 * time.Second)
	for r.c.unlinkDeferred(r.old) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := r.pending(); len(got) != 1 || got[0] != r.old {
		t.Fatalf("pending after the sweep: %v", got)
	}
}

// Registered again: dropped, never unlinked.
func TestRewriteUnlinkDroppedWhenListedAgain(t *testing.T) {
	r := newRewriteUnlinkRig(t)
	r.unlink()
	r.c.dropDeferred(r.old)
	if r.c.unlinkDeferred(r.old) || !r.exists(r.old) {
		t.Fatal("drop")
	}
}

// While deferred, the copy is still this node's only copy of the rows: the
// read side does not leave it out, the stale sweep does not remove it, and
// the Iceberg export keeps it.
func TestDeferredCopyIsStillTheNodesCopy(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	old := staleHour + "cpu_1.parquet"
	rw := staleHour + "cpu_1_rewrite_5.parquet"
	r.register(old, "all the rows")
	r.pass()
	// Registered, not on this node yet: the state a deferral is in.
	e := raft.FileEntry{Path: rw, SHA256: "x", SizeBytes: 13, Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.unregisterAs(old, deleteRewriteReason)
	r.c.noteLocalDeleted(old, deleteRewriteReason)
	r.c.deletePendingMu.Lock()
	r.c.deferred.add(old, deferredUnlink{reason: deleteRewriteReason, since: time.Now()})
	r.c.deletePendingMu.Unlock()
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("deferred copy excluded from reads: %v", got)
	}
	if r.c.LocalCopyDeleted(old) {
		t.Fatal("the Iceberg export would drop a deferred copy")
	}
	r.c.stale.confirm = 0
	r.pass()
	r.pass()
	if !r.exists(old) {
		t.Fatal("the stale sweep removed a deferred copy")
	}
}

// After a restart the deferred set is empty; the copies the previous run left
// are found again from the stale-local journal.
func TestRewriteUnlinksRederivedAtStart(t *testing.T) {
	r := staleRig(t)
	old := staleHour + "cpu_1.parquet"
	rw := staleHour + "cpu_1_rewrite_5.parquet"
	plain := staleHour + "cpu_2.parquet"
	r.register(old, "all the rows")
	r.register(plain, "rows")
	r.pass()
	e := raft.FileEntry{Path: rw, SHA256: "x", SizeBytes: 5, Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.unregisterAs(old, deleteRewriteReason)
	r.unregisterAs(plain, "retention:1")
	r.c.rederiveRewriteUnlinks(context.Background(), r.c.stale.seen.Load())
	r.c.deletePendingMu.Lock()
	var pend []string
	for _, d := range r.c.deletePending {
		pend = append(pend, d.path+"|"+d.reason)
	}
	r.c.deletePendingMu.Unlock()
	if len(pend) != 1 || pend[0] != old+"|"+deleteRewriteReason {
		t.Fatalf("pending %v", pend)
	}
	r.c.open.mu.Lock()
	_, tracked := r.c.open.localDeleted[old]
	r.c.open.mu.Unlock()
	if !tracked {
		t.Fatal("not tracked for reads")
	}
}

// Each stale-sweep guard on its own: the candidate list and the unlink both
// leave a deferred copy alone.
func TestStaleSweepGuardsADeferredCopy(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	old := staleHour + "cpu_1.parquet"
	r.register(old, "all the rows")
	r.pass()
	r.unregisterAs(old, deleteRewriteReason)
	r.c.deletePendingMu.Lock()
	r.c.deferred.add(old, deferredUnlink{reason: deleteRewriteReason, since: time.Now()})
	r.c.deletePendingMu.Unlock()
	r.c.stale.confirm = 0
	seen := r.c.stale.seen.Load()
	now := time.Now()
	r.c.staleCandidates(ctx, seen, now)
	if got := r.c.staleCandidates(ctx, seen, now.Add(time.Second)); len(got) != 0 {
		t.Fatalf("a deferred copy is a stale candidate: %v", got)
	}
	if r.c.unlinkStale(ctx, old) || !r.exists(old) {
		t.Fatal("the stale unlink removed a deferred copy")
	}
}

// Through the real register callback: a path listed again leaves the
// deferred set.
func TestRegisterCallbackDropsADeferredUnlink(t *testing.T) {
	origin := startHangingOrigin(t)
	c, _ := newManifestDeleteRig(t, origin.addr())
	key := staleHour + "back.parquet"
	c.deletePendingMu.Lock()
	c.deferred.add(key, deferredUnlink{reason: deleteRewriteReason, since: time.Now()})
	c.deletePendingMu.Unlock()
	e := raft.FileEntry{Path: key, SHA256: "s", SizeBytes: 4, Database: "testdb", Measurement: "cpu",
		OriginNodeID: manifestDeleteOrigin, Tier: "hot", CreatedAt: time.Now()}
	if err := c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if c.unlinkDeferred(key) {
		t.Fatal("still deferred after the path was registered again")
	}
}

// Through the real stale-local start: the journal opens and the copies a
// previous run left waiting are queued again.
func TestStaleLocalStartRederivesRewriteUnlinks(t *testing.T) {
	r := staleRig(t)
	old := staleHour + "cpu_1.parquet"
	rw := staleHour + "cpu_1_rewrite_5.parquet"
	r.register(old, "all the rows")
	r.pass()
	e := raft.FileEntry{Path: rw, SHA256: "x", SizeBytes: 5, Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.unregisterAs(old, deleteRewriteReason)
	r.c.stale.seen.Load().close()
	r.c.stale.opened = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	r.c.stale.wg.Add(1)
	go r.c.staleLocalRun(ctx)
	t.Cleanup(func() {
		cancel()
		r.c.stale.wg.Wait()
		if s := r.c.stale.seen.Load(); s != nil {
			s.close()
		}
	})
	waitFSM(t, func() bool {
		r.c.deletePendingMu.Lock()
		defer r.c.deletePendingMu.Unlock()
		for _, d := range r.c.deletePending {
			if d.path == old && d.reason == deleteRewriteReason {
				return true
			}
		}
		return false
	})
}

// A node booting behind its journal lists the old file and not yet its
// rewrite: the rewrite must not be taken for a leftover (only a path older
// than every listed member of its family is re-derived).
func TestRederiveSkipsALaggingBootShape(t *testing.T) {
	r := staleRig(t)
	x := staleHour + "cpu_1.parquet"
	rw := staleHour + "cpu_1_rewrite_5.parquet"
	r.register(x, "all the rows")
	r.register(rw, "kept")
	r.pass()
	r.unregisterAs(rw, "lag-sim") // the FSM's view while behind: R unlisted, X listed
	if !r.c.rederiveRewriteUnlinks(context.Background(), r.c.stale.seen.Load()) {
		t.Fatal("did not run")
	}
	r.c.deletePendingMu.Lock()
	n := len(r.c.deletePending)
	r.c.deletePendingMu.Unlock()
	if n != 0 {
		t.Fatalf("queued %d unlinks for a rewrite newer than the listed file", n)
	}
	if rewriteStamp(rw) != 5 || rewriteStamp(x) != 0 {
		t.Fatal("rewriteStamp")
	}
}

// A rewrite leaving the manifest wakes the unlinks waiting for it, through
// the real delete callback.
func TestRewriteLeavingTheManifestWakesItsFamily(t *testing.T) {
	r := newRewriteUnlinkRig(t)
	r.unlink()
	if !r.c.unlinkDeferred(r.old) {
		t.Fatal("precondition: deferred")
	}
	r.c.wakeDeferredFamilyAsync(r.rw) // with the rewrite still listed: stays
	time.Sleep(50 * time.Millisecond)
	if !r.c.unlinkDeferred(r.old) {
		t.Fatal("woken while its rewrite is listed and not here")
	}
	if err := r.c.raftNode.DeleteFile(r.rw, "compaction:j1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.c.wakeDeferredFamilyAsync(r.rw)
	waitFSM(t, func() bool { return !r.c.unlinkDeferred(r.old) })
}

// A rewrite that lands just before its wake: the read side finds it on its
// own (rate-limited), so the old copy and the rewrite are not both read.
func TestDeferredCopyLeftOutOnceTheRewriteLands(t *testing.T) {
	r := staleRig(t)
	ctx := context.Background()
	old := staleHour + "cpu_1.parquet"
	rw := staleHour + "cpu_1_rewrite_5.parquet"
	r.register(old, "all the rows")
	r.pass()
	e := raft.FileEntry{Path: rw, SHA256: "x", SizeBytes: int64(len("the kept rows")), Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.unregisterAs(old, deleteRewriteReason)
	r.c.noteLocalDeleted(old, deleteRewriteReason)
	r.c.deletePendingMu.Lock()
	r.c.deferred.add(old, deferredUnlink{reason: deleteRewriteReason, since: time.Now()})
	r.c.deletePendingMu.Unlock()
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 0 {
		t.Fatalf("rewrite not here: excluded %v", got)
	}
	r.write(rw, "the kept rows")
	r.c.open.mu.Lock()
	r.c.open.localDeleted[old].siblingCheckedAt = time.Time{} // the rate limit elapsed
	r.c.open.mu.Unlock()
	if got := r.c.OpenExcludedFor(ctx, openPrefix); len(got) != 1 || got[0] != old {
		t.Fatalf("rewrite landed before its wake: excluded %v", got)
	}
}
