package cluster

// Query-gate self-heal on manifest delete (#759, #795), driven through the
// production wiring: a bootstrapped Raft node, startFilePullerLocked (the real
// onRegister/onDelete callbacks and the real ManifestEntry hook), the real fetch
// client against a peer, and DeleteFileFromManifest on the leader path.
//
// Two orderings against the reader's catch-up pull:
//   1. the pull failed first, then the entry left the manifest;
//   2. the entry left the manifest while the pull was blocked on the peer.
// On main, ordering 1 keeps the gate red until restart; on the #790 shape,
// ordering 2 does.

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

const (
	manifestDeleteSecret  = "manifest-delete-secret-32-bytes-long!"
	manifestDeleteCluster = "test-cluster"
	manifestDeleteOrigin  = "writer-1"
	manifestDeleteReader  = "reader-1"
)

// hangingOrigin accepts fetch connections, reads the request, then holds the
// connection open until released, so the reader's pull stays in flight for as
// long as the test wants. A peer that stalls after accepting is exactly the
// shape that gave #795 its window.
type hangingOrigin struct {
	listener  net.Listener
	connected chan struct{}
	release   chan struct{}
	once      sync.Once
	wg        sync.WaitGroup
}

func startHangingOrigin(t *testing.T) *hangingOrigin {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	h := &hangingOrigin{listener: l, connected: make(chan struct{}), release: make(chan struct{})}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			h.wg.Add(1)
			go func() {
				defer h.wg.Done()
				defer conn.Close()
				if _, err := protocol.ReceiveMessage(conn, 5*time.Second); err != nil {
					return
				}
				h.once.Do(func() { close(h.connected) })
				<-h.release
			}()
		}
	}()
	t.Cleanup(h.stop)
	return h
}

func (h *hangingOrigin) addr() string { return h.listener.Addr().String() }

func (h *hangingOrigin) releaseAll() {
	select {
	case <-h.release:
	default:
		close(h.release)
	}
}

func (h *hangingOrigin) stop() {
	h.releaseAll()
	_ = h.listener.Close()
	h.wg.Wait()
}

// newManifestDeleteRig boots a single-node Raft cluster for a reader whose
// only peer lives at peerAddr, and starts the file puller through the same
// function main.go's Coordinator.Start uses.
func newManifestDeleteRig(t *testing.T, peerAddr string) (*Coordinator, *memBackend) {
	t.Helper()
	backend := newMemBackend()
	return newManifestDeleteRigWithBackend(t, peerAddr, backend, zerolog.Nop()), backend
}

func newManifestDeleteRigWithBackend(t *testing.T, peerAddr string, backend storage.Backend, logger zerolog.Logger) *Coordinator {
	t.Helper()
	raftNode := startRaftNode(t, manifestDeleteReader, allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	local := NewNode(manifestDeleteReader, manifestDeleteReader, RoleReader, manifestDeleteCluster)
	registry := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	peer := NewNode(manifestDeleteOrigin, manifestDeleteOrigin, RoleWriter, manifestDeleteCluster)
	peer.Address = peerAddr
	if err := registry.Register(peer); err != nil {
		t.Fatalf("register peer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{
		cfg: &config.ClusterConfig{
			ClusterName:                 manifestDeleteCluster,
			SharedSecret:                manifestDeleteSecret,
			ReplicationEnabled:          true,
			ReplicationCatchUpEnabled:   false, // the test drives RunCatchUp itself
			ReplicationPullWorkers:      1,
			ReplicationQueueSize:        8,
			ReplicationRetryMaxAttempts: 2,
			ReplicationFetchTimeoutMs:   5000,
		},
		storage:   backend,
		raftNode:  raftNode,
		localNode: local,
		registry:  registry,
		logger:    logger,
		ctx:       ctx,
	}
	c.mu.Lock()
	err := c.startFilePullerLocked()
	c.mu.Unlock()
	if err != nil {
		cancel()
		t.Fatalf("startFilePullerLocked: %v", err)
	}
	t.Cleanup(func() {
		c.puller.Stop()
		cancel()
		// The delete workers exit only on their stop signal, as in Stop:
		// cancelling the context no longer ends them (that was the leak).
		c.stopDeleteWorkers(c.deleteStop, c.deleteWg)
	})
	return c
}

func (c *Coordinator) catchUpFromFSM(t *testing.T) {
	t.Helper()
	fsm := c.raftNode.FSM()
	c.puller.RunCatchUp(c.ctx, func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		return fsm.GetFilesPaginated(cursor, limit)
	})
}

func waitForStatus(t *testing.T, c *Coordinator, what string, pred func(map[string]int64) bool) map[string]int64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s := c.puller.Stats()
		if pred(s) {
			return s
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; stats=%+v", what, c.puller.Stats())
	return nil
}

// reregisterRig is a reader on a real LocalBackend (or a shared-type backend)
// whose one peer is an origin serving path at oldBody and an unrelated
// registered path at unrelatedBody. After setup the origin serves newBody at
// path (same size) and its manifest lists newEntry; the reader's manifest
// still lists oldEntry.
type reregisterRig struct {
	c                  *Coordinator
	local              storage.Backend
	path               string
	oldBody, newBody   []byte
	oldEntry, newEntry raft.FileEntry
	unrelated          raft.FileEntry
	unrelatedBody      []byte
	// pulledBefore is the reader's pulled count once setup has settled.
	pulledBefore int64
}

type reregisterOpts struct {
	existing  bool // the reader already holds oldBody and the unrelated file on disk
	neverHeld bool // oldEntry is registered as this reader's own file, so it is never pulled
	shared    bool // a backend whose Type() is not "local"
	logger    zerolog.Logger
}

func newReregisterRig(t *testing.T, name string, o reregisterOpts) *reregisterRig {
	t.Helper()
	ctx := context.Background()
	r := &reregisterRig{
		path:          "testdb/cpu/2026/10/08/00/" + name + ".parquet",
		oldBody:       []byte("same-size old parquet payload"),
		newBody:       []byte("same-size new parquet payload"),
		unrelatedBody: []byte("an unrelated registered file"),
	}
	unrelatedPath := "testdb/cpu/2026/10/08/00/" + name + "-unrelated.parquet"

	originBackend := newMemBackend()
	if err := originBackend.Write(ctx, r.path, r.oldBody); err != nil {
		t.Fatal(err)
	}
	if err := originBackend.Write(ctx, unrelatedPath, r.unrelatedBody); err != nil {
		t.Fatal(err)
	}
	originFSM := raft.NewClusterFSM(zerolog.Nop())
	r.oldEntry = makeFileEntry(r.path, r.oldBody, manifestDeleteOrigin)
	r.unrelated = makeFileEntry(unrelatedPath, r.unrelatedBody, manifestDeleteOrigin)
	seedFileInFSM(t, originFSM, r.oldEntry)
	seedFileInFSM(t, originFSM, r.unrelated)
	origin := startOriginServer(t, originBackend, originFSM, manifestDeleteSecret, manifestDeleteCluster, manifestDeleteOrigin)
	t.Cleanup(origin.stop)

	if o.shared {
		r.local = newMemBackend()
	} else {
		local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
		if err != nil {
			t.Fatalf("NewLocalBackend: %v", err)
		}
		t.Cleanup(func() { _ = local.Close() })
		r.local = local
	}
	if o.existing {
		if err := r.local.Write(ctx, r.path, r.oldBody); err != nil {
			t.Fatal(err)
		}
		if err := r.local.Write(ctx, unrelatedPath, r.unrelatedBody); err != nil {
			t.Fatal(err)
		}
	}

	r.c = newManifestDeleteRigWithBackend(t, origin.addr(), r.local, o.logger)
	initial := r.oldEntry
	if o.neverHeld {
		initial.OriginNodeID = manifestDeleteReader
	}
	for _, e := range []raft.FileEntry{initial, r.unrelated} {
		if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
			t.Fatalf("register %s: %v", e.Path, err)
		}
	}
	settled := waitForStatus(t, r.c, "initial files settled", func(s map[string]int64) bool {
		if s["inflight_count"] != 0 {
			return false
		}
		switch {
		case o.existing:
			return s["skipped_local"] == 2
		case o.neverHeld:
			return s["pulled"] == 1 && s["skipped_self"] == 1
		default:
			return s["pulled"] == 2
		}
	})
	r.pulledBefore = settled["pulled"]
	if !o.neverHeld {
		if got, err := r.local.Read(ctx, r.path); err != nil || !bytes.Equal(got, r.oldBody) {
			t.Fatalf("initial version: got %q, err=%v, stats=%v", got, err, settled)
		}
	}

	r.newEntry = makeFileEntry(r.path, r.newBody, manifestDeleteOrigin)
	if err := originBackend.Write(ctx, r.path, r.newBody); err != nil {
		t.Fatal(err)
	}
	seedFileInFSM(t, originFSM, r.newEntry)
	return r
}

// deleteAndReregister applies a delete and a re-registration of the same path
// as one Raft batch, so the delete worker always finds the path listed again
// when its grace period ends.
func (r *reregisterRig) deleteAndReregister(t *testing.T, entry raft.FileEntry) {
	t.Helper()
	deleted, err := json.Marshal(raft.DeleteFilePayload{Path: entry.Path, Reason: "test: same-size re-register"})
	if err != nil {
		t.Fatal(err)
	}
	registered, err := json.Marshal(raft.RegisterFilePayload{File: entry})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.c.raftNode.BatchFileOps([]raft.BatchFileOp{
		{Type: raft.CommandDeleteFile, Payload: deleted},
		{Type: raft.CommandRegisterFile, Payload: registered},
	}, 5*time.Second); err != nil {
		t.Fatalf("delete + re-register: %v", err)
	}
}

// settle waits until the puller and the delete workers are idle and pred
// holds, and returns the stats.
func (r *reregisterRig) settle(t *testing.T, what string, pred func(map[string]int64) bool) map[string]int64 {
	t.Helper()
	return waitForStatus(t, r.c, what, func(s map[string]int64) bool {
		return s["inflight_count"] == 0 && s["queue_depth"] == 0 && r.c.deletesPending() == 0 && pred(s)
	})
}

// requireBytes fails unless path holds want, and the unrelated registered file
// is untouched.
func (r *reregisterRig) requireBytes(t *testing.T, want []byte) {
	t.Helper()
	ctx := context.Background()
	if got, err := r.local.Read(ctx, r.path); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("%s holds %q (err=%v), want %q", r.path, got, err, want)
	}
	if got, err := r.local.Read(ctx, r.unrelated.Path); err != nil || !bytes.Equal(got, r.unrelatedBody) {
		t.Fatalf("the unrelated registered file changed: %q, err=%v", got, err)
	}
}

func (c *Coordinator) deletesPending() int {
	c.deletePendingMu.Lock()
	defer c.deletePendingMu.Unlock()
	return len(c.deletePending) + int(c.deleteInFlight.Load())
}

// #798's shape: the node holds the old version, the path is deleted and
// re-registered at the same size inside the delete grace. The kept copy is
// hashed, found stale and replaced, with exactly one pull and nothing else
// pulled.
func TestManifestDelete_ReregisteredSameSizePathRefreshesLocalCopyIssue798(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := map[bool]string{false: "fresh", true: "existing"}[existing]
		t.Run(name, func(t *testing.T) {
			r := newReregisterRig(t, "issue798-"+name, reregisterOpts{existing: existing, logger: zerolog.Nop()})

			r.deleteAndReregister(t, r.newEntry)

			s := r.settle(t, "same-size replacement installed", func(s map[string]int64) bool {
				return s["pulled"] == r.pulledBefore+1
			})
			r.requireBytes(t, r.newBody)
			if s["forced_refresh"] != 1 || s["refresh_verified"] != 0 {
				t.Fatalf("forced_refresh=%d refresh_verified=%d, want 1 and 0; stats=%v", s["forced_refresh"], s["refresh_verified"], s)
			}
			// What an operator sees in GET /api/v1/cluster.
			if cs := r.c.puller.CatchUpStatus(); cs["forced_refresh"] != 1 || cs["refresh_verified"] != 0 || cs["refresh_pending"] != 0 {
				t.Fatalf("cluster status = %v, want forced_refresh 1, refresh_verified 0, refresh_pending 0", cs)
			}
		})
	}
}

// A node that never held the old version pulls the new one through the
// registration; the refresh then finds it already on disk and downloads
// nothing more.
func TestManifestDelete_ReregisterOnANodeThatNeverHeldThePathPullsOnce(t *testing.T) {
	r := newReregisterRig(t, "never-held", reregisterOpts{neverHeld: true, logger: zerolog.Nop()})

	r.deleteAndReregister(t, r.newEntry)

	s := r.settle(t, "refresh verified", func(s map[string]int64) bool { return s["refresh_verified"] == 1 })
	r.requireBytes(t, r.newBody)
	if s["pulled"] != r.pulledBefore+1 || s["forced_refresh"] != 0 {
		t.Fatalf("pulled=%d (before %d) forced_refresh=%d, want one pull and no forced download; stats=%v",
			s["pulled"], r.pulledBefore, s["forced_refresh"], s)
	}
	if cs := r.c.puller.CatchUpStatus(); cs["refresh_verified"] != 1 {
		t.Fatalf("cluster status = %v, want refresh_verified 1", cs)
	}
}

// A re-registration with the content the node already holds downloads
// nothing.
func TestManifestDelete_ReregisterWithIdenticalContentDownloadsNothing(t *testing.T) {
	r := newReregisterRig(t, "identical", reregisterOpts{logger: zerolog.Nop()})

	r.deleteAndReregister(t, r.oldEntry)

	s := r.settle(t, "refresh verified", func(s map[string]int64) bool { return s["refresh_verified"] == 1 })
	r.requireBytes(t, r.oldBody)
	if s["pulled"] != r.pulledBefore || s["forced_refresh"] != 0 {
		t.Fatalf("pulled=%d (before %d) forced_refresh=%d, want no download; stats=%v",
			s["pulled"], r.pulledBefore, s["forced_refresh"], s)
	}
}

// The ordering a real producer creates: the delete and the re-registration
// are two proposals. Single-node Raft applies the second well inside the
// grace; the precondition fails the test, rather than passing it vacuously,
// if it ever does not.
func TestManifestDelete_ReregisterAsSeparateProposalsRefreshesLocalCopy(t *testing.T) {
	r := newReregisterRig(t, "separate", reregisterOpts{logger: zerolog.Nop()})

	if err := r.c.raftNode.DeleteFile(r.path, "test: delete", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.raftNode.RegisterFile(r.newEntry, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	r.settle(t, "same-size replacement installed", func(s map[string]int64) bool {
		return s["pulled"] == r.pulledBefore+1
	})
	if r.c.deleteRelistedLogged.Load() == 0 {
		t.Fatal("precondition: the re-registration landed after the delete grace, so the relisted path was not exercised")
	}
	r.requireBytes(t, r.newBody)
}

// Re-registering after the unlink completed takes the ordinary path: the
// copy is gone, the registration pulls the new version, nothing is forced.
func TestManifestDelete_ReregisterAfterUnlinkStillConverges(t *testing.T) {
	r := newReregisterRig(t, "after-unlink", reregisterOpts{logger: zerolog.Nop()})
	ctx := context.Background()

	if err := r.c.raftNode.DeleteFile(r.path, "test: delete", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		exists, err := r.local.Exists(ctx, r.path)
		if err == nil && !exists && r.c.deletesPending() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the delete never unlinked %s", r.path)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := r.c.raftNode.RegisterFile(r.newEntry, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	s := r.settle(t, "new version pulled", func(s map[string]int64) bool { return s["pulled"] == r.pulledBefore+1 })
	r.requireBytes(t, r.newBody)
	if s["forced_refresh"] != 0 || r.c.deleteRelistedLogged.Load() != 0 {
		t.Fatalf("forced_refresh=%d relisted=%d, want the ordinary path; stats=%v", s["forced_refresh"], r.c.deleteRelistedLogged.Load(), s)
	}
}

// On a shared backend the object belongs to the writer: a manifest delete
// never reaches the delete workers, so nothing is unlinked and no refresh is
// asked for.
func TestManifestDelete_SharedBackendNeverUnlinksOrRefreshes(t *testing.T) {
	r := newReregisterRig(t, "shared", reregisterOpts{shared: true, logger: zerolog.Nop()})
	ctx := context.Background()

	if err := r.c.raftNode.DeleteFile(r.unrelated.Path, "test: delete", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.deleteAndReregister(t, r.newEntry)
	// A negative: wait out several grace periods, then look.
	time.Sleep(4 * deleteGrace)

	if exists, err := r.local.Exists(ctx, r.unrelated.Path); err != nil || !exists {
		t.Fatalf("a shared-backend delete unlinked %s (exists=%v, err=%v)", r.unrelated.Path, exists, err)
	}
	s := r.c.puller.Stats()
	if r.c.deletesPending() != 0 || r.c.deleteRelistedLogged.Load() != 0 || s["forced_refresh"] != 0 || s["refresh_verified"] != 0 {
		t.Fatalf("pending=%d relisted=%d stats=%v, want no delete-worker activity", r.c.deletesPending(), r.c.deleteRelistedLogged.Load(), s)
	}
}

// Coordinator.Stop stops the puller before it drains the delete workers, so
// a refresh asked for during the drain cannot run. The copy is kept (it may
// be the only one) and the node says the copy may be stale.
func TestManifestDelete_ReregisterAfterPullerStopWarns(t *testing.T) {
	var logs lockedLogBuffer
	r := newReregisterRig(t, "stopped", reregisterOpts{logger: zerolog.New(&logs)})

	r.c.puller.Stop()
	r.deleteAndReregister(t, r.newEntry)

	deadline := time.Now().Add(10 * time.Second)
	for r.c.deletesPending() != 0 || !strings.Contains(logs.String(), "file puller is stopped") {
		if time.Now().After(deadline) {
			t.Fatalf("no stopped-puller warning; pending deletes=%d, log=%s", r.c.deletesPending(), logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.requireBytes(t, r.oldBody)
	if strings.Contains(logs.String(), "refreshing the local copy") {
		t.Fatalf("logged a refresh that a stopped puller cannot run: %s", logs.String())
	}
}

// Ordering 1: every peer answers not-found, the pull gives up, the gate is
// red; deleting the entry through Raft reopens it with no restart. The origin
// knows the entry (it is in its manifest) but holds no bytes for it, which is
// the live shape of #759.
func TestManifestDelete_ReopensGateAfterFailedCatchUp(t *testing.T) {
	originFSM := raft.NewClusterFSM(zerolog.Nop())
	body := []byte("bytes no peer holds any more")
	const path = "testdb/cpu/2026/09/14/17/unpullable.parquet"
	entry := makeFileEntry(path, body, manifestDeleteOrigin)
	entry.CreatedAt = time.Now()
	seedFileInFSM(t, originFSM, entry)
	origin := startOriginServer(t, newMemBackend(), originFSM, manifestDeleteSecret, manifestDeleteCluster, manifestDeleteOrigin)
	defer origin.stop()

	c, _ := newManifestDeleteRig(t, origin.addr())

	if err := c.raftNode.RegisterFile(entry, 5*time.Second); err != nil {
		t.Fatalf("RegisterFile: %v", err)
	}
	c.catchUpFromFSM(t)

	waitForStatus(t, c, "catch-up pull to give up", func(s map[string]int64) bool {
		return s["catchup_failed"] == 1 && s["inflight_count"] == 0
	})
	if c.puller.FullyCaughtUp() {
		t.Fatal("precondition: gate must be red with a failed catch-up pull")
	}

	if err := c.DeleteFileFromManifest(context.Background(), path, "test: no peer holds it"); err != nil {
		t.Fatalf("DeleteFileFromManifest: %v", err)
	}
	// Apply is synchronous through the FSM, so the callback has already run.
	if !c.puller.FullyCaughtUp() {
		t.Fatalf("gate must reopen as soon as the entry leaves the manifest; status=%+v", c.puller.CatchUpStatus())
	}
	if _, ok := c.raftNode.FSM().GetFile(path); ok {
		t.Error("entry must be gone from the manifest")
	}
	if s := c.puller.CatchUpStatus(); s["catchup_failed"] != 0 || s["catchup_inflight"] != 0 {
		t.Errorf("want catchup_failed=0 catchup_inflight=0, got %+v", s)
	}
}

// Ordering 2 (#795): the delete lands while the reader's pull is blocked on a
// stalled peer. The gate must reopen before the pull is released, and the
// abandoned pull must record nothing when it finally fails.
func TestManifestDelete_ReopensGateWhilePullInflight(t *testing.T) {
	peer := startHangingOrigin(t)
	c, _ := newManifestDeleteRig(t, peer.addr())

	body := []byte("bytes behind a peer that never answers")
	const path = "testdb/cpu/2026/09/14/17/stalled.parquet"
	entry := makeFileEntry(path, body, manifestDeleteOrigin)
	entry.CreatedAt = time.Now()
	if err := c.raftNode.RegisterFile(entry, 5*time.Second); err != nil {
		t.Fatalf("RegisterFile: %v", err)
	}
	// The reactive pull from onRegister is now dialing the peer.
	select {
	case <-peer.connected:
	case <-time.After(10 * time.Second):
		t.Fatal("reader never connected to the peer")
	}
	// The startup walker tags the same path while that pull is in flight.
	c.catchUpFromFSM(t)
	s := c.puller.CatchUpStatus()
	if s["catchup_inflight"] != 1 || s["completed_at"] == 0 {
		t.Fatalf("precondition: walker done, one tagged pull in flight; got %+v", s)
	}
	if c.puller.FullyCaughtUp() {
		t.Fatal("precondition: gate must be red while the tagged pull is in flight")
	}

	if err := c.DeleteFileFromManifest(context.Background(), path, "test: deleted mid-pull"); err != nil {
		t.Fatalf("DeleteFileFromManifest: %v", err)
	}
	if !c.puller.FullyCaughtUp() {
		t.Fatalf("gate must reopen the moment the entry leaves the manifest, before the pull is released; status=%+v", c.puller.CatchUpStatus())
	}

	peer.releaseAll() // the blocked fetch now fails with EOF
	final := waitForStatus(t, c, "abandoned pull to finish", func(s map[string]int64) bool {
		return s["inflight_count"] == 0
	})
	if final["catchup_failed"] != 0 || final["failed"] != 0 {
		t.Errorf("a pull for a deleted entry must not count anywhere: catchup_failed=%d failed=%d", final["catchup_failed"], final["failed"])
	}
	if final["skipped_gone"] != 1 {
		t.Errorf("skipped_gone: want 1 (retry skipped because the entry is gone), got %d", final["skipped_gone"])
	}
	if !c.puller.FullyCaughtUp() {
		t.Errorf("gate must stay open; status=%+v", c.puller.CatchUpStatus())
	}
}
