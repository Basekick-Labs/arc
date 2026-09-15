package cluster

// Query-gate self-heal on manifest delete (#759, #795), driven through the
// production wiring: a bootstrapped Raft node, startFilePullerLocked (the real
// onRegister/onDelete callbacks and the real ManifestHas hook), the real fetch
// client against a peer, and DeleteFileFromManifest on the leader path.
//
// Two orderings against the reader's catch-up pull:
//   1. the pull failed first, then the entry left the manifest;
//   2. the entry left the manifest while the pull was blocked on the peer.
// On main, ordering 1 keeps the gate red until restart; on the #790 shape,
// ordering 2 does.

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
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
	backend := newMemBackend()
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
		logger:    zerolog.Nop(),
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
		c.deleteWg.Wait()
	})
	return c, backend
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
