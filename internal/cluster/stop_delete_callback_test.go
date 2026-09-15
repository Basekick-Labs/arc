package cluster

// Shutdown vs FSM callbacks (#797). Coordinator.Stop holds c.mu while it stops
// the Raft node, and Raft's shutdown joins the apply goroutine, so an FSM
// callback that takes c.mu (or a Node method that takes n.mu) deadlocks the
// process whenever an entry is applied while the node is stopping. Two tests:
// an invariant that holds c.mu and applies every wired command type, and the
// real Stop with a delete applied inside its locked window.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// newShutdownRig builds a coordinator around a real single-node Raft leader
// with the production FSM callbacks wired the way NewCoordinator and
// startFilePullerLocked wire them, and with everything Coordinator.Stop
// dereferences (running, stopCh, cancel, a never-started health checker).
// No cleanup is registered for the Raft node: Stop owns it, and a test that
// hits the deadlock must leak rather than hang the package on n.mu.
func newShutdownRig(t *testing.T, peerAddr string, fetchTimeoutMs int) (*Coordinator, *raft.Node) {
	t.Helper()
	raftNode := startRaftNode(t, "reader-1", allocFreePort(t), true)
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	local := NewNode("reader-1", "reader-1", RoleReader, manifestDeleteCluster)
	registry := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	peer := NewNode("writer-1", "writer-1", RoleWriter, manifestDeleteCluster)
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
			ReplicationCatchUpEnabled:   false,
			ReplicationPullWorkers:      1,
			ReplicationQueueSize:        8,
			ReplicationRetryMaxAttempts: 1,
			ReplicationFetchTimeoutMs:   fetchTimeoutMs,
		},
		storage:       newMemBackend(),
		raftNode:      raftNode,
		raftFSM:       raftNode.FSM(),
		localNode:     local,
		registry:      registry,
		logger:        zerolog.Nop(),
		ctx:           ctx,
		cancel:        cancel,
		stopCh:        make(chan struct{}),
		running:       true,
		healthChecker: NewHealthChecker(&HealthCheckerConfig{Registry: registry, Logger: zerolog.Nop()}),
	}
	fsm := raftNode.FSM()
	fsm.SetCallbacks(
		func(n *raft.NodeInfo) { c.onRaftNodeAdded(n) },
		func(id string) { c.onRaftNodeRemoved(id) },
		func(n *raft.NodeInfo) { c.onRaftNodeUpdated(n) },
	)
	fsm.SetWriterPromotedCallback(func(newPrimaryID, oldPrimaryID string) { c.onWriterPromoted(newPrimaryID, oldPrimaryID) })
	fsm.SetCompactorAssignedCallback(func(newID, oldID string) { c.onCompactorAssigned(newID, oldID) })
	c.mu.Lock()
	err := c.startFilePullerLocked()
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("startFilePullerLocked: %v", err)
	}
	return c, raftNode
}

func registerTestFile(t *testing.T, raftNode *raft.Node, path string) {
	t.Helper()
	entry := makeFileEntry(path, []byte("x"), "writer-1")
	entry.CreatedAt = time.Now()
	if err := raftNode.RegisterFile(entry, 5*time.Second); err != nil {
		t.Fatalf("RegisterFile %s: %v", path, err)
	}
}

// Every command type that reaches a wired callback must apply while the
// coordinator holds its write lock: that is exactly the state Stop is in when
// it joins the Raft node, and a callback that needs c.mu there is a shutdown
// deadlock. Completion is what matters; a command may legitimately return an
// FSM error (e.g. promoting an unknown node).
func TestFSMCallbacks_ApplyWhileCoordinatorLockHeld(t *testing.T) {
	c, raftNode := newShutdownRig(t, allocFreePort(t), 1000) // peer port closed: pulls fail fast
	const f1, f2, f3 = "testdb/cpu/2026/09/14/21/lock-1.parquet", "testdb/cpu/2026/09/14/21/lock-2.parquet", "testdb/cpu/2026/09/14/21/lock-3.parquet"
	registerTestFile(t, raftNode, f1)
	registerTestFile(t, raftNode, f2) // a delete of an unknown path fires no callback

	delPayload, _ := json.Marshal(raft.DeleteFilePayload{Path: f2, Reason: "test"})
	f3Entry := makeFileEntry(f3, []byte("x"), "writer-1")
	f3Entry.CreatedAt = time.Now()
	commands := map[string]func() error{
		"DeleteFile": func() error { return raftNode.DeleteFile(f1, "test", 5*time.Second) },
		"BatchFileOps": func() error {
			return raftNode.BatchFileOps([]raft.BatchFileOp{{Type: raft.CommandDeleteFile, Payload: delPayload}}, 5*time.Second)
		},
		"RegisterFile": func() error { return raftNode.RegisterFile(f3Entry, 5*time.Second) },
		"AddNode": func() error {
			return raftNode.AddNode(&raft.NodeInfo{ID: "x", Role: string(RoleReader), Address: "127.0.0.1:1"}, 5*time.Second)
		},
		"UpdateNodeState": func() error { return raftNode.UpdateNodeState("x", string(StateHealthy), 5*time.Second) },
		"RemoveNode":      func() error { return raftNode.RemoveNode("x", 5*time.Second) },
		"PromoteWriter":   func() error { return raftNode.PromoteWriter("reader-1", "", 5*time.Second) },
		"AssignCompactor": func() error { return raftNode.AssignCompactor("reader-1", "", 5*time.Second) },
	}

	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(commands))
	c.mu.Lock()
	for name, apply := range commands {
		go func(name string, apply func() error) { results <- result{name, apply()} }(name, apply)
	}
	done := map[string]bool{}
	deadline := time.After(10 * time.Second) // failure-only budget; the pass path takes tens of ms
collect:
	for len(done) < len(commands) {
		select {
		case r := <-results:
			done[r.name] = true
			t.Logf("%s applied with c.mu held (err=%v)", r.name, r.err)
		case <-deadline:
			break collect
		}
	}
	c.mu.Unlock() // let any blocked callback finish before teardown
	var stuck []string
	for name := range commands {
		if !done[name] {
			stuck = append(stuck, name)
		}
	}
	if len(stuck) > 0 {
		t.Errorf("commands whose FSM callback blocked on the coordinator lock (Stop would deadlock): %v", stuck)
	}
	for len(done) < len(commands) { // drain the stragglers released by the unlock
		r := <-results
		done[r.name] = true
	}
	if err := c.Stop(); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

// The real Stop with a delete applied inside its locked window. A pull in
// flight against a peer that never answers keeps puller.Stop, and therefore
// c.mu, held for the fetch timeout; a DeleteFile committed in that window is
// applied on the Raft goroutine Stop is about to join. Before #797 this hung
// forever; the test then reports the deadlock and leaks the rig on purpose.
func TestStop_DoesNotDeadlockOnDeleteAppliedDuringShutdown(t *testing.T) {
	peer := startHangingOrigin(t)
	c, raftNode := newShutdownRig(t, peer.addr(), 2000)
	const path = "testdb/cpu/2026/09/14/21/stop.parquet"
	registerTestFile(t, raftNode, path) // reactive pull -> connects to the peer and hangs
	select {
	case <-peer.connected:
	case <-time.After(10 * time.Second):
		t.Fatal("the reader never connected to the peer")
	}

	stopDone := make(chan error, 1)
	start := time.Now()
	go func() { stopDone <- c.Stop() }()

	// Wait until Stop is joining the puller (it is waiting on the hung
	// fetch). Since #813 Stop holds c.mu only for microseconds, so the
	// signal is the puller pointer it clears before the join, not the lock.
	joining := false
	for i := 0; i < 600 && !joining; i++ {
		if c.ReplicationCatchUpStatus() != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		joining = true
	}
	if !joining {
		t.Fatal("Stop never reached the puller join")
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- raftNode.DeleteFile(path, "test: applied during shutdown", 5*time.Second) }()
	// The delete must apply (callback included) while Stop is inside the
	// puller join; only then release the hung fetch so Stop can move on to
	// joining the Raft node. (Left alone, the fetch holds the lock for the
	// protocol's 15 s header timeout regardless of the fetch timeout, #796.)
	// Pre-fix the callback blocks here and this wait times out; the release
	// then lets Stop reach the Raft join, where it deadlocks.
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("DeleteFile during shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Log("delete did not apply within 5s while Stop was joining the puller (expected only on the pre-fix code)")
	}
	peer.releaseAll()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
		t.Logf("Stop returned after %s with a delete applied inside its puller join", time.Since(start))
	case <-time.After(15 * time.Second):
		t.Fatalf("Stop did not return within 15s: the FSM delete callback is blocked on the coordinator lock while Raft shutdown waits for it (#797); rig leaked on purpose")
	}
}

// localTypedBackend makes the callback take the local-unlink branch, which
// sends on the delete queue, so a stale callback after Stop would be visible.
type localTypedBackend struct{ *memBackend }

func (localTypedBackend) Type() string { return "local" }

// Stop must unregister the file callbacks it captured a queue for. An
// in-process Start restarts Raft, which replays the log into the same FSM
// before fresh callbacks are registered; a leftover callback from the
// previous life would send on the closed queue and panic.
func TestStop_UnregistersFileCallbacksBeforeClosingTheQueue(t *testing.T) {
	c, raftNode := newShutdownRig(t, allocFreePort(t), 1000)
	c.storage = localTypedBackend{newMemBackend()}
	// Rebuild the closures with the local-typed backend captured.
	c.mu.Lock()
	c.puller.Stop()
	c.puller = nil
	err := c.startFilePullerLocked()
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("startFilePullerLocked: %v", err)
	}
	const path = "testdb/cpu/2026/09/14/21/replayed.parquet"
	registerTestFile(t, raftNode, path)
	if err := raftNode.DeleteFile(path, "test", 5*time.Second); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Restart Raft alone, as Coordinator.Start does before it re-registers
	// callbacks: the log (register + delete) replays into the same FSM.
	if err := raftNode.Start(); err != nil {
		t.Fatalf("restart raft: %v", err)
	}
	defer func() { _ = raftNode.Stop() }()
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader after restart: %v", err)
	}
	// A fresh delete after replay must be a no-op too (no callback registered).
	registerTestFile(t, raftNode, path+".2")
	if err := raftNode.DeleteFile(path+".2", "test", 5*time.Second); err != nil {
		t.Fatalf("DeleteFile after restart: %v", err)
	}
}
