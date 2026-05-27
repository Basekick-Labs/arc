package cluster

// Multi-node Pattern 2 multi-writer integration test.
//
// Wires three real raft.Nodes (one bootstrap, two joiners) and three
// minimal Coordinator literals with cfg.SharedStorageMode=true, then
// verifies the load-bearing behaviours from PR1a end-to-end:
//
//   1. Initial state: the bootstrap writer is Raft leader →
//      IsPrimaryWriter() returns true on writer-A, false on
//      writer-B and writer-C.
//   2. Role gate (B1 regression check): if writer-A's local role is
//      mutated to RoleReader while still Raft leader, IsPrimaryWriter()
//      flips to false. This is the B1 fix from PR1a internal review —
//      a non-writer that wins Raft election must NOT run singleton
//      tasks.
//   3. Leader change: stop writer-A's Raft node entirely; within a few
//      seconds one of the remaining writers (writer-B or writer-C)
//      becomes leader and reports IsPrimaryWriter()=true.
//
// Why 3 nodes, not 2: hashicorp/raft quorum is N/2 + 1. With 2 nodes,
// killing one leaves the survivor alone and unable to form quorum to
// elect a new leader — singleton tasks would pause indefinitely (the
// documented Pattern 2 behaviour at writer.replicas=2 in the chart).
// 3 nodes is the production-realistic minimum for HA: tolerates 1
// failure and still elects a new leader for singleton tasks.
//
// Why a real-Raft test instead of an in-process mock: the only
// alternative — stubbing raftNode.IsLeader() — would not exercise
// the actual election + leader-change behaviour the singleton-task
// gate depends on. The docker-compose smoke covers the full
// container-level path; this test covers the in-process Coordinator
// gate logic the smoke can't observe directly.
//
// Port allocation: we pre-bind+close TCP listeners on 127.0.0.1:0
// to grab free ephemeral ports, then hand those to the
// raft.NodeConfig BindAddr. Racy on extremely loaded CI runners but
// standard idiom for in-process Raft tests.

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// allocFreePort grabs a free TCP port on 127.0.0.1 and immediately
// releases it, so raft.NetworkTransport can bind to it next. Standard
// race-y idiom; the race window is small enough in practice that all
// other in-process Raft tests in the ecosystem use it.
func allocFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("alloc free port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// startRaftNode constructs and starts a raft.Node bound to the given
// address. If bootstrap is true, the node bootstraps a new cluster
// (becomes its own leader after election). Test takes ownership;
// caller defers Stop.
func startRaftNode(t *testing.T, nodeID, bindAddr string, bootstrap bool) *raft.Node {
	t.Helper()
	fsm := raft.NewClusterFSM(zerolog.Nop())
	cfg := &raft.NodeConfig{
		NodeID:        nodeID,
		DataDir:       filepath.Join(t.TempDir(), nodeID),
		BindAddr:      bindAddr,
		AdvertiseAddr: bindAddr,
		Bootstrap:     bootstrap,
		// Aggressive timeouts to keep the test fast. Constraint:
		// LeaderLeaseTimeout must be <= HeartbeatTimeout (hashicorp/raft
		// validation). 100ms heartbeat → election typically within
		// 200-500ms; full test runs in ~2-5 seconds.
		ElectionTimeout:    200 * time.Millisecond,
		HeartbeatTimeout:   100 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		Logger:             zerolog.Nop(),
	}
	n, err := raft.NewNode(cfg, fsm)
	if err != nil {
		t.Fatalf("NewNode %s: %v", nodeID, err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start %s: %v", nodeID, err)
	}
	return n
}

// waitFor polls predicate up to timeout, returning true on success.
// Used to wait for leader change to propagate to the follower's
// IsLeader() view.
func waitFor(t *testing.T, timeout time.Duration, predicate func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

// TestMultiWriter_SharedStorageMode_LeaderElection_And_RoleGate is
// the load-bearing integration test for PR1a's Pattern 2 multi-writer
// semantics. It runs a 3-node Raft cluster end-to-end and verifies
// the three properties documented at the top of this file.
//
// The test is **flaky-sensitive to Raft election timing**: with the
// aggressive timeouts in startRaftNode (200ms election, 100ms
// heartbeat), election typically completes within 500ms, but a
// heavily-loaded CI runner could go longer. The waitFor timeouts are
// 10s each — generous enough to absorb scheduling jitter without
// sleeping unnecessarily on healthy runs.
func TestMultiWriter_SharedStorageMode_LeaderElection_And_RoleGate(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-writer integration test takes ~5-10s; skipped in -short mode")
	}

	// Pre-allocate three ephemeral ports.
	addrA := allocFreePort(t)
	addrB := allocFreePort(t)
	addrC := allocFreePort(t)

	// --- Boot writer-A as the bootstrap node ---
	raftA := startRaftNode(t, "writer-A", addrA, true)
	defer func() { _ = raftA.Stop() }()

	if err := raftA.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("writer-A WaitForLeader: %v", err)
	}
	if !raftA.IsLeader() {
		t.Fatal("writer-A should be Raft leader after bootstrap")
	}

	// --- Boot writer-B and writer-C as non-bootstrap joiners ---
	raftB := startRaftNode(t, "writer-B", addrB, false)
	defer func() { _ = raftB.Stop() }()
	raftC := startRaftNode(t, "writer-C", addrC, false)
	defer func() { _ = raftC.Stop() }()

	// writer-A adds B and C as voters (mirrors coordinator.go AddVoter
	// path at handleJoinRequest).
	if err := raftA.AddVoter("writer-B", addrB, 5*time.Second); err != nil {
		t.Fatalf("AddVoter writer-B: %v", err)
	}
	if err := raftA.AddVoter("writer-C", addrC, 5*time.Second); err != nil {
		t.Fatalf("AddVoter writer-C: %v", err)
	}

	// Both followers should recognise writer-A as leader.
	waitFor(t, 5*time.Second, func() bool {
		return raftB.LeaderID() == "writer-A"
	}, "writer-B to recognise writer-A as leader")
	waitFor(t, 5*time.Second, func() bool {
		return raftC.LeaderID() == "writer-A"
	}, "writer-C to recognise writer-A as leader")

	if raftB.IsLeader() || raftC.IsLeader() {
		t.Fatal("only writer-A should be leader at this point")
	}

	// --- Construct minimal Coordinators for all three nodes ---
	// We don't call NewCoordinator (requires license + lots of
	// dependencies); we construct the literal with just the fields
	// IsPrimaryWriter() and the methods we test touch.
	coordA := &Coordinator{
		cfg:       &config.ClusterConfig{SharedStorageMode: true},
		raftNode:  raftA,
		localNode: NewNode("writer-A", "writer-A", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}
	coordB := &Coordinator{
		cfg:       &config.ClusterConfig{SharedStorageMode: true},
		raftNode:  raftB,
		localNode: NewNode("writer-B", "writer-B", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}
	coordC := &Coordinator{
		cfg:       &config.ClusterConfig{SharedStorageMode: true},
		raftNode:  raftC,
		localNode: NewNode("writer-C", "writer-C", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}

	// --- Property 1: leader + RoleWriter => IsPrimaryWriter() true ---
	if !coordA.IsPrimaryWriter() {
		t.Error("writer-A: SharedStorageMode + Raft leader + RoleWriter must report IsPrimaryWriter()=true")
	}
	if coordB.IsPrimaryWriter() || coordC.IsPrimaryWriter() {
		t.Error("only the Raft leader should report IsPrimaryWriter()=true")
	}

	// --- Property 2: B1 regression — non-writer leader is rejected ---
	// Mutate writer-A's role to RoleReader while still Raft leader.
	// The B1 fix in PR1a means IsPrimaryWriter() must flip to false:
	// only RoleWriter may run singleton tasks, even if elected leader.
	coordA.localNode.Role = RoleReader
	if coordA.IsPrimaryWriter() {
		t.Error("writer-A: RoleReader + Raft leader must NOT report IsPrimaryWriter()=true (B1 regression)")
	}
	// Restore role for the next subtest.
	coordA.localNode.Role = RoleWriter

	// --- Property 3: leader change propagates ---
	// Stop writer-A's raft node entirely. With 2 of 3 nodes remaining,
	// Raft still has quorum and one of B/C will be elected leader
	// within a few election cycles.
	if err := raftA.Stop(); err != nil {
		t.Fatalf("stop writer-A: %v", err)
	}

	// Within 10s, exactly one of writer-B or writer-C must become
	// leader. 10s is 50x the election timeout — absorbs CI jitter.
	waitFor(t, 10*time.Second, func() bool {
		return raftB.IsLeader() || raftC.IsLeader()
	}, "writer-B or writer-C to become Raft leader after writer-A stops")

	// Exactly one new leader, not both.
	bLeader, cLeader := raftB.IsLeader(), raftC.IsLeader()
	if bLeader && cLeader {
		t.Fatal("both writer-B and writer-C report themselves as leader (split-brain)")
	}

	// The new leader's Coordinator must report IsPrimaryWriter()=true.
	if bLeader && !coordB.IsPrimaryWriter() {
		t.Error("writer-B: after leader change, IsPrimaryWriter must report true")
	}
	if cLeader && !coordC.IsPrimaryWriter() {
		t.Error("writer-C: after leader change, IsPrimaryWriter must report true")
	}

	// The non-leader follower must NOT report itself as primary.
	if bLeader && coordC.IsPrimaryWriter() {
		t.Error("writer-C: follower must report IsPrimaryWriter()=false")
	}
	if cLeader && coordB.IsPrimaryWriter() {
		t.Error("writer-B: follower must report IsPrimaryWriter()=false")
	}

	// writer-A is no longer a Raft leader (it stopped), so its
	// IsPrimaryWriter() must report false regardless of its role.
	if coordA.IsPrimaryWriter() {
		t.Error("writer-A: stopped node must report IsPrimaryWriter()=false")
	}
}

// TestMultiWriter_SharedStorageMode_SingleNode_IsPrimary ensures the
// production-style Raft (with Bootstrap + actual election) wires
// through IsPrimaryWriter() correctly. The unit tests in
// coordinator_test.go use a non-started raft.Node (covers the nil-
// and not-started-leader code paths); this covers the actual-leader
// path with a real election.
func TestMultiWriter_SharedStorageMode_SingleNode_IsPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; skipped in -short mode")
	}

	addr := allocFreePort(t)
	r := startRaftNode(t, "writer-solo", addr, true)
	defer func() { _ = r.Stop() }()

	if err := r.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	c := &Coordinator{
		cfg:       &config.ClusterConfig{SharedStorageMode: true},
		raftNode:  r,
		localNode: NewNode("writer-solo", "writer-solo", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}

	if !c.IsPrimaryWriter() {
		t.Error("single-node cluster: bootstrap writer with SharedStorageMode must report IsPrimaryWriter()=true")
	}

	// Stop the only Raft node — IsLeader() flips to false.
	if err := r.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Briefly wait for the in-process Raft state to settle.
	time.Sleep(100 * time.Millisecond)

	if c.IsPrimaryWriter() {
		t.Error("stopped Raft: IsPrimaryWriter must report false")
	}
}
