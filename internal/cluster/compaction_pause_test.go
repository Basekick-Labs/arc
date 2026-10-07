package cluster

// Coordinator tests for the cluster-wide compaction pause (#1087): the pause,
// quiesce, ack and resume round trip on a real single-node Raft; the wait set;
// expiry by the local clock; the forward allowlist and its identity check on
// the real leader-forwarding path; and the refresher marking a handle lost.

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// newPauseTestCoordinator builds the smallest coordinator that can pause:
// a node, a registry, the Raft node and its FSM. The constructor wires the
// FSM callback; this rig builds the struct by hand, so it wires it too.
func newPauseTestCoordinator(t *testing.T, id string, role NodeRole, raftNode *raft.Node) *Coordinator {
	t.Helper()
	c := newPauseForwardingCoordinator(t, id, role, raftNode)
	raftNode.FSM().SetCompactionPauseCallback(func(paused bool, gen uint64) { c.onCompactionPauseChanged(paused, gen) })
	return c
}

// newPauseForwardingCoordinator is the same without the FSM callback, for
// tests that drive the forwarded commands by hand and must not have the node
// quiesce and ack on its own.
func newPauseForwardingCoordinator(t *testing.T, id string, role NodeRole, raftNode *raft.Node) *Coordinator {
	t.Helper()
	local := NewNode(id, id, role, barrierTestCluster)
	c := &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: barrierTestCluster, SharedSecret: barrierTestSecret},
		localNode: local,
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()}),
		raftNode:  raftNode,
		raftFSM:   raftNode.FSM(),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}
	t.Cleanup(c.closeForwardConn)
	return c
}

func startSingleNodeRaft(t *testing.T, id string) *raft.Node {
	t.Helper()
	rn := startRaftNode(t, id, allocFreePort(t), true)
	t.Cleanup(func() { _ = rn.Stop() })
	if err := rn.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	return rn
}

func (h *CompactionPauseHandle) lostFlag() (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lost, h.lostErr
}

// The full round trip on one node: PauseCompaction does not return until the
// quiescer has, the FSM then holds the local ack, the pause is in force for
// the compaction gate, and Resume clears it.
func TestCompactionPause_PauseWaitsForTheQuiescerThenAcks(t *testing.T) {
	rn := startSingleNodeRaft(t, "writer-A")
	c := newPauseTestCoordinator(t, "writer-A", RoleWriter, rn)

	quiesceStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c.SetCompactionQuiescer(func(ctx context.Context) error {
		once.Do(func() { close(quiesceStarted) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	type result struct {
		h   *CompactionPauseHandle
		err error
	}
	done := make(chan result, 1)
	go func() {
		h, err := c.PauseCompaction(context.Background(), "restore b1")
		done <- result{h, err}
	}()
	select {
	case <-quiesceStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("the quiescer never ran")
	}
	select {
	case r := <-done:
		t.Fatalf("PauseCompaction returned before this node quiesced: %v", r.err)
	case <-time.After(500 * time.Millisecond):
	}
	if !c.CompactionPaused() {
		t.Fatal("the pause must be in force for the compaction gate while nodes quiesce")
	}
	close(release)
	var r result
	select {
	case r = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("PauseCompaction did not return after the quiescer")
	}
	if r.err != nil {
		t.Fatalf("PauseCompaction: %v", r.err)
	}
	s := rn.FSM().GetCompactionPause()
	if !s.Active || s.Generation != 1 || s.RequestedBy != "writer-A" || s.Reason != "restore b1" || s.Acks["writer-A"] != 1 {
		t.Fatalf("FSM state = %+v, want generation 1 by writer-A acked by writer-A", s)
	}
	status := c.CompactionPauseStatus()
	if status["active"] != true || status["requested_by"] != "writer-A" {
		t.Fatalf("status = %v", status)
	}
	if acks, _ := status["acks"].([]string); len(acks) != 1 || acks[0] != "writer-A" {
		t.Fatalf("status acks = %v", status["acks"])
	}
	if pending, ok := status["pending_acks"].([]string); !ok || len(pending) != 0 {
		t.Fatalf("status pending_acks = %v, want an empty list once every node acked", status["pending_acks"])
	}
	if reason := c.CompactionPauseReason(); !strings.Contains(reason, "restore b1") {
		t.Fatalf("reason = %q", reason)
	}
	if lost, err := r.h.Lost(); lost {
		t.Fatalf("a held pause reports lost: %v", err)
	}

	if err := r.h.Resume(context.Background()); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if c.CompactionPaused() {
		t.Fatal("still paused after Resume")
	}
	if s := rn.FSM().GetCompactionPause(); s.Active || s.Generation != 1 {
		t.Fatalf("FSM state after resume = %+v", s)
	}
	if c.CompactionPauseStatus()["active"] != false || c.CompactionPauseReason() != "" {
		t.Fatal("status still reports a pause after Resume")
	}
	if err := r.h.Resume(context.Background()); err != nil {
		t.Fatalf("second Resume: %v", err)
	}
}

// Whether a pause is in force is decided on the reading node, from its clock:
// a record whose expiry has passed is not a pause, and the next pause takes it
// over without the requester's help.
func TestCompactionPause_ExpiryIsDecidedLocally(t *testing.T) {
	rn := startSingleNodeRaft(t, "writer-A")
	c := newPauseTestCoordinator(t, "writer-A", RoleWriter, rn)

	stale := time.Now().Add(-5 * time.Minute)
	if err := rn.SetCompactionPause(raft.SetCompactionPausePayload{
		Paused: true, Generation: 1, RequestedBy: "writer-gone", Reason: "restore old",
		Now: stale, ExpiresAt: stale.Add(2 * time.Minute),
	}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if c.CompactionPaused() {
		t.Fatal("an expired record reads as paused")
	}
	status := c.CompactionPauseStatus()
	if status["active"] != false || status["expired"] != true || status["requested_by"] != "writer-gone" {
		t.Fatalf("status = %v, want inactive, expired, still naming the requester", status)
	}
	if c.CompactionPauseReason() != "" {
		t.Fatal("an expired record reports a reason")
	}

	h, err := c.PauseCompaction(context.Background(), "restore new")
	if err != nil {
		t.Fatalf("PauseCompaction over an expired record: %v", err)
	}
	if h.Generation() != 2 || !c.CompactionPaused() {
		t.Fatalf("generation = %d paused = %v, want 2 and paused", h.Generation(), c.CompactionPaused())
	}
	if err := h.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// The Start-time path: a pause that is already in the FSM when this process
// starts (restored from the local snapshot, or applied before the quiescer
// was reachable) fires no callback this process acts on, so Start spawns
// ackCompactionPauseIfActive. Run here directly, on a rig WITHOUT the FSM
// callback, so the ack can come from nothing else. An expired record is left
// alone.
func TestCompactionPause_StartTimeAckOfAnActivePause(t *testing.T) {
	rn := startSingleNodeRaft(t, "writer-A")
	c := newPauseForwardingCoordinator(t, "writer-A", RoleWriter, rn)
	ran := make(chan struct{}, 1)
	c.SetCompactionQuiescer(func(ctx context.Context) error { ran <- struct{}{}; return nil })

	now := time.Now()
	if err := rn.SetCompactionPause(raft.SetCompactionPausePayload{
		Paused: true, Generation: 1, RequestedBy: "primary-X", Reason: "restore b1",
		Now: now, ExpiresAt: now.Add(2 * time.Minute),
	}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if s := rn.FSM().GetCompactionPause(); len(s.Acks) != 0 {
		t.Fatalf("precondition: an ack landed without the start-time path: %v", s.Acks)
	}
	c.ackCompactionPauseIfActive()
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the start-time path did not run the quiescer")
	}
	if s := rn.FSM().GetCompactionPause(); s.Acks["writer-A"] != 1 {
		t.Fatalf("FSM acks after the start-time path = %v, want writer-A at generation 1", s.Acks)
	}
	// Idempotent: a second run neither quiesces again nor fails.
	c.ackCompactionPauseIfActive()
	select {
	case <-ran:
		t.Fatal("an already-acked pause was quiesced again")
	case <-time.After(300 * time.Millisecond):
	}

	// An expired record at startup is not acked. (Planted after a resume:
	// the FSM refuses to replace an in-force pause with one whose proposer
	// clock precedes its expiry, which is the point of the proposer clock.)
	if err := rn.SetCompactionPause(raft.SetCompactionPausePayload{Paused: false, Generation: 1, RequestedBy: "primary-X"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	stale := now.Add(-10 * time.Minute)
	if err := rn.SetCompactionPause(raft.SetCompactionPausePayload{
		Paused: true, Generation: 2, RequestedBy: "primary-Y", Reason: "restore b2",
		Now: stale, ExpiresAt: stale.Add(2 * time.Minute),
	}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	c.ackCompactionPauseIfActive()
	select {
	case <-ran:
		t.Fatal("an expired pause was quiesced at startup")
	case <-time.After(300 * time.Millisecond):
	}
	if s := rn.FSM().GetCompactionPause(); len(s.Acks) != 0 {
		t.Fatalf("an expired pause was acked: %v", s.Acks)
	}
}

// The two paths that can start a quiesce for the same generation (the FSM
// callback during log replay in Start, and ackCompactionPauseIfActive right
// after) must run the quiescer once and propose the ack once. The second
// caller arrives while the first is still inside the quiescer, before the
// FSM holds any ack, so only a single-flight gate can tell it to stand down.
// The ack count is read off the Raft log: exactly one entry must land.
func TestCompactionPause_QuiesceAndAckIsSingleFlightPerGeneration(t *testing.T) {
	rn := startSingleNodeRaft(t, "writer-A")
	c := newPauseForwardingCoordinator(t, "writer-A", RoleWriter, rn) // no FSM callback: only the test acks
	var quiesces atomic.Int32
	release := make(chan struct{})
	c.SetCompactionQuiescer(func(ctx context.Context) error {
		quiesces.Add(1)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	now := time.Now()
	if err := rn.SetCompactionPause(raft.SetCompactionPausePayload{
		Paused: true, Generation: 1, RequestedBy: "primary-X", Reason: "restore b1",
		Now: now, ExpiresAt: now.Add(2 * time.Minute),
	}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	before := lastRaftLogIndex(t, rn)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.quiesceAndAck(1)
		}()
	}
	// Both callers are in: the first inside the quiescer, the second either
	// gone (fixed) or inside the quiescer too (the bug).
	waitFor(t, 5*time.Second, func() bool { return quiesces.Load() >= 1 }, "no caller reached the quiescer")
	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := quiesces.Load(); got != 1 {
		t.Fatalf("the quiescer ran %d times for one generation, want 1", got)
	}
	if entries := lastRaftLogIndex(t, rn) - before; entries != 1 {
		t.Fatalf("%d Raft entries were appended for one ack, want 1", entries)
	}
	if s := rn.FSM().GetCompactionPause(); s.Acks["writer-A"] != 1 {
		t.Fatalf("acks = %v, want writer-A at generation 1", s.Acks)
	}
	// A later caller for an acked generation is a no-op (idempotence).
	c.quiesceAndAck(1)
	if quiesces.Load() != 1 {
		t.Fatal("an already-acked generation ran the quiescer again")
	}
}

func lastRaftLogIndex(t *testing.T, rn *raft.Node) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(rn.Stats()["last_log_index"], 10, 64)
	if err != nil {
		t.Fatalf("last_log_index: %v", err)
	}
	return v
}

// Every node in the FSM node table is waited for unless the registry marks it
// unhealthy or dead; a compactor-role node (no lease assigned) and the lease
// holder are waited for regardless; the local node is always in the set, and
// a node the registry does not know is waited for.
func TestCompactionPauseWaitSet(t *testing.T) {
	fsmNodes := []*raft.NodeInfo{
		{ID: "writer-A", Role: "writer"},
		{ID: "compactor-X", Role: "compactor"},
		{ID: "reader-dead", Role: "reader"},
		{ID: "writer-unhealthy", Role: "writer"},
		{ID: "reader-unknown", Role: "reader"},
		{ID: "writer-lease-dead", Role: "writer"},
		{ID: "compactor-dead", Role: "compactor"},
		{ID: "standalone-dead", Role: "standalone"},
		nil,
		{ID: "", Role: "reader"},
	}
	registry := map[string]NodeState{
		"writer-A":          StateHealthy,
		"compactor-X":       StateHealthy,
		"reader-dead":       StateDead,
		"writer-unhealthy":  StateUnhealthy,
		"writer-lease-dead": StateDead,
		"compactor-dead":    StateDead,
		"standalone-dead":   StateDead,
	}
	state := func(id string) (NodeState, bool) {
		s, ok := registry[id]
		return s, ok
	}

	got := compactionPauseWaitSet(fsmNodes, state, "writer-lease-dead", "primary-local")
	want := "compactor-X,primary-local,reader-unknown,writer-A,writer-lease-dead"
	if strings.Join(got, ",") != want {
		t.Fatalf("wait set with a lease = %v, want %s", got, want)
	}

	// No lease: every node whose role can compact is waited for, dead or
	// not. The standalone role has CanCompact too, and the compaction gate
	// lets such a node compact while no lease is assigned.
	got = compactionPauseWaitSet(fsmNodes, state, "", "primary-local")
	want = "compactor-X,compactor-dead,primary-local,reader-unknown,standalone-dead,writer-A"
	if strings.Join(got, ",") != want {
		t.Fatalf("wait set without a lease = %v, want %s", got, want)
	}

	// An empty FSM table (a fresh leader) still waits for the local node.
	got = compactionPauseWaitSet(nil, state, "", "primary-local")
	if strings.Join(got, ",") != "primary-local" {
		t.Fatalf("wait set with an empty table = %v, want the local node", got)
	}

	// The local node marked dead in its own registry is still waited for.
	registry["primary-local"] = StateDead
	got = compactionPauseWaitSet([]*raft.NodeInfo{{ID: "primary-local", Role: "writer"}}, state, "", "primary-local")
	if strings.Join(got, ",") != "primary-local" {
		t.Fatalf("local node dropped from its own wait set: %v", got)
	}
}

// A node in the FSM table that never acks makes the wait fail naming it, the
// pause is released on the way out, and a dead reader is not waited for.
func TestCompactionPause_WaitNamesTheNodesThatDidNotAck(t *testing.T) {
	rn := startSingleNodeRaft(t, "writer-A")
	c := newPauseTestCoordinator(t, "writer-A", RoleWriter, rn)

	// A dead reader in the table is skipped: the pause goes through.
	if err := rn.AddNode(&raft.NodeInfo{ID: "reader-Y", Role: string(RoleReader)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	deadReader := NewNode("reader-Y", "reader-Y", RoleReader, barrierTestCluster)
	if err := c.registry.Register(deadReader); err != nil {
		t.Fatal(err)
	}
	c.registry.UpdateNodeState("reader-Y", StateDead)
	h, err := c.PauseCompaction(context.Background(), "restore b1")
	if err != nil {
		t.Fatalf("PauseCompaction with a dead reader in the table: %v", err)
	}
	if err := h.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A compactor that never acks fails the wait and is named; the reader is
	// not. The caller's deadline stands in for the 10-minute ack timeout.
	if err := rn.AddNode(&raft.NodeInfo{ID: "compactor-X", Role: string(RoleCompactor)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	pauseErr := make(chan error, 1)
	go func() {
		_, err := c.PauseCompaction(ctx, "restore b2")
		pauseErr <- err
	}()
	// While the requester waits, the status names who it is waiting for.
	waitFor(t, 2*time.Second, func() bool {
		pending, _ := c.CompactionPauseStatus()["pending_acks"].([]string)
		return strings.Join(pending, ",") == "compactor-X"
	}, "status pending_acks never listed compactor-X alone", func() string {
		return fmt.Sprintf("status = %v", c.CompactionPauseStatus())
	})
	err = <-pauseErr
	if err == nil {
		t.Fatal("PauseCompaction returned without an ack from compactor-X")
	}
	if !strings.Contains(err.Error(), "compactor-X") || strings.Contains(err.Error(), "reader-Y") {
		t.Fatalf("err = %v, want it to name compactor-X and not reader-Y", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("wait did not honour the caller deadline: %s", time.Since(start))
	}
	waitFor(t, 10*time.Second, func() bool { return !rn.FSM().GetCompactionPause().Active },
		"the failed pause was not released on the way out")

	// Marked dead in the registry, a compactor-role node is still waited for
	// when no lease is assigned: it is a node that runs compaction.
	compactor := NewNode("compactor-X", "compactor-X", RoleCompactor, barrierTestCluster)
	if err := c.registry.Register(compactor); err != nil {
		t.Fatal(err)
	}
	c.registry.UpdateNodeState("compactor-X", StateDead)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if _, err := c.PauseCompaction(ctx2, "restore b3"); err == nil || !strings.Contains(err.Error(), "compactor-X") {
		t.Fatalf("a dead compactor was skipped: err = %v", err)
	}
}

// The forwarding path: a follower proposes through the leader. Setting the
// pause follows the manifest role rule (a reader is refused), the ack is open
// to every known peer, and both must name the node that signed the request.
func TestCompactionPause_ForwardedCommandsAreAllowlistedAndRoleGated(t *testing.T) {
	addrs := allocFreePorts(t, 2)
	raftA := startRaftNodeInDir(t, "writer-A", addrs[0], filepath.Join(t.TempDir(), "writer-A"), true)
	t.Cleanup(func() { _ = raftA.Stop() })
	if err := raftA.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	raftB := startRaftNodeInDir(t, "reader-B", addrs[1], filepath.Join(t.TempDir(), "reader-B"), false)
	t.Cleanup(func() { _ = raftB.Stop() })
	if err := raftA.AddVoter("reader-B", addrs[1], 10*time.Second); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	if err := raftB.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("reader-B WaitForLeader: %v", err)
	}

	peerB := NewNode("reader-B", "reader-B", RoleReader, barrierTestCluster)
	peerC := NewNode("writer-C", "writer-C", RoleWriter, barrierTestCluster)
	leader := startLeaderCoordinatorServer(t, raftA, peerB, peerC)
	if err := raftA.AddNode(&raft.NodeInfo{ID: "writer-A", Role: string(RoleWriter), Address: leader.addr()}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, ok := raftB.FSM().GetNode("writer-A")
		return ok
	}, "reader-B never applied the leader's AddNode")

	// Two followers sharing the follower Raft node: a reader and a writer.
	// Neither acks on its own here; every command is forwarded by hand.
	reader := newPauseForwardingCoordinator(t, "reader-B", RoleReader, raftB)
	writer := newPauseForwardingCoordinator(t, "writer-C", RoleWriter, raftB)
	ctx := context.Background()
	now := time.Now()
	setBy := func(by string, gen uint64) *raft.Command {
		cmd, err := raft.CompactionPauseCommand(raft.SetCompactionPausePayload{
			Paused: true, Generation: gen, RequestedBy: by, Reason: "restore b1", Now: now, ExpiresAt: now.Add(2 * time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
		return cmd
	}
	ackBy := func(by string, gen uint64) *raft.Command {
		cmd, err := raft.CompactionPauseAckCommand(raft.AckCompactionPausePayload{NodeID: by, Generation: gen})
		if err != nil {
			t.Fatal(err)
		}
		return cmd
	}

	if raftB.IsLeader() {
		t.Fatal("precondition: reader-B must be a follower")
	}
	// A reader may not set the pause.
	err := reader.applyOrForward(ctx, setBy("reader-B", 1))
	if err == nil || !strings.Contains(err.Error(), "unauthorized role") {
		t.Fatalf("reader Set: err = %v, want unauthorized role", err)
	}
	// A writer may, through the leader.
	if err := writer.applyOrForward(ctx, setBy("writer-C", 1)); err != nil {
		t.Fatalf("writer Set forwarded: %v", err)
	}
	if s := raftA.FSM().GetCompactionPause(); !s.Active || s.RequestedBy != "writer-C" {
		t.Fatalf("leader FSM after forwarded Set = %+v", s)
	}
	// A reader acks, and only for itself.
	if err := reader.applyOrForward(ctx, ackBy("reader-B", 1)); err != nil {
		t.Fatalf("reader Ack forwarded: %v", err)
	}
	err = reader.applyOrForward(ctx, ackBy("writer-C", 1))
	if err == nil || !strings.Contains(err.Error(), "must name the requesting node") {
		t.Fatalf("ack on behalf of another node: err = %v, want refused", err)
	}
	err = writer.applyOrForward(ctx, setBy("writer-A", 1))
	if err == nil || !strings.Contains(err.Error(), "must name the requesting node") {
		t.Fatalf("Set on behalf of another node: err = %v, want refused", err)
	}
	s := raftA.FSM().GetCompactionPause()
	if len(s.Acks) != 1 || s.Acks["reader-B"] != 1 {
		t.Fatalf("leader acks = %v, want reader-B only", s.Acks)
	}
	// The writer resumes its own pause through the leader.
	resume, err := raft.CompactionPauseCommand(raft.SetCompactionPausePayload{Paused: false, Generation: 1, RequestedBy: "writer-C"})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.applyOrForward(ctx, resume); err != nil {
		t.Fatalf("forwarded resume: %v", err)
	}
	if s := raftA.FSM().GetCompactionPause(); s.Active {
		t.Fatalf("leader FSM still paused after a forwarded resume: %+v", s)
	}
}

// The Pattern 1 shape: the requester is a Raft FOLLOWER. Its proposals are
// forwarded through the leader, its own FSM trails the leader by a few
// milliseconds when the proposal returns, the leader quiesces and acks on its
// own, and the follower acks through the leader. The pause must come back
// held, not refused as "no longer in force" because the follower read its
// own stale FSM first.
func TestCompactionPause_FollowerRequesterPausesThroughTheLeader(t *testing.T) {
	addrs := allocFreePorts(t, 2)
	raftA := startRaftNodeInDir(t, "writer-A", addrs[0], filepath.Join(t.TempDir(), "writer-A"), true)
	t.Cleanup(func() { _ = raftA.Stop() })
	if err := raftA.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	raftB := startRaftNodeInDir(t, "writer-B", addrs[1], filepath.Join(t.TempDir(), "writer-B"), false)
	t.Cleanup(func() { _ = raftB.Stop() })
	if err := raftA.AddVoter("writer-B", addrs[1], 10*time.Second); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	if err := raftB.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("writer-B WaitForLeader: %v", err)
	}

	peerB := NewNode("writer-B", "writer-B", RoleWriter, barrierTestCluster)
	leader := startLeaderCoordinatorServer(t, raftA, peerB)
	// The leader quiesces and acks like any node (nothing to quiesce here).
	raftA.FSM().SetCompactionPauseCallback(func(paused bool, gen uint64) { leader.coord.onCompactionPauseChanged(paused, gen) })
	if err := raftA.AddNode(&raft.NodeInfo{ID: "writer-A", Role: string(RoleWriter), Address: leader.addr()}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := raftA.AddNode(&raft.NodeInfo{ID: "writer-B", Role: string(RoleWriter)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, ok := raftB.FSM().GetNode("writer-B")
		return ok
	}, "writer-B never applied the node table")

	follower := newPauseTestCoordinator(t, "writer-B", RoleWriter, raftB)
	if raftB.IsLeader() {
		t.Fatal("precondition: writer-B must be a follower")
	}
	for i := 0; i < 3; i++ {
		h, err := follower.PauseCompaction(context.Background(), "restore b1")
		if err != nil {
			t.Fatalf("PauseCompaction from a follower (round %d): %v", i, err)
		}
		s := raftA.FSM().GetCompactionPause()
		if !s.Active || s.RequestedBy != "writer-B" || s.Acks["writer-A"] != s.Generation || s.Acks["writer-B"] != s.Generation {
			t.Fatalf("leader FSM = %+v, want both nodes acked for the follower's pause", s)
		}
		if !follower.CompactionPaused() {
			t.Fatal("the follower does not see its own pause")
		}
		if err := h.Resume(context.Background()); err != nil {
			t.Fatalf("Resume from a follower: %v", err)
		}
		waitFor(t, 10*time.Second, func() bool { return !follower.CompactionPaused() }, "the follower still sees the pause after Resume")
	}
}

// A refresh that commits only after the previous ExpiresAt has passed revived
// a pause that had lapsed on every node: their gates opened for the gap. The
// FSM then reads as healthy again, so the live check cannot see it; the
// refresher must mark the handle lost itself. The tick is driven directly
// (the loop's interval is 30 s) with the testing hook delaying the commit
// past a shortened expiry.
func TestCompactionPause_LateRefreshCommitMarksTheHandleLost(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 0)
	if refreshCommittedInTime(t0, t0) || refreshCommittedInTime(t0, t0.Add(time.Nanosecond)) || !refreshCommittedInTime(t0, t0.Add(-time.Nanosecond)) {
		t.Fatal("refreshCommittedInTime: a commit at or after the expiry must count as late")
	}

	rn := startSingleNodeRaft(t, "writer-A")
	c := newPauseTestCoordinator(t, "writer-A", RoleWriter, rn)
	h, err := c.PauseCompaction(context.Background(), "restore b1")
	if err != nil {
		t.Fatalf("PauseCompaction: %v", err)
	}
	h.mu.Lock()
	h.expiresAt = time.Now().Add(150 * time.Millisecond) // the previous refresh "committed" with this expiry
	h.afterPropose = func() { time.Sleep(400 * time.Millisecond) }
	h.mu.Unlock()

	if stop := h.refreshOnce(); !stop {
		t.Fatal("a refresh that committed after the pause lapsed did not stop the refresher")
	}
	lost, cause := h.lostFlag()
	if !lost || cause == nil || !strings.Contains(cause.Error(), "lapsed") {
		t.Fatalf("lost = %v cause = %v, want lost with the lapse named", lost, cause)
	}
	// The refresh did commit: the FSM reads healthy, which is exactly why the
	// live check alone is not enough.
	if s := rn.FSM().CompactionPauseBrief(); !s.Active || !s.ExpiresAt.After(time.Now().Add(compactionPauseTTL/2)) {
		t.Fatalf("FSM after the late refresh = %+v, want an active pause with the fresh expiry", s)
	}
	if lost, _ := h.Lost(); !lost {
		t.Fatal("Lost() does not report the late refresh")
	}
	if err := h.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A refresh that commits in time keeps the handle.
	h2, err := c.PauseCompaction(context.Background(), "restore b2")
	if err != nil {
		t.Fatal(err)
	}
	if stop := h2.refreshOnce(); stop {
		t.Fatal("a timely refresh stopped the refresher")
	}
	if lost, cause := h2.lostFlag(); lost {
		t.Fatalf("a timely refresh marked the handle lost: %v", cause)
	}
	if err := h2.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A handle is lost when the refresher meets a generation conflict (another
// requester took over an expired pause, or the pause was resumed), and when
// the FSM as read locally no longer shows this pause.
func TestCompactionPause_RefresherConflictMarksTheHandleLost(t *testing.T) {
	prev := compactionPauseRefreshInterval
	compactionPauseRefreshInterval = 50 * time.Millisecond
	t.Cleanup(func() { compactionPauseRefreshInterval = prev })

	rn := startSingleNodeRaft(t, "writer-A")
	c := newPauseTestCoordinator(t, "writer-A", RoleWriter, rn)
	h, err := c.PauseCompaction(context.Background(), "restore b1")
	if err != nil {
		t.Fatalf("PauseCompaction: %v", err)
	}
	// Held and refreshing: not lost.
	time.Sleep(200 * time.Millisecond)
	if lost, flag := h.lostFlag(); lost {
		t.Fatalf("a refreshing handle was marked lost: %v", flag)
	}

	// Another node takes the pause over as if it had expired (its clock is
	// ahead of the TTL; Apply trusts the payload). The next refresh is a
	// conflict.
	ahead := time.Now().Add(compactionPauseTTL + time.Minute)
	if err := rn.SetCompactionPause(raft.SetCompactionPausePayload{
		Paused: true, Generation: h.Generation() + 1, RequestedBy: "writer-B", Reason: "restore b2",
		Now: ahead, ExpiresAt: ahead.Add(2 * time.Minute),
	}, 5*time.Second); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { lost, _ := h.lostFlag(); return lost },
		"the refresher never marked the handle lost")
	if _, flag := h.lostFlag(); !raft.IsCompactionPauseConflict(flag) {
		t.Fatalf("lost reason = %v, want a generation conflict from the refresher", flag)
	}
	if lost, err := h.Lost(); !lost || err == nil {
		t.Fatalf("Lost() = %v, %v", lost, err)
	}
	// Resume of a lost handle is a no-op that does not touch the new pause.
	if err := h.Resume(context.Background()); err != nil {
		t.Fatalf("Resume of a lost handle: %v", err)
	}
	if s := rn.FSM().GetCompactionPause(); !s.Active || s.RequestedBy != "writer-B" {
		t.Fatalf("the lost handle's resume clobbered the new pause: %+v", s)
	}

	// The live check: a pause resumed under the handle is lost at once, with
	// no refresh needed.
	compactionPauseRefreshInterval = time.Hour
	if err := rn.SetCompactionPause(raft.SetCompactionPausePayload{Paused: false, Generation: h.Generation() + 1, RequestedBy: "writer-B"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	h2, err := c.PauseCompaction(context.Background(), "restore b3")
	if err != nil {
		t.Fatalf("PauseCompaction: %v", err)
	}
	if err := rn.SetCompactionPause(raft.SetCompactionPausePayload{Paused: false, Generation: h2.Generation(), RequestedBy: "operator"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	lost, cause := h2.Lost()
	if !lost || cause == nil || !strings.Contains(cause.Error(), "resumed") {
		t.Fatalf("Lost() after a foreign resume = %v, %v", lost, cause)
	}
	if err := h2.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
}
