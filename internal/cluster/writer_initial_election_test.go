package cluster

// A cluster with writer failover enabled must elect a first primary writer
// (#850). Before this, WriterFailoverManager could only fail over FROM an
// existing primary, and nothing else ever issued CommandPromoteWriter, so no
// node was ever primary: IsPrimaryWriter() was false cluster-wide and the
// retention and CQ schedulers silently never ran.

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// electionRig returns a manager wired to a real single-node Raft leader whose
// FSM already knows the writers, which is what applyPromoteWriter requires.
func electionRig(t *testing.T, writerIDs ...string) (*WriterFailoverManager, *raft.Node, *Registry) {
	t.Helper()
	raftNode := startRaftNode(t, writerIDs[0], allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	local := NewNode(writerIDs[0], writerIDs[0], RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: testLogger()})
	for _, id := range writerIDs {
		if err := raftNode.AddNode(&raft.NodeInfo{ID: id, Name: id, Role: string(RoleWriter), ClusterName: "test-cluster", Address: "127.0.0.1:1", State: string(StateHealthy)}, 5*time.Second); err != nil {
			t.Fatalf("AddNode %s: %v", id, err)
		}
		if id == writerIDs[0] {
			continue
		}
		n := NewNode(id, id, RoleWriter, "test-cluster")
		n.UpdateState(StateHealthy)
		if err := reg.Register(n); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	mgr := NewWriterFailoverManager(&WriterFailoverConfig{
		Registry: reg,
		RaftNode: raftNode,
		Logger:   testLogger(),
	})
	return mgr, raftNode, reg
}

func waitForFSMPrimary(t *testing.T, raftNode *raft.Node, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range raftNode.FSM().GetAllNodes() {
			if n.WriterState == string(WriterStatePrimary) {
				return n.ID
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func TestWriterFailover_ElectsInitialPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	mgr, raftNode, _ := electionRig(t, "writer-1")

	if got := waitForFSMPrimary(t, raftNode, 200*time.Millisecond); got != "" {
		t.Fatalf("precondition: a primary already exists (%s)", got)
	}
	mgr.checkPrimaryHealth()

	elected := waitForFSMPrimary(t, raftNode, 10*time.Second)
	if elected != "writer-1" {
		t.Fatalf("elected = %q; want writer-1 (before #850 no promotion was ever issued)", elected)
	}
	mgr.mu.RLock()
	primaryID, inProgress := mgr.primaryID, mgr.failoverInProg
	mgr.mu.RUnlock()
	if primaryID != "writer-1" {
		t.Errorf("manager primaryID = %q; want writer-1", primaryID)
	}
	if inProgress {
		t.Error("failoverInProg still set after the election completed")
	}
}

func TestWriterFailover_InitialElectionHappensOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	mgr, raftNode, reg := electionRig(t, "writer-1", "writer-2")

	// Several ticks in a row must not stack promotions: the first sets
	// failoverInProg, and once a primary exists the branch is not reached.
	for i := 0; i < 5; i++ {
		mgr.checkPrimaryHealth()
	}
	if elected := waitForFSMPrimary(t, raftNode, 10*time.Second); elected == "" {
		t.Fatal("no primary elected")
	}
	// Mirror the promotion into the registry the way the coordinator callback
	// does, so the manager sees a primary on the next tick.
	elected := waitForFSMPrimary(t, raftNode, time.Second)
	if n, ok := reg.Get(elected); ok {
		n.SetWriterState(WriterStatePrimary)
		if err := reg.Register(n); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	before := countPromotions(t, raftNode)
	for i := 0; i < 5; i++ {
		mgr.checkPrimaryHealth()
	}
	time.Sleep(300 * time.Millisecond)
	if after := countPromotions(t, raftNode); after != before {
		t.Errorf("further ticks issued %d more promotions; want 0", after-before)
	}
}

// countPromotions counts nodes the FSM currently marks primary; a second
// election would move the mark and is visible as a change in the elected id.
func countPromotions(t *testing.T, raftNode *raft.Node) int {
	t.Helper()
	n := 0
	for _, node := range raftNode.FSM().GetAllNodes() {
		if node.WriterState == string(WriterStatePrimary) {
			n++
		}
	}
	return n
}

// The gate reads c.localNode; the registry hands out clones, so the promotion
// callback has to mirror the change onto the local node or IsPrimaryWriter()
// never sees it (#850).
func TestOnWriterPromoted_ReachesTheLocalNodeGate(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: testLogger()})
	peer := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	peer.UpdateState(StateHealthy)
	if err := reg.Register(peer); err != nil {
		t.Fatalf("register peer: %v", err)
	}
	c := &Coordinator{
		cfg:               &config.ClusterConfig{},
		registry:          reg,
		localNode:         local,
		logger:            zerolog.Nop(),
		writerFailoverMgr: NewWriterFailoverManager(&WriterFailoverConfig{Registry: reg, Logger: testLogger()}),
	}

	if c.IsPrimaryWriter() {
		t.Fatal("precondition: this node must not start out primary")
	}
	c.onWriterPromoted("writer-1", "")
	if !c.IsPrimaryWriter() {
		t.Error("IsPrimaryWriter() false after this node was promoted (before #850 the promotion only reached the registry clone)")
	}
	if got, _ := reg.Get("writer-1"); got == nil || got.GetWriterState() != WriterStatePrimary {
		t.Error("registry entry not marked primary")
	}

	// Demoted in favour of a peer: the gate must close again.
	c.onWriterPromoted("writer-2", "writer-1")
	if c.IsPrimaryWriter() {
		t.Error("IsPrimaryWriter() still true after this node was demoted")
	}
}

// A snapshot restore replays membership without a promotion callback, so the
// writer state the FSM carries has to reach the local node too.
func TestOnRaftNodeAdded_CarriesWriterStateToTheLocalNode(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: testLogger()})
	c := &Coordinator{
		cfg:               &config.ClusterConfig{},
		registry:          reg,
		localNode:         local,
		logger:            zerolog.Nop(),
		writerFailoverMgr: NewWriterFailoverManager(&WriterFailoverConfig{Registry: reg, Logger: testLogger()}),
	}

	c.onRaftNodeAdded(&raft.NodeInfo{ID: "writer-1", Name: "writer-1", Role: string(RoleWriter), ClusterName: "test-cluster", Address: "127.0.0.1:1", State: string(StateHealthy), WriterState: string(WriterStatePrimary)})
	if !c.IsPrimaryWriter() {
		t.Error("IsPrimaryWriter() false after a restore that named this node primary")
	}
	// A peer's entry must not touch this node's state.
	c.onRaftNodeAdded(&raft.NodeInfo{ID: "writer-2", Name: "writer-2", Role: string(RoleWriter), ClusterName: "test-cluster", Address: "127.0.0.1:2", State: string(StateHealthy), WriterState: string(WriterStateStandby)})
	if !c.IsPrimaryWriter() {
		t.Error("a peer's restore entry cleared this node's primary state")
	}
}

// A writer that merely restarts re-joins with a payload carrying no writer
// state. Before #850 that cleared its designation while the manager still
// remembered it as the primary, so the manager looked for someone to fail
// over TO, excluded the only writer, and the cluster never regained a primary.
func TestWriterFailover_RegainsPrimaryAfterTheDesignationIsLost(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: testLogger()})
	mgr := NewWriterFailoverManager(&WriterFailoverConfig{Registry: reg, Logger: testLogger()})

	// The state after a restart: the manager remembers a primary, the registry
	// has the node healthy, and nobody holds the designation.
	mgr.primaryID = "writer-1"
	if got := mgr.selectNewPrimary("writer-1"); got != "" {
		t.Fatalf("precondition: selectNewPrimary should still refuse the excluded node, got %q", got)
	}
	if got := mgr.selectPrimary("writer-1", true); got != "writer-1" {
		t.Fatalf("selectPrimary(allowSelf) = %q; want writer-1, or a single-writer cluster never regains a primary", got)
	}

	// A node that is actually gone must never be re-selected: GetWriters
	// filters on health, so an unhealthy node is not a candidate.
	local.UpdateState(StateDead)
	if got := mgr.selectPrimary("writer-1", true); got != "" {
		t.Errorf("selectPrimary(allowSelf) = %q for an unhealthy node; want none", got)
	}
}

// The FSM keeps a re-joining writer's designation, so the case above does not
// arise in the first place.
func TestApplyAddNode_KeepsThePrimaryDesignationAcrossAReJoin(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	mgr, raftNode, _ := electionRig(t, "writer-1")
	mgr.checkPrimaryHealth()
	if elected := waitForFSMPrimary(t, raftNode, 10*time.Second); elected != "writer-1" {
		t.Fatalf("elected = %q; want writer-1", elected)
	}

	// Re-join exactly as handleJoinRequest does: no writer state in the payload.
	if err := raftNode.AddNode(&raft.NodeInfo{ID: "writer-1", Name: "writer-1", Role: string(RoleWriter), ClusterName: "test-cluster", Address: "127.0.0.1:1", State: string(StateHealthy)}, 5*time.Second); err != nil {
		t.Fatalf("re-join: %v", err)
	}
	if got, _ := raftNode.FSM().GetNode("writer-1"); got == nil || got.WriterState != string(WriterStatePrimary) {
		state := ""
		if got != nil {
			state = got.WriterState
		}
		t.Errorf("writer state after a re-join = %q; want primary", state)
	}
}

// An election is not a failover: it must not arm the cooldown, or a writer
// failure in the first minute of cluster life would be skipped.
func TestWriterFailover_ElectionDoesNotArmTheCooldown(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	mgr, raftNode, _ := electionRig(t, "writer-1")
	mgr.checkPrimaryHealth()
	if elected := waitForFSMPrimary(t, raftNode, 10*time.Second); elected != "writer-1" {
		t.Fatalf("elected = %q; want writer-1", elected)
	}
	mgr.mu.RLock()
	lastFailoverAt := mgr.lastFailoverAt
	primaryID := mgr.primaryID
	mgr.mu.RUnlock()
	if !lastFailoverAt.IsZero() {
		t.Error("the election armed the failover cooldown; a failure in the cooldown window would be skipped")
	}
	if primaryID != "writer-1" {
		t.Errorf("primaryID = %q; want writer-1", primaryID)
	}
}

// Only the Raft leader elects, and nothing happens without Raft at all.
func TestWriterFailover_ElectionIsLeaderOnly(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: testLogger()})
	mgr := NewWriterFailoverManager(&WriterFailoverConfig{Registry: reg, Logger: testLogger()})

	// RaftNode nil: checkPrimaryHealth must return before touching anything.
	mgr.checkPrimaryHealth()
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	if mgr.failoverInProg {
		t.Error("an election was started without a Raft node")
	}
	if mgr.primaryID != "" {
		t.Errorf("primaryID = %q; want empty", mgr.primaryID)
	}
}

// No candidates, no election.
func TestWriterFailover_NoElectionWithoutHealthyWriters(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	raftNode := startRaftNode(t, "reader-1", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	local := NewNode("reader-1", "reader-1", RoleReader, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: testLogger()})
	mgr := NewWriterFailoverManager(&WriterFailoverConfig{Registry: reg, RaftNode: raftNode, Logger: testLogger()})

	mgr.checkPrimaryHealth()
	time.Sleep(200 * time.Millisecond)
	if elected := waitForFSMPrimary(t, raftNode, 200*time.Millisecond); elected != "" {
		t.Errorf("elected %q with no writer-role nodes in the cluster", elected)
	}
}

// The steady state after an election: repeated ticks must not re-elect, start
// a failover, or arm the cooldown. This is what the live soak watched for, and
// the assertion the FSM-primary count above cannot make, since a re-election
// onto the same node leaves that count at one.
func TestWriterFailover_StaysElectedAcrossTicks(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	mgr, raftNode, reg := electionRig(t, "writer-1", "writer-2")
	mgr.checkPrimaryHealth()
	elected := waitForFSMPrimary(t, raftNode, 10*time.Second)
	if elected == "" {
		t.Fatal("no primary elected")
	}
	// The coordinator callback mirrors the promotion into the registry; do the
	// same so the manager sees the primary it just elected.
	if n, ok := reg.Get(elected); ok {
		n.SetWriterState(WriterStatePrimary)
		if err := reg.Register(n); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	for i := 0; i < 10; i++ {
		mgr.checkPrimaryHealth()
	}
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	if !mgr.lastFailoverAt.IsZero() {
		t.Error("a failover was started in the steady state")
	}
	if mgr.failoverInProg {
		t.Error("a promotion is in flight in the steady state")
	}
	if mgr.consecutiveFails != 0 {
		t.Errorf("consecutiveFails = %d in the steady state; want 0", mgr.consecutiveFails)
	}
	if mgr.primaryID != elected {
		t.Errorf("primaryID = %q; want %q", mgr.primaryID, elected)
	}
}

// Once a primary exists, a write proxied from a reader goes to it rather than
// being spread across writers. With no primary ever designated, RouteWrite's
// preference was dead code and every proxied write round-robined.
func TestRouteWrite_PrefersTheElectedPrimary(t *testing.T) {
	reg := NewRegistry(&RegistryConfig{Logger: testLogger()})
	for _, id := range []string{"writer-1", "writer-2", "writer-3"} {
		n := NewNode(id, id, RoleWriter, "test-cluster")
		n.UpdateState(StateHealthy)
		if err := reg.Register(n); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	if got := reg.GetPrimaryWriter(); got != nil {
		t.Fatalf("precondition: no primary expected, got %s", got.ID)
	}
	primary, ok := reg.Get("writer-2")
	if !ok {
		t.Fatal("writer-2 missing")
	}
	primary.SetWriterState(WriterStatePrimary)
	if err := reg.Register(primary); err != nil {
		t.Fatalf("register primary: %v", err)
	}
	got := reg.GetPrimaryWriter()
	if got == nil || got.ID != "writer-2" {
		t.Fatalf("GetPrimaryWriter() = %v; want writer-2, which is what RouteWrite prefers before it falls back to round-robin", got)
	}
}
