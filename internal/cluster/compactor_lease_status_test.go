package cluster

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
)

// #876 asked, as its minimum bar, that the lease holder and whether it is a
// dedicated compactor be visible in the status endpoint — "so the state is
// visible rather than inferred from a log line at assignment time". Before
// this, an operator watching a healthy cluster with an idle compactor pod had
// nothing in the API to look at.

func leaseStatusRig(t *testing.T) (*Coordinator, *Registry) {
	t.Helper()
	local := NewNode("node-1", "node-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 16, Logger: testLogger()})
	return &Coordinator{registry: reg, localNode: local, logger: testLogger()}, reg
}

func registerNode(t *testing.T, reg *Registry, id string, role NodeRole, state NodeState) {
	t.Helper()
	n := NewNode(id, id, role, "test-cluster")
	n.UpdateState(state)
	if err := reg.Register(n); err != nil {
		t.Fatalf("Register %s: %v", id, err)
	}
}

func TestCompactorLeaseStatusUnassigned(t *testing.T) {
	c, _ := leaseStatusRig(t)
	got := c.compactorLeaseStatus("")
	if assigned, _ := got["assigned"].(bool); assigned {
		t.Errorf("assigned=true with no lease: %v", got)
	}
	if _, ok := got["node_id"]; ok {
		t.Errorf("node_id present with no lease: %v", got)
	}
}

// The state the issue describes: a writer holds it while a compactor exists.
// is_dedicated=false is the field that makes that legible.
func TestCompactorLeaseStatusOnAWriter(t *testing.T) {
	c, reg := leaseStatusRig(t)
	registerNode(t, reg, "writer-1", RoleWriter, StateHealthy)

	got := c.compactorLeaseStatus("writer-1")
	if assigned, _ := got["assigned"].(bool); !assigned {
		t.Fatalf("assigned=false with a lease: %v", got)
	}
	if got["node_id"] != "writer-1" {
		t.Errorf("node_id=%v, want writer-1", got["node_id"])
	}
	if present, _ := got["present"].(bool); !present {
		t.Errorf("present=false for a node in the registry: %v", got)
	}
	if dedicated, _ := got["is_dedicated"].(bool); dedicated {
		t.Errorf("is_dedicated=true for a writer: %v", got)
	}
	if got["role"] != string(RoleWriter) {
		t.Errorf("role=%v, want writer", got["role"])
	}
}

func TestCompactorLeaseStatusOnADedicatedCompactor(t *testing.T) {
	c, reg := leaseStatusRig(t)
	registerNode(t, reg, "compactor-1", RoleCompactor, StateHealthy)

	got := c.compactorLeaseStatus("compactor-1")
	if dedicated, _ := got["is_dedicated"].(bool); !dedicated {
		t.Errorf("is_dedicated=false for a compactor node: %v", got)
	}
}

// A holder that is gone is exactly the state an operator is debugging, so it
// must be reported rather than omitted or guessed at.
func TestCompactorLeaseStatusWhenTheHolderIsGone(t *testing.T) {
	c, _ := leaseStatusRig(t)

	got := c.compactorLeaseStatus("ghost-1")
	if assigned, _ := got["assigned"].(bool); !assigned {
		t.Errorf("assigned=false for a lease naming a missing node: %v", got)
	}
	if got["node_id"] != "ghost-1" {
		t.Errorf("node_id=%v, want ghost-1", got["node_id"])
	}
	if present, ok := got["present"].(bool); !ok || present {
		t.Errorf("present must be false for a holder not in the registry: %v", got)
	}
	if _, ok := got["is_dedicated"]; ok {
		t.Errorf("is_dedicated must be absent rather than guessed for a missing holder: %v", got)
	}
}

// The manager-nil rule is "do not FLIP the gate", not "a manager is
// required". compactionClusterGate switches from the static role check to the
// FSM lease the moment the lease is non-empty, so:
//
//   - no lease  -> refuse, or every node moves onto a lease nothing maintains
//   - a lease   -> allow, because the cluster is ALREADY in that state and
//     this is the only lever that recovers it
//
// The second case is the compactor-side twin of #872's manual hand-over: a
// cluster whose licence lapsed with the lease pinned to a dead node has
// compaction stopped everywhere and nothing to unstick it.
func TestAssignCompactorRefusedWhenItWouldFlipTheGate(t *testing.T) {
	rNode := startRaftNode(t, "node-1", allocFreePort(t), true)
	t.Cleanup(func() { _ = rNode.Stop() })
	if err := rNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	local := NewNode("node-1", "node-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 16, Logger: testLogger()})
	registerNode(t, reg, "compactor-1", RoleCompactor, StateHealthy)

	c := &Coordinator{
		cfg:       &config.ClusterConfig{FailoverTimeoutSeconds: 5},
		registry:  reg,
		localNode: local,
		raftNode:  rNode,
		raftFSM:   rNode.FSM(),
		logger:    testLogger(),
		// compactorFailoverMgr deliberately nil: unlicensed, or
		// failover_enabled=false.
	}

	// No lease yet: refuse.
	if _, err := c.AssignCompactorViaRaft("compactor-1"); !errorIs(err, ErrCompactorLeaseNotManaged) {
		t.Fatalf("AssignCompactorViaRaft with no manager and no lease = %v, want ErrCompactorLeaseNotManaged", err)
	}
	if got := rNode.FSM().GetActiveCompactorID(); got != "" {
		t.Fatalf("a refused assignment set the lease to %q", got)
	}

	// Already in lease mode: allow, because nothing else can move it.
	registerNode(t, reg, "writer-9", RoleWriter, StateHealthy)
	if err := rNode.AssignCompactor("writer-9", "", 5*time.Second); err != nil {
		t.Fatalf("AssignCompactor: %v", err)
	}
	old, err := c.AssignCompactorViaRaft("compactor-1")
	if err != nil {
		t.Fatalf("AssignCompactorViaRaft on a lease-mode cluster with no manager: %v — this is the only lever that recovers such a cluster", err)
	}
	if old != "writer-9" {
		t.Errorf("old holder reported as %q, want writer-9", old)
	}
	if got := rNode.FSM().GetActiveCompactorID(); got != "compactor-1" {
		t.Errorf("lease is %q, want compactor-1", got)
	}
}

// The unmanaged path must apply the same target rules as the managed one, or
// the two drift and a reader can hold the lease on one path only.
func TestUnmanagedAssignAppliesTheSameTargetRules(t *testing.T) {
	rNode := startRaftNode(t, "node-1", allocFreePort(t), true)
	t.Cleanup(func() { _ = rNode.Stop() })
	if err := rNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	local := NewNode("node-1", "node-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 16, Logger: testLogger()})
	registerNode(t, reg, "reader-1", RoleReader, StateHealthy)
	registerNode(t, reg, "writer-9", RoleWriter, StateHealthy)
	if err := rNode.AssignCompactor("writer-9", "", 5*time.Second); err != nil {
		t.Fatalf("AssignCompactor: %v", err)
	}

	c := &Coordinator{
		cfg:       &config.ClusterConfig{FailoverTimeoutSeconds: 5},
		registry:  reg,
		localNode: local,
		raftNode:  rNode,
		raftFSM:   rNode.FSM(),
		logger:    testLogger(),
	}

	if _, err := c.AssignCompactorViaRaft("reader-1"); !errorIs(err, ErrCannotHoldCompactorLease) {
		t.Errorf("unmanaged assign to a reader = %v, want ErrCannotHoldCompactorLease", err)
	}
	if _, err := c.AssignCompactorViaRaft("ghost-1"); !errorIs(err, ErrNodeNotFound) {
		t.Errorf("unmanaged assign to an unknown node = %v, want ErrNodeNotFound", err)
	}
	if _, err := c.AssignCompactorViaRaft("writer-9"); !errorIs(err, ErrAlreadyCompactorLeaseHolder) {
		t.Errorf("unmanaged assign to the current holder = %v, want ErrAlreadyCompactorLeaseHolder", err)
	}
	if got := rNode.FSM().GetActiveCompactorID(); got != "writer-9" {
		t.Errorf("a refused assignment moved the lease to %q", got)
	}
}
