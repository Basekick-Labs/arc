package cluster

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #876: the compactor lease was never preempted. A writer that took it
// because it was the only candidate at assignment time kept it for the life
// of the cluster, so a dedicated compactor that joined even one tick late sat
// idle — with its own CPU budget and its own PVC — while a writer compacted
// on top of ingest. Reproduced on a live 4-node rig before any of this was
// written: one lease assignment ever, to a writer, with a healthy compactor
// node present and idle for three minutes.

// preemptRig builds a leader Coordinator-less manager on a real single-node
// Raft, so the tests drive the same path production does: checkCompactorHealth
// gates on IsLeader(), and the assignment goes through a real Raft Apply.
//
// The thresholds are small and the cooldown short because these tests drive
// ticks by hand; the production defaults (6 and 10x the failover cooldown) are
// asserted separately in TestPreemptDefaults.
func preemptRig(t *testing.T) (*CompactorFailoverManager, *raft.Node, *Registry) {
	t.Helper()
	raftNode := startRaftNode(t, "node-1", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	local := NewNode("node-1", "node-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 16, Logger: testLogger()})

	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry:           reg,
		RaftNode:           raftNode,
		RaftFSM:            raftNode.FSM(),
		FailoverTimeout:    5 * time.Second,
		CooldownPeriod:     time.Millisecond,
		UnhealthyThreshold: 3,
		PreemptThreshold:   3,
		PreemptCooldown:    time.Millisecond,
		Logger:             testLogger(),
	})
	return mgr, raftNode, reg
}

// addNode registers a healthy node of the given role, in the registry and in
// the Raft FSM (AssignCompactor does not require the latter, but the rest of
// the cluster reads it).
func addNode(t *testing.T, reg *Registry, raftNode *raft.Node, id string, role NodeRole) *Node {
	t.Helper()
	n := NewNode(id, id, role, "test-cluster")
	n.UpdateState(StateHealthy)
	if err := reg.Register(n); err != nil {
		t.Fatalf("Register %s: %v", id, err)
	}
	if err := raftNode.AddNode(&raft.NodeInfo{
		ID: id, Name: id, Role: string(role), ClusterName: "test-cluster",
		Address: "127.0.0.1:1", State: string(StateHealthy),
	}, 5*time.Second); err != nil {
		t.Fatalf("AddNode %s: %v", id, err)
	}
	return n
}

func giveLeaseTo(t *testing.T, raftNode *raft.Node, id string) {
	t.Helper()
	if err := raftNode.AssignCompactor(id, "", 5*time.Second); err != nil {
		t.Fatalf("AssignCompactor %s: %v", id, err)
	}
}

func currentLease(raftNode *raft.Node) string {
	return raftNode.FSM().GetActiveCompactorID()
}

// waitForLease polls until the lease reaches want, because the assignment is
// applied on its own goroutine — reading once immediately after driving the
// ticks samples the state before the apply lands, and a test written that way
// passes whether or not the code works.
func waitForLease(t *testing.T, raftNode *raft.Node, want string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if currentLease(raftNode) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the compactor lease is %q, want %q", currentLease(raftNode), want)
}

// leaseStaysAt fails if the lease ever moves away from want during d.
func leaseStaysAt(t *testing.T, raftNode *raft.Node, want string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if got := currentLease(raftNode); got != want {
			t.Fatalf("the compactor lease moved from %q to %q", want, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// tick drives n health checks, waiting between them for any asynchronous
// assignment to finish — otherwise failoverInProg makes the next check a
// no-op and the test counts ticks that never ran.
func tick(t *testing.T, mgr *CompactorFailoverManager, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		mgr.checkCompactorHealth()
		waitForApply(mgr)
	}
}

func waitForApply(mgr *CompactorFailoverManager) {
	for i := 0; i < 500; i++ {
		mgr.mu.RLock()
		inProg := mgr.failoverInProg
		mgr.mu.RUnlock()
		if !inProg {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The headline case, and the one reproduced live: a writer holds the lease, a
// dedicated compactor is healthy, and the lease moves to it.
func TestPreemptsAWriterWhenADedicatedCompactorIsAvailable(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")

	// Not before the threshold: the sustain counter is what stops a compactor
	// that appears for one tick from taking the lease.
	tick(t, mgr, 2)
	if got := currentLease(raftNode); got != "writer-1" {
		t.Fatalf("the lease moved to %q after 2 of 3 sustained checks", got)
	}

	tick(t, mgr, 1)
	waitForLease(t, raftNode, "compactor-1", 5*time.Second)
}

// And once it is there, it stays: this is what stops two compactors passing
// the lease back and forth.
func TestDoesNotPreemptADedicatedCompactor(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	addNode(t, reg, raftNode, "compactor-2", RoleCompactor)
	giveLeaseTo(t, raftNode, "compactor-2")

	tick(t, mgr, 10)
	leaseStaysAt(t, raftNode, "compactor-2", 300*time.Millisecond)
}

// The common case, on every cluster that never deployed a compactor: the
// branch runs on every tick and must be a no-op. A bug here would move the
// lease between writers forever.
func TestDoesNotPreemptWhenNoDedicatedCompactorExists(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "writer-2", RoleWriter)
	giveLeaseTo(t, raftNode, "writer-1")

	tick(t, mgr, 10)
	leaseStaysAt(t, raftNode, "writer-1", 300*time.Millisecond)
}

func TestDoesNotPreemptTowardAnUnhealthyCompactor(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	c := addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	c.UpdateState(StateUnhealthy)
	if err := reg.Register(c); err != nil {
		t.Fatalf("Register: %v", err)
	}
	giveLeaseTo(t, raftNode, "writer-1")

	tick(t, mgr, 10)
	leaseStaysAt(t, raftNode, "writer-1", 300*time.Millisecond)
}

// A compactor that flaps must never accumulate the sustained window. This is
// the guard the issue asks for by name: "so a flapping compactor cannot
// ping-pong the lease".
func TestAFlappingCompactorNeverAttractsTheLease(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	c := addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")

	for i := 0; i < 12; i++ {
		state := StateHealthy
		if i%2 == 0 {
			state = StateUnhealthy
		}
		c.UpdateState(state)
		if err := reg.Register(c); err != nil {
			t.Fatalf("Register: %v", err)
		}
		mgr.checkCompactorHealth()
		waitForApply(mgr)
	}
	leaseStaysAt(t, raftNode, "writer-1", 300*time.Millisecond)
}

// A flapping HOLDER must not have the preempt branch top up the health
// counter for it. UnhealthyThreshold documents itself as "consecutive
// unhealthy checks", and sharing one counter between the two branches would
// make unhealthy/healthy/unhealthy fire a health failover on the third tick.
//
// This is a guard against the alternative implementation rather than a
// regression test for the shipped bug — it passes on the old code too, which
// had no preempt branch at all to corrupt the counter.
func TestAFlappingHolderDoesNotAccumulateAcrossBranches(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	w := addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")

	for _, healthy := range []bool{false, true, false} {
		state := StateHealthy
		if !healthy {
			state = StateUnhealthy
		}
		w.UpdateState(state)
		if err := reg.Register(w); err != nil {
			t.Fatalf("Register: %v", err)
		}
		mgr.checkCompactorHealth()
		waitForApply(mgr)
	}

	mgr.mu.RLock()
	fails := mgr.consecutiveFails
	mgr.mu.RUnlock()
	if fails >= mgr.cfg.UnhealthyThreshold {
		t.Errorf("consecutiveFails is %d after unhealthy/healthy/unhealthy; the field means CONSECUTIVE unhealthy checks", fails)
	}
}

// Deterministic choice between two candidates. The registry returns nodes in
// Go map order, which is randomised per iteration.
func TestTwoDedicatedCompactorsPickTheLowestID(t *testing.T) {
	// The determinism lives in the selector, so hammer that rather than
	// standing up twenty Raft nodes: map iteration order is randomised per
	// range, so an unsorted selector fails this within a handful of rounds.
	reg := NewRegistry(&RegistryConfig{MaxNodes: 16, Logger: testLogger()})
	for _, id := range []string{"compactor-c", "compactor-a", "compactor-b"} {
		n := NewNode(id, id, RoleCompactor, "test-cluster")
		n.UpdateState(StateHealthy)
		if err := reg.Register(n); err != nil {
			t.Fatalf("Register %s: %v", id, err)
		}
	}
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: reg, RaftFSM: stubFailoverFSM(), Logger: testLogger(),
	})
	for i := 0; i < 200; i++ {
		if got := mgr.selectDedicatedCompactor(""); got != "compactor-a" {
			t.Fatalf("round %d: selectDedicatedCompactor = %q, want compactor-a", i, got)
		}
		if got := mgr.selectNewCompactor("compactor-a"); got != "compactor-b" {
			t.Fatalf("round %d: selectNewCompactor excluding a = %q, want compactor-b", i, got)
		}
	}

	// And end to end once, so the sorted selector is actually the one the
	// preemption path calls.
	mgr2, raftNode, reg2 := preemptRig(t)
	addNode(t, reg2, raftNode, "writer-1", RoleWriter)
	addNode(t, reg2, raftNode, "compactor-b", RoleCompactor)
	addNode(t, reg2, raftNode, "compactor-a", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")
	tick(t, mgr2, 3)
	waitForLease(t, raftNode, "compactor-a", 5*time.Second)
}

// Preemption must not arm the HEALTH failover cooldown. triggerFailoverLocked
// reads that field, so stamping it here would delay recovery from a compactor
// that dies right after the lease moved to it — a hand-over of a healthy lease
// must never slow down a real failure.
func TestPreemptionDoesNotArmTheHealthFailoverCooldown(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")

	tick(t, mgr, 3)
	waitForLease(t, raftNode, "compactor-1", 5*time.Second)

	mgr.mu.RLock()
	stamped := mgr.lastFailoverAt
	mgr.mu.RUnlock()
	if !stamped.IsZero() {
		t.Error("preemption armed the health-failover cooldown; a compactor dying now would wait out a window meant for flapping failovers")
	}
}

// The same defect, pre-existing and never noticed: every cluster start armed
// the health cooldown, because tryInitialAssignment completed through
// completeFailover. The writer manager split this out as completeElection for
// exactly this reason. Fails on the old code.
func TestInitialAssignmentDoesNotArmTheHealthFailoverCooldown(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)

	tick(t, mgr, 1)
	waitForLease(t, raftNode, "compactor-1", 5*time.Second)

	mgr.mu.RLock()
	stamped := mgr.lastFailoverAt
	mgr.mu.RUnlock()
	if !stamped.IsZero() {
		t.Error("the initial compactor assignment armed the health-failover cooldown; nothing failed and nothing was lost, so there is nothing to back off from")
	}
}

// The cooldown bounds how often a healthy lease can move.
func TestPreemptCooldownBlocksASecondPreemption(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	mgr.cfg.PreemptCooldown = time.Hour
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")

	tick(t, mgr, 3)
	waitForLease(t, raftNode, "compactor-1", 5*time.Second)

	// Put it back on the writer behind the manager's back, so the preempt
	// condition is true again, and confirm the cooldown holds it.
	giveLeaseTo(t, raftNode, "writer-1")
	tick(t, mgr, 10)
	leaseStaysAt(t, raftNode, "writer-1", 300*time.Millisecond)
}

// An operator assignment must survive preemption for the cooldown, or the
// endpoint advertises an action the same binary reverses a minute later.
func TestAnOperatorAssignmentSuppressesPreemption(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	mgr.cfg.PreemptCooldown = time.Hour
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "compactor-1")

	old, err := mgr.AssignTo("writer-1")
	if err != nil {
		t.Fatalf("AssignTo: %v", err)
	}
	if old != "compactor-1" {
		t.Errorf("AssignTo reported the old holder as %q, want compactor-1", old)
	}

	tick(t, mgr, 10)
	leaseStaysAt(t, raftNode, "writer-1", 300*time.Millisecond)
}

// A writer is a legal target. RoleCapabilities.CanCompact is FALSE for
// RoleWriter ("compaction runs on dedicated nodes"), which describes where
// compaction is meant to run and not who may hold the lease — failover has
// always fallen back to a writer, and must, or a cluster whose only compactor
// dies stops compacting entirely. Using CanCompact as the predicate here
// would refuse the lease to the node class the automatic path itself picks.
func TestAssignToAcceptsAWriter(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "compactor-1")

	if _, err := mgr.AssignTo("writer-1"); err != nil {
		t.Fatalf("AssignTo(writer): %v — a writer must be able to hold the lease, because failover hands it to one", err)
	}
	waitForLease(t, raftNode, "writer-1", 5*time.Second)
}

func TestAssignToRefusals(t *testing.T) {
	cases := []struct {
		name   string
		target string
		want   error
		setup  func(t *testing.T, mgr *CompactorFailoverManager, raftNode *raft.Node, reg *Registry)
	}{
		{
			name:   "a reader cannot hold the lease",
			target: "reader-1",
			want:   ErrCannotHoldCompactorLease,
			setup: func(t *testing.T, mgr *CompactorFailoverManager, raftNode *raft.Node, reg *Registry) {
				addNode(t, reg, raftNode, "reader-1", RoleReader)
			},
		},
		{
			name:   "an unknown node",
			target: "ghost-1",
			want:   ErrNodeNotFound,
			setup:  func(t *testing.T, mgr *CompactorFailoverManager, raftNode *raft.Node, reg *Registry) {},
		},
		{
			name:   "an unhealthy node",
			target: "compactor-1",
			want:   ErrNodeNotHealthy,
			setup: func(t *testing.T, mgr *CompactorFailoverManager, raftNode *raft.Node, reg *Registry) {
				c := addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
				c.UpdateState(StateUnhealthy)
				if err := reg.Register(c); err != nil {
					t.Fatalf("Register: %v", err)
				}
			},
		},
		{
			name:   "the node that already holds it",
			target: "compactor-1",
			want:   ErrAlreadyCompactorLeaseHolder,
			setup: func(t *testing.T, mgr *CompactorFailoverManager, raftNode *raft.Node, reg *Registry) {
				addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
				giveLeaseTo(t, raftNode, "compactor-1")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mgr, raftNode, reg := preemptRig(t)
			addNode(t, reg, raftNode, "writer-1", RoleWriter)
			tc.setup(t, mgr, raftNode, reg)
			before := currentLease(raftNode)

			_, err := mgr.AssignTo(tc.target)
			if err == nil {
				t.Fatalf("AssignTo(%q) succeeded, want %v", tc.target, tc.want)
			}
			if !errorIs(err, tc.want) {
				t.Errorf("AssignTo(%q) = %v, want %v", tc.target, err, tc.want)
			}
			if got := currentLease(raftNode); got != before {
				t.Errorf("a refused assignment moved the lease from %q to %q", before, got)
			}
		})
	}
}

// A manual assignment must not race an automatic one that has already chosen
// a different target.
func TestAssignToRefusesWhileAFailoverIsInFlight(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "compactor-1")

	mgr.mu.Lock()
	mgr.failoverInProg = true
	mgr.mu.Unlock()

	_, err := mgr.AssignTo("writer-1")
	if !errorIs(err, ErrCompactorFailoverInProgress) {
		t.Errorf("AssignTo during an in-flight failover = %v, want ErrCompactorFailoverInProgress", err)
	}

	mgr.mu.Lock()
	mgr.failoverInProg = false
	mgr.mu.Unlock()
}

// The production defaults, which the rig deliberately overrides.
func TestPreemptDefaults(t *testing.T) {
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry: NewRegistry(&RegistryConfig{Logger: testLogger()}),
		RaftFSM:  stubFailoverFSM(),
		Logger:   testLogger(),
	})
	// Deliberately larger than UnhealthyThreshold (3): this counter only
	// damps a flapping compactor, and nothing is broken while it counts.
	if mgr.cfg.PreemptThreshold != 6 {
		t.Errorf("PreemptThreshold: got %d, want 6", mgr.cfg.PreemptThreshold)
	}
	// Derived from CooldownPeriod so that raising cluster.failover_cooldown
	// raises both; a constant would ignore the setting.
	if mgr.cfg.PreemptCooldown != 10*mgr.cfg.CooldownPeriod {
		t.Errorf("PreemptCooldown: got %v, want 10x CooldownPeriod (%v)", mgr.cfg.PreemptCooldown, 10*mgr.cfg.CooldownPeriod)
	}
}

// An operator who raises cluster.failover_cooldown must get a longer preempt
// cooldown too. The coordinator passes cluster.failover_cooldown straight
// into CooldownPeriod and sets nothing else, so this constructor is the whole
// rule — which is the point: a second copy in the coordinator would let this
// test stay green while the wiring silently diverged.
//
// Verified end to end on a live cluster as well: with
// cluster.failover_cooldown=600 the assign endpoint reported
// preemption_suppressed_seconds=6000.
func TestPreemptCooldownFollowsANonDefaultFailoverCooldown(t *testing.T) {
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry:       NewRegistry(&RegistryConfig{Logger: testLogger()}),
		RaftFSM:        stubFailoverFSM(),
		CooldownPeriod: 600 * time.Second,
		Logger:         testLogger(),
	})
	if mgr.cfg.PreemptCooldown != 6000*time.Second {
		t.Errorf("PreemptCooldown with a 600s failover cooldown: got %v, want 6000s", mgr.cfg.PreemptCooldown)
	}
}

// errorIs is errors.Is, named locally so the table above reads cleanly.
func errorIs(err, target error) bool { return stderrors.Is(err, target) }

// A standalone-role node must be refused, even though its static capability
// says it can compact.
//
// Found on a live rig: a cluster whose nodes never set cluster.role is
// all-standalone, and selectNewCompactor matches none of them — it looks for
// RoleCompactor then RoleWriter. Accepting a manual assignment there would be
// a one-way door: the lease lands on a node no failover path can replace, and
// because compactionClusterGate switches to lease mode as soon as the lease is
// non-empty, losing that node stops compaction cluster-wide with no recovery.
//
// The rule this pins: the manual endpoint accepts exactly the set
// selectNewCompactor can choose from, and nothing else.
func TestAssignToRefusesAStandaloneNode(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "standalone-1", RoleStandalone)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "compactor-1")

	_, err := mgr.AssignTo("standalone-1")
	if !errorIs(err, ErrCannotHoldCompactorLease) {
		t.Fatalf("AssignTo(standalone) = %v, want ErrCannotHoldCompactorLease", err)
	}
	if got := currentLease(raftNode); got != "compactor-1" {
		t.Errorf("a refused assignment moved the lease to %q", got)
	}
}

// The invariant itself, stated directly so a future edit to either side is
// caught here rather than in production: every role the manual endpoint
// accepts must be one an automatic failover can select.
func TestEveryAssignableRoleIsAlsoSelectable(t *testing.T) {
	for _, role := range []NodeRole{RoleWriter, RoleReader, RoleCompactor, RoleStandalone} {
		reg := NewRegistry(&RegistryConfig{MaxNodes: 8, Logger: testLogger()})
		n := NewNode("node-x", "node-x", role, "test-cluster")
		n.UpdateState(StateHealthy)
		if err := reg.Register(n); err != nil {
			t.Fatalf("Register: %v", err)
		}
		mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
			Registry: reg, RaftFSM: stubFailoverFSM(), Logger: testLogger(),
		})
		selectable := mgr.selectNewCompactor("") == "node-x"
		if canHoldCompactorLease(role) != selectable {
			t.Errorf("role %q: canHoldCompactorLease=%v but selectNewCompactor picks it=%v — the manual endpoint must accept exactly the set failover can choose from, or a lease can land where nothing can move it",
				role, canHoldCompactorLease(role), selectable)
		}
	}
}

// A negative cluster.failover_cooldown is not validated anywhere in config,
// and a negative duration would make every time.Since comparison exceed it —
// i.e. no suppression at all, silently, including of an operator override.
func TestANegativeCooldownBecomesNoCooldownExplicitly(t *testing.T) {
	mgr := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry:       NewRegistry(&RegistryConfig{Logger: testLogger()}),
		RaftFSM:        stubFailoverFSM(),
		CooldownPeriod: -1 * time.Second,
		Logger:         testLogger(),
	})
	if mgr.cfg.PreemptCooldown < 0 {
		t.Errorf("PreemptCooldown is %v; a negative window silently disables suppression", mgr.cfg.PreemptCooldown)
	}
}

// Only the Raft leader moves the lease. A follower running this would race
// the leader and apply a competing assignment.
func TestPreemptionIsInertOnAFollower(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")

	// Swap in a manager with no Raft node at all: checkCompactorHealth's
	// first statement is the leader gate, and a nil node takes the same
	// branch a follower does.
	follower := NewCompactorFailoverManager(&CompactorFailoverConfig{
		Registry:         reg,
		RaftFSM:          raftNode.FSM(),
		PreemptThreshold: 1,
		PreemptCooldown:  time.Millisecond,
		Logger:           testLogger(),
	})
	for i := 0; i < 10; i++ {
		follower.checkCompactorHealth()
	}
	leaseStaysAt(t, raftNode, "writer-1", 200*time.Millisecond)
	follower.mu.RLock()
	sustained := follower.preemptSustain
	follower.mu.RUnlock()
	if sustained != 0 {
		t.Errorf("a non-leader accumulated %d sustained checks; it must not evaluate preemption at all", sustained)
	}
	_ = mgr
}

// Stop() must return with a lease change in flight. wg.Add(1) happens under
// m.mu inside the check path, which is safe only because checkLoop's own
// counter keeps the WaitGroup above zero — worth pinning, because a refactor
// that moved the Add would deadlock Stop on shutdown.
func TestStopTerminatesWithAPreemptionInFlight(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tick(t, mgr, 3)

	done := make(chan error, 1)
	go func() { done <- mgr.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Stop did not return with a lease change in flight")
	}
}

// The status block an operator reads when the lease is somewhere unexpected.
func TestPreemptStatusReportsProgressAndCooldown(t *testing.T) {
	mgr, raftNode, reg := preemptRig(t)
	mgr.cfg.PreemptCooldown = time.Hour
	addNode(t, reg, raftNode, "writer-1", RoleWriter)
	addNode(t, reg, raftNode, "compactor-1", RoleCompactor)
	giveLeaseTo(t, raftNode, "writer-1")

	tick(t, mgr, 1)
	st := mgr.PreemptStatus()
	if st["sustained_checks"].(int) != 1 {
		t.Errorf("sustained_checks=%v after one qualifying tick, want 1", st["sustained_checks"])
	}
	if st["required_checks"].(int) != mgr.cfg.PreemptThreshold {
		t.Errorf("required_checks=%v, want %d", st["required_checks"], mgr.cfg.PreemptThreshold)
	}
	if _, ok := st["cooldown_remaining_seconds"]; ok {
		t.Error("cooldown_remaining_seconds reported before any lease change happened")
	}

	tick(t, mgr, 2)
	waitForLease(t, raftNode, "compactor-1", 5*time.Second)
	st = mgr.PreemptStatus()
	remaining, ok := st["cooldown_remaining_seconds"].(int)
	if !ok || remaining <= 0 {
		t.Errorf("cooldown_remaining_seconds=%v after a lease change; this is the number that answers \"when can it move again\"", st["cooldown_remaining_seconds"])
	}
	// And the counter must not read as though it overshot the threshold.
	if got := st["sustained_checks"].(int); got > mgr.cfg.PreemptThreshold {
		t.Errorf("sustained_checks=%d exceeds required_checks=%d", got, mgr.cfg.PreemptThreshold)
	}
}
