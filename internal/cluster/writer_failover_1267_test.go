package cluster

// #1267: the failover manager decides from the FSM's designated primary. A
// Raft leader whose process joined after the primary was promoted has no
// writer state for it in its registry (the join response carries none); it
// used to see no primary, count three ticks and "fail over" — to the dead
// primary itself when that had the lowest id (a no-op promotion that started
// the 60 s cooldown), or away from a healthy primary.

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// failoverRig: local writer "w-5" is the single-node Raft leader; "w-1" and
// "w-2" are writers known to the FSM and the registry, all healthy, and the
// registry carries NO writer state (as after a join).
func failoverRig(t *testing.T) (*WriterFailoverManager, *raft.Node, *Registry) {
	t.Helper()
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	mgr, rn, reg := electionRig(t, "w-5", "w-1", "w-2")
	mgr.cfg.AutoFailover = true
	mgr.cfg.UnhealthyThreshold = 3
	mgr.cfg.CooldownPeriod = time.Hour
	return mgr, rn, reg
}

func designate(t *testing.T, rn *raft.Node, id string) {
	t.Helper()
	if err := rn.PromoteWriter(id, "", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCond(t, func() bool { return rn.FSM().GetPrimaryWriterID() == id })
}

func setHealth(t *testing.T, reg *Registry, id string, state NodeState) {
	t.Helper()
	n, ok := reg.Get(id)
	if !ok {
		t.Fatalf("no node %s", id)
	}
	n.UpdateState(state)
	if err := reg.Register(n); err != nil {
		t.Fatal(err)
	}
}

func waitCond(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// wtick runs n health checks and waits for any failover they started.
func wtick(mgr *WriterFailoverManager, n int) {
	for i := 0; i < n; i++ {
		mgr.checkPrimaryHealth()
		mgr.wg.Wait()
	}
}

func lastFailover(mgr *WriterFailoverManager) time.Time {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.lastFailoverAt
}

// A healthy designated primary is never failed over, whatever the registry's
// writer state says (the worse consequence of the bug).
func TestFailoverKeepsAHealthyDesignatedPrimary(t *testing.T) {
	mgr, rn, reg := failoverRig(t)
	designate(t, rn, "w-2") // not the lowest id
	if p := reg.GetPrimaryWriter(); p != nil {
		t.Fatalf("setup: the registry should carry no writer state, has primary %s", p.ID)
	}
	wtick(mgr, 5)
	if got := rn.FSM().GetPrimaryWriterID(); got != "w-2" {
		t.Fatalf("a healthy primary was failed over to %s", got)
	}
	if !lastFailover(mgr).IsZero() {
		t.Fatal("cooldown started with no failover")
	}
}

// The observed case: the designated primary died but is still "healthy" in
// the registry (heartbeats not yet stale) and has the lowest id. Nothing
// happens until it turns unhealthy; then the failover moves the role to
// another writer and only that starts the cooldown.
func TestFailoverNeverRepromotesTheFailingPrimary(t *testing.T) {
	mgr, rn, reg := failoverRig(t)
	designate(t, rn, "w-1") // lowest id
	wtick(mgr, 5)           // still healthy in the registry
	if got := rn.FSM().GetPrimaryWriterID(); got != "w-1" || !lastFailover(mgr).IsZero() {
		t.Fatalf("acted on a primary that is still healthy: primary %s, cooldown %v", got, lastFailover(mgr))
	}
	setHealth(t, reg, "w-1", StateUnhealthy)
	wtick(mgr, 3)
	if got := rn.FSM().GetPrimaryWriterID(); got != "w-2" {
		t.Fatalf("primary after failover = %s, want w-2 (lowest healthy, not the failing w-1)", got)
	}
	if lastFailover(mgr).IsZero() {
		t.Fatal("a completed failover did not start the cooldown")
	}
}

// A failed attempt starts no cooldown: the next tick tries again.
func TestFailedFailoverAttemptRetriesWithoutCooldown(t *testing.T) {
	mgr, rn, reg := failoverRig(t)
	designate(t, rn, "w-1")
	setHealth(t, reg, "w-1", StateUnhealthy)
	allow := false
	mgr.cfg.Eligible = func(string) bool { return allow }
	wtick(mgr, 3)
	if !lastFailover(mgr).IsZero() {
		t.Fatal("a failed attempt started the cooldown")
	}
	mgr.mu.RLock()
	failed, reason := mgr.failedAttempts, mgr.lastFailureReason
	mgr.mu.RUnlock()
	if failed != 1 || reason != "no_candidate" {
		t.Fatalf("failed attempts %d reason %q", failed, reason)
	}
	allow = true
	wtick(mgr, 1)
	if got := rn.FSM().GetPrimaryWriterID(); got != "w-2" {
		t.Fatalf("no retry on the next tick: primary %s", got)
	}
}

// A candidate the FSM refuses is skipped, not proposed every tick.
func TestFailoverSkipsACandidateTheFSMRefused(t *testing.T) {
	mgr, rn, reg := failoverRig(t)
	designate(t, rn, "w-1")
	setHealth(t, reg, "w-1", StateUnhealthy)
	ghost := NewNode("w-0", "w-0", RoleWriter, "test-cluster") // in the registry, not the FSM
	ghost.UpdateState(StateHealthy)
	if err := reg.Register(ghost); err != nil {
		t.Fatal(err)
	}
	wtick(mgr, 3)
	mgr.mu.RLock()
	failed := mgr.failedAttempts
	mgr.mu.RUnlock()
	if failed != 1 || !lastFailover(mgr).IsZero() {
		t.Fatalf("refused promotion: failed attempts %d, cooldown %v", failed, lastFailover(mgr))
	}
	wtick(mgr, 1)
	if got := rn.FSM().GetPrimaryWriterID(); got != "w-2" {
		t.Fatalf("primary %s: the refused w-0 was not skipped", got)
	}
}

// A count from an earlier leadership does not carry over.
func TestFailoverCounterResetsOffLeader(t *testing.T) {
	mgr := NewWriterFailoverManager(&WriterFailoverConfig{Registry: NewRegistry(&RegistryConfig{Logger: testLogger()}), Logger: testLogger()})
	mgr.consecutiveFails = 7
	mgr.checkPrimaryHealth()
	if mgr.consecutiveFails != 0 {
		t.Fatalf("consecutiveFails %d off the leader", mgr.consecutiveFails)
	}
}

// HandOver refuses while a failover runs.
func TestHandOverRefusedDuringFailover(t *testing.T) {
	mgr, rn, _ := failoverRig(t)
	designate(t, rn, "w-1")
	mgr.failoverInProg = true
	if _, err := mgr.HandOver("w-1"); err == nil {
		t.Fatal("HandOver ran during a failover")
	}
}

// The join response's re-registration keeps a writer state this node knows,
// from its registry or else from the FSM; and promoting the designated node
// again does not flip it to standby.
func TestJoinKeepsWriterState(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	rn := startSingleNodeRaft(t, "w-5")
	c := newPauseForwardingCoordinator(t, "w-5", RoleWriter, rn)
	for _, id := range []string{"w-1", "w-2"} {
		if err := rn.AddNode(&raft.NodeInfo{ID: id, Name: id, Role: string(RoleWriter), ClusterName: "c", Address: "127.0.0.1:1", State: string(StateHealthy)}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	designate(t, rn, "w-1")
	if got := c.knownWriterState("w-1"); got != WriterStatePrimary {
		t.Fatalf("FSM fallback: %q", got)
	}
	n := NewNode("w-2", "w-2", RoleWriter, "c")
	n.SetWriterState(WriterStateStandby)
	if err := c.registry.Register(n); err != nil {
		t.Fatal(err)
	}
	if got := c.knownWriterState("w-2"); got != WriterStateStandby {
		t.Fatalf("registry state not kept: %q", got)
	}
	// The FSM wins over a registry entry that disagrees with it.
	stale := NewNode("w-1", "w-1", RoleWriter, "c")
	stale.SetWriterState(WriterStateStandby)
	if err := c.registry.Register(stale); err != nil {
		t.Fatal(err)
	}
	if got := c.knownWriterState("w-1"); got != WriterStatePrimary {
		t.Fatalf("a stale registry entry won over the FSM: %q", got)
	}
}

// The unhealthy callback recognises the designated primary from the FSM, not
// from the registry's (possibly empty) writer state.
func TestUnhealthyCallbackUsesTheDesignation(t *testing.T) {
	mgr, rn, reg := failoverRig(t)
	designate(t, rn, "w-2")
	n, _ := reg.Get("w-2")
	if n.GetWriterState() != WriterStateNone {
		t.Fatal("setup: registry writer state should be empty")
	}
	n.UpdateState(StateUnhealthy)
	mgr.HandleWriterUnhealthy(n)
	mgr.mu.RLock()
	fails := mgr.consecutiveFails
	mgr.mu.RUnlock()
	if fails != 1 {
		t.Fatalf("the designated primary's unhealthy callback counted %d", fails)
	}
	other, _ := reg.Get("w-1")
	mgr.HandleWriterUnhealthy(other)
	mgr.mu.RLock()
	fails = mgr.consecutiveFails
	mgr.mu.RUnlock()
	if fails != 1 {
		t.Fatalf("a standby's unhealthy callback counted (%d)", fails)
	}
}

// The join response path itself keeps the writer state (the wiring, not only
// the helper).
func TestJoinResponseKeepsPrimaryWriterState(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	rn := startSingleNodeRaft(t, "w-5")
	c := newPauseForwardingCoordinator(t, "w-5", RoleWriter, rn)
	c.cfg.SharedSecret = "" // accept the response unsigned
	if err := rn.AddNode(&raft.NodeInfo{ID: "w-1", Name: "w-1", Role: string(RoleWriter), ClusterName: "c", Address: "127.0.0.1:1", State: string(StateHealthy)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	designate(t, rn, "w-1")
	resp := &protocol.JoinResponse{Success: true, LeaderID: "w-5", Nodes: []protocol.NodeInfo{
		{ID: "w-1", Name: "w-1", Role: string(RoleWriter), State: string(StateHealthy), CoordAddr: "127.0.0.1:1"},
	}}
	if err := c.handleJoinResponse(protocol.NewJoinResponse(resp), "seed", ""); err != nil {
		t.Fatal(err)
	}
	if n, ok := c.registry.Get("w-1"); !ok || n.GetWriterState() != WriterStatePrimary {
		t.Fatalf("after the join response w-1 is %+v", n)
	}
}

// Re-selecting the designated primary (healthy again, the only eligible
// writer) moves nothing and starts no cooldown.
func TestReselectingTheDesignatedPrimaryIsNotAFailover(t *testing.T) {
	mgr, rn, _ := failoverRig(t)
	designate(t, rn, "w-1")
	mgr.cfg.Eligible = func(id string) bool { return id == "w-1" }
	mgr.mu.Lock()
	mgr.failoverInProg = true
	mgr.mu.Unlock()
	mgr.executeFailover("w-1", true)
	if !lastFailover(mgr).IsZero() {
		t.Fatal("re-promoting the designated primary started the cooldown")
	}
	mgr.mu.RLock()
	inProg := mgr.failoverInProg
	mgr.mu.RUnlock()
	if inProg {
		t.Fatal("the attempt never completed")
	}
}

// A promotion Raft committed is a success even when the propagation wait sees
// its context done (failover timeout, Stop).
func TestCommittedPromotionWithDoneContextIsASuccess(t *testing.T) {
	mgr, rn, reg := failoverRig(t)
	designate(t, rn, "w-1")
	setHealth(t, reg, "w-1", StateUnhealthy)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mgr.ctx = ctx
	mgr.mu.Lock()
	mgr.failoverInProg = true
	mgr.mu.Unlock()
	mgr.executeFailover("w-1", true)
	if got := rn.FSM().GetPrimaryWriterID(); got != "w-2" {
		t.Fatalf("primary %s", got)
	}
	if lastFailover(mgr).IsZero() {
		t.Fatal("a committed promotion was reported as a failure")
	}
	mgr.mu.RLock()
	fails, primary, failed := mgr.consecutiveFails, mgr.primaryID, mgr.failedAttempts
	mgr.mu.RUnlock()
	if fails != 0 || primary != "w-2" || failed != 0 {
		t.Fatalf("after a committed promotion: consecutiveFails %d primaryID %q failedAttempts %d", fails, primary, failed)
	}
}

// A Raft failure (here: Raft shut down) is not a refusal of the candidate: it
// stays selectable for the next attempt.
func TestRaftErrorDoesNotBackOffTheCandidate(t *testing.T) {
	mgr, rn, reg := failoverRig(t)
	designate(t, rn, "w-1")
	setHealth(t, reg, "w-1", StateUnhealthy)
	if err := rn.Stop(); err != nil {
		t.Fatal(err)
	}
	mgr.mu.Lock()
	mgr.failoverInProg = true
	mgr.mu.Unlock()
	mgr.executeFailover("w-1", true)
	mgr.mu.RLock()
	failed := mgr.failedAttempts
	mgr.mu.RUnlock()
	mgr.refusedMu.Lock()
	refused := len(mgr.refused)
	mgr.refusedMu.Unlock()
	if failed != 1 || refused != 0 {
		t.Fatalf("failed attempts %d, refused candidates %d; want 1 and 0", failed, refused)
	}
}
