package cluster

import (
	"strings"
	"testing"
	"time"

	arcraft "github.com/basekick-labs/arc/internal/cluster/raft"
	hraft "github.com/hashicorp/raft"
)

// #880: #862 made Raft suffrage follow the node's role at JOIN time and
// deliberately left existing clusters alone, because AddNonvoter on a server
// that is already a voter updates its address and leaves Suffrage untouched.
// A reader or compactor recorded as a voter before that change stays one
// forever, and nothing reported it.
//
// These tests drive the decision against SYNTHETIC Raft configurations rather
// than a live cluster. Every interesting case needs a voter that should not be
// one, and manufacturing that on a real rig means two real voters — which
// flakes on CI, and which would also mean the test could not construct the
// dead-majority case at all without killing its own cluster.

// convergeRig builds a Coordinator with a real single-node Raft (for the FSM
// node table) and a registry, but drives planning with configurations the test
// supplies.
func convergeRig(t *testing.T, localID string) (*Coordinator, *Registry, *arcraft.Node) {
	t.Helper()
	rNode := startRaftNode(t, localID, allocFreePort(t), true)
	t.Cleanup(func() { _ = rNode.Stop() })
	if err := rNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	local := NewNode(localID, localID, RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 16, Logger: testLogger()})
	c := &Coordinator{registry: reg, localNode: local, raftNode: rNode, raftFSM: rNode.FSM(), logger: testLogger()}
	return c, reg, rNode
}

// knownNode records a node in the FSM (the authoritative role source) and,
// optionally, in the registry with a health state.
func knownNode(t *testing.T, c *Coordinator, reg *Registry, rNode *arcraft.Node, id, role string, inRegistry bool, state NodeState) {
	t.Helper()
	if err := rNode.AddNode(&arcraft.NodeInfo{
		ID: id, Name: id, Role: role, ClusterName: "test-cluster",
		Address: "127.0.0.1:1", State: string(StateHealthy),
	}, 5*time.Second); err != nil {
		t.Fatalf("AddNode %s: %v", id, err)
	}
	if !inRegistry {
		reg.Unregister(id)
		return
	}
	n := NewNode(id, id, ParseRole(role), "test-cluster")
	n.UpdateState(state)
	if err := reg.Register(n); err != nil {
		t.Fatalf("Register %s: %v", id, err)
	}
}

func raftCfg(servers ...hraft.Server) hraft.Configuration {
	return hraft.Configuration{Servers: servers}
}

func voter(id string) hraft.Server {
	return hraft.Server{ID: hraft.ServerID(id), Address: hraft.ServerAddress("127.0.0.1:1"), Suffrage: hraft.Voter}
}

func nonvoter(id string) hraft.Server {
	return hraft.Server{ID: hraft.ServerID(id), Address: hraft.ServerAddress("127.0.0.1:1"), Suffrage: hraft.Nonvoter}
}

// The headline case: a reader holding a vote from before #862.
func TestAReaderHoldingAVoteIsPlannedForDemotion(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)

	plan := c.planVoterConverge(raftCfg(voter("writer-1"), voter("reader-1")))
	if len(plan.planned) != 1 || plan.planned[0] != "reader-1" {
		t.Fatalf("planned=%v, want [reader-1]", plan.planned)
	}
	if plan.selfMismatch {
		t.Error("selfMismatch=true for a writer leader that correctly votes")
	}
}

// And a writer holding a vote is not touched.
func TestAWriterHoldingAVoteIsNotPlanned(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)

	plan := c.planVoterConverge(raftCfg(voter("writer-1"), voter("writer-2")))
	if len(plan.planned) != 0 {
		t.Fatalf("planned=%v, want none", plan.planned)
	}
}

// Role resolution reads the FSM first. Registry-first would launder an
// unrecognised role into "standalone" via ParseRole (nodeFromRaftInfo does
// exactly that), and standalone votes — so the record this tool exists to find
// would be reported as correct.
func TestAnUnrecognisedRoleIsUnresolvedNotLaundered(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	// In the FSM with a role Arc no longer recognises, AND in the registry,
	// where it has already been laundered to standalone.
	knownNode(t, c, reg, rNode, "legacy-1", "archiver", true, StateHealthy)

	if got, _ := reg.Get("legacy-1"); got == nil || got.Role != RoleStandalone {
		t.Fatalf("setup: the registry should have laundered the role to standalone, got %v", got)
	}

	res := c.resolveServerRole("legacy-1")
	if res.resolved {
		t.Fatalf("resolveServerRole laundered an unrecognised role to %q; registry-first would call it standalone, which votes, and report the mismatch as correct", res.role)
	}

	plan := c.planVoterConverge(raftCfg(voter("writer-1"), voter("legacy-1")))
	if len(plan.planned) != 0 {
		t.Errorf("planned=%v; a server whose role cannot be named must never be demoted", plan.planned)
	}
	if len(plan.skipped) != 1 || plan.skipped[0] != "legacy-1" {
		t.Errorf("skipped=%v, want [legacy-1]", plan.skipped)
	}
}

// An empty role is unresolved for the same reason: legitimate "unset" for
// local config, corrupt for a foreign record. ParseRole maps it to standalone.
func TestAnEmptyRoleIsUnresolved(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "blank-1", "", true, StateHealthy)

	if res := c.resolveServerRole("blank-1"); res.resolved {
		t.Errorf("an empty role resolved to %q", res.role)
	}
}

// A server in neither source is skipped, never demoted.
func TestAServerInNeitherSourceIsSkipped(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)

	plan := c.planVoterConverge(raftCfg(voter("writer-1"), voter("ghost-1")))
	if len(plan.planned) != 0 {
		t.Errorf("planned=%v, want none", plan.planned)
	}
	if len(plan.skipped) != 1 || plan.skipped[0] != "ghost-1" {
		t.Errorf("skipped=%v, want [ghost-1]", plan.skipped)
	}
}

// THE BLOCKER CASE. Demoting a live voter out of a set whose survivors are
// dead strands the configuration entry: it can never commit, no further
// membership change is possible, and the leader steps down. "Demote-only is
// safe" is false, and counting resolved servers does not catch it — at exactly
// half resolved the naive rule proceeds and kills the cluster.
func TestDemotionIsRefusedWhenItWouldLeaveADeadMajority(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)
	// Two stale legacy entries: in the Raft configuration, in neither source,
	// and not running. This is the correlation that makes the naive rule
	// dangerous — an unresolved server is disproportionately a dead one.
	cfg := raftCfg(voter("writer-1"), voter("reader-1"), voter("ghost-1"), voter("ghost-2"))

	plan := c.planVoterConverge(cfg)
	if len(plan.planned) != 1 || plan.planned[0] != "reader-1" {
		t.Fatalf("setup: planned=%v, want [reader-1]", plan.planned)
	}
	// Exactly half resolve, so a "fewer than half resolved" guard passes.
	if plan.resolved*2 < len(cfg.Servers) {
		t.Fatalf("setup: %d of %d resolved; this test needs exactly half", plan.resolved, len(cfg.Servers))
	}

	if _, err := c.checkConvergeSafety(plan.voters, plan.planned, false); err == nil {
		t.Fatal("demoting the only other live voter was allowed; the configuration change could never commit and the cluster would lose its leader permanently")
	} else if !errorIs(err, ErrVoterConvergeUnsafe) {
		t.Errorf("err=%v, want ErrVoterConvergeUnsafe", err)
	}
}

// The same shape, but the survivors are alive, so it is allowed.
func TestDemotionIsAllowedWhenTheSurvivingVotersAreHealthy(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)

	plan := c.planVoterConverge(raftCfg(voter("writer-1"), voter("writer-2"), voter("reader-1")))
	remaining, err := c.checkConvergeSafety(plan.voters, plan.planned, false)
	if err != nil {
		t.Fatalf("checkConvergeSafety: %v", err)
	}
	if len(remaining) != 2 {
		t.Errorf("remaining=%v, want two voters", remaining)
	}
}

// 2 -> 1 is a one-way door: if the last voter dies, no non-voter can ever
// campaign, joins cannot be processed and the manifest freezes. A warning is
// not a sufficient guard for that.
func TestCollapsingToASingleVoterNeedsExplicitOptIn(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)

	plan := c.planVoterConverge(raftCfg(voter("writer-1"), voter("reader-1")))
	if _, err := c.checkConvergeSafety(plan.voters, plan.planned, false); !errorIs(err, ErrVoterConvergeUnsafe) {
		t.Fatalf("err=%v, want a refusal without allow_single_voter", err)
	}
	if _, err := c.checkConvergeSafety(plan.voters, plan.planned, true); err != nil {
		t.Errorf("with allow_single_voter the operator has said it is intended, got %v", err)
	}
}

// Before #862 every joiner was AddVoter'd regardless of role, so a legacy
// cluster routinely has a READER as Raft leader — which is #862's whole
// motivation. Converging the others while the leader is itself a mismatched
// voter leaves IsPrimaryWriter matching nobody: retention, continuous queries
// and deletes stay dead, and reporting success would be a lie.
func TestAMismatchedLeaderIsDetectedRatherThanSilentlySkipped(t *testing.T) {
	c, reg, rNode := convergeRig(t, "reader-leader")
	knownNode(t, c, reg, rNode, "reader-leader", string(RoleReader), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)

	plan := c.planVoterConverge(raftCfg(voter("reader-leader"), voter("writer-1")))
	if !plan.selfMismatch {
		t.Fatal("the leader holds a vote its reader role does not grant, and the plan did not notice")
	}
	if len(plan.planned) != 0 {
		t.Errorf("planned=%v; self is never demoted directly — DemoteVoter refuses it, because ShutdownOnRemove would leave a zombie wrapper", plan.planned)
	}
}

// The transfer target must be a CURRENT voter whose role votes and that is
// healthy. timeoutNow makes the target a Candidate without checking suffrage,
// so transferring to a non-voter leaves the cluster leaderless.
func TestLeadershipTransferTargetMustBeAHealthyVotingVoter(t *testing.T) {
	c, reg, rNode := convergeRig(t, "reader-leader")
	knownNode(t, c, reg, rNode, "reader-leader", string(RoleReader), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-2", string(RoleReader), true, StateHealthy)      // role does not vote
	knownNode(t, c, reg, rNode, "writer-down", string(RoleWriter), true, StateUnhealthy) // not healthy
	knownNode(t, c, reg, rNode, "writer-nv", string(RoleWriter), true, StateHealthy)     // healthy but NOT a voter

	cfg := raftCfg(voter("reader-leader"), voter("reader-2"), voter("writer-down"), nonvoter("writer-nv"))
	if got := c.leadershipTransferTarget(cfg, "reader-leader"); got != "" {
		t.Errorf("target=%q; none of these is a healthy, voting-role, current voter", got)
	}

	knownNode(t, c, reg, rNode, "writer-ok", string(RoleWriter), true, StateHealthy)
	cfg = raftCfg(voter("reader-leader"), voter("writer-down"), voter("writer-ok"), nonvoter("writer-nv"))
	if got := c.leadershipTransferTarget(cfg, "reader-leader"); got != "writer-ok" {
		t.Errorf("target=%q, want writer-ok", got)
	}
}

// A node whose role votes but which is recorded as a non-voter is reported,
// never promoted. Promotion raises the quorum requirement, and if the promoted
// node is down the entry never commits and the cluster is unrecoverable.
func TestANonVoterThatShouldVoteIsReportedNotPromoted(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)

	plan := c.planVoterConverge(raftCfg(voter("writer-1"), nonvoter("writer-2")))
	if len(plan.planned) != 0 {
		t.Errorf("planned=%v; this endpoint never promotes", plan.planned)
	}
	if len(plan.notes) == 0 {
		t.Error("a non-voter whose role votes was neither promoted nor reported")
	}
}

// Detection must not require leadership, and must say whose view it is.
func TestMembershipStatusReportsTheLocalView(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)

	st := c.raftMembershipStatus()
	if st == nil {
		t.Fatal("membership status is nil with a Raft node configured")
	}
	if st["view"] != "local" || st["view_of"] != "writer-1" {
		t.Errorf("view=%v view_of=%v; a follower's configuration view can lag the leader's, so the block must say whose it is", st["view"], st["view_of"])
	}
	if _, ok := st["voting_servers"]; !ok {
		t.Error("voting_servers missing — the issue asks for the voter count by name")
	}
	if _, ok := st["voter_count"]; ok {
		t.Error("voter_count must not exist: raft.stats already publishes num_peers, which excludes self, and two adjacent counts differing by one is a support ticket")
	}
}

// A node with no Raft configured omits the block rather than publishing an
// empty one.
func TestMembershipStatusIsAbsentWithoutRaft(t *testing.T) {
	local := NewNode("solo", "solo", RoleStandalone, "test-cluster")
	c := &Coordinator{
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, Logger: testLogger()}),
		localNode: local,
		logger:    testLogger(),
	}
	if st := c.raftMembershipStatus(); st != nil {
		t.Errorf("membership status=%v, want nil with no Raft node", st)
	}
}

// A follower never converges.
func TestConvergeRefusesOnANonLeader(t *testing.T) {
	local := NewNode("solo", "solo", RoleWriter, "test-cluster")
	c := &Coordinator{
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, Logger: testLogger()}),
		localNode: local,
		logger:    testLogger(),
	}
	if _, err := c.ConvergeVoters(true, false); !errorIs(err, ErrClusterRaftNotConfigured) {
		t.Errorf("err=%v, want ErrClusterRaftNotConfigured with no Raft node", err)
	}
}

// A dry run with a NON-EMPTY plan must report it and change nothing.
//
// The first version of this test used a rig whose only server already matched,
// so `planned` was empty and ConvergeVoters returned before the dry-run gate
// was ever reached — deleting the gate entirely left it green. Driving a
// configuration that actually has something to demote is the whole point.
func TestDryRunWithANonEmptyPlanDemotesNothing(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)

	cfg := raftCfg(voter("writer-1"), voter("writer-2"), voter("reader-1"))
	before, err := rNode.GetConfiguration()
	if err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}

	res, err := c.convergeWithConfig(cfg, true, false, time.Now().Add(voterConvergeTimeout))
	if err != nil {
		t.Fatalf("convergeWithConfig: %v", err)
	}
	if len(res.WouldDemote) != 1 || res.WouldDemote[0] != "reader-1" {
		t.Fatalf("would_demote=%v, want [reader-1]", res.WouldDemote)
	}
	if len(res.Demoted) != 0 || len(res.Failed) != 0 {
		t.Errorf("a dry run acted: demoted=%v failed=%v", res.Demoted, res.Failed)
	}
	if res.VotingServersBefore != 3 || res.VotingServersAfter != 2 {
		t.Errorf("voting servers %d -> %d, want 3 -> 2", res.VotingServersBefore, res.VotingServersAfter)
	}

	after, err := rNode.GetConfiguration()
	if err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}
	if len(before.Servers) != len(after.Servers) {
		t.Errorf("a dry run changed the real configuration: %v -> %v", before.Servers, after.Servers)
	}
}

// And the same plan with dry_run=false reaches the demotion loop, so the gate
// is what separates them rather than an empty plan.
func TestARealRunReachesTheDemotionLoop(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)

	cfg := raftCfg(voter("writer-1"), voter("writer-2"), voter("reader-1"))
	res, err := c.convergeWithConfig(cfg, false, false, time.Now().Add(3*time.Second))
	if err != nil {
		t.Fatalf("convergeWithConfig: %v", err)
	}
	// reader-1 is not in this single-node Raft's real configuration, so the
	// demotion is attempted and does not succeed. Either outcome proves the
	// loop ran; what must NOT happen is the dry-run early return.
	if len(res.Demoted) == 0 && len(res.Failed) == 0 {
		t.Error("a real run neither demoted nor failed anything; it took the dry-run path")
	}
	if len(res.WouldDemote) != 0 {
		t.Errorf("a real run reported would_demote=%v", res.WouldDemote)
	}
}

// THE BLOCKER FIX. The plan is validated as a sequence of PREFIXES: the set
// left after planned[0..i]. If a demotion fails and the loop CONTINUES, the
// next one runs against a set nobody checked — skipping a dead node's
// demotion while proceeding with a healthy one is exactly how the guard is
// defeated, and the result is a configuration that can never commit and a
// cluster that can never elect a leader again.
//
// So the loop must stop at the first failure. Driven here with an expired
// deadline, which fails the first iteration deterministically: hashicorp/raft
// treats demoting a server that is not in the configuration as a successful
// no-op, so a bogus target cannot be used to force one.
func TestConvergeStopsAtTheFirstFailedDemotion(t *testing.T) {
	c, reg, rNode := convergeRig(t, "leader-1")
	knownNode(t, c, reg, rNode, "leader-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-a", string(RoleReader), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-b", string(RoleReader), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-3", string(RoleWriter), true, StateHealthy)

	cfg := raftCfg(voter("leader-1"), voter("writer-2"), voter("writer-3"),
		voter("reader-a"), voter("reader-b"))
	res, err := c.convergeWithConfig(cfg, false, false, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatalf("convergeWithConfig: %v", err)
	}
	if len(res.Failed) != 1 {
		t.Errorf("failed=%v (%d entries); the loop must stop at the FIRST failure, not carry on into a voter set it can no longer predict",
			res.Failed, len(res.Failed))
	}
	if _, ok := res.Failed["reader-a"]; !ok {
		t.Errorf("failed=%v, want the first planned demotion (reader-a)", res.Failed)
	}
	if len(res.Demoted) != 0 {
		t.Errorf("demoted=%v after the first step failed", res.Demoted)
	}
}

// Both demotions of a valid plan do run when nothing fails, so the stop above
// cannot be satisfied by refusing everything.
func TestConvergeDemotesEveryPlannedServerWhenNothingFails(t *testing.T) {
	c, reg, rNode := convergeRig(t, "leader-1")
	knownNode(t, c, reg, rNode, "leader-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-a", string(RoleReader), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-b", string(RoleReader), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-3", string(RoleWriter), true, StateHealthy)

	cfg := raftCfg(voter("leader-1"), voter("writer-2"), voter("writer-3"),
		voter("reader-a"), voter("reader-b"))
	res, err := c.convergeWithConfig(cfg, false, false, time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("convergeWithConfig: %v", err)
	}
	if len(res.Demoted) != 2 {
		t.Errorf("demoted=%v, want both planned servers", res.Demoted)
	}
	if len(res.Failed) != 0 {
		t.Errorf("failed=%v, want none", res.Failed)
	}
}

// The registry fallback: a server the FSM does not know but the registry does.
// That branch is what the FSM-first ordering falls back TO, and it was
// previously unexercised because every test node was added to the FSM first.
func TestRoleResolutionFallsBackToTheRegistry(t *testing.T) {
	c, reg, _ := convergeRig(t, "writer-1")
	// Registry only — never added to the FSM node table.
	n := NewNode("reg-only", "reg-only", RoleReader, "test-cluster")
	n.UpdateState(StateHealthy)
	if err := reg.Register(n); err != nil {
		t.Fatalf("Register: %v", err)
	}

	res := c.newRoleIndex().resolve("reg-only")
	if !res.resolved || res.role != RoleReader {
		t.Fatalf("resolve(reg-only) = %+v, want a resolved reader from the registry", res)
	}
}

// Phase 1's entire output, against a configuration that actually disagrees
// with the roles. Previously only view/view_of/key-presence were asserted.
func TestMembershipReportsMismatchesAndUnresolved(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)
	knownNode(t, c, reg, rNode, "legacy-1", "archiver", true, StateHealthy)

	m := c.membershipFromConfig(raftCfg(
		voter("writer-1"), voter("reader-1"), voter("legacy-1"), nonvoter("reader-2")))

	if m["voting_servers"].(int) != 3 {
		t.Errorf("voting_servers=%v, want 3", m["voting_servers"])
	}
	if m["mismatches"].(int) != 1 {
		t.Errorf("mismatches=%v, want 1 (reader-1 votes and should not)", m["mismatches"])
	}
	if m["unresolved"].(int) != 2 {
		t.Errorf("unresolved=%v, want 2 (legacy-1's role is unrecognised, reader-2 is in neither source)", m["unresolved"])
	}

	byID := map[string]map[string]interface{}{}
	for _, srv := range m["servers"].([]map[string]interface{}) {
		byID[srv["id"].(string)] = srv
	}
	if byID["reader-1"]["matches"] != false || byID["reader-1"]["expected"] != "nonvoter" {
		t.Errorf("reader-1 entry=%v, want expected=nonvoter matches=false", byID["reader-1"])
	}
	if byID["writer-1"]["matches"] != true {
		t.Errorf("writer-1 entry=%v, want matches=true", byID["writer-1"])
	}
	// Unresolved servers carry no expected/matches at all, rather than a
	// guess. A client must not read absence as "matches".
	if _, ok := byID["legacy-1"]["expected"]; ok {
		t.Errorf("legacy-1 carries an expected suffrage despite an unresolvable role: %v", byID["legacy-1"])
	}
}

// voterMismatchCount is what the health loop actually calls, and it counts only
// the direction converge can act on — a node whose role votes but which is
// recorded as a non-voter is re-promoted by its own next join, so warning about
// it would be an alarm whose remedy is a no-op, with text that is false for it.
func TestVoterMismatchCountOnlyCountsTheDemotableDirection(t *testing.T) {
	c, reg, rNode := convergeRig(t, "writer-1")
	knownNode(t, c, reg, rNode, "writer-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)

	// writer-2's role votes but it is recorded as a non-voter: the OTHER
	// direction. Nothing here can fix it, so it must not be counted.
	m := c.membershipFromConfig(raftCfg(voter("writer-1"), nonvoter("writer-2")))
	if m["mismatches"].(int) != 1 {
		t.Fatalf("setup: the status block should still REPORT it, mismatches=%v", m["mismatches"])
	}

	// Against the SAME configuration — voterMismatchCount reads the real Raft
	// config, so asserting on it here would silently test the rig's own
	// single-server config instead of this one. That is how the first version
	// of this test passed with the filter removed.
	if n := c.countDemotableMismatches(raftCfg(voter("writer-1"), nonvoter("writer-2"))); n != 0 {
		t.Errorf("countDemotableMismatches=%d; the warning must not fire for a direction converge cannot act on", n)
	}
	// And it does count the direction converge CAN act on.
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)
	if n := c.countDemotableMismatches(raftCfg(voter("writer-1"), voter("reader-1"))); n != 1 {
		t.Errorf("countDemotableMismatches=%d, want 1 for a reader holding a vote", n)
	}
}

// A node that is not the Raft leader refuses, via the leader check rather than
// via "no Raft node at all" — the previous test asserted the wrong branch.
func TestConvergeRefusesOnAFollower(t *testing.T) {
	// Bootstrap=false, so this node never becomes leader.
	rNode := startRaftNode(t, "follower-1", allocFreePort(t), false)
	t.Cleanup(func() { _ = rNode.Stop() })
	local := NewNode("follower-1", "follower-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	c := &Coordinator{
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, Logger: testLogger()}),
		localNode: local, raftNode: rNode, raftFSM: rNode.FSM(), logger: testLogger(),
	}
	if rNode.IsLeader() {
		t.Skip("node unexpectedly became leader")
	}
	if _, err := c.ConvergeVoters(true, false); !errorIs(err, ErrNotLeaderForTopology) {
		t.Errorf("err=%v, want ErrNotLeaderForTopology on a follower", err)
	}
}

// barrierWithin must not block past its deadline. Barrier's own timeout bounds
// only the enqueue; the future's Error() has no deadline at all.
func TestBarrierWithinRespectsItsDeadline(t *testing.T) {
	c, _, _ := convergeRig(t, "writer-1")
	if err := c.barrierWithin(0); err == nil {
		t.Error("a zero budget should not attempt a barrier")
	}
	start := time.Now()
	if err := c.barrierWithin(2 * time.Second); err != nil {
		t.Fatalf("barrierWithin on a healthy leader: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("barrierWithin took %v, past its own deadline", elapsed)
	}
}

// Converging below the documented number of voters is allowed — the issue is
// explicit that there must be no floor, because a cluster that genuinely has
// two ingest-capable nodes must still be able to converge — but it must be
// said out loud rather than done silently.
func TestConvergingBelowThreeVotersIsReported(t *testing.T) {
	c, reg, rNode := convergeRig(t, "leader-1")
	knownNode(t, c, reg, rNode, "leader-1", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "writer-2", string(RoleWriter), true, StateHealthy)
	knownNode(t, c, reg, rNode, "reader-1", string(RoleReader), true, StateHealthy)

	res, err := c.convergeWithConfig(raftCfg(voter("leader-1"), voter("writer-2"), voter("reader-1")),
		false, false, time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("convergeWithConfig: %v", err)
	}
	if res.VotingServersAfter != 2 {
		t.Fatalf("voting_servers_after=%d, want 2", res.VotingServersAfter)
	}
	found := false
	for _, n := range res.Notes {
		if strings.Contains(n, "documented minimum") {
			found = true
		}
	}
	if !found {
		t.Errorf("converging to %d voters produced no note about being below the recommended %d: %v",
			res.VotingServersAfter, writersForHA, res.Notes)
	}
}
