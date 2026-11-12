package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// #1619: DELETE /api/v1/cluster/nodes/:id answered 200 "node removed" for an
// ID that was never a member, after proposing a removal that changed nothing.
// An unknown ID now gets ErrNodeNotFound (404) and nothing is proposed. Every
// test below that expects a removal puts the ID in exactly one membership
// source, so dropping that source from the check fails exactly that test.

// removeRig is originRig with the FSM's node-removed callback wired to the
// registry, as the coordinator wires it, so a registry-only member's removal
// is observable.
func removeRig(t *testing.T) *Coordinator {
	t.Helper()
	c := originRig(t, "writer-1", RoleWriter)
	c.raftNode.FSM().SetCallbacks(nil, c.onRaftNodeRemoved, nil)
	return c
}

func configHas(t *testing.T, c *Coordinator, id string) bool {
	t.Helper()
	ids, err := c.raftNode.ConfigurationServerIDs()
	if err != nil {
		t.Fatalf("ConfigurationServerIDs: %v", err)
	}
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}

func TestRemoveUnknownNodeIsNotFoundAndProposesNothing(t *testing.T) {
	c := removeRig(t)
	if err := c.raftNode.Barrier(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	before, appliedBefore := c.raftNode.LastIndex(), c.raftNode.AppliedIndex()
	cfgBefore, _ := c.raftNode.ConfigurationServerIDs()

	err := c.RemoveNodeViaRaft("no-such-node")
	if !errorIs(err, ErrNodeNotFound) {
		t.Fatalf("removing an unknown ID = %v, want ErrNodeNotFound", err)
	}
	if strings.Contains(err.Error(), "not the leader") {
		t.Errorf("the not-found error %q contains \"not the leader\"; arcli would retry it on the leader", err)
	}
	time.Sleep(100 * time.Millisecond)
	if after := c.raftNode.LastIndex(); after != before {
		t.Errorf("an unknown ID was proposed: last index %d -> %d", before, after)
	}
	if after := c.raftNode.AppliedIndex(); after != appliedBefore {
		t.Errorf("an unknown ID was applied: applied index %d -> %d", appliedBefore, after)
	}
	cfgAfter, _ := c.raftNode.ConfigurationServerIDs()
	if strings.Join(cfgAfter, ",") != strings.Join(cfgBefore, ",") {
		t.Errorf("Raft configuration changed: %v -> %v", cfgBefore, cfgAfter)
	}
}

// writer-2 is in the FSM node table only (the rig adds it with AddNode; the
// registry is not told).
func TestRemoveFSMOnlyMember(t *testing.T) {
	c := removeRig(t)
	if _, ok := c.registry.Get("writer-2"); ok {
		t.Fatal("rig: writer-2 is in the registry")
	}
	if err := c.RemoveNodeViaRaft("writer-2"); err != nil {
		t.Fatalf("removing an FSM-only member: %v", err)
	}
	if _, ok := c.raftNode.FSM().GetNode("writer-2"); ok {
		t.Error("writer-2 is still in the cluster state")
	}
}

func TestRemoveRegistryOnlyMember(t *testing.T) {
	c := removeRig(t)
	registerNode(t, c.registry, "reg-only", RoleReader, StateDead)
	if _, ok := c.raftNode.FSM().GetNode("reg-only"); ok {
		t.Fatal("rig: reg-only is in the FSM")
	}
	if err := c.RemoveNodeViaRaft("reg-only"); err != nil {
		t.Fatalf("removing a registry-only member: %v", err)
	}
	waitFSM(t, func() bool { _, ok := c.registry.Get("reg-only"); return !ok })
}

// A stale Raft server missing from the registry and the FSM is the "dead node"
// the endpoint exists for. It is a nonvoter so the one-node test cluster keeps
// its quorum.
func TestRemoveRaftOnlyMember(t *testing.T) {
	c := removeRig(t)
	if err := c.raftNode.AddNonvoter("raft-only", allocFreePort(t), 5*time.Second); err != nil {
		t.Fatalf("AddNonvoter: %v", err)
	}
	if !configHas(t, c, "raft-only") {
		t.Fatal("rig: raft-only is not in the configuration")
	}
	if c.raftNode.FSM().ReferencesNode("raft-only") {
		t.Fatal("rig: the FSM references raft-only")
	}
	if err := c.RemoveNodeViaRaft("raft-only"); err != nil {
		t.Fatalf("removing a Raft-only member: %v", err)
	}
	if configHas(t, c, "raft-only") {
		t.Error("raft-only is still in the Raft configuration")
	}
}

// A writer that left gracefully is in none of the node table, the registry or
// the Raft configuration, only in the departed set. The operator removal is
// the documented way to give its instances and claims up before the departed
// TTL; a 404 here would leave them pinned for up to 24 h.
func TestRemoveDepartedWriter(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	if err := c.raftNode.RemoveNodeWithReason("writer-2", raft.RemoveReasonLeave, time.Now(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	fsm := c.raftNode.FSM()
	if _, ok := fsm.DepartedWriters()["writer-2"]; !ok {
		t.Fatal("rig: writer-2 did not depart")
	}
	if _, ok := fsm.GetNode("writer-2"); ok {
		t.Fatal("rig: writer-2 is still in the node table")
	}
	if err := c.RemoveNodeViaRaft("writer-2"); err != nil {
		t.Fatalf("removing a departed writer: %v", err)
	}
	if _, ok := fsm.DepartedWriters()["writer-2"]; ok {
		t.Error("writer-2 is still departed after the operator removal")
	}
}

// The flow the release notes document: a departed writer's claim is released
// by the operator removal.
func TestRemoveDepartedWriterReleasesItsClaims(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	// Only the designated primary takes over: make the claimant primary.
	if err := c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return c.raftNode.FSM().GetPrimaryWriterID() == "writer-2" })
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-1", Instance: 0x10, FreshStart: true})
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-1", By: "writer-2", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x10, Keys: map[string]raft.Intervals{"db/cpu": {{1, 5}}}}}})
	if err := c.raftNode.RemoveNodeWithReason("writer-2", raft.RemoveReasonLeave, time.Now(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	fsm := c.raftNode.FSM()
	if cl := fsm.ClaimsBy("writer-2"); len(cl) != 1 || cl[0].Released {
		t.Fatalf("rig: the departed writer's claim is %+v, want one unreleased", cl)
	}
	if err := c.RemoveNodeViaRaft("writer-2"); err != nil {
		t.Fatalf("removing a departed writer: %v", err)
	}
	if cl := fsm.ClaimsBy("writer-2"); len(cl) != 1 || !cl[0].Released {
		t.Errorf("the departed writer's claim is %+v after the removal, want released", cl)
	}
}

// The leader check comes before membership: a follower's view can lag, so it
// must answer "not the leader" (which arcli matches to retry on the leader),
// never a 404, even for an ID it does not know.
func TestRemoveOnNonLeaderIsNotLeaderEvenForAnUnknownID(t *testing.T) {
	rNode := startRaftNode(t, "follower-1", allocFreePort(t), false) // never bootstrapped: never leader
	t.Cleanup(func() { _ = rNode.Stop() })
	local := NewNode("follower-1", "follower-1", RoleReader, "test-cluster")
	c := &Coordinator{
		cfg:       &config.ClusterConfig{},
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()}),
		localNode: local,
		raftNode:  rNode,
		logger:    zerolog.Nop(),
	}
	err := c.RemoveNodeViaRaft("no-such-node")
	if err == nil || !strings.Contains(err.Error(), "not the leader") {
		t.Fatalf("removal on a non-leader = %v, want \"not the leader\"", err)
	}
	if errorIs(err, ErrNodeNotFound) {
		t.Errorf("a non-leader answered not-found: %v", err)
	}
}
