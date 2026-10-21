package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

var cpList = []string{raft.CapabilityClusterPolicies}

// #1324: every build advertises the cluster policies capability.
func TestClusterPoliciesCapabilityAdvertised(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if !hasCapability(c.localCapabilities(), raft.CapabilityClusterPolicies) {
		t.Fatalf("capabilities %v lack %s", c.localCapabilities(), raft.CapabilityClusterPolicies)
	}
}

// The leader enables cluster policies once every node in the FSM and every
// Raft server advertised it.
func TestClusterPoliciesEnableWaitsForEveryNode(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	capNodes(t, c, map[string][]string{"raft-1": cpList, "writer-1": cpList, "writer-2": cpList, "reader-1": capList})
	c.maybeEnableClusterPolicies()
	if c.ClusterPoliciesActive() {
		t.Fatal("enabled while reader-1 lacks the capability")
	}
	if st := c.ClusterPoliciesStatus(); len(st.Blocking) != 1 || st.Blocking[0] != "reader-1" {
		t.Fatalf("status = %+v, want reader-1 blocking", st)
	}
	capNodes(t, c, map[string][]string{"reader-1": cpList})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	if st := c.ClusterPoliciesStatus(); !st.Enabled || len(st.Blocking) != 0 {
		t.Fatalf("status = %+v", st)
	}
}

// A Raft server whose node record never applied keeps the flag off.
func TestClusterPoliciesEnableWaitsForRaftServers(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	capNodes(t, c, map[string][]string{"writer-1": cpList, "writer-2": cpList, "reader-1": cpList})
	c.maybeEnableClusterPolicies()
	time.Sleep(200 * time.Millisecond)
	if c.ClusterPoliciesActive() {
		t.Fatal("enabled with raft-1 unknown to the node table")
	}
	if st := c.ClusterPoliciesStatus(); len(st.Blocking) != 1 || st.Blocking[0] != "raft-1" {
		t.Fatalf("status = %+v", st)
	}
}

// A join holds the gate's lock until it finishes; once the flag is on, a join
// without the capability is refused before it enters the Raft configuration.
func TestClusterPoliciesJoinGate(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	why, release := c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9"})
	if why != "" {
		t.Fatalf("refused before the flag: %s", why)
	}
	if c.clusterPolicies.mu.TryLock() {
		t.Fatal("the gate released the lock before the join finished")
	}
	release()
	capNodes(t, c, map[string][]string{"raft-1": cpList, "writer-1": cpList, "writer-2": cpList, "reader-1": cpList})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	if why, _ := c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: capList}); why == "" {
		t.Fatal("an older build joined a cluster with cluster policies on")
	}
	if !c.clusterPolicies.mu.TryLock() {
		t.Fatal("a refused join left the gate locked")
	}
	c.clusterPolicies.mu.Unlock()
	why, release = c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: cpList})
	if why != "" {
		t.Fatalf("a capable node refused: %s", why)
	}
	release()
}

// A join without the capability waits for a barrier only while an enable
// proposal may still commit, and is refused when the barrier cannot run.
func TestClusterPoliciesJoinGateBarriersWhileAProposalIsInFlight(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if err := c.raftNode.Stop(); err != nil {
		t.Fatal(err)
	}
	old := &protocol.JoinRequest{NodeID: "w9", Capabilities: capList}
	why, release := c.clusterPoliciesJoinGate(old)
	if why != "" {
		t.Fatalf("no proposal in flight, refused: %s", why)
	}
	release()
	c.clusterPolicies.inFlight = true
	if why, _ := c.clusterPoliciesJoinGate(old); !strings.Contains(why, "could not confirm") {
		t.Fatalf("a proposal in flight and no barrier: %q", why)
	}
	if !c.clusterPolicies.mu.TryLock() {
		t.Fatal("a refused join left the gate locked")
	}
	c.clusterPolicies.mu.Unlock()
}

// Through handleJoinRequest: every join releases the gate, and once the flag
// is on a build without the capability never reaches the node table.
func TestClusterPoliciesJoinThroughTheHandler(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	both := []string{raft.CapabilityCompactionCommit, raft.CapabilityClusterPolicies}
	for _, id := range []string{"reader-9", "reader-8"} {
		if resp := deliverJoin(t, c, ccJoin(id, both)); resp == nil || !resp.Success {
			t.Fatalf("join %s: %+v", id, resp)
		}
	}
	if !c.clusterPolicies.mu.TryLock() {
		t.Fatal("a finished join left the gate locked")
	}
	c.clusterPolicies.mu.Unlock()
	capNodes(t, c, map[string][]string{"raft-1": cpList, "writer-1": cpList, "writer-2": cpList, "reader-1": cpList,
		"reader-9": both, "reader-8": both})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	if resp := deliverJoin(t, c, ccJoin("reader-7", ccList)); resp == nil || resp.Success {
		t.Fatalf("an older build joined with cluster policies on: %+v", resp)
	}
	if _, ok := c.raftNode.FSM().GetNode("reader-7"); ok {
		t.Fatal("the refused node is in the node table")
	}
	if resp := deliverJoin(t, c, ccJoin("reader-6", both)); resp == nil || !resp.Success {
		t.Fatalf("a capable join after a refusal: %+v", resp)
	}
	st, ok := c.Status()["cluster_policies"].(ClusterPoliciesStatus)
	if !ok || !st.Enabled {
		t.Fatalf("/cluster status cluster_policies = %#v", c.Status()["cluster_policies"])
	}
}

var cpRetList = []string{raft.CapabilityClusterPolicies, raft.CapabilityClusterRetention}

// #1324 stage 2: retention has its own flag. A node on a build that knows
// only tiering keeps it off (and is named), and once it is on such a build
// can no longer join: it would refuse the retention commands and drop the
// retention state from its snapshots.
func TestRetentionPoliciesEnableAndJoinGate(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if !hasCapability(c.localCapabilities(), raft.CapabilityClusterRetention) {
		t.Fatalf("capabilities %v lack %s", c.localCapabilities(), raft.CapabilityClusterRetention)
	}
	capNodes(t, c, map[string][]string{"raft-1": cpRetList, "writer-1": cpRetList, "writer-2": cpRetList, "reader-1": cpList})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	c.maybeEnableRetentionPolicies()
	if c.fsmOrNil().RetentionPoliciesEnabled() {
		t.Fatal("retention enabled while reader-1 runs a tiering-only build")
	}
	if st := c.ClusterPoliciesStatus(); st.RetentionEnabled || len(st.RetentionBlocking) != 1 || st.RetentionBlocking[0] != "reader-1" {
		t.Fatalf("status = %+v", st)
	}
	if _, ok := c.ClusterPoliciesStatus().Kinds[raft.PolicyKindRetention]; ok {
		t.Fatal("status lists retention before its flag")
	}
	// Before the flag a tiering-only build may still join.
	why, release := c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: cpList})
	if why != "" {
		t.Fatalf("refused before the retention flag: %s", why)
	}
	if c.retentionPolicies.mu.TryLock() {
		t.Fatal("the retention gate released its lock before the join finished")
	}
	release()

	capNodes(t, c, map[string][]string{"reader-1": cpRetList})
	c.maybeEnableRetentionPolicies()
	waitFSM(t, c.fsmOrNil().RetentionPoliciesEnabled)
	if why, _ := c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: cpList}); !strings.Contains(why, "retention") {
		t.Fatalf("a tiering-only build joined: %q", why)
	}
	for _, g := range []*clusterPoliciesGate{&c.clusterPolicies, &c.retentionPolicies} {
		if !g.mu.TryLock() {
			t.Fatal("a refused join left a gate locked")
		}
		g.mu.Unlock()
	}
	why, release = c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: cpRetList})
	if why != "" {
		t.Fatalf("a capable node refused: %s", why)
	}
	release()
	if st := c.ClusterPoliciesStatus(); !st.RetentionEnabled || st.Kinds[raft.PolicyKindRetention].Seeded {
		t.Fatalf("status = %+v", st)
	}
}

// The enable loop turns on both flags: cluster policies, then retention's.
func TestClusterPoliciesEnableLoopEnablesRetention(t *testing.T) {
	prev := clusterPoliciesEnableEvery
	clusterPoliciesEnableEvery = 20 * time.Millisecond
	t.Cleanup(func() { clusterPoliciesEnableEvery = prev })
	c := originRig(t, "writer-1", RoleWriter)
	all := append(append([]string(nil), cpRetList...), raft.CapabilityClusterCQ)
	capNodes(t, c, map[string][]string{"raft-1": all, "writer-1": all, "writer-2": all, "reader-1": all})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.clusterPoliciesEnableLoop(ctx)
	waitFSM(t, func() bool {
		return c.ClusterPoliciesActive() && c.fsmOrNil().RetentionPoliciesEnabled() && c.fsmOrNil().CQPoliciesEnabled()
	})
}
