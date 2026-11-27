package cluster

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #1627: this build advertises governance and adopts it. With every node on
// it, the enable pass turns the governance flag on, the kind is active, and
// the adoption loop collects it after a read-only probe.
func TestGovernancePoliciesEnableWhenEveryNodeAdvertises(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	local := c.localCapabilities()
	if !hasCapability(local, raft.CapabilityClusterGovernance) {
		t.Fatalf("this build does not advertise %s", raft.CapabilityClusterGovernance)
	}
	inKinds := false
	for _, k := range policyKinds {
		inKinds = inKinds || k == raft.PolicyKindGovernance
	}
	if !inKinds || !probesFirst(raft.PolicyKindGovernance) {
		t.Fatal("governance is not adopted, or not probed before a collect")
	}
	older := []string{raft.CapabilityClusterPolicies, raft.CapabilityClusterRetention, raft.CapabilityClusterCQ}
	capNodes(t, c, map[string][]string{"raft-1": local, "writer-1": local, "writer-2": local, "reader-1": older})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	c.maybeEnablePolicyKinds()
	waitFSM(t, func() bool { return c.fsmOrNil().CQPoliciesEnabled() })
	if c.fsmOrNil().GovernancePoliciesEnabled() {
		t.Fatal("governance enabled while reader-1 runs an older build")
	}
	if st := c.ClusterPoliciesStatus(); st.GovernanceEnabled || len(st.GovernanceBlocking) != 1 || st.GovernanceBlocking[0] != "reader-1" {
		t.Fatalf("status %+v", st)
	}
	if err := c.ChoosePolicyAdoption(raft.PolicyKindGovernance, "writer-2", "h", "admin"); !errors.Is(err, ErrGovernancePoliciesNotEnabled) {
		t.Fatalf("choice before the governance flag: %v", err)
	}
	capNodes(t, c, map[string][]string{"reader-1": local})
	c.maybeEnablePolicyKinds()
	waitFSM(t, c.fsmOrNil().GovernancePoliciesEnabled)
	if !c.policyKindActive(raft.PolicyKindGovernance) || !c.ClusterPoliciesStatus().GovernanceEnabled {
		t.Fatal("governance not active with its flag on")
	}
	if _, ok := c.ClusterPoliciesStatus().Kinds[raft.PolicyKindGovernance]; !ok {
		t.Fatal("status does not list governance")
	}
}

// Once the governance flag is on (here forced through Raft, as part 2's
// enable pass will propose it), a build without the capability cannot join,
// and a refused join leaves every gate unlocked. Before it, such a build
// joins, and the governance gate is held until the join finishes.
func TestGovernancePoliciesJoinGate(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	older := []string{raft.CapabilityClusterPolicies, raft.CapabilityClusterRetention, raft.CapabilityClusterCQ}
	all := append(append([]string(nil), older...), raft.CapabilityClusterGovernance)
	capNodes(t, c, map[string][]string{"raft-1": older, "writer-1": older, "writer-2": older, "reader-1": older})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	c.maybeEnablePolicyKinds()
	waitFSM(t, func() bool { return c.fsmOrNil().RetentionPoliciesEnabled() && c.fsmOrNil().CQPoliciesEnabled() })

	why, release := c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: older})
	if why != "" {
		t.Fatalf("refused before the governance flag: %s", why)
	}
	if c.governancePolicies.mu.TryLock() {
		t.Fatal("the governance gate released its lock before the join finished")
	}
	release()

	var ids []string
	for _, n := range c.fsmOrNil().GetAllNodes() {
		ids = append(ids, n.ID)
	}
	raw, _ := json.Marshal(raft.EnableGovernancePoliciesPayload{Nodes: ids, Now: time.Now().UTC()})
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableGovernancePolicies, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, c.fsmOrNil().GovernancePoliciesEnabled)
	if !c.policyKindActive(raft.PolicyKindGovernance) {
		t.Fatal("governance not active with its flag on")
	}
	if why, _ := c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: older}); !strings.Contains(why, "governance") {
		t.Fatalf("a build without governance support joined: %q", why)
	}
	for _, g := range []*clusterPoliciesGate{&c.clusterPolicies, &c.retentionPolicies, &c.cqPolicies, &c.governancePolicies} {
		if !g.mu.TryLock() {
			t.Fatal("a refused join left a gate locked")
		}
		g.mu.Unlock()
	}
	why, release = c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: all})
	if why != "" {
		t.Fatalf("a capable node refused: %s", why)
	}
	release()
	for _, g := range []*clusterPoliciesGate{&c.clusterPolicies, &c.retentionPolicies, &c.cqPolicies, &c.governancePolicies} {
		if !g.mu.TryLock() {
			t.Fatal("an admitted join left a gate locked after release")
		}
		g.mu.Unlock()
	}
}

// policyKindActive reads the table: tiering needs only the cluster policies
// flag, every other kind its own flag too, and an unknown kind is never
// active.
func TestPolicyKindActiveByTable(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if c.policyKindActive(raft.PolicyKindTiering) {
		t.Fatal("tiering active before the cluster policies flag")
	}
	capNodes(t, c, map[string][]string{"raft-1": cpList, "writer-1": cpList, "writer-2": cpList, "reader-1": cpList})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	if !c.policyKindActive(raft.PolicyKindTiering) {
		t.Fatal("tiering not active with the cluster policies flag")
	}
	for _, k := range []string{raft.PolicyKindRetention, raft.PolicyKindCQ, raft.PolicyKindGovernance, "", "nope"} {
		if c.policyKindActive(k) {
			t.Fatalf("%q active without a flag of its own", k)
		}
	}
	if err := c.ChoosePolicyAdoption(raft.PolicyKindCQ, "writer-2", "h", "admin"); !errors.Is(err, ErrCQPoliciesNotEnabled) {
		t.Fatalf("cq choice: %v", err)
	}
	if err := c.ChoosePolicyAdoption(raft.PolicyKindRetention, "writer-2", "h", "admin"); !errors.Is(err, ErrRetentionPoliciesNotEnabled) {
		t.Fatalf("retention choice: %v", err)
	}
}
