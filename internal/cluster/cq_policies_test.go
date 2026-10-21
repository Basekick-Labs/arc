package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

func cqrow(id int64, name, interval string, active bool, runs int64, lpt *time.Time) CQLocalRow {
	return CQLocalRow{ID: id, Name: name, Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_1h",
		Query: "SELECT 1", Interval: interval, IsActive: active, Executions: runs, LastProcessed: lpt}
}

func at(h int) *time.Time {
	t := time.Date(2026, 10, 1, h, 0, 0, 0, time.UTC)
	return &t
}

// The leader seeds continuous queries on its own when the nodes agree and
// every active CQ has completed a run on some node at any time; each takes
// the newest watermark any node holds for the same definition.
func TestPlanCQSeedAutomatic(t *testing.T) {
	seed, why := planCQSeed(map[string]CQLocalRows{
		"a": {Policies: []CQLocalRow{cqrow(3, "q1", "1h", true, 9, at(10)), cqrow(4, "off", "1h", false, 0, nil)}},
		"b": {Policies: []CQLocalRow{cqrow(1, "q1", "1h", true, 0, at(12))}},
		"c": {},
	}, nil)
	if why != "" {
		t.Fatal(why)
	}
	if len(seed.CQ) != 2 || seed.CQKeepIDs || seed.CQFloor != 5 {
		t.Fatalf("seed keep=%v floor=%d %+v", seed.CQKeepIDs, seed.CQFloor, seed.CQ)
	}
	q1 := seed.CQ[1]
	if q1.Name != "q1" || q1.LastProcessed == nil || !q1.LastProcessed.Equal(*at(12)) || len(q1.Sources) != 2 {
		t.Fatalf("q1 %+v: want b's newer watermark and both sources", q1)
	}
	for name, perNode := range map[string]map[string]CQLocalRows{
		"never ran":       {"a": {Policies: []CQLocalRow{cqrow(1, "q", "1h", true, 0, nil)}}},
		"two definitions": {"a": {Policies: []CQLocalRow{cqrow(1, "q", "1h", true, 1, nil)}}, "b": {Policies: []CQLocalRow{cqrow(1, "q", "5m", true, 1, nil)}}},
	} {
		if _, why := planCQSeed(perNode, nil); why == "" {
			t.Fatalf("%s: seeded", name)
		}
	}
	// nil and empty optional values are one definition.
	empty := ""
	r1, r2 := cqrow(1, "q", "1h", true, 1, nil), cqrow(1, "q", "1h", true, 0, nil)
	r1.Description, r1.TagColumns = &empty, []string{}
	if cqContentOf(r1) != cqContentOf(r2) {
		t.Fatal("a NULL and an empty description, or no tags and an empty list, are different definitions")
	}
}

// An admin's choice seeds the chosen node's CQs, never-run ones included, if
// they still hash to what the admin saw, with the newest watermark of the
// same definition anywhere: choosing a node that never ran a CQ opens no gap.
func TestPlanCQSeedChoice(t *testing.T) {
	perNode := map[string]CQLocalRows{
		"a": {Policies: []CQLocalRow{cqrow(1, "q", "1h", true, 7, at(15))}},
		"b": {Policies: []CQLocalRow{cqrow(2, "q", "1h", true, 0, nil), cqrow(3, "new", "1h", true, 0, nil)}},
	}
	h := CQRowsHash(perNode["b"].Policies)
	seed, why := planCQSeed(perNode, &raft.PolicyAdoptChoice{Kind: raft.PolicyKindCQ, Node: "b", Hash: h})
	if why != "" {
		t.Fatal(why)
	}
	var q raft.CQEntry
	for _, e := range seed.CQ {
		if e.Name == "q" {
			q = e
		}
	}
	if len(seed.CQ) != 2 || q.LastProcessed == nil || !q.LastProcessed.Equal(*at(15)) {
		t.Fatalf("seed %+v: want q at a's watermark", seed.CQ)
	}
	if _, why := planCQSeed(perNode, &raft.PolicyAdoptChoice{Node: "b", Hash: "stale"}); !strings.Contains(why, "changed since") {
		t.Fatalf("stale hash: %q", why)
	}
	// The hash ignores run counts and the watermark.
	again := perNode["b"].Policies
	again[0].Executions, again[0].LastProcessed = 5, at(1)
	if CQRowsHash(again) != h {
		t.Fatal("hash changed with run count or watermark")
	}
}

// Continuous query definitions are forwardable like retention's; a watermark
// is forwarded only by the node it speaks for, with the cluster's own storage
// mode, and never in shared-storage mode.
func TestForwardApply_CQCommands(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.raftFSM = c.raftNode.FSM()
	all := []string{raft.CapabilityClusterPolicies, raft.CapabilityClusterRetention, raft.CapabilityClusterCQ}
	capNodes(t, c, map[string][]string{"raft-1": all, "writer-1": all, "writer-2": all, "reader-1": all})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	c.maybeEnableCQPolicies()
	waitFSM(t, c.fsmOrNil().CQPoliciesEnabled)
	epoch, err := c.proposeLeader(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindCQ, Leader: "raft-1", Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.proposeLeader(raft.CommandSeedPolicies, raft.SeedPoliciesPayload{Kind: raft.PolicyKindCQ, Epoch: epoch, CQFloor: 1, Now: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	fwd := func(from string, typ raft.CommandType, p any) *protocol.ForwardApplyAck {
		raw, _ := json.Marshal(p)
		return forwardOnce(t, c, from, &raft.Command{Type: typ, Payload: raw})
	}
	if ack := fwd("reader-1", raft.CommandCreateCQ, raft.CreateCQPayload{Name: "q", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "o", Query: "q", Interval: "1h", IsActive: true, Now: time.Now().UTC()}); ack.Status != "ok" {
		t.Fatalf("create from a reader: %+v", ack)
	}
	if ack := fwd("reader-1", raft.CommandUpdateCQ, raft.UpdateCQPayload{ID: 1, Interval: strPtr("5m")}); ack.Status != "ok" {
		t.Fatalf("update from a reader: %+v", ack)
	}
	for name, p := range map[string]raft.SetCQWatermarkPayload{
		"speaks for another node": {Proposer: "writer-1", ID: 1, To: time.Now()},
		"claims shared storage":   {Proposer: "writer-2", Shared: true, ID: 1, To: time.Now()},
	} {
		if ack := fwd("writer-2", raft.CommandSetCQWatermark, p); ack.Status != "error" || ack.Code != protocol.ForwardCodeAuth {
			t.Fatalf("%s: %+v, want refused", name, ack)
		}
	}
	c.cfg.SharedStorageMode = true
	if ack := fwd("writer-2", raft.CommandSetCQWatermark, raft.SetCQWatermarkPayload{Proposer: "writer-2", Shared: true, ID: 1, To: time.Now()}); ack.Status != "error" || ack.Code != protocol.ForwardCodeAuth {
		t.Fatalf("forwarded in shared storage: %+v, want refused", ack)
	}
	c.cfg.SharedStorageMode = false
	if ack := fwd("reader-1", raft.CommandDeleteCQ, raft.DeleteCQPayload{ID: 1}); ack.Status != "ok" {
		t.Fatalf("delete from a reader: %+v", ack)
	}
}

func strPtr(s string) *string { return &s }

// The CQ flag waits for every node, and once on, a build without it can no
// longer join.
func TestCQPoliciesEnableAndJoinGate(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if !hasCapability(c.localCapabilities(), raft.CapabilityClusterCQ) {
		t.Fatal("the CQ capability is not advertised")
	}
	all := []string{raft.CapabilityClusterPolicies, raft.CapabilityClusterRetention, raft.CapabilityClusterCQ}
	capNodes(t, c, map[string][]string{"raft-1": all, "writer-1": all, "writer-2": all, "reader-1": cpRetList})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	c.maybeEnableCQPolicies()
	if st := c.ClusterPoliciesStatus(); st.CQEnabled || len(st.CQBlocking) != 1 || st.CQBlocking[0] != "reader-1" {
		t.Fatalf("status %+v", st)
	}
	if err := c.ChoosePolicyAdoption(raft.PolicyKindCQ, "writer-2", "h", "admin"); !errors.Is(err, ErrCQPoliciesNotEnabled) {
		t.Fatalf("choice before the CQ flag: %v", err)
	}
	if _, err := c.CQCandidates(context.Background()); !errors.Is(err, ErrCQPoliciesNotEnabled) {
		t.Fatalf("candidates before the CQ flag: %v", err)
	}
	capNodes(t, c, map[string][]string{"reader-1": all})
	c.maybeEnableCQPolicies()
	waitFSM(t, c.fsmOrNil().CQPoliciesEnabled)
	if why, _ := c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: cpRetList}); !strings.Contains(why, "continuous queries") {
		t.Fatalf("a build without CQ support joined: %q", why)
	}
	for _, g := range []*clusterPoliciesGate{&c.clusterPolicies, &c.retentionPolicies, &c.cqPolicies} {
		if !g.mu.TryLock() {
			t.Fatal("a refused join left a gate locked")
		}
		g.mu.Unlock()
	}
	why, release := c.clusterPoliciesJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: all})
	if why != "" {
		t.Fatal(why)
	}
	release()
	if err := c.ChoosePolicyAdoption(raft.PolicyKindCQ, "nope", "h", "admin"); !errors.Is(err, ErrUnknownPolicyNode) {
		t.Fatalf("choice of an unknown node: %v", err)
	}
}

// enableCQ turns the CQ flag on in the adoption rig and gives each node a
// lister answering rows[node].
func (r *adoptRig) enableCQ(t *testing.T, rows map[string]*CQLocalRows) {
	t.Helper()
	raw, _ := json.Marshal(raft.EnableCQPoliciesPayload{Nodes: []string{"L", "F"}, Now: time.Now()})
	if err := r.leader.raftNode.Apply(&raft.Command{Type: raft.CommandEnableCQPolicies, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.fsmOrNil().CQPoliciesEnabled() }, "follower never saw the CQ flag")
	for id, c := range map[string]*Coordinator{"L": r.leader, "F": r.follower} {
		id := id
		c.SetLocalPolicyLister(raft.PolicyKindCQ, func(context.Context) (json.RawMessage, error) { return json.Marshal(rows[id]) })
	}
}

// End to end on a real Raft pair: a CQ that ran is adopted by itself with the
// newest watermark, and the follower reads it; one that never ran waits.
func TestAdoptCQ(t *testing.T) {
	r := newAdoptRig(t)
	r.enableCQ(t, map[string]*CQLocalRows{
		"L": {Policies: []CQLocalRow{cqrow(2, "q", "1h", true, 0, at(9))}},
		"F": {Policies: []CQLocalRow{cqrow(2, "q", "1h", true, 4, at(11))}},
	})
	r.leader.maybeSeedPolicies(context.Background())
	if !r.leader.CQPoliciesSeeded() {
		t.Fatalf("not seeded: %+v", r.leader.ClusterPoliciesStatus())
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.CQPoliciesSeeded() }, "follower never saw the seed")
	if _, err := r.follower.CQCandidates(context.Background()); !errors.Is(err, ErrNotLeaderForTopology) {
		t.Fatalf("candidates on the follower: %v", err)
	}
	q, ok := r.follower.ClusterCQByName("q")
	if !ok || q.LastProcessed == nil || !q.LastProcessed.Equal(*at(11)) {
		t.Fatalf("follower reads %+v", q)
	}

	r2 := newAdoptRig(t)
	r2.enableCQ(t, map[string]*CQLocalRows{"L": {Policies: []CQLocalRow{cqrow(1, "q", "1h", true, 0, nil)}}, "F": {}})
	r2.leader.maybeSeedPolicies(context.Background())
	if r2.leader.CQPoliciesSeeded() {
		t.Fatal("seeded a CQ that never ran")
	}
	if st := r2.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindCQ]; !strings.Contains(st.LastError, "never run") || st.Collecting {
		t.Fatalf("status %+v", st)
	}
}
