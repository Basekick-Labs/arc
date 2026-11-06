package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/license"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func TestLicenseCapacitySelectsSupportedSubsetWithoutRemovingParticipants(t *testing.T) {
	nodes := []*raft.NodeInfo{
		{ID: "w2", Role: "writer", CoreCount: 4},
		{ID: "c1", Role: "compactor", CoreCount: 2},
		{ID: "r1", Role: "reader", CoreCount: 2},
		{ID: "w1", Role: "writer", CoreCount: 4},
	}
	c := lifecycleCapacity{Grant: "paid", MaxMachines: 2, MaxCores: 6, Ready: true}
	s, err := chooseServingAllocation(nodes, "w1", c)
	if err != nil {
		t.Fatal(err)
	}
	if s.Owner != "w1" || !reflect.DeepEqual(s.Allocation.ServingIDs, []string{"w1", "r1"}) {
		t.Fatalf("unexpected supported subset: %+v", s)
	}
	if len(s.Participants) != 4 {
		t.Fatal("capacity selection removed control participants")
	}
	for _, id := range []string{"w1", "r1"} {
		if !allocationAllows(s, nodes, c, id) {
			t.Fatalf("selected node %s fenced", id)
		}
	}
	for _, id := range []string{"w2", "c1"} {
		if allocationAllows(s, nodes, c, id) {
			t.Fatalf("excess node %s serves", id)
		}
	}
	// The old key remains separate; merely installing another key with equal
	// budgets cannot use the prior cohort's allocation.
	other := c
	other.Grant = "trial"
	if allocationAllows(s, nodes, other, "w1") {
		t.Fatal("different key reused allocation")
	}
	other = c
	other.Ready = false
	if allocationAllows(s, nodes, other, "w1") {
		t.Fatal("pending runtime served")
	}
	// A count refreshed by a retained-node rejoin must be checked immediately,
	// before the leader can commit its next allocation.
	nodes[2].CoreCount = 3
	if allocationAllows(s, nodes, c, "w1") {
		t.Fatal("stale allocation exceeded new core total")
	}
}

func TestLicenseCapacityRetainsPrimaryAndUsesRemainingBudget(t *testing.T) {
	nodes := []*raft.NodeInfo{
		{ID: "primary", Role: "writer", CoreCount: 8},
		{ID: "cheaper", Role: "writer", CoreCount: 2},
		{ID: "large-reader", Role: "reader", CoreCount: 9},
		{ID: "small-reader", Role: "reader", CoreCount: 2},
	}
	c := lifecycleCapacity{Grant: "paid", MaxMachines: 3, MaxCores: 6, Ready: true}
	if _, err := chooseServingAllocation(nodes, "primary", c); err == nil {
		t.Fatal("capacity selection bypassed designated primary's data ownership")
	}
	// Restart applied the smaller runtime core limit on the same writer.
	nodes[0].CoreCount = 2
	s, err := chooseServingAllocation(nodes, "primary", c)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.Allocation.ServingIDs, []string{"primary", "small-reader", "cheaper"}) {
		t.Fatalf("did not skip oversized node and fill supported budget: %v", s.Allocation.ServingIDs)
	}
	before := capacityTopology(nodes)
	nodes[0].State = "unhealthy"
	if capacityTopology(nodes) != before {
		t.Fatal("health changed durable capacity topology")
	}
	again, err := chooseServingAllocation(nodes, "primary", c)
	if err != nil || !reflect.DeepEqual(s.Allocation, again.Allocation) {
		t.Fatal("health reshuffled capacity ownership")
	}
}

func TestLicenseCapacityGrantBindsKeyAndCapabilities(t *testing.T) {
	l := &license.License{LicenseKey: "trial", MaxCores: 8, MaxMachines: 3, Features: []string{"clustering", "writer_failover"}}
	trial := licenseCapacity(l)
	l.LicenseKey = "separate-production"
	paid := licenseCapacity(l)
	if trial.Grant == paid.Grant {
		t.Fatal("different keys share grant")
	}
	l.Features = []string{"writer_failover", "clustering"}
	if licenseCapacity(l) != paid {
		t.Fatal("feature ordering changed grant")
	}
	l.MaxCores = 4
	if licenseCapacity(l).Grant == paid.Grant {
		t.Fatal("capacity change did not change grant")
	}
}

func TestLicenseCapacityRouterFencesLocalAndDirectWrites(t *testing.T) {
	registry := newTestRegistry()
	local := NewNode("excess", "excess", RoleWriter, "test-cluster")
	selected := NewNode("selected", "selected", RoleWriter, "test-cluster")
	for _, n := range []*Node{local, selected} {
		n.UpdateState(StateHealthy)
		if err := registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	r := NewRouter(&RouterConfig{LocalNode: local, Registry: registry, Allowed: func(id string) bool { return id == selected.ID }, Logger: zerolog.Nop()})
	target, err := r.writeTarget()
	if err != nil || target.ID != selected.ID {
		t.Fatalf("fenced local writer bypassed allocation: %v %v", target, err)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://example.invalid/api/v1/write", nil)
	for _, forward := range []func(context.Context, *Node, *http.Request) (*http.Response, error){r.ForwardWriteTo, r.ForwardImportTo} {
		if _, err := forward(context.Background(), local, req); !errors.Is(err, ErrWriteForwardUnavailable) {
			t.Fatalf("direct write bypassed allocation: %v", err)
		}
	}
	// Reader filtering must happen before fallback, so an excess reader does
	// not hide a supported writer's query capability.
	reader := NewNode("excess-reader", "excess-reader", RoleReader, "test-cluster")
	reader.UpdateState(StateHealthy)
	if err := registry.Register(reader); err != nil {
		t.Fatal(err)
	}
	if got := r.allowedNodes(registry.GetReaders()); len(got) != 0 {
		t.Fatal("fenced reader remained routable")
	}
}

func TestLicenseCapacityCompactorSelectionExcludesExcess(t *testing.T) {
	r := newTestRegistry()
	for _, id := range []string{"a-excess", "b-selected"} {
		n := NewNode(id, id, RoleCompactor, "test-cluster")
		n.UpdateState(StateHealthy)
		if err := r.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	m := NewCompactorFailoverManager(&CompactorFailoverConfig{Registry: r, Eligible: func(id string) bool { return id == "b-selected" }, Logger: zerolog.Nop()})
	if got := m.selectNewCompactor(""); got != "b-selected" {
		t.Fatalf("selected excess compactor: %s", got)
	}
}

func capacityTestApply(t *testing.T, f *raft.ClusterFSM, kind raft.CommandType, payload interface{}) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := json.Marshal(raft.Command{Type: kind, Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	if result := f.Apply(&hraft.Log{Data: cmd}); result != nil {
		t.Fatal(result)
	}
}

func TestLicenseCapacitySharedLeadershipUsesHealthySelectedVoter(t *testing.T) {
	fsm := raft.NewClusterFSM(zerolog.Nop())
	r := newTestRegistry()
	cfg := hraft.Configuration{}
	ids := []string{"w1", "w2", "excess", "reader", "nonvoter"}
	for _, id := range ids {
		role := RoleWriter
		if id == "reader" {
			role = RoleReader
		}
		n := NewNode(id, id, role, "test-cluster")
		n.UpdateState(StateHealthy)
		if id == "w1" {
			n.UpdateState(StateUnhealthy)
		}
		if err := r.Register(n); err != nil {
			t.Fatal(err)
		}
		capacityTestApply(t, fsm, raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: id, Role: string(role), CoreCount: 2}})
		suffrage := hraft.Voter
		if id == "nonvoter" {
			suffrage = hraft.Nonvoter
		}
		cfg.Servers = append(cfg.Servers, hraft.Server{ID: hraft.ServerID(id), Address: hraft.ServerAddress(id + ":9100"), Suffrage: suffrage})
	}
	s := raft.LicenseLifecycleState{Generation: 1, Mode: "active", Controller: "excess", Owner: "w1", Participants: ids, Allocation: &raft.LicenseServingAllocation{Grant: "paid", Topology: "topology", MaxMachines: 4, MaxCores: 8, ServingIDs: []string{"w1", "w2", "reader", "nonvoter"}}}
	capacityTestApply(t, fsm, raft.CommandLicenseLifecycle, s)
	c := &Coordinator{raftFSM: fsm, registry: r, localNode: NewNode("excess", "excess", RoleWriter, "test-cluster")}
	capacity := lifecycleCapacity{Grant: "paid", MaxMachines: 4, MaxCores: 8, Ready: true}
	if target := c.capacityLeadershipTarget(cfg, capacity); target != "w2" {
		t.Fatalf("dead owner blocked healthy selected successor: %q", target)
	}
	r.UpdateNodeState("w2", StateUnhealthy)
	if target := c.capacityLeadershipTarget(cfg, capacity); target != "" {
		t.Fatalf("transferred to excess, reader, dead node or nonvoter: %q", target)
	}
	r.UpdateNodeState("w2", StateHealthy)
	capacity.Grant = "replacement"
	if target := c.capacityLeadershipTarget(cfg, capacity); target != "" {
		t.Fatal("stale grant authorized transfer")
	}
}

func TestLicenseCapacityAgreementWaitsForEveryRetainedParticipant(t *testing.T) {
	fsm := raft.NewClusterFSM(zerolog.Nop())
	for _, id := range []string{"w1", "w2"} {
		capacityTestApply(t, fsm, raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: id, Role: "writer", CoreCount: 2}})
	}
	c := &Coordinator{raftFSM: fsm}
	active := license.ExpiryState{Phase: license.PhaseActive, Valid: true}
	paid := lifecycleCapacity{Grant: "paid", MaxMachines: 1, MaxCores: 2, Ready: true}
	for _, reports := range []map[string]lifecycleReport{
		{"w1": {Phase: active, Capacity: paid}},
		{"w1": {Phase: active, Capacity: paid}, "w2": {Phase: active, Capacity: lifecycleCapacity{Grant: "trial", MaxMachines: 1, MaxCores: 2, Ready: true}}},
		{"w1": {Phase: active, Capacity: paid}, "w2": {Phase: active, Capacity: lifecycleCapacity{Grant: "paid", MaxMachines: 1, MaxCores: 2, Ready: false}}},
	} {
		// There is deliberately no proposer: disagreement must return before
		// any leadership action, not reinterpret a missing peer as fenced.
		if err := c.reconcileServingAllocation(raft.LicenseLifecycleState{Mode: "active"}, reports); err != nil {
			t.Fatal(err)
		}
		if fsm.LicenseLifecycle().Generation != 0 {
			t.Fatal("partial agreement changed capacity")
		}
	}
}

func TestLicenseCapacitySharedOwnerRemovalSelectsRemainingWriter(t *testing.T) {
	nodes := []*raft.NodeInfo{{ID: "remaining", Role: "writer", CoreCount: 2}}
	preferred := retainedSharedWriter(nodes, "removed-owner", 2)
	s, err := chooseServingAllocation(nodes, preferred, lifecycleCapacity{Grant: "paid", MaxMachines: 1, MaxCores: 2, Ready: true})
	if err != nil || s.Owner != "remaining" {
		t.Fatalf("removed shared writer prevented capacity recovery: %+v %v", s, err)
	}
	if _, err := chooseServingAllocation(nodes, "removed-owner", lifecycleCapacity{Grant: "paid", MaxCores: 2, Ready: true}); err == nil {
		t.Fatal("local-storage ownership was silently reassigned")
	}
}
