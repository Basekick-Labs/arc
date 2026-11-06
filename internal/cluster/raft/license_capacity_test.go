package raft

import (
	"bytes"
	"io"
	"testing"

	hraft "github.com/hashicorp/raft"
)

func TestLicenseCapacityFSMRejectsInvalidAllocationsAndPromotions(t *testing.T) {
	f := newTestFSM()
	apply := func(kind CommandType, p interface{}) interface{} {
		return f.Apply(&hraft.Log{Data: makeCommand(t, kind, p)})
	}
	for _, n := range []NodeInfo{{ID: "w1", Role: "writer", CoreCount: 4}, {ID: "w2", Role: "writer", CoreCount: 4}, {ID: "r1", Role: "reader", CoreCount: 2}} {
		if err := apply(CommandAddNode, AddNodePayload{Node: n}); err != nil {
			t.Fatal(err)
		}
	}
	if err := apply(CommandPromoteWriter, PromoteWriterPayload{NodeID: "w1"}); err != nil {
		t.Fatal(err)
	}
	s := LicenseLifecycleState{Generation: 1, Mode: "active", Controller: "w1", Owner: "w1", Participants: []string{"w1", "w2", "r1"}, Allocation: &LicenseServingAllocation{Grant: "paid", Topology: "topology", MaxMachines: 2, MaxCores: 6, ServingIDs: []string{"w1", "r1"}}}
	for _, ids := range [][]string{{"w1", "w2"}, {"w1", "r1", "w2"}, {"w1", "w1"}, {"w1", "absent"}, {"w2"}} {
		bad := cloneLicenseLifecycle(s)
		bad.Allocation.ServingIDs = ids
		if err := apply(CommandLicenseLifecycle, bad); err == nil {
			t.Fatalf("accepted invalid serving IDs: %v", ids)
		}
	}
	if err := apply(CommandLicenseLifecycle, s); err != nil {
		t.Fatal(err)
	}
	if f.NodeCount() != 3 {
		t.Fatal("allocation deleted retained topology")
	}
	if err := apply(CommandPromoteWriter, PromoteWriterPayload{NodeID: "w2"}); err == nil {
		t.Fatal("promoted excess writer")
	}
	if err := apply(CommandAssignCompactor, AssignCompactorPayload{NodeID: "w2"}); err == nil {
		t.Fatal("assigned excess compactor")
	}
	if f.GetPrimaryWriterID() != "w1" {
		t.Fatal("rejected promotion changed primary")
	}
	got := f.LicenseLifecycle()
	got.Allocation.ServingIDs[0] = "w2"
	if f.LicenseLifecycle().Allocation.ServingIDs[0] != "w1" {
		t.Fatal("allocation getter leaked mutable snapshot state")
	}
	if err := apply(CommandAssignCompactor, AssignCompactorPayload{NodeID: "w1"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	var buf bytes.Buffer
	if err := snapshot.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	restored := newTestFSM()
	if err := restored.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if restored.NodeCount() != 3 || restored.LicenseLifecycle().Allocation.ServingIDs[0] != "w1" {
		t.Fatal("snapshot restore lost allocation or retained topology")
	}
}
