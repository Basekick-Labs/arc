package raft

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func applyCmd(t *testing.T, f *ClusterFSM, idx uint64, typ CommandType, payload any) interface{} {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(Command{Type: typ, Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	return f.Apply(&raft.Log{Index: idx, Data: data})
}

func claimsFSM(t *testing.T, nodes ...string) *ClusterFSM {
	t.Helper()
	f := NewClusterFSM(zerolog.Nop())
	for i, n := range nodes {
		if res := applyCmd(t, f, uint64(i+1), CommandAddNode, AddNodePayload{Node: NodeInfo{ID: n, Role: "writer"}}); res != nil {
			t.Fatalf("add %s: %v", n, res)
		}
	}
	return f
}

var t0 = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

func TestEnableStreamClaimsNeedsEveryNode(t *testing.T) {
	f := claimsFSM(t, "w1", "w2", "w3")
	if res := applyCmd(t, f, 10, CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: []string{"w1", "w2"}, Now: t0}); res == nil {
		t.Fatal("enabled with w3 unaccounted for")
	}
	if f.StreamClaimsEnabled() {
		t.Fatal("flag on after a refused enable")
	}
	if res := applyCmd(t, f, 11, CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: []string{"w1", "w2", "w3"}, Now: t0}); res != nil {
		t.Fatalf("enable: %v", res)
	}
	if !f.StreamClaimsEnabled() {
		t.Fatal("flag off after enable")
	}
}

// The capabilities live on the node record, replaced on every join: a node
// that re-joins without them (rolled back) loses them.
func TestCapabilitiesAreReplacedOnEveryJoin(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	applyCmd(t, f, 1, CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w1", Role: "writer", Capabilities: []string{CapabilityStreamClaims}}})
	if n, _ := f.GetNode("w1"); len(n.Capabilities) != 1 {
		t.Fatal("capability not recorded")
	}
	applyCmd(t, f, 2, CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w1", Role: "writer"}})
	if n, _ := f.GetNode("w1"); len(n.Capabilities) != 0 {
		t.Fatal("a re-join without the capability kept it")
	}
}
