package raft

// Snapshot restore delivers the restored membership to the AddNode callback
// (#807), refuses nil node entries, and tolerates a snapshot with no nodes.

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func restoreJSON(t *testing.T, snap FSMSnapshot) io.ReadCloser {
	t.Helper()
	js, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	return io.NopCloser(bytes.NewReader(js))
}

func TestClusterFSM_Restore_FiresNodeAddedForRestoredNodes(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	var delivered []*NodeInfo
	fsm.SetCallbacks(func(n *NodeInfo) { delivered = append(delivered, n) }, nil, nil)

	snap := FSMSnapshot{
		Nodes: map[string]*NodeInfo{
			"w1": {ID: "w1", Role: "writer", Address: "10.0.0.1:9100", State: "healthy", WriterState: "primary"},
			"w2": {ID: "w2", Role: "writer", Address: "10.0.0.2:9100", State: "healthy", WriterState: "standby"},
			"r1": {ID: "r1", Role: "reader", Address: "10.0.0.3:9100", State: "healthy"},
		},
		PrimaryWriterID: "w1",
	}
	if err := fsm.Restore(restoreJSON(t, snap)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if len(delivered) != 3 {
		t.Fatalf("callback delivered %d nodes, want 3 (before #807: 0)", len(delivered))
	}
	seen := map[string]*NodeInfo{}
	for _, n := range delivered {
		seen[n.ID] = n
	}
	for _, id := range []string{"w1", "w2", "r1"} {
		if seen[id] == nil {
			t.Fatalf("node %s not delivered", id)
		}
	}
	if seen["w1"].WriterState != "primary" || seen["w1"].Address != "10.0.0.1:9100" {
		t.Fatalf("delivered w1 = %+v; fields lost", seen["w1"])
	}
	// The callback receives a copy: mutating it must not alter the table.
	seen["w1"].Role = "reader"
	if got, _ := fsm.GetNode("w1"); got.Role != "writer" {
		t.Fatalf("callback mutation reached the node table: role = %q", got.Role)
	}
}

func TestClusterFSM_Restore_RefusesNilNodeEntries(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	var delivered []string
	fsm.SetCallbacks(func(n *NodeInfo) { delivered = append(delivered, n.ID) }, nil, nil)

	// A hand-built payload so the null value survives marshalling.
	js := []byte(`{"nodes":{"good":{"id":"good","role":"writer","address":"10.0.0.1:9100"},"bad":null}}`)
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(js))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if len(delivered) != 1 || delivered[0] != "good" {
		t.Fatalf("delivered = %v, want [good]", delivered)
	}
	if _, ok := fsm.GetNode("bad"); ok {
		t.Fatal("nil entry was kept in the node table")
	}
	if len(fsm.GetAllNodes()) != 1 {
		t.Fatalf("node table has %d entries, want 1", len(fsm.GetAllNodes()))
	}
}

func TestClusterFSM_Restore_NoNodesKeyLeavesAWritableTable(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	if err := fsm.Restore(io.NopCloser(bytes.NewReader([]byte(`{}`)))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	payload, _ := json.Marshal(AddNodePayload{Node: NodeInfo{ID: "n1", Role: "writer"}})
	data, _ := json.Marshal(Command{Type: CommandAddNode, Payload: payload})
	if res := fsm.Apply(&hraft.Log{Index: 1, Data: data}); res != nil {
		t.Fatalf("AddNode after a nodeless restore: %v (nil node table)", res)
	}
	if _, ok := fsm.GetNode("n1"); !ok {
		t.Fatal("n1 missing after AddNode")
	}
}

func TestClusterFSM_Restore_NilCallbackIsFine(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	snap := FSMSnapshot{Nodes: map[string]*NodeInfo{"w1": {ID: "w1", Role: "writer"}}}
	if err := fsm.Restore(restoreJSON(t, snap)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := fsm.GetNode("w1"); !ok {
		t.Fatal("w1 missing after restore")
	}
}
