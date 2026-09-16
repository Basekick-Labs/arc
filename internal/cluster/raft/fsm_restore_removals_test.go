package raft

// A snapshot restore replaces the node table wholesale. Nodes the snapshot no
// longer carries have to be announced as removed, or every consumer of the
// callbacks keeps listing a node the cluster dropped (#847).

import (
	"bytes"
	"encoding/json"
	"io"
	"sort"
	"testing"

	"github.com/rs/zerolog"
)

func restoreFrom(t *testing.T, fsm *ClusterFSM, snap FSMSnapshot) {
	t.Helper()
	js, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(js))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
}

func TestClusterFSM_Restore_AnnouncesNodesTheSnapshotDropped(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	var added, removed []string
	fsm.SetCallbacks(
		func(n *NodeInfo) { added = append(added, n.ID) },
		func(id string) { removed = append(removed, id) },
		nil,
	)

	// First restore establishes a three-node membership. Nothing existed
	// before it, so nothing is removed.
	restoreFrom(t, fsm, FSMSnapshot{Nodes: map[string]*NodeInfo{
		"w1": {ID: "w1", Role: "writer"},
		"w2": {ID: "w2", Role: "writer"},
		"r1": {ID: "r1", Role: "reader"},
	}})
	if len(removed) != 0 {
		t.Errorf("a first restore announced removals: %v", removed)
	}
	sort.Strings(added)
	if len(added) != 3 {
		t.Fatalf("added = %v; want three nodes", added)
	}

	// A later snapshot has dropped w2.
	added, removed = nil, nil
	restoreFrom(t, fsm, FSMSnapshot{Nodes: map[string]*NodeInfo{
		"w1": {ID: "w1", Role: "writer"},
		"r1": {ID: "r1", Role: "reader"},
	}})
	if len(removed) != 1 || removed[0] != "w2" {
		t.Errorf("removed = %v; want [w2], or a dropped node is listed forever", removed)
	}
	if _, ok := fsm.GetNode("w2"); ok {
		t.Error("w2 is still in the node table")
	}
	sort.Strings(added)
	if len(added) != 2 {
		t.Errorf("added = %v; want the two surviving nodes", added)
	}
}

func TestClusterFSM_Restore_RemovalCallbackIsOptional(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	fsm.SetCallbacks(nil, nil, nil)
	restoreFrom(t, fsm, FSMSnapshot{Nodes: map[string]*NodeInfo{"w1": {ID: "w1", Role: "writer"}}})
	restoreFrom(t, fsm, FSMSnapshot{Nodes: map[string]*NodeInfo{}})
	if _, ok := fsm.GetNode("w1"); ok {
		t.Error("w1 survived a restore that dropped it")
	}
}
