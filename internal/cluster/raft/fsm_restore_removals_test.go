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

func TestClusterFSM_Restore_AnnouncesFilesTheSnapshotDropped(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	type call struct{ path, reason string }
	var calls []call
	fsm.SetFileCallbacks(nil, func(path, reason string) {
		// Reading the FSM from inside the callback proves two things at once:
		// delivery happens after the write lock is released (a held lock would
		// deadlock here), and after the manifest swap (the path is gone).
		if _, exists := fsm.GetFile(path); exists {
			t.Errorf("callback for %q ran before the manifest dropped it", path)
		}
		calls = append(calls, call{path, reason})
	})

	dropped1 := makeFileEntry("db/cpu/2026/04/11/14/zz-file.parquet", "db", "cpu", 1024)
	dropped2 := makeFileEntry("db/cpu/2026/04/11/14/aa-file.parquet", "db", "cpu", 1024)
	keep := makeFileEntry("db/cpu/2026/04/11/14/keep.parquet", "db", "cpu", 1024)
	restoreFrom(t, fsm, FSMSnapshot{Files: map[string]*FileEntry{
		dropped1.Path: &dropped1,
		dropped2.Path: &dropped2,
		keep.Path:     &keep,
	}})
	// A restore into an empty manifest (a process restart loading its own
	// snapshot) has nothing to diff and announces nothing.
	if len(calls) != 0 {
		t.Fatalf("restore into an empty manifest announced %v, want nothing", calls)
	}

	restoreFrom(t, fsm, FSMSnapshot{Files: map[string]*FileEntry{
		keep.Path: &keep,
	}})

	want := []call{
		{dropped2.Path, UnlinkReasonSnapshotRemoved}, // sorted: aa- before zz-
		{dropped1.Path, UnlinkReasonSnapshotRemoved},
	}
	if len(calls) != len(want) {
		t.Fatalf("delete callbacks = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("delete callback %d = %v, want %v", i, calls[i], want[i])
		}
	}
	if _, exists := fsm.GetFile(keep.Path); !exists {
		t.Error("file the snapshot kept is missing from the manifest")
	}
}

func TestClusterFSM_Restore_DroppedFilesWithNoCallbackAreJustDropped(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	file := makeFileEntry("db/cpu/2026/04/11/14/file.parquet", "db", "cpu", 1024)
	restoreFrom(t, fsm, FSMSnapshot{Files: map[string]*FileEntry{file.Path: &file}})
	// No file callbacks wired (replication off, or before the puller starts):
	// the restore must still complete and the manifest must match the snapshot.
	restoreFrom(t, fsm, FSMSnapshot{})
	if _, exists := fsm.GetFile(file.Path); exists {
		t.Error("file dropped by snapshot is still in the manifest")
	}
}
