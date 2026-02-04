package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func newTestFSM() *ClusterFSM {
	return NewClusterFSM(zerolog.Nop())
}

func makeCommand(t *testing.T, cmdType CommandType, payload interface{}) []byte {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal payload: %v", err)
	}
	cmd := Command{
		Type:    cmdType,
		Payload: payloadBytes,
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}
	return data
}

func TestFSMAddNode(t *testing.T) {
	fsm := newTestFSM()

	node := NodeInfo{
		ID:          "node-1",
		Name:        "Test Node",
		Role:        "writer",
		ClusterName: "test-cluster",
		Address:     "10.0.0.1:9100",
		APIAddress:  "10.0.0.1:8000",
		State:       "healthy",
		Version:     "1.0.0",
	}

	data := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
	log := &raft.Log{Data: data}

	result := fsm.Apply(log)
	if result != nil {
		t.Errorf("Apply returned error: %v", result)
	}

	// Verify node was added
	got, exists := fsm.GetNode("node-1")
	if !exists {
		t.Fatal("Node should exist after add")
	}
	if got.ID != node.ID {
		t.Errorf("Node ID mismatch: got %s, want %s", got.ID, node.ID)
	}
	if got.Role != node.Role {
		t.Errorf("Node Role mismatch: got %s, want %s", got.Role, node.Role)
	}

	// Verify count
	if count := fsm.NodeCount(); count != 1 {
		t.Errorf("NodeCount() = %d, want 1", count)
	}
}

func TestFSMRemoveNode(t *testing.T) {
	fsm := newTestFSM()

	// First add a node
	node := NodeInfo{ID: "node-1", Name: "Test", Role: "writer"}
	addData := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
	fsm.Apply(&raft.Log{Data: addData})

	// Verify node exists
	if _, exists := fsm.GetNode("node-1"); !exists {
		t.Fatal("Node should exist after add")
	}

	// Remove the node
	removeData := makeCommand(t, CommandRemoveNode, RemoveNodePayload{NodeID: "node-1"})
	result := fsm.Apply(&raft.Log{Data: removeData})
	if result != nil {
		t.Errorf("Apply returned error: %v", result)
	}

	// Verify node was removed
	if _, exists := fsm.GetNode("node-1"); exists {
		t.Error("Node should not exist after remove")
	}

	if count := fsm.NodeCount(); count != 0 {
		t.Errorf("NodeCount() = %d, want 0", count)
	}
}

func TestFSMUpdateNodeState(t *testing.T) {
	fsm := newTestFSM()

	// Add a node
	node := NodeInfo{ID: "node-1", Name: "Test", Role: "writer", State: "healthy"}
	addData := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
	fsm.Apply(&raft.Log{Data: addData})

	// Update state
	updateData := makeCommand(t, CommandUpdateNodeState, UpdateNodeStatePayload{
		NodeID:   "node-1",
		NewState: "unhealthy",
	})
	result := fsm.Apply(&raft.Log{Data: updateData})
	if result != nil {
		t.Errorf("Apply returned error: %v", result)
	}

	// Verify state was updated
	got, _ := fsm.GetNode("node-1")
	if got.State != "unhealthy" {
		t.Errorf("Node state = %s, want unhealthy", got.State)
	}
}

func TestFSMUpdateNodeStateNotFound(t *testing.T) {
	fsm := newTestFSM()

	// Try to update non-existent node
	updateData := makeCommand(t, CommandUpdateNodeState, UpdateNodeStatePayload{
		NodeID:   "non-existent",
		NewState: "unhealthy",
	})
	result := fsm.Apply(&raft.Log{Data: updateData})
	if result == nil {
		t.Error("Apply should return error for non-existent node")
	}
}

func TestFSMGetNodesByRole(t *testing.T) {
	fsm := newTestFSM()

	// Add nodes with different roles
	nodes := []NodeInfo{
		{ID: "writer-1", Name: "Writer 1", Role: "writer"},
		{ID: "writer-2", Name: "Writer 2", Role: "writer"},
		{ID: "reader-1", Name: "Reader 1", Role: "reader"},
		{ID: "compactor-1", Name: "Compactor 1", Role: "compactor"},
	}

	for _, node := range nodes {
		data := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
		fsm.Apply(&raft.Log{Data: data})
	}

	// Test GetNodesByRole
	writers := fsm.GetNodesByRole("writer")
	if len(writers) != 2 {
		t.Errorf("GetNodesByRole(writer) returned %d nodes, want 2", len(writers))
	}

	readers := fsm.GetNodesByRole("reader")
	if len(readers) != 1 {
		t.Errorf("GetNodesByRole(reader) returned %d nodes, want 1", len(readers))
	}

	standalones := fsm.GetNodesByRole("standalone")
	if len(standalones) != 0 {
		t.Errorf("GetNodesByRole(standalone) returned %d nodes, want 0", len(standalones))
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	fsm := newTestFSM()

	// Add some nodes
	nodes := []NodeInfo{
		{ID: "node-1", Name: "Node 1", Role: "writer", State: "healthy"},
		{ID: "node-2", Name: "Node 2", Role: "reader", State: "healthy"},
	}

	for _, node := range nodes {
		data := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
		fsm.Apply(&raft.Log{Data: data})
	}

	// Create snapshot
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() failed: %v", err)
	}

	// Persist snapshot to buffer
	var buf bytes.Buffer
	sink := &testSnapshotSink{Writer: &buf}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist() failed: %v", err)
	}

	// Create new FSM and restore
	fsm2 := newTestFSM()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore() failed: %v", err)
	}

	// Verify restored state
	if fsm2.NodeCount() != 2 {
		t.Errorf("Restored FSM NodeCount() = %d, want 2", fsm2.NodeCount())
	}

	node1, exists := fsm2.GetNode("node-1")
	if !exists {
		t.Fatal("node-1 should exist after restore")
	}
	if node1.Role != "writer" {
		t.Errorf("node-1 role = %s, want writer", node1.Role)
	}
}

func TestFSMCallbacks(t *testing.T) {
	fsm := newTestFSM()

	var addedNode *NodeInfo
	var removedNodeID string
	var updatedNode *NodeInfo

	fsm.SetCallbacks(
		func(n *NodeInfo) { addedNode = n },
		func(id string) { removedNodeID = id },
		func(n *NodeInfo) { updatedNode = n },
	)

	// Test add callback
	node := NodeInfo{ID: "node-1", Name: "Test", Role: "writer"}
	addData := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
	fsm.Apply(&raft.Log{Data: addData})

	if addedNode == nil || addedNode.ID != "node-1" {
		t.Error("Add callback not called correctly")
	}

	// Test update callback
	updateData := makeCommand(t, CommandUpdateNodeState, UpdateNodeStatePayload{
		NodeID:   "node-1",
		NewState: "unhealthy",
	})
	fsm.Apply(&raft.Log{Data: updateData})

	if updatedNode == nil || updatedNode.ID != "node-1" {
		t.Error("Update callback not called correctly")
	}

	// Test remove callback
	removeData := makeCommand(t, CommandRemoveNode, RemoveNodePayload{NodeID: "node-1"})
	fsm.Apply(&raft.Log{Data: removeData})

	if removedNodeID != "node-1" {
		t.Error("Remove callback not called correctly")
	}
}

func TestFSMGetAllNodes(t *testing.T) {
	fsm := newTestFSM()

	// Add nodes
	for i := 1; i <= 3; i++ {
		node := NodeInfo{ID: "node-" + string(rune('0'+i)), Name: "Node", Role: "writer"}
		data := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
		fsm.Apply(&raft.Log{Data: data})
	}

	nodes := fsm.GetAllNodes()
	if len(nodes) != 3 {
		t.Errorf("GetAllNodes() returned %d nodes, want 3", len(nodes))
	}
}

func TestFSMCloneIsolation(t *testing.T) {
	fsm := newTestFSM()

	node := NodeInfo{ID: "node-1", Name: "Test", Role: "writer", State: "healthy"}
	data := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
	fsm.Apply(&raft.Log{Data: data})

	// Get a copy
	got1, _ := fsm.GetNode("node-1")
	got1.State = "modified"

	// Get another copy - should not see modification
	got2, _ := fsm.GetNode("node-1")
	if got2.State != "healthy" {
		t.Error("Modification of returned node affected FSM state")
	}
}

func TestFSMTotalCores(t *testing.T) {
	fsm := newTestFSM()

	// Add nodes with different core counts
	nodes := []NodeInfo{
		{ID: "node-1", Name: "Node 1", Role: "writer", CoreCount: 8},
		{ID: "node-2", Name: "Node 2", Role: "reader", CoreCount: 4},
		{ID: "node-3", Name: "Node 3", Role: "reader", CoreCount: 16},
	}

	for _, node := range nodes {
		data := makeCommand(t, CommandAddNode, AddNodePayload{Node: node})
		fsm.Apply(&raft.Log{Data: data})
	}

	// Total should be 28 (8+4+16)
	if total := fsm.TotalCores(); total != 28 {
		t.Errorf("TotalCores() = %d, want 28", total)
	}
}

func TestFSMTotalCoresAfterRemove(t *testing.T) {
	fsm := newTestFSM()

	// Add nodes
	fsm.Apply(&raft.Log{Data: makeCommand(t, CommandAddNode, AddNodePayload{
		Node: NodeInfo{ID: "node-1", Name: "Node 1", Role: "writer", CoreCount: 8},
	})})
	fsm.Apply(&raft.Log{Data: makeCommand(t, CommandAddNode, AddNodePayload{
		Node: NodeInfo{ID: "node-2", Name: "Node 2", Role: "reader", CoreCount: 4},
	})})

	// Total should be 12
	if total := fsm.TotalCores(); total != 12 {
		t.Errorf("TotalCores() = %d, want 12", total)
	}

	// Remove node-1
	fsm.Apply(&raft.Log{Data: makeCommand(t, CommandRemoveNode, RemoveNodePayload{
		NodeID: "node-1",
	})})

	// Total should now be 4
	if total := fsm.TotalCores(); total != 4 {
		t.Errorf("TotalCores() after remove = %d, want 4", total)
	}
}

func TestFSMTotalCoresEmpty(t *testing.T) {
	fsm := newTestFSM()

	// Empty FSM should have 0 cores
	if total := fsm.TotalCores(); total != 0 {
		t.Errorf("TotalCores() on empty FSM = %d, want 0", total)
	}
}

func TestFSMSnapshotRestoreWithCores(t *testing.T) {
	fsm := newTestFSM()

	// Add node with cores
	node := NodeInfo{ID: "node-1", Name: "Node 1", Role: "writer", CoreCount: 16}
	fsm.Apply(&raft.Log{Data: makeCommand(t, CommandAddNode, AddNodePayload{Node: node})})

	// Verify initial state
	if total := fsm.TotalCores(); total != 16 {
		t.Errorf("Initial TotalCores() = %d, want 16", total)
	}

	// Snapshot and restore
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() failed: %v", err)
	}

	var buf bytes.Buffer
	sink := &testSnapshotSink{Writer: &buf}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist() failed: %v", err)
	}

	fsm2 := newTestFSM()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore() failed: %v", err)
	}

	// Verify cores preserved
	restored, _ := fsm2.GetNode("node-1")
	if restored.CoreCount != 16 {
		t.Errorf("Restored CoreCount = %d, want 16", restored.CoreCount)
	}
	if fsm2.TotalCores() != 16 {
		t.Errorf("Restored TotalCores() = %d, want 16", fsm2.TotalCores())
	}
}

// testSnapshotSink implements raft.SnapshotSink for testing
type testSnapshotSink struct {
	io.Writer
	cancelled bool
}

func (s *testSnapshotSink) ID() string {
	return "test-snapshot"
}

func (s *testSnapshotSink) Cancel() error {
	s.cancelled = true
	return nil
}

func (s *testSnapshotSink) Close() error {
	return nil
}
