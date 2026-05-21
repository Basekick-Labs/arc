package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

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

// File manifest tests (peer replication, Phase 1)

func makeFileEntry(path, db, measurement string, size int64) FileEntry {
	// Fixed timestamps — FSM Apply requires CreatedAt to be set by the caller
	// (never stamped inside Apply because that would be non-deterministic
	// during log replay on different nodes).
	return FileEntry{
		Path:          path,
		SHA256:        "",
		SizeBytes:     size,
		Database:      db,
		Measurement:   measurement,
		PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		OriginNodeID:  "writer-1",
		Tier:          "hot",
		CreatedAt:     time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC),
	}
}

func TestFSMRegisterFile(t *testing.T) {
	fsm := newTestFSM()

	file := makeFileEntry("db/cpu/2026/04/11/14/file.parquet", "db", "cpu", 1024)
	data := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: file})

	// Simulate a Raft log entry with an Index (LSN)
	log := &raft.Log{Index: 42, Data: data}
	if result := fsm.Apply(log); result != nil {
		t.Fatalf("Apply returned error: %v", result)
	}

	got, exists := fsm.GetFile(file.Path)
	if !exists {
		t.Fatal("File should exist after register")
	}
	if got.Path != file.Path {
		t.Errorf("Path mismatch: got %s, want %s", got.Path, file.Path)
	}
	if got.Database != "db" || got.Measurement != "cpu" {
		t.Errorf("db/measurement mismatch: got %s/%s", got.Database, got.Measurement)
	}
	if got.LSN != 42 {
		t.Errorf("LSN should be stamped from log.Index: got %d, want 42", got.LSN)
	}
	if !got.CreatedAt.Equal(file.CreatedAt) {
		t.Errorf("CreatedAt should match input: got %v, want %v", got.CreatedAt, file.CreatedAt)
	}

	if count := fsm.FileCount(); count != 1 {
		t.Errorf("FileCount() = %d, want 1", count)
	}
}

// TestFSMRegisterFileRejectsZeroCreatedAt verifies that the FSM rejects
// register commands without CreatedAt — preventing non-deterministic
// time.Now() stamping inside Apply that would diverge state across nodes.
func TestFSMRegisterFileRejectsZeroCreatedAt(t *testing.T) {
	fsm := newTestFSM()

	file := FileEntry{
		Path:         "db/cpu/file.parquet",
		Database:     "db",
		Measurement:  "cpu",
		SizeBytes:    100,
		OriginNodeID: "writer-1",
		Tier:         "hot",
		// CreatedAt intentionally left zero
	}
	data := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: file})

	result := fsm.Apply(&raft.Log{Index: 1, Data: data})
	if result == nil {
		t.Fatal("Expected error for zero CreatedAt, got nil")
	}
	err, ok := result.(error)
	if !ok {
		t.Fatalf("Expected error result, got %T: %v", result, result)
	}
	if err.Error() == "" {
		t.Error("Expected non-empty error message")
	}

	if fsm.FileCount() != 0 {
		t.Error("File should not be registered when CreatedAt is zero")
	}
}

func TestFSMDeleteFile(t *testing.T) {
	fsm := newTestFSM()

	file := makeFileEntry("db/cpu/2026/04/11/14/file.parquet", "db", "cpu", 1024)
	regData := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: file})
	fsm.Apply(&raft.Log{Index: 1, Data: regData})

	delData := makeCommand(t, CommandDeleteFile, DeleteFilePayload{Path: file.Path, Reason: "retention"})
	if result := fsm.Apply(&raft.Log{Index: 2, Data: delData}); result != nil {
		t.Fatalf("Apply returned error: %v", result)
	}

	if _, exists := fsm.GetFile(file.Path); exists {
		t.Error("File should not exist after delete")
	}
	if count := fsm.FileCount(); count != 0 {
		t.Errorf("FileCount() = %d, want 0", count)
	}
}

func TestFSMDeleteNonexistentFile(t *testing.T) {
	fsm := newTestFSM()

	// Deleting a file that was never registered should be a no-op (idempotent)
	delData := makeCommand(t, CommandDeleteFile, DeleteFilePayload{Path: "db/cpu/ghost.parquet"})
	result := fsm.Apply(&raft.Log{Index: 1, Data: delData})
	if result != nil {
		t.Errorf("Delete of non-existent file should not error, got %v", result)
	}
}

func TestFSMGetFilesByDatabase(t *testing.T) {
	fsm := newTestFSM()

	files := []FileEntry{
		makeFileEntry("prod/cpu/file1.parquet", "prod", "cpu", 100),
		makeFileEntry("prod/memory/file2.parquet", "prod", "memory", 200),
		makeFileEntry("staging/cpu/file3.parquet", "staging", "cpu", 300),
	}

	for i, f := range files {
		data := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: f})
		fsm.Apply(&raft.Log{Index: uint64(i + 1), Data: data})
	}

	prodFiles := fsm.GetFilesByDatabase("prod")
	if len(prodFiles) != 2 {
		t.Errorf("GetFilesByDatabase(prod) = %d files, want 2", len(prodFiles))
	}

	stagingFiles := fsm.GetFilesByDatabase("staging")
	if len(stagingFiles) != 1 {
		t.Errorf("GetFilesByDatabase(staging) = %d files, want 1", len(stagingFiles))
	}

	emptyFiles := fsm.GetFilesByDatabase("nonexistent")
	if len(emptyFiles) != 0 {
		t.Errorf("GetFilesByDatabase(nonexistent) = %d files, want 0", len(emptyFiles))
	}
}

func TestFSMFileCallbacks(t *testing.T) {
	fsm := newTestFSM()

	var registeredFile *FileEntry
	var deletedPath, deletedReason string

	fsm.SetFileCallbacks(
		func(f *FileEntry) { registeredFile = f },
		func(path, reason string) { deletedPath = path; deletedReason = reason },
	)

	file := makeFileEntry("db/cpu/file.parquet", "db", "cpu", 512)
	regData := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: file})
	fsm.Apply(&raft.Log{Index: 10, Data: regData})

	if registeredFile == nil {
		t.Fatal("onFileRegistered callback should have fired")
	}
	if registeredFile.Path != file.Path {
		t.Errorf("callback got path %s, want %s", registeredFile.Path, file.Path)
	}
	if registeredFile.LSN != 10 {
		t.Errorf("callback got LSN %d, want 10", registeredFile.LSN)
	}

	delData := makeCommand(t, CommandDeleteFile, DeleteFilePayload{Path: file.Path, Reason: "compaction"})
	fsm.Apply(&raft.Log{Index: 11, Data: delData})

	if deletedPath != file.Path {
		t.Errorf("onFileDeleted callback got path %s, want %s", deletedPath, file.Path)
	}
	if deletedReason != "compaction" {
		t.Errorf("onFileDeleted callback got reason %s, want compaction", deletedReason)
	}
}

// TestFSMFilesByDatabaseIndexConsistency verifies that the filesByDB
// secondary index stays in sync with the primary files map across register,
// delete, and snapshot/restore operations.
func TestFSMFilesByDatabaseIndexConsistency(t *testing.T) {
	fsm := newTestFSM()

	// Register 3 files across 2 databases
	files := []FileEntry{
		makeFileEntry("prod/cpu/f1.parquet", "prod", "cpu", 100),
		makeFileEntry("prod/mem/f2.parquet", "prod", "mem", 200),
		makeFileEntry("staging/cpu/f3.parquet", "staging", "cpu", 300),
	}
	for i, f := range files {
		data := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: f})
		fsm.Apply(&raft.Log{Index: uint64(i + 1), Data: data})
	}

	// After register: prod=2, staging=1
	if got := len(fsm.GetFilesByDatabase("prod")); got != 2 {
		t.Errorf("After register: GetFilesByDatabase(prod) = %d, want 2", got)
	}
	if got := len(fsm.GetFilesByDatabase("staging")); got != 1 {
		t.Errorf("After register: GetFilesByDatabase(staging) = %d, want 1", got)
	}

	// Delete one prod file → prod=1, staging=1
	delData := makeCommand(t, CommandDeleteFile, DeleteFilePayload{Path: "prod/cpu/f1.parquet"})
	fsm.Apply(&raft.Log{Index: 10, Data: delData})

	if got := len(fsm.GetFilesByDatabase("prod")); got != 1 {
		t.Errorf("After delete one prod: GetFilesByDatabase(prod) = %d, want 1", got)
	}

	// Delete the remaining prod file → prod should be empty (nil or 0), staging=1
	delData2 := makeCommand(t, CommandDeleteFile, DeleteFilePayload{Path: "prod/mem/f2.parquet"})
	fsm.Apply(&raft.Log{Index: 11, Data: delData2})

	if got := len(fsm.GetFilesByDatabase("prod")); got != 0 {
		t.Errorf("After delete all prod: GetFilesByDatabase(prod) = %d, want 0", got)
	}
	// Internal consistency: the database bucket should be removed from filesByDB
	// when its last file is deleted, to prevent unbounded growth.
	fsm.mu.RLock()
	_, prodIdxExists := fsm.filesByDB["prod"]
	fsm.mu.RUnlock()
	if prodIdxExists {
		t.Error("filesByDB[prod] should be removed after last file deleted (prevents index bloat)")
	}

	// Snapshot + restore into a fresh FSM, then verify the index is rebuilt correctly
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	var buf bytes.Buffer
	if err := snapshot.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	fsm2 := newTestFSM()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// After restore, the index should be rebuilt and staging=1 should work
	if got := len(fsm2.GetFilesByDatabase("staging")); got != 1 {
		t.Errorf("After restore: GetFilesByDatabase(staging) = %d, want 1", got)
	}
	if got := len(fsm2.GetFilesByDatabase("prod")); got != 0 {
		t.Errorf("After restore: GetFilesByDatabase(prod) = %d, want 0", got)
	}

	// Verify internal index state matches the primary files map
	fsm2.mu.RLock()
	totalIndexed := 0
	for _, idx := range fsm2.filesByDB {
		totalIndexed += len(idx)
	}
	fileCount := len(fsm2.files)
	fsm2.mu.RUnlock()
	if totalIndexed != fileCount {
		t.Errorf("Index inconsistency after restore: totalIndexed=%d, fileCount=%d", totalIndexed, fileCount)
	}
}

func TestFSMSnapshotRestoreWithFiles(t *testing.T) {
	fsm := newTestFSM()

	// Add a node and some files
	node := NodeInfo{ID: "node-1", Role: "writer", State: "healthy"}
	fsm.Apply(&raft.Log{Index: 1, Data: makeCommand(t, CommandAddNode, AddNodePayload{Node: node})})

	files := []FileEntry{
		makeFileEntry("prod/cpu/file1.parquet", "prod", "cpu", 100),
		makeFileEntry("prod/mem/file2.parquet", "prod", "mem", 200),
	}
	for i, f := range files {
		data := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: f})
		fsm.Apply(&raft.Log{Index: uint64(i + 2), Data: data})
	}

	// Snapshot
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() failed: %v", err)
	}

	var buf bytes.Buffer
	sink := &testSnapshotSink{Writer: &buf}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist() failed: %v", err)
	}

	// Restore into a new FSM
	fsm2 := newTestFSM()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore() failed: %v", err)
	}

	if fsm2.NodeCount() != 1 {
		t.Errorf("Restored NodeCount() = %d, want 1", fsm2.NodeCount())
	}
	if fsm2.FileCount() != 2 {
		t.Errorf("Restored FileCount() = %d, want 2", fsm2.FileCount())
	}

	if _, exists := fsm2.GetFile("prod/cpu/file1.parquet"); !exists {
		t.Error("file1 should exist after restore")
	}
	if _, exists := fsm2.GetFile("prod/mem/file2.parquet"); !exists {
		t.Error("file2 should exist after restore")
	}
}

// --- CommandBatchFileOps ---

// makeBatchCommand builds a Raft log data blob for a CommandBatchFileOps.
func makeBatchCommand(t *testing.T, ops []BatchFileOp) []byte {
	t.Helper()
	return makeCommand(t, CommandBatchFileOps, BatchFileOpsPayload{Ops: ops})
}

// makeBatchOp is a helper that marshals a typed payload into a BatchFileOp.
func makeBatchOp(t *testing.T, typ CommandType, payload interface{}) BatchFileOp {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("makeBatchOp marshal: %v", err)
	}
	return BatchFileOp{Type: typ, Payload: b}
}

// TestFSMBatchFileOps applies a batch with 2 registers + 2 deletes and
// verifies correct FSM state, LSN stamping, and idempotency.
func TestFSMBatchFileOps(t *testing.T) {
	fsm := newTestFSM()

	// Pre-register the two files that will be deleted in the batch.
	old1 := makeFileEntry("db/cpu/old1.parquet", "db", "cpu", 100)
	old2 := makeFileEntry("db/cpu/old2.parquet", "db", "cpu", 200)
	for i, f := range []FileEntry{old1, old2} {
		fsm.Apply(&raft.Log{Index: uint64(i + 1), Data: makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: f})})
	}

	new1 := makeFileEntry("db/cpu/new1.parquet", "db", "cpu", 300)
	new2 := makeFileEntry("db/cpu/new2.parquet", "db", "cpu", 400)

	ops := []BatchFileOp{
		makeBatchOp(t, CommandRegisterFile, RegisterFilePayload{File: new1}),
		makeBatchOp(t, CommandRegisterFile, RegisterFilePayload{File: new2}),
		makeBatchOp(t, CommandDeleteFile, DeleteFilePayload{Path: old1.Path, Reason: "compaction"}),
		makeBatchOp(t, CommandDeleteFile, DeleteFilePayload{Path: old2.Path, Reason: "compaction"}),
	}

	const batchIndex = uint64(10)
	result := fsm.Apply(&raft.Log{Index: batchIndex, Data: makeBatchCommand(t, ops)})
	if result != nil {
		t.Fatalf("Apply batch returned error: %v", result)
	}

	// New files must be registered.
	for _, path := range []string{new1.Path, new2.Path} {
		got, exists := fsm.GetFile(path)
		if !exists {
			t.Fatalf("file %s should exist after batch register", path)
		}
		if got.LSN != batchIndex {
			t.Errorf("%s LSN: got %d, want %d (batch log index)", path, got.LSN, batchIndex)
		}
	}

	// Old files must be deleted.
	for _, path := range []string{old1.Path, old2.Path} {
		if _, exists := fsm.GetFile(path); exists {
			t.Errorf("file %s should be deleted after batch", path)
		}
	}

	if count := fsm.FileCount(); count != 2 {
		t.Errorf("FileCount() = %d, want 2", count)
	}

	// Idempotency: re-applying the same batch must not error.
	result = fsm.Apply(&raft.Log{Index: batchIndex, Data: makeBatchCommand(t, ops)})
	if result != nil {
		t.Errorf("Re-applying batch should be idempotent, got error: %v", result)
	}
}

// TestFSMBatchFileOpsRegistersOnly applies a batch containing only register
// operations and verifies they are all committed.
func TestFSMBatchFileOpsRegistersOnly(t *testing.T) {
	fsm := newTestFSM()

	files := []FileEntry{
		makeFileEntry("db/m/a.parquet", "db", "m", 10),
		makeFileEntry("db/m/b.parquet", "db", "m", 20),
	}
	ops := make([]BatchFileOp, len(files))
	for i, f := range files {
		ops[i] = makeBatchOp(t, CommandRegisterFile, RegisterFilePayload{File: f})
	}

	result := fsm.Apply(&raft.Log{Index: 5, Data: makeBatchCommand(t, ops)})
	if result != nil {
		t.Fatalf("Apply returned error: %v", result)
	}
	if count := fsm.FileCount(); count != 2 {
		t.Errorf("FileCount() = %d, want 2", count)
	}
}

// TestFSMBatchFileOpsUnknownOpType verifies that a batch containing an
// unsupported op type returns an error.
func TestFSMBatchFileOpsUnknownOpType(t *testing.T) {
	fsm := newTestFSM()

	ops := []BatchFileOp{
		// CommandAddNode is not a valid file-manifest op type.
		makeBatchOp(t, CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "n1"}}),
	}

	result := fsm.Apply(&raft.Log{Index: 1, Data: makeBatchCommand(t, ops)})
	if result == nil {
		t.Fatal("expected error for unsupported op type, got nil")
	}
	if _, ok := result.(error); !ok {
		t.Fatalf("expected error result, got %T: %v", result, result)
	}
}

// TestFSMBatchFileOpsEmpty verifies that a batch with zero ops is a no-op.
func TestFSMBatchFileOpsEmpty(t *testing.T) {
	fsm := newTestFSM()

	result := fsm.Apply(&raft.Log{Index: 1, Data: makeBatchCommand(t, []BatchFileOp{})})
	if result != nil {
		t.Errorf("empty batch should return nil, got %v", result)
	}
	if count := fsm.FileCount(); count != 0 {
		t.Errorf("FileCount() = %d, want 0", count)
	}
}

// TestApplyRegisterFile_RejectsMaliciousPath pins the security
// property of GHSA-f85q-mvg8-qf37 across every malicious path shape:
// Apply returns an error, the entry does NOT land in f.files, and
// the rejectedPaths counter increments. The error is delivered to
// the proposer via future.Response(); during log replay it's
// silently swallowed by hashicorp/raft because req.future == nil,
// but the security property (entry not in f.files) and visibility
// (counter + Error log line) both hold.
func TestApplyRegisterFile_RejectsMaliciousPath(t *testing.T) {
	t.Parallel()
	maliciousPaths := []string{
		"/etc/passwd",
		"s3://attacker-bucket/poisoned.parquet",
		"../../etc/shadow",
		"db\x00/etc/passwd",
		"file:///etc/passwd",
	}
	for _, mp := range maliciousPaths {
		t.Run(mp, func(t *testing.T) {
			fsm := newTestFSM()
			file := makeFileEntry(mp, "db", "cpu", 100)
			data := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: file})

			result := fsm.Apply(&raft.Log{Index: 1, Data: data})
			if result == nil {
				t.Fatalf("malicious path %q: Apply should have returned error", mp)
			}
			if _, ok := result.(error); !ok {
				t.Fatalf("malicious path %q: expected error result, got %T", mp, result)
			}
			if fsm.FileCount() != 0 {
				t.Errorf("malicious path %q: should not have landed in files (count=%d)", mp, fsm.FileCount())
			}
			if c := fsm.RejectedPathsCount(); c != 1 {
				t.Errorf("malicious path %q: rejectedPaths should be 1, got %d", mp, c)
			}
		})
	}
}

// TestApplyRegisterFile_MixedBatchIsolatesMaliciousEntries verifies
// that when a stream of Apply calls contains both legitimate and
// malicious entries, only the legitimate ones land in f.files and the
// counter reflects all rejections. Simulates the "leader applies a
// flush from a writer that has a malicious peer also proposing to
// the same FSM" shape (which doesn't happen in practice but pins the
// per-Apply isolation invariant).
func TestApplyRegisterFile_MixedBatchIsolatesMaliciousEntries(t *testing.T) {
	t.Parallel()
	fsm := newTestFSM()

	entries := []FileEntry{
		makeFileEntry("/etc/passwd", "db", "cpu", 1),
		makeFileEntry("db/cpu/2026/05/20/15/legit1.parquet", "db", "cpu", 100),
		makeFileEntry("s3://attacker/poisoned.parquet", "db", "cpu", 1),
		makeFileEntry("db/cpu/2026/05/20/15/legit2.parquet", "db", "cpu", 100),
	}
	for i, file := range entries {
		data := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: file})
		fsm.Apply(&raft.Log{Index: uint64(i + 1), Data: data})
	}

	if fsm.FileCount() != 2 {
		t.Errorf("expected 2 legit entries to land, got count=%d", fsm.FileCount())
	}
	if c := fsm.RejectedPathsCount(); c != 2 {
		t.Errorf("expected 2 rejected entries, got %d", c)
	}
	for _, malicious := range []string{"/etc/passwd", "s3://attacker/poisoned.parquet"} {
		if _, exists := fsm.GetFile(malicious); exists {
			t.Errorf("malicious entry %q must not be in f.files", malicious)
		}
	}
	for _, legit := range []string{
		"db/cpu/2026/05/20/15/legit1.parquet",
		"db/cpu/2026/05/20/15/legit2.parquet",
	} {
		if _, exists := fsm.GetFile(legit); !exists {
			t.Errorf("legit entry %q must be in f.files", legit)
		}
	}
}

// TestApplyUpdateFile_RejectsMaliciousPath pins that the same
// validation guards apply to update commands. Without this, an
// attacker who can submit Update could rewrite an existing entry's
// path to a malicious value.
func TestApplyUpdateFile_RejectsMaliciousPath(t *testing.T) {
	t.Parallel()
	fsm := newTestFSM()

	// Register a legit entry first so there's something to update.
	legit := makeFileEntry("db/cpu/2026/05/20/15/file.parquet", "db", "cpu", 100)
	regData := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: legit})
	if result := fsm.Apply(&raft.Log{Index: 1, Data: regData}); result != nil {
		t.Fatalf("legit register failed: %v", result)
	}

	// Attempt to "update" with a malicious path.
	bad := makeFileEntry("../../etc/shadow", "db", "cpu", 100)
	updateData := makeCommand(t, CommandUpdateFile, UpdateFilePayload{File: bad})
	result := fsm.Apply(&raft.Log{Index: 2, Data: updateData})
	if result == nil {
		t.Fatal("update with malicious path should error")
	}
	if _, ok := result.(error); !ok {
		t.Fatalf("expected error result, got %T", result)
	}

	// Original entry still present + unchanged.
	if _, exists := fsm.GetFile(legit.Path); !exists {
		t.Error("legitimate entry should be untouched by rejected update")
	}
	if _, exists := fsm.GetFile(bad.Path); exists {
		t.Error("malicious path should not appear in files")
	}
}

// TestApplyUpdateFile_BumpsLSN pins that applyUpdateFile stamps the
// current Raft log index into the entry's LSN, matching the contract
// applyRegisterFile already satisfies. Without this, an Update that
// mutates an existing entry would leave the LSN at its
// registration-time value, and downstream consumers that watch LSN
// for "did this entry change" (e.g. compaction watchers) couldn't
// distinguish a fresh state from a stale one. Gemini #446-r1 #1.
func TestApplyUpdateFile_BumpsLSN(t *testing.T) {
	t.Parallel()
	fsm := newTestFSM()

	// Register at log index 10 → LSN should be 10.
	legit := makeFileEntry("db/cpu/2026/05/20/15/file.parquet", "db", "cpu", 100)
	regData := makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: legit})
	if result := fsm.Apply(&raft.Log{Index: 10, Data: regData}); result != nil {
		t.Fatalf("legit register failed: %v", result)
	}
	got, exists := fsm.GetFile(legit.Path)
	if !exists {
		t.Fatal("entry should exist after register")
	}
	if got.LSN != 10 {
		t.Fatalf("post-register LSN: got %d, want 10", got.LSN)
	}

	// Update at log index 42 with same path but different size.
	updated := legit
	updated.SizeBytes = 999
	updateData := makeCommand(t, CommandUpdateFile, UpdateFilePayload{File: updated})
	if result := fsm.Apply(&raft.Log{Index: 42, Data: updateData}); result != nil {
		t.Fatalf("legit update failed: %v", result)
	}

	got, exists = fsm.GetFile(legit.Path)
	if !exists {
		t.Fatal("entry should still exist after update")
	}
	if got.LSN != 42 {
		t.Errorf("post-update LSN: got %d, want 42 (Update must stamp LSN from log index, not preserve registration-time value)", got.LSN)
	}
	if got.SizeBytes != 999 {
		t.Errorf("post-update SizeBytes: got %d, want 999 (update should land)", got.SizeBytes)
	}
}

// TestFSMRestore_QuarantinesMaliciousSnapshotEntries pins the
// snapshot-restore quarantine path. A snapshot containing 3 legit +
// 2 malicious entries must round-trip with only the 3 legit entries
// in f.files, and the quarantine counter must reflect both
// rejections.
func TestFSMRestore_QuarantinesMaliciousSnapshotEntries(t *testing.T) {
	t.Parallel()
	// Build a snapshot manually (not via Snapshot/Persist) so we can
	// inject malicious entries that Snapshot() would never produce
	// from our own state (Snapshot draws from f.files, which is
	// validation-protected at write time after this PR).
	snapshot := FSMSnapshot{
		Nodes:           map[string]*NodeInfo{},
		PrimaryWriterID: "",
		Files: map[string]*FileEntry{
			"db/cpu/2026/05/20/15/legit1.parquet":   {Path: "db/cpu/2026/05/20/15/legit1.parquet", Database: "db", Measurement: "cpu", CreatedAt: time.Now()},
			"db/cpu/2026/05/20/15/legit2.parquet":   {Path: "db/cpu/2026/05/20/15/legit2.parquet", Database: "db", Measurement: "cpu", CreatedAt: time.Now()},
			"db/cpu/2026/05/20/15/legit3.parquet":   {Path: "db/cpu/2026/05/20/15/legit3.parquet", Database: "db", Measurement: "cpu", CreatedAt: time.Now()},
			"/etc/passwd":                           {Path: "/etc/passwd", Database: "db", Measurement: "cpu", CreatedAt: time.Now()},
			"s3://attacker-bucket/poisoned.parquet": {Path: "s3://attacker-bucket/poisoned.parquet", Database: "db", Measurement: "cpu", CreatedAt: time.Now()},
		},
	}
	snapshotBytes, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	fsm := newTestFSM()
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(snapshotBytes))); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if fsm.FileCount() != 3 {
		t.Errorf("expected 3 legit entries restored, got %d", fsm.FileCount())
	}
	if c := fsm.RejectedPathsCount(); c != 2 {
		t.Errorf("expected 2 rejected entries, got %d", c)
	}
	if _, exists := fsm.GetFile("/etc/passwd"); exists {
		t.Error("/etc/passwd should not have been restored")
	}
	if _, exists := fsm.GetFile("s3://attacker-bucket/poisoned.parquet"); exists {
		t.Error("s3:// scheme path should not have been restored")
	}
	for _, legit := range []string{
		"db/cpu/2026/05/20/15/legit1.parquet",
		"db/cpu/2026/05/20/15/legit2.parquet",
		"db/cpu/2026/05/20/15/legit3.parquet",
	} {
		if _, exists := fsm.GetFile(legit); !exists {
			t.Errorf("legit entry %q should be restored", legit)
		}
	}
}

// TestApplyBatchFileOps_RejectsMaliciousPath_AtomicPrevalidation pins
// the atomicity invariant: when a batch contains a malicious entry —
// even mid-batch — NO ops from the batch have side-effects. The
// pre-validation pass walks every Register/Update path before any
// applyXxx is called, so the legit op BEFORE the malicious one does
// NOT land. This closes the High finding from internal review:
// without pre-validation, the leader's f.files would diverge from
// "the malicious entry never replicates" claim in the release notes.
func TestApplyBatchFileOps_RejectsMaliciousPath_AtomicPrevalidation(t *testing.T) {
	t.Parallel()
	fsm := newTestFSM()

	// Order matters: legit op FIRST so a non-atomic implementation
	// would have applied it before reaching the malicious op.
	legitFile := makeFileEntry("db/cpu/2026/05/20/15/legit.parquet", "db", "cpu", 100)
	maliciousFile := makeFileEntry("../../etc/shadow", "db", "cpu", 100)

	legitPayload, _ := json.Marshal(RegisterFilePayload{File: legitFile})
	maliciousPayload, _ := json.Marshal(RegisterFilePayload{File: maliciousFile})

	batchData := makeCommand(t, CommandBatchFileOps, BatchFileOpsPayload{
		Ops: []BatchFileOp{
			{Type: CommandRegisterFile, Payload: legitPayload},
			{Type: CommandRegisterFile, Payload: maliciousPayload},
		},
	})
	result := fsm.Apply(&raft.Log{Index: 1, Data: batchData})
	if result == nil {
		t.Fatal("batch with malicious entry should error")
	}
	if _, ok := result.(error); !ok {
		t.Fatalf("expected error result, got %T", result)
	}
	// Atomic pre-validation: no ops land in f.files, including the
	// legit op that came BEFORE the malicious one in the batch.
	if fsm.FileCount() != 0 {
		t.Errorf("batch should leave f.files unchanged on validation failure; got %d entries", fsm.FileCount())
	}
	if _, exists := fsm.GetFile(legitFile.Path); exists {
		t.Error("legit op preceding malicious op MUST NOT land — batch must be atomic on validation failure")
	}
	// The rejected-paths counter increments once per pre-validation
	// rejection (one per malicious op in the batch).
	if c := fsm.RejectedPathsCount(); c != 1 {
		t.Errorf("expected RejectedPathsCount=1 (one malicious op), got %d", c)
	}
}

// TestApplyBatchFileOps_RejectsZeroCreatedAt_AtomicPrevalidation pins
// that the batch pre-validation pass mirrors applyRegisterFileStruct's
// CreatedAt requirement. Without this, a batch containing a legit op
// followed by an op with zero CreatedAt would pass path-validation
// pre-pass → the legit op would land in f.files → the second op
// would fail mid-apply → batch atomicity violated. Gemini #446-r2.
func TestApplyBatchFileOps_RejectsZeroCreatedAt_AtomicPrevalidation(t *testing.T) {
	t.Parallel()

	t.Run("register with zero CreatedAt", func(t *testing.T) {
		t.Parallel()
		fsm := newTestFSM()

		legitFile := makeFileEntry("db/cpu/2026/05/20/15/legit.parquet", "db", "cpu", 100)
		// CreatedAt deliberately left zero.
		badFile := FileEntry{
			Path:         "db/cpu/2026/05/20/15/bad.parquet",
			Database:     "db",
			Measurement:  "cpu",
			SizeBytes:    100,
			OriginNodeID: "writer-1",
			Tier:         "hot",
		}

		legitPayload, _ := json.Marshal(RegisterFilePayload{File: legitFile})
		badPayload, _ := json.Marshal(RegisterFilePayload{File: badFile})

		batchData := makeCommand(t, CommandBatchFileOps, BatchFileOpsPayload{
			Ops: []BatchFileOp{
				{Type: CommandRegisterFile, Payload: legitPayload},
				{Type: CommandRegisterFile, Payload: badPayload},
			},
		})
		result := fsm.Apply(&raft.Log{Index: 1, Data: batchData})
		if result == nil {
			t.Fatal("batch with zero-CreatedAt op should error")
		}
		if _, ok := result.(error); !ok {
			t.Fatalf("expected error result, got %T", result)
		}
		// Atomic: legit op preceding bad op MUST NOT land.
		if fsm.FileCount() != 0 {
			t.Errorf("batch atomicity violated: f.files has %d entries (expected 0)", fsm.FileCount())
		}
	})

	t.Run("update with zero CreatedAt", func(t *testing.T) {
		t.Parallel()
		fsm := newTestFSM()

		// Pre-register an entry so the update has something to mutate.
		preReg := makeFileEntry("db/cpu/2026/05/20/15/pre.parquet", "db", "cpu", 100)
		fsm.Apply(&raft.Log{Index: 1, Data: makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: preReg})})

		legitUpd := preReg
		legitUpd.SizeBytes = 999
		badUpd := FileEntry{
			Path:         "db/cpu/2026/05/20/15/other.parquet",
			Database:     "db",
			Measurement:  "cpu",
			SizeBytes:    200,
			OriginNodeID: "writer-1",
			Tier:         "hot",
			// CreatedAt zero.
		}

		legitPayload, _ := json.Marshal(UpdateFilePayload{File: legitUpd})
		badPayload, _ := json.Marshal(UpdateFilePayload{File: badUpd})

		batchData := makeCommand(t, CommandBatchFileOps, BatchFileOpsPayload{
			Ops: []BatchFileOp{
				{Type: CommandUpdateFile, Payload: legitPayload},
				{Type: CommandUpdateFile, Payload: badPayload},
			},
		})
		result := fsm.Apply(&raft.Log{Index: 2, Data: batchData})
		if result == nil {
			t.Fatal("batch with zero-CreatedAt update should error")
		}
		// Pre-existing entry must keep its original SizeBytes — the
		// legit update preceding the bad one MUST NOT have landed.
		got, _ := fsm.GetFile(preReg.Path)
		if got.SizeBytes != 100 {
			t.Errorf("batch atomicity violated: legit update landed (SizeBytes=%d, want pre-batch value 100)", got.SizeBytes)
		}
	})
}

// TestApplyBatchFileOps_RejectsEmptyDeletePath_AtomicPrevalidation pins
// that the batch pre-pass also catches structural invariants for
// Delete ops (unmarshal success + non-empty path). Without this, a
// batch with [legit, delete-with-empty-path] would have applied the
// legit op before applyDeleteFile rejected the second — violating
// batch atomicity. Gemini #446-r4.
func TestApplyBatchFileOps_RejectsEmptyDeletePath_AtomicPrevalidation(t *testing.T) {
	t.Parallel()
	fsm := newTestFSM()

	legitFile := makeFileEntry("db/cpu/2026/05/20/15/legit.parquet", "db", "cpu", 100)
	legitPayload, _ := json.Marshal(RegisterFilePayload{File: legitFile})
	emptyDelPayload, _ := json.Marshal(DeleteFilePayload{Path: "", Reason: "test"})

	batchData := makeCommand(t, CommandBatchFileOps, BatchFileOpsPayload{
		Ops: []BatchFileOp{
			{Type: CommandRegisterFile, Payload: legitPayload},
			{Type: CommandDeleteFile, Payload: emptyDelPayload},
		},
	})
	result := fsm.Apply(&raft.Log{Index: 1, Data: batchData})
	if result == nil {
		t.Fatal("batch with empty-path Delete should error")
	}
	if _, ok := result.(error); !ok {
		t.Fatalf("expected error result, got %T", result)
	}
	// Atomic: legit Register MUST NOT land.
	if fsm.FileCount() != 0 {
		t.Errorf("batch atomicity violated: f.files has %d entries (expected 0)", fsm.FileCount())
	}
	if _, exists := fsm.GetFile(legitFile.Path); exists {
		t.Error("legit op preceding empty-path Delete MUST NOT land")
	}
	// Delete path is not validated for shape (it's a map-key lookup),
	// so the rejectedPaths counter is NOT incremented by this case —
	// only by ValidateManifestPath rejections.
	if c := fsm.RejectedPathsCount(); c != 0 {
		t.Errorf("empty-Delete-path rejection should NOT touch rejectedPaths counter (got %d)", c)
	}
}

// TestApplyBatchFileOps_RejectsMalformedDeletePayload_AtomicPrevalidation
// pins the second half of the Delete pre-check: if op.Payload doesn't
// even unmarshal as a DeleteFilePayload, refuse the whole batch.
// Gemini #446-r4.
func TestApplyBatchFileOps_RejectsMalformedDeletePayload_AtomicPrevalidation(t *testing.T) {
	t.Parallel()
	fsm := newTestFSM()

	legitFile := makeFileEntry("db/cpu/2026/05/20/15/legit.parquet", "db", "cpu", 100)
	legitPayload, _ := json.Marshal(RegisterFilePayload{File: legitFile})

	batchData := makeCommand(t, CommandBatchFileOps, BatchFileOpsPayload{
		Ops: []BatchFileOp{
			{Type: CommandRegisterFile, Payload: legitPayload},
			{Type: CommandDeleteFile, Payload: []byte("not-valid-json{")},
		},
	})
	result := fsm.Apply(&raft.Log{Index: 1, Data: batchData})
	if result == nil {
		t.Fatal("batch with malformed Delete payload should error")
	}
	if fsm.FileCount() != 0 {
		t.Errorf("batch atomicity violated: legit op landed despite malformed Delete (count=%d)", fsm.FileCount())
	}
}

// TestApplyUpdateFile_RejectsZeroCreatedAt pins the standalone (non-batch)
// path's CreatedAt requirement on Update. Gemini #446-r2.
func TestApplyUpdateFile_RejectsZeroCreatedAt(t *testing.T) {
	t.Parallel()
	fsm := newTestFSM()

	bad := FileEntry{
		Path:         "db/cpu/2026/05/20/15/file.parquet",
		Database:     "db",
		Measurement:  "cpu",
		SizeBytes:    100,
		OriginNodeID: "writer-1",
		Tier:         "hot",
		// CreatedAt zero.
	}
	data := makeCommand(t, CommandUpdateFile, UpdateFilePayload{File: bad})
	result := fsm.Apply(&raft.Log{Index: 1, Data: data})
	if result == nil {
		t.Fatal("update with zero CreatedAt should error")
	}
	if _, ok := result.(error); !ok {
		t.Fatalf("expected error result, got %T", result)
	}
	if fsm.FileCount() != 0 {
		t.Errorf("zero-CreatedAt update should not land in files (count=%d)", fsm.FileCount())
	}
}
