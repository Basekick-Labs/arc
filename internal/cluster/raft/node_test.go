package raft

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestNodeStartStop(t *testing.T) {
	// Create temp directory for Raft data
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fsm := NewClusterFSM(zerolog.Nop())

	cfg := &NodeConfig{
		NodeID:           "test-node-1",
		DataDir:          tmpDir,
		BindAddr:         "127.0.0.1:0", // Use port 0 for random available port
		Bootstrap:        true,
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 500 * time.Millisecond,
		Logger:           zerolog.Nop(),
	}

	node, err := NewNode(cfg, fsm)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	// Start the node
	if err := node.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for leader election
	err = node.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader failed: %v", err)
	}

	// Verify this node is the leader (it's the only node)
	if !node.IsLeader() {
		t.Error("Node should be the leader (only node in cluster)")
	}

	// Check leader address is set
	if node.LeaderAddr() == "" {
		t.Error("LeaderAddr should not be empty")
	}

	// Check leader ID is set
	if node.LeaderID() == "" {
		t.Error("LeaderID should not be empty")
	}

	// Stop the node
	if err := node.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestNodeAddNodeViaRaft(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fsm := NewClusterFSM(zerolog.Nop())

	// Track callback invocations
	var addedNodes []*NodeInfo
	fsm.SetCallbacks(
		func(n *NodeInfo) { addedNodes = append(addedNodes, n) },
		nil,
		nil,
	)

	cfg := &NodeConfig{
		NodeID:           "test-node-1",
		DataDir:          tmpDir,
		BindAddr:         "127.0.0.1:0",
		Bootstrap:        true,
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 500 * time.Millisecond,
		Logger:           zerolog.Nop(),
	}

	node, err := NewNode(cfg, fsm)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader failed: %v", err)
	}

	// Add a node via Raft
	testNode := &NodeInfo{
		ID:          "writer-1",
		Name:        "Writer Node 1",
		Role:        "writer",
		ClusterName: "test-cluster",
		Address:     "10.0.0.1:9100",
		APIAddress:  "10.0.0.1:8000",
		State:       "healthy",
		Version:     "1.0.0",
	}

	if err := node.AddNode(testNode, 5*time.Second); err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}

	// Verify the node was added to FSM
	got, exists := fsm.GetNode("writer-1")
	if !exists {
		t.Fatal("Node should exist in FSM after AddNode")
	}
	if got.ID != testNode.ID {
		t.Errorf("Node ID mismatch: got %s, want %s", got.ID, testNode.ID)
	}
	if got.Role != testNode.Role {
		t.Errorf("Node Role mismatch: got %s, want %s", got.Role, testNode.Role)
	}

	// Verify callback was invoked
	if len(addedNodes) != 1 {
		t.Errorf("Expected 1 callback invocation, got %d", len(addedNodes))
	}
}

func TestNodeUpdateNodeStateViaRaft(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fsm := NewClusterFSM(zerolog.Nop())

	cfg := &NodeConfig{
		NodeID:           "test-node-1",
		DataDir:          tmpDir,
		BindAddr:         "127.0.0.1:0",
		Bootstrap:        true,
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 500 * time.Millisecond,
		Logger:           zerolog.Nop(),
	}

	node, err := NewNode(cfg, fsm)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader failed: %v", err)
	}

	// First add a node
	testNode := &NodeInfo{
		ID:    "writer-1",
		Name:  "Writer Node 1",
		Role:  "writer",
		State: "healthy",
	}
	if err := node.AddNode(testNode, 5*time.Second); err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}

	// Update the node's state
	if err := node.UpdateNodeState("writer-1", "unhealthy", 5*time.Second); err != nil {
		t.Fatalf("UpdateNodeState failed: %v", err)
	}

	// Verify the state was updated
	got, _ := fsm.GetNode("writer-1")
	if got.State != "unhealthy" {
		t.Errorf("Node state = %s, want unhealthy", got.State)
	}
}

func TestNodeRemoveNodeViaRaft(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fsm := NewClusterFSM(zerolog.Nop())

	cfg := &NodeConfig{
		NodeID:           "test-node-1",
		DataDir:          tmpDir,
		BindAddr:         "127.0.0.1:0",
		Bootstrap:        true,
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 500 * time.Millisecond,
		Logger:           zerolog.Nop(),
	}

	node, err := NewNode(cfg, fsm)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader failed: %v", err)
	}

	// Add then remove a node
	testNode := &NodeInfo{ID: "writer-1", Name: "Writer", Role: "writer"}
	if err := node.AddNode(testNode, 5*time.Second); err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}

	// Verify node exists
	if _, exists := fsm.GetNode("writer-1"); !exists {
		t.Fatal("Node should exist after AddNode")
	}

	// Remove the node
	if err := node.RemoveNode("writer-1", 5*time.Second); err != nil {
		t.Fatalf("RemoveNode failed: %v", err)
	}

	// Verify node was removed
	if _, exists := fsm.GetNode("writer-1"); exists {
		t.Error("Node should not exist after RemoveNode")
	}
}

func TestNodeStats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fsm := NewClusterFSM(zerolog.Nop())

	cfg := &NodeConfig{
		NodeID:           "test-node-1",
		DataDir:          tmpDir,
		BindAddr:         "127.0.0.1:0",
		Bootstrap:        true,
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 500 * time.Millisecond,
		Logger:           zerolog.Nop(),
	}

	node, err := NewNode(cfg, fsm)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("WaitForLeader failed: %v", err)
	}

	// Get stats
	stats := node.Stats()
	if stats == nil {
		t.Fatal("Stats should not be nil")
	}

	// Verify some expected stats exist
	if _, ok := stats["state"]; !ok {
		t.Error("Stats should contain 'state'")
	}
	if _, ok := stats["num_peers"]; !ok {
		t.Error("Stats should contain 'num_peers'")
	}
}

func TestNodeDataDirCreated(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use a subdirectory that doesn't exist yet
	dataDir := filepath.Join(tmpDir, "subdir", "raft-data")

	fsm := NewClusterFSM(zerolog.Nop())
	cfg := &NodeConfig{
		NodeID:           "test-node-1",
		DataDir:          dataDir,
		BindAddr:         "127.0.0.1:0",
		Bootstrap:        true,
		ElectionTimeout:  500 * time.Millisecond,
		HeartbeatTimeout: 500 * time.Millisecond,
		Logger:           zerolog.Nop(),
	}

	node, err := NewNode(cfg, fsm)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	// Verify data directory was created
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Error("Data directory should have been created")
	}
}

func TestNodeConfigValidation(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())

	tests := []struct {
		name    string
		cfg     *NodeConfig
		wantErr bool
	}{
		{
			name:    "missing node ID",
			cfg:     &NodeConfig{DataDir: "/tmp", BindAddr: ":9200"},
			wantErr: true,
		},
		{
			name:    "missing data dir",
			cfg:     &NodeConfig{NodeID: "node-1", BindAddr: ":9200"},
			wantErr: true,
		},
		{
			name:    "missing bind addr",
			cfg:     &NodeConfig{NodeID: "node-1", DataDir: "/tmp"},
			wantErr: true,
		},
		{
			name:    "valid config",
			cfg:     &NodeConfig{NodeID: "node-1", DataDir: "/tmp", BindAddr: ":9200"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNode(tt.cfg, fsm)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
