package sharding

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetaFSM_AssignShard(t *testing.T) {
	fsm := NewMetaFSM(3, zerolog.Nop())

	// First add a node
	addPayload, _ := json.Marshal(AddNodePayload{
		Node: NodeInfo{
			ID:         "node-1",
			Name:       "node-1",
			APIAddress: "localhost:8000",
			CoordAddr:  "localhost:9100",
			State:      string(cluster.StateHealthy),
		},
	})
	addCmd, _ := json.Marshal(MetaCommand{
		Type:    CmdAddNode,
		Payload: addPayload,
	})
	fsm.Apply(&raft.Log{Data: addCmd})

	// Assign shard 0 to node-1 as primary
	assignPayload, _ := json.Marshal(AssignShardPayload{
		ShardID: 0,
		NodeID:  "node-1",
		Role:    RolePrimary,
	})
	assignCmd, _ := json.Marshal(MetaCommand{
		Type:    CmdAssignShard,
		Payload: assignPayload,
	})

	result := fsm.Apply(&raft.Log{Data: assignCmd})
	assert.Nil(t, result)

	// Verify assignment
	shardMap := fsm.GetShardMap()
	primary := shardMap.GetPrimary(0)
	require.NotNil(t, primary)
	assert.Equal(t, "node-1", primary.ID)
}

func TestMetaFSM_AssignShardReplica(t *testing.T) {
	fsm := NewMetaFSM(3, zerolog.Nop())

	// Add two nodes
	for _, nodeID := range []string{"node-1", "node-2"} {
		addPayload, _ := json.Marshal(AddNodePayload{
			Node: NodeInfo{
				ID:         nodeID,
				Name:       nodeID,
				APIAddress: "localhost:8000",
				CoordAddr:  "localhost:9100",
				State:      string(cluster.StateHealthy),
			},
		})
		addCmd, _ := json.Marshal(MetaCommand{
			Type:    CmdAddNode,
			Payload: addPayload,
		})
		fsm.Apply(&raft.Log{Data: addCmd})
	}

	// Assign primary
	assignPrimary, _ := json.Marshal(AssignShardPayload{ShardID: 0, NodeID: "node-1", Role: RolePrimary})
	assignPrimaryCmd, _ := json.Marshal(MetaCommand{Type: CmdAssignShard, Payload: assignPrimary})
	fsm.Apply(&raft.Log{Data: assignPrimaryCmd})

	// Assign replica
	assignReplica, _ := json.Marshal(AssignShardPayload{ShardID: 0, NodeID: "node-2", Role: RoleReplica})
	assignReplicaCmd, _ := json.Marshal(MetaCommand{Type: CmdAssignShard, Payload: assignReplica})
	fsm.Apply(&raft.Log{Data: assignReplicaCmd})

	// Verify
	shardMap := fsm.GetShardMap()
	group := shardMap.GetShardGroup(0)
	require.NotNil(t, group)
	assert.Equal(t, "node-1", group.Primary.ID)
	assert.Len(t, group.Replicas, 1)
	assert.Equal(t, "node-2", group.Replicas[0].ID)
}

func TestMetaFSM_UnassignShard(t *testing.T) {
	fsm := NewMetaFSM(3, zerolog.Nop())

	// Add node
	addPayload, _ := json.Marshal(AddNodePayload{
		Node: NodeInfo{ID: "node-1", State: string(cluster.StateHealthy)},
	})
	addCmd, _ := json.Marshal(MetaCommand{Type: CmdAddNode, Payload: addPayload})
	fsm.Apply(&raft.Log{Data: addCmd})

	// Assign shard
	assignPayload, _ := json.Marshal(AssignShardPayload{ShardID: 0, NodeID: "node-1", Role: RolePrimary})
	assignCmd, _ := json.Marshal(MetaCommand{Type: CmdAssignShard, Payload: assignPayload})
	fsm.Apply(&raft.Log{Data: assignCmd})

	// Verify assigned
	require.NotNil(t, fsm.GetShardMap().GetPrimary(0))

	// Unassign
	unassignPayload, _ := json.Marshal(UnassignShardPayload{ShardID: 0, NodeID: "node-1"})
	unassignCmd, _ := json.Marshal(MetaCommand{Type: CmdUnassignShard, Payload: unassignPayload})
	fsm.Apply(&raft.Log{Data: unassignCmd})

	// Verify unassigned
	assert.Nil(t, fsm.GetShardMap().GetPrimary(0))
}

func TestMetaFSM_RemoveNode(t *testing.T) {
	fsm := NewMetaFSM(3, zerolog.Nop())

	// Add node
	addPayload, _ := json.Marshal(AddNodePayload{
		Node: NodeInfo{ID: "node-1", State: string(cluster.StateHealthy)},
	})
	addCmd, _ := json.Marshal(MetaCommand{Type: CmdAddNode, Payload: addPayload})
	fsm.Apply(&raft.Log{Data: addCmd})

	// Assign shard to this node
	assignPayload, _ := json.Marshal(AssignShardPayload{ShardID: 0, NodeID: "node-1", Role: RolePrimary})
	assignCmd, _ := json.Marshal(MetaCommand{Type: CmdAssignShard, Payload: assignPayload})
	fsm.Apply(&raft.Log{Data: assignCmd})

	// Verify node and assignment exist
	_, exists := fsm.GetNode("node-1")
	require.True(t, exists)
	require.NotNil(t, fsm.GetShardMap().GetPrimary(0))

	// Remove node
	removePayload, _ := json.Marshal(RemoveNodePayload{NodeID: "node-1"})
	removeCmd, _ := json.Marshal(MetaCommand{Type: CmdRemoveNode, Payload: removePayload})
	fsm.Apply(&raft.Log{Data: removeCmd})

	// Verify node is gone and shard is unassigned
	_, exists = fsm.GetNode("node-1")
	assert.False(t, exists)
	assert.Nil(t, fsm.GetShardMap().GetPrimary(0))
}

func TestMetaFSM_SnapshotRestore(t *testing.T) {
	// Create FSM and populate it
	fsm := NewMetaFSM(3, zerolog.Nop())

	// Add nodes
	for _, nodeID := range []string{"node-1", "node-2", "node-3"} {
		addPayload, _ := json.Marshal(AddNodePayload{
			Node: NodeInfo{
				ID:         nodeID,
				Name:       nodeID,
				APIAddress: nodeID + ":8000",
				State:      string(cluster.StateHealthy),
			},
		})
		addCmd, _ := json.Marshal(MetaCommand{Type: CmdAddNode, Payload: addPayload})
		fsm.Apply(&raft.Log{Data: addCmd})
	}

	// Assign shards
	assignments := []struct {
		shardID int
		nodeID  string
		role    Role
	}{
		{0, "node-1", RolePrimary},
		{0, "node-2", RoleReplica},
		{1, "node-2", RolePrimary},
		{1, "node-3", RoleReplica},
		{2, "node-3", RolePrimary},
	}

	for _, a := range assignments {
		assignPayload, _ := json.Marshal(AssignShardPayload{ShardID: a.shardID, NodeID: a.nodeID, Role: a.role})
		assignCmd, _ := json.Marshal(MetaCommand{Type: CmdAssignShard, Payload: assignPayload})
		fsm.Apply(&raft.Log{Data: assignCmd})
	}

	// Create snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)

	// Serialize snapshot
	metaSnapshot := snapshot.(*MetaFSMSnapshot)
	data, err := json.Marshal(metaSnapshot)
	require.NoError(t, err)

	// Create new FSM and restore
	fsm2 := NewMetaFSM(3, zerolog.Nop())

	// Create a reader from the data
	reader := &mockReadCloser{data: data}
	err = fsm2.Restore(reader)
	require.NoError(t, err)

	// Verify restored state
	assert.Equal(t, 3, fsm2.GetShardMap().NumShards())
	assert.Len(t, fsm2.GetNodes(), 3)

	// Verify shard assignments
	require.NotNil(t, fsm2.GetShardMap().GetPrimary(0))
	assert.Equal(t, "node-1", fsm2.GetShardMap().GetPrimary(0).ID)

	require.NotNil(t, fsm2.GetShardMap().GetPrimary(1))
	assert.Equal(t, "node-2", fsm2.GetShardMap().GetPrimary(1).ID)

	require.NotNil(t, fsm2.GetShardMap().GetPrimary(2))
	assert.Equal(t, "node-3", fsm2.GetShardMap().GetPrimary(2).ID)
}

func TestMetaFSM_Callbacks(t *testing.T) {
	fsm := NewMetaFSM(3, zerolog.Nop())

	// Track callbacks with channels for synchronization
	assignChan := make(chan struct {
		shardID int
		nodeID  string
	}, 1)
	addChan := make(chan string, 1)

	fsm.SetCallbacks(
		func(shardID int, nodeID string, role Role) {
			assignChan <- struct {
				shardID int
				nodeID  string
			}{shardID, nodeID}
		},
		nil,
		func(node *NodeInfo) { addChan <- node.ID },
		nil,
	)

	// Add node
	addPayload, _ := json.Marshal(AddNodePayload{
		Node: NodeInfo{ID: "node-1", State: string(cluster.StateHealthy)},
	})
	addCmd, _ := json.Marshal(MetaCommand{Type: CmdAddNode, Payload: addPayload})
	fsm.Apply(&raft.Log{Data: addCmd})

	// Wait for callback
	select {
	case nodeID := <-addChan:
		assert.Equal(t, "node-1", nodeID)
	case <-time.After(time.Second):
		t.Fatal("onNodeAdded callback not called")
	}

	// Test shard assignment callback
	assignPayload, _ := json.Marshal(AssignShardPayload{ShardID: 1, NodeID: "node-1", Role: RolePrimary})
	assignCmd, _ := json.Marshal(MetaCommand{Type: CmdAssignShard, Payload: assignPayload})
	fsm.Apply(&raft.Log{Data: assignCmd})

	// Wait for callback
	select {
	case result := <-assignChan:
		assert.Equal(t, 1, result.shardID)
		assert.Equal(t, "node-1", result.nodeID)
	case <-time.After(time.Second):
		t.Fatal("onShardAssigned callback not called")
	}
}

func TestMetaFSM_GetNodes(t *testing.T) {
	fsm := NewMetaFSM(3, zerolog.Nop())

	// Initially empty
	assert.Empty(t, fsm.GetNodes())

	// Add nodes
	for i, nodeID := range []string{"node-1", "node-2"} {
		addPayload, _ := json.Marshal(AddNodePayload{
			Node: NodeInfo{
				ID:         nodeID,
				Name:       nodeID,
				APIAddress: "localhost:" + string(rune('0'+i)),
				State:      string(cluster.StateHealthy),
			},
		})
		addCmd, _ := json.Marshal(MetaCommand{Type: CmdAddNode, Payload: addPayload})
		fsm.Apply(&raft.Log{Data: addCmd})
	}

	nodes := fsm.GetNodes()
	assert.Len(t, nodes, 2)
	assert.Contains(t, nodes, "node-1")
	assert.Contains(t, nodes, "node-2")
}

// mockReadCloser implements io.ReadCloser for testing.
type mockReadCloser struct {
	data   []byte
	offset int
}

func (r *mockReadCloser) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, nil
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *mockReadCloser) Close() error {
	return nil
}
