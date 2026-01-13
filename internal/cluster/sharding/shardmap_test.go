package sharding

import (
	"strconv"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewShardMap(t *testing.T) {
	sm := NewShardMap(3)
	assert.Equal(t, 3, sm.NumShards())
	assert.Equal(t, uint64(0), sm.Version())

	// All shards should be initialized but unassigned
	for i := 0; i < 3; i++ {
		group := sm.GetShardGroup(i)
		require.NotNil(t, group)
		assert.Equal(t, i, group.ShardID)
		assert.Nil(t, group.Primary)
		assert.Empty(t, group.Replicas)
	}
}

func TestShardMapGetShard(t *testing.T) {
	sm := NewShardMap(3)

	// Same database should always hash to same shard
	shard1 := sm.GetShard("mydb")
	shard2 := sm.GetShard("mydb")
	assert.Equal(t, shard1, shard2)

	// Different databases may hash to different shards
	shardA := sm.GetShard("database_a")
	shardB := sm.GetShard("database_b")
	// They could be the same or different, just verify they're in range
	assert.GreaterOrEqual(t, shardA, 0)
	assert.Less(t, shardA, 3)
	assert.GreaterOrEqual(t, shardB, 0)
	assert.Less(t, shardB, 3)
}

func TestShardMapSetPrimary(t *testing.T) {
	sm := NewShardMap(3)

	node := &cluster.Node{ID: "node-1"}
	sm.SetPrimary(0, node)

	primary := sm.GetPrimary(0)
	require.NotNil(t, primary)
	assert.Equal(t, "node-1", primary.ID)

	// Version should increment
	assert.Equal(t, uint64(1), sm.Version())

	// Other shards should be unaffected
	assert.Nil(t, sm.GetPrimary(1))
	assert.Nil(t, sm.GetPrimary(2))
}

func TestShardMapAddRemoveReplica(t *testing.T) {
	sm := NewShardMap(3)

	node1 := &cluster.Node{ID: "replica-1"}
	node2 := &cluster.Node{ID: "replica-2"}

	// Add replicas
	sm.AddReplica(0, node1)
	sm.AddReplica(0, node2)

	group := sm.GetShardGroup(0)
	require.NotNil(t, group)
	assert.Len(t, group.Replicas, 2)

	// Adding same replica again should be idempotent
	sm.AddReplica(0, node1)
	group = sm.GetShardGroup(0)
	assert.Len(t, group.Replicas, 2)

	// Remove one replica
	sm.RemoveReplica(0, "replica-1")
	group = sm.GetShardGroup(0)
	assert.Len(t, group.Replicas, 1)
	assert.Equal(t, "replica-2", group.Replicas[0].ID)
}

func TestShardMapSelectNode(t *testing.T) {
	sm := NewShardMap(3)

	// No nodes assigned - should return nil
	assert.Nil(t, sm.SelectNode(0))

	// Set primary only
	primary := &cluster.Node{ID: "primary-1"}
	primary.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(0, primary)

	// Should return primary when no replicas
	selected := sm.SelectNode(0)
	require.NotNil(t, selected)
	assert.Equal(t, "primary-1", selected.ID)

	// Add healthy replica
	replica := &cluster.Node{ID: "replica-1"}
	replica.UpdateState(cluster.StateHealthy)
	sm.AddReplica(0, replica)

	// Should prefer replica for reads
	selected = sm.SelectNode(0)
	require.NotNil(t, selected)
	assert.Equal(t, "replica-1", selected.ID)
}

func TestShardMapGetAllNodes(t *testing.T) {
	sm := NewShardMap(3)

	primary := &cluster.Node{ID: "primary"}
	replica1 := &cluster.Node{ID: "replica-1"}
	replica2 := &cluster.Node{ID: "replica-2"}

	sm.SetPrimary(0, primary)
	sm.AddReplica(0, replica1)
	sm.AddReplica(0, replica2)

	nodes := sm.GetAllNodes(0)
	assert.Len(t, nodes, 3)

	// Verify all nodes are present
	ids := make(map[string]bool)
	for _, n := range nodes {
		ids[n.ID] = true
	}
	assert.True(t, ids["primary"])
	assert.True(t, ids["replica-1"])
	assert.True(t, ids["replica-2"])
}

func TestShardMapGetNodeShards(t *testing.T) {
	sm := NewShardMap(3)

	node1 := &cluster.Node{ID: "node-1"}
	node2 := &cluster.Node{ID: "node-2"}

	// node-1: primary for shard 0, replica for shard 1
	sm.SetPrimary(0, node1)
	sm.AddReplica(1, node1)

	// node-2: primary for shard 1, replica for shard 0
	sm.SetPrimary(1, node2)
	sm.AddReplica(0, node2)

	// Check node-1 shards
	roles := sm.GetNodeShards("node-1")
	assert.Len(t, roles, 2)

	roleMap := make(map[int]Role)
	for _, r := range roles {
		roleMap[r.ShardID] = r.Role
	}
	assert.Equal(t, RolePrimary, roleMap[0])
	assert.Equal(t, RoleReplica, roleMap[1])
}

func TestShardMapRemoveNode(t *testing.T) {
	sm := NewShardMap(3)

	node1 := &cluster.Node{ID: "node-1"}
	node2 := &cluster.Node{ID: "node-2"}

	// node-1: primary for shard 0, replica for shard 1
	sm.SetPrimary(0, node1)
	sm.AddReplica(1, node1)

	// node-2: primary for shard 1, replica for shard 0
	sm.SetPrimary(1, node2)
	sm.AddReplica(0, node2)

	// Remove node-1
	primaryShards := sm.RemoveNode("node-1")

	// Should report that shard 0 lost its primary
	assert.Contains(t, primaryShards, 0)
	assert.NotContains(t, primaryShards, 1)

	// Shard 0 should have no primary now
	assert.Nil(t, sm.GetPrimary(0))

	// Shard 1 should still have node-2 as primary
	assert.Equal(t, "node-2", sm.GetPrimary(1).ID)

	// node-1 should be removed from shard 1's replicas
	group := sm.GetShardGroup(1)
	for _, r := range group.Replicas {
		assert.NotEqual(t, "node-1", r.ID)
	}
}

func TestShardMapStats(t *testing.T) {
	sm := NewShardMap(3)

	node1 := &cluster.Node{ID: "node-1"}
	node2 := &cluster.Node{ID: "node-2"}

	sm.SetPrimary(0, node1)
	sm.SetPrimary(1, node2)
	sm.AddReplica(0, node2)
	sm.AddReplica(1, node1)

	stats := sm.Stats()

	assert.Equal(t, 3, stats["num_shards"])
	assert.Equal(t, 2, stats["assigned_shards"]) // 2 primaries set
	assert.Equal(t, 1, stats["unassigned"])      // shard 2 has no primary
	assert.Equal(t, 2, stats["total_replicas"])  // 2 replicas total
}

func TestShardMapDistribution(t *testing.T) {
	sm := NewShardMap(10)

	// Test that databases are roughly evenly distributed
	distribution := make(map[int]int)
	for i := 0; i < 1000; i++ {
		// Generate unique database names
		database := "database_" + strconv.Itoa(i)
		shard := sm.GetShard(database)
		distribution[shard]++
	}

	// Each shard should have roughly 100 databases (1000/10)
	// Allow for some variance (FNV-1a is well distributed)
	for shard := 0; shard < 10; shard++ {
		count := distribution[shard]
		assert.Greater(t, count, 50, "Shard %d has too few databases", shard)
		assert.Less(t, count, 150, "Shard %d has too many databases", shard)
	}
}
