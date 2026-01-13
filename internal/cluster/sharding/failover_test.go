package sharding

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverManager_StartStop(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:            sm,
		LocalNode:           localNode,
		HealthCheckInterval: 100 * time.Millisecond,
		Logger:              zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	assert.True(t, mgr.running.Load())

	err = mgr.Stop()
	require.NoError(t, err)
	assert.False(t, mgr.running.Load())
}

func TestFailoverManager_NodeHealthTracking(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	// Add some nodes to the shard map
	node1 := &cluster.Node{ID: "node-1"}
	node1.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(0, node1)

	node2 := &cluster.Node{ID: "node-2"}
	node2.UpdateState(cluster.StateHealthy)
	sm.AddReplica(0, node2)

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:            sm,
		LocalNode:           localNode,
		HealthCheckInterval: 50 * time.Millisecond,
		UnhealthyThreshold:  2,
		Logger:              zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Wait for health check
	time.Sleep(100 * time.Millisecond)

	// Check node health status
	status := mgr.GetNodeHealthStatus("node-1")
	require.NotNil(t, status)
	assert.True(t, status["is_healthy"].(bool))
}

func TestFailoverManager_ShardHealthTracking(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	// Add primary for shard 0
	primary := &cluster.Node{ID: "primary-node"}
	primary.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(0, primary)

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:            sm,
		LocalNode:           localNode,
		HealthCheckInterval: 50 * time.Millisecond,
		Logger:              zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Wait for health check
	time.Sleep(100 * time.Millisecond)

	// Check shard health status
	status := mgr.GetShardHealthStatus(0)
	require.NotNil(t, status)
	assert.Equal(t, "primary-node", status["primary_id"])
	assert.True(t, status["is_primary_up"].(bool))
	assert.False(t, status["failover_in_prog"].(bool))
}

func TestFailoverManager_DetectUnhealthyNode(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	// Add an unhealthy node
	unhealthyNode := &cluster.Node{ID: "unhealthy-node"}
	unhealthyNode.UpdateState(cluster.StateDead)
	sm.SetPrimary(0, unhealthyNode)

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:            sm,
		LocalNode:           localNode,
		HealthCheckInterval: 50 * time.Millisecond,
		UnhealthyThreshold:  2,
		Logger:              zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Wait for multiple health checks
	time.Sleep(200 * time.Millisecond)

	// Check that node is marked unhealthy
	status := mgr.GetNodeHealthStatus("unhealthy-node")
	require.NotNil(t, status)
	assert.False(t, status["is_healthy"].(bool))
}

func TestFailoverManager_FailoverCallbacks(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	failoverStarted := make(chan struct {
		shardID    int
		oldPrimary string
		newPrimary string
	}, 1)
	failoverCompleted := make(chan struct {
		shardID    int
		newPrimary string
		success    bool
	}, 1)

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:            sm,
		LocalNode:           localNode,
		HealthCheckInterval: 100 * time.Millisecond,
		FailoverTimeout:     5 * time.Second,
		Logger:              zerolog.Nop(),
	})

	mgr.SetCallbacks(
		func(shardID int, oldPrimary, newPrimary string) {
			failoverStarted <- struct {
				shardID    int
				oldPrimary string
				newPrimary string
			}{shardID, oldPrimary, newPrimary}
		},
		func(shardID int, newPrimary string, success bool) {
			failoverCompleted <- struct {
				shardID    int
				newPrimary string
				success    bool
			}{shardID, newPrimary, success}
		},
	)

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Callbacks are tested indirectly through TriggerManualFailover
	// For now, just verify they are set correctly
	assert.NotNil(t, mgr.onFailoverStart)
	assert.NotNil(t, mgr.onFailoverComplete)
}

func TestFailoverManager_TriggerManualFailover_NoPrimary(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Try to trigger failover for shard with no primary
	err = mgr.TriggerManualFailover(0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no primary")
}

func TestFailoverManager_TriggerManualFailover_AlreadyInProgress(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	primary := &cluster.Node{ID: "primary-node"}
	primary.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(0, primary)

	// Add a replica so failover doesn't complete immediately
	replica := &cluster.Node{ID: "replica-node"}
	replica.UpdateState(cluster.StateHealthy)
	sm.AddReplica(0, replica)

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:        sm,
		LocalNode:       localNode,
		FailoverTimeout: 10 * time.Second, // Longer timeout to keep failover in progress
		Logger:          zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Initialize node health so replica is known
	mgr.mu.Lock()
	mgr.nodeHealth["replica-node"] = &nodeHealthState{
		nodeID:    "replica-node",
		isHealthy: true,
	}
	mgr.mu.Unlock()

	// Manually set failover in progress to simulate concurrent request
	mgr.mu.Lock()
	mgr.shardHealth[0] = &shardHealthState{
		shardID:        0,
		primaryID:      "primary-node",
		failoverInProg: true,
	}
	mgr.mu.Unlock()

	// Try to trigger another failover (should fail since one is "in progress")
	err = mgr.TriggerManualFailover(0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already in progress")
}

func TestFailoverManager_Stats(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	node1 := &cluster.Node{ID: "node-1"}
	node1.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(0, node1)

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:            sm,
		LocalNode:           localNode,
		HealthCheckInterval: 50 * time.Millisecond,
		UnhealthyThreshold:  3,
		Logger:              zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Wait for health check
	time.Sleep(100 * time.Millisecond)

	stats := mgr.Stats()
	assert.True(t, stats["running"].(bool))
	assert.Equal(t, 3, stats["unhealthy_threshold"])
	assert.NotNil(t, stats["nodes"])
	assert.NotNil(t, stats["shards"])
}

func TestFailoverManager_SelectNewPrimary(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	// Setup shard with primary and replicas
	primary := &cluster.Node{ID: "primary-node"}
	primary.UpdateState(cluster.StateDead)
	sm.SetPrimary(0, primary)

	replica1 := &cluster.Node{ID: "replica-1"}
	replica1.UpdateState(cluster.StateHealthy)
	sm.AddReplica(0, replica1)

	replica2 := &cluster.Node{ID: "replica-2"}
	replica2.UpdateState(cluster.StateHealthy)
	sm.AddReplica(0, replica2)

	mgr := NewFailoverManager(&FailoverConfig{
		ShardMap:            sm,
		LocalNode:           localNode,
		HealthCheckInterval: 50 * time.Millisecond,
		Logger:              zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Wait for health tracking to initialize
	time.Sleep(100 * time.Millisecond)

	// Select new primary (should exclude the current primary)
	newPrimary := mgr.selectNewPrimary(0, "primary-node")
	assert.NotEmpty(t, newPrimary)
	assert.NotEqual(t, "primary-node", newPrimary)
	// Should be one of the healthy replicas
	assert.True(t, newPrimary == "replica-1" || newPrimary == "replica-2")
}
