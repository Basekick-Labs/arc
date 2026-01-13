package sharding

import (
	"testing"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardRaftManager_StartStop(t *testing.T) {
	localNode := &cluster.Node{ID: "local-node"}

	mgr := NewShardRaftManager(&ShardRaftConfig{
		DataDir:   t.TempDir(),
		BasePort:  9400,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	err := mgr.Start()
	require.NoError(t, err)
	assert.True(t, mgr.running)

	err = mgr.Stop()
	require.NoError(t, err)
	assert.False(t, mgr.running)
}

func TestShardRaftManager_JoinLeaveShard(t *testing.T) {
	localNode := &cluster.Node{ID: "local-node"}

	mgr := NewShardRaftManager(&ShardRaftConfig{
		DataDir:   t.TempDir(),
		BasePort:  9410,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	err := mgr.Start()
	require.NoError(t, err)
	defer mgr.Stop()

	// Join shard (lightweight mode, no actual Raft)
	err = mgr.JoinShard(0, nil, false)
	require.NoError(t, err)

	stats := mgr.Stats()
	assert.Equal(t, 1, stats["shard_count"])

	// Leave shard
	err = mgr.LeaveShard(0)
	require.NoError(t, err)

	stats = mgr.Stats()
	assert.Equal(t, 0, stats["shard_count"])
}

func TestShardRaftManager_LeadershipCallbacks(t *testing.T) {
	localNode := &cluster.Node{ID: "local-node"}

	becameLeader := make(chan int, 1)
	lostLeadership := make(chan int, 1)

	mgr := NewShardRaftManager(&ShardRaftConfig{
		DataDir:   t.TempDir(),
		BasePort:  9420,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	mgr.SetCallbacks(
		func(shardID int) { becameLeader <- shardID },
		func(shardID int) { lostLeadership <- shardID },
	)

	err := mgr.Start()
	require.NoError(t, err)
	defer mgr.Stop()

	// Join shard
	err = mgr.JoinShard(1, nil, false)
	require.NoError(t, err)

	// Manually set leadership
	mgr.mu.RLock()
	node := mgr.shards[1]
	mgr.mu.RUnlock()

	node.SetLeader(true)

	select {
	case shardID := <-becameLeader:
		assert.Equal(t, 1, shardID)
	default:
		t.Fatal("Expected becameLeader callback")
	}

	// Lose leadership
	node.SetLeader(false)

	select {
	case shardID := <-lostLeadership:
		assert.Equal(t, 1, shardID)
	default:
		t.Fatal("Expected lostLeadership callback")
	}
}

func TestShardRaftNode_IsLeader(t *testing.T) {
	node := &ShardRaftNode{
		shardID: 0,
		logger:  zerolog.Nop(),
	}

	// Initially not leader
	assert.False(t, node.IsLeader())

	// Set as leader
	node.SetLeader(true)
	assert.True(t, node.IsLeader())

	// Remove leadership
	node.SetLeader(false)
	assert.False(t, node.IsLeader())
}

func TestShardRaftNode_Stats(t *testing.T) {
	node := &ShardRaftNode{
		shardID: 5,
		running: true,
		logger:  zerolog.Nop(),
	}

	node.SetLeader(true)

	stats := node.Stats()
	assert.Equal(t, 5, stats["shard_id"])
	assert.True(t, stats["running"].(bool))
	assert.True(t, stats["is_leader"].(bool))
}

func TestShardFSM_GetPrimaryID(t *testing.T) {
	fsm := &ShardFSM{
		shardID:    0,
		replicaLag: make(map[string]int64),
		logger:     zerolog.Nop(),
	}

	// Initially empty
	assert.Empty(t, fsm.GetPrimaryID())

	// Set primary
	fsm.mu.Lock()
	fsm.primaryID = "node-1"
	fsm.mu.Unlock()

	assert.Equal(t, "node-1", fsm.GetPrimaryID())
}

func TestShardFSM_GetLastWriteSeq(t *testing.T) {
	fsm := &ShardFSM{
		shardID:    0,
		replicaLag: make(map[string]int64),
		logger:     zerolog.Nop(),
	}

	// Initially zero
	assert.Equal(t, uint64(0), fsm.GetLastWriteSeq())

	// Update
	fsm.mu.Lock()
	fsm.lastWriteSeq = 12345
	fsm.mu.Unlock()

	assert.Equal(t, uint64(12345), fsm.GetLastWriteSeq())
}

func TestShardRaftManager_Stats(t *testing.T) {
	localNode := &cluster.Node{ID: "local-node"}

	mgr := NewShardRaftManager(&ShardRaftConfig{
		DataDir:   t.TempDir(),
		BasePort:  9430,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	err := mgr.Start()
	require.NoError(t, err)
	defer mgr.Stop()

	// Add some shards
	err = mgr.JoinShard(0, nil, false)
	require.NoError(t, err)
	err = mgr.JoinShard(1, nil, false)
	require.NoError(t, err)

	stats := mgr.Stats()
	assert.True(t, stats["running"].(bool))
	assert.Equal(t, 2, stats["shard_count"])
	assert.NotNil(t, stats["shards"])
}
