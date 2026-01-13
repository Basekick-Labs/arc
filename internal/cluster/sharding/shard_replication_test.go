package sharding

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/replication"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardReplicationManager_StartStop(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	mgr := NewShardReplicationManager(&ShardReplicationConfig{
		ShardMap:   sm,
		LocalNode:  localNode,
		BufferSize: 100,
		Logger:     zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	assert.True(t, mgr.running.Load())

	err = mgr.Stop()
	require.NoError(t, err)
	assert.False(t, mgr.running.Load())
}

func TestShardReplicationManager_BecomePrimary(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	// Add a replica node
	replicaNode := &cluster.Node{ID: "replica-node", Address: "localhost:9999"}
	replicaNode.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(0, localNode)
	sm.AddReplica(0, replicaNode)

	mgr := NewShardReplicationManager(&ShardReplicationConfig{
		ShardMap:   sm,
		LocalNode:  localNode,
		BufferSize: 100,
		Logger:     zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Become primary for shard 0
	err = mgr.BecomePrimary(0)
	require.NoError(t, err)

	// Check stats
	stats := mgr.Stats()
	assert.Equal(t, 1, stats["shards_as_primary"])

	// Stop being primary
	mgr.StopPrimary(0)
	stats = mgr.Stats()
	assert.Equal(t, 0, stats["shards_as_primary"])
}

func TestShardSender_Replicate(t *testing.T) {
	cfg := &ShardReplicationConfig{
		BufferSize:   100,
		WriteTimeout: 5 * time.Second,
		LocalNode:    &cluster.Node{ID: "primary"},
		Logger:       zerolog.Nop(),
	}

	sender := newShardSender(0, cfg, zerolog.Nop())
	ctx := context.Background()
	err := sender.Start(ctx)
	require.NoError(t, err)
	defer sender.Stop()

	// Send some entries
	for i := 0; i < 10; i++ {
		entry := &replication.ReplicateEntry{
			TimestampUS: uint64(time.Now().UnixMicro()),
			Payload:     []byte("test payload"),
		}
		sender.Replicate(entry)
	}

	// Check stats
	stats := sender.Stats()
	assert.Equal(t, int64(10), stats["total_entries_received"])
}

func TestShardSender_BufferFull(t *testing.T) {
	cfg := &ShardReplicationConfig{
		BufferSize:   5, // Small buffer
		WriteTimeout: 5 * time.Second,
		LocalNode:    &cluster.Node{ID: "primary"},
		Logger:       zerolog.Nop(),
	}

	sender := newShardSender(0, cfg, zerolog.Nop())
	// Set running flag but don't start the distribution loop
	// This allows Replicate to queue entries without consuming them
	sender.running.Store(true)

	// Fill buffer past capacity
	for i := 0; i < 10; i++ {
		entry := &replication.ReplicateEntry{
			TimestampUS: uint64(time.Now().UnixMicro()),
			Payload:     []byte("test payload"),
		}
		sender.Replicate(entry)
	}

	// Buffer is 5, we sent 10, so at least 5 should be dropped
	assert.GreaterOrEqual(t, sender.totalEntriesDropped.Load(), int64(5))
}

func TestShardReceiverManager_StartStop(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	mgr := NewShardReceiverManager(&ShardReceiverConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	assert.True(t, mgr.running.Load())

	err = mgr.Stop()
	require.NoError(t, err)
	assert.False(t, mgr.running.Load())
}

func TestShardReceiverManager_BecomeReplica(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "replica-node"}
	primaryNode := &cluster.Node{ID: "primary-node", Address: "localhost:9998"}

	mgr := NewShardReceiverManager(&ShardReceiverConfig{
		ShardMap:          sm,
		LocalNode:         localNode,
		ReconnectInterval: 100 * time.Millisecond,
		Logger:            zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Become replica for shard 0
	err = mgr.BecomeReplica(0, primaryNode)
	require.NoError(t, err)

	// Check stats
	stats := mgr.Stats()
	assert.Equal(t, 1, stats["shards_as_replica"])

	// Stop being replica
	mgr.StopReplica(0)
	stats = mgr.Stats()
	assert.Equal(t, 0, stats["shards_as_replica"])
}

func TestShardReceiverManager_UpdatePrimary(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "replica-node"}
	primary1 := &cluster.Node{ID: "primary-1", Address: "localhost:9997"}
	primary2 := &cluster.Node{ID: "primary-2", Address: "localhost:9996"}

	mgr := NewShardReceiverManager(&ShardReceiverConfig{
		ShardMap:          sm,
		LocalNode:         localNode,
		ReconnectInterval: 100 * time.Millisecond,
		Logger:            zerolog.Nop(),
	})

	ctx := context.Background()
	err := mgr.Start(ctx)
	require.NoError(t, err)
	defer mgr.Stop()

	// Become replica for shard 0 with primary 1
	err = mgr.BecomeReplica(0, primary1)
	require.NoError(t, err)

	// Update to primary 2 (simulating failover)
	err = mgr.UpdatePrimary(0, primary2)
	require.NoError(t, err)

	// Should still have 1 shard as replica
	stats := mgr.Stats()
	assert.Equal(t, 1, stats["shards_as_replica"])

	// Verify the shard stats show the new primary
	shardStats := mgr.GetShardStats(0)
	require.NotNil(t, shardStats)
	assert.Equal(t, "primary-2", shardStats["primary_id"])
}

// Integration test for replication between sender and receiver
func TestShardReplication_Integration(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create a listener that simulates a replica accepting connections
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	replicaAddr := listener.Addr().String()

	// Track received entries
	entriesReceived := make(chan *replication.ReplicateEntry, 10)

	// Start mock replica server
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read sync request from primary
		msgType, _, err := replication.ReadMessage(conn)
		if err != nil {
			return
		}
		if msgType != replication.MsgReplicateSync {
			return
		}

		// Send sync ack
		syncAck := &replication.ReplicateSyncAck{
			CurrentSequence: 0,
			CanResume:       true,
		}
		if err := replication.WriteSyncAck(conn, syncAck); err != nil {
			return
		}

		// Read entries from primary
		for i := 0; i < 5; i++ {
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			msgType, payload, err := replication.ReadMessage(conn)
			if err != nil {
				return
			}
			if msgType == replication.MsgReplicateEntry {
				entry, err := replication.ParseEntry(payload)
				if err == nil {
					entriesReceived <- entry
				}
			}
		}
	}()

	// Setup shard map
	sm := NewShardMap(3)
	primaryNode := &cluster.Node{ID: "primary"}
	primaryNode.UpdateState(cluster.StateHealthy)

	// Replica node has the address of our mock server
	replicaNode := &cluster.Node{ID: "replica", Address: replicaAddr}
	replicaNode.UpdateState(cluster.StateHealthy)

	sm.SetPrimary(0, primaryNode)
	sm.AddReplica(0, replicaNode)

	// Create sender on primary
	sender := newShardSender(0, &ShardReplicationConfig{
		ShardMap:     sm,
		LocalNode:    primaryNode,
		BufferSize:   100,
		WriteTimeout: 5 * time.Second,
		Logger:       zerolog.Nop(),
	}, zerolog.Nop())

	ctx := context.Background()
	err = sender.Start(ctx)
	require.NoError(t, err)
	defer sender.Stop()

	// Add replica (this triggers connection to replicaAddr)
	err = sender.AddReplica(replicaNode)
	require.NoError(t, err)

	// Wait for connection to establish
	time.Sleep(500 * time.Millisecond)

	// Send entries through the sender
	for i := 0; i < 5; i++ {
		entry := &replication.ReplicateEntry{
			TimestampUS: uint64(time.Now().UnixMicro()),
			Payload:     []byte("test payload"),
		}
		sender.Replicate(entry)
	}

	// Wait for entries to be received by mock replica
	received := 0
	timeout := time.After(5 * time.Second)
	for received < 5 {
		select {
		case <-entriesReceived:
			received++
		case <-timeout:
			t.Fatalf("Timeout waiting for entries, received %d", received)
		}
	}

	assert.Equal(t, 5, received)
}

func TestShardReceiver_Stats(t *testing.T) {
	primaryNode := &cluster.Node{ID: "primary", Address: "localhost:9995"}
	localNode := &cluster.Node{ID: "replica"}

	receiver := newShardReceiver(0, primaryNode, &ShardReceiverConfig{
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	}, zerolog.Nop())

	stats := receiver.Stats()
	assert.Equal(t, 0, stats["shard_id"])
	assert.Equal(t, "primary", stats["primary_id"])
	assert.Equal(t, "localhost:9995", stats["primary_addr"])
	assert.False(t, stats["connected"].(bool))
}
