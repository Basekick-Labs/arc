package sharding

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardRouterRouteWriteLocalPrimary(t *testing.T) {
	sm := NewShardMap(3)

	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	// Make local node primary for shard 0
	sm.SetPrimary(0, localNode)

	router := NewShardRouter(&ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	// Create request for database that hashes to shard 0
	// We need to find a database name that hashes to shard 0
	var database string
	for i := 0; i < 100; i++ {
		testDB := "test_db_" + string(rune('a'+i))
		if sm.GetShard(testDB) == 0 {
			database = testDB
			break
		}
	}
	require.NotEmpty(t, database, "Could not find database that hashes to shard 0")

	req := httptest.NewRequest("POST", "/api/v1/write", strings.NewReader("data"))
	req.Header.Set("X-Arc-Database", database)

	resp, err := router.RouteWrite(context.Background(), req)

	// Should return ErrLocalNodeCanHandle since we're the primary
	assert.ErrorIs(t, err, ErrLocalNodeCanHandle)
	assert.Nil(t, resp)

	// Check stats
	stats := router.Stats()
	assert.Equal(t, int64(1), stats["local_writes"])
}

func TestShardRouterRouteWriteForward(t *testing.T) {
	// Create a test server to forward to
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify forwarding headers
		assert.NotEmpty(t, r.Header.Get("X-Arc-Forwarded-By"))
		assert.Equal(t, "true", r.Header.Get("X-Arc-Shard-Routed"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	}))
	defer targetServer.Close()

	sm := NewShardMap(3)

	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	// Find a shard that local node is NOT primary for
	remoteNode := &cluster.Node{ID: "remote-node"}
	remoteNode.APIAddress = targetServer.Listener.Addr().String()
	remoteNode.UpdateState(cluster.StateHealthy)

	// Make remote node primary for all shards
	for i := 0; i < 3; i++ {
		sm.SetPrimary(i, remoteNode)
	}

	router := NewShardRouter(&ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	req := httptest.NewRequest("POST", "/api/v1/write", strings.NewReader("data"))
	req.Header.Set("X-Arc-Database", "any_database")

	resp, err := router.RouteWrite(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Check stats
	stats := router.Stats()
	assert.Equal(t, int64(1), stats["forwarded_writes"])
}

func TestShardRouterRouteWriteNoDatabaseHeader(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	router := NewShardRouter(&ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	req := httptest.NewRequest("POST", "/api/v1/write", strings.NewReader("data"))
	// No X-Arc-Database header

	resp, err := router.RouteWrite(context.Background(), req)

	assert.ErrorIs(t, err, ErrNoDatabaseHeader)
	assert.Nil(t, resp)
}

func TestShardRouterRouteWriteNoPrimary(t *testing.T) {
	sm := NewShardMap(3)
	// No primaries assigned

	localNode := &cluster.Node{ID: "local-node"}

	router := NewShardRouter(&ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	req := httptest.NewRequest("POST", "/api/v1/write", strings.NewReader("data"))
	req.Header.Set("X-Arc-Database", "mydb")

	resp, err := router.RouteWrite(context.Background(), req)

	assert.ErrorIs(t, err, ErrNoShardPrimary)
	assert.Nil(t, resp)
}

func TestShardRouterRouteQuery(t *testing.T) {
	sm := NewShardMap(3)

	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	// Make local node a replica for shard 0
	primaryNode := &cluster.Node{ID: "primary-node"}
	primaryNode.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(0, primaryNode)
	sm.AddReplica(0, localNode)

	router := NewShardRouter(&ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	// Find database that hashes to shard 0
	var database string
	for i := 0; i < 100; i++ {
		testDB := "query_db_" + string(rune('a'+i))
		if sm.GetShard(testDB) == 0 {
			database = testDB
			break
		}
	}
	require.NotEmpty(t, database)

	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader("SELECT * FROM cpu"))

	resp, err := router.RouteQuery(context.Background(), database, req)

	// Should return ErrLocalNodeCanHandle since we're a replica
	assert.ErrorIs(t, err, ErrLocalNodeCanHandle)
	assert.Nil(t, resp)

	stats := router.Stats()
	assert.Equal(t, int64(1), stats["local_queries"])
}

func TestShardRouterCanHandleLocally(t *testing.T) {
	sm := NewShardMap(3)

	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	otherNode := &cluster.Node{ID: "other-node"}
	otherNode.UpdateState(cluster.StateHealthy)

	router := NewShardRouter(&ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	// Find databases for different shards
	var db0, db1 string
	for i := 0; i < 100; i++ {
		testDB := "can_handle_" + string(rune('a'+i))
		shard := sm.GetShard(testDB)
		if shard == 0 && db0 == "" {
			db0 = testDB
		}
		if shard == 1 && db1 == "" {
			db1 = testDB
		}
		if db0 != "" && db1 != "" {
			break
		}
	}
	require.NotEmpty(t, db0)
	require.NotEmpty(t, db1)

	// local is primary for shard 0, other is primary for shard 1
	sm.SetPrimary(0, localNode)
	sm.SetPrimary(1, otherNode)
	sm.AddReplica(1, localNode) // local is also replica for shard 1

	// Test writes (must be primary)
	shardID, canHandle := router.CanHandleLocally(db0, true)
	assert.Equal(t, 0, shardID)
	assert.True(t, canHandle, "Should handle writes for db0 (local is primary)")

	shardID, canHandle = router.CanHandleLocally(db1, true)
	assert.Equal(t, 1, shardID)
	assert.False(t, canHandle, "Should NOT handle writes for db1 (local is replica)")

	// Test reads (can be primary or replica)
	shardID, canHandle = router.CanHandleLocally(db0, false)
	assert.Equal(t, 0, shardID)
	assert.True(t, canHandle, "Should handle reads for db0 (local is primary)")

	shardID, canHandle = router.CanHandleLocally(db1, false)
	assert.Equal(t, 1, shardID)
	assert.True(t, canHandle, "Should handle reads for db1 (local is replica)")
}

func TestShardRouterGetShardForDatabase(t *testing.T) {
	sm := NewShardMap(5)

	localNode := &cluster.Node{ID: "local-node"}

	router := NewShardRouter(&ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	// Same database should always return same shard
	shard1 := router.GetShardForDatabase("mydb")
	shard2 := router.GetShardForDatabase("mydb")
	assert.Equal(t, shard1, shard2)

	// Shard should be in valid range
	assert.GreaterOrEqual(t, shard1, 0)
	assert.Less(t, shard1, 5)
}

func TestShardRouterStats(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}
	sm.SetPrimary(0, localNode)

	router := NewShardRouter(&ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	stats := router.Stats()

	assert.Contains(t, stats, "forwarded_writes")
	assert.Contains(t, stats, "forwarded_queries")
	assert.Contains(t, stats, "local_writes")
	assert.Contains(t, stats, "local_queries")
	assert.Contains(t, stats, "errors")
	assert.Contains(t, stats, "shard_map")
}
