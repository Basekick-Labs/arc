package sharding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScatterGather_NeedsFanout(t *testing.T) {
	sm := NewShardMap(3)

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap: sm,
		Logger:   zerolog.Nop(),
	})

	tests := []struct {
		name      string
		databases []string
		expected  bool
	}{
		{
			name:      "empty databases - needs fanout",
			databases: []string{},
			expected:  true,
		},
		{
			name:      "single database",
			databases: []string{"mydb"},
			expected:  false,
		},
		{
			name:      "multiple databases same shard",
			databases: []string{"db1", "db2"}, // May or may not be same shard
			expected:  false,                  // Depends on hash - will verify below
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "multiple databases same shard" {
				// Find two databases that hash to the same shard
				var sameShardDbs []string
				shardDBs := make(map[int]string)
				for i := 0; i < 100; i++ {
					db := "testdb" + string(rune('a'+i%26)) + string(rune('0'+i%10))
					shard := sm.GetShard(db)
					if existing, exists := shardDBs[shard]; exists {
						sameShardDbs = []string{existing, db}
						break
					}
					shardDBs[shard] = db
				}
				if len(sameShardDbs) == 2 {
					assert.False(t, sg.NeedsFanout(sameShardDbs))
				}
				return
			}
			result := sg.NeedsFanout(tt.databases)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestScatterGather_GroupDatabasesByShard(t *testing.T) {
	sm := NewShardMap(3)

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap: sm,
		Logger:   zerolog.Nop(),
	})

	// Create databases that will hash to different shards
	databases := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		databases = append(databases, "database_"+string(rune('a'+i)))
	}

	grouped := sg.groupDatabasesByShard(databases)

	// Verify all databases are grouped
	totalDBs := 0
	for _, dbs := range grouped {
		totalDBs += len(dbs)
	}
	assert.Equal(t, len(databases), totalDBs)

	// Verify each database is in the correct shard
	for shardID, dbs := range grouped {
		for _, db := range dbs {
			assert.Equal(t, shardID, sm.GetShard(db))
		}
	}
}

func TestScatterGather_QueryAllShards(t *testing.T) {
	// Create test servers for each shard
	responses := make(map[int]*httptest.Server)
	for i := 0; i < 3; i++ {
		shardID := i
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Verify scatter-gather header
			assert.Equal(t, "true", r.Header.Get("X-Arc-Scatter-Gather"))

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"shard_id": shardID,
				"data":     []int{1, 2, 3},
			})
		}))
		responses[i] = server
		defer server.Close()
	}

	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	// Set up shard assignments with remote nodes
	for i := 0; i < 3; i++ {
		node := &cluster.Node{
			ID:         "node-" + string(rune('a'+i)),
			APIAddress: responses[i].Listener.Addr().String(),
		}
		node.UpdateState(cluster.StateHealthy)
		sm.SetPrimary(i, node)
	}

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	ctx := context.Background()
	result, err := sg.QueryAllShards(ctx, func(shardID int, nodeAddr string) (*http.Request, error) {
		return http.NewRequest("GET", "http://"+nodeAddr+"/query", nil)
	})

	require.NoError(t, err)
	assert.Equal(t, 3, result.TotalShards)
	assert.Equal(t, 3, result.SuccessShards)
	assert.Equal(t, 0, result.FailedShards)
}

func TestScatterGather_QuerySpecificDatabases(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"result": "success",
		})
	}))
	defer server.Close()

	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}

	// Set up all shards with the same test server for simplicity
	for i := 0; i < 3; i++ {
		node := &cluster.Node{
			ID:         "node-" + string(rune('a'+i)),
			APIAddress: server.Listener.Addr().String(),
		}
		node.UpdateState(cluster.StateHealthy)
		sm.SetPrimary(i, node)
	}

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	// Query specific databases
	databases := []string{"db1", "db2", "db3"}

	ctx := context.Background()
	result, err := sg.Query(ctx, databases, func(shardID int, nodeAddr string) (*http.Request, error) {
		return http.NewRequest("GET", "http://"+nodeAddr+"/query", nil)
	})

	require.NoError(t, err)
	assert.GreaterOrEqual(t, result.TotalShards, 1)
	assert.Equal(t, result.TotalShards, result.SuccessShards)
}

func TestScatterGather_LocalNodeHandling(t *testing.T) {
	sm := NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	// Make local node the primary for all shards
	for i := 0; i < 3; i++ {
		sm.SetPrimary(i, localNode)
	}

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	ctx := context.Background()
	result, err := sg.QueryAllShards(ctx, func(shardID int, nodeAddr string) (*http.Request, error) {
		return http.NewRequest("GET", "http://"+nodeAddr+"/query", nil)
	})

	require.NoError(t, err)
	assert.Equal(t, 3, result.TotalShards)

	// All shards should be marked as local
	localShards := sg.GetLocalShards(result)
	assert.Len(t, localShards, 3)
}

func TestScatterGather_FailedShard(t *testing.T) {
	// Create test server that fails
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "internal error"}`))
	}))
	defer failServer.Close()

	// Create test server that succeeds
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"data": "success"})
	}))
	defer successServer.Close()

	sm := NewShardMap(2)
	localNode := &cluster.Node{ID: "local-node"}

	// Shard 0 fails, shard 1 succeeds
	failNode := &cluster.Node{ID: "fail-node", APIAddress: failServer.Listener.Addr().String()}
	failNode.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(0, failNode)

	successNode := &cluster.Node{ID: "success-node", APIAddress: successServer.Listener.Addr().String()}
	successNode.UpdateState(cluster.StateHealthy)
	sm.SetPrimary(1, successNode)

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	ctx := context.Background()
	result, err := sg.QueryAllShards(ctx, func(shardID int, nodeAddr string) (*http.Request, error) {
		return http.NewRequest("GET", "http://"+nodeAddr+"/query", nil)
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.TotalShards)
	assert.Equal(t, 1, result.SuccessShards)
	assert.Equal(t, 1, result.FailedShards)
}

func TestScatterGather_NoNodeAvailable(t *testing.T) {
	sm := NewShardMap(3)
	// Don't assign any primaries

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap: sm,
		Timeout:  5 * time.Second,
		Logger:   zerolog.Nop(),
	})

	ctx := context.Background()
	result, err := sg.QueryAllShards(ctx, func(shardID int, nodeAddr string) (*http.Request, error) {
		return http.NewRequest("GET", "http://"+nodeAddr+"/query", nil)
	})

	require.NoError(t, err)
	assert.Equal(t, 3, result.TotalShards)
	assert.Equal(t, 0, result.SuccessShards)
	assert.Equal(t, 3, result.FailedShards)

	// Verify error messages
	for _, r := range result.Results {
		assert.Contains(t, r.Error, "no node available")
	}
}

func TestScatterGather_MergeResults(t *testing.T) {
	sm := NewShardMap(3)

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap: sm,
		Logger:   zerolog.Nop(),
	})

	// Create sample results
	results := &ScatterGatherResult{
		Results: []*ShardQueryResult{
			{ShardID: 0, Data: json.RawMessage(`[1, 2, 3]`), Status: 200},
			{ShardID: 1, Data: json.RawMessage(`[4, 5, 6]`), Status: 200},
			{ShardID: 2, Error: "failed", Status: 500},
		},
		TotalShards:   3,
		SuccessShards: 2,
		FailedShards:  1,
	}

	// Merge function that concatenates arrays
	mergeFunc := func(accumulated, current json.RawMessage) (json.RawMessage, error) {
		var arr1, arr2 []int
		if err := json.Unmarshal(accumulated, &arr1); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(current, &arr2); err != nil {
			return nil, err
		}
		merged := append(arr1, arr2...)
		return json.Marshal(merged)
	}

	merged, err := sg.MergeResults(results, mergeFunc)
	require.NoError(t, err)

	var result []int
	err = json.Unmarshal(merged, &result)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, result)
}

func TestScatterGather_ConcurrencyLimit(t *testing.T) {
	// Create a server that tracks concurrent requests
	var concurrent, maxConcurrent int
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		concurrent++
		if concurrent > maxConcurrent {
			maxConcurrent = concurrent
		}
		mu.Unlock()

		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		concurrent--
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sm := NewShardMap(5)
	localNode := &cluster.Node{ID: "local-node"}

	// Set up 5 shards
	for i := 0; i < 5; i++ {
		node := &cluster.Node{
			ID:         "node-" + string(rune('a'+i)),
			APIAddress: server.Listener.Addr().String(),
		}
		node.UpdateState(cluster.StateHealthy)
		sm.SetPrimary(i, node)
	}

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap:            sm,
		LocalNode:           localNode,
		Timeout:             5 * time.Second,
		MaxConcurrentShards: 2, // Limit to 2 concurrent
		Logger:              zerolog.Nop(),
	})

	ctx := context.Background()
	result, err := sg.QueryAllShards(ctx, func(shardID int, nodeAddr string) (*http.Request, error) {
		return http.NewRequest("GET", "http://"+nodeAddr+"/query", nil)
	})

	require.NoError(t, err)
	assert.Equal(t, 5, result.TotalShards)
	assert.Equal(t, 5, result.SuccessShards)
	assert.LessOrEqual(t, maxConcurrent, 2, "Max concurrent should not exceed limit")
}

func TestScatterGather_Stats(t *testing.T) {
	sm := NewShardMap(3)

	sg := NewScatterGather(&ScatterGatherConfig{
		ShardMap:            sm,
		Timeout:             10 * time.Second,
		MaxConcurrentShards: 5,
		Logger:              zerolog.Nop(),
	})

	stats := sg.Stats()
	assert.Equal(t, 3, stats["num_shards"])
	assert.Equal(t, 5, stats["max_concurrent"])
	assert.Equal(t, int64(10000), stats["timeout_ms"])
}
