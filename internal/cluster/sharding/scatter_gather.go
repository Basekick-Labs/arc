package sharding

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
)

// ScatterGatherConfig holds configuration for scatter-gather query coordination.
type ScatterGatherConfig struct {
	// ShardMap provides shard-to-node mappings
	ShardMap *ShardMap

	// LocalNode is this node
	LocalNode *cluster.Node

	// HTTPClient for forwarding requests
	HTTPClient *http.Client

	// Timeout for individual shard queries
	Timeout time.Duration

	// MaxConcurrentShards limits parallel shard queries (0 = unlimited)
	MaxConcurrentShards int

	// Logger for scatter-gather events
	Logger zerolog.Logger
}

// ScatterGather coordinates queries across multiple shards.
// Used when a query needs data from multiple databases that live on different shards.
type ScatterGather struct {
	cfg    *ScatterGatherConfig
	logger zerolog.Logger
}

// ShardQueryResult represents the result from a single shard query.
type ShardQueryResult struct {
	ShardID  int                 `json:"shard_id"`
	NodeID   string              `json:"node_id"`
	Data     json.RawMessage     `json:"data,omitempty"`
	Error    string              `json:"error,omitempty"`
	Duration time.Duration       `json:"duration_ms"`
	Status   int                 `json:"status"`
}

// ScatterGatherResult represents the combined result from all shards.
type ScatterGatherResult struct {
	Results       []*ShardQueryResult `json:"results"`
	TotalShards   int                 `json:"total_shards"`
	SuccessShards int                 `json:"success_shards"`
	FailedShards  int                 `json:"failed_shards"`
	TotalDuration time.Duration       `json:"total_duration_ms"`
}

// NewScatterGather creates a new scatter-gather coordinator.
func NewScatterGather(cfg *ScatterGatherConfig) *ScatterGather {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{
			Timeout: cfg.Timeout,
		}
	}

	return &ScatterGather{
		cfg:    cfg,
		logger: cfg.Logger.With().Str("component", "scatter-gather").Logger(),
	}
}

// Query executes a query across multiple shards and combines the results.
// The databases parameter specifies which databases to query.
func (sg *ScatterGather) Query(ctx context.Context, databases []string, buildRequest func(shardID int, nodeAddr string) (*http.Request, error)) (*ScatterGatherResult, error) {
	if buildRequest == nil {
		return nil, fmt.Errorf("buildRequest callback is required")
	}

	start := time.Now()

	// Determine which shards need to be queried
	shardDBs := sg.groupDatabasesByShard(databases)

	sg.logger.Debug().
		Int("total_databases", len(databases)).
		Int("total_shards", len(shardDBs)).
		Msg("Starting scatter-gather query")

	// Create a semaphore for limiting concurrency
	var sem chan struct{}
	if sg.cfg.MaxConcurrentShards > 0 {
		sem = make(chan struct{}, sg.cfg.MaxConcurrentShards)
	}

	// Execute queries in parallel
	results := make([]*ShardQueryResult, 0, len(shardDBs))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for shardID, dbs := range shardDBs {
		wg.Add(1)
		go func(shardID int, dbs []string) {
			defer wg.Done()

			// Acquire semaphore
			if sem != nil {
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					return
				}
			}

			result := sg.queryOneShard(ctx, shardID, dbs, buildRequest)

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(shardID, dbs)
	}

	wg.Wait()

	// Build combined result
	combinedResult := &ScatterGatherResult{
		Results:       results,
		TotalShards:   len(shardDBs),
		TotalDuration: time.Since(start),
	}

	for _, r := range results {
		if r.Error == "" && r.Status >= 200 && r.Status < 300 {
			combinedResult.SuccessShards++
		} else {
			combinedResult.FailedShards++
		}
	}

	sg.logger.Debug().
		Int("total_shards", combinedResult.TotalShards).
		Int("success_shards", combinedResult.SuccessShards).
		Int("failed_shards", combinedResult.FailedShards).
		Dur("total_duration", combinedResult.TotalDuration).
		Msg("Scatter-gather query completed")

	return combinedResult, nil
}

// QueryAllShards executes a query on all shards (for queries without specific database targets).
func (sg *ScatterGather) QueryAllShards(ctx context.Context, buildRequest func(shardID int, nodeAddr string) (*http.Request, error)) (*ScatterGatherResult, error) {
	if buildRequest == nil {
		return nil, fmt.Errorf("buildRequest callback is required")
	}

	start := time.Now()
	numShards := sg.cfg.ShardMap.NumShards()

	sg.logger.Debug().
		Int("total_shards", numShards).
		Msg("Starting scatter-gather query to all shards")

	// Create a semaphore for limiting concurrency
	var sem chan struct{}
	if sg.cfg.MaxConcurrentShards > 0 {
		sem = make(chan struct{}, sg.cfg.MaxConcurrentShards)
	}

	// Execute queries in parallel
	results := make([]*ShardQueryResult, 0, numShards)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for shardID := 0; shardID < numShards; shardID++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()

			// Acquire semaphore
			if sem != nil {
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					return
				}
			}

			result := sg.queryOneShard(ctx, shardID, nil, buildRequest)

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(shardID)
	}

	wg.Wait()

	// Build combined result
	combinedResult := &ScatterGatherResult{
		Results:       results,
		TotalShards:   numShards,
		TotalDuration: time.Since(start),
	}

	for _, r := range results {
		if r.Error == "" && r.Status >= 200 && r.Status < 300 {
			combinedResult.SuccessShards++
		} else {
			combinedResult.FailedShards++
		}
	}

	sg.logger.Debug().
		Int("total_shards", combinedResult.TotalShards).
		Int("success_shards", combinedResult.SuccessShards).
		Int("failed_shards", combinedResult.FailedShards).
		Dur("total_duration", combinedResult.TotalDuration).
		Msg("Scatter-gather query to all shards completed")

	return combinedResult, nil
}

// queryOneShard executes a query on a single shard.
func (sg *ScatterGather) queryOneShard(ctx context.Context, shardID int, databases []string, buildRequest func(shardID int, nodeAddr string) (*http.Request, error)) *ShardQueryResult {
	start := time.Now()

	result := &ShardQueryResult{
		ShardID: shardID,
	}

	// Select a node for this shard (primary or replica)
	node := sg.cfg.ShardMap.SelectNode(shardID)
	if node == nil {
		result.Error = "no node available for shard"
		result.Status = http.StatusServiceUnavailable
		result.Duration = time.Since(start)
		return result
	}

	result.NodeID = node.ID

	// Check if this is the local node
	if sg.cfg.LocalNode != nil && node.ID == sg.cfg.LocalNode.ID {
		result.Error = "local_node"
		result.Status = http.StatusOK
		result.Duration = time.Since(start)
		return result
	}

	// Build request
	nodeAddr := node.APIAddress
	if nodeAddr == "" {
		nodeAddr = node.Address
	}

	req, err := buildRequest(shardID, nodeAddr)
	if err != nil {
		result.Error = fmt.Sprintf("build request: %v", err)
		result.Status = http.StatusInternalServerError
		result.Duration = time.Since(start)
		return result
	}

	// Set context with timeout
	reqCtx, cancel := context.WithTimeout(ctx, sg.cfg.Timeout)
	defer cancel()
	req = req.WithContext(reqCtx)

	// Add scatter-gather header to prevent loops
	req.Header.Set("X-Arc-Scatter-Gather", "true")

	// Execute request
	resp, err := sg.cfg.HTTPClient.Do(req)
	if err != nil {
		result.Error = fmt.Sprintf("request failed: %v", err)
		result.Status = http.StatusBadGateway
		result.Duration = time.Since(start)
		return result
	}
	defer resp.Body.Close()

	result.Status = resp.StatusCode
	result.Duration = time.Since(start)

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		result.Error = fmt.Sprintf("read response: %v", err)
		return result
	}

	result.Data = body
	return result
}

// groupDatabasesByShard groups databases by their shard ID.
func (sg *ScatterGather) groupDatabasesByShard(databases []string) map[int][]string {
	result := make(map[int][]string)
	for _, db := range databases {
		shardID := sg.cfg.ShardMap.GetShard(db)
		result[shardID] = append(result[shardID], db)
	}
	return result
}

// MergeResults merges query results from multiple shards.
// The merge function is provided by the caller since merging logic depends on the query type.
func (sg *ScatterGather) MergeResults(results *ScatterGatherResult, mergeFunc func(accumulated, current json.RawMessage) (json.RawMessage, error)) (json.RawMessage, error) {
	if len(results.Results) == 0 {
		return nil, nil
	}

	var accumulated json.RawMessage
	for _, result := range results.Results {
		// Skip failed shards
		if result.Error != "" && result.Error != "local_node" {
			continue
		}

		// Skip local node markers (caller handles these separately)
		if result.Error == "local_node" {
			continue
		}

		if len(result.Data) == 0 {
			continue
		}

		if accumulated == nil {
			accumulated = result.Data
		} else {
			merged, err := mergeFunc(accumulated, result.Data)
			if err != nil {
				return nil, fmt.Errorf("merge shard %d: %w", result.ShardID, err)
			}
			accumulated = merged
		}
	}

	return accumulated, nil
}

// GetLocalShards returns the list of shards that should be handled locally.
func (sg *ScatterGather) GetLocalShards(results *ScatterGatherResult) []int {
	localShards := make([]int, 0)
	for _, result := range results.Results {
		if result.Error == "local_node" {
			localShards = append(localShards, result.ShardID)
		}
	}
	return localShards
}

// NeedsFanout returns true if the query needs to be fanned out to multiple shards.
func (sg *ScatterGather) NeedsFanout(databases []string) bool {
	if len(databases) == 0 {
		return true // No specific databases = query all shards
	}

	// Check if all databases are on the same shard
	firstShard := -1
	for _, db := range databases {
		shardID := sg.cfg.ShardMap.GetShard(db)
		if firstShard == -1 {
			firstShard = shardID
		} else if shardID != firstShard {
			return true // Multiple shards needed
		}
	}

	return false // All on same shard
}

// Stats returns scatter-gather statistics.
func (sg *ScatterGather) Stats() map[string]interface{} {
	return map[string]interface{}{
		"num_shards":           sg.cfg.ShardMap.NumShards(),
		"max_concurrent":       sg.cfg.MaxConcurrentShards,
		"timeout_ms":           sg.cfg.Timeout.Milliseconds(),
	}
}

// TwoStageAggregation executes a query using two-stage distributed aggregation.
// The aggregation rewriter transforms the query to compute partial aggregates on shards,
// then combines them at the coordinator.
// Returns the final merged result as JSON.
func (sg *ScatterGather) TwoStageAggregation(
	ctx context.Context,
	databases []string,
	originalSQL string,
	rewriter *AggregationRewriter,
	buildRequest func(shardID int, nodeAddr string, shardSQL string) (*http.Request, error),
) (json.RawMessage, error) {
	// Check if query can use two-stage
	if !rewriter.CanRewrite(originalSQL) {
		return nil, fmt.Errorf("query not suitable for two-stage aggregation")
	}

	// Rewrite the query for partial aggregation
	rewrite, err := rewriter.Rewrite(originalSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to rewrite query: %w", err)
	}

	sg.logger.Info().
		Str("original_sql", originalSQL).
		Str("shard_sql", rewrite.ShardQuery).
		Int("final_aggregations", len(rewrite.FinalAggregations)).
		Strs("group_by", rewrite.GroupByColumns).
		Msg("Two-stage aggregation: query rewritten")

	// Execute on all shards with the rewritten query
	result, err := sg.Query(ctx, databases, func(shardID int, nodeAddr string) (*http.Request, error) {
		return buildRequest(shardID, nodeAddr, rewrite.ShardQuery)
	})
	if err != nil {
		return nil, fmt.Errorf("scatter phase failed: %w", err)
	}

	// Collect successful shard results
	var partialResults []json.RawMessage
	for _, r := range result.Results {
		if r.Error == "" && len(r.Data) > 0 {
			partialResults = append(partialResults, r.Data)
		}
	}

	if len(partialResults) == 0 {
		// Return empty result
		return json.Marshal(map[string]interface{}{
			"columns": []string{},
			"data":    [][]interface{}{},
		})
	}

	// Merge partial results at coordinator
	merged, err := rewriter.MergeAggregatedResults(rewrite, partialResults)
	if err != nil {
		return nil, fmt.Errorf("gather phase failed: %w", err)
	}

	sg.logger.Info().
		Int("shards_queried", result.TotalShards).
		Int("shards_success", result.SuccessShards).
		Int("partial_results", len(partialResults)).
		Msg("Two-stage aggregation: merge completed")

	return merged, nil
}
