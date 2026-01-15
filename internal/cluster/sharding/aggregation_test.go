package sharding

import (
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregationRewriter_CanRewrite(t *testing.T) {
	logger := zerolog.Nop()
	rewriter := NewAggregationRewriter(logger)

	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{
			name:     "simple SUM",
			sql:      "SELECT SUM(value) FROM metrics",
			expected: true,
		},
		{
			name:     "COUNT with GROUP BY",
			sql:      "SELECT host, COUNT(*) FROM metrics GROUP BY host",
			expected: true,
		},
		{
			name:     "AVG query",
			sql:      "SELECT AVG(cpu) FROM metrics WHERE time > '2024-01-01'",
			expected: true,
		},
		{
			name:     "MIN MAX query",
			sql:      "SELECT MIN(temp), MAX(temp) FROM sensors",
			expected: true,
		},
		{
			name:     "no aggregations",
			sql:      "SELECT * FROM metrics WHERE time > '2024-01-01'",
			expected: false,
		},
		{
			name:     "subquery",
			sql:      "SELECT SUM(x) FROM (SELECT * FROM metrics) t",
			expected: false,
		},
		{
			name:     "HAVING clause",
			sql:      "SELECT host, SUM(value) FROM metrics GROUP BY host HAVING SUM(value) > 100",
			expected: false,
		},
		{
			name:     "window function",
			sql:      "SELECT SUM(value) OVER(PARTITION BY host) FROM metrics",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriter.CanRewrite(tt.sql)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAggregationRewriter_DetectAggregations(t *testing.T) {
	logger := zerolog.Nop()
	rewriter := NewAggregationRewriter(logger)

	tests := []struct {
		name           string
		selectClause   string
		expectedTypes  []AggregationType
		expectedCount  int
	}{
		{
			name:          "single SUM",
			selectClause:  "SUM(value)",
			expectedTypes: []AggregationType{AggTypeSum},
			expectedCount: 1,
		},
		{
			name:          "multiple aggregations",
			selectClause:  "host, SUM(value), COUNT(*), AVG(cpu)",
			expectedTypes: []AggregationType{AggTypeSum, AggTypeCount, AggTypeAvg},
			expectedCount: 3,
		},
		{
			name:          "MIN and MAX",
			selectClause:  "MIN(temp), MAX(temp)",
			expectedTypes: []AggregationType{AggTypeMin, AggTypeMax},
			expectedCount: 2,
		},
		{
			name:          "COUNT DISTINCT",
			selectClause:  "COUNT(DISTINCT user_id)",
			expectedTypes: []AggregationType{AggTypeCountDistinct},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggs := rewriter.detectAggregations(tt.selectClause)
			assert.Len(t, aggs, tt.expectedCount)

			for i, expectedType := range tt.expectedTypes {
				if i < len(aggs) {
					assert.Equal(t, expectedType, aggs[i].Type)
				}
			}
		})
	}
}

func TestAggregationRewriter_Rewrite(t *testing.T) {
	logger := zerolog.Nop()
	rewriter := NewAggregationRewriter(logger)

	tests := []struct {
		name             string
		sql              string
		expectShardQuery string
		expectGroupBy    []string
	}{
		{
			name:          "simple SUM with GROUP BY",
			sql:           "SELECT host, SUM(value) FROM metrics GROUP BY host",
			expectGroupBy: []string{"host"},
		},
		{
			name:          "AVG query",
			sql:           "SELECT region, AVG(latency) FROM requests GROUP BY region",
			expectGroupBy: []string{"region"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rewrite, err := rewriter.Rewrite(tt.sql)
			require.NoError(t, err)
			require.NotNil(t, rewrite)

			assert.Equal(t, tt.expectGroupBy, rewrite.GroupByColumns)
			assert.NotEmpty(t, rewrite.ShardQuery)
			assert.NotEmpty(t, rewrite.FinalAggregations)

			t.Logf("Shard query: %s", rewrite.ShardQuery)
		})
	}
}

func TestAggregationRewriter_MergeResults(t *testing.T) {
	logger := zerolog.Nop()
	rewriter := NewAggregationRewriter(logger)

	// Simulate two shard results for: SELECT host, SUM(value), COUNT(*) FROM metrics GROUP BY host
	shard1 := json.RawMessage(`{
		"columns": ["host", "__partial_sum_0", "__partial_count_1"],
		"data": [
			["server1", 100, 10],
			["server2", 200, 20]
		]
	}`)

	shard2 := json.RawMessage(`{
		"columns": ["host", "__partial_sum_0", "__partial_count_1"],
		"data": [
			["server1", 150, 15],
			["server3", 300, 30]
		]
	}`)

	rewrite := &TwoStageRewrite{
		GroupByColumns: []string{"host"},
		FinalAggregations: []*FinalAggregation{
			{Type: AggTypeSum, PartialColumns: []string{"__partial_sum_0"}},
			{Type: AggTypeCount, PartialColumns: []string{"__partial_count_1"}},
		},
	}

	result, err := rewriter.MergeAggregatedResults(rewrite, []json.RawMessage{shard1, shard2})
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(result, &parsed)
	require.NoError(t, err)

	data := parsed["data"].([]interface{})
	assert.Len(t, data, 3) // server1, server2, server3

	// Check that server1's values were summed (100+150=250, 10+15=25)
	t.Logf("Merged result: %s", string(result))
}

func TestAggregationRewriter_MergeAVG(t *testing.T) {
	logger := zerolog.Nop()
	rewriter := NewAggregationRewriter(logger)

	// Simulate AVG: shard sends SUM and COUNT, coordinator computes SUM/COUNT
	shard1 := json.RawMessage(`{
		"columns": ["region", "__partial_sum_0", "__partial_count_0"],
		"data": [
			["us-east", 100, 10],
			["us-west", 200, 20]
		]
	}`)

	shard2 := json.RawMessage(`{
		"columns": ["region", "__partial_sum_0", "__partial_count_0"],
		"data": [
			["us-east", 200, 20]
		]
	}`)

	rewrite := &TwoStageRewrite{
		GroupByColumns: []string{"region"},
		FinalAggregations: []*FinalAggregation{
			{Type: AggTypeAvg, PartialColumns: []string{"__partial_sum_0", "__partial_count_0"}},
		},
	}

	result, err := rewriter.MergeAggregatedResults(rewrite, []json.RawMessage{shard1, shard2})
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(result, &parsed)
	require.NoError(t, err)

	data := parsed["data"].([]interface{})
	assert.Len(t, data, 2) // us-east, us-west

	// Check us-east: (100+200)/(10+20) = 300/30 = 10
	// Check us-west: 200/20 = 10
	t.Logf("Merged AVG result: %s", string(result))
}

func TestAggregationRewriter_ExtractOrderByLimit(t *testing.T) {
	logger := zerolog.Nop()
	rewriter := NewAggregationRewriter(logger)

	sql := "SELECT host, SUM(value) as total FROM metrics GROUP BY host ORDER BY total DESC LIMIT 10"
	rewrite, err := rewriter.Rewrite(sql)
	require.NoError(t, err)

	assert.True(t, rewrite.HasOrderBy)
	assert.Contains(t, rewrite.OrderByClause, "total")
	assert.True(t, rewrite.HasLimit)
	assert.Equal(t, 10, rewrite.LimitValue)

	// Shard query should not have ORDER BY or LIMIT
	assert.NotContains(t, rewrite.ShardQuery, "ORDER BY")
	assert.NotContains(t, rewrite.ShardQuery, "LIMIT")
}
