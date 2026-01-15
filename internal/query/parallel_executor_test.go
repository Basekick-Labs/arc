package query

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestDefaultParallelConfig(t *testing.T) {
	cfg := DefaultParallelConfig()
	assert.Equal(t, 4, cfg.MaxConcurrentPartitions)
	assert.Equal(t, 3, cfg.MinPartitionsForParallel)
}

func TestParallelExecutor_ShouldUseParallel(t *testing.T) {
	logger := zerolog.Nop()
	executor := NewParallelExecutor(nil, DefaultParallelConfig(), logger)

	tests := []struct {
		name           string
		partitionCount int
		expected       bool
	}{
		{"1 partition", 1, false},
		{"2 partitions", 2, false},
		{"3 partitions (threshold)", 3, true},
		{"4 partitions", 4, true},
		{"10 partitions", 10, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.ShouldUseParallel(tt.partitionCount)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParallelExecutor_ShouldUseParallel_CustomConfig(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &ParallelExecutorConfig{
		MaxConcurrentPartitions:  8,
		MinPartitionsForParallel: 5,
	}
	executor := NewParallelExecutor(nil, cfg, logger)

	assert.False(t, executor.ShouldUseParallel(4))
	assert.True(t, executor.ShouldUseParallel(5))
	assert.True(t, executor.ShouldUseParallel(10))
}

func TestParallelExecutor_BuildPartitionQuery(t *testing.T) {
	logger := zerolog.Nop()
	executor := NewParallelExecutor(nil, DefaultParallelConfig(), logger)

	template := "SELECT * FROM {PARTITION_PATH} WHERE time > '2024-01-01'"
	path := "/data/db/table/2024/01/01/*.parquet"
	options := "union_by_name=true"

	result := executor.buildPartitionQuery(template, path, options)

	expected := "SELECT * FROM read_parquet('/data/db/table/2024/01/01/*.parquet', union_by_name=true) WHERE time > '2024-01-01'"
	assert.Equal(t, expected, result)
}

func TestMergedRowIterator_NoRows(t *testing.T) {
	logger := zerolog.Nop()

	// Results with columns but no actual rows - should still fail as Rows is nil
	results := []*PartitionResult{
		{
			Error:   nil,
			Columns: []string{"time", "value", "host"},
			Rows:    nil, // No actual rows
			Path:    "/path/1",
		},
	}

	// Should fail because we need Rows to be non-nil for a "successful" result
	_, err := NewMergedRowIterator(results, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no successful partition results")
}

func TestMergedRowIterator_NoSuccessfulResults(t *testing.T) {
	logger := zerolog.Nop()

	results := []*PartitionResult{
		{
			Error: assert.AnError,
			Path:  "/path/1",
		},
		{
			Error: assert.AnError,
			Path:  "/path/2",
		},
	}

	_, err := NewMergedRowIterator(results, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no successful partition results")
}

func TestMergedRowIterator_AllErrors(t *testing.T) {
	logger := zerolog.Nop()

	// All partitions have errors
	results := []*PartitionResult{
		{
			Error:   assert.AnError,
			Columns: []string{"time", "value"},
			Path:    "/path/1",
		},
		{
			Error:   assert.AnError,
			Columns: []string{"time", "value", "host"},
			Path:    "/path/2",
		},
	}

	_, err := NewMergedRowIterator(results, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no successful partition results")
}
