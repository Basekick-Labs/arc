package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// ParallelExecutorConfig configures the parallel partition executor.
type ParallelExecutorConfig struct {
	// MaxConcurrentPartitions limits concurrent partition queries.
	// Set to 0 for no limit (uses all available connections).
	MaxConcurrentPartitions int

	// MinPartitionsForParallel is the minimum number of partitions
	// required to trigger parallel execution. Below this threshold,
	// we use standard DuckDB array syntax which may be more efficient.
	MinPartitionsForParallel int
}

// DefaultParallelConfig returns sensible defaults for parallel execution.
func DefaultParallelConfig() *ParallelExecutorConfig {
	return &ParallelExecutorConfig{
		MaxConcurrentPartitions:  4,
		MinPartitionsForParallel: 3,
	}
}

// ParallelExecutor executes queries across multiple partitions in parallel.
// Each partition query runs in its own goroutine with semaphore-based
// concurrency control to prevent overwhelming the DuckDB connection pool.
type ParallelExecutor struct {
	db     *sql.DB
	config *ParallelExecutorConfig
	logger zerolog.Logger
}

// NewParallelExecutor creates a new parallel executor.
func NewParallelExecutor(db *sql.DB, config *ParallelExecutorConfig, logger zerolog.Logger) *ParallelExecutor {
	if config == nil {
		config = DefaultParallelConfig()
	}
	return &ParallelExecutor{
		db:     db,
		config: config,
		logger: logger.With().Str("component", "parallel-executor").Logger(),
	}
}

// PartitionResult holds the result of a single partition query.
type PartitionResult struct {
	Rows    *sql.Rows
	Columns []string
	Error   error
	Path    string
}

// ShouldUseParallel returns true if parallel execution should be used
// for the given number of partitions.
func (e *ParallelExecutor) ShouldUseParallel(partitionCount int) bool {
	return partitionCount >= e.config.MinPartitionsForParallel
}

// ExecutePartitioned executes a query template across multiple partitions in parallel.
// The queryTemplate should contain {PARTITION_PATH} placeholder that will be replaced
// with each partition's read_parquet expression.
//
// Returns merged sql.Rows from all partitions. The caller is responsible for closing
// all returned rows.
func (e *ParallelExecutor) ExecutePartitioned(
	ctx context.Context,
	paths []string,
	queryTemplate string,
	readParquetOptions string,
) ([]*PartitionResult, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no partition paths provided")
	}

	start := time.Now()
	e.logger.Info().
		Int("partition_count", len(paths)).
		Int("max_concurrent", e.config.MaxConcurrentPartitions).
		Msg("Starting parallel partition execution")

	// Create semaphore for concurrency control
	var sem chan struct{}
	if e.config.MaxConcurrentPartitions > 0 {
		sem = make(chan struct{}, e.config.MaxConcurrentPartitions)
	}

	// Execute partition queries in parallel
	results := make([]*PartitionResult, len(paths))
	var wg sync.WaitGroup

	for i, path := range paths {
		wg.Add(1)
		go func(idx int, partitionPath string) {
			defer wg.Done()

			// Acquire semaphore
			if sem != nil {
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					results[idx] = &PartitionResult{
						Error: ctx.Err(),
						Path:  partitionPath,
					}
					return
				}
			}

			// Build partition-specific query
			partitionSQL := e.buildPartitionQuery(queryTemplate, partitionPath, readParquetOptions)

			// Execute query
			rows, err := e.db.QueryContext(ctx, partitionSQL)
			if err != nil {
				e.logger.Error().
					Err(err).
					Str("path", partitionPath).
					Msg("Partition query failed")
				results[idx] = &PartitionResult{
					Error: err,
					Path:  partitionPath,
				}
				return
			}

			// Get columns for this partition
			columns, err := rows.Columns()
			if err != nil {
				rows.Close()
				results[idx] = &PartitionResult{
					Error: err,
					Path:  partitionPath,
				}
				return
			}

			results[idx] = &PartitionResult{
				Rows:    rows,
				Columns: columns,
				Path:    partitionPath,
			}
		}(i, path)
	}

	wg.Wait()

	// Count successes/failures
	var successCount, failCount int
	for _, r := range results {
		if r.Error != nil {
			failCount++
		} else {
			successCount++
		}
	}

	e.logger.Info().
		Int("success", successCount).
		Int("failed", failCount).
		Dur("duration", time.Since(start)).
		Msg("Parallel partition execution completed")

	return results, nil
}

// buildPartitionQuery builds a query for a single partition.
func (e *ParallelExecutor) buildPartitionQuery(template, path, options string) string {
	// Build read_parquet expression for this partition
	readParquet := fmt.Sprintf("read_parquet('%s', %s)", path, options)

	// Replace placeholder in template
	return strings.Replace(template, "{PARTITION_PATH}", readParquet, 1)
}

// MergedRowIterator provides a unified interface over multiple partition results.
// It iterates through all partition results sequentially, presenting them as
// a single result set.
type MergedRowIterator struct {
	results     []*PartitionResult
	currentIdx  int
	columns     []string
	scanBuffer  []interface{}
	valuePtrs   []interface{}
	logger      zerolog.Logger
}

// NewMergedRowIterator creates an iterator over multiple partition results.
// It validates that all successful partitions have the same schema.
func NewMergedRowIterator(results []*PartitionResult, logger zerolog.Logger) (*MergedRowIterator, error) {
	// Find first successful result to get schema
	var columns []string
	for _, r := range results {
		if r.Error == nil && r.Rows != nil {
			columns = r.Columns
			break
		}
	}

	if columns == nil {
		return nil, fmt.Errorf("no successful partition results")
	}

	// Validate all successful results have same column count
	for _, r := range results {
		if r.Error == nil && r.Rows != nil && len(r.Columns) != len(columns) {
			return nil, fmt.Errorf("schema mismatch: expected %d columns, got %d for partition %s",
				len(columns), len(r.Columns), r.Path)
		}
	}

	// Create scan buffer
	numCols := len(columns)
	scanBuffer := make([]interface{}, numCols)
	valuePtrs := make([]interface{}, numCols)
	for i := range scanBuffer {
		valuePtrs[i] = &scanBuffer[i]
	}

	return &MergedRowIterator{
		results:    results,
		currentIdx: 0,
		columns:    columns,
		scanBuffer: scanBuffer,
		valuePtrs:  valuePtrs,
		logger:     logger,
	}, nil
}

// Columns returns the column names.
func (m *MergedRowIterator) Columns() []string {
	return m.columns
}

// Next advances to the next row, returning false when exhausted.
func (m *MergedRowIterator) Next() bool {
	for m.currentIdx < len(m.results) {
		result := m.results[m.currentIdx]

		// Skip failed partitions
		if result.Error != nil || result.Rows == nil {
			m.currentIdx++
			continue
		}

		// Try to get next row from current partition
		if result.Rows.Next() {
			return true
		}

		// Current partition exhausted, move to next
		m.currentIdx++
	}

	return false
}

// Scan copies the current row values into the provided destinations.
func (m *MergedRowIterator) Scan(dest ...interface{}) error {
	if m.currentIdx >= len(m.results) {
		return fmt.Errorf("no more rows")
	}

	result := m.results[m.currentIdx]
	if result.Rows == nil {
		return fmt.Errorf("invalid row state")
	}

	return result.Rows.Scan(dest...)
}

// ScanBuffer scans into the internal buffer and returns the values.
func (m *MergedRowIterator) ScanBuffer() ([]interface{}, error) {
	if err := m.Scan(m.valuePtrs...); err != nil {
		return nil, err
	}
	return m.scanBuffer, nil
}

// Err returns any error from the underlying iterators.
func (m *MergedRowIterator) Err() error {
	for _, r := range m.results {
		if r.Error == nil && r.Rows != nil {
			if err := r.Rows.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close closes all underlying result sets.
func (m *MergedRowIterator) Close() error {
	var firstErr error
	for _, r := range m.results {
		if r.Rows != nil {
			if err := r.Rows.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
