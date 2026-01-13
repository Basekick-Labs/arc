package compaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// MaxFilesPerBatch is the maximum number of files to process in a single compaction job.
// DuckDB can segfault/abort when processing too many files in a single read_parquet() call.
// This limit prevents OOM and crashes on partitions with many large files.
// With 1M buffer size, files are ~10-14MB each, so 30 files ≈ 300-420MB per batch.
const MaxFilesPerBatch = 30

// Candidate represents a partition candidate for compaction
type Candidate struct {
	Database      string
	Measurement   string
	PartitionPath string
	Files         []string
	FileCount     int
	Tier          string
	PartitionTime time.Time
	BatchNumber   int // Batch number when candidate is split (0 = not batched or first batch)
	TotalBatches  int // Total number of batches for this partition (0 = not batched)
}

// SplitCandidateIntoBatches splits a candidate with many files into multiple candidates,
// each with at most MaxFilesPerBatch files. This prevents DuckDB segfaults when processing
// thousands of files in a single read_parquet() call.
func SplitCandidateIntoBatches(c Candidate) []Candidate {
	if len(c.Files) <= MaxFilesPerBatch {
		return []Candidate{c}
	}

	// Calculate number of batches needed
	numBatches := (len(c.Files) + MaxFilesPerBatch - 1) / MaxFilesPerBatch

	batches := make([]Candidate, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		start := i * MaxFilesPerBatch
		end := start + MaxFilesPerBatch
		if end > len(c.Files) {
			end = len(c.Files)
		}

		batch := Candidate{
			Database:      c.Database,
			Measurement:   c.Measurement,
			PartitionPath: c.PartitionPath,
			Files:         c.Files[start:end],
			FileCount:     end - start,
			Tier:          c.Tier,
			PartitionTime: c.PartitionTime,
			BatchNumber:   i + 1,
			TotalBatches:  numBatches,
		}
		batches = append(batches, batch)
	}

	return batches
}

// Tier defines the interface for compaction tiers (hourly, daily, weekly, monthly)
type Tier interface {
	// GetTierName returns the human-readable tier name (e.g., 'daily', 'weekly', 'monthly')
	GetTierName() string

	// GetPartitionLevel returns the partition level for this tier (e.g., 'day', 'week', 'month')
	GetPartitionLevel() string

	// FindCandidates finds partitions that are candidates for compaction at this tier level
	FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error)

	// ShouldCompact determines if a partition should be compacted based on tier-specific criteria
	ShouldCompact(files []string, partitionTime time.Time) bool

	// GetCompactedFilename generates the filename for a compacted file
	GetCompactedFilename(measurement string, partitionTime time.Time) string

	// IsCompactedFile checks if a file is already a compacted file from this tier
	IsCompactedFile(filename string) bool

	// IsEnabled returns whether this tier is enabled
	IsEnabled() bool

	// GetStats returns tier statistics
	GetStats() map[string]interface{}
}

// BaseTier provides common functionality for all compaction tiers
type BaseTier struct {
	StorageBackend storage.Backend
	MinAgeHours    int
	MinFiles       int
	TargetSizeMB   int
	Enabled        bool

	// Metrics
	TotalCompactions    int
	TotalFilesCompacted int
	TotalBytesSaved     int64

	Logger zerolog.Logger
	mu     sync.Mutex
}

// BaseTierConfig holds configuration for creating a base tier
type BaseTierConfig struct {
	StorageBackend storage.Backend
	MinAgeHours    int
	MinFiles       int
	TargetSizeMB   int
	Enabled        bool
	Logger         zerolog.Logger
}

// NewBaseTier creates a new base tier with common functionality
func NewBaseTier(cfg *BaseTierConfig) *BaseTier {
	return &BaseTier{
		StorageBackend: cfg.StorageBackend,
		MinAgeHours:    cfg.MinAgeHours,
		MinFiles:       cfg.MinFiles,
		TargetSizeMB:   cfg.TargetSizeMB,
		Enabled:        cfg.Enabled,
		Logger:         cfg.Logger,
	}
}

// IsEnabled returns whether this tier is enabled
func (t *BaseTier) IsEnabled() bool {
	return t.Enabled
}

// GetBaseStats returns base statistics for a tier
func (t *BaseTier) GetBaseStats(tierName string) map[string]interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	return map[string]interface{}{
		"tier":                  tierName,
		"enabled":               t.Enabled,
		"min_age_hours":         t.MinAgeHours,
		"min_files":             t.MinFiles,
		"target_size_mb":        t.TargetSizeMB,
		"total_compactions":     t.TotalCompactions,
		"total_files_compacted": t.TotalFilesCompacted,
		"total_bytes_saved":     t.TotalBytesSaved,
		"total_bytes_saved_mb":  float64(t.TotalBytesSaved) / 1024 / 1024,
	}
}

// RecordCompaction records metrics for a completed compaction
func (t *BaseTier) RecordCompaction(filesCompacted int, bytesSaved int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.TotalCompactions++
	t.TotalFilesCompacted += filesCompacted
	t.TotalBytesSaved += bytesSaved
}

// GetCompactedFilename generates a filename for a compacted file
func (t *BaseTier) GetCompactedFilename(tierName, measurement string, partitionTime time.Time) string {
	timestamp := partitionTime.Format("20060102")
	return fmt.Sprintf("%s_%s_%s.parquet", measurement, timestamp, tierName)
}

// IsCompactedFile checks if a file is a compacted file from a specific tier
func (t *BaseTier) IsCompactedFile(tierName, filename string) bool {
	suffix := fmt.Sprintf("_%s.parquet", tierName)
	return len(filename) >= len(suffix) && filename[len(filename)-len(suffix):] == suffix
}

// ShouldCompactByFileSuffix determines if compaction is needed based on file classification.
// This is a shared helper that implements the common compaction decision logic:
//   - compactedSuffix: suffix for files already compacted at this tier (e.g., "_compacted.parquet")
//   - isUncompactedInput: function to determine if a file is valid uncompacted input for this tier
//
// Returns true if:
//   - No compacted files exist AND enough uncompacted input files are present
//   - Compacted files exist AND enough new uncompacted input files have accumulated
func (t *BaseTier) ShouldCompactByFileSuffix(
	files []string,
	compactedSuffix string,
	isUncompactedInput func(string) bool,
) bool {
	if len(files) < t.MinFiles {
		return false
	}

	var compactedFiles, uncompactedFiles []string
	for _, f := range files {
		if len(f) >= len(compactedSuffix) && f[len(f)-len(compactedSuffix):] == compactedSuffix {
			compactedFiles = append(compactedFiles, f)
		} else if isUncompactedInput(f) {
			uncompactedFiles = append(uncompactedFiles, f)
		}
	}

	// Case 1: No compacted files yet, and enough uncompacted files
	if len(compactedFiles) == 0 && len(uncompactedFiles) >= t.MinFiles {
		t.Logger.Debug().
			Int("uncompacted_count", len(uncompactedFiles)).
			Msg("First time compaction needed")
		return true
	}

	// Case 2: Has compacted files, but many new uncompacted files accumulated
	if len(compactedFiles) > 0 && len(uncompactedFiles) >= t.MinFiles {
		t.Logger.Debug().
			Int("compacted", len(compactedFiles)).
			Int("uncompacted", len(uncompactedFiles)).
			Msg("Re-compaction needed")
		return true
	}

	return false
}
