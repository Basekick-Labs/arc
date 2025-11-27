package compaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Candidate represents a partition candidate for compaction
type Candidate struct {
	Database      string
	Measurement   string
	PartitionPath string
	Files         []string
	FileCount     int
	Tier          string
	PartitionTime time.Time
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
