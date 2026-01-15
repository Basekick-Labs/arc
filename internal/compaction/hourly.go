package compaction

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// HourlyTier implements hourly compaction (Tier 1)
// Compacts small files within hourly partitions
type HourlyTier struct {
	*BaseTier
}

// HourlyTierConfig holds configuration for hourly compaction tier
type HourlyTierConfig struct {
	StorageBackend storage.Backend
	MinAgeHours    int  // Don't compact partitions younger than this (default: 1)
	MinFiles       int  // Only compact partitions with at least this many files (default: 10)
	TargetSizeMB   int  // Target size for compacted files (default: 512)
	Enabled        bool // Enable hourly compaction (default: true)
	Logger         zerolog.Logger
}

// NewHourlyTier creates a new hourly compaction tier
func NewHourlyTier(cfg *HourlyTierConfig) *HourlyTier {
	// Set defaults (MinAgeHours=0 is valid, meaning compact immediately)
	if cfg.MinAgeHours < 0 {
		cfg.MinAgeHours = 1
	}
	if cfg.MinFiles == 0 {
		cfg.MinFiles = 10
	}
	if cfg.TargetSizeMB == 0 {
		cfg.TargetSizeMB = 512
	}

	tier := &HourlyTier{
		BaseTier: NewBaseTier(&BaseTierConfig{
			StorageBackend: cfg.StorageBackend,
			MinAgeHours:    cfg.MinAgeHours,
			MinFiles:       cfg.MinFiles,
			TargetSizeMB:   cfg.TargetSizeMB,
			Enabled:        cfg.Enabled,
			Logger:         cfg.Logger.With().Str("tier", "hourly").Logger(),
		}),
	}

	tier.Logger.Info().
		Int("min_age_hours", cfg.MinAgeHours).
		Int("min_files", cfg.MinFiles).
		Int("target_size_mb", cfg.TargetSizeMB).
		Bool("enabled", cfg.Enabled).
		Msg("Hourly compaction tier initialized")

	return tier
}

// GetTierName returns the tier name
func (t *HourlyTier) GetTierName() string {
	return "hourly"
}

// GetPartitionLevel returns the partition level
func (t *HourlyTier) GetPartitionLevel() string {
	return "hour"
}

// FindCandidates finds hourly partitions that are candidates for compaction
func (t *HourlyTier) FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error) {
	if !t.Enabled {
		return nil, nil
	}

	var candidates []Candidate
	cutoffTime := time.Now().UTC().Add(-time.Duration(t.MinAgeHours) * time.Hour)

	t.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Time("cutoff", cutoffTime).
		Msg("Scanning for hourly compaction candidates")

	// List all hour partitions
	partitions, err := t.listHourPartitions(ctx, database, measurement, cutoffTime)
	if err != nil {
		return nil, err
	}

	for _, partition := range partitions {
		if t.ShouldCompact(partition.Files, partition.PartitionTime) {
			partition.Tier = t.GetTierName()
			partition.FileCount = len(partition.Files)
			candidates = append(candidates, partition)

			t.Logger.Info().
				Str("database", database).
				Str("partition", partition.PartitionPath).
				Int("file_count", len(partition.Files)).
				Msg("Found hourly compaction candidate")
		}
	}

	t.Logger.Info().
		Str("database", database).
		Str("measurement", measurement).
		Int("candidates", len(candidates)).
		Msg("Hourly compaction candidate scan complete")

	return candidates, nil
}

// ShouldCompact determines if an hourly partition should be compacted
func (t *HourlyTier) ShouldCompact(files []string, partitionTime time.Time) bool {
	return t.ShouldCompactByFileSuffix(
		files,
		"_compacted.parquet",
		func(f string) bool {
			// All non-compacted files are valid input for hourly compaction
			return !strings.Contains(f, "_compacted.parquet")
		},
	)
}

// IsCompactedFile checks if a file is a compacted hourly file
func (t *HourlyTier) IsCompactedFile(filename string) bool {
	return strings.HasSuffix(filename, "_compacted.parquet")
}

// GetStats returns tier statistics
func (t *HourlyTier) GetStats() map[string]interface{} {
	return t.GetBaseStats(t.GetTierName())
}

// listHourPartitions lists all hour partitions for a measurement
func (t *HourlyTier) listHourPartitions(ctx context.Context, database, measurement string, cutoffTime time.Time) ([]Candidate, error) {
	// Get all files for this database/measurement
	prefix := database + "/" + measurement + "/"
	objects, err := t.StorageBackend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	t.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Int("object_count", len(objects)).
		Msg("Listed storage objects")

	// Group files by hour partition
	partitions := make(map[string]*Candidate)

	for _, obj := range objects {
		// Parse path: database/measurement/year/month/day/hour/file.parquet
		parts := strings.Split(obj, "/")
		if len(parts) < 7 {
			continue
		}

		db, meas, year, month, day, hour := parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]

		// Validate database and measurement
		if db != database || meas != measurement {
			continue
		}

		// Validate hour is a valid hour (00-23)
		hourInt, err := strconv.Atoi(hour)
		if err != nil || hourInt < 0 || hourInt > 23 {
			continue // Not a valid hour, skip
		}

		// Parse partition time
		yearInt, _ := strconv.Atoi(year)
		monthInt, _ := strconv.Atoi(month)
		dayInt, _ := strconv.Atoi(day)

		partitionTime := time.Date(yearInt, time.Month(monthInt), dayInt, hourInt, 0, 0, 0, time.UTC)

		// Check if partition is old enough
		if partitionTime.After(cutoffTime) {
			continue
		}

		// Build partition path (includes database for full path)
		partitionPath := filepath.Join(database, measurement, year, month, day, hour)

		// Add to partition map
		if _, exists := partitions[partitionPath]; !exists {
			partitions[partitionPath] = &Candidate{
				Database:      database,
				Measurement:   measurement,
				PartitionPath: partitionPath,
				PartitionTime: partitionTime,
				Files:         []string{},
			}
		}

		partitions[partitionPath].Files = append(partitions[partitionPath].Files, obj)
	}

	// Convert map to slice
	result := make([]Candidate, 0, len(partitions))
	for _, p := range partitions {
		result = append(result, *p)
	}

	t.Logger.Info().
		Str("database", database).
		Str("measurement", measurement).
		Int("partition_count", len(result)).
		Time("cutoff", cutoffTime).
		Msg("Found hour partitions")

	return result, nil
}
