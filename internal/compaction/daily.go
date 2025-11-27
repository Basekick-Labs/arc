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

// DailyTier implements daily compaction (Tier 2)
// Compacts hourly-compacted files into daily files
type DailyTier struct {
	*BaseTier
}

// DailyTierConfig holds configuration for daily compaction tier
type DailyTierConfig struct {
	StorageBackend storage.Backend
	MinAgeHours    int  // Don't compact days younger than this (default: 24)
	MinFiles       int  // Only compact days with at least this many files (default: 12)
	TargetSizeMB   int  // Target size for compacted files (default: 2048)
	Enabled        bool // Enable daily compaction (default: true)
	Logger         zerolog.Logger
}

// NewDailyTier creates a new daily compaction tier
func NewDailyTier(cfg *DailyTierConfig) *DailyTier {
	// Set defaults
	if cfg.MinAgeHours == 0 {
		cfg.MinAgeHours = 24 // Full day must pass
	}
	if cfg.MinFiles == 0 {
		cfg.MinFiles = 12 // At least half a day of data
	}
	if cfg.TargetSizeMB == 0 {
		cfg.TargetSizeMB = 2048 // 2GB target
	}

	tier := &DailyTier{
		BaseTier: NewBaseTier(&BaseTierConfig{
			StorageBackend: cfg.StorageBackend,
			MinAgeHours:    cfg.MinAgeHours,
			MinFiles:       cfg.MinFiles,
			TargetSizeMB:   cfg.TargetSizeMB,
			Enabled:        cfg.Enabled,
			Logger:         cfg.Logger.With().Str("tier", "daily").Logger(),
		}),
	}

	tier.Logger.Info().
		Int("min_age_hours", cfg.MinAgeHours).
		Int("min_files", cfg.MinFiles).
		Int("target_size_mb", cfg.TargetSizeMB).
		Bool("enabled", cfg.Enabled).
		Msg("Daily compaction tier initialized")

	return tier
}

// GetTierName returns the tier name
func (t *DailyTier) GetTierName() string {
	return "daily"
}

// GetPartitionLevel returns the partition level
func (t *DailyTier) GetPartitionLevel() string {
	return "day"
}

// FindCandidates finds daily partitions that are candidates for compaction
func (t *DailyTier) FindCandidates(ctx context.Context, database, measurement string) ([]Candidate, error) {
	if !t.Enabled {
		return nil, nil
	}

	var candidates []Candidate
	cutoffTime := time.Now().UTC().Add(-time.Duration(t.MinAgeHours) * time.Hour)

	t.Logger.Debug().
		Str("database", database).
		Str("measurement", measurement).
		Time("cutoff", cutoffTime).
		Msg("Scanning for daily compaction candidates")

	// List all day partitions
	partitions, err := t.listDayPartitions(ctx, database, measurement, cutoffTime)
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
				Msg("Found daily compaction candidate")
		}
	}

	t.Logger.Info().
		Str("database", database).
		Str("measurement", measurement).
		Int("candidates", len(candidates)).
		Msg("Daily compaction candidate scan complete")

	return candidates, nil
}

// ShouldCompact determines if a day partition should be compacted
func (t *DailyTier) ShouldCompact(files []string, partitionTime time.Time) bool {
	if len(files) < t.MinFiles {
		t.Logger.Debug().
			Int("file_count", len(files)).
			Int("min_files", t.MinFiles).
			Msg("Skipping daily compaction: not enough files")
		return false
	}

	// Separate daily-compacted files from hourly files
	// Use path depth to distinguish:
	// - Daily files: measurement/year/month/day/file.parquet (5 parts)
	// - Hourly files: measurement/year/month/day/hour/file.parquet (6 parts)
	var dailyCompactedFiles, hourlyFiles []string

	for _, f := range files {
		parts := strings.Split(f, "/")
		if len(parts) == 5 {
			// Day-level file = daily compacted file
			dailyCompactedFiles = append(dailyCompactedFiles, f)
		} else {
			// Hour-level file = hourly file
			hourlyFiles = append(hourlyFiles, f)
		}
	}

	// Case 1: No daily compacted file yet, and has enough total files
	if len(dailyCompactedFiles) == 0 && len(files) >= t.MinFiles {
		t.Logger.Debug().
			Int("file_count", len(files)).
			Msg("First time daily compaction needed")
		return true
	}

	// Case 2: Has daily compacted file, but many new hourly files accumulated
	if len(dailyCompactedFiles) > 0 && len(hourlyFiles) >= t.MinFiles {
		t.Logger.Debug().
			Int("daily_files", len(dailyCompactedFiles)).
			Int("hourly_files", len(hourlyFiles)).
			Msg("Daily re-compaction needed")
		return true
	}

	return false
}

// GetCompactedFilename generates the filename for a compacted file
func (t *DailyTier) GetCompactedFilename(measurement string, partitionTime time.Time) string {
	timestamp := partitionTime.Format("20060102")
	return measurement + "_" + timestamp + "_daily.parquet"
}

// IsCompactedFile checks if a file is a compacted daily file
func (t *DailyTier) IsCompactedFile(filename string) bool {
	return strings.HasSuffix(filename, "_daily.parquet")
}

// GetStats returns tier statistics
func (t *DailyTier) GetStats() map[string]interface{} {
	return t.GetBaseStats(t.GetTierName())
}

// listDayPartitions lists all day partitions for a measurement
func (t *DailyTier) listDayPartitions(ctx context.Context, database, measurement string, cutoffTime time.Time) ([]Candidate, error) {
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

	// Group files by day partition
	partitions := make(map[string]*Candidate)

	for _, obj := range objects {
		// Parse path: database/measurement/year/month/day/[hour/]file.parquet
		parts := strings.Split(obj, "/")
		if len(parts) < 6 {
			continue
		}

		db, meas, year, month, day := parts[0], parts[1], parts[2], parts[3], parts[4]

		// Validate database and measurement
		if db != database || meas != measurement {
			continue
		}

		// Parse partition time (day level)
		yearInt, err := strconv.Atoi(year)
		if err != nil {
			continue
		}
		monthInt, err := strconv.Atoi(month)
		if err != nil {
			continue
		}
		dayInt, err := strconv.Atoi(day)
		if err != nil {
			continue
		}

		partitionTime := time.Date(yearInt, time.Month(monthInt), dayInt, 0, 0, 0, 0, time.UTC)

		// Check if partition is old enough
		if partitionTime.After(cutoffTime) {
			continue
		}

		// Build partition path (day level, includes database)
		partitionPath := filepath.Join(database, measurement, year, month, day)

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
		Msg("Found day partitions")

	return result, nil
}
