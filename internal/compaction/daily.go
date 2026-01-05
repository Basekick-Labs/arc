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
// Daily tier compacts hourly files (7 path parts) into daily files (6 path parts)
func (t *DailyTier) ShouldCompact(files []string, partitionTime time.Time) bool {
	return t.ShouldCompactByFileSuffix(
		files,
		"_daily.parquet",
		func(f string) bool {
			// Hourly files have 7 path parts: database/measurement/year/month/day/hour/file.parquet
			// These are valid input for daily compaction
			parts := strings.Split(f, "/")
			return len(parts) == 7
		},
	)
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

	// Convert map to slice, filtering by newest file creation time
	result := make([]Candidate, 0, len(partitions))
	for _, p := range partitions {
		// Check newest file creation time in this partition
		newestFileTime := extractNewestFileTime(p.Files)

		// Skip partition if newest file is too recent (younger than cutoff)
		// This handles late-arriving data: if files are still being written to this partition,
		// wait until all files are old enough before compacting
		if !newestFileTime.IsZero() && newestFileTime.After(cutoffTime) {
			t.Logger.Debug().
				Str("partition", p.PartitionPath).
				Time("newest_file", newestFileTime).
				Time("cutoff", cutoffTime).
				Msg("Skipping partition: has files newer than cutoff")
			continue
		}

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

// extractNewestFileTime extracts the newest file creation time from a list of file paths.
// Supports two formats:
// - Hourly files: {measurement}_{YYYYMMDD_HHMMSS}_{nanos}.parquet
// - Daily files: {measurement}_{YYYYMMDD}_daily.parquet
// Returns zero time if no valid timestamps found.
func extractNewestFileTime(files []string) time.Time {
	var newest time.Time

	for _, file := range files {
		// Extract filename from path
		parts := strings.Split(file, "/")
		filename := parts[len(parts)-1]

		// Remove .parquet extension
		filename = strings.TrimSuffix(filename, ".parquet")

		// Check if it's a daily compacted file: measurement_YYYYMMDD_daily
		if strings.HasSuffix(filename, "_daily") {
			// Remove _daily suffix
			filename = strings.TrimSuffix(filename, "_daily")
			fileParts := strings.Split(filename, "_")
			if len(fileParts) < 2 {
				continue
			}
			// Last part is the date
			datePart := fileParts[len(fileParts)-1]
			// Parse date: YYYYMMDD (daily files created at midnight UTC)
			fileTime, err := time.Parse("20060102", datePart)
			if err != nil {
				continue
			}
			if fileTime.After(newest) {
				newest = fileTime
			}
			continue
		}

		// Handle hourly file: measurement_YYYYMMDD_HHMMSS_nanos
		fileParts := strings.Split(filename, "_")
		if len(fileParts) < 3 {
			continue
		}

		// Get timestamp parts (second and third from end)
		// Format: ..._YYYYMMDD_HHMMSS_nanos
		dateTimePart := fileParts[len(fileParts)-3] + "_" + fileParts[len(fileParts)-2]

		// Parse timestamp: YYYYMMDD_HHMMSS
		fileTime, err := time.Parse("20060102_150405", dateTimePart)
		if err != nil {
			continue
		}

		// Keep track of newest
		if fileTime.After(newest) {
			newest = fileTime
		}
	}

	return newest
}
