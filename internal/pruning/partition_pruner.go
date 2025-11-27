package pruning

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// Pre-compiled regex patterns for time range extraction
// These are compiled once at package init rather than on every query
var (
	// Pattern to extract WHERE clause from SQL
	whereClausePattern = regexp.MustCompile(`(?i)\bWHERE\b\s+(.+?)(?:\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)`)

	// Patterns for start time extraction
	startTimePatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)time\s*>=\s*'([^']+)'`),
		regexp.MustCompile(`(?i)time\s*>\s*'([^']+)'`),
		regexp.MustCompile(`(?i)timestamp\s*>=\s*'([^']+)'`),
		regexp.MustCompile(`(?i)timestamp\s*>\s*'([^']+)'`),
	}

	// Patterns for end time extraction
	endTimePatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)time\s*<\s*'([^']+)'`),
		regexp.MustCompile(`(?i)time\s*<=\s*'([^']+)'`),
		regexp.MustCompile(`(?i)timestamp\s*<\s*'([^']+)'`),
		regexp.MustCompile(`(?i)timestamp\s*<=\s*'([^']+)'`),
	}

	// Pattern for BETWEEN clause
	betweenPattern = regexp.MustCompile(`(?i)time\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'`)

	// Pattern to parse storage paths
	storagePathPattern = regexp.MustCompile(`(.+)/([^/]+)/([^/]+)/\*\*/\*\.parquet$`)
)

// PartitionPruner provides file-level partition pruning to skip reading Parquet files
// that don't match the query's WHERE clause predicates.
//
// Key optimization: Instead of reading ALL files with /**/*.parquet glob,
// we extract time ranges from WHERE clauses and build a targeted file list.
//
// Example:
//
//	Query: SELECT * FROM cpu WHERE time >= '2024-03-15' AND time < '2024-03-16'
//	Before: Read all 8,760 hour partitions (1 year)
//	After: Read only 24 hour partitions (1 day)
//	Result: 365x fewer files, 10-100x faster
type PartitionPruner struct {
	enabled bool
	logger  zerolog.Logger
	stats   PrunerStats
}

// PrunerStats tracks partition pruning statistics using atomic counters for thread-safety
type PrunerStats struct {
	QueriesOptimized atomic.Int64
	FilesPruned      atomic.Int64
	FilesScanned     atomic.Int64
}

// TimeRange represents a time range filter
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// NewPartitionPruner creates a new partition pruner
func NewPartitionPruner(logger zerolog.Logger) *PartitionPruner {
	return &PartitionPruner{
		enabled: true,
		logger:  logger.With().Str("component", "partition-pruner").Logger(),
		stats:   PrunerStats{},
	}
}

// ExtractTimeRange extracts time range from WHERE clause
//
// Supports patterns like:
// - time >= '2024-03-15' AND time < '2024-03-16'
// - time BETWEEN '2024-03-15' AND '2024-03-16'
// - time > '2024-03-15 10:00:00'
// - time >= '2024-03-15T10:00:00Z'
func (p *PartitionPruner) ExtractTimeRange(sql string) *TimeRange {
	// Check if WHERE clause exists
	if !strings.Contains(strings.ToUpper(sql), "WHERE") {
		return nil
	}

	// Extract WHERE clause using pre-compiled pattern
	matches := whereClausePattern.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil
	}

	whereClause := matches[1]
	p.logger.Debug().Str("where_clause", whereClause[:min(100, len(whereClause))]).Msg("Analyzing WHERE clause")

	var startTime, endTime *time.Time

	// Try pre-compiled start time patterns
	for _, re := range startTimePatterns {
		if match := re.FindStringSubmatch(whereClause); match != nil {
			if t, err := parseDateTime(match[1]); err == nil {
				startTime = &t
				p.logger.Debug().Time("start_time", t).Msg("Found start time")
				break
			}
		}
	}

	// Try pre-compiled end time patterns
	for _, re := range endTimePatterns {
		if match := re.FindStringSubmatch(whereClause); match != nil {
			if t, err := parseDateTime(match[1]); err == nil {
				endTime = &t
				p.logger.Debug().Time("end_time", t).Msg("Found end time")
				break
			}
		}
	}

	// Try BETWEEN pattern
	if match := betweenPattern.FindStringSubmatch(whereClause); match != nil {
		if t1, err1 := parseDateTime(match[1]); err1 == nil {
			if t2, err2 := parseDateTime(match[2]); err2 == nil {
				startTime = &t1
				endTime = &t2
				p.logger.Debug().
					Time("start_time", t1).
					Time("end_time", t2).
					Msg("Found BETWEEN range")
			}
		}
	}

	// Build time range
	if startTime != nil && endTime != nil {
		return &TimeRange{Start: *startTime, End: *endTime}
	} else if startTime != nil {
		// Only start time - assume query up to "now + 1 day"
		end := time.Now().UTC().Add(24 * time.Hour)
		return &TimeRange{Start: *startTime, End: end}
	} else if endTime != nil {
		// Only end time - assume from beginning of data
		start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		return &TimeRange{Start: start, End: *endTime}
	}

	return nil
}

// GeneratePartitionPaths generates list of partition paths for the given time range
//
// Arc's partition structure: {database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
// Example: default/cpu/2024/03/15/14/cpu_20240315_140000_1000.parquet
func (p *PartitionPruner) GeneratePartitionPaths(basePath, database, measurement string, timeRange *TimeRange) []string {
	if timeRange == nil {
		return nil
	}

	paths := []string{}
	daysMap := make(map[string]bool)

	// Generate hour-by-hour paths
	current := timeRange.Start.Truncate(time.Hour)
	end := timeRange.End

	for current.Before(end) {
		year := current.Format("2006")
		month := current.Format("01")
		day := current.Format("02")
		hour := current.Format("15")

		// Track which days we're covering (for daily compacted files)
		dayKey := fmt.Sprintf("%s/%s/%s", year, month, day)
		daysMap[dayKey] = true

		// Build path for this hour partition (hourly compacted files)
		hourPath := filepath.Join(basePath, database, measurement, year, month, day, hour, "*.parquet")
		paths = append(paths, hourPath)

		current = current.Add(time.Hour)
	}

	// Add day-level paths for daily compacted files
	// Daily compacted files are stored at /year/month/day/*.parquet (not in hour subdirs)
	for dayKey := range daysMap {
		dayPath := filepath.Join(basePath, database, measurement, dayKey, "*.parquet")
		paths = append(paths, dayPath)
	}

	p.logger.Info().
		Int("total_paths", len(paths)).
		Int("hourly_paths", len(paths)-len(daysMap)).
		Int("daily_paths", len(daysMap)).
		Time("start", timeRange.Start).
		Time("end", timeRange.End).
		Msg("Generated partition paths")

	p.stats.QueriesOptimized.Add(1)

	return paths
}

// OptimizeTablePath optimizes a table path based on WHERE clause predicates
//
// Returns the optimized path (string or []string) and whether optimization was applied
func (p *PartitionPruner) OptimizeTablePath(originalPath, sql string) (interface{}, bool) {
	if !p.enabled {
		return originalPath, false
	}

	// Extract time range from query
	timeRange := p.ExtractTimeRange(sql)
	if timeRange == nil {
		p.logger.Debug().Msg("No time range found in query, skipping partition pruning")
		return originalPath, false
	}

	// Parse original path to extract components using pre-compiled pattern
	// Format: {base_path}/{database}/{measurement}/**/*.parquet
	matches := storagePathPattern.FindStringSubmatch(originalPath)

	if len(matches) < 4 {
		p.logger.Debug().Str("path", originalPath).Msg("Path format not recognized")
		return originalPath, false
	}

	basePath := matches[1]
	database := matches[2]
	measurement := matches[3]

	p.logger.Debug().
		Str("base_path", basePath).
		Str("database", database).
		Str("measurement", measurement).
		Msg("Parsed path components")

	// Generate optimized partition paths
	partitionPaths := p.GeneratePartitionPaths(basePath, database, measurement, timeRange)

	if len(partitionPaths) == 0 {
		p.logger.Info().Msg("No partitions found, using fallback")
		return originalPath, false
	}

	// Filter out non-existent paths for local storage
	if strings.HasPrefix(basePath, "/") || strings.HasPrefix(basePath, ".") {
		partitionPaths = filterExistingPaths(partitionPaths, p.logger)
		if len(partitionPaths) == 0 {
			p.logger.Info().Msg("No data exists for time range, using fallback")
			return originalPath, false
		}
	}

	if len(partitionPaths) == 1 {
		p.logger.Info().
			Str("optimized_path", partitionPaths[0]).
			Msg("Using single optimized path")
		return partitionPaths[0], true
	}

	p.logger.Info().
		Int("partition_count", len(partitionPaths)).
		Msg("Using multiple partition paths")

	return partitionPaths, true
}

// StatsSnapshot holds a point-in-time snapshot of pruner statistics
type StatsSnapshot struct {
	QueriesOptimized int64
	FilesPruned      int64
	FilesScanned     int64
}

// GetStats returns a snapshot of pruning statistics (thread-safe)
func (p *PartitionPruner) GetStats() StatsSnapshot {
	return StatsSnapshot{
		QueriesOptimized: p.stats.QueriesOptimized.Load(),
		FilesPruned:      p.stats.FilesPruned.Load(),
		FilesScanned:     p.stats.FilesScanned.Load(),
	}
}

// ResetStats resets statistics counters (thread-safe)
func (p *PartitionPruner) ResetStats() {
	p.stats.QueriesOptimized.Store(0)
	p.stats.FilesPruned.Store(0)
	p.stats.FilesScanned.Store(0)
}

// parseDateTime parses datetime string in various formats
func parseDateTime(timeStr string) (time.Time, error) {
	// Try common formats
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
		"2006/01/02 15:04:05",
		"2006/01/02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("could not parse datetime: %s", timeStr)
}

// filterExistingPaths filters out paths that don't have any matching files
func filterExistingPaths(paths []string, logger zerolog.Logger) []string {
	existingPaths := []string{}

	for _, pattern := range paths {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			logger.Debug().Err(err).Str("pattern", pattern).Msg("Glob failed")
			continue
		}

		if len(matches) > 0 {
			existingPaths = append(existingPaths, pattern)
			logger.Debug().Str("pattern", pattern).Int("files", len(matches)).Msg("Path exists")
		} else {
			logger.Debug().Str("pattern", pattern).Msg("Path skipped (no files)")
		}
	}

	logger.Info().
		Int("original_count", len(paths)).
		Int("existing_count", len(existingPaths)).
		Msg("Filtered existing paths")

	return existingPaths
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
