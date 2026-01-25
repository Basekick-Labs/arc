package pruning

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// globCacheEntry represents a cached glob result with expiration
type globCacheEntry struct {
	matches   []string
	expiresAt time.Time
}

// globCache provides a TTL cache for filepath.Glob results
// This avoids expensive filesystem operations for repeated queries
type globCache struct {
	mu      sync.RWMutex
	entries map[string]globCacheEntry
	ttl     time.Duration
	hits    atomic.Int64
	misses  atomic.Int64
}

// newGlobCache creates a new glob cache with the specified TTL
func newGlobCache(ttl time.Duration) *globCache {
	return &globCache{
		entries: make(map[string]globCacheEntry),
		ttl:     ttl,
	}
}

// get retrieves a cached glob result if it exists and hasn't expired
func (c *globCache) get(pattern string) ([]string, bool) {
	c.mu.RLock()
	entry, ok := c.entries[pattern]
	c.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	if time.Now().After(entry.expiresAt) {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	// Return a copy to prevent mutation of cached data
	result := make([]string, len(entry.matches))
	copy(result, entry.matches)
	return result, true
}

// set stores a glob result in the cache
func (c *globCache) set(pattern string, matches []string) {
	// Make a copy to prevent external mutation
	cached := make([]string, len(matches))
	copy(cached, matches)

	c.mu.Lock()
	c.entries[pattern] = globCacheEntry{
		matches:   cached,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

// invalidate removes all entries from the cache
func (c *globCache) invalidate() {
	c.mu.Lock()
	c.entries = make(map[string]globCacheEntry)
	c.mu.Unlock()
}

// cleanup removes expired entries from the cache
func (c *globCache) cleanup() int {
	now := time.Now()
	removed := 0

	c.mu.Lock()
	for pattern, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, pattern)
			removed++
		}
	}
	c.mu.Unlock()

	return removed
}

// stats returns cache statistics
func (c *globCache) stats() (hits, misses int64, size int) {
	c.mu.RLock()
	size = len(c.entries)
	c.mu.RUnlock()
	return c.hits.Load(), c.misses.Load(), size
}

// Pre-compiled regex patterns for time range extraction
// These are compiled once at package init rather than on every query
var (
	// Pattern to extract WHERE clause from SQL
	// Using [\s\S]+? instead of .+? to match across newlines
	whereClausePattern = regexp.MustCompile(`(?i)\bWHERE\b\s+([\s\S]+?)(?:\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)`)

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

	// Patterns for relative time expressions (NOW() +/- INTERVAL)
	// These capture: (1) the numeric amount, (2) the time unit, and detect +/- via separate patterns
	relativeStartSubtractPattern = regexp.MustCompile(`(?i)time\s*>=?\s*(?:NOW\s*\(\s*\)|CURRENT_TIMESTAMP)\s*-\s*INTERVAL\s*'(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)'`)
	relativeStartAddPattern      = regexp.MustCompile(`(?i)time\s*>=?\s*(?:NOW\s*\(\s*\)|CURRENT_TIMESTAMP)\s*\+\s*INTERVAL\s*'(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)'`)
	relativeEndSubtractPattern   = regexp.MustCompile(`(?i)time\s*<=?\s*(?:NOW\s*\(\s*\)|CURRENT_TIMESTAMP)\s*-\s*INTERVAL\s*'(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)'`)
	relativeEndAddPattern        = regexp.MustCompile(`(?i)time\s*<=?\s*(?:NOW\s*\(\s*\)|CURRENT_TIMESTAMP)\s*\+\s*INTERVAL\s*'(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months)'`)

	// Pattern to parse storage paths
	storagePathPattern = regexp.MustCompile(`(.+)/([^/]+)/([^/]+)/\*\*/\*\.parquet$`)
)

// GlobCacheTTL is the default TTL for glob result caching (30 seconds)
const GlobCacheTTL = 30 * time.Second

// PartitionCacheTTL is the default TTL for partition path caching (60 seconds)
const PartitionCacheTTL = 60 * time.Second

// partitionCacheEntry represents a cached partition path result
type partitionCacheEntry struct {
	result    interface{} // string or []string
	optimized bool
	expiresAt time.Time
}

// partitionCache provides a TTL cache for OptimizeTablePath results
// This avoids repeated partition calculations for the same queries
type partitionCache struct {
	mu      sync.RWMutex
	entries map[string]partitionCacheEntry
	ttl     time.Duration
	hits    atomic.Int64
	misses  atomic.Int64
}

// newPartitionCache creates a new partition cache with the specified TTL
func newPartitionCache(ttl time.Duration) *partitionCache {
	return &partitionCache{
		entries: make(map[string]partitionCacheEntry),
		ttl:     ttl,
	}
}

// cacheKey generates a cache key from the original path and SQL query
func (c *partitionCache) cacheKey(originalPath, sql string) string {
	// Use hash of SQL to avoid storing full query text
	h := fmt.Sprintf("%s:%x", originalPath, sha256.Sum256([]byte(sql)))
	return h
}

// get retrieves a cached partition result
func (c *partitionCache) get(key string) (interface{}, bool, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		return nil, false, false
	}

	if time.Now().After(entry.expiresAt) {
		c.misses.Add(1)
		return nil, false, false
	}

	c.hits.Add(1)
	return entry.result, entry.optimized, true
}

// set stores a partition result in the cache
func (c *partitionCache) set(key string, result interface{}, optimized bool) {
	c.mu.Lock()
	c.entries[key] = partitionCacheEntry{
		result:    result,
		optimized: optimized,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

// invalidate removes all entries from the cache
func (c *partitionCache) invalidate() {
	c.mu.Lock()
	c.entries = make(map[string]partitionCacheEntry)
	c.mu.Unlock()
}

// cleanup removes expired entries from the cache
func (c *partitionCache) cleanup() int {
	now := time.Now()
	removed := 0

	c.mu.Lock()
	for key, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, key)
			removed++
		}
	}
	c.mu.Unlock()

	return removed
}

// stats returns cache statistics
func (c *partitionCache) stats() (hits, misses int64, size int) {
	c.mu.RLock()
	size = len(c.entries)
	c.mu.RUnlock()
	return c.hits.Load(), c.misses.Load(), size
}

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
	enabled        bool
	logger         zerolog.Logger
	stats          PrunerStats
	globCache      *globCache
	partitionCache *partitionCache
	storage        storage.Backend // Optional storage backend for S3/Azure path validation
}

// PrunerStats tracks partition pruning statistics using atomic counters for thread-safety
type PrunerStats struct {
	QueriesOptimized atomic.Int64
}

// TimeRange represents a time range filter
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// evaluateRelativeTime converts a relative time expression to an absolute time.
// amount: the numeric value (e.g., "20")
// unit: the time unit (e.g., "days", "hours")
// isAddition: true for NOW() + INTERVAL, false for NOW() - INTERVAL
func evaluateRelativeTime(amount string, unit string, isAddition bool) (time.Time, error) {
	n, err := strconv.Atoi(amount)
	if err != nil {
		return time.Time{}, err
	}

	// For subtraction, make n negative
	if !isAddition {
		n = -n
	}

	now := time.Now().UTC()
	unit = strings.ToLower(strings.TrimSuffix(unit, "s")) // normalize: days -> day

	switch unit {
	case "second":
		return now.Add(time.Duration(n) * time.Second), nil
	case "minute":
		return now.Add(time.Duration(n) * time.Minute), nil
	case "hour":
		return now.Add(time.Duration(n) * time.Hour), nil
	case "day":
		return now.AddDate(0, 0, n), nil
	case "week":
		return now.AddDate(0, 0, n*7), nil
	case "month":
		return now.AddDate(0, n, 0), nil
	default:
		return time.Time{}, fmt.Errorf("unknown time unit: %s", unit)
	}
}

// NewPartitionPruner creates a new partition pruner
func NewPartitionPruner(logger zerolog.Logger) *PartitionPruner {
	return &PartitionPruner{
		enabled:        true,
		logger:         logger.With().Str("component", "partition-pruner").Logger(),
		stats:          PrunerStats{},
		globCache:      newGlobCache(GlobCacheTTL),
		partitionCache: newPartitionCache(PartitionCacheTTL),
	}
}

// SetStorageBackend sets the storage backend for remote path validation (S3/Azure).
// When set, the pruner will filter out non-existent partition paths before returning them.
func (p *PartitionPruner) SetStorageBackend(backend storage.Backend) {
	p.storage = backend
}

// ExtractTimeRange extracts time range from WHERE clause
//
// Supports patterns like:
// - time >= '2024-03-15' AND time < '2024-03-16'
// - time BETWEEN '2024-03-15' AND '2024-03-16'
// - time > '2024-03-15 10:00:00'
// - time >= '2024-03-15T10:00:00Z'
// - time > NOW() - INTERVAL '20 days'
// - time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
// - time < NOW() + INTERVAL '1 week'
func (p *PartitionPruner) ExtractTimeRange(sqlStr string) *TimeRange {
	// Check if WHERE clause exists
	if !strings.Contains(strings.ToUpper(sqlStr), "WHERE") {
		return nil
	}

	// Mask string literals to prevent regex from matching GROUP BY/ORDER BY/LIMIT inside strings
	// e.g., "WHERE message LIKE '%GROUP BY%'" should not stop at the literal GROUP BY
	maskedSQL, masks := sql.MaskStringLiterals(sqlStr, sql.HasQuotes(sqlStr))

	// Extract WHERE clause boundaries using masked SQL
	matches := whereClausePattern.FindStringSubmatch(maskedSQL)
	if len(matches) < 2 {
		return nil
	}

	// Get the masked WHERE clause, then unmask it to get actual time values
	maskedWhereClause := matches[1]
	whereClause := sql.UnmaskStringLiterals(maskedWhereClause, masks)
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

	// Try relative time patterns for start time if not found yet
	if startTime == nil {
		// Try NOW() - INTERVAL pattern (subtraction)
		if match := relativeStartSubtractPattern.FindStringSubmatch(whereClause); match != nil {
			if t, err := evaluateRelativeTime(match[1], match[2], false); err == nil {
				startTime = &t
				p.logger.Debug().Time("start_time", t).Str("expression", "NOW() - INTERVAL").Msg("Found relative start time")
			}
		}
		// Try NOW() + INTERVAL pattern (addition)
		if startTime == nil {
			if match := relativeStartAddPattern.FindStringSubmatch(whereClause); match != nil {
				if t, err := evaluateRelativeTime(match[1], match[2], true); err == nil {
					startTime = &t
					p.logger.Debug().Time("start_time", t).Str("expression", "NOW() + INTERVAL").Msg("Found relative start time")
				}
			}
		}
	}

	// Try relative time patterns for end time if not found yet
	if endTime == nil {
		// Try NOW() - INTERVAL pattern (subtraction)
		if match := relativeEndSubtractPattern.FindStringSubmatch(whereClause); match != nil {
			if t, err := evaluateRelativeTime(match[1], match[2], false); err == nil {
				endTime = &t
				p.logger.Debug().Time("end_time", t).Str("expression", "NOW() - INTERVAL").Msg("Found relative end time")
			}
		}
		// Try NOW() + INTERVAL pattern (addition)
		if endTime == nil {
			if match := relativeEndAddPattern.FindStringSubmatch(whereClause); match != nil {
				if t, err := evaluateRelativeTime(match[1], match[2], true); err == nil {
					endTime = &t
					p.logger.Debug().Time("end_time", t).Str("expression", "NOW() + INTERVAL").Msg("Found relative end time")
				}
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

	// Detect if this is a remote path (S3/Azure) - use string concatenation instead of filepath.Join
	// because filepath.Join will mangle URLs like s3://bucket to s3:/bucket
	isRemote := strings.HasPrefix(basePath, "s3://") || strings.HasPrefix(basePath, "azure://")

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
		var hourPath string
		if isRemote {
			// Use string concatenation for remote URLs to preserve protocol://
			hourPath = basePath + "/" + database + "/" + measurement + "/" + year + "/" + month + "/" + day + "/" + hour + "/*.parquet"
		} else {
			hourPath = filepath.Join(basePath, database, measurement, year, month, day, hour, "*.parquet")
		}
		paths = append(paths, hourPath)

		current = current.Add(time.Hour)
	}

	// Add day-level paths for daily compacted files
	// Daily compacted files are stored at /year/month/day/*.parquet (not in hour subdirs)
	for dayKey := range daysMap {
		var dayPath string
		if isRemote {
			dayPath = basePath + "/" + database + "/" + measurement + "/" + dayKey + "/*.parquet"
		} else {
			dayPath = filepath.Join(basePath, database, measurement, dayKey, "*.parquet")
		}
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

	// Check partition cache first
	cacheKey := p.partitionCache.cacheKey(originalPath, sql)
	if result, optimized, ok := p.partitionCache.get(cacheKey); ok {
		p.logger.Debug().Msg("Using cached partition paths")
		return result, optimized
	}

	// Extract time range from query
	timeRange := p.ExtractTimeRange(sql)
	if timeRange == nil {
		p.logger.Debug().Msg("No time range found in query, skipping partition pruning")
		// Cache negative result too
		p.partitionCache.set(cacheKey, originalPath, false)
		return originalPath, false
	}

	// Parse original path to extract components using pre-compiled pattern
	// Format: {base_path}/{database}/{measurement}/**/*.parquet
	matches := storagePathPattern.FindStringSubmatch(originalPath)

	if len(matches) < 4 {
		p.logger.Debug().Str("path", originalPath).Msg("Path format not recognized")
		p.partitionCache.set(cacheKey, originalPath, false)
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
		p.partitionCache.set(cacheKey, originalPath, false)
		return originalPath, false
	}

	// Filter out non-existent paths (works for both local and S3/Azure storage)
	partitionPaths = p.filterExistingPaths(partitionPaths)
	if len(partitionPaths) == 0 {
		p.logger.Info().Msg("No data exists for time range, using fallback")
		p.partitionCache.set(cacheKey, originalPath, false)
		return originalPath, false
	}

	var result interface{}
	if len(partitionPaths) == 1 {
		p.logger.Info().
			Str("optimized_path", partitionPaths[0]).
			Msg("Using single optimized path")
		result = partitionPaths[0]
	} else {
		p.logger.Info().
			Int("partition_count", len(partitionPaths)).
			Msg("Using multiple partition paths")
		result = partitionPaths
	}

	// Cache the result
	p.partitionCache.set(cacheKey, result, true)
	return result, true
}

// StatsSnapshot holds a point-in-time snapshot of pruner statistics
type StatsSnapshot struct {
	QueriesOptimized int64
}

// GetStats returns a snapshot of pruning statistics (thread-safe)
func (p *PartitionPruner) GetStats() StatsSnapshot {
	return StatsSnapshot{
		QueriesOptimized: p.stats.QueriesOptimized.Load(),
	}
}

// ResetStats resets statistics counters (thread-safe)
func (p *PartitionPruner) ResetStats() {
	p.stats.QueriesOptimized.Store(0)
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
			// Convert to UTC to match Arc's partition structure
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("could not parse datetime: %s", timeStr)
}

// filterExistingPaths filters out paths that don't have any matching files
// Uses a TTL cache to avoid repeated expensive filesystem operations
// Handles both local and remote (S3/Azure) paths
func (p *PartitionPruner) filterExistingPaths(paths []string) []string {
	if len(paths) == 0 {
		return paths
	}

	// Check if this is remote storage (S3/Azure)
	firstPath := paths[0]
	if strings.HasPrefix(firstPath, "s3://") || strings.HasPrefix(firstPath, "azure://") {
		return p.filterExistingRemotePaths(paths)
	}

	// Local path filtering using filepath.Glob
	return p.filterExistingLocalPaths(paths)
}

// filterExistingLocalPaths filters local paths using filepath.Glob
func (p *PartitionPruner) filterExistingLocalPaths(paths []string) []string {
	existingPaths := []string{}

	for _, pattern := range paths {
		// Check cache first
		if matches, ok := p.globCache.get(pattern); ok {
			if len(matches) > 0 {
				existingPaths = append(existingPaths, pattern)
				p.logger.Debug().Str("pattern", pattern).Int("files", len(matches)).Msg("Path exists (cached)")
			}
			continue
		}

		// Cache miss - perform actual glob
		matches, err := filepath.Glob(pattern)
		if err != nil {
			p.logger.Debug().Err(err).Str("pattern", pattern).Msg("Glob failed")
			continue
		}

		// Cache the result (even empty results to avoid repeated globs)
		p.globCache.set(pattern, matches)

		if len(matches) > 0 {
			existingPaths = append(existingPaths, pattern)
			p.logger.Debug().Str("pattern", pattern).Int("files", len(matches)).Msg("Path exists")
		} else {
			p.logger.Debug().Str("pattern", pattern).Msg("Path skipped (no files)")
		}
	}

	p.logger.Info().
		Int("original_count", len(paths)).
		Int("existing_count", len(existingPaths)).
		Msg("Filtered existing local paths")

	return existingPaths
}

// filterExistingRemotePaths filters S3/Azure paths by checking which directories exist
func (p *PartitionPruner) filterExistingRemotePaths(paths []string) []string {
	if p.storage == nil {
		// No storage backend configured - return all paths and let DuckDB handle errors
		p.logger.Debug().Msg("No storage backend configured, skipping remote path filtering")
		return paths
	}

	lister, ok := p.storage.(storage.DirectoryLister)
	if !ok {
		// Storage backend doesn't support directory listing
		p.logger.Debug().Msg("Storage backend doesn't support directory listing, skipping filtering")
		return paths
	}

	// Extract unique parent directories to check
	// Path format: s3://bucket/db/measurement/2025/12/17/16/*.parquet
	// We need to check if the hour directory exists
	dirsToCheck := make(map[string]struct{})
	pathToDir := make(map[string]string)

	for _, path := range paths {
		// Remove the glob suffix to get the directory
		dir := strings.TrimSuffix(path, "/*.parquet")
		dirsToCheck[dir] = struct{}{}
		pathToDir[path] = dir
	}

	// Group directories by parent to minimize ListDirectories calls
	// This reduces API calls from O(hours) to O(days) for time-range queries
	type dirInfo struct {
		parent string
		target string
	}
	dirDetails := make(map[string]dirInfo) // full dir -> {parent, target}
	uniqueParents := make(map[string]bool)

	for dir := range dirsToCheck {
		lastSlash := strings.LastIndex(dir, "/")
		if lastSlash == -1 {
			continue
		}
		parentDir := dir[:lastSlash+1] // Include trailing slash
		targetName := dir[lastSlash+1:]
		dirDetails[dir] = dirInfo{parent: parentDir, target: targetName}
		uniqueParents[parentDir] = true
	}

	// Cache of parent -> set of existing child directories
	parentChildren := make(map[string]map[string]bool)

	// Fetch child listings for each unique parent (one API call per parent)
	for parentDir := range uniqueParents {
		cacheKey := "remote:parent:" + parentDir

		if cached, ok := p.globCache.get(cacheKey); ok {
			// Cache hit - rebuild set from cached slice
			childSet := make(map[string]bool, len(cached))
			for _, child := range cached {
				childSet[child] = true
			}
			parentChildren[parentDir] = childSet
			p.logger.Debug().Str("parent", parentDir).Int("children", len(childSet)).Msg("Using cached parent listing")
			continue
		}

		// Cache miss - call ListDirectories once for this parent
		storagePrefix := p.extractStoragePrefix(parentDir)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		subdirs, err := lister.ListDirectories(ctx, storagePrefix)
		cancel()

		if err != nil {
			p.logger.Debug().Err(err).Str("prefix", storagePrefix).Msg("Failed to list remote directories")
			// On error, mark all children as existing to avoid false negatives
			parentChildren[parentDir] = nil // nil means "assume all exist"
			continue
		}

		// Build set of existing child directories
		childSet := make(map[string]bool, len(subdirs))
		childNames := make([]string, 0, len(subdirs))
		for _, subdir := range subdirs {
			subdir = strings.TrimSuffix(subdir, "/")
			if idx := strings.LastIndex(subdir, "/"); idx != -1 {
				subdir = subdir[idx+1:]
			}
			childSet[subdir] = true
			childNames = append(childNames, subdir)
		}

		// Cache by parent directory
		p.globCache.set(cacheKey, childNames)
		parentChildren[parentDir] = childSet
		p.logger.Debug().Str("parent", parentDir).Int("children", len(childNames)).Msg("Cached parent listing")
	}

	// Check which directories exist
	existingDirs := make(map[string]bool)
	for dir, info := range dirDetails {
		childSet := parentChildren[info.parent]
		if childSet == nil {
			// Parent listing failed - assume directory exists
			existingDirs[dir] = true
			p.logger.Debug().Str("dir", dir).Msg("Remote directory exists (assumed)")
		} else if childSet[info.target] {
			existingDirs[dir] = true
			p.logger.Debug().Str("dir", dir).Msg("Remote directory exists")
		} else {
			p.logger.Debug().Str("dir", dir).Msg("Remote directory does not exist")
		}
	}

	// Filter paths to only include existing directories/files
	existingPaths := []string{}
	for _, path := range paths {
		dir := pathToDir[path]
		if !existingDirs[dir] {
			continue
		}

		// For day-level paths (5 segments: db/measurement/year/month/day),
		// verify .parquet files exist directly at that level (not in subdirs).
		// This prevents "No files found" errors when daily compaction hasn't run yet.
		prefix := p.extractStoragePrefix(dir + "/")
		if parts := strings.Split(strings.Trim(prefix, "/"), "/"); len(parts) == 5 {
			// Check cache first for day-level file existence
			cacheKey := "remote:dayfiles:" + prefix
			var hasFiles bool
			if cached, ok := p.globCache.get(cacheKey); ok {
				hasFiles = len(cached) > 0
				p.logger.Debug().Str("prefix", prefix).Bool("has_files", hasFiles).Msg("Using cached day-level file check")
			} else {
				// Cache miss - make API call
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				files, err := p.storage.List(ctx, prefix)
				cancel()
				if err != nil {
					p.logger.Debug().Err(err).Str("prefix", prefix).Msg("Failed to list files at day-level path")
					continue
				}
				// Find direct parquet files (not in subdirs)
				var directFiles []string
				for _, f := range files {
					remaining := strings.TrimPrefix(f, prefix)
					if remaining != "" && !strings.Contains(remaining, "/") && strings.HasSuffix(remaining, ".parquet") {
						directFiles = append(directFiles, remaining)
					}
				}
				// Cache the result
				p.globCache.set(cacheKey, directFiles)
				hasFiles = len(directFiles) > 0
				p.logger.Debug().Str("prefix", prefix).Int("files", len(directFiles)).Msg("Cached day-level file check")
			}
			if !hasFiles {
				p.logger.Debug().Str("path", path).Msg("Day-level path has no direct parquet files, skipping")
				continue
			}
		}

		existingPaths = append(existingPaths, path)
	}

	p.logger.Info().
		Int("original_count", len(paths)).
		Int("existing_count", len(existingPaths)).
		Msg("Filtered existing remote paths")

	return existingPaths
}

// extractStoragePrefix converts an S3/Azure URL to a storage prefix
// e.g., s3://bucket/db/measurement/2025/ -> db/measurement/2025/
func (p *PartitionPruner) extractStoragePrefix(url string) string {
	// Remove protocol prefix
	if strings.HasPrefix(url, "s3://") {
		url = strings.TrimPrefix(url, "s3://")
		// Remove bucket name (first path component)
		if idx := strings.Index(url, "/"); idx != -1 {
			return url[idx+1:]
		}
	} else if strings.HasPrefix(url, "azure://") {
		url = strings.TrimPrefix(url, "azure://")
		// Remove container name (first path component)
		if idx := strings.Index(url, "/"); idx != -1 {
			return url[idx+1:]
		}
	}
	return url
}

// InvalidateGlobCache clears the glob cache
// Call this after compaction or when new partitions are created
func (p *PartitionPruner) InvalidateGlobCache() {
	p.globCache.invalidate()
	p.logger.Info().Msg("Glob cache invalidated")
}

// InvalidatePartitionCache clears the partition path cache
// Call this after compaction or when new partitions are created
func (p *PartitionPruner) InvalidatePartitionCache() {
	p.partitionCache.invalidate()
	p.logger.Info().Msg("Partition cache invalidated")
}

// InvalidateAllCaches clears both glob and partition caches
// Call this after compaction or when new partitions are created
func (p *PartitionPruner) InvalidateAllCaches() {
	p.globCache.invalidate()
	p.partitionCache.invalidate()
	p.logger.Info().Msg("All caches invalidated")
}

// CleanupGlobCache removes expired entries from the glob cache
func (p *PartitionPruner) CleanupGlobCache() int {
	removed := p.globCache.cleanup()
	if removed > 0 {
		p.logger.Debug().Int("removed", removed).Msg("Cleaned up expired glob cache entries")
	}
	return removed
}

// CleanupPartitionCache removes expired entries from the partition cache
func (p *PartitionPruner) CleanupPartitionCache() int {
	removed := p.partitionCache.cleanup()
	if removed > 0 {
		p.logger.Debug().Int("removed", removed).Msg("Cleaned up expired partition cache entries")
	}
	return removed
}

// GetGlobCacheStats returns glob cache statistics
func (p *PartitionPruner) GetGlobCacheStats() map[string]interface{} {
	hits, misses, size := p.globCache.stats()
	total := hits + misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	return map[string]interface{}{
		"cache_size":       size,
		"cache_hits":       hits,
		"cache_misses":     misses,
		"hit_rate_percent": hitRate,
		"ttl_seconds":      p.globCache.ttl.Seconds(),
	}
}

// GetPartitionCacheStats returns partition cache statistics
func (p *PartitionPruner) GetPartitionCacheStats() map[string]interface{} {
	hits, misses, size := p.partitionCache.stats()
	total := hits + misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	return map[string]interface{}{
		"cache_size":       size,
		"cache_hits":       hits,
		"cache_misses":     misses,
		"hit_rate_percent": hitRate,
		"ttl_seconds":      p.partitionCache.ttl.Seconds(),
	}
}

// GetAllCacheStats returns combined statistics for all caches
func (p *PartitionPruner) GetAllCacheStats() map[string]interface{} {
	return map[string]interface{}{
		"glob_cache":      p.GetGlobCacheStats(),
		"partition_cache": p.GetPartitionCacheStats(),
	}
}
