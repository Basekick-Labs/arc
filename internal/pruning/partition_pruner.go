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
	mu         sync.RWMutex
	entries    map[string]globCacheEntry
	ttl        time.Duration
	maxEntries int
	hits       atomic.Int64
	misses     atomic.Int64
}

// newGlobCache creates a new glob cache with the specified TTL
func newGlobCache(ttl time.Duration) *globCache {
	return &globCache{
		entries:    make(map[string]globCacheEntry),
		ttl:        ttl,
		maxEntries: maxCacheEntries,
	}
}

// get retrieves a cached glob result if it exists and hasn't expired.
// Expired entries are evicted on read so that a workload with distinct
// (path, sql) keys can't grow the map monotonically between janitor sweeps.
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
		// Evict-on-read: remove the stale entry so it can't accumulate.
		c.mu.Lock()
		// Re-check under the write lock: another goroutine may have
		// refreshed this key with a still-valid entry in the meantime.
		if e, ok := c.entries[pattern]; ok && time.Now().After(e.expiresAt) {
			delete(c.entries, pattern)
		}
		c.mu.Unlock()
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
	// Bound the cache: if inserting a new key would exceed maxEntries,
	// drop expired entries first, and if still full, refuse the insert.
	// This caps attacker-controlled memory from varying the time literal
	// per request. Refreshing an existing key is always allowed.
	if _, exists := c.entries[pattern]; !exists && len(c.entries) >= c.maxEntries {
		c.evictExpiredLocked()
		if len(c.entries) >= c.maxEntries {
			c.mu.Unlock()
			return
		}
	}
	c.entries[pattern] = globCacheEntry{
		matches:   cached,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

// evictExpiredLocked removes all expired entries. Caller must hold c.mu.
func (c *globCache) evictExpiredLocked() {
	now := time.Now()
	for pattern, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, pattern)
		}
	}
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

// maxCacheEntries bounds the number of live keys in each of the glob and
// partition caches. Without a cap, an attacker varying the time literal per
// request produces distinct (path, sql) keys that grow the maps for the
// process lifetime (see #536 and the comment on StartCleanup). At this size
// the two caches together hold on the order of a few MB even with long paths;
// well above any realistic dashboard's distinct-query working set.
const maxCacheEntries = 10_000

// maxPartitionPaths caps how many partition paths GeneratePartitionPaths will
// materialize for a single query. Arc's partition layout yields ~1 path/hour
// plus ~1 path/day, so ~9,150 paths per year of range; 50k covers ~5 years of
// hourly pruning. Above the cap the pruner returns no paths and the caller
// falls back to the single unpruned /**/*.parquet glob — correct, just not
// pruned. This defuses the amplification DoS in #536: a WHERE time >=
// '0001-01-01' request can no longer force the server to allocate millions of
// path strings and glob/LIST each one.
const maxPartitionPaths = 50_000

// minPartitionDate is the floor the pruner clamps a query's start time up to
// before generating partition paths. It exists only to bound the degenerate
// unbounded-downward case (e.g. '0001-01-01'), not to constrain real data:
// 1970-01-01 UTC (the Unix epoch) is below any plausible telemetry timestamp,
// including multi-decade aerospace archives. Clamping UP to this floor is
// lossless — Arc has no data before the epoch, so no real partition is skipped
// — while it caps the number of pre-data hours the loop would otherwise walk.
// We clamp rather than reject so a two-sided query like
// `time >= '1960-01-01' AND time < '2024-06-01'` still prunes to
// [1970, 2024-06) instead of silently substituting a later floor and dropping
// legitimate pre-2020 rows.
var minPartitionDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// partitionCacheEntry represents a cached partition path result
type partitionCacheEntry struct {
	result    interface{} // string or []string
	optimized bool
	expiresAt time.Time
}

// partitionCache provides a TTL cache for OptimizeTablePath results
// This avoids repeated partition calculations for the same queries
type partitionCache struct {
	mu         sync.RWMutex
	entries    map[string]partitionCacheEntry
	ttl        time.Duration
	maxEntries int
	hits       atomic.Int64
	misses     atomic.Int64
}

// newPartitionCache creates a new partition cache with the specified TTL
func newPartitionCache(ttl time.Duration) *partitionCache {
	return &partitionCache{
		entries:    make(map[string]partitionCacheEntry),
		ttl:        ttl,
		maxEntries: maxCacheEntries,
	}
}

// cacheKey generates a cache key from the original path and SQL query
func (c *partitionCache) cacheKey(originalPath, sql string) string {
	// Use hash of SQL to avoid storing full query text
	h := fmt.Sprintf("%s:%x", originalPath, sha256.Sum256([]byte(sql)))
	return h
}

// get retrieves a cached partition result. Expired entries are evicted on
// read so distinct (path, sql) keys can't grow the map monotonically between
// janitor sweeps.
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
		// Evict-on-read: remove the stale entry so it can't accumulate.
		c.mu.Lock()
		// Re-check under the write lock in case another goroutine refreshed it.
		if e, ok := c.entries[key]; ok && time.Now().After(e.expiresAt) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return nil, false, false
	}

	c.hits.Add(1)
	return entry.result, entry.optimized, true
}

// set stores a partition result in the cache
func (c *partitionCache) set(key string, result interface{}, optimized bool) {
	c.mu.Lock()
	// Bound the cache: if inserting a new key would exceed maxEntries, drop
	// expired entries first, and if still full, refuse the insert. Refreshing
	// an existing key is always allowed.
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxEntries {
		c.evictExpiredLocked()
		if len(c.entries) >= c.maxEntries {
			c.mu.Unlock()
			return
		}
	}
	c.entries[key] = partitionCacheEntry{
		result:    result,
		optimized: optimized,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

// evictExpiredLocked removes all expired entries. Caller must hold c.mu.
func (c *partitionCache) evictExpiredLocked() {
	now := time.Now()
	for key, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, key)
		}
	}
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

	// cleanupStarted ensures StartCleanup spawns at most one janitor
	// goroutine per pruner. Idempotent on repeat calls — second and
	// later invocations log a warn and return without spawning. Guards
	// against a future hot-reload or test refactor accidentally
	// multiplying goroutines silently.
	cleanupStarted atomic.Bool
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
func (p *PartitionPruner) GeneratePartitionPaths(ctx context.Context, basePath, database, measurement string, timeRange *TimeRange) []string {
	if timeRange == nil {
		return nil
	}

	// Detect if this is a remote path (S3/Azure) - use string concatenation instead of filepath.Join
	// because filepath.Join will mangle URLs like s3://bucket to s3:/bucket
	isRemote := strings.HasPrefix(basePath, "s3://") || strings.HasPrefix(basePath, "azure://")

	// Generate hour-by-hour paths
	current := timeRange.Start.Truncate(time.Hour)
	end := timeRange.End

	// Clamp the start up to the epoch floor. Arc has no data before 1970, so
	// this is lossless (no real partition is skipped), and it bounds the number
	// of empty pre-data hours a degenerate start (e.g. '0001-01-01') would
	// otherwise force the loop to walk. We clamp rather than reject so a
	// two-sided query with a very old start still prunes correctly instead of
	// dropping legitimate rows (#536).
	if current.Before(minPartitionDate) {
		current = minPartitionDate
	}

	// Cap the number of paths BEFORE allocating anything. The path count is
	// ~1 per hour plus ~1 per covered day; bound it by an over-estimate of the
	// total (hours rounded up, plus one day per 24 hours) so the guard never
	// under-counts the paths the loop actually materializes. Above the cap we
	// return nil so the caller falls back to the single unpruned glob —
	// correct, just not pruned. This prevents a very wide time range from
	// forcing the server to materialize millions of path strings and
	// glob/LIST each one (#536).
	if span := end.Sub(current); span > 0 {
		// ceil(span / hour) hourly paths, plus at most ceil(hours/24)+1 daily.
		hourlyPaths := int64((span + time.Hour - 1) / time.Hour)
		dailyPaths := hourlyPaths/24 + 1
		if estPaths := hourlyPaths + dailyPaths; estPaths > int64(maxPartitionPaths) {
			p.logger.Warn().
				Int64("estimated_paths", estPaths).
				Int("max_partition_paths", maxPartitionPaths).
				Time("start", current).
				Time("end", timeRange.End).
				Msg("Time range too wide for partition pruning; falling back to unpruned glob")
			return nil
		}
	}

	paths := []string{}
	daysMap := make(map[string]bool)

	// ctxCheckInterval bounds how often we poll ctx.Err() inside the loop.
	// The loop is already bounded to <= maxPartitionPaths iterations by the cap
	// above; this just lets a client disconnect / deadline abort even that work.
	const ctxCheckInterval = 4096
	iter := 0

	for current.Before(end) {
		if iter%ctxCheckInterval == 0 && ctx.Err() != nil {
			p.logger.Warn().
				Int("paths_so_far", len(paths)).
				Msg("Partition path generation cancelled")
			return nil
		}
		iter++

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
func (p *PartitionPruner) OptimizeTablePath(ctx context.Context, originalPath, sql string) (interface{}, bool) {
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
	partitionPaths := p.GeneratePartitionPaths(ctx, basePath, database, measurement, timeRange)

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

// DefaultCleanupInterval is how often the background janitor sweeps
// expired entries from both caches. 30 seconds matches the shorter of
// the two TTLs (GlobCacheTTL = 30s, PartitionCacheTTL = 60s) — after
// expiry an entry can sit in the map for at most one sweep interval
// before it's removed, bounding worst-case retention at ~2× TTL.
const DefaultCleanupInterval = 30 * time.Second

// minCleanupInterval is the smallest interval StartCleanup will honor.
// Anything below this is clamped up to prevent a misconfigured caller
// (or a fuzz input that passes a sub-millisecond duration) from
// pinning a CPU core in a tight ticker loop. 1ms is a generous floor
// — orders of magnitude below any plausible production interval and
// any fast-iteration test (tests in this package use 5–10ms).
const minCleanupInterval = 1 * time.Millisecond

// StartCleanup spawns a background goroutine that periodically removes
// expired entries from both caches. The goroutine exits when ctx is
// cancelled (or its Done channel closes). Caller controls lifetime via
// the context.
//
// Idempotent: only the first call per *PartitionPruner spawns the
// janitor. Subsequent calls log a warn and return without spawning,
// so a hot-reload path or a test refactor can't silently multiply
// goroutines.
//
// Why this exists: get() returns "expired" as a cache miss but does
// NOT remove the stale entry — there's no eviction path on read.
// Combined with no max-size cap on either cache, a workload with
// high-cardinality glob patterns or distinct (path, sql) keys will
// grow the maps monotonically over the lifetime of the process. The
// public Cleanup{Glob,Partition}Cache methods exist but had no
// production caller — this fills that gap.
//
// interval=0 uses DefaultCleanupInterval. Negative values are
// rejected by clamping to the default.
func (p *PartitionPruner) StartCleanup(ctx context.Context, interval time.Duration) {
	if !p.cleanupStarted.CompareAndSwap(false, true) {
		p.logger.Warn().Msg("Partition pruner cache janitor already started; ignoring duplicate StartCleanup call")
		return
	}
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}
	if interval < minCleanupInterval {
		p.logger.Warn().
			Dur("requested", interval).
			Dur("clamped", minCleanupInterval).
			Msg("Cleanup interval below floor; clamping to prevent tight-loop sweep")
		interval = minCleanupInterval
	}
	go p.cleanupLoop(ctx, interval)
	p.logger.Info().
		Dur("interval", interval).
		Msg("Partition pruner cache janitor started")
}

// cleanupLoop is the body of the janitor goroutine. Runs until ctx is
// cancelled. One sweep per tick; sweeps are cheap (single mu.Lock +
// map iteration, no allocations) — at realistic key cardinality the
// sweep cost is on the order of microseconds, well below the
// goroutine-spawn + WaitGroup-sync overhead that parallelizing the
// two sweeps would add. Run sequentially; if a future profile ever
// shows the sweep itself blocking concurrent get() calls under a
// hot-cache workload, that's the trigger to revisit.
func (p *PartitionPruner) cleanupLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			p.logger.Info().Msg("Partition pruner cache janitor stopped")
			return
		case <-ticker.C:
			p.CleanupGlobCache()
			p.CleanupPartitionCache()
		}
	}
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
