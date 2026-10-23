package pruning

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// globCacheEntry represents a cached glob result with expiration
type globCacheEntry struct {
	matches   []string
	expiresAt time.Time
	setAt     time.Time
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
	now := time.Now()
	c.entries[pattern] = globCacheEntry{
		matches:   cached,
		expiresAt: now.Add(c.ttl),
		setAt:     now,
	}
	c.mu.Unlock()
}

// getFresh is get restricted to entries stored within maxAge; maxAge <= 0
// means any unexpired entry. It is what the empty-range proof (#928) uses
// so a stale "absent" verdict cannot stand in for a fresh listing.
func (c *globCache) getFresh(pattern string, maxAge time.Duration) ([]string, bool) {
	if maxAge <= 0 {
		return c.get(pattern)
	}
	c.mu.RLock()
	entry, ok := c.entries[pattern]
	c.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) || time.Since(entry.setAt) > maxAge {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	result := make([]string, len(entry.matches))
	copy(result, entry.matches)
	return result, true
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

	// Pattern to parse storage paths.
	//
	// The root group is `(.*)`, not `(.+)`: a LocalBackend rooted at "/" has a
	// storage root of "/", so its glob is "/db/cpu/**/*.parquet" and there is
	// nothing before the leading separator for `(.+)` to consume. That failed to
	// match, and a glob that does not parse is a glob that never gets pruned.
	storagePathPattern = regexp.MustCompile(`(.*)/([^/]+)/([^/]+)/\*\*/\*\.parquet$`)
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

// cacheKey generates a cache key from the original path, the SQL query and
// the live epoch (the start of the current hour, see live_hours.go): a list
// pruned before an hour started is never served after it.
func (c *partitionCache) cacheKey(originalPath, sql string, epoch string) string {
	// epoch is CacheEpoch's: the current hour, and a relative range's hours.
	// Use hash of SQL to avoid storing full query text
	h := fmt.Sprintf("%s:%s:%x", originalPath, epoch, sha256.Sum256([]byte(sql)))
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

	// File-level time pruning (see file_time_pruning.go). nowFn is
	// injectable for tests; production always uses time.Now.
	// Atomic so a configuration reload can change them while queries run.
	fileTimePruning atomic.Bool
	fileTimeMargin  atomic.Int64 // time.Duration
	nowFn           func() time.Time

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
	// StartAssumed marks a range whose Start was not extracted from the query
	// but assumed (an end-only predicate defaults Start to the 1970 partition floor). A
	// consumer pruning storage that can legitimately hold data older than the
	// assumption (a cold archive tier) must not trust Start for exclusion.
	StartAssumed bool
	// EndAssumed marks a range whose End was not extracted from the query
	// (a start-only predicate, or an end literal the extractor could not
	// parse) and was assumed as now plus one day. The empty-range proof
	// (#928) must not trust it: the real end may be later.
	EndAssumed bool
}

// evaluateRelativeTime converts a relative time expression to an absolute time.
// amount: the numeric value (e.g., "20")
// unit: the time unit (e.g., "days", "hours")
// isAddition: true for NOW() + INTERVAL, false for NOW() - INTERVAL
func evaluateRelativeTime(amount string, unit string, isAddition bool) (time.Time, error) {
	return evaluateRelativeTimeAt(time.Now().UTC(), amount, unit, isAddition)
}

// evaluateRelativeTimeAt is evaluateRelativeTime from a given now.
func evaluateRelativeTimeAt(now time.Time, amount string, unit string, isAddition bool) (time.Time, error) {
	n, err := strconv.Atoi(amount)
	if err != nil {
		return time.Time{}, err
	}
	// Far past any data and far from overflowing a time.Duration.
	if n > 1_000_000 {
		return time.Time{}, fmt.Errorf("interval amount %d too large", n)
	}

	// For subtraction, make n negative
	if !isAddition {
		n = -n
	}

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
		// DuckDB clamps to the target month's last day (Mar 31 - 1 month is
		// Feb 28); Go's AddDate normalises past it (Mar 3), a later start.
		return addMonthsClamped(now, n), nil
	default:
		return time.Time{}, fmt.Errorf("unknown time unit: %s", unit)
	}
}

// addMonthsClamped adds n months the way DuckDB does: the day of month is
// kept, or clamped to the last day of the target month.
func addMonthsClamped(t time.Time, n int) time.Time {
	first := time.Date(t.Year(), t.Month(), 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location()).AddDate(0, n, 0)
	last := first.AddDate(0, 1, -1).Day()
	day := t.Day()
	if day > last {
		day = last
	}
	return first.AddDate(0, 0, day-1)
}

// NewPartitionPruner creates a new partition pruner
func NewPartitionPruner(logger zerolog.Logger) *PartitionPruner {
	return &PartitionPruner{
		enabled:        true,
		logger:         logger.With().Str("component", "partition-pruner").Logger(),
		stats:          PrunerStats{},
		globCache:      newGlobCache(GlobCacheTTL),
		partitionCache: newPartitionCache(PartitionCacheTTL),
		nowFn:          time.Now,
	}
}

// SetStorageBackend sets the storage backend for remote path validation (S3/Azure).
// When set, the pruner will filter out non-existent partition paths before returning them.
func (p *PartitionPruner) SetStorageBackend(backend storage.Backend) {
	p.storage = backend
}

// ExtractTimeRange returns the time range every row of the query lies in, or
// nil. See time_bounds.go for the rule; in short, bounds come only from the
// top-level AND terms of a single-table query's one WHERE clause that are a
// bare comparison of the time column (time, "time", alias.time, (time)) with
// a literal ('…', '…'::timestamp, TIMESTAMP '…'), now()/CURRENT_TIMESTAMP
// plus or minus one interval, or a BETWEEN; either operand order. An
// inclusive end is returned one microsecond past its literal.
func (p *PartitionPruner) ExtractTimeRange(sqlStr string) *TimeRange {
	b, ok := scanTimeBounds(sqlStr)
	if !ok {
		return nil
	}
	switch {
	case b.start != nil && b.end != nil:
		return &TimeRange{Start: *b.start, End: *b.end}
	case b.start != nil:
		// Only start time - assume query up to "now + 1 day"
		end := time.Now().UTC().Add(24 * time.Hour)
		return &TimeRange{Start: *b.start, End: end, EndAssumed: true}
	default:
		// Only an end: the data may start at the partition floor. A range
		// this wide exceeds the path cap and falls back to the unpruned
		// glob, which is correct; assuming 2020 dropped older rows.
		return &TimeRange{Start: minPartitionDate, End: *b.end, StartAssumed: true}
	}
}

// GeneratePartitionPaths generates list of partition paths for the given time range
//
// Arc's partition structure: {database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
// Example: default/cpu/2024/03/15/14/cpu_20240315_140000_1000.parquet
func (p *PartitionPruner) GeneratePartitionPaths(ctx context.Context, basePath, database, measurement string, timeRange *TimeRange) []string {
	if timeRange == nil {
		return nil
	}

	// Validate here as well as at storage.GetStoragePath, because this is a
	// genuinely separate entry point rather than a second check on the same
	// value (#746). Whenever pruning fires, GetStoragePath's glob is DISCARDED:
	// OptimizeTablePath parses it back into (basePath, database, measurement)
	// and the loop below re-concatenates the string DuckDB actually reads. So
	// the segments arriving here have been through a regex round-trip, and
	// PruneTierPaths passes them in from extractDBMeasurementFromPath, which
	// has its own take-the-last-two-components fallback. Returning nil makes
	// the caller fall back to the unpruned glob, which is the same safe
	// behaviour as exceeding the path cap below.
	if storage.ValidateKeySegment(database) != nil || storage.ValidateGlobSafe(database) != nil ||
		storage.ValidateKeySegment(measurement) != nil || storage.ValidateGlobSafe(measurement) != nil {
		p.logger.Warn().
			Str("database", database).
			Str("measurement", measurement).
			Msg("Not generating partition paths for names that are not usable path segments")
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
	result, optimized, _ := p.optimize(ctx, originalPath, sql, false)
	return result, optimized
}

// optimize is the shared body of OptimizeTablePath and
// OptimizeTablePathVerdict. With wantProof false it behaves exactly as
// OptimizeTablePath always has; with it true, an empty existence pass is
// followed by the empty-range proof (empty_range.go).
func (p *PartitionPruner) optimize(ctx context.Context, originalPath, sql string, wantProof bool) (interface{}, bool, bool) {
	if !p.enabled {
		return originalPath, false, false
	}

	// The range is read once and keys the cache: a relative range's hours
	// move with now() (CacheEpoch).
	timeRange := p.ExtractTimeRange(sql)
	epoch := strconv.FormatInt(p.liveEpoch(), 10)
	if relativeTimeWord.MatchString(sql) {
		epoch = p.epochFor(timeRange)
	}
	cacheKey := p.partitionCache.cacheKey(originalPath, sql, epoch)
	if result, optimized, ok := p.partitionCache.get(cacheKey); ok {
		// A cached fallback verdict is exactly the stale-empty case the proof
		// exists for: with wantProof, re-derive instead of trusting it.
		if optimized || !wantProof {
			p.logger.Debug().Msg("Using cached partition paths")
			return result, optimized, false
		}
	}

	if timeRange == nil {
		p.logger.Debug().Msg("No time range found in query, skipping partition pruning")
		// Cache negative result too
		p.partitionCache.set(cacheKey, originalPath, false)
		return originalPath, false, false
	}

	// Parse original path to extract components using pre-compiled pattern
	// Format: {base_path}/{database}/{measurement}/**/*.parquet
	matches := storagePathPattern.FindStringSubmatch(originalPath)

	if len(matches) < 4 {
		p.logger.Debug().Str("path", originalPath).Msg("Path format not recognized")
		p.partitionCache.set(cacheKey, originalPath, false)
		return originalPath, false, false
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
		return originalPath, false, false
	}

	// A start-only range: add the partitions that exist past the assumed
	// end, and remember the newest one for an empty window (open_end.go).
	var newestGlob string
	if timeRange.EndAssumed {
		var lister dirLister
		if strings.HasPrefix(basePath, "s3://") || strings.HasPrefix(basePath, "azure://") {
			lister = p.remoteDirLister(basePath)
		} else {
			lister = localDirLister(basePath)
		}
		beyond, newest, err := discoverOpenEnd(ctx, lister, database, measurement, timeRange.End)
		switch {
		case errors.Is(err, errTooManyBeyond):
			p.logger.Info().Msg("Too many partitions past the assumed end; using the unpruned glob")
			p.partitionCache.set(cacheKey, originalPath, false)
			return originalPath, false, false
		case err != nil:
			// The generated range alone, as before: a listing hiccup must
			// not turn into a full scan.
			p.logger.Debug().Err(err).Msg("Could not list partitions past the assumed end")
		default:
			partitionPaths = appendBeyond(partitionPaths, beyond, basePath, database, measurement)
			if newest != nil && !newest.end().After(timeRange.Start) {
				newestGlob = partGlob(basePath, database, measurement, *newest)
			}
		}
	}

	// Filter out non-existent paths (works for both local and S3/Azure storage)
	generated := partitionPaths
	existing, verified, hits := p.filterExistingPathsOpts(ctx, partitionPaths, basePath, 0)
	partitionPaths = existing
	// A live partition left out because it has no files yet makes the result
	// valid only until that partition's first flush: never cached, here or
	// by the SQL transform cache.
	liveDropped := p.liveWindow().droppedLive(generated, existing)
	if liveDropped {
		markVolatile(ctx)
	}
	if len(partitionPaths) == 0 {
		if wantProof && p.proveEmpty(ctx, basePath, database, measurement, sql, timeRange, generated, hits, verified) {
			// Never cached: the next query must prove it again.
			return originalPath, false, true
		}
		// An empty start-only window: every partition is older than its
		// start, so the newest one answers it (the time predicate filters
		// it to no rows) instead of the whole measurement. Only with the
		// complete schema anchor in front of the read, which supplies every
		// column, and only when the extracted start is the query's real
		// predicate for its only table (a JOIN, subquery or OR applies it to
		// rows it does not bound, and the newest partition would drop them);
		// never cached (the window's live hour has no files yet).
		if newestGlob != "" && hasCompleteSchemaAnchor(ctx) && IsStartOnlyTimeRange(sql) {
			if have, _, _ := p.filterExistingPathsOpts(ctx, []string{newestGlob}, basePath, 0); len(have) == 1 {
				markVolatile(ctx)
				p.logger.Debug().Str("path", newestGlob).Msg("Empty open-ended window; reading the newest partition only")
				return newestGlob, true, false
			}
		}
		p.logger.Info().Msg("No data exists for time range, using fallback")
		if !liveDropped {
			p.partitionCache.set(cacheKey, originalPath, false)
		}
		return originalPath, false, false
	}

	// File-level time pruning of the live-hour boundary glob. When it
	// modifies the set, the result is volatile (an explicit file list goes
	// stale the moment the next flush lands) and must not be cached here
	// or by the SQL transform cache (the context flag carries that).
	partitionPaths, _, fileEligible := p.fileTimePass(ctx, partitionPaths, timeRange)

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

	// Cache the result — except when file-level pruning looked at the live
	// hour: an explicit file list is stale the moment the next flush lands,
	// and a declined pass (every file in range, too many kept) can expand on
	// the next query once older files fall out of the range. The per-query
	// re-glob this costs is a few ms.
	if !fileEligible && !liveDropped {
		p.partitionCache.set(cacheKey, result, true)
	}
	return result, true, false
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
func (p *PartitionPruner) filterExistingPaths(paths []string, basePath string) []string {
	existing, _, _ := p.filterExistingPathsOpts(context.Background(), paths, basePath, 0)
	return existing
}

// filterExistingPathsOpts is filterExistingPaths returning, besides the
// existing paths, whether every absence was verified by a listing (false when
// any listing failed, which the plain filter treats as "assume present" for
// directories and "drop" for day-level file checks) and how many answers
// came from the cache. maxAge > 0 accepts only cache entries younger than it.
func (p *PartitionPruner) filterExistingPathsOpts(ctx context.Context, paths []string, basePath string, maxAge time.Duration) ([]string, bool, int) {
	if len(paths) == 0 {
		return paths, true, 0
	}

	// Check if this is remote storage (S3/Azure)
	firstPath := paths[0]
	if strings.HasPrefix(firstPath, "s3://") || strings.HasPrefix(firstPath, "azure://") {
		return p.filterExistingRemotePathsOpts(ctx, paths, basePath, maxAge)
	}

	// Local path filtering using filepath.Glob
	return p.filterExistingLocalPathsOpts(paths, maxAge)
}

// filterExistingLocalPaths filters local paths using filepath.Glob
func (p *PartitionPruner) filterExistingLocalPaths(paths []string) []string {
	existing, _, _ := p.filterExistingLocalPathsOpts(paths, 0)
	return existing
}

func (p *PartitionPruner) filterExistingLocalPathsOpts(paths []string, maxAge time.Duration) ([]string, bool, int) {
	existingPaths := []string{}
	hits := 0
	live := p.liveWindow()

	for _, pattern := range paths {
		// Check cache first; a live partition's absence is never taken from
		// it (the entry may predate the hour becoming live)
		if matches, ok := p.globCache.getFresh(pattern, maxAge); ok && (len(matches) > 0 || !live.isLive(pattern)) {
			hits++
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

		// Cache the result (even empty results to avoid repeated globs,
		// except a live partition's: it can appear at the next flush)
		if len(matches) > 0 || !live.isLive(pattern) {
			p.globCache.set(pattern, matches)
		}

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

	// filepath.Glob only errors on a malformed pattern, which the generator
	// never produces, so a local pass is always verified.
	return existingPaths, true, hits
}

// filterExistingRemotePaths filters S3/Azure paths by checking which directories exist.
//
// basePath is the storage root the paths were generated against
// ("s3://bucket/prefix"), and is what turns a full URL back into the
// backend-relative key a listing needs. See extractStoragePrefix.
func (p *PartitionPruner) filterExistingRemotePaths(paths []string, basePath string) []string {
	existing, _, _ := p.filterExistingRemotePathsOpts(context.Background(), paths, basePath, 0)
	return existing
}

func (p *PartitionPruner) filterExistingRemotePathsOpts(ctx context.Context, paths []string, basePath string, maxAge time.Duration) ([]string, bool, int) {
	verified := true
	hits := 0
	if p.storage == nil {
		// No storage backend configured - return all paths and let DuckDB handle errors
		p.logger.Debug().Msg("No storage backend configured, skipping remote path filtering")
		return paths, false, 0
	}

	lister, ok := p.storage.(storage.DirectoryLister)
	if !ok {
		// Storage backend doesn't support directory listing
		p.logger.Debug().Msg("Storage backend doesn't support directory listing, skipping filtering")
		return paths, false, 0
	}

	// Extract unique parent directories to check
	// Path format: s3://bucket/db/measurement/2025/12/17/16/*.parquet
	// We need to check if the hour directory exists
	dirsToCheck := make(map[string]struct{})
	pathToDir := make(map[string]string)
	liveDirs := make(map[string]bool)
	live := p.liveWindow()

	for _, path := range paths {
		// Remove the glob suffix to get the directory
		dir := strings.TrimSuffix(path, "/*.parquet")
		dirsToCheck[dir] = struct{}{}
		pathToDir[path] = dir
		if live.isLive(path) {
			liveDirs[dir] = true
		}
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

		if cached, ok := p.globCache.getFresh(cacheKey, maxAge); ok {
			// Cache hit - rebuild set from cached slice
			childSet := make(map[string]bool, len(cached))
			for _, child := range cached {
				childSet[child] = true
			}
			// A live child missing from the cached listing may have
			// appeared since: list again rather than remember its absence.
			missingLive := false
			for dir, info := range dirDetails {
				if info.parent == parentDir && liveDirs[dir] && !childSet[info.target] {
					missingLive = true
					break
				}
			}
			if !missingLive {
				hits++
				parentChildren[parentDir] = childSet
				p.logger.Debug().Str("parent", parentDir).Int("children", len(childSet)).Msg("Using cached parent listing")
				continue
			}
		}

		// Cache miss - call ListDirectories once for this parent
		storagePrefix, ok := p.extractStoragePrefix(parentDir, basePath)
		if !ok {
			// Listing with an unresolvable prefix would scan the wrong key
			// space (or, with "", the entire bucket) and report every child
			// absent. Assume all exist, the same fail-open the listing-error
			// branch below takes.
			parentChildren[parentDir] = nil
			verified = false
			continue
		}
		listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		subdirs, err := lister.ListDirectories(listCtx, storagePrefix)
		cancel()

		if err != nil {
			p.logger.Debug().Err(err).Str("prefix", storagePrefix).Msg("Failed to list remote directories")
			// On error, mark all children as existing to avoid false negatives
			parentChildren[parentDir] = nil // nil means "assume all exist"
			verified = false
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
		prefix, ok := p.extractStoragePrefix(dir+"/", basePath)
		if !ok {
			// Cannot resolve: keep the path rather than judging it empty.
			existingPaths = append(existingPaths, path)
			verified = false
			continue
		}
		if parts := strings.Split(strings.Trim(prefix, "/"), "/"); len(parts) == 5 {
			// Check cache first for day-level file existence
			cacheKey := "remote:dayfiles:" + prefix
			var hasFiles bool
			if cached, ok := p.globCache.getFresh(cacheKey, maxAge); ok {
				hits++
				hasFiles = len(cached) > 0
				p.logger.Debug().Str("prefix", prefix).Bool("has_files", hasFiles).Msg("Using cached day-level file check")
			} else {
				// Cache miss - make API call
				listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				files, err := p.storage.List(listCtx, prefix)
				cancel()
				if err != nil {
					p.logger.Debug().Err(err).Str("prefix", prefix).Msg("Failed to list files at day-level path")
					// Dropped, as before, but the absence is not verified.
					verified = false
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

	return existingPaths, verified, hits
}

// extractStoragePrefix converts a full S3/Azure URL into the backend-relative
// prefix a listing call needs, given the storage root those URLs were built
// against.
//
// It trims basePath rather than just the scheme and bucket, and that is the fix
// (#746). A backend configured with storage.s3_prefix = "tenant" has a root of
// "s3://bucket/tenant", and List/ListDirectories prepend "tenant/" themselves.
// Stripping only "s3://bucket/" left the prefix attached, so the listing ran
// against "tenant/tenant/..." and came back EMPTY WITH NO ERROR. Every
// partition was then judged absent, filterExistingPaths returned nothing, and
// OptimizeTablePath fell back to the unpruned glob: single-tier partition
// pruning was silently dead on every prefixed-S3 deployment. Results stayed
// correct, cost did not.
//
// tier_pruning.go documents this exact hazard and filterTierRemotePaths already
// avoids it the same way, by trimming the tier's own root URL. Only the
// single-tier path was left doing scheme-and-bucket surgery.
//
// Deriving the root from the caller rather than from the backend's concrete
// type is deliberate: the tiered path filters against a DIFFERENT backend than
// p.storage, and a root parsed from the glob is correct for any backend,
// including ones that only implement the interface.
func (p *PartitionPruner) extractStoragePrefix(url, basePath string) (string, bool) {
	root := strings.TrimSuffix(basePath, "/") + "/"
	if rel, ok := strings.CutPrefix(url, root); ok {
		return rel, true
	}
	// Not under the root we were given. Every path reaching here is built as
	// basePath + "/" + ... by GeneratePartitionPaths, so this is unreachable
	// from the only caller and means the glob and the generated paths have
	// diverged.
	//
	// Returning a scheme-and-bucket-stripped prefix here, which is what this
	// did before #746, would be the WORST answer available: the backend
	// re-prefixes it, so the listing would run against some other key space and
	// confidently report every partition absent. Return "" instead, which the
	// caller treats as "cannot verify" and falls back to the unpruned glob.
	// That is what filterTierRemotePaths already does for the same situation.
	p.logger.Warn().Str("url", url).Str("base_path", basePath).
		Msg("Partition path is not under the storage root it was generated from; skipping existence filtering")
	return "", false
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

// InvalidateAllCachesQuiet is InvalidateAllCaches without the Info line, for
// a caller that invalidates around every chunk of a long operation.
func (p *PartitionPruner) InvalidateAllCachesQuiet() {
	p.globCache.invalidate()
	p.partitionCache.invalidate()
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
