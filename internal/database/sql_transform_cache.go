package database

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// SQLTransformCacheTTL is the default TTL for SQL transform cache entries (60 seconds)
const SQLTransformCacheTTL = 60 * time.Second

// DefaultSQLTransformCacheMaxSize is the default maximum number of cached entries
const DefaultSQLTransformCacheMaxSize = 10000

// cacheShardCount is the number of shards to distribute lock contention
// 16 shards means ~16x reduction in lock contention under concurrent load
const cacheShardCount = 16

// Aliases for backwards compatibility
const QueryCacheTTL = SQLTransformCacheTTL
const DefaultQueryCacheMaxSize = DefaultSQLTransformCacheMaxSize

// sqlTransformCacheEntry represents a cached transformed SQL with expiration
type sqlTransformCacheEntry struct {
	transformed string
	metadata    []string
	expiresAt   time.Time
	// stale, when set, is asked on every read: true makes the entry a miss.
	// It lets the owner tie an entry to state the key cannot name (which
	// measurements the transform read is known only after it ran).
	stale func() bool
}

// cacheShard is a single shard of the cache with its own lock
type cacheShard struct {
	mu      sync.RWMutex
	entries map[string]sqlTransformCacheEntry
}

// SQLTransformCache provides a sharded TTL cache for SQL-to-storage-path transformations.
// This caches the result of converting table references (e.g., FROM mydb.cpu)
// to DuckDB read_parquet() calls (e.g., FROM read_parquet('./data/mydb/cpu/**/*.parquet')).
// This is NOT a query results cache or query plan cache - it only caches the
// regex-based string transformation that happens before DuckDB sees the query.
//
// The cache is sharded into 16 buckets to reduce lock contention under concurrent load.
// Each shard has its own RWMutex, so concurrent queries hitting different shards
// don't block each other.
type SQLTransformCache struct {
	shards       [cacheShardCount]*cacheShard
	ttl          time.Duration
	maxSizeTotal int // Total max size across all shards
	hits         atomic.Int64
	misses       atomic.Int64
	evictions    atomic.Int64
}

// NewSQLTransformCache creates a new SQL transform cache with the specified TTL and max size
func NewSQLTransformCache(ttl time.Duration, maxSize int) *SQLTransformCache {
	if ttl <= 0 {
		ttl = SQLTransformCacheTTL
	}
	if maxSize <= 0 {
		maxSize = DefaultSQLTransformCacheMaxSize
	}

	c := &SQLTransformCache{
		ttl:          ttl,
		maxSizeTotal: maxSize,
	}

	// Initialize all shards
	for i := 0; i < cacheShardCount; i++ {
		c.shards[i] = &cacheShard{
			entries: make(map[string]sqlTransformCacheEntry),
		}
	}

	return c
}

// NewQueryCache creates a new query cache (alias for backwards compatibility)
func NewQueryCache(ttl time.Duration, maxSize int) *SQLTransformCache {
	return NewSQLTransformCache(ttl, maxSize)
}

// QueryCache is an alias for SQLTransformCache (backwards compatibility)
type QueryCache = SQLTransformCache

// shardFor selects a shard for a cache key using FNV-1a.
//
// The full key string is stored as the map key (see Get/Set), so lookups are an
// exact string match — there is NO risk of two different queries colliding onto
// one entry, unlike a scheme that hashes the SQL into the key. This hash is used
// ONLY to pick a shard: a hash collision here merely lands two distinct keys in
// the same shard, which is harmless (they remain separate map entries). That
// lets us use a fast non-cryptographic hash — SHA256 was needlessly expensive
// (a 256-bit crypto digest plus a hex-encoding allocation) for an internal cache
// key with no adversarial-collision concern (#331).
func (c *SQLTransformCache) shardFor(key string) *cacheShard {
	h := fnv.New32a()
	// fnv Write never returns an error; ignore it.
	_, _ = h.Write([]byte(key))
	idx := h.Sum32() % cacheShardCount
	return c.shards[idx]
}

// Get retrieves a cached transformed SQL if it exists and hasn't expired
func (c *SQLTransformCache) Get(sql string) (string, bool) {
	transformed, _, ok := c.GetWithMetadata(sql)
	return transformed, ok
}

// GetWithMetadata retrieves a cached transformed SQL and the metadata stored
// with it. The metadata is the cache's own: callers must not modify it.
func (c *SQLTransformCache) GetWithMetadata(sql string) (string, []string, bool) {
	shard := c.shardFor(sql)

	shard.mu.RLock()
	entry, ok := shard.entries[sql]
	shard.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		return "", nil, false
	}

	if time.Now().After(entry.expiresAt) || (entry.stale != nil && entry.stale()) {
		c.misses.Add(1)
		return "", nil, false
	}

	c.hits.Add(1)
	return entry.transformed, entry.metadata, true
}

// Set stores a transformed SQL in the cache
func (c *SQLTransformCache) Set(sql, transformed string) {
	c.SetWithMetadata(sql, transformed, nil)
}

// SetWithMetadata stores transformed SQL with immutable metadata that remains
// associated with the transform on cache hits.
func (c *SQLTransformCache) SetWithMetadata(sql, transformed string, metadata []string) {
	c.SetWithMetadataCheck(sql, transformed, metadata, nil)
}

// SetWithMetadataCheck is SetWithMetadata with a staleness check: stale is
// called on every read of the entry, and a true answer makes the entry a
// miss. It is also called while the shard is locked, when a full shard looks
// for entries to evict, so it must not call back into the cache; it must be
// cheap and safe to call from any goroutine. nil means the entry only
// expires.
func (c *SQLTransformCache) SetWithMetadataCheck(sql, transformed string, metadata []string, stale func() bool) {
	shard := c.shardFor(sql)

	shard.mu.Lock()

	// Calculate per-shard max size (distribute evenly, minimum 1 per shard)
	maxPerShard := c.maxSizeTotal / cacheShardCount
	if maxPerShard < 1 {
		maxPerShard = 1
	}

	// Check if we need to evict entries from this shard. Replacing an entry
	// that is already there never grows the shard, so it is always allowed:
	// otherwise a stale entry in a full shard could not be refreshed, and its
	// statement would be transformed again on every query until it expired.
	if _, replacing := shard.entries[sql]; !replacing && len(shard.entries) >= maxPerShard {
		// Probabilistic eviction: only check up to 10 random entries
		// This avoids O(n) scan of entire map while holding lock
		now := time.Now()
		evicted := 0
		for key, entry := range shard.entries {
			if now.After(entry.expiresAt) || (entry.stale != nil && entry.stale()) {
				delete(shard.entries, key)
				evicted++
				if evicted >= 10 {
					break
				}
			}
		}
		c.evictions.Add(int64(evicted))

		// If still at capacity after eviction, skip caching this entry
		if len(shard.entries) >= maxPerShard {
			shard.mu.Unlock()
			return
		}
	}

	shard.entries[sql] = sqlTransformCacheEntry{
		transformed: transformed,
		metadata:    append([]string(nil), metadata...),
		expiresAt:   time.Now().Add(c.ttl),
		stale:       stale,
	}
	shard.mu.Unlock()
}

// Invalidate removes all entries from the cache
func (c *SQLTransformCache) Invalidate() {
	for i := 0; i < cacheShardCount; i++ {
		shard := c.shards[i]
		shard.mu.Lock()
		shard.entries = make(map[string]sqlTransformCacheEntry)
		shard.mu.Unlock()
	}
}

// Cleanup removes expired entries from the cache
func (c *SQLTransformCache) Cleanup() int {
	now := time.Now()
	totalRemoved := 0

	for i := 0; i < cacheShardCount; i++ {
		shard := c.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.entries {
			if now.After(entry.expiresAt) {
				delete(shard.entries, key)
				totalRemoved++
			}
		}
		shard.mu.Unlock()
	}

	return totalRemoved
}

// Stats returns cache statistics as a map
func (c *SQLTransformCache) Stats() map[string]interface{} {
	totalSize := 0
	for i := 0; i < cacheShardCount; i++ {
		shard := c.shards[i]
		shard.mu.RLock()
		totalSize += len(shard.entries)
		shard.mu.RUnlock()
	}

	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return map[string]interface{}{
		"cache_size":       totalSize,
		"cache_max_size":   c.maxSizeTotal,
		"cache_shards":     cacheShardCount,
		"cache_hits":       hits,
		"cache_misses":     misses,
		"hit_rate_percent": hitRate,
		"evictions":        c.evictions.Load(),
		"ttl_seconds":      c.ttl.Seconds(),
	}
}

// Size returns the current number of entries in the cache
func (c *SQLTransformCache) Size() int {
	totalSize := 0
	for i := 0; i < cacheShardCount; i++ {
		shard := c.shards[i]
		shard.mu.RLock()
		totalSize += len(shard.entries)
		shard.mu.RUnlock()
	}
	return totalSize
}
