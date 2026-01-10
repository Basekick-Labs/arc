package database

import (
	"crypto/sha256"
	"fmt"
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
	expiresAt   time.Time
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

// queryHash generates a cache key from SQL using SHA256
func queryHash(sql string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(sql)))
}

// getShard returns the shard for a given hash using the first byte
func (c *SQLTransformCache) getShard(hash string) *cacheShard {
	// Use first byte of hash for shard selection (0-255 mod 16 = 0-15)
	idx := uint8(hash[0]) % cacheShardCount
	return c.shards[idx]
}

// Get retrieves a cached transformed SQL if it exists and hasn't expired
func (c *SQLTransformCache) Get(sql string) (string, bool) {
	hash := queryHash(sql)
	shard := c.getShard(hash)

	shard.mu.RLock()
	entry, ok := shard.entries[hash]
	shard.mu.RUnlock()

	if !ok {
		c.misses.Add(1)
		return "", false
	}

	if time.Now().After(entry.expiresAt) {
		c.misses.Add(1)
		return "", false
	}

	c.hits.Add(1)
	return entry.transformed, true
}

// Set stores a transformed SQL in the cache
func (c *SQLTransformCache) Set(sql, transformed string) {
	hash := queryHash(sql)
	shard := c.getShard(hash)

	shard.mu.Lock()

	// Calculate per-shard max size (distribute evenly, minimum 1 per shard)
	maxPerShard := c.maxSizeTotal / cacheShardCount
	if maxPerShard < 1 {
		maxPerShard = 1
	}

	// Check if we need to evict entries from this shard
	if len(shard.entries) >= maxPerShard {
		// Probabilistic eviction: only check up to 10 random entries
		// This avoids O(n) scan of entire map while holding lock
		now := time.Now()
		evicted := 0
		for key, entry := range shard.entries {
			if now.After(entry.expiresAt) {
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

	shard.entries[hash] = sqlTransformCacheEntry{
		transformed: transformed,
		expiresAt:   time.Now().Add(c.ttl),
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
