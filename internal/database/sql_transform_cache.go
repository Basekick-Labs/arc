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

// Aliases for backwards compatibility
const QueryCacheTTL = SQLTransformCacheTTL
const DefaultQueryCacheMaxSize = DefaultSQLTransformCacheMaxSize

// sqlTransformCacheEntry represents a cached transformed SQL with expiration
type sqlTransformCacheEntry struct {
	transformed string
	expiresAt   time.Time
}

// SQLTransformCache provides a TTL cache for SQL-to-storage-path transformations.
// This caches the result of converting table references (e.g., FROM mydb.cpu)
// to DuckDB read_parquet() calls (e.g., FROM read_parquet('./data/mydb/cpu/**/*.parquet')).
// This is NOT a query results cache or query plan cache - it only caches the
// regex-based string transformation that happens before DuckDB sees the query.
type SQLTransformCache struct {
	mu         sync.RWMutex
	entries    map[string]sqlTransformCacheEntry
	ttl        time.Duration
	maxSize    int
	hits       atomic.Int64
	misses     atomic.Int64
	evictions  atomic.Int64
}

// NewSQLTransformCache creates a new SQL transform cache with the specified TTL and max size
func NewSQLTransformCache(ttl time.Duration, maxSize int) *SQLTransformCache {
	if ttl <= 0 {
		ttl = SQLTransformCacheTTL
	}
	if maxSize <= 0 {
		maxSize = DefaultSQLTransformCacheMaxSize
	}
	return &SQLTransformCache{
		entries: make(map[string]sqlTransformCacheEntry),
		ttl:     ttl,
		maxSize: maxSize,
	}
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

// Get retrieves a cached transformed SQL if it exists and hasn't expired
func (c *SQLTransformCache) Get(sql string) (string, bool) {
	hash := queryHash(sql)

	c.mu.RLock()
	entry, ok := c.entries[hash]
	c.mu.RUnlock()

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

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we need to evict entries
	if len(c.entries) >= c.maxSize {
		// Simple eviction: remove expired entries first
		now := time.Now()
		removed := 0
		for key, entry := range c.entries {
			if now.After(entry.expiresAt) {
				delete(c.entries, key)
				removed++
			}
		}
		c.evictions.Add(int64(removed))

		// If still at capacity, skip caching this entry
		// (LRU would be more sophisticated but adds complexity)
		if len(c.entries) >= c.maxSize {
			return
		}
	}

	c.entries[hash] = sqlTransformCacheEntry{
		transformed: transformed,
		expiresAt:   time.Now().Add(c.ttl),
	}
}

// Invalidate removes all entries from the cache
func (c *SQLTransformCache) Invalidate() {
	c.mu.Lock()
	c.entries = make(map[string]sqlTransformCacheEntry)
	c.mu.Unlock()
}

// Cleanup removes expired entries from the cache
func (c *SQLTransformCache) Cleanup() int {
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

// Stats returns cache statistics as a map
func (c *SQLTransformCache) Stats() map[string]interface{} {
	c.mu.RLock()
	size := len(c.entries)
	c.mu.RUnlock()

	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return map[string]interface{}{
		"cache_size":       size,
		"cache_max_size":   c.maxSize,
		"cache_hits":       hits,
		"cache_misses":     misses,
		"hit_rate_percent": hitRate,
		"evictions":        c.evictions.Load(),
		"ttl_seconds":      c.ttl.Seconds(),
	}
}

// Size returns the current number of entries in the cache
func (c *SQLTransformCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
