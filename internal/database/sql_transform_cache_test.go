package database

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQueryCache_GetSet(t *testing.T) {
	cache := NewQueryCache(time.Minute, 100)

	sql := "SELECT * FROM mydb.cpu WHERE time > 123"
	transformed := "SELECT * FROM read_parquet('./data/mydb/cpu/**/*.parquet') WHERE time > 123"

	// Initially should miss
	result, ok := cache.Get(sql)
	if ok {
		t.Error("expected cache miss on empty cache")
	}

	// Set and get
	cache.Set(sql, transformed)
	result, ok = cache.Get(sql)
	if !ok {
		t.Error("expected cache hit after Set")
	}
	if result != transformed {
		t.Errorf("got %q, want %q", result, transformed)
	}

	// Different SQL should miss
	_, ok = cache.Get("SELECT * FROM other")
	if ok {
		t.Error("expected cache miss for different SQL")
	}
}

func TestQueryCache_Expiration(t *testing.T) {
	cache := NewQueryCache(10*time.Millisecond, 100)

	sql := "SELECT * FROM mydb.cpu"
	cache.Set(sql, "transformed")

	// Should hit immediately
	_, ok := cache.Get(sql)
	if !ok {
		t.Error("expected cache hit before expiration")
	}

	// Wait for expiration
	time.Sleep(15 * time.Millisecond)

	// Should miss after expiration
	_, ok = cache.Get(sql)
	if ok {
		t.Error("expected cache miss after expiration")
	}
}

func TestQueryCache_MaxSize(t *testing.T) {
	// Use a larger max size to properly test with sharded cache (16 shards)
	// Each shard gets maxSize/16 capacity, so we need at least 16+ entries
	maxSize := 32
	cache := NewQueryCache(time.Minute, maxSize)

	// Fill cache to max - use more entries than maxSize to ensure some are rejected
	for i := 0; i < maxSize+10; i++ {
		cache.Set(fmt.Sprintf("query_%d", i), fmt.Sprintf("transformed_%d", i))
	}

	// Size should not exceed max (within reasonable tolerance for shard distribution)
	// With sharding, exact size depends on hash distribution across shards
	if cache.Size() > maxSize {
		t.Errorf("cache exceeded max size: %d > %d", cache.Size(), maxSize)
	}

	// Verify we can still retrieve cached entries
	retrieved := 0
	for i := 0; i < maxSize+10; i++ {
		if _, ok := cache.Get(fmt.Sprintf("query_%d", i)); ok {
			retrieved++
		}
	}
	if retrieved == 0 {
		t.Error("expected at least some cached entries to be retrievable")
	}
}

func TestQueryCache_Invalidate(t *testing.T) {
	cache := NewQueryCache(time.Minute, 100)

	cache.Set("query1", "transformed1")
	cache.Set("query2", "transformed2")

	if cache.Size() != 2 {
		t.Errorf("expected size 2, got %d", cache.Size())
	}

	cache.Invalidate()

	if cache.Size() != 0 {
		t.Errorf("expected size 0 after invalidate, got %d", cache.Size())
	}

	_, ok := cache.Get("query1")
	if ok {
		t.Error("expected cache miss after invalidate")
	}
}

func TestQueryCache_Cleanup(t *testing.T) {
	cache := NewQueryCache(10*time.Millisecond, 100)

	cache.Set("query1", "transformed1")
	cache.Set("query2", "transformed2")

	// Wait for expiration
	time.Sleep(15 * time.Millisecond)

	removed := cache.Cleanup()
	if removed != 2 {
		t.Errorf("expected 2 entries cleaned up, got %d", removed)
	}

	if cache.Size() != 0 {
		t.Errorf("expected size 0 after cleanup, got %d", cache.Size())
	}
}

func TestQueryCache_Stats(t *testing.T) {
	cache := NewQueryCache(time.Minute, 100)

	cache.Set("query1", "transformed1")
	cache.Get("query1") // hit
	cache.Get("query1") // hit
	cache.Get("query2") // miss

	stats := cache.Stats()

	if stats["cache_hits"].(int64) != 2 {
		t.Errorf("expected 2 hits, got %d", stats["cache_hits"])
	}
	if stats["cache_misses"].(int64) != 1 {
		t.Errorf("expected 1 miss, got %d", stats["cache_misses"])
	}
	if stats["cache_size"].(int) != 1 {
		t.Errorf("expected size 1, got %d", stats["cache_size"])
	}
}

func TestQueryCache_Concurrent(t *testing.T) {
	cache := NewQueryCache(time.Minute, 1000)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sql := "query" + string(rune(n%10))
			cache.Set(sql, "transformed"+string(rune(n)))
			cache.Get(sql)
		}(i)
	}
	wg.Wait()

	// Should not panic and should have some entries
	if cache.Size() == 0 {
		t.Error("expected some entries after concurrent access")
	}
}

// BenchmarkQueryCache_Get benchmarks cache lookup performance
func BenchmarkQueryCache_Get(b *testing.B) {
	cache := NewQueryCache(time.Minute, 10000)

	// Pre-populate cache
	testSQL := "SELECT * FROM mydb.cpu WHERE time > 1609459200000000"
	transformed := "SELECT * FROM read_parquet('./data/mydb/cpu/**/*.parquet') WHERE time > 1609459200000000"
	cache.Set(testSQL, transformed)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(testSQL)
	}
}

// BenchmarkQueryCache_Set benchmarks cache write performance
func BenchmarkQueryCache_Set(b *testing.B) {
	cache := NewQueryCache(time.Minute, 100000)

	testSQL := "SELECT * FROM mydb.cpu WHERE time > 1609459200000000"
	transformed := "SELECT * FROM read_parquet('./data/mydb/cpu/**/*.parquet') WHERE time > 1609459200000000"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(testSQL, transformed)
	}
}

// BenchmarkQueryCache_Hash benchmarks the per-call key-hashing overhead — the
// FNV-1a shard selection that replaced the old SHA256+hex key derivation (#331).
func BenchmarkQueryCache_Hash(b *testing.B) {
	cache := NewQueryCache(time.Minute, 100)
	testSQL := "SELECT * FROM mydb.cpu WHERE time > 1609459200000000 AND host = 'server01' GROUP BY time ORDER BY time DESC LIMIT 100"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.shardFor(testSQL)
	}
}

// TestQueryCache_DistinctKeysNoCollision verifies that different SQL strings are
// stored as distinct entries even when they hash to the same shard — the map key
// is the full SQL string, so there is no cross-query collision (#331). Uses many
// distinct queries so some necessarily share a shard (pigeonhole: >16 keys).
func TestQueryCache_DistinctKeysNoCollision(t *testing.T) {
	cache := NewQueryCache(time.Minute, 100000)

	const n = 500
	for i := 0; i < n; i++ {
		sql := fmt.Sprintf("SELECT * FROM db.m WHERE id = %d", i)
		cache.Set(sql, fmt.Sprintf("transformed_%d", i))
	}

	// Every query must return its OWN transformed value, never another's.
	for i := 0; i < n; i++ {
		sql := fmt.Sprintf("SELECT * FROM db.m WHERE id = %d", i)
		got, ok := cache.Get(sql)
		if !ok {
			t.Fatalf("query %d missing from cache", i)
		}
		want := fmt.Sprintf("transformed_%d", i)
		if got != want {
			t.Fatalf("query %d returned wrong value: got %q, want %q (cross-key collision)", i, got, want)
		}
	}
}

// TestQueryCache_ShardDistribution verifies FNV-1a spreads distinct keys across
// all shards rather than piling them into a few (the old scheme keyed the shard
// off the first hex character of a SHA256 digest). We don't require perfect
// balance — just that every shard receives at least one of a large key set, so
// no shard is starved and none is a hotspot for the whole workload.
func TestQueryCache_ShardDistribution(t *testing.T) {
	cache := NewQueryCache(time.Minute, 1000000)

	used := make(map[*cacheShard]int)
	for i := 0; i < 10000; i++ {
		sql := fmt.Sprintf("SELECT col_%d FROM measurement_%d WHERE ts > %d", i%97, i%53, i)
		used[cache.shardFor(sql)]++
	}

	if len(used) != cacheShardCount {
		t.Errorf("expected all %d shards to receive keys, got %d distinct shards", cacheShardCount, len(used))
	}
	// Sanity: no shard should hold a wildly disproportionate share. With 10k keys
	// over 16 shards the mean is 625; flag if any shard exceeds 3x the mean.
	for shard, count := range used {
		if count > 3*(10000/cacheShardCount) {
			t.Errorf("shard %p over-loaded: %d keys (mean ~%d) — poor distribution", shard, count, 10000/cacheShardCount)
		}
	}
}
