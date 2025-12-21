package pruning

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestNewPartitionPruner tests pruner creation
func TestNewPartitionPruner(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPartitionPruner(logger)

	if p == nil {
		t.Fatal("NewPartitionPruner returned nil")
	}
	if !p.enabled {
		t.Error("Pruner should be enabled by default")
	}
}

// TestExtractTimeRange tests time range extraction from SQL queries
func TestExtractTimeRange(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPartitionPruner(logger)

	tests := []struct {
		name      string
		sql       string
		wantStart string
		wantEnd   string
		wantNil   bool
	}{
		{
			name:      "basic >= and <",
			sql:       "SELECT * FROM cpu WHERE time >= '2024-03-15' AND time < '2024-03-16'",
			wantStart: "2024-03-15",
			wantEnd:   "2024-03-16",
		},
		{
			name:      "with timestamp",
			sql:       "SELECT * FROM cpu WHERE time >= '2024-03-15 10:00:00' AND time < '2024-03-15 12:00:00'",
			wantStart: "2024-03-15 10:00:00",
			wantEnd:   "2024-03-15 12:00:00",
		},
		{
			name:      "RFC3339 format",
			sql:       "SELECT * FROM cpu WHERE time >= '2024-03-15T10:00:00Z' AND time < '2024-03-15T12:00:00Z'",
			wantStart: "2024-03-15T10:00:00Z",
			wantEnd:   "2024-03-15T12:00:00Z",
		},
		{
			name:      "BETWEEN clause",
			sql:       "SELECT * FROM cpu WHERE time BETWEEN '2024-03-15' AND '2024-03-16'",
			wantStart: "2024-03-15",
			wantEnd:   "2024-03-16",
		},
		{
			name:      "only start time",
			sql:       "SELECT * FROM cpu WHERE time >= '2024-03-15'",
			wantStart: "2024-03-15",
			wantEnd:   "", // Will be computed as now + 1 day
		},
		{
			name:      "only end time",
			sql:       "SELECT * FROM cpu WHERE time < '2024-03-16'",
			wantStart: "", // Will default to 2020-01-01
			wantEnd:   "2024-03-16",
		},
		{
			name:    "no WHERE clause",
			sql:     "SELECT * FROM cpu",
			wantNil: true,
		},
		{
			name:    "no time condition",
			sql:     "SELECT * FROM cpu WHERE host = 'server1'",
			wantNil: true,
		},
		{
			name:      "with GROUP BY",
			sql:       "SELECT * FROM cpu WHERE time >= '2024-03-15' AND time < '2024-03-16' GROUP BY host",
			wantStart: "2024-03-15",
			wantEnd:   "2024-03-16",
		},
		{
			name:      "with LIMIT",
			sql:       "SELECT * FROM cpu WHERE time >= '2024-03-15' AND time < '2024-03-16' LIMIT 100",
			wantStart: "2024-03-15",
			wantEnd:   "2024-03-16",
		},
		{
			name:      "lowercase where",
			sql:       "select * from cpu where time >= '2024-03-15' and time < '2024-03-16'",
			wantStart: "2024-03-15",
			wantEnd:   "2024-03-16",
		},
		{
			name:      "mixed case",
			sql:       "SELECT * FROM cpu Where TIME >= '2024-03-15' AND time < '2024-03-16'",
			wantStart: "2024-03-15",
			wantEnd:   "2024-03-16",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := p.ExtractTimeRange(tt.sql)

			if tt.wantNil {
				if tr != nil {
					t.Errorf("Expected nil time range, got %+v", tr)
				}
				return
			}

			if tr == nil {
				t.Fatal("Expected non-nil time range, got nil")
			}

			if tt.wantStart != "" {
				expectedStart, _ := parseDateTime(tt.wantStart)
				if !tr.Start.Equal(expectedStart) {
					t.Errorf("Start time = %v, want %v", tr.Start, expectedStart)
				}
			}

			if tt.wantEnd != "" {
				expectedEnd, _ := parseDateTime(tt.wantEnd)
				if !tr.End.Equal(expectedEnd) {
					t.Errorf("End time = %v, want %v", tr.End, expectedEnd)
				}
			}
		})
	}
}

// TestParseDateTime tests the datetime parsing function
func TestParseDateTime(t *testing.T) {
	tests := []struct {
		input    string
		wantErr  bool
		wantYear int
	}{
		{"2024-03-15", false, 2024},
		{"2024-03-15 10:30:00", false, 2024},
		{"2024-03-15T10:30:00Z", false, 2024},
		{"2024/03/15", false, 2024},
		{"2024/03/15 10:30:00", false, 2024},
		{"2024-03-15 10:30", false, 2024},
		{"invalid", true, 0},
		{"", true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseDateTime(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error for input %q", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.Year() != tt.wantYear {
				t.Errorf("Year = %d, want %d", result.Year(), tt.wantYear)
			}
		})
	}
}

// TestGeneratePartitionPaths tests partition path generation
func TestGeneratePartitionPaths(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPartitionPruner(logger)

	t.Run("single hour", func(t *testing.T) {
		start, _ := parseDateTime("2024-03-15 10:00:00")
		end, _ := parseDateTime("2024-03-15 11:00:00")
		tr := &TimeRange{Start: start, End: end}

		paths := p.GeneratePartitionPaths("/data", "mydb", "cpu", tr)

		// Should have hourly path + daily path
		if len(paths) < 1 {
			t.Errorf("Expected at least 1 path, got %d", len(paths))
		}

		// Check a path contains expected components
		found := false
		for _, path := range paths {
			if contains(path, "2024") && contains(path, "03") && contains(path, "15") {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected path containing date components")
		}
	})

	t.Run("multiple hours", func(t *testing.T) {
		start, _ := parseDateTime("2024-03-15 10:00:00")
		end, _ := parseDateTime("2024-03-15 14:00:00")
		tr := &TimeRange{Start: start, End: end}

		paths := p.GeneratePartitionPaths("/data", "mydb", "cpu", tr)

		// Should have 4 hourly paths + 1 daily path = 5
		if len(paths) != 5 {
			t.Errorf("Expected 5 paths for 4-hour range, got %d", len(paths))
		}
	})

	t.Run("multiple days", func(t *testing.T) {
		start, _ := parseDateTime("2024-03-15 00:00:00")
		end, _ := parseDateTime("2024-03-17 00:00:00")
		tr := &TimeRange{Start: start, End: end}

		paths := p.GeneratePartitionPaths("/data", "mydb", "cpu", tr)

		// 48 hourly paths + 2 daily paths
		expectedHourly := 48
		expectedDaily := 2
		if len(paths) != expectedHourly+expectedDaily {
			t.Errorf("Expected %d paths, got %d", expectedHourly+expectedDaily, len(paths))
		}
	})

	t.Run("nil time range", func(t *testing.T) {
		paths := p.GeneratePartitionPaths("/data", "mydb", "cpu", nil)

		if paths != nil {
			t.Errorf("Expected nil for nil time range, got %v", paths)
		}
	})
}

// TestOptimizeTablePath tests table path optimization
func TestOptimizeTablePath(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPartitionPruner(logger)

	t.Run("no time range", func(t *testing.T) {
		path := "/data/mydb/cpu/**/*.parquet"
		sql := "SELECT * FROM cpu WHERE host = 'server1'"

		result, optimized := p.OptimizeTablePath(path, sql)

		if optimized {
			t.Error("Should not be optimized without time range")
		}
		if result != path {
			t.Errorf("Result = %v, want original path", result)
		}
	})

	t.Run("with time range - non-local path", func(t *testing.T) {
		// Use s3:// prefix to avoid local filesystem filtering
		path := "s3://bucket/mydb/cpu/**/*.parquet"
		sql := "SELECT * FROM cpu WHERE time >= '2024-03-15' AND time < '2024-03-16'"

		result, optimized := p.OptimizeTablePath(path, sql)

		if !optimized {
			t.Error("Should be optimized with time range for non-local path")
		}

		// Result could be a string or []string
		switch r := result.(type) {
		case string:
			if !contains(r, "2024") || !contains(r, "03") || !contains(r, "15") {
				t.Errorf("Optimized path should contain date components: %s", r)
			}
		case []string:
			if len(r) == 0 {
				t.Error("Expected non-empty path list")
			}
			// Check that paths contain date components
			hasDatePath := false
			for _, p := range r {
				if contains(p, "2024") && contains(p, "03") && contains(p, "15") {
					hasDatePath = true
					break
				}
			}
			if !hasDatePath {
				t.Error("Expected at least one path with date components")
			}
		default:
			t.Errorf("Unexpected result type: %T", result)
		}
	})

	t.Run("invalid path format", func(t *testing.T) {
		path := "/invalid/path"
		sql := "SELECT * FROM cpu WHERE time >= '2024-03-15' AND time < '2024-03-16'"

		result, optimized := p.OptimizeTablePath(path, sql)

		if optimized {
			t.Error("Invalid path should not be optimized")
		}
		if result != path {
			t.Error("Should return original path for invalid format")
		}
	})

	t.Run("disabled pruner", func(t *testing.T) {
		p.enabled = false
		path := "/data/mydb/cpu/**/*.parquet"
		sql := "SELECT * FROM cpu WHERE time >= '2024-03-15' AND time < '2024-03-16'"

		result, optimized := p.OptimizeTablePath(path, sql)

		if optimized {
			t.Error("Disabled pruner should not optimize")
		}
		if result != path {
			t.Error("Should return original path when disabled")
		}
		p.enabled = true // Re-enable for other tests
	})
}

// TestGlobCache tests the glob cache functionality
func TestGlobCache(t *testing.T) {
	cache := newGlobCache(100 * time.Millisecond)

	t.Run("set and get", func(t *testing.T) {
		pattern := "/data/*.parquet"
		matches := []string{"file1.parquet", "file2.parquet"}

		cache.set(pattern, matches)

		result, ok := cache.get(pattern)
		if !ok {
			t.Error("Expected cache hit")
		}
		if len(result) != len(matches) {
			t.Errorf("Expected %d matches, got %d", len(matches), len(result))
		}
	})

	t.Run("cache miss", func(t *testing.T) {
		_, ok := cache.get("/nonexistent/*.parquet")
		if ok {
			t.Error("Expected cache miss for unknown pattern")
		}
	})

	t.Run("expiration", func(t *testing.T) {
		pattern := "/expiring/*.parquet"
		cache.set(pattern, []string{"file.parquet"})

		// Wait for expiration
		time.Sleep(150 * time.Millisecond)

		_, ok := cache.get(pattern)
		if ok {
			t.Error("Expected cache miss after expiration")
		}
	})

	t.Run("invalidate", func(t *testing.T) {
		cache.set("/test1/*.parquet", []string{"a.parquet"})
		cache.set("/test2/*.parquet", []string{"b.parquet"})

		cache.invalidate()

		_, ok1 := cache.get("/test1/*.parquet")
		_, ok2 := cache.get("/test2/*.parquet")
		if ok1 || ok2 {
			t.Error("Expected all entries to be invalidated")
		}
	})

	t.Run("cleanup", func(t *testing.T) {
		shortCache := newGlobCache(50 * time.Millisecond)
		shortCache.set("/cleanup1/*.parquet", []string{"a.parquet"})
		shortCache.set("/cleanup2/*.parquet", []string{"b.parquet"})

		time.Sleep(60 * time.Millisecond)

		removed := shortCache.cleanup()
		if removed != 2 {
			t.Errorf("Expected 2 entries removed, got %d", removed)
		}
	})

	t.Run("stats", func(t *testing.T) {
		statsCache := newGlobCache(1 * time.Second)

		// Cause some hits and misses
		statsCache.set("/hit/*.parquet", []string{"file.parquet"})
		statsCache.get("/hit/*.parquet")      // hit
		statsCache.get("/hit/*.parquet")      // hit
		statsCache.get("/miss/*.parquet")     // miss
		statsCache.get("/miss2/*.parquet")    // miss

		hits, misses, size := statsCache.stats()
		if hits != 2 {
			t.Errorf("Expected 2 hits, got %d", hits)
		}
		if misses != 2 {
			t.Errorf("Expected 2 misses, got %d", misses)
		}
		if size != 1 {
			t.Errorf("Expected size 1, got %d", size)
		}
	})
}

// TestPartitionCache tests the partition cache functionality
func TestPartitionCache(t *testing.T) {
	cache := newPartitionCache(100 * time.Millisecond)

	t.Run("cache key generation", func(t *testing.T) {
		key1 := cache.cacheKey("/data/mydb/cpu/**/*.parquet", "SELECT * FROM cpu")
		key2 := cache.cacheKey("/data/mydb/cpu/**/*.parquet", "SELECT * FROM cpu")
		key3 := cache.cacheKey("/data/mydb/cpu/**/*.parquet", "SELECT * FROM memory")

		if key1 != key2 {
			t.Error("Same inputs should produce same key")
		}
		if key1 == key3 {
			t.Error("Different SQL should produce different key")
		}
	})

	t.Run("set and get", func(t *testing.T) {
		key := "test-key-1"
		paths := []string{"path1", "path2"}

		cache.set(key, paths, true)

		result, optimized, ok := cache.get(key)
		if !ok {
			t.Error("Expected cache hit")
		}
		if !optimized {
			t.Error("Expected optimized=true")
		}
		resultPaths, ok := result.([]string)
		if !ok || len(resultPaths) != 2 {
			t.Error("Expected 2 paths in result")
		}
	})

	t.Run("expiration", func(t *testing.T) {
		key := "expiring-key"
		cache.set(key, "result", true)

		time.Sleep(150 * time.Millisecond)

		_, _, ok := cache.get(key)
		if ok {
			t.Error("Expected cache miss after expiration")
		}
	})

	t.Run("stats", func(t *testing.T) {
		statsCache := newPartitionCache(1 * time.Second)

		statsCache.set("hit-key", "result", true)
		statsCache.get("hit-key")    // hit
		statsCache.get("miss-key")   // miss

		hits, misses, size := statsCache.stats()
		if hits != 1 {
			t.Errorf("Expected 1 hit, got %d", hits)
		}
		if misses != 1 {
			t.Errorf("Expected 1 miss, got %d", misses)
		}
		if size != 1 {
			t.Errorf("Expected size 1, got %d", size)
		}
	})
}

// TestPrunerStats tests statistics tracking
func TestPrunerStats(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPartitionPruner(logger)

	// Initial stats should be zero
	stats := p.GetStats()
	if stats.QueriesOptimized != 0 {
		t.Errorf("Initial QueriesOptimized = %d, want 0", stats.QueriesOptimized)
	}

	// Generate some partition paths (this increments QueriesOptimized)
	start, _ := parseDateTime("2024-03-15")
	end, _ := parseDateTime("2024-03-16")
	p.GeneratePartitionPaths("/data", "db", "cpu", &TimeRange{Start: start, End: end})

	stats = p.GetStats()
	if stats.QueriesOptimized != 1 {
		t.Errorf("QueriesOptimized = %d, want 1", stats.QueriesOptimized)
	}

	// Reset stats
	p.ResetStats()
	stats = p.GetStats()
	if stats.QueriesOptimized != 0 {
		t.Errorf("QueriesOptimized after reset = %d, want 0", stats.QueriesOptimized)
	}
}

// TestCacheInvalidation tests cache invalidation methods
func TestCacheInvalidation(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPartitionPruner(logger)

	// Populate caches
	p.globCache.set("/test/*.parquet", []string{"file.parquet"})
	p.partitionCache.set("test-key", "result", true)

	t.Run("invalidate glob cache", func(t *testing.T) {
		p.InvalidateGlobCache()

		_, ok := p.globCache.get("/test/*.parquet")
		if ok {
			t.Error("Glob cache should be invalidated")
		}

		// Partition cache should still work
		_, _, ok = p.partitionCache.get("test-key")
		if !ok {
			t.Error("Partition cache should not be affected")
		}
	})

	// Repopulate glob cache
	p.globCache.set("/test/*.parquet", []string{"file.parquet"})

	t.Run("invalidate partition cache", func(t *testing.T) {
		p.InvalidatePartitionCache()

		_, _, ok := p.partitionCache.get("test-key")
		if ok {
			t.Error("Partition cache should be invalidated")
		}

		// Glob cache should still work
		_, ok = p.globCache.get("/test/*.parquet")
		if !ok {
			t.Error("Glob cache should not be affected")
		}
	})

	// Repopulate both
	p.globCache.set("/test/*.parquet", []string{"file.parquet"})
	p.partitionCache.set("test-key", "result", true)

	t.Run("invalidate all caches", func(t *testing.T) {
		p.InvalidateAllCaches()

		_, ok1 := p.globCache.get("/test/*.parquet")
		_, _, ok2 := p.partitionCache.get("test-key")
		if ok1 || ok2 {
			t.Error("All caches should be invalidated")
		}
	})
}

// TestGetCacheStats tests cache statistics retrieval
func TestGetCacheStats(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPartitionPruner(logger)

	// Generate some cache activity
	p.globCache.set("/test/*.parquet", []string{"file.parquet"})
	p.globCache.get("/test/*.parquet") // hit
	p.globCache.get("/miss/*.parquet") // miss

	t.Run("glob cache stats", func(t *testing.T) {
		stats := p.GetGlobCacheStats()

		if stats["cache_size"].(int) != 1 {
			t.Errorf("cache_size = %v, want 1", stats["cache_size"])
		}
		if stats["cache_hits"].(int64) != 1 {
			t.Errorf("cache_hits = %v, want 1", stats["cache_hits"])
		}
		if stats["cache_misses"].(int64) != 1 {
			t.Errorf("cache_misses = %v, want 1", stats["cache_misses"])
		}
		if stats["hit_rate_percent"].(float64) != 50.0 {
			t.Errorf("hit_rate_percent = %v, want 50", stats["hit_rate_percent"])
		}
	})

	t.Run("partition cache stats", func(t *testing.T) {
		stats := p.GetPartitionCacheStats()

		if _, ok := stats["cache_size"]; !ok {
			t.Error("Expected cache_size in stats")
		}
		if _, ok := stats["ttl_seconds"]; !ok {
			t.Error("Expected ttl_seconds in stats")
		}
	})

	t.Run("all cache stats", func(t *testing.T) {
		stats := p.GetAllCacheStats()

		if _, ok := stats["glob_cache"]; !ok {
			t.Error("Expected glob_cache in stats")
		}
		if _, ok := stats["partition_cache"]; !ok {
			t.Error("Expected partition_cache in stats")
		}
	})
}

// TestFilterExistingPaths tests filtering of non-existent paths
func TestFilterExistingPaths(t *testing.T) {
	logger := zerolog.Nop()
	p := NewPartitionPruner(logger)

	// Create a temporary directory with test files
	tmpDir, err := os.MkdirTemp("", "arc-pruner-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test structure
	existingDir := filepath.Join(tmpDir, "2024", "03", "15")
	if err := os.MkdirAll(existingDir, 0755); err != nil {
		t.Fatalf("Failed to create test dir: %v", err)
	}

	testFile := filepath.Join(existingDir, "test.parquet")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	t.Run("filters existing paths", func(t *testing.T) {
		paths := []string{
			filepath.Join(existingDir, "*.parquet"),
			filepath.Join(tmpDir, "nonexistent", "*.parquet"),
		}

		filtered := p.filterExistingPaths(paths)

		if len(filtered) != 1 {
			t.Errorf("Expected 1 existing path, got %d", len(filtered))
		}
	})

	t.Run("caches results", func(t *testing.T) {
		pattern := filepath.Join(existingDir, "*.parquet")

		// First call - cache miss
		p.filterExistingPaths([]string{pattern})

		// Second call should use cache
		hits1, _, _ := p.globCache.stats()
		p.filterExistingPaths([]string{pattern})
		hits2, _, _ := p.globCache.stats()

		if hits2 <= hits1 {
			t.Error("Expected cache hit on second call")
		}
	})
}

// TestPartitionCacheConcurrency tests concurrent access to partition cache
func TestPartitionCacheConcurrency(t *testing.T) {
	cache := newPartitionCache(1 * time.Second)

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent writes
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10) // Some key overlap
			cache.set(key, fmt.Sprintf("value-%d", i), i%2 == 0)
		}(i)
	}

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			cache.get(key)
		}(i)
	}

	wg.Wait()

	// Should not panic and cache should have some entries
	_, _, size := cache.stats()
	if size == 0 {
		t.Error("Expected some entries in cache after concurrent access")
	}
}

// TestGlobCacheConcurrency tests concurrent access to glob cache
func TestGlobCacheConcurrency(t *testing.T) {
	cache := newGlobCache(1 * time.Second)

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent writes
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pattern := fmt.Sprintf("/data/%d/*.parquet", i%10)
			cache.set(pattern, []string{fmt.Sprintf("file%d.parquet", i)})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pattern := fmt.Sprintf("/data/%d/*.parquet", i%10)
			cache.get(pattern)
		}(i)
	}

	// Concurrent cleanup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.cleanup()
		}()
	}

	wg.Wait()

	// Should not panic
}

// TestMinFunction tests the min helper function
func TestMinFunction(t *testing.T) {
	tests := []struct {
		a, b, want int
	}{
		{1, 2, 1},
		{2, 1, 1},
		{5, 5, 5},
		{0, 10, 0},
		{-1, 1, -1},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("min(%d,%d)", tt.a, tt.b), func(t *testing.T) {
			if got := min(tt.a, tt.b); got != tt.want {
				t.Errorf("min(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// Helper function
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
