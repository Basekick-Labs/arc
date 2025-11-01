# Query Cache Improvements

**Date:** 2025-11-01
**Status:** Planning
**Priority:** High Impact Optimizations

## Overview

This document outlines planned improvements to Arc's query result cache (`api/query_cache.py`) to significantly improve cache hit rates, memory efficiency, and overall query performance.

## Current Implementation

The current query cache (`QueryCache` class) provides:
- In-memory result caching with TTL expiration (default 60s)
- LRU eviction when cache is full (max 100 entries)
- Size-based filtering (max 10MB per result)
- Thread-safe operations
- Basic hit/miss metrics

**Current Limitations:**
1. No query normalization - similar queries with different literals miss cache
2. No compression - large results consume excessive memory
3. Simple LRU eviction - doesn't consider query frequency/importance
4. Rough size estimation (100 bytes/cell estimate)

## Planned Improvements

### 1. Query Normalization (HIGH IMPACT)

**Problem:**
Currently, these queries are treated as different and get separate cache entries:
```sql
SELECT * FROM cpu WHERE time > '2024-01-01' AND value > 100
SELECT * FROM cpu WHERE time > '2024-01-02' AND value > 200
```

This drastically reduces cache hit rates for time-series queries where the time range constantly shifts (e.g., "last 1 hour" queries).

**Solution:**
Normalize queries by replacing literals with placeholders to create query templates:
```sql
SELECT * FROM cpu WHERE time > ? AND value > ?
```

**Implementation Plan:**

1. Add `_normalize_query(sql: str) -> Tuple[str, List[Any]]` method
   - Replace string literals (`'...'`) with `?`
   - Replace numeric literals (`123`, `45.67`) with `?`
   - Extract and store parameters
   - Normalize whitespace

2. Update `_make_key()` to use normalized SQL template
   - Key based on: `normalized_sql:limit`
   - Parameters stored but not used in key

3. **Important:** Only normalize for cache key generation
   - Original SQL is still executed (no prepare() issues)
   - Normalization is purely for cache matching

**Regex Patterns:**
```python
# String literals (single quotes)
r"'([^']*)'"  -> "?"

# Numeric literals (integers and floats)
r'(?<![a-zA-Z0-9_])(\d+\.?\d*)'  -> "?"
# Note: Negative lookbehind prevents matching "col123"

# Whitespace normalization
' '.join(sql.split())
```

**Expected Impact:**
- **30-50% improvement in cache hit rate** for time-series dashboards
- Especially beneficial for Grafana dashboards with auto-refresh
- Queries with `WHERE time > $__timeFrom` will share cache entries

**Edge Cases to Handle:**
- Column names with numbers: `cpu_percent_1`, `temp_sensor_2` (don't normalize)
- SQL keywords in strings: `SELECT 'DROP TABLE'` (preserve in strings)
- Special identifiers: `$__timeFrom`, `$__interval` (Grafana variables)

### 2. Compression (HIGH IMPACT)

**Problem:**
Time-series query results are highly compressible (repetitive timestamps, numeric patterns) but stored uncompressed in cache. A 10MB result limit means we can only cache ~10 queries if all results are large.

**Solution:**
Compress cached results using fast compression algorithms optimized for JSON/structured data.

**Implementation Plan:**

1. Add compression backend with pluggable algorithms:
   ```python
   from typing import Protocol

   class Compressor(Protocol):
       def compress(self, data: bytes) -> bytes: ...
       def decompress(self, data: bytes) -> bytes: ...
   ```

2. Implement compression options:
   - **LZ4** (default) - Fast compression/decompression, 2-3x compression
   - **Zstd** - Better compression ratio, 3-10x compression, slightly slower
   - **None** - Disable compression (backward compat)

3. Update cache storage:
   ```python
   # Store compressed results
   self.cache[key] = (compressed_result, cached_at, original_size, compressed_size)
   ```

4. Add compression stats:
   - `compression_ratio`: Average compression achieved
   - `compression_time_ms`: Time spent compressing/decompressing
   - `memory_saved_mb`: Total memory saved by compression

**Configuration:**
```python
QUERY_CACHE_COMPRESSION = "lz4"  # Options: "lz4", "zstd", "none"
QUERY_CACHE_COMPRESSION_LEVEL = 1  # 1-9 for zstd (higher = better compression, slower)
```

**Expected Impact:**
- **3-10x more results cached** with same memory footprint
- **Minimal performance overhead**: LZ4 compression is ~500MB/s
- For a 1MB result: ~2ms compression time (negligible vs query execution)

**Compression Ratio Examples** (time-series data):
```
Raw JSON: 5.2 MB
LZ4:      1.8 MB (2.9x compression)
Zstd-1:   1.1 MB (4.7x compression)
Zstd-9:   0.7 MB (7.4x compression, slower)
```

**Trade-offs:**
- Compression CPU cost: ~2-5ms per result
- Decompression CPU cost: ~1-2ms per cache hit
- Net benefit: Compress once, decompress many times (high ROI)

**Dependencies:**
```python
# Add to requirements.txt
lz4>=4.3.2
zstandard>=0.22.0  # Optional, for zstd support
```

### 3. Hit Count Tracking (MEDIUM IMPACT)

**Problem:**
Current LRU eviction only considers recency, not frequency. A query used 100x in 5 minutes is evicted if not used in the last minute, while a one-off query used once stays cached.

**Solution:**
Implement LFU (Least Frequently Used) + LRU hybrid eviction strategy.

**Implementation Plan:**

1. Update cache entry structure:
   ```python
   @dataclass
   class CacheEntry:
       result: Dict[str, Any]
       cached_at: datetime
       last_used: datetime
       hit_count: int = 0
       compressed: bool = False
       compression_ratio: float = 1.0
   ```

2. Track per-entry metrics:
   - `hit_count`: Number of times this entry was accessed
   - `last_used`: Most recent access timestamp
   - `access_frequency`: Hits per minute since creation

3. Implement weighted eviction scoring:
   ```python
   def _eviction_score(self, entry: CacheEntry) -> float:
       """
       Lower score = more likely to evict

       Score considers:
       - Recency: More recent = higher score
       - Frequency: More hits = higher score
       - Age: Older entries with few hits = lower score
       """
       age_minutes = (datetime.now() - entry.cached_at).total_seconds() / 60
       recency_minutes = (datetime.now() - entry.last_used).total_seconds() / 60

       # Frequency: hits per minute since creation
       frequency = entry.hit_count / max(age_minutes, 1)

       # Recency penalty: exponential decay
       recency_score = math.exp(-recency_minutes / 10)  # Decay over 10 minutes

       # Combined score
       return frequency * 0.6 + recency_score * 0.4
   ```

4. Evict lowest scoring entry when cache is full

5. Add metrics to stats endpoint:
   - `avg_hits_per_entry`: Average hit count across all entries
   - `most_popular_queries`: Top 10 queries by hit count
   - `least_popular_queries`: Bottom 10 (candidates for eviction)

**Expected Impact:**
- **Better cache utilization**: Popular queries stay cached longer
- **Reduced eviction churn**: Frequently-used queries not evicted prematurely
- **Dashboard performance**: Grafana dashboards (high-frequency queries) benefit most

**Metrics to Track:**
```python
{
  "cache_efficiency": {
    "total_hits": 1523,
    "total_misses": 234,
    "hit_rate_percent": 86.7,
    "avg_hits_per_entry": 15.2,  # NEW
    "eviction_reasons": {         # NEW
      "low_frequency": 12,
      "old_and_unused": 8,
      "size_limit": 3
    }
  },
  "top_queries": [                # NEW
    {
      "sql_template": "SELECT * FROM cpu WHERE time > ? LIMIT ?",
      "hit_count": 247,
      "frequency_per_minute": 4.1,
      "cache_age_seconds": 3600
    }
  ]
}
```

### 4. Additional Improvements (Lower Priority)

#### 4.1 Smarter Size Estimation
Replace rough estimate with actual memory measurement:
```python
import sys
import pickle

def _estimate_size_mb(self, result: Dict[str, Any]) -> float:
    # Use pickle for accurate size estimation
    try:
        pickled = pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)
        return len(pickled) / (1024 * 1024)
    except Exception:
        # Fallback to current estimation
        return self._estimate_size_rough(result)
```

#### 4.2 Pattern-based Invalidation
Implement cache invalidation by table pattern:
```python
# Invalidate all queries involving 'cpu' table
query_cache.invalidate(pattern="cpu")

# Invalidate all queries in a namespace
query_cache.invalidate(pattern="systems.*")
```

**Use cases:**
- After bulk data ingestion: `invalidate(pattern="systems.cpu")`
- After schema changes: `invalidate(pattern="*")`
- After retention policy runs: `invalidate(pattern="{table}")`

#### 4.3 Adaptive TTL
Adjust TTL based on query frequency:
```python
def _calculate_ttl(self, entry: CacheEntry) -> int:
    """
    High-frequency queries: longer TTL (up to 5 minutes)
    Low-frequency queries: shorter TTL (down to 30 seconds)
    """
    base_ttl = self.ttl_seconds
    frequency = entry.hit_count / max(age_minutes, 1)

    if frequency > 10:  # >10 hits/minute
        return min(base_ttl * 5, 300)  # Up to 5 minutes
    elif frequency < 1:  # <1 hit/minute
        return max(base_ttl // 2, 30)  # Down to 30 seconds
    else:
        return base_ttl
```

#### 4.4 Memory Pressure Handling
```python
import psutil

def _check_memory_pressure(self) -> bool:
    """Check if system memory is under pressure"""
    memory = psutil.virtual_memory()
    return memory.percent > 80  # >80% memory usage

def set(self, sql: str, limit: int, result: Dict[str, Any]) -> bool:
    # Don't cache if system memory is critical
    if self._check_memory_pressure():
        logger.warning("Skipping cache due to memory pressure")
        return False
    # ... rest of set logic
```

## Implementation Phases

### Phase 1: Query Normalization (Week 1)
- [ ] Implement `_normalize_query()` method
- [ ] Update `_make_key()` to use normalized SQL
- [ ] Add unit tests for normalization edge cases
- [ ] Benchmark cache hit rate improvement
- [ ] Document normalization behavior

**Success Criteria:**
- 30%+ improvement in cache hit rate for time-series queries
- No performance regression (normalization <1ms overhead)
- All existing tests pass

### Phase 2: Compression (Week 2)
- [ ] Add `Compressor` protocol and LZ4 implementation
- [ ] Update cache entry structure to store compressed data
- [ ] Add compression configuration options
- [ ] Update stats endpoint with compression metrics
- [ ] Benchmark memory savings and performance impact

**Success Criteria:**
- 3-10x more results cached with same memory
- <5ms compression overhead per result
- <2ms decompression overhead per cache hit

### Phase 3: Hit Count Tracking (Week 3)
- [ ] Add `CacheEntry` dataclass with hit tracking
- [ ] Implement weighted eviction scoring
- [ ] Update stats endpoint with frequency metrics
- [ ] Add "most popular queries" view
- [ ] Benchmark eviction efficiency improvement

**Success Criteria:**
- Popular queries stay cached 2-3x longer
- Lower eviction rate for high-frequency queries
- Enhanced metrics provide actionable insights

### Phase 4: Additional Improvements (Future)
- [ ] Smarter size estimation with sys.getsizeof()
- [ ] Pattern-based cache invalidation
- [ ] Adaptive TTL based on query frequency
- [ ] Memory pressure handling with psutil

## Testing Strategy

### Unit Tests
```python
# test_query_cache_normalization.py
def test_normalize_string_literals():
    sql = "SELECT * FROM cpu WHERE host = 'server1'"
    normalized, params = cache._normalize_query(sql)
    assert normalized == "SELECT * FROM cpu WHERE host = ?"
    assert params == ['server1']

def test_normalize_numeric_literals():
    sql = "SELECT * FROM cpu WHERE value > 100 AND temp < 75.5"
    normalized, params = cache._normalize_query(sql)
    assert "value > ?" in normalized
    assert params == [100, 75.5]

def test_normalize_preserves_column_names():
    sql = "SELECT cpu_1, temp_2 FROM sensors"
    normalized, params = cache._normalize_query(sql)
    assert "cpu_1" in normalized  # Not replaced
    assert "temp_2" in normalized  # Not replaced
```

### Integration Tests
```python
def test_cache_hit_with_different_literals():
    """Verify normalized queries share cache entries"""
    cache.set("SELECT * FROM cpu WHERE time > '2024-01-01'", 1000, result1)
    hit, _ = cache.get("SELECT * FROM cpu WHERE time > '2024-01-02'", 1000)
    assert hit is not None  # Cache hit despite different date!

def test_compression_roundtrip():
    """Verify compression doesn't corrupt data"""
    original_result = execute_query("SELECT * FROM cpu LIMIT 10000")
    cache.set(sql, 10000, original_result)
    cached_result, _ = cache.get(sql, 10000)
    assert cached_result == original_result
```

### Performance Benchmarks
```python
def benchmark_normalization_overhead():
    """Measure normalization time"""
    sql = "SELECT * FROM cpu WHERE time > '2024-01-01' LIMIT 1000"

    times = []
    for _ in range(1000):
        start = time.perf_counter()
        cache._normalize_query(sql)
        times.append(time.perf_counter() - start)

    avg_time_ms = sum(times) / len(times) * 1000
    assert avg_time_ms < 1.0  # Must be <1ms
    print(f"Normalization: {avg_time_ms:.3f}ms avg")

def benchmark_compression():
    """Measure compression time and ratio"""
    result = execute_query("SELECT * FROM cpu LIMIT 10000")

    start = time.perf_counter()
    compressed = compress(pickle.dumps(result))
    compress_time_ms = (time.perf_counter() - start) * 1000

    start = time.perf_counter()
    decompressed = pickle.loads(decompress(compressed))
    decompress_time_ms = (time.perf_counter() - start) * 1000

    ratio = len(pickle.dumps(result)) / len(compressed)

    print(f"Compression: {compress_time_ms:.1f}ms, ratio: {ratio:.1f}x")
    print(f"Decompression: {decompress_time_ms:.1f}ms")

    assert compress_time_ms < 10  # <10ms acceptable
    assert decompress_time_ms < 5  # <5ms acceptable
    assert ratio > 2  # At least 2x compression
```

## Monitoring & Observability

Add new metrics to `/api/v1/cache/stats` endpoint:

```json
{
  "enabled": true,
  "ttl_seconds": 60,
  "max_size": 100,
  "current_size": 87,
  "utilization_percent": 87.0,

  "normalization": {
    "enabled": true,
    "unique_templates": 23,
    "avg_queries_per_template": 3.8
  },

  "compression": {
    "enabled": true,
    "algorithm": "lz4",
    "avg_compression_ratio": 4.2,
    "memory_saved_mb": 234.5,
    "total_compress_time_ms": 1234,
    "total_decompress_time_ms": 456
  },

  "metrics": {
    "total_requests": 5234,
    "hits": 4521,
    "misses": 713,
    "hit_rate_percent": 86.4,
    "evictions": 45,
    "expirations": 123,
    "avg_hits_per_entry": 15.2
  },

  "top_queries": [
    {
      "sql_template": "SELECT * FROM systems.cpu WHERE time > ? AND time < ? LIMIT ?",
      "hit_count": 247,
      "frequency_per_minute": 4.1,
      "avg_execution_time_ms": 42.3,
      "cache_age_seconds": 3542,
      "last_hit": "2025-11-01T14:23:45Z"
    }
  ]
}
```

## Configuration Reference

```bash
# Enable/disable query cache
QUERY_CACHE_ENABLED=true

# Cache settings
QUERY_CACHE_TTL=60                    # Cache TTL in seconds
QUERY_CACHE_MAX_SIZE=100              # Max number of cached queries
QUERY_CACHE_MAX_RESULT_MB=10          # Max size per result

# Normalization
QUERY_CACHE_NORMALIZATION=true        # Enable query normalization

# Compression
QUERY_CACHE_COMPRESSION=lz4           # Options: lz4, zstd, none
QUERY_CACHE_COMPRESSION_LEVEL=1       # 1-9 for zstd (higher = better, slower)

# Eviction strategy
QUERY_CACHE_EVICTION_STRATEGY=hybrid  # Options: lru, lfu, hybrid
QUERY_CACHE_FREQUENCY_WEIGHT=0.6      # 0.0-1.0, weight for frequency in hybrid mode
```

## Performance Goals

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Cache hit rate (dashboards) | 45% | 75%+ | +67% |
| Memory efficiency | 1x | 4-10x | +300-900% |
| Avg queries cached | 100 | 400-1000 | +300-900% |
| Normalization overhead | N/A | <1ms | Negligible |
| Compression overhead | N/A | <5ms | Negligible |
| Eviction efficiency | Baseline | +50% | Better retention |

## Rollout Plan

1. **Development:** Implement in feature branches with extensive testing
2. **Staging:** Deploy to staging environment with production-like load
3. **Canary:** Enable for 10% of production traffic
4. **Gradual rollout:** Increase to 50%, then 100% over 1 week
5. **Monitor:** Track cache hit rates, memory usage, query performance

## Rollback Plan

All improvements should be feature-flagged and independently configurable:

```bash
# Disable individual features if issues arise
QUERY_CACHE_NORMALIZATION=false
QUERY_CACHE_COMPRESSION=none
QUERY_CACHE_EVICTION_STRATEGY=lru  # Revert to simple LRU
```

## Related Issues

- DuckDB prepare() bug: https://github.com/duckdb/duckdb/issues/17237
  - Query plan caching deferred until this is fixed
  - Focus on result caching improvements in the meantime

## References

- Current implementation: `api/query_cache.py`
- Cache usage: `api/main.py` (lines 1084-1138)
- API endpoints: `/api/v1/cache/*`
- LZ4 compression: https://github.com/python-lz4/python-lz4
- Zstandard: https://github.com/indygreg/python-zstandard

## Questions / Discussion

1. **Normalization edge cases:** How to handle Grafana template variables (`$__timeFrom`, `$__interval`)?
   - Recommendation: Preserve these as-is, they're already parameterized

2. **Compression algorithm:** LZ4 (fast) vs Zstd (better ratio)?
   - Recommendation: Default to LZ4, make Zstd optional for memory-constrained deployments

3. **Eviction strategy weights:** What's the optimal frequency vs recency balance?
   - Recommendation: 60% frequency / 40% recency, make configurable

4. **Memory limits:** Should we add a global memory limit for the cache?
   - Recommendation: Yes, add `QUERY_CACHE_MAX_MEMORY_MB` option (default: 1GB)

---

**Last Updated:** 2025-11-01
**Author:** Arc Team
**Status:** Ready for Implementation
