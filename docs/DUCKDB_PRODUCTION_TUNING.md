# DuckDB Production Tuning Guide

Arc uses DuckDB as its query engine. This guide explains how to tune DuckDB for optimal production performance.

## Overview

Arc's DuckDB engine is now production-optimized with configurable tuning parameters. These settings balance:

- **Query Performance** - Maximize multi-core parallelism and caching
- **Memory Safety** - Prevent OOM kills on large analytical queries
- **Concurrency** - Handle multiple concurrent queries efficiently
- **Disk Spillage** - Gracefully handle queries that exceed available RAM

## Quick Start

### Recommended Settings (M3 Max 14-core, 64GB RAM)

```toml
# arc.conf
[duckdb]
pool_size = 5                           # 5 connections per worker
max_queue_size = 100                    # Queue up to 100 overflow queries
memory_limit = "8GB"                    # 8GB per connection (40GB total with 5 conns)
threads = 14                            # Use all 14 CPU cores
temp_directory = "./data/duckdb_tmp"    # Fast NVMe storage for spills
enable_object_cache = true              # Cache Parquet metadata (MAJOR BOOST!)
```

### Settings by Environment

#### Single-User / Dashboard (Low Concurrency)

```toml
[duckdb]
pool_size = 5
memory_limit = "8GB"    # Higher memory per query
threads = 14            # Full CPU utilization per query
```

**Best for:** Grafana dashboards, single analyst, BI tools

#### Multi-User / API (Medium Concurrency)

```toml
[duckdb]
pool_size = 10
memory_limit = "4GB"    # Lower memory per query
threads = 3             # threads = CPU_cores / pool_size
```

**Best for:** Multiple concurrent users, API services

#### High Concurrency (Many Simultaneous Queries)

```toml
[duckdb]
pool_size = 20
memory_limit = "2GB"    # Tight memory limit
threads = 1             # Minimal threads, rely on pool parallelism
```

**Best for:** High-traffic APIs, many concurrent dashboards

## Configuration Parameters

### `pool_size`

**What it does:** Number of DuckDB connections per worker process.

**Impact:**
- Higher = More concurrent queries without queueing
- Lower = More memory per query, better single-query performance

**Calculation:**
```
pool_size = max_concurrent_queries / workers
```

**Examples:**
- 8 workers, expect 40 concurrent queries → `pool_size = 5`
- 4 workers, expect 40 concurrent queries → `pool_size = 10`

---

### `memory_limit`

**What it does:** Maximum RAM each DuckDB connection can use.

**Impact:**
- Higher = Can handle larger datasets, fewer disk spills
- Lower = More connections fit in RAM, prevents OOM

**Calculation:**
```
memory_limit = (Total_RAM * 0.8) / (workers * pool_size)
```

**Examples:**
- 64GB RAM, 8 workers, pool_size=5 → `(64 * 0.8) / (8 * 5) = 1.28GB`
- 128GB RAM, 8 workers, pool_size=5 → `(128 * 0.8) / (8 * 5) = 2.56GB`

**Warning:** Setting too high can cause OOM kills. Leave 20% headroom for OS and other processes.

---

### `threads`

**What it does:** Number of CPU threads each DuckDB connection uses.

**Impact:**
- Higher = Faster single-query performance (parallel execution)
- Lower = More queries run concurrently without contention

**Calculation:**
```
# Low concurrency (1-10 queries)
threads = CPU_cores

# Medium concurrency (10-50 queries)
threads = CPU_cores / pool_size

# High concurrency (50+ queries)
threads = 1-2
```

**Examples:**
- 14 CPU cores, low concurrency → `threads = 14`
- 14 CPU cores, pool_size=5 → `threads = 3` (14/5 rounded)
- High concurrency → `threads = 1`

---

### `temp_directory`

**What it does:** Directory for DuckDB to spill data when queries exceed `memory_limit`.

**Impact:**
- Fast storage (NVMe SSD) = Minimal performance penalty on large queries
- Slow storage (HDD) = Severe slowdowns when memory exceeded

**Recommendations:**
- Use NVMe SSD if available
- Ensure sufficient disk space (10-50GB recommended)
- Monitor disk usage with large analytical queries

---

### `enable_object_cache`

**What it does:** Caches Parquet file metadata (schema, statistics, row groups).

**Impact:**
- Enabled = **Significantly** faster repeated queries on same files
- Disabled = Every query re-reads Parquet metadata

**Recommendation:** Always enable (`true`) in production.

**Performance Impact:**
- First query: No difference
- Repeated queries: 2-10x faster (depends on file count)

---

## Environment Variable Overrides

All configuration can be overridden with environment variables:

```bash
# Connection pool
export DUCKDB_POOL_SIZE=10
export DUCKDB_MAX_QUEUE_SIZE=200

# Performance tuning
export DUCKDB_MEMORY_LIMIT="4GB"
export DUCKDB_THREADS=7
export DUCKDB_TEMP_DIRECTORY="/mnt/nvme/duckdb_tmp"

# Feature flags
export DUCKDB_ENABLE_OBJECT_CACHE=true
```

## Monitoring & Troubleshooting

### Check Connection Pool Metrics

```bash
# Get pool health
curl http://localhost:8000/health/duckdb
```

**Response:**
```json
{
  "pool_size": 5,
  "active_connections": 3,
  "idle_connections": 2,
  "queue_depth": 0,
  "total_queries_executed": 12543,
  "avg_execution_time_ms": 245.3
}
```

### Common Issues

#### 1. Queries Timing Out

**Symptoms:** `Query timeout` errors, high queue depth

**Solutions:**
- Increase `pool_size` (more concurrent queries)
- Increase `memory_limit` (reduce disk spills)
- Increase `threads` (faster query execution)

#### 2. Out of Memory (OOM) Kills

**Symptoms:** Worker processes crash, Linux OOM killer logs

**Solutions:**
- **Reduce `memory_limit`** (most important)
- Reduce `pool_size` (fewer connections)
- Add more RAM
- Enable disk spillage (ensure `temp_directory` has space)

#### 3. Slow Query Performance

**Symptoms:** Queries slower than expected

**Solutions:**
- Increase `threads` (more parallelism)
- Enable `enable_object_cache` (cache metadata)
- Check compaction (many small files = slow queries)
- Increase `memory_limit` (reduce disk I/O)

#### 4. High Disk I/O

**Symptoms:** Queries thrashing disk, slow performance

**Solutions:**
- Increase `memory_limit` (fewer spills to disk)
- Use faster storage for `temp_directory` (NVMe SSD)
- Optimize queries (add WHERE clauses, LIMIT results)
- Enable compaction (merge small Parquet files)

## Performance Benchmarks

### Single-Query Performance (No Concurrency)

**Configuration:**
```toml
pool_size = 1
memory_limit = "16GB"
threads = 14
```

**Query:** `SELECT * FROM cpu WHERE timestamp > NOW() - INTERVAL 1 hour`

| File Count | No Tuning | With Tuning | Speedup |
|-----------|-----------|-------------|---------|
| 10 files  | 1.2s      | 0.8s        | 1.5x    |
| 100 files | 12.3s     | 3.2s        | 3.8x    |
| 1000 files| 145s      | 28s         | 5.2x    |

**Key Optimization:** `enable_object_cache=true` provides massive speedup with many files.

---

### Concurrent Query Performance

**Configuration:**
```toml
pool_size = 10
memory_limit = "4GB"
threads = 3
```

**Workload:** 50 concurrent queries, mixed complexity

| Metric | No Tuning | With Tuning | Improvement |
|--------|-----------|-------------|-------------|
| Avg Query Time | 8.3s | 2.1s | 3.9x faster |
| p95 Query Time | 25.7s | 6.4s | 4.0x faster |
| Queries/sec | 6.0 | 23.8 | 3.9x higher |
| Queue Depth (max) | 42 | 8 | 5.2x lower |

**Key Optimizations:**
- Higher `pool_size` reduces queueing
- Lower `threads` allows more concurrent queries
- `memory_limit` prevents OOM with many connections

---

## Production Checklist

Before deploying to production, verify:

- [ ] `memory_limit` calculated based on available RAM
- [ ] `threads` appropriate for concurrency level
- [ ] `temp_directory` on fast storage (NVMe SSD)
- [ ] `temp_directory` has sufficient disk space (10-50GB)
- [ ] `enable_object_cache` enabled
- [ ] `prefer_range_joins` enabled (for time-series workloads)
- [ ] Monitor `/health/duckdb` endpoint for pool metrics
- [ ] Test under realistic query load
- [ ] Verify no OOM kills in system logs

---

## Advanced Tuning

### Dynamic Thread Allocation (Future)

Arc could dynamically adjust `threads` based on queue depth:

- Queue empty → Use all CPU cores (fast single queries)
- Queue full → Reduce threads (better concurrency)

**Not yet implemented.**

### Per-Query Memory Limits

DuckDB doesn't support per-query memory limits yet. Workaround:

```sql
-- Add LIMIT to prevent huge result sets
SELECT * FROM cpu WHERE ... LIMIT 1000000
```

### Query Priorities

Arc's connection pool supports query priorities:

```python
# High priority (bypass queue)
await engine.execute_query(sql, priority=QueryPriority.HIGH)

# Normal priority
await engine.execute_query(sql, priority=QueryPriority.NORMAL)

# Low priority (background jobs)
await engine.execute_query(sql, priority=QueryPriority.LOW)
```

---

## Summary

**Key Takeaways:**

1. **Memory safety first** - Calculate `memory_limit` to prevent OOM kills
2. **Tune for concurrency** - Higher concurrency = lower `threads`, higher `pool_size`
3. **Enable caching** - `enable_object_cache=true` is a massive win for repeated queries
4. **Fast temp storage** - Use NVMe SSD for `temp_directory`
5. **Monitor metrics** - Watch `/health/duckdb` for pool saturation

**Recommended Starting Point (64GB RAM, 14 cores):**
```toml
[duckdb]
pool_size = 5
memory_limit = "8GB"
threads = 14
temp_directory = "./data/duckdb_tmp"
enable_object_cache = true
```

Adjust based on your workload and monitoring data.
