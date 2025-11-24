# DuckDB Multi-Worker Configuration Guide

## Overview

Arc uses DuckDB as its high-performance query engine and supports flexible deployment configurations. This guide helps you optimize DuckDB settings for your specific deployment scenario—whether single-worker development or multi-worker production.

**Key takeaway**: Arc achieves parallelism through multiple worker processes (Gunicorn/Uvicorn), so DuckDB should be configured for single-threaded operation per worker.

---

## Recommended Configurations

### Production Multi-Worker Deployment

Typical production setup with 8 workers:

```toml
[server]
workers = 8              # Handles concurrent requests efficiently

[duckdb]
pool_size = 1            # One connection per worker (optimal)
threads = 1              # Let workers provide parallelism
memory_limit = "2.8GB"   # Formula: (System RAM × 0.7) / workers
```

**Why this works**: 8 workers × 1 thread = efficient process-level parallelism without context switching overhead.

### Development / Single-Worker

Local development or single-user analytics:

```toml
[server]
workers = 1              # Single process

[duckdb]
pool_size = 1
threads = 14             # Use all CPU cores (adjust for your system)
memory_limit = "16GB"    # Generous memory for complex queries
```

**Why this works**: With only one worker, DuckDB can safely use all CPU cores for maximum single-query performance.

---

## Configuration Formula

Use this formula to calculate optimal `memory_limit`:

```
memory_limit = (System RAM × 0.7) / workers
```

**Examples**:

| System RAM | Workers | Calculation | memory_limit |
|------------|---------|-------------|--------------|
| 32GB | 8 | (32 × 0.7) / 8 | **2.8GB** |
| 64GB | 8 | (64 × 0.7) / 8 | **5.6GB** |
| 32GB | 4 | (32 × 0.7) / 4 | **5.6GB** |
| 16GB | 4 | (16 × 0.7) / 4 | **2.8GB** |

The 0.7 factor leaves 30% headroom for OS, other processes, and safety margin.

---

## Understanding Worker vs Thread Parallelism

### Multi-Worker (Production Pattern)

```
8 workers × 1 thread each = 8 parallel execution units
```

**Benefits**:
- Clean process isolation
- No shared state between queries
- Efficient CPU utilization
- Stable memory usage

### Multi-Threaded (Anti-Pattern for Multi-Worker)

```
8 workers × 4 threads each = 32 threads competing for CPU
```

**Issues**:
- Context switching overhead on limited CPU cores
- Shared state complexity
- Memory multiplication (8 workers × 4 threads × memory_limit)

**The Rule**: Choose EITHER worker parallelism OR thread parallelism, not both.

---

## Configuration by Use Case

### Small Team Dashboard (4-10 users)

```toml
[server]
workers = 4

[duckdb]
pool_size = 1
threads = 1
memory_limit = "4GB"     # Assuming 16-32GB RAM
```

**Handles**: ~10 concurrent queries comfortably

### Medium Production (20-50 users)

```toml
[server]
workers = 8

[duckdb]
pool_size = 1
threads = 1
memory_limit = "2.8GB"   # Assuming 32GB RAM
```

**Handles**: ~20 concurrent queries efficiently

### Large Production (100+ users)

```toml
[server]
workers = 16

[duckdb]
pool_size = 1
threads = 1
memory_limit = "2.8GB"   # Assuming 64GB RAM
```

**Handles**: ~40+ concurrent queries with headroom

---

## Common Scenarios

### Benchmarking (ClickBench, etc.)

For single-query performance testing:

```bash
WORKERS=1 DUCKDB_THREADS=14 DUCKDB_MEMORY_LIMIT=16GB ./start.sh native
```

See [CLICKBENCH_CONFIG.md](CLICKBENCH_CONFIG.md) for details.

### Development with Many Test Queries

High worker count for development/testing:

```toml
[server]
workers = 14             # For testing concurrent behavior

[duckdb]
pool_size = 1
threads = 1
memory_limit = "1GB"     # Lower per-worker limit for safety
```

**Note**: High worker counts (20+) are typically only needed for large-scale production or specific testing scenarios.

---

## Monitoring & Validation

### Check Your Configuration

After starting Arc, verify settings in logs:

```
DuckDB Performance Config: threads=1, memory_limit=2.8GB, object_cache=True
```

### Monitor Concurrent Queries

Track active query count to ensure configuration handles your load:

```sql
-- Via Arc's internal metrics (if enabled)
SELECT * FROM system.active_queries;
```

Typical concurrent query counts:
- Small deployment: 1-5 concurrent
- Medium deployment: 5-15 concurrent
- Large deployment: 15-40 concurrent

If you consistently see higher concurrency, consider:
1. Increasing worker count
2. Optimizing slow queries
3. Adding query result caching

---

## Environment Variable Overrides

Override arc.conf settings via environment variables:

```bash
# Temporary override for testing
DUCKDB_THREADS=1 DUCKDB_MEMORY_LIMIT=2GB ./start.sh native

# Or in .env file
DUCKDB_THREADS=1
DUCKDB_MEMORY_LIMIT=2GB
DUCKDB_POOL_SIZE=1
```

Priority: Environment variables > arc.conf

---

## Best Practices

1. **Start conservative**: Use recommended settings, then adjust based on monitoring
2. **Match your workload**: Single-worker for development, multi-worker for production
3. **Monitor memory**: Ensure total usage stays below 70% of system RAM
4. **Test before production**: Validate configuration with realistic query patterns
5. **Document changes**: Note why you adjusted from defaults

---

## Additional Resources

- [REAL_WORLD_DUCKDB_CONFIG.md](REAL_WORLD_DUCKDB_CONFIG.md) - Production deployment patterns
- [CLICKBENCH_CONFIG.md](CLICKBENCH_CONFIG.md) - Benchmark optimization guide
- [arc.conf](../arc.conf) - Main configuration file with detailed comments

---

## Summary

**Production Multi-Worker** (Recommended):
- workers = 8-16
- threads = 1
- memory_limit = (RAM × 0.7) / workers

**Development Single-Worker**:
- workers = 1
- threads = CPU_CORES
- memory_limit = High (8-16GB)

**Benchmarking**:
- workers = 1
- threads = CPU_CORES
- memory_limit = Very high (16-32GB)

The key is matching your configuration to your deployment pattern. Arc's defaults are optimized for typical production multi-worker deployments.
