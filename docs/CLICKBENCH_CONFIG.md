# ClickBench Configuration Guide

## Overview

ClickBench is a sequential, single-user benchmark designed to measure single-query performance. The optimal DuckDB configuration for ClickBench is **different** from production multi-user deployments.

## Key Difference

**ClickBench** = Optimize for **single-query speed** (one query at a time)
**Production** = Optimize for **concurrent stability** (many queries simultaneously)

---

## ClickBench Optimal Configuration

For maximum ClickBench performance, use **single-worker mode** with **full CPU parallelism**:

### arc.conf
```toml
[server]
workers = 1              # Single worker (ClickBench is sequential)

[duckdb]
pool_size = 1            # Only need one connection
threads = 14             # Use all CPU cores (adjust for your system)
memory_limit = "16GB"    # Give queries lots of memory
```

### Or via Environment Variables (.env)
```bash
WORKERS=1
DUCKDB_POOL_SIZE=1
DUCKDB_THREADS=14        # Match your CPU core count
DUCKDB_MEMORY_LIMIT=16GB
```

### Or Command-Line Override
```bash
WORKERS=1 DUCKDB_THREADS=14 DUCKDB_MEMORY_LIMIT=16GB ./start.sh native
```

---

## Why This Configuration Works

### The Math

**ClickBench scenario** (sequential queries):
```
1 worker × 1 connection × 14 threads = 14 threads total ✅
1 query at a time × 16GB = 16GB memory usage ✅
```

**Result**: Full CPU utilization, maximum single-query performance

### vs Production Configuration

**Production scenario** (concurrent queries):
```
8 workers × 1 connection × 14 threads = 112 threads ❌ Thread explosion!
8 concurrent queries × 16GB = 128GB ❌ Memory exhaustion!
```

**Result**: System crash under load

---

## Performance Impact

### Expected ClickBench Performance

| Configuration | Threads | Performance | Use Case |
|--------------|---------|-------------|----------|
| **ClickBench Optimal** | 14 | 100% (baseline) | Benchmarking |
| **Production Safe** | 1 | 20-50% slower | Multi-user production |

### Why the Difference?

**Complex analytical queries** (like ClickBench) benefit from parallelism:
- Scanning large datasets
- Complex aggregations
- Join operations
- Sorting and grouping

**With `threads=14`**: Query parallelizes across all 14 cores → **Fast**
**With `threads=1`**: Query runs single-threaded → **Slower**, but safer for concurrent load

---

## Switching Between Configs

### For Benchmarking
```bash
# Use environment variables to override arc.conf temporarily
WORKERS=1 DUCKDB_THREADS=14 DUCKDB_MEMORY_LIMIT=16GB ./start.sh native
```

### For Production
```bash
# Use arc.conf defaults (or standard .env)
./start.sh native
```

### Quick Toggle Script

Create `benchmark.sh`:
```bash
#!/bin/bash
echo "Starting Arc in ClickBench mode (single-worker, max performance)"
WORKERS=1 \
DUCKDB_THREADS=14 \
DUCKDB_MEMORY_LIMIT=16GB \
DUCKDB_POOL_SIZE=1 \
./start.sh native
```

Create `production.sh`:
```bash
#!/bin/bash
echo "Starting Arc in production mode (multi-worker, safe concurrency)"
# Uses arc.conf defaults
./start.sh native
```

---

## System-Specific Thread Counts

Adjust `DUCKDB_THREADS` to match your CPU:

| System | CPU Cores | DUCKDB_THREADS |
|--------|-----------|----------------|
| M3 Max | 14 cores | 14 |
| M2 Pro | 12 cores | 12 |
| M1 Pro | 10 cores | 10 |
| Intel i9 (12th gen) | 16 cores | 16 |
| AMD Ryzen 9 | 16 cores | 16 |
| Cloud VM (8 vCPU) | 8 cores | 8 |

To auto-detect:
```bash
# macOS
DUCKDB_THREADS=$(sysctl -n hw.ncpu)

# Linux
DUCKDB_THREADS=$(nproc)
```

---

## Memory Limit Recommendations

| System RAM | Single Query | Multiple Concurrent |
|-----------|--------------|---------------------|
| 16GB | 8GB | 1GB per worker |
| 32GB | 16GB | 2-4GB per worker |
| 64GB | 32GB | 4-8GB per worker |
| 128GB+ | 64GB | 8-16GB per worker |

**Formula**:
- **ClickBench**: `memory_limit = System RAM × 0.5` (50% for single query)
- **Production**: `memory_limit = (System RAM × 0.7) / workers`

---

## Verifying Configuration

After starting Arc, check the logs for DuckDB config:

```bash
# Should see this for ClickBench mode:
DuckDB Performance Config: threads=14, memory_limit=16GB, object_cache=True

# Should see this for production mode:
DuckDB Performance Config: threads=1, memory_limit=1GB, object_cache=True
```

---

## Common Mistakes

### ❌ Running ClickBench with Production Config
```bash
# This will work but be 2-5x slower than it could be
WORKERS=8 DUCKDB_THREADS=1 DUCKDB_MEMORY_LIMIT=1GB
# ClickBench query: Single-threaded, only 1GB memory → Slow
```

### ❌ Running Production with ClickBench Config
```bash
# This will CRASH under concurrent load
WORKERS=8 DUCKDB_THREADS=14 DUCKDB_MEMORY_LIMIT=16GB
# 8 concurrent queries: 112 threads, 128GB memory → Crash!
```

### ✅ Correct: Match Config to Use Case
```bash
# ClickBench: Single worker, full parallelism
WORKERS=1 DUCKDB_THREADS=14 DUCKDB_MEMORY_LIMIT=16GB

# Production: Multiple workers, single-threaded
WORKERS=8 DUCKDB_THREADS=1 DUCKDB_MEMORY_LIMIT=2GB
```

---

## FAQ

### Q: Can I use threads=14 in production if I limit concurrent queries?

**A**: Yes, if you **guarantee** max 1-2 concurrent queries:
```bash
WORKERS=2               # Only 2 workers max
DUCKDB_THREADS=7        # Half the cores per worker
DUCKDB_MEMORY_LIMIT=8GB # (32GB × 0.7) / 2
```

But this is risky - unexpected load spikes can still cause crashes.

### Q: Will ClickBench results be valid with threads=1?

**A**: Yes, but they'll be **slower** than optimal. You're measuring "Arc with conservative multi-user config" rather than "Arc at max performance".

For fair ClickBench comparison with other systems, use threads=14.

### Q: Does this affect ingestion performance?

**A**: No. Ingestion is I/O-bound and uses separate Arrow/Parquet writers. This only affects **query** performance.

### Q: Should I publish ClickBench results with threads=1 or threads=14?

**A**: Use **threads=14** and note the configuration:

> "ClickBench run on Arc v25.12.1 (single-worker mode, threads=14, memory_limit=16GB, M3 Max 14-core)"

This is the honest comparison - ClickBench is designed for single-query performance.

---

## Summary

**The Rule**:
- **Benchmarking**: `workers=1, threads=CPU_CORES, memory_limit=HIGH`
- **Production**: `workers=MANY, threads=1, memory_limit=LOW_PER_WORKER`

**Why**:
- ClickBench is sequential → exploit thread-level parallelism
- Production is concurrent → exploit worker-level parallelism

**Never mix** worker parallelism + thread parallelism = system crashes.
