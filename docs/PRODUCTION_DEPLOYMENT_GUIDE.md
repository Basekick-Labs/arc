# Arc Production Deployment Guide

## Overview

This guide helps you configure Arc for production deployments with optimal performance and resource utilization. We'll cover deployment sizing, configuration patterns, and monitoring best practices.

---

## Quick Reference

| Deployment Size | Users | Workers | Memory Limit | Expected Concurrent Queries |
|----------------|-------|---------|--------------|----------------------------|
| Small | 5-10 | 4 | 4GB | 5-10 |
| Medium | 20-50 | 8 | 2.8GB | 10-20 |
| Large | 50-100+ | 16 | 2.8GB | 20-40 |

---

## Deployment Patterns

### Small Team (5-10 users)

**Typical Use Case**: Single team dashboard, moderate query frequency

```toml
[server]
workers = 4

[duckdb]
pool_size = 1
threads = 1
memory_limit = "4GB"
```

**System Requirements**:
- 16-32GB RAM
- 4-8 CPU cores
- NVMe SSD recommended for data storage

**Expected Performance**:
- Handles 5-10 concurrent queries
- Sub-second query response for typical dashboards
- Suitable for teams up to 10 active users

---

### Medium Production (20-50 users)

**Typical Use Case**: Multiple dashboards, varied query patterns, auto-refresh

```toml
[server]
workers = 8

[duckdb]
pool_size = 1
threads = 1
memory_limit = "2.8GB"
```

**System Requirements**:
- 32GB RAM minimum
- 8-16 CPU cores
- NVMe SSD required

**Expected Performance**:
- Handles 10-20 concurrent queries efficiently
- Supports dashboard auto-refresh patterns
- Suitable for 20-50 active users

---

### Large Enterprise (100+ users)

**Typical Use Case**: Organization-wide analytics, many concurrent dashboards

```toml
[server]
workers = 16

[duckdb]
pool_size = 1
threads = 1
memory_limit = "2.8GB"
```

**System Requirements**:
- 64GB RAM minimum
- 16-32 CPU cores
- NVMe SSD array

**Expected Performance**:
- Handles 20-40+ concurrent queries
- Supports hundreds of dashboard viewers
- Enterprise-scale query workloads

---

## Configuration Formula

Calculate optimal `memory_limit` for your deployment:

```
memory_limit = (System RAM × 0.7) / workers
```

**Examples**:

32GB RAM, 8 workers:
```
(32GB × 0.7) / 8 = 2.8GB per worker
```

64GB RAM, 16 workers:
```
(64GB × 0.7) / 16 = 2.8GB per worker
```

128GB RAM, 16 workers:
```
(128GB × 0.7) / 16 = 5.6GB per worker
```

The 0.7 factor ensures 30% headroom for OS, caching, and safety margin.

---

## Dashboard Auto-Refresh Patterns

### Avoiding Synchronized Load

Dashboard auto-refresh can create query bursts if not managed properly.

**Problem**: All dashboards refreshing at round intervals (every 60s at :00)

**Solution 1: Stagger refresh intervals**

Use slightly different intervals per dashboard:
```
Dashboard A: 59 seconds
Dashboard B: 61 seconds
Dashboard C: 67 seconds (prime number)
```

**Solution 2: Add random jitter**

```javascript
const baseInterval = 60000; // 60s
const jitter = (Math.random() - 0.5) * 10000; // ±5s
const actualInterval = baseInterval + jitter;
```

**Solution 3: Enable query result caching**

```toml
[query_cache]
enabled = true
ttl_seconds = 60
max_size = 100
```

With caching, multiple dashboards requesting the same query within 60s will receive cached results instantly.

---

## Memory Management

### Understanding Memory Usage

**Total memory ceiling**:
```
workers × pool_size × memory_limit = Total DuckDB memory
```

For production (8 workers × 1 pool × 2.8GB):
```
8 × 1 × 2.8GB = 22.4GB DuckDB memory
```

Plus ~8-10GB for OS, Python, buffers = **~32GB total**

### Monitoring Memory

Check system memory usage:

```bash
# Linux
free -h

# macOS
vm_stat

# Watch Arc workers
ps aux | grep gunicorn
```

**Healthy patterns**:
- Total memory usage: 60-75% of system RAM
- Swap usage: 0 (any swap indicates a problem)
- Worker RSS stable over time

---

## Query Concurrency Patterns

### Typical Concurrency by Workload

**Light Load** (Small team, occasional queries):
- Peak concurrent queries: 3-5
- Average concurrent queries: 1-2
- Configuration: 4 workers sufficient

**Moderate Load** (Active dashboards):
- Peak concurrent queries: 10-15
- Average concurrent queries: 5-8
- Configuration: 8 workers recommended

**Heavy Load** (Many dashboards, auto-refresh):
- Peak concurrent queries: 20-40
- Average concurrent queries: 10-20
- Configuration: 16 workers required

### Calculating Expected Concurrency

```
Concurrent Queries ≈ (Queries per Minute / 60) × Average Query Time (seconds)
```

**Example**:
- 100 queries/minute
- Average query time: 2 seconds
- Expected concurrency: (100 / 60) × 2 ≈ **3.3 concurrent queries**

---

## Performance Optimization

### Query Result Caching

Enable caching for frequently-accessed data:

```toml
[query_cache]
enabled = true
ttl_seconds = 60           # Cache for 1 minute
max_size = 100             # Cache up to 100 queries
max_result_mb = 10         # Don't cache results > 10MB
```

**Benefits**:
- Reduces load on DuckDB
- Sub-millisecond response for cached queries
- Especially valuable for auto-refreshing dashboards

### Partition Pruning

Arc automatically optimizes queries with time filters:

```sql
-- Automatically optimized
SELECT * FROM cpu WHERE time >= '2025-11-01' AND time < '2025-11-02'
```

No configuration needed—Arc's partition pruner automatically generates targeted file paths.

### Compaction

Keep compaction enabled for optimal query performance:

```toml
[compaction]
enabled = true
schedule = "*/10 * * * *"  # Every 10 minutes
min_files = 50
target_file_size_mb = 512
```

Compaction reduces file count by 100x, improving query speed by 10-50x.

---

## Monitoring & Alerts

### Key Metrics to Track

1. **Concurrent Query Count**
   - Alert threshold: > workers × 2
   - Action: Increase workers or investigate slow queries

2. **Memory Usage**
   - Alert threshold: > 80% of system RAM
   - Action: Reduce memory_limit or add RAM

3. **Query Latency**
   - p95 latency > 5 seconds
   - Action: Optimize queries or add query caching

4. **Compaction Lag**
   - Uncompacted files > 1000 per measurement
   - Action: Increase compaction frequency

### Health Checks

```bash
# System health
curl http://localhost:8000/health

# DuckDB connection pool stats
curl http://localhost:8000/api/v1/stats/connections
```

---

## Scaling Strategies

### Vertical Scaling (Single Node)

**When to use**: Most deployments (up to 100+ users)

Increase resources on single node:
- 32GB → 64GB RAM
- 8 → 16 workers
- Add NVMe SSD

**Advantages**:
- Simpler operations
- Lower latency
- No distributed complexity

### Horizontal Scaling (Multiple Nodes)

**When to use**: 200+ concurrent users, geographic distribution

Add load balancer + multiple Arc nodes:
```
[Load Balancer]
     ├── Arc Node 1 (8 workers)
     ├── Arc Node 2 (8 workers)
     └── Arc Node 3 (8 workers)
          ↓
    [Shared Storage: S3/MinIO]
```

**Advantages**:
- Higher total throughput
- Geographic proximity
- Fault tolerance

---

## Common Patterns

### Pattern 1: High-Frequency Ingestion + Queries

```toml
[server]
workers = 12              # More workers for mixed workload

[ingestion]
buffer_size = 50000       # Fast flushes
buffer_age_seconds = 5

[duckdb]
memory_limit = "2GB"      # Conservative per-worker limit
threads = 1
```

### Pattern 2: Batch Analytics (Low Concurrency)

```toml
[server]
workers = 4               # Fewer workers

[duckdb]
memory_limit = "8GB"      # Higher per-worker limit
threads = 1               # Still single-threaded
```

### Pattern 3: Many Small Queries (Dashboards)

```toml
[server]
workers = 16              # Many workers

[query_cache]
enabled = true            # Essential for dashboard pattern
ttl_seconds = 30

[duckdb]
memory_limit = "2GB"      # Conservative limit
threads = 1
```

---

## Deployment Checklist

**Pre-Production**:
- [ ] Calculate memory_limit using formula
- [ ] Set workers based on expected concurrency
- [ ] Enable query result caching
- [ ] Configure compaction schedule
- [ ] Set up monitoring and alerts
- [ ] Test with realistic query patterns

**Post-Deployment**:
- [ ] Monitor memory usage first 24 hours
- [ ] Track concurrent query counts
- [ ] Measure query latency p95/p99
- [ ] Verify compaction running successfully
- [ ] Check cache hit rates

---

## Troubleshooting

### High Memory Usage

**Symptom**: Memory usage > 80%

**Solutions**:
1. Reduce `memory_limit` per worker
2. Reduce `workers` count
3. Add more system RAM
4. Enable query result caching to reduce load

### Slow Queries

**Symptom**: p95 latency > 5 seconds

**Solutions**:
1. Check compaction is running (file count)
2. Add indexes on time column (automatic in Arc)
3. Enable query result caching
4. Optimize query patterns (use time filters)

### Connection Timeouts

**Symptom**: Queries timeout waiting for connection

**Solutions**:
1. Increase `workers` count
2. Increase `max_queue_size`
3. Reduce `memory_limit` (allows more workers with same RAM)
4. Add query result caching

---

## Summary

**Key Principles**:
1. Use formula: `memory_limit = (RAM × 0.7) / workers`
2. Always set `threads = 1` in multi-worker deployments
3. Enable query result caching for dashboards
4. Monitor memory usage and concurrent queries
5. Start conservative, scale based on data

**Most Common Configuration** (32GB RAM, 8 workers):
```toml
[server]
workers = 8

[duckdb]
pool_size = 1
threads = 1
memory_limit = "2.8GB"

[query_cache]
enabled = true
ttl_seconds = 60
```

This handles 20-50 users with auto-refreshing dashboards comfortably.
