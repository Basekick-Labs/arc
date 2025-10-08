# Arc Architecture

This document explains Arc's internal architecture, data flow, and design decisions.

## Table of Contents

- [High-Level Overview](#high-level-overview)
- [Data Flow](#data-flow)
- [Buffering System](#buffering-system)
- [Storage Layout](#storage-layout)
- [Schema Management](#schema-management)
- [Query Engine](#query-engine)
- [Durability and Data Loss](#durability-and-data-loss)
- [Performance Tuning](#performance-tuning)

## High-Level Overview

Arc is a time-series data warehouse optimized for high-throughput ingestion and fast analytical queries. It combines:

- **FastAPI** for HTTP API (with Gunicorn/Uvicorn workers)
- **MessagePack binary protocol** for high-performance writes (1.89M records/sec)
- **InfluxDB Line Protocol** for drop-in compatibility with existing workloads
- **In-memory buffering** for batching writes
- **Parquet files** for columnar storage
- **DuckDB** for analytical queries
- **S3-compatible storage** (MinIO, AWS S3, GCS) for scalability

> **Performance Note:** MessagePack is the **preferred ingestion format** for Arc, delivering 7.9× faster throughput than Line Protocol. Line Protocol is provided solely for compatibility with existing InfluxDB/Telegraf deployments.

```
┌─────────────────────────────────────────────────────────────┐
│                        Arc Core                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐      ┌──────────────┐                    │
│  │  MessagePack │      │  Line Proto  │                     │
│  │  (Preferred) │      │ (InfluxDB)   │                     │
│  └──────┬───────┘      └──────┬───────┘                     │
│         │                     │                              │
│         └─────────┬───────────┘                              │
│                   ▼                                          │
│         ┌─────────────────────┐                             │
│         │  In-Memory Buffers  │  ← Per-measurement          │
│         │  (50K records/5s)   │                             │
│         └─────────┬───────────┘                             │
│                   ▼                                          │
│         ┌─────────────────────┐                             │
│         │  Parquet Writer     │  ← Snappy compression       │
│         │  (Direct Arrow)     │                             │
│         └─────────┬───────────┘                             │
│                   ▼                                          │
│         ┌─────────────────────┐                             │
│         │  Storage Backend    │  ← MinIO/S3/GCS             │
│         │  (Hour Partitions)  │                             │
│         └─────────────────────┘                             │
│                                                              │
│         ┌─────────────────────┐                             │
│         │  DuckDB Query Engine│  ← Parallel + pruning       │
│         │  (Connection Pool)  │                             │
│         └─────────────────────┘                             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### Write Path

1. **HTTP Request** arrives (MessagePack preferred, or Line Protocol for InfluxDB compatibility)
2. **Parser** converts to internal format:
   ```python
   {
       "measurement": "cpu",
       "time": datetime(2025, 10, 8, 14, 30, 0),
       "host": "server01",        # tag
       "region": "us-east",       # tag
       "usage_idle": 95.0,        # field
       "usage_user": 3.2          # field
   }
   ```
3. **Buffer** accumulates records in memory (per-measurement)
4. **Flush trigger** fires when either:
   - Buffer size ≥ 50,000 records (default)
   - Buffer age ≥ 5 seconds (default)
5. **Parquet Writer** creates temporary file:
   - Converts to Arrow RecordBatch (zero-copy)
   - Writes to `/tmp/xyz.parquet` with Snappy compression
   - Includes min/max statistics for query pruning
6. **Storage Upload** copies to S3/MinIO:
   - Path: `s3://bucket/cpu/2025/10/08/14/cpu_20251008_143000_50000.parquet`
   - Atomic operation (temp file → object storage)
7. **HTTP Response** returns 204 No Content

**Throughput:**
- **MessagePack (preferred):** 1.89M records/sec
- **Line Protocol (InfluxDB compat):** ~240K records/sec (7.9× slower)

### Read Path

1. **HTTP Query** arrives with SQL:
   ```sql
   SELECT * FROM cpu WHERE host = 'server01' AND time > now() - INTERVAL '1 hour'
   ```
2. **SQL Conversion** transforms table references to S3 paths:
   ```sql
   SELECT * FROM read_parquet('s3://arc/cpu/**/*.parquet', union_by_name=true)
   WHERE host = 'server01' AND time > now() - INTERVAL '1 hour'
   ```
3. **DuckDB Optimization**:
   - **Partition pruning**: Only reads `2025/10/08/14/*.parquet` files
   - **Predicate pushdown**: Filters at Parquet file level
   - **Statistics pruning**: Skips files based on min/max metadata
   - **Parallel scan**: Reads multiple files concurrently
4. **Connection Pool** manages 5 DuckDB connections (default)
5. **HTTP Response** returns JSON results

**Query Speed:** Sub-second for 100M+ rows with proper indexing

## Buffering System

### Buffer Configuration

Arc uses **per-measurement buffers** to isolate flush behavior:

```python
# Default configuration
WRITE_BUFFER_SIZE=50000      # Records per measurement
WRITE_BUFFER_AGE=5           # Seconds before force flush
WRITE_COMPRESSION=snappy     # Parquet compression
```

### Buffer Lifecycle

Each measurement has its own buffer with independent lifecycle:

```
Time →
0s     2s     4s     6s     8s     10s
│      │      │      │      │      │
cpu:   [──────records──────]       │  ← Flushed at 6s (age limit)
       │      │      │      │      │
memory:        [──────50K records] │  ← Flushed at 8s (size limit)
       │      │      │      │      │
disk:          [──────records──────]  ← Flushed at 10s (age limit)
```

### Flush Triggers

**Size-based flush:**
```python
if len(buffer[measurement]) >= 50000:
    await flush_measurement(measurement)
```

**Time-based flush** (background task checks every 5 seconds):
```python
for measurement, start_time in buffer_start_times.items():
    if (now - start_time).total_seconds() >= 5:
        await flush_measurement(measurement)
```

### Error Handling

If a flush fails (network error, S3 unavailable):
1. Records are **re-added to buffer** (retry)
2. If buffer exceeds 2× size limit, **records are dropped** (prevents memory exhaustion)
3. Error is logged with `total_errors` metric

## Storage Layout

### Directory Structure

Arc uses **hour-level time partitioning** for optimal query performance:

```
s3://arc/
├── cpu/
│   ├── 2025/
│   │   ├── 10/
│   │   │   ├── 08/
│   │   │   │   ├── 14/
│   │   │   │   │   ├── cpu_20251008_140530_50000.parquet
│   │   │   │   │   ├── cpu_20251008_141045_50000.parquet
│   │   │   │   │   └── cpu_20251008_142310_50000.parquet
│   │   │   │   └── 15/
│   │   │   │       └── cpu_20251008_150102_50000.parquet
├── memory/
│   └── 2025/10/08/14/*.parquet
├── disk/
│   └── 2025/10/08/14/*.parquet
└── http_logs/
    └── 2025/10/08/14/*.parquet
```

### File Naming Convention

```
{measurement}_{timestamp}_{record_count}.parquet
     ↓            ↓              ↓
    cpu    20251008_140530    50000

- measurement: Table/collection name
- timestamp: First record timestamp (YYYYMMdd_HHMMSS)
- record_count: Number of records in file
```

### Parquet File Features

Each file includes:
- **Snappy compression** (fast encode/decode, ~2-3x compression)
- **Column statistics** (min/max/null_count per column)
- **Dictionary encoding** (for repeated values like tags)
- **Sorted by time** (enables better compression + pruning)

### Storage Backends

Arc supports multiple backends with identical API:

| Backend | Use Case | Configuration |
|---------|----------|---------------|
| **MinIO** | Development, self-hosted | `STORAGE_BACKEND=minio` |
| **AWS S3** | Production, cloud-native | `STORAGE_BACKEND=s3` |
| **S3 Express** | Ultra-low latency (<10ms) | `S3_USE_DIRECTORY_BUCKET=true` |
| **GCS** | Google Cloud | `STORAGE_BACKEND=gcs` |
| **Local** | Testing only | `STORAGE_BACKEND=local` |

## Schema Management

### Automatic Schema Evolution

Arc automatically handles schema changes without downtime:

**Example evolution:**
```python
# Day 1: cpu measurement has 3 fields
{"measurement": "cpu", "time": ..., "usage_idle": 95.0, "usage_user": 3.2}
# Creates: cpu_day1.parquet with columns [time, measurement, usage_idle, usage_user]

# Day 2: Add new field
{"measurement": "cpu", "time": ..., "usage_idle": 95.0, "usage_user": 3.2, "usage_system": 1.8}
# Creates: cpu_day2.parquet with columns [time, measurement, usage_idle, usage_user, usage_system]

# Day 3: Query both days
SELECT * FROM cpu WHERE time >= '2025-10-07'
# DuckDB merges schemas automatically:
# - Day 1 rows have usage_system = NULL
# - Day 2 rows have all fields
```

### Schema Inference

Arrow/Parquet schema is inferred from data types:

| Python Type | Arrow Type | Parquet Type |
|-------------|------------|--------------|
| `datetime` | `timestamp[ms]` | INT64 (TIME_MILLIS) |
| `int` | `int64` | INT64 |
| `float` | `float64` | DOUBLE |
| `str` | `utf8` | BYTE_ARRAY (UTF8) |
| `bool` | `bool_` | BOOLEAN |

### Multi-Measurement Isolation

Each measurement is completely independent:

```python
# Three measurements with different schemas
analytics_events: [time, event_type, user_id, session_id, properties]
http_logs:        [time, method, path, status_code, response_time]
host_metrics:     [time, host, cpu_percent, memory_used, disk_io]

# Stored in separate directories
s3://arc/analytics_events/...
s3://arc/http_logs/...
s3://arc/host_metrics/...

# Queried independently
SELECT * FROM http_logs WHERE status_code >= 500
```

**No cross-measurement overhead** - buffers, flushes, and queries are isolated.

## Query Engine

### DuckDB Connection Pool

Arc uses a connection pool to handle concurrent queries:

```python
DUCKDB_POOL_SIZE=5          # Number of connections
DUCKDB_MAX_QUEUE_SIZE=100   # Max queued queries
```

**Query priorities:**
- `CRITICAL`: Instant execution (user-facing dashboards)
- `HIGH`: Fast lane (API queries)
- `NORMAL`: Default (background analytics)
- `LOW`: Batch jobs (scheduled reports)

### Query Optimization

DuckDB applies multiple optimizations:

1. **Partition Pruning** (time-based):
   ```sql
   -- Only scans 2025/10/08/14/ and 2025/10/08/15/
   WHERE time BETWEEN '2025-10-08 14:00' AND '2025-10-08 15:59'
   ```

2. **Predicate Pushdown** (column filters):
   ```sql
   -- Filters applied at Parquet file level
   WHERE host = 'server01' AND cpu_percent > 80
   ```

3. **Statistics Pruning** (min/max skip):
   ```sql
   -- Skips files where max(time) < query start_time
   WHERE time > '2025-10-08 15:00'
   ```

4. **Parallel Scan** (multiple files concurrently):
   ```
   DUCKDB_THREADS=4  ← Reads 4 files simultaneously
   ```

### Query Examples

**Time-series aggregation:**
```sql
SELECT
    time_bucket(INTERVAL '5 minutes', time) as bucket,
    host,
    AVG(usage_idle) as avg_idle,
    MAX(usage_user) as max_user
FROM cpu
WHERE time > now() - INTERVAL '1 hour'
GROUP BY bucket, host
ORDER BY bucket DESC
```

**Cross-measurement JOIN:**
```sql
SELECT
    c.time,
    c.host,
    c.usage_user as cpu,
    m.used / m.total as memory_percent
FROM cpu c
JOIN memory m ON c.host = m.host AND c.time = m.time
WHERE c.time > now() - INTERVAL '15 minutes'
```

## Durability and Data Loss

### Data Loss Window

Arc prioritizes **throughput over durability** in the current alpha release:

```
Write Request → In-Memory Buffer → Parquet File → S3 Upload
                     ↑                                ↑
                     │                                │
              Data loss risk                    Durable
              (0-5 seconds)                    (permanent)
```

**Risk scenarios:**
1. **Instance crash** between receiving data and flush → Lost (0-5 seconds of data)
2. **S3 network error** during flush → Retried, then lost if persistent
3. **Out of memory** with buffer overflow → Oldest records dropped

### Mitigation Strategies

**Reduce data loss window:**
```env
# High-durability mode (accept lower throughput)
WRITE_BUFFER_SIZE=10000   # Flush more frequently
WRITE_BUFFER_AGE=1        # Maximum 1 second in memory
```

**Client-side redundancy:**
```python
# Write to multiple Arc instances
async def write_with_redundancy(data):
    await asyncio.gather(
        write_to_arc_instance_1(data),
        write_to_arc_instance_2(data),
        write_to_arc_instance_3(data)
    )
```

**Future improvements** (roadmap):
- [ ] Write-Ahead Log (WAL) to local disk
- [ ] Synchronous write mode (`durability=strict`)
- [ ] Native clustering with replication
- [ ] Kafka/Pulsar integration for guaranteed delivery

## Performance Tuning

### Write Performance

**Goal: Maximize throughput (records/sec)**

```env
# High-throughput configuration
WRITE_BUFFER_SIZE=100000        # Larger batches
WRITE_BUFFER_AGE=10             # Less frequent flushes
WRITE_COMPRESSION=snappy        # Fast compression
WORKERS=84                      # 3× CPU cores for I/O-bound workload
```

**Tradeoffs:**
- ✅ Higher throughput (2M+ records/sec possible)
- ✅ Fewer Parquet files (easier to manage)
- ❌ Higher data loss risk (up to 10 seconds)
- ❌ Higher memory usage (~100MB per measurement)

### Query Performance

**Goal: Minimize query latency (milliseconds)**

```env
# Query optimization
DUCKDB_THREADS=8                # Parallel file reads
DUCKDB_MEMORY_LIMIT=16GB        # More memory for aggregations
DUCKDB_POOL_SIZE=10             # Handle more concurrent queries
QUERY_CACHE_ENABLED=true        # Cache identical queries
QUERY_CACHE_TTL=60              # 1 minute cache
```

**Query best practices:**
1. Always include time range filter: `WHERE time > now() - INTERVAL '1 hour'`
2. Use specific measurements: `FROM cpu` not `FROM *`
3. Limit result size: `LIMIT 10000` for large queries
4. Leverage indexes: `WHERE host = 'x'` (if host is indexed)

### Storage Optimization

**Goal: Balance file size and query speed**

```env
# Optimal balance (default)
WRITE_BUFFER_SIZE=50000         # ~5-10MB files
WRITE_BUFFER_AGE=5              # Flush every 5 seconds
WRITE_COMPRESSION=snappy        # 2-3× compression, fast decode
```

**File size considerations:**
- **Too small** (<1MB): S3 API overhead, slow queries
- **Too large** (>100MB): Slow uploads, harder to prune
- **Optimal** (5-20MB): Fast uploads, good pruning, parallel reads

### Monitoring

**Key metrics to watch:**

```python
# Buffer stats
GET /api/monitoring/buffer
{
    "total_records_buffered": 1250000,
    "total_records_written": 50000000,
    "total_flushes": 1000,
    "total_errors": 2,
    "current_buffer_sizes": {
        "cpu": 25000,
        "memory": 18000
    },
    "oldest_buffer_age_seconds": 3.2
}

# Query pool stats
GET /api/monitoring/pool
{
    "pool_size": 5,
    "active_connections": 3,
    "idle_connections": 2,
    "queue_depth": 0,
    "avg_wait_time_ms": 2.1,
    "avg_execution_time_ms": 45.3
}
```

## Production Checklist

Before deploying Arc to production:

- [ ] Configure production storage backend (MinIO, AWS S3, or GCS - not local)
- [ ] Set appropriate buffer sizes for your workload
- [ ] Enable query cache for dashboard queries
- [ ] Configure DuckDB memory limit
- [ ] Set up monitoring/alerting
- [ ] Plan backup strategy (S3 versioning, lifecycle policies, MinIO replication)
- [ ] Test failure scenarios (network outage, disk full)
- [ ] Document data loss acceptance criteria
- [ ] Configure multi-region replication (if needed)
- [ ] Load test with production-like traffic

## Future Architecture

Planned improvements for future releases:

- **Write-Ahead Log (WAL)**: Persist data to disk before buffering
- **Compaction**: Merge small files into larger ones
- **Clustering**: Multi-node deployments with load balancing
- **Hot/Cold tiering**: SSD for recent data, S3 Glacier for archives
- **Materialized views**: Pre-aggregated queries for dashboards
- **Secondary indexes**: Faster non-time queries
- **Streaming queries**: Real-time aggregations

---

**Questions or feedback?** Open an issue on GitHub or join our community discussions!
