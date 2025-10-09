# Arc Architecture

This document explains Arc's internal architecture, data flow, and design decisions.

## Table of Contents

- [High-Level Overview](#high-level-overview)
- [Data Flow](#data-flow)
- [Buffering System](#buffering-system)
- [Write-Ahead Log (WAL)](#write-ahead-log-wal)
- [Storage Layout](#storage-layout)
- [Multi-Database Architecture](#multi-database-architecture)
- [Compaction System](#compaction-system)
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
- **Write-Ahead Log (WAL)** for optional zero data loss durability
- **Parquet files** for columnar storage
- **Automatic compaction** for query performance optimization
- **Multi-database architecture** for data isolation and multi-tenancy
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
│         │  WAL (Optional)     │  ← Zero data loss           │
│         │  (Per-worker file)  │     (fdatasync/fsync)       │
│         └─────────┬───────────┘                             │
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
│         │  Multi-Database     │     {db}/{measurement}/...  │
│         │  (Hour Partitions)  │                             │
│         └─────────┬───────────┘                             │
│                   │                                          │
│                   ▼                                          │
│         ┌─────────────────────┐                             │
│         │  Compaction System  │  ← Merges small files       │
│         │  (Cron: 5 * * * *)  │     Optimizes queries       │
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

## Write-Ahead Log (WAL)

Arc includes an **optional Write-Ahead Log** for zero data loss guarantees. When enabled, data is persisted to disk before being acknowledged to clients.

### WAL Architecture

```
Write Request → WAL Append → HTTP 202 → Buffer → Parquet → S3
                    ↓                                        ↓
                Durable                                 Durable
              (on disk)                              (permanent)
```

**Per-Worker WAL Files:**
```
./data/wal/
├── worker-1-20251008_140530.wal
├── worker-2-20251008_140530.wal
├── worker-3-20251008_140530.wal
└── worker-4-20251008_140530.wal
```

Each Gunicorn worker has its own WAL file to avoid lock contention, enabling parallel writes.

### WAL Configuration

```env
# Enable WAL with balanced durability
WAL_ENABLED=true
WAL_DIR=./data/wal
WAL_SYNC_MODE=fdatasync        # Recommended (near-zero data loss)
WAL_MAX_SIZE_MB=100            # Rotate at 100MB
WAL_MAX_AGE_SECONDS=3600       # Rotate after 1 hour
```

**Sync Modes:**
- `fsync`: Full metadata sync - zero data loss (~1.61M rec/s, -17% throughput)
- `fdatasync`: Data-only sync - near-zero data loss (~1.56M rec/s, -19% throughput)
- `async`: OS buffer cache - <1s data loss risk (~1.60M rec/s, -17% throughput)

### WAL Recovery

On startup, Arc automatically recovers from WAL files:

```
2025-10-08 14:30:00 [INFO] WAL recovery started: 4 files
2025-10-08 14:30:01 [INFO] Recovering WAL: worker-1-20251008_143000.wal
2025-10-08 14:30:01 [INFO] WAL read complete: 1000 entries, 5242880 bytes
2025-10-08 14:30:05 [INFO] WAL recovery complete: 4000 batches, 200000 entries
2025-10-08 14:30:05 [INFO] WAL archived: worker-1-20251008_143000.wal.recovered
```

**Recovery features:**
- Parallel recovery across workers
- Checksum verification (CRC32)
- Corrupted entries are skipped and logged
- Automatic cleanup of old recovered files (24 hours)

### WAL Trade-offs

**Enable WAL if:**
- ✅ Zero data loss is required
- ✅ Regulated industry (finance, healthcare)
- ✅ Can accept 19% throughput reduction

**Disable WAL if:**
- ⚡ Maximum throughput is priority (1.93M rec/s)
- 💰 Can tolerate 0-5s data loss risk
- 🔄 Have upstream retry/queue mechanisms

See [docs/WAL.md](docs/WAL.md) for complete documentation.

## Storage Layout

### Directory Structure

Arc uses **multi-database architecture** with **hour-level time partitioning** for optimal query performance:

```
s3://arc/
├── default/                          ← Database namespace
│   ├── cpu/                          ← Measurement (table)
│   │   ├── 2025/                     ← Year partition
│   │   │   ├── 10/                   ← Month partition
│   │   │   │   ├── 08/               ← Day partition
│   │   │   │   │   ├── 14/           ← Hour partition
│   │   │   │   │   │   ├── cpu_20251008_140530_50000.parquet
│   │   │   │   │   │   ├── cpu_20251008_141045_50000.parquet
│   │   │   │   │   │   └── cpu_20251008_142310_50000.parquet
│   │   │   │   │   └── 15/
│   │   │   │   │       └── cpu_20251008_150102_50000.parquet
│   ├── memory/
│   │   └── 2025/10/08/14/*.parquet
│   └── disk/
│       └── 2025/10/08/14/*.parquet
├── production/                       ← Another database
│   ├── cpu/
│   │   └── 2025/10/08/14/*.parquet
│   └── memory/
│       └── 2025/10/08/14/*.parquet
└── benchmark/                        ← Testing database
    ├── cpu/
    └── disk/
```

**Path format:** `{bucket}/{database}/{measurement}/{year}/{month}/{day}/{hour}/{file}.parquet`

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

## Multi-Database Architecture

Arc supports **multiple databases** (namespaces) within a single instance for data isolation and multi-tenancy.

### Database Isolation

Each database is a separate namespace with its own measurements and data:

```sql
-- Write to specific database via HTTP header
POST /write/v2/msgpack
Headers:
  x-arc-database: production

-- Query specific database
SELECT * FROM production.cpu WHERE time > now() - INTERVAL '1 hour'

-- Query different database
SELECT * FROM staging.cpu WHERE time > now() - INTERVAL '1 hour'

-- Show all databases
SHOW DATABASES
-- Returns: default, production, staging, benchmark

-- Show tables in specific database
SHOW TABLES FROM production
-- Returns: cpu, memory, disk
```

### Database Benefits

**Multi-Tenancy:**
```
production/    ← Customer A data
staging/       ← Testing environment
development/   ← Development environment
```

**Environment Isolation:**
```
prod/          ← Production metrics (critical)
test/          ← Testing data (can be deleted)
benchmark/     ← Performance testing (temporary)
```

**Cost Tracking:**
- Each database has independent storage costs
- Query costs can be tracked per-database
- Easy to analyze per-tenant usage

### Database Routing

**Write Path:**
```python
# Default database (no header)
POST /write/v2/msgpack → default/cpu/...

# Explicit database (x-arc-database header)
POST /write/v2/msgpack
x-arc-database: production → production/cpu/...
```

**Query Path:**
```sql
-- Explicit database.table syntax
SELECT * FROM production.cpu

-- SHOW TABLES with database filter
SHOW TABLES FROM production

-- Cross-database queries
SELECT
  p.time,
  p.usage as prod_usage,
  s.usage as staging_usage
FROM production.cpu p
JOIN staging.cpu s ON p.time = s.time
```

### Superset Integration

Arc databases are exposed as **schemas** in Apache Superset:

1. Connect to Arc using `arc://` connection string
2. Superset calls `SHOW DATABASES` → sees all databases
3. User selects schema (e.g., "production")
4. Superset calls `SHOW TABLES FROM production` → shows measurements
5. Queries are database-scoped automatically

See [arc-superset-dialect](https://github.com/basekick-labs/arc-superset-dialect) for details.

## Compaction System

Arc includes an **automatic compaction system** that merges small Parquet files into larger, optimized files for faster queries.

### The Small File Problem

At 1.93M records/sec with 5-second flush interval:
```
→ 9.65M records every 5 seconds
→ ~12 files per minute per measurement
→ ~720 files per hour per measurement
→ ~17,280 files per day per measurement
```

**Impact on queries:**
- 🐌 Slow queries (DuckDB must open/scan hundreds of files)
- 💸 High S3 costs (more API calls)
- 📊 Poor compression (small files compress less efficiently)

### Compaction Process

```
┌─────────────────────────────────────────────────────────┐
│  1. Scheduler (Cron: "5 * * * *")                       │
│     Wakes up every hour at :05                          │
└──────────────────┬──────────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────────┐
│  2. Find Candidates                                      │
│     - Partitions older than 1 hour                      │
│     - At least 10 files in partition                    │
│     - Not already compacted                             │
└──────────────────┬──────────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────────┐
│  3. For each partition:                                  │
│     a. Acquire SQLite lock (prevents concurrent)        │
│     b. Download small files to temp directory           │
│     c. Merge using DuckDB (sorted by time)              │
│     d. Upload compacted file (ZSTD compression)         │
│     e. Delete old small files                           │
│     f. Release lock                                     │
└─────────────────────────────────────────────────────────┘
```

**Before compaction:**
```
default/cpu/2025/10/08/14/
├── cpu_20251008_140001_5000.parquet  (5MB)
├── cpu_20251008_140006_5000.parquet  (5MB)
├── ... (720 files)
Total: 720 files × 5MB = 3.6GB
```

**After compaction:**
```
default/cpu/2025/10/08/14/
└── cpu_20251008_14_compacted.parquet  (512MB, ZSTD)

1 file, 85% compression → Query speed: 37× faster!
```

### Compaction Configuration

```toml
[compaction]
enabled = true
min_age_hours = 1              # Don't compact current hour
min_files = 10                 # Minimum files to trigger compaction
target_file_size_mb = 512      # Target compacted file size
schedule = "5 * * * *"         # Every hour at :05
max_concurrent_jobs = 2        # Parallel compaction jobs
compression = "zstd"           # Better compression than Snappy
compression_level = 3          # Balanced speed vs size
```

### Compaction Features

**Per-Database:**
- Each database is compacted independently
- Compaction jobs are scoped to database namespace
- Locks include database prefix (parallel across databases)

**Crash-Safe:**
- SQLite-based distributed locking
- Locks expire after 2 hours (automatic recovery)
- Failed jobs are logged and retried next cycle

**Query-Friendly:**
- Queries work during compaction (non-blocking)
- Late-arriving data creates side-files (seamless for DuckDB)
- Compacted files include time-ordering for better compression

**Monitoring:**
```bash
# Check compaction status
GET /api/compaction/status

# View recent jobs
GET /api/compaction/stats

# Manually trigger compaction
POST /api/compaction/trigger
```

See [docs/COMPACTION.md](docs/COMPACTION.md) for complete documentation.

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

**Implemented improvements:**
- ✅ Write-Ahead Log (WAL) for zero data loss
- ✅ Multi-database architecture for data isolation
- ✅ Automatic compaction for query optimization

**Future improvements** (roadmap):
- [ ] Native clustering with replication
- [ ] Kafka/Pulsar integration for guaranteed delivery
- [ ] Real-time streaming queries

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

### Implemented Features (Current Release)

- ✅ **Write-Ahead Log (WAL)**: Optional zero data loss with fdatasync/fsync
- ✅ **Multi-Database Architecture**: Data isolation and multi-tenancy support
- ✅ **Automatic Compaction**: Merge small files for 37× faster queries

### Planned Improvements (Roadmap)

- **Clustering**: Multi-node deployments with load balancing
- **Hot/Cold tiering**: SSD for recent data, S3 Glacier for archives
- **Materialized views**: Pre-aggregated queries for dashboards
- **Secondary indexes**: Faster non-time queries
- **Streaming queries**: Real-time aggregations with subscriptions
- **Native replication**: Multi-region data redundancy
- **Query federation**: Cross-instance distributed queries

---

**Questions or feedback?** Open an issue on GitHub or join our community discussions!
