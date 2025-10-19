# Arc: A High-Performance Time-Series Database Through Architectural Simplicity

**Date:** October 19, 2025
**Authors:** Basekick Labs Engineering Team
**Version:** 1.2

---

## Abstract

We present Arc, a time-series database that achieves exceptional performance through architectural simplicity rather than complexity. By building on proven components—DuckDB for query execution, Apache Parquet for storage, and Apache Arrow for data transport—Arc achieves 2.42M records/second ingestion, 5× better storage efficiency than QuestDB (13.76 GiB vs 67.84 GiB), and dominates cold query performance on the ClickBench analytical benchmark (22.64s on M3 Max). Real-world production testing demonstrates compaction's dramatic impact: 2,704 files reduced to 3 (901× reduction) with 80.4% compression ratio. This paper explores Arc's architecture, explaining how strategic design decisions and a sophisticated compaction system enable these results without custom query optimization or tuning.

## 1. Introduction

### 1.1 The Time-Series Challenge

Modern applications generate time-series data at unprecedented rates. IoT sensors, financial markets, application metrics, and industrial systems all produce continuous streams of timestamped measurements. Traditional relational databases struggle with this workload, leading to the proliferation of specialized time-series databases.

However, many time-series databases introduce significant complexity: custom storage engines, distributed consensus protocols, proprietary query languages, and intricate tuning parameters. This complexity creates operational burden and makes reproducible performance difficult to achieve.

### 1.2 The Journey to Arc

Arc didn't emerge from a whiteboard—it evolved from real-world experience building time-series systems. The journey began in November 2022 when Ignacio Van Droogenbroeck started building APIs to automate deployment of time-series tools across cloud providers. By June 2023, this became a full-time pursuit focused on IoT solutions in Latin America.

The first attempt, Wavestream, achieved approximately 20,000 writes per second on modest hardware but ultimately ended in 2024. Rather than salvaging that codebase, a fresh start seemed necessary—the previous attempt felt tied to lessons learned the hard way.

The path continued through Historian (2024-2025), designed as a tiered storage solution for InfluxDB. This platform stored data in Parquet files on S3 and successfully attracted InfluxDB Enterprise customers facing scaling challenges. During Historian's development, integrating DuckDB for SQL queries on Parquet files proved transformative. Time-based indexing and incremental improvements gradually revealed something more ambitious than an archive solution.

Arc represents the culmination of this evolution—learning from what worked, discarding what didn't, and embracing simplicity.

### 1.3 The Arc Hypothesis

We asked a different question: What if we could achieve exceptional performance by composing battle-tested open-source components rather than building everything from scratch?

Arc's design validates this hypothesis. By leveraging DuckDB's analytical query engine, Parquet's columnar storage format, and Arrow's zero-copy data representation, Arc achieves performance that rivals or exceeds purpose-built time-series databases—while maintaining a codebase of approximately 5,500 lines across its core modules.

### 1.4 Key Results

Arc's approach delivers measurable benefits:

- **Ingestion Performance**: 2.42M records/second with columnar MessagePack format on Apple M3 Max (native deployment, 400 workers)
- **Storage Efficiency**: 13.76 GiB for the ClickBench dataset compared to 67.84 GiB for QuestDB (5× reduction)
- **Cold Query Performance**: Fastest across all 43 ClickBench queries without cache warmup (22.64s total on M3 Max, 35.18s on AWS c6a.4xlarge)
- **Real-World Compaction**: 2,704 files reduced to 3 files (901× reduction) with 80.4% compression ratio (3.7 GB → 724 MB)
- **Authentication Overhead**: Near-zero with intelligent caching (99.9%+ hit rate, 30s TTL)
- **Operational Simplicity**: Single-node deployment with straightforward configuration
- **Standards Compliance**: Uses industry-standard formats (Parquet, Arrow) and protocols (HTTP, SQL)
- **Community Adoption**: Over 200 GitHub stars within six days of public release

The remainder of this paper explores how Arc achieves these results through careful architectural design.

## 2. System Architecture Overview

### 2.1 Layered Design

Arc's architecture follows a clear separation of concerns, organized into four primary layers:

**Storage Layer**: Manages persistence across multiple backend types (local filesystem, Amazon S3, MinIO, Google Cloud Storage, Ceph). This layer abstracts storage details, allowing the same codebase to operate on commodity hardware or cloud object storage.

**Query Engine Layer**: Integrates DuckDB, an embedded analytical database optimized for OLAP workloads. Rather than implementing a custom query planner and executor, Arc leverages DuckDB's mature, battle-tested engine.

**API Layer**: Provides HTTP-based access through FastAPI, a modern Python framework. The API offers both JSON and Apache Arrow response formats, serving different client needs efficiently.

**Compaction Layer**: Continuously optimizes storage by merging small files into larger, well-compressed units. This background process is critical to Arc's storage efficiency and query performance.

Each layer operates independently, connected through well-defined interfaces. This modularity enables testing, optimization, and replacement of individual components without systemic rewrites.

### 2.2 Data Flow

Understanding Arc's data flow illuminates its performance characteristics.

**Write Path**: When data arrives (via InfluxDB line protocol, MessagePack binary format, or HTTP JSON), Arc buffers it in memory. Once a buffer accumulates 50,000 rows or five seconds elapses, Arc writes a Parquet file to storage. This batching amortizes I/O costs while maintaining write latency in the single-digit seconds.

**Read Path**: Query requests arrive at the API layer, which consults a cache. Cache misses trigger query execution: the SQL is rewritten to reference Parquet files, submitted to DuckDB's connection pool, executed across partitioned data, and returned as either JSON or Arrow format. Connection pooling with priority queuing ensures fair resource allocation under load.

**Compaction Path**: A background scheduler identifies hour-partitions containing many small files. These partitions are downloaded, validated, merged using DuckDB, compressed with ZSTD, and uploaded back to storage. Old files are then deleted. This process runs continuously, keeping storage optimized without manual intervention.

## 3. Storage Layer: Flexibility Without Complexity

### 3.1 The Storage Abstraction

Arc's storage layer presents a unified interface regardless of the underlying backend. Whether data resides on a local SSD, MinIO cluster, AWS S3, or Google Cloud Storage, the same eight methods handle all operations:

```python
upload_file(local_path, key, database_override)
upload_parquet_files(local_dir, measurement)
get_s3_path(measurement, year, month, day, hour)
list_objects(prefix, max_keys)
download_file(key, local_path)
delete_file(key)
configure_duckdb_s3(duckdb_conn)
```

This abstraction emerged from a key insight: time-series data naturally partitions by time, creating a predictable storage hierarchy. Arc stores files following this pattern:

```
{bucket}/{database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
```

For example: `s3://analytics/production/cpu/2025/10/16/14/data_1697465234.parquet`

This structure enables efficient range queries (DuckDB can skip entire directories based on time predicates) and straightforward compaction (merge files within hour boundaries).

### 3.2 Database Scoping

An important architectural decision involves database scoping. Each storage backend instance operates within a specific database namespace. The backend's `list_objects()` method returns paths relative to its database, not absolute paths.

This scoping simplifies multi-database deployments. A single Arc instance can serve multiple databases by initializing separate backend instances, each scoped to its namespace. DuckDB queries use database-qualified table names (`production.cpu` vs `staging.cpu`), which Arc's SQL rewriter translates to appropriate storage paths.

### 3.3 Local vs Cloud Optimizations

Arc's storage backends optimize for their medium's characteristics.

**Local Filesystem Backend**: Uses Python's `Path` API and `aiofiles` for async I/O. Upload concurrency is set to 50 simultaneous operations—local disks handle this load easily. When downloading files (for compaction), the local backend creates symlinks rather than copying, saving I/O and latency.

**Cloud Storage Backends**: Use boto3 for S3-compatible services. Upload concurrency drops to 20 to avoid overwhelming network connections or triggering rate limits. DuckDB configuration includes endpoint URLs, access keys, SSL settings, and URL style (path vs virtual-hosted).

For AWS S3 specifically, Arc supports both standard S3 and S3 Express One Zone. The latter uses directory buckets and requires special endpoint configuration:

```python
endpoint = f"s3express-{availability_zone}.{region}.amazonaws.com"
```

**Google Cloud Storage**: Presents unique challenges since DuckDB's GCS support uses signed URLs. Arc's GCS backend generates these URLs using service account credentials or HMAC keys, enabling DuckDB to read Parquet files directly from `gs://` URLs.

### 3.4 Storage Backend Performance Characteristics

Each storage backend exhibits distinct performance characteristics, validated through production benchmarking:

| Backend | Throughput | Use Case | Key Advantages | Trade-offs |
|---------|-----------|----------|----------------|------------|
| **Local NVMe** | **2.42M RPS** | Single-node, development, edge | Direct I/O, minimal overhead, zero network latency | No distribution, single point of failure |
| **MinIO** | **~2.1M RPS** | Distributed production, multi-tenant | S3-compatible, scalable, high availability, erasure coding | Requires MinIO service, slight network overhead |
| **AWS S3** | Cloud-dependent | Production, unlimited scale | Fully managed, 99.999999999% durability, global availability | Network latency, per-request costs |
| **Google Cloud Storage** | Cloud-dependent | Google Cloud deployments | Integrated with GCP, global CDN, multi-region | Network latency, egress costs |

These measurements reflect real-world ingestion performance on Apple M3 Max (14 cores, 36GB RAM) with columnar MessagePack protocol. Network-based backends (MinIO, S3, GCS) introduce minimal overhead—Arc's async I/O and batching amortize network round-trips effectively.

The performance gap between local NVMe (2.42M RPS) and MinIO (~2.1M RPS) represents approximately 13% overhead for distributed storage—a remarkably small price for the operational benefits of distributed architecture.

### 3.5 Why This Matters

Storage layer flexibility has practical implications. Development teams can run Arc on laptops using local storage. Staging environments might use MinIO for cost savings. Production can leverage S3's durability and availability. The same codebase, unchanged, operates across all these environments.

This flexibility also future-proofs Arc. New storage backends (Azure Blob Storage, for instance) require implementing eight methods. No changes to query execution, API endpoints, or compaction logic.

## 4. Query Engine: Standing on Giants' Shoulders

### 4.1 Why DuckDB?

Building a high-performance analytical query engine requires years of optimization work: vectorized execution, pushdown predicates, adaptive query planning, JIT compilation. Rather than reinventing these wheels, Arc uses DuckDB.

DuckDB is an embedded analytical database—think SQLite for analytics. It excels at scanning columnar data (Parquet), processes data in batches (vectorization), and pushes filters into file readers (avoiding unnecessary I/O). For Arc's workload (large analytical queries over Parquet files), DuckDB is ideal.

Arc leverages DuckDB's production-ready capabilities while adding intelligent tuning for real-world workloads. While ClickBench benchmarks use default settings (ensuring fair comparison), production deployments benefit from careful optimization of memory limits, parallelism, and caching strategies.

### 4.2 Connection Pool Architecture

A naive approach would create a DuckDB connection per query, destroying it afterward. This wastes resources: connection creation involves loading extensions (httpfs, aws), configuring S3 credentials, and initializing internal structures.

Arc instead maintains a connection pool—currently five connections by default, configurable via `DUCKDB_POOL_SIZE`. These connections are:

1. Created at startup
2. Configured once with S3/GCS credentials
3. Reused across queries
4. Health-checked every 60 seconds
5. Recreated if unhealthy

When queries arrive faster than connections are available, Arc queues them using a priority system:

- **CRITICAL**: Health checks, system queries (highest priority)
- **HIGH**: Interactive dashboard queries
- **NORMAL**: Standard user queries
- **LOW**: Background batch queries

Within each priority level, queuing follows FIFO order. A query waits up to its timeout (default: 300 seconds) for an available connection.

### 4.3 Production Performance Tuning

While Arc's ClickBench results use DuckDB's default settings (ensuring benchmark fairness), production deployments require careful tuning to prevent out-of-memory kills, maximize multi-core utilization, and optimize query performance under concurrent load.

Arc implements comprehensive DuckDB tuning across four critical dimensions:

**Memory Management**: Each DuckDB connection receives a configurable memory limit (default: 8GB), preventing individual queries from exhausting system memory. This hard limit protects against runaway analytical queries that might otherwise trigger Linux OOM killer, crashing worker processes. When queries exceed their memory budget, DuckDB gracefully spills intermediate results to disk (configured temp directory), maintaining availability at the cost of some performance.

The memory limit calculation follows a simple formula:
```
memory_limit = (Total_RAM × 0.8) / (workers × pool_size)
```

For a 64GB server running 8 workers with pool_size=5: `(64GB × 0.8) / (8 × 5) = 1.28GB per connection`. This ensures 40 connections (8 workers × 5 pool) fit comfortably in RAM with 20% headroom for OS and other processes.

**CPU Parallelism**: DuckDB's thread setting controls how many CPU cores each query can utilize. Arc defaults to matching physical core count (14 on M3 Max), but production tuning depends on concurrency expectations:

- **Low concurrency** (1-10 queries): `threads = CPU_cores` maximizes single-query performance
- **Medium concurrency** (10-50 queries): `threads = CPU_cores / pool_size` balances speed and fairness
- **High concurrency** (50+ queries): `threads = 1-2` prevents thread contention, relies on pool parallelism

This dynamic approach prevents the common mistake of over-threading: with 14 cores and 40 active queries each using 14 threads, you'd have 560 threads competing for 14 cores—pure overhead.

**Object Cache**: DuckDB's object cache stores Parquet file metadata (schema, row group statistics, column statistics) in memory. This provides dramatic speedups for repeated queries on the same files—Arc's benchmarks show 2-10× performance improvement on subsequent queries.

The cache is especially valuable for dashboard workloads where the same queries execute repeatedly with different time ranges. First execution reads and caches metadata for all relevant Parquet files. Subsequent executions skip metadata reading entirely, proceeding directly to data scanning.

**Compression and Optimization**: Arc configures DuckDB to use ZSTD compression for intermediate results, reducing memory pressure and disk I/O when queries spill to temporary storage. Additionally, `preserve_insertion_order=false` gives the query optimizer freedom to reorder operations for maximum efficiency—time-series data doesn't require insertion order preservation since queries order by timestamp explicitly.

**Configuration Interface**: All tuning parameters expose through arc.conf and environment variables:

```toml
[duckdb]
pool_size = 5                        # Concurrent connections per worker
max_queue_size = 100                 # Query overflow queue depth
memory_limit = "8GB"                 # Per-connection memory cap
threads = 14                         # CPU cores per query
temp_directory = "./data/duckdb_tmp" # Spill location (NVMe recommended)
enable_object_cache = true           # Metadata caching (2-10× speedup)
```

Environment variable overrides (`DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS`, etc.) support Docker and Kubernetes deployments where configuration files are inconvenient.

**Production Impact**: These optimizations transform DuckDB from a research-quality engine into a production-hardened component. Memory limits prevent cascading failures. Thread tuning ensures fair resource allocation under load. Object caching eliminates redundant metadata reads. The result: stable, predictable performance across diverse workloads without manual per-query tuning.

For complete tuning guidance including troubleshooting and monitoring strategies, see `docs/DUCKDB_PRODUCTION_TUNING.md`.

### 4.4 Query Execution Flow

Let's trace a query's journey through Arc's engine:

**Step 1: Cache Check**
Arc maintains a query cache indexed by SQL and limit. Cache hits return immediately—no database access. Cache TTL is configurable (default: 60 seconds).

**Step 2: SQL Rewriting**
Arc's SQL rewriter transforms logical table references into physical Parquet paths. Consider this query:

```sql
SELECT AVG(cpu_usage) FROM production.cpu
WHERE timestamp > '2025-10-16'
LIMIT 100
```

The rewriter converts it to:

```sql
SELECT AVG(cpu_usage) FROM read_parquet(
    's3://bucket/production/cpu/**/*.parquet',
    union_by_name=true
)
WHERE timestamp > '2025-10-16'
LIMIT 100
```

The `union_by_name=true` parameter handles schema evolution—if different Parquet files have different columns (a sensor was added or removed), DuckDB fills missing values with NULL rather than failing.

**Step 3: Connection Acquisition**
The rewritten query enters the connection pool. If a connection is immediately available (pool not exhausted), execution proceeds. Otherwise, the query waits in its priority queue.

**Step 4: Execution**
The connection executes the query in a thread pool executor (queries are CPU-bound and block). DuckDB reads Parquet files, applying predicates at the file level (skipping entire files based on metadata), row group level, and row level.

**Step 5: Early Return**
Here's a critical optimization: Arc returns the connection to the pool *before* serializing results. While serialization happens (converting DuckDB's internal format to JSON or Arrow), other queries can use that connection. This maximizes connection utilization.

**Step 6: Serialization**
For JSON responses, Arc converts each row to JSON-serializable types (datetime objects become ISO strings, etc.). For Arrow responses, Arc serializes the result to Arrow IPC stream format—a binary columnar representation that clients can read with zero-copy efficiency.

### 4.5 Custom Commands

DuckDB doesn't natively understand `SHOW TABLES` or `SHOW DATABASES` in Arc's multi-database context. Arc intercepts these queries before reaching DuckDB:

**SHOW TABLES**: Lists measurements in a database by scanning storage for Parquet files. For each measurement, Arc reports the database name, table name, storage path, file count, and total size. This works across all storage backends through the unified `list_objects()` interface.

**SHOW DATABASES**: Scans the bucket root for top-level directories (database names). On S3, this uses the `list_objects_v2` API with delimiter="/" to get common prefixes efficiently.

These custom commands enable standard SQL tooling (like Apache Superset) to browse Arc's schema without special configuration.

### 4.6 Memory Management

Long-running Python processes (especially with Gunicorn workers) can accumulate memory if not carefully managed. Arc implements several safeguards:

**Connection State Reset**: Between queries, Arc executes `SELECT NULL` on each connection. This forces DuckDB to release its internal result cache, preventing memory buildup.

**Explicit Arrow Cleanup**: After serializing Arrow responses, Arc explicitly deletes the Arrow table, buffer, and writer objects, then triggers garbage collection. Python's GC might delay cleanup otherwise, causing memory spikes.

**Pool Metrics**: Arc exposes connection pool metrics (`/metrics/query-pool`) showing active connections, queue depth, and average wait/execution times. This helps operators identify memory or performance issues.

## 5. API Layer: Modern Interfaces for Diverse Clients

### 5.1 FastAPI and ORJSON

Arc's API layer uses FastAPI, a modern Python framework supporting async/await, automatic OpenAPI documentation, and dependency injection. FastAPI's performance characteristics suit Arc well—benchmark results show it matches Go and Node.js frameworks in throughput.

Arc configures FastAPI with `ORJSONResponse` as the default response class. ORJSON is a Rust-based JSON library using SIMD instructions, delivering 20-50% faster serialization than Python's stdlib json. For analytical queries returning thousands of rows, this matters.

### 5.2 Middleware Pipeline

Requests traverse four middleware layers (order matters):

**1. Request ID Middleware**: Generates a unique ID for each request, adding it to log context. This enables tracing a request through logs even with multiple workers.

**2. CORS Middleware**: Configured to allow all origins (`*`). This supports Excel add-ins and web applications without complex origin management. Credentials are disabled (required when using wildcard origins).

**3. Authentication Middleware**: Validates API tokens from the `X-API-Key` header. Token validation uses an LRU cache (TTL: 30 seconds) to avoid database lookups on every request. Configured endpoints (like `/health`, `/docs`) bypass auth via an allowlist.

This caching strategy eliminates authentication as a performance bottleneck. Production benchmarks demonstrate near-zero overhead:

| Configuration | Throughput | p50 Latency | p95 Latency | p99 Latency |
|--------------|-----------|-------------|-------------|-------------|
| Auth Disabled | 2.42M RPS | 1.64ms | 27.27ms | 41.63ms |
| **Auth + Cache (30s TTL)** | **2.42M RPS** | **1.74ms** | **28.13ms** | **45.27ms** |
| Auth (no cache) | 2.31M RPS | 6.36ms | 41.41ms | 63.31ms |

Token caching achieves 99.9%+ hit rate at sustained high throughput, adding only 0.1ms overhead versus no authentication. The cache accepts a 30-second revocation delay—deleted tokens remain valid until cache expiry. For immediate revocation, administrators can manually invalidate the cache via `POST /auth/cache/invalidate`.

**4. Request Size Middleware**: Rejects POST/PUT/PATCH requests exceeding 100MB (configurable). This prevents memory exhaustion from malicious large uploads.

### 5.3 Primary Worker Detection

When deploying Arc with multiple Gunicorn workers (common for production—the codebase references 42 workers), each worker initializes independently. Without coordination, this creates problems:

- 42 compaction schedulers running simultaneously (file conflicts)
- 42 workers logging "compaction scheduler started" (log noise)
- 42 admin tokens generated on first run (confusion)

Arc solves this with file-based locking using `fcntl.flock()`. At startup, each worker attempts to acquire an exclusive, non-blocking lock on `/tmp/arc_primary_worker.lock`. Only one succeeds—this becomes the primary worker.

**Primary worker**:
- Logs at INFO level
- Runs compaction scheduler
- Generates initial admin tokens
- Aggregates metrics

**Secondary workers**:
- Log at DEBUG level
- Execute queries (all workers handle query traffic)
- Handle write requests (all workers write data)
- Skip compaction scheduling (delegated to primary)

This pattern reduces log volume from 42× messages to 1× while maintaining full query/write capacity across all workers.

### 5.4 Dual Response Formats

Arc supports two response formats for queries, serving different client needs:

**JSON Format** (`POST /query`):
```json
{
  "success": true,
  "columns": ["timestamp", "cpu_usage", "host"],
  "data": [
    ["2025-10-16T14:00:00", 45.2, "server1"],
    ["2025-10-16T14:00:01", 46.1, "server1"]
  ],
  "row_count": 2,
  "execution_time_ms": 125.4
}
```

This format works universally—web browsers, curl, any HTTP client. Data representation is simple (nested arrays), and JSON parsers exist everywhere.

**Arrow Format** (`POST /query/arrow`):
```
[Binary Apache Arrow IPC Stream]
Headers:
  Content-Type: application/vnd.apache.arrow.stream
  X-Row-Count: 1000000
  X-Execution-Time-Ms: 89.3
```

Arrow format returns data as a binary columnar stream. Clients with Arrow libraries (pandas, polars, Superset) read this with zero-copy efficiency. The ClickBench results show 28-75% performance improvement with Arrow format—less serialization overhead, more efficient data representation, better compression.

### 5.5 Startup and Shutdown Lifecycle

Arc's startup sequence follows a carefully orchestrated process:

1. **Primary Worker Detection**: Acquire lock, determine logging verbosity
2. **Configuration Loading**: Read `config.yaml` or environment variables
3. **Storage Connection**: Auto-create if none exists, validate backend type
4. **DuckDB Engine**: Initialize with connection pool, configure S3/GCS
5. **Export Scheduler**: Start background job scheduler for data pipeline
6. **Metrics Collection**: Begin collecting system metrics (CPU, memory, disk)
7. **Write Buffers**: Initialize Parquet writers for ingestion endpoints
8. **Compaction**: Create manager and scheduler (primary worker only)
9. **Query Cache**: Initialize TTL-based cache
10. **Admin Token**: Generate if first run, display prominently

Shutdown reverses this process: stop schedulers, flush buffers, close connections, release locks.

## 6. The Compaction System: Arc's Secret Weapon

### 6.1 The Problem

Arc's write path prioritizes throughput: buffer 50,000 rows, flush to Parquet, repeat. Over hours, this creates thousands of small files:

```
cpu/2025/10/16/14/data_1697465234.parquet (2.1 MB)
cpu/2025/10/16/14/data_1697465290.parquet (1.8 MB)
cpu/2025/10/16/14/data_1697465345.parquet (2.3 MB)
... (hundreds more)
```

Small files hurt performance:

- **Query Overhead**: Opening hundreds of files adds latency
- **Metadata Costs**: Each file has a Parquet footer (metadata)
- **Poor Compression**: Small files don't compress as well as large ones
- **Storage Waste**: Block alignment and minimum allocation units waste space

Without addressing this, Arc would fail to achieve competitive performance.

### 6.2 The Solution: Time-Based Compaction

Arc's compaction system merges small files within hourly partitions. Once an hour-partition completes (current time > hour + min_age_hours), and it contains enough files (>= min_files), compaction triggers.

The compaction job:

1. **Downloads** all Parquet files for that hour-partition to a temp directory
2. **Validates** each file by attempting to read it with DuckDB (quarantines corrupted files)
3. **Merges** valid files using DuckDB's `COPY` command
4. **Optimizes** the output with ZSTD compression level 3 and 122,880 row groups
5. **Uploads** the compacted file back to storage
6. **Deletes** the original small files
7. **Cleans** the temp directory

This transforms hundreds of 2MB files into one 500MB file, dramatically improving query performance and storage efficiency.

### 6.3 The DuckDB Merge Operation

Arc uses DuckDB itself to perform compaction. This might seem circular (using the query engine for storage management), but it's brilliant—DuckDB already knows how to:

- Read Parquet files efficiently
- Handle schema differences (`union_by_name`)
- Apply optimal compression
- Write well-structured Parquet output

The merge query looks like:

```sql
COPY (
    SELECT * FROM read_parquet('/tmp/compaction/*.parquet', union_by_name=true)
    ORDER BY time
) TO '/tmp/output.parquet' (
    FORMAT PARQUET,
    COMPRESSION ZSTD,
    COMPRESSION_LEVEL 3,
    ROW_GROUP_SIZE 122880
)
```

The `ORDER BY time` clause ensures the output maintains chronological order—critical for range queries where DuckDB can stop scanning once it passes the time filter.

### 6.4 ZSTD Compression

Arc uses Zstandard (ZSTD) compression at level 3. Why level 3 specifically?

**Lower levels (1-2)**: Faster compression, lower ratios
**Level 3**: Sweet spot—good compression ratios (40-60% typical) with acceptable CPU cost
**Higher levels (4+)**: Better compression, but CPU cost grows exponentially

For analytical workloads, storage I/O often dominates. Reading a 200MB compressed file is faster than reading a 500MB uncompressed file, even accounting for decompression CPU time. Level 3 achieves most of the compression benefit without excessive CPU cost.

### 6.5 Corruption Handling

Real-world systems encounter corrupted data: hardware failures, network interruptions, buggy clients. Arc's compaction gracefully handles this:

Before merging, Arc validates each file:

```python
try:
    con.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path}')").fetchone()
    valid_files.append(file_path)
except Exception as e:
    logger.error(f"Skipping corrupted file {file_path.name}: {e}")
    quarantine_dir.mkdir(exist_ok=True)
    file_path.rename(quarantine_dir / file_path.name)
```

Corrupted files move to a quarantine directory. Valid files proceed to compaction. This approach:

- **Prevents cascading failures**: One bad file doesn't block compaction
- **Preserves evidence**: Corrupted files remain for investigation
- **Maintains availability**: System continues serving queries from valid data

### 6.6 Multi-Database Compaction

Arc supports multiple databases within a single instance. Compaction must handle this: scanning all databases, switching storage backend database context safely.

The challenge: multiple concurrent compaction jobs share one storage backend instance. If Job A changes `backend.database = "production"` while Job B reads it, race conditions occur.

Arc solves this with a storage lock:

```python
async with self._storage_lock:
    original_database = self.storage_backend.database
    self.storage_backend.database = job_database
    await job.run()
    self.storage_backend.database = original_database
```

Each job acquires the lock, switches database, executes, restores original database. Concurrent jobs queue on the lock, preventing corruption.

### 6.7 Concurrency Control

Arc limits concurrent compaction jobs (default: 2) using an async semaphore:

```python
semaphore = asyncio.Semaphore(max_concurrent)

async def _compact_with_limit(candidate):
    async with semaphore:
        return await self.compact_partition(candidate)
```

Why limit concurrency?

- **I/O Pressure**: Each job downloads/uploads hundreds of MB
- **Memory Usage**: Merging holds files in memory
- **Storage Throttling**: Cloud providers rate-limit API calls

Two concurrent jobs provide reasonable throughput without overwhelming resources.

### 6.8 Compaction Scheduling

Compaction runs on a cron-style schedule (default: `5 * * * *`—every hour at 5 minutes past). The scheduler:

1. Finds candidate partitions (old enough, enough files, not yet compacted)
2. Launches compaction jobs (up to max_concurrent)
3. Logs results (successes, failures, bytes saved)

Only the primary worker runs the scheduler. Secondary workers have compaction managers (for manual triggers via API) but don't auto-schedule.

### 6.9 Real-World Compaction Results

Production testing validates compaction's dramatic impact. In a high-throughput ingestion scenario, Arc accumulated 2,704 small Parquet files (ranging from 1-5 MB each) totaling 3.7 GB of storage.

After compaction:
- **File Count**: 2,704 files → 3 files (901× reduction)
- **Storage Size**: 3.7 GB → 724 MB (80.4% compression ratio)
- **Query Performance**: Initial scan time 5+ seconds → <0.05 seconds (100× improvement)
- **File Format**: Snappy-compressed small files → ZSTD level 3 optimized files

This isn't synthetic benchmarking—these numbers come from real production workloads. The 80.4% compression ratio reflects ZSTD's superior compression on large blocks (512MB files) versus Snappy on small blocks (2MB files). Larger blocks expose more redundancy, enabling better compression.

The 100× query improvement stems from DuckDB's file opening overhead. Opening 2,704 files requires 2,704 S3 API calls, 2,704 Parquet footer reads, and 2,704 metadata structure constructions. Opening 3 files eliminates 99.9% of this overhead.

### 6.10 Why This Matters for ClickBench

The ClickBench dataset contains 99,997,497 rows across multiple columns. Without compaction, this becomes ~50,000 small Parquet files. With compaction, it consolidates to ~200 optimized files.

The impact:

**Storage**: 13.76 GiB (Arc) vs 67.84 GiB (QuestDB) = **5× smaller**

How? QuestDB uses row-based storage with indexes. Indexes duplicate data (you store the value, plus index entries pointing to it). Arc stores only Parquet files—no indexes, just compressed columns.

**Cold Query Performance**: Arc dominates because:
- Fewer file opens (200 vs 50,000)
- Better compression (ZSTD level 3 on large blocks)
- Sequential reads (time-ordered data)
- Metadata efficiency (one Parquet footer vs thousands)

Compaction transforms Arc from a functional time-series database into a ClickBench leader.

## 7. Performance Deep Dive

### 7.1 Ingestion Performance: 2.42M Records Per Second

Before exploring query performance, Arc's write path deserves examination. The system achieves 2.42M records/second ingestion using columnar MessagePack format—a testament to architectural decisions prioritizing zero-copy data paths.

**Write Performance by Format** (Apple M3 Max, 400 workers, authentication enabled):

| Wire Format | Throughput | p50 Latency | p95 Latency | p99 Latency | Notes |
|-------------|------------|-------------|-------------|-------------|-------|
| **MessagePack Columnar** | **2.42M RPS** | **1.74ms** | **28.13ms** | **45.27ms** | Zero-copy passthrough + auth cache (RECOMMENDED) |
| **MessagePack Row** | **908K RPS** | **136.86ms** | **851.71ms** | **1542ms** | Legacy format with conversion overhead |
| **Line Protocol** | **240K RPS** | N/A | N/A | N/A | InfluxDB compatibility mode |

The columnar format's 2.66× advantage over row format stems from eliminating data transformation. Clients send data already organized as columns (arrays), which Arc passes directly to PyArrow RecordBatch creation, then to Parquet writes. No row-to-column conversion, no flattening of nested structures—just validation and passthrough.

This performance isn't theoretical. Native deployment on Apple M3 Max delivers these numbers with authentication enabled (99.9%+ cache hit rate), realistic batch sizes (1,000 records per request), and concurrent workers (400 workers = ~30× core count for I/O-bound workloads).

### 7.2 The ClickBench Results

ClickBench measures analytical database performance using a real-world dataset (web analytics clicks) and 43 queries of varying complexity. Databases run on identical hardware with default configurations.

Arc's results:

**Hardware: AWS c6a.4xlarge** (16 vCPU AMD EPYC 7R13, 32GB RAM, 500GB gp2)
- **Cold Run Total**: 35.18s (sum of 43 queries, first execution)
- **Hot Run Average**: 0.81s (average per query after caching)
- **Storage**: 500 GP2
- **Success Rate**: 43/43 queries (100%)

**Hardware: Apple M3 Max** (14 cores ARM, 36GB RAM)
- **Cold Run Total**: 22.64s (sum of 43 queries, first execution)
- **With Query Cache**: 16.87s (60s TTL caching enabled, 1.34× speedup)
- **Cache Hit Performance**: 3-20ms per query (sub-second for all cached queries)
- **Storage**: Local NVMe SSD
- **Success Rate**: 43/43 queries (100%)

How did Arc achieve this without custom tuning?

### 7.3 Architecture Enables Performance

**Parquet's Columnar Layout**: Analytical queries typically scan few columns across many rows (e.g., `SELECT AVG(price) FROM clicks`). Parquet stores columns separately—reading `price` doesn't require reading `user_id`, `timestamp`, or other columns. Row-based storage reads entire rows even when only one column matters.

**DuckDB's Vectorized Execution**: DuckDB processes data in batches (vectors), applying operations to thousands of values simultaneously. Modern CPUs excel at this—SIMD instructions operate on multiple values per instruction. Vectorization is why DuckDB outperforms row-at-a-time interpreters.

**Predicate Pushdown**: When queries filter by time (`WHERE timestamp > '2025-10-16'`), DuckDB reads Parquet file metadata, skips files outside the range, scans relevant files, and uses Parquet's column statistics to skip row groups. This multi-level filtering minimizes I/O.

**Compression Benefits**: ZSTD-compressed data is smaller, meaning fewer bytes transfer from storage. For cloud deployments, network transfer dominates latency. Reading 200MB compressed beats reading 500MB uncompressed, even accounting for decompression CPU time.

**No Distributed Overhead**: Distributed databases pay coordination costs—network round-trips for consensus, data shuffling between nodes, partial aggregation then final merging. Arc runs in-process (DuckDB is embedded). Query execution is pure computation without network hops.

### 7.4 Connection Pool Impact

Without connection pooling, each query pays setup costs:

- DuckDB connection creation: ~50ms
- Extension loading (httpfs, aws): ~30ms
- S3 credential configuration: ~10ms

For a 100ms query, this 90ms overhead is 90% of total time.

With pooling, these costs amortize across thousands of queries. Connections persist for the worker's lifetime (hours or days). Setup happens once per connection, not per query.

The pool also provides fairness. High-priority queries (dashboard refreshes) jump ahead of low-priority batch jobs. Without priority queuing, a flood of batch queries could starve interactive users.

### 7.5 Early Connection Return

Consider a query returning 1,000,000 rows:

- Query execution: 200ms
- JSON serialization: 300ms
- Total: 500ms

Naive approach: hold the connection for 500ms.

Arc's approach: return connection after 200ms, serialize while the connection handles other queries. This 2.5× improvement in connection utilization matters under load.

### 7.6 Arrow Format Performance

The ClickBench improvement (28-75%) with Arrow format comes from:

**Zero-Copy**: Arrow's columnar memory layout matches Parquet and DuckDB's internal representation. Converting DuckDB results to Arrow is nearly free—just metadata manipulation. Converting to JSON requires iterating rows and serializing each value.

**Compression**: Arrow IPC streams support LZ4 compression. Large result sets compress well (numeric columns have patterns). JSON is verbose (lots of `[`, `]`, `,`, `"` characters).

**Client Efficiency**: Arrow-aware clients (pandas, polars) read columnar data directly into efficient structures. JSON requires parsing strings, converting types, constructing objects.

For the Superset integration, Arrow format provides smooth interactions—queries feel snappier, large result sets don't lag the browser.

## 8. Operational Considerations

### 8.1 Deployment Patterns

Arc's flexibility enables diverse deployment patterns:

**Single-Node Development**: Local filesystem backend on a laptop. Perfect for testing, development, demo environments. No cloud dependencies, no costs.

**Single-Node Production**: MinIO or S3 backend with one Arc instance. Handles moderate workloads (millions of rows/day) with simple operations. Vertical scaling (bigger instance) provides more query throughput.

**Multi-Worker Production**: Gunicorn with 8-42 workers, S3 backend. High query concurrency, robust compaction, comprehensive monitoring. This is the configuration validated by ClickBench testing.

**Distributed Storage**: Multiple Arc instances writing to the same S3 bucket (different databases). Central S3 provides shared storage, instances handle different data sources. Compaction coordinates via database locks.

### 8.2 Monitoring and Observability

Production systems require visibility. Arc provides multiple monitoring endpoints:

**System Metrics** (`/metrics`): CPU usage, memory consumption, disk I/O, network stats. Collected every 30 seconds, retained for 24 hours.

**Connection Pool Metrics** (`/metrics/query-pool`): Pool size, active/idle connections, queue depth, average wait/execution times. Critical for understanding query performance under load.

**Cache Metrics** (`/cache/stats`): Hit rate, cache size, eviction count. Low hit rates suggest TTL tuning or workload unsuitability for caching.

**Compaction Metrics** (`/compaction/stats`): Jobs completed/failed, files compacted, bytes saved. Tracks storage optimization over time.

**Logs** (`/logs`): Recent application logs filterable by level and time. Structured JSON format integrates with log aggregation systems (ELK, Splunk, Datadog).

### 8.3 Configuration Management

Arc supports three configuration sources (in precedence order):

1. **Environment Variables**: `STORAGE_BACKEND=s3`, `DUCKDB_POOL_SIZE=10`
2. **config.yaml**: Structured configuration for storage, compaction, auth
3. **Defaults**: Sensible fallbacks (local storage, 5-connection pool, hourly compaction)

This flexibility supports different deployment methods:

- Docker: env vars via `docker run -e`
- Kubernetes: ConfigMaps and Secrets
- Systemd: Environment files
- Local development: config.yaml

### 8.4 Security

Arc implements several security layers:

**Authentication**: Token-based API authentication with bcrypt hashing. Tokens stored in SQLite, validated via LRU cache (reduces database load). Admin token generated on first run, rotatable via API.

**Authorization**: Future work—currently, valid tokens grant full access. Planned: role-based access control (read-only vs read-write tokens, database-scoped tokens).

**Network Security**: Arc expects deployment behind reverse proxy (nginx, Traefik) handling TLS termination. API supports CORS for web clients but should restrict origins in production.

**Rate Limiting**: SlowAPI middleware limits request rates per IP (100/minute default, 10/minute for token creation). Prevents brute-force attacks and abusive clients.

## 9. Future Directions

### 9.1 Write Performance Optimization

Current write path buffers 50,000 rows (5-second timeout). For ultra-high throughput (millions of rows/second), this becomes a bottleneck. Potential improvements:

**Parallel Buffers**: Multiple independent buffers per measurement, writing concurrently.

**Direct Arrow Writing**: MessagePack endpoint uses Arrow directly, but line protocol converts to DataFrame first. Native Arrow ingestion could eliminate this conversion.

**Write-Ahead Log**: Optional WAL (currently implemented but not extensively tested) provides durability guarantees at slight latency cost.

### 9.2 Distributed Query Execution

Arc currently runs single-node. For datasets exceeding single-instance capacity (TBs or tens of billions of rows), distributed execution becomes necessary.

Potential approach: Multiple Arc query instances pointing to partitioned S3 prefixes, coordinator aggregates results. DuckDB supports parallel execution natively, simplifying implementation.

### 9.3 Advanced Compaction Strategies

Current compaction uses fixed hourly partitions. More sophisticated strategies could:

**Adaptive Partitioning**: Compact based on data volume, not just time. High-rate measurements (1M rows/hour) might compact daily; low-rate (1K rows/hour) might compact monthly.

**Tiered Compaction**: Multiple compaction levels—first pass combines 10 files to 1, second pass combines 10 of those to 1 more, etc. Reduces total compaction work.

**Incremental Compaction**: Rather than downloading all files, download and merge in streaming fashion. Reduces memory footprint for huge partitions.

### 9.4 Query Optimization

DuckDB handles most optimization, but Arc could add:

**Materialized Views**: Pre-compute common aggregations (daily/hourly rollups). Queries against materialized views return instantly.

**Query Rewriting**: Automatically rewrite expensive queries to use materialized views when applicable.

**Adaptive Caching**: Increase cache TTL for stable data (historical partitions), decrease for recent data (still receiving writes).

### 9.5 Integration Ecosystem

Currently, Arc integrates with Apache Superset (via custom SQLAlchemy dialect). Future integrations:

**Grafana**: Native datasource plugin using Arrow endpoint.

**Jupyter**: Python client library with pandas/polars integration.

**Streaming**: Kafka connector for real-time ingestion.

**Prometheus**: Metrics exporter for Prometheus scraping.

## 10. Conclusion

### 10.1 Validating the Hypothesis

Arc demonstrates that exceptional database performance doesn't require building everything from scratch. By thoughtfully composing proven components—DuckDB, Parquet, Arrow, FastAPI—and adding a sophisticated compaction system, Arc achieves:

- **2.42M records/second ingestion** with columnar MessagePack format
- **5× storage efficiency** compared to purpose-built time-series databases (13.76 GiB vs 67.84 GiB)
- **Leading cold query performance** on analytical workloads (22.64s on M3 Max, 35.18s on c6a.4xlarge)
- **Real-world compaction gains** (2,704 files → 3 files, 901× reduction, 80.4% compression)
- **Near-zero authentication overhead** (99.9%+ cache hit rate)
- **Operational simplicity** with straightforward deployment and monitoring
- **Standards compliance** enabling broad tool integration

The architecture's simplicity is its strength. Each layer—storage, query, API, compaction—focuses on one concern and does it well. This modularity enables testing, optimization, and future enhancement without systemic rewrites.

### 10.2 Community Validation

The market responded quickly to Arc's approach. Within six days of the GitHub repository going public, the project garnered over 200 stars—validating that the developer community recognizes the value of simplicity and composition. The team rapidly developed integrations with Apache Superset and Telegraf, demonstrating ecosystem commitment.

This reception suggests the industry is ready for databases that prioritize understandability over complexity, proven components over custom implementations, and reproducible results over marketing benchmarks.

### 10.3 The Path Forward

Arc's journey—from Wavestream's early experiments through Historian's production lessons to Arc's refined simplicity—demonstrates that database design benefits from iteration and real-world feedback. Each version taught lessons about what matters: not theoretical purity, but practical performance; not feature counts, but operational clarity.

Perhaps most importantly, Arc's design is reproducible. The components are open-source, the techniques are documented, the benchmark results are public. Other teams can build on these ideas, adapt them to their needs, and contribute improvements back. This openness extends Arc's impact beyond a single database—it becomes a template for how to build high-performance systems through thoughtful composition.

### 10.4 Final Thoughts

As data volumes continue growing and analytical workloads become ubiquitous, Arc's approach—simplicity over complexity, composition over invention, standards over proprietary formats—offers a path forward. The time-series database space doesn't need another custom storage engine or distributed consensus protocol. It needs systems operators can understand, debug, and trust.

Arc aims to be that system. The numbers validate the approach. The architecture invites inspection. The code awaits contributions. And the journey continues.

---

## Appendix A: Codebase Statistics

**Total Lines of Code Analyzed**: ~5,500 lines across core modules

**Key Files and Line Counts**:
- `api/main.py`: 1,903 lines (API layer, startup/shutdown, endpoints)
- `api/duckdb_engine.py`: 1,107 lines (query engine, SQL rewriting, custom commands)
- `api/duckdb_pool.py`: 711 lines (connection pooling, priority queuing)
- `storage/compaction.py`: 684 lines (compaction jobs, manager, multi-database)
- `storage/local_backend.py`: 220 lines (local filesystem storage)
- `storage/minio_backend.py`: 257 lines (MinIO/S3-compatible storage)

**Language Distribution**:
- Python: 100% (application code)
- SQL: Embedded (DuckDB queries, schema)
- YAML: Configuration files
- Markdown: Documentation

**Dependencies** (key libraries):
- FastAPI: Web framework
- DuckDB: Query engine
- PyArrow: Arrow format support
- boto3: S3 client
- aiofiles: Async file I/O

## Appendix B: ClickBench Query Examples

**Q1 - Simple Aggregation**:
```sql
SELECT COUNT(*) FROM hits
```
Arc: 0.115s, QuestDB: 0.141s (Arc 23% faster)

**Q8 - Complex Aggregation**:
```sql
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)
FROM hits
```
Arc: 0.494s, QuestDB: 2.456s (Arc 5× faster)

**Q32 - Large Result Set**:
```sql
SELECT SearchPhrase, COUNT(*) AS c
FROM hits
WHERE SearchPhrase != ''
GROUP BY SearchPhrase
ORDER BY c DESC
LIMIT 10
```
Arc: 2.805s, QuestDB: 5.545s (Arc 2× faster)

## Appendix C: Configuration Reference

**Storage Backend Configuration** (config.yaml):
```yaml
storage:
  backend: s3  # local, minio, s3, gcs, ceph
  s3:
    bucket: analytics-data
    region: us-east-1
    database: production
```

**Compaction Configuration**:
```yaml
compaction:
  enabled: true
  schedule: "5 * * * *"
  min_age_hours: 1
  min_files: 10
  target_file_size_mb: 512
  max_concurrent_jobs: 2
```

**Query Engine Configuration** (arc.conf and environment variables):
```toml
[duckdb]
pool_size = 5                        # Connection pool size per worker
max_queue_size = 100                 # Query overflow queue depth
memory_limit = "8GB"                 # Per-connection memory limit
threads = 14                         # CPU threads per query
temp_directory = "./data/duckdb_tmp" # Disk spill location
enable_object_cache = true           # Parquet metadata cache (2-10× speedup)

[query_cache]
enabled = true                       # Enable query result caching
ttl_seconds = 60                     # Cache TTL in seconds
```

Environment variables (override arc.conf):
```bash
DUCKDB_POOL_SIZE=5
DUCKDB_MAX_QUEUE_SIZE=100
DUCKDB_MEMORY_LIMIT="8GB"
DUCKDB_THREADS=14
DUCKDB_TEMP_DIRECTORY="./data/duckdb_tmp"
DUCKDB_ENABLE_OBJECT_CACHE=true
QUERY_CACHE_ENABLED=true
QUERY_CACHE_TTL=60
```

**Write Buffer Configuration**:
```bash
WRITE_BUFFER_SIZE=50000
WRITE_BUFFER_AGE=5
WRITE_COMPRESSION=snappy
```

---

**Document Version**: 1.2
**Last Updated**: October 19, 2025
**License**: AGPL-3.0
**Contact**: support@basekick.net

**Changelog**:
- v1.2 (Oct 19, 2025): Added comprehensive DuckDB production tuning section covering memory management, CPU parallelism, object caching, and configuration strategies for production deployments
- v1.1 (Oct 17, 2025): Added ingestion performance benchmarks, authentication caching details, storage backend comparison, real-world compaction results, origin story, and community validation
- v1.0 (Oct 16, 2025): Initial architecture paper based on codebase review
