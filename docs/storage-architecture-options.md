# Storage Architecture Options: Performance vs Scalability

**Date**: 2025-10-10
**Context**: MinIO S3 storage shows 4.2x slower performance vs local NVMe and causes system instability (36GB swap usage, crashes). Need to evaluate architectural options for production deployments.

---

## Current Performance Gap

| Storage Backend | ClickBench Time | Throughput | Stability | Scalability |
|----------------|----------------|------------|-----------|-------------|
| **Local NVMe** | 22.64s | 4.4M rows/sec | Stable | Limited by disk size |
| **MinIO S3** | 94s+ | ~1.1M rows/sec | Crashes (36GB swap) | Unlimited |

**Performance ratio**: 4.2x slower with MinIO
**Critical issue**: Memory exhaustion causes system crashes

---

## Root Cause Analysis

### Why MinIO Performs Poorly

1. **Network Layer Overhead**
   - Even local MinIO adds HTTP/S3 protocol overhead
   - Each query requires multiple HTTP requests (metadata + data chunks)
   - TCP/IP stack adds latency vs direct file I/O

2. **Memory Management Issues**
   - 36GB swap usage suggests excessive buffering
   - Possible causes:
     - DuckDB buffering entire S3 objects before processing
     - MinIO client buffering on Arc side
     - Concurrent queries (42 workers) × S3 buffers = memory explosion
     - Lack of streaming/chunked reads

3. **DuckDB S3 Extension Behavior**
   - May not be optimized for MinIO's S3 implementation
   - Could be doing full file downloads vs range requests
   - Metadata overhead for Parquet files (reading footers, column chunks)

4. **Concurrency Bottlenecks**
   - 42 workers × S3 connections = connection pool exhaustion
   - MinIO connection limits may be throttling requests
   - Lock contention in MinIO server with high concurrency

### Questions to Answer

- [ ] What is MinIO's current configuration? (connection limits, buffer sizes, etc.)
- [ ] Is MinIO running locally or remote? Network bandwidth?
- [ ] What DuckDB S3 extension settings are active? (`SET s3_*` variables)
- [ ] Memory profiling: Where are the 36GB being allocated?
- [ ] What's the network traffic pattern during queries? (packet capture)
- [ ] Does MinIO show errors/throttling in logs during benchmark?

---

## Option 1: Optimize MinIO Configuration

**Goal**: Get MinIO performance to acceptable levels (target: 2x slower vs local, not 4.2x)

### MinIO Server Tuning

```bash
# Increase MinIO API worker threads
export MINIO_API_REQUESTS_MAX=1000
export MINIO_API_REQUESTS_DEADLINE=10s

# Optimize memory settings
export MINIO_CACHE_SIZE=8GB
export MINIO_CACHE_EXCLUDE="*.parquet"  # Don't cache, stream directly

# Connection pool settings
export MINIO_CONN_READ_DEADLINE=5m
export MINIO_CONN_WRITE_DEADLINE=5m
```

### DuckDB S3 Extension Tuning

```sql
-- In Arc's DuckDB connection initialization
SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='localhost:9000';
SET s3_use_ssl=false;

-- Critical performance settings
SET s3_uploader_max_parts_per_file=10000;
SET s3_uploader_thread_limit=50;

-- Connection pooling
SET s3_max_connections=100;  -- Match or exceed worker count

-- Memory/caching
SET enable_http_metadata_cache=true;
SET http_timeout=30000;  -- 30 seconds
```

### Arc Application Changes

1. **Limit DuckDB worker concurrency** when using S3:
   ```python
   # In duckdb_pool.py
   if storage_backend == "s3":
       max_workers = min(cpu_count(), 14)  # Cap at 14 for S3
   else:
       max_workers = cpu_count() * 3  # 42 for local
   ```

2. **Implement connection pooling** for S3 clients:
   ```python
   # Reuse S3 connections across queries
   s3_client_pool = {}
   ```

3. **Add query hints** for S3-backed tables:
   ```sql
   -- Force streaming instead of materialization
   PRAGMA enable_object_cache=false;
   ```

### Testing Plan

1. Baseline MinIO with default settings
2. Apply MinIO server tuning → measure
3. Apply DuckDB S3 settings → measure
4. Reduce Arc worker count → measure
5. Memory profile each configuration
6. Identify bottleneck (CPU, memory, network, MinIO)

**Success criteria**: < 45s ClickBench time (< 2x slowdown), no swap usage

---

## Option 2: Alternative Distributed Storage

If MinIO can't be optimized, consider alternatives with better DuckDB integration.

### 2A: Cloud Object Storage (S3/GCS/Azure)

**Pros**:
- Native DuckDB support with extensive optimization
- Proven at scale (DuckDB team tests against S3)
- No infrastructure to manage
- Pay-per-use pricing

**Cons**:
- Vendor lock-in
- Data egress costs can be high
- Network latency (cross-region)
- Requires cloud deployment

**DuckDB S3 Performance**:
- DuckDB team reports < 2x slowdown vs local for analytical queries
- Intelligent prefetching and range requests
- Metadata caching reduces round trips

**Cost Analysis** (example for 1TB dataset):
```
S3 Storage:     $23/month
S3 Requests:    ~$5/month (1M GET requests)
Data Transfer:  $90/TB egress

MinIO (self-hosted):
Storage:        $50/month (2TB SSD)
Compute:        $50/month (VM)
Total:          $100/month (no egress fees)
```

### 2B: Distributed Filesystems

#### Ceph (Open Source)

**Pros**:
- Self-hosted (no cloud dependency)
- Object + block + filesystem interfaces
- Production-proven (OpenStack, etc.)
- S3-compatible API

**Cons**:
- Complex setup and operations
- Requires 3+ nodes for redundancy
- Resource-intensive

**Performance**: Similar to MinIO (both use S3 API)

#### SeaweedFS

**Pros**:
- Simpler than Ceph
- Optimized for small files and large files
- S3-compatible + native filer
- Lower resource requirements

**Cons**:
- Less mature than Ceph or MinIO
- Smaller community
- Limited DuckDB testing

**Potential**: Worth testing, may have better S3 implementation

#### GlusterFS

**Pros**:
- POSIX filesystem (could mount as local path)
- Simpler than Ceph
- Good for large files (Parquet)

**Cons**:
- Not object storage (different paradigm)
- DuckDB would need local mount (not S3 API)
- Network filesystem overhead still present

### 2C: Lakehouse Formats (Delta Lake / Apache Iceberg)

**Concept**: Add metadata layer on top of object storage

**Pros**:
- Better metadata management (reduce S3 requests)
- Transaction support (ACID)
- Time travel, versioning
- Partition pruning optimizations
- DuckDB has native support

**Cons**:
- Adds complexity to Arc
- Write path changes (need to write Delta/Iceberg format)
- May not solve underlying S3 performance issues

**DuckDB Integration**:
```sql
-- Read Delta Lake directly
SELECT * FROM delta_scan('s3://bucket/table');

-- Read Iceberg
SELECT * FROM iceberg_scan('s3://bucket/table');
```

**Performance**: Fewer S3 requests due to metadata layer, could reduce latency by 20-30%

---

## Option 3: Hybrid Tiered Storage

**Concept**: Combine local NVMe (hot tier) + distributed storage (cold tier)

### 3A: Query Routing by Data Temperature

```
Recent data (last 7 days):  Local NVMe (fast queries)
Historical data (> 7 days): MinIO/S3 (slower, acceptable)
```

**Implementation**:
1. Arc partitions data by time on write
2. Query analyzer determines which partitions needed
3. Route to appropriate storage backend
4. DuckDB can query both in single query (federation)

**Example**:
```sql
-- Query spans hot + cold data
SELECT * FROM local.hits_recent
UNION ALL
SELECT * FROM s3.hits_archive
WHERE EventDate > '2025-09-01'
```

**Pros**:
- Best of both worlds (performance + scalability)
- Most queries hit recent data (fast)
- Historical data available (slower but acceptable)

**Cons**:
- Complexity in query routing
- Need data lifecycle management (migration local → S3)
- Cross-tier queries are slower

### 3B: Local Caching Layer

**Concept**: Use local NVMe as cache for frequently accessed S3 data

```
Query Request
    ↓
Check Local Cache
    ↓ (miss)
Fetch from S3
    ↓
Store in Local Cache (LRU eviction)
    ↓
Return Results
```

**Implementation Options**:

1. **DuckDB Level**: Use DuckDB's object cache
   ```sql
   SET enable_object_cache=true;
   SET object_cache_size='100GB';
   ```

2. **Application Level**: Arc caches Parquet files
   ```python
   # In Arc: Download S3 Parquet to /tmp, query locally
   if not file_in_cache(s3_path):
       download_to_cache(s3_path, local_path)
   query_local(local_path)
   ```

3. **Filesystem Level**: S3FS with local cache
   ```bash
   # Mount S3 with local cache
   s3fs bucket /mnt/data -o use_cache=/tmp/s3cache
   ```

**Pros**:
- Transparent to queries
- Frequently accessed data is fast
- Simple architecture (still single storage backend)

**Cons**:
- Cache warmup period (first queries slow)
- Cache invalidation complexity
- Limited by local disk size
- Cache misses still slow

### 3C: Data Locality Scheduling

**Concept**: Co-locate compute with data (move query to data, not data to query)

```
Data Partition A → Arc Worker 1 (has local copy)
Data Partition B → Arc Worker 2 (has local copy)
Data Partition C → Arc Worker 3 (has local copy)
```

**Architecture**:
- Each Arc worker has local NVMe with subset of data
- Query coordinator routes sub-queries to workers with local data
- Workers return results, coordinator aggregates

**Pros**:
- Eliminates network transfer for most queries
- Scales horizontally (add workers = add capacity)
- Each worker is fast (local NVMe)

**Cons**:
- Major architectural change (Arc becomes distributed)
- Data replication/sharding complexity
- Worker failures require data rebalancing
- Query coordinator becomes bottleneck

**Similar to**: ClickHouse, Druid (distributed columnar databases)

---

## Option 4: Accept Trade-off & Position Differently

**Strategy**: Offer two deployment modes with clear performance characteristics

### Deployment Mode 1: High Performance

**Configuration**:
- Local NVMe storage
- Single-node or sharded across nodes with local disks
- 42 workers (3x CPU cores)

**Performance**:
- ClickBench: 22.64s
- Throughput: 4.4M rows/sec
- Latency: P95 < 100ms

**Use Cases**:
- Real-time dashboards
- Low-latency analytics
- High query throughput
- Smaller datasets (< 10TB)

**Limitations**:
- Storage capacity limited by local disks
- Scaling requires adding nodes
- Data durability depends on RAID/backups

### Deployment Mode 2: Scalable

**Configuration**:
- MinIO/S3 storage
- Compute-storage separation
- 14 workers (1x CPU cores, reduced for S3)

**Performance**:
- ClickBench: 45-60s (target with optimizations)
- Throughput: 1.7-2.2M rows/sec
- Latency: P95 < 500ms

**Use Cases**:
- Large datasets (> 10TB)
- Infrequent queries (batch analytics)
- Cost-sensitive deployments
- Long-term data retention

**Benefits**:
- Unlimited storage capacity
- Independent compute/storage scaling
- Lower cost per GB stored
- Geographic distribution

### Marketing Position

**Documentation**:
```markdown
## Deployment Modes

Arc supports two deployment modes optimized for different use cases:

### High-Performance Mode (Recommended for < 10TB)
- **Storage**: Local NVMe SSD
- **Performance**: 4.4M rows/sec, sub-100ms queries
- **Best for**: Real-time dashboards, high-concurrency workloads

### Scalable Mode (Recommended for > 10TB)
- **Storage**: S3-compatible object storage (MinIO, S3, GCS)
- **Performance**: 1.7-2.2M rows/sec, sub-500ms queries
- **Best for**: Large-scale data lakes, cost-sensitive deployments
```

**Pricing Tiers**:
- **Performance Tier**: $X/month per node (includes local NVMe)
- **Scalable Tier**: $Y/month per node + storage costs

---

## Recommendation Matrix

| Priority | Recommended Option | Rationale |
|----------|-------------------|-----------|
| **Quick Win** | Option 1: Optimize MinIO | Low risk, could get 2x improvement |
| **Best Performance** | Option 3A: Hybrid Tiered | 90% of queries fast, 10% acceptable |
| **Simplest** | Option 4: Accept Trade-off | No architectural changes, clear positioning |
| **Most Scalable** | Option 2A: Cloud S3 | Proven at scale, DuckDB optimized |
| **Most Flexible** | Option 3B: Local Caching | Transparent, adapts to workload |

---

## Next Steps

### Week 1: Investigation & Quick Wins
1. **Profile MinIO performance**
   - Memory profiling (where is 36GB going?)
   - Network analysis (bandwidth, latency, request patterns)
   - MinIO server logs (errors, throttling)
   - DuckDB S3 extension logs

2. **Apply MinIO optimizations** (Option 1)
   - Tune MinIO server settings
   - Configure DuckDB S3 extension
   - Reduce Arc worker count for S3
   - Benchmark and measure improvement

3. **Document baseline metrics**
   - Cost per GB (NVMe vs MinIO vs S3)
   - Performance across different query patterns
   - Scalability limits (max dataset size)

### Week 2: Prototype Alternatives
1. **Test cloud S3** (Option 2A)
   - Deploy Arc pointing to AWS S3
   - Run ClickBench, compare performance
   - Calculate cost at scale (1TB, 10TB, 100TB)

2. **Prototype hybrid tiered storage** (Option 3A)
   - Implement time-based partitioning
   - Query router for hot/cold data
   - Measure performance on mixed workload

3. **Build decision matrix**
   - Performance requirements (P50, P95, P99 latencies)
   - Cost requirements ($/GB stored, $/query)
   - Scalability requirements (max dataset size)
   - Operational complexity (deployment, maintenance)

### Week 3: Architecture Decision
1. **Review findings** with stakeholders
2. **Select primary architecture** for production
3. **Plan migration path** (if changing from MinIO)
4. **Update roadmap** with implementation timeline

---

## Open Questions

1. **Customer Requirements**
   - What's acceptable query latency? (100ms, 500ms, 1s?)
   - What's typical dataset size? (1TB, 10TB, 100TB?)
   - What's query pattern? (real-time dashboards vs batch analytics)
   - What's budget? (performance vs cost trade-off)

2. **Technical Constraints**
   - Can we deploy to cloud? (AWS, GCP, Azure)
   - Can we change storage format? (Parquet vs Delta/Iceberg)
   - Can we change Arc architecture? (single-node vs distributed)

3. **Business Strategy**
   - Is "separation of compute and storage" core value prop?
   - Or is "fastest time-series database" the positioning?
   - Can we offer both modes? (performance + scalable tiers)

---

## Appendix: Performance Testing Checklist

### MinIO Optimization Testing

```bash
# Baseline test
./benchmarks/clickbench/run_benchmark.py --storage=minio --cache=false

# Test 1: MinIO server tuning
export MINIO_API_REQUESTS_MAX=1000
# restart MinIO, re-run benchmark

# Test 2: DuckDB S3 settings
# Update arc/api/duckdb_pool.py with s3_* settings
# Re-run benchmark

# Test 3: Reduced workers
# Set workers=14 in arc config
# Re-run benchmark

# Test 4: Memory profiling
memory_profiler python -m arc.api.main &
# Run benchmark
# Analyze memory usage
```

### Alternative Storage Testing

```bash
# AWS S3 test
export S3_ENDPOINT=s3.amazonaws.com
export S3_BUCKET=arc-benchmark-data
./benchmarks/clickbench/run_benchmark.py --storage=s3

# SeaweedFS test
docker run -p 8333:8333 chrislusf/seaweedfs server -s3
# Upload data, run benchmark

# Delta Lake test
# Convert Parquet to Delta format
# Update Arc to use delta_scan()
# Run benchmark
```

### Metrics to Collect

| Metric | Local NVMe | MinIO | S3 | Target |
|--------|-----------|-------|-----|--------|
| ClickBench time (cold) | 22.64s | ? | ? | < 45s |
| Memory usage (peak) | 8GB | 36GB+ | ? | < 16GB |
| Network I/O | 0 | ? | ? | N/A |
| Storage cost (1TB) | $200 | $50 | $23 | N/A |
| Query latency P50 | 40ms | ? | ? | < 100ms |
| Query latency P95 | 80ms | ? | ? | < 200ms |
| Query latency P99 | 120ms | ? | ? | < 500ms |

---

**Document prepared for**: Architecture review meeting
**Owner**: Engineering team
**Review date**: Week of 2025-10-14
