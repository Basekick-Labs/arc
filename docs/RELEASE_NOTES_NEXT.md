# Release Notes - v25.12.1 (Draft)

> **Note**: This file tracks improvements for the upcoming release. Do not commit this file.
>
> **Target Release**: December 2025 (v25.12.1)
> **Fallback**: v25.11.2 if critical bugs are found

## 🚀 New Features

### File-Level Partition Pruning

Arc now includes intelligent partition pruning that dramatically accelerates queries with time filters. Instead of scanning all files using expensive glob patterns, the query engine extracts time ranges from WHERE clauses and generates targeted file paths to read only the partitions that contain relevant data.

**How it works**:
- Automatically extracts time ranges from WHERE clauses (e.g., `time >= '2025-11-10' AND time < '2025-11-16'`)
- Generates specific hour and day partition paths instead of recursive `/**/*.parquet` globs
- Seamlessly handles both hourly and daily compacted files
- Automatically falls back to full scan when no time filter is present
- Filters out non-existent paths for graceful handling of sparse data

**Expected performance improvements**:
- Queries with time filters execute significantly faster
- Reduced file I/O and faster query response times
- Particularly effective for queries over large time ranges with selective filters

**Available in**: Query operations, DELETE operations, continuous queries, and retention policies

Files: [api/partition_pruner.py](api/partition_pruner.py), [api/duckdb_engine.py](api/duckdb_engine.py), [api/delete_routes.py](api/delete_routes.py), [api/continuous_query_routes.py](api/continuous_query_routes.py), [api/retention_routes.py](api/retention_routes.py)

---

### Statistics-Based File Skipping

Arc now leverages Parquet file metadata statistics to skip files that don't match query time ranges. This provides additional query acceleration on top of partition pruning by using lightweight metadata checks to determine which files can be safely skipped without reading their contents.

**How it works**:
- Reads min/max time values from Parquet file metadata
- Skips files where time ranges don't overlap with the query
- Only accesses lightweight metadata - no data downloaded
- Works seamlessly with both regular and compacted files
- All files written by Arc automatically include statistics metadata

**Expected performance improvements**:
- Additional query acceleration when combined with partition pruning
- Reduced file I/O for queries with selective time filters
- Faster query response times across all query patterns

Files: [api/parquet_stats_filter.py](api/parquet_stats_filter.py), [api/partition_pruner.py](api/partition_pruner.py)

---

### Cloud Storage Support for DELETE Operations

DELETE operations now support cloud storage backends including S3, MinIO, GCS, and Ceph. This extends Arc's data deletion capabilities beyond local storage to all supported storage backends.

**How it works**:
- Automatically detects storage backend type (local vs cloud)
- Downloads affected Parquet files for filtering
- Applies WHERE clause filter using DuckDB
- Uploads filtered files back to cloud storage
- Atomically replaces or deletes files based on results

**Supported backends**:
- Local filesystem
- Amazon S3
- MinIO
- Google Cloud Storage (GCS)
- Ceph Object Storage

**DELETE with partition pruning**:
DELETE operations now use the same partition pruning optimization as queries. When time filters are present in WHERE clauses, only relevant partitions are scanned instead of all files.

**Performance impact**:
- Faster file discovery for DELETE operations with time filters
- Reduced cloud storage API calls
- Efficient handling of large datasets across all storage backends

**Example**:
```bash
POST /api/v1/delete
{
  "database": "production",
  "measurement": "logs",
  "where": "time < '2025-01-01' AND severity = 'DEBUG'",
  "dry_run": false,
  "confirm": true
}
```

**Safety features**:
- Dry-run mode for testing
- Confirmation required for large deletes (>10K rows)
- Maximum rows per delete limit (1M rows default)
- SQL injection protection
- Physical deletion (data actually removed)

Files: [api/delete_routes.py](api/delete_routes.py)

---

### Partition Pruning for Continuous Queries

Continuous queries now leverage partition pruning for dramatically faster aggregation execution. When continuous queries include time filters in their SQL, Arc automatically uses targeted partition scanning instead of scanning all files.

**How it works**:
- Continuous query SQL templates use `{start_time}` and `{end_time}` placeholders
- Query engine automatically detects time ranges and applies partition pruning
- Only scans partitions within the aggregation time window
- Works transparently - no changes needed to existing continuous query definitions

**Expected performance improvements**:
- Faster continuous query execution on large datasets
- Reduced I/O when processing time-ranged aggregations
- More efficient resource utilization for scheduled aggregations

**Example**:
```sql
SELECT
  time_bucket(INTERVAL '1 hour', time) as time,
  host,
  AVG(usage_user) as usage_user_avg
FROM cpu
WHERE time >= {start_time} AND time < {end_time}
GROUP BY 1, 2
```

Files: [api/continuous_query_routes.py](api/continuous_query_routes.py)

---

### Partition Pruning for Retention Policies

Retention policies now use partition pruning to efficiently identify and delete old data. Instead of scanning all files, Arc only examines partitions within the retention time range.

**How it works**:
- Generates partition paths from 2015 (or configured start) to retention cutoff date
- Only scans files that could potentially contain old data
- Checks file metadata to confirm all rows are older than cutoff
- Deletes entire files where appropriate (no partial rewrites needed)

**Cloud storage support**:
- Full support for S3, MinIO, GCS, and Ceph retention policies
- Uses partition pruning for efficient cloud file listing
- Minimizes cloud storage API calls
- Downloads only metadata for most files

**Expected performance improvements**:
- Faster retention policy execution on large datasets
- Reduced file scanning for retention checks
- Efficient cloud storage retention enforcement

**Example**:
```bash
POST /api/v1/retention/1/execute
{
  "dry_run": false,
  "confirm": true
}
```

Files: [api/retention_routes.py](api/retention_routes.py)

---

## 🔧 Technical Details

### New Files
- `api/partition_pruner.py` - Core partition pruning engine (shared by queries and deletes)
- `api/parquet_stats_filter.py` - Statistics-based file filtering

### Modified Files
- `api/duckdb_engine.py` - Integrated partition pruner with query execution
- `api/partition_pruner.py` - Added statistics filtering integration
- `api/delete_routes.py` - Added partition pruning and cloud storage support for DELETE operations
- `api/continuous_query_routes.py` - Integrated partition pruning for continuous query execution
- `api/retention_routes.py` - Added partition pruning and cloud storage support for retention policies

---

## 🐛 Bug Fixes

### CTE (Common Table Expression) Support

Fixed a critical bug where CTEs (Common Table Expressions) were incorrectly treated as physical tables, causing "No files found" errors when using `WITH ... AS` clauses in queries.

**What was broken**:
- Queries using CTEs would fail with "No files found that match the pattern"
- Arc's query rewriter attempted to convert CTE names to Parquet file paths
- Example: `WITH campaign AS (...) SELECT * FROM campaign` would try to find `/data/arc/default/campaign/**/*.parquet`

**What's fixed**:
- Query engine now detects and preserves CTE names during SQL rewriting
- Supports multiple CTEs in a single query
- Supports both `WITH cte_name AS (...)` and `WITH RECURSIVE cte_name AS (...)` syntax
- Works with CTEs in FROM and JOIN clauses

**Additionally fixed**:
- Database and table names starting with digits now work correctly (e.g., `3am.logs`)
- JOIN clauses with database.table syntax now properly resolved (e.g., `LEFT JOIN prod.metrics`)

**Example queries that now work**:
```sql
-- CTE with CROSS JOIN
WITH campaign AS (
  SELECT time, event_type
  FROM prod.events
  WHERE event_type = 'marketing_campaign_started'
  LIMIT 1
)
SELECT l.time, l.service, l.level, camp.event_type
FROM campaign camp
CROSS JOIN prod.logs l
WHERE l.time BETWEEN camp.time AND camp.time + INTERVAL '30 minutes';

-- Database names starting with digits
SELECT * FROM 3am.logs WHERE time > NOW() - INTERVAL '1 hour';
```

Files: [api/duckdb_engine.py](api/duckdb_engine.py)

---

### Parallel Compaction Across Measurements

Fixed a performance bottleneck where compaction jobs ran sequentially despite async code and `max_concurrent` setting, limiting throughput to one partition at a time.

**What was broken**:
- Global storage backend lock serialized ALL compaction jobs
- Only 1 partition could compact at a time, regardless of measurement type
- `max_concurrent=2` setting was ineffective - always sequential
- Total compaction time = sum of all individual job durations
- Example: cpu, memory, and logs compaction took 30 seconds (10s each, sequential)

**What's fixed**:
- Per-measurement locking allows different measurements to compact in parallel
- Jobs for same measurement still serialized (prevents storage backend race conditions)
- `max_concurrent` setting now effective - enables true parallelization
- Example: cpu, memory, and logs now compact simultaneously in 10 seconds

**Performance improvements**:
- With 2 measurements + max_concurrent=2: **~2x faster**
- With 4 measurements + max_concurrent=4: **~4x faster**
- With 10 measurements + max_concurrent=10: **~10x faster**
- Actual speedup depends on measurement count and partition distribution

**How it works**:
- Each `(database, measurement)` tuple gets its own lock
- Different measurements (cpu, memory, logs) can compact simultaneously
- Same measurement partitions wait for each other (safe database property access)
- Logs show `⚡ Parallel compaction: N measurements running concurrently` when active

**Example timeline**:

Before (sequential - 30 seconds):
```
0-10s:  cpu compaction     (memory and logs waiting)
10-20s: memory compaction  (logs waiting)
20-30s: logs compaction
```

After (parallel - 10 seconds):
```
0-10s: cpu + memory + logs all compact simultaneously ⚡
```

**Implementation details**:
- `_measurement_locks` dictionary stores per-measurement locks
- `_get_measurement_lock(database, measurement)` creates/retrieves locks on-demand
- Locks are cached for the lifetime of the compaction manager
- Thread-safe lock creation using `_locks_lock` guard

Files: [storage/compaction.py](storage/compaction.py)

---

## ⚠️ Breaking Changes

None

---

## 🎯 Upgrade Notes

No special upgrade steps required. Changes are backward compatible.

**Recommendations**:
- Consider adding time filters to queries for better performance
- Consider adding time filters to DELETE operations for faster execution
- Continuous queries automatically benefit from partition pruning - no changes needed
- Retention policies now support cloud storage - test with dry_run before execution
- Monitor query performance metrics after upgrade
- Check logs to see which queries, deletes, and retention policies benefit from partition pruning
- If using cloud storage, DELETE and retention operations now work with S3/MinIO/GCS/Ceph
- Test DELETE operations with dry_run=true before executing on production data

---

## 🙏 Credits

- Implementation: Claude Code + Arc team
- Production testing and validation: Arc users

---

**Target Version**: v25.12.1
**Target Release Date**: December 2025
**Status**: In Development
