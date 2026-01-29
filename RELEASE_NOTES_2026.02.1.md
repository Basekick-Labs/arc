# Arc 2026.02.1 Release Notes

## New Features

### InfluxDB Client Compatibility

Arc's Line Protocol endpoints now use the same paths as InfluxDB, enabling drop-in compatibility with all official InfluxDB client libraries.

**Endpoint changes:**
| Old Path | New Path | Compatible With |
|----------|----------|-----------------|
| `/api/v1/write` | `/write` | InfluxDB 1.x clients, Telegraf |
| `/api/v1/write/influxdb` | `/api/v2/write` | InfluxDB 2.x clients |

**Supported clients (no code changes required):**
- Go: `github.com/influxdata/influxdb-client-go`
- Python: `influxdb-client`
- JavaScript/Node.js: `@influxdata/influxdb-client`
- Java: `influxdb-client-java`
- C#: `InfluxDB.Client`
- PHP, Ruby, and other official InfluxDB clients
- Telegraf (InfluxDB output plugin)
- Node-RED: `node-red-contrib-influxdb`

**Usage:** Point your existing InfluxDB client at Arc's URL - it just works.

```python
# Python example - works unchanged
from influxdb_client import InfluxDBClient

client = InfluxDBClient(url="http://localhost:8000", token="your-token", org="myorg")
write_api = client.write_api()
write_api.write(bucket="mydb", record="cpu,host=server01 usage=90.5")
```

**Authentication methods supported:**
| Method | Example | Notes |
|--------|---------|-------|
| Bearer token | `Authorization: Bearer <token>` | Standard OAuth2 style |
| Token header | `Authorization: Token <token>` | InfluxDB 2.x style |
| API key header | `x-api-key: <token>` | Simple header auth |
| Query parameter | `?p=<token>` | InfluxDB 1.x compatibility (username `u=` is ignored) |

```bash
# InfluxDB 1.x style auth with token in password parameter
curl -X POST "http://localhost:8000/write?db=mydb&u=ignored&p=your-token" \
  -d 'cpu,host=server01 usage=90.5'
```

**Arc-native endpoint preserved:**
- `/api/v1/write/line-protocol` - Uses `x-arc-database` header (no query params)

### MQTT Ingestion Support

Arc now supports native MQTT subscription for IoT and edge data ingestion. Connect directly to MQTT brokers to ingest time-series data without requiring additional infrastructure.

**Key features:**
- Subscribe to multiple MQTT topics with wildcard support (`+`, `#`)
- Dynamic subscription management via REST API
- TLS/SSL connections with certificate validation
- Authentication via username/password or client certificates
- Connection auto-reconnect with exponential backoff
- Per-subscription statistics and monitoring
- Passwords encrypted at rest
- Auto-start subscriptions on server restart

**Enable MQTT in arc.toml:**
```toml
[mqtt]
enabled = true
```

**REST API for subscription management:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/mqtt/subscriptions` | Create a new subscription |
| `GET` | `/api/v1/mqtt/subscriptions` | List all subscriptions |
| `GET` | `/api/v1/mqtt/subscriptions/:id` | Get subscription details |
| `PUT` | `/api/v1/mqtt/subscriptions/:id` | Update a subscription |
| `DELETE` | `/api/v1/mqtt/subscriptions/:id` | Delete a subscription |
| `POST` | `/api/v1/mqtt/subscriptions/:id/start` | Start a subscription |
| `POST` | `/api/v1/mqtt/subscriptions/:id/stop` | Stop a subscription |
| `POST` | `/api/v1/mqtt/subscriptions/:id/restart` | Restart a subscription |
| `GET` | `/api/v1/mqtt/subscriptions/:id/stats` | Get subscription statistics |
| `GET` | `/api/v1/mqtt/stats` | Aggregate stats for all subscriptions |
| `GET` | `/api/v1/mqtt/health` | MQTT service health check |

**Example - Create subscription via API:**
```bash
curl -X POST "http://localhost:8000/api/v1/mqtt/subscriptions" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "factory-sensors",
    "broker": "tcp://mqtt.example.com:1883",
    "topics": ["sensors/+/temperature", "sensors/+/humidity"],
    "qos": 1,
    "database": "iot",
    "username": "arc",
    "password": "secret",
    "auto_start": true,
    "topic_mapping": {
      "sensors/+/temperature": "temperature",
      "sensors/+/humidity": "humidity"
    }
  }'
```

**Example - List subscriptions:**
```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/mqtt/subscriptions
```

**Subscription options:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique subscription name |
| `broker` | string | Yes | Broker URL (tcp://, ssl://, ws://) |
| `topics` | array | Yes | Topics to subscribe (supports wildcards) |
| `database` | string | Yes | Target Arc database |
| `qos` | int | No | QoS level: 0, 1, or 2 (default: 1) |
| `client_id` | string | No | MQTT client ID (auto-generated if not set) |
| `username` | string | No | MQTT username |
| `password` | string | No | MQTT password (encrypted at rest) |
| `tls_enabled` | bool | No | Enable TLS/SSL |
| `auto_start` | bool | No | Start on creation and server restart |
| `topic_mapping` | object | No | Map topics to measurement names |

### S3 File Caching via cache_httpfs Extension (PR #149)

Arc now supports optional in-memory caching of S3 Parquet files via DuckDB's `cache_httpfs` community extension. This can significantly improve query performance (5-10x) for workloads with repeated file access.

**When this helps:**
- CTEs (Common Table Expressions) that read the same table multiple times
- Subqueries accessing the same data
- Grafana dashboards with multiple panels querying similar time ranges

**Configuration:**
```toml
[query]
# Enable S3 file caching (disabled by default)
enable_s3_cache = true

# Cache size (default: 128MB)
s3_cache_size = "256MB"

# Cache TTL in seconds (default: 3600 = 1 hour)
s3_cache_ttl_seconds = 3600
```

**Environment variables:**
- `ARC_QUERY_ENABLE_S3_CACHE` - Enable/disable caching
- `ARC_QUERY_S3_CACHE_SIZE` - Cache size (e.g., "128MB", "256MB")
- `ARC_QUERY_S3_CACHE_TTL_SECONDS` - TTL in seconds

**Key features:**
- **In-memory only** - No disk caching, preserves Arc's stateless compute philosophy
- **Opt-in** - Disabled by default, no impact unless enabled
- **Configurable** - Tune cache size and TTL for your workload
- **Graceful degradation** - If extension fails to load, Arc continues without caching

**Trade-off:** Increases memory usage by the configured cache size (default 128MB).

*Contributed by [@khalid244](https://github.com/khalid244)*

### Relative Time Expression Support in Partition Pruning

Queries using relative time expressions like `NOW() - INTERVAL` now benefit from partition pruning, dramatically reducing query times for time-filtered queries.

**Previously:** Queries with relative time filters scanned ALL parquet files because the partition pruner only recognized literal timestamp strings.

**Now:** Relative time expressions are evaluated at query time and converted to absolute timestamps for proper partition pruning.

**Supported expressions:**
| Expression | Status |
|------------|--------|
| `time >= '2024-03-15'` | ✓ Works (existing) |
| `time > NOW() - INTERVAL '20 days'` | ✓ Now works |
| `time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'` | ✓ Now works |
| `time < NOW() - INTERVAL '7 days'` | ✓ Now works |
| `time > NOW() + INTERVAL '1 day'` | ✓ Now works |

**Supported time units:** seconds, minutes, hours, days, weeks, months

**Example - Before vs After:**
```sql
-- This query now prunes to only relevant partitions
SELECT * FROM production.cpu
WHERE time > NOW() - INTERVAL '4 minutes'
  AND time < NOW() - INTERVAL '2 minutes'

-- EXPLAIN ANALYZE shows proper time bounds:
-- Filters: time>'2026-01-07 17:18:02'::TIMESTAMP WITH TIME ZONE
--      AND time<'2026-01-07 17:20:02'::TIMESTAMP WITH TIME ZONE
```

## Bug Fixes

### Control Characters in Measurement Names Break S3 (Issue #122)

Fixed an issue where measurement names containing ASCII control characters (0x01-0x08, 0x0B-0x0C, 0x0E-0x1F) would cause S3 ListObjectsV2 operations to fail with XML parsing errors.

**Cause:** S3 returns XML 1.0 responses which forbid certain control characters. Measurement names from Line Protocol, MsgPack, and Continuous Query endpoints were not validated, allowing invalid characters to be used as S3 key prefixes.

**Fix:** Added strict validation for measurement names across all ingestion endpoints:
- Must start with a letter (a-z, A-Z)
- May only contain alphanumeric characters, underscores, and hyphens
- Maximum length of 128 characters

**Affected endpoints:**
- `/write` and `/api/v2/write` (Line Protocol)
- `/api/v1/write/msgpack` (MsgPack)
- `/api/v1/continuous-queries` (Continuous Query create/update)

Invalid measurement names now return a `400 Bad Request` with a descriptive error message.

### Partition Pruner Fails on Non-Existent S3 Partitions (Issue #125, #144, PR #145)

Fixed an issue where time-filtered queries would fail with "No files found" errors when the requested time range included partitions that don't exist in S3 storage. This particularly affected queries for recent data (< 24 hours) before daily compaction has run.

**Cause:** The partition pruner generated paths for all hours in a time range without verifying existence. For local storage, it used `filepath.Glob()` to filter paths, but for S3/Azure storage, paths were passed directly to DuckDB which threw errors for missing partitions.

Additionally, for day-level paths (`year/month/day/*.parquet`), the pruner only checked if the **directory** existed (which passes when hourly subdirectories exist), but didn't verify that actual `.parquet` files exist at the day level.

**Fix:** Extended `filterExistingPaths()` to handle S3/Azure storage:
- Uses `ListDirectories()` to verify which partition paths actually exist
- For day-level paths (5 segments), verifies that `.parquet` files exist directly at that level (not just in subdirectories)
- Filters out non-existent partitions before passing to DuckDB
- Also fixed a pre-existing bug where `filepath.Join()` was mangling S3 URLs (`s3://bucket` → `s3:/bucket`)

**Result:** Queries on sparse datasets (with gaps in time partitions) now succeed and return data from existing partitions instead of failing. Grafana dashboards querying recent data (< 24 hours) on S3 now work correctly.

*Day-level file verification contributed by [@khalid244](https://github.com/khalid244)*

### Server Timeout Config Values Ignored (Issue #126)

Fixed an issue where `server.read_timeout` and `server.write_timeout` configuration values were ignored, with the server always using hardcoded 30-second timeouts.

**Cause:** The timeout values in `cmd/arc/main.go` were hardcoded to 30 seconds instead of using the loaded configuration values.

**Fix:** Now uses `cfg.Server.ReadTimeout` and `cfg.Server.WriteTimeout` from the configuration, allowing users to customize timeouts via `arc.toml` or environment variables (`ARC_SERVER_READ_TIMEOUT`, `ARC_SERVER_WRITE_TIMEOUT`).

**Note:** Default values remain at 30 seconds for backward compatibility.

### Large Payload Ingestion (413 Request Entity Too Large)

Fixed an issue where large ingestion requests (>4MB) would fail with `413 Request Entity Too Large` even though `server.max_payload_size` was configured to allow larger payloads (default: 1GB).

**Cause:** The `MaxPayloadSize` config value was not being passed to Fiber's `BodyLimit` setting, so Fiber used its default 4MB limit.

**Fix:** Now passes the configured `MaxPayloadSize` to Fiber's `BodyLimit`, allowing payloads up to the configured limit (default 1GB).

### Query Results Timestamp Timezone Inconsistency

Fixed a bug where `time.Time` values in query results could be returned with the server's local timezone instead of UTC. This caused timestamp inconsistencies when servers were running in non-UTC timezones.

**Before:** Timestamps in query results used the server's local timezone, potentially causing mismatches with stored UTC data.

**After:** All timestamps in query results are now explicitly normalized to UTC via `.UTC()` before formatting, ensuring consistency regardless of server timezone.

**Impact:** Users querying data will now always receive UTC timestamps. To display in local timezone, convert client-side or use DuckDB's `AT TIME ZONE` in queries:
```sql
SELECT time AT TIME ZONE 'America/New_York' as local_time, value
FROM mydb.cpu
```

### Azure Blob Storage Query SSL Certificate Errors on Linux (PR #92)

Fixed SSL certificate validation errors when querying data from Azure Blob Storage on Linux. The DuckDB azure extension was failing to find CA certificates due to path resolution issues with static linking.

**Fix:** On Linux, Arc now sets `azure_transport_option_type = 'curl'` which uses the system's curl library for SSL handling instead of DuckDB's built-in implementation.

*Contributed by [@schotime](https://github.com/schotime)*

### UTC Consistency for Compaction Filenames (PR #132)

Fixed timezone inconsistency in compacted file naming. Files were being named using the server's local timezone instead of UTC, which could cause issues when servers in different timezones processed the same data.

**Changes:**
- Compacted filenames now use UTC timestamps consistently (`time.Now().UTC()`)
- Updated daily file parsing to handle the full timestamp format (`YYYYMMDD_HHMMSS`)
- Removed unused `GetCompactedFilename` methods from tier implementations

*Contributed by [@schotime](https://github.com/schotime)*

### S3 Subprocess Configuration Issues (Issue #131)

Fixed compaction failures on S3-compatible storage services (Hetzner, MinIO, etc.) due to missing subprocess configuration.

**Issue 1: Credentials not forwarded**
- Subprocess fell back to AWS EC2 IMDS for credentials, timing out on non-AWS environments
- Error: `"operation error S3: GetObject, exceeded maximum number of attempts"`
- Fix: S3 credentials now passed via `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables

**Issue 2: SSL config not forwarded**
- Subprocess defaulted to HTTP when main process used HTTPS (port mismatch)
- Fix: `use_ssl` config now included in subprocess configuration

### Query Failures with Non-UTF8 Data (Issue #136)

Fixed query failures caused by non-UTF-8 characters in ingested data. Users ingesting rsyslog messages or data containing binary/non-UTF-8 content would encounter DuckDB query errors like "is not valid UTF8" when querying the data.

**Root cause:** Arc had no UTF-8 validation in the ingestion pipeline. Non-UTF-8 bytes passed through to Parquet files, which DuckDB then rejected at query time.

**Fix:** Added automatic UTF-8 sanitization during ingestion:
- Invalid UTF-8 sequences are replaced with the Unicode replacement character (U+FFFD)
- Applies to both MessagePack and Line Protocol string fields
- Optimized fast-path: valid UTF-8 (99%+ of data) adds only ~6-25ns overhead with zero allocations
- Batch-level logging warns when sanitization occurs (avoids log spam)

**Impact:** Data with invalid UTF-8 is now queryable. Users see a warning log when sanitization occurs, making it visible without breaking the ingestion flow.

### Nanosecond Timestamp Support for MessagePack Ingestion

Added support for nanosecond-precision timestamps in the MessagePack ingestion endpoint. This is particularly important for users migrating from InfluxDB, which uses nanosecond timestamps by default.

**Root cause:** The timestamp auto-detection logic only handled seconds, milliseconds, and microseconds. Nanosecond timestamps (19-digit values like `1737298800000000000`) were incorrectly treated as microseconds, resulting in dates far in the future (year ~57,000).

**Fix:** Extended timestamp detection to recognize nanosecond precision:
- Seconds: < 1e10 (10 digits) → multiply by 1,000,000
- Milliseconds: < 1e13 (13 digits) → multiply by 1,000
- Microseconds: < 1e16 (16 digits) → no conversion
- Nanoseconds: >= 1e16 (19 digits) → divide by 1,000

**Note:** Line Protocol already correctly handles nanoseconds per the InfluxDB specification.

### WHERE Clause Regex Fails to Match Multi-Line Queries (Issue #146, PR #148)

Fixed an issue where the partition pruner failed to extract time ranges from multi-line SQL queries, causing full table scans instead of targeted partition access.

**Root cause:** The `whereClausePattern` regex used `.+?` which does not match newlines in Go's regex engine. Multi-line queries with WHERE clauses spanning multiple lines would fail to extract time bounds.

**Symptoms:**
- Queries with newlines in the WHERE clause skipped partition pruning entirely
- Debug logs showed "No time range found in query, skipping partition pruning"
- Resulted in `/**/*.parquet` glob patterns (full table scan) instead of targeted partitions
- Increased S3 costs and query latency

**Fix:** Changed `.+?` to `[\s\S]+?` in the WHERE clause pattern to explicitly match any character including newlines.

**Example query now working:**
```sql
SELECT region, COUNT(*)
FROM metrics
WHERE time >= '2026-01-21T07:00:00Z'
  AND time < '2026-01-21T08:00:00Z'
GROUP BY region
```

*Contributed by [@khalid244](https://github.com/khalid244)*

### String Literals Containing SQL Keywords Break Partition Pruning

Fixed an issue where string literals containing SQL keywords (`GROUP BY`, `ORDER BY`, `LIMIT`) would cause the WHERE clause regex to terminate prematurely, potentially missing time range extraction.

**Root cause:** The `whereClausePattern` regex stopped at SQL keywords without checking if they were inside string literals. A query like `WHERE time >= '2024-01-01' AND message LIKE '%GROUP BY%'` would only capture `time >= '2024-01-01' AND message LIKE '%` before hitting the embedded `GROUP BY`.

**Fix:** Added string literal masking before regex matching. String literals are replaced with placeholders (`__STR_0__`, etc.) during WHERE clause boundary detection, then restored for time value extraction.

**Example queries now working:**
```sql
-- String containing GROUP BY
SELECT * FROM logs
WHERE time >= '2024-03-15' AND error LIKE '%GROUP BY%' AND time < '2024-03-16'
GROUP BY host

-- String containing ORDER BY
SELECT * FROM logs
WHERE time >= '2024-03-15' AND query = 'SELECT * ORDER BY id' AND time < '2024-03-16'
ORDER BY time

-- String containing LIMIT
SELECT * FROM logs
WHERE time >= '2024-03-15' AND msg LIKE '%LIMIT 100%' AND time < '2024-03-16'
LIMIT 50
```

### Buffer Age-Based Flush Timing Under High Load (Issue #142)

Fixed an issue where buffers configured with `max_buffer_age_ms` would flush significantly later than configured under high throughput scenarios.

**Root cause:** Under heavy load, the periodic flush goroutine could be starved or delayed by intensive I/O operations and lock contention. With the ticker firing every `max_buffer_age_ms`, buffers created between ticker fires had to wait for the next check cycle, and under load this delay compounded.

**Symptoms:**
- Buffers configured with `max_buffer_age_ms=5000` flushing at 6700-7000ms
- Buffers configured with `max_buffer_age_ms=1000` flushing at 2000ms+
- More pronounced under high throughput with frequent size-based flushes

**Fix:** The ticker now fires every `max_buffer_age_ms / 2` (e.g., every 2500ms for 5000ms config) while keeping the age threshold at `max_buffer_age_ms`. This gives the periodic flush goroutine more opportunities to run even under heavy load, without flushing buffers prematurely.

**Impact:**
- Before: age=6700-7000ms (with max_buffer_age_ms=5000)
- After:  age=5100-5700ms (with max_buffer_age_ms=5000)
- Improvement: ~25% faster flush timing
- Throughput: Minimal overhead (~1%)

### Arrow Writer Panic During High-Concurrency Writes (Issue #130)

Fixed a panic that occurred during high-concurrency writes when batches had different column sets (schema evolution).

**Symptoms:**
- Panic: `runtime error: index out of range [N] with length M`
- Occurred in `sliceColumnsByIndices()` during flush operations
- More likely with high write concurrency (many workers, large batches)

**Root cause:** When batches with different schemas were merged (e.g., some records had `cpu` field, others had `temperature`), the `mergeBatches()` function created columns of different lengths. When `groupByHour()` generated indices from the `time` column, accessing those indices on shorter columns caused an out-of-bounds panic.

**Fix:**
- `mergeBatches()` now normalizes all columns to the same length, using zero values for sparse positions
- `sliceColumnsByIndices()` now includes defensive bounds checking as an additional safety layer

### Empty Directories Not Cleaned Up After Daily Compaction

Fixed an issue where empty hour-level partition directories were left behind after daily compaction consolidated files into day-level partitions.

**Before:** After daily compaction deleted files from hour folders (`database/measurement/YYYY/MM/DD/HH/`), the empty directories remained, accumulating over time.

**After:** Empty directories are now automatically cleaned up after compaction:
- Removes empty hour directories after daily compaction
- Walks up the directory tree (hour → day → month → year) removing empty parents
- Stops at measurement level to preserve database structure
- Only applies to local filesystem storage (S3/Azure don't have physical folders)
- Best-effort cleanup - errors don't fail the compaction job

### Compactor OOM and Segfaults with Large Datasets (Issue #102)

Fixed critical memory issues in the compactor that caused OOM kills and DuckDB segfaults when compacting partitions with large datasets (2B+ rows, thousands of files).

**Root causes:**
1. **Memory loading**: Downloads and uploads loaded entire files into memory instead of streaming
2. **DuckDB memory limit**: The subprocess wasn't using the configured `database.memory_limit`
3. **Too many files**: DuckDB segfaults when processing 8000+ files in a single `read_parquet()` call

**Fixes applied:**
- **Streaming I/O**: Downloads now use `ReadTo()` and uploads use `WriteReader()` to stream directly to/from disk without loading files into memory
- **Memory limit passthrough**: Compaction subprocess now applies the configured `database.memory_limit` to DuckDB
- **File batching**: Partitions with >1000 files are automatically split into batches of 1000 files each, processed sequentially to avoid DuckDB limitations
- **Adaptive batch sizing on failure**: When compaction fails with recoverable errors (segfault, OOM kill, memory allocation errors), the batch is automatically split in half and retried. This continues recursively until either success or minimum batch size (2 files) is reached. Non-recoverable errors (permission denied, file not found) are not retried.

**Result:** Compaction now handles tables with billions of rows without OOM or segfaults. Even when individual batches fail due to memory pressure, adaptive splitting ensures eventual success with smaller batches. Query performance improved ~2x after successful compaction due to reduced file count.

**Optional profiling:** Set `ARC_COMPACTION_PROFILE=1` to enable heap profiling during compaction (writes to `/tmp/arc_compaction_heap.pprof`).

### Orphaned Compaction Temp Directories (Issue #164, PR #165)

Fixed an issue where compaction temp directories (`./data/compaction/{job_id}/`) accumulated on disk when compaction subprocesses crashed or were OOM-killed.

**Root cause:** Compaction runs in a subprocess for memory isolation. Each subprocess has a `defer cleanupTemp()` for cleanup, but when the subprocess is killed (SIGKILL, OOMKilled), the defer never executes.

**Fix:** Two-layer cleanup strategy:
1. **Startup cleanup**: `CleanupOrphanedTempDirs()` runs on Arc startup to remove orphaned directories from previous runs/crashes
2. **Parent-side cleanup**: After each subprocess completes (success or failure), the parent process removes the job's temp directory

**Result:** Temp directories are now cleaned up even when:
- Subprocess crashes or is OOM-killed mid-operation
- Pod crashes and restarts
- Arc is restarted after abnormal shutdown

### Compaction Data Duplication on Crash (Issue #157, PR #163)

Fixed an issue where compaction crashes could cause data duplication. If a pod crashed after uploading the compacted file but before deleting the source files, restarting compaction would re-compact the same data.

**Root cause:** No tracking mechanism existed to know which compaction operations were in progress. After a crash, Arc had no way to determine if a compacted file had been successfully uploaded.

**Fix:** Manifest-based tracking stored in S3 at `_compaction_state/`:
1. Before compaction starts, a manifest is written with input files and expected output
2. After successful upload, the manifest tracks what needs to be deleted
3. On startup, orphaned manifests are recovered - either completing deletions or retrying compaction
4. Stale manifests (older than 7 days) are automatically deleted with a warning

**Features:**
- Manifests stored in S3, preserving compute/storage separation
- Size validation detects partial uploads
- Files tracked by manifests are excluded from re-compaction candidates
- New metric: `arc_compaction_manifests_recovered_total`

*Contributed by [@khalid244](https://github.com/khalid244)*

### WAL-Based S3 Recovery (Issue #159, PR #162)

Fixed data loss during S3 outages. Previously, when S3 was unavailable, data in the Arrow buffer would be lost after the WAL callback (which preserves data for recovery) was a no-op since `ArrowBuffer` wasn't available at that point.

**Root cause:** The WAL recovery callback was registered before `ArrowBuffer` was initialized, making it impossible to replay records through the normal ingestion path.

**Fix:** Complete WAL recovery implementation:
1. **Startup recovery**: On startup, any WAL files from previous runs are replayed through `ArrowBuffer.WriteColumnarDirect()`
2. **Periodic recovery**: Background goroutine runs every `recovery_interval_seconds` (default: 300s) to recover from transient S3 failures
3. **Active file protection**: Periodic recovery skips the currently active WAL file to avoid reading uncommitted entries
4. **Backpressure handling**: Records are replayed in configurable batches (`recovery_batch_size`, default: 10000) to avoid overwhelming the buffer

**New configuration options:**
```toml
[wal]
recovery_interval_seconds = 300  # How often to check for WAL files to recover (default: 5 minutes)
recovery_batch_size = 10000      # Max records per recovery batch (default: 10000)
```

**New metrics:**
- `arc_wal_records_preserved_total` - Records written to WAL for potential recovery
- `arc_wal_recovery_total` - Number of WAL recovery operations
- `arc_wal_recovery_records_total` - Records successfully recovered from WAL

*Contributed by [@khalid244](https://github.com/khalid244)*

### Tiered Storage Query Routing (Issue #166, PR #167)

Fixed X-Arc-Database header queries not routing to cold tier data and database listing not showing cold-only databases.

**Problem:** When data was fully migrated to cold tier (S3/Azure) and no longer existed in hot tier (local), queries using the `X-Arc-Database` header would fail with "No files found" errors. Additionally, `GET /api/v1/databases` and `SHOW DATABASES` wouldn't list databases that only existed in cold storage.

**Fix:**
- Query routing now checks tiering metadata and builds multi-tier `read_parquet()` expressions when cold tier data exists
- Database and measurement listing APIs now merge results from both hot and cold tiers
- `SHOW DATABASES` and `SHOW TABLES` SQL commands now include cold tier data
- Added new `tier` column to `SHOW DATABASES` output showing 'local', 'hot', 'cold', or 'hot,cold'

**New API methods in tiering metadata:**
- `GetAllDatabases()` - List all databases across tiers
- `GetMeasurementsByDatabase()` - List measurements in a database across tiers
- `GetTiersForDatabase()` - Get which tiers contain data for a database

### Retention Policies for S3/Azure Storage Backends (Issue #169, PR #170)

Retention policies now work with all storage backends (local, S3, Azure) instead of just local filesystem.

**Problem:** When S3 or Azure was configured as the primary storage backend, retention policies silently did nothing because the implementation used filesystem-only operations (`filepath.Walk`, `os.Remove`).

**Fix:**
- Refactored `deleteOldFiles()` to use storage backend interface (`List()`, `Delete()`)
- Refactored `getMeasurementsToProcess()` to use `storage.List()` for measurement discovery
- Added `buildParquetPath()` helper to construct correct DuckDB paths for each backend type
- Fixed `getFileMaxTimeAndRowCount()` to handle `time.Time` directly from DuckDB driver

**Supported backends:**
- Local filesystem: `/path/to/base/database/measurement/...`
- S3: `s3://bucket/database/measurement/...`
- Azure: `azure://container/database/measurement/...`

### Retention Policy Empty Directory Cleanup (Issue #171)

Retention policies now clean up empty directories after deleting parquet files on local filesystem storage.

**Problem:** When retention policies deleted old parquet files, the empty YYYY/MM/DD/HH directory structure was left behind, causing directory clutter over time.

**Fix:**
- Added `cleanupEmptyDirectories()` using existing `DirectoryRemover` interface
- Recursively removes empty parent directories up to measurement level
- Only applies to local filesystem (S3/Azure don't have physical directories)
- Follows same pattern as compaction cleanup

### Query Timeout for S3 Disconnection (Issue #151, PR #152)

Added configurable query timeout to prevent indefinite hangs when S3 becomes unavailable during query execution.

**Problem:** When S3 connectivity was lost mid-query, DuckDB would hang waiting for its internal HTTP timeout (120+ seconds), causing queries to appear frozen and client connections to timeout unpredictably.

**Fix:** New `query.timeout` configuration with context-based cancellation:
- All query endpoints (JSON, Arrow, Estimate) now respect the timeout
- Returns HTTP 504 Gateway Timeout when exceeded
- Profiled queries also support timeout via new `QueryWithProfileContext` method

**Configuration:**
```toml
[query]
timeout = 300  # Query timeout in seconds (default: 300s, 0 = no timeout)
```

**Environment variable:** `ARC_QUERY_TIMEOUT`

**New metric:** `arc_query_timeouts_total` - Counter of queries that exceeded the timeout

**Note:** The context cancellation signals the timeout but doesn't immediately stop DuckDB's internal HTTP operations. The query will return 504 to the client while DuckDB completes in the background.

*Contributed by [@khalid244](https://github.com/khalid244)*

## Improvements

### Configurable Server Idle and Shutdown Timeouts

Server idle timeout and graceful shutdown timeout are now configurable instead of being hardcoded.

**New configuration options:**
| Setting | Config Key | Environment Variable | Default |
|---------|------------|---------------------|---------|
| Idle Timeout | `server.idle_timeout` | `ARC_SERVER_IDLE_TIMEOUT` | 120 seconds |
| Shutdown Timeout | `server.shutdown_timeout` | `ARC_SERVER_SHUTDOWN_TIMEOUT` | 30 seconds |

**Example configuration:**
```toml
[server]
idle_timeout = 300      # 5 minutes
shutdown_timeout = 60   # 1 minute
```

This completes the server timeout configuration options alongside the existing `read_timeout` and `write_timeout` settings.

### Automatic Time Function Query Optimization (GROUP BY Performance)

Queries using `time_bucket()` and `date_trunc()` are now automatically rewritten to epoch-based arithmetic, providing **2-2.5x performance improvement** for GROUP BY queries without any code changes. This optimization is particularly impactful for time-series aggregation queries that group data by time intervals.

**How it works:**
- `time_bucket('1 hour', time)` → `to_timestamp((epoch(time)::BIGINT / 3600) * 3600)`
- `time_bucket('30 minutes', time)` → `to_timestamp((epoch(time)::BIGINT / 1800) * 1800)`
- `date_trunc('day', time)` → `to_timestamp((epoch(time)::BIGINT / 86400) * 86400)`
- `date_trunc('hour', time)` → `to_timestamp((epoch(time)::BIGINT / 3600) * 3600)`

**Performance results:**
| Query | Before | After | Improvement |
|-------|--------|-------|-------------|
| `date_trunc('day', time) GROUP BY` | 4000ms | 1560ms | **2.6x faster** |
| `date_trunc('hour', time) GROUP BY` | 4000ms | 1560ms | **2.6x faster** |
| `time_bucket('1 hour', time)` | 2814ms | 1560ms | **1.8x faster** |
| `time_bucket('30 minutes', time)` | 2894ms | 1173ms | **2.5x faster** |

**Supported patterns:**
- `time_bucket()` with all interval types
- `date_trunc()` with second, minute, hour, day, week
- 3-argument time_bucket form with origin timestamp
- Multiple time function calls in the same query

**Note:** `time_bucket('1 month', time)` and `date_trunc('month', time)` are preserved as-is because months have variable length.

**Fast-path optimization (PR #99):** Queries that don't use `time_bucket` or `date_trunc` now skip regex processing entirely via a simple `strings.Contains` check. This eliminates ~21 unnecessary allocations (~44KB) per query, providing an **8.8x speedup** for the SQL transformation step on queries without time functions.

### Parallel Partition Scanning

Queries spanning multiple time partitions now execute in parallel, providing **2-4x speedup** for time-range queries on partitioned data.

**How it works:**
- When partition pruning identifies 3+ partition paths (configurable), queries execute concurrently
- Each partition query runs in its own goroutine with semaphore-based concurrency control
- Results are merged via a streaming iterator that presents partitions as a single result set
- Default: 4 concurrent partition queries (bounded by DuckDB connection pool)

**Example - Query spanning 24 hourly partitions:**
```sql
SELECT host, AVG(cpu) FROM metrics
WHERE time >= '2024-01-01' AND time < '2024-01-02'
GROUP BY host
```

| Execution Mode | Time |
|----------------|------|
| Sequential (before) | ~2400ms |
| Parallel 4x (after) | ~600ms |

**Configuration:**
- `MinPartitionsForParallel`: Minimum partitions to trigger parallel execution (default: 3)
- `MaxConcurrentPartitions`: Maximum concurrent partition queries (default: 4)

**Benefits:**
- Transparent to clients - no query changes required
- Bounded memory usage - results stream incrementally
- Works with existing partition pruning (time-range WHERE clauses)

### Two-Stage Distributed Aggregation (Arc Enterprise)

Aggregation queries on distributed clusters now use two-stage execution, providing **5-20x speedup** for cross-shard aggregations.

**How it works:**
1. **Scatter phase**: Query is rewritten to compute partial aggregates on each shard
2. **Gather phase**: Coordinator merges partial results into final aggregates

**Aggregation transformations:**
| Function | Shard Query | Coordinator |
|----------|-------------|-------------|
| `SUM(x)` | `SUM(x)` | `SUM(partial_sums)` |
| `COUNT(*)` | `COUNT(*)` | `SUM(partial_counts)` |
| `AVG(x)` | `SUM(x), COUNT(x)` | `SUM(sums)/SUM(counts)` |
| `MIN(x)` | `MIN(x)` | `MIN(partial_mins)` |
| `MAX(x)` | `MAX(x)` | `MAX(partial_maxes)` |

**Example - 4-shard cluster:**
```sql
-- Original query
SELECT region, AVG(latency), COUNT(*) FROM requests GROUP BY region

-- Shard query (sent to each shard)
SELECT region, SUM(latency) AS __partial_sum_0, COUNT(latency) AS __partial_count_0
FROM requests GROUP BY region

-- Coordinator merges: AVG = SUM(__partial_sum_0) / SUM(__partial_count_0)
```

**Benefits:**
- Reduces network transfer (partial aggregates vs full rows)
- Enables aggregations on datasets larger than any single shard
- Automatic query detection - no hints required
- Falls back to standard execution for unsupported patterns (HAVING, window functions, subqueries)

### DuckDB Query Engine Optimizations

Added DuckDB configuration settings that improve query performance across all query types. All settings now use `SET GLOBAL` to ensure they apply consistently across all connections in the DuckDB pool (PR #172).

**Settings enabled:**
- `parquet_metadata_cache=true` - Caches Parquet file metadata (schema, row group info) to reduce I/O on repeated access
- `prefetch_all_parquet_files=true` - Prefetches all Parquet files for S3 queries, reducing latency
- `preserve_insertion_order=true` - Ensures deterministic results for LIMIT queries

**Pool-wide consistency (PR #172):**
Previously, DuckDB settings were applied with `SET`, which only affects the connection that executes the statement. With connection pooling (`SetMaxOpenConns`), other connections in the pool would not inherit these settings. All settings are now applied with `SET GLOBAL` to ensure consistent behavior across the entire pool.

**Performance impact:**
- Aggregation queries (SUM, COUNT, AVG): **18-24% faster**
- Full table scans: **2-3% faster**
- Repeated queries on same data: benefit from metadata caching

*`SET GLOBAL` fix contributed by [@khalid244](https://github.com/khalid244)*

### Automatic Regex-to-String Function Optimization

Queries using `REGEXP_REPLACE` or `REGEXP_EXTRACT` for URL domain extraction are now automatically rewritten to use native string functions, providing **2x+ performance improvement** without any code changes.

**How it works:**

Arc detects common URL domain extraction patterns and rewrites them to equivalent CASE expressions using `split_part()` and `substr()`:

```sql
-- Original query (slow - regex engine overhead)
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS domain
FROM requests

-- Automatically rewritten to (fast - native string functions)
SELECT CASE
  WHEN Referer LIKE 'https://www.%' THEN split_part(substr(Referer, 13), '/', 1)
  WHEN Referer LIKE 'http://www.%' THEN split_part(substr(Referer, 12), '/', 1)
  WHEN Referer LIKE 'https://%' THEN split_part(substr(Referer, 9), '/', 1)
  WHEN Referer LIKE 'http://%' THEN split_part(substr(Referer, 8), '/', 1)
  ELSE split_part(Referer, '/', 1)
END AS domain
FROM requests
```

**Performance results:**
| Pattern | Before | After | Improvement |
|---------|--------|-------|-------------|
| `REGEXP_REPLACE` domain extraction | 5700ms | 2600ms | **2.2x faster** |
| `REGEXP_EXTRACT` domain extraction | 2600ms | 2100ms | **24% faster** |

**Supported patterns:**
- `REGEXP_REPLACE(column, '^https?://(?:www\.)?([^/]+)/.*$', '\1')` - Full URL to domain
- `REGEXP_EXTRACT(column, '^https?://(?:www\.)?([^/]+)', 1)` - Extract domain capture group

**Key features:**
- Transparent to applications - no query changes required
- Fast-path check skips transformation for queries without regex functions
- Non-matching patterns pass through unchanged (safe fallback)
- Handles all protocol variations: `http://`, `https://`, with/without `www.`

### Database Header for Query Optimization

Query endpoints now support the `x-arc-database` header for specifying the database context, providing **4-17% performance improvement** by skipping database extraction regex patterns.

**How it works:**
- When `x-arc-database` header is set, queries use simple table names (`SELECT * FROM cpu`) instead of `db.table` syntax
- The optimized parsing path skips 2 regex pattern matches (`patternDBTable`, `patternJoinDBTable`)
- Cross-database queries (`db.table` syntax) are rejected when header is set to enforce single-database context

**Performance results:**
| Query Type | Improvement |
|------------|-------------|
| COUNT(*) | 5-6% faster |
| SELECT with LIMIT | 3-7% faster |
| Aggregations (AVG/MIN/MAX) | **10.6% faster** |
| GROUP BY queries | **17.3% faster** |
| Overall throughput | +5-7.5% |

**Usage:**
```bash
# Header mode (optimized) - simpler SQL, faster parsing
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -H "x-arc-database: production" \
  -d '{"sql": "SELECT * FROM cpu LIMIT 100"}'

# Legacy mode (still supported) - db.table syntax in SQL
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM production.cpu LIMIT 100"}'
```

**Key features:**
- Backward compatible - header is optional, existing `db.table` syntax continues to work
- Works with both `/api/v1/query` (JSON) and `/api/v1/query/arrow` (Arrow IPC) endpoints
- Cache key includes database from header to ensure correct results
- Follows same pattern as ingestion endpoints which already use `x-arc-database` header

### MQTT Client Auto-Generated Client ID

When `client_id` is not specified in the MQTT configuration, Arc now auto-generates a unique client ID using the format `arc-{random-suffix}`. This prevents client ID collisions when running multiple Arc instances.

### MQTT Restart Endpoint

Added `/api/v1/restart` endpoint to restart the MQTT client, allowing configuration changes to be applied without restarting the entire Arc server.

## Security

### Token Hashing Security Model

Arc uses a defense-in-depth approach for API token security:

- **Storage**: All new tokens are hashed with **bcrypt** (cost factor 10) before storage
- **Lookup optimization**: SHA256-based prefixes enable O(1) database lookups without exposing tokens
- **Cache keys**: In-memory cache uses SHA256 for fast key derivation (not security-sensitive)
- **Legacy support**: Pre-v26 tokens using SHA256 hashes continue to work for backward compatibility

New tokens created since v26 use bcrypt exclusively for storage. The SHA256 usage for cache keys and database indexes is a performance optimization - security is provided by the bcrypt-hashed storage, not the lookup indexes.

## Breaking Changes

### Line Protocol Endpoint Paths Changed

The Line Protocol write endpoints have been renamed to match InfluxDB's API paths for client compatibility:

| Old Path (removed) | New Path | Action Required |
|--------------------|----------|-----------------|
| `/api/v1/write` | `/write` | Update client config |
| `/api/v1/write/influxdb` | `/api/v2/write` | Update client config |

**Impact:** If you were using the old Arc-specific paths directly, update your client configuration to use the new paths. If you're using official InfluxDB client libraries, no changes are needed - the new paths are what those clients expect.

**Unaffected:** `/api/v1/write/line-protocol` and `/api/v1/write/msgpack` remain unchanged.

## Upgrade Notes

1. **MQTT feature**: MQTT ingestion is disabled by default. Set `mqtt.enabled = true` in your configuration to enable it.

2. **Empty directory cleanup**: The compaction cleanup is automatic and requires no configuration. Existing empty directories from previous compaction runs will not be automatically cleaned up - only new compaction runs will clean up after themselves.

## Dependencies

- Added `github.com/eclipse/paho.mqtt.golang` for MQTT client support

## Arc v26.01.2 Release Notes

Bugfix release addressing Azure Blob Storage backend issues and authentication configuration.

### Bug Fixes

#### Azure Blob Storage Backend
- **Fix queries failing with Azure backend** - Queries were incorrectly using local filesystem paths (`./data/...`) instead of Azure blob paths (`azure://...`) when using Azure Blob Storage as the storage backend.
- **Fix compaction subprocess Azure authentication** - Compaction subprocess was failing with "DefaultAzureCredential: failed to acquire token" because credentials weren't being passed to the subprocess. Now passes `AZURE_STORAGE_KEY` via environment variable.

#### Configuration
- **Authentication enabled by default** - `auth.enabled` is now `true` by default in `arc.toml` for improved security out of the box.

### Files Changed
- `internal/api/query.go` - Add Azure case to `getStoragePath()`
- `internal/database/duckdb.go` - Add `configureAzureAccess()` for DuckDB azure extension
- `internal/compaction/manager.go` - Pass Azure credentials to subprocess via env var
- `internal/compaction/subprocess.go` - Read Azure credentials from env var
- `internal/storage/azure.go` - Add `GetAccountKey()` method
- `arc.toml` - Set `auth.enabled = true` by default

### Upgrade Notes
- If you were relying on authentication being disabled by default, you'll need to explicitly set `auth.enabled = false` in your `arc.toml`.

# Arc 2026.01.1 Release Notes

## New Features

### Official Python SDK

The official Python SDK for Arc is now available on PyPI as `arc-tsdb-client`.

**Installation:**
```bash
pip install arc-tsdb-client

# With DataFrame support
pip install arc-tsdb-client[pandas]   # pandas
pip install arc-tsdb-client[polars]   # polars
pip install arc-tsdb-client[all]      # all optional dependencies
```

**Key features:**
- High-performance MessagePack columnar ingestion (10M+ records/sec)
- Query support with JSON, Arrow IPC, pandas, polars, and PyArrow responses
- Full async API with httpx
- Buffered writes with automatic batching (size and time thresholds)
- Complete management API (retention policies, continuous queries, delete operations, authentication)
- DataFrame integration for pandas, polars, and PyArrow

**Quick example:**
```python
from arc_client import ArcClient

with ArcClient(host="localhost", token="your-token") as client:
    # Write data (columnar format - fastest)
    client.write.write_columnar(
        measurement="cpu",
        columns={
            "time": [1633024800000000, 1633024801000000],
            "host": ["server01", "server01"],
            "usage_idle": [95.0, 94.5],
        },
    )

    # Query to pandas DataFrame
    df = client.query.query_pandas("SELECT * FROM default.cpu")
```

**Documentation:** https://docs.basekick.net/arc/sdks/python

### Azure Blob Storage Backend
Arc now supports Azure Blob Storage as a storage backend, enabling deployment on Microsoft Azure infrastructure.

**Configuration options:**
- `storage_backend = "azure"` or `"azblob"`
- Connection string authentication
- Account key authentication
- SAS token authentication
- Managed Identity support (recommended for Azure deployments)

**Example configuration:**
```toml
[storage]
backend = "azure"
azure_container = "arc-data"
azure_account_name = "mystorageaccount"
# Use one of: connection_string, account_key, sas_token, or managed identity
azure_use_managed_identity = true
```

### Native TLS/SSL Support
Arc now supports native HTTPS/TLS without requiring a reverse proxy, ideal for users running Arc from native packages (deb/rpm) on bare metal or VMs.

**Configuration options:**
- `server.tls_enabled` - Enable/disable native TLS
- `server.tls_cert_file` - Path to certificate PEM file
- `server.tls_key_file` - Path to private key PEM file

**Environment variables:**
- `ARC_SERVER_TLS_ENABLED`
- `ARC_SERVER_TLS_CERT_FILE`
- `ARC_SERVER_TLS_KEY_FILE`

**Example configuration:**
```toml
[server]
port = 443
tls_enabled = true
tls_cert_file = "/etc/letsencrypt/live/example.com/fullchain.pem"
tls_key_file = "/etc/letsencrypt/live/example.com/privkey.pem"
```

**Key features:**
- Uses Fiber's built-in `ListenTLS()` for direct HTTPS support
- Automatic HSTS header (`Strict-Transport-Security`) when TLS is enabled
- Certificate and key file validation on startup
- Backward compatible - TLS disabled by default

### Configurable Ingestion Concurrency
Ingestion concurrency settings are now configurable to support high-concurrency deployments with many simultaneous clients (e.g., 50+ Telegraf agents).

**Configuration options:**
- `ingest.flush_workers` - Async flush worker pool size (default: 2x CPU cores, min 8, max 64)
- `ingest.flush_queue_size` - Pending flush queue capacity (default: 4x workers, min 100)
- `ingest.shard_count` - Buffer shards for lock distribution (default: 32)

**Environment variables:**
- `ARC_INGEST_FLUSH_WORKERS`
- `ARC_INGEST_FLUSH_QUEUE_SIZE`
- `ARC_INGEST_SHARD_COUNT`

**Example configuration for high concurrency:**
```toml
[ingest]
flush_workers = 32        # More workers for parallel I/O
flush_queue_size = 200    # Larger queue for burst handling
shard_count = 64          # More shards to reduce lock contention
```

**Key features:**
- Defaults scale dynamically with CPU cores (similar to QuestDB and InfluxDB)
- Previously hardcoded values now tunable for specific workloads
- Helps prevent flush queue overflow under high concurrent load

### Data-Time Partitioning

Parquet files are now organized by the data's timestamp instead of ingestion time, enabling proper backfill of historical data.

**Key features:**
- Historical data lands in correct time-based partitions (e.g., December 2024 data goes to `2024/12/` folders, not today's folder)
- Batches spanning multiple hours are automatically split into separate files per hour
- Data is sorted by timestamp within each Parquet file for optimal query performance
- Enables accurate partition pruning for time-range queries

**How it works:**
- Single-hour batches: sorted and written to one file
- Multi-hour batches: split by hour boundary, each hour sorted independently

**Example:** Backfilling data from December 1st, 2024:
```
# Before: All data went to ingestion date
data/mydb/cpu/2025/01/04/...  (wrong - today's partition)

# After: Data goes to correct historical partition
data/mydb/cpu/2024/12/01/14/...  (correct - data's timestamp)
data/mydb/cpu/2024/12/01/15/...
```

*Contributed by [@schotime](https://github.com/schotime)*

### Compaction API Triggers

Hourly and daily compaction now have separate schedules and can be triggered manually via API.

**API Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/compaction/hourly` | Trigger hourly compaction |
| `POST` | `/api/v1/compaction/daily` | Trigger daily compaction |

**Configuration:**
```toml
[compaction]
hourly_schedule = "0 * * * *"   # Every hour
daily_schedule = "0 2 * * *"    # Daily at 2 AM
```

*Contributed by [@schotime](https://github.com/schotime)*

### Configurable Max Payload Size
The maximum request payload size for write endpoints is now configurable, with the default increased from 100MB to 1GB.

**Configuration options:**
- `server.max_payload_size` - Maximum payload size (e.g., "1GB", "500MB")
- Environment variable: `ARC_SERVER_MAX_PAYLOAD_SIZE`

**Example configuration:**
```toml
[server]
max_payload_size = "2GB"
```

**Key features:**
- Applies to both compressed and decompressed payloads
- Supports human-readable units: B, KB, MB, GB
- Improved error messages suggest batching when limit is exceeded
- Default increased 10x from 100MB to 1GB to support larger bulk imports

### Database Management API

New REST API endpoints for managing databases programmatically, enabling pre-creation of databases before agents send data.

**Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/databases` | List all databases with measurement counts |
| `POST` | `/api/v1/databases` | Create a new database |
| `GET` | `/api/v1/databases/:name` | Get database info |
| `GET` | `/api/v1/databases/:name/measurements` | List measurements in a database |
| `DELETE` | `/api/v1/databases/:name` | Delete a database (requires `delete.enabled=true`) |

**Example usage:**
```bash
# List databases
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/v1/databases

# Create a database
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "production"}' \
  http://localhost:8000/api/v1/databases

# Delete a database (requires confirmation)
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/databases/old_data?confirm=true"
```

**Key features:**
- Database name validation (alphanumeric, underscore, hyphen; must start with letter; max 64 characters)
- Reserved names protected (`system`, `internal`, `_internal`)
- DELETE respects `delete.enabled` configuration for safety
- DELETE requires `?confirm=true` query parameter
- Works with all storage backends (local, S3, Azure)

### DuckDB S3 Query Support (httpfs)
Arc now configures the DuckDB httpfs extension automatically, enabling direct queries against Parquet files stored in S3.

**Key improvements:**
- Automatic httpfs extension installation and configuration
- S3 credentials passed to DuckDB for authenticated access
- `SET GLOBAL` used to persist credentials across connection pool
- Works with standard S3 buckets (note: S3 Express One Zone uses different auth and is not supported by httpfs)

**Configuration:**
```toml
[storage]
backend = "s3"
s3_bucket = "my-bucket"
s3_region = "us-east-2"
# Credentials via environment variables recommended:
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
```

## Improvements

### Storage Backend Interface Enhancements
- Added `ListDirectories()` method for efficient partition discovery
- Added `ListObjects()` method for listing files within partitions
- Both local and S3 backends implement the enhanced interface

### Compaction Subprocess Improvements
- Fixed "argument list too long" error when compacting partitions with many files
- Job configuration now passed via stdin instead of command-line arguments
- Supports compaction of partitions with 15,000+ files

### Arrow Writer Enhancements
- Added row-to-columnar conversion for efficient data ingestion
- Improved buffer management for high-throughput scenarios

### Ingestion Pipeline Optimizations
- **Zstd compression support**: Added Zstd decompression for MessagePack payloads. Zstd achieves **9.57M rec/sec** with only 5% overhead vs uncompressed (compared to 12% overhead with GZIP at 8.85M rec/sec). Auto-detected via magic bytes - no client configuration required.
- **Consolidated type conversion helpers**: Extracted common `toInt64()`, `toFloat64()`, `firstNonNil()` functions, eliminating ~100 lines of duplicate code across the ingestion pipeline.
- **O(n log n) column sorting**: Replaced O(n²) bubble sort with `sort.Slice()` for column ordering in schema inference.
- **Single-pass timestamp normalization**: Reduced from 2-3 passes to single pass for timestamp type conversion and unit normalization.
- **Result**: 7% throughput improvement (9.47M → 10.1M rec/s), 63% p50 latency reduction (8.40ms → 3.09ms), 84% p99 latency reduction (42.29ms → 6.73ms).

### Authentication Performance Optimizations
- **Token lookup index**: Added `token_prefix` column with database index for O(1) token lookup instead of O(n) full table scan. Reduces bcrypt comparisons from O(n/2) average to O(1-2) per cache miss.
- **Atomic cache counters**: Replaced mutex-protected counters with `atomic.Int64` operations, eliminating lock contention on cache hit/miss tracking.
- **Auth metrics integration**: Added Prometheus metrics for authentication requests, cache hits/misses, and auth failures for better observability.
- **Consolidated token extraction**: Extracted common `ExtractTokenFromRequest()` helper eliminating duplicate token header parsing between middleware and auth handler.

### Query Performance Optimizations
- **Arrow IPC throughput boost**: Arrow IPC query responses now deliver **5.2M rows/sec** (80% improvement from 2.88M rows/sec). Full table scans achieve **927M rows/sec** (596M records in 685ms).
- **SQL transform caching**: Added 60-second TTL cache for SQL-to-storage-path transformations. This caches the result of converting table references (e.g., `FROM mydb.cpu`) to DuckDB `read_parquet()` calls (e.g., `FROM read_parquet('./data/mydb/cpu/**/*.parquet')`). Benchmark shows 49-104x speedup on cache hits (~300ns vs 13-37μs per transformation). Particularly beneficial for dashboard refresh scenarios where the same queries are executed repeatedly.
- **Partition path caching**: Added 60-second TTL cache for `OptimizeTablePath()` results. Saves 50-100ms per recurring query pattern (significant for dashboard refresh scenarios).
- **Glob result caching**: Added 30-second TTL cache for `filepath.Glob()` results. Saves 5-10ms per query for large partition sets by avoiding repeated filesystem operations.
- Cache statistics available via `pruner.GetAllCacheStats()` for monitoring hit rates.

### Storage Roundtrip Optimizations
- **Fixed N+1 query pattern in database listing**: Listing databases with measurement counts now uses 2 storage calls instead of N+1 (90% reduction for 20 databases).
- **Optimized database existence checks**: Direct marker file lookup via `storage.Exists()` instead of listing all databases (O(1) vs O(n)).
- **Removed redundant existence checks**: `handleListMeasurements` now combines marker file check with measurement listing in a single flow.
- **Batch row counting in delete handler**: Replaced N individual COUNT queries with single batch query using `read_parquet()` with file list.
- **Combined before/after row counts**: Single query with `COUNT(*) FILTER` replaces two separate COUNT queries during delete operations.
- **Extracted partition pruning helper**: Reduced ~190 lines of duplicated code to ~90 lines with `buildReadParquetExpr()` helper.

## Bug Fixes

- Fixed DuckDB S3 credentials not persisting across connection pool (changed `SET` to `SET GLOBAL`)
- Fixed compaction subprocess failing with large file counts
- **Fixed CTE (Common Table Expressions) support** - CTEs now work correctly in queries. Previously, CTE names like `WITH campaign AS (...)` were incorrectly converted to physical storage paths, causing "No files found" errors. CTE names are now properly recognized and preserved as virtual table references.
- **Fixed JOIN clause table resolution** - `JOIN database.table` syntax now correctly converts to `read_parquet()` paths. Previously only `FROM` clauses were handled.
- **Fixed string literal corruption in queries** - String literals containing SQL keywords (e.g., `WHERE msg = 'SELECT * FROM mydb.cpu'`) are no longer incorrectly rewritten. String content is now protected during SQL-to-storage-path conversion.
- **Fixed SQL comment handling** - Comments containing table references (e.g., `-- FROM mydb.cpu`) are no longer incorrectly converted to storage paths. Both single-line (`--`) and multi-line (`/* */`) comments are now properly stripped before processing.
- **Added LATERAL JOIN support** - `LATERAL JOIN`, `CROSS JOIN LATERAL`, and other LATERAL join variants now correctly convert table references to storage paths.
- **Fixed UTC consistency in path generation** - Storage paths now consistently use UTC time instead of local timezone, preventing partition misalignment across different server timezones.

## Performance

Tested at 10.1M records/second with:
- p50 latency: 3.09ms
- p95 latency: 5.16ms
- p99 latency: 6.73ms
- p999 latency: 9.29ms

## Breaking Changes

None

## Upgrade Notes

1. **S3 credentials**: For S3 storage backend, credentials are now also passed to DuckDB for httpfs queries. Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables are set, or configure `s3_access_key` and `s3_secret_key` in the config file.

2. **Azure backend**: New storage backend option. No changes required for existing S3 or local deployments.

3. **Token prefix migration**: Existing API tokens will be automatically migrated on startup. Legacy tokens are marked with a special prefix and continue to work normally. New tokens and rotated tokens benefit from O(1) lookup performance. No action required.

## Contributors

Thanks to the following contributors for this release:

- [@schotime](https://github.com/schotime) (Adam Schroder) - Data-time partitioning, compaction API triggers, UTC fixes, Azure SSL certificate fix
- [@khalid244](https://github.com/khalid244) - Multi-line WHERE clause regex fix (Issue #146, PR #148), S3 day-level file verification (Issue #144, PR #145), S3 file caching (PR #149), Manifest-based compaction recovery (Issue #157, PR #163), WAL-based S3 recovery (Issue #159, PR #162), Query timeout for S3 disconnection (Issue #151, PR #152)

## Dependencies

- Added `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` for Azure Blob Storage support
- Added `github.com/Azure/azure-sdk-for-go/sdk/azidentity` for Azure authentication
