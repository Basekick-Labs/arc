# Arc 2026.02.1 Release Notes

## New Features

### MQTT Ingestion Support

Arc now supports native MQTT subscription for IoT and edge data ingestion. Connect directly to MQTT brokers to ingest time-series data without requiring additional infrastructure.

**Key features:**
- Subscribe to multiple MQTT topics with wildcard support (`+`, `#`)
- Flexible payload mapping from JSON to Arc measurements
- Automatic timestamp extraction from payloads or broker receive time
- QoS 0, 1, and 2 support
- TLS/SSL connections with certificate validation
- Authentication via username/password or client certificates
- Connection auto-reconnect with exponential backoff
- Per-subscription statistics and monitoring

**Configuration:**
```toml
[mqtt]
enabled = true
broker_url = "tcp://localhost:1883"
client_id = "arc-subscriber"
username = "arc"
password = "secret"

[[mqtt.subscriptions]]
topic = "sensors/+/temperature"
qos = 1
database = "iot"
measurement = "temperature"
payload_format = "json"

[mqtt.subscriptions.field_mappings]
value = "$.temp"
sensor_id = "$.device_id"
```

**Payload formats supported:**
- JSON with JSONPath field extraction
- Plain text/numeric values

**REST API for subscription management:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/mqtt/subscriptions` | List all subscriptions with stats |
| `POST` | `/api/v1/mqtt/subscriptions` | Add a new subscription |
| `DELETE` | `/api/v1/mqtt/subscriptions/:topic` | Remove a subscription |
| `GET` | `/api/v1/mqtt/stats` | Get MQTT client statistics |
| `POST` | `/api/v1/restart` | Restart MQTT client (applies config changes) |

**Example - Add subscription via API:**
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "factory/+/metrics",
    "qos": 1,
    "database": "manufacturing",
    "measurement": "machine_metrics",
    "payload_format": "json",
    "field_mappings": {
      "temperature": "$.temp",
      "pressure": "$.psi",
      "machine_id": "$.id"
    }
  }' \
  http://localhost:8080/api/v1/mqtt/subscriptions
```

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

**Result:** Compaction now handles tables with billions of rows without OOM or segfaults. Query performance improved ~2x after successful compaction due to reduced file count.

**Optional profiling:** Set `ARC_COMPACTION_PROFILE=1` to enable heap profiling during compaction (writes to `/tmp/arc_compaction_heap.pprof`).

## Improvements

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

None

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

## Dependencies

- Added `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` for Azure Blob Storage support
- Added `github.com/Azure/azure-sdk-for-go/sdk/azidentity` for Azure authentication
