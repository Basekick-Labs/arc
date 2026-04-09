# Arc v2026.05.1 Release Notes

## Deprecations

### Authentication via `?p=` Query Parameter (InfluxDB 1.x Compatibility)

Authentication via the `?p=token` query parameter (InfluxDB 1.x compatibility) is now **deprecated**. Tokens passed in URLs are exposed in HTTP access logs from reverse proxies, load balancers, and web servers — creating a credential leak risk.

The `?p=` method continues to work but Arc now logs a one-time warning on first use. Migrate clients to the `Authorization: Bearer <token>` header instead.

## Security Fixes

### Sensitive Directories Created with World-Readable Permissions (0755)

Five directories containing sensitive data (SQLite databases with API tokens, RBAC config, audit logs, and Raft consensus state) were created with 0755 permissions, allowing any user on the system to read their contents — including SQLite WAL and SHM sidecar files.

**Affected locations:** `auth.go` (auth DB), `continuous_query.go` (CQ DB), `retention.go` (retention DB), `raft/node.go` (Raft state), `sharding/shard_raft.go` (shard Raft state).

**Fix:** All five changed to 0700 (owner-only), consistent with WAL, local storage, and temp directories which already used 0700.

**Note:** `os.MkdirAll` does not change permissions on existing directories, so existing deployments retain their current permissions. Operators upgrading from earlier versions should manually run `chmod 700` on their data directory if desired.

### SQL Injection in DuckDB `SET memory_limit` (compaction + database init)

The `SET memory_limit` command in both the main DuckDB connection (`duckdb.go`) and the compaction subprocess (`subprocess.go`) interpolated the configured memory limit value without escaping single quotes. An attacker with access to the configuration could inject arbitrary SQL.

**Fix:** Applied `escapeSQLString()` (single-quote doubling) to the memory limit value at both call sites. The existing config validation regex (`memoryLimitRe`) provides defense-in-depth but the SQL escaping is now a failsafe regardless of how the value arrives.

### SQL Injection via Sort Key Names in Compaction `ORDER BY` Clause

`buildOrderByClause()` wrapped sort key column names in double quotes but did not escape internal double quotes, allowing identifier breakout. A sort key containing a `"` character could inject arbitrary SQL into the compaction `COPY ... ORDER BY` statement.

**Fix:** Added `strings.ReplaceAll(key, "\"", "\"\"")` before quoting, matching the escaping pattern already used in `dedup.go` for tag column identifiers.

## Bug Fixes

### WAL Filename Rotation Collision

Fixed a bug where WAL filenames used second precision (`arc-YYYYMMDD_HHMMSS.wal`), allowing two rotations within the same second to produce identical filenames. The second rotation would reopen the existing file via `O_APPEND` and write a new header, corrupting the WAL structure.

**Fix:** WAL filenames now use nanosecond precision (`arc-YYYYMMDD_HHMMSS.000000000.wal`), eliminating the collision window. The new format remains lexicographically sortable and compatible with all existing `*.wal` glob patterns.

### Query Registry Reports 0 Row Count for Arrow-Path Queries

Fixed a bug where the query registry always recorded `row_count = 0` for queries served via the Arrow path.

**Root cause:** `queryRegistry.Complete(queryID, 0)` was called synchronously right after `arrowJSONQueryFunc` returned, but the Arrow path streams its response asynchronously via `SetBodyStreamWriter`. At that point the real row count from `streamArrowJSON` is not yet known — so 0 was always hardcoded. Additionally, the Arrow path's error and timeout branches returned `handled=true` without notifying the registry at all, leaving queries stuck in "running" state permanently.

**Fix:** Added `onComplete func(int)`, `onFail func(string)`, and `onTimeout func()` callbacks to `executeArrowJSONQuery`. The call site in `query.go` passes closures that invoke the appropriate registry method. The success path calls `onComplete(rc)` inside the `SetBodyStreamWriter` callback with the real row count; the error and timeout paths call `onFail`/`onTimeout` immediately before returning.

**Impact:** The query registry now correctly reflects row counts, failure reasons, and timeout status for all Arrow-path queries.

### Low-Volume Measurements Starved of Age-Based Flushes Under High Write Load

Fixed a bug in the ArrowBuffer periodic flush goroutine where low-volume measurements could be indefinitely starved of age-based flushing when a high-volume workload was running concurrently.

**Root cause:** `periodicFlush` unconditionally reset its self-adjusting timer every time a new buffer was created (signalled via `newBufferCh`). Under high write load, high-volume measurements hit their size limit frequently, triggering size-based flushes followed by immediate buffer re-creation — each re-creation sent a `newBufferCh` signal. This caused the timer to be continuously reset to `now + maxAge`, pushing the flush deadline out and preventing low-volume buffers (which rely solely on age-based flushing) from ever being flushed within the expected window.

**Fix:** The timer is now only reset when the new computed deadline is earlier than the already-scheduled deadline. If a new buffer's expiry is further in the future than the current timer, the timer is left unchanged. This guarantees that an existing pending flush deadline is never extended by a newer, shorter-lived buffer.

**Impact:** Low-volume measurements (e.g. alerts, heartbeats, status events) now reliably flush within `max_buffer_age_ms` even when co-located with high-throughput measurements on the same Arc instance.

### Memory Not Released After Delete / Retention Execution

Fixed a memory retention issue where running a retention policy or the delete API endpoint caused Arc to hold onto several GBs of memory that were never released — requiring periodic container restarts to recover.

**Root cause:** DuckDB's internal parquet metadata cache and data block cache were populated during `read_parquet()` queries executed by delete and retention operations, but never cleared after completion. Compaction already performed this cleanup; delete and retention did not.

**Changes:**

- Both the delete handler and retention handler now call `ClearHTTPCache()` after completing their file operations — always, including dry runs and no-match paths, since `read_parquet()` populates the cache regardless of whether rows are actually deleted. This evicts DuckDB's glob result cache, file metadata cache, and data block cache for the files that were rewritten or deleted.
- `debug.FreeOSMemory()` is debounced via atomic CAS and fires at most once every 30 seconds in a background goroutine. This prevents GC storms when multiple concurrent delete or retention requests complete in rapid succession, while still returning freed heap pages to the OS promptly.
- The delete file-rewrite COPY queries now include `ROW_GROUP_SIZE 122880` (extracted as a named constant `parquetRowGroupSize`), matching the compaction writer. This limits DuckDB's internal write buffer per row group, reducing peak memory during large rewrites.

**Impact:** Memory usage should return to baseline after a retention policy run or delete operation, instead of climbing GBs per execution. Particularly noticeable in Docker/Kubernetes deployments with memory limits, where this could cause OOM kills during nightly retention runs.

## Dependencies

### DuckDB v1.5.1

Upgraded DuckDB Go binding from `v2.5.5` (DuckDB 1.4.4) to `v2.10501.0` (DuckDB 1.5.1). All platform-specific binary packages (`duckdb-go-bindings`, `lib/darwin-arm64`, `lib/linux-amd64`, `lib/linux-arm64`, `lib/windows-amd64`) updated in lockstep.

### AWS SDK v2 (S3/S3-compatible storage)

Bumped `aws-sdk-go-v2` core (1.40→1.41.5), `service/s3` (1.92→1.99), `aws/protocol/eventstream` (1.7.3→1.7.8), and all internal sub-packages. Key fixes:

- DNS timeout errors are now retried automatically, improving S3 reliability on flaky networks
- Fix for config load failure when a non-existent AWS profile is configured
- `smithy-go` 1.23→1.24 protocol layer update
