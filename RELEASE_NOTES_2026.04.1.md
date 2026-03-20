# Arc v2026.04.1 Release Notes

## Performance

### Typed JSON Streaming Serialization

Replaced the query response serialization path with a typed JSON streaming writer. Instead of accumulating all rows into `[][]interface{}` and calling `json.Marshal` (which uses reflection for every cell), the new path:

1. Maps DuckDB column types once via `rows.ColumnTypes()`
2. Streams JSON directly to the HTTP response using `bufio.Writer`
3. Serializes values with `strconv.AppendInt`, `strconv.AppendFloat`, `time.AppendFormat` — zero-allocation per cell

**Results (1.8B row dataset, Apple M3 Max):**

| Query | Before (ms) | After (ms) | Improvement |
|-------|------------|------------|-------------|
| SELECT * LIMIT 100K | 144.5 | 134.2 | -7.2% |
| SELECT * LIMIT 500K | 432.8 | 386.8 | -10.6% |
| SELECT * LIMIT 1M | 806.4 | 706.4 | -12.4% |

Additional benefits:
- **Constant memory**: Streaming with periodic flush (every 5K rows) means memory usage is ~8KB regardless of result set size, eliminating OOM risk on very large result sets
- **Micro-benchmark**: 2.3x faster serialization, 99.9% fewer allocations (5 vs 30,016 allocs per 10K rows)
- No change to the JSON response format — fully backwards compatible

### DuckDB Native Arrow Query Path

Bypasses `database/sql` row-by-row scanning entirely by using DuckDB's native Arrow API (`duckdb.Arrow.QueryContext()`). Query results are read directly from DuckDB's internal columnar chunks as Arrow record batches — no `Scan()`, no `interface{}` boxing, no per-cell heap allocations.

This benefits both response formats:
- **JSON**: Typed values read directly from Arrow column arrays (`(*array.Int64).Value(row)`) instead of `interface{}` type-switching
- **Arrow IPC**: Batches go straight from DuckDB to the IPC writer — no intermediate conversion

**Results (1.88B row dataset, Apple M3 Max):**

| Endpoint | Before | After | Improvement |
|----------|--------|-------|-------------|
| JSON (`/api/v1/query`) | 1.43M rows/sec | **2.28M rows/sec** | +59% |
| Arrow IPC (`/api/v1/query/arrow`) | 2.45M rows/sec | **6.29M rows/sec** | +157% |

Detailed JSON benchmarks:

| Query | Before (ms) | After (ms) | Improvement |
|-------|------------|------------|-------------|
| SELECT * LIMIT 100K | 132.3 | 105.6 | -20.2% |
| SELECT * LIMIT 500K | 382.8 | 253.1 | -33.9% |
| SELECT * LIMIT 1M | 697.8 | 437.8 | -37.3% |

- No change to the JSON response format — fully backwards compatible
- Automatic fallback to `database/sql` path when Arrow API is unavailable
- Always enabled — the native Arrow path is compiled by default with no build tag required
- Arrow status is logged at startup: `duckdb_arrow=true`

### Basekick-Labs/msgpack v6

Migrated from `vmihailenco/msgpack/v5` to our optimized fork `Basekick-Labs/msgpack/v6`. The fork reduces allocations in the decode path, resulting in lower GC pressure under sustained high-throughput ingestion.

**Results (60s sustained load, 12 workers, Apple M3 Max):**

| Metric | vmihailenco v5.4.1 | Basekick-Labs v6.0.0 |
|--------|-------------------|---------------------|
| Avg throughput | 16.78M rec/s | **18.23M rec/s** |
| p50 latency | 0.52 ms | **0.47 ms** |
| p99 latency | 3.72 ms | **3.58 ms** |
| 60s degradation | 22% | **13%** |

The flatter degradation curve means throughput stays more consistent over time instead of dropping as GC pressure accumulates

## Observability

### WAL Dropped Entries Metric

Exposed WAL buffer drops as a Prometheus counter (`arc_wal_dropped_entries_total`) so operators can detect and alert on WAL drops in real time. Previously, drop counts were only available via `Stats()` at WAL close time.

The WAL async write buffer size is now configurable:

```toml
[wal]
buffer_size = 10000   # default: 10000 entries
```

Env var: `ARC_WAL_BUFFER_SIZE`

Operators experiencing drops under sustained load can increase this to reduce entry loss.

### Slow Query Logging

Configurable slow query detection with WARN-level logging and a Prometheus counter. When a query exceeds the threshold, Arc logs the SQL, execution time, row count, and token name — giving operators immediate visibility into queries that may need optimization.

**Configuration:**
```toml
[query]
slow_query_threshold_ms = 1000   # 0 = disabled (default)
```

Env var: `ARC_QUERY_SLOW_QUERY_THRESHOLD_MS`

**Log output:**
```
WRN Slow query detected component=query-handler execution_time_ms=1250 row_count=500000 sql="SELECT * FROM ..." token_name=my-api-token
```

**Prometheus metric:** `arc_slow_queries_total` — counter incremented for each query exceeding the threshold.

Covers all query paths: standard JSON, parallel JSON, measurement queries, and Arrow IPC JSON.

## Storage

### S3 Path Prefix Support

Added `ARC_STORAGE_S3_PREFIX` configuration option that prepends a path prefix to all S3 storage operations. This enables shared-bucket multi-tenant deployments where many instances share a single S3 bucket with path-based isolation.

**Configuration:**
```toml
[storage]
s3_bucket = "arc-cloud-data"
s3_prefix = "instances/abc123/"
```

Env var: `ARC_STORAGE_S3_PREFIX`

Files are stored as: `s3://arc-cloud-data/instances/abc123/{database}/{measurement}/...`

Works transparently with cold storage tiering, compaction, queries, and all existing storage operations. When not set, behavior is unchanged (fully backwards compatible). The prefix is validated with a character allowlist (alphanumeric, `/`, `-`, `_`, `.`) and path traversal protection.

## Dependencies

### DuckDB 1.4.3 → 1.4.4

Upgraded the DuckDB query engine (`duckdb-go` v2.5.4 → v2.5.5). Key fixes:

- **Parquet UTF-8 string stats tolerance** — Invalid UTF-8 in string statistics now tolerated instead of throwing errors, preventing query failures on data with non-UTF-8 characters
- **Arrow string view pushdown fix** — Correctness fix for the native Arrow query path, preventing incorrect varchar filter pushdown
- **`date_trunc` stat propagation** — Corrected statistics calculation for date truncation, improving row group skipping on time-based queries
- **`mode()` use-after-free** — Memory safety fix for the `mode()` aggregate function
- **RadixPartitionedHashTable stability** — Defensive fixes for GROUP BY operations under concurrent load
- **Secret secure clear** — S3/Azure credentials properly cleared from memory after use
- **httpfs upstream fixes** — Improved S3 connection stability
- **Pragma input sanitization** — Defense in depth against malformed pragma inputs

### gRPC 1.79.1 → 1.79.3 (Security)

Updated the `google.golang.org/grpc` indirect dependency. Key fixes:

- **Authorization bypass fix (1.79.3)** — Malformed `:path` headers missing the leading slash could bypass path-based "deny" rules in `grpc/authz` interceptors. Non-canonical paths are now immediately rejected with `Unimplemented`
- **Redundant error logging (1.79.2)** — Fixed spurious error logs in health/ORCA producers when no stats handler is configured

### Arrow Go v18.4.1 → v18.5.2

Upgraded the Apache Arrow columnar format library. Key fixes:

- **Large string Parquet writes** — Fixed serialization of strings exceeding certain size thresholds, preventing potential data corruption on large log messages or JSON payloads
- **Decompression regression** — Restored proper Parquet decompression that had degraded in a prior release
- **Reduced GC pressure** — Fewer object allocations in hot paths, benefiting high-throughput ingestion
- **Empty binary value handling** — Fixed edge case in BinaryBuilder for empty string values

## Ingestion

### Bulk UTF-8 Payload Pre-Validation

Replaced per-field UTF-8 validation with a single bulk validation pass over the entire HTTP payload. Previously, every string field was individually validated via `SanitizeUTF8()` during parsing — for a 1000-row batch with 5 string fields, this meant ~5000 validation calls. Now, `ValidateUTF8Bytes()` validates the entire payload once; when valid (the common case), all per-field sanitization is skipped.

This optimization applies to both ingestion paths:
- **Line Protocol**: Pre-validates in `ParseBatchWithPrecision`, skips 3 `SanitizeUTF8` call sites in `parseFieldValue`
- **MessagePack**: Pre-validates in `Decode`, short-circuits `sanitizeStringColumns` and `sanitizeStringFields`

**Benchmark results (Apple M3 Max, arm64):**

| Buffer Size | Time/op | Throughput |
|-------------|---------|------------|
| 1 KB | 17.4 ns | 58.8 GB/s |
| 4 KB | 59.3 ns | 68.9 GB/s |
| 16 KB | 208 ns | 78.6 GB/s |
| 64 KB | 793 ns | 82.5 GB/s |
| 1 MB | 12.4 μs | 84.3 GB/s |

Zero allocations on the fast path. Go's `utf8.ValidString` on arm64 already leverages NEON SIMD internally, achieving 58-84 GB/s throughput.

**Optional SIMD acceleration:** Build with `-tags simdutf` to use the [simdutf](https://github.com/simdutf/simdutf) library for large buffers (≥4KB). This provides AVX2/SSE4 acceleration on x86 servers where Go's stdlib lacks SIMD UTF-8 validation. On arm64, the standard build is already optimal. Requires `libsimdutf` installed on the build machine.

## Security

### Admin Authorization on Mutating Endpoints

Added `RequireAdmin` authorization middleware to all mutating API endpoints that previously accepted any valid token. While all endpoints already required authentication via the global token middleware, these admin-only operations (create, update, delete, execute, trigger) were accessible to read-only tokens:

- **Continuous query endpoints**: create, update, delete, execute
- **Delete endpoints**: delete, config, database delete
- **Retention policy endpoints**: create, update, delete, execute
- **Compaction endpoint**: trigger
- **Scheduler endpoints**: CQ reload, retention trigger

Read-only endpoints (list, get, status) remain accessible to any authenticated token.

### Hardened Delete WHERE Clause Validation

Expanded the forbidden keyword list for delete WHERE clauses to block `UNION`, `SELECT`, `CREATE`, `COPY`, `ATTACH`, `LOAD`, `PRAGMA`, `CALL`, and `SET` — preventing SQL injection vectors specific to DuckDB's query dialect.

### Temp Directory Permissions

Changed all temp directories (compaction, delete rewrite) from world-readable (`0755`) to owner-only (`0700`), preventing other system users from reading uncompacted or in-flight data files.

## Bug Fixes

### CQ Scheduler Reload on Update

Continuous query updates now immediately reload the scheduler with the new definition. Previously, updated CQ definitions were only picked up after a scheduler restart.

### Atomic CQ Execution Recording

Successful CQ execution recording and `last_processed_time` update are now wrapped in a SQLite transaction. Previously, a failure between the two writes could leave the time window stale, causing duplicate or missing data on the next execution.

### Database Delete Batch Fallback

When S3/Azure batch delete fails, the handler now falls back to individual file deletion instead of reporting a complete failure. This improves reliability of database deletion on cloud storage.

### S3 Delete File Rewrite Streaming

The S3 delete-rewrite path now streams the temp file to S3 via `WriteReader` instead of loading the entire file into memory with `os.ReadFile`. This prevents OOM on large Parquet files.

### CQ Scheduler Graceful Shutdown

The CQ scheduler now cancels in-flight query executions when stopping, instead of waiting for the full 10-minute timeout to expire.

### Compactor Subprocess Signal Handling

Compaction subprocesses now respond to SIGTERM/SIGINT via `signal.NotifyContext`, allowing DuckDB queries to be cancelled when the parent process times out.

### Streaming Backup Restore

Backup restore now streams Parquet files through a temp file instead of loading the entire file into memory. This prevents OOM during restore of databases with large Parquet files (100MB+).

### Local Storage Optimizations

- **WriteReader directory cache**: The streaming write path (`WriteReader`) now uses the same directory cache optimization as `Write`, reducing filesystem lock contention under sustained load
- **Context-aware file listing**: `List` and `ListObjects` now check for context cancellation during directory walks, allowing long listings on large databases to be cancelled promptly
- **DeleteBatch error reporting**: Batch deletes now return all errors (via `errors.Join`) instead of only the last one, improving diagnostics when multiple files fail to delete

### Token Expiration Display Fix

Fixed non-expiring admin tokens incorrectly showing as "Expired" in the UI. The `TokenInfo.ExpiresAt` field used Go's `time.Time` zero value (`0001-01-01T00:00:00Z`) for tokens without expiration, which was serialized to JSON and interpreted as an expired date. Changed `ExpiresAt` from `time.Time` to `*time.Time` so non-expiring tokens serialize as `null` and are correctly displayed as "Never expires".
