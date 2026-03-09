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

### Arrow Go v18.4.1 → v18.5.2

Upgraded the Apache Arrow columnar format library. Key fixes:

- **Large string Parquet writes** — Fixed serialization of strings exceeding certain size thresholds, preventing potential data corruption on large log messages or JSON payloads
- **Decompression regression** — Restored proper Parquet decompression that had degraded in a prior release
- **Reduced GC pressure** — Fewer object allocations in hot paths, benefiting high-throughput ingestion
- **Empty binary value handling** — Fixed edge case in BinaryBuilder for empty string values

## Bug Fixes

### Token Expiration Display Fix

Fixed non-expiring admin tokens incorrectly showing as "Expired" in the UI. The `TokenInfo.ExpiresAt` field used Go's `time.Time` zero value (`0001-01-01T00:00:00Z`) for tokens without expiration, which was serialized to JSON and interpreted as an expired date. Changed `ExpiresAt` from `time.Time` to `*time.Time` so non-expiring tokens serialize as `null` and are correctly displayed as "Never expires".
