# Arc v2026.04.1 Release Notes

## Performance

### Typed JSON Streaming Serialization

Replaced the query response serialization path with a typed JSON streaming writer. Instead of accumulating all rows into `[][]interface{}` and calling `json.Marshal` (which uses reflection for every cell), the new path:

1. Maps DuckDB column types once via `rows.ColumnTypes()`
2. Streams JSON directly to the HTTP response using `bufio.Writer`
3. Serializes values with `strconv.AppendInt`, `strconv.AppendFloat`, `time.AppendFormat` — zero-allocation per cell

**Results (1.8B row dataset, Apple M2 Pro):**

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

**Results (1.88B row dataset, Apple M2 Pro):**

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
- Build-tag gated (`duckdb_arrow`) — minimal builds unaffected. Will become the default (no tag required) in v2026.05.1
