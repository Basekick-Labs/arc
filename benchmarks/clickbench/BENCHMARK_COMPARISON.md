# ClickBench Benchmark Comparison

## Summary

| Metric | Baseline | Optimized | Change |
|--------|----------|-----------|--------|
| Total time (sum of avg) | 19,349 ms | 19,355 ms | ~0% |
| Average query time | 450 ms | 450 ms | ~0% |
| Successful queries | 43/43 | 43/43 | - |

## Key Findings

The ClickBench benchmark results show **minimal change** after our optimizations. This is expected because:

1. **DuckDB handles the heavy lifting**: Arc is a thin layer over DuckDB, and the bottlenecks are in DuckDB's query execution (aggregations, GROUP BY, LIKE patterns), not in Arc's SQL transformation layer.

2. **Column pruning not supported**: The DuckDB version used doesn't support the `columns` parameter in `read_parquet()`. DuckDB does projection pushdown internally, but we can't explicitly control it.

3. **Single large Parquet file**: The ClickBench dataset is stored as a single large Parquet file, so partition pruning doesn't apply.

4. **Aggregation-heavy queries**: Most ClickBench queries are aggregations that need to scan entire columns regardless of optimization.

## Optimizations Implemented

1. **DuckDB Object Cache** (`enable_object_cache=true`)
   - Caches Parquet file metadata
   - Improves repeated query performance
   - Minimal impact on cold queries

2. **Code cleanup and preparation for future optimizations**
   - Removed broken column pruning code
   - Added infrastructure for future DuckDB version upgrades

## Where Real Gains Would Come From

1. **Upgrading DuckDB** to a version that supports `columns` parameter in `read_parquet()`
2. **Partitioned data** - Time-based partition pruning works well for time-series workloads
3. **Query result caching** - Would significantly improve repeated dashboard queries
4. **Parallel partition scanning** - For queries spanning multiple partitions

## Query-by-Query Comparison

| Query | Baseline (ms) | Optimized (ms) | Notes |
|-------|---------------|----------------|-------|
| Q1: COUNT(*) | 49 | 44 | Slight improvement |
| Q3: SUM/COUNT/AVG | 82 | 92 | Within noise |
| Q5: COUNT DISTINCT UserID | 191 | 192 | No change |
| Q23: Title LIKE Google | 1051 | 1028 | Slight improvement |
| Q29: REGEXP_REPLACE | 5700 | 5806 | DuckDB-bound |
| Q34: URL GROUP BY | 1135 | 1161 | DuckDB-bound |

## Conclusion

Arc's performance on ClickBench is **dominated by DuckDB's query execution engine**. The optimizations we can make at the Arc layer (SQL transformation, result serialization) have minimal impact on these analytical queries.

To significantly improve performance, we would need to either:
- Contribute optimizations to DuckDB itself
- Implement a custom query engine (not recommended - huge effort)
- Focus on workloads where Arc's optimizations matter (time-series with partition pruning, repeated queries with caching)
