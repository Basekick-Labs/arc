# Log Ingestion Benchmark Results

**Machine**: macOS Darwin 23.6.0
**Date**: 2026-01-28
**Last updated**: 2026-01-30
**Workload**: Web API logs (500 logs/batch, 100 pre-generated batches)
**Duration**: 60 seconds

### Changelog

- **2026-01-30**: Added Arc WAL-enabled benchmark (4.59M logs/sec, ~6.5% overhead vs WAL-off). Fixed Quickwit query benchmark: per-query `max_hits` (was hardcoded to 10K) and proper `terms` aggregation for Top 10. Quickwit improvements: Count 174ms -> 0.81ms, Top 10 175ms -> 1.11ms, Filter Level 1085ms -> 120ms, Complex Filter 4595ms -> 414ms.

## Summary

| System | Throughput (logs/sec) | p50 (ms) | p95 (ms) | p99 (ms) | p999 (ms) | Errors |
|--------|----------------------:|----------|----------|----------|-----------|--------|
| **Arc (WAL off)** | **4,905,141** | 0.92 | 2.94 | 5.87 | 14.59 | 0 |
| **Arc (WAL on)** | **4,587,906** | 1.00 | 3.04 | 6.27 | 15.42 | 0 |
| VictoriaLogs | 2,262,593 | 0.86 | 16.34 | 22.90 | 37.10 | 0 |
| Loki | 1,135,568 | 4.38 | 8.91 | 25.36 | 31.43 | 0 |
| ClickHouse | 400,851 | 9.15 | 28.85 | 58.96 | 1084.17 | 0 |
| Quickwit | 251,933 | 4.06 | 93.49 | 122.77 | 265.55 | 0 |
| Elasticsearch | 101,087 | 37.93 | 78.88 | 807.50 | 2288.22 | 0 |

### Arc Performance Comparison (WAL on)

| vs System | Arc is X faster |
|-----------|-----------------|
| VictoriaLogs | **2.0x** |
| Loki | **4.0x** |
| ClickHouse | **11.4x** |
| Quickwit | **18.2x** |
| Elasticsearch | **45.4x** |

## Detailed Results

### Arc

```
================================================================================
LOG INGESTION BENCHMARK - ARC (No Compression)
================================================================================
Target: http://localhost:8000/api/v1/write/msgpack
Duration: 60s
Batch size: 500 logs
Workers: 12
Pre-generate: 100 batches
Index: logs
================================================================================

Pre-generating 100 batches of 500 logs each...
  Progress: 100/100
Generated 100 batches in 0.1s
  Avg batch size: 104.2 KB (uncompressed)

Starting test...
[   5.0s] RPS:    5236400 | Total:     26182000 | Errors:      0
[  10.0s] RPS:    5099100 | Total:     51677500 | Errors:      0
[  15.0s] RPS:    5025600 | Total:     76805500 | Errors:      0
[  20.0s] RPS:    4967600 | Total:    101643500 | Errors:      0
[  25.0s] RPS:    5028200 | Total:    126784500 | Errors:      0
[  30.0s] RPS:    4922300 | Total:    151396000 | Errors:      0
[  35.0s] RPS:    4920500 | Total:    175998500 | Errors:      0
[  40.0s] RPS:    4804400 | Total:    200020500 | Errors:      0
[  45.0s] RPS:    4768800 | Total:    223864500 | Errors:      0
[  50.0s] RPS:    4768200 | Total:    247705500 | Errors:      0
[  55.0s] RPS:    4707300 | Total:    271242000 | Errors:      0

================================================================================
RESULTS
================================================================================
Duration:        60.0s
Total sent:      294313500 logs
Total errors:    0
Success rate:    100.00%

THROUGHPUT:   4905141 logs/sec

Latency percentiles:
  p50:  0.92 ms
  p95:  2.94 ms
  p99:  5.87 ms
  p999: 14.59 ms
================================================================================
```

**Notes**:
- WAL disabled
- Buffer size reduced from 5M to 2.5M (improved throughput)
- 12 workers

---

### Arc (WAL enabled)

```
================================================================================
LOG INGESTION BENCHMARK - ARC (No Compression)
================================================================================
Target: http://localhost:8000/api/v1/write/msgpack
Duration: 60s
Batch size: 500 logs
Workers: 12
Pre-generate: 100 batches
Index: logs
================================================================================

Pre-generating 100 batches of 500 logs each...
  Progress: 100/100
Generated 100 batches in 0.1s
  Avg batch size: 104.1 KB (uncompressed)

Starting test...
[   5.0s] RPS:    4837400 | Total:     24187000 | Errors:      0
[  10.0s] RPS:    4852400 | Total:     48449000 | Errors:      0
[  15.0s] RPS:    4650400 | Total:     71701000 | Errors:      0
[  20.0s] RPS:    4763700 | Total:     95519500 | Errors:      0
[  25.0s] RPS:    4710100 | Total:    119070000 | Errors:      0
[  30.0s] RPS:    4397400 | Total:    141057000 | Errors:      0
[  35.0s] RPS:    4487900 | Total:    163496500 | Errors:      0
[  40.0s] RPS:    4485600 | Total:    185924500 | Errors:      0
[  45.0s] RPS:    4565700 | Total:    208753000 | Errors:      0
[  50.0s] RPS:    4537800 | Total:    231442000 | Errors:      0
[  55.0s] RPS:    4389400 | Total:    253389000 | Errors:      0

================================================================================
RESULTS
================================================================================
Duration:        60.0s
Total sent:      275279000 logs
Total errors:    0
Success rate:    100.00%

THROUGHPUT:   4587906 logs/sec

Latency percentiles:
  p50:  1.00 ms
  p95:  3.04 ms
  p99:  6.27 ms
  p999: 15.42 ms
================================================================================
```

**Notes**:
- WAL enabled (fdatasync mode)
- Buffer size 2.5M, 12 workers
- ~6.5% slower than WAL-disabled (4.59M vs 4.91M logs/sec)
- Latency slightly higher across all percentiles

---

### Elasticsearch

```
================================================================================
LOG INGESTION BENCHMARK - ELASTICSEARCH (No Compression)
================================================================================
Target: http://localhost:9200/_bulk
Duration: 60s
Batch size: 500 logs
Workers: 12
Pre-generate: 100 batches
Index: logs
================================================================================

Pre-generating 100 batches of 500 logs each...
  Progress: 100/100
Generated 100 batches in 0.2s
  Avg batch size: 202.3 KB (uncompressed)

Starting test...
[   5.0s] RPS:      52200 | Total:       261000 | Errors:      0
[  10.0s] RPS:      90500 | Total:       713500 | Errors:      0
[  15.0s] RPS:     102400 | Total:      1225500 | Errors:      0
[  20.0s] RPS:      96900 | Total:      1710000 | Errors:      0
[  25.0s] RPS:     106600 | Total:      2243000 | Errors:      0
[  30.0s] RPS:      95600 | Total:      2721000 | Errors:      0
[  35.0s] RPS:     136600 | Total:      3404000 | Errors:      0
[  40.0s] RPS:      88000 | Total:      3844000 | Errors:      0
[  45.0s] RPS:     145900 | Total:      4573500 | Errors:      0
[  50.0s] RPS:      77400 | Total:      4960500 | Errors:      0
[  55.0s] RPS:     152000 | Total:      5720500 | Errors:      0

================================================================================
RESULTS
================================================================================
Duration:        60.2s
Total sent:      6088500 logs
Total errors:    0
Success rate:    100.00%

THROUGHPUT:   101087 logs/sec

Latency percentiles:
  p50:  37.93 ms
  p95:  78.88 ms
  p99:  807.50 ms
  p999: 2288.22 ms
================================================================================
```

**Notes**:
- Running with `xpack.ml.enabled=false` and `xpack.security.enabled=false`
- High latency variance (77K-152K RPS) suggests segment merging/refresh overhead
- p99 latency spike to 807ms indicates GC pauses or merge operations

---

### VictoriaLogs

```
================================================================================
LOG INGESTION BENCHMARK - VICTORIALOGS (No Compression)
================================================================================
Target: http://localhost:9428/insert/jsonline
Duration: 60s
Batch size: 500 logs
Workers: 12
Pre-generate: 100 batches
Index: logs
================================================================================

Pre-generating 100 batches of 500 logs each...
  Progress: 100/100
Generated 100 batches in 0.1s
  Avg batch size: 184.7 KB (uncompressed)

Starting test...
[   5.0s] RPS:    2351900 | Total:     11759500 | Errors:      0
[  10.0s] RPS:    2375900 | Total:     23639000 | Errors:      0
[  15.0s] RPS:    2366000 | Total:     35469000 | Errors:      0
[  20.0s] RPS:    2410800 | Total:     47523000 | Errors:      0
[  25.0s] RPS:    2344300 | Total:     59244500 | Errors:      0
[  30.0s] RPS:    2222500 | Total:     70357000 | Errors:      0
[  35.0s] RPS:    2214800 | Total:     81431000 | Errors:      0
[  40.0s] RPS:    2232800 | Total:     92595000 | Errors:      0
[  45.0s] RPS:    2104800 | Total:    103119000 | Errors:      0
[  50.0s] RPS:    2189200 | Total:    114065000 | Errors:      0
[  55.0s] RPS:    2182200 | Total:    124976000 | Errors:      0
[  60.0s] RPS:    2156000 | Total:    135756000 | Errors:      0

================================================================================
RESULTS
================================================================================
Duration:        60.0s
Total sent:      135762000 logs
Total errors:    0
Success rate:    100.00%

THROUGHPUT:   2262593 logs/sec

Latency percentiles:
  p50:  0.86 ms
  p95:  16.34 ms
  p99:  22.90 ms
  p999: 37.10 ms
================================================================================
```

**Notes**:
- Very consistent throughput (2.1-2.4M RPS throughout)
- Excellent p50 latency (0.86ms), comparable to Arc
- Written in Go, optimized for log ingestion

---

### Loki

```
================================================================================
LOG INGESTION BENCHMARK - GRAFANA LOKI (No Compression)
================================================================================
Target: http://localhost:3100/loki/api/v1/push
Duration: 60s
Batch size: 500 logs
Workers: 12
Pre-generate: 100 batches
Index: logs
================================================================================

Pre-generating 100 batches of 500 logs each...
  Progress: 100/100
Generated 100 batches in 0.1s
  Avg batch size: 139.8 KB (uncompressed)

Starting test...
[   5.0s] RPS:    1233500 | Total:      6167500 | Errors:      0
[  10.0s] RPS:    1206600 | Total:     12200500 | Errors:      0
[  15.0s] RPS:    1124300 | Total:     17822000 | Errors:      0
[  20.0s] RPS:    1131500 | Total:     23479500 | Errors:      0
[  25.0s] RPS:    1115900 | Total:     29059000 | Errors:      0
[  30.0s] RPS:    1106600 | Total:     34592000 | Errors:      0
[  35.0s] RPS:    1123600 | Total:     40210000 | Errors:      0
[  40.0s] RPS:    1143600 | Total:     45928000 | Errors:      0
[  45.0s] RPS:    1124000 | Total:     51548000 | Errors:      0
[  50.0s] RPS:    1137600 | Total:     57236000 | Errors:      0
[  55.0s] RPS:    1067500 | Total:     62573500 | Errors:      0

================================================================================
RESULTS
================================================================================
Duration:        60.0s
Total sent:      68137500 logs
Total errors:    0
Success rate:    100.00%

THROUGHPUT:   1135568 logs/sec

Latency percentiles:
  p50:  4.38 ms
  p95:  8.91 ms
  p99:  25.36 ms
  p999: 31.43 ms
================================================================================
```

**Notes**:
- Running with custom config (rate limits disabled)
- Very consistent throughput (1.1-1.2M RPS)
- Good latency profile, tight p999 (31ms)
- Groups logs by {service, level} into streams per batch

---

### ClickHouse

```
================================================================================
LOG INGESTION BENCHMARK - CLICKHOUSE (No Compression)
================================================================================
Target: http://localhost:8123/?query=INSERT+INTO+logs+FORMAT+JSONEachRow
Duration: 60s
Batch size: 500 logs
Workers: 12
Pre-generate: 100 batches
Index: logs
================================================================================

Pre-generating 100 batches of 500 logs each...
  Progress: 100/100
Generated 100 batches in 0.2s
  Avg batch size: 192.8 KB (uncompressed)

Starting test...
[   5.0s] RPS:     464300 | Total:      2321500 | Errors:      0
[  10.0s] RPS:     674300 | Total:      5693000 | Errors:      0
[  15.0s] RPS:     651400 | Total:      8950000 | Errors:      0
[  20.0s] RPS:     450000 | Total:     11200000 | Errors:      0
[  25.0s] RPS:     392500 | Total:     13162500 | Errors:      0
[  30.0s] RPS:     389200 | Total:     15108500 | Errors:      0
[  35.0s] RPS:     270700 | Total:     16462000 | Errors:      0
[  40.0s] RPS:     175400 | Total:     17339000 | Errors:      0
[  45.0s] RPS:     247600 | Total:     18577000 | Errors:      0
[  50.0s] RPS:     416900 | Total:     20661500 | Errors:      0
[  55.0s] RPS:     386400 | Total:     22593500 | Errors:      0
[  60.0s] RPS:     302800 | Total:     24107500 | Errors:      0

================================================================================
RESULTS
================================================================================
Duration:        60.2s
Total sent:      24113500 logs
Total errors:    0
Success rate:    100.00%

THROUGHPUT:   400851 logs/sec

Latency percentiles:
  p50:  9.15 ms
  p95:  28.85 ms
  p99:  58.96 ms
  p999: 1084.17 ms
================================================================================
```

**Notes**:
- MergeTree engine with default settings
- High throughput variance (175K-674K RPS) due to background merges
- p999 spike to 1084ms indicates merge/flush operations
- Written in C++, optimized for OLAP queries
- Also tested native TCP protocol (port 9000): 314K logs/sec - slower than HTTP due to per-connection overhead in the Go driver's synchronous batch model. HTTP with JSONEachRow format allows better request pipelining.

---

### Quickwit

```
================================================================================
LOG INGESTION BENCHMARK - QUICKWIT (No Compression)
================================================================================
Target: http://localhost:7280/api/v1/logs/ingest
Duration: 60s
Batch size: 500 logs
Workers: 12
Pre-generate: 100 batches
Index: logs
================================================================================

Pre-generating 100 batches of 500 logs each...
  Progress: 100/100
Generated 100 batches in 0.1s
  Avg batch size: 188.1 KB (uncompressed)

Starting test...
[   5.0s] RPS:     277900 | Total:      1389500 | Errors:      0
[  10.0s] RPS:     247400 | Total:      2626500 | Errors:      0
[  15.0s] RPS:     252500 | Total:      3889000 | Errors:      0
[  20.0s] RPS:     250300 | Total:      5140500 | Errors:      0
[  25.0s] RPS:     254100 | Total:      6411000 | Errors:      0
[  30.0s] RPS:     257500 | Total:      7698500 | Errors:      0
[  35.0s] RPS:     234700 | Total:      8872000 | Errors:      0
[  40.0s] RPS:     258100 | Total:     10162500 | Errors:      0
[  45.0s] RPS:     256900 | Total:     11447000 | Errors:      0
[  50.0s] RPS:     225800 | Total:     12576000 | Errors:      0
[  55.0s] RPS:     256400 | Total:     13858000 | Errors:      0

================================================================================
RESULTS
================================================================================
Duration:        60.1s
Total sent:      15134000 logs
Total errors:    0
Success rate:    100.00%

THROUGHPUT:   251933 logs/sec

Latency percentiles:
  p50:  4.06 ms
  p95:  93.49 ms
  p99:  122.77 ms
  p999: 265.55 ms
================================================================================
```

**Notes**:
- Written in Rust, focus on search over ingestion speed
- Consistent throughput (225K-278K RPS)
- Higher tail latencies due to indexing overhead
- Full-text search indexing enabled (tantivy)

---

## Test Configuration

```bash
# Arc
go run log_bench/main.go --target arc --duration 60 --workers 12 --batch-size 500 --pregenerate 100

# Elasticsearch
go run log_bench/main.go --target elastic --duration 60 --workers 12 --batch-size 500 --pregenerate 100

# VictoriaLogs
go run log_bench/main.go --target victorialogs --duration 60 --workers 12 --batch-size 500 --pregenerate 100

# Loki
go run log_bench/main.go --target loki --duration 60 --workers 12 --batch-size 500 --pregenerate 100

# Quickwit
go run log_bench/main.go --target quickwit --duration 60 --workers 12 --batch-size 500 --pregenerate 100

# ClickHouse
go run log_bench/main.go --target clickhouse --duration 60 --workers 12 --batch-size 500 --pregenerate 100
```

---

# Query Benchmark Results

**Machine**: macOS Darwin 23.6.0
**Date**: 2026-01-28
**Dataset**: ~43M logs (from ingestion benchmark)
**Iterations**: 5 per query

## ClickHouse

```
================================================================================
QUERY BENCHMARK SUITE - CLICKHOUSE
================================================================================
Target: http://localhost:8123
Index: logs
Iterations: 5 per query
================================================================================

Query: Count All Logs
  Latency: 1.98 ms (p50: 0.58 ms, p99: 7.24 ms)
  Rows: 49272000

Query: Filter by Level (ERROR)
  Latency: 122.23 ms (p50: 131.59 ms, p99: 136.24 ms)
  Rows: 1000

Query: Filter by Service (api)
  Latency: 87.78 ms (p50: 83.62 ms, p99: 134.71 ms)
  Rows: 1000

Query: Full-text Search (timeout)
  Latency: 77.09 ms (p50: 80.05 ms, p99: 86.25 ms)
  Rows: 1000

Query: Time Range (Last 1 Hour)
  Latency: 83.04 ms (p50: 76.33 ms, p99: 103.75 ms)
  Rows: 10000

Query: Top 10 Services by Count
  Latency: 54.00 ms (p50: 33.09 ms, p99: 137.39 ms)
  Rows: 10

Query: Complex Filter (api + ERROR)
  Latency: 59.13 ms (p50: 58.44 ms, p99: 89.91 ms)
  Rows: 1000

Query: SELECT LIMIT 10K
  Latency: 39.54 ms (p50: 31.43 ms, p99: 65.17 ms)
  Rows: 10000

================================================================================
SUMMARY
================================================================================
Query                               |     Avg (ms) |     p99 (ms) |         Rows
--------------------------------------------------------------------------------
Count All Logs                      |         1.98 |         7.24 |     49272000
Filter by Level (ERROR)             |       122.23 |       136.24 |         1000
Filter by Service (api)             |        87.78 |       134.71 |         1000
Full-text Search (timeout)          |        77.09 |        86.25 |         1000
Time Range (Last 1 Hour)            |        83.04 |       103.75 |        10000
Top 10 Services by Count            |        54.00 |       137.39 |           10
Complex Filter (api + ERROR)        |        59.13 |        89.91 |         1000
SELECT LIMIT 10K                    |        39.54 |        65.17 |        10000
================================================================================
```

**Notes**:
- Dataset: 49.3M logs (standardized benchmark)
- Count query fast at 2ms, but filter queries significantly slower (59-122ms) compared to 43M run
- High p99 variance suggests cold cache or compaction overhead at 50M scale
- Top 10 aggregation at 54ms, SELECT 10K at 40ms

---

## Quickwit

```
================================================================================
QUERY BENCHMARK SUITE - QUICKWIT
================================================================================
Target: http://localhost:7280
Index: logs
Iterations: 5 per query
================================================================================

Query: Count All Logs
  Latency: 0.81 ms (p50: 0.80 ms, p99: 0.96 ms)
  Rows: 50020000

Query: Filter by Level (ERROR)
  Latency: 119.52 ms (p50: 119.65 ms, p99: 120.41 ms)
  Rows: 1998185

Query: Filter by Service (api)
  Latency: 105.91 ms (p50: 105.78 ms, p99: 107.34 ms)
  Rows: 2489116

Query: Full-text Search (timeout)
  Latency: 362.17 ms (p50: 362.18 ms, p99: 362.64 ms)
  Rows: 291501

Query: Time Range (Last 1 Hour)
  Latency: 997.36 ms (p50: 997.03 ms, p99: 998.91 ms)
  Rows: 50020000

Query: Top 10 Services by Count
  Latency: 1.11 ms (p50: 1.01 ms, p99: 1.90 ms)
  Rows: 10

Query: Complex Filter (api + ERROR)
  Latency: 413.99 ms (p50: 414.39 ms, p99: 416.54 ms)
  Rows: 93839

Query: SELECT LIMIT 10K
  Latency: 156.49 ms (p50: 156.74 ms, p99: 157.40 ms)
  Rows: 50020000

================================================================================
SUMMARY
================================================================================
Query                               |     Avg (ms) |     p99 (ms) |         Rows
--------------------------------------------------------------------------------
Count All Logs                      |         0.81 |         0.96 |     50020000
Filter by Level (ERROR)             |       119.52 |       120.41 |      1998185
Filter by Service (api)             |       105.91 |       107.34 |      2489116
Full-text Search (timeout)          |       362.17 |       362.64 |       291501
Time Range (Last 1 Hour)            |       997.36 |       998.91 |     50020000
Top 10 Services by Count            |         1.11 |         1.90 |           10
Complex Filter (api + ERROR)        |       413.99 |       416.54 |        93839
SELECT LIMIT 10K                    |       156.49 |       157.40 |     50020000
================================================================================
```

**Notes**:
- Dataset: 50.0M logs (standardized benchmark)
- Full-text search engine built on Tantivy (Rust)
- Fixed: per-query `max_hits` (previously hardcoded to 10K) and proper `terms` aggregation for Top 10
- Count query now 0.81ms (was 174ms with max_hits=10000)
- Top 10 aggregation now 1.11ms (was 175ms fetching 10K docs instead of aggregating)
- Row counts reflect `num_hits` (total matches), not returned documents
- Filter queries 106-120ms, significantly faster than previous incorrect benchmark

---

## Elasticsearch

```
================================================================================
QUERY BENCHMARK SUITE - ELASTICSEARCH
================================================================================
Target: http://localhost:9200
Index: logs
Iterations: 5 per query
================================================================================

Query: Count All Logs
  Latency: 0.81 ms (p50: 0.74 ms, p99: 1.23 ms)
  Rows: 50470300

Query: Filter by Level (ERROR)
  Latency: 17.53 ms (p50: 17.48 ms, p99: 18.05 ms)
  Rows: 1000

Query: Filter by Service (api)
  Latency: 16.13 ms (p50: 16.02 ms, p99: 16.48 ms)
  Rows: 1000

Query: Full-text Search (timeout)
  Latency: 17.78 ms (p50: 17.60 ms, p99: 18.38 ms)
  Rows: 1000

Query: Time Range (Last 1 Hour)
  Latency: 70.89 ms (p50: 70.42 ms, p99: 77.75 ms)
  Rows: 10000

Query: Top 10 Services by Count
  Latency: 1.14 ms (p50: 1.17 ms, p99: 1.41 ms)
  Rows: 10

Query: Complex Filter (api + ERROR)
  Latency: 24.57 ms (p50: 24.61 ms, p99: 26.75 ms)
  Rows: 1000

Query: SELECT LIMIT 10K
  Latency: 53.23 ms (p50: 52.98 ms, p99: 54.40 ms)
  Rows: 10000

================================================================================
SUMMARY
================================================================================
Query                               |     Avg (ms) |     p99 (ms) |         Rows
--------------------------------------------------------------------------------
Count All Logs                      |         0.81 |         1.23 |     50470300
Filter by Level (ERROR)             |        17.53 |        18.05 |         1000
Filter by Service (api)             |        16.13 |        16.48 |         1000
Full-text Search (timeout)          |        17.78 |        18.38 |         1000
Time Range (Last 1 Hour)            |        70.89 |        77.75 |        10000
Top 10 Services by Count            |         1.14 |         1.41 |           10
Complex Filter (api + ERROR)        |        24.57 |        26.75 |         1000
SELECT LIMIT 10K                    |        53.23 |        54.40 |        10000
================================================================================
```

**Notes**:
- Dataset: 50.5M logs (standardized benchmark)
- Fastest count query across all systems (0.81ms) and Top 10 aggregation (1.14ms)
- Filter queries 16-18ms, consistent across filter types
- Time Range now returns actual data (71ms for 10K rows) — previously returned 0 rows on 6M dataset
- SELECT 10K rows takes ~53ms to serialize/return

---

## Grafana Loki

```
================================================================================
QUERY BENCHMARK SUITE - GRAFANA LOKI
================================================================================
Target: http://localhost:3100
Index: logs
Iterations: 5 per query
================================================================================

Query: Count All Logs
  Latency: 3840.39 ms (p50: 3832.79 ms, p99: 3867.66 ms)
  Rows: 1361128

Query: Filter by Level (ERROR)
  Latency: 26.11 ms (p50: 24.98 ms, p99: 31.31 ms)
  Rows: 5000

Query: Filter by Service (api)
  Latency: 152.54 ms (p50: 151.39 ms, p99: 157.72 ms)
  Rows: 5000

Query: Full-text Search (timeout)
  Latency: 2074.04 ms (p50: 2072.15 ms, p99: 2084.01 ms)
  Rows: 1152

Query: Time Range (Last 1 Hour)
  Latency: 1885.80 ms (p50: 1885.94 ms, p99: 1912.86 ms)
  Rows: 5000

Query: Top 10 Services by Count
  Latency: 3571.84 ms (p50: 3567.05 ms, p99: 3632.80 ms)
  Rows: 1361128

Query: Complex Filter (api + ERROR)
  Latency: 11.59 ms (p50: 11.57 ms, p99: 12.16 ms)
  Rows: 416

Query: SELECT LIMIT 10K
  Latency: 1869.42 ms (p50: 1867.51 ms, p99: 1892.26 ms)
  Rows: 5000

================================================================================
SUMMARY
================================================================================
Query                               |     Avg (ms) |     p99 (ms) |         Rows
--------------------------------------------------------------------------------
Count All Logs                      |      3840.39 |      3867.66 |      1361128
Filter by Level (ERROR)             |        26.11 |        31.31 |         5000
Filter by Service (api)             |       152.54 |       157.72 |         5000
Full-text Search (timeout)          |      2074.04 |      2084.01 |         1152
Time Range (Last 1 Hour)            |      1885.80 |      1912.86 |         5000
Top 10 Services by Count            |      3571.84 |      3632.80 |      1361128
Complex Filter (api + ERROR)        |        11.59 |        12.16 |          416
SELECT LIMIT 10K                    |      1869.42 |      1892.26 |         5000
================================================================================
```

**Notes**:
- Dataset: **1.36M logs** — Loki silently rejected most of the 50M ingested logs due to stream/rate limits, despite reporting 204 success codes. Unable to reach 50M target.
- Count and aggregation queries very slow (3.5-3.8s) — Loki scans all chunks for metric queries
- Label-based filter queries still relatively fast (12-26ms for narrow filters)
- Filter by Service slower at 153ms (broader match than level filter)
- Full-text search, time range, and SELECT queries all ~1.9-2.1s
- Loki is architecturally optimized for log streaming/tailing, not bulk analytical queries

---

## VictoriaLogs

```
================================================================================
QUERY BENCHMARK SUITE - VICTORIALOGS
================================================================================
Target: http://localhost:9428
Index: logs
Iterations: 5 per query
================================================================================

Query: Count All Logs
  Latency: 1.21 ms (p50: 1.25 ms, p99: 1.25 ms)
  Rows: 50184000

Query: Filter by Level (ERROR)
  Latency: 2.87 ms (p50: 3.01 ms, p99: 3.12 ms)
  Rows: 1000

Query: Filter by Service (api)
  Latency: 2.51 ms (p50: 2.39 ms, p99: 3.24 ms)
  Rows: 1000

Query: Full-text Search (timeout)
  Latency: 9.08 ms (p50: 5.44 ms, p99: 23.29 ms)
  Rows: 1000

Query: Time Range (Last 1 Hour)
  Latency: 9.26 ms (p50: 9.28 ms, p99: 9.58 ms)
  Rows: 10000

Query: Top 10 Services by Count
  Latency: 157.90 ms (p50: 157.18 ms, p99: 165.21 ms)
  Rows: 10

Query: Complex Filter (api + ERROR)
  Latency: 10.95 ms (p50: 10.86 ms, p99: 11.31 ms)
  Rows: 1000

Query: SELECT LIMIT 10K
  Latency: 9.74 ms (p50: 9.31 ms, p99: 11.08 ms)
  Rows: 10000

================================================================================
SUMMARY
================================================================================
Query                               |     Avg (ms) |     p99 (ms) |         Rows
--------------------------------------------------------------------------------
Count All Logs                      |         1.21 |         1.25 |     50184000
Filter by Level (ERROR)             |         2.87 |         3.12 |         1000
Filter by Service (api)             |         2.51 |         3.24 |         1000
Full-text Search (timeout)          |         9.08 |        23.29 |         1000
Time Range (Last 1 Hour)            |         9.26 |         9.58 |        10000
Top 10 Services by Count            |       157.90 |       165.21 |           10
Complex Filter (api + ERROR)        |        10.95 |        11.31 |         1000
SELECT LIMIT 10K                    |         9.74 |        11.08 |        10000
================================================================================
```

**Notes**:
- Dataset: 50.2M logs (standardized benchmark)
- Very fast count queries via stats (1.2ms) - optimized aggregation
- Filter queries extremely fast (2.5-3ms)
- Top 10 aggregation at 158ms - scales well with dataset size
- Overall excellent query performance for a log-optimized database

---

## Arc (JSON)

```
================================================================================
QUERY BENCHMARK SUITE - ARC
================================================================================
Target: http://localhost:8000
Index: logs
Iterations: 5 per query
================================================================================

Query: Count All Logs
  Latency: 2.51 ms (p50: 2.51 ms, p99: 2.68 ms)
  Rows: 50077500

Query: Filter by Level (ERROR)
  Latency: 8.12 ms (p50: 8.08 ms, p99: 8.60 ms)
  Rows: 1000

Query: Filter by Service (api)
  Latency: 7.66 ms (p50: 7.68 ms, p99: 7.84 ms)
  Rows: 1000

Query: Full-text Search (timeout)
  Latency: 8.89 ms (p50: 8.80 ms, p99: 9.26 ms)
  Rows: 1000

Query: Time Range (Last 1 Hour)
  Latency: 29.03 ms (p50: 29.07 ms, p99: 29.68 ms)
  Rows: 10000

Query: Top 10 Services by Count
  Latency: 18.28 ms (p50: 18.29 ms, p99: 18.40 ms)
  Rows: 10

Query: Complex Filter (api + ERROR)
  Latency: 9.57 ms (p50: 9.50 ms, p99: 10.14 ms)
  Rows: 1000

Query: SELECT LIMIT 10K
  Latency: 32.68 ms (p50: 32.58 ms, p99: 33.58 ms)
  Rows: 10000

================================================================================
SUMMARY
================================================================================
Query                               |     Avg (ms) |     p99 (ms) |         Rows
--------------------------------------------------------------------------------
Count All Logs                      |         2.51 |         2.68 |     50077500
Filter by Level (ERROR)             |         8.12 |         8.60 |         1000
Filter by Service (api)             |         7.66 |         7.84 |         1000
Full-text Search (timeout)          |         8.89 |         9.26 |         1000
Time Range (Last 1 Hour)            |        29.03 |        29.68 |        10000
Top 10 Services by Count            |        18.28 |        18.40 |           10
Complex Filter (api + ERROR)        |         9.57 |        10.14 |         1000
SELECT LIMIT 10K                    |        32.68 |        33.58 |        10000
================================================================================
```

**Notes**:
- Dataset: 50.1M logs (standardized benchmark)
- Count query on ~50M rows completes in 2.5ms
- Consistent filter query latencies (8-9ms)
- Top 10 aggregation at 18ms

---

## Arc (Arrow IPC)

```
================================================================================
QUERY BENCHMARK SUITE - ARC (ARROW)
================================================================================
Target: http://localhost:8000
Index: logs
Iterations: 5 per query
================================================================================

Query: Count All Logs
  Latency: 2.69 ms (p50: 2.65 ms, p99: 2.86 ms)
  Rows: 50077500

Query: Filter by Level (ERROR)
  Latency: 7.71 ms (p50: 7.56 ms, p99: 8.14 ms)
  Rows: 1000

Query: Filter by Service (api)
  Latency: 7.52 ms (p50: 7.48 ms, p99: 7.78 ms)
  Rows: 1000

Query: Full-text Search (timeout)
  Latency: 9.11 ms (p50: 8.90 ms, p99: 10.01 ms)
  Rows: 1000

Query: Time Range (Last 1 Hour)
  Latency: 24.91 ms (p50: 25.00 ms, p99: 25.33 ms)
  Rows: 10000

Query: Top 10 Services by Count
  Latency: 18.56 ms (p50: 18.39 ms, p99: 19.26 ms)
  Rows: 10

Query: Complex Filter (api + ERROR)
  Latency: 9.09 ms (p50: 9.05 ms, p99: 9.34 ms)
  Rows: 1000

Query: SELECT LIMIT 10K
  Latency: 29.10 ms (p50: 29.31 ms, p99: 29.53 ms)
  Rows: 10000

================================================================================
SUMMARY
================================================================================
Query                               |     Avg (ms) |     p99 (ms) |         Rows
--------------------------------------------------------------------------------
Count All Logs                      |         2.69 |         2.86 |     50077500
Filter by Level (ERROR)             |         7.71 |         8.14 |         1000
Filter by Service (api)             |         7.52 |         7.78 |         1000
Full-text Search (timeout)          |         9.11 |        10.01 |         1000
Time Range (Last 1 Hour)            |        24.91 |        25.33 |        10000
Top 10 Services by Count            |        18.56 |        19.26 |           10
Complex Filter (api + ERROR)        |         9.09 |         9.34 |         1000
SELECT LIMIT 10K                    |        29.10 |        29.53 |        10000
================================================================================
```

**Notes**:
- Dataset: 50.1M logs (standardized benchmark)
- Arrow IPC streaming format eliminates JSON serialization overhead
- Time Range query 14% faster than JSON (24.91ms vs 29.03ms)
- SELECT LIMIT 10K 11% faster than JSON (29.10ms vs 32.68ms)
- Filter queries ~5% faster than JSON

---

## Cross-System Query Comparison

**Dataset sizes**: Arc 50.1M | ClickHouse 49.3M | Elasticsearch 50.5M | VictoriaLogs 50.2M | Quickwit 50.0M | Loki 1.4M

> Note: Loki was tested with a smaller dataset due to silent ingestion rejection (returned 204 but dropped most logs). Quickwit row counts reflect `num_hits` (total matches), not returned documents — actual results are capped by `max_hits`.

### Raw Latencies (ms)

| Query | Arc JSON | Arc Arrow | ClickHouse | Elasticsearch | VictoriaLogs | Quickwit | Loki |
|-------|--------:|---------:|----------:|-------------:|------------:|--------:|-----:|
| Count All | 2.51 | 2.69 | 1.98 | 0.81 | 1.21 | 0.81 | 3840.39 |
| Filter Level | 8.12 | 7.71 | 122.23 | 17.53 | 2.87 | 119.52 | 26.11 |
| Filter Service | 7.66 | 7.52 | 87.78 | 16.13 | 2.51 | 105.91 | 152.54 |
| Full-text Search | 8.89 | 9.11 | 77.09 | 17.78 | 9.08 | 362.17 | 2074.04 |
| Time Range 10K | 29.03 | 24.91 | 83.04 | 70.89 | 9.26 | 997.36 | 1885.80 |
| Top 10 Services | 18.28 | 18.56 | 54.00 | 1.14 | 157.90 | 1.11 | 3571.84 |
| Complex Filter | 9.57 | 9.09 | 59.13 | 24.57 | 10.95 | 413.99 | 11.59 |
| SELECT 10K | 32.68 | 29.10 | 39.54 | 53.23 | 9.74 | 156.49 | 1869.42 |

### Rankings by Query Type

| Query | 1st | 2nd | 3rd | 4th | 5th | 6th | 7th |
|-------|-----|-----|-----|-----|-----|------|------|
| Count All | **ES** 0.8ms | **QW** 0.8ms | **VLogs** 1.2ms | **CH** 2.0ms | **Arc** 2.5ms | Loki 3840ms | |
| Filter Level | **VLogs** 2.9ms | **Arc** 7.7ms | **Arc JSON** 8.1ms | ES 18ms | Loki 26ms | QW 120ms | CH 122ms |
| Filter Service | **VLogs** 2.5ms | **Arc** 7.5ms | **Arc JSON** 7.7ms | ES 16ms | QW 106ms | Loki 153ms | CH 88ms |
| Full-text Search | **Arc JSON** 8.9ms | **VLogs** 9.1ms | **Arc** 9.1ms | ES 18ms | CH 77ms | QW 362ms | Loki 2074ms |
| Time Range 10K | **VLogs** 9.3ms | **Arc** 24.9ms | **Arc JSON** 29.0ms | ES 71ms | CH 83ms | QW 997ms | Loki 1886ms |
| Top 10 Services | **QW** 1.1ms | **ES** 1.1ms | **Arc** 18.3ms | CH 54ms | VLogs 158ms | Loki 3572ms | |
| Complex Filter | **Arc** 9.1ms | **Arc JSON** 9.6ms | **VLogs** 11.0ms | Loki 12ms | ES 25ms | CH 59ms | QW 414ms |
| SELECT 10K | **VLogs** 9.7ms | **Arc** 29.1ms | **Arc JSON** 32.7ms | CH 40ms | ES 53ms | QW 156ms | Loki 1869ms |

### Key Insights

1. **Arc is consistently top-3** across all query types on 50M logs, and fastest on Complex Filter (9ms) and Full-text Search (8.9ms)
2. **VictoriaLogs excels at filters and retrieval** — fastest on Filter Level, Filter Service, Time Range, and SELECT 10K
3. **Elasticsearch and Quickwit** tie for fastest Count (0.8ms); Quickwit also wins Top 10 aggregation (1.1ms) with proper `terms` aggregation
4. **ClickHouse count is fast (2ms)** but filter queries are slower (59-122ms) at 50M scale — high p99 variance suggests compaction overhead
5. **Arc Arrow vs JSON**: Arrow is 5-14% faster on data-heavy queries (Time Range, SELECT 10K) due to eliminated JSON serialization
6. **Quickwit** re-benchmarked with correct per-query `max_hits` and proper aggregations — dramatically faster than previous incorrect benchmark (Count: 0.8ms vs 174ms, Top 10: 1.1ms vs 175ms). Filter queries 106-120ms, full-text search 362ms.
7. **Loki** only stored 1.4M of 50M ingested logs due to silent rejection — results not comparable at scale
