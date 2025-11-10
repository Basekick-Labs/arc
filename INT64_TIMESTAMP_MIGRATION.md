# int64 Timestamp Migration Guide

## Overview

Arc now stores timestamps as int64 (microseconds since epoch) instead of DuckDB timestamp type. This provides **3-4x faster query performance** for large result sets but requires updating queries that use time functions.

## Benefits

- ✅ **3-4x faster JSON queries**: 1M rows in ~2s instead of ~7s
- ✅ **No datetime serialization overhead**: int64 passed through directly
- ✅ **Same storage size**: 8 bytes (identical to timestamp type)
- ✅ **UTC by default**: All timestamps are microseconds since Unix epoch

## Breaking Changes

### What Breaks

Queries using time functions on the `time` column:

```sql
-- ❌ BREAKS: DuckDB can't cast BIGINT to TIMESTAMP automatically
SELECT DATE_TRUNC('minute', time) AS time, AVG(value)
FROM telegraf.cpu
WHERE time >= '2025-01-01' AND time < '2025-01-02'
GROUP BY 1
```

**Error**: `Conversion Error: Unimplemented type for cast (BIGINT -> TIMESTAMP WITH TIME ZONE)`

### How to Fix

**Option 1: Use epoch_us() for comparisons**

```sql
-- ✅ WORKS: Compare int64 timestamps directly
SELECT time, value
FROM telegraf.cpu
WHERE time >= epoch_us(TIMESTAMP '2025-01-01')
  AND time < epoch_us(TIMESTAMP '2025-01-02')
```

**Option 2: Cast time column to TIMESTAMP for functions**

```sql
-- ✅ WORKS: Cast time to timestamp for DATE_TRUNC
SELECT
  DATE_TRUNC('minute', CAST(time AS TIMESTAMP)) AS time,
  AVG(value) AS avg_value
FROM telegraf.cpu
WHERE time >= epoch_us(TIMESTAMP '2025-01-01')
  AND time < epoch_us(TIMESTAMP '2025-01-02')
GROUP BY 1
```

**Option 3: Use time arithmetic on int64**

```sql
-- ✅ WORKS: Truncate to minute using modulo (fastest)
SELECT
  (time / 60000000) * 60000000 AS time_minute,  -- Truncate to minute
  AVG(value) AS avg_value
FROM telegraf.cpu
WHERE time >= 1704067200000000  -- 2025-01-01 00:00:00 UTC in microseconds
  AND time < 1704153600000000   -- 2025-01-02 00:00:00 UTC
GROUP BY 1
```

## Migration Steps

### 1. Clear Existing Data (Alpha Only)

Since you're in alpha, the cleanest approach is to clear old data:

```bash
# Stop Arc
docker compose down

# Clear data directory
rm -rf /app/data/arc/*

# Restart Arc
docker compose up -d
```

New data will use int64 timestamps automatically.

### 2. Update Grafana Dashboards

Most Grafana dashboards use `DATE_TRUNC()` for time bucketing. Update them:

**Before**:
```sql
SELECT
  DATE_TRUNC('$__interval', time) AS time,
  AVG(usage_idle) * -1 + 100 AS usage
FROM telegraf.cpu
WHERE $__timeFilter(time)
GROUP BY 1
ORDER BY 1
```

**After**:
```sql
SELECT
  DATE_TRUNC('$__interval', CAST(time AS TIMESTAMP)) AS time,
  AVG(usage_idle) * -1 + 100 AS usage
FROM telegraf.cpu
WHERE time >= epoch_us($__timeFrom())
  AND time < epoch_us($__timeTo())
GROUP BY 1
ORDER BY 1
```

Or use the faster int64 arithmetic approach:

```sql
-- For 1-minute intervals (60 seconds = 60,000,000 microseconds)
SELECT
  (time / 60000000) * 60000000 AS time,
  AVG(usage_idle) * -1 + 100 AS usage
FROM telegraf.cpu
WHERE time >= epoch_us($__timeFrom())
  AND time < epoch_us($__timeTo())
GROUP BY 1
ORDER BY 1
```

### 3. Update Application Queries

If you have application code querying Arc, update time filters:

**Python Example**:
```python
import time

# Get timestamp in microseconds
start_us = int(time.time() * 1_000_000)
end_us = start_us + (86400 * 1_000_000)  # +1 day

query = f"""
SELECT time, value
FROM telegraf.cpu
WHERE time >= {start_us}
  AND time < {end_us}
"""
```

## Query Patterns

### Time Range Filters

```sql
-- ✅ FAST: Direct int64 comparison
WHERE time >= 1704067200000000 AND time < 1704153600000000

-- ✅ WORKS: Convert timestamp strings to microseconds
WHERE time >= epoch_us(TIMESTAMP '2025-01-01')
  AND time < epoch_us(TIMESTAMP '2025-01-02')
```

### Time Bucketing (Aggregation)

```sql
-- ✅ FASTEST: Int64 arithmetic
SELECT
  (time / 3600000000) * 3600000000 AS time_hour,  -- Hourly buckets
  AVG(value)
FROM telegraf.cpu
GROUP BY 1

-- ✅ WORKS: Cast to timestamp
SELECT
  DATE_TRUNC('hour', CAST(time AS TIMESTAMP)) AS time_hour,
  AVG(value)
FROM telegraf.cpu
GROUP BY 1
```

### Date Extraction

```sql
-- ❌ OLD: DATE_PART directly on time (breaks)
SELECT DATE_PART('hour', time) AS hour, COUNT(*)
FROM telegraf.cpu
GROUP BY 1

-- ✅ NEW: Cast to timestamp first
SELECT DATE_PART('hour', CAST(time AS TIMESTAMP)) AS hour, COUNT(*)
FROM telegraf.cpu
GROUP BY 1

-- ✅ FASTEST: Use int64 arithmetic
SELECT (time / 3600000000) % 24 AS hour, COUNT(*)
FROM telegraf.cpu
GROUP BY 1
```

## Helper Functions

### Time Bucket Arithmetic

```sql
-- Microseconds per time unit
1 second      = 1,000,000 µs
1 minute      = 60,000,000 µs
1 hour        = 3,600,000,000 µs
1 day         = 86,400,000,000 µs

-- Truncate to N-minute buckets
SELECT (time / (N * 60000000)) * (N * 60000000) AS time_bucket

-- Examples:
-- 5-minute buckets:  (time / 300000000) * 300000000
-- 15-minute buckets: (time / 900000000) * 900000000
-- 1-hour buckets:    (time / 3600000000) * 3600000000
```

### Epoch Conversion

```sql
-- String to microseconds
epoch_us(TIMESTAMP '2025-01-01 12:00:00')

-- Microseconds to timestamp (for display)
CAST(time AS TIMESTAMP)

-- Current time in microseconds
epoch_us(NOW())
```

## Performance Comparison

### Before (timestamp type)
```
SELECT * FROM telegraf.cpu LIMIT 1000000;
Execution Time: 6873ms
```

### After (int64)
```
SELECT * FROM telegraf.cpu LIMIT 1000000;
Execution Time: ~2000ms (3.4x faster!)
```

The speedup comes from eliminating 1M `datetime.isoformat()` calls in Python.

## Rollback

If you need to rollback, change the schema inference:

```python
# ingest/arrow_writer.py, line ~279
elif col_name == 'time' and isinstance(sample, int):
    arrow_type = pa.timestamp('us')  # Revert to timestamp type
```

Then clear data and re-ingest.

## Common Errors

### Error: "Unimplemented type for cast (BIGINT -> TIMESTAMP)"

**Cause**: Using time functions directly on int64 column

**Fix**: Cast to timestamp first:
```sql
DATE_TRUNC('minute', CAST(time AS TIMESTAMP))
```

### Error: "Conversion Error: Could not convert string to BIGINT"

**Cause**: Comparing int64 time with string literals

**Fix**: Use `epoch_us()` to convert:
```sql
WHERE time >= epoch_us(TIMESTAMP '2025-01-01')
```

## FAQ

**Q: Why not store as timestamp type?**

A: Timestamp type requires DuckDB to convert to Python datetime objects for JSON serialization, which takes ~4s for 1M rows. Int64 is passed through directly (~2s).

**Q: What about timezone support?**

A: All timestamps are UTC. Clients handle timezone conversion for display.

**Q: Can I mix timestamp and int64 columns?**

A: Yes. The `time` column uses int64, other datetime columns can use timestamp type.

**Q: Does this affect Arrow queries?**

A: No. Arrow queries are already fast (binary format). This optimization is for JSON endpoint.

## Support

For questions or issues with the migration, please file an issue at:
https://github.com/Basekick-Labs/arc/issues
