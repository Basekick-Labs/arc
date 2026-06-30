# Arc PI / AF Data Reference demo data

Self-contained scripts to generate synthetic sensor data and load it into an Arc
instance, for building and benchmarking a PI-style integration (PlotValues,
RecordedValues, Summaries) and for the anomaly-detection demo.

Two datasets:

| Script | Dataset | Use for |
|---|---|---|
| `generate_trend_data.py` | 3 tags x 365 days, irregular timestamps, ~31.5M rows (`historian.sensors`) | **PlotValues / trend** queries — the AF Data Reference case |
| `generate_anomaly_data.py` | 3 machines x 60 days @ 5s, 2 injected faults, ~3.1M rows (`factory.vibration`) | Anomaly detection (z-score, MAD, seasonal residual) |

Both shape the data exactly like Arc stores it natively (columnar Parquet,
`time`-partitioned), so query performance is representative.

## Prerequisites

```bash
pip install duckdb msgpack
```

A running Arc instance. Default target is `http://localhost:8000`. To run Arc
locally with auth disabled (simplest for a demo):

```bash
docker run -d -p 8000:8000 -e ARC_AUTH_ENABLED=false \
  ghcr.io/basekick-labs/arc:latest
```

If auth is enabled, pass your write/admin token with `--token`.

## Generate + load

```bash
# PI-style trend dataset (~31.5M rows; a few minutes over HTTP)
python generate_trend_data.py

# Anomaly-detection dataset (~3.1M rows; under a minute)
python generate_anomaly_data.py

# Against a remote Arc with auth:
python generate_trend_data.py --arc-url https://arc.example.com --token "$ARC_TOKEN"

# Just produce a local .parquet without loading (fast, for inspection):
python generate_trend_data.py --parquet-only
```

Each script prints a sample query when it finishes.

> **Tip for the trend dataset:** Arc partitions by `year/month/day/hour`. A
> freshly-loaded year is thousands of small hourly files, and a wide-range query
> opens all of them. Let Arc's **compaction** run (enabled by default) so the
> files coalesce — a year-trend query drops from ~1.5 s to ~120 ms once the data
> is compacted. This is the single biggest performance lever for wide trends.

## PlotValues query (the trend case)

This is the PI plot algorithm: bucket the time range into `intervals` pixel-bins
and return min / max / first / last per bin, so a year-long trend renders from
~`intervals` rows instead of millions. `arg_min`/`arg_max` pick the first/last
*value at timestamp* per bin (faster and more correct than `FIRST_VALUE`/
`LAST_VALUE` window functions under Arc's multi-threaded execution).

```sql
SELECT
  bucket_id,
  min(value)            AS mn,         -- min in bin
  max(value)            AS mx,         -- max in bin
  arg_min(value, time)  AS first_val,  -- first reading (value at earliest ts)
  arg_max(value, time)  AS last_val,   -- last reading  (value at latest ts)
  min(time)             AS first_ts,
  max(time)             AS last_ts
FROM (
  SELECT value, time,
    (epoch_ms(time) - epoch_ms(TIMESTAMP '<start>')) // <bucket_ms> AS bucket_id
  FROM historian.sensors
  WHERE tag = '<tag>'
    AND time >= TIMESTAMP '<start>'
    AND time <  TIMESTAMP '<end>'
)
GROUP BY bucket_id
ORDER BY bucket_id;
```

Map AF's `PlotValues(timeRange, intervals, ...)`:

- `<bucket_ms> = (end_ms - start_ms) / intervals` — `intervals` is your pixel count.
- Keep the time range tight (`WHERE time >= ... AND time < ...`): Arc physically
  skips Parquet files outside the range. This is the biggest perf lever after
  compaction.
- Select only the columns you need (`time`, `value`, `tag`) — Arc is columnar, so
  unread columns never touch disk.

POST it to `/api/v1/query` (JSON is fine for the small decimated result). For
**raw** bulk pulls (`RecordedValues` over a big range), use `/api/v1/query/arrow`
— Arrow IPC is ~5x faster and ~3x smaller than JSON for large row counts.

## Anomaly-detection queries (the factory dataset)

Three pure-SQL detectors against `factory.vibration`. The injected faults:
compressor-01 spike on 2025-05-31 02:00–08:00, pump-01 bearing ramp 2025-06-10..15.

**Rolling z-score** (sudden events / step changes):

```sql
WITH series AS (
  SELECT machine, time_bucket(INTERVAL '10 minutes', time) AS ts, avg(value) AS v
  FROM vibration GROUP BY machine, ts
),
z AS (
  SELECT machine, ts, v,
         avg(v) OVER w AS mean, stddev(v) OVER w AS sd
  FROM series
  WINDOW w AS (PARTITION BY machine ORDER BY ts
              ROWS BETWEEN 144 PRECEDING AND 1 PRECEDING)
)
SELECT machine, ts, v, (v - mean) / NULLIF(sd, 0) AS zscore
FROM z
WHERE abs((v - mean) / NULLIF(sd, 0)) > 4
ORDER BY abs((v - mean) / NULLIF(sd, 0)) DESC;
```

**Seasonal residual** (gradual drift, fleet-wide; the production-friendly one):

```sql
WITH baseline AS (
  SELECT machine, extract('hour' FROM time_bucket(INTERVAL '1 hour', time)) AS hod,
         avg(value) AS expected, stddev(value) AS expected_sd
  FROM vibration
  WHERE time < TIMESTAMP '2025-05-31'        -- known-good training window
  GROUP BY machine, hod
),
recent AS (
  SELECT machine, time_bucket(INTERVAL '1 hour', time) AS ts, avg(value) AS v
  FROM vibration
  WHERE time >= TIMESTAMP '2025-06-05'
  GROUP BY machine, ts
)
SELECT r.machine, r.ts, r.v, b.expected,
       (r.v - b.expected) / NULLIF(b.expected_sd, 0) AS residual_z
FROM recent r
JOIN baseline b ON r.machine = b.machine AND extract('hour' FROM r.ts) = b.hod
WHERE abs((r.v - b.expected) / NULLIF(b.expected_sd, 0)) > 3
ORDER BY r.machine, r.ts;
```

Full write-up: https://basekick.net/blog/anomaly-detection-with-arc

## Notes

- The synthetic data is dated 2025 (the demo's reference year); adjust the
  `TIMESTAMP` literals in the generators if you want a different epoch.
- `time` is sent as epoch milliseconds; Arc stores it as `Timestamp(us)`.
- Questions: support[at]basekick[dot]net
