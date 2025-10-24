# Continuous Queries (Downsampling & Aggregation)

Continuous queries allow you to automatically downsample and aggregate time-series data into materialized views for faster queries and reduced storage. Manual execution only - automatic execution is reserved for enterprise edition.

## Features

- **Manual trigger only** - Automatic execution is enterprise feature
- **Flexible aggregations** - Support for AVG, SUM, MIN, MAX, COUNT, and more
- **Time-based grouping** - Aggregate by minute, hour, day, week, month
- **Custom retention** - Define how long aggregated data is kept
- **Dry-run support** - Test queries before executing
- **Execution history** - Track all query executions
- **Source data preservation** - Original data remains intact (or specify deletion)

## Use Cases

### 1. Downsampling for Long-Term Storage
```
Raw data (10s): cpu usage every 10 seconds → Keep for 7 days
Downsampled (1m): avg cpu usage per minute → Keep for 90 days
Downsampled (1h): avg cpu usage per hour → Keep for 1 year
```

### 2. Pre-Aggregated Dashboards
```
Dashboard showing hourly metrics:
- Instead of: SELECT AVG(value) FROM cpu WHERE time > now() - 30d GROUP BY hour
- Use: SELECT * FROM cpu_hourly WHERE time > now() - 30d
(10x faster queries!)
```

### 3. Summary Tables
```
Daily summaries:
- Total requests per day
- Average response time per day
- Peak CPU usage per day
```

---

## Architecture

### Continuous Query Definition

```json
{
  "name": "cpu_1h_downsample",
  "source_measurement": "cpu",
  "destination_measurement": "cpu_1h",
  "database": "default",
  "query": "SELECT mean(usage_user) as usage_user_avg, mean(usage_system) as usage_system_avg FROM cpu GROUP BY time(1h), host",
  "interval": "1h",
  "fill_policy": "null",
  "retention_days": 365,
  "delete_source_data": false,
  "is_active": true
}
```

### Query Syntax

Arc uses DuckDB SQL for continuous queries:

```sql
SELECT
  epoch_us(date_trunc('hour', time::TIMESTAMP)) as time,  -- Use epoch_us() for optimal performance
  host,
  region,
  AVG(usage_idle) as usage_idle_avg,
  AVG(usage_user) as usage_user_avg,
  MAX(usage_user) as usage_user_max,
  MIN(usage_user) as usage_user_min,
  COUNT(*) as sample_count
FROM cpu
WHERE time::TIMESTAMP >= {start_time}::TIMESTAMP AND time::TIMESTAMP < {end_time}::TIMESTAMP
GROUP BY 1, 2, 3  -- More efficient than repeating expressions
ORDER BY time
```

**Performance Best Practices:**
- ✅ **Use `epoch_us()`** - Returns time as integer microseconds (faster than string parsing)
- ✅ **Use `date_trunc()`** - For time bucketing (hour, day, etc.)
- ✅ **Use GROUP BY 1, 2, 3** - Reference columns by position
- ❌ Avoid `time_bucket()` - Use `date_trunc()` instead for DuckDB

**Placeholders:**
- `{start_time}` - Automatically filled based on last execution
- `{end_time}` - Automatically filled (typically NOW)

**Time Format Note:**
- Source data stores time as `timestamp[us]` in Parquet (microseconds)
- Using `epoch_us()` returns integer microseconds directly (no string conversion overhead)
- Alternative: `date_trunc('hour', time)` returns human-readable string (slower but easier to read)

---

## API Endpoints

### Create Continuous Query

```bash
POST /api/v1/continuous_queries
```

**Request:**
```json
{
  "name": "cpu_hourly_avg",
  "description": "Hourly average CPU metrics for long-term storage",
  "database": "default",
  "source_measurement": "cpu",
  "destination_measurement": "cpu_1h",
  "query": "SELECT time_bucket(INTERVAL '1 hour', time) as time, host, AVG(usage_idle) as usage_idle_avg, AVG(usage_user) as usage_user_avg, AVG(usage_system) as usage_system_avg, COUNT(*) as sample_count FROM cpu WHERE time >= {start_time} AND time < {end_time} GROUP BY time_bucket(INTERVAL '1 hour', time), host ORDER BY time",
  "interval": "1h",
  "retention_days": 365,
  "delete_source_after_days": null,
  "is_active": true
}
```

**Response:**
```json
{
  "id": 1,
  "name": "cpu_hourly_avg",
  "description": "Hourly average CPU metrics for long-term storage",
  "database": "default",
  "source_measurement": "cpu",
  "destination_measurement": "cpu_1h",
  "query": "SELECT ...",
  "interval": "1h",
  "retention_days": 365,
  "delete_source_after_days": null,
  "is_active": true,
  "last_execution_time": null,
  "last_execution_status": null,
  "last_processed_time": null,
  "last_records_written": null,
  "created_at": "2025-10-24T16:00:00.000Z",
  "updated_at": "2025-10-24T16:00:00.000Z"
}
```

### List Continuous Queries

```bash
GET /api/v1/continuous_queries
```

**Optional Query Parameters:**
- `database` - Filter by database
- `source_measurement` - Filter by source measurement
- `is_active` - Filter by active status

**Response:**
```json
[
  {
    "id": 1,
    "name": "cpu_hourly_avg",
    "database": "default",
    "source_measurement": "cpu",
    "destination_measurement": "cpu_1h",
    "interval": "1h",
    "is_active": true,
    "last_execution_time": "2025-10-24T15:00:00.000Z",
    "last_execution_status": "completed",
    "last_records_written": 1440,
    "created_at": "2025-10-24T10:00:00.000Z"
  }
]
```

### Get Single Continuous Query

```bash
GET /api/v1/continuous_queries/{query_id}
```

### Update Continuous Query

```bash
PUT /api/v1/continuous_queries/{query_id}
```

**Request:**
```json
{
  "is_active": false,
  "retention_days": 730
}
```

### Delete Continuous Query

```bash
DELETE /api/v1/continuous_queries/{query_id}
```

**Optional Query Parameters:**
- `delete_destination=true` - Also delete destination measurement data

### Execute Continuous Query (Manual Trigger)

```bash
POST /api/v1/continuous_queries/{query_id}/execute
```

**Request:**
```json
{
  "start_time": "2025-10-20T00:00:00Z",
  "end_time": "2025-10-24T00:00:00Z",
  "dry_run": false
}
```

**Response:**
```json
{
  "query_id": 1,
  "query_name": "cpu_hourly_avg",
  "execution_id": "cq-exec-12345",
  "status": "completed",
  "start_time": "2025-10-20T00:00:00Z",
  "end_time": "2025-10-24T00:00:00Z",
  "records_read": 34560000,
  "records_written": 1440,
  "execution_time_seconds": 12.5,
  "destination_measurement": "cpu_1h",
  "dry_run": false,
  "executed_at": "2025-10-24T16:30:00.000Z"
}
```

### Get Execution History

```bash
GET /api/v1/continuous_queries/{query_id}/executions
```

**Response:**
```json
[
  {
    "execution_id": "cq-exec-12345",
    "query_id": 1,
    "status": "completed",
    "start_time": "2025-10-20T00:00:00Z",
    "end_time": "2025-10-24T00:00:00Z",
    "records_read": 34560000,
    "records_written": 1440,
    "execution_time_seconds": 12.5,
    "executed_at": "2025-10-24T16:30:00.000Z"
  }
]
```

---

## Configuration

### Enable Continuous Queries

```toml
# arc.conf
[continuous_queries]
enabled = true

# Maximum concurrent query executions
max_concurrent = 2

# Query timeout (seconds)
timeout = 3600

# Temp directory for staging aggregated data before writing
temp_dir = "./data/cq_temp"
```

---

## Examples

### Example 1: Hourly CPU Average

```json
{
  "name": "cpu_hourly",
  "source_measurement": "cpu",
  "destination_measurement": "cpu_1h",
  "query": "SELECT time_bucket(INTERVAL '1 hour', time) as time, host, AVG(usage_idle) as usage_idle_avg, AVG(usage_user) as usage_user_avg, COUNT(*) as count FROM cpu WHERE time >= {start_time} AND time < {end_time} GROUP BY 1, 2",
  "interval": "1h",
  "retention_days": 365
}
```

### Example 2: Daily Request Summaries

```json
{
  "name": "requests_daily",
  "source_measurement": "http_requests",
  "destination_measurement": "http_requests_daily",
  "query": "SELECT time_bucket(INTERVAL '1 day', time) as time, endpoint, COUNT(*) as total_requests, AVG(response_time) as avg_response_time, MAX(response_time) as max_response_time, SUM(bytes_sent) as total_bytes FROM http_requests WHERE time >= {start_time} AND time < {end_time} GROUP BY 1, 2",
  "interval": "1d",
  "retention_days": 730
}
```

### Example 3: 5-Minute IoT Sensor Averages

```json
{
  "name": "sensors_5m",
  "source_measurement": "temperature",
  "destination_measurement": "temperature_5m",
  "query": "SELECT time_bucket(INTERVAL '5 minutes', time) as time, sensor_id, location, AVG(value) as temp_avg, MIN(value) as temp_min, MAX(value) as temp_max FROM temperature WHERE time >= {start_time} AND time < {end_time} GROUP BY 1, 2, 3",
  "interval": "5m",
  "retention_days": 90
}
```

---

## Storage Impact

### Before Continuous Queries
```
Raw data: 10s interval, 7 days retention
- 6 samples/minute × 60 minutes × 24 hours × 7 days = 60,480 records per week
- Storage: ~60K records × 100 bytes = 6 MB per metric
```

### After Continuous Queries
```
Raw data: 10s interval, 7 days retention
- Storage: 6 MB

Downsampled (1 hour): 365 days retention
- 24 samples/day × 365 days = 8,760 records per year
- Storage: ~8.7K records × 100 bytes = 870 KB per metric

Total storage: 6 MB + 870 KB = 6.87 MB
Query speed for yearly dashboards: 10-50x faster!
```

---

## Best Practices

1. **Start with conservative intervals** - Begin with hourly aggregations before going to 5-minute
2. **Keep source data** - Don't delete source data until you've verified aggregations
3. **Test with dry-run** - Always test queries with dry_run=true first
4. **Monitor execution time** - Long-running queries may need optimization
5. **Use appropriate aggregations** - AVG for metrics, SUM for counters, MAX/MIN for ranges
6. **Preserve tags** - Include important tag columns in GROUP BY
7. **Add sample counts** - Include COUNT(*) to know how many samples were aggregated

---

## Limitations (Core Version)

- Manual execution only (no automatic scheduling)
- No backfill automation (must specify time ranges manually)
- Single query execution at a time per CQ (no parallel backfill)
- No alerting on CQ failures

## Enterprise Features (Reserved)

- Automatic execution on schedule (cron-like)
- Smart backfill (automatically detect gaps)
- Parallel execution for faster backfill
- CQ execution monitoring and alerting
- Advanced aggregation functions (percentiles, histograms)
- Cross-database queries
