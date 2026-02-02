# Arc 2026.03.1 Release Notes

## New Features

### Bulk Import API (CSV & Parquet)

Arc now supports bulk importing data from CSV and Parquet files via dedicated API endpoints. Files are uploaded via multipart form data, processed by DuckDB, and written directly into Arc's partitioned Parquet storage layout — bypassing the ingestion buffer for maximum throughput on large imports.

**Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/import/csv` | Import a CSV file |
| `POST` | `/api/v1/import/parquet` | Import a Parquet file |
| `GET` | `/api/v1/import/stats` | Import handler statistics |

**CSV import example:**
```bash
curl -X POST "http://localhost:8000/api/v1/import/csv?measurement=cpu" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-arc-database: production" \
  -F "file=@cpu_metrics.csv"
```

**Parquet import example:**
```bash
curl -X POST "http://localhost:8000/api/v1/import/parquet?measurement=events" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-arc-database: analytics" \
  -F "file=@events_export.parquet"
```

**Response:**
```json
{
  "status": "ok",
  "result": {
    "database": "production",
    "measurement": "cpu",
    "rows_imported": 1500000,
    "partitions_created": 24,
    "time_range_min": "2025-01-01 00:00:00",
    "time_range_max": "2025-01-01 23:59:59",
    "columns": ["time", "host", "usage_idle", "usage_system"],
    "duration_ms": 3400
  }
}
```

**CSV options:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `measurement` | *(required)* | Target measurement name |
| `time_column` | `time` | Column containing timestamps |
| `time_format` | auto-detect | `epoch_s`, `epoch_ms`, `epoch_us`, `epoch_ns` |
| `delimiter` | `,` | CSV field delimiter |
| `skip_rows` | `0` | Rows to skip before header |

**Key features:**
- **DuckDB-powered** — leverages DuckDB's native CSV/Parquet parsing with automatic type inference
- **Automatic hourly partitioning** — data is split into Arc's standard `YYYY/MM/DD/HH/` layout
- **Time column renaming** — non-standard time column names are automatically mapped to `time`
- **All storage backends** — works with local filesystem, S3, and Azure Blob Storage
- **Auth & RBAC** — same authentication and permission model as write endpoints

**Documentation:** See [docs/bulk-import.md](docs/bulk-import.md) for full usage guide.

## Enterprise Features

### Audit Logging

Arc Enterprise now includes structured audit logging that records authentication events, RBAC changes, data access, and administrative actions to SQLite. All audit data is queryable via REST API.

**Tracked events:**

| Event Type | Trigger |
|------------|---------|
| `auth.failed` | 401/403 responses |
| `token.created` | Token creation |
| `token.deleted` | Token deletion |
| `token.rotated` | Token rotation |
| `rbac.{resource}.{created,updated,deleted}` | RBAC changes (orgs, teams, roles, memberships) |
| `data.query` | Query execution |
| `data.write` | Data ingestion |
| `data.import` | Data import |
| `data.delete` | Data deletion |
| `database.created` | Database creation |
| `database.deleted` | Database deletion |
| `mqtt.*` | MQTT configuration changes |
| `compaction.triggered` | Manual compaction triggers |
| `tiering.*` | Tiering configuration changes |

**Configuration:**
```toml
[audit_log]
enabled = true
retention_days = 90          # Auto-cleanup of old entries
include_reads = false        # Log GET/query requests (high volume)
```

**Query audit logs:**
```bash
# Get recent auth failures
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/audit/logs?event_type=auth.failed&limit=50"

# Get all events for a specific actor
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/audit/logs?actor=deploy-token&since=2026-02-01T00:00:00Z"

# Get event stats for last 24 hours
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/audit/stats"
```

**API endpoints (admin-only, license-gated):**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/audit/logs` | Query audit logs with filters |
| `GET` | `/api/v1/audit/stats` | Aggregate counts by event type |

**Query parameters for `/api/v1/audit/logs`:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `event_type` | string | Filter by event type |
| `actor` | string | Filter by actor (token name) |
| `database` | string | Filter by database name |
| `since` | RFC3339 | Start time filter |
| `until` | RFC3339 | End time filter |
| `limit` | int | Max results (default: 100, max: 10000) |
| `offset` | int | Pagination offset |

**Requires:** Enterprise license with `audit_logging` feature.

### Tiered Storage: Daily-Compacted-Only Migration Gate

The tiered storage migrator now only moves daily-compacted files to cold tier (S3 GLACIER / Azure Archive). Raw ingestion files and hourly-compacted files are skipped, ensuring fewer, larger objects land in archive storage.

**Before:** All files exceeding `hot_max_age_days` were migrated regardless of compaction status, resulting in many small files in cold storage — increasing object count, S3 costs, and cold-tier query latency.

**After:** Only files with the `_daily.parquet` suffix are eligible for cold migration. Since the daily compaction schedule runs well within the `hot_max_age_days` window, data is guaranteed to be fully compacted before moving to archive.

## New Metrics

| Metric | Description |
|--------|-------------|
| `arc_audit_events_total` | Total audit events logged |
| `arc_audit_write_errors` | Audit log write failures |

## Bug Fixes

*None yet*

## Improvements

- **Audit log SQLite storage**: Added `auto_vacuum = INCREMENTAL` to prevent database file bloat. Runs full `VACUUM` on startup and incremental vacuum after retention cleanup deletes old entries.

## Upgrade Notes

1. **Bulk import** is available immediately with no configuration required. The endpoints respect the existing `server.max_payload_size` setting (default: 1GB) for upload size limits.

## Dependencies

*No new dependencies*
