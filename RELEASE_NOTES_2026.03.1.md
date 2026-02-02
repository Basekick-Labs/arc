# Arc 2026.03.1 Release Notes

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

### Tiered Storage: Daily Compaction Gate

Tiered storage migration now only moves daily-compacted files (`*_daily.parquet`) to cold tier. This prevents migrating raw or hourly-compacted files that haven't been fully consolidated, ensuring optimal storage efficiency and query performance on cold tier.

**Impact:** Files must complete daily compaction before becoming eligible for cold tier migration. This guarantees that cold tier contains only fully compacted data, reducing file count and improving query performance on S3/Azure.

## New Metrics

| Metric | Description |
|--------|-------------|
| `arc_audit_events_total` | Total audit events logged |
| `arc_audit_write_errors` | Audit log write failures |
