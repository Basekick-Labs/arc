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

### Query Governance (Rate Limiting & Quotas)

Arc Enterprise now includes per-token query governance with sliding window rate limiting and hourly/daily query quotas. This gives teams resource predictability and prevents runaway queries from impacting cluster performance.

**Capabilities:**
- Per-token **rate limiting** (requests/minute, requests/hour) using a sliding window algorithm
- Per-token **query quotas** (max queries/hour, max queries/day)
- Per-token **max rows per query** — queries stop early and return partial results with a warning
- Per-token **timeout override** — enforce shorter timeouts for specific tokens
- **Default policies** via config — apply limits to all tokens without explicit policies
- **Hot-reconfigurable** — policy changes take effect immediately without restart

**Configuration:**
```toml
[governance]
enabled = true
default_rate_limit_per_min = 60      # 0 = unlimited
default_rate_limit_per_hour = 1000
default_max_queries_per_hour = 500
default_max_queries_per_day = 5000
default_max_rows_per_query = 100000
```

**API endpoints (admin-only, license-gated):**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/governance/policies` | Create policy for a token |
| `GET` | `/api/v1/governance/policies` | List all policies |
| `GET` | `/api/v1/governance/policies/:token_id` | Get policy for token |
| `PUT` | `/api/v1/governance/policies/:token_id` | Update policy |
| `DELETE` | `/api/v1/governance/policies/:token_id` | Delete policy |
| `GET` | `/api/v1/governance/usage/:token_id` | Current usage + remaining quota |

**Rate limited responses return HTTP 429 with `Retry-After` header:**
```json
{
  "success": false,
  "error": "Rate limit exceeded: 60 queries per minute",
  "retry_after_sec": 3
}
```

**Requires:** Enterprise license with `query_governance` feature.

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

### Automatic Writer Failover (< 30s RTO)

Arc Enterprise clusters now support automatic single-writer failover. When the primary writer node fails, a healthy standby writer is automatically promoted via Raft consensus. The failover manager monitors writer health, selects the best standby, and coordinates promotion across the cluster.

**Key capabilities:**
- Health-based failure detection (3 consecutive failed checks)
- Standby writer selection with automatic Raft-based promotion
- Cooldown period to prevent flapping
- Manual failover trigger support
- Router automatically routes writes to the new primary

**Configuration:**
```toml
[cluster]
failover_enabled = true
failover_timeout = 30       # seconds
failover_cooldown = 60      # seconds between failovers
```

**Requires:** Enterprise license with `writer_failover` feature.

### Cluster-Wide Core Limit Enforcement

Enterprise license core limits (`MaxCores`) are now enforced cluster-wide instead of per-node. This ensures the licensed core count applies to the total cores across all nodes in the cluster.

**How it works:**
- Each node reports its core count (`runtime.GOMAXPROCS(0)`) when joining the cluster
- The cluster leader validates that adding a new node won't exceed the license `MaxCores` limit
- Nodes that would cause the cluster to exceed the limit are rejected with a clear error message

**Example with Enterprise license (128 cores):**
| Configuration | Total Cores | Allowed? |
|---------------|-------------|----------|
| 4 nodes × 32 cores | 128 | ✅ Yes |
| 2 nodes × 64 cores | 128 | ✅ Yes |
| 4 nodes × 64 cores | 256 | ❌ Rejected |

**Cluster status now shows core usage:**
```json
{
  "total_cores": 64,
  "max_cores": 128,
  "cores_remaining": 64
}
```

**Note:** Per-node GOMAXPROCS limiting is still applied as defense-in-depth. The `Unlimited` tier (`MaxCores=0`) skips cluster-wide validation.

### Long-Running Query Management

Arc Enterprise now includes real-time query tracking, cancellation, and history. Administrators can monitor active queries, cancel runaway queries mid-flight, and review recent query history — all through a REST API.

**Capabilities:**
- **Active query tracking** — every query is assigned a unique ID and tracked in-memory with start time, SQL text, token info, and execution status
- **Query cancellation** — cancel any running query by ID via `DELETE /api/v1/queries/:id`, which propagates cancellation to DuckDB through Go's context mechanism
- **Query history** — configurable ring buffer (default: 100) of recently completed, cancelled, failed, and timed-out queries
- **Client correlation** — `X-Arc-Query-ID` response header returned on every tracked query for client-side tracking
- **Parallel query fix** — parallel executor now respects query timeout (previously used `context.Background()` with no timeout)

**Configuration:**
```toml
[query_management]
enabled = true
history_size = 100        # Ring buffer size for completed query history
```

**API endpoints (admin-only, license-gated):**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/queries/active` | List all currently running queries |
| `GET` | `/api/v1/queries/history?limit=50` | Recent completed/cancelled/failed queries |
| `GET` | `/api/v1/queries/:id` | Get specific query details |
| `DELETE` | `/api/v1/queries/:id` | Cancel a running query |

**Requires:** Enterprise license with `query_management` feature.

### Tiered Storage: Daily-Compacted-Only Migration Gate

The tiered storage migrator now only moves daily-compacted files to cold tier (S3 GLACIER / Azure Archive). Raw ingestion files and hourly-compacted files are skipped, ensuring fewer, larger objects land in archive storage.

**Before:** All files exceeding `hot_max_age_days` were migrated regardless of compaction status, resulting in many small files in cold storage — increasing object count, S3 costs, and cold-tier query latency.

**After:** Only files with the `_daily.parquet` suffix are eligible for cold migration. Since the daily compaction schedule runs well within the `hot_max_age_days` window, data is guaranteed to be fully compacted before moving to archive.

## New Metrics

| Metric | Description |
|--------|-------------|
| `arc_governance_rate_limited_total` | Queries rejected by rate limiting |
| `arc_governance_quota_exhausted_total` | Queries rejected by quota limits |
| `arc_governance_policies_active` | Number of active governance policies |
| `arc_audit_events_total` | Total audit events logged |
| `arc_audit_write_errors` | Audit log write failures |
| `arc_query_mgmt_active_queries` | Currently running tracked queries |
| `arc_query_mgmt_cancelled_total` | Total queries cancelled via management API |
| `arc_query_mgmt_history_size` | Completed queries in history buffer |

## Bug Fixes

### WAL Periodic Recovery Causes 2x Data Duplication (#199)

During normal runtime (no restarts), the periodic WAL maintenance goroutine replayed all entries from rotated WAL files — including entries already flushed to parquet by the normal buffer flush cycle. This caused every record to be stored exactly twice. On S3 backends with high ingestion volumes, this resulted in 2x the expected row counts.

**Root cause:** The periodic goroutine unconditionally called `RecoverWithOptions()` on rotated WAL files, replaying data through the callback into ArrowBuffer, which then flushed to parquet a second time. The shutdown/restart path was already fixed (PR #173), but the runtime path was not.

**Fix:** The periodic goroutine now operates in two modes:
- **Normal operation:** Purges rotated WAL files older than a safe age threshold (`3 × MaxBufferAgeMS`, floor 30s) — their data is guaranteed to be in parquet already. No replay, no `FlushAll()`.
- **Storage failure recovery:** When a flush failure is detected (e.g., S3 outage), falls back to WAL replay to recover data cleared from buffers after the failed flush.

**Additional fixes:**
- Empty WAL files (7-byte header-only) that accumulated indefinitely are now cleaned up during recovery
- WAL purge methods consolidated to eliminate code duplication

**Credit:** Bug identified and initial fix contributed by [@khalid244](https://github.com/khalid244) in PR #199.

### S3 Flush Hang: Workers Block Forever on Slow/Unresponsive Storage (#197)

When S3 becomes slow or unresponsive, flush workers called `storage.Write()` with `context.Background()` — no timeout, no cancellation. This caused all flush workers to block indefinitely, preventing new data from being persisted, blocking age-based flushes for all measurements, and causing `Close()` to hang.

**Fix:** All storage write paths now use `context.WithTimeout` derived from the buffer's parent context with a configurable timeout (default 30s). `Close()` cancels in-flight writes immediately. Context cleanup is handled on queue-full drops and channel close to prevent leaks.

**Configuration:**
```toml
[ingest]
flush_timeout_seconds = 30  # default: 30s, 0 = no timeout
```

**Credit:** Bug identified and fix contributed by [@khalid244](https://github.com/khalid244) in PR #197.

### Daily Compaction Blocked for Backfilled Data (#187)

Daily compaction previously checked the **file creation timestamp** (extracted from filename) against a 24-hour cutoff for all partitions. This blocked compaction of backfilled historical data — files created today for data from years ago would be skipped because the files were "too new."

**Fix:** Partitions older than 7 days now bypass the file creation time check entirely, since no late-arriving data is expected for old partitions. Recent partitions (≤7 days) retain the check to protect IoT late-sync scenarios.

**Configuration:**
```toml
[compaction]
daily_skip_file_age_check_days = 7  # default: 7 (0 = disabled)
```

**Env var:** `ARC_COMPACTION_DAILY_SKIP_FILE_AGE_CHECK_DAYS`

### HTTP Write Timeout vs Query Timeout Mismatch (#185)

HTTP `WriteTimeout` (30s default) was lower than `query.timeout` (300s default), causing long-running queries to fail with a connection timeout before the result could be written back. Arc now auto-syncs `WriteTimeout` to match `query.timeout` at startup when the mismatch is detected, with a warning log.

## Improvements

### LIKE Query Predicate Optimization

Queries combining `LIKE '%pattern%'` with empty string checks (`col <> ''`) are now automatically optimized by reordering WHERE clause predicates to put cheaper operations first.

**How it works:**
- Empty string checks (`col <> ''`) are O(1) per row
- `LIKE '%pattern%'` scans are O(n*m) where n=string length, m=pattern length
- DuckDB evaluates predicates left-to-right and can short-circuit
- By filtering out empty strings first, fewer expensive LIKE operations are executed

**Example transformation:**
```sql
-- Original query
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c
FROM hits
WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10

-- Automatically rewritten to
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c
FROM hits
WHERE SearchPhrase <> '' AND Title LIKE '%Google%' AND URL NOT LIKE '%.google.%'
GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

**Performance results (ClickBench Q23):**
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Avg latency | 1106ms | 967ms | **12.6% faster** |

**Key features:**
- Transparent to applications - no query changes required
- Fast-path: queries without LIKE or WHERE skip optimization entirely
- Only reorders when beneficial (predicates must contain LIKE)
- Works with both LIKE and NOT LIKE patterns

### Per-Database Compaction (#184)

The compaction trigger API now supports an optional `database` parameter to compact a single database instead of all databases. This is useful for large deployments where you want to prioritize compaction for high-traffic databases or trigger compaction after a bulk import to a specific database.

```bash
# Compact only the "production" database
curl -X POST "http://localhost:8000/api/v1/compaction/trigger?database=production&tier=hourly" \
  -H "Authorization: Bearer $TOKEN"
```

When no `database` parameter is provided, compaction runs across all databases as before (backward compatible).

- **Audit log SQLite storage**: Added `auto_vacuum = INCREMENTAL` to prevent database file bloat. Runs full `VACUUM` on startup and incremental vacuum after retention cleanup deletes old entries.

## Upgrade Notes

1. **Bulk import** is available immediately with no configuration required. The endpoints respect the existing `server.max_payload_size` setting (default: 1GB) for upload size limits.

## Dependencies

- **github.com/gofiber/fiber/v2**: 2.52.10 → 2.52.11 — defensive copying bug fixes, limiter middleware improvements, mounted sub-app error handler fix
