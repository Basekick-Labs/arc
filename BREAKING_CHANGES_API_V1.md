# API Endpoint Standardization - Breaking Changes

**Date:** 2025-10-21
**Version:** Arc v1.0.0 (Pre-release)
**Impact:** All API endpoints have been standardized to `/api/v1/...` pattern

## Overview

All Arc API endpoints have been migrated to a consistent `/api/v1/...` structure. This change affects **external integrations** including:
- Superset database dialect
- VS Code extension
- Telegraf output plugin
- Custom scripts and automation

## Endpoint Changes

### Write Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/write` | `/api/v1/write/line-protocol` | CHANGED |
| `/write/v1/msgpack` | `/api/v1/write/msgpack` | CHANGED |
| `/api/v1/msgpack` | `/api/v1/write/msgpack` | REMOVED (duplicate) |
| `/api/v1/write` | `/api/v1/write` | NO CHANGE |
| `/api/v1/write/influxdb` | `/api/v1/write/influxdb` | NO CHANGE |
| `/write/health` | `/api/v1/write/health` | CHANGED |
| `/write/stats` | `/api/v1/write/stats` | CHANGED |
| `/write/flush` | `/api/v1/write/flush` | CHANGED |
| `/write/v1/msgpack/stats` | `/api/v1/write/msgpack/stats` | CHANGED |
| `/write/v1/msgpack/spec` | `/api/v1/write/msgpack/spec` | CHANGED |

### Query Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/query` | `/api/v1/query` | CHANGED |
| `/query/estimate` | `/api/v1/query/estimate` | CHANGED |
| `/query/stream` | `/api/v1/query/stream` | CHANGED |
| `/query/arrow` | `/api/v1/query/arrow` | CHANGED |
| `/query/{measurement}` | `/api/v1/query/{measurement}` | CHANGED |
| `/query/{measurement}/csv` | `/api/v1/query/{measurement}/csv` | CHANGED |
| `/api/v1/query` | `/api/v1/query` | NO CHANGE (from line_protocol_routes) |

### Auth Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/auth/verify` | `/api/v1/auth/verify` | CHANGED |
| `/auth/tokens` | `/api/v1/auth/tokens` | CHANGED |
| `/auth/tokens/{id}` | `/api/v1/auth/tokens/{id}` | CHANGED |
| `/auth/tokens/{id}/rotate` | `/api/v1/auth/tokens/{id}/rotate` | CHANGED |
| `/auth/cache/stats` | `/api/v1/auth/cache/stats` | CHANGED |
| `/auth/cache/invalidate` | `/api/v1/auth/cache/invalidate` | CHANGED |

### Connection Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/connections/datasource` | `/api/v1/connections/datasource` | CHANGED |
| `/connections/influx` | `/api/v1/connections/influx` | CHANGED |
| `/connections/storage` | `/api/v1/connections/storage` | CHANGED |
| `/connections/http_json` | `/api/v1/connections/http_json` | CHANGED |
| `/connections/{type}/{id}/activate` | `/api/v1/connections/{type}/{id}/activate` | CHANGED |
| `/connections/{type}/{id}` | `/api/v1/connections/{type}/{id}` | CHANGED |
| `/connections/{type}/test` | `/api/v1/connections/{type}/test` | CHANGED |
| `/api/http-json/connections` | `/api/v1/http-json/connections` | CHANGED |

### Job Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/jobs` | `/api/v1/jobs` | CHANGED |
| `/jobs/{id}` | `/api/v1/jobs/{id}` | CHANGED |
| `/jobs/{id}/executions` | `/api/v1/jobs/{id}/executions` | CHANGED |
| `/jobs/{id}/cancel` | `/api/v1/jobs/{id}/cancel` | CHANGED |
| `/jobs/{id}/run` | `/api/v1/jobs/{id}/run` | CHANGED |
| `/monitoring/jobs` | `/api/v1/monitoring/jobs` | CHANGED |

### Compaction Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/status` | `/api/v1/compaction/status` | CHANGED |
| `/stats` | `/api/v1/compaction/stats` | CHANGED |
| `/candidates` | `/api/v1/compaction/candidates` | CHANGED |
| `/trigger` | `/api/v1/compaction/trigger` | CHANGED |
| `/jobs` | `/api/v1/compaction/jobs` | CHANGED |
| `/history` | `/api/v1/compaction/history` | CHANGED |

### WAL Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/status` | `/api/v1/wal/status` | CHANGED |
| `/stats` | `/api/v1/wal/stats` | CHANGED |
| `/files` | `/api/v1/wal/files` | CHANGED |
| `/cleanup` | `/api/v1/wal/cleanup` | CHANGED |
| `/health` | `/api/v1/wal/health` | CHANGED |
| `/recovery/history` | `/api/v1/wal/recovery/history` | CHANGED |

### Metrics & Monitoring Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/metrics` | `/api/v1/metrics` | CHANGED |
| `/metrics/timeseries/{type}` | `/api/v1/metrics/timeseries/{type}` | CHANGED |
| `/metrics/endpoints` | `/api/v1/metrics/endpoints` | CHANGED |
| `/metrics/query-pool` | `/api/v1/metrics/query-pool` | CHANGED |
| `/metrics/memory` | `/api/v1/metrics/memory` | CHANGED |
| `/logs` | `/api/v1/logs` | CHANGED |

### Cache Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/cache/stats` | `/api/v1/cache/stats` | CHANGED |
| `/cache/health` | `/api/v1/cache/health` | CHANGED |
| `/cache/clear` | `/api/v1/cache/clear` | CHANGED |

### Avro Schema Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/avro/schemas` | `/api/v1/avro/schemas` | CHANGED |
| `/avro/schemas/{id}` | `/api/v1/avro/schemas/{id}` | CHANGED |
| `/avro/schemas/topic/{name}` | `/api/v1/avro/schemas/topic/{name}` | CHANGED |

### Measurements Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/measurements` | `/api/v1/measurements` | CHANGED |

### Setup Endpoints

| Old Endpoint | New Endpoint | Status |
|---|---|---|
| `/setup/default-connections` | `/api/v1/setup/default-connections` | CHANGED |

### Unchanged Endpoints

These endpoints remain unchanged (standard patterns):
- `/health` - Health check
- `/ready` - Readiness check
- `/` - Root/info endpoint
- `/docs` - OpenAPI documentation
- `/openapi.json` - OpenAPI schema

## Migration Guide

### For Telegraf Output Plugin

Update your Telegraf configuration:

**Old:**
```toml
[[outputs.http]]
  url = "http://arc:8000/write"
  data_format = "influx"
```

**New:**
```toml
[[outputs.http]]
  url = "http://arc:8000/api/v1/write/line-protocol"
  data_format = "influx"
```

### For VS Code Extension

Update API calls in extension code:

**Old:**
```typescript
const response = await fetch(`${baseUrl}/query`, {
  method: 'POST',
  body: JSON.stringify({ sql: query })
});
```

**New:**
```typescript
const response = await fetch(`${baseUrl}/api/v1/query`, {
  method: 'POST',
  body: JSON.stringify({ sql: query })
});
```

### For Superset Database Dialect

Update the Arc SQLAlchemy dialect:

**Old:**
```python
def do_ping(self, connection):
    return connection.execute(text("SELECT 1")).fetchone() is not None

# Query endpoint
query_url = f"{base_url}/query"
```

**New:**
```python
def do_ping(self, connection):
    return connection.execute(text("SELECT 1")).fetchone() is not None

# Query endpoint
query_url = f"{base_url}/api/v1/query"
```

### For Custom Scripts

**Python Example:**

**Old:**
```python
import requests

response = requests.post(
    "http://arc:8000/query",
    headers={"x-api-key": token},
    json={"sql": "SELECT * FROM cpu LIMIT 10"}
)
```

**New:**
```python
import requests

response = requests.post(
    "http://arc:8000/api/v1/query",
    headers={"x-api-key": token},
    json={"sql": "SELECT * FROM cpu LIMIT 10"}
)
```

**JavaScript Example:**

**Old:**
```javascript
fetch('http://arc:8000/query', {
  method: 'POST',
  headers: {
    'x-api-key': token,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({ sql: 'SELECT * FROM cpu LIMIT 10' })
})
```

**New:**
```javascript
fetch('http://arc:8000/api/v1/query', {
  method: 'POST',
  headers: {
    'x-api-key': token,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({ sql: 'SELECT * FROM cpu LIMIT 10' })
})
```

**curl Example:**

**Old:**
```bash
curl -X POST http://arc:8000/query \
  -H "x-api-key: YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM cpu LIMIT 10"}'
```

**New:**
```bash
curl -X POST http://arc:8000/api/v1/query \
  -H "x-api-key: YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM cpu LIMIT 10"}'
```

## Verification

After updating your integrations, verify they work with the new endpoints:

```bash
# Test auth
curl http://arc:8000/api/v1/auth/verify \
  -H "x-api-key: YOUR_TOKEN"

# Test query
curl -X POST http://arc:8000/api/v1/query \
  -H "x-api-key: YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 as test"}'

# Test write (Line Protocol)
echo "test,tag=value field=123" | curl -X POST \
  http://arc:8000/api/v1/write/line-protocol \
  -H "x-api-key: YOUR_TOKEN" \
  --data-binary @-

# Test health (unchanged)
curl http://arc:8000/health
```

## Affected Repositories

The following repositories need to be updated:

1. **Telegraf Output Plugin**: `basekick-labs/telegraf-output-arc`
   - Update write endpoint URLs

2. **VS Code Extension**: `basekick-labs/arc-vscode`
   - Update all API calls to use `/api/v1/...` pattern

3. **Superset Dialect**: `basekick-labs/superset-arc-dialect`
   - Update query and connection test endpoints

4. **Example Scripts**: Update all documentation examples in:
   - `/docs/*.md` files
   - README examples
   - Integration guides

## Timeline

- **Effective Date**: Immediate (Arc v1.0.0)
- **Deprecation Period**: None (breaking change)
- **Support for Old Endpoints**: None

## Questions & Support

For questions or issues related to this migration:
- GitHub Issues: https://github.com/basekick-labs/arc/issues
- Discord: Arc Community Server

---

**Note**: This is a one-time breaking change to establish a consistent API structure before Arc v1.0 general release. All future endpoint changes will follow proper versioning and deprecation cycles.
