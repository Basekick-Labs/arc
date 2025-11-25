# Arc Connection Pool Fix - Deployment Guide

## Problem Summary

Arc production deployments were experiencing connection pool exhaustion with error:
```
"No available connections after 5.0s"
```

## Root Cause

A broken connection pool implementation (`duckdb_pool_simple.py`) that was never tracked in git got into production containers. This implementation had a critical bug where failed connection cleanups would **permanently remove connections from the pool** instead of returning them.

## The Fix

**Commit:** `a4ef316` - "fix: Prevent connection pool deadlock by recreating connections immediately"

### Changes Made

1. **Fixed `api/duckdb_pool.py`**:
   - Recreate connections immediately after closing (for large queries >5000 rows)
   - Never put `None` into the connection pool (prevents deadlock)
   - Removed None-handling logic from `get_connection()`

2. **Removed `api/duckdb_pool_simple.py`**:
   - This file should NOT exist in any deployment
   - It was a failed experiment that never worked correctly

3. **Ensured `api/duckdb_engine.py` imports correctly**:
   ```python
   from api.duckdb_pool import DuckDBConnectionPool, QueryPriority
   ```
   NOT:
   ```python
   from api.duckdb_pool_simple import SimpleDuckDBPool  # ❌ WRONG
   ```

## Files to Check in Your Deployment

### ✅ Required Files (from git):
- `api/duckdb_pool.py` (the correct, fixed pool)
- `api/duckdb_engine.py` (imports DuckDBConnectionPool)

### ❌ Files That Should NOT Exist:
- `api/duckdb_pool_simple.py` (broken implementation)
- `api/__pycache__/duckdb_pool_simple*.pyc` (compiled bytecode)

## Deployment Checklist

When deploying to a new environment or updating existing deployments:

1. **Pull latest code**: `git pull origin main`
2. **Verify files**:
   ```bash
   # Should exist
   ls api/duckdb_pool.py
   ls api/duckdb_engine.py

   # Should NOT exist
   ls api/duckdb_pool_simple.py 2>/dev/null && echo "❌ REMOVE THIS FILE" || echo "✅ Good"
   ```

3. **Check import in engine**:
   ```bash
   grep "from api.duckdb_pool" api/duckdb_engine.py
   # Should show: from api.duckdb_pool import DuckDBConnectionPool, QueryPriority
   ```

4. **Rebuild container from scratch**:
   ```bash
   docker-compose down
   docker-compose build --no-cache  # Important: --no-cache
   docker-compose up -d
   ```

5. **Verify deployment**:
   ```bash
   # Test simple query
   curl -X POST https://YOUR_DOMAIN/api/v1/query \
     -H "Authorization: Bearer YOUR_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT 1 as test", "format": "json"}'

   # Should return: {"success":true,...}
   ```

## Quick Fix for Running Containers (Temporary)

If you need to fix a running container without rebuilding:

```bash
# 1. Copy correct files to container
scp api/duckdb_pool.py user@server:/tmp/
scp api/duckdb_engine.py user@server:/tmp/

# 2. Copy into container and remove broken file
docker cp /tmp/duckdb_pool.py CONTAINER_ID:/app/api/
docker cp /tmp/duckdb_engine.py CONTAINER_ID:/app/api/
docker exec CONTAINER_ID rm -f /app/api/duckdb_pool_simple.py
docker exec CONTAINER_ID rm -f /app/api/__pycache__/duckdb_pool_simple*.pyc

# 3. Restart container
docker restart CONTAINER_ID
```

## Verification After Deployment

Run 10 concurrent queries to verify the pool doesn't exhaust:

```bash
for i in {1..10}; do
  curl -s -X POST https://YOUR_DOMAIN/api/v1/query \
    -H "Authorization: Bearer YOUR_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"sql\": \"SELECT $i as test\", \"format\": \"json\"}" &
done
wait

# All should return: {"success":true,...}
# None should return: {"detail":"No available connections..."}
```

## Technical Details

### How the Fixed Pool Works

1. **Connection Pool Size**: Default 5 connections per worker (configurable via `DUCKDB_POOL_SIZE`)
2. **Large Query Handling**: Queries returning >5000 rows trigger connection recreation to release DuckDB's internal memory
3. **Connection Recreation**: Happens immediately, synchronously, before returning to pool (no `None` values ever enter the pool)
4. **Health Checks**: Unhealthy connections are detected and recreated on-demand
5. **Timeout**: 5 seconds default wait for available connection

### Pool Metrics

Monitor pool health via the metrics endpoint:
```bash
curl https://YOUR_DOMAIN/api/v1/metrics
```

Look for:
- `pool_size`: Should match your config (default: 5)
- `active_connections`: Should be between 0 and pool_size
- `total_queries_timeout`: Should be 0 or very low

## Common Issues

### Issue: Still getting "No available connections"

**Possible causes:**
1. Old `duckdb_pool_simple.py` still in container
2. Python cached bytecode (.pyc) still loading old pool
3. Container not fully restarted after file changes

**Solution:**
```bash
# Remove all pool-related cache
docker exec CONTAINER_ID find /app -name "*duckdb_pool*.pyc" -delete
docker restart CONTAINER_ID
```

### Issue: Import errors after deployment

**Error:** `ImportError: cannot import name 'SimpleDuckDBPool'`

**Solution:** Your `duckdb_engine.py` still has the wrong import. Update it:
```python
# Change this:
from api.duckdb_pool_simple import SimpleDuckDBPool

# To this:
from api.duckdb_pool import DuckDBConnectionPool, QueryPriority
```

## Related Commits

- `a4ef316` - Fix connection pool deadlock (THE FIX)
- `7639aa9` - Add aggressive GC and worker recycling
- `eb73bce` - Close DuckDB cursor to prevent memory leak

## Questions?

If you encounter issues:
1. Check the container logs for "Connection pool" messages
2. Verify no `duckdb_pool_simple` references in logs
3. Ensure pool is using `DuckDBConnectionPool` not `SimpleDuckDBPool`
