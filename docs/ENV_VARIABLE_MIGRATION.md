# Environment Variable Naming - Migration Guide

**Date:** 2025-11-05
**Issue:** Inconsistent environment variable names vs arc.conf keys
**Status:** ✅ Fixed with backward compatibility

---

## Problem

Environment variable names were inconsistent with `arc.conf` configuration keys, causing confusion:

| arc.conf Key | Old Env Var (Inconsistent) | What Users Expected |
|--------------|---------------------------|---------------------|
| `buffer_size` | `WRITE_BUFFER_SIZE` ❌ | `BUFFER_SIZE` ✅ |
| `buffer_age_seconds` | `WRITE_BUFFER_AGE` ❌ | `BUFFER_AGE_SECONDS` ✅ |
| `compression` | `WRITE_COMPRESSION` ❌ | `COMPRESSION` ✅ |

**Real-world confusion:**

```yaml
# User sets this (thinking it matches arc.conf):
environment:
  - BUFFER_AGE_SECONDS=5  # ❌ Didn't work!

# But Arc expected this:
environment:
  - WRITE_BUFFER_AGE=5  # ✅ Worked, but unexpected name
```

---

## Solution

**New consistent naming** that matches `arc.conf` keys, with **backward compatibility** for existing deployments.

### Priority Order (Highest to Lowest)

For each setting, Arc checks environment variables in this order:

1. **New consistent name** (e.g., `BUFFER_AGE_SECONDS`)
2. **Legacy name** (e.g., `WRITE_BUFFER_AGE`) - for backward compatibility
3. **arc.conf value** (e.g., `buffer_age_seconds = 5`)
4. **Built-in default** (e.g., `60`)

---

## New Environment Variable Names

### Ingestion Settings

| arc.conf Key | ✅ New Env Var (Recommended) | ⚠️ Legacy Env Var (Still Works) |
|--------------|------------------------------|----------------------------------|
| `buffer_size` | `BUFFER_SIZE` | `WRITE_BUFFER_SIZE` |
| `buffer_age_seconds` | `BUFFER_AGE_SECONDS` | `WRITE_BUFFER_AGE` |
| `compression` | `COMPRESSION` | `WRITE_COMPRESSION` |

### Example

```yaml
# docker-compose.yml

# ✅ NEW RECOMMENDED WAY (matches arc.conf keys)
services:
  arc:
    environment:
      - BUFFER_SIZE=17000
      - BUFFER_AGE_SECONDS=5
      - COMPRESSION=snappy

# ⚠️ OLD WAY (still works for backward compatibility)
services:
  arc:
    environment:
      - WRITE_BUFFER_SIZE=17000
      - WRITE_BUFFER_AGE=5
      - WRITE_COMPRESSION=snappy
```

---

## Migration Guide

### No Action Required! 🎉

**Existing deployments continue to work** without any changes. The old environment variable names are still supported.

### Recommended: Update to New Names

When convenient, update your configurations to use the new consistent names:

#### Before (Old Names)
```yaml
# docker-compose.yml
services:
  arc:
    environment:
      - WRITE_BUFFER_SIZE=17000
      - WRITE_BUFFER_AGE=5
      - WRITE_COMPRESSION=snappy
```

#### After (New Consistent Names)
```yaml
# docker-compose.yml
services:
  arc:
    environment:
      - BUFFER_SIZE=17000
      - BUFFER_AGE_SECONDS=5
      - COMPRESSION=snappy
```

### Benefits of Updating

1. **Consistency:** Env var names match arc.conf keys
2. **Predictability:** Easier to remember (no "WRITE_" prefix)
3. **Future-proof:** Old names may be deprecated in Arc 2.0

---

## Full Environment Variable Reference

### Server Configuration

| arc.conf Section | Key | Environment Variable | Type | Default |
|------------------|-----|---------------------|------|---------|
| `[server]` | `host` | `ARC_HOST`, `HOST` | string | `0.0.0.0` |
| `[server]` | `port` | `ARC_PORT`, `PORT` | int | `8000` |
| `[server]` | `workers` | `ARC_WORKERS`, `WORKERS` | int | `4` |

### Authentication

| arc.conf Section | Key | Environment Variable | Type | Default |
|------------------|-----|---------------------|------|---------|
| `[auth]` | `enabled` | `AUTH_ENABLED` | bool | `true` |
| `[auth]` | `default_token` | `DEFAULT_API_TOKEN` | string | `""` |
| `[auth]` | `allowlist` | `AUTH_ALLOWLIST` | string | `/health,/ready` |
| `[auth]` | `cache_ttl` | `AUTH_CACHE_TTL` | int | `30` |

### Query Cache

| arc.conf Section | Key | Environment Variable | Type | Default |
|------------------|-----|---------------------|------|---------|
| `[query_cache]` | `enabled` | `QUERY_CACHE_ENABLED` | bool | `true` |
| `[query_cache]` | `ttl_seconds` | `QUERY_CACHE_TTL` | int | `60` |
| `[query_cache]` | `max_size` | `QUERY_CACHE_MAX_SIZE` | int | `100` |
| `[query_cache]` | `max_result_mb` | `QUERY_CACHE_MAX_RESULT_MB` | int | `10` |

### DuckDB

| arc.conf Section | Key | Environment Variable | Type | Default |
|------------------|-----|---------------------|------|---------|
| `[duckdb]` | `pool_size` | `DUCKDB_POOL_SIZE` | int | `5` |
| `[duckdb]` | `max_queue_size` | `DUCKDB_MAX_QUEUE_SIZE` | int | `100` |
| `[duckdb]` | `enable_object_cache` | `DUCKDB_ENABLE_OBJECT_CACHE` | bool | `true` |

### Ingestion (NEW NAMES!)

| arc.conf Section | Key | ✅ New Env Var | ⚠️ Legacy Env Var | Type | Default |
|------------------|-----|---------------|------------------|------|---------|
| `[ingestion]` | `buffer_size` | `BUFFER_SIZE` | `WRITE_BUFFER_SIZE` | int | `10000` |
| `[ingestion]` | `buffer_age_seconds` | `BUFFER_AGE_SECONDS` | `WRITE_BUFFER_AGE` | int | `60` |
| `[ingestion]` | `compression` | `COMPRESSION` | `WRITE_COMPRESSION` | string | `snappy` |

### Write-Ahead Log (WAL)

| arc.conf Section | Key | Environment Variable | Type | Default |
|------------------|-----|---------------------|------|---------|
| `[wal]` | `enabled` | `WAL_ENABLED` | bool | `false` |
| `[wal]` | `dir` | `WAL_DIR` | string | `./data/wal` |
| `[wal]` | `sync_mode` | `WAL_SYNC_MODE` | string | `fdatasync` |
| `[wal]` | `max_size_mb` | `WAL_MAX_SIZE_MB` | int | `100` |
| `[wal]` | `max_age_seconds` | `WAL_MAX_AGE_SECONDS` | int | `3600` |

### Compaction

| arc.conf Section | Key | Environment Variable | Type | Default |
|------------------|-----|---------------------|------|---------|
| `[compaction]` | `enabled` | `COMPACTION_ENABLED` | bool | `true` |
| `[compaction]` | `min_age_hours` | `COMPACTION_MIN_AGE_HOURS` | int | `1` |
| `[compaction]` | `min_files` | `COMPACTION_MIN_FILES` | int | `50` |
| `[compaction]` | `target_file_size_mb` | `COMPACTION_TARGET_FILE_SIZE_MB` | int | `512` |
| `[compaction]` | `schedule` | `COMPACTION_SCHEDULE` | string | `*/10 * * * *` |
| `[compaction]` | `max_concurrent_jobs` | `COMPACTION_MAX_CONCURRENT_JOBS` | int | `4` |

### Delete Operations

| arc.conf Section | Key | Environment Variable | Type | Default |
|------------------|-----|---------------------|------|---------|
| `[delete]` | `enabled` | `DELETE_ENABLED` | bool | `false` |
| `[delete]` | `confirmation_threshold` | `DELETE_CONFIRMATION_THRESHOLD` | int | `10000` |
| `[delete]` | `max_rows_per_delete` | `DELETE_MAX_ROWS` | int | `1000000` |

---

## Testing the Migration

### Verify Old Names Still Work

```bash
# Set old environment variable
export WRITE_BUFFER_AGE=5

# Start Arc
python -m uvicorn api.main:app

# Check logs - should see:
# INFO: Loaded configuration with buffer_age_seconds=5
```

### Test New Names

```bash
# Set new environment variable
export BUFFER_AGE_SECONDS=5

# Start Arc
python -m uvicorn api.main:app

# Check logs - should see:
# INFO: Loaded configuration with buffer_age_seconds=5
```

### Test Priority Order

```bash
# Set both (new name takes priority)
export BUFFER_AGE_SECONDS=5
export WRITE_BUFFER_AGE=10

# Start Arc - should use 5 (new name wins)
```

---

## Deprecation Timeline

**Now (Arc 1.x):**
- ✅ Both old and new names work
- ✅ New names have priority
- ✅ No breaking changes

**Future (Arc 2.0):**
- ⚠️ Old names may show deprecation warnings
- ⚠️ Consider removing legacy names in major version bump

---

## Common Issues

### Issue: "My BUFFER_AGE_SECONDS isn't working!"

**Diagnosis:**
```bash
# Check priority order
echo "BUFFER_AGE_SECONDS: $BUFFER_AGE_SECONDS"
echo "WRITE_BUFFER_AGE: $WRITE_BUFFER_AGE"

# Check arc.conf
grep buffer_age_seconds arc.conf

# Check if arc.conf is being loaded
docker logs arc 2>&1 | grep "Loaded configuration"
```

**Solution:**
1. Make sure only ONE is set (or use new name for priority)
2. Verify arc.conf is mounted if using file config
3. Check logs to confirm which value Arc is using

### Issue: "Still using 60 seconds default"

**Diagnosis:**
Arc is falling back to defaults, meaning:
- Environment variables not set correctly
- arc.conf not found/not loaded
- Typo in environment variable name

**Solution:**
```bash
# Check if arc.conf exists and is loaded
docker exec arc cat /app/arc.conf

# Check environment variables inside container
docker exec arc printenv | grep -E 'BUFFER|WRITE'

# Check Arc logs
docker logs arc 2>&1 | grep -E "buffer_age|BUFFER_AGE"
```

---

## Summary

✅ **New consistent naming:** `BUFFER_SIZE`, `BUFFER_AGE_SECONDS`, `COMPRESSION`
✅ **Backward compatible:** Old names (`WRITE_BUFFER_AGE`, etc.) still work
✅ **No migration required:** Existing deployments continue to work
✅ **Recommended:** Update to new names when convenient

**Priority order:**
1. New env var (e.g., `BUFFER_AGE_SECONDS`)
2. Legacy env var (e.g., `WRITE_BUFFER_AGE`)
3. arc.conf value
4. Built-in default

This change makes Arc's configuration more predictable and easier to understand!
