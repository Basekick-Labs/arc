# Critical Memory Leak & CPU Spike Fixes - Progress

**Branch:** `fix/critical-memory-leaks-cpu-spikes`
**Started:** 2025-11-01
**Status:** In Progress (1 of 5 critical fixes complete)

## ✅ Completed Fixes

### 1. SQLite Connection Leaks in retention_routes.py
**Commit:** 59f51d7
**Files:** [api/retention_routes.py](../api/retention_routes.py)
**Methods Fixed:** 9 methods
- `_init_tables()`
- `create_policy()`
- `get_policies()`
- `get_policy()`
- `update_policy()`
- `delete_policy()`
- `get_executions()`
- `record_execution_start()`
- `record_execution_complete()`

**Fix Applied:** Wrapped all `sqlite3.connect()` calls in context managers (`with` statement)

**Before:**
```python
def create_policy(self, policy):
    conn = sqlite3.connect(self.db_path)
    cursor = conn.cursor()
    cursor.execute(...)
    conn.commit()
    conn.close()  # Missed if exception occurs!
```

**After:**
```python
def create_policy(self, policy):
    with sqlite3.connect(self.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(...)
        conn.commit()
    # Auto-closes even on exception
```

**Impact:** Prevents connection leaks under sustained errors (network failures, disk full, etc.)

---

## 🔄 In Progress

### 2. SQLite Connection Leaks in continuous_query_routes.py
**Status:** Pending
**Files:** `api/continuous_query_routes.py`
**Methods to Fix:** ~11 methods in `ContinuousQueryManager` class
**Estimated Time:** 30 minutes

**Same pattern as retention_routes.py** - wrap all SQLite connections in context managers.

---

### 3. SQLite Connection Leaks in compaction_lock.py
**Status:** Pending
**Files:** `api/compaction_lock.py`
**Methods to Fix:** Multiple methods in `CompactionLock` class
**Estimated Time:** 20 minutes

---

## 📋 Remaining Critical Fixes

### 4. DuckDB In-Memory Connection Leaks
**Status:** Pending
**Files:** `api/delete_routes.py` (lines 156, 184)
**Methods to Fix:**
- `count_matching_rows()`
- Other DELETE helper functions

**Fix Required:**
```python
async def count_matching_rows(table, where_clause, engine):
    conn = duckdb.connect(':memory:')
    try:
        result = conn.execute(query).fetchone()
        return result[0]
    finally:
        conn.close()  # ADD THIS
```

**Estimated Time:** 1 hour
**Impact:** Memory leak on every DELETE operation

---

### 5. Event Loop Blocking in Connection Pool
**Status:** Pending
**Files:** `api/duckdb_pool.py` (lines 373-378)
**Method:** Connection timeout polling logic

**Fix Required:** Replace synchronous polling with async waiting:
```python
# BEFORE (BAD - blocks event loop)
while (time.time() - start_wait) < timeout:
    conn = self.get_connection(timeout=1.0)
    if conn:
        break

# AFTER (GOOD - non-blocking)
async def _wait_for_connection_async(self, timeout: float):
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < timeout:
        conn = self.get_connection(timeout=0.1)
        if conn:
            return conn
        await asyncio.sleep(0.1)  # Non-blocking!
    raise TimeoutError()
```

**Estimated Time:** 2 hours
**Impact:** 100% CPU spike during connection wait

---

### 6. Health Check Infinite Loop Without Backoff
**Status:** Pending
**Files:** `api/duckdb_pool.py` (lines 669-685)
**Method:** `health_check_loop()`

**Fix Required:** Add exponential backoff on errors:
```python
async def health_check_loop(self):
    backoff = 1
    while True:
        try:
            await asyncio.sleep(self.health_check_interval)
            # ... health check logic
            backoff = 1  # Reset on success
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Health check error: {e}")
            await asyncio.sleep(min(60, backoff))  # ADD THIS
            backoff = min(backoff * 2, 60)  # Exponential backoff
```

**Estimated Time:** 30 minutes
**Impact:** CPU spike when health check fails

---

### 7. Query Cache Size Estimation Wrong
**Status:** Pending
**Files:** `api/query_cache.py` (lines 78-105)
**Method:** `_estimate_size_mb()`

**Fix Required:** Use accurate size measurement instead of "100 bytes per cell" estimate:
```python
def _estimate_size_mb(self, result):
    import sys
    data = result.get("data", [])
    if not data:
        return 0.0

    # Sample first 10 rows for accurate estimate
    sample_size = 0
    for row in data[:10]:
        sample_size += sum(sys.getsizeof(cell) for cell in row)

    # Extrapolate to all rows
    avg_row_size = sample_size / min(10, len(data))
    total_bytes = avg_row_size * len(data)
    return total_bytes / (1024 * 1024)
```

**Estimated Time:** 1 hour
**Impact:** Unbounded memory growth in query cache

---

## Summary

| Fix | Status | Time Estimate | Impact |
|-----|--------|---------------|--------|
| 1. SQLite (retention) | ✅ Complete | - | Memory leak fix |
| 2. SQLite (continuous query) | 🔄 Pending | 30 min | Memory leak fix |
| 3. SQLite (compaction) | 🔄 Pending | 20 min | Memory leak fix |
| 4. DuckDB in-memory leaks | 🔄 Pending | 1 hour | Memory leak fix |
| 5. Event loop blocking | 🔄 Pending | 2 hours | CPU spike fix |
| 6. Health check loop | 🔄 Pending | 30 min | CPU spike fix |
| 7. Query cache estimation | 🔄 Pending | 1 hour | Memory leak fix |
| **TOTAL** | **14% (1/7)** | **5.3 hours** | **All critical** |

---

## Testing Strategy

After all fixes are complete:

1. **Unit Tests** - Add tests for each fixed method
2. **Memory Leak Test** - Run 10k operations, monitor memory growth
3. **Load Test** - 1000 concurrent requests for 5 minutes
4. **Error Injection Test** - Simulate network/disk failures

---

## Next Steps

1. Continue with `continuous_query_routes.py` (same pattern as retention)
2. Fix `compaction_lock.py` (same pattern)
3. Fix DELETE route DuckDB leaks (add `finally: conn.close()`)
4. Fix event loop blocking (replace polling with async waiting)
5. Fix health check backoff (add exponential backoff)
6. Fix query cache estimation (use `sys.getsizeof()`)
7. Test all fixes before merging

---

**Estimated Completion:** 5-6 hours of focused work
**Priority:** CRITICAL - These are memory leaks and CPU spikes in production code
