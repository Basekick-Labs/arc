# Memory Leak & Performance Issues - Action Plan

**Date:** 2025-11-01
**Analysis Report:** [ARC_PERFORMANCE_ANALYSIS.md](../ARC_PERFORMANCE_ANALYSIS.md)

## Quick Summary

Found **23 issues** across the codebase that could cause memory leaks, CPU spikes, or blocking operations:
- **5 CRITICAL** - Immediate action required
- **8 HIGH** - Address within 1-2 weeks
- **6 MEDIUM** - Schedule for next sprint
- **4 LOW** - Nice to have improvements

## Critical Issues (Fix Immediately)

### 1. Unclosed SQLite Connections ⚠️ **MEMORY LEAK**

**Files Affected:**
- `api/retention_routes.py` - 11 methods
- `api/continuous_query_routes.py` - 11 methods
- `api/compaction_lock.py` - Multiple methods

**Problem:** SQLite connections opened but not closed in exception paths.

**Impact:** Each error leaves a connection open → eventual resource exhaustion

**Fix:** Wrap all SQLite operations in context managers
```python
# BEFORE (BAD)
def create_policy(self, policy):
    conn = sqlite3.connect(self.db_path)
    cursor = conn.cursor()
    cursor.execute(...)
    conn.commit()
    conn.close()  # Missed if exception occurs!

# AFTER (GOOD)
def create_policy(self, policy):
    with sqlite3.connect(self.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(...)
        conn.commit()
    # Auto-closes even on exception
```

**Effort:** 4 hours
**Priority:** 🔴 CRITICAL

---

### 2. Unclosed DuckDB In-Memory Connections ⚠️ **MEMORY LEAK**

**File:** `api/delete_routes.py:156, 184`

**Problem:** DuckDB `:memory:` connections created but never closed

**Impact:** Memory leak on every DELETE operation

**Fix:**
```python
# BEFORE (BAD)
async def count_matching_rows(table, where_clause, engine):
    conn = duckdb.connect(':memory:')
    try:
        result = conn.execute(query).fetchone()
        return result[0]
    except Exception as e:
        raise HTTPException(...)
    # MISSING: conn.close()

# AFTER (GOOD)
async def count_matching_rows(table, where_clause, engine):
    conn = duckdb.connect(':memory:')
    try:
        result = conn.execute(query).fetchone()
        return result[0]
    except Exception as e:
        raise HTTPException(...)
    finally:
        conn.close()  # ALWAYS close
```

**Effort:** 1 hour
**Priority:** 🔴 CRITICAL

---

### 3. Event Loop Blocking in Connection Pool ⚠️ **CPU SPIKE**

**File:** `api/duckdb_pool.py:373-378`

**Problem:** Synchronous polling with `time.time()` in tight loop blocks event loop

**Impact:** 100% CPU spike during connection wait

**Fix:**
```python
# BEFORE (BAD)
start_wait = time.time()
while (time.time() - start_wait) < timeout:
    conn = self.get_connection(timeout=1.0)
    if conn:
        break
    # Tight loop, blocks event loop!

# AFTER (GOOD)
async def _wait_for_connection_async(self, timeout: float):
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < timeout:
        conn = self.get_connection(timeout=0.1)
        if conn:
            return conn
        await asyncio.sleep(0.1)  # Non-blocking!
    raise TimeoutError("No connection available")
```

**Effort:** 2 hours
**Priority:** 🔴 CRITICAL

---

### 4. Health Check Infinite Loop Without Backoff ⚠️ **CPU SPIKE**

**File:** `api/duckdb_pool.py:669-685`

**Problem:** Exception in health check causes tight loop with no delay

**Impact:** 100% CPU, log spam, application unresponsive

**Fix:**
```python
# BEFORE (BAD)
async def health_check_loop(self):
    while True:
        try:
            await asyncio.sleep(self.health_check_interval)
            # ... health check logic
        except Exception as e:
            logger.error(f"Health check error: {e}")
            # Falls through and retries immediately!

# AFTER (GOOD)
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
            # Exponential backoff on errors
            await asyncio.sleep(min(60, backoff))
            backoff = min(backoff * 2, 60)
```

**Effort:** 30 minutes
**Priority:** 🔴 CRITICAL

---

### 5. Query Cache Inaccurate Size Estimation ⚠️ **MEMORY LEAK**

**File:** `api/query_cache.py:78-105`

**Problem:** Assumes "100 bytes per cell" - highly inaccurate

**Impact:** Large results bypass limits, unbounded memory growth

**Fix:**
```python
# BEFORE (BAD)
def _estimate_size_mb(self, result):
    num_cells = len(data) * len(columns)
    bytes_estimate = num_cells * 100  # Wildly inaccurate!
    return bytes_estimate / (1024 * 1024)

# AFTER (GOOD)
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

**Effort:** 1 hour
**Priority:** 🔴 CRITICAL

---

## High Priority Issues (Next 1-2 Weeks)

### 6. Connection Pool Deadlock Risk

**File:** `api/duckdb_pool.py:282-317`
**Issue:** Nested locks in connection health check
**Fix:** Move `conn.close()` outside lock
**Effort:** 1 hour

### 7. No Timeout on Cloud Storage Operations

**Files:** `storage/minio_backend.py`, `storage/s3_backend.py`, `storage/gcs_backend.py`
**Issue:** Upload/download operations can hang indefinitely
**Fix:** Add `asyncio.wait_for()` with 5-minute timeout
**Effort:** 3 hours

### 8. Parquet Buffer Overflow in Error Paths

**File:** `ingest/arrow_writer.py:203-272`
**Issue:** Buffer not flushed on certain exceptions
**Fix:** Ensure flush in all exception paths
**Effort:** 2 hours

### 9. WAL Synchronous I/O Blocking Event Loop

**File:** `ingest/parquet_buffer.py:145-167`
**Issue:** `open()` and `write()` calls are synchronous
**Fix:** Use `aiofiles` for async file I/O
**Effort:** 3 hours

### 10. No Decompression Size Limit (DoS Vector)

**File:** `api/msgpack_routes.py:93-120`
**Issue:** Accepts unlimited compressed payloads
**Fix:** Add `max_decompressed_size` check (e.g., 100MB)
**Effort:** 1 hour

### 11. Auth Token Cache Never Expires

**File:** `api/auth.py:118-145`
**Issue:** Token validation cache grows indefinitely
**Fix:** Add TTL or LRU eviction to cache
**Effort:** 2 hours

### 12. Metrics Collection Errors Silently Ignored

**File:** `api/monitoring.py:127-157`
**Issue:** Exceptions swallowed without alerting
**Fix:** Add error counter and alert on sustained failures
**Effort:** 1 hour

### 13. Background Task Exceptions Not Logged

**File:** `api/main.py:567-581`
**Issue:** Startup tasks may fail silently
**Fix:** Wrap tasks with exception handlers
**Effort:** 1 hour

---

## Medium Priority Issues (Next Sprint)

### 14. No Pagination on Large Query Results
**File:** `api/main.py:1081-1138`
**Fix:** Add cursor-based pagination for >10k rows
**Effort:** 4 hours

### 15. Retention Policy O(n) Scan Every 5 Minutes
**File:** `api/retention_routes.py:279-308`
**Fix:** Index by next_run timestamp
**Effort:** 3 hours

### 16. Continuous Query Polling Without Backoff
**File:** `api/continuous_query_routes.py:280-320`
**Fix:** Add exponential backoff on query errors
**Effort:** 2 hours

### 17. Regex Recompilation on Every Request
**File:** `api/models.py:293-306`
**Fix:** Pre-compile regex patterns at module load
**Effort:** 30 minutes

### 18. Unbounded Metrics Dictionary Growth
**File:** `api/monitoring.py:68-93`
**Fix:** Add retention window (e.g., last 24 hours)
**Effort:** 2 hours

### 19. Connection Lifecycle Not Tracked in Metrics
**File:** `api/duckdb_pool.py`
**Fix:** Add connection creation/destruction counters
**Effort:** 1 hour

---

## Low Priority Issues (Nice to Have)

### 20. Large Parquet Files Not Streamed
**File:** `storage/local_backend.py:45-67`
**Fix:** Use streaming reads for >100MB files
**Effort:** 2 hours

### 21. No Rate Limiting on Write Endpoints
**File:** `api/line_protocol_routes.py`, `api/msgpack_routes.py`
**Fix:** Add per-IP rate limiting (e.g., 1000 req/min)
**Effort:** 3 hours

### 22. Missing Circuit Breaker for External Services
**Fix:** Add circuit breaker pattern for S3/MinIO/GCS
**Effort:** 4 hours

### 23. No Connection Pool Size Tuning Guidance
**Fix:** Document optimal pool size based on CPU cores
**Effort:** 1 hour

---

## Implementation Priority

### Week 1: Critical Fixes (Total: 8.5 hours)
1. ✅ Wrap SQLite connections in context managers (4h)
2. ✅ Close DuckDB in-memory connections (1h)
3. ✅ Replace event loop polling with async waiting (2h)
4. ✅ Add health check backoff (0.5h)
5. ✅ Fix query cache size estimation (1h)

### Week 2: High Priority (Total: 14 hours)
6. ✅ Fix connection pool deadlock risk (1h)
7. ✅ Add cloud storage timeouts (3h)
8. ✅ Fix parquet buffer overflow (2h)
9. ✅ Use async file I/O for WAL (3h)
10. ✅ Add decompression size limit (1h)
11. ✅ Add auth cache eviction (2h)
12. ✅ Log metrics collection errors (1h)
13. ✅ Handle background task exceptions (1h)

### Week 3: Medium Priority (Total: 12.5 hours)
14-19. Address medium priority issues

### Week 4+: Low Priority (Total: 10 hours)
20-23. Nice-to-have improvements

---

## Testing Plan

### Critical Fixes - Before Merging
- [ ] Unit tests for all fixed methods
- [ ] Memory leak test: Run 10k operations, check memory growth
- [ ] Load test: 1000 concurrent requests for 5 minutes
- [ ] Error injection test: Simulate network/disk failures

### Integration Testing
- [ ] Deploy to staging with production-like load
- [ ] Monitor for 48 hours:
  - Memory usage (should be flat)
  - CPU usage (no sustained spikes)
  - Error rates (no increase)
  - Response times (no degradation)

### Performance Benchmarks
- [ ] Before/after query latency (p50, p95, p99)
- [ ] Before/after memory usage (resident set size)
- [ ] Before/after CPU usage (average and peak)
- [ ] Connection pool exhaustion test

---

## Monitoring Checklist

Add alerts for:
- [ ] Memory growth >500MB/hour
- [ ] CPU >80% for >5 minutes
- [ ] Connection pool exhaustion (all connections busy)
- [ ] SQLite "database is locked" errors
- [ ] DuckDB connection creation failures
- [ ] Health check failures >3 consecutive
- [ ] Query cache eviction rate >50/minute
- [ ] Upload timeout rate >5% of requests

---

## Rollback Plan

All fixes should be:
1. Deployed behind feature flags where possible
2. Monitored for 24 hours before full rollout
3. Easily revertable via git revert
4. Documented with rollback procedures

**Critical fix rollback triggers:**
- Memory leak >1GB/hour
- Error rate increase >10%
- Response time degradation >50%
- Connection pool exhaustion
- Application crashes

---

## Additional Resources

- **Full Analysis Report:** [ARC_PERFORMANCE_ANALYSIS.md](../ARC_PERFORMANCE_ANALYSIS.md)
- **Query Cache Improvements:** [query-cache-improvements.md](query-cache-improvements.md)
- **Python Memory Profiling:** Use `memory_profiler` or `tracemalloc`
- **Load Testing:** Use `locust` or `k6` for stress testing

---

## Questions & Decisions

### Decision Needed: Context Manager Migration Strategy
**Option A:** Fix all SQLite methods in one PR (large changeset)
**Option B:** Fix one module at a time over multiple PRs (safer)

**Recommendation:** Option B - fix retention_routes.py first (most critical), then continuous queries, then compaction

### Decision Needed: Query Cache Behavior
Should oversized results:
1. Be rejected entirely? (current behavior)
2. Be stored but count extra toward size limit?
3. Be compressed before size check?

**Recommendation:** Option 3 - compress first, then check size (enables caching more queries)

---

**Status:** Ready for Implementation
**Owner:** TBD
**Target Completion:** 3 weeks
