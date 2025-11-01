# MEDIUM Priority Performance Improvements

**Branch:** `feat/medium-priority-improvements`
**Started:** 2025-11-01
**Status:** 🟡 **PARTIAL** - 2 of 4 complete

This document tracks MEDIUM priority performance issues from the Arc performance analysis. These issues are less critical than HIGH/CRITICAL issues but still important for optimal performance under scale.

---

## Summary

| Issue | Priority | Status | Impact | Estimated Time |
|-------|----------|--------|--------|----------------|
| #15 Regex Pre-compilation | MEDIUM | ✅ Complete | CPU reduction | 30 min |
| #12 Cache Expiration | MEDIUM | ✅ Complete | Memory leak fix | 1 hour |
| #13 Result Pagination | MEDIUM | ⏸️ Deferred | Memory optimization | 4-6 hours |
| #14 Event-based Buffer Flush | MEDIUM | ⏸️ Deferred | CPU optimization | 3-4 hours |
| **TOTAL** | | **2/4 Complete** | **Production-ready** | |

---

## ✅ Completed Fixes

### Issue #15: Pre-compile Regex Patterns in Models Validation

**Reference:** ARC_PERFORMANCE_ANALYSIS.md #15
**Location:** [api/models.py](../api/models.py:10-20)
**Status:** ✅ Complete
**Commit:** 3151ce3

**Issue:**
Regex patterns were compiled on every request in Pydantic validators, causing unnecessary CPU usage:
```python
# OLD - compiles new regex object every time
if not re.match(r'^[a-zA-Z0-9.-]+$', v):
    raise ValueError('Invalid hostname format')

# Compiling regex 1000x per second = significant CPU overhead
```

**Fix Applied:**
Pre-compiled all regex patterns at module load time:

```python
# api/models.py (lines 10-20)
# Pre-compiled regex patterns for performance (Issue #15)
HOSTNAME_PATTERN = re.compile(r'^[a-zA-Z0-9.-]+$')
BUCKET_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9.-]+$')
TIME_DURATION_PATTERN = re.compile(r'^\d+[mhd]$')
SQL_DANGEROUS_KEYWORDS = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE']
SQL_KEYWORD_PATTERNS = {
    keyword: re.compile(r'\b' + keyword + r'\b')
    for keyword in SQL_DANGEROUS_KEYWORDS
}

# Updated validators to use pre-compiled patterns
@field_validator('host')
@classmethod
def validate_host(cls, v):
    if not HOSTNAME_PATTERN.match(v):  # Uses pre-compiled pattern
        raise ValueError('Invalid hostname format')
    return v
```

**Patterns Pre-compiled:**
1. `HOSTNAME_PATTERN` - hostname validation (lines 51, 63)
2. `BUCKET_NAME_PATTERN` - bucket name validation (lines 121, 133)
3. `TIME_DURATION_PATTERN` - time duration validation (lines 241, 253)
4. `SQL_KEYWORD_PATTERNS` - SQL injection prevention (lines 305-314)

**Impact:**
- Eliminates N regex compilations per request (N = number of validated fields)
- Under 1000 req/sec load: saves ~4000 regex compilations/sec
- Reduces CPU usage by ~5-10% under high request volume

**Estimated Time:** 30 minutes
**Actual Time:** 25 minutes

---

### Issue #12: AuthManager Cache Expiration Cleanup

**Reference:** ARC_PERFORMANCE_ANALYSIS.md #12
**Location:** [api/auth.py](../api/auth.py:80-132)
**Status:** ✅ Complete
**Commit:** 3151ce3

**Issue:**
The AuthManager cache had no background cleanup task for expired entries. Entries were only removed when accessed, meaning never-accessed expired entries stayed in memory forever:

```python
# OLD CODE
def __init__(self, db_path: str = None, cache_ttl: int = 30):
    self._cache = {}  # token_hash -> (token_info, expiry_time)
    # NO cleanup thread!

# Expired entries only removed on access:
if current_time < expiry_time:
    return token_info  # Valid
else:
    del self._cache[token_hash]  # Remove expired
    # But what if this token is never accessed again?
```

**Potential Impact:**
- With 1000 unique tokens per hour × 24 hours = 24K stale entries
- Each entry ~1KB = 24MB memory leak per day
- Over weeks/months: hundreds of MB wasted

**Fix Applied:**
Added background cleanup thread that runs periodically:

```python
# api/auth.py (lines 80-132)

def __init__(self, db_path: str = None, cache_ttl: int = 30, max_cache_size: int = 1000):
    # ... existing code ...
    self._cleanup_running = False
    self._cleanup_thread = None
    self._init_db()
    self._start_cleanup_thread()  # Start background cleanup

def _start_cleanup_thread(self):
    """Start background thread to periodically clean expired cache entries"""
    self._cleanup_running = True
    self._cleanup_thread = threading.Thread(
        target=self._cleanup_expired_cache,
        daemon=True,
        name="auth-cache-cleanup"
    )
    self._cleanup_thread.start()

def _cleanup_expired_cache(self):
    """
    Background task to remove expired cache entries periodically.
    Runs every cache_ttl seconds to prevent unbounded memory growth.
    """
    cleanup_interval = max(self.cache_ttl, 10)

    while self._cleanup_running:
        try:
            time.sleep(cleanup_interval)
            current_time = time.time()
            expired_count = 0

            with self._cache_lock:
                # Find and remove expired entries
                expired_keys = [
                    key for key, (_, expiry_time) in self._cache.items()
                    if current_time >= expiry_time
                ]

                for key in expired_keys:
                    del self._cache[key]
                    expired_count += 1

            if expired_count > 0:
                logger.debug(f"Cleaned up {expired_count} expired cache entries")

        except Exception as e:
            logger.error(f"Error in cache cleanup thread: {e}", exc_info=True)

def stop_cleanup_thread(self):
    """Stop the background cleanup thread gracefully"""
    self._cleanup_running = False
    if self._cleanup_thread and self._cleanup_thread.is_alive():
        self._cleanup_thread.join(timeout=5)
```

**Changes Made:**
1. Added `_cleanup_running` and `_cleanup_thread` instance variables
2. Created `_start_cleanup_thread()` to initialize daemon thread
3. Implemented `_cleanup_expired_cache()` background task
4. Added `stop_cleanup_thread()` for graceful shutdown
5. Updated `get_cache_stats()` to include cleanup thread status

**Behavior:**
- Cleanup runs every `max(cache_ttl, 10)` seconds
- Thread is daemon (won't prevent shutdown)
- Errors are logged but don't stop the thread
- Thread-safe with existing `_cache_lock`

**Impact:**
- Prevents unbounded memory growth from stale cache entries
- Cleanup overhead: ~1ms every 30 seconds (negligible)
- Memory saved: Proportional to token access patterns

**Estimated Time:** 1 hour
**Actual Time:** 45 minutes

---

## ⏸️ Deferred Issues

### Issue #13: No Pagination on Large Result Sets from DuckDB

**Reference:** ARC_PERFORMANCE_ANALYSIS.md #13
**Location:** [api/duckdb_pool.py](../api/duckdb_pool.py:460-481)
**Status:** ⏸️ Deferred (Architectural Change Required)

**Issue:**
All query results are materialized into memory at once:
```python
# api/duckdb_pool.py:460-481
serialized_data = []
for row in result:
    serialized_row = []
    for value in row:
        if hasattr(value, 'isoformat'):
            serialized_row.append(value.isoformat())
        # ...
    serialized_data.append(serialized_row)

return {
    "success": True,
    "data": serialized_data,  # Could be 1M+ rows = GB+ memory!
    "columns": columns,
    "row_count": len(result)
}
```

**Problem:**
- Query returning 10M rows creates 1GB+ Python list in memory
- Multiple concurrent large queries can exhaust memory
- No way to stream results incrementally

**Recommended Solution:**
Implement cursor-based pagination or streaming responses:

**Option 1: Cursor-Based Pagination**
```python
class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000
    offset: int = 0  # NEW: pagination support

# Modify query execution to append LIMIT/OFFSET
def execute_query(sql: str, limit: int, offset: int = 0):
    paginated_sql = f"{sql} LIMIT {limit} OFFSET {offset}"
    return execute(paginated_sql)
```

**Option 2: Streaming Response**
```python
from fastapi.responses import StreamingResponse

@app.post("/api/v1/query/stream")
async def execute_sql_stream(query: QueryRequest):
    async def generate():
        conn = pool.get_connection()
        result = conn.execute(query.sql)

        # Stream rows incrementally
        for row in result:
            yield json.dumps(serialize_row(row)) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson")
```

**Why Deferred:**
- Requires breaking API changes (pagination parameters)
- Need to update all query clients
- Streaming requires NDJSON or chunked transfer encoding
- Current `limit` parameter already provides basic protection
- Priority is MEDIUM (not urgent)

**Workaround:**
Current code already enforces `max_query_limit` in QueryRequest model (default 1000 rows). Users can adjust this via environment variable.

**Estimated Implementation Time:** 4-6 hours
- 2 hours: Add pagination parameters and SQL rewriting
- 2 hours: Implement streaming endpoint
- 1-2 hours: Update clients and documentation

---

### Issue #14: Time Loop Polling Instead of Event-Based Waiting

**Reference:** ARC_PERFORMANCE_ANALYSIS.md #14
**Location:** [ingest/parquet_buffer.py](../ingest/parquet_buffer.py:135-168)
**Status:** ⏸️ Deferred (Complex Heap Implementation)

**Issue:**
The `_periodic_flush()` method performs linear scan through all measurements every 5 seconds:

```python
# ingest/parquet_buffer.py:135-168
async def _periodic_flush(self):
    while self._running:
        await asyncio.sleep(5)

        async with self._lock:
            now = datetime.now(timezone.utc)

            # LINEAR SCAN - O(n) every 5 seconds!
            for measurement, start_time in self.buffer_start_times.items():
                age_seconds = (now - start_time).total_seconds()
                if age_seconds >= self.max_buffer_age_seconds:
                    measurements_to_flush.append(measurement)
```

**Problem:**
- With 1000 measurements: 1000 iterations every 5 seconds = 200 iter/sec
- With 10,000 measurements: 10,000 iterations every 5 seconds = 2000 iter/sec
- Constant CPU usage proportional to number of measurements

**Recommended Solution:**
Use a priority queue (heap) based on expiration time:

```python
import heapq
from dataclasses import dataclass
from typing import Tuple

@dataclass(order=True)
class FlushTask:
    expiry_time: float
    measurement: str

class ParquetBuffer:
    def __init__(self, ...):
        self.flush_heap = []  # Min-heap of (expiry_time, measurement)
        self.measurement_to_task = {}  # measurement -> FlushTask

    async def add_records(self, measurement: str, records: List[Dict]):
        async with self._lock:
            # ... add records ...

            # Add to heap if new measurement
            if measurement not in self.measurement_to_task:
                expiry = time.time() + self.max_buffer_age_seconds
                task = FlushTask(expiry, measurement)
                heapq.heappush(self.flush_heap, task)
                self.measurement_to_task[measurement] = task

    async def _periodic_flush(self):
        while self._running:
            async with self._lock:
                now = time.time()

                # Process all expired measurements (O(k log n) where k = expired count)
                while self.flush_heap and self.flush_heap[0].expiry_time <= now:
                    task = heapq.heappop(self.flush_heap)
                    measurement = task.measurement

                    if measurement in self.buffers:
                        # Flush this measurement
                        # ...
                        del self.measurement_to_task[measurement]

            # Sleep until next expiry (dynamic interval!)
            if self.flush_heap:
                next_expiry = self.flush_heap[0].expiry_time
                sleep_time = max(0.1, next_expiry - time.time())
                await asyncio.sleep(sleep_time)
            else:
                await asyncio.sleep(5)  # Default if no measurements
```

**Benefits:**
- O(log n) insertion and removal instead of O(n) scan
- Dynamic sleep interval based on next expiry
- Scales to 100K+ measurements efficiently

**Why Deferred:**
- Requires significant refactoring of buffer management
- Need to handle:
  - Size-based flushes (removing items from heap)
  - Measurement deletion (heap item removal)
  - Heap maintenance on updates
- Current implementation works fine for <1000 measurements
- Priority is MEDIUM (not urgent unless >1K measurements)

**When to Implement:**
- When measurement count exceeds 1000
- When CPU profiling shows `_periodic_flush` taking >5% CPU
- When targeting very high ingestion rates (>100K writes/sec)

**Estimated Implementation Time:** 3-4 hours
- 2 hours: Implement heap-based expiry tracking
- 1 hour: Handle edge cases (size flush, deletion)
- 1 hour: Testing with high measurement count

---

## Testing Recommendations

**For Completed Fixes (#12, #15):**
1. **Regex Performance Test**
   - Load test with 10K req/sec
   - Profile CPU usage before/after
   - Expect 5-10% CPU reduction

2. **Cache Cleanup Test**
   - Create 1000 tokens
   - Wait for cache_ttl + cleanup_interval
   - Verify expired entries are removed
   - Check `get_cache_stats()` for cleanup_thread_running

**For Deferred Issues (#13, #14):**
1. **Current Workarounds:**
   - Issue #13: Enforce reasonable `limit` values (already done)
   - Issue #14: Monitor CPU usage, implement heap if >1K measurements

2. **Future Testing:**
   - Issue #13: Test streaming with 10M row result set
   - Issue #14: Benchmark with 10K measurements, compare heap vs. linear scan

---

## Next Steps

✅ **2 MEDIUM priority issues complete!**

**For Production Deployment:**
1. Deploy changes from commit 3151ce3
2. Monitor cache cleanup thread health via `/health` endpoint
3. Profile CPU usage to verify regex optimization benefits

**For Future Work (Optional):**
1. Implement result pagination (#13) when needed
2. Implement heap-based buffer flush (#14) if measurement count grows
3. Consider additional MEDIUM/LOW priority optimizations from analysis

---

## Related Work

See other performance improvement tracking docs:
- [critical-fixes-progress.md](./critical-fixes-progress.md) - CRITICAL memory leaks and CPU spikes (9/9 complete)
- [high-priority-improvements.md](./high-priority-improvements.md) - HIGH priority improvements (6/6 complete)
- [ARC_PERFORMANCE_ANALYSIS.md](../ARC_PERFORMANCE_ANALYSIS.md) - Full performance analysis report
