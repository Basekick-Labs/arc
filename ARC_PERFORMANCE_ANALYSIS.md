# Arc Codebase Performance & Memory Analysis Report

## Executive Summary

The Arc codebase demonstrates generally good practices for a high-performance time-series database, with careful attention to async I/O, connection pooling, and buffer management. However, several critical issues have been identified that could lead to memory leaks, CPU spikes, and blocking operations under high load conditions.

**Critical Issues Found: 5**  
**High Priority Issues: 8**  
**Medium Priority Issues: 6**  
**Low Priority Issues: 4**

---

## CRITICAL ISSUES

### 1. Unclosed SQLite Database Connections Without Context Managers

**Location:** Multiple files with synchronous database access patterns:
- `/Users/nacho/dev/basekick-labs/arc/api/retention_routes.py` (lines 103-137, 147-165, etc.)
- `/Users/nacho/dev/basekick-labs/arc/api/continuous_query_routes.py` (lines 116-158, 168-191, etc.)
- `/Users/nacho/dev/basekick-labs/arc/api/compaction_lock.py` (lines 42-55, 91-102, etc.)

**Issue Description:**  
Multiple database operations create SQLite connections and only close them in some code paths. While many methods have `conn.close()` calls, there are execution paths where exceptions occur before closing.

**Example Code (retention_routes.py:147-165):**
```python
def create_policy(self, policy: RetentionPolicyRequest) -> int:
    try:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(...)
        policy_id = cursor.lastrowid
        conn.commit()
        conn.close()  # GOOD
        
        return policy_id
    
    except sqlite3.IntegrityError as e:
        if "UNIQUE constraint" in str(e):
            raise ValueError(...)
        raise  # PROBLEM: conn.close() never called on error path!
```

**Potential Impact:**
- **CRITICAL Memory Leak:** Each exception leaves a connection open. Under sustained errors (network failures, disk full, etc.), connections accumulate
- **Resource Exhaustion:** SQLite can run out of file handles, causing complete application failure
- **Cascading Failures:** New requests fail with "database is locked" errors even though application is still running

**Severity:** CRITICAL

**Recommended Fix:**
```python
def create_policy(self, policy: RetentionPolicyRequest) -> int:
    try:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(...)
            policy_id = cursor.lastrowid
            conn.commit()  # Context manager auto-closes
            return policy_id
    except sqlite3.IntegrityError as e:
        if "UNIQUE constraint" in str(e):
            raise ValueError(f"Policy '{policy.name}' already exists")
        raise
```

Apply this pattern to all methods in:
- `RetentionPolicyManager` (11 methods)
- `ContinuousQueryManager` (11 methods)  
- `CompactionLock` (multiple methods)

---

### 2. Unclosed DuckDB In-Memory Connections

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/delete_routes.py` (lines 184, 156)

**Issue Description:**
```python
async def count_matching_rows(table: pa.Table, where_clause: str, engine) -> int:
    import duckdb
    conn = duckdb.connect(':memory:')  # Creates in-memory database
    try:
        conn.register('temp_table', table)
        query = f"SELECT COUNT(*) as count FROM temp_table WHERE {where_clause}"
        result = conn.execute(query).fetchone()
        return result[0] if result else 0
    except Exception as e:
        ...
        raise HTTPException(...)
    # MISSING: conn.close() or context manager!
```

Also in `api/delete_routes.py:184` and potentially called multiple times per delete operation.

**Potential Impact:**
- **Memory Leak:** Each DELETE operation creates memory that's never released
- **CPU Spike:** DuckDB processes accumulate, consuming CPU without limit
- **GC Pressure:** Garbage collector must constantly clean up unreferenced DuckDB connections
- **Cascading Failure:** Eventually system becomes unresponsive

**Severity:** CRITICAL

**Recommended Fix:**
```python
async def count_matching_rows(table: pa.Table, where_clause: str, engine) -> int:
    import duckdb
    conn = duckdb.connect(':memory:')
    try:
        conn.register('temp_table', table)
        query = f"SELECT COUNT(*) as count FROM temp_table WHERE {where_clause}"
        result = conn.execute(query).fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Error counting matching rows: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid WHERE clause: {str(e)}")
    finally:
        conn.close()  # ALWAYS close
```

---

### 3. Synchronous Database Calls Blocking Event Loop in async_handler

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/duckdb_pool.py` (lines 373-378)

**Issue Description:**
The query queue polling loop uses blocking sleeps instead of async events:

```python
# Execute query timeout logic with SYNCHRONOUS sleep
start_wait = time.time()
while (time.time() - start_wait) < timeout:
    conn = self.get_connection(timeout=1.0)
    if conn:
        break
    
    # This is CPU-intensive polling
    # and blocks the event loop!
```

**Potential Impact:**
- **Event Loop Blocking:** `time.time()` calls in tight loop consume CPU
- **High CPU Spike:** 100% CPU on one core while waiting for connection
- **Slow Request Handling:** All async operations slower while polling occurs
- **Timeout Precision Loss:** Actual timeout may exceed specified timeout

**Severity:** CRITICAL (for high-concurrency scenarios)

**Recommended Fix:**
```python
# Use asyncio.wait_for with proper async waiting
async def execute_async(self, sql: str, priority: QueryPriority = ..., timeout: float = 300.0):
    conn = self.get_connection(timeout=0.1)
    
    if conn is None:
        try:
            conn = await asyncio.wait_for(
                self._wait_for_connection_async(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            self.total_queries_timeout += 1
            return {"success": False, "error": "..."}
    ...

async def _wait_for_connection_async(self):
    """Async waiting for connection (non-blocking)"""
    while True:
        conn = self.get_connection(timeout=0.1)
        if conn:
            return conn
        await asyncio.sleep(0.1)  # Non-blocking wait
```

---

### 4. Infinite Loop Without Timeout in Connection Pool Health Check

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/duckdb_pool.py` (lines 669-685)

**Issue Description:**
```python
async def health_check_loop(self):
    while True:  # INFINITE LOOP - no timeout, no cancellation handling!
        try:
            await asyncio.sleep(self.health_check_interval)
            logger.debug("Running connection pool health check")
            
            with self.pool_lock:
                for conn in self.connections:
                    if not conn.health_check():
                        logger.warning(...)
        
        except asyncio.CancelledError:
            logger.info("Health check loop cancelled")
            break  # OK, but...
        except Exception as e:
            logger.error(f"Health check error: {e}")
            # Falls through and continues loop without delay!
```

**Potential Impact:**
- **CPU Spike:** Exception in health_check causes tight loop with no delay
- **Log Spam:** Continuous error messages if health_check fails
- **Resource Exhaustion:** Retrying operations rapidly without backoff
- **Unresponsive Application:** All async tasks starved by health check loop

**Severity:** CRITICAL (when exception occurs)

**Recommended Fix:**
```python
async def health_check_loop(self):
    while True:
        try:
            await asyncio.sleep(self.health_check_interval)
            logger.debug("Running connection pool health check")
            
            with self.pool_lock:
                for conn in self.connections:
                    if not conn.health_check():
                        logger.warning(...)
        
        except asyncio.CancelledError:
            logger.info("Health check loop cancelled")
            break
        except Exception as e:
            logger.error(f"Health check error: {e}")
            # CRITICAL: Add exponential backoff on errors
            await asyncio.sleep(min(60, self.health_check_interval * 2))
```

---

### 5. Memory Accumulation in Query Cache Without Forced Eviction

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/query_cache.py` (lines 180-196)

**Issue Description:**
The query cache uses LRU eviction, but has a potential issue with the estimation logic:

```python
def set(self, sql: str, limit: int, result: Dict[str, Any]) -> bool:
    size_mb = self._estimate_size_mb(result)  # Rough estimate
    if size_mb > self.max_result_size_mb:
        return False
    
    key = self._make_key(sql, limit)
    
    with self.lock:
        # Evict oldest if cache is full
        if len(self.cache) >= self.max_size and key not in self.cache:
            oldest_key, _ = self.cache.popitem(last=False)
            self.evictions += 1
        
        # Store result
        self.cache[key] = (result, datetime.now())
        return True
```

**Problem:** The estimate assumes "avg 100 bytes per cell" which is highly inaccurate:
- A single large string field can be 10KB+
- Boolean fields: ~1 byte
- Timestamps: ~8 bytes  
- Floating point: ~8 bytes

**Result:** Large query results cached silently despite exceeding estimates, causing unbounded memory growth.

**Severity:** CRITICAL (memory leak under high query volume)

**Recommended Fix:**
```python
def _estimate_size_mb(self, result: Dict[str, Any]) -> float:
    try:
        import sys
        data = result.get("data", [])
        if not data:
            return 0.0
        
        # Use actual object size measurement
        total_size = sys.getsizeof(result)  # Recursive measurement
        
        # Sample first few rows for accurate estimate
        sample_rows = min(10, len(data))
        for row in data[:sample_rows]:
            total_size += sum(sys.getsizeof(cell) for cell in row)
        
        # Extrapolate
        if sample_rows > 0:
            avg_row_size = total_size / sample_rows
            total_size = avg_row_size * len(data)
        
        mb_estimate = total_size / (1024 * 1024)
        return mb_estimate
    except Exception as e:
        logger.warning(f"Failed to estimate result size: {e}")
        # Conservative: assume 1MB per 1000 rows
        return len(result.get("data", [])) / 1000.0
```

---

## HIGH PRIORITY ISSUES

### 6. Potential Connection Pool Deadlock with Nested Locks

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/duckdb_pool.py` (lines 282-317)

**Issue Description:**
```python
def get_connection(self, timeout: float = 5.0) -> Optional[DuckDBConnection]:
    try:
        conn = self.pool.get(timeout=timeout)
        
        if not conn.stats.is_healthy:
            if not conn.health_check():
                try:
                    conn.close()
                    conn = DuckDBConnection(...)
                    with self.pool_lock:  # LOCK 1
                        self.connections[conn.stats.connection_id] = conn
                except Exception as e:
                    logger.error(...)
                    self.pool.put(conn)  # Returns to pool
                    return None
        
        return conn
    except Empty:
        return None
```

**Issue:** If DuckDBConnection.close() acquires a lock, and we already hold pool_lock, potential for deadlock.

**Severity:** HIGH

**Recommended Fix:**
```python
def get_connection(self, timeout: float = 5.0) -> Optional[DuckDBConnection]:
    try:
        conn = self.pool.get(timeout=timeout)
        
        if not conn.stats.is_healthy:
            if not conn.health_check():
                try:
                    # Close OUTSIDE lock to prevent deadlock
                    conn.close()
                    new_conn = DuckDBConnection(...)
                    
                    # NOW acquire lock for state update
                    with self.pool_lock:
                        self.connections[new_conn.stats.connection_id] = new_conn
                    
                    return new_conn
                except Exception as e:
                    logger.error(...)
                    self.pool.put(conn)
                    return None
        
        return conn
    except Empty:
        return None
```

---

### 7. No Timeout on External Network Calls to Storage Backends

**Location:** Multiple storage backend files:
- `/Users/nacho/dev/basekick-labs/arc/storage/minio_backend.py`
- `/Users/nacho/dev/basekick-labs/arc/storage/s3_backend.py`
- `/Users/nacho/dev/basekick-labs/arc/storage/gcs_backend.py`

**Issue Description:**
Upload operations to cloud storage may hang indefinitely if network is flaky.

**Example Pattern:**
```python
async def upload_file(self, local_path: str, remote_path: str):
    # No timeout specified on S3/MinIO/GCS operations
    self.client.upload_file(local_path, remote_path)
    # If network hangs, this blocks entire write worker
```

**Potential Impact:**
- **Blocking:** Write operations hang, blocking buffer flushes
- **Memory Accumulation:** Buffers fill up while waiting for upload
- **Cascading Failure:** Application becomes unresponsive

**Severity:** HIGH

**Recommended Fix:**
```python
async def upload_file(self, local_path: str, remote_path: str, timeout: int = 300):
    """Upload with timeout (default 5 minutes for large files)"""
    try:
        # Use asyncio.wait_for to enforce timeout
        await asyncio.wait_for(
            self._upload_async(local_path, remote_path),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        logger.error(f"Upload timeout for {remote_path} after {timeout}s")
        raise
```

---

### 8. ParquetBuffer Error Path Re-adds Data Without Limit Check

**Location:** `/Users/nacho/dev/basekick-labs/arc/ingest/parquet_buffer.py` (lines 246-257)

**Issue Description:**
```python
except Exception as e:
    logger.error(f"Failed to flush measurement '{measurement}': {e}")
    self.total_errors += 1
    
    # Re-add records to buffer on error
    async with self._lock:
        # Check: only re-add if buffer isn't already too large
        if len(self.buffers[measurement]) < self.max_buffer_size * 2:
            self.buffers[measurement].extend(records)  # Could exceed 2x limit!
            logger.warning(f"Re-added {len(records)} records to buffer after flush error")
        else:
            logger.error(f"Dropping {len(records)} records - buffer at capacity")
            logger.error(f"This may indicate persistent storage issues...")
```

**Issue:** The check uses `<` (less than) but extends AFTER the check. If `len(buffer) == max_size * 2 - 1`, adding a large batch could exceed limits significantly.

**Severity:** HIGH

**Recommended Fix:**
```python
except Exception as e:
    logger.error(f"Failed to flush measurement '{measurement}': {e}")
    self.total_errors += 1
    self.consecutive_errors = getattr(self, 'consecutive_errors', 0) + 1
    
    # Implement exponential backoff - don't keep retrying immediately
    if self.consecutive_errors > 3:
        logger.error(f"Consecutive errors ({self.consecutive_errors}). Dropping records.")
        return
    
    # Re-add records to buffer with strict limit check
    async with self._lock:
        buffer_records = self.buffers[measurement]
        available_space = self.max_buffer_size * 2 - len(buffer_records)
        
        if available_space >= len(records):
            buffer_records.extend(records)
            logger.warning(f"Re-added {len(records)} records (buffer at {len(buffer_records)}/{self.max_buffer_size * 2})")
        else:
            # Drop excess records
            dropped = len(records) - available_space
            buffer_records.extend(records[:available_space])
            logger.error(f"Dropped {dropped} records due to buffer overflow")
```

---

### 9. WAL Write Synchronous Calls in Request Handler

**Location:** `/Users/nacho/dev/basekick-labs/arc/storage/wal.py` (lines 124-165)

**Issue Description:**
```python
def append(self, records: List[Dict[str, Any]]) -> bool:
    try:
        payload = msgpack.packb(records, ...)
        payload_len = len(payload)
        checksum = self._crc32(payload)
        timestamp_us = int(datetime.now().timestamp() * 1_000_000)
        entry = struct.pack('>IQI', payload_len, timestamp_us, checksum) + payload
        
        # BLOCKING synchronous I/O in potentially async context
        bytes_written = os.write(self.current_fd, entry)
        self.current_size += bytes_written
        
        # Then fsync - even more blocking!
        self._sync()
```

The `os.write()` and `fsync()` calls are synchronous and block the event loop when called from async handlers.

**Severity:** HIGH

**Recommended Fix:**
Ensure WAL writes are offloaded to thread pool:
```python
async def append_async(self, records: List[Dict[str, Any]]) -> bool:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, self.append, records)

# In request handlers:
await wal_writer.append_async(records)
```

---

### 10. No Resource Limits on Line Protocol Message Size

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/line_protocol_routes.py`

**Issue Description:**
The gzip decompression in `decode_body()` doesn't check decompressed size:

```python
async def decode_body(body: bytes, content_encoding: Optional[str] = None) -> str:
    if is_gzipped:
        loop = asyncio.get_event_loop()
        body = await loop.run_in_executor(None, _decompress_gzip_sync, body)
    
    # No check if decompressed body is 10GB or 100GB!
    return body.decode('utf-8')
```

**Attack Vector:** Client sends 100MB gzip → decompresses to 10GB → OOM kill

**Severity:** HIGH

**Recommended Fix:**
```python
async def decode_body(body: bytes, content_encoding: Optional[str] = None, max_decompressed_size: int = 500_000_000) -> str:
    """Decode with decompressed size limit (default 500MB)"""
    if is_gzipped:
        import io
        try:
            # Decompress with size check
            decompressed = gzip.decompress(body)
            if len(decompressed) > max_decompressed_size:
                raise ValueError(f"Decompressed size {len(decompressed)} exceeds {max_decompressed_size}")
            return decompressed.decode('utf-8')
        except gzip.BadGzipFile as e:
            logger.error(f"Invalid gzip data: {e}")
            raise HTTPException(status_code=400, detail="Invalid gzip")
    else:
        if len(body) > max_decompressed_size:
            raise HTTPException(status_code=413, detail="Payload too large")
        return body.decode('utf-8')
```

---

### 11. Unchecked `list.append()` in Monitoring Metrics

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/monitoring.py` (lines 280-289)

**Issue Description:**
```python
def _collection_loop(self):
    while not self._stop_event.is_set():
        try:
            now = datetime.now()
            system_metrics = self._collect_system_metrics(now)
            self.system_metrics.append(system_metrics)  # Using deque
            
            # If collection fails, silently swallows exception
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
        
        self._stop_event.wait(self.sample_interval_seconds)
```

The `self.system_metrics` uses `deque(maxlen=...)` so it auto-limits, but the code doesn't handle the case where metrics collection fails repeatedly—it just silently logs and moves on.

**Severity:** HIGH (combined with other issues)

---

### 12. Missing Cleanup in AuthManager Cache

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/auth.py` (lines 24-38)

**Issue Description:**
```python
class AuthManager:
    def __init__(self, db_path: str = None, cache_ttl: int = 30):
        self.cache_ttl = cache_ttl
        self._cache = {}  # token_hash -> (token_info, expiry_time)
        self._cache_lock = threading.Lock()
        self._cache_hits = 0
        self._cache_misses = 0
        self._init_db()
```

There's no background task to clean expired cache entries. Over time, the cache can grow indefinitely with expired tokens.

**Severity:** HIGH

**Recommended Fix:**
```python
def __init__(self, db_path: str = None, cache_ttl: int = 30):
    self.cache_ttl = cache_ttl
    self._cache = {}
    self._cache_lock = threading.Lock()
    self._init_db()
    
    # Start cache cleanup task
    self._cleanup_task = threading.Thread(target=self._cleanup_expired, daemon=True)
    self._cleanup_task.start()

def _cleanup_expired(self):
    """Remove expired cache entries periodically"""
    while True:
        time.sleep(60)  # Cleanup every minute
        with self._cache_lock:
            now = time.time()
            expired_keys = [k for k, (_, exp_time) in self._cache.items() if exp_time < now]
            for key in expired_keys:
                del self._cache[key]
            if expired_keys:
                logger.debug(f"Cleaned {len(expired_keys)} expired cache entries")
```

---

## MEDIUM PRIORITY ISSUES

### 13. No Pagination on Large Result Sets from DuckDB

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/duckdb_pool.py` (lines 448-459)

**Issue Description:**
All query results are serialized into memory at once:

```python
# Serialize data for JSON AFTER releasing connection
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
    "data": serialized_data,  # Could be 1M+ rows!
}
```

**Issue:** A query returning 10M rows creates a 1GB+ Python list in memory

**Severity:** MEDIUM

**Recommended Fix:**
Implement streaming responses or pagination:
```python
async def execute_async(self, sql: str, priority: QueryPriority = ..., limit: int = 10000, offset: int = 0):
    # Add LIMIT/OFFSET to SQL
    paginated_sql = f"{sql} LIMIT {limit} OFFSET {offset}"
    result, columns = await loop.run_in_executor(None, conn.execute, paginated_sql)
    
    # Serialize smaller result
    serialized_data = [...]
    
    return {
        "success": True,
        "data": serialized_data,
        "limit": limit,
        "offset": offset,
        "may_have_more": len(result) == limit
    }
```

---

### 14. Time Loop Polling Instead of Event-Based Waiting

**Location:** `/Users/nacho/dev/basekick-labs/arc/ingest/parquet_buffer.py` (lines 135-168)

**Issue Description:**
```python
async def _periodic_flush(self):
    while self._running:
        try:
            await asyncio.sleep(5)  # OK
            
            # But the checking loop inside...
            records_to_flush = []
            async with self._lock:
                now = datetime.now(timezone.utc)
                measurements_to_flush = []
                
                # Linear scan through ALL measurements every 5 seconds
                for measurement, start_time in self.buffer_start_times.items():
                    age_seconds = (now - start_time).total_seconds()
                    if age_seconds >= self.max_buffer_age_seconds:
                        measurements_to_flush.append(measurement)
```

**Issue:** If you have 1000 measurements, this loops 1000 times every 5 seconds = 200 loops/sec × 1000 measurements = constant CPU usage

**Severity:** MEDIUM

**Recommended Fix:**
Use a priority queue or heapq for O(log n) instead of O(n):
```python
import heapq

def __init__(self, ...):
    self.buffers = {}
    self._flush_queue = []  # (expiry_time, measurement)

async def _periodic_flush(self):
    while self._running:
        try:
            # Get next expiry time
            if self._flush_queue:
                next_expiry = self._flush_queue[0][0]
                now = datetime.now(timezone.utc).timestamp()
                wait_time = min(5, max(0, next_expiry - now))
            else:
                wait_time = 5
            
            await asyncio.sleep(wait_time)
            
            # Only check items that might have expired
            now = datetime.now(timezone.utc).timestamp()
            while self._flush_queue and self._flush_queue[0][0] <= now:
                _, measurement = heapq.heappop(self._flush_queue)
                # Flush only the measurement that's due
                await self._flush_records(measurement, ...)
```

---

### 15. Inefficient Regex Compilation in Models Validation

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/models.py`

**Issue Description:**
Regex patterns are compiled on every request:

```python
if not re.match(r'^[a-zA-Z0-9.-]+$', v):
    raise ValueError(...)

if not re.match(r'^\d+[mhd]$', v):
    raise ValueError(...)

# SQL keyword validation with re.search - compiled each time!
pattern = r'\b(SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|EXEC)\b'
if re.search(pattern, sql_upper):
    raise ValueError(...)
```

**Issue:** Each validation compiles a new regex object

**Severity:** MEDIUM (CPU spike under high request volume)

**Recommended Fix:**
```python
import re
from functools import lru_cache

# Compile once at module load
VALID_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9.-]+$')
VALID_DURATION_PATTERN = re.compile(r'^\d+[mhd]$')
SQL_KEYWORD_PATTERN = re.compile(r'\b(SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|EXEC)\b')

class QueryRequest(BaseModel):
    @field_validator('database')
    def validate_database(cls, v):
        if not VALID_NAME_PATTERN.match(v):
            raise ValueError(...)
        return v
```

---

### 16. Unbounded Metrics History in Memory

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/monitoring.py` (lines 82-84)

**Issue Description:**
```python
self.response_times = deque(maxlen=1000)  # Last 1000 requests
self.requests_by_endpoint = defaultdict(int)  # UNBOUNDED!
self.request_times = deque(maxlen=1000)
```

The `requests_by_endpoint` dict grows unboundedly—new endpoints add new keys forever.

**Severity:** MEDIUM

**Recommended Fix:**
```python
class MetricsCollector:
    def __init__(self, ...):
        self.requests_by_endpoint = defaultdict(int)
        self._max_tracked_endpoints = 100
    
    def record_request_start(self, endpoint: str):
        # Only track if within limit or already tracked
        if endpoint not in self.requests_by_endpoint:
            if len(self.requests_by_endpoint) >= self._max_tracked_endpoints:
                logger.debug(f"Max tracked endpoints ({self._max_tracked_endpoints}) reached, discarding {endpoint}")
                return
        
        self.requests_by_endpoint[endpoint] += 1
```

---

### 17. No Connection Lifecycle Events in StorageManager

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/storage_manager.py` (lines 57-104)

**Issue Description:**
The SQLiteConnectionPool has a busy timeout and retries, but no metrics or alerts:

```python
conn.execute(f"PRAGMA busy_timeout={60000}")  # 60 seconds
# If hits timeout repeatedly, no warning logged
```

**Severity:** MEDIUM

---

## LOW PRIORITY ISSUES

### 18. Potential Division by Zero in Metrics

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/duckdb_pool.py` (lines 659-662)

**Issue Description:**
```python
"avg_execution_time_ms": round(
    (conn.stats.total_execution_time / conn.stats.total_queries * 1000)
    if conn.stats.total_queries > 0 else 0.0,
    2
),
```

If `conn.stats.total_queries == 0`, returns 0, but code assumes division. Safe but unnecessary check.

**Severity:** LOW

---

### 19. Missing Timeouts on Compaction Lock Retry

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/compaction_lock.py` (lines 60-68)

**Issue Description:**
```python
for attempt in range(max_retries):
    try:
        conn = sqlite3.connect(self.db_path)
        # ...
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e) and attempt < max_retries - 1:
            import time
            time.sleep(retry_delay * (attempt + 1))  # Exponential backoff: good!
            continue
```

While exponential backoff is implemented, max 5 attempts × 0.1s × 5 = 2.5s total wait is short.

**Severity:** LOW

---

### 20. Inefficient List Comprehension in Logging

**Location:** `/Users/nacho/dev/basekick-labs/arc/api/query_cache.py` (lines 241-250)

**Issue Description:**
```python
"entries": [
    {
        "key_hash": key[:16],
        "age_seconds": round((datetime.now() - cached_at).total_seconds(), 1),
        "row_count": data.get("row_count", 0),
        "columns": len(data.get("columns", [])),
        "cached_at": cached_at.isoformat()
    }
    for key, (data, cached_at) in list(self.cache.items())[:20]  # Last 20 only
]
```

Creates list of entire cache just to slice it. Should use `itertools.islice()`.

**Severity:** LOW

---

## SUMMARY TABLE

| ID | Issue | File(s) | Severity | Type |
|:---|:------|:--------|:---------|:-----|
| 1 | Unclosed SQLite Connections | retention_routes, continuous_query_routes, compaction_lock | CRITICAL | Memory Leak |
| 2 | Unclosed DuckDB In-Memory | delete_routes | CRITICAL | Memory Leak |
| 3 | Sync DB Calls Blocking Event Loop | duckdb_pool | CRITICAL | Blocking/CPU |
| 4 | Infinite Loop in Health Check | duckdb_pool | CRITICAL | CPU Spike |
| 5 | Query Cache Memory Accumulation | query_cache | CRITICAL | Memory Leak |
| 6 | Pool Deadlock Risk | duckdb_pool | HIGH | Deadlock |
| 7 | No Timeout on Network Calls | storage backends | HIGH | Blocking |
| 8 | Buffer Error Path Overflow | parquet_buffer | HIGH | Memory Leak |
| 9 | WAL Synchronous I/O | wal | HIGH | Blocking |
| 10 | No Size Limit on Decompression | line_protocol_routes | HIGH | Dos/Memory |
| 11 | Metrics Silently Fail | monitoring | HIGH | Observability |
| 12 | Auth Cache Never Expires | auth | HIGH | Memory Leak |
| 13 | No Pagination on Results | duckdb_pool | MEDIUM | Memory |
| 14 | Time Loop Polling | parquet_buffer | MEDIUM | CPU |
| 15 | Regex Recompilation | models | MEDIUM | CPU |
| 16 | Unbounded Metrics Dict | monitoring | MEDIUM | Memory |
| 17 | No Connection Alerts | storage_manager | MEDIUM | Observability |
| 18 | Division by Zero Check | duckdb_pool | LOW | Code Quality |
| 19 | Short Lock Timeout | compaction_lock | LOW | Timeout |
| 20 | Inefficient List Slicing | query_cache | LOW | CPU |

---

## RECOMMENDATIONS

### Immediate Actions (Next Sprint)
1. **Wrap all SQLite operations in context managers** (Issues #1)
   - Estimated effort: 4 hours
   - Impact: Prevents critical memory leaks

2. **Close all DuckDB in-memory connections** (Issue #2)
   - Estimated effort: 1 hour
   - Impact: Prevents memory accumulation in delete operations

3. **Replace event loop polling with async waiting** (Issue #3)
   - Estimated effort: 2 hours
   - Impact: Reduces CPU spikes under load

4. **Add backoff to health check exception loop** (Issue #4)
   - Estimated effort: 30 minutes
   - Impact: Prevents CPU spikes on health check failure

### Short-term Actions (Next 2 Sprints)
5. Fix query cache memory estimation (Issue #5)
6. Add timeouts to storage backend operations (Issue #7)
7. Implement exponential backoff in parquet buffer (Issue #8)
8. Ensure WAL writes use thread pool (Issue #9)

### Medium-term Actions (Next Month)
9. Implement result pagination (Issue #13)
10. Replace polling loops with event-based design (Issue #14)
11. Pre-compile regex patterns (Issue #15)
12. Implement cache expiration in AuthManager (Issue #12)

### Long-term Improvements
13. Add comprehensive load testing for memory and CPU
14. Implement distributed tracing for connection lifecycle
15. Create observability dashboards for pool exhaustion, cache hit rate, etc.

