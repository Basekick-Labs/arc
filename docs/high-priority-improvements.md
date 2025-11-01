# HIGH Priority Performance Improvements

**Branch:** `feat/high-priority-performance-improvements`
**Started:** 2025-11-01
**Status:** In Progress (0 of 6 complete)

This document tracks the remaining HIGH priority issues from the performance analysis that were not critical memory leaks or CPU spikes, but still important for production stability.

---

## Remaining HIGH Priority Issues

### 6. Potential Connection Pool Deadlock with Nested Locks
**Reference:** ARC_PERFORMANCE_ANALYSIS.md #6
**Location:** [api/duckdb_pool.py](../api/duckdb_pool.py:282-317)
**Status:** 🔴 Not Started

**Issue:**
The `get_connection()` method acquires `pool_lock` while potentially calling `conn.close()`, which could lead to deadlock if `close()` also needs locks.

**Current Code:**
```python
def get_connection(self, timeout: float = 5.0) -> Optional[DuckDBConnection]:
    try:
        conn = self.pool.get(timeout=timeout)

        if not conn.stats.is_healthy:
            if not conn.health_check():
                try:
                    conn.close()
                    conn = DuckDBConnection(...)
                    with self.pool_lock:  # LOCK while holding other resources
                        self.connections[conn.stats.connection_id] = conn
```

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
```

**Impact:** Prevents potential deadlocks in connection pool management
**Estimated Time:** 1 hour

---

### 7. No Timeout on External Network Calls to Storage Backends
**Reference:** ARC_PERFORMANCE_ANALYSIS.md #7
**Location:** Multiple storage backends
**Status:** 🔴 Not Started

**Files:**
- [storage/minio_backend.py](../storage/minio_backend.py)
- [storage/s3_backend.py](../storage/s3_backend.py)
- [storage/gcs_backend.py](../storage/gcs_backend.py)

**Issue:**
Upload operations to cloud storage may hang indefinitely if network is flaky, blocking buffer flushes and causing memory accumulation.

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

**Impact:** Prevents write operations from hanging indefinitely
**Estimated Time:** 3 hours (multiple backends)

---

### 8. ParquetBuffer Error Path Re-adds Data Without Limit Check
**Reference:** ARC_PERFORMANCE_ANALYSIS.md #8
**Location:** [ingest/parquet_buffer.py](../ingest/parquet_buffer.py:246-257)
**Status:** ⚠️ Already Fixed (needs verification)

**Issue:**
The error path re-adds records to buffer with a check that could be exceeded.

**Current Code:**
```python
# Check: only re-add if buffer isn't already too large
if len(self.buffers[measurement]) < self.max_buffer_size * 2:
    self.buffers[measurement].extend(records)  # Could exceed 2x limit!
```

**Status:** Need to verify current implementation handles this correctly with strict limit checking.

**Impact:** Prevents buffer overflow on repeated errors
**Estimated Time:** 1 hour (verification + fix if needed)

---

### 10. No Resource Limits on Line Protocol Message Size
**Reference:** ARC_PERFORMANCE_ANALYSIS.md #10
**Location:** [api/line_protocol_routes.py](../api/line_protocol_routes.py)
**Status:** 🔴 Not Started

**Issue:**
The gzip decompression doesn't check decompressed size, allowing potential DoS attack (100MB gzip → 10GB decompressed → OOM).

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

**Impact:** Prevents DoS attacks via gzip bomb
**Estimated Time:** 2 hours

---

### 11. Unchecked Metrics Collection Failures
**Reference:** ARC_PERFORMANCE_ANALYSIS.md #11
**Location:** [api/monitoring.py](../api/monitoring.py:280-289)
**Status:** 🔴 Not Started

**Issue:**
Metrics collection failures are silently logged without alerting, making observability issues invisible.

**Recommended Fix:**
- Add consecutive failure counter
- Emit warning after N failures
- Expose health check endpoint for metrics collection

**Impact:** Improves observability and monitoring reliability
**Estimated Time:** 2 hours

---

### 9. WAL Write Synchronous Calls in Request Handler
**Reference:** ARC_PERFORMANCE_ANALYSIS.md #9
**Location:** [storage/wal.py](../storage/wal.py:124-165)
**Status:** ⚠️ Already Fixed (needs verification)

**Issue:**
The `os.write()` and `fsync()` calls are synchronous and could block the event loop.

**Status:** Need to verify WAL writes are already offloaded to thread pool via executor in request handlers.

**Impact:** Prevents event loop blocking on WAL writes
**Estimated Time:** 1 hour (verification)

---

## Summary

| Issue | Priority | Status | Estimated Time | Impact |
|-------|----------|--------|----------------|--------|
| #6 Pool Deadlock | HIGH | 🔴 Not Started | 1 hour | Deadlock prevention |
| #7 Network Timeouts | HIGH | 🔴 Not Started | 3 hours | Blocking prevention |
| #8 Buffer Overflow | HIGH | ⚠️ Verify | 1 hour | Memory protection |
| #10 Gzip Bomb DoS | HIGH | 🔴 Not Started | 2 hours | Security/DoS |
| #11 Metrics Failures | HIGH | 🔴 Not Started | 2 hours | Observability |
| #9 WAL Blocking | HIGH | ⚠️ Verify | 1 hour | Event loop |
| **TOTAL** | | **0/6 Done** | **10 hours** | |

---

## Testing Strategy

After all fixes are complete:

1. **Deadlock Testing** - Concurrent connection pool stress test
2. **Network Failure Testing** - Simulate slow/hanging cloud storage
3. **Buffer Overflow Testing** - Repeated flush failures
4. **Gzip Bomb Testing** - Send compressed payloads with size limits
5. **Metrics Health Testing** - Verify alerting on collection failures
6. **WAL Blocking Testing** - Verify async behavior under load

---

## Next Steps

1. Verify issues #8 and #9 (already fixed?)
2. Fix #6 (pool deadlock) - quick win
3. Fix #10 (gzip bomb) - security critical
4. Fix #7 (network timeouts) - most complex
5. Fix #11 (metrics failures) - observability
6. Test all fixes before merging
