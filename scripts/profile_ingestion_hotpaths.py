#!/usr/bin/env python3
"""
Profile Arc ingestion hot paths with detailed instrumentation

This adds timing instrumentation to the ingestion pipeline to identify bottlenecks.
"""

import sys
import os

# Add timing instrumentation to arrow_writer.py
INSTRUMENTATION_CODE = '''
import time
from collections import defaultdict
import threading

# Global timing stats
_timing_stats = defaultdict(list)
_stats_lock = threading.Lock()

def _record_time(operation: str, duration: float):
    """Record timing for an operation"""
    with _stats_lock:
        _timing_stats[operation].append(duration)

def _get_timing_stats():
    """Get timing statistics"""
    with _stats_lock:
        stats = {}
        for operation, times in _timing_stats.items():
            if times:
                stats[operation] = {
                    'count': len(times),
                    'total': sum(times),
                    'avg': sum(times) / len(times),
                    'min': min(times),
                    'max': max(times),
                    'p50': sorted(times)[len(times)//2],
                    'p95': sorted(times)[int(len(times)*0.95)],
                }
        return stats

# Instrumented lock class
class InstrumentedLock:
    """Lock with timing instrumentation"""
    def __init__(self, name="lock"):
        import asyncio
        self._lock = asyncio.Lock()
        self.name = name

    async def __aenter__(self):
        start = time.perf_counter()
        await self._lock.acquire()
        wait_time = time.perf_counter() - start
        _record_time(f"lock_wait_{self.name}", wait_time)
        self._hold_start = time.perf_counter()
        return self

    async def __aexit__(self, *args):
        hold_time = time.perf_counter() - self._hold_start
        _record_time(f"lock_hold_{self.name}", hold_time)
        self._lock.release()
'''

WRITE_METHOD_INSTRUMENTATION = '''
    async def write(self, records: List[Dict[str, Any]]):
        """Add records to buffer (with optional WAL - row format only) - INSTRUMENTED"""
        import asyncio
        from datetime import datetime, timezone
        from collections import defaultdict

        _start_total = time.perf_counter()

        if not records:
            return

        # Write to WAL first (if enabled) - BEFORE buffering
        # Note: WAL only supports row format currently
        if self.wal_enabled and self.wal_writer:
            _wal_start = time.perf_counter()
            # Filter out columnar records for WAL (not supported yet)
            row_records = [r for r in records if not r.get('_columnar')]
            if row_records:
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(None, self.wal_writer.append, row_records)
                if not success:
                    logger.error("WAL append failed, records may be lost on crash")
            _record_time("write_wal", time.perf_counter() - _wal_start)

        # OPTIMIZATION: Extract records to flush while holding lock, then flush outside lock
        # This prevents blocking all writes during flush operations
        records_to_flush = {}

        _grouping_start = time.perf_counter()
        # Group records by measurement
        by_measurement = defaultdict(list)
        for record in records:
            measurement = record.get('measurement', 'unknown')
            by_measurement[measurement].append(record)
        _record_time("write_grouping", time.perf_counter() - _grouping_start)

        _lock_ops_start = time.perf_counter()
        async with self._lock:
            _lock_start = time.perf_counter()
            # Add to buffers and identify measurements that need flushing
            for measurement, measurement_records in by_measurement.items():
                if measurement not in self.buffer_start_times:
                    self.buffer_start_times[measurement] = datetime.now(timezone.utc)

                self.buffers[measurement].extend(measurement_records)

                # Count records (columnar vs row)
                num_records = sum(
                    len(r['columns']['time']) if r.get('_columnar') else 1
                    for r in measurement_records
                )
                self.total_records_buffered += num_records

                # Check if buffer should be flushed
                # For columnar, count total rows across all columnar batches
                buffer_size = sum(
                    len(r['columns']['time']) if r.get('_columnar') else 1
                    for r in self.buffers[measurement]
                )

                if buffer_size >= self.max_buffer_size:
                    logger.debug(f"Arrow buffer for '{measurement}' reached size limit, flushing")
                    # Extract records while holding lock
                    records_to_flush[measurement] = self.buffers[measurement]
                    self.buffers[measurement] = []
                    del self.buffer_start_times[measurement]
            _record_time("write_lock_hold", time.perf_counter() - _lock_start)
        _record_time("write_lock_total", time.perf_counter() - _lock_ops_start)

        # OPTIMIZATION: Flush outside lock - allows concurrent writes during flush
        _flush_start = time.perf_counter()
        for measurement, flush_records in records_to_flush.items():
            await self._flush_records(measurement, flush_records)
        if records_to_flush:
            _record_time("write_flush", time.perf_counter() - _flush_start)

        _record_time("write_total", time.perf_counter() - _start_total)
'''

def add_instrumentation():
    """Add instrumentation to arrow_writer.py"""
    arrow_writer_path = "ingest/arrow_writer.py"

    with open(arrow_writer_path, 'r') as f:
        content = f.read()

    # Check if already instrumented
    if '_record_time' in content:
        print("✅ Already instrumented")
        return

    # Add instrumentation imports at top
    lines = content.split('\n')
    import_index = 0
    for i, line in enumerate(lines):
        if line.startswith('import ') or line.startswith('from '):
            import_index = i

    # Insert instrumentation code after imports
    lines.insert(import_index + 1, INSTRUMENTATION_CODE)

    # Save instrumented version
    instrumented = '\n'.join(lines)

    with open(arrow_writer_path + '.instrumented', 'w') as f:
        f.write(instrumented)

    print(f"✅ Instrumented version saved to: {arrow_writer_path}.instrumented")
    print("   Review the changes and manually apply if needed")

    # Show timing stats helper
    print("\n📊 To view timing stats during runtime, add to your code:")
    print("""
from ingest.arrow_writer import _get_timing_stats
import json

# After benchmark:
stats = _get_timing_stats()
print(json.dumps(stats, indent=2))
""")


def main():
    print("🔍 Arc Ingestion Hot Path Profiler")
    print("=" * 60)
    print("\nThis will add detailed timing instrumentation to identify:")
    print("  • Lock wait times")
    print("  • Lock hold times")
    print("  • Grouping overhead")
    print("  • Flush times")
    print("  • Total write times")
    print()

    add_instrumentation()

    print("\n" + "=" * 60)
    print("📝 Next Steps:")
    print("1. Review ingest/arrow_writer.py.instrumented")
    print("2. Apply instrumentation if it looks good")
    print("3. Restart Arc server")
    print("4. Run benchmark")
    print("5. Check timing stats")


if __name__ == "__main__":
    main()
