#!/usr/bin/env python3
"""
Systematic memory leak investigation script.
Run this inside the Arc container to trace memory usage.
"""

import sys
import gc
import os
import tracemalloc
import time
import duckdb

def measure_memory():
    """Get current RSS memory in MB"""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except:
        return 0

def test_duckdb_cursor_leak():
    """Test if DuckDB cursor needs explicit close"""
    print("\n" + "="*60)
    print("TEST 1: DuckDB Cursor Leak")
    print("="*60)

    conn = duckdb.connect(':memory:')

    # Create test data
    conn.execute("CREATE TABLE test AS SELECT range as x FROM range(10000)")

    mem_before = measure_memory()
    print(f"Memory before queries: {mem_before:.1f} MB")

    # Test WITHOUT cursor.close()
    print("\nRunning 10 queries WITHOUT cursor.close()...")
    for i in range(10):
        result = conn.execute("SELECT * FROM test").fetchall()
        data = [list(row) for row in result]
        del result
        del data

    gc.collect()
    mem_after_no_close = measure_memory()
    print(f"Memory after (no close): {mem_after_no_close:.1f} MB")
    print(f"Growth: {mem_after_no_close - mem_before:.1f} MB")

    # Close the connection properly
    conn.close()
    del conn
    gc.collect()
    time.sleep(1)

    # Create new connection
    conn = duckdb.connect(':memory:')
    conn.execute("CREATE TABLE test AS SELECT range as x FROM range(10000)")

    mem_before = measure_memory()
    print(f"\nMemory before queries: {mem_before:.1f} MB")

    # Test WITH cursor.close()
    print("Running 10 queries WITH cursor.close()...")
    for i in range(10):
        cursor = conn.execute("SELECT * FROM test")
        result = cursor.fetchall()
        data = [list(row) for row in result]
        del result
        cursor.close()
        del cursor
        del data

    gc.collect()
    mem_after_with_close = measure_memory()
    print(f"Memory after (with close): {mem_after_with_close:.1f} MB")
    print(f"Growth: {mem_after_with_close - mem_before:.1f} MB")

    conn.close()

    print(f"\n✓ Difference: {(mem_after_no_close - mem_before) - (mem_after_with_close - mem_before):.1f} MB")
    if abs((mem_after_no_close - mem_before) - (mem_after_with_close - mem_before)) < 1:
        print("⚠ cursor.close() makes NO difference - not the leak!")
    else:
        print("✓ cursor.close() DOES help - this is part of the leak")

def test_fastapi_response_leak():
    """Test if FastAPI response holds references"""
    print("\n" + "="*60)
    print("TEST 2: FastAPI Response Leak")
    print("="*60)

    from pydantic import BaseModel
    from typing import List
    from datetime import datetime

    class QueryResponse(BaseModel):
        success: bool
        columns: List[str]
        data: List[List]
        row_count: int
        timestamp: datetime

    mem_before = measure_memory()
    print(f"Memory before: {mem_before:.1f} MB")

    print("Creating 100 response objects with 100 rows each...")
    for _ in range(100):
        # Simulate query result
        result = {
            "columns": ["col1", "col2", "col3"],
            "data": [[1, 2, 3] for _ in range(100)],
            "row_count": 100
        }

        response = QueryResponse(
            success=True,
            columns=result["columns"],
            data=result["data"],
            row_count=result["row_count"],
            timestamp=datetime.now()
        )

        # Simulate what happens in production - response is returned
        # We explicitly delete everything to simulate end of request
        del result
        del response

    mem_after = measure_memory()
    print(f"Memory after: {mem_after:.1f} MB")
    print(f"Growth: {mem_after - mem_before:.1f} MB")

    print("\nRunning GC...")
    collected = gc.collect()
    mem_after_gc = measure_memory()
    print(f"GC collected {collected} objects")
    print(f"Memory after GC: {mem_after_gc:.1f} MB")
    print(f"Memory freed by GC: {mem_after - mem_after_gc:.1f} MB")

    if mem_after_gc < mem_after:
        print("✓ GC freed some memory - objects were collectible")
    else:
        print("⚠ GC freed nothing - objects are still referenced somewhere")

    # Additional test: Check if it's just Python's memory allocator
    print("\nTrying malloc_trim()...")
    try:
        import ctypes
        import platform
        if platform.system() == 'Linux':
            libc = ctypes.CDLL('libc.so.6')
            libc.malloc_trim(0)
            mem_after_trim = measure_memory()
            print(f"Memory after malloc_trim: {mem_after_trim:.1f} MB")
            print(f"Memory freed by malloc_trim: {mem_after_gc - mem_after_trim:.1f} MB")
        else:
            print("malloc_trim not available on this platform")
    except Exception as e:
        print(f"malloc_trim failed: {e}")

def test_tracemalloc():
    """Use tracemalloc to find where allocations happen"""
    print("\n" + "="*60)
    print("TEST 3: Tracemalloc - Find Allocation Sources")
    print("="*60)

    tracemalloc.start()

    # Simulate a query execution
    conn = duckdb.connect(':memory:')
    conn.execute("CREATE TABLE test AS SELECT range as x FROM range(1000)")

    snapshot1 = tracemalloc.take_snapshot()

    # Execute query 10 times
    for _ in range(10):
        try:
            cursor = conn.execute("SELECT * FROM test")
            result = cursor.fetchall()
            data = [list(row) for row in result]
            del result
            cursor.close()
            del cursor
            del data
        except Exception as e:
            print(f"Error during query: {e}")
            break

    snapshot2 = tracemalloc.take_snapshot()

    top_stats = snapshot2.compare_to(snapshot1, 'lineno')

    print("\nTop 10 memory allocations:")
    for stat in top_stats[:10]:
        print(f"{stat.size_diff / 1024:.1f} KB: {stat}")

    tracemalloc.stop()
    conn.close()
    del conn

def main():
    print("="*60)
    print("Arc Memory Leak Investigation")
    print("="*60)
    print(f"Python version: {sys.version}")
    print(f"Process PID: {os.getpid()}")

    try:
        test_duckdb_cursor_leak()
    except Exception as e:
        print(f"❌ Test 1 failed: {e}")
        import traceback
        traceback.print_exc()

    try:
        test_fastapi_response_leak()
    except Exception as e:
        print(f"❌ Test 2 failed: {e}")
        import traceback
        traceback.print_exc()

    try:
        test_tracemalloc()
    except Exception as e:
        print(f"❌ Test 3 failed: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*60)
    print("Investigation Complete")
    print("="*60)

if __name__ == "__main__":
    main()
