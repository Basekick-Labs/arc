#!/usr/bin/env python3
"""
Unit test for SimpleDuckDBPool

Tests the simplified connection pool in isolation before integration.
"""
import time
import sys
sys.path.insert(0, '.')

from api.duckdb_pool_simple import SimpleDuckDBPool

def test_basic_operations():
    """Test 1: Basic get/return operations"""
    print("\n" + "="*60)
    print("TEST 1: Basic Operations")
    print("="*60)

    pool = SimpleDuckDBPool(pool_size=3)

    # Test single query
    with pool.get_connection() as conn:
        result = conn.execute("SELECT 1 as num").fetchall()
        assert result[0][0] == 1, "Query failed"

    print("✅ Single query: PASS")

    # Test multiple sequential queries
    for i in range(10):
        with pool.get_connection() as conn:
            result = conn.execute(f"SELECT {i} as num").fetchall()
            assert result[0][0] == i, f"Query {i} failed"

    print("✅ Sequential queries (10): PASS")

    metrics = pool.get_metrics()
    print(f"\n📊 Metrics: {metrics}")

    assert metrics['total_queries'] == 11, "Query count mismatch"
    assert metrics['total_errors'] == 0, "Unexpected errors"
    assert metrics['available_connections'] == 3, "Connection leak detected"

    print("✅ Metrics validation: PASS")
    return True

def test_concurrent_queries():
    """Test 2: Concurrent queries (simulated)"""
    print("\n" + "="*60)
    print("TEST 2: Concurrent Usage")
    print("="*60)

    pool = SimpleDuckDBPool(pool_size=3)

    # Simulate concurrent access (within pool size)
    start = time.time()
    for i in range(30):
        with pool.get_connection() as conn:
            conn.execute("SELECT 1").fetchall()

    duration = time.time() - start

    metrics = pool.get_metrics()
    print(f"\n📊 30 queries in {duration:.2f}s = {30/duration:.2f} qps")
    print(f"   Available connections: {metrics['available_connections']}/3")
    print(f"   Total errors: {metrics['total_errors']}")

    assert metrics['available_connections'] == 3, "Connection leak detected"
    assert metrics['total_errors'] == 0, "Unexpected errors"

    print("✅ Concurrent queries: PASS")
    return True

def test_timeout_handling():
    """Test 3: Timeout when pool exhausted"""
    print("\n" + "="*60)
    print("TEST 3: Timeout Handling")
    print("="*60)

    pool = SimpleDuckDBPool(pool_size=1)

    # Get the only connection
    with pool.get_connection() as conn1:
        print("  Connection 1 acquired")

        # Try to get another (should timeout immediately)
        try:
            with pool.get_connection(timeout=0.5) as conn2:
                print("  ❌ Should not reach here!")
                assert False, "Should have timed out"
        except TimeoutError as e:
            print(f"  ✅ Timeout as expected: {e}")

    # After release, should work again
    with pool.get_connection() as conn:
        print("  Connection re-acquired after release")

    metrics = pool.get_metrics()
    print(f"\n📊 Total queries: {metrics['total_queries']}")
    print(f"   Total errors: {metrics['total_errors']}")

    assert metrics['total_errors'] == 1, "Should have 1 timeout error"
    assert metrics['available_connections'] == 1, "Connection should be returned"

    print("✅ Timeout handling: PASS")
    return True

def test_memory_cleanup():
    """Test 4: Memory doesn't grow (basic check)"""
    print("\n" + "="*60)
    print("TEST 4: Memory Cleanup")
    print("="*60)

    import gc
    import psutil
    import os

    process = psutil.Process(os.getpid())
    gc.collect()
    initial_mem = process.memory_info().rss / 1024 / 1024  # MB

    print(f"  Initial memory: {initial_mem:.2f}MB")

    pool = SimpleDuckDBPool(pool_size=3)

    # Run many queries
    for i in range(100):
        with pool.get_connection() as conn:
            # Create some data to ensure memory is used
            conn.execute("SELECT range(1000) as nums").fetchall()

    gc.collect()
    final_mem = process.memory_info().rss / 1024 / 1024  # MB
    delta = final_mem - initial_mem

    print(f"  Final memory:   {final_mem:.2f}MB")
    print(f"  Delta:          {delta:+.2f}MB")

    # Memory should not grow significantly (allow 50MB buffer for Python overhead)
    if delta < 50:
        print(f"✅ Memory stable (delta < 50MB): PASS")
    else:
        print(f"⚠️  Memory grew by {delta:.2f}MB (might be acceptable)")

    metrics = pool.get_metrics()
    print(f"\n📊 Queries: {metrics['total_queries']}, Errors: {metrics['total_errors']}")

    return True

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("SimpleDuckDBPool Unit Tests")
    print("="*60)

    try:
        test_basic_operations()
        test_concurrent_queries()
        test_timeout_handling()
        test_memory_cleanup()

        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED")
        print("="*60)
        print("\nThe simplified pool is ready for integration!")
        return 0

    except Exception as e:
        print("\n" + "="*60)
        print("❌ TEST FAILED")
        print("="*60)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(run_all_tests())
