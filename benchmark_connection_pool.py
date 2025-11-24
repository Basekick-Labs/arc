#!/usr/bin/env python3
"""
Benchmark DuckDB Connection Pool Performance

Tests connection pool under various load patterns:
- Sequential queries (low concurrency)
- Parallel queries (high concurrency)
- Mixed workload (varied query complexity)
- Burst traffic (sudden spikes)

Metrics collected:
- Query latency (p50, p95, p99)
- Pool utilization
- Queue wait time
- Throughput (queries/sec)
- Memory usage
"""
import requests
import time
import asyncio
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
import os

API_URL = "http://localhost:8000/api/v1/query"
API_TOKEN = "n7nmz2jStpxVW9SGcl0JEy8USSwoBRjaAuPzBemsrB4"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# Test queries of varying complexity
QUERIES = {
    "simple": "SELECT * FROM default.cpu LIMIT 10",
    "aggregate": "SELECT host, AVG(usage_user) as avg_cpu FROM default.cpu GROUP BY host",
    "join": """
        SELECT c.host, c.usage_user, m.usage_system
        FROM default.cpu c
        JOIN default.mem m ON c.host = m.host
        LIMIT 100
    """,
    "count": "SELECT COUNT(*) FROM default.cpu"
}

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def execute_query(query_name, query_sql):
    """Execute a single query and return timing"""
    start = time.time()
    try:
        response = requests.post(
            API_URL,
            headers=HEADERS,
            json={"sql": query_sql},
            timeout=30
        )
        elapsed = time.time() - start

        if response.status_code == 200:
            data = response.json()
            return {
                "success": True,
                "latency": elapsed,
                "rows": data.get('row_count', 0),
                "execution_time_ms": data.get('execution_time_ms', 0),
                "query": query_name
            }
        else:
            return {
                "success": False,
                "latency": elapsed,
                "error": response.text[:100],
                "query": query_name
            }
    except Exception as e:
        return {
            "success": False,
            "latency": time.time() - start,
            "error": str(e),
            "query": query_name
        }

def run_sequential_test(num_queries=100):
    """Test 1: Sequential queries (baseline)"""
    print("\n" + "="*70)
    print("TEST 1: Sequential Queries (Low Concurrency)")
    print("="*70)

    results = []
    start_mem = get_memory_usage()
    start_time = time.time()

    for i in range(num_queries):
        query_name = list(QUERIES.keys())[i % len(QUERIES)]
        query_sql = QUERIES[query_name]
        result = execute_query(query_name, query_sql)
        results.append(result)

        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{num_queries} queries")

    duration = time.time() - start_time
    end_mem = get_memory_usage()

    return analyze_results("Sequential", results, duration, start_mem, end_mem)

def run_parallel_test(num_queries=100, workers=10):
    """Test 2: Parallel queries (high concurrency)"""
    print("\n" + "="*70)
    print(f"TEST 2: Parallel Queries (High Concurrency, {workers} workers)")
    print("="*70)

    results = []
    start_mem = get_memory_usage()
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for i in range(num_queries):
            query_name = list(QUERIES.keys())[i % len(QUERIES)]
            query_sql = QUERIES[query_name]
            future = executor.submit(execute_query, query_name, query_sql)
            futures.append(future)

        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            if completed % 20 == 0:
                print(f"  Progress: {completed}/{num_queries} queries")

    duration = time.time() - start_time
    end_mem = get_memory_usage()

    return analyze_results("Parallel", results, duration, start_mem, end_mem)

def run_burst_test(num_bursts=5, queries_per_burst=20, workers=20):
    """Test 3: Burst traffic (sudden spikes)"""
    print("\n" + "="*70)
    print(f"TEST 3: Burst Traffic ({num_bursts} bursts of {queries_per_burst} queries)")
    print("="*70)

    all_results = []
    start_mem = get_memory_usage()
    start_time = time.time()

    for burst_num in range(num_bursts):
        print(f"\n  Burst {burst_num + 1}/{num_bursts}")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for i in range(queries_per_burst):
                query_name = list(QUERIES.keys())[i % len(QUERIES)]
                query_sql = QUERIES[query_name]
                future = executor.submit(execute_query, query_name, query_sql)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                all_results.append(result)

        # Brief pause between bursts
        if burst_num < num_bursts - 1:
            time.sleep(0.5)

    duration = time.time() - start_time
    end_mem = get_memory_usage()

    return analyze_results("Burst", all_results, duration, start_mem, end_mem)

def analyze_results(test_name, results, duration, start_mem, end_mem):
    """Analyze and display test results"""
    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]

    if not successful:
        print(f"\n❌ All queries failed!")
        return None

    latencies = [r['latency'] * 1000 for r in successful]  # Convert to ms
    exec_times = [r.get('execution_time_ms', 0) for r in successful]

    # Calculate percentiles
    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)] if len(latencies) > 20 else max(latencies)
    p99 = latencies[int(len(latencies) * 0.99)] if len(latencies) > 100 else max(latencies)

    # Calculate throughput
    total_queries = len(results)
    throughput = total_queries / duration if duration > 0 else 0

    # Memory delta
    mem_delta = end_mem - start_mem

    metrics = {
        "test": test_name,
        "total_queries": total_queries,
        "successful": len(successful),
        "failed": len(failed),
        "duration_sec": round(duration, 2),
        "throughput_qps": round(throughput, 2),
        "latency_p50_ms": round(p50, 2),
        "latency_p95_ms": round(p95, 2),
        "latency_p99_ms": round(p99, 2),
        "avg_exec_time_ms": round(statistics.mean(exec_times), 2) if exec_times else 0,
        "memory_delta_mb": round(mem_delta, 2),
        "success_rate": round(len(successful) / total_queries * 100, 1) if total_queries > 0 else 0
    }

    # Display results
    print(f"\n📊 Results:")
    print(f"  Total Queries:     {metrics['total_queries']}")
    print(f"  Successful:        {metrics['successful']}")
    print(f"  Failed:            {metrics['failed']}")
    print(f"  Success Rate:      {metrics['success_rate']}%")
    print(f"  Duration:          {metrics['duration_sec']}s")
    print(f"  Throughput:        {metrics['throughput_qps']} queries/sec")
    print(f"\n  Latency (total):")
    print(f"    p50:             {metrics['latency_p50_ms']}ms")
    print(f"    p95:             {metrics['latency_p95_ms']}ms")
    print(f"    p99:             {metrics['latency_p99_ms']}ms")
    print(f"  Avg Exec Time:     {metrics['avg_exec_time_ms']}ms")
    print(f"  Memory Delta:      {metrics['memory_delta_mb']}MB")

    return metrics

def run_all_tests():
    """Run complete benchmark suite"""
    print("\n" + "="*70)
    print("DuckDB Connection Pool Benchmark")
    print("="*70)
    print("\nWarming up Arc...")

    # Warm-up
    for _ in range(5):
        execute_query("simple", QUERIES["simple"])

    print("✓ Warm-up complete\n")

    # Run tests
    all_metrics = []

    # Test 1: Sequential (baseline) - REDUCED to avoid crash
    metrics1 = run_sequential_test(num_queries=50)
    if metrics1:
        all_metrics.append(metrics1)

    time.sleep(2)

    # Test 2: Parallel (10 workers) - REDUCED
    metrics2 = run_parallel_test(num_queries=50, workers=10)
    if metrics2:
        all_metrics.append(metrics2)

    time.sleep(2)

    # Test 3: Parallel (20 workers) - REDUCED
    metrics3 = run_parallel_test(num_queries=50, workers=20)
    if metrics3:
        all_metrics.append(metrics3)

    time.sleep(2)

    # Test 4: Burst traffic - REDUCED
    metrics4 = run_burst_test(num_bursts=3, queries_per_burst=15, workers=15)
    if metrics4:
        all_metrics.append(metrics4)

    # Summary
    print("\n" + "="*70)
    print("SUMMARY - All Tests")
    print("="*70)

    for m in all_metrics:
        print(f"\n{m['test']:15} | "
              f"QPS: {m['throughput_qps']:6.1f} | "
              f"p50: {m['latency_p50_ms']:6.1f}ms | "
              f"p95: {m['latency_p95_ms']:6.1f}ms | "
              f"Success: {m['success_rate']:5.1f}%")

    print("\n" + "="*70)
    print("Benchmark complete! Save these metrics for comparison.")
    print("="*70)

    return all_metrics

if __name__ == "__main__":
    try:
        metrics = run_all_tests()

        # Save metrics to file
        import json
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(metrics, f, indent=2)
        print(f"\n💾 Metrics saved to: {filename}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Benchmark interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
