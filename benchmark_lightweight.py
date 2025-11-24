#!/usr/bin/env python3
"""
Lightweight Connection Pool Benchmark

This is a MINIMAL test to avoid system crashes.
We'll use the crash behavior itself as a data point!
"""
import requests
import time
import statistics

API_URL = "http://localhost:8000/api/v1/query"
API_TOKEN = "n7nmz2jStpxVW9SGcl0JEy8USSwoBRjaAuPzBemsrB4"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def execute_query(query_sql):
    """Execute a single query and return timing"""
    start = time.time()
    try:
        response = requests.post(
            API_URL,
            headers=HEADERS,
            json={"sql": query_sql},
            timeout=10
        )
        elapsed = time.time() - start

        if response.status_code == 200:
            data = response.json()
            return {
                "success": True,
                "latency": elapsed * 1000,  # ms
                "execution_time_ms": data.get('execution_time_ms', 0)
            }
        else:
            return {"success": False, "latency": elapsed * 1000}
    except Exception as e:
        return {"success": False, "latency": (time.time() - start) * 1000}

def run_simple_test(num_queries=30):
    """Very simple sequential test"""
    print(f"Running {num_queries} sequential queries...")

    query = "SELECT * FROM default.cpu LIMIT 10"
    results = []

    start_time = time.time()
    for i in range(num_queries):
        result = execute_query(query)
        results.append(result)
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{num_queries}...")

    duration = time.time() - start_time

    successful = [r for r in results if r.get('success')]
    if successful:
        latencies = [r['latency'] for r in successful]
        print(f"\n✅ Results:")
        print(f"  Success: {len(successful)}/{num_queries}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Throughput: {len(successful)/duration:.2f} qps")
        print(f"  Latency p50: {statistics.median(latencies):.2f}ms")
        print(f"  Latency p95: {sorted(latencies)[int(len(latencies)*0.95)]:.2f}ms")

        return {
            "success_count": len(successful),
            "total": num_queries,
            "duration": duration,
            "qps": len(successful)/duration,
            "p50_ms": statistics.median(latencies),
            "p95_ms": sorted(latencies)[int(len(latencies)*0.95)]
        }
    else:
        print(f"\n❌ All queries failed")
        return None

if __name__ == "__main__":
    print("="*60)
    print("LIGHTWEIGHT Connection Pool Baseline")
    print("="*60)
    print("\nThis is intentionally small to avoid system crashes.")
    print("Previous crash = evidence of memory issues!\n")

    # Warm up
    print("Warming up...")
    for _ in range(3):
        execute_query("SELECT * FROM default.cpu LIMIT 5")
    print("✓ Warm-up complete\n")

    # Run baseline
    baseline = run_simple_test(num_queries=30)

    if baseline:
        print("\n" + "="*60)
        print("BASELINE METRICS (BEFORE simplification)")
        print("="*60)
        print(f"QPS: {baseline['qps']:.2f}")
        print(f"p50: {baseline['p50_ms']:.2f}ms")
        print(f"p95: {baseline['p95_ms']:.2f}ms")
        print("\n💾 Save these for comparison after simplification!")

        # Save to file
        import json
        with open('baseline_metrics.json', 'w') as f:
            json.dump(baseline, f, indent=2)
        print("\n✓ Saved to baseline_metrics.json")
