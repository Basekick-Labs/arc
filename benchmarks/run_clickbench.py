#!/usr/bin/env python3
"""
Run ClickBench queries against Arc and measure performance
"""

import time
import requests
import json
from pathlib import Path

# Configuration
ARC_URL = "http://localhost:8000"
ARC_TOKEN = "E8zXmKgI8KgUVE6ufUKfdTimhGbYpoksZyW3jVBWDA0"
QUERIES_FILE = Path(__file__).parent / "queries_benchmark.sql"

# Load queries
queries = []
with open(QUERIES_FILE) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('--'):
            queries.append(line)

print(f"Running {len(queries)} ClickBench queries...")
print()

# Clear cache
try:
    requests.post(
        f"{ARC_URL}/cache/clear",
        headers={"Authorization": f"Bearer {ARC_TOKEN}"},
        timeout=10
    )
    print("✅ Cache cleared")
except:
    print("⚠️  Could not clear cache")

print()

results = []
total_time = 0

for i, query in enumerate(queries, 1):
    print(f"Query {i:2d}/43: ", end="", flush=True)

    try:
        start = time.time()

        response = requests.post(
            f"{ARC_URL}/query",
            headers={
                "Authorization": f"Bearer {ARC_TOKEN}",
                "Content-Type": "application/json"
            },
            json={"sql": query, "format": "json", "limit": 1000000},
            timeout=300
        )

        elapsed = time.time() - start

        if response.status_code == 200:
            data = response.json()
            row_count = data.get("row_count", 0)
            results.append(elapsed)
            total_time += elapsed
            print(f"{elapsed:6.3f}s ({row_count:,} rows)")
        else:
            results.append(None)
            print(f"ERROR {response.status_code}")
            print(f"       {response.text[:200]}")

    except Exception as e:
        results.append(None)
        print(f"ERROR: {e}")

print()
print("=" * 80)
print(f"Total time: {total_time:.2f}s")
print(f"Success rate: {sum(1 for r in results if r is not None)}/43")
print(f"Average query time: {total_time / len([r for r in results if r is not None]):.3f}s")
print("=" * 80)

# Save results
output = {
    "total_time": round(total_time, 2),
    "success_count": sum(1 for r in results if r is not None),
    "query_times": results
}

output_file = Path(__file__).parent / "clickbench_results.json"
with open(output_file, "w") as f:
    json.dump(output, f, indent=2)

print(f"\nResults saved to: {output_file}")
