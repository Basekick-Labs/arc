#!/usr/bin/env python3
"""
Test script to compare regular JSON query vs Arrow query performance.

This script tests both endpoints with the same query and compares:
- Response time
- Response size
- Memory efficiency
"""

import requests
import time
import pyarrow as pa
import json
from io import BytesIO

# Configuration
BASE_URL = "http://localhost:8000"
TOKEN = "w__5EhIYLLmSAeI7CvSp0t6Lf0p-sohTuGiT2sewGqw"  # Replace with your token

# Test queries with different result sizes
QUERIES = [
    ("Small (1K rows)", "SELECT * FROM systems.cpu LIMIT 1000"),
    ("Medium (10K rows)", "SELECT * FROM systems.cpu LIMIT 10000"),
    ("Large (100K rows)", "SELECT * FROM systems.cpu LIMIT 100000"),
]


def test_regular_query(sql: str):
    """Test regular JSON query endpoint"""
    headers = {"Authorization": f"Bearer {TOKEN}"}
    payload = {"sql": sql}

    start = time.time()
    response = requests.post(f"{BASE_URL}/query", json=payload, headers=headers)
    duration = time.time() - start

    if response.status_code != 200:
        print(f"  ❌ Regular query failed: {response.text}")
        return None, None, None

    data = response.json()
    row_count = data.get("row_count", 0)
    response_size = len(response.content)

    return duration, row_count, response_size


def test_arrow_query(sql: str):
    """Test Arrow query endpoint"""
    headers = {"Authorization": f"Bearer {TOKEN}"}
    payload = {"sql": sql}

    start = time.time()
    response = requests.post(f"{BASE_URL}/query/arrow", json=payload, headers=headers)
    duration = time.time() - start

    if response.status_code != 200:
        print(f"  ❌ Arrow query failed: {response.text}")
        return None, None, None

    # Parse Arrow IPC stream
    reader = pa.ipc.open_stream(BytesIO(response.content))
    arrow_table = reader.read_all()
    row_count = len(arrow_table)
    response_size = len(response.content)

    # Get metadata from headers
    exec_time = response.headers.get("X-Execution-Time-Ms", "N/A")

    return duration, row_count, response_size


def format_size(bytes_size):
    """Format bytes to human-readable size"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def main():
    print("=" * 80)
    print("Arrow Query Performance Test")
    print("=" * 80)
    print()

    for query_name, sql in QUERIES:
        print(f"📊 Testing: {query_name}")
        print("-" * 80)

        # Test regular JSON endpoint
        print("  Regular JSON query...")
        json_duration, json_rows, json_size = test_regular_query(sql)

        if json_duration:
            print(f"    Duration: {json_duration:.3f}s")
            print(f"    Rows: {json_rows:,}")
            print(f"    Size: {format_size(json_size)}")

        # Test Arrow endpoint
        print("\n  Arrow query...")
        arrow_duration, arrow_rows, arrow_size = test_arrow_query(sql)

        if arrow_duration:
            print(f"    Duration: {arrow_duration:.3f}s")
            print(f"    Rows: {arrow_rows:,}")
            print(f"    Size: {format_size(arrow_size)}")

        # Compare results
        if json_duration and arrow_duration:
            print("\n  📈 Comparison:")
            speedup = json_duration / arrow_duration
            size_reduction = (1 - arrow_size / json_size) * 100

            print(f"    Speedup: {speedup:.2f}x {'🚀' if speedup > 1 else ''}")
            print(f"    Size reduction: {size_reduction:.1f}% {'💾' if size_reduction > 0 else ''}")

        print()

    print("=" * 80)
    print("✅ Test complete!")
    print()


if __name__ == "__main__":
    main()
