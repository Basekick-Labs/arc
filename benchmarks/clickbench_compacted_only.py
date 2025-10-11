#!/usr/bin/env python3
"""
ClickBench Benchmark - Compacted Data Only

Assumes data is already loaded. This script:
1. Triggers compaction
2. Waits for completion
3. Runs queries
4. Compares with baseline
"""

import os
import sys
import time
import json
import requests
from datetime import datetime
from pathlib import Path

# Configuration
ARC_URL = os.getenv("ARC_URL", "http://localhost:8000")
ARC_TOKEN = os.getenv("ARC_TOKEN")
DATABASE = "benchmark"  # Assuming hits table is in benchmark database

# Load queries
SCRIPT_DIR = Path(__file__).parent.parent.parent / "ClickBench" / "arc"
QUERIES_FILE = SCRIPT_DIR / "queries.sql"


def load_clickbench_queries():
    """Load ClickBench queries from queries.sql"""
    with open(QUERIES_FILE) as f:
        queries = []
        for line in f:
            line = line.strip()
            if line and not line.startswith('--'):
                queries.append(line)
        return queries


def trigger_compaction():
    """Trigger compaction via API"""
    print("\n" + "=" * 80)
    print("Step 1: Triggering Compaction")
    print("=" * 80)
    print()

    try:
        response = requests.post(
            f"{ARC_URL}/api/compaction/trigger",
            headers={"Authorization": f"Bearer {ARC_TOKEN}"},
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            print(f"✅ Compaction triggered")
            print(f"   {result.get('message', 'Running')}")
        else:
            print(f"❌ Failed to trigger compaction: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error triggering compaction: {e}")
        return False

    return True


def wait_for_compaction():
    """Wait for compaction to complete"""
    print("\n" + "=" * 80)
    print("Step 2: Waiting for Compaction to Complete")
    print("=" * 80)
    print()

    print("Monitoring compaction progress...")
    start_time = time.time()
    max_wait = 1800  # 30 minutes max
    last_status = None

    while True:
        try:
            response = requests.get(
                f"{ARC_URL}/api/compaction/status",
                headers={"Authorization": f"Bearer {ARC_TOKEN}"},
                timeout=10
            )

            if response.status_code == 200:
                status = response.json()
                active_jobs = status.get("active_jobs", 0)

                # Show status if changed
                if status != last_status:
                    elapsed = time.time() - start_time
                    if active_jobs > 0:
                        print(f"  [{elapsed:6.1f}s] Active jobs: {active_jobs}")
                    last_status = status

                if active_jobs == 0:
                    elapsed = time.time() - start_time
                    print()
                    print(f"✅ Compaction complete! Duration: {elapsed:.1f}s")
                    break
                else:
                    time.sleep(5)
            else:
                print(f"⚠️  Could not check status: {response.status_code}")
                time.sleep(10)

        except Exception as e:
            print(f"⚠️  Error checking status: {e}")
            time.sleep(10)

        # Timeout check
        if time.time() - start_time > max_wait:
            print(f"❌ Timeout waiting for compaction ({max_wait}s)")
            return False

    # Get compaction stats
    try:
        response = requests.get(
            f"{ARC_URL}/api/compaction/stats",
            headers={"Authorization": f"Bearer {ARC_TOKEN}"},
            timeout=10
        )

        if response.status_code == 200:
            stats = response.json()
            print()
            print("Compaction Statistics:")
            print(f"   Total jobs: {stats.get('total_jobs_run', 'N/A')}")
            print(f"   Files compacted: {stats.get('total_files_compacted', 'N/A')}")
            print(f"   Files created: {stats.get('total_files_created', 'N/A')}")
            if 'total_size_reduction_mb' in stats:
                print(f"   Size reduction: {stats['total_size_reduction_mb']:.2f} MB")
    except:
        pass

    return True


def run_clickbench_queries():
    """Run all ClickBench queries and measure performance"""
    print("\n" + "=" * 80)
    print("Step 3: Running ClickBench Queries (Compacted Data)")
    print("=" * 80)
    print()

    queries = load_clickbench_queries()
    print(f"Loaded {len(queries)} queries from {QUERIES_FILE}")
    print()

    # Clear cache before benchmark
    try:
        requests.post(
            f"{ARC_URL}/cache/clear",
            headers={"Authorization": f"Bearer {ARC_TOKEN}"},
            timeout=10
        )
        print("✅ Query cache cleared")
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
                json={"sql": query, "format": "json"},
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
                print(f"ERROR {response.status_code}: {response.text[:100]}")

        except Exception as e:
            results.append(None)
            print(f"ERROR: {e}")

    print()
    print("=" * 80)
    print("Results Summary")
    print("=" * 80)
    print()
    print(f"Total time (compacted): {total_time:.2f}s")
    print(f"Success rate: {sum(1 for r in results if r is not None)}/43")
    print()

    # Save results
    output = {
        "timestamp": datetime.now().isoformat(),
        "database": DATABASE,
        "mode": "compacted",
        "total_time": round(total_time, 2),
        "success_count": sum(1 for r in results if r is not None),
        "queries": results
    }

    output_file = Path(__file__).parent / "clickbench_compacted_results.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"💾 Results saved to: {output_file}")
    print()

    return total_time, results


def main():
    print("=" * 80)
    print("ClickBench Benchmark - Production Mode (Compacted Data)")
    print("=" * 80)
    print()
    print(f"Arc URL: {ARC_URL}")
    print(f"Database: {DATABASE}")
    print()

    # Check prerequisites
    if not ARC_TOKEN:
        print("❌ ARC_TOKEN environment variable not set")
        print("   Usage: export ARC_TOKEN='your-token'")
        sys.exit(1)

    # Check Arc health
    try:
        response = requests.get(f"{ARC_URL}/health", timeout=5)
        if response.status_code != 200:
            print("❌ Arc health check failed")
            sys.exit(1)
        print("✅ Arc is running")
    except:
        print("❌ Arc is not reachable")
        sys.exit(1)

    # Check if queries file exists
    if not QUERIES_FILE.exists():
        print(f"❌ Queries file not found: {QUERIES_FILE}")
        print("   Make sure ClickBench repository is in parent directory")
        sys.exit(1)

    # Run benchmark
    try:
        # Step 1: Trigger compaction
        if not trigger_compaction():
            print("❌ Failed to trigger compaction")
            sys.exit(1)

        # Step 2: Wait for compaction
        if not wait_for_compaction():
            print("❌ Compaction failed or timed out")
            sys.exit(1)

        # Step 3: Run queries
        total_time, results = run_clickbench_queries()

        # Final summary
        print("=" * 80)
        print("FINAL RESULTS")
        print("=" * 80)
        print()
        print(f"✅ ClickBench (compacted): {total_time:.2f}s")
        print(f"   Baseline (no compaction): 22.64s")
        print()

        if total_time < 22.64:
            improvement = ((22.64 - total_time) / 22.64) * 100
            speedup = 22.64 / total_time
            print(f"   🚀 {improvement:.1f}% faster ({speedup:.2f}x speedup)")
        elif total_time > 22.64:
            regression = ((total_time - 22.64) / 22.64) * 100
            print(f"   ⚠️  {regression:.1f}% slower")
        else:
            print(f"   Same performance")

        print()

    except KeyboardInterrupt:
        print("\n\n❌ Benchmark interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
