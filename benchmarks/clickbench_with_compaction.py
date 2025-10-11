#!/usr/bin/env python3
"""
ClickBench Benchmark with Compaction

Tests Arc's performance on ClickBench queries AFTER running compaction.
This shows production-mode performance with optimized file layout.

Steps:
1. Ingest ClickBench data (100M rows) via MessagePack
2. Trigger compaction (merge small files → large 512MB files)
3. Wait for compaction to complete
4. Run all 43 ClickBench queries
5. Compare with baseline (without compaction)
"""

import os
import sys
import time
import json
import requests
import msgpack
import duckdb
from datetime import datetime
from pathlib import Path

# Configuration
ARC_URL = os.getenv("ARC_URL", "http://localhost:8000")
ARC_TOKEN = os.getenv("ARC_TOKEN")
PARQUET_FILE = os.getenv("PARQUET_FILE", "benchmarks/clickbench/data/hits.parquet")
DATABASE = "clickbench"
BATCH_SIZE = 10000  # Records per batch

# Load queries
SCRIPT_DIR = Path(__file__).parent
QUERIES_FILE = SCRIPT_DIR / "clickbench" / "queries.sql"


def load_clickbench_queries():
    """Load ClickBench queries from queries.sql"""
    with open(QUERIES_FILE) as f:
        queries = []
        for line in f:
            line = line.strip()
            if line and not line.startswith('--'):
                queries.append(line)
        return queries


def check_arc_health():
    """Check if Arc is running"""
    try:
        response = requests.get(f"{ARC_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False


def ingest_clickbench_data():
    """
    Ingest ClickBench data via MessagePack binary protocol.

    This simulates high-throughput production ingestion which creates
    many small Parquet files (similar to real-world scenario).
    """
    print("\n" + "=" * 80)
    print("Step 1: Ingesting ClickBench Data (100M rows)")
    print("=" * 80)
    print(f"Source: {PARQUET_FILE}")
    print(f"Target database: {DATABASE}")
    print(f"Batch size: {BATCH_SIZE:,} records")
    print()

    if not os.path.exists(PARQUET_FILE):
        print(f"❌ Parquet file not found: {PARQUET_FILE}")
        print("Please download it first:")
        print("  wget https://datasets.clickhouse.com/hits_compatible/hits.parquet")
        sys.exit(1)

    # Read Parquet file with DuckDB
    print("Loading Parquet file into memory...")
    conn = duckdb.connect()

    # Get total row count
    total_rows = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{PARQUET_FILE}')").fetchone()[0]
    print(f"Total rows: {total_rows:,}")

    # Read data in batches
    start_time = time.time()
    total_sent = 0
    batch_num = 0

    print("\nIngesting data...")
    for offset in range(0, total_rows, BATCH_SIZE):
        batch_num += 1

        # Read batch from Parquet
        query = f"""
            SELECT * FROM read_parquet('{PARQUET_FILE}')
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """
        result = conn.execute(query).fetchall()
        columns = [desc[0] for desc in conn.description]

        # Convert to MessagePack format
        batch = []
        for row in result:
            record = {
                "m": "hits",  # measurement name
                "t": int(time.time() * 1000),  # current timestamp
                "fields": {},
                "tags": {}
            }

            # Map columns to fields/tags
            for col_name, value in zip(columns, row):
                # Skip None values
                if value is None:
                    continue

                # Simple heuristic: strings are tags, numbers are fields
                if isinstance(value, str):
                    record["tags"][col_name] = value
                else:
                    record["fields"][col_name] = value

            batch.append(record)

        # Send batch to Arc via MessagePack
        payload = {"batch": batch}
        packed = msgpack.packb(payload)

        try:
            response = requests.post(
                f"{ARC_URL}/write/v2/msgpack",
                headers={
                    "Authorization": f"Bearer {ARC_TOKEN}",
                    "Content-Type": "application/msgpack",
                    "x-arc-database": DATABASE
                },
                data=packed,
                timeout=30
            )

            if response.status_code == 204:
                total_sent += len(batch)

                # Progress update every 100 batches
                if batch_num % 100 == 0:
                    elapsed = time.time() - start_time
                    rps = total_sent / elapsed if elapsed > 0 else 0
                    progress = (total_sent / total_rows) * 100
                    print(f"  [{progress:5.1f}%] Sent {total_sent:,} / {total_rows:,} rows "
                          f"({rps:,.0f} records/sec)")
            else:
                print(f"❌ Write failed: {response.status_code} - {response.text}")
                sys.exit(1)

        except Exception as e:
            print(f"❌ Error sending batch: {e}")
            sys.exit(1)

    elapsed = time.time() - start_time
    rps = total_sent / elapsed if elapsed > 0 else 0

    print()
    print(f"✅ Ingestion complete!")
    print(f"   Total rows: {total_sent:,}")
    print(f"   Duration: {elapsed:.1f}s")
    print(f"   Throughput: {rps:,.0f} records/sec")
    print()


def trigger_compaction():
    """Trigger compaction via API"""
    print("\n" + "=" * 80)
    print("Step 2: Triggering Compaction")
    print("=" * 80)
    print(f"Database: {DATABASE}")
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
            print(f"   Status: {result.get('message', 'Running')}")
        else:
            print(f"❌ Failed to trigger compaction: {response.status_code}")
            print(f"   Response: {response.text}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error triggering compaction: {e}")
        sys.exit(1)


def wait_for_compaction():
    """Wait for compaction to complete"""
    print("\n" + "=" * 80)
    print("Step 3: Waiting for Compaction to Complete")
    print("=" * 80)
    print()

    print("Checking compaction status...")
    start_time = time.time()
    max_wait = 1800  # 30 minutes max

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

                if active_jobs == 0:
                    elapsed = time.time() - start_time
                    print(f"✅ Compaction complete! ({elapsed:.1f}s)")
                    break
                else:
                    print(f"   Active jobs: {active_jobs}, waiting...")
                    time.sleep(10)
            else:
                print(f"⚠️  Could not check status: {response.status_code}")
                time.sleep(10)

        except Exception as e:
            print(f"⚠️  Error checking status: {e}")
            time.sleep(10)

        # Timeout check
        if time.time() - start_time > max_wait:
            print(f"❌ Timeout waiting for compaction ({max_wait}s)")
            sys.exit(1)

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
            print(f"   Files before: {stats.get('total_files_compacted', 'N/A')}")
            print(f"   Files after: {stats.get('total_files_created', 'N/A')}")
            print(f"   Size reduction: {stats.get('total_size_reduction_mb', 'N/A')} MB")
    except:
        pass


def run_clickbench_queries():
    """Run all ClickBench queries and measure performance"""
    print("\n" + "=" * 80)
    print("Step 4: Running ClickBench Queries on Compacted Data")
    print("=" * 80)
    print()

    queries = load_clickbench_queries()
    print(f"Loaded {len(queries)} queries")
    print()

    results = []
    total_time = 0

    for i, query in enumerate(queries, 1):
        print(f"Query {i:2d}/43: ", end="", flush=True)

        # Adapt query for Arc
        arc_query = query.replace("hits", f"{DATABASE}.hits")

        try:
            start = time.time()

            response = requests.post(
                f"{ARC_URL}/query",
                headers={
                    "Authorization": f"Bearer {ARC_TOKEN}",
                    "Content-Type": "application/json"
                },
                json={"sql": arc_query, "format": "json"},
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
                print(f"ERROR: {response.status_code}")

        except Exception as e:
            results.append(None)
            print(f"ERROR: {e}")

    print()
    print("=" * 80)
    print("Results Summary")
    print("=" * 80)
    print()
    print(f"Total time: {total_time:.2f}s")
    print(f"Success rate: {sum(1 for r in results if r is not None)}/43")
    print()

    # Save results
    output = {
        "timestamp": datetime.now().isoformat(),
        "database": DATABASE,
        "mode": "compacted",
        "total_time": round(total_time, 2),
        "queries": results
    }

    output_file = SCRIPT_DIR / "clickbench_compacted_results.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Results saved to: {output_file}")
    print()

    return total_time, results


def main():
    print("=" * 80)
    print("ClickBench with Compaction - Production Mode Performance")
    print("=" * 80)
    print()
    print(f"Arc URL: {ARC_URL}")
    print(f"Database: {DATABASE}")
    print()

    # Check prerequisites
    if not ARC_TOKEN:
        print("❌ ARC_TOKEN environment variable not set")
        sys.exit(1)

    if not check_arc_health():
        print("❌ Arc is not running or not reachable")
        print(f"   Check: {ARC_URL}/health")
        sys.exit(1)

    print("✅ Arc is running")

    # Run benchmark
    try:
        # Step 1: Ingest data
        ingest_clickbench_data()

        # Step 2: Trigger compaction
        trigger_compaction()

        # Step 3: Wait for compaction
        wait_for_compaction()

        # Step 4: Run queries
        total_time, results = run_clickbench_queries()

        # Final summary
        print("=" * 80)
        print("FINAL RESULTS")
        print("=" * 80)
        print()
        print(f"✅ ClickBench (with compaction): {total_time:.2f}s")
        print(f"   Baseline (without compaction): 22.64s")

        if total_time < 22.64:
            improvement = ((22.64 - total_time) / 22.64) * 100
            print(f"   Improvement: {improvement:.1f}% faster 🚀")
        else:
            regression = ((total_time - 22.64) / 22.64) * 100
            print(f"   Regression: {regression:.1f}% slower ⚠️")

        print()

    except KeyboardInterrupt:
        print("\n\n❌ Benchmark interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
