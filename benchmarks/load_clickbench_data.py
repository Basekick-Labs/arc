#!/usr/bin/env python3
"""
Load ClickBench Data into Arc

Ingests 100M rows from hits.parquet into Arc via MessagePack binary protocol.
This creates many small Parquet files in Arc's storage (realistic production scenario).
"""

import os
import sys
import time
import requests
import msgpack
import duckdb
from datetime import datetime

# Configuration
ARC_URL = os.getenv("ARC_URL", "http://localhost:8000")
ARC_TOKEN = os.getenv("ARC_TOKEN")
PARQUET_FILE = os.getenv("PARQUET_FILE", "../historian_product/benchmarks/clickbench/data/hits.parquet")
DATABASE = "benchmark"
MEASUREMENT = "hits"
BATCH_SIZE = 5000  # Smaller batches for faster progress updates

def main():
    print("=" * 80)
    print("ClickBench Data Ingestion into Arc")
    print("=" * 80)
    print()
    print(f"Source: {PARQUET_FILE}")
    print(f"Target: {ARC_URL}")
    print(f"Database: {DATABASE}")
    print(f"Measurement: {MEASUREMENT}")
    print(f"Batch size: {BATCH_SIZE:,} records")
    print()

    # Check prerequisites
    if not ARC_TOKEN:
        print("❌ ARC_TOKEN not set")
        print("   Usage: export ARC_TOKEN='your-token'")
        sys.exit(1)

    if not os.path.exists(PARQUET_FILE):
        print(f"❌ Parquet file not found: {PARQUET_FILE}")
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

    print()
    print("=" * 80)
    print("Loading Parquet file...")
    print("=" * 80)
    print()

    # Connect to DuckDB and read Parquet
    conn = duckdb.connect()

    # Get total row count
    total_rows = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{PARQUET_FILE}')"
    ).fetchone()[0]

    print(f"Total rows to ingest: {total_rows:,}")
    print()

    # Start ingestion
    print("=" * 80)
    print("Ingesting data...")
    print("=" * 80)
    print()

    start_time = time.time()
    total_sent = 0
    batch_num = 0
    failed_batches = 0

    for offset in range(0, total_rows, BATCH_SIZE):
        batch_num += 1

        # Read batch from Parquet
        query = f"""
            SELECT * FROM read_parquet('{PARQUET_FILE}')
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """

        try:
            result = conn.execute(query).fetchall()
            columns = [desc[0] for desc in conn.description]
        except Exception as e:
            print(f"\n❌ Error reading Parquet at offset {offset}: {e}")
            failed_batches += 1
            continue

        # Convert to MessagePack format
        batch = []
        for row in result:
            record = {
                "m": MEASUREMENT,
                "t": int(time.time() * 1000),  # Current timestamp
                "fields": {},
                "tags": {}
            }

            # Map columns to fields/tags
            for col_name, value in zip(columns, row):
                if value is None:
                    continue

                # Heuristic: strings -> tags, numbers -> fields
                if isinstance(value, str):
                    # Limit tag size
                    record["tags"][col_name] = value[:500] if len(value) > 500 else value
                else:
                    record["fields"][col_name] = float(value) if isinstance(value, int) else value

            batch.append(record)

        # Send to Arc
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
                timeout=60
            )

            if response.status_code == 204:
                total_sent += len(batch)
            else:
                print(f"\n❌ Write failed at offset {offset}: {response.status_code}")
                failed_batches += 1

        except Exception as e:
            print(f"\n❌ Error sending batch at offset {offset}: {e}")
            failed_batches += 1

        # Progress update every 50 batches
        if batch_num % 50 == 0:
            elapsed = time.time() - start_time
            rps = total_sent / elapsed if elapsed > 0 else 0
            progress = (total_sent / total_rows) * 100
            eta_seconds = (total_rows - total_sent) / rps if rps > 0 else 0
            eta_minutes = eta_seconds / 60

            print(f"[{progress:5.1f}%] {total_sent:,} / {total_rows:,} rows | "
                  f"{rps:,.0f} rec/s | ETA: {eta_minutes:.1f}m | "
                  f"Batch: {batch_num:,} | Failed: {failed_batches}")

    elapsed = time.time() - start_time
    rps = total_sent / elapsed if elapsed > 0 else 0

    print()
    print("=" * 80)
    print("Ingestion Complete!")
    print("=" * 80)
    print()
    print(f"Total rows sent: {total_sent:,} / {total_rows:,}")
    print(f"Duration: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"Throughput: {rps:,.0f} records/sec")
    print(f"Failed batches: {failed_batches}")
    print()

    if total_sent == total_rows:
        print("✅ All data loaded successfully!")
    else:
        print(f"⚠️  Loaded {(total_sent/total_rows)*100:.1f}% of data")

    print()
    print("Next steps:")
    print("  1. Trigger compaction: python benchmarks/clickbench_compacted_only.py")
    print("  2. Or run full benchmark with compaction")


if __name__ == "__main__":
    main()
