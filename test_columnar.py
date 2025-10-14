#!/usr/bin/env python3
"""
Test script for columnar MessagePack format

Usage:
    python test_columnar.py --columnar    # Test columnar format (FAST)
    python test_columnar.py --row         # Test row format (LEGACY)
"""

import msgpack
import requests
import time
from datetime import datetime, timezone

# Configuration
ARC_URL = "http://localhost:8000/write/v2/msgpack"
API_KEY = "your-api-key-here"  # Update this

def test_columnar_format(num_batches=100, batch_size=1000):
    """
    Test columnar format (RECOMMENDED)

    Expected: 25-35% faster than row format
    """
    print(f"\n🚀 Testing COLUMNAR format ({num_batches} batches × {batch_size} records)")

    start_time = time.time()
    total_records = 0

    for batch_idx in range(num_batches):
        # Generate columnar data
        base_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

        payload = {
            "m": "cpu",
            "columns": {
                "time": [base_ts + i for i in range(batch_size)],
                "region": ["us-east"] * batch_size,
                "datacenter": [f"dc{i % 5}" for i in range(batch_size)],
                "usage_idle": [95.0 - (i % 10) for i in range(batch_size)],
                "usage_user": [3.0 + (i % 5) for i in range(batch_size)],
                "usage_system": [2.0 + (i % 3) for i in range(batch_size)],
            }
        }

        # Serialize to MessagePack
        packed = msgpack.packb(payload)

        # Send to Arc
        response = requests.post(
            ARC_URL,
            data=packed,
            headers={
                "Content-Type": "application/msgpack",
                "x-api-key": API_KEY
            }
        )

        if response.status_code != 204:
            print(f"❌ Batch {batch_idx} failed: {response.status_code} {response.text}")
            break

        total_records += batch_size

        if (batch_idx + 1) % 10 == 0:
            elapsed = time.time() - start_time
            rps = total_records / elapsed
            print(f"  [{batch_idx + 1}/{num_batches}] {total_records:,} records | {rps:,.0f} RPS")

    elapsed = time.time() - start_time
    rps = total_records / elapsed

    print(f"\n✅ Columnar test complete:")
    print(f"   Total records: {total_records:,}")
    print(f"   Elapsed time: {elapsed:.2f}s")
    print(f"   Throughput: {rps:,.0f} records/sec")

    return rps


def test_row_format(num_batches=100, batch_size=1000):
    """
    Test row format (LEGACY)

    Expected: Slower due to flattening + row→column conversion
    """
    print(f"\n🐢 Testing ROW format ({num_batches} batches × {batch_size} records)")

    start_time = time.time()
    total_records = 0

    for batch_idx in range(num_batches):
        # Generate row data
        base_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

        batch = []
        for i in range(batch_size):
            record = {
                "m": "cpu",
                "t": base_ts + i,
                "fields": {
                    "usage_idle": 95.0 - (i % 10),
                    "usage_user": 3.0 + (i % 5),
                    "usage_system": 2.0 + (i % 3),
                },
                "tags": {
                    "region": "us-east",
                    "datacenter": f"dc{i % 5}"
                }
            }
            batch.append(record)

        payload = {"batch": batch}

        # Serialize to MessagePack
        packed = msgpack.packb(payload)

        # Send to Arc
        response = requests.post(
            ARC_URL,
            data=packed,
            headers={
                "Content-Type": "application/msgpack",
                "x-api-key": API_KEY
            }
        )

        if response.status_code != 204:
            print(f"❌ Batch {batch_idx} failed: {response.status_code} {response.text}")
            break

        total_records += batch_size

        if (batch_idx + 1) % 10 == 0:
            elapsed = time.time() - start_time
            rps = total_records / elapsed
            print(f"  [{batch_idx + 1}/{num_batches}] {total_records:,} records | {rps:,.0f} RPS")

    elapsed = time.time() - start_time
    rps = total_records / elapsed

    print(f"\n✅ Row test complete:")
    print(f"   Total records: {total_records:,}")
    print(f"   Elapsed time: {elapsed:.2f}s")
    print(f"   Throughput: {rps:,.0f} records/sec")

    return rps


if __name__ == "__main__":
    import sys

    if "--row" in sys.argv:
        row_rps = test_row_format()
    elif "--columnar" in sys.argv:
        columnar_rps = test_columnar_format()
    else:
        print("=" * 80)
        print("Columnar vs Row Format Benchmark")
        print("=" * 80)

        row_rps = test_row_format()
        time.sleep(2)  # Brief pause between tests
        columnar_rps = test_columnar_format()

        print("\n" + "=" * 80)
        print("📊 Results:")
        print("=" * 80)
        print(f"Row format:      {row_rps:>12,.0f} records/sec (baseline)")
        print(f"Columnar format: {columnar_rps:>12,.0f} records/sec")
        print(f"Improvement:     {columnar_rps / row_rps:>12.1%} faster")
        print("=" * 80)
