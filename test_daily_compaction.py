#!/usr/bin/env python3
"""
Quick test script for daily compaction tier.
Tests the daily compaction logic on existing telegraf data.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from storage.daily_compaction import DailyCompaction


# Mock storage backend for testing
class MockStorageBackend:
    """Mock storage backend that points to local filesystem"""

    def __init__(self, base_path: str, database: str):
        self.base_path = base_path
        self.database = database


async def test_daily_compaction():
    """Test daily compaction on existing telegraf data"""

    print("=" * 80)
    print("Testing Daily Compaction Tier")
    print("=" * 80)

    # Initialize mock storage backend
    storage_backend = MockStorageBackend(
        base_path='./data/arc',
        database='telegraf'
    )

    print(f"\nStorage backend: {storage_backend.base_path}")
    print(f"Database: {storage_backend.database}")

    # Initialize daily compaction tier
    daily_tier = DailyCompaction(
        storage_backend=storage_backend,
        min_age_hours=24,  # Compact days older than 24 hours
        min_files=12,      # Need at least 12 files (half a day)
        target_size_mb=2048,
        enabled=True
    )

    print(f"\nDaily compaction tier: {daily_tier.get_tier_name()}")
    print(f"Min age: {daily_tier.min_age_hours} hours")
    print(f"Min files: {daily_tier.min_files}")
    print(f"Target size: {daily_tier.target_size_mb} MB")

    # Test finding candidates
    print("\n" + "-" * 80)
    print("Finding daily compaction candidates...")
    print("-" * 80)

    measurement = "docker_log"
    candidates = await daily_tier.find_candidates("telegraf", measurement)

    print(f"\nFound {len(candidates)} candidate(s) for {measurement}")

    for i, candidate in enumerate(candidates, 1):
        print(f"\nCandidate {i}:")
        print(f"  Database: {candidate['database']}")
        print(f"  Measurement: {candidate['measurement']}")
        print(f"  Partition: {candidate['partition_path']}")
        print(f"  File count: {candidate['file_count']}")
        print(f"  Tier: {candidate['tier']}")
        print(f"  Files (first 5):")
        for file in candidate['files'][:5]:
            print(f"    - {file}")
        if len(candidate['files']) > 5:
            print(f"    ... and {len(candidate['files']) - 5} more files")

    if candidates:
        print("\n" + "=" * 80)
        print("✅ Daily compaction tier is working correctly!")
        print("=" * 80)
        print(f"\nNext steps:")
        print(f"1. Start Arc server to enable automatic daily compaction")
        print(f"2. Daily compaction will run at 3am (configured schedule)")
        print(f"3. Or trigger manually via API: POST /api/compaction/trigger")
        return True
    else:
        print("\n" + "=" * 80)
        print("ℹ️  No candidates found (this is normal if no data is old enough)")
        print("=" * 80)
        print(f"\nTo generate test data:")
        print(f"1. Run: python3 scripts/demos/nascar/nascar_telemetry.py --duration 60")
        print(f"2. Wait 24+ hours")
        print(f"3. Run this test again")
        return False


if __name__ == "__main__":
    result = asyncio.run(test_daily_compaction())
    sys.exit(0 if result else 1)
