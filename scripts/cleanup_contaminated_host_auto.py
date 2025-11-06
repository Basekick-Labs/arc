#!/usr/bin/env python3
"""
Auto cleanup - removes contaminated h01 host data from sitrack without prompts.
USE WITH CAUTION: This will delete data immediately!
"""

import requests
import json
import time

# Configuration
ARC_URL = "https://arc.basekick.net"
API_KEY = "SUrawdObUZ4ocyvFd46Y0hAeIIdr6KrikK7TEX-tXyE"
DATABASE = "sitrack"
CONTAMINATED_HOST = "h01-customers-basekick-net"

headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}

# Measurements with contaminated data (from scan)
CONTAMINATED_MEASUREMENTS = [
    ('cpu', 75924),
    ('mem', 1291),
    ('disk', 2630),
    ('net', 45164),
    ('netstat', 1376),
    ('system', 3909),
    ('processes', 1383),
    ('kernel', 1279),
    ('swap', 2614),
    ('linux_sysctl_fs', 1306),
]


def delete_data(measurement: str, where_clause: str, dry_run: bool = False, confirm: bool = True):
    """Delete data using Arc's delete API"""
    response = requests.post(
        f"{ARC_URL}/api/v1/delete",
        headers=headers,
        json={
            "database": DATABASE,
            "measurement": measurement,
            "where": where_clause,
            "dry_run": dry_run,
            "confirm": confirm
        }
    )
    response.raise_for_status()
    return response.json()


def main():
    print("="*80)
    print("AUTOMATIC CLEANUP - Removing h01 host from sitrack database")
    print("="*80)
    print(f"Total measurements: {len(CONTAMINATED_MEASUREMENTS)}")
    print(f"Total records to delete: {sum(count for _, count in CONTAMINATED_MEASUREMENTS):,}")
    print("="*80)
    print()

    where_clause = f"host = '{CONTAMINATED_HOST}'"

    successes = 0
    failures = 0
    total_deleted = 0

    for measurement, expected_count in CONTAMINATED_MEASUREMENTS:
        print(f"\n{'='*80}")
        print(f"Cleaning {measurement} ({expected_count:,} records)")
        print(f"{'='*80}")

        try:
            # Delete with confirmation
            result = delete_data(measurement, where_clause, dry_run=False, confirm=True)

            deleted = result.get('deleted_count', 0)
            affected_files = result.get('affected_files', 0)
            rewritten_files = result.get('rewritten_files', 0)
            exec_time = result.get('execution_time_ms', 0)

            print(f"✅ Deleted {deleted:,} records")
            print(f"   Files: {affected_files} affected, {rewritten_files} rewritten")
            print(f"   Time: {exec_time:.1f}ms")

            successes += 1
            total_deleted += deleted

        except Exception as e:
            print(f"❌ Failed: {e}")
            failures += 1

        # Brief pause between measurements
        time.sleep(0.5)

    # Final summary
    print(f"\n{'='*80}")
    print("CLEANUP COMPLETE")
    print(f"{'='*80}")
    print(f"✅ Successfully cleaned: {successes} measurement(s)")
    print(f"❌ Failed: {failures} measurement(s)")
    print(f"Total deleted: {total_deleted:,} records")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAborted by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
