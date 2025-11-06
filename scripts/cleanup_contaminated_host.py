#!/usr/bin/env python3
"""
Clean up contaminated host data from sitrack database.

This script removes all records for h01-customers-basekick-net from the sitrack
database across all measurements. This host should only exist in the telegraf
database, not sitrack.

Root Cause: Race condition in compaction (fixed in commit 4e670b2)
"""

import requests
import json
import time
from typing import Dict, List

# Configuration
ARC_URL = "https://arc.basekick.net"
API_KEY = "SUrawdObUZ4ocyvFd46Y0hAeIIdr6KrikK7TEX-tXyE"
DATABASE = "sitrack"
CONTAMINATED_HOST = "h01-customers-basekick-net"

# Headers for API requests
headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}


def query_arc(query: str, database: str = DATABASE) -> Dict:
    """Execute SQL query against Arc"""
    response = requests.post(
        f"{ARC_URL}/api/v1/query",
        headers=headers,
        json={"database": database, "sql": query}  # Arc API uses "sql" not "query"
    )

    # Print detailed error if query fails
    if response.status_code != 200:
        print(f"Query failed: {query}")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")

    response.raise_for_status()
    return response.json()


def delete_data(measurement: str, where_clause: str, dry_run: bool = True, confirm: bool = False) -> Dict:
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


def find_contaminated_measurements() -> List[tuple]:
    """Find all measurements with contaminated host data"""
    print(f"Scanning {DATABASE} database for contaminated measurements...")

    # Common Telegraf measurements
    measurements = [
        'cpu', 'mem', 'disk', 'diskio', 'net', 'netstat', 'nstat',
        'system', 'processes', 'kernel', 'swap', 'internal',
        'linux_sysctl_fs', 'docker_log'
    ]

    contaminated = []

    for measurement in measurements:
        try:
            # Check if measurement has contaminated records
            # Query with full database.measurement format
            check_query = f"SELECT COUNT(*) as count FROM {DATABASE}.{measurement} WHERE host = '{CONTAMINATED_HOST}'"
            result = query_arc(check_query, database=DATABASE)

            # Arc API returns dict with 'data' containing list of lists
            if isinstance(result, dict) and 'data' in result and len(result['data']) > 0:
                # Data is list of lists: [[75924]]
                count = result['data'][0][0]
                if count > 0:
                    contaminated.append((measurement, count))
                    print(f"  ✗ {measurement}: {count:,} contaminated records")
                else:
                    print(f"  ✓ {measurement}: clean")
            else:
                print(f"  ✓ {measurement}: clean (no data)")

        except Exception as e:
            # Measurement might not exist, skip silently
            if '422' in str(e) or 'not found' in str(e).lower():
                print(f"  ⊘ {measurement}: does not exist")
            else:
                print(f"  ? {measurement}: error checking - {e}")

    return contaminated


def cleanup_measurement(measurement: str, count: int):
    """Clean up a single measurement"""
    where_clause = f"host = '{CONTAMINATED_HOST}'"

    print(f"\n{'='*80}")
    print(f"Cleaning up {measurement} ({count:,} records)")
    print(f"{'='*80}")

    # 1. Dry run first
    print(f"\n1. Dry run to verify...")
    try:
        dry_result = delete_data(measurement, where_clause, dry_run=True)
        print(f"   Dry run results:")
        print(f"   - Would delete: {dry_result['deleted_count']:,} rows")
        print(f"   - Affected files: {dry_result['affected_files']}")
        print(f"   - Execution time: {dry_result['execution_time_ms']:.1f}ms")

        if dry_result['deleted_count'] != count:
            print(f"   ⚠️  WARNING: Count mismatch! Expected {count:,}, dry run found {dry_result['deleted_count']:,}")

    except Exception as e:
        print(f"   ❌ Dry run failed: {e}")
        return False

    # 2. Ask for confirmation
    print(f"\n2. Ready to delete {dry_result['deleted_count']:,} records from {measurement}")
    response = input(f"   Proceed? [y/N]: ")

    if response.lower() != 'y':
        print(f"   ⏭️  Skipped by user")
        return False

    # 3. Actual delete
    print(f"\n3. Deleting records...")
    try:
        delete_result = delete_data(measurement, where_clause, dry_run=False, confirm=True)
        print(f"   ✅ Delete completed:")
        print(f"   - Deleted: {delete_result['deleted_count']:,} rows")
        print(f"   - Affected files: {delete_result['affected_files']}")
        print(f"   - Rewritten files: {delete_result['rewritten_files']}")
        print(f"   - Execution time: {delete_result['execution_time_ms']:.1f}ms")

        return True

    except Exception as e:
        print(f"   ❌ Delete failed: {e}")
        return False


def main():
    """Main cleanup process"""
    print("="*80)
    print("Arc Database Cleanup - Remove Contaminated Host Data")
    print("="*80)
    print(f"Database: {DATABASE}")
    print(f"Host to remove: {CONTAMINATED_HOST}")
    print(f"Arc URL: {ARC_URL}")
    print("="*80)
    print()

    # Check if delete operations are enabled
    try:
        config_response = requests.get(
            f"{ARC_URL}/api/v1/delete/config",
            headers=headers
        )
        config = config_response.json()

        if not config.get('enabled'):
            print("❌ ERROR: Delete operations are disabled!")
            print("   Enable in arc.conf:")
            print("   [delete]")
            print("   enabled = true")
            return

        print(f"✅ Delete operations enabled")
        print(f"   Confirmation threshold: {config.get('confirmation_threshold', 'N/A'):,}")
        print(f"   Max rows per delete: {config.get('max_rows_per_delete', 'N/A'):,}")
        print()

    except Exception as e:
        print(f"⚠️  Warning: Could not check delete config: {e}")
        print("   Proceeding anyway...")
        print()

    # Find contaminated measurements
    contaminated = find_contaminated_measurements()

    if not contaminated:
        print("\n✅ No contaminated measurements found!")
        print(f"   All measurements in {DATABASE} are clean.")
        return

    # Summary
    total_records = sum(count for _, count in contaminated)
    print(f"\n{'='*80}")
    print(f"Summary: Found {len(contaminated)} contaminated measurement(s)")
    print(f"Total contaminated records: {total_records:,}")
    print(f"{'='*80}")

    # Confirm before proceeding
    print(f"\nReady to clean up {len(contaminated)} measurement(s)")
    response = input(f"Proceed with cleanup? [y/N]: ")

    if response.lower() != 'y':
        print("Aborted by user")
        return

    # Clean up each measurement
    successes = 0
    failures = 0

    for measurement, count in contaminated:
        if cleanup_measurement(measurement, count):
            successes += 1
        else:
            failures += 1

        # Brief pause between measurements
        time.sleep(0.5)

    # Final summary
    print(f"\n{'='*80}")
    print("Cleanup Complete!")
    print(f"{'='*80}")
    print(f"✅ Successfully cleaned: {successes} measurement(s)")
    print(f"❌ Failed: {failures} measurement(s)")
    print()

    # Verify cleanup
    print("Verifying cleanup...")
    remaining = find_contaminated_measurements()

    if not remaining:
        print(f"\n✅ SUCCESS! All contaminated records removed from {DATABASE} database")
        print(f"   Host '{CONTAMINATED_HOST}' no longer exists in {DATABASE}")
    else:
        print(f"\n⚠️  WARNING: {len(remaining)} measurement(s) still have contaminated data:")
        for measurement, count in remaining:
            print(f"   - {measurement}: {count:,} records")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAborted by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
