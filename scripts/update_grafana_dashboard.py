#!/usr/bin/env python3
"""
Update Grafana dashboard to work with int64 timestamps.

Converts:
- DATE_TRUNC('minute', time) → DATE_TRUNC('minute', CAST(time AS TIMESTAMP))
- $__timeFilter(time) → time >= epoch_us($__timeFrom()) AND time < epoch_us($__timeTo())

Usage:
    python scripts/update_grafana_dashboard.py input.json output.json
"""

import json
import re
import sys
from pathlib import Path


def update_query(sql: str) -> str:
    """
    Update SQL query to work with int64 timestamps.

    Args:
        sql: Original SQL query

    Returns:
        Updated SQL query
    """
    if not sql or not isinstance(sql, str):
        return sql

    # Replace DATE_TRUNC with CAST version
    # Pattern: DATE_TRUNC('...', time)
    # Replace: DATE_TRUNC('...', CAST(time AS TIMESTAMP))
    sql = re.sub(
        r"DATE_TRUNC\s*\(\s*'([^']+)'\s*,\s*time\s*\)",
        r"DATE_TRUNC('\1', CAST(time AS TIMESTAMP))",
        sql,
        flags=re.IGNORECASE
    )

    # Replace $__timeFilter(time) with epoch_us version
    # Old: $__timeFilter(time)
    # New: time >= epoch_us($__timeFrom()) AND time < epoch_us($__timeTo())
    sql = re.sub(
        r'\$__timeFilter\s*\(\s*time\s*\)',
        r"time >= epoch_us($__timeFrom()) AND time < epoch_us($__timeTo())",
        sql
    )

    # Also handle LAG/LEAD window functions
    # Pattern: LAG(AVG(...)) OVER (PARTITION BY ... ORDER BY DATE_TRUNC('...', time))
    # Keep the window ORDER BY clause unchanged (already has CAST)

    return sql


def update_dashboard(dashboard: dict) -> dict:
    """
    Recursively update all SQL queries in dashboard.

    Args:
        dashboard: Grafana dashboard JSON

    Returns:
        Updated dashboard
    """
    if isinstance(dashboard, dict):
        for key, value in dashboard.items():
            if key in ('rawSql', 'sql', 'query') and isinstance(value, str):
                # Found a SQL query, update it
                updated = update_query(value)
                if updated != value:
                    dashboard[key] = updated
                    print(f"Updated {key}:")
                    print(f"  Before: {value[:100]}...")
                    print(f"  After:  {updated[:100]}...")
                    print()
            else:
                # Recurse into nested structures
                dashboard[key] = update_dashboard(value)
    elif isinstance(dashboard, list):
        return [update_dashboard(item) for item in dashboard]

    return dashboard


def main():
    if len(sys.argv) != 3:
        print("Usage: python update_grafana_dashboard.py input.json output.json")
        sys.exit(1)

    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2])

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    print(f"Reading dashboard from: {input_file}")
    with open(input_file, 'r') as f:
        dashboard = json.load(f)

    print(f"\nUpdating queries...")
    print("=" * 80)
    updated_dashboard = update_dashboard(dashboard)

    print("=" * 80)
    print(f"\nWriting updated dashboard to: {output_file}")
    with open(output_file, 'w') as f:
        json.dump(updated_dashboard, f, indent=2)

    print(f"\nDone! Import {output_file} into Grafana.")
    print("\nNote: You may need to:")
    print("  1. Update the dashboard UID/ID if importing as new")
    print("  2. Test queries after import")
    print("  3. Adjust any custom queries not caught by this script")


if __name__ == '__main__':
    main()
