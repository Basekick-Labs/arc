#!/usr/bin/env python3
"""
Example: Query Arc using Apache Arrow format with Polars

Polars is even faster than Pandas for large datasets thanks to:
- Lazy evaluation
- Better parallelization
- Lower memory footprint
- Native Arrow support

Requirements:
- pip install requests pyarrow polars
"""

import os
import requests
import pyarrow as pa
import polars as pl
from datetime import datetime

# Configuration
ARC_URL = os.getenv("ARC_URL", "http://localhost:8000")
ARC_TOKEN = os.getenv("ARC_TOKEN", "your-token-here")


def query_arrow_polars(sql: str) -> pl.DataFrame:
    """
    Execute SQL query and return Polars DataFrame using Arrow format

    Args:
        sql: SQL query to execute

    Returns:
        Polars DataFrame with query results
    """
    print(f"Executing query...")
    print(f"SQL: {sql[:100]}...")

    response = requests.post(
        f"{ARC_URL}/query/arrow",
        headers={"Authorization": f"Bearer {ARC_TOKEN}"},
        json={"sql": sql},
        timeout=300
    )

    if response.status_code != 200:
        raise Exception(f"Query failed: {response.status_code} - {response.text}")

    # Get metadata from headers
    row_count = response.headers.get("X-Row-Count", "unknown")
    exec_time = response.headers.get("X-Execution-Time-Ms", "unknown")

    print(f"Response: {row_count} rows in {exec_time}ms")
    print(f"Payload: {len(response.content) / 1024 / 1024:.2f} MB")

    # Parse Arrow IPC stream
    reader = pa.ipc.open_stream(response.content)
    arrow_table = reader.read_all()

    # Convert to Polars (zero-copy, very fast!)
    df = pl.from_arrow(arrow_table)

    print(f"DataFrame shape: {df.shape}")
    return df


def main():
    print("=" * 80)
    print("Arc Arrow Query Example with Polars")
    print("=" * 80)
    print()

    # Example 1: Simple aggregation
    print("Example 1: Time-series aggregation")
    print("-" * 80)

    df = query_arrow_polars("""
        SELECT
            time_bucket(INTERVAL '1 hour', time) as hour,
            host,
            AVG(usage_idle) as avg_cpu_idle,
            MAX(usage_user) as max_cpu_user,
            COUNT(*) as sample_count
        FROM cpu
        WHERE time > now() - INTERVAL '24 hours'
        GROUP BY hour, host
        ORDER BY hour DESC
        LIMIT 100
    """)

    print("\nFirst 5 rows:")
    print(df.head())
    print()

    # Example 2: Lazy evaluation for large datasets
    print("Example 2: Lazy evaluation (Polars superpower)")
    print("-" * 80)

    # Query large dataset
    df_large = query_arrow_polars("""
        SELECT *
        FROM cpu
        WHERE time > now() - INTERVAL '6 hours'
        LIMIT 100000
    """)

    # Lazy operations - computed only when needed
    result = (
        df_large
        .lazy()
        .filter(pl.col("usage_idle") < 50)
        .group_by("host")
        .agg([
            pl.col("usage_idle").mean().alias("avg_idle"),
            pl.col("usage_user").max().alias("max_user"),
            pl.count().alias("count")
        ])
        .sort("avg_idle")
        .collect()  # Execute the lazy plan
    )

    print(f"\nProcessed {len(df_large)} rows, result: {len(result)} hosts")
    print(result)
    print()

    # Example 3: High-performance filtering and sorting
    print("Example 3: Fast filtering and complex operations")
    print("-" * 80)

    df = query_arrow_polars("""
        SELECT time, host, usage_idle, usage_user, usage_system
        FROM cpu
        WHERE time > now() - INTERVAL '1 hour'
        LIMIT 50000
    """)

    # Complex operations in Polars (very fast!)
    result = (
        df
        .filter(
            (pl.col("usage_idle") < 80) &
            (pl.col("usage_user") > 10)
        )
        .with_columns([
            (pl.col("usage_user") + pl.col("usage_system")).alias("total_active"),
            (100 - pl.col("usage_idle")).alias("cpu_busy")
        ])
        .sort("cpu_busy", descending=True)
        .head(20)
    )

    print(f"\nTop 20 busiest CPU samples:")
    print(result)
    print()

    # Example 4: Statistical analysis
    print("Example 4: Statistical analysis")
    print("-" * 80)

    df = query_arrow_polars("""
        SELECT host, usage_idle, usage_user
        FROM cpu
        WHERE time > now() - INTERVAL '30 minutes'
        LIMIT 10000
    """)

    # Polars makes stats easy
    print("\nDescriptive statistics:")
    print(df.describe())

    print("\nPer-host statistics:")
    stats = (
        df
        .group_by("host")
        .agg([
            pl.col("usage_idle").mean().alias("avg_idle"),
            pl.col("usage_idle").std().alias("std_idle"),
            pl.col("usage_user").mean().alias("avg_user"),
            pl.count().alias("samples")
        ])
        .sort("avg_idle")
    )
    print(stats)
    print()

    # Example 5: Time-based operations
    print("Example 5: Time-based aggregations")
    print("-" * 80)

    df = query_arrow_polars("""
        SELECT
            time,
            host,
            usage_idle
        FROM cpu
        WHERE time > now() - INTERVAL '2 hours'
        ORDER BY time
        LIMIT 10000
    """)

    # Resample to 5-minute intervals
    result = (
        df
        .sort("time")
        .group_by_dynamic("time", every="5m", group_by="host")
        .agg([
            pl.col("usage_idle").mean().alias("avg_idle"),
            pl.count().alias("sample_count")
        ])
    )

    print(f"\nResampled to 5-minute intervals: {len(result)} rows")
    print(result.head(10))
    print()

    print("=" * 80)
    print("All Polars examples completed!")
    print("Note: Polars is optimized for large datasets with lazy evaluation")
    print("=" * 80)


if __name__ == "__main__":
    main()
