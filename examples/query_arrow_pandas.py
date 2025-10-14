#!/usr/bin/env python3
"""
Example: Query Arc using Apache Arrow format with Pandas

This example demonstrates how to use Arc's Arrow endpoint for efficient
columnar data transfer directly into Pandas DataFrames.

Performance:
- 7.36x faster for large result sets (100K+ rows)
- 43% smaller payloads compared to JSON
- Zero-copy conversion to Pandas

Requirements:
- pip install requests pyarrow pandas
"""

import os
import requests
import pyarrow as pa
import pandas as pd
from datetime import datetime

# Configuration
ARC_URL = os.getenv("ARC_URL", "http://localhost:8000")
ARC_TOKEN = os.getenv("ARC_TOKEN", "your-token-here")


def query_arrow(sql: str) -> pd.DataFrame:
    """
    Execute SQL query and return Pandas DataFrame using Arrow format

    Args:
        sql: SQL query to execute

    Returns:
        Pandas DataFrame with query results
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
    wait_time = response.headers.get("X-Wait-Time-Ms", "unknown")

    print(f"Response received: {row_count} rows in {exec_time}ms (wait: {wait_time}ms)")
    print(f"Payload size: {len(response.content) / 1024 / 1024:.2f} MB")

    # Parse Arrow IPC stream
    reader = pa.ipc.open_stream(response.content)
    arrow_table = reader.read_all()

    # Convert to Pandas (zero-copy)
    df = arrow_table.to_pandas()

    print(f"DataFrame shape: {df.shape}")
    return df


def main():
    print("=" * 80)
    print("Arc Arrow Query Example with Pandas")
    print("=" * 80)
    print()

    # Example 1: Simple aggregation
    print("Example 1: Time-series aggregation")
    print("-" * 80)

    df = query_arrow("""
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

    # Example 2: Large result set
    print("Example 2: Large result set (demonstrates Arrow performance)")
    print("-" * 80)

    df_large = query_arrow("""
        SELECT *
        FROM cpu
        WHERE time > now() - INTERVAL '1 hour'
        LIMIT 10000
    """)

    print(f"\nDataFrame info:")
    print(df_large.info())
    print()

    # Example 3: Window functions
    print("Example 3: Window functions (moving average)")
    print("-" * 80)

    df_window = query_arrow("""
        SELECT
            time,
            host,
            usage_idle,
            AVG(usage_idle) OVER (
                PARTITION BY host
                ORDER BY time
                ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ) as moving_avg_6
        FROM cpu
        WHERE time > now() - INTERVAL '1 hour'
        ORDER BY time DESC
        LIMIT 1000
    """)

    print(f"\nCalculated moving average for {len(df_window)} records")
    print(df_window.head(10))
    print()

    # Example 4: Cross-measurement join
    print("Example 4: Join CPU and Memory data")
    print("-" * 80)

    df_join = query_arrow("""
        SELECT
            c.time,
            c.host,
            c.usage_idle as cpu_idle,
            m.used_percent as mem_used
        FROM cpu c
        JOIN mem m ON c.time = m.time AND c.host = m.host
        WHERE c.time > now() - INTERVAL '10 minutes'
        ORDER BY c.time DESC
        LIMIT 100
    """)

    print(f"\nJoined data: {len(df_join)} rows")
    print(df_join.head())
    print()

    # Data analysis with Pandas
    print("Example 5: Data analysis with Pandas")
    print("-" * 80)

    print("\nStatistical summary:")
    print(df_join.describe())

    print("\nCorrelation between CPU idle and Memory used:")
    if 'cpu_idle' in df_join.columns and 'mem_used' in df_join.columns:
        correlation = df_join['cpu_idle'].corr(df_join['mem_used'])
        print(f"Correlation coefficient: {correlation:.4f}")

    print("\n" + "=" * 80)
    print("All examples completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()
