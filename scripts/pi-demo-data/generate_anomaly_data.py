#!/usr/bin/env python3
"""
Generate the anomaly-detection demo dataset and load it into Arc.

3 machines x 60 days of vibration readings (mm/s), one every 5 seconds, with a
clean baseline (daily thermal cycle + 8-hour shift cycle + sensor noise) plus
TWO deliberately injected faults:

  * compressor-01 : a sudden 6-hour excursion to ~11 mm/s on 2025-05-31
                    (think: a valve event).
  * pump-01       : a gradual bearing degradation over 2025-06-10..15, ramping
                    from 4.5 up to ~7.5 mm/s, then snapping back after a repair.

Result: db=`factory`, measurement=`vibration`, columns (time, machine, value).
~3.1M rows. This is the exact dataset behind the anomaly-detection tutorial,
so the z-score / MAD / seasonal-residual queries reproduce as written.

Usage:
    pip install duckdb msgpack
    # start Arc (default http://localhost:8000), then:
    python generate_anomaly_data.py
    python generate_anomaly_data.py --arc-url http://host:8000 --token $ARC_TOKEN

Pass --parquet-only to just write a local parquet and skip the Arc load.
"""

import argparse

import duckdb

from arc_ingest import ingest_table

DATABASE = "factory"
MEASUREMENT = "vibration"


def build(con):
    print("generating anomaly dataset (3 machines x 60 days @ 5s) ...")
    # Base grid: one row per machine per 5 seconds across 60 days.
    con.execute("""
        CREATE TABLE base AS
        SELECT
          TIMESTAMP '2025-05-01 00:00:00' + INTERVAL (s) SECOND
            + INTERVAL (CAST(random() * 2000 AS BIGINT)) MILLISECOND AS time,
          m.machine,
          s AS sec
        FROM range(0, 60 * 24 * 3600, 5) t(s)
        CROSS JOIN (VALUES ('pump-01'), ('pump-02'), ('compressor-01')) m(machine);
    """)

    # Vibration signal in mm/s: baseline 4.5 + daily + shift cycles + noise,
    # plus the two injected faults and rare transient sensor glitches.
    con.execute("""
        CREATE TABLE vibration AS
        SELECT time, machine,
          GREATEST(0.1,
            4.5
            + 0.8 * sin(sec / 86400.0 * 2 * pi())          -- daily thermal cycle
            + 0.4 * sin(sec / 28800.0 * 2 * pi())          -- 8-hour shift cycle
            + (random() - 0.5) * 0.6                       -- sensor noise

            -- FAULT 1: pump-01 bearing degradation, ramps up over 5 days
            + CASE WHEN machine = 'pump-01'
                   AND time BETWEEN TIMESTAMP '2025-06-10' AND TIMESTAMP '2025-06-15'
                THEN 3.0 * (epoch(time) - epoch(TIMESTAMP '2025-06-10'))
                          / (5 * 86400.0)
                ELSE 0 END

            -- FAULT 2: compressor-01 sudden 6-hour spike event
            + CASE WHEN machine = 'compressor-01'
                   AND time BETWEEN TIMESTAMP '2025-05-31 02:00:00'
                                AND TIMESTAMP '2025-05-31 08:00:00'
                THEN 6.5 ELSE 0 END

            -- transient point glitches (rare)
            + CASE WHEN random() < 0.0002 THEN random() * 8 ELSE 0 END
          )::DOUBLE AS value
        FROM base;
    """)

    n = con.execute("SELECT count(*) FROM vibration").fetchone()[0]
    print(f"  generated {n:,} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arc-url", default="http://localhost:8000")
    ap.add_argument("--token", default=None, help="Arc write/admin token (omit if auth disabled)")
    ap.add_argument("--parquet-only", action="store_true",
                    help="write vibration.parquet locally and skip the Arc load")
    args = ap.parse_args()

    con = duckdb.connect()
    build(con)

    # Order by time so Arc's hour-partitioning lands cleanly.
    source_sql = """
        SELECT epoch_ms(time)::BIGINT AS time, machine, value
        FROM vibration ORDER BY time
    """

    if args.parquet_only:
        con.execute(f"COPY ({source_sql}) TO 'vibration.parquet' (FORMAT parquet)")
        print("  wrote vibration.parquet (skipped Arc load)")
        return

    ingest_table(
        con,
        source_sql=source_sql,
        measurement=MEASUREMENT,
        database=DATABASE,
        columns=["time", "machine", "value"],
        arc_url=args.arc_url,
        token=args.token,
    )
    print(f"\nLoaded. Try a query:\n"
          f'  curl -X POST {args.arc_url}/api/v1/query \\\n'
          f'    -H "content-type: application/json" -H "x-arc-database: {DATABASE}" \\\n'
          f"""    -d '{{"sql":"SELECT machine, count(*), round(avg(value),2) FROM {MEASUREMENT} GROUP BY machine"}}'""")


if __name__ == "__main__":
    main()
