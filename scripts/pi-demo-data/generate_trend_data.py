#!/usr/bin/env python3
"""
Generate the PI-style trend demo dataset and load it into Arc.

A high-rate historian-shaped dataset for exercising PlotValues / trend queries
(the AF Data Reference use case): irregularly-spaced sensor readings over a full
year, like real PI exception/compression archive data.

3 tags x 365 days, one base reading every ~3 seconds with random sub-second
jitter on the timestamps. Signal is a slow daily sine + hourly sine + noise +
occasional spikes. ~31.5M rows.

Result: db=`historian`, measurement=`sensors`, columns (time, tag, value, status).

This is the dataset for the PlotValues benchmark: bucket a wide time range into
N pixel-bins and return min/max/first/last per bin so a year-long trend renders
from ~1000 rows instead of millions. See README.md for the PlotValues query.

Usage:
    pip install duckdb msgpack
    # start Arc (default http://localhost:8000), then:
    python generate_trend_data.py
    python generate_trend_data.py --arc-url http://host:8000 --token $ARC_TOKEN

Pass --parquet-only to just write a local parquet and skip the Arc load.
Note: 31.5M rows over HTTP takes a few minutes; --parquet-only is much faster
if you want to inspect the data locally first.
"""

import argparse

import duckdb

from arc_ingest import ingest_table

DATABASE = "historian"
MEASUREMENT = "sensors"


def build(con):
    print("generating PI-style trend dataset (3 tags x 365 days, irregular) ...")
    # range() gives a base point every 3s; the MILLISECOND jitter makes the
    # timestamps irregular, the way exception/compression archives are.
    con.execute("""
        CREATE TABLE sensors AS
        SELECT
          TIMESTAMP '2025-01-01 00:00:00'
            + INTERVAL (s) SECOND
            + INTERVAL (CAST(random() * 900 AS BIGINT)) MILLISECOND AS time,
          tag,
          (50
            + 30 * sin(s / 86400.0 * 2 * pi())             -- daily cycle
            + 5  * sin(s / 3600.0  * 2 * pi())             -- hourly cycle
            + (random() - 0.5) * 8                         -- noise
            + CASE WHEN random() < 0.001 THEN (random() - 0.5) * 60
                   ELSE 0 END                              -- occasional spikes
          )::DOUBLE AS value,
          CASE WHEN random() < 0.0005 THEN 1 ELSE 0 END AS status  -- mostly good
        FROM range(0, 365 * 24 * 3600, 3) t(s)
        CROSS JOIN (VALUES ('vibration'), ('temp'), ('pressure')) tags(tag);
    """)
    n = con.execute("SELECT count(*) FROM sensors").fetchone()[0]
    print(f"  generated {n:,} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arc-url", default="http://localhost:8000")
    ap.add_argument("--token", default=None, help="Arc write/admin token (omit if auth disabled)")
    ap.add_argument("--parquet-only", action="store_true",
                    help="write sensors.parquet locally and skip the Arc load")
    args = ap.parse_args()

    con = duckdb.connect()
    build(con)

    source_sql = """
        SELECT (epoch(time) * 1000)::BIGINT AS time, tag, value, status
        FROM sensors ORDER BY time
    """

    if args.parquet_only:
        con.execute(f"COPY ({source_sql}) TO 'sensors.parquet' (FORMAT parquet)")
        print("  wrote sensors.parquet (skipped Arc load)")
        return

    ingest_table(
        con,
        source_sql=source_sql,
        measurement=MEASUREMENT,
        database=DATABASE,
        columns=["time", "tag", "value", "status"],
        arc_url=args.arc_url,
        token=args.token,
    )
    print(f"\nLoaded. See README.md for the PlotValues query against "
          f"{DATABASE}.{MEASUREMENT}.")


if __name__ == "__main__":
    main()
