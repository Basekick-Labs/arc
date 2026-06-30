"""
Shared helper: stream a DuckDB table into Arc over the MessagePack columnar
write endpoint (POST /api/v1/write/msgpack).

Used by generate_anomaly_data.py and generate_trend_data.py. Pure stdlib +
msgpack + duckdb; no Arc client library required.

The columnar payload Arc expects is a top-level array of records, each:
    {"m": "<measurement>", "columns": {"<col>": [values...], ...}}
with the target database passed in the `x-arc-database` header. The `time`
column must be epoch milliseconds (int64); Arc stores it as Timestamp(us).
"""

import time
import urllib.request

import msgpack


def ingest_table(
    con,
    *,
    source_sql,
    measurement,
    database,
    columns,
    arc_url="http://localhost:8000",
    token=None,
    chunk_rows=400_000,
):
    """
    Stream rows from a DuckDB query into Arc.

    con          : an open duckdb connection
    source_sql   : SELECT that yields the columns named in `columns`, ordered by time.
                   It must NOT include LIMIT/OFFSET — this helper paginates for you.
    measurement  : Arc measurement name (e.g. "vibration")
    database     : Arc database name (e.g. "factory") -> x-arc-database header
    columns      : list of column names to send, in order. The first MUST be the
                   epoch-millisecond int64 time column (named "time" in Arc).
    arc_url      : base URL of the Arc instance
    token        : optional admin/write API token (Bearer). Omit if auth disabled.
    chunk_rows   : rows per HTTP POST.
    """
    write_url = arc_url.rstrip("/") + "/api/v1/write/msgpack"
    headers = {
        "Content-Type": "application/msgpack",
        "x-arc-database": database,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    total = con.execute(f"SELECT count(*) FROM ({source_sql})").fetchone()[0]
    print(f"  ingesting {total:,} rows into {database}.{measurement} "
          f"in chunks of {chunk_rows:,} ...")

    sent, offset, t0 = 0, 0, time.time()
    while offset < total:
        rows = con.execute(
            f"SELECT {', '.join(columns)} FROM ({source_sql}) "
            f"LIMIT {chunk_rows} OFFSET {offset}"
        ).fetchall()
        if not rows:
            break

        # Transpose row tuples into per-column arrays (columnar payload).
        cols = {name: [] for name in columns}
        for r in rows:
            for i, name in enumerate(columns):
                cols[name].append(r[i])

        payload = msgpack.packb(
            [{"m": measurement, "columns": cols}], use_bin_type=True
        )
        req = urllib.request.Request(write_url, data=payload, headers=headers)
        resp = urllib.request.urlopen(req, timeout=120)
        if resp.status not in (200, 204):
            raise RuntimeError(f"write failed: HTTP {resp.status} {resp.read()[:200]!r}")

        sent += len(rows)
        offset += chunk_rows

    elapsed = time.time() - t0
    rate = sent / elapsed / 1e6 if elapsed else 0
    print(f"  done: {sent:,} rows in {elapsed:.1f}s ({rate:.2f}M rows/sec)")
    return sent
