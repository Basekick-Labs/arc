# Arc v2026.06.3 Release Notes

> **Status:** In progress.

> **This is a patch release.** A single reliability fix — no new features, no breaking API changes, no schema migrations. Drop-in upgrade from v2026.06.2.

## Reliability fix

**Improved database routing under heavy concurrent writes.** This release fixes an edge case where, under sustained high-throughput concurrent ingest, a write tagged with one database (via the `x-arc-database` header or the `db`/`bucket` query parameter) could occasionally be stored under a different database. The affected name showed up shortened to the length of the intended one — for example, writes meant for `energy_grid` (11 characters) landing under `vessels_tra` or `system_moni`, the leading 11 characters of other databases being written at the same moment.

The cause was a subtle string-lifetime detail at the request boundary. Arc's write handlers read the target database (and, on the TLE and import paths, the measurement) using Fiber's `c.Get` / `c.Query`, which hand back lightweight strings that point into the request buffer and are only meant to be used while the handler runs. Arc's ingest path keeps that string a little longer — the background Arrow-buffer flush uses it to pick the storage directory for the database — and if another request reused the buffer in between, the value could change out from under it. Copying the value at the boundary resolves it completely.

The behavior only showed up under concurrency with buffer reuse, so a steady or moderate write stream was very unlikely to see it, while a rapid burst of large payloads alongside other concurrent writers could hit it more often. It touched the MessagePack, line-protocol (native and InfluxDB-compatible), TLE, and bulk-import (CSV / Parquet / line-protocol / TLE) write paths. Reads and queries were never affected. There are no configuration, API, or on-disk format changes, and the fix is a drop-in upgrade.

If you happen to find rows under an unexpected database or measurement directory after running heavy concurrent ingest on an earlier version, those files are ordinary, intact Parquet and can simply be re-ingested once you're on 26.06.3.
