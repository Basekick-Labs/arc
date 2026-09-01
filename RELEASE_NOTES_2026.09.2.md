# Arc v2026.09.2 Release Notes

> **Status:** Planned — October 2026 patch release.

## New (experimental): file-level time pruning for high-frequency ingest (`query.file_time_pruning`)

**This feature is experimental in 26.09.2**: it ships disabled by default behind
`query.file_time_pruning` and is being soaked continuously in our own dev environment
(a one-second-ingest workload querying it around the clock). Tracking issue for
promotion: [#659](https://github.com/Basekick-Labs/arc/issues/659) — if the multi-week
soak and field feedback hold up, it becomes **stable and enabled by default in 27.01.1**.

Arc's partition pruning narrows queries to hour directories — but a high-frequency
ingest workload (one flush per second) accumulates thousands of small Parquet files in
the **current, not-yet-compacted hour**, and every query listed and footer-read all of
them. Measured on a live one-second workload: the same 5-minute dashboard query cost
19ms at the top of the hour and 340ms+ as the hour filled.

Two new keys (opt-in, local storage backend only):

```toml
[query]
file_time_pruning = false                # enable file-level pruning of the live hour
file_time_pruning_margin_seconds = 300   # writer clock-skew allowance
```

When enabled, Arc expands the one hour-glob containing the query's lower time bound —
only when that hour is the current UTC wall-clock hour — and drops files whose
filename flush-timestamp proves they cannot contain rows in range. **Measured result:
340ms → 29ms (11.7×) over a 7,893-file live hour; a 60-second window runs in 12ms.**
As a side effect, DuckDB's file/object caches stop inflating with the live-hour file
count — resident memory on the same workload dropped from ~450MB to ~90MB.

Safety properties: filename timestamps are flush times, so only the lower bound is
ever pruned (a file written after the range can still hold in-range rows and is always
read); backfill is unaffected (old data arriving now lands in new files, which are
always kept); unrecognized filenames and compacted outputs are always kept; a
zero-survivor result falls back to the unpruned glob. Expanded file lists are never
cached — every query re-lists the live hour, so freshly flushed data is always
visible. The one documented caveat: rows stamped further ahead of the server clock
than the margin may be invisible until their hour closes; size the margin to your
worst writer clock skew.

## Fixed: DuckDB-native `INTERVAL` syntax silently disabled partition pruning

Time-range extraction only recognized SQL-standard quoted intervals
(`INTERVAL '300 seconds'`). The equally valid DuckDB-native unquoted form
(`INTERVAL 300 SECOND`) extracted no time range at all — queries using it got **no
partition pruning whatsoever** and scanned every partition, silently. Both forms now
prune. The unquoted patterns are matched against the literal-masked query text and
require a word boundary after the unit, so interval-lookalike text inside string
literals (e.g. a log-search `LIKE '%INTERVAL 5 MINUTE%'`) and identifiers such as
`interval2` can never be misread as time bounds; compound quoted intervals like
`'1 day 12 hours'` remain unpruned (never mis-pruned as their first component).

## Fixed: Compaction job history no longer retains evicted entries ([#315](https://github.com/Basekick-Labs/arc/issues/315))

When compaction history trims to its newest 100 jobs, the retained map references are now copied into fresh slice backing storage. This prevents an older history slice from sharing the manager's current array and retaining evicted pointer-containing entries; the retained maps are intentionally not deep-copied.
