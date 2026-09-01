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

## Fixed: Live SQLite backups and restores no longer risk torn database state

Backups now snapshot live SQLite databases before copying them, so concurrent
WAL writers cannot interleave pages into a backup. Restores replace the database
by rename, remove stale WAL/SHM sidecars, and report that a restart is required
before further writes. Temporary snapshots are restricted to the owner.

## Bug fixes

### Iceberg version hints no longer advance before metadata copies publish ([#636](https://github.com/Basekick-Labs/arc/issues/636))

Directory-based Iceberg readers fetch `version-hint.text` before opening the matching
`v<N>.metadata.json` copy. A transient metadata read or copy failure could previously
still advance the hint, leaving those readers pointed at an unavailable snapshot.

Arc now publishes the hint only after the matching metadata copy succeeds. The previous
hint remains valid during the failure, and the existing reconciliation retry path
self-heals once storage recovers.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#663](https://github.com/Basekick-Labs/arc/pull/663).

### Invalid Iceberg reconcile intervals now fail at config load

When Iceberg export is enabled, `iceberg.reconcile_interval` must be positive.
Zero and negative values are rejected instead of silently falling back to the
five-minute scheduler interval.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#664](https://github.com/Basekick-Labs/arc/pull/664).

### Iceberg skips unreadable databases during reconciliation

An unreadable database no longer aborts the entire Iceberg reconcile pass. Arc
logs the database and continues reconciling the remaining databases, while a
failure enumerating the top-level database list remains fatal for that pass.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#665](https://github.com/Basekick-Labs/arc/pull/665).

### Dedicated SQLite WAL and SHM sidecars are owner-only

Dedicated SQLite handles now apply 0600 permissions to the database and its
WAL/SHM sidecars, including when the database path uses a symlink. Missing
sidecars remain harmless, and auth-owned handles are not reopened or modified.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#666](https://github.com/Basekick-Labs/arc/pull/666).

### Empty Iceberg measurements cache their negative catalog result

Permanently empty measurement directories with no Iceberg table now cache that
negative state, avoiding repeated catalog lookups while preserving normal table
creation after re-ingest and cache cleanup when the measurement disappears.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#667](https://github.com/Basekick-Labs/arc/pull/667).

### Azure not-found error detection handles joined multi-errors ([#319](https://github.com/Basekick-Labs/arc/issues/319))

Azure not-found error detection (`isAzureNotFoundError`) now uses `errors.As` instead
of a custom single-Unwrap loop, so an `azcore.ResponseError` is correctly identified
even when wrapped inside joined multi-errors (`errors.Join`).

Contributed by [@Thundercloud12](https://github.com/Thundercloud12) in [#670](https://github.com/Basekick-Labs/arc/pull/670).
