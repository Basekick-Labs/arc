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

### Live SQLite backups and restores no longer risk torn database state ([#635](https://github.com/Basekick-Labs/arc/issues/635))

Backups now snapshot live SQLite databases before copying them, so concurrent
WAL writers cannot interleave pages into a backup. Restores replace the database
by rename, remove stale WAL/SHM sidecars, and report that a restart is required
before further writes. Temporary snapshots are restricted to the owner.

Contributed by [@atirna](https://github.com/atirna) in [#678](https://github.com/Basekick-Labs/arc/pull/678).

### Compaction job history no longer retains evicted entries ([#315](https://github.com/Basekick-Labs/arc/issues/315))

When compaction history trims to its newest 100 jobs, the retained map references are now copied into fresh slice backing storage. This prevents an older history slice from sharing the manager's current array and retaining evicted pointer-containing entries; the retained maps are intentionally not deep-copied.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#679](https://github.com/Basekick-Labs/arc/pull/679).

### Local storage directory cache is bounded ([#318](https://github.com/Basekick-Labs/arc/issues/318))

The local backend's directory cache, which avoids redundant `MkdirAll` calls under
sustained ingest load, previously grew without bound as new partition directories
were created. It is now capped at 1,024 entries with eviction on insert; a cache
miss just repeats an idempotent directory creation.

Contributed by [@mah1104ahm](https://github.com/mah1104ahm) in [#674](https://github.com/Basekick-Labs/arc/pull/674).

### Tiered queries now prune partitions per tier ([#662](https://github.com/Basekick-Labs/arc/issues/662))

Since tiering shipped, a query over a measurement with an active cold tier
bypassed partition pruning entirely: both tiers' full globs were scanned on
every query, including S3/Azure LIST and GET calls on the cold archive. The
multi-tier path now prunes each tier against its own backend: hour and day
partitions are generated per tier, existence-filtered with backend-relative
listings, and a tier verified to hold no data for the query's time range is
dropped from the query outright, so a recent-range dashboard query no longer
touches cold object storage at all. File-level time pruning (26.09.2's
`query.file_time_pruning`) applies to the hot tier inside tiered queries too.
Listing failures fail open to the tier's full glob, end-only time predicates
never exclude cold data older than the assumed range start, and completed
tier migrations now invalidate the query caches the same way compaction does.

### Query time zone is now pinned to UTC ([#682](https://github.com/Basekick-Labs/arc/issues/682))

**Behavior change on servers whose host time zone is not UTC.** DuckDB
defaulted its session zone to the host's zone, while Arc's partition pruner,
UTC-hour partition layout, arcxrouter, and JSON output all assume UTC. On a
non-UTC host, a naive timestamp literal (`WHERE time >= '2024-03-15 14:00:00'`)
meant one instant to the pruner and a different one to the engine, so pruned
queries could silently miss matching rows. Every Arc session now runs with
`TimeZone='UTC'`, in the query engine and the compaction subprocess alike.

What changes on non-UTC hosts: naive timestamp literals are always UTC;
`date_trunc`, `time_bucket`, and `::DATE` casts on `TIMESTAMPTZ` bucket at UTC
boundaries, so continuous queries with daily or weekly buckets will align to UTC
midnight from this release onward. Zone-aware predicates remain available via
offset literals (`'2024-03-15 14:00:00+02:00'`) or `AT TIME ZONE`. Query JSON
output is unchanged (it was already normalized to UTC), and hosts already
running in UTC see no change at all.

### Daily-compacted files now register with tiering and migrate to cold ([#683](https://github.com/Basekick-Labs/arc/issues/683))

The tiering scanner only accepted hour-level paths, while the migrator only
moves daily-compacted `*_daily.parquet` files, which daily compaction writes
at day level. On deployments relying on the scan for registration, scheduled
hot-to-cold migration could therefore never find a candidate. The scanner now
registers day-level files (partition time = start of day), and it no longer
re-registers a file as hot when its metadata row already says cold, so a
failed post-migration cleanup stays visible to orphan reconciliation instead
of being re-uploaded every cycle.

### Spoke-namespace files now register with tiering ([#686](https://github.com/Basekick-Labs/arc/pull/686) follow-up)

On an edge-sync hub, spoke data lives one path level deeper than the standard
layout, and the tiering scanner errored on every spoke file. The scanner now
parses partition paths by their date tail (the same approach hub compaction
adopted in #619) and registers spoke files under the query-visible naming
(database = spoke ID). Spoke files are deliberately excluded from hot-to-cold
migration for now: legacy spoke-synced daily files carry sync receipts that a
migration delete would forget, re-introducing upload duplicates; cold
migration of spoke data ships separately with receipt-aware handling. Tiered
queries touching spoke namespaces stay correct and unpruned: a tier may only
be dropped from a query on the strength of a positive pruning result from
another tier.

### Spoke-namespace files now migrate to cold storage ([#687](https://github.com/Basekick-Labs/arc/issues/687))

On an edge-sync hub, spoke daily-compacted files now participate in
hot-to-cold migration. Before a hot copy is removed (by migration or by
orphan reconciliation), its sync receipt is marked the same way hub
compaction marks consumed inputs, so a spoke re-offering the file gets
"already present" instead of re-uploading a duplicate next to the cold copy.
The marking runs before tier metadata flips and before any delete, and a
marking failure aborts the migration batch, so a sync-index outage can never
strand an unmarked deletion. On deployments without an edge-sync hub, spoke
files (if any exist) remain registered but excluded from migration.

### Config-only restores now report `restart_required` (follow-up to [#678](https://github.com/Basekick-Labs/arc/pull/678))

`POST /api/v1/backup/restore` returned `restart_required: true` only when
metadata (SQLite databases) was restored. A config-only restore needs the same
restart — the restored `arc.toml` only takes effect on reload, which
`restoreConfig` already logs — but the response did not say so. The flag now
covers both restore kinds; a data-only restore still omits it. The restored-
database immunity test also fails instead of skipping when a fresh connection
cannot read the restored file.

Contributed by [@atirna](https://github.com/atirna) in [#689](https://github.com/Basekick-Labs/arc/pull/689).
