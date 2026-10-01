# Arc v2026.09.3 Release Notes

> **Status:** Planned — November 2026 patch release.

## New: per-peer replication lag gauges ([#819](https://github.com/Basekick-Labs/arc/issues/819))

The writer now exposes two Prometheus gauges per connected WAL replication
reader, labelled by `peer`: `arc_replication_lag_entries`, the entries the
writer has accepted that the reader has not acknowledged, and
`arc_replication_lag_seconds`, the age of the oldest of those entries since
the writer appended it to its WAL. Both are read from the live connections
at scrape time, so a reader that disconnects leaves no stale series, and a
caught-up reader reads zero on both. The age is taken on the writer's clock
at both ends, so it does not depend on clock agreement between nodes, and
no replication protocol change is involved.

Things to know when alerting on them. A healthy reader's `seconds` sawtooths
between zero and `cluster.replication_ack_interval` (100 ms by default), so
a threshold must exceed a non-default large value of that setting. Lag is
measured from the moment a reader attaches: the sender does not replay
entries accepted before that, so a reader that restarts does not show the
writer's whole history as lag. Once a reader is more than
`cluster.replication_buffer_size` entries behind, the writer no longer holds
the timestamp of the oldest outstanding entry and `seconds` reports the age
of the oldest it still holds, a lower bound that is tight while the buffer
is full; `entries` includes entries the full buffer dropped, which that
reader will never receive on this stream. Pair a `seconds` alert with
`arc_replication_lag_entries >= <cluster.replication_buffer_size>` or
`rate(arc_replication_entries_dropped_total[5m]) > 0`, which are the
saturation signals and are never omitted.

Two fixes came with it. A reader reconnecting under the same ID could be
dropped at once by the old connection's cleanup, costing an extra reconnect
cycle every time; cleanup now removes only the connection it belongs to.
And the `lag` field in the replication status API no longer wraps to a
19-digit number when a reader's acknowledged position is ahead of the
writer's sequence after a writer restart.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#908](https://github.com/Basekick-Labs/arc/pull/908).

## Bug fixes

### A reader could serve old bytes after an in-place rewrite during an active pull ([#798](https://github.com/Basekick-Labs/arc/issues/798))

An in-place rewrite through the delete API could update the manifest while a
reader was still pulling the previous version. The reader now checks the
current manifest version before each attempt and hands the in-flight slot to
the newest content version, so it does not retry or count a superseded pull as
a failure. Same-size content changes are signalled by SHA-256/size changes;
same-size stale copies installed by a Raft snapshot remain outside this fix
because snapshot restore fires no file callbacks.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#907](https://github.com/Basekick-Labs/arc/pull/907).

### A comma cross-join `FROM a, b` read only the first measurement ([#978](https://github.com/Basekick-Labs/arc/issues/978))

Arc resolves measurement names to Parquet paths by rewriting the table after
`FROM` and after each `JOIN`. A table that continued the FROM list after a
comma, the SQL-92 cross-join form `FROM otel_logs a, otel_logs b`, was left as
a bare name, and DuckDB failed the whole query with `Catalog Error: Table with
name otel_logs does not exist`. The explicit `JOIN` spelling of the same query
worked.

The rewriters now resolve comma-continued tables with the same handling as the
`FROM` position: a plain name, a `database.measurement` name, and a quoted
identifier are rewritten; a CTE name, a table function, and a subquery after
the comma are left alone. Whether a comma continues a table list is decided by
the FROM-clause walker the replacement-scan validator already used, so a comma
in a projection, a `GROUP BY`, an `IN` list, a function argument, or DuckDB's
`FROM t SELECT a, b` form is never mistaken for a table. Three related gaps
closed with it. The RBAC permission check now sees the comma-continued table,
so a query is checked against every measurement it reads. The cross-database
check under an `x-arc-database` header now rejects `FROM cpu, otherdb.mem` the
way it rejects `FROM otherdb.mem`. And the no-regex fast path for single-table
queries under that header, which a quote-free comma join used to take, now
defers to the full rewriter.

Two validation gaps in the same table-position logic are closed as well.
Validation now rejects a string literal standing as the table part of a
qualified name (`FROM db.'…'`), in the `FROM`, `JOIN` and comma positions, and
the transform never turns a masked literal into a storage path. And the
replacement-scan check now runs on the normalisation that keeps quoted
identifiers distinct from strings, so a quoted reserved word used as an alias
no longer hides a string literal that follows it in table position. A list or
struct literal inside an `ON` predicate, which that check wrongly rejected
before, is accepted.

Probing that fast path turned up two more shapes it mishandled, fixed with it.
A `JOIN` that starts a new line (`FROM a` then `JOIN b` on the next line) was
not recognised as a join, so only the `FROM` table was rewritten and DuckDB
reported the joined table missing. And a table function in `FROM` position
(`FROM generate_series(1, 10)`) was rewritten as if it were a measurement.
Both affected only a query sent with an `x-arc-database` header and carrying
no string literal or comment, which is what qualified it for that path.

Found while wiring Arc into the SearchBench harness.

### A long but legal source key made every backup fail permanently ([#761](https://github.com/Basekick-Labs/arc/issues/761))

A backup stores each data file under `<backup ID>/data/<source key>`, which is
37 bytes longer than the source key. The storage key limit is 1019 bytes, so a
source key of 983 bytes or more, legal everywhere else in Arc, produced a
destination the backup store refused. That write failure was classified as
fatal, as every backup-storage write failure is, so the run aborted, left a
partial tree with no manifest, and failed the same way on every later attempt.

The backup now checks the destination length before copying and skips such a
file the way it skips a file deleted between the listing and the copy: with a
warning naming the file and the threshold, counted in the manifest's
`skipped_files` (or `skipped_metadata_files` for Iceberg metadata), and
subject to the existing 10% skip-ratio guard, so a deployment where most keys
overrun still fails loudly rather than silently. Every other backup-storage
write failure stays fatal. Arc's own partition layout stays well under the
threshold; this protects against keys placed in the storage root by other
tools.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#903](https://github.com/Basekick-Labs/arc/pull/903).

### A pull that crashed one step before the rename left a file that read as present and was never finalised ([#963](https://github.com/Basekick-Labs/arc/issues/963))

On a per-node-storage cluster, the file puller writes each incoming file to a
`.part` staging file and renames it into place once the last byte and the
checksum are in. Its "already here" test asked the local backend for the
file's size and compared it with the manifest's, and the backend answered with
the staging file's size when the final file was absent, so that an interrupted
download could resume where it stopped. A crash or a kill in the one step
between the last byte and the rename left a staging file of exactly the
manifest's size and no final file. From then on the test said "present": the
worker skipped the pull, the catch-up walks skipped it, nothing renamed it,
and queries on that node, which read `*.parquet`, returned fewer rows with no
error. The storage listing hides staging files, so reconciliation never saw it
either.

Presence now requires the final file. When the backend reports a staged
partial for a path, the puller confirms the final file exists before it calls
the path present; without it the entry is pulled again, and the fresh pull
truncates the stale staging file and renames the result into place. Resuming
a short partial is unchanged, and the S3 and Azure backends, which have no
staging files, are unaffected.

Contributed by [@pujitha24](https://github.com/pujitha24) in [#965](https://github.com/Basekick-Labs/arc/pull/965).
