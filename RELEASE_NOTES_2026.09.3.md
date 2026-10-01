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
