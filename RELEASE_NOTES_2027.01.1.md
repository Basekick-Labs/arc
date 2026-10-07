# Arc v2027.01.1 Release Notes

> **Status:** Planned — January 2027 release.

## New features

### Backups can be scoped to one or more databases ([#1084](https://github.com/Basekick-Labs/arc/issues/1084))

`POST /api/v1/backup` was whole-instance: its body took only `include_metadata`
and `include_config`. It now also takes `databases`:

```json
{"databases": ["audit"]}
```

A scoped backup copies those databases and nothing else: their data files,
their schema anchors under `_schema/` and their compaction state under
`_compaction_state/` (parked `.quarantined` manifests included). The manifest
records the list in `scope`, the backup list and the status endpoint show it,
and the 202 echoes `databases`. `backup_type` stays `full`. An empty or absent
list is the whole-instance backup, unchanged.

Rules a scoped backup applies:

- `include_metadata` and `include_config` default to **false**. An explicit
  `include_metadata: true` is refused with 400: the SQLite database holds every
  database's tier rows, the tokens, the continuous queries and the audit log,
  so it cannot ride along with one database. `include_config: true` is allowed.
- Every name must pass the storage-segment rule (the one that names an existing
  database; no `..`, no separators, at most 256 names, no duplicates), and must
  be a database this node knows: its hot prefix has a file, or `_schema/<db>/`
  has an anchor, or the tier metadata has rows for it. The third rule is what
  accepts a fully cold database (an audit database with a long retention, the
  case that started this work) instead of answering 400. Its backup completes
  with zero data files: the cold tier is not copied yet (#1086), and the
  INCOMPLETE marker for that arrives with the remote-targets stage (#1085).
  Unknown names answer 400 naming them. The probes are bounded (one indexed
  query, then a listing that stops at the first object the storage would
  return), so a large database does not hold the request. A database whose only files have keys no listing
  can return is still recognised, by a fourth check that runs only after the
  three above said no, so the run fails with the rename advice rather than
  calling the database unknown.
- The databases' Iceberg namespace directories (`<prefix>_<db>.db/`) are
  excluded and counted (`iceberg_namespace_files_excluded`,
  `iceberg_namespaces_excluded` on the manifest): the Iceberg SQL catalog is
  instance-wide and rides with the metadata a scoped backup refuses, so
  restoring those files would land tables no catalog can resolve.
- Scope is the storage-root segment. Edge-sync spoke data lives under
  `<spoke>/<db>/…`, so `databases: ["prod"]` does not include `spoke1/prod`
  and `databases: ["spoke1"]` takes the whole spoke, anchors and compaction
  state included. A spoke namespace below the root segment cannot be named.

Restoring a scoped backup restores only its databases, in either mode. On a
cluster node, a restore of a scoped backup that does not name a `mode` runs in
`replace` — the mode a per-database backup is for (an additive restore of an
audit database resurrects everything retention removed since) — and the 202
echoes the effective mode. An explicit `"mode": "merge"` is honoured; a
standalone node stays additive, as before; an unscoped backup still defaults
to `merge`. In replace mode the current files to remove are selected by the
storage path's first segment when the backup is scoped (a spoke file's
manifest entry carries the canonical database, not the spoke). Replace is
refused, with 400, when the scoped backup holds no data files for one of its
databases: a fully cold database, or one dropped and re-created since, has
nothing in the backup to put back, so replace would only remove its current
files. The message points at `merge`, or at a fresh backup once the database
has hot files again. The cluster-wide compaction pause (#1087) is taken for a
scoped cluster restore exactly as for any other.

A request body that is not JSON (curl's `-d` default is form-encoded) now
answers 400 instead of being read as an empty request: for a scoping feature
the worst outcome is a malformed `databases` silently becoming a
whole-instance backup. An empty body still means the defaults.

Storage backends gained a bounded existence probe (`PrefixProber`) for the
known-database check; the local, S3 and Azure backends implement it and a
backend without it falls back to a listing.

A backup is a copy of the files on disk at the moment each is read: rows
still in the ingest buffers are not in it. "Point in time" means the last
flush.

Not in this stage: a different backup target per database (#1085), the cold
tier (#1086), `arcli backup create --database` (tracked in Basekick-Labs/arcli#44),
and scoping below the storage-root segment.

### Azure Blob Storage takes a key prefix ([#1102](https://github.com/Basekick-Labs/arc/issues/1102))

S3 has always had `storage.s3_prefix`, so one bucket could hold Arc's data
under `arc/` beside something else, or two Arc deployments under disjoint
prefixes. Azure had no equivalent: the key was the blob name, so a container
could hold exactly one Arc deployment and nothing else.

Two new keys close that:

```toml
[storage]
backend = "azure"
azure_container = "arc"
# azure_prefix = "instances/abc123/"

[tiered_storage.cold]
backend = "azure"
# azure_prefix = "cold/"
```

Both default to empty, which is the container root and byte-for-byte the
behaviour you have today. Set one and every key Arc writes, reads, lists,
stats and deletes moves under it, including the one that leaves the process:
the compaction subprocess receives the prefix in its job configuration, so
compaction reads and writes under the prefix rather than at the container root.
The DuckDB secret Arc creates for the query engine is scoped to the container
**and** the prefix rather than the whole container. That last one is what lets a primary store and a cold tier
share one container distinguished only by prefix, which was not previously
expressible.

The prefix is validated, not repaired. A value that cannot form usable keys
(an empty segment, a leading slash, a space, a character outside
`A-Za-z0-9/._-`) is rejected when the configuration loads, with the offending
value named, rather than being silently rewritten into something that writes to
the container root. That applies to the cold-tier key too, and to the two S3
prefix keys, which are now checked in the same place. The cold-tier keys are the
reason the check sits at load: a bad `tiered_storage.cold.s3_prefix` was
reported as a failure to build the backend and then left the node running with
no cold tier at all, which is a silent outage until someone reads that one log
line. A trailing slash is added if you leave it
off, so `arc` and `arc/` are the same destination.

One caveat worth knowing before you pick a value. If a prefix has three or more
segments and the last one looks like a year, such as `tenants/eu/2026/`,
queries against a tiered deployment can return zero rows: the code that works
out which database and measurement a path belongs to scans backwards for a
year-shaped segment and finds yours. A one- or two-segment prefix is
unaffected, because the year then sits too near the front for that scan to use
it. Arc warns at startup if your prefix has the affected shape. This predates
the Azure key and affects `storage.s3_prefix` identically; it is tracked in
[#1108](https://github.com/Basekick-Labs/arc/issues/1108).

### Backup and restore runs have a configurable timeout ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Both runs were bounded by a hardcoded two hours. That is a guess that is
simultaneously too long for an operator who wants a stuck backup to give up
and alert, and too short for a first backup of a large dataset.

```toml
[backup]
# operation_timeout = "2h"
```

The default is the old value, so nothing changes unless you set it. A zero,
negative or unparseable value is a startup error naming the value rather than
an unbounded operation.

## Bug fixes

### Query path extraction no longer folds backslashes ([#750](https://github.com/Basekick-Labs/arc/issues/750))

Query tier selection no longer converts backslashes in storage keys into path
separators, which could identify a different database or measurement. Native
separators remain permitted only within a trusted local storage root; the key
portion retains its slash-separated identity.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#918](https://github.com/Basekick-Labs/arc/pull/918).

### Local storage closes appended files exactly once ([#750](https://github.com/Basekick-Labs/arc/issues/750))

`LocalBackend.AppendReader` previously closed the staging file explicitly on
successful promotion and again through a deferred call. It now closes the file
once after each copy attempt, including failed reads, and checks close errors
before promoting a completed transfer. Failed copies and failed closes retain
the staging file for retry and leave an existing committed file in place.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#919](https://github.com/Basekick-Labs/arc/pull/919).

### Distinct Arrow schemas no longer share an ingest cache entry ([#750](https://github.com/Basekick-Labs/arc/issues/750))

The ingest schema cache now uses deterministic, length-prefixed column identities
instead of ambiguous slice formatting. It includes decimal precision/scale and
canonicalises column and tag ordering, preventing stale Arrow types or metadata
when a measurement's schema changes.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#913](https://github.com/Basekick-Labs/arc/pull/913).

### Fallback compaction job IDs support spoke namespaces ([#750](https://github.com/Basekick-Labs/arc/issues/750))

When a compaction job was created without an explicit JobID, its fallback ID
included the raw database name. An edge-sync pseudo-database such as
`rocket-01/telemetry` introduced a slash into the ID, causing completion-manifest
validation to reject it and creating an unintended nested temporary directory.
Fallback IDs now use the same database-name sanitiser as manager-generated IDs,
while preserving the actual database value and any caller-supplied JobID.

This addresses item 4 of #750. The manager already supplied sanitised IDs;
the fix covers the fallback in `NewJob`.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#912](https://github.com/Basekick-Labs/arc/pull/912).

### Iceberg export reclaims the manifest files expired snapshots leave behind ([#835](https://github.com/Basekick-Labs/arc/issues/835))

Iceberg export never deleted the manifest lists and manifests of snapshots it had expired, so a
table's `metadata/` directory grew with every commit for the life of the deployment and the whole
pile was copied into every backup. On a rig with one live snapshot there were fifteen `.avro` files.

The reconciler now sweeps them. After each pass it computes which manifest lists and manifests are
still reachable — from **every** `metadata.json` left on disk, not just the current snapshot, since
Arc deliberately keeps `retain_snapshots + 1` `v<N>.metadata.json` copies and iceberg-go keeps its
own metadata log as entry points a directory reader can resolve — and deletes the `.avro` files in
that directory that nothing can reach. Measured on the rig above: fifteen files down to ten, with
every retained metadata version still resolving and DuckDB still reading the table.

Three things bound what it will touch. Only files ending `.avro` directly in the table's metadata
directory are ever deleted, so data files (`.parquet`, and outside that directory in any case),
`version-hint.text`, the metadata files themselves and Iceberg's Puffin statistics are not
deletable by this code path at all. A file must be older than a grace period — one hour, or twice
`iceberg.reconcile_interval` if that is longer — so a manifest written by a commit that has not yet
landed its `metadata.json` is left alone. And every failure fails closed: if the listing, a
`metadata.json` or a manifest list cannot be read, the reachable set is incomplete and nothing is
deleted at all. A warning says so; note that a warehouse restored with its `.avro` files lagging
its `metadata.json` files can stay in that state until they are reconciled, since the sweep will
not act on a reachable set it cannot complete.

What the grace does **not** change is the window for a reader that has just resolved an older
`v<N>.metadata.json`: the grace is keyed to a file's age, not to how long it has been unreachable,
and a manifest is usually hours old by the time the last metadata version naming it is retired.
That race is bounded as it was before this change, by `pruneOldVersionFiles` keeping
`retain_snapshots + 1` of those copies so the version a reader just resolved is not the one being
retired.

This bounds the metadata directory rather than emptying it. The retained metadata versions keep
their manifests alive on purpose, so the steady state is on the order of `retain_snapshots`
commits' worth of `.avro` instead of unbounded growth.

Set `iceberg.orphan_sweep_enabled = false` to restore the previous behaviour. It is the one deleter
in the exporter whose work nothing regenerates, so it has an off switch; Arc logs a warning at
startup when it is off.

The second half of #835 — iceberg-go carrying forward manifests that hold only DELETED entries, so
the live manifest list grows with the removal history and readers open every one when planning — is
tracked in [#1106](https://github.com/Basekick-Labs/arc/issues/1106). It is a planning cost, not
disk growth, and the fix belongs upstream.
### A backup fails loudly when compaction recovery state cannot be copied ([#1100](https://github.com/Basekick-Labs/arc/issues/1100))

Object stores cap a key at 1024 bytes, and a backup writes every source key
under a longer destination key. Arc's three copy paths disagreed about what to
do when that destination key would overrun.

Data files checked before writing and skipped, counting and naming what they
skipped. The Iceberg warehouse checked and failed the whole run. Compaction
recovery state did not check at all, so the overrun surfaced from inside the
storage write as a generic "failed to write to backup storage" and failed the
run with a message about storage rather than about the key.

Compaction recovery state now checks before writing and still fails the run,
with an error that names the file, the destination size, the limit, and the
length a source key has to come under. The behaviour is deliberately not the
data path's skip, and the reason is worth stating because it looks like an
inconsistency:

- A skipped **data file** is detectable. It is counted in the manifest, named
  in the skip sample, carried into the `INCOMPLETE` marker, and the source
  still holds it, so you can see what the backup is missing.
- A skipped **recovery manifest** is invisible **to a restore**. It is counted,
  but the restore path reads four other skip fields and not that one, and
  restoring without the manifest brings back a compacted output *next to the
  inputs it replaced* with nothing to reconcile them. That partition then
  serves every row twice, permanently. A third option would have been to skip
  and set the `INCOMPLETE` marker; what rules it out is that a restore does not
  look at this counter.

So the paths differ on whether absence can be noticed, not arbitrarily. Only
the data path skips.

Reaching this at all needs a file with a very long name dropped under
`_compaction_state/`; Arc's own state keys are short and fixed-shape.

### A failed write to a remote backup destination no longer leaves an object behind ([#1101](https://github.com/Basekick-Labs/arc/issues/1101))

Backup cleanup only ran for a destination that stages its writes, which in
practice means a local directory: it removed the `.part` file and stopped.
For an S3 or Azure destination it returned immediately on the grounds that
those do not stage.

That is true and it was the wrong conclusion. Neither commits anything on a
failed upload (the AWS SDK aborts its own multipart, and an Azure block-blob
commit is atomic), but a commit that *succeeds* while its response is lost
leaves an object that no backup references. Cleanup now deletes the
destination key in that case, logs at debug if the delete itself fails, and
never fails the backup over it. The copy of the metadata database, which had
no cleanup at all, is covered too.

Nothing changes for you yet. A backup destination is still always a local
directory, which does stage, so the new branch is unreachable until a backup
can be sent to a named remote target. This lands now because that is the
change it is a prerequisite for.

Restore cleanup deliberately did not change: it is handed live data keys, and
since a failed overwrite leaves the previous object intact, deleting there
would destroy a registered file to clean up a write that did no damage.

### Existing Iceberg tables honor retention changes ([#1093](https://github.com/Basekick-Labs/arc/issues/1093))

`iceberg.retain_snapshots` now updates Iceberg's metadata-file retention properties on existing tables. Reconciliation skips commits when the properties already match.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1095](https://github.com/Basekick-Labs/arc/pull/1095).

### Backup and restore are cluster-safe ([#1083](https://github.com/Basekick-Labs/arc/issues/1083))

On a cluster node a backup or a restore was undefined behaviour: any role
could run one, a restore wrote files into storage without registering them in
the Raft file manifest (so they never replicated, and an enabled reconciliation
sweep removed them as orphans after its grace window), restored files got no
tier row, and a backup taken during a compaction commit copied the compacted
output together with the inputs it replaced, which a restore then served twice
on every node. Nothing changes for a standalone node except that every backup
now also writes a `manifest-files.json` sidecar next to its `manifest.json`.

`POST /api/v1/backup` and `POST /api/v1/backup/restore` now run only on the
primary writer. Standby writers, readers, the compactor, and a node configured
`cluster.role = "standalone"` inside a cluster that is not on shared storage,
answer **503** with `route to the primary writer`, the shape the delete API
uses (409 keeps its meaning of "an operation is already running"). The check is
made per request, so a promotion takes effect without a restart. A primary
demoted while a run is in progress finishes it, forwarding its manifest
operations to the leader; a run started on the new primary meanwhile is not
excluded, so wait for the old primary's run to end before starting one.

A cluster backup waits for its node's manifest to catch up with the Raft leader
(the same barrier replication catch-up uses, with the same timeout), snapshots
the manifest right after the storage listing, and copies only the data files
the manifest lists. A listed data file the manifest does not know is a
compaction or retention input awaiting unlink, a pre-cluster file or a dropped
registration, and is left out, counted in a new `unregistered_skipped` field
with up to 32 names in `unregistered_sample`. A manifest entry this node does
not hold (a new primary still catching up) is counted in `manifest_only_files`
with a sample, and the backup is incomplete by that many files. An empty
manifest while the node lists data files refuses the backup instead of
producing an empty one that reports success, and a cluster node without a Raft
manifest (`cluster.raft_data_dir` unset) runs backups without the cross-check
and restores without registration.

Compaction commits in two Raft phases on two watcher ticks: the output is
registered when written, and the inputs are manifest-deleted on a later tick,
with no wait for peers to pull the output in between. A backup taken between
the two therefore copies the inputs, which are still registered. To close most
of that window, the backup waits for the manifest again and re-reads it once
at the end of the data copy, and settles every provisional decision against
it: a copied data file the manifest has since stopped listing is removed from
the backup again (counted in `left_manifest_during_run` with a sample); a
listed file registered since, or a manifest entry this node has pulled since
the listing, is copied after all; and a file skipped because it could not be
read at copy time but gone from the manifest by then is counted in
`skipped_reconciled` as not missing data. The residual window is a phase 2
that has not landed by the end of the run while the output is already local:
the backup then holds inputs and output next to each other, exactly as the
cluster does at that moment, and a merge restore of it serves those rows twice
until the next compaction cycle on the restored data. The sidecar lists every
data file copied with the SHA-256 of its bytes, its size, and the manifest
entry's database, measurement, partition time and `created_at`; a standalone
backup fills the last four from the path and the backup time.
`GET /api/v1/backup` carries the three new counts. Reserved roots (the
`_schema` anchors, compaction state) are copied as before and never
cross-checked or registered.

A cluster restore waits for the manifest the same way, then registers every
data file it writes from the sidecar, in batches of at most 1000 operations and
256 KiB of payload, the file registrar's own caps, because the primary writer
is routinely a Raft follower and every batch is forwarded to the leader inside
a 1 MiB frame; a batch refused only because a leader election is in progress
is retried for 15 s. A path the manifest already lists keeps the manifest's
database, measurement, partition time and `created_at`. Writes and
registrations interleave per batch. Before a data file is written its bytes
are checked against the sidecar on the way through the temp file: a size or
SHA-256 mismatch, or a file with no sidecar row, is not written at all (the
live copy stays), is counted in `sidecar_mismatches` with a sample, and the
restore ends `failed`; a same-size corruption registered under the sidecar's
hash would otherwise fail every peer's checksum forever. If the manifest
refuses a batch the restore stops there, ends `failed`, and names the
written-but-unregistered files in `registration_failed` and
`registration_failed_sample`; nothing re-registers them, and an enabled
reconciliation sweep (it is opt-in, and report-only until its dry run is turned
off) removes them after its grace window of 24 h plus 5 min of clock skew, so
the recovery is to run the restore again. A restore interrupted between a write
and its batch leaves the same window. A backup taken before this release has no
sidecar and a cluster node refuses to restore it; restore it on a standalone
node or take it again. Every data file a restore writes is also reported to
this node's tier metadata, on a cluster or on a standalone node with tiering,
so the query layer routes to it without waiting for the next tier scan; a path
whose tier row says cold keeps that row, and the restored hot copy is read from
cold until the next tier scan.

Registering a path the manifest already lists is not a no-op: every peer gets
one callback, a stat and, where the SHA differs from the entry it holds, a
re-pull of the file from the restoring node. With
`cluster.query_gate_on_catchup = true` readers answer 503 until they have
converged, and a restore of many files can push the replication queue past its
1024-entry bound, after which the remaining files are only re-discovered by the
reconciliation walk. Plan a large cluster restore for a quiet window.

The restore request gains `mode`. `merge` is the default and today's behaviour:
additive, which on a cluster resurrects every file retention, compaction or the
delete API removed since the backup, on every node. `replace`, cluster nodes
only, first removes the current manifest entries of every database the backup
holds (reason `restore:replace`; the local-delete workers unlink the copies on
every node, and on shared storage the restoring node deletes the objects after
their entries), except the paths the restore is about to write, which are
overwritten and re-registered instead. Files the manifest does not list are
left untouched, and on a shared backend the replaced files' hot tier rows on
the other nodes are not retired until their next tier scan, as after a
retention delete. It refuses to remove anything when the backup is incomplete
by its own manifest (skips it could not reconcile, unaddressable files,
manifest-only files) or by what is in backup storage, and when more than 10%
of the data files the backup node listed were unregistered at backup time,
which means the backup was taken against a stale or partial manifest view. A
path in the backup whose object cannot be read keeps its current bytes and
entry. A compaction job finishing on a restored database could manifest-delete
inputs whose output the restore has just replaced, with no check that the
output is still there; compaction is now paused cluster-wide for the duration
of every cluster restore, automatically (see
[#1087](https://github.com/Basekick-Labs/arc/issues/1087) below), so the
operator no longer has to disable it or stop the compactor. Standalone nodes
refuse `mode: "replace"` with 400.

On a cluster node `restore_metadata` now defaults to false and an explicit
`true` is refused with 400, as is `restore_config: true`: the SQLite database
holds Raft-replicated tokens, this node's tier rows and the audit log, and
`arc.toml` holds `cluster.node_id`, the role, the seeds, `raft_bootstrap` and
the shared secret, so either one from a backup would give the node another
node's state. A client that sends `restore_metadata: true` explicitly by
default will be refused on every cluster node and must stop sending it. A
standalone config restore still writes the literal `arc.toml` in the working
directory, not the path the server was started with.

### Cluster restores pause compaction cluster-wide ([#1087](https://github.com/Basekick-Labs/arc/issues/1087))

Compaction commits in two Raft phases on the compactor: the output is
registered when it is written, and the inputs are manifest-deleted on a later
watcher tick, after the compaction subprocess has already deleted them from
storage, with no check in between that the output is still in the manifest. A
cluster restore whose manifest snapshot fell between the two phases raced the
job. A `replace` restore removed the compacted output (it is not in the
backup) and re-registered the backup's inputs, and the late phase then
manifest-deleted those inputs on every node, leaving the partition empty; in
either mode the late phase could unlink a freshly restored input in the short
gap before its batched register, leaving a manifest entry whose file no node
holds. Stage 0 of the backup work asked the operator to disable compaction or
stop the compactor for a `replace` restore.

Every cluster restore, `merge` and `replace`, now pauses compaction
cluster-wide before its first manifest read and releases the pause after its
last register. The restoring primary proposes the pause through Raft; every
node, readers included, stops starting compaction batches at once, lets the
batch it is running finish (a compaction subprocess is never killed for the
pause: a kill between its two commit phases is exactly the state the pause
exists to prevent), drains the manifest commits it has pending, and
acknowledges. The restore starts only once every node in the cluster node
table has acknowledged, and fails after 10 minutes naming the nodes that did
not: a long batch may still be running on them, they hold compaction commits
their completion watcher is not applying, or they are not on this release.
**Every node of a cluster must run 27.01.1 for a cluster restore**: an older
Raft leader refuses the pause outright and the restore fails at once with a
clear error; an older compactor never acknowledges and the restore fails on
the timeout. A node the RESTORING node's own registry has marked unhealthy or
dead since that node started is not waited for; a node in the cluster node
table that the restoring node has not heard from (for example right after the
restoring node restarted, or a node whose leave never reached the leader) is
waited for, and the restore fails after 10 minutes naming it until an operator
removes it from the cluster. One exception to the unhealthy/dead skip: the
node holding the compactor lease, and every node whose role can compact
(compactor, standalone) while no lease is assigned, is always waited for,
because those are the nodes that run compaction. A dead dedicated compactor
therefore fails every cluster restore on the 10-minute timeout, naming it;
remove it from the cluster, or let the lease move, before restoring. One more
case fails closed the same way: a node that held the compactor lease, lost it
while a compaction job had deleted its inputs from storage but not yet from
the manifest (a `sources_deleted` completion manifest still pending), stops
its completion watcher on the lease loss and so never drains that commit; it
never acknowledges a pause, and every cluster restore fails on the timeout
naming it. Move the lease back to that node (or restart it, when it is a
dedicated compactor) so its watcher applies the pending commit, then restore.

The pause carries a six-minute expiry from the requester's clock and the
restoring node refreshes it every 30 seconds while it runs, so a restore whose
process dies releases compaction within six minutes with no operator action
(six minutes, not less, because every node judges the expiry by its own clock
and the cluster tolerates up to five minutes of skew between nodes). During
those six minutes a restore started from another node is refused with
`compaction is already paused by <node> (restore <id>) until <time>` and
succeeds once the expiry has passed; the same node may retry at once, since a
requester takes over its own pause. If the pause stops being the restore's own
while it runs (another node took over an expired pause, or it could not be
refreshed until it expired), the restore stops at its next manifest batch and ends `failed` saying so, because
a compaction job may have raced it; take a fresh backup and restore again. On
a node that acquires the compactor lease during a pause, or a dedicated
compactor that restarts during one, the scheduler still arms itself and the
first tick after the resume runs. Compaction state is unchanged for standalone
nodes and for cluster nodes without a Raft manifest, where no pause is taken.

What operators see: restore progress (`GET /api/v1/backup/status`) carries a
`compaction_pause` field, `waiting` while the nodes quiesce, `paused` while
the restore runs, `released` once resumed, `lost` when the restore failed for
that reason; `GET /api/v1/cluster` carries a `compaction_pause` object
with the active flag, the requesting node, the reason (`restore <backup-id>`),
the expiry, the node IDs that have acknowledged (`acks`) and those the
requester is still waiting for (`pending_acks`); `GET
/api/v1/compaction/stats` carries `paused`, and a cycle that stopped for the
pause records the status `paused`. `POST /api/v1/compaction/trigger` answers
**409** `compaction is paused cluster-wide` while a pause is in force, and a
scheduled cycle that falls inside one is skipped at Info, not logged as a
failure. The `replace` restore's start-up warning that asked the operator to
disable compaction is gone.

Not in this release: backups do not pause compaction (a backup taken during a
compaction commit still has the residual window described under #1083), and
there are no operator endpoints to pause or resume compaction by hand; the
pause is taken and released by restores only. One residual window remains on
a node that acknowledged a pause and then restarts while it is in force: its
acknowledgement is already recorded, so it is not asked to quiesce again, and
in the seconds between its compaction scheduler arming and its Raft state
catching up with the leader it does not yet see the pause, so a scheduled
tick landing exactly then could start a cycle. Avoid restarting compactor
nodes during a cluster restore.

### The measurement endpoint's `where` parameter no longer rejects values that contain SQL words ([#987](https://github.com/Basekick-Labs/arc/issues/987))

`GET /api/v1/query/:measurement?where=...` pre-filters the clause for
statement-level SQL before the shared validator checks the assembled statement.
That pre-filter ran a plain substring match on the raw, upper-cased text, so it
refused any value containing a forbidden word (`msg = 'created at noon'` failed
on `CREATE`), any identifier containing one (`created_at`), and any value with a
comment marker (`note = 'a--b'`).

Keywords are now matched as whole words on a copy of the clause with its string
literals masked by the same masker the query path uses; `;` and comment markers
are still refused outside a literal, and a keyword glued to a number or a
literal (`1UNION`, `'a'union`) is still seen. The assembled statement still goes
through the shared validator with its file-I/O and replacement-scan checks,
which is where this endpoint's security boundary has been since 26.09.1.

The `xp_`/`sp_` entries are gone from this list. They were SQL Server procedure
prefixes with no meaning to DuckDB, and here they were dead code: lowercase
patterns compared against an upper-cased clause. The delete API's copy of the
same two strings is tracked separately in #1077.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#988](https://github.com/Basekick-Labs/arc/pull/988).

### Delete API WHERE validation no longer rejects SQL words and punctuation inside string literals ([#834](https://github.com/Basekick-Labs/arc/issues/834))

`POST /api/v1/delete` scans the WHERE clause for statement-level SQL before it
interpolates the clause into the DuckDB statement: forbidden keywords (`DROP`,
`UPDATE`, `SET`, ...), `;` and comment markers, and DuckDB's file-I/O table
functions. Those scans ran on the raw text, so a value that merely contained
one of those words was refused as if it were SQL: `status = 'delete-pending'`,
`action = 'update'` or `note = 'a;b--c'` could not be deleted through the API
at all.

The scans now run on the clause with its string literals masked by the same
masker the query path uses, which knows plain `'...'`, escape-string `E'...'`
and dollar-quoted `$tag$...$tag$` forms, so a literal is data whatever it says.
Backtick identifiers are normalised first, as the query path does, and the
file-I/O scan still sees an identifier-quoted call such as `"glob"(...)`. The
raw clause is what reaches DuckDB, and the unmatched-quote and
unmatched-parenthesis checks still run on it. The same syntax outside a literal
is refused exactly as before, including the escaped-quote and dollar-tag shapes
the masker was hardened against in 26.09.1 and 26.09.2.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#937](https://github.com/Basekick-Labs/arc/pull/937).

### A replica refreshes a file whose manifest content changes while it is being pulled ([#798](https://github.com/Basekick-Labs/arc/issues/798))

The puller deduplicated arrivals by path: a manifest update for a path whose
pull was already in flight was dropped as a duplicate, and a rewrite that kept
the file's size was then skipped by the size-only presence check, so a reader
kept serving the old bytes until the next content change of that path. The FSM
now signals a content change (a different checksum or size) separately from the
plain registration, the puller hands an in-flight path over to the newest
version instead of dropping it, a content change bypasses the size check, and a
forced refresh that fails or is dropped is remembered by path so the next
arrival of that path is forced too. A verified copy of the previous version is
kept until its successor is installed, so a failed refresh never leaves the
node without the file. On shared-storage clusters, where every node reads the
writer's own object, forced refreshes are off: the object is never re-uploaded
by a reader.

Two shapes stay outside this fix: a delete followed by a same-size
re-registration of the same path inside the delete grace window, which the
registrar sees as a new file rather than a change; and a rewrite a node learns
of from a Raft snapshot rather than from the log, since a snapshot restore
fires no registration callbacks (#1071 tracks the snapshot side).

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#907](https://github.com/Basekick-Labs/arc/pull/907).

## Internal changes

These do not change how Arc behaves. They are here because the codebase is the
thing a new maintainer has to learn, and a refactor that moves a decision from
four places to one is worth knowing about before you go looking for it in the
old place.

### One shared constructor for storage backends ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Arc talks to three kinds of storage (a local directory, S3 and compatible
stores, Azure Blob Storage) and it needed one of them in four unrelated places:
primary storage, the tiering cold tier, the compaction subprocess, and the
backup destination. Each place had written its own `switch` on a backend name,
so the answer to "which backend names does Arc accept" lived in four files and
differed between them: primary storage took five spellings (`local`, `s3`,
`minio`, `azure`, `azblob`), the cold tier took two, the subprocess took three.

There is now one constructor, `storage.NewBackend`, in
`internal/storage/factory.go`. It takes a `BackendSpec` naming the type and
carrying the backend's own config struct, and it returns a `storage.Backend`
or an error. It deliberately does nothing else: it does not log, does not
register anything for shutdown, and does not decide whether a failure should
stop the process. Those three differ at every call site and they stayed there.
Primary storage still treats a failure as fatal and registers the backend to be
closed at shutdown; the cold tier still logs the failure and runs on without a
cold tier; the backup manager still returns the error to its caller.

The reason this was worth doing is a specific bug class, not tidiness. A Go
interface holding a nil pointer is not equal to nil, so a constructor that
returns `(*S3Backend)(nil)` alongside an error, stored straight into a
`storage.Backend` variable, produces a value that passes `!= nil` and then
panics on first use (#713). The cold-tier code carried a hand-written guard
against exactly this, twice, with a comment naming the three places that check
`coldBackend != nil` — the startup tier scan, the file drainer's existence
probe, and the query router's cold glob. That guarantee is now part of the
constructor's contract and is covered by a test, so the next caller inherits it
instead of having to know about it.

Two things stayed where they were, both on purpose:

- **The compaction subprocess keeps its own factory.** It runs in a separate
  process and receives its job as JSON, and credentials are deliberately never
  written into that JSON: S3 credentials come from the environment the parent
  set, and Azure infers managed-identity use from whether `AZURE_STORAGE_KEY`
  is present. Every other caller passes whatever the operator configured, so
  routing the subprocess through the shared constructor would make its
  empty-credential spec look like a mistake rather than the contract. A comment
  there explains this and points at the shared constructor.
- **Credential refreshing is unrelated to this code.** The refresher
  (`internal/database/credrefresh.go`, from #600 and #601) refreshes *DuckDB
  secrets* so the query engine can keep reading an object store with temporary
  credentials. It never touches a `storage.Backend`: the write path uses either
  static credentials or the AWS SDK's own self-refreshing chain, which is why
  writes never suffered from the bug that motivated it.

This is the first of three changes behind per-database backup targets
([#1085](https://github.com/Basekick-Labs/arc/issues/1085)): a backup
destination is a `storage.Backend`, so named remote targets are mostly a
configuration and routing problem once there is a single place that turns a
destination description into a backend.

### The Azure prefix reaches four places that do not go through the backend ([#1102](https://github.com/Basekick-Labs/arc/issues/1102))

Worth knowing if you ever add a field to a storage backend, because the
compile-time safety you would expect is not there.

Adding the prefix to `AzureBlobBackend` and routing its fourteen
key-taking methods through `prefixedKey` is the easy half. The dangerous half
is that four places build an Azure location or hand an Azure configuration to
something else **without going through the backend object**, so each one keeps
compiling and starts being wrong the moment the prefix is non-empty:

- `AzureBlobBackend.ConfigJSON` and the azure case of
  `internal/compaction/subprocess.go`. Compaction runs in a separate process
  that rebuilds the backend from that JSON. The S3 case had already carried a
  comment about this; the Azure case had never been tested at all. Missing the
  prefix here reroots compaction at the container root, so it reads nothing or
  writes output where queries do not look.
- `iceberg.DefaultWarehouse` in `internal/iceberg/paths.go`, which type-switches
  on the backend to build the warehouse root. Missing the prefix would put table
  metadata at the container root while the data sits under the prefix. This one
  is **not reachable today**: `config.Load` refuses `iceberg.enabled=true`
  unless the primary backend is local, so the object-store arms of that switch
  are dead code until Iceberg export supports object stores. It is fixed anyway,
  because the next person to lift that restriction should not have to find it.
- `azureSecretScope` in `internal/database/duckdb.go` and the sandbox allowlist
  in `internal/database/sandbox.go`.
- Two `cmd/arc/main.go` call sites, for the primary store and the cold tier.

This is the same failure the broken `iceberg.warehouse` key was
([#534](https://github.com/Basekick-Labs/arc/issues/534)): an expression that
is correct only while a new key sits at its default. The defence is the same
one that caught it here, which is running the binary with the key set to
something other than its default.

`ValidateS3Prefix` is now `ValidateObjectPrefix` in
`internal/storage/objectprefix.go`, with no alias, because all three backends
share it and two names for one function is how the next reader comes to believe
there are two rules.
