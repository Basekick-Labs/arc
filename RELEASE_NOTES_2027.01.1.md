# Arc v2027.01.1 Release Notes

> **Status:** Planned — January 2027 release.

## Upgrade note: two configurations now refuse to start

Read this before upgrading if you set either `storage.local_path` or
`backup.local_path`. Both refusals apply only when `backup.enabled` is true,
which is the default, and both are decided on **resolved absolute** paths:
relative spellings, trailing slashes and symlinks are normalised first, and in
the container image the working directory is `/app`.

**1. The backup destination and the primary storage root may no longer be the
same directory, and neither may contain the other.** With `backup.local_path`
left at its default `./data/backups`, that means Arc refuses to start if
`storage.local_path` is `/app/data`, `/app`, `.`, `data`, `./data`, or empty. In
practice the one realistic break is a single-volume deployment that points
`ARC_STORAGE_LOCAL_PATH` at the whole mount instead of a subdirectory of it.

Sibling names are fine: `./data/arc` beside `./data/arc-backups` starts
normally. Different kinds never overlap, so a local backup directory with S3
primary storage starts normally. The cold-tier half of the check applies only
when both `tiered_storage.enabled` and `tiered_storage.cold.enabled` are on.

**2. `backup.local_path` may no longer be empty or whitespace-only unless a
backup target is configured.** That combination previously started with the
backup API silently disabled. An empty *environment variable* still falls
through to the default, so only an empty value in the config file, or a stray
space, triggers this.

To fix either: move one path outside the other, point `backup.default_target` at
a configured target, or set `backup.enabled = false`, which skips the check
entirely.

**Every shipped artifact is unaffected.** The stock `arc.toml` uses `./data/arc`
and `./data/backups`; the Helm charts and Kubernetes manifests set
`/app/data/storage` or `/app/data/arc` and leave `backup.local_path` at its
default.

Why the refusal exists: a backup destination inside the storage root makes each
backup copy the previous one, so the inventory compounds every run. Worse, those
copies look like ordinary data files to the reconciliation sweep, which deletes
them as orphans when reconciliation is enabled and dry-run is off. The check
costs one comparison at startup and removes a class of silent data loss.

## New features

### Scheduled edge-to-hub replication for paid licenses ([#828](https://github.com/Basekick-Labs/arc/issues/828))

Network spokes can now replicate automatically. Like continuous-query and
retention scheduling, this requires a valid paid license: Starter,
Professional, Enterprise, or Unlimited, including the license grace period.
Manual `POST /api/v1/spoke-sync/run` and air-gap bundle export remain available
without a license. Bundle export stays manual.

With `edge_sync.spoke.enabled = true` and a valid paid license, the first
scheduled pass starts after `edge_sync.spoke.sync_interval` (default `5m`).
Failed or incomplete passes retry from `edge_sync.spoke.sync_retry_interval`
(default `30s`), doubling up to the normal interval and resetting after a
completed pass. Both durations must be at least one second, and the retry
interval must be shorter than the normal interval. Environment overrides are
`ARC_EDGE_SYNC_SPOKE_SYNC_INTERVAL` and
`ARC_EDGE_SYNC_SPOKE_SYNC_RETRY_INTERVAL`.

Each tick rechecks the paid-license entitlement and primary-writer role.
Losing either cancels an active scheduled pass; a running scheduler resumes
when eligibility returns. Manual and scheduled passes share an overlap guard:
a busy manual request returns HTTP 409. Shutdown cancels and joins scheduled
work before closing the ledger. Failed deliveries do not release the
compaction defer gate.

Scheduled spokes expose `arc_edgesync_spoke_scheduler_enabled`,
`arc_edgesync_spoke_last_success_timestamp_seconds`, and
`arc_edgesync_spoke_pass_failures_total` in Prometheus, with corresponding
`edge_sync_spoke_*` JSON metrics. The success timestamp advances only after
validated hub contact and a complete pass; an empty backlog, partial transfer,
or conflict cannot report successful replication. Skipped overlaps and
eligibility cancellations do not count as hub failures. These metrics are
absent when the scheduler was not started.

This combines work by [@jallegri](https://github.com/jallegri) in
[#1119](https://github.com/Basekick-Labs/arc/pull/1119) and
[@efegokdemir](https://github.com/efegokdemir) in
[#924](https://github.com/Basekick-Labs/arc/pull/924), with shared paid-license
checks and additional regression coverage.

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

Not in this stage: the cold tier (#1086), `arcli backup create --database`
(tracked in Basekick-Labs/arcli#44), and scoping below the storage-root
segment. A different backup target per database landed separately in #1085,
below.

### Backups carry the cold tier ([#1086](https://github.com/Basekick-Labs/arc/issues/1086))

A backup used to copy hot storage only. On a deployment with tiered storage on,
everything that had been migrated to the cold tier was simply not in it — which
for a long-retention database like an audit log meant most of the data. Arc now
backs up the cold tier too, and a restore puts each file back on the tier it
came from.

The manifest says how much came from cold:

```json
{
  "total_files": 1812,
  "cold_files": 412,
  "cold_size_bytes": 8294400
}
```

`cold_files` is also in the backup listing and per destination in `GET
/api/v1/backup/:id`, so with per-database routing you can see which target
holds the cold slice.

**Restores.** A file the backup read from cold goes back to this node's cold
tier, and its tier row is recorded — that row is what makes it queryable, so no
manual tiering scan is needed afterwards. Restoring onto a node with **no**
cold tier is still fine: those files land in hot storage and the status reports
`cold_files_restored_to_hot`, so the data is all there and all hot. Two smaller
counts cover the cases where a file arrives but stays unreadable:
`cold_restore_quarantine_skipped` (the path has a quarantined tier row, which a
restore must not resurrect) and `cold_rows_not_recorded` (the bytes landed but
the row could not be written).

**What this changes operationally.** Backups of tiered deployments get bigger
and slower, by roughly the size of your cold tier. There is deliberately no
switch to turn it off: a backup that silently omits most of a database is the
problem this fixes. If you want a smaller backup, scope it with `databases`.

**An unreachable cold store now fails the backup** rather than quietly
producing a smaller one. A missing hot file is tolerated and skipped, because
compaction or retention may legitimately have removed it between the listing
and the copy; nothing in Arc ever deletes a cold object, so a cold read that
fails means the store is unreachable, and continuing would report success over
missing data.

Three reconciliation counts appear when this node's tier metadata and its cold
store disagree. `cold_objects_unrecorded` counts objects in the cold store that
have no tier row — they are **backed up anyway**, because the store is the
authority on what exists, and refusing them would mean a node with incomplete
metadata backs up no cold data at all. `cold_files_excluded` now counts the
reverse: tier rows whose object is missing from the store, i.e. data that is
genuinely gone — and `cold_rows_stale_but_hot` counts the third case, a row
that says cold whose file is still in hot storage and therefore **is** in the
backup. That last one matters because without it the backup would report a file
it holds as permanently gone. On a standalone node, or a cluster without shared
storage or replication, an unrecorded object is a permanent condition rather
than a lag — the cold-metadata sync that would record it does not run there.

`cold_files_excluded` keeps its per-database breakdown in
`cold_files_excluded_databases`, so tooling built on the stage-B field keeps
working.

**Restore cold files on the primary writer.** A cold restore leaves the tier
row that makes the file readable, and if the node already had a hot copy at
that key the row flips hot-to-cold, leaving the hot copy stale. Tiering's
orphan reconciliation removes it — but that runs only on the migrating node,
and restores are not writer-gated, so a restore on a follower leaves the stale
copy behind.

Needs the tiered-storage licence, which tiering already required. A node
without it backs up no cold data and says nothing about it, the same hole the
`cold_files_excluded` marker has.

### A backup says how many cold-tier files it is not carrying ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

A backup copies hot storage. On a deployment with tiered storage on, the files
that have been migrated to the cold tier are not in it, and until now nothing
in the backup said so: a manifest that read `total_files: 1400` was describing
1400 hot files and however many cold ones you had, silently.

Every backup manifest now carries `cold_files_excluded`, and
`cold_files_excluded_databases` breaks it down per database, because a single
total does not tell you which database has data the backup is missing:

```json
{
  "backup_id": "backup-20261007-141500",
  "total_files": 1400,
  "cold_files_excluded": 412,
  "cold_files_excluded_databases": { "audit": 400, "metrics": 12 }
}
```

The figure also appears in the backup list, in `GET
/api/v1/backup/:id` per target (so you can see which destination holds the
gap, if you route databases to different targets), and in a warning the
restore logs:

```
WARN The backup being restored did not carry these cold-tier files: a backup
     copies hot storage only ... backup_cold_files_excluded=412
```

Three things to know about it.

**It does not block anything.** In particular a replace-mode restore of a
partly-cold backup still succeeds. Replace deletes the current files of the
databases it restores, so it refuses to run from a backup that is *incomplete*
— but a cold-tier object is not something the backup failed to get, it is
something no backup carries yet, and replace cannot delete it either (it
deletes through the cluster file manifest, which a migrated file has already
left). Refusing here would prevent nothing and would make tiered storage and
replace-mode restore mutually exclusive.

**It is the reporting node's view.** The count comes from this node's tier
metadata. On a cluster where one node migrates and the others learn about it
through the cold-tier metadata sync, a node whose sync has not run yet reports
a lower number. Take the backup on the primary writer for the full picture.

**The cold tier is still not backed up.** That is the next piece of this work
([#1086](https://github.com/Basekick-Labs/arc/issues/1086)). Until it lands,
`cold_files_excluded` is how you size what a restore from this backup would not
bring back — and the cold objects themselves are untouched and still readable
where they are.

The field is absent in four cases, which the JSON cannot tell apart: tiering
is off, nothing has been migrated, the count could not be taken (the backup
still completes and a warning names the failure) — and **this node has no
tiered-storage licence**. That last one is worth knowing, because it is where
the marker would help most: tiering is licence-gated at startup, so an
unlicensed node reports nothing even though its cold objects and tier metadata
are still there and its migrations have stopped. **An absent field is not
evidence that nothing was migrated.** A licence that lapses while Arc is
running keeps reporting; the next restart goes quiet.

### A different backup target per database ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

A backup had one destination. Now each configured target may name the databases
whose files go to it, and everything else goes to `backup.default_target`:

```toml
[backup]
default_target = "main"

[backup.targets.main]
type = "local"
local_path = "/srv/arc-backups"

[backup.targets.audit]
type = "s3"
s3_bucket = "acme-arc-audit"
databases = ["audit", "compliance"]
```

`databases` also takes a comma-separated string, and
`ARC_BACKUP_TARGETS_AUDIT_DATABASES=audit,compliance` overrides whatever the
file says. A database may be named by exactly one target: naming it twice is
refused at startup, naming it on the default target is allowed and does
nothing. The names are storage-root segments, exactly as a scoped backup names
them, so an edge-sync spoke is named as the **spoke** — `["prod"]` does not mean
`spoke1/prod`. A database's own field schema anchors and compaction recovery
state travel with its data to its target.

The instance-wide state always goes to the default target, because it belongs
to no one database: the SQLite database, the Iceberg SQL catalog, Iceberg table
metadata, an out-of-root Iceberg warehouse, and `arc.toml`.

**What a routed backup writes.** One manifest and one file sidecar per target,
each describing that target's own slice and each naming the whole run, so a
manifest found on its own is self-describing. Plus `<backup_id>/index.json` on
the default target, written before the first copy, naming every target the run
touched. A target that receives no files still commits an empty manifest and
sidecar, so a routed database whose name is a typo shows up as a leg with
`total_files: 0` rather than as a target the run failed to reach.

**The listing** returns one entry per backup id whatever it is spread across,
with `targets` naming every destination that holds a slice of it (`target`
stays set only when there is exactly one). It fans out over every target
concurrently, so one unreachable store no longer consumes the request budget
and cancels the rest: it is named in `unreachable_targets` and the response is
still 200 with everything the others hold. 503 is now only for a listing where
nothing could be read at all. A run that did not commit to every target it
named appears in `incomplete_runs`, marked `aborted` or `possibly_in_flight` —
the second because a cluster reader listing a shared destination has no view of
the primary's live run and must not report a healthy backup as aborted. A
target that would not answer is reported as `unknown_targets` on that entry
rather than as `missing_targets`: "missing" means the listing looked and found
no manifest, so a store that is briefly down no longer makes a complete backup
read as a run that did not finish.

**`GET /api/v1/backup/:id`** still answers the manifest at the top level, now
the merged view of the whole run, with a `targets` array of the per-target
slices beside it. **A delete** sweeps every target under one lock, and names
the ones it could not reach having deleted where it could, so a second delete
finishes the job.

**A restore** reads every target of the run and assembles the merged view
before any of its gates, so a scoped backup that lives on one target is not
mistaken for a backup holding nothing. It **refuses before writing anything**
when a target of the run is not configured on this node, will not answer, or
holds no manifest for the id. Restoring only the reachable part would report
success over a set it did not restore, and in replace mode would delete the
live files of a database whose backup bytes are on the target it could not
read. A backup with a single destination resolves no names at all, so one
whose target has since been renamed still restores.

`include_config` now defaults to false when **any** configured target is
remote, not only when the default one is: `arc.toml` carries every target's
credentials, so a local default beside one remote routed target still means
copying it puts the keys to that store inside a backup it holds. Asking for it
explicitly is honoured and warns, naming each remote target.

`/api/v1/backup/status` gains a `targets` array while a routed backup runs:
per-target files, bytes, skips and status.

A **target nothing is routed to is inert**: no backup writes to it, and a
warning at startup names it. It is deliberately not a refusal — adding the
target block in one commit and its `databases` in the next is ordinary — and
deliberately not a backup leg either. Every leg is probed before anything is
copied, so taking every configured target as a leg would make an unrelated
store a precondition of every backup in the instance, whole-instance ones
included.

In a listing, an entry whose targets could not all be read now says so.
`targets` names every destination **the run recorded**, the single-target
`target` field is cleared when there is more than one, and `partial_view: true`
marks an entry whose file and byte counts are summed over fewer legs than that.
The matching `incomplete_runs` entry says which target is unaccounted for, and
its `state` is `undetermined` when nothing is known to be missing — a complete
backup read while one store is briefly down is not a run that failed.

Only the **comma** separates names in `databases`. A database name may contain
a space, so `databases = "my db"` is one name and `databases = ["a", "b"]` is
two.

Startup now also refuses two backup targets that contain one another — same
bucket with one prefix a parent of the other — for the reason it already
refuses an overlap with primary storage or the cold tier: the two would be one
listing, so each leg would enumerate the other's objects and deleting one
backup id would reach both. Two targets in one bucket under disjoint prefixes
stay legal.

Not in this stage: `cold_files_excluded` (#1086 is still the cold tier), backup
scheduling, and incremental backups.

### A backup can be written somewhere other than a local directory ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Until now a backup went to `backup.local_path`, a directory on the server
itself. A named target can now point it at S3, MinIO, Azure Blob Storage, or a
different local path.

```toml
[backup]
default_target = "audit"

[backup.targets.audit]
type = "s3"                   # local | s3 | minio | azure | azblob
s3_bucket = "acme-arc-audit-backups"
s3_region = "eu-west-1"
s3_prefix = "arc/"            # optional, and it counts against the key budget
```

With a target configured, `backup.local_path` is not required at all. Nothing in
the backup path needs local scratch space: the database snapshot is staged
beside the database, and a restore stages beside its destination.

Two details worth knowing before you write a target.

**Target names take lowercase letters, digits and underscore only.** A hyphen is
refused on purpose, and this contradicts the example in the original issue. The
reason is that configuration keys are lowercased and a hyphen cannot appear in
an environment variable name, so a hyphenated target could be set in a file and
then never be overridable from the environment. `audit_bucket`, not
`audit-bucket`.

**A target is refused at startup if it overlaps primary storage or the cold
tier.** Overlap means the same store and bucket with one prefix equal to or a
parent of the other, or, for local paths, one directory containing the other.
The same bucket with disjoint prefixes is fine. See the upgrade note above for
what this means for an existing deployment.

The field names match the cold tier's, so `s3_bucket` rather than `bucket`. From
the environment alone it takes three variables, because a target name cannot be
discovered from a configuration file that does not exist:

```
ARC_BACKUP_TARGET_NAMES=audit
ARC_BACKUP_TARGETS_AUDIT_TYPE=s3
ARC_BACKUP_TARGETS_AUDIT_S3_BUCKET=acme-arc-audit-backups
ARC_BACKUP_DEFAULT_TARGET=audit
```

Three behaviours change when the target is remote. `include_config` defaults to
false, because `arc.toml` carries that target's credentials — with per-database
routing the rule widened to **any** configured target being remote, as
described above. The usable source-key length shrinks by the length of the
target prefix, and the figure Arc reports to you accounts for it. And a prefix
long enough to leave no room for a backup's own keys is refused when the
configuration loads rather than later.

**Backups now record which instance wrote them,** so two Arc instances sharing
one bucket and prefix do not merge their listings. The identity is
`cluster.cluster_name` on a cluster and a generated identifier otherwise, which
means **you should set `cluster.cluster_name`**: two unrelated clusters both
left at the default would read as one.

A restore never adopts the identity of the backup it reads, so recovering onto
replacement hardware leaves the new machine with its own identity and the old
backups marked as another instance's. Those are hidden from the listing by
default; add `?include_foreign=true` to see them, and the listing tells you when
it has withheld any. Restoring one is allowed, and logs whose backup it was.

Per-database routing, where different databases go to different targets, is
described above; this section is the single-destination half it was built on.

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

### Crash smokes verify acknowledged records after WAL replay

The enterprise-shared crash scenarios restart the killed writer and wait for
`/ready`, then check the host IDs from every successful write batch. WAL replay
duplicates and records persisted by failed requests are allowed, but cannot mask
missing acknowledged records. Crash runs reject an idle kill target or a crash
that never triggered. The base scenario keeps its exact row-count assertion and
now exits successfully when it passes.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1004](https://github.com/Basekick-Labs/arc/pull/1004).

### Compaction subprocess threads respect license and effective-core limits ([#1036](https://github.com/Basekick-Labs/arc/issues/1036))

Each compaction subprocess is now capped at the lower of the license's
`MaxCores` and the effective cores available to Arc, after automatic thread
defaults have been resolved. Lower configured values are preserved. This is a
per-process cap, not an aggregate reservation: the main process and multiple
subprocesses can still request more threads in total than `MaxCores`. Capping a
previously higher setting can reduce compaction throughput.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1043](https://github.com/Basekick-Labs/arc/pull/1043).

### Database API storage calls now have a deadline ([#1065](https://github.com/Basekick-Labs/arc/issues/1065))

Database API handlers now bound storage calls with a request context. Database
deletion gets a deadline scaled to the number of listed files, so a large
database is not cut off by the same fixed limit as a small one; partial-delete
errors continue to be collected and reported, including failure to delete the
database marker. Database details return an error if measurement listing fails,
rather than reporting a successful response with a zero measurement count.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1067](https://github.com/Basekick-Labs/arc/pull/1067).

### Forwarded writes and WAL replication honor cancellation during TLS setup ([#1064](https://github.com/Basekick-Labs/arc/issues/1064))

Leader forwarding and WAL replication now pass their existing contexts into
peer dials, so cancellation interrupts a stalled TLS handshake instead of
waiting for the dial timeout. Regression tests cover leader-dial cancellation,
the receiver's cancellation error, and receiver shutdown during TLS setup.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1068](https://github.com/Basekick-Labs/arc/pull/1068) and
[@jallegri](https://github.com/jallegri) in
[#1117](https://github.com/Basekick-Labs/arc/pull/1117), combined here with
coverage from both contributions.

### Float-to-integer conversion rejects the rounded upper bound and NaN ([#936](https://github.com/Basekick-Labs/arc/pull/936))

Converting `math.MaxInt64` to a float rounds it to `2^63`, so the previous
bounds check accepted that out-of-range value; NaN also passed the comparisons.
Both now fail conversion to `int64`. The MessagePack typed decoder shares the
same guard, so single-map columnar payloads follow the same rejection rules as
the generic conversion path used for batch and array payloads and when decimal
columns are configured. Invalid values in integer-inferred columns return an
error instead of being silently converted to an architecture-dependent integer.

Finite values in the valid range still truncate toward zero. Infinities and
values beyond the boundaries remain rejected, as before.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#936](https://github.com/Basekick-Labs/arc/pull/936).

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

The second half of #835 is fixed below in
[#1106](https://github.com/Basekick-Labs/arc/issues/1106). Two claims made here when #835 shipped
turned out to be wrong once measured: it is not only a planning cost (the larger cost is on Arc's
own commit path), and the fix does not belong upstream (an upstream fix removes half the growth).

The same fix was implemented independently, and three weeks earlier, by
[@efegokdemir](https://github.com/efegokdemir) in
[#923](https://github.com/Basekick-Labs/arc/pull/923), which was open against this issue and had not
been reviewed when the work above started. The two designs converged on the same approach. One check
#923 carries that the shipped sweep does not — reloading the table and re-running reachability
immediately before deleting, so a scan that has gone stale cannot delete — is tracked in
[#1125](https://github.com/Basekick-Labs/arc/issues/1125).

### Iceberg export keeps a table's manifest set bounded ([#1106](https://github.com/Basekick-Labs/arc/issues/1106))

Every reconcile pass that removed a file added **two** manifests to the table and nothing ever shed
one: the manifest holding the removed entry, rewritten with that entry marked DELETED, plus this
pass's additions in a new manifest. iceberg-go carries every untouched manifest into the next
snapshot verbatim, so the set only grew — on the order of 48-96 manifests a day on a measurement
with hourly compaction and daily retention, forever.

Measured on a 300-file table, one file swapped per pass, so only the inherited manifests grow:

| removal passes | manifests | reconcile pass | scan plan |
|---|---|---|---|
| 1 | 3 | 20 ms | 2.3 ms |
| 16 | 33 | 71 ms | 7.0 ms |
| 64 | 129 | 230 ms | 19.2 ms |
| 80 | 161 | 276 ms | 21.8 ms |

Both costs grow at ~1.6 ms per manifest per reconcile pass and ~0.13 ms per manifest per scan plan,
and both slopes are independent of the table size — it is per-manifest open overhead, not per-entry
work. The larger of the two is Arc's own write path: a pass has to read every manifest to find the
ones holding the files it is removing, so pass cost grows quadratically with the number of passes.
The scan-plan cost is paid by every reader of the table — DuckDB, Spark, Trino — on every query.

A pass that finds the table at 12 data manifests or more now also merges them into one, in the same
commit, and both figures return to their one-manifest baseline. The merge adds 8 ms to a pass on a
300-file table and 61 ms on a 10 000-file one — it rewrites the live set, so its cost grows with the
table while the saving does not, and that is where the threshold of 12 comes from. Once a pile has
actually built up the merge is not merely affordable: above ~2000 live files the merging pass is
*cheaper* than the ordinary pass it replaces (244 ms against 352 ms at 10 000 files), because it
trades 33 manifest writes for one.

It applies to any pass — additions, removals, or both — so a measurement that only loses files to
retention is covered too. On iceberg-go v0.7.0, which this release also moves to, a table reaches
the threshold more slowly than it did: see the upgrade note below for which passes still grow the
manifest set. The one pass it deliberately skips is the one that leaves a
measurement empty — whether because every data file is gone, or because the only candidates left
are files Arc cannot map to a partition and has to skip. With no live files there is nothing for a
merged manifest to hold, and Iceberg rejects an empty one.

The threshold is not configurable: a wrong value means a slower or a more frequent merge, never
data loss.

Two notes for operators of large tables. A merged table's whole live file list sits in one manifest
file, so a backup of the Iceberg warehouse sees fewer, larger `.avro` files. And a merge pass writes
the manifest set twice within its commit; the superseded copy is reclaimed by the orphan sweep from
#835 above, once the metadata versions referencing it retire. If you have turned that sweep off
with `iceberg.orphan_sweep_enabled = false`, this residue is permanent like the rest of it — a
merge pass is a new, modest contributor to the growth that switch accepts. One consequence worth
knowing: a merged table's live file list is concentrated in a single manifest, so where losing one
manifest used to cost a fraction of the file list it now costs all of it, and nothing re-registers
a lost manifest.

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

### Empty Arrow IPC queries complete successfully ([#731](https://github.com/Basekick-Labs/arc/issues/731))

When a measurement had no Parquet files, `POST /api/v1/query/arrow` returned
HTTP 500 and recorded the query as failed, unlike the JSON and MessagePack
paths. It now returns a valid empty Arrow IPC stream and records a completed
query with zero rows. Empty results release their query timeout context without
a cleanup panic. Missing field-schema anchors remain errors.

Contributed by [@jallegri](https://github.com/jallegri) in [#1120](https://github.com/Basekick-Labs/arc/pull/1120).

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

### The tiering files endpoint rejects an invalid `limit` ([#1135](https://github.com/Basekick-Labs/arc/issues/1135))

`GET /api/v1/tiering/files?limit=-1` returned a 500 and logged a stack trace. The handler read the
limit without validating it and then sliced the result with it, and the guard it used
(`len(files) > limit`) can never be false for a negative value — so `files[:-1]` panicked, even when
no files were tiered. It needed an authenticated admin on a licensed deployment, and Arc's panic
recovery turned it into a 500 rather than a crash.

Invalid limits now return 400, and the slice is clamped independently so the panic cannot return if
the validation is ever moved.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#1051](https://github.com/Basekick-Labs/arc/pull/1051).

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

### Cold files are a third object set, not merged into the hot one ([#1086](https://github.com/Basekick-Labs/arc/issues/1086))

The obvious implementation — add the cold listing to the file list a backup
copies — is wrong, and wrong in a way no test without a real cold tier can see.
**Five** mechanisms in the cluster path would each destroy or misreport a
merged cold file:

- the Raft manifest adapter drops every cold entry, so a scope whose data is
  all cold contributes zero manifest entries while the listing is non-empty,
  and `crossCheckManifest` **refuses the run outright**;
- that same cross-check holds back any data file the manifest does not list,
  which every cold file is by construction (tiering deletes a migrated file's
  entry as phase 2 of the migration);
- the end-of-run recheck **deletes the backup object** for any sidecar row the
  fresh manifest snapshot lacks — so a cold file would be copied and then
  removed again, on every cluster backup, with the status reporting success;
- the recheck's last pass would label a skipped cold file "reconciled: not
  missing data", because a cold file is never in the manifest;
- a path the cross-check held back as unregistered is reported as "not backed
  up" even when the cold tier supplied it — and since that count gates
  replace-mode restore past a 10% skip ratio, a deployment with a backlog of
  failed hot deletes could have had a complete backup refuse its own restore.

So `backupLeg` has a third set, `cold`, which bypasses all five, and
`ManifestFile.Tier` on the sidecar carries the per-file answer the restore
needs. There is a regression test per mechanism.

**The cold listing is authoritative and the tier rows are the cross-check,
which is the inverse of the hot path.** `crossCheckManifest` trusts the Raft
manifest over this node's listing, because on a cluster the listing can be a
stale view of a store several nodes write. The cold listing is not that: it is
this node reading the cold store directly. Hence an object with no row is
carried and counted rather than held back.

**Mid-migration, the cold copy wins.** A migration copies to cold before
releasing the hot copy, so one path can be in both listings. Taking the hot
side loses the file: if the migration completes during the run, the manifest
entry goes away and the recheck deletes the hot copy's backup object, leaving
the file in neither set. The cold object is canonical and nothing in Arc ever
deletes one, and the tier row has already been flipped to cold by that point,
so cold is both the safe and the consistent choice.

**The restore's cold path is separate on purpose.** A cold file is registered
in no Raft manifest — registering one would create an entry the next tiering
cycle wants gone, and would make peers try to replicate a file that is not in
hot storage — and it is not reported through `RecordRestoredFile` either, whose
handler stats the hot backend and whose upsert is guarded to hot rows, so a
cold report is dropped silently twice over. It has its own recorder, which
stamps `migrated_at` as **now**: only a row inside the orphan-reconciliation
window lets a stale hot copy at the same key be cleaned up, and the cost is one
hot-side existence check per restored file per cycle until the rows age out.

`RecordColdFile` also gained a `quarantined_at IS NULL` guard and now reports
whether it wrote. That is **defensive** rather than a fix for an active bug:
the old query had no `WHERE` at all, so it would set `tier = 'cold'` on a
quarantined row, but it never cleared `quarantined_at`, and the condition needs
a key some backend still lists while tiering has given up on it. The guard
earns its place because stage C adds a second caller — a restore recording a
row for a file it has just written — where acting on a quarantined path would
be a new way to lose the record of an unusable key.

Two shapes worth knowing for whoever tunes this next. The cold walk loads every
non-quarantined cold row once per backup, through a narrow two-column query
rather than the full-row accessor, because that load sits on the one shared
SQLite connection and so blocks auth, audit and tier registration while it
runs. And the restore writes one row per cold file **synchronously** — the
asynchronous tier-event path cannot be used, since its handler stats the hot
backend and its upsert is guarded to hot rows — so a large cold restore is one
fsync per file. Batching those writes is the obvious next improvement.

### The cold-file marker's shape ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

`backup.ColdCounter` is a one-method interface (`CountColdFilesByDatabase`)
wired from the tiering manager in `cmd/arc/main.go` beside `SetTierLookup`, nil
when tiering is off — the same pattern #1084 used for the fully-cold database
check, and for the same reason: the backup package should not import tiering.

Because that wiring sits inside the tiering block, which is gated on the
**licence** and not only on `tiered_storage.enabled`, an unlicensed node wires
no counter at all. That is left as it is: the alternative is building a tiering
`MetadataStore`, schema included, on an unlicensed node, which is the boundary
the licence pattern exists to hold. The field's doc comment and the operator
note above both say so, since an absent field would otherwise read as "nothing
was migrated".

Three decisions worth knowing before changing this code.

**One grouped query, not one per database.** `SELECT database, COUNT(*) ...
WHERE tier = ? AND quarantined_at IS NULL GROUP BY database`. The set a caller
wants is "every database with cold rows", and that is *not* the backup
inventory — a fully cold database has no hot files, so it is absent from both
the listing and `manifest.Databases` while still holding the rows this counts.
Grouping means the caller never has to discover the set first, and it cannot
half-fail the way N point queries can, which is what lets the count be
all-or-nothing instead of needing an "unavailable" flag. The plan is a seek on
`idx_tier_files_tier` plus a temporary b-tree over only that tier's rows,
measured and unchanged by `ANALYZE`; `idx_tier_files_database_tier` does not
serve it, because `tier` is that index's second column. It is not the fastest
plan available and the comment in the code says so: a covering index on
`(tier, database, quarantined_at)` is about 6x faster, and is deliberately not
added because it costs ~7% of the database file and a sixth b-tree on a table
the ingest flush path writes to, to save ~18 ms once per backup run. The note
records the scale at which that trade flips.

**Per leg, by the same rule the data follows.** Since a backup can fan out to
one destination per database, the count is attributed with `run.legFor`, so a
database routed to the audit target has its gap on that target's manifest and
nowhere else, and `mergeRunManifests` sums the legs into the run-level view
every consumer reads. A new per-leg counter that is added to the struct but not
to that merge reads zero on every multi-leg backup and correct on every
single-leg one, because the one-leg path is a clone — the kind of bug that
looks right in most tests.

**Informational by construction.** A counting failure warns and the backup
completes: the count describes data the backup was never going to carry, so
failing the run would trade a healthy backup for no backup over a diagnostic.
There is a comment at the replace-mode incompleteness refusal explaining why
this count deliberately does not join it; a regression test fails if it ever
does.

### Stale compaction manifests follow normal recovery ([#750](https://github.com/Basekick-Labs/arc/issues/750))

Corrected the `ManifestMaxAge` and recovery comments and an earlier release note
that said manifests over seven days old are deleted. Age triggers an investigation
warning; recovery still validates the output and applies its usual cleanup and retry rules. A
regression test verifies input cleanup for an eight-day-old manifest with a
valid output. Runtime behavior is unchanged.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#921](https://github.com/Basekick-Labs/arc/pull/921).

### Compaction database-name sanitization is documented as non-unique ([#750](https://github.com/Basekick-Labs/arc/issues/750))

The `sanitizeDBForName` comment now explains that replacing slashes with dots
produces a path-safe token, not a unique database identity: spoke IDs may
contain dots, so distinct pseudo-database names can collide. A regression test
documents the collision; runtime behavior is unchanged.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#920](https://github.com/Basekick-Labs/arc/pull/920).

### Iceberg export moves to iceberg-go v0.7.0

The library Arc's Iceberg exporter is built on goes from v0.6.0 to v0.7.0. Arc's own code needed no
changes for it; what changes is behaviour underneath.

**The manifest pile gets smaller on its own.** v0.6.0 carried a manifest whose entries were all
tombstones into every later snapshot forever, so a removal pass added two manifests. v0.7.0 drops
it, and parallelises the manifest scan. Measured on the same 300-file table, one file replaced per
pass:

| removal passes | manifests (v0.6.0 → v0.7.0) | scan plan (v0.6.0 → v0.7.0) |
|---|---|---|
| 16 | 33 → 18 | 6.87 → 2.99 ms |
| 64 | 129 → 66 | 18.62 → 7.12 ms |

Per accumulated manifest, a reconcile pass costs ~0.24 ms instead of ~1.6 ms.

What remains for the manifest collapse from #1106 above to handle is narrower than that table
suggests, and worth knowing if you are watching a warehouse:

- A pass that **only adds** files adds one manifest, and those accumulate until the collapse merges
  them. Verified on a running node: the count climbed 2, 3, 4 … 12 over twelve append passes, then
  the next pass merged them back to one.
- A pass that **only removes** files usually adds nothing at all, because v0.7.0 drops a manifest
  its removal leaves empty rather than carrying it forward. Retention on a quiet measurement no
  longer grows the set.
- A pass containing an **in-place rewrite** — what a `DELETE` that matches only some rows of a file
  produces — already merges the whole manifest set as a side effect of re-registering the rewritten
  path, and has since #633. Those passes need no collapse and do not get one.

So the collapse is the backstop for append-only growth, and a table that is both written and deleted
from reaches the threshold slowly or never. The `.avro` count on disk is unchanged by this upgrade,
so the orphan sweep from #835 is still the only thing that reclaims metadata files.

**Snapshot-expiry defaults changed upstream, and Arc is unaffected.** The retention keys moved to a
`history.expire.*` prefix (with fallbacks) and the default maximum snapshot age went from
effectively forever to 5 days. Arc passes an explicit age cutoff on every expire, so
`iceberg.retain_snapshots` remains the only thing that decides how much history is kept.

**A dotted database name is now refused rather than exported.** Arc builds one Iceberg namespace per
database, `<prefix>_<database>`, and v0.7.0 addresses a namespace whose component contains a dot by
a different catalog key than the directory Arc writes on disk. A table published that way would be
unreadable by DuckDB or Spark, would shadow any table already exported for that database, and would
be walked back in by the exporter as if it were a user database. Arc therefore:

- refuses `iceberg.namespace_prefix` containing a dot at startup, since it would affect every
  database on the node;
- refuses an individual database whose name would produce a dotted namespace, logging it and
  skipping that measurement while the rest of the node keeps exporting.

Arc database names cannot contain a dot, so this is reachable only through an edge-sync spoke ID,
which may contain one. If you export Iceberg from a hub with such a spoke, that spoke's tables stop
being published and are reported in the log. Tracked in
[#1129](https://github.com/Basekick-Labs/arc/issues/1129), which covers both emitting an
addressable namespace and migrating tables already published under a dotted one.

**New transitive dependencies.** v0.7.0 pulls in OpenTelemetry's API, RoaringBitmap and geospatial
encoders for features Arc does not use (deletion vectors, geometry columns, remote scan planning).
No telemetry is registered or emitted: the OpenTelemetry **SDK** is not in Arc's dependency graph,
the only instrumented path requires a scan-planning mode Arc never selects, and the metrics reporter
defaults to a no-op. There is no new network traffic and no new log output.

### Iceberg manifest merging no longer leaves its tuning on the table (#1106)

`manifestMergeOn` sets `commit.manifest.min-count-to-merge=2` and
`commit.manifest.target-size-bytes=1 GiB` for the commit in flight. iceberg-go v0.6.0 has no way to
remove a property from a transaction, so those keys stayed on the table after the commit; until now
that only happened on tables that had hit #633, and from this release nearly every exported table
commits through that path. They are inert for Arc, but they are visible table properties that
another engine writing the table would honour as if Arc had chosen them for it. `manifestMergeOff`
now writes iceberg-go's own defaults back (100 and 8 MiB).

The per-file fallback for day-straddling files (`replaceDataFilesOneByOne`) stages one snapshot and
one manifest per added file, and the path is sticky — a straddling file is never registered, so it
is back in the diff on every later pass. It takes the manifest merge too; it was the package's
heaviest manifest producer and would otherwise have been the one path exempt.

`internal/iceberg/deleted_manifest_cost_test.go` is the measurement harness behind the figures
quoted for #1106, from 30 to 10 000 live files. It is opt-in — set
`ARC_ICEBERG_MANIFEST_BENCH=1` to run it — because CI runs the whole suite under `-race` without
`-short`, and these tests take minutes each.

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

This was the first of several changes behind per-database backup targets
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

### What a remote backup destination changed underneath ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Three things are worth knowing if you work on backups next.

**The partial-write cleanup is safe for a reason that is easy to misstate.**
When a backup write fails, Arc deletes the destination key. That is safe because
every key it can be handed lives under a backup identifier minted by the run in
progress, and nothing else in Arc writes that shape. It is **not** the overlap
refusal that makes it safe, although an earlier draft of this work said so in
four places. The refusal exists for the recursion and the reconciliation sweep
described in the upgrade note.

**Listing backups no longer walks the whole destination.** It used to list every
object and match on a filename suffix, which on a remote target meant
enumerating the bucket and would match anything else that happened to share the
prefix, including the cold tier. It now enumerates directories and filters on
the backup identifier shape.

A trap for the next person: `List` does not mean the same thing on every
backend. The local one treats its argument as a directory to walk; the
object-store ones treat it as a key prefix to match. A listing under a `backup-`
prefix would have returned everything on S3 and nothing locally. `ListDirectories`
is the one listing whose meaning is identical across all three.

**Two spellings of the same object store used to compare as different stores.**
The default AWS endpoint and an explicit regional one for the same bucket are
the same place, and the overlap check did not know it. Endpoints are now
canonicalised: AWS regional spellings fold together, a virtual-hosted bucket
label is stripped, and an Azure account is preferred over the endpoint it can be
derived from. Deliberately not folded: a different AWS partition, a hostname
that merely ends in something that looks like an AWS suffix, and any self-hosted
store, because treating two of those as one location would refuse a legitimate
configuration.

### Backups are written per leg rather than per destination ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Per-database routing turned one destination into a set, and the refactor that
made that possible is worth knowing before you go looking for the old shape.

**A run is now a set of legs.** `backupRun` holds one `backupLeg` per target it
routes to, and a leg owns its destination, its `Manifest`, its `dbMap`, its
`sidecarBuilder`, its `skipTally`, its key headroom and its own state and data
skip counters. The run owns `Progress`, the run-wide skip ratio, the cluster
cross-check and the index. Files are partitioned into legs **up front**, before
any copy, and every copy function takes a leg: routing inside
`streamBackupFile` would make the destination a function of the path at every
write and force a per-file lookup of the right manifest, sidecar and tally.

A leg carries a back-pointer to its run so each copy function takes one
argument. That is not tidiness: with the run and the leg as separate
parameters a caller can pass a leg belonging to another run, and the per-target
progress is then published for a leg nothing is writing to — a mismatch that
compiles, runs, and reports plausible numbers for the wrong destination.

The state-vs-data skip split used to be a global subtraction — snapshot
`progress.SkippedFiles` after the state copies, subtract. That assumed every
state copy precedes every data copy, which per-leg interleaving (A-state,
A-data, B-state, B-data) breaks, so each leg counts its own and
`progress.SkippedFiles` stays a run-wide total used only for the gauge and the
ratio. The out-of-root warehouse's skips now stay out of both by construction,
where the subtraction had to take them back out.

**`destination()` is gone.** The no-argument accessor stage B2b-1 introduced is
replaced by `defaultDestination()` and a `backupTarget` value threaded through
every call site, including the ones that were never `destination()` callers and
are equally per-target: `describeDestination` (now `backupTarget.describe`),
the key-headroom arithmetic (now on `backupTarget`, because two targets with
different prefixes have different usable source-key lengths), the sidecar
writes, and the restore's reads.

**`Manager.targets` holds only the NON-default targets, and is nil for both
single-destination shapes** — no target configured, and exactly one target,
where the default must be it and routing is a no-op. `defaultDestination()`
re-reads the flat `backupStorage`/`targetName`/`targetKeyPrefix`/`targetRemote`
fields on every call and caches nothing. That is a compatibility rule, not a
style: twelve tests swap `m.backupStorage` on a Manager built by `NewManager`
to stand in for a counting, failing, unreachable, write-refusing or stalling
destination, and a populated map or a cached destination would route past every
one of those fakes and leave them green against a broken implementation.

**`ListBackups` fans out and returns a struct.** `BackupListing` carries the
backups, the foreign-filter count, the unreachable targets and the incomplete
runs; `ListBackupsDetailed` is what the API answers from, and
`ListBackups`/`ListAllBackups`/`ListBackupsFilteringForeign` remain as
convenience forms over the same fan-out. The fan-out is concurrent for a reason
that is not arithmetic: `withDestinationTimeout` derives from the caller's
context, so N targets cannot each cost a fresh 60 s, but **one** dead target
consumes the whole 30 s handler budget and cancels the rest — so a serial
listing could never report the unreachable target it is supposed to mark.

**The end-of-run cluster re-check is its own ordered step.**
`recheckClusterManifest` both writes and deletes after every leg has copied,
and its delete is routed to the leg that holds the file. Unrouted it would go
to the default destination, where `LocalBackend.Delete` returns nil for a
missing key and S3's `DeleteObject` is idempotent: it would **report success
while the file stayed on the routed target**, which is the double-serve #1083
and #930 exist to prevent, arrived at through a delete that looks fine.

**`replaceDatabases` runs once, over the union of every leg.** Per target it
would be unsound: `owns()` keys on the cluster entry's `Database` label for an
unscoped backup while `writes` is built from the backup listing, so one leg's
`owns()` can be true for a spoke entry whose bytes are in another leg's listing
— and it would `BatchDelete` live files the other leg is about to restore. The
two sets are only mutually protective when the file set is the whole run. The
per-leg copy loop sits inside the single compaction pause the caller already
holds, so the run still takes one pause and not one per leg.

**Known single-destination assumption, deferred deliberately.** The
single-operation lock stays one lock for the whole manager. A run spans every
target it routes to, so per-target locking would not make "back up to A while
restoring from B" safe, and `DeleteBackup` must sweep every target under one
`TryLock`. `/api/v1/backup/status` gained the per-target fields so an operator
can at least see which destinations a busy run is writing to.

`Progress.Targets` is replaced wholesale on every publish and never mutated in
place. `setProgress` publishes a shallow copy, so every published snapshot
shares that slice header and an element written after publication races every
reader of `/status` — the same reason `SkippedSample` is handed over once and
never appended to.

**A run has an ANCHOR leg, and the three default-leg restore steps read from
it.** `runRead.anchor` is the leg whose manifest asserted `HasMetadata`,
`HasConfig` and `IcebergWarehouse` — by `IsDefaultTarget` where that is set,
and otherwise by being the only leg. SQLite, `arc.toml` and an outside-root
Iceberg warehouse are read from `read.anchor.target` and never from
`m.defaultDestination()`.

Those two are the same target right up until an operator adds a second target
and re-points `backup.default_target`, which is the documented migration path.
After that the gates still fire — they read the MERGED view, so `HasMetadata`
is true because some leg set it — while the read goes to a target holding
nothing. SQLite and config then fail outright, in replace mode *after*
`replaceDatabases` has already deleted and rewritten live files, and every
retry fails identically until the default is pointed back. The warehouse arm
does not fail at all: `restoreIcebergWarehouse` returns nil on a zero-object
listing, because that is a legitimate "this run had no warehouse", so the
restore reports **completed** having written no warehouse while the catalog it
just restored points at absolute paths under it — #637's failure mode, by a
different route. The anchor must NOT be keyed off `IsDefaultTarget` alone:
`planRun` writes that field only for multi-leg runs, so the single-target
manifest this exists to fix has it false.

**The run index is read from every configured target, written to one.** The
write only ever goes to the default, so a healthy destination needs one read;
the read is wide because the default can MOVE, and a reader of only the current
default silently stops enumerating exactly the leftovers the index exists to
find. Same shape for `DeleteBackup`'s owner echo, which now tries every target
so a backup held only on a routed one is not deleted without one. The remaining
limit is recorded in the code: a run that died before any manifest is invisible
when no reachable target holds its index, because the directory listing that
produces the ID and the index key are on the same store.

**The `databases` accessor is a type switch on `v.Get`, not `v.GetStringSlice`.**
That accessor casts a scalar through `cast.ToStringSlice`, which runs
`strings.Fields`, so it split on whitespace as well as on the comma: a database
really can be called `my db` (`storage.ValidateKeySegment` rejects only NUL, a
backslash, a separator, the empty string, `.` and `..`), and the scalar
spelling turned it into two routing keys for databases that do not exist while
the real one fell through to the default target. The array spelling of the same
name was already correct, which is the worst shape a bug of this kind can take,
since the two spellings are documented as equivalent.

**The `include_config` credential warning comes from the configured set, not
the run's legs.** What leaks is the file, and `arc.toml` holds every target's
credentials, so a scoped backup of an unrelated database — whose legs are just
the default, and which is the one request that can override `include_config`
without a refusal — must still name every configured remote target.

**`backupRun.fail` marks every leg that has not committed.** A leg that
committed keeps that status, because its manifest really did land; everything
still pending, copying or copied is marked failed, so `/status` does not show a
half-green run that produced nothing restorable there.

Every leg's `sidecarBuilder` ALIASES one `byPath` map — the first cluster
manifest snapshot, the same immutable reference set for every leg. Only
`recheckClusterManifest`'s `addLate` writes into it, after every leg has
copied, on one goroutine. If the leg copies are ever parallelised, that map has
to become per leg or the write has to be guarded.

**A cold restore writes its tier rows in batches, not one per file** (#1141).
Stage C of the backup work gave a restore its own tier-row recorder, because
neither of the hot paths can report a cold file: the cluster manifest drops
cold entries, and `RecordRestoredFile` enqueues an event whose handler stats
the hot backend and whose upsert is guarded to hot rows. That recorder wrote
one row per file, synchronously — one implicit transaction, and so one fsync,
each — on a handle limited to a single connection and shared with auth, audit,
MQTT and the ingest path's own tier registration. A restore of a few hundred
thousand cold files was therefore a few hundred thousand fsyncs with every
other SQLite user in the process queued behind them.

The rows are now accumulated and written a thousand at a time in one explicit
transaction each: `RecordColdFilesBatch` and `RecordRestoredHotFilesBatch` in
the tiering metadata store, `RecordRestoredColdFiles` and
`RecordRestoredHotFiles` on the manager, and a `coldRowBatch` in the restore
beside the manifest registration that already batches the same way.

No field changes and no final value changes: `cold_files_restored_to_cold`,
`cold_files_restored_to_hot`, `cold_restore_quarantine_skipped` and
`cold_rows_not_recorded` keep their per-file meanings, which is why the batch
reports back the paths it did not write rather than a count — a quarantined row
and a path tiering cannot parse are different facts about a key, and they land
in different fields.

**One thing an operator will notice**, which is the honest cost of batching:
three of those counters are no longer live during a run. They move at a flush,
so a `/status` poll through a 900-file cold restore shows `processed_files`
climbing while `cold_files_restored_to_cold` sits at 0 until the run ends, then
jumps to 900. A restore larger than a thousand files steps in thousands. The
final values are what they always were. `cold_files_restored_to_hot` is
unaffected — it counts the routing decision, taken per file before the bytes
are written, not the row.

Four details worth keeping straight, since this is the first explicit
transaction in `internal/tiering`:

- **One transaction per call, and the caller chunks.** That contract is what
  makes the accounting exact: an error means nothing in the call was written,
  so the caller counts the whole chunk as unrecorded without having to ask how
  far it got. Chunking inside the store would commit some chunks and roll back
  one, and no return value short of a per-path map could then describe it.
- **Every statement goes through the transaction, never the pool.** The handle
  allows exactly one connection, so a `db` call made while the transaction is
  open would wait forever for the connection the transaction itself holds.
  That is a self-deadlock, not a slow query. The rollback on the failure path
  is what releases that connection, and the test for it is the one that proves
  a batch after a failed batch can still get a connection at all.
- **Every flush runs on a context detached from the restore's**, not only the
  final one. A transaction cannot begin or continue on a dead context, and a
  restore runs under a fixed `operation_timeout` deadline — so that deadline
  can land inside a mid-run flush of a full chunk, which is the
  longest-running thing a restore does to SQLite. A flush takes its rows out
  of the batch before it writes them, so a chunk lost that way cannot be
  retried: a thousand files would be left with bytes in the cold store and no
  tier row. Each flush is bounded by its own timeout, so a cancelled restore
  still exits after at most one. Unlike the manifest registration beside it,
  the flush also runs on every exit path: the bytes are already in the cold
  store, and on a standalone node, or a cluster without shared storage or
  replication, nothing else ever writes the row, because the cold-metadata
  sync does not run there.
- **`migrated_at` is still written as text in `2006-01-02 15:04:05`.** The
  column is compared as a string against every other row, and go-sqlite3 binds
  a `time.Time` with its offset appended, so a bound time here would sort
  against every row Arc has ever written. The cold-tier metadata sync still
  uses the single-file `RecordColdFile`, which writes the same shape; a test
  asserts the two are byte-identical for the same moment.

The cold-tier metadata sync keeps its per-file writes: it records rows it
discovers one at a time as it walks a listing, and is not a burst.

**Correction to the paragraph that shipped with the batching change above.**
It said a cold-carried file restored into hot storage could have that hot copy
deleted by orphan reconciliation while its forced-hot row was still pending,
on a node whose cold tier is *configured but disabled*, and advised running
such a restore with tiering stopped. That was wrong, and the advice is
withdrawn. The fallback branch is reached only when this node has no usable
cold backend, and a disabled cold tier has no backend **object** either —
`cmd/arc/main.go` constructs one only inside `if cold.Enabled`, nothing
assigns the flag after configuration load, and there is no reload. The sweep
therefore has nothing to verify a cold copy against and keeps the hot file,
which is what it already did. No window existed to widen.

**Orphan reconciliation is gated on `tiered_storage.cold.enabled`, and the
cold accessors no longer disagree** (#1143). Two related pieces of tidying,
neither of which changes what a correctly configured node does:

- A node with tiering on and cold off ran the orphan sweep and the manifest
  sweep every migration cycle, and the orphan sweep logged an **error and
  counted a failure for every ORPHAN it examined, every cycle** — every cold
  row in its 48-hour window whose hot copy is still present — for work it
  could never do, since it needs a cold copy to verify and there is no cold
  backend to verify against. A cold row whose hot copy is gone costs one
  silent existence check and was never the problem. Both sweeps are now
  skipped on such a node, so the cycle is quiet and its error count is honest.
  For the manifest sweep the skip is a pure no-op: it already returned
  immediately on a nil cold backend.

  Two smaller operator-visible consequences of skipping, rather than letting
  the sweep run into its keep-everything branch. The orphan sweep also marks a
  permanently unusable storage key as quarantined, and it does that *before*
  it looks at the cold tier — so on a cold-disabled node that mark is now
  deferred until cold comes back, and since the rows age out of the 48-hour
  window meanwhile, in practice it is not taken. That is harmless while the
  sweep is not running, because the mark exists to stop the sweep retrying
  that key. And the per-orphan error was incidentally the only line saying
  such a node holds a hot copy under a cold row; a skipped cycle now says so
  once, at debug level, instead of once per orphan at error level.
- `GetBackendForTier(TierCold)` now ANDs `cold.enabled`, as `ColdBackend()`
  already did, and both answer through one predicate alongside the migration
  gate, the tier stats, the cold-metadata sync and the query glob. The old
  split was documented as deliberate, with a doc comment asserting that every
  other consumer paired the two checks itself — the orphan sweep did not,
  which is how this was found. Nothing reachable depended on the difference,
  because a disabled cold tier has no backend to return; enforcing it in one
  place means a future change that constructs the backend unconditionally
  cannot turn that latent inconsistency into a live one.

One claim worth not making: the "no usable cold tier" startup warning on a
replicating local-storage node is unchanged by this. It keys off
`GetBackendForTier(TierCold) != nil`, and with cold disabled that was already
nil before this change, so the warning was already firing. Nothing at that
call site behaves differently in any reachable configuration.
