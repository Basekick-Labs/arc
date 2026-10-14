# Arc v2027.01.1 Release Notes

> **Status:** Planned — January 2027 release.

## New: MessagePack columnar tag keys (#275)

MessagePack columnar payloads now accept an optional `tag_keys` field to
identify tag columns explicitly. Generic and typed decoding, WAL recovery
and replication preserve these names for downstream tag metadata and
compaction deduplication. Payloads without `tag_keys` remain supported.
A name that would stop compaction, such as one that is not a column of the
data, is left out with a warning, and the write succeeds
([#1258](https://github.com/Basekick-Labs/arc/issues/1258)).

Contributed by [@efegokdemir](https://github.com/efegokdemir)
in [#905](https://github.com/Basekick-Labs/arc/pull/905).

## Upgrade note: three configurations now refuse to start

Read this before upgrading if you set `storage.local_path`,
`backup.local_path` or `compaction.temp_directory`. The first two refusals apply
only when `backup.enabled` is true, which is the default, and both are decided
on **resolved absolute** paths: relative spellings, trailing slashes and
symlinks are normalised first, and in the container image the working directory
is `/app`. The third applies independently of `backup.enabled`.

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

**3. `storage.local_path` and `compaction.temp_directory` may no longer contain
a glob metacharacter** — one of `*`, `?`, `[`, `]`, `{`, `}`. All six are legal
in a POSIX directory name, so `/data/arc[prod]` was an accepted configuration
before this release.

It cannot stay accepted. Compaction interpolates a full filesystem path into
DuckDB's `read_parquet()`, which reads its argument as a **pattern**: with a
metacharacter in the path, Go's literal `os.Open` and DuckDB can resolve the
same string to **different files**, so a job could validate one file and read
another, then delete the inputs it never read. The storage root is checked for
the local backend only, since that is the only one whose paths reach a local
glob; `compaction.temp_directory` is checked whenever compaction is enabled,
because inputs from object storage are streamed into it and read back from
there.

To fix: rename the directory. The refusal names the offending character.

Arc also refuses such a path per-job as defence-in-depth, but that is not where
an operator should find out: every input would fail identically on every cycle,
so compaction would never progress while the logs showed jobs completing.

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

## Upgrade note: a node that cannot list its cold bucket stops migrating

If you run tiering with a cold tier, check that the cold credentials allow
**listing** the bucket or container, not only reading, writing and deleting
objects.

From this release every node with a usable cold tier reads the cold listing at
the start of each tier scan, and a scan whose cold listing fails skips migration
and orphan reconciliation for that cycle — see "Cold-tier metadata is rebuilt on
standalone nodes". On clustered nodes both halves already applied. On a
**standalone** node neither did, so a deployment whose cold policy grants
`GetObject`, `PutObject` and `DeleteObject` but not `ListBucket` has been
migrating happily and will stop after upgrading: hot storage keeps growing and
the cycle logs

> Cold tier could not be listed, so migration and orphan reconciliation are
> skipped this cycle and hot storage will keep growing until a listing succeeds.

once per cycle. `GET /api/v1/tiering/status` reports it as
`last_scan.cold_sync_failed`.

The same halt is reachable without a permissions problem: the cold listing is
spent from `tiered_storage.scan_timeout`, before the hot walk gets any of it. If
you sized that budget from how long a hot scan takes — which is what `arc.toml`
suggested — a large cold bucket can now exhaust it. Raise it above the time a
full cold listing *and* a full hot scan take together.

Nothing changes for a node with `tiered_storage.cold.enabled` off, or one whose
cold backend failed to construct: the listing is skipped and migration runs as
before.

## Upgrade note: an isolated primary writer stops accepting writes ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

This applies to Pattern 1 clusters (local storage) with more than one writer.

A writer cut off from the cluster used to keep acknowledging writes. Ingest was
never gated on being the primary; only `/ready/write` was. Meanwhile failover
could promote another writer. The isolated writer's rows then reached the rest
of the cluster only after the partition healed, or never if its disk failed
first. A writer also accepted writes whenever they reached it while it was a
standby, including after it had been replaced as primary.

A writer now refuses writes with `503` and `Retry-After: 5` when:
- **another writer is the designated primary.** That covers standby writers, and
  a former primary after failover or a manual hand-over. It needs no failover
  licence.
- **it lost contact with the cluster.** It is not the Raft leader and has not
  heard from the leader for longer than half of `cluster.health_check_interval`
  × `cluster.unhealthy_threshold` (7.5 s by default, at least 3 s). This rule
  needs automatic writer failover (`cluster.failover_enabled` and the
  `writer_failover` licence feature), at least three writers, and Raft. Failover
  takes longer to notice a lost primary: about 30 s at defaults.

  Once contact returns, writes resume only after contact has held for 2 s and
  the writer has caught up with the cluster log. That way a writer that was
  replaced while it was away refuses writes instead of acting on its stale view.

The refusal happens before anything is written, the WAL included, so a client
retry is safe. A line-protocol or import request that spans several
measurements is checked once up front. A fence closing in the middle of such a
request can still leave its earlier measurements written.

`/ready/write` reports not-ready while a writer refuses writes.
`arc_cluster_ingest_fenced` is 1 while a writer has lost contact; a standby
refusing writes is normal and does not set it.
`arc_cluster_ingest_fenced_writes_total` counts refused writes, by `reason` (see
the bug fix for [#1268](https://github.com/Basekick-Labs/arc/issues/1268)).

MQTT subscriptions on a writer that refuses writes are suspended: disconnected
without unsubscribing. With `clean_session=false` the broker queues QoS 1 and 2
messages until writes resume. With `clean_session=true`, or QoS 0, messages
published meanwhile are lost. Because standby writers always refuse writes, run
MQTT subscriptions on the primary. On a cluster node, MQTT subscriptions now
start after the cluster coordinator.

`cluster.ingest_fence_after_ms` overrides the contact delay. It is capped at
`health_check_interval` × `unhealthy_threshold` and raised to three election
timeouts. Size client retry buffers for the failover time. The contact rule
watches Raft. A network failure that blocks only the Raft port, and not the
cluster heartbeat port, fences the primary while failover sees it as healthy:
writes stop until Raft connectivity returns. Edge-sync bundles received by a hub
write directly to storage and are not fenced.

## Upgrade note: Raft pre-vote ([#1272](https://github.com/Basekick-Labs/arc/issues/1272))

Arc 27.01.1 moves to hashicorp/raft v1.7.3 and runs the Raft pre-vote round by
default. A writer that cannot hear the leader no longer raises its term on
every election timeout. Before, when the leader reached it again, the higher
term made a healthy leader step down: restarting one standby writer cost the
cluster a leader change.

**Rolling upgrade from an earlier release.** A node running an older release
does not answer pre-vote, and an upgraded node counts that as a refusal. While
the cluster runs both versions, if the leader dies and the two survivors are one
old and one upgraded node with the more recent log, neither becomes leader until
the restarted node rejoins on 27.01.1. Upgrade writers one at a time, and wait
for each to rejoin before the next. To rule the window out completely, set
`cluster.raft_pre_vote = false` (environment `ARC_CLUSTER_RAFT_PRE_VOTE=false`)
on every node for the upgrade, then remove it (one more rolling restart) once
every node runs 27.01.1.

During the mixed window, old nodes log `failed to decode incoming command ...
unknown rpc type` and upgraded nodes log `failed to make requestVote RPC` when
an election runs. Both stop once every node is upgraded.

A leadership transfer now returns only after the leader actually stepped down
(up to one election timeout), and reports a timeout if it did not.

## Upgrade note: compaction commit stays on once enabled ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

A cluster enables compaction commit by itself once every node runs this
release (`compaction_commit.enabled` in `GET /api/v1/cluster`). The flag stays
on, and a node running an earlier release can no longer join.

Do not roll a node back to an earlier release in place after that. The earlier
release skips the new cluster log commands and ignores their checks, so its
view of the cluster manifest silently diverges from the other nodes. To run an
earlier release on a node, remove the node from the cluster, wipe its data
directory, and join it fresh. To roll back the whole cluster, restore it from a
backup taken before the upgrade.

## Upgrade note: the live WAL replication stream is off ([#888](https://github.com/Basekick-Labs/arc/issues/888))

This applies to clusters where readers or compactors run with both
`wal.enabled` and `cluster.replication_enabled`. With default values the Helm
chart enables the WAL only on writer pods, so a chart-deployed cluster never ran
the stream on those roles and nothing changes for it. The manifests in
`deploy/kubernetes-local` did enable it on readers.

**What changed.** Writers no longer start the WAL replication sender, and
readers and compactors no longer receive from it. Every node with `wal.enabled`
and `cluster.replication_enabled` logs this once at startup:

> WAL replication stream not started: readers and compactors read registered
> Parquet from the primary, and the standby-writer stream is not available yet

**Why.** The stream reached only readers and compactors. Each applied what it
received to its own ingest buffer and flushed it to its own Parquet. Queries
read every file on the node, so a reader returned every replicated row twice:
once from its own flush and once from the copy it pulled from the primary. A
compactor also registered its copy in the cluster manifest, so every node pulled
it, the primary included, and the duplicates showed up cluster-wide. Neither
role can be promoted, so the stream gave failover nothing in exchange. It
returns for standby writers only, holding the primary's unflushed window for
failover without writing a second copy
([#1207](https://github.com/Basekick-Labs/arc/issues/1207)).

**What does not change.** `cluster.replication_enabled` still starts peer file
replication. Readers and compactors get every row by pulling the primary's
registered Parquet, as they already did for anything flushed. A reader sees a
new row once the writer flushes it (`ingest.max_buffer_age_ms`) and the file has
been pulled.

**Metrics.** `arc_replication_lag_entries` and `arc_replication_lag_seconds`
were reported per connected reader. With no reader connected they have no
series, and `arc_replication_entries_dropped_total` stays at zero, so alerts
built on them will not fire. They describe the stream only, not peer file
replication.

**Upgrade order.** Upgrade readers and compactors first, and stop them
gracefully. A graceful stop flushes and purges what they received. After a crash,
the first boot replays those received entries once more and writes one more
duplicate copy. A reader that is still on the old version keeps dialling the
writers, and every few seconds until that reader is upgraded an upgraded writer
refuses it and logs

> Replication sync rejected: not a writer or replication not enabled

**Duplicates already on disk.** Which copies exist depends on the role that
wrote them.

- **Compactors, and readers that were ever Raft leader.** These copies are
  registered in the manifest, so every node pulled them. Readers could vote, and
  so lead, before v26.09.2. A copy that has not been compacted yet is listed in
  `GET /api/v1/cluster/files` with the compactor or reader as `origin_node_id`
  and a filename that does not contain `_compacted`. After compaction, the copy
  and the primary's original are merged into one `_compacted` file.
  Line-protocol rows are deduplicated in that merge when the primary's original
  was compacted in the same job. Msgpack rows never are, so their duplicates are
  inside the compacted files and need to be rewritten.
- **Other readers.** Their copies exist only on their own disk, outside the
  manifest, and nothing deletes them: retention, deletes and cold migration do
  not reach them. Reconciliation finds them as orphan storage. On a local-storage
  cluster, enable `reconciliation.enabled` on the reader and run
  `POST /api/v1/reconciliation/trigger` on that reader. That run is a dry run;
  check `orphan_storage_count` (the sample lists at most
  `reconciliation.sample_paths_cap` paths). Then delete with
  `POST /api/v1/reconciliation/trigger?dry_run=false&act=true`. A run deletes at
  most `reconciliation.max_deletes_per_run` files, and files younger than the
  grace window (`grace_window_seconds` plus `clock_skew_allowance_seconds`,
  24 h 5 min by default) are skipped. On shared storage, reconciliation runs only
  on the active compactor, and a reader's copies are in the shared bucket.
- **Clusters upgraded from v26.09.2 or earlier.** Replicated line-protocol rows
  were filed under the `default` database until #889 was fixed in v26.09.3, so
  some of these copies are in `default`.
- **Writers.** Do not run orphan-storage cleanup on writers to fix this. A
  writer's unregistered files can be the only copy of rows it recovered from its
  WAL after a crash
  ([#1206](https://github.com/Basekick-Labs/arc/issues/1206)).

## New features

### Writer failover picks the standby with the lowest node id ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

When several healthy standby writers can replace a failed primary, the leader
now picks the one with the lowest node id. Before, the choice depended on map
order and could differ between leaders.

### Experimental: WAL coverage on cluster writers ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

This is the first step toward letting a standby writer take over the rows a
failed primary acknowledged but did not flush yet. It is off by default and
changes nothing unless you set `cluster.wal_coverage = true`.

On a Pattern 1 writer (local storage) with the WAL enabled, the setting:
- **gives every ingest batch a coverage id.** The id is unique per writer
  process and measurement, and assigned in commit order. It is written into the
  batch's WAL entry and into the registration journal with every file the batch
  is flushed into.
- **keeps WAL entries until their files are registered.** Before, a WAL entry
  could be purged once its file was flushed. Now it stays until the cluster
  manifest has the file. During a manifest outage (lost Raft quorum, for
  example) the WAL grows with the unregistered data, as the registration journal
  already does. The hold survives restarts: WAL recovery keeps a file whose
  batches still wait to register, along with every file after it, and the node
  deletes them once the registrations land. A clean shutdown keeps them too,
  instead of purging the whole WAL. A start whose cluster does not come up keeps
  them until a later start registers them, logged at Error.
  `arc_wal_held_files` counts the files kept for this reason, and a Warn every
  10 minutes reports how many while any are held. The hold covers files journaled with
  coverage even after the setting is turned off.
- **refuses a write before it reaches the WAL.** A write the buffer turns away
  (schema churn, a shard closing during shutdown, a cancelled request) no
  longer leaves an entry in the WAL that a later recovery would replay.

New metrics:
- `arc_wal_registered_floor_lag`: batches committed but not yet in a registered
  file. It returns to 0 when every flushed file is registered; a lost batch
  (below) stays in it until the process restarts.
- `arc_wal_coverage_lost_total`: batches whose file settled without being
  registered (the file was gone or rewritten before registration), or whose
  flush failed with no complete WAL copy. Those rows are in no registered file.
  Alert on any increase.

**Standby writers keep a copy.** With the setting on, the designated primary
also streams every committed batch to the standby writers. Each standby keeps
it on disk under `<wal.directory>/received/`, and never applies it. It discards
a batch once the batch is in a registered file and the standby holds every
current file of that measurement locally. This is the copy stream takeover
(below) publishes the acknowledged rows of a primary that died before
flushing them from. Details:
- Standby writers connect to the primary on the cluster port
  (`cluster.coordinator_addr`). This needs `cluster.shared_secret`, and
  `cluster.tls_enabled` if the network is untrusted: batches are authenticated
  but not encrypted without TLS.
- `cluster.standby_store_max_bytes` (default 1 GiB) caps the copy. Past it, a
  standby first evicts batches of a primary process that has since restarted.
  If that is not enough, it keeps nothing new until trimming frees room, and
  records the missed batches as gaps. The directory counts against the WAL
  volume but not `arc_wal_dir_bytes`.
- `cluster.origin_stream_queue_bytes` (default 64 MiB) caps what the primary
  holds in memory for a standby that is not keeping up. Past it, that standby
  misses batches; the others do not.
- A covered file now counts as registered only once the writer's own copy of the
  manifest shows it. On a writer that is not the Raft leader this adds one Raft
  barrier per registration batch.

Metrics on the primary: `arc_origin_stream_sessions`,
`arc_origin_stream_dropped_total`. On standby writers:
`arc_standby_stream_connected`, `arc_standby_stream_entries_total`,
`arc_standby_stream_gaps_total`, `arc_standby_store_bytes`,
`arc_standby_store_incomplete_keys`, `arc_standby_store_trimmed_bytes_total`,
`arc_standby_store_dropped_total`, `arc_standby_store_evicted_bytes_total`,
`arc_standby_store_lost_records`, `arc_standby_store_blocked_keys` and
`arc_standby_store_replaced_bytes`. A measurement stays in
`arc_standby_store_blocked_keys` while a file of it is not on the standby: a
file the puller cannot fetch keeps that measurement from being trimmed.

All writers in the cluster must run this release before the setting is turned
on: a writer from an older release does not understand the standby connection
and logs it as an unknown message type.

**Stream takeover building blocks.** Nothing here changes behaviour unless
every node sets `cluster.stream_takeover` (see "Stream takeover" below):
- A new setting, `cluster.stream_takeover` (default off), opts a node into
  stream takeover. Takeover lets a standby writer publish the rows a failed
  primary acknowledged but never registered, exactly once. It needs every node
  of the cluster to opt in, writers with `cluster.wal_coverage` on.
- Only nodes with the setting on advertise takeover support when they join.
  The advertisement is covered by its own MAC next to the existing join MAC, so
  a rolling upgrade keeps working against a leader from an older release.
- A covered file's registration carries the exact batches it holds. When a
  flush spans several partition hours, each hour's file lists only the batches
  with rows in it. This is the one cost of `cluster.wal_coverage` without
  takeover: up to 64 KiB per registration in the Raft log, usually a few
  bytes, which a cluster without takeover discards.
- The cluster manifest has the rules that will refuse a second copy of a
  batch, and the origin's copy of batches a standby took over.

- A refused registration is parked rather than left in place: the file
  moves out of every query path to `<storage>/_refused/`, its WAL entries stay
  held, and it is deleted only once each of its rows is in a registered file
  this node holds. A DELETE that touches a parked file is refused (retry later).
  Records live in `refused-registrations.jsonl` next to the registration
  journal. Metrics: `arc_wal_parked_files`, `arc_wal_parked_bytes`.
- Writers with the setting on announce each process to the cluster before
  accepting writes, once takeover is on: until the announcement applies, writes
  get a retryable 503. Each writer records its last process in
  `instance.last` files in the WAL directory, the Raft data directory and
  `<storage>/_instances/`.
- `cluster.stream_takeover` now needs an explicit `cluster.node_id`. Takeover
  claims name nodes, and the default node id (host name and process id)
  changes at every restart outside Kubernetes. A node with the setting on and
  no `cluster.node_id` logs an error and does not advertise takeover support,
  so the cluster cannot turn takeover on while that node is a member.
- Once takeover is on, the standby store no longer evicts, at its cap, the
  batches of a writer process that a takeover could still need. When the store
  is full and holds such batches of a process its writer has since restarted,
  new batches are not kept and an error is logged; the new gauge
  `arc_standby_store_pinned_bytes` shows how much the store keeps that it would
  otherwise have evicted.
- A standby publishes the batches it takes over straight to Parquet, without
  touching the WAL or the live buffers; a claim grows in steps; a claimant
  releases its own claim when its copy is gone; and a claimant file the
  settled index has no room for is refused (the index keeps tracking the
  writer process instead of giving up on it).
- A writer that leaves the cluster gracefully (a restart, a rolling upgrade)
  keeps its takeover state while it is gone: when it comes back it picks up its
  own unflushed batches first, instead of having them taken over. Only an
  operator node removal gives them up, or the departure expiring after
  `cluster.stream_takeover_departed_ttl` (default `24h`). Departed writers are
  listed by the new admin endpoint `GET /api/v1/cluster/stream-claims` and
  counted by `arc_stream_takeover_departed_writers`.
- New setting `cluster.stream_takeover_grace` (default `60s`, at least `10s`):
  how long a primary that stopped answering may take to come back before the
  new primary takes over its batches.
- A file that vanished before any node pulled it now gives its rows back for a
  takeover, in its partition hour only; the rest of the writer process stays
  settled.
- Failover never promotes a writer whose current process another writer has
  claimed.

**Stream takeover (live, opt-in).** With `cluster.stream_takeover = true` on
every node (writers also need `cluster.wal_coverage = true` and an explicit
`cluster.node_id`), the cluster turns takeover on by itself once every node
advertises support (`stream_claims_v7`; builds that advertised `v1` to `v6`
block it). From then on:
- **A dead primary's acknowledged rows are published by the new primary.**
  When the primary stops answering and failover designates another writer,
  that writer waits `cluster.stream_takeover_grace`, then claims every batch
  of the old primary it holds in its standby copy that is not in a registered
  file, replays them to Parquet, and registers them. The cluster manifest
  refuses any row registered twice, so a batch the old primary registered
  before it died, in whole or in part, is published once.
- **A writer whose process was claimed exits with code 3.** If the old
  primary was only cut off and comes back, it refuses writes (503) the moment
  it sees the claim, flushes, and exits with status 3 so the restart policy
  starts it again; its next process is not claimed. Run writers under a
  restart policy that restarts on a non-zero exit (systemd
  `Restart=on-failure`, Kubernetes, Docker `restart: on-failure` or
  `unless-stopped`). Its copies of the claimed batches are parked, not
  registered. A writer that is the Raft leader steps down as it exits.
- **A primary that restarts within the grace keeps its own batches.** A
  restart, crash or rolling upgrade that brings the writer back with its
  volumes before the grace ends takes nothing over: the writer announces the
  earlier process and publishes it itself. A graceful shutdown first reports
  everything it registered, so a takeover after it claims only what is really
  unregistered.
- **Standby writers refuse writes** (503, retryable) while another writer is
  the designated primary, checked on every write.
- **Copies a standby holds that the new primary lacked are published too.**
  Once a claim finishes, any other writer that holds batches of the old
  process the claim did not publish claims those (each writer once per
  process), as long as the old primary has not come back with a new process
  and is not the designated primary. A process its writer gave up (it
  restarted without it) is published by any writer holding it, at once.
- **A DELETE stays deleted through a takeover.** With takeover on, a row
  DELETE (`POST /api/v1/delete`) or a database deletion
  (`DELETE /api/v1/databases/{name}`) is recorded in the cluster before it
  runs. Rows acknowledged before it that are not in a registered file yet (a
  standby's copy a takeover replays, an old primary's WAL, a writer's pending
  files) are rewritten without the rows it removes before they are
  registered, with the DELETE's own SQL, and the cluster manifest refuses any
  such file that was not. Rows acknowledged after the DELETE's own flush
  are outside the record (a write concurrent with the DELETE may or may not
  be deleted). So:
  - the DELETE no longer waits for a takeover (the 503 "a stream takeover is
    publishing rows" is gone);
  - `now()`, `current_timestamp`, `current_date` and the other per-statement
    time functions in the WHERE clause are fixed to the time the DELETE ran,
    in UTC, so a later replay matches the same rows; `random()`, `uuid()`,
    `nextval()` and similar are refused (400);
  - the primary first waits (up to 30 seconds) for every file the manifest
    lists of the measurement to be on its disk, since the DELETE rewrites
    the files it has. A file that does not arrive answers 503 with
    `unreachable_files`; set `"allow_unreachable": true` to run the DELETE
    anyway: the response lists the files it skipped, and their rows are not
    deleted;
  - each writer acknowledges every DELETE with the batches it had accepted at
    that point, and refuses writes (503, retryable) for the moment it takes
    (about 100 ms);
  - for a few seconds, a node that is not the primary can still show deleted
    rows in a file it has just written and not registered yet. The primary
    never does once the DELETE has answered;
  - the cluster keeps at most 256 DELETEs until every writer process they
    name has settled the batches that came before. Past that, the oldest one
    that only retired writer processes still hold is dropped, and those
    processes are removed (their copies are parked if they come back);
    otherwise a DELETE answers 503 until older ones settle. A writer that is
    down, and neither back nor given up on (removed, or its departure
    expired), holds every DELETE made while it is down.
    `GET /api/v1/cluster/stream-claims` lists the DELETEs kept
    (`delete_records`) and the writer processes that have not reported their
    batches for each (`unacknowledged`).
- **Operator controls** (admin):
  `GET /api/v1/cluster/stream-claims` lists processes and claims;
  `POST /api/v1/cluster/stream-claims/{instance}/release` with
  `{"epoch": n}` releases an unfinished claim (any writer may claim it again,
  the same one included), and with `{"epoch": n, "abandon": true}` finishes
  it, irreversibly: batches it has not published are left to their origin,
  which loses them when the origin is gone, and the claimant drops its copy
  (every batch the claim held is reported unreplayable). Leader only (409 elsewhere). The leader also
  releases an unfinished claim by itself when its claimant has been gone for
  the grace.
- New gauge `arc_stream_takeover_oldest_claim_age_seconds`: how long the
  oldest unfinished claim has been open. A claim whose replay keeps failing
  (a full disk, for example) is never given up on by itself: alert on this
  gauge.
- **An old primary that comes back with its volume publishes the rest.**
  Rows it acknowledged that no standby held (the replication lag), and rows
  the new primary could not publish, are in its WAL: it reads them back and
  publishes them, once, and its copies of rows a takeover already published
  are refused and removed. Flushes never put batches another writer claimed in
  the same file as batches nobody claimed, nor batches of two processes of the
  writer, so a refusal never holds back rows nobody else has.
- **A writer the cluster gave up on is retired, not forgotten.** When every
  writer has finished with a process of a writer that was removed, or whose
  departure expired, the process is kept as `retired` (the last 256), with
  what was registered of it: if that writer comes back with the volume after
  all, it publishes exactly the rows nobody registered. Copies of an older
  retired process are refused and parked.
- **Files nobody pulled are not left behind.** A file the old primary
  registered counts as published, but if no other node pulled it before the
  primary's volume was lost, its rows are gone. So, for every file of the old
  process whose rows its standby copy holds, a claim finishes only once the
  new primary holds the file too. If every node, asked directly, says it does
  not have it (or no node could answer for it during twice the grace), the
  file is removed from the manifest and its rows are published from the
  standby copy; a node that later comes back with a copy of it removes that
  copy. A file the standby copy could not publish again is never removed and
  does not hold the claim. New gauge `arc_stream_takeover_unpulled_files`:
  files the new primary still waits for.

What takeover does not cover: rows a primary acknowledged but had not
streamed to any standby yet when its volume was lost (the replication lag,
usually milliseconds) and that it never comes back with.

**Rollback warning.** WAL entries written with the setting on use a new format.
This release reads it, but older releases cannot: they quarantine those entries
and lose any rows that were not flushed yet. Before rolling a node back to an
older release, either turn the setting off and restart once, or stop the node
cleanly so it flushes and purges its WAL. A registration journal written with the
setting on loads in older releases; they ignore the coverage.

Writes with the setting on cost about 25% more per request when every request
is a single row to one measurement. With batches of a few hundred rows the
difference is within measurement noise.

### Compaction cycles can be looked up by id ([#1162](https://github.com/Basekick-Labs/arc/issues/1162))

`POST /api/v1/compaction/trigger` answers with a `cycle_id`. Until now that id
had nowhere to be resolved: `GET /api/v1/compaction/stats` carries a single
global `last_cycle`, so by the time you polled it could already describe a
later cycle, and nothing in the payload said which cycle you were looking at
relative to the one you asked about. Two new admin-only routes close that:

- **`GET /api/v1/compaction/cycles/{id}`** — one cycle.
- **`GET /api/v1/compaction/cycles?limit=N`** — the retained window, newest
  first. `limit` defaults to 10.

A record carries more than counters, because counters alone cannot tell one
cycle from another. Each one reports the **scope** it was asked for
(`databases`, `measurement`, `tiers`), **when** it ran (`started_at`,
`finished_at`, `duration_seconds`), and **where it came from** (`source`:
`api` for a manual trigger, `scheduler` for a scheduled one). If you drive
compaction one database at a time, this is what separates your trigger's cycle
from the hourly scheduler's cycle that fired in between.

Failures name themselves, completely. Alongside `failed_batches` a record
carries `failed_partitions` — an object mapping each partition that failed to
how many of its batches failed — plus `failed_partition_count` and `error`.
Previously a failure count was a dead end: there was no way to get from "forty
batches failed" to which partitions to retry.

The list is deduplicated by partition, which is what makes it the retry list
rather than a sample. A partition split into nineteen batches that all fail is
one entry with a count of nineteen, so a cycle over eighty partitions and
fifteen hundred batches yields eighty entries. Up to **200 distinct
partitions** are recorded per cycle; past that the record carries
`failed_partitions_truncated: true` so a capped list is never read as
complete.

**`GET /api/v1/compaction/history` gains cycle attribution and a working
`limit`.** Each job record now carries `cycle_id` and `attempt_depth`, and the
endpoint takes `?cycle_id=N` to show only one cycle's work.

Two things to know about reading it. First, `?limit=N` previously returned at
most ten records whatever you asked for — `recent_jobs` was truncated to ten
before the limit was applied — so this is a bug fix, and the parameter now
works up to the 100 records retained. (`scripts/range_schema_acceptance.py`
has been requesting `limit=50` and passing only because its target happened to
land in the last ten.) Second, **these records count attempts, not batches**:
the adaptive splitter retries a failed batch at half size, and every attempt
writes its own record with the same `partition_path`. `attempt_depth` tells
them apart. Do **not** read depth-0 failures as the batch failures: a batch
that fails at depth 0 and is then rescued by splitting counts as *succeeded*
and contributes no failed partition, while still leaving a depth-0
`success: false` record here. The response says `"unit": "attempts"` to make
that hard to misread, and the rule that does hold exactly is
`sum(failed_partitions) == failed_batches` on the cycle record, when not
truncated.

Because attempts multiply, one cycle can emit several hundred records against
a hundred-record ring. The response is explicit about it rather than quietly
partial: `matched` is how many retained records matched before `limit` was
applied — counted over the ring, so on a full ring it is a floor rather than a
total — `page_size` what you got, and `truncated` whether anything is missing.
For a **cycle-filtered** read, `truncated` also accounts for eviction, with
`may_have_evicted_earlier_attempts` saying so; that only fires when the ring is
full *and* the filtered cycle is old enough to have lost records, so it stays
informative on a busy node instead of being permanently true. An unfiltered
read makes no completeness claim — compare `retained` with `capacity` for
that. A cycle-filtered
response also carries that cycle's authoritative outer-level counters
(`cycle_failed_batches`, `cycle_started_batches`, `cycle_failed_partitions`),
which are retained far longer than the job ring and are the numbers to trust.
**For "which partitions do I retry", read `failed_partitions` on the cycle
record** — job history is the attempt-level detail beneath it.

`GET /api/v1/compaction/cycles` (the list) carries `failed_partition_count` but
not the full map: 500 retained cycles of 200 partitions each would be a
multi-megabyte response. Fetch the map from `/cycles/{id}` or
`/history?cycle_id=`.

One smaller shape change: `recent_jobs` in `/api/v1/compaction/stats` was
`null` on a node that had compacted nothing and is now `[]`.

If the cycle you ask about is no longer retained, the response says
`cycle_retained: false` with the retained id range, because an empty result
otherwise reads as "this cycle failed nothing" — the most expensive wrong
answer the endpoint could give.

A cycle that is still running is visible too, as `status: "running"` with live
counters and no `finished_at`, so you can watch a long cycle progress rather
than only read its obituary. A cycle that has been claimed but has not yet
started — the millisecond or two between the trigger's reply and the cycle
body starting — answers `status: "claimed"`, a four-field body with no scope
yet recorded. Note that `GET /cycles` does **not** list a cycle in that state:
its `running_cycle_id` is derived from the retained history so that the list
can never contradict itself, which means during those few milliseconds the
by-id lookup answers `claimed` while the list shows neither the cycle nor a
`running_cycle_id`. Poll the id the trigger gave you, not the list.

`source` has a third value, `unspecified`, for cycles started through an
internal entry point that is neither the API nor the scheduler. No shipped
code path produces it today; it exists so that a future caller is not
silently misattributed to the scheduler.

The list response also carries `retained`, the number of cycles currently
held. A cycle's `error` is truncated at 512 bytes with a trailing
`... (truncated)`, since up to 500 of them are retained at once.

**Three limits worth knowing**, all reported in the 404 body rather than left
to be discovered:

- **History is in memory and bounded at 500 cycles.** At the default schedules
  (`compaction.hourly_schedule = "5 * * * *"` plus
  `compaction.daily_schedule = "0 3 * * *"`) a node consumes 25 cycle ids a day
  even when every cycle finds nothing to do, so 500 is roughly 20 days. Tighten
  the hourly schedule and the window shortens proportionally — about 41 hours
  at `*/5 * * * *`. Nothing is persisted, so a restart empties it.
- **Cycle ids restart at 1 when the process restarts.** An id recorded before a
  restart will not resolve afterwards, and it is not a different cycle's id —
  it is simply gone. The 404 says so instead of claiming the cycle never ran.
- **Cycles run on the node holding the compactor lease**, and the lookup
  answers for the node it is asked on. A cycle triggered on another node is not
  recorded here. On a cluster that manages a compactor lease, the 404 names its
  holder in `lease_holder`, so the response says where to ask instead — the
  same datum the trigger's 503 already reports.

`last_cycle` in `/api/v1/compaction/stats` is unchanged, including its field
names and its meaning: it still describes the most recent *finished* cycle and
never reports `running`.

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
it holds as permanently gone. Until 27.01.1 an unrecorded object was a
permanent condition rather than a lag on a standalone node, or on a cluster
without shared storage or replication, because the cold-metadata sync did not
run there; it does now (see "Cold-tier metadata is rebuilt on standalone
nodes"), so such an object is recorded on the next scan.

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

### The reconciler no longer deletes files a writer has not registered yet ([#1214](https://github.com/Basekick-Labs/arc/issues/1214))

On shared storage, each writer uploads a flushed file to the bucket first,
then registers it in the cluster manifest. A writer that cannot reach the
cluster leader keeps the file pending until it can. The orphan reconciler
runs on the active compactor, which cannot see another writer's pending
files. Once such a file was older than the grace window, the reconciler
deleted it as an orphan. When the writer came back, the file was gone, and
because the writer had already released those rows from its WAL, the
acknowledged rows were lost. A compactor partitioned from the leader could do
the same to files the rest of the cluster had registered, because it compared
the bucket against its own stale copy of the manifest.

A reconciliation run now:

- Catches up with the cluster leader before each sweep. Without a reachable
  leader, the run stops before deleting anything (`abort_reason`
  `manifest_not_synced`, or `leader_barrier_unsupported` when the leader runs
  an older release).
- Asks every node that can ingest (writers) which files it still has pending,
  over an authenticated cluster message. If any of them does not answer, the
  storage half of the run is held: nothing is deleted from storage, the run
  reports `storage_sweep_held` with `storage_sweep_held_reason` naming the
  node, and the new gauge `arc_reconciliation_storage_sweep_held` is 1. A
  writer that is down, or a node that was decommissioned but never removed
  from the cluster, holds the storage sweep until it is back or removed.
- Skips every file listed in the bucket under `_registration_pending/`. Each
  writer writes its pending files there when it stops and whenever a file
  stays pending for more than a minute, and removes the list once everything
  is registered. A writer that stops with files pending does not leave the
  cluster until that list is written, so it keeps holding the sweep if the
  list cannot be written.
- Skips compaction outputs that the manifest repair will adopt.

The skips are counted in the new run fields `skipped_pending_remote` and
`skipped_adoptable`.

`reconciliation.grace_window_seconds` now has a minimum of one hour. A
smaller value is raised to one hour with a warning.

During a rolling upgrade, the storage sweep stays held until every writer
runs this release, because older writers do not answer the new message.

Two smaller fixes on shared storage:

- **DELETE during a rolling upgrade.** While compaction commit is not yet on,
  a row DELETE that reaches a file another writer flushed less than an hour
  ago and has not registered yet now leaves that file as it is and answers
  `503` with `Retry-After`. It used to rewrite the file without registering
  the rewrite, and the reconciler later deleted the rewrite with its surviving
  rows. If other files also failed, the answer is `207` and names both.
- **Ghost manifest entries.** A writer now checks, after registering, that
  each file it just flushed still exists. A file another node deleted in the
  meantime is removed from the manifest again, instead of being left as an
  entry with no object that made DELETEs on that measurement answer `503`
  until the hourly repair.


### A failover no longer floods the logs with two errors per refused write ([#1268](https://github.com/Basekick-Labs/arc/issues/1268))

A writer that refuses writes (it lost contact with the cluster leader, another
writer is the primary, or a stream takeover holds it) logged two ERR lines for
every refused request: `Failed to write to Arrow buffer` and
`HTTP request error status=503`. During a failover a client retrying at a few
hundred writes per second produced over a thousand lines per second. Kubernetes
rotated the container logs within seconds, and the lines that explained the
failover were gone.

- A refused write is logged once per reason every 30 seconds, at WARN:
  `Writes refused on this node (retryable)`, with the reason, where the write
  came from, the error, and how many refusals were not logged since the last
  line. A new reason logs at once, so a standby's routine refusals never hide a
  primary that fences itself.
- The refusal is no longer counted in `arc_ingest_errors_total`, on every write
  path (MessagePack, line protocol, TLE, the import endpoints, MQTT).
- The answer is unchanged: `503` with `Retry-After: 5`. It also carries
  `X-Arc-Ingest-Fenced: 1`. A reader or compactor that forwarded the write logs
  the refusal the same way (reason `forwarded`), not once per request.
- `arc_cluster_ingest_fenced_writes_total` has a `reason` label: `contact`,
  `standby`, `claimed`, `announcing`, `not_designated`, `delete_ack`, `other`.
  It now also counts the stream-takeover refusals, which it missed before. Sum
  by reason for the old total. A refusal is counted per attempt: an MQTT message
  retried while refused counts once per retry.
- Other 503s (shutting down, schema churn) are still logged per request.
### A new Raft leader no longer re-promotes a dead primary writer, or fails over a healthy one ([#1267](https://github.com/Basekick-Labs/arc/issues/1267))

When a node joined the cluster after the primary writer was designated, its
registry lost that writer's `primary` state: the join response carries no writer
state. If that node later became the Raft leader, its failover manager saw no
primary at all:

- If the primary had just died, the new leader "failed over" within three health
  checks to the lowest-id writer still marked healthy. That was the dead primary
  itself while its heartbeats were not yet stale. The no-op promotion counted as
  a success and started the 60 s `cluster.failover_cooldown`, so writes were
  refused for 83 s on a QA cluster instead of about 35 s.
- If the primary was alive and not the lowest id, the new leader failed it over
  for no reason.

Now:

- Failover decisions use the cluster's designated primary (the Raft state) and
  that node's health, not the registry's writer state. A join keeps the writer
  state it knows.
- Re-promoting the designated primary is not a failover.
- Only a completed failover starts the cooldown. A failed attempt is retried on
  the next health check (5 s). A writer the cluster state refuses to promote is
  skipped for a minute; a Raft error (lost leadership, timeout) does not count
  against it.
- A promotion Raft committed is a success, even when the failover's wait times
  out.
- `GET /api/v1/cluster` reports each node's `writer_state`, the
  `primary_writer_id`, and `writer_failover` (on the Raft leader: failed
  attempts, the last failure and its reason, the last failover).

### A tag name with no matching column no longer stops a partition from compacting ([#1258](https://github.com/Basekick-Labs/arc/issues/1258))

Ingest records each file's tag names in its `arc:tags` metadata, and
compaction deduplicates on them. A name with no column of that name in the
files could not be used by compaction, so every cycle failed the same way and
the partition never compacted. Because compaction combines the tag names of
all files in a partition, one such file was enough to stop the whole
measurement-hour. Three inputs produced these names:

- A MessagePack `tag_keys` entry that is not a column of the payload, such as
  a typo, or a name containing a comma. New in this release.
- A line-protocol tag key that starts with `_`. Arc does not store columns
  whose name starts with `_`, but it still recorded the key as a tag. This
  affects earlier releases.
- A line-protocol tag key with an escaped comma (`a\,b=1`). The metadata
  separates names with commas, so it read back as two tags, `a` and `b`. This
  affects earlier releases.

Ingest no longer records a tag name that is empty, is `time`, contains a
comma, starts with `_`, repeats an earlier name, or is not a column of the
data being written. Names are matched to columns exactly, including case. The
rows are always written, and a warning names what was left out. The warning
is logged at most once per second.

Compaction now ignores tag names its files have no column for and
deduplicates on the remaining ones. Partitions stuck by files from earlier
releases compact again without any action. Deduplication still removes only
rows that are identical in every column, so a shorter tag list never makes
two different rows equal.

Tag names with other characters, such as the dot in OTel-style keys like
`service.name`, are handled as before. They are kept, and compaction skips
deduplication for any batch of files that includes one.

Creating or updating a continuous query now refuses a `tag_columns` entry
that starts with `_`, because Arc does not store those columns. A query
already saved with one keeps running, and that name is left out of its
output's tag metadata.

### Compaction works with Azure storage on a connection string or SAS token ([#1104](https://github.com/Basekick-Labs/arc/issues/1104))

Compaction runs in a separate process, and that process authenticated to Azure
Blob Storage its own way: with the account key, or with managed identity when
there was none. Azure primary storage that used either of the other two
authentication methods the configuration accepts could not compact. Every
compaction job failed, and data stayed in its small ingest files:

- **`storage.azure_connection_string`** with no `azure_account_name`: every job
  failed with `no valid Azure authentication method configured`.
- **A connection string together with an account name and key:** the compaction
  process used the key against the public Azure endpoint instead of the endpoint
  in the connection string (Azurite, sovereign clouds).
- **`storage.azure_sas_token`:** the compaction process fell back to managed
  identity, which fails outside Azure.

The compaction process now gets the same credentials as the server and builds
its storage connection the same way. Account-key, managed-identity and S3
deployments were not affected.

The credentials now travel in the job the server hands the compaction process,
not in its environment. If the binary is downgraded while the server keeps
running, the older compaction process looks for them in the environment and
compaction fails until the server is restarted. An upgrade in place is
handled.

### Retention policies apply to the cold tier ([#168](https://github.com/Basekick-Labs/arc/issues/168))

With tiered storage, retention deleted expired files from local storage only.
Files tiering had moved to the cold tier were kept forever, so cold storage grew
without bound, and a measurement whose data had all moved to cold was never
examined at all.

A policy now covers the cold tier too. After each measurement's local files,
retention checks the cold files whose partition starts before the cutoff. A
file is deleted from cold storage when every row in it is older than the
cutoff, then its tier row is removed. Measurements that exist only in the cold
tier are processed. The scheduled run, manual execution and dry runs all
include the cold tier.

The execute response reports the cold tier separately in `cold_files_deleted`
and `cold_rows_deleted`. These are also included in `deleted_count` and
`files_deleted`.

The cold tier never stops local retention. A cold file that cannot be read or
deleted, a quarantined one, and the cold rows on a node with
`tiered_storage.cold.enabled` off are left in place. They are counted in
`cold_files_skipped`, explained in `cold_skipped_reason`, and the execution is
recorded as `completed_with_errors`. A tier row whose cold object is already
gone is removed, or pointed back at the local copy of the file when one exists.

Backups now tolerate a cold object that retention deletes while a backup is
running. The object is skipped and counted like a local file that disappears
mid-run. Any other cold read failure still fails the backup.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#1053](https://github.com/Basekick-Labs/arc/pull/1053).

### Compaction waits for recent compaction outputs again ([#1235](https://github.com/Basekick-Labs/arc/issues/1235))

Each tier skips a partition while its newest file is younger than the tier's
`min_age_hours`, so compaction does not race late writes. Since 26.09.1,
compaction outputs carry a batch number (`_b<N>_compacted`, `_b<N>_daily`), and
the check could not read their timestamp, so it ignored them.

The visible effect was on the daily tier, whose inputs are mostly hourly
outputs. A day whose every hour had been hourly-compacted passed the check
whatever its age. With the default schedule it was compacted at the first
03:00 run after the day, while late writes could still be arriving.

The check reads these names again. Such a day now waits until its newest hourly
output is `daily_min_age_hours` old: with the defaults, the second 03:00 run
after the day, as before 26.09.1. Days older than
`daily_skip_file_age_check_days` (default 7) are not checked, as before.

Contributed by [@jallegri](https://github.com/jallegri) in [#1250](https://github.com/Basekick-Labs/arc/pull/1250).

### WAL-on ingest is no longer capped by one writer's fsync latency ([#1246](https://github.com/Basekick-Labs/arc/issues/1246))

With the WAL enabled, ingest topped out at a rate set by the storage's fsync
latency, not by CPU or disk bandwidth. One goroutine wrote every WAL entry and
ran `fdatasync` itself every 1 MiB, and on every buffer flush's checkpoint,
waiting each time for the disk to answer. On AWS gp3 that is about 170 MB/s of
WAL, roughly 3.5M points/s of MessagePack, and it was the same on a 4-, 8- or
16-vCPU server. Above it the WAL queue fills and entries are dropped. The
write is still acknowledged, so those rows have no WAL copy until the next
flush writes them to Parquet, and a crash in that window loses them.
`arc_wal_dropped_entries_total` counts these drops.

Syncs now run on their own goroutine, beside the writer (group commit). The
writer keeps appending while a sync is in flight, and one sync covers
everything written before it started, so throughput follows the disk's
bandwidth instead of its round-trip time.

Measured on m7i.4xlarge with the sizing campaign's gp3 volume (500 MB/s, 6,000
IOPS), MessagePack, 8 workers × 5,000-record batches, WAL on with
`fdatasync`, the same builds before and after this change:

| Offered | WAL entries dropped, before | after | WAL disk writes, before | after |
|---|---|---|---|---|
| 4M rows/s, 120 s | 2,637 | 0 | 195 MB/s | 226 MB/s |
| 6M rows/s, 30 s | 7,379 | 0 | 144 MB/s | 284 MB/s |
| 8M rows/s, 30 s | 20,797 | 0 | 148 MB/s | 357 MB/s |
| 10M rows/s, 30 s | 34,625 | 0 | 156 MB/s | 408 MB/s |

Write p95 stayed at 2.0–2.3 ms in every run. The WAL-on figures in the sizing
guide were measured before this change.

Durability is unchanged:

- A flush checkpoint is still answered only after a sync that covers it, in
  every sync mode, `async` included.
- A data sync is still requested once 1 MiB is unsynced, and every 100 ms
  while anything is unsynced.
- A rotation still syncs the old file before it is closed.

A failed sync is now reported to the checkpoints it covered, as before. It also
makes the writer move to a new WAL file, because Linux reports a lost
write-back only once, and a later sync of the same file can succeed over data
it never wrote. Failed syncs are counted in the new `arc_wal_failed_syncs_total`.

`wal.sync_bytes` (default 1048576, `ARC_WAL_SYNC_BYTES`) sets the byte
threshold. It bounds how much written data a sync may leave behind, not ingest
throughput. A negative value is refused at startup.

Above the new ceiling, when the WAL queue is full, an entry is still dropped and
the write still acknowledged. What admission should do instead is
[#676](https://github.com/Basekick-Labs/arc/issues/676).

### Ingest memory is visible while flushes back up ([#1038](https://github.com/Basekick-Labs/arc/issues/1038))

`arc_buffer_records_buffered` counts only records still in a buffer. A record
handed to the flush queue, or being written, left that gauge while it was still
in memory. So under backpressure, when ingest holds the most, the gauge could
read 0: in one measurement it stayed at 0 for 40 seconds while 43 million
queued rows were written out.

Two new gauges cover the rest, and the three sum to the records ingest holds
in memory:

- `arc_buffer_records_queued`: records in flush tasks waiting for a worker.
- `arc_buffer_records_inflight`: records being written to storage, including
  synchronous flushes (`FlushAll`, the flush before a DELETE, shutdown).

They also appear as `buffer_records_queued` and `buffer_records_inflight` in
`GET /api/v1/metrics` and `/metrics` as JSON, under `buffer` in
`GET /api/v1/metrics/endpoints`, and in the stored time series. A climbing queued
count with a flat buffered one means storage is the bottleneck, not the write
rate.

### Ingest reports buffered memory in bytes, and warns before it becomes a problem ([#1025](https://github.com/Basekick-Labs/arc/issues/1025))

Ingest buffers grow with active measurements, `max_buffer_size` and slow
storage, and Arc does not cap them. Until now the only signal was a record count,
and bytes per record vary more than 100x between workloads, so it could not
answer how much memory ingest holds. Three new gauges and a warning do:

- `arc_buffer_bytes_buffered`: approximate bytes held by ingest, across
  buffers, the flush queue and writes in progress. MessagePack writes count
  their payload size; other writes estimate from their columns.
- `arc_buffer_oldest_unflushed_seconds`: age of the oldest record not yet in
  storage, including queued and in-flight flushes. It is what a crash would
  lose without the WAL.
- `arc_buffer_memory_limit_bytes` and `arc_buffer_memory_pressure`: the memory
  limit Arc detected (the container limit where there is one) and whether
  buffered bytes have reached half of it.

When buffered bytes reach 50% of the limit, Arc logs a warning naming the two
settings that reduce held memory: `ingest.max_buffer_size` and
`ingest.max_buffer_age_ms`. Lowering either flushes sooner, at the cost of more,
smaller Parquet files and more compaction work. The warning fires once per
episode: it re-arms only after usage falls below 40%. Arc still rejects nothing
and caps nothing.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1040](https://github.com/Basekick-Labs/arc/pull/1040).

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1042](https://github.com/Basekick-Labs/arc/pull/1042).

### Arc's own storage roots are no longer treated as databases ([#1012](https://github.com/Basekick-Labs/arc/issues/1012))

`_schema` (the field-schema anchors of every database) and `_compaction_state`
are directories Arc keeps for itself at the storage root. `GET /api/v1/databases`
hid them, but the database-scoped endpoints accepted their names:
`GET /api/v1/databases/_schema/measurements` listed the anchor tree as if the real
databases were measurements. That made `_schema` look like a stray database to
delete.

They are now refused wherever a database is named:

- `GET /api/v1/databases/{name}` and `/measurements`: 404.
- `DELETE /api/v1/databases/{name}`: 403, as before.
- Creating or updating a retention policy, and `POST /api/v1/delete`: 400.

A stored retention policy on one of these roots logs a warning at startup.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1032](https://github.com/Basekick-Labs/arc/pull/1032).

### OSS Helm chart: `arc.config.enabled` no longer leaves the pod unable to start ([#1194](https://github.com/Basekick-Labs/arc/issues/1194))

With `arc.config.enabled=true`, the OSS chart mounted an `arc.toml` ConfigMap
that no template created, so the pod never started (`MountVolume.SetUp failed
for volume "config"`). `helm lint` and `helm template` both passed. The template existed
but was never committed: the `.gitignore` rule for the `arc` binary also
matched the `helm/arc/` directory, so new chart files were silently ignored.
The rule now matches only the binary at the repository root and a binary
built inside `cmd/arc`.

- **The ConfigMap renders**, and a change to `arc.config.contents` now rolls
  the pod on `helm upgrade`. The file is mounted with `subPath`, which a
  running pod never refreshes, so without the roll an upgrade kept the old
  config.
- **New `auth.bootstrapToken` values** set the initial admin token, from a
  chart-created Secret (`value`) or your own (`existingSecret`, key
  `bootstrap-token`), as the Enterprise chart already does. Arc uses it on
  first run only; once tokens exist it is ignored.
- **Both charts refuse a bootstrap token shorter than 32 characters** at
  render time. Arc rejects such a token, logs one error and starts with no
  admin token, so the install looked healthy and nobody could log in.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1198](https://github.com/Basekick-Labs/arc/pull/1198).

### A DELETE removes rows that were still buffered ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

`POST /api/v1/delete` and `DELETE /api/v1/databases/{name}` only ever looked at
Parquet files. Rows written shortly before (still in the ingest buffer, not yet
flushed) were left out, and showed up once the buffer flushed. Now both flush
the buffered rows of that measurement (or database) first and wait for them to
reach files, so every row acknowledged before the request is deleted. A dry run
does not flush. While an earlier flush failure is outstanding (its rows wait
for WAL recovery), both answer 503 with `Retry-After: 30` instead.

A database deletion also withdraws the files of that database still waiting to
be registered in the cluster manifest, so none registers after the database is
gone.

### A DELETE no longer publishes a rewrite of a file a compaction replaced ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

On a cluster, `POST /api/v1/delete` scans the files of the measurement, then
publishes a rewrite of each file without the deleted rows. A compaction of the
same hour could replace one of those files in between. The DELETE then still
registered its rewrite of the replaced file, next to the compaction's output,
which still held the deleted rows. The deleted rows came back and the others
were counted twice.

Now a DELETE's manifest update applies only while the file is still registered
with the content the DELETE read. If a compaction replaced it, the DELETE
waits until the compaction's output reaches the node (up to 30 s), scans
again, and deletes from the output. After three rounds of files
changing under it, it answers `503` and lists the files it did process; the
rows of those files are deleted. Retry to finish. `allow_unreachable: true`
also applies to the later scans; files it skipped are listed in
`unreachable_files`.

This covers a compaction that commits while the DELETE runs. The opposite
order, a compaction that read a file before the DELETE rewrote it and commits
after the DELETE, is covered by the staged compaction commit below, on local
and object storage.

A DELETE also leaves alone files in storage that the cluster manifest does not
list and this node is not registering, such as a compaction output not yet
committed. It first catches up with the cluster manifest, so a file registered
a moment ago is not missed; if it cannot, it answers `503`.

With compaction commit on, a DELETE scans the files the cluster manifest lists
for the measurement, read after it caught up with the manifest, not only the
files it found in storage. A compaction can commit between the DELETE's
storage listing and that catch-up: the listing then has the compaction's
inputs, which the manifest no longer lists, and not its output. The DELETE
used to skip the inputs, never see the output, and answer success having
deleted nothing, while the output kept the rows.

A file the manifest lists that is not on the node yet is waited for (up to
30 s per scan; the manifest is read again meanwhile, so a file it stops listing
is not waited for). A file still missing then answers `503` listing it
in `unreachable_files`; `allow_unreachable: true` runs the DELETE without it
and lists it in `unreachable_files`, including when nothing matched. A dry run
does not wait, and lists the files it could not count. This applies on
clusters without stream takeover too. Before, rewriting such a file deleted it
and left its rewrite unregistered, so it never replicated.

The cluster turns this on by itself once every node runs this release (`GET
/api/v1/cluster` shows `compaction_commit.enabled`, and on the leader the
nodes keeping it off in `blocking_nodes`). Until then DELETE behaves as
before. From then on a node running an earlier release cannot join. See the
upgrade note below about rolling a node back.

### Deleting a database also removes its parked files, and survives a leader change ([#1236](https://github.com/Basekick-Labs/arc/issues/1236))

With stream takeover on, a file whose registration the cluster refused is
parked under `_refused/<database>/`, and its WAL entries are kept until it is
resolved. `DELETE /api/v1/databases/{name}` did not see those files: they and
their WAL entries outlived the database, and a later re-flush could bring the
deleted data back. Database deletion now removes them.

A parked file can be the only copy of acknowledged rows, so it is removed only
when the cluster's record of this database deletion covers every batch in it.
Its batches are then settled as deleted and its WAL entries released. The
removal is journaled first, so a deletion interrupted by a crash finishes on its
own after the restart. If the record does not cover a file, or a registration
for it is still in flight, the request fails with a retryable 503 and the file
is kept.

On clusters without tiered storage, a leader election during the deletion used
to fail the request with a 500 after some manifest batches were applied. It is
now retried for about 7 seconds, as on the tiered-storage path. A longer
election still fails the request; retrying it completes the deletion.

Contributed by [@jallegri](https://github.com/jallegri) in [#1252](https://github.com/Basekick-Labs/arc/pull/1252).

### A node that missed a delete removes its stale copy ([#1071](https://github.com/Basekick-Labs/arc/issues/1071))

On a cluster with local storage, each node removes its own copy of a file when
the cluster manifest drops it. A node missed that when it was down while the
file was deleted and rejoined from the leader's snapshot, or when it crashed
with the removal still queued (or the removal failed). The copy stayed on its
disk, and its queries kept returning the deleted rows, or duplicates of
compacted ones, until the opt-in orphan sweep removed it. A rig reproduced it:
a reader down while a measurement was deleted kept all 6 files and returned
the 50 deleted rows after it rejoined.

Each node now records the local files it has seen registered, in
`seen-local.log` in `cluster.raft_data_dir`. Every 5 minutes it syncs with the
leader and removes a recorded file the manifest no longer lists, when:

- the node's manifest has applied past the file's registration;
- the file has been unlisted for 10 minutes (two passes that ran: a pass
  skipped for a compaction pause or a failed sync starts the wait again);
- it is not waiting to register on this node, not held by an edge-sync
  delivery, not part of a compaction in progress, and not in the cold tier on
  this node (an unreadable tier record counts as cold);
- it is still the same file (size, modification time and inode). The copy is
  moved aside under `_stale_local/` and checked again there before it is
  deleted, so a file written to the path meanwhile goes back.

A file the node never saw registered is never removed by this sweep: a writer's
flushed files not yet registered, a restore in progress, edge-sync deliveries,
and copies left over from before the upgrade (those stay for the reconciler's
orphan sweep). In the rig above, the reader removed the 6 copies within two
passes and kept the others.

A pass that would remove more than `cluster.stale_local_sweep_max_fraction`
(default `0.5`) of the node's tracked files, and at least 1000, removes
nothing: a manifest rolled back by an unsafe Raft recovery looks like that.
The node logs an error and `arc_stale_local_blocked` reads `1`; if the
deletes are real (a long outage with heavy retention), raise the setting and
restart the node. A value of 0 or below, or not a number, is refused at
startup.

The sweep is on by default (`cluster.stale_local_sweep`) on local storage with
`cluster.replication_enabled`, and stops with
`cluster.replication_catchup_enabled = false`. It holds about 150–200 bytes
of memory per local file, and a journal of up to about 1.5 times that on the
Raft volume; if the journal cannot be written, the node stops tracking (and
removing) until a restart and `arc_stale_local_tracking_failed` reads `1`.
`GET /api/v1/cluster` reports the last pass under `stale_local`. New metrics:
`arc_stale_local_unlinked_total`, `arc_stale_local_tracked`,
`arc_stale_local_blocked` and `arc_stale_local_tracking_failed`. An earlier
release ignores `seen-local.log` and `_stale_local/`.

### Compaction no longer loses rows when the compactor's disk fails, or brings deleted rows back ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

On a cluster with local storage, a compaction registered its output and
deleted its inputs in two steps, after it had already removed its own copies
of the inputs. Nothing checked that the inputs were still the files it had
read. Two things went wrong:

- The other nodes removed their copies of the inputs as soon as the deletes
  applied, while the output existed only on the compactor until they pulled
  it. If the compactor's disk was lost in that window, the rows were gone on
  every node.
- A row DELETE that rewrote an input while the job ran left the job's output,
  built from the input as it was before the DELETE, registered next to the
  rewrite. The deleted rows came back and the others were doubled.

Now, once compaction commit is on (see the DELETE entry above), a compaction
on local storage is staged first:

1. The compactor writes its output under `_compaction_staged/` in its storage,
   where no query reads it, and leaves its inputs alone.
2. It stages the job in the cluster. Every other node that keeps a local copy
   of the data pulls the staged output, makes it durable and reports holding
   it.
3. Once at least one other node holds it, the compactor commits. The cluster
   registers the output and deletes the inputs in one step, only if every
   input is still registered exactly as the job read it. Each node holding the
   staged copy moves it into place, so it never pulls the output again.

If an input changed (a DELETE, retention, tiering or another compaction got
there first), the commit is refused and the job is dropped. Its inputs were
never touched, so nothing is lost, and a later cycle compacts the partition
again. A compaction pause (a cluster restore) drops jobs that have not
committed yet.

What to expect:

- A staged output takes disk space on every node that holds it until the
  commit, then replaces the inputs.
- If no other node that keeps local copies is up, a staged job waits for one
  and is dropped after 10 minutes (longer for a large output), then
  recompacted on a later cycle. Compaction makes no progress while the
  compactor is the only node holding data.
- A node that missed a commit moves its staged copy into place when it
  catches up; one that has no copy pulls the output as before.
- A job staged by a compactor that crashed is aborted when the compactor
  restarts, or by the leader 10 minutes after the compactor stopped
  answering.

The second problem existed on object storage (S3, Azure) too, where a
compaction is now staged as well:

1. The compactor uploads its output under `_compaction_staged/` in the bucket,
   recording its checksum on the object, and leaves its inputs alone.
2. It stages and commits the job at once (there is no copy to wait for on
   other nodes: the bucket is shared). The commit registers the output and
   unregisters the inputs only if every input is still registered exactly as
   the job read it.
3. A finisher copies the staged object to the output's path inside the store
   (S3 CopyObject, or a multipart copy above 5 GiB; Azure Put Blob From URL,
   or blocks from URL above 5000 MiB), deletes the input objects, and ends
   the job. The output appears at its path only once complete. If Azure
   refuses the server-side copy (some Entra or managed-identity setups), the
   finisher copies the bytes through the node instead.

The job's compactor runs the finisher at once; if it is gone, the leader takes
the job over after 5 minutes. One node at a time finishes a job (a lease in
the cluster state).

Between the commit and the copy, reads see the inputs' rows (the input objects
are still there), and a DELETE on that measurement waits for the output, up to
30 s per scan, then answers `503`; retry it. A DELETE that rewrites or deletes
the output removes the job's input objects at once. Between the copy and the
input deletes, reads see both, for a moment, as before this change.

If the output leaves the cluster manifest before the finisher runs:

- removed by a DELETE, retention, a database deletion, tiering or a restore:
  its rows were dealt with, and the input objects are deleted too;
- removed by anything else (the reconciler's orphan sweep): the inputs are
  registered again, if all their objects are there;
- otherwise the job is marked wedged: `GET /api/v1/cluster` lists it under
  `compaction_commit.wedged_jobs`, the cluster logs an error, and nothing is
  deleted. Check the data, then end it with
  `POST /api/v1/cluster/compaction-jobs/{job}/abort` (admin).

A compaction pause (a cluster restore) waits for committed jobs to finish
before it takes effect; jobs not yet committed are dropped. A backup taken
while a job finishes copies its output from the staged object, and leaves the
input objects out. Tiering does not move a file a compaction owns. The leader
removes staged objects no job owns after 6 hours; on S3, add a lifecycle rule
that aborts incomplete multipart uploads, as for any bucket Arc writes to.

New metrics: `arc_compaction_finished_total` (committed jobs their finisher
ended), `arc_compaction_wedged_total` (jobs marked wedged) and
`arc_compaction_copy_fallback_total` (copies Azure refused to do server-side).
`GET /api/v1/cluster` reports `compaction_commit.finalizing` and
`compaction_commit.wedged_jobs`.

On a shared bucket, compaction now also keeps the cluster manifest. Until now,
a cluster on S3 or Azure with `cluster.replication_enabled = false` (as the
Helm chart and the docs set it for shared storage) never ran the compaction
completion watcher. So compaction never updated the cluster manifest there:

- its outputs stayed unregistered;
- the inputs it consumed stayed listed after their objects were deleted.

Queries were not affected, since they read the bucket. Every cluster node now
runs the watcher, whatever its storage.

On every cluster, compaction used to register its outputs with the compaction
tier (`hourly`, `daily`) as their tier, which the staged compaction commit
does not take as inputs. They are now registered `hot`. The leader's manifest
repair (below) moves older entries to `hot`, each only if it has not changed
since it was read, and a DELETE scans them as hot files meanwhile.

To bring an upgraded cluster's manifest back in line with its bucket, the
leader repairs it. The first run comes 2 minutes after a node becomes leader,
then the repair runs hourly (10 minutes after a run that stopped early). It
waits while a compaction pause is in force. On a shared bucket, with stream
claims off, each run also does two things:

- It drops a hot entry whose object is gone, unless a finalizing compaction is
  still copying that object into place.
- It registers a compaction output the manifest does not list, once the output
  is over an hour old, unless tiering records it as cold. A compaction output
  is a `*_compacted.parquet` file in an hour directory or a `*_daily.parquet`
  file in a day directory. Each one is read for its checksum, at most 1000
  objects or 10 GiB per run; the rest waits for the next run. On a large
  bucket, the first repairs after an upgrade can take a while. While outputs
  of a measurement wait, its stale entries are kept.

Other objects the manifest does not list are left to the reconciler. These
are leftovers of a delete that failed after its manifest change. The repair
registers compaction outputs well before the reconciler's 24-hour orphan
grace, so the reconciler no longer counts them as orphans.

A DELETE on a shared bucket scans what queries read, so it works before the
repair has run:

- A listed file whose object is gone is skipped, not waited for.
- An object the manifest does not list is scanned too. Its rewrite is
  registered (an unregistered rewrite would be an orphan the reconciler
  removes); when every row matches, it is removed. With stream claims on, such
  an object is left alone, as before.
- The input objects of a committed job that has not finished yet are skipped,
  because its output holds their rows.

As on replication clusters, a node that lost the compactor role while it
still had unprocessed completion manifests keeps a cluster-wide compaction
pause (a restore) from taking effect until it compacts again or restarts.

New metrics: `arc_manifest_repair_dropped_total`,
`arc_manifest_repair_adopted_total` and the gauge
`arc_manifest_repair_pending`. On the leader, `GET /api/v1/cluster` reports
the last repair under `compaction_commit.manifest_repair` (`dropped`,
`adopted`, `retiered`, `pending`, `error`).

### Delete API catches dangerous keywords glued to numbers or literals ([#1080](https://github.com/Basekick-Labs/arc/issues/1080))

The delete endpoint's keyword scan used `\b` directly on the masked WHERE
clause. Digits and the masker's identifier-shaped placeholders can prevent a
word boundary, allowing inputs such as `1=1UNION ...` or `host='a'UNION ...`
to pass this pre-filter. The validator now separates token-start keywords
from numbers and removes literal placeholders before scanning, matching the
query endpoint's existing handling.

Both endpoints now split off a number the way DuckDB reads it, as one token:
with `_` separators, a fraction (`1.`, `.5`) and an exponent with or without a
sign. Before, only a plain digit run was split, so `1e0UNION`, `1.5e3UNION`,
`1.e1UNION` or `1_0UNION` still hid the keyword from the delete endpoint and
from the measurement query endpoint's WHERE check, while DuckDB ran them as a
`UNION`.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1081](https://github.com/Basekick-Labs/arc/pull/1081).

### A storage error that only mentions "404" no longer reads as a missing object ([#1238](https://github.com/Basekick-Labs/arc/issues/1238))

Arc decided whether an S3 or Azure object was missing by looking for `404`,
`NotFound` or `NoSuchKey` anywhere in the error's text. That text carries the
request URL (endpoint, bucket, key and, for a backup, the backup id), the
storage service's request id and, for a connection failure, the local port. So
an outage, a throttled request or a refused connection could read as "the
object is not there" whenever one of those happened to contain the digits.
Arc's file names carry nanosecond timestamps and backup ids carry the time
they were taken, so this was not rare: an unreachable backup target answered
`GET /api/v1/backup/{id}` with 404 `Backup not found` for every backup taken
at a minute such as 14:04, and `POST /api/v1/backup/restore` started a restore
with a guessed mode (202) instead of refusing. File-existence checks answered
"absent" in the same cases. Tiering, edge sync, compaction, backup restore and
the cluster's file replication all branch on those checks.

A missing object is now recognised only from the storage service's own
answer: `NoSuchKey` on an S3 read, the bodiless 404 of an S3 `HEAD`, and
`BlobNotFound` on Azure. The backup API answers an unreachable target with 503
naming the target, as it already did when the error text happened to contain
no `404`.

Two narrower changes come with it:

- **A missing bucket or container is not a missing object.** Reading an object
  from an S3 bucket that does not exist (`NoSuchBucket`), or from an Azure
  container that does not exist, is now an error that says the store is
  missing, instead of "object not found". The backup API answers 503 for a
  target whose bucket does not exist, where it answered 404 or started a
  restore. Checking whether a file exists in a missing bucket or container is
  an error too ([#1242](https://github.com/Basekick-Labs/arc/issues/1242)): an
  S3 `HEAD` cannot tell a missing bucket from a missing key, so after a `HEAD`
  that finds no object Arc asks S3 whether the bucket exists, at most once a
  minute while it does. An answer other than "no such bucket", including a 403
  for credentials that may not list the bucket, keeps the object absent, as
  before. The places that run before a store's first write treat a missing
  store as empty, because most S3-compatible stores create the bucket on that
  write: a backup to a new target, a restore into a new cold bucket, an edge
  hub receiving its first file, and `GET` or `POST /api/v1/databases` on a
  fresh deployment. Creating a database on a store whose bucket did not exist
  yet answered 500 before and works now.
- **A 404 without a storage error code is an error.** A proxy or gateway
  answering 404 with its own page, rather than the storage service's error, is
  no longer taken to mean the object is missing. An S3-compatible store that
  answers a read of a missing key without the `NoSuchKey` code would now
  surface an error; AWS S3 and SeaweedFS both send it.

A local backend read of a missing file (`ReadTo`, ranged and staged reads) is
now recognised as missing on every path, not only when the path contained
`404`.

### Updating a continuous query or retention policy no longer resets the fields you leave out ([#1023](https://github.com/Basekick-Labs/arc/issues/1023))

`PUT /api/v1/continuous_queries/:id` and `PUT /api/v1/retention/:id` wrote
every column from the request body. Any field the body left out was stored as
its zero value, and the call still returned `200`:

- **A continuous query stopped producing.** Leaving out `is_active` stored
  `false`, and the scheduler stopped the job and did not restart it. The only
  signals were `is_active: false` in the response and a debug log line.
  Leaving out `tag_columns` removed the dedup key compaction uses for the
  query's output ([#521](https://github.com/Basekick-Labs/arc/issues/521)).
- **A retention policy could delete more than it was set to.** Leaving out
  `measurement` turned a policy pinned to one measurement into one that
  deletes across the whole database. Leaving out `buffer_days` brought the
  cutoff forward by the stored buffer, so data that many days younger was
  deleted. Leaving out `is_active` paused the policy.

**Both endpoints now take a partial update.** A field the body leaves out keeps
its stored value. A field the body carries is checked with the same rules
create applies, then written. So `{"is_active": false}` on its own pauses
either kind of object. That is the request the Launchpad and Arc Cloud
retention pause buttons and the Python client's update methods already send:
it returned `400` before, and now works. A body with no fields at all is refused with `400`.

`null` clears a field where an empty value means something:

| Endpoint | Fields `null` clears | `null` refused (`400`) |
|---|---|---|
| Continuous query | `description`, `tag_columns`, `retention_days`, `delete_source_after_days` | `name`, `database`, `source_measurement`, `destination_measurement`, `query`, `interval`, `is_active` |
| Retention policy | `measurement` (applies the policy to the whole database) | `name`, `database`, `retention_days`, `buffer_days`, `is_active` |

`retention_days` must still be greater than `buffer_days`. When the body
carries only one of the two, the other is the stored value, and the check runs
in the same statement as the write.

**What changes for API clients:**

- **To clear a field, send `null`. Leaving the field out no longer clears
  it.** This matters for an edit form that drops empty fields from the body:
  clearing a continuous query's `description`, `retention_days` or
  `delete_source_after_days`, or setting a pinned retention policy back to
  "all measurements", is now ignored instead of applied. For a retention
  policy, that means it keeps deleting only the measurement it was pinned to.
- **Creating a continuous query without `is_active` now creates an active
  one.** The column has always defaulted to active, but create stored `false`
  instead, so such a query never ran. Send `"is_active": false` to create one
  paused. Retention policies keep their create defaults on purpose
  (`is_active` false, `buffer_days` 0): turning `is_active` on by default would
  make policies that existing clients create start deleting data.
- **A negative `buffer_days` is refused** on create and update. It moved the
  cutoff the other way, deleting data younger than `retention_days`.
- **A duplicate retention-policy name on update is now a `400`,** as on create,
  instead of a `500`.
- **An empty `measurement` on a retention policy is stored as `null`.** Both
  have always meant the whole database. `GET` now returns `null` for both
  instead of returning whichever one was sent.

This replaces the 26.09.3 advice to send the whole definition on every `PUT`.
Clients that do so keep working unchanged.

Leaving `tag_columns` out of an update that changes the query keeps the stored
list. That can no longer cost rows: compaction removes only rows identical in
every column, see the next entry.

### Compaction removes only exact duplicate rows ([#1232](https://github.com/Basekick-Labs/arc/issues/1232), [#1227](https://github.com/Basekick-Labs/arc/issues/1227))

Compaction deduplicated a measurement by a key: one row per tag values and
`time` when the files carry tag metadata, or one row per `time` for
continuous-query output without `tag_columns`. Every other row sharing that key
was deleted, whatever its field values. Which row survived was arbitrary; in
practice it was the one in the oldest file. This lost data in several ways,
none of them reported:

- **A corrected or re-sent point disappeared.** A second write with the same
  tags and time but a new value was deleted, and the old value kept.
- **Two writes of the same point with different fields lost one of them.** For
  example, `usage=1` from one write and `temp=5` from another, same series and
  time. Two inputs reporting the same series is a common source of this.
- **A grouped continuous query without `tag_columns` lost series.** For example
  `GROUP BY host` with no `tag_columns` declared. A window in which one host
  reported was marked for time-only deduplication. Once it shared a partition
  with a window in which several hosts reported, every host but one was
  deleted from the multi-host windows.
- **A continuous query whose `tag_columns` did not cover its grouping lost
  rows.** For example, it groups by `host, region` but lists only `host`.

**Compaction now removes a row only when another row is identical to it in
every column**: time, every tag and every field. Rows that share their tags and
time but differ anywhere are different data, and all are kept. The tag columns,
or `time` for continuous-query output without them, only narrow where whole
rows are compared. On typical measurements, and on very wide sparse ones,
compaction costs about the same as before. A partition in which most rows are
duplicates costs more, up to about half again as long. An hour replayed whole
from the WAL is one example.

What is still removed:
- a copy replayed from the WAL;
- a retried write;
- a replicated copy;
- any other byte-for-byte duplicate.

**Behaviour changes:**

- **Re-sending a point with a new value no longer replaces the old one.** Both
  rows stay, and queries see both: `count` counts two, `avg` averages them.
  Delete the old row explicitly if it must go.
- **Re-running a continuous-query window usually leaves a second row.** This
  covers a retry, a run after a crash, and a manual `execute` of a window
  already processed. Floating-point aggregates (`avg`, `sum`, `stddev`) are not
  bit-for-bit reproducible across runs, so the re-run's value almost always
  differs in its last digits and is kept beside the first. A re-run with late
  data is kept beside the first by design.
- **Declaring fewer `tag_columns` than the query groups by no longer loses
  rows.**
- **Tagless raw rows are deduplicated only alongside continuous-query output.**
  Raw ingest without tags is still never deduplicated, unless its rows share a
  partition with continuous-query output. Those rows then get the same
  exact-row check.
- **`0.0` and `-0.0` count as equal**, and so do two NaN values.
- **Duplicates split across compaction batches can stay.** A pair split by the
  batching of a large partition, or between a compacted file and a WAL-replayed
  one, may stay as a duplicate. That was already the case before.

Rows this bug already deleted cannot be recovered by the upgrade.

**Rollback:** continuous-query output without `tag_columns` is now marked with a
new Parquet key, `arc:dedup_exact`, instead of `arc:dedup_time`. Earlier
versions read `arc:dedup_time` as "keep one row per timestamp". If they saw it
on this release's output, which can hold several rows per timestamp, they would
delete rows. They ignore the new key, so a rolled-back or not-yet-upgraded node
leaves that output undeduplicated rather than losing rows. Files marked
`arc:dedup_time` by earlier versions are still recognised.

### The WAL is reclaimed again after a crash recovery ([#1222](https://github.com/Basekick-Labs/arc/issues/1222))

After a restart that replayed WAL entries from the previous run, the periodic
WAL purge stopped deleting files for the rest of the process's life. The WAL
directory then grew with ingest until a clean shutdown. A node that crashed
once and kept running for days kept every WAL file it wrote, until disk
pressure. One object-store outage could do the same with no crash at all: the
recovery that runs after a failed flush replays a large entry in row ranges.

The cause was a safeguard that never lifted. A replayed entry's flush records
a checkpoint for the earlier run's entry, and that checkpoint can be the only
proof that the entry was flushed. The purge therefore never deleted the file
holding it, nor any file after it. The proof matters only while a WAL file
from an earlier run still exists. Recovery deletes that file as soon as its
entries are durable, so the safeguard was holding back files for no reason.

The purge now keeps such a file only while a WAL file this process did not
write is still in the directory. If the directory cannot be listed, it keeps
it. While it waits, it logs the file it is waiting on:

```
WAL purge stopped at a file holding flush proof for an earlier process WAL file; it resumes once recovery has removed that file
```

Nothing to configure. A WAL directory that grew under this bug is reclaimed
when the node restarts on this release: startup recovery deletes the files
whose entries are already flushed.

### Rows written without a time column keep their ingest time after a WAL replay ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

A columnar MessagePack write without a `time` column gets the time it was
ingested. The WAL kept the payload as the client sent it, without that time.
A replay of the entry after a crash, or after a failed flush, generated the
time again, as the replay time. The rows landed with a later timestamp than
the one they were acknowledged with, possibly in a later hour partition.

The WAL entry of such a write now holds the generated time. These writes no
longer take the typed fast decode path, which cannot keep the time in the WAL.
Writes that carry a `time` column are unchanged.

### A write with a numeric measurement no longer poisons its WAL file ([#1225](https://github.com/Basekick-Labs/arc/issues/1225))

A columnar MessagePack write may name its measurement with an integer
(`"m": 5`), and ingest stores it as `measurement_5`. The WAL keeps the payload
as the client sent it, and WAL recovery accepted only a string. Recovery
therefore failed the whole WAL file holding such a write, including entries
that were already flushed, and replayed no later files in that pass. After
three failed passes it quarantined the file as `.wal.failed`, and every
unflushed entry in it was no longer replayed.

Recovery now names an integer measurement exactly as ingest does. A WAL file
that failed for this reason and is still in the WAL directory replays on the
next pass. One already quarantined stays quarantined. Do not simply rename it
back to `.wal`: recovery would replay every entry in it, including ones
already stored, and duplicate them. Follow the quarantine procedure in
`docs/operations/wal-recovery.md`.

More generally, an intact WAL entry whose payload cannot be decoded no longer
fails its file when its flush checkpoint shows it is already in storage.

### A quarantined WAL file no longer stops the WAL purge until restart ([#1225](https://github.com/Basekick-Labs/arc/issues/1225))

The recovery that runs after a failed flush can quarantine a WAL file this
process wrote. The writer was not told, and kept waiting for that file's
unflushed entries. From then on the periodic WAL purge deleted no WAL file
until the process restarted, and the WAL directory grew with ingest.

When recovery quarantines a file, the writer now stops waiting for the
entries in it and the purge continues. It logs:

```
Released the pending WAL entries of a quarantined WAL file so the WAL purge can continue; keep the quarantined file, it holds those entries and may hold flush proof for other WAL files
```

New metric: `arc_wal_quarantine_released_entries_total`. Keep `.wal.failed`
files until you have dealt with them: they hold the entries that could not be
replayed. Renaming one back to `.wal` now duplicates more readily than before:
the purge may already have deleted the later WAL file that proved its other
entries were stored.

### Two WAL rotations in the same clock tick no longer share a file

WAL files are named by the time they are created. Two rotations within one
clock tick (one microsecond on macOS) produced the same name, and the second
appended to the first file instead of starting a new one. A rotation now
refuses to open an existing file and moves the name one microsecond later, so
names still sort in rotation order.

### A forwarded file registration must name the forwarding node as its origin ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

A cluster node that is not the Raft leader forwards its file registrations to
the leader. The leader checked that the forwarding node was a known member
allowed to register files, but not that each file named that node as its
origin. Any authenticated member could register a file as if another node had
written it. Peers pull a file from its origin, and the upcoming standby-writer
takeover decides which writer's files to accept by origin.

The leader now refuses a forwarded registration whose origin is not the
forwarding node. Every node in this release stamps its own id, so nothing
legitimate is refused.

### Every file a cluster writer flushes reaches the cluster manifest ([#1206](https://github.com/Basekick-Labs/arc/issues/1206), [#1209](https://github.com/Basekick-Labs/arc/issues/1209))

Readers and the other writers learn about a writer's Parquet files from the
cluster manifest, and a file that never reaches it can be queried on its writer
only. Several paths lost files that way, with nothing to re-register them:

- **Startup.** The manifest registrar was attached only after the cluster
  coordinator had started. Files flushed before that, above all by startup WAL
  recovery after a crash, which then deletes the WAL files, were never
  announced. MQTT ingest, which starts first, was affected too. With auth
  enabled the window includes waiting up to 30 s for a leader.
- **Steady state.** The registrar held a 4096-entry in-memory queue, dropped
  files when it was full, gave up on a file after one failed attempt (for
  example while the cluster had no quorum), and kept nothing on disk. A crash
  between a flush and its registration also lost the file, because the flush
  checkpoints the WAL right after registering.

With `reconciliation.enabled`, the orphan-storage sweep could then delete such
a file after the grace window, and on a writer it was the only copy.

A node that ingests (a `writer`, or `standalone` with `cluster.enabled`) now
records every file it flushes in `pending-registrations.jsonl` in
`cluster.raft_data_dir`. With the WAL enabled, each flush makes those records
durable before it checkpoints the WAL, with one fsync per flush; without the WAL
there is no checkpoint to order and no fsync is added. The node registers the files once
the cluster is up, in batches, retrying every failure until the manifest
confirms them. A file that is gone or has changed size by then is skipped. The
record is removed once the manifest confirms the file, and the journal is
emptied whenever nothing is pending, so it stays near zero bytes in normal
operation.

While the manifest cannot be reached, each flushed file adds about 300 bytes to
the journal on the Raft data volume and about 500 bytes of memory, so a
million-file backlog is about 0.3 GB on disk and 0.5 GB in memory. New metrics:
`arc_cluster_file_registrations_pending`,
`arc_cluster_file_registration_oldest_pending_seconds`,
`arc_cluster_file_registrations_confirmed_total` and
`arc_cluster_file_registration_journal_errors_total`. Once the oldest pending
file has waited a minute, the node logs at Error every five minutes. Do not
delete the journal by hand while it holds records. Records in it are tied to
`cluster.cluster_name`, and a node started under a different cluster name drops
them with a warning. Readers and compactors do not keep one: nothing they flush
should be announced.

Files still waiting for registration are also protected from the paths that
treat "not in the manifest" as safe to act on
([#1210](https://github.com/Basekick-Labs/arc/issues/1210),
[#1212](https://github.com/Basekick-Labs/arc/issues/1212)):

- A row DELETE that rewrites such a file announces the rewrite in its place,
  and one that removes every row of it withdraws it. Before, the rows that
  survived a DELETE could stay on that writer only.
- Compaction leaves such files out of a cycle until they are registered, so it
  cannot race their registration and leave a manifest entry for a file it is
  about to delete. A partition left below the tier's minimum file count waits
  for the next cycle.
- The reconciliation orphan-storage sweep skips them, and files confirmed in
  the last minute, and reports how many in the run's `skipped_pending`. Before,
  a backlog older than the grace window was deleted as orphans.

These checks see only the node's own pending files, so they protect a writer
from its own DELETEs, compaction and reconciliation. With shared storage they do
not cover another node acting on a writer's pending files: a DELETE run by the
leader writer, or compaction and reconciliation on the active compactor
([#1214](https://github.com/Basekick-Labs/arc/issues/1214)). Files already left
unregistered by earlier versions are not picked up retroactively.

### Delete API removes the inactive SQL Server prefix scan ([#1077](https://github.com/Basekick-Labs/arc/issues/1077))

The WHERE validator no longer lists or checks `xp_` and `sp_`. The lowercase
patterns were compared against an uppercase clause and never rejected a
request. Removing this inactive SQL Server-specific check preserves existing
validation behavior; keyword, punctuation and file-access checks remain.

Original fix proposed by [@abhicodes-007](https://github.com/abhicodes-007) in [#1078](https://github.com/Basekick-Labs/arc/pull/1078).

### Cold-tier metadata is rebuilt on standalone nodes ([#1179](https://github.com/Basekick-Labs/arc/issues/1179))

A node with a usable cold tier now reads the cold listing at the start of every
tier scan, whatever its cluster role. Previously that step was skipped unless a
cluster gate was wired, which meant it never ran on a standalone node.

In steady state this changes nothing: a standalone node writes its own cold rows
as the second phase of its own migration, so the listing finds nothing new. The
case it exists for is **recovery**, where it is the only mechanism — nothing else
re-derives a cold row from the bucket:

- a metadata restore that predates the migrations, so the tier rows and the
  bucket disagree;
- SQLite loss or corruption, where the cold objects are intact but no row claims
  them — and the query layer omits the cold glob entirely for a measurement with
  no cold row, so the data reads as absent rather than mis-tiered;
- moving a cold bucket to a new standalone node, which until now needed
  hand-written SQL.

The listing is bounded by `tiered_storage.scan_timeout`, the same budget the hot
scan uses.

**If the cold listing fails, the migration cycle now skips migration and orphan
reconciliation** and waits for a cycle whose listing succeeds, rather than acting
on a view of the tiers it knows is incomplete. That already applied on clustered
nodes; it now applies everywhere. See the upgrade note above, because it is
reachable on deployments that have been running happily without it.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1183](https://github.com/Basekick-Labs/arc/pull/1183).

### Local `StatFile` ignores staged partial writes ([#1178](https://github.com/Basekick-Labs/arc/issues/1178))

`LocalBackend.StatFile` now reports only committed files. In-progress writes
remain available through `StagingInspector`, so resumable transfers can inspect
partial data without callers mistaking it for a complete object.

`StatFile` is documented as returning `-1` for an object that does not exist,
and S3 and Azure always did. The local backend returned the length of the
`.part` staging file an interrupted transfer leaves behind, so a half-arrived
file read as a complete one — on the backend single-node installs and tests
actually use.

It reads as an internal contract fix, but four things a local-storage operator
can see change with it, all in the safe direction:

- a replicated file still only partly arrived no longer gets a tier row
  claiming the node holds it;
- a tier row for a file that is only a partial is now retired by the next
  scan instead of being kept indefinitely;
- `DROP DATABASE` no longer reports a spurious "hot object remains after
  database listing" on its first attempt when a partial is still on disk;
- a backup counts such a path as absent — and says so in its incomplete
  sample — instead of recording a partial's size and then failing to copy it.

One asymmetry to know about, because it is the thing most easily got wrong
from the sentence above: **`ReadToAt` still serves staged data** when no
committed file exists. The resume path depends on that. So a caller must not
pair a committed-only `StatFile` size with a `ReadToAt` read and assume both
saw the same bytes; the interface comments now say so, and making `ReadToAt`
honest too is tracked separately.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1190](https://github.com/Basekick-Labs/arc/pull/1190).

### Reported core counts account for the CPU quota ([#1039](https://github.com/Basekick-Labs/arc/issues/1039))

`runtime.NumCPU()` reflects a cpuset but not a CFS quota, and Kubernetes
`limits.cpu` and `docker --cpus` are quotas — so a pod limited to 2 CPUs on a
64-core node reported **64** both to the license activation server and to fleet
telemetry. Neither number told anyone the node could only use two.

Both reports now carry the usable count as well as the machine count:

- **License activation** sends a new `usable_cores` field beside the existing
  `cores`. `cores` keeps reporting the machine's logical CPUs, so the admin
  console's "Cores" column means what it always meant.
- **Telemetry** sends a new `usable_cores` field beside `physical_cores` and
  `logical_cores`, which also keep reporting the machine. No existing field
  changes, so no fleet series steps down on upgrade.

Three things worth knowing, because they are the questions this invites:

**Machine-bound licenses keep verifying.** The machine fingerprint still reads
`runtime.NumCPU()`, deliberately and separately: that value is part of what a
bound license is checked against, so changing it would invalidate every bound
license in the field. It is unchanged, and a test now fails if anyone sweeps it.

**Neither number is an entitlement input.** The activation server accepts an
activation whatever the core count and leaves the limit to the client, which
enforces it at startup across GOMAXPROCS, DuckDB threads and flush workers. The
reported values are for reporting.

**The reported usable count is taken at process start.** A license enforces
itself by pinning GOMAXPROCS down to its core limit, so a value read later
would be `min(quota, licensed_cores)` — on a 64-core machine with a 32-core
license, a re-activation or a telemetry tick would have reported 32 as the
node's size, on hardware with no CPU quota at all. Taking the reading before
anything can pin it keeps one process's reported size stable for its whole life
and keeps the field's subject the CPU quota rather than the license.

One consequence of that choice, stated rather than left to be discovered: a CPU
limit changed **in place** without a restart — an in-place pod resize, `docker
update --cpus` — is not picked up. The Go runtime notices within about a second,
but the reported value does not, so it stays at whatever the limit was at boot
until the process restarts. That is the accepted side of the trade: reading live
instead would be wrong on every licensed node, permanently, rather than on a
resized one until its next restart.

Cluster join requests are unchanged. The core count there is an enforcement
input checked against the license's limit, is covered by the join MAC, and is
read after the limit is applied by design.


### Cancel queries when the client disconnects ([#979](https://github.com/Basekick-Labs/arc/issues/979))

Queries now stop when Arc detects that the client connection has closed before
the response completes. This follows Go's `net/http` request-context semantics:
a client that half-closes its write side after sending a request may be treated
as disconnected even if it still intends to read the response. On upgrade, this
can make a query that previously completed appear as cancelled with
`client disconnected` in query history. Set
`query.cancel_on_client_disconnect = false` to disable cancellation for clients
that use that half-close pattern. If a pipelined request is already pending,
Arc leaves its bytes for the HTTP parser and stops monitoring that connection
for the query, so a later disconnect on the same connection is not observed.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#982](https://github.com/Basekick-Labs/arc/pull/982).

### Iceberg orphan sweep re-checks the table before deleting ([#1125](https://github.com/Basekick-Labs/arc/issues/1125))

The orphan metadata sweep (#835) decided which manifest files no metadata
version references from one listing, then deleted them. A reference that
appeared in between was missed: a commit by another writer, a restore, or a
retained `v<N>.metadata.json` rewritten in place. The sweep could then delete a
manifest that had just become reachable. Arc's own reconciler does not race
itself here, so the window was open only to another process or a restore
writing the same warehouse.

The sweep now reloads the catalog and re-lists the metadata directory after the
first listing. It works out reachability from that final listing, checks the
catalog once more, and only then deletes. It skips the pass, deleting nothing,
if the catalog moved, a check fails, or the listing has no metadata files.
Files that appear after the first listing are never deleted in that pass. A
commit landing during the deletes themselves is still not covered.

Contributed by [@jallegri](https://github.com/jallegri) in [#1174](https://github.com/Basekick-Labs/arc/pull/1174), after the guard in [@efegokdemir](https://github.com/efegokdemir)'s [#923](https://github.com/Basekick-Labs/arc/pull/923).

### Compaction reports how many jobs are in flight ([#1168](https://github.com/Basekick-Labs/arc/issues/1168))

Two endpoints advertised a count of in-flight compaction work and neither ever
produced one. `GET /api/v1/compaction/status` emitted
`"manager": {"active_jobs": null}`, and `GET /api/v1/compaction/jobs` answered
`"active_jobs": 0` **unconditionally** — including in the middle of a cycle
with work running — because it read a key the manager never set and fell back
to zero when the read failed. Nothing tracked the number: compaction jobs run
in a subprocess, and the count was never added when that design landed.

It is tracked now. An attempt registers as in flight once it holds its
partition lock and deregisters when it returns, so all three surfaces report a
real number: `/status` (`manager.active_jobs`), `/jobs` (`active_jobs`), and
`/stats`, which gains the same `active_jobs` key.

An attempt stays counted until it returns, which is *after* the compaction
subprocess has exited and the parent has finished marking receipts, deleting
the manifest entry and invalidating caches. For that short tail the attempt is
counted in both `active_jobs` and `total_completed`, so on a busy node those
two will not always look disjoint.

**The unit is attempts, not batches.** This is the same unit as the records in
`/api/v1/compaction/history` and as `total_jobs_completed` / `total_jobs_failed`
beside it, and `/jobs` now says so explicitly with `"unit": "attempts"`. A
batch too large to compact in one pass is split by the adaptive splitter and
retried at successively smaller sizes, so one batch can account for several
attempts in sequence. If you are reconciling this against a cycle's
`discovered_batches` or `failed_batches` from `/api/v1/compaction/cycles/{id}`,
those are batches and will not match attempt counts.

`/jobs` still returns an empty `"jobs"` array. Only the count is tracked, not
the identity of each in-flight attempt; the array is there for compatibility
and listing which partitions are being compacted remains future work.

A partition skipped because another attempt already holds its lock does no work
and does not register as in flight, matching how it is already excluded from
the `total_jobs_*` totals.

If you use `arcli compaction status`, the `active` column stops showing `-` and
starts showing the count, with no client upgrade required.

### Compaction requests are audited per method instead of all as triggers ([#1168](https://github.com/Basekick-Labs/arc/issues/1168))

With audit logging enabled, every request under `/api/v1/compaction` was
recorded with the action `compaction.triggered` regardless of method — so an
auditor filtering for who triggered a compaction also got everyone who read the
status page, the candidate list or the cycle history. The sibling prefixes
already did this correctly (`mqtt.put`, `tiering.put`).

`POST /api/v1/compaction/trigger` keeps the action `compaction.triggered`, so
existing filters and alerts on the one action that mutates anything are
unaffected. The seven read routes now carry their own method:
`compaction.get`, and `compaction.head` for the HEAD requests that Fiber
registers alongside every GET.

**Part of this is visible without changing any audit setting.**
`audit_log.include_reads` (default `false`) suppresses GET requests but does
not suppress HEAD, and Arc registers HEAD alongside every GET route — so a
`HEAD` against a compaction read route was already being recorded as
`compaction.triggered` and will now be recorded as `compaction.head`. That
path needs a caller who clears the admin check (the compaction routes are
admin-only, and a rejected request is already audited as `auth.failed`
instead), so in practice it means an admin-token monitoring probe, or a
deployment running with `auth.enabled = false`.

That HEAD requests bypass `include_reads` at all is a separate gap, left
as-is here: it applies to every audited prefix, not just compaction, so
closing it belongs in its own change. If a HEAD probe polls a compaction route
on your deployment, expect one `compaction.head` row per poll at the default
setting, exactly as you were getting one `compaction.triggered` row before.

With `include_reads = true` the seven GET routes change from
`compaction.triggered` to `compaction.get`. If you have a dashboard or alert
that counts `compaction.triggered` events, it was over-counting and the number
will drop.

A `POST` to a compaction *read* route — which Arc answers with 405 — was also
recorded as `compaction.triggered`, and is now `compaction.post`. Only
`POST /api/v1/compaction/trigger` itself carries the trigger action.

The prefix is also matched at a path-segment boundary now, so a request to an
unrelated path that merely begins with the same characters — `/api/v1/compactionfoo`
— is no longer recorded as a compaction action. The `mqtt` and `tiering`
prefixes still match mid-segment; that is unchanged here.

Audit logging remains an enterprise feature: it is mounted only when
`audit_log.enabled` is true and the license includes `audit_logging`, so
nothing in this fix is reachable on OSS.

### The compaction trigger now claims its cycle before it answers

`POST /api/v1/compaction/trigger` decided whether a cycle was already running
by reading a flag, then left the actual claim — the compare-and-swap that makes
a cycle exclusive — to the background goroutine it started afterwards. Two
things followed.

**Concurrent triggers were accepted when only one could run.** The window
between the read and the claim is not narrow: the goroutine is scheduled
lazily, so a handler routinely finished its read *and sent its response* before
any goroutine claimed. Measured against 32 simultaneous triggers on the old
code, a substantial fraction were accepted — between 3 and 16, varying by
machine and scheduler. Each loser's cycle never ran, and while the server did
log the loss, the caller had been told the trigger succeeded and was handed a
cycle id belonging to the winner.

**The reported cycle id was a guess.** The response carried "current id plus
one", computed in the handler, while the id was really assigned later inside
the cycle. With two drivers, both were told the same id and only one cycle
existed, so the loser matched that id against `last_cycle` in
`/api/v1/compaction/stats` and read the winner's counters as its own. Since
`last_cycle` is a single global record, matching on `cycle_id` is the only way
an operator can tell whose result they are reading — and it was exactly the
mechanism that was unsound.

The handler now takes the claim synchronously and reports the id that claim
assigned. Taking the claim and publishing its id are one step, so a refusal can
also name the running cycle without ever reporting a previous, finished one.

**Behaviour change:** a second concurrent trigger now gets **409** immediately,
where it previously got a 200 whose cycle was silently dropped. If you drive
compaction from more than one place — one process per database, for example —
you will start seeing those 409s. They are the honest answer to what was
already happening; the cycle was never going to run. The 409 carries
`cycle_id`, `is_running`, and is now logged, so a refused trigger leaves a
trace on the server.

Scheduled compaction behaves exactly as before: the cron path and the internal
trigger claim and release the cycle in one call, with the same refusal logging
and the same return values on a lost claim.

### One configurable budget for the tier scan, and a truncated scan you can see ([#1154](https://github.com/Basekick-Labs/arc/issues/1154))

The tier scan runs on four paths, and until now they carried two different
budgets. A scheduled migration cycle gave the whole cycle 2 hours. The scan at
startup and `POST /api/v1/tiering/scan` were each capped at **30 minutes**, and
neither was configurable. The short cap was on both paths an operator reaches
for, so a warning that the startup scan did not finish pointed at the scheduled
cycle while the manual endpoint carried the same cap that had just failed.

`tiered_storage.scan_timeout` now bounds the scan on every path, defaulting to
`2h` so all four agree. It is a Go duration; zero, negative and malformed values
are refused at startup.

Inside a migration cycle the scan runs on a budget **derived from** the cycle's,
so it gets whichever is shorter. At the default the two are equal — the cycle
budget is also 2 hours — so the derivation changes nothing there. It matters once
the key is set *below* 2 hours: the scan then gives up on its own budget and
migration, the step that actually moves files, keeps the remainder of the cycle.
Setting it above 2 hours is harmless, because the cycle still bounds the scan.
The cycle budget itself is unchanged.

**Why the scan finishing matters.** The scan is the only thing that writes a
tier row for a compacted file on the node that produced it: the ingest flush
path and the replication workers are the other writers, and neither sees
compaction output. A node whose data is largely compacted therefore depends on
the scan entirely, and a truncated one costs partition pruning — a measurement
whose remaining rows are all cold drops this node's local files from the read.

**Setting the key below the time a full scan takes is harmful, not
conservative.** Hot-row retirement runs only after the whole walk, so a scan
that always truncates never retires a row for a file that has left hot storage.

**A truncated scan is now reported instead of being logged once and forgotten:**

- `ScanResult` carries `truncated`, set on every path that can run out of
  budget — the cold-tier sync, the hot walk, and the retirement pass, which
  returns no error of its own and so used to be reported as a complete scan.
- The partial counts come back rather than being discarded. `POST
  /api/v1/tiering/scan` answered a bare `500` with `context deadline exceeded`
  and nothing else; it now answers **503** with the counts and `truncated`, and
  distinguishes a scan the node cancelled while shutting down from one that ran
  out of budget. Any other failure keeps its `500`.
- `GET /api/v1/tiering/status` reports the last scan under `last_scan`. This is
  the only way a truncated **startup** scan is observable after the fact: it has
  no HTTP response, and a health check cannot ask the scan endpoint without
  starting a scan.
- A truncated scan now invalidates the query caches for what it did reach. The
  rows it wrote were real, but the query path kept serving its pre-scan answer.

**A second concurrent scan is refused with `409`.** The endpoint has no handler
deadline and a client disconnecting does not cancel the scan, so an operator who
gave up on a slow one and retried would start another full scan over the same
storage root, against the same SQLite handle, each invalidating the tier cache
the query path reads. A scan inside a migration cycle is deliberately not
refused — the cycle must always scan, or it would migrate from stale rows.

Fixed alongside: `scanTiers` never copied `HotRowsWritten` out of the walk, so
every consumer read zero. The cache invalidations keyed on it never fired,
including the one a standalone node needs on its first boot, when the scan
writes every hot row it has.


### The manual compaction trigger now requires the compactor lease

`POST /api/v1/compaction/trigger` ran a full compaction cycle on whatever node
received it. It checked whether a cycle was already running *on that process*
and nothing else — not the node role, not the compactor lease. Only the
scheduled cycles ever consulted the lease, and `compaction.enabled` defaults to
true, so every node in a cluster built a compaction manager and served a live,
unguarded trigger endpoint. A reader, a standby writer or a second compactor
would all accept one. Readers and non-lease writers are now refused; a second
compactor-role node still is not, unless compactor failover is enabled — see
the last bullet below.

That is unsafe because the partition lock is per-process: a trigger sent to a
non-lease node could select the same partition as the lease holder and delete
the same input files. With `cluster.replication_enabled` on it is worse and
needs no second node at all. The completion-manifest watcher — the component
that registers a compacted output and the deletion of its sources in the Raft
manifest — starts only on a node that may compact, but the job's own source
deletion is unconditional. So one trigger on a non-lease node deleted the
source files from storage while nothing recorded either the new output or the
deletions: phantom manifest entries pointing at files that are gone, and an
orphan output no manifest knows about. Nothing re-registers either, and an
enabled reconciliation sweep deletes the orphan and strips the phantoms.

The trigger now consults the same gate the schedulers use, re-read on every
request so a lease hand-over or a demotion takes effect without a restart. A
node that may not compact answers **503** with `can_compact: false`, its
`role`, and `lease_holder` (empty when the cluster manages no lease). 503
rather than 409 because this endpoint already answers 409 for a paused cluster
and for a cycle already in progress, and arcli reports any 409 here as "a
compaction cycle is already running (cycle N)" — a role rejection has no cycle
id, so it would have been reported as cycle 0. The remedy is carried in the
`error` field rather than only in `message`, because arcli renders `error` and
discards `message`.

The refusal distinguishes the two reasons a node may not compact, because they
have different remedies. If a lease exists and another node holds it, the
message names the holder and points at
`POST /api/v1/cluster/compactor/assign`. If the cluster manages no lease at
all — the default, since `cluster.failover_enabled` is false — the refusal came
from the node role, and the message says so and does *not* recommend the assign
endpoint, which answers "this cluster does not manage a compactor lease" in
exactly that state.

Read-only compaction routes are unchanged: they report the node's own state,
which is accurate everywhere.

Three consequences worth knowing before you upgrade:

- **A cluster-configured node whose license check fails now refuses manual
  triggers** on writer and reader roles. Such a node logs "running in
  standalone mode" but keeps its configured role, and its scheduler was already
  gated, so nothing scheduled ever ran there — the manual path was the only one
  left, and it is now closed. The node still shares storage with its licensed
  peers, which is why it is gated rather than exempted. Fix the license, or set
  `cluster.enabled=false` to run it as a genuine standalone node.
- **If the lease sits on a node that cannot be recovered, compaction stops
  cluster-wide and the manual trigger is no longer a way around it.** Move the
  lease with `POST /api/v1/cluster/compactor/assign`; the 503 body says so.
  That endpoint accepts compactor and writer targets.
- **A licensed cluster with no compactor-role node now has no manual
  compaction path.** With explicit `writer` and `reader` roles and
  `cluster.failover_enabled` at its default `false`, no node has the static
  capability and no lease exists to hold. Scheduled compaction never ran on
  those nodes either — the scheduler was already gated — so this configuration
  was already not compacting, but the manual trigger used to work and now
  returns 503. Give one node `cluster.role=compactor`, or set
  `cluster.failover_enabled=true` so the lease can be assigned to a writer.
- **A cluster whose nodes never set `cluster.role` is still permissive.** Every
  node is then `role=standalone`, which cannot hold the compactor lease, so the
  gate falls back to the static role capability — and that is true for
  standalone. Every node still accepts a trigger, exactly as every node already
  runs its own scheduled cycles in that configuration. That is a separate
  pre-existing problem, tracked as #892, and it is not changed here. Set
  explicit roles on a cluster.
- **Two compactor-role nodes with failover off both still accept a trigger.**
  Both have the static capability and no lease is ever assigned, so neither can
  be told it is the standby. Enable `cluster.failover_enabled` so exactly one
  holds the lease, or run a single compactor-role node — the same constraint
  Iceberg export already documents.
### Iceberg export serves a dotted edge-sync spoke, and migrates tables already published under one ([#1129](https://github.com/Basekick-Labs/arc/issues/1129))

An edge-sync spoke ID may contain a dot (`validateSpokeID` rejects `/`, `\`, `:`
and `..`, but not `.`), and Arc used to fold the spoke/database separator into
one, publishing a single Iceberg namespace component such as
`arc_rocket.01.telemetry`. iceberg-go v0.7.0 addresses a namespace whose
component contains a dot by a JSON-encoded catalog key rather than the plain
dotted string, so those tables stopped being found — and the v0.7.0 bump
refused such a database rather than publish one nothing could read.

A spoke pseudo-database `{spoke}/{db}` now keeps its boundary as **two**
namespace components, `{arc_rocket.01, telemetry}`, and Arc handles the encoded
catalog key end to end: the warehouse walk recognises the encoded directory, so
the exporter no longer reads its own metadata back in as a user database, and
the discovery files are published at the decoded path that directory-based
readers actually open.

**A spoke ID without a dot is unaffected and needs no migration.** Its catalog
key is byte-identical to the one Arc wrote before this change —
`rocket01/telemetry` is `arc_rocket01.telemetry` either way — so those tables
keep their identity and their history.

**A spoke ID with a dot needs its catalog row rekeyed, and that is previewed
before it runs.** `iceberg.namespace_migration_dry_run` defaults to **true**:
each reconcile pass reports the migration it would perform and changes nothing.
While a table is awaiting migration that measurement is **not exported** — the
legacy table stays readable and no second table is created beside it — and the
log names each affected database and measurement every pass, counted as
`awaiting_namespace_migration` in the pass summary. Set
`iceberg.namespace_migration_dry_run = false` to apply it.

**The migration rewrites the catalog row and moves nothing on disk.** That is
deliberate. Iceberg records manifest and data locations as absolute paths inside
the table metadata, and relocating a table does not rewrite them, so a migration
that moved the warehouse directory would leave the table's current snapshot
still reading its manifest chain out of the old directory. Rekeying leaves one
directory, not two: the table keeps the location it already has, later commits
write new metadata versions beside the existing ones, and nothing has to be
cleaned up afterwards. One consequence worth knowing if you browse the
warehouse: a migrated table's directory keeps its original
`arc_<spoke>.<db>.db` name, which no longer matches its catalog key. Tables
created after the upgrade use the encoded name.

The rekey is a single transaction, so it is idempotent and safe to interrupt,
and it refuses rather than guesses if the row changed underneath it.

### A year-shaped storage prefix no longer makes tiered queries return zero rows ([#1108](https://github.com/Basekick-Labs/arc/issues/1108))

If `storage.s3_prefix` or `storage.azure_prefix` had three or more segments and
the last one looked like a partition year, such as `tenants/eu/2026/`, queries
against a tiered deployment could return **zero rows with HTTP 200 and no
error**. Tier routing works out which database and measurement a storage path
belongs to by scanning backwards for a year-shaped segment; it found the one in
your prefix and read the two segments in front of it as the database and
measurement. Nothing was ever written there, the tier-metadata lookup came back
empty, and the read fell back to a glob under those wrong names.

The prefix is now removed before that scan. If you ran a tiered deployment with
a prefix of this shape, query results were silently incomplete and are correct
after upgrading; nothing on disk was affected, so no backfill is needed. Arc
used to warn about such a prefix at startup and no longer does, because it is
no longer a hazard — there is no need to change a prefix you already run.

Deployments without tiering were unaffected: the misread names reached only the
stable-schema anchor lookup, which simply found nothing, and the query still
read the correct location. Partition pruning was never affected either — it
parses the glob from the end and always kept the prefix intact.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1156](https://github.com/Basekick-Labs/arc/pull/1156).

### A restore no longer resurrects a hot copy of a file that has since migrated ([#1139](https://github.com/Basekick-Labs/arc/issues/1139))

A file that was in the hot tier when a backup was taken, and that tiering
migrated to cold afterwards, was restored like any other hot file: the bytes
went back to hot storage and the file was registered in the cluster manifest,
while the guarded tier upsert refused to move the row off cold and said nothing
about it. The row and the objects then disagreed, and the query path reads both
tiers for a measurement that claims both — so the restored rows came back
**twice** if anything else in that measurement was still hot. They came back
**not at all** when the measurement had gone entirely cold and the cold object
was missing or unreadable — the restored copy then sat on disk with nothing
routing a reader to it.

The local tier row now decides where a restored file belongs, because it is
this node's current truth and a restore's job is to make the bytes at that
location correct rather than to re-decide tiering. For a file the backup holds
as hot whose row says cold:

- a cold object whose size matches the backup's record is already the restored
  copy, so the write is skipped — counted as `cold_files_skipped_already_cold`;
- a cold object that is missing, differs in size, or is only a half-finished
  staging file is overwritten **in cold storage**, where the row says it is;
- a node with no cold backend configured has nowhere to put it, so it is
  restored hot and its row forced hot, exactly as before — counted as
  `cold_files_restored_to_hot`, not as a route to cold;
- a backup carrying no sidecar row for the file leaves the cold copy **exactly
  as it is**, counted as `cold_rows_skipped_unverifiable` and named in the log.
  There is nothing to check replacement bytes against, and overwriting the only
  copy with unverifiable bytes is worse than not restoring it. Those files are
  not restored.

The first two cases are counted as `hot_backup_files_routed_to_cold`, and a
routed write is verified against the sidecar's size and SHA-256 before it
replaces the cold object — on a standalone node as well as in a cluster. A
damaged backup therefore fails the restore with `sidecar_mismatch_sample`
instead of replacing the only copy of the file.

Where a cold backend exists, no hot copy is created for a file whose row says
cold, so this restore does not create a row/object disagreement — which means
the tiering orphan sweep, which deletes a hot copy once it confirms a cold one,
cannot act on a file this restore has just written. It does not clean up
duplicates an earlier restore already produced; those age out of the sweep's
window on their own.

Size is the discriminator because a tier row stores no checksum of the backup
object. On its own that is weak, but migration is a byte-preserving move of the
same path and the writers that produce new content emit new filenames, so a
cold object at that path with a matching size is that file. The check looks at
the final object specifically: on local disk a stat alone reports the size of a
`.part` staging file, which would let a half-transferred object pass as a
complete one.

Separately, a replication report refused by the cold-row guard now logs the
path, the tier that refused it and whether the row is quarantined, instead of
being dropped silently. The read behind that log cannot fail the event it is
describing.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1145](https://github.com/Basekick-Labs/arc/pull/1145), carried forward with the routing change.

### Database deletion cleans up tiered storage ([#1091](https://github.com/Basekick-Labs/arc/issues/1091))

`DELETE /api/v1/databases/:name` removed the database's hot files and nothing
else. Its `tier_files` rows survived, and so did its cold-tier objects and
their storage cost — indefinitely, because the cold sync records what is in the
cold store and never reverts a row. A database re-created under the same name
inherited them, and because the query path routes a measurement to cold when a
cold row exists, those stale rows could point reads at the previous
incarnation's objects.

Deleting a database now also removes its cold-tier objects and retires its tier
rows. In a cluster the database's manifest entries are removed before any hot
object is deleted, in batches, and a manifest failure aborts the deletion
before anything is removed from storage (with tiered storage disabled, see
[#1094](https://github.com/Basekick-Labs/arc/issues/1094) below). The response breaks the count out as
`hot_files_deleted` and `cold_files_deleted` rather than one total, because the
cold half is the irreversible part.

Four things worth knowing before you upgrade:

- **Tier-metadata cleanup is node-local.** The node serving the request retires
  the rows in its own metadata store. Other nodes keep their own rows for that
  database until the deletion is repeated there, and nothing retires a peer's
  cold row automatically. On a cluster, issue the DELETE on each node, or
  expect the database to stay listed on the others.
- **A database with only cold data is now deletable.** `databaseExists`
  consults tier rows, so a database whose hot files are already gone can be
  deleted instead of answering 404 — which is how the stale rows above get
  cleaned up. The same check makes `POST /api/v1/databases` answer **409** for
  a name that still carries tier rows; delete it first, then create.
- **A partial failure keeps the database.** If any file, cold object or tier
  row cannot be removed, the marker, the field-schema anchors and the Iceberg
  catalog entry are all left in place and the response is a 500 listing what
  failed. Re-running the DELETE retries the whole operation. The database is
  partially deleted in that state, not intact — queries over it return
  whatever survived.
- **With `tiered_storage.cold.enabled` off**, a database holding cold rows
  cannot be finished: there is no backend to delete its objects through or to
  verify them against, so the rows are retained and the response says so once,
  naming the key. Re-enable cold storage and repeat the deletion.

A deletion also reserves the tier lifecycle while it runs, so a tier scan or
migration cannot sweep a tier the deletion has already passed. A delete that
arrives while a scan or cycle holds that reservation answers 409 naming which
one; on a cluster node that may not mutate the tiered store it answers 503 and
names the primary writer to route to.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1166](https://github.com/Basekick-Labs/arc/pull/1166).

### Database deletion on a cluster without tiered storage reaches every node ([#1094](https://github.com/Basekick-Labs/arc/issues/1094))

The cluster half of the #1091 fix above runs through the tiered-storage
manager. That manager exists only when `tiered_storage.enabled` is on, so a
cluster without tiered storage kept the old behaviour: `DELETE
/api/v1/databases/:name` deleted the database's files on the node that served
it and nowhere else. Two things followed:

- **Other nodes kept their copies.** Readers and the compactor kept their
  replicas and kept answering queries for the dropped database.
- **The manifest still listed the files.** A backup reported them as
  manifest-only, and the re-pull path could copy them back onto the primary.

The handler now takes the same steps on such a cluster:

- **Primary only:** a node that is not the primary writer answers **503**,
  naming its role. This applies on clusters with shared storage or
  replication, the same rule as with tiered storage enabled. Without either,
  nodes share nothing, so each node may still delete its own files.
- **Manifest first:** the database's manifest entries are removed in batches
  of 500 before any local file is deleted, so every node's delete worker
  unlinks its copy.
- **Abort on failure:** a failed batch aborts with a 500 before the local
  sweep. Retry the request; batches already applied stay applied.
- **Edge-sync spokes untouched:** on a hub, a spoke's database of the same
  name is not touched, since its files live under `{spoke}/{db}/`.
- **Database visible from the manifest:** a database the manifest still lists
  counts as existing on a node that holds none of its files locally. So
  `DELETE` there no longer answers 404, `GET /api/v1/databases/:name` finds
  it, and `POST` answers 409 for that name.

**With tiered storage enabled**, nothing changes: the tiered-storage manager
already performs these steps. **Standalone**, nothing changes.

Two limits, unchanged by this fix:
- **In-memory unlink queue:** a peer's delete worker queues the unlink in
  memory. If that node restarts or loses the primary before the unlink runs,
  its copy stays on disk with no manifest entry.
- **Leader change mid-delete:** fixed by #1236 (below): a leader change is
  retried for about 7 seconds, as on the tiered-storage path.

Contributed by [@jallegri](https://github.com/jallegri) in [#1176](https://github.com/Basekick-Labs/arc/pull/1176).

### Crash smokes verify acknowledged records after WAL replay

The enterprise-shared crash scenarios restart the killed writer and wait for
`/ready`, then check the host IDs from every successful write batch. WAL replay
duplicates and records persisted by failed requests are allowed, but cannot mask
missing acknowledged records. Crash runs reject an idle kill target or a crash
that never triggered. The base scenario keeps its exact row-count assertion and
now exits successfully when it passes.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1004](https://github.com/Basekick-Labs/arc/pull/1004).

### Cluster compaction recovery records outputs before deleting sources ([#1155](https://github.com/Basekick-Labs/arc/issues/1155))

When recovery finds an uploaded compacted file, it now writes the parent-side completion record before removing its inputs, then records the source deletions for Raft. Previously the inputs could go while Raft never learned about the output, leaving a compacted file that no manifest entry described — and on a cluster with `reconciliation.enabled`, an orphan the sweep would eventually delete.

How a failure to write that record is handled depends on whether it can ever clear. A transient failure retains the inputs and the storage manifest, and the next cycle retries. A failure that cannot clear — the stored output disagrees with what the manifest records, or the completion record on disk belongs to a different job — **parks** the manifest under `.quarantined` instead, because retaining it would make recovery re-read and re-hash the whole compacted output on every cycle, for ever. A stored output that is simply shorter than the manifest records is treated as the partial upload it is: the output and the manifest are both discarded so the next cycle redoes the job, which is what Arc already did when the backend could be asked for object sizes directly.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1157](https://github.com/Basekick-Labs/arc/pull/1157).

### Partial DELETE rewrites remain immutable without duplicating data ([#975](https://github.com/Basekick-Labs/arc/issues/975))

Partial DELETE now publishes the surviving rows under a fresh immutable Parquet path and retires the superseded object in every storage mode, including standalone OSS deployments. The rewrite filename is normalized before compaction, tiering and file-time pruning classify it. This closes the same-size/different-content replication class for DELETE rewrites, without leaving deleted rows visible through Arc's glob-based query path. Producers that still reuse a path (restores, imports, edge-sync after the hub deleted it) are covered by the #798 entry.

**Two DELETEs that touch the same file no longer merge.** Before this release they did, because the
rewrite kept the file at its original path, so the second request read what the first had written.
With immutable rewrites the rewrite phase is serialized, and a request whose source was retired
while it waited fails with `source file was retired by a concurrent delete, retry the request`
rather than acting on a stale file list. Retrying it succeeds — the retry rebuilds the affected-file
list and sees the new path. Nothing is lost and no deleted rows come back, but a client that fires
overlapping DELETEs (or times out and retries one) will now see that error where it previously saw
success, and should retry rather than treat it as a failure.

If the old-object delete fails after the manifest commit or standalone publish, Arc reports the storage failure explicitly because the old object can remain query-visible until cleanup. A crash in that window can likewise leave both objects; operators should treat the corresponding error as a signal to inspect the partition before retrying.

### Compaction subprocess threads respect license and effective-core limits ([#1036](https://github.com/Basekick-Labs/arc/issues/1036))

Each compaction subprocess is now capped at the lower of the license's
`MaxCores` and the effective cores available to Arc, after automatic thread
defaults have been resolved. Lower configured values are preserved. This is a
per-process cap, not an aggregate reservation: the main process and multiple
subprocesses can still request more threads in total than `MaxCores`. Capping a
previously higher setting can reduce compaction throughput.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1043](https://github.com/Basekick-Labs/arc/pull/1043).

### Raising compaction.max_concurrent no longer multiplies the subprocess thread count ([#1037](https://github.com/Basekick-Labs/arc/issues/1037))

When `compaction.threads` is left at its automatic default, Arc divided the CPUs
this process may use by two, regardless of how many compaction jobs could run at
once. The halving hardcoded the default `compaction.max_concurrent = 2`, so an
operator who raised concurrency got that many subprocesses each still claiming
half the cores: at `max_concurrent = 4` on 16 CPUs, four jobs at 8 threads
each. The memory budget already tracked the setting — `compaction.memory_limit`
divides by `max_concurrent` — so the two derivations disagreed about the same
knob.

The automatic default is now those CPUs divided by `max(2,
compaction.max_concurrent)`, minimum 1. "CPUs this process may use" is
`min(runtime.NumCPU(), runtime.GOMAXPROCS(0))`, which reflects a quota, a
cpuset, or an explicitly set `GOMAXPROCS` without distinguishing which one
imposed the limit.

**At the default `max_concurrent = 2` nothing changes** — the divisor is still
2, so no existing deployment loses compaction throughput on upgrade. Above the
default it tightens: `max_concurrent = 4` on 16 CPUs now gives 4 threads per
subprocess rather than 8. That is a deliberate throughput trade in the
configuration that was oversubscribing, and it is not backed by a workload
benchmark at raised concurrency — what is measured is that thread count trades
against the per-subprocess memory budget rather than being uniformly better or
worse (see #1030 in the 26.09.3 notes for the numbers). An explicit
`compaction.threads` value is untouched by the automatic default, though the
licence cap above still applies to it.

The divisor covers compaction subprocesses only. The main process competes for
the same CPUs and is not accounted for here, so these remain caps on
parallelism, not reservations.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1041](https://github.com/Basekick-Labs/arc/pull/1041).

### Iceberg snapshot history no longer advertises files Arc has deleted ([#1092](https://github.com/Basekick-Labs/arc/issues/1092))

**Arc does not support Iceberg time travel across compaction, retention or a
partial DELETE, and the metadata now says so instead of failing on the read.**

Every Arc mechanism that removes a data file — compaction (scheduled hourly by
default), retention, and both branches of the delete API — removes it from
storage and from the current Iceberg snapshot. Older snapshots kept naming the
deleted path, so an external engine time-travelling to one got:

```
IO Error: Cannot open file ".../cpu_20261008_155058_567682000.parquet": No such file or directory
```

That is indistinguishable from object storage having lost the object, which is
the wrong thing for an operator to go and investigate. A reconcile pass that
removes any path now expires every older snapshot, so the same query returns a
routine `Could not find snapshot with id ...` — a normal outcome of any
retention policy, which Iceberg consumers already handle by re-reading the
current snapshot.

**This loses no readable history.** A removed path is referenced by every
snapshot from the one that added it onward, so each of those was already
unreadable. An append-only pass removes nothing and still keeps
`iceberg.retain_snapshots` snapshots (default 10), because that history does
work.

Why Arc cannot do real copy-on-write here: Iceberg keeps superseded data files
alive until the snapshots referencing them expire, but Arc reads a partition
with a glob, so any superseded file left in place is double-counted by every
ordinary query — the bug [#1073](https://github.com/Basekick-Labs/arc/pull/1073)
fixed, where a 3-row table returned 11. The two requirements are in direct
conflict, and honest metadata is the half Arc can deliver.

**Two limitations worth knowing:**

- The fix applies to the entry point readers use, `version-hint.text`. Arc also
  keeps `retain_snapshots + 1` older `v<N>.metadata.json` copies for
  directory-based readers, and those still list the pre-floor snapshots. A
  consumer that pins one of them directly still gets the `IO Error` until the
  copy is pruned, roughly `retain_snapshots` passes later.
- History already broken on an existing deployment is cleaned on that table's
  next pass that removes something. A table whose file set has gone quiet is
  skipped by the reconciler, so its stale metadata stays until it changes again.

A pass that drops history logs at `warn` with the before and after snapshot
counts, so an operator who notices one-deep history finds the reason rather
than inferring it. A failed expiry now also leaves the measurement uncached, so
the next pass retries it; previously only a failed version-hint write did that.

### Database API storage calls now have a deadline ([#1065](https://github.com/Basekick-Labs/arc/issues/1065))

Database API handlers now bound storage calls with a request context. Database
deletion gets a deadline scaled to the number of listed files, so a large
database is not cut off by the same fixed limit as a small one; partial-delete
errors continue to be collected and reported, including failure to delete the
database marker. Database details return an error if measurement listing fails,
rather than reporting a successful response with a zero measurement count.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1067](https://github.com/Basekick-Labs/arc/pull/1067).
### Object-store prefixes count toward the storage key limit ([#1121](https://github.com/Basekick-Labs/arc/issues/1121))

S3 and Azure now validate the complete object name, including its configured
prefix, before sending it to the object store. Listings and prefix probes use
the same check; previously stored over-limit names are omitted from ordinary
listings and reported by `ListUnusable`. Azure batch deletion applies the same
check and continues deleting the other valid keys in the batch.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1132](https://github.com/Basekick-Labs/arc/pull/1132).

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

### A fresh object store no longer makes the database and table listings fail ([#945](https://github.com/Basekick-Labs/arc/issues/945))

Most S3-compatible stores create the bucket on the first authenticated write,
so a deployment read before its first write had no bucket yet and every
listing failed: `GET /api/v1/databases`, `GET /api/v1/databases/{db}/measurements`,
`SHOW DATABASES` and `SHOW TABLES` all answered 500 with `NoSuchBucket` until
data arrived. Those four now answer an empty result instead.

The five listing operations on both object-store backends — listing keys,
listing directories, listing objects, enumerating unusable objects, and
checking whether a prefix holds any object — report a bucket or container that
does not exist as a distinct error rather than as an error message, which is
what lets those four read paths recognise it. Only the structured
`NoSuchBucket` and `ContainerNotFound` responses are recognised; transport
failures, authorization failures and every other storage error stay visible.
A read of a specific object from a missing bucket or container is reported the
same way since [#1238](https://github.com/Basekick-Labs/arc/issues/1238) (see
that entry).

Everything else that lists storage still treats a store it cannot find as a
failure, deliberately. Query planning drops a whole tier from a read when a
listing proves the tier holds no files for the time range, and retention,
backup, reconciliation and the Iceberg export all decide what to delete or
publish from a listing. For those, a bucket name with a typo in it answering
"empty" would not be a slow query or a missing log line — it would be a
silently wrong answer, or a deletion. The two states produce the same listing
and only the error tells them apart, so the error is what they get.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#950](https://github.com/Basekick-Labs/arc/pull/950).

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

### Backup listings report unreadable manifests instead of hiding backups ([#1123](https://github.com/Basekick-Labs/arc/issues/1123))

When a backup ID was listed but its manifest could not be read or parsed, Arc
silently skipped it. The listing now reports that destination as incomplete;
when every configured destination is affected, it returns an error instead of
an empty successful result. Readable backups on other destinations remain
available in a partial listing.

An incomplete destination contributes none of its backups, including the ones
whose manifests read fine, because a listing that silently leaves out a backup
is the problem this fixes. With a single destination, one manifest that cannot
be parsed therefore makes `GET /api/v1/backup` answer 503 until it is repaired
or removed. Retrying does not help in that case; the server log names the
manifest (`Failed to parse manifest; target listing is incomplete`), and the
other backups can still be fetched and restored by id.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1138](https://github.com/Basekick-Labs/arc/pull/1138).

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

### Behavior change for clustered deployments: backup deletion requires the primary writer ([#1134](https://github.com/Basekick-Labs/arc/issues/1134))

`DELETE /api/v1/backup/:id` now uses the same primary-writer gate as backup
creation and restore. Previously, a request reaching a reader, standby writer,
or compactor could delete a backup across that node's configured targets.
Non-primary nodes now return **503** with `route to the primary writer` before
accessing backup storage. Direct backup deletion requests, including those from
`arcli backup delete`, to the primary writer rather than a service address that
can send them to other nodes.

Eligibility is checked on every request, so promotion and demotion take effect
without a restart. A deletion already admitted is not cancelled on demotion.
Standalone and OSS deployments without a cluster coordinator keep their
existing behavior.

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

### Keep replayed WAL files until recovery flushes succeed

WAL recovery now waits for queued and direct flushes before deleting replayed
files, and retains them if any flush fails during the recovery pass, including
an asynchronous flush that finishes before the deletion barrier begins. A later
pass can retry after storage recovers. Periodic cleanup uses the unflushed
sequence floor instead of file age so a slow flush cannot lose its durable copy
(#1009, #966).

An incomplete final WAL header or payload after an interrupted append no longer
counts as a poison entry: complete preceding entries still require a successful
flush barrier before reclamation. Complete entries with invalid checksums remain
recovery failures. Checkpoints in quarantined WAL files remain part of recovery's
durability proof, so a later retry does not replay already-flushed entries merely
because their checkpoint file was renamed.

Replication followers prepare buffer ownership before appending local WAL.
Ignored payloads no longer leave an unowned identity that prevents reclamation.
An entry containing multiple measurements gets a separate local WAL payload and
identity for each buffer, preserving its database envelope. Flushing one
measurement cannot checkpoint another measurement's pending rows; rejecting a
later measurement does not release an earlier buffer's identity. Normal
single-measurement payloads retain their original bytes. Replication sequence
and acknowledgement semantics are unchanged.

Recovery barriers now use the configured flush worker pool instead of serial
per-measurement writes, and release shard locks before waiting. Each barrier
has a total wait budget of `ingest.flush_timeout_seconds` (30 seconds by default)
or the caller's earlier deadline. The first observed flush failure stops further
admission. Unsubmitted records remain buffered; queued/in-flight writes retain
their normal per-write deadlines and WAL identities. A timeout or failed
barrier retains the WAL for a later recovery pass, including when storage ignores
cancellation. Concurrent ingestion can continue while recovery waits.

Completed failed replay attempts now survive process restarts in synced
per-file `.wal.recovery` sidecars, so repeated restarts cannot reset the poison
file quarantine threshold. Cancellation and flush-barrier outages do not add
strikes. Unreadable or unwritable attempt metadata reports an error and retains
the WAL; quarantine still preserves its original bytes and valid checkpoints.
The operational guide describes sidecar recovery and cleanup behavior.

Recovery orders ordinary WAL files by their timestamped filenames rather than
filesystem modification times. Altered or tied mtimes can no longer make it
reclaim a later checkpoint file before an earlier retained data file, which
could otherwise cause already-flushed records to be replayed on retry.

Recovery skips redundant flush barriers for batches covered entirely by durable
checkpoints. Mixed batches still fence all replayed entries, and parent
checkpoint failures still retain files with fully covered row ranges.

Forced maintenance rotation now waits for WAL queue capacity instead of
returning `ErrWALDropped` and skipping recovery under sustained queue pressure.
It preserves FIFO ordering and uses a cancellable wait bounded by
`ingest.flush_timeout_seconds`. An admitted command may finish after timeout;
the failed wait keeps recovery pending. Normal append admission is unchanged.

Tracked row recovery now honors `wal.recovery_batch_size`. Durable row-range
checkpoints let a restart skip the precise persisted portion even after a
batch-size change; the original entry is checkpointed only after its complete
flush barrier succeeds. This bounds callback batches, not whole-file decoding
or total recovery memory. Legacy content-hash entries remain at-least-once.
Before downgrading to a binary without row-range support, drain recovery with
this version: older binaries can replay partially completed entries in full.
See the [restart compatibility guide](docs/operations/wal-recovery.md#bounded-row-replay-and-restart-compatibility).

Retained and quarantined WAL files are not reclaimed by age. The
[WAL operations guide](docs/operations/wal-recovery.md#reclaiming-retained-and-quarantined-files)
now documents offline inventory, verified backup, record reconciliation and
the checkpoint-dependency gate required before an operator reclaims quarantine
space. It includes a read-only empty-WAL check and a whole-directory relocation
procedure for capacity shortages. A clean recovery result alone does not prove
quarantined data is disposable.

Contributed by [@jallegri](https://github.com/jallegri) in [#1118](https://github.com/Basekick-Labs/arc/pull/1118).

### WAL recovery follow-ups: honest shutdown attribution, an exit for a bad checkpoint, and a directory that reclaims itself

Seven findings left open when #1118 merged. None of them lost data; several had
no operator exit.

**Two metrics shipped in #1118 without being announced.**
`arc_wal_dir_bytes` is the size of the WAL directory, sampled every 15 seconds,
and `arc_wal_quarantined_files_total` counts files moved aside after repeated
replay failures. The [WAL operations guide](docs/operations/wal-recovery.md)
carries alert expressions for both.

**`arc_wal_dir_bytes` now counts every file in the WAL directory**, not just
`*.wal` and `*.failed`. The names it skipped — `.wal.recovery` attempt
sidecars, `.wal-recovery-*` write temporaries, legacy `.wal.recovered` files —
are exactly what accumulates when recovery is failing, which is the state its
capacity alert exists for. **Expect a one-time step up on upgrade**, enough to
trip a growth alert once. Legacy `.wal.recovered` files are still not reclaimed
automatically; the guide says how to remove them.

**A clean shutdown after a kept WAL file no longer reports a flush failure that
did not happen.** A startup recovery pass that retained a file, or whose flush
barrier failed, armed the retry through the same flag the shutdown WAL purge
reads — so every later shutdown logged, at `Error`, "buffer flush did not
complete cleanly on shutdown … Investigate the buffer flush errors logged
above", pointing the operator at logs that do not exist. The WAL is still
retained, which was always correct; the message is now a `Warn` naming the
outstanding recovery pass. A genuine flush failure keeps the `Error`, and keeps
it when both are true.

**A row checkpoint describing rows beyond its entry no longer stalls recovery
permanently.** It abandoned the whole pass without recording a replay attempt,
so the quarantine threshold never advanced, the file was never moved aside, and
every later WAL file was left unread — with no way out. It is now treated as the
poison entry it is: the file is kept, struck, and quarantined on the third pass,
with its bytes and checkpoints preserved. Clean files already replayed in the
same batch are now reclaimed rather than retained alongside it.

**Recovery reclaims its own residue.** Every pass now removes
`.wal-recovery-*` temporaries left by a crash and attempt sidecars whose WAL
file is gone — every purge path deleted the `*.wal` and left the sidecar
behind, for the life of the node. A sidecar is kept when its WAL file merely
cannot be stat'ted, and when a quarantined sibling exists, because it is the
strike history those files are reconciled against.

**Recovery's parent checkpoints wait for WAL queue capacity** instead of
returning `ErrWALDropped`, the same trade the forced maintenance rotation took
in #1118 — same pass, same queue pressure, but finalization was left on the
dropping path. `ingest.flush_timeout_seconds` bounds **admission** to the queue;
a timed-out or refused admission retains the file for a later pass. Once a
checkpoint is admitted, recovery waits for it to be written: the entry is
written either way, and abandoning the wait would skip the pending-identity
release, pin the unflushed sequence floor, and stop the periodic purge from
reclaiming any WAL file for the life of the process. The ingest flush path is
unchanged and still admits without blocking.

**A repaired WAL file is more likely to start a fresh attempt series.** The
attempt sidecar now records a CRC of the file's final 64 KiB alongside its size
and timestamp. On a filesystem with one-second timestamp granularity — some
network mounts, which is where repairs happen — a file rewritten in place to the
same length within one tick looked untouched and inherited its strikes. A repair
confined to the middle of a large file still does; remove the file's
`.wal.recovery` sidecar to reset it explicitly. The sidecar format version is
unchanged, so an earlier binary still reads it.

**New: `arc_wal_partial_row_recovery_pending`.** `1` while the last recovery
pass retained a file for which at least one row range of a parent entry was
replayed — the state an older binary would replay in full — and `0` once a pass
ends with none. This makes the guide's "finish recovery before downgrading"
checkable instead of advisory. Read it with the quarantine counter and the
recovery error lines: a pass that cannot read a file at all reports `0` while
that file's residue remains.

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

A delete followed by a same-size re-registration of the same path is now
refreshed too. The re-registration arrives as a new file, not a change, so it
was not forced; and a reader's local delete waits a short grace period before
it unlinks, so when the path was listed again by then the delete kept the
local copy, which then passed the size check with the old bytes. The delete
now asks for a refresh of the path whenever it keeps a copy for that reason.
The copy is never unlinked there, because the node may hold the only one: it
stays readable until the new version is installed.

That refresh hashes the local copy first and downloads only when it is not the
listed version. A node that never held the old version pulls the new one
through the registration, and a re-registration with unchanged content needs
nothing, so neither downloads the file twice. The node logs it (the first
time, then at every power of two):

```
Local delete skipped: the manifest lists the path again; refreshing the local copy unless it already is the listed version
```

Three counters in `replication_catchup_status` on `GET /api/v1/cluster` (and
in the query gate's 503 body) show this working. They count every forced
refresh, including the content-change refreshes above, not only this path:
`forced_refresh` counts forced refreshes that downloaded the file,
`refresh_verified` counts refreshes whose local copy was already the listed
version, and `refresh_pending` is the number of failed or dropped forced
refreshes waiting for their path's next arrival.

If the refresh cannot be queued, the line is a warning, and the local copy
may be the previous version for a while. When the node is shutting down the
refresh is lost; when the pull queue is full it is retried on the path's next
arrival or the periodic reconciliation.

Since [#975](https://github.com/Basekick-Labs/arc/issues/975), Arc's DELETE
rewrites and compaction publish new files under fresh paths, so these shapes
come only from producers that reuse a path: restores, imports, edge-sync
after the hub deleted the path, and external Raft writers. Two gaps remain for them:

- A forced refresh that fails, or is dropped, is remembered in memory only. A
  restart forgets it, and the startup catch-up then accepts the old copy by its
  size and reports the node caught up. With
  `cluster.replication_catchup_enabled = false`, nothing retries it at all.
  [#1182](https://github.com/Basekick-Labs/arc/issues/1182) tracks this.
- A rewrite a node learns of from a Raft snapshot rather than from the log
  fires no callback. Since the fix for
  [#1071](https://github.com/Basekick-Labs/arc/issues/1071) (below), the
  node's next replication walk has a copy it recorded under the earlier
  registration re-checked and pulled again if its content differs; a copy
  from before the upgrade is not recorded, so it is still kept.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#907](https://github.com/Basekick-Labs/arc/pull/907), and by
[@jallegri](https://github.com/jallegri) in
[#1173](https://github.com/Basekick-Labs/arc/pull/1173) (delete then
re-register).

### Compaction reuses local inputs and validates DuckDB paths ([#969](https://github.com/Basekick-Labs/arc/issues/969))

For local storage, compaction reads Parquet inputs from their existing paths
instead of first copying them into the compaction temporary directory. This
removes the input-staging copy, but DuckDB still reads the inputs and compaction
still writes and uploads its output. Reads therefore remain on the data volume;
operators who place `compaction.temp_directory` on a separate volume should
account for that change.

The input-copy byte figure in the acceptance harness is calculated from input
file sizes, not measured storage I/O. Arc's parent `/metrics` endpoint does not
aggregate storage counters from compaction subprocesses, so those counters
cannot measure compaction reads or writes. The optional large-partition run
records cycle wall time and sampled peak temporary-directory usage; its 10 ms
sampling interval can miss short-lived peaks.

In a single same-settings comparison with 42 Parquet inputs, 1.05 million rows
and 9,130,407 input bytes, the baseline and this change completed in 0.905 s
and 0.894 s. Sampled temporary-directory peaks were 1,878,338 bytes and
356,610 bytes, respectively. The harness calculates 9,130,407 baseline input
copy bytes versus zero with local-path reuse; it does not measure total disk
I/O. This one run does not establish a throughput improvement.

An input that disappears before validation is skipped safely. If it disappears
after validation and before DuckDB reads it, the batch fails as a permanent
missing-input error without adaptive splitting or retries; surviving inputs
remain in storage for the next cycle to rediscover. A permanent failure is now
recognised in either the subprocess stderr or the parent error, rather than only
in stderr, so such a batch stops immediately instead of spending its full retry
budget.

Because DuckDB reads a `read_parquet()` path as a pattern, a path containing a
glob metacharacter could resolve to a different file than the one Arc named and
validated. Such a path now **fails the compaction job** rather than skipping the
file, and the two operator-set directories that reach `read_parquet` are
refused at startup instead — see the upgrade note at the top of these notes. A
skipped file would have failed identically on every later cycle, so compaction
would never have progressed while every metric showed jobs completing.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#990](https://github.com/Basekick-Labs/arc/pull/990).

#### The output side (maintainer follow-up, not part of #990)

Everything above is the input half of #969. The **output** side no longer copies
either. On a local backend a compacted file is moved into the storage root rather
than copied into it, so publishing the output costs a rename instead of a full
read plus a full write of the file.

Two conditions apply, and both are ordinary configuration rather than edge
cases:

- **The backend has to be local.** On S3 and Azure the upload is a network
  transfer and nothing changes.
- **`compaction.temp_directory` has to be on the same filesystem as
  `storage.local_path`.** They are independently configurable and pointing the
  temporary directory at a larger or faster volume is the reason the key exists.
  When they are on different filesystems the rename is impossible and Arc copies
  exactly as before, logging one warning per job that names both paths. If you
  see that warning and want the saving, co-locate the two paths.

Do **not** put `compaction.temp_directory` inside `storage.local_path` to
satisfy that condition. Everything under the storage root is treated as storage:
the job temporary directory is listed as a database by `SHOW DATABASES`, and a
backup of the instance sweeps it, so a half-written compaction output can end up
in a backup. An ordinary query is not affected -- it globs
`<database>/<measurement>/**/*.parquet`, which a job directory at another top
level does not match. Use a sibling directory on the same filesystem, which is
what the defaults (`./data/arc` and `./data/compaction`) already are.

In clustered deployments the output is still read once more, to compute the
SHA-256 the completion manifest publishes and peers verify their pulled bytes
against. That digest is now taken before the output is published rather than
after, because a moved file is no longer there to read. So on a cluster this
removes one of two passes over the output, not both; a single node writes no
completion manifest and skips the digest entirely.

`arc_storage_write_bytes_total` counts a moved file exactly as it counted a
copied one, so the counter is unchanged by this work and is not where the saving
shows up.

## Internal changes

These do not change how Arc behaves. They are here because the codebase is the
thing a new maintainer has to learn, and a refactor that moves a decision from
four places to one is worth knowing about before you go looking for it in the
old place.

### The unused `license.enabled` setting is removed ([#1195](https://github.com/Basekick-Labs/arc/issues/1195))

`license.enabled` (`ARC_LICENSE_ENABLED`) was read into the config and never
used. Enterprise is enabled by `license.file_path` or `license.key` alone, and
that does not change. A leftover `enabled` key or environment variable is
still ignored, and Arc starts as before.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#1241](https://github.com/Basekick-Labs/arc/pull/1241).

### `PurgeFlushed` and WAL recovery are now driven by the same test

No test in `internal/wal` ran the writer's sequence-floor purge and a recovery
pass together, and that interaction is where the row-range proof hazard lives: a
flush writes a row-range checkpoint into the current file while the entries it
describes sit in an earlier file recovery retained. A retained file from a
previous process is not in the writer's own file order, so the purge walk never
sees it and never stops at it — and the file carrying its only proof is an
ordinary candidate. Deleting that proof makes the next pass replay rows already
in storage.

The #1118 fix-up pinned the proof file; nothing pinned the pin.
`purge_recovery_interaction_test.go` now drives both, and fails if the
`foreignProof` guard is removed. A companion test takes the inverse direction —
full coverage replays nothing and reclaims both files — so the first cannot pass
for the wrong reason.

Two tests were also weaker than their names.
`TestDirectoryBytesIncludesWALAndQuarantinedFiles` pinned the suffix exclusion
for arbitrary non-WAL names but had no recovery-artifact fixture, which is the
exclusion that mattered; it now covers sidecars, write temporaries and legacy
`.recovered` files, and is named for what it asserts.
`TestRecoveryAttemptMetadataFailureKeepsWAL`'s `directory` case read as
unwritable-metadata coverage while only exercising the read path — `os.ReadFile`
rejects a directory before the write is reached — and is renamed accordingly.
Those cases also pin that a malformed sidecar still stalls a pass, which is
deliberate and now says so: the error names the file and removing the sidecar
clears it, where a bad checkpoint lives inside WAL bytes an operator cannot edit.

### A quarantine test waited on the counter, not the bookkeeping it asserts ([#1146](https://github.com/Basekick-Labs/arc/issues/1146))

`TestPullerQuarantineKeepsTheQueryGateClosed` reddened `Build & Test`
intermittently with `catchupInflight = 1, want 0`. Nothing was wrong with the
puller. `totalInvalidPath` is incremented inside `processEntryOnce`, while the
in-flight bookkeeping the test asserts is released by that function's deferred
`finishEntry` — so a wait predicate watching only the counter can return in
between, and the assertion reads the pre-drain state.

This is [#972](https://github.com/Basekick-Labs/arc/issues/972) a second time.
That issue diagnosed the identical mechanism in
`TestPullerQuarantineIsIdempotentAcrossReenqueues` and fixed it by adding
`inflight_count == 0` to that test's wait; the two sibling tests in the same
file that share the window were never updated. Both now gate on the
bookkeeping, and `TestPullerQuarantinesUnusableKeyWithoutTouchingPeers` needed
it too — its shared-metric assertion sat in the same gap, three statements wide.

The assertions keep their teeth: `waitStats` returns the last snapshot on
timeout rather than failing, so removing the real drain in `finishEntry` still
fails both tests on the full deadline. Verified, along with the mechanism, by
temporarily widening the window — a 300 ms sleep after the counter increment
fails both tests 10/10 before the change and 0/10 after.

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

### Immutable DELETE rewrite filenames are normalized centrally

Partial DELETEs use the `_rewrite_<unixnano>.parquet` naming convention. The suffix parser now lives in `internal/storage` and is shared by DELETE, compaction, tiering and file-time pruning, so rewritten daily and compacted files retain their logical classification and timestamps.

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

**A dotted database name is exported through v0.7.0's encoded catalog key.** Arc used to build one
Iceberg namespace component per database, `<prefix>_<database>`, folding an edge-sync spoke's
separator into a dot — and v0.7.0 addresses a namespace whose component contains a dot by a
different catalog key than the plain dotted string. The bump shipped two refusals so nothing was
published broken: a dotted `iceberg.namespace_prefix` at startup, and a dotted database per
measurement.

The per-database refusal is gone, because the cause is fixed — see the #1129 entry above for how
spoke namespaces are built now and how tables already published under a dotted one are migrated.
The `iceberg.namespace_prefix` refusal stays: a prefix is a free choice that would put the encoded
spelling on every database on the node, and a prefix without a dot costs nothing.

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

- **The compaction subprocess kept its own factory here.** It has since moved to
  the shared constructor too, receiving the parent's credentials in its job
  (#1104, under Bug fixes).
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
  store, and a row the backup does not write is not written by anything else
  in the same pass. (Before 27.01.1 that was permanent on a standalone node,
  or on a cluster without shared storage or replication, because the
  cold-metadata sync did not run there. It does now, so the next scan records
  it — but the flush still runs on every exit path, because waiting for a scan
  is not the same as having the row.)
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

### `storage.FileAdopter`, and what a compaction-manifest failure does not retry into

`storage.FileAdopter` is a new optional backend interface, in the shape of the
existing `BatchDeleter`: a backend whose objects really are local files can
implement `AdoptFile` and take ownership of a file the caller has already
written, instead of copying it through `WriteReader`. `LocalBackend` implements
it; `S3Backend` and `AzureBlobBackend` deliberately do not, and a test asserts
they never will, because the contract **consumes the caller's file** and a
backend that copies bytes cannot honour that.

`ErrAdoptUnsupported` means "this pair of paths cannot be adopted, copy
instead" and is not a failure. `AdoptFile` returns it for **any** rename the
filesystem refuses, not only a cross-device one, and also when the source
filesystem will not let Arc set the file mode. The reason is the same in both
cases: before this change the copy was unconditional, so a filesystem that
refuses either operation would go from "compacts, slowly" to "compaction fails
every cycle". Falling back is safe for any rename error, because a rename is
atomic — a failed one moved nothing, and the source the fallback reads is still
there. Only a caller error (a key that escapes the storage root, a source that
is not a regular file) is a hard failure, and both are rejected before the
rename.

The `chmod` is what keeps a moved object at mode `0600`, the same mode
`WriteReader` gives every other local object; a rename otherwise preserves
whatever mode DuckDB wrote the output under, which is umask-derived. It runs
before the rename, so a refused adopt can leave the source file at `0600` — the
contract on `ErrAdoptUnsupported` says so, since it promises only that nothing
was moved.

Unlike `WriteReader`, `AdoptFile` does not stage through a `.part` name. That
name exists so an interrupted *copy* leaves resumable bytes on disk, and a
rename has no partial state — so renaming straight to the final path is
strictly stronger: there is no window in which a reader can observe a short
object.

Separately, a comment in `internal/compaction/job.go` claimed that failing a job
on a completion-manifest write error means the next cycle retries and the
cluster eventually sees the output. It does not, and the comment now says so.
Recovery finds the output present with a matching size, deletes the inputs and
deletes the compaction manifest without ever writing a completion manifest, so
the Raft `RegisterFile` never fires and the compacted file ends up with no
manifest entry describing it. Only the comment changed here; the recovery gap
is tracked in [#1155](https://github.com/Basekick-Labs/arc/issues/1155).
