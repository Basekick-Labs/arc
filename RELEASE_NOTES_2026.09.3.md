# Arc v2026.09.3 Release Notes

> **Status:** Released 2026-10-05.

## Fixed: a cluster node's tier metadata now follows its own disk, not only what it ingested

On a cluster with peer file replication and a cold tier, a node's `tier_files`
rows only ever described files that node had written itself. Files the
replication puller pulled from a peer were never recorded, and files another
node migrated to cold were not marked cold locally until the receiving node's
next cold-tier sync. The scan that would have reconciled either runs from the
migration schedule — `0 2 * * *` by default — and from
`POST /api/v1/tiering/scan`; nothing ran it at startup.

Because the query layer routes reads from those rows, the gap had two effects
on any node that was not currently ingesting a measurement:

- **Partition pruning was lost.** With no row for the measurement, the read
  fell back to the unpruned `{database}/{measurement}/**/*.parquet` glob, so a
  windowed query scanned every partition instead of the hours it asked for.
- **Reads could silently omit local data.** The hot tier is included in a
  multi-tier read only when a row says this node has hot data. A measurement
  that had a cold row and no hot row — a node whose hot rows were retired after
  another node migrated the measurement, or one that joined and received its
  files by catch-up after its own scan — dropped its local files from the read
  entirely. A windowed `count(*)` over data sitting on that node's disk
  returned `0`, with `success: true` and no error; an unwindowed `count(*)`
  undercounted.

Three changes close it:

- The replication puller reports every file it pulls and keeps, and the
  local-delete workers report every copy they remove, to this node's tier
  metadata. Both reports are applied by a single background writer, so a
  catch-up burst cannot pile writers onto the SQLite handle that auth, audit,
  continuous queries and the ingest flush path share. The cold-tier existence
  checks a migration's unlinks need run in parallel ahead of the writes, so a
  nightly migration of thousands of files is recorded on every node within
  about a minute rather than being cut off at a deadline, and the query
  layer's caches are dropped only when the set of tiers a measurement reads
  from actually changes — not once per replicated file.
- The node decides what a removal means from evidence rather than from the
  manifest delete's reason, which an operator can supply: it checks whether
  the object is actually in cold before marking the row cold, and retires the
  hot row otherwise.
- Tier metadata is now scanned once at startup, in the background. This also
  covers files that arrived while a node was down, and files already on disk
  when upgrading to this release. On a shared-storage cluster the scan lists
  the shared hot bucket from every node at boot — the same listing the nightly
  cycle already performs, now also once per restart.
- A cold tier whose backend fails to construct — bad credentials, an
  unreachable profile — is now left genuinely absent rather than present and
  unusable. Previously the failed constructor's nil was stored behind the
  backend interface, so every "is there a cold tier" check passed and the
  first cold listing dereferenced it; the startup scan would have turned that
  into a crash at boot.

Two visible consequences. `GET /api/v1/tiering/files` and
`/api/v1/tiering/stats` counts now converge across nodes within seconds of a
file arriving instead of diverging until the next scan — a node that reported
`0` files for a measurement it holds will now report them. And the `tier`
column in `SHOW DATABASES` changes from `local` to `hot` for databases whose
files reach a node only by replication, because that column reflects the tier
rows.

New counters for the path: `tier_registered` alongside `pulled` in
`/api/v1/cluster`'s `replication_catchup_status` — it counts pulls a tier
recorder accepted, so it tracks `pulled` where tiering is enabled and stays at
zero where it is not — and `replication_events` in `/api/v1/tiering/status`,
whose `dropped` is zero on a healthy node: reports are queued without blocking
the pull or delete workers and the queue grows as a burst needs; it drops only
if the drainer stops making progress for long enough to reach its memory bound,
or at shutdown with work still queued, and either way the next tier scan
reconciles.

One behaviour note for the failure case: if the tiering manager cannot start —
an unparseable `migration_schedule`, say — queries still route across tiers and
files are still registered, but no migration or scheduled scan runs. The
startup log line now says so rather than reporting only the failure.

## Changed: `arc.toml` no longer ships MinIO connection values for the cold tier

The sample `arc.toml` — which is copied into the container image — set
`s3_endpoint = "localhost:9000"`, static `minioadmin` credentials,
`s3_use_ssl = false` and `s3_path_style = true` under
`[tiered_storage.cold]`, with a comment telling operators to leave the
endpoint empty for AWS. **That instruction could not be followed from the
environment.** Arc reads configuration through viper's `AutomaticEnv`, which
treats an empty environment variable as unset, so
`ARC_TIERED_STORAGE_COLD_S3_ENDPOINT=""` did not clear the file's value: a cold
tier intended for AWS S3 silently addressed `localhost:9000` instead. Every
cold listing then failed, which sets `ColdSyncFailed` and makes each migration
cycle skip.

Those keys, and `s3_bucket`, are now commented out in the sample file, so Arc's
built-in defaults apply — no endpoint, HTTPS, virtual-hosted addressing, and
credentials from the AWS chain, which is what IRSA and instance-role detection
need. The values remain in the file as a commented MinIO/dev block.

**If you enabled the cold tier and relied on the shipped MinIO values, uncomment
that block as a set.** Leaving only `s3_endpoint` set now means HTTPS and
virtual-hosted addressing against a MinIO endpoint. Deployments that configure
the cold tier through environment variables or Helm are unaffected. Note the
general rule this illustrates: an empty environment variable does not blank a
key that a configuration file sets — give the key the value you want, or remove
it from the file.

## Peer file fetches ignored cancellation during TCP/TLS connection setup ([#901](https://github.com/Basekick-Labs/arc/issues/901))

A follow-up to the fetch-timeout fix shipped in 26.09.2 ([#899](https://github.com/Basekick-Labs/arc/pull/899)): connection establishment itself was not context-aware. `FetchClient.Fetch` checked `ctx.Err()` and derived a bounded dial timeout, but dialed through `security.Dial`, which wraps `tls.DialWithDialer` for TLS. That performs the TLS handshake against a background context internally, so cancelling the caller's context could not interrupt a peer that accepted the TCP connection, received the ClientHello, and then never responded. The connection-close cancellation hook was also installed only after the dial succeeded, so it offered no protection during dialing. A cancelled fetch could hold its pull worker until the ten-second dial timeout the coordinator hardcodes for peer fetches elapsed, delaying `Puller.Stop`, which cancels and joins its workers.

`FetchClient` now dials through a new `security.DialContext`, which uses `net.Dialer.DialContext` for plain TCP and `tls.Dialer.DialContext` for TLS — the latter threads the context through to the handshake via `tls.Conn.HandshakeContext`, so a cancelled context now interrupts a stalled handshake instead of only being noticed after it. `security.Dial` is unchanged and still used by five other cluster-internal call sites. Three of them — the leave broadcast, the heartbeat send and the seed join — have no context in scope, so there is nothing for them to honour. Two do: the leader-forward dial and the WAL replication receiver's reconnect, where a stalled handshake still holds the caller for its dial timeout and defeats a surrounding shutdown select. Those are the same bug on different paths and are not fixed here.

Contributed by [@pujitha24](https://github.com/pujitha24) in [#902](https://github.com/Basekick-Labs/arc/pull/902).

## Fixed: the WAL purge no longer infers durability from a file's age ([#1009](https://github.com/Basekick-Labs/arc/issues/1009))

The periodic purge deleted rotated WAL files once their modification time passed a threshold of three times `ingest.max_buffer_age_ms`, taking that age as proof the data had reached Parquet. Whenever a flush took longer than the threshold — a slow or unavailable object store being the obvious way — the only remaining copy of acknowledged writes was deleted before it was durable. PR [#997](https://github.com/Basekick-Labs/arc/pull/997) measured 48,500 records lost this way.

Rotated files that carry tracked WAL entries are now reclaimed only below a floor: the lowest sequence this process has appended that no durable flush checkpoint covers. No clock is involved, so neither mtime granularity nor an NTP step can affect it. The floor is released when a flush checkpoints the entry; when a write is **abandoned after its WAL append** — a type-mismatched column, the schema-churn guard; and when the checkpoint itself fails to persist, since the data did reach storage and only the record of it is missing. Without those a single rejected write would hold the floor for the life of the process, and because the purge stops at the first file it must retain rather than skipping it, nothing after that file would be reclaimed either.

An identity a *replay* inherited is deliberately not released: it belongs to a WAL file whose keep-or-delete decision is recovery's, and the periodic recovery replays this process's own files.

Two classes of file have no flush state to reason about, and those are still reclaimed by age — which is the only signal that exists for them, and a far narrower role for the clock than before:

- **Files written by a previous process.** Their sequences belong to another numbering domain, so no floor of this process says anything about them. Recovery deletes the ones it replays, but deliberately keeps a file holding an entry it could not apply and one left by an unclean shutdown.
- **Files holding only untracked entries.** A replication follower writes every replicated entry untracked, because its durability comes from the primary and from peer Parquet replication rather than from its own WAL. On a reader node that is every file it writes, so without this a follower's WAL directory would grow until the disk filled.

A file carrying tracked sequences is never purged by age, whatever its age.

`ingest.max_buffer_age_ms` therefore no longer bounds how long a rotated WAL file holding tracked data is kept; the flush floor does. It still sets the age threshold (at three times its value) for the two unaccounted classes above — so **raising it raises WAL disk usage on reader nodes**, where every file falls into the untracked class.

One new figure to watch: `pending_unflushed` in the WAL stats is the floor's working set. It climbs while flushes fail and does not come back down until they succeed, so a steadily rising value is an object-store problem rather than a WAL one. It is also the memory this floor costs — one entry per unflushed write — which is reported rather than capped, as with the ingest buffers.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#1056](https://github.com/Basekick-Labs/arc/pull/1056).

## Compaction dedup metrics count Parquet rows correctly ([#1015](https://github.com/Basekick-Labs/arc/issues/1015))

Deduplication row counts now come from DuckDB's `parquet_file_metadata`, where
`num_rows` is available, so the before/after counts and dedup-ratio log can be
produced. A count failure is now reported at Warn level instead of being
silently discarded.

The count had been read from `parquet_metadata`, which has no `num_rows`
column, so the query returned a binder error on every compaction and the error
was discarded at both call sites. The row count was therefore always zero and
the log it gates — `Deduplication removed duplicate rows`, the only output Arc
produces for how many rows de-duplication discarded — **never fired on any
release up to and including 2026.09.2**. An operator who saw no such line was
reading an absent signal, not an absence of duplicates, and no historical dedup
volume can be recovered from the logs. That matters beyond the missing metric:
it is the line that would have surfaced the row loss described under
[#1005](https://github.com/Basekick-Labs/arc/issues/1005) in these notes, where
a `key=value` compaction temp directory collapsed the de-duplication key and
discarded rows of distinct series as duplicates.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1019](https://github.com/Basekick-Labs/arc/pull/1019).

## Fixed: edge-sync names reject DuckDB glob and Hive partition syntax ([#994](https://github.com/Basekick-Labs/arc/issues/994))

Spoke IDs and received sync-path segments containing DuckDB glob metacharacters
or `=` are now rejected. A spoke ID is the first path segment of everything that
spoke writes into the hub's storage root and the sync path supplies the rest, so
these are long-lived directory names: a glob metacharacter makes the path a
pattern rather than a name, and `=` is read as a Hive partition key, which can
replace a stored column's value with the one in the path.

Existing registrations are not modified. A spoke whose stored ID is no longer
accepted is reported at hub startup, by ID and reason, and its transfers are
refused from then on. There is no rename: re-registering mints a new secret and
needs the edge box reconfigured, and the data already under the old namespace
stays on disk where it is, unmigrated. Nothing is deleted.

This closes the admission side only, and only for new names: a `key=value`
directory already on disk is unaffected by this change. The read side is fixed
separately in [#1005](https://github.com/Basekick-Labs/arc/issues/1005), below.

## Fixed: a `key=value` directory could silently overwrite a column's values ([#1005](https://github.com/Basekick-Labs/arc/issues/1005))

DuckDB derives a column from any `key=value` directory component of a path it
reads. Where that name matched a column already in the file, the value from the
path replaced the stored value and its type, for every row, with no error. Arc
never asked for that inference and never used it — the storage layout is parsed
explicitly — but it did not switch it off, so any Arc deployment whose storage
root or compaction temp directory contained a `key=value` directory read
altered data.

Deletes were the worst of it, because the `WHERE` clause was evaluated against
the substituted value. A delete naming the directory's value matched every row
in the file and took the "all rows deleted" path, which removes the file
outright; a delete naming the value actually stored matched nothing, reported
success and removed nothing. Where a delete did rewrite a file, the rows it kept
were written back carrying the path's value instead of their own. Compaction
running through a `key=value` temp directory baked the substituted column into
its output, and if the inferred name matched a tag the de-duplication key
collapsed, so rows of distinct series were discarded as duplicates.

Every read Arc issues now disables the inference. Reads are built through a
single helper so the flag cannot be forgotten, and a test refuses a
`read_parquet` call written anywhere else — there is no DuckDB setting for this,
so the flag has to travel with each call, and nothing but that test keeps the
next one honest. On an unaffected deployment nothing changes: no part of Arc
consumed an inferred column. The arcx engine is deliberately untouched; it reads
Parquet itself, performs no such inference, and rejects the option.

**Checking whether a deployment was affected.** Two configured directories end
up inside a `read_parquet` path: `storage.local_path`, and
`compaction.temp_directory`, into which compaction downloads its input files
before reading them. (`database.temp_directory` is DuckDB's own spill
directory and never appears in a read, so it does not matter here.) The
`key=value` component can be anywhere in the path, including above the
configured directory, so test the whole path and not just the tree beneath it.
Set the two variables to the values as written in the config and run this from
the working directory Arc runs in:

```sh
for d in "$STORAGE_LOCAL_PATH" "$COMPACTION_TEMP_DIRECTORY"; do
  [ -n "$d" ] && [ -d "$d" ] || { echo "skipped (not a directory): ${d:-<unset>}"; continue; }
  case "$d" in /*) abs=$d ;; *) abs=$PWD/$d ;; esac
  case "$abs" in *=*) echo "ancestor: $abs" ;; esac
  find "$abs" -type d -name '*=*'
done
```

It prints nothing when the deployment is unaffected. Every `skipped` line is an
unchecked directory, not a clean one.

Symlinks are deliberately not resolved, because neither Arc nor DuckDB resolves
them: Arc makes the configured path absolute lexically, and DuckDB infers from
the string it is handed. A symlink whose target happens to sit under a
`key=value` directory is therefore not affected, and a `key=value` component in
the path as written is, whatever it resolves to. The one case this misses is
Arc's own working directory being reached through a symlink while a relative
path is configured; compare `pwd` with `pwd -P` if that applies.

On S3 and Azure the same test is against `storage.s3.bucket` plus its prefix,
or the Azure container name, and the keys underneath them. Arc's own keys are
`{database}/{measurement}/{year}/{month}/{day}/{hour}/`, so a `=` can only come
from the configured prefix or from a database or measurement name — which the
write path has rejected since [#992](https://github.com/Basekick-Labs/arc/issues/992).

**What a `key=value` path leaves behind, and what can be done about it.** The
inference only mattered where the derived name matched a column that was
already there; where it did not, it added a column Arc ignored.

- *Field-schema anchors* recorded under such a path hold the extra column. It is
  now always empty. A schema rebuild alone will not remove it — a rebuild merges
  and never drops a field — so delete the measurement's anchor object,
  `_schema/{database}/{measurement}.parquet` in the storage root, and only then
  `POST /api/v1/databases/{database}/measurements/{measurement}/schema/rebuild`.
  Note that backup and restore copy anchors, so restoring a backup taken
  beforehand brings the column back.
- *Files a delete rewrote, and files a delete removed* were altered or lost when
  it happened. Neither can be reconstructed from what is on disk.
- *Compacted output* written through a `key=value` compaction temp directory has
  the substituted column baked in, and where the derived name matched a tag,
  rows of distinct series were discarded as duplicates. There is no log line to
  look back for: the de-duplication ratio Arc emits after a compaction counted
  its input rows with a query that always errored, so the one signal that would
  have reported the loss never fired on any release up to and including
  2026.09.2. That count is fixed in this release
  ([#1015](https://github.com/Basekick-Labs/arc/issues/1015)), which makes the
  signal available from now on but recovers nothing retrospectively.
- *Continuous-query destinations* hold the aggregates that were computed from
  the substituted column. Re-running a continuous query appends rather than
  corrects, so the affected windows have to be removed first.

For everything in that list the recovery is a restore from a backup taken before
the affected operation ran — a backup taken after it contains the same altered
files. Backups and Iceberg exports perform none of these reads themselves, so
neither introduced the problem, but an Iceberg export over affected files
carries the substituted column in its schema.

One shape to expect after upgrading on an affected deployment: a query naming a
column that only ever existed because of the inference will now fail to bind
where it previously returned the path's value, unless the column was recorded in
a field-schema anchor, in which case it binds and returns empty.

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

## Backups name the files they skipped ([#977](https://github.com/Basekick-Labs/arc/issues/977))

A backup that skips a file, because it vanished between the listing and the
copy or because its backup destination key would exceed the storage key limit,
used to record only a count. The file's name appeared in the log alone, and
for the second cause the operator has to rename that file, so a count was not
enough to act on.

The manifest now names up to 32 of the skipped data and Iceberg metadata
files in `skipped_sample`, in copy order (a skipped compaction recovery
manifest or outside-root warehouse file is counted but named only in the
log), and says how many of the skips were for an overlong key in
`skipped_overlong_keys`. The backup listing (`GET /api/v1/backup`) carries
`skipped_files`, `skipped_metadata_files` and `unaddressable_files` from the
manifest, so an entry whose `total_files` counts files that were not stored
now says so. The status endpoint names a backup's skipped files the way it
already did for a restore, published once the copy phases finish and before
the skip-ratio check, so a run the ratio fails, which writes no manifest,
still leaves them there until the next operation. That failure's message now
gives each cause's count
(how many files could not be read, how many have source keys longer than 982
bytes) instead of naming both as possibilities. And a new gauge,
`arc_backup_skipped_files`, reports the most recent backup's skipped count
across every file group, set by every backup that finishes its copy phases
and cleared by the next clean one, so an incomplete backup can be alerted on.

Erratum: the 26.09.2 notes named the unaddressable-files gauge
`arc_storage_unaddressable_files_total`. Its name is
`arc_storage_unaddressable_files`, and it is a gauge; the 26.09.2 notes are
corrected.

## Security fixes

### RBAC read and write restrictions were unreachable, and the database listings had no authorization ([GHSA-mcqm-h7hj-99fg](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-mcqm-h7hj-99fg))

**Read the behaviour-change list below before upgrading a deployment that uses RBAC.**

Two authorization decisions never took effect.

On an RBAC denial, the permission check fell back to the token's coarse
permission list, which does no database or measurement filtering. At the same
time the route middleware consulted only that same coarse list and never RBAC.
So for an RBAC denial to stand, a token had to hold the coarse `read` bit to
pass the middleware *and* not hold it for the fallback to decline — which is
impossible. No read or write path in Arc could be narrowed by RBAC under any
configuration. That covers query permission checks, the per-measurement
endpoint, `SHOW DATABASES`, `SHOW TABLES FROM <db>`, the measurement listing
and the shared write check used by every ingest path.

Separately, `GET /api/v1/databases`, `/api/v1/databases/:name` and
`/api/v1/databases/:name/measurements` were registered with no middleware and
no permission check at all, while their mutating counterparts were admin-gated.
Any valid token, including a write-only ingest token, could enumerate every
database and measurement name, and could probe which databases exist from the
404-versus-200 distinction.

Enforcement is now decided by a token's **team memberships**, not by the
license: a coarse `admin` token is allowed as break-glass, a token with
memberships is governed by RBAC and a denial is final, and a token with no
memberships resolves to its coarse permissions exactly as before — the
"backward compatible with OSS tokens" guarantee the original RBAC work made.

Enforcement no longer consults the license anywhere, and that is deliberate.
A license counts as valid only while active or inside its grace period, and
the client is dropped entirely if validation fails at startup — so a lapsed
trial, a revoked key or a long outage would otherwise switch enforcement off
and silently widen every tenant token to full read at whatever hour it
expired. The license still gates RBAC *management*, so a lapse costs you the
ability to change grants, never the grants' effect. A failure to load a
token's grants now denies instead of falling through.

The middleware half ships with it: a resource-scoped middleware that consults
RBAC already existed and was wired to nothing. **The three database listing
routes** now use it, so a token created with no coarse permissions — the
documented way to ask for an RBAC-only token — can reach those handlers and be
scoped by its grants instead of being refused in front of them. The query and
ingest routes still require the coarse `read`/`write` bit, so such a token
cannot yet query or write; those routes are authoritative on scoping already
(`checkQueryPermissions`, `CheckWritePermissions`), and adopting the middleware
there is a follow-up.

**Grants are enforced per measurement, including for listings.** A role is
restricted to its measurement grants for every question asked of it. That was
previously true only when a specific measurement was named: an empty
measurement skipped the grant list and fell through to the role's
database-level permission, which made the empty string the most permissive
value in the system and reachable from any route that named no measurement.
Listing a database's contents is a genuinely different and weaker question —
"may this caller enumerate here" — and now has its own predicate rather than
borrowing an empty measurement, so it cannot be used to sidestep table-level
scoping.

Listings are therefore filtered rather than all-or-nothing: a caller granted
`db1.cpu` sees `["cpu"]` from `GET /api/v1/databases/db1/measurements`,
`SHOW TABLES FROM db1` and `GET /api/v1/measurements?database=db1` alike —
not the whole table list, and not a `403`. One batched permission check per
listing regardless of how many names it holds.

**A grant pattern with a leading wildcard now matches.** `*metrics` and
`*-metrics` were accepted at creation — the validator admits them and its
rejection message advertises them — but the matcher had no branch for a
leading `*` without an underscore, so they matched nothing. While an RBAC
denial fell back to coarse permissions that was invisible; with a denial now
final it would have been a silent, total lockout of the token it was meant to
authorize. Every pattern the validator accepts now has a matcher branch.

**Behaviour changes.** All three are intended, and all three are visible:

- **A token with team memberships is now restricted to its grants.** It
  previously was not restricted at all. Review your grants before upgrading.
- **A write-only or permissionless token can no longer list** databases or
  measurements.
- **A token scoped to specific databases now receives `403` from
  `GET /api/v1/databases`** and must name its database via
  `GET /api/v1/databases/<name>`, the same bar `SHOW DATABASES` applies.
  Client tooling that lists databases to populate a picker needs to handle
  that; updates to the CLI, console, MCP server and Python client ship
  alongside.
- **A grant pattern with a leading wildcard starts matching.** If you hold a
  `*suffix` pattern it previously matched nothing; it now matches as
  documented. Review any such pattern before upgrading.
- **Removing a token from a team no longer requires a license.** Every other
  RBAC mutation still does. Since enforcement is license-independent, a token
  whose grants deny more than intended would otherwise be unrecoverable on a
  lapsed license except by rotating its credential or escalating it to admin;
  removing a membership can only narrow RBAC's reach, so it is the escape
  hatch.
- **The compaction and retention read-only endpoints now require admin.**
  `GET /api/v1/compaction/{status,stats,candidates,jobs,history}` and
  `GET /api/v1/retention{,/:id,/:id/executions}` previously accepted any
  authenticated token. Compaction is cluster-wide operator work that cannot
  be configured per team or per database, and retention policies are
  configured by admins only — but `/candidates`, `/jobs`, `/history` and a
  policy row all name the databases and measurements they apply to, so the
  endpoints were a tenant-name enumeration surface for a token with no grant
  on either. Monitoring that polls them with a non-admin token needs an
  admin token.

### A subquery in the `where` parameter read a measurement the check never saw ([GHSA-qcf2-6hm7-62c5](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-qcf2-6hm7-62c5))

`GET /api/v1/query/:measurement` authorized only the database and measurement
named in its route and query string, then assembled a statement around a
user-supplied `where` fragment and handed the whole thing to the rewriter,
which resolves table references in any table position — including inside that
fragment. A reference to another database there was read without ever being
authorized, and since the endpoint returns rows and counts, the fragment also
worked as an oracle. No header and no license were required.

The fragment validator could not have caught it: it is a substring blocklist
for statement terminators, comments and DDL/DML keywords, not a parser.
Enumerating read syntaxes would not work either — DuckDB spells a table
reference in a scalar subquery three different ways, and `FROM` is legal
inside `EXTRACT`, `SUBSTRING` and `TRIM`.

The endpoint now authorizes every table reference in the assembled statement.
One detail is load-bearing: the database that *bare* references resolve to is
now supplied explicitly by the caller, because this endpoint takes its
database from `?database=` and gives the rewriter no header, so a bare name in
`where` resolves to `default` and must be checked as `default`. Passing the
`x-arc-database` header instead — the obvious implementation — would have
authorized one database while reading another, reintroducing the same class of
defect in the fix. A regression test pins it.


### Replacement-scan guard covered only `FROM` and `JOIN` ([GHSA-9rgq-j585-5fhq](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-9rgq-j585-5fhq))

Arc refuses a path in table position, because the permission check authorizes
measurements and a bare path is not one. That guard was applied only to the
`FROM` and `JOIN` keywords.

DuckDB introduces a relation after several other keywords, each of which
accepts a path there and reads the file. Arc's table-position scanner did not
arm on them, so those positions were never examined: the statement passed
validation, the permission check derived **no** table reference from it — and a
query with no references is authorized outright — and the rewriter passed the
statement through unchanged. A read-capable token could read any Parquet file
inside the DuckDB sandbox allowlist, which spans the whole local storage root,
with no grant, no `x-arc-database` header and no license required.

The sandbox itself held throughout: paths outside the allowlist were, and are,
refused by DuckDB.

The scanner now arms on those keywords as well, so a literal standing in any of
those positions meets the same guard that already covered `FROM`. The arming is
narrower than for `FROM`/`JOIN`: those mark a clause that can continue across a
comma and the new forms cannot, so arming that state for them would have made a
later comma a table position and refused legitimate statements. Every form of
those keywords that does not put a literal in relation position keeps working,
including the trailing `PIVOT`/`UNPIVOT` forms and `DESCRIBE`/`SUMMARIZE` over a
query.

This closes the keywords DuckDB has today without changing the shape of the
defence — a keyword the scanner does not know still fails open. Deriving the
relation set from a parse tree instead of from keyword scanning is tracked in
[#764](https://github.com/Basekick-Labs/arc/issues/764) and
[#491](https://github.com/Basekick-Labs/arc/issues/491); see also
[#991](https://github.com/Basekick-Labs/arc/issues/991).


### RBAC: three normalisation divergences let a query read a measurement the permission check never saw ([GHSA-h3rq-5r29-2wrh](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-h3rq-5r29-2wrh))

Arc decides twice which measurements a query touches. The RBAC extractor
normalises the SQL and builds the set to authorize; the query rewriter
normalises it again and decides which names become Parquet paths. The two must
agree, because a query whose extracted set is empty is authorized outright —
so any divergence that empties the set is a total bypass rather than a partial
one.

Three divergences are fixed:

- The CTE pattern has an alternative that matches a comma-separated `name AS (`
  clause with no `WITH` anchor. The extractor consulted that pattern on every
  query while the rewriters consulted it only when they found a `WITH` keyword,
  so one legal clause shape could make a real measurement look virtual to the
  permission check and real to the rewriter. The `WITH` predicate now lives
  inside the CTE extractor itself, so all five callers share one expression and
  the two sides cannot disagree about whether to consult it. This is the fix
  that matters: the defect was the asymmetry, not a keyword spelling.
- Three rewriter gates tested for `WITH` followed by a space, while the
  extractor matches it as a word followed by any whitespace — so a `WITH`
  clause broken across lines, ordinary formatting for a multi-line query, was
  read differently by the two sides. Those gates now match `WITH` as a word,
  via the same helper [#978](https://github.com/Basekick-Labs/arc/issues/978)
  introduced when it fixed the identical keyword-as-word mistake for `JOIN`. A
  measurement genuinely named `with_history` is still a measurement.
- The single-table fast paths located `FROM` without requiring a word boundary
  before the keyword, while the extractor requires one. They now require the
  same boundary. A name that merely ends in the keyword was never, and is still
  not, a table reference.

All three required RBAC to be enabled, a valid read token, and an
`x-arc-database` header. Without the header the no-header rewriter extracted
CTE names unconditionally and used only boundary-correct patterns, so it always
agreed with the extractor. No configuration key gates any of it. Deployments
with RBAC disabled have no per-measurement authorization to bypass.

A regression suite (`internal/api/rbac_normalisation_parity_test.go`) now
asserts the invariant directly — that the set the permission check extracts is
never smaller than the set the executed query reads — across all three
rewriters, and each fix was verified to be the sole thing keeping its own cases
passing. See the advisory for the affected shapes and the upgrade guidance.

Found internally while verifying that
[#827](https://github.com/Basekick-Labs/arc/issues/827) — a different
case-folding defect in the same seam — was already fixed in 26.09.2. All three
predate that fix and #978.

## Bug fixes

### Fallback compaction job IDs support spoke namespaces ([#750](https://github.com/Basekick-Labs/arc/issues/750))

When a compaction job was created without an explicit JobID, its fallback ID
included the raw database name. Edge-sync pseudo-databases contain a slash,
so the generated ID failed completion-manifest validation and could create an
unintended nested temporary directory. Fallback IDs now use the existing
database-name sanitiser while preserving the original database and any
caller-supplied JobID.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#912](https://github.com/Basekick-Labs/arc/pull/912).

### A node that rejoins through a Raft snapshot now removes the replicas the cluster deleted while it was away ([#962](https://github.com/Basekick-Labs/arc/issues/962))

On a per-node-storage cluster every node unlinks its local copy of a file when
the manifest drops it, through the FSM delete callback. That callback fired only
when a delete was applied from the Raft log. A node that fell far enough behind
to receive a snapshot instead rebuilt its manifest from the snapshot without
firing it, so it kept every replica it held before the outage, including the
files compaction, retention and tiering had deleted in the meantime. Its reads
glob the disk, so that node alone returned duplicate rows and rows that should
have been gone, silently, until the orphan sweep (opt-in) removed them.

Restoring a snapshot now diffs the manifest this node last held against the
restored one and hands every path that disappeared to the same delete workers,
with the reason `snapshot:removed`. Because that reason cannot say why the file
left, the unlink is recorded in this node's tier metadata the way an abandoned
pull is: the cold tier is asked whether the file moved there, so a measurement
migrated while the node was away reads from cold instead of vanishing from that
node's results. A process restart that loads its own snapshot into an empty
manifest has nothing to diff and fires nothing.

One gap remains after a restart: the diff is against the node's last local Raft
snapshot (`cluster.raft_snapshot_threshold`, default 10,000 entries), not its
disk. Files this node pulled after that snapshot and the cluster deleted while
it was down are still removed only by the orphan sweep
([#1071](https://github.com/Basekick-Labs/arc/issues/1071)).

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#983](https://github.com/Basekick-Labs/arc/pull/983).

### A compaction batch is now applied to the Raft manifest atomically ([#447](https://github.com/Basekick-Labs/arc/issues/447))

Compaction commits its result to the cluster manifest as one Raft entry that
registers the compacted output and deletes the source files it replaced. The
FSM applied that batch one operation at a time, taking and releasing the
manifest lock for each, so a reader listing the manifest between two operations
could see the output already registered while some sources were still listed:
duplicate rows for that query on every node that reads the manifest, in a window
of microseconds that opened on every compaction. The callbacks that drive
replication saw the same half-applied states.

The whole batch now runs under one write lock, and its callbacks are delivered
afterwards, in order, against the completed manifest. Single-operation
registers, updates and deletes behave exactly as before.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#910](https://github.com/Basekick-Labs/arc/pull/910).

### A delete-API rewrite after a writer failover now names the node that holds the new bytes ([#976](https://github.com/Basekick-Labs/arc/issues/976))

The delete API rewrites a Parquet file in place on the primary writer and
re-commits its manifest entry with the new size and checksum, copying every
other field from the existing entry, including the origin node. After a writer
failover the primary is not the node that first wrote the file, so the new bytes
existed only on the new primary while the manifest still named the old origin.
Every replica tried the old origin first and received the pre-rewrite bytes;
before the #999 fix above, that single checksum failure ended the pull and the
replica dropped its own copy, and since it the pull falls through to the next
peer at the cost of a wasted full transfer from the stale origin every time. The
old origin itself was never corrected: it saw an update to a file it had written,
skipped it as its own, and kept serving the deleted rows.

The rewritten entry now records the rewriting node as its origin. Replicas fetch
from the node that produced the bytes on the first try, and the old origin sees
a foreign-origin update and pulls the new file like any other replica, provided
the rewrite changed the file's size, since the puller's "already here" check
compares size only (#975). The `DeleteCoordinator` interface gained
`LocalNodeID()` so the compiler enforces the dependency rather than a runtime
type assertion.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#984](https://github.com/Basekick-Labs/arc/pull/984).

### Duplicate token-name races no longer increment the auth rejection counter ([#964](https://github.com/Basekick-Labs/arc/issues/964))

Concurrent bootstrap or admin creates for an existing token name are now treated as expected name conflicts rather than malformed or forged entries. Identical Raft replays remain no-ops, while non-identical same-name creates still return an `already exists` error without incrementing `arc_cluster_auth_rejected_total`.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#973](https://github.com/Basekick-Labs/arc/pull/973).

### Audit events no longer alias Fiber request buffers ([#837](https://github.com/Basekick-Labs/arc/issues/837))

The audit middleware builds each event from Fiber accessors (`c.Method()`,
`c.Path()`, `c.Get`, `c.Params`, `c.Query`) and hands it to a background
writer that serialises it up to a second later. Arc runs Fiber with
`Immutable=false`, so every one of those strings aliased a pooled request
buffer that the next request overwrites. An audit row could therefore record a
later request's method, path, database, measurement or user agent, or garbage.

The middleware now copies every request-derived string before enqueueing the
event. It also takes its own copy of handler-supplied detail, so a handler no
longer has to copy before setting it; the two existing producers already did.
A regression test mutates the borrowed buffers after the event is queued and
checks every field.

Contributed by [@0utsights](https://github.com/0utsights) in [#871](https://github.com/Basekick-Labs/arc/pull/871).

### Graceful shutdown registers final Parquet files in the cluster manifest ([#1014](https://github.com/Basekick-Labs/arc/issues/1014))

On a cluster node, the files written by the final Arrow buffer flush during a
graceful shutdown never reached the cluster manifest. The shutdown sequence
runs every hook before any component, and the cluster coordinator — which owns
Raft — was a hook, so Raft was gone before the buffer flushed; the registrar
that announces files then had nothing to apply to, and the files existed only
on that node's disk, invisible to peers and to replication.

Raft now outlives the final flush: the cluster coordinator and tiering stop as
shutdown components, after the Arrow buffer, the file registrar and WAL cleanup,
in that order. The registrar drains what the final flush queued as batched
Raft entries — sized to fit the peer protocol frame when a non-leader forwards
them, retried through a leader election — lets an apply in flight finish
instead of cancelling it, and reports at `Warn` with a count if any
registration was not confirmed. A manifest apply that fails at any time is now
a rate-limited `Warn` rather than `Debug`: nothing re-registers such a file,
and where the storage reconciliation sweep is enabled a file with no manifest
entry is an orphan-storage delete candidate once past its grace window. The
inbound replication receiver still stops before the buffers close, from its
own hook, and cannot be re-attached while the node shuts down. Tiering's
SQLite handle, where the node owns it, is no longer closed under the final
flush's tier registrations.

Queue-full warnings also no longer promise anti-entropy recovery that is not
implemented.

Contributed by [@efegokdemir](https://github.com/efegokdemir)
for [#1014](https://github.com/Basekick-Labs/arc/issues/1014).
### ListStaged reported partials its own DeleteStaged refused ([#772](https://github.com/Basekick-Labs/arc/issues/772))

`ListStaged` walked for `*.part` files and reported each stripped key, but
`DeleteStaged` resolves a key through the validation every backend applies
(`ValidateKey`), minus the reserved-suffix rule so a legacy `x.part` partial
stays reclaimable. A partial left behind by an older Arc version can belong to a key
that is illegal under today's contract, such as one containing a backslash, so
`ListStaged` reported it and `DeleteStaged` then refused it. Both consumers pipe
one into the other: the `DROP DATABASE` reclaim of abandoned partials and the
edge-sync staging sweep each logged a warning for that file on every run and
never reclaimed it.

`ListStaged` now applies the same validation before reporting an entry, so every
key it returns is one `DeleteStaged` accepts, and a test pins that round trip.
A partial with an illegal key is not lost from view: `ListUnusable` already
reported it and still does; it is simply no longer also reported as reclaimable.
`DROP DATABASE` now checks `ListUnusable` once it has removed everything it can
address, and warns with the count and up to ten paths of anything left under the
database, since those files keep the database visible and nothing else names them.
A committed object whose real name ends in `.part` remains indistinguishable
from a partial by name and is out of scope here.

Contributed by [@pujitha24](https://github.com/pujitha24) in [#906](https://github.com/Basekick-Labs/arc/pull/906).

### Line-protocol WAL entries now carry their database ([#889](https://github.com/Basekick-Labs/arc/issues/889))

The WAL has two append paths and only one of them carried the database. Native
msgpack writes store the client's bytes behind an envelope that names the
database; line-protocol writes, which reach the buffer as columns with no raw
bytes, fell back to a bare row-format entry. The replication stream carries the
entry as written, and a receiver parses it with `default` as the fallback
database, so every line-protocol row a replica received was filed under
`default` rather than the database it was written to. The row-format path now
writes the same envelope. A receiver resolves the real database; recovery on
the writing node is unchanged, since it already routed by the `_database`
stamped on each record, and any binary from 26.05.1 on reads the entry.

This is one of the three issues that make up the local-storage replication
design work ([#886](https://github.com/Basekick-Labs/arc/issues/886),
[#888](https://github.com/Basekick-Labs/arc/issues/888), #889), and the other
two are deliberately not in this release. A replica that applies replicated
rows flushes them into its own storage and its queries read that copy alongside
the file it pulls from the primary, so a receiver must not start on a node
without the hand-off #888 describes. Helm-deployed clusters run no receivers on
readers or compactors and see no behaviour change from this fix. On a cluster
where readers do receive (a local WAL enabled on the reader), line-protocol rows
now land in the right database instead of `default`. The receiving reader still
flushes and announces its own copy of them, so until #888 lands those rows count
twice on every node, as native msgpack rows already did.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#1055](https://github.com/Basekick-Labs/arc/pull/1055). [@drakeo338](https://github.com/drakeo338) proposed the same source-side fix in #889.

## Experimental arcx Arrow IPC streams signal writer panics ([#846](https://github.com/Basekick-Labs/arc/issues/846))

When the experimental arcx Arrow IPC stream writer panics, the response now
includes an invalid Arrow IPC message marker and an `Arc-Stream-Truncated`
trailer. Clients can detect the incomplete result instead of accepting a
stream that ends at a batch boundary as complete. The standard DuckDB Arrow
path is unchanged.

Contributed by [@efegokdemir](https://github.com/efegokdemir)
for [#846](https://github.com/Basekick-Labs/arc/issues/846).

## Experimental arcx Arrow IPC queries now appear in query management ([#731](https://github.com/Basekick-Labs/arc/issues/731))

When the experimental arcx engine serves an Arrow IPC query, the request now
receives an `X-Arc-Query-ID` and appears in active-query tracking and history.
Registry cancellation propagates into execution. Success, failure, timeout
and recovered panic paths dispose of the entry rather than leaving it running.
A declined arcx request reuses the same entry when DuckDB takes over.

The standard Arrow path continues to use its existing registry lifecycle.

Contributed by [@efegokdemir](https://github.com/efegokdemir)
for [#731](https://github.com/Basekick-Labs/arc/issues/731).

### Arc no longer overrides DuckDB's own container-aware memory and thread limits for the query engine ([#1026](https://github.com/Basekick-Labs/arc/issues/1026))

**If you run Arc in a container with a memory limit, this changes how much memory DuckDB is allowed, and you should read on.**

Arc derived `database.memory_limit` from the CPU count — `min(NumCPU(), 32) GB` — and `database.thread_count` from `NumCPU()` directly. `runtime.NumCPU()` reflects cpuset/affinity but **not** a CFS quota, and Kubernetes `limits.cpu` and docker `--cpus` are quotas. So a pod limited to 2 CPUs on a 64-core node saw 64 cores, estimated 128 GB of system memory, and was handed a **32 GB** DuckDB memory limit inside a 2Gi container along with `SET GLOBAL threads=64`. DuckDB then planned and allocated against that budget instead of spilling to `database.temp_directory`, and the kernel OOM-killed the container — which is the outcome `memory_limit` exists to prevent.

The fix is to stop overriding, because **DuckDB already does this correctly**. `duckdb::CGroups::GetMemoryLimit` and `GetCPULimit` are in the library Arc links; DuckDB reads `memory.max`, `memory.limit_in_bytes` and `cpu.max`, and with no explicit limit it uses 80% of what it finds. Measured: 28.7 GiB on a 36 GiB host, 409.5 MiB in a `--memory=512m` container, and `threads=2` under `--cpus=2`.

So `database.memory_limit` now defaults to empty and `database.thread_count` to `0`, both meaning "leave DuckDB's own value". An explicit setting still wins, and the licensed-core cap is unaffected.

**What changes for you:**

- **Container with a memory limit** — DuckDB's limit is now derived from your container, not from the host's core count. In the 2Gi example above it drops from 32 GB to roughly 1.6 GiB. This is the fix.
- **No memory limit set** (including the OSS Helm chart, which ships `resources: {}`) — DuckDB sees the node's total memory and takes 80% of it. On a 16-core/64 GiB node that is a **rise**, from Arc's 16 GB to about 51 GiB. If you were relying on Arc's accidental cap, set `database.memory_limit` explicitly, or set container limits.
- **A smaller limit makes spilling more likely.** Size `database.temp_directory` accordingly — in Kubernetes, the emptyDir or volume behind it. Be aware that spilling is not unconditional: measured against the engine, below roughly 85 MB a 2M-row `GROUP BY` does not spill, it raises `Out of Memory Error`, at any thread count. Compaction treats that as recoverable and halves its batch (30 → 15 → 7 → 3) with a warning before giving up, so it is bounded and visible rather than silent — but a container small enough to derive a per-subprocess limit in that range will make no compaction progress. A 256 MiB container at the default `max_concurrent = 2` derives about 68 MiB, which is inside that range.

**Compaction.** `compaction.memory_limit` auto-derives from `database.memory_limit`, which is now empty — and left alone, every compaction subprocess would fall back to DuckDB's own default and take 80% of the *same* cgroup, so a main process plus the default two subprocesses would budget 240% of the container. Arc now detects the limit itself for that one purpose and gives each subprocess `detected x 0.8 / (max_concurrent + 1)`. Detection reads cgroup v2, then v1, then `/proc/meminfo`, and `hw.memsize` on macOS; if nothing can be determined the previous behaviour is kept.

To be precise about what that bounds: it caps the **subprocesses'** combined budget at roughly one share, but it does not make the total fit the container — the main process still takes the engine's own 80% of the whole cgroup, so at the default `max_concurrent` of 2 the worst case is about 133% of the limit. That is far better than what it replaced (a 2Gi pod previously budgeted 32 GB for the main process plus 16 GB per subprocess) and it is tolerable because these are spill thresholds rather than reservations, but it is not a guarantee. Bounding Arc's total memory means deciding how much of the machine it may use in aggregate, which is [#1025](https://github.com/Basekick-Labs/arc/issues/1025).

**`compaction.threads` was left overridden by this change and is fixed separately**, in [#1030](https://github.com/Basekick-Labs/arc/issues/1030) below: a compaction subprocess in that same 2-CPU pod was getting `SET threads=32` while the main process correctly got 2.

**Also fixed:** `database.memory_limit = "50%"` or `"0"` passed configuration validation and then hard-failed startup inside DuckDB with a bare parser error. Both are now rejected at load with a message naming the key and listing the accepted units, using the same rule `compaction.memory_limit` already enforced.


### WAL replay is now idempotent, so a retained file stops re-applying its healthy entries ([#1009](https://github.com/Basekick-Labs/arc/issues/1009))

A replayed WAL entry correctly writes nothing back to the WAL — the copy being replayed is already on disk. But it also produced no flush *checkpoint*, because a checkpoint is keyed on the identity the WAL assigns at append time and a replay never appends. So every subsequent recovery pass replayed the same entry again.

That matters whenever recovery **keeps** a file, which one poisoned entry is enough to cause ([#590](https://github.com/Basekick-Labs/arc/issues/590)): the file's healthy entries were re-applied on every pass, and for measurements without tags compaction can never remove the resulting duplicate rows. The periodic purge eventually deletes the kept file and bounds the damage to one duplicate set. That backstop survives the move to flush-aware purging described above: a file recovery keeps is one no checkpoint will ever cover, so it is reclaimed by age rather than by the flush floor.

A replayed batch now inherits the identity of the entry it came from, so the flush that persists it checkpoints the **original** entry and no later pass replays it. Two deliberate exclusions, both of which would otherwise turn duplication into loss:

- **The row-format recovery path does not inherit.** It fans one WAL entry out into one buffer write per record, routed by each record's own measurement, so a checkpoint for the entry could mark data durable that a sibling write discarded. That path is the rare non-msgpack fallback and keeps today's at-least-once behaviour.
- **Untracked entries do not inherit.** Their identity is a SHA-256 of the payload rather than a writer-assigned sequence, and two legitimately identical payloads share it — two tagless rows with the same values and timestamp are two real events. Checkpointing one would make recovery skip the other. #948's fix moved off content hashes for this reason; inheritance is gated on the 32-character tracked form.

### WAL recovery no longer replays data that already reached Parquet ([#948](https://github.com/Basekick-Labs/arc/issues/948))

**If you run with `wal.enabled = true` and ingest measurements without tags, this fixes a permanent over-counting bug.**

WAL recovery replayed every entry in the files it found, including entries whose batch had already been written to Parquet before the crash. Nothing acknowledged was lost — the failure was in the other direction. For tagged measurements the duplicates are exact copies and compaction removes them at the partition's next pass, so queries merely read high for a while. For measurements **without tags** compaction deliberately does not dedup (two tagless rows sharing a timestamp can be two legitimate events), so those duplicates were never removed: a restart after a hard crash silently double-counted up to a full WAL window of already-durable data, permanently.

Every WAL entry now carries a tracked identity, and a checkpoint entry listing flushed identities is appended to the WAL — and durably synced — only *after* the Parquet write for that batch succeeds. Recovery gathers those checkpoints across all WAL files, including recently rotated files too young to replay and the active file it deliberately skips, and replays only the entries they do not cover. Writer identities come from `crypto/rand`, so a restarted process cannot reuse the previous one's identity space and have a fresh entry silently covered by a stale checkpoint — that inversion would have turned this over-counting bug into real data loss.

Measured on the issue's own acceptance criterion — tagless measurement, `kill -9` mid-ingest, 310 acknowledged records of which 300 had flushed:

| | replayed entries | post-restart rows |
|---|---|---|
| before | 310 | 610 (300 permanent duplicates) |
| after | 10 | 310 (exact) |

The ordering is load-bearing in one direction only: the checkpoint is written strictly after the flush is confirmed, never before. One window remains — a crash in the gap between the Parquet write completing and the checkpoint becoming durable replays that batch — so replay is still at-least-once in the strict sense, but the window is a single in-flight batch rather than a whole WAL file. **Historical over-counts from a crash recovery on an earlier version are not repaired retroactively.**

Checkpoint writes cost nothing measurable at Arc's flush rate: measured ABAB with the WAL on `fdatasync` and local storage at 683 flushes/s, 1,366,233 rows/s against 1,320,917 before the change — inside run-to-run noise.

**Follow-up ([#1045](https://github.com/Basekick-Labs/arc/issues/1045)):** as first merged, this recorded checkpoints only for *synchronous* flushes — the age sweep, a schema change, `FlushAll` and shutdown. The flush task for an **asynchronous, size-triggered** flush was built without its WAL identities, so the checkpoint call received nothing and the main ingest path still replayed after a crash. Measured on the same crash test with the buffer configured to flush by size rather than age: 310 acknowledged records of which 300 had flushed replayed all 310 and left 600 rows queryable. Fixed before release; both paths now checkpoint, and the regression test drives the asynchronous path specifically.

One related WAL problem is **not** fixed by this and remains tracked in [#1009](https://github.com/Basekick-Labs/arc/issues/1009): recovery still deletes a replayed file as soon as its records are back in the buffer, before they reach Parquet. The age-based purge of rotated files *is* fixed — see the entry above.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#998](https://github.com/Basekick-Labs/arc/pull/998).

### Compaction subprocesses now size their threads from the container's CPU quota ([#1030](https://github.com/Basekick-Labs/arc/issues/1030))

`compaction.threads` defaults to half the available cores, which it derived from `runtime.NumCPU()`. That reflects cpuset/affinity but **not** a CFS quota, and Kubernetes `limits.cpu` and docker `--cpus` are quotas — so on a 2-CPU pod on a 64-core node every compaction subprocess ran `SET threads=32`. This is the same root cause as [#1026](https://github.com/Basekick-Labs/arc/issues/1026) above, in the one place that fix did not reach, and it mattered twice over: the subprocesses are separate processes in the *same* cgroup as the main process, and DuckDB's sort and scan buffers scale with the thread count, so the oversubscription cost memory as well as scheduling.

The default is now derived from `min(runtime.NumCPU(), runtime.GOMAXPROCS(0))`. Since Go 1.25 the runtime computes `GOMAXPROCS` as `min(affinity CPUs, max(ceil(quota), 2))`, so it reads the quota — including under cgroup v1 — while `NumCPU()` covers cpuset limits that no quota expresses. Measured on an 8-CPU VM: `--cpus=2` yields 2, `--cpus=1.5` and `--cpus=0.5` also yield 2 (the runtime floors at 2), and `--cpuset-cpus=0-1` yields 2 through `NumCPU()`. An explicit `compaction.threads` is untouched, and with no CPU quota the resolved value is identical to previous releases.

This does **not** make the totals fit the quota, and it is worth being precise: on a 2-CPU quota at the default `max_concurrent = 2`, the two subprocesses take 1 thread each and the main process takes the quota's 2, for 4 threads against 2 CPUs. As with memory, these are caps on parallelism rather than reservations, and the kernel throttles the aggregate — the point is to stop a subprocess planning as if it had 32 cores.

**Measured, and it is a trade rather than a free win.** Under `--cpus=2` on 351 files / ~300 MB of input, varying only the per-subprocess memory budget:

| `compaction.memory_limit` | `threads=1` (new default) | `threads=32` (old default) |
|---|---|---|
| 4.47 GB — what that container derives with no memory limit set | success, 13.0 s | success, **9.8 s** |
| 1 GB | success, 20.5 s | **`Out of Memory Error`** |
| 512 MB | success, 22.7 s | **`Out of Memory Error`** |

With memory to spare, 32 threads is about 1.3x faster even under a 2-CPU quota — so where a container limits CPU but not memory, this change costs compaction throughput, and setting `compaction.threads` explicitly is the right move. Where the container limits both, the old default did not merely waste CPU: it failed. DuckDB's own error names the cause — *"Possible solutions: Reducing the number of threads (SET threads=X)"* — and a 2-CPU / 2 Gi pod on a 64-core node derives about 546 MB per subprocess at the default `max_concurrent`, which is squarely in the failing range. That is the deployment this issue is about.

Compaction classifies an out-of-memory error as recoverable and retries with a halved batch, up to four times, before giving up (`30 -> 15 -> 7 -> 3` at the default `max_files_per_batch`). So the old behaviour surfaced as slow, repeatedly-shrinking compaction cycles rather than a hard stop, and at a small enough budget as partitions that simply never compacted — which is why it was not obvious from the outside.

Two residuals are deliberate. A `GOMAXPROCS` environment variable set above the quota is honoured up to the machine's core count, and `GODEBUG=containermaxprocs=0` disables the runtime's cgroup read entirely; both are an operator explicitly overriding their own runtime's container awareness. Conversely, `GOMAXPROCS` set *below* the core count on a machine with no quota now lowers this default where it previously did not.

**`database.max_connections` and `ingest.flush_workers` are deliberately left host-derived.** Both were part of the original report and neither is a CPU-capacity proxy:

- `max_connections` bounds statements in flight. Arc queries are frequently S3-I/O-bound, so taking a 2-CPU pod from 64 pool slots to 4 would convert concurrency into client timeouts against the 30 s HTTP write timeout. It is still worth tuning by hand in a small container, because this pool is the only global admission control on concurrent query execution and the Go-side result memory of an in-flight query is not bounded by DuckDB's `memory_limit`.
- `flush_workers` was measured rather than reasoned about. Against a storage backend taking 500 ms per upload, with the Go path held to 2 cores and the same offered load in each arm (131.6M against 131.5M rows acknowledged), 8 workers — what a 2-CPU quota would produce — pushed **61%** as many rows to storage over the same 90 s window as 64 workers did: 68.0M against 111.3M, ABAB. Both arms sat at their upload-concurrency ceiling, 13.6 of a theoretical 16 uploads/s and 112 of 128, which is the finding: the pool is bound by concurrent uploads, not by cores. Against a 1 ms backend the two were indistinguishable — 8 workers slightly ahead — and both were limited by the ingest path instead, so the slower pool is not merely losing a race that no deployment runs.

  The remainder of the offered rows in each arm was queued or in flight when the window closed, not lost, and the smaller pool held far more of it: around 22,500 deferrals against 1,187–3,768. That is the wrong direction for the small container that would have been the one to get the smaller pool, given that Arc deliberately does not cap ingest buffer memory. No acknowledged write is lost at either size — drained to completion with 8 workers, 49,765,000 rows acknowledged became 49,765,000 written, with all 8,065 deferrals retried.

**A licensed core cap no longer raises a limit to meet the licence.** `MaxCores` enforcement gated on `runtime.NumCPU() > MaxCores` and then assigned `MaxCores` outright, so a 4-core licence in that 2-CPU pod *raised* `GOMAXPROCS` from 2 to 4 and set DuckDB `threads=4` — overriding the container-correct value #1026 had just arranged. Each surface is now clamped independently: `GOMAXPROCS` and an explicitly configured `database.thread_count` can only move down, and `ingest.flush_workers` is capped whether or not a quota is in play.

Three things to know about the new behaviour, because none of them is obvious:

- **`GOMAXPROCS` is now pinned on a licensed node, in both directions.** Pinning is what stops a quota raised later (an in-place pod resize, `docker update --cpus`) lifting it past the licence — but the Go runtime disables automatic updates rather than capping them, so a licensed node whose quota is *lowered* keeps its boot value while an unlicensed node would follow the quota down.
- **The clamp can still exceed the container's CPU quota in one case.** When `database.thread_count` is unset, the licence replaces it only if the licence is below the *machine's* core count — that, not `GOMAXPROCS`, is the bound on what DuckDB would otherwise choose, because DuckDB reads `cpu.max` directly and has never read `GOMAXPROCS`. The value written is clamped by the effective core count, so it cannot exceed what this process may use; but where an operator has inflated that themselves (`GOMAXPROCS` above the quota, or `GODEBUG=containermaxprocs=0`) the result can sit above the quota. It is always within the licence.
- **`compaction.threads` is still not licence-capped at all.** It is resolved during config load, before the licence is applied, and a subprocess is a separate process that the parent's `GOMAXPROCS` cannot reach — so a 4-core licence on a 64-core host with no quota still runs each subprocess with 32 threads. Pre-existing, unchanged by this release, and tracked separately.

Enforcement remains boot-time: nothing re-applies it after periodic re-validation.

**For clustered Enterprise deployments, this changes core accounting.** Nodes report `GOMAXPROCS` as their core count in the join payload, and the cluster sums those to check the licence. A node in a 2-CPU pod on a 64-core host with a 4-core licence previously reported 4 — the raised value — and now reports 2, so twice as many such nodes fit one licence. Each node genuinely has 2 usable cores, so 2 x 2 = 4 is the licence being counted accurately rather than evaded, but the number of nodes that can join may change.

### Quota-derived compaction threads account for concurrent jobs ([#1037](https://github.com/Basekick-Labs/arc/issues/1037))

When `compaction.threads` is left at its automatic default under a CPU quota,
its value now accounts for `compaction.max_concurrent` and the main process.
The existing automatic value on hosts without a CPU quota and explicit thread
settings are unchanged.

### A deferred flush now waits for a free worker instead of the next age sweep ([#1008](https://github.com/Basekick-Labs/arc/issues/1008))

When the flush queue is full, Arc keeps the batch in its in-memory buffer rather
than discarding it. Nothing then re-enqueued that buffer: it waited for the next
write to the *same* measurement, for the age-based flush, or for shutdown. With
ingest spread across many measurements, a buffer could sit while flush workers
went idle.

Arc now re-enqueues deferred buffers, oldest first, as soon as a worker frees a
queue slot. Measured on a node with 150 measurements, one flush worker and a
storage backend taking 1.5 s per write: before, records written stayed flat at
16,800 for 48 seconds with an idle worker until the age trigger fired; after,
they climb continuously and the worker stays saturated.

Two new signals, both already present in `GetStats` as `total_flush_deferred` and
`deferred_buffers`:

- `arc_ingest_flush_deferred_total` — counter, incremented every time a flush
  could not be queued. Under sustained backpressure that is close to once per
  write to a hot measurement, so read the rate as saturation, not volume.
- `arc_buffer_deferred_buffers` — gauge, the number of buffers currently holding
  records no worker could take. This is the one to alert on; it should return to
  zero within roughly one flush duration.

**What this does not fix.** Deferred records are held in memory, and Arc does not
bound that — deliberately. `ingest.max_buffer_size` is a *per-measurement* flush
trigger, so worst-case held memory is roughly
`active_measurements x max_buffer_size x bytes_per_record`, and a global cap low
enough to prevent that would reject many-measurement workloads that work today.
The knobs are yours: lowering `max_buffer_size` or `max_buffer_age_ms` makes
flushes smaller and more frequent, at the cost of more Parquet files for
compaction to merge. Better signals for deciding that are tracked in
[#1025](https://github.com/Basekick-Labs/arc/issues/1025).
The drain also competes with ordinary writes for a freed slot, and a writer
already holding its shard lock wins, so under *sustained* saturation a cold
deferred buffer can still fall through to the age sweep — the guarantee is that
it no longer needs an idle system to be picked up, not that it is always first.
On a cluster reader, which applies replicated entries into its own buffer for
query freshness, deferrals drain the same way but the reader cannot push back on
the replication stream; see #1025.


### Ingestion retains buffered batches when the flush queue is full ([#966](https://github.com/Basekick-Labs/arc/issues/966))

A size-triggered flush used to extract the whole buffer and delete it, and only
then attempt a non-blocking send to the bounded flush queue. When the queue had
no room the batch was discarded and the write still returned success — so a
buffer built from many already-acknowledged writes was lost. With the WAL
disabled, which is the shipped default, it was gone immediately; with the WAL
enabled, nothing replayed it before the periodic purge deleted the file.

Nothing is extracted now unless it has somewhere to go. Records that cannot be
queued stay in the in-memory buffer, and the send happens under the shard lock,
so the decision is atomic with respect to the buffer.

A retained buffer is flushed as soon as a flush worker frees a queue slot (see
the next entry), by the next write to the same measurement, by the age-based
flush, or by shutdown. How many records a node retains is bounded by your
`ingest.max_buffer_size` and `ingest.max_buffer_age_ms` settings rather than by a
cap Arc applies — see the note on the drain below. A sampled warning and
the new `arc_ingest_flush_deferred_total` metric report the condition that used
to be a silent drop, so it is visible rather than inferred.

This closes the first of #966's five routes; what remains is
[#1008](https://github.com/Basekick-Labs/arc/issues/1008) and
[#1009](https://github.com/Basekick-Labs/arc/issues/1009).

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#997](https://github.com/Basekick-Labs/arc/pull/997).


### A continuous query could write into Arc's reserved storage root ([#1010](https://github.com/Basekick-Labs/arc/issues/1010))

A continuous-query definition is a row that outlives the request that wrote it,
and the create and update endpoints applied no name rule to its `database`
([#993](https://github.com/Basekick-Labs/arc/issues/993)). The run did not check
either. Its source-path builder refuses glob metacharacters, so a database of
`**` already failed — but it accepts a leading underscore or dot deliberately,
because that is the storage key contract rather than a name format. So a
definition stored by any build to date, by an older build, or written straight
into the metadata database, could carry `_schema`, `_compaction_state` or
`.hidden`, and its output landed inside the directory Arc reserves for its own
state.

Nothing refused it and nothing reported it. The database listing hides
underscore- and dot-prefixed entries while table resolution still reads them, so
the rows were queryable but absent from every listing. Every walker that
enumerates the storage root skips reserved directories, so that data was never
aged out by retention, never compacted, never tiered and never exported to
Iceberg; for an underscore-prefixed name it was **not backed up** either (backup
skips underscore-prefixed roots only, so a dot-prefixed one was copied). The
field-schema registry wrote anchors for the pseudo-database, nesting its own
anchor tree inside itself at `_schema/_schema/`, which also made every real
database name enumerate as a measurement of it. And because anchor cleanup
removes everything under that prefix, deleting an unrelated real database whose
name matched the continuous query's destination measurement would delete the
continuous query's output with it.

A run now re-validates the stored `database` against the same rule the ingest
endpoints apply, before it builds any path: start with a letter, then letters,
digits, underscores or hyphens, at most 64 characters.

**A definition that fails now reports a failed run** rather than executing. The
reason is recorded with the execution and readable through
`GET /api/v1/continuous_queries/:id/executions`, naming both the continuous
query and the offending value, so one that stops producing after an upgrade says
why. The remedy is to update that definition with a valid database name, or delete it
and create it again; what is not available is keeping the old value, because
update applies the same rule to the body it is given (#993).

One deployment shape to check before upgrading. The rule is stricter than the
storage layer's, and an edge-sync hub's spoke directories are top-level storage
roots whose IDs are validated by a blocklist — a spoke ID may begin with a digit
or contain a dot, which this rule refuses. A continuous query whose `database`
names such a spoke runs today and will now report a failed run; recreate it
against a database name that satisfies the rule. Nothing that produces a
well-formed database directory is affected.

Creating a continuous query requires an admin token, so this was a
data-integrity bug rather than a vulnerability: no permission check was bypassed
and no path escaped the storage root.
### A graceful shutdown no longer abandons flushes, and a flush timeout no longer starts before a worker picks the task up ([#1006](https://github.com/Basekick-Labs/arc/issues/1006), [#1007](https://github.com/Basekick-Labs/arc/issues/1007))

Two ways an acknowledged write could reach no storage at all.

A flush task's timeout was created when the task was **queued**, not when a
worker picked it up, so it was consumed while the task waited its turn. With a
backlog of N tasks at T seconds per flush, the task at the back arrived with
`flush_timeout_seconds - N*T` left and could already be expired; the storage
write then failed with `context deadline exceeded` and the batch was dropped as
though storage had failed. A queueing delay was reported — and handled — as a
storage outage.

`Close` cancelled flushes in progress and **discarded** whatever was still in
the flush queue, in favour of WAL replay. Those records had already been removed
from the in-memory buffer when they were queued, so nothing else would ever
write them. A client disconnecting during a schema-evolution flush aborted that
flush too, even though the rows in it belonged to other clients' earlier,
already-acknowledged writes. With the WAL disabled — the shipped default — every
graceful stop under load lost those records.

What changed:

- Flush I/O runs on a context that neither shutdown nor a client's cancellation
  reaches. The timeout starts when a worker receives the task.
- `Close` flushes every buffer, then flushes everything still queued instead of
  discarding it, then waits for any flush a writer is finishing on its own
  goroutine — and only then reports whether the shutdown was clean.
- A write that arrives after its own shard has already been flushed by shutdown
  is refused with `503` and counted as WAL-only, so the shutdown WAL purge is
  skipped rather than deleting the only remaining copy. The check is per shard,
  not global: a write arriving while shutdown is still working through the other
  shards is accepted and flushed as usual.
- `Close` bounds itself by **half** of `server.shutdown_timeout`, because it is
  one shutdown component among several and the coordinator checks its own
  deadline only between them. If that slice expires, `Close` stops flushing,
  cancels any write still in progress, and reports the shutdown unclean — so the
  remaining steps, including the WAL writer's final sync, still run. Records it
  did not get to are left in the WAL and replayed on the next start. With the
  WAL **disabled** there is nothing to retain, so those records are lost; a
  shutdown that reports unclean with `wal.enabled = false` is telling you data
  did not land.

Two limits worth knowing. On the **local** storage backend a write already in
progress cannot be interrupted, because that backend does not observe its
context; local writes are fast, but a graceful stop can wait for one. And
Parquet files written during shutdown are still not registered in the cluster
manifest, because the file registrar stops before the buffer does — pre-existing,
and tracked separately.

**Not covered by this change:** a full flush queue still drops its batch
([#966](https://github.com/Basekick-Labs/arc/issues/966) route 1, in progress),
and with the WAL off a storage write that fails still loses that batch
([#1008](https://github.com/Basekick-Labs/arc/issues/1008),
[#1009](https://github.com/Basekick-Labs/arc/issues/1009)).


### Continuous queries re-validate their stored definition before each run

A continuous-query definition is validated when it is created or updated, but
it is a row that outlives the request that wrote it: it re-executes on a
schedule, and nothing re-checked it in between. So a definition could reach
DuckDB having never been checked by the rules in force at the time it ran —
one stored before the create-time validator existed, one stored by an older
build whose validator knew fewer cases, or one written directly into the
shared metadata database.

`executeAggregation` now runs the shared validator over the definition before
executing it, which covers the scheduler, the admin execute endpoint, and any
future caller in one place. The statement validated is the real one, with the
`{start_time}`/`{end_time}` placeholders already substituted, rather than a
representative probe.

**A definition that fails now reports a failed run** rather than executing.
The reason is recorded with the execution and is readable through
`GET /api/v1/continuous_queries/:id/executions`, so a continuous query that
stops producing after an upgrade says why. If you see one, the definition
needs editing to satisfy the current validator — most likely it references a
file path directly, calls a filesystem I/O function, or contains more than one
statement.

### Continuous query create and update apply Arc's database-name rule ([#993](https://github.com/Basekick-Labs/arc/issues/993))

`POST` and `PUT /api/v1/continuous_queries` validated `destination_measurement`
but applied no name rule to `database` beyond a non-empty check — the only
ingest-reaching `database` in the tree that did not. The value is not inert: it
becomes a storage path segment for both the source read and the destination
write, so a continuous query could be created whose output landed in a
directory named `**`, `db*`, `db[1]` or `host=hub01`.

Nothing read such a path unsafely. Every `read_parquet` sink that interpolates a
storage path applies the glob-safety guard, and a stored definition whose
database carries a glob metacharacter fails its run rather than producing such a
path — with the name-rule error above, since
[#1010](https://github.com/Basekick-Labs/arc/issues/1010) checks the stored name
before the source path is built. The reason to close it at the boundary is
that the guard is each sink's to remember, and this field should not be able to
produce such a name in the first place. Creating a continuous query requires an
admin token, so this is defence in depth, not a privilege-escalation path.

Two behaviour changes come with it. `database` must now satisfy the same rule as
everywhere else — start with a letter, then letters, digits, underscores or
hyphens, at most 64 characters — on both create and update. And because `PUT`
overwrites the whole definition, it now requires a valid `database` in the body:
a partial update that omitted the field previously succeeded and blanked the
stored row, and is now refused. A continuous query whose stored `database`
already violates the rule cannot be updated — delete it and recreate it with a
valid name.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#995](https://github.com/Basekick-Labs/arc/pull/995).

### Continuous query measurement names are validated, and update requires a complete definition ([#1011](https://github.com/Basekick-Labs/arc/issues/1011))

The sibling of the two `database` fixes above, for the two measurement names.
`source_measurement` had no name rule at all — create checked only that it was
non-empty, update did not check it — and `destination_measurement` had one on
create but only `if provided` on update. Since `PUT` overwrites every column, a
body that omitted a field stored a blank one.

Both are storage path segments of every run, and the write side applies no name
rule of its own: the only key check happens at flush, inside the storage
backend, long after the run has reported its result. Two consequences followed,
and neither was visible at the time it happened.

**A blank `destination_measurement` made a run report success and lose its
rows.** The write landed on a key with an empty segment, which the buffer
accepted; the execution was recorded as `completed` with a row count and no
error, readable that way through
`GET /api/v1/continuous_queries/:id/executions`; and the flush then failed with
`contains an empty segment`, taking the rest of that flush call with it. The
aggregation looked like it worked and produced nothing.

**A `destination_measurement` carrying a separator wrote into a measurement
nobody configured.** A stored value of `a/b` produced
`db/a/b/2026/10/02/12/a/b_….parquet` — nine path segments, each individually
legal, so the key contract accepted it. Those rows are not orphaned, which
would have been the safer failure: a measurement is read with a recursive glob,
so they read back as part of measurement `a`, they are listed as measurement
`a`, retention deletes them under that name, and Iceberg exports them into that
table. Hourly compaction and tiering are the two that skip them, incidentally —
the extra segments shift the partition fields, so the year fails to parse.

Both endpoints now validate both names, and a run re-validates them before it
builds any path — the same retroactive check
[#1010](https://github.com/Basekick-Labs/arc/issues/1010) added for `database`,
since a definition stored by an earlier build is not covered by a boundary rule
added today. **A definition that fails reports a failed run** rather than
executing, with the reason recorded against the execution.

The two fields get different rules, deliberately. `destination_measurement`
names a measurement Arc is about to create, so it gets the rule every
client-facing ingest endpoint applies: start with a letter, then letters,
digits, underscores or hyphens, at most 128 characters.
`source_measurement` names a measurement that already exists, so it gets the
rule Arc applies elsewhere to a name it is merely given — one storage path
segment, no separator, no `.` or `..`, no leading dot, no glob metacharacter.
That is the pair `POST /api/v1/delete` applies to a measurement in its body,
and the segment half of it is what the retention endpoints apply to a policy's
measurement. The stricter rule would have been wrong here: measurement names
predate it, and backup restore and WAL replay still admit names it refuses, so
a source of `_internal`, `7cpu` or `cpu.v2` keeps working.

**One deployment shape to check before upgrading.** The rule for a source
measurement is looser than the create-time one, but it is not looser in every
direction: it declines a **dot-prefixed** name, which the storage layer accepts
and which plain line protocol accepted before measurement validation shipped in
v26.02.1. A continuous query reading a measurement called `.hidden` resolves
today and will now report a failed run, and because the API declines to name
dot-prefixed directories anywhere, there is no way to rename it — the data has
to be re-ingested under a valid name, or the definition deleted. A stored
`destination_measurement` that violates the create-time rule fails its runs
likewise; that one is ordinary to fix, since the destination is the continuous
query's own output, so updating the definition is enough. Definitions created
before v26.02.1 are the population to look at for both: that release is when
`destination_measurement` validation was added to create.

**`PUT` now requires a complete definition** — `name`, `database`,
`source_measurement`, `destination_measurement`, `query` with both placeholders,
and `interval` — continuing what #993 began with `database`: a field left out
was previously written to the row as a blank. Note also what `PUT` has always
done with the fields it does **not** require: `description`, `tag_columns`,
`retention_days`, `delete_source_after_days` and `is_active` are written from
the body unconditionally, so omitting them zeroes them, and omitting
`is_active` sets it `false` and stops the continuous query. Send the whole
definition: read it, change the field, send it back. Both #993 and this change
land in 26.09.3, so for anyone upgrading from 26.09.2 a partial `PUT` stops
working in this release — a client that sends `{"is_active": false}` alone
needs updating.

### The delete WHERE validator reuses the shared table-position guard

`POST /api/v1/delete` takes a WHERE fragment and interpolates it into a
`read_parquet(...)` query, but validated it with its own set of scans —
punctuation, keywords, I/O-function names, prefixes — and none of them
examined table position. A string literal standing where a relation belongs
is resolved by DuckDB rather than treated as a value, and that class has no
function name for a denylist to match.

The fragment now goes through the same guard the query endpoints use, applied
to the fragment itself rather than the assembled statement (by then it sits
inside Arc's own `read_parquet(...)`, which would self-trip the I/O denylist).
Reusing that guard rather than extending this file's keyword list means it
tracks the relation-introducing keywords DuckDB has rather than drifting from
them. Ordinary predicates are unaffected, including values that look
path-like.


### A newline before a table function's parenthesis made the function a measurement

Arc's RBAC extractor and its SQL rewriter each decide independently whether a
name in table position is a table-valued function call, and they disagreed on
what counts as whitespace before the parenthesis. The extractor skips a
table-valued function by looking for the next non-whitespace byte after the name, counting
space, tab, carriage return and newline; the rewriter's equivalent check
trimmed only spaces and tabs. So a table function whose opening parenthesis sat
on the next line was a function to the permission check and a measurement to
the rewriter, which emitted a `read_parquet` for the function's name.

DuckDB rejects the resulting SQL with a parser error rather than reading
anything, so this was never a bypass — but it broke legitimate multi-line SQL,
and the two sides must agree for the authorization set to mean anything. Both
now use the same whitespace set.

Found while fixing
[GHSA-h3rq-5r29-2wrh](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-h3rq-5r29-2wrh);
the two are the same extractor-versus-rewriter seam in two different table
positions.

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
so a comma-continued measurement is no longer read unchecked. The cross-database
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

### One stale peer could block a file from replicating, and took the local copy with it ([#999](https://github.com/Basekick-Labs/arc/issues/999))

On a per-node-storage cluster, the file puller asks candidate peers for a file
in turn and verifies every byte against the checksum the cluster manifest
names. When a peer's bytes failed that check the puller stopped: it did not ask
the remaining candidates, and it deleted its own copy of the file.

Stopping was meant to treat a bad checksum as a data-integrity signal rather
than a reason to keep shopping. But a mismatch is a fact about the peer that
answered, not about the file. A rewrite reaches every node's manifest through
Raft before the new bytes reach every replica, so a peer that is merely behind
rejects on checksum while a peer that has the bytes would have served them. The
peer that is behind is routinely the first one asked: the puller tries the
file's origin node first, a rewrite in place kept the original origin (fixed by #976, below), and a
compacted file's origin is the node that compacted it — none of which is
necessarily the node that performed the rewrite. The origin is also the one
candidate whose position is fixed, so every retry put the same stale peer first
and the loop broke on it again. Deleting the local copy then turned "this node
serves the previous generation of the file" into "this node has no file for that
partition", which queries report as fewer rows rather than as an error, because
the read path globs `*.parquet`.

A rejected checksum now moves on to the next candidate, which is enough for the
common case of a stale origin in front of a healthy replica. The fall-through
is bounded at three peers per attempt: a peer can reject either in its reply
header, before any bytes move, or on the hash of the body it just sent, and the
puller cannot tell which it will be before asking — so the bound counts peers
rather than bytes. Because the attempt loop is unchanged, an entry that no peer
can serve now costs at most three rejections per attempt instead of one, nine
in total at the default retry count. Asking more peers cannot cause bad bytes to
be accepted: every candidate's body is still verified against the manifest
checksum before it is committed.

The rejected bytes are discarded and the committed file is left in place, so a
node keeps serving the generation it already had until some peer can supply the
one the manifest names. A path left in place this way is remembered, and the
next time it comes round the puller fetches it rather than trusting its size:
whether a file is present is otherwise decided by size alone, so a rewrite that
did not change the length would make the kept copy look correct forever — the
delete this release removes was what used to guarantee the retry. On local storage the rejected bytes sit in the write
staging file and only that is removed. S3 and Azure never commit them at all —
an upload whose source ends early or errors is never completed — so there is
nothing to clean up and, since this release, nothing is deleted there either.

A new `checksum_mismatch_exhausted` counter in the replication stats reports
entries that failed after every reachable peer disagreed with the manifest,
which is the state that warrants operator attention; it is also surfaced in the
body of a 503 from the catch-up query gate, because it names a reason the gate
will not clear on its own. `checksum_mismatch` continues to count individual
peer rejections, and now rises by more than one per attempt when the puller
falls through. Individual rejections moved from warning to debug level, since a
peer that has not yet caught up to a rewrite is expected to reject.

Resuming an interrupted transfer is now sized and hashed from the staging file
alone, and is refused outright when a committed file is present. **This closes a
silent corruption path that did not need a checksum mismatch to reach.** A
resume hashes a prefix and appends a tail, but the calls that sized and read the
prefix answer with the committed file whenever one exists, while the tail is
appended to the staging file. A transfer interrupted mid-body over an older,
shorter generation of the same path therefore hashed the old file as the
"prefix" of the new one; where the old generation was a byte prefix of the new,
the combined checksum verified and a file holding only the tail was committed
and counted as a successful pull. The consequence of the new rule is that an
interrupted transfer over an existing file restarts from zero rather than
resuming; a first-time pull still resumes as before. Backends with no staging
area, S3 and Azure, never had a partial to resume from and now skip the probe
instead of discovering it through a failed append — which also means
`bad_offset_backend` now stays at zero in every shipping configuration.

The same diagnosis was reached independently by
[@efegokdemir](https://github.com/efegokdemir) in
[#989](https://github.com/Basekick-Labs/arc/pull/989) — that a checksum
mismatch describes the peer that answered rather than the file, so the loop
should continue and the local replica should survive. That reading is correct
and is what this change implements.

Note for anyone tracing this further: presence is still decided by size alone,
so a stale copy whose length happens to match the manifest's reads as present.
This change cannot strand an entry on that rule — an exhausted pull remembers
the path and forces one re-pull rather than trusting the size — but the rule
itself is unchanged, and [#975](https://github.com/Basekick-Labs/arc/issues/975)
is where a durable fix belongs.
