# Arc v2026.09.3 Release Notes

> **Status:** Planned — November 2026 patch release.

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
  look back for: the de-duplication ratio Arc emits after a compaction counts
  its input rows with a query that has always errored, so the one signal that
  would have reported the loss has never fired. Tracked separately as
  [#1015](https://github.com/Basekick-Labs/arc/issues/1015).
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
why. The remedy is to delete that definition and create it again with a valid
database name; editing it is not available, because update applies the same rule
to the body it is given (#993).

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
file's origin node first, a rewrite in place keeps the original origin, and a
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
