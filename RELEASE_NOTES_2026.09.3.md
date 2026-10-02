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

### Compaction reuses local inputs safely and avoids glob/hive path interpretation ([#969](https://github.com/Basekick-Labs/arc/issues/969))

Local compaction inputs are read in place instead of being copied into the job
temporary directory. Compaction now rejects glob-sensitive paths before handing
them to DuckDB, disables Hive partition inference for input paths, preserves
temporary-file collision protection for streamed backends, and treats a
concurrent input disappearance as a permanent skip for the current batch. Local
input reads remain on the data volume and therefore do not contribute to the
storage ReadTo byte metrics; the configured temporary directory continues to
apply to streamed remote inputs and output staging.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#990](https://github.com/Basekick-Labs/arc/pull/990).

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
