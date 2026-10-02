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
