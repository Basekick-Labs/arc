# Arc v2027.01.1 Release Notes

> **Status:** Planned — January 2027 release.

## Bug fixes

### Compaction subprocess threads respect license and effective-core limits ([#1036](https://github.com/Basekick-Labs/arc/issues/1036))

Each compaction subprocess is now capped at the lower of the license's
`MaxCores` and the effective cores available to Arc, after automatic thread
defaults have been resolved. Lower configured values are preserved. This is a
per-process cap, not an aggregate reservation: the main process and multiple
subprocesses can still request more threads in total than `MaxCores`. Capping a
previously higher setting can reduce compaction throughput.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1043](https://github.com/Basekick-Labs/arc/pull/1043).

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

