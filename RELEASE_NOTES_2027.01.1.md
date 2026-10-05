# Arc v2027.01.1 Release Notes

> **Status:** Planned — January 2027 release.

## Features

### DuckDB extensions can be configured per deployment ([#440](https://github.com/Basekick-Labs/arc/issues/440))

Operators can set `database.extensions` or the `ARC_DATABASE_EXTENSIONS`
environment variable to install and load DuckDB extensions at startup. Arc
loads them before enabling its DuckDB external-access lockdown; the setting
does not enable unsigned extensions.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1082](https://github.com/Basekick-Labs/arc/pull/1082).

## Bug fixes

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
