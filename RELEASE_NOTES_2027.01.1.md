# Arc v2027.01.1 Release Notes

> **Status:** Planned — January 2027 release.

## Bug fixes

### Delete API WHERE validation no longer rejects SQL words and punctuation inside string literals ([#834](https://github.com/Basekick-Labs/arc/issues/834))

`POST /api/v1/delete` scans the WHERE clause for statement-level SQL before it
interpolates the clause into the DuckDB statement: forbidden keywords (`DROP`,
`UPDATE`, `SET`, ...), `;` and comment markers, and DuckDB's file-I/O table
functions. Those scans ran on the raw text, so a value
that merely contained one of those words was refused as if it were SQL:
`status = 'delete-pending'`, `action = 'update'` or `note = 'a;b--c'` could not
be deleted through the API at all.

The scans now run on the clause with its string literals masked by the same
masker the query path uses, which knows plain `'...'`, escape-string `E'...'`
and dollar-quoted `$tag$...$tag$` forms, so a literal is data whatever it says.
The file-I/O scan masks literals the same way while still seeing an
identifier-quoted call such as `"glob"(...)`. The raw clause is what still
reaches DuckDB, and the unmatched-quote and unmatched-parenthesis checks still
run on it. The same syntax outside a literal is refused exactly as before,
including the escaped-quote and dollar-tag shapes the masker was hardened
against in 26.09.1 and 26.09.2.

Contributed by [@hizlidepoo](https://github.com/hizlidepoo) in [#841](https://github.com/Basekick-Labs/arc/pull/841). [@efegokdemir](https://github.com/efegokdemir) proposed the same fix, including the file-I/O scan, in #937.
