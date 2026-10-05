# Arc v2027.01.1 Release Notes

> **Status:** Unreleased.

## Bug fixes

### Delete API: remove the dead `xp_`/`sp_` prefix check

`validateWhereClause` listed lowercase `xp_`/`sp_` patterns and matched them
against the upper-cased WHERE clause with `strings.Contains`, so the loop never
refused anything. The prefixes are SQL Server vocabulary with no meaning to
DuckDB; the patterns and the loop are removed.

Contributed by [@abhicodes-007](https://github.com/abhicodes-007) in [#1078](https://github.com/Basekick-Labs/arc/pull/1078).
