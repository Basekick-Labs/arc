# DuckDB sandbox scoping: what is and is not possible

Date: 2026-09-12
Issue: #641 (per-tenant scoping of the DuckDB sandbox `allowed_directories`)
Status: design note. No per-tenant scoping shipped. See "Decision" below.

## The question

Arc locks DuckDB down once at startup (`internal/database/sandbox.go`): it sets
`allowed_directories` to every prefix the deployment legitimately needs, then
sets `enable_external_access = false`. The allowlist is therefore the union of
all tenants' directories, for the life of the process.

#641 asked whether that allowlist can be narrowed per query, so that a query
authorised only for `db1` cannot open a file under `db2` even if the SQL
validator is bypassed. This note records what was measured, and why the answer
is "not on the shared handle, and not worth it on separate handles".

## Measured constraints

Measured against the DuckDB that Arc ships, `github.com/duckdb/duckdb-go/v2`
v2.10505.0 (DuckDB 1.5.5). Each of these was reproduced directly, and the ones
that guard Arc's own behaviour are pinned by tests in
`internal/database/duckdb_sandbox_constraints_test.go`; see "When to revisit"
for exactly which.

1. **`allowed_directories` is GLOBAL-only.** `duckdb_settings()` reports scope
   `GLOBAL`. `SET SESSION allowed_directories = [...]` fails with
   `Catalog Error: option "allowed_directories" cannot be set locally`, and
   `SET LOCAL` with `Not implemented Error`. There is no per-connection or
   per-statement scope, so two concurrent queries on one handle cannot see
   different allowlists.

2. **It is immutable after lockdown.** Once `enable_external_access` is false,
   narrowing the list fails with `Invalid Input Error: Cannot change
   allowed_directories when enable_external_access is disabled`. `RESET` fails
   the same way. Re-enabling external access to get the setting back is also
   refused: `Cannot enable external access while database is running`. The
   lockdown is deliberately one-way.

3. **It is inert before lockdown.** With the allowlist narrowed to `db1` but
   `enable_external_access` still true, reading a file under `db2` succeeds.
   The allowlist only becomes an access check once external access is off.
   This is why `lockdownExternalAccess` sets the two in that order and verifies
   the flip, and it means there is no such thing as a cheap "born scoped"
   handle: an instance must complete its whole INSTALL/LOAD/CREATE SECRET
   sequence first, and is entirely unsandboxed until it does.

4. **It cannot be set in the DSN.** Opening with
   `?allowed_directories=[...]&enable_external_access=false` fails at open with
   `database/sql/driver: could not set invalid or local option for global
   database config`. The SET-then-lockdown sequence after open is mandatory.

5. **No LOCAL-scope setting opens a file of the query's choosing.** The
   complete LOCAL-scope set in 1.5.5 is 16 settings: profiling (including
   `custom_profiling_settings` and `profiling_coverage`), progress bar, search
   path, schema, streaming buffer size, caching operators, HTTP logging, and
   `debug_force_external`. Three of them name a file DuckDB *writes*
   (`profile_output`, `profiling_output`, `http_logging_output`); the sandbox
   still gates those writes after lockdown, which is asserted rather than
   assumed. None of the 16 reads a path the query supplies.
   `disabled_filesystems` is GLOBAL-only too, and is all-or-nothing per
   filesystem rather than per path.

6. **The allowlist is per DuckDB instance.** Two instances in one process hold
   independent allowlists and genuinely deny each other's directories with
   `Permission Error: ... file system operations are disabled by
   configuration`. This is the only mechanism in DuckDB 1.5.5 that can give two
   queries different filesystem scopes.

Consequence: the mechanism #641 proposed does not exist. Per-query scoping is
reachable only by routing the query to a different DuckDB instance.

## Why the instance pool was not built

The natural design is a bounded LRU pool of DuckDB instances keyed by the
query's authorisation scope (the set of databases it references, each already
checked by `checkQueryPermissions`). It was rejected for one structural reason
and several cost reasons.

### The structural reason: the key comes from the parser the threat model assumes is defeated

The scope key can only be derived from `extractTableReferences`
(`internal/api/query.go`), which matches identifiers after `FROM` and `JOIN`
and deliberately skips anything followed by `(` (`isFunctionCallAt`).
`checkQueryPermissions` then returns early when that set is empty.

So the queries this feature exists to contain are exactly the queries that
produce no key at all:

| query | derived scope |
|---|---|
| `SELECT * FROM read_parquet('<root>/db2/**/*.parquet')` | empty |
| `SELECT * FROM parquet_scan('<root>/db2/**/*.parquet')` | empty |
| `SELECT * FROM glob('<root>/**')` | empty |
| `SELECT * FROM read_text('/etc/passwd')` | empty |
| `WITH t AS (SELECT * FROM read_parquet('<root>/db2/**')) SELECT * FROM t` | empty |
| `SELECT * FROM db1.cpu, '<root>/db2/x.parquet' b` | `[db1]` |

#641's acceptance criterion says to test with validation stubbed. With
validation stubbed, the attack query is a bare `read_parquet(...)`, and the
routing decision that would select a narrow instance is made by the same
function that failed to see it. Routing an unkeyed query to the full-root
instance gives zero protection.

The design does contain the mixed shapes (last row): a `[db1]`-keyed instance
does deny the `db2` path. That is real but partial coverage, and it is the
harder class for an attacker to be forced into.

### The cheap variant of this, costed out

Most of what follows is an objection to keying N instances by scope. There is a
much cheaper shape that deserves its own accounting, because none of the cost
objections below apply to it: **one** extra DuckDB instance, created once at
startup with an empty allowlist, used only for queries whose derived scope is
empty.

It inverts the structural problem rather than suffering from it. The rule is
"a query the extractor could not attribute runs where nothing can be read", so
the parser failing to see `read_parquet` is precisely what routes the query
somewhere it cannot read. Every cost objection below dissolves: there is one
fixed instance rather than N, it creates no S3 or Azure secret so the refresher
collision cannot arise, it can take a token `memory_limit` and thread count
because it has no data to scan, it is built at startup so no query pays a pool
miss, and it is never evicted. An empty `allowed_directories` plus lockdown is
accepted by DuckDB, and such an instance still serves `SELECT 1` and
`range(1000000)` normally while denying `read_csv` and `glob('/**')`.

Its soundness rests on one invariant: a query with no extracted table
references must never need a storage path. That was measured rather than
assumed, by running Arc's real extractor and its real transform over the same
corpus. Every zero-reference query produced no emitted path, with one
exception: a bare single-quoted string in table position
(`SELECT * FROM '/root/db2/x.parquet'`) yields no references yet still reaches
the path builder. That shape is rejected by `ValidateSQLRequest` today, and
with validation stubbed the transform emits syntactically invalid SQL for it
(the literal is interpolated with its own quotes intact), so DuckDB fails it at
parse or bind time and the attacker's path never reaches a file-opening
function. The invariant holds in practice, and the exception fails closed.

Two things stop it from being the answer here. It contains only the unkeyed
class, so the mixed shape in the last table row still reads `db2` through the
full-root handle. And it still needs the routing hook at every dispatch site,
which is the entire integration cost of the pool design and the part that
actually touches the hot path. The Go-side assertion described under "Decision"
covers both classes, covers the arcx engine as well, and needs one insertion
point rather than fourteen, which is why it is preferred. If that assertion
proves unworkable, this variant is the fallback worth building, and it is
recorded in #764 rather than discarded.

### The cost reasons

- **Threads multiply.** `configureDatabase` issues `SET GLOBAL threads` per
  instance from `database.thread_count`, which defaults to `runtime.NumCPU()`,
  and DuckDB spawns the workers eagerly. The cost is one OS worker thread per
  configured DuckDB thread, so it scales with the host: measured 15 per
  instance at `threads=16` (6 baseline, 21 with one instance, 126 with eight),
  and 7 per instance at the default on an 8-core box. Because the default is
  `NumCPU`, an eight-entry pool on a 64-core host is roughly 512 DuckDB worker
  threads in one process.
- **Memory budget multiplies.** `memory_limit` is per instance, so N instances
  can allocate N times the configured budget before any of them spills, which
  removes DuckDB's spill-to-disk pressure valve and converts graceful
  degradation into an OOM kill. Arc already fixed this exact class of bug once
  for compaction subprocesses (`internal/config/config.go`, the derived
  per-subprocess budget). Dividing the limit by the pool bound instead makes
  every ordinary single-tenant query N times more likely to spill.
- **The credential refreshers collide.** `d.s3Refreshers` is keyed by secret
  name and each refresher is bound to one handle, and `startRefresher` stops
  any existing refresher with the same name. A second instance needing
  `arc_s3_primary` would stop the first instance's refresher, so that handle
  keeps a static copy of an STS credential until it expires while `/health`
  still reports healthy. Correcting this needs per-instance keying and
  multiplies the IMDS/STS call rate by the pool size.
- **A pool miss lands on the query hot path.** A miss pays extension
  INSTALL/LOAD (including `cache_httpfs` from the community repo and arcx with
  a 30s timeout) plus secret creation, whose first credential resolve is
  synchronous and bounded at 10s. A thrashing pool pays that continuously.
- **Eviction does not bound anything.** Closing a handle with a live streaming
  result does not free it: DuckDB refcounts internally, so reads continue
  correctly, and the evicted instance keeps its buffer pool and its worker
  threads until the query finishes. The bound that justifies the pool does not
  actually bound memory or threads.

### It also would not cover both engines or all dispatch sites

The plan assumed two execution seams. There are at least nine, and one of them
holds a separately captured handle: `query.NewParallelExecutor(db.DB(), ...)`
stores the raw `*sql.DB` at handler construction, and serves the largest
queries (those over the partition threshold). Any seam left on the unscoped
handle is a complete bypass.

Separately, arcx is a second execution engine. On an `arcx_engine` build with
`ARC_ROUTER=serve`, eligible queries never reach DuckDB at all; arcx does its
own file reads in Go, bounded by the `AllowedDirs` slice it is handed from
`(*database.DuckDB).AllowedDirectories()`. DuckDB-level scoping enforces
nothing on that path.

## Why `lock_configuration` was not enabled either

`SET GLOBAL lock_configuration = true` looked like free hardening: after
lockdown, `SET GLOBAL disabled_filesystems = 'LocalFileSystem'` still succeeds
and bricks the instance for its own reads, and `lock_configuration` blocks it
(along with `memory_limit` and `threads`) while leaving allowed reads working.
It is not reachable from user SQL today, since `dangerousSQLPattern` rejects
bare `SET`, so it would be defence in depth for exactly the "validator
bypassed" threat model of this issue.

It was rejected because it also blocks `parquet_metadata_cache`, and Arc
toggles that setting at runtime in `ClearHTTPCache` after every delete,
compaction, and retention pass to drop cached metadata pointing at files that
no longer exist. Under `lock_configuration` that toggle fails permanently, so
the cache is never reset.

There is no lock-immune substitute in 1.5.5. `PRAGMA disable_object_cache` and
`PRAGMA enable_object_cache` do survive the lock, but they are inert: neither
changes `parquet_metadata_cache`, and neither changes `enable_object_cache`
itself. `lock_configuration` is also one-way, so it cannot be lifted for the
toggle and restored.

Enabling it would trade a hardening that is only reachable after another
control has already failed for a correctness regression operators would hit on
every delete. If `ClearHTTPCache` is ever reworked to avoid the setting toggle,
this is worth revisiting.

## Decision

No per-tenant `allowed_directories` scoping. The startup lockdown stays as it
is, and this note is the record of why.

The enforcement point worth building instead is in Go, not in DuckDB: assert,
on the final rewritten SQL immediately before dispatch, that every path literal
it contains is one Arc itself emitted and lies under an allowlist derived from
the RBAC decision just made. That inverts the broken dependency, because it
inspects the SQL actually being executed rather than what the reference
extractor managed to parse, so the unkeyed `read_parquet` case is caught by
construction. It is also the mechanism arcx already uses, so it can cover both
engines, and it needs no second instance and therefore none of the thread,
memory, credential, or latency costs above. Its weakness is honest: it is Arc
code checking Arc code rather than the engine refusing. It is tracked in #764,
which also carries #641's three unmet acceptance criteria.

## When to revisit

`TestSandboxScopingConstraints` pins constraints 1, 2, 3 and 6.
`TestNoLocalScopeSettingAffectsFileAccess` pins constraint 5, both the set of
LOCAL-scope settings and the fact that the sandbox still gates
`profile_output`. Constraint 4 (the DSN rejection) is not pinned, because it is
a driver-level behaviour rather than a DuckDB one and it is not what the
decision rests on. `TestLockConfigurationStillBlocksCacheInvalidation` pins the
`lock_configuration` trade-off.

If a DuckDB bump makes `allowed_directories` settable per session or mutable
after lockdown, or gives `lock_configuration` a way to coexist with cache
invalidation, those tests fail and say so, and this note plus #641 should be
reopened.
