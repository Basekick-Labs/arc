# Arc v2026.06.1 Release Notes

> **Status:** In progress. Entries are added as PRs land.

## Security fixes

### DuckDB I/O sandbox closes arbitrary file-read by any authenticated caller

Reported by [Alex Manson](https://neurowinter.com/) ([@NeuroWinter](https://github.com/NeuroWinter)) — thank you for the detailed report and proof-of-concept.

An external security report against the v26.05.1 main branch found that any token holding even an empty `permissions: []` set could read arbitrary local files through DuckDB's I/O function family — `read_csv_auto`, `read_json`, `read_text`, `read_blob`, `glob`, `parquet_metadata`, `parquet_schema`, `read_xlsx`, etc. The existing user-SQL denylist blocked only `read_parquet(` and `arc_partition_agg(`, and RBAC table-level checks inspected `FROM`/`JOIN` clauses, so a scalar table function in the `SELECT` list slipped past both layers.

Impact on a deployment with auth enabled but RBAC not subscribed: a single-line POST to `/api/v1/query` returned the contents of `auth.db` (bcrypt hashes plus legacy SHA-256 rows), `arc.toml` (S3 secrets), TLS private keys, `/proc/self/environ`, cross-tenant Parquet files, and — when `httpfs` was loaded — instance-metadata IPs over SSRF.

26.06.1 replaces the denylist with a structural fix at the DuckDB layer: after every `INSTALL`/`LOAD` and every `SET GLOBAL` Arc itself needs has run, the startup path sets

```sql
SET GLOBAL allowed_directories = ['<local_storage_root>/', '<temp_directory>/', '<upload_dir>/', '<compaction_temp>/', 's3://<hot_bucket>/<prefix>/', 's3://<cold_bucket>/<prefix>/', 'azure://<container>/', 'azure://<cold_container>/']
SET GLOBAL enable_external_access = false
```

After this flip DuckDB refuses to open any file outside the allowlist, refuses any further `INSTALL`/`LOAD`, and rejects attempts to re-enable external access at runtime. Already-loaded extensions (httpfs, azure, cache_httpfs, arcx) remain fully callable — `enable_external_access` is checked at extension-load time, file access at file-open time. The allowlist is enforced uniformly across every pool connection (DuckDB's `ExtensionManager` and `SET GLOBAL` propagate database-wide), so a per-connection regression is not possible. The flip is read back and verified at startup so a future DuckDB release silently rejecting it would surface as a startup error rather than a silently-broken sandbox.

A startup-time guard rejects operator-supplied paths that contain any Unicode control character (Cc — `\0`, `\n`, `\r`, `\t`, vertical tab, form feed, 0x7F), formatting character (Cf — LRM/RLM/LRE/RLE/PDF/LRO/RLO/LRI/RLI/FSI/PDI, ZWSP/ZWNJ/ZWJ, BOM, soft hyphen), or line/paragraph separator (Zl/Zp — U+2028/U+2029) before they reach the SQL literal — closing both newline injection and bidi-override log-spoofing surfaces. The upload directory used for multipart imports is created with mode `0700`, chmod'd back to `0700` if it already existed with looser permissions, and rejected at startup if a symlink has been pre-staged in its place.

**Operator-facing change — paths must resolve to absolute**: `storage.local_path`, `database.temp_directory`, `compaction.temp_directory`, and (when arcx is enabled) the arcx storage root are now all resolved to absolute, forward-slash paths before being passed to DuckDB's allowlist. The defaults (`./data/arc`, `./.tmp`, `./data/compaction`) continue to work — they're resolved against the process working directory at startup. Operators who set these to a non-absolute path in `arc.toml` will see the resolved absolute path in the startup log line `DuckDB external access locked down (sandbox active)`.

**Import upload directory moved**: multipart uploads landed in `os.TempDir()` (typically `/tmp` or `/var/folders/.../T/`) before 26.06.1. They now land in a dedicated `arc-uploads` subdirectory under the operator-configured temp directory — `${database.temp_directory}/arc-uploads`. Existing imports continue to work; deployments that scrape `/tmp` for monitoring no longer see Arc upload files.

**Compaction and DELETE on S3 backends use the same allowlisted staging area** so their `COPY ... TO` writes succeed under the sandbox. No operator action required — the wiring is internal.

**Profile-mode queries** write the JSON profiling output under `${database.temp_directory}` instead of `os.TempDir()` so `PRAGMA profiling_output` writes through an allowlisted path. Profile data was already silently empty in some DuckDB releases that route the profile writer through `OpenerFileSystem`; this fix is preemptive.

The arcx loader was simplified: the previous per-connection `connInitFn` (which executed `LOAD '<arcx-path>'` on every new pooled connection) has been replaced with a one-shot LOAD during `configureDatabase`, plus `SET GLOBAL "arcx.storage_root" = '<path>'`. Both propagate database-wide. The per-connection LOAD was a leftover from before we'd verified DuckDB's extension model; functionally identical, simpler to reason about.

Tests added: `TestSandbox` (CVE reproduction + full I/O family + SSRF + `COPY TO` local + `COPY TO 's3://...'` + `EXPORT DATABASE` outside allowlist + `INSTALL` after lockdown + cross-connection enforcement + `range()` remains callable + lockdown is one-way), `TestBuildAllowedDirectories` (12 table cases covering hot/cold S3 dedup, leading/interior-slash collapse, trailing-slash idempotence, empty-config behavior), `TestSandboxEmptyAllowlistLogsButDoesNotPanic`. The existing arcx tests confirm `arcx_version()` and `SET GLOBAL arcx.storage_root` propagate across 3–4 concurrent pool connections.

### Go pprof endpoints no longer reachable from the public API port

Reported by [Alex Manson](https://neurowinter.com/) ([@NeuroWinter](https://github.com/NeuroWinter)) — thank you for the detailed report.

Pre-26.06.1, `internal/api/server.go` called `app.Use(pprof.New())` unconditionally on the public Fiber app, and `cmd/arc/main.go` added `/debug/pprof` to the auth middleware's `PublicPrefixes` list. The combined effect: any network-reachable caller — no token, no auth header, no anything — could fetch:

- `GET /debug/pprof/heap` — leaks in-memory state (live SQL strings, decoded msgpack records, decompressed request bodies, cached `*TokenInfo` derived from plaintext-token hashes in the auth cache).
- `GET /debug/pprof/goroutine?debug=2` — leaks every goroutine's call stack, identifying internal code paths and surfaces.
- `GET /debug/pprof/profile?seconds=N` — pins a CPU core for arbitrary duration. One request = minutes of server CPU. Trivial DoS amplification.
- `GET /debug/pprof/trace?seconds=N` — same CPU-burn profile via a different handler.

26.06.1 removes pprof from the public Fiber app entirely. The endpoints are now opt-in via `ARC_DEBUG_PPROF=1`, and when enabled they bind to a separate `127.0.0.1:6060` listener (override via `ARC_DEBUG_PPROF_ADDR`; a non-loopback bind additionally requires `ARC_DEBUG_PPROF_ALLOW_NON_LOOPBACK=1` so a single typo in the address knob cannot expose pprof to the network). The localhost listener registers its handlers on a private `*http.ServeMux`, NOT on `http.DefaultServeMux` — so even if a future PR adds an `http.Server` somewhere with `Handler: nil`, that server will not unintentionally serve pprof (verified at merge time that no such caller exists in the binary). The listener is wired into the existing shutdown coordinator at `PriorityHTTPServer` and shuts down via `srv.Close()` (force-close, not `srv.Shutdown`) so an in-flight long-running `/debug/pprof/profile?seconds=N` capture cannot pin the coordinator's shared 30-second shutdown budget and starve downstream shutdown hooks. Server-side timeouts (`ReadHeaderTimeout=5s`, `WriteTimeout=10m`, `IdleTimeout=60s`) bound slow-client attacks on the debug surface.

Operator-facing changes:

- **Default behavior changed**: pprof is no longer reachable on Arc's API port (`:8000` by default). Existing deployments that relied on `curl http://arc:8000/debug/pprof/heap` will start getting `404 Not Found`. Set `ARC_DEBUG_PPROF=1` and reach pprof on `127.0.0.1:6060` instead.
- **`/metrics` is unchanged**: Prometheus scrapers continue to work as before. Only `/debug/pprof/*` moved.
- **Non-loopback bind requires a second opt-in**: `ARC_DEBUG_PPROF_ADDR` accepts any bind string Go's `net.Listen` understands, but a non-loopback override (e.g. `0.0.0.0:6060`) additionally requires `ARC_DEBUG_PPROF_ALLOW_NON_LOOPBACK=1`. Without that second env var Arc logs an `Error` and refuses to start the pprof listener; with it, Arc logs an `Error` (not Warn) at startup naming the bound address so cross-host exposure shows up in default alerting policies.

A **defense-in-depth fix to the auth middleware's `PublicPrefixes` matcher** is also included. The previous `strings.HasPrefix(path, prefix)` would match `/metrics` against `/metrics`, `/metrics/prometheus`, AND `/metricsX`, `/metrics-secret`, etc. — any byte-prefix match silently bypassed auth. Three changes:

1. **Anchored prefix match**: the matcher now requires exact-equal or true-subdirectory (`prefix + "/"`); a sibling path with the same prefix bytes no longer slips through. Same shape as the prefix-match gap gemini-code-assist flagged on PR #442's `deniedRoots` check.
2. **Path normalisation before match**: the matcher runs `path.Clean(c.Path())` first, so non-canonical request shapes like `/metrics//foo`, `/metrics/./x`, `/metrics/../sensitive` are normalised to their canonical form before the bypass branch checks them. Without normalisation, an attacker-controlled `/metrics/../api/v1/query` lexically starts with `/metrics/` and would slip past the anchored check; after normalisation it becomes `/api/v1/query` and correctly requires auth.
3. **Empty-prefix guard**: an empty string in `PublicPrefixes` (no legitimate config has one, but a future bug — e.g. an env-var split producing an empty slice entry — could) would otherwise short-circuit every request because `HasPrefix(anyPath, "")` is true. The matcher now skips empty entries.

With `/debug/pprof` removed from the public prefix list, items 1 and 2 are not currently reachable for any production route — the fixes are guards for any prefix added in the future.

Tests added: `TestServer_PprofNotRegisteredOnPublicApp` (12 pprof paths against the public Fiber app, all must 404), `TestMiddleware_PublicPrefixes_AnchoredMatch` (10 subtests: exact match + trailing-slash match + true subdir + deep subdir bypass + 3 sibling-byte-prefix shapes that must require auth + 2 parent-traversal escape shapes that must require auth + empty-prefix guard), plus the new `cmd/arc/debug_pprof_test.go` (no-op when disabled, binds-and-serves when enabled, `isTruthy` env-var contract, `isLoopbackBindAddr` detection incl. fail-closed on unresolvable hosts).

### `/api/v1/internal/cache/invalidate` now requires HMAC-SHA256 cluster auth

Reported by [Alex Manson](https://neurowinter.com/) ([@NeuroWinter](https://github.com/NeuroWinter)) — closes audit finding #3.

Pre-26.06.1 the post-compaction cluster cache-invalidate endpoint was gated by a static header (`X-Arc-Internal: cache-invalidate`). Any network-reachable caller — no token, no credentials — could spam the endpoint, forcing DuckDB's `cache_httpfs` glob results, metadata cache, and file-handle cache to repopulate. On S3-backed deployments this is a cost-amplification surface (`ListObjectsV2` calls multiply) and a latency-amplification surface (p99 spikes during repopulation). The static header carried no secret and was logged in the cluster fan-out code, so it offered no protection beyond raising the bar for someone reading network traces.

26.06.1 replaces the static header with HMAC-SHA256 over `{nonce, sender nodeID, clusterName, timestamp}` keyed by the cluster shared secret (`cluster.shared_secret` in `arc.toml`). Five request headers carry the auth state:

- `X-Arc-Node-ID` — sender's node ID, bound into the MAC.
- `X-Arc-Cluster` — cluster name, bound into the MAC; receiver also checks it matches its own cluster name before HMAC computation (cluster A's MAC cannot be replayed against cluster B).
- `X-Arc-Nonce` — 32 random bytes, hex-encoded.
- `X-Arc-Timestamp` — unix seconds; ±5-minute freshness window matches the project's other HMAC endpoints.
- `X-Arc-HMAC` — `hex(HMAC-SHA256(secret, "cache-invalidate:" + nonce + ":" + nodeID + ":" + clusterName + ":" + timestamp))`.

The label prefix `cache-invalidate:` in the signed message is distinct from `ComputeForwardHMAC` (no prefix) / `ComputeFetchHMAC` (path-bound) / `ComputeHMAC` (no prefix). A leaked MAC for one endpoint can NOT be replayed against another even within the freshness window — verified by `TestCacheInvalidateHMAC_LabelBinding_NoCrossEndpointReplay`. Replay within the same endpoint is blocked by an in-process `NonceCache` (5-minute TTL, lazy eviction).

Operator-facing changes:

- **OSS deployments are unaffected.** Post-compaction cache invalidation in OSS happens in-process (`db.ClearHTTPCache()` + `queryHandler.InvalidateCaches()` are called directly from the compaction callback). No HTTP request is issued; no cluster-internal endpoint is touched. The `/api/v1/internal/cache/invalidate` route exists but refuses every request with 403 when `cluster.shared_secret` is unset.
- **Cluster deployments without `cluster.shared_secret` configured**: the post-compaction fan-out is now SKIPPED instead of sending the static-header request that would have been refused anyway. The skip is logged once per process at the first post-compaction trigger (via `sync.Once`) so the message does not repeat per compaction. Set `cluster.shared_secret` to re-enable cross-node cache invalidation.
- **Cluster deployments WITH `cluster.shared_secret` configured**: no change from the operator's side — every node already uses the same secret for leader-forwarding and peer-fetch HMACs; this endpoint now uses the same secret.
- The receiver returns 403 (not 401) for every rejection path — missing headers, wrong secret, stale timestamp, future timestamp, self-addressed request, wrong cluster name, replay. Uniform rejection prevents an attacker from probing to distinguish "no secret configured" from "wrong MAC". Every rejection path emits a Debug log including the remote IP plus whatever non-secret context is known (cluster name, node ID, timestamp string); operators chasing a misconfiguration can flip `internal/api`'s logger to Debug. Debug-level on purpose: at Warn the endpoint would amplify a network flooder into a log-DoS.
- A request claiming `X-Arc-Node-ID` equal to the receiver's own node ID is refused: local invalidation runs in-process, never over HTTP, so a self-addressed HTTP request is either a misconfiguration or a confused attacker.

**Rolling upgrade.** During a rolling restart from 26.05.x → 26.06.1, cross-node cache invalidation is briefly disrupted in both directions: 26.05.x senders use the static `X-Arc-Internal` header that 26.06.1 receivers now reject, and 26.06.1 senders emit five HMAC headers that 26.05.x receivers never check. Practical impact is bounded by the duration of the rolling restart — for each affected reader, the worst case is one compaction cycle of stale `cache_httpfs` glob results, which surfaces as a brief uptick in 404s and p99 on that node until the next compaction (the in-process invalidation on each node still runs locally). Either ramp through the upgrade quickly or schedule it during a low-write window so compaction triggers are infrequent.

Tests added: `TestComputeCacheInvalidateHMAC_Determinism` (pins the wire format + MAC reference value so a future refactor that breaks wire-compat fails CI), `TestValidateCacheInvalidateHMAC_{Valid, WrongSecret, StaleTimestamp, FutureTimestamp, FieldBinding}`, `TestCacheInvalidateHMAC_LabelBinding_NoCrossEndpointReplay` (cross-endpoint replay protection, now covers Forward + Fetch + Join in both directions), and 9 handler-level tests covering every rejection path plus the valid-auth proxy + replay-after-success.

## Bug fixes

### Parser no longer mis-resolves bare `time` column inside `EXTRACT(YEAR FROM time)`

The regex-based query rewriter previously matched the `FROM` keyword inside `EXTRACT(YEAR FROM time)` (and the same shape in `SUBSTRING(s FROM 1 FOR 3)`, `TRIM(LEADING '0' FROM x)`, `OVERLAY(s PLACING 'x' FROM 2)`) and rewrote the column reference as a measurement, producing:

```
Binder Error: Function "read_parquet" is a table function but it was used as a scalar function.
LINE 1: SELECT EXTRACT(YEAR FROM read_parquet('/app/data/arc/.../time/**/*.parquet'...
```

`time` is the canonical column name across InfluxDB / Telegraf / Prometheus-derived schemas, so every customer migrating from those systems hit this. Workarounds were `YEAR(time)`, `date_trunc('year', time)`, or quoting / qualifying the column.

26.06.1 adds a pre-pass that masks `FROM` keywords inside the argument list of `EXTRACT`, `SUBSTRING`, `TRIM`, and `OVERLAY` before the table-rewrite regex runs, then restores them afterwards. The masker tracks paren depth so nested calls like `EXTRACT(YEAR FROM CAST(t AS DATE))` are handled correctly. Both the standard and `x-arc-database` header-optimized rewriter paths are covered, and the fast paths bail to the masking path when these functions are present. The single-table query optimization continues to apply for queries that don't use these functions.

**Overhead** (per cache-miss query — cached queries skip the entire rewriter): ~350 ns and **0 allocations** for queries that don't use these functions (the common case); ~625 ns and ~900 bytes for queries that do. The rewriter cache absorbs repeated identical queries, so this is paid once per unique SQL string. Bench numbers from `internal/sql/mask_test.go` on an M3 Max:

```
BenchmarkContainsFromKeywordFunction_Miss        348 ns/op      0 B/op   0 allocs/op
BenchmarkContainsFromKeywordFunction_Hit          21 ns/op      0 B/op   0 allocs/op
BenchmarkMaskFromKeywordsInFunctionBodies_Miss   348 ns/op      0 B/op   0 allocs/op
BenchmarkMaskFromKeywordsInFunctionBodies_Hit    316 ns/op    336 B/op   4 allocs/op
BenchmarkUnmaskFromKeywordsInFunctionBodies      285 ns/op    560 B/op   3 allocs/op
```

Tests added: `TestMaskFromKeywordsInFunctionBodies`, `TestUnmaskAfterLengthChangingRewrite`, `TestContainsFromKeywordFunction` in `internal/sql/mask_test.go`; `TestConvertSQLToStoragePaths_FromKeywordFunctions`, `TestConvertSQLToStoragePaths_ExtractAfterFrom`, `TestConvertSQLToStoragePathsWithHeaderDB_FromKeywordFunctions` in `internal/api/query_test.go`.

This is a narrow regex pre-pass, not a full SQL-parser swap. The fix triggered an evaluation of replacing the regex rewriter with a real SQL parser — no Go SQL parser currently handles DuckDB's full syntax (lambdas `x -> y`, list literals `[1,2,3]`, `QUALIFY`, `EXCLUDE`/`REPLACE`, FROM-first, `:=` named args, `PIVOT`/`UNPIVOT`). A proper parser migration is tracked as a separate, larger initiative.

### Arrow IPC responses now carry server-side execution time

The HTTP/JSON query endpoint already reports `execution_time_ms` in the response body, but the Arrow IPC endpoint (`/api/v1/query/arrow`) exposed no server-side timing. Clients had to rely on wall-clock measurement, which overstates Arc's actual performance when the network is in the way — a 2,830ms server-side aggregation looked like 4,014ms from Costa Rica against a US-hosted demo box.

26.06.1 publishes an `Arc-Execution-Time-Ms` HTTP response trailer at the end of every Arrow IPC stream. The trailer carries the same integer that the server logs internally. Clients consume the full Arrow stream and then read the trailer — e.g. in Python:

```python
import pyarrow as pa, requests
r = requests.post(
    "http://arc:8000/api/v1/query/arrow",
    json={"sql": "SELECT count(*) FROM cpu WHERE time >= now() - INTERVAL 1 DAY"},
    headers={"Authorization": f"Bearer {TOKEN}", "x-arc-database": "default"},
    stream=True,
)
reader = pa.ipc.open_stream(r.raw)
for batch in reader:
    ...
print("server-side ms:", r.headers.get("Arc-Execution-Time-Ms"))
```

The trailer is also emitted on the error path (with time-until-failure) so partial-stream timing is still observable. Trailers require HTTP/1.1 chunked transfer or HTTP/2 — both already in use by Arc's fasthttp-backed Fiber stack. Clients that ignore trailers degrade gracefully to wall-clock measurement.

The JSON path (`/api/v1/query`) is unchanged — `execution_time_ms` was already in the response body.

### Orphaned DuckDB spill files now cleaned at startup

DuckDB writes query spill files (`duckdb_temp_storage_*.tmp`) when intermediate state for a HASH_GROUP_BY, sort, or join exceeds `memory_limit`. On graceful close DuckDB unlinks them itself. On `kill -9`, OOM-kill, container restart, or crash the unlink never runs and the files survive — they are not `O_TMPFILE`. A development machine accumulated **40 GB** of orphaned spill files across 17 days; the largest single file was 8.83 GB.

26.06.1 adds the missing cleanup:

- **New config: `database.temp_directory`** (default `./.tmp`, resolved to an absolute path at startup). DuckDB is pinned to this directory via `SET GLOBAL temp_directory`, so the path the engine spills to and the path the sweep walks are guaranteed to match. The directory is created with `0o700` so intermediate query state — which can contain post-filter, post-join rows — is not world-readable on shared hosts.
- **Startup sweep:** before `database.New` opens the connection pool, Arc walks `temp_directory` and removes regular files matching `duckdb_temp_storage_*.tmp` that are older than 60 seconds. The age threshold protects any concurrent arc instance; the durable invariant is "one arc per `temp_directory`," which operators should preserve.
- **Single summary log:** per-file failures (permission denied, race with a concurrent unlink) are counted and surfaced as one Warn line with a sample, not one Warn per file. On a recovery from a large leak this is the difference between five thousand log lines and one.

Subdirectories are deliberately ignored — DuckDB 1.5.1 uses a flat layout. If a future DuckDB version nests per-query subdirs, the regression will surface as orphans reappearing, and the sweep will need to switch to `filepath.WalkDir`. A test in `internal/database/spill_cleanup_test.go` pins this expectation.

The previous draft also added a post-`db.Close()` sweep; review caught that it was effectively dead code (DuckDB had already unlinked, and the 60 s mtime guard would skip anything still in flight) and risked stalling shutdown past `systemd`'s `TimeoutStopSec`. It was dropped before merge.

### Startup banner now invites OSS operators to Arc Enterprise

When Arc starts without a working Enterprise license — either because no `license.key` was configured, or because activation/verification failed and `licenseClient` was reset to nil — startup logs now include a single `Info` line:

```
Running Arc OSS — try Arc Enterprise for tiering, clustering, RBAC, audit, and arcx
  url=https://basekick.net/enterprise
```

Fires exactly once per startup. Placed after both no-license code paths so the message is the same regardless of how we got there. Operators who have a license configured see it validated successfully and the invite line never appears.

## Experiments

### `POST /api/v1/query/msgpack` — columnar MessagePack query response (experimental)

A new endpoint streams query results as **columnar MessagePack** — `data` is an array of per-column arrays, not the row-oriented `[[v,v,v], [v,v,v]]` shape clients usually expect. Arc's storage, ingest, and DuckDB execution are all columnar; the query response now matches. Same execution pipeline as `POST /api/v1/query` (auth, RBAC, governance, ctx-timeout, profile, slow-query logging, query-registry callbacks); only the response serialization differs. Gated by the `duckdb_arrow` build tag; without it the route returns 501.

**End-to-end head-to-head** (same query suite via `benchmarks/query_suite/main.go`, 5 iterations per query, against a 393M-row `cpu` measurement on a single Arc instance, p50 latency in milliseconds):

| Query | JSON | msgpack (columnar) | Arrow IPC | msgpack vs JSON | msgpack vs Arrow IPC |
|---|---:|---:|---:|---:|---:|
| `LIMIT 1K`   |  14.8 |  18.2 |  14.0 | (noise floor) | (noise floor) |
| `LIMIT 10K`  |  18.4 |  16.6 |  14.7 | 1.11×         | 0.89× |
| `LIMIT 100K` |  48.1 |  33.2 |  31.0 | **1.45×**     | 0.93× |
| `LIMIT 500K` | 173.2 |  81.1 |  61.1 | **2.14×**     | 0.75× |
| `LIMIT 1M`   | 334.2 | **133.6** | 105.4 | **2.49×** | **0.78× of Arrow IPC** |

DuckDB-bound queries (`SUM/AVG/MIN/MAX`, `Percentile (p95)`, `GROUP BY host + hour`, etc.) are within run-to-run noise across all three wire formats — serialization is sub-millisecond and DuckDB execution dominates.

**The columnar shape was the win.** An earlier draft of this endpoint used a row-oriented envelope (`data: [[v,v,v], ...]`) and delivered 1.74× over JSON on `LIMIT 1M`. The per-cell type-switch dominated encode CPU: 1M rows × 7 cols = 7M type assertions. The columnar redesign hoists the type-switch outside the row loop — one switch per *column*, then a typed loop over the column's values — and delivered 2.49× over JSON, closing 78% of the gap to Arrow IPC.

**Encoder microbench** (added under `benchmarks/msgpack_bench/msgpack_query_test.go`, M3 Max, columnar shape, against the same `QueryResponse` fixtures used by the JSON benchmarks):

```
BenchmarkJSON_Segmentio_Marshal_SmallQuery     1955 ns/op    1120 B/op  2 allocs
BenchmarkMsgpack_Stream_SmallQuery              441 ns/op  1680 MB/s   0 B/op  0 allocs   (4.4× faster)
BenchmarkJSON_Segmentio_Marshal_LargeQuery   325532 ns/op  140010 B/op  2 allocs
BenchmarkMsgpack_Stream_LargeQuery            44349 ns/op  1897 MB/s   9 B/op  0 allocs   (7.3× faster)
```

Wire size shrinks 1.34× (small, 992→739 bytes) and 1.57× (large, 131784→84159 bytes) vs JSON. The microbench overstates the production win because it doesn't model DuckDB Arrow record iteration, Fiber chunked-transfer, or TCP — see the next section.

**Why the production gap doesn't match the microbench (and why we stopped optimizing).** A CPU profile (`go tool pprof` against the live server under 100 concurrent `LIMIT 1M` queries on a 14-core box) shows the Go-side encoder accounts for **zero measurable samples**. The 25-second profile captured 100ms of total Go-side work; 30ms of that is `runtime.cgocall` (DuckDB query execution through the Go/C boundary, opaque to Go pprof) and 30ms is `syscall.rawsyscalln` (TCP writes through fasthttp's chunked encoder). The msgpack encode loop, the type-switches, the bufio writes — all of them rounded to zero on the profile.

So the 28ms gap to Arrow IPC on `LIMIT 1M` lives on the **C++ side of DuckDB**, in how the Arrow record materialization differs between the two endpoints. Arrow IPC writes DuckDB's internal column buffers via `ipcWriter.Write(batch)` — effectively a memcpy of the column buffer plus a small framing header. The msgpack endpoint forces DuckDB to walk each record's cells via `c.Value(i)`, which makes DuckDB do per-cell materialization work that Arrow IPC's batch-buffer write skips entirely. **No amount of Go-side optimization closes that gap; it's structural to the columnar-encode-per-cell vs columnar-buffer-write difference.**

Three optimization rounds confirmed this empirically:

| Change | Saved on `LIMIT 1M` |
|---|---:|
| Row-oriented → columnar envelope | **56 ms** (193 → 137) |
| 4 KiB → 256 KiB `bufio.Writer` | ~4 ms (137 → 133) |
| Inner-loop tweaks (per-batch ctx check, `EncodeInt64` direct, `enc.EncodeTime` for timestamps, drop NaN check, no per-column flush) | ~0 ms (within run-to-run noise) |

The columnar redesign was the single change that mattered. Everything else was diminishing returns. The pprof data is the receipt.

**Where Arrow IPC still wins.** `LIMIT 1M` Arrow IPC 105ms vs columnar msgpack 133ms — Arrow IPC is **1.27× faster than msgpack**, **3.18× faster than JSON**. Arrow IPC remains the right choice for analytical clients (Grafana, pyarrow, polars) that can take an Arrow dependency. The msgpack endpoint targets clients that already speak msgpack or want a smaller wire than JSON without adding Arrow.

**Why "experimental".** The endpoint may move, change shape, or be removed based on production feedback. There's no operator-configurable row cap yet (governance policy is the only ceiling); if msgpack graduates we'll re-introduce a `database.msgpack_max_buffered_rows` knob. Use `--target arc-msgpack` in `query_suite/main.go` to measure against your own data and workloads before committing client code to it.

**Wire format spec.** Single msgpack map per response; `data` is columnar:

```
map(7 or 8) {
  "success":           bool                                    // true on success
  "columns":           [string, ...]                           // column names (numCols entries)
  "types":             [string, ...]                           // arrow.DataType.String() per column, parallel to columns
  "data":              [[col0_v, col0_v, ...], [col1_v, ...]]  // numCols outer entries, numRows inner
  "row_count":         uint                                    // = inner length, redundant for client convenience
  "execution_time_ms": uint                                    // millisecond integer (JSON sends a float)
  "timestamp":         string                                  // RFC3339
  "profile":           map                                     // only when ?profile=true (json-tagged QueryProfile struct)
}
```

Per-column primitive encoding:
- **Int8/16/32/64, Uint8/16/32/64**: msgpack int / uint. `Int64`/`Uint64` use `EncodeInt64`/`EncodeUint64` (always 9 bytes) to skip the library's size-class branching.
- **Float32/Float64**: msgpack float. NaN / ±Inf are emitted as IEEE 754 bits — clients receive them natively, not coerced to nil.
- **Boolean**: msgpack bool.
- **Timestamp / Date32**: msgpack native **timestamp Ext type** (4/8/12 bytes, no RFC3339 round-trip). Saves ~25 bytes per cell vs the row-oriented draft's RFC3339Nano string.
- **String / LargeString**: msgpack str.
- **Binary**: msgpack **bin** (native). Clients should treat `bin` values as untrusted bytes — they may contain terminal escape sequences depending on the column source. Do not write directly to a TTY or text log.
- **Null cells**: msgpack nil. Per-column null bitmap is encoded inline (one nil per missing cell) so clients with column-mode decoders can treat each entry uniformly.

**Important operational constraints.**

- **Buffered, not streamed.** msgpack arrays require an explicit length prefix, so the endpoint drains every Arrow batch into memory before emitting the data array. The only row ceiling is `governance.max_rows` (Enterprise token policy); without it, the response is unbounded — same hazard the JSON `database/sql` fallback carries, just sharper because msgpack materializes the whole result before sending the first byte. If msgpack graduates we'll re-introduce an operator-configurable ceiling.
- **Parallel-partition execution is bypassed.** The msgpack endpoint forces the standard Arrow dispatch even when the query would otherwise be eligible for the parallel-partition executor. Parallel queries route through the JSON-streaming merge iterator and don't share the msgpack encode path; coupling them would multiply the experimental surface area.
- **No `database/sql` fallback.** The Arrow path is the entire reason for the endpoint; falling back to `Scan`-based row iteration would defeat the contract. When the Arrow driver is unavailable (build without `duckdb_arrow`, or driver capability mismatch), the route returns `501 Not Implemented` rather than silently downgrading.
- **Errors, `SHOW DATABASES`, `SHOW TABLES`** are also encoded as columnar msgpack. The same `wire_format` request-local that routes the streaming hot path also drives a `respondError` / `respondSuccessRows` / `respondEmptySuccess` helper trio so every response from the shared `executeQuery` pipeline matches the content type the caller asked for.
- **`profile` struct tag.** The msgpack library defaults to reading `msgpack:"..."` struct tags. `QueryProfile` has only `json:"..."` tags, so the encoder calls `SetCustomStructTag("json")` once at the top of the stream to keep field names in `snake_case` on the wire. Without this the profile field ships in `CamelCase`.

**Client decode example (Python):**

```python
import msgpack, requests
r = requests.post("http://arc:8000/api/v1/query/msgpack",
                  json={"sql": "SELECT * FROM cpu LIMIT 1000"},
                  headers={"Authorization": f"Bearer {TOKEN}", "x-arc-database": "default"})
resp = msgpack.unpackb(r.content, raw=False, timestamp=3)  # timestamp=3 → datetime
ncols = len(resp["columns"])
nrows = resp["row_count"]
print(f"{nrows} rows × {ncols} cols in {resp['execution_time_ms']} ms")
# Iterate row-major if your application code expects rows:
for row_idx in range(nrows):
    row = [resp["data"][c][row_idx] for c in range(ncols)]
    ...
# Or operate column-major (faster — matches the wire format):
for col_idx, col_name in enumerate(resp["columns"]):
    values = resp["data"][col_idx]
    ...
```

**To reproduce the numbers.** Encoder microbench + wire sizes:

```
cd benchmarks/msgpack_bench
go test -v -run TestWireSize                                # prints byte sizes
go test -bench='Marshal_|Stream_|Unmarshal_' -benchmem -benchtime=1s
```

End-to-end against a running Arc server:

```
go build -tags=duckdb_arrow -o arc ./cmd/arc
ARC_AUTH_ENABLED=false ./arc &
go run benchmarks/query_suite/main.go --target arc-msgpack --measurement cpu --iterations 5
# Compare to:
go run benchmarks/query_suite/main.go --target arc        --measurement cpu --iterations 5  # JSON
go run benchmarks/query_suite/main.go --target arc-arrow  --measurement cpu --iterations 5  # Arrow IPC
```

## Hardening

### HTTP server now supports binding to a specific host/address — closes #439

The HTTP listener has always bound the wildcard address (`":<port>"`), giving operators no way to restrict the bind from the config file. Deployments wanting loopback-only behavior (Arc fronted by a sidecar adapter on the same host) reached for `systemd` `IPAddressDeny`, `ufw`, or a reverse proxy. The `[server]` config block was missing the corresponding knob.

26.06.1 adds `server.host` to the `[server]` config block, plumbed through to the Fiber listener via `net.JoinHostPort`. Any address Go's `net` package recognizes works — IPv4 literals, IPv6 literals (write them without brackets — the server adds them when constructing the listen address; surrounding brackets in user input are stripped defensively), hostnames, the explicit IPv4 wildcard `0.0.0.0`, the explicit IPv6 wildcard `::`. The matching env override is `ARC_SERVER_HOST`.

**Default is empty string** — the listener constructs `":<port>"`, byte-identical to the historical `fmt.Sprintf(":%d", port)`. This preserves Linux dual-stack wildcard behavior (IPv4 + IPv6 via IPv4-mapped addresses); explicit `"0.0.0.0"` would force IPv4-only and silently break IPv6 clients on upgrade, so it's an opt-in. **Zero behavioral change on the listener for operators who don't touch their config.**

```toml
[server]
host = "127.0.0.1"   # loopback-only
# host = "::1"        # IPv6 loopback
# host = "192.0.2.10" # specific NIC
# host = "0.0.0.0"    # explicit IPv4-only wildcard
# host = "::"         # explicit IPv6-only wildcard
# host = ""           # default; dual-stack wildcard (matches pre-26.06.1 behavior)
```

The startup log line now also reports the bound host, so `journalctl -u arc | grep 'Starting Arc server'` answers "what did we bind to?" without grepping config files:

```
INF Starting Arc server component=api-server host=127.0.0.1 port=8000 tls_enabled=false protocol=HTTP
```

Operators currently relying on the `systemd` `IPAddressDeny` workaround can keep it — `ss -ltnp` will now show the bind address Arc actually opened, but the systemd filter remains valid defense-in-depth.

### Hard Query Gating During Replication Catch-Up (Enterprise, opt-in) — closes #392

Reader nodes in a clustered Arc Enterprise deployment previously accepted queries the moment they started, even while peer file replication was still pulling Parquet files the rest of the cluster already had. The Raft manifest knew about the missing files; the local storage didn't have them yet; `read_parquet()` globbed against local storage rather than the manifest. The result was **silent partial results** during the catch-up window. The Phase 3 release explicitly deferred this fix; 26.05.1 closed half the gap (WAL replication makes unflushed writer data queryable on readers within milliseconds), but flushed Parquet files still depended on async background pullers.

26.06.1 closes the remaining gap behind a single config flag.

**Configuration:**

- `cluster.query_gate_on_catchup` (default `false`) — when true, all user-facing read endpoints (`/api/v1/query`, `/api/v1/query/arrow`, `/api/v1/query/estimate`, `/api/v1/query/:measurement`, `/api/v1/measurements`) return `503 Service Unavailable` until peer file replication has fully converged. Off by default to preserve existing behavior; operators who want correctness over availability flip it on.

**Readiness signal:** the gate consumes a predicate scoped specifically to the **startup catch-up batch**, not to all puller activity. This is the load-bearing detail. A naive "wait for everything to settle" predicate would mean the reader returns 503 every few seconds in normal operation, since steady-state ingest constantly puts new files in flight. The gate's job is "the reader has finished bootstrapping its view of the manifest as of startup," not "no pulls are happening anywhere right now."

26.06.1 introduces `Puller.FullyCaughtUp()`, which requires:

1. The startup catch-up walker has completed (`catchupCompletedAt > 0`).
2. No paths the walker tagged are still in flight (`catchupInflight == 0`). Steady-state pulls from reactive FSM callbacks are deliberately excluded — they're tracked separately and don't affect the gate.
3. No catch-up-batch pulls failed after retries (`catchupFailed == 0`).
4. No catch-up-batch pulls were dropped due to queue saturation (`catchupDropped == 0`).

The walker tags each path it enqueues so workers can attribute outcomes correctly. Each worker tracks its own success/failure outcome in a local variable (not a global counter delta) so concurrent workers cannot cross-pollinate failures. The catch-up tag is also checked at outcome-time inside the worker's defer rather than snapshotted at entry, so a tag the walker added *after* a reactive worker began processing the same path is still observed when the worker decides whether to record a failure. Failures and drops outside the catch-up window do not affect the gate — they're operational concerns surfaced via `Stats()` but not correctness blockers, since by the time the catch-up batch has settled the reader has reconciled its view of the manifest as of walker start.

`/api/v1/cluster/status` keeps the existing `failed` / `dropped` / `pulled` / `skipped_dup` keys with their original cumulative semantics so dashboards landed in earlier releases continue to report whole-puller-lifetime numbers. The new catch-up-scoped values are exposed under explicit `catchup_failed` / `catchup_dropped` / `catchup_inflight` keys for new dashboards that want gate-relevant numbers.

**Self-heal**: catch-up failures and drops both clear without a process restart. When a later pull succeeds for a previously-affected path (a reactive FSM callback re-enqueueing after the underlying issue resolves, or a subsequent catch-up scan), the corresponding scoped counter decrements and the gate re-opens automatically. Both `catchupFailed` and `catchupDropped` track affected paths in dedicated sets (`catchupFailedPaths`, `catchupDroppedPaths`) so the worker's success path can attribute a successful pull back to the original failure or drop and remove it from the count.

`Coordinator.ReplicationReady()` delegates to `FullyCaughtUp()`. OSS / standalone deployments (no puller) are always ready, so the gate is a no-op there.

**Configuration validation**: when `cluster.query_gate_on_catchup=true` is set together with `cluster.replication_catchup_enabled=false` (the emergency off-switch for pathologically large manifests), the catch-up walker never runs and the gate would never clear. Arc detects this combination at startup, logs a `WARN`, and auto-disables the gate so the deployment isn't bricked by the conflict. Operators see a clear log line and can fix the configuration at their leisure.

**Performance**: `Puller.inflightCount` and `Puller.catchupInflight` are `atomic.Int64` mirrors of their respective map sizes, updated under their respective mutexes in the same critical section as the map. Hot-path readers (the gate middleware, the `/api/v1/cluster` status endpoint) are lock-free and don't contend with puller workers under sustained 503 storms.

**503 response shape**: structured for client-side bounded retry, no log scraping required:

```json
{
  "success": false,
  "error": "replication_catch_up_in_progress",
  "message": "Reader is still catching up on replicated files. Retry shortly or check /api/v1/cluster for catch-up progress.",
  "catchup_status": {
    "completed_at": 0,
    "catchup_inflight": 2,
    "catchup_failed": 0,
    "catchup_dropped": 0,
    "queue_depth": 7,
    "inflight_count": 2,
    "pulled": 1278,
    ...
  }
}
```

A `Retry-After: 5` header is also set so HTTP-aware load balancers and clients back off automatically.

**Observability**:

- `QueryHandler.QueryGate503Total()` exposes the cumulative count of gated 503s for Prometheus / metrics dashboards. Operators can alert on a non-zero rate without inferring from generic HTTP error logs.
- A sampled (1Hz) `Warn` log fires while the gate is active, with the gate counter and request path. Avoids flooding under sustained catch-up while still surfacing the degraded state.
- The `/api/v1/cluster` status endpoint exposes the new `queue_depth`, `inflight_count`, `failed`, and `dropped` fields under `replication_catchup_status` for operator dashboards.

**Cache-invalidate exception**: the internal cache-invalidation endpoint (`/api/v1/internal/cache/invalidate`) is deliberately NOT gated — peer nodes need to invalidate caches *during* catch-up, and rejecting those calls would break the cache-invalidation protocol exactly when it matters most.

**Known limitation**: there is a sub-millisecond window between `applyRegisterFile` committing a manifest entry to the Raft FSM and the `onRegister` callback firing `puller.Enqueue`. A query landing in that window can observe `ReplicationReady() == true` while a manifest entry the same Raft commit produced is not yet in the in-flight set. Closing this gap requires a per-query Raft `LastApplied()` barrier on the query path, which is out of scope for this gate. The gate's contract is *"every file the puller has observed has been pulled,"* not *"every file the manifest currently contains has been pulled."* For the operator, this means the gate may unblock a fraction of a second before the very last files committed before the gate-clear are queryable; a tracked follow-up issue will close this if any deployment finds it problematic in practice.

### MQTT API Disabled-Response Consistency (PR #418, follow-up to #416)

The two MQTT API handlers (`MQTTHandler` for stats/health, `MQTTSubscriptionHandler` for CRUD/lifecycle) now share one nil-guard policy. Previously, after PR #416 landed handler-side guards on stats/health, the CRUD handler was still gated at wiring time, so disabled MQTT produced 503 on `/api/v1/mqtt/{stats,health}` and 404 on `/api/v1/mqtt/subscriptions/*`.

A new `requireEnabled(c)` helper on `MQTTSubscriptionHandler` short-circuits every CRUD/lifecycle/stats endpoint with the same 503 + `"MQTT subsystem disabled"` body when the manager is nil. The wiring-side gate in `cmd/arc/main.go` was removed; both handlers now register unconditionally. Regression test in `internal/api/mqtt_subscriptions_test.go` covers six representative routes. Monitors and ops dashboards now see one consistent disabled-response shape across the full MQTT API surface.

### MQTT Nil-Guard on Stats / Health Endpoints (PR #416, @SAY-5)

Closed issue #304: `MQTTHandler.handleStats` and `handleHealth` previously panicked when MQTT was disabled (the handler was wired with a nil manager) or when `GetAllStats` encountered a nil entry in the subscribers map (mid-shutdown / failed-start). Both endpoints now nil-guard the manager and return:

- **503 + `{"success": false, "error": "MQTT subsystem disabled"}`** on `/api/v1/mqtt/stats` when MQTT is off.
- **200 + `{"status": "disabled", "healthy": false}`** on `/api/v1/mqtt/health` when MQTT is off (200 because "disabled" is a steady state, not a degraded one — uptime checks should not page operators about a configured-off subsystem).

`SubscriptionManager.GetAllStats` mirrors the existing single-id `GetStats` pattern with `ok && subscriber != nil`, falling back to the DB-loaded `SubscriptionStats` when the in-memory entry is missing or nil. Regression coverage in `internal/api/mqtt_test.go` and `internal/mqtt/manager_test.go`.

## Dependencies

### Dependabot group bump — fiber, thrift, otel (PR #430)

Dependabot-grouped go.mod refresh. Three updates, +17/-16 across `go.mod` and `go.sum`. Full module test suite (35 packages, `-tags duckdb_arrow`) passed locally before merge.

- **`github.com/gofiber/fiber/v2`** 2.52.12 → 2.52.13 — direct. Single change: escape HTML output in `Ctx.Format` (gofiber/fiber#4232). Arc does not call `ctx.Format`, so no behavior change, but the bump closes a defense-in-depth gap.
- **`github.com/apache/thrift`** 0.22.0 → 0.23.0 — indirect (via `arrow-go`). Go-side change is THRIFT-5896 (race in `TServerSocket.Addr()`), irrelevant to Arc since we don't expose a Thrift server. Bulk of the changelog is cross-language and doesn't affect the Go module.
- **`go.opentelemetry.io/otel`** 1.39.0 → 1.41.0 — indirect (via `arrow-go/parquet → grpc`). 1.41 is the last release supporting Go 1.24; Arc is on Go 1.26 so the upcoming Go 1.25 floor is already cleared. 1.41 also tightens `Baggage.New`/`Parse` validation and rejects `insecure + TLS` exporter configs — Arc does not wire OTel exporters, so this is benign.

One new transitive entry in `go.sum`: `github.com/rogpeppe/go-internal v1.14.1` (test infra pulled by OTel).

## Enterprise

### arcx — proprietary DuckDB extension loader (scaffold)

Arc Enterprise can now load the proprietary **arcx** DuckDB extension at startup. The extension lives in a separate private repo (`Basekick-Labs/arcx`, not yet public) and will host operators that bypass DuckDB's general-purpose query path for workloads where profiling showed DuckDB itself is the bottleneck — partition-aware scans, manifest-backed `read_parquet`, partition-aligned aggregation fast paths. v0.1 ships only a `arcx_version()` proof-of-life UDF; real operators land in follow-up releases.

**Configuration.** Set `database.arcx_extension_path` (env: `ARC_DATABASE_ARCX_EXTENSION_PATH`) to the absolute path of the `arcx.duckdb_extension` binary. The loader is gated by the new `arcx` license feature — Arc refuses to issue `LOAD` if the license does not include it. OSS Arc deployments never load arcx (no path configured by default, no license gate to satisfy).

**Wiring.** When `database.arcx_extension_path` is set, Arc routes the DuckDB pool through `duckdb.NewConnector` with a per-connection init callback that runs `LOAD '...'` on every new pooled connection. DuckDB's `LOAD` is per-connection (no `SET GLOBAL` equivalent), so a one-shot `db.Exec("LOAD …")` would only register arcx on whichever pool member happened to receive the call — the connector-with-init approach guarantees every connection in the pool has arcx loaded before `database/sql` hands it to a query. After the pool is wired, `verifyArcxLoaded()` pins a connection via `db.Conn(ctx)` and runs `SELECT arcx_version()` as proof-of-life. The DSN includes `?allow_unsigned_extensions=true` when arcx is configured (the extension is unsigned by design — see the arcx repo README for the security model).

**License enforcement** is entirely Arc-side. The extension binary does no in-process verification; Arc's `licenseClient.CanUseArcx()` is the sole authority. The licensing perimeter is binary distribution — the `.duckdb_extension` file is internal-only and ships bundled with Arc Enterprise builds. License expiry mid-process does **not** unload arcx (DuckDB has no `UNLOAD`); operators who need to revoke arcx must restart Arc.

**Security note.** `allow_unsigned_extensions=true` in the DSN relaxes DuckDB's signed-extension policy at the **database** level — that is, every connection in the pool runs with the relaxed policy for its lifetime, not just the connection that loads arcx. Only arcx is loaded by Arc, but the flag in principle permits other unsigned extensions if loaded via raw SQL. Out of scope for v1 since user SQL is denied `LOAD`/`INSTALL` by the existing `dangerousSQLPattern` validator (regression test at `internal/api/query_test.go`).

### arcx — `arc_partition_agg` operator wiring (Arc-side companion to arcx PR #1)

The first real arcx operator, `arc_partition_agg(database, measurement, unit)`, ships in the private arcx repo with the v0.2 binary. It answers `SELECT date_trunc(unit, time), COUNT(*) FROM <measurement> GROUP BY 1` (`unit` ∈ `{year, month, day, hour}`) from parquet footers — no row scan. Measured on a local M1 against real Arc data:

| Workload                              | Files  |   Rows | Speedup |
| ------------------------------------- | -----: | -----: | ------: |
| citibike (14 yr, daily-compacted)     | 4,782  | 137 M  |    5.1× |
| production (1 hr, hourly compacted)   |     5  | 393 M  |     35× |
| synth (20 days × 24 hr × 5/hr)        | 2,400  | 189 B  |     76× |

The cost model is linear in **file count**, not row count — so the speedup grows with dataset size at fixed file density.

26.06.1 ships the **Arc-side wiring** required to make the operator usable from Arc's DuckDB pool:

- **`arcx.storage_root` setting.** The operator needs to resolve `{database}/{measurement}/...` paths to absolute filesystem paths without taking the storage root as an argument (which would be ugly and would surface internal paths in user-visible function signatures). Arc's `connInitFn` now runs `SET arcx.storage_root = '<cfg.Storage.LocalPath>'` immediately after the existing `LOAD '<path>'`, on every pooled connection. The setting is registered by the arcx extension at LOAD time; Arc populates it from `cfg.Storage.LocalPath` (the local backend's data root). When `database.arcx_extension_path` is empty (OSS or license-disabled), the SET is skipped entirely.
- **`database.Config.ArcxStorageRoot`.** New field, set from `cfg.Storage.LocalPath` in `cmd/arc/main.go` only when the arcx loader is enabled — same guard as `ArcxExtensionPath`. The DB layer ignores the field when arcx isn't configured.
- **`ValidateSQLRequest` denylist for `arc_partition_agg(`.** Matching the existing `read_parquet(` block, raw user SQL containing a call to `arc_partition_agg(...)` is rejected as a SQL validation error. The operator takes raw `(database, measurement)` strings and globs the filesystem; without this denylist, an authenticated user scoped to `db1` could call `arc_partition_agg('db2', 'mem', 'hour')` and enumerate row counts in databases they don't own — the same RBAC-bypass shape that `read_parquet` was denylisted to close in an earlier release. The denylist runs on string-literal-masked, comment-stripped SQL — literals containing the text `arc_partition_agg` are not false-positives. Five new test cases at `internal/api/query_test.go#TestValidateSQLRequest_BypassesAndFalsePositives` cover direct calls, uppercase, whitespace-before-paren, inside CTE, and the literal-text false-positive.

**The operator is reachable today only via raw SQL (now blocked)** — Arc's query rewriter does not yet detect the eligible shape and emit `arc_partition_agg(...)` automatically. The full productisation step is tracked in the arcx repo's roadmap (`docs/arcx-roadmap.md` in the arcx tree) as the v1.1 blocker. Until then, the operator exists for internal benchmarking and for customer trials that manually opt in. Single-tenant Enterprise deployments can begin evaluating perf gains against their own workloads with no risk of cross-tenant leakage.

Tests added: `TestArcxStorageRootIsSetOnEveryConn` (opt-in integration test, requires `ARCX_TEST_PATH`; CI does not set it) confirms the setting is applied across **distinct pool connections** so a rolling failover to a fresh pool member doesn't break the function. Five validation tests cover the denylist behavior.

## Bug Fixes

### S3-Backed Retention/Delete: RSS Recovery After Long Sweeps (PR #420)

Customers running Arc on the S3 backend reported that container RSS climbed many GB during overnight retention/delete operations and stayed there until container restart. The local-storage backend never showed the symptom. The leaked bytes lived **outside Go's heap**, so the existing `debug.FreeOSMemory()` call after each operation could not reclaim them: DuckDB's `httpfs` extension caches data blocks in libduckdb's native heap, and the AWS SDK Go v2 transport accumulated idle keep-alive connections that retained per-connection HTTP/2 frame buffers. glibc itself does not always return freed pages to the OS without an explicit `malloc_trim(0)`.

26.06.1 ships two production changes and one diagnostic surface:

**Bounded AWS SDK HTTP transport.** [internal/storage/s3.go](internal/storage/s3.go) now configures the SDK with `MaxIdleConns=100`, `MaxIdleConnsPerHost=16`, `IdleConnTimeout=90s`. The per-host bound is sized to comfortably absorb two concurrent multipart uploads at `multipartConcurrency=5`; the idle timeout matches Go's default so cold-start and high-RTT setups don't pay reconnect overhead. Dial / TLS-handshake / expect-continue timeouts are intentionally left at SDK defaults — earlier draft values regressed slow MinIO and high-RTT cross-region paths without affecting the leak.

**Native-heap trim after cache clear.** A new `internal/memtrim` package wraps glibc's `malloc_trim(0)` via cgo, guarded with `#ifdef __GLIBC__` so musl/Alpine builds still compile (the call becomes a no-op stub there), and throttled to once per 30s across the process so concurrent retention/delete/compaction callers can't serialize on the allocator lock. `DuckDB.ClearHTTPCache()` now calls `memtrim.ReleaseToOS()` after the existing `cache_httpfs_clear_cache()` and parquet-metadata-cache reset, so every existing call site (retention.go, delete.go, compaction cache-invalidation) benefits without touching their code paths.

**`/api/v1/debug/{memstats,duckdb-memory,free-os-memory}`.** Three admin-auth diagnostic endpoints used to attribute the leak (Go heap vs DuckDB native heap vs glibc arenas) and retained for future support cases. `/free-os-memory` is itself throttled at 30s and returns `429` with a `retry_after_seconds` field if hit too soon, so a polled dashboard cannot pin the runtime in stop-the-world GC. When `auth.enabled=false`, the endpoints register without auth (matching every other handler in the codebase) but a `WARN` line at startup flags the exposure.

**Measured impact** (controlled Docker harness, 873 small parquet files, 7-day retention, ~10% DELETE):

| metric                                         | before  | after  |
| ---                                            |    ---: |   ---: |
| RSS at `post_delete_5m`                        | 220 MB  | 157 MB |
| Net residue (`post_delete_5m - baseline`)      | +120 MB | +47 MB |
| RSS growth during 5-min idle window            | +72 MB  |   0 MB |

The eliminated +72 MB-during-idle growth is the customer's exact symptom — RSS climbing while no work was happening. It's gone.

**Note on `MALLOC_ARENA_MAX=2`.** A separate experiment with `MALLOC_ARENA_MAX=2` cut residue further but caused 25–100% latency regression across the standard query suite, so it was not adopted. The 30s throttle on `ReleaseToOS()` keeps allocator-lock contention bounded without restricting glibc's per-thread arena count.

### Query Path: Abort Streaming on Client Disconnect (PR #TBD)

The Arrow-based streaming query handlers (`/api/v1/query` and `/api/v1/query/arrow`) write results to the client via Fiber's async `SetBodyStreamWriter`. When a client closed the connection mid-stream — closing a Grafana panel, killing a browser tab, Cmd-W on a dashboard — the streaming goroutine had no way to learn the client had gone away. fasthttp's `RequestCtx.Done()` only fires on server shutdown, not per-request disconnect ([fasthttp@v1.51.0/server.go:2719-2745](https://github.com/valyala/fasthttp/blob/v1.51.0/server.go#L2719-L2745)), and the streaming code used `context.Background()` deliberately because `c.UserContext()` is cancelled when the handler returns (before the async stream writer runs).

The streaming loop would keep calling `reader.Next()` on the Arrow record reader, draining DuckDB result batches into a buffer nobody was reading, until either the query naturally completed or the per-request `queryTimeout` (default 300s) fired. For heavy time-bucket aggregations and wide GROUP BYs on long time ranges, that's tens of MB of result-set memory held per cancelled query.

The fix is mechanical: capture the error from `bufio.Writer.Flush()` and break the streaming loop on the first failed flush, which is the canonical signal in fasthttp's streaming model that the underlying TCP connection has been closed. Six lines of change per handler in [internal/api/query_arrow.go](internal/api/query_arrow.go) and [internal/api/query_arrow_json.go](internal/api/query_arrow_json.go). Regression test in `query_arrow_json_test.go` uses an `io.Writer` that fails after N bytes and asserts the loop breaks before draining the full result set.

This is **complementary to but distinct from** the #420 retention/delete leak: that fix targeted DuckDB native heap residue after S3 reads; this one targets in-flight Arrow record batches held on the goroutine stack when the client abandons a query mid-stream.

### S3 Endpoint Scheme Normalization for DuckDB (PR #422)

The AWS SDK Go v2 accepts `s3_endpoint` with or without an `http(s)://` prefix; DuckDB's `httpfs` extension expects a bare `host:port` and prepends scheme based on `s3_use_ssl`. With `s3_endpoint = "http://host:port"` in `arc.toml` (matching the AWS SDK convention), DuckDB built malformed URLs of the form `http://http://host:port/...` and every `read_parquet()` against S3 failed with `Could not resolve hostname`.

Added a small `stripURLScheme` helper in [internal/database/duckdb.go](internal/database/duckdb.go), called at both `SET GLOBAL s3_endpoint` sites (startup and runtime reconfigure for tiered storage). Case-insensitive, also trims whitespace and trailing slashes — accepts `http://host:port`, `https://host:port/`, `HTTP://host:port`, ` host:port `, and the bare `host:port` form transparently. 18 unit test cases.

### DELETE Rewrite on Non-TLS S3 (PR #423)

The DELETE API rewrites parquet files to remove rows matching a WHERE clause and uploads them back to S3. Against plain-HTTP S3 (MinIO, Garage), every rewrite failed with `compute input header checksum failed, unseekable stream is not supported without TLS and trailing checksum`. AWS SDK Go v2 (`aws-sdk-go-v2/service/s3 v1.99.0`, post-Feb 2025) requires either TLS or a seekable body for the mandatory request checksum, and the previous `io.TeeReader` single-pass SHA256+upload pattern lost the underlying `*os.File`'s seekability.

Replaced the TeeReader with a two-step "hash, then seek-and-upload": `io.Copy` into the SHA256 hasher, `Seek(0, io.SeekStart)`, pass the seekable `*os.File` directly to `storage.WriteReader`. The second read hits OS page cache so disk I/O is unchanged. Validated against MinIO over plain HTTP: 199/199 files rewritten (pre-fix: 0/200).

---

_Maintainer notes: keep this file at the repo root (per [memory/project_release_strategy.md](memory/project_release_strategy.md)); do not write to `docs/RELEASE_NOTES_*` (that path is stale)._
