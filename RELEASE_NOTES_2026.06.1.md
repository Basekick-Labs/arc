# Arc v2026.06.1 Release Notes

> **Status:** In progress. Entries are added as PRs land.

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

## Hardening

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
