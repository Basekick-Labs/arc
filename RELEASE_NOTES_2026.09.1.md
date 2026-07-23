# Arc v2026.09.1 Release Notes

> **Status:** In development.

## Security hardening

### Strip client-controlled forwarding headers at the inter-node boundary (CVE-2026-45045 class)

Dependabot flagged [CVE-2026-45045 / GHSA-gcfq-8gqf-4876](https://github.com/advisories/GHSA-gcfq-8gqf-4876) in GoFiber: the `BalancerForward` proxy helper injects `X-Real-IP` with `Header.Add()` instead of `Header.Set()`, appending the real client IP as a *second* header value so upstreams that read the first value trust an attacker-supplied IP.

**Arc is not affected by the CVE itself.** Arc does not import or use GoFiber's `middleware/proxy` package — `BalancerForward` is not compiled into the binary (verified against the full build graph). Arc's own reverse-proxy path (the cluster request router) already uses `Header.Set()` for `X-Forwarded-For`, and — critically — **no Arc code on the receiving side trusts `X-Real-IP`, `X-Forwarded-For`, `Forwarded`, or `X-Arc-Original-Host` for anything.** Client IP for audit logs, query attribution, and every other decision is derived exclusively from the TCP socket via Fiber's `c.IP()`, and Fiber is not configured to trust proxy headers (`EnableTrustedProxyCheck` / `ProxyHeader` are unset). So the spoofing primitive the CVE describes has no consumer in Arc.

Because there is **no patched GoFiber v2 release** to bump to (the fix landed only in v3), and because migrating the entire HTTP layer to Fiber v3 would be disproportionate for a vulnerability that does not affect us, we have instead **closed the vulnerability class directly in Arc's own code** as defense-in-depth:

- When a clustered node forwards a write or query to a peer, it now **strips all client-supplied forwarding/identity headers** before the request leaves the node: `X-Real-IP`, `X-Forwarded-For`, `X-Forwarded-Host`, `X-Forwarded-Proto`, `X-Forwarded-Port`, `Forwarded` (RFC 7239), `X-Arc-Forwarded-By`, `X-Arc-Original-Host`, `X-Arc-Shard-Routed`, and the CDN client-IP headers `True-Client-IP`, `CF-Connecting-IP`, and `X-Client-IP`. The forwarding node re-establishes the trustworthy values itself from the socket peer and its own node identity.
- This guarantees a peer can never receive an attacker-injected forwarding header, keeping the "nothing downstream trusts these" property true regardless of what future code on a receiving node might choose to read.

Legitimate end-to-end headers (`Authorization`, `Content-Type`, `x-arc-database`, custom application headers, and non-identity multi-value headers such as `Via`) are unchanged and still forwarded verbatim.

### Routing-integrity fix: `X-Arc-Forwarded-By` loop guard is no longer client-influenceable

While hardening the forwarding path we found a related, lower-severity issue reachable in **clustered** deployments. Arc uses the `X-Arc-Forwarded-By` header as a loop guard: a request that already carries it is treated as "already forwarded, handle locally." That header is client-settable, and the check ran *before* the node's capability check. An **authenticated** caller could therefore set `X-Arc-Forwarded-By` on a direct request to a node that cannot serve it locally (e.g. a write to a reader node, or a query to a compactor node) and suppress the forward — forcing the node onto a local path that is structurally guaranteed to fail.

This was never a privilege escalation, data-exposure, or cross-tenant issue — the peer re-authenticates the forwarded `Authorization` token, and identity is always socket-derived. It was a self-inflicted routing break available only to already-authenticated callers (CWE-290, authentication-bypass-by-spoofing class, but bounded to routing behavior).

The fix reorders the decision so the header can no longer force a doomed local path:

- If the node **can** serve the request type locally, it does — the `X-Arc-Forwarded-By` header is not consulted at all (the common case).
- If the node **cannot** serve locally and the request carries the marker, this is a genuine routing loop *or* a spoofed header; the node now returns a deterministic **`508 Loop Detected`** (`request already forwarded and cannot be served by this node`) instead of silently attempting local processing.
- If the node cannot serve locally and there is no marker, it forwards to a capable peer as before.

Genuine peer-to-peer loops (which should never happen in a healthy cluster) now terminate cleanly with the same clear error instead of a confusing local failure.

### Partition-pruner amplification DoS: cap path generation, floor the start date, cancel on disconnect ([#536](https://github.com/Basekick-Labs/arc/issues/536))

The query partition pruner (`internal/pruning`) generated one storage path per hour plus one per day across a query's time range, with **no upper bound on the number of paths, no start-date floor, and no cancellation**. A single small request with a very wide range — e.g. `WHERE time >= '0001-01-01'` — forced the server to materialize on the order of a million path strings and then glob (local) or LIST (S3/Azure) every one of them: an amplification DoS turning one HTTP request into large server CPU, memory, and object-storage LIST billing.

The fix closes the amplification at three points:

- **Path-count cap.** Path generation is now bounded by a hard cap (`maxPartitionPaths = 50,000`, ~5 years of hourly pruning). The count is estimated *before* any allocation; a range over the cap returns no paths and the query falls back to the single unpruned `/**/*.parquet` glob — correct results, just not partition-pruned. Millions of path strings are never allocated.
- **Epoch floor on the start date.** A query start earlier than `1970-01-01 UTC` is clamped up to the epoch before path generation. Arc has no data before the epoch, so clamping is lossless — a legitimate multi-decade range still prunes from 1970 through its end, no rows dropped — while the degenerate unbounded-downward case (`0001-01-01`) can no longer drive a huge pre-data range.
- **Cancellation on client disconnect.** Path generation now honors the request `context.Context`; a client disconnect or deadline aborts the loop instead of running to completion server-side.

Separately, the pruner's two internal TTL caches (glob results and partition paths) are now **bounded**: expired entries are evicted on read, and each cache refuses new keys past a `10,000`-entry cap (after first dropping expired entries). This closes a slow-growth vector where an attacker varying the time literal per request pinned attacker-controlled memory for the process lifetime.

Reachable in every deployment mode (the pruner runs on every `FROM`/`JOIN` query); pre-existing on `main`, unrelated to any other work in this release.

## Bug fixes

### `date_trunc`/`time_bucket` epoch rewrite no longer corrupts parenthesized column arguments ([#535](https://github.com/Basekick-Labs/arc/issues/535))

Arc rewrites `date_trunc('hour', col)` (and `time_bucket(...)`) into faster epoch arithmetic. The rewriter extracted the column argument with a paren-blind regex that stopped at the **first** `)` rather than the matching one. When the column argument itself contained parentheses — `coalesce(time, a)`, `(time)`, `CAST(ts AS TIMESTAMP)`, or any nested call — the capture was truncated and the `::BIGINT` cast was spliced into the wrong place, producing SQL that failed at the DuckDB binder (`No function matches ... 'epoch(BIGINT)'`) on a query DuckDB would otherwise have run correctly.

This was an **availability** bug, not a wrong-answer bug: every corrupted form failed loudly at the binder rather than returning a wrong number.

The fix leaves any `date_trunc`/`time_bucket` call whose column argument contains a parenthesis **unrewritten**, so DuckDB evaluates it natively — correct results, just without the epoch optimization for that one call. The common `date_trunc('hour', time)` bare-column form is unaffected and still gets the optimization; a query mixing both forms optimizes the bare one and passes the parenthesized one through. Introduced in `161be30` (2026-01-07); present on `main`.

### Continuous queries no longer produce duplicate aggregate rows ([#521](https://github.com/Basekick-Labs/arc/issues/521))

Continuous-query output was append-only with no idempotency: a window could be aggregated and written more than once — after a crash between the destination write and the watermark advance, or a re-run over an overlapping range — leaving **duplicate rows** in the destination measurement. Separately, when the aggregation query selected no `time` column, every output row was stamped with `time.Now()` (the ingestion wall-clock) instead of the window time, so the rollup's timestamps were wrong and the duplicates weren't even dedupe-able.

Two fixes, which together make CQ output **idempotent via compaction**:

- **Window-time stamping.** A CQ that doesn't select a time column now stamps each output row with the window's start time (the `[start, end)` bucket boundary), not `time.Now()`. Timestamps are correct, and re-running the same window produces byte-identical `(dimensions, time)` keys.
- **Dedup metadata on CQ output.** CQ output now carries the Parquet metadata compaction needs to collapse duplicate windows to one row. Declare the grouping dimensions in a new optional **`tag_columns`** field on the CQ definition (e.g. `"tag_columns": ["host"]` for `GROUP BY host`) — these are written as `arc:tags`, and compaction dedups on `(tags, time)`. A CQ with **no** grouping (one row per window, e.g. `SELECT avg(x) …`) is detected automatically and deduped on time alone. Duplicate emissions are collapsed the next time the destination partition compacts.

**Safety:** a CQ that groups by a dimension but does **not** declare it in `tag_columns` is detected (its output has multiple rows per timestamp) and is **not** marked for time-only dedup — this avoids silently dropping series. Such a CQ logs a warning asking the operator to declare `tag_columns`. Existing CQ definitions are migrated automatically (a new nullable column); a CQ without `tag_columns` behaves exactly as before except for the corrected timestamp. This is the foundation for late-data reprocessing ([#522](https://github.com/Basekick-Labs/arc/issues/522)).

Note: this makes output **eventually** idempotent (duplicates collapse at compaction), not atomically exactly-once — the write and the watermark advance remain separate steps. True exactly-once is tracked in #522.

**Upgrade impact — this is not a breaking change.** Existing continuous queries keep working: the CQ database is migrated automatically (a new nullable `tag_columns` column is added on startup), old definitions read and execute exactly as before, and `tag_columns` is optional. Two behavior changes to be aware of:

- **Timestamps for CQs that don't select a `time` column.** These previously stamped output with `time.Now()` (ingestion wall-clock) and now stamp the window start. This corrects a real bug, but it means such a destination has a timestamp discontinuity at the upgrade point (old rows keep their `time.Now()` values, new rows get window-start values). CQs that *do* select a time column (the common case, e.g. `date_trunc('hour', time) AS time`) are completely unchanged.
- **Dedup is opt-in for grouped CQs.** An existing `GROUP BY <dimension>` CQ gets no idempotency benefit until you add `tag_columns` (via an update); until then it behaves exactly as before (append-only). This is deliberate — a grouped CQ is **never** auto-deduped without declared tags, because deduping multi-series output on time alone would delete series. A genuinely ungrouped CQ (one row per window) is deduped automatically after the upgrade.

No action is required to upgrade; add `tag_columns` to your grouped CQs when you want the duplicate-collapsing behavior.

### MQTT subscriptions honor an explicit QoS 0 ([#326](https://github.com/Basekick-Labs/arc/issues/326))

Creating an MQTT subscription with `"qos": 0` (at-most-once / fire-and-forget) silently persisted it as QoS 1. The create request modeled `qos` as a plain integer, so an explicit `0` was indistinguishable from an omitted field, and the "default unset QoS to 1" step rewrote it. A subscription requested as fire-and-forget was quietly upgraded to at-least-once.

The create request now models `qos` as optional: an omitted field still defaults to 1 (at-least-once, the safe ingestion default), but an explicit `0`, `1`, or `2` is preserved as written. QoS defaulting was moved out of the shared `SetDefaults` step so a persisted, explicitly-chosen QoS 0 can never be rewritten. (The update endpoint already handled this correctly.)

Related fix in the same handler: an out-of-range or otherwise invalid subscription request now returns **`400 Bad Request`** with the validation message instead of `500 Internal Server Error`.

### Removed the non-functional MQTT `reconnect_min_seconds` setting ([#327](https://github.com/Basekick-Labs/arc/issues/327))

MQTT subscriptions accepted a `reconnect_min_seconds` field that was validated, defaulted, and persisted but **never applied** — the MQTT client library hardcodes the initial reconnect backoff at 1 second and exposes no setter for it, so the value had no effect. It has been removed from the API: create/update request bodies no longer accept it, and it no longer appears in `GET`/`LIST` subscription responses. `reconnect_max_seconds` is unaffected — it still caps the exponential backoff.

This is not a breaking change: an omitted or extra `reconnect_min_seconds` in a request body is simply ignored, and existing MQTT subscription databases are untouched (the underlying column is left in place, unused, so no migration runs). The reconnect behavior itself is unchanged — the minimum was already a fixed 1 second in practice.

### `UpdateOrganization`/`UpdateTeam` now validate the name on update ([#324](https://github.com/Basekick-Labs/arc/issues/324))

`UpdateOrganization` and `UpdateTeam` accepted any string as a new name — including empty strings and names starting with a digit — because the `validateName` check that `CreateOrganization` and `CreateTeam` apply was missing from the update path. An invalid name could be written to the database and later cause confusing errors or inconsistent behavior downstream.

Both update methods now call `validateName` on the supplied name before any write (OSS direct-SQLite path and cluster Raft path). The validation rules are the same as create: must start with a letter, alphanumeric plus underscore/hyphen, at most 64 characters. An invalid name returns an error and the update is rejected.

The API handlers now return **`400 Bad Request`** when name validation fails, so client-supplied invalid values get the correct status code. On the create path this replaces a `500 Internal Server Error` that was returned for every rule except the empty-name check (so `name` values containing spaces, or starting with a digit, previously surfaced as a server error). On the update path there was no status code to replace — the invalid name was accepted with `200 OK` and written to the database, which is the bug described above.

### RBAC endpoints no longer return `500` for client errors ([#549](https://github.com/Basekick-Labs/arc/issues/549))

Two remaining cases in the RBAC API reported client-supplied bad input as `500 Internal Server Error`. A 5xx tells the caller the server broke — it drives client retries, pages on-call, and burns error budgets — when the correct response is "fix your request".

- **Duplicate organization/team names now return `409 Conflict`** (previously `500`) on all four create and update endpoints. Renaming an organization to a name already in use, or creating a team whose name is taken within its organization, is a conflict with existing state, not a server fault.
- **`PATCH /api/v1/rbac/roles/:id` now returns `400 Bad Request`** (previously `500`) for an invalid `database_pattern` or an unrecognized entry in `permissions`. These were already validated — the errors simply were not mapped to a status code. The matching create endpoint already returned `400`, so create and update now agree.
- **`POST /api/v1/rbac/roles/:role_id/measurements` now returns `400 Bad Request`** (previously `500`) for an invalid `measurement_pattern`, matching the sibling `permissions` check on the same endpoint, which already returned `400`.

Error *messages* are unchanged — only the status codes differ. Clients that branch on the response body are unaffected; clients that branch on the status code get a correct one. A regression test pins the exact message text so this stays true.

Internally, these paths now classify errors with typed sentinels (`errors.Is`) instead of matching on message text, so rewording one of them cannot change a status code. The remaining not-found and required-field conditions were converted in the same release (see below).

### RBAC status codes no longer depend on error-message text

Follow-up to the sentinel work above. The RBAC handlers previously decided several status codes by comparing `err.Error()` against exact strings such as `"team not found"` and `"database pattern is required"` — 16 comparisons across the RBAC routes and the token-membership endpoints. Rewording any of those messages would have silently changed an endpoint's status code, with nothing to catch it.

All of them now use typed sentinels (`ErrNotFound`, `ErrMissingField`, `ErrConflict`) matched with `errors.Is`. Error messages and every status code are unchanged — this is an internal robustness change with no API-visible effect.

Scope: this covers every status decision in the RBAC handlers (`rbac_routes.go`) and the two RBAC-gated token-membership endpoints. The plain token endpoints (`PATCH`/`DELETE /api/v1/auth/tokens/:id`) still match `"token not found"` by string; those are served by `AuthManager`, not the RBAC manager, and converting them is separate work.

One asymmetry is deliberately preserved: a missing entity returns **`404`** when it is the target of the request (`PATCH /organizations/:id`) but **`400`** when it is the parent of a create (`POST /organizations/:org_id/teams`). Both surface the same underlying error, so the distinction lives in the handlers; a regression test now pins it, since a central not-found mapping would otherwise have flipped three create endpoints from `400` to `404`.

### Optional timestamp fields are now omitted when unset, and rendered in UTC ([#546](https://github.com/Basekick-Labs/arc/issues/546))

Several API response fields typed as a Go `time.Time` carried an `omitempty` tag that does nothing — `encoding/json` never omits a `time.Time`, so an unset value rendered as the confusing placeholder `"0001-01-01T00:00:00Z"` rather than being absent. Fields where "unset" is a meaningful state are now proper optional fields (omitted when there is no value):

- MQTT subscription stats: `last_message_at` (no message received yet) and `connected_since` (not connected).
- API token info: `last_used_at` (a token that has never been used).

The MQTT stats timestamps are also now rendered in **UTC** (matching every other timestamp in the API) instead of the server's local timezone. Two internal/debug-only fields (the license-server response's `expires_at`, the compaction subprocess's `partition_time`) had the misleading `omitempty` dropped for honesty; their values are unchanged.

Minor response-shape change: clients that previously read `"0001-01-01T00:00:00Z"` from these fields will now find them absent. That placeholder carried no information, so this is safe.

## Performance

### Faster local-storage directory listing ([#347](https://github.com/Basekick-Labs/arc/issues/347))

The local storage backend's `List` and `ListObjects` now use `filepath.WalkDir` instead of `filepath.Walk`. `WalkDir` reads directory entries without an `lstat(2)` per entry:

- **`List`** needs only the entry name and is-dir bit (both free on `fs.DirEntry`), so it now does **no** per-entry stat at all — down from one stat on every file, directory, and hidden file. Benchmarked at ~19% fewer allocations and ~38% less allocated memory on a 2,000-file partition tree.
- **`ListObjects`** needs each returned file's size and mod-time, so it still stats those — but skips the stat on directories and hidden files (matched by name first). The saving grows with the fraction of the tree that is directories/dotfiles.

The win is largest on cold caches and network filesystems, where the `lstat` syscall dominates. Local backend only; the returned results are unchanged.

### Cheaper SQL-transform cache key ([#331](https://github.com/Basekick-Labs/arc/issues/331))

The per-request SQL-transform cache (which memoizes rewriting `FROM db.measurement` into `read_parquet(...)`) derived its map key with SHA256 plus a hex-encode — a 256-bit cryptographic digest and an allocation, for an internal cache key with no adversarial-collision concern. The cache now stores the SQL string itself as the map key (an exact match, so there is **no** possibility of two different queries colliding onto one entry) and uses a fast FNV-1a hash **only** to pick a shard.

Per-op benchmarks on the cache: `Get` **257 ns → 71 ns**, `Set` **269 ns → 86 ns**, and each drops from **192 B / 4 allocations to zero allocations**. This is a per-request improvement (it removes allocation pressure from the query path); it does not affect the time DuckDB spends executing a query, so end-to-end latency on large scans is unchanged. This change only swaps the hash and key — the cache's existing sharding, sizing, and eviction are untouched.

### Centralized memory-release throttle ([#421](https://github.com/Basekick-Labs/arc/issues/421))

The three copies of the 30-second memory-release debounce (the post-delete/retention `debug.FreeOSMemory` throttle, the `/api/v1/debug/free-os-memory` endpoint, and the Linux `malloc_trim` throttle) are now a single `internal/throttle.Debouncer`. Behavior is unchanged — same monotonic-clock window and the same first-call sentinel that fires the very first request rather than throttling it. One minor improvement: when two callers race the `/api/v1/debug/free-os-memory` endpoint at the same instant and one loses, the loser's `429` response now includes a `retry_after_seconds` hint (previously it was omitted only in that narrow race).

### MQTT message hot path no longer takes a mutex per message ([#328](https://github.com/Basekick-Labs/arc/issues/328))

Every received MQTT message took a full write-lock on the subscriber's state mutex just to update the `last_message_at` timestamp. That serialized the message hot path against itself and against the stats endpoint. The timestamp is now an `atomic.Int64` (Unix nanoseconds), so the per-message stat update is lock-free — matching the other per-message counters, which were already atomic. The stats-reporting output is unchanged. Under contention the per-message stat update is ~19% faster in a microbenchmark; the real benefit is removing the serialization point from a high-throughput ingest path.

## Impact by deployment mode

The forwarding-header and loop-guard changes are cluster-only; the partition-pruner DoS fix (#536) applies to **every** deployment mode, since the pruner runs on every query.

| Deployment | Forwarding-header / loop-guard changes | Partition-pruner DoS fix (#536) |
|---|---|---|
| Single-node / OSS standalone (no cluster router) | **No.** The forwarding path never executes; behavior is byte-for-byte identical to 26.06.3. | **Yes.** Applies to every query. |
| Clustered, homogeneous nodes (every node can serve every request) | Header-stripping applies to forwarded requests; loop-guard reorder is a no-op because nodes serve locally. | **Yes.** Applies to every query. |
| Clustered with role separation (reader / writer / compactor) | Header-stripping applies; a spoofed or looped `X-Arc-Forwarded-By` on a non-capable node now returns `508` instead of a failed local attempt. | **Yes.** Applies to every query. |

## Upgrade notes

1. **No configuration change required.** Drop in the new binary; existing `arc.toml` and license keys work as-is. The partition-pruner limits (#536) are hardcoded safety bounds, not tunable config keys.
2. **No API or on-disk format changes.** Reads, queries, and storage layout are untouched. Queries with a start date earlier than `1970-01-01` are pruned from the epoch forward; since Arc stores no pre-epoch data, results are unchanged.
3. **Clustered operators:** if any external tooling deliberately sets `X-Arc-Forwarded-By`, `X-Real-IP`, or `X-Forwarded-*` headers on requests to Arc and expects them to survive an inter-node forward, note that these are now stripped on the forwarding hop and re-established by Arc. Client IP has never been derived from these headers, so log/audit attribution is unchanged.
4. **Active licenses keep working.** No re-activation required.

## Dependencies

No product dependency changes in this release. GoFiber remains at `v2.52.13`; Dependabot alert #12 (CVE-2026-45045) is addressed by the in-code hardening above rather than a dependency bump, because no patched v2 release exists and the vulnerable code path (`middleware/proxy.BalancerForward`) is not present in Arc's build graph.
