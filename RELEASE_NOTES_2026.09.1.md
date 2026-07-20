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
