# Arc v2026.06.4 Release Notes

> **Status:** In development.

> **This is a security-hardening patch release.** Defense-in-depth on the cluster request-forwarding path, plus a routing-integrity fix for clustered deployments. No breaking API changes, no schema migrations, no configuration changes. Drop-in upgrade from v2026.06.3. **Single-node (non-clustered) deployments are unaffected by every change here** — the forwarding path they harden never executes without a cluster router.

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

## Impact by deployment mode

| Deployment | Affected by these changes? |
|---|---|
| Single-node / OSS standalone (no cluster router) | **No.** The forwarding path never executes; behavior is byte-for-byte identical to 26.06.3. |
| Clustered, homogeneous nodes (every node can serve every request) | Header-stripping applies to forwarded requests; loop-guard reorder is a no-op because nodes serve locally. |
| Clustered with role separation (reader / writer / compactor) | Header-stripping applies; a spoofed or looped `X-Arc-Forwarded-By` on a non-capable node now returns `508` instead of a failed local attempt. |

## Upgrade notes

1. **No configuration change required.** Drop in the new binary; existing `arc.toml` and license keys work as-is.
2. **No API or on-disk format changes.** Reads, queries, and storage layout are untouched.
3. **Clustered operators:** if any external tooling deliberately sets `X-Arc-Forwarded-By`, `X-Real-IP`, or `X-Forwarded-*` headers on requests to Arc and expects them to survive an inter-node forward, note that these are now stripped on the forwarding hop and re-established by Arc. Client IP has never been derived from these headers, so log/audit attribution is unchanged.
4. **Active licenses keep working.** No re-activation required.

## Dependencies

No product dependency changes in this release. GoFiber remains at `v2.52.13`; Dependabot alert #12 (CVE-2026-45045) is addressed by the in-code hardening above rather than a dependency bump, because no patched v2 release exists and the vulnerable code path (`middleware/proxy.BalancerForward`) is not present in Arc's build graph.
