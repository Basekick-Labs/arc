# Arc v2026.09.2 Release Notes

> **Status:** Planned — October 2026 patch release.

## New: MessagePack binary columns in Parquet ([#440](https://github.com/Basekick-Labs/arc/issues/440))

Columnar MessagePack ingest now accepts binary values and stores them as Arrow
Binary / Parquet BYTE_ARRAY without converting their bytes to UTF-8 text.
Nulls and valid empty byte sequences remain distinct. Binary columns are
supported through buffer merging, sorting and hour-partition splitting.

The typed MessagePack fast path continues to fall back to the generic decoder
for binary payloads. This change does not emit GeoParquet metadata, declare
geometry types, configure DuckDB extensions or add spatial indexes.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#916](https://github.com/Basekick-Labs/arc/pull/916).

## New: administrative cluster file deletion (`DELETE /api/v1/cluster/files`) ([#830](https://github.com/Basekick-Labs/arc/pull/830))

A new administrative endpoint `DELETE /api/v1/cluster/files?path=...&confirm=true` allows cluster operators to remove an entry from the cluster-wide Raft manifest.

**Destructive behavior:** removing a manifest entry via Raft triggers physical file deletion across the cluster. On nodes using local storage, the FSM delete callback enqueues physical deletion through the delete worker pool, unlinking the local file on every node. On shared backends (S3, Azure), the physical object remains until swept by the reconciliation cleaner. Because this operation permanently unlinks local data, the endpoint requires explicit confirmation via `?confirm=true` (rejecting with `400 Bad Request` if omitted).

The endpoint looks up entries by exact path (capped at 4096 bytes) and returns `404 Not Found` if absent. This allows operators to remove corrupted or unaddressable keys (e.g. malformed paths from older Arc releases) that retention and reconciliation sweeps cannot reach; when an unaddressable key is removed, it is flagged with `unaddressable_key=true` in audit logs and warnings. The endpoint also enforces a 256-character limit on the optional `reason` parameter (defaulting to `"operator"`), logs deletions at `Warn` level with node identification, returns `409 Conflict` when clustering is disabled on the node, and returns `503 Service Unavailable` with a `Retry-After: 1` header when Raft consensus or leader forwarding is temporarily unavailable.

Contributed by [@Thundercloud12](https://github.com/Thundercloud12) in [#830](https://github.com/Basekick-Labs/arc/pull/830).

## New (experimental): file-level time pruning for high-frequency ingest (`query.file_time_pruning`)

**This feature is experimental in 26.09.2**: it ships disabled by default behind
`query.file_time_pruning` and is being soaked continuously in our own dev environment
(a one-second-ingest workload querying it around the clock). Tracking issue for
promotion: [#659](https://github.com/Basekick-Labs/arc/issues/659) — if the multi-week
soak and field feedback hold up, it becomes **stable and enabled by default in 27.01.1**.

Arc's partition pruning narrows queries to hour directories — but a high-frequency
ingest workload (one flush per second) accumulates thousands of small Parquet files in
the **current, not-yet-compacted hour**, and every query listed and footer-read all of
them. Measured on a live one-second workload: the same 5-minute dashboard query cost
19ms at the top of the hour and 340ms+ as the hour filled.

Two new keys (opt-in, local storage backend only):

```toml
[query]
file_time_pruning = false                # enable file-level pruning of the live hour
file_time_pruning_margin_seconds = 300   # writer clock-skew allowance
```

When enabled, Arc expands the one hour-glob containing the query's lower time bound —
only when that hour is the current UTC wall-clock hour — and drops files whose
filename flush-timestamp proves they cannot contain rows in range. **Measured result:
340ms → 29ms (11.7×) over a 7,893-file live hour; a 60-second window runs in 12ms.**
As a side effect, DuckDB's file/object caches stop inflating with the live-hour file
count — resident memory on the same workload dropped from ~450MB to ~90MB.

Safety properties: filename timestamps are flush times, so only the lower bound is
ever pruned (a file written after the range can still hold in-range rows and is always
read); backfill is unaffected (old data arriving now lands in new files, which are
always kept); unrecognized filenames and compacted outputs are always kept; a
zero-survivor result falls back to the unpruned glob. Expanded file lists are never
cached — every query re-lists the live hour, so freshly flushed data is always
visible. The one documented caveat: rows stamped further ahead of the server clock
than the margin may be invisible until their hour closes; size the margin to your
worst writer clock skew.

## Fixed: DuckDB-native `INTERVAL` syntax silently disabled partition pruning

Time-range extraction only recognized SQL-standard quoted intervals
(`INTERVAL '300 seconds'`). The equally valid DuckDB-native unquoted form
(`INTERVAL 300 SECOND`) extracted no time range at all — queries using it got **no
partition pruning whatsoever** and scanned every partition, silently. Both forms now
prune. The unquoted patterns are matched against the literal-masked query text and
require a word boundary after the unit, so interval-lookalike text inside string
literals (e.g. a log-search `LIKE '%INTERVAL 5 MINUTE%'`) and identifiers such as
`interval2` can never be misread as time bounds; compound quoted intervals like
`'1 day 12 hours'` remain unpruned (never mis-pruned as their first component).

### The Helm chart now deploys three writers by default

Readers replicate the write-ahead log and serve queries, but they are never promoted to writer, so the failover pool is made of writer-role nodes. A deployment with one writer therefore cannot fail over, and two absorbs exactly one failure before it is down to a single writer with no spare left and no pod that can be drained for a rolling upgrade, which is why the chart already refused it. The writer default moves from one to three, which is the minimum for high availability in both deployment patterns: on shared storage all three take ingest behind the load balancer, and on local storage, with `cluster.failover_enabled` on, one is elected primary while the other two stand by. Leaving failover off on local storage used to mean no primary election at all, so every writer-role node treated itself as the primary for retention, continuous queries and deletes. A primary is now elected either way; what failover adds is a replacement chosen automatically when that primary dies. Arc still warns about a cluster running below three writers, because losing the one it has is then a manual recovery. The shared-storage example overlay moves to three as well, so following the Pattern 2 guide no longer silently overrides the new default back to one.

Existing installs are unaffected until they next apply their own values. An install that deliberately wants a single writer, for development or a single-node deployment, can still set the count to one.

## New: a write-pool health check at `GET /ready/write` ([#857](https://github.com/Basekick-Labs/arc/issues/857))

`/ready` answers whether a node is healthy, and every healthy node answers it, readers included. A load balancer's write pool pointed at it therefore sends writes to nodes that can only proxy them onward, which makes those nodes responsible for carrying other nodes' ingest.

`GET /ready/write` answers the narrower question: should writes be sent here. It returns 200 on a node that accepts ingest and 503 otherwise, with a body naming which. In shared-storage mode every healthy writer answers 200, because that is the whole point of the pattern. In local-storage mode only the elected primary writer does. Readers and compactors never do, and a single-node deployment always does. One load-balancer configuration is therefore correct in both patterns.

It is unauthenticated, like `/ready`, and strictly narrower: anything that makes a node unready for traffic also makes it unready for writes, so a node draining on expired storage credentials or still replaying its write-ahead log is not a write target either. Point the write pool at `/ready/write` and leave query pools and Kubernetes probes on `/ready`, whose meaning is unchanged.

Writes that arrive at a node which is not a write target are still accepted and proxied, as before. This endpoint lets a load balancer avoid that hop rather than changing what happens without one.

## Changed: telemetry now reports the arcli installations an instance served

Arc's opt-out telemetry gains a `clients` section describing the [arcli](https://github.com/Basekick-Labs/arcli) installations that talked to this instance since the last successful report. arcli sends a random per-installation UUID and its version with each request. Arc counts an installation only on a request that succeeded (a status below 400) and, when authentication is configured, that carried a valid token, so unauthenticated endpoints such as `/health` never contribute. It then reports, per installation, the id, the version, and the time it was last seen, plus a count and a `truncated` flag once more than 256 distinct installations have been seen between reports. The section is omitted entirely when no arcli client was seen.

The identifier is generated by arcli and is not derived from a user, a machine, or a network address, and no query text, database or measurement names, tokens, or IP addresses are included. The purpose is to tell how many distinct CLI installations an instance serves and which versions are in use.

This rides on the existing telemetry channel and the existing switch: `telemetry.enabled = false` in `arc.toml`, or `ARC_TELEMETRY_ENABLED=false`, disables all of it including this section.

## Security fixes

### Read-SQL gate hardening: quoted-construct boundaries

Closes a read-path gate bypass in the same family as the earlier quoted-name and
replacement-scan hardening. The scanner the read-SQL gates depend on could
disagree with DuckDB about where a quoted construct ends, so part of a statement
could reach the engine without having been gated. Four spellings are fixed and
pinned by tests, and the scanners behind them are now shared so the two entry
points cannot drift apart again.

The DuckDB sandbox's storage-root allowlist bounded impact throughout: files
outside the configured storage root were refused regardless. RBAC-enabled
multi-tenant deployments are the ones that should upgrade.

Full technical detail will accompany the corresponding security advisory once it
is published. Found during internal review.

### RBAC table-reference deduplication preserves case ([#750](https://github.com/Basekick-Labs/arc/issues/750))

Simple table references (`FROM table`) and JOIN table references (`JOIN table`) now preserve identifier case in deduplication keys during RBAC permission extraction. Previously, the deduplication key folded table names to lowercase while the downstream RBAC pattern matcher evaluated case-sensitively against case-sensitive storage backends. A query referencing measurements differing only by case (e.g., `SELECT * FROM cpu WHERE x IN (SELECT y FROM CPU)`) folded both references into one, authorizing the query if the principal had access to only one of the spellings. Both references are now checked against permissions independently.

Contributed by [@Thundercloud12](https://github.com/Thundercloud12) in [#832](https://github.com/Basekick-Labs/arc/pull/832).

### Read-SQL validator hardening ([GHSA-w6w2-x8xv-q8x2](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-w6w2-x8xv-q8x2))

Closes a read-path validator bypass on RBAC-enabled multi-tenant deployments, in
the same family as the earlier quoted-name and replacement-scan hardening. The
sandbox storage-root allowlist bounded impact throughout, and single-tenant and
OSS deployments were not exposed to a new risk.

Full technical detail will accompany the corresponding security advisory once it
is published. Responsibly reported by **[@rexpository](https://github.com/rexpository)**.

### PREPARE and EXECUTE are rejected by the read-SQL validator ([#739](https://github.com/Basekick-Labs/arc/issues/739))

`PREPARE` and `EXECUTE`, DuckDB's indirect-execution statements, are now blocked up front by the read-SQL validator on every user query endpoint. They were not exploitable before this change — the single-statement rule already rejects the two-statement chain — so this is defense-in-depth in case the statement splitter is ever relaxed. As with the other blocked keywords, a column literally named `prepare` or `execute` must be double-quoted.

Contributed by [@Thundercloud12](https://github.com/Thundercloud12) in [#767](https://github.com/Basekick-Labs/arc/pull/767).



### Dependency bump: Apache Thrift 0.23.0 → 0.24.0 ([GHSA-8wv5-x4w7-5gww](https://github.com/advisories/GHSA-8wv5-x4w7-5gww))

`github.com/apache/thrift` is bumped to 0.24.0, which patches a high-severity
infinite loop in the Go bindings when parsing malformed Thrift input. Thrift is
an indirect dependency reached through arrow-go's Parquet metadata decoding
(Parquet footers are Thrift-encoded), so the vulnerable code is in Arc's build.
The ingest, API, query, and Iceberg test suites were verified against 0.24.0.

### Dependency bump: gRPC-Go 1.83.1 → 1.83.2 ([#710](https://github.com/Basekick-Labs/arc/pull/710))

`google.golang.org/grpc` is bumped to 1.83.2, which rejects requests missing
both the `:authority` and `Host` headers instead of serving them. The fix is
server-side, and Arc never starts a gRPC server or client — the library is
linked in transitively through arrow-go's Flight package — so the vulnerable
path was not reachable, but the code is no longer in the binary and dependency
scanners come up clean.

The upgrade carries gRPC's own minimum-version requirements with it, so the
same bump moves `golang.org/x/net` (0.55.0 → 0.58.0), `golang.org/x/crypto`
(0.53.0 → 0.55.0), `golang.org/x/text` (0.39.0 → 0.41.0), `golang.org/x/sync`
(0.21.0 → 0.22.0), `golang.org/x/sys` (0.46.0 → 0.47.0), plus `golang.org/x/mod`
and `golang.org/x/term`. Two of those are load-bearing for Arc: `x/crypto`
supplies bcrypt for auth password hashing, and `x/sync` supplies the semaphore
and errgroup primitives used by tiering and Iceberg. The auth, cluster-security,
tiering, and Iceberg suites were verified against the new versions, the latter
two under `-race`.

### Cluster hardening: fully authenticated coordinator handshake (Enterprise)

Every field of every coordinator-handshake message is now covered by its
HMAC, in both directions. Previously the join, heartbeat and leave messages
authenticated only `{message type, nonce, node ID, cluster name, timestamp}`
while the handlers went on to consume other fields from the same message — a
joining node's role and its advertised Raft, API and coordinator addresses; a
heartbeat's self-reported state. On a cluster with `cluster.tls_enabled`
unset (the default), an attacker positioned on the inter-node network could
rewrite any of those and the signature still verified. The responses —
join result, leader redirect, and the forward-apply acknowledgement — carried
no authentication at all, so the same position allowed feeding a joining node
a fabricated cluster membership list, redirecting it to an attacker-chosen
coordinator, or telling a follower its replicated write had failed (or
succeeded) when it had not.

Three further changes close the same class of gap:

- **Replay protection** on join, heartbeat and leave. These messages were
  previously bounded only by the five-minute freshness window, inside which a
  captured message could be resent verbatim. Nonces are now consumed on
  receipt. The nonce cache's lifetime was also corrected — it must outlive the
  freshness window by more than the window itself, because a message may be
  stamped up to one tolerance in the future — which likewise tightens the
  existing cache-invalidate and edge-sync replay guards.
- **Uniform pre-authentication join errors.** A rejection that happens before
  the peer proves it holds the shared secret now returns one opaque string;
  previously the cluster-name mismatch echoed the expected cluster name back
  to an unauthenticated caller and the three failure modes were individually
  distinguishable. The specific cause is still logged server-side.
- **Canonical encoding.** The signed payload is length-prefixed rather than
  NUL-delimited. The coordinator protocol is raw TCP carrying JSON, which
  passes NUL through, so a delimiter-joined encoding let an attacker who
  controlled a field's contents re-partition the signed input.

Clustering is an Enterprise feature and is off by default; single-node and
OSS deployments are unaffected.

> **Upgrade note (clustered Enterprise deployments):** the handshake wire
> format has changed, so a node on this version cannot authenticate with a
> node on an older one. Upgrade the cluster as a coordinated restart: **stop
> all cluster nodes, upgrade the binary on every node, then restart all
> nodes.**
>
> A rolling restart does not degrade gracefully. Across a mixed-version
> window: cross-version nodes fail each other's heartbeats and are marked
> unhealthy within roughly three health-check intervals (~15s by default),
> withdrawing them from query routing; with `cluster.failover_enabled` set, an
> automatic writer or compactor failover is committed through Raft about 30
> seconds in, while the original primary is still alive, and that promotion
> persists after the upgrade completes. A follower that is restarted
> *gracefully* under a not-yet-upgraded leader is removed from the Raft
> configuration by its own leave notification and cannot rejoin until the
> leader is upgraded. Writes forwarded from an upgraded follower to an
> older leader are applied by that leader but reported to the follower as
> failed, because the acknowledgement is unsigned — file registrations are
> retried by anti-entropy, compaction retries on its next tick, and an
> upgraded node may log `Failed to create initial admin token` at startup
> (non-fatal).
>
> The handshake authenticates inter-node messages; for confidentiality on the
> interconnect, enable `cluster.tls_enabled`.

Full technical detail will accompany the corresponding security advisory once
it is published. Responsibly reported by **[@rexpository](https://github.com/rexpository)**.

### Replicate-sync now authenticates `SupportsBinaryEntries` ([#714](https://github.com/Basekick-Labs/arc/issues/714))

The replicate-sync handshake (a reader requesting WAL replication from a
writer) authenticates `{reader_id, last_known_seq, nonce, cluster_name,
timestamp}`, but the `SupportsBinaryEntries` flag that negotiates binary-framed
WAL entries (see *Replication can now carry WAL entries up to the full
payload cap* below) rode outside the MAC. It was left unsigned on the
reasoning that folding it in would break mixed-version handshakes; that
reasoning no longer holds once the coordinator handshake above became a hard
cutover regardless, so the flag is now bound into `ComputeReplicateSyncHMAC`
and `ValidateReplicateSyncHMAC` alongside the rest of the message. An
on-path tamperer who flipped the previously-unsigned flag could only force a
framing downgrade or a dropped connection — both already available to anyone
who can modify the stream — so this closes a completeness gap rather than a
previously exploitable one. It carries the same replicate-sync wire format
change and coordinated-restart requirement as the handshake hardening above,
so it ships in the same release rather than forcing a second cutover.

Contributed by [@pujitha24](https://github.com/pujitha24) in [#715](https://github.com/Basekick-Labs/arc/pull/715).

### Expired API tokens are now rejected on cache hits

Arc caches successful token verifications in memory for `auth.cache_ttl`
seconds (default 300) so that ingestion does not pay a SQLite lookup per
request. That cache checked only its own entry deadline, not the token's
`expires_at`, so a token that expired *while cached* kept authorizing requests
until the entry aged out — up to one cache TTL past the expiry an operator
configured. The same token was correctly rejected whenever the lookup reached
the database, so the behaviour depended on cache state rather than on the
credential.

Token expiry is now enforced on every request regardless of cache state: an
entry whose token has expired is evicted and re-validated against the database,
which rejects it. Cache entries are additionally never held past the token's own
expiry. Revoking, deleting, updating, or rotating a token already invalidated
the cache immediately and was never affected; only passive expiry was.

Full technical detail will accompany the corresponding security advisory once it
is published. Responsibly reported by **[@rexpository](https://github.com/rexpository)**.

### Investigated: per-tenant scoping of the DuckDB sandbox ([#641](https://github.com/Basekick-Labs/arc/issues/641))

No behaviour change in this release. Recorded here because the investigation
settled a question that had been open since the sandbox shipped, and the answer
constrains anything built on top of it.

Arc locks DuckDB down once at startup: it sets `allowed_directories` to every
prefix the deployment needs, then sets `enable_external_access = false`. The
allowlist is therefore the union of all tenants' directories for the life of the
process, so an attacker who got past the read-SQL validator would be bounded by
the deployment, not by the database their token can read. #641 asked whether the
allowlist could be narrowed per query to close that gap.

It cannot, on the handle Arc runs queries through. Measured against DuckDB
1.5.5: `allowed_directories` is GLOBAL-only (there is no session or connection
scope, so two concurrent queries on one handle cannot see different allowlists),
it is immutable once external access is off, and the lockdown is deliberately
one-way. It also cannot be set in the connection string, and no LOCAL-scope
setting in 1.5.5 affects file access. The only mechanism that gives two queries
different filesystem scopes is a second DuckDB instance.

Routing queries to per-scope instances was designed and rejected. The decisive
problem is that the routing key could only come from the same SQL reference
extractor the threat model assumes has already been defeated: a query written as
`read_parquet('...')` yields no table references at all, so the queries the
feature exists to contain are exactly the ones that would produce no key. The
supporting costs were also severe, since DuckDB's thread count and memory limit
are both per instance, and Arc's S3 and Azure credential refreshers are keyed by
secret name in a way that would let a second instance silently stop the first
one's refresher.

`lock_configuration` was evaluated as cheap hardening in the same pass and
rejected for a concrete reason: it blocks the `parquet_metadata_cache` toggle
that Arc performs after every delete, compaction, and retention pass to drop
cached metadata pointing at deleted files, and DuckDB 1.5.5 offers no
lock-immune substitute.

The full analysis, including the measurements and the enforcement point that is
worth building instead (a Go-side assertion that every path literal in the
rewritten SQL is one Arc emitted, tracked in
[#764](https://github.com/Basekick-Labs/arc/issues/764)), is in
`docs/progress/2026-09-12-duckdb-sandbox-scoping.md`. The DuckDB constraints the
decision rests on are pinned by tests, so a future DuckDB bump that lifts one
fails the build rather than leaving the note quietly wrong.

## Upgrade notes

### A node asked to bootstrap Raft on a non-writing role now exits at startup

`cluster.raft_bootstrap` has been a no-op on any node that already had Raft state, so setting it everywhere rather than on one node was harmless and some deployments do exactly that. From this release, a node with `cluster.raft_bootstrap=true` and a role that does not accept writes logs the reason and exits, because bootstrapping on such a role hands the new cluster the precise failure this release fixes: that node is elected, then fails the writer half of every singleton-task check, and there is no second voter that could take leadership away from it.

Set `ARC_CLUSTER_RAFT_BOOTSTRAP=true` only on the node that bootstraps, and only on one that accepts writes. Arc's own Helm chart and Compose files already do. The check does not fire when `cluster.raft_data_dir` is empty, since there is no Raft node to bootstrap.

### A clustered node with an unrecognised `cluster.role` now exits at startup

Previously a typo such as `ARC_CLUSTER_ROLE=Reader` or `writter` started the node as standalone, and in a cluster it joined as one. From this release that node logs the offending value and exits, so a misconfigured pod crash-loops visibly instead of serving from an isolated registry. See the entry under Bug fixes for why exiting is the safer failure.

Check `cluster.role` on every node before upgrading a cluster. The accepted values are `standalone`, `writer`, `reader` and `compactor`, and leaving the key unset means `standalone`. Nodes that joined an older cluster with a role it did not recognise are unaffected on the wire, because every Arc build has always sent its own already-parsed role, but that node will not start again until its own configuration is corrected.

1. **Clustered Enterprise deployments require a coordinated restart.** The
   coordinator handshake **and the replicate-sync handshake** wire formats
   changed (see *Cluster hardening* and *Replicate-sync now authenticates
   `SupportsBinaryEntries`* above): stop all cluster nodes, upgrade the binary
   on every node, then restart all nodes. A rolling restart causes
   cross-version nodes to mark each other unhealthy, can trigger an automatic
   writer failover, and can remove a gracefully-restarted follower from the
   Raft configuration until its leader is upgraded. A cross-version reader
   additionally cannot establish WAL replication with a writer and will fall
   behind until both ends are upgraded. Single-node, non-clustered and OSS
   deployments need no action.
2. **No configuration change is required, with one edge-sync exception.**
   Existing `arc.toml` files and license keys work as-is and no new keys were
   added. The exception: a spoke whose `spoke_id` contains `..`, is longer than
   128 bytes, or has leading or trailing whitespace is now refused, so such a
   spoke fails to start until its ID is changed. See *Edge-sync spoke IDs can
   no longer collide with another spoke's namespace* below. The hub names any
   stored ID in that state at startup.
3. **A misconfigured `storage.s3_prefix` now stops startup instead of silently
   using the bucket root.** If Arc previously started with a prefix containing
   `..`, a space, or any character outside `[A-Za-z0-9/._-]`, it was writing to
   the top of the bucket rather than under that prefix, and it will now refuse
   to boot until the value is corrected. Check the prefix before upgrading: the
   data is wherever it was actually being written, not where the config says.
4. **A few malformed names are now refused where they were previously
   accepted and quietly rewritten** (see *Local storage rejects malformed paths
   instead of rewriting them* below). Three are operator-visible: a retention
   policy whose database or measurement name contains a separator or is empty
   is rejected on create and on update, and existing ones are named in the log
   at startup; an MQTT subscription whose database or topic-mapping target is
   not a usable name now fails at startup; and an edge-sync spoke may no longer
   sync a path with a dot-prefixed segment such as `db/./cpu/x.parquet`. Note
   that `PUT /api/v1/retention/:id` replaces the whole row, so a request that
   omits `database` is now a 400 rather than silently storing an empty name,
   which used to make that policy enumerate every database.
5. **Retention will delete a backlog on its first run if you use an S3 prefix.**
   If `storage.s3_prefix` is set, retention has been deleting nothing at all
   since v26.03.2 (see *Retention deleted nothing on S3 deployments with a
   configured prefix* below), while reporting every run as completed. After
   upgrading, the first run of each policy will remove everything already past
   its cutoff, which on a long-running deployment can be most of the data in the
   affected measurements. This is the policy doing what it was configured to do,
   but it is not a small delete and it is not reversible. Before upgrading,
   check what each policy would remove with a dry run
   (`POST /api/v1/retention/:id/execute` with `{"dry_run": true}`, which is now
   accurate where it previously reported zero), and confirm the retention window
   is still the one you want. Deployments on local storage, on Azure, or on S3
   without a prefix are unaffected: retention has been working correctly there.
6. **Query response envelopes gained two optional keys** (`rows_capped`,
   `row_cap`), emitted only when an Enterprise governance row cap truncated
   the result. Conforming JSON and msgpack decoders are unaffected: the keys
   are absent from every uncapped response, and a capped msgpack envelope
   declares its own map length. A client that hardcodes the msgpack envelope
   at seven or eight keys should read the length instead. Only deployments
   with `governance.enabled = true` and a `max_rows_per_query` policy can see
   them at all.

7. **Edge-sync spoke IDs containing `:` must be re-registered before upgrade.**
   See *Edge-sync spoke IDs no longer create manifest-invalid keys* below;
   existing files remain under the old namespace.
8. **A shutdown that cannot flush every buffer now exits with code 1.**
   Previously it exited 0 regardless. Container runtimes will show the
   container as errored rather than cleanly stopped, and the exit is
   accompanied by a `Retaining WAL files: ...` log line. The data is safe — it
   stays in the WAL and replays on the next start — but operators who alert on
   container exit codes will see a new signal. See *A graceful shutdown could
   delete the WAL that still held unflushed data* below. Pod restart behaviour
   is unchanged (both charts use the Kubernetes default `restartPolicy:
   Always`).
9. **Clustered upgrade order: writers before readers.** A follower's startup catch-up now forwards a
   barrier command through the Raft leader ([#799](https://github.com/Basekick-Labs/arc/issues/799)). A
   leader running an older release rejects the command and the reader falls back to the previous
   behaviour (walking a possibly stale manifest) until the leader is upgraded. Upgrade the writer
   nodes, which are the leader candidates, first.

## Bug fixes

### Configurable compaction cycle budget and cancellation ([#915](https://github.com/Basekick-Labs/arc/issues/915))

Scheduled and manual compaction use the same configurable cycle deadline
(`compaction.cycle_timeout`, default `30m`). Cancellation stops new work,
waits for active workers and records separate completed, failed, interrupted
and discovered-but-unstarted batch counts. Manual execution supports
`POST /api/v1/compaction/trigger?database=db&measurement=cpu`, with `tier`
remaining optional. The measurement filter requires a valid database and
applies to manifest recovery as well as new candidate discovery; recovery
spans every tier regardless of which tier the cycle runs. Recovery and
eligibility failures now fail the cycle, while completed recovery progress
survives cancellation. A manifest that cannot be read is retained and fails
the cycle closed; a manifest that reads but cannot be decoded (for example a
zero-length file left by a crash) is parked under the `.quarantined` suffix
so it stops blocking compaction, and candidate filtering ignores it until
then (#926 counts those parks). Normal cancellation does not enter the
adaptive retry path or emit misleading batch-failure logs.

Increasing the deadline does not reduce peak memory demand or guarantee
completion.

### Measurement fields bind the same over every time range ([#914](https://github.com/Basekick-Labs/arc/issues/914))

A field that no Parquet file in the queried time range carried failed to bind
(`Binder Error: Referenced column "x" not found`), while the same projection
over a wider range succeeded with NULLs. Arc rewrites `FROM measurement` into
`read_parquet(<selected files>, union_by_name=true)`, so DuckDB only ever saw
the selected files' columns, and a Grafana panel worked or broke depending on
the zoom level. Empty ranges fell back to the whole measurement and advertised
every column, which made runtime schema discovery misleading as well.

Every measurement now has a registered field schema: a zero-row Parquet
"anchor" stored at `_schema/{database}/{measurement}.parquet`, maintained by
ingest on every flush (HTTP, MQTT, WAL replay, replicated ingest entries and
the import API all go through it) and listed first in every `read_parquet` the
query path emits, including the parallel partition path, tiered queries and
continuous queries. A registered field absent from the selected files binds
as a typed NULL column, `SELECT *` has the same columns in the same order over
any range, an empty range returns zero rows with the full schema, and an
unknown field still raises a Binder Error. The plan stays a single Parquet
scan with projection and filter pushdown.

The anchor records the narrowest type seen for a field (BOOLEAN below
TINYINT below SMALLINT below INTEGER below BIGINT below FLOAT below DOUBLE
below VARCHAR, DECIMAL below DOUBLE, a DECIMAL with smaller precision and
scale below a larger one, TIMESTAMP below TIMESTAMPTZ); a pair DuckDB cannot
order, such as BIGINT against DECIMAL, is recorded as BOOLEAN, which DuckDB
promotes to every other type. Either way the anchor never changes what a
query binds where files carry the column; DuckDB keeps promoting per query
where files disagree, and conflicting writes are logged. Fields are only
ever added; deleting a database deletes its anchors, and nothing else
removes one (a measurement emptied by retention keeps its anchor).

Measurements written before this release get an anchor in the background the
first time they are queried, built from their files
(`query.stable_schema_bootstrap`): every file when the measurement holds at
most `query.stable_schema_bootstrap_max_files` of them (default 500),
otherwise a sample of that size, newest days first, compacted files
preferred. Until it exists, queries behave as before. `GET /api/v1/databases/{db}/measurements/{m}/schema`
returns the registered fields and types; `POST .../schema/rebuild` (admin)
queues a rebuild. `query.stable_schema = false` restores the previous SQL
byte for byte. The stored anchors are shared state on the storage backend,
read by every node and re-read on a short TTL, so a field added on one node
binds on the others within a minute; each node keeps the copy DuckDB reads
under its upload directory. `_schema/` is a reserved root directory:
compaction, reconciliation, tiering, edge sync and the Iceberg exporter skip
it. Backups copy it with the rest of the storage root, as auxiliary files
outside the database inventory (#927), and restore it with the data. A node
that receives Parquet files from a peer rather than through its own ingest
path relies on bootstrap for those measurements.

`scripts/range_schema_acceptance.py` runs the original reproducer (a late
field inside one day, a field first written 59 days after the earlier day was
compacted, a field that stops being written, daily compaction and restarts)
against a native build; see `docs/testing/range-schema-26.09.2.md`.

### Parked unparseable compaction manifests are counted ([#926](https://github.com/Basekick-Labs/arc/issues/926))

Since #915, recovery parks a crash-recovery manifest whose body does not
decode (typically a zero-length file left by a crash before the rename was
durable) under the `.quarantined` suffix, so it stops holding back every
compaction candidate on the node. The only signal was one Error log line.
A new counter, `arc_compaction_manifests_parked_unparseable_total`
(`compaction_manifests_parked_unparseable_total` in the JSON snapshot),
increments once per successful park, after the parked copy and the delete
both landed, never on a park that failed and will be retried. Growth means a
manifest stopped blocking compaction without being completed: the parked
file name gives the tier, database and job, and that partition should be
checked for a zero-length `_compacted` output or for duplicate rows. The
existing `arc_storage_invalid_path_quarantined_total` keeps counting the
other park route, an output key no backend can address.

### Backups no longer list the schema anchor directory as a database ([#927](https://github.com/Basekick-Labs/arc/issues/927))

The field schema anchors under `_schema/` (#914) are Parquet objects, so a
backup inventoried them as a database named `_schema` with one
"measurement" per real database: a bogus entry in the manifest, in
`GET /api/v1/backups` and in the database count. They are now recorded as
auxiliary files, reported in the manifest's new `auxiliary_files` count,
and kept out of `databases`. They stay inside `total_files` and
`total_size_bytes` on purpose: the restore compares that count against
every Parquet object it finds, and an anchor left out of it would have
hidden a missing data file. Restore copies every object under `data/` back,
anchors included; the one exception, compacted inputs a backed-up recovery
manifest shows were already replaced by their output, is #930 below.
Backups written before this release restore the same way.

### Empty time ranges can be answered from the schema anchor (experimental, [#928](https://github.com/Basekick-Labs/arc/issues/928))

When partition pruning found no directory for a query's time range it fell
back to the whole measurement glob, so an empty dashboard panel scanned every
file of the measurement to return zero rows. With `query.empty_range_anchor_scan
= true` (default off), a range proven empty is answered by scanning the
measurement's field schema anchor alone: zero rows, the registered columns,
and no data file opened.

The proof is deliberately narrow, because the range the pruner extracts is a
regular-expression reading of the WHERE clause and today's fallback is what
keeps its imprecision harmless. It applies only to a single-table query
(no JOIN, subquery, CTE or set operation) whose WHERE clause is a conjunction
with both bounds stated as bare `time` comparisons against a literal or
`NOW() +/- INTERVAL`, over at most 7 days, on a measurement whose directory
holds year directories (a hub's spoke namespaces do not qualify), whose
anchor is complete (created by ingest for a measurement that had no files
yet, or bootstrapped from every file), and only after every generated
partition directory was verified absent by listings no older than two
seconds; any listing failure keeps the full scan. A proven-empty verdict is
never cached. Everything outside those conditions behaves exactly as before.

An anchor's completeness is recorded on the stored anchor. Anchors created
before this release, or by ingest over files that predate the registry, are
incomplete and keep the full scan; `POST .../schema/rebuild` on a measurement
with at most `query.stable_schema_bootstrap_max_files` files reads every file
and makes it complete. Completeness assumes every data file of the
measurement passes through this node's ingest: a cluster whose nodes have
separate storage and receive each other's files by replication, or a restore
into an existing measurement, can leave a column unregistered on the
receiving node, so keep the flag off there or rebuild after such events.

### A restore no longer serves compacted rows twice ([#930](https://github.com/Basekick-Labs/arc/issues/930))

A compaction job writes its crash-recovery manifest under
`_compaction_state/`, uploads the compacted output, deletes the input files,
and only then deletes the manifest. Backups copied Parquet files and Iceberg
metadata but never those manifests, so a backup whose listing fell between
the upload and the input deletion held both the output and its inputs with
nothing to reconcile them. A restore put both back, and every row of that
partition was served twice, permanently.

Backups now copy the compaction state (manifests and parked `.quarantined`
manifests) before the data files. The order matters: a job that finishes
during the copy then leaves the backup with the manifest and the output,
which recovery completes, never with the output and the inputs and no
manifest. A manifest that cannot be read while it still exists fails the
backup rather than being skipped, for the same reason. The state is reported
in the backup manifest's `compaction_state_files` and counted in the backup
progress but not in `total_files`, which the restore compares against the
Parquet objects it finds (the schema anchors of #927 are Parquet and stay
inside that count; manifests are not).

The restore reconciles the state itself rather than waiting for a
compaction cycle, because a cycle may be disabled on the restored node or may
race the restore. Before copying, it reads the backed-up manifests and does
not restore the inputs of any manifest whose output the backup holds intact
(present, and of the size the manifest recorded); the restore progress
reports them as `consumed_inputs_skipped` and the restored manifests as
`compaction_state_restored`. The next compaction cycle finds the output,
tolerates the absent inputs, fires the receipt hooks and retires the
manifest. A manifest whose output the backup does not hold, or holds
damaged, keeps its inputs, and recovery deletes the manifest (and a damaged
output) so compaction retries. If metadata was restored as well, restart
before that cycle so the staged metadata is applied first; on an edge sync
hub, a cycle that runs before the output has landed retires the manifest
without marking receipts, which the hub's discovery then covers. A restored
manifest older than seven days logs compaction's stale warning once when
processed; that is expected after a restore.

This applies to backups taken with this release; a mid-compaction backup
taken by an earlier release has no manifest to reconcile with. Restoring a
mid-compaction backup onto a store that has since compacted the same
partition again is a separate, pre-existing duplication that no manifest
covers.

### Peer file fetches now respect the overall timeout ([#796](https://github.com/Basekick-Labs/arc/issues/796))

The configured `cluster.replication_fetch_timeout_ms` did not reliably bound a file fetch. Reading the acknowledgement header could replace the context deadline with a longer timeout, and the subsequent body transfer could block indefinitely if a peer stopped sending data.

Fetches now keep the overall deadline effective across the request, acknowledgement and body transfer. Cancelling the fetch also closes the connection to unblock stalled network reads. The coordinator derives the acknowledgement timeout from the configured fetch budget, and regression tests cover stalled headers, partial bodies and cancellation.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#899](https://github.com/Basekick-Labs/arc/pull/899).

### Compaction preserves files with identical basenames ([#826](https://github.com/Basekick-Labs/arc/issues/826))

Daily compaction previously downloaded files from different hour partitions using only their basenames. Identically named files could overwrite one another, potentially duplicating some rows and losing others.

Temporary input filenames now include the download index, and exclusive file creation prevents accidental overwrites. Regression tests cover filename collisions, existing temporary files, and real Parquet compaction preserving rows from both hour partitions.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#898](https://github.com/Basekick-Labs/arc/pull/898).

### Replication sequence tests no longer depend on warmup delivery timing

The sequence handshake tests now initialize the sender's sequence directly instead of queuing warmup entries. Pending warmup entries could reach a newly connected reader and advance its sequence before the assertion, causing intermittent CI failures on unrelated changes. The restart test still verifies delivery of a real entry after the handshake; production replication behavior is unchanged.

Contributed by [@xe-nvdk](https://github.com/xe-nvdk) in [#900](https://github.com/Basekick-Labs/arc/pull/900).

### Compaction dropped the implicit "time" sort key when a custom sort key was configured ([#792](https://github.com/Basekick-Labs/arc/issues/792))

Ingest sorts each flush by the configured sort keys with `time` appended last, so rows land in `(configured-keys..., time)` order within a file. Compaction rebuilds long-lived files from many such inputs and is meant to preserve that order — the comment above the `ORDER BY` call site says so directly — but `Manager.GetSortKeys` returned the configured keys unchanged, without appending `time`. With any custom `ingest.sort_keys` or `ingest.default_sort_keys` configured, compacted files were re-sorted by the configured columns only, and rows within each group landed in whatever order the multi-file `read_parquet(..., union_by_name=true)` scan produced, not time order.

The visible effect is narrower query pruning on compacted data: `time` row-group min/max statistics widen once the intra-group time ordering is lost, so DuckDB skips fewer row groups on time-filtered queries. The default configuration (`default_sort_keys = "time"`) was unaffected, since `["time"]` was already the same list with or without the append.

`GetSortKeys` now appends `time` the same way ingest's `getSortKeys` in `arrow_writer.go` already does, so compaction and ingest agree on sort order again.

Contributed by [@pujitha24](https://github.com/pujitha24) in [#890](https://github.com/Basekick-Labs/arc/pull/890).

### WAL replication attached to an arbitrary writer, not the primary ([#885](https://github.com/Basekick-Labs/arc/issues/885))

A replica chose which writer to stream WAL entries from by walking the node registry and taking the first healthy writer it found. It never looked at whether that writer was the primary. In local-storage mode only the primary ingests, so a standby writer's replication sender has nothing to send: a replica attached to one received no live entries at all, silently, for as long as it stayed attached. With this release's three-writer default that was roughly two replicas in three.

This was never data loss. Flushed Parquet reaches every node through the Raft file manifest and the peer-pull path, independently of the WAL stream, so an affected replica was stale by about one flush interval (`ingest.max_buffer_age_ms`, five seconds by default) plus pull latency rather than missing rows. What it lost was the live window the WAL stream exists to provide.

A replica now prefers the designated primary and falls back to any healthy writer only when no primary exists — the same preference, in the same order, that write routing already made. It also re-evaluates that choice: on a writer promotion, on a writer demotion, and on a periodic re-check. The re-check is the one that matters most in practice. A replica that joins a cluster which elected its primary *before* it joined never sees a promotion event at all, because the join response carries no writer state, so its first choice is necessarily the fallback; the periodic pass is what upgrades it. Restarts outnumber live hand-overs.

Re-targeting replaces the receiver rather than re-pointing the existing one, for the reason in the next note.

### A writer restart wedged WAL replication until the sequence caught up ([#887](https://github.com/Basekick-Labs/arc/issues/887))

Replication entries carry a sequence number, and a receiver enforces that it strictly advances — a repeated or backwards sequence means a replay or a misbehaving writer, and the connection is dropped. But that sequence is an in-memory counter that restarts at zero every time a writer process starts, while the receiver's high-water mark deliberately survives reconnects.

So a writer restart put the two permanently out of step. The receiver, holding a mark from the writer's previous process, rejected the first entry of the new one, dropped the connection, reconnected, and rejected it again — a loop that applied nothing at all until the restarted writer had emitted more entries than its predecessor ever had. On a low-rate deployment that is hours. The replica stayed on the file-manifest path throughout, so again this was staleness rather than loss, but it was unbounded staleness and nothing in the logs named a cause beyond a repeating sequence warning.

A receiver now reconciles with the writer's sequence space during the handshake: if the writer reports a position below the receiver's mark, the receiver rewinds to match. A writer that is merely *ahead* — the ordinary "we missed entries while disconnected" case — leaves the mark alone. The strict-advance check is a within-connection replay defence and is unchanged; what protects entries across connections is the per-connection session key, which is derived fresh from a new handshake nonce every time and so does not depend on the sequence at all.

This is also what makes the re-targeting above safe. Pointing a receiver at a different writer walks into exactly the same mismatch, since no two writers share a sequence space, which is why a re-target builds a new receiver instead of re-using the old one.

A related arithmetic fault is fixed alongside it: the writer's "can this reader resume" calculation subtracted the replication buffer size from the current sequence without guarding the underflow, so it reported false for every connection made before the writer had emitted one buffer's worth of entries — precisely the freshly-restarted writer this note is about. The value is reported in logs and read by no decision today; the fix is so that it stays correct when something does read it.

### A demoted writer kept accepting replication connections

Nothing checked, when a reader asked a writer to stream to it, whether that writer was the primary. A writer that had been demoted still had a running sender, so it accepted the connection and held it open without ever sending an entry.

In local-storage mode a writer now refuses replication sync when the cluster has designated *another* node as primary, naming that node in the refusal. The condition is deliberately that narrow. A cluster that has designated nobody — one still starting up, or one where a hand-over found no eligible successor — accepts as before, because there is no basis on which to prefer one writer over another and refusing would leave a starting cluster unable to replicate at all.

Both sides now read the same record to decide this: the replica picks its source from the cluster's durable designation rather than from its own health-filtered view of the registry. Those two answers diverge exactly when the primary looks unhealthy, and a replica acting on the second would have been refused by every writer acting on the first. The same applies when the primary has dropped out of the replica's registry entirely — evicted, or restarted and not yet re-discovered — so the address is resolved from the cluster record too, rather than falling back to a writer that is certain to turn the replica away.

Shared-storage clusters are unaffected: every writer there ingests and none is designated primary, so the check is not applied.

### Two nodes could compact the same data at once

Compaction checked whether this node was allowed to compact when its scheduler started, and never again. That check runs before the cluster coordinator exists, so it fell back to the node's configured role: a node whose role is compactor armed its schedule unconditionally, whether or not it actually held the compactor lease.

Nothing stopped it afterwards either. The callback that shuts a compaction schedule down fires only on the node that *held* the lease and lost it, so a node that never held it was never told. On any cluster where a writer took the lease — which is every cluster where the compactor was slower to join, and every existing one after the fix above — the dedicated compactor and the lease-holding writer both compacted the same partitions, producing two sets of outputs over the same rows and registering both.

The check now runs on every tick, which is what the cluster-operations rule in this repository has always required and what the retention, continuous-query, Iceberg and reconciliation schedulers already did. A lease change now takes effect without a restart, which is the other half of the same rule.

### An upgraded cluster could keep a reader or compactor voting in Raft, with nothing reporting it ([#880](https://github.com/Basekick-Labs/arc/issues/880))

Arc now grants a Raft vote only to nodes that can accept writes, so a reader or compactor cannot win leadership and stall every task that runs on exactly one node. That rule applies when a node **joins**. It does not change a vote a node already holds, because adding a server that is already a voter as a non-voter updates its address and leaves its suffrage alone — that is how the underlying Raft library works, deliberately.

Most clusters converge anyway. A node that shuts down gracefully announces it, the leader drops it from the Raft configuration, and its next start is a clean add with the right suffrage; a rolling upgrade does that for most of a cluster as a side effect. What does not converge is a node killed ungracefully, a node whose departure arrived while there was no leader to process it, the departing leader itself, and any node re-added while it was still in the configuration.

So a cluster could sit indefinitely with a reader holding a vote, and there was nothing to look at: the only record of a server's suffrage was inside a diagnostic string in the Raft statistics block.

**`GET /api/v1/cluster` now reports the voter set.** A new `raft.membership` block lists every server with its suffrage and the role the cluster has on record for it; where that role is one Arc recognises, it also carries the suffrage the role calls for and whether the two agree. A server whose role cannot be determined — an entry left by a version that recorded something Arc no longer knows, or one present in the Raft configuration but in no node record — is reported as unresolved, with no verdict attached, and is never acted on. The block also carries counts of voting servers, disagreements and unresolved entries, and names the node it came from, because a follower's view of the Raft configuration can lag the leader's.

When disagreements persist, the leader now says so in the log, once a minute, pointing at the endpoint below.

**`POST /api/v1/cluster/voters/converge` fixes it.** Admin-only. It plans by default: send `{"dry_run": false}` to act. It only ever **revokes** votes, never grants them — granting one raises the number of nodes that must agree before anything can change, and if the newly-voting node is down or behind, nothing can commit, no further membership change is possible, and the cluster loses its leader with no way back. Revoking is safe in the other direction because it is self-repairing: a node that should vote gets its vote back the next time it joins.

Revoking is not automatically safe either, and the endpoint refuses rather than guesses:

- It will not revoke a vote if doing so would leave the remaining voters without a healthy majority. A configuration change takes effect the moment it is made, so removing a live voter from a group whose survivors are down strands the change permanently — it can never be agreed, and the cluster cannot elect a leader again.
- It will not drop a cluster to a single voter unless you say so explicitly with `{"allow_single_voter": true}`. One voter works until that node dies, after which nothing can ever elect a leader.
- A server whose role the cluster cannot determine is reported and left alone. Those entries are disproportionately nodes that are already gone, which is exactly what must not be counted on for a majority.
- If the node you call is itself voting when its role says it should not — which is common on a cluster upgraded from before the rule existed, where every node was given a vote regardless of role — it hands leadership to a node whose role does vote and tells you to re-run against the new leader. Without that step it would revoke everyone else's vote, report success, and leave the cluster in the state you were trying to fix.

If a revocation fails, the endpoint stops there rather than continuing down the list, and reports what it did and what it did not. Carrying on would apply the remaining changes to a voter set that was never checked, which is the one way the safety rules above can be defeated.

An unreadable or absent request body is treated as a dry run rather than rejected: the safe reading of an unclear instruction to change cluster membership is to not change it.

Convergence is deliberately never automatic. Doing it on its own when a node takes leadership was considered and rejected for this release: it is the kind of change that is safest when an operator chooses the moment, can see the plan first, and is watching when it lands.

### The compactor lease never moved to a dedicated compactor, and there was no way to move it by hand ([#876](https://github.com/Basekick-Labs/arc/issues/876))

The other half of the fix above. A cluster hands the compactor lease to the best candidate it can see when it first assigns one, and prefers a node whose role is `compactor` — but if no such node is visible at that moment it falls back to a writer, and there it stayed. The lease was only ever moved again when its holder became *unhealthy*. A healthy writer kept it for the life of the cluster.

Two ordinary routes into that state. The compactor pod is slower to join than the leader's first assignment tick, which is a race it loses on most cold starts. Or you upgrade past the chart fix below, which is the first time a compactor pod ever joined at all — so every existing chart cluster has a writer holding the lease precisely because no compactor was ever visible. In both cases a node provisioned for compaction, with its own CPU and memory budget and its own volume, sat idle while a writer compacted on top of ingest.

There was no operator lever either. An internal manual-failover function existed and had its own unit test, but nothing routed to it: no endpoint, no CLI verb, no config. The only way to move the lease was to make the holder unhealthy — restarting a node that is also taking writes.

Three things change.

**The lease now moves to a dedicated compactor on its own.** When a writer holds it and a node whose role is `compactor` has been healthy for six consecutive checks (a minute at the default interval), the lease is handed over once. The sustained window is what stops a compactor that flaps between healthy and unhealthy from attracting the lease, and a cooldown bounds how often it can move at all. Nothing else is preempted: the lease never moves between two writers, and never off a dedicated compactor.

**`POST /api/v1/cluster/compactor/assign` hands it to a node you name.** Admin-only, body `{"node_id": "..."}`. The target is explicit and required, which is deliberately the opposite of the writer hand-over endpoint — there, clearing the designation is what lets the cluster elect, while here the automatic choice landing in the wrong place is the whole problem, so picking for you would reproduce it. Readers are refused, as are unhealthy nodes, a node that already holds the lease, and a request to a node that is not the Raft leader. Writers are accepted: automatic failover has always been able to hand the lease to a writer, and must, or a cluster whose only compactor dies stops compacting entirely.

Note that this is an override with an expiry, not a permanent setting. Automatic preemption is suppressed for the cooldown the response reports (`preemption_suppressed_seconds`), and after that the lease returns to a dedicated compactor if one is healthy. To keep compaction off a node for good, change that node's role or remove it from the cluster.

**The lease is visible.** `GET /api/v1/cluster` now carries an `active_compactor` block — who holds it, whether that node is a dedicated compactor, and whether it is still in the registry at all — and every node in `/api/v1/cluster/nodes` carries `is_active_compactor`. Previously the only record of a lease assignment was a log line written once, at the moment it happened.

One smaller fix came out of the same work: the initial assignment of the lease at cluster start armed the *failover* cooldown, so a compactor that died shortly after a cluster came up waited out a window meant for damping repeated failovers — after an event where nothing failed and nothing was lost. Assignments and failovers are now accounted separately.

Two limitations worth knowing, neither introduced here. A node removed from the cluster while it holds the lease shuts its Raft down before it applies the removal, so it never learns it lost the lease and keeps compacting until the process is stopped — stop it, don't just remove the node. And the lease is not released when its holder is removed; it moves on about thirty seconds later, once the holder reads as unhealthy. Releasing it immediately sounds like an improvement and is not: an empty lease means "no lease" to every consumer, which puts the cluster back into the state where each node decides for itself whether to compact, and on a cluster with no failover manager it would stay there.

`cluster.failover_cooldown` now also sets how long an operator override of the compactor lease lasts, at ten times its value — 600 seconds at the 60-second default. Setting it to `0` means the default, not "no cooldown". The assign endpoint can answer `409` while an automatic lease change is already in flight; retry.

### Every writer ran retention and continuous queries when automatic failover was off ([#872](https://github.com/Basekick-Labs/arc/issues/872))

On local storage, retention, continuous queries and deletes are meant to run on one writer. Deciding which one requires a promotion through Raft, and nothing issued one unless writer failover was both enabled and licensed. With no promotion, every writer-role node considered itself the primary and ran all of it. Three writers meant three nodes executing the same continuous queries and the same deletes.

Arc now elects a primary writer on any local-storage cluster with Raft, whatever `cluster.failover_enabled` says and whatever the licence contains. Having one writer in charge is not a paid capability; it is the difference between a working cluster and one doing everything three times.

What the flag and the licence gate is unchanged and is what the feature is named for: **automatic** failover, meaning a replacement chosen for you when the primary dies. A cluster without it elects a first primary and keeps it. If that primary goes away, nothing promotes a successor, and Arc says so in a rate-limited warning rather than leaving you to infer it.

The line between the two is drawn on durable cluster state rather than on anything a process remembers. A cluster that has never designated a primary needs one, and that is bootstrapping. A cluster whose designated primary is unhealthy needs a replacement, and that is failover. The old code made this distinction with an in-memory field, which is empty on a freshly started process — so a leader restart or a leadership change looked like a cluster that had never had a primary, and it would elect one. That was a replacement under another name.

**New: `POST /api/v1/cluster/writers/{id}/demote`.** Hands the primary-writer role off the named node so the cluster elects a new one. Admin-only, and not gated on the failover licence, because it is how a cluster without automatic failover recovers: the record still names the node that died, so nothing elects until an operator clears it. On a cluster that does have automatic failover, it is also how you drain a writer deliberately. It refuses a node that is not the current primary rather than appearing to succeed, and it names which node the cluster actually has on record.

Handing over is a promotion of somebody else rather than a demotion of the node named, because a promotion already carries the demotion with it and announces both sides at once. Demoting and letting the cluster elect would leave it free to choose the same node straight back, which is not a hand-over. When there is nobody else to promote, the designation is released anyway so the cluster is not pinned to a writer that may never return, and the response says plainly that nobody took it.

Two things were fixed that this endpoint would otherwise have exposed. A demotion announced nothing, so the node being demoted kept believing it was the primary and kept running the singleton work, while the cluster still saw a live primary and never elected anyone. And removing the designated primary left the record naming a node that no longer existed, which meant no election could ever run again — no primary, no retention, no continuous queries, no deletes, and nothing reporting why. Removing the dead primary is the obvious operator move, so that was the likely path.

**Behaviour change worth knowing about.** On local storage, `/ready/write` now reports not-ready on a writer that is not the primary. Clusters with automatic failover already behaved this way; clusters without it reported every writer ready, which was consistent with every writer also running the singleton work, and both were wrong. A load balancer pool built on that endpoint will drop to the primary alone. Kubernetes pod readiness is unaffected, since the chart probes `/ready`.

A crash was fixed along the way. The failover path derived its timeout from a context that only exists once the manager has started, while the callback that can reach it is wired one line earlier, with health checks already running. Hitting that window panicked the goroutine and took the process down.

### A node that left gracefully and restarted was never listed again ([#858](https://github.com/Basekick-Labs/arc/issues/858))

Restart a cluster's Raft leader with a normal shutdown and it came back healthy, serving, and invisible. It listed every node; every other node listed everything except it. Nothing recovered from that. On local storage the detached node also kept its primary-writer designation across the restart and went on running retention and continuous queries the rest of the cluster had moved past.

Two things combined. A node broadcasts a leave on shutdown and each peer drops it from its local list, but the removal from shared cluster state happens only on the leader. When the departing node is itself the leader, no peer is leader at that moment, so nothing removes it from shared state while every peer has already dropped it locally.

The node should then re-announce itself on restart, and that is the part that failed. Peer discovery stopped as soon as Raft knew who the leader was. A node restarting into a cluster it was still configured in learns that within about a second, so it got one attempt, and that attempt fell inside the leaderless window its own departure had created. It failed for want of a leader to talk to, and nothing tried again.

Discovery now stops when a join has actually succeeded rather than when a leader happens to be known. The choice is deliberately "have I joined" and not "does the cluster list me": a node an operator has removed on purpose must stay removed, and re-deriving membership every few seconds would quietly undo that. As a side effect it fixes a removal that was already being undone, since a removed follower keeps a live Raft whose leader pointer clears on the next timeout.

Discovery alone is not enough, because a node that takes leadership has nobody to join. That is not a corner case: where the restarting node is the cluster's only voter, which is the shape of a single-writer deployment, it wins its own election every time. So a node that takes leadership at startup now re-announces itself in cluster state, where before it skipped that whenever its own record was still present, which is exactly when it is needed. Both paths are verified on three-node and two-node clusters of real binaries, including by removing each fix and watching the cluster stay broken.

A node also no longer re-joins the cluster it is in the middle of leaving, which the previous condition prevented by accident and this one has to prevent deliberately, including for a join already in flight when the shutdown begins.

### A reader or compactor could win Raft leadership and stall every singleton task ([#862](https://github.com/Basekick-Labs/arc/issues/862))

On shared storage, retention, continuous queries and deletes run on the node that is both the Raft leader and a writer. The role half of that is deliberate: a reader must never run them against the shared bucket. But every node that joined became a Raft voter regardless of its role, so a reader or the compactor could be elected leader. When that happened the leader failed the role half and every writer failed the leader half, so **no node in the cluster passed the gate** and all of that work simply stopped, with nothing to force leadership back to a writer.

Suffrage now follows the role. Nodes that accept writes vote, which is writers and standalone nodes. Readers and compactors join as non-voting members: they replicate the log and see all cluster state exactly as before, they just never campaign and cannot be elected. Compaction is unaffected either way, because it gates on the compactor lease rather than on writer state.

Arc also refuses to start a node that is asked to bootstrap a new cluster on a role that does not vote. That node would be the only server in the cluster and an ineligible leader by construction, so the cluster would never elect anyone and nothing would recover from it.

**Most of an existing cluster converges on its own, and the rest does not.** A node that shuts down gracefully broadcasts a leave, and the leader removes it from the Raft configuration, so its next start is a fresh join that gets the correct suffrage. A rolling upgrade is a rolling restart, so it converges most of the cluster as a side effect.

What stays behind: a node terminated ungracefully, one whose leave arrived while there was no leader to process it, and the leader itself, since nobody else is leader at that moment to remove it. Those keep their vote indefinitely, because adding a server that is already a voter as a non-voter updates its address and leaves its suffrage untouched. The manual fix is to remove the node through the cluster API and let it re-join. Converging automatically is [#880](https://github.com/Basekick-Labs/arc/issues/880), and it is deliberately separate: the promotion half of such a reconcile is the one path that can leave a cluster permanently leaderless, and Arc has no cluster-recovery path to undo that.

**Quorum is now a property of the nodes that can ingest.** Three writers survive losing one, which is the documented shape for both patterns and what the charts have asked for since the writer default moved to three. A cluster with a single writer makes that writer the only voter: it is always the leader, but losing it takes Raft's quorum with it, so token and permission writes and cluster reconfiguration stop too, where previously the readers would have kept Raft alive. That is the intended trade. A cluster that cannot ingest should not be reporting itself as healthy, and it is the same conclusion the writer-redundancy warning reaches from the promotion side.

### A node role Arc does not recognise is no longer silently treated as standalone ([#848](https://github.com/Basekick-Labs/arc/issues/848))

`ParseRole` answers "what role should this node have", and for an unset value standalone is the right answer. That made it the wrong function for the two callers asking "is this a role at all", because a typo and a deliberate standalone became the same node.

Starting a node with `ARC_CLUSTER_ROLE=writter` produced a node reporting itself as standalone. Standalone ingests, so nothing looked broken, and the writer the operator thought they had was not one: in shared-storage mode it would never pass the primary-writer gate, and it did not count toward the writer redundancy the chart and the new warning both ask for. The coordinator did carry a check for this, but it could never fire, because the fallback had already turned the typo into a valid role.

A clustered node now exits on an unrecognised role, naming the value and listing the ones it accepts. Exiting rather than logging, because the alternative is worse than the bug: the coordinator's own error is not fatal, it falls through to standalone mode, and a node that quietly stops clustering also loses its file registrar. It would keep writing Parquet that never enters the Raft manifest, invisible to readers and to compaction. This matches the shared secret, which has failed the same way for the same reason since 26.06.2.

The same fallback applied to the role a joining node presents. It was stored as sent, so a node that holds the cluster shared secret and presents a role nobody configured was recorded as ingest-capable, passed the manifest-command role gate, and appeared in listings with a role no operator had chosen. The join is now refused. This is not a trust-boundary fix, since such a node already holds the shared secret; it is the difference between a cluster whose recorded roles mean something and one whose roles are whatever was sent.

Nodes always send their own already-parsed role, so a cluster upgrading from an older build is unaffected, including one where a node was misconfigured: that node has been running as standalone and says so.

A role that a cluster recorded before this change is still accepted, because dropping an existing member during a restore would be worse than carrying it, but it is now logged rather than absorbed in silence. That happens where such a record actually arrives, which is Raft log replay and snapshot restore.

### The Helm chart's compactor pod never joined the cluster ([#870](https://github.com/Basekick-Labs/arc/issues/870))

Every Arc node joins Raft, whatever its role. The Enterprise chart gave the compactor pod a coordinator address but no Raft address, so its Raft transport refused an address it could not advertise, the coordinator failed to start, and the node carried on in standalone mode. The pod passed its health checks the whole time.

The damage was not that the compactor sat idle. It was that it kept working, alone. A node outside the cluster falls back to its configured role for the decision of whether to compact, and its role says yes, so it compacted the shared bucket on its own schedule while the node actually holding the compactor lease compacted it too. Two compactors on one bucket is the duplicate-output hazard the single-compactor design exists to prevent. Joining the cluster ends the part where it compacts from outside; the entry below ends the part where it compacts from inside without holding the lease.

Readers shipped with the same gap once and were fixed. The compactor was never given the same treatment, and the helper that carries the Raft settings still described them as writer-only, which is the belief that produced both. It now applies to every role, so each one binds its Raft port explicitly instead of relying on a default that merely happened to match. A rendered-template check in CI asserts that every clustered role carries a Raft address, against the default values and both deployment presets.

The local-storage preset also still asked for a single writer, so following the Pattern 1 guide silently overrode the new default of three back down to one. It now matches.

**Upgrading:** applying the new chart restarts the compactor pod, after which it joins the cluster for the first time and stops compacting behind the cluster's back.

It will not immediately start compacting on the cluster's behalf either. The compactor lease is deliberately never preempted: whichever node holds it keeps it until it becomes unhealthy. On an existing cluster that node is a writer, chosen precisely because no compactor was visible. So after the upgrade a writer continues to do the work. Until the fix below, the compactor pod did not idle while that happened — it compacted too, which is the duplicate-output problem in a second form. That is fixed; what remains is that the node you provisioned for compaction is not the one doing the work. To hand the lease over, restart the writer that holds it. That node is named in the `Compactor lease assigned` log line on the Raft leader, which is the only place it is reported: no endpoint exposes the current holder today. Tracked as [#876](https://github.com/Basekick-Labs/arc/issues/876).

Two things this does **not** change in the chart's default shared-storage mode, contrary to what you might expect. Compacted files are still not registered in the Raft manifest, because that path is gated on peer replication, which shared storage does not use. And the "No compactor elected" warning was never firing there for the same reason, so its absence is not evidence of anything.

### An unlimited Enterprise license rejected every cluster join ([#869](https://github.com/Basekick-Labs/arc/issues/869))

Licenses on the unlimited tier carry a core limit of `-1`. The cluster join validator treated only `0` as unlimited, so it compared every joining node's core count against `-1`, found it larger, and refused. The seed node ran alone, every other node retried forever, and the log said "cluster core limit exceeded ... license limit=-1". A cluster on that tier could never form. Single-node deployments were unaffected, which is why this survived: the check only runs when a second node tries to join.

Any non-positive limit now means unlimited, which is what the rest of Arc already assumed. The startup clamp that pins GOMAXPROCS, DuckDB threads and flush workers to the licensed core count returns early on a non-positive limit, and the cluster status endpoint only reports remaining cores when the limit is positive. The join validator was the sole outlier.

### A cluster with too few writers now says so instead of failing silently later ([#856](https://github.com/Basekick-Labs/arc/issues/856))

Arc's clustering documentation described the local-storage pattern as one writer plus several readers and called the readers the failover pool. They are not. Promotion only ever considers writer-role nodes, and nothing changes a node's role at runtime, so in that topology the loss of the single writer stopped ingest until an operator intervened. The chart, the example overlay and the documentation now all call for three writer-role nodes, and Arc no longer waits for the outage to tell you.

A cluster running below three writer-role nodes now logs a rate-limited warning that names the count and the fix. The count is by role rather than by health, because this is a statement about how the cluster was deployed and not about who happens to be up. A writer that is down right now already surfaces, as an unhealthy node and then a dead one.

The deficit has to hold for two minutes before anything is logged. That covers the two ways a healthy cluster passes through a low writer count on its way somewhere else. A cluster is short of writers for the first seconds of its life while peers are still joining. And a rolling upgrade cycles one writer at a time, and a leaving node tells its peers to drop it, so a correct three-writer cluster walks through two writers on every node, every time. A single-node install never warns at all: it has no redundancy of any kind and its operator knows that. The shape this exists for is the one that looks highly available, several nodes of which exactly one is a writer.

Every cluster mode gets the warning, with the right explanation for each. With local storage and failover enabled, the message explains that readers are never promotion candidates. With shared storage, that writer promotion is deliberately suppressed in that pattern, so the load balancer's backend count is the only thing between a writer crash and an ingest outage. With local storage and no automatic failover, because the flag is off or the license does not carry the feature, it says that a primary is elected but no replacement will be chosen when it goes, and it names the endpoint that hands the role over by hand.

### Shutdown now stops replication, and stopping it cannot deadlock ([#853](https://github.com/Basekick-Labs/arc/issues/853))

Shutting a node down never stopped its replication sender or receiver. They exited only when the shared context was cancelled and were never waited for, so a receiver could still be applying entries while the write-ahead log and the in-memory buffer were being closed underneath it, since every shutdown hook runs before any component is closed. Both are now stopped and joined as part of the coordinator's shutdown, before the Raft node goes down, and the wait for the receiver is bounded so a peer that stops answering cannot hold a shutdown open.

`StopReplication` also held the coordinator's lock while waiting for those goroutines to finish, and the receiver's own goroutines take that lock to apply an entry. It had no callers, so the deadlock was never reached, but it is the shape that 26.09.2 removed from the shutdown path and it is now removed here too: the lock is held only to take a reference, and the waiting happens outside it.

One related crash is fixed. The write-ahead log's replication hook calls into the sender for every appended entry, and stopping replication used to clear that reference, so any write still in flight during shutdown would have dereferenced nothing. The hook is now detached before the sender stops, the reference is left in place, and the sender itself refuses work once stopped.

### Shutdown ran its hooks in an order that depended on the configuration ([#854](https://github.com/Basekick-Labs/arc/issues/854))

Shutdown hooks carry a priority and run from lowest to highest, but the sort reordered hooks that shared a priority. Ten of them share one, so their relative order depended on how many unrelated lower-priority hooks happened to be registered, which varies with the configuration: the same binary shut down in a different order standalone than in a cluster, and the comment in the code describing the intended order was true only by accident.

The sort is now stable, so hooks of equal priority run in the order they were registered, and the same applies to components. The cluster-gated schedulers — hourly and daily compaction, continuous queries, retention and reconciliation — also move to their own priority ahead of the cluster coordinator, because each of them asks the coordinator whether it may run. They now quiesce before it stops, rather than possibly after it.

### A heartbeat from a node the cluster has forgotten is no longer discarded in silence ([#849](https://github.com/Basekick-Labs/arc/issues/849))

A node that believes it is a cluster member sends heartbeats to its peers. If a peer has no record of it, the heartbeat was dropped and acknowledged anyway, so neither side could tell: the sender saw a healthy acknowledgement, the receiver logged nothing, and the state persisted until some other operation failed. That is why a node which left the cluster and never re-joined stayed invisible until a forwarded write was rejected.

Such a heartbeat is now counted in `arc_cluster_heartbeats_unknown_node_total` and logged as a warning naming the node, at most once a minute per node and with a bound on how many nodes are tracked, since node identifiers change when a peer restarts. Alert on a non-zero growth rate: it means a node is heartbeating a cluster that has forgotten it and needs to re-join. The heartbeat is still acknowledged, because nothing on the sending side acts on a refusal today and changing that would be a protocol change.
### A node dropped from the cluster stayed in every other node's list after a snapshot restore ([#847](https://github.com/Basekick-Labs/arc/issues/847))

Each node keeps an in-memory view of cluster membership that the health checker, the heartbeat fan-out, the node listings and the file puller read. A Raft snapshot restore replaces the authoritative node table wholesale, and since 26.09.2 it announces the nodes it brings back, but it said nothing about the ones the snapshot no longer carried. A node that had been removed from the cluster therefore stayed in that view indefinitely on any node that restored from a later snapshot: it was health-checked, offered as a replication peer and listed by the API, until the process restarted.

The restore now announces those removals as well. A first restore on a fresh node announces none, because there was no previous membership to differ from. A removal naming the local node is ignored rather than applied, since a node should not evict itself from its own view; it is logged as a warning instead, which also gives a node an operator-visible signal when it is deliberately removed from a cluster.
### The first forwarded write after an idle period failed once ([#851](https://github.com/Basekick-Labs/arc/issues/851))

A node that is not the Raft leader forwards writes it cannot apply itself — manifest registration, token and RBAC changes, the startup barrier — over a connection it keeps open between commands. The leader closes that connection after thirty seconds with nothing on it, and the client found out only by writing into a dead socket, so the command failed with a broken pipe. Callers with their own retry loop recovered quietly; a single-shot caller did not, so creating a token on a follower that had been idle returned 500 and succeeded on the next attempt.

The client now stops trusting a cached connection before the leader's timeout can have closed it, and redials instead. That is the fix. There is also a narrow retry for the minority of closes that surface while writing rather than while waiting: measured on loopback, a leader's graceful close lets the write succeed about nine times in ten and fails the read instead. Only a failure to put the request on the wire is retried, because the leader cannot have applied a command it never received; a failure while waiting for the acknowledgement is never retried, since the command may have been applied and only the reply lost. The retry replays the same signed request, so a first attempt that did land is refused by the leader as a duplicate rather than applied twice. A caller that passes no deadline of its own gives the retry a fresh budget, so in that case a forwarded command can take about twice as long as before in the worst case.

### No primary writer was ever elected, so retention and continuous queries silently never ran ([#850](https://github.com/Basekick-Labs/arc/issues/850))

**Affects clusters with `cluster.failover_enabled=true` and `cluster.shared_storage_mode=false`, which is the Enterprise Helm chart's default for local-storage deployments.**

Singleton work — the retention and continuous-query schedulers, and the non-dry-run retention, CQ and delete endpoints — is gated on `IsPrimaryWriter()`. With writer failover enabled, that gate was false on every node in the cluster, forever, so retention never deleted anything, continuous queries never ran, and those endpoints answered 503 `is not primary writer`. Nothing logged an error, because each node simply believed it was not the writer. Turning failover off avoided it, since the gate then falls back to a plain role check.

Two defects combined. The writer failover manager could only fail over *from* an existing primary: its health check triggered a promotion only when it had already recorded one, and nothing else ever issued a promotion, so the first one could never happen. That is fixed by electing an initial primary when a cluster has none, mirroring what the compactor manager already did for its own lease. Second, the promotion updated the node registry, which hands out copies, while the gate reads the coordinator's own node object, so even a promotion that did happen never reached it. The promotion and the snapshot-restore path now both update that object.

Shared-storage multi-writer clusters (`cluster.shared_storage_mode=true`) were never affected: writer failover is suppressed there by design and the gate keys off Raft leadership instead.

A writer that simply restarted also used to lose the designation, because a re-join replaces the node's cluster record and the join payload carries no writer state. The cluster then had a primary it could not name, and in a single-writer deployment it never recovered one, because the only candidate was the node the failover logic had just excluded. The node table now keeps the designation across a re-join, and a healthy writer that is the only candidate can be re-elected rather than skipped.

One behaviour changes as a result. A write that arrives at a reader is proxied to a writer, and with no primary ever designated those proxied writes were spread across all healthy writers. They now go to the elected primary, which is what Pattern 1 intends, since only that node should be ingesting. Writers still serve their own traffic directly, and shared-storage clusters are unaffected.

This changes how two replicated commands are applied, so upgrade a cluster fully rather than leaving it mixed-version for long: an old binary elected as Raft leader will not elect a primary, and a new one will once it takes over.

`GET /api/v1/cluster/nodes` now reports each node's `writer_state`, which it did not before. That absence is a large part of why this went unnoticed: there was no way to ask the cluster which node held the primary role. `GET /api/v1/cluster/local` reports the same field for the node serving the request, and that is the one the scheduler gate actually reads.

Two known gaps remain and are tracked separately. Readers are still not promotion candidates ([#856](https://github.com/Basekick-Labs/arc/issues/856)), so a cluster needs more than one writer-role node to survive losing one; the Enterprise Helm chart's Pattern 1 example is corrected accordingly. And a load balancer still has no way to target the elected primary, because `/ready` does not distinguish roles ([#857](https://github.com/Basekick-Labs/arc/issues/857)).

### Cluster shutdown no longer holds the coordinator and Raft locks while it waits for its subsystems ([#813](https://github.com/Basekick-Labs/arc/issues/813))

Stopping a clustered node joined every subsystem — the file puller, the writer and compactor failover managers, the Raft node, the delete worker — while holding the coordinator's lock, and the Raft node's own `Stop` held its lock across the Raft shutdown, which waits for the goroutine that applies entries to the FSM. Anything on one of those joined goroutines that took either lock deadlocked the shutdown, and a deadlocked shutdown hook hangs the whole process until the supervisor kills it. #797 fixed one such caller, the FSM delete callback, and left the structure in place with a contract comment and a test that guards the FSM callbacks only; a puller gate check had already been working around it with a try-lock.

`Coordinator.Stop` now takes its lock only to flip state, close its channels and snapshot the subsystem pointers, joins them with the lock released, and takes it again briefly to clear the fields. The Raft node's `Stop` releases its lock around the shutdown as well and keeps the instance in place, so callers that read it during the join get the shutting-down instance's answers (not leader, `ErrRaftShutdown`) rather than a block. A second `Stop` during the join returns at once and a `Start` during it is refused as already running; the coordinator remains single-use. The failover managers are still joined before the Raft node stops, so a failover tick cannot propose into a Raft instance that is going down; before this fix that order was also what kept them off the Raft node's lock, and it is now written down. The FSM-callback contract stays in force (callbacks run under both locks during the startup snapshot restore) and is guarded by the existing test plus three new ones: two hold a callback inside the Raft join while it takes the coordinator lock and while it reads the Raft node, and one checks that a second `Stop` and a `Start` during the join behave.

### A leader restarted from a Raft snapshot rejected every forwarded write as an unknown node ([#807](https://github.com/Basekick-Labs/arc/issues/807))

Cluster nodes forward writes they cannot apply themselves to the Raft leader: manifest registration from a standby writer, the compaction bridge, token and RBAC changes, and the startup barrier. The leader authorised the forwarding node through its in-memory node registry only. That registry is filled by the join flow and by node-added entries replayed from the Raft log; a snapshot restore fills the FSM node table directly and fires no such callback. So a leader that restarted from a snapshot knew only itself, and since a follower whose peer discovery ran after Raft already knew the leader never re-joins, every forwarded write from such a follower was rejected with `unknown node` until something triggered a join. A write on the follower still returned success, because ingestion is local, but its file never reached the manifest and was invisible to the rest of the cluster. The same empty registry made the restarted leader heartbeat nobody, so followers marked it unhealthy, and made a restarted follower answer join requests with an empty leader address.

The leader now resolves the forwarding node from the Raft FSM node table, which is Raft-committed data written by the authenticated join and is restored with the snapshot; the registry is consulted only when the table has no entry, as defence in depth. The role gate is unchanged: a reader in the node table is still refused for manifest commands, and a node in neither store is still rejected. A snapshot restore now also delivers every restored node to the same callback a log replay would, so the registry, the leader's heartbeats, node listings, the file puller's origin lookup and the join redirect all see the membership again without a re-join. Restored node entries carry the writer state, so the registry and the writer-failover manager know the primary writer after a restore as well. A `null` node entry in a snapshot is refused and logged instead of crashing the node at boot, and a snapshot with no nodes leaves a writable table.

Reproduced on the enterprise-local compose cluster: after stopping every node and restarting all of them without seeds, token creation on a follower failed with HTTP 500 and a follower's flushed file never appeared in the leader's manifest; with the fix both succeed and every node lists the full membership.

### The arcx engine's three response stream writers now recover from panics ([#717](https://github.com/Basekick-Labs/arc/issues/717))

**Affects only builds made with the `arcx_engine` tag, which no release ships.**

The earlier fix for this issue (below, under "A panic while streaming a response no longer crashes the server") wrapped every streaming response writer that ships, and left the three writers in the arcx serve path (Arrow IPC, MessagePack, JSON) unwrapped because they build only under the `arcx_engine` tag, which no CI job can compile-check. They are wrapped now, with the same wrapper and the same disposition: a panic is logged, counted as a query error, and, for the MessagePack and JSON writers, fails the query's registry entry so it does not stay listed as running. The Arrow IPC writer has no entry to fail, because that endpoint registers the query only after the arcx hook declines. The CI guard that requires every body-stream writer to go through the wrapper no longer exempts that file; the change was compile-checked locally against the arcx library.
### Iceberg export registered a compacted file next to the files it replaced ([#638](https://github.com/Basekick-Labs/arc/issues/638))

**Affects `iceberg.enabled = true` deployments with compaction running.**

Compaction uploads the compacted file into the partition before it deletes the source files, which is the right order for Arc's own crash safety. A reconcile pass that listed the partition in that window registered the compacted file and every source it replaced in one snapshot, so external readers saw roughly twice the rows for that hour or day until a later pass removed the sources: up to two reconcile intervals at the default, and in steady state with compaction running across many measurements it happened regularly.

The reconciler now reads compaction's crash-recovery manifests before it stats a partition's files and leaves out any compacted output whose manifest still exists and at least one of whose source files is still present. The output is registered on the pass after the compaction has replaced its sources, in one snapshot with the sources' removal, which is the transition Arc's own file set makes. The order of the reads is what makes this safe: compaction deletes its manifest only after every source is gone, or after it has removed an output it could not keep, so a manifest that is absent means the sources vanish from the same pass, and a manifest that is present means the output is held back. A compaction that commits in the few milliseconds between the manifest read and the file stats leaves that partition out for one pass (an empty snapshot if it is the measurement's only partition), refilled on the next. The state is consulted per measurement on every pass, fresh from storage, so a compaction that starts mid-pass is seen. The lookup is wired even when compaction is disabled, since a manifest from an earlier run can still be in storage.

One under-count remains, bounded by the compaction schedule: if a compaction deletes some sources and fails on the rest, its output stays hidden while the surviving sources are exported, until the next compaction cycle's recovery finishes the job (with compaction disabled no recovery runs, and the partition stays that way until compaction is re-enabled or the manifest is removed by hand). That replaces a double count with a short, bounded under-count. A manifest that cannot be parsed is ignored and reported once, since it names nothing; recovery parks it under the `.quarantined` suffix.

### Arrow IPC queries were invisible to query management and slow-query logging ([#309](https://github.com/Basekick-Labs/arc/issues/309))

**Affects `POST /api/v1/query/arrow` on deployments with `query_management.enabled = true` or `query.slow_query_threshold_ms > 0`.**

The Arrow endpoint had caught up with the JSON endpoint on RBAC, query governance, the row-cap trailer and the cluster catch-up gate, but not on the query registry or slow-query logging. An Arrow query never appeared in `GET /api/v1/queries/active` or `/history`, could not be cancelled through `DELETE /api/v1/queries/:id`, carried no `X-Arc-Query-ID` header, and never counted as slow no matter how long it ran.

The Arrow endpoint now registers every query, returns `X-Arc-Query-ID`, derives its execution context from the registry so a cancel reaches DuckDB, and records the same dispositions as the JSON endpoint: completed with the row count, failed with the sanitized cause, timed out, or cancelled, with the governance row cap noted in history. Because DuckDB materializes an Arrow result before streaming, a cancel that lands during execution ends the request with `500 Query cancelled`; only a cancel during the short streaming phase ends it as a truncated Arrow stream with the truncation trailer. `query.slow_query_threshold_ms` now covers Arrow queries and moves `arc_slow_queries_total`, and a deadline that fires mid-stream now counts in `arc_query_timeouts_total`, which only the pre-stream timeout did before.

Giving the Arrow endpoint the same dispositions exposed an ordering bug on the JSON endpoint: its error branches released the timeout context before reading the cause, which turned every plain execution failure into `context canceled` and filed it as "already cancelled", leaving the registry entry listed as running forever. Both JSON branches now read the cause first.

Two things did not change: a client that hangs up during execution does not stop DuckDB on either endpoint (the registry cancel and the timeout are the levers), and an Arrow query against a measurement with no files still returns `500` and lands in history as failed, where the JSON endpoint returns an empty result.

### MQTT ingest accepted database and measurement names that are not valid storage segments ([#300](https://github.com/Basekick-Labs/arc/issues/300))

**Affects `mqtt.enabled = true` deployments.**

A subscription's `database`, every `topic_mapping` target, and each message's measurement become segments of a storage key, and the MQTT path is the one ingest surface with no HTTP handler in front of it to check them. 26.09.1 accepted any string for all three. The storage-key rule added for [#741](https://github.com/Basekick-Labs/arc/issues/741) in this release covered subscription updates but not creates, because the create-request validator built its temporary subscription without the topic mapping, and nothing covered the measurement, which the publisher controls. What happened depended on the shape: a target or measurement containing a slash was written under a mis-partitioned path no query can address; one containing `..`, or empty, was accepted into the buffer and refused at flush by the storage key contract, which latches the buffer's flush-failure state (WAL retained, non-clean shutdown) until the subscription or publisher is fixed. The storage layer's containment check held throughout; this was a data-integrity gap, not a traversal.

Create requests now validate mapping targets with the same rule as `database`, and a subscription whose persisted targets fail the rule (a row edited in SQLite, or one written before the rule) no longer starts: it is left in the `error` status with the reason on the row. The subscriber also re-checks the resolved database on every message and drops one bound for an invalid name before decoding it, counted in `messages_failed` and the MQTT failed-messages metric, with one error log per offending value per subscriber run. A measurement that fails the HTTP write path's rule (letter first, then letters, digits, underscore or hyphen, at most 128 characters) is refused as a decode error and never reaches the buffer.

### Backups skipped an Iceberg warehouse outside the storage root, and a restore then wedged every reconcile pass ([#637](https://github.com/Basekick-Labs/arc/issues/637))

**Affects `iceberg.enabled = true` deployments whose `iceberg.warehouse` points outside `storage.local_path`.**

Backup found Iceberg table metadata only by filtering the data-storage listing, which cannot see a warehouse that lives elsewhere. The backup completed and reported success while carrying catalog rows whose metadata locations pointed at files it never copied. Restored on a fresh host, the reconciler loaded each catalog row, failed to open the missing metadata, fell through to creating the table, wrote a fresh metadata file into the warehouse and then hit the catalog's primary key. That repeated on every pass, forever, and left another orphan metadata file behind each time.

Backup now walks an outside-root warehouse itself and stores its table metadata under `<backup_id>/iceberg/`, recorded in the manifest as `iceberg_warehouse` (source path, file count, bytes). The walk copies only Arc's own layout, `<namespace_prefix>_<db>.db/<table>/metadata/`, so a warehouse that happens to contain the storage root or the backup directory does not sweep other files in, and it walks the symlink-resolved directory so a symlinked warehouse is not silently backed up as empty. Restore writes those files back into this node's configured `iceberg.warehouse` whenever data or metadata is restored. The manifest also records the source's configured spelling (`configured_path`), and the restore warns when that spelling, evaluated on the target, does not land in the directory being written: the catalog stores absolute paths, so the target's `iceberg.warehouse` has to be the backup's path (a symlink from that path works). A node running Iceberg with no such warehouse fails the restore when it would otherwise stage a catalog that cannot load; a node with Iceberg off skips the files, counts them as `iceberg_warehouse_files_skipped` on the status endpoint, and completes.

The SQLite snapshot is now taken before the Iceberg metadata is copied, so a reconcile commit that lands during a long backup no longer leaves a catalog row pointing at a file the backup does not hold; the remaining window is `iceberg.retain_snapshots + 1` commits on one table between the snapshot and the copy, which iceberg-go's delete-after-commit needs before it removes the snapshotted version. Restart promptly after a restore that includes Iceberg: the catalog is applied on the next start, and a reconciler still running against the old catalog can expire metadata the restore just wrote.

The reconciler no longer falls through to table creation when a catalog row exists but cannot be loaded. A missing metadata file now fails that measurement's reconcile with an error that names the table, the cause, and the two ways out: restore the warehouse files, or delete the row from the `iceberg_tables` SQLite table so the table is recreated. Nothing is written to the warehouse in that state.

### Iceberg export kept serving deleted rows after a partial row-level delete ([#633](https://github.com/Basekick-Labs/arc/issues/633))

**Affects `iceberg.enabled = true` deployments that use `POST /api/v1/delete`.**

A delete whose WHERE clause matches only some rows of a Parquet file rewrites that file in place: same path, fewer rows, smaller size. The Iceberg reconciler detected changes by file path alone, so the rewritten file was never re-registered and the Iceberg manifest kept the original `record_count` and `file_size_in_bytes` for good. Engines that answer `COUNT(*)` from manifest statistics kept counting the deleted rows, and engines that trust `file_size_in_bytes` for split planning could fail on the shorter file. A full-file match, which removes the file outright, was already handled.

The reconciler now compares path **and size** against the manifest. A rewritten file is dropped and re-registered in one commit, so readers of the current snapshot never see it absent (only time travel to the intermediate snapshot does, until it expires), and the manifest picks up the new row count, size and column bounds within one reconcile interval. Files that already changed before this release are re-registered on the first pass after upgrade, with no operator action.

Re-registering a path needs Iceberg manifest merging: iceberg-go carries every removed file's `DELETED` manifest entry forever and refuses to add such a path again, whether it was rewritten in place or restored from a backup. For the commit that re-registers such a path Arc enables `commit.manifest-merge.enabled` (with `commit.manifest.min-count-to-merge=2` and a 1 GiB `commit.manifest.target-size-bytes`), which rewrites the table's manifests into one and drops the stale entries, then switches it back off in the same transaction (the two auxiliary keys stay on the table and are inert while merging is off). Ordinary passes are unchanged: one overwrite snapshot per pass, no merge. A pass that re-registers files leaves one merged manifest the size of the table's live file list in the metadata directory, and every later removal pass rewrites that manifest minus the removed entries until they age out; like every superseded manifest, none of it is reclaimed yet ([#835](https://github.com/Basekick-Labs/arc/issues/835)). The reconciler also stats every listed file on every pass now, since the size is part of the change key; on local storage that is tens of milliseconds per hundred thousand files.

Known limit: a rewrite that produces a file of exactly the same byte size at the same path is still invisible to the reconciler.

### JSON query endpoints return DECIMAL values as numbers ([#818](https://github.com/Basekick-Labs/arc/issues/818))

`POST /api/v1/query` now keeps DuckDB DECIMAL results numeric instead of changing their wire type depending on which JSON query path served the request. The Arrow-backed JSON writer normalizes decimal batches with the same schema/cast path already used by Arrow IPC and msgpack, so common aggregate results such as `SUM(integer)` and `AVG(...)` no longer fall through to quoted strings. The database/sql fallback now recognizes `duckdb.Decimal` directly instead of JSON-marshalling the driver struct into an object cell.

This restores endpoint parity for dashboard clients that infer numeric columns from JSON values; null, row-cap, truncation, and timestamp behavior is unchanged.

Decimal columns are cast the same way as on the Arrow and msgpack endpoints: scale-zero decimals become 64-bit
integers and scaled decimals become doubles. A `DECIMAL(38,0)` result outside the int64 range therefore ends the
response with `truncated: true` and a `decimal cast failed` reason, as it already does on msgpack, instead of a
quoted string.

Contributed by [@TayfurYldz](https://github.com/TayfurYldz) in [#831](https://github.com/Basekick-Labs/arc/pull/831).

### Manifest applies respect caller cancellation and deadlines ([#394](https://github.com/Basekick-Labs/arc/issues/394))

Manifest register/delete operations and the current compaction batch apply path
now preserve caller deadline budgets. The caller deadline bounds the
pre-apply cancellation check, leader-side enqueue timeout, follower dial, and
follower send/receive. The Raft `future.Error()` commit wait retains existing
Raft semantics.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#787](https://github.com/Basekick-Labs/arc/pull/787).

### Compaction cleanup removes only the owning job's temp directory ([#749](https://github.com/Basekick-Labs/arc/issues/749))

Parent-side compaction cleanup now removes only the exact JobID-owned temp directory, so it can no longer sweep another concurrent job whose names collapse to the same underscore prefix. If parent cleanup itself fails, the leftover is retained for `CleanupOrphanedTempDirs` to remove on the next startup instead of being hidden by a broad prefix sweep.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#786](https://github.com/Basekick-Labs/arc/pull/786).

### Iceberg export fails when storage hides data files ([#760](https://github.com/Basekick-Labs/arc/issues/760))

Iceberg export now checks the storage backend's unusable-object enumeration and
fails a measurement when a Parquet data file is hidden from normal listings. The
reconciler logs an `Error` on every reconcile pass until the operator renames the
named files, and the table stays at the last published snapshot instead of
silently dropping rows that Arc's query path can still read.

Renaming the named files unblocks export, and the next reconcile pass publishes
that table again. The local Iceberg path performs a second directory walk per
measurement per pass to find these hidden files, which is the safe fallback; a
single combined walk is the future optimization if reconcile time becomes an
issue.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#785](https://github.com/Basekick-Labs/arc/pull/785).

### Iceberg export on an edge-sync hub produced one garbage table per spoke ([#634](https://github.com/Basekick-Labs/arc/issues/634))

**Affects hubs only** — a node receiving edge-sync data with `iceberg.enabled = true`.

A hub stores received data one level deeper than local data: `{spoke_id}/{db}/{measurement}/{y}/{m}/{d}/{h}/*.parquet`. The Iceberg walk read the top two directory levels as `(database, measurement)`, so a spoke `rocket-01` holding database `factory` was discovered as **one** measurement named `factory` in a database named `rocket-01`.

Every Parquet file from **every** measurement under that spoke then landed in a single file list, and their schemas were unioned. Either the union failed on a cross-measurement type collision — logging an error every pass, forever — or it succeeded and minted a catalog table `arc_rocket-01.factory` mixing all measurements into one franken-schema. Hub compaction then churned those file sets, snapshotting the garbage every cycle.

Compaction learned to expand spoke namespaces in 26.09.1 ([#619](https://github.com/Basekick-Labs/arc/issues/619)); the Iceberg source never got the same treatment. It does now: spoke namespaces expand into `{spoke}/{db}` pseudo-databases with their real measurements, so received data exports as the tables it actually is.

The separator is mapped to `.` in the Iceberg namespace (`arc_rocket-01.factory`), because the SQL catalog names namespace directories `<namespace>.db` and an unsanitized slash would nest that directory one level deeper than the warehouse walk expects — feeding the exporter's own metadata back in as a user database. A real Arc database name cannot contain a `.`, so there is no collision with a genuine database.

If the spoke lookup fails, spoke namespaces are **skipped** for that pass rather than exported un-expanded: exporting them wrong mints catalog tables that then have to be cleaned up by hand, so skipping is the cheaper failure.

<Callout type="warn" title="Existing hubs may already have garbage tables">
If you ran `iceberg.enabled` on a hub before this release, the catalog may contain a table per spoke namespace (`arc_<spoke>.<db>`) whose schema is a union of unrelated measurements. Those tables are not repaired automatically — drop them, and the next reconcile pass will create the correct per-measurement tables.
</Callout>


### Iceberg export reported successful snapshot expiry as failure, and published dangling metadata ([#632](https://github.com/Basekick-Labs/arc/issues/632))

**Anyone running `iceberg.enabled = true` should upgrade.** Once a table's history exceeded `iceberg.retain_snapshots`, every reconcile pass logged

```
Iceberg ExpireSnapshots commit failed (non-fatal) — snapshot history grows until it recovers
```

from a commit that had actually **succeeded**, and published a `version-hint.text` pointing at metadata that still listed snapshots whose manifest-list files had just been deleted. A directory reader doing snapshot listing or time travel then failed on the missing files, and a measurement that went quiet kept that partially-dangling hint indefinitely.

iceberg-go runs orphan deletion for an expiry as a **post-commit hook**: the catalog commit lands, then it removes the expiring snapshots' manifest lists, manifests, and any data files they referenced, joining every failure into the error `Commit` returns. In Arc, files leave a table precisely *because Arc already deleted them* — compaction, retention, the delete API — so the hook reported `ENOENT` for files that were supposed to be gone, and the error list grew every pass as each expiring manifest carried entries for every file ever removed.

Arc now passes `WithPostCommit(false)`, so the exporter expires snapshots in the catalog and leaves file deletion to Arc.

<Callout type="warn" title="The exporter no longer holds delete authority over your data files">
This is the more important half of the fix. With the previous default, iceberg-go could physically `os.Remove` Arc's **primary Parquet data files** during expiry. Any future listing bug that transiently omitted live files for `retain` generations would have turned into silent deletion of customer data by the export subsystem.

Arc owns the data-file lifecycle. The exporter must never delete data files, and now cannot.
</Callout>

The residue is expired metadata files no longer referenced by any snapshot. `pruneOldVersionFiles` already bounds the `v<N>.metadata.json` copies; the rest is small and bounded by `retain`.

The existing expiry test could not catch this because it left every Parquet file on disk, so the post-commit hook always succeeded. The regression test added here deletes each superseded file **before** the pass that expires the snapshot referencing it — the production order — and asserts both symptoms: no false "commit failed" log, and no published snapshot whose manifest list is missing from disk.


### Removed: `arc_replication_sequence_gaps_total` ([#810](https://github.com/Basekick-Labs/arc/issues/810))

**This metric has been removed.** If you scrape it, drop it from your dashboards and alerts — it has read `0` on every Arc since it was introduced.

It was announced in 26.03.1 alongside a claim that "receivers now log a warning with gap details when non-consecutive sequences are received." That detection was never implemented: nothing in the replication receiver has ever checked for a gap, and nothing ever incremented the counter.

It is being removed rather than implemented, because a silent gap **cannot occur** on a replication connection. The receiver requires each checkpoint's `LastSequence` to equal exactly what it has applied, and both ends carry a cumulative SHA-256 over every payload since the handshake — a skipped entry diverges the hashes. Either check drops the connection, and checkpoints are emitted every 1024 entries, so the blast radius is bounded. Failing closed at the receive path is strictly stronger than a counter scraped after the fact.

An exported counter that can never change value is worse than no counter: an alert built on it is permanently green and asserts a guarantee the code never checked. Removing it is the honest option.

The signal operators actually need for replication health is **lag**, not gaps — the delta between what the writer has produced and what the receiver has applied. That is tracked separately.
### DuckDB connection-pool metrics are populated ([#809](https://github.com/Basekick-Labs/arc/issues/809))

`arc_db_connections_open` and `arc_db_connections_in_use` were exported and never set, so they read `0` forever and the `pool` block of `GET /api/v1/metrics/query-pool` reported zeros. Any "is DuckDB saturated?" panel built on `connections_in_use / connections_open` was dividing zero by zero.

They are now sampled from `sql.DBStats` when metrics are read, along with three that were not exported at all:

```
arc_db_connections_max        # pool limit (database.max_connections)
arc_db_connections_open       # in use + idle
arc_db_connections_in_use
arc_db_connections_idle
arc_db_wait_count_total       # cumulative waits for a connection
arc_db_wait_seconds_total     # cumulative time blocked waiting
```

**Alert on waits, not on the in-use ratio.** The saturation signal is `arc_db_wait_count_total`: a pool sitting at its limit with zero waits is simply busy, while sustained wait growth means queries are actually blocking on a connection.

```
rate(arc_db_wait_count_total[5m]) > 0
```

Sampling happens at read time rather than on a background ticker — `sql.DBStats` is a point-in-time snapshot that is only meaningful when observed — and all three metrics endpoints refresh it, so the JSON and Prometheus surfaces agree.

**`arc_db_queries_total` and `arc_db_query_errors_total` have been removed** rather than wired. Counting them at the `DuckDB.Query`/`Exec` wrappers would have missed the hot path: the query handler runs through `query.ParallelExecutor`, which holds the raw `*sql.DB` and never passes through those wrappers. A counter named "total" that silently omits most queries is the same failure mode as the Arrow under-counting fixed above, so it is better absent than partial. Use `arc_query_requests_total` and `arc_query_errors_total`, which are counted at every API entry point.
### Removed: `arc_decomp_buffer_discards_total` ([#817](https://github.com/Basekick-Labs/arc/issues/817))

**This metric has been removed.** If you scrape it, drop it — it has always read `0`.

It counted "oversized decompression buffers not returned to the pool", a standard guard for a `sync.Pool` of variable-size buffers. That guard was never reached: the per-handler pooled codecs it belonged to were replaced by the package-level `decompressGzipPooled` / `decompressZstdPooled`, and the buffer pool they left behind is never taken from at runtime. With nothing drawing a buffer, nothing could discard one.

Also removed alongside it: a second, package-local discard counter with an exported `GetDecompBufferDiscards()` accessor, and the unused `maxPooledBufferSize` threshold the guard would have used.

The `sync.Pool` and `PooledBuffer` themselves stay — they are documented in the code as deliberately retained for a future pooled path, and their `Release()` idempotency is still covered by tests. Only the metrics go, because an exported counter that can never move reports health it never checked.


### Shutdown no longer deadlocks when a manifest delete is applied while the coordinator stops ([#797](https://github.com/Basekick-Labs/arc/issues/797))

`Coordinator.Stop` holds the coordinator lock for its whole body, including stopping the Raft node,
and Raft's shutdown waits for its apply goroutine to finish. The FSM delete callback that unlinks a
replicated file locally ran on that goroutine and took the same lock to read the storage backend, so a
`DeleteFile` or batch delete (retention, compaction, the reconciliation sweep) applied while the node
was stopping blocked the apply goroutine on the lock, the Raft shutdown waited on it, and `Stop` never
returned: the process hung until its supervisor killed it. Any node with `replication_enabled` (writers
and compactors run the puller too) could hit it, most likely during a retention window; on nodes with
writer or compactor failover enabled the hang showed up earlier, inside the failover manager's stop,
because its Raft futures wait on the same blocked goroutine.

The callback now uses the storage backend and delete queue captured when it was registered (both are
set once, before the callbacks exist) and takes no coordinator lock. FSM callbacks run on the Raft apply
goroutine that shutdown waits on and must take neither the coordinator lock nor the Raft node's; that
contract is now documented where they are registered, and a test holds the coordinator lock while
applying every command type wired in the cluster package to enforce it. `Stop` also unregisters the
file callbacks once Raft is joined, so an in-process restart cannot replay a delete into a queue the
previous run had closed.

### `server.shutdown_timeout` was parsed and then ignored ([#805](https://github.com/Basekick-Labs/arc/issues/805))

The key was documented, defaulted, read into config, and plumbed into the HTTP server config — and then never used. Three separate shutdown budgets hardcoded 30 seconds instead, so an operator who raised the value to give a slow object store more room to flush got no effect at all.

That budget is load-bearing for durability: when it expires, the coordinator skips the remaining shutdown steps, and a buffer flush that has not finished is abandoned. Raising `terminationGracePeriodSeconds` in Kubernetes without a matching Arc setting simply gave the pod longer to sit idle after Arc had already given up at 30 seconds.

The coordinator budget and the HTTP drain now both derive from `server.shutdown_timeout`. A non-positive value is rejected with a warning and falls back to the 30-second default, rather than cancelling the shutdown context immediately and skipping every step.

The unused `ShutdownTimeout` field has been removed from `api.ServerConfig`. `Server.Shutdown` takes its budget as a parameter; the field was assigned by callers and read by nothing, which is what made the key look wired when it was not.


### The Arrow query endpoint counted its failures but not its requests ([#801](https://github.com/Basekick-Labs/arc/issues/801))

`POST /api/v1/query/arrow` incremented `arc_query_errors_total` on failure but never `arc_query_requests_total` or `arc_query_success_total`. Three consequences, all of which land on the first dashboard anyone builds:

- **The obvious error-rate expression was unbounded.** `rate(arc_query_errors_total[5m]) / rate(arc_query_requests_total[5m])` counted Arrow failures in the numerator with no Arrow traffic in the denominator, so on an Arrow-heavy deployment the ratio could exceed 1 — or divide by zero if no JSON queries ever ran.
- **Arrow throughput was not observable at all.** No counter moved on a successful Arrow query.
- `arc_query_success_total + arc_query_errors_total` did not equal `arc_query_requests_total`, so none of the three was safe as a denominator.

Arrow is the path performance-sensitive clients are steered to, so this was under-counting the majority of query traffic in exactly the deployments most likely to be monitored closely.

The endpoint now counts requests at entry, and success, rows and latency on completion, matching the JSON path. Verified on a running binary: one successful and one failing Arrow query move the counters to `requests 2, success 1, errors 1`, where the same sequence previously produced `requests 0, success 0, errors 1`.

All four query entry points (`/api/v1/query`, `/api/v1/query/msgpack`, `/api/v1/query/:measurement`, `/api/v1/query/arrow`) now count requests consistently.
### Deployment artifacts: wrong storage variable, no-op autoscaling values, and no WAL ([#804](https://github.com/Basekick-Labs/arc/issues/804))

Three defects in the shipped Kubernetes manifests and the OSS Helm chart. Each is small on its own; together they meant a user following our own deployment files could run without a write-ahead log, or write data outside the persistent volume.

**`ARC_STORAGE_BASE_PATH` was not a real setting.** `deploy/kubernetes-local/statefulset.yaml` set it on both the writer and the reader, but no such config key exists — the correct name is `ARC_STORAGE_LOCAL_PATH` (`storage.local_path`). The variable was silently ignored and Arc used its default `./data/arc`, relative to the container working directory, so whether data landed on the mounted PVC was incidental. Fixed in both StatefulSets.

**`deploy/kubernetes/` set no storage path and no WAL at all**, relying on binary defaults for both. Both are now explicit and point under the volume mount.

**The OSS Helm chart now enables the WAL.** `wal.enabled` defaults to `false` in the binary for backwards compatibility, and the chart did not override it — so `helm install arc` produced a deployment with no write-ahead log, where a crash loses every record buffered since the last flush. Every shipped Docker Compose file already set `ARC_WAL_ENABLED=true`; the chart omitting it was an oversight. It is now on by default and configurable:

```yaml
arc:
  wal:
    enabled: true
    directory: /app/data/wal
```

**The OSS chart's `autoscaling` values have been removed.** They were never backed by a `HorizontalPodAutoscaler` template: setting `autoscaling.enabled=true` only dropped `replicas` from the Deployment, leaving a single replica with nothing managing it. Horizontal scaling of a single OSS Arc is not viable in any case — the default `Recreate` strategy over a ReadWriteOnce PVC prevents replicas from sharing the volume. Scaling out requires shared object storage with Arc Enterprise clustering, where readers are separate StatefulSets. Existing values files that set `autoscaling.*` keep rendering; the keys are simply ignored, as they were in practice before.

### Readers no longer walk a half-replayed manifest at startup ([#799](https://github.com/Basekick-Labs/arc/issues/799))

Before walking the cluster manifest for its startup catch-up, a node waited on a Raft barrier so the
walk would see every committed entry. That barrier is leader-only: on a follower it returned at once,
the node logged "proceeding against possibly-stale manifest", and on every reader restart with
unapplied log behind it the walk ran before the replay. Entries that landed a moment later were
pulled outside the gated batch, so with `cluster.query_gate_on_catchup` enabled a reader could answer
queries while still missing files the manifest listed.

A follower now forwards a no-op barrier entry through the leader, using the same authenticated path
as every forwarded manifest write, and waits until its own FSM has applied it. Raft applies the log in
order, so once the barrier is visible locally the whole backlog before it is too, and only then does
the walk start; the query gate stays closed meanwhile. The leader keeps using Raft's own barrier. The
barrier map is part of FSM snapshots, so a follower that catches up by snapshot install resolves the
wait immediately. The leader's coordinator address is now also resolved from the FSM node table when a
freshly restarted follower's in-memory registry does not have it yet, which also removes a latent
post-restart failure for every other forwarded write.

The default `cluster.replication_catchup_barrier_timeout_ms` moves from 10000 to 30000: after an outage
longer than ~10 s the leader's replication to the returning follower backs off for up to 10.24 s, and
the old default expired at that edge. On timeout the node proceeds as before and logs its applied,
commit and last log index.
### Buffer, audit and MQTT metrics that were exported but never populated ([#802](https://github.com/Basekick-Labs/arc/issues/802))

Several metrics were exported at `/metrics` with HELP and TYPE strings and then never incremented, so they scraped as a permanent `0`. That is worse than an absent metric: a panel built on one looks healthy rather than broken. These are now wired.

**Ingest backpressure is observable for the first time.** `arc_buffer_records_buffered`, `arc_buffer_flushes_total`, `arc_buffer_records_written_total` and `arc_buffer_queue_depth` now carry real values. `arc_buffer_records_buffered` is the one to watch: records accepted but not yet written to storage. It is published by a one-second sampler rather than on flush completion, because a flush is what empties the buffer — publishing only there would report `0` in exactly the backlog case an operator needs to see.

**Audit events can no longer be lost silently.** `internal/audit` did not reference the metrics package at all, while `LogEvent` drops events when its queue is full. For a compliance feature, undetectable loss is the worst failure mode. Three paths are now instrumented:

- **New metric `arc_audit_events_dropped_total`** counts events discarded before they were ever queued. These never reach the writer, so they cannot be counted as write errors — they needed their own counter.
- `arc_audit_write_errors_total` now counts events that failed to persist, **by batch size**: a failed transaction loses every event in the batch, so counting a single error would understate the loss.
- `arc_audit_events_total` counts events that actually committed.

If you run audit logging for compliance, alert on `arc_audit_events_dropped_total > 0`. The audit queue is currently a fixed 1000 events with no configuration key, so a sustained non-zero value means audit events are being lost faster than they can be written and needs investigation rather than tuning.

**`arc_mqtt_decode_errors_total`** now increments when a payload parses as neither MessagePack nor JSON. Previously every decode failure was folded into `arc_mqtt_messages_failed_total`, which also covers write failures, so a publisher sending malformed payloads was indistinguishable from a storage problem.

One group remains unwired after this release and still reads `0`: the DuckDB pool gauges (`arc_db_connections_open`, `arc_db_connections_in_use`, `arc_db_queries_total`, and the `pool` block of `GET /api/v1/metrics/query-pool`), tracked in [#809](https://github.com/Basekick-Labs/arc/issues/809).
The remaining unwired metrics are addressed separately in this release: the DuckDB pool gauges are now populated ([#809](https://github.com/Basekick-Labs/arc/issues/809)) and `arc_replication_sequence_gaps_total` has been removed ([#810](https://github.com/Basekick-Labs/arc/issues/810)) — both below.

### A graceful shutdown could delete the WAL that still held unflushed data ([#803](https://github.com/Basekick-Labs/arc/issues/803))

**Any deployment running with `wal.enabled = true` should upgrade.** On shutdown, Arc
flushes its in-memory buffers and then purges the WAL, on the premise that everything
is now durable in Parquet. Two defects broke that premise, and together they could lose
data on a *clean* `SIGTERM` that a hard `SIGKILL` would have preserved.

First, the purge step ran in the wrong order. It was registered as a shutdown *hook*,
but the coordinator runs every hook before any component, and the buffer flush is a
*component* — so the WAL was deleted before the flush it was supposed to follow. The
in-code comment claiming it ran "after ArrowBuffer flushes" described an ordering that
never happened.

Second, `ArrowBuffer.Close()` logged per-buffer flush failures and then returned
success unconditionally, so nothing downstream could tell that records had not reached
storage.

The window is small but lands at the worst moment: a rolling restart or node drain
during an object-store outage, expired storage credentials, or a flush timeout. Every
Kubernetes rollout, eviction and drain issues `SIGTERM`, so this is reachable during
routine operations, not only during incidents.

The purge now runs after the flush, and only when the flush actually succeeded. It
accounts for all three ways a record can fail to land: a synchronous flush error, a
record dropped to WAL-replay (rejected at enqueue or abandoned in the flush queue when
the workers are cancelled), and an earlier asynchronous flush failure. When any of
those occurred, Arc keeps the WAL and logs:

```
Retaining WAL files: buffer flush did not complete cleanly on shutdown.
Unflushed records remain in the WAL and will be replayed on next startup.
```

The retained WAL replays on the next start and is reclaimed by the periodic WAL
maintenance sweep once the backend recovers, so this does not grow without bound. A
healthy shutdown still purges exactly as before.

**Behaviour change — new non-zero exit code.** A shutdown that could not flush every
buffer now exits **1** instead of 0, because `Close()` reports the failure it
previously swallowed. Container runtimes will show the container as errored rather than
cleanly stopped. This is intentional: a shutdown that could not persist its data should
not report success. Both Helm charts leave `restartPolicy` at the Kubernetes default of
`Always`, so pod restart behaviour is unchanged; operators alerting on container exit
codes should expect this signal to accompany the `Retaining WAL files` log line.

### Removing a manifest entry reopens the replication query gate without a restart ([#759](https://github.com/Basekick-Labs/arc/issues/759), [#795](https://github.com/Basekick-Labs/arc/issues/795))

With `cluster.query_gate_on_catchup` enabled, a reader whose startup catch-up could not pull a
manifest entry (a file no peer holds, or a key no backend can address) answered 503 to every
query until the process was restarted. The catch-up failure could only be cleared by a later
successful pull of the same path, and for those entries there never is one. Removing the entry
from the cluster manifest, which is what retention, compaction and the reconciliation sweep do
and is the remedy for a file no node holds, changed nothing on the reader.

The FSM delete callback now tells the puller the entry is gone. A recorded catch-up failure or
drop for that path is cleared and the gate reopens immediately. A pull still queued or in flight
for it is dropped from the catch-up batch (the tag is removed in the same critical section, and
the worker checks manifest membership before every attempt and before counting a failure), so
the delete cannot race the pull into a permanent red gate: this covers a delete that lands
mid-pull and, because the catch-up walker can wait minutes inside a page at queue high water,
one that lands before the walker has even tagged the entry. Queue-full drops use the same
membership check, and a follower that restores from a Raft snapshot (which fires no delete
callbacks) has any stale failures pruned within one periodic reconciliation interval
(`cluster.replication_reconciliation_interval_seconds`, default 5 minutes). Retention,
compaction or a reconciliation sweep deleting entries while a reader is still catching up no
longer strands that reader either. The catch-up status in the 503 body and on `/api/v1/cluster`
gains `skipped_gone`, the count of pulls abandoned because their entry left the manifest.

What removes an entry: for a file no node holds, the Phase 5 reconciliation sweep on the origin
writer (`reconciliation.enabled`, or `POST /api/v1/reconciliation/trigger?dry_run=false&act=true`).
A key no backend can address has no automated remover yet: retention cannot read it and the sweep
only reports it, so that class still needs the operator delete endpoint tracked in
[#794](https://github.com/Basekick-Labs/arc/issues/794); once it lands, the same self-heal applies.

Groundwork by [@Thundercloud12](https://github.com/Thundercloud12) in [#790](https://github.com/Basekick-Labs/arc/pull/790); the mid-pull ordering was found and reproduced live while reviewing it.

### MQTT startSubscriber validates reservation before installing live subscriber ([#770](https://github.com/Basekick-Labs/arc/issues/770))

`startSubscriber` now performs a compare-and-swap check under the manager lock after
connecting, ensuring the reservation placeholder is still present and unmodified before
installing the running subscriber. If a concurrent `Delete` or `Shutdown` cleared the
reservation while the subscriber was connecting, the live subscriber is immediately stopped
to prevent leaking broker connections and ingest writers. In addition, boot-time auto-start
now uniformly reserves the slot placeholder and cleans up on failure.

Contributed by [@Thundercloud12](https://github.com/Thundercloud12) in [#788](https://github.com/Basekick-Labs/arc/pull/788).

### Edge-sync rejects source paths that exceed the hub storage budget ([#757](https://github.com/Basekick-Labs/arc/issues/757))

The hub now accounts for spoke namespace and staging prefixes before accepting an edge-sync source path, returning a client error instead of retryable 503 responses for paths it cannot store.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#783](https://github.com/Basekick-Labs/arc/pull/783).

### Backup no longer skips temp-file write failures ([#779](https://github.com/Basekick-Labs/arc/issues/779))

Backup now treats failures writing its temporary file as fatal instead of
misclassifying them as unreadable source files and counting them as skipped.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#784](https://github.com/Basekick-Labs/arc/pull/784).

### Remaining DuckDB string-literal escaping uses the shared helper ([#781](https://github.com/Basekick-Labs/arc/issues/781))

The remaining compaction and database DuckDB string-literal escaping now uses
the shared `sqlutil` helper. Compaction no longer incorrectly doubles ordinary
backslashes in standard DuckDB string literals.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#782](https://github.com/Basekick-Labs/arc/pull/782).

### The compaction candidates preview lists each measurement once ([#316](https://github.com/Basekick-Labs/arc/issues/316))

`GET /api/v1/compaction/candidates` asked every enabled compaction tier for
its candidates, and each tier listed the same `database/measurement/` prefix
in the object store itself, so with hourly and daily enabled every measurement
was listed twice per call. On a bucket with many files the listing is the
expensive part of the request. The tiers can now scan a listing the caller
already holds, and the preview lists each measurement once and hands the
result to every tier. The scheduled compaction cycle is unchanged: it keeps
one listing per tier on purpose, because hourly deletes its inputs before
daily runs.

Contributed by [@alexeymoskalev-devops](https://github.com/alexeymoskalev-devops) in [#789](https://github.com/Basekick-Labs/arc/pull/789).

### Metadata cache invalidation is ordered with cache fills ([#344](https://github.com/Basekick-Labs/arc/issues/344))

Removed the redundant `MetadataStore` mutex that duplicated the serialization
provided by the SQLite connection pool. Tier-cache fills are now ordered against
invalidation with a generation counter, preventing stale query results from
being stored after an invalidation.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#777](https://github.com/Basekick-Labs/arc/pull/777).

### Edge-sync spoke IDs no longer create manifest-invalid keys ([#751](https://github.com/Basekick-Labs/arc/issues/751))

Spoke IDs containing a colon, such as `rocket:01`, are now rejected during
registration and input validation because the resulting storage key would be
refused by the cluster manifest path validator.

A spoke registered with a colon before this version is refused after upgrade:
the hub rejects its syncs and the spoke will not start. Re-register it under a
new ID; files already written under the old namespace stay where they are.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#776](https://github.com/Basekick-Labs/arc/pull/776).
### Retention reports files hidden by storage listings ([#771](https://github.com/Basekick-Labs/arc/issues/771))

Retention already had a skipped-file counter, but since #744 normal listing no
longer returns a key that `readParquetPath` rejects, the counter stopped being
fed and remained zero. Retention now reports those files, including keys
rejected by the storage contract; they remain untouched, while dry-run and real
executions report the same skipped-file count and reason.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#775](https://github.com/Basekick-Labs/arc/pull/775).

### DuckDB path quoting uses one shared escaping helper ([#752](https://github.com/Basekick-Labs/arc/issues/752))

DuckDB path interpolation now uses one shared string-literal quoting helper
across query, delete, retention, and parallel execution paths, preserving the
existing escaping behavior for paths containing single quotes.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#780](https://github.com/Basekick-Labs/arc/pull/780).

### Numeric MessagePack host values are logged when coerced ([#768](https://github.com/Basekick-Labs/arc/issues/768))

The MessagePack decoder accepts a numeric `host` and coerces it to `host_<num>`.
That behaviour is unchanged, but the coercion now emits a debug-level log line
naming the resulting host, so a misconfigured client can be spotted without
changing ingest behaviour. The log reuses the string already built for the
return value, so the cost with debug disabled is not measurable under parallel
ingest.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#769](https://github.com/Basekick-Labs/arc/pull/769).

### A restore no longer reports success while dropping files ([#762](https://github.com/Basekick-Labs/arc/issues/762))

A restore could finish with `status: completed` while having written only part
of the backup. Every per-file failure in the data copy was logged and skipped:
a backup object that could not be read, a temp file that could not be created,
and a write into the live data store that failed all looked the same and none
of them was counted. A backup whose objects had gone missing after it was
written listed short, so every listed file restored and the restore also
reported success. Backup had the accounting for this (the skipped-file count,
the "will be incomplete" warning, the guard against too much loss); restore,
the side where the gap costs data, had none of it.

What changes:

- **A restore that could not restore every data file now ends `failed`**, with
  an error saying so and, on the status endpoint, `skipped_files` (backup
  objects that could not be read), `missing_files` (files the manifest
  inventoried that backup storage no longer holds), and `skipped_sample` (up to
  32 of the unreadable paths). The files that could be restored are written
  first and stay in place, so a recovery from a damaged backup still gets
  everything that can be read. There is no tolerated fraction: backup tolerates
  a few unreadable files because compaction or retention can remove one between
  listing and copy, and nothing removes objects under a backup while it is
  being restored, so every gap on restore is damage.
- **A failed write into data storage now aborts the restore** instead of being
  skipped, and the staging partial it leaves behind is removed. A restore onto
  a full or read-only volume previously reported `completed`.
- A temp-file failure is told apart from an unreadable backup object even
  though both surface from the same read call, so a full temp filesystem aborts
  the restore rather than counting as skipped objects.
- **Data files the backup holds under names its listing hides are reported,
  not silently left behind.** An object store returns dot-prefixed keys
  (`._foo.parquet` debris from a sync tool, for example) and the backup copied
  them, but the local backup store's listing hides dot-prefixed names, so the
  restore could never see them. They are now counted as `unaddressable_files`
  with an `unaddressable_sample`, and the error says to rename them in the
  backup and re-run, which recovers them. They are told apart from files that
  are genuinely gone (`missing_files`).
- **Restoring a backup that was itself incomplete is announced** at the start
  and reported as `backup_skipped_files` and `backup_unaddressable_files` on the
  status endpoint, so a gap that predates the restore is not mistaken for one it
  caused.
- The backup manifest's `skipped_files` now counts data files only, the same
  population as `total_files`, so a restore can compare the two; Iceberg
  warehouse metadata that could not be read at backup time is recorded
  separately as `skipped_metadata_files`. Previously both were folded together,
  and a restore comparing them would have let one missing data file per
  metadata skip go undetected.

### TOCTOU race in MQTT subscription restart ([#301](https://github.com/Basekick-Labs/arc/issues/301))

`RestartSubscription` now reserves the subscription slot with a nil-placeholder before
releasing the manager lock, preventing concurrent start or restart operations from
launching duplicate subscribers while configuration is loaded and the new subscriber starts.
A start or restart that lands during an in-flight restart now receives `409 Conflict`,
and the previous subscriber's disconnect no longer runs under the manager lock.

Contributed by [@Thundercloud12](https://github.com/Thundercloud12) in [#766](https://github.com/Basekick-Labs/arc/pull/766).

### Tiering no longer retries an unusable storage key every cycle ([#758](https://github.com/Basekick-Labs/arc/issues/758))

[#747](https://github.com/Basekick-Labs/arc/issues/747) taught compaction,
reconciliation, replication and edge-sync export to quarantine a storage key no
backend can address instead of retrying it, and left tiering for a follow-up.
This is that follow-up.

The tiering migration is cron-driven and recomputes its candidate set from the
SQLite file index on every cycle, with no failure backoff, no skip list and no
attempt cap. A hot file whose key the storage contract refuses (one written
under a folded name by a pre-[#741](https://github.com/Basekick-Labs/arc/issues/741)
Arc, indexed by the pre-migration scan because local listings do not filter
such keys) failed the copy with the same permanent error every night, stayed in
the hot tier, was re-selected the next night, and wrote a fresh failed-migration
row into the history table each time, forever. Reconciliation had the smaller
version of the same loop: a cold row with such a key failed its hot existence
check on every pass for the 48 hours the reconcile window covers.

Both sites now recognise the permanent error, log once at Error, count it in
`arc_storage_invalid_path_quarantined_total`, and **quarantine the file index
row**. Because the work set is rebuilt from SQLite every cycle, the quarantine
is persisted on the row (`tier_files.quarantined_at` and `quarantine_reason`,
added on first open of an existing database) rather than remembered in memory,
so it survives restarts and the nightly rescan. A quarantined row keeps its tier
and its file: on local storage the file is a real data file inside the
partition glob and the query path still serves it, so the index keeps saying it
exists in hot. Only the two work-set queries, migration candidates and
recently-migrated reconciliation, stop returning it.

What the operator sees:

- The cycle that discovers the condition reports one failed migration and
  writes one failed-migration history row, and the next cycle reports none.
- `GET /api/v1/tiering/status` gains `quarantined_files`, which should be zero.
- `GET /api/v1/tiering/files` shows `quarantined_at` and `quarantine_reason` on
  affected rows.

Nothing clears a quarantine, deliberately. The only remedy for a permanently
unusable key is renaming the object in storage, and a renamed object has a new
key that the next scan registers as a fresh row and migrates normally.

Quarantine is decided on `storage.ErrInvalidPath` alone. A transient copy or
existence-check failure keeps the row in the work set and is retried next cycle
as before, and each site carries a test that pins that, because widening the
rule to "any failure" would retire a file from tiering on a network blip. The
streaming copy also now reports the permanent error whichever side of the pipe
fails first, since which goroutine wins that race is not deterministic and the
classification must not depend on it. The tiering test double now enforces the
storage key contract like the production backends, so these branches are not
dead code under test.

### A backup no longer reports success while silently omitting files ([#756](https://github.com/Basekick-Labs/arc/issues/756))

A backup could finish, report success, and be missing data files that exist in
storage, with nothing anywhere saying so.

Listings deliberately hide a file whose key does not follow the storage key
rules, because handing that key back turns every read that follows into a
failure. On local storage those hidden files are not debris: they are real
Parquet files holding real rows, and the query path still returns those rows,
because it matches files on the filesystem rather than going through the storage
layer. So the file was queryable and invisible at the same time.

Backup inventories what a listing returns, and everything that exists to make an
incomplete backup visible (the skipped-file count, the "backup will be
incomplete" warning, and the guard that fails a backup when too much of it could
not be read) counts only files that were inventoried and then failed to copy. A
file the listing never returned reached none of them. Before the listings
started filtering, the same file failed loudly when the backup tried to read it,
and was counted.

Two shapes produce such a file, and both are what an older Arc wrote rather than
anything you can create today: a filename containing a backslash, which is a
legal filename on Linux and refused because Azure treats it as a separator, and
a path longer than the key limit, which is reachable by nesting.

What changes:

- A backup now reports these files: a count and a sample of their paths in the
  backup manifest, a warning naming them, and a metric
  (`arc_storage_unaddressable_files_total`) so it is visible without reading a
  manifest. The message says to rename them, which is what actually recovers the
  data.
- **A backup whose data files are all unaddressable now fails.** It previously
  reported success over a backup containing nothing, because the guard that
  catches a partial backup divides by the number of files it inventoried, and
  that number was zero.
- Storage backends gained a way to enumerate what a listing hid, which is what
  makes any of the above possible. This is the counterpart the previous release
  already established for write-staging partials, which were hidden from
  listings and given their own way to be found; the key-rule drop had no
  equivalent.

The enumeration is defined as what a full listing sees and the ordinary listing
does not return, rather than by re-checking the key rules. That distinction
found a second case: a file whose name begins with a dot passes the key rules
and is writable and readable through the storage layer, yet listings skip it, so
it was missing from backups too. Checking the rules again would never have
reported it.

Nothing about which files are queryable changes, and a healthy deployment
reports nothing.

### Cleanup and replication loops no longer retry an unusable storage key forever ([#747](https://github.com/Basekick-Labs/arc/issues/747))

[#743](https://github.com/Basekick-Labs/arc/issues/743) made a refused storage
key report a permanent, identifiable error, and documented that a loop meeting
one should quarantine the entry rather than retry it. Nothing acted on that yet,
and the loops the note was written about kept treating it as a passing I/O
failure.

A key gets into this state by being stored, not by being typed. The cluster
manifest's own validator is looser than the storage contract on purpose: it runs
inside Raft `Apply`, which includes log replay, so tightening it would make a
node refuse an entry an older binary accepted and two versions would build
different state from one log. The gap that leaves is exactly six spellings, and
compaction manifests and edge-sync ledger rows are persisted state that can
carry one written by an earlier version.

What each loop did with such an entry, and does now:

- **Peer file replication** fetched the whole file body from a peer and only
  then failed writing it, once per candidate peer, once per attempt, on every
  catch-up walk and every reconciliation pass. The entry is now recognised at
  the first backend call and no peer is contacted. Its test suite runs in 3
  seconds where it took 108 before, which is the retry storm made visible.

  The reader's query gate stays **closed** for that entry, deliberately. The
  file really is absent, the read path is a glob so a query over that partition
  would just return fewer rows, and `query.gate_on_catchup` exists to turn that
  silence into a 503. An entry no peer holds is equally unsatisfiable and holds
  the gate today; this one gets no exemption. Puller stats and the 503 body
  gained an `invalid_path` key so the reason is legible.

- **Compaction manifest recovery** could not delete the manifest and could not
  confirm the output, so it reprocessed it every cycle and its input files were
  held out of compaction indefinitely. The manifest is now parked alongside
  itself with a `.quarantined` suffix, which takes it out of the recovery work
  set and releases its inputs while keeping the record of what it described.
  It is parked rather than deleted because the inputs may already be gone: the
  only binary that could have written such a manifest is one whose upload and
  source deletion both succeeded.

- **A compaction job** failed outright when one of its inputs had an unusable
  key, every cycle, because the "already compacted, skip it" escape hatch is
  gated on an existence check that fails the same way. That input is now
  skipped and the rest of the partition is compacted. The skipped file is not
  deleted and its manifest entry is not dropped.

- **Reconciliation** counted these as transient in a bucket whose log line
  promises "next run retries". Both sweeps now separate the two, and a run
  report carries `skipped_transient` and `skipped_invalid_path` as separate
  numbers, because waiting helps with one and never helps with the other. The
  entries are reported, not deleted: these sweeps issue the irreversible
  operations, and on an object store an object can sit under the literal key
  where the listing filter hides it, so absence is unobservable here rather
  than established.

- **Air-gap bundle export** kept the entry on an existence-check error, by a
  rule written for transient failures, and the copy then aborted the entire
  export rather than one file. Nothing on that path caps attempts, so a single
  unusable file stopped all telemetry leaving the site permanently, on the
  deployment least able to receive a visit. It is now marked skipped.

- Serving a peer fetch for such a path answered `backend`, which reads as a
  transient fault on the peer; it now answers `invalid_path`, which is the code
  that already meant this. Peer-fallback behaviour is unchanged.

- The Phase 4 local delete worker never retried, so nothing was stuck there, but
  it logged a permanent condition at the same level as a backend hiccup. It now
  says plainly that the local copy can never be removed by Arc.

A new counter, `arc_storage_invalid_path_quarantined_total`, aggregates every
one of these. It matters more than most: after this change there is no retry
storm, no growing error log and no stuck queue to notice, so the counter and the
per-site Error lines are the whole signal. It should sit at zero.

**Azure batch deletes were also unsafe** and are fixed here, because the test
that pins the contract is what exposed it. `DeleteBatch` was the one key-taking
method on any backend that never validated its input, and on Azure a backslash
is a path separator, so a batch carrying `a\b.parquet` deleted the unrelated
blob `a/b.parquet`. Bad keys are now collected and reported the way S3 already
did, without failing the rest of the batch.

**A five-byte window in this release's own manifest fix is closed here too.**
The manifest filename fix above generates a hashed name once the filename passes
255 bytes, but the key validator subtracts the write-staging suffix and refuses
anything over 250, so a job id of 246 to 250 characters produced a filename the
generator considered fine and every write refused: the manifest write failed and
that partition's compaction failed on every cycle, which is exactly the failure
that fix removed. The limits the validator actually enforces are now exported,
so a caller that builds a key bounds it by the same number the backend checks.
Neither the window nor the fix ever appeared in a release.

One asymmetry is left in place and tracked separately: local listings do not
filter unusable keys the way S3 and Azure have since #743. Filtering them there
would be consistent, but on local these are real data files rather than the
object stores' directory markers, and silently omitting them from a backup is
worse than the listing being inconsistent. That trade-off deserves its own
decision rather than a drive-by.

### A write could destroy an already-acknowledged write ([#744](https://github.com/Basekick-Labs/arc/issues/744))

Local storage stages every streamed write at `{key}.part` and renames it into
place on success. The staging file of key `x` was therefore the same file as the
committed object `x.part`, and the two destroyed each other.

Writing `x.parquet.part` returned success. The next write of `x.parquet` opened
its staging file with truncate, wiped the committed object, and renamed it away,
so the first key stopped existing. Three further orderings were worse: a
committed `x.part` with no `x` present made size and range reads report the
wrong object's bytes while existence checks said it was absent, which could make
the cluster puller skip replicating a file that is not there; and an append
could rename a committed object over an unrelated third key.

The staging suffix is now reserved, so no key can name a staging file, and the
callers that legitimately need a partial (edge-sync resume, backup cleanup)
address it through an explicit interface rather than by spelling the suffix
themselves. Staged partials are also hidden from listings, because a listing
must never return a key the backend would refuse, and a new listing method
exists so an abandoned partial can still be reclaimed rather than becoming
invisible and permanent.

This needed a key ending in `.part`, which Arc's own writers never produce, so
it was reachable only by restoring a backup that contained an orphaned staging
file.

Two consequences worth knowing. Deleting a database now also reclaims any
staged partial underneath it, which a failed upload could previously leave
behind indefinitely, and which also kept the directory from being removed. And
because the reservation applies to every backend so that a key stays portable,
an object literally named `something.part` that already exists in an S3 bucket
or Azure container is no longer readable or deletable through Arc. Arc cannot
have written one, since only local storage stages; remove it with your provider's
own tooling if you have one.

### Compaction manifests no longer exceed the storage key limit ([#744](https://github.com/Basekick-Labs/arc/issues/744))

A compaction manifest filename repeated the partition path that its job id
already contained, and the database three times over. With a 30-character
database and a 60-character measurement that produced a 270-byte filename, past
the 255-byte limit a path component can have, so writing the manifest failed and
compaction for that partition failed on every cycle. At the longest permitted
names it reached 508 bytes.

The filename is now the job id alone, which is already unique and already
carries the partition. Nothing reads these names, so manifests written by an
earlier version are still found and recovered, and no migration is needed.

### Retention deleted nothing on S3 deployments with a configured prefix ([#746](https://github.com/Basekick-Labs/arc/issues/746))

If `storage.s3_prefix` was set, retention never deleted a single file.

Retention lists the files for a measurement, then asks DuckDB for each file's
maximum timestamp to decide whether it is past the cutoff. It built that file's
URL as `s3://{bucket}/{key}` and left the configured prefix out, while every
write goes under the prefix. So the URL named an object that does not exist,
the read failed, and the per-file error handler logged a warning and moved on to
the next file. Every file took that path, so nothing was ever eligible.

Nothing said so. The policy run then recorded itself as `completed` with a
deleted count of zero, which is also what the API and the execution history
reported, so an operator checking whether retention was working saw a series of
successful runs. The only symptom was data that never aged out, and one
`Failed to read file metadata` line per file per cycle.

Live since **v26.03.2**, when the prefix option was added. That change updated
the identical URL builder in the delete path and did not update this one.
Unprefixed S3, Azure and local deployments were never affected.

The root cause was that the same builder existed in several places by hand.
There is now one, `storage.ObjectURI`, which every component that reads the
object store directly goes through, so a backend's prefix cannot be honoured in
one place and forgotten in another. See the upgrade note above before
upgrading: the first cycle after this fix will delete everything that
accumulated while retention was doing nothing.

### Partition pruning silently stopped working on S3 deployments with a configured prefix ([#746](https://github.com/Basekick-Labs/arc/issues/746))

Also only with `storage.s3_prefix` set, and from the same cause in the opposite
direction.

Partition pruning narrows a time-bounded query to the hour and day directories
it needs, then checks which of them exist before handing the list to DuckDB.
Converting a directory URL back into something it could list stripped the
scheme and the bucket but not the configured prefix, and the listing call adds
the prefix itself. The listing therefore ran against `prefix/prefix/...` and
came back empty **with no error**, so every partition was judged absent and the
query fell back to scanning the measurement's full glob.

Results stayed correct throughout; only the optimisation was lost. On a large
measurement that is the difference between reading one hour and reading
everything, so affected deployments should see time-bounded queries get faster
after upgrading.

Tiered queries were not affected: per-tier pruning already trimmed the tier's
own root and carries a comment describing this exact hazard. Only the
single-tier path was doing scheme-and-bucket surgery. It now trims the storage
root it parsed out of the measurement's own glob, the same way the tiered path
does, and a test pins that the parsed root and the URL builder agree. A URL that
does not sit under that root no longer falls back to stripping the scheme: that
produced a prefix the backend would re-prefix and list against some other key
space, reporting every partition absent with full confidence. Existence
filtering is skipped instead, so the query falls back to the full glob.

### The query path now validates database and measurement names against the storage key contract ([#746](https://github.com/Basekick-Labs/arc/issues/746))

No operator-visible behaviour changes here for valid names. This closes the
structural gap the two fixes above came out of.

The previous release made the key rule a property of every storage backend
(#743). That covers writes. Reads never go through a backend at all: Arc builds
an `s3://` or `azure://` URL and hands it to DuckDB, which reads the object
store itself, and the Iceberg exporter hands paths to its own file layer the
same way. A name that a backend would refuse could therefore still reach the
object store through a query.

Reads now go through the same contract, plus one rule that writes do not need.
A key containing `*`, `?`, `[` or `{` names exactly one object to a write, so
the contract accepts it; interpolated into a query it is a pattern, and
`cpu*` would read every measurement whose name starts with `cpu`. Read paths
reject those characters, writes still accept them.

A name that cannot be turned into a path now fails the query with an explicit
error. The alternative, substituting a path that matches nothing, would have
been reported to the client as a successful query over an empty table, because
Arc deliberately treats "no files matched" as an empty result rather than an
error. Listing endpoints, whose names come from enumerating storage rather than
from a request, leave the displayed path empty instead of failing the listing.

Also in this change, all of it internal:

- The five separate implementations of "is this name safe as a path" are
  reconciled. They disagreed with each other: the cluster file-fetch validator
  was believed to be stricter than the contract and was measurably looser,
  accepting backslashes and over-long keys the contract refuses, and the delete
  endpoint refused any name containing `..` anywhere, so a measurement called
  `a..b` could be written and queried but not deleted from. They now share one
  rule, with the two deliberate divergences documented and pinned by a test:
  the Raft manifest validator stays looser because it runs during log replay
  and tightening it would make different versions of Arc build different state
  from one log, and the HTTP API additionally hides dot-prefixed names, which is
  a display rule rather than a storage one.
- Retention now reports when it had to skip a file whose stored path it cannot
  read, instead of counting the run as a clean success. Such a file is skipped
  on every future run too, so its data never ages out, and that is exactly the
  silent shape of the prefix bug above.
- The documented list of ways a list prefix is allowed to be looser than an
  object key was one entry out of date, and is now checked by a test rather
  than only stated in a comment.
- Three unused S3 URL builders were removed rather than left as further copies
  of the builder above.

### Every storage backend now enforces the same key contract ([#743](https://github.com/Basekick-Labs/arc/issues/743))

The previous release made local storage refuse malformed keys
([#741](https://github.com/Basekick-Labs/arc/issues/741)). It was the only
backend that did, so one key behaved three ways, and the fix's own comment
claimed a guarantee that held for one implementation out of three.

The rule is now a property of the `Backend` interface, enforced by local, S3 and
Azure alike. What it guarantees is that two different keys can never name one
object, which is the property whose absence produced #574, #737 and #741.

**It also fixes a collision #741 introduced.** `"coll"` and `"coll/"` resolved
to the same local file: two keys, one object, and the first write silently lost.
The exemption existed because list prefixes legitimately end in a separator, and
applying a prefix rule to keys is what broke it. Keys and list prefixes are now
validated separately, so `""` and a trailing separator remain valid for
enumeration and are refused for a key.

Behaviour on the object stores, all measured against a live MinIO and Azurite
rather than reasoned about:

- A leading separator is refused. MinIO silently strips it, so `/a/x` and `a/x`
  were one object there, while S3 and Azure keep them apart. One key, three
  outcomes.
- A backslash is refused, because Azure treats it as a separator: `a\b` and
  `a/b` are **one blob**.
- `.` and `..` segments and empty interior segments are refused locally now,
  with a message naming the key. MinIO already rejected all three, but as a
  remote 400 describing an S3 API error rather than the key at fault. Azure
  stored them literally.
- Keys that merely contain dots, such as `a..b` or `..foo`, are accepted
  everywhere and stored under the name asked for.

**The S3 prefix is validated rather than repaired.** The old sanitiser's damage
was on its success path: `/` stayed `/`, `a//b` became `a//b/` and `.` became
`./`, each of which made every write fail against MinIO, while `a/..b` was
silently replaced with the empty string. That last one is not a safe fallback,
it is the bucket root, so a typo relocated an entire deployment without a word.
A prefix that cannot form usable keys now stops the backend from starting.

A listing also never returns a key the backend would then refuse. Object stores
carry "directory marker" objects whose key ends in a separator, written by
consoles and sync tools rather than by Arc, and Arc feeds listings straight into
reads and deletes in a dozen places. Returning one would have been worse than
the original problem: a restore would skip the file and still report success.
Those entries are filtered out of listings, so what a listing returns is always
usable.

A MinIO service is wired into CI for this. Every behaviour above is a property
of the servers rather than of any mock, no unit test could have found them, and
the directory-marker case was found only by writing one through the raw S3 API
and watching a read of it fail.

### Local storage rejects malformed paths instead of rewriting them ([#741](https://github.com/Basekick-Labs/arc/issues/741))

The local storage backend used to repair the paths it was given: every `..` was
replaced with `_` and NUL bytes were stripped, with a comment saying this
prevented directory traversal. It did not. Containment was enforced separately,
by resolving the path and checking it still sat under the storage root.

What the rewrite did do was make two different keys able to name one file.
`a..b` and `a_b` were the same location, and so were `..foo` and `_foo`. That
is the root of two bugs already fixed in this release: the edge-sync source
path ([#574](https://github.com/Basekick-Labs/arc/pull/574)) and the edge-sync
spoke ID ([#737](https://github.com/Basekick-Labs/arc/issues/737)). Each was
closed by teaching one caller not to send `..`, which left the next caller to
rediscover it.

A malformed path is now refused rather than quietly turned into a different
one, so the mapping from key to file is one-to-one for every caller at once.
Refused: absolute paths, any `.` or `..` segment, an empty interior segment
such as `a//b`, NUL bytes, and backslashes. Still accepted, and now stored under
the name actually asked for: filenames that merely contain dots, such as `a..b`
or `..foo`.

The check got cheaper as a side effect. Once a path is known to be clean and
relative, the three `path/filepath` calls that followed it were redundant: one
normalised input proven not to need it, one re-resolved an already absolute
path, and one recomputed a containment property that now holds by construction.
Validation is a single pass with no allocation, and the containment proof moved
into a property test rather than being paid for on every call.

```
                          before      after
BenchmarkValidatePath      605 ns/op   110 ns/op   1 alloc, unchanged
BenchmarkValidatePathDeep  757 ns/op   150 ns/op   1 alloc, unchanged
```

In proportion: about 0.06% of a 4 MiB Parquet write, and roughly half of a bare
existence check. It matters in the stat-heavy loops that reconciliation and
tiering run, and is noise on the ingest path. Nothing gets slower and the
allocation count is unchanged, which is the part that matters under
concurrency.

Two audits back the change. Instrumenting the backend and running the full test
suite found no path anywhere in Arc that the rewrite altered, and none that
normalisation altered either. Both are evidence rather than proof: neither can
see a path built from stored operator configuration or from an ingest field the
validators happened to skip, and review found one of each.

So the surfaces that build a storage key from a name now validate it. Retention
policies reject a malformed database or measurement on create and on update, and
any already stored that can no longer resolve is named at startup. The per-name
database routes return 400 rather than letting the storage layer refuse the key
later. A MessagePack record with an empty measurement name is refused instead of
being skipped by the validator and reaching the writer, and MQTT does the same,
including for the database its topic mappings select. An empty measurement used
to produce `{database}//{year}/...`, which normalisation collapsed so the rows
landed a level up, under the database itself.

The `mqtt.subscriptions` config is now validated at load: a subscription whose
database, or whose topic-mapping target, is not a usable name fails at startup
rather than at the first flush.

If you have a deployment that stored data under a folded name, that file is now
addressed by its real name and the folded spelling no longer reaches it. The one
known way to be in that state is an edge-sync spoke registered before
[#737](https://github.com/Basekick-Labs/arc/issues/737), which the hub already
names at startup.

### Edge-sync spoke IDs can no longer collide with another spoke's namespace ([#737](https://github.com/Basekick-Labs/arc/issues/737))

A spoke identifier containing an embedded `..`, such as `rocket..01`, passed
validation. The local storage backend then folded every `..` to `_` while
resolving the path, so that identifier and the legitimate `rocket_01` landed in
one directory, and a spoke authenticated as one could write into the other's
namespace. The HMAC proves which spoke is asking; the namespace mapping is what
decides where it may write, and that mapping was not injective.

`validateSpokeID` now rejects the sequence anywhere in the identifier, closing
both the receive path and registration. This is the same rule
[#574](https://github.com/Basekick-Labs/arc/pull/574) applied to the source
path, which left the identity that prefixes it open. The validator also gained
the bounds it was missing next to a field it shares a table with: a 128-byte
cap, no control characters, and no leading or trailing whitespace. An over-long
ID used to register successfully and then fail every write with
`ENAMETOOLONG`, wedging the spoke with nothing said at registration.

Reaching the collision required an administrator to create a spoke ID
containing `..` in the first place: registration is admin-only, secrets are
generated server-side, and there is no self-registration, so no advisory was
issued. It affects the local filesystem backend only, since S3 and Azure
preserve key spelling, and edge sync is opt-in.

**If you already run a spoke whose ID is now refused**, the hub says so at
startup, naming each stored ID and why, so you do not have to wait for an edge
box to fail. Re-registering is not simply a rename: for an ID like
`rocket..01`, the data it already synced is on disk under `rocket_01/`, which
is the other spoke's namespace, and the hub's file index is keyed on the spoke
ID, so a spoke re-pointed at a fresh ID finds no index rows and re-uploads its
whole backlog into the new namespace. Plan the move deliberately, deciding what
happens to the files already written under the folded name.

Reported by **[@rexpository](https://github.com/rexpository)**.

### Arrow IPC cleanup no longer runs twice on the panic path ([#733](https://github.com/Basekick-Labs/arc/issues/733))

No behaviour changes for anyone running Arc. This is recorded because the code
was correct for a reason nothing in the repo could check.

`POST /api/v1/query/arrow` freed its DuckDB reader, pooled connection and query
timeout from two places: an ordinary statement at the end of the stream writer,
and again from the panic handler. A panic raised after the first one, in the
trailer or logging statements that follow it, ran the whole cleanup a second
time.

That was harmless, and it is worth being precise about why, because the first
version of this report claimed otherwise. The DuckDB reader's `Release` has an
explicit "already at zero" guard, `*sql.Conn.Close` returns
`sql.ErrConnDone` rather than failing, and cancelling a context twice is
allowed. Nothing was over-released and nothing leaked.

The problem was that none of those three properties is promised by the
interfaces Arc codes against, and none was pinned by a test. Arrow's own readers
guard over-release behind an assertion that is compiled out unless the build
sets `-tags assert`, which no Arc build does, so losing the invariant would not
have failed anything anywhere. It would have gone unnoticed rather than
corrupted anything, which is the point: there was no signal to rely on.

Cleanup is now a single deferred call, which runs exactly once whether the
writer returns normally or unwinds, and matches what every other stream writer
in Arc already did. It also covers a panic raised inside the panic handler
itself, ahead of where the release used to sit, which would have stranded a
pooled connection.
### The measurement endpoint now appears in query management ([#731](https://github.com/Basekick-Labs/arc/issues/731))

`GET /api/v1/query/:measurement` never registered with the query registry, so
it was missing from `GET /api/v1/queries/active` and `/history` and could not be
stopped through `DELETE /api/v1/queries/:id`. An operator watching queries could
not see it, and an expensive one could not be stopped.

It now registers like `POST /api/v1/query`: the response carries
`X-Arc-Query-ID`, the query shows up while running and lands in history with the
right disposition, including `timed_out` rather than a generic failure, and a
capped result carries `row_cap` as it does elsewhere.

**These queries are now cancellable**, which they were not before. Cancelling
stops the stream at the next chunk boundary; a single long-running sort or
aggregate is not interrupted mid-chunk, so a cancel can take until that chunk
materialises. A cancelled stream is marked truncated by the same signalling as
any other interrupted response, so a client never reads a cancelled result as
complete.

`HEAD` on this route is deliberately not registered. Fiber routes `HEAD` to the
same handler while the response body is discarded, so registering would leave a
history entry whose only outcome is a spurious connection failure.

`POST /api/v1/query/arrow` is unchanged and still creates no history entry;
#731 stays open for it.

### Arrow IPC responses no longer write trailers from the wrong goroutine ([#729](https://github.com/Basekick-Labs/arc/issues/729))

`POST /api/v1/query/arrow` set its HTTP trailers from inside the body-stream
writer. fasthttp runs that writer on a goroutine of its own while the connection
goroutine serialises the response head, and both mutate the same response-header
buffer. fasthttp states the rule this broke: "Response instance MUST NOT be used
from concurrently running goroutines."

**Affects v26.06.1 through v26.09.1.** The `Arc-Execution-Time-Ms` trailer
arrived in v26.06.1 and is set on every Arrow IPC response, so every such
response carried the defect. The two trailers added earlier in this release
cycle, `Arc-Stream-Truncated` and `Arc-Rows-Capped`, inherited it.

Two ways it could surface. The mild one is a torn status line or a garbled
trailer on a single response. The serious one needs a client disconnect: Arc
abandons the body stream when the write fails, resets the response, and reuses
it for the next request on a keep-alive connection, while the stream writer is
still running and still sets its trailer. That write lands on a response that
now belongs to a different request.

Both are timing-dependent and neither corrupts stored data. Nothing needs to be
re-run or repaired after upgrading, and no configuration changes.

Trailer values are now collected by the writer and published by the connection
goroutine, in the window fasthttp guarantees between the body ending and the
trailers being written. A contract test pins that ordering, so a future fasthttp
upgrade that changes it fails a test rather than silently emitting empty
trailers.

It went unseen because no test had ever driven that handler to a successful
response over real HTTP. Adding one for #724 is what surfaced it.
### Query history records when a governance row cap truncated a result ([#728](https://github.com/Basekick-Labs/arc/issues/728))

An Enterprise governance row cap was visible to the client and in the operator
log, but `GET /api/v1/queries/history` still showed a capped query as an
ordinary success carrying the truncated count. Someone reviewing history to
work out why a report came back short found no answer on the surface built for
that question.

History entries now carry `row_cap` when the query reached a cap, alongside the
existing `row_count`. The status stays `completed`, because the query did
finish and every other status value means it did not; the cap is a property of
the result, not a different outcome. The field is recorded before the query is
dispositioned, so a result that reached the cap and then failed on the way out
keeps the cap and appears in history as both capped and failed.

As on the wire, the field means the result reached the cap and may therefore be
incomplete. It does not prove rows were dropped: a query whose own `LIMIT`
equals the cap reaches it on every run with nothing lost. The entry's SQL sits
alongside, which is what tells the two apart.

Note that `/api/v1/query/arrow` does not create history entries at all, so a
capped result there is still absent from history rather than misreported
([#731](https://github.com/Basekick-Labs/arc/issues/731)).
`/api/v1/query/:measurement` is covered, see below.

### A governance row cap no longer looks like a complete result ([#724](https://github.com/Basekick-Labs/arc/issues/724))

When an Enterprise governance policy capped a query with `max_rows_per_query`,
the response was byte-identical to a complete one. The cap path returned no
error, logged the ordinary "query completed" line, and emitted a normal
end-of-stream marker and trailer on every wire format. Nothing said the result
had been cut. Unlike the Arrow IPC truncation described below, this is reachable on
purpose and by configuration, so an operator could hit it every day without
knowing, and a dashboard quietly capped at 10,000 rows drew conclusions from
truncated data.

The 2026.02.2 notes that introduced the feature described the cap as returning
"partial results with a warning". No warning existed. This release makes that
claim true rather than correcting it.

A capped result now says so on all four wire formats:

- The JSON envelope carries `"rows_capped": true` and `"row_cap": <N>`. This
  covers both JSON writers: the Arrow-backed one behind `POST /api/v1/query`
  and the `database/sql` fallback that also serves the parallel-partition and
  measurement paths.
- The msgpack envelope carries the same two keys.
- Both keys are present together or not at all, so an uncapped response is
  unchanged on the wire and the msgpack envelope keeps its historical seven or
  eight keys.
- Arrow IPC carries an `Arc-Rows-Capped` response trailer holding the cap. A
  trailer for the same reason `Arc-Execution-Time-Ms` is one: whether the cap
  was reached is only known once the last batch is written, long after the
  status line went out. Like every registered trailer it is emitted on every
  response, empty when it does not apply, so the client contract is "non-empty
  means capped".

A plain response header was the first choice and does not work. Headers are
committed before the body streams, so a header can announce that a cap applies
but never that the result reached it, which is the fact a client needs.

Read `Arc-Rows-Capped` alongside `Arc-Stream-Truncated`, which means close to
the opposite. `Arc-Stream-Truncated` says the body is short because the stream
failed and must not be trusted. `Arc-Rows-Capped` says the body is short
because policy said so, and is a valid, complete result up to the cap.

This pairs with `truncated` / `truncation_reason` from the JSON fix below, and
the two answer different questions. `truncated` means the stream failed and the
result is short by accident. `rows_capped` means policy stopped it on purpose
and the rows delivered are a valid result up to the cap. A response can carry
both, when a stream reaches the cap and then fails on the way out; each field
still means exactly what it means alone.

Server side, a capped query now emits a WARN naming the responsible token
(`token_id`, `token_name`, `row_cap`, `row_count`) and increments the new
`arc_governance_queries_capped_total` counter. The "query completed" line stays
at Info, so existing log filters keep working. Only queries that actually reach
a cap log or count, so a token that merely has a policy adds no log volume.

One consequence worth knowing before you alert on the counter: a query whose own
`LIMIT` equals the policy cap reaches the cap on every run, so it is marked, and
logged, and counted every time, even though nothing was ever dropped. If a
dashboard issues `LIMIT 10000` against a `max_rows_per_query` of 10000, raise
the cap above the limit and the noise goes away. Arc cannot tell the two apart
without fetching a row past the cap, and that fetch is not free:

The marker means "this result reached the cap and may be incomplete", not "rows
were definitely dropped". A stream that stops exactly at the cap cannot know
whether another row was waiting without fetching one, and fetching one is worse
than the ambiguity: on the Arrow IPC path it would let a policy timeout firing
during that extra fetch poison a result that was complete at the cap, turning a
readable response into an undecodable one. Reaching the cap is what the client
needs in order to stop trusting the row count, and it is always known for
certain.

One related gap is unchanged. The experimental arcx Arrow IPC serve path
applies no row cap at all, and returns before any trailer is registered, so an
arcx-served Arrow IPC response carries none of the three trailers
([#727](https://github.com/Basekick-Labs/arc/issues/727)); clients should read a
missing trailer as "unknown", never as "not capped".

`[governance]` is also now documented in the reference `arc.toml`, which
shipped with none of its keys.
### JSON query responses now say when the result was cut short ([#723](https://github.com/Basekick-Labs/arc/issues/723))

A JSON response opens with `{"success":true,...,"data":[` before the first row
is known, and both streaming writers closed the document whether or not the
stream had failed. A client that was still connected therefore received HTTP
200, valid JSON, `success: true`, and silently fewer rows than its query
matched, with `row_count` reporting the rows delivered rather than the rows
found. A query that exceeded `query.timeout` partway through delivering a large
result was the likeliest way to hit it.

A failed stream now adds `"truncated": true` and a `truncation_reason` to the
closing envelope. **Clients that need to distinguish a complete result from a
partial one should check `truncated`.** The document still parses, and the rows
already delivered are still there, so a client that ignores the field behaves
exactly as before.

`success` is deliberately left as-is. It is written before the first row and, on
any result large enough to have flushed, has already reached the client and
cannot be revised. Emitting a different shape for small results would make the
response depend on whether the result happened to fit in a buffer.

This is the JSON counterpart to the Arrow IPC fix in the same release. Failures
that happen before streaming begins are unaffected: those still return a real
error status, as they always have. MessagePack responses were never affected,
because they commit their counts up front and a cut is a hard decode error.

### A truncated Arrow IPC response is no longer readable as a complete one ([#721](https://github.com/Basekick-Labs/arc/issues/721))

When an Arrow IPC stream was cut short, the client could not tell. Arc flushes
after every record batch, so a stream that stopped early stopped on a batch
boundary, and an Arrow reader treats that as a clean end. The paths that still
closed the writer went further and appended a valid end-of-stream marker. The
result was HTTP 200, a well-formed Arrow stream, and silently fewer rows than
the query matched. A query that exceeded `query.timeout` partway through
delivering its results was the most likely way to hit it; a failure before the
first batch produced a schema-only stream that read as a legitimate empty
result, which a dashboard shows as no data rather than as an error.

A failed stream is now marked so the client's decode fails instead. Complete
responses are unchanged, and a client that has already disconnected is not
written to, since there is nobody left to inform.

A stream cut short by a panic now also carries a reason in the
`Arc-Stream-Truncated` trailer. The in-body marker was already there, so clients
could always tell such a stream apart from a complete one; what they could not
see was why. The error path has always named its cause, and the panic path named
nothing, because setting a trailer from that goroutine was unsafe until #729.

Arc also now distinguishes a dropped connection from an encoder failure on this
path. Both previously surfaced the same way, so a client hanging up mid-stream
was logged as a server-side error and missed the client-disconnect metric.

### A panic while streaming a response no longer crashes the server ([#717](https://github.com/Basekick-Labs/arc/issues/717))

Query responses are streamed from a callback that fasthttp runs on its own
goroutine, where an unrecovered panic takes down the whole process rather than
failing the one request. Only the Arrow IPC writer recovered; the JSON, msgpack,
parallel-partition and measurement writers did not, so a panic on any of those
paths (the DuckDB cgo boundary, a decimal cast, an encoder) would stop the
server. Every streaming writer now recovers, logs the panic, and counts it as a
query error, and the client sees a truncated response instead of a dropped
service.

Recovering also exposed what the unwind had been skipping. Those writers
released their result set, pooled connection and query timeout in ordinary
statements after the streaming call, so the fix moves each into a deferred block
that keeps the original release order. The Arrow IPC writer was the exception
and kept a split between an inline release and the panic handler until the #733
entry above, later in this same release, brought it into line. Six of the writers also disposed of their
query-registry entry only on the normal path, which meant a panicking query would
have sat in `GET /api/v1/queries/active` as `running` forever, holding its SQL
text and inflating the active-query gauge, with no reaper to clear it; those
entries are now failed on the panic path.

### A panic while streaming Arrow IPC no longer leaks a database connection ([#716](https://github.com/Basekick-Labs/arc/issues/716))

`POST /api/v1/query/arrow` streams its response from a callback that fasthttp
runs on its own goroutine, where an unrecovered panic would take down the
process, so the callback recovers. The cleanup that released the DuckDB result
reader, returned the pooled connection and stopped the query timeout timer ran
as ordinary statements after the streaming call, and a panic unwound straight
past them. The connection was the costly one: it was never returned to the
pool, so each occurrence permanently shrank the pool and repeated occurrences
would starve the endpoint until a restart. Cleanup now runs on the recovery
path too, from a single deferred call that covers both paths (the original fix
ran it from the panic handler as well as inline, which the #733 entry above
replaced later in this same release). The per-batch records the stream owns
(row-cap slices,
decimal casts, dictionary-encoded batches) are released through a defer so a
panic mid-batch cannot strand their buffers either, which for DuckDB-backed
records are C-allocated and not reclaimed by the garbage collector.

### Every query endpoint now enforces query governance ([#702](https://github.com/Basekick-Labs/arc/issues/702))

Enterprise query governance was enforced only on `POST /api/v1/query`, so
a token with governance limits could bypass rate limits, quotas, the
per-query row cap, and the per-token execution timeout through the three
other user-SQL endpoints: `GET /api/v1/query/:measurement`,
`POST /api/v1/query/arrow`, and `POST /api/v1/query/estimate` (whose
`COUNT(*)` wrapper executes the full subquery, so it carries real scan
cost). All four now share one enforcement path: rejected requests get 429
(with `Retry-After` for rate limits), the policy's `max_rows_per_query`
caps streamed rows on the row-returning endpoints (JSON, Arrow IPC, and
the database/sql fallback), and `max_scan_duration_sec` overrides the
global `query.timeout` everywhere. That cap was silent when this landed;
see *A governance row cap no longer looks like a complete result* above
for the marker that now accompanies it. RBAC was never affected; this closes a
limits and accounting gap, not an authorization hole.

### The measurement endpoint now honors the configured query timeout ([#308](https://github.com/Basekick-Labs/arc/issues/308))

`GET /api/v1/query/:measurement` executed against `context.Background()` with
no deadline, so a configured `query.timeout` never applied to it and a slow
measurement query could run indefinitely. The handler now derives its context
from the request (so client disconnects cancel the query) and wraps it with
the configured timeout, matching `POST /api/v1/query`: a query that exceeds
the timeout returns 504 `"Query timed out"` and increments the timeout
metrics, on both the Arrow and database/sql paths.

Contributed by [@MrBeldum](https://github.com/MrBeldum) in [#701](https://github.com/Basekick-Labs/arc/pull/701).

### Token `expires_at` is now stored in UTC

`api_tokens.expires_at` is written as a Go `time.Time`, and go-sqlite3
text-encodes those using the value's own location. An Arc running in a non-UTC
zone therefore stored `2026-09-10 15:32:45.958903-06:00` where a UTC one stored
`2026-09-10 21:32:45.958903+00:00` for the very same instant. Both parse back
correctly and expiry is compared as a parsed instant in Go, so no token was ever
accepted or rejected incorrectly — but two rows holding the same moment sorted
differently as text, so any future SQL comparison on the column would have
disagreed with itself depending on which node wrote the row.

Writes now normalize to UTC on the create and update paths, matching the
clustered apply path, which already did this ([#459](https://github.com/Basekick-Labs/arc/pull/459) /
[#460](https://github.com/Basekick-Labs/arc/issues/460)), and the "Arc-stamped timestamps are
UTC" rule from [#546](https://github.com/Basekick-Labs/arc/issues/546). Note this removes the
offset *variance*; it does not make the column text-comparable with
`created_at`, which SQLite's `CURRENT_TIMESTAMP` writes without an offset or
fractional seconds. `expires_at` is still compared as a parsed instant, never as
a string. Existing rows are left as they are — they parse back to the correct
instant, so no token's lifetime changes.
### Tiered query routing filters file metadata in SQL ([#346](https://github.com/Basekick-Labs/arc/issues/346))

`GetStoragePathsForQuery` fetched every file recorded for a database from the
tiering metadata store and filtered by measurement and time range in Go,
pulling every unrelated row out of SQLite per query. The measurement and
partition-time filters are now pushed into the SQL WHERE clause, so SQLite's
indexes prune rows before they cross the query boundary. Returned paths are
unchanged; a busy tiering database just holds far less metadata in memory
per query.

Contributed by [@pujitha24](https://github.com/pujitha24) in [#707](https://github.com/Basekick-Labs/arc/pull/707).

### Arrow IPC streaming has direct disconnect regression coverage ([#425](https://github.com/Basekick-Labs/arc/issues/425))

The Arrow IPC batch loop is now independently testable, with regression checks
for client disconnects, cancelled contexts, deferred reader failures, and decimal
cast cleanup. This protects the existing behavior that stops consuming results as
soon as the output connection fails.

Contributed by [@be-student](https://github.com/be-student) in [#706](https://github.com/Basekick-Labs/arc/pull/706).

### Helm recovery no longer crash-loops during long WAL replay ([#676](https://github.com/Basekick-Labs/arc/issues/676))

The OSS Helm chart now uses a five-minute startup probe before liveness starts,
so a pod replaying its WAL can recover without Kubernetes repeatedly killing it.
The default 10Gi PVC is documented as development-scale; object-storage
deployments should size it for peak ingest, WAL flush lag, cache, and recovery
headroom.

Contributed by [@atirna](https://github.com/atirna) in [#700](https://github.com/Basekick-Labs/arc/pull/700).

### Periodic peer file replication reconciliation repairs missed FSM callbacks ([#393](https://github.com/Basekick-Labs/arc/issues/393))

Cluster nodes now re-walk the Raft file manifest every five minutes by default
after the startup catch-up attempt. The interval is configurable with
`cluster.replication_reconciliation_interval_seconds`. The pass uses the
existing paginated manifest walk, pull queue, deduplication, retries, checksum
verification, and local-file checks, so a missed reactive callback can be
repaired without restarting the node. Healthy writer, reader, and compactor
nodes may run the pass; joining, leaving, unhealthy, and standalone nodes are
gated. Startup catch-up remains a one-shot operation: periodic failures never
reopen readiness, while a successful reconciliation pull may heal previously
recorded startup failure or drop state for that path. The existing
`cluster.replication_catchup_enabled` switch disables both startup and
periodic reconciliation. Recheck outcomes are exposed under
`replication_catchup_status` as `replication_recheck_*` counters.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#697](https://github.com/Basekick-Labs/arc/pull/697).

### Replication can now carry WAL entries up to the full payload cap ([#698](https://github.com/Basekick-Labs/arc/issues/698))

The replication wire format JSON-encoded every entry, and base64 inflates the
payload by 4/3 — so a WAL entry above roughly 75MB could never be framed under
the 100MB message cap even though the WAL stored it happily. The failed entry
tore down the stream, and the resume hit the same entry again: a deterministic
stall for that follower. Entries now travel in a binary frame
(`MsgReplicateEntryBin`) that carries the payload as raw bytes, negotiated per
connection: readers advertise support in the replication handshake, writers
fall back to the JSON framing for readers that predate it, and the per-entry
MAC tags and full-HMAC checkpoints are unchanged by the framing. Mixed-version
pairs keep working exactly as before, including the old limitation, until both
sides run this release.

### Wide-row ingest requests no longer silently bypass the WAL ([#677](https://github.com/Basekick-Labs/arc/issues/677))

A single ingest request larger than the WAL's 100MB per-entry payload cap
(wide rows make this easy to hit well under the 1GB request limit) was
rejected wholesale by the WAL, so ingest kept reporting healthy while the
WAL directory held only a 7-byte header file: writes were durable in
Parquet flushes alone and nothing on the metrics endpoint said so. The WAL
now splits an oversized payload into multiple entries at msgpack element
boundaries (row-format requests between records, columnar requests by row
range), each under the cap and independently replayable with no format
change. A payload that still cannot be split, a single record or row above
the cap, keeps the loud rejection, now also counted in the new
`arc_wal_oversized_payloads_total` metric so a WAL that records nothing
never looks healthy again.

Contributed by [@atirna](https://github.com/atirna) in [#696](https://github.com/Basekick-Labs/arc/pull/696).

### Expired tier-cache entries are pruned on store

Each tier-cache insertion removes expired entries while holding the cache lock,
preventing entries for old partitions from accumulating indefinitely. Expiry
uses the same boundary as the read path and requires no background goroutine.

Contributed by [@mah1104ahm](https://github.com/mah1104ahm) in [#675](https://github.com/Basekick-Labs/arc/pull/675).

### MQTT shutdown waits for unsubscribe acknowledgements

Stop and pause now wait for each unsubscribe token with a bounded timeout and
always disconnect. The manager persists the stopped or paused status even if
the broker rejects an unsubscribe or times out, and returns the shutdown error
after attempting the status update.

Contributed by [@mah1104ahm](https://github.com/mah1104ahm) in [#673](https://github.com/Basekick-Labs/arc/pull/673).

### S3 query paths now follow calendar days across DST ([#321](https://github.com/Basekick-Labs/arc/issues/321))

S3 time-range path generation now builds one inclusive partition path per UTC
calendar day covering the range, matching Arc's UTC partition layout. Non-UTC
inputs are normalized to UTC, and daylight-saving transitions can no longer
shift, skip, or duplicate a date.

Contributed by [@copacabanaservice01](https://github.com/copacabanaservice01) in [#690](https://github.com/Basekick-Labs/arc/pull/690).

### Iceberg version hints no longer advance before metadata copies publish ([#636](https://github.com/Basekick-Labs/arc/issues/636))

Directory-based Iceberg readers fetch `version-hint.text` before opening the matching
`v<N>.metadata.json` copy. A transient metadata read or copy failure could previously
still advance the hint, leaving those readers pointed at an unavailable snapshot.

Arc now publishes the hint only after the matching metadata copy succeeds. The previous
hint remains valid during the failure, and the existing reconciliation retry path
self-heals once storage recovers.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#663](https://github.com/Basekick-Labs/arc/pull/663).

### Invalid Iceberg reconcile intervals now fail at config load

When Iceberg export is enabled, `iceberg.reconcile_interval` must be positive.
Zero and negative values are rejected instead of silently falling back to the
five-minute scheduler interval.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#664](https://github.com/Basekick-Labs/arc/pull/664).

### Iceberg skips unreadable databases during reconciliation

An unreadable database no longer aborts the entire Iceberg reconcile pass. Arc
logs the database and continues reconciling the remaining databases, while a
failure enumerating the top-level database list remains fatal for that pass.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#665](https://github.com/Basekick-Labs/arc/pull/665).

### Dedicated SQLite WAL and SHM sidecars are owner-only

Dedicated SQLite handles now apply 0600 permissions to the database and its
WAL/SHM sidecars, including when the database path uses a symlink. Missing
sidecars remain harmless, and auth-owned handles are not reopened or modified.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#666](https://github.com/Basekick-Labs/arc/pull/666).

### Empty Iceberg measurements cache their negative catalog result

Permanently empty measurement directories with no Iceberg table now cache that
negative state, avoiding repeated catalog lookups while preserving normal table
creation after re-ingest and cache cleanup when the measurement disappears.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#667](https://github.com/Basekick-Labs/arc/pull/667).

### Azure not-found error detection handles joined multi-errors ([#319](https://github.com/Basekick-Labs/arc/issues/319))

Azure not-found error detection (`isAzureNotFoundError`) now uses `errors.As` instead
of a custom single-Unwrap loop, so an `azcore.ResponseError` is correctly identified
even when wrapped inside joined multi-errors (`errors.Join`).

Contributed by [@Thundercloud12](https://github.com/Thundercloud12) in [#670](https://github.com/Basekick-Labs/arc/pull/670).

### Live SQLite backups and restores no longer risk torn database state ([#635](https://github.com/Basekick-Labs/arc/issues/635))

Backups now snapshot live SQLite databases before copying them, so concurrent
WAL writers cannot interleave pages into a backup. Restores no longer touch the
live database at all: the restored database is STAGED next to the live one and
applied at the next server start, before anything opens it. The running server
keeps serving consistent pre-restore data, and the swap (with a complete
`.before-restore` safety copy and stale-sidecar cleanup) happens where no
connection can observe a half-applied state. A staged restore that fails
integrity validation at boot is quarantined instead of applied, and deleting
the `.pending-restore` file before restarting cancels the restore. Temporary
staging files are restricted to the owner.

Contributed by [@atirna](https://github.com/atirna) in [#678](https://github.com/Basekick-Labs/arc/pull/678).

### Compaction job history no longer retains evicted entries ([#315](https://github.com/Basekick-Labs/arc/issues/315))

When compaction history trims to its newest 100 jobs, the retained map references are now copied into fresh slice backing storage. This prevents an older history slice from sharing the manager's current array and retaining evicted pointer-containing entries; the retained maps are intentionally not deep-copied.

Contributed by [@bferanmi806-sketch](https://github.com/bferanmi806-sketch) in [#679](https://github.com/Basekick-Labs/arc/pull/679).

### Local storage directory cache is bounded ([#318](https://github.com/Basekick-Labs/arc/issues/318))

The local backend's directory cache, which avoids redundant `MkdirAll` calls under
sustained ingest load, previously grew without bound as new partition directories
were created. It is now capped at 1,024 entries with eviction on insert; a cache
miss just repeats an idempotent directory creation.

Contributed by [@mah1104ahm](https://github.com/mah1104ahm) in [#674](https://github.com/Basekick-Labs/arc/pull/674).

### Tiered queries now prune partitions per tier ([#662](https://github.com/Basekick-Labs/arc/issues/662))

Since tiering shipped, a query over a measurement with an active cold tier
bypassed partition pruning entirely: both tiers' full globs were scanned on
every query, including S3/Azure LIST and GET calls on the cold archive. The
multi-tier path now prunes each tier against its own backend: hour and day
partitions are generated per tier, existence-filtered with backend-relative
listings, and a tier verified to hold no data for the query's time range is
dropped from the query outright, so a recent-range dashboard query no longer
touches cold object storage at all. File-level time pruning (26.09.2's
`query.file_time_pruning`) applies to the hot tier inside tiered queries too.
Listing failures fail open to the tier's full glob, end-only time predicates
never exclude cold data older than the assumed range start, and completed
tier migrations now invalidate the query caches the same way compaction does.

### Query time zone is now pinned to UTC ([#682](https://github.com/Basekick-Labs/arc/issues/682))

**Behavior change on servers whose host time zone is not UTC.** DuckDB
defaulted its session zone to the host's zone, while Arc's partition pruner,
UTC-hour partition layout, arcxrouter, and JSON output all assume UTC. On a
non-UTC host, a naive timestamp literal (`WHERE time >= '2024-03-15 14:00:00'`)
meant one instant to the pruner and a different one to the engine, so pruned
queries could silently miss matching rows. Every Arc session now runs with
`TimeZone='UTC'`, in the query engine and the compaction subprocess alike.

What changes on non-UTC hosts: naive timestamp literals are always UTC;
`date_trunc`, `time_bucket`, and `::DATE` casts on `TIMESTAMPTZ` bucket at UTC
boundaries, so continuous queries with daily or weekly buckets will align to UTC
midnight from this release onward. Zone-aware predicates remain available via
offset literals (`'2024-03-15 14:00:00+02:00'`) or `AT TIME ZONE`. Query JSON
output is unchanged (it was already normalized to UTC), and hosts already
running in UTC see no change at all.

### Daily-compacted files now register with tiering and migrate to cold ([#683](https://github.com/Basekick-Labs/arc/issues/683))

The tiering scanner only accepted hour-level paths, while the migrator only
moves daily-compacted `*_daily.parquet` files, which daily compaction writes
at day level. On deployments relying on the scan for registration, scheduled
hot-to-cold migration could therefore never find a candidate. The scanner now
registers day-level files (partition time = start of day), and it no longer
re-registers a file as hot when its metadata row already says cold, so a
failed post-migration cleanup stays visible to orphan reconciliation instead
of being re-uploaded every cycle.

### Spoke-namespace files now register with tiering ([#686](https://github.com/Basekick-Labs/arc/pull/686) follow-up)

On an edge-sync hub, spoke data lives one path level deeper than the standard
layout, and the tiering scanner errored on every spoke file. The scanner now
parses partition paths by their date tail (the same approach hub compaction
adopted in #619) and registers spoke files under the query-visible naming
(database = spoke ID). Spoke files are deliberately excluded from hot-to-cold
migration for now: legacy spoke-synced daily files carry sync receipts that a
migration delete would forget, re-introducing upload duplicates; cold
migration of spoke data ships separately with receipt-aware handling. Tiered
queries touching spoke namespaces stay correct and unpruned: a tier may only
be dropped from a query on the strength of a positive pruning result from
another tier.

### Spoke-namespace files now migrate to cold storage ([#687](https://github.com/Basekick-Labs/arc/issues/687))

On an edge-sync hub, spoke daily-compacted files now participate in
hot-to-cold migration. Before a hot copy is removed (by migration or by
orphan reconciliation), its sync receipt is marked the same way hub
compaction marks consumed inputs, so a spoke re-offering the file gets
"already present" instead of re-uploading a duplicate next to the cold copy.
The marking runs before tier metadata flips and before any delete, and a
marking failure aborts the migration batch, so a sync-index outage can never
strand an unmarked deletion. On deployments without an edge-sync hub, spoke
files (if any exist) remain registered but excluded from migration.

### Config-only restores now report `restart_required` (follow-up to [#678](https://github.com/Basekick-Labs/arc/pull/678))

`POST /api/v1/backup/restore` returned `restart_required: true` only when
metadata (SQLite databases) was restored. A config-only restore needs the same
restart, the restored `arc.toml` only takes effect on reload, which
`restoreConfig` already logs, but the response did not say so. The flag now
covers both restore kinds and a data-only restore still omits it; metadata
restores additionally report `staged: true`, since the restored databases are
applied at the next start rather than swapped live.

Contributed by [@atirna](https://github.com/atirna) in [#689](https://github.com/Basekick-Labs/arc/pull/689).

### Iceberg audit sweep completion, part 1 ([#639](https://github.com/Basekick-Labs/arc/issues/639) items 2, 5, 7, 8)

Four small hardenings from the Iceberg audit. A cluster node whose reconciler
is gated off (no compactor role) now warns once at first occurrence instead of
only logging at debug level every tick, so an export that silently never runs
becomes visible. `warehouseRelKey` resolves symlinks before comparing paths,
so a symlinked storage root no longer lands every commit in "hint
unaddressable" mode. Local warehouse metadata written by the embedded Iceberg
library is tightened to owner-only permissions after each commit (tighten
only; deliberately restrictive modes are never loosened). Restore staging
streams from backup storage instead of buffering entire databases in memory.

### Dropping a database now clears its Iceberg catalog artifacts ([#639](https://github.com/Basekick-Labs/arc/issues/639) item 3)

`DELETE /api/v1/databases/{name}` previously deleted the data files but left
the database's Iceberg namespace, table rows, and warehouse metadata behind
forever. The delete now also drops each catalog table, the namespace, and the
warehouse metadata directory, after the files are removed, so a racing
reconcile pass can only empty tables rather than recreate them. Cleanup
failures are logged and re-running the DELETE retries them, including when the
files are already gone.
