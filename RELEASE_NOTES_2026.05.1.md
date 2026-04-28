# Arc v2026.05.1 Release Notes

## Deployment

### Arc Enterprise Helm Chart

A production-ready Helm chart for Arc Enterprise ships in `helm/arc-enterprise/`. It covers both documented deployment patterns — shared object storage (bundled MinIO or bring-your-own S3/Azure) and local storage with peer replication — through a single `storage.mode` value.

Highlights:

- **Role-separated StatefulSets** for writer, reader, and compactor, with per-role scheduling (`writer.nodeSelector`, `writer.tolerations`, `writer.affinity`, and the same for reader and compactor), per-role resource sizing, and per-role PVCs sized for each deployment pattern.
- **Correct HA bootstrap**: only pod ordinal `-0` bootstraps Raft. The chart refuses `writer.replicas=2` (Raft split-brain hazard) — use 1 for dev or 3+ for HA.
- **Automatic failover wiring**: a single `cluster.failover.enabled` knob activates both writer and compactor failover in the binary.
- **Durable ingest by default**: writers ship with WAL enabled (`writer.wal.enabled=true`), sync mode configurable.
- **Peer-replication tuning exposed**: `cluster.replication.pullWorkers`, `cluster.replication.fetchTimeoutMs`, `cluster.replication.serveTimeoutMs`, and catch-up parameters are values, not opaque defaults.
- **Fail-fast install validation**: missing license key, shared secret, TLS secret (when TLS is enabled), MinIO credentials, or external-S3 credentials all produce a clear error at `helm install` time instead of a `CreateContainerConfigError` pod stuck later.
- **Secure defaults**: no default MinIO credentials (chart refuses to install without explicit values), reader `Service` defaults to `ClusterIP`, MinIO runs as non-root with the console disabled, bundled-MinIO pinned to a tagged release (not `:latest`), `seccompProfile: RuntimeDefault` and `allowPrivilegeEscalation: false` on Arc pods.
- **Initial admin token plumbing**: `auth.bootstrapToken.value` or `auth.bootstrapToken.existingSecret` pre-sets the admin token, removing the first-boot log-scraping step from deploy automation.
- **Cluster TLS**: when `cluster.tls.enabled=true`, the chart wires both the server cert/key and the CA certificate (for mutual TLS peer verification) from the referenced Kubernetes Secret.

Quick-start preset files land in the chart root: `values-shared-storage.yaml` and `values-local-storage.yaml`.

### Traefik-Based Docker Compose Examples

The two docker-compose examples under `deploy/docker-compose/` (shared storage) and `deploy/docker-compose-local/` (local storage with peer replication) have switched from nginx to **Traefik v3.6** using the Docker provider. Routing is now declared via container labels — writers, readers, and any-node endpoints are discovered automatically, so adding a reader or writer is a single compose edit with no separate routing config to maintain. The Traefik dashboard is exposed on `:8080` for dev visibility (and documented off-by-default guidance for production).

### Enterprise Deployment Patterns Documentation

The docs site now includes a **Deployment Patterns** page comparing the two supported cluster topologies side by side — shared object storage vs. local storage with peer replication — with sizing guidance, operational trade-offs, and the security posture for each. The clustering and overview pages cross-link to the new page, and the hero architecture diagram was refreshed.

See [Deployment Patterns](https://docs.basekick.net/arc-enterprise/deployment-patterns).

## Deprecations

### Authentication via `?p=` Query Parameter (InfluxDB 1.x Compatibility)

Authentication via the `?p=token` query parameter (InfluxDB 1.x compatibility) is now **deprecated**. Tokens passed in URLs are exposed in HTTP access logs from reverse proxies, load balancers, and web servers — creating a credential leak risk.

The `?p=` method continues to work but Arc now logs a one-time warning on first use. Migrate clients to the `Authorization: Bearer <token>` header instead.

## New Features

### Peer File Replication (Enterprise)

Arc Enterprise clusters now replicate Parquet files between nodes automatically. When a writer flushes a file, all other nodes pull the bytes from the origin peer (or any healthy peer that already has a copy) and write them to their own local storage. This enables bare-metal, VM, and edge deployments where each node has its own local SSDs and shared storage (S3, MinIO, Azure) is not available.

Every file is SHA-256 hashed at flush time and the hash is committed into a Raft-backed cluster manifest. Receivers verify the hash after download — checksum mismatches trigger automatic retry. When a node starts (or restarts), it walks the manifest and catches up on any files it missed, pulling from whichever healthy peer has them. The original writer doesn't need to be alive.

- Non-leader nodes forward manifest commands to the Raft leader automatically — no silent data loss if the writer isn't the Raft leader.
- `GET /api/v1/cluster/files` returns the full cluster manifest (supports `?database=` filter).
- Replication status and catch-up progress are surfaced via `/api/v1/cluster/status`.
- Zero overhead for OSS / standalone deployments.
- **Resumable transfers**: interrupted pulls resume from the last committed byte instead of restarting from zero. Especially valuable for large compacted Parquet outputs on slow or flaky links. On retry the puller checks how many bytes are already on disk, hashes the prefix to continue the SHA-256 chain, and requests only the remaining tail from the peer. Full-file hash verification is still enforced. S3 and Azure Blob backends fall back to a full re-fetch on resume (append is not supported by those APIs); local-SSD nodes get complete resume support.

**Configuration** (`ARC_CLUSTER_REPLICATION_*` env vars or `cluster.replication_*` in `arc.toml`):

| Setting | Default | Description |
|---------|---------|-------------|
| `replication_enabled` | `false` | Master switch; requires `shared_secret` |
| `replication_pull_workers` | `4` | Concurrent fetch workers per node |
| `replication_queue_size` | `1024` | Bounded queue; excess entries reconciled on restart |
| `replication_fetch_timeout_ms` | `60000` | Per-fetch timeout (puller side) |
| `replication_serve_timeout_ms` | `120000` | Body-stream timeout (origin side) |
| `replication_retry_max_attempts` | `3` | Retry attempts before dropping an entry |
| `replication_catchup_enabled` | `true` | Startup catch-up; disable for emergency |
| `replication_catchup_barrier_timeout_ms` | `10000` | Raft barrier timeout before walking |

### Dedicated Compactor Role with Automatic Failover (Enterprise)

In clustered deployments, compaction now runs on exactly one node to prevent duplicate outputs on shared storage and eliminate redundant CPU/IO work across nodes.

- Set `ARC_CLUSTER_ROLE=compactor` on one node. Writers and readers automatically gate their compaction schedulers. OSS deployments are unaffected — compaction runs unconditionally.
- Compacted outputs are registered in the Raft manifest and replicated to all peers automatically.
- **Automatic failover**: when `cluster.failover_enabled=true`, the Raft leader monitors the active compactor and reassigns it to another healthy node after ~30s of unresponsiveness. No restart required. Prefers compactor-role nodes, falls back to writers. 60s cooldown prevents rapid cycling.
- Health warnings surface when no compactor is elected or when multiple compactors are configured (misconfiguration that re-introduces duplicate outputs).

**Compaction configuration** (`ARC_COMPACTION_*` env vars):

| Setting | Default | Description |
|---------|---------|-------------|
| `completion_watcher_interval_ms` | `1000` | How often the watcher scans for pending manifests |
| `completion_dir` | auto-derived | Local directory for subprocess → parent handoff |
| `completion_orphan_timeout_ms` | `600000` | Sweep stale manifests from crashed subprocesses |

### Cluster Health Detection via Heartbeats (Enterprise)

Cluster nodes now exchange periodic heartbeat messages over the coordinator protocol. The health checker uses heartbeat freshness to detect unresponsive peers — nodes that miss 3 consecutive heartbeats (~15s) are marked unhealthy, enabling automatic failover for both writers and compactors. Self-reported node state is propagated alongside heartbeats so peers can detect degraded nodes that are still reachable but unable to serve.

### Cluster TLS Encryption and Shared Secret Authentication (Enterprise)

Arc Enterprise clustering now supports encrypted inter-node communication and authenticated cluster joins — bringing production-grade security to multi-node deployments.

- **Shared secret authentication** (`cluster.shared_secret`): When configured, join requests include an HMAC-SHA256 signature over a random nonce, node ID, cluster name, and timestamp. The leader validates the signature and rejects unauthorized joins. Timestamps are checked within a 5-minute tolerance to prevent replay attacks.
- **TLS encryption** (`cluster.tls_enabled` + cert/key files): All inter-node TCP connections — coordinator, WAL replication, shard replication, and Raft consensus — are wrapped in TLS. Raft transport uses a custom `TLSStreamLayer` implementing `raft.StreamLayer`. Optional CA certificate (`cluster.tls_ca_file`) enables mutual TLS for peer certificate verification.
- Both features are opt-in and backward compatible. When disabled, behavior is unchanged.

**Configuration example:**
```yaml
cluster:
  shared_secret: "my-cluster-secret"
  tls_enabled: true
  tls_cert_file: "/etc/arc/cluster-cert.pem"
  tls_key_file: "/etc/arc/cluster-key.pem"
```

### Kubernetes-Ready Cluster Node Identity (Enterprise)

Cluster node identity is now stable across pod reschedules. When `cluster.node_id` is not explicitly set, Arc uses the OS hostname as the node ID — in Kubernetes StatefulSets, this is the pod name (e.g., `arc-writer-0`), which survives reschedules. A restarted pod rejoins the cluster with the same identity instead of registering as a new node and leaving a dead Raft voter behind.

Additionally, cluster nodes now broadcast a `LeaveNotify` message to all peers during graceful shutdown. Peers immediately remove the departing node from Raft and the registry, rather than waiting for the heartbeat timeout to detect the departure. This makes rolling updates and scale-down operations clean and predictable.

### Reader Query Freshness via WAL Replication (Enterprise)

Reader nodes now apply replicated WAL entries to their local ArrowBuffer, enabling near-real-time query freshness. Previously, readers received WAL entries from the writer but only persisted them to their local WAL — the data was invisible to queries until flushed to Parquet. Now, replicated entries are decoded (both columnar and row formats) and written directly to the reader's in-memory buffer, making unflushed writer data queryable on readers.

This is the foundation for zero-latency reads across clustered deployments — ingested data is queryable on readers within milliseconds of arriving at the writer.

### Dead Node Removal API (Enterprise)

New admin endpoint `DELETE /api/v1/cluster/nodes/:id` to remove a dead or permanently scaled-down node from the cluster. This removes the node from both the Raft voting configuration and the cluster FSM state, preventing dead voters from accumulating and eventually breaking quorum.

```bash
curl -X DELETE http://localhost:8000/api/v1/cluster/nodes/arc-writer-2 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

Self-removal is prevented — use graceful shutdown (`LeaveNotify`) instead.

### RBAC Cache Lifecycle Management (Enterprise)

RBACManager now has proper lifecycle management and bounded memory usage:

- Added `Close()` method with graceful shutdown of the background cache cleanup goroutine. RBACManager is registered with the shutdown coordinator.
- Permission and token caches are now bounded with a configurable `MaxCacheSize` (default 10,000 entries per cache). When exceeded, a random entry is evicted on insertion, working alongside the existing TTL cleanup.

### Batched Raft Commands for Compaction Manifests (Enterprise)

The CompletionWatcher now applies all RegisterFile and DeleteFile operations for a single compaction manifest in **one Raft log entry** (`CommandBatchFileOps`) instead of one entry per file. For a typical manifest with 20 outputs and 20 deleted sources this reduces the Raft round-trips from 40 to 1, cutting manifest apply latency from ~200ms to ~5ms. The two-phase write-then-delete ordering invariant is preserved, and the batch command is fully idempotent (replaying it on restart is a no-op).

### Manifest-vs-Storage Reconciliation (Enterprise)

A periodic reconciler now detects and repairs drift between the Raft-replicated cluster file manifest and physical storage. Two kinds of drift are addressed:

- **Orphan manifest entries** — manifest references a file path that no longer exists in storage. Caused by retention/compaction/delete succeeding storage-side then losing Raft quorum before the manifest update commits. Today these entries waste FSM memory and can cause `ErrFileNotOnPeer` errors on peer-replication catch-up.
- **Orphan storage files** — file exists in storage but no manifest entry references it. Caused by a crash between `storage.Write` and the file-registrar Raft propose, or by files predating the Phase 1 manifest.

The reconciler ships **off by default** AND **report-only on first run**. Once enabled (`reconciliation.enabled=true`), the cron runs daily at 04:17 producing dry-run audit reports. After reviewing the reports operators flip `manifest_only_dry_run=false` to allow real deletes. Pre-manifest cleanup (files outside the standard Arc layout, e.g. shared-bucket strays or pre-Phase-1 history) requires a separate explicit opt-in via `delete_pre_manifest_orphans=true`. Steady-state behavior matches Druid's coordinator kill task and Pinot's retention manager: opt-in, conservative grace window, blast cap.

- **Cluster gating**: on shared storage (S3, Azure, MinIO) the reconciler runs on the active compactor only — reusing the failover-managed lease as a single-sweeper election. On local storage every node walks its own backend filtered by `OriginNodeID == self`. In standalone Arc the orphan-storage sweep refuses to act (no manifest to reconcile against).
- **Manifest-before-storage invariant**: orphan-manifest sweep runs first; orphan-storage sweep runs only if the first half completes cleanly. Raft quorum loss aborts the run before any storage bytes are touched. Manifest-write errors are classified via a typed sentinel (`raft.ErrManifestApply`) instead of brittle log-string matching, so audit dashboards report `raft_quorum_loss` / `lease_lost` / `unknown` distinctly.
- **Re-checks before every delete** catch concurrent writer/retention/compaction races. A file that re-appears between snapshot and apply is skipped, never deleted. Re-checks run in parallel (default 8 workers, configurable) so HEAD-per-file on remote backends doesn't bottleneck large sweeps.
- **Bounded blast radius**: per-run cap, 24-hour grace window, manifest-size ceiling (200,000 entries default), per-prefix list timeout, and a root-walk fan-out cap (1,000 unknown databases per tick) all default to conservative values. The strict `delete_pre_manifest_orphans=false` mode requires the full 7-segment Arc layout (`db/m/yyyy/mm/dd/hh/file`) and rejects `.`/`..` segments to keep stray files in shared buckets untouched.
- **Conservative on un-mtime'd backends**: when a backend doesn't expose `LastModified` via the `ObjectLister` interface (custom or future implementations), files are treated as YOUNG (protected) rather than eligible — a deliberate trade-off favoring leaked orphans over data loss. A single Warn per run signals the safety net engaged.
- **Operator visibility**: every run produces an audit trail (`reconcile.run_started/completed/aborted/manifest_batch_deleted/storage_batch_deleted/cap_hit`). The most recent 10 runs are queryable via `GET /api/v1/reconciliation/status` (auth-required, `RequireRead`). Run summaries include a `walk_partial` flag and per-prefix errors so a "0 orphans found" run that was actually blind to most of the bucket is unambiguous. Manual `POST /api/v1/reconciliation/trigger` (`RequireAdmin`) defaults to dry-run; `dry_run=false&act=true` is required to act.

**Configuration** (`ARC_RECONCILIATION_*` env vars or `reconciliation.*` in `arc.toml`):

| Setting | Default | Description |
|---------|---------|-------------|
| `enabled` | `false` | Master switch — explicit operator opt-in |
| `schedule` | `17 4 * * *` | Cron expression (daily 04:17) |
| `grace_window_seconds` | `86400` | Orphan storage files younger than this are NEVER deleted |
| `clock_skew_allowance_seconds` | `300` | Added to grace window |
| `max_deletes_per_run` | `10000` | Per-run blast cap (manifest + storage combined) |
| `max_manifest_size` | `200000` | Aborts the run if the FSM is larger |
| `max_root_walk_databases` | `1000` | Cap on unknown databases the root-walk fallback descends into per tick (0 disables) |
| `recheck_concurrency` | `8` | Worker count for parallel `storage.Exists` re-checks during the manifest sweep (1 forces sequential) |
| `manifest_only_dry_run` | `true` | **Secure-by-default**: every cron run is dry-run until the operator flips this to `false` after reviewing dry-run audits. Mirrors the safe-first-run posture of Druid's coordinator kill task and Iceberg's `remove_orphan_files`. |
| `delete_pre_manifest_orphans` | `false` | **Secure-by-default**: orphan storage files outside the standard 7-segment Arc layout (`database/measurement/yyyy/mm/dd/hh/file.parquet`, with `.`/`..` segments rejected) are NOT eligible for deletion. Operators on shared buckets where unrelated stray files must be left alone keep this off. Operators who deliberately want pre-Phase-1 / migration-residual cleanup explicitly opt in by setting this to `true`. |

## Hardening

### Ingestion Pipeline Hardening and Performance

A focused audit and optimization pass across all three ingestion paths (MessagePack columnar, MessagePack row, and Line Protocol) produced the following improvements:

**Reliability**

- **Multi-hour flush atomicity**: When a write spans multiple hour buckets, tiering metadata and the Raft cluster manifest are now updated only after all Parquet files have been successfully written to storage. Previously, the manifest could be updated for completed hours even if a later hour's write failed, requiring manual reconciliation. The fix uses a collect-then-register pattern — all hour buckets write first, then all registrations proceed.
- **WAL error observability**: WAL write failures (disk full, permission errors) were already logged at ERROR level but were not counted. A new `total_wal_errors` counter is now exposed in the stats endpoint so operators can alert on WAL degradation without grepping logs.
- **Line protocol timestamp validation**: The parser now enforces upper and lower bounds on raw timestamp values before applying precision multipliers (`ms`, `s`). Values that would overflow `int64` after multiplication are replaced with the server's current time, preventing silently mis-timestamped records in edge cases. Pre-epoch (negative) timestamps remain valid and pass through unchanged.
- **Validity bitmap contract honored across sort, slice, and merge**: `TypedColumnBatch` documents that a `nil` validity entry for a column means "all values valid". Three paths that reorder or combine batches (`sortTypedColumnBatchByKeys`, `sliceTypedColumnBatchByIndices`, `mergeBatches`) now preserve this semantic — previously a `nil` entry could be silently converted to all-null, or cause an index-out-of-range panic if an external caller provided one. All internal producers already avoid the pattern, so no live workload was affected, but the code is now robust against third-party batch construction.

**Performance**

- **Column signature caching and type-aware detection**: Schema change detection previously recomputed a sorted, joined column name string on every write. The signature is now computed once when the typed column batch is constructed and cached as a field, so hot-path schema checks are a field read with zero allocation. The signature also encodes each column's Go type (`i64`, `f64`, `str`, `bool`, `dec`), so a type change on the same column name (e.g. `int64`→`float64`) is now detected as schema evolution and triggers a flush before the new-schema data is appended.
- **Pre-built Parquet writer properties**: `parquet.WriterProperties` and `pqarrow.ArrowWriterProperties` were reconstructed on every Parquet flush. These are stateless configuration objects that cannot change after startup and are now built once in `NewArrowWriter` and reused across all flushes.
- **Sort permutation reuse**: `sortTypedColumnBatchByKeys` previously sorted the main column data and then sorted again to reorder validity bitmaps. It now computes the permutation index once from the first sort pass and applies it directly to validity bitmaps, eliminating the duplicate sort.
- **`mergeBatches` value types**: The `colInfo` structs in `mergeBatches` were heap-allocated as pointers. Changed to value semantics (`map[string]colInfo`), removing per-column heap allocation during batch merges.

### Ingestion Critical-Path Hardening (26.05.1 Pre-GA)

A second-pass post-implementation review of the ingestion path (MessagePack, Line Protocol, TLE, bulk import, plus the underlying ArrowBuffer and WAL) surfaced five critical issues. All are fixed in this release; sustained-load benchmarks show **p99 latency improved ~17% (3.68ms → 3.13ms)** and throughput uplifted to ~19M records/sec on MessagePack columnar with 0% errors over 60-second runs.

**Concurrency**
- **Graceful-shutdown panic eliminated**: `ArrowBuffer.Close()` previously closed the internal flush queue *after* cancelling the buffer context, creating a narrow race where a writer goroutine past the shard mutex but not yet at the channel send would panic with "send on closed channel". The channel is no longer closed; workers exit on context cancellation, and senders short-circuit on a `closing` atomic flag set before the cancel. Regression test runs concurrent writers across the Close call to lock the contract in.
- **Schema-evolution corruption under concurrent writes eliminated**: When two writers raced through a schema change against the same `(database, measurement)`, one writer could append records into a buffer the other had just re-keyed with a third schema, producing column mismatches at flush. The schema-change path now uses a bounded loop (8 iterations) that re-checks the schema entry after every flush completes under a re-acquired lock, with `ctx.Err()` checked per iteration so a cancelled request is never starved by a rotating-schema racer. Hitting the cap (sustained adversarial schema-rotation churn against the same buffer) now returns a typed `ingest.ErrSchemaChurnExceeded` sentinel rather than silently committing a wide schema-mixed buffer to disk; LP and TLE write handlers map the error to HTTP 503 so upstream senders back off, and the new `total_schema_churn_exceeded` counter in `Stats()` lets operators alert on a non-zero rate.

**Durability and observability**
- **WAL backpressure no longer silently masquerades as durability**: When the WAL's async entry channel was full, `AppendRaw`/`AppendRawWithMeta` previously returned `nil` while incrementing a hidden counter. Downstream code logged "data preserved in WAL for recovery" — which was untrue for the dropped entries. WAL writes now return a typed `wal.ErrWALDropped` sentinel on backpressure; ingest callers differentiate it from real I/O errors via `errors.Is`, increment a separate `total_wal_dropped` counter, and emit a sampled (max 1/sec) Warn instead of an unsampled per-record Error. Operators can now alert on backpressure as a distinct degraded state.
- **Cluster-replication receivers tolerate WAL backpressure**: The replication and shard receivers previously treated `LocalWAL.AppendRaw` errors as fatal — propagating to the receive loop, advancing `lastSeq`, and silently diverging the follower from the primary. The receivers now treat `wal.ErrWALDropped` as non-fatal: the entry is still applied to the in-memory ingest buffer (which has its own WAL path), a `total_local_wal_dropped` counter is incremented, and durability is closed by the primary's WAL plus Phase 2 peer Parquet replication.

**Security**
- **Gzip decompression bomb in line-protocol and TLE handlers fixed**: Fiber's transparent gunzip on `Content-Encoding: gzip` had no decompressed-size cap — a 1 MB gzip payload that decompressed to multiple gigabytes would OOM the process before any handler-level check fired. Both handlers now read the raw request body, detect gzip by magic bytes, and decompress through a pooled limited reader with a hard cap. Mirrors the pattern the MessagePack handler already used. Decompression-failure responses now include the underlying parser error so clients can diagnose oversized payloads without checking server logs.
- **Defensive body copy in line-protocol and TLE handlers**: After switching to `c.Request().Body()` (raw fasthttp pointer) to bypass uncapped transparent gunzip, the no-compression branch handed a fasthttp-owned slice into the parser. The current LP and TLE parsers copy strings out of the byte slice before any async handoff, so today's behavior is safe — but documenting that "no future change may retain a sub-slice of the request body" is brittle, and the gemini code-review surfaced this as a silent-aliasing footgun. The defensive copy now lives in the shared `decompressRequest` helper's uncompressed branch — gzip/zstd already return fresh slices, so memory is doubled only when needed. The cost is one memcpy per uncompressed request, dwarfed by per-record allocations.
- **Zstd decompression-bomb vulnerability fixed**: The msgpack zstd path (and the new line-protocol/TLE zstd path) used `zstd.Decoder.DecodeAll`, which grows its output buffer to fit the entire decompressed stream regardless of `WithDecoderMaxMemory` (that option only bounds the decoder's per-frame window, not the output buffer). A high-ratio zstd bomb (e.g. 28 KB compressed → 256 MB decompressed, ratio 9000×) would have OOM'd the process before any post-hoc length check could fire — symmetric to the gzip-bomb fix earlier in this release. All three handlers now use streaming `decoder.Reset` + `io.LimitReader` + `io.ReadAll` with the same hard 100 MB output cap as gzip; the bound is enforced *during* decoding so the bomb is rejected with the bounded `len(maxSize)+1` allocation. Regression tests construct a 256 MB zstd bomb and assert clean rejection.
- **Write endpoints now require write-tier auth**: Five ingestion endpoints (`/api/v1/write/msgpack`, `/write`, `/api/v2/write`, `/api/v1/write/line-protocol`, `/api/v1/write/tle`) and four bulk-import endpoints (`/api/v1/import/csv`, `/api/v1/import/parquet`, `/api/v1/import/lp`, `/api/v1/import/tle`) lacked explicit write-tier auth middleware. A token issued with read-only permissions could write data when RBAC was disabled (the OSS default). All write endpoints now use `auth.RequireWrite`. Import endpoints, which can rewrite history, and the line-protocol global flush endpoint use the stricter `auth.RequireAdmin`. The auth manager is now passed to ingest handlers as the concrete `*auth.AuthManager`, replacing a local interface that could silently fail a type assertion and disable protection.

### Line Protocol and TLE: zstd Decompression Support

The Line Protocol (`/api/v1/write/line-protocol`, `/write`, `/api/v2/write`) and TLE (`/api/v1/write/tle`) endpoints now accept `Content-Encoding: zstd` in addition to `gzip`. Compression is detected by magic bytes (`0x28 0xB5 0x2F 0xFD`), decompressed through a pooled `zstd.Decoder` shared with the MessagePack handler, and capped at the same 100 MB decompressed-size limit as the gzip path. Zstd decompresses 3–5× faster than gzip on typical line-protocol and TLE payloads, which matters most for satellite-fleet TLE senders that push large catalog refreshes on each tick. No client-side change is required; existing gzip clients continue to work unchanged.

### Directory Permissions

Directories containing sensitive data (auth database, continuous query definitions, retention policies, Raft consensus state, telemetry, and import output) now use 0700 (owner-only) permissions, consistent with WAL and local storage directories.

**Note:** `os.MkdirAll` does not change permissions on existing directories, so existing deployments retain their current permissions. Operators upgrading from earlier versions should manually run `chmod 700` on their data directory if desired.

### SQL Escaping in Compaction

Applied defense-in-depth SQL escaping to DuckDB `SET memory_limit` (single-quote escaping) and compaction `ORDER BY` sort key names (double-quote identifier escaping). Both already had config-level validation, but the runtime escaping provides an additional safety layer.

### Cluster-Safe Retention Policy Execution (Enterprise)

In clustered deployments, retention policies now run exclusively on the primary writer node and propagate file deletions through the Raft manifest to all peers.

- **Writer-only execution**: The retention scheduler checks a cluster gate before starting. Only the primary writer node runs — reader and compactor nodes stay idle and log a clear message. On writer failover, the newly promoted primary picks up on the next cron tick. Standalone deployments are unaffected.
- **Cluster manifest updates**: After each file is deleted from storage, the retention handler commits the deletion into the Raft log via `DeleteFileFromManifest`. This keeps the manifest consistent and prevents orphaned entries from interfering with peer replication catch-up.
- **Reader node cleanup (local storage)**: The `onFileDeleted` FSM callback and delete-worker pool (shared with compaction) handle retention-triggered deletes on reader nodes, removing their local copy of the file and preventing unbounded disk growth on per-node storage deployments.

### Cluster-Safe Continuous Queries (Enterprise)

The CQ scheduler now gates execution on the primary writer role. In a cluster, only the primary writer executes scheduled continuous queries — reader nodes skip each tick and log a `DEBUG` message. Role transitions (failover, demotion) take effect on the next tick without a restart.

Previously, all nodes ran the CQ scheduler independently, causing duplicate records to be written to destination measurements and `last_processed_time` to diverge across nodes.

### Cluster-Safe DELETE Endpoint (Enterprise)

The `POST /api/v1/delete` endpoint is now cluster-aware. Reader nodes reject delete requests with `503 Service Unavailable` — only the primary writer may execute mutations.

- **Writer-only gate**: Checked before any storage scan, so reader nodes are rejected immediately without running expensive DuckDB queries.
- **Manifest-before-storage for full-file deletes**: When a DELETE removes an entire file (all rows match the WHERE clause), the path is committed to the Raft manifest before `storage.Delete`. All peer nodes learn about the removal via the `onFileDeleted` FSM hook and clean up their local copies. Partial rewrites (file overwritten in-place, path unchanged) require no manifest entry.
- **Path traversal hardening**: `database` and `measurement` inputs are now validated to reject `..`, `/`, and `\` characters.

## Bug Fixes

### Row-Format MessagePack Flush Hardening

Issue #401 reported that row-format MessagePack writes could be silently dropped at flush time with `no time data in batch`. The failure could not be reproduced end-to-end on current builds, but Arc now has explicit regression coverage for the affected path and better visibility if a future flush failure occurs.

**Changes:**

- Added end-to-end regression tests for row-format MessagePack writes covering direct row ingestion, decoder-driven ingestion, and age-based flush.
- Added a dedicated Prometheus counter, `arc_buffer_flush_failures_total`, to surface flush failures that are preserved in WAL for recovery.
- Exposed the new flush failure counter through the JSON metrics endpoints and internal time-series collector.

### RBACManager Goroutine Leak — No Close() Method

The RBACManager background cache cleanup goroutine (`cacheCleanupLoop`) ran in an infinite loop with no shutdown mechanism, leaking a goroutine on every Arc restart.

**Fix:** Added a `done` channel and `Close()` method following the same pattern used by AuthManager. RBACManager is now registered with the shutdown coordinator at `PriorityAuth`.

### RBAC Permission Caches Grow Unbounded

Both the token cache and permission cache in RBACManager had no maximum size — only TTL-based expiration. Under high cardinality (many tokens × databases × measurements), the caches could grow indefinitely.

**Fix:** Added a configurable `MaxCacheSize` (default 10,000 entries per cache). When a cache exceeds its limit, a random entry is evicted on insertion. Combined with the existing TTL cleanup, this bounds memory usage while maintaining cache effectiveness.

### WAL Filename Rotation Collision

Fixed a bug where WAL filenames used second precision (`arc-YYYYMMDD_HHMMSS.wal`), allowing two rotations within the same second to produce identical filenames. The second rotation would reopen the existing file via `O_APPEND` and write a new header, corrupting the WAL structure.

**Fix:** WAL filenames now use nanosecond precision (`arc-YYYYMMDD_HHMMSS.000000000.wal`), eliminating the collision window. The new format remains lexicographically sortable and compatible with all existing `*.wal` glob patterns.

### Query Registry Reports 0 Row Count for Arrow-Path Queries

Fixed a bug where the query registry always recorded `row_count = 0` for queries served via the Arrow path.

**Root cause:** `queryRegistry.Complete(queryID, 0)` was called synchronously right after `arrowJSONQueryFunc` returned, but the Arrow path streams its response asynchronously via `SetBodyStreamWriter`. At that point the real row count from `streamArrowJSON` is not yet known — so 0 was always hardcoded. Additionally, the Arrow path's error and timeout branches returned `handled=true` without notifying the registry at all, leaving queries stuck in "running" state permanently.

**Fix:** Added `onComplete func(int)`, `onFail func(string)`, and `onTimeout func()` callbacks to `executeArrowJSONQuery`. The call site in `query.go` passes closures that invoke the appropriate registry method. The success path calls `onComplete(rc)` inside the `SetBodyStreamWriter` callback with the real row count; the error and timeout paths call `onFail`/`onTimeout` immediately before returning.

**Impact:** The query registry now correctly reflects row counts, failure reasons, and timeout status for all Arrow-path queries.

### Low-Volume Measurements Starved of Age-Based Flushes Under High Write Load

Fixed a bug in the ArrowBuffer periodic flush goroutine where low-volume measurements could be indefinitely starved of age-based flushing when a high-volume workload was running concurrently.

**Root cause:** `periodicFlush` unconditionally reset its self-adjusting timer every time a new buffer was created (signalled via `newBufferCh`). Under high write load, high-volume measurements hit their size limit frequently, triggering size-based flushes followed by immediate buffer re-creation — each re-creation sent a `newBufferCh` signal. This caused the timer to be continuously reset to `now + maxAge`, pushing the flush deadline out and preventing low-volume buffers (which rely solely on age-based flushing) from ever being flushed within the expected window.

**Fix:** The timer is now only reset when the new computed deadline is earlier than the already-scheduled deadline. If a new buffer's expiry is further in the future than the current timer, the timer is left unchanged. This guarantees that an existing pending flush deadline is never extended by a newer, shorter-lived buffer.

**Impact:** Low-volume measurements (e.g. alerts, heartbeats, status events) now reliably flush within `max_buffer_age_ms` even when co-located with high-throughput measurements on the same Arc instance.

### Memory Not Released After Delete / Retention Execution

Fixed a memory retention issue where running a retention policy or the delete API endpoint caused Arc to hold onto several GBs of memory that were never released — requiring periodic container restarts to recover.

**Root cause:** DuckDB's internal parquet metadata cache and data block cache were populated during `read_parquet()` queries executed by delete and retention operations, but never cleared after completion. Compaction already performed this cleanup; delete and retention did not.

**Changes:**

- Both the delete handler and retention handler now call `ClearHTTPCache()` after completing their file operations — always, including dry runs and no-match paths, since `read_parquet()` populates the cache regardless of whether rows are actually deleted. This evicts DuckDB's glob result cache, file metadata cache, and data block cache for the files that were rewritten or deleted.
- `debug.FreeOSMemory()` is debounced via atomic CAS and fires at most once every 30 seconds in a background goroutine. This prevents GC storms when multiple concurrent delete or retention requests complete in rapid succession, while still returning freed heap pages to the OS promptly.
- The delete file-rewrite COPY queries now include `ROW_GROUP_SIZE 122880` (extracted as a named constant `parquetRowGroupSize`), matching the compaction writer. This limits DuckDB's internal write buffer per row group, reducing peak memory during large rewrites.

**Impact:** Memory usage should return to baseline after a retention policy run or delete operation, instead of climbing GBs per execution. Particularly noticeable in Docker/Kubernetes deployments with memory limits, where this could cause OOM kills during nightly retention runs.

### Writer-Only Schedulers Skipped All Ticks Without Failover Enabled (Enterprise)

Fixed a bug where the CQ and retention schedulers skipped execution on every tick when `cluster.failover_enabled=false` — which is the default and the typical single-writer cluster configuration.

**Root cause:** `IsPrimaryWriter()` on a cluster node returns `true` only when `WriterState == WriterStatePrimary`. That state is set exclusively via the writer failover manager's `CommandPromoteWriter` Raft entry. When failover is disabled, no promotion command is ever issued, so `WriterState` remains at its zero value and `IsPrimaryWriter()` always returns `false` — even on writer nodes. Both the retention and CQ gate adapters in `main.go` (`retentionClusterGate`, `cqClusterGate`) called `node.IsPrimaryWriter()` directly, bypassing the coordinator entirely.

**Fix:**
- `Coordinator.IsPrimaryWriter()` now falls back to a role check (`node.Role == RoleWriter`) when no failover manager is configured. With failover enabled, the existing `WriterState == WriterStatePrimary` semantics are preserved.
- `retentionClusterGate` and `cqClusterGate` now delegate to `coordinator.IsPrimaryWriter()` instead of calling `node.IsPrimaryWriter()` directly, so the fallback is respected.

**Impact:** Retention policies and continuous query schedules now execute correctly on writer nodes in clusters running without automatic failover. Previously both would silently skip every scheduled tick, meaning retention was never applied and CQs never ran in the default cluster configuration.

### Continuous Query Not Scheduled After API Creation

Fixed a bug where a CQ created via `POST /api/v1/continuous_queries` was not picked up by the scheduler until the node was restarted.

**Root cause:** `handleCreate` inserted the CQ into SQLite but did not notify the scheduler. The scheduler's `ReloadCQ` was only called from `handleUpdate`.

**Fix:** `handleCreate` now calls `scheduler.StartJobDirect(queryID, name, interval, isActive)` after a successful insert, passing the data already in hand to avoid a redundant SQLite re-read. If the scheduler is not running (no license, or standalone without license), the call is a no-op.

## Dependencies

### DuckDB v1.5.1

Upgraded DuckDB Go binding from `v2.5.5` (DuckDB 1.4.4) to `v2.10501.0` (DuckDB 1.5.1). All platform-specific binary packages (`duckdb-go-bindings`, `lib/darwin-arm64`, `lib/linux-amd64`, `lib/linux-arm64`, `lib/windows-amd64`) updated in lockstep.

### AWS SDK v2 (S3/S3-compatible storage)

Bumped `aws-sdk-go-v2` core (1.40→1.41.5), `service/s3` (1.92→1.99), `aws/protocol/eventstream` (1.7.3→1.7.8), and all internal sub-packages. Key fixes:

- DNS timeout errors are now retried automatically, improving S3 reliability on flaky networks
- Fix for config load failure when a non-existent AWS profile is configured
- `smithy-go` 1.23→1.24 protocol layer update
