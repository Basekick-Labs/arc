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

## Hardening

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

**Fix:** `handleCreate` now calls `scheduler.ReloadCQ(queryID)` after a successful insert. If the scheduler is not running (no license, or standalone without license), the call is a no-op.

## Dependencies

### DuckDB v1.5.1

Upgraded DuckDB Go binding from `v2.5.5` (DuckDB 1.4.4) to `v2.10501.0` (DuckDB 1.5.1). All platform-specific binary packages (`duckdb-go-bindings`, `lib/darwin-arm64`, `lib/linux-amd64`, `lib/linux-arm64`, `lib/windows-amd64`) updated in lockstep.

### AWS SDK v2 (S3/S3-compatible storage)

Bumped `aws-sdk-go-v2` core (1.40→1.41.5), `service/s3` (1.92→1.99), `aws/protocol/eventstream` (1.7.3→1.7.8), and all internal sub-packages. Key fixes:

- DNS timeout errors are now retried automatically, improving S3 reliability on flaky networks
- Fix for config load failure when a non-existent AWS profile is configured
- `smithy-go` 1.23→1.24 protocol layer update
