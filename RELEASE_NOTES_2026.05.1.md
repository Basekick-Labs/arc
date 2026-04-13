# Arc v2026.05.1 Release Notes

## Deprecations

### Authentication via `?p=` Query Parameter (InfluxDB 1.x Compatibility)

Authentication via the `?p=token` query parameter (InfluxDB 1.x compatibility) is now **deprecated**. Tokens passed in URLs are exposed in HTTP access logs from reverse proxies, load balancers, and web servers — creating a credential leak risk.

The `?p=` method continues to work but Arc now logs a one-time warning on first use. Migrate clients to the `Authorization: Bearer <token>` header instead.

## New Features

### Cluster File Manifest (Enterprise — Foundation for Peer Replication)

Arc Enterprise now maintains a cluster-wide file manifest in the Raft FSM. Every Parquet file flushed by a writer (or produced by compaction) is announced to the cluster via a new Raft command, producing an authoritative view of all files known to the cluster. This is the foundation for peer-to-peer file replication in bare-metal and VM deployments where nodes have their own local storage.

**What this delivers today:**
- New Raft commands `CommandRegisterFile` and `CommandDeleteFile` added to `ClusterFSM`
- File manifest persisted in Raft snapshots and replicated to all cluster members via standard Raft consensus
- New API endpoint `GET /api/v1/cluster/files` returns the complete cluster file manifest (supports `?database=<name>` filter)
- Async, non-blocking registration from the flush path — the file registrar enqueues entries and a background worker appends to Raft, so write latency is unaffected
- Zero overhead for OSS / standalone deployments — no coordinator, no registrar, no manifest

### Peer File Replication (Enterprise — Phase 2)

Building on the cluster file manifest, Arc Enterprise now replicates **actual Parquet bytes** between cluster nodes. When a writer flushes a file, readers automatically pull the bytes from the origin peer over the existing coordinator TCP protocol and write them to their own local storage backend. This unlocks bare-metal, VM, and edge deployments where each node has its own local SSDs and shared storage (S3, MinIO, Azure) is not available — on-prem, aerospace, defense, and edge use cases.

**How it works:**
- Every flushed Parquet file is SHA-256 hashed on the writer (on the in-memory buffer, before writing to the backend) and the hash is committed into the Raft manifest alongside the file metadata. Every node in the cluster learns the authoritative hash as soon as the Raft log replicates.
- On each non-origin node, an `onFileRegistered` callback fires from the FSM when a new entry commits. A background `Puller` worker pool enqueues a fetch, skipping files that originated on the local node or already exist on the local backend.
- Workers dial the origin peer using the cluster's existing TLS-wrapped coordinator connection and send a new `MsgFetchFile` request, authenticated with an HMAC that binds `{nonce, nodeID, clusterName, path, timestamp}` — including the path prevents a stolen MAC from being replayed to fetch a different file within the freshness window.
- The origin validates the HMAC, sanitizes the path, confirms the file is in the cluster manifest (so peers cannot request arbitrary local files), and streams the body directly over the connection.
- The puller streams body bytes through an `io.MultiWriter` tee into a `sha256.New()` hasher while writing into the local backend via `WriteReader`, and compares the computed hash against the manifest hash before accepting the file. On mismatch the partial file is deleted and the fetch retries.

**Configuration** (defaults shown, set via `ARC_CLUSTER_REPLICATION_*` env vars or `cluster.replication_*` in `arc.toml`):
- `replication_enabled` — master switch; requires `cluster.shared_secret` to be set
- `replication_pull_workers = 4` — concurrent fetch workers per node
- `replication_queue_size = 1024` — bounded queue; excess entries are dropped with a rate-limited warning and reconciled later
- `replication_fetch_timeout_ms = 60000` — puller-side per-fetch timeout
- `replication_serve_timeout_ms = 120000` — origin-side body-stream timeout; raise for large Parquet files or slow links
- `replication_retry_max_attempts = 3` — immediate retry attempts before dropping the entry

**Security:**
- Peer replication is gated by `FeatureClustering` — a user without an enterprise license cannot construct the puller.
- Shared secret is required at startup; Arc refuses to boot if `replication_enabled=true` and `shared_secret` is empty.
- All fetch traffic flows over the cluster coordinator port (not the public API port), physically isolating replication bytes from client traffic.
- When `cluster.tls_enabled=true`, the fetch client reuses the cluster TLS config for end-to-end encryption.

**Scope of Phase 2:** async replication only. Phase 2 does **not** include: resume via HTTP Range, catch-up for nodes joining with existing data, quorum durability, multi-peer fanout, or compaction-aware routing. These land in Phases 3–5.

**Design inspiration:** ClickHouse's ReplicatedMergeTree model — a Raft-equivalent op log + HTTP-pull-based file transfer. Research compared InfluxDB Enterprise (anti-entropy + HHQ), ClickHouse (log + HTTP pull), TimescaleDB (deprecated multi-node), Apache Pinot (peer fetcher fallback), and Apache Druid (metadata-driven assignment). ClickHouse's model is the cleanest fit for Arc's existing Raft + coordinator TCP infrastructure.

### Peer File Replication — Catch-up on Join (Enterprise — Phase 3)

Phase 2 replicated files reactively — a node only pulled files that committed to Raft while its puller was running. That left two fatal gaps on Kubernetes: (1) a node that was down when a file was flushed never got the callback and had the file missing on restart, and (2) a brand-new reader joining a cluster with existing data received the Raft log but couldn't pull anything because `entry.OriginNodeID` often pointed to a pod that had been rescheduled and no longer existed. Queries against such nodes silently returned incomplete results.

Phase 3 closes both gaps with a **startup reconciliation walker** and a **multi-peer fallback resolver**. A node that comes online now converges on the manifest within a bounded time, regardless of whether the original writer is still in the cluster.

**How it works:**
- After the puller starts, the coordinator spawns a background goroutine — guarded by `sync.Once` so it runs exactly once per coordinator lifetime — that waits for a Raft leader, issues a new `Node.Barrier` wrapper over `hraft.Raft.Barrier` so the local FSM reflects every committed entry, then feeds `fsm.GetAllFiles()` through a new `Puller.RunCatchUp` method. On `WaitForLeader` or `Barrier` timeout the walker proceeds against the follower's possibly-stale view — better a partial walk than no walk, and the reactive FSM callback path catches anything the walker missed as it applies.
- The walker enqueues each manifest entry through the existing Phase 2 worker pool, so all the Phase 2 retry, backoff, checksum verification, and metrics apply automatically — catch-up is data flowing through the same code path as reactive pulls.
- A per-path **inflight set** on the puller deduplicates enqueues: if the walker and a reactive FSM callback both try to enqueue path X during a write committing mid-walk, only the first call queues the entry and the rest are counted as `skipped_dup`. The slot is released via `defer` inside `processEntry` so the set stays bounded even on worker panic.
- **Queue-depth backpressure** — when the queue is above `replication_catchup_queue_high_water` (default 0.8), the walker sleeps 50ms between enqueues to let workers drain. Prevents thundering-herd drop storms on large manifests without needing a second worker pool.

**Multi-peer fallback resolver:** the Phase 2 `PeerResolver` interface returned a single `(addr, ok)` pair for the origin. Phase 3 changes it to `ResolvePeers(originNodeID, path string) []string` — the resolver takes just the two fields it needs (no `*raft.FileEntry` coupling), and returns an **ordered list**: origin first (if still healthy), then every other healthy peer excluding self. The `path` parameter is part of the signature so Phase 4+ can add shard-aware or compactor-aware routing without changing the interface; Phase 3 ignores it.

The puller iterates the list within a single attempt and falls through to the next candidate on any per-peer failure **except** checksum mismatch. A corrupt body from peer-1 is a data integrity signal, not a "try another peer" signal — if the puller fell through on checksum mismatch it would pull-and-corrupt from every healthy peer in turn. `ErrChecksumMismatch` breaks out of the per-attempt peer loop and the attempt-level retry handles it via the existing delete-and-redownload path.

**Typed ack error codes** — the ack header gained a new `protocol.AckErrorCode` typed field with constants (`AckCodeNotFound`, `AckCodeManifest`, `AckCodeAuth`, `AckCodeBackend`, `AckCodeRaft`, `AckCodeInvalidPath`) so the puller can distinguish "peer doesn't have this file" from "peer rejected me" without substring matching:

| Code | Puller behavior |
|---|---|
| `not_found` / `manifest` | Fall through to next peer |
| `auth` / `backend` / `raft` / `invalid_path` | Fail attempt (don't fall through) |

The `Code` field is a JSON `omitempty` addition — **backward compatible** with Phase 2 peers. When `Code` is empty (Phase 2 peer), the client falls back to **exact-match** (not substring — an adversary can't craft an error string like `"file not found on local backend: /etc/passwd"` to confuse the check) against `protocol.ErrMsgFileNotInManifest` / `protocol.ErrMsgFileNotFound`. Both the typed codes and the Phase 2 fallback strings live together in `internal/cluster/protocol/messages.go` so a refactor of either touches both sites at once.

**Configuration** (set via `ARC_CLUSTER_REPLICATION_CATCHUP_*` env vars or `cluster.replication_catchup_*` in `arc.toml`):
- `replication_catchup_enabled = true` — master switch; disable as an emergency kill-switch on pathologically large manifests
- `replication_catchup_barrier_timeout_ms = 10000` — Raft barrier timeout before walking
- `replication_catchup_queue_high_water = 0.8` — walker pauses enqueueing when the queue is above this fraction

No new worker-count knob — catch-up shares the Phase 2 `replication_pull_workers` pool.

**Observability:** `Puller.Stats()` grew new counters surfaced through the coordinator:
- `catchup_started_at` / `catchup_completed_at` (unix seconds; the latter goes non-zero once the walker has finished its pass, not once all queued pulls have drained)
- `catchup_entries_walked` / `catchup_enqueued` / `catchup_skipped_local`
- `skipped_dup` (new — counts Phase 3 dedup skips)

New `Coordinator.ReplicationCatchUpStatus()` returns the catch-up-specific stats as a JSON-serializable map intended for `/api/v1/cluster/status`. New `Coordinator.ReplicationReady()` returns `true` once the walker has completed its pass — not currently consumed (queries can hit a node during catch-up and see eventually-consistent results), but Phase 5 will use it to hard-gate the query path so the accessor lands now and avoids another coordinator surface change later. Operators can use the status endpoint for external readiness probes in the meantime.

**Security review:**
- Multi-peer fallback expands the set of trusted peers from `OriginNodeID` to any healthy cluster member — but all peers are already mutually authenticated via shared secret + TLS, and the Phase 2 path-bound HMAC is preserved so a stolen MAC still can't fetch a different file. Regression-tested.
- Checksum mismatch short-circuit is tested explicitly (`TestPullerMultiPeerChecksumMismatchDoesNotFallThrough`) to lock in the "one corrupt peer can't force every reader to pull-and-corrupt" invariant.
- Every `sendFetchError` call site uses a typed `AckErrorCode` constant — no path where user input flows into `Code`.

**Scope of Phase 3:** startup-only reconciliation with any-peer fallback. Phase 3 does **not** include: periodic reconciliation (reactive callbacks handle steady-state drift), orphan detection (Phase 4 compaction concern), paginated FSM walks (accepted O(N) snapshot copy — emergency kill-switch exists for 10M+ file deployments), hard query gating (Phase 5), bandwidth caps (Phase 5), or HTTP Range resume (Phase 5).

**What's coming next** (separate PRs):
- Phase 5: Optional write quorum for strong durability, hard query gating on catch-up, paginated FSM iteration, periodic reconciliation, bandwidth caps, resumable transfers via Range, automatic compactor failover, and proper leader forwarding for non-leader compactors.

### Peer File Replication — Dedicated Compactor Role (Enterprise — Phase 4)

Phase 2 and 3 replicated files correctly, but compaction still ran independently on every cluster node. On shared-storage deployments (S3, MinIO, Azure) this was a real correctness bug: two nodes picking up the same hour of source files would produce two distinct compacted outputs with unique filenames, both would land in the bucket, and queries would double-count rows until retention cleaned up. On local-storage deployments it was "only" wasteful — every node ran the same DuckDB merge on its own copy of the files, burning N× CPU and I/O for identical work.

**Phase 4 introduces a dedicated `RoleCompactor`** that runs compaction on exactly one node per cluster, and wires the compacted output into the existing Raft manifest so Phase 2/3 replication automatically distributes it.

**How it works:**
- When `ARC_CLUSTER_ROLE=compactor` is set on a node, the compaction scheduler starts as today. On any other role (`writer`, `reader`) the scheduler logs `"Compaction scheduler gated: node role does not have CanCompact capability"` at Info and stays idle. OSS and standalone deployments pass no gate and are unaffected — compaction runs unconditionally, preserving the OSS feature contract.
- Compaction still runs in a subprocess for memory isolation. Phase 4 adds a **completion-manifest handoff** on local disk at `{temp_directory}/.completion/pending/{job_id}.json`. The subprocess atomically writes the manifest in state `output_written` immediately after upload succeeds (the critical durability point), then advances it to `sources_deleted` once the source files are removed from storage.
- A parent-side **`CompletionWatcher`** polls the directory on a 1-second tick, reads each pending manifest, and translates it to `RegisterFile` / `DeleteFile` Raft commands via a new `CompactionBridge`. Successful apply removes the manifest. Failure (including the leader-flap case) leaves the manifest in place for the next poll — Raft FSM handlers are already idempotent, so retries are safe.
- The bridge checks `IsLeader()` before every Raft command and returns a new `ErrNotLeader` sentinel when the local compactor is not the Raft leader. The watcher recognizes this as a transient retry condition (not logged as an error). By the time leadership stabilizes (sub-second in practice), the retry succeeds.
- On readers, the existing Phase 2/3 `onFileRegistered` callback fires automatically when the compacted file lands in the manifest, and the puller pulls the bytes from the compactor. No changes to the replication layer.
- Phase 4 also activates the previously-logging-only `onFileDeleted` callback to **delete local copies** of source files after a 500ms grace period (giving in-flight queries time to finish reading). Local-storage readers shed their stale source copies; shared-storage deployments skip this entirely because the compactor already removed the shared object.

**Two operator-visible health warnings** surface rate-limited (once per minute) via the cluster health checker when `cluster.enabled && cluster.replication_enabled && compaction.enabled`:

```
No compactor elected: compacted files will accumulate.
  Set ARC_CLUSTER_ROLE=compactor on one node and restart.

Multiple compactors elected (N): shared storage may see
  duplicate outputs. Only one node should have
  ARC_CLUSTER_ROLE=compactor.
```

Both are `Warn` (not Error) — Arc keeps running, queries still work, but operators should fix the configuration. The second warning is the Phase 4 safety net for the exact misconfiguration the feature is designed to prevent.

**Configuration knobs** (all default to sensible values):
- `compaction.completion_watcher_interval_ms = 1000` — how often the watcher scans for pending manifests
- `compaction.completion_dir` (auto-derived from `compaction.temp_directory` when empty)
- `compaction.completion_orphan_timeout_ms = 600000` — sweep `writing_output` manifests older than this on startup (indicates a crashed subprocess; later states are never swept)

**No license flag for compaction.** Gating compaction would follow InfluxDB's well-known path and is explicitly rejected. The compactor-coordination mechanism is scoped under `FeatureClustering` because the `CompletionWatcher` and `CompactionBridge` only activate when clustering + peer replication are both enabled.

**Known Phase 4 limitations, all targeted by Phase 5:**
- **Single point of failure**: if the compactor node dies, compaction stalls until an operator sets another node's role to `compactor` and restarts. No automatic failover.
- **Leader-flap stalls**: if the compactor is not the Raft leader, the bridge short-circuits with `ErrNotLeader` and the watcher retries. Prolonged leader elections pause compaction progress until leadership stabilizes. Phase 5 will add proper leader forwarding.
- **Orphan compacted files**: if the subprocess crashes in the narrow window between `WriteReader` and writing the completion manifest (~1ms), a compacted file may exist in storage without a corresponding manifest entry. Operator cleanup required until Phase 5 adds automatic reconciliation.

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

## Hardening

### Directory Permissions

Directories containing sensitive data (auth database, continuous query definitions, retention policies, Raft consensus state, telemetry, and import output) now use 0700 (owner-only) permissions, consistent with WAL and local storage directories.

**Note:** `os.MkdirAll` does not change permissions on existing directories, so existing deployments retain their current permissions. Operators upgrading from earlier versions should manually run `chmod 700` on their data directory if desired.

### SQL Escaping in Compaction

Applied defense-in-depth SQL escaping to DuckDB `SET memory_limit` (single-quote escaping) and compaction `ORDER BY` sort key names (double-quote identifier escaping). Both already had config-level validation, but the runtime escaping provides an additional safety layer.

## Bug Fixes

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

## Dependencies

### DuckDB v1.5.1

Upgraded DuckDB Go binding from `v2.5.5` (DuckDB 1.4.4) to `v2.10501.0` (DuckDB 1.5.1). All platform-specific binary packages (`duckdb-go-bindings`, `lib/darwin-arm64`, `lib/linux-amd64`, `lib/linux-arm64`, `lib/windows-amd64`) updated in lockstep.

### AWS SDK v2 (S3/S3-compatible storage)

Bumped `aws-sdk-go-v2` core (1.40→1.41.5), `service/s3` (1.92→1.99), `aws/protocol/eventstream` (1.7.3→1.7.8), and all internal sub-packages. Key fixes:

- DNS timeout errors are now retried automatically, improving S3 reliability on flaky networks
- Fix for config load failure when a non-existent AWS profile is configured
- `smithy-go` 1.23→1.24 protocol layer update
