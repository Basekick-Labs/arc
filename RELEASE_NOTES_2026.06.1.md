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
- `X-Arc-HMAC` — `hex(HMAC-SHA256(secret, "cache-invalidate" \x00 nonce \x00 nodeID \x00 clusterName \x00 timestamp))`. Fields are NUL-delimited rather than colon-delimited so an identifier containing a colon cannot be smuggled to collide with a different field arrangement; NUL is forbidden in HTTP header values by Go's `net/http` (`x/net/http/httpguts`), so the sender cannot produce a NUL-containing field even via malicious config.

The label `cache-invalidate` is the first field of the canonical input, distinct from the message formats used by `ComputeForwardHMAC` (no label, payload-bound), `ComputeFetchHMAC` (no label, path-bound), and `ComputeHMAC` (no label, join). A leaked MAC for one endpoint can NOT be replayed against another even within the freshness window — verified by `TestCacheInvalidateHMAC_LabelBinding_NoCrossEndpointReplay`. Replay within the same endpoint is blocked by an in-process `NonceCache` (5-minute TTL, lazy eviction).

**Wire-format change across all cluster HMAC endpoints.** During the same protocol bump, the existing leader-forward, peer-fetch, and join HMACs (`ComputeForwardHMAC`, `ComputeFetchHMAC`, `ComputeHMAC`) ALSO switch from colon-delimited message formats (`nonce:nodeID:clusterName:...`) to NUL-delimited (`nonce \x00 nodeID \x00 clusterName \x00 ...`). This closes the same field-ambiguity class of attack on all four endpoints. Receivers also now decode the received MAC from hex and compare raw bytes via `hmac.Equal`, which is more idiomatic and fails closed on a malformed hex MAC (the previous shape compared hex strings directly, which also worked but did not validate the hex envelope). New determinism tests `TestComputeHMAC_Determinism`, `TestComputeFetchHMAC_Determinism`, and `TestComputeForwardHMAC_Determinism` pin reference MAC values for each endpoint so any future format drift fails CI.

Operator-facing changes:

- **OSS deployments are unaffected.** Post-compaction cache invalidation in OSS happens in-process (the in-process invalidation hooks are called directly from the compaction callback). No HTTP request is issued; no cluster-internal endpoint is touched. The `/api/v1/internal/cache/invalidate` route is NOT registered at all when `cluster.shared_secret` is unset, so an OSS node returns 404 (not 403) for the path — there is nothing to probe.
- **Cluster deployments without `cluster.shared_secret` configured**: the post-compaction fan-out is now SKIPPED instead of sending the static-header request that would have been refused anyway. The skip is logged once per process at the first post-compaction trigger (via `sync.Once`) so the message does not repeat per compaction. Set `cluster.shared_secret` to re-enable cross-node cache invalidation.
- **Cluster deployments WITH `cluster.shared_secret` configured**: no change from the operator's side — every node already uses the same secret for leader-forwarding and peer-fetch HMACs; this endpoint now uses the same secret.
- The receiver returns 403 (not 401) for every rejection path — missing headers, wrong secret, stale timestamp, future timestamp, self-addressed request, wrong cluster name, replay. Uniform rejection prevents an attacker from probing to distinguish rejection reasons. Every rejection path emits a Debug log including the remote IP plus whatever non-secret context is known (cluster name, node ID, timestamp string); operators chasing a misconfiguration can flip `internal/api`'s logger to Debug. Debug-level on purpose: at Warn the endpoint would amplify a network flooder into a log-DoS.
- A request claiming `X-Arc-Node-ID` equal to the receiver's own node ID is refused: local invalidation runs in-process, never over HTTP, so a self-addressed HTTP request is either a misconfiguration or a confused attacker.

**Rolling upgrade.** The NUL-delimited message format is a protocol bump that affects **every** cluster HMAC endpoint, not just the new cache-invalidate one. A 26.05.x node and a 26.06.1 node cannot validate each other's MACs during the upgrade window:

- **Cache invalidate** (new in 26.06.1): 26.05.x senders use the static `X-Arc-Internal` header that 26.06.1 receivers reject; 26.06.1 senders emit HMAC headers that 26.05.x receivers never check. Impact bounded by one compaction cycle of stale `cache_httpfs` glob results on the affected reader (in-process invalidation still runs locally on each node).
- **Leader forwarding** (existing): 26.05.x → 26.06.1 forwarding requests fail HMAC validation; the leader rejects mid-flight writes from old followers. Same in reverse.
- **Peer fetch** (existing): 26.05.x → 26.06.1 file-fetch requests fail HMAC validation. Replication of new files between mixed-version peers stalls until both ends are upgraded.
- **Join** (existing): a 26.05.x node attempting to join a 26.06.1-led cluster (or vice versa) will be rejected during the join handshake.

**Recommended procedure**: drain writes / pause compaction → stop all nodes → upgrade all binaries → restart. This avoids the mixed-version window entirely. If a true rolling restart is required, expect ~one compaction cycle of stale-cache symptoms and brief write/replication failures while nodes are mixed; the cluster heals automatically once every node is on 26.06.1.

**Threat-model notes for operators.** A few intentional trade-offs to be aware of:

- The endpoint sits outside the Fiber auth middleware (it must, because cluster peers don't carry user auth tokens — HMAC is the auth). It is therefore reachable by any caller who can connect to Arc's API port. Place cluster API ports on a private network; the existing audit guidance applies.
- Rejection paths are **not constant-time** with respect to one another. A LAN-adjacent attacker without the shared secret can latency-probe to fingerprint the cluster name (fast-reject before HMAC compute) and the local node ID (fast-reject on self-addressed). Neither value is a secret in this codebase (node IDs are broadcast in cluster gossip; cluster name is in every config file and log line), but the threat-model assumption is that an attacker who knows topology still cannot forge a MAC without the shared secret.
- Browser-originated CSRF is structurally blocked by the HMAC requirement — a victim admin's browser cannot forge `X-Arc-HMAC` without the secret. The endpoint requires no CSRF token of its own.
- A compromised cluster peer (one with the shared secret) can issue valid MACs with fresh nonces and inflate the receiver's nonce cache. Today the cache has no hard upper bound; memory cost is bounded by `peer_request_rate × 5 min × ~160 bytes/entry`. This is acceptable for the operator-trust threat model (anyone with the shared secret already has full cluster-internal access); follow-up work tracked in the post-merge issues to add a hard cap + rejection metrics.

Tests added: `TestCompute{,Fetch,Forward,CacheInvalidate}HMAC_Determinism` (pin reference MACs for every cluster HMAC endpoint so any future format drift fails CI), `TestValidate_RejectsMalformedHexMAC` (pins fail-closed on non-hex receivedMAC across all four validators), `TestValidateCacheInvalidateHMAC_{Valid, WrongSecret, StaleTimestamp, FutureTimestamp, FieldBinding}`, `TestCacheInvalidateHMAC_LabelBinding_NoCrossEndpointReplay` (cross-endpoint replay protection, covers Forward + Fetch + Join in both directions), `TestNewCacheInvalidateHandler_RejectsInvalidConfig` (constructor panics on each kind of misconfiguration), and 9 handler tests asserting both the response status AND the onInvalidate callback fired-exactly-once / never. The positive-path test asserts 204 + callback invoked; `TestCacheInvalidate_NonceNotBurnedByBadMAC` pins the ordering invariant (nonce cache `Track()` runs AFTER HMAC validation, so an off-path attacker without the secret cannot consume nonce-cache slots).

### Cluster FSM rejects malicious paths in manifest-registration proposals (Enterprise)

Reported by [Alex Manson](https://neurowinter.com/) ([@NeuroWinter](https://github.com/NeuroWinter)) — closes audit finding X2 / [GHSA-f85q-mvg8-qf37](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-f85q-mvg8-qf37). **Enterprise-only critical** (clustering requires Arc Enterprise license — OSS deployments do not construct the cluster FSM and are unaffected).

Pre-26.06.1 the Raft FSM (`internal/cluster/raft/fsm.go:applyRegisterFile` + `applyUpdateFile`) only checked that the proposed file path was non-empty. An attacker with one valid cluster credential — or one compromised cluster node — could issue Raft proposals registering arbitrary paths into the authoritative cluster manifest:

- `/etc/passwd` as a "cluster data file" — other nodes that fetch the file to serve queries read host-level files outside Arc's storage root.
- `s3://attacker-controlled-bucket/poisoned.parquet` — every cluster reader pulls attacker-controlled data into query results.
- `../../etc/shadow` via parent-traversal — depending on how the storage backend resolves the path.

Combined with [audit X1](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-wfgr-8x84-22q7) (no HMAC on `MsgReplicateSync`, fix tracked separately), this was a **cluster-wide worm primitive**: a single-node compromise propagated through the entire data plane.

26.06.1 introduces shape-based path validation in `internal/cluster/raft/path_validation.go`. Every manifest entry passes through `ValidateManifestPath` before landing in the FSM. The validator rejects:

- **URL schemes** — `s3://`, `http://`, `file://`, etc. Legitimate Arc paths are relative to the storage backend root; the scheme is added by the backend at read time, never stored.
- **Absolute paths** — leading `/` (POSIX) or `C:\` / `C:/` (Windows drive prefix). Legitimate paths are always backend-relative.
- **Parent-traversal segments** — any segment exactly equal to `..`, treating both `/` and `\` as separators in a single pass so mixed-separator paths like `db/..\etc` are caught. Filenames containing `..` as a substring inside a longer token (e.g. `metric_20260520..parquet`) are NOT false-positives — the check is on segment equality, not substring containment.
- **NUL bytes** — `\x00` smuggles through C-string callers (everything before NUL evaluates, everything after silently drops). Never legitimate.
- **Empty paths** — already rejected pre-PR; preserved.
- **Paths longer than 4096 bytes** — typical POSIX `PATH_MAX`. A malicious sender could otherwise propose a multi-megabyte path string to inflate the FSM's in-memory map.

Validator returns sentinel errors (`ErrPath{Empty, TooLong, Traversal, Absolute, Scheme, NullByte}`) so callers can branch on the rejection reason via `errors.Is`. Every code path that mutates the manifest — `applyRegisterFile`, `applyUpdateFile`, `applyBatchFileOps` (atomic pre-validation pass), and snapshot `Restore` — logs at `Error` with the path + rejection reason as structured fields.

**How rejection interacts with hashicorp/raft.** It is important to be precise about what "Apply rejects" actually does — the worm primitive is killed by the FSM, not by Raft's commit machinery:

- A proposer (Arc's `Node.RegisterFile` / `.UpdateFile` etc.) calls `raftNode.Apply(cmd, timeout)`. Inside hashicorp/raft, the leader writes the command to its local log AND ships it to follower replicators BEFORE `fsm.Apply` runs. By the time `fsm.Apply` returns an error, the entry is already in every node's Raft log and is committed at the cluster level.
- The security property is that **every node's FSM independently refuses to put the malicious path into `f.files`**: each `applyRegisterFile` validates the path, increments `rejectedPaths`, logs at `Error`, and returns the validation error. The proposer sees the error via `future.Response()` and propagates it; during log replay the error is silently swallowed (`req.future == nil`), but the entry still doesn't land and the counter still increments.
- The malicious entry stays in the Raft log until the next snapshot rotation, at which point `Snapshot()` drains `f.files` (which never contained the malicious entry) and writes a clean snapshot; older log entries past the snapshot are subsequently truncated by hashicorp/raft.

**Operator alerting.** `ClusterFSM.RejectedPathsCount()` returns a monotonic atomic counter of every refused entry across snapshot `Restore`, runtime Apply, and log-replay Apply. A non-zero growth rate is the load-bearing signal that somebody — a peer, a stored snapshot, or a Raft log entry from before validation was enforced — proposed a path this FSM refused. Operators should scrape this counter (or the per-entry `manifest path validation failed` Error log lines) and alert on growth. The Raft `Node.Start` also emits an `Error`-level summary at end-of-boot if any snapshot entries were refused during `Restore`.

**Snapshot Restore vs. log replay.** hashicorp/raft splits boot into two phases: snapshot restore (synchronous inside `NewRaft`) and log replay past the snapshot (asynchronous on the `runFSM` goroutine, post-NewRaft). The FSM treats both identically: `ValidateManifestPath` runs, the rejected entry doesn't land, the counter increments. The cluster never refuses to boot because of a malicious entry — boot continues with the offending entry refused and the operator alert raised.

**Atomicity of batched ops.** `applyBatchFileOps` (the common compaction path) runs a pre-validation pass over every Register/Update path in the batch before any state mutation. If ANY op carries a malicious path, the entire batch is refused and NO ops have side-effects. Without this pre-pass, a batch containing `[legit, malicious]` would have applied the legit op before the malicious one was caught — the test `TestApplyBatchFileOps_RejectsMaliciousPath_AtomicPrevalidation` pins this invariant.

Approach note: shape-based validation, not backend-aware prefix matching. The legitimate path format produced by Arc's writer (`internal/cluster/file_registrar.go`) and compactor (`internal/cluster/compaction_bridge.go`) is `{database}/{measurement}/{year}/{month}/{day}/{hour}/{filename}.parquet` — always relative, always scheme-less, no `..`. The shape check catches every malicious shape from the audit without requiring `*config.StorageConfig` to be plumbed into the FSM (which would expand security surface unnecessarily). Backend-aware prefix matching can land later as a tightening if a path is found that the shape check accepts but the backend should reject.

Tests added in `internal/cluster/raft/path_validation_test.go`: table-driven rejection matrix covering every shape from the audit + close-shape variants, sentinel-error verification via `errors.Is`, and a positive set covering production writer/compaction output. In `internal/cluster/raft/fsm_test.go`: `TestApplyRegisterFile_RejectsMaliciousPath` (table-driven across every malicious shape, asserts error + counter + non-membership in `f.files`), `TestApplyRegisterFile_MixedBatchIsolatesMaliciousEntries` (sequential Apply calls with legit/malicious interleaved — legit lands, malicious refused, counter reflects all), `TestApplyUpdateFile_RejectsMaliciousPath`, `TestApplyBatchFileOps_RejectsMaliciousPath_AtomicPrevalidation` (atomic batch: legit op preceding malicious op MUST NOT land), and `TestFSMRestore_QuarantinesMaliciousSnapshotEntries` (snapshot round-trip: 3 legit + 2 malicious entries → only 3 legit land, counter = 2).

### Cluster replication stream now HMAC-authenticated end-to-end (Enterprise)

Reported by [Alex Manson](https://neurowinter.com/) ([@NeuroWinter](https://github.com/NeuroWinter)) — closes audit finding X1 / [GHSA-wfgr-8x84-22q7](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-wfgr-8x84-22q7) / CVE-2026-48106. **Enterprise-only critical** (WAL replication requires Arc Enterprise license — OSS deployments do not run the writer→reader replication stream and are unaffected).

Pre-26.06.1 the writer accepted `MsgReplicateSync` on its cluster-coordinator listener and immediately attached the connection to the replication sender — no application-layer authentication whatsoever. The only check was network reachability: any peer that could complete the TCP/TLS handshake (every node trusted by the cluster CA) could open a replica connection and inject WAL entries that every reader would apply to its in-memory ingest buffer and local WAL. Combined with the X2 path-validation gap (fixed above), a single compromised cluster credential turned into cluster-wide data injection.

26.06.1 introduces a three-layer HMAC scheme designed to add as close to zero overhead as possible on Arc's 19.9M records/sec ingest path:

- **Layer 1 — Handshake HMAC.** Every `MsgReplicateSync` now carries `{nonce, cluster_name, timestamp, hmac}` computed over `replicate-sync\0nonce\0reader_id\0cluster_name\0last_known_seq\0timestamp` keyed by `cluster.shared_secret`. The writer (`internal/cluster/coordinator.go` `handleReplicateSync`) validates the MAC against a ±5 minute symmetric timestamp window, checks the existing `NonceCache` for replay, rejects self-addressed requests, rejects cluster-name mismatch, and refuses every request uniformly when `cluster.shared_secret` is empty — same refuse-when-unconfigured posture as the cache-invalidate endpoint shipped earlier in this release.
- **Layer 2 — Per-entry truncated MAC tag.** Both ends derive a per-connection 32-byte session key via HKDF-SHA-256 over `(shared_secret, handshake_nonce, "arc-replication-session:")` — single-use because the handshake nonce is single-use. Every streamed `ReplicateEntry` carries an 8-byte truncated HMAC-SHA-256 tag over `("entry-frame:", sequence_be64, sha256(payload)[:8])` keyed by the session key. The receiver rejects any entry with a missing, malformed, or invalid tag by dropping the connection. The receiver also enforces strictly-monotonic sequences (any entry with `Sequence <= lastSeq` drops the connection) — protocol-level replay protection that the original code lacked even before considering authentication.
- **Layer 3 — Periodic full-HMAC checkpoint.** Every 1024 entries the sender emits a `MsgReplicateCheckpoint` carrying the running SHA-256 over every payload since the handshake, plus a full HMAC-SHA-256 over `(replicate-checkpoint, nonce, sender_node_id, cluster_name, cumulative_hash, last_seq, timestamp)` keyed by the **cluster shared secret directly** (not the session key — so a hypothetical session-key compromise mid-stream still cannot forge a checkpoint). The receiver maintains its own running hash; any mismatch drops the connection. The truncated per-entry tag gives 2⁻⁶⁴ forgery probability per entry; the periodic checkpoint is the full-strength backstop. The interval default (1024) lives in `SenderConfig.CheckpointInterval` and is not currently surfaced through `arc.toml` — operators who need to tune it should open an issue.

The session key, cumulative hash, and per-entry counter all live for the lifetime of a single connection and are reset on reconnect — never reused across handshakes.

**Performance.** Bench (`internal/cluster/replication/auth_bench_test.go`), 256-byte payload on Apple M3 Max:

- Naive single-call (`ComputeReplicationEntryTag`): 513 ns / 784 B / 11 allocs.
- Broadcast-friendly with pre-computed payload hash (`ComputeReplicationEntryTagFromHash`): 376 ns / 784 B / 11 allocs — saves the per-reader SHA-256 over the payload.
- **Sender production hot path** (`ComputeReplicationEntryTagWithMAC` with a per-reader reusable `hmac.Hash`): **120 ns / 72 B / 3 allocs** — 3.1× faster and ~11× lower allocations than the naive variant.
- **Receiver production hot path** (`ValidateReplicationEntryTagWithMAC` with the same reusable-mac pattern, fed the SHA-256 the receiver already computes for the cumulative-hash checkpoint): **127 ns / 72 B / 3 allocs** — ~4× faster than the naive validate.

For Arc's typical batch-of-records-per-entry shape (~100 records/entry → ~200k entries/sec at 19.9M records/sec) the per-entry overhead works out to **~0.025% of a single core per side** (sender + receiver each ~0.024%). The full HMAC fires once every 1024 entries, so its cost amortises away. For multi-reader deployments the broadcast path computes SHA-256 once per entry and reuses it across every reader; the receiver computes SHA-256 once per entry and reuses it across tag-verify + cumulative-hash.

**Operator-facing change.** `cluster.shared_secret` is now **required** for WAL replication. Pre-26.06.1 deployments that ran clustered replication with an empty shared secret will start logging `replication sender refuses connection: cluster.shared_secret not configured` on reader nodes and `Replication sync rejected: cluster.shared_secret not configured` on writer nodes until the secret is populated on every node. The same secret must match across all nodes — same value already used for the optional join/leave/forward-apply HMACs. Rolling upgrades are NOT supported for this change: pre-26.06.1 readers send no auth fields and 26.06.1 writers refuse them; the supported upgrade path is a full cluster restart with the secret pre-staged on every node.

Tests added in `internal/cluster/replication/auth_roundtrip_test.go`: `TestAcceptReaderRefusesWhenSharedSecretEmpty` (refuse-when-unconfigured at the sender), `TestAcceptReaderRefusesWhenHandshakeNonceEmpty` (defensive — coordinator should have rejected first), `TestSenderStampsValidTagOnEveryEntry` (green path: every wire entry carries a tag that verifies under the same session key the receiver would derive, plus per-entry tampered-payload and sequence-shifted-tag negative cases), `TestSenderEmitsCheckpointAtInterval` (checkpoint follows exactly N entries with verifying cumulative hash and full HMAC), `TestSenderDoesNotEmitCheckpointBeforeInterval` (checkpoint timing is exact, not approximate), `TestEntryTagPerConnectionKeyIsolation` (a tag valid on connection A does NOT verify under connection B's key — single-use nonce → isolated session key). In `internal/cluster/security/auth_test.go`: determinism, label-binding, tamper-detection, and timestamp-window coverage for `ComputeReplicateSyncHMAC`, `ComputeReplicationEntryTag`, `ComputeReplicationCheckpointHMAC`, and `DeriveReplicationSessionKey`.

Out of scope for this PR: the per-shard replication code in `internal/cluster/sharding/shard_replication.go` / `shard_receiver.go` is currently dead code — `NewShardReplicationManager` and `NewShardReceiverManager` are not wired into the coordinator dispatch and cannot be reached in any deployed Arc instance today. Extending the HMAC plumbing to those paths will land when shard replication is activated.

**Two adjacent pre-existing replication bugs fixed alongside the auth work**, surfaced by the end-to-end two-node smoke test:

- **Sync-ack wire-format mismatch.** The writer used to send the success `MsgReplicateSyncAck` via the replication-package wire format (type 0x13), but the receiver reads with the cluster-protocol decoder, which only knows type 0x11 — every Arc Enterprise replication handshake failed silently with `read sync response: failed to unmarshal payload: unknown message type: 19`, retrying every 5 seconds with zero entries actually streamed. The coordinator now sends the success ack in the cluster-protocol format, matching the rejection paths in `handleReplicateSync`. The handshake completes end-to-end for the first time.
- **30-second idle disconnect cycle.** Both sender (`internal/cluster/replication/sender.go` `receiveLoop`) and receiver (`receiveLoop`) checked for the read-deadline timeout via `err.(net.Error)`, but `ReadMessage` wraps the underlying network error with `fmt.Errorf("...: %w", err)` — the plain type assertion failed, the code took the "Connection closed" branch instead of the timeout-continue, and the connection tore down every 30 seconds on any idle stream. Fixed to unwrap with `errors.As(err, &netErr)`. Replication is now stable across multi-minute idle windows.

Together these fixes mean that pre-26.06.1 Arc Enterprise WAL replication was effectively broken even when the operator did everything right; this PR is the first release where it works end-to-end. **Verified via two-node local smoke test**: 31 records ingested on writer → 31 replicated to reader via the WAL stream (independent from peer file replication, which delivered the parquet artifacts via the manifest puller), zero handshake failures, zero disconnects across 55 seconds of idle, all per-entry tags verified, no checkpoint hash mismatches.

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

### Partition pruner caches no longer grow unboundedly over process lifetime

`internal/pruning/partition_pruner.go` has two TTL caches — `globCache` (30 s TTL) and `partitionCache` (60 s TTL) — populated on every query. Their `get()` returns "expired" as a cache miss but does **not** evict the stale entry; neither cache has a max-size cap. The public `CleanupGlobCache()` and `CleanupPartitionCache()` methods existed since the 2024-12 and 2026-01 cache PRs respectively but had **zero production callers** anywhere in the binary. Under workloads with high-cardinality glob patterns or distinct `(path, sql)` keys (typical for satellite-telemetry dashboards), both maps accumulate Go-heap entries monotonically until either `InvalidateAllCaches()` runs post-compaction or the process exits — one component of a 24-hour RSS climb observed on a demo container that required daily restarts.

26.06.1 wires a janitor goroutine that sweeps both caches at a 30-second interval (matching the shorter TTL → worst-case entry retention is bounded at ~2× TTL):

- **`PartitionPruner.StartCleanup(ctx, interval)`** — public method that spawns the sweeper. Exits cleanly when `ctx` is cancelled. Idempotent via a `cleanupStarted` atomic flag, so a hot-reload path or test refactor can't silently multiply goroutines.
- **`QueryHandler.StartBackgroundWorkers(ctx)`** — the seam from `main.go`. Today this just delegates to the pruner; the wrapper exists so future per-handler background workers land in one obvious place.
- **`cmd/arc/main.go`** wires the lifecycle: ad-hoc `context.WithCancel`, hook registered at `shutdown.PriorityHTTPServer` (the earliest tier — the janitor owns nothing but a ticker and reads in-memory maps, so it's safe to stop first). Mirrors the WAL maintenance pattern already in `main.go`.

Operator-visible: one new info line at startup —

```
Partition pruner cache janitor started interval=30000
```

and the symmetric "stopped" line on graceful shutdown. No new configuration knob; the 30 s interval is hard-coded today and operators do not need to tune it. The fix has no effect on the cache **hit rate** — entries are only swept after they're already expired by TTL, which the existing `get()` already treats as a miss.

Out of scope for this release: a max-size LRU layer on top of both caches. The sweep-cost analysis showed this is unlikely to bite at realistic key cardinality between 30-second sweeps; will revisit when telemetry shows sweep latency above 10 ms.

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

### Inter-node HTTP traffic now respects `server.tls_enabled`

Before this release, two **wired** inter-node HTTP call sites silently hardcoded `http://`:

- The post-compaction cache-invalidate fan-out (introduced in PR #444 — same release).
- `internal/cluster/router.go` leader-forwarding (long-standing).

(`internal/cluster/sharding/router.go` has the same shape but no production caller today; it will be addressed when the sharding path is wired in.)

When an operator ran Arc in cluster mode with `server.tls_enabled = true`, every peer's Fiber listener served HTTPS — and these senders dialed plaintext, failing the TLS handshake with no useful error. Cluster fan-out and leader-forwarding were both effectively broken in that configuration.

26.06.1 adds two helpers in `internal/cluster/security/tls.go`:

- `NewClusterHTTPTransport(*tls.Config) *http.Transport` — returns a transport with the supplied `*tls.Config` (cloned) on its `TLSClientConfig`, and the historical pool defaults (`MaxIdleConns=100`, `MaxIdleConnsPerHost=10`, `IdleConnTimeout=90s`) extracted to shared constants. Passing nil yields a plaintext transport.
- `SchemeForServer(serverTLSEnabled bool) string` — returns `"http"` or `"https"` based on the operator's `server.tls_enabled`. All cluster nodes are expected to be configured identically (heterogeneous clusters are unsupported).

The cluster `Coordinator` now loads cluster TLS once at construction and exposes it via `Coordinator.ClusterTLSConfig()`. The request router and the cache-invalidate fan-out both consume that single `*tls.Config`, so cert/key/CA are read from disk once at startup (the previous shape would have re-read them per consumer and re-emitted the "certificate expires in N days" warning multiple times).

Two new coordinator-startup Warns surface common misconfigurations:

- **`server.tls_enabled=true` with `cluster.tls_enabled=false`**: inter-node HTTP will verify peers against the system root CA pool, not a private cluster CA. An attacker who can present any system-trusted cert for the peer's address can MitM. The Warn recommends setting `cluster.tls_enabled` + `cluster.tls_ca_file`.
- **`cluster.tls_enabled=true` with empty `cluster.tls_ca_file`**: verification falls back to system roots, and self-signed cluster certs will fail every handshake. The Warn recommends pointing `cluster.tls_ca_file` at the CA that signed `cluster.tls_cert_file`.

**Operator action:** if you run Arc with `server.tls_enabled=true` in cluster mode, set `cluster.tls_enabled=true` + `cluster.tls_ca_file=/path/to/cluster/ca.pem` to pin inter-node TLS verification to a private cluster CA. Without this, peer cert verification uses the system root pool, which is a weaker (but functional) posture.

Tests: `TestNewClusterHTTPTransport_{PlainHTTP, TLSConfigCloned}` cover the back-compat and happy paths (the latter generates a self-signed ECDSA cert in `t.TempDir()` and asserts the cloned `TLSClientConfig` carries `Certificates`, `RootCAs`, `MinVersion=TLS12`, and `InsecureSkipVerify=false`). `TestClusterTLSConfig_MissingFiles` asserts fail-loud via both substring match on the wrapped error and `errors.Is(err, fs.ErrNotExist)`. `TestSchemeForServer` pins the http/https switch.

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

### Cluster Auth Convergence — Phase A (token replication) (Enterprise)

Before 26.06.1, every Arc Enterprise cluster node carried its **own** SQLite auth DB (`auth.db_path`, default `./data/arc.db`). A token created via `POST /api/v1/auth/tokens` on the writer was **not** valid on the reader — the reader's local SQLite never saw the row. Operators worked around this by setting `ARC_AUTH_BOOTSTRAP_TOKEN` to the same value on every node, but **API-created tokens and revocations did not propagate**. Combined: a security-incident-response revocation on the writer left the same token still valid on every reader for the lifetime of the affected reader process — a real customer-visible auth gap that nothing in the cluster protocol fixed automatically.

26.06.1 closes this for the `api_tokens` surface. Auth **writes** are now routed through the existing Raft FSM (the same FSM that already holds node membership, the file manifest, and the compactor lease). Auth **reads** stay local-cache-fast — no Raft round trip on every API call. End-to-end behaviour: a token created on any node propagates to every node's local SQLite within one Raft commit (typically <50ms over loopback); a revocation invalidates the cached `*TokenInfo` on every node in the same window.

**Architecture.** A new `RaftProposer` interface on `*AuthManager` (`internal/auth/raft_proposer.go`) supplies the seam. In OSS / standalone (no cluster) the proposer is nil and writes hit local SQLite directly — **OSS path is byte-identical to pre-26.06.1**. In cluster mode, writes are marshalled into one of five new Raft commands (`CommandCreateToken=12`, `CommandUpdateToken=13`, `CommandRevokeToken=14`, `CommandDeleteToken=15`, `CommandRotateToken=16`) and proposed via `cluster.CoordinatorAuthProposer` — leader applies directly, followers forward to the leader using the same `forwardApplyToLeader` plumbing as the file manifest. Every node's FSM materialises the result into its local SQLite via `AuthManager.ApplyCreateToken` / `ApplyUpdateToken` / `ApplyRevokeToken` / `ApplyDeleteToken` / `ApplyRotateToken`, then calls `InvalidateCache()` so the next `VerifyToken` reflects the new state.

**Plaintext-secrecy invariant.** The plaintext token value is generated on the proposing node, bcrypt-hashed in-process, and returned to the API caller out-of-band. **Only the bcrypt hash + prefix go into the Raft payload** — the plaintext never crosses the wire, never lands in the Raft log, and never lands in any FSM snapshot. Pinned by `TestTwoNodeAuth_CreateOnAVerifyOnB`, which snapshots both FSMs after the round-trip and `strings.Contains`-greps the persisted blob for the plaintext substring.

**Deterministic token IDs.** New tokens are stamped with `ID = raft.Log.Index` (mirror of the `FileEntry.LSN` pattern from the file manifest). Both nodes apply the same log entry at the same index and produce the same `INSERT OR IGNORE INTO api_tokens(id, ...)` row, so the SQLite primary key is identical on every node. Existing rows (from pre-26.06.1 deployments) keep their `AUTOINCREMENT` IDs; both shapes fit in `INTEGER`.

**Applier-side validation.** Every token command runs `validateTokenEntry` on the applier — same precedent as the manifest path validation shipped in PR #446. Empty name, missing bcrypt hash, missing prefix, invalid permission string, or `created_at == 0` cause the entry to be rejected on every node (the malicious entry never lands in `f.tokens` and never propagates to local SQLite), the `arc_cluster_auth_rejected_total` counter increments, and an `Error` log line surfaces the rejection reason. The Raft entry itself still commits (hashicorp/raft's commit machinery is upstream of `fsm.Apply`); the security property is that **every node's FSM independently refuses to put the malicious token into its state**.

**Bootstrap idempotency.** `EnsureInitialToken` now routes through Raft when in cluster mode. All nodes call it on boot with their own randomly-generated bootstrap token; only the Raft leader's proposal lands, the FSM's `applyCreateToken` rejects the others with `"token name already exists"` (matching the `api_tokens(name)` `UNIQUE` constraint), and the proposer treats that error as success-with-no-banner. Net effect: exactly one node prints the `Admin API token:` banner per cluster boot — the leader.

**No migration of pre-existing tokens.** Tokens created on a pre-26.06.1 node by the local-only API path remain valid **only on that node** after upgrade. Operators are expected to re-issue API tokens through the new replicated path so they take effect cluster-wide. Bootstrap tokens set via `ARC_AUTH_BOOTSTRAP_TOKEN` are unaffected if the same value was used on every node (which the previous workaround required).

**Divergence detection on upgrade.** Pre-26.06.1 tokens were stamped with `AUTOINCREMENT` IDs (1, 2, 3…). Cluster-replicated tokens land at `ID = Raft log index`, which can be 5+ for a freshly-bootstrapped cluster. An operator with many pre-existing tokens may have a pre-existing AUTOINCREMENT row whose ID happens to match the new cluster row's ID — silent `INSERT OR IGNORE` would mask the new row on that node while applying on every other node, leaving `VerifyToken` results divergent across the cluster. 26.06.1 detects this at materialise time: if a row at the same ID exists with a DIFFERENT token hash or name, `ApplyCreateToken` returns an error rather than overwriting silently, the `arc_cluster_auth_rejected_total` counter increments, and the cluster Apply path logs the divergence at `Error` level. Operator action: drop the diverging local `auth.db` rows (or the whole local auth DB and let the FSM repopulate) before re-joining. Identical hash + name is treated as idempotent log replay (no-op, no error), so a restart of a healthy cluster does not surface this.

**Operator-facing changes.**

- **No new env vars.** `auth.db_path` and `ARC_AUTH_BOOTSTRAP_TOKEN` work as before.
- **Bootstrap banner now prints on only one node** (the Raft leader). Each cluster node calls `EnsureInitialToken` with its own random plaintext; only the Raft leader's proposal lands cluster-wide. Followers receive the FSM's `"token name already exists"` rejection and return an empty plaintext to the caller, so no banner is emitted on losers. The losing node's local SQLite still gets the winner's hash + prefix via the FSM materialise callback — every node converges on the same admin token.
- **New Prometheus counters**:
  - `arc_cluster_auth_apply_create_total` / `arc_cluster_auth_apply_update_total` / `arc_cluster_auth_apply_revoke_total` / `arc_cluster_auth_apply_delete_total` / `arc_cluster_auth_apply_rotate_total` — count of applies on this node, per command. Healthy cluster: every node sees the same monotonic count (they all apply the same Raft log).
  - `arc_cluster_auth_rejected_total` — applier-side validation refusals. Non-zero growth is the security alerting signal: somebody is proposing invalid tokens (malformed bcrypt hash, missing prefix, empty name, malformed permission string).
- **Rolling upgrade is supported.** The new command types are additive (12-16 in the existing `CommandType` enum); a pre-26.06.1 follower receiving a 26.06.1 token command will reject it at the FSM with `unknown command type`, but the protocol allows mixed-version operation for a short window. The supported path is still a full cluster restart with all binaries upgraded together.

**Out of scope for Phase A** (tracked as Phase A.1 / A.2 / B in [project_cluster_auth.md](memory/project_cluster_auth.md)):

- **RBAC commands** (`organizations`, `teams`, `roles`, `measurement_permissions`, `token_memberships`) — landed in the same 26.06.1 release as Phase A.1. See the next section below.
- **SSO / OIDC / LDAP** — Phase B, separate roadmap item.
- **Audit log replication** — never; intentionally per-node (high-volume append-only, no consensus requirement).
- **Read-after-write `Barrier`** — eventual consistency (Strategy A) is the contract today; sub-50ms typical convergence makes a per-query Raft barrier unnecessary for the customer surface, and SDKs already retry on transient 401. Phase A.2 only if a real customer report lands.

**Context plumbing.** All 7 `AuthManager` write methods (`CreateToken`, `CreateTokenWithValue`, `UpdateToken`, `DeleteToken`, `RevokeToken`, `RotateToken`, `ForceAddRecoveryToken`) accept a `context.Context` as their first parameter. In cluster mode that context bounds the Raft proposal wait so a higher-level timeout can cancel in-flight Raft applies. In OSS / standalone mode the context is accepted for signature consistency but currently unused — single SQLite writes don't observe cancellation today without re-plumbing the entire `database/sql` layer. API handlers in `internal/api/auth_routes.go` pass `c.UserContext()`, which today resolves to `context.Background()` for every Fiber request because no middleware in this codebase calls `SetUserContext` — the plumbing is structurally correct but runtime-inert until that wiring lands in a follow-up. The bootstrap path in `cmd/arc/main.go` wraps the deferred initial-token call in a real `context.WithTimeout(30s)` ceiling so a pathological "no leader ever elected" boot still terminates cleanly rather than blocking startup indefinitely.

**Local-cache divergence detection.** The four materialise methods (`ApplyUpdateToken`, `ApplyRevokeToken`, `ApplyDeleteToken`, `ApplyRotateToken`) now check `RowsAffected()` after their SQLite mutation. A zero count means the cluster FSM thinks this node holds a token row that local SQLite doesn't — most likely a pre-26.06.1 AUTOINCREMENT row that was lost in the upgrade, or operator-driven manual cleanup of `auth.db`. Update / Revoke / Rotate return an `Error`-level log + an error from the apply (the local SQLite is now divergent and the customer-visible auth behaviour on this node is wrong). Delete logs a `Warn` but doesn't error — the post-state matches (no row either way), so the divergence is observable in logs without breaking the apply. In all four cases the cache is invalidated so the next `VerifyToken` reads from SQLite rather than a stale in-memory entry. Note: the FSM callback signature is `func(*TokenEntry)` (no error return), so the materialise error doesn't reach the API caller on the leader — the caller still sees 200 OK. The cluster's in-memory FSM map remains authoritative; the divergence is at the local-cache layer only. A future PR will broaden the callback signature so the leader's API call surfaces divergence to the operator.

Tests added: 22 FSM-level tests in `internal/cluster/raft/fsm_token_test.go` (apply success + every validation rejection shape + ID-stamping-from-log-index + prefix-index maintenance + tokensByName secondary index maintenance + permission-string whitespace tolerance + snapshot round-trip + idempotent replay of unknown-ID revoke/delete); 6 AuthManager round-trip tests in `internal/auth/cluster_proposer_test.go` (proposer-nil OSS fallthrough, create→VerifyToken via proposer, revoke→cache-invalidation via proposer, `EnsureFirstToken` no-op-on-loser path, wire-format JSON round-trip pinning, materialise-divergence-detection); 2 two-node integration tests in `internal/cluster/auth_cluster_integration_test.go` (create-on-A → verify-on-B success; revoke-on-A → invalidate-on-B; plaintext-non-leak via snapshot grep). All under `-race`.

### Cluster Auth Convergence — Phase A.1 (RBAC replication) (Enterprise)

Phase A.1 extends the Phase A FSM seam to the five RBAC tables — `rbac_organizations`, `rbac_teams`, `rbac_roles`, `rbac_measurement_permissions`, `rbac_token_memberships`. Same divergence shape Phase A closed for tokens: an organization, team, role, or token-team membership created on one cluster node was previously visible only on that node, while RBAC-gated reads from any other node could not see the new permissions. 26.06.1 routes the 13 RBAC writes through the same Raft proposer used by tokens, so every write applies on every node within the same ~50ms commit window.

**The 13 new commands** extend the FSM `CommandType` enum at values 17-29: `CommandCreateOrganization`/`Update`/`Delete`, `CommandCreateTeam`/`Update`/`Delete`, `CommandCreateRole`/`Update`/`Delete`, `CommandCreateMeasurementPermission`/`Delete`, `CommandAddTokenToTeam`, `CommandRemoveTokenFromTeam`. A wire-format pinning test (`TestProposalCommandTypesMatchFSM` in `internal/cluster/raft/fsm_proposal_pin_test.go`) covers all 18 values (5 token + 13 RBAC) so the `internal/auth` mirror constants and the `cluster/raft` enum cannot drift silently. Adding a new command type requires updating both sides and extending the table.

**FSM cascade-in-apply.** `applyDeleteOrganization` walks `teamsByOrg → rolesByTeam → measurementPermsByRole → tokenMembershipsByTeam` and removes every descendant under a single Raft log entry. `applyDeleteTeam` does the same minus the org level; `applyDeleteRole` cascades only to measurement_permissions. Mirroring SQLite's `ON DELETE CASCADE` foreign keys at the FSM layer keeps the in-memory state consistent on every node atomically — a concurrent `CreateTeam` under an org that's being deleted is serialised by `f.mu.Lock()` and sees the post-cascade state (the team's parent org no longer exists, so the apply is rejected). The local SQLite-side cascade fires naturally inside `ApplyDeleteOrganization` via the existing FK definitions — two independent mechanisms, each correct in its own layer.

**Phase A's `applyDeleteToken` was extended** to also clear `tokenMemberships` from FSM state when a token is hard-deleted, mirroring the SQLite FK `rbac_token_memberships.token_id REFERENCES api_tokens(id) ON DELETE CASCADE`. Without this, deleting a token via Phase A would leave orphaned membership rows in the FSM's in-memory map pointing at a non-existent token. `TestApplyDeleteToken_CascadesMemberships` pins this.

**Secondary indices for O(1) UNIQUE enforcement on the single-threaded apply path:**

- `organizationsByName` — UNIQUE(name) cluster-wide
- `teamsByOrg` — folded UNIQUE-and-traversal index, orgID → name → teamID (serves both UNIQUE(org_id, name) and the "all teams under this org" cascade query)
- `tokenMembershipsByPair` — UNIQUE(token_id, team_id), nested tokenID → teamID → membershipID
- `rolesByTeam`, `measurementPermsByRole`, `tokenMembershipsByToken`, `tokenMembershipsByTeam` — traversal-only, used by the cascade paths to find descendants without scanning the primary maps

All indices are rebuilt on `Restore` from the primary maps — secondary indices are never persisted, so future index additions don't break old-snapshot reads.

**13 new Apply\<X\> methods on `*RBACManager`** in `internal/auth/cluster_rbac_apply.go` materialise the Raft-applied state into local SQLite. Same pattern as Phase A's token apply methods: SELECT-first by id catches identical re-applies (log replay) as no-ops; content drift at the same id surfaces a loud cluster↔local divergence error. The 13 `RBACManager` write methods (`CreateOrganization`, `UpdateOrganization`, `DeleteOrganization`, `CreateTeam`, `UpdateTeam`, `DeleteTeam`, `CreateRole`, `UpdateRole`, `DeleteRole`, `CreateMeasurementPermission`, `DeleteMeasurementPermission`, `AddTokenToTeam`, `RemoveTokenFromTeam`) accept `ctx context.Context` as their first parameter; in cluster mode that context bounds the Raft proposal wait, in OSS / standalone it falls through to the existing direct-SQLite path unchanged.

**Fourteen new Prometheus counters** (13 apply + 1 rejected) plus the existing auth counters from Phase A: `arc_cluster_rbac_apply_{create,update,delete}_organization_total`, `arc_cluster_rbac_apply_{create,update,delete}_team_total`, `arc_cluster_rbac_apply_{create,update,delete}_role_total`, `arc_cluster_rbac_apply_{create,delete}_measurement_permission_total`, `arc_cluster_rbac_apply_{add,remove}_token_to_team_total` (13 apply counters total), and `arc_cluster_rbac_rejected_total` (single counter aggregating applier-side validation rejections across all 13 command types — non-zero growth is the security-alerting signal that a proposer is submitting malformed RBAC commands). Same per-node monotonic semantics as the Phase A counters.

**Upgrade-seed for organizations.** `RBACManager.SeedRBACFromLocalSQLite` runs leader-only at startup (gated by `proposer.IsLeader()` and a `WaitForLeader(30s)` precondition in `cmd/arc/main.go`). It walks `rbac_organizations` and proposes a `CommandCreateOrganization` for every pre-26.06.1 row, so the cluster FSM learns about local orgs that existed before Phase A.1. The proposer's own `ApplyCreateOrganization` handles the resulting ID mismatch via a single SQLite transaction: when the local INSERT trips `UNIQUE(name)` (because the pre-A.1 AUTOINCREMENT row already holds that name under a different surrogate ID), the materialise DELETEs the old row by name and re-INSERTs it at the FSM-stamped log-index ID. The leader's local SQLite ends up at the same ID as the cluster FSM and every follower, so subsequent `CreateTeam` proposals under that org id resolve cleanly against the local FK. **Crucially, the DELETE cascades via SQLite `ON DELETE CASCADE` to every pre-A.1 child row attached to that org** — local teams, roles, measurement_permissions, and token_memberships under the old AUTOINCREMENT id are lost on the leader as part of the realign. This is intentional and the only correct behaviour: pre-A.1 child rows reference the old surrogate id, the cluster's authoritative state doesn't know them at all, and re-issuing them via the post-upgrade API is the path that gets them replicated cluster-wide. **Teams, roles, measurement_permissions, and token_memberships are NOT auto-seeded.** They reference parent entities by surrogate ID, and the pre-A.1 local AUTOINCREMENT IDs do not match the cluster's log-index-stamped IDs after the org seed runs — re-issuing these rows via the API post-upgrade is the documented path. If any child RBAC rows are present locally at seed time, the seed logs a `Warn` listing the counts per table and telling the operator to re-issue.

**Operator-facing changes from Phase A.1:**

- **No new env vars or required configuration.** `cluster.enabled=true` + an Enterprise license already gate the whole Phase A / Phase A.1 surface.
- **The forward-apply allowlist in `internal/cluster/forward_apply.go` was extended.** Followers forward the 13 new RBAC command types to the leader using the same `forwardApplyToLeader` machinery used by Phase A's token commands. Role gate stays the same: any known authenticated peer can forward an RBAC write (the admin-check already happened on the proposing node's HTTP handler via `RequireAdmin` + the RBAC license gate).
- **Pre-26.06.1 RBAC child rows (teams, roles, measurement_permissions, token_memberships)** attached to an org that the upgrade-seed re-aligns are **cascade-deleted** on the leader as a side effect of the realign — they live in local SQLite under the old AUTOINCREMENT parent id, which gets DELETEd as part of the seed's transaction. The cluster's authoritative state never observed them. Operator action: re-issue each affected team/role/measurement_permission/token_membership via the API post-upgrade. The seed logs a `Warn` at startup listing the counts per table so the operator knows what to re-issue. (Pre-26.06.1 tokens themselves are handled separately by Phase A and stay intact.)
- **Cascade-on-delete soft cap (Phase A.2 Item 2).** `DeleteOrganization` and `DeleteTeam` enumerate every descendant under `f.mu.Lock()` on the single-threaded `runFSM` goroutine. For a tenant with N teams × M roles × K measurement_permissions × J memberships, the cascade is O(N×(M×(K+J)+J)) map deletions inside one Raft apply. A pathological delete (~100k+ descendants) can hold the FSM apply goroutine long enough to (a) blow past the proposer-side 5 s `proposeTimeout` so the originating client sees an opaque timeout while the apply still completes in the background, and (b) back the apply pipeline up so later commands queue behind it and risk failing their own `proposeTimeout` budget. (hashicorp/raft runs `runFSM` async of heartbeats, so a long apply does NOT directly cost the leader its lease — the failure mode is client-visible timeouts and apply-pipeline backpressure, not lost leadership.) v26.06.1 ships a configurable proposer-side cap: `cluster.rbac.max_cascade_descendants` (env `ARC_CLUSTER_RBAC_MAX_CASCADE_DESCENDANTS`, default **50000**). The cluster-mode `DeleteOrganization` / `DeleteTeam` proposer counts the descendants in local SQLite before proposing; if the total exceeds the cap, it refuses with **HTTP 409 Conflict** + a clear operator-facing error (`cascade exceeds configured limit: <N> descendants under <entity> <id> (max <cap>); delete child entities first`) and increments `arc_cluster_rbac_cascade_rejected_total`. No Raft log entry is spent on a rejected cascade. `DeleteRole`'s 1-level cascade is not capped (it can't plausibly blow up the apply path). Operator workaround when 409 lands: `DeleteRole` / `DeleteTeam` the affected children first, then retry the parent delete. Operators with tenants larger than the cap who know their workload can raise it or set `cluster.rbac.max_cascade_descendants = 0` to disable the check entirely (escape hatch). The pre-check costs four small `COUNT(*)` queries against the local indexed columns — sub-ms at realistic cap values.

**Cluster-mode Delete returns "not found" for missing entities.** All five cluster-mode Delete entry points (`DeleteOrganization`, `DeleteTeam`, `DeleteRole`, `DeleteMeasurementPermission`, `RemoveTokenFromTeam`) run a local-SQLite existence pre-check before proposing. If the entity doesn't exist locally, the call returns the standard `"<entity> not found"` error (HTTP 404) without proposing — matching the OSS path's behaviour. On a follower with a stale local cache the pre-check may 404 for an entity just-created elsewhere not yet replicated; that race window is the same ~50ms eventual-consistency window Phase A documents, and 404 honestly conveys "doesn't exist to me yet." Pre-fix, cluster-mode Delete returned 200 OK for a non-existent entity because the FSM's `applyDelete*` treats not-found as an idempotent no-op (correct for log replay) and the proposer surfaced the FSM's nil return.

### Phase A.1 hardening from review (Gemini PR #458, internal review)

Phase A.1 went through 9 rounds of Gemini Code Assist review plus two internal deep-review passes before merge. A handful of fixes are worth calling out because they're either operator-visible or document an invariant the original implementation didn't make explicit:

- **`readBackAfterPropose` retry helper.** When a Create<X> RBAC method proposes via Raft, on the leader the apply callback has already fired by the time `Propose` returns. On a **follower**, however, `Propose` blocks only until the leader commits — the follower's local apply runs asynchronously on the `runFSM` goroutine and may not have hit local SQLite yet. The new helper retries the read-back with exponential backoff (0 → 10 → 25 → 50 → 100 → 200 → 400 ms, ~785 ms total, well under the 5 s `proposeTimeout`) on `sql.ErrNoRows`. Wired into all five Create paths (`CreateOrganization`, `CreateTeam`, `CreateRole`, `CreateMeasurementPermission`, `AddTokenToTeam`). The retry loop honours the request `context.Context` via a `select` arm on `time.NewTimer` + `ctx.Done()` plus a top-of-loop `ctx.Err()` check that catches pre-cancelled contexts and between-scan cancellations the select arm misses. Without this helper, a follower-side write could surface as a 500 to the API caller during the sub-50 ms window between log commit and local apply.
- **Concurrent-identical-Create race fix.** `CreateRole` and `CreateMeasurementPermission` have no schema-level UNIQUE on `(team_id, database_pattern, permissions)` / `(role_id, measurement_pattern, permissions)`. Two concurrent identical creates produce two real rows, and the original read-back used `ORDER BY created_at DESC LIMIT 1` — so the slower caller would read back the faster caller's row, both ending up with the same ID. Fixed by matching on `created_at = proposer's now` (Go monotonic nanosecond, unique per call across the process); the FSM stamps the proposer-supplied timestamp verbatim. Pinned by `TestProposer_CreateRole_ConcurrentIdenticalDistinctIDs` (8 goroutines, all get distinct IDs).
- **Snapshot Restore quarantines duplicate keys.** A corrupted or malformed snapshot can contain two organizations with the same name, two teams with the same `(org_id, name)`, or two memberships with the same `(token_id, team_id)`. Pre-fix the rebuilt secondary index silently overwrote — primary map held the loser, name/pair index pointed at the winner, FSM permanently inconsistent. Restore now detects duplicates, quarantines them (skip + `Error` log + `arc_cluster_rbac_rejected_total` increment) the same way it already handled malformed and orphan entries.
- **FSM apply atomicity on duplicate `ChangedFields`.** `applyUpdateOrganization` and `applyUpdateTeam` previously mutated the secondary index inside their `ChangedFields` loop, *before* the update was guaranteed to succeed. A pathological payload listing `"name"` twice would mutate the index in the first iteration, then trip the duplicate-check in the second iteration, and return an error — leaving the index pointing at a name the primary map had never been updated with. Fixed by staging the index mutation behind a `nameChanged` boolean and committing primary + index together only after the loop succeeds; a duplicate `"name"` entry is now a no-op rather than a corruption. Pinned by two regression tests feeding `ChangedFields: []string{"name", "name"}`.
- **Defensive nil-map init on all five nested-map writes in apply functions.** `applyCreateTeam`, `applyCreateRole`, `applyCreateMeasurementPermission`, and `applyAddTokenToTeam` (three sites) all write to nested maps that should always be present but could be nil after a corrupted restore or a future-buggy code path. All five sites now `if !ok || inner == nil { make(...) }` rather than the simpler `if !ok` guard.
- **Seed runs in a background goroutine and respects shutdown.** `SeedRBACFromLocalSQLite` originally ran synchronously inside the cluster wire-up — blocking the HTTP server for up to 30 s on a cold start where the Raft leader hadn't elected yet, long enough for Kubernetes liveness/readiness probes to time out and trigger restart loops. The seed now runs on a goroutine so HTTP comes up immediately; the seed's `context.Context` is cancellable via a `shutdownCoordinator` hook registered at priority 5 (fires before `PriorityHTTPServer = 10`) so a mid-startup SIGTERM cancels the seed cleanly rather than fighting the coordinator teardown. `HasLocalRBACState(ctx)` provides a cheap fast-exit so a cluster with no pre-A.1 RBAC rows skips the seed pass entirely.
- **`ctx` propagated everywhere.** All 13 OSS-path SQL calls in `RBACManager` now use `ExecContext` / `QueryRowContext`. The seed's per-org `Query` uses `QueryContext`. The 4 child-table `COUNT(*)` queries in the seed are now skipped (rather than flooding 4 useless `context canceled` Warn lines) if `ctx` cancelled mid-loop. `HasLocalRBACState` accepts `ctx`. The full Phase A.1 surface honours `context.Context` cancellation end-to-end.

Tests added: 31 FSM-level tests in `internal/cluster/raft/fsm_rbac_test.go` (apply-success + validation-rejection for each of the 13 command types; UNIQUE collision and parent-FK rejection coverage; the four-level cascade test `TestApplyDeleteOrganization_CascadesAllLevels`; cascade tests for `DeleteTeam` and `DeleteRole`; the cross-Phase test `TestApplyDeleteToken_CascadesMemberships`; snapshot round-trip preserving all 5 RBAC maps and rebuilding every secondary index; quarantine-on-restore tests for malformed, orphan, and duplicate-key entries; FSM atomicity pins for `applyUpdate{Organization,Team}` on duplicate `ChangedFields`). 18 round-trip tests in `internal/auth/cluster_rbac_proposer_test.go` covering each RBAC write path end-to-end through a fake proposer + RBACManager + local SQLite (including `TestProposer_DeleteNonExistent_ReturnsNotFound` covering all 5 Delete paths, `TestProposer_CreateRole_ConcurrentIdenticalDistinctIDs` for the race fix, and three `TestReadBackAfterPropose_*` tests pinning retry, ctx-cancel, and pre-cancelled-ctx behaviour). 5 two-node integration tests in `internal/cluster/auth_cluster_integration_test.go` (`TestTwoNodeRBAC_CreateOnAReadOnB`, `TestTwoNodeRBAC_DeleteOrgCascadesAcrossNodes`, `TestTwoNodeRBAC_TokenMembershipRoundTrip`, `TestTwoNodeRBAC_DeleteTokenCascadesMemberships`, and `TestRBACSeed_PopulatesOrgsFromLocalSQLite` including the upgrade-seed re-align assertion that the leader's SQLite agrees with the FSM on the org id post-seed, the pre-A.1 child-row cascade-delete pin, and the seed re-run idempotency check). 1 unit test in `internal/auth/rbac_seed_test.go` pinning the ctx-cancelled-mid-seed skip-child-counts path. Plus an updated wire-format pin (`TestProposalCommandTypesMatchFSM`) covering all 18 command types. All under `-race`.

### Pattern 2 Shared-Storage Multi-Writer (Enterprise) — PR1a + PR1b

26.06.1 adds support for **multiple `RoleWriter` nodes sharing a single object-storage backend** (S3, Azure Blob, MinIO) behind a load balancer. Pre-26.06.1 every cluster was single-writer; Pattern 2 multi-writer scales write acceptance horizontally and removes the writer as a single point of failure without operator-driven failover. (Pattern 1 multi-writer — per-shard ownership with replicated WAL — is a separate future initiative; the current shipping behaviour for Pattern 1 deployments is unchanged single-writer + reader-failover.)

**Enable** by setting `cluster.shared_storage_mode = true` (env `ARC_CLUSTER_SHARED_STORAGE_MODE`). Startup refuses to begin if: `cluster.enabled = false` (no cluster coordinator means singleton gate is nil, two such "standalone" nodes pointed at the same bucket would each run retention/CQ/delete); the storage backend is `local` (per-node filesystems cannot be shared); or the license lacks the `shared_storage_multi_writer` feature.

**Behaviour change in shared-storage mode:**

- **All `RoleWriter` nodes accept writes concurrently.** No leader-forwarding, no "primary writer" designation. Filename collision across writers is prevented by the existing per-process nanosecond suffix in `generateStoragePath`.
- **Singleton background tasks** (retention, continuous queries, delete coordinator, reconciliation) gate on the cluster Raft leader instead of on a singleton "primary writer." `IsPrimaryWriter()` returns `raftNode.IsLeader() && Role == RoleWriter` — the role check is load-bearing because every joining node becomes a Raft voter regardless of role, and without it a `RoleReader` or `RoleCompactor` that wins election would run singleton tasks against the shared bucket.
- **Writer failover is suppressed.** No `CommandPromoteWriter` proposals; no health-check loop trying to elect a new primary. The load balancer handles writer-crash recovery via its own `/ready` poll.
- **`/ready` endpoint now means "safe for the LB to route traffic here."** Returns 503 during startup (before WAL recovery completes — the existing recovery path is unchanged but now signals readiness through `/ready`) and during graceful shutdown (10s drain delay in clustered mode so LBs observe the 503 before the listener closes; 0s in standalone mode). `/health` (liveness) is unchanged.
- **The cluster Router's `RouteWrite` fallback now uses round-robin** (`selectWriter`) instead of always picking `writers[0]`. Previously, when `GetPrimaryWriter()` returned nil (the always-true case in shared-storage mode + the transient failover window in legacy mode), all forwarded writes from non-writer nodes piled on the first writer in the registry. Now they distribute across all healthy writers via a separate `writerIndex` counter that doesn't interleave-skew with reader rotation.

**No new operator-facing behaviour for single-writer Pattern 2 deployments.** Setting `cluster.shared_storage_mode = true` with `writer.replicas = 1` is functionally identical to today's single-writer shared-storage deployment — the single writer is its own Raft leader, the role gate trivially holds, and writer failover suppression has nothing to suppress.

**WAL crash recovery is unchanged and free for Pattern 2.** Arc's existing WAL recovery (`internal/wal/recovery.go` + the wiring at `cmd/arc/main.go:701-746`) already runs on every writer startup, replays un-flushed entries into the Arrow buffer, and deletes the WAL files on successful replay. A writer crash loses only the in-flight buffer (records that arrived in memory but had not yet been flushed to S3); records that completed the S3 PUT before the crash are durable; the WAL recovers anything un-flushed when the writer restarts.

**Forwarding-layer cleanup (PR1a + PR1b drive-by fixes).** `BuildHTTPRequest` in `internal/api/routing.go` had several pre-existing bugs surfaced during PR1a review and fixed alongside the multi-writer work: (a) `req.Header.Set` overwriting multi-value headers (Via, X-Forwarded-For, multiple Accept content types — `Set` was silently dropping every value except the last; replaced with direct map-write that preserves all values); (b) connection-specific hop-by-hop headers (Connection, Keep-Alive, Proxy-Authenticate, Proxy-Authorization, Te, Trailers, Transfer-Encoding, Upgrade) being forwarded along with the request when RFC 7230 §6.1 requires intermediaries to strip them (response path was already filtering via the same `isHopByHop` helper; request path now matches); (c) the upstream `Content-Length` header being forwarded alongside the body even though `net/http` sets `req.ContentLength` from the `*bytes.Reader` length and writes the wire-side header from that — duplicate or stale header risk; now filtered; (d) the upstream client's `Host` header lingering in the forwarded `req.Header` map even though `net/http` writes the wire-side `Host` from `req.Host` — observable to middleware/logging that reads `req.Header.Get("Host")` instead of `req.Host`; now filtered. All four are independent of the multi-writer work but were bundled into PR1a to avoid a separate follow-up PR.

**Deployment artifacts (PR1b):**

- `deploy/docker-compose/enterprise-shared/docker-compose.yml` — adds `ARC_CLUSTER_SHARED_STORAGE_MODE: "true"` to the shared env block. The existing 3-writer + 1-reader + Traefik LB topology already supported multi-writer on the routing side; the env var flip activates the new semantics.
- `deploy/docker-compose/enterprise-shared/docker-compose.override.smoke.yml` + `smoke.sh` — new smoke harness that builds Arc from source, boots the cluster, and runs one of three scenarios: `base` (ingest N records via Traefik LB → query reader → assert exact count), `non-leader-crash` (kill a non-leader writer mid-ingest → assert durability invariant: every HTTP-success record is queryable), `leader-crash` (kill the current Raft leader → assert Raft election + durability invariant). The crash scenarios accept in-flight buffer loss on the killed writer; the assertion is that no record whose HTTP POST returned 2xx is missing afterwards.
- `helm/arc-enterprise/templates/_helpers.tpl` — adds `ARC_CLUSTER_SHARED_STORAGE_MODE=true` to the cluster env helper, gated on `storage.mode=shared` (so it lands on every pod that opts into the shared-storage chart variant).
- `helm/arc-enterprise/values-shared-storage.yaml` + chart README — updated to document the multi-writer semantics and the `writer.replicas` sizing trade-offs (1 = dev, 2 = refused by `_validation.tpl` due to lack of failure tolerance — a Raft quorum of 2 stalls on any single-pod loss — 3+ = HA with one-failure tolerance on both the LB and Raft sides).

**Tests** (`internal/cluster/multi_writer_integration_test.go`): a 3-node real-Raft integration test that boots three `raft.Node`s (one bootstrap + two joiners), adds them as voters, then exercises three properties end-to-end: (1) the bootstrap writer + `RoleWriter` reports `IsPrimaryWriter()=true` and the two followers report `false`; (2) mutating the leader's local role to `RoleReader` flips `IsPrimaryWriter()` to `false` (the B1 regression catch — a non-writer that wins Raft election must not run singleton tasks); (3) stopping the leader triggers Raft re-election within ~1s; exactly one of the remaining writers becomes leader and reports `IsPrimaryWriter()=true`. Runs in ~1s under `-race`; verified 5x flake-free.

Plus 4 unit tests in `internal/cluster/coordinator_test.go` covering the `IsPrimaryWriter` matrix (legacy mode unchanged, shared-storage no-Raft / not-started-Raft / role-mismatch all return false), and 4 server tests for the new `/ready` semantics (starts not-ready, MarkReady flips to 200, MarkNotReady flips to 503, /health stays 200 throughout). 3 sub-tests in `internal/cluster/router_test.go` pin the round-robin writer selection: even distribution across 3 writers; writer rotation independent of reader rotation; and `RouteWrite` actually invokes `selectWriter` rather than the pre-PR1b `writers[0]` always-pick.

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
