# Arc v2026.09.1 Release Notes

> **Status:** In development.

## New: Apache Iceberg export (opt-in)

Arc can now publish its data as **Apache Iceberg tables** so any Iceberg-aware engine — Spark, Trino, DuckDB, Snowflake, PyIceberg — can query Arc's data directly, without going through Arc's API. This is the strongest form of the "your data, no lock-in" promise: Arc already writes open Parquet files you own; now those same files are also a standard Iceberg lakehouse table.

**How it works (and what it doesn't do):** Iceberg is a *table format* — a metadata layer over Parquet — not a new file format. Arc's ingest path is **completely unchanged**; nothing in the 20M+ rec/s write path is touched. A background reconciler periodically registers Arc's **existing** Parquet files into an Iceberg table by reference (Iceberg's `add_files` — **no data is copied or rewritten**) and keeps the table's file set in sync as compaction and retention change the underlying files. Because it registers files in place rather than re-exporting them, there is effectively no storage overhead beyond the small Iceberg metadata.

Enable it with one setting:

```toml
[iceberg]
enabled = true          # default false
```

Then point any engine at the table. Example with DuckDB:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('/path/to/data/arc/arc_<database>.db/<measurement>');
```

**Highlights:**
- **Zero-copy** — registers your existing Parquet, no rewrite (unlike export plugins that duplicate data).
- **Automatic maintenance** — Arc's compaction and retention are reflected into the Iceberg table; snapshots are expired on a retention policy so metadata stays bounded.
- **Schema evolution** — measurements that gain columns over time evolve the Iceberg table automatically; older files stay readable.
- **Cross-engine verified** — read-tested against DuckDB, PyIceberg, and Apache Spark (host and containerized).
- **Backup-aware** — the Iceberg metadata is included in Arc's backup/restore.
- **Efficient** — the reconciler skips measurements whose file set hasn't changed since the last pass (no wasted work at steady state).

**Configuration:**

| Key | Default | Meaning |
|---|---|---|
| `iceberg.enabled` | `false` | Enable the export reconciler. |
| `iceberg.reconcile_interval` | `300` | Seconds between reconcile passes. |
| `iceberg.retain_snapshots` | `10` | Iceberg snapshots (and metadata versions) kept per table; older are expired. |
| `iceberg.namespace_prefix` | `"arc"` | Iceberg namespace prefix; tables land in `<prefix>_<database>`. |
| `iceberg.warehouse` | *storage root* | Where table metadata is written (defaults alongside the data). |
| `iceberg.catalog_db_path` | *shared auth DB* | SQLite catalog location. |

Env vars follow the usual pattern, e.g. `ARC_ICEBERG_ENABLED=true`.

**v1 scope / limits:** Iceberg export requires a **local storage backend** and is **not compatible with cold-tier tiering** (a file migrated to S3 would leave the Iceberg table) — Arc refuses to start with both enabled. `iceberg.retain_snapshots` must be at least 1; 0 is rejected rather than silently meaning "keep every snapshot forever". Directory-based readers are supported via an emitted `version-hint.text`; catalog-based discovery works today with PyIceberg / the SQLite catalog. If `iceberg.warehouse` points outside the storage root the discovery files cannot be addressed and are not written — Arc warns once per table, and catalog-based readers are unaffected. See the full guide in the docs (**Integrations → Apache Iceberg**) for engine-by-engine instructions, the trade-offs, and the cluster single-writer note.

**Backups cover the catalog wherever it lives.** Backup copies the Iceberg warehouse metadata alongside the Parquet data, and now also the Iceberg SQL catalog itself when `iceberg.catalog_db_path` points somewhere other than the shared database. The catalog holds every table's schema and snapshot pointers, so a backup without it restores data whose tables no longer resolve. Restores of older backups that predate this are unaffected — a missing catalog copy is skipped, not an error.

## New: tunable compaction batch size

Compaction splits a large partition into batches, each becoming an independent job with its own output file. That batch size was a hardcoded constant (30 files); it is now configurable:

```toml
[compaction]
max_files_per_batch = 30   # default; valid range [2, 500]
```

Env var: `ARC_COMPACTION_MAX_FILES_PER_BATCH`.

This is a **file-count** bound, not a byte bound — compacted output size tracks input file size, which follows the ingest buffer settings. Lowering it yields smaller, independently-transferable compacted files, which matters when those files are shipped over a constrained or intermittent link (edge and field deployments). The cost is more compaction jobs per partition, and in cluster mode proportionally more Raft manifest entries — at `5`, a 600-file partition produces 120 manifest entries instead of 20.

Out-of-range values fall back to the default with a startup warning rather than failing. **`1` is not usable** and is treated as out of range: compaction's adaptive retry rejects any batch below two files, so a batch size of one would fail every batch of every partition.

## Groundwork: edge-to-cloud sync (not yet usable)

Arc runs at the edge today — a standalone binary with local storage in a vehicle, a factory cell, or a forward deployment — with no first-class way to ship that data to a central Arc. Backup is a full DR snapshot rather than incremental sync, and Parquet import re-ingests rows through the write path, which breaks the end-to-end checksum and double-counts on retry.

**Edge sync** ([#569](https://github.com/Basekick-Labs/arc/issues/569)) addresses this by shipping immutable Parquet *files* from a spoke (edge) to a hub (central Arc): the spoke initiates, the hub verifies each file's SHA256 before committing it, and re-delivery of an already-received file is a no-op. Connectivity is treated as the exception rather than the norm — discovery is a single batched round-trip regardless of backlog size, and transfers resume from a byte offset when a link drops mid-file.

**Nothing is user-visible in this release, on disk or otherwise.** The work is landing as a sequence of reviewed changes, and this release contains only the first: the spoke-side ledger that tracks which files have reached which hub. No configuration key enables it, no endpoint exposes it, and no startup path constructs it — so its SQLite schema is not even created yet. It is recorded here so the release history matches the work, not because there is anything to use or configure.

Manual export/import will be OSS when it ships; the automatic scheduled agent will be an Enterprise feature.

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

### Replacement-scan RBAC bypass: reject single-quoted strings in table position ([GHSA-w8x2-cccw-25f7](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-w8x2-cccw-25f7))

An authenticated tenant on an RBAC-enabled multi-tenant instance could read any Parquet file inside the allow-listed storage root — including other tenants' databases — by writing a **bare single-quoted string in table position**: `SELECT * FROM '/data/arc/db2/secrets/*.parquet'`. DuckDB resolves such a string as a *replacement scan* that reads the file directly, with **no function name**. Arc's two query-authorization controls both key on function names — the I/O-function denylist (`read_parquet`, `parquet_scan`, `glob`, …) and the RBAC table extractor — so the bare string slipped past the denylist, and the extractor masked the quoted path to a placeholder before matching, so RBAC never saw the foreign path. The comma form (`SELECT b.s FROM cpu, '/data/arc/db2/secrets/*.parquet' b`) needed only a single legitimate grant.

This is an incomplete-fix residual of [GHSA-93cm-2v4m-c56c](https://github.com/Basekick-Labs/arc/security/advisories/GHSA-93cm-2v4m-c56c): the same root cause (authorization keyed on function names), via the one table-position syntax that carries no function name. It is a within-allow-list cross-database read — it does **not** escape the DuckDB sandbox to arbitrary OS files (`enable_external_access=false` holds).

The fix rejects, in `ValidateSQLRequest` (before any transform or RBAC check runs), any single-quoted string standing where a table reference belongs — after `FROM`/`JOIN`, or continuing a `FROM` clause's table list via a cross-join comma — while leaving single-quoted strings used as values (`WHERE`, `IN`, function arguments) and legitimately quoted *identifiers* (`FROM "my table"`) untouched. Arc's own transform layer, which emits `read_parquet('…')` under the caller's identity, runs after validation and is unaffected. The denylist test matrix now covers the replacement-scan forms — single-file, glob, comma cross-join, `JOIN`, subquery, and the no-whitespace (`FROM'…'`) variant.

Reachable only when RBAC is enabled (the multi-tenant authorization boundary); no CVE is being filed, as it is a residual of GHSA-93cm scoped to within the allow-list, tracked under the advisory above. Reported by [@arpitjain099](https://github.com/arpitjain099).

## Bug fixes

### Compaction no longer treats a storage failure as "no manifests exist" ([#314](https://github.com/Basekick-Labs/arc/issues/314))

`ListManifests` discarded every error from the storage backend and returned an empty list, with no log line. "No manifests exist" and "we could not find out" are opposite instructions to the caller: an empty manifest set means nothing is being compacted and the candidate may proceed, while a failed lookup means the files in flight cannot be identified.

The candidate filter already had the correct guard — it skips the partition when the manifest lookup fails, explicitly *"to avoid re-compaction"* — but swallowing the error inside `ListManifests` made that guard unreachable. A transient failure (S3 throttling, an expired credential, a network blip) therefore let Arc treat files already claimed by an in-flight compaction as untracked, and compact them a second time.

Errors now propagate. A missing manifest directory remains a non-error: the local backend skips directories that do not exist, and the object-store backends return an empty listing for a prefix with no objects, so any error reaching this layer is a real failure.

### Ingest no longer drops columns that are entirely null in a batch ([#337](https://github.com/Basekick-Labs/arc/issues/337))

A column whose every value was null in a batch was dropped, because no type can be inferred from it. In most cases readers absorbed this — queries and compaction union schemas by name — but a column that is null in **every** batch never appeared in any file, so querying it failed with `Binder Error: Referenced column "depth" not found` instead of returning NULLs. Realistic triggers: an optional field absent for a whole batch, or a sensor reporting null through an outage.

Such columns are now written as an all-null placeholder that keeps the column present and every value NULL. A later batch carrying real values still infers its own type, so nothing is pinned to the placeholder. The `time` column is exempt and an all-null time is now rejected outright: a VARCHAR time column makes a partition un-compactable.

### `fdatasync` WAL sync mode is now actually fdatasync on Linux ([#305](https://github.com/Basekick-Labs/arc/issues/305))

`wal.sync_mode` accepted `fdatasync`, and it is the default when the WAL is enabled, but both `fsync` and `fdatasync` called the same full `Sync()`. Operators who selected the balanced mode silently got the strictest one.

Linux now uses a real `fdatasync(2)`, which skips the metadata-journal flush (retried on `EINTR` so an interrupted call cannot report durability it did not achieve). Go does not expose `fdatasync` on macOS or Windows, so those platforms honestly fall back to a full `Sync()` and now **log that fact once at startup** rather than reporting a mode they are not performing. The startup log also carries `fdatasync_supported` so the effective behavior is visible.

Expect no measurable throughput change: WAL syncs are driven by a 100 ms ticker rather than per-write, so there are at most ~10 per second regardless of ingest rate. This is a correctness and honesty fix, not a performance one.

### Features share the auth manager's SQLite handle instead of opening their own ([#329](https://github.com/Basekick-Labs/arc/issues/329), [#562](https://github.com/Basekick-Labs/arc/issues/562))

Arc keeps auth, audit, tiering, governance, retention, continuous-query and MQTT metadata in a single SQLite file (`auth.db_path`, default `./data/arc.db`). Each of those features opened its **own** connection to that file, so a default deployment ran six independent connection pools — each capped at one connection, since SQLite has a single writer — competing for the same write lock. They now borrow the auth manager's existing handle instead.

Three of those handles were also **leaked**: nothing closed the audit, tiering or governance connections on shutdown. When these features now open their own handle (see below) it is closed on every path, including the failure paths that previously returned early.

**Retention and continuous queries keep their own config keys.** `retention.db_path` and `continuous_query.db_path` merely *default* to the auth database; an operator may point either at a different file and expects a genuinely separate database. Those features borrow the auth handle only when the configured path resolves to the same file — compared as a cleaned absolute path with symlinks resolved, so `./data/arc.db` and an absolute path to that file are correctly recognized as one database. Point them elsewhere and they open and own a separate handle exactly as before.

One behavior change worth noting: the auth connection enables SQLite foreign-key enforcement, which retention's and CQ's own connections did not. Their schemas declare foreign keys (`retention_executions` → `retention_policies`, `continuous_query_executions` → `continuous_queries`) and both delete child rows before parents, so enforcement is satisfied — it now also rejects the orphan-creating order, which the code never used.

MQTT initialization moved after auth initialization in startup so the handle exists to be borrowed. MQTT's repository tracks whether it owns its handle and closes it only if so — MQTT shuts down at ingest priority, well before the auth manager closes the shared handle, so closing a borrowed connection there would have taken the database out from under every component still shutting down.

**Auth-disabled deployments are unaffected in behavior:** these features are each enabled independently of auth, so when auth is off they open and own a handle exactly as before. That path is now hardened to match what the auth manager does: the parent directory is created, and the database file is pre-created with owner-only (0600) permissions rather than being left at the process umask (typically 0644, world-readable). The file holds audit logs, tiering metadata and encrypted MQTT broker credentials. Previously, an MQTT-enabled deployment with auth disabled and no existing data directory **failed to start**.

### Audit log disk reclamation no longer runs a no-op vacuum

Audit set `PRAGMA auto_vacuum = INCREMENTAL` during schema init, then ran `PRAGMA incremental_vacuum` after each retention cleanup. Because audit shares the auth database, whose tables already exist by then, the pragma was rejected — `auto_vacuum` can only be changed on an empty database — and SQLite reports no error for the rejection, so the setting silently stayed at NONE and every subsequent vacuum was a no-op.

Both statements are removed rather than repaired. `audit_logs` is written on every request, and vacuuming a continuously-written table causes write amplification: the vacuum shrinks the file and the next insert re-grows it. Pages freed by the retention `DELETE` are reused by later inserts, which is what actually bounds file growth. No operational change — the vacuum was already doing nothing.

### Compaction subprocesses no longer drop the S3 prefix ([#560](https://github.com/Basekick-Labs/arc/issues/560))

Compaction runs in a forked subprocess for DuckDB memory isolation, and the child rebuilds its storage backend from the parent's `Type()` and `ConfigJSON()`. `S3Backend.ConfigJSON()` emitted `prefix`, but the subprocess's parse struct had no matching field, so `json.Unmarshal` silently discarded it and the rebuilt backend was left with an empty prefix.

Because the prefix is applied to every key the S3 backend touches — read, write, delete, and list — a compaction subprocess on a deployment with `storage.s3_prefix` set operated against the **bucket root** instead of the configured prefix, with no error raised. Only deployments that set a non-empty prefix were affected; it defaults to empty, which is why "prefixed" and "unprefixed" were the same string everywhere the code was exercised.

The prefix is now parsed and forwarded. A round-trip test drives the real reconstruction path and compares the rebuilt backend's configuration against the parent's, so any field added to `ConfigJSON()` in future without a matching parse field fails immediately rather than silently.

### Removed the unused `ResilientBackend` storage wrapper ([#320](https://github.com/Basekick-Labs/arc/issues/320))

`internal/storage/resilient.go` provided a retry + circuit-breaker wrapper around a storage backend, but nothing ever constructed one: no call sites in `cmd/` or `internal/`, no tests, no type assertions. It dates to the original Go migration and was never wired in. Having drifted behind the `Backend` interface — missing `ReadToAt`, `StatFile`, `Type`, and `ConfigJSON` — it no longer satisfied the interface it was written against, which is what surfaced it.

The file is removed rather than completed. Retry and circuit-breaking around cloud storage remain worth having, but an unused, untested wrapper would simply drift again with the next interface change; a future implementation should be written against the interface as it stands then, and actually wired in. No behavior change — the code was unreachable.

### Auth `last_used_at` updates no longer outlive shutdown ([#325](https://github.com/Basekick-Labs/arc/issues/325))

Every token verification that missed the auth cache spawned a fire-and-forget goroutine to run `UPDATE api_tokens SET last_used_at = ?`. Nothing tracked those goroutines, so `AuthManager.Close()` could close the database out from under one still in flight; it then failed with `sql: database is closed` and logged at **Error** level, making an otherwise clean shutdown look like a failure. The update itself was also lost.

The same pattern had a second cost independent of shutdown. The auth database runs with `SetMaxOpenConns(1)` — SQLite has a single writer — so a burst of verifications against distinct tokens could pile an unbounded number of goroutines onto that one connection, competing with the live authentication queries sharing it.

Both are now handled by a single writer goroutine fed by a bounded queue. Verification hands off the update and returns immediately, so the authentication hot path never blocks; at most one `UPDATE` is ever in flight; and `Close()` drains the queue before closing the database, so pending updates are persisted rather than lost. If the queue is full the update is dropped (logged at debug) rather than blocking a request — `last_used_at` is a coarse "when was this token last seen" statistic, and under a burst large enough to fill the queue the dropped updates carry essentially the same timestamp as the queued ones.

Follow-up cleanup, no runtime behavior change: the channel governing the auth manager's background goroutines was renamed `cleanupDone` → `shutdown`, since it stops both the cache janitor and the `last_used_at` writer, not just the janitor. The old name read as though it covered only cache cleanup — which never touches the database — and that reading is part of why the writer was left untracked in the first place. Separately, the auth package's `app.Test` calls now pass an explicit timeout: PBKDF2 verification costs ~56ms normally but ~1.07s under the race detector, which exceeded Fiber's 1000ms default and made `go test -race ./internal/auth/` fail deterministically. The package now passes under `-race`.

### Compaction batch splitting no longer produces aliased file slices ([#292](https://github.com/Basekick-Labs/arc/issues/292))

`SplitCandidateIntoBatches` partitioned a candidate's file list into batches using `c.Files[start:end]` sub-slices, which share the original's backing array — so every batch pointed into the same underlying memory. The same pattern existed in `compactFilesAdaptively`, which split a failed batch in half with `files[:mid]` / `files[mid:]`.

No current code path mutates a batch's `Files` in place, so this was latent rather than an active source of corruption — compaction jobs never produced wrong file lists because of it. But it left every future caller one in-place sort or append away from silently rewriting sibling batches, with no error or log signal.

Both sites now copy into independent backing arrays, including the single-batch (`len(Files) <= DefaultMaxFilesPerBatch`) path that previously returned the caller's slice unchanged. Batch isolation no longer depends on file count: mutating any returned batch cannot affect another batch or the original candidate.

### Compaction batch splitting no longer strands a sub-minimum remainder

Splitting a partition into batches could leave a trailing batch of one file — 31 files at the default batch size of 30 produced batches of 30 and 1. `compactFilesAdaptively` rejects any batch below two files on its first attempt (`compaction failed: batch size 1 below minimum 2`), so that remainder failed every cycle and its file never compacted. The partition's tail stayed permanently uncompacted, with the only symptom a per-batch failure log.

A trailing remainder smaller than the two-file floor is now folded into the final batch, which overshoots the configured size by at most one file. Every emitted batch is now large enough to actually compact.

### `compaction.target_size_mb` removed (it never did anything)

The compaction tiers carried a `TargetSizeMB` field, defaulted to 512 (hourly) and 2048 (daily), plumbed through five structs and the parent↔subprocess IPC, and reported as `target_size_mb` on `GET /api/v1/compaction/stats`. Nothing ever read it: the file-selection path has no size information (`Candidate` carries no file sizes), and the compaction `COPY` never emitted a `FILE_SIZE_BYTES` clause. Compacted file size was, and is, governed by the batch file count.

There was no config key for it, so no deployment could have set it — the removal cannot change anyone's behavior. **The `target_size_mb` field is gone from the `tiers[]` entries of `GET /api/v1/compaction/stats`**; the tier-initialization log lines no longer include it either.

### Backup no longer loads entire files into memory ([#322](https://github.com/Basekick-Labs/arc/issues/322))

`CreateBackup` read each Parquet file entirely into memory (`dataStorage.Read` → `backupStorage.Write`) before writing it to backup storage. With large Parquet files (the default 1M buffer size produces ~10–14MB files each; partitions can have hundreds), a backup of a moderately-sized database could spike process memory to gigabytes, triggering OOM kills or swapping.

Backup now streams each file through a temp file: `dataStorage.ReadTo` writes the source into a temp file on disk, then `backupStorage.WriteReader` streams the temp file to backup storage. Peak memory per file is bounded by the I/O buffer size (typically 32KB) instead of the file size. The SQLite metadata backup uses the same streaming pattern (`os.Open` → `WriteReader`) instead of `os.ReadFile`. The pattern matches the existing restore path (`streamRestoreFile`), which already streamed in both directions.

Failure handling is deliberately narrow about what it tolerates. Only a failure to **read the source file** is skippable — that file may legitimately have been removed by compaction or retention between the listing and the copy, and aborting a whole backup over that benign race would be wrong. Every other failure (temp file creation, seek, backup-storage write) is fatal and aborts the backup: those indicate a broken environment, not a race, and a backup that silently omits files while reporting success is worse than one that fails loudly.

Skipped files are now counted in the backup manifest (`skipped_files`) and in the progress API, so an incomplete backup is visible without reading logs — the manifest's `total_files` describes what was inventoried, not necessarily what was stored. If **every** file proves unreadable, the backup now fails rather than producing an empty backup that reports success.

Skipping is now bounded ([#556](https://github.com/Basekick-Labs/arc/issues/556)). Tolerating a skipped file exists for one narrow race — a file removed by compaction or retention between the listing and the copy — which touches a handful of files at the tail of a run. A **large fraction** of the backup failing to read is a different event (throttling, credential expiry, a storage outage), and returning a fraction of the data as a successful backup is how an operator discovers the gap at restore time instead of at backup time. If more than 10% of data files are unreadable, the backup now fails with `source storage may be degraded` rather than reporting success. The skip count is still recorded on the failed run.

A failed write no longer leaves an orphaned `.part` staging file in backup storage ([#555](https://github.com/Basekick-Labs/arc/issues/555)). The local storage backend deliberately preserves `<path>.part` on failure so the file-replication puller can resume from the last committed byte, but backup has no resume path — a retried backup starts over under a fresh backup ID — so the staging file was unreferenced garbage: never read, never listed as a backup, and holding disk equal to the bytes transferred before the failure. Cleanup is best-effort and never masks the original write error.

**Operational note:** streaming trades memory pressure for temp disk usage. Each file being backed up requires temp disk space equal to its size. In container environments with a small `/tmp` (tmpfs), set `TMPDIR` to a volume with sufficient space, or ensure the default temp directory has room for the largest Parquet file. If the temp directory is unusable the backup fails with a clear error rather than silently skipping files.

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

The forwarding-header and loop-guard changes are cluster-only; the partition-pruner DoS fix (#536) applies to **every** deployment mode, since the pruner runs on every query. The replacement-scan RBAC fix (GHSA-w8x2) applies wherever RBAC is enabled — it is the multi-tenant authorization boundary.

| Deployment | Forwarding-header / loop-guard changes | Partition-pruner DoS fix (#536) | Replacement-scan RBAC fix (GHSA-w8x2) |
|---|---|---|---|
| Single-node / OSS standalone (no cluster router) | **No.** The forwarding path never executes; behavior is byte-for-byte identical to 26.06.3. | **Yes.** Applies to every query. | **Only if RBAC is enabled.** The table-position check runs on every query, but the cross-tenant read it prevents requires RBAC. |
| Clustered, homogeneous nodes (every node can serve every request) | Header-stripping applies to forwarded requests; loop-guard reorder is a no-op because nodes serve locally. | **Yes.** Applies to every query. | **Only if RBAC is enabled.** |
| Clustered with role separation (reader / writer / compactor) | Header-stripping applies; a spoofed or looped `X-Arc-Forwarded-By` on a non-capable node now returns `508` instead of a failed local attempt. | **Yes.** Applies to every query. | **Only if RBAC is enabled.** |

## Upgrade notes

1. **No configuration change required.** Drop in the new binary; existing `arc.toml` and license keys work as-is. The partition-pruner limits (#536) are hardcoded safety bounds, not tunable config keys.
2. **No API or on-disk format changes.** Reads, queries, and storage layout are untouched. Queries with a start date earlier than `1970-01-01` are pruned from the epoch forward; since Arc stores no pre-epoch data, results are unchanged.
3. **Clustered operators:** if any external tooling deliberately sets `X-Arc-Forwarded-By`, `X-Real-IP`, or `X-Forwarded-*` headers on requests to Arc and expects them to survive an inter-node forward, note that these are now stripped on the forwarding hop and re-established by Arc. Client IP has never been derived from these headers, so log/audit attribution is unchanged.
4. **Active licenses keep working.** No re-activation required.
5. **Nothing changes on disk or in the database.** The edge-sync groundwork above defines two SQLite tables (`sync_ledger`, `sync_history`), but no startup path constructs the ledger, so the schema is never created and the tables do not appear in `auth.db_path`. They will be created by the release that first wires edge sync into the server.

## Dependencies

No product dependency changes in this release. GoFiber remains at `v2.52.13`; Dependabot alert #12 (CVE-2026-45045) is addressed by the in-code hardening above rather than a dependency bump, because no patched v2 release exists and the vulnerable code path (`middleware/proxy.BalancerForward`) is not present in Arc's build graph.
