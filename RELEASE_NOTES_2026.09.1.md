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

## New: compaction subprocess resource bounds (`compaction.memory_limit`, `compaction.threads`)

Compaction runs each job in an isolated subprocess so DuckDB's memory is fully returned on exit — but the *peak* was under-constrained. Each subprocess inherited the full `database.memory_limit`, and with the default `max_concurrent = 2`, compaction alone could reach **2× the configured limit on top of the main process's own DuckDB** — the RSS spike operators saw during backfill catch-up, when months of partitions become candidates in a single cycle. Each subprocess also used DuckDB's default of *all* CPU cores, competing with ingest and query work (and sort/scan buffers scale with threads, so this compounded the memory peak).

Two new keys bound this:

```toml
[compaction]
memory_limit = ""   # per-subprocess DuckDB memory limit; "" (default) = auto
threads = 0         # per-subprocess DuckDB threads; 0 (default) = auto
```

Env vars: `ARC_COMPACTION_MEMORY_LIMIT`, `ARC_COMPACTION_THREADS`.

**Auto defaults:** `memory_limit` derives as `database.memory_limit / max_concurrent`, so all concurrent compaction jobs together stay within roughly one `database.memory_limit` regardless of concurrency (an operator running `database.memory_limit = "8GB"` with default concurrency now gets 4GB per subprocess instead of 8GB each). `threads` defaults to half the CPU cores, minimum 1 — the two default subprocesses together use about one machine's worth of cores. Accepts absolute sizes with a unit (`"8GB"`, `"512MB"`, `"0.5GB"`); percent and unit-less forms are rejected at startup because DuckDB's `SET memory_limit` does not support them, as are other invalid values. The effective values are logged at startup (`subprocess_memory_limit`, `subprocess_threads`) and by each subprocess as it applies them.

Compaction subprocesses also now spill to a managed location: a `duckdb-spill/` directory inside the job's own temp directory (under `compaction.temp_directory`), instead of DuckDB's default `.tmp` relative to the server's working directory. Spill files are covered by the existing job cleanup and crash sweeps, so they can never outlive the job — and they land on the volume you sized for compaction, not wherever Arc was started from.

## New: tunable compaction batch size

Compaction splits a large partition into batches, each becoming an independent job with its own output file. That batch size was a hardcoded constant (30 files); it is now configurable:

```toml
[compaction]
max_files_per_batch = 30   # default; valid range [2, 500]
```

Env var: `ARC_COMPACTION_MAX_FILES_PER_BATCH`.

This is a **file-count** bound, not a byte bound — compacted output size tracks input file size, which follows the ingest buffer settings. Lowering it yields smaller, independently-transferable compacted files, which matters when those files are shipped over a constrained or intermittent link (edge and field deployments). The cost is more compaction jobs per partition, and in cluster mode proportionally more Raft manifest entries — at `5`, a 600-file partition produces 120 manifest entries instead of 20.

Out-of-range values fall back to the default with a startup warning rather than failing. **`1` is not usable** and is treated as out of range: compaction's adaptive retry rejects any batch below two files, so a batch size of one would fail every batch of every partition.

## Faster ingest, part 2: typed msgpack decode (+30% on top of the dictionary change)

The msgpack columnar write path now decodes payloads directly into typed column
arrays, eliminating the per-value interface boxing that previously dominated
decode CPU and fed GC pressure. Combined with the dictionary change below, sustained
ingest on the IOT benchmark moved from 20.6M records/sec (26.06.3) to **34.0M
records/sec — 2,043,451,000 records in a 60-second run** (p50 0.29ms, p99 1.40ms;
paired A/B runs attribute +30% to this change alone, peaking at 35.5M). The
compressed variants ride the same path after decompression: gzip 19.7M → 24.6M,
zstd 20.2M → 24.9M (+23-24%).

There is no configuration and no wire-format change. The typed decoder handles the
standard single-map columnar payload (`{m: "...", columns: {...}}`) and
transparently falls back to the previous decoder for batch/array/row formats,
deployments with configured decimal columns, and any payload shape it does not
recognize — the previous decoder remains the authority on what is accepted or
rejected, so client-visible semantics are unchanged. Decoder statistics
(`/api/v1/write/msgpack/stats`) expose `typed_decode_hits`/`typed_decode_misses`
so operators can confirm the fast path is engaged.

## Faster ingest: Parquet dictionary encoding at write time is now off by default

Sustained msgpack ingest throughput improves **~26%** (20.65M → 26.1M records/sec — 1.56 billion records in a 60-second run — on the IOT sustained-load benchmark, Apple M3 Max, 12 workers) by no longer dictionary-encoding Parquet columns at ingest time:

```toml
[ingest]
use_dictionary = false        # default; was true
numeric_dictionary = false    # default (only relevant with use_dictionary = true)
```

Env vars: `ARC_INGEST_USE_DICTIONARY`, `ARC_INGEST_NUMERIC_DICTIONARY`.

**Why this is safe:** ingest files are transient staging — hourly and daily compaction rewrite them through DuckDB, which re-encodes every column with its own adaptive dictionary/compression choices regardless of how the source files were encoded. Dictionary-encoding at ingest paid a hash-table insert plus an interface allocation for every value (~8–10% of write CPU in profiles) to compress files that were about to be rewritten anyway. Long-term storage efficiency is unchanged; the tradeoff is a temporarily larger uncompacted hot partition (up to ~2× for highly repetitive data) until the next compaction pass.

The write path also profiles GC-bound at sustained 20M+ records/sec (interface boxing across the decode → buffer → flush pipeline); operators chasing peak sustained throughput on memory-rich hosts can additionally set `GOGC=200-300` (measured +7% and flat RPS over time, at roughly double the resident memory). A typed-column ingest pipeline that removes the boxing entirely is planned follow-up work.

**Restoring the old behavior:** `use_dictionary = true` re-enables dictionaries for string columns only (a middle ground measured at +12% over the old default with tag columns still dictionary-compressed); adding `numeric_dictionary = true` restores the exact pre-26.09.1 all-columns encoding.

## WAL reliability improvements ([#594](https://github.com/Basekick-Labs/arc/issues/594), [#590](https://github.com/Basekick-Labs/arc/issues/590))

This release includes a set of fixes that make WAL-based crash recovery
significantly more robust. If you run with `wal.enabled = true` (off by
default), we recommend picking up 26.09.1.

**#594 — startup recovery could interfere with the active WAL file.** WAL
recovery at startup scanned the WAL directory after the writer had already
created its current file and could clean that file up as an empty leftover,
which prevented WAL entries written during the session from being available
for crash recovery later. Startup recovery now excludes the active file
(the periodic recovery path already did), and the periodic path gained a
minimum-file-age guard against the same interaction during file rotation.

**#590 — replayed data now behaves exactly like live-ingested data.** WAL
crash replay and cluster WAL replication feed the original client bytes back
through ingestion, and previously skipped some of the live decode path's
post-processing. Now aligned:

- Timestamp units are normalized on replay, so clients sending second- or
  millisecond-precision timestamps recover into the correct time partitions.
- Clients that omit the `time` column have timestamps regenerated on replay
  (stamped at replay time rather than original ingest time — an inherent
  semantic for omitted timestamps).
- Strings are UTF-8-sanitized on replay, matching live ingest.

Additionally, a single problematic WAL entry no longer stops replay of the
remaining entries in its file (the failure is counted in recovery stats).
These improvements also apply to Enterprise cluster WAL replication, which
shares the same ingestion path on replica nodes.

## JSON and msgpack query responses honor Accept-Encoding (zstd/gzip)

`/api/v1/query` and `/api/v1/query/msgpack` now compress responses when the
client asks via the standard `Accept-Encoding` header (zstd preferred over
gzip — send `Accept-Encoding: zstd, gzip` for the best of both). No client
code changes: curl, browsers, `requests`, and Grafana negotiate this
automatically; clients that don't send the header get byte-identical
responses to before (verified: zero overhead on the identity path).

Measured on a 5M-row result (M3 Max): JSON **405.6MB → 128.1MB (−68%)** with
zstd at +8% server CPU; msgpack 184.0MB → 113.9MB (−38%). Encoders are
pooled (zstd fastest level, single-threaded) — steady-state allocation cost
is near zero.

**When it helps:** network-constrained clients — at 1Gbps the JSON transfer
above drops from 3.2s to 1.0s. On loopback or very fast links, compression
adds latency; simply don't send the header. Two secondary effects on
compressed streams: progressive delivery is chunkier (the encoder buffers
~128KB blocks), and mid-stream client-disconnect detection is correspondingly
slower for slow-trickling queries. This does not change query execution time
— only transfer.

## Arrow IPC egress: opt-in dictionary encoding and buffer compression

The Arrow IPC query endpoint (`/api/v1/query/arrow`) can now halve its wire
size, opted in per request:

- `x-arc-arrow-dictionary: true` — low-cardinality string columns (symbols,
  hostnames, sides) are dictionary-encoded in the stream: the dictionary is
  re-sent only when it grows and each row carries a 4-byte index instead of
  the repeated string. Column selection is adaptive (first-batch cardinality
  analysis); high-cardinality strings (URLs, IDs) stay plain. The affected
  columns arrive as standard Arrow dictionary arrays — verified readable by
  pyarrow, polars, and pandas. One caveat: column selection is decided on
  the first batch, so a query ordered BY a medium-cardinality string column
  can qualify a column whose dictionary then grows large over the stream —
  Arc logs a warning when that happens; omit the header for such shapes.
- `x-arc-arrow-compression: zstd` (or `lz4`) — standard Arrow IPC buffer
  compression, decompressed natively by Arrow clients.

Measured on a 500M-row trades table (5 columns, 8 parallel readers, M3 Max):
**39.0 → 19.4 bytes/row (−50%) with both enabled** — 19.5GB → 9.7GB on the
wire for the same result set.

**When to use it:** these encodings trade server/client CPU for wire bytes.
On loopback or very fast links, leave them off (the extra encode/decode work
lowers rows/sec). On network-constrained links they win decisively — at
1Gbps, transmitting the 500M-row result takes 156s plain vs 78s with
dictionary+zstd, roughly 2× faster end-to-end. Rule of thumb: enable both
for remote analytical clients pulling large result sets; leave off for
same-host consumers. Existing clients are completely unaffected — without
the headers, the stream is byte-identical to before.

## Insertion-order preservation is now configurable, and off by default

Arc previously forced DuckDB's `preserve_insertion_order=true`, which makes queries **without** an `ORDER BY` return rows in file/insertion order — at the cost of order-preserving buffering in the engine. That setting is now configurable and defaults to **false**:

```toml
[database]
preserve_insertion_order = false   # default; set true for pre-26.09.1 behavior
```

Env var: `ARC_DATABASE_PRESERVE_INSERTION_ORDER`.

The default follows SQL semantics (row order without `ORDER BY` is unspecified) and DuckDB's guidance that disabling order preservation can reduce memory usage and unlock additional parallelism on large result materializations. On ClickBench-style aggregation suites — where nearly every query carries an explicit `ORDER BY` — measured throughput is unchanged; the setting matters for large un-ordered scans and exports.

**Behavior change:** queries without an explicit `ORDER BY` may now return rows in any order, as SQL semantics allow. Queries with an `ORDER BY` are completely unaffected. If a dashboard or integration relies on implicit insertion order (e.g. `SELECT * FROM cpu LIMIT 10` expecting oldest-first), either add an explicit `ORDER BY time` (recommended) or set `preserve_insertion_order = true`.

Internal write paths that rebuild parquet files keep the same ordering behavior as before this change: compaction with configured `sort_keys` sorts explicitly, while sort-keys-less compaction and DELETE file rewrites force order preservation on their own database session (session-scoped, so concurrent queries are unaffected).

## New: edge-to-cloud sync (manual)

Arc runs at the edge today — a standalone binary with local storage in a vehicle, a factory cell, or a forward deployment — with no first-class way to ship that data to a central Arc. Backup is a full DR snapshot rather than incremental sync, and Parquet import re-ingests rows through the write path, which breaks the end-to-end checksum and double-counts on retry.

**Edge sync** ([#569](https://github.com/Basekick-Labs/arc/issues/569)) addresses this by shipping immutable Parquet *files* from a spoke (edge) to a hub (central Arc). The hub verifies each file's SHA256 before committing it, and re-delivery of an already-received file is a no-op.

It ships over **two transports**, because "the edge" is not one thing:

- **Network** — the spoke initiates, so an edge behind NAT with no inbound reachability still works. Connectivity is the exception rather than the norm: discovery is a single batched round-trip regardless of backlog size, and transfers resume from a byte offset when a link drops mid-file.
- **Air gap** — for a spoke with no network path at all (a submarine, a classified facility, a vehicle whose data comes off on a drive), the spoke writes a signed bundle to removable media, the hub imports it, and a signed receipt travels back on the same drive.

Both are **manual** in this release: an operator triggers each step. Everything below is OSS; the scheduled agent that automates it is Enterprise and lands later.

Start with the hub receive endpoint:

```toml
[edge_sync]
enabled = true                     # default false
hub_id = "ground-station"          # required when enabled; bound into every request's HMAC
max_file_bytes = 536870912         # 512MiB default; must not exceed server.max_payload_size
max_reconcile_entries = 10000      # ~2MB per discovery batch
```

With it enabled, `POST /api/v1/sync/file` accepts an authenticated file push from a registered spoke, and `POST /api/v1/sync/reconcile` answers — in **one round-trip** — which of a spoke's pending files the hub already holds. That second endpoint is what makes a long disconnection survivable: a spoke returning with 5,000 pending files asks once rather than 5,000 times, and any file whose acknowledgment was lost is discovered in bulk rather than re-uploaded. Every upload is streamed to a staging area, hashed on the way in, and **verified before it is promoted** to its final location — a checksum mismatch is discarded and never appears where a reader would find it. Redelivering a file the hub already holds is a no-op; the same path arriving with *different* content is refused with `409` rather than overwritten, because one of the two copies is wrong and silently replacing either destroys the evidence.

Two independent authentication layers apply: Arc's API token middleware (write level — the endpoints are ingest-shaped, and a spoke should not hold admin credentials), and a per-spoke HMAC binding the spoke, the hub, the path, and the content digest. The spoke presents both: the HMAC is derived from its secret, and the API token comes from a second environment-only credential, `ARC_EDGE_SYNC_HUB_TOKEN` — any hub token with write permission. A spoke without it gets a `401` whose error text names the variable, and warns at startup. Only a hub running with auth disabled needs none.

**Spokes are registered through the API.** `POST /api/v1/sync-spokes` generates a cryptographically random secret and returns it **once** — it is never readable again, so an operator who loses it rotates rather than retrieves. Secrets are encrypted at rest with `ARC_ENCRYPTION_KEY`, the same key MQTT uses for broker passwords; a hub enabled without that key **refuses to start** rather than storing write credentials in a database that also holds audit logs. Spokes can be listed, rotated, disabled (reversible, keeping history and counters), and deleted — deleting a registration deliberately leaves the files that spoke already sent.

Reconcile is answered from a hub-side index of received files rather than by reading Parquet, so it stays cheap regardless of backlog size and works identically on a standalone hub and a clustered one. A batch larger than `max_reconcile_entries` is refused with `413` and the limit, so a spoke pages rather than sending an unbounded body — the request body is buffered before authentication, so an uncapped batch would be a memory claim by an unauthenticated caller.

Resume is supported on local storage. On S3 and Azure a dropped transfer restarts from zero, because block objects cannot be appended to; this is a throughput cost on intermittent links, not a correctness problem.

Abandoned partial uploads are swept from the staging area hourly once they are older than `edge_sync.staging_sweep_max_age_hours` (default 72 — deliberately longer than a plausible contact gap, because a staged partial is also the spoke's resume checkpoint; 0 disables the sweep).

### The spoke side

An edge Arc now syncs to a hub on demand. Enable it on the spoke:

```toml
[edge_sync.spoke]
enabled = true                        # default false
hub_url = "https://hub.example.com"   # required when enabled
spoke_id = "rocket-01"                # this spoke's ID, as registered on the hub
hub_id = "ground-station"             # the REMOTE hub's edge_sync.hub_id
max_attempts = 5                      # attempts before a file is marked failed
max_concurrent = 2                    # simultaneous transfers
batch_size = 1000                     # files per reconcile round-trip; 0 = whole backlog at once
ledger_retention_days = 90            # prune synced/skipped ledger rows; 0 = never
```

A reconcile page the hub refuses as too large (over its `max_reconcile_entries`, or its byte limit) is **split and retried within the same pass** — the 413 carries the hub's cap, and the agent adapts to it. No `batch_size` value can leave a backlog undrainable.

The secret is **environment-only**: `ARC_EDGE_SYNC_SPOKE_SECRET`, the value the hub returned once at registration; the hub API token is `ARC_EDGE_SYNC_HUB_TOKEN`, handled the same way. A secret in the config file is **refused at startup** rather than ignored — one that is ignored still leaks, and leaving it in place makes the committed copy look load-bearing. `hub_id` is validated at load for the same reason it is easy to get wrong: it is bound into every request MAC, so a mismatch fails *every* request with a `400` that looks like a hub problem.

Three admin endpoints drive it:

| Endpoint | Purpose |
|---|---|
| `POST /api/v1/spoke-sync/run` | Run one sync pass and return what it did. |
| `GET /api/v1/spoke-sync/status` | Pending/synced/failed counts and sync lag. |
| `GET /api/v1/spoke-sync/ledger` | Per-file state, attempts, and last error. |

A pass recovers transfers interrupted by a crash, discovers new files, reconciles the backlog, and streams what the hub lacks — **newest first**, so a contact window that closes mid-backlog has already delivered the freshest telemetry. It **pages until the backlog drains**, so one pass on a spoke returning from a long outage moves everything, not just the first batch. Conflicts are reported in full rather than counted and are not retried: the same path holding different content means a spoke-ID collision or corruption, and re-sending would either be refused or destroy evidence.

Files are hashed once at discovery, and the ledger survives restarts, so a spoke re-run after a crash neither re-hashes nor re-sends what already landed. Nothing is deleted from the spoke — sync is a copy, and local retention stays in the operator's hands.

**A tracked file that vanishes before delivery is marked `skipped`, not retried.** Compaction (on by default) rewrites raw Parquet and deletes the sources; retention deletes whole partitions. A file caught by either after discovery but before delivery has nothing left to send — the ledger records it as `skipped` (reported in `/status` and each pass/export result) instead of burning the retry budget or, on the air-gap path, failing the export outright. The check is deliberately one-directional: only a storage backend positively reporting the file gone skips it; a transient storage error never does. Terminal rows (`synced`, `skipped`) are pruned automatically after `ledger_retention_days`.

**Compaction on a syncing spoke duplicates rows on the hub.** Raw files synced before compaction stay on the hub; the compacted file carrying the same rows then syncs as a new path, and hub queries over that partition double-count. Until hub-side supersede logic ships, either disable compaction on databases a spoke syncs, or account for duplicates in hub queries.

### Air-gap bundles

Some spokes have no network path at all — a submarine, a classified facility, a vehicle whose data comes off on a physical drive. For those, a spoke writes a **signed bundle** to removable media:

```toml
[edge_sync.spoke.bundle]
enabled = true                        # default false
allowed_dirs = ["/mnt/usb"]           # REQUIRED; an empty list refuses every export
max_files = 10000                     # per bundle
max_bytes = 68719476736               # 64 GiB per bundle
```

`edge_sync.spoke.bundle.enabled` is **independent of `edge_sync.spoke.enabled`**: a fully air-gapped spoke exports bundles and never runs the network path, so it needs no `hub_url` at all. A spoke that has both intermittent connectivity and a drive courier can run both.

```bash
curl -X POST https://edge.local:8000/api/v1/spoke-sync/export \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -d '{"path": "/mnt/usb"}'
```

A bundle is a **directory**, not an archive:

```
bundle-submarine-01-06FXVSQXJ2C0EBDFDQ9D24S1E8/
  manifest.json     signed header: bundle ID, spoke, hub, entry digest, MAC
  entries.jsonl     one JSON object per file
  data/             the Parquet files, under their original paths
```

Chosen over a tar for two reasons. **Resume is free** — an interrupted copy leaves whole files, and the manifest's per-file SHA identifies exactly which landed, so resuming re-copies the mismatches rather than restarting. And it is **auditable**: someone has to inspect what crosses an air gap, and `ls` plus `sha256sum entries.jsonl` answers that without opening anything.

Signing uses a third HMAC family (`sync-bundle`) alongside the two online ones. The canonical input is length-prefixed, so a bundle MAC cannot validate on `/sync/file` or `/sync/reconcile`, nor the reverse. Unlike those families the bundle MAC carries **no timestamp window** — a bundle legitimately crosses an air gap over weeks, and a freshness check would reject exactly the artifacts this transport exists to carry. Replay protection is the hub's dedup ledger, which arrives with the import side.

**Where a bundle may be written is explicit.** Every other Arc write path is confined to the storage root by its backend; a USB mount is outside that root by definition, so `allowed_dirs` is required and an empty list refuses every export. Paths are resolved through symlinks before the check, compared at a path-segment boundary, and Arc **refuses to export into its own storage root** — the next discovery pass would otherwise find the exported copies and queue them for sync.

Exported files move to a new ledger state, `exported`, rather than staying pending. Without it a capped export would re-take the newest files every time and the oldest would never leave the box. They are reported separately from `pending` in `/status` but still counted in `pending_bytes`, because a file on a drive in transit has not arrived. If a drive is lost, `POST /api/v1/spoke-sync/export/{bundle_id}/revert` returns just that bundle's files to pending.

### Importing a drive on the hub

The other half. A hub takes a bundle off removable media:

```toml
[edge_sync.import]
enabled = true                        # default false
allowed_dirs = ["/mnt/usb"]           # REQUIRED; an empty list refuses every import
max_files = 10000                     # refuses a manifest declaring more
```

Independent of `edge_sync.enabled`: a hub that only ever takes drives exposes **no** network-writable surface — `/api/v1/sync/file` returns 404.

```bash
curl -X POST https://hub.example.com/api/v1/bundle-import \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -d '{"path": "/mnt/usb/bundle-submarine-01-06FXW4H1BHR3XHWK2J826G28JG"}'
```

**Nothing is committed until the whole bundle verifies.** The MAC, `entries.jsonl`'s own hash, the canonical entries digest, every file's size and digest, and the absence of any undeclared file are all checked first. A tampered drive is refused with `422` naming the offending file, and not one byte reaches storage.

Three refusals an operator will actually meet:

| Situation | Response |
|---|---|
| Already imported | `409`, with when it arrived and how many files |
| Tampered, truncated, wrong hub, unknown or disabled spoke | `422`, naming what failed |
| Path outside `allowed_dirs` | `400` |

**Replay protection is a dedup ledger, not a clock.** The hub records every imported bundle keyed `(spoke_id, bundle_id)` — namespaced, so a compromised spoke cannot burn IDs in another's space. A re-imported drive is refused rather than re-applied. A *refused* bundle is never recorded, so a corrected drive still imports.

In cluster mode, manifest registrations are **batched at 1000 ops per Raft proposal**. The online path is naturally rate-limited to one proposal per HTTP request; an import is a tight loop, so a 2,500-file bundle costs 3 proposals rather than 2,500.

`GET /api/v1/bundle-import/history/{spoke_id}` answers "did last month's drive ever arrive?" — which, on a link with no telemetry, nothing else can.

### The acknowledgment

The return leg, and the piece that makes air-gap sync converge. On a successful import the hub writes a signed `ack.json` **into the bundle directory** — so the drive that goes back carries the receipt, and it cannot be separated from the bundle it answers.

An operator plugs the drive back into the spoke:

```bash
curl -X POST https://edge.local:8000/api/v1/spoke-sync/ack \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -d '{"path": "/mnt/usb/bundle-submarine-01-06FXWFA2NYJHJJFAJAXBDV4PKC"}'
```

```json
{
  "applied": true,
  "bundle_id": "06FXWFA2NYJHJJFAJAXBDV4PKC",
  "hub_id": "shore-station",
  "imported_at": "2026-08-07T21:59:56Z",
  "synced": 4,
  "conflicts": []
}
```

Those files move from `exported` to `synced`, which is what finally makes them **prunable**. Before this, `synced` was unreachable on an air-gapped spoke: `PruneSynced` never pruned and the ledger grew without bound on the box least able to receive a site visit.

The ack is signed with a fourth HMAC family (`sync-ack`, length 8 — distinct from the other three, so the length-prefixed canonical input keeps them non-interchangeable). The hub signs with the **same per-spoke secret** the spoke signs with: it is symmetric, so the key that lets a spoke prove authorship lets the hub prove receipt. No new key material.

The spoke recomputes the path digest rather than trusting the one in the file — the MAC binds the digest, so a tampered path list with a stale digest would otherwise validate and license marking files synced the hub never received.

**Conflicted paths are deliberately not acknowledged.** A conflict means the hub holds *different* content there, so the spoke's copy was never delivered; those entries stay `exported` and are reported for a human. Re-applying an ack is harmless — already-synced entries are a no-op — so a drive plugged in twice changes nothing.

Like the bundle it answers, the ack carries **no freshness window**: it rides the same drive back and is subject to the same weeks-long latency.

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

### Quoted identifiers in SQL now resolve to storage paths

`SELECT * FROM "my-db".cpu` used to return zero rows, silently. The query
layer masked double-quoted tokens as if they were string literals, so the
quotes were spliced back **inside** the generated `read_parquet` glob —
`…/"my-db"/cpu/**` — a literal directory that never exists. Since a hyphenated
name *must* be quoted (unquoted it is a SQL parser error), any database or
measurement with a hyphen was unqueryable by any syntax. Edge sync made this
mainline: the hub stores each spoke's data under the spoke ID, and every
spoke-ID example in the docs is hyphenated.

Double-quoted tokens are now masked as a distinct identifier class, resolved
to their unquoted names (and validated — a quoted identifier carrying path
characters is refused rather than globbed) before storage paths are built.
The RBAC permission extractor resolves the same way, so quoted references are
permission-checked under their real names, and the cross-database rejection
for `x-arc-database` requests now sees quoted `db.table` syntax too. Quoted
CTE names, quoted column names, and string literals that merely *contain*
table-like text all behave as before.

### License-server outages no longer crash-loop Enterprise clusters ([field report])

Every pod boot used to make one blocking license check against `enterprise.basekick.net` with no retry and no memory: any transient failure — a deploy-window ingress 404, DNS, an egress blip — dropped the pod to OSS mode, and for enterprise-required configurations (shared-storage multi-writer) that is a **fatal exit**. One customer watched a writer accumulate 27 restarts during a brief license-server deploy. License-server availability was, in effect, a hard runtime dependency of every pod restart.

Boot now works like this:

1. **Bounded retry** — up to three attempts with jittered backoff, the whole phase capped at 45 seconds, so a black-holed endpoint can't stretch startup past probe budgets.
2. **Failure classification** — a *definitive* answer (the server spoke the license protocol and said no: revoked, suspended, expired, unknown key) still lands in OSS mode immediately, no retries. Only *non-definitive* failures — unreachable, 5xx, rate-limited, or a 4xx **without** the protocol's JSON body (an ingress error page, exactly the field incident) — proceed to:
3. **The cached license** — after every successful activation or verification, Arc persists the server's signed response (`license_cache.json`, `0600`, next to the auth DB). When the server can't answer, the cache is re-verified from scratch — RSA signature against the pinned key, license key must match the configured key, machine binding must match **this** machine, and expiry is enforced — and if it holds, the pod boots **fully licensed until the license's own expiry**, with a prominent warning and background re-verification continuing. A cache file copied to another machine fails the binding check (the fingerprint is hostname/hardware-derived — this stops casual copying, the same bar as online activation, not a determined cloner); removing or changing `license.key` stops the cache being consulted at all; **no new grace period exists** — the cache is honored to `expires_at`, never past it. One accepted property, stated plainly: a license revoked *during* an outage keeps working from cache until the server is reachable again or the license expires — revocation was only ever enforceable online, and a definitive server answer always wins the moment one arrives.

Two more fixes in the same change:

- **A latent crash-loop nobody had reported yet**: the license server reaps activations that haven't been heard from in 72 hours, but the "heard from" signal was only ever sent by a heartbeat call Arc never made — so any machine with a stable fingerprint was silently revoked 72 hours after activating, and its next restart crash-looped. Arc now re-activates on that condition (a deliberately revoked *license* still refuses definitively), and the license server counts successful verifications as liveness.
- The Helm chart's Arc pods gain a **startupProbe** (5 minutes of grace): the liveness probe used to be able to kill a legitimately slow boot — license retries plus multi-writer WAL recovery — at about 60 seconds, before the HTTP listener even bound.

**Offline (air-gapped) license files.** For environments that cannot reach `enterprise.basekick.net` at all, an administrator can now download an offline license file from the activation server (an explicit *site license*: unbound, activates any machine until its expiry, acknowledged at mint time and audit-logged) and point Arc at it:

```toml
[license]
file_path = "/etc/arc/license.json"   # or ARC_LICENSE_FILE_PATH
```

`/health` now also carries a `license` field — tier, status, source (`server` / `cache` / `file`), and expiry, with status derived at read time so an expiring license goes visibly non-`active` within a probe interval. A pod whose configured license failed reports `tier: oss, status: unlicensed` instead of being indistinguishable from a healthy one — the silently-degraded state behind the crash-loop incident above. No key material, customer identity, or feature list is exposed. Deployments with no license configured omit the field.

The file is the same signed payload the online activation emits, verified from disk against the pinned public key — **no network calls, ever**: no activation, no periodic validation, no cache. `file_path` wins over `license.key`, and it fails closed: a rejected file means OSS mode, never a silent fallback to online licensing. A file bound to a specific machine (an online activation response) still enforces its binding, and expiry is checked at every boot — a file that expires while a process is running is rejected at the next restart. Verified end-to-end: a production-signed site file boots a fully licensed Arc in a container with **no network attached**.

### Azure managed-identity credentials on the query path now refresh — and no longer die an hour in ([#605](https://github.com/Basekick-Labs/arc/issues/605))

The same class as the S3/IRSA bug above, on the other cloud: DuckDB's azure `CREDENTIAL_CHAIN` secret materializes an AAD token **once** at secret creation and never refreshes it, while AAD access tokens live about an hour — so on `storage.backend = azure` with managed identity or a service-principal environment, query reads died roughly an hour after each process start, with ingest unaffected.

Arc now resolves AAD tokens itself through `azidentity`'s `DefaultAzureCredential` — the same chain ingest uses, so query identity equals ingest identity — and hands DuckDB `PROVIDER ACCESS_TOKEN` secrets, re-issued before each expiry by the same refresher machinery that manages the AWS sources. Near the end of a token's life MSAL serves its cache for up to ~5 minutes; the refresher's rotation-wait handles that, exactly as it does for IMDS and Pod Identity. Verified end-to-end against live Azure Blob Storage with real hour-long AAD tokens, through a full rotation.

Also in this change:

- **`storage.azure_endpoint` now reaches the query path.** It was silently ignored there — every azure secret targeted `blob.core.windows.net`, so sovereign-cloud endpoints only worked for writes. Full-URL values are normalized to DuckDB's host-suffix form; path-style endpoints (Azurite) don't fit that model and are logged and skipped. The sovereign token *audience* remains a known, ingest-shared limitation.
- **SAS deployments never start a refresher.** `storage.azure_sas_token` is now visible to the credential routing: a deliberately-scoped SAS reports `sas / unknown` in `/health` and keeps its existing behavior — a managed refresher would have acquired a broader identity than the operator intended. (The query path has never consumed the SAS itself; its DuckDB secret is chain-shaped. That pre-existing asymmetry is unchanged and now at least visible.)
- Deployments with no resolvable Azure credentials degrade to the previous chain secret with a demoted background retry, like S3.

### `/health` now reports storage credential state — a dead read path can no longer hide behind green probes ([#603](https://github.com/Basekick-Labs/arc/issues/603))

The incident that motivated this: a reader ran ~21 hours READY 1/1 with `/health` green while every S3-backed query failed on expired credentials — the only failure signal lived in query responses, invisible to probes, monitoring, and orchestration.

`/health` now carries a `storage` field describing every configured tier — hot always; cold once its configuration succeeded (a cold tier whose secret creation failed is absent from the payload rather than falsely green):

```json
"storage": {
  "hot":  {"backend": "s3", "credentials": "sdk_managed", "state": "ok",
           "expires_at": "2026-08-18T20:24:18Z", "source": "CredentialsEndpointProvider",
           "last_refresh": "2026-08-18T20:12:26Z"},
  "cold": {"backend": "azure", "credentials": "sas", "state": "unknown"}
}
```

States: `ok`, `degraded` (refreshes failing, credentials still valid), `expired` (the incident case — red within a minute of expiry), `fallback` (no resolvable credentials; running on DuckDB's chain), `unknown` (Arc has no visibility — see below). The state is computed from the credential refresher's in-memory records at read time: **`/health` never probes S3 or Azure**, so there is nothing to flap and no per-probe storage traffic. Pre-expiry alerting belongs on `expires_at`, not on a state change — a healthy IMDS or Pod Identity session routinely waits for server-side rotation near its end and stays `ok` while doing so.

Honesty notes: an Azure **SAS token** (configured directly or embedded in a connection string) reports `credentials: sas, state: unknown` — SAS tokens expire and Arc cannot see when, and calling that "ok" would recreate exactly the incident above. Azure **managed identity** reports full refresher state (`sdk_managed`), since [#605](https://github.com/Basekick-Labs/arc/issues/605) above brought it under Arc's credential management. Local storage reports `local / none / ok`.

**Opt-in readiness gating**: `server.storage_credentials_fail_ready = true` (default **false**) makes `/ready` return 503 while any tier's credential state is `expired`, so Kubernetes recycles the pod — a restart re-resolves credentials. Only `expired` drains a node; `fallback`/`unknown`/`degraded` never do (those deployments may be healthy or credential-less by design). Intended for reader pools: Arc logs a startup warning when it is enabled on a cluster writer, whose ingest keeps working through credential expiry. Startup and shutdown readiness gating are unaffected — the knob can only remove readiness, never grant it.

The field is served unauthenticated like the rest of `/health` and `/metrics`: it contains no bucket or container names, endpoints, prefixes, paths, or key material.

### S3 queries no longer fail with `ExpiredToken` an hour after startup on EKS/IRSA ([#600](https://github.com/Basekick-Labs/arc/issues/600))

On EKS with IRSA — where pods authenticate to S3 through a projected service-account token instead of static keys — Arc's query path stopped being able to read S3 roughly **one hour after each process start**. Every S3-backed query failed with `ExpiredToken`, while ingest and `/health` kept working normally, so Kubernetes probes never noticed and only a pod restart recovered it.

The root cause is that DuckDB resolves temporary STS credentials **once**, when the S3 secret is created, and has no working refresh path for Arc's workload. We verified this against live AWS STS: even DuckDB 1.5.5's `web_identity` auto-refresh never fires for globbed reads (Arc's only read shape), and its reactive re-authentication arms on HTTP 401/403 while an expired STS token surfaces as HTTP 400.

**The fix: Arc now manages these credentials itself.** When no static S3 keys are configured, a background refresher resolves credentials through the AWS SDK — the same code path ingest uses, which is why writes never suffered from this bug — and hands DuckDB session credentials, re-issued about ten minutes before each expiry. Verified end-to-end against real AWS STS with hour-long sessions.

The refresher covers **every credential source the AWS SDK can resolve** ([#601](https://github.com/Basekick-Labs/arc/issues/601)): when no static S3 keys are configured in `arc.toml`, Arc resolves credentials through the SDK's chain and routes on the result. Expiring credentials — IRSA, **EC2 instance roles**, **EKS Pod Identity**, SSO, process credentials — get the refresher; non-expiring ones (environment or profile keys) are emitted once; and if nothing resolves at all, Arc falls back to DuckDB's own credential chain so today's behavior is preserved while a background retry keeps probing. Query identity now equals ingest identity by construction — both come from the same SDK chain. Both the hot and cold S3 tiers are covered, and a pod that starts before its service-account token is projected degrades to a logged retry loop instead of failing startup.

Arc logs the credential route at startup (`credential_mode=sdk_managed` / `static_keys` / `credential_chain` for the fallback) and each refresh logs the concrete source (`EC2RoleProvider`, `WebIdentityCredentials`, `CredentialsEndpointProvider`, …) and the new expiry. Two behaviors that are **expected**, not bugs: on EC2 the SDK caps reported expiry at one hour, so the secret is re-emitted hourly even though IMDS sessions last ~6h; and near the end of a session the log line `credential source has not rotated yet; polling` is the refresher waiting (at one-minute intervals) for IMDS or the Pod Identity agent to rotate on their own schedule.

**Operational notes:**

- A deployment with **no resolvable AWS credentials at all** (rare — S3 backend with no keys anywhere, off AWS) pays a measured **4–5 seconds** at startup probing the EC2 metadata service, twice if a keyless cold tier is configured. Set `AWS_EC2_METADATA_DISABLED=true` to skip the probe (<1ms). Previously such a deployment **failed startup outright** (DuckDB validates chain secrets at creation); it now starts on the fallback and self-heals if credentials appear later.
- EKS Pod Identity was verified against a live credential-endpoint rotation with real STS sessions; real-world Pod Identity uses the token-file variant of the same SDK provider. EC2 instance roles were verified against the SDK's IMDS provider with a local metadata endpoint; the flat one-minute rotation poll tolerates any server-side rotation lead ≥1 minute.

**Related hardening:** `CREATE SECRET` / `DROP SECRET` statements are now rejected in user SQL. While testing this fix we found the query API accepted them, which would let any authenticated user replace or delete Arc's S3 credentials.

### DuckDB upgraded to 1.5.5

Arc's bundled DuckDB moves from 1.5.1 to 1.5.5, picking up four upstream releases of fixes. The most relevant to Arc: hardening across many Parquet decompression and deserialization paths, a fix for an out-of-bounds read in dictionary-string decompression, and a deadlock fix in DuckDB's `TemporaryMemoryManager` (reachable by queries that spill). Since Arc's entire storage layer is Parquet and it serves queries over it, the Parquet hardening matters even for deployments that never see the deadlock.

The upgrade also required a fix on Arc's side. As of 1.5.5 DuckDB's secrets manager stats its `secret_directory` on **every** `CREATE SECRET`, including the temporary, in-memory secrets Arc exclusively creates. That directory defaults to `~/.duckdb/stored_secrets`, which sits outside Arc's DuckDB sandbox allowlist — so on 1.5.5 any secret created *after* the sandbox locked down would have failed with `Permission Error: Cannot access directory`. In practice that is the **tiered-storage cold-tier path** (S3 and Azure alike), which configures its credentials at runtime by design.

Arc now opens DuckDB with `allow_persistent_secrets=false`. Arc has never used DuckDB's on-disk secret storage — credentials live in memory for the life of the process — so nothing is lost, DuckDB skips the directory check, and the guarantee that Arc never writes an unencrypted credential to disk is now enforced by DuckDB itself rather than by convention.

No configuration change is required.

## `/api/v1/query/msgpack` is now stable

The MessagePack query endpoint graduates out of experimental. Its response shape and type vocabulary are a published contract — clients can bind to them.

MessagePack is now the best general-purpose format for a client that isn't using Arrow directly. It is columnar, carries a per-column `types` array, accepts `SHOW` statements (which the Arrow endpoint rejects), and honors `Accept-Encoding` — zstd cuts a 7.4MB response to 4.5MB, about 39%. End-to-end it runs roughly 2–3× faster than JSON on large result sets; Arrow remains faster for raw throughput but has neither `SHOW` support nor response compression.

What changed to get here, all in this release: decimal columns now transmit as numbers rather than strings, the `types` vocabulary is frozen and pinned by a golden test, and a client disconnect no longer logs as a server error. The endpoint requires the `duckdb_arrow` build tag, which every shipped artifact sets.

See the API reference for the response shape and the full type vocabulary.

### A client disconnecting mid-response no longer logs as a server error on the msgpack path

When a client hangs up partway through a large `/api/v1/query/msgpack` response, Arc logged the truncation at **Error**, while `/api/v1/query` logged the identical condition at **Warn**. A browser tab closing on a big dashboard query is routine operational noise, not a server fault, so this produced spurious Error-level entries for operators alerting on them.

The severity decision is shared between both paths and keys off a client-disconnect sentinel, but the msgpack emit loop returned the encoder's bare socket error without that marker — so the shared helper classified a client hangup as a server-side failure. The JSON path already wrapped its equivalent flush failure. Genuine timeouts and cancellations are passed through unchanged, so a real deadline is still distinguishable from a disconnect.

### msgpack query responses: decimal columns are now numbers, and the `types` array is a frozen contract

Two related fixes to `/api/v1/query/msgpack`, ahead of that endpoint graduating out of experimental.

**Decimal columns disagreed with their declared type.** DuckDB returns `SUM(integer)` as `decimal(38,0)` and `AVG` as `decimal(x,y)`. The Arrow IPC path normalizes those to `int64`/`float64`; the msgpack path did not, and its encoder has no decimal case — so decimal columns fell through to the string fallback. The response advertised `decimal(38, 0)` in `types` while transmitting the msgpack **string** `"3"`, and the same query returned a number on `/api/v1/query/arrow` but a string on `/api/v1/query/msgpack`. Decimals are now normalized on the msgpack path too: `SUM(int)` yields `int64`, scaled decimals yield `float64`, values are numeric, and the two binary formats agree for the same SQL.

**The `types` array is now Arc's own contract rather than arrow-go debug output.** Type names previously came from `arrow.DataType.String()`, which upstream treats as debug output and reformats in patch releases — `list<item: int32>` gained `, nullable` in 2021, its hardcoded `item` became the element name a week later, and struct gained an (un-comma'd) ` nullable` in the arrow-go release Arc currently pins. A client binding to those strings would break on a routine dependency bump.

Names are now derived from stable Arrow type IDs:

- **Scalars are unchanged** — `int64`, `utf8`, `float64`, `bool`, `binary`, `date32` and friends keep their exact spelling, so existing clients see no difference.
- **Parameterized types keep their information** in an Arc-owned format: `timestamp[us]` (the unit is load-bearing) and `decimal(p, s)`.
- **Nested types collapse to a bare `list` / `struct` / `map`.** The encoder has no list or struct case, so those values already go out as strings; a bare token says "opaque" instead of promising element typing that is not delivered.
- **Types the encoder renders as text report `string_encoded`** — `DATE64`, `TIME`, `INTERVAL`, `DURATION`, `FLOAT16`, and fixed-size binary have no typed encoder case, so their values are transmitted as msgpack strings. They previously reported precise names like `duration[ns]`, which is the same "numeric-looking name over a string payload" defect as the decimal bug above; the name now matches the wire.
- **Unrecognized types emit an `unknown:` sentinel** and log once, so a client cannot accidentally bind to a spelling Arc does not control. DuckDB `ENUM` columns land here, since DuckDB exports them as Arrow dictionaries.

The SHOW handlers, which emit the same `types` field from a second code path, now share one set of constants with the streaming path, so the two cannot drift. A golden test pins every string the contract can produce.

### Arc refuses to start when built without `-tags=duckdb_arrow`

Arc's query fast path uses DuckDB's Arrow interface, which `duckdb-go/v2` places behind the `duckdb_arrow` build tag — the tag is upstream's, not Arc's, and `NewArrowFromConn` does not exist in the package without it. Arc mirrored that with tagged files and a `database/sql` fallback, so a binary built without the tag still compiled and still started.

That binary was not a lighter variant, it was a strictly worse one. On a 2M-row query, `/api/v1/query` served the fallback at **1425ms against 637ms** for the same query on a tagged build — byte-identical responses, 2.2× the latency. `/api/v1/query/arrow` was worse than unavailable: the route is never registered, so the request fell through to the `GET /api/v1/query/:measurement` pattern and returned `405 Method Not Allowed`, which a client cannot distinguish from using the wrong verb. `/api/v1/query/msgpack` returned `501`.

Every artifact Arc ships already sets the tag — `make build`, the Dockerfile, and all release targets including FIPS — so no released binary was ever affected. The untagged build was reachable only by someone running a bare `go build ./cmd/arc`, who would then be silently served half-speed queries and a misleading `405`.

Arc now checks the tag at startup and refuses to run without it, alongside the existing FIPS fail-closed check:

```
Arc was built without -tags=duckdb_arrow; refusing to start.
The Arrow query path is required: rebuild with `make build`.
```

The untagged tree still compiles, so `go build ./...` and editor tooling are unaffected — the failure moved from silent runtime degradation to an explicit startup error. The `compact` subcommand is unaffected; it does not use the query path.

### `LEFT`/`RIGHT`/`FULL OUTER`/`NATURAL` joins are no longer rewritten to inner joins ([#586](https://github.com/Basekick-Labs/arc/issues/586))

Arc rewrites table references into `read_parquet(...)` calls before handing the query to DuckDB. The join rewriter matched the join operator but replaced it with a hardcoded bare `JOIN`, discarding whatever modifier the query actually used. `LEFT JOIN metrics` became `JOIN read_parquet(…)`.

This was a **wrong-answer** bug, and the most severe kind: the query succeeded, returned `success: true`, and produced a plausible result set that was **silently missing rows**. A `LEFT JOIN` against a table with unmatched rows returned only the matched ones — the exact rows an outer join exists to preserve. Every modifier was affected: `LEFT`, `RIGHT`, and `FULL OUTER` were demoted to inner joins, and `ASOF`/`SEMI`/`ANTI`/`POSITIONAL` lost their semantics. `NATURAL JOIN` failed loudly instead — stripping `NATURAL` left no join condition, so DuckDB rejected the statement with a parser error.

The rewriter now captures the join operator and emits it verbatim, so `LEFT OUTER JOIN mydb.cpu` rewrites to `LEFT OUTER JOIN read_parquet(…)`. Irregular whitespace (newlines, repeated spaces, tabs between `LEFT` and `JOIN`) is normalized to single spaces rather than corrupting the emitted operator.

The same change fixes a related defect on bare joins, **found and fixed by [@schotime](https://github.com/schotime)** in [#585](https://github.com/Basekick-Labs/arc/pull/585): because the join modifier was optional *and* its trailing whitespace was optional, the pattern collapsed to "optionally consume whitespace" for an unmodified `JOIN` — so the match began at the space *before* it, and replacing the match deleted the separator. `FROM a JOIN mydb.cpu` fused the preceding alias into a reference to a fabricated measurement `aJOIN` and dropped the join operator entirely. That fix — requiring whitespace after a modifier rather than allowing it to float — is the basis of the corrected patterns shipped here, extended to also capture the operator for the modifier fix above.

`INNER JOIN` was unaffected by both defects, which is why they survived: the existing tests only covered modifier forms that happened to round-trip.

RBAC table extraction shares these patterns but was **not** affected — it reads the table identifier, which was never part of the corrupted text, so permission checks always evaluated the correct table.

### Table rewriting no longer depends on the length of the table name

Fixing the joins above surfaced a second, unrelated defect in the same rewriter. Before rewriting an identifier, Arc checks whether it is followed by `.` or `(` — a database qualifier or a table-valued function call, neither of which is a measurement. That check recovered the identifier's position by searching a lowercased copy of the SQL that was computed **once, before** the earlier rewrite passes ran. The recovered offset therefore pointed into the original string while the text was read from the already-rewritten one.

The consequences depended on where the stale offset happened to land, which in practice meant **the length of the table name**:

- `SELECT * FROM a JOIN cross ON 1=1` left `cross` un-rewritten (the offset landed on the `(` of a previously emitted `read_parquet(`), so DuckDB failed to resolve the table — while the one-character-longer `SELECT * FROM a JOIN crossx ON 1=1` worked.
- In the other direction, `JOIN generate_series(1, 10)` was rewritten **as if it were a measurement**, because the offset missed the `(` that marks it a function call.

The lookahead now uses the match offset the regex already provides, so it always reads the string actually being rewritten. Table-valued functions and database-qualified names are still skipped, including when whitespace separates the name from the paren.

### Compaction skips partitions without a `time` column instead of failing every cycle

Every compaction query normalizes the `time` column via a `SELECT * REPLACE (...)` expression (and sorts by `time` by default). A partition whose Parquet files have no `time` column at all — data loaded by external tools rather than Arc's ingest path, which always writes one — could never satisfy either, so every cycle failed with `Binder Error: Column "time" in REPLACE list not found in FROM clause`, forever.

Worse, the adaptive batch splitter treated that deterministic error as potentially memory-related and walked its full retry ladder (30 → 15 → 7 → 3 files), re-downloading the batch at every rung — turning one impossible partition into up to eight failed jobs with gigabytes of wasted I/O, every cycle.

Two fixes ship together:

- Compaction now probes the unified schema up front (a footer-only `DESCRIBE`, no data scan) and, when **no** input file has a `time` column, completes as a zero-work skip: a single warning (`Skipping compaction: no 'time' column in any input file`), sources left in place, counted as completed rather than failed. Partitions where only *some* files have `time` compact normally — the missing values are backfilled as `NULL`.
- Deterministic DuckDB SQL failures (`Binder Error`, `Catalog Error`, `Parser Error`) are now classified as non-recoverable, so the batch splitter no longer retries errors that fail identically at any batch size. DuckDB's own out-of-memory errors remain recoverable — retrying with a smaller batch is exactly the right response, and they are now recognized on the subprocess result path too, not just in stderr.

Zero-work completions (skips, and the existing "all files already compacted" path) also no longer invalidate the parquet-metadata and query caches — nothing changed on storage, and dropping those caches forced a cold re-read on every in-flight query. In cluster mode they now clean up their pending completion manifest instead of leaving one orphaned file per cycle for the startup sweep.

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
5. **Edge sync is off by default and nothing changes unless you enable it.** With `edge_sync.enabled=false` (the default) the hub mounts no routes and `/api/v1/sync/file` returns 404. With `edge_sync.spoke.enabled=false` (the default) no spoke routes are mounted, no `sync_ledger` or `sync_history` tables are created, and nothing is read from local storage. Both sides are independent: a hub need not be a spoke, and a spoke need not be a hub. (One related change applies everywhere: the quoted-identifier query fix under Bug fixes — quoted names now resolve instead of returning zero rows.)

## Dependencies

No product dependency changes in this release. GoFiber remains at `v2.52.13`; Dependabot alert #12 (CVE-2026-45045) is addressed by the in-code hardening above rather than a dependency bump, because no patched v2 release exists and the vulnerable code path (`middleware/proxy.BalancerForward`) is not present in Arc's build graph.
