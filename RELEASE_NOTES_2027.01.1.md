# Arc v2027.01.1 Release Notes

> **Status:** Planned — January 2027 release.

## Fix: a DELETE could miss a flush failure, and a WAL recovery pass could clear the failure while its rows were still only in the WAL ([#1938](https://github.com/Basekick-Labs/arc/issues/1938))

**Who is affected:** builds of `main` with the WAL on (`wal.enabled`).

A DELETE (and dropping a database) refuses while a flush failure is waiting for WAL recovery, because recovery would bring the failed rows back. A failure in a short window just before the DELETE flushed its scope, or while a recovery pass was clearing the failure state, was not seen: the DELETE went ahead, and the next recovery pass restored the matching rows. Separately, a recovery pass could count a flush that failed during the pass as already handled and clear the failure state, although that flush's rows were only in the newest WAL file, which the pass does not read. A clean shutdown then deleted the WAL, and those rows with it.

All three are closed:

- The DELETE reads the failure state once, and its flush fails if any flush failed after that reading. The same holds for the flush a shared-storage DELETE asks of the other nodes.
- Clearing the failure state is one atomic step that fails if anything was marked since the pass read it, so it can no longer hide a failure that arrives at the same moment.
- A recovery pass reads the failure state before it rotates the WAL, so a flush that fails after the pass started keeps the state set for the next pass. Such a failure now keeps DELETEs refused for one more pass (`wal.recovery_interval_seconds`).

A DELETE refused because a recovery pass left WAL files to replay now says so ("a WAL recovery pass left files to replay; retry after the next pass"), instead of reporting a flush failure. The status code is unchanged (503).

New metric `arc_ingest_flush_failure_latch_age_seconds` (gauge): how long the failure state has been set, during which DELETEs and database drops that flush this node's buffer are refused (on shared storage, also those issued on other nodes). It is 0 when clear and with the WAL off, sampled once a second. A state that clears and is set again within one second reads as one continuing period.

`wal.recovery_interval_seconds` must be greater than 0 when `wal.enabled = true`: Arc now refuses to start with a clear error instead of panicking when the recovery pass starts.

**Cost.** None on the write or successful flush path: a failed flush does one atomic compare-and-swap, a DELETE one atomic read.

## New: query latency and outcomes per engine (DuckDB, arcx) ([#1292](https://github.com/Basekick-Labs/arc/pull/1292), [#823](https://github.com/Basekick-Labs/arc/issues/823))

**Who is affected:** anyone scraping Arc's Prometheus metrics, or reading `/api/v1/metrics`.

Two new metrics:

- `arc_query_engine_executions_total{engine,outcome}`: query executions by engine (`duckdb`,
  `arcx`) and outcome (`success`, `error`). In the JSON metrics: `query_engine_executions`.
- `arc_query_latency_seconds{engine}`: a latency histogram per engine, failures included, with
  buckets from 1 ms to 30 s. Before, query latency was only a lifetime average.

They count queries that reached an engine. A request refused earlier (validation, permissions, the
SQL transform) appears only in `arc_query_requests_total` and `arc_query_errors_total`, so the sum
of `arc_query_engine_executions_total` can be lower than `arc_query_requests_total` by design.

Existing counters change too, so that `arc_query_success_total + arc_query_errors_total` matches
`arc_query_requests_total` on every query route. Dashboards built on them may move after the
upgrade:

- An Arrow IPC query that times out before its stream starts now also counts as an error.
- An empty result (a measurement with no data) counts as a success on `/api/v1/query` (every
  encoding), `/api/v1/query/msgpack` and `/api/v1/query/arrow`, and goes into the lifetime latency
  average. Before, the Arrow-JSON and MessagePack paths counted it as neither, and the JSON path
  left it out of the average. `/api/v1/query/:measurement` still answers it as an error.
- A query that shutdown cancels counts as an error on every route. Before, the Arrow IPC,
  Arrow-JSON and MessagePack paths did not count it. The query registry still records it as
  cancelled.
- A response whose final flush fails now counts as a failed query, and on the JSON and Arrow paths
  also in `arc_query_client_disconnects_total`. Before, it counted as a success.
- A stream that panics before it finishes counts once as a failure; a panic after the outcome was
  recorded does not count the query again.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1292](https://github.com/Basekick-Labs/arc/pull/1292). Thank you.

## New: a write can wait until its rows are visible to a query (`x-arc-wait: visible`) ([#1759](https://github.com/Basekick-Labs/arc/issues/1759))

**Who is affected:** standalone deployments that turn on the new key **`ingest.wait_visible_enabled`** (`ARC_INGEST_WAIT_VISIBLE_ENABLED`, default `false`). Off, nothing changes. A request carrying the header is then refused with 403, and nothing is written.

A write is acknowledged once its rows are buffered, and a query sees them only after their flush. That comes at most `ingest.max_buffer_age_ms` (5 s by default) later. A client that writes and then reads, such as a test, an agent's memory, or a job step, had to sleep or poll. With the key on, a write to `/api/v1/write/msgpack` or `/api/v1/write/line-protocol` can send **`x-arc-wait: visible`** and is answered once its rows are visible:

- **204, `X-Arc-Visible: true`.** Every row is in a stored file. With tiered storage, that file is also registered. "Visible" means visible to a query over HTTP `/api/v1/query` on this node that starts after the answer. Flight SQL is not covered. A concurrent DELETE can still remove the rows afterwards.
- **202, `X-Arc-Visible: false`, `X-Arc-Visible-Reason: <reason>`**, body `{"visible": false, "reason", "rows_written"}`. The rows were accepted, as a plain 204 says, but they are not known to be visible. Do not resend them. The reasons:
  - `timeout`: 20 s passed first.
  - `shutdown`: the server began shutting down. The rows are still flushed at close.
  - `flush_failed_wal_replay`: the flush failed, and a WAL copy was accepted; recovery replays it. With `wal.sync_mode = "async"` that copy may not have reached the disk yet.
  - `not_confirmed`: a tiering registration failed, or the rows did not all add up.
  - `storage_not_confirmable`: the storage backend is S3, MinIO, GCS or Azure. In this version only local storage confirms.
  - `future_stamped`: a row is stamped more than `query.file_time_pruning_margin_seconds` ahead, in the current hour. File-level time pruning can hide it until the hour closes.
- **500, `X-Arc-Visible-Reason: flush_failed_rows_at_risk`.** A flush holding rows without a WAL copy failed. The body has `rows_at_risk` and says the rows may still be stored, so check before resending. An import answers the same way since #1888.
- **Answered at once, status unchanged:** a partial write (400, some points dropped) carries `X-Arc-Visible: false` and `partial_write`; a write stopped part way (500) carries `write_stopped`. An empty MessagePack write keeps its 204, with `X-Arc-Visible: true`. The absence of `X-Arc-Visible` never means visible.
- **Refused before anything is written:**
  - any other value of the header: 400;
  - a cluster (`cluster.enabled`): 400, before any forward;
  - the header on `/write`, `/api/v2/write`, Prometheus remote write, TLE, or an import (`/api/v1/import/csv`, `parquet`, `lp`, `tle`): 400. Other routes, such as the edge-sync bundle routes, ignore it and answer without `X-Arc-Visible`;
  - more than 1024 waiting writes on the node: 503 with `X-Arc-Ingest-Refused: wait_visible_busy` and `Retry-After: 5`. The limit is per node and shared by every client: one client sending visible writes faster than about 200 a second at the default age can hold it and leave the others at 503. Writes without the header are not limited.

**Latency.** The write's measurements are flushed early, at most once per `ingest.wait_visible_min_interval_ms` (5000 by default) per measurement and only while the flush pipeline has spare capacity. Otherwise the write waits for its rows' normal age or size flush, which is up to `ingest.max_buffer_age_ms` plus the flush time.

- **The new key `ingest.wait_visible_min_interval_ms`** (`ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS`, default `5000`) must be between 1000 and 3600000; any other value stops Arc at startup. A measurement idle for longer than the interval is flushed right after the write is buffered, when spare capacity allows. With the interval at most `ingest.max_buffer_age_ms`, a writer that keeps one measurement busy gets an early flush at most once per interval and waits about half the interval on average; with the interval above the age, the age flush comes first and the wait is about half the age, as without the key.
- **Spare capacity** means: the flush queue under half full, no flush deferred within the last `ingest.max_buffer_age_ms` (at least 1 s), buffered bytes under half of `ingest.max_buffered_bytes` (when set), and at most `max(1, ingest.flush_workers / 4)` early flushes at once. A request flushes early only the measurements it wrote itself. A write that cannot flush early is not refused: it waits for the age flush. One request writing many measurements can hold all of the node's early-flush slots for a while; other requests' writes then fall back to the age flush.
- **Files.** An early flush writes a file like an age flush. A measurement gets at most 3 600 000 / interval (in ms) early flushes an hour, 720 at the default. Every flush restarts its measurement's age, so a measurement written continuously is flushed at most once per the shorter of the interval and the age, plus its size flushes: at most `ingest.max_buffer_age_ms` / interval times the files of the age flush alone. That is no more at the defaults, and five times as many at the 1 s floor with the default age.
- **Where.** Standalone with local storage only. On object storage the write is not flushed early and still ends as 202 `storage_not_confirmable`.
- `arc_ingest_forced_flushes_total{source, result}` counts the early flushes asked for (`source="write"`), by result: `forced`, `not_due` (flushed within the interval), `over_budget` (no spare capacity), `nothing`, `not_wanted` (nothing of the request's left to flush), `closing`. `over_budget` counts requests refused for lack of spare capacity. Such a write is often still answered early, by another request's early flush of the same measurement, so read it together with `arc_ingest_wait_visible_seconds`.

A load warning names `ingest.max_buffer_age_ms` above 10000 with the key on, `ingest.wait_visible_min_interval_ms` above `ingest.max_buffer_age_ms` (a measurement written continuously is then flushed by age first), the key on a cluster, and the key on object storage (where every waited write ends as 202 `storage_not_confirmable`). Proxies and load balancers in front of Arc need idle timeouts above 20 s. Browsers may send the header (CORS), and the two answer headers are exposed; every CORS answer now lists them in `Access-Control-Expose-Headers`.

**Cost.** Without the header, a write pays a header lookup and a request-local lookup, within noise in the write benchmarks. A waiting write holds no lock while it waits, no WAL reservation and no request body; an early flush takes its measurement's locks briefly. With the key on, on a standalone node with local storage, every flush on the node also records when its measurement was flushed: one more small lock and a map write per flush, not per write. That record takes about 100 to 150 bytes per measurement flushed in the last hour, and an entry is pruned about an hour after its flush (up to a minute later) by a walk, once a minute, that locks each of its 64 stripes in turn. A visible write also records the measurements it wrote: a small lock per batch. Waited writes are left out of `arc_http_*` latency. Instead, metrics `arc_ingest_wait_visible_total{outcome}`, `arc_ingest_wait_visible_waiters` and `arc_ingest_wait_visible_seconds` record them.

## Fix: a single-hour flush registers its file in tiered storage after a slow write or at shutdown ([#1933](https://github.com/Basekick-Labs/arc/issues/1933))

**Who is affected:** tiered storage (`tiered_storage.enabled`).

A flush whose rows all fall in one hour recorded its file in the tiering metadata with the context its storage write had used. When that write outlasted `ingest.flush_timeout_seconds` but completed, or completed as shutdown cancelled the flush, the record failed with only a warning, and the file had no hot tier row until the next tier scan (at start-up and each migration cycle, daily by default). On a measurement whose tier rows were all cold, reads left out local files, so its new rows stayed invisible until then. The record now runs on its own context, bounded by its own `flush_timeout_seconds` and not cancelled with the flush, as the multi-hour path's already did ([#1693](https://github.com/Basekick-Labs/arc/issues/1693)); at shutdown, a write that completes as the flush is cancelled can add that record's time to the close. A record that still fails for another reason is unchanged: a warning, and the next scan registers the file.

## Fix: an import whose rows had no WAL copy answers only once they are stored, and fails if they were not ([#1888](https://github.com/Basekick-Labs/arc/issues/1888))

**Who is affected:** nobody on a released version. In 26.09.x an import flushed every buffer on the node before it answered. This release made imports answer once their rows are handed to the flush (#1296, #1429), and this fix closes the gap that change opened before it shipped.

A bulk import (Line Protocol, CSV, Parquet, TLE) answers once its rows are handed to the flush; the files follow as the flushes finish. With a WAL copy of every row that is a durable answer, because a failed flush is replayed from the WAL. Rows without a WAL copy are held only in memory, and a failed flush loses them. That happens with the WAL off (the default) or when a queued-mode WAL dropped the append. The import had already answered 200, and nothing told the client.

Now an import tracks its own rows through the flush:

- **If every row has a WAL copy, the import answers at once, as before.**
- **If any row had no WAL copy, the import waits for the flushes holding its rows**, including the ones its size-triggered writes started on the workers, before it answers.
  - If one of those flushes failed, it answers 500: "failed to flush imported data; the rows were accepted and may still be stored, check before running the file again", with how many of its rows were at risk. "May still be stored" is meant: a flush can fail after storing some of its hour files, or a write that timed out can land anyway.
  - If the server shuts down during the wait, it answers 500 saying the outcome is not known.
  - The wait takes no barrier lock, so a DELETE or another flush barrier never waits behind it.
- **Only the import's own rows count.** This covers the flushes the import runs itself: the final flush, and the flushes it runs while pacing behind the memory limit. Another writer's failed flush, even on the same measurement, does not fail an import whose rows were stored. Before, any failure in those flushes failed the import, whoever's rows were lost. With every row WAL-copied, a failed flush the import runs itself still fails it, as before.
- **The TLE import now flushes its own measurement instead of the whole node.** A node-wide flush cut every other writer's buffers into small files, and failed the import on any of their errors.

Measured on a laptop, local storage, WAL off. Five Line Protocol files of 1M rows, imported one after another, went from 8.67 s to 9.08 s, about 5%: an import's tail flush no longer overlaps the next file's parsing. Imported five at a time (`ingest.import_write_concurrency = 4`), it took 3.1–3.3 s either way, with the same memory.

## New: a REST catalog table load can wait for the files written before it ([#1752](https://github.com/Basekick-Labs/arc/issues/1752))

**Who is affected:** deployments that read Arc's tables through its Iceberg REST catalog and set the new key. Off by default.

An ingest file reaches its table at most `iceberg.ingest_export_interval` (60 s by default) after the table's previous export. A reader that loads the table in between got the version before it. With **`iceberg.load_export_min_interval`** (`ARC_ICEBERG_LOAD_EXPORT_MIN_INTERVAL`, seconds, default `0` = off) set, a `loadTable` on the REST catalog first waits, at most 2 s, until every file reported to the export for the table before the load is in the published version, then answers as before.

- **It never commits from the request.** It moves the table's queued ingest export earlier, to no sooner than `load_export_min_interval` after the table's last export, and the export serves it as usual (an append). Concurrent loads share that export. A table nobody reads keeps `ingest_export_interval`. A value equal to `ingest_export_interval` moves nothing earlier; the load only waits for an export already due.
- **It waits only where an append can serve it soon:** a table whose last export settled and whose last append did not fall back to a pass, not paused for a restore, with fewer than `ingest_export_concurrency` exports due within the next 2 s. Otherwise, during a timed pass, or beyond 64 waiting loads per table or 1024 in all, the load reads the published version at once. When the export is busy (a batch of appends or the requests ahead of it), it reads it after the 2 s.
- **Bounds:** `0`, or between `5` and `ingest_export_interval`, which must be on; anything else is refused at startup. It does nothing, and changes nothing, with `ingest_append_enabled = false`, without `rest_catalog_enabled`, or on a cluster over a shared bucket (each logged at startup). On a cluster with local storage, only the exporting node answers the catalog, as before, and a file counts once it has reached that node. Not run on a cluster.
- **Cost:** loads add at most `ingest_export_concurrency` exports per 2 s over all tables, and at most one per table per `load_export_min_interval`. A reader that loads a table often makes it export at that rate instead of every `ingest_export_interval`; a catalog that loads tables on its own clock does that for every table it loads, and such a table gets its full reconcile pass every 120 appends, so sooner. Table versions are then kept by the shorter spacing: iceberg-go keeps `min_version_age / load_export_min_interval` previous metadata files per table, at least `retain_snapshots` (60 instead of 10 at the defaults with the key at 5), and each table takes one metadata property commit when that spacing changes.
- **Auth:** the load stays read-level. The wait runs after the caller's read permission on the table is checked, and only on a `GET`. With auth off, anyone who reaches the catalog can make loads wait, within the bounds above.
- **Metric:** `arc_iceberg_load_fresh_total{outcome}`: `nothing_pending`, `covered`, `skipped` and `timeout` (the last two read the published version).
- Checked with the binary, standalone, `load_export_min_interval = 7` and `ingest_export_interval = 10`: a load 6 s after a write returned the row 12 times in 12 (1 in 12 with the key off), each load taking 20–35 ms instead of 2 ms; a load with nothing pending took 0.7 ms either way.

## Fix: compaction stops at shutdown, and no compaction child outlives Arc ([#1887](https://github.com/Basekick-Labs/arc/issues/1887))

Shutdown used to stop only the compaction schedulers. A cycle started with `POST /api/v1/compaction/trigger` kept running. The scheduler's stop waited for its running cycle with no bound, so a long batch could use up the whole shutdown budget. A compaction subprocess could also outlive Arc and run beside the replacement process, which treats that job's manifest as orphaned.

Now shutdown handles compaction in two steps:

- **Compaction stops first.** Before the readiness flag drops and before the HTTP drain:
  - No new cycle starts. Every trigger answers **503** with `"stopping": true`, even while a cycle is running; outside shutdown, a trigger during a running cycle still answers 409.
  - A running cycle, scheduled or triggered, stops at its next boundary: before the next batch, measurement, database, manifest to recover, or half of a split batch. Its status is `"stopped"`. A stop is not counted as a failure, and a split stopped part way does not mark the partition as too wide.
  - Batches already running keep going while requests drain and buffers flush.
- **Arc waits for that batch later, with a bound.** The wait comes after the WAL closes and before the Iceberg export's drain, the cluster coordinator, storage and the databases. It lasts until a fifth of `server.shutdown_timeout` is left.
  - If the batch is still running then, Arc kills its subprocess and waits for the cycle to finish its own clean-up.
  - The next start recovers the killed job from its manifest, as after a crash.
  - If the shutdown deadline passed before the wait could run, Arc kills the cycle as its last step before exiting. No compaction child is left running after Arc exits.
- **On a cluster, the completion watcher drains after that wait.** Its final pass now sees the records of batches that ended during the wait, while Raft is still up. Before, a scheduled batch that ended after the watcher's final pass left its record unapplied.
- When a batch overruns and is killed, the Iceberg export's final drain gets less of the budget: a tenth, or a twentieth with Glue on.
- With auth off, an edge-sync node closes its own SQLite handle in a hook. A batch that ends during the wait then cannot record its output there. The manifest is kept and the next start re-applies it.

Checked with the binary at `server.shutdown_timeout = 5`. A triggered cycle whose child was frozen (SIGSTOP) was killed 4.0 s after SIGTERM, and Arc exited cleanly with no child left. After restart, the next cycle compacted the partition.

## New: `telemetry.instance_id` and `telemetry.data_dir` ([#1885](https://github.com/Basekick-Labs/arc/issues/1885))

Telemetry identifies an installation by a random id kept in `./data/.instance_id`, relative to the working directory. A deployment without a persistent disk got a new id on every start, and counted as a new installation each time. Examples are a pod with an `emptyDir`, or a container with nothing mounted at `/app/data`.

- **`telemetry.instance_id`** (`ARC_TELEMETRY_INSTANCE_ID`) is an id to use instead of the file; nothing is written to disk.
  - It must be a UUID: generate one with `uuidgen`, and don't copy one from an example. A value that is not a UUID, or is a placeholder (the nil or max UUID, or the textbook `550e8400-…` example), stops Arc at startup while telemetry is on.
  - It is stored in canonical form, so upper case or braces do not make a second id.
  - On a standalone node it is the id: give each installation its own. On a cluster, Arc derives a per-node id from it and `cluster.node_id` (or the host name when `node_id` is unset). So one value in a multi-replica template still counts each replica once.
  - Setting it on an installation that already has an id file changes its id for as long as it is set; unsetting it brings the file's id back. A standalone node can keep its current id by setting the key to the file's contents.
- **`telemetry.data_dir`** (`ARC_TELEMETRY_DATA_DIR`) is where the id file lives when no id is configured. The default is `./data`, as before; an empty value means the same. It does not follow `auth.db_path` or `storage.local_path`.
- The id file is now written `0600`. A file that holds only whitespace is replaced with a new id instead of sending an empty one.

## Change: compaction cycle ids carry the process they belong to, and the trigger can wait ([#1884](https://github.com/Basekick-Labs/arc/issues/1884))

Compaction cycle ids are counted from 1 in each Arc process. A client that keeps an id across requests can read another cycle's record under the same number. That happens when Arc restarts between its trigger and its poll, or when the poll lands on another node behind a load balancer. This was seen with a CronJob: two triggers an hour apart, with a restart between them, both got `cycle_id: 1`.

- **`boot_id`:** a random 16-hex value made once per process. These responses carry it: the trigger (its 200, and its 409 "already running"), `/cycles`, `/cycles/{id}`, `/history` and `/stats`. Arc also logs it once at startup, so cycle log lines from before and after a restart can be told apart.
- **Lookups that send `boot_id`:**
  - `GET /api/v1/compaction/cycles/{id}?boot_id=<x>` answers 404 with `"boot_id_mismatch": true` when `x` is not this process's, even if this process has a record with that number. It names this process's `boot_id`, and the lease holder on a cluster. The cause can be a restart or another node; the server cannot tell which.
  - `GET /api/v1/compaction/history?cycle_id=<n>&boot_id=<x>` answers 200 with `cycle_retained: false`, `boot_id_mismatch: true` and no jobs.
  - Without `boot_id`, or with it empty, nothing changes.
- **`POST /api/v1/compaction/trigger?wait=<duration>`** (for example `90s`, `5m`, at most 10 minutes) answers when the cycle ends: 200 with the cycle's record. Check its `status`: a cycle that failed is a 200 too.
  - If the wait runs out, it answers 202 with the trigger's answer and `"status": "running"`; the cycle carries on.
  - If the server starts shutting down meanwhile, it answers 202 with `"status": "unknown"`; a manual cycle stops with the process.
  - A wait that is not a positive duration is rejected with 400 before any cycle is claimed. Without `wait` the trigger answers at once, as before.
  - A client that gives up early does not stop the server waiting: the answer is written when the cycle ends or the wait runs out.
  - Behind a load balancer, keep the wait under its idle timeout (often 60 s), or the balancer answers 504 while the cycle carries on.

Ids stay numeric and still restart at 1. Seeding them from the clock would have made `arcli compaction run --wait` report a cycle as finished after a restart, because it compares the id against `/stats`' `current_cycle_id`.


## Fix: files flushed at shutdown reach the Iceberg table, and Glue, before Arc exits ([#1886](https://github.com/Basekick-Labs/arc/issues/1886))

On SIGTERM, ingest's final flush writes what is still buffered. The Iceberg export had already stopped by then, so the files from that flush were not added to their tables. Files waiting for their next ingest export (up to `iceberg.ingest_export_interval`, 60 s by default) were left out too, along with export requests the loop had taken but not served. The tables stayed behind the bucket until Arc started again; on a node that does not come back (decommissioned, scaled away) they stayed behind. Glue, stopped earlier still, never learned of a version published that late.

Now, at shutdown:

- The export's loop stops early, as before, so no timed pass competes with the final flush. Requests it had taken but not served go back in the queue.
- Once the buffer and the WAL have closed, the export serves what is queued, the final flush's files included, whatever each request's due time:
  - one table at a time, each tried once (again only if a newer file was reported for it);
  - with an append where one is possible, otherwise that table's own pass;
  - then the requests a compaction, DELETE, retention or tiering change made.
- A pass's commit reserve, which keeps the end of its deadline for the commit, is scaled to the time left: a third of it, at most the usual 20 s. With the usual reserve, a pass with less than 20 s left read no file at all.
- Glue then syncs what was published, and the export's own catalog database closes.

This uses what is left of `server.shutdown_timeout` after the buffer's close, less a tenth for the closes that follow and, with Glue on, a twentieth for Glue. Nothing is taken from the HTTP drain. The default `server.shutdown_timeout` (30 s) is unchanged.

**Limits:**

- **Long shutdowns.** Tables are served one at a time, and on S3 an append is a few requests. With many tables and little time left, the drain stops at its deadline, and the next start's pass reconciles the rest. Raise `server.shutdown_timeout` if every table must be current at exit.
- **Shared bucket.** A table there is exported by a pass, and on a cluster the shutdown spends part of the budget elsewhere first: the ready drain, the HTTP drain, the file registrar. There, the exporting node first catches up with the cluster manifest, for at most 5 s, so the last registrations' files are queued. On such a cluster, `server.shutdown_timeout` of 45 s or more gives the drain room.
- **`iceberg.ingest_export_interval = 0`.** Flushes report nothing to the export, so the drain serves only the other requests: a compaction's, a DELETE's, database drops. The final flush's files wait for the next start, as before.
- **A node that does not export.** It serves its database drops, as the loop always did, and nothing else.
- **A pass that ignores cancellation.** The loop's stop is bounded. If a pass does not stop in time (for example, a cluster manifest barrier), the drain and the catalog close are skipped, and that pass may still be running while the components after it close.

Verified with a binary on S3 (SeaweedFS): two writes ten seconds apart, then SIGTERM straight after the second. Before, the second file was in the bucket and not in the table when Arc exited; now it is in the table. The same holds with the WAL on (`ack_mode = durable`), with `iceberg.ingest_export_interval = 300`, with `iceberg.ingest_append_enabled = false`, and with `server.shutdown_timeout = 5`.


## Fix: Arc no longer prints the admin token you configured at startup ([#1883](https://github.com/Basekick-Labs/arc/issues/1883))

**Who is affected:** anyone who sets `auth.bootstrap_token` (`ARC_AUTH_BOOTSTRAP_TOKEN`), with or without
`auth.force_bootstrap`.

When Arc installed the initial admin token it printed a banner with the token value to stderr. That is
right for a token Arc generated, because the operator has no other way to see it. When the value came
from configuration the operator already has it, and printing it only copied a secret into the logs. On a
deployment whose auth database is not persistent, such as a container with an ephemeral disk, the token
is installed again on every start, so the value was printed on every start too.

- The token value is printed only when Arc generated it.
- For a configured token the banner says it was installed from configuration and leaves the value out.
  With `auth.force_bootstrap` it keeps the recovery wording and the reminder to remove the setting.
- The "FIRST RUN" wording appears only for a generated token.
- `auth.force_bootstrap` without `auth.bootstrap_token` generates a token only when no tokens exist
  (a first run), as before; the banner now says "FIRST RUN" for it instead of "RECOVERY TOKEN ADDED -
  EXISTING TOKENS PRESERVED".

No new configuration keys.

Contributed by [@branson404](https://github.com/branson404) in [#1917](https://github.com/Basekick-Labs/arc/pull/1917).

## Fix: a rejected `auth.bootstrap_token` stops Arc instead of leaving it with no admin token ([#1915](https://github.com/Basekick-Labs/arc/issues/1915))

**Action required on upgrade** if `auth.bootstrap_token` (`ARC_AUTH_BOOTSTRAP_TOKEN`) is set to a value shorter than 32 bytes once surrounding whitespace is removed, with auth on. Arc now refuses to start with that value, where it used to log `Failed to create initial admin token` at every start and run on. Such a value has never been installed: every path that stores a token requires 32 bytes. So a deployment that runs today uses another token, and the short value can simply be removed, or replaced with a 32-byte one (`openssl rand -hex 32`). A Helm install that passes the value through `existingSecret` is not checked at render time: its pods stop at startup with the message instead.

With a short value and auth on, Arc used to keep running with no admin token, so every request got 401. A fresh install looked healthy and nobody could log in. Now:

- **A short token stops Arc at startup.** The message names `auth.bootstrap_token`, the length after trimming, and the 32-byte rule. It never includes the value.
- **A configured value that was not installed now says so.** A value is installed only into an empty auth database. When the database already has tokens and the value is not one of them, Arc logs a warning that it was not installed, and how to add it: `auth.force_bootstrap=true` for one start. This used to be a debug line, so an operator who set or fixed the value on a populated database got 401 with nothing saying why.
  - A value that is already a valid token stays quiet, which is the normal restart.
  - A value matching a disabled (revoked) or expired token gets its own warning. `auth.force_bootstrap=true` adds it back as a new recovery token.
  - The exception is a token from before token prefixes (December 2025): if the value equals one, it is reported as not installed although it authenticates.
- **A forced recovery that adds nothing now says so.** Recovery adds a token named `arc-recovery`, and the name is unique. Two cases add nothing:
  - one is left from an earlier recovery with a different value;
  - it holds this value but is disabled.
  
  Arc used to log that the caller "still holds the token value they provided". It now says the value was not added and to **delete** `arc-recovery` through the API (`DELETE /api/v1/auth/tokens/<id>`); revoking only disables it, and the name stays taken. On a cluster, where the node's copy may lag, it says the same without checking the value.
- **`auth.force_bootstrap` without `auth.bootstrap_token` logs a warning at load.** The flag acts only together with a token.

The check for whether the value is installed reads the token table directly. It does not stamp `last_used_at` on the admin token at every start, as authenticating with it would. On a cluster the check is not exact on a follower, so Arc logs what to do at Info without claiming the value is absent.


## Fix: re-home jobs run with `iceberg.warehouse` outside the storage root ([#1906](https://github.com/Basekick-Labs/arc/issues/1906))

**Who is affected:** standalone deployments with the Iceberg export on and `iceberg.warehouse` set to
a directory outside the storage root.

The export writes no discovery files (`version-hint.text`) in such a warehouse, and catalog readers
are the only readers there, so the catalog's commit is the publish. A re-home job's check read the
published version from the discovery files, found none, and refused every job ("did not publish its
version"). After a pass of its own, the export now takes the catalog's commit as published there.
On a cluster, nodes that do not export answer from the discovery files only, so in this layout a
job still waits at them, as before: re-home with the warehouse outside the storage root is a
standalone fix.
## Fix: a node with the Iceberg export, failover and replication on local storage joins its cluster ([#1914](https://github.com/Basekick-Labs/arc/issues/1914))

**Who is affected:** clusters whose nodes list more than 16 capabilities, for example on local
storage with `cluster.replication_enabled`, `iceberg.enabled` and `cluster.failover_enabled` (with a
licence that includes writer failover), or with `cluster.stream_takeover` (experimental) on top of
replication on local storage or of the Iceberg export and failover on a shared object store.

A joining node lists the cluster capabilities it supports, and the member it contacts refused a list
longer than 16. With the Iceberg export, failover and replication on local storage a node lists 17,
so every node but the first was refused ("join rejected: authentication failed" on the joiner, "join capabilities exceed
their bounds" on the member) and the cluster never formed. The limit is now 64; a node lists at most
18 today. No released version had the limit.

**Upgrading a cluster that runs an earlier main build with such a configuration.** That build
refuses more than 16, and this one lists 17 for the same configuration, so a member restarted on
this build is refused by the members still on the earlier one. Measured on three nodes on local
storage with replication, the Iceberg export and failover, restarted one at a time (compactor,
writer, reader): the compactor left the cluster and answered neither writes nor queries until the
writer had been restarted too; then all three were healthy with all their rows, without
intervention. The only procedure tested end to end is restarting all the nodes together: all three
rejoined with every row, and the Iceberg table matched the files. No release has the limit.

## Fix: a node that gets the Iceberg export back without restarting exports a current table ([#1903](https://github.com/Basekick-Labs/arc/issues/1903))

**Who is affected:** clusters on local storage with the Iceberg export on, whose compactor lease moves
between nodes: by failover (such a cluster forms from this release on, [#1914](https://github.com/Basekick-Labs/arc/issues/1914)),
or by `POST /api/v1/cluster/compactor/assign`, which also moves a lease left from an earlier failover
setup.

While another node held the lease, a node dropped its own ingest export requests, and files its table
named could go away (compaction inputs, retention, DELETE). When the lease came back, the next file it
flushed or received was appended to the table as the node had left it: the published table then named
the files that were gone and missed the ones written while another node exported, until the next timed
pass (`iceberg.reconcile_interval`, 5 minutes by default). A table nobody wrote to stayed as the node
had left it for up to that long too, while the node's REST catalog already served it again. Now the
node checks the lease every 5 seconds even when idle, and when it gets it runs a full pass at once
instead of appending, which brings every table in line with the node's files. That includes a node
taking the lease for the first time; on a shared bucket that is the lease holder's pass of every table,
run when it takes the lease instead of at its next timed pass. The final export at shutdown
([#1886](https://github.com/Basekick-Labs/arc/issues/1886)) does the same when the lease came back just
before it.




## Change: an Iceberg pass that a compaction raced is retried within seconds, and counted ([#1755](https://github.com/Basekick-Labs/arc/issues/1755))

**Who is affected:** deployments with the Iceberg export on.

When a file vanishes during an export pass (a compaction, a DELETE or retention deletes it between
the listing and the read), the pass leaves the files of that UTC day for the next pass; a day that
also loses files that pass waits whole, new files included. That next pass came on the ordinary
short interval, 30 seconds at the default `iceberg.reconcile_interval`. It now comes after 5
seconds, then 10 and 20 if files keep vanishing, before the ordinary interval takes over. Such
passes are counted in a new metric, `arc_iceberg_vanished_passes_total` (`iceberg_vanished_passes`
in the JSON metrics), so how often a deployment hits the race can be seen.

## Fix: two re-home edges with tiered storage and the Iceberg export ([#1848](https://github.com/Basekick-Labs/arc/issues/1848))

**Who is affected:** deployments with tiered storage or the Iceberg export on that run re-home
jobs.

- **A file removed since the tiering scan listed it is no longer a failed migration.** A scan whose
  listing predates a re-home job's (or a compaction's, or a DELETE's) removal of a file re-inserts
  its hot row; the next migration then tried to copy a file that was gone and logged
  "Failed to migrate file" at Error, counting a failure. It now sees the file is gone, copies
  nothing and logs at Info; `POST /api/v1/tiering/migrate` reports such files in a new `gone`
  count instead of `errors`, and the next scan retires the row as before.
- **A job found at start before it switched reads is undone when the Iceberg export did not start
  this time** (it failed to build, for example `wireIcebergCold` failing). Before, it switched
  reads and then waited for an export that never runs, until the wait was waived. Reads still used
  the old file, so the undo changes nothing for them; the request can be sent again once the
  export runs.

## Change: a re-home job checks there is room on disk before it starts ([#1851](https://github.com/Basekick-Labs/arc/issues/1851) item 3)

**Who is affected:** anyone running re-home jobs (`POST /api/v1/rehome`), dry runs included.

A job copies the file to a scratch directory and splits it there, which holds about twice the
file's size side by side, then moves the new files to the hot tier. For a large file, a cold-tier
file especially, that could fill the volume other writers share. A job is now refused up front,
with the space it needs and the space that is free, when the scratch volume lacks twice the file's
size plus 64 MiB or the hot storage volume lacks its size plus 64 MiB (three times the size when
they are one volume). It is an estimate: DuckDB may spill to scratch as well. A volume whose free
space cannot be read is not refused on.

## Change: a re-home job whose Iceberg wait was waived still has its old file removed from the table with its new files ([#1845](https://github.com/Basekick-Labs/arc/issues/1845) item 1)

**Who is affected:** standalone deployments that waive a re-home job's wait for the Iceberg export
(`POST /api/v1/rehome/<id>/resume {"skip_export_wait": true}`).

A waived job ends before the export has published the table without its old file. Until now the
pass that later removed the old file no longer knew what had replaced it, so the files holding its
rows in other days could commit in a later step of that pass, and a pass that stopped part way
published the table without some of those rows for a while. The job now leaves a small record
(`_rehome/replaced/<id>.json`, written and made durable before the old file is removed) that the
export reads as it would a running job's: the old file's removal commits together with its new
files. A new file that cannot be registered is logged and left out instead of holding the table
(the old file is already gone), and a file that vanishes during that pass holds back only its own
day, not every day of the commit. The export removes the record once the published table does not
name the old file, or the table is gone. If the record cannot be written, the old file is still
removed as asked and the server logs an error; a server stopping at that moment keeps the job
instead, and it waits again after the restart. The record is not a job: it holds no job slot and owns no file, and an older
build ignores it. Not yet on a cluster, where the waived job's record lives in Raft and goes with
the job.

## Change: on a cluster, the Iceberg export picks up a file as soon as it reaches the exporting node ([#1756](https://github.com/Basekick-Labs/arc/issues/1756))

**Who is affected:** clusters with the Iceberg export on.

On a cluster only one node exports, and a file written on another node waited for the exporting node's
next timed pass (`iceberg.reconcile_interval`, 5 minutes by default) before it was in the table. Now a
file that reaches the exporting node from another (pulled onto its disk on local storage, or
registered in the shared bucket) asks for an export of its table, as an ingest flush does on a
standalone node (`iceberg.ingest_export_interval`; on local storage it appends just the new files
when `iceberg.ingest_append_enabled` is on, on a shared bucket it runs a pass of the table).
The export of a table then follows within `iceberg.ingest_export_interval` (60 seconds by default),
as on a standalone node. Measured on a three-node cluster with local storage, a row written on one
node was in the exporting node's table 58 seconds after its flush at the default (8 seconds with the
interval at 10, under a second for a table's first file), against 276–300 seconds before. On local
storage, rows written on the exporting node itself follow the same interval too
([#1902](https://github.com/Basekick-Labs/arc/issues/1902)): on local storage every node's own
flushes ask for the export, and the nodes that do not export drop the request.
`iceberg.ingest_export_interval = 0` keeps the timed passes only.

## Change: a re-home job checks it can write the Iceberg table before it starts ([#1845](https://github.com/Basekick-Labs/arc/issues/1845) item 3)

**Who is affected:** re-home jobs with the Iceberg export on.

Before a job changes anything for readers it asks the export for a pass of the table. A pass that
needed no change wrote nothing, so a table whose metadata directory could not be written
(read-only, or not writable with the export's credentials) passed the check, and the job only stopped later, waiting for an
export that could not publish, with queries already switched to the new files. The check now also
writes and deletes a small probe object (`.arc-write-probe-<random>`) in the table's metadata
directory, through the same file IO the export's commits use (also for a warehouse outside the
storage root), and refuses the job with the error when that fails. A nearly full volume can still
pass: the probe is a few bytes. On a cluster the exporting node
runs the probe as part of its check before it acks the job.

## Change: a compaction racing an Iceberg export holds back fewer of the files ingest just wrote ([#1755](https://github.com/Basekick-Labs/arc/issues/1755))

**Who is affected:** deployments with the Iceberg export on.

When a file vanished while an export pass was running (deleted by a compaction, a DELETE rewrite,
retention, a tier move or a re-home job), the pass left every new file of that file's UTC day for
its retry, 30 seconds later at the default `iceberg.reconcile_interval`, on the reasoning that the
day's other new files were the same compaction's inputs about to go too. Files ingest wrote in the
last 45 minutes on the current UTC day are no compaction's input, so they now stay in the pass and
are registered with the rest. Only for a day the pass removes nothing from: a day that also loses
files in the pass is still deferred whole, fresh files with it, since a file named like a fresh
ingest file can still be the replacement of a removed one (a re-home whose wait was waived, a
restored original). A day where a fresh file itself vanished (a DELETE or a re-home over
today's rows) also goes whole, as before.

## Fix: files a backup restore writes are read at once ([#1764](https://github.com/Basekick-Labs/arc/issues/1764))

**Who is affected:** anyone restoring data files from a backup into measurements that already have
files.

A restore writes its files without an ingest flush, so the query caches were not told about them,
and a query could keep answering from a cache made before the restore ("this hour is empty", or a
file list without them) for up to 60 seconds. A restore of data files now drops the query caches
when it ends, whatever its outcome. Files an edge-sync hub receives were already read at once: on
the hub's layout pruning never caches an empty hour, and the query reads the full glob.
## New: moving a misfiled file's rows on a cluster with stream takeover on ([#1872](https://github.com/Basekick-Labs/arc/issues/1872))

**Who is affected:** clusters with stream takeover on (`cluster.stream_takeover`) that run re-home
jobs (#1825).

A re-home job was refused whenever stream takeover was on. It now runs there for a file that carries
no stream coverage, which is how it ends with takeover off: the job's new files are registered without
coverage and the old file's batches stay settled, as after a compaction. A file that carries coverage
(its rows settle a writer's batches, which a move cannot divide among the new files) is still refused,
with the reason. Every node must run this release: while one runs an earlier one, jobs are refused
with stream takeover on.

## Fix: on a cluster, a late row is read at once on every node, not only on the one that wrote it ([#1763](https://github.com/Basekick-Labs/arc/issues/1763))

**Who is affected:** clusters that receive rows stamped outside the current hour (late or future data).

#1757 made a node that flushes a file outside the current hour stop using what its query caches held
for that measurement, so its own queries read the row at once. Other nodes kept answering without it
for up to 30 seconds (any query) or 60 seconds (a repeated query text). Each node now does the same
when it gains a file from another node: when its puller lands it (local storage), or when the cluster
registers it (shared object store). Compaction outputs and DELETE rewrites are not reported: they
replace files of an hour that already had some. Measured on a reader of a three-node cluster: a
late row read 25–85 seconds (local storage) or 15–65 seconds (shared object store) after its flush
before, 0.1 seconds after.

## Change: a re-home dry run runs while a re-home job waits for the Iceberg export ([#1845](https://github.com/Basekick-Labs/arc/issues/1845))

**Who is affected:** standalone nodes with the Iceberg export on that run re-home jobs (#1825).

A job holds its slot while it waits for the export to stop naming the old file, which can take a
long time, and every other request, dry runs included, was refused with 409 meanwhile. A dry run
changes nothing, so it now has its own slot: one dry run and one job can run at once. A second job
(or taking one up again with `/resume`), or a second dry run, is still refused. A job in progress is
no longer dropped from `GET /api/v1/rehome` by newer ones. Jobs on a cluster never held this slot.
## Fix: a measurement whose rows were all cold reads its first hot file at once ([#1765](https://github.com/Basekick-Labs/arc/issues/1765))

**Who is affected:** nodes with tiered storage on.

When no tier row of a measurement says hot, a query reads only its cold tier, and that query's SQL
is cached. A flush reports a new file to the query layer before it records the file's tier row, so
a repeated query run between the two could cache SQL without the hot tier, and keep leaving the new
file out for up to the cache's lifetime (60 seconds). Such cached SQL now goes stale as soon as the
measurement's tier rows change (a per-measurement counter checked in memory on each use).

## Fix: deleting a backup answers 404 for an unknown id and 503 for an unreachable destination ([#1122](https://github.com/Basekick-Labs/arc/issues/1122))

**Who is affected:** clients of `DELETE /api/v1/backup/:id`.

It answered 500 for both, so an operator could not tell a wrong id from a destination that is down.
It now answers as `GET /api/v1/backup/:id` does: **404** when no configured target holds the backup,
and **503** naming the target(s) that could not be reached or did not delete every file (in `target`
and `targets`) — the backup is removed from every target that could be, and running the delete again
finishes it. A file a target failed to delete was only logged before, and the delete answered 200. A client that
treated any 5xx as "retry" keeps working; one that checked for exactly 500 should check for 503.

## Change: on a cluster that shares one object store, a node serves no reads while its cluster state may be behind ([#1852](https://github.com/Basekick-Labs/arc/issues/1852), [#1825](https://github.com/Basekick-Labs/arc/issues/1825))

On a cluster whose nodes share one bucket (S3, Azure or GCS as primary storage), every node
reads the same files, and what a read leaves out of them comes from the node's own copy of
the cluster state: the inputs of a compaction whose output is in place, a file a DELETE
replaced. A node whose copy is behind the leader's leaves the wrong files out and can count
rows twice. Two ordinary situations put a node there:

- **A restart.** A node applies its local snapshot and served queries before it had
  replayed what the cluster committed since. A graceful restart also removes the node from
  the cluster until it rejoins, so nothing waited for it.
- **A node cut off from the leader**, which kept answering queries with whatever state it
  had, for as long as the partition lasted.

Such a node now answers reads with **503**, `"error": "cluster_state_not_synced"`, a
`Retry-After` header and the reason, until it holds a **read lease**: it has synced with the
current leader since it last lost contact or saw the leader change, it has heard from the
leader within the last 5 seconds, and it has known who the leader is within the last 5
seconds. The sync is a barrier when the lease is taken; the lease says the node was caught
up then and is in contact now, not that it has applied every entry at every moment.

- **What is gated:** the query API in all its formats, Flight SQL, the measurement and schema
  listings, and continuous queries (a run is skipped before it reads, with no execution row
  and its window kept; a manual run answers 503). A node that only forwards a query to a peer
  (a compactor) passes it on, and the peer answers for itself. Writes are not affected.
- **What shows it:** `GET /api/v1/cluster` has `read_ready` and, when false,
  `read_not_ready_reason`. A new probe, `GET /ready/read` (unauthenticated, like `/ready` and
  `/ready/write`), answers 503 `not_read_target` while the node holds no lease, so a load
  balancer can take it out of the read pool. Taking and losing the lease are logged, and
  refused reads are logged at most once a second, not once per request.

Measured on SeaweedFS:

| Cluster | Event | The reader |
|---|---|---|
| One writer, one reader | The reader restarted | 503 four times, then 200, 0.5 s after it started |
| One writer, one reader | The writer (the leader) killed | 200 until 5.3 s later, then 503 |
| One writer, one reader | The writer back | 200 again 1.5 s after it started |
| Three writers, one reader | The leader killed | 200 throughout: 60 of 60 queries over 15 s |
| Three writers, one reader | A second writer killed (quorum lost) | 503 5.9 s later; `/ready/read` 503, `/ready` 200 |
| Three writers, one reader | Both writers back | 200 again 1.4 s after they started |

**What this costs.** Only writers vote in Raft elections. So:

- **With one writer** (one writer and any number of readers), every time that writer stops,
  crashes or is upgraded, every node stops serving reads 5 seconds later, until the writer is
  back and each node has synced with it. That is the "leader killed" row above.
- **With two writers**, a crash (not a graceful leave) of either loses the quorum, with the
  same effect.
- **Any node** that cannot reach the leader serves no reads after 5 seconds.

In those cases nothing can commit, so a node's state is not getting further behind; the
refusal holds anyway, because the node cannot tell how far behind it already is. Before, it
went on answering, possibly wrong. Run three writers or more to keep reads through a writer
restart, point clients at more than one node and retry on 503, or route reads with
`/ready/read`. Raft timings raised far above their defaults (`cluster.raft_heartbeat_timeout`,
`cluster.raft_election_timeout`) make every failover longer than 5 seconds, and so a read
outage too.

Clusters on local storage (which replicate files and have their own opt-in gate,
`cluster.query_gate_on_catchup`) and standalone nodes are unchanged.

## Fix: a re-home job waits for an arcx-served result to finish streaming ([#1858](https://github.com/Basekick-Labs/arc/issues/1858))

**Who is affected:** standalone nodes using the arcx engine, during a re-home job (#1825).

A result the arcx engine serves reads its Parquet files as it streams, after the request's handler
has returned. A re-home job waited for queries only until the handler returned, so it could
remove a file such a stream was still reading, and the query failed part way. It now waits until
the stream ends. Results were never wrong. DuckDB-served results (JSON, Arrow IPC, the
per-measurement endpoint) are complete before the handler returns and are unchanged; arcx does not
serve on a cluster sharing an object store, so compactions there are unaffected.
## Fix: with the S3 cache on, queries no longer fail or hang while files are replaced ([#1861](https://github.com/Basekick-Labs/arc/issues/1861))

**Who is affected:** deployments with `query.enable_s3_cache = true` (off by default), standalone or
cluster.

Arc clears the S3 cache whenever files are replaced (a compaction, a DELETE, a re-home). The
cache extension also cached open file handles, and a clear reset that cache without a lock while
other queries were opening files through it: those queries failed with `INTERNAL Error: Attempted to dereference shared_ptr that is
NULL!`, or hung inside DuckDB until their client gave up. On a 5-node cluster during 10 compactions,
two nodes failed about 5,600 queries each over 2.5 minutes. The file-handle cache is now off; the
data, glob and metadata caches stay. Same run: no errors, no hung queries. The cost: each query
opens the files it reads again instead of reusing an open handle, which can add a request per file
to the object store; the file data itself is still served from the cache.
## Fix: with the S3 cache on, a file listing from before a compaction or DELETE is no longer used after it ([#1864](https://github.com/Basekick-Labs/arc/issues/1864))

**Who is affected:** deployments with `query.enable_s3_cache = true` (off by default).

The S3 cache also cached directory listings (for 10 seconds). Arc clears the cache when files are
replaced, but a listing another query had in progress at that moment was stored when it finished,
after the clear: for up to 10 seconds, queries could read the old set of files, without a
compaction's new output while its inputs were already left out, so those rows were missing. The
listing cache is now off; the data and metadata caches stay. The cost is an S3 LIST per glob per
query: in a test against a local SeaweedFS, about 7 ms more per small query (a query loop answered
3,110 queries in two minutes instead of 3,776); more against a remote bucket, and little next to a
query that reads many rows.
## Fix: on a cluster, a re-home job does not start under a backup running on another node ([#1857](https://github.com/Basekick-Labs/arc/issues/1857))

**Who is affected:** clusters sharing one object store with re-home enabled (#1825): every backup,
restore and backup deletion there.

A backup or restore kept re-home jobs out only on the node it ran on. After a failover, a job the
new primary writer started could move a file the backup on the former one was copying, leaving the
backup with the file and the files replacing it, or neither. A backup, a restore and a backup
deletion now record themselves in the cluster (renewed every 30 seconds, ignored 90 seconds after
the last renewal; `GET /api/v1/cluster` lists them under `rehome.backups`), and a job is not staged
while one is recorded anywhere; a backup still refuses to start while a job runs. One that cannot
record itself (no cluster leader) is refused. The renewals compare clocks of different nodes: keep
them in sync. Nothing is recorded while re-home is off (an older leader would refuse it), so a
backup that started before the cluster turned re-home on is not protected by a mark.

Also: a DELETE that started on a former primary cannot register its rewrite of a file a job is
moving (the cluster refuses it), but could leave the rewrite's object in storage, read beside the
job's new files. The job now removes such objects before it removes the file and again before it
ends, and that DELETE's registration then settles as gone; one that lands later is removed by the
DELETE's own cleanup. Either way the rows that DELETE removed are back (they are in the job's new
files): run it again.

## Fix: on a cluster sharing one object store, a query no longer counts a compaction's rows twice ([#1852](https://github.com/Basekick-Labs/arc/issues/1852))

**Who is affected:** clusters whose primary storage is S3, Azure or GCS, with compaction on.

When a compaction finished on such a cluster, it copied its output into place and deleted its
inputs without waiting for queries on other nodes. A query that started just before could read
the inputs and the output together, counting those rows twice, or fail on an input deleted
under it. Reproduced on a 5-node SeaweedFS cluster: during 10 hourly compactions, every query node
returned wrong totals at least once, and a node that had just restarted did so 66 times.

Each step of a compaction now waits until every node has finished the queries that started under
the previous step, and has dropped the caches that may remember the old files, so no query counts a
compaction's rows twice or misses them (a query still running when the inputs are deleted can still
fail and ask to be retried):

1. the output is copied into place while every node still reads the inputs and leaves the output
   out;
2. every node drops its caches; then queries switch to the output and leave the inputs out;
3. the input objects are deleted; every node drops its caches; then the job ends.

- **A node that is down does not hold compaction up.** A node the leader has not heard from for
  30 seconds is skipped: it has lost its read lease, which cancels its queries, and it re-syncs
  and drops its caches before it serves queries again. A node that is skipped while it was in fact
  reading cancels its queries and re-syncs as if it had lost its lease.
- **What waits:** a compaction now takes at least as long as the longest query running on any
  node while it finishes. While it finishes, a DELETE of its files and a database delete are
  refused (retry once it ends), retention leaves its files for its next run, and a backup or a
  restore is refused. A query stuck inside the query engine more than 30 seconds past its own
  deadline is not waited for (it is logged; it fails if its files are removed).
- **Queries cancelled by a lease loss stay cancelled** after the lease is retaken, so a query that
  started before a node lost contact never reads with what it knew before.
- **A query that hangs inside DuckDB** delays compaction until 30 seconds after its own deadline, or
  for as long as it hangs with `query.timeout = 0` and a client that stays connected (with the S3
  cache on such hangs were common; fixed by
  [#1861](https://github.com/Basekick-Labs/arc/issues/1861), above).
- **What shows it:** `GET /api/v1/cluster`, `compaction_commit.read_fences_enabled` and, while a
  compaction waits, `read_fences_behind` (the nodes it waits for).
- **Rolling upgrade:** the change turns on once every node runs this release, like compaction
  commit; a node on an older release is then refused at join. Compactions that were finishing when
  it turned on finish the earlier way.
## Fix: a node restarted with tiering, the Iceberg export or edge sync on no longer counts as taking part in a re-home job ([#1859](https://github.com/Basekick-Labs/arc/issues/1859))

**Who is affected:** clusters sharing one object store, while a re-home job runs.

A node that came back (after a crash or a kill, without leaving the cluster) with one of those
turned on was refused at join while a job ran, and so kept its old record, which said it could take
part. The job went on as if the node were in scope. Its join is now accepted: its record says it is
out of scope, and a job that has not yet switched reads to its new files is reverted by its driver;
one that has finishes. A node joining for the first time, or one that left the cluster on a clean
shutdown (a clean shutdown leaves), is still refused while a job runs.

## New: moving a misfiled file's rows on a cluster with the Iceberg export on ([#1870](https://github.com/Basekick-Labs/arc/issues/1870))

**Who is affected:** clusters with the Iceberg export on and compactor failover running
(`cluster.failover_enabled = true` with a licence that includes it): the export follows the
compactor lease, so one node exports. `POST /api/v1/rehome` was refused on a cluster with the export
on; it now runs there, as it does on a standalone node (#1844). Without compactor failover, a cluster
with the export on still refuses it.

Each node with the export on does the export's part of the job on its own acknowledgement of each
step, which the job waits for:

- Before the outputs are published, the node that exports checks the table can be published; every
  other node checks that some node exports. Otherwise the job reverts after 15 minutes.
- Once reads use the outputs, the old file is removed only when the published table no longer names
  it. The export removes it in the same commit that adds the files replacing it. On a shared object
  store every node with the export on waits for that same published table, whichever node exports and
  however the compactor lease moves. On local storage the node that exports waits for its own table
  (each node publishes its own warehouse; only the exporting node's is current, #1756).
- This wait has no time limit: while it lasts the job's files stay out of compaction, DELETE and
  retention, and backups wait. `POST /api/v1/rehome/<id>/resume {"skip_export_wait": true}` ends one
  node's wait (send it to each node `GET /api/v1/cluster` lists as missing for the job): the old file is
  then removed while the table still names it, and readers of the table fail on it until the export's
  next commit.

## New: moving a misfiled file's rows on a cluster with tiered storage on, for a file on the hot tier ([#1869](https://github.com/Basekick-Labs/arc/issues/1869))

**Who is affected:** clusters with tiered storage (shared object store, or local storage with
replication). `POST /api/v1/rehome` was refused on a cluster with tiered storage on; it now runs
there for a file on the hot tier, as it does on a standalone node (#1847). A file on the cold tier
is still refused on a cluster.

Each node keeps its own record of which files are hot and cold, so each node's part of a job keeps
it: before it acks the step that publishes the outputs, it checks that its tiering has the file on
the hot tier and that cold storage holds no copy of it, and records the outputs as hot files; once
the file is gone it removes the file's row; a reverted job removes the outputs' rows. A file a
migration to cold had copied before a node saw the job makes that node hold the step until the
migration has given the file back to the hot tier (it does, once it sees the job), or the job
reverts after 15 minutes, rather than leave a cold copy read beside the outputs. The primary writer
checks the file the same way before it starts the job. A node whose cold tier is configured and not
running (no licence, cold storage unreachable at start) does not take part, so no such job starts
on the cluster until it does.

Also: a migration to cold whose batch the cluster refused because a compaction or a re-home job took
one of its files since the migration checked it now drops that file and proposes the rest again,
instead of stopping with the rest of the batch recorded cold and their hot copies kept until
reconciliation.

A node on an earlier release with tiered storage on does not take part, so no job starts until every
node runs this one.

## New: moving a misfiled file's rows on a cluster with local storage ([#1860](https://github.com/Basekick-Labs/arc/issues/1860))

**Who is affected:** clusters on local storage with `cluster.replication_enabled = true` (a node
takes part once its file puller runs), with
tiered storage, the Iceberg export and edge sync off on every node and stream takeover off.
`POST /api/v1/rehome` (not a dry run) now runs there, on the primary writer, as it does on a
cluster sharing an object store. A local-storage cluster without replication is still refused
(each node holds only its own files).

Every node keeps its own copy of each file, so every node does each step on its own disk, and acks:

1. The job is recorded. The primary writer writes the outputs to a staging directory
   (`_rehome_staged/<job>/`, which no read or walker looks at).
2. Every node fetches the outputs into its staging directory (from the primary writer or any node
   that has them), checks their checksums, and acks.
3. Every node records the job on its own disk (`_rehome/<job>.json`), so its reads leave the
   outputs out, then moves them to their final keys and acks.
4. One entry registers the outputs and removes the file's manifest entry. Every node records that
   its reads leave the file out, waits for the reads that started before, removes its copy of the
   file, and acks.
5. Every node ends its record. The job ends.

What a node's reads leave out comes from its own record, not from its copy of the cluster's state,
so a node that restarts mid-job reads every row once even before it has caught up with the leader;
the old file stays on a node until its record says the outputs are in place there. A query that had
listed the file before it was removed and opens it after fails (503, retry) rather than returning
wrong rows.

- **A node that stops answering** holds the job at its step: until the job switches reads to the
  new files (steps 2 and 3) it reverts after 15 minutes; from then on it waits. `GET
  /api/v1/cluster` (`rehome.jobs`) names the nodes it waits for. A node shut down cleanly leaves
  the cluster, and the job goes on without it 30 seconds later; when it comes back it settles each
  job it missed by what the manifest lists (keeps the outputs, removes the old file, or the reverse
  for a reverted job), checking every minute until the outputs have reached it.
- **A node that joins mid-job** fetches the outputs itself, or records the job before its puller
  brings them, so it reads the old file until every output is there.
- No read lease is added on local storage: queries are served as before throughout.
- A node that rejoins with no record of a job (it stopped before the outputs were published, or
  joins fresh) while the job runs or after it ended brings the old file and the outputs in through
  its file puller, independently: for a moment its queries may see both or neither (one wrong total
  in about 2,600 queries on such a node in testing). Every other node, and that node once its pulls
  land, reads each row once.

## New: moving a misfiled file's rows on a cluster whose nodes share one object store ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

**Who is affected:** clusters whose primary storage is S3, Azure or GCS, with tiered storage,
the Iceberg export and edge sync off on every node and stream takeover off. `POST
/api/v1/rehome` (not a dry run) now runs there, on the primary writer; elsewhere on a cluster it
is refused with the reason, as before.

Every node reads the same bucket, so every node must agree, at every moment, on which side of the
move its reads leave out. The job's steps are entries in the cluster's replicated state, and a step
that changes what reads leave out waits for **every node to acknowledge** it has ended the reads
that started before:

1. The job is recorded (the outputs' keys, sizes and checksums). Nothing is written yet.
2. Every node leaves the outputs out of reads and acks; the primary writer then uploads the
   outputs to their final keys.
3. Every node drops its query caches and acks.
4. One entry registers the outputs and removes the file's manifest entry; every node leaves the
   file out and reads the outputs, and acks.
5. The file's object is deleted; every node drops its caches and acks. The job ends.

At no step does a query on any node count a row twice or miss one.

- **A node that stops answering** holds the job at its step. Before the outputs are published,
  the job reverts after 15 minutes. After, it waits; `GET /api/v1/cluster` (`rehome.jobs`) names
  the nodes it waits for. Removing a node that is gone for good from the cluster lets the job go
  on 30 seconds later. A reverted job also waits for every node before it ends (it keeps the
  file out of other jobs until then): the same removal applies.
- **On every node of a cluster sharing an object store, losing the read lease now cancels the
  queries in flight** (they fail as cancelled, recorded in `/api/v1/queries` with the reason),
  and any that had passed the check but not started, besides refusing new ones. That is what lets
  a job stop waiting for a removed node after a fixed time. Each such node also drops its query
  caches whenever it re-syncs with the leader, and reads what jobs leave out and acks their
  steps whatever its own configuration.
- **Failover.** A job whose driver stopped is taken up by the primary writer or the Raft leader
  once its 5-minute lease lapses (within 30 seconds on the same node after a restart). Taken up before its outputs
  are published, it is reverted: only the node that split the file writes them. A reverted job
  keeps its output keys out of reads for 20 minutes, in case a former driver's upload was still in
  flight. A step that fails for a passing reason (a leader election, a storage error) is retried
  where it is.
- **Refusals.** A job is refused while any node does not take part (an older release, or tiered
  storage, the Iceberg export or edge sync on there), with stream takeover on for a file that
  carries stream coverage (its rows settle a writer's batches, which a move cannot divide among
  the new files; a file without coverage is moved as with takeover off,
  [#1872](https://github.com/Basekick-Labs/arc/issues/1872)), or for a file on
  the cold tier; and a node that does not take part cannot join while a job is running. A node
  that crashes and comes back with one of those turned on while a job runs is let back in (it never
  left): its record says it is out of scope, and a job not yet switched to its new files is
  reverted, one already switched finishes
  ([#1859](https://github.com/Basekick-Labs/arc/issues/1859)); after a clean shutdown it rejoins
  once the job has ended. Meanwhile it reads what the job
  leaves out, keeps tiering and compaction off the job's files, and acks. While a job runs, a DELETE,
  retention, a database delete, a backup or a compaction of its files is refused or leaves them
  alone, on any node; so is any of them on a node that has not synced with the leader yet.

Not handled in this version:
- A backup on a node cut off from the cluster leader for longer than 90 seconds loses its mark
  ([#1857](https://github.com/Basekick-Labs/arc/issues/1857)): a job may then start under it. The
  node logs an Error saying so; take that backup again. Retention and DELETE started on a former
  primary fail safe: the cluster refuses their change to the job's file.
- A new job is started on the primary writer only.

## Change: a cluster records re-home jobs in its replicated state; an older build cannot join once every node supports it ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

**Who is affected:** clusters. Standalone nodes are not. No job runs on a cluster yet (a
re-home there is still refused); this is groundwork for moving a misfiled file's rows on a
cluster whose nodes share one object store. What changes now is the cluster's handshake and
state:

- **A new capability, and a flag.** Every node advertises re-home support at join. Once
  compaction commit is on and every node and Raft server advertises it, the leader turns the
  re-home flag on, as it does for compaction commit. From then on, a node whose build lacks it
  is refused at join ("re-home is enabled in this cluster: the node must run a release that
  supports it"). For a node on a 2026 release this changes nothing: compaction commit, which the
  flag needs, already turns those away.
- **What shows it:** `GET /api/v1/cluster` has `rehome` with `enabled`, on the leader the
  `blocking_nodes` keeping it off (or `(compaction commit is off)`), and the jobs in flight
  (none yet).
- **What it holds, for the next release step:** a job's source file and its outputs, its state,
  and which nodes have acknowledged each state. While a job owns a path, the cluster refuses
  every other manifest change to it (a DELETE, retention, a registration, a compaction) except
  the job's own switch-over, and a node removed from the cluster stops being waited for. A job
  goes forward only while every node takes part in jobs and has a configuration one can run
  under (no tiered storage, no Iceberg export, no edge sync) and stream takeover is off, and
  only on a file on the hot tier; otherwise it can only be reverted. No node says it takes part
  yet.

## Fix: at shutdown, a connection that never sent a request no longer holds the HTTP drain or is reported as a cancelled query ([#1840](https://github.com/Basekick-Labs/arc/issues/1840))

**Who is affected:** any node stopped while a client holds a connection it has not used yet: a load balancer or Kubernetes TCP check, a client pool that opens connections ahead of use, a port scan. Builds of `main` only: no released version has this. The TLS wait came with the newer fasthttp this release uses, which gives a new connection its 5 s from the end of the TLS handshake where the older one counted from accept; the false warnings came with the staged drain (#1644).

At shutdown the HTTP server waits a grace period for requests in flight, then cancels the queries still running, then closes the connections left. A connection that was accepted and had sent nothing was waited on as if a request were running on it.

- **With `server.tls_enabled`:** a connection that never finished its TLS handshake was left to its read timeout (30 s), longer than the drain, so the drain ran its whole grace and went on to cancel queries and close connections: 10 s at the default `server.shutdown_timeout` on a standalone node (the 8 s grace and a 2 s stage after it), for a stop that otherwise takes milliseconds.
- **With a shorter grace** (`server.shutdown_timeout` under about 21 on a standalone node): the grace ran out on such a connection, and the log then said `cancelled the queries still running after the grace period` and `closed connections still open after in-flight queries were cancelled`, with no query running. At `server.shutdown_timeout = 10` that used 3.3 s of the 4 s the shutdown hooks share. In a cluster at the default 30 it happens to a connection opened in the last second or so of the ready drain, which is when a load balancer is still checking.
- **Now**, during the drain, a connection that has never sent a request is closed once it is 5 s old, counted from when it was accepted, with or without TLS, and at the end of the grace if that comes first. `cancelled the queries still running` is logged only when a request is in flight at the end of the grace, and `closed connections still open` only when the drain closed one; the unused ones are reported as `closed connections that never sent a request`, at info level. A keep-alive connection between requests is not counted as a query either.
- **Not changed:** the 5 s itself. It is the allowance Go's `net/http` gives a new connection at shutdown, and it protects a client whose first request, or handshake, is still on its way: a request that arrives inside it is answered. A stop can therefore still wait up to 5 s on a connection opened just before it. Requests in flight, idle keep-alive connections and the stage lengths are as before.

## Fix: `POST /api/v1/tiering/migrate` refuses a body it cannot read instead of migrating every database ([#1841](https://github.com/Basekick-Labs/arc/issues/1841))

The migrate endpoint ignored a body it could not parse, and treated it as an empty body, which migrates every database. In each of these cases a request meant for one database migrated all of them, and the answer looked like the scoped run that was asked for:

- `{"database": "x"}` sent with `curl -d`, which sends a form Content-Type;
- the same body sent with `text/plain` or no Content-Type;
- malformed JSON;
- a misspelt field such as `databse`.

Worse, `dry_run=true` sent as a form field was dropped, so a dry run asked for that way ran a real migration.

Now:

- An empty body migrates every database, as before.
- A body starting with `{` is read as JSON whatever its Content-Type. Unknown fields and anything after the object are refused.
- Form fields (`application/x-www-form-urlencoded`) are read by their documented names (`database`, `measurement`, `from_tier`, `to_tier`, `dry_run`), and `dry_run` now works. Unknown or repeated keys are refused, and so is a `dry_run` that is not `true` or `false`, including a bare `dry_run` or an empty `dry_run=`.
- In either form, a filter that is present but empty or `null` (`database=` from an unset shell variable, `{"database": ""}`, `{"database": null}`) is refused rather than meaning every database. To migrate every database, leave the field out.
- Anything else is refused, including a multipart body. A refused body answers `400` and nothing is migrated.

What may now get `400`:

- a client that sent extra fields in the JSON body;
- a form that used the Go field names the old decoder also matched (`dryrun`, `Database`, `fromtier`);
- `curl -F` (multipart).

Each must switch to the documented names or to JSON. `POST /api/v1/tiering/reconcile` likewise refuses an unknown field instead of running its default 48-hour window.

## Fix: with Flight SQL on, a stop with no Flight query running no longer exits 1 on a short shutdown timeout or an unused internal connection ([#1834](https://github.com/Basekick-Labs/arc/issues/1834))

**Who is affected:** builds of `main` with `flight_sql.enabled = true`. Flight SQL is new in this release, so no released version has this.

Flight SQL runs its statements over an internal loopback HTTP listener, which is drained last when the Flight SQL listener stops. The drain waited for every connection on it to be gone, checking only every 100 ms, and reported a timeout as a failed stop: `shutdown internal Flight SQL query bridge: context deadline exceeded`, exit code 1, after an otherwise clean shutdown.

- **Short timeouts:** with `server.shutdown_timeout` of 2 or less on a standalone node, and 5 or less in a cluster, the drain gets less than 100 ms. A node that had served a Flight statement then exited 1 on every stop, because the connection kept open for the next statement was still closing when the drain ended.
- **Any timeout, the default included:** a connection to the internal listener that was opened and not yet used was not closed for 5 s, so it failed the stop after the whole drain (2 s at most). The internal HTTP client leaves such connections behind after concurrent Flight statements: at the default `server.shutdown_timeout`, a node stopped after a burst of 80 concurrent statements exited 1 on every stop.
- **Now** the drain waits for requests, not connections. A connection with no request in flight is closed, and a stop with nothing running takes a few milliseconds instead of 100. The stop still fails, as before, when a request is still running at the end of the drain (a handler that has not returned, or a result still being streamed): the error now says how many, and their connections are closed.
- **Also fixed:** a stop that arrives in the instant before the Flight SQL listener starts serving no longer reports `grpc: the server has been stopped` as a failure.
## Fix: a replicated compaction output is always recorded in the receiving node's tiering index ([#1831](https://github.com/Basekick-Labs/arc/issues/1831))

On a node-local-storage cluster, each node moves its staged copy of a committed compaction output into place and records the file in its tiering index as hot. When the pull the commit queued was still waiting (a backlogged pull queue), the 30-second staged sweep moved the copy into place itself, without recording it. The queued pull then found the file present and skipped it without recording it either.

The node had the file on disk and in the cluster manifest, but tiering did not know about it:

- No drop or failure counter moved, and nothing was logged.
- `POST /api/v1/tiering/migrate`, which does not scan first, left the file out, and a dry run did not list it.
- Only the next scan picked it up: the one at startup, the scheduled migration cycle's, or `POST /api/v1/tiering/scan`.

One deployment saw 4 such files on its writer over about 190 daily compaction cycles.

Now the sweep (and every other direct move of a staged copy) records the file, as the puller's own adoption always did. It also checks that the manifest still registers the output, and that it is still hot, before counting a copy already at the path as adopted. Adoptions by the puller now count in the puller's `tier_registered`; the sweep's direct adoptions record the file but are not counted there.

If you saw this before upgrading, `POST /api/v1/tiering/scan` on the affected node before migrating records what was missed; a non-zero `hot_rows_written` in its answer can mean files had been left out.

## New: moving a misfiled file's rows to the directories they belong to, on a standalone node ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

A file is misfiled when it holds rows whose time is outside the hour or day of the
directory it sits in. Queries that filter by time do not read those rows, compaction
leaves the file alone and lists it, and a file spanning more than one day is left out of
its Iceberg table. `POST /api/v1/rehome` with `{"path": "<storage key>"}` now moves the
rows: it writes one file per UTC hour (or per UTC day, for a file in a day directory) in
the directory each belongs to, and removes the file. `"dry_run": true` still only
reports.

On a file holding rows of two days, a query for the second day returned 1 of 2 rows
before and 2 of 2 after; unfiltered queries returned the same rows throughout.

**Where it runs in this release.** A standalone node with local storage. It is refused,
with the reason, on a cluster, with edge sync, and on storage that is not local; those
come in later releases. With the Iceberg export or tiered storage enabled it runs as
described in the next two sections. The dry run works everywhere, one job at a time.

**What a query sees while a job runs.** Every row, once. While the new files are put in
place queries keep reading the original; once all are in place queries read the new
files; the original is removed after every query planned before that switch has
finished. A query in flight at the moment the original is removed can get the existing
"data read by this query was removed while it ran; retry the query" answer (503). With
eight clients querying in a loop through 19 jobs, 5,114 answers were right, 27 were that
retry answer, and none was wrong.

**What happens on a stop.** Each step is recorded in a journal file under `_rehome/` in
the storage root. At the next start a job that had begun to switch queries over is
finished, and one that had only staged its files is dropped (the file is as it was;
send the request again). The process was killed a few milliseconds into a job sixteen
times, four of them part way through the move; every restart ended with the right rows
and nothing left behind.

- **A job that stops part way in a running process** (a storage error) keeps its record
  and its files stay owned. `POST /api/v1/rehome/<id>/resume` takes it up again, and so
  does the next start.
- **A job that cannot be taken up** is kept as recorded, logged at Error with what is
  wrong, and listed by `GET /api/v1/rehome` as `stuck`. That happens only when a file the
  job recorded was removed or changed by hand: the file being moved while queries still
  read it, an output after queries were switched to the outputs, or another file sitting
  at an output's name. Until it is put right, queries may be missing that file's rows.
- **A journal from a run under another configuration.** If the node has since become a
  cluster member, or tiering or edge sync was turned on, a job that
  had not switched queries over is undone, and one that had is kept as recorded with an
  Error line saying how to finish it.
- **Queries that do not end.** If queries in flight when the job starts have not ended
  after 15 minutes, the job is undone before it moves anything. After the switch the job
  waits for as long as it takes, with the original kept and left out of queries.

**What leaves a job's files alone while it runs.** Compaction, DELETE (the response is
207 naming the files left for a retry), retention (it takes the file on its next run),
and a database delete (409). A backup or a restore and a job refuse each other (409).

**What is refused, with the reason.** Besides the deployments above and what the dry run
refuses: a file whose rows all belong where it sits; a file a DELETE rewrote whose
rewrite is still beside it (run a DELETE on the measurement first, which removes the
stale copy); a file another operation already leaves out of reads; a measurement with a
compaction job open; a file another job owns.

**Output files.** Named like ingest files (hour) or `..._daily.parquet` (day), ZSTD,
with Arc's dedup metadata carried over. Rows whose time is NULL or infinite stay in the
original's directory, in a file of their own. The staging directory `_rehome_staged/`
holds a job's files before they are moved into place; backup skips it.

**Audit.** A job start is recorded as `rehome.started`.

**Startup.** Every node now reads `_rehome/` in its storage root at start, on any
storage: one listing of a prefix that is empty unless a job was left. A record there
that cannot be read stops the start and names the file.

**Not in this release:** undoing a job by request, and metrics for jobs.

## New: moving a misfiled file's rows with the Iceberg export on ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

`POST /api/v1/rehome` now runs with `iceberg.enabled = true` on a standalone node with
local storage. A file spanning more than one day, which the export leaves out of the
table (#1754), is in the table once it is moved: on such a file the table went from 1 of
4 rows to 4 of 4, and `arc_iceberg_files_left_out` returned to 0.

**The table never loses the rows on the way.** The file being moved stays in storage,
left out of Arc's queries, until the export has published a version of the table that
does not name it; and the export commits that file's removal together with the files
that replace it, in one snapshot, whatever days they are in. Other new files of the
measurement are not pulled into that commit: they are registered in chunks as before.

With a reader counting the table's rows in a loop through 14 jobs, compaction on, 259
reads were right, none was wrong and none failed. The process was killed a few tens of
milliseconds into a job 24 times, 11 of them while the job waited for the export; every
restart ended with Arc and the table holding the same rows.

**What a job does differently with the export on.**

- **Before it changes anything**, it asks the export for a pass of the measurement's
  table, and a second time if the first fails (a pass can fail once on a file another
  operation removed under it). If both fail, or the export is paused or does not export
  the table yet
  (a namespace awaiting migration), the job is refused with the export's reason and the
  file is as it was.
- **After queries use the new files**, it asks for a pass again and removes the old file
  only when the published table does not name it. `GET /api/v1/rehome/<id>` shows
  `waiting_for` while it waits.
- **That wait has no time limit.** If the export cannot commit (a full or read-only
  warehouse, a schema conflict that appeared since the job started), the old file is
  kept and the job stays `running`, with a Warn line every five minutes carrying the
  export's error. Arc's queries are right all the while. Until the job ends, backup,
  restore, a delete of that database and other re-home jobs (dry runs included: one job
  runs at a time) are refused, and a row DELETE or retention leaves the job's files for
  a retry.
- **Ending the wait by hand.** Repair the export and the job ends by itself. Or
  `POST /api/v1/rehome/<id>/resume` with `{"skip_export_wait": true}` removes the old
  file without the export's commit: the table then names a file that is gone, and
  readers of the table fail on it, until the export next commits. That later commit is
  an ordinary one, no longer tied to the job: if it stops part way, the table can be
  published for a while without some of the file's rows. The request is honoured once
  the export has answered the job at least once.

**What is refused with the export on.** A file holding rows with a NULL or infinite
time: the export cannot give a file of only such rows a day, so splitting them off
would take them out of the table. The dry run reports them as `remainder_rows`.

**Things to know.**

- The check before the job cannot see a fault that only shows when the export writes.
  With the table's metadata directory made read-only, the check passed (the table needed
  no change), and the job waited after switching queries until the directory was writable
  again.
- A job found at start past its commit is left as recorded until the export exists,
  and taken up then, not inline during startup. One whose old file is already gone ends
  without asking the export.
- The export turned off and on again across a job is an export-off period like any
  other: with it off the job ends without asking, and a table left from before names
  the old file until the export is on again and reconciles.
- Each job adds two passes of one table to the export's work. At
  `iceberg.retain_snapshots` 1 or 2 passes of a table are spaced two seconds apart, so a
  job takes that much longer.

Run with these `iceberg.` settings at non-default values, four jobs each, Arc and the
table agreeing after every one: `ingest_append_enabled = false`, `warehouse` set to a
subdirectory of the storage root, `layout = "colocated"`, `retain_snapshots = 1`,
`ingest_export_interval = 0`, `reconcile_concurrency = 1`.


## New: moving a misfiled file's rows with tiered storage on, for a file on the hot tier ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

`POST /api/v1/rehome` now runs with `tiered_storage.enabled = true` on a standalone node
whose hot tier is local storage, for a file that is on the hot tier. On a measurement
with files on both tiers, a query for the misfiled rows' day went from 1 of 13 rows to
13 of 13 across 12 jobs. A daily file spanning three days was split into three day
files, and the next `POST /api/v1/tiering/migrate` moved them to cold storage with no
scan in between.

**A hot file that cold storage also holds is refused** (a migration that kept its hot
copy, a file restored to hot from a backup), whatever the file's name: splitting the hot
copy would leave the cold one to be read beside the new files.
`POST /api/v1/tiering/reconcile` finishes such a migration. A file that is on the cold
tier only is moved as described in the next section.

**What changes with tiering on.**

- **Tiering leaves a job's files on the hot tier.** The file being moved and the files
  replacing it are not migration candidates while the job runs; a migration that had
  already picked one stops before its copy, or removes its copy before the tier row
  changes. `POST /api/v1/tiering/migrate` during a job reported no candidates for them
  and moved them after it.
- **The job writes the tier rows.** The new files are recorded as hot before queries are
  sent to them, and the old file's row is removed with it. A measurement whose other
  files are all on the cold tier is read from hot only when it has a hot row, so without
  this the new files would not be read until the next tiering scan.
- **Queries across both tiers are right throughout.** With eight clients querying a
  measurement that has cold files through 12 jobs, 6,291 answers were right, 6 were the
  "retry the query" 503, none was wrong. The process was killed part way 20 times; every
  restart ended with the right rows, and with hot files and hot tier rows agreeing.
- **With the Iceberg export on too**, Arc and the table agreed after each job and after
  the new day files were migrated to an S3 cold tier.

**What is refused, with the reason.**

- A file being moved to cold storage, or on the hot tier with a copy in cold storage.
- Any file while cold storage cannot be asked: `tiered_storage.cold.enabled = true` and
  the cold tier could not be reached at start, or tiering is not running at all (no
  licence). With no cold tier configured there is nothing to ask and the job runs.
- A file with rows before 1970: tiering records no file in such a directory, so the new
  files there would not be read.

**Things to know.**

- A job found at start past its commit, with tiered storage or the Iceberg export
  configured, is taken up once startup has wired them, not inline. If tiered storage now
  holds a copy of its file (the job was recorded under another configuration), a job
  that had not switched queries is undone; one that had is finished, with an Error line
  saying the cold copy must be removed by hand. If tiered storage cannot be asked at
  that moment, a job that had not switched queries is kept as recorded for
  `POST /api/v1/rehome/<id>/resume`. The resume asks tiered storage again before it
  goes on: the job stays as recorded while the answer cannot be had, and is undone if
  cold storage turns out to hold its file.
- `tiered_storage.cold.enabled = true` with the Iceberg export on and no usable cold tier
  leaves the export unstarted (as before). A job then gets "the Iceberg export is not
  started" as the export's answer at once instead of waiting for it.
- A migration that meets a file a job owns before copying it skips it: it is not an
  error and not "not attempted". One that had already copied the file when the job took
  it removes the copy, logs a Warn, and counts that file as failed for the cycle.

Run with `tiered_storage.default_hot_max_age_days = 1`, `migration_batch_size = 1` and
`migration_max_concurrent = 1` (non-default) together with the Iceberg export.


## Fix: Iceberg reconcile passes fail on a table that has a cold file the export must read ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

With the Iceberg export tracking an S3 cold tier, reconcile passes of a table failed
once the table held a cold file the export has to read: one it leaves out of the table
because its rows span more than one day (#1754), or cold data that predates the export
and could not be registered. The first pass that read the cold tier went through. Every
later pass that had anything to register or remove failed with

    build data file for s3://...: open s3://...: operation error S3: GetObject, context canceled

and committed nothing, until the server was restarted, which bought one more pass. Files
migrated in the usual way do not cause it: the export takes them from the table's own
entry and never reads them.

Measured with `iceberg.ingest_append_enabled = false` and `tiered_storage.cold.s3_prefix`
set, on a table with one such cold file and an ingest flush every nine seconds: the
first two passes went through and the next eight failed, one per flush, so nothing more
reached the table. With ingest appends on (the default) new ingest files reach a table
without a pass; what needs a pass (compactions, deletes, retention, files moving to
cold) is what would have been stuck. That case was not measured.

**The cause.** The export kept one reader for the cold tier and built it with the context
of the pass that first needed it. iceberg-go's S3 reader issues every request under the
context it was built with, and a pass's context is cancelled when the pass ends. A
reader is now built for each pass, so a cold read is still bound by its pass's deadline.

**What to do.** Nothing. A table that was stuck catches up at its next pass.

## New: moving a misfiled file's rows when the file is on the cold tier ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

`POST /api/v1/rehome` now takes a file that is in cold storage: a daily file that spans
days and has since been migrated. It is read from cold storage, its rows are written to
new files on the hot tier in the directories they belong to, and it is removed from cold
storage with its tier row. Day files among the new ones migrate again on a later
tiering cycle. `"dry_run": true` works on a cold file too, and reports what the real run
would refuse. Standalone node, hot tier on local storage.

On ten cold daily files each holding one row of another month, a query for that month
returned 1 row once the first file was moved and 10 once all were; unfiltered queries
returned the same 32 rows throughout.

**What a query sees.** No row twice, on either tier, and every row a query could see
before. While the new files are put in place queries read the cold file; once they are
in place queries leave it out by its address in cold storage and read the new files;
then it is removed.

A time-filtered query sees a misfiled row only once its file has been moved, as before
the job. One case looks like a loss and is not: a query for a range with no directory
at all reads the whole measurement, so it sees misfiled rows of that range by accident.
The first job that creates a directory for the range ends that: the query is pruned to
the directory and returns only the rows moved so far. In the run below a query for a
month with no directory returned 10 rows before any job, 0 while the first job had
created the directory and not yet switched over, 1 after it, and 10 after the tenth. With eight clients
querying through 10 jobs, 2,812 answers were right, none was a retry, none was wrong.
With a job held switched-over and the old file still in cold storage, every query
returned the exact count.

**Checked before anything changes.**

- The file must be cold and only cold: a tier row that says cold, the object in cold
  storage, nothing at that name on the hot tier, no DELETE rewrite of it beside it on
  either tier. Otherwise the job is refused with the reason.
- The job asks the query engine what name it reports for the file, through the same
  kind of read a query makes, and is refused unless that is the name Arc would leave it
  out by. That name depends on the cold backend and its configuration; if they ever
  differed, queries would read the old file beside the new ones.

**Removal.** The query caches are dropped, the object is deleted, cold storage is asked
that it is gone, and its tier row is retired; the job does not end before that. If the removal fails the
job stays where it is, the old file left out of queries, and
`POST /api/v1/rehome/<id>/resume` tries again.

**Other operations on the cold tier.** A row DELETE and retention leave a cold file a
job owns: DELETE answers 207, says how many files a re-home job holds and points to
`GET /api/v1/rehome`; retention takes the file on its next run. Neither can remove the
file after the job has read it: DELETE asks who owns each file under the lock the job
checks its file under, and retention holds a claim on each file, which a job cannot
start against, from before it looks at the file until it moves on.

A tiering sync that lists cold storage while a file is being removed (by a job, a cold
DELETE or retention) no longer records that file again: it skips any file whose cold
row was retired since its listing began. It used to come back as a cold
row with no object, reported on `/health` and in the log as a lost file.

**With the Iceberg export on.** As for a hot file: the old file stays in cold storage
until the published table no longer names it. One case specific to a cold file is
closed in the export: a pass whose listing happened to span the whole switch would
have published the cold file beside its replacements; such a listing is now taken
again.

**Things to know.**

- The job's record names the cold store the file was in. Taken up after a restart
  against another bucket or prefix, with no cold tier, or with tiering not running, the
  job is kept as recorded and listed as `stopped`: "not there" is not taken for
  "removed". It goes on holding backup, restore and a delete of that database off until
  it ends: put the cold store back as it was and `POST /api/v1/rehome/<id>/resume`.
- Do not roll back to an earlier build while a job on a cold file is recorded
  (`GET /api/v1/rehome`): an earlier build reads the record as a hot file's, ends the
  job, and leaves the old file in cold storage beside the new ones.
- A file that is not on the hot tier while cold storage cannot be reached is answered
  502, not 404.
- A query planned while a measurement's last cold file was still there, and run just
  after it went, used to be answered as an empty measurement. It is now answered 503
  "retry" for a minute after Arc removes cold files (a job, a cold DELETE, retention).
- The new files are on the hot tier until tiering migrates them: a large cold file
  needs that much room on the hot volume, twice while the job runs.
- Refused: a cold file with no tier row (run `POST /api/v1/tiering/scan`), a quarantined
  one, a file on both tiers.

## Fix: a migrate cut off by its time budget no longer leaves files read twice ([#1830](https://github.com/Basekick-Labs/arc/issues/1830))

`POST /api/v1/tiering/migrate` ran everything on one context bounded by `tiered_storage.migration_timeout`, including releasing the hot copies of the files it had just moved. When the budget ran out, the files copied in the last second or so were already marked cold, but their manifest update was refused on the expired context. They were left in cold, still hot on every node, and still in the cluster manifest.

Queries read both copies, so their rows were counted twice. A reproduction on a 3-node cluster read 750 rows out of 600. Nothing cleaned them up unless the scheduled migration cycle ran, and the call still answered `200 "Migration completed successfully"`.

Now:

- **Files already moved are always released.** Their hot copies are released on a context of their own, which runs up to 2 minutes past the budget and ends when tiering stops.
- **A copy cut off by the budget leaves its file hot, with nothing in cold.** An upload the object store finished just as the budget ended used to leave a cold object under a row that still said hot, and its rows were read from both tiers. That object is now removed.
- **Once the budget has ended, or tiering is stopping, no new copy or batch starts.** This applies to the scheduled cycle too.
- **Tiering's shutdown waits for API work.** It waits up to 60 seconds for an API-triggered migration or reconciliation to finish before the metadata database closes.
- **The answer says what happened.** `status` is `completed` or `incomplete`, and the body now includes `candidates`, `not_attempted`, `kept_hot_copies` and `timed_out` alongside `migrated` and `errors`.
  - A run cut off by the budget answers 200 `incomplete` with the count not attempted. Run it again to move them.
  - Files left both cold and hot (a manifest update or hot delete that genuinely failed) make it answer **500** `incomplete` with `kept_hot_copies`.
  - A copy cut off by the budget counts as not attempted, not as an error. So do the files a run did not reach because the node stopped being the primary writer, tiering was stopping, or a DELETE held them.
- **New: `POST /api/v1/tiering/reconcile`** (admin, tiering licence, primary writer only). It runs on demand what the scheduled cycle runs after migrating:
  1. the cold metadata sync and hot scan;
  2. the orphan sweep: files whose row says cold but whose hot copy is still there are removed from the manifest, then from hot storage, after the cold copy is verified;
  3. the manifest sweep.

  This is how an API-driven deployment cleans up files left both cold and hot, without enabling the schedule or restarting. The orphan sweep looks back 48 hours by default; `{"window_hours": N}` (up to 8760) reaches older files. The body is read as JSON whatever its content type, and the answer echoes the window used. It answers:
  - 503 when the cold tier cannot be listed;
  - 409 on a non-primary node, while a migration runs, or without a usable cold tier.
- The log line for kept files now says that they are read twice and what finishes them. Before, it said reconciliation would.

**Rebuilding a node from the cold tier on fresh volumes:** each node rebuilds its cold rows from the cold listing at startup. That listing must fit in `tiered_storage.scan_timeout`; check `last_scan` in `GET /api/v1/tiering/status`. Fresh volumes also wipe what the data directory holds besides hot data: by default the Raft state (`raft/`), `arc.db` (API tokens, tier rows, retention policies, continuous queries) and the WAL. A writer whose Raft state is empty bootstraps a new cluster, so rebuild every node together, never one node's Raft state alone.


## Fix: with Flight SQL on, a normal stop no longer exits 1 and in-flight Flight queries get their grace period ([#1835](https://github.com/Basekick-Labs/arc/pull/1835))

**Who is affected:** builds of `main` with `flight_sql.enabled = true`. Flight SQL is new in this release, so no released version has this.

At shutdown the Flight SQL listener scales its waits (5 s for in-flight queries, 2 s before open connections are closed, 2 s for the internal drain) to fit in half of the time the shutdown hooks have left. That scaling multiplied two durations in nanoseconds and overflowed, so the waits came out wrong, most of them negative, with `server.shutdown_timeout` from 10 to 55 on a standalone node and from 19 to 80 in a cluster, the default 30 included. Shorter timeouts were scaled correctly, and longer ones are not scaled.

- **At the default, standalone:** every wait was negative. An idle node stopped with `Flight SQL server did not stop after its connections were closed` and exit code 1 on every `SIGTERM`, after an otherwise clean shutdown (the buffer flush, the WAL and the database all closed). A supervisor that reads the exit code saw a failed stop.
- **In-flight Flight queries** were cancelled the moment shutdown began, and their connections closed, instead of getting the grace period. In a cluster at the default the grace was negative too.
- **Now** each wait is its share of the budget, never negative and never more than the unscaled value: at the default 30 s, in-flight queries get about 2.7 s on a standalone node and about 1.4 s in a cluster, and an idle listener stops at once with exit code 0.
- **What this changes for the rest of shutdown:** the Flight SQL listener stops before the HTTP server, and its waits were never taken before. A Flight client that stalls now uses up to half of the hooks' time (6 s at the default, standalone), and the HTTP drain gets what is left. The time kept for the buffer flush, the WAL and the database is not touched. `docs/operations/flight-sql.md` has the numbers.
- **Fixed separately:** a clean stop could still exit 1 with `server.shutdown_timeout` of 2 or less (5 or less in a cluster), where the internal drain gets less than 100 ms. See the #1834 entry above.

Reported, with the cause and a patch, by Grump.
## Fix: a continuous query's read of its source runs under the run's timeout ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

A continuous query ran its aggregation statement without the run's context. The run's
10-minute timeout therefore never reached DuckDB: the statement could wait for a pooled
connection, and then run, with no bound at all. The statement now runs under the run's
context, for scheduled and manual runs alike.

What changes for a running system: a run that reaches its 10 minutes now fails instead
of running on, and so does a run in flight when its continuous query is updated or
deleted or the scheduler is reloaded. A failed run is recorded as failed, its window is
not marked done, and the next run covers it.

Found while preparing the move of misfiled files (#1825), which needs to know that no
read planned before a file swap is still on its way to the files. For that:

- **Reads in flight are counted.** Every query route, Flight SQL (which goes through
  them) and every continuous query run is counted from before its SQL is built until its
  statement has run. Nothing uses the count yet. The file move will wait on it, together
  with separate handling for the readers it does not cover (the Iceberg export, DELETE,
  retention, compaction, other nodes of a cluster).
- **A query reads what it must leave out before it lists directories, not after.** The
  list of files a compaction or a DELETE is replacing was read after the query had
  worked out which hour and day directories exist, so a read could pair an older view of
  the directories with a newer list of files to leave out, and miss rows. With the new
  order the opposite pairing is possible: a replacement that lands in between is read
  beside the file it replaces, and its rows count twice in that one read. That window
  already existed during a compaction; it is slightly longer now. It was chosen because
  a double count is visible and gone on the next read, where missing rows are silent.
## Fix: a leader elected late keeps its node record current ([#1829](https://github.com/Basekick-Labs/arc/issues/1829))

A node writes its own record in cluster state (its addresses, version and role) only while it is the Raft leader; a follower's record is written by the leader when the follower joins. The leader used to write its record once, and only if it won an election within 30 seconds of starting. A writer elected later kept the record of its previous process. That happens after a full cold start where peers are not yet resolvable, and after a failover hours in.

Every node then showed that leader with its old version and old `api_address`. After a cold start that reshuffled pod IPs, that address belonged to a reader:

- Writes forwarded to the leader went to the reader, which refused them with 508. Nothing was written.
- Cache invalidations meant for the leader also went to the reader, so the leader could serve stale cached results until they expired.

It lasted until the leader restarted.

Now:

- **The leader keeps its record current for as long as it runs.** It writes the record the first time it is leader in a process, whenever that is. After that it checks once a second and rewrites the record if its name, role, cluster name, address, API address, version or capabilities in cluster state differ from its own. A record that matches is not rewritten. Core count, state and writer state are not compared. As at startup before, the write clears a "standby" mark on a writer that becomes leader; the primary designation is kept.
- **A join the leader cannot record is refused.** If writing the joiner's record failed, the leader still answered success, and the joiner stopped trying. The joiner kept its old record for the rest of its life. Now it gets an error and tries again on its next discovery attempt.
- **Peers' versions are no longer blank after a join.** The join response carries no version, so a node that had just joined showed an empty `version` for its peers in `GET /api/v1/cluster`. It now keeps the version it already knows from cluster state.

Restarting the leader after a cold start is no longer needed.

## New: a dry run that shows where a misfiled file's rows belong ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

A file is misfiled when it holds rows whose time is outside the hour or day of the
directory it sits in. Compaction leaves such a file alone and lists it
(`GET /api/v1/compaction/misfiled`), the Iceberg export leaves a day-spanning one out of
its table (`arc_iceberg_files_left_out`), and queries that filter by time do not read
its rows.

`POST /api/v1/rehome` with `{"path": "<storage key>", "dry_run": true}` reads one file
and reports where its rows belong. It changes nothing in storage. Moving the rows is
not in this release: a request without `dry_run` is refused.

- **What it does:** copies the file to a scratch directory on local disk, splits its
  rows into one file per UTC hour (a file in an hour directory) or per UTC day (a file
  in a day directory), checks the split, reports, and removes the scratch directory.
- **What it reports:** rows, how many belong in another directory, and per destination
  the directory and the rows (up to 5,000 destinations listed; the total is always
  given). Rows whose time is NULL or infinite have no directory to belong to and are
  counted apart. For each file the split wrote: its rows, size and time range.
- **A very scattered file:** when the rows belong to more than 500 hours or days, the
  destinations are reported and the split is skipped (`split_checked: false`, with the
  reason). A split reads the file once per output.
- **The check:** every output holds the rows counted for it, so together they hold
  every row once, and every output's times are inside its directory, by the same read
  compaction's own check uses.
- **What is refused, with the reason:** a file whose `time` is not a timestamp (a
  string, an integer, a date) or is in nanoseconds, since splitting it would change
  its values; a file holding a time outside years 0001 to 9999 (usually nanoseconds
  stored as microseconds), which no directory can hold; a file with two columns named
  `time` in different cases; a path that is not a data file of a database and
  measurement; files of an edge-sync spoke namespace; a file that is not in this node's
  storage, which includes files on the cold tier.
- **A job that fails** says which step failed. The error itself is in the server log
  under the job's id, not in the response.
- **GeoParquet:** the split's files carry Arc's own Parquet metadata (dedup columns and
  markers) and not the `geo` entry, as a DELETE rewrite does.
- **How it runs:** in the background, one job at a time, on a private single-threaded
  DuckDB instance with a 1 GiB memory limit, for up to 30 minutes.
  `GET /api/v1/rehome/<id>` returns the job and its report; `GET /api/v1/rehome` lists
  the jobs this process remembers. Reports are kept in memory and are gone at a restart.
- **Cost:** local disk for a copy of the file and for the split, about twice the file's
  size plus what DuckDB spills, in the staging directory DELETE uses. Leftovers of a
  process that stopped mid-job are removed at the next start; a shutdown stops a running
  job.
- **Who may call it:** admin tokens only.

## Fix: compaction no longer merges a file whose rows belong to another hour or day ([#1825](https://github.com/Basekick-Labs/arc/issues/1825))

A partition is a directory, and a compaction job merged every file of it without
looking at their times. A file sitting in the wrong directory was merged with its
neighbours into an output holding rows of two days. The Iceberg export cannot give such
a file a day and leaves it out (#1754), so rows that were in the Iceberg table left it:
in the reproduction the table went from 5 rows to 1 while Arc still returned 5.

Arc's own writers do not put a file in the wrong directory: ingest files every row
under the UTC hour of its time, and imports and continuous queries go through ingest. A
normal workload (three days, rows on hour and day boundaries, one request spanning
midnight, late rows) went through hourly and daily compaction with nothing refused and
the same rows. Such files come from data written by old versions (directories by
ingestion time before 26.02.1; times before 1970 before 26.06.2), from a restore or an
edge sync of a store that holds them, or from a file placed by hand.

- **A job reads each input's time range first.** The range is the file's times as the
  compaction would write them, read from the rows, not from footer statistics. The
  hourly tier requires them inside the directory's hour, the daily tier inside its UTC
  day.
- **A job with such an input compacts nothing.** No output, no input removed. The file
  is remembered on local disk (`{compaction.temp_directory}/misfiled.json`), and the
  partition is judged from then on as if the file were not there. So the first cycle
  after the upgrade may compact nothing in an affected partition, and the cycle after
  compacts the rest, if the rest is still enough files for a job: an hour left with
  fewer than `hourly_min_files` waits for the daily tier, and a day older than
  `daily_skip_file_age_check_days` left with fewer than `daily_min_files` gets its
  daily file as a settled day (#1774).
- **A file in the right day but the wrong hour is healed by the daily tier.** The hourly
  tier leaves it alone; the daily tier takes it, and the day's file is read by every
  query of that day.
- **A file whose rows are of another day stays where it is.** It is never compacted, so
  it never becomes part of a daily file and never moves to the cold tier. Queries that
  filter by time do not read its rows and queries that do not filter do; that was
  already so before this release, and is unchanged. Moving such a file to the right
  directories is tracked in #1825.
- **Outputs that already span two days are not undone.** One that becomes an input of
  a later job is refused and listed like any other file. A daily file of a day past
  `daily_skip_file_age_check_days` is never an input again, so it is not listed; the
  Iceberg export reports it (`arc_iceberg_files_left_out`).

What to look at:

- `arc_compaction_files_misfiled` (gauge): files compaction leaves alone on this node.
- `arc_compaction_inputs_misfiled_total` (counter): inputs refused by jobs.
- `GET /api/v1/compaction/misfiled` (admin): each file, its tier, its time range.
  `DELETE /api/v1/compaction/misfiled` (optionally `?tier=` and `?path=`) forgets
  them, so the next cycle reads them again.
- One Error log line per refused job, with up to three files and their ranges.

The list is per node, like the list of partitions too wide to compact: on a cluster the
compactor's list is the one in use, and a node that takes over compaction finds the
files again with one job per affected partition. A deployment that still holds data
filed by ingestion time (written before 26.02.1) will see most of it listed: that data
was merged into multi-day outputs by every earlier version.

`compaction.refuse_misfiled_inputs = false` (`ARC_COMPACTION_REFUSE_MISFILED_INPUTS`)
restores the previous behaviour: jobs do not check, and the list is neither read nor
used, so files remembered earlier are compacted like any other. The list stays on disk
for a later start with the check on.

The list holds up to 100,000 entries. Past that, a job's files are not remembered and
the job is refused again each time it runs, with a warning in the log.

The compaction subprocess now sets its time zone to UTC for every connection of its
DuckDB pool. Before, only the connection that ran the setting was pinned. A job runs its
statements one after another and so reused that connection; this makes it hold whichever
connection a statement gets.

## Fix: with `ingest.write_statistics = false`, Iceberg readers that filter by time no longer miss rows ([#1754](https://github.com/Basekick-Labs/arc/issues/1754))

An Iceberg table is partitioned by day, and the export takes a file's day from the
min/max of `time` in the file's Parquet footer. With `ingest.write_statistics = false`
the footer has none, and the file was registered under no day at all, without an error
or a log line. Readers skip such a file for any filter on time. Measured with DuckDB's
Iceberg reader on five rows over three days: an unfiltered read returned 5, a read of
one day returned 0 where Arc returned 2. With the default (`true`) nothing was wrong.

- **The ingest writer always keeps min/max for `time`.** Every other column still
  follows `ingest.write_statistics`. On a 1M-row flush with statistics off the writer
  took 34.6 ms with the time statistics and 34.3 ms without (six runs each; the runs
  overlap).
- **A file whose footer has no min/max for `time` gets its day from its rows.** The
  export reads the file's time column once and registers the file under that day. This
  covers files written before this release and files that reach storage another way
  (a restore, edge sync).
- **Entries already registered without a day are corrected.** The first reconcile pass
  of each table after the upgrade re-registers them under their day, up to 256 files per
  pass; a table with more is finished over the following passes, on the short retry
  interval. Nothing needs to be run, and the table's snapshot history is kept.
- **An entry whose file has no single day leaves the table.** If the rows of such a
  file span more than one UTC day, hold no time value, or cannot be read, the entry is
  removed and the file is counted in the metric below. Before, an unfiltered Iceberg
  read returned those rows and a read filtered by time did not.

On the store from the measurement above, the first pass of the new binary changed the
one-day reads from 0, 0 and 0 to 2, 2 and 1.

### New metric: `arc_iceberg_files_left_out`

A file that cannot be given a single day is left out of its table: its rows span more
than one UTC day, its time column holds no value, or (for a file whose footer has no
time statistics) its time column cannot be decoded. Those rows are readable through
Arc and missing from the Iceberg table, and until now the only sign was an Error log
line. `arc_iceberg_files_left_out` (gauge; `iceberg_files_left_out` in the JSON
metrics) is how many such files there are, as of each table's last reconcile pass
that went through all its new files. It is 0 on a node that does not run the export,
and after a restart until each table's first pass. The log line names each file and
gives the reason.

Arc's own writers do not produce such a file: ingest splits every flush by the UTC
hour of each row, and imports and continuous queries go through ingest. Compaction
merges all the files of one directory without looking at their times, so a file that
sits in the wrong day's directory becomes part of an output that spans two days. That
was reproduced by placing a file by hand; it can also come from data written before
26.06.2 with times before 1970, or restored or synced from such a store. The
compaction side is tracked separately.
## Breaking: `/metrics` and `/api/v1/metrics` need a token while auth is on

**Who is affected:** anyone scraping or polling Arc's metrics with authentication on, which is the default. That means Prometheus, Grafana panels, Telegraf, scripts, or a dashboard reading `/api/v1/metrics`. `/health`, `/ready` and `/ready/write` stay public for load balancers and probes. With auth off nothing changes.

`/metrics` (Prometheus text) and `/api/v1/metrics` with its sub-paths (`/memory`, `/query-pool`, `/endpoints`, `/timeseries/:type`) were served without a token. They carry operational counters, Go runtime and host details, and, with `iceberg.export_lag_table_series` above 0, database and measurement names (#1772). They now need a valid token while auth is on. Any token works, including a read-only one, so give the scraper its own read-only token.

**After upgrading,** a scraper without a token gets `401` on every scrape, so in Prometheus `up` goes to 0 for Arc and an `up == 0` alert fires. Add the token:

```yaml
scrape_configs:
  - job_name: arc
    authorization:
      credentials: <a read-only Arc token>   # sent as "Authorization: Bearer ..."
    static_configs:
      - targets: ["arc:8000"]
```

Use `authorization`, not `basic_auth`: Arc does not accept basic auth. For other clients, send `Authorization: Bearer <token>` or `x-api-key: <token>`.

**To keep them public,** set `auth.metrics_public = true` (`ARC_AUTH_METRICS_PUBLIC=true`). With auth on, Arc then logs a warning at start; with auth off the key changes nothing.

Reads of the JSON metrics family are no longer audited, whatever `include_reads` says, as `/metrics` already was not: dashboards poll it every few seconds.

## New: Iceberg export lag per table on /metrics ([#1772](https://github.com/Basekick-Labs/arc/issues/1772))

The node-wide `arc_iceberg_export_lag_seconds` and `arc_iceberg_export_pending_seconds` (#1750) say how late the export is, not which table is late. With `iceberg.export_lag_table_series` set to a number N (`ARC_ICEBERG_EXPORT_LAG_TABLE_SERIES`, 0–1000, default 0 = off), `/metrics` also has, labelled `database` and `measurement`:

- **`arc_iceberg_export_table_pending_seconds`:** per table, how long its oldest file waiting for an export has waited. Only the N tables waiting longest are shown, and only those past a floor, so a healthy table between two exports is not listed. The floor is twice `ingest_export_interval`, or twice `reconcile_interval` where the ingest trigger is off. The wait is counted in one of two ways:
  - from when the file was reported, with the ingest trigger on (a standalone node, or a cluster's exporting node);
  - from when it was written, for files a table has committed but not yet published, in every mode, counting files written after the process started.

  So with `ingest_export_interval = 0`, a table whose commits keep failing is not shown, and after a restart neither is one whose publishes keep failing, until it commits again.
- **`arc_iceberg_export_tables_pending`:** how many tables are past that floor, so falling out of the top N can be told from catching up.
- **`arc_iceberg_export_table_last_lag_seconds`:** per table, the longest time from an ingest file being written to its table version being published, among the files its last publish covered.
  - Only files written after the process started count.
  - Only publishes within the last hour count, or within three `reconcile_interval`s if that is longer.
  - Only the N longest are shown.

  A table missing from this series is not known to be healthy; it has only not been slow recently.

**These series name databases and measurements.** That is why they are off by default. Since this release `/metrics` needs a token while auth is on, unless `auth.metrics_public` is set (see above). Any valid token can read them, including a token scoped to one database, which then sees the names of every exported table. The JSON metrics endpoints never carry them. A node that is not the exporter reports none, checked at each scrape, so during a writer failover two nodes do not report the same table.

## Fix: in a cluster, compaction recovery no longer leaves the manifest listing an output it deleted ([#1811](https://github.com/Basekick-Labs/arc/issues/1811))

**Who is affected:** clusters whose compaction does not use the staged commit, which today means a cluster with a node too old to support it, or leftovers from before it was turned on. A fully upgraded cluster's jobs do not take this path.

When an interrupted compaction job is recovered and its output's stored size is not the size the job recorded, the output is deleted so the job is redone from its inputs. In a cluster that file may already be registered in the cluster manifest, and the node also keeps a record that registers it. The file was deleted from storage first and neither was touched: the manifest kept an entry no node could serve, and a restart registered the deleted file again.

- **The output leaves the cluster manifest before it leaves storage.** Recovery removes the node's record for the job, then the manifest entry, and only then the file. The same when the output is already missing and the job is given up, provided its inputs are all still stored.
- **If the cluster cannot be updated** (no quorum, or compaction is paused cluster-wide), nothing is deleted from storage. The recovery record stays under `_compaction_state/` and the next compaction cycle that covers that database tries again; nothing else does. Until then the job's inputs stay out of compaction.
- **An output that is not in storage is not registered.** Before a node registers a finished output it now checks that the file is there, at the recorded size. If it is not, it logs `Compaction output is not in storage as its completion record describes` once and looks again about once a minute.
- **Not changed:** a single node, and a node that could not join its cluster and runs standalone, recover as before.

Not known to be reachable without outside help: the product writes the record only after the upload reports success, so the size has to change afterwards (the object replaced or damaged, or a storage fault).

Checked on a two-node cluster (a writer and a compactor, local storage): an output was registered, its size changed on disk, and a compaction cycle run. Afterwards neither node's manifest listed it, the file, its recovery record and the node's record were gone, and the input was kept.
## Fix: on S3 and Azure, an object whose size the store does not report is no longer read as empty ([#1812](https://github.com/Basekick-Labs/arc/issues/1812))

**Who is affected:** S3, S3-compatible and Azure storage, only with a store or proxy that answers an object's metadata request without a content length. AWS S3 and Azure send it, and so does SeaweedFS (tested). Local storage and GCS are unchanged.

Arc read a missing length as a size of 0. Some consequences:

- **Compaction recovery** compares an output's size with the size its manifest recorded (#1809). A good output was taken for a partial upload: it was discarded and the job redone, or, with an input missing, the manifest was kept and reported every cycle under the wrong cause.
- **Other paths recorded the 0:**
  - the object-storage compaction finisher compares sizes the same way;
  - manifest repair could adopt an entry with size 0;
  - Iceberg cold-tier exports could write a file size of 0;
  - the registration outbox could take a pending file for one that was gone or rewritten.

**Now:** when the metadata response has no length, Arc reads the object's size from the store's listing of that exact key, which always carries one. Only when the listing has no size for it either is the result an error naming the object (`the object store response carries no content length`), never a size of 0. On a store like that, every size check keeps failing until the store or proxy in front of it sends the length.

## Fix: a crash while a stale local copy was being removed no longer deletes a newer file written to its path ([#1814](https://github.com/Basekick-Labs/arc/issues/1814))

**Who is affected:** clusters with local storage and replication on, where the stale-copy sweep removes local copies whose delete the node missed. Object storage and single nodes are unchanged.

To remove a stale copy, the sweep moves it into `_stale_local/` and checks it there. If a new file was written to the same path in that moment, for example by a pull, it is moved back. Two problems:

- **A crash in that window deleted the new file at the next start.** The file existed only under a random name in `_stale_local/`, and startup deleted every such name.
- **No fsync after the put-back.** A power loss soon after could lose the file at its path.

Now:

- **The aside records what it was.** A copy moved aside goes to `_stale_local/<id>.<inode>.<size>.<mtime>/<its path>`, so the name records what the stale copy was and where it came from.
- **At start, Arc removes a file there only when it is provably the stale copy.** That means it matches the recorded identity, or it is the same file as what its path holds (a put-back that finished).
- **Any other file there is kept for an operator,** under `_stale_local/keep-<id>.<inode>.<size>.<mtime>/<its path>`, and logged as an error. Compare it with what its path holds and move it back by hand if it is wanted. It is not put back automatically, because at start Arc does not yet know whether the manifest still lists the path. Replication pulls a listed path again only while catch-up is on and another node holds the file.
- **A put-back is fsynced** before its aside name is removed. If the fsync fails, the aside name stays, and the next start removes it once it finds the same file at the path.
- **This also runs with replication catch-up off,** where the sweep itself does not run, so a file kept from an earlier crash is reported. With replication off it does not run.
- **Older versions' asides** are flat files in `_stale_local/` and record nothing, so they are now kept as `keep-<name>`, with a warning, instead of being deleted.

## Fix: restoring a backup no longer leaves Iceberg tables stale or broken until the restart ([#1813](https://github.com/Basekick-Labs/arc/issues/1813))

**Who is affected:** anyone with Iceberg export on who restores a backup. On a cluster a restore never stages a catalog (`restore_metadata` is refused there), so only the fix to the version hint applies.

A restore of metadata stages the SQLite database, including the Iceberg catalog, and applies it at the next start. Meanwhile it wrote the backup's Iceberg files live, and the export kept running on the old catalog. Two things went wrong between the restore and the restart:

- **Warehouse outside the storage root (`iceberg.warehouse` elsewhere): a table could stop exporting for good.** Each export commit deletes metadata files older than its retention, and that could include the file the restored catalog row points at. After the restart, every pass of that table failed with `no such file or directory` until someone fixed it by hand. Reproduced with the backup one commit behind and three commits before the restart.
- **Warehouse in the storage root (the default): directory readers saw the backup's table.** The restore wrote the backup's `version-hint.text` over the live one. DuckDB `iceberg_scan` on the table directory, Spark, and anything else that reads the hint then read the table as it was at the backup: 3 rows instead of 18 in the reproduction. On a table with no new data, that lasted until the restart. Arc's own queries were not affected.

Now:

- **The export pauses for the restore.** A restore that stages a catalog pauses the Iceberg export before it writes anything, waiting up to 150 s for exports already running, and keeps it paused until the restart. The restore status shows `"iceberg_export_pause": "paused_until_restart"`. If the restore fails before it stages the catalog, the pause is lifted (`"resumed"`). If an export is still running after the wait, the restore is refused and nothing is written.
- **While paused:** no table is exported, and the log says so once (`Iceberg export is paused until the server restarts`). A `DELETE` of a database drops its data but not its Iceberg tables, and answers with `iceberg_cleanup: pending`, as when a drop cannot run. Repeat it after the restart. A second restore before the restart is not held up by the first one's pause; the last restore before the restart wins, as before.
- **A restore no longer overwrites a table's live hint.** When a table's metadata directory already has a `version-hint.text`, the restore leaves the hint and every `v<N>.metadata.json` there as they are, and counts them as `iceberg_discovery_files_kept`. If the check for a hint fails, the restore logs it and leaves them out too.
  - A directory with no hint gets the backup's discovery files, but none is written over a file that is already there.
  - The rest of the table's metadata is restored as before.
  - After the restart, the restored catalog row is moved onto the live published version, as since #1613.
- **Trade-off:** a restore can no longer repair a damaged live `version-hint.text` or `v<N>.metadata.json`. Delete the damaged file first if you need the backup's copy.
- **Left behind on a warehouse outside the storage root:** the metadata files of the versions exported after the backup stay there unreferenced. Nothing prunes a warehouse outside the root, restore or not.

## Fix: on object storage, a failed storage listing no longer leaves a day's daily files out of a query ([#1810](https://github.com/Basekick-Labs/arc/issues/1810))

For a time-ranged query on object storage, Arc lists each day's directory to learn which hours exist and whether the day holds daily files. When that listing failed, Arc guessed: it assumed every requested hour existed (a missing one then failed the query with a 503) and left the day's daily files out, so the query could answer 200 without them.

- **A day that cannot be listed is now read whole,** on S3, Azure and GCS. Arc hands DuckDB one pattern for the day (`<day>/**/*.parquet`) in place of that day's hour and daily patterns. DuckDB lists the day itself and the query's time filter still decides which rows count, so the answer is complete, and a missing hour no longer fails the query.
- **Cost, only when the listing fails:** a query over one hour opens every file of that day. Measured with the built binary on SeaweedFS S3, on a day of 3,004 files, with the day's listing failing: the same row counts as without the failure, in 0.6 to 4.3 s where the same queries took 0.14 to 0.63 s. That time includes the storage client's own retries of the failed listing, which were not measured separately. Such a result is never remembered, so the next query lists again.
- **New metric `arc_query_days_read_unlisted_total`** (`query_days_read_unlisted_total` in the JSON metrics): days a query read this way. It staying above zero means listings of your bucket are failing.
- **If the day holds nothing at all,** the query answers the retryable 503, with a warning in the log saying the day's listing failed, not an empty result.
- **Azure** was measured the same way on Azurite, the Azure Storage emulator (not against Azure itself), a day of 94 files with its listing failing: the same row counts as without the failure, each query waiting out the 5 second listing limit first.
- **GCS** was measured on Google Cloud Storage itself, a day of 64 files with its listing made to fail in a test build: the same row counts as without the failure.

Also in this change, on every object store:

- **The day check asks storage for one level only.** It used to list every object under the day, hour directories included, to find the files at the top: thousands of keys on a day with many files not yet compacted, under a 5 second limit, which is the listing that failed most. The check for which hours exist already lists the day's directory, and that same listing now answers both, in the hot read, the cold tier read and for queries with only a start time. Measured as above, first query after start: the check's own listing, 4 requests of up to 1,000 keys each, is gone, with the same answer and the same query time (0.63 s).
- **Hours of a day that does not exist are not read.** When the month's listing, made for that query, does not show a day and the day's own listing fails, its hours were assumed to exist and the query failed with a 503. It now answers.

## Fix: in a cluster with local storage, a compacted file's last copy is no longer removed when moving it into place fails ([#1807](https://github.com/Basekick-Labs/arc/issues/1807))

**Who is affected:** clusters with local storage, replication on and staged compaction (#1207). Object storage and single nodes are unchanged.

A node that holds a staged copy of a committed compaction output moves it into place. When that move failed, the node's cleanup removed the staged copy anyway and asked a peer for the file. The staged copy can be the last copy. That happens on a holder node when the compacting node has already removed its own copy and lost its disk, or on the compacting node itself when its compaction watcher did not start. In those cases nothing was left to pull, and the file the manifest registers was gone from that node. Arc now removes a staged copy only on positive proof that it is not needed:

- the output left the manifest or moved to the cold tier;
- a file of the output's size is already at its path (the check the compacting node itself makes before it removes its copy);
- the staged copy is missing or incomplete.

Any failed check keeps the copy, and the next sweep (every 30 s) tries the move again. While a copy is kept, a warning names the job, at most once every 10 minutes.

**Also:**

- **No pointless move.** A staged output is checked against the manifest before it is moved into place, not only after.
  - One that already left the manifest was moved in and then deleted, which recreated its partition directories and left them empty.
  - One that had moved to the cold tier was moved in as a hot copy of a cold file.
- **A replaced stale copy is put back.** A stale local copy is moved aside before it is removed. If a new file was written to its path in that moment, it is moved back. When a directory cleanup took the emptied partition directory in between, Arc now recreates the directory and puts the file back. Before, it kept the file aside and logged an error asking for a manual move.

## Fix: compaction recovery confirms an output before it deletes the inputs ([#1809](https://github.com/Basekick-Labs/arc/pull/1809))

When a compaction job is interrupted after uploading its output, the next cycle finishes it: it checks the output and deletes the inputs the output replaces. The check compared the stored output's size with the size the job recorded, but only when a listing of the output succeeded. If that listing failed or came back empty, the comparison was skipped and the inputs were deleted anyway.

- **The size is always read, and must match.** If it cannot be read, the inputs and the recovery record are kept and the next cycle tries again.
- **A short output is discarded only when the inputs are all still there.** The job is then redone from them, as before. If an input is missing or cannot be checked, nothing is deleted: the output may be the only copy of those rows. Arc logs `Compaction output size differs from its manifest and its inputs are not all in storage` at error level on every compaction cycle, and keeps those files out of compaction, until the record under `_compaction_state/` is removed by hand.
- **The recovery record is removed only after the short output is really gone.** Before, a failed delete left the output and its inputs together with nothing to stop the next cycle compacting them into duplicate rows.

## Fix: a compaction job that could not delete its inputs no longer has them compacted again ([#1809](https://github.com/Basekick-Labs/arc/pull/1809))

A job that uploads its output but cannot delete its inputs leaves a record so those inputs stay out of compaction until the deletion is finished. Jobs run in a subprocess, and the main process kept its own list of such records for 30 seconds, so it could miss one for that long. The daily tier later in the same cycle, or a cycle starting right after, could then compact the new output together with the inputs it already contained, which duplicates their rows. The list is now refreshed whenever a job returns.

## Fix: after a storage listing fails, queries no longer keep leaving out a day's daily files ([#1809](https://github.com/Basekick-Labs/arc/pull/1809))

On S3, Azure and GCS, Arc lists a day's directory to see whether it holds daily files. If that listing fails, the day's daily files are left out of the query. That result was then remembered for up to a minute, so queries over the day kept answering without them after storage was answering again. A result that depends on a failed listing is no longer remembered: the next query lists again.

- **The query whose listing fails is not changed**: it still answers without that day's daily files.
- **Cost:** while listings keep failing, every query over the day asks storage again.
## Fix: a write is no longer lost to a directory cleanup climbing the same partition ([#1804](https://github.com/Basekick-Labs/arc/issues/1804))

**Who is affected:** Arc with local storage, mostly on macOS, while tiering, a database delete, retention or compaction removes empty directories. Linux is rarely affected; object storage is unchanged.

When a directory cleanup removes a partition's emptied directories while a file is being written into that partition, Arc recreates the directories and retries (#1425). It retried once. Tiering's cleanup and a database delete's directory sweep climb from the partition up to the database directory, removing one level after another, and Arc spends a directory fsync on each level it recreates, so the cleanup could remove a level again during that one retry. On macOS, where a directory fsync is a full flush to disk, two of three racing writes failed this way when measured; the same test did not fail on Linux.

What a failed ingest flush cost depends on the WAL:

- **WAL off (the default):** the flush's rows were dropped, with the log line `Flush failed; rows without a WAL copy were lost`, although the write had been acknowledged.
- **WAL on:** the rows were kept for WAL recovery, and until it ran, `DELETE` requests that need a flush were refused.

Arc now retries recreating the directories once for each level of the path, so one cleanup climbing the path cannot fail the write. Two cleanups at once on the same path still can, with the costs above.

**Details:**

- **Moving a staged compaction output into place** (cluster with local storage) now also recreates a destination directory removed under it and retries, instead of falling back to the next attempt or a copy from a peer.
- **The spelling check for a new database or measurement name** (#1509, on volumes that fold letter case, such as default macOS) is no longer skipped when a database delete removes the database directory at the same moment. The check recreates the directories and runs. Before, the write was admitted without the check.
- **Unchanged:** the conditional write that publishes Iceberg metadata. It writes into a directory that holds the metadata file it has just read, so a cleanup cannot remove that directory.

## Fix: on macOS, a write no longer fails when its directory is removed at the same moment ([#1767](https://github.com/Basekick-Labs/arc/issues/1767))

**Who is affected:** Arc on macOS with local storage. Object storage is unchanged; on Linux and Windows only the "removed between its creation and the check" case below changes.

When a database is deleted, or retention or compaction removes a directory that has just emptied, a write into that same directory at that moment, such as a field-schema anchor, a compaction manifest or an ingested file, finds it gone. Arc recreates the directory and retries once (#1425). Linux reports the missing directory as `ENOENT`. macOS (APFS) reports it either as `ENOENT` or, when the call reached the directory just before the removal finished, as `EINVAL` ("invalid argument"). Arc retried only on `ENOENT`, so on macOS the write failed with `failed to create temp file: … invalid argument`, or `failed to create staging file`, or `failed to create directory`, and the next anchor flush or compaction cycle had to repeat it. On macOS, Arc now treats that `EINVAL` the same as `ENOENT`.

**Details:**

- **Covered:** the paths that already retried: creating the temporary file of an ordinary write (anchors, manifests, ingested files), creating the staging file of a streamed write, creating a missing directory level, and moving a compacted file into place.
- **Not covered:** the conditional write that publishes Iceberg metadata, and moving an object within storage, which do not retry on any platform ([#1804](https://github.com/Basekick-Labs/arc/issues/1804)).
- **Compaction keeps its copy fallback.** When the retried move into place is refused again, the compacted file is copied in, as for any other refused move. If recreating the directory itself fails, the compaction job fails and the next cycle runs it again, as on Linux.
- **A directory removed between its creation and the check that it exists** is now recreated and retried, on every platform, instead of failing with `… exists and is not a directory`, which was not true of a directory that had simply gone.
- **Still fails:** a write whose directory is removed again during the retry. The next flush or cycle repeats it, as before.

## Fix: the JSON metrics no longer report a query count that is always 0 ([#1747](https://github.com/Basekick-Labs/arc/issues/1747))

**Who is affected:** anyone reading the keys below from Arc's JSON metrics endpoints: dashboards, scripts or agents. They always read `0`, because nothing ever counted them, and they are now gone from the response. **This is a response-shape change**: a client that requires these keys must stop reading them. Prometheus `/metrics` (text format) is unchanged: it stopped serving `arc_db_queries_total` in v26.09.2 (#802) and never served a DuckDB-level error counter.

| Endpoint | Removed keys | Read instead |
|---|---|---|
| `GET /api/v1/metrics/query-pool` | `pool.queries_total`, `pool.query_errors_total` | `pool.query_requests`, `pool.query_success`, `pool.query_errors` (same object) |
| `GET /api/v1/metrics`, and `GET /metrics` with `Accept: application/json` | `db_queries_total`, `db_query_errors_total` | `query_requests_total`, `query_success_total`, `query_errors_total` |
| `GET /api/v1/metrics/timeseries/api` | `db_queries_total`, `db_query_errors_total` | `query_requests_total` in `GET /api/v1/metrics/timeseries/application`; no series carries query errors, so read `query_errors_total` from the snapshot |

Why they read 0: nothing ever incremented them. They were meant to count queries at Arc's DuckDB wrapper, but most queries run through DuckDB's Arrow path and the arcx engine, which never pass through it, so #802 removed the Prometheus counter rather than wire a partial count. The JSON endpoints kept serving them. On the query-pool endpoint this put `queries_total: 0` next to a real `query_requests`, and `query_errors_total` (always `0`) next to the real `query_errors`. A panel built on the wrong one stayed at zero, and `queries_total` read as "this node served no queries".

The counters that replace them are counted at every query entry point (`POST /api/v1/query`, `GET /api/v1/query/:measurement`, `/api/v1/query/arrow`, msgpack, Flight SQL). The `connections_*` pool fields are unchanged.

## Fix: a sum or a stored decimal beyond the int64 range is answered as a 400 with the reason ([#1784](https://github.com/Basekick-Labs/arc/issues/1784))

**Who is affected:** only queries that already failed. A query whose result contains a scale-0 decimal value outside the int64 range (−9,223,372,036,854,775,808 to 9,223,372,036,854,775,807), such as `sum()` of a large integer column or a `DECIMAL(38,0)` column stored with `ingest.decimal_columns` (#276), now gets an error status with a clear reason instead of an empty or cut result. **Every query that returned a complete result before returns exactly the same now.**

**Nothing to enable or redefine:** this is the default behaviour; there is no setting and no header. Stored data, Parquet files and `ingest.decimal_columns` are not touched, so no table has to be redefined, re-ingested or migrated. The change is only in how the query response is built.

Why it failed: Arc sends a scale-0 decimal result column as `int64` (#599), and DuckDB returns `sum()` of any integer column as a 128-bit integer (HUGEINT, exported as `DECIMAL(38,0)`). A value beyond int64 failed that conversion after the response had started:

| Endpoint | Before | Now (value in the first chunk of the result, at most 2,048 rows) |
|---|---|---|
| `POST /api/v1/query` (JSON) | 200, `success: true`, `truncated` | **400** with the reason |
| `POST /api/v1/query/arrow` | 200, a cut stream, the reason in the trailer | **400** with the reason |
| `POST /api/v1/query/msgpack` | 500 | **400** with the reason (any row within the governance cap) |
| Flight SQL (new in this release) | — | `INVALID_ARGUMENT` with the reason |

**The error text changes**, for clients that match on it:

- Before: `decimal cast failed at row N: invalid: integer value out of bounds` (JSON, msgpack) or `failed to cast decimal columns at row N: …` (Arrow trailer); N was a batch boundary, not the row of the value.
- Now: `decimal value beyond the int64 range: column "s" (DECIMAL(38,0)/HUGEINT) at row N; cast it in SQL: ::DOUBLE (approximate) or ::VARCHAR (exact digits)`, with N the value's own row.

**Details:**

- **A value after the first chunk** (at most 2,048 rows) still cuts the streaming responses as before: `truncated` on JSON, the trailer on Arrow, now with the new text; Flight SQL ends with `INVALID_ARGUMENT` and the reason. In those reasons the column name shows as `"..."`, because Arc masks quoted text in every error it sends as a truncation reason. msgpack reads the whole result first and answers 400 for any row within the governance cap.
- **Governance row cap:** only rows within the cap are checked. On JSON, a value beyond the cap used to cut a capped result short; it now returns the capped rows, as Arrow and msgpack already did.
- **Clients**, for a value in the first chunk: arcli prints the reason and exits 1 (as before) without writing a partial table; the Python SDK (`arc-tsdb-client` 0.2.1) raises `ArcQueryError` from `query()`, `query_arrow()` and `query_msgpack()`, where `query()` used to return an empty result marked `truncated`. `query()` and `query_arrow()` carry the reason as text; `query_msgpack()` puts the server's MessagePack error body in its message, as it did for the 500.
- **Unchanged:** values that fit (still `int64`), decimals with a scale (still `float64`), the `types` field on msgpack, and the schema a Flight SQL prepare reports.
- **Getting the value:** cast it in SQL, `sum(x)::DOUBLE` (approximate) or `sum(x)::VARCHAR` (exact digits). This also works on older versions.

## Fix: on a shared bucket, an Iceberg table recreated with a column of another type, or restored after a delete, stopped exporting ([#1801](https://github.com/Basekick-Labs/arc/issues/1801), [#1799](https://github.com/Basekick-Labs/arc/issues/1799), [#1796](https://github.com/Basekick-Labs/arc/issues/1796))

Two failures on a shared bucket (where the node that handles a `DELETE` is not the node that exports), both permanent: every reconcile pass of the table failed until the process was restarted or the table was dropped by hand.

- **A database deleted on another node and written again with a column of another type** ([#1801](https://github.com/Basekick-Labs/arc/issues/1801)): `column "value" type mismatch: Iceberg table has double, new Parquet files have string`. The exporting node rebuilt the table from what it had cached of the dropped one, because that cache was thrown away only for drops handled in its own process. On a three-node S3 cluster the build without the fix logged 22 failed passes and left the table empty; with the fix, no failed pass and the table held the 12 new rows with the string column. The cache is now let go of whenever the table's lineage changes under the exporter: its row recovered after a drop, the table removed as predating a drop (before the pass reads anything of it), or another node's recreated table adopted after a lease move. Only that table's cache goes; the database's other tables keep theirs.
- **A database restored after a delete, when the restored `version-hint.text` names a version newer than the files the backup listed** ([#1799](https://github.com/Basekick-Labs/arc/issues/1799)): `read published Iceberg metadata .../v4.metadata.json: file not found`. The drop's per-table marker outlives the restore (backups do not copy it), and while one is set the fallback that follows the newest version present ([#1613](https://github.com/Basekick-Labs/arc/issues/1613)) was off. A marker no longer counts as a drop in progress once the database's drop record ([#1792](https://github.com/Basekick-Labs/arc/issues/1792)) says that drop is over. Where there is no record (a drop by a node on an older version), it counts as one, as before.
- **A restore still rebuilds the tables of a database deleted after the backup**: the drop record stays, and the restored tables, which predate it, are removed once and exported again from their data, without their snapshot history (on the cluster run: 20 of 20 restored tables rebuilt, all matching storage, no failed pass). Clearing the record at restore was tried and rejected in review: a broken table left by a drop then stays broken for good, and tables the restore does not bring back lose the marker that lets other nodes forget their rows.
- **A drop that fails no longer holds the export back for 15 minutes**: its record is marked finished, unless it failed on its own deadline or a cancelled request, where deletes may still be in flight and the record stays "running" until it is stale. The tables it left are removed by the exporting node once the database is written again; until then they stay visible to outside readers, and the `DELETE` reply says `"iceberg_cleanup": "pending: ..."` as before.
- **Checked, nothing to change:** what a former exporter leaves when it publishes during a drop and never looks at its end (it crashed or lost the export) is collected by the orphan sweep on the new exporter once older than its grace; a node with no row follows the new table.
- Not covered: tables an exporter on an older version recreated after an upgraded writer's drop are still rebuilt once (no way to tell them from tables that predate the drop). Upgrade every node before deleting databases. No new configuration.

## New: Flight SQL can list databases as schemas, so Tableau can browse Arc ([#1645](https://github.com/Basekick-Labs/arc/issues/1645))

Flight SQL metadata names a database as a catalog with one empty-named schema. Tableau's JDBC connection cannot select an empty schema, so it listed Arc's databases but never reached a table. A client can now send the call header `x-arc-metadata: schemas`: there are then no catalogs, each database is a schema, and measurements are its tables.

- **Without the header nothing changes:** every client that does not send it (DBeaver, ADBC) gets the same metadata as before. `catalogs` asks for the default explicitly; any other value is `INVALID_ARGUMENT`.
- **Tableau:** add `x-arc-metadata=schemas` to the JDBC URL (the driver passes unknown URL properties as call headers), for example `jdbc:arrow-flight-sql://host:port?useEncryption=false&x-arc-metadata=schemas`. With ADBC, set `adbc.flight.sql.rpc.call_header.x-arc-metadata`.
- **In the schemas shape** the catalog filter is ignored (Tableau sends one it remembers), the database named exactly as the schema pattern is listed first, and the key listings (primary, imported, exported, cross-reference) answer empty instead of unimplemented: Tableau ends the connection on an unimplemented call when a table is added. Permissions, governance limits and `GetSqlInfo` are the same in both shapes.
- **Tested** with Tableau Desktop 2026.3 and `flight-sql-jdbc-driver` 19.0.0. `docs/operations/flight-sql.md` has a Tableau section, including the cast that makes `time` a date in Tableau.
- Flight SQL is off by default (`flight_sql.enabled = false`); nothing here runs unless it is on and the header is sent.

## Fix: with two processes exporting one bucket, Iceberg tables could hold a data file twice, and then stop exporting ([#1795](https://github.com/Basekick-Labs/arc/issues/1795))

When two Arc processes export the same bucket to Iceberg (two standalone nodes on one bucket and prefix: nothing can detect that without a cluster coordinator), a data file could be added to the table twice:

1. node A's ingest writes a file and reports it to A;
2. node B's timed pass lists the bucket, finds the file, adds it, and publishes that table version;
3. node A's next ingest export takes over B's published version, then appends the file again. The append path does not check the table for the file (that check reads every manifest, the cost #1751 removed); what kept a file from being added twice was what A itself had added and listed, which says nothing about B's pass.

Readers then counted that file's rows twice. And iceberg-go cannot remove a path the table holds twice: once that file left storage (a compaction's input, retention, a delete), every pass of the table failed with "cannot delete files that do not belong to the table", so the table stopped exporting and kept naming a file that was gone. Traced from a two-node run's own manifests and logs, and reproduced by tests; the evidence is on the issue.

- **An ingest append now goes on only from the table version this process last left it at** (its own last append, or the end of its last settled pass). A version another process committed, or a catalog row recovered from a dead metadata file, makes that export a reconcile pass, which diffs the table and adds nothing it already holds. On a node with no election (no cluster coordinator) this is logged at Warn once per table and process, naming the two causes: another process exports this bucket, or this node recovered the table's catalog row because its metadata file was gone. With two exporters every ingest export after the other's commit is a full pass, which is the append path effectively off; run one exporter per bucket.
- **A pass repairs a table that already holds a path more than once**: one commit removes every data entry and adds each live path back once (merged into one manifest), then history is floored to that snapshot, since every older one holds the paths twice. Logged at Warn with the count and examples. It is best-effort where nothing was committed: if it cannot be staged or the catalog refuses it cleanly, the pass goes on as before and the next pass tries again; if the commit's outcome is unknown, the pass ends, as with any other commit. The repair rewrites the table's whole manifest set in one commit, the same cost as the manifest collapse (#1106): a table too large to rewrite within one pass's deadline is not repaired. It is refused, with an Error, on a table that has delete files (Arc writes none): the re-added entries would no longer be covered by them.
- After a repair, the snapshot summary's running totals (`total-data-files`, `total-records`) stay high by the extra entries; iceberg-go carries them forward. Nothing in Arc reads them.
- Not covered: two processes committing at the same moment are still kept apart only by the publish fence (#1609), as before.
- No configuration changes.
## Fix: on a shared bucket, a database deleted on one node while another exported it could leave a broken Iceberg table ([#1792](https://github.com/Basekick-Labs/arc/issues/1792))

On a shared bucket the node that handles a `DELETE` of a database (the primary writer) drops its Iceberg tables for every node, and the node that exports them (the compactor-lease holder) is usually another one. The lock that keeps a drop and an export apart ([#1789](https://github.com/Basekick-Labs/arc/issues/1789), below) is one process's, so a reconcile pass on the exporting node could commit and publish after the drop had listed what it would delete. What the pass wrote stayed, naming manifests that were gone, under a catalog row that still loaded: every later pass of that table failed (`could not open manifest file`), also once the database was written again.

- **Reproduced** with two exporters on one storage root, the pass held at its commit while the other node's drop listed the table: the table was left broken every time. The order needs the commit to land in the short gap between the drop's listing and its deletes. Runs of a three-node cluster on S3 with the database deleted every 12 to 20 s under ingest (three valid runs, 20 and 60 tables) did not hit it on the build without the fix, where the existing recovery (the exporter forgetting its row for a dropped table) coped.
- **The fix: a drop record per database, in the bucket.** The dropping node writes it before it lists anything and marks it done at its end. The exporting node reads it in each pass that is about to read or write one of the database's tables: while a drop is running it gives way, as it does for a drop in its own process. Each table carries the id of the record it was created under, so a table from before the latest drop is recognised, also after a restart or by a node that takes over the export, and removed (its metadata and its catalog row) at the end of the pass the drop overlapped or in the table's next pass. A table's first export overlapping the `DELETE` is covered the same way.
- **On the running binary** (three nodes on one S3 bucket with 30 ms added per request): with 60 tables and the database deleted 8 times under ingest, the build with the fix logged no failed pass and ended with all 60 tables matching storage, the exporter giving way to each drop. With a finished drop record put in the bucket under steady ingest and no `DELETE` (what a restore or a mixed-version drop looks like), the exporter removed all 20 tables that predated it and exported them again: all 20 matched storage, and no metadata of the old tables was left. A standalone node wrote no record and behaved as before.
- **Cost:** on a shared bucket, one small read per pass that has work to do (none for a table whose files are unchanged) and one at its end. Nothing on a standalone node or a cluster with node-local warehouses.
- **A drop that is cut short** (its deadline, or its request cancelled) leaves its record saying "running": the export of that database (written again under the same name) waits for it, up to 15 minutes, or until the `DELETE` is repeated. The drop itself stops deleting after 7.5 minutes and reports `"iceberg_cleanup": "pending: ... repeat the DELETE to drop them"`. A drop that fails otherwise marks its record finished ([#1796](https://github.com/Basekick-Labs/arc/issues/1796), above).
- **A table older than its database's latest drop is removed and exported again from its data, without its snapshot history.** That is what a broken table needs. It also happens, once per table, to a healthy one: a database restored from a backup made before it was dropped (the record is in the bucket, not in the backup), and tables created by an exporter still running an older version after an upgraded writer dropped the database. Upgrade every node before deleting databases.
- **Clocks:** nodes of a shared bucket must agree to within 7.5 minutes, or an exporter could take a running drop for a dead one.
- Ingest-triggered appends are not involved: they do not run on a cluster's shared bucket. No new configuration.

## Fix: an Iceberg snapshot expiry that failed is retried: every pass tries to expire down to `retain_snapshots` ([#1791](https://github.com/Basekick-Labs/arc/issues/1791))

A reconcile pass expired snapshots over `iceberg.retain_snapshots` only when it had committed something, when an ingest append had come in since the last expiry, or on a table's first pass after a restart. So a table could stay over the bound once it went quiet:

- **An expiry that failed was not tried again.** The pass that failed was left unsettled and the next one ran, but it committed nothing and so did not expire; it settled, and the extra snapshots stayed until the table's next commit.
- **The same for an expiry that never ran**: a node that lost the writer role between its last commit and the end of its pass, or another node that appended or committed and stopped before expiring, when the node that took over had already reconciled the table in its process.

Every pass that reaches its end as the writer now tries to expire down to `retain_snapshots`, whether or not it committed. A table within the bound costs nothing more: the count is read from the metadata the pass already loaded, and no commit is made. The exporter no longer keeps a record of which tables are owed an expiry.

- **While an expiry keeps failing** the table's pass stays unsettled and runs in full at every `iceberg.reconcile_interval`, logging the failure at Error each time, and its ingest exports run as passes. That was already so for a table that keeps receiving data; for a quiet one it used to stop after one pass, with the snapshots left in place.
- A node without the writer role still makes no expiry commit.
- No configuration changes. Found by reading during #1787; the failing sequence is reproduced by the new tests, not seen in a deployment.
## Fix: a database deleted while its Iceberg tables were being exported could leave them broken ([#1789](https://github.com/Basekick-Labs/arc/issues/1789))

A `DELETE` of a database drops its Iceberg tables from the request itself, and nothing kept that from landing in the middle of a reconcile pass or an append of one of those tables. The pass had loaded its table before the drop and committed or published after it, and the catalog ended with a table whose metadata named files the drop had deleted. Such a table still loads, so nothing treated it as gone: every later pass of it failed (`could not open manifest file`), and a database written again under the same name was exported into it. It did not heal.

- **Reproduced:** a test that deletes a database (its files, then its tables) while a timed pass runs and then writes it again ended with broken or missing tables in 19 of 30 runs with passes one at a time and 25 of 30 with four at once. With the fix, 0 of 30 at both.
- **On the running binary** (S3 API with 30 ms added per request, 20 tables under ingest, the database deleted every 20 s while ingest kept writing it): the build without the fix logged 26, 47 and 79 failed passes in three runs (tables naming deleted manifests and metadata); the build with it logged none in six, at the default and at 8 passes at once. In the four of those where the writes outlasted the deletes, every table matched storage at the end (as it did in the one such run without the fix, where later deletes had cleared the broken tables). The `DELETE` took the same 8 to 15 s in both.
- **The fix:** every export of a table holds its database's lock from before it lists the measurement to its end, and the drop holds it alone. A drop waits for the exports of that database that are running, and none starts until it is done. An export does not wait for a drop: it gives way at once and its table is tried again shortly, so a database being dropped does not hold up the export of the others.
- **A pass does not trust what was remembered of a dropped table.** A database written again with other columns, or with a column of another type, is exported with the new schema; before, a pass that was already queued built the new table from the old one's.
- **A `DELETE` can wait** for an export of the database's tables in progress: usually milliseconds, at most 150 s or the request's own budget (the delete's hold on tiering lasts that much longer). If the export does not end in time the tables are left as they are, the data is deleted all the same, and the response says so: `"iceberg_cleanup": "pending: ... repeat the DELETE to drop them"`. Repeating the `DELETE` drops them; it answers 404, since the database no longer exists, and carries the same note if the drop is refused again.
- **A shared bucket** is covered separately: there the node that handles the `DELETE` is usually not the node that exports, and one node's lock does not hold another's pass ([#1792](https://github.com/Basekick-Labs/arc/issues/1792), above).
- No new configuration.

## Fix: a quiet Iceberg table no longer repeats its pass and re-publishes while old versions wait out `min_version_age` ([#1787](https://github.com/Basekick-Labs/arc/issues/1787))

A pass that kept published versions on disk only because of `iceberg.min_version_age` marked the table so that every later pass ran in full, whatever its listing, until those versions were old enough to prune. Each of those passes committed nothing, yet read the table's manifests and wrote `v<N>.metadata.json` (refused as existing, then read back to compare) and `version-hint.text` again: 15 requests on S3, at every `iceberg.reconcile_interval`.

- **The table now gets one pass, when the versions are due.** The prune reports when the last version it kept for its age can go; until then an unchanged table is skipped like any other (the listing only, 3 requests in the trace), and the pass at that time prunes them all. The versions that come of age earlier wait for it, less than one `min_version_age` more.
- **A pass that finds its own publish in place writes nothing.** When this process already wrote that exact metadata file to `version-hint.text` and the hint read at the start of the pass still names it, the pass no longer writes the copy and the hint again. The first pass of a process, a pass after any commit, a pass that finds the hint missing, behind or unreadable, and every pass of a table still on an adopted row publish as before.
- **Given up:** a pass that commits nothing no longer puts back a `v<N>.metadata.json` deleted from outside Arc while the hint still names it. The next commit or restart writes it.
- **A prune that could not finish is retried.** When the metadata directory cannot be listed, or a copy cannot be deleted, the table gets a pass one `min_version_age` later that tries again, and the failure is logged at Warn (it was at Debug, or not logged). Before, a table that then went quiet kept those copies until its files next changed. While the failure lasts that is one full pass of the table per `min_version_age`.
- **Copies a pass of several commits keeps for itself are removed by the next pass.** They used to stay over `retain_snapshots + 1` on a table that went quiet.
- **Measured** with the built binary on SeaweedFS, `reconcile_interval = 15`, `min_version_age = 120`, 30 writes and then nothing: before, `version-hint.text` was written again at each of the 7 passes that followed the last commit; after, not once: the six passes that followed the last publish skipped the table, and the seventh, 105 s after it, pruned the table from 10 copies to `retain_snapshots + 1`. One run before, two after.
- No configuration changes.

## New: reconcile passes of several Iceberg tables run at once ([#1786](https://github.com/Basekick-Labs/arc/issues/1786))

After #1778 the appends of different tables overlap, but a reconcile pass still ran for one table at a time, and every table gets one: at its first export, every `iceberg.reconcile_interval`, and after 120 appends. On object storage a pass is about half a second of waiting on storage requests (14 to 16 of them for a table that only took appends), so a timed pass over 200 tables took minutes, during which those tables' appends were served one batch per table visited. The passes of different tables now overlap too.

- **New key `iceberg.reconcile_concurrency`** (1 to 16; default `0`, which picks by the storage: `4` on an object store, `1` on local storage). `1` reconciles one table at a time, as before. The value in effect is logged at startup. Azure and GCS count as object stores and so default to 4; they were not run.
- **Where it applies:** the timed pass's walk of the tables, and the passes ingest requests need (a table's first export when its data arrives after the node started, a fallback, every request with `ingest_append_enabled = false`) when `iceberg.ingest_export_concurrency` is above 1. Passes asked for by compaction, DELETE, retention or tiering still run one at a time.
- **Standalone nodes only.** A node of a cluster, and a node on a shared bucket with no election of the exporter, runs one at a time whatever the key says, and logs it when the key was set higher.
- As with the appends, the scheduler starts a batch, waits for all of it, and only then records what each pass did: a table is still exported by one thing at a time.

Measured on AWS S3, standalone on EC2 in the bucket's region, 200 tables written at once with `ingest_export_interval = 5` and 16 appends at once, over 10 minutes after a 6-minute warm-up (two timed passes by the timer; the instance's logs were not kept); one run each:

| Passes at once | Average export lag | Appends per second | Longest wait at the end of the run |
|---|---|---|---|
| 1 | 5.1 s | 16.2 | 28 s |
| 4 | 3.2 s | 19.5 | 2 s |
| 8 | 2.1 s | 21.0 | 1 s |

A second set on the final build, the same load over minutes 5 to 10 of the run (a timed pass is due at minute 5), and how long the first export of the 200 tables took after the node started (their data arrives after the start, so those are passes ingest requests ask for):

| `reconcile_concurrency` | Average export lag | First export of all 200 tables |
|---|---|---|
| 1 | 7.9 s | 2 min 38 s |
| not set (4 on object storage) | 3.5 s | 48 s |
| 8 | 2.0 s | 27 s |

The same load through a local S3 server with 30 ms added to every request, where the logs give the lengths:

| Passes at once | The timed pass's walk of 200 tables | First export of all 200 tables after start |
|---|---|---|
| 1 | about 5 minutes | 5 min 10 s |
| 4 | 77 s | 83 s |
| 8 | 45 s | 46 s |

(The lag in those local runs is not comparable across rows: the 4-minute window lay inside the walk at 1, overlapped it by seconds at 4, and came after it at 8.)

- **At the default `ingest_export_interval` of 60 s nothing changes**: 30.3 s at one pass at a time and 29.9 s at 4 on AWS, which is what a 60 s interval gives. The gain is at short intervals, in how long a timed pass takes, and at start-up.
- In every run no append fell back, there were no Iceberg warnings or errors, row counts matched storage on the tables checked and no file was in a table twice.
- **Why the default is 1 on local storage:** a pass there is tens of milliseconds, and running several at once bought little and cost write latency. 200 tables at 5 s on a laptop that also ran the load, two runs each:

  | Appends / passes at once | Average export lag | Write round trip p95 | p99 |
  |---|---|---|---|
  | 1 / 1 | 1.00, 0.86 s | 4.8, 4.2 ms | 68, 58 ms |
  | 8 / 1 | 0.78, 0.77 s | 7.5, 6.8 ms | 73, 72 ms |
  | 8 / 4 | 0.66, 0.46 s | 13.9, 9.9 ms | 96, 81 ms |

Notes:

- **A timed pass now serves compaction requests, drops and ingest once per batch of tables**, not once per table.
- **The passes of a batch share** 16 storage reads in flight (footers and existence checks) and the 64 MiB of footers one pass alone may hold. Their listings, metadata reads and commits are not bounded together.
- Each pass in a batch has the two minutes a pass always had; the batch ends when its slowest pass does.
- **Not run on the binary:** `reconcile_concurrency = 16` on AWS (it ran on a local S3 server), a cold tier, Glue sync, a warehouse outside the storage root, Azure and GCS.
- **A database deleted during a reconcile pass** could leave a table missing or broken, with one pass at a time as with several; fixed in [#1789](https://github.com/Basekick-Labs/arc/issues/1789), above.
- While a table's published versions were held by `iceberg.min_version_age`, each of its passes repeated its requests and re-published without having committed anything; fixed in [#1787](https://github.com/Basekick-Labs/arc/issues/1787), above.

## New: ingest appends of several Iceberg tables run at once ([#1778](https://github.com/Basekick-Labs/arc/issues/1778))

On object storage an ingest-triggered export is a few hundred milliseconds of waiting on storage requests, and Arc exported one table at a time: under three tables a second in a bucket's region. With many tables being written the tables queued behind each other, and the lag was the length of the queue, whatever `ingest_export_interval` said. The appends of different tables now overlap.

- **New key `iceberg.ingest_export_concurrency`** (default `8`, 1 to 32): how many tables' ingest appends run at once. `1` exports one table at a time, as before.
- **Reconcile passes still run one table at a time**: a table's first export, the pass every `iceberg.reconcile_interval`, the pass after 120 appends, and any fallback. Only the append (#1751) is run for several tables at once.
- **On a cluster** (since [#1756](https://github.com/Basekick-Labs/arc/issues/1756), above), the exporting node uses it for its appends on local storage. Reconcile passes still run one table at a time there. Not run with several tables on a cluster.
- A table is still exported by one thing at a time, and a file is still added to its table once: the scheduler starts a batch of appends, waits for all of them, and only then records what they did.

Measured on AWS S3, standalone on EC2 in the bucket's region, over 4 to 5 minutes after a warm-up of 4 to 6 (one run each, except where two figures are given):

| Tables written at once | `ingest_export_interval` | Appends at once | Average export lag | Appends per second |
|---|---|---|---|---|
| 50 | 5 s | 1 | 10.6 s | 2.4 |
| 50 | 5 s | 8 | 0.98 s | 5.4 |
| 200 | 5 s | 1 | 48.0 s | 2.0 |
| 200 | 5 s | 8 | 12.4 s, 11.8 s | 9.8, 10.3 |
| 200 | 5 s | 16 | 7.1 s | 13.7 |
| 200 | 15 s | 8 | 10.4 s | 10.4 |
| 200 | 60 s (default) | 1 | 48.5 s | 1.9 |
| 200 | 60 s (default) | 8 | 30.2 s | 2.9 |

- **With 50 tables a 5 s interval is met**: about a second from a file being written to its table, where it was over ten.

- **At the default interval the interval is met with 200 tables**: 30 s is what a 60 s interval gives on average. Before, the lag was 48 s and files were still waiting 75 s at the end of the run.
- **A short interval is not met at 200 tables**, though the lag is a quarter of what it was (a sixth with 16 at once). What is left is the reconcile passes, which every table still gets one at a time, at its first export and every `reconcile_interval`. Each measured window above began a minute after a timed pass started, so most of it was spent with that pass walking the 200 tables (in the local run with 30 ms added per request the walk took about 4.7 minutes): the figures are for the slow part of the cycle, and between two timed passes the lag is lower.
- No append fell back in any run, there were no Iceberg warnings or errors, row counts matched storage on the tables checked and no file was in a table twice. On S3, write round trips were the same at every setting.
- **Cost:** Arc's CPU time over the 11-minute run went from 1:43 at one at a time to 3:07 at 8 and 3:52 at 16 with the 5 s interval: it did five to seven times as many exports. At the default interval it was 1:44 against 1:52.
- The same 200-table load at 5 s through a local S3 server with 30 ms added to every request: 97 s, 15.8 s and 9.0 s at 1, 8 and 16.
- **On local storage**, where an export is tens of milliseconds, there is little to gain and a small cost: three runs each at 1 and at 8, 200 tables at 5 s, on a laptop that also ran the load: average lag 1.76, 0.58 and 1.65 s at one at a time and 0.79, 0.62 and 0.77 s at 8; an append took about 27 ms alone and about 39 ms beside others; the p95 of write round trips was 4.4, 3.9 and 4.9 ms at 1 and 8.4, 5.8 and 9.8 ms at 8, with the median and the p99 unchanged; CPU time was the same. So on a local-disk node the default buys a steadier lag and costs a few milliseconds at the p95 of writes under this load; `ingest_export_concurrency = 1` gives the previous behaviour. One run at 32 behaved as 8 did.

Notes:

- **Each append that runs beside others has 60 s**; one that runs out falls back to a reconcile pass (the fallback counter counts it, and the log names "context deadline exceeded"). With `ingest_export_concurrency = 1` an append and its fallback share the pass's two minutes, as before.
- **Between two tables of a timed reconcile pass**, one batch of appends is served (before: one request), and still at most one request that needs a reconcile pass.
- **Memory and connections grow with the key**: each append in flight holds the footers of its new files (a few MiB for a large append, 64 MiB at most) and reads at least four of them at once.
- The catalog's SQLite database takes one statement at a time (it is usually the one the auth store uses); the appends' statements queue there for a few milliseconds each.
- **Not run on the binary:** `ingest_export_concurrency = 32` on object storage (it was run on local storage), Glue sync, a warehouse outside the storage root, Azure and GCS.

## Fix: Arc no longer keeps one DuckDB connection busy from startup ([#1782](https://github.com/Basekick-Labs/arc/issues/1782))

The DuckDB check Arc runs at startup (`SELECT 'Hello from Arc in Go!' …`) left its result set open until the process exited, so one connection of the pool (`database.max_connections`) was never available to queries. With `database.max_connections = 1` no query was answered at all: HTTP and Flight SQL requests waited for a connection that never came free. With any other positive value, queries had one connection fewer than configured, which is 3 instead of 4 on small machines (the default is at least 4). Releases since at least v26.01.1 are affected.

The check now gives its connection back as soon as it has read its row.

- With `database.max_connections = 1`, queries over HTTP (JSON, Arrow, MessagePack, profiled) and Flight SQL are answered.
- With 2 connections and 2 concurrent queries (5 rounds of a ~1 s query), the pool waited 5 times for 5.5 s before this fix and not at all after it; the 5 rounds took 11.4 s before and 6.4 s after.
- **Visible change in the pool gauges.** At idle, `arc_db_connections_in_use` (`db_connections_in_use` in the JSON metrics, `connections_in_use` in the `pool` block of `GET /api/v1/metrics/query-pool`) is now 0 instead of 1, and the idle count is 1 higher. `arc_db_connections_open` (`connections_open`) can now drop to 0 after 10 minutes without queries, when idle connections are closed; before, the held connection kept it at 1 or more. The startup line `Database connection pool stats` shows `in_use: 0`. Dashboards that subtracted the idle 1, or alerts on no open connections, need adjusting.
- The log messages of the startup check are unchanged.

No configuration change.

## Improvement: an Iceberg export on object storage makes fewer requests ([#1778](https://github.com/Basekick-Labs/arc/issues/1778))

On S3 an export is a chain of requests made one after another, and the runs for #1751 showed that this, not CPU, is what an export costs there: about half a second per table in the bucket's region. A trace of those requests showed several that were not needed.

- **A new data file is read once.** Its footer was fetched for the schema, and then iceberg-go fetched the file again to register it: a GET of the whole object, whose body it dropped, and two more reads of the footer. The export now hands iceberg-go the bytes it already has. No request asks for a whole data file any more.
- **No HEAD per file reported by ingest**, on object storage. The size the flush wrote is used, and the footer read checks it against the stored object: a file that is gone is left out of the append as before, and one whose stored length differs makes that export a full pass. Local files are still stat'ed.
- **`version-hint.text` is read once per export**, not twice, and **the count of small manifests is carried from one append to the next** in place of a read of the manifest list at every append. The carried count is an estimate: a table can hold a few more small manifests than before between two full passes.

Requests for one export, counted through a proxy in front of an S3 server (`TestMeasure_S3RequestsPerExport`; a table of 49 files, the exporter set up as a production node). The counts are exact and tests pin them. The times are one run each with the proxy holding every request for 30 ms, to show what the counts are worth with a network in between; they are not AWS figures:

| Export | Requests before | After | Time before | After |
|---|---|---|---|---|
| Append, 1 new file | 17 | 11 | 657 ms | 442 ms |
| Append, 19 new files | 112 | 34 | 1,800 ms | 717 ms |
| Full pass, 1 new file | 24 | 20 | 929 ms | 798 ms |
| Full pass, 19 new files | 98 | 40 | 1,259 ms | 884 ms |

Of the 11 requests left in an append, five are writes that must follow one another, and three are reads of the table's `metadata.json` (the load, iceberg-go's check before the commit, and the copy published as `v<N>.metadata.json`).

On AWS S3, the build before this change and the build with it, on the same instance and bucket (standalone, append on, 3 to 5 minutes of writes):

| Where Arc ran | Tables, interval | One export before | After | Average lag before | After |
|---|---|---|---|---|---|
| EC2 in the bucket's region | 10, 5 s (two runs each) | 494, 491 ms | 357, 354 ms | 0.94, 0.97 s | 0.60, 0.56 s |
| EC2 in the bucket's region | 50, 15 s | 598 ms | 417 ms | 18.8 s | 13.3 s |
| EC2 in the bucket's region | 200, 60 s (default) | 901 ms | 572 ms | 122 s | 82 s |
| 77 ms from the bucket | 10, 5 s (two runs each) | 2.44, 2.48 s | 1.50, 1.52 s | 15.6, 15.5 s | 10.8, 10.4 s |

An export takes 28 to 39% less time. No append fell back, row counts matched storage on the tables checked and no file was in a table twice. With 200 tables the build before still had files waiting 216 s when the run ended; this one had none. The 50- and 200-table rows are one run each.

- Azure and GCS take the same code path and were not run.
- **Tables are still exported one at a time.** This makes each export cheaper; it does not let several tables export at once, which is what many tables at a short `ingest_export_interval` need on object storage: with 200 tables at the default interval the lag is still over a minute. That remains open in #1778.
- No change on local storage, and no new configuration. On the running binary (S3 API, 10 tables at a 5 s interval): 120 appends, none fell back, row counts matched storage and no file was in a table twice; an export averaged 41 ms against 57 ms on the previous build, in one run of each with no network in between and no repeat to show the spread.
- The footers held for one export are bounded at 64 MiB (about a thousand new files); past that, the remaining files of that export are read by iceberg-go as before.

## Fix: late data no longer rewrites an old day's daily file ([#1775](https://github.com/Basekick-Labs/arc/issues/1775))

When `compaction.daily_min_files` or more hour-level files (default 12) landed in a day that already had a daily file, daily compaction rewrote the whole day to fold them in: the job read the existing daily file and wrote it out again with the new rows. The cost followed the size of the day, not the size of the late data, and it repeated each time enough late files built up. One deployment rewrote 339 GB across 373 days to fold in 18 GB.

A day older than `compaction.daily_skip_file_age_check_days` (default 7) that already has a daily file is no longer rewritten:

- Its hour-level files compact into **one more daily file** beside the existing one, the same way #1774 below handles a day with fewer files. Any number of them, from one up.
- **It waits until the day is quiet:** until its newest file was written more than `compaction.daily_min_age_hours` ago (default 24). A backfill still writing into an old day is compacted once it stops, not once per cycle. A day with no daily file yet is compacted as before, without waiting.
- `GET /api/v1/compaction/candidates` shows these days as `"settled": true`, and their `file_count` no longer includes the daily file.
- A day younger than `daily_skip_file_age_check_days` is unchanged: its daily file is still rewritten together with the late files.

What to expect:

- **Duplicate rows are merged only for days younger than `daily_skip_file_age_check_days`.** Compaction removes exact duplicate rows within one job only (ingest with tags, and continuous-query output). Rewriting the daily file used to merge a late row with an identical row already in it; for an older day both are now kept. This affects re-running a continuous query over windows older than that, and a merge-mode restore of a backup taken before an old day was compacted. Within that age, both still collapse as before. Raise `daily_skip_file_age_check_days` to keep the old behaviour for longer.
- **More daily files per day.** Each fold adds a daily file and nothing merges daily files with each other. Because a day must be quiet for `daily_min_age_hours` first, folds come one per quiet period, not one per late file.
- **These days queue behind the regular ones.** An old day with late data used to compact in the next cycle as a regular day. It now waits for quiet, runs after the tier's other days within the 500-day cap, and, like any settled day, is not run again for 7 days if its job fails and leaves the files (a new file in the day ends that wait).
- **"Quiet" is judged by file names.** Files that keep the name they had elsewhere (edge-sync deliveries, a merge-mode restore) look old as soon as they land, so a day that keeps receiving them can fold once per daily cycle.
- **A day remembered as too wide to compact is tried again.** If an old day was skipped because rewriting its daily file ran out of memory, its smaller job without the daily file is retried. If that also fails, it is remembered again, after one or two failed cycles depending on the error.

## New: ingest files are appended to their Iceberg table without re-reading it, and the export interval can go down to 5 s ([#1751](https://github.com/Basekick-Labs/arc/issues/1751))

Since #1750 an ingest flush asks for an export of its table. Each of those exports was a full pass of the measurement: list its files, read every manifest of the table, commit, expire snapshots, prune and sweep. That work grows with the table, and the published versions were kept by count, which tied how often ingest could export to `retain_snapshots`. Both change here.

- **Append path.** The flush now names the file it registered, and the export adds just the files named: a footer read per new file, one commit, one publish. No listing, and no read of the table's manifests. Anything that is not plainly an append (a file with a new column, a table that does not exist yet, a table whose last pass did not settle, a file older than 30 minutes) runs the full pass as before.
- **New key `iceberg.min_version_age`** (seconds, default `300`): a published table version stays on disk for this long after the next one is published, on top of the newest `retain_snapshots + 1`. A reader that resolved a version, or Glue until its next sync, can still open it. It must be at least 120 s, and at least `iceberg.glue.sync_interval + 60` with Glue on. A value you set below that is refused at startup; the default is raised with a warning instead (to 660 s at `glue.sync_interval = 600`).
- **`iceberg.ingest_export_interval` is no longer tied to `retain_snapshots`.** It must be `0` (off) or at least `5`. The rule from #1750 (`ingest_export_interval × retain_snapshots ≥ 120`) is gone: keeping versions by age gives the same guarantee directly. The default stays `60`.
- **New key `iceberg.ingest_append_enabled`** (default `true`). `false` makes every ingest-triggered export a full pass, as in #1750.
- **On a cluster** (since [#1756](https://github.com/Basekick-Labs/arc/issues/1756), above), on local storage, the exporting node appends the files that reach it and its own flushes. Keeping versions by age applies on every node.

What one ingest export costs, as a full pass and as an append, through the scheduler on local disk (median of 7; `TestMeasure_AppendAgainstAPass`):

| Files in the table | Full pass, 1 new file | Append, 1 new file | Full pass, 30 new files | Append, 30 new files |
|---|---|---|---|---|
| 1,000 | 43 ms | 30 ms | 43 ms | 34 ms |
| 8,000 | 122 ms | 31 ms | 121 ms | 34 ms |
| 20,000 | 251 ms | 28 ms | 266 ms | 34 ms |

The full pass grows with the table; the append does not.

On an S3 API (SeaweedFS on the same machine, so no network latency), a 400-file table: 35 ms as a pass, 10–17 ms as an append.

On the running binary, 200 measurements written for 4 minutes, standalone, local storage:

| `ingest_export_interval` | Append | Average export lag | Iceberg metadata written |
|---|---|---|---|
| 60 s (default) | on | 27.5 s | 26 MB |
| 15 s | on | 7.3 s | 113 MB |
| 10 s | on | 4.7 s | 188 MB |
| 5 s | on | 1.0 s | 223 MB |
| 5 s | off | 1.0 s | 252 MB |

- **The interval is what shortens the lag** (on local disk; for object storage see the notes below). At 5 s a file is in its table about a second after it is written with 200 tables being written at once (one goroutine serves them all, so they queue), and 54 ms after with 10. At a given interval the append path does not change the lag.
- **The append path is what keeps an export cheap as a table grows** (the table above). In these runs the tables held about 48 files each and a full pass was already cheap, so on and off look the same. One pair of runs with ten tables that grew to about 1,300 files, and about 28 new files per export, averaged 57 ms per export as an append and 70 ms as a pass; it was a single run of each on a machine also serving the test's queries, so read it as "no worse", not as a measured gain.
- **A short interval costs metadata.** Every export writes a new table version, about 40 KB of metadata in these runs: about eight times as much at 5 s as at 60 s. (In this load each table got a new file about every 9 s, so 5 s and 10 s made nearly the same number of exports and wrote nearly the same amount; they differ in lag.)
- **What stays on disk is bounded.** In a 75-minute run (20 tables, 5 s interval, `min_version_age = 120`, a full pass every 30 s; 9,778 appends, none fell back) the published versions held at 18–23 per table and iceberg-go's own metadata files at 25 per table throughout. Manifest files grew by about 15 per table per minute for the first hour and then levelled off at about 920 per table, once the orphan sweep began removing the ones older than an hour. At the end the 20 tables held 125 MB of Iceberg metadata, about 6 MB each, and an export took the same time as at the start (35 ms on average).
- **A table that stops receiving data** drops back to `retain_snapshots + 1` published versions within a few minutes, and its passes are skipped again.
- In every run the Iceberg row counts matched storage on the tables checked, no file was in a table twice, and no append fell back to a full pass.

New counters: `arc_iceberg_ingest_appends_total`, `arc_iceberg_ingest_append_files_total`, and `arc_iceberg_ingest_append_fallbacks_total` (exports that had files to add and ran a full pass instead; the log names the reason once per table). The `/metrics` JSON form carries `iceberg_ingest_appends`, `iceberg_ingest_append_files` and `iceberg_ingest_append_fallbacks`.

Notes:

- **Snapshots are expired, versions pruned and orphans swept by the full pass**, which still runs every `iceberg.reconcile_interval`. Between two passes a table that is appended to carries the snapshots of those appends. A table also takes a full pass after 120 appends, so a long `reconcile_interval` does not let them pile up.
- **Manifests:** each append adds one. Once there are four small ones the append merges them, up to about 4,000 files per manifest; a larger manifest is never rewritten by an append.
- **A file ingest deletes after storing it** (the late-landed copy of a failed flush) now asks for an export of its table, so it leaves the table at once and not at the next timed pass.
- **Also run on the binary:** S3 as primary storage (SeaweedFS in a container: appends, row counts equal, an export about 49 ms as an append and 63 ms as a pass); `iceberg.warehouse` in a subdirectory of the storage root and outside it; a three-node cluster on shared storage, where the append path is off: the compactor exported on its timer, no node appended, and published versions held at 11 per table across a restart of all three nodes.
- **A three-node cluster with node-local storage** (EC2, writer, reader and compactor, each on its own disk): the compactor exported the replicated files on its timer, no node appended or took an ingest request, row counts matched and no file was in a table twice, and published versions held at 11 per table across a restart of all three nodes (1,238 files).
- **On object storage the export is slower, and one goroutine serves every table.** Against AWS S3, standalone, 3-minute writes, `ingest_export_interval = 5` unless said otherwise:

  | Where Arc ran | Tables written | Append | One export | Average export lag |
  |---|---|---|---|---|
  | EC2 in the bucket's region | 10 | on | 525 ms | 1.0 s |
  | EC2 in the bucket's region | 10 | off | 846 ms | 2.9 s |
  | EC2 in the bucket's region | 200 | on (no table reached it: every export was a full pass) | about 1 s | 61 s and still growing when the run ended |
  | EC2 in the bucket's region, interval 60 s (default), 5 minutes | 200 | on | 954 ms | 116 s |
  | The same, append off | 200 | off | 915 ms | 107 s |
  | 77 ms from the bucket | 10 | on | 2.5 s | 15 s |
  | 77 ms from the bucket | 10 | off | 3.1 s | 17.5 s |

  An export on S3 is a few dozen requests made one after another, so it takes about half a second in the bucket's region and seconds from far away, and the tables queue behind each other. The interval you set is reached while `tables written at once × one export` fits in it: about ten tables at 5 s, and somewhere between 60 and 100 at the default 60 s, in the bucket's region. Past that the tables wait for each other and the interval no longer sets the lag. The timed pass before #1750 exported tables the same way, one after another; it was not measured on S3. With one new file the append took about 40% less than the pass in the bucket's region and about 20% less from far away (one run each); with many new files per export it reads each one in turn and cost the same as the pass. No 200-table run reached a steady state, so read their lag as "minutes", not as a figure. By these runs each further file in an export costs about 24 ms in the bucket's region (525 ms with one file, 954 ms with about 19), so a node exports at most about 40 files a second there, and the 200-table load (about 22 files a second) used about half of that: the estimate for its steady state is a cycle of about 3.5 minutes. Above about 40 files a second the queue would not be bounded. These are estimates from two runs, not measurements. Serving tables in parallel is [#1778](https://github.com/Basekick-Labs/arc/issues/1778).
- In those S3 runs no append fell back, row counts matched storage and no file was in a table twice; on a table left quiet the published versions went from 26 to 11 within two minutes (the age rule reads S3's own timestamps).

## New: Flight SQL prepared statements, so the JDBC driver and Tableau can run queries ([#1645](https://github.com/Basekick-Labs/arc/issues/1645))

The Flight SQL listener now takes prepared statements without parameters. The Arrow Flight SQL JDBC driver, which Tableau and DBeaver use, prepares every query, even a plain `Statement.executeQuery`, so until now it could connect and browse tables but every query failed with `UNIMPLEMENTED`. With JDBC 19.0.0, `executeQuery` and `PreparedStatement` now return results. The ADBC drivers, which prepare before each execute when the server allows it, now take the prepared path too.

- **A prepare binds the statement; the execute runs it.** DuckDB resolves the statement's tables and columns, which lists the files and reads their Parquet footers but scans no data, and the result schema comes from that. It is the schema the execute streams, with Arc's decimal handling. The execute then runs as an unprepared statement does. The bind is not free of work: DuckDB's optimizer is off during it, but the binder still evaluates constant arguments of functions (`SELECT length(repeat('x', 500000000))` costs a large part of what running it does), and DuckDB does not interrupt a bind, so the query timeout applies when it ends.
- **One query per execute.** The prepare is not counted as a query: it does not appear in `/api/v1/queries`, the query metrics or the slow-query log, and it does not spend governance rate limits or quotas. But it is refused with `RESOURCE_EXHAUSTED` when the token's next query would be. It runs the query's checks: SQL validation, RBAC on every table (no schema for a table the token cannot read), and the query or governance timeout.
- **Each prepare costs one bind**, one per `executeQuery` with JDBC, and one per execute with ADBC. The bind lists the measurement's files, so it grows with their number: on a measurement of 2,000 Parquet files on local disk it took about 43 ms, and an ADBC execute of an aggregate over them went from about 110 ms to about 155 ms; on a measurement of a few files it is about 1 ms. (One machine; on a loopback S3 server the same execute went from about 410 ms to about 590 ms.) At most a quarter of `database.max_connections` binds run at once (at least one; one when it is 0 or less, unlimited); more wait for a slot. New metrics: `arc_flight_sql_prepare_binds_total{result}` and `arc_flight_sql_prepare_bind_duration_seconds`. A bind that takes `query.slow_query_threshold_ms` or more, not counting the wait for a slot, is logged as a warning. With audit logging on, a prepare is an `api.post` event with the detail `{"operation": "flight_sql.prepare"}`, not a `data.query`.
- **No server state.** The handle carries the SQL and the `x-arc-database` header the prepare saw, so closing a statement frees nothing.
- **`UNIMPLEMENTED`, so ADBC runs the statement unprepared:** a `PIVOT` (over JDBC, write it as `sum(x) FILTER (WHERE k = 'v')`, which prepares), a result column Arc cannot describe without running the query (a struct with unnamed fields, such as `(1, 2)`), and any prepare on a node that does not run queries (a compactor; the JDBC driver cannot run queries there).
- **`INVALID_ARGUMENT`:** parameters (`?`), and a statement that does not bind because of the statement itself (a syntax error, an unknown column). A bind that could not read storage, or ran out of memory, is `UNAVAILABLE`, to be retried; a damaged Parquet file is `INTERNAL`. A query on a measurement with no data yet has a result without columns, which JDBC 19.0.0 reads as an update; that execute is refused with a message that says so.
- **Documentation:** `docs/operations/flight-sql.md` has a prepared-statements section.

No new configuration keys.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in [#1779](https://github.com/Basekick-Labs/arc/pull/1779).

## Fix: a day with few files gets a daily file, so it can move to the cold tier ([#1774](https://github.com/Basekick-Labs/arc/issues/1774))

Daily compaction needed `compaction.daily_min_files` hour-level files in a day (default 12), and tiering moves only daily files to cold. A measurement that writes a few points a day ends up with one or two hour-level files per day, so those days never compacted and stayed on the hot tier for good. Lowering `daily_min_files` did not help: a day with a single file failed with `batch size 1 below minimum 2`, and a day that already had a daily file had it rewritten together with its stragglers.

Daily compaction now takes a **settled** day with fewer than `daily_min_files` hour-level files:

- **Settled** means the day is older than `compaction.daily_skip_file_age_check_days` (default 7) and its newest file was written more than `compaction.daily_min_age_hours` ago (default 24). The second condition holds back a backfill that is still writing into an old day, which would otherwise get a new small daily file every cycle.
- Its hour-level files compact into **one more daily file**, even when there is only one. A daily file the day already has is left as it is, not rewritten. That includes a day whose daily file has already moved to cold: the new daily file follows it there.
- These days run **after** the tier's other days, at most **500 days per cycle** per tier (a day with many files can split into several jobs). A day the filters hold back (files pending registration, not yet delivered by edge sync, remembered as too wide) does not use one of the 500. The rest wait for the next cycle, and a log line says how many.
- **A day whose job runs and leaves its files is not run again** until a new file lands in the day, for up to 7 days: for example a lone file the job cannot read, or files without a `time` column. Such files never made it into a daily file before either; now they are tried once a week instead of every cycle. A new file in the day makes it a new attempt. The memory is per node and starts empty after a restart.
- `GET /api/v1/compaction/candidates` marks these days `"settled": true`; their `file_count` is the hour-level files only.

What to expect:

- **A one-time backlog on upgrade.** Every settled day with fewer than `daily_min_files` hour-level files compacts once, one job each. These are not necessarily small: a measurement written during business hours, or by a batch load, can have up to 11 full-size hourly files per day that were never compacted into a daily file. On a store with many such days the backlog takes several daily cycles, 500 days each; the regular days still run first.
- **They reach cold after the later of** the database's hot age (`tiered_storage.default_hot_max_age_days` or its own policy) and `daily_skip_file_age_check_days`.
- **To turn it off,** raise `daily_skip_file_age_check_days`: no day younger than that is settled. The only other effect is that days up to that age keep their file-age check, which is harmless. Setting it to `0` does not turn it off: `0` means the default, 7 days.
- **If you lowered `daily_min_files` to work around this,** set it back to 12. A settled day no longer needs it, and on a day younger than `daily_skip_file_age_check_days` a low value still rewrites the day's daily file whenever that many stragglers arrive (#1775 above covers older days).
- **Duplicates are not merged across daily files.** Compaction removes duplicate rows only within one job, so a straggler that repeats a row of the day's existing daily file keeps it. Before this fix that row was never compacted at all.
- **File names, not file times, say when a file was written.** Files that keep the name they had elsewhere (received from an edge-sync spoke, restored from a backup) can count as settled as soon as they land.

## New: ingested data reaches Iceberg tables within a minute, and the export's lag is measured ([#1750](https://github.com/Basekick-Labs/arc/issues/1750))

A file written by ingest used to reach its Iceberg table only at the next timed export pass: up to `iceberg.reconcile_interval` (default 300 s) plus the pass. Compaction, DELETE, retention and tiering already asked for an export when they changed files; ingest did not. Now an ingest flush asks for its measurement's table too.

- **New key `iceberg.ingest_export_interval`** (seconds, default `60`): the least time between two exports of one table that ingest causes. A flushed file is exported after at most this long, plus the export itself. `0` turns the trigger off and leaves the timed passes only.
- **Bound:** it must be `0` or at least `5`. (As first merged, #1750 also required `ingest_export_interval × retain_snapshots ≥ 120`; #1751, above, replaced that with `iceberg.min_version_age`.)
- **On a cluster since [#1756](https://github.com/Basekick-Labs/arc/issues/1756)** (above): a file reaching the exporting node asks for the export, whichever node wrote it, and the pending gauge is set there; on the other nodes it stays at 0 (traced from code, not yet observed on a cluster).

Measured on a standalone node, local storage, default buffer age:

| | Files exported | Within 60 s | Average lag | Table versions committed | Iceberg metadata on disk |
|---|---|---|---|---|---|
| One measurement, 130 s, timed pass at an hour | 24 of 26 | all 24 (longest 58.6 s) | 28.7 s | 3 | — |
| 200 measurements for 4 min, trigger on, `reconcile_interval = 120` | 5,302 of 5,550 | 5,294 | 27.7 s | 1,015 | 24 MB, 4,836 files |
| The same with the trigger off | 2,829 of 5,528 | 1,384 | 61.1 s | 200 | 7.4 MB, 1,645 files |

In the 200-measurement runs the timed passes still completed, write round trips were 24 ms at p99 with the trigger on and 21 ms with it off, and query round trips 13 ms and 10 ms. The files not exported were still waiting for their table's next export when the run ended.

Three metrics, the first Iceberg ones:

- `arc_iceberg_export_lag_seconds` (histogram): per ingest file published in a table, the time from the file being written to the table version that holds it being published (`version-hint.text` written). The Glue update follows about 2 s later and is not included.
- `arc_iceberg_export_pending_seconds` (gauge): how long the oldest ingest file reported to the export has waited for an export that published it; `0` when none waits. This is the one to alert on: it keeps growing when the export is stuck or cannot publish, which a histogram of finished exports cannot show. It is fed by the ingest trigger, so it stays at 0 on a cluster node and with the trigger off.
- `arc_iceberg_export_backlog_files_total` (counter): ingest files published that were written before the process started, such as existing data when the export is first enabled. Their age is not a lag of the running export, so they are kept out of the histogram.

The `/metrics` JSON form carries `iceberg_export_lag_count`, `iceberg_export_lag_sum_seconds`, `iceberg_export_pending_seconds` and `iceberg_export_backlog_files`. The "Reconciled Iceberg table" log line gains `oldest_ingest_file_lag`.

Notes:

- **What an export costs is unchanged:** it still lists the measurement, reads the table's manifests and commits. The trigger only changes when it runs, which is more often: about five times the table versions and three times the metadata in the run above. With Glue on, each export is also one Glue update for its table. With more measurements than the exports can keep up with they queue, one table at a time, and the real interval stretches; the pending gauge shows it. Requests from compaction, DELETE, retention and tiering are served first.
- **Spacing counts from the attempt:** a table whose export fails, or commits and cannot publish, is tried again once per interval, with or without further flushes. An export of any kind that changes a table also restarts the interval, so an ingest-triggered export does not publish right behind a compaction's.
- **More table versions:** a table written to continuously now gets a snapshot per interval instead of per timed pass. With the append path of #1751 (above) most of those exports log at Debug; a full pass still logs one "Reconciled Iceberg table" line at Info. How long versions are kept is `retain_snapshots` and, since #1751, `iceberg.min_version_age`.
- No labels: the metrics cover every table.

## Fix: a late or future-stamped row is read by queries as soon as its file is written ([#1757](https://github.com/Basekick-Labs/arc/issues/1757))

A row stamped outside the current hour could be flushed to storage and still be missing from queries for up to a minute. That covers late data (a row for an hour that has already ended) and rows stamped ahead of the clock.

When a query finds an hour with no files, Arc remembers that so the next query does not list the hour again. It never remembers it for the current hour, which fills as data arrives. For any other hour it did, and nothing told the query layer when an ingest flush then wrote the first file there. In a test, one row stamped 3 hours back was written into an hour a query had just found empty, then flushed:

| | Repeating the same query | A query not sent before |
|---|---|---|
| Before | missing for up to 60 s | missing for up to 30 s |
| Now | read at the next query (2–23 ms) | read at the next query (2–23 ms) |

The "before" times count from the query that found the hour empty; the "now" times from the flush returning. Measured on a standalone node with rows stamped 3 hours back and 2 hours ahead. Before: 12 of 12 trials on local storage and 2 of 2 on S3 (SeaweedFS, late rows only). Now: 9 of 9 on local storage and 9 of 9 on S3, with a non-default `storage.s3_prefix` and with `query.enable_s3_cache` on.

- **How:** each file an ingest flush writes outside the current hour marks its measurement, and anything the query caches hold for that measurement from before is not used again. Nothing else is dropped: a backfill of one measurement leaves the cached plans of the others in place, apart from the few that share its marker (1 in 4,096).
- **What triggers it:** every writer that goes through the ingest buffer: the write endpoints, imports, MQTT, continuous-query output and WAL replay.
- **Cost:** one atomic increment per flushed file. Files in the current hour skip it, so a measurement being written to keeps its cached plans. A measurement that is written outside the current hour all the time (a source that always reports late) no longer has its plans cached: each of its queries lists storage, as queries on the current hour already do.

Notes:

- **Clusters:** this covers the node that wrote the file. Another node still waits out the caches (30 s / 60 s as before): on node-local storage after it has pulled the file, on shared storage after the file is in the bucket.
- **Not covered, unchanged:** files that arrive without an ingest flush on the node (a backup restore, files received by an edge-sync hub), and a measurement whose rows were all in the cold tier that gets its first local file.
- **A write that returned an error** is reported like one that succeeded, in case it landed anyway. If it lands only after that report (a request that times out and completes later), the old wait applies to that file.
- **Rows stamped ahead of the clock inside the current hour** are still left out of queries whose range starts in that hour until the hour closes, when they are stamped more than `query.file_time_pruning_margin_seconds` (default 300) ahead. That is file-time pruning's documented limit, not this fix.
## New: a metric for how long ingested rows wait before their file is published, and a harness that measures freshness ([#1758](https://github.com/Basekick-Labs/arc/issues/1758))

Arc had no number for how long a row waits between being written and being readable. `arc_buffer_oldest_unflushed_seconds` reports the age of what is still buffered, not how long flushed rows had waited.

- `arc_ingest_publish_delay_seconds` (histogram): one observation for each Parquet file a successful ingest flush publishes. It is the time from the oldest row of the flushed buffer being buffered to the file being in the node's storage. No row of the file waited longer than that.
- `arc_ingest_publish_delay_max_seconds` (gauge): the largest observation since the process started.
- The `/metrics` JSON form carries `ingest_publish_delay_count`, `ingest_publish_delay_sum_seconds` and `ingest_publish_delay_max_seconds`.

With `ingest.max_buffer_age_ms` at its default of 5000, expect observations of about 5 s for measurements written to slowly, and less for ones that reach `ingest.max_buffer_size` first. With many measurements it can be more: in one local run writing to 500 measurements, the mean was 5.5 s and the largest 9.6 s.

Notes:

- It measures publication on the node that flushed. It does not include the time for a query to pick the file up, or for another cluster node to receive it.
- Rows written by imports, continuous queries and MQTT go through the same buffers and are counted.
- After a failed flush, WAL replay refills the buffer and the wait starts again from the replay, so the time the failure cost is not in the histogram. Files a failed flush left in storage are not observed either. `arc_buffer_flush_failures_total` reports the failure.
- A flush that splits into several files (rows in more than one hour) observes each file, all measured from the same buffer start. An hour file that waits for the rest of its flush is observed when the flush registers it, not when it is written.
- No labels: the histogram covers every database and measurement.

`scripts/freshness` is a harness for the reader's side of the same question. It writes marker rows, polls a query, and reports the time from the write's acknowledgement to the first query that returned the row, next to the server's histogram for the same period. A second mode writes one late or future-stamped row into an hour that a query has just found empty and times how long queries keep answering without it ([#1757](https://github.com/Basekick-Labs/arc/issues/1757)). On a cluster, `steady -probe <reader URL>` writes to the writer given with `-http` and queries the reader ([#1769](https://github.com/Basekick-Labs/arc/issues/1769)). See `scripts/freshness/README.md`. No product behaviour changes.

## Fix: on a node-local-storage cluster, deleting a database drops every node's Iceberg tables ([#1731](https://github.com/Basekick-Labs/arc/issues/1731))

On a cluster whose nodes each keep their own disk, every node has its own Iceberg warehouse and catalog. A database `DELETE` runs on the primary writer and dropped only the writer's tables. Every other node that exported the database kept its tables: the node running the export, and any node that ran it before the compactor role moved. Those tables named deleted files. They stayed listed to directory readers, the REST catalog and Glue, and were never removed. In a 3-node test (writer, compactor, reader), the compactor kept 12 metadata files and its catalog row after the `DELETE`. Now it keeps none.

- **How:** after a `DELETE` completes (hot files, cold objects and its own tables), the writer sends a replicated notice. Each node drops its own tables of the database once its local copies of the files are unlinked. That includes nodes that don't currently export. A repeated `DELETE`, which answers 404, sends the notice again.
- **Nodes that missed the notice:** a node can be offline during the delete, catch up from a snapshot, or restart before its drop runs. Every 10 minutes, each node also drops the tables of any database in its own catalog that has no files in the cluster and none on its disk. A database emptied by retention loses its empty table the same way; new data recreates it.
- **What stays:** the emptied directory tree on non-writer nodes.
- **A recreated database is kept:** a node skips the drop while the cluster again has files under the database. That covers a database recreated right after its delete, and an old notice replayed when a node restarts. In the test, the recreated database was exported again, and a compactor restart left its table alone.
- **Shared buckets are unchanged:** the writer's drop already covers every node there.
- **Mixed-version clusters:** the notice is sent only once every node runs a build that applies it. Until then, the writer logs which nodes are behind.

No new configuration keys.

## Fix: an Iceberg export failing on a column type change names the column ([#1744](https://github.com/Basekick-Labs/arc/issues/1744))

When a measurement's column has different types in different files, or a new file disagrees with the table, Iceberg cannot hold both and the export fails that measurement on every pass. The log line said `column "..." has conflicting types`: Arc masks quoted text in every logged error, so the column name was masked too. Found on real data whose source schema changed over the years (#1690).

- **Now the failure log carries fields:** `column`, `type_first` and `type_then`, `conflict_with` (`files` when two files disagree, `table` when new files disagree with the table), and `file_first` / `file_then`, the storage keys of a file holding each type, where known.
- **The error text is still masked.** One wording changes: a new file that disagrees with the table used to be reported as "conflicting types across files". It now reads "type mismatch: Iceberg table has … new Parquet files have …", the wording Arc already used for the same conflict found elsewhere. The fields are identifiers and storage keys, which Arc already logs as plain fields. A column name is cut to 256 bytes, and invalid UTF-8 is quoted, as everywhere else Arc logs one.
- **Behaviour is unchanged:** the measurement still fails until its files agree.

No new configuration keys.

## New: Arc's Iceberg tables in AWS Glue ([#1690](https://github.com/Basekick-Labs/arc/issues/1690))

With `iceberg.glue.enabled = true`, Arc keeps one AWS Glue table for each Iceberg table its export publishes. Each Glue table points at the version Arc just published. Athena, EMR, Redshift Spectrum and Snowflake (`CATALOG_SOURCE = GLUE`) read Arc's tables from Glue, and so do engines that federate Glue. Before, Glue knew nothing about Arc's tables.

- **Turn it on:** set `iceberg.glue.enabled = true`. It needs `iceberg.enabled = true` and data on AWS S3; Arc refuses to start with another S3-compatible endpoint.
- **Names:** the Glue database is the Iceberg namespace (`arc_<database>`) and the Glue table is the measurement, lowercased, with any character other than `a-z`, `0-9` and `_` turned into `_`. Two Arc names that map to one Glue name: the first registered keeps it, and Arc logs an error for the other.
- **Freshness:** each publish is pushed to Glue at once. In a test on AWS, Glue named the new version within 2 seconds of Arc publishing it. Athena read it right away, and Snowflake about a minute later. Failures are retried every `iceberg.glue.sync_interval` seconds (default 60). Every hour, and whenever a node takes over the export, Arc checks every table against Glue.
- **A pointer, not a copy:** Arc keeps writing its own metadata. Glue stores where the current version is. Updates do not archive a Glue table version, so Glue's version quota is not consumed.
- **Arc only changes what it created:** Glue tables carry `arc.managed` and the warehouse they belong to. Arc never modifies or deletes a table it did not create, one created for another warehouse, a copy of one of its tables under another name, or one another writer has committed to. Each refusal is logged as an error once per process.
- **Removal:** when an Arc table or database is dropped, its Glue table is deleted. The table must be found missing on two consecutive complete checks at least 10 minutes apart, and the Glue table is read again just before the delete. That deletes Glue metadata only, never data. Glue databases are never deleted.
- **Credentials and region:** Glue uses the S3 storage credentials, in `iceberg.glue.region` or else `storage.s3_region`. Set `iceberg.glue.catalog_id` for a catalog in another account. The credentials need `glue:GetDatabase`, `CreateDatabase`, `GetDatabases`, `GetTable`, `GetTables`, `CreateTable`, `UpdateTable` and `DeleteTable`. Lake Formation grants are yours to set.
- **Clusters:** only the node running the export updates Glue. Give Glue permissions to every node that can take over the export.
- **Databricks** reads the tables through Unity Catalog's Glue federation (verified on a workspace in us-east-2). It needs `iceberg.layout = "colocated"`: Databricks reads only files under a table's location, and with the default `warehouse` layout the data files live outside it. The foreign catalog also needs a writable `storage_root`; give it a scratch bucket, not Arc's. New versions show up with no refresh.
- **Read-only consumers:** unlike Arc's REST catalog, Glue accepts writes. A `DROP TABLE … PURGE`, a `VACUUM` or an orphan-file cleanup run through Glue deletes the data files the table's metadata names, and those are Arc's own files in either layout. Give Athena, Spark and every other Glue consumer read-only S3 access to Arc's bucket, and leave Glue's table optimizers off. Arc logs a warning at startup.
- **One prefix per deployment:** two Arc deployments that share a Glue catalog and region need different `iceberg.namespace_prefix` values; otherwise their tables collide. Changing `iceberg.warehouse` leaves the Glue tables Arc created under the old warehouse in place. Arc refuses to touch them and logs how to clear them.

New configuration keys: `iceberg.glue.enabled` (default `false`), `iceberg.glue.region` (default: `storage.s3_region`), `iceberg.glue.catalog_id` (default: the credentials' account), `iceberg.glue.sync_interval` (default `60`).

## New: a read-only Iceberg REST catalog for Snowflake, Trino, Spark, PyIceberg and DuckDB ([#1690](https://github.com/Basekick-Labs/arc/issues/1690))

Arc can now serve the tables its Iceberg export publishes through the Iceberg REST catalog protocol. Before, an engine found an Arc table only by its path (`metadata/vN.metadata.json` or `version-hint.text`). Engines that discover tables through a catalog could not list them, and Snowflake read one snapshot until someone ran `ALTER ICEBERG TABLE … REFRESH` by hand. Now an engine points its catalog at `http(s)://<arc>/iceberg` and lists namespaces and tables, and loads the current version of each.

- **Turn it on:** set `iceberg.rest_catalog_enabled = true` (default `false`). It needs `iceberg.enabled = true`. Arc refuses to start if the storage backend cannot read the warehouse, because no table version is ever published there. The case that reaches this check is a local `iceberg.warehouse` outside the storage root.
- **Names:** the same as the export's. A database is the namespace `<prefix>_<database>` (`arc_mydb` by default), and a measurement is a table.
- **Snowflake:** create a catalog integration with `CATALOG_SOURCE = ICEBERG_REST`, `CATALOG_URI` set to the Arc URL ending in `/iceberg`, `CATALOG_API_TYPE = PUBLIC`, and `REST_AUTHENTICATION = (TYPE = BEARER BEARER_TOKEN = '<Arc token>')`. Snowflake reads the data through an external volume over the bucket. With `AUTO_REFRESH = TRUE`, a table follows new versions as the export publishes them. In a test against Arc on EC2 behind TLS, Snowflake showed a new version about 80 seconds after Arc published it. A catalog-linked database (`CREATE DATABASE … LINKED_CATALOG = (CATALOG = '<integration>')`) discovers Arc's tables on its own, including tables created later.
- **Other engines:** PyIceberg, Spark and Trino use `type=rest`, `uri=<arc>/iceberg` and the Arc token as `token`. DuckDB uses `ATTACH '...' (TYPE iceberg, ENDPOINT '<arc>/iceberg', SECRET ...)` with an iceberg secret holding the token.
- **What is served:** the version `version-hint.text` names, which is the same file directory readers use. A table without a hint is not listed. Every node that sees the warehouse gives the same answer. Each load reads storage fresh. Listings that found tables are cached for 30 seconds.
- **Read-only:** Arc is the only writer of its tables. Create, commit, rename and drop are refused with `406 UnsupportedOperationException`, and `/v1/config` does not advertise them.
- **Auth:** Arc tokens work as bearer tokens. Give each engine a token with read permission only, not an admin token: the engine stores it, and some engines record it in their query history. A token sees only the namespaces and tables it can read. A table it cannot read answers 404, the same as a table that does not exist. Arc does not hand out storage credentials: each engine reads storage with its own. Arc's permissions therefore control what the catalog lists, not who can read the bucket.
- **Failures are not absences:** when storage or the permission store cannot be read, the catalog answers `503`. It never answers with a shorter list, so a remote catalog does not drop tables during an outage. A table whose current version cannot be served also answers `503`: its hint names a missing version, or its current manifest list is gone. A damaged table fails only its own load, not the listings. The `503` body is generic; the cause is in Arc's log.
- **Dropping a database:** Arc deletes a database's data files before its table metadata. The catalog serves the table until its `version-hint.text` is deleted, and reads of it fail in the meantime, as they do for directory readers. If the drop stops part way, that lasts until the `DELETE` is repeated.
- **Clusters:** with a shared bucket, every node can answer. With node-local storage, only the node running the export holds the metadata, and the other nodes answer `503` naming why. This also applies with `cluster.enabled` but no running coordinator, where the export runs on the compactor-role node.
- **Limits:** edge-sync spoke namespaces are not listed, but stay readable by path. There is no credential vending and no OAuth token endpoint. Data files that a compaction deletes stay in the served version until the export's next pass, which the compaction requests right away (#1722).

New configuration key: `iceberg.rest_catalog_enabled` (default `false`).

## Fix: deleting a spoke on an edge-sync hub drops its Iceberg tables ([#1730](https://github.com/Basekick-Labs/arc/issues/1730))

On a hub, `DELETE /api/v1/databases/{spoke}` removed the spoke's data but left its Iceberg tables in place. This affected the default warehouse layout. Each table's catalog row and warehouse metadata (`arc_<spoke>.<db>.db/`) survived, still naming the deleted data files. Iceberg readers listing the namespace failed on them. In a local test with one spoke database of two tables, both catalog rows and 14 metadata files remained; now none do.

- **What is dropped now:** every database the spoke synced, wherever it is found.
  - **This node's catalog:** the spoke's databases it has rows for.
  - **The warehouse:** its directories are listed too, so a shared-bucket node that never exported the spoke also drops them.
  - **Spoke IDs with a dot:** tables written before #1129 are found too, whether or not the namespace migration has run.
- **What is never touched:** another spoke's tables, even when a dotted spoke ID makes the names look alike (`edge` and `edge.x`).
- **Limits:** the colocated layout already handled spokes. With a warehouse outside the storage root, Arc cannot reach the metadata files; it now drops the spoke databases' catalog rows and leaves the files. Only Iceberg metadata files are deleted, never another spoke's data, even if its name looks like a warehouse directory.

No new configuration keys.

## Fix: in a cluster, Iceberg tables catch up after a DELETE, retention, tiering or a reconciliation sweep ([#1728](https://github.com/Basekick-Labs/arc/issues/1728))

#1727 made these paths ask the Iceberg export to reconcile, on a single node. In a cluster they run on the primary writer, while the export runs on the node holding the compaction lease, so the writer's requests were dropped. The table named the removed files until the exporter's next timed pass, up to `iceberg.reconcile_interval` (300 s by default). Now the exporter node acts on what it sees itself.

- **Local-storage cluster:** each node keeps its own copies. When the exporter removes its copy of a file the cluster deleted, the export reconciles that measurement right away. Tested on two-node clusters (writer plus compactor/exporter, 300 s interval), reading the exporter's table every ~0.1 s:

| Topology | Change on the writer | Before | After |
|---|---|---|---|
| Local storage | Row DELETE | 152 of 169 reads failed | 1 of 172 |
| Local storage | Retention | 155 of 173 | 0 of 173 |
| Shared S3 bucket | Retention | 52 of 59 | 3 of 159 |
| Shared S3 bucket | Row DELETE | 53 of 60 | 1 of 157 |
| Shared S3 bucket | Whole-file DELETE | 53 of 60 | 3 of 159 |
| Local hot, S3 cold tier | Cold retention | 56 of 65 | 2 of 202 |
  - **DELETE rewrites:** the original is reported only once the rewrite that replaces it is on the exporter node. Reporting it earlier could publish a table that briefly lacks the rows the DELETE kept.
  - **Rewrite arriving:** when that rewrite lands, the export is asked two seconds later. Several rewrites then land in one snapshot rather than one each.
- **Shared bucket:** the exporter learns of each delete from the cluster manifest, which the writer updates before it removes the object.
  - **DELETE rewrites:** a rewrite's original is left out of reads from that moment, so the export is asked right away.
  - **Retention and whole-file DELETE:** each removed file is asked about at least 2, 10 and 30 seconds after its manifest delete. A file the writer takes longer to remove waits for the timed pass.
  - **Compactions:** these already ask on their own (#1722).
- **Database deletes never ask, on any topology:** a database delete drops its own tables, and a reconcile racing that drop could recreate one.
- **Tiering, any cluster:** when the exporter's tier record moves a file out of its hot tier, the export is asked and then reads the cold copy. Files replicated in from other nodes (every ingest batch) do not ask.
- **Reconciliation sweep:** when the sweep deletes orphan objects the export may have published, it asks the export to reconcile.
- **Cold tier, any cluster:** when a writer removes cold-tier objects, it tells every node through a small Raft notice naming the measurements. This covers retention, a cold DELETE, and the migrator dropping a cold copy. Removals within a second share one notice. The exporter then drops its cached cold listing for those databases and reconciles them. Before this, its table named the removed cold objects for up to `iceberg.cold_rescan_interval` (3600 s by default) plus a timed pass.
  - **Rolling upgrades:** the notice carries no cluster state. It is only sent once every node runs a build that understands it. Until then the export sees cold removals when its cached cold listing expires, as before.
  - **Export moving between nodes:** a node that becomes the exporter again first drops the cold listings it cached during its earlier turn.

No new configuration keys.

## Fix: Iceberg tables catch up right after a row DELETE, retention or tiering on a single node ([#1727](https://github.com/Basekick-Labs/arc/issues/1727))

The previous fix (#1722) made a finished compaction ask the Iceberg export to reconcile at once. Several other paths remove, move or rewrite data files and did not ask:

- a row DELETE, rewrites and whole-file deletes alike (hot and cold tier);
- the background drain of files earlier DELETEs replaced;
- retention (hot and cold tier);
- tiering moves from hot to cold;
- `delete_after_sync` on an edge-sync spoke;
- a database delete that stops part-way.

After any of them, the table kept naming the old files until the next reconcile pass, up to `iceberg.reconcile_interval` (300 s by default), and external reads of the affected data failed with a missing file. In a local test at the default interval, a row DELETE and a retention run each made about 90% of reads fail for the 20 seconds watched; with this change, one read failed in each.

- **What changed:** each of these paths now reports the storage paths it changed. The Iceberg export reconciles the measurements holding them right away, and drops its cached cold-tier listing first so a cold removal is seen.
- **How paths map to tables:** only onto tables the export already has. On an edge-sync hub, a DELETE or a retention run on a spoke's data reaches that spoke's existing tables and never creates a new one.
- **Single node.** A request is served only on the node that runs the export. In a cluster these paths mostly run on the primary writer, while the export runs on the node holding the compaction lease. There the request is dropped and the timed passes still converge, as before. Cluster coverage is tracked in [#1728](https://github.com/Basekick-Labs/arc/issues/1728).
- **Database deletes:** only a database delete that stops part-way asks; a completed one drops the database's Iceberg tables itself.
- **Not covered here:** tiering's crash-recovery orphan cleanup and cold re-syncs still wait for the timed pass.
- **When it lands:** a request waits for any reconcile already in progress to finish; one measurement's reconcile is capped at 2 minutes.

No new configuration keys.

## Fix: Iceberg tables stop naming a compaction's deleted files within seconds, not at the next reconcile pass ([#1722](https://github.com/Basekick-Labs/arc/issues/1722))

With Iceberg export on, compaction deletes its source files as soon as its output is written, but the Iceberg table only dropped them at its next reconcile pass. For up to `iceberg.reconcile_interval` (300 s by default) after every compaction, an external engine reading the affected hour or day (Spark, Trino, DuckDB `iceberg_scan`, PyIceberg, OneLake) failed with a missing file. In a local test at the default interval, reading the table every 20 ms, every read from the compaction to the end of the 25-second watch failed (203 of 222).

- **A finished compaction now asks the Iceberg export to reconcile that measurement right away.** The request is handled by the reconcile loop between measurements of a running pass, or at once when the loop is idle, so it never runs alongside another reconcile of the same table. The timed passes keep their schedule. In the same test, 2 of about 220 reads failed, all within one second of the compaction.
- **When it fires:**
  - **Standalone node, ordinary compaction:** once its output is written, including when deleting some sources failed. The table then holds the compacted file in place of all of them, until compaction's recovery deletes the rest.
  - **Cluster, staged compaction on local storage:** when the cluster commits it.
  - **Cluster, staged compaction on an object store:** when the cluster ends the job.
  - **A node that does not run the Iceberg export** ignores the request; its exporter's own passes converge as before.
- **Repeated reconciles of one table:** at `iceberg.retain_snapshots` 1 or 2, a compaction can reconcile a table at most once every 2 seconds. Back-to-back reconciles could otherwise delete the metadata version a reader has just looked up. With several compactions in one cycle, a later one's deleted files can therefore stay in the table for up to 2 seconds. At the default 10, or at 0 (keep all), there is no such limit.
- **A reconcile pass that runs between a compaction's upload and its deletes no longer drops that job's rows from the table.** Until now such a pass left out both the compacted file (still being committed) and its sources (already replaced), and committed the table without either. After a failed delete or a crash, that lasted until compaction's next recovery. Now the table holds the compacted file in place of the sources.

**Still possible:** a query that an external engine planned from the previous snapshot just before the switch, and that opens a source file after it is deleted, can still fail; retry it. Closing that window needs the sources kept until the table no longer names them, which is a larger change, tracked in [#1725](https://github.com/Basekick-Labs/arc/issues/1725).

No new configuration keys.

## New: Flight SQL clients can choose a database with the `x-arc-database` header ([#1720](https://github.com/Basekick-Labs/arc/issues/1720))

A Flight SQL statement can now carry the same `x-arc-database` header as the HTTP query API. Unqualified table names then resolve in that database, so `SELECT * FROM cpu` works without writing `"mydb"."cpu"`. Before, the Flight SQL listener ignored the header: an unqualified name resolved in the `default` database, and a query against a measurement that only existed elsewhere quietly returned no rows.

With the ADBC Flight SQL driver, set the connection option `adbc.flight.sql.rpc.call_header.x-arc-database` to the database name. Any client that can send gRPC call headers can do the same.

- **Same rules as HTTP:** the name is validated, and RBAC applies to the resolved table. A query that names any database (`"mydb"."cpu"` or `other.cpu`) is refused while the header is set, even when it names the header's own database. BI tools that write qualified names should leave the header unset.
- **Which call carries it:** the header can be sent when the query is planned (GetFlightInfo), when its result is fetched (DoGet), or on both. If both carry it, the two values must match. On those calls, sending it more than once or sending an invalid name is refused with `INVALID_ARGUMENT`.
- **Catalog calls ignore it:** listing catalogs, tables and table schemas (GetCatalogs, GetDbSchemas, GetTables) and SqlInfo are unaffected, so tools that browse every database keep working with the header set.
- **No other `x-arc-*` header is forwarded:** of the client's headers, only `x-arc-database`, the bearer token and the User-Agent reach the query, as before apart from the new header.
- **Documentation:** `docs/operations/flight-sql.md` covers the header and shows the ADBC option.
- **Cut-off results:** a Flight SQL result that Arc cut short now ends with `INTERNAL: Arc query stream was truncated`, instead of a decoding error about the stream's last message.

No new configuration keys.

## Fix: the arcx build's API tests run to completion ([#1016](https://github.com/Basekick-Labs/arc/issues/1016))

Only affects Arc built with the arcx engine (`-tags=duckdb_arrow,arcx_engine`); no change for servers, which always have a DuckDB engine.

`go test -tags=duckdb_arrow,arcx_engine ./internal/api/` stopped with a nil-pointer panic in the tests that build a query handler without DuckDB: the arcx hook asked that missing engine for its directory allowlist. The governance row-cap test crashed the run, and an RBAC end-to-end test passed on a hidden `500` because its recover middleware caught the same panic. Now:

- the arcx hooks leave a handler without DuckDB to its normal path, as the stock build does;
- `AllowedDirectories` on a nil engine returns no list instead of panicking. arcx denies every path when given no list, so that answer can never widen what arcx can read.

No new configuration keys.

## Fix: under arcx serve, a scan whose file is removed mid-query gets the retryable 503 ([#1357](https://github.com/Basekick-Labs/arc/issues/1357))

Applies only to Arc built with the arcx engine and run with `ARC_ROUTER=serve`. Stock builds and the default `shadow` mode are unchanged.

A scan without `ORDER BY` that arcx serves reads its Parquet files as the result streams. If compaction, retention or a `DELETE` removed one of those files while the scan was still running, the client got arcx's raw error, which names the absolute storage path. The status depended on the format:

- **msgpack:** `500`.
- **JSON and Arrow:** a `200`. The result was already marked as cut off (`"truncated": true` in JSON, the `Arc-Stream-Truncated` trailer in Arrow IPC).

Now:

- **msgpack** answers `503` with `Retry-After: 1` and the message DuckDB gives when a file it planned is already gone by the time it starts reading: "data read by this query was removed while it ran (retention, compaction or a delete); retry the query". The retry lists the files again.
- **JSON and Arrow** cannot change the status once the result has started, so they stay a `200` marked as cut off. The reason (`truncation_reason` in JSON, the trailer in Arrow) is now that message, not the path.
- The server logs warnings with the removed file's path and arcx's own error. These cases are now logged as warnings instead of errors.
- **Aggregate shapes and `ORDER BY` scans** do all their reading before any result is sent. As before, a removed file there sends the query back to DuckDB, which lists the files again and answers it; that fallback is now logged as a warning instead of `engine ERROR`.

**How Arc decides:** a stream error counts as a removed file only when its text names one of the query's planned files and that file no longer exists. The planned path is matched both as configured and with symlinks resolved. Any other arcx error keeps its earlier answer.

**Other changes to the arcx msgpack drain:**

- If the drain hits the query deadline, the answer is now `504 Query timed out`, as on the DuckDB path, instead of a `500`.
- Every failed drain now counts in the query-error metric, which the `500` case did not before.

No new configuration keys.

## Fix: the bootstrap admin token is trimmed of surrounding whitespace ([#1713](https://github.com/Basekick-Labs/arc/issues/1713))

`auth.bootstrap_token` (`ARC_AUTH_BOOTSTRAP_TOKEN`) was stored exactly as given. A value with surrounding whitespace, typically the trailing newline of a file made with `openssl rand ... > token` and loaded into a Kubernetes Secret with `--from-file`, became a token no `Authorization` or `x-api-key` header can carry: every request got `401`, while the first-boot banner showed a value that looked right.

- Arc now removes leading and trailing whitespace from the value when the configuration is loaded, from the environment or from `arc.toml`. Whitespace inside the value is kept. This covers both the first admin token and the recovery token added with `auth.force_bootstrap`.
- When trimming changed the value, Arc logs a warning at startup for `auth.bootstrap_token`. The warning never includes the value.
- With `auth.enabled = true`, a value that is only whitespace now stops Arc at startup with an error, instead of Arc quietly generating a random admin token. With auth disabled the key is not used and nothing changes; enabling auth later turns such a value into that startup error.
- The 32-character minimum now applies to the trimmed value, so a 31-character token followed by a newline is refused. Since #1915 that means Arc does not start; see that entry. The `arc` and `arc-enterprise` Helm charts check the trimmed length too, so they refuse such a `auth.bootstrapToken.value` at render time.
- Tokens already stored are not changed. A bootstrap token an earlier release stored with the whitespace cannot be sent in a header, but the `p` query parameter still accepts it with the whitespace URL-encoded, for example `curl -G --data-urlencode "p@token-file" http://<host>:8000/api/v1/auth/verify` (the `@file` form keeps the trailing newline; `$(cat token-file)` drops it). Use it once to create a new admin token through the API, then revoke the old one. Restarting with `ARC_AUTH_FORCE_BOOTSTRAP=true` and the token value also adds it, as the `arc-recovery` admin token; if an `arc-recovery` token already exists from an earlier attempt it is not replaced (since #1915 the restart logs that the value was not added), so delete it through the API first.
- During a rolling upgrade of a cluster that has never created its first admin token, a node still on the earlier release can create it with the untrimmed value. Upgrade every node before the first boot, or check that the token works afterwards.

No new configuration keys.
## Security fix: on a cluster, revoking a token created before v26.06.1 now takes effect ([#1630](https://github.com/Basekick-Labs/arc/issues/1630))

A token created before v26.06.1 lives only in the database of the node that created it, and so does one created while a node ran without its cluster; the cluster does not manage either. On a cluster, `POST /api/v1/auth/tokens/:id/revoke`, `…/rotate`, `PATCH /api/v1/auth/tokens/:id` and `DELETE /api/v1/auth/tokens/:id` for such a token answered 200 and changed nothing: a revoked token kept working, and a rotate returned a value that never authenticated. If a cluster token had the same ID (old tokens are numbered per node, cluster tokens by the cluster log), the request changed **that** token, on every node.

- These requests now change the token on the node that received them, and only there. They must name that node with `?node=<node_id>`:
  - without it, the answer is `409` with `"code": "node_local_token"`, the node's ID in `node_id`, and nothing is changed;
  - with it, the change is made on that node. The response keeps its `message` and adds `"scope": "node"`, `node_id`, the token's `id`, `name` and `fingerprint`, and a `warning`;
  - a request that reaches another node than the one named answers `409` (`wrong_node`) before looking the token up, so a load balancer cannot apply it to a different token with the same ID; an empty `node=` answers `400` (`invalid_node`);
  - a cluster token with `?node` answers `409` (`cluster_token`); without it, cluster tokens behave as before, with the same response bodies.
- If the same old token exists on several nodes (for example a token created with the same value on every node), repeat the request on each of them. A rotated old token's new value authenticates on that node only.
- A cluster token whose entry this node still holds but the cluster state no longer does (`not_in_cluster_state`) can be revoked or deleted on the node with `?node`; a rotate or update of it answers `409` (`revoke_or_delete_only`).
- A token row that comes from another cluster history (`history_mismatch`: the cluster state holds a different token at its ID) can only be deleted on the node with `?node`; a revoke, rotate or update answers `409` (`delete_only`). At its next start the node then holds the cluster's token at that ID. A revoke is refused because the node rewrites such a row to the cluster's token at startup and never re-enables a token, so the cluster token would stay disabled on that node.
- While a node has not yet caught up with the cluster state, such a request answers `503` (`not_caught_up`) with `Retry-After`. A request with `?node` on a node that cannot yet tell its own node ID answers `503` (`node_id_unknown`) with `Retry-After`, and nothing is changed.
- A rename of such a token to a name a cluster token holds answers `409` (`name_held_by_cluster`). A request that finds the token changed underneath it answers `409` (`token_changed`); retry.
- `GET /api/v1/auth/tokens` and `GET /api/v1/auth/tokens/:id` on a cluster node show every token a write would treat as not the cluster's (the three cases above) with `"node_local": true` and `node_id`.
- At startup, a cluster node also brings each cluster token's creation time in its own database back to the cluster's, which the classification above uses to recognise the token.
- Teams: adding such a token to a team answers `409` (`node_local_token`, or `not_in_cluster_state` / `history_mismatch` for those rows): the cluster does not manage the token; create a cluster token and add that one. Removing it from a team (`DELETE …/tokens/:id/teams/:team_id?node=<node_id>`) applies on that node only. A membership this node cannot remove locally answers `409` (`cluster_membership`).
- Standalone nodes and OSS are unchanged; `?node` is ignored there.
- Audit (enterprise): `POST …/revoke` is now recorded as `token.revoked` (it was `token.created`), `PATCH …/tokens/:id` as `token.updated` (was `api.patch`), and adding or removing a team as `token.team_added` / `token.team_removed` (were `token.created` / `token.deleted`). A change on one node records `scope=node`, `node_id`, `token_id` and `token_name` in the event detail; a refusal records its `code`.
- `operations/pre-26.06.1-token-upgrade.md` describes the API way to remove an old token from each node.

Performance: authentication is unchanged. A token write on a cluster node adds one in-memory lookup in the cluster state; a change to a node-local token is a single SQLite statement instead of a cluster round-trip.

## Security fix: on a cluster, team memberships always follow the token they were granted to ([#1630](https://github.com/Basekick-Labs/arc/issues/1630))

On clusters upgraded from a release before v26.06.1, a team membership could be applied to the wrong token when a token kept from that release shares its ID with a cluster token. Memberships now count only for the token they were granted to, and a node drops a token's cached permissions whenever it changes that token's row.

- Affected: clusters upgraded from before v26.06.1 that still hold tokens from that release. Standalone nodes that never joined a cluster are unaffected.
- After upgrading, check the teams of tokens marked `"node_local": true` in `GET /api/v1/auth/tokens` with `GET /api/v1/auth/tokens/:id/teams`. On a node whose Raft data was rebuilt, review those tokens' teams by hand.

Performance: one more primary-key join when a token's teams are loaded on a permission-cache miss (at most once per token every 30 s by default): about 4 µs more per load in a local benchmark, same allocations. Cache hits and authentication are unchanged.

## Fix: token, team and role changes are recorded in the audit log ([#1630](https://github.com/Basekick-Labs/arc/issues/1630))

Audit logging (Enterprise) has never recorded a change made through `/api/v1/auth/tokens/*` or `/api/v1/rbac/*`. Those routes were registered ahead of the audit middleware, so the `token.*` and `rbac.*` events in the documentation were never written, on any version since audit logging shipped.

- **Now recorded:** creating, updating, rotating, revoking and deleting a token, adding a token to a team or removing it, and every change to an organization, team, role or measurement permission. The actor is the token name.
- **Refusals:** a request whose token is valid but lacks the permission (403) is recorded as `auth.failed`, with the token name as actor. A request to an authenticated route with no token or an invalid one is refused by authentication **before** the audit log and is **not** recorded, as before.
- **Requests without a token that are recorded:** a public route sits ahead of authentication, so a write (POST, PUT, PATCH, DELETE) to one is recorded with actor `anonymous`, as before this release. In practice: `POST /api/v1/internal/cache/invalidate`, the HMAC-signed cache invalidation cluster peers send after a compaction (one row per call, a refused signature included), and, only with `auth.metrics_public` on, a write to `/api/v1/metrics` or a path under it or under `/metrics/`, which has no write route and is answered 404 or 405.
- **Never recorded:** `/health`, `/healthz`, `/metrics`, `/api/v1/ready`, `/api/v1/logs`, and now `/api/v1/auth/verify`, in any method; and reads of the `/api/v1/metrics` family.
- **Event names:**
  - RBAC events are now named after the resource the route changes, not the first segment of its path.
  - A team created under an organization is `rbac.team.created` (it would have been `rbac.org.created`).
  - A role created under a team is `rbac.role.created`.
  - A measurement permission is `rbac.measurement_permission.created` or `.deleted`.
  - An update, which these routes do with `PATCH`, is `rbac.<resource>.updated` (it would have been `.read`).
  - Token events use the names in the section above.
- **Reads:** `audit_log.include_reads` now covers HEAD and OPTIONS as well as GET. Before, HEAD and OPTIONS requests were recorded at the default setting; now none of the three is recorded unless `include_reads = true`, which also records token and RBAC reads. `/api/v1/auth/verify` is not recorded even then: it is public, so an event could never name the caller, and clients poll it.
- **Status recorded:** an event for a request that ended in an error (for example a `404` for an unknown ID, a `405`, or a `500`) now records that status. It used to record `200`.
- **Recorded fields:** the stored path, database and measurement are capped at 256 bytes, like the user agent already was. The event type is decided on the full path first, so padding an ID with leading zeros cannot turn a rotate or revoke into an unnamed `api.post`.
- **Startup check:** with audit logging on, Arc checks at startup that every non-public route runs after the audit middleware and that it is installed once. A failure is logged at error level, naming the routes; startup continues.
- **Not recovered:** changes made before this release were not recorded and cannot be recovered.
- **Performance:** none on the request path. The audit middleware moved earlier in the same stack; one walk of the route table at startup.

## Dependencies: tests build msgpack payloads with the encoder production uses ([#1709](https://github.com/Basekick-Labs/arc/issues/1709))

Test-only; no product behaviour changes. Five test files (in `internal/cluster`, `internal/wal` and `internal/ingest`) built their msgpack payloads with upstream `github.com/vmihailenco/msgpack/v5`, while Arc itself encodes and decodes with its fork, `github.com/Basekick-Labs/msgpack/v6`. They now use the fork, so the tests encode with the same library Arc decodes with (the payload bytes are equivalent either way). `vmihailenco/msgpack/v5` is no longer a direct dependency; it stays in `go.mod` as an indirect one, pulled in by `iceberg-go` through `uptrace/bun`, which uses it only for fields tagged `bun:",msgpack"`; iceberg-go's catalog models have none, so that copy is linked but never runs. `go mod tidy` also records `google.golang.org/grpc` as the direct dependency it already was (Flight SQL imports it).

## Fix: Prometheus Remote Write reuses Arc's Snappy dependency (#1702)

The Prometheus Remote Write receiver now uses Arc's existing `klauspost/compress/snappy` dependency instead of adding a separate Snappy module. It decodes the Snappy block format at the same speed as before, and malformed or truncated bodies get the same `400` and error text (`snappy: corrupt input`) as before. The one difference: a body using the s2 format's offset-0 copy, which no Snappy encoder produces, is now decoded rather than refused; its decoded size is still bounded before decoding, and the protobuf parser still validates the result.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1711](https://github.com/Basekick-Labs/arc/pull/1711).

## Fix: Iceberg table repairs are retried, and their metadata copies cleaned up ([#1097](https://github.com/Basekick-Labs/arc/issues/1097), [#1098](https://github.com/Basekick-Labs/arc/issues/1098))

The Iceberg exporter repairs two table properties when they drift: the column name mapping external readers need for columns added later, and the metadata-retention settings after `iceberg.retain_snapshots` changes.

- **A failed repair is now retried on the next pass.** A pass whose repair commit failed could still be recorded as settled, so a measurement whose files had stopped changing was not looked at again until a file changed or Arc restarted, while the log promised "will retry next pass". A failed repair now leaves the pass unsettled, as a failed `version-hint.text` write already does, and the next pass retries it. As for a failed hint write, a repair that keeps failing makes every pass reconcile that measurement again, with a warning each time. The Reconciled log line (on passes that commit data) gains `heal_ok`.
- **A repair's metadata copy is pruned on the same pass.** Each repair publishes its own `v<N>.metadata.json`. On a pass with no data changes nothing pruned it, so the table sat one copy over `retain_snapshots + 1` until the next pass that changed data. Every pass whose final `version-hint.text` write lands now prunes, keeping `retain_snapshots + 1` copies as before.
  - A pass never prunes a version a reader may be resolving: the version `version-hint.text` named when the pass started, the table's version then (they differ after an earlier failed hint write), and every version the pass itself wrote to the hint. A pass that publishes several times can therefore leave a few copies over the bound until a later pass prunes again.
  - Nothing is pruned when the pass's final hint write fails, or when the hint could not be read at the start of the pass; such a pass stays unsettled and is retried. If hint writes keep failing, copies accumulate until they succeed again.
  - A node that stops being the writer during a pass skips the repairs, and from the moment it notices publishes and prunes nothing; the new writer settles the table. Only a loss between the pass's last check and its end still publishes what the pass committed, without expiring, pruning or sweeping.
  - Cost: one read of `version-hint.text` per pass that reconciles a table, and one listing of its metadata directory on passes that commit no data.

No configuration changes.

## Fix: every configuration key Arc reads has a declared default ([#1109](https://github.com/Basekick-Labs/arc/issues/1109))

`storage.s3_prefix` and 15 other keys were read without a default declared next to the rest: the other S3 and Azure primary-storage keys, `tiered_storage.cold.s3_prefix`, `auth.bootstrap_token`, `auth.force_bootstrap` and `iceberg.warehouse`. They now have one. A spoke no longer reads `edge_sync.spoke.relay.staging_dir`, which only the hub uses. Every new default is the value an unset key already had (empty, or `false`), so nothing changes at runtime; this keeps the code and the documented defaults from drifting.

A test now fails when a key read in `internal/config` has no default, including the target fields of the edge-sync relay. Four keys are read on purpose without one, each listed with its reason: `backup.targets` (a map: a default would be a phantom target) and the edge-sync spoke secrets (`edge_sync.spoke.secret`, `shared_secret`, `hub_token`), which are refused in the configuration file and come from `ARC_EDGE_SYNC_SPOKE_SECRET` / `ARC_EDGE_SYNC_HUB_TOKEN`. Per-target backup keys (`backup.targets.<name>.*`) get their defaults when the targets are loaded, as before.

## Fix: Iceberg export no longer fails a measurement because compaction deleted a file mid-pass ([#1706](https://github.com/Basekick-Labs/arc/pull/1706))

Compaction, a `DELETE`, retention and tier moves delete data files while an Iceberg export pass may be reading them. Two things then failed the measurement, logged at Error and counted as `failed`, until the next pass on the full interval:

- the listing kept changing under every attempt (`kept changing under the listing … keeping the previous snapshot this pass`);
- a file listed a moment earlier was gone when its Parquet footer was read (`NoSuchKey`, `no such file`).

Compaction's minimum ages keep it away from files that are still being written. They do not keep it away from the export, and the first export of an existing backlog covers exactly the older days daily compaction picks. Seen in testing against S3 with a daily compaction cycle running during a first registration.

A measurement in that state is now reported as `busy`: a new count in the pass summary, logged at Info, and the next pass comes early, as for `in_progress` (after 30 seconds at the default `iceberg.reconcile_interval`).

- **The listing check is unchanged.** A set listed while files were vanishing is still never exported: the vanished file's rows may be in a file that listing missed. Only how it is reported changed.
- **A file that disappears after the listing** is confirmed with its own storage backend (the cold tier for a cold file) before anything is decided. A read error on a file that still exists, or any other read error, fails the measurement as before.
- **New files:** the vanished file's whole UTC day is left for the next pass; the day's other new files are, almost always, inputs of the same compaction about to be deleted too. The rest of the pass registers as usual. These files were not in the table, so readers lose nothing.
- **A day the pass was also removing files from** (a compaction output, a rewrite, a tier move) is left exactly as the table has it, removals included, and the other days commit. A day is never split, so readers never see a compacted day with rows missing. If any file in that commit has a path outside the `year/month/day` layout, the whole commit waits for the next pass instead.
- A pass that left anything behind is never cached as settled, so the next pass lists again.
- **Still visible if it does not clear.** A measurement that stays busy, with nothing committed or settled, for 30 minutes is logged at Warn and counted as `failed`, and no longer brings passes forward. The clock is kept in memory: a restart or writer failover starts it again.

No configuration changes.

## Fix: Iceberg export registers a large measurement over several passes instead of never ([#1686](https://github.com/Basekick-Labs/arc/issues/1686))

The first export of a measurement with many files (enabling Iceberg on an existing deployment, or recreating a table) read every new file's Parquet footer twice and then committed once, all inside the 2-minute per-measurement limit. A measurement too large for that, or on a slow or distant object store, never committed anything: each pass hit the limit, logged `context deadline exceeded`, and the next pass started from zero.

New files are now committed in chunks of up to 256. Each chunk reads its own footers, adds any new columns to the table, and commits. A pass that runs out of time keeps every chunk it committed, and the next pass continues from what the table holds, including after a restart.

- **Progress is not a failure.** A measurement that is still being registered is counted as `in_progress` in the pass summary, not `failed`. A pass that could not commit any chunk at all is still reported as `failed`, and does not bring the next pass forward. One Info line gives how many new files were registered and how many remain. The next pass starts after 30 seconds (or a tenth of `iceberg.reconcile_interval`, if larger) instead of the full interval.
- **Newest files first.** Recent data is queryable first while older data is still being registered. While that is in progress, readers see a table with part of the measurement's history.
- **Slow stores still converge.** When a chunk does not fit in the time left, it commits nothing, and the chunk size for that measurement halves, down to 8 files. The size doubles again after each chunk that finishes in under 15 seconds. The chunk size is kept in memory, so a restart starts again at 256.
- **What changes together still commits together.** Removals, tier moves, files rewritten in place, and any new file in a UTC day that also loses a file all go in one commit, as before. Compaction (inputs out, output in) therefore never shows readers a day with its rows missing. Only new files with no removal in their day are chunked.
- **Once per pass:** `version-hint.text`, snapshot expiry (beyond the history floor below), version-file pruning and the orphan sweep run after the pass's commits, not after each chunk. A chunk that adds columns still publishes the discovery files when it changes the schema, as before. In a pass of several commits, each one also writes its `v<N>.metadata.json` copy before the next, so a stale second writer on a shared bucket still collides with a published version and is stopped (#1609); those copies are kept until the next pass.
- **A node that loses the writer role mid-pass** stops before its next commit, including a schema change, and writes nothing more.
- **A table is never created before a footer is read.** Its `day(time)` partition comes from the schema of the first chunk, which is read first. If that read fails, no table is created.
- **History floor retried.** The snapshot-history floor after a removal (#1092) is taken straight after the commit that removed the files, as before. If it fails, later passes now retry it until it succeeds, including passes with nothing to commit. Before, the retry pass took the "already converged" path, which never expired snapshots. The retry is remembered in memory, so it does not survive a restart.

No configuration changes. The 2-minute per-measurement limit is unchanged: it now limits how much one pass does, not whether it can succeed.

Approach and tests build on [#1704](https://github.com/Basekick-Labs/arc/pull/1704) by @kathennn.

## Fix: with the WAL on, a flush that fails part-way no longer stores some rows twice ([#1697](https://github.com/Basekick-Labs/arc/issues/1697))

With the WAL on, a write whose rows span several hours is flushed as one Parquet file per hour, and each file is registered and its rows checkpointed in the WAL as soon as it is stored (#1492), so a failure later in the flush replays only the hours not yet written. That relies on the WAL's row checkpoint naming every row of the file. Some files hold rows it cannot name:

- rows of a chunked payload (one batch stored as several WAL entries);
- every row of a file whose rows need more than 1,024 row ranges to name. A backfill sent as many small writes, each with rows in the same hours, does this with no special layout, and so does one large write whose rows alternate between hours.

Those files were registered as stored, with their rows left unchecked. If a later hour then failed, WAL replay wrote those rows again into new files: they were stored twice, in storage and in the cluster manifest. In a reproduction with 1,054 two-hour batches and the second hour file failing, 3,162 rows were stored for 2,108 sent.

Such a file is now **deferred**: stored, but registered only after the flush's last hour (before its WAL checkpoint), and written after the files that can be named. If the flush fails part-way, a deferred file is left unregistered and handed to the cleanup that already removes the late-landing copy of a failed write ([#1635](https://github.com/Basekick-Labs/arc/issues/1635)). It is removed only after WAL replay has stored every one of its rows in another file, never before. If that does not happen within an hour (a WAL that accepted an entry and then failed to write it, or replay storing the rows under an identity the cleanup cannot match), or by shutdown, the file is registered instead and kept as the copy of its rows: a possible duplicate, as before, never a loss, and never an unregistered file a cluster's reconciliation sweep could take for an orphan. The same reproduction now stores each row once.

- A file that holds a row with no WAL copy (a replicated entry) is that row's only copy and keeps the previous behaviour: registered as stored, with a possible duplicate after replay. So does a file with rows whose WAL identity replay cannot match (a content hash, or a recovery replay with no identity).
- On a single node, a deferred file gets no flush intent ([#1646](https://github.com/Basekick-Labs/arc/issues/1646)): an intent naming only some of its rows would let recovery adopt the file and keep its other rows twice.
- Until replay runs (the next WAL recovery pass), the rows of a deferred file are in storage but not registered: on a cluster, readers that go by the manifest do not see them; on a single node, queries list storage and see them, and briefly see the replayed copy too, until the cleanup removes the deferred file.
- A cluster with coverage on announces the flush's coverage group even when every file is deferred.
- If the cleanup's tracker is full, a deferred file is registered at once and logged: its rows are stored twice after replay, as before.
- What is cleaned up: a deferred file of a flush that **fails** while Arc runs. A crash mid-flush leaves its deferred files unregistered with nothing tracking them (the tracker is in memory): replay after the restart stores their rows again; on a single node, queries then see them twice, as before; on a cluster the leftover file is an unregistered orphan. Rows of a chunked line-protocol (row-format) payload are usually replayed in row ranges, which the cleanup cannot match against the whole entry: those files are registered after the hour, as before.

## Fix: without a WAL, a flush that fails part-way keeps track of the hour files it stored ([#1693](https://github.com/Basekick-Labs/arc/issues/1693))

With the WAL off (the default), a write whose rows span several hours is flushed as one Parquet file per hour. If an upload failed part-way, the hour files already stored stayed in storage but were never registered: they were missing from the tiering metadata and, on a cluster, from the file manifest. The flush was also logged and counted as losing **every** row, stored ones included.

Each hour file is now registered as soon as it is stored, so a failure later in the flush leaves the earlier files tracked:
- **Tiering:** they reach the tier metadata, and are migrated and covered by tier-aware retention.
- **Cluster:** they are in the manifest, so readers that go by the manifest see them, and the reconciliation sweep does not treat them as orphans to delete. As for every registration made without a WAL, the registration journal entry is not fsynced, so it survives a process crash but not a power loss.

The failure log now reports `records_stored` and `files_stored`, and `records_without_wal` and `arc_buffer_records_dropped_total` count only the rows in no stored file. Two reproductions:
- On Amazon S3, a 20,000-row backfill over 2,000 hours was cut off by a shutdown. The old binary stored 204 hour files and counted all 20,000 rows as lost. The fixed one stored 205 files and reported 2,050 rows stored and 17,950 lost.
- On MinIO, paused part-way through a 3,650-row, 365-hour backfill, 337 files were stored, 280 rows were counted lost, and 3,370 rows were queryable.

With the WAL on, each hour file was already registered and checkpointed as it was stored (#1492). One change applies to both: an hour file whose upload completes just as the shutdown budget runs out still gets its tiering record (bounded by `ingest.flush_timeout_seconds`) instead of failing it on the cancelled flush. The cluster manifest registration takes no deadline and already happened in that case, so only the tiering record changes; what happens if that file's WAL checkpoint is dropped is unchanged (a single node adopts the file through its flush intent, #1646; a cluster node replays its rows, at least once, as before). The count of rows a shutdown leaves for WAL replay is also corrected for a flush that stored only some of its hour files.

## Fix: a long backfill flush no longer holds up age-based flushing, or DELETEs on other databases ([#1695](https://github.com/Basekick-Labs/arc/issues/1695))

Since #1692, a write spanning many hours can legitimately take minutes to flush, one file per hour. Two waits that were implicitly capped at about 30 s now waited for the whole backfill.

- **Age-based flushing.** The periodic sweep writes buffers that have reached `ingest.max_buffer_age_ms`. It used to wait for each batch of those writes, so one backfill held up age-based flushing of every buffer on the node, and low-rate measurements were written late (and, with the WAL off, held in memory longer).
  - The sweep now starts each write and moves on. It has `ingest.flush_workers` write slots of its own and waits only when all of them are busy.
  - A measurement whose previous age-based write is still running is not picked up again until that write ends. Its new rows stay buffered, so one slow backfill cannot occupy every slot.
  - Aged data still never queues behind size-triggered flushes and never takes a writer's slot. Shutdown still waits for every write in progress.
  - The sweep still stalls once every slot is held by a slow write, as it did before: with `flush_workers = 1`, one backfill is enough; with N workers, N backfills of different measurements running at once.
- **Flushes before a DELETE or a CQ watermark.** A row-level DELETE (local, cluster, or the peer flush it asks for) and a cluster continuous query's output flush wait for buffered rows of their own database (or measurement) to be written. They used to wait for every flush in progress on the node, and failed after `ingest.flush_timeout_seconds` while any backfill ran on any database. They now wait only for flushes of their own scope, and a wait for one scope no longer blocks a wait for another while it waits for its flushes to finish. A timeout says what it was waiting for: a flush of its scope still in progress, another flush barrier, or flush-queue space.
  - **What still applies.** The WAL recovery barrier still waits for every flush, so with the WAL on, WAL purging waits for a running backfill to finish. A flush failure on any database during the wait still fails the DELETE. If size-triggered flushes fill every flush worker and the queue, a DELETE's own flushes still queue behind them, and while it waits for queue space it also holds up other scopes' waits. A DELETE on the database being backfilled still fails if the backfill outlasts its budget, and nothing is deleted.

## Fix: a backfill flush is no longer cut off at 30 seconds in total ([#1692](https://github.com/Basekick-Labs/arc/issues/1692))

`ingest.flush_timeout_seconds` (default 30) is documented as the timeout for
storage writes during a flush, but it bounded the **whole** flush. A write whose
rows span many hours flushes as one Parquet file per hour, uploaded one after
another, so a backfill hit the deadline part-way however healthy each upload
was. Measured on S3 in the same region (~25 ms per file), any single write
spanning more than about 1,100 hours failed; over a slower link far fewer. With
the WAL off the rows not yet uploaded were lost after the write had been
acknowledged; with the WAL on they were kept but replayed only at the next
restart.

The timeout now applies to **each hour file** of a flush, and covers
everything done for that file: the Parquet encode, the storage write, the
field-schema registration (which reads and writes storage), the tiering and
cluster-manifest registration, and the WAL row checkpoint. A single-hour
flush, which is every flush in live ingest, still gets one timeout (now
starting after the in-memory merge and sort). A flush of N hour files can take
up to N × the timeout and has no cap on N. Cluster replay of a claimed WAL
range, which had no time limit, now also gets the timeout per hour file.

- One stuck upload is still cut off at the timeout, as before.
- At shutdown, Arc stops a flush between hour files once its shutdown budget
  runs out (it already cancelled uploads in progress then); previously a flush
  on local storage kept writing its remaining files. The hours not yet written
  stay in the WAL (with the WAL off they are lost, as any unflushed rows are at
  shutdown).
- **While a large backfill flushes, other work waits longer than before**, up
  to the length of the flush instead of ~30 s:
  - Age-based flushing of **every other buffer on the node**: the age sweep
    writes its buffers and waits for them, so a backfill flushed by age holds
    up the next sweep. Size-triggered flushes are not affected. Nothing is
    lost, but low-rate measurements are written late.
  - A write request that triggers a schema-change flush, and import and
    flush-all calls, run until the flush is done.
  - Calls that wait for in-flight flushes with a budget of one timeout fail
    safe while a backfill flush runs, on any database: WAL recovery keeps its
    files and completes on a later pass; a row `DELETE` and a cluster
    continuous-query run answer an error and change nothing. Retry after the
    backfill.

  Before this fix the same backfill failed after ~30 s and lost or deferred its
  rows; follow-up work to keep it from holding up other buffers is tracked in
  [#1695](https://github.com/Basekick-Labs/arc/issues/1695).
- The config comment said `0 = no timeout`; a value of 0 or less has always
  meant 30 s. The comment now says so.

No configuration change. Throughput of live ingest is unchanged: one extra
context per flush, not per row.

## Fix: `cluster.seeds` written as a TOML array is read ([#1675](https://github.com/Basekick-Labs/arc/issues/1675))

`seeds = ["10.0.0.1:9100", "10.0.0.2:9100"]` in `arc.toml` now gives the node those seeds. Before, Arc read the TOML array as no seeds at all and gave no warning. The node started with `seed_count: 0`, logged `no known peers, aborting election`, and never joined. Only the comma-separated string worked. An array element that contains commas is split into one seed per address.

`backup.target_names` had the same bug and is fixed the same way. A target named only in a TOML array, with its fields set from the environment, was never discovered.

For every other spelling nothing changes. The comma-separated string (`seeds = "a:9100,b:9100"`) and the environment variables (`ARC_CLUSTER_SEEDS`, `ARC_BACKUP_TARGET_NAMES`) read exactly as before, and an environment variable still overrides the file. Every shipped deployment (docker-compose, Kubernetes, Helm) passes seeds through `ARC_CLUSTER_SEEDS`, so none of them was affected. `arc.toml` now documents `cluster.seeds` with both spellings.

**Upgrade note:** a TOML array that was ignored before now takes effect. Seeds listed in a `seeds` array are dialled. Every name in a `target_names` array must be a valid target name and, with backups enabled, a fully configured target, or Arc refuses to start. If an old array was left in `arc.toml` by mistake, remove it before upgrading.

## Fix: backups find the Iceberg metadata of edge-sync spoke databases ([#1202](https://github.com/Basekick-Labs/arc/issues/1202))

Two fixes in how a backup recognises the Iceberg exporter's namespace directories.

- **Unscoped backups, warehouse outside the storage root.** The Iceberg table metadata of an edge-sync spoke whose id contains a dot is now backed up. iceberg-go stores such a namespace under a JSON-encoded directory name (`__iceberg_namespace_v1__:[...].db`), and the backup only walked directories starting with `<prefix>_`. Such a backup carried the catalog rows for those tables but not the metadata they point at. A warehouse under the storage root was not affected, because the data listing covers it. If one of these directory names is too long to store as a backup key, the backup now fails and names it. Before, it left those tables out without saying so. This is the same rule as for any other warehouse file.
- **Scoped backups.** A backup scoped to an edge-sync spoke now reports the spoke's Iceberg metadata that it leaves out (`iceberg_namespace_files_excluded`, `iceberg_namespaces_excluded`, and the log line that says to take an unscoped backup for the Iceberg tables). Before, it always reported zero. A spoke has one namespace directory per database (`arc_<spoke>.<db>.db`, or the encoded form), and the backup looked for a single `arc_<spoke>.db` that never exists. What a scoped backup copies has not changed: it never copied the warehouse. A directory name that could belong to either of two scoped names (`arc_a.b.db`: spoke `a`'s database `b`, or a name `a.b`) is reported as left out by whichever is in scope. As before, the count is made only when the warehouse is on local storage.

## Fix: an edge-sync `allowed_dirs` path containing a space can be set from the environment ([#1680](https://github.com/Basekick-Labs/arc/issues/1680))

`edge_sync.import.allowed_dirs` and `edge_sync.spoke.bundle.allowed_dirs` now read a string, from `arc.toml` or `ARC_EDGE_SYNC_IMPORT_ALLOWED_DIRS` / `ARC_EDGE_SYNC_SPOKE_BUNDLE_ALLOWED_DIRS`, as a **comma-separated** list. Before, a string was split on whitespace, so `"/Volumes/Air Gap"` became two allow-list entries, `/Volumes/Air` and `Gap`. The second resolved against Arc's working directory. Bundles were refused from the directory the operator named, and silently allowed in two nobody named. A comma-separated list (`"/a,/b"`) used to be read as one path; it is now two. TOML arrays read as before. A path that contains a comma needs the array spelling.

Every `allowed_dirs` entry must now be an absolute path. With import or bundle export enabled, Arc refuses to start on a relative entry and names it. A relative entry was resolved against Arc's working directory. The realistic way to end up with one is a separator in the wrong place, such as `"/mnt/a,b"` in the environment, which reads as `/mnt/a` and `b`. A refused bundle path now lists each allowed directory in quotes, so a list that was read differently from how it was written is visible.

**Upgrade note:** a whitespace-separated list in either variable, such as `"/mnt/a /mnt/b"`, is now read as a single path. Arc still starts, but every import or export is refused. Rewrite it as `"/mnt/a,/mnt/b"` or as a TOML array. This form was never documented. A relative `allowed_dirs` entry, also never documented, now stops startup. Make it absolute.

`arc.toml` now documents both spellings of `allowed_dirs`, and the environment form of `ingest.sort_keys` and `ingest.decimal_columns`. Their entries contain commas, so in the environment the entries are separated by whitespace: `ARC_INGEST_SORT_KEYS="cpu:host,region mem:host"`. That behaviour is unchanged, and now pinned by a test. `compaction.exclude_databases` is also unchanged: a comma inside one of its entries is deliberately kept whole, because a spoke ID may contain one.
## New: a spoke can delete its local copy once the hub has verified it ([#569](https://github.com/Basekick-Labs/arc/issues/569), phase 3a)

With `edge_sync.spoke.delete_after_sync = true`, a spoke reclaims disk space by deleting its own copy of a file after the hub has **verified** that it still holds the file. This is off by default.

- **When a file is deleted.** Verification is a separate request from the one that synced the file, sent on a later pass and at least `edge_sync.spoke.delete_after_sync_delay` after the sync (default `0s`). The hub answers "held" only when it still has the file itself, at the path it received it under, and on a cluster only if the cluster still lists it. Only then is the spoke's copy deleted.
- **When a file is kept.** The hub may have merged the file into a compacted file (`edge_sync.compact_received_namespaces`, on by default) or removed it on purpose. A removed file is always kept. For a compacted file, see the next section: by default the spoke keeps its copy and asks again a day later. A file the hub has forgotten entirely (its admin forget route) is sent again.
- **Older hubs.** The spoke first sends the hub an empty test request. If the hub is too old to verify, the spoke deletes nothing, logs a warning, and asks again a day later. It never sends such a hub its list of synced files.
- **Turning it on for a spoke that has been syncing for a while** verifies, and may delete, every synced file still in the ledger: up to `ledger_retention_days` (90 days by default) of data on the first passes.
- **A failed verification** never fails the sync pass and deletes nothing. The pass reports why in `verify_error`.
- **Refused at startup** without `edge_sync.spoke.enabled`, bundle export or the relay ([#1685](https://github.com/Basekick-Labs/arc/issues/1685)), on a cluster node, or, with the network path on, with a `hub_url` that is neither `https://` nor a loopback address. The hub's answer decides which local files are deleted, so it must not be forgeable in transit.
- **Local queries** on the spoke no longer see data whose files were deleted.
- **Licence.** The setting itself needs no licence, so manual passes (`POST /api/v1/spoke-sync/run`) delete. Scheduled passes run only with the edge-sync scheduler licence, as before. A pass reports `local_deleted`, `verify_kept`, `verify_requeued` and `verify_unsupported`.

Air-gapped spokes can use it too: see "`delete_after_sync` for air-gapped spokes" ([#1684](https://github.com/Basekick-Labs/arc/issues/1684)).

## New: a spoke can also delete files the hub has compacted, if it opts in ([#1683](https://github.com/Basekick-Labs/arc/issues/1683))

The hub now records where a spoke file's content went when it compacts the file, when it compacts that compacted file again, and when it migrates it to the cold tier. A verification can then confirm that the hub still holds the file it went into. Before, a file the hub had compacted could never be verified, so a spoke never deleted it. Hub compaction of received data is on by default, which made this the common case.

Such files are reported separately, and a spoke deletes them only with `edge_sync.spoke.delete_after_sync_accept_compacted = true` (default `false`). Hub compaction does not keep the spoke's exact bytes:

- It drops rows that are identical in every column.
- It widens a column that mixes integers and floats to floating point, which loses precision above 2^53.
- It turns timestamps it cannot parse into NULL.

With the setting on, the spoke's only exact copy goes. Some files have no recorded destination and are always kept: files the hub compacted or moved to the cold tier before this release, and files a row-level `DELETE` rewrote on the hub. If hub retention later deletes the compacted file a spoke file went into, that spoke file is kept too, and the spoke keeps asking about it once a day.

## New: `delete_after_sync` for air-gapped spokes ([#1684](https://github.com/Basekick-Labs/arc/issues/1684))

An air-gapped spoke can now use `edge_sync.spoke.delete_after_sync` with only bundle export enabled (`edge_sync.spoke.bundle.enabled`).

- **How it works.** When the hub imports a bundle, it checks which of its files it still holds, the same check a networked spoke gets. It signs the answer into the acknowledgement it writes onto the drive. When the spoke applies that acknowledgement (`POST /api/v1/spoke-sync/ack`), it deletes its local copies of those files: right away with the default delay of 0, or later, by the check that runs every 15 minutes, with a longer delay. It applies the same rules as networked spokes: `delete_after_sync_delay` and, for files the hub compacted, `delete_after_sync_accept_compacted`. A check every 15 minutes deletes what the delay held back.
- **Only the bundle that carried the file.** An acknowledgement deletes only files that left on that very bundle and that nothing has superseded since. Replaying an old drive after the hub forgot a file deletes nothing. Neither does an acknowledgement older than a more recent network check.
- **A time limit on the acknowledgement.** It rides a drive and cannot see what happens on the hub after the import, such as retention or a forget. So it is honoured only up to `edge_sync.spoke.delete_after_sync_ack_max_age` after the bundle was exported, measured on the spoke's own clock. The default is `168h` (7 days); raise it knowingly. It must be longer than `delete_after_sync_delay`. If the hub cannot fit the attestation within the acknowledgement's size limit (very large bundles), it writes the acknowledgement without it.
- **On a spoke with both the network path and bundles**, a network check that answers after the bundle went out always wins over the acknowledgement.
- **Older versions.** An acknowledgement from an older hub carries no such attestation and deletes nothing. An older spoke ignores the attestation and applies the acknowledgement as before. The ack response reports `attested` and `local_deleted`.

## New: edge sync through a shared bucket ([#1685](https://github.com/Basekick-Labs/arc/issues/1685))

A spoke and its hub can now exchange data through an object store both can reach (S3, MinIO, Azure Blob, Google Cloud Storage, or a shared directory), for spokes behind NAT, on intermittent links, or on networks that refuse inbound connections to the hub. It is the air-gap bundle protocol carried by a bucket instead of a drive: the same signed bundles, the same verify-before-commit import, the same signed acknowledgement, and `delete_after_sync` works as for air-gapped spokes. Off by default.

- **Spoke** (`edge_sync.spoke.relay.*`): every `interval` (default `5m`) it applies the acknowledgements waiting in the bucket, then exports pending files as a bundle under `<spoke_id>/outbox/<bundle_id>/`. Each file streams from spoke storage straight into the bucket, hashed on the way, so the edge box keeps no second copy. The manifest is written last, and a bundle without one is never imported.
- **Hub** (`edge_sync.import.relay.*`): every `interval` (default `1m`) it lists the bucket, one listing per spoke, and checks each new bundle cheapest first. A prefix that names no registered, enabled spoke is skipped without downloading anything. Next it fetches the manifest (at most 64 KiB) and skips a bundle addressed to another hub, remembering it. Then it fetches the entry list (at most 64 MiB) and checks the spoke's signature. Only after that does it download exactly the files the spoke signed, each capped at its declared size, into `staging_dir`, import them as a drive import does, and write the acknowledgement to `<spoke_id>/acks/<bundle_id>.json`. Someone with write access to the bucket but not the spoke's secret can make the hub fetch at most a manifest and an entry list per bundle they plant under a registered spoke's prefix, and each of those is refused once. A drive import in progress is detected before any data is downloaded.
- **Bundle size.** Relay bundles use the drive export's caps, `edge_sync.spoke.bundle.max_files` and `max_bytes` (default 10,000 files and 64 GiB), even with drive export off. A manual pass is bounded by `edge_sync.spoke.bundle.timeout`. The hub's `staging_dir` must have room for one whole bundle.
- **Bucket layout and keys.** The relay target takes a backup target's keys: `type` (`local`, `s3`, `minio`, `azure`, `azblob`, `gcs`) and the matching `local_path`, `s3_*`, `azure_*` or `gcs_*` keys (GCS authenticates as for `storage.gcs_*`: a key file, else Application Default Credentials; not in the FIPS build). The target's own prefix (`s3_prefix`, `azure_prefix`, `gcs_prefix`) is the relay root, and spoke and hub must name the same bucket and prefix. Arc refuses to start if the relay overlaps primary storage, the cold tier or a backup target, or if the hub's staging directory is inside `storage.local_path`.
- **Crash between import and acknowledgement.** A hub that imported a bundle but did not write its acknowledgement rebuilds it on the next poll from its own receipts, without downloading the bundle again.
- **A bundle nobody answers.** After `edge_sync.spoke.relay.ack_timeout` (default `72h`, longer than `interval`), the spoke takes the bundle back, and its files go in the next bundle. A file the hub already holds is answered `already present` then. A bundle whose manifest or whole prefix disappears from the bucket is taken back on the spoke's next pass. A drive bundle in transit is never affected.
- **A bundle the hub refuses** gets a refusal note in `<spoke_id>/rejected/`, signed with the spoke's secret. That covers a manifest or entry list whose signature does not verify, one that is malformed, one stored under another key than the one it names, or one over `edge_sync.import.max_files`. The spoke marks the bundle's files failed, so they are not sent again, and logs the reason. Requeue them with `POST /api/v1/spoke-sync/ledger/requeue` once the cause is fixed. The spoke ignores a note that does not verify. Anything found wrong after the signature has verified is retried instead and is never a refusal: a data object that is missing, the wrong size or the wrong content, or a hub-side error. If the hub never accepts the bundle, the spoke re-sends the files after `ack_timeout`. A bundle for an unknown or disabled spoke, or addressed to another hub, is left alone with no note: a bucket can be shared by several hubs.
- **What the acknowledgement does not advance.** An acknowledgement advances only files that left on that bundle and are still waiting for it, so an old acknowledgement replayed into the bucket advances nothing. A file it reports as a conflict is failed, as on the network path. Any other file in the bundle that it does not acknowledge goes back to pending.
- **Clean-up order.** The spoke deletes its outbox bundle after applying the acknowledgement or refusal; the hub deletes the acknowledgement or note once that bundle is gone. The hub never deletes a spoke's bundle. Credentials can follow that split: the spoke writes and deletes `outbox/` and reads `acks/` and `rejected/`; the hub reads `outbox/` and writes and deletes `acks/` and `rejected/`.
- **Licence.** Scheduled passes, on both sides, need the edge-sync scheduler licence, checked at startup and on every pass. Manual passes are free: `POST /api/v1/spoke-sync/relay/run` on the spoke and `POST /api/v1/bundle-import/relay/run` on the hub, both admin-only. On a cluster, only the primary writer runs scheduled passes; like the drive import route, the manual routes are not restricted to it.
- **Either side can run alone.** A spoke can use the relay with or without the network path and drive export, and a hub can poll a bucket without `edge_sync.import.enabled` (the drive route).
- **Storage.** Azure Blob storage gains a conditional write (`If-None-Match: *`), used for acknowledgements. A store that does not honour the condition on a write gets a plain write: an acknowledgement written twice is harmless.

## Query governance policies are cluster-wide ([#1627](https://github.com/Basekick-Labs/arc/issues/1627))

Once every node runs this version, the cluster adopts the nodes' query governance policies (rate limits, quotas, row and scan-time caps per token). From then on a policy created, changed or deleted on any node applies on every node, and survives a failover. The cluster state is the only copy that is read; each node's own table, on nodes where governance is off too, is kept equal to it as a backup and is never read again.

- **Adoption.**
  - The cluster takes the union of the nodes' policies, so a policy set on one node now applies on all of them.
  - Policies whose token no longer exists are dropped, and each one is logged.
  - A node that is away (restarting) when the cluster adopts the policies is not asked. When it comes back, its own policies that the cluster did not adopt are kept in its `governance_policies_dormant` table and logged with their token IDs, never enforced. That table can also hold policies deleted in the cluster while the node was away, so check before recreating any through the API.
  - If two nodes define the same token's policy differently, an admin chooses one node's set with `POST /api/v1/cluster/policies/adopt` (`"kind": "governance"`). `GET /api/v1/cluster/policies?kind=governance` lists each node's policies and the hash to name. The other nodes' own governance policies are then no longer enforced; each node keeps them in its `governance_policies_dormant` table.
  - Adoption waits while a node holds a policy for a token created on that node before it joined the cluster (before v26.06.1), or a token the node cannot report on. The wait names the node and the token: re-issue the token through the cluster, or delete that policy. `GET /api/v1/cluster` shows it under `cluster_policies.kinds.governance`.
- **After adoption.**
  - Policies apply to cluster tokens only. A create or delete for any other token answers 404, "not a cluster token"; a change answers 404, no policy.
  - A policy applies only to the token it was set for: a different token that a node keeps at the same ID gets that node's defaults.
  - A node that restarts never enforces its own table again. Until its cluster state holds the adopted policies it applies only `governance.default_*`: policy creates, changes, deletes, the list and a policy GET answer 503 (retry), while the usage endpoint answers 200 with `"scope": "node"` and the defaults. It then applies its last Raft snapshot's policies, and the changes made since as it replays them from the leader.
  - A node that joins after the adoption enforces its own table's policies, as before, until its cluster state has caught up; its table is then replaced by the cluster's, and its own policies that the cluster does not hold as they are go to `governance_policies_dormant`.
  - Deleting a token deletes its governance policy.
  - The policy list, get and usage answers carry `"scope": "cluster"` when the cluster's policies are what this node enforces, and `"node"` otherwise (standalone, before adoption, or a node that has not caught up). Nothing else in them changes; a policy with `id` 0 is still the server defaults.
- **What applies where.** `max_rows_per_query` and `max_scan_duration_sec` apply identically everywhere. Rate limits and quotas are counted per node: a policy of 60 per minute allows 60 per minute on each node that runs queries. `governance.default_*` stays per node, so set it the same on every node; adoption logs a warning naming nodes that differ. A query forwarded to another node is governed once, by the node that runs it.
- **Rollback.** The governance flag (`cluster_policies.governance_enabled` in `GET /api/v1/cluster`; until then the leader's `governance_blocking_nodes` names the nodes still on an earlier release) turns on by itself once every node runs this release, and stays on, like the other cluster policy flags. After that, any node on an earlier Arc release is refused at join, wiped or not: the cluster can go back to an earlier release only by restoring the whole cluster from a backup taken before the upgrade. Removing a node, wiping its data directory and joining it fresh is for rejoining a node on this same release, for example one run for a while with `cluster.enabled=false`; do that before such a node rejoins: unlike retention, which sorts unadopted rows into its dormant tables at every pass, governance copies a node's own policies to `governance_policies_dormant` only the first time its table becomes the cluster's, so policies created on such a node after that are replaced, not kept, when it rejoins.

Performance: on the query path, once adopted, `Admit` reads the cluster's policies from an immutable in-memory snapshot (one atomic load, one map lookup, one string compare), without the lock it took before. `Admit` also no longer allocates per limit: one allocation per admitted query instead of 13 to 16. Measured on Atila against `main`, 16 alternating runs each: a token with a policy 1.26 µs to 0.74 µs (standalone and cluster-wide alike), a token on the defaults 1.31 µs to 0.78 µs, and `Admit` from every core while file registrations are applied 1.53 µs to 0.84 µs per call, with the apply rate unchanged or better (70.4k to 73.8k per second). Ingest and import run no governance code; the snapshot is rebuilt (one map of the governance policies) only when a cluster policy of any kind changes, a token with a governance policy is rotated or deleted, or a Raft snapshot is restored, never on a file registration.

## Fix: a deleted spoke database stays deleted, and can be re-synced on request ([#1186](https://github.com/Basekick-Labs/arc/issues/1186))

On an edge-sync hub, what happened after `DELETE /api/v1/databases/{spoke}?confirm=true` depended on configuration. With tiering active the spoke never sent the data again. Without it, a re-sync brought back only part of the namespace: the files hub compaction had not merged. Hub retention over a spoke namespace looped: retention deleted the files, the spoke sent them again, and the next pass deleted them again.

The hub now records content it deletes on purpose as **removed**, with the reason (`operator_delete` or `retention`), in every configuration. This covers a confirmed database delete of a spoke namespace and hot and cold retention. The spoke does not send that content again, and neither does an old air-gap bundle.

To get deleted data back from the spoke instead of from a backup, ask for it explicitly:

```
POST /api/v1/sync-spokes/{spoke_id}/forget
{"database": "<the spoke's database>"}
```

This is admin-only. The hub forgets the removed receipts under that database, and the spoke sends those files again the next time it asks about them. A spoke with `delete_after_sync` asks at its next verification, which can be up to a day later for a file it already recorded as kept. A file the spoke already deleted cannot be sent again. Otherwise it asks only after its ledger retention (`ledger_retention_days`) prunes the row and discovery finds the file again. Receipts for content the hub still holds, inside a compacted file, are kept. The call is refused with 409 while files remain under `{spoke}/{database}/` in hot or cold storage, because sending the originals back next to those files would duplicate rows. In practice that makes it the recovery from a whole-database delete. After retention it is refused for as long as newer files remain under the database.

On a **cluster** hub this also holds on the node the spoke syncs to ([#1667](https://github.com/Basekick-Labs/arc/issues/1667)). Before that node forgets a receipt whose file is gone, it checks the cluster's replicated file list. A file the cluster no longer lists is treated as removed cluster-wide, so its receipt is recorded as removed (reason `manifest_removed`) and the spoke does not send the file again. That includes the rare file the cluster dropped from its list because the object was lost (manifest repair, the reconciliation sweep). The admin forget route gets such a file sent again. A file the cluster still lists is a local copy that went missing, and the spoke sends it again, as before. If the node cannot catch up with the cluster within 5 s, the spoke's sync attempt fails and it retries later. A cluster run without Raft (`cluster.raft_data_dir` empty) has no such list, and behaves as a single node. The same applies to the row-level `DELETE` fix below.

## Fix: a row-level DELETE on a spoke database no longer brings the deleted rows back ([#1666](https://github.com/Basekick-Labs/arc/issues/1666))

On an edge-sync hub, `DELETE` with a `WHERE` clause on a spoke's data either rewrites a file without the matching rows, under a new name, or removes the file when every row matches. Neither updated the sync receipt for the original file. At the next reconcile the hub saw that file gone, and the spoke sent the original again: the deleted rows came back, and after a rewrite the surviving rows were stored twice.

The hub now updates the receipt before it changes the file:

- For a rewrite, the original is recorded as merged into the new file.
- For a full removal, it is recorded as removed (reason `row_delete`).

Either way the spoke does not send the original again. This also covers cold-tier files whose rows all match. If the receipt cannot be updated, the file is left unchanged and listed under `failed_files`.

## Upgrade note: with the WAL on, a write is acknowledged only once it is durable ([#1499](https://github.com/Basekick-Labs/arc/issues/1499))

`wal.ack_mode` now defaults to `"auto"`, which is `"durable"` on every node
with `wal.enabled = true`: a 2xx means a sync covering the write's WAL entry
has completed, so a crash loses no acknowledged write. Before, the default was
`"queued"`: a write was acknowledged once its entry was queued for the WAL
writer, and a kill -9 right after the acknowledgement could lose it. #1492's
crash harness lost the last acknowledged request, 50,000 rows, in one of seven
runs that way. Nothing in `arc.toml` or the docs' defaults said that "WAL on"
did not mean "acknowledged means durable"; now it does.

`"auto"` resolves to `"queued"` where durable is not available: with the WAL
off (the key has no effect) and where `cluster.wal_coverage = true` is in
effect, on a clustered local-storage writer (experimental,
[#1207](https://github.com/Basekick-Labs/arc/issues/1207)), whose covered append
commits under the ingest shard lock, where a durable wait would stall every
writer to the measurement. A standalone node with the key set ignores it and
stays durable. The coverage case is logged as a config warning; the resolved
mode is in the `WAL enabled` startup line (`ack_mode`, `ack_mode_auto`).
Setting `wal.ack_mode = "queued"` restores the old behaviour; `"durable"`
with `cluster.wal_coverage` is still refused at startup.

What changes with the WAL on:

- **Latency.** Each request waits for the sync that covers its entry, about
  one `fsync` (shared between the requests in flight: a durable entry starts a
  sync at once, it does not wait for the 100 ms interval). Sequential small
  writers feel it most. Measured on an M3 Max (`fdatasync` falls back to a
  full `fsync` there), 8.7M rows of MessagePack columnar writes: 50,000-row
  requests from one client 4.7M rows/s → 2.9M rows/s; 5,000-row requests from
  one client 2.8M → 0.66M rows/s; 5,000-row requests from eight clients 9.4M →
  2.4M rows/s. Throughput-first benchmarks should set `wal.ack_mode =
  "queued"` to stay comparable with earlier numbers.
- **A write the WAL cannot make durable is refused**, with `503`,
  `Retry-After` and `X-Arc-Ingest-Refused: wal_not_durable`, counted in the
  new `arc_wal_not_durable_rejections_total`. Before, the write was
  acknowledged and the entry dropped (`arc_wal_dropped_entries_total`) or
  lost at the crash. The refusal comes after the append: the entry may still
  reach the WAL file and replay at the next start, so a client's retry can
  duplicate rows (at-least-once). A request spanning several measurements is
  written one at a time; a failure after some were buffered answers `500`
  naming what was written, as for other refusals.
- **A stalled WAL device refuses writes instead of losing them.** With
  `"queued"`, a hung disk meant the WAL queue filled, entries were dropped and
  ingest kept answering 204 for writes that were never durable. With
  `"durable"` a request waits for its sync for at most
  `wal.ack_timeout_seconds` (default 30; `0` = no bound,
  [#1501](https://github.com/Basekick-Labs/arc/issues/1501)); past that it is
  refused (`503`, `wal_not_durable`) and its WAL entry retracted without
  waiting on the device, so the client retries and nothing is left pinned.
  The disk-pressure gate (#676) covers low free space, not a hung device.
  Watch `arc_wal_durable_ack_timeouts_total` (a stalled device),
  `arc_wal_failed_syncs_total` (a failing one),
  `arc_wal_not_durable_rejections_total` and the `wal_not_durable` refusal
  log. The HTTP request context plays no part: in Fiber it has no deadline
  and ends only at server shutdown.
- **MQTT.** While the WAL refuses writes as not durable (a timed-out or failed
  sync: the device is stalled or failing), MQTT subscriptions are suspended,
  as they are for a fence, WAL disk pressure and a full buffer
  ([#1502](https://github.com/Basekick-Labs/arc/issues/1502)), and resume
  once a sync succeeds; the writer syncs the WAL file once a second
  meanwhile, so an idle node recovers too. With `clean_session=false`
  and QoS 1 or 2 the broker holds the messages. The message in flight when
  the refusal starts is still briefly retried and then dropped, as for the
  other reasons, and a merely slow device alternates between suspended and
  resumed at about the ack timeout. `arc_wal_durable_ack_failing` is 1 while
  the state holds.
- **The shipped examples that enable the WAL** (`deploy/docker-compose/`
  `oss-local`, `oss-s3`, `oss-traefik` and `enterprise-shared`, and
  `deploy/kubernetes*`) take the new default; `enterprise-local` leaves the
  WAL off.

## Fix: S3 batch-delete errors name the key Arc knows ([#1111](https://github.com/Basekick-Labs/arc/issues/1111))

With an S3 object prefix configured, a failed object in a batch delete (compaction cleanup, a database delete, backup deletion and restore, the reconciliation sweep) was reported under the prefixed object name, a name that appears in no manifest, listing or other log line. The `key` log field and the error now carry the key Arc uses, as Azure and GCS already did. The request sent to S3 is unchanged.

## Fix: a long query no longer costs the buffer flush and WAL close at shutdown ([#1644](https://github.com/Basekick-Labs/arc/issues/1644))

At shutdown the HTTP server waited for in-flight requests for the whole
`server.shutdown_timeout`, and nothing ended a running query. One query that
outlived the budget, or an Arrow stream to a slow client, used all of it. Arc
then skipped everything left: the ingest buffer flush, the WAL close and the
database close. With the WAL off, the rows acknowledged since the last flush
were lost. With it on, they were replayed at the next start, and the process
exited 1. In a cluster the 10 s ready drain ran first and left even less.

Shutdown now keeps a share of the budget for the buffer flush, the WAL and the
database: 60% of `server.shutdown_timeout`, half for the buffer flush as
before and a tenth for what closes after it. The HTTP drain, the Flight SQL
drain, the cluster ready drain and the scheduler stops share the rest:

- the HTTP drain gives in-flight requests a grace, then cancels the queries
  still running, and then closes the connections still open, such as a stream
  to a client that stopped reading;
- the cluster ready drain takes at most half of what is left, so the HTTP
  drain keeps a grace of its own. At the default 30 s that is 6 s rather than
  10 s, so a load balancer that polls `/ready` every 10 s may still route a
  request or two to the closing node. Set `server.shutdown_timeout` to 50 s or
  more to keep the full 10 s;
- a hook that runs past its share no longer skips the components. The hooks
  after it still run, on an expired context.

At the default 30 s, a request has about 8 s to finish once shutdown starts on
a standalone node, and about 3.5 s in a cluster, where a 6 s ready drain comes
first. It used to have the whole 30 s, with the flush and WAL close as the
price. A client whose query is cancelled at shutdown gets a 503 ("Server
shutting down"). Admin writes (tokens, RBAC, cluster policies) are not
cancelled. The HTTP drain fails the shutdown (exit 1) only when a request
waits on neither its query nor its connection. Below a
`server.shutdown_timeout` of 2 s the drain's last stages can still run into
the share kept for the WAL close.

## New: configurable bounds for migrations, manual edge syncs and bundle transfers ([#1112](https://github.com/Basekick-Labs/arc/issues/1112))

Four long-running operations had a hardcoded time limit. Each now has a setting. The defaults are the old values, so nothing changes unless you set one.

| Setting | Bounds | Default |
|---|---|---|
| `tiered_storage.migration_timeout` | one migration cycle, scheduled or `POST /api/v1/tiering/migrate` (and, since #1830, `POST /api/v1/tiering/reconcile`) | `2h` |
| `edge_sync.spoke.sync_timeout` | one manual sync pass, `POST /api/v1/spoke-sync/run` | `2h` |
| `edge_sync.spoke.bundle.timeout` | one air-gap export, `POST /api/v1/spoke-sync/export` | `2h` |
| `edge_sync.import.timeout` | one air-gap import on the hub, `POST /api/v1/bundle-import` | `4h` |

Each takes a Go duration (`45m`, `6h`). A value that does not parse, or that is zero or negative, stops Arc at startup with an error that names the setting and the value. All four are checked even when the feature is off, as `backup.operation_timeout` is.

- Raise `migration_timeout` for a first cold-tier migration of a large dataset. Lower it to make a stuck migration give up and alert sooner.
- The scan at the start of each migration cycle runs inside the cycle's limit. A `migration_timeout` shorter than `tiered_storage.scan_timeout` therefore cuts that scan short too. Arc logs a warning at startup when you configure that.
- Closing the HTTP connection does not stop a manual sync, export or import. Only these settings, or shutting the server down, do.

## Fix: forwarded requests no longer read freed request state ([#1629](https://github.com/Basekick-Labs/arc/issues/1629))

On a compactor, a reader or a standby writer, a forward that was cancelled or
timed out, or whose peer answered before reading the whole body (a 413, for
example), could leave a background read of the original request's recycled
context, or of its body buffer. The race detector reported it, and in
principle the peer could receive bytes of a later request. Forwards now run
on a context of their own, which ends at server shutdown, at the forward's
timeout, or when the query is cancelled or its client leaves, as before. A
body larger than 4KB is not copied: it is released when the forward returns.
`TestForwardedQueryIsListedAndCancellable` is no longer flaky.

## Ingest: buffers that reach `max_buffer_age_ms` are flushed in parallel ([#1632](https://github.com/Basekick-Labs/arc/issues/1632))

Since #1618 every local flush syncs its file and directory to disk. The periodic flush of aged buffers wrote them one at a time, so with many low-rate measurements a single sweep paid for every sync back to back. Buffers then waited several sweep periods past `max_buffer_age_ms`, and files grew larger than intended.

The sweep now writes aged buffers in batches of up to `ingest.flush_workers`, in parallel, and does not hold the shard lock while it writes. Storage can therefore see up to twice `flush_workers` writes at once: the worker pool's plus the sweep's. It does not use the flush queue, so aged data never waits behind size-triggered flushes and never takes a queue slot from a writer. Measured on macOS (APFS, full sync) with 1,000 measurements at one row per second: about 84 aged flushes per second against 23 before, and the backlog clears within about 2 s of the load stopping instead of about 11 s. Gains depend on how well the disk overlaps syncs.

Nothing else changes: what is flushed, when a buffer counts as aged, and how a flush interacts with the WAL and with shutdown. When shutdown begins during a sweep, the sweep finishes the batch it is writing and leaves the rest to shutdown's own flush. A shutdown therefore waits for at most one batch, as it waited for at most one write before.

## Fix: a crash right after a flush no longer stores its rows twice ([#1646](https://github.com/Basekick-Labs/arc/issues/1646))

With the WAL on, a flush stores its Parquet file and then writes the WAL
checkpoint that stops recovery from replaying those rows. A crash between the
two (a kill, an OOM kill, a power loss) left the file in storage with its WAL
entries unchecked, so the restart replayed them into a second file. Every row
of that flush was stored twice. On a tagged measurement, compaction removes
the second copy; on a tagless one it stays. In crash tests with
frequent flushes, about one SIGKILL in five did this. No acknowledged row was
lost.

Before storing a file, a flush now writes a small record into the WAL. It
names the file, its size, and the WAL entries the file holds. At recovery, when
an entry has no checkpoint but such a record names it, and the file is in
storage at that size, Arc does not replay the entry. It writes the missing
checkpoint and keeps the file as the stored copy, re-registering its field
schema from the file's footer and, with tiering, its tiering metadata. Arc treats the file as all or nothing: if any
of its entries was already checkpointed, it replays the rest, as before.

The record costs almost nothing. It forces no disk sync of its own (the
flush's checkpoint, written after it, syncs it). A flush waits at most 100 ms
for room in the WAL queue to write it. Under durable acknowledgements at
100 ms flushes, every flush wrote its record and ingest throughput was
unchanged. A record that never reaches disk only means the replay there
always was. Arc skips the record when it finds no room in the queue in time,
or when it would be over 1 MiB.

Not in a cluster: there a stored file also needs its manifest registration,
which the crash may have cut short. A cluster node writes no records and
replays, as before.

Old binaries read these records as checkpoints that match nothing, so
downgrading is safe.

New counters:

- `arc_wal_flush_intents_written_total`
- `arc_wal_flush_intents_dropped_total`
- `arc_wal_flush_intents_adopted_total`

## Fix: a damaged WAL entry no longer holds back replay of newer WAL files ([#1634](https://github.com/Basekick-Labs/arc/issues/1634))

When one WAL entry was damaged (a checksum mismatch, for example from bit rot
or a torn sector), recovery kept that file and stopped there. Every newer WAL
file stayed unreplayed until the damaged file was quarantined after three
failed passes: startup plus two periodic passes, about 10 minutes at the
default `wal.recovery_interval_seconds`. Acknowledged rows from those newer
files were missing from queries in the meantime, and the WAL directory kept
growing. Nothing was lost, but the only sign was a field on a log line.

A WAL file whose only problem is damaged entries is now quarantined on the
first pass, in this order:

1. The entries read intact are replayed.
2. They are flushed by the same barrier used before a recovered file is
   deleted.
3. The file is renamed to `.wal.failed` and kept for inspection.

Recovery then replays the newer files in the same pass. The damaged entries
cannot be replayed, and neither can any entries the damage made unreadable.

This applies only to checksum mismatches, which are bytes on disk.

Files that fail for any other reason keep the three-pass rule, and recovery
still stops at them:

- **A replay that fails**, such as a refused buffer or unreachable storage.
  The cause may be transient, and continuing would charge every newer file
  with the same failure.
- **An intact entry this build cannot decode.** A later build may be able
  to decode it.
- **Any other damage**: a read error, a length past the limit, or a frame
  that does not parse. A read error can be transient, and after a broken
  length field the reader cannot find the next entry.
- **A file under the cluster registration hold.** It is struck instead, and
  no newer file of the pass is deleted.

New counter: `arc_wal_damaged_entries_total`, the damaged entries in files
quarantined on the first pass. `arc_wal_quarantined_files_total` counts these files as
well.

## Fix: a damaged WAL length field no longer drops the entries after it ([#1641](https://github.com/Basekick-Labs/arc/issues/1641))

Each WAL entry starts with its length. If that field was damaged so it pointed
past the end of the file, the reader took the entry for a write that was torn
by a crash and stopped reading the file there. Recovery replayed the entries
before it and then **deleted the file**. Every later entry in the file was lost
without an error, a quarantine or a metric. Those entries included
acknowledged, unflushed rows and the flush checkpoints that keep entries from
being replayed twice.

The entry's header also stores the checksum of its payload. When only the
length is damaged, the payload is intact and its checksum matches at exactly
the true length. Arc now looks for that length when a length points past the
end of the file or above the entry size limit. It accepts the length only if
what follows confirms it: the end of the file exactly, or a next entry that
fits in the file and passes its own checksum. The repaired entry is then read
like any other. All of the WAL's readers use the same repair, so the replay,
the checkpoint scan and the identity scan agree on where every entry is.

Arc logs a warning for each file with a repaired entry. New counter:
`arc_wal_repaired_length_entries_total`.

These cases are still not repaired:

- **The length and the payload (or checksum) of one entry are both
  damaged,** or the damaged entry is followed directly by a torn write.
  Nothing confirms a length, so the entry still reads as a torn write.
- **A length damaged to be longer but still inside the file.** The entry
  fails its checksum and counts as damaged, so the file is kept or
  quarantined (see #1634 above), never deleted. Repairing it
  would make the replay and the checkpoint scan disagree about where entries
  start, because the checkpoint scan steps over entries by their stated
  length without reading them. An entry could then be replayed twice.
- **A length damaged to be shorter than the payload.** The entry fails its
  checksum, and the reads after it are misaligned.
- **Each file gets one repair attempt.** A second damaged length in the same
  file reads as before.

### WAL length-damage test accepts both safe recovery outcomes ([#1670](https://github.com/Basekick-Labs/arc/issues/1670))

The test now accepts either retention of a damaged WAL at its original path or
intact quarantine. It checks that the bytes are preserved and the checkpointed
row is not replayed.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1674](https://github.com/Basekick-Labs/arc/pull/1674).

## Tests: two intermittent CI failures fixed ([#1671](https://github.com/Basekick-Labs/arc/issues/1671), [#1672](https://github.com/Basekick-Labs/arc/issues/1672))

Test-only; no product behaviour changes.

- `TestASyncDoesNotAnswerACheckpointWrittenAfterItStarted` could hang
  `internal/wal` until the 30-minute package timeout. It read its entry count
  after an append the writer loop may already have counted, then waited for
  two more that never came, and the failure's cleanup blocked on a sync the
  test still held. The count is read before the append, held syncs are
  released before the writer closes, and every wait is bounded.
- `TestSlowStandbyOnlyLosesItsOwn` reported gaps on the healthy standby on a
  loaded runner. The server keeps each standby's queue separate; the test's
  fixed-rate producer let a slowed healthy standby overflow its own queue.
  The producer now stays within 32 entries of what the healthy standby has
  stored, so a gap can only come from the stuck one.

## Fix: a compressed MessagePack write over the size limit answers 413 ([#1651](https://github.com/Basekick-Labs/arc/pull/1651))

A gzip or zstd MessagePack body that fits `server.max_payload_size` but
decompresses past it was answered `400`, the status for a malformed request.
A client that splits a batch on `413` (the Node-RED node, [node-red-arc#3](https://github.com/Basekick-Labs/node-red-arc/issues/3))
therefore failed the whole batch instead of splitting it. It now answers `413`,
as an oversized uncompressed body already did:

```
413 {"error":"Payload too large: decompressed payload exceeds 4MB limit. Consider batching into smaller requests."}
```

The message keeps the text the `400` carried (`decompressed payload exceeds
<limit> limit`); its `Invalid gzip compression:` / `Invalid zstd compression:`
prefix is gone. Under a limit below 32 MB, a zstd frame whose window is over
the limit answers `413` too (`the zstd frame's 2MB window exceeds the 1MB
limit`): `zstd -3`, and Node's `zstdCompressSync`, give a large batch a 2 MB
window, and a smaller batch gets a smaller one. A corrupt body, and a frame
whose window is over 32 MB (`--ultra`, `--long`), still answer `400`. `GET /api/v1/write/msgpack/spec` says so. Line
Protocol and TLE writes are unchanged.

Contributed by [@efegokdemir](https://github.com/efegokdemir).

## Fix: a flush write that times out but lands no longer duplicates rows after WAL replay ([#1635](https://github.com/Basekick-Labs/arc/issues/1635))

With the WAL on and object storage as the backend, a flush whose upload hit
`ingest.flush_timeout_seconds` was treated as failed: its rows stayed in the
WAL and the next recovery pass wrote them to a new file. An object store that
had already received the upload could still finish it after Arc gave up (a
stalled S3, a slow network, an overloaded gateway). That left two files with
the same rows, and queries read both. Acknowledged rows were never lost.

Arc now keeps the key of a failed flush write together with the WAL entries
whose rows it held. When another flush has stored all of those rows, usually
the recovery pass's replay, Arc checks the key. If the object is there and is
the size the failed upload sent, Arc deletes it and logs a warning. It checks
again 1, 5 and 15 minutes later, because the abandoned upload can land after
the first check.

Arc deletes the object only when every row in it is provably stored in
another file. A flush that does not meet this condition is not tracked, and a
duplicate is still possible, as before:

- The WAL is off.
- Some of its rows have no WAL copy.
- It names rows by identities that replay does not carry.

Other limits:

- **Restart.** The list of failed keys is kept in memory. After a restart,
  the startup replay leaves a copy that already landed in place. On shutdown,
  Arc logs a warning for each key it is still tracking.
- **Compaction.** If compaction merges the late copy before Arc deletes it,
  the duplicate rows become part of the compacted file.
- **Brief duplicates.** Between the late upload landing and the check, queries
  see the rows twice.

New counters: `arc_ingest_ambiguous_writes_recorded_total`,
`arc_ingest_ambiguous_writes_removed_total` and
`arc_ingest_ambiguous_writes_abandoned_total` (no longer tracked without a
removal: rows not seen stored again within a day, or an object of another
size at the key). No new configuration.

## New: Iceberg tables with their metadata beside the data, for OneLake and Microsoft Fabric ([#1676](https://github.com/Basekick-Labs/arc/issues/1676))

`iceberg.layout = "colocated"` creates each new exported table in the
measurement's own directory: its metadata goes in
`{database}/{measurement}/metadata/`, and the table's location is the
measurement directory, so every data file it references is inside the table
folder. That is what readers that reach a table only through its folder
need. A Microsoft Fabric OneLake shortcut is one: with the default layout it
converts the table's metadata, then Fabric fails on the first data file,
because the data is outside the shortcut. With the colocated layout the same
files read in place, still zero-copy, and Power BI reports on them through
Direct Lake.

- The default stays `"warehouse"` (`<warehouse>/<prefix>_<database>.db/<measurement>`).
- The layout decides where **new** tables are created. An existing table keeps
  its location, whichever layout is set, so switching either way moves
  nothing. To move a table to the configured layout: stop Arc on **every**
  node, delete the table's row from the `iceberg_tables` table of each node's
  catalog, delete its metadata directory (`<prefix>_<database>.db/<measurement>/`
  when moving to colocated, `<database>/<measurement>/metadata/` when moving
  back), and start Arc again. The next pass recreates the table in the
  configured layout and registers every file again, reading each file's
  footer once. A node left with a row whose metadata is gone keeps failing
  that table until its row is deleted.
- Every node exporting to the same bucket must use the same layout: two nodes
  with no row and different layouts would each create the table in their own
  layout's directory.
- A node with no catalog row for a table adopts it under whichever layout it
  was created, rather than creating a second one.
- `colocated` needs local or S3 storage (the backends the export supports)
  and the warehouse at the storage root (the default); an `iceberg.warehouse`
  pointing elsewhere is refused at startup.
- A database `DELETE` leaves a colocated table's metadata to the Iceberg
  cleanup, which writes the drop markers first and removes it after
  ([#1611](https://github.com/Basekick-Labs/arc/issues/1611) still holds).
  On a hub, deleting a spoke namespace also removes its spoke tables' colocated
  metadata and catalog rows.
- Metadata restored from a backup of another storage root (another bucket,
  prefix or path) is not adopted: Arc logs the directory to delete, and the
  export recreates the table.
- A measurement's `metadata/` directory is not data: query pruning and the
  field-schema registry set it aside.
- **Do not run orphan-file cleanup** (`remove_orphan_files` in Spark or Trino)
  against a colocated table. Its location holds Arc's data, including files
  the export has not registered yet, and that cleanup deletes every
  unreferenced file under the location.
- **Caveats:**
  - Files that move to a cold tier leave the table folder, so folder-scoped
    readers cannot read them (catalog readers can). Arc warns at startup.
  - On a cluster with node-local storage the metadata exists only on the
    exporting node. Arc warns at startup.
  - On object storage, a measurement whose data is all gone but whose table
    metadata remains is still listed.

## Fix: local storage fsyncs what it writes, so a power loss cannot take acknowledged rows ([#1618](https://github.com/Basekick-Labs/arc/issues/1618))

The local storage backend renamed each written file into place without
fsyncing it or its directory. With the WAL on, a flush checkpoints the WAL
as soon as the file is written, and recovery skips checkpointed entries. A
power loss or kernel crash before the kernel wrote the file back therefore
lost rows that had been acknowledged as `durable`. A process crash (`kill -9`)
was never affected, because the page cache survives it. Compaction and the
DELETE rewrite had the same gap: both remove their source files once the new
file is written, so a power loss could leave neither.

Every write to local storage now fsyncs the file before it gets its final
name, and fsyncs the directory after. This covers ingest flushes, compaction
output, the DELETE rewrite, tiering's cold-to-hot copies, cluster peer
fetches, backup and restore, and Iceberg metadata. New partition directories
are synced into their parents. Object storage (S3, Azure, GCS) is unchanged:
an object is durable once its upload returns.

Behaviour on failure:

- **A failed file fsync fails the write.** Nothing is published. A
  `.part` staging file is removed rather than resumed, because after a failed
  fsync its bytes cannot be trusted.
- **A failed directory fsync after the rename also fails the write**, and
  the file stays in place: its bytes are already on disk, only its name could
  be lost. With the WAL on, the flush's rows replay, so they can be read twice
  until compaction removes the duplicates. A duplicate, never a loss.
- **The exception is a copy into a new key** (compaction output, tiering and
  peer copies). The copied file is removed, because the caller still has the
  source and retries.
- **Directory fsync refused outright.** Volumes that refuse directory fsync
  (`EINVAL`, `ENOTSUP` or `ENOSYS`, e.g. some FUSE and CIFS mounts) are
  accepted, with one warning.

**Cost.** Each flushed file costs two more fsyncs on the flush worker. On
Linux the cost is unmeasured so far. Measured on an M3 Max, where Go's
`File.Sync` is `F_FULLFSYNC` (the worst case), with the sustained MessagePack
bench (`arc.toml`, 12 workers, alternating runs):

| Configuration | Before | After |
|---|---|---|
| WAL off | 34.3M rec/s | 34.0M rec/s (noise) |
| WAL on, `ack_mode = "queued"` | 32.0M rec/s | 32.0M rec/s (noise) |
| WAL on, `ack_mode = "durable"` | 1.22M rec/s | 0.90M rec/s (−26%) |

The cost scales with files written per second, not rows.

- **Many measurements flushed by age.** Small buffers flushed often pay the
  most. With 1,000 measurements each receiving one row a second, the aged
  sweep (every 5 s, one buffer after another) took about 30 s to flush them
  all on macOS instead of about 2 s. Buffers wait longer and files get larger.
  Ingest requests were not slowed (p50 about 20 ms either way), and no rows
  were lost.
- **Restore and tiering.** Copies of many files (backup restore, tiering
  cold-to-hot) are slower by two fsyncs per file.

## Fix: a tiering policy needs an existing database; an Iceberg warehouse inside the storage root is not a database ([#1626](https://github.com/Basekick-Labs/arc/issues/1626))

**Behaviour change: `PUT /api/v1/tiering/policies/{database}` now answers
404 `Database '<name>' not found` for a database that does not exist.** It
used to store the policy. A database exists by the rule `GET
/api/v1/databases/{name}` answers with: its marker, a file, a cold-tier row
or (in a cluster) a manifest entry. If you set policies before the first
write, create the database first with `POST /api/v1/databases`. The PUT also
answers:

- **400** `invalid database name "<name>"` for a name that is not a valid
  storage segment (before: stored);
- **400** with the name-spelling text for a name that exists in another
  spelling on a volume that folds case ([#1509](https://github.com/Basekick-Labs/arc/issues/1509));
- **500** `Failed to check database` when the check itself fails.

`GET`, `DELETE` and `/effective` are unchanged: `DELETE` still removes a
policy left behind by a deleted database. The check runs on the node that
answers: on a cluster that keeps data per node, a database that so far holds
only the marker made by `POST /api/v1/databases` exists only on the writer, so
set its policy on the writer, or after the first write.

With Iceberg export on, the warehouse's directories in the storage root are
no longer treated as databases: the namespace directories (`arc_<db>.db`), and
a warehouse set to a subdirectory of the storage root
(`iceberg.warehouse=<root>/lv_wh`, on local storage or an S3 prefix).
`GET /api/v1/databases`, `SHOW DATABASES`, `SHOW TABLES` and
`GET /api/v1/measurements` leave them out; `GET /api/v1/databases/{name}` and
its `/measurements` answer 404, `DELETE` answers 403, and
`POST /api/v1/databases` answers 400 `Database name '<name>' is reserved`. A
write to a database named after the subdirectory warehouse (line protocol,
MessagePack, TLE, every `/api/v1/import/*` format, a continuous query's
database) is refused with 400 `database name "<name>" is reserved for the
Iceberg warehouse`, so writes cannot land inside a subdirectory warehouse.
Creating or updating an MQTT subscription whose database or `topic_mapping`
target is that name is refused the same way; an incoming MQTT message that
still maps to it is dropped, logged and counted as a failed message. A directory whose name only
starts with the warehouse's (`lv_wh-other`) is an ordinary database. A
warehouse nested deeper (`<root>/a/b`) hides measurement `b` of database `a`,
and `a` stays listed; `DELETE` of database `a` answers 403, because it would
delete the warehouse with it. Writes to measurement `b` of database `a` are
**not** refused: they land inside the warehouse directory, are hidden from the
listings and are not exported, so do not write there (a startup warning names
that measurement). The export walk applies the same rule, so it no longer walks a
subdirectory warehouse as a database. Configure Iceberg the same way on every
node: a node with Iceberg off lists the warehouse and accepts writes to it.

`/api/v1/import/csv` and `/api/v1/import/parquet` now honour their own
refusals of an invalid database or measurement name and of an RBAC denial:
they answer the 400 or 403 they wrote, where they used to carry on with an
empty database name and answer a later 500 or 422.

Cost: one existence check per policy PUT (an admin call); an in-memory name
comparison per listed directory; one atomic load and string comparison per
write request (per message for MQTT), never per row.

## Fix: re-running a continuous query with nothing new to process answers 200 ([#1628](https://github.com/Basekick-Labs/arc/issues/1628))

`POST /api/v1/continuous_queries/{id}/execute` answered `400 start_time must be
before end_time` when the window it derived was empty: a second run within the
same second, or a run within a minute of a watermark an earlier `end_time` set
ahead of the clock. The scheduler logged the same case at Error as "Scheduled
CQ execution failed".
- Such a run now answers 200 with `records_written: 0`, and `start_time` and
  `end_time` both at the watermark. Nothing runs: `execution_id` is `""`
  (no execution history row is written for it), `watermark_status` is absent,
  and `last_processed_time` is left as it is. In a cluster it proposes no
  watermark of its own. The scheduler logs it at Debug.
- A watermark more than a minute ahead of the clock is still refused (the same
  400, and Error in the scheduler), so a CQ stuck behind clock skew stays
  visible.
- An explicit `start_time` not before `end_time`, or an explicit `end_time`
  before the watermark, is still a 400 with the same text.
- Python SDK: such a re-run returns a normal result instead of raising
  `ArcError`.

## Fix: an MQTT start or restart that cannot connect to the broker answers 502 with the reason ([#1619](https://github.com/Basekick-Labs/arc/issues/1619))

`POST /api/v1/mqtt/subscriptions/{id}/start` and `.../restart` answered 500
`"Failed to start subscription"` when the broker could not be reached, and the
reason was only in the subscription's `error_message`. They now answer
**502** with the reason in `error`, the same text as `error_message`:

- **`could not connect to the broker: ...`** when the broker cannot be reached:
  DNS, a refused or reset connection, a TLS handshake failure (a wrong CA
  included), or `connection timeout after N seconds`.
- **`the broker refused the connection: ...`** when the broker answered and
  refused it (`bad user name or password`, `not Authorized`, and the other
  CONNACK refusals).

The text names at most the dialled host and port, never the broker URL or its
credentials. The subscription is left with status `error` and is not running,
also after a restart, which stopped the old connection first; stop still
answers 409. Other start failures answer as before (404, 409 or 500).

The stored `error_message` for these failures reads the same, where it read
`connection failed: ...` or `connection timeout after N seconds`; this
includes the create response with `auto_start`.

A start that timed out also left the MQTT client connecting in the
background. A broker that answered late (paho retries once with MQTT 3.1, with
a fresh handshake deadline) then had a client subscribed and ingesting for a
subscription stored as `error`, which stop could not reach. A failed connect
now disconnects the client, and a connection that still completes is refused
without subscribing and without setting `arc_mqtt_connected` to 1. A
reconnect after writes resume (a suspended subscription) that fails is
disconnected the same way.

The "Connecting to MQTT broker" and "Created MQTT subscription" log lines no
longer carry the broker URL's password or query values: they read `REDACTED`,
and a URL that cannot be split safely is logged as `(unparsable broker URL)`.
Creating or updating a subscription with a broker URL that does not parse now
answers 400 `not a valid URL` instead of quoting the URL, password included.

## Fix: a tiering policy PUT reports the stored row ([#1619](https://github.com/Basekick-Labs/arc/issues/1619))

`PUT /api/v1/tiering/policies/{database}` answered `"id": 0` and zero
`created_at`/`updated_at`, on a standalone node and on a cluster. On a
standalone node a `GET` of the policy did the same until a restart, because
the policy store cached the request instead of the stored row. The PUT
response and the `GET` now carry the stored id and timestamps, and a second
PUT keeps the id and `created_at`. On a standalone node the write reads them
back in the same statement (no extra query); on a cluster they come from the
cluster's copy of the policy.

## Fix: removing a node that is not a member answers 404 ([#1619](https://github.com/Basekick-Labs/arc/issues/1619))

`DELETE /api/v1/cluster/nodes/:id` answered 200 "node removed from cluster"
for any ID, including a typo, after proposing a removal that changed nothing.
It now answers **404** `{"success":false,"error":"node <id> is not a member of
the cluster"}` and proposes nothing.

A node still counts as a member, and is removed as before, when it is in any
of: the node registry, the cluster state, or the Raft configuration (a stale
voter missing from the other two is exactly the dead node the endpoint is
for). It also counts when the cluster state still holds something for it: a
writer that left gracefully (listed under `departed` by
`GET /api/v1/cluster/stream-claims`) and whose instances and claims the
removal gives up, or a staged compaction, cold-mark entry or designation that
names it.

- On a node that is not the Raft leader the answer is unchanged: 500 with
  "not the leader", checked before membership.
- A 404 on a retry means the earlier removal took effect. A removed node that
  still holds the compactor lease, or a compaction-pause acknowledgement,
  keeps answering 200 until the lease moves or the pause ends.
- Membership is read from the leader's registry, so a stale entry that only a
  follower's registry holds (left by an earlier leader whose cluster-state
  update failed) is no longer cleared by this call: it answers 404.
- Self-removal is still a 400.
- With clustering on but Raft not configured, the call still only unregisters
  the node locally and answers 200 for any ID, as before.

## Fix: an MQTT subscription created with `auto_start` starts at once ([#1585](https://github.com/Basekick-Labs/arc/issues/1585))

`auto_start` is documented as "start on creation and on server restart", but
only the restart half ran. `POST /api/v1/mqtt/subscriptions` with
`"auto_start": true` answered `"status": "stopped"`, and nothing was ingested
until `POST /api/v1/mqtt/subscriptions/{id}/start` or a restart.

The create now starts the subscription and answers with its status:

- **`running`** once connected. While writes are refused on the node (fenced,
  WAL disk pressure and the other suspend reasons) it starts suspended, as
  `POST .../start` does, and the status still reads `running`.
- **`error`**, with the reason in `error_message`, when it cannot connect. The
  subscription is still created (201): fix the broker or credentials, then
  `POST .../start`.

The create waits for the connection, up to `connect_timeout_seconds`
(default 30). A client or proxy that gives up sooner sees a failure, although
the subscription was created: list the subscriptions before creating it again.
Without `auto_start` nothing changes.

## Upgrade note: continuous queries stay as configured in an existing `arc.toml`

26.09.4 turned continuous queries on in the packaged `arc.toml`. From this
release a package upgrade no longer replaces an existing `/etc/arc/arc.toml`
([#1465](https://github.com/Basekick-Labs/arc/issues/1465)), so an install that
already has the file keeps its setting. To turn continuous queries on there,
set `[continuous_query] enabled = true` (or `ARC_CONTINUOUS_QUERY_ENABLED=true`)
and restart.

## Fix: every read sent to a compactor is answered by a reader, and `/api/v1/queries` covers what it forwarded

A compactor answered several reads itself, from its own storage and field
schema, instead of forwarding them as it forwards `/api/v1/query`. It now
forwards them all:

- `POST /api/v1/query/estimate`
- `GET /api/v1/query/:measurement`
- `GET /api/v1/measurements`
- `GET /api/v1/databases/:database/measurements/:measurement/schema`
- `GET /api/v1/databases`, `GET /api/v1/databases/:name` and
  `GET /api/v1/databases/:name/measurements`

Forwarding rules are the same as for Arrow queries (next section). The
target must advertise `forward_read_queries_v1`. While nodes exist but none
does, the compactor answers itself; with no reader or writer at all it
answers 503. Forwarded estimates and `GET /api/v1/query/:measurement` are not
cut by `cluster.route_timeout`. The listings are bounded at 30 s, as they are
locally.

**`/api/v1/queries` on a compactor** (licensed query management) now lists
the queries it forwarded:

- `/api/v1/query`, `/query/msgpack`, `/query/arrow`, estimates and
  `GET /api/v1/query/:measurement`.
- Each entry carries `forwarded_to`, the node it was sent to.
- `DELETE /api/v1/queries/:id` cancels the forward. That stops the query on the
  answering node when it cancels queries on client disconnect
  (`query.cancel_on_client_disconnect`, on by default).
- The entry's ID is the compactor's. A forwarded response's `X-Arc-Query-ID`
  header carries that ID, so it can be cancelled on the compactor. The
  answering node's ID is in `X-Arc-Forwarded-Query-ID`.
- History records whether the forward succeeded, not a row count.
- A client that disconnects from a compactor now also cancels its forwarded
  read, as it cancels a local one, with or without query management.

`POST /api/v1/databases` (create) is a write; see the next section.

## Fix: a database created on a compactor or reader is created by the writer

`POST /api/v1/databases` ran on whichever node received it. On a compactor or
reader, the database's marker was written to that node's storage, and the
"already exists" check read that node's storage too. On shared object storage
that was visible to every node anyway. On a cluster where each node has its
own storage, the database was created somewhere its writes never land.

A node that does not ingest now forwards the create to the writer, as it does
every other write: the same writer a write would go to (the primary, when
there is one), sent once and never retried. The writer validates the request
and checks the caller's admin permission itself. The response, including
`409` for a database that already exists, is the writer's.

- With no healthy writer, the answer is `503` ("No writer node available in
  the cluster"); with a writer that cannot be reached, `503` ("Database not
  created: the writer could not be reached; retry"). Nothing is created in
  either case. Before, the database was created locally. A writer that takes
  the create but whose answer is lost gives `504`; a retry answers `409` if
  the database was created.
- **Failover.** While a designated primary writer is unreachable, the create
  answers `503` instead of going to another writer, which may be a standby.
  Retry once the primary is back or a new one is promoted.
- **Rolling upgrades.** A compactor or reader forwards the create only to a
  writer that advertises it at join (`forward_database_create_v1`). With a
  writer that has not been upgraded yet, the database is created locally, as
  before, and the node logs a warning at most once a minute.
- A compactor or reader with authentication disabled, in front of a writer
  with it enabled, now gets `401` for a create, as it already did for writes.
- On a cluster where each node has its own storage, an empty database exists
  only on the writer until data arrives, as before: database markers are not
  replicated, so a listing answered by a reader does not show it yet.
- `DELETE /api/v1/databases/:name` is unchanged.
- A create sent directly to a standby writer is still made by that writer
  rather than forwarded to the primary.

## Fix: Arrow queries sent to a compactor are answered by a reader ([#1567](https://github.com/Basekick-Labs/arc/issues/1567))

A node that cannot serve queries (a compactor) forwarded `POST /api/v1/query`
and `/api/v1/query/msgpack` to a reader, but ran `POST /api/v1/query/arrow`
itself. The Arrow endpoint now forwards too, so query load stays off
compactors whichever endpoint a client uses.

A forwarded Arrow result is streamed, not buffered, and keeps its trailers:

- `Arc-Stream-Truncated`
- `Arc-Rows-Capped`
- `Arc-Execution-Time-Ms`

So a client sees a truncated or capped result as such, as it does when it asks
the reader directly. `cluster.route_timeout` does not apply to the stream:
that 5-second default would cut any longer result. The answering node bounds
the query, with its query timeout and any governance limit on the token. The
compactor only bounds the wait for the answer to start, by its own query
timeout plus 30 s.

If the answering node fails partway through, the forwarded response ends as a
broken transfer (the connection closes before the end of the body), not as a
short result that looks complete.

**Rolling upgrades.** A compactor forwards an Arrow query only to nodes that
advertise the new `forward_read_queries_v1` capability: readers, or writers
when the cluster has no readers. While nodes exist but none advertises it, it
runs the query itself, as before. With no reader or writer at all it answers
503, as `/api/v1/query` does.

A routing error no longer includes the peer's internal address in the
response body. A failed forward now answers "Failed to route request to
cluster node"; "no reader/writer available" keeps its own message.

The other read endpoints are forwarded too; see the previous section.

## Fix: the backup listing says which skipped files were not missing data ([#1555](https://github.com/Basekick-Labs/arc/issues/1555))

`GET /api/v1/backup` now carries `skipped_reconciled` and
`cluster_manifest_checked`, which until now only the full manifest
(`GET /api/v1/backup/:id`) had. `skipped_reconciled` is the part of
`skipped_files` whose file had left the cluster manifest by the end of the run
(compaction, retention or tiering removed it), so it is not missing data.
Without it a client had to count every skipped file in the listing as a gap,
while the same backup's manifest said otherwise. A client checking completeness
from the listing now counts `skipped_files - skipped_reconciled`, as a
replace-mode restore already did.

Both fields are omitted when zero or false, so a standalone backup's listing
entry is unchanged. `cluster_manifest_checked` is never sent as `false`: it is
absent on a backup that did not cross-check against the cluster manifest. On a
multi-target run whose listing could read only some targets, `skipped_files`
and `skipped_reconciled` cover the targets it read, the same as
`GET /api/v1/backup/:id`.

## Fix: forwarded responses keep every value of a repeated header ([#1556](https://github.com/Basekick-Labs/arc/pull/1556))

When a cluster node forwards a request to another node, the response
returned to the client kept only the last value of an end-to-end header that
appeared more than once (`Vary`, `Link`, `WWW-Authenticate`, …). Every value
now reaches the client, in order. `Set-Cookie` was not affected. A header
both nodes set, such as `Access-Control-Allow-Origin`, is still sent once,
with the answering node's value.

Thanks to [@lecodev-26](https://github.com/lecodev-26) for the fix.
## Fix: a token limited to some databases can list them ([#1562](https://github.com/Basekick-Labs/arc/issues/1562))

Listing every database required a read grant covering every database, so a
token whose RBAC role names specific databases got 403 from
`GET /api/v1/databases`, from `SHOW DATABASES`, and from
`GET /api/v1/measurements` without `?database=`, even though it could query
its databases. (2026.09.3 introduced that bar when these listings gained RBAC.)

These listings now show each caller the databases it can read something in:

- **What a scoped token sees:** a token limited to some databases sees those
  databases and no others. A token whose role is limited to some measurements
  sees every database its role's database pattern matches, as
  `GET /api/v1/databases/:name` already answered for each of them; a database
  where it can read none of the measurements shows `measurement_count` 0.
- **Counts:** in `GET /api/v1/databases`, `measurement_count` counts only the
  measurements the caller can read, as `GET /api/v1/databases/:name` already
  did.
- **Unchanged for full access:** a token that can read every database, a token
  that is in no team, and any request with auth disabled see the full list,
  as before.
- **No read grant anywhere:** still 403.
- **Grants unreadable:** if the permission store cannot be read, the listings
  answer 503 instead of an empty list.
- **Other endpoints:** `SHOW DATABASES` sent to `/api/v1/query/estimate` or
  `/api/v1/query/arrow` answers 400 "use /api/v1/query" for every caller.
  Before, a token without the all-databases grant got 403 there.

## Fix: edge-sync pushes are decided by RBAC on the spoke's database ([#1561](https://github.com/Basekick-Labs/arc/issues/1561))

The edge-sync hub endpoints (`POST /api/v1/sync/file` and
`/api/v1/sync/reconcile`) checked only the token's own `write` permission and
no RBAC grant. A push also needs the spoke's HMAC secret, and a pushed file
always lands in that spoke's namespace, which queries read as database
`{spoke_id}` with the spoke's own database as the measurement
(`FROM "rocket-01".metrics`). So a token in an RBAC team was not limited by
its team's grants there, unlike on every other ingest endpoint.

Both endpoints now check the token's `write` grant on that pair, after the
request's signature is verified and before anything is stored:

- **`/file`:** a push the token may not make is refused with 403 and
  `"reason":"permission_denied"`, and nothing is stored.
- **`/reconcile`:** a file the token may not write is reported as missing,
  without looking up whether the hub holds it. The other files in the batch are
  answered as usual, so a spoke with one ungranted measurement keeps syncing
  the rest.
- **Admission:** the endpoints admit a token whose access comes only from RBAC,
  as the other ingest endpoints do since #1559.
- **Tokens in no team:** unchanged. Their own `write` permission decides, as
  before.
- **Root-level spoke files:** a file at the root of a spoke's storage
  (`x.parquet`) is in no measurement, so it needs `write` on the whole
  database `{spoke_id}`. A grant on one measurement is not enough.

**What operators may see.** A spoke whose token is in a team without a
`write` grant on its namespace now gets 403 on those files. Each 403 counts
against the file's attempts (five by default), after which the spoke stops
offering it. Grant the team `write` on database `{spoke_id}` (or the spoke's
database as the measurement), then requeue the files with
`POST /api/v1/spoke-sync/ledger/requeue` on the spoke. Giving the spoke a
token that is in no team also works.

If the hub cannot read the permission store, the endpoints answer 503
`hub_unavailable`. A spoke treats that like any other hub-side failure.

## Fix: tokens whose access comes from RBAC can query and ingest ([#1559](https://github.com/Basekick-Labs/arc/issues/1559))

A token with no permissions of its own, given access through an RBAC team
(`POST /api/v1/auth/tokens` with `permissions: []`), was refused with 403 by
the query and ingest endpoints, even though RBAC granted the access,
`GET /api/v1/databases` accepted it, and
`GET /api/v1/auth/tokens/:id/permissions` reported the grant. These
endpoints first checked the token's own `read` or `write` permission and did
not ask RBAC. A token could therefore only be limited by RBAC if it also held
that permission.

The endpoints now admit a token that holds the permission anywhere,
through RBAC or its own permissions. Each endpoint then checks every table a
query reads and every measurement a write names, as before:

- `POST /api/v1/query`, `/api/v1/query/msgpack`, `/api/v1/query/arrow` and
  `/api/v1/query/estimate`
- `GET /api/v1/measurements`, `GET /api/v1/query/:measurement` and
  `GET /api/v1/databases/:database/measurements/:measurement/schema`
- `POST /write`, `/api/v2/write`, `/api/v1/write/line-protocol`,
  `/api/v1/write/msgpack` and `/api/v1/write/tle`

Tokens that are in no team are unchanged: their own permissions decide, as
they always have.

**Behaviour change for team members.** A token in a team is decided by RBAC
on these endpoints, as it already was for every table and measurement. A
token that holds `read` itself but whose teams grant `read` on nothing is now
refused before the query runs. Before, it got through, every table it named
was refused, and only a query with no table (`SELECT 1`) succeeded. The same
applies to `write`. A health check that runs `SELECT 1` with such a token
gets 403: grant its team `read`, or use a token that is in no team.

**When the permission store cannot be read** (the SQLite database holding
tokens and RBAC grants), these endpoints now answer 503 "Permission store
unavailable" once a token's cached grants expire, for every token except
admin tokens. Before, a token that is in no team kept passing this first
check, because it never read the store. The store is not bypassed for such
a token, because it might have been added to a team.

**Rolling upgrades.** A request forwarded to a node that has not been
upgraded yet is checked by that node the old way, so an RBAC-only token can
see a 403 until every node runs this version.

The edge-sync hub endpoints moved to RBAC as well; see
"Fix: edge-sync pushes are decided by RBAC on the spoke's database"
([#1561](https://github.com/Basekick-Labs/arc/issues/1561)).

## Fix: recent-data queries keep working on cluster nodes while the cold tier is unreachable ([#1552](https://github.com/Basekick-Labs/arc/issues/1552), part 2 of 2)

The cluster counterpart of #1550. On a cluster node, a read whose cold tier listing fails now answers from hot data when the cluster's cold mark (part 1, below) shows the read cannot need cold data:

- the mark is trusted: enabled, fenced, seeded from this node's own cold store, and every node and Raft server confirmed it within 15 minutes;
- the read states a start (`time >= ...`), and that start is on a UTC day after the measurement's mark, or the measurement was never marked;
- this node is caught up with the leader: a Raft read barrier (at most 2 seconds), after which the trust and the mark are read again;
- every cold partition the read kept is one whose listing failed, as in #1550. Only that site drops cold partitions: a read the pruner could not prune (no time range, only an end, pruning off, or a range over the path cap) keeps them.

Otherwise the read fails as it did; on a cluster node that includes every read with no stated start. Standalone nodes keep #1550's rule.

Also on cluster nodes: a measurement this node holds no cold-tier rows for used to be read from hot only, silently leaving out cold data the cluster had (its rows lag the cold store until its next cold-tier metadata sync). The read now includes the cold tier when the trusted mark says the range may hold cold data and the mark was raised within the last 48 hours: two default migration cycles, the window in which this node's rows may not know of a recent cold write. Outside that window, or with an untrusted mark, reads are as before.

Admins can reset the mark's seed with `POST /api/v1/cluster/cold-mark/reset` (admin token), for example after fixing a node whose cold tier pointed at another store. Fix every node's cold tier config first: the first complete cold-tier metadata sync after the reset, on any node whose tiering is not role-gated, seeds it again from that node's store, and reads do not use the mark until then. A reset on a cluster that never enabled the mark answers 409.

Observability: `arc_query_cold_mark_skipped_total` counts reads answered this way, with a `Warn` at most once a minute per measurement; `GET /api/v1/cluster` shows `cold_mark.trusted` and why not.

Performance, healthy reads (cold listing succeeds): on a cluster node, one cached trust check (refreshed at most every 5 seconds per process, by one caller; it reads the Raft configuration) and one in-memory mark lookup per tiered read. A measurement with no local cold rows reads cold, uncached, only within 48 hours of a raise of its mark that its range reaches. Standalone and non-tiered reads run none of this.

Performance, outage path (a cold listing failed): only a read the mark could drop pays a Raft read barrier, and concurrent reads share one: at most one barrier per node is in flight, the next one starting when it ends (about one per 2-second window under load, not one per read). That rate stays far below the 256 recent barrier tokens Raft keeps, so a node's own barrier token cannot be evicted before it sees it.

## Cluster: a replicated record of which days may be in the cold tier ([#1552](https://github.com/Basekick-Labs/arc/issues/1552), part 1 of 2)

Groundwork for answering recent-data queries on cluster nodes while the cold tier cannot be listed (the standalone case is #1550). This release changes no query behaviour.

The cluster now keeps, through Raft, a **cold mark** per database and measurement: the latest day that may have data in the cold tier.

- The migrator, and a restore that writes to the cold tier, raise it before writing the object: one synchronous Raft round trip before the copy, only the first time a measurement gets a newer day. Once every node runs this release, a cold write whose mark cannot be raised (no Raft quorum) does not start; the file is migrated on a later cycle, and a restore fails. Before that, cold writes run as they do today and propose nothing.
- It never goes down. Retention, DELETE and policy changes leave it where it is.
- Existing cold data is folded in from a complete cold listing, taken after every node has confirmed it has no unmarked cold write in flight.
- It turns on by itself once every node runs this release. From then on a node on an older release cannot join, as with the other cluster capability flags.
- Every node confirms every 5 minutes that it maintains the mark. The mark counts as trusted only while every node and every Raft server has confirmed within the last 15 minutes, so a member that comes back on an older release, which would write to the cold tier without raising it, makes it untrusted until it is upgraded or removed. That leaves a window of up to 15 minutes in which such a member can write unmarked while the mark is still trusted. A confirmation more than 1 minute in a reader's future counts as missing, so node clocks must agree to within a minute.
- A node trusts the mark only if its own cold tier (backend, endpoint, region or account, bucket and prefix) is the store the mark was seeded from. If two nodes seed it from different stores, it is never trusted again; this release has no way to reset that (part 2 adds one).
- `GET /api/v1/cluster` reports it under `cold_mark` (enabled, fenced, seeded, nodes still to confirm, trusted and why not). It does not name the cold store.

Performance: one small Raft entry the first time a migration or restore writes a newer day for a measurement (at most one per measurement per day in steady state); one small Raft entry per node every 5 minutes for the liveness confirmation; one seed per cluster, in entries of up to 1,000 measurements; state of one integer per measurement and one timestamp per node. Queries are unchanged.

## New: cold tier files deleted outside Arc are reported ([#1551](https://github.com/Basekick-Labs/arc/issues/1551))

A cold tier object deleted outside Arc, for example by a bucket lifecycle or expiry rule, used to show up only as a `Warn` line in each tier scan. Queries over that data still answer `200` with the rows that remain (this is unchanged, #1364), so nothing an operator monitors reported the gap.

At the end of each cold-tier metadata sync, Arc now checks again each cold row whose object the listing did not return: it stats the cold object, re-reads the row and stats the hot copy, so an object written or a row retired while the sync ran is not counted. It reports:

- `arc_tiering_cold_rows_missing_objects`: rows with no cold object and no hot copy. These files cannot be read and nothing in Arc restores them. Alert on this one. Arc also logs an `Error` with a sample of paths.
- `arc_tiering_cold_rows_missing_objects_hot_copy`: rows whose cold object is gone while the hot copy is still read. No data is missing.
- `GET /api/v1/tiering/status` → `cold_missing_objects` (`lost`, `hot_copy`, `unchecked`, `sample`, `checked_at`).
- `/health` → `tiering` with the two counts and the check time, no paths.

Notes:

- Reported on standalone nodes and on the cluster's primary writer only. A reader keeps the cold rows of files the primary deleted ([#1563](https://github.com/Basekick-Labs/arc/issues/1563)), so there it would report every normal DELETE or retention as missing; the fields are absent on readers, and the gauges are 0 (on a demoted primary, from its next sync). A reader promoted by failover can report the rows it inherited until #1563 is fixed.
- A sync that overlaps a cold DELETE, retention run or database delete keeps the previous result rather than count rows that run is about to retire. A DELETE or retention whose row retirement failed (it logs that) leaves rows that are counted as lost until a later run retires them.
- At most 1,000 rows, within 30 seconds, are checked per sync, in no fixed order; the rest are reported as `unchecked`. A check that could not examine every row never lowers the lost count: it keeps the previous result, so an alert clears only once a check has seen every row.
- The fields are absent, and the gauges 0, until the first cold sync after startup. With the default `migration_schedule` that sync runs at startup and then daily.
- Do not apply lifecycle or expiry rules, or deletes outside Arc, to Arc's cold prefix. Remove data through Arc (DELETE, retention, database delete) so its rows are retired with it.

## Fix: recent-data queries keep working while the cold tier is unreachable, on standalone nodes ([#1550](https://github.com/Basekick-Labs/arc/issues/1550))

When the cold tier could not be listed (expired or missing credentials, a
network failure, an unreachable endpoint), a query on any measurement with
data in the cold tier failed, even if its time range only covered hot data.
Arc tried to check the cold tier for every partition in the range and then
read those it could not check, which hit the same error. A cold-tier outage
took down recent-data dashboards along with archive queries.

On a standalone node, Arc now uses its tier metadata when the cold tier cannot
be listed. If no cold file is recorded for the query's time range, the read
leaves the cold tier out and answers from hot data. The query fails as before
in every other case:
- cold files are recorded for the range, so the answer would be incomplete;
- the metadata lookup fails;
- the query has no time range and the measurement has cold data;
- a listing that did answer found cold data the metadata does not record.

Arc only uses this shortcut while its tier metadata is a complete record of
the cold tier:
- **Never on a node configured for a cluster.** This covers
  `cluster.enabled` and `cluster.shared_storage_mode`, including a node whose
  cluster coordinator did not start. Cluster nodes keep the previous
  behaviour ([#1552](https://github.com/Basekick-Labs/arc/issues/1552)).
- **Only after a complete cold-tier metadata sync on this version.** The sync
  must record every cold object against the node's current database. A new,
  lost or moved database starts without that record.
- **Not after a metadata restore,** until the next complete sync. The
  restored rows predate files migrated since the backup.
- **Not during or after a data restore on a node with a cold tier,** until
  the next complete sync. This holds even when the restore succeeds.
- **Not after a sync that could not record some cold objects,** until a later
  complete sync. This includes a `.parquet` object under a measurement with
  cold data whose key Arc cannot parse. A failure that repeats on every sync
  keeps the shortcut off. The warning it logs names sample objects.

A quarantined cold row counts as cold data for the range: its object is still
read on a healthy cold tier, so the read keeps the cold tier and fails.

If a data restore cannot record that the tier metadata may become incomplete
(neither the marker file next to the database nor the database itself can be
written), the restore is refused before it writes anything.

`GET /api/v1/tiering/status` reports the state as
`cold_metadata.authoritative`, with the reason when it is false.

**Upgrading:** the shortcut is available only after the first complete
cold-tier metadata sync on this version. That sync runs with the startup tier
scan. A node whose cold tier is already unreachable when it starts on this
version keeps the previous behaviour until the cold tier is reachable again.

Other changes:
- **Fewer timeouts during an outage.** On a node where the shortcut is
  available, once a cold-tier listing fails, Arc stops listing the cold tier
  for the rest of that read. An unreachable cold endpoint then costs one
  listing timeout (5 s) per tiered measurement in each query, instead of one
  per day in the range. Arc does not remember the failure between queries, so
  each query pays it again. Cluster nodes, nodes whose metadata is not yet
  trusted, and hot-tier listings are unchanged.
- **Visibility.** Each read that leaves the cold tier out this way increments
  `arc_query_cold_unlisted_skipped_total`. Arc also logs a warning at most
  once a minute per measurement.
- **Not cached.** These reads are not cached, so the cold tier is read again
  as soon as it can be listed.

Thanks to [@officialasishkumar](https://github.com/officialasishkumar) for the
first fix of this issue in
[#1553](https://github.com/Basekick-Labs/arc/pull/1553). Its unverified
listing outcome and its 403-backed S3 test shaped this one.

## Fix: names that differ only in case stay apart on case-insensitive volumes ([#1509](https://github.com/Basekick-Labs/arc/issues/1509))

On a volume that does not distinguish names by case (macOS APFS by default,
Docker Desktop bind mounts from a Mac, NTFS), `cpu` and `CPU` were one
directory. A write, query or delete under either name acted on the same
files, while permissions matched names exactly. Arc now checks the local
storage volume at startup. If it folds names, a database or measurement is
only accepted in the spelling it already has on disk:
- **Ingest** to another spelling of an existing database or measurement is
  refused and reported like other unstorable names, before anything reaches
  the WAL. Two first writes racing with different spellings can't both win,
  first writes racing with the same spelling are all accepted, and a write
  refused for its columns or values doesn't claim a spelling.
- **Queries** are refused with a 400 that names the existing spelling.
  Before, `FROM CPU` on a Mac returned `cpu`'s rows. On such a volume,
  rewritten SQL is no longer cached, so a spelling accepted before another
  one existed is checked again on every query.
- **Database get, create and delete** under another spelling are refused
  with a 400 before anything else runs.
- **Measurement deletes, retention and listings** under another spelling
  are refused instead of acting on the existing directory.

Linux filesystems, S3 and Azure are unaffected, and the check is skipped
there. If the startup check itself fails (for example a read-only storage
root), Arc assumes names fold. A startup warning says when case folding was
detected. To keep names that differ only by case, use a case-sensitive
volume.

**Upgrading on macOS:** a retention policy, continuous query or client that
names a database or measurement in a different case from its directory now
fails instead of acting on that directory. Check them after upgrading.
Retention that fails lets disk usage grow.

Known limits:
- Only ingest is checked on the write side. Restores, edge-sync imports,
  cluster file replication and WAL replay write what they were given.
- Several nodes sharing one case-insensitive network volume are not
  coordinated with each other.
- An empty measurement directory removed by a cleanup while its rows are
  still buffered can be re-created in another spelling before those rows
  flush. The rows then land in that directory.
- Data already stored under mixed spellings before the upgrade isn't
  migrated. With tiering, files moved to a case-sensitive cold store under
  the other spelling are not reachable by either name.

## Docs: the cluster trust model is written down ([#1510](https://github.com/Basekick-Labs/arc/issues/1510))

A new `SECURITY.md` explains how to report a vulnerability privately and what
the cluster's shared secret guarantees. Every node holding
`cluster.shared_secret` is a fully trusted member, and a node ID in a peer
message is a claim, not a proven identity. Cluster TLS with a CA proves that
a peer belongs to the cluster, not which node it is.
- **Code comments** that described some peer checks as authenticating the
  sender now call them what they are: consistency checks on the node ID
  the request claims.
- **The startup warning** for cluster TLS without `cluster.tls_ca_file` now
  says that client certificates are not verified and that cluster
  membership, not node identity, rests on the shared secret.
- **A new Info log** appears when a standby's origin-stream session is
  replaced by a connection from a different address. The replacement itself
  is unchanged: the primary cannot tell a live session from a half-open one,
  and a standby that restarts with a new address must still be able to
  reconnect.

Per-node peer identity remains open in #1510.

## Security: Arc's internal SQL placeholder text is reserved

Before its checks, Arc swaps string literals, quoted names and some keywords
for generated placeholders, for example the `FROM` inside
`EXTRACT(YEAR FROM time)`. It restores them by matching their text once the
checks have run. Text a caller wrote that matched a placeholder was restored
too, so the statement that ran could differ from the one that was checked.
- **Queries** are refused with a 400 when `__STR_`, `__IDENT_` or
  `__FROM_MASK_` appears outside a string literal or quoted identifier,
  comments included. The
  rule also catches text that only forms once a literal is masked. Inside a
  literal or quoted name these sequences remain ordinary data. The check
  runs on every query endpoint, including continuous queries, and the FROM
  part runs again inside the storage-path rewrite.
- **DELETE `WHERE` clauses** follow the same rule. Before, they had no such
  check.
- **Database names**, including the `x-arc-database` header, also refuse
  `__FROM_MASK_`, in addition to the existing `__STR_` and `__IDENT_`.
- **The DELETE time pin** used with stream takeover restores each string
  literal once, where it was. Before, it replaced every copy of the
  literal's placeholder text.
- **The storage-path rewrite** restores `FROM` keywords before it inserts
  storage paths, so a path is never read by that restore.

Matching is exact and case-sensitive, as the restore itself is. Queries that
don't contain these sequences outside literals are unaffected.

Full technical detail will accompany the corresponding security advisory once
it is published. Responsibly reported by **[@garnetarc](https://github.com/garnetarc)**.

## Ingest: Parquet row groups capped at 524,288 rows (page index available, off)

Each ingest flush used to become one Parquet row group (arrow-go's default cap is
64 Mi rows), so an hourly file was a single 3.3 M-row group: no row-group
min/max pruning inside the file, and one unit of parallel decode per file for
every reader. Ingest now caps row groups at 524,288 rows. Measured on the
sustained msgpack benchmark (iot, 12 workers, 60 s, 5 M-row flushes) this cap
is free — 36.9 M rec/s against 36.1 M on the previous build, within run-to-run
noise — where compaction's 122,880 cost 1.7 % alone and 3.7 % together with the
page index. Through the arcx engine on 40 M rows, a time-ordered
`ORDER BY time DESC LIMIT 100` with a tag filter goes from 70 ms to 5 ms, an
hourly `date_trunc` bucket from 67 to 58 ms, filtered scans 10–20 % faster;
DuckDB gains less because it prunes fewer shapes by row-group statistics.

The Parquet page index (column and offset index) can be written too, but it is
off by default: nothing in Arc reads it yet, and its write cost grows with the
row-group count (−3.7 % at 41 groups per flush). Compaction's DuckDB `COPY`
does not preserve it, tiering moves files whole, and continuous-query output
and `/api/v1/import` go through the same writer and get both settings. The
column index needs `write_statistics = true` (the default; with it off, only
the time column has one, since that column always keeps its statistics); the
offset index is always written when enabled. Values of `max_row_group_rows` between 1 and
1023, or negative, are rejected at startup.

```toml
[ingest]
max_row_group_rows = 524288   # 0 restores one row group per flush; 122880 matches compaction (−1.7 % sustained ingest)
write_page_index = false      # true writes column + offset indexes (no reader in Arc yet)
```

Env vars: `ARC_INGEST_MAX_ROW_GROUP_ROWS`, `ARC_INGEST_WRITE_PAGE_INDEX`.
Measurements: plannings `Arc/Arc_research_ingest_writer_knobs.md`.

## arcx router: arithmetic and more predicates in `HAVING` route to the engine

These `HAVING` forms now route to the arcx engine (arcX feat/res-3c):
- arithmetic: `+`, `-`, `*`, `/`, unary minus and parentheses, over aggregates (selected or not),
  aggregate aliases and numeric literals: `HAVING sum(bytes) / count(*) > 1024`,
  `HAVING max(x) - min(x) > 10`, `HAVING (count(*) + 1) * 2 BETWEEN 4 AND 10`;
- scientific-notation literals (`HAVING count(*) > 1e3`, `2.5E-1`);
- `IS DISTINCT FROM` and `IS NOT DISTINCT FROM`, against a literal, `NULL`, another aggregate or
  an expression;
- `IS TRUE`, `IS NOT TRUE`, `IS FALSE` and `IS NOT FALSE` on `bool_and` / `bool_or`;
- `LIKE` and `NOT LIKE` with a string pattern, on the tag column.

The engine types and evaluates arithmetic the way DuckDB 1.5.6 does: exact 64- and 128-bit
integers, exact decimals, `/` always a double, and DuckDB's own rounding when it converts a
128-bit sum or a decimal to a double. An integer overflow sends the query back to DuckDB, which
reports it, as before.

These forms still run on DuckDB:
- `%`, `//` and `^`; a negative-zero (`-0e0`) or out-of-range (`1e400`) scientific literal;
- arithmetic over the tag column, the time bucket, `min`/`max` of the time column, `bool_and` /
  `bool_or` or `regr_count`;
- two float-accumulated aggregates in one comparison (`sum(x) - sum(y) > 0`), or one of them added
  to or subtracted from another aggregate (`avg(x) - max(x) < 0`, `avg(x) + count(*) > 3`).
  DuckDB 1.5.6's value for these depends on the thread count, and a subtraction turns those last
  digits into the sign of the answer: `sum(x) - sum(y) > 0` returned between 7 and 15 groups across
  runs of the same query. Multiplying or dividing one of them (`sum(bytes) / count(*) > 1024`,
  `avg(x) * 2 > 5`) routes. The router counts `sum` as float-accumulated because it cannot tell
  an integer `sum` from a float one;
- a comparison with aggregates on both sides when any of them is float-accumulated, as for a plain
  aggregate-vs-aggregate comparison;
- a decimal past 2^53 with a fractional part converted to a double (`sum(i) + 0.5 > max(x)` with
  sums above about 10^15): DuckDB 1.5.6 and 2.0 round it differently, so the engine sends the
  query back;
- the literal `-9223372036854775808` next to arithmetic: DuckDB 1.5.6 answers `max(x) * -1 >
  -9223372036854775808` as false for every group;
- `ORDER BY` an expression or an aggregate that is not in the select list.

## arcx router: more `HAVING` forms route to the engine

These `HAVING` forms now route to the arcx engine (arcX feat/having-grammar):
- an aggregate that is not in the select list, including a select list with no aggregate at
  all: `SELECT host FROM cpu GROUP BY host HAVING count(*) > 100`. The engine computes it and
  leaves it out of the result;
- `NOT`, at any depth;
- `IS NULL` and `IS NOT NULL`;
- `BETWEEN` and `NOT BETWEEN` with literal bounds;
- `IN` and `NOT IN` with a list of literals;
- a literal on the left (`100 < count(*)`);
- one aggregate compared with another (`count(*) > max(retries)`, `max(x) > min(x)`,
  `bool_and(ok) <> bool_or(ok)`), or a time bucket compared with `min`/`max` of its own
  time column.

An aggregate compared with another aggregate routes only when both sides are exact:
`count`, `count_if`, `regr_count`, `min`, `max`, the bitwise and Boolean aggregates, and
`quantile_disc`. The time bucket can be compared with `min`/`max` of its own time column. A tag
column compared with an aggregate stays on DuckDB.

`avg`, `sum`, the variance family, `median`, `quantile_cont`, `mad`, the Kahan sums, the
regression and correlation aggregates, `skewness`/`kurtosis` and `arg_max`/`arg_min` stay on
DuckDB in that position. DuckDB 1.5.6 accumulates those in floating point, and the last digits
depend on how many threads ran the query. On 3 million rows of a constant 0.1, `avg(x)` came out
above `max(x)` with 1 thread, below it with 4, and above it again with 14, so neither engine has a
stable answer to compare.

These forms still run on DuckDB:
- `ORDER BY` an aggregate that is not in the select list (shadow mode cannot check an order whose
  values are not in the result);
- `IN` with `NULL` or a column in the list;
- an aggregate that is not in the select list and that is grouped by a tag column, when the same
  aggregate would stay on DuckDB if selected (the variance family, order statistics, regression
  aggregates, predicate aggregates).

When a hidden integer `sum` overflows 64 bits, the engine sends the query back to DuckDB, which
computes the `HAVING` in 128 bits. In a query that has a hidden aggregate, an overflow in a selected
sum is also sent back; DuckDB then reports the overflow, as before.

A float aggregate compared with a literal equal to a constant group's value (`HAVING avg(x) > 0.1`
over a gauge stuck at 0.1) can be answered either way by DuckDB itself, depending on the thread
count. This was possible since the `HAVING` change below and is easier to reach now that the
aggregate does not have to be selected. Shadow mode can report it as a mismatch.

## arcx router: `HAVING` routes over every aggregate the engine serves

A grouped query's `HAVING` now routes to the arcx engine for every aggregate the engine serves
(arcX feat/having-all-aggs). Until now the router sent these to DuckDB:
- `median`, `quantile_cont`, `quantile_disc`, `quantile` and `mad`;
- `skewness`, `kurtosis` and `kurtosis_pop`;
- `bool_and` / `bool_or`, bare (`HAVING bool_and(ok)`) or compared with `= TRUE`, `= FALSE`,
  `<> TRUE` or `<> FALSE`;
- `count_if`, `bool_and` and `bool_or` with a predicate argument (`HAVING count_if(status >= 500)
  > 10`), when written the same way as in the select list;
- a decimal literal (`HAVING count(*) > 2.5`, `HAVING avg(value) > 0.25`);
- an aggregate's alias (`count(*) AS n … HAVING n > 100`).

A decimal compared with a whole-number aggregate (`count`, `sum`, `min` or `max` of a `BIGINT`,
`regr_count`) is compared exactly, as DuckDB does, without going through floating point. The same
applies to `WHERE`: `WHERE status > 2.5` on a `BIGINT` column now runs on the engine instead of
falling back. An alias in `HAVING` is resolved the way DuckDB 1.5.6 resolves it. A group key wins
over an alias, and an alias wins over a table column of the same name that is not grouped. The
router sends the aggregate itself to the engine in place of the alias.

`HAVING regr_count(…)` was routed before but always fell back, costing a second run on DuckDB. It
now runs on the engine.

These forms still run on DuckDB:
- an aggregate in `HAVING` that is not in the select list, `IS NULL`, `BETWEEN`, `NOT`, `IN`, a
  literal on the left and two aggregates compared with each other (all route since the change
  above), and arithmetic;
- a decimal literal with more than 15 significant digits;
- `bool_and` / `bool_or` compared with a number or a string, or with `<` / `>` against `TRUE` /
  `FALSE` (comparing two Boolean aggregates routes since the change above);
- a number compared with the time bucket;
- a `HAVING` or `ORDER BY` name that DuckDB reads as something other than a column or alias: a
  keyword (`AS true … HAVING true > 5` compares the literal `true`) or a SQL value function
  (`current_timestamp`, `current_date`, `localtimestamp`, `user`, …, which DuckDB evaluates);
- `HAVING` over `arg_max`/`arg_min`/`max_by`/`min_by`, `skewness`/`kurtosis`/`kurtosis_pop`, the Kahan sums,
  `covar_pop`, `covar_samp`, `regr_sxy` or `regr_intercept`, when the query also has a `LIMIT` or
  orders by an aggregate. Ordering by the group keys still routes.

DuckDB 1.5.6's results for those last aggregates vary between runs or drift on real data. When such
a value sits close to the `HAVING` literal, a group can be in one engine's result and not the
other's. Shadow mode records a group present on only one side under `argdiff` or `oraclediff` (a
WARN), not as a mismatch. Groups present on both sides are still compared value by value. A
difference in any other column is a mismatch, as is a group key that appears twice, or a result
with no group in common with DuckDB's. `median`, `quantile_cont` and `mad` are not treated this
way: DuckDB's ARM build can differ from the engine in the last bit of an interpolated value, so a
value sitting exactly on the `HAVING` literal could in principle show as a mismatch.

The same change fixes a router check for top-N queries. An aggregate that carries a floating-point
tolerance must be the last sort key, but the check did not apply when that aggregate came first in
the select list (`SELECT avg(x), host … ORDER BY 1, 2`). The engine declined those queries, so
DuckDB ran them a second time. They now go to DuckDB directly.

## arcx router: grouped top-N (`ORDER BY … LIMIT`) routes to the engine

Grouped aggregates with an `ORDER BY` over aggregates, `DESC`, `NULLS FIRST`/`LAST`, up to four
sort keys and a `LIMIT` now route to the arcx engine (arcX feat/res-2-topn). This covers the
"top 10 hosts by error count" panel, `SELECT host, count(*) AS n … GROUP BY host ORDER BY n DESC
LIMIT 10`, and multi-key orders such as `ORDER BY count(*) DESC, host`. Until now only
`ORDER BY` a group key, ascending, routed.

A sort key can be a select-list position, the group key, the bucket alias, an aggregate's alias, or
an aggregate written exactly as it is in the select list. Ordering follows DuckDB: `NULLS LAST` by
default in both directions, NaN above every number, and -0 equal to +0. When groups tie on every
sort key, the engine breaks the tie by group key; DuckDB returns tied groups in no fixed order.

These forms still run on DuckDB:
- `LIMIT` without `ORDER BY`, `OFFSET`, and sort expressions (`ORDER BY count(*) + 1`);
- an aggregate that is not in the select list;
- sorting by `arg_max`/`arg_min`/`max_by`/`min_by`, `skewness`/`kurtosis`, `covar_pop`,
  `covar_samp`, `regr_sxy`, `regr_intercept` or the Kahan sums (`fsum`, `favg`, …): DuckDB
  1.5.6's results for these vary between runs or drift on real data, so its order would too;
- an aggregate whose result carries a floating-point tolerance (`sum`, `avg`, the variance and
  standard-deviation family, `median`, `quantile_cont`, `mad`, …) as any key but the last;
- a query that mentions `default_order` or `default_null_order`.

In shadow mode the order of the rows is compared as part of the answer. Groups that tie on every sort
key may come back in a different order without counting as a difference. When the `LIMIT` cut falls
inside a run of tied groups, the two engines can legitimately keep different groups from that run.
Shadow mode then asks DuckDB again, this time with the engine's tie-break appended, so the answer is
deterministic. The engine's rows must match that answer exactly, values included. Only then is the
difference recorded under a new `tiediff` outcome (a WARN). A wrong row at the cut, or any other
difference in membership, order or values, is a mismatch. The second query runs only when a tie falls
at the cut.

## arcx router: aliased aggregates route to the engine

Aggregates with an `AS` alias now route to the arcx engine
(arcX feat/res-1-agg-aliases). This covers `SELECT avg(value) AS value, count(*) AS n …`,
grouped queries such as `SELECT date_trunc('hour', time) AS time, avg(value) AS value …
GROUP BY 1`, and the footer fast paths (`SELECT count(*) AS n FROM cpu`, `max(time) AS newest`).
Until now any alias on an aggregate sent the query to DuckDB, and most dashboard panels alias
their aggregates. The output column carries the alias exactly as written.

These forms still run on DuckDB:
- an alias without `AS` (`min(x) mx`);
- a quoted alias (`AS "time"`);
- in a grouped query, an alias that matches the group key, the bucket alias or another alias;
- an alias that is a SQL clause word (`AS from`, `AS where`), which DuckDB accepts;
- a `HAVING` that names an aggregate's alias. That routes since the `HAVING` change above,
  and `ORDER BY` an alias since the top-N change above.

Shadow mode now also compares result column names, since aliases make them user-controlled. One
existing difference is exempted. When DuckDB serves an unaliased `date_trunc` bucket (grouped,
or the plain `date_trunc(…), count(*)` form), Arc's rewrite of the bucket expression names that
column after the rewritten expression (`make_timestamptz(…)`). The engine names it
`date_trunc('hour', "time")`. Aliased buckets (`AS time`) are named identically on both paths.

## arcx router: skewness and kurtosis route to the engine

The arcx engine now serves `skewness(x)`, `kurtosis(x)` and `kurtosis_pop(x)` over `BIGINT` and
`DOUBLE` columns (arcX feat/agg-8-skew-kurt). The recognizer routes them in ungrouped aggregates
and under a time bucket; grouped by a tag column they stay on DuckDB. A `HAVING` that
references one of them stayed on DuckDB too, until the `HAVING` change above.

Measured through Arc on a synthetic 132-million-row corpus (DuckDB → engine): `skewness`
155 → 90 ms, `kurtosis` 233 → 90 ms, all three together 555 → 157 ms, an hourly-bucket
`skewness` + `kurtosis` 555 → 216 ms.

The engine computes these with central moments. DuckDB 1.5.6 uses raw power sums, which lose
precision when values are large compared with their spread. That covers most telemetry, from a
12 V rail to a temperature sensor or an epoch-scale counter. In those cases the engine is exact
where DuckDB's answer drifts or comes back NULL or NaN. On a constant column (a stuck sensor) the
engine returns NULL; DuckDB 1.5.6 returns a value that depends on the CPU architecture.
`skewness` of a constant column is NULL rather than DuckDB 1.5.6's NaN, so stuck sensors no longer
sort above every real value. Shadow mode therefore records any difference on these three functions
under the `oraclediff` outcome (a WARN), not as a mismatch.

## arcx router: median, quantiles, mad and mode route to the engine

The arcx engine now serves DuckDB's order statistics (arcX feat/agg-7-order-stats):
`median(x)`, `quantile_cont(x, q)`, `quantile_disc(x, q)` and its alias `quantile(x, q)` and
`mad(x)` over `BIGINT` and `DOUBLE` columns, and `mode(x)` over `BIGINT` and `VARCHAR` columns. `q` is a single
literal between 0 and 1. The recognizer routes these in ungrouped aggregates and under a time
bucket. Grouped by a tag column they stay on DuckDB, as does `mode` in any grouped query. A
`HAVING` that references one of these functions stayed on DuckDB too, until the `HAVING` change
above. A list of fractions, a negative
fraction, `mode` over `DOUBLE`, and timestamp or text arguments to the other functions also stay
on DuckDB; the router cannot see column types, so these reach the engine and fall back
immediately.

Measured through Arc on a 33-million-row window of a synthetic corpus (DuckDB → engine):
`median` 436 → 53 ms, `quantile_cont(…, 0.99)` 271 → 58 ms, `quantile_disc` 218 → 50 ms,
`mad` 1074 → 54 ms, `mode` 43 → 32 ms, an hourly-bucket `median` + p99 375 → 90 ms.
DuckDB finalizes each of these with a single-threaded selection; the engine selects in
parallel.

Like DuckDB, the engine keeps every value in memory for these functions. It refuses a query
whose values would exceed 1 GiB — for example a median over 132 million rows, or three
quantiles over 33 million — using the Parquet row counts, before reading any data, and
DuckDB serves it instead.

`mode` falls back to DuckDB whenever two values tie for most frequent, because DuckDB 1.5.6
and 2.0 pick different winners in that case. A tie is only known after the engine has counted
every value, so a tied `mode` costs the engine's scan plus DuckDB's. Shadow mode compares `median`, `quantile_cont` and
`mad` within the usual 1e-9 tolerance, since DuckDB's ARM build can differ from its x86 build in
the last bit of an interpolated value. `quantile_disc` and `mode` compare exactly.

## arcx router: count_if / bool_and / bool_or with a predicate argument route to the engine

The arcx engine now serves the predicate form of the Boolean aggregates (arcX
feat/agg-6b2-predicate): `count_if(status >= 500)`, `countif(...)`, `bool_and(cpu_user < 90)`,
`bool_or(host = 'web-1')`. The argument may combine comparisons, `IS [NOT] NULL`,
`BETWEEN`, `[NOT] LIKE`, `AND`, `OR` and parentheses. The recognizer routes these in
ungrouped aggregates and under a time bucket (`date_trunc` or the epoch-math form); grouped by a tag column they
stay on DuckDB, as the column forms do. Forms whose output column name DuckDB 1.5.6 and
2.0 render differently stay on DuckDB: `IN`, `NOT`, arithmetic, timestamp literals, and
non-canonical number spellings such as `007` or `-0.0`. A `HAVING` that references a
predicate aggregate also stayed on DuckDB, until the `HAVING` change above.

Measured through Arc on a synthetic 132-million-row corpus (DuckDB → engine):
`count_if(cpu_user > 90)` 103 → 76 ms, `count_if(code >= 400)` 34 → 19 ms,
`count_if(host = '…')` 38 → 24 ms, `bool_or(…) + bool_and(…)` 145 → 98 ms, three predicate
counts in one query 184 → 93 ms, an hourly-bucket `count_if` + `bool_or` 405 → 235 ms;
`count_if(host LIKE '…')` at parity (27 → 25 ms). A string column read only by predicate
arguments is evaluated on its Parquet dictionary.

The engine now names derived columns with DuckDB 1.5.6's full keyword table, so a column
named like a keyword is quoted exactly as DuckDB quotes it. Two names changed: `sum(level)`
is now `sum("level")`, and `sum(date)` is now `sum(date)` rather than `sum("date")`, since
`date` is not a DuckDB keyword. A grouped `date_trunc` over a non-keyword column also lost
its quotes (`date_trunc('hour', ts)`). These are the names DuckDB has always returned for
the same queries, so served results now carry the same column names as DuckDB fallbacks.

The engine and the recognizer now decline three things DuckDB rejects or binds
differently: a bare reserved keyword used as a column (`sum(end)`), `!=` written directly
against a sign (`x!=-2`, which DuckDB reads as an unknown operator), and decimal literals
with more than 15 digits (counted after any leading zeros of the integer part). DuckDB 1.5.6 converts the last of these to a slightly
different double, which can change the match.

## arcx router: product, Kahan sums, bitwise and boolean aggregates route to the engine

The arcx engine now serves `fsum` / `sumkahan` / `kahan_sum`, `favg`, `bit_and`, `bit_or`,
`bit_xor`, and — over a `BOOLEAN` column — `bool_and`, `bool_or` and `count_if` / `countif`
(arcX feat/agg6bp-numeric). The recognizer routes them in ungrouped aggregates and under a
`date_trunc` time bucket. Grouped by a tag column they stay on DuckDB (measured 1.1–1.4×
slower in the engine). The engine also implements `product`, but it is not routed: it
measured slower than DuckDB. Predicate arguments (`count_if(status >= 500)`) still run on
DuckDB; they are the next engine slice. `bool_and` / `bool_or` in `HAVING` stayed on DuckDB as
well, until the `HAVING` change above.

Measured through Arc on a synthetic 132-million-row corpus: `bit_and` / `bit_or` / `bit_xor`
46 → 25 ms, `count_if` / `bool_or` / `bool_and` 40 → 36 ms, an hourly-bucket
`fsum` + `bit_or` 360 → 250 ms; `fsum` + `favg` at parity.

The engine declines a Kahan sum that comes out non-finite (DuckDB's own result there depends
on how it splits the work). Shadow mode compares the Kahan sums within the 1e-9 tolerance; on
cancelling data DuckDB's Kahan sums swing between thread counts, so a value-only difference on
them is a WARN under the `oraclediff` outcome. The bitwise, boolean and `count_if` results
compare exactly.

**Shadow comparator fix:** a difference between a finite value and ±Inf, or between +Inf and
−Inf, passed the tolerance test as a match, because the tolerance itself became infinite. Such
differences are now reported (`Inf-ness`) in both the ungrouped and grouped comparators.

## arcx router: covariance, correlation and regression aggregates route to the engine

The arcx engine now serves DuckDB's two-argument statistics family, `fn(y, x)` over two
`BIGINT` or `DOUBLE` columns (arcX feat/agg6b-binary): `covar_pop`, `covar_samp`, `corr`,
`regr_count`, `regr_avgx`, `regr_avgy`, `regr_slope`, `regr_intercept`, `regr_r2`,
`regr_sxx`, `regr_syy` and `regr_sxy`. The recognizer routes them in ungrouped aggregates,
in aggregates grouped by a `date_trunc` time bucket, and in `HAVING` when the same
aggregate is selected (`HAVING regr_count(…)` fell back to DuckDB until the `HAVING` change
above). Grouped by a tag column they stay on DuckDB, like the variance
family. A NaN or ±Inf in either argument makes the engine decline the query; DuckDB then
answers (or raises its own out-of-range error) exactly as before.

Shadow mode compares the new columns within the same 1e-9 tolerance as `sum` and `avg`;
`regr_count` (an unsigned 32-bit integer in DuckDB) compares exactly. On four of these
functions DuckDB 1.5.6 itself returns values that move by more than the tolerance between
thread counts on data with heavy cancellation (`covar_pop`, `covar_samp` and `regr_sxy`
near zero at large magnitudes, `regr_intercept` far from the data's mean). A value-only
difference on those four is logged at WARN and counted under a new `oraclediff` shadow
outcome instead of the mismatch alarm, the same treatment `arg_max` ties already get under
`argdiff`. A NULL-ness, NaN-ness or type difference on them still raises the alarm.

Nothing changes for a deployment without the engine linked in (`arcx_engine` build tag) or
with `ARC_ROUTER=off`.

## arcx router: variance and standard deviation aggregates route to the engine

The arcx engine now serves the unary moment aggregates `var_pop`, `var_samp`,
`variance`, `stddev`, `stddev_samp`, `stddev_pop` and `sem`, plus `mean` as an
alias of `avg`, over `BIGINT` and `DOUBLE` columns (arcX feat/agg6-moments). The
eligibility recognizer accepts them in ungrouped aggregates, in aggregates
grouped by a `date_trunc` time bucket, and in `HAVING` when the same aggregate
is also selected. Grouped by a tag column they stay on DuckDB: measured on a
3.95-billion-row production corpus the engine folds bucket runs 1.5× faster
than DuckDB but is 2.7× slower per host, and a known loss is not routed. The function name is lowercased in the derived
column name and the argument keeps its spelling (`VARIANCE(X)` → `variance(X)`),
as DuckDB does.

A column holding `NaN` or `±Inf` makes DuckDB raise an out-of-range error for
these functions; the engine declines such a query instead, so the router falls
back and DuckDB raises its own error. The user sees the same outcome either way,
and shadow mode counts a decline, not an engine error.

Over a constant column (a stuck sensor) they return exactly `0`, as DuckDB does — including for
values with no exact binary form, such as `0.1` repeated.

Still on DuckDB, deliberately: `DISTINCT` and expression arguments, and map-subscript arguments
(`skewness`/`kurtosis` and the covariance family route since — see their sections). In shadow
mode the new columns compare within the same 1e-9 tolerance as `sum` and `avg`.

Nothing changes for a deployment without the engine linked in (`arcx_engine`
build tag) or with `ARC_ROUTER=off`.

## arcx router: map subscripts `col['key']` route to the engine

The arcx engine now serves `col['key']` over `MAP(VARCHAR, VARCHAR)` columns in
projection and WHERE (arcX #72), the shape OpenTelemetry logs need for
`ResourceAttributes`, `ScopeAttributes` and `LogAttributes`. Arc's eligibility
recognizer previously declined any query containing `[`, so none of those
queries could reach the engine. It now recognizes the subscript exactly where
the engine serves it: as a select item, as the head of a WHERE atom (`=`,
`<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] NULL`, `[NOT] LIKE`, `[NOT] IN`,
`BETWEEN`) in single-table scans and ungrouped aggregates, and as the argument
of `count(...)` in an ungrouped aggregate (arcX #73). Subscripts in ORDER BY,
under any other aggregate, in grouped aggregates, in arithmetic or inside a
projection function stay on DuckDB, as do keys containing `'` or `\`.

Nothing changes for a deployment without the engine linked in (`arcx_engine`
build tag) or with `ARC_ROUTER=off`; the DuckDB path runs the user's SQL
unchanged. In shadow mode the derived column is named `col['key']` on both
engines and compares cell for cell; a mismatch log names the column with the
key masked (`col['...']`), since a key is user data. One metrics detail for
engine builds: the shape census now files any query containing brackets
(`arr[1]`, `m['a']['b']`) under a shape reason instead of `unlexable`.

## Fix: a crash during a multi-hour flush no longer replays the hours already stored ([#1492](https://github.com/Basekick-Labs/arc/issues/1492))

A flush whose rows span several hours writes one Parquet file per hour. The
WAL checkpoint that stops those rows replaying was written once, after the
last file, so a crash (kill -9, OOM, power loss of the process) between the
first file and that checkpoint replayed every row of the flush on restart: the
hours already in storage were stored twice. Compaction removes the duplicates
of a tagged measurement at its next pass; a tagless measurement kept them
([#948](https://github.com/Basekick-Labs/arc/issues/948)). How many rows that
is depends on how many hours one flush spans: with the sample `arc.toml`
(`max_buffer_size = 5000000`, `max_buffer_age_ms = 100`) a kill -9 during a
backfill duplicated 21–23% of the rows acknowledged in the run, eleven whole
hours each stored twice.

Each hour file's rows are now checkpointed in the WAL as soon as that file is
stored and registered, with the row-range checkpoint recovery already used
for its own split replays. A crash replays only the rows of the files not yet
written, and recovery replays a columnar entry's uncovered rows alone. The
duplicate window is now at most the rows of the hour file each in-flight flush
was writing when the process died.

What changes for an operator:
- fewer duplicates after a crash, and for a tagless measurement no longer
  whole hours of them;
- one durable WAL write per hour file of a multi-hour flush, before the
  flush's own checkpoint, so a backfill flush spanning 30 hours writes 30
  small checkpoints where it wrote one; on a cluster node each hour file also
  makes its registration durable (one outbox sync) before its checkpoint. A
  flush within one hour (live ingest) is unchanged;
- `arc_wal_row_checkpoints_written_total` and
  `arc_wal_row_checkpoints_dropped_total`: a dropped checkpoint (the WAL
  writer refused it or it timed out) only means those rows replay as before;
- a flush that fails part-way now reports the rows and files it did store
  (`records_stored`, `files_stored`) and counts as lost only rows with no WAL
  copy that reached no file.

Downgrade: an Arc without this change replays an entry with row-range
checkpoints in full, which is today's duplicate window, not a loss. The gauge
`arc_wal_partial_row_recovery_pending` reports only ranges left by a recovery
pass, not those a live flush leaves.

On a writer with WAL coverage (`cluster.wal_coverage`,
[#1207](https://github.com/Basekick-Labs/arc/issues/1207),
[#1498](https://github.com/Basekick-Labs/arc/issues/1498)) each hour file
registers with the group's coverage for that file as before, only as soon as
it is stored. For a flush that fails part-way, the registered-floor tracker
counts the stored rows once those files' registrations settle, never earlier,
and the floor advances once recovery's replay of the other rows completes each
batch; the flush's WAL entries are kept until then.

## Fix: a time bound written with `epoch_ms()`, `make_timestamp()` or `to_timestamp()` prunes ([#1491](https://github.com/Basekick-Labs/arc/issues/1491))

A query bounded by an epoch number read every partition of the measurement.
It now reads only the partitions and files of its range, the same as one
written with a `TIMESTAMP` literal. Clients that keep event time as epoch
milliseconds usually write it this way:

```sql
SELECT ... FROM market.quotes
WHERE time >= epoch_ms(1790856000000) AND time < epoch_ms(1790856060000)
```

On 8.7M forex ticks the one-minute query above went from 97 ms to 1.8 ms on
freshly ingested data (656 files), and from 9.7 ms to 3.1 ms after compaction
(43 files): the same as the `TIMESTAMP` literal.

Accepted, with one plain number as the argument:
- `epoch_ms(n)`: milliseconds;
- `make_timestamp(n)`: microseconds;
- `to_timestamp(x)`: seconds, whole or decimal.

Anything else in the call, such as arithmetic, a cast, a column or a second
argument, gives no bound from that term, as before. So does a bound outside
years 0001 to 9999, counting the second added to a `to_timestamp` bound and
the microsecond added to an inclusive end. This also applies to a literal:
`time <= '9999-12-31 23:59:59.999999'` no longer gives an end in year 10000,
which a query that needs an unavailable cold tier read as "no cold data"
instead of answering 503. A DELETE with such a bound now also reads only the files
of its range (`delete.time_pruning`).

`to_timestamp` is computed through a double and compared in the session time
zone, which Arc pins to UTC. Its bound is widened by one second. These bounds
narrow what is read but are not used to answer a query as empty without
reading it.
## Fix: query pre-processing rewrites stay inside their own text ([#1488](https://github.com/Basekick-Labs/arc/issues/1488))

Before running a query, Arc applies a few text rewrites that speed common
shapes up — `time_bucket`/`date_trunc` to epoch arithmetic, some
`REGEXP_REPLACE`/`REGEXP_EXTRACT` to string functions, and a reorder of `LIKE`
predicates. These ran on the raw query text, so a rewrite could match a string
literal's content or a comment, or copy a column argument that contained a
quote, and the rewritten query no longer matched the text that was validated.

The rewrites now skip any match that begins inside a string literal (`'…'`,
`E'…'`, `"…"`, `$tag$…$tag$`) or a comment, and `time_bucket`/`date_trunc` are
left unchanged when their column argument is not a plain unquoted expression.
No query's result changes. A query whose `time_bucket`/`date_trunc` column is a
quoted identifier (for example `"my col"`) is now evaluated by DuckDB natively
rather than through the epoch-arithmetic rewrite — the same answer, without that
speedup. A query that would have been rewritten into something other than what
was validated is now run as written.

## New: Iceberg export keeps files moved to an S3 cold tier ([#939](https://github.com/Basekick-Labs/arc/issues/939))

Iceberg export and cold-tier tiering can now run together when the cold tier is
S3 (`tiered_storage.cold.backend = "s3"`). A file that tiering moves to cold
stays in the Iceberg table under its cold URI, with the same rows and
statistics. External readers see the same data before, during and after a
migration. Supported with a local or an S3 primary.

- **Mixed-location tables:** with a local primary, an exported table holds
  `file://` paths for hot data and `s3://` paths for cold data. A reader needs
  access to both, so it must run on the Arc host with S3 credentials.
- **Which copy is exported:** the one Arc's own queries read. When a file is
  on both tiers (during a migration, or a hot-only database's cold copy), the
  hot copy is exported until Arc's tier record says the file is cold.
- **No extra reads:** a migrated file's entry is copied from the hot one, and
  the cold object is not read. Only cold data the table never held hot is
  read, once, with the cold tier's own credentials.
- **Cost:** the cold tier is listed per measurement and the listing is cached.
  Arc re-lists after every change it makes to the cold tier: a tiering cycle,
  cold retention, a DELETE of a cold file, a database delete. Otherwise it
  re-lists every `iceberg.cold_rescan_interval` seconds (new, default `3600`),
  which bounds changes made outside Arc. Before the table drops a file that
  left the hot tier, Arc checks the cold tier for it (one HEAD), so a
  migration never makes the table lose a file.
- **Window:** during the one pass between the hot copy's delete and the table
  moving to the cold URI, a reader that already resolved the old URI can get
  a not-found error. It never gets wrong rows. Compaction and retention
  already have the same window.
- **Refused at startup:** an Azure or GCS cold tier, or S3 cold on Google
  Cloud Storage's interop endpoint, with Iceberg enabled.
- **Unavailable cold tier:** if the cold tier is configured but cannot be
  built, the export does not start, and the tables keep their last snapshot.
- **Licensing:** tiering requires an Enterprise license and Iceberg export does
  not. Without the license Arc disables tiering, so with a cold tier still
  configured the export does not start (as for an unavailable cold tier) and
  the tables keep their last snapshot until the license is restored.

DELETE of a whole cold file now also drops the query caches, as cold retention
already did.

## New: Iceberg export on S3 primary storage ([#939](https://github.com/Basekick-Labs/arc/issues/939))

Iceberg export now runs with `storage.backend = "s3"` (AWS S3 and S3-compatible
stores), not only on local storage. Tables are written under the storage root
(`s3://<bucket>/<prefix>/arc_<database>.db/<measurement>/`) and read by
Iceberg engines with S3 access (verified with PyIceberg and DuckDB
`iceberg_scan`).

- **Credentials:** the export uses Arc's own S3 credentials and renews them the
  same way: static keys, the default chain, IRSA on EKS, or an assumed role. No
  credential is written into Iceberg metadata, the catalog or the logs.
  Readers need their own read access to the bucket.
- **Endpoint:** `storage.s3_endpoint` and `storage.s3_path_style` apply to the
  export too. The `AWS_S3_ENDPOINT` environment variable no longer changes
  where it writes.
- **Cost:** schemas are read from each new file's footer (the last 64 KiB of
  the object), never the whole file, and sizes come from the listing. After a
  restart an unchanged table reads no footers.

**Refused at startup with Iceberg enabled:**
- `storage.backend` other than `local` or `s3` (Azure, GCS);
- `storage.backend = "s3"` on Google Cloud Storage's S3-compatible endpoint;
- an `iceberg.warehouse` outside the storage root (another bucket, another
  prefix, or a local path with an S3 backend);
- `cluster.shared_storage_mode = true` without `cluster.failover_enabled =
  true`;
- cold-tier tiering, as before.

**Shared-storage clusters:** only the node holding the compactor lease
exports, with no fallback to the static compactor role, and the lease is
re-checked for each measurement. The table's current version is the one
published in the warehouse, not the node's own catalog row:
- a node that takes over, or comes back after a failover, continues from
  the published version;
- if two nodes commit the same version, the second one's publish is refused,
  its pass fails, and it adopts the published version on the next pass,
  logged as `another writer published this table version first`;
- a node that lost its catalog (a new node, a deleted SQLite) continues the
  existing table instead of creating a second one in the same directory. On
  local storage too.

**Upgrade:** nothing to do for local-storage deployments. To export an existing
S3 deployment, set `iceberg.enabled = true`. The first pass registers every
existing data file, reading each file's footer twice (once for the schema,
once for its statistics); expect two ranged GETs per file on that pass.

## Fix: Iceberg export counts each row once during a DELETE or a compaction ([#939](https://github.com/Basekick-Labs/arc/issues/939))

Arc's queries leave out a file whose rows another file already holds, such as
the file a row DELETE replaced while it is still in storage next to its
rewrite. On a standalone node the Iceberg export did not: a reconcile pass
between a partial DELETE publishing its rewrite and removing the old file
published both, and external readers (PyIceberg, DuckDB `iceberg_scan`)
counted the surviving rows twice. The window is normally milliseconds; if the
removal failed, it lasted until the leftover drainer removed the old file.

The export now leaves out exactly the files Arc's queries leave out (DELETE
rewrites, compactions in flight, and on a cluster the same set its queries
use). Where a query would guess (a stat that fails, an unreadable DELETE
journal, the stale-local breaker), the export does not: that measurement's
pass fails, keeps the previous snapshot, and is retried on the next pass.

**A file removed while a measurement is being listed** (a compaction or DELETE
finishing mid-listing) no longer drops its rows from that pass: the
measurement is listed again, up to three times. If the files keep changing,
the pass fails for that measurement and keeps the previous snapshot, logged
as `file set of <db>/<measurement> kept changing under the listing`.

## New: an opt-in limit on unflushed ingest data, refusing writes with 503 ([#1468](https://github.com/Basekick-Labs/arc/issues/1468))

When storage falls behind (a slow disk or object store), the flush queue
fills and each measurement's buffer keeps growing in memory with no limit,
until the process runs out of memory. The new `ingest.max_buffered_bytes`
makes Arc stop taking new writes first. It is **off by default**; nothing
changes until you set it. Once the rows waiting to be flushed
(`arc_buffer_bytes_buffered`) reach the limit, every write request is refused
before any of it is written:

- HTTP writes (MessagePack, line protocol, TLE, imports) answer **503** with
  `Retry-After: 5` and `X-Arc-Ingest-Refused: buffer_full`. Nothing of a
  refused request is stored, so retrying it is safe. Clients that already
  retry the other 503 refusals (WAL disk pressure, cluster fencing) need no
  change.
- Bulk imports are refused from 90% of the limit, and a running import paces
  its writes to stay below that level (each grouped write can still add up to
  a few tens of MB at once). The limit never refuses an import part-way, but
  while other writers keep the node near it a running import can wait out its
  2-minute flush wait and end with a partial write, as it could before when
  flushes did not keep up.
- MQTT subscriptions are suspended until the buffer drains below 90% of the
  limit (and for at least 5 seconds), as they are for WAL disk pressure. With
  `clean_session = false` and QoS 1 or 2 the broker holds the messages; a
  `clean_session = true` subscription loses what is published meanwhile. A
  message already being handled when the limit is reached is retried for 2
  seconds and then dropped (counted as failed).
- WAL replay and continuous-query writes are never refused.
- New counter `arc_ingest_buffer_full_rejections_total` (one per refused
  request, imports refused from 90% included), and a warning in the log at
  80% of the limit.

Set `"auto"` for half the memory limit Arc detects (the container limit where
there is one; off when none is detected), or a size (`"4GB"`, at least `1MB`).
The default, `"0"`, keeps the limit off. Startup logs the value in use:

```toml
[ingest]
max_buffered_bytes = "auto"   # or "4GB"; "0" (default) is off
```

Or `ARC_INGEST_MAX_BUFFERED_BYTES`. A refusal does not itself start a flush:
an explicit limit below what buffers normally hold before their size or age
flush (`max_buffer_size`, `max_buffer_age_ms`) refuses writes until the next
one, even with storage keeping up, so set it well above that. It is a
backstop, not a bound on memory:
the estimate is a MessagePack request's payload size and leaves out the
copies a flush makes, requests admitted at the same time can each overshoot it
by their own size (up to `server.max_payload_size`), and WAL replay at startup
can put a node over it until the replayed rows are flushed. The existing
`arc_buffer_memory_pressure` gauge and its warning at 50% of the memory limit
are unchanged.

## Fix: a flush with more than 2 GiB in one text column is written instead of failing ([#1284](https://github.com/Basekick-Labs/arc/issues/1284))

When storage fell behind and the flush queue stayed full, a measurement's
buffer kept growing until one string or binary column of its flush held more
than 2 GiB. Arrow addresses such a column with 32-bit offsets, so the flush
failed with `failed to write Parquet: failed to write record batch: ...
slice bounds out of range [:-2147483616]`. With the WAL on the rows stayed in
the WAL and were replayed later; with it off they were lost. One MessagePack
request could get there alone too: invalid UTF-8 is stored as U+FFFD, three
bytes for each invalid byte, so a request under the 1 GiB payload limit could
hold more than 2 GiB of text, and its flush failed on every replay until the
WAL quarantined it.

A flush now writes its rows as several row groups of the same file, cutting
before any row that would take a string or binary column of the row group
past 1 GiB. Files, WAL checkpoints and cluster file registration are as
before: one flush still writes the same files. Flushes within the limit are
written exactly as before. A flush that is cut holds the Arrow arrays of one
row group at a time instead of the whole flush's.

What a buffer retains while storage is behind is bounded separately, by
`ingest.max_buffered_bytes` ([#1468](https://github.com/Basekick-Labs/arc/issues/1468),
see its entry).

Thanks to [@efegokdemir](https://github.com/efegokdemir), who traced the
overflow to buffers retained under flush-queue backpressure and proposed the
first fix in [#1321](https://github.com/Basekick-Labs/arc/pull/1321); its
tests are the basis of this change's.
## Fix: license gates expire while Arc is running (#1442)

License checks now use the signed expiry at the time of each check. A final
definitive server rejection clears the current license and attempts to remove
its disk cache. Removal failures are logged; if deletion fails, a restart
during an outage may still reuse the rejected cache. Transient network or
protocol failures preserve the license until that signed
expiry. Both machine-reactivation responses still attempt activation before
the final result determines whether to invalidate the license. Health status
and days remaining use the same expiry calculation.

Existing RBAC restrictions remain enforced when a license lapses.

## New: a grace period after a paid license expires

A paid license keeps its Enterprise features for fourteen grace days after it
expires, counted from the midnight UTC that follows its expiry. Grace days 1-6
run as usual; days 4-6 log a warning every hour. On grace days 7-14 Enterprise
features stay on and live ingestion is refused with `503`,
`X-Arc-Ingest-Refused: license_grace` and `Retry-After: 300`, counted in
`arc_license_write_refusals_total`, and so are files pushed to an edge-sync
hub; MQTT subscriptions are suspended so the
broker holds messages (QoS 1 or 2, `clean_session=false`, within the broker's
own limits). WAL replay, continuous queries, compaction and retention keep
running. At the start of day 15 the license expires and every Enterprise gate
closes; the node then accepts writes again, without Enterprise features. A
self-serve trial ends at its expiry, with no grace.

`/health` reports `grace_period`, `writes_stopped` or `expired`, with
`grace_day`, `writes_stop_at` and `cutoff` during grace; `days_remaining`
counts to the expiry while the license is active and to the cutoff during
grace. The schedule is computed in Arc from the signed license, the same way
online and for license files, and needs no license-server change. A restart
during grace keeps the license (from the verified cache, or the license file),
and so does the license server refusing to re-activate a license past its
term. A machine whose fingerprint changed (a rescheduled pod, new hardware),
or whose license cache was lost, cannot activate during grace and starts
without Enterprise features. A renewal takes effect at the next periodic
license check (every 4 hours) or a restart.

Setting the host clock back does not extend a license: the license clock
never goes below a high-water mark kept in `license_clock.json` beside the
auth database. The mark comes only from the license server's clock (or a
license file's signed issue time) and advances by elapsed time, so a host
clock that is wrong, ahead or behind, is never written into it. Arc logs a
warning at startup when the host clock reads earlier than the mark; deleting
the file resets the guard.

## Fix: the MessagePack spec endpoint describes the endpoint as it is ([#1434](https://github.com/Basekick-Labs/arc/issues/1434))

`GET /api/v1/write/msgpack/spec` said authentication was "not yet
implemented", although the route it describes (and the spec route itself)
requires a token. The document now matches the code:

- `authentication`: `Authorization: Bearer <token>`, also `Token <token>`, a bare `Authorization: <token>`,
  `x-api-key` and the deprecated `?p=`; write permission is required.
- New sections: `database` (the `x-arc-database` header, default `default`),
  `time` (seconds, milliseconds, microseconds or nanoseconds, detected from the
  first value; nanoseconds were missing), `responses` (status codes), and
  `method`.
- Columnar format documents `tag_keys`, and the columnar example now uses it.
- `compression` is now an object: detection by magic bytes and the payload
  limits. `content_type` notes that the server does not check it.
- The `performance` section is removed. Its throughput figures were years out
  of date; current numbers are in the README.

Clients that read `authentication.header` now get `Authorization` instead of
`x-api-key`. Clients that read `compression` as a string need to read the new
object.

**Behaviour change: a bad item in a MessagePack batch is reported.** In a
`batch`, or a top-level array of items, an item that could not be decoded (no
`fields`, columns of different lengths, a time that is not a number, an item
that is not a map) was logged and skipped, and the request answered **204** as
if everything had been written. It is now dropped the way any other point that
cannot be written is: the other items are still written, and the request
answers **400** `partial write: N points dropped: ... an item of the batch or
array could not be decoded: <reason>; the other points were written`. Each
dropped item counts as one point, however many rows it held. A client that treated 204 as "all written"
was silently losing those items; one that retries on 400 should not retry a
partial write, since the rest is already stored. A batch nested inside a
batch or array, which answered 500 and stored nothing, is now written like a
top-level one.

## Fix: a cluster node stalled for 15 seconds or more rejoins Raft ([#1431](https://github.com/Basekick-Labs/arc/issues/1431))

A Raft node that stopped responding for about 15 seconds or longer (a paused
container, a long GC or I/O stall, a VM migration) never rejoined once it
resumed. It logged `raft auth handshake (accept): read client mac: EOF` and
`pre-vote campaign failed` about once a second until it was restarted, while
`/health`, `/ready` and `/api/v1/cluster/nodes` still reported it healthy. Raft
stopped applying its file registrations, and on shared storage every `DELETE`
and database drop answered 503 unless `allow_unreachable=true` was passed.

The cause was the interaction between the Raft peer-authentication handshake
and hashicorp/raft's accept loop. The connections that queued up during the
stall had already been abandoned by their peers, so each one failed its
handshake. raft then waited up to a second before accepting the next one, and
by the time it did, that peer had given up too.

Handshakes now run alongside each other, off raft's accept loop, and raft
only ever receives connections that passed. Abandoned connections drain at
once, and a peer that connects without speaking no longer delays the others.
In testing, nodes paused for 15 s and for 40 s both rejoined without a restart.

What operators see in logs changes:

- A peer that answers and is refused — a full handshake response that does not
  verify (a wrong `cluster.shared_secret` or cluster name), or a TLS client
  certificate that fails verification (`cluster.tls_enabled`) — is logged
  as a Warn, `Rejected Raft connection: peer failed authentication`, with the
  error and remote address. Each source host is logged at most once every
  10 seconds, and the Warn carries a count of the rejections suppressed since
  the previous one. These used to appear as a raft
  `failed to accept connection` error line per attempt.
- A connection whose peer hung up, went silent or sent only part of a
  handshake response, which is routine after a stall, is logged at Debug only.
  A port scan, or a probe that sends fewer bytes than a handshake response, is
  therefore no longer visible at the default log level.

A node whose Raft startup fails before the Raft instance is created (for example,
an unopenable log store) now releases the Raft port instead of holding it
until the process exits.

No configuration change. Every node can be upgraded independently; the
handshake on the wire is unchanged.

## Fix: Google Cloud Storage through the S3-compatible endpoint (#1399)

GCS S3 interoperability now leaves `Accept-Encoding` out of request signatures
while preserving the SDK's identity encoding on the wire. Optional upload
checksum trailers are disabled for `storage.googleapis.com`, allowing object
writes without the SDK's streaming trailer format. Other S3 endpoints retain
their existing signing and checksum behavior.

## New: MQTT topic mappings can set measurements and tags (#479)

`topic_mapping` still accepts the existing topic-to-database strings and now
also accepts rule objects:

```json
"topic_mapping": {
  "factory/+/+/metrics": {
    "measurement": "machine_metrics",
    "tags_from_topic": [
      {"position": 1, "tag_name": "line"},
      {"position": 2, "tag_name": "machine_id"}
    ]
  },
  "legacy/topic": "otherdb"
}
```

- A key is an MQTT topic filter: `+` matches one level, a trailing `#` any
  number. An exact key wins; then the filter with more literal levels, then
  more `+`, then fewer `#`. A leading wildcard does not match `$`-prefixed
  topics.
- `database`, `measurement` and `tags_from_topic` are each optional; a rule
  sets at least one. What a rule leaves out still comes from the payload.
- `position` counts levels of the full topic from 0. A position past the
  filter's last level is refused, unless the filter ends in `#`, where a topic
  without that level is written without the tag.
- A topic tag replaces a payload tag of the same name, in any letter case. A
  payload field of exactly that name is stored as `<name>_value`; one that
  differs only in letter case, or a column already stored under another
  spelling, is a case clash resolved as for any write: one of the two names is
  dropped, with a warning.
- An unknown key in a rule is refused, and a refused request body now says
  what is wrong instead of only `Invalid request body`.

**Existing mappings.** A stored key containing `+` or `#` used to match only a
topic spelled exactly like it, which a published topic never is; it is now a
wildcard and starts routing the topics it matches. Check stored mappings for
such keys before upgrading. A stored key that is not a valid filter (for
example `sensors/temp+x`, which no published topic can equal, or one longer
than 1024 characters) now matches nothing: the subscription keeps running and
logs a warning naming the key. An API update that leaves `topic_mapping` out is
accepted; one that sets it requires valid filters, so remove such a key with
the first edit that touches the mapping (`{}` clears it).

**Downgrading.** An earlier release cannot read a rule object: with one stored,
it fails to list or auto-start any MQTT subscription. Before downgrading,
change every rule object back to a database string, or remove it.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1402](https://github.com/Basekick-Labs/arc/pull/1402).

## Fix: cluster cache invalidation reaches peers with wildcard API addresses (#1398)

Cache-invalidation fan-out now resolves hostless and wildcard API addresses
(`:8000`, `0.0.0.0:8000`, `[::]:8000`) using the peer's coordinator host,
retaining the API port. Previously these requests could reach the sender
itself and be rejected with 403, leaving peer caches stale after compaction,
retention, DELETE or database/measurement removal. Explicit API hosts remain
unchanged. Forwarded requests and invalidation now share the same resolver.

## New: a configurable SQL length limit ([#1448](https://github.com/Basekick-Labs/arc/issues/1448))

Arc refused any query longer than 10,000 characters, a fixed limit. A query
naming many series, for example the latest value of 2,500 channels listed in
an `IN (...)`, needs far more. The limit is now `query.max_sql_length`
(env `ARC_QUERY_MAX_SQL_LENGTH`), in bytes:

```toml
[query]
max_sql_length = 200000
```

- The default stays 10000. Values from 1000 to 1048576 (1 MiB) are accepted,
  and the value must be smaller than `server.max_payload_size`, which limits
  the request body that carries the SQL. Anything else refuses to start,
  naming the key.
- It applies to every endpoint that runs SQL (JSON, MessagePack, Arrow and the
  row estimate) and to continuous query definitions. A continuous query is
  checked again at every run, so lowering the limit below a stored
  definition's length makes that query fail until it is shortened.
- Preparing a long query now takes time in proportion to its length. Before,
  restoring its string literals copied the whole statement once per literal,
  so a 1 MB query with 50,000 literals spent 9.3 s there, and a 2.3 MB one
  34 s. Both now take under 0.6 s. Validation and rewriting are linear in
  the query's length (since [#1479](https://github.com/Basekick-Labs/arc/issues/1479)),
  but their cost depends on the query's shape. At the 1 MiB ceiling one
  request costs about 1.7 s of CPU for a JOIN chain, and up to about 4.1 s
  for a comma join of many short references (`FROM x,x,x…`; see
  [#1482](https://github.com/Basekick-Labs/arc/issues/1482)). At the 10 KB
  default, about 0.2 s at worst. `query.timeout` covers that preparation
  too.
- Queries longer than 10,000 characters are not kept in the SQL transform
  cache, so a raised limit leaves the cache's memory bound unchanged.

## New: MessagePack columnar tag keys (#275)

MessagePack columnar payloads now accept an optional `tag_keys` field to
identify tag columns explicitly. Generic and typed decoding, WAL recovery
and replication preserve these names for downstream tag metadata and
compaction deduplication. Payloads without `tag_keys` remain supported.
A name that would stop compaction, such as one that is not a column of the
data, is left out with a warning, and the write succeeds
([#1258](https://github.com/Basekick-Labs/arc/issues/1258)).

Contributed by [@efegokdemir](https://github.com/efegokdemir)
in [#905](https://github.com/Basekick-Labs/arc/pull/905).

## Changed: telemetry also reports Python SDK installations

Arc's opt-out telemetry `clients` section, which lists the arcli
installations an instance served, gains a `python_sdk` entry for the
[Python SDK](https://github.com/Basekick-Labs/arc-client-python). The SDK
sends a random per-installation UUID in a new, generic
`Arc-Client-Installation-Id` header; arcli's `Arcli-Installation-Id` header
is still accepted, and the new header wins when both are present. The client
kind comes from the User-Agent (`arcli/...` or `arc-client-python/...`). Per
SDK installation Arc reports the id, the SDK version, the Python version and
the `os/arch` platform from the SDK's User-Agent (`unknown` when the
User-Agent comment is absent or malformed), and the time it was last seen.
Runtime and platform are limited to letters, digits and `. _ + / -`; anything
else is reported as `unknown`.

The same gates as for arcli apply: only requests that succeeded and, with
authentication configured, carried a valid token count; public routes never
do. Each client kind is capped at 256 distinct installations per report,
independently, with its own `truncated` flag, so containers minting a new SDK
id on every start cannot push arcli installations out of the report. The
`clients.arcli` object is unchanged. Both installation headers are stripped
when a cluster node forwards a request, so a forwarded request is counted
once, by the node that received it.

One behaviour change for arcli: a request carrying an installation id whose
User-Agent names neither arcli nor the SDK is no longer counted (it used to be
recorded with version `unknown`), so foreign traffic cannot inflate the
counts. arcli always sends its own User-Agent, so its counts are unaffected.

The SDK's own switches (`DO_NOT_TRACK=1`, `ARC_CLIENT_SEND_INSTALLATION_ID=0`,
or `send_installation_id=False`) stop it sending the id, and
`telemetry.enabled = false` / `ARC_TELEMETRY_ENABLED=false` on the node
disables the whole section. In ephemeral containers, setting
`ARC_CLIENT_INSTALLATION_ID` to a fixed UUID keeps each start from counting
as a new installation.

As with arcli, the counts are only as trustworthy as the callers: any token
holder (any caller, without authentication) can send 256 random ids with an
SDK User-Agent and fill that kind's slots for one report, which sets
`truncated`. The per-kind cap keeps that from affecting arcli's counts.

## Security: a crafted request can no longer crash Arc or exhaust its memory

**A crafted zstd write crashed Arc.** klauspost/compress, which Arc uses to
decode zstd-compressed write requests, panics on a crafted 27 KB frame
([GHSA-fpmh-frq8-7rj8](https://github.com/klauspost/compress/security/advisories/GHSA-fpmh-frq8-7rj8),
affecting every release up to 1.20.1). Arc decoded those bodies on a goroutine
of the decoder's own, where the panic could not be caught, so a single
`POST` with `Content-Encoding: zstd` (or a zstd body without the header) to
`/api/v1/write/msgpack`, `/api/v1/write/line-protocol`, `/write`,
`/api/v2/write` or `/api/v1/write/tle` ended the process. 26.09.4 fixed it
by moving to klauspost/compress 1.20.4. Arc now also decodes request bodies on
the request's own goroutine and answers `400` to a frame the decoder cannot
handle, whatever the library version. (A zstd-compressed Parquet file uploaded to the import
endpoint could hit the same panic; there it already failed only that request,
with a `500`, and it is now answered `422` like any other corrupt file.)

**A small compressed request could use gigabytes of memory.** Arc's HTTP
framework decompresses a request body sent with `Content-Encoding` (gzip,
deflate, br) without a size limit the first time a handler reads it: a 1 MB
gzip body to `/api/v1/query` made the server allocate 3.4 GB. Only the write
endpoints above decode compressed bodies, themselves and within their size
limits; every other endpoint now answers `415` to a request with a
`Content-Encoding` other than `identity` (in any of its header lines). None of
Arc's clients send one there.

**A tiny zstd frame could reserve half a gigabyte.** A zstd frame declares its
window size and the decoder allocates it up front: a 10-byte frame declaring a
512 MB window allocated 512 MB, and the pooled decoder kept it. Windows are
now limited to 32 MB. zstd levels 1 to 20 stay within it (levels 1-19 use at
most 8 MB); a body compressed with `--ultra` levels 21-22 or with `--long` is
refused with an error naming the window. Arc's own clients and Telegraf
compress at level 3 or with gzip. Decoders are also pooled per size limit, so
a msgpack payload is no longer rejected because a decoder last served an
endpoint with a smaller limit.

golang.org/x/net is at 0.61. Go 1.26.10, klauspost/compress 1.20.4 and
andybalholm/brotli 1.2.7 shipped in 26.09.4.

## Security: the query API runs read statements only

The query endpoints (`/api/v1/query` and its MessagePack, Arrow and estimate
forms) and continuous queries now accept read statements only: `SELECT`,
`WITH`, `FROM`, `VALUES`, `PIVOT`, `UNPIVOT`, `SHOW`, `DESCRIBE`, `SUMMARIZE`,
and `EXPLAIN` of one of these. Any other statement is answered `400`. A few
DuckDB functions that change or expose server-wide state are refused in
queries and in `DELETE` predicates, and the checks Arc runs on query text now
read it the way DuckDB does in more cases. A stored continuous query that is
not a read statement fails its runs from this release.

Full technical detail will accompany the corresponding security advisory once it
is published. Responsibly reported by **[@rexpository](https://github.com/rexpository)**.

## Security: more relation keywords are checked in user SQL

A string literal in table position is refused because DuckDB would read it as
a file. The check now also covers `PIVOT_WIDER`, `PIVOT_LONGER`, `DESC` and
`SHOW`, DuckDB's other spellings of `PIVOT`, `UNPIVOT` and `DESCRIBE`, which
read a file the same way (`SHOW` at the top level too, the others inside a
subquery), and `ONLY` between such a keyword and the relation
(`FROM ONLY …`). Such a query is answered `400` on every query endpoint, in
continuous queries and in `DELETE` predicates. Queries that use these keywords
on a measurement are unaffected, and a column of one of these names referenced
through a table qualifier (`c.show = 'a'`) is no longer refused.

Full technical detail will accompany the corresponding security advisory once
it is published. Responsibly reported by **[@garnetarc](https://github.com/garnetarc)**.
## Security: a malformed Parquet footer no longer stops Iceberg export

Some malformed Parquet footers make the library Arc reads them with panic
instead of returning an error. With Iceberg export on, the export pass reads
the footer of every file it publishes, including files EdgeSync spokes send,
so one such file stopped the process, and again on every restart while the
file stayed in storage. The footer reads now return the panic as an error:
the measurement holding the file fails its export pass, is logged with the
file's name, and stays blocked until the file is removed, while every other
measurement is exported. A panic elsewhere on the export goroutine is
contained to its measurement the same way and logged with its stack. Footers
over 16 MiB are refused before they are read (Arc's own are a few KiB). A
Parquet import whose footer panics is answered `422` instead of `500`, and a
schema anchor whose footer panics is treated as undecodable.

Full technical detail will accompany the corresponding security advisory once
it is published. Responsibly reported by **[@glacieraxiom](https://github.com/glacieraxiom)**.
## Security: a write with a new key on every point is refused instead of exhausting memory

Arc builds a write's columns dense: every column holds a value or a NULL for
every row. A write in which each point brought a tag or field key no other
point had built rows x rows cells from a body linear in rows, so a few hundred
KB of line protocol could hold more than a GB while it was parsed. A write may
now leave at most eight cells empty for each cell it fills, or about four
million, whichever is more, counting keys sent only as NULLs and room
reserved for the first row's columns; past that it is answered `413` and
nothing of it is written. The bound follows the values a write carries, not
its size, so padding or compressing a body buys nothing. This covers line
protocol and MessagePack rows, with or without `ingest.decimal_columns`, MQTT
messages (refused and logged at most once per topic per interval) and line
protocol imports, where the chunks before the refused one may already be
written, as for any import that stops part-way. A dense write leaves no cell
empty; a sparse one is admitted down to columns about 11% filled past the
floor. Send points with different keys as separate writes or measurements.
Parsing also builds each column at its final size once, roughly halving its
peak memory for wide batches.

Full technical detail will accompany the corresponding security advisory once
it is published. Responsibly reported by **[@glyphanchor](https://github.com/glyphanchor)**.

## Security: a cached RBAC decision can no longer apply to another database

The RBAC permission cache stored the database and measurement names of a check
without copying them. On a keep-alive connection the HTTP framework reuses the
request buffer those names point into, so a cached allow could come to match a
different name of the same length, and a token scoped by RBAC could read a
database it was not granted. The cache now stores its own copy of each name.
Deployments without RBAC roles were not affected.

The tiered-storage policy cache had the same shape for the database name of a
policy. It now stores its own copy too.

Full technical detail will accompany the corresponding security advisory once
it is published. Responsibly reported by **[@gablealder823](https://github.com/gablealder823)**.

## Security: more DuckDB functions are refused in user SQL

Some functions of the DuckDB release Arc links were not covered by the checks
Arc runs on query text, and could reach data or state outside the caller's
scope. They are now refused with `400` on every query endpoint, in continuous
queries and in `DELETE` predicates; a stored continuous query that calls one
fails its runs. `DELETE` predicates now apply exactly the checks queries do.
A test now reviews the linked DuckDB's functions so an upgrade cannot add one
unnoticed.

Full technical detail will accompany the corresponding security advisory once
it is published. Responsibly reported by **[@harsh-prajapati1312](https://github.com/harsh-prajapati1312)**.

## Security: a cluster node that took a Raft snapshot no longer keeps revoked tokens ([#1336](https://github.com/Basekick-Labs/arc/issues/1336))

**Enterprise clusters with auth enabled.** Each node checks API tokens and
RBAC grants against its own SQLite, which followed the replicated auth state
only through per-command callbacks. Two paths skipped them:

- **A Raft snapshot install.** A node that was down or partitioned long
  enough to need one, or one that joined after the leader had snapshotted,
  never applied the token and RBAC changes the snapshot covered.
- **Startup.** Entries applied before the callbacks were wired at boot
  never reached SQLite. For RBAC that window included the wait for a leader.

On such a node, until the same token changed again:

- tokens revoked or deleted elsewhere still authenticated;
- a rotated token kept accepting its old secret, and rejected the new one;
- tokens created elsewhere were rejected;
- a removed team membership, role or measurement permission still granted
  access.

Every other node was correct. Every release since 26.06.1 is affected.

Each node now rewrites its local auth and RBAC rows from the replicated state
after every snapshot install, and once at startup after it has caught up with
the leader. Until it has caught up, it retries every 15 s; reconciling against
the snapshot it booted from would bring back what was revoked since.

**What changes for operators**

- Only rows the node wrote from the replicated state are changed or removed
  (`raft_materialised`, a new column on `api_tokens` and the five `rbac_*`
  tables). The cluster's apply commands now also act only on those rows: a
  legacy or standalone-created row that happens to share an ID with a
  replicated token is no longer revoked, renamed or rotated by it.
- Legacy rows (pre-26.06.1, or created while a node ran standalone) are left
  in place and reported at `Warn` on every restart. See
  [Upgrading pre-26.06.1 API tokens](operations/pre-26.06.1-token-upgrade.md).
- A token disabled on this node but enabled in the cluster stays disabled:
  the cluster has no re-enable command, so this only happens after a local
  revoke while the node ran standalone. A token *deleted* on a node while it
  ran standalone comes back on its next reconcile if the cluster still has it:
  revoke or delete it through the cluster instead. Organizations and teams
  follow the cluster both ways, because their update command carries
  `enabled`.
- A Raft wiped and re-bootstrapped with the auth database kept is detected:
  nothing is deleted, the old cluster rows become local-only, and an `Error`
  log says so.
- Until a node's first reconcile, the cluster's RBAC updates and deletes reach
  any row at the ID, as before. From then on only rows the reconcile has
  matched to the cluster's state. Team-membership removal always applies: it
  is keyed by the token and team, which name the same grant whatever the
  row's ID.
- The new column is added at startup. A database file Arc cannot write to
  now fails startup there, not at the first token write.
- New metrics: `arc_cluster_auth_reconcile_total`,
  `arc_cluster_auth_reconcile_failed_total`,
  `arc_cluster_auth_reconcile_local_rows`,
  `arc_cluster_auth_reconcile_collisions` and
  `arc_cluster_auth_reconcile_kept_rows`.

## Upgrade note: tag and field names Arc cannot read back are refused ([#1342](https://github.com/Basekick-Labs/arc/issues/1342))

Ingest used to store three kinds of names that Arc could not read back.
Once one reached a measurement, compaction of the measurement failed (`no
'time' column`), `SELECT *` failed with `Invalid unicode`, the DELETE API
failed with `Unimplemented type for cast (DOUBLE -> TIMESTAMP WITH TIME
ZONE)`, and pyarrow and other external readers failed too:

- **A name that is not valid UTF-8**, such as a Latin-1 `°` (`0xB0`) from a
  migration tool.
- **A name equal to `time` ignoring case**, such as a field `Time` or a tag
  `TIME`. DuckDB matches column names without regard to case. In files where
  that column came before Arc's own `time` column, `time` resolved to it.
- **Two names of one measurement that differ only in case**, such as `Temp`
  and `temp`. DuckDB reads them as one column, so one spelling's data shows up
  under the other.

These names are now refused everywhere a write enters Arc:

| Path | What happens |
|---|---|
| Line protocol (`/write`, `/api/v2/write`, `/api/v1/write/line-protocol`) and msgpack rows | The point is dropped and the rest are written, as for a key named exactly `time` today. The answer is a partial-write `400` naming the measurement, the name and the reason. |
| msgpack columnar | The item is dropped whole (its names belong to all its rows) and the other items are written. Partial-write `400`. |
| CSV, Parquet and LP imports | The file is refused with a `400` before anything is written. A CSV's own time column may be called `Time` (as in Grafana's CSV export); it is renamed to `time`. |
| MQTT | The reading is kept and only the offending key is dropped, with a warning at most once every 10 minutes per name. Tasmota's `SENSOR` messages carry a `"Time"` key, which is now dropped while the rest of the reading is stored. |
| Continuous queries | A run whose output has such a column fails with an error naming it. A query that aliases a column `AS Time` now fails every run; rename the alias. |
| WAL replay and standby or stream replay | A column an older binary accepted under such a name is dropped, and the rows and their other columns are written. Logged at `Error`. |

For two names that differ only in case, the check covers both the write
itself and the measurement's existing columns, as recorded in its field
schema (`query.stable_schema`). The spelling the measurement already has
wins. The comparison against existing columns uses the schema in memory, so
right after a restart, until the measurement's schema is loaded, only the
write is checked.

**What clients see.** A client that has been sending, say, a field `Time`
(pandas DataFrames often have one) got its writes accepted until now. They
were accepted, and they broke the measurement later. Now those points are
dropped with a `400`. The answer is the same partial-write answer line protocol
gives for a key named exactly `time`.

A case-twin refusal is answered only for measurements the token may
write. For any other measurement, the check looks at the request alone, so
an answer cannot reveal which columns that measurement already has.

**Existing measurements.**
- **Field schema:** the measurement's field schema no longer lists these
  names, so reads of time ranges without such files work again. The stored
  schema is rewritten on the next flush.
- **Mixed versions:** during a rolling upgrade, a node still on an older
  release can write the old names back. The upgraded nodes then rewrite the
  schema again, every few minutes, until every node is upgraded.
- **Files already on disk:** files written with such a column are not
  rewritten. Reads and compaction of the hours they hold still fail. Arc
  cannot export them either, because the bad column breaks every query that
  touches those files. They leave with retention, or with the database when
  it is deleted. Before that, rewriting them takes a tool that reads the
  Parquet files directly and renames the column, then an import of the result.
- **Edge sync:** a spoke on an older release can still send such files to
  its hub; upgrade spokes first.

## Upgrade note: a cold tier that overlaps primary storage refuses to start ([#1105](https://github.com/Basekick-Labs/arc/issues/1105))

With `tiered_storage.enabled` and `tiered_storage.cold.enabled`, Arc now
refuses to start when the cold tier is the same location as primary
storage, or one is inside the other (the same bucket with equal or nested
prefixes), for every backend. This is a new check; nothing compared the two
before. At the same location a file and its cold copy have one key, so
migration copied the file onto itself and then deleted it: data loss.
Nested, each tier's listing took in the other's objects and registered
them as files of a database that does not exist. Move the cold tier to
another bucket or to a prefix that is not a parent or child of the
primary's.

## Upgrade note: a query that needs an unavailable cold tier answers 503 ([#1105](https://github.com/Basekick-Labs/arc/issues/1105))

When the cold tier is enabled but its backend failed to start (a bad
endpoint or credentials file, for any backend), a query whose time range
reaches cold-tier files that this node's tier metadata records now answers
HTTP 503 naming the cold tier. A node whose tier metadata holds no cold
rows (a fresh or rebuilt one, which cannot rebuild them while the cold tier
is down) still answers from the hot tier alone. It used
to leave the cold files out and answer with the hot rows alone, as a
success. Queries that need no cold data are served as before, and the
failed cold tier is absent from `/health` (it was reported from a secret
nothing read through).

## Upgrade note: packaged installs keep backups in `/var/lib/arc/backups` ([#1463](https://github.com/Basekick-Labs/arc/issues/1463))

Applies to the .deb, .rpm and Arch packages. The container image, Helm charts
and Kubernetes manifests are unaffected.

The packages' systemd unit runs Arc from `/var/lib/arc` with storage at
`/var/lib/arc/data`. The packaged `arc.toml` puts backups at `./data/backups`,
which from there is `/var/lib/arc/data/backups`, inside the storage root. The
new overlap refusal (next note) rejects that, so without this fix a packaged
install did not start: a fresh install and an upgrade both crash-looped under
systemd. The unit now also sets
`ARC_BACKUP_LOCAL_PATH=/var/lib/arc/backups`, and Arc creates that directory on
first start.

The unit sets the variable rather than the packaged `arc.toml`, because an rpm
or Arch upgrade keeps an existing `/etc/arc/arc.toml`. The same environment
variable also beats the file:

- **If you changed `backup.local_path` in `/etc/arc/arc.toml`,** the unit now
  overrides it, and Arc logs a warning at startup naming both paths. All three
  packages now keep your `arc.toml` across the upgrade (the .deb from this
  release on, see the next note); an edit that an earlier .deb upgrade already
  reset is gone and draws no warning. Keep your directory with a drop-in:

  ```
  sudo systemctl edit arc
  # add:
  [Service]
  Environment="ARC_BACKUP_LOCAL_PATH=/your/backup/dir"
  ```

  A drop-in survives upgrades. (`storage.local_path` already worked this way on
  packaged installs.)

- **Backups taken by an earlier release are still in
  `/var/lib/arc/data/backups`**, whatever the primary storage backend: an
  install with S3, Azure or GCS primary storage started before this fix and
  wrote its backups there. Arc logs a warning at startup when it finds them.
  The backup API does not list them until they are moved. With local primary
  storage they are also inside the storage root, so:
  - the next backup copies their data files as data;
  - on a cluster with reconciliation enabled and `manifest_only_dry_run` off,
    the sweep deletes their data files, leaving backups that look complete and
    cannot be restored;
  - each of them already holds a copy of the ones taken before it.

  Move them before starting 27.01.1. The loop moves only directories that
  contain a `manifest.json`, so a database named `backups` stays where it is:

  ```
  sudo systemctl stop arc
  sudo mkdir -p /var/lib/arc/backups
  sudo sh -c 'for d in /var/lib/arc/data/backups/*/; do
    if [ -f "$d/manifest.json" ]; then mv "$d" /var/lib/arc/backups/; fi
  done'
  # Fails, harmlessly, if anything is left: a database named backups, or a
  # partial backup with no manifest.json. Inspect what is left before deleting it.
  sudo rmdir /var/lib/arc/data/backups
  sudo chown -R arc:arc /var/lib/arc/backups
  sudo chmod 700 /var/lib/arc/backups
  ```

  With local primary storage, restoring one of these older backups with
  `restore_data` writes the data files of the backups nested in it back to
  `/var/lib/arc/data/backups/<id>/data/...`, without their `manifest.json`.
  Arc does not warn about those; delete them by hand.

- **After upgrading a .deb or .rpm from 26.09.x, re-enable the service.** The
  26.09 package's remove script stops and disables `arc.service` on every
  upgrade, and on an upgrade that script comes from the old package. Run
  `sudo systemctl enable --now arc` after the upgrade. From this release on,
  the deb and rpm scripts stop and disable the service only on removal, and
  an upgrade restarts a running service with the new binary. Arch upgrades
  never stopped the service and still do not restart it.

The release pipeline now installs the .deb, starts it under systemd and
reinstalls it over itself to check the service survives an upgrade, and it
boots the Arch package with its unit's user, working directory and environment.
Before, every boot test ran the binary from a directory of its own choosing,
and none of them saw the packaged layout. The three packages now share one unit
file, `deploy/systemd/arc.service`.

## Fix: .deb upgrades keep `/etc/arc/arc.toml` ([#1465](https://github.com/Basekick-Labs/arc/issues/1465))

Every .deb upgrade used to replace `/etc/arc/arc.toml` with the stock file,
discarding the operator's edits (auth, retention, credentials, ports) without
a prompt or a backup copy. The rpm and Arch packages already kept the file.

From this release the .deb ships the stock config at `/usr/share/arc/arc.toml`
and installs it at `/etc/arc/arc.toml` only when that file does not exist. The
content of an existing file is never changed, including on the upgrade from
26.09.x: the config you have now is the one 27.01.1 starts with. A symlinked
`/etc/arc/arc.toml` stays a symlink. As before, the package sets the file's
owner to `arc:arc`. Keys that are not in your file take Arc's built-in
defaults, so a config from an older release keeps working; diff it against
`/usr/share/arc/arc.toml` to see what the new stock file documents.

Upgrades never prompt, so non-interactive installs (`apt-get install -y`,
configuration management) keep working without `--force-confold`. The file
is not a dpkg conffile: an edited conffile prompts on every upgrade whose
stock file changed, which is nearly every release, and the upgrade from 26.09.x
failed non-interactively even for an unedited file.

- `dpkg --remove` leaves `/etc/arc/arc.toml`. `dpkg --purge` removes it,
  unless the other variant (`arc` or `arc-fips`) is installed and still uses
  it.
- `dpkg -S /etc/arc/arc.toml` no longer names a package, because no package
  owns the file.
- Downgrading to 26.09.x or earlier replaces `/etc/arc/arc.toml` with that
  release's stock file, as every upgrade used to. Back it up first.

## Fix: backups include, and restores replace, the config file Arc actually loaded ([#1466](https://github.com/Basekick-Labs/arc/issues/1466))

`include_config` and `restore_config` used `arc.toml` in the working
directory. Arc finds its config through a search path (the working directory,
then `/etc/arc/`, then `~/.arc/`), so on any install whose config lives
elsewhere both were wrong. On the .deb/.rpm/Arch packages the working
directory is `/var/lib/arc` and the config is `/etc/arc/arc.toml`:

- A backup never held the config. The backup succeeded, and the only sign
  was a `Failed to backup config file` warning in the log.
- A config restore wrote `/var/lib/arc/arc.toml`. The working directory is
  searched first, so from the next start that file was the config, and edits
  to `/etc/arc/arc.toml` had no effect.

Both now use the file Arc loaded at startup. The startup line
`Backup/restore enabled` shows it as `config_file`, and `config_restore`
says whether this node can restore a config at all.

**Backups on a package install now hold the config, and its credentials.**
`include_config` defaults to true for a backup to a local target, so every
default backup now contains `/etc/arc/arc.toml`, with any S3, Azure or GCS
keys and the cluster shared secret in it. Before, these backups silently held
no config. Treat the backup directory as secret, or send
`"include_config": false`.

**`restore_config` is refused with 400 before anything is restored** when
the backup holds a config and the node cannot write it. The config step runs
last, so where the write failed (a read-only ConfigMap, for example) the
restore used to fail after the data had been restored. It is refused when:

- Arc started without a config file. Arc logs a warning at startup, and each
  backup logs that it holds no config.
- The config file is not writable by the Arc process. This covers the rpm and
  Arch packages, where `/etc/arc/arc.toml` is root's, and a Kubernetes
  ConfigMap mount, which is read-only.
- Arc cannot create the safety copy next to the config file. On the .deb,
  `/etc/arc` is root's.

**This changes what `restore_config` does on a package install.** It used to
"work" by writing the shadow file described above; it is now refused. Restore
the config by hand from `<backup_id>/config/arc.toml` in the backup instead.
Docker is unaffected: the config there is `/app/arc.toml`, in the working
directory. Note that it is in the container layer, not the `/app/data` volume,
so a restored config does not survive recreating the container.

Before overwriting, Arc copies the current config to
`<config>.before-restore-<backup_id>`. A failed copy now fails the config
restore, instead of logging a warning and overwriting anyway. An existing copy
is never replaced, so neither a restore of another backup nor a retry of the
same one overwrites the copy of the config it first replaced.

**If you ran `restore_config` on a package install before this release,**
`/var/lib/arc/arc.toml` is still your config, and `/etc/arc/arc.toml` is
ignored. Arc now logs a warning at startup naming both files. Merge what you
need into `/etc/arc/arc.toml`, then remove `/var/lib/arc/arc.toml`. The
storage and backup paths are set by the systemd unit and override either file.

## Fix: `IS [NOT] DISTINCT FROM` works in queries ([#1472](https://github.com/Basekick-Labs/arc/issues/1472))

Arc read the `FROM` in `x IS [NOT] DISTINCT FROM y` as the start of a table
reference, so standard SQL using the operator failed:

- A literal operand (`host IS DISTINCT FROM 'a'`) was refused with "String
  literal not allowed in table position".
- A column or `NULL` operand was rewritten into a table scan, and DuckDB
  failed with `Binder Error: Function "read_parquet" is a table function`.
- With RBAC on, the operand was checked as a table, so a query could be
  refused for a grant it does not need.
- A comma after the operand (`SELECT a IS DISTINCT FROM b, c FROM cpu`)
  turned `c` into a cross-join table.
- `DELETE … WHERE host IS DISTINCT FROM 'a'` was refused.
- Time-range pruning skipped queries that used the operator.

All of these now treat the operator as a comparison. The same change fixes a
literal after the `FROM` of a function form, such as
`trim(BOTH FROM '  x  ')`, which was refused the same way.

Arc also no longer reads a parenthesis or `FROM` inside a SQL comment as part
of `EXTRACT`, `SUBSTRING`, `TRIM` or `OVERLAY`. A comment such as
`/* trim( */` could previously make a real table reference after it invisible
to the RBAC check and to the query rewrite, and the query then failed.

The scan behind all of this is now one linear pass. It used to rescan each
line once per keyword: at the default `query.max_sql_length` (10 KB) a
crafted single-line query cost tens of milliseconds, and with the limit raised
to 1 MiB it cost minutes. It now costs milliseconds at 1 MiB.

Two spellings still fail as before, because Arc reads them as one word:
an operand glued to the keyword (`IS DISTINCT FROM'a'`, `'a'IS DISTINCT FROM
x`), and a non-ASCII space between the words. Put an ASCII space there.

The workaround for earlier releases is `(x != 'a' OR x IS NULL)`.

## Fix: a long comma join is prepared without rescanning its own rewrite ([#1482](https://github.com/Basekick-Labs/arc/issues/1482))

Before running a query, Arc rewrites each table reference into a read of its
files, in several passes. The comma-join pass ran first, and its output is
about 30 times as long as the references it replaced, so every later pass
scanned tens of MB for a 1 MiB query. Each pass now leaves a one-byte marker
and the reads are put in once at the end, so every pass scans text about as
long as the query. A whole request at the 1 MiB `query.max_sql_length`
ceiling, before and after, measured the same way (in-process, one
machine; earlier figures for these shapes came from a different setup):

| Query (1 MiB) | Before | After |
|---|---|---|
| Comma join, `SELECT 1 FROM x,x,x,…` | 14.3 s | 4.1 s |
| Comma join, `SELECT 1 FROM x, x, x, …` | 9.8 s | 2.9 s |
| `SELECT 1 FROM x from x from …` | 3.4 s | 2.3 s |
| `JOIN x USING (a)` repeated | 1.7 s | 1.7 s |

The rest is validation and DuckDB's own parse. At the 10 KB default the worst
shape goes from 0.31 s to 0.20 s.

- `query.timeout` (or a governance policy's maximum duration) now bounds the
  whole request, preparing the query included. Before, preparation ran
  outside it and could not be stopped. A request whose deadline passes while
  its query is prepared is answered `504 Query timed out`, as one that times
  out while running is, on every query endpoint (JSON, MessagePack, Arrow,
  the row estimate and `GET /api/v1/query/{measurement}`).
- A query containing a NUL byte is refused with 400, on every query
  endpoint. DuckDB stops reading a statement at the first NUL, in a string
  literal or anywhere else, so such a query used to run cut short at that
  byte: a different query from the one Arc validated. Continuous query
  definitions are checked too, at creation and at every run, so a stored
  definition containing a NUL now fails instead of running cut short.
- A storage path that contains `from` or `join` followed by a word is no
  longer rewritten inside the read Arc emits for it.

## Fix: joins of two measurements bind unqualified columns ([#1483](https://github.com/Basekick-Labs/arc/issues/1483))

A join of two measurements failed with `Binder Error: Ambiguous reference to
table "read_parquet"` whenever DuckDB had to resolve an unqualified column or
`*` across them, with or without an `x-arc-database` header:

```sql
SELECT count(*) FROM cpu JOIN mem USING (time, host) WHERE host = 'a';
SELECT * FROM cpu JOIN mem ON cpu.host = mem.host;
SELECT * FROM cpu, mem;
```

DuckDB names every unaliased file read `read_parquet`, so the two sides had
the same name. Arc gave a table its own name only when the query qualified a
column with it (`cpu.host`, since
[#1447](https://github.com/Basekick-Labs/arc/issues/1447)). Now, when one
FROM clause reads two or more measurements, each is named after itself,
unless:

- you gave it an alias, which it keeps;
- the query already uses that name for something else: a CTE, another
  relation, a column, a SELECT-list alias (`SELECT x AS mem … GROUP BY mem`),
  or an outer query's column. DuckDB would bind the name to the table first,
  so that measurement keeps its old naming. If the join is then still
  ambiguous, give it an alias.

Queries reading one measurement per FROM clause, including those with
subqueries over another, are rewritten exactly as before.

Joining a measurement to itself, or to the measurement of the same name in
another database, still needs an alias on at least one side
(`FROM cpu a JOIN plant.cpu b USING (time)`): both sides have the same name
either way.

## Fix: a query naming many tables no longer takes quadratic time to prepare ([#1479](https://github.com/Basekick-Labs/arc/issues/1479))

Before running a query, Arc rewrites each table reference into a read of its
files and works out which time partitions that read needs. For every
reference it re-read the whole query: its time range, whether that range is
relative to `now()`, and a hash for the partition cache. None of these depend
on the table. A query made of many short references therefore took time
quadratic in its length. Any user with query rights could hold a CPU core
with one request:

| Crafted query (whole request) | 10 KB (default limit) | 20 KB |
|---|---|---|
| Comma join, `SELECT 1 FROM x,x,x,…` | 9.8 s → 0.21 s | |
| Comma join, `SELECT 1 FROM x, x, x, …` | 5.6 s → 0.15 s | 21.8 s → 0.29 s |
| `SELECT 1 x from x from …` | 1.6 s → 0.06 s | 6.6 s → 0.10 s |

Arc now reads them once per query and shares them across its references.
Results are unchanged. A time range is only ever found for a single-table
query, so no query's pruning differs.

## Fix: a backtick in SQL is refused with a clear error ([#1480](https://github.com/Basekick-Labs/arc/issues/1480))

DuckDB does not quote identifiers with backticks; outside quoted text a
backtick is an operator character, so `` SELECT `value` FROM cpu `` has never
run. Arc's own checks on query text, however, read backticks as quotes, so
Arc and DuckDB could disagree about what a query said. One result was the
reported query, which answered `success: true` with no columns instead of an
error.

A backtick outside a string literal, a `"…"` identifier or a comment is now
refused with `400` and `backticks are not identifier quotes in DuckDB; quote
identifiers with double quotes ("name")`. This applies to every query
endpoint, continuous queries, the `where` parameter of
`/api/v1/query/:measurement`, and `DELETE` predicates. Arc's checks now read
the query exactly as DuckDB will. `` SHOW TABLES FROM `mydb` `` still works,
because Arc answers it itself. A backtick inside a literal or a quoted
identifier is unaffected.

Two related fixes:
- **Stream takeover `DELETE` with a backtick in a string literal.** With
  stream takeover on, a `DELETE` whose predicate had both a backtick in a
  string literal and a time function such as `now()` (`WHERE msg = 'a`b' AND
  time < now()`) ran with the backtick turned into a double quote, so it
  matched the wrong rows. It now runs as written.
- **Stored `DELETE` records.** A record applied to a pending file is checked
  again before it runs. A record the current checks refuse is skipped, so
  the rows that `DELETE` removed elsewhere remain in that file. The error in
  the log names the file and the predicate; issue the `DELETE` again with a
  predicate the current checks accept.

## Fix: continuous queries rewrite only the source reference itself ([#1487](https://github.com/Basekick-Labs/arc/issues/1487))

Before a continuous query runs, Arc replaces its `FROM <database>.<measurement>`
with a read of the measurement's files. It found that text anywhere in the
query, including inside a string literal, a quoted identifier or a comment,
and rewrote those occurrences too. In a `'…'` literal that made the query fail
to parse. In a dollar-quoted string (`$$…$$`) or a quoted identifier, the query
ran, and the storage path ended up inside the string value or the output
column's name. Arc now rewrites only a real table reference and leaves
everything else exactly as written.

Related fixes:
- A comment between `FROM` and the name (`FROM /* c */ db.cpu`,
  `FROM -- c` then a newline and `db.cpu`) no longer stops the rewrite. Such
  queries used to fail with `Table with name "db.cpu" does not exist`.
- A query with a CTE named after its source measurement now runs, e.g.
  `WITH cpu AS (SELECT … FROM db.cpu …) SELECT … FROM cpu`. Before, Arc
  skipped the rewrite for it, and the query failed with `Table db.cpu does not
  exist`.

## Fix: SQL checks read three more string spellings as DuckDB does

Arc masks string literals before its read-only checks run, so anything the
masker counts as a literal is invisible to them. Three spellings were masked
differently from how DuckDB reads them, which could leave part of a statement
outside a literal for the checks while DuckDB kept it inside one, or the
reverse:
- **A dollar-quoted tag with a non-ASCII letter** (`$é$…$é$`) now opens a
  string. DuckDB accepts these tags, as Postgres does.
- **`e'…'` after a name that contains `$`.** In `a$e'…'` the `e` belongs to
  the name, so the quote opens a plain string, not an escape string.
- **An escape string continued across a newline** (`E'a'` newline `'b'`, which
  DuckDB joins into one string) is now read as one string, so a backslash in
  the continued part still escapes and the string ends where DuckDB ends it.

This covers every query endpoint, continuous queries and `DELETE` predicates,
and the masking of SQL in logs. A query that does not use these spellings is
unaffected.

## Upgrade note: three configurations now refuse to start

Read this before upgrading if you set `storage.local_path`,
`backup.local_path` or `compaction.temp_directory`. The first two refusals apply
only when `backup.enabled` is true, which is the default, and both are decided
on **resolved absolute** paths: relative spellings, trailing slashes and
symlinks are normalised first, and in the container image the working directory
is `/app`. The third applies independently of `backup.enabled`.

**1. The backup destination and the primary storage root may no longer be the
same directory, and neither may contain the other.** With `backup.local_path`
left at its default `./data/backups`, that means Arc refuses to start if
`storage.local_path` is `/app/data`, `/app`, `.`, `data`, `./data`, or empty. In
practice the one realistic break is a single-volume deployment that points
`ARC_STORAGE_LOCAL_PATH` at the whole mount instead of a subdirectory of it.

Sibling names are fine: `./data/arc` beside `./data/arc-backups` starts
normally. Different kinds never overlap, so a local backup directory with S3
primary storage starts normally. The cold-tier half of the check applies only
when both `tiered_storage.enabled` and `tiered_storage.cold.enabled` are on.

**2. `backup.local_path` may no longer be empty or whitespace-only unless a
backup target is configured.** That combination previously started with the
backup API silently disabled. An empty *environment variable* still falls
through to the default, so only an empty value in the config file, or a stray
space, triggers this.

**3. `storage.local_path` and `compaction.temp_directory` may no longer contain
a glob metacharacter** — one of `*`, `?`, `[`, `]`, `{`, `}`. All six are legal
in a POSIX directory name, so `/data/arc[prod]` was an accepted configuration
before this release.

It cannot stay accepted. Compaction interpolates a full filesystem path into
DuckDB's `read_parquet()`, which reads its argument as a **pattern**: with a
metacharacter in the path, Go's literal `os.Open` and DuckDB can resolve the
same string to **different files**, so a job could validate one file and read
another, then delete the inputs it never read. The storage root is checked for
the local backend only, since that is the only one whose paths reach a local
glob; `compaction.temp_directory` is checked whenever compaction is enabled,
because inputs from object storage are streamed into it and read back from
there.

To fix: rename the directory. The refusal names the offending character.

Arc also refuses such a path per-job as defence-in-depth, but that is not where
an operator should find out: every input would fail identically on every cycle,
so compaction would never progress while the logs showed jobs completing.

To fix either: move one path outside the other, point `backup.default_target` at
a configured target, or set `backup.enabled = false`, which skips the check
entirely.

**The container image, Helm charts and Kubernetes manifests are unaffected.**
The stock `arc.toml` uses `./data/arc` and `./data/backups`; the Helm charts and
Kubernetes manifests set `/app/data/storage` or `/app/data/arc` and leave
`backup.local_path` at its default. **The .deb, .rpm and Arch packages were
affected**, and their systemd unit now sets the backup directory itself: see
the packaged-installs note above.

Why the refusal exists: a backup destination inside the storage root makes each
backup copy the previous one, so the inventory compounds every run. Worse, those
copies look like ordinary data files to the reconciliation sweep, which deletes
them as orphans when reconciliation is enabled and dry-run is off. The check
costs one comparison at startup and removes a class of silent data loss.

## Upgrade note: a node that cannot list its cold bucket stops migrating

If you run tiering with a cold tier, check that the cold credentials allow
**listing** the bucket or container, not only reading, writing and deleting
objects.

From this release every node with a usable cold tier reads the cold listing at
the start of each tier scan, and a scan whose cold listing fails skips migration
and orphan reconciliation for that cycle — see "Cold-tier metadata is rebuilt on
standalone nodes". On clustered nodes both halves already applied. On a
**standalone** node neither did, so a deployment whose cold policy grants
`GetObject`, `PutObject` and `DeleteObject` but not `ListBucket` has been
migrating happily and will stop after upgrading: hot storage keeps growing and
the cycle logs

> Cold tier could not be listed, so migration and orphan reconciliation are
> skipped this cycle and hot storage will keep growing until a listing succeeds.

once per cycle. `GET /api/v1/tiering/status` reports it as
`last_scan.cold_sync_failed`.

The same halt is reachable without a permissions problem: the cold listing is
spent from `tiered_storage.scan_timeout`, before the hot walk gets any of it. If
you sized that budget from how long a hot scan takes — which is what `arc.toml`
suggested — a large cold bucket can now exhaust it. Raise it above the time a
full cold listing *and* a full hot scan take together.

Nothing changes for a node with `tiered_storage.cold.enabled` off, or one whose
cold backend failed to construct: the listing is skipped and migration runs as
before.

## Upgrade note: an isolated primary writer stops accepting writes ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

This applies to Pattern 1 clusters (local storage) with more than one writer.

A writer cut off from the cluster used to keep acknowledging writes. Ingest was
never gated on being the primary; only `/ready/write` was. Meanwhile failover
could promote another writer. The isolated writer's rows then reached the rest
of the cluster only after the partition healed, or never if its disk failed
first. A writer also accepted writes whenever they reached it while it was a
standby, including after it had been replaced as primary.

A writer now refuses writes with `503` and `Retry-After: 5` when:
- **another writer is the designated primary.** That covers standby writers, and
  a former primary after failover or a manual hand-over. It needs no failover
  licence.
- **it lost contact with the cluster.** It is not the Raft leader and has not
  heard from the leader for longer than half of `cluster.health_check_interval`
  × `cluster.unhealthy_threshold` (7.5 s by default, at least 3 s). This rule
  needs automatic writer failover (`cluster.failover_enabled` and the
  `writer_failover` licence feature), at least three writers, and Raft. Failover
  takes longer to notice a lost primary: about 30 s at defaults.

  Once contact returns, writes resume only after contact has held for 2 s and
  the writer has caught up with the cluster log. That way a writer that was
  replaced while it was away refuses writes instead of acting on its stale view.

The refusal happens before anything is written, the WAL included, so a client
retry is safe. A line-protocol or import request that spans several
measurements is checked once up front. A fence closing in the middle of such a
request can still leave its earlier measurements written.

`/ready/write` reports not-ready while a writer refuses writes.
`arc_cluster_ingest_fenced` is 1 while a writer has lost contact; a standby
refusing writes is normal and does not set it.
`arc_cluster_ingest_fenced_writes_total` counts refused writes, by `reason` (see
the bug fix for [#1268](https://github.com/Basekick-Labs/arc/issues/1268)).

MQTT subscriptions on a writer that refuses writes are suspended: disconnected
without unsubscribing. With `clean_session=false` the broker queues QoS 1 and 2
messages until writes resume. With `clean_session=true`, or QoS 0, messages
published meanwhile are lost. Because standby writers always refuse writes, run
MQTT subscriptions on the primary. On a cluster node, MQTT subscriptions now
start after the cluster coordinator.

`cluster.ingest_fence_after_ms` overrides the contact delay. It is capped at
`health_check_interval` × `unhealthy_threshold` and raised to three election
timeouts. Size client retry buffers for the failover time. The contact rule
watches Raft. A network failure that blocks only the Raft port, and not the
cluster heartbeat port, fences the primary while failover sees it as healthy:
writes stop until Raft connectivity returns. Edge-sync bundles received by a hub
write directly to storage and are not fenced.

## Upgrade note: Raft pre-vote ([#1272](https://github.com/Basekick-Labs/arc/issues/1272))

Arc 27.01.1 moves to hashicorp/raft v1.7.3 and runs the Raft pre-vote round by
default. A writer that cannot hear the leader no longer raises its term on
every election timeout. Before, when the leader reached it again, the higher
term made a healthy leader step down: restarting one standby writer cost the
cluster a leader change.

**Rolling upgrade from an earlier release.** A node running an older release
does not answer pre-vote, and an upgraded node counts that as a refusal. While
the cluster runs both versions, if the leader dies and the two survivors are one
old and one upgraded node with the more recent log, neither becomes leader until
the restarted node rejoins on 27.01.1. Upgrade writers one at a time, and wait
for each to rejoin before the next. To rule the window out completely, set
`cluster.raft_pre_vote = false` (environment `ARC_CLUSTER_RAFT_PRE_VOTE=false`)
on every node for the upgrade, then remove it (one more rolling restart) once
every node runs 27.01.1.

During the mixed window, old nodes log `failed to decode incoming command ...
unknown rpc type` and upgraded nodes log `failed to make requestVote RPC` when
an election runs. Both stop once every node is upgraded.

A leadership transfer now returns only after the leader actually stepped down
(up to one election timeout), and reports a timeout if it did not.

## Upgrade note: compaction commit stays on once enabled ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

A cluster enables compaction commit by itself once every node runs this
release (`compaction_commit.enabled` in `GET /api/v1/cluster`). The flag stays
on, and a node running an earlier release can no longer join.

Do not roll a node back to an earlier release in place after that. The earlier
release skips the new cluster log commands and ignores their checks, so its
view of the cluster manifest silently diverges from the other nodes. A node on
an earlier release is refused at join whether or not its data directory was
wiped, so the cluster can go back to an earlier release only by restoring the
whole cluster from a backup taken before the upgrade. Removing a node, wiping
its data directory and joining it fresh is for a node on this same release.

## Upgrade note: cluster-wide tiering policies ([#1324](https://github.com/Basekick-Labs/arc/issues/1324))

A cluster turns on cluster policies by itself once every node runs this
release (`cluster_policies.enabled` in `GET /api/v1/cluster`). Like compaction
commit, the flag stays on, and a node running an earlier release can no longer
join, wiped or not. Roll back the same way: only by restoring the whole
cluster from a backup taken before the upgrade, never one node in place.

Right after that, the cluster takes over the tiering policies each node holds
in its own database. During the takeover a tiering policy write answers `503`
for up to a few seconds; retry it.

Every node must answer, or nothing is taken over:

- The leader first checks that it can reach every node. If one is down, it
  takes nothing over, so writes are never held, and the policies stay on each
  node exactly as before.
- It tries again after 30 seconds, doubling the wait after each failed attempt
  up to 10 minutes.
- `cluster_policies.kinds.tiering` shows `missing_nodes`, `last_error` and
  `failed_attempts`.
- A node that is gone for good must be removed from the cluster before the
  policies can be taken over.

The policies are merged in the direction that moves less data. A database that
is `hot_only` on any node stays `hot_only`. Otherwise it keeps the longest hot
age any node applied to it: the policy's `hot_max_age_days`, or that node's
`default_hot_max_age_days` where the policy left it unset. Afterwards:

- every node reports the same policies;
- `GET /api/v1/tiering/policies` lists them all from any node;
- every node that runs tiering keeps its own `tiering_policies` table equal to
  them, so a backup of such a node carries the current policies.

## Upgrade note: cluster-wide retention policies ([#1324](https://github.com/Basekick-Labs/arc/issues/1324))

Retention has its own flag, `cluster_policies.retention_enabled` in
`GET /api/v1/cluster`. A cluster turns it on by itself once every node runs
this release; until then the leader's `retention_blocking_nodes` names the
nodes still on an earlier one. Like the tiering flag it stays on, and a node running an
earlier release can no longer join. Roll back the whole cluster, never one
node in place.

Then the leader reads every node's own retention policies. A retention policy
deletes data, so the leader takes them over by itself only when:

- every node answers;
- no policy name has two different definitions on two nodes;
- every active policy has already completed a run on some node since it was
  last changed. Inactive policies are taken over as they are.

A policy that never ran is never activated without an admin choosing it: it
may have been created on a node that never runs retention.

Otherwise nothing changes: each node keeps its own policies and the primary
runs its own, as before. `cluster_policies.kinds.retention.last_error` says
why, and the leader checks again after 30 seconds, doubling the wait up to 10
minutes. To choose a node's policies, an admin calls, on the Raft leader:

- `GET /api/v1/cluster/policies`: every node's own policies, with a hash of
  each node's set;
- `POST /api/v1/cluster/policies/adopt` with
  `{"kind": "retention", "node": "<id>", "hash": "<hash>"}`. The policies are
  taken over only if the node still holds exactly the ones the hash names.

Both are admin-only; once `retention_enabled` is on, any other node answers
`409` with the leader's ID. `POST` answers `202`, and
`cluster_policies.kinds.retention` shows the outcome. A node where retention is
disabled counts too: its database may still hold the policies of when
retention ran there.

When the policies are taken over:

- **IDs.** Policies that all come from one node keep their IDs, as long as no
  other node ever issued those IDs. Otherwise every policy gets a new ID above
  any ID a node held, and the old IDs answer `404`. An old ID never names a
  different policy.
- **The other nodes' policies.** A policy the cluster did not take over is
  moved, with its execution history, to `retention_policies_dormant` and
  `retention_executions_dormant` in that node's database. It never runs again.
- **Writes wait a few seconds.** While the leader reads the nodes' policies, a
  retention write answers `503`; retry it.
- **History stays per node.** Execution history stays on the node that ran the
  policy. After a failover the new primary shows none until it runs it.
- **Restoring a backup.** In a cluster, restoring `arc.db` from a backup no
  longer restores retention policies: the cluster's policies win, and the
  backup's own rows go to the dormant tables. A full disaster recovery onto a
  fresh Raft cluster takes the restored policies over again, with new IDs,
  because every node then holds the same rows.
- **A restarted node waits.** A node whose own table already holds the
  cluster's policies answers retention requests with `503`, and runs nothing,
  until it has caught up with the cluster after a restart (seconds).
- **Backups carry them.** Every node that runs retention keeps its own
  `retention_policies` table equal to the cluster's policies.

A retention database of its own (`retention.db_path` set to a file other
than the auth database) now opens like the auth database: foreign keys on,
WAL journal, and a 5-second busy timeout.

## Upgrade note: cluster-wide continuous queries ([#1324](https://github.com/Basekick-Labs/arc/issues/1324))

Continuous queries have their own flag, `cluster_policies.cq_enabled` in
`GET /api/v1/cluster`, turned on by the cluster once every node runs this
release (the leader's `cq_blocking_nodes` names the rest). Like the other
policy flags it stays on: roll back the whole cluster, never one node in place.

Then the leader reads every node's own continuous queries and takes them over
by itself when every node answers, no name has two definitions, and every
active query has completed a run on some node. Each query keeps the newest
`last_processed_time` any node holds for the same definition. Otherwise
nothing changes until an admin chooses a node's queries, with the same
endpoints as retention: `GET /api/v1/cluster/policies?kind=cq` and
`POST /api/v1/cluster/policies/adopt` with `"kind": "cq"`. IDs, dormant tables
(`continuous_queries_dormant`, `continuous_query_executions_dormant`), the
`503` while the queries are read, and a restarted node's wait work as for
retention policies.

Once the cluster has adopted continuous queries, each run on the primary
(scheduled or manual) writes its window's end to the cluster: one Raft entry,
two when the primary is not the Raft leader (it first confirms it holds the
latest watermark). A manual run's response says what happened to the
watermark in `watermark_status`.

A continuous query database of its own (`continuous_query.db_path` set to a
file other than the auth database) now opens like the auth database: foreign
keys on, WAL journal, and a 5-second busy timeout.

## Upgrade note: the live WAL replication stream is removed ([#888](https://github.com/Basekick-Labs/arc/issues/888), [#1181](https://github.com/Basekick-Labs/arc/issues/1181))

This applies to clusters where readers or compactors run with both
`wal.enabled` and `cluster.replication_enabled`. With default values the Helm
chart enables the WAL only on writer pods, so a chart-deployed cluster never ran
the stream on those roles and nothing changes for it. The manifests in
`deploy/kubernetes-local` did enable it on readers.

**What changed.** Writers no longer run the WAL replication sender, and
readers and compactors no longer receive from it: the stream is removed. Its
`cluster.replication_lag_limit`, `cluster.replication_buffer_size` and
`cluster.replication_ack_interval` settings are ignored if set.

**Why.** The stream reached only readers and compactors. Each applied what it
received to its own ingest buffer and flushed it to its own Parquet. Queries
read every file on the node, so a reader returned every replicated row twice:
once from its own flush and once from the copy it pulled from the primary. A
compactor also registered its copy in the cluster manifest, so every node pulled
it, the primary included, and the duplicates showed up cluster-wide. Neither
role can be promoted, so the stream gave failover nothing in exchange. Standby
writers hold the primary's unflushed window through a separate stream that
writes no second copy
([#1207](https://github.com/Basekick-Labs/arc/issues/1207)).

**What does not change.** `cluster.replication_enabled` still starts peer file
replication. Readers and compactors get every row by pulling the primary's
registered Parquet, as they already did for anything flushed. A reader sees a
new row once the writer flushes it (`ingest.max_buffer_age_ms`) and the file has
been pulled.

**Metrics.** `arc_replication_lag_entries`, `arc_replication_lag_seconds` and
`arc_replication_entries_dropped_total` are removed, and
`replication_entries_dropped_total` from the JSON metrics. They described the
stream only, not peer file replication; remove alerts built on them.

**Upgrade order.** Upgrade readers and compactors first, and stop them
gracefully. A graceful stop flushes and purges what they received. After a crash,
the first boot replays those received entries once more and writes one more
duplicate copy. A reader that is still on the old version keeps dialling the
writers, and every few seconds until that reader is upgraded an upgraded writer
refuses it, closes the connection and logs

> Replication sync rejected: this release has no WAL replication stream;
> upgrade the reader

**Duplicates already on disk.** Which copies exist depends on the role that
wrote them.

- **Compactors, and readers that were ever Raft leader.** These copies are
  registered in the manifest, so every node pulled them. Readers could vote, and
  so lead, before v26.09.2. A copy that has not been compacted yet is listed in
  `GET /api/v1/cluster/files` with the compactor or reader as `origin_node_id`
  and a filename that does not contain `_compacted`. After compaction, the copy
  and the primary's original are merged into one `_compacted` file.
  Line-protocol rows are deduplicated in that merge when the primary's original
  was compacted in the same job. Msgpack rows never are, so their duplicates are
  inside the compacted files and need to be rewritten.
- **Other readers.** Their copies exist only on their own disk, outside the
  manifest, and nothing deletes them: retention, deletes and cold migration do
  not reach them. Reconciliation finds them as orphan storage. On a local-storage
  cluster, enable `reconciliation.enabled` on the reader and run
  `POST /api/v1/reconciliation/trigger` on that reader. That run is a dry run;
  check `orphan_storage_count` (the sample lists at most
  `reconciliation.sample_paths_cap` paths). Then delete with
  `POST /api/v1/reconciliation/trigger?dry_run=false&act=true`. A run deletes at
  most `reconciliation.max_deletes_per_run` files, and files younger than the
  grace window (`grace_window_seconds` plus `clock_skew_allowance_seconds`,
  24 h 5 min by default) are skipped. On shared storage, reconciliation runs only
  on the active compactor, and a reader's copies are in the shared bucket.
- **Clusters upgraded from v26.09.2 or earlier.** Replicated line-protocol rows
  were filed under the `default` database until #889 was fixed in v26.09.3, so
  some of these copies are in `default`.
- **Writers.** Do not run orphan-storage cleanup on writers to fix this. A
  writer's unregistered files can be the only copy of rows it recovered from its
  WAL after a crash
  ([#1206](https://github.com/Basekick-Labs/arc/issues/1206)).

## Upgrade note: HTTP server (fasthttp 1.76)

Arc's HTTP server library moved from fasthttp 1.51 to 1.76. Two changes are
visible to clients:

- **An HTTP/1.1 request without a `Host` header is answered `400`** and the
  connection closed. HTTP/1.1 requires the header and every common client
  sends it (curl, Go, Python, Telegraf, Grafana); HTTP/1.0 requests, which
  have no such rule, are still served. A hand-written client or health check
  that sends a bare `GET /health HTTP/1.1` needs a `Host` line.
- **Arrow IPC responses no longer send an empty trailer.** The
  `Arc-Stream-Truncated` and `Arc-Rows-Capped` trailers are still announced in
  the `Trailer` header of every `/api/v1/query/arrow` response, but each is
  sent only when it has a value; before, an unset one was sent empty. The
  client contract is unchanged: a non-empty value means the stream was
  truncated, or capped. A client that checked for the trailer's presence
  rather than its value has to check the value.

`klauspost/compress`, which fasthttp 1.76 asks for at 1.20, has been at 1.20.4
since 26.09.4; its notes give the arm64 snappy cost
([#1582](https://github.com/Basekick-Labs/arc/issues/1582)).

## Upgrade note: DuckDB 1.5.6 ([#1372](https://github.com/Basekick-Labs/arc/issues/1372))

Arc's query engine moved from DuckDB 1.5.5 to 1.5.6, a bug-fix release.
Among its fixes are queries that could return wrong results, if the SQL sent
to Arc has these shapes:

- a `UNION ALL` whose branches share a join;
- Top-N over a window function (`row_number() … WHERE rn <= N`), including
  ordering expressions that can be NULL.

Arc's configuration and API do not change. Two things are visible:

- **Error text quotes identifiers.** DuckDB now quotes table and column names
  in its error messages, so the text of a query error can differ from 1.5.5.
- **Extensions are rebuilt.** DuckDB extensions are built per DuckDB version,
  so Arc uses the 1.5.6 builds. The Docker image carries httpfs and aws.
  Arc downloads azure (for Azure storage) and cache_httpfs (with
  `query.enable_s3_cache` with S3 primary storage) on first start when the configuration uses
  them, and outside the image it downloads whichever of the four the storage
  configuration uses.
  - An air-gapped host needs the 1.5.6 builds of the ones it uses copied in,
    as for any DuckDB upgrade.
  - An arcx extension loaded with `database.arcx_extension_path` must be a
    build for DuckDB 1.5.6.

## Upgrade note: file-level time pruning is on by default ([#659](https://github.com/Basekick-Labs/arc/issues/659))

`query.file_time_pruning` now defaults to `true`. It was added in 26.09.2 as
an opt-in.

**What it does.** Under high-frequency ingest, the current hour holds one
small Parquet file per buffer flush, often thousands. A query whose range
starts in the current UTC hour used to read every one of them. It now skips
the files in that hour flushed before the query's start minus
`query.file_time_pruning_margin_seconds` (default 300). Only local storage is
affected; files on S3 or Azure are read as before.

**Which queries gain.** Only those whose range starts in the current UTC hour,
such as "last 5 minutes" at 10:40. A "last 1 hour" panel always starts in the
previous hour and never gains. At the default
`compaction.hourly_min_age_hours = 1`, the previous hour is not compacted until
the first compaction run more than an hour after it ends (later with a larger
min age), so it also still holds every per-flush file. On local storage, a
query that starts in the current hour now lists that hour's directory and is
not served from the query plan cache.

**What it can hide.** A row stamped more than the margin ahead of the clock of
the node that flushed it, and still within the current hour, can be missing
from such a query until the hour closes. Rows stamped that far ahead in a
later hour are not affected. `arc_ingest_future_stamped_rows_total` counts
these rows on the node that flushed them: a writer, or an edge spoke whose
files a hub receives. A cluster reader or hub that misses them counts
nothing, so check the counter on the writers and spokes. Size the margin to
the worst clock skew among them.

**To turn it off,** set `query.file_time_pruning = false` (or
`ARC_QUERY_FILE_TIME_PRUNING=false`) and restart. The node logs "File-level
time pruning disabled" at startup.

A negative `compaction.daily_min_age_hours` now reads as 24, as 0 already did.
Before, it put the cutoff in the future, and daily compaction could fold the
current hour while ingest was still writing it.

## Upgrade note: a DELETE reads only the partitions of its time range (`delete.time_pruning`, [#1371](https://github.com/Basekick-Labs/arc/pull/1371))

`delete.time_pruning` is new and defaults to `true`. A row DELETE whose WHERE
bounds `time` now reads only the hour and day directories that range can
touch (see the bug fix "A DELETE reads only the files its time range can
touch" below).

It relies on every file under an hour (or day) directory holding only rows
of that hour (or day). The query path's partition pruning has always relied
on the same thing. Every Arc release since 26.01.1 writes files that way.
Arc 25.12 and earlier put a file in the directory of the hour it was
flushed, and compaction and cold migration keep a file's directory. On a
deployment that still holds data written by those versions, a row that
arrived late sits in a later hour's directory, and a DELETE of its hour
would not read it.

**To turn it off,** set `delete.time_pruning = false` (or
`ARC_DELETE_TIME_PRUNING=false`) and restart. Every DELETE then reads every
file of the measurement again. The startup line "Delete operations enabled"
shows the setting.

## Upgrade note: JSON query results encode BLOB as base64 and UUID as text ([#440](https://github.com/Basekick-Labs/arc/issues/440))

The JSON query output (`/api/v1/query` without a `format`, and every
endpoint that answers in the same shape) now writes:

- **BLOB and GEOMETRY** values as standard base64 strings (RFC 4648, padded).
  They used to be the raw bytes inside a JSON string, which is invalid JSON
  for any value holding a byte of 0x80 or above. The Arrow and MessagePack
  outputs are unchanged: they already carry real bytes, and the Grafana
  plugin already shows binary values as base64 on both, so all three agree.
  A spatial query's geometry therefore reads back as base64 WKB; use
  `ST_AsText(...)` or `ST_AsGeoJSON(...)` for a readable value.
- **UUID** values as their canonical text (`6f1c2c1e-8a3b-...`). They used
  to come out as 16 raw bytes.

Before this release no column Arc stored was a BLOB, so this changes the
output of a query only where the query itself produced a BLOB, GEOMETRY or
UUID (a cast, `uuid()`, a spatial function). During a rolling upgrade a
proxied query is encoded by the node that answers it.

## Upgrade note: GeoParquet columns read as binary ([#440](https://github.com/Basekick-Labs/arc/issues/440))

Arc's query, compaction and DELETE engines now read a GeoParquet column (one
a file's `geo` metadata names) as the `BLOB` it is stored as, not as
`GEOMETRY`. With the engine default, one GeoParquet file in a measurement
turned that column into `GEOMETRY` for every file read with it; the other
files' `BLOB` values cannot be cast to it, so every query that read the
column failed, and so did compaction of the partition, on every cycle.

What changes: a GeoParquet file placed straight into a measurement's storage
used to read its geometry as `GEOMETRY` (`ST_X(geom)` worked); it now reads as
`BLOB` like every geometry Arc stores, so use `ST_X(ST_GeomFromWKB(geom))`. A
file that uses Parquet's own `GEOMETRY` type (written by pyarrow, GDAL, DuckDB's
GeoParquet V2) still reads as `GEOMETRY` when placed into storage directly;
import it through the Parquet import API instead, which stores it as binary.
During a rolling upgrade, a node still on an earlier release reads a
GeoParquet file placed into storage as `GEOMETRY`, as before.

## New features

### Arrow Flight SQL query endpoint ([#1385](https://github.com/Basekick-Labs/arc/issues/1385))

An optional `[flight_sql]` listener exposes Arc's existing read-only query path
over Arrow Flight SQL. It uses Arc token authentication and the configured TLS
certificate, and maps databases and measurements to Flight SQL catalogs and
tables for ADBC clients. Results stopped at a governance row cap end with a
`ResourceExhausted` status rather than appearing to be complete.

Flight queries are audited and logged like HTTP queries, with the client's
address. Metadata calls run one query per database, plus one per measurement
when schemas are requested, and count against governance limits. Prepared
statements without parameters are supported (see the entry above), so the
JDBC driver and the BI tools built on it can run queries. See
`docs/operations/flight-sql.md`.

Both Helm charts can turn it on: `flightSql.enabled` (default `false`),
`flightSql.port` (default 50051) and `flightSql.servicePort`. When on, the OSS
chart's Deployment and Service, and the enterprise chart's writers and readers
with their Services, get a `flight-sql` port (`appProtocol: grpc`); compactors
are not given the listener. A port that collides with one of Arc's is refused
when the chart renders.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1419](https://github.com/Basekick-Labs/arc/pull/1419).

### Flight SQL answers SqlInfo ([#1645](https://github.com/Basekick-Labs/arc/issues/1645))

The Flight SQL listener answers `GetSqlInfo` with the server name (`Arc`), the
Arc and Arrow versions, that it is read-only and takes SQL but not Substrait,
transactions, cancellation, bulk ingestion or DDL, the identifier quote (`"`),
the metadata search-pattern escape (`\`), the catalog term (`database`), and
the query grammar it supports: column and table aliases, `GROUP BY`, `UNION`
and `UNION ALL`, full outer joins, subqueries (correlated too), expressions and
unrelated columns in `ORDER BY`, and `LIKE ... ESCAPE`. The JDBC driver reads a
value the server leaves out as `false`, so without these it would report that
Arc has no `GROUP BY`, `UNION`, joins or subqueries.
Before, every SqlInfo request failed with `NotFound`: ADBC's `GetInfo` failed
with it (`adbc_get_info()` in Python), and with the Arrow Flight SQL JDBC driver
every `DatabaseMetaData` call that reads SqlInfo threw, such as
`getDatabaseProductName()` and `getIdentifierQuoteString()`. A request that
includes IDs Arc has no value for, as ADBC's does for the Substrait version
range, is answered with the rest. Queries through the JDBC driver need
prepared statements, which are in this release too (see the entry above). See
`docs/operations/flight-sql.md`.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in [#1657](https://github.com/Basekick-Labs/arc/pull/1657); the query-grammar values were added in review.

### Flight SQL accepts an Arc token in a bearer-token handshake ([#1645](https://github.com/Basekick-Labs/arc/issues/1645))

The Flight SQL listener accepts a `Handshake` that carries the Arc token as
`authorization: Bearer <token>`, and answers it with the same header. The
Arrow Flight SQL JDBC driver opens the connection that way when given its
`token` property (`jdbc:arrow-flight-sql://host:50051?token=<token>`, with
the token's trailing `=` written as `%3D`), and
every such connection failed with `UNAUTHENTICATED: only Basic Auth
implemented`: only a Basic handshake, with the token as the password, was
accepted. An unknown token is refused with `invalid Arc token`, as on any
other call. Basic handshakes and calls that send the bearer header without a
handshake (the ADBC drivers) are unchanged. See
`docs/operations/flight-sql.md`.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in [#1658](https://github.com/Basekick-Labs/arc/pull/1658).

### Prometheus Remote Write 1.0 ingestion (#1383)

Arc accepts Prometheus Remote Write 1.0 samples at `/api/v1/write/prometheus`,
so Prometheus, Grafana Alloy and other remote-write senders can ship metrics to
Arc. Each metric is a measurement, in the layout arc-opentelemetry-exporter
writes for OTLP metrics and named the way it names them (characters other than letters, digits and `_` become `_`, so a
recording rule's `job:up:sum` is `job_up_sum`); labels are tag columns and
samples a float64 `value` column. Staleness markers are not stored. A series
Arc cannot store (a label named `value` or `time`, a label name starting with
`_`, no `__name__`) is dropped and the rest written, answered with a `partial
write` 400 as for line protocol. The receiver bounds the decoded payload, the
materialised columns and the label bytes repeated across samples (413), ignores
metric metadata, and does not store exemplars or native histograms. A Remote
Write 2.0 request is answered 415. Prometheus
and Grafana Alloy configuration examples are in the
[integration guide](docs/integrations/prometheus-remote-write.md).

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1418](https://github.com/Basekick-Labs/arc/pull/1418).

Follow-up hardening, in the same release:

- The compressed body, the decoded protobuf and the label bytes repeated
  across a request's samples follow `server.max_payload_size`, at most 100MB
  (413 above it, `remote write payload exceeds 10MB limit` on a node set to
  `10MB`). Before, a small body could decode to 100 MiB whatever the setting.
- The repeated label bytes count label names as well as values: a name has no
  length limit, and every sample row repeats it.
- A request reserves the WAL space its rows actually take, rather than its
  body size, and one larger than `wal.max_directory_mb` can ever hold is
  answered 413 instead of a 503 Prometheus would resend forever (reachable
  only with a small `wal.max_size_mb`: the directory holds at least four WAL
  files).
- A label spelled two ways in a metric (`Host` and `host`) drops only the
  series using the other spelling, not every sample of the metric: the stored
  column's spelling wins, otherwise the byte-smallest in the request, in any
  series order.
- New counters `arc_prometheus_remote_write_requests_total`, `_errors_total`,
  `_records_total` and `_bytes_total`. Remote write no longer counts in the
  line-protocol counters or `/api/v1/write/line-protocol/stats`.

### Fix: `server.max_payload_size = 0` falls back to the default (#1383)

A `server.max_payload_size` of `0` could not work: the HTTP server read it as
its own 4MB default and MessagePack writes refused every body. Arc now logs a
startup warning and uses the default (`1GB`); any other value is unchanged.

### Peer replication bandwidth cap ([#397](https://github.com/Basekick-Labs/arc/issues/397))

`cluster.replication_pull_bytes_per_second` caps the inbound bandwidth of a
node's peer file replication: one budget shared by all its pull workers, so
reactive pulls, the startup catch-up and the periodic recheck together stay
under it. It takes bytes per second or a size such as `"10MB"` (1 MB = 1024 KB);
the default `0` is unlimited. A negative or unparseable value fails startup,
instead of reading as unlimited. Helm: `cluster.replication.pullBytesPerSecond`.

The cap does not apply to the staged compaction commit: a compacted output is
fetched from the start every time, so a capped fetch could never deliver one
larger than the cap allows within `cluster.replication_serve_timeout_ms`, and
the partition would be compacted and aborted again on every cycle.

Under a low cap a large file can take longer than one fetch. Each fetch is
still bounded by `cluster.replication_fetch_timeout_ms` on the pulling node and
by `cluster.replication_serve_timeout_ms` on the node serving the file; when
either ends it, the next attempt resumes from the bytes already received. That
now includes the first attempt of a new pull: before, only retries within one
pull resumed, and every new pull of the same file (a manifest callback, the
catch-up, the recheck) started again from zero, so a file too large for one
pull's attempts never completed.
A partial left by an older version of the same file fails the checksum when
resumed; it is discarded and the same peer is asked again from the start, and
the mismatch is not counted against that peer.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1404](https://github.com/Basekick-Labs/arc/pull/1404).

### Google Cloud Storage: primary storage, cold tier and backup target ([#1105](https://github.com/Basekick-Labs/arc/issues/1105))

`storage.backend = "gcs"` stores Arc's data in a Google Cloud
Storage bucket, standalone or as the shared storage of a cluster, through GCS's own API rather than its S3 interoperability
endpoint, so it authenticates the way Google workloads do: no HMAC keys.
GCS also serves as the tiered-storage cold tier
(`tiered_storage.cold.backend = "gcs"`) and as a backup target
(`backup.targets.<name>.type = "gcs"`), each taking the same four keys
(`gcs_bucket`, `gcs_prefix`, `gcs_credentials_file`, `gcs_endpoint`) and
its own identity.

```toml
[storage]
backend = "gcs"
gcs_bucket = "my-arc-data"
# gcs_prefix = "instances/abc123/"          # default: the bucket root
# gcs_credentials_file = "/etc/arc/key.json" # default: Application Default Credentials
# gcs_endpoint = "https://storage-x.p.googleapis.com" # https only, e.g. Private Service Connect
```

- **Authentication.** Application Default Credentials by default: GKE
  Workload Identity, the GCE or Cloud Run metadata server, or
  `GOOGLE_APPLICATION_CREDENTIALS`. `gcs_credentials_file` names a
  service-account key instead; only that key type is accepted. Ingest,
  compaction and queries use the same identity.
- **IAM.** `roles/storage.objectAdmin` on the bucket is enough to run. It
  does not include `storage.buckets.get`, which the startup checks below
  need; without it Arc logs a warning that the bucket's settings were not
  checked and starts anyway, so a requester-pays bucket then fails at its
  first query rather than at startup. Grant `storage.buckets.get` too.
- **Queries.** DuckDB reads `gs://` with a bearer token Arc resolves from that
  identity. Arc starts replacing it 10 minutes before it expires; with a key
  file or user credentials the new token arrives then, while the GCE/GKE
  metadata server keeps returning its current token until close to expiry,
  so there the token is replaced when the server issues a new one (Arc polls
  each minute meanwhile and warns if fewer than 5 minutes remain). `query.enable_s3_cache` does not
  apply to GCS. `/health` reports the
  hot tier as `"backend": "gcs"`. A new credential state, **`unavailable`**,
  means no token has been resolved yet, so every query read of the bucket
  fails (GCS has no fallback the way S3 and Azure do); Arc keeps retrying and
  logs each failure at error level. With
  `server.storage_credentials_fail_ready = true`, `/ready` answers 503 with
  `storage_credentials_unavailable` in that state, as it does for `expired`.
- **Refused at startup** (the bucket checks need `storage.buckets.get`): a
  bucket with hierarchical namespace or with requester pays; an `http://`
  `gcs_endpoint`; and Iceberg export, which is local-only.
- **Clusters.** GCS is the shared storage of a Pattern 2 cluster
  (`cluster.shared_storage_mode`: every writer writes the bucket, staged
  compaction commits through the compactor), and the cold tier of a
  Pattern 1 cluster. Each node resolves its own query token from its own
  identity. Verified on a 5-node Pattern 2 cluster (3 writers, reader,
  compactor) and a 4-node Pattern 1 cluster with a GCS cold tier, both on
  real GCS: ingest on every writer, staged compaction, DELETE across
  writers with unflushed rows, leader and compactor loss, writer failover
  with migration continuing on the new primary, reconciliation dry-run.
  The enterprise Helm chart deploys it with
  `storage.shared.backend: gcs` and either Workload Identity (a
  chart-created ServiceAccount, `serviceAccount.create: true`, carrying the
  `iam.gke.io/gcp-service-account` annotation) or a service-account key
  Secret (`storage.shared.gcs.credentialsSecret`, shared storage only).
- **Rate limits.** GCS allows about one write per second to one object name
  and ramps a new bucket's request rate up. A rate-limited (429) read,
  listing, copy or delete now waits at least half a second (a second on
  average) and is retried for up to a minute and a half, where other
  transient errors keep their short schedule; uploads back off from one
  second up to 16 s across their 12 retries.
- **Warned at startup**, by what the bucket is for. Primary: soft delete
  (on by default for new buckets, 7 days), a retention policy and a default
  storage class other than STANDARD — compaction and retention delete
  objects continuously, so soft delete keeps every deleted byte billed for
  its period, a retention policy makes those deletes fail until objects are
  old enough, and colder classes charge a minimum storage duration on each;
  turn soft delete off on a bucket used only by Arc. Cold tier: a retention
  policy, and a class other than STANDARD, which charges retrieval on every
  query read of cold data — use STANDARD, or Autoclass, which charges
  neither retrieval nor early deletion (a lifecycle rule that changes the
  class of cold objects is not visible to this check). Backup target: a
  retention policy only, since it stops a backup from being deleted or a
  partial one from being cleaned up; a colder class is a reasonable choice
  there.
- **Endpoint.** DuckDB uses only the host of `gcs_endpoint` and speaks GCS's
  XML API on it, so the endpoint must serve both the JSON API (ingest,
  compaction) and the XML API (queries) on one host. Verified with
  `https://storage.googleapis.com`; a Private Service Connect endpoint has
  not been tested yet. An emulator that serves only the JSON API cannot
  serve queries.
- **Cold tier.** Files migrate to a GCS cold tier from any primary, and a
  GCS primary takes a cold tier of any backend (verified with local → GCS
  and GCS → GCS; the other pairs use the same generic migration path). The cold tier has its
  own query secret and identity; `/health` reports it under `"cold"`, and a
  cold tier whose credentials never resolved is `unavailable`, which drains
  `/ready` when `server.storage_credentials_fail_ready` is on (an S3 cold
  tier in the same state reports `fallback` and stays ready). With no credentials at
  boot the cold tier still starts and its reads fail until they resolve, as
  for S3 and Azure.
- **Backup target.** Every #1085 behaviour applies: per-database routing,
  key headroom for the prefix, cold-tier files carried once, restore into a
  node with a cold tier at another prefix (files go to that cold tier) or
  without one (they are restored hot). A target whose bucket does not exist
  fails the backup rather than completing it.
- An S3-typed target or cold tier pointed at the same bucket through
  `storage.googleapis.com` is the same storage for the overlap checks.
- **Not in the FIPS build (`arc-fips`)**
  ([#1587](https://github.com/Basekick-Labs/arc/issues/1587)). The Google API
  client links `golang.org/x/crypto/hkdf`, outside the Go Cryptographic
  Module the FIPS build is limited to. `arc-fips` refuses
  `storage.backend`, `tiered_storage.cold.backend` and a backup target's
  `type` set to `"gcs"` when it loads its config, naming the key. A FIPS
  deployment on Google Cloud uses GCS through its S3-compatible endpoint
  instead: type `"s3"` with the corresponding `s3_endpoint` set to
  `storage.googleapis.com` and HMAC keys.

### MessagePack binary values are stored as binary columns ([#440](https://github.com/Basekick-Labs/arc/issues/440))

A MessagePack `bin` value is now stored as a Parquet BYTE_ARRAY column
(DuckDB `BLOB`), byte for byte. It used to be refused ("unsupported column
type"). This is the first step of spatial support: a geometry sent as WKB
bytes is stored as-is, and `ST_GeomFromWKB(geom)` reads it once the `spatial`
extension is available.

- Every MessagePack shape (columnar, row, batch) and MQTT payloads accept
  it. A NULL stays NULL and an empty value stays an empty value, through the
  WAL and its recovery as well.
- JSON query results show it as base64 (see the upgrade note above); Arrow
  and MessagePack results carry the bytes.
- Iceberg export maps it to the Iceberg `binary` type. It used to stop the
  measurement's export with "unsupported Arrow type".
- A continuous query whose output has a UUID column writes it as text (the
  run used to fail); a BLOB or GEOMETRY output column is written as binary.
- **A column cannot change between string and binary.** A write that sends
  bytes to a column stored as a string, or a string to a column stored as
  binary, is refused with HTTP 400 and nothing of the request is written (an
  import already part-way through answers as a partial import, naming what
  it wrote, as for any other failure). DuckDB reads
  a column that is a string in some files and binary in others as binary, so
  one such write would turn every stored string of that column into bytes for
  every query. The check uses the field schema Arc keeps per measurement
  (`query.stable_schema`, on by default) and knows a column once a file
  holding it has been flushed or the measurement queried on that node. So it
  is best-effort: it is not made with `query.stable_schema = false`, for a
  column whose first values are still in the buffer (up to
  `ingest.max_buffer_age_ms` after it first appears), on a writer that has
  not flushed or queried the measurement since it started, for a
  measurement whose schema was evicted from memory (more than 10,000
  measurements in use), or between two writers changing the same column at
  the same time. A change that gets through leaves the column mixed for
  good, and its string rows read back as binary.
- Parquet import is unchanged: a BYTE_ARRAY column without a string
  annotation is still imported as a string, as Spark and Hive write string
  columns that way.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#916](https://github.com/Basekick-Labs/arc/pull/916).

### Spatial SQL functions with `database.extensions = ["spatial"]` ([#440](https://github.com/Basekick-Labs/arc/issues/440))

`database.extensions` (or `ARC_DATABASE_EXTENSIONS`, comma-separated) loads
DuckDB extensions at startup. The one supported today is `spatial`: its
`ST_*` functions work in queries, continuous queries and DELETE predicates,
over DOUBLE longitude/latitude columns (`ST_Point(lon, lat)`) or over WKB
stored in a binary column (`ST_GeomFromWKB(geom)`). The default is empty.

- **Only supported extensions load.** An extension adds SQL functions Arc's
  query validators have to know about, so each one is reviewed before it is
  supported. Any other name, a file path or a URL stops Arc at startup with
  an error naming the entry. Community extensions are not supported.
- **Functions that open files are refused.** `ST_Read`, `ST_Read_Meta`,
  `ST_ReadOSM`, `ST_ReadSHP`, `shapefile_meta`, `ST_Drivers` and
  `ST_Transform` are rejected by the query, continuous query and DELETE
  validators, like `read_csv` and the other file readers. Data comes into
  Arc through its write endpoints. `ST_Transform` is among them because a
  coordinate reference system can name a file; data stays in the reference
  system it was written in. For distances in metres over longitude/latitude
  use `ST_Distance_Sphere` or `ST_Distance_Spheroid`.
- **No network needed.** The container image ships the extension (about
  60 MB of the image, whether or not it is enabled); on a
  binary install the first start downloads it into DuckDB's extension
  directory (`~/.duckdb/extensions`), which an air-gapped host must
  pre-populate.
- **Set the same list on every node.** Each node loads its own; a query
  routed to a node without the extension fails with an unknown-function
  error. On a cluster, a DELETE whose WHERE uses a spatial function cannot
  be applied by a node without the extension to the files it has not
  registered yet; those files wait, and the node logs a warning naming
  `database.extensions`, until the setting matches.
- A JSON query result holding a geometry shows it as base64 WKB; use
  `ST_AsText(...)` or `ST_AsGeoJSON(...)` for a readable value.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1082](https://github.com/Basekick-Labs/arc/pull/1082).

### Writer failover picks the standby with the lowest node id ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

When several healthy standby writers can replace a failed primary, the leader
now picks the one with the lowest node id. Before, the choice depended on map
order and could differ between leaders.

### Experimental: WAL coverage on cluster writers ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

This is the first step toward letting a standby writer take over the rows a
failed primary acknowledged but did not flush yet. It is off by default and
changes nothing unless you set `cluster.wal_coverage = true`.

On a Pattern 1 writer (local storage) with the WAL enabled, the setting:
- **gives every ingest batch a coverage id.** The id is unique per writer
  process and measurement, and assigned in commit order. It is written into the
  batch's WAL entry and into the registration journal with every file the batch
  is flushed into.
- **keeps WAL entries until their files are registered.** Before, a WAL entry
  could be purged once its file was flushed. Now it stays until the cluster
  manifest has the file. During a manifest outage (lost Raft quorum, for
  example) the WAL grows with the unregistered data, as the registration journal
  already does. The hold survives restarts: WAL recovery keeps a file whose
  batches still wait to register, along with every file after it, and the node
  deletes them once the registrations land. A clean shutdown keeps them too,
  instead of purging the whole WAL. A start whose cluster does not come up keeps
  them until a later start registers them, logged at Error.
  `arc_wal_held_files` counts the files kept for this reason, and a Warn every
  10 minutes reports how many while any are held. The hold covers files journaled with
  coverage even after the setting is turned off.
- **refuses a write before it reaches the WAL.** A write the buffer turns away
  (schema churn, a shard closing during shutdown, a cancelled request) no
  longer leaves an entry in the WAL that a later recovery would replay.

New metrics:
- `arc_wal_registered_floor_lag`: batches committed but not yet in a registered
  file. It returns to 0 when every flushed file is registered; a lost batch
  (below) stays in it until the process restarts.
- `arc_wal_coverage_lost_total`: batches whose file settled without being
  registered (the file was gone or rewritten before registration), or whose
  flush failed with no complete WAL copy. Those rows are in no registered file.
  Alert on any increase.

**Standby writers keep a copy.** With the setting on, the designated primary
also streams every committed batch to the standby writers. Each standby keeps
it on disk under `<wal.directory>/received/`, and never applies it. It discards
a batch once the batch is in a registered file and the standby holds every
current file of that measurement locally. This is the copy stream takeover
(below) publishes the acknowledged rows of a primary that died before
flushing them from. Details:
- Standby writers connect to the primary on the cluster port
  (`cluster.coordinator_addr`). This needs `cluster.shared_secret`, and
  `cluster.tls_enabled` if the network is untrusted: batches are authenticated
  but not encrypted without TLS.
- `cluster.standby_store_max_bytes` caps the copy. The default `0` means a
  quarter of the WAL volume, at least 1 GiB; a negative value means no cap.
  Past the cap, a standby first evicts batches of a primary process that has
  since restarted. If that is not enough, it keeps nothing new until trimming
  frees room, and records the missed batches as gaps. The copy is trimmed every
  5 seconds, and at once when it passes half its cap. The directory counts
  against the WAL volume but not `arc_wal_dir_bytes`. The store also stops
  keeping batches while the volume has less than 10 % (at least 2 GiB) free
  (see the bug fix for [#1266](https://github.com/Basekick-Labs/arc/issues/1266)).
- `cluster.origin_stream_queue_bytes` (default 64 MiB) caps what the primary
  holds in memory for a standby that is not keeping up. Past it, that standby
  misses batches; the others do not.
- A covered file now counts as registered only once the writer's own copy of the
  manifest shows it. On a writer that is not the Raft leader this adds one Raft
  barrier per registration batch.

Metrics on the primary: `arc_origin_stream_sessions`,
`arc_origin_stream_dropped_total`. On standby writers:
`arc_standby_stream_connected`, `arc_standby_stream_entries_total`,
`arc_standby_stream_gaps_total`, `arc_standby_store_bytes`,
`arc_standby_store_incomplete_keys`, `arc_standby_store_trimmed_bytes_total`,
`arc_standby_store_dropped_total`, `arc_standby_store_evicted_bytes_total`,
`arc_standby_store_lost_records`, `arc_standby_store_blocked_keys` and
`arc_standby_store_replaced_bytes`. `arc_standby_store_blocked_keys` counts
the measurements with registered batches the standby may not trim yet: a file
holding their rows is not on the standby (a file the puller cannot fetch keeps
those batches), the standby's manifest is behind the primary's, a claim is
open, or the primary is not heard from.

All writers in the cluster must run this release before the setting is turned
on: a writer from an older release does not understand the standby connection
and logs it as an unknown message type.

**Stream takeover building blocks.** Nothing here changes behaviour unless
every node sets `cluster.stream_takeover` (see "Stream takeover" below):
- A new setting, `cluster.stream_takeover` (default off), opts a node into
  stream takeover. Takeover lets a standby writer publish the rows a failed
  primary acknowledged but never registered, exactly once. It needs every node
  of the cluster to opt in, writers with `cluster.wal_coverage` on.
- Only nodes with the setting on advertise takeover support when they join.
  The advertisement is covered by its own MAC next to the existing join MAC, so
  a rolling upgrade keeps working against a leader from an older release.
- A covered file's registration carries the exact batches it holds. When a
  flush spans several partition hours, each hour's file lists only the batches
  with rows in it. This is the one cost of `cluster.wal_coverage` without
  takeover: up to 64 KiB per registration in the Raft log, usually a few
  bytes, which a cluster without takeover discards.
- The cluster manifest has the rules that will refuse a second copy of a
  batch, and the origin's copy of batches a standby took over.

- A refused registration is parked rather than left in place: the file
  moves out of every query path to `<storage>/_refused/`, its WAL entries stay
  held, and it is deleted only once each of its rows is in a registered file
  this node holds. A DELETE that touches a parked file is refused (retry later).
  Records live in `refused-registrations.jsonl` next to the registration
  journal. Metrics: `arc_wal_parked_files`, `arc_wal_parked_bytes`.
- Writers with the setting on announce each process to the cluster before
  accepting writes, once takeover is on: until the announcement applies, writes
  get a retryable 503. Each writer records its last process in
  `instance.last` files in the WAL directory, the Raft data directory and
  `<storage>/_instances/`.
- `cluster.stream_takeover` now needs an explicit `cluster.node_id`. Takeover
  claims name nodes, and the default node id (host name and process id)
  changes at every restart outside Kubernetes. A node with the setting on and
  no `cluster.node_id` logs an error and does not advertise takeover support,
  so the cluster cannot turn takeover on while that node is a member.
- Once takeover is on, the standby store no longer evicts, at its cap, the
  batches of a writer process that a takeover could still need. When the store
  is full and holds such batches of a process its writer has since restarted,
  new batches are not kept and an error is logged; the new gauge
  `arc_standby_store_pinned_bytes` shows how much the store keeps that it would
  otherwise have evicted.
- A standby publishes the batches it takes over straight to Parquet, without
  touching the WAL or the live buffers; a claim grows in steps; a claimant
  releases its own claim when its copy is gone; and a claimant file the
  settled index has no room for is refused (the index keeps tracking the
  writer process instead of giving up on it).
- A writer that leaves the cluster gracefully (a restart, a rolling upgrade)
  keeps its takeover state while it is gone: when it comes back it picks up its
  own unflushed batches first, instead of having them taken over. Only an
  operator node removal gives them up, or the departure expiring after
  `cluster.stream_takeover_departed_ttl` (default `24h`). Departed writers are
  listed by the new admin endpoint `GET /api/v1/cluster/stream-claims` and
  counted by `arc_stream_takeover_departed_writers`.
- New setting `cluster.stream_takeover_grace` (default `60s`, at least `10s`):
  how long a primary that stopped answering may take to come back before the
  new primary takes over its batches.
- A file that vanished before any node pulled it now gives its rows back for a
  takeover, in its partition hour only; the rest of the writer process stays
  settled.
- Failover never promotes a writer whose current process another writer has
  claimed.

**Stream takeover (live, opt-in).** With `cluster.stream_takeover = true` on
every node (writers also need `cluster.wal_coverage = true` and an explicit
`cluster.node_id`), the cluster turns takeover on by itself once every node
advertises support (`stream_claims_v7`; builds that advertised `v1` to `v6`
block it). From then on:
- **A dead primary's acknowledged rows are published by the new primary.**
  When the primary stops answering and failover designates another writer,
  that writer waits `cluster.stream_takeover_grace`, then claims every batch
  of the old primary it holds in its standby copy that is not in a registered
  file, replays them to Parquet, and registers them. The cluster manifest
  refuses any row registered twice, so a batch the old primary registered
  before it died, in whole or in part, is published once.
- **A writer whose process was claimed exits with code 3.** If the old
  primary was only cut off and comes back, it refuses writes (503) the moment
  it sees the claim, flushes, and exits with status 3 so the restart policy
  starts it again; its next process is not claimed. Run writers under a
  restart policy that restarts on a non-zero exit (systemd
  `Restart=on-failure`, Kubernetes, Docker `restart: on-failure` or
  `unless-stopped`). Its copies of the claimed batches are parked, not
  registered. A writer that is the Raft leader steps down as it exits.
- **A primary that restarts within the grace keeps its own batches.** A
  restart, crash or rolling upgrade that brings the writer back with its
  volumes before the grace ends takes nothing over: the writer announces the
  earlier process and publishes it itself. A graceful shutdown first reports
  everything it registered, so a takeover after it claims only what is really
  unregistered.
- **Standby writers refuse writes** (503, retryable) while another writer is
  the designated primary, checked on every write.
- **Copies a standby holds that the new primary lacked are published too.**
  Once a claim finishes, any other writer that holds batches of the old
  process the claim did not publish claims those (each writer once per
  process), as long as the old primary has not come back with a new process
  and is not the designated primary. A process its writer gave up (it
  restarted without it) is published by any writer holding it, at once.
- **A DELETE stays deleted through a takeover.** With takeover on, a row
  DELETE (`POST /api/v1/delete`) or a database deletion
  (`DELETE /api/v1/databases/{name}`) is recorded in the cluster before it
  runs. Rows acknowledged before it that are not in a registered file yet (a
  standby's copy a takeover replays, an old primary's WAL, a writer's pending
  files) are rewritten without the rows it removes before they are
  registered, with the DELETE's own SQL, and the cluster manifest refuses any
  such file that was not. Rows acknowledged after the DELETE's own flush
  are outside the record (a write concurrent with the DELETE may or may not
  be deleted). So:
  - the DELETE no longer waits for a takeover (the 503 "a stream takeover is
    publishing rows" is gone);
  - `now()`, `current_timestamp`, `current_date` and the other per-statement
    time functions in the WHERE clause are fixed to the time the DELETE ran,
    in UTC, so a later replay matches the same rows; `random()`, `uuid()`,
    `nextval()` and similar are refused (400);
  - the primary first waits (up to 30 seconds) for every file the manifest
    lists of the measurement to be on its disk, since the DELETE rewrites
    the files it has. A file that does not arrive answers 503 with
    `unreachable_files`; set `"allow_unreachable": true` to run the DELETE
    anyway: the response lists the files it skipped, and their rows are not
    deleted;
  - each writer acknowledges every DELETE with the batches it had accepted at
    that point, and refuses writes (503, retryable) for the moment it takes
    (about 100 ms);
  - for a few seconds, a node that is not the primary can still show deleted
    rows in a file it has just written and not registered yet. The primary
    never does once the DELETE has answered;
  - the cluster keeps at most 256 DELETEs until every writer process they
    name has settled the batches that came before. Past that, the oldest one
    that only retired writer processes still hold is dropped, and those
    processes are removed (their copies are parked if they come back);
    otherwise a DELETE answers 503 until older ones settle. A writer that is
    down, and neither back nor given up on (removed, or its departure
    expired), holds every DELETE made while it is down.
    `GET /api/v1/cluster/stream-claims` lists the DELETEs kept
    (`delete_records`) and the writer processes that have not reported their
    batches for each (`unacknowledged`).
- **Operator controls** (admin):
  `GET /api/v1/cluster/stream-claims` lists processes and claims;
  `POST /api/v1/cluster/stream-claims/{instance}/release` with
  `{"epoch": n}` releases an unfinished claim (any writer may claim it again,
  the same one included), and with `{"epoch": n, "abandon": true}` finishes
  it, irreversibly: batches it has not published are left to their origin,
  which loses them when the origin is gone, and the claimant drops its copy
  (every batch the claim held is reported unreplayable). Leader only (409 elsewhere). The leader also
  releases an unfinished claim by itself when its claimant has been gone for
  the grace.
- New gauge `arc_stream_takeover_oldest_claim_age_seconds`: how long the
  oldest unfinished claim has been open. A claim whose replay keeps failing
  (a full disk, for example) is never given up on by itself: alert on this
  gauge.
- **An old primary that comes back with its volume publishes the rest.**
  Rows it acknowledged that no standby held (the replication lag), and rows
  the new primary could not publish, are in its WAL: it reads them back and
  publishes them, once, and its copies of rows a takeover already published
  are refused and removed. Flushes never put batches another writer claimed in
  the same file as batches nobody claimed, nor batches of two processes of the
  writer, so a refusal never holds back rows nobody else has.
- **A writer the cluster gave up on is retired, not forgotten.** When every
  writer has finished with a process of a writer that was removed, or whose
  departure expired, the process is kept as `retired` (the last 256), with
  what was registered of it: if that writer comes back with the volume after
  all, it publishes exactly the rows nobody registered. Copies of an older
  retired process are refused and parked.
- **Files nobody pulled are not left behind.** A file the old primary
  registered counts as published, but if no other node pulled it before the
  primary's volume was lost, its rows are gone. So, for every file of the old
  process whose rows its standby copy holds, a claim finishes only once the
  new primary holds the file too. If every node, asked directly, says it does
  not have it (or no node could answer for it during twice the grace), the
  file is removed from the manifest and its rows are published from the
  standby copy; a node that later comes back with a copy of it removes that
  copy. A file the standby copy could not publish again is never removed and
  does not hold the claim. New gauge `arc_stream_takeover_unpulled_files`:
  files the new primary still waits for.

What takeover does not cover: rows a primary acknowledged but had not
streamed to any standby yet when its volume was lost (the replication lag,
usually milliseconds) and that it never comes back with.

**Rollback warning.** WAL entries written with the setting on use a new format.
This release reads it, but older releases cannot: they quarantine those entries
and lose any rows that were not flushed yet. Before rolling a node back to an
older release, either turn the setting off and restart once, or stop the node
cleanly so it flushes and purges its WAL. A registration journal written with the
setting on loads in older releases; they ignore the coverage.

Writes with the setting on cost about 25% more per request when every request
is a single row to one measurement. With batches of a few hundred rows the
difference is within measurement noise.

### Compaction cycles can be looked up by id ([#1162](https://github.com/Basekick-Labs/arc/issues/1162))

`POST /api/v1/compaction/trigger` answers with a `cycle_id`. Until now that id
had nowhere to be resolved: `GET /api/v1/compaction/stats` carries a single
global `last_cycle`, so by the time you polled it could already describe a
later cycle, and nothing in the payload said which cycle you were looking at
relative to the one you asked about. Two new admin-only routes close that:

- **`GET /api/v1/compaction/cycles/{id}`** — one cycle.
- **`GET /api/v1/compaction/cycles?limit=N`** — the retained window, newest
  first. `limit` defaults to 10.

A record carries more than counters, because counters alone cannot tell one
cycle from another. Each one reports the **scope** it was asked for
(`databases`, `measurement`, `tiers`), **when** it ran (`started_at`,
`finished_at`, `duration_seconds`), and **where it came from** (`source`:
`api` for a manual trigger, `scheduler` for a scheduled one). If you drive
compaction one database at a time, this is what separates your trigger's cycle
from the hourly scheduler's cycle that fired in between.

Failures name themselves, completely. Alongside `failed_batches` a record
carries `failed_partitions` — an object mapping each partition that failed to
how many of its batches failed — plus `failed_partition_count` and `error`.
Previously a failure count was a dead end: there was no way to get from "forty
batches failed" to which partitions to retry.

The list is deduplicated by partition, which is what makes it the retry list
rather than a sample. A partition split into nineteen batches that all fail is
one entry with a count of nineteen, so a cycle over eighty partitions and
fifteen hundred batches yields eighty entries. Up to **200 distinct
partitions** are recorded per cycle; past that the record carries
`failed_partitions_truncated: true` so a capped list is never read as
complete.

**`GET /api/v1/compaction/history` gains cycle attribution and a working
`limit`.** Each job record now carries `cycle_id` and `attempt_depth`, and the
endpoint takes `?cycle_id=N` to show only one cycle's work.

Two things to know about reading it. First, `?limit=N` previously returned at
most ten records whatever you asked for — `recent_jobs` was truncated to ten
before the limit was applied — so this is a bug fix, and the parameter now
works up to the 100 records retained. (`scripts/range_schema_acceptance.py`
has been requesting `limit=50` and passing only because its target happened to
land in the last ten.) Second, **these records count attempts, not batches**:
the adaptive splitter retries a failed batch at half size, and every attempt
writes its own record with the same `partition_path`. `attempt_depth` tells
them apart. Do **not** read depth-0 failures as the batch failures: a batch
that fails at depth 0 and is then rescued by splitting counts as *succeeded*
and contributes no failed partition, while still leaving a depth-0
`success: false` record here. The response says `"unit": "attempts"` to make
that hard to misread, and the rule that does hold exactly is
`sum(failed_partitions) == failed_batches` on the cycle record, when not
truncated.

Because attempts multiply, one cycle can emit several hundred records against
a hundred-record ring. The response is explicit about it rather than quietly
partial: `matched` is how many retained records matched before `limit` was
applied — counted over the ring, so on a full ring it is a floor rather than a
total — `page_size` what you got, and `truncated` whether anything is missing.
For a **cycle-filtered** read, `truncated` also accounts for eviction, with
`may_have_evicted_earlier_attempts` saying so; that only fires when the ring is
full *and* the filtered cycle is old enough to have lost records, so it stays
informative on a busy node instead of being permanently true. An unfiltered
read makes no completeness claim — compare `retained` with `capacity` for
that. A cycle-filtered
response also carries that cycle's authoritative outer-level counters
(`cycle_failed_batches`, `cycle_started_batches`, `cycle_failed_partitions`),
which are retained far longer than the job ring and are the numbers to trust.
**For "which partitions do I retry", read `failed_partitions` on the cycle
record** — job history is the attempt-level detail beneath it.

`GET /api/v1/compaction/cycles` (the list) carries `failed_partition_count` but
not the full map: 500 retained cycles of 200 partitions each would be a
multi-megabyte response. Fetch the map from `/cycles/{id}` or
`/history?cycle_id=`.

One smaller shape change: `recent_jobs` in `/api/v1/compaction/stats` was
`null` on a node that had compacted nothing and is now `[]`.

If the cycle you ask about is no longer retained, the response says
`cycle_retained: false` with the retained id range, because an empty result
otherwise reads as "this cycle failed nothing" — the most expensive wrong
answer the endpoint could give.

A cycle that is still running is visible too, as `status: "running"` with live
counters and no `finished_at`, so you can watch a long cycle progress rather
than only read its obituary. A cycle that has been claimed but has not yet
started — the millisecond or two between the trigger's reply and the cycle
body starting — answers `status: "claimed"`, a four-field body with no scope
yet recorded. Note that `GET /cycles` does **not** list a cycle in that state:
its `running_cycle_id` is derived from the retained history so that the list
can never contradict itself, which means during those few milliseconds the
by-id lookup answers `claimed` while the list shows neither the cycle nor a
`running_cycle_id`. Poll the id the trigger gave you, not the list.

`source` has a third value, `unspecified`, for cycles started through an
internal entry point that is neither the API nor the scheduler. No shipped
code path produces it today; it exists so that a future caller is not
silently misattributed to the scheduler.

The list response also carries `retained`, the number of cycles currently
held. A cycle's `error` is truncated at 512 bytes with a trailing
`... (truncated)`, since up to 500 of them are retained at once.

**Three limits worth knowing**, all reported in the 404 body rather than left
to be discovered:

- **History is in memory and bounded at 500 cycles.** At the default schedules
  (`compaction.hourly_schedule = "5 * * * *"` plus
  `compaction.daily_schedule = "0 3 * * *"`) a node consumes 25 cycle ids a day
  even when every cycle finds nothing to do, so 500 is roughly 20 days. Tighten
  the hourly schedule and the window shortens proportionally — about 41 hours
  at `*/5 * * * *`. Nothing is persisted, so a restart empties it.
- **Cycle ids restart at 1 when the process restarts.** An id recorded before a
  restart will not resolve afterwards, and it is not a different cycle's id —
  it is simply gone. The 404 says so instead of claiming the cycle never ran.
- **Cycles run on the node holding the compactor lease**, and the lookup
  answers for the node it is asked on. A cycle triggered on another node is not
  recorded here. On a cluster that manages a compactor lease, the 404 names its
  holder in `lease_holder`, so the response says where to ask instead — the
  same datum the trigger's 503 already reports.

`last_cycle` in `/api/v1/compaction/stats` is unchanged, including its field
names and its meaning: it still describes the most recent *finished* cycle and
never reports `running`.

### Scheduled edge-to-hub replication for paid licenses ([#828](https://github.com/Basekick-Labs/arc/issues/828))

Network spokes can now replicate automatically. Like continuous-query and
retention scheduling, this requires a valid paid license: Starter,
Professional, Enterprise, or Unlimited, including the license grace period.
Manual `POST /api/v1/spoke-sync/run` and air-gap bundle export remain available
without a license. Bundle export stays manual.

With `edge_sync.spoke.enabled = true` and a valid paid license, the first
scheduled pass starts after `edge_sync.spoke.sync_interval` (default `5m`).
Failed or incomplete passes retry from `edge_sync.spoke.sync_retry_interval`
(default `30s`), doubling up to the normal interval and resetting after a
completed pass. Both durations must be at least one second, and the retry
interval must be shorter than the normal interval. Environment overrides are
`ARC_EDGE_SYNC_SPOKE_SYNC_INTERVAL` and
`ARC_EDGE_SYNC_SPOKE_SYNC_RETRY_INTERVAL`.

Each tick rechecks the paid-license entitlement and primary-writer role.
Losing either cancels an active scheduled pass; a running scheduler resumes
when eligibility returns. Manual and scheduled passes share an overlap guard:
a busy manual request returns HTTP 409. Shutdown cancels and joins scheduled
work before closing the ledger. Failed deliveries do not release the
compaction defer gate.

Scheduled spokes expose `arc_edgesync_spoke_scheduler_enabled`,
`arc_edgesync_spoke_last_success_timestamp_seconds`, and
`arc_edgesync_spoke_pass_failures_total` in Prometheus, with corresponding
`edge_sync_spoke_*` JSON metrics. The success timestamp advances only after
validated hub contact and a complete pass; an empty backlog, partial transfer,
or conflict cannot report successful replication. Skipped overlaps and
eligibility cancellations do not count as hub failures. These metrics are
absent when the scheduler was not started.

This combines work by [@jallegri](https://github.com/jallegri) in
[#1119](https://github.com/Basekick-Labs/arc/pull/1119) and
[@efegokdemir](https://github.com/efegokdemir) in
[#924](https://github.com/Basekick-Labs/arc/pull/924), with shared paid-license
checks and additional regression coverage.

### Backups can be scoped to one or more databases ([#1084](https://github.com/Basekick-Labs/arc/issues/1084))

`POST /api/v1/backup` was whole-instance: its body took only `include_metadata`
and `include_config`. It now also takes `databases`:

```json
{"databases": ["audit"]}
```

A scoped backup copies those databases and nothing else: their data files,
their schema anchors under `_schema/` and their compaction state under
`_compaction_state/` (parked `.quarantined` manifests included). The manifest
records the list in `scope`, the backup list and the status endpoint show it,
and the 202 echoes `databases`. `backup_type` stays `full`. An empty or absent
list is the whole-instance backup, unchanged.

Rules a scoped backup applies:

- `include_metadata` and `include_config` default to **false**. An explicit
  `include_metadata: true` is refused with 400: the SQLite database holds every
  database's tier rows, the tokens, the continuous queries and the audit log,
  so it cannot ride along with one database. `include_config: true` is allowed.
- Every name must pass the storage-segment rule (the one that names an existing
  database; no `..`, no separators, at most 256 names, no duplicates), and must
  be a database this node knows: its hot prefix has a file, or `_schema/<db>/`
  has an anchor, or the tier metadata has rows for it. The third rule is what
  accepts a fully cold database (an audit database with a long retention, the
  case that started this work) instead of answering 400. Its backup completes
  with zero data files: the cold tier is not copied yet (#1086), and the
  INCOMPLETE marker for that arrives with the remote-targets stage (#1085).
  Unknown names answer 400 naming them. The probes are bounded (one indexed
  query, then a listing that stops at the first object the storage would
  return), so a large database does not hold the request. A database whose only files have keys no listing
  can return is still recognised, by a fourth check that runs only after the
  three above said no, so the run fails with the rename advice rather than
  calling the database unknown.
- The databases' Iceberg namespace directories (`<prefix>_<db>.db/`) are
  excluded and counted (`iceberg_namespace_files_excluded`,
  `iceberg_namespaces_excluded` on the manifest): the Iceberg SQL catalog is
  instance-wide and rides with the metadata a scoped backup refuses, so
  restoring those files would land tables no catalog can resolve.
- Scope is the storage-root segment. Edge-sync spoke data lives under
  `<spoke>/<db>/…`, so `databases: ["prod"]` does not include `spoke1/prod`
  and `databases: ["spoke1"]` takes the whole spoke, anchors and compaction
  state included. A spoke namespace below the root segment cannot be named.

Restoring a scoped backup restores only its databases, in either mode. On a
cluster node, a restore of a scoped backup that does not name a `mode` runs in
`replace` — the mode a per-database backup is for (an additive restore of an
audit database resurrects everything retention removed since) — and the 202
echoes the effective mode. An explicit `"mode": "merge"` is honoured; a
standalone node stays additive, as before; an unscoped backup still defaults
to `merge`. In replace mode the current files to remove are selected by the
storage path's first segment when the backup is scoped (a spoke file's
manifest entry carries the canonical database, not the spoke). Replace is
refused, with 400, when the scoped backup holds no data files for one of its
databases: a fully cold database, or one dropped and re-created since, has
nothing in the backup to put back, so replace would only remove its current
files. The message points at `merge`, or at a fresh backup once the database
has hot files again. The cluster-wide compaction pause (#1087) is taken for a
scoped cluster restore exactly as for any other.

A request body that is not JSON (curl's `-d` default is form-encoded) now
answers 400 instead of being read as an empty request: for a scoping feature
the worst outcome is a malformed `databases` silently becoming a
whole-instance backup. An empty body still means the defaults.

Storage backends gained a bounded existence probe (`PrefixProber`) for the
known-database check; the local, S3 and Azure backends implement it and a
backend without it falls back to a listing.

A backup is a copy of the files on disk at the moment each is read: rows
still in the ingest buffers are not in it. "Point in time" means the last
flush.

Not in this stage: the cold tier (#1086), `arcli backup create --database`
(tracked in Basekick-Labs/arcli#44), and scoping below the storage-root
segment. A different backup target per database landed separately in #1085,
below.

### Backups carry the cold tier ([#1086](https://github.com/Basekick-Labs/arc/issues/1086))

A backup used to copy hot storage only. On a deployment with tiered storage on,
everything that had been migrated to the cold tier was simply not in it — which
for a long-retention database like an audit log meant most of the data. Arc now
backs up the cold tier too, and a restore puts each file back on the tier it
came from.

The manifest says how much came from cold:

```json
{
  "total_files": 1812,
  "cold_files": 412,
  "cold_size_bytes": 8294400
}
```

`cold_files` is also in the backup listing and per destination in `GET
/api/v1/backup/:id`, so with per-database routing you can see which target
holds the cold slice.

**Restores.** A file the backup read from cold goes back to this node's cold
tier, and its tier row is recorded — that row is what makes it queryable, so no
manual tiering scan is needed afterwards. Restoring onto a node with **no**
cold tier is still fine: those files land in hot storage and the status reports
`cold_files_restored_to_hot`, so the data is all there and all hot. Two smaller
counts cover the cases where a file arrives but stays unreadable:
`cold_restore_quarantine_skipped` (the path has a quarantined tier row, which a
restore must not resurrect) and `cold_rows_not_recorded` (the bytes landed but
the row could not be written).

**What this changes operationally.** Backups of tiered deployments get bigger
and slower, by roughly the size of your cold tier. There is deliberately no
switch to turn it off: a backup that silently omits most of a database is the
problem this fixes. If you want a smaller backup, scope it with `databases`.

**An unreachable cold store now fails the backup** rather than quietly
producing a smaller one. A missing hot file is tolerated and skipped, because
compaction or retention may legitimately have removed it between the listing
and the copy; nothing in Arc ever deletes a cold object, so a cold read that
fails means the store is unreachable, and continuing would report success over
missing data.

Three reconciliation counts appear when this node's tier metadata and its cold
store disagree. `cold_objects_unrecorded` counts objects in the cold store that
have no tier row — they are **backed up anyway**, because the store is the
authority on what exists, and refusing them would mean a node with incomplete
metadata backs up no cold data at all. `cold_files_excluded` now counts the
reverse: tier rows whose object is missing from the store, i.e. data that is
genuinely gone — and `cold_rows_stale_but_hot` counts the third case, a row
that says cold whose file is still in hot storage and therefore **is** in the
backup. That last one matters because without it the backup would report a file
it holds as permanently gone. Until 27.01.1 an unrecorded object was a
permanent condition rather than a lag on a standalone node, or on a cluster
without shared storage or replication, because the cold-metadata sync did not
run there; it does now (see "Cold-tier metadata is rebuilt on standalone
nodes"), so such an object is recorded on the next scan.

`cold_files_excluded` keeps its per-database breakdown in
`cold_files_excluded_databases`, so tooling built on the stage-B field keeps
working.

**Restore cold files on the primary writer.** A cold restore leaves the tier
row that makes the file readable, and if the node already had a hot copy at
that key the row flips hot-to-cold, leaving the hot copy stale. Tiering's
orphan reconciliation removes it — but that runs only on the migrating node,
and restores are not writer-gated, so a restore on a follower leaves the stale
copy behind.

Needs the tiered-storage licence, which tiering already required. A node
without it backs up no cold data and says nothing about it, the same hole the
`cold_files_excluded` marker has.

### A backup says how many cold-tier files it is not carrying ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

A backup copies hot storage. On a deployment with tiered storage on, the files
that have been migrated to the cold tier are not in it, and until now nothing
in the backup said so: a manifest that read `total_files: 1400` was describing
1400 hot files and however many cold ones you had, silently.

Every backup manifest now carries `cold_files_excluded`, and
`cold_files_excluded_databases` breaks it down per database, because a single
total does not tell you which database has data the backup is missing:

```json
{
  "backup_id": "backup-20261007-141500",
  "total_files": 1400,
  "cold_files_excluded": 412,
  "cold_files_excluded_databases": { "audit": 400, "metrics": 12 }
}
```

The figure also appears in the backup list, in `GET
/api/v1/backup/:id` per target (so you can see which destination holds the
gap, if you route databases to different targets), and in a warning the
restore logs:

```
WARN The backup being restored did not carry these cold-tier files: a backup
     copies hot storage only ... backup_cold_files_excluded=412
```

Three things to know about it.

**It does not block anything.** In particular a replace-mode restore of a
partly-cold backup still succeeds. Replace deletes the current files of the
databases it restores, so it refuses to run from a backup that is *incomplete*
— but a cold-tier object is not something the backup failed to get, it is
something no backup carries yet, and replace cannot delete it either (it
deletes through the cluster file manifest, which a migrated file has already
left). Refusing here would prevent nothing and would make tiered storage and
replace-mode restore mutually exclusive.

**It is the reporting node's view.** The count comes from this node's tier
metadata. On a cluster where one node migrates and the others learn about it
through the cold-tier metadata sync, a node whose sync has not run yet reports
a lower number. Take the backup on the primary writer for the full picture.

**The cold tier is still not backed up.** That is the next piece of this work
([#1086](https://github.com/Basekick-Labs/arc/issues/1086)). Until it lands,
`cold_files_excluded` is how you size what a restore from this backup would not
bring back — and the cold objects themselves are untouched and still readable
where they are.

The field is absent in four cases, which the JSON cannot tell apart: tiering
is off, nothing has been migrated, the count could not be taken (the backup
still completes and a warning names the failure) — and **this node has no
tiered-storage licence**. That last one is worth knowing, because it is where
the marker would help most: tiering is licence-gated at startup, so an
unlicensed node reports nothing even though its cold objects and tier metadata
are still there and its migrations have stopped. **An absent field is not
evidence that nothing was migrated.** A licence that lapses while Arc is
running keeps reporting; the next restart goes quiet.

### A different backup target per database ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

A backup had one destination. Now each configured target may name the databases
whose files go to it, and everything else goes to `backup.default_target`:

```toml
[backup]
default_target = "main"

[backup.targets.main]
type = "local"
local_path = "/srv/arc-backups"

[backup.targets.audit]
type = "s3"
s3_bucket = "acme-arc-audit"
databases = ["audit", "compliance"]
```

`databases` also takes a comma-separated string, and
`ARC_BACKUP_TARGETS_AUDIT_DATABASES=audit,compliance` overrides whatever the
file says. A database may be named by exactly one target: naming it twice is
refused at startup, naming it on the default target is allowed and does
nothing. The names are storage-root segments, exactly as a scoped backup names
them, so an edge-sync spoke is named as the **spoke** — `["prod"]` does not mean
`spoke1/prod`. A database's own field schema anchors and compaction recovery
state travel with its data to its target.

The instance-wide state always goes to the default target, because it belongs
to no one database: the SQLite database, the Iceberg SQL catalog, Iceberg table
metadata, an out-of-root Iceberg warehouse, and `arc.toml`.

**What a routed backup writes.** One manifest and one file sidecar per target,
each describing that target's own slice and each naming the whole run, so a
manifest found on its own is self-describing. Plus `<backup_id>/index.json` on
the default target, written before the first copy, naming every target the run
touched. A target that receives no files still commits an empty manifest and
sidecar, so a routed database whose name is a typo shows up as a leg with
`total_files: 0` rather than as a target the run failed to reach.

**The listing** returns one entry per backup id whatever it is spread across,
with `targets` naming every destination that holds a slice of it (`target`
stays set only when there is exactly one). It fans out over every target
concurrently, so one unreachable store no longer consumes the request budget
and cancels the rest: it is named in `unreachable_targets` and the response is
still 200 with everything the others hold. 503 is now only for a listing where
nothing could be read at all. A run that did not commit to every target it
named appears in `incomplete_runs`, marked `aborted` or `possibly_in_flight` —
the second because a cluster reader listing a shared destination has no view of
the primary's live run and must not report a healthy backup as aborted. A
target that would not answer is reported as `unknown_targets` on that entry
rather than as `missing_targets`: "missing" means the listing looked and found
no manifest, so a store that is briefly down no longer makes a complete backup
read as a run that did not finish.

**`GET /api/v1/backup/:id`** still answers the manifest at the top level, now
the merged view of the whole run, with a `targets` array of the per-target
slices beside it. **A delete** sweeps every target under one lock, and names
the ones it could not reach having deleted where it could, so a second delete
finishes the job.

**A restore** reads every target of the run and assembles the merged view
before any of its gates, so a scoped backup that lives on one target is not
mistaken for a backup holding nothing. It **refuses before writing anything**
when a target of the run is not configured on this node, will not answer, or
holds no manifest for the id. Restoring only the reachable part would report
success over a set it did not restore, and in replace mode would delete the
live files of a database whose backup bytes are on the target it could not
read. A backup with a single destination resolves no names at all, so one
whose target has since been renamed still restores.

`include_config` now defaults to false when **any** configured target is
remote, not only when the default one is: `arc.toml` carries every target's
credentials, so a local default beside one remote routed target still means
copying it puts the keys to that store inside a backup it holds. Asking for it
explicitly is honoured and warns, naming each remote target.

`/api/v1/backup/status` gains a `targets` array while a routed backup runs:
per-target files, bytes, skips and status.

A **target nothing is routed to is inert**: no backup writes to it, and a
warning at startup names it. It is deliberately not a refusal — adding the
target block in one commit and its `databases` in the next is ordinary — and
deliberately not a backup leg either. Every leg is probed before anything is
copied, so taking every configured target as a leg would make an unrelated
store a precondition of every backup in the instance, whole-instance ones
included.

In a listing, an entry whose targets could not all be read now says so.
`targets` names every destination **the run recorded**, the single-target
`target` field is cleared when there is more than one, and `partial_view: true`
marks an entry whose file and byte counts are summed over fewer legs than that.
The matching `incomplete_runs` entry says which target is unaccounted for, and
its `state` is `undetermined` when nothing is known to be missing — a complete
backup read while one store is briefly down is not a run that failed.

Only the **comma** separates names in `databases`. A database name may contain
a space, so `databases = "my db"` is one name and `databases = ["a", "b"]` is
two.

Startup now also refuses two backup targets that contain one another — same
bucket with one prefix a parent of the other — for the reason it already
refuses an overlap with primary storage or the cold tier: the two would be one
listing, so each leg would enumerate the other's objects and deleting one
backup id would reach both. Two targets in one bucket under disjoint prefixes
stay legal.

Not in this stage: `cold_files_excluded` (#1086 is still the cold tier), backup
scheduling, and incremental backups.

### A backup can be written somewhere other than a local directory ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Until now a backup went to `backup.local_path`, a directory on the server
itself. A named target can now point it at S3, MinIO, Azure Blob Storage, or a
different local path.

```toml
[backup]
default_target = "audit"

[backup.targets.audit]
type = "s3"                   # local | s3 | minio | azure | azblob
s3_bucket = "acme-arc-audit-backups"
s3_region = "eu-west-1"
s3_prefix = "arc/"            # optional, and it counts against the key budget
```

With a target configured, `backup.local_path` is not required at all. Nothing in
the backup path needs local scratch space: the database snapshot is staged
beside the database, and a restore stages beside its destination.

Two details worth knowing before you write a target.

**Target names take lowercase letters, digits and underscore only.** A hyphen is
refused on purpose, and this contradicts the example in the original issue. The
reason is that configuration keys are lowercased and a hyphen cannot appear in
an environment variable name, so a hyphenated target could be set in a file and
then never be overridable from the environment. `audit_bucket`, not
`audit-bucket`.

**A target is refused at startup if it overlaps primary storage or the cold
tier.** Overlap means the same store and bucket with one prefix equal to or a
parent of the other, or, for local paths, one directory containing the other.
The same bucket with disjoint prefixes is fine. See the upgrade note above for
what this means for an existing deployment.

The field names match the cold tier's, so `s3_bucket` rather than `bucket`. From
the environment alone it takes three variables, because a target name cannot be
discovered from a configuration file that does not exist:

```
ARC_BACKUP_TARGET_NAMES=audit
ARC_BACKUP_TARGETS_AUDIT_TYPE=s3
ARC_BACKUP_TARGETS_AUDIT_S3_BUCKET=acme-arc-audit-backups
ARC_BACKUP_DEFAULT_TARGET=audit
```

Three behaviours change when the target is remote. `include_config` defaults to
false, because `arc.toml` carries that target's credentials — with per-database
routing the rule widened to **any** configured target being remote, as
described above. The usable source-key length shrinks by the length of the
target prefix, and the figure Arc reports to you accounts for it. And a prefix
long enough to leave no room for a backup's own keys is refused when the
configuration loads rather than later.

**Backups now record which instance wrote them,** so two Arc instances sharing
one bucket and prefix do not merge their listings. The identity is
`cluster.cluster_name` on a cluster and a generated identifier otherwise, which
means **you should set `cluster.cluster_name`**: two unrelated clusters both
left at the default would read as one.

A restore never adopts the identity of the backup it reads, so recovering onto
replacement hardware leaves the new machine with its own identity and the old
backups marked as another instance's. Those are hidden from the listing by
default; add `?include_foreign=true` to see them, and the listing tells you when
it has withheld any. Restoring one is allowed, and logs whose backup it was.

Per-database routing, where different databases go to different targets, is
described above; this section is the single-destination half it was built on.

### Azure Blob Storage takes a key prefix ([#1102](https://github.com/Basekick-Labs/arc/issues/1102))

S3 has always had `storage.s3_prefix`, so one bucket could hold Arc's data
under `arc/` beside something else, or two Arc deployments under disjoint
prefixes. Azure had no equivalent: the key was the blob name, so a container
could hold exactly one Arc deployment and nothing else.

Two new keys close that:

```toml
[storage]
backend = "azure"
azure_container = "arc"
# azure_prefix = "instances/abc123/"

[tiered_storage.cold]
backend = "azure"
# azure_prefix = "cold/"
```

Both default to empty, which is the container root and byte-for-byte the
behaviour you have today. Set one and every key Arc writes, reads, lists,
stats and deletes moves under it, including the one that leaves the process:
the compaction subprocess receives the prefix in its job configuration, so
compaction reads and writes under the prefix rather than at the container root.
The DuckDB secret Arc creates for the query engine is scoped to the container
**and** the prefix rather than the whole container. That last one is what lets a primary store and a cold tier
share one container distinguished only by prefix, which was not previously
expressible.

The prefix is validated, not repaired. A value that cannot form usable keys
(an empty segment, a leading slash, a space, a character outside
`A-Za-z0-9/._-`) is rejected when the configuration loads, with the offending
value named, rather than being silently rewritten into something that writes to
the container root. That applies to the cold-tier key too, and to the two S3
prefix keys, which are now checked in the same place. The cold-tier keys are the
reason the check sits at load: a bad `tiered_storage.cold.s3_prefix` was
reported as a failure to build the backend and then left the node running with
no cold tier at all, which is a silent outage until someone reads that one log
line. A trailing slash is added if you leave it
off, so `arc` and `arc/` are the same destination.

### Backup and restore runs have a configurable timeout ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Both runs were bounded by a hardcoded two hours. That is a guess that is
simultaneously too long for an operator who wants a stuck backup to give up
and alert, and too short for a first backup of a large dataset.

```toml
[backup]
# operation_timeout = "2h"
```

The default is the old value, so nothing changes unless you set it. A zero,
negative or unparseable value is a startup error naming the value rather than
an unbounded operation.

## Bug fixes

### Fix: an Arrow query served by arcx honours the governance row cap ([#727](https://github.com/Basekick-Labs/arc/issues/727))

A `POST /api/v1/query/arrow` query that arcx served ignored the token's
governance `max_rows_per_query` and streamed every row; only the DuckDB path
stopped at the cap. It also declared only the `Arc-Stream-Truncated` trailer,
so a client, or a compactor forwarding the query (#1567), never saw
`Arc-Rows-Capped` or `Arc-Execution-Time-Ms` from it.

Now the arcx path streams through the same writer as the DuckDB path:
- The result stops at the cap, `Arc-Rows-Capped` names it, and the query's
  history entry records it, as on DuckDB. An Arrow Flight SQL query that arcx
  serves now ends with `ResourceExhausted` at the cap, as the Flight SQL
  documentation already said; before, it returned every row with no error.
- All three trailers are declared. `Arc-Execution-Time-Ms` is measured from
  the same point as on DuckDB (when the request arrived), and is set on every
  response, including a failed one.
- A query whose timeout passes, or that is cancelled, while arcx is still
  computing the result now answers `504` (or `500` for a cancel, `503
  "Server shutting down"` when shutdown ended it) before any body, as on
  DuckDB. Before, it answered `200` with a body marked truncated.
- A deadline that expires mid-stream counts in `arc_query_timeouts_total`, and
  the stream's error, disconnect and truncation-reason accounting is the
  DuckDB path's.
- A completed query logs the `Arrow streaming query completed` line, and one
  slower than `query.slow_query_threshold_ms` logs `Slow query detected` and
  counts in `arc_slow_queries_total`, as on DuckDB. Neither happened for an
  arcx-served Arrow query.
- The arcx reader is released as soon as the response ends. A result stopped
  early (at the cap, on a cancel or an error) used to keep its last batch of
  native memory and park arcx's decode workers until a Go garbage collection;
  the same applied to arcx JSON, msgpack and shadow queries, which now
  release theirs too.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1294](https://github.com/Basekick-Labs/arc/pull/1294).

### Fix: a token's `expires_in` must be in the future, and a rename cannot clear its name ([#1623](https://github.com/Basekick-Labs/arc/pull/1623))

`POST /api/v1/auth/tokens` and `PATCH /api/v1/auth/tokens/{id}` accepted a
zero or negative `expires_in` (`"-24h"`, `"0d"`) and answered success for a
token that had already expired, so it could not authenticate. A day count too
large for Go's duration type, positive or negative, overflowed into an
arbitrary expiry, past or future (`"99999999d"` gave 2246, `"-177004d"`
2126). Both routes now answer `400`: `"expires_in must be positive"`, or
`"expires_in is too long: at most 36500d (100 years)..."` for anything over
100 years, and store nothing. A token that should not expire omits
`expires_in`, as before. An unreadable `expires_in` on `PATCH` now gets the
same message as on create, naming the accepted forms.

`PATCH` with `"name": ""` renamed the token to an empty name, which create
refuses. It now answers `400 "Token name cannot be empty"`.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar).

### Fix: token routes answer `409` for a taken name or a revoked token, and `404` for an unknown ID ([#1625](https://github.com/Basekick-Labs/arc/issues/1625))

`POST /api/v1/auth/tokens` with a name another token already has answered
`500` and logged an error, and so did `PATCH /api/v1/auth/tokens/{id}` renaming
a token to such a name. Both now answer `409 "token with name '<name>' already
exists"` on a standalone node and in a cluster. Before, only a standalone create
used that text: a standalone rename answered `Failed to update token: UNIQUE
constraint failed: api_tokens.name`, and in a cluster the message carried the
`cluster apply failed` wrapping (and, on a follower, the `forward apply: leader
rejected` prefix). The first token is unchanged.

On a cluster node, `PATCH`, `DELETE`, `POST .../rotate` and `POST .../revoke`
for an ID with no token answered `200`, because the cluster applies a write to
an unknown token as a no-op. `rotate` even returned a `new_token` that
authenticated nothing. They now answer `404 "Token not found"`, as a standalone
node already did, and propose nothing. A `PATCH` with no fields for an unknown
ID is now a `404` on both. As with the RBAC routes, a follower that has not yet
applied a token created elsewhere answers `404` for it for that short moment.

`POST .../rotate` on a revoked token answered `200` with a `new_token` that
could never authenticate: revocation is permanent and no route re-enables a
token. It now answers `409`, and the token stays revoked. Create a new token
instead; a new token has a new ID, so re-attach its governance policies and
team memberships. Expired tokens can still be rotated, and revoking an already
revoked token still answers `200`.

In a cluster, a rotate is checked against the node's own copy of the token
before it is proposed, and again after it has been applied: if the token was
revoked or deleted on another node just before, the rotate answers `409` or
`404` instead of a `new_token` that never authenticates. On the node that leads
the cluster, that second check sees every revoke and delete ordered before the
rotate. On a follower it does too once cluster policies are enabled, as long as
the follower's wait to apply the rotate itself does not time out. A follower
that has not yet applied a revoke or delete made elsewhere, even one already
acknowledged, can still answer `200` with a `new_token` that never
authenticates, for a revoke, or for a delete, where it would otherwise answer
`404`: before cluster policies are enabled, or when that wait times out. The
revoked or deleted token stays unusable either way.

Duplicate-name and unknown-ID parts contributed by
[@officialasishkumar](https://github.com/officialasishkumar) in
[#1622](https://github.com/Basekick-Labs/arc/pull/1622).

### Fix: execution history answers `404` for an unknown continuous query or retention policy ([#1624](https://github.com/Basekick-Labs/arc/pull/1624))

`GET /api/v1/continuous_queries/{id}/executions` and
`GET /api/v1/retention/{id}/executions` answered `200 {"executions": null}`
for an ID with no continuous query or policy behind it, so a typo or a deleted
definition looked like one that had never run. In a cluster, a deleted
definition's history could still be listed for a moment after the delete,
until the node's own tables caught up with the cluster. Both routes now answer
`404 "Continuous query not found"` or `404 "Retention policy not found"`, as
`GET /{id}` already did, and `503` while the node is not yet able to read the cluster's definitions, as
`GET /{id}` does. A definition that has not run yet lists `"executions": []`
instead of `null`.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar).
### Fix: a plain-path `iceberg.warehouse` inside the storage root is recognised ([#1616](https://github.com/Basekick-Labs/arc/issues/1616))

On local storage, `iceberg.warehouse` set to a plain path (for example
`/data/wh`, without `file://`) was treated as outside the storage root even
when it was inside it. Arc published no `version-hint.text` or
`v<N>.metadata.json`, so DuckDB and Spark could not find the tables by
directory, and logged that the warehouse was not under the storage root. It
also skipped tightening the metadata files' permissions, which stayed
world-readable (`0644`).

Now:
- A plain path, absolute or relative to the working directory, is turned
  into the `file://` URI of its directory at startup, so new tables get
  `file://` locations like the default warehouse's.
- Tables created before keep their plain-path locations, which Arc now
  recognises: their version hints are published, their metadata permissions
  tightened, and old versions pruned, on the next export pass.
- **A database delete now removes these tables' warehouse metadata**, as it
  always did for a `file://` warehouse. Before, it left the metadata behind.
  Tables in the misspelled directories of the last bullet are still left.
- The `file` scheme is matched in any case and with one slash (`file:/data`),
  as iceberg-go accepts, by the export and by backup. `file:data` (no slash)
  is not a path and is left alone.
- Metadata permissions are tightened only inside the configured warehouse.
- A plain path with a space, a leading `../`, or a dotted namespace (#1129)
  was written by iceberg-go to a directory spelled differently from the
  configured one (`my%20wh`, the `..` dropped). Existing tables there stay
  where they are; new tables go to the configured directory.

### Fix: a backup's Iceberg metadata matches its catalog, and a restored catalog never rolls a table back ([#1613](https://github.com/Basekick-Labs/arc/issues/1613))

A whole-instance backup listed the in-root Iceberg warehouse at the start,
copied the data, snapshotted the SQLite catalog, and only then copied the
metadata from that early listing. The export keeps committing through the
data copy, so the backup could hold catalog rows pointing at metadata it never
copied. The version hint, read at copy time, could also name a `v<N>` the
backup did not hold. A restored node then failed to export that table on every
pass.

Separately, restoring a catalog behind its warehouse on **local storage** (a
restore in place, or any restore whose warehouse is newer than its catalog)
let the export commit from the stale row. On a table with no new files it
quietly moved the version hint back, and the orphan sweep then deleted the
newer manifests as unreachable. S3 warehouses already avoided this.

Now:
- **Backup:** the in-root Iceberg metadata is listed after the SQLite
  snapshot, so the metadata a backed-up row references is in the backup
  (unless the table commits more than `retain_snapshots` times during the
  copy; the restore then recovers the row onto the newest version the backup
  holds). Each table's version hint is copied last, pointed at the newest
  `v<N>` the backup holds when the live hint is ahead of it (logged at Info).
  A hint that cannot be read is skipped like a data file.
- **Every warehouse adopts forward:** each pass moves a table's catalog row
  onto the newest version published in its warehouse when the row is behind,
  on local storage as on S3. A restore writes the backup's version hint over
  the live one, so the first check of each table after a restart also lists
  its `v<N>` files and takes the newest. This costs one hint read per table
  per pass, plus one listing per table per process. A warehouse outside the
  storage root publishes no hint and is unaffected.
- **A hint naming a missing version:** adoption and dead-row recovery fall
  back to the newest `v<N>` present instead of failing every pass. A live row
  is never moved to an older version, and a table with no catalog row still
  fails rather than adopt an older version (a database delete in progress
  removes the `v<N>` files before the hint).
- The export is not paused for a backup, so the backed-up warehouse can be
  ahead of its catalog. A restore adopts forward to it.

### Fix: Iceberg export recovers after a database is dropped and recreated on a shared bucket ([#1611](https://github.com/Basekick-Labs/arc/issues/1611))

A database delete runs on the primary writer. On a shared bucket it deleted
the database's Iceberg warehouse objects, which every node shares, but only
the primary's own catalog rows. Another node that had exported the table (a
former compactor-lease holder) kept rows pointing at deleted metadata. Once
the database was recreated, that node's export of the table failed on every
pass with "the table is in the catalog but its metadata file is missing",
until an operator deleted the row by hand.

When a node had never exported the database, which is the usual case for a
primary writer when another node holds the export, the delete did nothing to
Iceberg at all: the old table's history carried on after a recreate. It still
logged that the Iceberg artefacts had been removed.

Now:
- **Missing metadata:** when a catalog row's metadata file is gone and the
  warehouse publishes the table, the row moves onto the published version,
  whatever its version number. This holds on every backend, and also heals a
  row restored ahead of its warehouse files (see #1613 below).
- **Nothing published:** the row is forgotten and the table exported anew,
  but only on a shared bucket in a cluster, and only when the delete left a
  drop marker for that table. Markers live under the reserved
  `_iceberg_dropped/` root directory, which listings ignore and backups do not
  copy. The delete writes them before deleting anything, so a failed delete,
  re-run, writes them again. A table's marker is removed by its next
  successful publish. A standalone node keeps the existing error (restore the
  warehouse: its whole-instance backup holds it).
- **Database delete:** removes the shared warehouse objects even on a node
  that never exported the database.
- **Paths:** a local warehouse path whose directories are gone is now matched
  through the same symlink resolution as the warehouse, so it is recognised.

### Fix: Iceberg export on a shared bucket without `shared_storage_mode` ([#1609](https://github.com/Basekick-Labs/arc/issues/1609))

On a cluster whose nodes share an S3 bucket without
`cluster.shared_storage_mode` (single-writer shared storage, #1603/#1606),
Iceberg export treated the warehouse as one node's. Three decisions keyed on
the flag rather than on the bucket.
- **Adoption.** The version published in the warehouse was not adopted before
  a node loaded its own catalog row. After the export moved between nodes
  with the compactor lease, a node with a stale row committed from it, lost
  the create-only fence on each pass, and then published its own lineage once
  it caught up. The data stayed current, but the snapshot history skipped the
  other node's versions, which breaks incremental readers.
- **Manifest barrier.** Files this node's manifest did not know yet were
  exported without the barrier.
- **Gate.** The export gate fell back to the static compactor role.

Iceberg now decides "shared" from the bucket.
- **Adoption** runs on every S3 warehouse. That includes a standalone S3 node,
  whose catalog a backup restore can leave behind its warehouse. It costs one
  catalog read and one version-hint `GET` per changed table per pass. A
  version hint that is not a number (only an outside writer can produce one)
  now stops that table's export until it is fixed, instead of being
  overwritten.
- **Adoption before the first load.** The scheduler's first loads after a
  restart now adopt too. A catalog row whose metadata file another node had
  pruned used to fail that measurement on every pass, even in
  `shared_storage_mode`.
- **The manifest barrier** applies on any shared bucket. A listed file the
  manifest still does not know after one barrier (for example, written before
  the cluster existed) is remembered, so it no longer costs a Raft barrier on
  every pass.
- **The compactor lease** gates the export where compactor failover runs.
  Without it (or on a cluster node without a clustering licence), every
  compactor-role node exports, and a startup Warn says to run exactly one or
  to enable `cluster.failover_enabled`.

### Fix: `cluster.replication_enabled` is ignored on an object-store backend ([#1605](https://github.com/Basekick-Labs/arc/issues/1605))

Peer file replication copies Parquet files between nodes' own disks. On a
shared object-store bucket, every node already reads every file, yet the
puller still started when `cluster.replication_enabled` was set.
- **Load:** every catch-up pass (at startup and every 5 minutes by default)
  stat'ed the manifest's files, one S3 `HEAD` per file, per node, per pass.
- **Blocked reads:** a file no peer could serve kept catch-up unfinished, so
  with `cluster.query_gate_on_catchup` set that node answered queries `503`.
  A `DELETE` with `allow_unreachable` could also wait on such a file.

Arc now ignores the setting on an object-store backend, with a startup Warn,
as the Helm chart's shared mode already does. Such nodes now behave as
shared-storage nodes with replication off already did:
- `/api/v1/cluster/status` reports no catch-up status;
- the catch-up query gate never closes;
- a stream takeover no longer probes peers for a registered file missing from
  the bucket;
- staged compaction adopts outputs without the puller;
- the "no compactor" startup Warn for replicating clusters does not apply. The example
`deploy/docker-compose/enterprise-shared` stack no longer sets it on its
reader.

### Fix: the Helm chart can run shared storage without the multi-writer licence ([#1606](https://github.com/Basekick-Labs/arc/issues/1606))

The Enterprise chart's `storage.mode: shared` always set
`cluster.shared_storage_mode`, which refuses to start without the
`shared_storage_multi_writer` licence feature, even with a single writer.
Its comment called the flag a no-op for one writer; it was not.

The new value `storage.shared.multiWriter` chooses:
- **`true`** (default, unchanged): Pattern 2 multi-writer.
- **`false`**: single-writer shared storage. One elected primary takes every
  write, the other writer pods are standbys that forward writes to it, and
  `cluster.failover.enabled` replaces a lost primary. It needs the
  `clustering` licence feature (and `writer_failover` for failover), not
  `shared_storage_multi_writer`.

The README documents both modes, and `NOTES.txt` names the one rendered.

In that single-writer configuration, Arc now refuses to start a node licensed
for clustering whose cluster coordinator did not start (Raft or TLS). It used
to log a Warn and run tiering, retention, continuous queries and deletes
ungated against the shared bucket. Shared-storage mode already refused this
state. A node without a clustering licence still only warns.

### Fix: a single-writer cluster on an object store runs tiering on the primary only ([#1603](https://github.com/Basekick-Labs/arc/issues/1603))

A cluster on an object-store backend without `cluster.shared_storage_mode` is
the single-writer form of Pattern 2. Every node shares one bucket, one primary
writer is elected, and writes go to it. It needs no multi-writer licence. Its
tiered storage was not gated, because the gate was keyed on the flag rather
than on the bucket. Every node, readers included, migrated the shared bucket,
ran the hot-only cold cleanup and orphan reconciliation, and reported the
other nodes' moves as missing. Tiering now recognises a shared bucket from the
storage backend, as reconciliation already did. Only the primary writer
migrates; the other nodes sync their tier metadata and retire cold rows the
primary removed (#1563).

Tiering now follows the primary's availability, like retention and
continuous queries: it pauses until the first primary is elected, and with
writer failover off, after the primary is lost until an operator promotes
another.

In that configuration:
- A node with the default `cluster.role` (`standalone`) and a clustering
  licence now refuses to start, as it already does with
  `cluster.shared_storage_mode`. It never passes the primary-writer gate:
  retention, continuous queries and deletes already ran on no node, and with
  this fix tiering would too. Without a clustering licence no coordinator is
  built and the node is not refused.
- An empty `cluster.raft_data_dir` (no primary is elected, so every writer
  acts as primary) logs a Warn.
- So does a node whose cluster coordinator is not running because the
  licence lacks clustering, which then runs everything ungated against the
  shared bucket. A licensed node whose coordinator failed to start is refused
  (#1606).

### Fix: a local-storage cluster without replication warns, and its readers stop reporting a writer's deletes as lost ([#1599](https://github.com/Basekick-Labs/arc/issues/1599))

A cluster on local storage with `cluster.replication_enabled=false` (the
default) is neither supported cluster pattern. No node reads another's disk:
queries see only the receiving node's own files plus the shared cold tier, and
readers hold no hot data. Arc now logs a Warn at startup in that
configuration, and says to set `cluster.replication_enabled=true` on every node.

Its tiered storage was ungated on every node, so a reader recorded the writers'
cold objects as its own rows. After a writer's `DELETE` or retention removed
them, the reader kept the rows for good and reported them as lost data
(#1551). Readers and compactors there are now gated like readers in the
supported patterns. They only sync their tier metadata, and retire such rows
(#1563). Writers stay ungated, each migrating the files on its own disk, and
so still report cold rows another node deleted as lost; the warning says so.
Enabling replication is the fix.

`DELETE /api/v1/databases/{name}` now runs only on the primary writer in every
cluster. In that configuration any node could run it, a reader included,
deleting every cold object of the database and every node's manifest entries
for it. Two configurations now refuse it where they did not:
- a cluster node on the default `cluster.role` (`standalone`), where row
  `DELETE`, retention and continuous queries were already refused;
- a cluster on an object-store backend without `cluster.shared_storage_mode`
  (#1603).

The example Kubernetes manifest (`deploy/kubernetes/statefulset.yaml`) ran its
writers without replication while its reader had it on. It now enables
replication on the writers too.

### Fix: cluster readers retire cold rows the primary removed ([#1563](https://github.com/Basekick-Labs/arc/issues/1563))

A cold `DELETE`, a retention run or a database delete runs on the primary
writer. It removed the cold objects and the primary's own tier rows, and
nothing removed the rows on readers. Every reader kept them for good. Each
cycle it logged `Cold tier rows have no object in cold storage`, and a reader
promoted by failover reported those rows as lost data (#1551).

A reader's cold-tier sync now retires such a row itself. It acts only when the
cold object and the hot copy are both still absent at the moment it acts, and
the row is still cold and not quarantined, the same evidence the primary's
`DELETE` and retention act on. After each retirement it checks the cold
object again: if the object is there, a writer raced the check, and the row
is written back.

A reader retires nothing in these cases, and logs why:
- a restore is running on it;
- the cluster's cold mark was seeded from a different cold store than its own
  (#1552);
- the store identity is unknown and the cold listing came back empty.

The pass is bounded by time, not by row count, so a day of retention clears in
one sync. It never takes more than half of what is left of the scan's budget.
It deletes rows in batches, one tier-cache invalidation per measurement. The
first syncs after an upgrade may take several cycles to clear the backlog of
everything the primary ever removed; each logs how many rows are left. The
reader compares cold stores by their configured identity (backend, endpoint,
region or account, bucket and prefix), so every node's `tiered_storage.cold`
settings must be spelled identically.

A cold row whose object is gone but whose hot copy the manifest still lists
(the primary rolled a move back before unlinking the hot copy, for example
after a compaction took the file over) is now pointed back at hot on readers
too. Before, that happened only for hot-only databases. A reader that kept such a row cold
refused its own hot registration over it, so a measurement whose only hot file
it was could not be read on that reader.

New counter: `arc_tiering_cold_rows_retired_by_reader_total`. A reader cannot
tell the primary's removal from an object deleted outside Arc (a bucket
lifecycle rule). The primary still reports the latter as lost for as long as it
leads. After a failover, the new primary no longer does, because its rows were
retired while it was a reader.

### Fix: deleting a continuous query stops its scheduled runs

On a standalone node, `DELETE /api/v1/continuous_queries/{id}` removed the
continuous query but left its scheduler job running. The job kept firing at
the query's interval until the next restart or `POST
/api/v1/schedulers/cq/reload`. Each run failed on the missing definition and
logged `Scheduled CQ execution failed` at Error, and `GET /api/v1/schedulers`
kept listing the deleted query among its jobs. The delete now stops the job,
and an update or `POST /api/v1/schedulers/cq/reload` that runs at the same
moment can no longer start it again. In a cluster, the job of a deleted
continuous query was already stopped.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1600](https://github.com/Basekick-Labs/arc/pull/1600).

### Fix: governance policy routes answer `409` and `404` instead of `500` and `200`

`POST /api/v1/governance/policies` for a token that already had a policy
answered `500 "Failed to create policy"` and logged an error, so an ordinary
mistake looked like a server fault. It now answers `409 "A policy already
exists for this token"`; change the existing policy with `PUT`.
`DELETE /api/v1/governance/policies/{token_id}` for a token with no policy
answered `200 "Policy deleted"`. It now answers `404 "No policy found for this
token"`, as `GET` and `PUT` already did, so a second `DELETE` is a `404`.
Automation that deletes a policy unconditionally should accept `404`.

A `PUT` whose policy is deleted between its existence check and its write
now answers `404` too. It used to answer `200` and put the deleted policy back
into the in-memory cache, where it was enforced until a restart. More
generally, policy creates, updates and deletes no longer interleave, so the
enforced policies always match the stored ones. Before, a `DELETE` racing a
`POST` or `PUT` of the same token could leave a deleted policy enforced, or a
stored one not enforced, until a restart.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1601](https://github.com/Basekick-Labs/arc/pull/1601).

### Fix: an unknown MQTT subscription ID is answered `404`

`/api/v1/mqtt/subscriptions/{id}` answered an ID with no stored subscription
in three different ways. `GET` answered `404`. `PUT`, `DELETE`, `POST
.../start`, `POST .../restart` and `GET .../stats` answered `500 "Failed to
... subscription"` and logged an error, so a typo, or a subscription another
admin had already deleted, looked like a server fault. `POST .../stop` and
`POST .../pause` answered `409 "subscription not running"`, as if the
subscription existed.

Every one of these routes now answers `404 "Subscription not found"` for such
an ID, logged at Debug. A second `DELETE` of the same subscription is a
`404`. A stored subscription that is not running still answers `409` to
stop and pause. `GET` on a subscription the store cannot read is now a
`500`. It answered `404` for any failure, so a store fault looked like a
deleted subscription.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1596](https://github.com/Basekick-Labs/arc/pull/1596).

### Fix: an MQTT subscription request racing its `DELETE` answers `404` ([#1597](https://github.com/Basekick-Labs/arc/issues/1597))

A stop, pause, start or restart of a subscription that a concurrent `DELETE`
removed mid-request answered `500` and logged an error, instead of the `404`
every route gives an unknown ID. A start could also go the other way: if the
`DELETE` completed between the start reading the subscription and reserving
it, the start answered `200` and ran a subscriber for a subscription that no
longer existed.

`DELETE` now removes the stored subscription first and then stops whatever
runs under it. A start reserves the subscription before reading it, and once
its subscriber is running it reads the subscription again, stopping the
subscriber if it is gone. A request that loses this race answers `404`, and
no subscriber outlives its subscription unless that last read fails (logged
at Warn). A `DELETE` that fails to remove the subscription no longer stops
it first, so it stays stored and running. The status write of a subscriber
stopped after its subscription was deleted is logged at Debug, not Error.

### Fix: a governance 429 says when a retry can succeed ([#1519](https://github.com/Basekick-Labs/arc/issues/1519))

With query governance on, a refused query's `Retry-After` was wrong or missing:

- **A rate limit** answered with the time left in the window's current slot, so
  `Retry-After: 1` after a burst on a per-minute limit and about `60` at most on
  a per-hour limit, whatever the limit's real state. A client that honoured it
  was refused again each time it came back, about 60 times per window, each a
  log warning and a count in the rate-limit metric. It now answers with the
  time until enough of the oldest requests have left the window to bring the
  count under the limit: about 60 seconds after a burst on a per-minute limit.
  When the per-minute and per-hour limits are both exhausted, it answers with
  the longer of the two waits.
- **A quota** (`max_queries_per_hour`, `max_queries_per_day`) answered with no
  `Retry-After` at all. It now carries the seconds until the exhausted quota
  resets, or until the daily reset when both are exhausted. The rejection's log
  line gains the `retry_after_sec` field the rate-limit line already has.

A quota now resets exactly on the hour or day boundary, not just after it, so a
client that waits exactly `Retry-After` is not refused. `Retry-After` is always
at least 1. See also #1524 below: a refused query no longer counts against any
limit. Clients that retry on `Retry-After` will retry
later than they did, and less often.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1520](https://github.com/Basekick-Labs/arc/pull/1520).

### Fix: a governance-refused query is counted against no limit ([#1524](https://github.com/Basekick-Labs/arc/issues/1524))

The rate limits and quotas were checked one after another, each counting the
query as it passed: a query refused by the per-hour limit had already used a
per-minute slot, and one refused by a quota had used both rate limits. A client
that kept retrying after a refusal used up the earlier limits too, and could be
refused by one of those when it came back on time. The limits are now checked
together and a query is counted against all of them only when every one admits
it: a refused query spends nothing, and a client that waits `Retry-After` finds
every limit as it was told. A refusal names the limit with the longest wait
(the reason in the 429, the log line, and `arc_governance_rate_limited_total`
or `arc_governance_quota_exhausted_total`), and `Retry-After` is that wait.
Refused queries no longer appear in the counts the governance usage endpoint
reports. When the hourly and daily quotas are both exhausted, the refusal now
names the daily one, whose reset it waits for. A client that keeps hammering
after a refusal is still refused on every request until the wait has passed.

### A scheduled compaction that finds a cycle running runs when it ends ([#220](https://github.com/Basekick-Labs/arc/issues/220))

The hourly and daily compaction schedulers share one cycle claim with the manual
trigger. A tick that found the claim held logged an Info line and did nothing
until its next tick, which costs the hourly tier an hour and the daily tier a
day. If the collision repeats, daily compaction never runs. At the default
schedules it happens whenever the 03:00 daily cycle is still running at the
03:05 hourly tick; schedules that share a minute, a raised
`compaction.cycle_timeout` or a manual cycle at the tick cause it too.

Such a tick now runs once, as soon as the claim is released. It passes the same
compactor lease and cluster-wide pause checks as any tick when it does. It gives
up at the scheduler's next tick, and a tick lost that way is logged at Warn and
counted in the new `arc_compaction_scheduled_skips_total{tier}`. If the claim is
released just before that next tick, the deferred run starts and the next tick
waits behind it, so the tier runs twice in a row. Stopping compaction, at
shutdown or when this node loses the compactor lease, tells both schedulers to
stop before waiting for either, so a waiting tick never starts a cycle then.
The schedules, the per-tier API trigger and the status endpoint are
unchanged.

This also removes a rare deadlock: `Stop` could wait forever for a tick that had
just started, which was itself waiting for the lock `Stop` held.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar)
([#1507](https://github.com/Basekick-Labs/arc/pull/1507)).

### `allow_unreachable=true` stops waiting for files nothing is delivering ([#1426](https://github.com/Basekick-Labs/arc/issues/1426))

On a cluster, a DELETE waits for files the manifest lists that are not on the
primary yet: replicated files and compaction outputs still on their way. It
waits up to 30 seconds, then answers `503` naming them in
`unreachable_files`, or with `allow_unreachable=true` runs without them. With
the flag it still waited the full 30 seconds first, once per scan, even for a
file no node holds any more. A DELETE can scan several times, before and after
it is recorded and after a compaction replaced a file it read, so it could
take minutes. A client with a 30-second timeout saw a connection failure for
a DELETE that ran, and lost the list of files it skipped.

With `allow_unreachable=true`, a missing file is now waited for only while
something is delivering it: the replication puller has it queued or in
progress, or on shared storage a staged compaction holds it. A file missing
for 5 seconds with nothing delivering it is given up for the rest of the
request and reported in `unreachable_files`. A file still on its way is waited
for up to 30 seconds, as before. Without the flag nothing changes.

The rows of a file in `unreachable_files` are not deleted. Run the DELETE
again once the file is on the primary to remove them. On shared storage, the
flush of the other nodes before a DELETE keeps its own timeouts.

### Storage errors stay readable in the log ([#1113](https://github.com/Basekick-Labs/arc/issues/1113))

Arc masks every quoted span in a logged error, because engine errors quote the
values they refuse, and it logs only an error's first line. Storage errors that
quoted the offending value lost exactly that part in the log, and a value with
a quote in it masked the rest of the line:
`storage prefix "..." contains an unsupported character '...'`.

Storage errors no longer quote values. They say what is wrong instead:

- An unsupported character in an object-store prefix, or a glob metacharacter
  in a path, is named by its code point (`U+0027`), and the prefix error lists
  the allowed characters.
- Key and prefix errors describe the problem (an empty, `.` or `..` segment, a
  trailing separator, the write-staging suffix, a length over the limit)
  without repeating the key. A database name starting with `_` is refused as
  reserved.
- An unknown backend type, a GCS endpoint that is not an http(s) URL, and a
  GCS bucket Arc cannot use are described, and the bucket is named unquoted.
- A batch delete on S3, Azure or GCS logs each key it refuses as a warning
  with the key as a `key` field, and names the key in front of its reason in
  the returned error. Only the first line of the batch error reaches the log.

Contributed in part by [@efegokdemir](https://github.com/efegokdemir) in [#1326](https://github.com/Basekick-Labs/arc/pull/1326).

### A delete's WHERE may qualify columns by the measurement name ([#1451](https://github.com/Basekick-Labs/arc/issues/1451))

`POST /api/v1/delete` with `"where": "cpu.host = 'web-1'"` on measurement `cpu`
failed with `Referenced table "cpu" not found`, while `"where": "host = 'web-1'"`
worked. The caller sends only the predicate, so there was no alias to give the
measurement either. A qualifier naming the measurement is now dropped before
the clause is validated, recorded or run: `cpu.host`, `"cpu".host` and
`CPU.host` all mean the column `host`. A name used bare (`cpu = 'x'`), one
deeper in a path (`x.cpu.host`), and text inside string literals are left as
written.

The rows read are not renamed after the measurement, on purpose. A
measurement often has a field named like itself, and in a file without that
field the name would then stand for the whole row: `temperature IS NOT NULL`
would match every row of such a file and delete it.

As with a query's table-name qualifier
([#1447](https://github.com/Basekick-Labs/arc/issues/1447)), a struct column
named like its measurement is no longer reached through that name with a
field (`cpu.x` now means the column `x`).

### A column qualified by its table's name binds ([#1447](https://github.com/Basekick-Labs/arc/issues/1447), [#1450](https://github.com/Basekick-Labs/arc/issues/1450))

`SELECT readings.value FROM readings` failed with `Referenced table "readings"
not found`. Arc replaces a table reference with a `read_parquet(...)` read, and
DuckDB names that relation `read_parquet`, so a column qualified by the table's
own name no longer matched it. The query worked only when it gave the table an
alias of its own (`FROM readings r ... r.value`). Postgres and TimescaleDB SQL
qualifies columns this way all the time, for example the LATERAL subquery that
reads the latest value of each channel at a point in time:

```sql
SELECT c.ch, r.time, r.value
FROM (VALUES ('x'), ('y'), ('z')) c(ch)
LEFT JOIN LATERAL (
  SELECT time, value FROM readings
  WHERE readings.channel_id = c.ch AND time <= TIMESTAMP '2026-11-03 16:22:00'
  ORDER BY time DESC LIMIT 1
) r ON true
```

The rewritten read now carries the table's name (`AS "readings"`) whenever the
query qualifies a column with it and the reference has no alias of its own.
This covers `FROM`, every `JOIN` form, comma joins, `database.table` references
(named by the table), and queries with or without the `x-arc-database` header.
It also covers continuous queries, whose source read is named after the source
measurement: a body such as
`SELECT cpu.host, avg(cpu.usage) AS usage FROM prod.cpu WHERE time >= {start_time} AND time < {end_time} GROUP BY 1`
failed at every scheduled run and now writes its rows. A query that never qualifies by
table name is rewritten exactly as before.

A query that also gives the name to another relation, as in
`FROM readings JOIN (SELECT ...) readings USING (channel_id)` or
`FROM (...) readings WHERE EXISTS (SELECT 1 FROM readings WHERE readings.value = 100)`,
keeps the unaliased read: naming the table as well would be a duplicate alias
in the first, and would change which relation `readings.value` reads in the
second. This holds wherever the other relation is in the query, including a
subquery or `LATERAL` join aliased with the table's name
(`LEFT JOIN LATERAL (SELECT ... FROM readings ...) readings ON true`); give one
of the two a different alias to qualify by the table's name there. A column named like the table, such as Telegraf's `cpu` tag on the
`cpu` measurement, is not an alias wherever it appears, so it does not stop
the table being named.

Two things now behave the way Postgres does, where before they relied on the
relation having no name:

- **A struct column named like its table.** In a query that also qualifies by
  the table name, `readings.value` now names the table's column `value`, not a
  field of a struct column called `readings`.
- **`read_parquet.col`.** A query that qualified columns with the internal name
  `read_parquet` keeps working unless it also qualifies by the table name.

A table reference followed by `WITH ORDINALITY` keeps its old form, because
DuckDB accepts that clause only before an alias. A delete's `WHERE` may
qualify columns by the measurement name too; see
[#1451](https://github.com/Basekick-Labs/arc/issues/1451) above.

### A MessagePack value its column cannot hold is a client error, not a 500 ([#1424](https://github.com/Basekick-Labs/arc/issues/1424))

A columnar MessagePack record with a value of the wrong type for its column,
such as `{"m": "cpu", "columns": {"time": [1, 2], "v": [1, "oops"]}}`, was
answered `500 Failed to write records`, while the same conflict in row records
or over line protocol is a `400` partial write. Clients take a 500 for a server
failure: the Python SDK's buffered writer, for one, could not tell whether the
batch was written. The record is now refused for its content, like a column
that changes between string and binary: a request whose only record it is gets
a `400` naming the measurement and column, and in a batch the record is
dropped, the others are written, and the answer is the `400` partial write.
Nothing of the refused record reaches the WAL, so it is not replayed at the
next start. The same applies to a value outside a decimal column's precision,
which over line protocol on a node with `ingest.decimal_columns` also answered
500. An MQTT message is handled the same way: its other records are written
and the refused one is logged as dropped points, and a message refused whole
is logged as refused for its content, at most once per topic every ten
minutes, instead of as an error each time.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1444](https://github.com/Basekick-Labs/arc/pull/1444).

### The tiering file list applies the tier and database filters together ([#1430](https://github.com/Basekick-Labs/arc/issues/1430))

`GET /api/v1/tiering/files?tier=cold&database=X` returned the database's files
in every tier: with `database` set, `tier` was ignored. With both set, the list
is now the database's files in that tier, newest partition first as for
`database` alone, read from SQLite on its `(database, tier)` index. `limit`
applies to that list as before. Either filter alone is unchanged. A `tier`
other than `hot` or `cold`, such as `COLD` or `warm`, used to list the hot
files with a `200`; it is now answered `400 invalid tier`, with or without
`database`.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1446](https://github.com/Basekick-Labs/arc/pull/1446).

### The scheduler control routes say why a scheduler is not running ([#1433](https://github.com/Basekick-Labs/arc/issues/1433))

`POST /api/v1/schedulers/cq/reload` and `POST
/api/v1/schedulers/retention/trigger` answered `503 "... is not running -
enterprise license required"` whenever the scheduler was not running, also on
a licensed node with `continuous_query.enabled` or `retention.enabled` set to
false. They now give the reason `GET /api/v1/schedulers/` gives: without a
valid license, or with a tier that does not include the scheduler, a `403`, as
other license-gated routes answer; with the license in order, a `503` saying
the feature is not enabled or the scheduler failed to start, which the startup
log reports. `GET /api/v1/schedulers/` now names that second cause too.

A retention trigger the scheduler refuses without running anything is no
longer a `500`: on a cluster node that is not the primary writer, or while a
retention cycle is running, it is a `409`. A trigger after the license stopped
including the scheduler, which answered `200 "Retention triggered
successfully"` without running anything, is a `403`. A CQ reload after the
license stopped including the scheduler stopped every continuous query and
answered `200 "CQ scheduler reloaded successfully"`; it is now a `403` and
leaves the running continuous queries as they were.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1445](https://github.com/Basekick-Labs/arc/pull/1445).

### A query reads only what its permission check authorised ([#764](https://github.com/Basekick-Labs/arc/issues/764))

With RBAC on, a query's permission check reads the table names in the SQL it
receives, and the query transform separately turns those names into the
storage paths that are read. Where the two disagreed about a name, the query
read a database and measurement that nobody checked (#986 fixed three such
cases). The transform now records the database and measurement of every
storage path it builds, including on a cached transform, and any the
permission check did not cover is checked before the query runs. A query
refused there answers 403, on `/api/v1/query` (JSON and MessagePack),
`/api/v1/query/arrow`, `/api/v1/query/estimate` and `GET
/api/v1/query/:measurement`. A transform that reads storage without recording
what it reads is refused with a 500 rather than run unchecked.

When the permission check saw everything the transform reads, which is the
case for every query today, the new check makes no RBAC call; recording the
reads adds about 3-5% to the transform.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1261](https://github.com/Basekick-Labs/arc/pull/1261).

### Time-bounded arcx queries avoid listing unrelated partitions ([#661](https://github.com/Basekick-Labs/arc/issues/661))

The arcx router listed every file of the measurement for each routed query. A
date-trunc or flat-scan query whose `WHERE` bounds `time` on both sides now
lists only the hour and day partitions of that range, as the DuckDB path does.
On a local measurement with a year of hourly partitions, a one-hour window
went from 198 ms to 0.1 ms and a seven-day window to 5.4 ms. A query bounded
only by a start (`time > now() - INTERVAL '1 hour'`) still lists the whole
measurement, so rows stamped ahead of the clock are never left out. Other
query shapes, and object storage, keep the full listing.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1293](https://github.com/Basekick-Labs/arc/pull/1293).

### A line-protocol write is no longer refused part-way by the ingest gate ([#1422](https://github.com/Basekick-Labs/arc/issues/1422))

A line-protocol body with several measurements is buffered one measurement at
a time, and the ingest gate (the fence and stream-takeover pauses, #1207) was
checked again for each one. When the gate closed part-way, during a writer
failover or a stream takeover, the write stopped after its first measurements
were already buffered, and a client that resent the request, such as Telegraf
or an InfluxDB client, wrote those rows twice. The gate is now checked once
for the whole request, before anything is written, as it already was for a
MessagePack batch: a request admitted before the gate closes is no longer
refused part-way by it. Other failures can still stop a request part-way, and
are answered with what was written (#1437). The line-protocol, CSV and Parquet
imports still check the gate for each buffer call, so it stops a long import
part-way.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1428](https://github.com/Basekick-Labs/arc/pull/1428).

### A refused write is no longer replayed from the WAL ([#1438](https://github.com/Basekick-Labs/arc/issues/1438))

With the WAL enabled and `cluster.wal_coverage` off (every standalone node, and
clusters that leave it off), a live write appended its WAL entry before the
checks that can still refuse it: schema churn on the measurement, a request
cancelled by its client, a node shutting down, and (for columnar MessagePack)
a column that could not be converted. The client got the refusal, usually a
`503` it resent, but the entry stayed in the WAL. A crash before the next
purge, or for a shutdown refusal the next start, replayed it, so the rows were
stored twice.

- **`wal.ack_mode = "queued"`** (the default until [#1499](https://github.com/Basekick-Labs/arc/issues/1499) made `"auto"`, durable with the WAL on, the default): the WAL entry is now built
  before the shard lock, as before, but only queued for the WAL file after
  those checks pass, under the same lock that puts the batch in the buffer.
  WAL coverage already worked this way. A refused write leaves nothing in the
  WAL. The on-disk format is unchanged.
- **`wal.ack_mode = "durable"`:** a write is acknowledged only once its entry
  is synced, and that wait cannot happen under the shard lock, so the entry is
  still written first. A refusal after it now retracts the entry with a
  checkpoint, the same record a flush writes, so recovery never replays it.
  The same applies when one chunk of a split payload fails after the others
  were written, and when the sync itself fails (the checkpoint is then
  attempted, and usually fails with it). A retraction that cannot be written
  leaves the old behaviour: the write may come back at the next start. Each
  one is counted in `arc_wal_retractions_failed_total` and logged at Error, at
  most once every 10 seconds.
- **The conversion now runs before the WAL append** in every mode, so a value
  it refuses never reaches the WAL.
- **A full WAL queue** that drops a later chunk of a large write at commit
  keeps the chunks already queued tracked, so their flush checkpoints them.
  Before, they were replayed at the next start next to the rows the buffer had
  already stored.

In queued mode this also closes a race with the flush-failure recovery pass.
A write's WAL entry and its batch now enter the WAL queue and the buffer in
one step, so the pass can no longer replay an entry whose rows are still in
the live buffer.

### Filesystem-equivalent spoke IDs cannot share a sync namespace ([#740](https://github.com/Basekick-Labs/arc/issues/740))

Spoke IDs that differ only in letter case or Unicode normalization
(`rocket_01` and `ROCKET_01`, or `café` in NFC and NFD) are one directory on
storage that folds names: a local root on macOS APFS (by default) or Windows.
There either spoke could write into the other's namespace.

- Registering an ID that collides this way with one registered on the same
  hub node is refused with `409`, on every storage backend. The comparison
  folds case the way APFS does, including scripts whose cases plain Unicode
  folding leaves apart (Cherokee).
- An existing pair is handled by what the hub's storage does, which Arc
  probes at startup on the local storage root, for case and for
  normalization separately. A pair that is one directory there is refused for
  sync, every member of it (`401`, logged on the hub), and reported at
  startup. A pair that is two directories there keeps syncing, and the startup
  log names it, since moving that storage to a volume that folds the
  difference would merge them: any pair on Linux filesystems, S3, Azure and
  GCS, and a case-only pair on case-sensitive APFS, which still normalizes.
- A refused member logs a warning on the hub at every sync attempt. Disabling
  or deleting the other member of the pair lets it sync again; re-register the
  edge box you moved under a distinct ID. On a volume that folds names the
  remaining spoke's directory already holds both spokes' files: deleting a
  registration keeps its data.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1401](https://github.com/Basekick-Labs/arc/pull/1401).

### A write that fails part-way says what was written ([#1437](https://github.com/Basekick-Labs/arc/issues/1437))

A line-protocol or MessagePack request with several measurements is buffered
one measurement at a time. If one failed after the earlier ones were buffered,
the answer read as if nothing had been written. For a fence or a WAL entry that
could not be made durable, that was a 503 with `Retry-After` and
`X-Arc-Ingest-Fenced` or `X-Arc-Ingest-Refused`. For schema churn or a shutdown,
it was a 503 "Write rejected". Otherwise it was a 500. A client that resent the
request, as Telegraf and the InfluxDB clients do on any 5xx, wrote the buffered
measurements twice. MQTT subscriptions resent a message refused with
`wal_not_durable` the same way.

- **A failure after part of the request was buffered is now a `500` that says
  what was written.** The body has `rows_written`, `written_measurements` and
  `stopped_at_measurement`, the fields an import that stops part-way already
  answers with. Points left out for good (a field named `time`, a value that
  does not fit its column, a measurement refused for its content) add
  `points_dropped` and `dropped`. The answer has no `Retry-After` and no
  `X-Arc-Ingest-*` header. The part of the request it stopped at, and every part
  after it, was not buffered. With the WAL enabled, the part it stopped at may
  still come back from the WAL at the next start.
- **Why 500 and not a 4xx.** Telegraf and the InfluxDB clients drop a batch
  answered `4xx` for good, which would lose the measurements that were not
  written. They do still resend a `500`, which duplicates the written part.
  Compaction collapses rows identical in every column, so timestamped lines
  converge. Lines without a timestamp get a new server time when resent, so
  their duplicates remain. A client that reads the body can resend only the
  measurements not listed in `written_measurements`.
- **MQTT:** a message stopped part-way is no longer resent whole. It is counted
  in the failed messages and logged with what was written.
- **A measurement refused for its content during the write** (a column changed
  between string and binary since the request was checked, or a column name
  that cannot be stored) is now dropped, and the other measurements are written.
  The answer is the usual `400` `partial write: N points dropped: ... the other
  points were written`. Before, the request stopped there with a `400` that read
  as if nothing had been written.
- **Unchanged:** a failure before anything was buffered keeps its answer, so a
  `503` with `Retry-After` still means nothing was written. A request refused
  up front (a fence, WAL disk pressure, a column type change caught before the
  write) is also unchanged.

The record path that deployments with decimal columns use now writes
measurements in name order, as the default path already did. The
`wal_not_durable` refusal text no longer says earlier parts of the request may
already be stored: a refusal is now only answered when nothing was buffered.

### Failed flushes report rows without a WAL copy ([#1377](https://github.com/Basekick-Labs/arc/issues/1377))

With `wal.enabled = false` (the default) a failed flush loses the rows it
held: nothing else has a copy, and nothing retries it. Arc logged them as
"data preserved in WAL for recovery". It now says what happened:

- A failed flush logs `Flush failed; rows without a WAL copy were lost` with
  `records_without_wal`, and adds them to the new counter
  `arc_buffer_records_dropped_total` (also in `/api/v1/metrics` and the
  metrics time series). With the WAL on, rows whose WAL append succeeded are
  not counted, and the message says they are replayed by the next recovery
  pass (`wal.recovery_interval_seconds`) or on restart; rows whose append
  failed have no copy and are counted. A flush that fails after writing some
  of its hours counts all of its rows without a copy, including those hours.
- Shutdown does the same for rows it cannot write in its time budget: those
  without a WAL copy are logged and counted as lost, not as "remain in the
  WAL", and `arc_wal_records_preserved_total` counts only rows the WAL holds.
- One failed flush logs one Error, not two.

Alert on `increase(arc_buffer_records_dropped_total[5m]) > 0`. To keep rows
across a failed flush, enable the WAL.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1379](https://github.com/Basekick-Labs/arc/pull/1379).

### A deleted database disappears from the database list ([#1425](https://github.com/Basekick-Labs/arc/issues/1425))

`GET /api/v1/databases` and `SHOW DATABASES` built their lists from
directories, so a deleted database whose directories outlived its files stayed
listed, with `measurement_count: 0`, and `GET
/api/v1/databases/:name/measurements` still listed its measurements, while
`GET /api/v1/databases/:name` answered 404. Two stores do that:

- **Local storage.** The delete removed the files but left their
  `measurement/YYYY/MM/DD/HH` directories. It now removes the empty
  directories, deepest first; a directory that still holds a file stays, with
  the directories above it. Deleting a database an earlier version left
  behind still answers 404, and now removes its directories too.
- **Object stores that keep empty prefixes** (SeaweedFS). A prefix outlives
  its last object, and Arc cannot remove it through the storage API.

So all three now answer by the rule `GET /api/v1/databases/:name` uses: a
database is its marker, a file, a tiering metadata row, or a cluster manifest
entry, and directories alone are not one.

What that costs:

- `GET /api/v1/databases` already reads every file, so a database with a file
  or a tiering row costs nothing extra. Each other database is checked, 16 at
  a time. On a store that keeps empty prefixes every database ever deleted
  stays one of those, so the list's cost grows with them.
- `SHOW DATABASES` read only directories, and now looks for each database's
  first file, 16 at a time: usually one listing request on an object store,
  plus one for a database with only its marker.
- The measurements endpoint, for a database without a marker, now also looks
  for its first file.
- `GET /api/v1/databases/:name`, for a database without a marker, now stops at
  its first file instead of listing all of them. That is every database
  created by a write rather than by `POST /api/v1/databases`.

Contributed by [@officialasishkumar](https://github.com/officialasishkumar) in
[#1427](https://github.com/Basekick-Labs/arc/pull/1427).

On local storage the delete also removes the empty directories the database's
own Arc state left beside it: `_schema/<db>/` (its field-schema anchors) and
`_compaction_state/<tier>/<db>/` (its compaction manifests). Only empty ones
go: any manifest left in one (a compaction in flight, or one parked
`.quarantined`) keeps its directory, and the shared tier directories stay.
Repeating the delete of a database removed by an earlier version clears them
too. On a cluster with per-node local storage this covers the node that serves
the delete; empty compaction-state directories on another node, such as a
separate compactor, are not removed (follow-up to
[#1425](https://github.com/Basekick-Labs/arc/issues/1425)).

### Running retention executions remain visible in history ([#1415](https://github.com/Basekick-Labs/arc/issues/1415))

The execution-history endpoint now includes runs whose duration is still
`NULL`. Their existing numeric `execution_duration_ms` field is `0` until the
run completes; completed and failed durations are unchanged. Runs started in
the same second are listed newest first, matching `last_execution_status`, and
a row the endpoint cannot read is logged instead of being dropped silently.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1417](https://github.com/Basekick-Labs/arc/pull/1417).

### Compaction keeps sort-key files ordered by hour, and compacts a partition that lacks a sort-key column ([#1409](https://github.com/Basekick-Labs/arc/pull/1409))

With `ingest.sort_keys` (or `ingest.default_sort_keys`) set, ingest writes each
file within one hour, sorted by the keys and then time. The daily compaction
then sorted the whole day by the keys alone, so each row group of the
long-lived compacted file mixed the hours of the day, and a query over a few
minutes read every row group: on a 54M-row daily file of TSBS IoT data sorted
by `fleet,name`, every row group spanned about 17 hours, nearly all of that
file's time range.

- **Compacted files are now ordered by hour, then the sort keys, then time.**
  Each row group holds contiguous hours, so time-range pruning works on
  compacted files again, while rows stay grouped by the keys within each hour.
  A measurement whose hour fills a row group (122,880 rows) gets row groups of
  about an hour; a smaller one gets fewer, wider row groups, as it did without
  sort keys. Measured by rewriting that 54M-row file each way (DuckDB 1.5.6,
  warm medians):

  | Query | Time only (default) | Keys, before | Hour, then keys |
  |---|---|---|---|
  | One truck, 24 hours | 51 ms | 8 ms | 9 ms |
  | One fleet, 10 minutes | 11 ms | 26 ms | 11 ms |
  | All trucks, 10 minutes | 9 ms | 38 ms | 11 ms |
  | Latest position per truck, one fleet | 75 ms | 42 ms | 32 ms |
  | File size | 984 MB | 802 MB | 807 MB |

  Rewriting that file in the new order took about 10% longer (7.0 s to 7.8 s)
  with about 2% more peak memory. Hourly compaction is unchanged (its rows are all one
  hour), and so is compaction without sort keys.
- **Only newly compacted files get the new order.** A daily file compacted by
  an earlier version keeps its order until it is rewritten; it is still read
  correctly, just with the old pruning.
- **A sort key the partition's files have no column for no longer fails its
  compaction.** The ORDER BY named every configured key, and a key missing
  from all of a batch's files was a bind error, so that partition failed to
  compact on every cycle. The key is now left out of that batch's order, and a
  Warn names it when it is configured for the measurement in
  `ingest.sort_keys`; a key from `ingest.default_sort_keys` that a measurement
  does not have is logged at Debug, since that is normal. Names match
  case-insensitively, as DuckDB binds them.

### A sort key the data has no column for no longer leaves the file unsorted ([#1410](https://github.com/Basekick-Labs/arc/pull/1410))

With `ingest.sort_keys` or `ingest.default_sort_keys` set, a flush whose rows
had no column for one of the keys failed its sort, and the flush wrote the
rows in arrival order: not sorted by the other keys, and not by time either,
which also cost time-range pruning inside the file. Nothing was logged. (The
documentation said such a flush fails; it never did, and no rows were
lost.)

- **The flush now sorts by the keys it has a column for, then time,** and
  leaves the missing key out. A key configured for the measurement in
  `ingest.sort_keys` is logged at Warn, once a minute per measurement, naming
  the key. A key from `ingest.default_sort_keys` is logged at Debug: it applies
  to every measurement without its own entry, and many do not have that
  column. Compaction leaves such a key out of that partition's order as well
  ([#1409](https://github.com/Basekick-Labs/arc/pull/1409)); before, it failed
  the partition's compaction on every cycle.
- **Key names match case-insensitively**, as compaction already binds them,
  so a key configured as `Host` sorts a `host` column at ingest too. Before,
  such a key failed the sort, and the file was written unsorted.
- **Should a sort ever fail, the flush is written sorted by time** and logged
  at Error, once a minute per measurement, rather than unsorted and silent.

### Ingest with sort keys is as fast as without them ([#1411](https://github.com/Basekick-Labs/arc/pull/1411))

A flush sorted by `ingest.sort_keys` (or `ingest.default_sort_keys`) went
through a comparison sort that checked each key column's type on every one of
its comparisons: 2.60 s for a 5M-row flush sorted by two tag columns and time,
against 0.21 s for time alone. On a sustained load that was most of the cost
of sort keys at ingest.

- **The sort is now one linear, stable pass per key**: a radix sort for
  integer and float keys, and for tag (string and binary) keys a counting sort
  on each value's rank among the flush's distinct values. Rows already in
  order are not copied, as with time-only sorting.
- **A key with many distinct values** (more than a quarter of the flush's
  rows, such as a request id) or a decimal key has no such pass; the flush is
  then sorted by one comparison over all its keys, about as fast as before
  (1M rows by an id and time: 371 ms before, 339 ms now).
- **Measured**, sorting a flush by `fleet, name, time` (10,000 names): 50,000
  rows (the default flush size) 13.9 ms to 6.3 ms; 1M rows 445 ms to 77 ms;
  5M rows 2.60 s to 0.51 s. While it runs, the sort needs about 24 bytes of
  scratch memory a row plus 4 for each tag or boolean key, against 8 before
  (about 160 MB instead of 40 MB for 5M rows sorted by two tags and time).
- **End to end**, loading 155.6M TSBS IoT rows with
  `sort_keys = ["readings:fleet,name", "diagnostics:fleet,name"]` and
  `wal.ack_mode = "durable"`: 36.6 s (29.7 to 49.0 across three runs) before,
  25.4 s (25.3 to 25.7) now, against 26.1 s with no sort keys. Rows were
  queryable after 27.9 s, against 44.2 s before.
- **Order changes only where it was undefined.** Rows equal on every key now
  keep their arrival order (the old sort did not promise any). A float sort
  key orders -0 and +0 as equal and NaN after +Infinity; the old sort treated
  NaN as equal to every value, which left the order of rows with NaN keys
  unspecified.

### Azure Blob Storage error messages are shorter

Errors from the Azure Blob Storage backend are shorter. Azure credential
values (connection string, SAS token, account key) and the endpoint are
trimmed of surrounding whitespace, so a value read from a file with a trailing
newline works everywhere it is used; one with another control character is
refused at startup. Errors from the licence server check are shorter in the
same way.

### Parquet import keeps binary columns ([#440](https://github.com/Basekick-Labs/arc/issues/440))

The Parquet import stored every binary column (a BYTE_ARRAY without a string
annotation) as a string. When the bytes were not text, such as a GeoParquet
file's WKB geometries, every query that read the column failed with "Invalid
string encoding". A binary column is now stored as binary when:

- it has Parquet's `GEOMETRY` or `GEOGRAPHY` type, or the file's GeoParquet
  `geo` metadata names it with WKB encoding (another encoding is refused with
  422 before anything is written),
- the file was written by a writer that marks its string columns: Arc itself
  (ingest and compaction), DuckDB, pyarrow, or any file carrying an Arrow
  schema, so Arc's own files keep their types when imported into another Arc,
- the measurement already stores the column as binary, or
- any of its values in the file is not valid UTF-8.

Otherwise (Spark and Hive write string columns without the annotation) a
column of text throughout is a string, as before. The whole file is checked
before anything is written, so an import is never refused part-way for this.
Two column types that were refused as unsupported now import: `LargeBinary`,
and Parquet `UUID`, stored as its canonical text as Arc shows a UUID
everywhere else. A GeoArrow column stored as `large_binary` is still refused.

A column an earlier import already stored as a string holding binary data
cannot be read. Import the binary values into a new column: writing bytes to
the existing string column is refused, and the refusal says so.

### DuckDB no longer caches blocks of deleted files on local storage ([#1372](https://github.com/Basekick-Labs/arc/issues/1372))

DuckDB keeps a file cache of blocks from the Parquet files it reads. It frees
that cache only when it reaches its memory limit, which defaults to 80% of RAM
or of the container. Compaction deletes and replaces files all day, so on a
busy node most of the cache belonged to files that no longer existed, and the
process kept growing.

On a node whose data is all on local disk, Arc now turns that cache off. The
operating system's page cache already holds the same blocks. Measured on the
same workload (ingest, compaction every minute, continuous queries):

- **macOS, 73 minutes:** 1.6 GB physical footprint before, 760 MB after.
- **Linux (EC2 c7i.xlarge), 80 minutes:** 1.6 GB resident before, 785 MB
  after. Neither process gave any of it back over the next 8 idle hours, so
  the old figure is not a peak but where a node stays.

Turning it off did not slow queries:

- **macOS:** compacted and small-file queries within ±2%, warm and hot.
- **Linux (EC2 c7i.xlarge):** compacted-data queries within ±2%, and
  small-file queries 5–7% faster.
- **8 concurrent clients:** the same throughput on both platforms.

A node that may read S3 or Azure, as primary storage or as a cold tier, keeps
the cache: there it is the cache for remote reads.

`database.external_file_cache` (`ARC_DATABASE_EXTERNAL_FILE_CACHE`) controls
it:

- `"auto"`, the default, applies the rule above.
- `"true"` turns the cache on everywhere.
- `"false"` turns it off everywhere, including on a node that reads S3 or
  Azure.

Arc logs the value in effect at startup ("DuckDB file cache"), and
`/api/v1/debug/duckdb-memory` reports it as `external_file_cache`.

**DuckDB's Parquet metadata cache still grows.** It also keeps entries for
deleted files, and DuckDB offers no way to empty it. It is smaller (220–285 MB
after the same runs) and is freed under memory pressure, so it stays
within DuckDB's memory limit. The step Arc ran after every compaction, DELETE
and retention pass to "reset" it freed nothing, and has been removed.

### Rebuilding cold metadata no longer restarts every tier lookup ([#1188](https://github.com/Basekick-Labs/arc/issues/1188))

When a node rebuilds its cold tier metadata — after a restored or lost
`arc.db`, where every object in the cold bucket is new to it — the sync wrote
one row per object, and each write cleared the tier cache for the whole
process. A node serving queries during that first sync re-ran the tier
lookup for every query, on the single shared SQLite connection, for as long
as the walk took.

The sync now writes in transactions of 1,000 rows and clears the cache once
per database/measurement per transaction. Each row keeps the cold object's
own timestamp. If a transaction fails, its rows are retried one at a time and
a row that still fails is logged and skipped, as before.

The cold rows the manifest sweep works from are now read only where the sweep
runs — on the primary, after migration — rather than by every sync on every
node, which also lowers the sync's peak memory on a large bucket.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1197](https://github.com/Basekick-Labs/arc/pull/1197), with review and fixes
from [@jallegri](https://github.com/jallegri) in
[#1309](https://github.com/Basekick-Labs/arc/pull/1309).

### A query on a measurement whose files are all gone no longer fails ([#1364](https://github.com/Basekick-Labs/arc/issues/1364))

With tiered storage, a node whose tier metadata still listed a measurement
with no file left in any tier answered `Binder Error: * expression without
FROM clause!`, even for `SELECT count(*)`. This happened after a DELETE,
retention or tiering removed every file of the measurement. Such a
measurement now reads as empty:
- **With a field schema anchor** (`query.stable_schema`, on by default):
  zero rows with the columns the anchor records, so `count(*)` answers `0`
  and queries naming those columns run.
- **Without an anchor:** the empty result (no rows at all, not even a
  `count(*)` row) that a measurement with no files already gets. As for such
  a measurement, a JOIN or UNION that includes it then answers empty too.

**Limitations:**
- A node with no tier metadata for the measurement, an Arc without tiering,
  or a query made before the anchor is first built answers the empty result
  (no rows) rather than `0`. The answer can therefore differ by node: `0` on
  one, no rows on another.
- The read never falls back to local files the tier metadata leaves out.

### A row DELETE reaches cold storage ([#1350](https://github.com/Basekick-Labs/arc/issues/1350))

A row DELETE (`POST /api/v1/delete`) read and rewrote hot-tier files only.
Rows in files that tiering had moved to cold storage were never matched, and
the response gave no sign: a clause whose rows were all in cold storage
answered 200 with `affected_files: 0`, and queries kept returning those rows.

A DELETE now looks at cold storage too:
- **A cold file whose rows all match is deleted**, the way retention deletes
  cold files, and its rows are counted in `deleted_count`. The response
  lists these files in `cold_deleted_files`.
- **A cold file where only some rows match is left as it is.** The answer is
  207: `cold_files` and `failed_files` name the file, and
  `cold_rows_not_deleted` counts its matching rows. Arc does not rewrite part
  of a cold file: cold storage has nothing that keeps readers from counting
  the kept rows twice while the old file is replaced. Narrow the clause, or
  delete the whole file's rows, or let retention remove it.
- **A dry run reports both kinds.** `deleted_count` includes only the rows
  the real run would delete.
- **Cold rows that would be deleted count against `max_rows_per_delete` and
  the confirmation threshold.**
- If cold storage is configured but could not be examined (its backend is
  unusable on this node, or listing it failed), the response says so in
  `cold_not_examined` and answers 207 instead of 200.
- A cold file whose hot copy still exists (a move that has not finished) is
  never deleted from cold storage. It is reported, and a retry once the move
  completes handles it.
- A cold file that does not have a column the clause names (an older file,
  from before the column existed) is counted together with files that have
  it, as one read across every file would count it. When no file in the
  DELETE's time range has the column, it matches nothing, as for hot files.
- A cold tier row whose object is gone is retired, or pointed back at the
  file's hot copy if one exists. Both storage tiers are checked again at
  that moment, as retention does, so a file a migration has just copied
  keeps its row.

Every cold file in the DELETE's time range (every cold file of the
measurement, when the clause bounds no time range) is read to count its
matching rows, so a DELETE over many files in cold storage costs one read of
each file's footer and the columns the clause uses.

Also fixed: in a database made `hot_only` (or given a longer hot age) after
some of its files reached cold storage, a DELETE could rewrite a file that
still had a cold copy. The next cold sync then recorded the old copy, and
queries returned the deleted rows again and counted the kept ones twice. A
DELETE now checks cold storage for a copy of every daily file, whatever the
database's current policy, and leaves such a file as it is. In a `hot_only`
database the hot-only cleanup then removes the copy; with a longer hot age,
the file stays until it is moved to cold storage again under the new
policy. As a consequence, while cold storage is unreachable, a DELETE that
touches a daily file answers the retryable 503, even for recent data.

### Compaction leaves alone a partition too wide to compact, instead of failing it every cycle ([#1388](https://github.com/Basekick-Labs/arc/pull/1388))

Compaction sorts a batch's rows by time in one DuckDB job, and a sort holds
whole rows. On a table of thousands of columns that does not fit the memory a
job has (`compaction.memory_limit`, by default `database.memory_limit` divided
by `compaction.max_concurrent`): measured on DuckDB 1.5.6, two files of 3,100
columns ran out of memory at 2 GB on one thread, and 905 columns at 2 GB on
any thread count. The batch splitter halved the batch until it could not
split further (3 files at the default 30 per batch), failed again, and the next
cycle started over, downloading the files each time, for good.

- **A partition that fails at the smallest batch is remembered and left
  alone** while its files and the job sizing stay the same. Only memory
  evidence counts, at most once per partition per cycle: DuckDB's
  out-of-memory error marks it after one cycle and holds; a kill, a crash or a
  cycle deadline that cut off an attempt which had nine tenths of the cycle's
  budget marks it after two cycles in a row (a kill can hit another job
  running alongside), and that mark expires after a day, so the partition is
  tried once a day. Any other failure (a SQL error, a missing file, a full
  disk, object-store errors) never counts. A new input file in the partition,
  or a change of `compaction.memory_limit`, `compaction.threads`,
  `compaction.max_concurrent`, `compaction.max_files_per_batch` or
  `compaction.cycle_timeout`, tries it again; a partition that then compacts
  is forgotten.
- **What is remembered survives a restart**: it is kept per node in
  `too_wide.json` under `compaction.temp_directory`. A compactor failover
  starts the new node with its own list: each partition fails there again
  (once or twice, as above) before it is left alone.
- **Seeing and clearing it**: `GET /api/v1/compaction/too-wide` lists the
  partitions on the node that answers (with the settings each failed under
  and the current ones; on a cluster, ask the compactor node),
  `DELETE /api/v1/compaction/too-wide` (optionally `?tier=` and
  `?partition=`) forgets them so the next cycle tries again; both admin-only.
  `/api/v1/compaction/candidates` marks those the next cycle skips `too_wide`, each cycle record and
  `/api/v1/compaction/stats` count `skipped_too_wide`, and the metrics are
  `arc_compaction_partitions_skipped_too_wide_total` and
  `arc_compaction_partitions_too_wide`. A Warn names each partition when it is
  first remembered.
- **What it means for that table:** its files stay as written. Queries read
  them as before. Tiering moves only daily-compacted files to cold storage, so
  the table stays on hot storage until retention removes it (as it did while
  compaction kept failing). The remedy is more memory per job: a higher
  `compaction.memory_limit` or a lower `compaction.max_concurrent` (measured
  on DuckDB 1.5.6: two files of 906 columns and 3.93M rows compacted in 72 s
  with 4 GB on one thread, and ran out of memory at 2 GB).
- **The splitter no longer makes a batch it refuses.** A 3-file batch was split
  into 1 and 2 files, and the 1-file half was refused unattempted, losing the
  real error; a batch under twice the minimum, or one at the splitter's
  deepest level (4 halvings), is now the smallest. A child
  that died of memory (a Go runtime "out of memory", a C++ `std::bad_alloc`)
  is classified as a memory error even when its earlier log lines quote SQL
  error text; before, it read as a permanent SQL error.

### A DELETE rewrites wide files in bounded memory, and keeps Arc's file metadata ([#1380](https://github.com/Basekick-Labs/arc/pull/1380))

A row DELETE rewrites each file it removes rows from by copying the rows it
keeps to a new file. That copy ran on every DuckDB thread, and each thread held
a decoded row group (the one it read and the 122,880-row one it wrote, about
9 bytes per value however sparse the data) plus the Parquet writer's state for
every column, part of it outside `database.memory_limit`. On wide files and many-core
nodes it spilled for a long time and then ran out of memory: a customer
measured 8 minutes per file at 905 columns and 39 minutes per row group at
3,100 columns (46 cores, 4 GB limit).

- **A file too wide for the main DuckDB is rewritten on a private
  single-threaded one.** Before each rewrite Arc reads the file's footer and
  estimates what the copy holds per thread (columns × rows per row group read
  or written, plus the writer's per-column state) times the threads it would
  use. Above half of DuckDB's effective memory limit, the file goes to a
  private DuckDB instance: one thread, half the main memory limit, its own
  spill directory, smaller row groups, sandboxed to the file's own directories,
  with no extensions. The row order is kept. On
  object storage the file is downloaded first. Measured on DuckDB 1.5.6 with a
  4 GB limit on a 14-core machine:

  | File | Before | Now |
  |---|---|---|
  | 3,100 columns, 8 row groups | out of memory | 36 s, 2.0 GB |
  | 905 columns, 16 row groups | 32 s, 5.0 GB | 20 s, 0.9 GB |
  | 10,000 columns, 1 row group | stopped after 15 min | 23 s, 2.6 GB |

  Narrower files are rewritten as before. A long narrow file can cross the
  line too on a many-core node (50 columns and 33 full row groups at 46
  threads and 4 GB), and is then rewritten on one thread. Rewrites run one at
  a time (they already did). Wide output has smaller row groups (32,768 rows at
  905 columns, 8,192 at 3,100, 2,048 at 10,000), so it is larger (1.35× at
  3,100 columns, 7× at 10,000) and its footer is bigger, which costs reads: a
  one-second query on a rewritten 3,100-column file took 240 ms and 330 MB
  instead of 30 ms and 50 MB on the original (16,384-row groups would have
  halved that again, but went past the memory limit). A second DELETE on such
  a file goes to the private instance again. The private
  instance's memory limit is on top of the main one's, not part of it: with the
  default limit (80% of the machine) the two together can exceed the machine
  while queries run. A WHERE that calls a function from an extension (arcx,
  httpfs) fails on a wide file. No setting.
- **A file that cannot be sized is not rewritten.** If its footer cannot be
  read, or the private instance cannot be opened, that file's rewrite fails
  (`failed_files`, 207) and the DELETE can be retried; it does not go to the
  main DuckDB unsized.
- **A rewritten file keeps Arc's metadata.** Every DELETE rewrite dropped the
  file's `arc:tags`, `arc:dedup_exact` and `arc:decimals` entries, so a
  rewritten continuous-query file lost its exact-duplicate marker at
  compaction. They are now carried over, along with the legacy
  `arc:dedup_time`. Other entries (`ARROW:schema`) are not: they describe the
  file as it was first written.

### A DELETE reads only the files its time range can touch ([#1371](https://github.com/Basekick-Labs/arc/pull/1371))

A row DELETE (`POST /api/v1/delete`) listed every file of the measurement and
counted its matches with one read across all of them, whatever its WHERE. Its
memory grew with the table's file count times its width, not with the
window: on a wide table (thousands of columns), a DELETE of one minute could
need hundreds of GiB, and `database.memory_limit` did not bound it.

- **Only the partitions in the time range are read.** When the WHERE bounds
  `time` (`time >= … AND time < …`, `BETWEEN`, or `now()` plus or minus an
  interval), the DELETE reads only the hour and day directories that range
  can touch, plus one hour on each side. This applies to hot files, to the
  files the cluster manifest lists (a DELETE no longer waits for, checks, or
  answers 503 over a file outside its range), and to cold storage. A WHERE
  without such a range (an `OR` at the top level, `time = …`, `time IN (…)`)
  reads every file, as before.
- **Matches are counted 64 files at a time, without keeping the files'
  metadata.** Arc keeps Parquet footers in memory for queries; a DELETE kept
  every footer it read until it finished, and on wide files the footers are
  most of the cost. It now counts with that cache off. Measured on 1,416
  files of 1,000 columns: +2.7 GB before, +0.4 GB now for a DELETE over all
  of them, and +2 MB for one of a minute.
- **A hot file that cannot be counted stops the DELETE** before anything is
  rewritten. Before, the per-file fallback logged "skipping file" and went
  on: the file's matching rows stayed and the DELETE answered success. A file
  replaced since the listing (a compaction) answers 503, retry. A file that
  cannot be read answers 500 and names it, on every DELETE whose range
  includes it, until it is repaired or removed; there is no option to skip
  it. A clause that binds but fails on a file's values (comparing a column
  that is text in some files with a number) also answers 500 now, where it
  used to be skipped. A cold file that cannot be counted is reported as
  before (207, `cold_files`).
- **A file that does not have a column the WHERE names, or has it with
  another type, is counted together with files that do,** as the single
  read across every file counted it. Before, a cold file that could not bind
  the clause on its own counted as no match. For cold files whose rows all
  match, the file is now deleted. A hot file counted this way is still
  rewritten on its own, which fails to bind as it did before this change: it
  is reported in `failed_files` (207), not left out silently. A column that
  no file in the range has still matches nothing there.

`delete.time_pruning` turns the first point off (see the upgrade note above).

### Retiring tier rows no longer grows the tier metadata WAL without bound ([#1373](https://github.com/Basekick-Labs/arc/pull/1373))

Removing tier rows one at a time grew the WAL of the shared SQLite database
(`arc.db-wal`) by tens of KB per row, and nothing shrank it. A startup tier
scan that retired 6.2 million stale hot rows took one customer's WAL to
99.6 GB. Each removal ended in a way that skipped SQLite's automatic WAL
checkpoint. Measured on 200,000 rows: 5.9 GB of WAL before, 3 MB now.

The same removal is used when a file is compacted or deleted on a node with
replication on, and when cold rows are retired by retention or a DELETE. All
of those are fixed.

Also:
- The scan reads hot rows 10,000 at a time instead of all at once. A single
  read held the database's only connection, which auth, audit and file
  registration share, for the whole read. A file flushed during that read
  could fail to record its tier row (`failed to record file: context
  deadline exceeded`). Such a row was, and still is, written by the next
  complete scan.
- After each tier scan, a WAL file larger than 64 MB (one an earlier version
  grew) is truncated back to its minimum size. Before, a WAL that had grown
  stayed that size on disk even after its contents were checkpointed. The
  truncation can make auth, audit and file registration wait for up to a few
  seconds once, on the first scan after the upgrade; a WAL kept small by the
  fix is never truncated.

### A compaction run within a minute of its files' registration no longer leaves them behind, counted twice ([#1358](https://github.com/Basekick-Labs/arc/issues/1358))

On a cluster with a shared object store, a compaction finishes by deleting
its input files. The finisher kept any input this node had confirmed as
registered in the previous minute, and ended the job anyway. Once the job
ended, nothing deleted those inputs or left them out of reads, so every node
counted their rows twice, alongside the compaction's output. This happened
when a compaction ran less than a minute after the inputs were registered.
Examples:
- a manual or backfill compaction right after a flush or an import;
- a scheduled hourly run (by default at :05) catching an hour that had
  ended minutes earlier, while its last flush was still being registered;
- a daily run over old days, which skips the file-age check
  (`daily_skip_file_age_check_days`), right after an import into them;
- a registration backlog drained after a Raft outage.

**The fix:**
- The finisher syncs with the leader before it decides. A confirmation
  recorded before that sync belongs to a registration the compaction has
  since removed, so the input is deleted.
- If an input's registration is still pending on this node, or was
  confirmed after the sync started, the finisher waits and retries rather
  than ending the job with the file left behind. The input is then either
  registered again (kept) or not (deleted). A job that keeps waiting is
  logged at Warn every 5 minutes.
- An input parked as refused is kept, logged at Warn naming the job and
  the file, and the job ends as before.
- The finisher sorts every input into keep, wait or delete before it
  deletes any.
- A node no longer registers a file again if a compaction that has not
  ended took it as an input. This can happen after a registration whose
  reply was lost, or one replayed from the journal. The file's rows are in
  the compaction's output, so it is treated as registered. Before checking
  a replayed or retried file, the node syncs with the leader.

**Limitations:**
- Inputs left behind by an older version, from jobs that have already
  ended, are not removed. They are not in the cluster manifest. The opt-in
  reconciliation sweep (`reconciliation.enabled`, then
  `reconciliation.manifest_only_dry_run=false`) deletes them after its grace
  window.
- The check that keeps a consumed input from being registered again only
  runs while the compaction's job is open. If another node's finisher
  deletes the file and ends the job just after this node has checked it is
  still in storage, the manifest gets an entry for a file that is gone. The
  same happened before this fix. Queries list the bucket, so they are not
  affected; the reconciliation sweep removes such an entry when it is
  enabled.
- While a finished compaction waits for an input's registration to settle
  (at most about a minute for a recent confirmation), the node that made
  that registration counts the input next to the output.

### `time_bucket` and `date_trunc` put rows in the right bucket ([#1352](https://github.com/Basekick-Labs/arc/issues/1352))

Arc runs `time_bucket(INTERVAL …, time)` and `date_trunc('…', time)` as epoch
arithmetic, for speed. That arithmetic had two errors; both affected every
query API answer (JSON, Arrow, msgpack, Grafana), and totals over a window
were never affected:

- **The last half second of every bucket was counted in the next one.** A row
  at `03:29:59.7` was in the `03:30` minute. Any per-second, per-minute or
  per-hour series over sub-second data had rows in the wrong bucket at every
  edge.
- **Some bucket widths did not line up with DuckDB's.** Arc aligned buckets
  on 1970-01-01 (a Thursday); DuckDB aligns them on 2000-01-03 (a Monday),
  which is also where ISO weeks start. Weekly buckets started on Thursdays,
  `date_trunc('week', time)` was not the ISO week, and widths such as
  `2 days`, `5 days` or `7 hours` started at different instants than
  DuckDB's. Widths that divide the gap between the two dates evenly were
  already the same: the common dashboard widths (1, 5, 10, 15 or 30 minutes,
  1, 2, 3, 4, 6, 12 or 24 hours, `1 day`) did not move. Others (`7 minutes`,
  `5 hours`, `2 days`, weeks) now match DuckDB, so those panels show
  different bucket starts after the upgrade.

Instants before the origin (before 1970 or 2000) floor correctly, a
`time_bucket` origin with a fractional second keeps it, and the arithmetic is
in integer microseconds, so it is exact for every timestamp. Continuous
queries were not affected: they never used this rewrite.

### A measurement's first files moved to cold storage are read on every node ([#1349](https://github.com/Basekick-Labs/arc/issues/1349))

A node reads a measurement from cold storage only once it has a cold row for
it. When tiering moved a measurement's first files to cold, other nodes could
miss those rows until their own next cold metadata sync (about 24 hours with
the default schedule), and read the files from neither tier until then:

- **Shared object store:** other nodes were never told about the move.
  Now every node records the cold copy when the migrating node releases the
  hot one. A node that installs a Raft snapshot after missing migrations
  checks the cold tier for each path the snapshot dropped, as nodes with
  local storage already did. This works with `cluster.replication_enabled`
  off, which is how the Helm chart runs shared storage. Before, a node
  learned of manifest deletes only through the peer puller, which runs only
  with replication on.
- **Shared object store, DELETE rewrites:** for the same reason, a node
  with replication off kept reading a file that a row DELETE had rewritten,
  next to its rewrite, until the old object was removed. In that window it
  counted the rows the DELETE kept twice. It now leaves the old file out of
  reads once the rewrite is in the bucket, as nodes with replication on
  already did.
- **Both storage modes:** if the cold-tier check failed, the node waited for
  the next sync. It now checks again after 1, 2 and 4 seconds. A cold tier
  unreachable for longer than that still waits for the sync, or for the
  startup scan at the next restart. A retry skips a path that has since been
  written and registered again.

Before recording a cold row, a node always checks that the cold copy exists.
On a shared store the size comes from the same request, so each node makes
one cold-tier request per moved file it has no row for yet.

**Limitations:**
- On a shared object store, other nodes now count a moved file twice until
  the migrating node deletes its hot object. That node deletes them one at a
  time, up to 200 per manifest release, so this can last a few seconds.
  Before this fix, those nodes did not count the file at all.

Contributed by [@efegokdemir](https://github.com/efegokdemir)
in [#1354](https://github.com/Basekick-Labs/arc/pull/1354).

### Compaction job metrics count on `/metrics` ([#1343](https://github.com/Basekick-Labs/arc/issues/1343))

`arc_compaction_jobs_total`, `arc_compaction_jobs_success_total` and
`arc_compaction_jobs_failed_total` always read `0`, whatever
`/api/v1/compaction/stats` said. They were counted inside the compaction
subprocess, whose own metrics nothing scrapes. The storage reads, writes and
bytes the subprocess did were lost the same way.

They are now counted by the server, from each subprocess's result:

- **Attempts, not partitions.** A batch that fails and is retried at half
  size counts twice, as in `/api/v1/compaction/stats`. A subprocess that
  crashed or was killed without reporting counts as failed. One cut short by
  a shutdown or a cancelled cycle counts as interrupted, in the new
  `arc_compaction_jobs_interrupted_total`. `arc_compaction_jobs_total` is the
  sum of the three.
- **Staged jobs** (clusters with compaction commit) count as succeeded once
  their output is staged, before the cluster commits it.
- **On `/metrics`:** `arc_compaction_files_compacted_total`,
  `arc_compaction_bytes_read_total` and `arc_compaction_bytes_written_total`,
  which existed only in the JSON metrics until now.
- **Storage:** the subprocess's storage reads, writes, bytes, errors and
  quarantined paths are added to `arc_storage_*`.

**Alerts.** An alert on `arc_compaction_jobs_failed_total` (for example
`rate(...[1h]) > 0`) never fired before this fix, and now will. A partition
that cannot be compacted fails more than once per cycle as its batch is
halved. An alert on `increase(arc_compaction_jobs_success_total[24h]) == 0`,
which fired all the time, now means something.

Also, a failed job's `writing_output` record in the compaction completion
directory is removed once its subprocess exits, unless crash recovery still
needs it. These records used to stay until a restart. On a node with staged
compaction on local storage, they also kept the failed job's staged output
from ever being swept. The JSON metrics (`/api/v1/metrics`) also gain
`jobs_interrupted`. A subprocess that is killed reports nothing, so the
storage I/O it did before it died is not counted.

### A manifest walk no longer stalls Raft applies during heavy ingest ([#1341](https://github.com/Basekick-Labs/arc/issues/1341))

**Enterprise clusters with file replication.** The periodic manifest walk
(`cluster.replication_reconciliation_interval_seconds`, default 5 minutes),
the startup catch-up and the paginated `GET /api/v1/cluster/files` read the
manifest in 1,000-entry pages over a sorted list of every file path. Two things
made that expensive under ingest:

- The sorted list was rebuilt whenever any file was registered, updated or
  deleted, so a walk re-sorted it on nearly every page.
- Each rebuild ran under the Raft FSM's write lock: about 260 ms at 1.07M files,
  on each of roughly 1,070 pages.

Every Raft apply waited behind it: file registrations, compaction commits,
deletes, token and RBAC changes. Under sustained heavy ingest the walk held
that lock for minutes at a time. On 26.09.x, registrations failed outright
in those windows, and their files were never replicated. On main the outbox
retried them, but ingest registration and compaction commits still stalled,
and compaction batches logged `raft: manifest apply failed`.

The walk now never takes the write lock:

- It copies the paths under the read lock: about 9 ms at 1.07M files.
- It sorts them with no lock held.
- It does this once per walk, and only when files were added or removed since
  the last sort. A content update of an existing file triggers no re-sort.
- In the measurement, an apply arriving mid-sort waited under a microsecond.

Files registered during a walk are listed by the next walk. Replication picks
them up from the registration as before. Files a Raft snapshot install brings
in are listed by the walk in progress if they sort after its position, and
otherwise by the next walk.

A page of `GET /api/v1/cluster/files?cursor=…` can now be empty while
`next_cursor` is not: its files were deleted after the walk's first page.
Keep paging until `next_cursor` is empty; an empty page does not mean the
end.

The workaround of raising `replication_reconciliation_interval_seconds` during
heavy loads is no longer needed.

### Queries no longer count a compaction's rows twice

Compaction writes its output into the partition and then deletes the files
it merged. A query that ran in between read both, so it counted every row of
that compaction twice. This happened with default settings, including OSS
standalone and every cluster node, on JSON, Arrow and msgpack queries.
- **How long:** normally milliseconds to seconds (longer on S3 with many
  inputs). If deleting an input failed, it lasted until the next compaction
  cycle recovered the job, up to an hour, or until the first cycle after a
  restart.
- **Clusters:** a staged compaction has the same window on every node.
  - With shared storage, it runs from the output's copy to the deletion of
    its inputs; a failing delete or a dead finisher held it open.
  - With local storage, each node read the inputs it still had on disk
    until its own delete of them ran. A delete that failed on a node left
    that node reading both sides until the file was removed by hand.
- **Permanent through continuous queries:** a continuous query that ran
  inside the window wrote the doubled aggregate back to storage.

**The fix:**
- Arc now tracks each compaction whose output and inputs can both be in
  storage. It does so from before the output is written until its inputs are
  known to be gone. After a restart, the jobs are rebuilt from the
  compaction's recovery manifests before any query is served.
- Once a compaction's output is in storage, queries, continuous queries and
  the arcx engine leave its inputs out, because the output holds their rows.
  This holds until the inputs are deleted, including after a failed delete
  or a restart.
- In a cluster, every node does the same for staged compactions, whether
  or not it compacts:
  - With shared storage, a committed job's inputs are left out once its
    output is at its final key, until the job ends. Once the output is
    removed from the cluster manifest, nothing is left out: the job is then
    either undone, with its inputs registered again, or its inputs are
    deleted too.
  - With local storage, a node leaves out an input it still has on disk once
    the job's output is on the same disk.
  - With local storage, files that retention or a whole-file DELETE removed
    are left out as soon as the manifest drops them, rather than when each
    node's delete runs.
  - A path the manifest lists again is always read.
  - If the node's deleted-file counts look like a rolled-back manifest
    (`cluster.stale_local_sweep_max_fraction`), it leaves nothing out.
- Outside those windows nothing changes. While a compaction is open, queries
  on that measurement are not cached, so a read planned before the output
  appeared is never served after it. With `query.enable_s3_cache` on, DuckDB's
  object-store listing cache is dropped when an output is first seen.

**Limitations:**
- A DELETE that rewrites a file, and tiering moves, still have their own
  short windows in a cluster; they are not covered here.
- One short race remains, as before: a query planned just before the output
  is written can still read both sides, if its read lists the files after
  the upload and before the inputs are deleted (normally milliseconds).
  Leaving the output out of such a query would instead lose the job's rows
  when the inputs are deleted first, which is worse.

### A retried DELETE no longer leaves the surviving rows in two files

A row DELETE writes each affected file again without the deleted rows, then
removes the old file. If removing the old file failed, it stayed in storage
next to the new one, and queries counted its rows twice. That much is a
window (see the limitations). What made it permanent:
- **Retrying the DELETE:** the old file still had the matching rows, so the
  retry wrote it again. Two files then held the surviving rows, and every
  later DELETE rewrote both. On a shared object store cluster, the retry also
  registered the second copy in the cluster manifest.
- **Compaction:** standalone compaction, and cluster compaction with
  compaction commit off, merged the old file and its replacement into one
  output with the rows twice.
- **The manifest repair** (shared object store clusters) registered a
  compaction output a DELETE had rewritten whose old copy was still in the
  bucket, so nothing ever removed it.

**The fix:**
- A DELETE retries removing the file it replaced a few times before giving
  up, and so does the registration outbox when it rewrites a pending file.
- Every rewrite of a file is named after it, so the DELETE can find the old
  copy: of the copies of one file, the one with the fewest rows is the
  current one (each rewrite only removes rows).
- **Standalone:**
  - Before removing the replaced file, the DELETE records it in the storage
    backend (`_delete_retired/`). Arc removes recorded files at start and
    every minute, so a failed removal is finished without a retry, after a
    restart too.
  - A later DELETE on the measurement leaves out and removes any older copy
    it finds, recorded or not.
  - A backup leaves out recorded files, so a restore does not bring both
    copies back.
- **Clusters:** the DELETE first catches up with the cluster manifest. It
  then leaves out a file the manifest does not list while another copy of
  the same file is listed or still being registered.
  - With compaction commit on (a cluster turns it on once every node
    supports it), the DELETE also
    removes such a file when it is provably the older copy: the listed copy
    is in storage at its registered size and has fewer rows.
  - The manifest repair no longer registers such a file. When the
    reconciler is enabled and acting (`reconciliation.enabled`, with
    `reconciliation.manifest_only_dry_run = false`), it removes the file as
    an orphan.
- Compaction (outside staged compaction, which compacts only files the
  cluster manifest lists) skips every copy of a file while more than one is
  in storage, and any file the standalone record names.
- A DELETE whose clause no longer matches anything when it rewrites a file
  (a clause on `now()`) leaves the file as it is instead of writing the same
  rows under a new name.

**Limitations:**
- While both copies are in storage, compaction skips that file.
- Pairs of copies left by earlier versions (both rewrites of the bug above)
  hold the same rows: neither can be told apart, so both are kept, read and
  rewritten together. A DELETE on them warns. Remove one by hand.
- Standalone with tiering: if a recorded copy moves to the cold tier before
  it is removed, it stays there.
- On a shared object store cluster, an old copy whose replacement was later
  compacted, tiered or deleted away while the copy was still in the bucket:
  - is never registered again or rewritten (the record keeps it out);
  - is not removed either, since nothing proves it is old any more;
  - is read until removed by hand.
- On a shared object store cluster with tiering, an old copy whose
  replacement is only in the cold tier is left out of every DELETE and not
  removed: a DELETE leaves its matching rows in place (and logs a warning)
  until it is removed by hand or by the reconciler.

### Queries no longer count a DELETE's surviving rows twice while it rewrites a file

A row DELETE writes each affected file again without the deleted rows, then
removes the old file. Between the two, every query read both: the surviving
rows twice, and the deleted rows again. Normally this lasts milliseconds; if
removing the old file failed, it lasted until something removed it. Reads
now leave the old file out once its replacement is in storage.
- **Standalone:** from the moment the replacement is written. After a
  restart, reads start from the DELETE's record of the file.
- **Clusters (both storage patterns):** every node does the same once the
  cluster manifest has dropped the old file and a registered copy of the
  same file is in storage at its registered size.
- With `query.enable_s3_cache` on, DuckDB's object-store listing cache is
  dropped the first time the replacement is seen.

**The DELETE's record of a replaced file, on clusters too:**
- The DELETE writes the record before it changes the cluster manifest. It
  lives in the storage backend (`_delete_retired/`, in the shared bucket on
  an object store).
- Each node settles its own records every minute. On a shared object store
  only the primary writer does, checked every time.
- A record is only acted on when it is safe:
  - The old file is removed once the manifest no longer lists it and a
    listed copy of it is in storage at its registered size (standalone: once
    its replacement is in storage).
  - A rewrite the manifest still does not list about 5 minutes after the
    settling node first saw its record is removed (its change never
    applied or was refused); the original is kept.
  - If the rewrite took and has since moved on (compacted, rewritten
    again) while the old file was still in storage, the record stays, so
    the old file is never scanned or registered again.
- A DELETE that cannot write its record first does not change the manifest
  (it answers with an error). A DELETE retried while an earlier rewrite of
  the same file is still settling answers 503 until it is settled.
- The manifest repair never registers a file a record names, and the
  reconciler does not treat one as an orphan.
- A restore clears the record of every file it restores, and a backup leaves
  a recorded file out only while its replacement is in the same backup.

**Two ways a cluster DELETE could lose rows, fixed:**
- When the manifest change returned an error whose outcome is unknown (a
  forwarded change that timed out, leadership lost after it was proposed),
  the DELETE removed its rewrite. If the change applied anyway, the manifest
  listed a file that no longer existed and the old file was gone too. The
  DELETE now keeps both files and its record (it still answers with an
  error), and the record is settled once the manifest shows the outcome.
- A file the node's own manifest did not list (with compaction commit off)
  was removed with its rewrite left unregistered, even when the node's
  manifest was only behind. The DELETE now catches up with the cluster
  manifest first, and treats the file as unlisted only if it still is.

**Limitations:**
- The window between a replacement's upload and the manifest change that
  registers it remains, on every node of a cluster: for that moment reads
  see both files. Normally milliseconds.
- A node whose manifest lags behind the cluster reads both copies until it
  catches up.
- On a shared object store cluster, a node that restarts reads both copies
  of a file whose old copy is still in the bucket, until the primary
  writer's drainer removes it (about a minute).
- Standalone: if writing the record failed too (likely in the same storage
  outage that stopped the removal), reads after a restart see both copies
  until a DELETE runs on that measurement.
- When compaction takes the replacement, the old file, if still in storage,
  is read again until removed (its rows are not lost).

### A cluster node no longer loses a DELETE rewrite's rows while the rewrite is on its way

On a cluster with local storage, every node removes its copy of a file a
row DELETE rewrote 500 ms after the cluster manifest drops it. The node
receives the rewrite only when its pull completes, which can take seconds,
or until the next reconciliation walk (5 minutes) when the pull queue was
full. Until then that node had neither copy, so its queries were missing
every row the DELETE kept from that file. A continuous query that ran
there in that window stored the shortfall.

**The fix:**
- A node now keeps its copy of the old file until a registered copy of the
  rewrite is on its disk at its registered size. The removal waits on the
  side: the delete workers are never blocked by it.
- Reads on that node keep counting the old file while it waits: the
  node answers as before the DELETE (the deleted rows included) until the
  rewrite arrives, rather than missing the file's rows. Once the rewrite
  lands, the old file is left out of reads and removed. Queries on that
  measurement are not cached while a file waits.
- The old file is removed at once if the manifest no longer lists any
  rewrite of it (its rows went elsewhere or were deleted), and kept if the
  manifest lists the file itself again.
- There is no time limit. A copy waits as long as its rewrite has not
  arrived; removing it earlier would trade correct answers for disk space.
- A node that stops keeps such copies on disk. After the restart, once
  the node has caught up with the cluster manifest, they are found again
  from its record of the files it has seen registered.
- The stale-local sweep and the Iceberg export treat a waiting copy as the
  node's own data.
- New gauge `arc_cluster_local_delete_deferred`: removals waiting for their
  rewrite. It rises while pulls are slow; a value that never falls means the
  rewrites are not arriving.

**Limitations:**
- If the rewrite is compacted before it reaches the node, the old file is
  removed when the rewrite leaves the manifest, even if the node does not
  hold the compaction's output yet.
- A node that catches up through a snapshot install removes the files the
  snapshot dropped without waiting.
- A node without that record does not keep copies waiting, and removes
  the old file as before. That covers `cluster.replication_catchup_enabled
  = false`, no Raft data directory, or a record that cannot be opened.

### Tiering: a migration no longer brings deleted rows back, and reads count its files once

Moving a file to cold storage copies it, marks it cold, and only then
removes the hot copy. Three problems, all fixed:

- **A DELETE during the move brought its rows back for good.** A row DELETE
  that rewrote a file while it was being copied left the cold copy with the
  deleted rows. The cold sync then recorded that copy as a cold file, so the
  deleted rows came back and the file's other rows were counted twice, for
  good.
  - A DELETE and a migration now never work on the same file at once.
  - A DELETE leaves alone a file that is being moved, or one that already
    has a copy in cold storage (a move that was interrupted). Its response
    is 207 and names the files and why. A DELETE does not reach files in
    cold storage ([#1350](https://github.com/Basekick-Labs/arc/issues/1350)),
    so once the move completes those rows stay; delete them before the file
    ages into cold storage.
  - When cold storage cannot be checked for such a copy, the DELETE answers
    503 and the file is left as it is; retry.
  - Only daily compacted files old enough for their database's tiering
    policy are checked, so a DELETE of recent data does not touch cold
    storage.
  - Removing the leftover of an earlier DELETE also waits for a file being
    moved.
- **A file an earlier DELETE replaced could be moved to cold storage.**
  Copied there, the leftover still held the deleted rows. A migration now
  skips such a file:
  - when the DELETE's record names it;
  - in a cluster, when the manifest no longer lists it but lists its
    replacement;
  - standalone, when its hot copy disappears during the copy.
  The check runs again just before the file is marked cold, and a copy
  already made is deleted.
- **Reads counted migrated files twice until the whole batch was done.**
  The hot copies were removed only after every file in the batch (up to
  `migration_batch_size`, default 100) had been copied. Until then a query
  read both copies of every file already moved. Hot copies are now released
  as files are moved, at least once a second, and the query caches are
  dropped around each release. Measured on a standalone node with one copy
  held mid-migration: 7,200 rows instead of 6,000 before, 6,000 after.

**Also:**
- A file is marked cold only if its tier row still says hot. If the row
  changed during the copy (another node moved it), the cold copy is kept,
  since it may be the only one.
- A node that stops being the primary writer during a copy leaves the file
  for the new primary instead of marking it cold.
- A manifest failure stops the whole migration cycle, not just the current
  batch. Files already marked cold keep their hot copies until
  reconciliation removes them.
- A copy that has finished is still marked cold when the cycle is cancelled
  (for example at shutdown).
- On a cluster with local storage, a node that records another node's
  moved file as cold drops its query caches. Before, it could keep serving
  a cached listing without the file for up to 30 seconds.

**Limitations:**
- Between a file being marked cold and its hot copy being released, reads
  on that node still count it twice. This lasts about a second, or longer
  when files are moved faster than the cluster manifest releases them (one
  release of up to 200 files a second).
- On a cluster with local storage, the other nodes count a moved file twice
  until they remove their own copy.
- On a cluster with a shared object store, the other nodes count a moved
  file twice until the migrating node deletes the hot object (see #1349
  above).
- While a migration runs, the migrating node drops its query caches up to
  twice a second.
- A DELETE arriving on a new primary writer while the old one is still
  copying the same file (during a failover) is not covered: the claim
  between DELETE and migration only holds within one process.
- The age check uses the database's current tiering policy. A file moved
  under a shorter `hot_max_age_days`, with the policy raised since, is not
  checked for a cold copy.
- Continuous queries read the hot tier only and were never affected.

### A DELETE no longer drops rows its clause cannot decide

A row DELETE rewrote each file keeping the rows where `NOT (clause)` held. A
row where the clause is NULL (for example `host <> 'a'` on a row with no
`host`) fails that test too, so it was dropped from the rewritten file
without being counted or matched: rows nobody asked to delete were lost. The
rewrite now keeps every row the clause does not match, the same rows the
DELETE counts as matched, on standalone and clusters, and for DELETE records
applied to pending files.

### A cached query no longer misses an hour its relative end has just reached

Arc caches a query's pruned plan for up to 60 seconds, keyed by the current
hour. A query whose end is relative, such as `time < now() - INTERVAL '10
minutes'`, reaches a new hour partway through the current one. A plan cached
just before that kept the new hour out until the entry expired, so the rows
in it were missing. Both pruning caches are now keyed by the hours the
query's range starts and ends in, as well as the current hour.

### File-level time pruning: sub-second flushes, margin minimum, and metrics

These changes apply to `query.file_time_pruning`, which is now on by default
(see the upgrade note above).

- **Sub-second flushes are no longer dropped.** File name times were read to
  the second, so with a margin under one second, a file flushed late in a
  second could be left out of a query that started earlier in the same
  second. Name times are now read to the nanosecond.
- **`query.file_time_pruning_margin_seconds` has a minimum of 1 second.**
  A value of 0 or less is raised to 1, with a warning.
- **A live-hour plan is no longer cached when file pruning declines to act.**
  This covers the cases where every file is in range or too many are kept.
  Before, such a plan stayed cached for 60 seconds and the next queries
  skipped the pruning. Every query whose range starts in the current hour
  now lists that hour's directory, which takes a few milliseconds. When more
  than 1000 files are in range, the query lists them
  and then reads the whole hour anyway. The `cap_fallback` counter shows how
  often that happens.
- **New counters** on `/metrics` (and in the JSON snapshot) show what the
  feature does on a given workload:
  - `arc_query_file_time_pruning_passes_total`
  - `..._expanded_total`
  - `..._files_listed_total`
  - `..._files_dropped_total`
  - `..._nothing_dropped_total`
  - `..._cap_fallback_total`
  - `..._zero_survivors_total`
  - `..._anomalous_kept_total`
- **New counter `arc_ingest_future_stamped_rows_total`,** whether or not the
  feature is on. It counts rows stamped more than the margin after their
  flush and still in the flush's hour. These are the only rows file pruning
  can hide, until their hour closes. A warning names the measurement, at
  most once a minute per measurement.
- **The "more than 1 hour in future" ingest warning now checks a batch's
  latest timestamp, not its earliest.** It is also logged even when the same
  batch holds data more than 7 days old, and at most once a minute per
  measurement.

### Time-filtered queries no longer return too few rows, and Grafana panels are pruned again

Arc reads the time range from a query's WHERE clause and reads only the hour
partitions inside it. The old reader took the first `time >= …` it found
anywhere in the first WHERE and applied it to every table. With default
settings, these queries returned too few rows, with `success: true`:

- a bound joined with **OR** or under **NOT**: `time >= X OR host = 'a'` lost
  the older `host = 'a'` rows, and `NOT (time >= X)` returned nothing;
- a column whose name merely ends in `time` (`downtime`, `end_time`), or a
  column named `timestamp`, read as the time column;
- **arithmetic after the bound**: `time >= '…'::timestamp - INTERVAL '3
  hours'` and `now() - INTERVAL '1 day' - INTERVAL '6 hours'` read only the
  first part;
- an **inclusive end on an hour boundary**: `time <= '10:00:00'` and
  `BETWEEN … AND '10:00:00'` left out the 10:00 hour;
- an **end-only** range (`time < '2024-01-01'`) left out everything before
  2020;
- a **commented-out** bound (`-- AND time >= …`), or one in **QUALIFY**;
- **JOINs** (including `FROM a, b`), **UNION**, **CTEs** and **subqueries**:
  one table's bound was applied to every table, or a subquery's bound to the
  outer query;
- `now() - INTERVAL '1 month'` on the 29th to the 31st started up to three
  days later than DuckDB's own result (Mar 31 minus a month is Feb 28, not
  Mar 3);
- a literal with an explicit UTC offset under a `::timestamp` cast, which
  DuckDB reads without the offset.

A range is now read only from the query's single WHERE clause, and only from
its top-level `AND` terms that are exactly a comparison of `time` with a
literal, with `now()` / `CURRENT_TIMESTAMP` plus or minus one interval, with
an epoch call of one number (since
[#1491](https://github.com/Basekick-Labs/arc/issues/1491)), or a `BETWEEN`. Other terms are ignored, which only widens the range. A top-level
`OR`, a join, a UNION, a CTE or a subquery means no range: the query reads
the whole measurement, correctly but without pruning.

An aggregate's `FILTER (WHERE …)` clause, such as `count(*) FILTER (WHERE
status = 500)`, is not a second WHERE. The query's own WHERE still prunes
(#1360). A time bound inside a FILTER never narrows what the query reads. A
subquery, or a FROM or JOIN, inside a FILTER still means no range.

Accepted spellings:
- the column as `time`, `"time"`, `alias.time` or Grafana's `(time)`;
- the literal as `'…'`, `'…'::timestamp` or `TIMESTAMP '…'`;
- (since [#1491](https://github.com/Basekick-Labs/arc/issues/1491)) `epoch_ms(n)`,
  `make_timestamp(n)` or `to_timestamp(x)` of one numeric literal;
- the reversed form `'…' <= time`;
- one trailing `;`.

A literal with an explicit offset (`+02:00`) gives no range.

An end-only range (`time < X`) no longer prunes. It reads the whole
measurement, which is correct where the old 2020 assumption was not.

**Grafana panels are pruned again.** Since grafana-arc-datasource 1.4.0,
`$__timeFilter` writes `(time) >= '…' AND (time) < '…'`. Arc did not recognise
that form, so every Grafana panel read its whole measurement. It is now
recognised for partition pruning, the empty-range answer and the open-ended
shortcut.

### A query no longer answers empty when an hour it reads is removed while it runs

Arc narrows a time-filtered query to the hour partitions that exist and hands
DuckDB that list. If one of those hours was removed before DuckDB read it, the
query answered `success: true` with **no rows at all**. Every row was lost,
not just the removed hour's. This happened on JSON, Arrow and msgpack queries
with default settings. The usual causes are retention, compaction or a DELETE
removing the hour while the query was in flight, or a cached plan (kept up to
60 seconds) that still named it.

Such a query now answers **503 with `Retry-After: 1`** and the message "data
read by this query was removed while it ran". The query caches are cleared,
so a retry re-plans without the removed hour and returns the remaining rows.
A measurement with no data at all still answers an empty result, as before.

**S3 or Azure primary storage, or a cold tier:** when the object store cannot
list a partition folder, Arc keeps that folder in the plan without checking
it. If a kept folder turns out to hold no files, the query now gets the 503
instead of an empty, incomplete answer, for as long as the listing keeps
failing.

**A file removed while DuckDB is reading the hour gets the same answer
(#1355).** Compaction removes an hour's input files in batches as it merges
them. A query can already have listed the hour's files, and then go to open
one that is no longer there. That query failed with HTTP 500 (`Cannot open
file … No such file or directory`, or the S3 404 / Azure `BlobNotFound`
equivalent). It now gets the same retryable 503, on every query route.

- On S3 or Azure, Arc also clears its object-store cache (at most once a
  second), so the retry lists the hour again. Before, with
  `query.enable_s3_cache`, a cached listing kept naming the removed object
  until its entry expired, and every retry failed the same way. While
  compaction keeps removing files that queries are reading, this clear can
  repeat once a second.
- A file that is missing for good also answers the 503, every time. Examples
  are a broken symlink under the storage root, or a cold-tier object that a
  stale tiering record still lists. Arc logs a warning that names the path.
- Only DuckDB's own error counts. Text that a query puts in a string, and
  DuckDB quotes back in another error, gets neither this answer nor #1333's
  empty result.

### Iceberg export on a local-storage cluster no longer publishes a file next to the one that replaced it

On a cluster with local storage, a node removes its copy of a deleted file
some time after the cluster manifest drops it. These deletes all leave a
window where both the deleted file and the file holding its rows are on disk:

- A staged compaction moves its output into place before the inputs are
  unlinked.
- A partial DELETE's new file can arrive before the old one is unlinked.
- An unlink can fail, or be lost to a crash, and stay until the stale-local
  sweep removes the copy.

An Iceberg export pass inside that window published both files, so external
readers saw those rows twice. The existing check for in-flight compactions
never saw staged compactions, which are on by default on clusters. The window
normally lasts under a second to about 5 seconds. It lasts as long as an
unlink keeps failing, and the duplicated snapshot stays in the table's history
until it expires.

The export now leaves out any file the node saw registered that the manifest
no longer lists. It uses the journal the stale-local sweep keeps, so it also
covers copies left by a crash. It follows the sweep's own rules:
- Only the exact file seen registered is left out. A different file written
  at the same path since, and new flushes not yet registered, still export.
- When the manifest has dropped more of the node's files than
  `cluster.stale_local_sweep_max_fraction` allows (a manifest rolled back by a
  recovery looks like this), the export keeps them all and logs an error.

In return, a file the cluster deleted can disappear from the table slightly
before the file holding its rows appears. On the compacting node that gap is
milliseconds. On a node that has to pull a DELETE's new file, it lasts until
the pull, and readers see those rows missing for one pass instead of
duplicated.

The journal runs when `cluster.replication_enabled`,
`cluster.replication_catchup_enabled` (on by default) and
`cluster.raft_data_dir` are set. A local-storage cluster exporting to Iceberg
without them logs a warning at startup.

### A cluster on `storage.backend = "azblob"` reconciles its container as shared storage

`azblob` is an accepted spelling of the Azure backend. The orphan reconciler
decided whether storage was shared from the configured name. Its list knew
`azure` but not `azblob`, so a cluster configured with `azblob` reconciled as if
every node had its own disk:

- Every node ran the storage scan and the manifest sweep against the one
  container, instead of only the active compactor.
- The check that keeps files other writers have flushed but not yet registered
  ([#1214](https://github.com/Basekick-Labs/arc/issues/1214)) was off. Only
  the grace window protected those files.
- A node with file replication on and catch-up off logged a warning about
  restoring an empty data disk, which does not apply to shared storage.

With `reconciliation.manifest_only_dry_run = false`, several nodes could delete
from the container without that check.

The reconciler now decides from the backend Arc actually built, using the same
rule as the pending-registration publisher. An `azblob` cluster is reconciled exactly
like an `azure` one. Runs, `GET /api/v1/reconciliation/status` and audit events
now report `backend_kind: "shared"` instead of `"local"`. During a rolling
upgrade, nodes still on the old version keep sweeping as before. Keep
`reconciliation.manifest_only_dry_run = true` until every node is upgraded.

On shared storage, reconciliation runs only on the node that holds the
compactor lease. Compactor failover (`cluster.failover_enabled = true`, a Raft
data directory and the `writer_failover` licence feature) is what assigns
that lease and moves it off a failed node. Without it, no node reconciles on
`s3`, `minio`, `azure` or `azblob`, unless a lease is still held from an
earlier failover-enabled run. Until now that showed only in debug logs; a node
without compactor failover now logs a warning at startup. An `azblob` cluster
without failover moves from "every node reconciles" to "no node reconciles".
Enable compactor failover to keep reconciliation running.

### Setting a database to `hot_only` reports existing cold files ([#1304](https://github.com/Basekick-Labs/arc/issues/1304))

The tiering-policy response now reports how many non-quarantined cold files
this node's metadata has recorded for the database. It also clarifies that
`hot_only` affects future tiering and does not restore files already in cold
storage.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1311](https://github.com/Basekick-Labs/arc/pull/1311).

### A tiering policy set on any node applies, and survives a failover ([#1324](https://github.com/Basekick-Labs/arc/issues/1324))

In a cluster, each node kept its own tiering policies, while only the primary
writer migrates files to cold storage. So:

- A policy set on a reader, or on any node other than the primary, was stored
  and reported with `200` but never applied.
- After a failover, the new primary ignored every policy that had been set on
  the old one. Databases pinned `hot_only` started moving to cold storage.

Tiering policies now live in the cluster state. A policy set on any node
applies cluster-wide, every node reports the same policies, and a new primary
applies them. A standalone node is unchanged. The upgrade note above explains
how a cluster takes over the policies each node already holds.

Status codes for `PUT` and `DELETE /api/v1/tiering/policies/{database}`:

| Status | When |
|---|---|
| `503` | The cluster cannot take the write right now (no leader, or the policies are being taken over). Retry. |
| `400` | The policy is invalid, for example a database name longer than 256 bytes. |
| `404` | `DELETE` of a database with no policy. |
| `500` | `DELETE` hit a database error. It used to answer `404` for any error. |

A token or RBAC change made on a follower is forwarded to the leader. It now
returns only once the follower that forwarded it has applied the change, up to
the write's timeout, so reading it back on that node sees it. Before, the read
could briefly miss it.

### A retention policy created on any node runs, and survives a failover ([#1324](https://github.com/Basekick-Labs/arc/issues/1324))

In a cluster, each node kept its own retention policies, while only the
primary writer runs retention. So:

- A policy created on a reader, or on any node other than the primary, was
  stored and answered `201`, but never ran.
- After a failover, the new primary ran only its own policies. A policy that
  had been deleting data stopped, and one that had never run could start.

Retention policies now live in the cluster state. A policy written on any node
applies cluster-wide, every node reports the same policies, and every run,
scheduled or manual, reads the policy as the cluster holds it when the run
starts. A new primary runs the same policies. A standalone node is unchanged.
The upgrade note above explains how a cluster takes over the policies each
node already holds.

Status codes for `POST`, `PUT` and `DELETE /api/v1/retention`:

| Status | When |
|---|---|
| `202` | `POST` only: the cluster took the policy, but this node has not applied it yet. List the policies to see it. |
| `400` | The policy is invalid (including a name, database or measurement longer than 256 bytes), its name is taken, or `retention_days` is not greater than `buffer_days`. |
| `404` | No such policy. |
| `503` | The cluster cannot take the write right now (no leader, or the policies are being taken over). Retry. |

### A continuous query created on any node runs, and continues across a failover ([#1324](https://github.com/Basekick-Labs/arc/issues/1324))

In a cluster, each node kept its own continuous queries and their
`last_processed_time`, while only the primary writer runs them. So:

- A query created on any node other than the primary answered `201` but never
  ran. An update made on another node never reached the primary's schedule.
- After a failover the new primary ran only its own queries, from its own
  `last_processed_time`. With none, its first run covered only the last hour,
  and the data since the old primary's last run was never aggregated. With an
  old one, it aggregated those windows again.

Continuous queries and their `last_processed_time` now live in the cluster
state. Every node's scheduler follows them, and the primary flushes each run's
output to storage and then records the window's end in the cluster before that
query's next run, so a new primary continues where the old one stopped, and a
recorded window's output is never only in the old primary's memory. (The
flush before the record comes from @jallegri's #1328.) If the primary dies, or is
demoted, after writing a run's output and before the cluster recorded its end,
the new primary writes that one window again; a primary that cannot reach the cluster skips that query's runs
until it can, so it never grows past one window. A standalone node is
unchanged.

A manual `POST /api/v1/continuous_queries/{id}/execute` with an `end_time` more
than a minute in the future now answers `400`. The run's end becomes
`last_processed_time`, and a future one made every later run fail until the
clock caught up.

### A `hot_only` database no longer keeps a cold copy left by a failed migration ([#1324](https://github.com/Basekick-Labs/arc/issues/1324))

A hot-to-cold migration that copied a file to the cold tier, then failed
before marking it cold, left two copies of the file. After the database was
pinned `hot_only`, nothing removed the cold one, which kept costing storage.

On the primary writer, the cold-tier sync now deletes such a cold copy, but
only when every one of these holds:

- the file's metadata still says hot;
- the hot copy exists;
- the cluster manifest lists the file at the same size;
- the cold object is the same size.

A copy already recorded as cold is left alone, and so is any file whose hot
copy cannot be confirmed. A failed delete is retried on the next cycle, and the
sync carries on with the other files. Query caches are refreshed after a
removal.

On a reader whose replica of the hot file has not arrived yet, the cold copy
stays recorded so queries still find the rows. Once the hot copy arrives and
the cold object is gone, the reader marks the file hot again.

Contributed by [@jallegri](https://github.com/jallegri) in [#1329](https://github.com/Basekick-Labs/arc/pull/1329).

### Aggregates over several hours no longer return one result per hour on `/api/v1/query`

Since 26.02.1, a JSON query with the database in the `x-arc-database`
header, one table, no quoted strings or comments, and a time range covering
three or more hour partitions (a typical dashboard query, such as
`SELECT COUNT(*) FROM cpu WHERE time > NOW() - INTERVAL 6 hour`) was fanned
out as one DuckDB query per hour, and the per-hour results were
concatenated. The response was a `200` with wrong data:

- `COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX` and `GROUP BY` returned one row
  per hour instead of one result;
- `ORDER BY ... LIMIT n` returned up to `n` rows per hour, unordered across
  hours; `DISTINCT` and window functions were also applied per hour.

The same shape also failed in two other ways:

- each hour held a DuckDB connection until the response was sent, so a
  query over more hours than free connections (`database.max_connections`,
  at least 4) never answered;
- on `/api/v1/query/msgpack` it failed with `500` "Parser Error: syntax
  error at or near "{"".

The fan-out is removed: the hours a query selects are read in one
`read_parquet` list, which DuckDB scans in parallel itself (measured over
12 hour partitions: the same time for a full scan, faster for a filtered
one). `/api/v1/query/arrow` was not affected. The query-management API still
reports `is_parallel` and `partition_count`; they are now always `false` and
omitted.

### A new hour is queryable at its first flush; DELETE, retention and database delete no longer break query results

The query path caches which hour partitions exist, for up to a minute and
longer when its caches chain:

- **A new hour was missing.** A query whose range reached the current hour
  (`time > NOW() - INTERVAL 1 hour`, a range without an end, or an end at or
  after the current hour) run before that hour's first flush left the hour
  out, and every repeat of that query for the next minute or more answered
  without it, even after the hour had data. Dashboards that refresh every
  few seconds lost the newest hour after each hour boundary. Tiered
  deployments (a cold tier configured) were affected the same way.
- **DELETE and retention emptied query results.** After a DELETE removed
  every row of an hour, or retention removed an hour's files, a repeat of a
  query whose range covered that hour answered `200` with no rows at all,
  including the rows of the hours still there, until the cache expired.
- **A dropped and re-created database answered empty.** After
  `DELETE /api/v1/databases/{name}`, a repeat of a query over that database
  failed once ("No files found") and then answered `200` with no rows until
  the cache expired, even once new data was written.

Compaction already cleared these caches. DELETE (including one that stops
part-way), retention (including a run that aborts part-way; not a dry run)
and database delete now clear them the same way, on every node of a cluster
that has `cluster.shared_secret` set.

The current hour's absence is no longer cached, nor the previous hour's for
the first 5 minutes after the boundary (rows stamped just before the hour can
flush just after it). A cached list made before an hour started is never
used after it. Once the current hour has files, queries are cached as
before; until then, a repeated query checks again which hours exist: one
directory read locally, one directory listing on S3 or Azure. Hours that
have not started yet are cached as absent, as before: rows stamped in the
future show up within about two and a half minutes of their flush.

### A range without an end returns rows stamped more than a day ahead, and an empty one no longer reads the whole measurement

A time range with a start and no end (`time > NOW() - INTERVAL '1 hour'`,
`time >= '2026-10-19T00:00:00Z'`) asks for every row from its start on. The
query path read the hour partitions only up to 24 hours from now:

- **Rows stamped later were left out.** Forecasts, schedules and devices
  whose clocks run ahead write rows more than a day in the future; a query
  without an end answered `200` without them. A range with an explicit end
  was not affected.
- **An empty window read every file.** When no partition existed from the
  start on (a source that stopped sending, a dashboard watching the last
  hour of it), the query read the whole measurement to answer zero rows, on
  every refresh.

A range without an end now lists the measurement's directories, newest
first, and also reads every partition that exists past 24 hours ahead, one
path per hour, however far ahead; like other partition listings, one
written after a query is seen by its repeats within about a minute. An
empty window reads only the newest partition (it answers zero rows) instead
of the whole measurement when the query reads one measurement with only
`AND`-ed conditions (no JOIN, subquery, `OR` or `NOT`) and that
measurement's schema anchor is complete (`query.stable_schema`, on by
default; the anchor still supplies every column); otherwise it reads the
whole measurement as before. More than
10,000 partitions past 24 hours ahead read the whole measurement rather
than leave rows out. Locally this costs a few directory reads per uncached
query; on S3 and Azure, a few directory listings, cached like the existing
partition listings.

### A forwarded request no longer carries the caller's token to the peer ([#1305](https://github.com/Basekick-Labs/arc/issues/1305))

In a cluster, a node that cannot serve a request itself forwards it to a
peer: a write or import sent to a reader, compactor or standby writer goes
to the writer, and a query sent to a compactor goes to a reader. The forward
used to copy the caller's credential (`Authorization`, `x-api-key`, `Cookie`
and the `?p=` parameter) to the peer, in clear text unless the cluster uses
TLS. The forwarding node has already verified that token: it now removes the
credential and sends the token's identity instead, signed with
`cluster.shared_secret`. The peer accepts that identity only on the
forwarded endpoints and only from a node of its own cluster, and checks the
token's permissions and RBAC grants as before.

Upgrade note: a node forwards this way only to peers running this release;
older peers keep receiving the credential, so a rolling upgrade keeps
working, and the credential stops crossing the hop once every node is
upgraded. A cluster without Raft (where each node keeps its own tokens)
forwards the credential as before. A node with authentication disabled
cannot forward to a peer that has it enabled: the peer answers `401`. The
signed identity is valid for 30 seconds, so the clocks of a cluster's nodes
must agree within that for forwarded requests to be accepted (joins already
require 5 minutes).

### In shared storage, a DELETE no longer leaves rows another writer has not flushed ([#1265](https://github.com/Basekick-Labs/arc/issues/1265))

In shared storage (`cluster.shared_storage_mode`) every writer takes writes,
and a DELETE runs on the primary only. It flushed the primary's own buffer
before it scanned, but not the others'. Rows another writer had acknowledged
and still buffered reached storage after the scan and stayed, and the DELETE
answered success. A database deletion did the same: the other writers' rows
recreated the database after it answered success.

Before it scans, the primary now asks every other node of the cluster
(readers and compactors too: they can buffer rows from MQTT or an import) to
flush its buffered rows of the DELETE's scope, and waits for the files they
flushed to be in the cluster manifest. A node that does not answer, or whose
files do not register within 30 seconds, is named in a new `unflushed_nodes`
field, and the DELETE answers 503 with `Retry-After`. `allow_unreachable=true`
runs it anyway and leaves the rows those nodes hold, as it already did for
manifest files a node does not have. The database deletion takes the same
parameter as `?allow_unreachable=true`.

On a three-writer test cluster on SeaweedFS, a DELETE sent to the primary
while another writer still buffered 500 matching rows left all 500 before
the fix, and none after it. A database deletion left 200 rows that recreated
it, and none after. Under steady ingest into all three writers, 12 of 12
DELETEs answered 200, in 0.25 to 0.8 seconds.

Also fixed on a shared object store: a DELETE could remove or rewrite another
writer's file in the seconds before that writer registered it, and the
writer then registered a file that no longer existed. A DELETE now leaves an
unregistered file under two minutes old as it is and answers 503; a retry
finds it registered. A database deletion checks for such files before it
changes anything, and leaves a file that is flushed while it runs.

Without a WAL, one failed flush no longer refuses every later DELETE on the
node until a restart. With the WAL on, a DELETE is still refused while a
failed flush's rows wait for WAL recovery, since recovery would bring them
back after it.

**Upgrade note.** A node before this release does not answer the flush
request, so DELETEs in a shared-storage cluster answer 503 until every node
runs this release, unless `allow_unreachable` is set. A node that crashed and
is still listed in the cluster is asked too, so it blocks DELETEs until it
returns or is removed. Set `cluster.node_id` so a restarted node does not come
back under a new id and leave its old entry behind.

### Flushed WAL files are deleted within seconds, not every five minutes ([#676](https://github.com/Basekick-Labs/arc/issues/676))

A WAL file whose data had reached storage was deleted only on the periodic
WAL pass, every `wal.recovery_interval_seconds` (default 300 s). Between
passes the WAL kept everything already flushed: at 1M rows/s that is about
7 GB, enough to fill a writer's volume on its own. Arc now checks as soon as
a WAL file rotates or a flush completes, at most once per second, so the WAL
holds roughly ingest rate × buffer age plus the file being written. On a test
node ingesting 1M rows/s for two minutes, the WAL peaked at 7.3 GB before and
81 MiB after, with every row in storage either way. Files whose data has not
reached storage are kept as before; the periodic pass still runs.

Two failures on a full WAL volume are fixed:

- **A WAL that could never be purged again.** An entry that could not be
  written, and whose buffer then failed to flush, kept the WAL from deleting
  any later file until a restart, so the volume stayed full even after
  storage came back. The entry is now released when it is lost.
- **A failed write that could hide the entries after it.** A write that
  failed part-way left a partial entry at the end of the file, and the next
  entry written after it could not be read back on replay. Arc now cuts the
  partial entry off before writing again, and on a full disk it no longer
  creates and removes a new WAL file for every failed write.

New metrics: `arc_wal_disk_full_writes_total` (entries lost because the disk
was full) and `arc_wal_recovered_writes_total` (writes that failed and then
succeeded on a new file). `arc_wal_failed_writes_total` now counts entries
that could not be written; a write failure that a retry recovers is no longer
counted there.

Ideas from [@jallegri](https://github.com/jallegri)'s
[#1115](https://github.com/Basekick-Labs/arc/pull/1115) shaped this change and
the next one.

### A writer whose WAL volume runs low refuses writes instead of losing them ([#676](https://github.com/Basekick-Labs/arc/issues/676))

With the WAL on, a write was acknowledged before its WAL entry reached the
disk. On a full volume the entry could not be written: the client had its
204 and the data was lost if the buffer holding it did not flush. A volume
that filled completely also kept the node from starting again, since the WAL
could not write its first file header.

Arc now checks the free space on the WAL volume every second, and also at once
after a rotation or a failed write. Between checks it counts every byte queued
for the WAL, so a burst cannot run past the limit. Below
`wal.disk_min_free_mb` of free space, live writes are refused with
**503 Service Unavailable**, `Retry-After: 5` and the header
`X-Arc-Ingest-Refused: wal_disk`. Nothing from a refused request is written,
so retrying it is safe. Each request reserves its size (its body, decompressed)
when it is admitted, so a large request, or several at once, is refused before
it can run the volume past that level. Writes are accepted again once free
space is back above that level plus two WAL files (`wal.max_size_mb`), and no
sooner than 5 s after they were first refused.

- Every write path is covered: MessagePack, line protocol, TLE, the LP, TLE,
  CSV and Parquet imports, continuous queries and MQTT. A scheduled continuous
  query run is skipped, with a warning at most once a minute and no failed
  execution recorded, and the next run covers its window; a manual execute
  answers the same 503. MQTT subscriptions disconnect as soon as writes are
  refused and reconnect once they have been accepted again, no sooner than 5 s
  after disconnecting. Subscriptions with
  `clean_session=false` and QoS 1 or 2 find their messages queued at the
  broker. Subscriptions with `clean_session=true` lose what is published in
  the meantime.
- A request is refused whole or accepted whole: a line protocol write that
  spans several measurements is never half written.
- WAL replay, recovery and replication are never refused.
- `/health` and `/ready` are unaffected.

A 64 MiB reserve file, `.arc-wal-reserve`, is kept in the WAL directory. A
node that starts with less than 32 MiB free on the WAL volume deletes it
before anything else writes there, logs an error and starts. The reserve is
created again once writes are accepted and there is room for it above the
level at which they resume. With the WAL or `disk_pressure_enabled` turned
off, a reserve left by an earlier start is removed.

New settings (all `[wal]`, with `ARC_WAL_*` environment variables):

| Setting | Default | Meaning |
|---|---|---|
| `disk_pressure_enabled` | `true` | Turns the check and the reserve file on or off. |
| `disk_min_free_mb` | `0` | Free space below which writes are refused. `0` = max(512, 2 × `max_size_mb`). It is capped at a fifth of the volume, with a warning, so a setting too large for a small volume does not refuse every write. |
| `max_directory_mb` | `0` | Cap on the WAL directory's size (`0` = none). Use it where free space does not show the real limit: a Kubernetes `emptyDir` reports the node's disk, not its `sizeLimit`. A cap below four WAL files (4 × `max_size_mb`) is raised to that at start, with a warning, so the check can always clear. Quarantined `*.failed` files do not count against it. |

The "WAL enabled" startup line logs the effective reserve, the level at which
writes resume and the cap. The Helm charts take
`arc.wal.diskPressureEnabled`, `diskMinFreeMB` and `maxDirectoryMB`
(`writer.wal.*` in arc-enterprise). With `persistence.enabled: false`, the OSS
chart caps the WAL at 80% of the new `persistence.emptyDirSizeLimitMB`, or at
1024 MB when no size limit is set.

New metrics: `arc_wal_disk_pressure` (1 while writes are refused),
`arc_wal_disk_free_bytes`, `arc_wal_disk_pressure_rejections_total` and
`arc_wal_disk_stat_errors_total`. When the free space cannot be read, the
check lets writes through and this counter goes up. Refusals are logged once
per 30 s, not once per request.

**Windows:** a standby writer's store now reads its volume's size and free
space on Windows too. Its automatic cap (a quarter of the volume) and its
free-space reserve were not enforced there before (the cap fell back to
1 GiB); they now are, as on Linux and macOS.

### Two flushes in one clock tick no longer overwrite each other's file ([#1282](https://github.com/Basekick-Labs/arc/issues/1282))

A flushed Parquet file was named from the wall clock alone. Flush workers run
concurrently, and two flushes of the same measurement and hour that read the
same clock value got the same name: the second upload replaced the first. Both
flushes reported success and the WAL checkpointed both, so the first file's
rows were lost after they had been acknowledged. On a macOS rig (the clock
resolves microseconds) at 1M rows/s for 120 s, 29 of 2000 files collided and
1.74M of 120M rows were lost. Linux clocks resolve nanoseconds, so collisions
there were rarer but possible, and likelier on VMs and containers with coarse
clocks. Every backend overwrote silently.

Each name a process issues is now unique: when the clock repeats, the name's
time moves 1 ns past the last one issued. The name format is unchanged, so
existing files and older binaries read the same. Compaction outputs and DELETE
rewrites use the same stamp. Two nodes writing to one bucket (shared-storage
multi-writer) are not kept apart by this; their clocks would have to agree to
the nanosecond.

### A standby writer's copy of the stream is trimmed every 5 seconds, sized from its volume, and gives way when the volume runs low ([#1266](https://github.com/Basekick-Labs/arc/issues/1266))

With WAL coverage on, a standby writer keeps a copy of the primary's batches
until their files are registered and on the standby. That copy was trimmed
every 30 seconds against a fixed 1 GiB cap. At about a million rows a second,
30 seconds of stream is already about a gigabyte. On a QA cluster both standbys
dropped 27-31 % of the primary's batches in a 30-second run, and a takeover in
that window could not have published those rows.

- The copy is trimmed every 5 seconds, and as soon as it passes half its cap.
  Same QA run with the 1 GiB cap, while the standbys' pulls kept up: no batch
  dropped, and the copy stayed under 160 MB.
- A batch may leave the copy once the files holding its rows (by their
  coverage) are on the standby; a segment of the copy goes when all its batches
  may. Before, a measurement's batches waited until every current file of the
  measurement was there, so one unpulled file (the newest flush, or a
  compaction output) held back every batch of the measurement, in every
  segment: when a standby's pulls fell behind, right after a restart or a
  catch-up, the copy filled and 20-35 % of the batches were dropped on the QA
  cluster. Same cluster now, 1.11M rows/s with a standby restarted mid-run and
  a 1 GiB cap: no batch dropped, the copy peaked at 434 MB; a primary killed at
  that rate came back exact (107,405,000 rows stored of 107,405,000 acked, the
  last 799,000 published from the copy). Rows that sit only in a file
  without coverage of the batch (a compaction output) were never republished
  from the copy; the compaction commit keeps those (peers pull an output before
  its inputs are deleted). With stream takeover off, nothing claims from the
  copy, and a batch goes once it is registered. `arc_standby_store_blocked_keys`
  counts the measurements with batches that may not go yet.
- `cluster.standby_store_max_bytes` defaults to a quarter of the WAL volume, at
  least 1 GiB and at most 32 GiB (a full store is read whole at startup). The
  startup log names the volume size and the cap. On a volume
  that does not enforce its size (k3s `local-path`, `hostPath`, Docker named
  volumes) the volume is the host's disk.
- Free-space reserve: below 10 % of the volume free (at least 2 GiB), the store
  keeps no new batches, first evicting what it may evict. It resumes 1 GiB
  above that, so the WAL (and the Parquet files, when local storage is on the
  same volume) keep their room. On a volume of 2 GiB or less the store keeps
  nothing; the startup log says so.
- The capped state now clears when trimming frees room even if the primary has
  gone quiet.
- New gauges: `arc_standby_store_capped`, `arc_standby_store_low_space`,
  `arc_standby_store_max_bytes`. Alert on
  `increase(arc_standby_store_dropped_total[5m]) > 0`: it counts batches a
  takeover could not publish.

### A standby writer keeps up with the primary's stream at a million rows a second ([#1287](https://github.com/Basekick-Labs/arc/issues/1287))

With stream takeover on, a primary acknowledges a write before any standby has
it and sends every batch to each standby through an in-memory queue
(`cluster.origin_stream_queue_bytes`, 64 MiB). A standby that does not read in
time misses batches: they are gaps in its copy, and until their files are
registered they exist only on the primary, so a primary lost with its volume in
that window takes acknowledged rows with it.

A standby flushed its copy to disk every 100 ms while holding the lock the
receiving session appends under. On a QA cluster at 1.15-1.2M rows/s that lock
was held for 22-30 % of the time, up to 430 ms at once, the 64 MiB queue (about
1.8 s of stream) overflowed, and the primary dropped 1,200-18,662 batches per
standby in two minutes (about 6.6 % in one run).

- The standby store now does its disk work without that lock: fsyncs, the
  fsync of a full segment, new directory entries, the primary's floors, and
  removing trimmed or evicted files (renamed away under the lock, deleted
  after it; a restart deletes any left).
- The floors apply once their file is durable, so trimming never runs ahead of
  what a restart reads back.
- An fsync that fails starts a new segment for the next batches: the failed
  one's unsynced pages may be gone, and a second fsync would not say so.
- Same QA cluster: 0 batches dropped by the primary and 0 gaps at 1.24M rows/s.
  A primary killed at that rate and held down: 90,701,000 rows stored of
  90,701,000 acknowledged in one run, 107,616,000 of 107,618,000 in another.
  The 2 batches were acknowledged but neither streamed nor written to the WAL
  when the process was killed: both happen after the acknowledgement, so the
  newest few milliseconds of writes are on no standby and on no disk when a
  primary dies without warning.

Alert on `increase(arc_origin_stream_dropped_total[5m]) > 0` on the primary
and `increase(arc_standby_stream_gaps_total[5m]) > 0` on standbys: either means
acknowledged rows were, for a while, on the primary only.

### Writes sent to any writer reach the primary, and a forwarded write is never sent twice ([#1270](https://github.com/Basekick-Labs/arc/issues/1270))

With stream takeover on, a standby writer refuses writes (503), and the
Enterprise chart's writer Service sends writes to every writer. On a QA cluster
a client behind the Service saw 18 to 33 refused batches per run in steady
state, and tens of thousands during a failover.

- A standby writer now forwards a write to the primary when it can trust its
  view: it is in steady contact with the cluster leader, no stream-takeover
  state holds it, and the primary is healthy. Otherwise it still answers 503.
  A forwarded write makes one hop at most.
- A forwarded write (from a standby, a reader or a compactor) is sent once.
  Before, the router retried a write that timed out after it was sent, which
  could write its rows two or more times.
  - If the writer could not be reached, nothing was written: `503` with
    `Retry-After` and `X-Arc-Ingest-Fenced`.
  - If the write was sent but no answer came back in
    `cluster.write_forward_timeout_ms` (default 60000), it may have been
    written: `504`. It is never answered `502`.
- Readers and compactors forward writes to the writer the cluster designates as
  primary, even when their own view of writer states is incomplete.
- A node that binds every interface advertises its API as `:<port>`, with no
  host. A forward to such a node dialed the sender itself: a reader's write
  failed (508), and so did a standby's. The host is now taken from the node's
  cluster address.
- The body is forwarded as received. A compressed body was decompressed
  without the size cap and sent on under its original `Content-Encoding`.
- `arc_cluster_writes_forwarded_total{result}`: `ok`, `refused`, `error`,
  `unavailable`, `ambiguous`.
- MQTT and continuous queries on a standby still answer 503; imports forward
  (#1070 below).

### Imports sent to a reader, a compactor or a standby writer reach a writer ([#1070](https://github.com/Basekick-Labs/arc/issues/1070))

The import endpoints (`/api/v1/import/csv`, `parquet`, `lp`, `tle`) never
forwarded. A reader or a compactor imported the file into its own buffer,
outside the writer's WAL, and a standby writer refused it with `503`.

- A reader or a compactor forwards an import to a writer, and a standby writer
  forwards it to the primary, by the rules of the other write endpoints
  (#1270 above): once, with the upload as received, one hop at most. `503`
  with `Retry-After` and `X-Arc-Ingest-Fenced` means nothing was written; `504`
  means it was sent and may have been written.
- A forwarded import is not bound by `cluster.write_forward_timeout_ms`, so a
  large import is not answered `504` while the writer is still importing it.
  It is bound by `cluster.import_forward_timeout_ms` (new, default `3600000`,
  1 hour; `0` = no bound), a backstop for a writer that never answers: the
  forward is not cancelled when the client gives up, and until then the
  forwarding node holds the upload.
- An upload larger than the 500 MB import limit is refused with `413` by the
  node that received it, before it is forwarded.
- A line-protocol import with several measurements that stopped after some of
  them were written answered `503` "retry", as if nothing had been written:
  with `X-Arc-Ingest-Fenced` when the node was fenced part-way, without it when
  the buffer was closing.
  A re-run wrote the earlier measurements again. It now answers `500` without
  those headers, with `written_measurements` and `stopped_at_measurement`. A
  refusal before anything is written is still `503`.
- An import whose rows were all accepted and whose flush then failed says the
  rows may still be stored; check before running the file again.
- A forwarded import is logged by the audit log of both nodes, as other
  forwarded writes are.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1075](https://github.com/Basekick-Labs/arc/pull/1075).

### Bulk imports write in parallel, and wait for a slot at most 30 seconds

Earlier builds of this release let one bulk import (`/api/v1/import/lp`, `csv`,
`parquet`) write at a time per node, whatever its size: a 46-core writer
loading 64 MB line-protocol files with tsm2arc ran at about 85 MB/s, against
about 1,250 MB/s on 26.09.2, with the CPUs mostly idle. More requests in flight
only made each wait longer. A request that waited could not be abandoned: the
server does not see a client that gives up, so the import later wrote all its
rows, and a client retry wrote them a second time.

Measured on a 14-core laptop, 64 MB line-protocol files, 16 in flight:

| | Before | Now |
|---|---|---|
| Throughput | 67 MB/s | 319 MB/s |
| Median import | 15.4 s | 3.0 s |
| Peak memory | 4.5 GB | 5.7 GB |

- New `ingest.import_write_concurrency` (default `0` = auto): how many imports
  write at once. Auto is as many as fit an eighth of the memory limit at one
  import allowance each (an eighth of the memory limit, 64 MB to 512 MB), at
  most the CPUs Arc runs on (`GOMAXPROCS`, which a licence's core count can
  lower), at least one: one on a 3 GiB writer, nine on a 36 GiB laptop, 46 on
  a 46-core, 320 GiB writer licensed for all its cores. The value is logged at
  startup ("Bulk import write slots configured"). Each writing import may hold
  about one allowance plus its upload, and each import that wakes from a wait
  writes one batch before it paces again; set a lower value to bound memory
  more tightly. An explicit value (1 to 1024) is used as is. `1` restores one
  import writing at a time; the next one starts while the previous one is
  still flushing, within the same allowance.
- New `ingest.import_queue_timeout_ms` (default `30000`; `0` = until a slot
  frees, since the server does not see a client that gives up): an import that waited this long for a slot is refused with
  `503`, `X-Arc-Ingest-Refused: import_queue` and `Retry-After`. Nothing of it
  was written, so it is safe to retry. Keep it well under the client's own
  timeout.
- A client that times out while its import is writing still gets no answer,
  and the import still completes: treat such an attempt as possibly written
  before retrying it.
- An import ends by flushing its own measurements' buffers, not every buffer
  on the node, and it gives its slot (and its allowance) to the next import
  before that flush. An import that stops part-way flushes what it wrote
  before it answers. While an import waits for flushes it flushes its own
  measurements again every second; a flush that fails there stops it part-way
  (`500`), as the first one did.
  The `200` contract is unchanged: the rows are in the buffer and the WAL
  (when enabled), files follow as their flushes finish.
- A write refused because other writers of the same measurement keep
  changing its columns (`ErrSchemaChurnExceeded`) is tried again up to five
  times inside the import before it fails (not for line-protocol imports on
  deployments with `ingest.decimal_columns`, which write records). With imports running side by
  side, two imports into one measurement whose columns change type can now
  meet more often; that still fails part-way, as before.

### Bulk imports no longer hold the whole file in memory several times

The import endpoints (`/api/v1/import/csv`, `parquet`, `lp`, `tle`) read the
upload, parsed all of it, and wrote it in one go. On a QA cluster a 172 MB
line-protocol file OOM-killed a writer with a 3 GiB limit; since #1070 a reader
or a standby forwards such an import to the primary, so the primary took the
hit and failed over. Live memory one import adds, measured from the moment the
handler starts (the request body is already held), WAL off (the WAL adds more
to the old numbers):

| File | Before | Now |
|---|---|---|
| Line protocol, 197 MB | +3.8 GB | +71 MB |
| CSV, 175 MB | +1.1 GB | +27 MB |
| Parquet, 274 MB, one row group | +1.6 GB | +77 MB |

On the QA cluster (writers limited to 3 GiB) the 172 MB file that OOM-killed
the primary now imports in 16 s, and 499 MB line protocol, 452 MB CSV and
478 MB Parquet imports through a reader or a standby complete with exact row
counts and no restarts; the primary, which also holds the forwarded body,
peaked at 2.2 GiB.

- The file is read in place in the request body. Before, it was copied twice
  more (the multipart form, then a full read).
- Line protocol, CSV and Parquet are processed in chunks (about 4 MB of line
  protocol; 100,000 CSV or Parquet rows, fewer for wide files: at most a
  million CSV cells or two million Parquet values), and each chunk is written
  before the next is read. Parquet column chunks are read a buffer at a time.
- An import's writes are paced to the flushes: what it has written and not yet
  flushed stays under an eighth of the memory limit (64 MB to 512 MB) above
  what the buffer held when it started. Over that it flushes its database's
  buffers and waits; if the buffered rows stay above the limit for two
  minutes, it stops (a failure part-way, below).
- Imports write in parallel, up to `ingest.import_write_concurrency` at once
  on a node (#1429, above). Each writing import paces against one limit
  shared by all of them: the lowest buffered level any of them started at,
  plus the allowance of every import holding a slot. Every request body is held in memory while it waits for a slot.
  A forwarded import waiting for a slot on the writer still counts down
  `cluster.import_forward_timeout_ms` on the node that forwarded it.
- A validation pass reads the whole file, chunk by chunk, before anything is
  written, so a bad file is still refused with nothing written: a bad time
  value, an invalid measurement name, a measurement the token may not write,
  a CSV parse error. CSV column types are still decided by the whole column,
  and a line-protocol field's type by its first value in the file. Line
  protocol is also refused (`400`) when a column mixes strings or booleans with
  numbers (a field, or a field and a tag of the same name in other lines), or
  a line is longer than 64 MB; before, a mixed column failed while being
  written. Decimal columns (`ingest.decimal_columns`) convert any value, as
  before.
- An import that fails after part of it was written (the node is fenced, the
  buffer is closing, flushes are not keeping up, a later part of the file
  cannot be decoded) answers `500` with `rows_written` and
  `written_measurements`, and no `Retry-After`: running the whole file again
  writes those rows again.
- A request with a `Content-Encoding` header other than `identity` is refused
  with `415`; compress the file itself (gzip line protocol) instead. Before,
  the server decompressed such a body with no size limit.
- A `file` form field sent without a filename is not taken as the upload, as
  before. A request body cut short answers `400` for every format.
- A `200` still means every row is in the write buffer and the WAL (when
  enabled); the files follow as their flushes finish.
- Rows are written grouped by hour, so an unsorted file makes about one file
  per hour as before (see "Imports group rows by hour" below).
- TLE files are parsed whole, as before, but read once.

### NULLs survive a WAL replay; `time` and the WAL's own names are no longer column names

Three ways a crash and WAL replay changed data, and two names that broke it:

- **CSV, Parquet and TLE imports:** an empty cell or a null value was written
  into the WAL as `0`, `""` or `false`. If the node crashed before the rows
  were flushed, replay stored those values instead of NULL. NULLs now replay
  as NULL. WAL entries written before this release still hold the values.
- **Sparse columns:** replay re-reads an entry in batches of
  `wal.recovery_batch_size` (10,000) rows. A batch holding none of a column's
  values wrote that column as an all-NULL text column, and once that file
  existed, `sum()` on the column failed for the whole hour with
  `sum(VARCHAR)`. On a QA run (20,000 line-protocol points, a field set only
  in the first 5,000, `kill -9` before the flush) main failed `sum(v)` after
  restart; it now returns the right total. A line-protocol or msgpack column
  NULL in every row of an entry still replays as the all-NULL column the live
  write wrote; an import's (CSV/Parquet/TLE) is left out, since the replay
  cannot know its type. Entries already in the WAL when you upgrade keep their
  old form and can replay as before, once.
- **Columns named `measurement`, `m` or `database`** were dropped on replay,
  and a column named `_database` or `_measurement` sent its rows to another
  database or measurement. Those three names now replay as columns. Imports
  refuse `_measurement` and `_database` as column, tag or field names (`400`,
  nothing written); through `/write` such a column is still lost on replay,
  but no longer moves its rows.
- **A tag or field named `time`:** a field silently replaced the point's
  timestamp (`time=5i` stored the point in 1970), and a tag failed the write
  part-way. Line-protocol imports refuse such a file (`400`, nothing
  written). `/write` drops only those points, writes the others, and answers
  `400` "partial write: N points dropped", as InfluxDB does.

### Imports group rows by hour, and line protocol is parsed straight into typed columns

Chunked imports (above) wrote each chunk's flush as one file per hour it
touched, so an unsorted file made a file per hour per chunk. On a QA host, a
4M-row file shuffled over a year (8,760 hours) made 455,420 files as line
protocol and 350,398 as CSV or Parquet. Now each makes 8,760, with identical
per-hour counts and sums. Peak RSS for the three imports went from 2.2 GB to
1.2 GB. Times: line protocol 27.6 s to 16.5 s; CSV 8.6 s to 11.2 s and
Parquet 5.6 s to 7.4 s (about 30% slower: rows are copied into their hour
groups before writing).

- **Hour grouping.** An import holds its parsed rows, grouped by hour, up to
  half its memory allowance, then writes them one batch per measurement and
  hour (at most a million cells per write). A file sorted by time writes as
  before; an unsorted one makes about one file per hour per window of that
  size. Deployments with `ingest.decimal_columns` set keep writing
  line-protocol imports chunk by chunk, so the WAL keeps decimal values as
  sent. Only the import endpoints group: the write endpoints (msgpack, line
  protocol, `/write`, `/api/v2/write`) are unchanged and leave a random-order
  backfill's small files to compaction. For a large backfill, use an import
  endpoint or send the data sorted by time.
- **Typed line-protocol parse.** Each chunk is parsed straight into typed
  columns, without a map per tag and field per line: a 197 MB line-protocol
  import now adds 25 MB of memory (71 MB before). The result is the same as
  before (checked against the old path over a fixed corpus and 2.6 million
  fuzzed inputs).
- **A line whose keys land in one column is refused** (`400`, nothing
  written): a tag `x`, a field `x` (stored as `x_value`) and a field
  `x_value`. Before, which of the two values was kept depended on map order.
- **Failures:** an import that fails before its first window is written
  (a corrupt Parquet page found while writing, for example) now writes
  nothing and answers the plain error, not a `500` with `rows_written`. A
  failure part-way still answers `500` with `rows_written`, which now counts
  written rows across hours, not a leading part of the file.
- **WAL accounting:** CSV, Parquet and TLE imports wrote their WAL entries
  without the database envelope and without charging them to the import's
  WAL reservation (#676), so during a long import the disk gate counted those
  bytes twice: once as reserved, once as written. They are now charged as
  appended, as line-protocol writes already were.

### Line-protocol writes are parsed straight into typed columns, and drop only the points that do not fit

The line-protocol write endpoints (`/write`, `/api/v2/write`,
`/api/v1/write/line-protocol`) now parse a request straight into typed
columns, as line-protocol imports do (above), instead of a map per tag and
field per line. In-process benchmarks (1,000-line requests, Apple M3 Max)
moved from 4.18M to 7.61M rows/s for the `sustained_bench` IoT shape with the
WAL off, and from 2.26M to 4.48M with the WAL on; other shapes gained 24% to
98%, with 8% to 41% fewer allocations per request.

- **A point that does not fit is dropped, not the whole measurement.** A
  column is still typed by its first value in the request. A later point
  whose value does not fit (a string in a float column, a number in a tag
  column, an unsigned value above the int64 range or NaN/Inf in an integer
  column) is dropped, and every other point is written, the same measurement
  included. Before, such a value failed that measurement's write with a
  `500` after other measurements of the request might already have been
  written. A point whose keys land in one column (a tag `x`, a field `x`
  stored as `x_value`, and a field `x_value`) is dropped too; before, which
  value was kept depended on map order.
- **The answer** is `400` "partial write: N points dropped: ..." naming the
  measurement, column and reason, as InfluxDB does (the `time` key drops
  above answer the same way). Each request logs its drops at Error level, one
  line per measurement, column and reason (up to 10, then one line counting
  the rest), only when the request was otherwise written.
- **WAL entries** for typed writes (line protocol, and CSV, Parquet and TLE
  imports) are encoded straight from the columns. A line-protocol entry now
  holds the value the buffer stored (an integer column's `2.9` is `2`).
- Deployments with `ingest.decimal_columns` set keep the previous write path.

### Flushes copy each column once less, and two column names no longer crash a flush

Every flush (msgpack, line protocol, imports, WAL replay) builds its Parquet
file with fewer copies of the buffered data:

- **Sorted merge.** A flush used to concatenate every column of its buffered
  writes into one batch, then copy every column again in time order. When the
  measurement sorts by time alone and the flush spans one hour (the common
  live-ingest case), it now sorts the time column and writes every other
  column once, straight from the buffered writes, in that order. The files
  are the same, row order included.
- **No copy into Arrow.** Integer, float and timestamp columns go to the
  Parquet writer as they are, instead of being copied into Arrow builders;
  string, boolean and decimal builders are sized once instead of growing.
- **Parquet output buffer** sized from the measurement's last file of at
  least 1,024 rows (never past the write's own column bytes) instead of
  growing by doubling.

In-process benchmarks (1,000-row requests through the handlers, Apple M3
Max) allocate 16% to 28% fewer bytes per msgpack columnar request and 5% to
11% fewer per line-protocol request, at the same speed or slightly faster.
End to end with `sustained_bench` on the same laptop (12 workers, WAL off):
msgpack columnar 37.2M to 38.1M rec/s, line protocol 8.94M to 9.32M rec/s
(p99 3.89 ms to 3.34 ms), and msgpack throughput declines less over a
60-second run (38.5M to 35.6M before, 39.0M to 37.0M now).

**Fixed: a column named `_...` or with an empty name crashed the node.** Such
columns are never stored, but a flush still merged them: a `_x` column sent
as an integer in one request and a float in the next panicked the flush
worker, and a column with an empty name (`{"": [...]}` in msgpack) panicked
every flush of its measurement, taking the process down. They are now
skipped wherever a flush merges columns or builds a schema.

### Telegraf and batch msgpack writes take the fast path; a few bytes can no longer exhaust a node's memory

**Telegraf arrays and `{"batch": [...]}`.** Telegraf's Arc output sends a
top-level array of columnar maps whenever a flush holds more than one
measurement, and the Python client's `encode_batch` sends `{"batch": [...]}`.
Both went through the generic decoder (every value boxed), and each item's
WAL entry was rebuilt as one map per row. Each item now goes through the
typed columnar decoder, and its WAL entry is the item's own bytes, as a
single columnar map's is. In-process (1,000-row requests, M3 Max), a
Telegraf-shaped request went from 306 µs to 80 µs with the WAL on, and a
batch request from 284 µs to 55 µs (43 µs to 29 µs with the WAL off), with
42% to 81% fewer bytes allocated per request. If any
item is not a columnar map the typed decoder can take (a row-format item,
one without a time column), the whole request is decoded as before. Tag
columns (`tag_keys`) of array and batch items now survive a WAL replay; the
row-form entries lost them.

**Malformed payloads.** Four msgpack payloads did harm out of proportion to
their size; each is now a `400`:

- A value declaring more bytes than the request holds: five bytes
  (`c6 7f ff ff ff`, "2 GB of binary follow") made the decoder allocate 2 GB
  before failing, so a few concurrent requests could exhaust a node's memory.
  A column declaring a million values in a 25-byte request allocated 16 MB.
- Arrays nested a few million deep (a few MB of `0x91`) overflowed the
  decoder's stack, which kills the process. Nesting is capped at 64 levels;
  real payloads use four or five.
- A map with a `nil` key panicked inside the decoder (a `500` with a stack
  trace in the log).
- A column name repeated in `columns`, or an extension value next to the
  columns, could be read differently by the fast and the generic decoder;
  such payloads now take the generic decoder, which decides.

### Row-format msgpack writes are decoded straight into typed columns, keep the client's host tag, and drop only the points that do not fit

Row-format msgpack (`{"m", "t", "h", "fields", "tags"}`, alone, in an array,
or in `{"batch": [...]}`: the Python client's `write_records`) went through a
map per row and per field, then a regrouping into columns. It is now decoded
straight into typed columns per measurement. In-process (1,000 rows per
request, M3 Max): 359 µs to 139 µs with the WAL off, 595 µs to 264 µs with it
on, half the bytes and allocations.

- **The client's host tag is kept.** A row without `h` had its `host` tag
  replaced by `unknown`, so every row the Python client wrote (it sends
  `tags`, never `h`) was stored as `host=unknown`. Now `h` sets the host
  when sent, the row's own `host` tag is kept when not, and `unknown` is
  written only when neither is there, as before.
- **Points that do not fit are dropped, not the measurement** (as line
  protocol writes, above): a value that does not fit its column's type (the
  first value in the request decides), a tag or field named `time`, or two
  keys of one row landing in one column. The others are written and the
  answer is `400` "partial write: N points dropped"; the drops are logged
  at Error. Before, a value of the wrong type failed the measurement's write
  with a `500`; a field named `time` corrupted the time column; and a row
  with a tag `x` and a field `x_value`, next to a row with a field `x`,
  was accepted with a `204` and then made every flush of the buffer fail,
  retried forever (or, depending on map order, panicked the flush and the
  process). Rows still decoded the old way (decimal deployments, rows with
  an unusual value type such as a float32 or a non-string tag) now get the
  same treatment: the rows that cannot be written are dropped and the
  answer is the same `400` partial write; MQTT logs them at Error and
  writes the rest.
- Tag values are made valid UTF-8, as field values already were.

### Query responses stream straight to the socket: Arrow up to 60% faster

A query's response was encoded on a goroutine of its own into an in-memory
pipe, and the connection copied the pipe to the socket 4 KB at a time,
flushing the socket after every 4 KB. The encoder now runs on the connection
itself and every batch it flushes goes out as one chunk, with no pipe, no copy
and no second goroutine. Over loopback, against 2M rows of local Parquet:

| query | endpoint | 26.09 | 27.01 |
|---|---|---|---|
| `LIMIT 1000000` | Arrow IPC | 80 ms | 32 ms |
| `LIMIT 1000000` | MessagePack | 91 ms | 69 ms |
| `LIMIT 1000000` | JSON | 394 ms | 326 ms |
| `LIMIT 1000000` | JSON, gzip | 794 ms | 730 ms |
| `LIMIT 100000` | Arrow IPC | 11.8 ms | 6.9 ms |
| `LIMIT 100000` | MessagePack | 11.6 ms | 10.2 ms |
| `COUNT(*)` | any | ~1.05 ms | ~0.98 ms |

Responses are byte-for-byte the same; only their chunking on the wire
changed. A client that goes away mid-response still stops the query at the
next batch, and a response that is never sent (a `HEAD` request) cancels its
query before releasing it.

### JSON query responses are encoded on several cores: up to 4x faster

Turning a result into JSON (floats and timestamps above all) was nearly the
whole cost of a large JSON response, and it ran on one core. From a result's
third batch on (about 4,000 rows), batches are encoded on other cores while
the query goroutine reads the next ones, and written in order; timestamps
reuse the formatted date and time of the second before. Over loopback,
against 2M rows of local Parquet:

| query | 27.01.1 before | 27.01.1 |
|---|---|---|
| `LIMIT 1000000`, JSON | 323 ms | 79 ms |
| `LIMIT 1000000`, JSON with zstd | 600 ms | 406 ms |
| `LIMIT 1000000`, JSON with gzip | 729 ms | 515 ms |
| `LIMIT 100000`, JSON | 35.9 ms | 11.9 ms |
| `LIMIT 10000`, JSON | 6.9 ms | 5.3 ms |
| `COUNT(*)` | 0.96 ms | 0.94 ms |

Responses are byte-for-byte the same, compressed ones included (compression
itself still runs as one stream). Encoding takes at most GOMAXPROCS-1 extra
goroutines across the whole process and at most four batches per query, and a
core is held only while a batch is being encoded: with fourteen queries
running at once, `LIMIT 100000` was 29% faster and smaller queries unchanged.
Encoded batches go to the client as soon as they are ready, and a cancelled
query still sends every row it read before the cancellation.

### MessagePack query responses are encoded on several cores too: up to 40% faster

`/api/v1/query/msgpack` responses are columnar, so a column's values are cut
into runs of about 16,000 rows (or 1 MB) that are encoded on the same encoder
goroutines as JSON's batches and written in order. Small results (up to two
batches) keep the single-goroutine encoder. Over loopback, against 2M rows of
local Parquet:

| query | 27.01.1 before | 27.01.1 |
|---|---|---|
| `LIMIT 1000000`, MessagePack | 67.1 ms | 39.6 ms |
| `LIMIT 1000000`, MessagePack with zstd | 147 ms | 121 ms |
| `LIMIT 100000`, MessagePack | 9.8 ms | 7.2 ms |
| `LIMIT 10000`, MessagePack | 4.2 ms | 4.1 ms |

Responses are byte-for-byte the same.

### Stream takeover publishes the rows of unpulled files in one go, not in waves ([#1269](https://github.com/Basekick-Labs/arc/issues/1269))

When a primary writer dies with files nobody had pulled yet, the new primary
waits twice `cluster.stream_takeover_grace` for some node to answer for each
file, then publishes its rows again from its copy of the stream. Only 32 files
were checked at a time, and a file's wait started only when it was first
checked, so with more files their rows came back in waves about three minutes
apart. On a QA cluster with 70 such files, the rows took 10 minutes to be
complete after the crash.

- Every missing file's wait starts when the takeover sees it missing, from the
  start of the claim, while the rest of the claim replays.
- Each check asks first about the files whose wait is over, then the ones not
  asked yet. A file is still published again only on an answer from that
  check.
- After files are taken back, the next check runs on the next pass, not 30 s
  later. A check spends at most 10 seconds asking; slow peers make it take more
  checks, not block the takeover.

The grace itself is unchanged.
### Tier status no longer lists files the node's own compaction or retention removed ([#1271](https://github.com/Basekick-Labs/arc/issues/1271))

On a cluster with local storage, a node's tiering metadata kept a `hot` row for
every file that node itself had removed before the cluster's delete reached it:
the inputs of its own compactions (the dedicated compactor) and the files its
retention run removed. `GET /api/v1/tiering/status` and `/files` reported files
that no longer existed until the next daily tier cycle retired them (465 extra
rows on a QA compactor after one compaction cycle).

The node now retires those rows as the delete arrives, after checking the file
is really gone. A `cold` row is never touched. A path registered again while a
compaction input waited for its staged output also keeps its fresh copy now.
### A standby writer no longer drops batches it received while trimming its copy ([#1266](https://github.com/Basekick-Labs/arc/issues/1266))

A standby writer trims its copy of the primary's stream once the batches are in
registered files that are on this node. Trimming chose its segments, then
checked the files without holding the store lock. The primary kept streaming
into the open segment meanwhile, and the trim deleted that segment with the
batches that had just arrived, which were not registered yet. Had the primary
then failed with its volume, a stream takeover could not have published those
rows. A segment that grew during the check is now kept until the next pass.

### The reconciler no longer deletes files a writer has not registered yet ([#1214](https://github.com/Basekick-Labs/arc/issues/1214))

On shared storage, each writer uploads a flushed file to the bucket first,
then registers it in the cluster manifest. A writer that cannot reach the
cluster leader keeps the file pending until it can. The orphan reconciler
runs on the active compactor, which cannot see another writer's pending
files. Once such a file was older than the grace window, the reconciler
deleted it as an orphan. When the writer came back, the file was gone, and
because the writer had already released those rows from its WAL, the
acknowledged rows were lost. A compactor partitioned from the leader could do
the same to files the rest of the cluster had registered, because it compared
the bucket against its own stale copy of the manifest.

A reconciliation run now:

- Catches up with the cluster leader before each sweep. Without a reachable
  leader, the run stops before deleting anything (`abort_reason`
  `manifest_not_synced`, or `leader_barrier_unsupported` when the leader runs
  an older release).
- Asks every node that can ingest (writers) which files it still has pending,
  over an authenticated cluster message. If any of them does not answer, the
  storage half of the run is held: nothing is deleted from storage, the run
  reports `storage_sweep_held` with `storage_sweep_held_reason` naming the
  node, and the new gauge `arc_reconciliation_storage_sweep_held` is 1. A
  writer that is down, or a node that was decommissioned but never removed
  from the cluster, holds the storage sweep until it is back or removed.
- Skips every file listed in the bucket under `_registration_pending/`. Each
  writer writes its pending files there when it stops and whenever a file
  stays pending for more than a minute, and removes the list once everything
  is registered. A writer that stops with files pending does not leave the
  cluster until that list is written, so it keeps holding the sweep if the
  list cannot be written.
- Skips compaction outputs that the manifest repair will adopt.

The skips are counted in the new run fields `skipped_pending_remote` and
`skipped_adoptable`.

`reconciliation.grace_window_seconds` now has a minimum of one hour. A
smaller value is raised to one hour with a warning.

During a rolling upgrade, the storage sweep stays held until every writer
runs this release, because older writers do not answer the new message.

Two smaller fixes on shared storage:

- **DELETE during a rolling upgrade.** While compaction commit is not yet on,
  a row DELETE that reaches a file another writer flushed less than an hour
  ago and has not registered yet now leaves that file as it is and answers
  `503` with `Retry-After`. It used to rewrite the file without registering
  the rewrite, and the reconciler later deleted the rewrite with its surviving
  rows. If other files also failed, the answer is `207` and names both.
- **Ghost manifest entries.** A writer now checks, after registering, that
  each file it just flushed still exists. A file another node deleted in the
  meantime is removed from the manifest again, instead of being left as an
  entry with no object that made DELETEs on that measurement answer `503`
  until the hourly repair.


### A failover no longer floods the logs with two errors per refused write ([#1268](https://github.com/Basekick-Labs/arc/issues/1268))

A writer that refuses writes (it lost contact with the cluster leader, another
writer is the primary, or a stream takeover holds it) logged two ERR lines for
every refused request: `Failed to write to Arrow buffer` and
`HTTP request error status=503`. During a failover a client retrying at a few
hundred writes per second produced over a thousand lines per second. Kubernetes
rotated the container logs within seconds, and the lines that explained the
failover were gone.

- A refused write is logged once per reason every 30 seconds, at WARN:
  `Writes refused on this node (retryable)`, with the reason, where the write
  came from, the error, and how many refusals were not logged since the last
  line. A new reason logs at once, so a standby's routine refusals never hide a
  primary that fences itself.
- The refusal is no longer counted in `arc_ingest_errors_total`, on every write
  path (MessagePack, line protocol, TLE, the import endpoints, MQTT).
- The answer is unchanged: `503` with `Retry-After: 5`. It also carries
  `X-Arc-Ingest-Fenced: 1`. A reader or compactor that forwarded the write logs
  the refusal the same way (reason `forwarded`), not once per request.
- `arc_cluster_ingest_fenced_writes_total` has a `reason` label: `contact`,
  `standby`, `claimed`, `announcing`, `not_designated`, `delete_ack`, `other`.
  It now also counts the stream-takeover refusals, which it missed before. Sum
  by reason for the old total. A refusal is counted per attempt: an MQTT message
  retried while refused counts once per retry.
- Other 503s (shutting down, schema churn) are still logged per request.

### A new Raft leader no longer re-promotes a dead primary writer, or fails over a healthy one ([#1267](https://github.com/Basekick-Labs/arc/issues/1267))

When a node joined the cluster after the primary writer was designated, its
registry lost that writer's `primary` state: the join response carries no writer
state. If that node later became the Raft leader, its failover manager saw no
primary at all:

- If the primary had just died, the new leader "failed over" within three health
  checks to the lowest-id writer still marked healthy. That was the dead primary
  itself while its heartbeats were not yet stale. The no-op promotion counted as
  a success and started the 60 s `cluster.failover_cooldown`, so writes were
  refused for 83 s on a QA cluster instead of about 35 s.
- If the primary was alive and not the lowest id, the new leader failed it over
  for no reason.

Now:

- Failover decisions use the cluster's designated primary (the Raft state) and
  that node's health, not the registry's writer state. A join keeps the writer
  state it knows.
- Re-promoting the designated primary is not a failover.
- Only a completed failover starts the cooldown. A failed attempt is retried on
  the next health check (5 s). A writer the cluster state refuses to promote is
  skipped for a minute; a Raft error (lost leadership, timeout) does not count
  against it.
- A promotion Raft committed is a success, even when the failover's wait times
  out.
- `GET /api/v1/cluster` reports each node's `writer_state`, the
  `primary_writer_id`, and `writer_failover` (on the Raft leader: failed
  attempts, the last failure and its reason, the last failover).

### A tag name with no matching column no longer stops a partition from compacting ([#1258](https://github.com/Basekick-Labs/arc/issues/1258))

Ingest records each file's tag names in its `arc:tags` metadata, and
compaction deduplicates on them. A name with no column of that name in the
files could not be used by compaction, so every cycle failed the same way and
the partition never compacted. Because compaction combines the tag names of
all files in a partition, one such file was enough to stop the whole
measurement-hour. Three inputs produced these names:

- A MessagePack `tag_keys` entry that is not a column of the payload, such as
  a typo, or a name containing a comma. New in this release.
- A line-protocol tag key that starts with `_`. Arc does not store columns
  whose name starts with `_`, but it still recorded the key as a tag. This
  affects earlier releases.
- A line-protocol tag key with an escaped comma (`a\,b=1`). The metadata
  separates names with commas, so it read back as two tags, `a` and `b`. This
  affects earlier releases.

Ingest no longer records a tag name that is empty, is `time`, contains a
comma, starts with `_`, repeats an earlier name, or is not a column of the
data being written. Names are matched to columns exactly, including case. The
rows are always written, and a warning names what was left out. The warning
is logged at most once per second.

Compaction now ignores tag names its files have no column for and
deduplicates on the remaining ones. Partitions stuck by files from earlier
releases compact again without any action. Deduplication still removes only
rows that are identical in every column, so a shorter tag list never makes
two different rows equal.

Tag names with other characters, such as the dot in OTel-style keys like
`service.name`, are handled as before. They are kept, and compaction skips
deduplication for any batch of files that includes one.

Creating or updating a continuous query now refuses a `tag_columns` entry
that starts with `_`, because Arc does not store those columns. A query
already saved with one keeps running, and that name is left out of its
output's tag metadata.

### Compaction works with Azure storage on a connection string or SAS token ([#1104](https://github.com/Basekick-Labs/arc/issues/1104))

Compaction runs in a separate process, and that process authenticated to Azure
Blob Storage its own way: with the account key, or with managed identity when
there was none. Azure primary storage that used either of the other two
authentication methods the configuration accepts could not compact. Every
compaction job failed, and data stayed in its small ingest files:

- **`storage.azure_connection_string`** with no `azure_account_name`: every job
  failed with `no valid Azure authentication method configured`.
- **A connection string together with an account name and key:** the compaction
  process used the key against the public Azure endpoint instead of the endpoint
  in the connection string (Azurite, sovereign clouds).
- **`storage.azure_sas_token`:** the compaction process fell back to managed
  identity, which fails outside Azure.

The compaction process now gets the same credentials as the server and builds
its storage connection the same way. Account-key, managed-identity and S3
deployments were not affected.

The credentials now travel in the job the server hands the compaction process,
not in its environment. If the binary is downgraded while the server keeps
running, the older compaction process looks for them in the environment and
compaction fails until the server is restarted. An upgrade in place is
handled.

### Retention policies apply to the cold tier ([#168](https://github.com/Basekick-Labs/arc/issues/168))

With tiered storage, retention deleted expired files from local storage only.
Files tiering had moved to the cold tier were kept forever, so cold storage grew
without bound, and a measurement whose data had all moved to cold was never
examined at all.

A policy now covers the cold tier too. After each measurement's local files,
retention checks the cold files whose partition starts before the cutoff. A
file is deleted from cold storage when every row in it is older than the
cutoff, then its tier row is removed. Measurements that exist only in the cold
tier are processed. The scheduled run, manual execution and dry runs all
include the cold tier.

The execute response reports the cold tier separately in `cold_files_deleted`
and `cold_rows_deleted`. These are also included in `deleted_count` and
`files_deleted`.

The cold tier never stops local retention. A cold file that cannot be read or
deleted, a quarantined one, and the cold rows on a node with
`tiered_storage.cold.enabled` off are left in place. They are counted in
`cold_files_skipped`, explained in `cold_skipped_reason`, and the execution is
recorded as `completed_with_errors`. A tier row whose cold object is already
gone is removed, or pointed back at the local copy of the file when one exists.

Backups now tolerate a cold object that retention deletes while a backup is
running. The object is skipped and counted like a local file that disappears
mid-run. Any other cold read failure still fails the backup.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#1053](https://github.com/Basekick-Labs/arc/pull/1053).

### Compaction waits for recent compaction outputs again ([#1235](https://github.com/Basekick-Labs/arc/issues/1235))

Each tier skips a partition while its newest file is younger than the tier's
`min_age_hours`, so compaction does not race late writes. Since 26.09.1,
compaction outputs carry a batch number (`_b<N>_compacted`, `_b<N>_daily`), and
the check could not read their timestamp, so it ignored them.

The visible effect was on the daily tier, whose inputs are mostly hourly
outputs. A day whose every hour had been hourly-compacted passed the check
whatever its age. With the default schedule it was compacted at the first
03:00 run after the day, while late writes could still be arriving.

The check reads these names again. Such a day now waits until its newest hourly
output is `daily_min_age_hours` old: with the defaults, the second 03:00 run
after the day, as before 26.09.1. Days older than
`daily_skip_file_age_check_days` (default 7) are not checked, as before.

Contributed by [@jallegri](https://github.com/jallegri) in [#1250](https://github.com/Basekick-Labs/arc/pull/1250).

### WAL-on ingest is no longer capped by one writer's fsync latency ([#1246](https://github.com/Basekick-Labs/arc/issues/1246))

With the WAL enabled, ingest topped out at a rate set by the storage's fsync
latency, not by CPU or disk bandwidth. One goroutine wrote every WAL entry and
ran `fdatasync` itself every 1 MiB, and on every buffer flush's checkpoint,
waiting each time for the disk to answer. On AWS gp3 that is about 170 MB/s of
WAL, roughly 3.5M points/s of MessagePack, and it was the same on a 4-, 8- or
16-vCPU server. Above it the WAL queue fills and entries are dropped. The
write is still acknowledged, so those rows have no WAL copy until the next
flush writes them to Parquet, and a crash in that window loses them.
`arc_wal_dropped_entries_total` counts these drops.

Syncs now run on their own goroutine, beside the writer (group commit). The
writer keeps appending while a sync is in flight, and one sync covers
everything written before it started, so throughput follows the disk's
bandwidth instead of its round-trip time.

Measured on m7i.4xlarge with the sizing campaign's gp3 volume (500 MB/s, 6,000
IOPS), MessagePack, 8 workers × 5,000-record batches, WAL on with
`fdatasync`, the same builds before and after this change:

| Offered | WAL entries dropped, before | after | WAL disk writes, before | after |
|---|---|---|---|---|
| 4M rows/s, 120 s | 2,637 | 0 | 195 MB/s | 226 MB/s |
| 6M rows/s, 30 s | 7,379 | 0 | 144 MB/s | 284 MB/s |
| 8M rows/s, 30 s | 20,797 | 0 | 148 MB/s | 357 MB/s |
| 10M rows/s, 30 s | 34,625 | 0 | 156 MB/s | 408 MB/s |

Write p95 stayed at 2.0–2.3 ms in every run. The WAL-on figures in the sizing
guide were measured before this change.

Durability is unchanged:

- A flush checkpoint is still answered only after a sync that covers it, in
  every sync mode, `async` included.
- A data sync is still requested once 1 MiB is unsynced, and every 100 ms
  while anything is unsynced.
- A rotation still syncs the old file before it is closed.

A failed sync is now reported to the checkpoints it covered, as before. It also
makes the writer move to a new WAL file, because Linux reports a lost
write-back only once, and a later sync of the same file can succeed over data
it never wrote. Failed syncs are counted in the new `arc_wal_failed_syncs_total`.

`wal.sync_bytes` (default 1048576, `ARC_WAL_SYNC_BYTES`) sets the byte
threshold. It bounds how much written data a sync may leave behind, not ingest
throughput. A negative value is refused at startup.

Above the new ceiling, when the WAL queue is full, an entry is still dropped and
the write still acknowledged. What admission should do instead is
[#676](https://github.com/Basekick-Labs/arc/issues/676).
### Initial writer-election tests wait for the manager's published state ([#1196](https://github.com/Basekick-Labs/arc/issues/1196))

Raft records a promoted writer before the election goroutine updates the manager's
`primaryID` and clears `failoverInProg`. Under load, the tests could observe the
Raft state in that gap and fail. They now wait for the manager state they assert.
Runtime election behaviour is unchanged.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1200](https://github.com/Basekick-Labs/arc/pull/1200).

### Ingest memory is visible while flushes back up ([#1038](https://github.com/Basekick-Labs/arc/issues/1038))

`arc_buffer_records_buffered` counts only records still in a buffer. A record
handed to the flush queue, or being written, left that gauge while it was still
in memory. So under backpressure, when ingest holds the most, the gauge could
read 0: in one measurement it stayed at 0 for 40 seconds while 43 million
queued rows were written out.

Two new gauges cover the rest, and the three sum to the records ingest holds
in memory:

- `arc_buffer_records_queued`: records in flush tasks waiting for a worker.
- `arc_buffer_records_inflight`: records being written to storage, including
  synchronous flushes (`FlushAll`, the flush before a DELETE, shutdown).

They also appear as `buffer_records_queued` and `buffer_records_inflight` in
`GET /api/v1/metrics` and `/metrics` as JSON, under `buffer` in
`GET /api/v1/metrics/endpoints`, and in the stored time series. A climbing queued
count with a flat buffered one means storage is the bottleneck, not the write
rate.

### Ingest reports buffered memory in bytes, and warns before it becomes a problem ([#1025](https://github.com/Basekick-Labs/arc/issues/1025))

Ingest buffers grow with active measurements, `max_buffer_size` and slow
storage, and Arc does not cap them. Until now the only signal was a record count,
and bytes per record vary more than 100x between workloads, so it could not
answer how much memory ingest holds. Three new gauges and a warning do:

- `arc_buffer_bytes_buffered`: approximate bytes held by ingest, across
  buffers, the flush queue and writes in progress. MessagePack writes count
  their payload size; other writes estimate from their columns.
- `arc_buffer_oldest_unflushed_seconds`: age of the oldest record not yet in
  storage, including queued and in-flight flushes. It is what a crash would
  lose without the WAL.
- `arc_buffer_memory_limit_bytes` and `arc_buffer_memory_pressure`: the memory
  limit Arc detected (the container limit where there is one) and whether
  buffered bytes have reached half of it.

When buffered bytes reach 50% of the limit, Arc logs a warning naming the two
settings that reduce held memory: `ingest.max_buffer_size` and
`ingest.max_buffer_age_ms`. Lowering either flushes sooner, at the cost of more,
smaller Parquet files and more compaction work. The warning fires once per
episode: it re-arms only after usage falls below 40%. Arc still rejects nothing
and caps nothing.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1040](https://github.com/Basekick-Labs/arc/pull/1040).

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1042](https://github.com/Basekick-Labs/arc/pull/1042).

### Arc's own storage roots are no longer treated as databases ([#1012](https://github.com/Basekick-Labs/arc/issues/1012))

`_schema` (the field-schema anchors of every database) and `_compaction_state`
are directories Arc keeps for itself at the storage root. `GET /api/v1/databases`
hid them, but the database-scoped endpoints accepted their names:
`GET /api/v1/databases/_schema/measurements` listed the anchor tree as if the real
databases were measurements. That made `_schema` look like a stray database to
delete.

They are now refused wherever a database is named:

- `GET /api/v1/databases/{name}` and `/measurements`: 404.
- `DELETE /api/v1/databases/{name}`: 403, as before.
- Creating or updating a retention policy, and `POST /api/v1/delete`: 400.

A stored retention policy on one of these roots logs a warning at startup.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1032](https://github.com/Basekick-Labs/arc/pull/1032).

### OSS Helm chart: `arc.config.enabled` no longer leaves the pod unable to start ([#1194](https://github.com/Basekick-Labs/arc/issues/1194))

With `arc.config.enabled=true`, the OSS chart mounted an `arc.toml` ConfigMap
that no template created, so the pod never started (`MountVolume.SetUp failed
for volume "config"`). `helm lint` and `helm template` both passed. The template existed
but was never committed: the `.gitignore` rule for the `arc` binary also
matched the `helm/arc/` directory, so new chart files were silently ignored.
The rule now matches only the binary at the repository root and a binary
built inside `cmd/arc`.

- **The ConfigMap renders**, and a change to `arc.config.contents` now rolls
  the pod on `helm upgrade`. The file is mounted with `subPath`, which a
  running pod never refreshes, so without the roll an upgrade kept the old
  config.
- **New `auth.bootstrapToken` values** set the initial admin token, from a
  chart-created Secret (`value`) or your own (`existingSecret`, key
  `bootstrap-token`), as the Enterprise chart already does. Arc uses it on
  first run only; once tokens exist it is ignored.
- **Both charts refuse a bootstrap token shorter than 32 characters** at
  render time. Arc rejects such a token, logs one error and starts with no
  admin token, so the install looked healthy and nobody could log in.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1198](https://github.com/Basekick-Labs/arc/pull/1198).

### A DELETE removes rows that were still buffered ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

`POST /api/v1/delete` and `DELETE /api/v1/databases/{name}` only ever looked at
Parquet files. Rows written shortly before (still in the ingest buffer, not yet
flushed) were left out, and showed up once the buffer flushed. Now both flush
the buffered rows of that measurement (or database) first and wait for them to
reach files, so every row acknowledged before the request is deleted. A dry run
does not flush. While an earlier flush failure is outstanding (its rows wait
for WAL recovery), both answer 503 with `Retry-After: 30` instead.

A database deletion also withdraws the files of that database still waiting to
be registered in the cluster manifest, so none registers after the database is
gone.

### A DELETE no longer publishes a rewrite of a file a compaction replaced ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

On a cluster, `POST /api/v1/delete` scans the files of the measurement, then
publishes a rewrite of each file without the deleted rows. A compaction of the
same hour could replace one of those files in between. The DELETE then still
registered its rewrite of the replaced file, next to the compaction's output,
which still held the deleted rows. The deleted rows came back and the others
were counted twice.

Now a DELETE's manifest update applies only while the file is still registered
with the content the DELETE read. If a compaction replaced it, the DELETE
waits until the compaction's output reaches the node (up to 30 s), scans
again, and deletes from the output. After three rounds of files
changing under it, it answers `503` and lists the files it did process; the
rows of those files are deleted. Retry to finish. `allow_unreachable: true`
also applies to the later scans; files it skipped are listed in
`unreachable_files`.

This covers a compaction that commits while the DELETE runs. The opposite
order, a compaction that read a file before the DELETE rewrote it and commits
after the DELETE, is covered by the staged compaction commit below, on local
and object storage.

A DELETE also leaves alone files in storage that the cluster manifest does not
list and this node is not registering, such as a compaction output not yet
committed. It first catches up with the cluster manifest, so a file registered
a moment ago is not missed; if it cannot, it answers `503`.

With compaction commit on, a DELETE scans the files the cluster manifest lists
for the measurement, read after it caught up with the manifest, not only the
files it found in storage. A compaction can commit between the DELETE's
storage listing and that catch-up: the listing then has the compaction's
inputs, which the manifest no longer lists, and not its output. The DELETE
used to skip the inputs, never see the output, and answer success having
deleted nothing, while the output kept the rows.

A file the manifest lists that is not on the node yet is waited for (up to
30 s per scan; the manifest is read again meanwhile, so a file it stops listing
is not waited for). A file still missing then answers `503` listing it
in `unreachable_files`; `allow_unreachable: true` runs the DELETE without it
and lists it in `unreachable_files`, including when nothing matched. A dry run
does not wait, and lists the files it could not count. This applies on
clusters without stream takeover too. Before, rewriting such a file deleted it
and left its rewrite unregistered, so it never replicated.

The cluster turns this on by itself once every node runs this release (`GET
/api/v1/cluster` shows `compaction_commit.enabled`, and on the leader the
nodes keeping it off in `blocking_nodes`). Until then DELETE behaves as
before. From then on a node running an earlier release cannot join. See the
upgrade note below about rolling a node back.

### Deleting a database also removes its parked files, and survives a leader change ([#1236](https://github.com/Basekick-Labs/arc/issues/1236))

With stream takeover on, a file whose registration the cluster refused is
parked under `_refused/<database>/`, and its WAL entries are kept until it is
resolved. `DELETE /api/v1/databases/{name}` did not see those files: they and
their WAL entries outlived the database, and a later re-flush could bring the
deleted data back. Database deletion now removes them.

A parked file can be the only copy of acknowledged rows, so it is removed only
when the cluster's record of this database deletion covers every batch in it.
Its batches are then settled as deleted and its WAL entries released. The
removal is journaled first, so a deletion interrupted by a crash finishes on its
own after the restart. If the record does not cover a file, or a registration
for it is still in flight, the request fails with a retryable 503 and the file
is kept.

On clusters without tiered storage, a leader election during the deletion used
to fail the request with a 500 after some manifest batches were applied. It is
now retried for about 7 seconds, as on the tiered-storage path. A longer
election still fails the request; retrying it completes the deletion.

Contributed by [@jallegri](https://github.com/jallegri) in [#1252](https://github.com/Basekick-Labs/arc/pull/1252).

### A node that missed a delete removes its stale copy ([#1071](https://github.com/Basekick-Labs/arc/issues/1071))

On a cluster with local storage, each node removes its own copy of a file when
the cluster manifest drops it. A node missed that when it was down while the
file was deleted and rejoined from the leader's snapshot, or when it crashed
with the removal still queued (or the removal failed). The copy stayed on its
disk, and its queries kept returning the deleted rows, or duplicates of
compacted ones, until the opt-in orphan sweep removed it. A rig reproduced it:
a reader down while a measurement was deleted kept all 6 files and returned
the 50 deleted rows after it rejoined.

Each node now records the local files it has seen registered, in
`seen-local.log` in `cluster.raft_data_dir`. Every 5 minutes it syncs with the
leader and removes a recorded file the manifest no longer lists, when:

- the node's manifest has applied past the file's registration;
- the file has been unlisted for 10 minutes (two passes that ran: a pass
  skipped for a compaction pause or a failed sync starts the wait again);
- it is not waiting to register on this node, not held by an edge-sync
  delivery, not part of a compaction in progress, and not in the cold tier on
  this node (an unreadable tier record counts as cold);
- it is still the same file (size, modification time and inode). The copy is
  moved aside under `_stale_local/` and checked again there before it is
  deleted, so a file written to the path meanwhile goes back.

A file the node never saw registered is never removed by this sweep: a writer's
flushed files not yet registered, a restore in progress, edge-sync deliveries,
and copies left over from before the upgrade (those stay for the reconciler's
orphan sweep). In the rig above, the reader removed the 6 copies within two
passes and kept the others.

A pass that would remove more than `cluster.stale_local_sweep_max_fraction`
(default `0.5`) of the node's tracked files, and at least 1000, removes
nothing: a manifest rolled back by an unsafe Raft recovery looks like that.
The node logs an error and `arc_stale_local_blocked` reads `1`; if the
deletes are real (a long outage with heavy retention), raise the setting and
restart the node. A value of 0 or below, or not a number, is refused at
startup.

The sweep is on by default (`cluster.stale_local_sweep`) on local storage with
`cluster.replication_enabled`, and stops with
`cluster.replication_catchup_enabled = false`. The tracking it removes by also
records what each local copy holds (#1182, below), so it runs with
`cluster.stale_local_sweep = false` too: that setting stops the removals only,
and the status then reads `removals_off: true`. It holds about 150–200 bytes
of memory per local file, and a journal of up to about 1.5 times that on the
Raft volume; if the journal cannot be written, the node stops tracking (and
removing) until a restart and `arc_stale_local_tracking_failed` reads `1`.
`GET /api/v1/cluster` reports the last pass under `stale_local`. New metrics:
`arc_stale_local_unlinked_total`, `arc_stale_local_tracked`,
`arc_stale_local_blocked` and `arc_stale_local_tracking_failed`. An earlier
release ignores `seen-local.log` and `_stale_local/`.

### Compaction no longer loses rows when the compactor's disk fails, or brings deleted rows back ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

On a cluster with local storage, a compaction registered its output and
deleted its inputs in two steps, after it had already removed its own copies
of the inputs. Nothing checked that the inputs were still the files it had
read. Two things went wrong:

- The other nodes removed their copies of the inputs as soon as the deletes
  applied, while the output existed only on the compactor until they pulled
  it. If the compactor's disk was lost in that window, the rows were gone on
  every node.
- A row DELETE that rewrote an input while the job ran left the job's output,
  built from the input as it was before the DELETE, registered next to the
  rewrite. The deleted rows came back and the others were doubled.

Now, once compaction commit is on (see the DELETE entry above), a compaction
on local storage is staged first:

1. The compactor writes its output under `_compaction_staged/` in its storage,
   where no query reads it, and leaves its inputs alone.
2. It stages the job in the cluster. Every other node that keeps a local copy
   of the data pulls the staged output, makes it durable and reports holding
   it.
3. Once at least one other node holds it, the compactor commits. The cluster
   registers the output and deletes the inputs in one step, only if every
   input is still registered exactly as the job read it. Each node holding the
   staged copy moves it into place, so it never pulls the output again.

If an input changed (a DELETE, retention, tiering or another compaction got
there first), the commit is refused and the job is dropped. Its inputs were
never touched, so nothing is lost, and a later cycle compacts the partition
again. A compaction pause (a cluster restore) drops jobs that have not
committed yet.

What to expect:

- A staged output takes disk space on every node that holds it until the
  commit, then replaces the inputs.
- If no other node that keeps local copies is up, a staged job waits for one
  and is dropped after 10 minutes (longer for a large output), then
  recompacted on a later cycle. Compaction makes no progress while the
  compactor is the only node holding data.
- A node that missed a commit moves its staged copy into place when it
  catches up; one that has no copy pulls the output as before.
- A job staged by a compactor that crashed is aborted when the compactor
  restarts, or by the leader 10 minutes after the compactor stopped
  answering.

The second problem existed on object storage (S3, Azure) too, where a
compaction is now staged as well:

1. The compactor uploads its output under `_compaction_staged/` in the bucket,
   recording its checksum on the object, and leaves its inputs alone.
2. It stages and commits the job at once (there is no copy to wait for on
   other nodes: the bucket is shared). The commit registers the output and
   unregisters the inputs only if every input is still registered exactly as
   the job read it.
3. A finisher copies the staged object to the output's path inside the store
   (S3 CopyObject, or a multipart copy above 5 GiB; Azure Put Blob From URL,
   or blocks from URL above 5000 MiB), deletes the input objects, and ends
   the job. The output appears at its path only once complete. If Azure
   refuses the server-side copy (some Entra or managed-identity setups), the
   finisher copies the bytes through the node instead.

The job's compactor runs the finisher at once; if it is gone, the leader takes
the job over after 5 minutes. One node at a time finishes a job (a lease in
the cluster state).

Between the commit and the copy, reads see the inputs' rows (the input objects
are still there), and a DELETE on that measurement waits for the output, up to
30 s per scan, then answers `503`; retry it. A DELETE that rewrites or deletes
the output removes the job's input objects at once. Between the copy and the
input deletes, reads see both, for a moment, as before this change.

If the output leaves the cluster manifest before the finisher runs:

- removed by a DELETE, retention, a database deletion, tiering or a restore:
  its rows were dealt with, and the input objects are deleted too;
- removed by anything else (the reconciler's orphan sweep): the inputs are
  registered again, if all their objects are there;
- otherwise the job is marked wedged: `GET /api/v1/cluster` lists it under
  `compaction_commit.wedged_jobs`, the cluster logs an error, and nothing is
  deleted. Check the data, then end it with
  `POST /api/v1/cluster/compaction-jobs/{job}/abort` (admin).

A compaction pause (a cluster restore) waits for committed jobs to finish
before it takes effect; jobs not yet committed are dropped. A backup taken
while a job finishes copies its output from the staged object, and leaves the
input objects out. Tiering does not move a file a compaction owns. The leader
removes staged objects no job owns after 6 hours; on S3, add a lifecycle rule
that aborts incomplete multipart uploads, as for any bucket Arc writes to.

New metrics: `arc_compaction_finished_total` (committed jobs their finisher
ended), `arc_compaction_wedged_total` (jobs marked wedged) and
`arc_compaction_copy_fallback_total` (copies Azure refused to do server-side).
`GET /api/v1/cluster` reports `compaction_commit.finalizing` and
`compaction_commit.wedged_jobs`.

On a shared bucket, compaction now also keeps the cluster manifest. Until now,
a cluster on S3 or Azure with `cluster.replication_enabled = false` (as the
Helm chart and the docs set it for shared storage) never ran the compaction
completion watcher. So compaction never updated the cluster manifest there:

- its outputs stayed unregistered;
- the inputs it consumed stayed listed after their objects were deleted.

Queries were not affected, since they read the bucket. Every cluster node now
runs the watcher, whatever its storage.

On every cluster, compaction used to register its outputs with the compaction
tier (`hourly`, `daily`) as their tier, which the staged compaction commit
does not take as inputs. They are now registered `hot`. The leader's manifest
repair (below) moves older entries to `hot`, each only if it has not changed
since it was read, and a DELETE scans them as hot files meanwhile.

To bring an upgraded cluster's manifest back in line with its bucket, the
leader repairs it. The first run comes 2 minutes after a node becomes leader,
then the repair runs hourly (10 minutes after a run that stopped early). It
waits while a compaction pause is in force. On a shared bucket, with stream
claims off, each run also does two things:

- It drops a hot entry whose object is gone, unless a finalizing compaction is
  still copying that object into place.
- It registers a compaction output the manifest does not list, once the output
  is over an hour old, unless tiering records it as cold. A compaction output
  is a `*_compacted.parquet` file in an hour directory or a `*_daily.parquet`
  file in a day directory. Each one is read for its checksum, at most 1000
  objects or 10 GiB per run; the rest waits for the next run. On a large
  bucket, the first repairs after an upgrade can take a while. While outputs
  of a measurement wait, its stale entries are kept.

Other objects the manifest does not list are left to the reconciler. These
are leftovers of a delete that failed after its manifest change. The repair
registers compaction outputs well before the reconciler's 24-hour orphan
grace, so the reconciler no longer counts them as orphans.

A DELETE on a shared bucket scans what queries read, so it works before the
repair has run:

- A listed file whose object is gone is skipped, not waited for.
- An object the manifest does not list is scanned too. Its rewrite is
  registered (an unregistered rewrite would be an orphan the reconciler
  removes); when every row matches, it is removed. With stream claims on, such
  an object is left alone, as before.
- The input objects of a committed job that has not finished yet are skipped,
  because its output holds their rows.

As on replication clusters, a node that lost the compactor role while it
still had unprocessed completion manifests keeps a cluster-wide compaction
pause (a restore) from taking effect until it compacts again or restarts.

New metrics: `arc_manifest_repair_dropped_total`,
`arc_manifest_repair_adopted_total` and the gauge
`arc_manifest_repair_pending`. On the leader, `GET /api/v1/cluster` reports
the last repair under `compaction_commit.manifest_repair` (`dropped`,
`adopted`, `retiered`, `pending`, `error`).

### Local `ReadToAt` no longer returns staged partials ([#1199](https://github.com/Basekick-Labs/arc/issues/1199))

No operator-visible change. On local storage, a ranged read of a file (the
peer file fetch that replication uses, and edge sync resuming a transfer) could
return the bytes of an interrupted transfer when the committed file was
missing. It now reads committed files only, like the size and existence checks;
a missing file is reported as not found. Resuming an interrupted replication
pull still works: it reads the partial through the staging interface.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1203](https://github.com/Basekick-Labs/arc/pull/1203).

### Delete API catches dangerous keywords glued to numbers or literals ([#1080](https://github.com/Basekick-Labs/arc/issues/1080))

The delete endpoint's keyword scan used `\b` directly on the masked WHERE
clause. Digits and the masker's identifier-shaped placeholders can prevent a
word boundary, allowing inputs such as `1=1UNION ...` or `host='a'UNION ...`
to pass this pre-filter. The validator now separates token-start keywords
from numbers and removes literal placeholders before scanning, matching the
query endpoint's existing handling.

Both endpoints now split off a number the way DuckDB reads it, as one token:
with `_` separators, a fraction (`1.`, `.5`) and an exponent with or without a
sign. Before, only a plain digit run was split, so `1e0UNION`, `1.5e3UNION`,
`1.e1UNION` or `1_0UNION` still hid the keyword from the delete endpoint and
from the measurement query endpoint's WHERE check, while DuckDB ran them as a
`UNION`.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1081](https://github.com/Basekick-Labs/arc/pull/1081).

### A storage error that only mentions "404" no longer reads as a missing object ([#1238](https://github.com/Basekick-Labs/arc/issues/1238))

Arc decided whether an S3 or Azure object was missing by looking for `404`,
`NotFound` or `NoSuchKey` anywhere in the error's text. That text carries the
request URL (endpoint, bucket, key and, for a backup, the backup id), the
storage service's request id and, for a connection failure, the local port. So
an outage, a throttled request or a refused connection could read as "the
object is not there" whenever one of those happened to contain the digits.
Arc's file names carry nanosecond timestamps and backup ids carry the time
they were taken, so this was not rare: an unreachable backup target answered
`GET /api/v1/backup/{id}` with 404 `Backup not found` for every backup taken
at a minute such as 14:04, and `POST /api/v1/backup/restore` started a restore
with a guessed mode (202) instead of refusing. File-existence checks answered
"absent" in the same cases. Tiering, edge sync, compaction, backup restore and
the cluster's file replication all branch on those checks.

A missing object is now recognised only from the storage service's own
answer: `NoSuchKey` on an S3 read, the bodiless 404 of an S3 `HEAD`, and
`BlobNotFound` on Azure. The backup API answers an unreachable target with 503
naming the target, as it already did when the error text happened to contain
no `404`.

Two narrower changes come with it:

- **A missing bucket or container is not a missing object.** Reading an object
  from an S3 bucket that does not exist (`NoSuchBucket`), or from an Azure
  container that does not exist, is now an error that says the store is
  missing, instead of "object not found". The backup API answers 503 for a
  target whose bucket does not exist, where it answered 404 or started a
  restore. Checking whether a file exists in a missing bucket or container is
  an error too ([#1242](https://github.com/Basekick-Labs/arc/issues/1242)): an
  S3 `HEAD` cannot tell a missing bucket from a missing key, so after a `HEAD`
  that finds no object Arc asks S3 whether the bucket exists, at most once a
  minute while it does. An answer other than "no such bucket", including a 403
  for credentials that may not list the bucket, keeps the object absent, as
  before. The places that run before a store's first write treat a missing
  store as empty, because most S3-compatible stores create the bucket on that
  write: a backup to a new target, a restore into a new cold bucket, an edge
  hub receiving its first file, and `GET` or `POST /api/v1/databases` on a
  fresh deployment. Creating a database on a store whose bucket did not exist
  yet answered 500 before and works now.
- **A 404 without a storage error code is an error.** A proxy or gateway
  answering 404 with its own page, rather than the storage service's error, is
  no longer taken to mean the object is missing. An S3-compatible store that
  answers a read of a missing key without the `NoSuchKey` code would now
  surface an error; AWS S3 and SeaweedFS both send it.

A local backend read of a missing file (`ReadTo`, ranged and staged reads) is
now recognised as missing on every path, not only when the path contained
`404`.

### Updating a continuous query or retention policy no longer resets the fields you leave out ([#1023](https://github.com/Basekick-Labs/arc/issues/1023))

`PUT /api/v1/continuous_queries/:id` and `PUT /api/v1/retention/:id` wrote
every column from the request body. Any field the body left out was stored as
its zero value, and the call still returned `200`:

- **A continuous query stopped producing.** Leaving out `is_active` stored
  `false`, and the scheduler stopped the job and did not restart it. The only
  signals were `is_active: false` in the response and a debug log line.
  Leaving out `tag_columns` removed the dedup key compaction uses for the
  query's output ([#521](https://github.com/Basekick-Labs/arc/issues/521)).
- **A retention policy could delete more than it was set to.** Leaving out
  `measurement` turned a policy pinned to one measurement into one that
  deletes across the whole database. Leaving out `buffer_days` brought the
  cutoff forward by the stored buffer, so data that many days younger was
  deleted. Leaving out `is_active` paused the policy.

**Both endpoints now take a partial update.** A field the body leaves out keeps
its stored value. A field the body carries is checked with the same rules
create applies, then written. So `{"is_active": false}` on its own pauses
either kind of object. That is the request the Launchpad and Arc Cloud
retention pause buttons and the Python client's update methods already send:
it returned `400` before, and now works. A body with no fields at all is refused with `400`.

`null` clears a field where an empty value means something:

| Endpoint | Fields `null` clears | `null` refused (`400`) |
|---|---|---|
| Continuous query | `description`, `tag_columns`, `retention_days`, `delete_source_after_days` | `name`, `database`, `source_measurement`, `destination_measurement`, `query`, `interval`, `is_active` |
| Retention policy | `measurement` (applies the policy to the whole database) | `name`, `database`, `retention_days`, `buffer_days`, `is_active` |

`retention_days` must still be greater than `buffer_days`. When the body
carries only one of the two, the other is the stored value, and the check runs
in the same statement as the write.

**What changes for API clients:**

- **To clear a field, send `null`. Leaving the field out no longer clears
  it.** This matters for an edit form that drops empty fields from the body:
  clearing a continuous query's `description`, `retention_days` or
  `delete_source_after_days`, or setting a pinned retention policy back to
  "all measurements", is now ignored instead of applied. For a retention
  policy, that means it keeps deleting only the measurement it was pinned to.
- **Creating a continuous query without `is_active` now creates an active
  one.** The column has always defaulted to active, but create stored `false`
  instead, so such a query never ran. Send `"is_active": false` to create one
  paused. Retention policies keep their create defaults on purpose
  (`is_active` false, `buffer_days` 0): turning `is_active` on by default would
  make policies that existing clients create start deleting data.
- **A negative `buffer_days` is refused** on create and update. It moved the
  cutoff the other way, deleting data younger than `retention_days`.
- **A duplicate retention-policy name on update is now a `400`,** as on create,
  instead of a `500`.
- **An empty `measurement` on a retention policy is stored as `null`.** Both
  have always meant the whole database. `GET` now returns `null` for both
  instead of returning whichever one was sent.

This replaces the 26.09.3 advice to send the whole definition on every `PUT`.
Clients that do so keep working unchanged.

Leaving `tag_columns` out of an update that changes the query keeps the stored
list. That can no longer cost rows: since 26.09.4 compaction removes only rows
identical in every column ([#1232](https://github.com/Basekick-Labs/arc/issues/1232)).

### The WAL is reclaimed again after a crash recovery ([#1222](https://github.com/Basekick-Labs/arc/issues/1222))

After a restart that replayed WAL entries from the previous run, the periodic
WAL purge stopped deleting files for the rest of the process's life. The WAL
directory then grew with ingest until a clean shutdown. A node that crashed
once and kept running for days kept every WAL file it wrote, until disk
pressure. One object-store outage could do the same with no crash at all: the
recovery that runs after a failed flush replays a large entry in row ranges.

The cause was a safeguard that never lifted. A replayed entry's flush records
a checkpoint for the earlier run's entry, and that checkpoint can be the only
proof that the entry was flushed. The purge therefore never deleted the file
holding it, nor any file after it. The proof matters only while a WAL file
from an earlier run still exists. Recovery deletes that file as soon as its
entries are durable, so the safeguard was holding back files for no reason.

The purge now keeps such a file only while a WAL file this process did not
write is still in the directory. If the directory cannot be listed, it keeps
it. While it waits, it logs the file it is waiting on:

```
WAL purge stopped at a file holding flush proof for an earlier process WAL file; it resumes once recovery has removed that file
```

Nothing to configure. A WAL directory that grew under this bug is reclaimed
when the node restarts on this release: startup recovery deletes the files
whose entries are already flushed.

### Rows written without a time column keep their ingest time after a WAL replay ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

A columnar MessagePack write without a `time` column gets the time it was
ingested. The WAL kept the payload as the client sent it, without that time.
A replay of the entry after a crash, or after a failed flush, generated the
time again, as the replay time. The rows landed with a later timestamp than
the one they were acknowledged with, possibly in a later hour partition.

The WAL entry of such a write now holds the generated time. These writes no
longer take the typed fast decode path, which cannot keep the time in the WAL.
Writes that carry a `time` column are unchanged.

### A write with a numeric measurement no longer poisons its WAL file ([#1225](https://github.com/Basekick-Labs/arc/issues/1225))

A columnar MessagePack write may name its measurement with an integer
(`"m": 5`), and ingest stores it as `measurement_5`. The WAL keeps the payload
as the client sent it, and WAL recovery accepted only a string. Recovery
therefore failed the whole WAL file holding such a write, including entries
that were already flushed, and replayed no later files in that pass. After
three failed passes it quarantined the file as `.wal.failed`, and every
unflushed entry in it was no longer replayed.

Recovery now names an integer measurement exactly as ingest does. A WAL file
that failed for this reason and is still in the WAL directory replays on the
next pass. One already quarantined stays quarantined. Do not simply rename it
back to `.wal`: recovery would replay every entry in it, including ones
already stored, and duplicate them. Follow the quarantine procedure in
`docs/operations/wal-recovery.md`.

More generally, an intact WAL entry whose payload cannot be decoded no longer
fails its file when its flush checkpoint shows it is already in storage.

### A quarantined WAL file no longer stops the WAL purge until restart ([#1225](https://github.com/Basekick-Labs/arc/issues/1225))

The recovery that runs after a failed flush can quarantine a WAL file this
process wrote. The writer was not told, and kept waiting for that file's
unflushed entries. From then on the periodic WAL purge deleted no WAL file
until the process restarted, and the WAL directory grew with ingest.

When recovery quarantines a file, the writer now stops waiting for the
entries in it and the purge continues. It logs:

```
Released the pending WAL entries of a quarantined WAL file so the WAL purge can continue; keep the quarantined file, it holds those entries and may hold flush proof for other WAL files
```

New metric: `arc_wal_quarantine_released_entries_total`. Keep `.wal.failed`
files until you have dealt with them: they hold the entries that could not be
replayed. Renaming one back to `.wal` now duplicates more readily than before:
the purge may already have deleted the later WAL file that proved its other
entries were stored.

### Two WAL rotations in the same clock tick no longer share a file

WAL files are named by the time they are created. Two rotations within one
clock tick (one microsecond on macOS) produced the same name, and the second
appended to the first file instead of starting a new one. A rotation now
refuses to open an existing file and moves the name one microsecond later, so
names still sort in rotation order.

### A forwarded file registration must name the forwarding node as its origin ([#1207](https://github.com/Basekick-Labs/arc/issues/1207))

A cluster node that is not the Raft leader forwards its file registrations to
the leader. The leader checked that the forwarding node was a known member
allowed to register files, but not that each file named that node as its
origin. Any authenticated member could register a file as if another node had
written it. Peers pull a file from its origin, and the upcoming standby-writer
takeover decides which writer's files to accept by origin.

The leader now refuses a forwarded registration whose origin is not the
forwarding node. Every node in this release stamps its own id, so nothing
legitimate is refused.

### Every file a cluster writer flushes reaches the cluster manifest ([#1206](https://github.com/Basekick-Labs/arc/issues/1206), [#1209](https://github.com/Basekick-Labs/arc/issues/1209))

Readers and the other writers learn about a writer's Parquet files from the
cluster manifest, and a file that never reaches it can be queried on its writer
only. Several paths lost files that way, with nothing to re-register them:

- **Startup.** The manifest registrar was attached only after the cluster
  coordinator had started. Files flushed before that, above all by startup WAL
  recovery after a crash, which then deletes the WAL files, were never
  announced. MQTT ingest, which starts first, was affected too. With auth
  enabled the window includes waiting up to 30 s for a leader.
- **Steady state.** The registrar held a 4096-entry in-memory queue, dropped
  files when it was full, gave up on a file after one failed attempt (for
  example while the cluster had no quorum), and kept nothing on disk. A crash
  between a flush and its registration also lost the file, because the flush
  checkpoints the WAL right after registering.

With `reconciliation.enabled`, the orphan-storage sweep could then delete such
a file after the grace window, and on a writer it was the only copy.

A node that ingests (a `writer`, or `standalone` with `cluster.enabled`) now
records every file it flushes in `pending-registrations.jsonl` in
`cluster.raft_data_dir`. With the WAL enabled, each flush makes those records
durable before it checkpoints the WAL, with one fsync per flush; without the WAL
there is no checkpoint to order and no fsync is added. The node registers the files once
the cluster is up, in batches, retrying every failure until the manifest
confirms them. A file that is gone or has changed size by then is skipped. The
record is removed once the manifest confirms the file, and the journal is
emptied whenever nothing is pending, so it stays near zero bytes in normal
operation.

While the manifest cannot be reached, each flushed file adds about 300 bytes to
the journal on the Raft data volume and about 500 bytes of memory, so a
million-file backlog is about 0.3 GB on disk and 0.5 GB in memory. New metrics:
`arc_cluster_file_registrations_pending`,
`arc_cluster_file_registration_oldest_pending_seconds`,
`arc_cluster_file_registrations_confirmed_total` and
`arc_cluster_file_registration_journal_errors_total`. Once the oldest pending
file has waited a minute, the node logs at Error every five minutes. Do not
delete the journal by hand while it holds records. Records in it are tied to
`cluster.cluster_name`, and a node started under a different cluster name drops
them with a warning. Readers and compactors do not keep one: nothing they flush
should be announced.

Files still waiting for registration are also protected from the paths that
treat "not in the manifest" as safe to act on
([#1210](https://github.com/Basekick-Labs/arc/issues/1210),
[#1212](https://github.com/Basekick-Labs/arc/issues/1212)):

- A row DELETE that rewrites such a file announces the rewrite in its place,
  and one that removes every row of it withdraws it. Before, the rows that
  survived a DELETE could stay on that writer only.
- Compaction leaves such files out of a cycle until they are registered, so it
  cannot race their registration and leave a manifest entry for a file it is
  about to delete. A partition left below the tier's minimum file count waits
  for the next cycle.
- The reconciliation orphan-storage sweep skips them, and files confirmed in
  the last minute, and reports how many in the run's `skipped_pending`. Before,
  a backlog older than the grace window was deleted as orphans.

These checks see only the node's own pending files, so they protect a writer
from its own DELETEs, compaction and reconciliation. With shared storage they do
not cover another node acting on a writer's pending files: a DELETE run by the
leader writer, or compaction and reconciliation on the active compactor
([#1214](https://github.com/Basekick-Labs/arc/issues/1214)). Files already left
unregistered by earlier versions are not picked up retroactively.

### Legacy standalone restores preserve unverifiable cold objects ([#1189](https://github.com/Basekick-Labs/arc/issues/1189))

Standalone tier scans rebuild cold metadata from the cold listing. When a
pre-stage-C backup has no per-file sidecar row for an object already held in
cold storage, restore leaves that object untouched and reports
`cold_rows_skipped_unverifiable` instead of replacing the only cold copy with
bytes it cannot verify.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1205](https://github.com/Basekick-Labs/arc/pull/1205).

### The cold-tier storage backend closes during shutdown ([#1103](https://github.com/Basekick-Labs/arc/issues/1103))

The S3 or Azure backend used by tiering is now closed with the other storage
backends after the tiering manager stops.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1204](https://github.com/Basekick-Labs/arc/pull/1204).

### Delete API removes the inactive SQL Server prefix scan ([#1077](https://github.com/Basekick-Labs/arc/issues/1077))

The WHERE validator no longer lists or checks `xp_` and `sp_`. The lowercase
patterns were compared against an uppercase clause and never rejected a
request. Removing this inactive SQL Server-specific check preserves existing
validation behavior; keyword, punctuation and file-access checks remain.

Original fix proposed by [@abhicodes-007](https://github.com/abhicodes-007) in [#1078](https://github.com/Basekick-Labs/arc/pull/1078).

### Hot-only databases keep their hot metadata when a cold object is listed ([#1187](https://github.com/Basekick-Labs/arc/issues/1187))

For a database pinned to the hot tier, cold metadata sync no longer records a
cold object while this node still holds its hot copy (or cannot tell). A cold
object left behind by a failed migration can no longer make the orphan sweep
remove the hot copy. A cold object with no local hot copy, such as one
migrated before the pin, is still recorded, so queries keep reading it.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1192](https://github.com/Basekick-Labs/arc/pull/1192).

### Cold-tier metadata is rebuilt on standalone nodes ([#1179](https://github.com/Basekick-Labs/arc/issues/1179))

A node with a usable cold tier now reads the cold listing at the start of every
tier scan, whatever its cluster role. Previously that step was skipped unless a
cluster gate was wired, which meant it never ran on a standalone node.

In steady state this changes nothing: a standalone node writes its own cold rows
as the second phase of its own migration, so the listing finds nothing new. The
case it exists for is **recovery**, where it is the only mechanism — nothing else
re-derives a cold row from the bucket:

- a metadata restore that predates the migrations, so the tier rows and the
  bucket disagree;
- SQLite loss or corruption, where the cold objects are intact but no row claims
  them — and the query layer omits the cold glob entirely for a measurement with
  no cold row, so the data reads as absent rather than mis-tiered;
- moving a cold bucket to a new standalone node, which until now needed
  hand-written SQL.

The listing is bounded by `tiered_storage.scan_timeout`, the same budget the hot
scan uses.

**If the cold listing fails, the migration cycle now skips migration and orphan
reconciliation** and waits for a cycle whose listing succeeds, rather than acting
on a view of the tiers it knows is incomplete. That already applied on clustered
nodes; it now applies everywhere. See the upgrade note above, because it is
reachable on deployments that have been running happily without it.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1183](https://github.com/Basekick-Labs/arc/pull/1183).

### Local `StatFile` ignores staged partial writes ([#1178](https://github.com/Basekick-Labs/arc/issues/1178))

`LocalBackend.StatFile` now reports only committed files. In-progress writes
remain available through `StagingInspector`, so resumable transfers can inspect
partial data without callers mistaking it for a complete object.

`StatFile` is documented as returning `-1` for an object that does not exist,
and S3 and Azure always did. The local backend returned the length of the
`.part` staging file an interrupted transfer leaves behind, so a half-arrived
file read as a complete one — on the backend single-node installs and tests
actually use.

It reads as an internal contract fix, but four things a local-storage operator
can see change with it, all in the safe direction:

- a replicated file still only partly arrived no longer gets a tier row
  claiming the node holds it;
- a tier row for a file that is only a partial is now retired by the next
  scan instead of being kept indefinitely;
- `DROP DATABASE` no longer reports a spurious "hot object remains after
  database listing" on its first attempt when a partial is still on disk;
- a backup counts such a path as absent — and says so in its incomplete
  sample — instead of recording a partial's size and then failing to copy it.

Ranged reads (`ReadToAt`) now read committed files only as well
([#1199](https://github.com/Basekick-Labs/arc/issues/1199)), so every local
read and size answers about the same committed file.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1190](https://github.com/Basekick-Labs/arc/pull/1190).

### Telemetry reports the machine's memory, and what the process may use ([#1412](https://github.com/Basekick-Labs/arc/issues/1412))

Telemetry's `memory.total_gb` was documented as the machine's RAM but was
`runtime.MemStats.Sys`: the memory the Arc process itself had obtained from the
OS. A freshly started node reported a few tens of MB whatever the host had, so
every earlier `total_gb` value is the process footprint, not a machine size.

- `total_gb` is now the machine's memory: `/proc/meminfo` `MemTotal` on Linux,
  `hw.memsize` on macOS. The unit is GiB, as before.
- A new `usable_gb` is what a cgroup limit leaves the process, the memory
  counterpart of `usable_cores`. It is the same detection the ingest buffer
  sizes itself from: the lower of `memory.max` and `memory.high`, so with the
  Kubernetes MemoryQoS feature gate enabled a 2Gi pod reports about 1.8. With
  no limit it equals `total_gb`.

Inside a container `total_gb` is the host's memory, unless lxcfs is mounted
over `/proc/meminfo`, in which case it reads as the container limit. Both values
are read once at startup, like `usable_cores`, so a limit changed in place is
reported from the next restart. A value that cannot be read is sent as `null`,
never a guess.

### Reported core counts account for the CPU quota ([#1039](https://github.com/Basekick-Labs/arc/issues/1039))

`runtime.NumCPU()` reflects a cpuset but not a CFS quota, and Kubernetes
`limits.cpu` and `docker --cpus` are quotas — so a pod limited to 2 CPUs on a
64-core node reported **64** both to the license activation server and to fleet
telemetry. Neither number told anyone the node could only use two.

Both reports now carry the usable count as well as the machine count:

- **License activation** sends a new `usable_cores` field beside the existing
  `cores`. `cores` keeps reporting the machine's logical CPUs, so the admin
  console's "Cores" column means what it always meant.
- **Telemetry** sends a new `usable_cores` field beside `physical_cores` and
  `logical_cores`, which also keep reporting the machine. No existing field
  changes, so no fleet series steps down on upgrade.

Three things worth knowing, because they are the questions this invites:

**Machine-bound licenses keep verifying.** The machine fingerprint still reads
`runtime.NumCPU()`, deliberately and separately: that value is part of what a
bound license is checked against, so changing it would invalidate every bound
license in the field. It is unchanged, and a test now fails if anyone sweeps it.

**Neither number is an entitlement input.** The activation server accepts an
activation whatever the core count and leaves the limit to the client, which
enforces it at startup across GOMAXPROCS, DuckDB threads and flush workers. The
reported values are for reporting.

**The reported usable count is taken at process start.** A license enforces
itself by pinning GOMAXPROCS down to its core limit, so a value read later
would be `min(quota, licensed_cores)` — on a 64-core machine with a 32-core
license, a re-activation or a telemetry tick would have reported 32 as the
node's size, on hardware with no CPU quota at all. Taking the reading before
anything can pin it keeps one process's reported size stable for its whole life
and keeps the field's subject the CPU quota rather than the license.

One consequence of that choice, stated rather than left to be discovered: a CPU
limit changed **in place** without a restart — an in-place pod resize, `docker
update --cpus` — is not picked up. The Go runtime notices within about a second,
but the reported value does not, so it stays at whatever the limit was at boot
until the process restarts. That is the accepted side of the trade: reading live
instead would be wrong on every licensed node, permanently, rather than on a
resized one until its next restart.

Cluster join requests are unchanged. The core count there is an enforcement
input checked against the license's limit, is covered by the join MAC, and is
read after the limit is applied by design.


### Cancel queries when the client disconnects ([#979](https://github.com/Basekick-Labs/arc/issues/979))

Queries now stop when Arc detects that the client connection has closed before
the response completes. This follows Go's `net/http` request-context semantics:
a client that half-closes its write side after sending a request may be treated
as disconnected even if it still intends to read the response. On upgrade, this
can make a query that previously completed appear as cancelled with
`client disconnected` in query history. Set
`query.cancel_on_client_disconnect = false` to disable cancellation for clients
that use that half-close pattern. If a pipelined request is already pending,
Arc leaves its bytes for the HTTP parser and stops monitoring that connection
for the query, so a later disconnect on the same connection is not observed.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#982](https://github.com/Basekick-Labs/arc/pull/982).

### Iceberg orphan sweep re-checks the table before deleting ([#1125](https://github.com/Basekick-Labs/arc/issues/1125))

The orphan metadata sweep (#835) decided which manifest files no metadata
version references from one listing, then deleted them. A reference that
appeared in between was missed: a commit by another writer, a restore, or a
retained `v<N>.metadata.json` rewritten in place. The sweep could then delete a
manifest that had just become reachable. Arc's own reconciler does not race
itself here, so the window was open only to another process or a restore
writing the same warehouse.

The sweep now reloads the catalog and re-lists the metadata directory after the
first listing. It works out reachability from that final listing, checks the
catalog once more, and only then deletes. It skips the pass, deleting nothing,
if the catalog moved, a check fails, or the listing has no metadata files.
Files that appear after the first listing are never deleted in that pass. A
commit landing during the deletes themselves is still not covered.

Contributed by [@jallegri](https://github.com/jallegri) in [#1174](https://github.com/Basekick-Labs/arc/pull/1174), after the guard in [@efegokdemir](https://github.com/efegokdemir)'s [#923](https://github.com/Basekick-Labs/arc/pull/923).

### Compaction reports how many jobs are in flight ([#1168](https://github.com/Basekick-Labs/arc/issues/1168))

Two endpoints advertised a count of in-flight compaction work and neither ever
produced one. `GET /api/v1/compaction/status` emitted
`"manager": {"active_jobs": null}`, and `GET /api/v1/compaction/jobs` answered
`"active_jobs": 0` **unconditionally** — including in the middle of a cycle
with work running — because it read a key the manager never set and fell back
to zero when the read failed. Nothing tracked the number: compaction jobs run
in a subprocess, and the count was never added when that design landed.

It is tracked now. An attempt registers as in flight once it holds its
partition lock and deregisters when it returns, so all three surfaces report a
real number: `/status` (`manager.active_jobs`), `/jobs` (`active_jobs`), and
`/stats`, which gains the same `active_jobs` key.

An attempt stays counted until it returns, which is *after* the compaction
subprocess has exited and the parent has finished marking receipts, deleting
the manifest entry and invalidating caches. For that short tail the attempt is
counted in both `active_jobs` and `total_completed`, so on a busy node those
two will not always look disjoint.

**The unit is attempts, not batches.** This is the same unit as the records in
`/api/v1/compaction/history` and as `total_jobs_completed` / `total_jobs_failed`
beside it, and `/jobs` now says so explicitly with `"unit": "attempts"`. A
batch too large to compact in one pass is split by the adaptive splitter and
retried at successively smaller sizes, so one batch can account for several
attempts in sequence. If you are reconciling this against a cycle's
`discovered_batches` or `failed_batches` from `/api/v1/compaction/cycles/{id}`,
those are batches and will not match attempt counts.

`/jobs` still returns an empty `"jobs"` array. Only the count is tracked, not
the identity of each in-flight attempt; the array is there for compatibility
and listing which partitions are being compacted remains future work.

A partition skipped because another attempt already holds its lock does no work
and does not register as in flight, matching how it is already excluded from
the `total_jobs_*` totals.

If you use `arcli compaction status`, the `active` column stops showing `-` and
starts showing the count, with no client upgrade required.

### Compaction requests are audited per method instead of all as triggers ([#1168](https://github.com/Basekick-Labs/arc/issues/1168))

With audit logging enabled, every request under `/api/v1/compaction` was
recorded with the action `compaction.triggered` regardless of method — so an
auditor filtering for who triggered a compaction also got everyone who read the
status page, the candidate list or the cycle history. The sibling prefixes
already did this correctly (`mqtt.put`, `tiering.put`).

`POST /api/v1/compaction/trigger` keeps the action `compaction.triggered`, so
existing filters and alerts on the one action that mutates anything are
unaffected. The seven read routes now carry their own method:
`compaction.get`, and `compaction.head` for the HEAD requests that Fiber
registers alongside every GET.

**Part of this is visible without changing any audit setting.**
`audit_log.include_reads` (default `false`) suppresses GET requests but does
not suppress HEAD, and Arc registers HEAD alongside every GET route — so a
`HEAD` against a compaction read route was already being recorded as
`compaction.triggered` and will now be recorded as `compaction.head`. That
path needs a caller who clears the admin check (the compaction routes are
admin-only, and a request without admin rights is audited as `auth.failed`
(403) instead; one without a valid token is refused by authentication before
it reaches the audit log and is not recorded), so in practice it means an
admin-token monitoring probe, or a deployment running with
`auth.enabled = false`.

That HEAD requests bypassed `include_reads` at all is closed in this release
by #1630 (see "token, team and role changes are recorded in the audit log"):
HEAD and OPTIONS are now reads like GET, so at the default setting a HEAD
probe of a compaction route is not recorded; with `include_reads = true` it is
recorded as `compaction.head`.

With `include_reads = true` the seven GET routes change from
`compaction.triggered` to `compaction.get`. If you have a dashboard or alert
that counts `compaction.triggered` events, it was over-counting and the number
will drop.

A `POST` to a compaction *read* route — which Arc answers with 405 — was also
recorded as `compaction.triggered`, and is now `compaction.post`. Only
`POST /api/v1/compaction/trigger` itself carries the trigger action.

The prefix is also matched at a path-segment boundary now, so a request to an
unrelated path that merely begins with the same characters — `/api/v1/compactionfoo`
— is no longer recorded as a compaction action. The `mqtt` and `tiering`
prefixes still match mid-segment; that is unchanged here.

Audit logging remains an enterprise feature: it is mounted only when
`audit_log.enabled` is true and the license includes `audit_logging`, so
nothing in this fix is reachable on OSS.

### The compaction trigger now claims its cycle before it answers

`POST /api/v1/compaction/trigger` decided whether a cycle was already running
by reading a flag, then left the actual claim — the compare-and-swap that makes
a cycle exclusive — to the background goroutine it started afterwards. Two
things followed.

**Concurrent triggers were accepted when only one could run.** The window
between the read and the claim is not narrow: the goroutine is scheduled
lazily, so a handler routinely finished its read *and sent its response* before
any goroutine claimed. Measured against 32 simultaneous triggers on the old
code, a substantial fraction were accepted — between 3 and 16, varying by
machine and scheduler. Each loser's cycle never ran, and while the server did
log the loss, the caller had been told the trigger succeeded and was handed a
cycle id belonging to the winner.

**The reported cycle id was a guess.** The response carried "current id plus
one", computed in the handler, while the id was really assigned later inside
the cycle. With two drivers, both were told the same id and only one cycle
existed, so the loser matched that id against `last_cycle` in
`/api/v1/compaction/stats` and read the winner's counters as its own. Since
`last_cycle` is a single global record, matching on `cycle_id` is the only way
an operator can tell whose result they are reading — and it was exactly the
mechanism that was unsound.

The handler now takes the claim synchronously and reports the id that claim
assigned. Taking the claim and publishing its id are one step, so a refusal can
also name the running cycle without ever reporting a previous, finished one.

**Behaviour change:** a second concurrent trigger now gets **409** immediately,
where it previously got a 200 whose cycle was silently dropped. If you drive
compaction from more than one place — one process per database, for example —
you will start seeing those 409s. They are the honest answer to what was
already happening; the cycle was never going to run. The 409 carries
`cycle_id`, `is_running`, and is now logged, so a refused trigger leaves a
trace on the server.

Scheduled compaction behaves exactly as before: the cron path and the internal
trigger claim and release the cycle in one call, with the same refusal logging
and the same return values on a lost claim.

### One configurable budget for the tier scan, and a truncated scan you can see ([#1154](https://github.com/Basekick-Labs/arc/issues/1154))

The tier scan runs on four paths, and until now they carried two different
budgets. A scheduled migration cycle gave the whole cycle 2 hours. The scan at
startup and `POST /api/v1/tiering/scan` were each capped at **30 minutes**, and
neither was configurable. The short cap was on both paths an operator reaches
for, so a warning that the startup scan did not finish pointed at the scheduled
cycle while the manual endpoint carried the same cap that had just failed.

`tiered_storage.scan_timeout` now bounds the scan on every path, defaulting to
`2h` so all four agree. It is a Go duration; zero, negative and malformed values
are refused at startup.

Inside a migration cycle the scan runs on a budget **derived from** the cycle's,
so it gets whichever is shorter. At the default the two are equal — the cycle
budget is also 2 hours — so the derivation changes nothing there. It matters once
the key is set *below* 2 hours: the scan then gives up on its own budget and
migration, the step that actually moves files, keeps the remainder of the cycle.
Setting it above 2 hours is harmless, because the cycle still bounds the scan.
The cycle budget itself is unchanged.

**Why the scan finishing matters.** The scan is the only thing that writes a
tier row for a compacted file on the node that produced it: the ingest flush
path and the replication workers are the other writers, and neither sees
compaction output. A node whose data is largely compacted therefore depends on
the scan entirely, and a truncated one costs partition pruning — a measurement
whose remaining rows are all cold drops this node's local files from the read.

**Setting the key below the time a full scan takes is harmful, not
conservative.** Hot-row retirement runs only after the whole walk, so a scan
that always truncates never retires a row for a file that has left hot storage.

**A truncated scan is now reported instead of being logged once and forgotten:**

- `ScanResult` carries `truncated`, set on every path that can run out of
  budget — the cold-tier sync, the hot walk, and the retirement pass, which
  returns no error of its own and so used to be reported as a complete scan.
- The partial counts come back rather than being discarded. `POST
  /api/v1/tiering/scan` answered a bare `500` with `context deadline exceeded`
  and nothing else; it now answers **503** with the counts and `truncated`, and
  distinguishes a scan the node cancelled while shutting down from one that ran
  out of budget. Any other failure keeps its `500`.
- `GET /api/v1/tiering/status` reports the last scan under `last_scan`. This is
  the only way a truncated **startup** scan is observable after the fact: it has
  no HTTP response, and a health check cannot ask the scan endpoint without
  starting a scan.
- A truncated scan now invalidates the query caches for what it did reach. The
  rows it wrote were real, but the query path kept serving its pre-scan answer.

**A second concurrent scan is refused with `409`.** The endpoint has no handler
deadline and a client disconnecting does not cancel the scan, so an operator who
gave up on a slow one and retried would start another full scan over the same
storage root, against the same SQLite handle, each invalidating the tier cache
the query path reads. A scan inside a migration cycle is deliberately not
refused — the cycle must always scan, or it would migrate from stale rows.

Fixed alongside: `scanTiers` never copied `HotRowsWritten` out of the walk, so
every consumer read zero. The cache invalidations keyed on it never fired,
including the one a standalone node needs on its first boot, when the scan
writes every hot row it has.


### The manual compaction trigger now requires the compactor lease

`POST /api/v1/compaction/trigger` ran a full compaction cycle on whatever node
received it. It checked whether a cycle was already running *on that process*
and nothing else — not the node role, not the compactor lease. Only the
scheduled cycles ever consulted the lease, and `compaction.enabled` defaults to
true, so every node in a cluster built a compaction manager and served a live,
unguarded trigger endpoint. A reader, a standby writer or a second compactor
would all accept one. Readers and non-lease writers are now refused; a second
compactor-role node still is not, unless compactor failover is enabled — see
the last bullet below.

That is unsafe because the partition lock is per-process: a trigger sent to a
non-lease node could select the same partition as the lease holder and delete
the same input files. With `cluster.replication_enabled` on it is worse and
needs no second node at all. The completion-manifest watcher — the component
that registers a compacted output and the deletion of its sources in the Raft
manifest — starts only on a node that may compact, but the job's own source
deletion is unconditional. So one trigger on a non-lease node deleted the
source files from storage while nothing recorded either the new output or the
deletions: phantom manifest entries pointing at files that are gone, and an
orphan output no manifest knows about. Nothing re-registers either, and an
enabled reconciliation sweep deletes the orphan and strips the phantoms.

The trigger now consults the same gate the schedulers use, re-read on every
request so a lease hand-over or a demotion takes effect without a restart. A
node that may not compact answers **503** with `can_compact: false`, its
`role`, and `lease_holder` (empty when the cluster manages no lease). 503
rather than 409 because this endpoint already answers 409 for a paused cluster
and for a cycle already in progress, and arcli reports any 409 here as "a
compaction cycle is already running (cycle N)" — a role rejection has no cycle
id, so it would have been reported as cycle 0. The remedy is carried in the
`error` field rather than only in `message`, because arcli renders `error` and
discards `message`.

The refusal distinguishes the two reasons a node may not compact, because they
have different remedies. If a lease exists and another node holds it, the
message names the holder and points at
`POST /api/v1/cluster/compactor/assign`. If the cluster manages no lease at
all — the default, since `cluster.failover_enabled` is false — the refusal came
from the node role, and the message says so and does *not* recommend the assign
endpoint, which answers "this cluster does not manage a compactor lease" in
exactly that state.

Read-only compaction routes are unchanged: they report the node's own state,
which is accurate everywhere.

Three consequences worth knowing before you upgrade:

- **A cluster-configured node whose license check fails now refuses manual
  triggers** on writer and reader roles. Such a node logs "running in
  standalone mode" but keeps its configured role, and its scheduler was already
  gated, so nothing scheduled ever ran there — the manual path was the only one
  left, and it is now closed. The node still shares storage with its licensed
  peers, which is why it is gated rather than exempted. Fix the license, or set
  `cluster.enabled=false` to run it as a genuine standalone node.
- **If the lease sits on a node that cannot be recovered, compaction stops
  cluster-wide and the manual trigger is no longer a way around it.** Move the
  lease with `POST /api/v1/cluster/compactor/assign`; the 503 body says so.
  That endpoint accepts compactor and writer targets.
- **A licensed cluster with no compactor-role node now has no manual
  compaction path.** With explicit `writer` and `reader` roles and
  `cluster.failover_enabled` at its default `false`, no node has the static
  capability and no lease exists to hold. Scheduled compaction never ran on
  those nodes either — the scheduler was already gated — so this configuration
  was already not compacting, but the manual trigger used to work and now
  returns 503. Give one node `cluster.role=compactor`, or set
  `cluster.failover_enabled=true` so the lease can be assigned to a writer.
- **A cluster whose nodes never set `cluster.role` is still permissive.** Every
  node is then `role=standalone`, which cannot hold the compactor lease, so the
  gate falls back to the static role capability — and that is true for
  standalone. Every node still accepts a trigger, exactly as every node already
  runs its own scheduled cycles in that configuration. That is a separate
  pre-existing problem, tracked as #892, and it is not changed here. Set
  explicit roles on a cluster.
- **Two compactor-role nodes with failover off both still accept a trigger.**
  Both have the static capability and no lease is ever assigned, so neither can
  be told it is the standby. Enable `cluster.failover_enabled` so exactly one
  holds the lease, or run a single compactor-role node — the same constraint
  Iceberg export already documents.
### Iceberg export serves a dotted edge-sync spoke, and migrates tables already published under one ([#1129](https://github.com/Basekick-Labs/arc/issues/1129))

An edge-sync spoke ID may contain a dot (`validateSpokeID` rejects `/`, `\`, `:`
and `..`, but not `.`), and Arc used to fold the spoke/database separator into
one, publishing a single Iceberg namespace component such as
`arc_rocket.01.telemetry`. iceberg-go v0.7.0 addresses a namespace whose
component contains a dot by a JSON-encoded catalog key rather than the plain
dotted string, so those tables stopped being found — and the v0.7.0 bump
refused such a database rather than publish one nothing could read.

A spoke pseudo-database `{spoke}/{db}` now keeps its boundary as **two**
namespace components, `{arc_rocket.01, telemetry}`, and Arc handles the encoded
catalog key end to end: the warehouse walk recognises the encoded directory, so
the exporter no longer reads its own metadata back in as a user database, and
the discovery files are published at the decoded path that directory-based
readers actually open.

**A spoke ID without a dot is unaffected and needs no migration.** Its catalog
key is byte-identical to the one Arc wrote before this change —
`rocket01/telemetry` is `arc_rocket01.telemetry` either way — so those tables
keep their identity and their history.

**A spoke ID with a dot needs its catalog row rekeyed, and that is previewed
before it runs.** `iceberg.namespace_migration_dry_run` defaults to **true**:
each reconcile pass reports the migration it would perform and changes nothing.
While a table is awaiting migration that measurement is **not exported** — the
legacy table stays readable and no second table is created beside it — and the
log names each affected database and measurement every pass, counted as
`awaiting_namespace_migration` in the pass summary. Set
`iceberg.namespace_migration_dry_run = false` to apply it.

**The migration rewrites the catalog row and moves nothing on disk.** That is
deliberate. Iceberg records manifest and data locations as absolute paths inside
the table metadata, and relocating a table does not rewrite them, so a migration
that moved the warehouse directory would leave the table's current snapshot
still reading its manifest chain out of the old directory. Rekeying leaves one
directory, not two: the table keeps the location it already has, later commits
write new metadata versions beside the existing ones, and nothing has to be
cleaned up afterwards. One consequence worth knowing if you browse the
warehouse: a migrated table's directory keeps its original
`arc_<spoke>.<db>.db` name, which no longer matches its catalog key. Tables
created after the upgrade use the encoded name.

The rekey is a single transaction, so it is idempotent and safe to interrupt,
and it refuses rather than guesses if the row changed underneath it.

### A year-shaped storage prefix no longer makes tiered queries return zero rows ([#1108](https://github.com/Basekick-Labs/arc/issues/1108))

If `storage.s3_prefix` or `storage.azure_prefix` had three or more segments and
the last one looked like a partition year, such as `tenants/eu/2026/`, queries
against a tiered deployment could return **zero rows with HTTP 200 and no
error**. Tier routing works out which database and measurement a storage path
belongs to by scanning backwards for a year-shaped segment; it found the one in
your prefix and read the two segments in front of it as the database and
measurement. Nothing was ever written there, the tier-metadata lookup came back
empty, and the read fell back to a glob under those wrong names.

The prefix is now removed before that scan. If you ran a tiered deployment with
a prefix of this shape, query results were silently incomplete and are correct
after upgrading; nothing on disk was affected, so no backfill is needed. Arc
used to warn about such a prefix at startup and no longer does, because it is
no longer a hazard — there is no need to change a prefix you already run.

Deployments without tiering were unaffected: the misread names reached only the
stable-schema anchor lookup, which simply found nothing, and the query still
read the correct location. Partition pruning was never affected either — it
parses the glob from the end and always kept the prefix intact.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1156](https://github.com/Basekick-Labs/arc/pull/1156).

### A restore no longer resurrects a hot copy of a file that has since migrated ([#1139](https://github.com/Basekick-Labs/arc/issues/1139))

A file that was in the hot tier when a backup was taken, and that tiering
migrated to cold afterwards, was restored like any other hot file: the bytes
went back to hot storage and the file was registered in the cluster manifest,
while the guarded tier upsert refused to move the row off cold and said nothing
about it. The row and the objects then disagreed, and the query path reads both
tiers for a measurement that claims both — so the restored rows came back
**twice** if anything else in that measurement was still hot. They came back
**not at all** when the measurement had gone entirely cold and the cold object
was missing or unreadable — the restored copy then sat on disk with nothing
routing a reader to it.

The local tier row now decides where a restored file belongs, because it is
this node's current truth and a restore's job is to make the bytes at that
location correct rather than to re-decide tiering. For a file the backup holds
as hot whose row says cold:

- a cold object whose size matches the backup's record is already the restored
  copy, so the write is skipped — counted as `cold_files_skipped_already_cold`;
- a cold object that is missing, differs in size, or is only a half-finished
  staging file is overwritten **in cold storage**, where the row says it is;
- a node with no cold backend configured has nowhere to put it, so it is
  restored hot and its row forced hot, exactly as before — counted as
  `cold_files_restored_to_hot`, not as a route to cold;
- a backup carrying no sidecar row for the file leaves the cold copy **exactly
  as it is**, counted as `cold_rows_skipped_unverifiable` and named in the log.
  There is nothing to check replacement bytes against, and overwriting the only
  copy with unverifiable bytes is worse than not restoring it. Those files are
  not restored.

The first two cases are counted as `hot_backup_files_routed_to_cold`, and a
routed write is verified against the sidecar's size and SHA-256 before it
replaces the cold object — on a standalone node as well as in a cluster. A
damaged backup therefore fails the restore with `sidecar_mismatch_sample`
instead of replacing the only copy of the file.

Where a cold backend exists, no hot copy is created for a file whose row says
cold, so this restore does not create a row/object disagreement — which means
the tiering orphan sweep, which deletes a hot copy once it confirms a cold one,
cannot act on a file this restore has just written. It does not clean up
duplicates an earlier restore already produced; those age out of the sweep's
window on their own.

Size is the discriminator because a tier row stores no checksum of the backup
object. On its own that is weak, but migration is a byte-preserving move of the
same path and the writers that produce new content emit new filenames, so a
cold object at that path with a matching size is that file. The check looks at
the final object specifically: on local disk a stat alone reports the size of a
`.part` staging file, which would let a half-transferred object pass as a
complete one.

Separately, a replication report refused by the cold-row guard now logs the
path, the tier that refused it and whether the row is quarantined, instead of
being dropped silently. The read behind that log cannot fail the event it is
describing.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1145](https://github.com/Basekick-Labs/arc/pull/1145), carried forward with the routing change.

### Database deletion cleans up tiered storage ([#1091](https://github.com/Basekick-Labs/arc/issues/1091))

`DELETE /api/v1/databases/:name` removed the database's hot files and nothing
else. Its `tier_files` rows survived, and so did its cold-tier objects and
their storage cost — indefinitely, because the cold sync records what is in the
cold store and never reverts a row. A database re-created under the same name
inherited them, and because the query path routes a measurement to cold when a
cold row exists, those stale rows could point reads at the previous
incarnation's objects.

Deleting a database now also removes its cold-tier objects and retires its tier
rows. In a cluster the database's manifest entries are removed before any hot
object is deleted, in batches, and a manifest failure aborts the deletion
before anything is removed from storage (with tiered storage disabled, see
[#1094](https://github.com/Basekick-Labs/arc/issues/1094) below). The response breaks the count out as
`hot_files_deleted` and `cold_files_deleted` rather than one total, because the
cold half is the irreversible part.

Four things worth knowing before you upgrade:

- **Tier-metadata cleanup is node-local.** The node serving the request retires
  the rows in its own metadata store. Other nodes keep their own rows for that
  database until the deletion is repeated there, and nothing retires a peer's
  cold row automatically. On a cluster, issue the DELETE on each node, or
  expect the database to stay listed on the others.
- **A database with only cold data is now deletable.** `databaseExists`
  consults tier rows, so a database whose hot files are already gone can be
  deleted instead of answering 404 — which is how the stale rows above get
  cleaned up. The same check makes `POST /api/v1/databases` answer **409** for
  a name that still carries tier rows; delete it first, then create.
- **A partial failure keeps the database.** If any file, cold object or tier
  row cannot be removed, the marker, the field-schema anchors and the Iceberg
  catalog entry are all left in place and the response is a 500 listing what
  failed. Re-running the DELETE retries the whole operation. The database is
  partially deleted in that state, not intact — queries over it return
  whatever survived.
- **With `tiered_storage.cold.enabled` off**, a database holding cold rows
  cannot be finished: there is no backend to delete its objects through or to
  verify them against, so the rows are retained and the response says so once,
  naming the key. Re-enable cold storage and repeat the deletion.

A deletion also reserves the tier lifecycle while it runs, so a tier scan or
migration cannot sweep a tier the deletion has already passed. A delete that
arrives while a scan or cycle holds that reservation answers 409 naming which
one; on a cluster node that may not mutate the tiered store it answers 503 and
names the primary writer to route to.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1166](https://github.com/Basekick-Labs/arc/pull/1166).

### Database deletion on a cluster without tiered storage reaches every node ([#1094](https://github.com/Basekick-Labs/arc/issues/1094))

The cluster half of the #1091 fix above runs through the tiered-storage
manager. That manager exists only when `tiered_storage.enabled` is on, so a
cluster without tiered storage kept the old behaviour: `DELETE
/api/v1/databases/:name` deleted the database's files on the node that served
it and nowhere else. Two things followed:

- **Other nodes kept their copies.** Readers and the compactor kept their
  replicas and kept answering queries for the dropped database.
- **The manifest still listed the files.** A backup reported them as
  manifest-only, and the re-pull path could copy them back onto the primary.

The handler now takes the same steps on such a cluster:

- **Primary only:** a node that is not the primary writer answers **503**,
  naming its role. This applies on clusters with shared storage or
  replication, the same rule as with tiered storage enabled. Without either,
  nodes share nothing, so each node may still delete its own files.
- **Manifest first:** the database's manifest entries are removed in batches
  of 500 before any local file is deleted, so every node's delete worker
  unlinks its copy.
- **Abort on failure:** a failed batch aborts with a 500 before the local
  sweep. Retry the request; batches already applied stay applied.
- **Edge-sync spokes untouched:** on a hub, a spoke's database of the same
  name is not touched, since its files live under `{spoke}/{db}/`.
- **Database visible from the manifest:** a database the manifest still lists
  counts as existing on a node that holds none of its files locally. So
  `DELETE` there no longer answers 404, `GET /api/v1/databases/:name` finds
  it, and `POST` answers 409 for that name.

**With tiered storage enabled**, nothing changes: the tiered-storage manager
already performs these steps. **Standalone**, nothing changes.

Two limits, unchanged by this fix:
- **In-memory unlink queue:** a peer's delete worker queues the unlink in
  memory. If that node restarts or loses the primary before the unlink runs,
  its copy stays on disk with no manifest entry.
- **Leader change mid-delete:** fixed by #1236 (below): a leader change is
  retried for about 7 seconds, as on the tiered-storage path.

Contributed by [@jallegri](https://github.com/jallegri) in [#1176](https://github.com/Basekick-Labs/arc/pull/1176).

### Crash smokes verify acknowledged records after WAL replay

The enterprise-shared crash scenarios restart the killed writer and wait for
`/ready`, then check the host IDs from every successful write batch. WAL replay
duplicates and records persisted by failed requests are allowed, but cannot mask
missing acknowledged records. Crash runs reject an idle kill target or a crash
that never triggered. The base scenario keeps its exact row-count assertion and
now exits successfully when it passes.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1004](https://github.com/Basekick-Labs/arc/pull/1004).

### Cluster compaction recovery records outputs before deleting sources ([#1155](https://github.com/Basekick-Labs/arc/issues/1155))

When recovery finds an uploaded compacted file, it now writes the parent-side completion record before removing its inputs, then records the source deletions for Raft. Previously the inputs could go while Raft never learned about the output, leaving a compacted file that no manifest entry described — and on a cluster with `reconciliation.enabled`, an orphan the sweep would eventually delete.

How a failure to write that record is handled depends on whether it can ever clear. A transient failure retains the inputs and the storage manifest, and the next cycle retries. A failure that cannot clear — the stored output disagrees with what the manifest records, or the completion record on disk belongs to a different job — **parks** the manifest under `.quarantined` instead, because retaining it would make recovery re-read and re-hash the whole compacted output on every cycle, for ever. A stored output that is simply shorter than the manifest records is treated as the partial upload it is: the output and the manifest are both discarded so the next cycle redoes the job, which is what Arc already did when the backend could be asked for object sizes directly.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1157](https://github.com/Basekick-Labs/arc/pull/1157).

### Partial DELETE rewrites remain immutable without duplicating data ([#975](https://github.com/Basekick-Labs/arc/issues/975))

Partial DELETE now publishes the surviving rows under a fresh immutable Parquet path and retires the superseded object in every storage mode, including standalone OSS deployments. The rewrite filename is normalized before compaction, tiering and file-time pruning classify it. This closes the same-size/different-content replication class for DELETE rewrites, without leaving deleted rows visible through Arc's glob-based query path. Producers that still reuse a path (restores, imports, edge-sync after the hub deleted it) are covered by the #798 entry.

**Two DELETEs that touch the same file no longer merge.** Before this release they did, because the
rewrite kept the file at its original path, so the second request read what the first had written.
With immutable rewrites the rewrite phase is serialized, and a request whose source was retired
while it waited fails with `source file was retired by a concurrent delete, retry the request`
rather than acting on a stale file list. Retrying it succeeds — the retry rebuilds the affected-file
list and sees the new path. Nothing is lost and no deleted rows come back, but a client that fires
overlapping DELETEs (or times out and retries one) will now see that error where it previously saw
success, and should retry rather than treat it as a failure.

If the old-object delete fails after the manifest commit or standalone publish, Arc reports the storage failure explicitly because the old object can remain query-visible until cleanup. A crash in that window can likewise leave both objects; operators should treat the corresponding error as a signal to inspect the partition before retrying.

### Compaction subprocess threads respect license and effective-core limits ([#1036](https://github.com/Basekick-Labs/arc/issues/1036))

Each compaction subprocess is now capped at the lower of the license's
`MaxCores` and the effective cores available to Arc, after automatic thread
defaults have been resolved. Lower configured values are preserved. This is a
per-process cap, not an aggregate reservation: the main process and multiple
subprocesses can still request more threads in total than `MaxCores`. Capping a
previously higher setting can reduce compaction throughput.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1043](https://github.com/Basekick-Labs/arc/pull/1043).

### Raising compaction.max_concurrent no longer multiplies the subprocess thread count ([#1037](https://github.com/Basekick-Labs/arc/issues/1037))

When `compaction.threads` is left at its automatic default, Arc divided the CPUs
this process may use by two, regardless of how many compaction jobs could run at
once. The halving hardcoded the default `compaction.max_concurrent = 2`, so an
operator who raised concurrency got that many subprocesses each still claiming
half the cores: at `max_concurrent = 4` on 16 CPUs, four jobs at 8 threads
each. The memory budget already tracked the setting — `compaction.memory_limit`
divides by `max_concurrent` — so the two derivations disagreed about the same
knob.

The automatic default is now those CPUs divided by `max(2,
compaction.max_concurrent)`, minimum 1. "CPUs this process may use" is
`min(runtime.NumCPU(), runtime.GOMAXPROCS(0))`, which reflects a quota, a
cpuset, or an explicitly set `GOMAXPROCS` without distinguishing which one
imposed the limit.

**At the default `max_concurrent = 2` nothing changes** — the divisor is still
2, so no existing deployment loses compaction throughput on upgrade. Above the
default it tightens: `max_concurrent = 4` on 16 CPUs now gives 4 threads per
subprocess rather than 8. That is a deliberate throughput trade in the
configuration that was oversubscribing, and it is not backed by a workload
benchmark at raised concurrency — what is measured is that thread count trades
against the per-subprocess memory budget rather than being uniformly better or
worse (see #1030 in the 26.09.3 notes for the numbers). An explicit
`compaction.threads` value is untouched by the automatic default, though the
licence cap above still applies to it.

The divisor covers compaction subprocesses only. The main process competes for
the same CPUs and is not accounted for here, so these remain caps on
parallelism, not reservations.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1041](https://github.com/Basekick-Labs/arc/pull/1041).

### Iceberg snapshot history no longer advertises files Arc has deleted ([#1092](https://github.com/Basekick-Labs/arc/issues/1092))

**Arc does not support Iceberg time travel across compaction, retention or a
partial DELETE, and the metadata now says so instead of failing on the read.**

Every Arc mechanism that removes a data file — compaction (scheduled hourly by
default), retention, and both branches of the delete API — removes it from
storage and from the current Iceberg snapshot. Older snapshots kept naming the
deleted path, so an external engine time-travelling to one got:

```
IO Error: Cannot open file ".../cpu_20261008_155058_567682000.parquet": No such file or directory
```

That is indistinguishable from object storage having lost the object, which is
the wrong thing for an operator to go and investigate. A reconcile pass that
removes any path now expires every older snapshot, so the same query returns a
routine `Could not find snapshot with id ...` — a normal outcome of any
retention policy, which Iceberg consumers already handle by re-reading the
current snapshot.

**This loses no readable history.** A removed path is referenced by every
snapshot from the one that added it onward, so each of those was already
unreadable. An append-only pass removes nothing and still keeps
`iceberg.retain_snapshots` snapshots (default 10), because that history does
work.

Why Arc cannot do real copy-on-write here: Iceberg keeps superseded data files
alive until the snapshots referencing them expire, but Arc reads a partition
with a glob, so any superseded file left in place is double-counted by every
ordinary query — the bug [#1073](https://github.com/Basekick-Labs/arc/pull/1073)
fixed, where a 3-row table returned 11. The two requirements are in direct
conflict, and honest metadata is the half Arc can deliver.

**Two limitations worth knowing:**

- The fix applies to the entry point readers use, `version-hint.text`. Arc also
  keeps `retain_snapshots + 1` older `v<N>.metadata.json` copies for
  directory-based readers, and those still list the pre-floor snapshots. A
  consumer that pins one of them directly still gets the `IO Error` until the
  copy is pruned, roughly `retain_snapshots` passes later.
- History already broken on an existing deployment is cleaned on that table's
  next pass that removes something. A table whose file set has gone quiet is
  skipped by the reconciler, so its stale metadata stays until it changes again.

A pass that drops history logs at `warn` with the before and after snapshot
counts, so an operator who notices one-deep history finds the reason rather
than inferring it. A failed expiry now also leaves the measurement uncached, so
the next pass retries it; previously only a failed version-hint write did that.

### Database API storage calls now have a deadline ([#1065](https://github.com/Basekick-Labs/arc/issues/1065))

Database API handlers now bound storage calls with a request context. Database
deletion gets a deadline scaled to the number of listed files, so a large
database is not cut off by the same fixed limit as a small one; partial-delete
errors continue to be collected and reported, including failure to delete the
database marker. Database details return an error if measurement listing fails,
rather than reporting a successful response with a zero measurement count.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1067](https://github.com/Basekick-Labs/arc/pull/1067).
### Object-store prefixes count toward the storage key limit ([#1121](https://github.com/Basekick-Labs/arc/issues/1121))

S3 and Azure now validate the complete object name, including its configured
prefix, before sending it to the object store. Listings and prefix probes use
the same check; previously stored over-limit names are omitted from ordinary
listings and reported by `ListUnusable`. Azure batch deletion applies the same
check and continues deleting the other valid keys in the batch.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1132](https://github.com/Basekick-Labs/arc/pull/1132).

### Forwarded writes honor cancellation during TLS setup ([#1064](https://github.com/Basekick-Labs/arc/issues/1064))

Leader forwarding now passes its existing context into peer dials, so
cancellation interrupts a stalled TLS handshake instead of waiting for the dial
timeout. Regression tests cover leader-dial cancellation.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1068](https://github.com/Basekick-Labs/arc/pull/1068) and
[@jallegri](https://github.com/jallegri) in
[#1117](https://github.com/Basekick-Labs/arc/pull/1117), combined here with
coverage from both contributions.

### A fresh object store no longer makes the database and table listings fail ([#945](https://github.com/Basekick-Labs/arc/issues/945))

Most S3-compatible stores create the bucket on the first authenticated write,
so a deployment read before its first write had no bucket yet and every
listing failed: `GET /api/v1/databases`, `GET /api/v1/databases/{db}/measurements`,
`SHOW DATABASES` and `SHOW TABLES` all answered 500 with `NoSuchBucket` until
data arrived. Those four now answer an empty result instead.

The five listing operations on both object-store backends — listing keys,
listing directories, listing objects, enumerating unusable objects, and
checking whether a prefix holds any object — report a bucket or container that
does not exist as a distinct error rather than as an error message, which is
what lets those four read paths recognise it. Only the structured
`NoSuchBucket` and `ContainerNotFound` responses are recognised; transport
failures, authorization failures and every other storage error stay visible.
A read of a specific object from a missing bucket or container is reported the
same way since [#1238](https://github.com/Basekick-Labs/arc/issues/1238) (see
that entry).

Everything else that lists storage still treats a store it cannot find as a
failure, deliberately. Query planning drops a whole tier from a read when a
listing proves the tier holds no files for the time range, and retention,
backup, reconciliation and the Iceberg export all decide what to delete or
publish from a listing. For those, a bucket name with a typo in it answering
"empty" would not be a slow query or a missing log line — it would be a
silently wrong answer, or a deletion. The two states produce the same listing
and only the error tells them apart, so the error is what they get.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#950](https://github.com/Basekick-Labs/arc/pull/950).

### Float-to-integer conversion rejects the rounded upper bound and NaN ([#936](https://github.com/Basekick-Labs/arc/pull/936))

Converting `math.MaxInt64` to a float rounds it to `2^63`, so the previous
bounds check accepted that out-of-range value; NaN also passed the comparisons.
Both now fail conversion to `int64`. The MessagePack typed decoder shares the
same guard, so single-map columnar payloads follow the same rejection rules as
the generic conversion path used for batch and array payloads and when decimal
columns are configured. Invalid values in integer-inferred columns return an
error instead of being silently converted to an architecture-dependent integer.

Finite values in the valid range still truncate toward zero. Infinities and
values beyond the boundaries remain rejected, as before.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#936](https://github.com/Basekick-Labs/arc/pull/936).

### Query path extraction no longer folds backslashes ([#750](https://github.com/Basekick-Labs/arc/issues/750))

Query tier selection no longer converts backslashes in storage keys into path
separators, which could identify a different database or measurement. Native
separators remain permitted only within a trusted local storage root; the key
portion retains its slash-separated identity.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#918](https://github.com/Basekick-Labs/arc/pull/918).

### Local storage closes appended files exactly once ([#750](https://github.com/Basekick-Labs/arc/issues/750))

`LocalBackend.AppendReader` previously closed the staging file explicitly on
successful promotion and again through a deferred call. It now closes the file
once after each copy attempt, including failed reads, and checks close errors
before promoting a completed transfer. Failed copies and failed closes retain
the staging file for retry and leave an existing committed file in place.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#919](https://github.com/Basekick-Labs/arc/pull/919).

### Distinct Arrow schemas no longer share an ingest cache entry ([#750](https://github.com/Basekick-Labs/arc/issues/750))

The ingest schema cache now uses deterministic, length-prefixed column identities
instead of ambiguous slice formatting. It includes decimal precision/scale and
canonicalises column and tag ordering, preventing stale Arrow types or metadata
when a measurement's schema changes.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#913](https://github.com/Basekick-Labs/arc/pull/913).

### Fallback compaction job IDs support spoke namespaces ([#750](https://github.com/Basekick-Labs/arc/issues/750))

When a compaction job was created without an explicit JobID, its fallback ID
included the raw database name. An edge-sync pseudo-database such as
`rocket-01/telemetry` introduced a slash into the ID, causing completion-manifest
validation to reject it and creating an unintended nested temporary directory.
Fallback IDs now use the same database-name sanitiser as manager-generated IDs,
while preserving the actual database value and any caller-supplied JobID.

This addresses item 4 of #750. The manager already supplied sanitised IDs;
the fix covers the fallback in `NewJob`.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#912](https://github.com/Basekick-Labs/arc/pull/912).

### Iceberg export reclaims the manifest files expired snapshots leave behind ([#835](https://github.com/Basekick-Labs/arc/issues/835))

Iceberg export never deleted the manifest lists and manifests of snapshots it had expired, so a
table's `metadata/` directory grew with every commit for the life of the deployment and the whole
pile was copied into every backup. On a rig with one live snapshot there were fifteen `.avro` files.

The reconciler now sweeps them. After each pass it computes which manifest lists and manifests are
still reachable — from **every** `metadata.json` left on disk, not just the current snapshot, since
Arc deliberately keeps `retain_snapshots + 1` `v<N>.metadata.json` copies and iceberg-go keeps its
own metadata log as entry points a directory reader can resolve — and deletes the `.avro` files in
that directory that nothing can reach. Measured on the rig above: fifteen files down to ten, with
every retained metadata version still resolving and DuckDB still reading the table.

Three things bound what it will touch. Only files ending `.avro` directly in the table's metadata
directory are ever deleted, so data files (`.parquet`, and outside that directory in any case),
`version-hint.text`, the metadata files themselves and Iceberg's Puffin statistics are not
deletable by this code path at all. A file must be older than a grace period — one hour, or twice
`iceberg.reconcile_interval` if that is longer — so a manifest written by a commit that has not yet
landed its `metadata.json` is left alone. And every failure fails closed: if the listing, a
`metadata.json` or a manifest list cannot be read, the reachable set is incomplete and nothing is
deleted at all. A warning says so; note that a warehouse restored with its `.avro` files lagging
its `metadata.json` files can stay in that state until they are reconciled, since the sweep will
not act on a reachable set it cannot complete.

What the grace does **not** change is the window for a reader that has just resolved an older
`v<N>.metadata.json`: the grace is keyed to a file's age, not to how long it has been unreachable,
and a manifest is usually hours old by the time the last metadata version naming it is retired.
That race is bounded as it was before this change, by `pruneOldVersionFiles` keeping
`retain_snapshots + 1` of those copies so the version a reader just resolved is not the one being
retired.

This bounds the metadata directory rather than emptying it. The retained metadata versions keep
their manifests alive on purpose, so the steady state is on the order of `retain_snapshots`
commits' worth of `.avro` instead of unbounded growth.

Set `iceberg.orphan_sweep_enabled = false` to restore the previous behaviour. It is the one deleter
in the exporter whose work nothing regenerates, so it has an off switch; Arc logs a warning at
startup when it is off.

The second half of #835 is fixed below in
[#1106](https://github.com/Basekick-Labs/arc/issues/1106). Two claims made here when #835 shipped
turned out to be wrong once measured: it is not only a planning cost (the larger cost is on Arc's
own commit path), and the fix does not belong upstream (an upstream fix removes half the growth).

The same fix was implemented independently, and three weeks earlier, by
[@efegokdemir](https://github.com/efegokdemir) in
[#923](https://github.com/Basekick-Labs/arc/pull/923), which was open against this issue and had not
been reviewed when the work above started. The two designs converged on the same approach. One check
#923 carries that the shipped sweep does not — reloading the table and re-running reachability
immediately before deleting, so a scan that has gone stale cannot delete — is tracked in
[#1125](https://github.com/Basekick-Labs/arc/issues/1125).

### Iceberg export keeps a table's manifest set bounded ([#1106](https://github.com/Basekick-Labs/arc/issues/1106))

Every reconcile pass that removed a file added **two** manifests to the table and nothing ever shed
one: the manifest holding the removed entry, rewritten with that entry marked DELETED, plus this
pass's additions in a new manifest. iceberg-go carries every untouched manifest into the next
snapshot verbatim, so the set only grew — on the order of 48-96 manifests a day on a measurement
with hourly compaction and daily retention, forever.

Measured on a 300-file table, one file swapped per pass, so only the inherited manifests grow:

| removal passes | manifests | reconcile pass | scan plan |
|---|---|---|---|
| 1 | 3 | 20 ms | 2.3 ms |
| 16 | 33 | 71 ms | 7.0 ms |
| 64 | 129 | 230 ms | 19.2 ms |
| 80 | 161 | 276 ms | 21.8 ms |

Both costs grow at ~1.6 ms per manifest per reconcile pass and ~0.13 ms per manifest per scan plan,
and both slopes are independent of the table size — it is per-manifest open overhead, not per-entry
work. The larger of the two is Arc's own write path: a pass has to read every manifest to find the
ones holding the files it is removing, so pass cost grows quadratically with the number of passes.
The scan-plan cost is paid by every reader of the table — DuckDB, Spark, Trino — on every query.

A pass that finds the table at 12 data manifests or more now also merges them into one, in the same
commit, and both figures return to their one-manifest baseline. The merge adds 8 ms to a pass on a
300-file table and 61 ms on a 10 000-file one — it rewrites the live set, so its cost grows with the
table while the saving does not, and that is where the threshold of 12 comes from. Once a pile has
actually built up the merge is not merely affordable: above ~2000 live files the merging pass is
*cheaper* than the ordinary pass it replaces (244 ms against 352 ms at 10 000 files), because it
trades 33 manifest writes for one.

It applies to any pass — additions, removals, or both — so a measurement that only loses files to
retention is covered too. On iceberg-go v0.7.0, which this release also moves to, a table reaches
the threshold more slowly than it did: see the upgrade note below for which passes still grow the
manifest set. The one pass it deliberately skips is the one that leaves a
measurement empty — whether because every data file is gone, or because the only candidates left
are files Arc cannot map to a partition and has to skip. With no live files there is nothing for a
merged manifest to hold, and Iceberg rejects an empty one.

The threshold is not configurable: a wrong value means a slower or a more frequent merge, never
data loss.

Two notes for operators of large tables. A merged table's whole live file list sits in one manifest
file, so a backup of the Iceberg warehouse sees fewer, larger `.avro` files. And a merge pass writes
the manifest set twice within its commit; the superseded copy is reclaimed by the orphan sweep from
#835 above, once the metadata versions referencing it retire. If you have turned that sweep off
with `iceberg.orphan_sweep_enabled = false`, this residue is permanent like the rest of it — a
merge pass is a new, modest contributor to the growth that switch accepts. One consequence worth
knowing: a merged table's live file list is concentrated in a single manifest, so where losing one
manifest used to cost a fraction of the file list it now costs all of it, and nothing re-registers
a lost manifest.

### Backup listings report unreadable manifests instead of hiding backups ([#1123](https://github.com/Basekick-Labs/arc/issues/1123))

When a backup ID was listed but its manifest could not be read or parsed, Arc
silently skipped it. The listing now reports that destination as incomplete;
when every configured destination is affected, it returns an error instead of
an empty successful result. Readable backups on other destinations remain
available in a partial listing.

An incomplete destination contributes none of its backups, including the ones
whose manifests read fine, because a listing that silently leaves out a backup
is the problem this fixes. With a single destination, one manifest that cannot
be parsed therefore makes `GET /api/v1/backup` answer 503 until it is repaired
or removed. Retrying does not help in that case; the server log names the
manifest (`Failed to parse manifest; target listing is incomplete`), and the
other backups can still be fetched and restored by id.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1138](https://github.com/Basekick-Labs/arc/pull/1138).

### A backup fails loudly when compaction recovery state cannot be copied ([#1100](https://github.com/Basekick-Labs/arc/issues/1100))

Object stores cap a key at 1024 bytes, and a backup writes every source key
under a longer destination key. Arc's three copy paths disagreed about what to
do when that destination key would overrun.

Data files checked before writing and skipped, counting and naming what they
skipped. The Iceberg warehouse checked and failed the whole run. Compaction
recovery state did not check at all, so the overrun surfaced from inside the
storage write as a generic "failed to write to backup storage" and failed the
run with a message about storage rather than about the key.

Compaction recovery state now checks before writing and still fails the run,
with an error that names the file, the destination size, the limit, and the
length a source key has to come under. The behaviour is deliberately not the
data path's skip, and the reason is worth stating because it looks like an
inconsistency:

- A skipped **data file** is detectable. It is counted in the manifest, named
  in the skip sample, carried into the `INCOMPLETE` marker, and the source
  still holds it, so you can see what the backup is missing.
- A skipped **recovery manifest** is invisible **to a restore**. It is counted,
  but the restore path reads four other skip fields and not that one, and
  restoring without the manifest brings back a compacted output *next to the
  inputs it replaced* with nothing to reconcile them. That partition then
  serves every row twice, permanently. A third option would have been to skip
  and set the `INCOMPLETE` marker; what rules it out is that a restore does not
  look at this counter.

So the paths differ on whether absence can be noticed, not arbitrarily. Only
the data path skips.

Reaching this at all needs a file with a very long name dropped under
`_compaction_state/`; Arc's own state keys are short and fixed-shape.

### A failed write to a remote backup destination no longer leaves an object behind ([#1101](https://github.com/Basekick-Labs/arc/issues/1101))

Backup cleanup only ran for a destination that stages its writes, which in
practice means a local directory: it removed the `.part` file and stopped.
For an S3 or Azure destination it returned immediately on the grounds that
those do not stage.

That is true and it was the wrong conclusion. Neither commits anything on a
failed upload (the AWS SDK aborts its own multipart, and an Azure block-blob
commit is atomic), but a commit that *succeeds* while its response is lost
leaves an object that no backup references. Cleanup now deletes the
destination key in that case, logs at debug if the delete itself fails, and
never fails the backup over it. The copy of the metadata database, which had
no cleanup at all, is covered too.

Nothing changes for you yet. A backup destination is still always a local
directory, which does stage, so the new branch is unreachable until a backup
can be sent to a named remote target. This lands now because that is the
change it is a prerequisite for.

Restore cleanup deliberately did not change: it is handed live data keys, and
since a failed overwrite leaves the previous object intact, deleting there
would destroy a registered file to clean up a write that did no damage.

### Existing Iceberg tables honor retention changes ([#1093](https://github.com/Basekick-Labs/arc/issues/1093))

`iceberg.retain_snapshots` now updates Iceberg's metadata-file retention properties on existing tables. Reconciliation skips commits when the properties already match.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#1095](https://github.com/Basekick-Labs/arc/pull/1095).

### Empty Arrow IPC queries complete successfully ([#731](https://github.com/Basekick-Labs/arc/issues/731))

When a measurement had no Parquet files, `POST /api/v1/query/arrow` returned
HTTP 500 and recorded the query as failed, unlike the JSON and MessagePack
paths. It now returns a valid empty Arrow IPC stream and records a completed
query with zero rows. Empty results release their query timeout context without
a cleanup panic. Missing field-schema anchors remain errors.

Contributed by [@jallegri](https://github.com/jallegri) in [#1120](https://github.com/Basekick-Labs/arc/pull/1120).

### Behavior change for clustered deployments: backup deletion requires the primary writer ([#1134](https://github.com/Basekick-Labs/arc/issues/1134))

`DELETE /api/v1/backup/:id` now uses the same primary-writer gate as backup
creation and restore. Previously, a request reaching a reader, standby writer,
or compactor could delete a backup across that node's configured targets.
Non-primary nodes now return **503** with `route to the primary writer` before
accessing backup storage. Direct backup deletion requests, including those from
`arcli backup delete`, to the primary writer rather than a service address that
can send them to other nodes.

Eligibility is checked on every request, so promotion and demotion take effect
without a restart. A deletion already admitted is not cancelled on demotion.
Standalone and OSS deployments without a cluster coordinator keep their
existing behavior.

### Backup and restore are cluster-safe ([#1083](https://github.com/Basekick-Labs/arc/issues/1083))

On a cluster node a backup or a restore was undefined behaviour: any role
could run one, a restore wrote files into storage without registering them in
the Raft file manifest (so they never replicated, and an enabled reconciliation
sweep removed them as orphans after its grace window), restored files got no
tier row, and a backup taken during a compaction commit copied the compacted
output together with the inputs it replaced, which a restore then served twice
on every node. Nothing changes for a standalone node except that every backup
now also writes a `manifest-files.json` sidecar next to its `manifest.json`.

`POST /api/v1/backup` and `POST /api/v1/backup/restore` now run only on the
primary writer. Standby writers, readers, the compactor, and a node configured
`cluster.role = "standalone"` inside a cluster that is not on shared storage,
answer **503** with `route to the primary writer`, the shape the delete API
uses (409 keeps its meaning of "an operation is already running"). The check is
made per request, so a promotion takes effect without a restart. A primary
demoted while a run is in progress finishes it, forwarding its manifest
operations to the leader; a run started on the new primary meanwhile is not
excluded, so wait for the old primary's run to end before starting one.

A cluster backup waits for its node's manifest to catch up with the Raft leader
(the same barrier replication catch-up uses, with the same timeout), snapshots
the manifest right after the storage listing, and copies only the data files
the manifest lists. A listed data file the manifest does not know is a
compaction or retention input awaiting unlink, a pre-cluster file or a dropped
registration, and is left out, counted in a new `unregistered_skipped` field
with up to 32 names in `unregistered_sample`. A manifest entry this node does
not hold (a new primary still catching up) is counted in `manifest_only_files`
with a sample, and the backup is incomplete by that many files. An empty
manifest while the node lists data files refuses the backup instead of
producing an empty one that reports success, and a cluster node without a Raft
manifest (`cluster.raft_data_dir` unset) runs backups without the cross-check
and restores without registration.

Compaction commits in two Raft phases on two watcher ticks: the output is
registered when written, and the inputs are manifest-deleted on a later tick,
with no wait for peers to pull the output in between. A backup taken between
the two therefore copies the inputs, which are still registered. To close most
of that window, the backup waits for the manifest again and re-reads it once
at the end of the data copy, and settles every provisional decision against
it: a copied data file the manifest has since stopped listing is removed from
the backup again (counted in `left_manifest_during_run` with a sample); a
listed file registered since, or a manifest entry this node has pulled since
the listing, is copied after all; and a file skipped because it could not be
read at copy time but gone from the manifest by then is counted in
`skipped_reconciled` as not missing data. The residual window is a phase 2
that has not landed by the end of the run while the output is already local:
the backup then holds inputs and output next to each other, exactly as the
cluster does at that moment, and a merge restore of it serves those rows twice
until the next compaction cycle on the restored data. The sidecar lists every
data file copied with the SHA-256 of its bytes, its size, and the manifest
entry's database, measurement, partition time and `created_at`; a standalone
backup fills the last four from the path and the backup time.
`GET /api/v1/backup` carries the three new counts, plus `skipped_reconciled`
and `cluster_manifest_checked` ([#1555](https://github.com/Basekick-Labs/arc/issues/1555)). Reserved roots (the
`_schema` anchors, compaction state) are copied as before and never
cross-checked or registered.

A cluster restore waits for the manifest the same way, then registers every
data file it writes from the sidecar, in batches of at most 1000 operations and
256 KiB of payload, the file registrar's own caps, because the primary writer
is routinely a Raft follower and every batch is forwarded to the leader inside
a 1 MiB frame; a batch refused only because a leader election is in progress
is retried for 15 s. A path the manifest already lists keeps the manifest's
database, measurement, partition time and `created_at`. Writes and
registrations interleave per batch. Before a data file is written its bytes
are checked against the sidecar on the way through the temp file: a size or
SHA-256 mismatch, or a file with no sidecar row, is not written at all (the
live copy stays), is counted in `sidecar_mismatches` with a sample, and the
restore ends `failed`; a same-size corruption registered under the sidecar's
hash would otherwise fail every peer's checksum forever. If the manifest
refuses a batch the restore stops there, ends `failed`, and names the
written-but-unregistered files in `registration_failed` and
`registration_failed_sample`; nothing re-registers them, and an enabled
reconciliation sweep (it is opt-in, and report-only until its dry run is turned
off) removes them after its grace window of 24 h plus 5 min of clock skew, so
the recovery is to run the restore again. A restore interrupted between a write
and its batch leaves the same window. A backup taken before this release has no
sidecar and a cluster node refuses to restore it; restore it on a standalone
node or take it again. Every data file a restore writes is also reported to
this node's tier metadata, on a cluster or on a standalone node with tiering,
so the query layer routes to it without waiting for the next tier scan; a path
whose tier row says cold keeps that row, and the restored hot copy is read from
cold until the next tier scan.

Registering a path the manifest already lists is not a no-op: every peer gets
one callback, a stat and, where the SHA differs from the entry it holds, a
re-pull of the file from the restoring node. With
`cluster.query_gate_on_catchup = true` readers answer 503 until they have
converged, and a restore of many files can push the replication queue past its
1024-entry bound, after which the remaining files are only re-discovered by the
reconciliation walk. Plan a large cluster restore for a quiet window.

The restore request gains `mode`. `merge` is the default and today's behaviour:
additive, which on a cluster resurrects every file retention, compaction or the
delete API removed since the backup, on every node. `replace`, cluster nodes
only, first removes the current manifest entries of every database the backup
holds (reason `restore:replace`; the local-delete workers unlink the copies on
every node, and on shared storage the restoring node deletes the objects after
their entries), except the paths the restore is about to write, which are
overwritten and re-registered instead. Files the manifest does not list are
left untouched, and on a shared backend the replaced files' hot tier rows on
the other nodes are not retired until their next tier scan, as after a
retention delete. It refuses to remove anything when the backup is incomplete
by its own manifest (skips it could not reconcile, unaddressable files,
manifest-only files) or by what is in backup storage, and when more than 10%
of the data files the backup node listed were unregistered at backup time,
which means the backup was taken against a stale or partial manifest view. A
path in the backup whose object cannot be read keeps its current bytes and
entry. A compaction job finishing on a restored database could manifest-delete
inputs whose output the restore has just replaced, with no check that the
output is still there; compaction is now paused cluster-wide for the duration
of every cluster restore, automatically (see
[#1087](https://github.com/Basekick-Labs/arc/issues/1087) below), so the
operator no longer has to disable it or stop the compactor. Standalone nodes
refuse `mode: "replace"` with 400.

On a cluster node `restore_metadata` now defaults to false and an explicit
`true` is refused with 400, as is `restore_config: true`: the SQLite database
holds Raft-replicated tokens, this node's tier rows and the audit log, and
`arc.toml` holds `cluster.node_id`, the role, the seeds, `raft_bootstrap` and
the shared secret, so either one from a backup would give the node another
node's state. A client that sends `restore_metadata: true` explicitly by
default will be refused on every cluster node and must stop sending it. A
standalone config restore writes the config file the server loaded at
startup; see [#1466](https://github.com/Basekick-Labs/arc/issues/1466).

### Cluster restores pause compaction cluster-wide ([#1087](https://github.com/Basekick-Labs/arc/issues/1087))

Compaction commits in two Raft phases on the compactor: the output is
registered when it is written, and the inputs are manifest-deleted on a later
watcher tick, after the compaction subprocess has already deleted them from
storage, with no check in between that the output is still in the manifest. A
cluster restore whose manifest snapshot fell between the two phases raced the
job. A `replace` restore removed the compacted output (it is not in the
backup) and re-registered the backup's inputs, and the late phase then
manifest-deleted those inputs on every node, leaving the partition empty; in
either mode the late phase could unlink a freshly restored input in the short
gap before its batched register, leaving a manifest entry whose file no node
holds. Stage 0 of the backup work asked the operator to disable compaction or
stop the compactor for a `replace` restore.

Every cluster restore, `merge` and `replace`, now pauses compaction
cluster-wide before its first manifest read and releases the pause after its
last register. The restoring primary proposes the pause through Raft; every
node, readers included, stops starting compaction batches at once, lets the
batch it is running finish (a compaction subprocess is never killed for the
pause: a kill between its two commit phases is exactly the state the pause
exists to prevent), drains the manifest commits it has pending, and
acknowledges. The restore starts only once every node in the cluster node
table has acknowledged, and fails after 10 minutes naming the nodes that did
not: a long batch may still be running on them, they hold compaction commits
their completion watcher is not applying, or they are not on this release.
**Every node of a cluster must run 27.01.1 for a cluster restore**: an older
Raft leader refuses the pause outright and the restore fails at once with a
clear error; an older compactor never acknowledges and the restore fails on
the timeout. A node the RESTORING node's own registry has marked unhealthy or
dead since that node started is not waited for; a node in the cluster node
table that the restoring node has not heard from (for example right after the
restoring node restarted, or a node whose leave never reached the leader) is
waited for, and the restore fails after 10 minutes naming it until an operator
removes it from the cluster. One exception to the unhealthy/dead skip: the
node holding the compactor lease, and every node whose role can compact
(compactor, standalone) while no lease is assigned, is always waited for,
because those are the nodes that run compaction. A dead dedicated compactor
therefore fails every cluster restore on the 10-minute timeout, naming it;
remove it from the cluster, or let the lease move, before restoring. One more
case fails closed the same way: a node that held the compactor lease, lost it
while a compaction job had deleted its inputs from storage but not yet from
the manifest (a `sources_deleted` completion manifest still pending), stops
its completion watcher on the lease loss and so never drains that commit; it
never acknowledges a pause, and every cluster restore fails on the timeout
naming it. Move the lease back to that node (or restart it, when it is a
dedicated compactor) so its watcher applies the pending commit, then restore.

The pause carries a six-minute expiry from the requester's clock and the
restoring node refreshes it every 30 seconds while it runs, so a restore whose
process dies releases compaction within six minutes with no operator action
(six minutes, not less, because every node judges the expiry by its own clock
and the cluster tolerates up to five minutes of skew between nodes). During
those six minutes a restore started from another node is refused with
`compaction is already paused by <node> (restore <id>) until <time>` and
succeeds once the expiry has passed; the same node may retry at once, since a
requester takes over its own pause. If the pause stops being the restore's own
while it runs (another node took over an expired pause, or it could not be
refreshed until it expired), the restore stops at its next manifest batch and ends `failed` saying so, because
a compaction job may have raced it; take a fresh backup and restore again. On
a node that acquires the compactor lease during a pause, or a dedicated
compactor that restarts during one, the scheduler still arms itself and the
first tick after the resume runs. Compaction state is unchanged for standalone
nodes and for cluster nodes without a Raft manifest, where no pause is taken.

What operators see: restore progress (`GET /api/v1/backup/status`) carries a
`compaction_pause` field, `waiting` while the nodes quiesce, `paused` while
the restore runs, `released` once resumed, `lost` when the restore failed for
that reason; `GET /api/v1/cluster` carries a `compaction_pause` object
with the active flag, the requesting node, the reason (`restore <backup-id>`),
the expiry, the node IDs that have acknowledged (`acks`) and those the
requester is still waiting for (`pending_acks`); `GET
/api/v1/compaction/stats` carries `paused`, and a cycle that stopped for the
pause records the status `paused`. `POST /api/v1/compaction/trigger` answers
**409** `compaction is paused cluster-wide` while a pause is in force, and a
scheduled cycle that falls inside one is skipped at Info, not logged as a
failure. The `replace` restore's start-up warning that asked the operator to
disable compaction is gone.

Not in this release: backups do not pause compaction (a backup taken during a
compaction commit still has the residual window described under #1083), and
there are no operator endpoints to pause or resume compaction by hand; the
pause is taken and released by restores only. One residual window remains on
a node that acknowledged a pause and then restarts while it is in force: its
acknowledgement is already recorded, so it is not asked to quiesce again, and
in the seconds between its compaction scheduler arming and its Raft state
catching up with the leader it does not yet see the pause, so a scheduled
tick landing exactly then could start a cycle. Avoid restarting compactor
nodes during a cluster restore.

### The measurement endpoint's `where` parameter no longer rejects values that contain SQL words ([#987](https://github.com/Basekick-Labs/arc/issues/987))

`GET /api/v1/query/:measurement?where=...` pre-filters the clause for
statement-level SQL before the shared validator checks the assembled statement.
That pre-filter ran a plain substring match on the raw, upper-cased text, so it
refused any value containing a forbidden word (`msg = 'created at noon'` failed
on `CREATE`), any identifier containing one (`created_at`), and any value with a
comment marker (`note = 'a--b'`).

Keywords are now matched as whole words on a copy of the clause with its string
literals masked by the same masker the query path uses; `;` and comment markers
are still refused outside a literal, and a keyword glued to a number or a
literal (`1UNION`, `'a'union`) is still seen. The assembled statement still goes
through the shared validator with its file-I/O and replacement-scan checks,
which is where this endpoint's security boundary has been since 26.09.1.

The `xp_`/`sp_` entries are gone from this list. They were SQL Server procedure
prefixes with no meaning to DuckDB, and here they were dead code: lowercase
patterns compared against an upper-cased clause. The delete API's copy of the
same two strings is tracked separately in #1077.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#988](https://github.com/Basekick-Labs/arc/pull/988).

### Delete API WHERE validation no longer rejects SQL words and punctuation inside string literals ([#834](https://github.com/Basekick-Labs/arc/issues/834))

`POST /api/v1/delete` scans the WHERE clause for statement-level SQL before it
interpolates the clause into the DuckDB statement: forbidden keywords (`DROP`,
`UPDATE`, `SET`, ...), `;` and comment markers, and DuckDB's file-I/O table
functions. Those scans ran on the raw text, so a value that merely contained
one of those words was refused as if it were SQL: `status = 'delete-pending'`,
`action = 'update'` or `note = 'a;b--c'` could not be deleted through the API
at all.

The scans now run on the clause with its string literals masked by the same
masker the query path uses, which knows plain `'...'`, escape-string `E'...'`
and dollar-quoted `$tag$...$tag$` forms, so a literal is data whatever it says.
Backtick identifiers are normalised first, as the query path does, and the
file-I/O scan still sees an identifier-quoted call such as `"glob"(...)`. The
raw clause is what reaches DuckDB, and the unmatched-quote and
unmatched-parenthesis checks still run on it. The same syntax outside a literal
is refused exactly as before, including the escaped-quote and dollar-tag shapes
the masker was hardened against in 26.09.1 and 26.09.2.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#937](https://github.com/Basekick-Labs/arc/pull/937).

### Keep replayed WAL files until recovery flushes succeed

WAL recovery now waits for queued and direct flushes before deleting replayed
files, and retains them if any flush fails during the recovery pass, including
an asynchronous flush that finishes before the deletion barrier begins. A later
pass can retry after storage recovers. Periodic cleanup uses the unflushed
sequence floor instead of file age so a slow flush cannot lose its durable copy
(#1009, #966).

An incomplete final WAL header or payload after an interrupted append no longer
counts as a poison entry: complete preceding entries still require a successful
flush barrier before reclamation. Complete entries with invalid checksums remain
recovery failures. Checkpoints in quarantined WAL files remain part of recovery's
durability proof, so a later retry does not replay already-flushed entries merely
because their checkpoint file was renamed.

Recovery barriers now use the configured flush worker pool instead of serial
per-measurement writes, and release shard locks before waiting. Each barrier
has a total wait budget of `ingest.flush_timeout_seconds` (30 seconds by default)
or the caller's earlier deadline. The first observed flush failure stops further
admission. Unsubmitted records remain buffered; queued/in-flight writes retain
their normal per-write deadlines and WAL identities. A timeout or failed
barrier retains the WAL for a later recovery pass, including when storage ignores
cancellation. Concurrent ingestion can continue while recovery waits.

Completed failed replay attempts now survive process restarts in synced
per-file `.wal.recovery` sidecars, so repeated restarts cannot reset the poison
file quarantine threshold. Cancellation and flush-barrier outages do not add
strikes. Unreadable or unwritable attempt metadata reports an error and retains
the WAL; quarantine still preserves its original bytes and valid checkpoints.
The operational guide describes sidecar recovery and cleanup behavior.

Recovery orders ordinary WAL files by their timestamped filenames rather than
filesystem modification times. Altered or tied mtimes can no longer make it
reclaim a later checkpoint file before an earlier retained data file, which
could otherwise cause already-flushed records to be replayed on retry.

Recovery skips redundant flush barriers for batches covered entirely by durable
checkpoints. Mixed batches still fence all replayed entries, and parent
checkpoint failures still retain files with fully covered row ranges.

Forced maintenance rotation now waits for WAL queue capacity instead of
returning `ErrWALDropped` and skipping recovery under sustained queue pressure.
It preserves FIFO ordering and uses a cancellable wait bounded by
`ingest.flush_timeout_seconds`. An admitted command may finish after timeout;
the failed wait keeps recovery pending. Normal append admission is unchanged.

Tracked row recovery now honors `wal.recovery_batch_size`. Durable row-range
checkpoints let a restart skip the precise persisted portion even after a
batch-size change; the original entry is checkpointed only after its complete
flush barrier succeeds. This bounds callback batches, not whole-file decoding
or total recovery memory. Legacy content-hash entries remain at-least-once.
Before downgrading to a binary without row-range support, drain recovery with
this version: older binaries can replay partially completed entries in full.
See the [restart compatibility guide](docs/operations/wal-recovery.md#bounded-row-replay-and-restart-compatibility).

Retained and quarantined WAL files are not reclaimed by age. The
[WAL operations guide](docs/operations/wal-recovery.md#reclaiming-retained-and-quarantined-files)
now documents offline inventory, verified backup, record reconciliation and
the checkpoint-dependency gate required before an operator reclaims quarantine
space. It includes a read-only empty-WAL check and a whole-directory relocation
procedure for capacity shortages. A clean recovery result alone does not prove
quarantined data is disposable.

Contributed by [@jallegri](https://github.com/jallegri) in [#1118](https://github.com/Basekick-Labs/arc/pull/1118).

### WAL recovery follow-ups: honest shutdown attribution, an exit for a bad checkpoint, and a directory that reclaims itself

Seven findings left open when #1118 merged. None of them lost data; several had
no operator exit.

**Two metrics shipped in #1118 without being announced.**
`arc_wal_dir_bytes` is the size of the WAL directory, sampled every 15 seconds,
and `arc_wal_quarantined_files_total` counts files moved aside after repeated
replay failures. The [WAL operations guide](docs/operations/wal-recovery.md)
carries alert expressions for both.

**`arc_wal_dir_bytes` now counts every file in the WAL directory**, not just
`*.wal` and `*.failed`. The names it skipped — `.wal.recovery` attempt
sidecars, `.wal-recovery-*` write temporaries, legacy `.wal.recovered` files —
are exactly what accumulates when recovery is failing, which is the state its
capacity alert exists for. **Expect a one-time step up on upgrade**, enough to
trip a growth alert once. Legacy `.wal.recovered` files are still not reclaimed
automatically; the guide says how to remove them.

**A clean shutdown after a kept WAL file no longer reports a flush failure that
did not happen.** A startup recovery pass that retained a file, or whose flush
barrier failed, armed the retry through the same flag the shutdown WAL purge
reads — so every later shutdown logged, at `Error`, "buffer flush did not
complete cleanly on shutdown … Investigate the buffer flush errors logged
above", pointing the operator at logs that do not exist. The WAL is still
retained, which was always correct; the message is now a `Warn` naming the
outstanding recovery pass. A genuine flush failure keeps the `Error`, and keeps
it when both are true.

**A row checkpoint describing rows beyond its entry no longer stalls recovery
permanently.** It abandoned the whole pass without recording a replay attempt,
so the quarantine threshold never advanced, the file was never moved aside, and
every later WAL file was left unread — with no way out. It is now treated as the
poison entry it is: the file is kept, struck, and quarantined on the third pass,
with its bytes and checkpoints preserved. Clean files already replayed in the
same batch are now reclaimed rather than retained alongside it.

**Recovery reclaims its own residue.** Every pass now removes
`.wal-recovery-*` temporaries left by a crash and attempt sidecars whose WAL
file is gone — every purge path deleted the `*.wal` and left the sidecar
behind, for the life of the node. A sidecar is kept when its WAL file merely
cannot be stat'ted, and when a quarantined sibling exists, because it is the
strike history those files are reconciled against.

**Recovery's parent checkpoints wait for WAL queue capacity** instead of
returning `ErrWALDropped`, the same trade the forced maintenance rotation took
in #1118 — same pass, same queue pressure, but finalization was left on the
dropping path. `ingest.flush_timeout_seconds` bounds **admission** to the queue;
a timed-out or refused admission retains the file for a later pass. Once a
checkpoint is admitted, recovery waits for it to be written: the entry is
written either way, and abandoning the wait would skip the pending-identity
release, pin the unflushed sequence floor, and stop the periodic purge from
reclaiming any WAL file for the life of the process. The ingest flush path is
unchanged and still admits without blocking.

**A repaired WAL file is more likely to start a fresh attempt series.** The
attempt sidecar now records a CRC of the file's final 64 KiB alongside its size
and timestamp. On a filesystem with one-second timestamp granularity — some
network mounts, which is where repairs happen — a file rewritten in place to the
same length within one tick looked untouched and inherited its strikes. A repair
confined to the middle of a large file still does; remove the file's
`.wal.recovery` sidecar to reset it explicitly. The sidecar format version is
unchanged, so an earlier binary still reads it.

**New: `arc_wal_partial_row_recovery_pending`.** `1` while the last recovery
pass retained a file for which at least one row range of a parent entry was
replayed — the state an older binary would replay in full — and `0` once a pass
ends with none. This makes the guide's "finish recovery before downgrading"
checkable instead of advisory. Read it with the quarantine counter and the
recovery error lines: a pass that cannot read a file at all reports `0` while
that file's residue remains.

### The tiering files endpoint rejects an invalid `limit` ([#1135](https://github.com/Basekick-Labs/arc/issues/1135))

`GET /api/v1/tiering/files?limit=-1` returned a 500 and logged a stack trace. The handler read the
limit without validating it and then sliced the result with it, and the guard it used
(`len(files) > limit`) can never be false for a negative value — so `files[:-1]` panicked, even when
no files were tiered. It needed an authenticated admin on a licensed deployment, and Arc's panic
recovery turned it into a 500 rather than a crash.

Invalid limits now return 400, and the slice is clamped independently so the panic cannot return if
the validation is ever moved.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#1051](https://github.com/Basekick-Labs/arc/pull/1051).

### A replica refreshes a file whose manifest content changes while it is being pulled ([#798](https://github.com/Basekick-Labs/arc/issues/798))

The puller deduplicated arrivals by path: a manifest update for a path whose
pull was already in flight was dropped as a duplicate, and a rewrite that kept
the file's size was then skipped by the size-only presence check, so a reader
kept serving the old bytes until the next content change of that path. The FSM
now signals a content change (a different checksum or size) separately from the
plain registration, the puller hands an in-flight path over to the newest
version instead of dropping it, a content change bypasses the size check, and a
forced refresh that fails or is dropped is remembered by path so the next
arrival of that path is forced too. A verified copy of the previous version is
kept until its successor is installed, so a failed refresh never leaves the
node without the file. On shared-storage clusters, where every node reads the
writer's own object, forced refreshes are off: the object is never re-uploaded
by a reader.

A delete followed by a same-size re-registration of the same path is now
refreshed too. The re-registration arrives as a new file, not a change, so it
was not forced; and a reader's local delete waits a short grace period before
it unlinks, so when the path was listed again by then the delete kept the
local copy, which then passed the size check with the old bytes. The delete
now asks for a refresh of the path whenever it keeps a copy for that reason.
The copy is never unlinked there, because the node may hold the only one: it
stays readable until the new version is installed.

That refresh hashes the local copy first and downloads only when it is not the
listed version. A node that never held the old version pulls the new one
through the registration, and a re-registration with unchanged content needs
nothing, so neither downloads the file twice. The node logs it (the first
time, then at every power of two):

```
Local delete skipped: the manifest lists the path again; refreshing the local copy unless it already is the listed version
```

Three counters in `replication_catchup_status` on `GET /api/v1/cluster` (and
in the query gate's 503 body) show this working. They count every forced
refresh, including the content-change refreshes above, not only this path:
`forced_refresh` counts forced refreshes that downloaded the file,
`refresh_verified` counts refreshes whose local copy was already the listed
version, and `refresh_pending` is the number of failed or dropped forced
refreshes waiting for their path's next arrival.

If the refresh cannot be queued, the line is a warning, and the local copy
may be the previous version for a while. When the node is shutting down the
refresh is lost until the next start's walk; when the pull queue is full it is
retried on the path's next arrival or the next manifest walk.

Since [#975](https://github.com/Basekick-Labs/arc/issues/975), Arc's DELETE
rewrites and compaction publish new files under fresh paths, so these shapes
come only from producers that reuse a path: restores, imports, edge-sync
after the hub deleted the path, and external Raft writers. A forced refresh
that fails or is dropped, then a restart, and a rewrite a node learns of from
a Raft snapshot (which fires no callback) are covered by
[#1182](https://github.com/Basekick-Labs/arc/issues/1182) (below): the next
manifest walk finds such a copy stale and refreshes it. With
`cluster.replication_catchup_enabled = false` there are no walks, and nothing
retries it.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#907](https://github.com/Basekick-Labs/arc/pull/907), and by
[@jallegri](https://github.com/jallegri) in
[#1173](https://github.com/Basekick-Labs/arc/pull/1173) (delete then
re-register).

### A restarted replica no longer trusts a stale copy of the same size ([#1182](https://github.com/Basekick-Labs/arc/issues/1182))

On a cluster with local storage, when the manifest listed new content at a
path a node already held at the same size, and the node's refresh of it
failed (no peer reachable that held the new bytes), a restart lost the retry:
the startup catch-up accepted the old copy by its size, the query gate went
green, and the node served the old bytes until something else rewrote the
path. A restore of a backup, edge-sync after the hub deleted a path, and name
collisions from writers on an older release produce such content changes.

Each node now records what each local copy holds: the checksum it pulled,
hashed, or wrote itself (a flush, a compaction output), kept while the file
stays the same one. The startup catch-up and every periodic walk compare that
with the manifest. A copy known to hold other bytes is pulled again; a copy
whose content the node does not know is hashed first and pulled only if it
differs. These refreshes count in the startup batch, so with
`cluster.query_gate_on_catchup` the gate stays red until each is settled, and
a node that cannot refresh one reports it in `catchup_failed`. A copy is
never removed by this: when no peer serves the listed bytes, the node keeps
its copy and retries on the next walk.

**Upgrade cost.** A node upgraded from an earlier release has no record of
its copies, so its first start hashes every local file once at the manifest
size, on the pull workers, in the background (a full read of the node's data:
roughly 5 to 15 minutes per 500 GB). Pulls of new files queue behind it; with
the query gate on, the gate waits for it. Later starts hash only what changed.

Also:

- A pull that found no candidate peer in any attempt finished as if it had
  succeeded, so the startup gate could go green over a file the node never
  fetched. It now counts as failed (`no_peer_exhausted`).
- A hash of a local copy is bounded by a 30 s stall, not a deadline, so a
  large copy on a busy disk is read to the end instead of downloaded again.
- New counters in `replication_catchup_status`: `listed_unvouched` (walk
  entries refreshed because the node could not vouch for the copy),
  `verify_unreadable` (hashes that could not be read; the next walk retries)
  and `no_peer_exhausted`. `GET /api/v1/cluster` reports `local_copy_trust`:
  `recorded`, or `size_only` when the record cannot be kept (the tracking
  journal failed or is not open yet).
- The tracking journal (`seen-local.log`, from #1071) moves to version 2; a
  version 1 journal from a pre-release build is loaded with every checksum
  unknown, so those copies are hashed once.

### Compaction reuses local inputs and validates DuckDB paths ([#969](https://github.com/Basekick-Labs/arc/issues/969))

For local storage, compaction reads Parquet inputs from their existing paths
instead of first copying them into the compaction temporary directory. This
removes the input-staging copy, but DuckDB still reads the inputs and compaction
still writes and uploads its output. Reads therefore remain on the data volume;
operators who place `compaction.temp_directory` on a separate volume should
account for that change.

The input-copy byte figure in the acceptance harness is calculated from input
file sizes, not measured storage I/O. Arc's parent `/metrics` endpoint does not
aggregate storage counters from compaction subprocesses, so those counters
cannot measure compaction reads or writes. The optional large-partition run
records cycle wall time and sampled peak temporary-directory usage; its 10 ms
sampling interval can miss short-lived peaks.

In a single same-settings comparison with 42 Parquet inputs, 1.05 million rows
and 9,130,407 input bytes, the baseline and this change completed in 0.905 s
and 0.894 s. Sampled temporary-directory peaks were 1,878,338 bytes and
356,610 bytes, respectively. The harness calculates 9,130,407 baseline input
copy bytes versus zero with local-path reuse; it does not measure total disk
I/O. This one run does not establish a throughput improvement.

An input that disappears before validation is skipped safely. If it disappears
after validation and before DuckDB reads it, the batch fails as a permanent
missing-input error without adaptive splitting or retries; surviving inputs
remain in storage for the next cycle to rediscover. A permanent failure is now
recognised in either the subprocess stderr or the parent error, rather than only
in stderr, so such a batch stops immediately instead of spending its full retry
budget.

Because DuckDB reads a `read_parquet()` path as a pattern, a path containing a
glob metacharacter could resolve to a different file than the one Arc named and
validated. Such a path now **fails the compaction job** rather than skipping the
file, and the two operator-set directories that reach `read_parquet` are
refused at startup instead — see the upgrade note at the top of these notes. A
skipped file would have failed identically on every later cycle, so compaction
would never have progressed while every metric showed jobs completing.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#990](https://github.com/Basekick-Labs/arc/pull/990).

#### The output side (maintainer follow-up, not part of #990)

Everything above is the input half of #969. The **output** side no longer copies
either. On a local backend a compacted file is moved into the storage root rather
than copied into it, so publishing the output costs a rename instead of a full
read plus a full write of the file.

Two conditions apply, and both are ordinary configuration rather than edge
cases:

- **The backend has to be local.** On S3 and Azure the upload is a network
  transfer and nothing changes.
- **`compaction.temp_directory` has to be on the same filesystem as
  `storage.local_path`.** They are independently configurable and pointing the
  temporary directory at a larger or faster volume is the reason the key exists.
  When they are on different filesystems the rename is impossible and Arc copies
  exactly as before, logging one warning per job that names both paths. If you
  see that warning and want the saving, co-locate the two paths.

Do **not** put `compaction.temp_directory` inside `storage.local_path` to
satisfy that condition. Everything under the storage root is treated as storage:
the job temporary directory is listed as a database by `SHOW DATABASES`, and a
backup of the instance sweeps it, so a half-written compaction output can end up
in a backup. An ordinary query is not affected -- it globs
`<database>/<measurement>/**/*.parquet`, which a job directory at another top
level does not match. Use a sibling directory on the same filesystem, which is
what the defaults (`./data/arc` and `./data/compaction`) already are.

In clustered deployments the output is still read once more, to compute the
SHA-256 the completion manifest publishes and peers verify their pulled bytes
against. That digest is now taken before the output is published rather than
after, because a moved file is no longer there to read. So on a cluster this
removes one of two passes over the output, not both; a single node writes no
completion manifest and skips the digest entirely.

`arc_storage_write_bytes_total` counts a moved file exactly as it counted a
copied one, so the counter is unchanged by this work and is not where the saving
shows up.

## Internal changes

These do not change how Arc behaves. They are here because the codebase is the
thing a new maintainer has to learn, and a refactor that moves a decision from
four places to one is worth knowing about before you go looking for it in the
old place.

### The buffer refuses a database or measurement name containing "/" ([#1475](https://github.com/Basekick-Labs/arc/issues/1475))

Buffer, coverage and tier-cache keys are `database + "/" + measurement`, and
storage paths are built the same way. Ingest and cluster code split buffer
keys at the first `/`, which needs a database name with no `/`; exact lookups
and storage paths need a measurement name with no `/`. Every ingest entry
point already guarantees both by validating names, but entry points have
missed the database check before (#300, #993, #1010). The buffer now refuses
either name itself, before any WAL work, as a content refusal, so a future
entry point that skipped validation cannot make two series share a key. No
supported write path can reach it: edge-sync spoke namespaces, whose database
names do contain `/`, reach storage as files and never pass through the
buffer.

A test now also pins how the Iceberg exporter reads a metadata file's version
(`00004-<uuid>.metadata.json` and `v4.metadata.json` are both version `4`): it
is the integer `version-hint.text` holds, so the leading zeros are dropped on
purpose. Both are the hardening #750 handed to #1475.

### Hourly compaction file-count guidance (#793)

Documented `compaction.hourly_min_files` as an independent per-partition floor,
not a value derived from `ingest.max_buffer_age_ms`. Actual file cadence also
depends on traffic gaps, row limits, schema changes and flush backpressure; the
default remains 10.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in
[#1370](https://github.com/Basekick-Labs/arc/pull/1370).

### The unused `license.enabled` setting is removed ([#1195](https://github.com/Basekick-Labs/arc/issues/1195))

`license.enabled` (`ARC_LICENSE_ENABLED`) was read into the config and never
used. Enterprise is enabled by `license.file_path` or `license.key` alone, and
that does not change. A leftover `enabled` key or environment variable is
still ignored, and Arc starts as before.

Contributed by [@lecodev-26](https://github.com/lecodev-26) in [#1241](https://github.com/Basekick-Labs/arc/pull/1241).

### `PurgeFlushed` and WAL recovery are now driven by the same test

No test in `internal/wal` ran the writer's sequence-floor purge and a recovery
pass together, and that interaction is where the row-range proof hazard lives: a
flush writes a row-range checkpoint into the current file while the entries it
describes sit in an earlier file recovery retained. A retained file from a
previous process is not in the writer's own file order, so the purge walk never
sees it and never stops at it — and the file carrying its only proof is an
ordinary candidate. Deleting that proof makes the next pass replay rows already
in storage.

The #1118 fix-up pinned the proof file; nothing pinned the pin.
`purge_recovery_interaction_test.go` now drives both, and fails if the
`foreignProof` guard is removed. A companion test takes the inverse direction —
full coverage replays nothing and reclaims both files — so the first cannot pass
for the wrong reason.

Two tests were also weaker than their names.
`TestDirectoryBytesIncludesWALAndQuarantinedFiles` pinned the suffix exclusion
for arbitrary non-WAL names but had no recovery-artifact fixture, which is the
exclusion that mattered; it now covers sidecars, write temporaries and legacy
`.recovered` files, and is named for what it asserts.
`TestRecoveryAttemptMetadataFailureKeepsWAL`'s `directory` case read as
unwritable-metadata coverage while only exercising the read path — `os.ReadFile`
rejects a directory before the write is reached — and is renamed accordingly.
Those cases also pin that a malformed sidecar still stalls a pass, which is
deliberate and now says so: the error names the file and removing the sidecar
clears it, where a bad checkpoint lives inside WAL bytes an operator cannot edit.

### A quarantine test waited on the counter, not the bookkeeping it asserts ([#1146](https://github.com/Basekick-Labs/arc/issues/1146))

`TestPullerQuarantineKeepsTheQueryGateClosed` reddened `Build & Test`
intermittently with `catchupInflight = 1, want 0`. Nothing was wrong with the
puller. `totalInvalidPath` is incremented inside `processEntryOnce`, while the
in-flight bookkeeping the test asserts is released by that function's deferred
`finishEntry` — so a wait predicate watching only the counter can return in
between, and the assertion reads the pre-drain state.

This is [#972](https://github.com/Basekick-Labs/arc/issues/972) a second time.
That issue diagnosed the identical mechanism in
`TestPullerQuarantineIsIdempotentAcrossReenqueues` and fixed it by adding
`inflight_count == 0` to that test's wait; the two sibling tests in the same
file that share the window were never updated. Both now gate on the
bookkeeping, and `TestPullerQuarantinesUnusableKeyWithoutTouchingPeers` needed
it too — its shared-metric assertion sat in the same gap, three statements wide.

The assertions keep their teeth: `waitStats` returns the last snapshot on
timeout rather than failing, so removing the real drain in `finishEntry` still
fails both tests on the full deadline. Verified, along with the mechanism, by
temporarily widening the window — a 300 ms sleep after the counter increment
fails both tests 10/10 before the change and 0/10 after.

### Cold files are a third object set, not merged into the hot one ([#1086](https://github.com/Basekick-Labs/arc/issues/1086))

The obvious implementation — add the cold listing to the file list a backup
copies — is wrong, and wrong in a way no test without a real cold tier can see.
**Five** mechanisms in the cluster path would each destroy or misreport a
merged cold file:

- the Raft manifest adapter drops every cold entry, so a scope whose data is
  all cold contributes zero manifest entries while the listing is non-empty,
  and `crossCheckManifest` **refuses the run outright**;
- that same cross-check holds back any data file the manifest does not list,
  which every cold file is by construction (tiering deletes a migrated file's
  entry as phase 2 of the migration);
- the end-of-run recheck **deletes the backup object** for any sidecar row the
  fresh manifest snapshot lacks — so a cold file would be copied and then
  removed again, on every cluster backup, with the status reporting success;
- the recheck's last pass would label a skipped cold file "reconciled: not
  missing data", because a cold file is never in the manifest;
- a path the cross-check held back as unregistered is reported as "not backed
  up" even when the cold tier supplied it — and since that count gates
  replace-mode restore past a 10% skip ratio, a deployment with a backlog of
  failed hot deletes could have had a complete backup refuse its own restore.

So `backupLeg` has a third set, `cold`, which bypasses all five, and
`ManifestFile.Tier` on the sidecar carries the per-file answer the restore
needs. There is a regression test per mechanism.

**The cold listing is authoritative and the tier rows are the cross-check,
which is the inverse of the hot path.** `crossCheckManifest` trusts the Raft
manifest over this node's listing, because on a cluster the listing can be a
stale view of a store several nodes write. The cold listing is not that: it is
this node reading the cold store directly. Hence an object with no row is
carried and counted rather than held back.

**Mid-migration, the cold copy wins.** A migration copies to cold before
releasing the hot copy, so one path can be in both listings. Taking the hot
side loses the file: if the migration completes during the run, the manifest
entry goes away and the recheck deletes the hot copy's backup object, leaving
the file in neither set. The cold object is canonical and nothing in Arc ever
deletes one, and the tier row has already been flipped to cold by that point,
so cold is both the safe and the consistent choice.

**The restore's cold path is separate on purpose.** A cold file is registered
in no Raft manifest — registering one would create an entry the next tiering
cycle wants gone, and would make peers try to replicate a file that is not in
hot storage — and it is not reported through `RecordRestoredFile` either, whose
handler stats the hot backend and whose upsert is guarded to hot rows, so a
cold report is dropped silently twice over. It has its own recorder, which
stamps `migrated_at` as **now**: only a row inside the orphan-reconciliation
window lets a stale hot copy at the same key be cleaned up, and the cost is one
hot-side existence check per restored file per cycle until the rows age out.

`RecordColdFile` also gained a `quarantined_at IS NULL` guard and now reports
whether it wrote. That is **defensive** rather than a fix for an active bug:
the old query had no `WHERE` at all, so it would set `tier = 'cold'` on a
quarantined row, but it never cleared `quarantined_at`, and the condition needs
a key some backend still lists while tiering has given up on it. The guard
earns its place because stage C adds a second caller — a restore recording a
row for a file it has just written — where acting on a quarantined path would
be a new way to lose the record of an unusable key.

Two shapes worth knowing for whoever tunes this next. The cold walk loads every
non-quarantined cold row once per backup, through a narrow two-column query
rather than the full-row accessor, because that load sits on the one shared
SQLite connection and so blocks auth, audit and tier registration while it
runs. And the restore writes one row per cold file **synchronously** — the
asynchronous tier-event path cannot be used, since its handler stats the hot
backend and its upsert is guarded to hot rows — so a large cold restore is one
fsync per file. Batching those writes is the obvious next improvement.

### Immutable DELETE rewrite filenames are normalized centrally

Partial DELETEs use the `_rewrite_<unixnano>.parquet` naming convention. The suffix parser now lives in `internal/storage` and is shared by DELETE, compaction, tiering and file-time pruning, so rewritten daily and compacted files retain their logical classification and timestamps.

### The cold-file marker's shape ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

`backup.ColdCounter` is a one-method interface (`CountColdFilesByDatabase`)
wired from the tiering manager in `cmd/arc/main.go` beside `SetTierLookup`, nil
when tiering is off — the same pattern #1084 used for the fully-cold database
check, and for the same reason: the backup package should not import tiering.

Because that wiring sits inside the tiering block, which is gated on the
**licence** and not only on `tiered_storage.enabled`, an unlicensed node wires
no counter at all. That is left as it is: the alternative is building a tiering
`MetadataStore`, schema included, on an unlicensed node, which is the boundary
the licence pattern exists to hold. The field's doc comment and the operator
note above both say so, since an absent field would otherwise read as "nothing
was migrated".

Three decisions worth knowing before changing this code.

**One grouped query, not one per database.** `SELECT database, COUNT(*) ...
WHERE tier = ? AND quarantined_at IS NULL GROUP BY database`. The set a caller
wants is "every database with cold rows", and that is *not* the backup
inventory — a fully cold database has no hot files, so it is absent from both
the listing and `manifest.Databases` while still holding the rows this counts.
Grouping means the caller never has to discover the set first, and it cannot
half-fail the way N point queries can, which is what lets the count be
all-or-nothing instead of needing an "unavailable" flag. The plan is a seek on
`idx_tier_files_tier` plus a temporary b-tree over only that tier's rows,
measured and unchanged by `ANALYZE`; `idx_tier_files_database_tier` does not
serve it, because `tier` is that index's second column. It is not the fastest
plan available and the comment in the code says so: a covering index on
`(tier, database, quarantined_at)` is about 6x faster, and is deliberately not
added because it costs ~7% of the database file and a sixth b-tree on a table
the ingest flush path writes to, to save ~18 ms once per backup run. The note
records the scale at which that trade flips.

**Per leg, by the same rule the data follows.** Since a backup can fan out to
one destination per database, the count is attributed with `run.legFor`, so a
database routed to the audit target has its gap on that target's manifest and
nowhere else, and `mergeRunManifests` sums the legs into the run-level view
every consumer reads. A new per-leg counter that is added to the struct but not
to that merge reads zero on every multi-leg backup and correct on every
single-leg one, because the one-leg path is a clone — the kind of bug that
looks right in most tests.

**Informational by construction.** A counting failure warns and the backup
completes: the count describes data the backup was never going to carry, so
failing the run would trade a healthy backup for no backup over a diagnostic.
There is a comment at the replace-mode incompleteness refusal explaining why
this count deliberately does not join it; a regression test fails if it ever
does.

### Stale compaction manifests follow normal recovery ([#750](https://github.com/Basekick-Labs/arc/issues/750))

Corrected the `ManifestMaxAge` and recovery comments and an earlier release note
that said manifests over seven days old are deleted. Age triggers an investigation
warning; recovery still validates the output and applies its usual cleanup and retry rules. A
regression test verifies input cleanup for an eight-day-old manifest with a
valid output. Runtime behavior is unchanged.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#921](https://github.com/Basekick-Labs/arc/pull/921).

### Compaction database-name sanitization is documented as non-unique ([#750](https://github.com/Basekick-Labs/arc/issues/750))

The `sanitizeDBForName` comment now explains that replacing slashes with dots
produces a path-safe token, not a unique database identity: spoke IDs may
contain dots, so distinct pseudo-database names can collide. A regression test
documents the collision; runtime behavior is unchanged.

Contributed by [@efegokdemir](https://github.com/efegokdemir) in [#920](https://github.com/Basekick-Labs/arc/pull/920).

### Iceberg export moves to iceberg-go v0.7.0

The library Arc's Iceberg exporter is built on goes from v0.6.0 to v0.7.0. Arc's own code needed no
changes for it; what changes is behaviour underneath.

**The manifest pile gets smaller on its own.** v0.6.0 carried a manifest whose entries were all
tombstones into every later snapshot forever, so a removal pass added two manifests. v0.7.0 drops
it, and parallelises the manifest scan. Measured on the same 300-file table, one file replaced per
pass:

| removal passes | manifests (v0.6.0 → v0.7.0) | scan plan (v0.6.0 → v0.7.0) |
|---|---|---|
| 16 | 33 → 18 | 6.87 → 2.99 ms |
| 64 | 129 → 66 | 18.62 → 7.12 ms |

Per accumulated manifest, a reconcile pass costs ~0.24 ms instead of ~1.6 ms.

What remains for the manifest collapse from #1106 above to handle is narrower than that table
suggests, and worth knowing if you are watching a warehouse:

- A pass that **only adds** files adds one manifest, and those accumulate until the collapse merges
  them. Verified on a running node: the count climbed 2, 3, 4 … 12 over twelve append passes, then
  the next pass merged them back to one.
- A pass that **only removes** files usually adds nothing at all, because v0.7.0 drops a manifest
  its removal leaves empty rather than carrying it forward. Retention on a quiet measurement no
  longer grows the set.
- A pass containing an **in-place rewrite** — what a `DELETE` that matches only some rows of a file
  produces — already merges the whole manifest set as a side effect of re-registering the rewritten
  path, and has since #633. Those passes need no collapse and do not get one.

So the collapse is the backstop for append-only growth, and a table that is both written and deleted
from reaches the threshold slowly or never. The `.avro` count on disk is unchanged by this upgrade,
so the orphan sweep from #835 is still the only thing that reclaims metadata files.

**Snapshot-expiry defaults changed upstream, and Arc is unaffected.** The retention keys moved to a
`history.expire.*` prefix (with fallbacks) and the default maximum snapshot age went from
effectively forever to 5 days. Arc passes an explicit age cutoff on every expire, so
`iceberg.retain_snapshots` remains the only thing that decides how much history is kept.

**A dotted database name is exported through v0.7.0's encoded catalog key.** Arc used to build one
Iceberg namespace component per database, `<prefix>_<database>`, folding an edge-sync spoke's
separator into a dot — and v0.7.0 addresses a namespace whose component contains a dot by a
different catalog key than the plain dotted string. The bump shipped two refusals so nothing was
published broken: a dotted `iceberg.namespace_prefix` at startup, and a dotted database per
measurement.

The per-database refusal is gone, because the cause is fixed — see the #1129 entry above for how
spoke namespaces are built now and how tables already published under a dotted one are migrated.
The `iceberg.namespace_prefix` refusal stays: a prefix is a free choice that would put the encoded
spelling on every database on the node, and a prefix without a dot costs nothing.

**New transitive dependencies.** v0.7.0 pulls in OpenTelemetry's API, RoaringBitmap and geospatial
encoders for features Arc does not use (deletion vectors, geometry columns, remote scan planning).
No telemetry is registered or emitted: the OpenTelemetry **SDK** is not in Arc's dependency graph,
the only instrumented path requires a scan-planning mode Arc never selects, and the metrics reporter
defaults to a no-op. There is no new network traffic and no new log output.

### Iceberg manifest merging no longer leaves its tuning on the table (#1106)

`manifestMergeOn` sets `commit.manifest.min-count-to-merge=2` and
`commit.manifest.target-size-bytes=1 GiB` for the commit in flight. iceberg-go v0.6.0 has no way to
remove a property from a transaction, so those keys stayed on the table after the commit; until now
that only happened on tables that had hit #633, and from this release nearly every exported table
commits through that path. They are inert for Arc, but they are visible table properties that
another engine writing the table would honour as if Arc had chosen them for it. `manifestMergeOff`
now writes iceberg-go's own defaults back (100 and 8 MiB).

The per-file fallback for day-straddling files (`replaceDataFilesOneByOne`) stages one snapshot and
one manifest per added file, and the path is sticky — a straddling file is never registered, so it
is back in the diff on every later pass. It takes the manifest merge too; it was the package's
heaviest manifest producer and would otherwise have been the one path exempt.

`internal/iceberg/deleted_manifest_cost_test.go` is the measurement harness behind the figures
quoted for #1106, from 30 to 10 000 live files. It is opt-in — set
`ARC_ICEBERG_MANIFEST_BENCH=1` to run it — because CI runs the whole suite under `-race` without
`-short`, and these tests take minutes each.

### One shared constructor for storage backends ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Arc talks to three kinds of storage (a local directory, S3 and compatible
stores, Azure Blob Storage) and it needed one of them in four unrelated places:
primary storage, the tiering cold tier, the compaction subprocess, and the
backup destination. Each place had written its own `switch` on a backend name,
so the answer to "which backend names does Arc accept" lived in four files and
differed between them: primary storage took five spellings (`local`, `s3`,
`minio`, `azure`, `azblob`), the cold tier took two, the subprocess took three.

There is now one constructor, `storage.NewBackend`, in
`internal/storage/factory.go`. It takes a `BackendSpec` naming the type and
carrying the backend's own config struct, and it returns a `storage.Backend`
or an error. It deliberately does nothing else: it does not log, does not
register anything for shutdown, and does not decide whether a failure should
stop the process. Those three differ at every call site and they stayed there.
Primary storage still treats a failure as fatal and registers the backend to be
closed at shutdown; the cold tier still logs the failure and runs on without a
cold tier; the backup manager still returns the error to its caller.

The reason this was worth doing is a specific bug class, not tidiness. A Go
interface holding a nil pointer is not equal to nil, so a constructor that
returns `(*S3Backend)(nil)` alongside an error, stored straight into a
`storage.Backend` variable, produces a value that passes `!= nil` and then
panics on first use (#713). The cold-tier code carried a hand-written guard
against exactly this, twice, with a comment naming the three places that check
`coldBackend != nil` — the startup tier scan, the file drainer's existence
probe, and the query router's cold glob. That guarantee is now part of the
constructor's contract and is covered by a test, so the next caller inherits it
instead of having to know about it.

Two things stayed where they were, both on purpose:

- **The compaction subprocess kept its own factory here.** It has since moved to
  the shared constructor too, receiving the parent's credentials in its job
  (#1104, under Bug fixes).
- **Credential refreshing is unrelated to this code.** The refresher
  (`internal/database/credrefresh.go`, from #600 and #601) refreshes *DuckDB
  secrets* so the query engine can keep reading an object store with temporary
  credentials. It never touches a `storage.Backend`: the write path uses either
  static credentials or the AWS SDK's own self-refreshing chain, which is why
  writes never suffered from the bug that motivated it.

This was the first of several changes behind per-database backup targets
([#1085](https://github.com/Basekick-Labs/arc/issues/1085)): a backup
destination is a `storage.Backend`, so named remote targets are mostly a
configuration and routing problem once there is a single place that turns a
destination description into a backend.

### The Azure prefix reaches four places that do not go through the backend ([#1102](https://github.com/Basekick-Labs/arc/issues/1102))

Worth knowing if you ever add a field to a storage backend, because the
compile-time safety you would expect is not there.

Adding the prefix to `AzureBlobBackend` and routing its fourteen
key-taking methods through `prefixedKey` is the easy half. The dangerous half
is that four places build an Azure location or hand an Azure configuration to
something else **without going through the backend object**, so each one keeps
compiling and starts being wrong the moment the prefix is non-empty:

- `AzureBlobBackend.ConfigJSON` and the azure case of
  `internal/compaction/subprocess.go`. Compaction runs in a separate process
  that rebuilds the backend from that JSON. The S3 case had already carried a
  comment about this; the Azure case had never been tested at all. Missing the
  prefix here reroots compaction at the container root, so it reads nothing or
  writes output where queries do not look.
- `iceberg.DefaultWarehouse` in `internal/iceberg/paths.go`, which type-switches
  on the backend to build the warehouse root. Missing the prefix would put table
  metadata at the container root while the data sits under the prefix. This one
  is **not reachable today**: `config.Load` refuses `iceberg.enabled=true`
  unless the primary backend is local, so the object-store arms of that switch
  are dead code until Iceberg export supports object stores. It is fixed anyway,
  because the next person to lift that restriction should not have to find it.
- `azureSecretScope` in `internal/database/duckdb.go` and the sandbox allowlist
  in `internal/database/sandbox.go`.
- Two `cmd/arc/main.go` call sites, for the primary store and the cold tier.

This is the same failure the broken `iceberg.warehouse` key was
([#534](https://github.com/Basekick-Labs/arc/issues/534)): an expression that
is correct only while a new key sits at its default. The defence is the same
one that caught it here, which is running the binary with the key set to
something other than its default.

`ValidateS3Prefix` is now `ValidateObjectPrefix` in
`internal/storage/objectprefix.go`, with no alias, because all three backends
share it and two names for one function is how the next reader comes to believe
there are two rules.

### What a remote backup destination changed underneath ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Three things are worth knowing if you work on backups next.

**The partial-write cleanup is safe for a reason that is easy to misstate.**
When a backup write fails, Arc deletes the destination key. That is safe because
every key it can be handed lives under a backup identifier minted by the run in
progress, and nothing else in Arc writes that shape. It is **not** the overlap
refusal that makes it safe, although an earlier draft of this work said so in
four places. The refusal exists for the recursion and the reconciliation sweep
described in the upgrade note.

**Listing backups no longer walks the whole destination.** It used to list every
object and match on a filename suffix, which on a remote target meant
enumerating the bucket and would match anything else that happened to share the
prefix, including the cold tier. It now enumerates directories and filters on
the backup identifier shape.

A trap for the next person: `List` does not mean the same thing on every
backend. The local one treats its argument as a directory to walk; the
object-store ones treat it as a key prefix to match. A listing under a `backup-`
prefix would have returned everything on S3 and nothing locally. `ListDirectories`
is the one listing whose meaning is identical across all three.

**Two spellings of the same object store used to compare as different stores.**
The default AWS endpoint and an explicit regional one for the same bucket are
the same place, and the overlap check did not know it. Endpoints are now
canonicalised: AWS regional spellings fold together, a virtual-hosted bucket
label is stripped, and an Azure account is preferred over the endpoint it can be
derived from. Deliberately not folded: a different AWS partition, a hostname
that merely ends in something that looks like an AWS suffix, and any self-hosted
store, because treating two of those as one location would refuse a legitimate
configuration.

### Backups are written per leg rather than per destination ([#1085](https://github.com/Basekick-Labs/arc/issues/1085))

Per-database routing turned one destination into a set, and the refactor that
made that possible is worth knowing before you go looking for the old shape.

**A run is now a set of legs.** `backupRun` holds one `backupLeg` per target it
routes to, and a leg owns its destination, its `Manifest`, its `dbMap`, its
`sidecarBuilder`, its `skipTally`, its key headroom and its own state and data
skip counters. The run owns `Progress`, the run-wide skip ratio, the cluster
cross-check and the index. Files are partitioned into legs **up front**, before
any copy, and every copy function takes a leg: routing inside
`streamBackupFile` would make the destination a function of the path at every
write and force a per-file lookup of the right manifest, sidecar and tally.

A leg carries a back-pointer to its run so each copy function takes one
argument. That is not tidiness: with the run and the leg as separate
parameters a caller can pass a leg belonging to another run, and the per-target
progress is then published for a leg nothing is writing to — a mismatch that
compiles, runs, and reports plausible numbers for the wrong destination.

The state-vs-data skip split used to be a global subtraction — snapshot
`progress.SkippedFiles` after the state copies, subtract. That assumed every
state copy precedes every data copy, which per-leg interleaving (A-state,
A-data, B-state, B-data) breaks, so each leg counts its own and
`progress.SkippedFiles` stays a run-wide total used only for the gauge and the
ratio. The out-of-root warehouse's skips now stay out of both by construction,
where the subtraction had to take them back out.

**`destination()` is gone.** The no-argument accessor stage B2b-1 introduced is
replaced by `defaultDestination()` and a `backupTarget` value threaded through
every call site, including the ones that were never `destination()` callers and
are equally per-target: `describeDestination` (now `backupTarget.describe`),
the key-headroom arithmetic (now on `backupTarget`, because two targets with
different prefixes have different usable source-key lengths), the sidecar
writes, and the restore's reads.

**`Manager.targets` holds only the NON-default targets, and is nil for both
single-destination shapes** — no target configured, and exactly one target,
where the default must be it and routing is a no-op. `defaultDestination()`
re-reads the flat `backupStorage`/`targetName`/`targetKeyPrefix`/`targetRemote`
fields on every call and caches nothing. That is a compatibility rule, not a
style: twelve tests swap `m.backupStorage` on a Manager built by `NewManager`
to stand in for a counting, failing, unreachable, write-refusing or stalling
destination, and a populated map or a cached destination would route past every
one of those fakes and leave them green against a broken implementation.

**`ListBackups` fans out and returns a struct.** `BackupListing` carries the
backups, the foreign-filter count, the unreachable targets and the incomplete
runs; `ListBackupsDetailed` is what the API answers from, and
`ListBackups`/`ListAllBackups`/`ListBackupsFilteringForeign` remain as
convenience forms over the same fan-out. The fan-out is concurrent for a reason
that is not arithmetic: `withDestinationTimeout` derives from the caller's
context, so N targets cannot each cost a fresh 60 s, but **one** dead target
consumes the whole 30 s handler budget and cancels the rest — so a serial
listing could never report the unreachable target it is supposed to mark.

**The end-of-run cluster re-check is its own ordered step.**
`recheckClusterManifest` both writes and deletes after every leg has copied,
and its delete is routed to the leg that holds the file. Unrouted it would go
to the default destination, where `LocalBackend.Delete` returns nil for a
missing key and S3's `DeleteObject` is idempotent: it would **report success
while the file stayed on the routed target**, which is the double-serve #1083
and #930 exist to prevent, arrived at through a delete that looks fine.

**`replaceDatabases` runs once, over the union of every leg.** Per target it
would be unsound: `owns()` keys on the cluster entry's `Database` label for an
unscoped backup while `writes` is built from the backup listing, so one leg's
`owns()` can be true for a spoke entry whose bytes are in another leg's listing
— and it would `BatchDelete` live files the other leg is about to restore. The
two sets are only mutually protective when the file set is the whole run. The
per-leg copy loop sits inside the single compaction pause the caller already
holds, so the run still takes one pause and not one per leg.

**Known single-destination assumption, deferred deliberately.** The
single-operation lock stays one lock for the whole manager. A run spans every
target it routes to, so per-target locking would not make "back up to A while
restoring from B" safe, and `DeleteBackup` must sweep every target under one
`TryLock`. `/api/v1/backup/status` gained the per-target fields so an operator
can at least see which destinations a busy run is writing to.

`Progress.Targets` is replaced wholesale on every publish and never mutated in
place. `setProgress` publishes a shallow copy, so every published snapshot
shares that slice header and an element written after publication races every
reader of `/status` — the same reason `SkippedSample` is handed over once and
never appended to.

**A run has an ANCHOR leg, and the three default-leg restore steps read from
it.** `runRead.anchor` is the leg whose manifest asserted `HasMetadata`,
`HasConfig` and `IcebergWarehouse` — by `IsDefaultTarget` where that is set,
and otherwise by being the only leg. SQLite, `arc.toml` and an outside-root
Iceberg warehouse are read from `read.anchor.target` and never from
`m.defaultDestination()`.

Those two are the same target right up until an operator adds a second target
and re-points `backup.default_target`, which is the documented migration path.
After that the gates still fire — they read the MERGED view, so `HasMetadata`
is true because some leg set it — while the read goes to a target holding
nothing. SQLite and config then fail outright, in replace mode *after*
`replaceDatabases` has already deleted and rewritten live files, and every
retry fails identically until the default is pointed back. The warehouse arm
does not fail at all: `restoreIcebergWarehouse` returns nil on a zero-object
listing, because that is a legitimate "this run had no warehouse", so the
restore reports **completed** having written no warehouse while the catalog it
just restored points at absolute paths under it — #637's failure mode, by a
different route. The anchor must NOT be keyed off `IsDefaultTarget` alone:
`planRun` writes that field only for multi-leg runs, so the single-target
manifest this exists to fix has it false.

**The run index is read from every configured target, written to one.** The
write only ever goes to the default, so a healthy destination needs one read;
the read is wide because the default can MOVE, and a reader of only the current
default silently stops enumerating exactly the leftovers the index exists to
find. Same shape for `DeleteBackup`'s owner echo, which now tries every target
so a backup held only on a routed one is not deleted without one. The remaining
limit is recorded in the code: a run that died before any manifest is invisible
when no reachable target holds its index, because the directory listing that
produces the ID and the index key are on the same store.

**The `databases` accessor is a type switch on `v.Get`, not `v.GetStringSlice`.**
That accessor casts a scalar through `cast.ToStringSlice`, which runs
`strings.Fields`, so it split on whitespace as well as on the comma: a database
really can be called `my db` (`storage.ValidateKeySegment` rejects only NUL, a
backslash, a separator, the empty string, `.` and `..`), and the scalar
spelling turned it into two routing keys for databases that do not exist while
the real one fell through to the default target. The array spelling of the same
name was already correct, which is the worst shape a bug of this kind can take,
since the two spellings are documented as equivalent.

**The `include_config` credential warning comes from the configured set, not
the run's legs.** What leaks is the file, and `arc.toml` holds every target's
credentials, so a scoped backup of an unrelated database — whose legs are just
the default, and which is the one request that can override `include_config`
without a refusal — must still name every configured remote target.

**`backupRun.fail` marks every leg that has not committed.** A leg that
committed keeps that status, because its manifest really did land; everything
still pending, copying or copied is marked failed, so `/status` does not show a
half-green run that produced nothing restorable there.

Every leg's `sidecarBuilder` ALIASES one `byPath` map — the first cluster
manifest snapshot, the same immutable reference set for every leg. Only
`recheckClusterManifest`'s `addLate` writes into it, after every leg has
copied, on one goroutine. If the leg copies are ever parallelised, that map has
to become per leg or the write has to be guarded.

**A cold restore writes its tier rows in batches, not one per file** (#1141).
Stage C of the backup work gave a restore its own tier-row recorder, because
neither of the hot paths can report a cold file: the cluster manifest drops
cold entries, and `RecordRestoredFile` enqueues an event whose handler stats
the hot backend and whose upsert is guarded to hot rows. That recorder wrote
one row per file, synchronously — one implicit transaction, and so one fsync,
each — on a handle limited to a single connection and shared with auth, audit,
MQTT and the ingest path's own tier registration. A restore of a few hundred
thousand cold files was therefore a few hundred thousand fsyncs with every
other SQLite user in the process queued behind them.

The rows are now accumulated and written a thousand at a time in one explicit
transaction each: `RecordColdFilesBatch` and `RecordRestoredHotFilesBatch` in
the tiering metadata store, `RecordRestoredColdFiles` and
`RecordRestoredHotFiles` on the manager, and a `coldRowBatch` in the restore
beside the manifest registration that already batches the same way.

No field changes and no final value changes: `cold_files_restored_to_cold`,
`cold_files_restored_to_hot`, `cold_restore_quarantine_skipped` and
`cold_rows_not_recorded` keep their per-file meanings, which is why the batch
reports back the paths it did not write rather than a count — a quarantined row
and a path tiering cannot parse are different facts about a key, and they land
in different fields.

**One thing an operator will notice**, which is the honest cost of batching:
three of those counters are no longer live during a run. They move at a flush,
so a `/status` poll through a 900-file cold restore shows `processed_files`
climbing while `cold_files_restored_to_cold` sits at 0 until the run ends, then
jumps to 900. A restore larger than a thousand files steps in thousands. The
final values are what they always were. `cold_files_restored_to_hot` is
unaffected — it counts the routing decision, taken per file before the bytes
are written, not the row.

Four details worth keeping straight, since this is the first explicit
transaction in `internal/tiering`:

- **One transaction per call, and the caller chunks.** That contract is what
  makes the accounting exact: an error means nothing in the call was written,
  so the caller counts the whole chunk as unrecorded without having to ask how
  far it got. Chunking inside the store would commit some chunks and roll back
  one, and no return value short of a per-path map could then describe it.
- **Every statement goes through the transaction, never the pool.** The handle
  allows exactly one connection, so a `db` call made while the transaction is
  open would wait forever for the connection the transaction itself holds.
  That is a self-deadlock, not a slow query. The rollback on the failure path
  is what releases that connection, and the test for it is the one that proves
  a batch after a failed batch can still get a connection at all.
- **Every flush runs on a context detached from the restore's**, not only the
  final one. A transaction cannot begin or continue on a dead context, and a
  restore runs under a fixed `operation_timeout` deadline — so that deadline
  can land inside a mid-run flush of a full chunk, which is the
  longest-running thing a restore does to SQLite. A flush takes its rows out
  of the batch before it writes them, so a chunk lost that way cannot be
  retried: a thousand files would be left with bytes in the cold store and no
  tier row. Each flush is bounded by its own timeout, so a cancelled restore
  still exits after at most one. Unlike the manifest registration beside it,
  the flush also runs on every exit path: the bytes are already in the cold
  store, and a row the backup does not write is not written by anything else
  in the same pass. (Before 27.01.1 that was permanent on a standalone node,
  or on a cluster without shared storage or replication, because the
  cold-metadata sync did not run there. It does now, so the next scan records
  it — but the flush still runs on every exit path, because waiting for a scan
  is not the same as having the row.)
- **`migrated_at` is still written as text in `2006-01-02 15:04:05`.** The
  column is compared as a string against every other row, and go-sqlite3 binds
  a `time.Time` with its offset appended, so a bound time here would sort
  against every row Arc has ever written. The cold-tier metadata sync still
  uses the single-file `RecordColdFile`, which writes the same shape; a test
  asserts the two are byte-identical for the same moment.

The cold-tier metadata sync keeps its per-file writes: it records rows it
discovers one at a time as it walks a listing, and is not a burst.

**Correction to the paragraph that shipped with the batching change above.**
It said a cold-carried file restored into hot storage could have that hot copy
deleted by orphan reconciliation while its forced-hot row was still pending,
on a node whose cold tier is *configured but disabled*, and advised running
such a restore with tiering stopped. That was wrong, and the advice is
withdrawn. The fallback branch is reached only when this node has no usable
cold backend, and a disabled cold tier has no backend **object** either —
`cmd/arc/main.go` constructs one only inside `if cold.Enabled`, nothing
assigns the flag after configuration load, and there is no reload. The sweep
therefore has nothing to verify a cold copy against and keeps the hot file,
which is what it already did. No window existed to widen.

**Orphan reconciliation is gated on `tiered_storage.cold.enabled`, and the
cold accessors no longer disagree** (#1143). Two related pieces of tidying,
neither of which changes what a correctly configured node does:

- A node with tiering on and cold off ran the orphan sweep and the manifest
  sweep every migration cycle, and the orphan sweep logged an **error and
  counted a failure for every ORPHAN it examined, every cycle** — every cold
  row in its 48-hour window whose hot copy is still present — for work it
  could never do, since it needs a cold copy to verify and there is no cold
  backend to verify against. A cold row whose hot copy is gone costs one
  silent existence check and was never the problem. Both sweeps are now
  skipped on such a node, so the cycle is quiet and its error count is honest.
  For the manifest sweep the skip is a pure no-op: it already returned
  immediately on a nil cold backend.

  Two smaller operator-visible consequences of skipping, rather than letting
  the sweep run into its keep-everything branch. The orphan sweep also marks a
  permanently unusable storage key as quarantined, and it does that *before*
  it looks at the cold tier — so on a cold-disabled node that mark is now
  deferred until cold comes back, and since the rows age out of the 48-hour
  window meanwhile, in practice it is not taken. That is harmless while the
  sweep is not running, because the mark exists to stop the sweep retrying
  that key. And the per-orphan error was incidentally the only line saying
  such a node holds a hot copy under a cold row; a skipped cycle now says so
  once, at debug level, instead of once per orphan at error level.
- `GetBackendForTier(TierCold)` now ANDs `cold.enabled`, as `ColdBackend()`
  already did, and both answer through one predicate alongside the migration
  gate, the tier stats, the cold-metadata sync and the query glob. The old
  split was documented as deliberate, with a doc comment asserting that every
  other consumer paired the two checks itself — the orphan sweep did not,
  which is how this was found. Nothing reachable depended on the difference,
  because a disabled cold tier has no backend to return; enforcing it in one
  place means a future change that constructs the backend unconditionally
  cannot turn that latent inconsistency into a live one.

One claim worth not making: the "no usable cold tier" startup warning on a
replicating local-storage node is unchanged by this. It keys off
`GetBackendForTier(TierCold) != nil`, and with cold disabled that was already
nil before this change, so the warning was already firing. Nothing at that
call site behaves differently in any reachable configuration.

### `storage.FileAdopter`, and what a compaction-manifest failure does not retry into

`storage.FileAdopter` is a new optional backend interface, in the shape of the
existing `BatchDeleter`: a backend whose objects really are local files can
implement `AdoptFile` and take ownership of a file the caller has already
written, instead of copying it through `WriteReader`. `LocalBackend` implements
it; `S3Backend` and `AzureBlobBackend` deliberately do not, and a test asserts
they never will, because the contract **consumes the caller's file** and a
backend that copies bytes cannot honour that.

`ErrAdoptUnsupported` means "this pair of paths cannot be adopted, copy
instead" and is not a failure. `AdoptFile` returns it for **any** rename the
filesystem refuses, not only a cross-device one, and also when the source
filesystem will not let Arc set the file mode. The reason is the same in both
cases: before this change the copy was unconditional, so a filesystem that
refuses either operation would go from "compacts, slowly" to "compaction fails
every cycle". Falling back is safe for any rename error, because a rename is
atomic — a failed one moved nothing, and the source the fallback reads is still
there. Only a caller error (a key that escapes the storage root, a source that
is not a regular file) is a hard failure, and both are rejected before the
rename.

The `chmod` is what keeps a moved object at mode `0600`, the same mode
`WriteReader` gives every other local object; a rename otherwise preserves
whatever mode DuckDB wrote the output under, which is umask-derived. It runs
before the rename, so a refused adopt can leave the source file at `0600` — the
contract on `ErrAdoptUnsupported` says so, since it promises only that nothing
was moved.

Unlike `WriteReader`, `AdoptFile` does not stage through a `.part` name. That
name exists so an interrupted *copy* leaves resumable bytes on disk, and a
rename has no partial state — so renaming straight to the final path is
strictly stronger: there is no window in which a reader can observe a short
object.

Separately, a comment in `internal/compaction/job.go` claimed that failing a job
on a completion-manifest write error means the next cycle retries and the
cluster eventually sees the output. It does not, and the comment now says so.
Recovery finds the output present with a matching size, deletes the inputs and
deletes the compaction manifest without ever writing a completion manifest, so
the Raft `RegisterFile` never fires and the compacted file ends up with no
manifest entry describing it. Only the comment changed here; the recovery gap
is tracked in [#1155](https://github.com/Basekick-Labs/arc/issues/1155).
