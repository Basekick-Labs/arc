# Runtime ingest buffer configuration

Arc can adjust its Arrow ingest buffer thresholds without restarting the process. The runtime API applies new values immediately and can store them in Arc's metadata SQLite database so they are restored after a restart.

## Optional elastic flush-queue reserve

Arc can optionally reserve bounded in-memory capacity for size-triggered flush tasks that find `flushQueue` full. It is disabled by default. The reserve is only a temporary overflow cushion: it does not increase sustained flush throughput, cover age-triggered flushes or worker-held writes, or guarantee durable recovery after the reserve fills.

The setting is measured and reported in **records**. Its capacity is an admission budget for batches: a batch is accepted only when its record count fits in the remaining capacity. Arc preallocates bounded task-reference slots when enabling/resizing the reserve and checks available memory before changing the live setting. If capacity cannot be validated or allocated, the API returns an error and leaves the prior runtime and SQLite state unchanged. Reserve admission happens only after the normal flush queue is full; the ordinary per-record ingest path does not poll the reserve, read SQLite/environment settings, or scan queue state.

The reserve stores the existing extracted flush task and its batch references; it does not serialize a second copy of records. Worker dequeue events move pending tasks back to `flushQueue`, without a timer or polling loop. Occupancy should return to zero after the backlog drains. Arc rejects a resize below current occupancy or a disable while records remain reserved.

The API is process-local and does not replicate through Raft. A cluster operator must configure each Arc node and store the override in each node's own metadata SQLite database.

When `persistent:true` is supplied, Arc saves the reserve setting in the independent `arc_runtime_ingest_elastic_reserve` table. Reserve persistence is opt-in and defaults to false for runtime API changes. Omitting `persistent` preserves the current persistence mode for a saved override; `persistent:false` removes the saved row while leaving the live setting active until restart. `DELETE` disables the reserve and removes only its saved override. Resetting the `max_buffer_size` / `max_buffer_age_ms` override does not reset this reserve setting.

```sh
# Enable a runtime-only reserve with a 250,000-record capacity
curl -X PATCH \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"capacity_records":250000,"persistent":false}' \
  http://localhost:8000/api/v1/config/runtime/ingest/elastic-reserve

# Read settings and current occupancy
curl -H "Authorization: Bearer $ARC_TOKEN" \
  http://localhost:8000/api/v1/config/runtime/ingest/elastic-reserve

# Persist the live setting for restart
curl -X PATCH \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"persistent":true}' \
  http://localhost:8000/api/v1/config/runtime/ingest/elastic-reserve
```

If the flush queue and reserve are both full, Arc uses the existing WAL fallback when a WAL writer is configured. A configured WAL is not proof that every affected record was appended successfully: correlate this counter with WAL append-error telemetry and WAL recovery status. With WAL disabled, overflow after reserve exhaustion is unprotected and may lose records. Arc reports it separately; the reserve is volatile memory and does not change that durability contract.

New Prometheus metrics expose reserve enablement, configured/current record capacity, admissions, queue-full records, re-queued records, fallback records, and unprotected overflow: `arc_buffer_elastic_reserve_enabled`, `arc_buffer_elastic_reserve_capacity_records`, `arc_buffer_elastic_reserve_records`, `arc_buffer_elastic_reserve_admissions_total`, `arc_buffer_flush_queue_enqueued_records_total`, `arc_buffer_flush_queue_full_records_total`, `arc_buffer_flush_fallback_records_total`, `arc_buffer_flush_fallback_wal_configured_records_total`, and `arc_buffer_unprotected_overflow_records_total`. The WAL-configured fallback counter is not a durable-append confirmation. Use sequence-tagged test traffic to establish exact loss or duplication; production counters support flow comparison and alerting, not record-level proof.

## Settings and scope

| Setting | Type | Unit | Validation |
| --- | --- | --- | --- |
| `max_buffer_size` | integer | records | Greater than zero |
| `max_buffer_age_ms` | integer | milliseconds | Greater than zero and representable as a Go duration |

The settings apply to the current Arc process. Each node reads its own override from the metadata SQLite database at startup. The Arc API does not broadcast changes to cluster peers; `arcli ingest buffer set` coordinates the same change across all healthy nodes after preflighting them.

The size and age thresholds are shared process settings, but each threshold is evaluated independently for every logical ingest buffer, keyed by database and measurement. `max_buffer_size` is therefore a per-buffer threshold, not a process-wide cap: multiple active measurements can collectively hold more records than this value. Shards partition the buffer map to reduce lock contention; they do not create separate configurations. Flush workers consume queued flush tasks and do not own separate ingest buffers or threshold values.

## Ingest hot path and runtime update cost

The buffer-size threshold is checked once for each buffered Arrow batch, not once for each record. This check already existed before runtime reconfiguration: the original writer compared the accumulated record count with its immutable startup configuration. Runtime reconfiguration changes where that threshold comes from: Arc now loads it from an atomic in-memory value so an API update can take effect without rebuilding the writer.

The write path does not read environment variables, `arc.toml`, SQLite, or the runtime API to obtain the threshold. The persistent override is loaded from SQLite once during startup. When `persistent:true` is requested, SQLite is written on the administrative configuration path, not on ingest writes. The runtime check adds one atomic load per buffered batch in place of the prior ordinary field read; no benchmark is claimed here, so its workload-specific cost has not been quantified.

The age threshold is handled by Arc's background flusher. It is not polled for every record or batch. Updating the age signals that flusher to recompute its timer and flush deadlines; normal age-based scans happen in that background path.

In a cluster, a direct API request still changes only the addressed process. `arcli` preflights the cluster and sends the same update to each healthy node, where it is persisted in that node's own metadata SQLite database. This is coordinated fan-out, not Raft replication or a distributed transaction. If an update fails partway through, `arcli` attempts best-effort rollback on nodes that may have changed; a rollback can also fail and must be treated as a partial cluster update.

## API

All three methods use `/api/v1/config/runtime/ingest`:

| Method | Behavior |
| --- | --- |
| `GET` | Read effective values and their source. |
| `PATCH` | Change one or both values and optionally persist the resulting pair. |
| `DELETE` | Remove the saved override and restore the values loaded at process startup. |

When Arc authentication is enabled, these routes require an administrator bearer token. When authentication is disabled, the routes follow Arc's unauthenticated mode.

### Read the active configuration

```sh
curl -H "Authorization: Bearer $ARC_TOKEN" \
  http://localhost:8000/api/v1/config/runtime/ingest
```

Example response:

```json
{
  "max_buffer_size": 200000,
  "max_buffer_age_ms": 30000,
  "scope": "current_process",
  "persistent": true,
  "source": "persistent_override"
}
```

`source` is `persistent_override` when a saved override is active, `runtime_override` when values were changed for this process only, or `startup_config` when the effective values come from Arc's startup configuration. `persistent` indicates whether the override row exists.

### Change one or both values

`PATCH` accepts either threshold, both, and an optional `persistent` boolean. If a threshold is omitted, its current effective value is retained. `persistent` defaults to `true` for compatibility. Set it to `false` to apply the values only to the current process and remove any saved override.

```sh
curl -X PATCH \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"max_buffer_size":250000,"max_buffer_age_ms":15000,"persistent":true}' \
  http://localhost:8000/api/v1/config/runtime/ingest
```

For a partial change, send only the field to change:

```sh
curl -X PATCH \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"max_buffer_age_ms":10000}' \
  http://localhost:8000/api/v1/config/runtime/ingest
```

To persist the current effective values without changing thresholds, send `{"persistent":true}`. An empty body, `persistent:false` without a threshold, malformed JSON, non-positive value, or duration overflow is rejected. If Arc cannot save/delete a valid change, it rolls back the in-memory setting and returns a server error.

### Restore startup values

```sh
curl -X DELETE -H "Authorization: Bearer $ARC_TOKEN" \
  http://localhost:8000/api/v1/config/runtime/ingest
```

Arc removes the override and restores the startup values immediately. Subsequent restarts continue using startup configuration until another `PATCH` creates an override.

## Persistence and precedence

At startup, Arc reads the optional saved override before constructing the ingest buffer. When a row exists, it takes precedence over the values loaded from `arc.toml`, environment variables, or defaults. Without a row, Arc uses its normal startup configuration resolution.

The override is stored in the singleton `arc_runtime_ingest_config` table in Arc's metadata database, at the database path configured for Arc authentication (default `./data/arc.db`). Preserve this database across container replacement. For Docker deployments, keep the persistent volume mounted at Arc's data directory.

When `persistent:false` is used with changed thresholds, Arc removes any saved row and keeps the new values in memory only. `GET` then reports `persistent:false` and `source:"runtime_override"`. A restart returns to normal startup configuration.

`DELETE` removes the override row rather than storing a copy of the startup values. This lets later startup configuration changes take effect after the reset.

## Observability delay

`GET /api/v1/config/runtime/ingest` and `arcli ingest buffer show` return the current process values immediately. They do not trigger a telemetry scrape, a write, or a Grafana refresh. A dashboard built from stored telemetry can therefore show the previous sample for a short time after a successful `PATCH`.

For example, the Arc Wikimedia lab samples this API every `TELEMETRY_SECONDS` (10 seconds by default), writes the sample to `sse_ingestion_telemetry`, and refreshes its buffer dashboard every 15 seconds. Arc makes that telemetry row queryable after the row's own buffer flushes: when the buffer reaches `max_buffer_size` or `max_buffer_age_ms`, whichever happens first. A useful nominal delay estimate is:

```text
telemetry sampling interval + Arc buffer age threshold + Grafana refresh interval
```

With the lab's default 30,000 ms buffer age, that is about 55 seconds; with a 5,000 ms age, about 30 seconds. These are estimates, not a delivery guarantee; queueing, write errors, and query time add delay. The collector's `FLUSH_SECONDS` controls source-data batches and is separate from the telemetry sampling loop.

## Implementation and verification

- `internal/api/runtime_ingest_config.go` implements the HTTP handlers and serializes reads and mutations.
- `internal/ingest/runtime_config_store.go` validates and saves the optional SQLite override.
- `cmd/arc/main.go` loads the override before creating the Arrow buffer and registers the API routes.
- Tests cover persistence, reset behavior, invalid values, authorization, database failures, and startup reload.
