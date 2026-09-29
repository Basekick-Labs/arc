# Runtime ingest buffer configuration

Arc can adjust its Arrow ingest buffer thresholds without restarting the process. The runtime API applies the new values immediately and stores them in Arc's metadata SQLite database so they are restored after a restart.

## Settings and scope

| Setting | Type | Unit | Validation |
| --- | --- | --- | --- |
| `max_buffer_size` | integer | records | Greater than zero |
| `max_buffer_age_ms` | integer | milliseconds | Greater than zero and representable as a Go duration |

The settings apply to the current Arc process. Each node reads its own override from the metadata SQLite database at startup. The Arc API does not broadcast changes to cluster peers; `arcli ingest buffer set` coordinates the same change across all healthy nodes after preflighting them.

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
