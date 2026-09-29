# Runtime ingest buffer configuration

Arc can adjust its Arrow ingest buffer thresholds without restarting the process. The runtime API applies the new values immediately and stores them in Arc's metadata SQLite database so they are restored after a restart.

## Settings and scope

| Setting | Type | Unit | Validation |
| --- | --- | --- | --- |
| `max_buffer_size` | integer | records | Greater than zero |
| `max_buffer_age_ms` | integer | milliseconds | Greater than zero and representable as a Go duration |

The settings apply to the current Arc process. The API does not broadcast changes to cluster peers; call it on each Arc node that needs the new values. Each process persists its override in the metadata database it uses.

## API

All three methods use `/api/v1/config/runtime/ingest`:

| Method | Behavior |
| --- | --- |
| `GET` | Read effective values and their source. |
| `PATCH` | Change one or both values and persist the resulting pair. |
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

`source` is `persistent_override` when Arc loaded a saved override, or `startup_config` when the effective values come from Arc's startup configuration. `persistent` indicates whether the override row exists.

### Change one or both values

`PATCH` accepts either field or both. If a field is omitted, its current effective value is retained and the resulting pair is saved.

```sh
curl -X PATCH \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"max_buffer_size":250000,"max_buffer_age_ms":15000}' \
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

An empty body, malformed JSON, non-positive value, or duration overflow is rejected. If Arc cannot save a valid change, it rolls back the in-memory setting and returns a server error.

### Restore startup values

```sh
curl -X DELETE -H "Authorization: Bearer $ARC_TOKEN" \
  http://localhost:8000/api/v1/config/runtime/ingest
```

Arc removes the override and restores the startup values immediately. Subsequent restarts continue using startup configuration until another `PATCH` creates an override.

## Persistence and precedence

At startup, Arc reads the optional saved override before constructing the ingest buffer. When a row exists, it takes precedence over the values loaded from `arc.toml`, environment variables, or defaults. Without a row, Arc uses its normal startup configuration resolution.

The override is stored in the singleton `arc_runtime_ingest_config` table in Arc's metadata database, at the database path configured for Arc authentication (default `./data/arc.db`). Preserve this database across container replacement. For Docker deployments, keep the persistent volume mounted at Arc's data directory.

`DELETE` removes the override row rather than storing a copy of the startup values. This lets later startup configuration changes take effect after the reset.

## Implementation and verification

- `internal/api/runtime_ingest_config.go` implements the HTTP handlers and serializes reads and mutations.
- `internal/ingest/runtime_config_store.go` validates and saves the optional SQLite override.
- `cmd/arc/main.go` loads the override before creating the Arrow buffer and registers the API routes.
- Tests cover persistence, reset behavior, invalid values, authorization, database failures, and startup reload.
