# Arc 2026.03.1 Release Notes

## Bug Fixes

### Null Handling in Line Protocol Ingestion (#202)

Fixed a bug where missing fields in line protocol ingestion were stored as `0` instead of `NULL`. When ingesting lines with different field sets to the same measurement (e.g., line 1 has `field1`, line 2 has `field2`), the missing fields now correctly produce `NULL` values in the Parquet output instead of zero values.

**Root cause:** The type conversion pipeline (`convertColumnsToTyped`) was discarding null information when converting `[]interface{}` columns to typed arrays, and the Parquet writer was not passing validity bitmaps to Arrow's `AppendValues`.

**Fix:** Introduced `TypedColumnBatch` to carry validity bitmaps alongside typed column data throughout the ingestion pipeline (merge, sort, slice, write). Arrow now receives proper validity bitmaps so Parquet files correctly distinguish between "value is 0" and "value is absent."

*Reported by [@bjarneksat](https://github.com/bjarneksat) — thank you!*

### Stale Cache After Compaction Causes 404 Errors (#204)

Fixed a bug where queries would intermittently fail with HTTP 404 errors after compaction deleted old parquet files from S3. DuckDB's `cache_httpfs` extension was caching glob results (directory listings) that still referenced deleted files, causing queries to attempt reading non-existent objects.

**Root cause:** After compaction merged and deleted old parquet files, no cache invalidation was performed. DuckDB's `cache_httpfs` glob cache, `parquet_metadata_cache`, and Arc's partition pruner caches all retained stale references until their TTLs expired (~1 hour).

**Fix:** Added post-compaction cache invalidation that clears all relevant caches (DuckDB `cache_httpfs`, `parquet_metadata_cache`, partition pruner, and SQL transform cache) immediately after each successful compaction job completes.

*Reported by [@khalid244](https://github.com/khalid244) — thank you!*

### Distributed Cache Invalidation for Enterprise Clustering (#204, #206)

Extended the post-compaction cache fix (#204) to support enterprise clustering, where compaction runs on a dedicated Compactor node separate from Reader/Writer nodes. After each successful compaction job, the Compactor now broadcasts cache invalidation to all healthy cluster peers via `POST /api/v1/internal/cache/invalidate`.

**How it works:** Fire-and-forget goroutines with a 5-second timeout broadcast to each peer. If a reader is temporarily unreachable, its cache expires naturally via TTL. The next compaction cycle implicitly retries. Local node caches are invalidated before the broadcast.

### Generic Query Error Messages (#207)

All query endpoints (`/api/v1/query`, `/api/v1/query/arrow`, `/api/v1/query/:measurement`) now return the actual DuckDB error message instead of a generic `"Query execution failed"`. Users can now see actionable errors like `Parser Error: syntax error at or near "SELEC"` directly in API responses and Grafana, making SQL debugging significantly easier.

*Reported by [@khalid244](https://github.com/khalid244) — thank you!*

## New Features

### Backup & Restore API

Arc now includes a full backup and restore system via REST API. Backups capture parquet data files, SQLite metadata (auth, audit, MQTT config), and the `arc.toml` configuration file — with async operations, real-time progress tracking, and selective restore.

**API endpoints (admin-only):**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/backup` | Trigger a full backup (async) |
| `GET` | `/api/v1/backup` | List all available backups |
| `GET` | `/api/v1/backup/status` | Progress of active operation |
| `GET` | `/api/v1/backup/:id` | Get backup manifest |
| `DELETE` | `/api/v1/backup/:id` | Delete a backup |
| `POST` | `/api/v1/backup/restore` | Restore from a backup (async) |

**Create a backup:**
```bash
curl -X POST "http://localhost:8000/api/v1/backup" \
  -H "Authorization: Bearer $TOKEN"

# Response: 202 Accepted
# {"message": "Backup started", "status": "running"}
```

**Poll progress:**
```bash
curl "http://localhost:8000/api/v1/backup/status" \
  -H "Authorization: Bearer $TOKEN"

# {"operation": "backup", "backup_id": "backup-20260211-143022-a1b2c3d4",
#  "status": "running", "total_files": 1200, "processed_files": 450,
#  "total_bytes": 5368709120, "processed_bytes": 2147483648}
```

**Restore from backup:**
```bash
curl -X POST "http://localhost:8000/api/v1/backup/restore" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "backup_id": "backup-20260211-143022-a1b2c3d4",
    "restore_data": true,
    "restore_metadata": true,
    "restore_config": false,
    "confirm": true
  }'
```

**Key features:**
- **Async operations** — backup and restore run in background goroutines (2-hour timeout). Clients poll `/status` for progress.
- **What gets backed up** — parquet data files, SQLite database (with WAL checkpoint for consistency), and `arc.toml` config
- **Selective restore** — independently choose to restore data, metadata, and/or config
- **Pre-restore safety** — existing SQLite and config files are copied with `.before-restore` suffix before overwriting
- **Destructive restore protection** — restore requires explicit `confirm: true` in the request body
- **Serialized operations** — only one backup or restore can run at a time
- **All storage backends** — works with local filesystem, S3, and Azure Blob Storage

**Configuration:**
```toml
[backup]
enabled = true                  # default: true
local_path = "./data/backups"   # default: ./data/backups
```

**Backup structure:**
```
{backup_id}/
  manifest.json        # metadata: databases, measurements, file counts, sizes
  data/                # parquet files preserving partition layout
  metadata/arc.db      # SQLite database snapshot
  config/arc.toml      # configuration file
```

### Line Protocol Bulk Import

New endpoint `POST /api/v1/import/lp` for bulk importing InfluxDB Line Protocol files. Enables one-command migration from InfluxDB to Arc by uploading `.lp` or `.txt` files (plain or gzip-compressed).

Data flows through Arc's high-performance columnar ingest pipeline (ArrowBuffer → ArrowWriter → Parquet → storage) — the same path used by streaming LP ingestion — so bulk imports benefit from the same throughput and sort optimization.

**Endpoint:**
```bash
curl -X POST "http://localhost:8000/api/v1/import/lp" \
  -H "X-Arc-Database: mydb" \
  -F "file=@export.lp"
```

**Query parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `measurement` | *(all)* | Filter to a single measurement from the LP file |
| `precision` | `ns` | Timestamp precision: `ns`, `us`, `ms`, `s` |

**Features:**
- **Gzip support** — automatically detects and decompresses `.gz` files (magic byte detection)
- **Precision-aware timestamps** — lossless conversion from any precision to Arc's internal microsecond format
- **Multi-measurement** — a single LP file can contain multiple measurements; all are imported in one request
- **RBAC-aware** — checks write permissions for every measurement in the file
- **500 MB size limit** — enforced on both compressed and uncompressed data

**Example with precision:**
```bash
# Import LP file with second-precision timestamps
curl -X POST "http://localhost:8000/api/v1/import/lp?precision=s" \
  -H "X-Arc-Database: mydb" \
  -F "file=@export_seconds.lp"
```

**Response:**
```json
{
  "status": "ok",
  "result": {
    "database": "mydb",
    "measurements": ["cpu", "mem"],
    "rows_imported": 150000,
    "precision": "ns",
    "duration_ms": 342
  }
}
```
