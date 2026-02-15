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
