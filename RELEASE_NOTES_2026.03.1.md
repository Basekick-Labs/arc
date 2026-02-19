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

### time_bucket and date_trunc Return Per-Second Rows Instead of Proper Buckets (#212)

Fixed a bug where `time_bucket()` and `date_trunc()` GROUP BY queries returned one row per unique second instead of proper time buckets. A 7-day hourly query returned 604,801 rows / 16.9MB instead of 169 rows / 5KB, causing Grafana dashboards to timeout on ranges > 24h.

**Root cause:** Arc's query rewriter (`rewriteTimeBucket`, `rewriteDateTrunc`) converts time-bucketing SQL to epoch-based arithmetic for performance. The rewritten SQL used DuckDB's `/` operator for division, which performs **float division** on integers (unlike PostgreSQL). This meant `(epoch(time)::BIGINT / 3600) * 3600` returned the original value — no bucketing happened.

**Fix:** Changed `/` to `//` (DuckDB's integer division operator) in all three rewrite locations. `(epoch(time)::BIGINT // 3600) * 3600` now correctly truncates to hour boundaries.

*Reported by [@khalid244](https://github.com/khalid244) — thank you!*

### Unified cache_httpfs TTLs and Scaled Cache Sizes (#214)

DuckDB's `cache_httpfs` glob, metadata, and file handle caches are now properly tuned. Metadata and file handle TTLs match `s3_cache_ttl_seconds` (these reference immutable parquet files). Glob TTL is fixed at 10 seconds — directory listings change during compaction, and S3 LIST overhead is negligible. Cache sizes now scale proportionally with `s3_cache_size` (glob: 5% of block count, metadata/file handles: 10%), with floors at DuckDB defaults for small deployments. No new config settings.

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

### TLE (Two-Line Element) Ingestion & Import

Native support for ingesting satellite orbital data in the standard TLE format used by Space-Track.org, CelesTrak, and ground station pipelines. TLE data is parsed into a configurable measurement (default: `satellite_tle`) with orbital elements as fields and satellite identifiers as tags.

Two endpoints are provided:

**Streaming ingestion** — `POST /api/v1/write/tle` — for continuous TLE feeds, cron jobs, and real-time updates:
```bash
curl -X POST "http://localhost:8000/api/v1/write/tle" \
  -H "X-Arc-Database: satellites" \
  --data-binary @stations.tle
# → 204 No Content
```

**Bulk import** — `POST /api/v1/import/tle` — for historical backfill from Space-Track.org exports or CelesTrak catalog dumps:
```bash
curl -X POST "http://localhost:8000/api/v1/import/tle" \
  -H "X-Arc-Database: satellites" \
  -F "file=@catalog.tle"
```

```json
{
  "status": "ok",
  "result": {
    "database": "satellites",
    "measurement": "satellite_tle",
    "satellite_count": 28000,
    "rows_imported": 28000,
    "duration_ms": 1250
  }
}
```

**Headers:**

| Header | Default | Description |
|--------|---------|-------------|
| `X-Arc-Database` | `default` | Target database |
| `X-Arc-Measurement` | `satellite_tle` | Target measurement name |

**Custom measurement example:**
```bash
curl -X POST "http://localhost:8000/api/v1/write/tle" \
  -H "X-Arc-Database: satellites" \
  -H "X-Arc-Measurement: iss_orbital_elements" \
  --data-binary @iss.tle
```

**Schema (default measurement `satellite_tle`):**

| Column | Type | Description |
|--------|------|-------------|
| `norad_id` | tag | NORAD catalog number |
| `object_name` | tag | Satellite name |
| `classification` | tag | U (unclassified), C, S |
| `international_designator` | tag | Launch year + piece |
| `orbit_type` | tag | LEO, MEO, GEO, HEO |
| `inclination_deg` | field | Orbital inclination |
| `raan_deg` | field | Right ascension of ascending node |
| `eccentricity` | field | Orbital eccentricity |
| `arg_perigee_deg` | field | Argument of perigee |
| `mean_anomaly_deg` | field | Mean anomaly |
| `mean_motion_rev_day` | field | Revolutions per day |
| `bstar` | field | BSTAR drag coefficient |
| `semi_major_axis_km` | field | Derived: semi-major axis |
| `period_min` | field | Derived: orbital period |
| `apogee_km` | field | Derived: apogee altitude |
| `perigee_km` | field | Derived: perigee altitude |

**Features:**
- Pure Go parser — no external dependencies
- Supports both 3-line (with name) and 2-line (no name) TLE formats, including mixed-format files
- Gzip-compressed payloads auto-detected
- Checksum validation with graceful skip on bad entries (warnings collected, not fatal)
- Derived orbital metrics computed automatically (semi-major axis, period, apogee, perigee, orbit classification)
- RBAC-aware, cluster routing enabled
- 500 MB size limit on bulk imports

**Performance:** TLE ingestion uses a typed columnar fast path that bypasses the `[]interface{}` intermediary and `convertColumnsToTyped` pass used by generic ingestion. The parser operates directly on `[]byte` input with contiguous record allocation and single-pass typed column construction, achieving ~3.5M records/sec on commodity hardware.

**Example query:**
```sql
SELECT object_name, orbit_type, period_min, perigee_km, apogee_km
FROM satellite_tle
WHERE orbit_type = 'LEO'
ORDER BY period_min
```
