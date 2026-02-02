# Backup & Restore

Arc includes built-in backup and restore for all users (OSS). A backup captures parquet data files, the SQLite metadata database, and the `arc.toml` configuration file. Backups are stored as directory trees (not archives), which enables streaming, partial restores, and future incremental backup support.

## Configuration

Enable backup in `arc.toml`:

```toml
[backup]
enabled = true
local_path = "./data/backups"
```

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `true` | Enable the backup API endpoints |
| `local_path` | `./data/backups` | Local directory where backups are stored |

## API Endpoints

All endpoints are under `/api/v1/backup` and require admin authentication (when auth is enabled).

### Create a Backup

```
POST /api/v1/backup
```

Triggers a full backup asynchronously. Returns immediately with status `202 Accepted` — poll `/api/v1/backup/status` for progress.

**Request body** (all fields optional):

```json
{
  "include_metadata": true,
  "include_config": true
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `include_metadata` | `true` | Include the SQLite metadata database (auth tokens, audit logs, tiering policies) |
| `include_config` | `true` | Include the `arc.toml` configuration file |

**Response** (`202`):

```json
{
  "message": "Backup started",
  "status": "running"
}
```

Only one backup or restore operation can run at a time. If one is already running, returns `409 Conflict`.

### List Backups

```
GET /api/v1/backup
```

Returns summaries of all available backups.

**Response** (`200`):

```json
{
  "backups": [
    {
      "backup_id": "backup-20260202-150000-a1b2c3d4",
      "created_at": "2026-02-02T15:00:00Z",
      "backup_type": "full",
      "total_files": 1250,
      "total_size_bytes": 524288000,
      "database_count": 3
    }
  ],
  "count": 1
}
```

### Get Backup Details

```
GET /api/v1/backup/:id
```

Returns the full manifest for a specific backup, including per-database and per-measurement breakdowns.

**Response** (`200`):

```json
{
  "version": "dev",
  "backup_id": "backup-20260202-150000-a1b2c3d4",
  "created_at": "2026-02-02T15:00:00Z",
  "backup_type": "full",
  "total_files": 1250,
  "total_size_bytes": 524288000,
  "has_metadata": true,
  "has_config": true,
  "databases": [
    {
      "name": "production",
      "file_count": 1000,
      "size_bytes": 419430400,
      "measurements": [
        { "name": "cpu", "file_count": 600, "size_bytes": 314572800 },
        { "name": "mem", "file_count": 400, "size_bytes": 104857600 }
      ]
    }
  ]
}
```

### Delete a Backup

```
DELETE /api/v1/backup/:id
```

Removes all files associated with a backup.

**Response** (`200`):

```json
{
  "message": "Backup deleted",
  "backup_id": "backup-20260202-150000-a1b2c3d4"
}
```

### Check Progress

```
GET /api/v1/backup/status
```

Returns the progress of the currently active backup or restore operation.

**Response when active** (`200`):

```json
{
  "operation": "backup",
  "backup_id": "backup-20260202-150000-a1b2c3d4",
  "status": "running",
  "total_files": 1250,
  "processed_files": 430,
  "total_bytes": 524288000,
  "processed_bytes": 180355072,
  "started_at": "2026-02-02T15:00:00Z"
}
```

**Response when idle** (`200`):

```json
{
  "status": "idle"
}
```

After completion, the last operation's result remains visible until a new operation starts.

### Restore from Backup

```
POST /api/v1/backup/restore
```

Restores data from a previously created backup. This is a destructive operation that requires explicit confirmation.

**Request body**:

```json
{
  "backup_id": "backup-20260202-150000-a1b2c3d4",
  "restore_data": true,
  "restore_metadata": true,
  "restore_config": false,
  "confirm": true
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `backup_id` | *required* | The ID of the backup to restore from |
| `restore_data` | `true` | Restore parquet data files |
| `restore_metadata` | `true` | Restore the SQLite metadata database |
| `restore_config` | `false` | Restore `arc.toml` (requires restart to take effect) |
| `confirm` | *required, must be `true`* | Safety flag — restore will be rejected without this |

**Response** (`202`):

```json
{
  "message": "Restore started",
  "backup_id": "backup-20260202-150000-a1b2c3d4",
  "status": "running"
}
```

Runs asynchronously. Poll `/api/v1/backup/status` for progress.

**Safety behavior during restore:**
- Before overwriting the SQLite database, the current one is saved as `<path>.before-restore`
- Before overwriting `arc.toml`, the current one is saved as `<path>.before-restore`
- SQLite database is written with `0600` permissions

## Backup Directory Structure

Each backup is stored as a directory tree:

```
backup-20260202-150000-a1b2c3d4/
├── manifest.json
├── data/
│   └── {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/*.parquet
├── metadata/
│   └── arc.db
└── config/
    └── arc.toml
```

The `manifest.json` file contains a full inventory of the backup contents, including per-database and per-measurement file counts and sizes.

## Typical Workflow

```bash
# 1. Create a backup
curl -X POST http://localhost:8080/api/v1/backup \
  -H "Authorization: Bearer <token>"

# 2. Poll for completion
curl http://localhost:8080/api/v1/backup/status \
  -H "Authorization: Bearer <token>"

# 3. List backups
curl http://localhost:8080/api/v1/backup \
  -H "Authorization: Bearer <token>"

# 4. Inspect a specific backup
curl http://localhost:8080/api/v1/backup/backup-20260202-150000-a1b2c3d4 \
  -H "Authorization: Bearer <token>"

# 5. Restore (data + metadata only, no config)
curl -X POST http://localhost:8080/api/v1/backup/restore \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"backup_id":"backup-20260202-150000-a1b2c3d4","restore_config":false,"confirm":true}'

# 6. Delete an old backup
curl -X DELETE http://localhost:8080/api/v1/backup/backup-20260202-150000-a1b2c3d4 \
  -H "Authorization: Bearer <token>"
```

## Off-Site Storage & Disaster Recovery

Backups are plain directory trees on the local filesystem. After a backup completes, you can copy the entire backup directory to any off-site storage — S3, Azure Blob, an external drive, NAS, tape, etc. — using standard tools (`aws s3 cp`, `rsync`, `cp`, etc.).

To restore from an off-site backup:

1. Copy the backup directory back into Arc's backup path (default `./data/backups/`), preserving the directory structure
2. Verify the backup appears with `GET /api/v1/backup` (it will be discovered automatically via its `manifest.json`)
3. Trigger the restore with `POST /api/v1/backup/restore`

```bash
# Example: copy backup from S3 back to Arc's backup directory
aws s3 cp --recursive s3://my-backups/backup-20260202-150000-a1b2c3d4 \
  ./data/backups/backup-20260202-150000-a1b2c3d4

# Restore it
curl -X POST http://localhost:8080/api/v1/backup/restore \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"backup_id":"backup-20260202-150000-a1b2c3d4","confirm":true}'
```

This makes Arc backups fully portable — they are not tied to the original instance or storage backend.

## Notes

- Only one backup/restore operation can run at a time.
- SQLite backup performs a `PRAGMA wal_checkpoint(TRUNCATE)` before copying to ensure consistency.
- Backup IDs follow the format `backup-{YYYYMMDD}-{HHMMSS}-{8 hex chars}` and are validated on all endpoints to prevent path traversal.
- Async operations (backup and restore) use a detached context with a 2-hour timeout, independent of the HTTP request lifecycle.
- Skipped files during backup (read errors) are logged as warnings but do not fail the entire backup.
- Failed files during restore cause the operation to fail immediately.
