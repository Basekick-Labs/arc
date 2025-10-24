# Retention Policies

Retention policies allow you to automatically manage data lifecycle by defining how long data should be kept. Manual execution only - automatic execution is reserved for enterprise edition.

## Features

- **Manual trigger only** - Automatic execution is enterprise feature
- **Database-wide or measurement-specific** - Apply policies to entire database or individual measurements
- **Dry-run support** - Test policies before executing
- **Execution history** - Track all policy executions
- **Safety buffers** - Add buffer days to prevent accidental deletion of recent data
- **Confirmation required** - Prevents accidental data loss

## API Endpoints

### Create Retention Policy

```bash
POST /api/v1/retention
```

**Request:**
```json
{
  "name": "telegraf_90day_retention",
  "database": "telegraf",
  "measurement": "cpu",
  "retention_days": 90,
  "buffer_days": 7,
  "is_active": true
}
```

**Response:**
```json
{
  "id": 1,
  "name": "telegraf_90day_retention",
  "database": "telegraf",
  "measurement": "cpu",
  "retention_days": 90,
  "buffer_days": 7,
  "is_active": true,
  "last_execution_time": null,
  "last_execution_status": null,
  "last_deleted_count": null,
  "created_at": "2025-10-23T16:00:00.000Z",
  "updated_at": "2025-10-23T16:00:00.000Z"
}
```

### List Retention Policies

```bash
GET /api/v1/retention
```

**Response:**
```json
[
  {
    "id": 1,
    "name": "telegraf_90day_retention",
    "database": "telegraf",
    "measurement": "cpu",
    "retention_days": 90,
    "buffer_days": 7,
    "is_active": true,
    "last_execution_time": "2025-10-23T15:30:00.000Z",
    "last_execution_status": "completed",
    "last_deleted_count": 15000,
    "created_at": "2025-10-23T16:00:00.000Z",
    "updated_at": "2025-10-23T16:00:00.000Z"
  }
]
```

### Get Single Policy

```bash
GET /api/v1/retention/{policy_id}
```

### Update Policy

```bash
PUT /api/v1/retention/{policy_id}
```

### Delete Policy

```bash
DELETE /api/v1/retention/{policy_id}
```

### Execute Retention Policy (Manual Trigger)

```bash
POST /api/v1/retention/{policy_id}/execute
```

**Dry-run first (recommended):**
```json
{
  "dry_run": true,
  "confirm": false
}
```

**Actual execution:**
```json
{
  "dry_run": false,
  "confirm": true
}
```

**Response:**
```json
{
  "policy_id": 1,
  "policy_name": "telegraf_90day_retention",
  "deleted_count": 15000,
  "execution_time_ms": 1234.56,
  "dry_run": false,
  "cutoff_date": "2025-07-25T16:00:00.000Z",
  "affected_measurements": ["cpu"]
}
```

### Get Execution History

```bash
GET /api/v1/retention/{policy_id}/executions?limit=50
```

## How Retention Works

### Physical File Deletion

Retention policies work by **physically deleting Parquet files** from storage. Unlike row-level deletion (which rewrites files), retention:

1. **Scans Parquet files** in the measurement directory
2. **Reads file metadata** to find the maximum timestamp in each file
3. **Identifies files** where ALL rows are older than the cutoff date
4. **Physically deletes** those entire files from disk

**Important:** Files containing ANY data newer than the cutoff are kept. Only files that are completely old are deleted.

### Cutoff Calculation

Data is deleted based on: **cutoff_date = today - retention_days - buffer_days**

Example:
- Today: October 23, 2025
- Retention days: 90
- Buffer days: 7
- **Cutoff date: July 17, 2025** (97 days ago)
- All data **before** July 17, 2025 will be deleted

### File Analysis Example

```
data/telegraf/cpu/
├── cpu_20250715_120000.parquet  # max_time: Jul 15 → DELETE ✓
├── cpu_20250717_140000.parquet  # max_time: Jul 17 → DELETE ✓
├── cpu_20250719_100000.parquet  # max_time: Jul 19 → KEEP (newer than cutoff)
└── cpu_20250920_080000.parquet  # max_time: Sep 20 → KEEP (much newer)

Result: 2 files deleted, ~150,000 rows removed
```

### Retention Flow

1. **Create policy** - Define retention rules
2. **Dry-run** - Test policy to see what would be deleted (no actual deletion)
3. **Execute** - Manually trigger policy with `confirm=true` (physical deletion)
4. **Review** - Check execution history and verify deleted_count

## Usage Examples

### Example 1: Measurement-Specific Retention (90 days)

Keep CPU metrics for 90 days with 7-day safety buffer:

```bash
curl -X POST http://localhost:8000/api/v1/retention \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-admin-token" \
  -d '{
    "name": "cpu_90day_retention",
    "database": "telegraf",
    "measurement": "cpu",
    "retention_days": 90,
    "buffer_days": 7,
    "is_active": true
  }'
```

### Example 2: Database-Wide Retention (365 days)

Keep all measurements in database for 1 year:

```bash
curl -X POST http://localhost:8000/api/v1/retention \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-admin-token" \
  -d '{
    "name": "telegraf_1year_retention",
    "database": "telegraf",
    "measurement": null,
    "retention_days": 365,
    "buffer_days": 30,
    "is_active": true
  }'
```

### Example 3: Dry-Run Before Execution

Always test first:

```bash
# 1. Create policy
POLICY_ID=$(curl -X POST http://localhost:8000/api/v1/retention \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-admin-token" \
  -d '{
    "name": "test_30day_retention",
    "database": "telegraf",
    "measurement": "cpu",
    "retention_days": 30,
    "buffer_days": 7,
    "is_active": true
  }' | jq -r '.id')

# 2. Dry-run to see what would be deleted
curl -X POST http://localhost:8000/api/v1/retention/$POLICY_ID/execute \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-admin-token" \
  -d '{
    "dry_run": true,
    "confirm": false
  }' | jq

# 3. If satisfied, execute for real
curl -X POST http://localhost:8000/api/v1/retention/$POLICY_ID/execute \
  -H "Content-Type: application/json" \
  -H "x-api-key: your-admin-token" \
  -d '{
    "dry_run": false,
    "confirm": true
  }' | jq
```

### Example 4: Check Execution History

```bash
curl -X GET http://localhost:8000/api/v1/retention/$POLICY_ID/executions?limit=10 \
  -H "x-api-key: your-admin-token" | jq
```

## Permissions Required

- **All operations**: Any authenticated user with valid API token
- Authentication handled by Arc's AuthMiddleware
- No special permissions required (managed at token level)

## Best Practices

1. **Always dry-run first** - Use `dry_run: true` to preview deletions
2. **Use buffer days** - Add safety margin (7-30 days) to prevent accidental deletion of recent data
3. **Start with longer retention** - It's easier to shorten retention than recover deleted data
4. **Test on non-production first** - Verify behavior before applying to production
5. **Review execution history** - Monitor `last_deleted_count` to ensure expected behavior
6. **Database-wide vs measurement-specific** - Use measurement-specific for granular control

## Database Schema

### retention_policies table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| name | TEXT | Unique policy name |
| database | TEXT | Target database |
| measurement | TEXT | Target measurement (NULL for database-wide) |
| retention_days | INTEGER | Days to retain data |
| buffer_days | INTEGER | Safety buffer days |
| is_active | BOOLEAN | Whether policy is active |
| created_at | TIMESTAMP | Creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |

### retention_executions table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| policy_id | INTEGER | Foreign key to retention_policies |
| execution_time | TIMESTAMP | When executed |
| status | TEXT | running, completed, failed |
| deleted_count | INTEGER | Rows deleted |
| cutoff_date | TIMESTAMP | Date cutoff used |
| execution_duration_ms | FLOAT | Execution time in ms |
| error_message | TEXT | Error details if failed |

## Limitations

- **Manual execution only** - No automatic/scheduled execution (enterprise feature)
- **File-level granularity** - Deletes entire Parquet files where ALL rows are older than cutoff
- **Local storage only** - Cloud storage (S3/MinIO/GCS/Ceph) not yet implemented
- **No rollback** - Deleted data cannot be recovered (physical file deletion)
- **Sequential execution** - Multiple measurements processed one at a time
- **Works best with compaction** - Compacted files are time-ordered, making retention more effective

## How to Verify It Works

After executing a retention policy, verify the deletion:

### 1. Check Execution Response
```json
{
  "policy_id": 1,
  "policy_name": "telegraf_90day_retention",
  "deleted_count": 15000,        // Number of rows deleted
  "execution_time_ms": 245.67,
  "dry_run": false,
  "cutoff_date": "2025-07-18T00:00:00",
  "affected_measurements": ["cpu"]
}
```

### 2. Check Filesystem
```bash
# Before retention
$ ls -lh data/telegraf/cpu/*.parquet | wc -l
50 files

# After retention (old files deleted)
$ ls -lh data/telegraf/cpu/*.parquet | wc -l
35 files
```

### 3. Query the Data
```bash
# Verify old data is gone
curl -X POST http://localhost:8000/api/v1/query \
  -H "x-api-key: your-token" \
  -d '{"sql": "SELECT COUNT(*) FROM telegraf.cpu WHERE time < '\''2025-07-18'\''"}'

# Should return 0 if retention worked
```

### 4. Check Execution History
```bash
curl http://localhost:8000/api/v1/retention/1/executions \
  -H "x-api-key: your-token" | jq '.'
```

## Performance Characteristics

Retention policies are designed for **zero impact** on normal operations:

- **Write performance:** No overhead (physical file deletion only)
- **Query performance:** No overhead (deleted files don't exist)
- **Execution time:** ~1-2ms per Parquet file analyzed
- **Deletion:** Instant (os.unlink)

**Example:** Analyzing 1000 files takes ~1-2 seconds, with zero impact on concurrent writes/queries.

## Roadmap (Enterprise Features)

- Automatic scheduled execution via cron
- Cloud storage support (S3, MinIO, GCS, Ceph)
- Policy conflicts detection
- Retention policy inheritance
- Archive before delete
- Compliance reporting
- Row-level deletion within files (currently file-level only)
