# Delete Operations in Arc

Arc supports deleting data points or ranges using tombstone-based logical deletes. This approach provides fast deletion while maintaining data integrity and supporting audit trails.

## Overview

**Status:** Feature Branch (`feature/delete-operations`)

**Key Characteristics:**
- **Logical Deletes**: Data is marked as deleted, not physically removed immediately
- **Tombstone Files**: Deleted row identifiers stored in `.deletes/` subdirectory
- **Fast Performance**: Delete operations complete in milliseconds
- **Automatic Cleanup**: Physical removal happens during hourly compaction
- **Audit Trail**: All delete operations are logged with timestamp and user
- **Safety Mechanisms**: Prevents accidental mass deletions

## Configuration

Delete operations are **disabled by default** and must be explicitly enabled in `arc.conf`:

```toml
[delete]
# Enable delete operations (default: false for safety)
enabled = false

# Require confirmation for large deletes (default: 10000 rows)
confirmation_threshold = 10000

# Maximum rows per delete operation (default: 1000000)
max_rows_per_delete = 1000000

# Tombstone retention period in days (default: 30)
tombstone_retention_days = 30

# Enable audit logging (default: true)
audit_enabled = true
```

### Environment Variables

```bash
DELETE_ENABLED=true
DELETE_CONFIRMATION_THRESHOLD=10000
DELETE_MAX_ROWS=1000000
DELETE_TOMBSTONE_RETENTION_DAYS=30
DELETE_AUDIT_ENABLED=true
```

## API Endpoints

### DELETE Data

**Endpoint:** `POST /api/v1/delete`

Delete data matching a WHERE clause.

**Request:**
```json
{
  "database": "telegraf",
  "measurement": "cpu",
  "where": "host = 'server01' AND time BETWEEN '2024-01-01' AND '2024-01-02'",
  "dry_run": false,
  "confirm": false
}
```

**Parameters:**
- `database` (required): Database name
- `measurement` (required): Measurement (table) name
- `where` (required): SQL WHERE clause to filter rows
- `dry_run` (optional): If true, return count without deleting (default: false)
- `confirm` (optional): Required for large deletes or full table deletes (default: false)

**Response:**
```json
{
  "deleted_count": 1500,
  "execution_time_ms": 45.2,
  "tombstone_file": ".deletes/delete_20240101_150000.parquet",
  "dry_run": false
}
```

**Example (curl):**
```bash
curl -X POST http://localhost:8000/api/v1/delete \
  -H "x-api-key: YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "database": "telegraf",
    "measurement": "cpu",
    "where": "host = '\''server01'\'' AND time < '\''2024-01-01'\''"
  }'
```

### Dry Run Mode

Test a delete without actually removing data:

```bash
curl -X POST http://localhost:8000/api/v1/delete \
  -H "x-api-key: YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "database": "telegraf",
    "measurement": "cpu",
    "where": "host = '\''server01'\''",
    "dry_run": true
  }'
```

**Response:**
```json
{
  "deleted_count": 15000,
  "execution_time_ms": 12.3,
  "dry_run": true
}
```

### Get Delete Configuration

**Endpoint:** `GET /api/v1/delete/config`

Get current delete configuration settings.

**Example:**
```bash
curl http://localhost:8000/api/v1/delete/config \
  -H "x-api-key: YOUR_TOKEN"
```

**Response:**
```json
{
  "enabled": true,
  "confirmation_threshold": 10000,
  "max_rows_per_delete": 1000000,
  "tombstone_retention_days": 30,
  "audit_enabled": true
}
```

### Get Delete Audit Log

**Endpoint:** `GET /api/v1/delete/audit`

Retrieve delete operation audit trail.

**Query Parameters:**
- `database` (optional): Filter by database
- `measurement` (optional): Filter by measurement
- `limit` (optional): Max entries to return (default: 100)

**Example:**
```bash
curl "http://localhost:8000/api/v1/delete/audit?database=telegraf&limit=10" \
  -H "x-api-key: YOUR_TOKEN"
```

## How It Works

### 1. Tombstone Architecture

When you delete data, Arc creates a tombstone file containing hashes of deleted rows:

```
data/telegraf/cpu/
  ├── data_20240101_120000.parquet    ← Original data
  ├── data_20240101_130000.parquet
  └── .deletes/
      ├── delete_20240101_150000.parquet  ← Tombstone file
      └── delete_20240101_160000.parquet
```

**Tombstone Schema:**
```
time:         TIMESTAMP  # Deleted row timestamp
_hash:        STRING     # SHA256 hash of row identifier
_deleted_at:  TIMESTAMP  # When deletion occurred
_deleted_by:  STRING     # User/token that deleted
database:     STRING     # Database name
measurement:  STRING     # Measurement name
```

### 2. Query-Time Filtering

Queries automatically filter out deleted rows by checking tombstone files.

**Original Query:**
```sql
SELECT * FROM telegraf.cpu
WHERE time > now() - INTERVAL 1 HOUR
```

**Internal (with tombstone filtering):**
```sql
SELECT * FROM telegraf.cpu
WHERE time > now() - INTERVAL 1 HOUR
  AND _row_hash NOT IN (SELECT _hash FROM .deletes/*)
```

**Performance Impact:** Minimal (< 5% overhead with normal tombstone volumes)

### 3. Physical Cleanup via Compaction

Compaction (runs hourly by default) physically removes deleted rows:

1. **Read tombstone files** for each measurement
2. **Rewrite Parquet files** excluding deleted rows
3. **Remove old tombstone files** past retention period
4. **Update file metadata**

**Storage Reclamation:**
- Tombstones: Immediate logical deletion (fast)
- Data files: Next compaction run (within 1 hour by default)
- Tombstone files: Removed after `tombstone_retention_days`

## Safety Mechanisms

### 1. WHERE Clause Required

Prevents accidental full table deletes:

```bash
# ❌ ERROR: WHERE clause required
{
  "database": "telegraf",
  "measurement": "cpu",
  "where": ""
}

# ✅ OK: Explicit full table delete with confirmation
{
  "database": "telegraf",
  "measurement": "cpu",
  "where": "1=1",
  "confirm": true
}
```

### 2. Confirmation Threshold

Large deletes require explicit confirmation:

```bash
# Delete affects 15,000 rows (threshold: 10,000)
# ❌ ERROR: Requires confirm=true

{
  "database": "telegraf",
  "measurement": "cpu",
  "where": "time < '2024-01-01'",
  "confirm": false  # ← Will fail if > threshold
}

# ✅ OK: With confirmation
{
  "database": "telegraf",
  "measurement": "cpu",
  "where": "time < '2024-01-01'",
  "confirm": true
}
```

### 3. Maximum Rows Limit

Hard limit prevents extremely large deletes:

```bash
# ❌ ERROR: Would delete 2M rows (limit: 1M)
# Refine WHERE clause or increase max_rows_per_delete
```

### 4. Audit Logging

All delete operations are logged:

```
2024-01-01T15:00:00Z DELETE AUDIT: {
  "database": "telegraf",
  "measurement": "cpu",
  "where_clause": "host = 'server01' AND time < '2024-01-01'",
  "deleted_by": "admin-token",
  "deleted_count": 1500,
  "tombstone_file": ".deletes/delete_20240101_150000.parquet",
  "execution_time_ms": 45.2
}
```

## Usage Examples

### Delete Old Data (Time-Based)

```python
import requests

response = requests.post(
    "http://localhost:8000/api/v1/delete",
    headers={"x-api-key": "YOUR_TOKEN"},
    json={
        "database": "telegraf",
        "measurement": "cpu",
        "where": "time < now() - INTERVAL 90 DAYS"
    }
)

print(f"Deleted {response.json()['deleted_count']} rows")
```

### Delete Specific Host Data

```python
response = requests.post(
    "http://localhost:8000/api/v1/delete",
    headers={"x-api-key": "YOUR_TOKEN"},
    json={
        "database": "telegraf",
        "measurement": "cpu",
        "where": "host = 'decommissioned-server'"
    }
)
```

### Delete with Dry Run First

```python
# 1. Dry run to see what would be deleted
dry_run = requests.post(
    "http://localhost:8000/api/v1/delete",
    headers={"x-api-key": "YOUR_TOKEN"},
    json={
        "database": "telegraf",
        "measurement": "cpu",
        "where": "env = 'dev' AND time < '2024-01-01'",
        "dry_run": True
    }
)

print(f"Would delete {dry_run.json()['deleted_count']} rows")

# 2. If acceptable, perform actual delete
if dry_run.json()['deleted_count'] < 50000:
    actual = requests.post(
        "http://localhost:8000/api/v1/delete",
        headers={"x-api-key": "YOUR_TOKEN"},
        json={
            "database": "telegraf",
            "measurement": "cpu",
            "where": "env = 'dev' AND time < '2024-01-01'",
            "dry_run": False
        }
    )
    print(f"Deleted {actual.json()['deleted_count']} rows")
```

### Delete Time Range with Confirmation

```python
response = requests.post(
    "http://localhost:8000/api/v1/delete",
    headers={"x-api-key": "YOUR_TOKEN"},
    json={
        "database": "telegraf",
        "measurement": "cpu",
        "where": "time BETWEEN '2024-01-01' AND '2024-01-31'",
        "confirm": True  # Required if > threshold
    }
)
```

## Performance

**Delete Operation:**
- Tombstone creation: ~50-100ms for 1,000 rows
- Tombstone creation: ~200-500ms for 10,000 rows
- Scales linearly with row count

**Query Overhead:**
- Normal load (< 1000 tombstones): < 1% overhead
- High load (1000-10,000 tombstones): 1-5% overhead
- Very high (> 10,000 tombstones): 5-10% overhead

**Recommendation:** Run compaction regularly (default: hourly) to keep tombstone count low.

**Storage:**
- Tombstone file size: ~100 bytes per deleted row
- 10,000 deletes = ~1 MB tombstone file

## Best Practices

### 1. Enable Only When Needed

```toml
[delete]
# Keep disabled in production unless required
enabled = false
```

### 2. Use Dry Run for Large Deletes

Always test with `dry_run: true` first for large operations.

### 3. Monitor Tombstone Accumulation

```bash
# Check tombstone directory sizes
du -sh data/*/.deletes/
```

### 4. Adjust Retention Based on Compliance

```toml
[delete]
# Regulatory compliance: Keep tombstones for 90 days
tombstone_retention_days = 90

# Quick cleanup: Remove tombstones immediately
tombstone_retention_days = 0
```

### 5. Use Time-Based Deletions

Delete old data regularly to manage storage:

```python
# Automated cleanup script
DELETE FROM telegraf.cpu
WHERE time < now() - INTERVAL 365 DAYS
```

### 6. Combine with Compaction Schedule

```toml
[compaction]
schedule = "5 * * * *"  # Every hour at :05

[delete]
tombstone_retention_days = 1  # Clean up next day
```

## Troubleshooting

### Delete Operations Disabled

```json
{
  "detail": "Delete operations are disabled. Set delete.enabled=true in arc.conf to enable."
}
```

**Solution:** Enable in `arc.conf`:
```toml
[delete]
enabled = true
```

### Confirmation Required

```json
{
  "detail": "Delete would affect 15000 rows (threshold: 10000). Set confirm=true to proceed."
}
```

**Solution:** Add `confirm: true` to request:
```json
{
  "confirm": true
}
```

### Exceeds Maximum Rows

```json
{
  "detail": "Delete would affect 2000000 rows, exceeding maximum of 1000000."
}
```

**Solutions:**
1. Refine WHERE clause to delete fewer rows
2. Increase `max_rows_per_delete` in `arc.conf`
3. Split into multiple delete operations

### Query Performance Degradation

**Symptom:** Queries slower after many deletes

**Solution:** Run compaction to remove tombstone files:
```bash
curl -X POST http://localhost:8000/compaction/trigger \
  -H "x-api-key: YOUR_TOKEN"
```

## Limitations

**Current Limitations:**
1. **No Rollback**: Deletes cannot be undone (tombstones are permanent)
2. **No DELETE Permission**: Currently any authenticated user can delete (permission system coming)
3. **No Transaction Support**: Deletes are not atomic with other operations
4. **Audit Storage**: Audit log currently in application logs only (dedicated storage coming)

**Future Enhancements:**
- Fine-grained permissions (DELETE permission on tokens)
- Transaction support (atomic delete + insert)
- Dedicated audit log storage
- DELETE via SQL syntax (in addition to API)
- Soft delete with retention + rollback
- Incremental tombstone cleanup

## Security

**Current:**
- Requires valid authentication token
- All deletes are audit logged
- Disabled by default (explicit opt-in)

**Future (with permission system):**
```json
{
  "name": "admin-token",
  "permissions": {
    "can_read": true,
    "can_write": true,
    "can_delete": true  ← New permission
  }
}
```

## FAQ

**Q: Does delete actually remove data immediately?**
A: No. It creates a tombstone file marking rows as deleted. Physical removal happens during compaction (hourly by default).

**Q: Can I rollback a delete?**
A: Not currently. Deletes create permanent tombstone markers.

**Q: How do I delete all data from a table?**
A: Use `WHERE 1=1` with `confirm: true`:
```json
{
  "where": "1=1",
  "confirm": true
}
```

**Q: What happens to deleted data in queries?**
A: Deleted rows are automatically filtered out at query time.

**Q: How much storage do tombstones use?**
A: ~100 bytes per deleted row. 10K deletes = ~1 MB.

**Q: Can I disable audit logging?**
A: Yes, set `audit_enabled = false` in `arc.conf`.

**Q: How do I see what was deleted?**
A: Check application logs for DELETE AUDIT entries or use `/api/v1/delete/audit` endpoint (when implemented).

---

**Feature Status:** Implementation in progress on `feature/delete-operations` branch.

For questions or issues, see [GitHub Issues](https://github.com/basekick-labs/arc/issues).
