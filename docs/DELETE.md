# Delete Operations in Arc

Arc supports deleting data points or ranges using a rewrite-based approach. This provides precise deletion with zero overhead on write and query operations.

## Overview

**Status:** Available in `feature/rewrite-based-delete` branch

**Key Characteristics:**
- **Physical Deletes**: Data is physically removed by rewriting Parquet files
- **Zero Write Overhead**: No hashing or tombstone tracking during writes
- **Zero Query Overhead**: Deleted data is gone - no filtering needed
- **Expensive Deletes**: File rewrites are costly, but acceptable for rare operations
- **Precise Control**: Delete any rows matching a WHERE clause
- **Safety Mechanisms**: Prevents accidental mass deletions

## Configuration

Delete operations are **disabled by default** and must be explicitly enabled in `arc.conf`:

```toml
[delete]
# Enable delete operations (default: false for safety)
enabled = true

# Require confirmation for large deletes (default: 10000 rows)
confirmation_threshold = 10000

# Maximum rows per delete operation (default: 1000000)
max_rows_per_delete = 1000000
```

### Environment Variables

```bash
DELETE_ENABLED=true
DELETE_CONFIRMATION_THRESHOLD=10000
DELETE_MAX_ROWS=1000000
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
  "where": "host = 'server01' AND time < '2024-01-01'",
  "dry_run": false,
  "confirm": false
}
```

**Parameters:**
- `database` (required): Database name
- `measurement` (required): Measurement (table) name
- `where` (required): SQL WHERE clause to filter rows
- `dry_run` (optional): If true, return count without deleting (default: false)
- `confirm` (optional): Required for large deletes (default: false)

**Response:**
```json
{
  "deleted_count": 1500,
  "affected_files": 3,
  "rewritten_files": 2,
  "execution_time_ms": 245.7,
  "dry_run": false,
  "files_processed": [
    "data/telegraf/cpu/cpu_20231215.parquet",
    "data/telegraf/cpu/cpu_20231220.parquet"
  ]
}
```

**Response Fields:**
- `deleted_count`: Number of rows deleted
- `affected_files`: Number of files containing matching rows
- `rewritten_files`: Number of files rewritten (excludes fully deleted files)
- `execution_time_ms`: Total execution time
- `dry_run`: Whether this was a dry run
- `files_processed`: List of affected file paths

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
  "affected_files": 5,
  "rewritten_files": 0,
  "execution_time_ms": 45.3,
  "dry_run": true,
  "files_processed": []
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
  "max_rows_per_delete": 1000000
}
```

## How It Works

### 1. File Rewrite Architecture

When you delete data, Arc rewrites Parquet files excluding the deleted rows:

**Before Delete:**
```
data/telegraf/cpu/
  ├── cpu_20240101.parquet    # Contains: server01, server02, server03
  ├── cpu_20240102.parquet    # Contains: server01, server02, server03
  └── cpu_20240103.parquet    # Contains: server01, server02, server03
```

**After DELETE WHERE host = 'server01':**
```
data/telegraf/cpu/
  ├── cpu_20240101.parquet    # Contains: server02, server03 (rewritten)
  ├── cpu_20240102.parquet    # Contains: server02, server03 (rewritten)
  └── cpu_20240103.parquet    # Contains: server02, server03 (rewritten)
```

### 2. Delete Process

**Step 1: Find Affected Files**
- Scan all Parquet files in the measurement directory
- Use DuckDB to evaluate WHERE clause on each file
- Identify files containing rows that match the WHERE clause

**Step 2: Rewrite Files**
For each affected file:
1. Load the Parquet file into Arrow table
2. Use DuckDB to filter out rows matching WHERE clause: `SELECT * FROM table WHERE NOT (your_where_clause)`
3. Write filtered data to temporary file
4. Atomically replace original file with new file using `os.replace()`

**Step 3: Cleanup**
- If all rows deleted from a file → delete the entire file
- If some rows remain → replace with rewritten file
- Remove temporary files

### 3. Performance Characteristics

**Write Operations:**
- ✅ Zero overhead (no hashing, no tombstone tracking)
- Same performance as before DELETE feature

**Query Operations:**
- ✅ Zero overhead (deleted data physically gone)
- No tombstone filtering needed
- Same performance as before DELETE feature

**Delete Operations:**
- ❌ Expensive (file rewrites required)
- Time proportional to affected file sizes
- Acceptable for rare operations (GDPR, error cleanup, manual maintenance)

**Example Timings:**
- Small file (10 MB, 1K rows): ~50-100ms rewrite
- Medium file (100 MB, 10K rows): ~200-500ms rewrite
- Large file (1 GB, 100K rows): ~2-5s rewrite

## Safety Mechanisms

### 1. WHERE Clause Required

Prevents accidental full table deletes:

```bash
# ❌ ERROR: Empty WHERE clause not allowed
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

### 4. Atomic File Replacement

- Uses `os.replace()` for atomic file updates
- Temporary files written first, then atomically moved
- System crash during delete → either old file or new file exists (never corrupted)

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
        "where": "time < '2024-01-01'"
    }
)

result = response.json()
print(f"Deleted {result['deleted_count']} rows from {result['affected_files']} files")
print(f"Execution time: {result['execution_time_ms']}ms")
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

result = dry_run.json()
print(f"Would delete {result['deleted_count']} rows from {result['affected_files']} files")

# 2. If acceptable, perform actual delete
if result['deleted_count'] < 50000:
    actual = requests.post(
        "http://localhost:8000/api/v1/delete",
        headers={"x-api-key": "YOUR_TOKEN"},
        json={
            "database": "telegraf",
            "measurement": "cpu",
            "where": "env = 'dev' AND time < '2024-01-01'",
            "confirm": True
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

## Best Practices

### 1. Enable Only When Needed

```toml
[delete]
# Keep disabled in production unless required
enabled = false
```

### 2. Use Dry Run for Large Deletes

Always test with `dry_run: true` first for operations affecting many rows:

```bash
# First: dry run
curl -X POST http://localhost:8000/api/v1/delete \
  -H "x-api-key: YOUR_TOKEN" \
  -d '{"database": "telegraf", "measurement": "cpu", "where": "...", "dry_run": true}'

# Then: actual delete if acceptable
curl -X POST http://localhost:8000/api/v1/delete \
  -H "x-api-key: YOUR_TOKEN" \
  -d '{"database": "telegraf", "measurement": "cpu", "where": "...", "confirm": true}'
```

### 3. Consider Retention Policies for Time-Based Cleanup

For automated time-based deletion, use [Retention Policies](retention_policies.md) instead:
- More efficient for time-based cleanup
- File-level granularity (faster than row-level)
- Designed for recurring cleanup operations

Use DELETE for:
- GDPR/compliance (specific user data)
- Error cleanup (bad data)
- Decommissioned hosts/sensors
- One-off cleanup operations

### 4. Monitor Execution Time

Large deletes can take significant time:

```python
result = requests.post(...).json()
if result['execution_time_ms'] > 60000:  # 1 minute
    print(f"Warning: Delete took {result['execution_time_ms']/1000}s")
```

### 5. Batch Large Deletes by Time Range

Instead of:
```sql
-- ❌ Single large delete (2M rows)
WHERE time < '2024-01-01'
```

Use:
```sql
-- ✅ Multiple smaller deletes
WHERE time < '2023-01-01'  # Delete oldest month first
WHERE time >= '2023-01-01' AND time < '2023-02-01'  # Then next month
-- etc.
```

### 6. Understand Storage Impact

**During Delete:**
- Temporary files created (doubles storage temporarily)
- Ensure sufficient disk space (2x affected file sizes)

**After Delete:**
- Storage immediately reclaimed
- Parquet files are smaller or removed

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

### Insufficient Disk Space

**Error:** `OSError: No space left on device`

**Cause:** Delete creates temporary files (doubles storage temporarily)

**Solutions:**
1. Free up disk space before deleting
2. Delete smaller batches
3. Add more storage

### File Permission Errors

**Error:** `PermissionError: [Errno 13] Permission denied`

**Cause:** Arc process doesn't have write permissions on data files

**Solution:** Fix file permissions:
```bash
chown -R arc:arc /path/to/data/
chmod -R 755 /path/to/data/
```

## Comparison with Other Approaches

| Approach | Write Perf | Query Perf | Delete Precision | Best For |
|----------|-----------|-----------|------------------|----------|
| **Rewrite-based (current)** | ✅ Zero impact | ✅ Zero impact | ✅ Precise | Rare deletes (GDPR, errors) |
| File-level (retention) | ✅ Zero impact | ✅ Zero impact | ❌ Limited | Time-based cleanup |
| Tombstone-based (disabled) | ❌ 4.6x slower | ❌ Slower | ✅ Precise | ❌ Not recommended |

**Why rewrite-based?**
- Optimizes for the common case (writes/queries)
- Deletes are rare in time-series workloads
- Physical deletion = no query-time overhead
- Simple implementation without complex tombstone tracking

## Limitations

**Current Limitations:**
1. **Expensive Deletes**: File rewrites can be slow for large files
2. **Temporary Storage**: Requires 2x storage during delete operation
3. **No Rollback**: Deletes cannot be undone (data is physically removed)
4. **No DELETE Permission**: Currently any authenticated user can delete (permission system coming)
5. **No Transaction Support**: Deletes are not atomic with other operations
6. **Local Storage Only**: S3/remote storage support coming

**Future Enhancements:**
- Fine-grained permissions (DELETE permission on tokens)
- Transaction support (atomic delete + insert)
- Remote storage support (S3, GCS, Azure)
- DELETE via SQL syntax (in addition to API)
- Parallel file processing for faster large deletes
- Audit logging with dedicated storage

## Security

**Current:**
- Requires valid authentication token
- Disabled by default (explicit opt-in)
- Safety thresholds and confirmation requirements

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
A: Yes. Data is physically removed by rewriting Parquet files. The deleted rows are gone.

**Q: Can I rollback a delete?**
A: No. Deletes are permanent. Always use `dry_run: true` first to verify.

**Q: How do I delete all data from a table?**
A: Use `WHERE 1=1` with `confirm: true`:
```json
{
  "where": "1=1",
  "confirm": true
}
```

**Q: What happens to storage after delete?**
A: Storage is immediately reclaimed. Parquet files are rewritten smaller or deleted entirely.

**Q: Is there overhead on queries after delete?**
A: No. Zero overhead. Deleted data is physically gone, no filtering needed.

**Q: Should I use DELETE or Retention Policies for time-based cleanup?**
A: Use [Retention Policies](retention_policies.md) for time-based cleanup. They're more efficient (file-level granularity). Use DELETE for precise operations like GDPR requests or error cleanup.

**Q: How much disk space do I need for delete operations?**
A: Temporarily 2x the size of affected files (for temporary rewrites). After completion, space is reclaimed.

**Q: Can I delete while writes are happening?**
A: Yes, but not recommended for the same measurement. Concurrent writes may create new files with matching data during the delete operation.

---

**Feature Status:** Available in `feature/rewrite-based-delete` branch.

For questions or issues, see [GitHub Issues](https://github.com/basekick-labs/arc/issues).
