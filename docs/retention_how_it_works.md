# How Retention Policies Actually Delete Data

## The Problem

Arc stores time-series data in **Parquet files** on disk (or cloud storage). SQL `DELETE` statements don't work on Parquet files because they're immutable columnar files.

## The Solution: Physical File Deletion

Retention policies work by **physically deleting entire Parquet files** that contain only old data.

### Step-by-Step Process

#### 1. Calculate Cutoff Date
```
cutoff_date = today - retention_days - buffer_days
```

Example:
- Today: October 23, 2025
- Retention: 90 days
- Buffer: 7 days
- **Cutoff: July 18, 2025**

All data **before** July 18, 2025 will be deleted.

#### 2. Find Parquet Files

Arc stores data in partitioned directories:
```
data/
└── telegraf/           # Database
    └── cpu/            # Measurement
        ├── cpu_20250715_120000.parquet  # Older than cutoff
        ├── cpu_20250720_130000.parquet  # Mixed (some old, some new)
        └── cpu_20250725_140000.parquet  # Newer than cutoff
```

#### 3. Analyze Each File

For each Parquet file:
1. **Read metadata** to get time range
2. **Find max timestamp** in the file
3. **Check if max_time < cutoff_date**

```python
# Read file
table = pq.read_table("cpu_20250715_120000.parquet")

# Get max time
max_time = compute.max(table.column('time'))
# max_time = July 15, 2025 12:00:00

# Check if ALL rows are older than cutoff
if max_time < cutoff_date:  # July 15 < July 18 ✓
    # Safe to delete entire file
    mark_for_deletion(file)
```

#### 4. Delete Marked Files

```python
# Only delete files where ALL rows are old
for file in files_to_delete:
    os.unlink(file)  # Physical deletion
    deleted_count += file.num_rows
```

### Important: File-Level Granularity

⚠️ **Retention policies delete entire files, not individual rows.**

This means:
- If a file has ANY data newer than cutoff → **file is kept**
- Only files where **ALL rows** are older than cutoff → **file is deleted**

### Example Scenario

**Configuration:**
- Retention: 30 days
- Buffer: 3 days
- Cutoff: September 20, 2025

**File Analysis:**
```
File                         | Min Time      | Max Time      | Action
-----------------------------|---------------|---------------|--------
cpu_2025_09_15_120000.parquet| Sep 15 12:00  | Sep 15 18:00  | DELETE (all old)
cpu_2025_09_19_120000.parquet| Sep 19 12:00  | Sep 20 06:00  | KEEP (some new)
cpu_2025_09_21_120000.parquet| Sep 21 00:00  | Sep 21 12:00  | KEEP (all new)
```

**Result:**
- Only `cpu_2025_09_15_120000.parquet` is deleted
- Files with mixed data are preserved

### Why This Approach Works

✅ **Efficient**: Deletes entire files instead of rewriting Parquet
✅ **Safe**: Only deletes files where 100% of data is old
✅ **Fast**: No data rewriting or tombstones needed
✅ **Simple**: Works with Arc's existing partitioned storage

### Best Practices

1. **Use appropriate buffer_days** - Prevents deleting recent data
2. **Compaction helps** - Compacted files group data by time range
3. **Test with dry_run first** - See what would be deleted
4. **Monitor execution history** - Track what was deleted

### Limitations

#### File-Level Granularity
If you have large files spanning multiple days, only files that are **completely** older than cutoff will be deleted.

**Example Problem:**
```
File: cpu_big_file.parquet
- Contains data from July 1 - Aug 15
- Cutoff: July 18
- Result: File is KEPT (because it has data after July 18)
```

**Solution:** Run compaction regularly to create smaller, time-ordered files.

#### Compaction Dependency
Retention policies work best when:
- Data is compacted into time-ordered files
- Files don't span large time ranges
- Partitioning aligns with retention period

### Storage Backends

#### Supported:
- ✅ **Local filesystem** - Fully implemented

#### Coming Soon:
- ⏳ **S3/MinIO/GCS/Ceph** - Cloud storage support

For cloud storage, the same logic applies but uses cloud APIs (e.g., `s3.delete_object()`) instead of `os.unlink()`.

### Monitoring & Verification

After execution, check:
1. **deleted_count** - Number of rows deleted
2. **execution_time_ms** - How long it took
3. **affected_measurements** - Which tables were processed
4. **Execution history** - Track all runs

Example response:
```json
{
  "policy_id": 1,
  "policy_name": "telegraf_90day_retention",
  "deleted_count": 150000,
  "execution_time_ms": 245.67,
  "dry_run": false,
  "cutoff_date": "2025-07-18T00:00:00",
  "affected_measurements": ["cpu", "mem", "disk"]
}
```

### Dry-Run First!

Always test with `dry_run: true` to see:
- How many rows would be deleted
- Which measurements are affected
- If the cutoff date is correct

```bash
curl -X POST /api/v1/retention/1/execute \
  -d '{"dry_run": true, "confirm": false}'
```

Response shows what **would** happen without actually deleting.
