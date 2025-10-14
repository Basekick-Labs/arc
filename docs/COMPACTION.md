# Compaction - File Optimization for Query Performance

This document explains Arc's automatic compaction system, how it works, and how to configure it.

## Table of Contents

- [Overview](#overview)
- [Why Compaction Matters](#why-compaction-matters)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Monitoring](#monitoring)
- [Performance Impact](#performance-impact)
- [Troubleshooting](#troubleshooting)
- [API Reference](#api-reference)

## Overview

**Compaction** is Arc's automatic file optimization system that merges small Parquet files into larger, more efficient files. This dramatically improves query performance and reduces storage overhead.

**Key Features:**
- ✅ **Automatic** - Runs on schedule (default: hourly at :05)
- ✅ **Configurable** - Flexible cron-style scheduling
- ✅ **Safe** - Locked partitions prevent concurrent compaction
- ✅ **Efficient** - Uses DuckDB for fast, parallel merging
- ✅ **Non-blocking** - Queries work during compaction
- ✅ **Crash-safe** - Automatic lock recovery
- ✅ **Enabled by default** - Essential for production

## Why Compaction Matters

### The Small File Problem

Arc's high-performance ingestion creates many small files:

```
At 2.11M records/sec with 5-second flush interval:
→ 9.65M records every 5 seconds
→ ~12 files per minute per measurement
→ ~720 files per hour per measurement
→ ~17,280 files per day per measurement
```

**Impact on Queries:**
- 🐌 **Slow queries** - DuckDB must open/scan hundreds of files
- 💸 **High costs** - More S3/MinIO API calls
- 📊 **Poor compression** - Small files compress less efficiently
- 🔍 **Reduced pruning** - Less effective partition elimination

### After Compaction

**Real Production Test Results:**
```
Before: 2,704 small files (Snappy) = 3.7 GB
After:  3 compacted files (ZSTD)   = 724 MB

Compression: 80.4% space savings
File reduction: 901x fewer files (2,704 → 3)
Compaction time: 5 seconds
```

**Per-Measurement Breakdown:**
- **mem**: 888 files → 1 file, 1,213 MB → 239 MB (80.3% compression)
- **disk**: 906 files → 1 file, 1,237 MB → 242 MB (80.4% compression)
- **cpu**: 910 files → 1 file, 1,246 MB → 243 MB (80.5% compression)

**Query Performance:**
- 🚀 **10-50x faster** - Single file scan vs hundreds
- 💰 **99% fewer API calls** - Massive cost reduction (2,704 → 3 LIST operations)
- 📦 **80.4% compression** - ZSTD compaction vs Snappy writes
- 🎯 **Effective pruning** - DuckDB can skip entire files

## How It Works

### High-Level Flow

```
1. Scheduler wakes up (cron: "5 * * * *")
   ↓
2. Scan storage for eligible partitions
   ↓
3. For each partition:
   - Check age (>1 hour old?)
   - Check file count (≥10 files?)
   - Check if already compacted?
   ↓
4. Acquire partition lock (SQLite)
   ↓
5. Download small files to temp directory
   ↓
6. Compact using DuckDB (parallel, sorted)
   ↓
7. Upload compacted file to storage
   ↓
8. Delete old small files
   ↓
9. Release lock & cleanup temp files
   ↓
10. Repeat for next partition
```

### Detailed Compaction Process

#### Phase 1: Discovery

```python
# List all databases in storage
databases = ["default", "production", "staging"]

# For each database, list all measurements
for database in databases:
    measurements = list_measurements(database)  # ["cpu", "mem", "disk", ...]

    # For each measurement, list hourly partitions
    partitions = [
        f"{database}/cpu/2025/10/08/14",  # 2PM hour
        f"{database}/cpu/2025/10/08/15",  # 3PM hour
        f"{database}/cpu/2025/10/08/16",  # 4PM hour (current - skip!)
    ]

    # Filter to eligible partitions
    eligible = filter(partitions, lambda p:
        age(p) > min_age_hours and        # Not active hour
        file_count(p) >= min_files and    # Enough files
        not has_compacted_file(p)         # Not already compacted
    )
```

#### Phase 2: Locking

```sql
-- Try to acquire lock in SQLite (atomic)
-- Lock includes full database-qualified path
INSERT INTO compaction_locks (partition_path, worker_id, locked_at, expires_at)
VALUES ('production/cpu/2025/10/08/14', 12345, NOW(), NOW() + 2 HOURS);

-- If INSERT succeeds → We have the lock
-- If INSERT fails (UNIQUE constraint) → Another worker has it

-- Different databases can compact same measurement/partition concurrently:
-- ✅ 'production/cpu/2025/10/08/14' (locked by worker 1)
-- ✅ 'staging/cpu/2025/10/08/14'    (locked by worker 2) - allowed!
```

**Lock Features:**
- **Automatic expiration** - Locks expire after 2 hours (crash recovery)
- **Per-partition** - Different partitions can compact concurrently
- **Database-aware** - Locks include database prefix, enabling parallel compaction across databases
- **Database-backed** - ACID guarantees, survives restarts
- **Distributed-ready** - Works across multiple Arc instances

#### Phase 3: Download

```python
# Download all small files in partition (database-qualified path)
files = list_objects(prefix="production/cpu/2025/10/08/14/")
# ["production/cpu/2025/10/08/14/cpu_20251008_140001_5000.parquet", ...]

temp_dir = "./data/compaction/production_cpu_2025_10_08_14/"
for file in files:
    download_file(file, temp_dir)
```

#### Phase 4: Compaction (DuckDB)

```sql
-- DuckDB reads all Parquet files and writes one optimized file
COPY (
    SELECT * FROM read_parquet('./data/compaction/*.parquet')
    ORDER BY time  -- Critical: maintain time-ordering
) TO 'cpu_20251008_14_compacted.parquet' (
    FORMAT PARQUET,
    COMPRESSION ZSTD,           -- Better compression than Snappy
    COMPRESSION_LEVEL 3,        -- Balance speed vs size
    ROW_GROUP_SIZE 122880       -- ~120K rows = optimal for DuckDB
);
```

**Why DuckDB?**
- ✅ Parallel processing (uses all CPU cores)
- ✅ Efficient memory usage (streams data)
- ✅ Optimized row groups for queries
- ✅ Better compression detection
- ✅ Automatic schema validation

#### Phase 5: Upload & Cleanup

```python
# Upload compacted file (database-qualified path)
upload_file(
    local="cpu_20251008_14_compacted.parquet",
    remote="production/cpu/2025/10/08/14/cpu_20251008_14_compacted.parquet"
)

# Delete old small files (atomic operation)
for file in old_files:
    delete_file(file)

# Cleanup temp directory
rmtree(temp_dir)

# Release lock (with database-qualified path)
DELETE FROM compaction_locks WHERE partition_path = 'production/cpu/2025/10/08/14';
```

### Handling Late Data

Arc uses a **side-file strategy** for late-arriving data:

```
Scenario: Hour 14 is compacted, but late data arrives for hour 14

Before compaction:
production/cpu/2025/10/08/14/
├── cpu_20251008_140001_5000.parquet
├── cpu_20251008_140006_5000.parquet
└── ... (100+ files)

After compaction:
production/cpu/2025/10/08/14/
└── cpu_20251008_14_compacted.parquet  (1 large file)

Late data arrives:
production/cpu/2025/10/08/14/
├── cpu_20251008_14_compacted.parquet  (original compacted file)
└── cpu_20251008_140500_late.parquet   (new small file for late data)

Query behavior:
DuckDB reads BOTH files automatically (database-scoped):
SELECT * FROM 's3://bucket/production/cpu/2025/10/08/14/*.parquet'
→ Scans compacted file + late file seamlessly
```

**Why side-file instead of recompaction?**
- ✅ **Avoids expensive recompaction** - Don't re-download/re-process large file
- ✅ **Query-friendly** - DuckDB handles multiple files transparently
- ✅ **Rare occurrence** - Late data is uncommon with proper time sync
- ✅ **Future compaction** - Next compaction cycle can merge side-files

## Architecture

### Multi-Database Support

Arc's compaction system is **database-aware**. Each database namespace is compacted independently:

```
Storage Structure:
{bucket}/{database}/{measurement}/{year}/{month}/{day}/{hour}/

Examples:
- s3://arc-data/default/cpu/2025/10/08/14/*.parquet
- s3://arc-data/production/cpu/2025/10/08/14/*.parquet
- s3://arc-data/staging/cpu/2025/10/08/14/*.parquet
```

**How It Works:**
- Each database has its own compaction schedule and state
- Compaction jobs are scoped to a single database
- Locks are per-partition within a database
- Statistics track per-database metrics

**Configuration:**
```toml
# Compaction applies to all databases
[compaction]
enabled = true
schedule = "5 * * * *"

# Each database is compacted independently
# default/cpu/2025/10/08/14 → compacted separately from
# production/cpu/2025/10/08/14
```

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Compaction System                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐     ┌──────────────────┐             │
│  │ Scheduler        │────▶│ Manager          │             │
│  │ (Cron-based)     │     │ (Job executor)   │             │
│  └──────────────────┘     └─────────┬────────┘             │
│                                      │                       │
│                                      ▼                       │
│                           ┌──────────────────┐              │
│                           │ CompactionJob    │              │
│                           │ (Per partition)  │              │
│                           └─────────┬────────┘              │
│                                      │                       │
│         ┌────────────────────────────┼────────────────┐     │
│         │                            │                │     │
│         ▼                            ▼                ▼     │
│  ┌──────────┐              ┌──────────────┐   ┌──────────┐│
│  │ Lock     │              │ Storage      │   │ DuckDB   ││
│  │ Manager  │              │ Backend      │   │ Engine   ││
│  │ (SQLite) │              │ (MinIO/S3)   │   │ (Merge)  ││
│  └──────────┘              └──────────────┘   └──────────┘│
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Class Hierarchy

```python
# storage/compaction.py
class CompactionJob:
    """Single partition compaction job"""
    def __init__(measurement, partition_path, files, storage_backend):
        ...

    async def run() -> bool:
        # Download → Compact → Upload → Delete
        ...

class CompactionManager:
    """Orchestrates all compaction jobs"""
    def __init__(storage_backend, lock_manager, config):
        ...

    async def find_candidates() -> List[Partition]:
        # Scan storage for eligible partitions
        ...

    async def compact_partition(partition) -> bool:
        # Acquire lock → Create job → Run → Release lock
        ...

    async def run_compaction_cycle():
        # Find candidates → Compact each (respecting max_concurrent)
        ...

# storage/compaction_scheduler.py
class CompactionScheduler:
    """Cron-style scheduler"""
    def __init__(compaction_manager, schedule="5 * * * *"):
        ...

    async def start():
        # Parse cron → Wait for next run → Trigger compaction
        ...

# api/database.py
class CompactionLock:
    """SQLite-based distributed locking"""
    def acquire_lock(partition_path, ttl_hours=2) -> bool:
        # INSERT into compaction_locks (atomic)
        ...

    def release_lock(partition_path):
        # DELETE from compaction_locks
        ...
```

## Configuration

### arc.conf

```toml
[compaction]
# Enable automatic compaction (recommended: true)
enabled = true

# Compact partitions older than this (don't compact active hour)
# Recommended: 1 hour (wait for hour to complete)
min_age_hours = 1

# Only compact partitions with at least this many files
# Lower = more aggressive, higher = less overhead
# Recommended: 10 files
min_files = 10

# Target file size for compacted Parquet files (MB)
# Recommended: 512MB (DuckDB's sweet spot)
#   256MB = More granular, faster compaction
#   512MB = Balanced (recommended)
#   1024MB = Fewer files, best compression
target_file_size_mb = 512

# Compaction schedule (cron syntax: minute hour day month weekday)
# Default: "5 * * * *" = Every hour at :05
# Examples:
#   "5 * * * *"        - Every hour at :05 (recommended)
#   "0 2,14 * * *"     - 2am and 2pm daily
#   "*/15 * * * *"     - Every 15 minutes (aggressive)
#   "0 3 * * 0"        - 3am every Sunday (weekly)
schedule = "5 * * * *"

# Max concurrent compaction jobs
# Recommended: 2-4 (balance speed vs resource usage)
max_concurrent_jobs = 2

# Lock time-to-live in hours (crash recovery)
lock_ttl_hours = 2

# Temp directory for compaction work
temp_dir = "./data/compaction"

# Performance tuning
compression = "zstd"           # zstd = better compression, snappy = faster
compression_level = 3          # 1-9 (higher = better compression, slower)
```

### Environment Variables

Override arc.conf settings via environment variables:

```bash
COMPACTION_ENABLED=true
COMPACTION_MIN_AGE_HOURS=1
COMPACTION_MIN_FILES=10
COMPACTION_TARGET_FILE_SIZE_MB=512
COMPACTION_SCHEDULE="5 * * * *"
COMPACTION_MAX_CONCURRENT_JOBS=2
COMPACTION_LOCK_TTL_HOURS=2
COMPACTION_TEMP_DIR="./data/compaction"
COMPACTION_COMPRESSION="zstd"
COMPACTION_COMPRESSION_LEVEL=3
```

### Reducing File Count at Source (Recommended First Step)

**Before tuning compaction, reduce file generation at the source:**

```toml
[ingestion]
buffer_size = 200000           # Up from 50,000 (4x fewer files)
buffer_age_seconds = 10        # Up from 5 (2x fewer files)
```

**Impact:**
- Files generated: 2,000/hour → 250/hour (8x reduction)
- Compaction time: 150s → 20s (7x faster)
- Memory: +300MB per worker
- Trade-off: 10s query freshness vs 5s

**This is the most effective optimization** - fewer files means faster compaction AND faster queries.

### Use Case Configurations

#### High-Volume Production (Recommended)

```toml
# Reduce files at source first!
[ingestion]
buffer_size = 200000
buffer_age_seconds = 10

# Then configure compaction
[compaction]
enabled = true
min_age_hours = 1              # Wait for hour to complete
min_files = 10                 # Don't compact tiny partitions
target_file_size_mb = 512      # DuckDB sweet spot
schedule = "5 * * * *"         # Every hour at :05
max_concurrent_jobs = 4        # Parallel compaction
compression = "zstd"           # Best compression
compression_level = 3          # Balanced
```

#### Low-Volume / Testing

```toml
[compaction]
enabled = true
min_age_hours = 0              # Compact current hour too
min_files = 2                  # Aggressive compaction
target_file_size_mb = 256      # Smaller files
schedule = "*/15 * * * *"      # Every 15 minutes
max_concurrent_jobs = 2
compression = "snappy"         # Faster
compression_level = 1
```

#### Cost-Optimized (Minimize Storage)

```toml
[compaction]
enabled = true
min_age_hours = 1
min_files = 5                  # Compact aggressively
target_file_size_mb = 1024     # Larger files = better compression
schedule = "5 * * * *"
max_concurrent_jobs = 2
compression = "zstd"
compression_level = 9          # Maximum compression
```

#### Performance-First (Minimize Compaction Overhead)

```toml
[compaction]
enabled = true
min_age_hours = 2              # Less frequent
min_files = 50                 # Only big partitions
target_file_size_mb = 512
schedule = "0 3 * * *"         # Once daily at 3am
max_concurrent_jobs = 1        # Low resource usage
compression = "snappy"
compression_level = 1
```

## Monitoring

### API Endpoints

#### GET /api/compaction/status

Get current compaction status:

```bash
curl -H "x-api-key: YOUR_TOKEN" \
  http://localhost:8000/api/compaction/status
```

Response:
```json
{
  "scheduler": {
    "enabled": true,
    "running": true,
    "schedule": "5 * * * *",
    "next_run": "2025-10-08T16:05:00"
  },
  "manager": {
    "active_jobs": 2,
    "total_completed": 47,
    "total_failed": 1
  }
}
```

#### GET /api/compaction/stats

Get detailed statistics:

```bash
curl -H "x-api-key: YOUR_TOKEN" \
  http://localhost:8000/api/compaction/stats
```

Response:
```json
{
  "total_jobs_completed": 47,
  "total_jobs_failed": 1,
  "total_files_compacted": 3384,
  "total_bytes_saved": 12884901888,
  "total_bytes_saved_mb": 12288.0,
  "active_jobs": 2,
  "recent_jobs": [
    {
      "job_id": "cpu_2025_10_08_14_1728409500",
      "measurement": "cpu",
      "partition_path": "cpu/2025/10/08/14",
      "status": "completed",
      "files_compacted": 72,
      "bytes_before": 360000000,
      "bytes_after": 51200000,
      "compression_ratio": 0.858,
      "duration_seconds": 45.3,
      "started_at": "2025-10-08T14:05:00",
      "completed_at": "2025-10-08T14:05:45"
    }
  ]
}
```

#### GET /api/compaction/candidates

See what partitions are eligible for compaction:

```bash
curl -H "x-api-key: YOUR_TOKEN" \
  http://localhost:8000/api/compaction/candidates
```

Response:
```json
{
  "count": 3,
  "candidates": [
    {
      "measurement": "cpu",
      "partition_path": "cpu/2025/10/08/14",
      "file_count": 72,
      "files": ["cpu_20251008_140001.parquet", "..."]
    },
    {
      "measurement": "mem",
      "partition_path": "mem/2025/10/08/14",
      "file_count": 68,
      "files": ["mem_20251008_140001.parquet", "..."]
    }
  ]
}
```

#### POST /api/compaction/trigger

Manually trigger compaction (doesn't wait for schedule):

```bash
curl -X POST -H "x-api-key: YOUR_TOKEN" \
  http://localhost:8000/api/compaction/trigger
```

Response:
```json
{
  "message": "Compaction triggered",
  "status": "running"
}
```

#### GET /api/compaction/jobs

Get currently active compaction jobs:

```bash
curl -H "x-api-key: YOUR_TOKEN" \
  http://localhost:8000/api/compaction/jobs
```

#### GET /api/compaction/history

Get recent job history:

```bash
curl -H "x-api-key: YOUR_TOKEN" \
  http://localhost:8000/api/compaction/history?limit=10
```

### Logs

Compaction logs appear in Arc's structured logs:

```json
// Scheduler starts
{"logger": "storage.compaction_scheduler", "message": "Compaction scheduler started (schedule: 5 * * * *)"}

// Next run scheduled
{"logger": "storage.compaction_scheduler", "message": "Next compaction scheduled for 2025-10-08 16:05:00 (3600s from now)"}

// Compaction cycle starts
{"logger": "storage.compaction_scheduler", "message": "Triggering scheduled compaction at 2025-10-08T16:05:00"}

// Found candidates
{"logger": "storage.compaction", "message": "Found 5 compaction candidates"}

// Job starts
{"logger": "storage.compaction", "message": "Starting compaction job cpu_2025_10_08_14: 72 files in cpu/2025/10/08/14"}

// Progress
{"logger": "storage.compaction", "message": "Downloaded 72 files (360.0 MB)"}
{"logger": "storage.compaction", "message": "Compacted to cpu_20251008_14_compacted.parquet (51.2 MB)"}

// Job completes
{"logger": "storage.compaction", "message": "Compaction job completed: 72 files → 1 file, 360.0 MB → 51.2 MB (85.8% compression), duration: 45.3s"}

// Cycle completes
{"logger": "storage.compaction_scheduler", "message": "Compaction cycle completed in 128.5s"}
```

## Performance Impact

### Compaction Overhead

**Resource Usage:**
- **CPU**: Moderate (DuckDB uses multiple cores)
- **Memory**: ~2-4GB per concurrent job
- **Disk I/O**: Moderate (temp files during compaction)
- **Network**: High (download + upload files)

**Recommendations:**
- Run during low-traffic periods (configure `schedule`)
- Limit `max_concurrent_jobs` (default: 2)
- Use local NVMe for `temp_dir` (faster I/O)

### Query Performance Improvement

**Before Compaction:**
```
Query: SELECT AVG(usage_idle) FROM cpu WHERE time > NOW() - INTERVAL 1 HOUR

Files scanned: 720 small files
Query time: 4.5 seconds
API calls: 720 (S3 LIST + GET operations)
```

**After Compaction:**
```
Query: SELECT AVG(usage_idle) FROM cpu WHERE time > NOW() - INTERVAL 1 HOUR

Files scanned: 1 large file
Query time: 0.12 seconds  (37x faster!)
API calls: 2 (S3 LIST + GET operations)
```

### Storage Savings

**Measured compression ratios from production testing:**

```
Test: High-volume ingestion (3 measurements: cpu, mem, disk)

Before compaction (Snappy):  2,704 files = 3.7 GB
After compaction (ZSTD):     3 files     = 724 MB

Space savings: 80.4% compression ratio
File reduction: 901x fewer files

Per-measurement breakdown:
- mem:  888 files (1,213 MB) → 1 file (239 MB) = 80.3% compression
- disk: 906 files (1,237 MB) → 1 file (242 MB) = 80.4% compression
- cpu:  910 files (1,246 MB) → 1 file (243 MB) = 80.5% compression
```

**Why such high compression?**
- ZSTD (compaction) vs Snappy (writes): ZSTD achieves 2-3x better compression
- Larger blocks: More data = better compression dictionary learning
- Sorted data: Time-ordered data compresses extremely well
- Columnar format: Parquet's columnar layout + compression = optimal efficiency

## Troubleshooting

### Compaction Not Running

**Check scheduler status:**
```bash
curl -H "x-api-key: TOKEN" http://localhost:8000/api/compaction/status
```

**Common causes:**
- ❌ `enabled = false` in arc.conf
- ❌ No eligible partitions (too new, too few files)
- ❌ Scheduler crashed (check logs)
- ❌ Invalid cron schedule

**Solution:**
```bash
# Check configuration
grep "compaction" arc.conf

# Check logs for errors
grep -i "compaction" logs/arc-api.log

# Manually trigger
curl -X POST -H "x-api-key: TOKEN" \
  http://localhost:8000/api/compaction/trigger
```

### Compaction Jobs Failing

**Check failed jobs:**
```bash
curl -H "x-api-key: TOKEN" \
  http://localhost:8000/api/compaction/stats | jq '.recent_jobs[] | select(.status=="failed")'
```

**Common causes:**
- ❌ Out of disk space (temp directory)
- ❌ Storage backend unavailable
- ❌ Permission errors
- ❌ Corrupted Parquet files

**Solution:**
```bash
# Check disk space
df -h

# Check temp directory
ls -lh ./data/compaction/

# Check storage connection
curl -H "x-api-key: TOKEN" http://localhost:8000/api/connections/storage
```

### Stuck Locks

**Symptoms:**
- Compaction never runs for certain partitions
- Logs show "partition already locked"

**Check active locks:**
```bash
sqlite3 ./data/arc.db "SELECT * FROM compaction_locks;"
```

**Clean up expired locks:**
```python
# Locks auto-expire after lock_ttl_hours (default: 2)
# Or manually cleanup:
sqlite3 ./data/arc.db "DELETE FROM compaction_locks WHERE expires_at < datetime('now');"
```

### Slow Compaction

**Check job duration:**
```bash
curl -H "x-api-key: TOKEN" \
  http://localhost:8000/api/compaction/stats | jq '.recent_jobs[].duration_seconds'
```

**Optimization tips:**
- ✅ Use local NVMe for `temp_dir`
- ✅ Increase `max_concurrent_jobs` (if resources allow)
- ✅ Use `compression = "snappy"` (faster than zstd)
- ✅ Reduce `compression_level` (1-3 instead of 9)
- ✅ Increase `target_file_size_mb` (fewer, larger files)

## API Reference

### CompactionManager

```python
from storage.compaction import CompactionManager

manager = CompactionManager(
    storage_backend=storage,
    lock_manager=lock,
    database="default",  # Database namespace
    min_age_hours=1,
    min_files=10,
    target_size_mb=512,
    max_concurrent=2
)

# Find eligible partitions (within the database)
candidates = await manager.find_candidates()

# Compact a partition (paths are relative to database)
success = await manager.compact_partition(
    measurement="cpu",
    partition_path="cpu/2025/10/08/14",
    files=[...]
)

# Run full compaction cycle
await manager.run_compaction_cycle()

# Get statistics
stats = manager.get_stats()
```

### CompactionScheduler

```python
from storage.compaction_scheduler import CompactionScheduler

scheduler = CompactionScheduler(
    compaction_manager=manager,
    schedule="5 * * * *",
    enabled=True
)

# Start scheduler
await scheduler.start()

# Manual trigger
await scheduler.trigger_now()

# Get next run time
next_run = scheduler.get_next_run()

# Stop scheduler
await scheduler.stop()
```

### CompactionLock

```python
from api.database import CompactionLock

lock = CompactionLock(db_path="./data/arc.db")

# Acquire lock
if lock.acquire_lock("cpu/2025/10/08/14", ttl_hours=2):
    # Do work
    ...
    # Release lock
    lock.release_lock("cpu/2025/10/08/14")

# Get active locks
locks = lock.get_active_locks()

# Cleanup expired locks
cleaned = lock.cleanup_expired_locks()
```

---

**Need help?** Open an issue at https://github.com/Basekick-Labs/arc/issues
