# Write-Ahead Log (WAL) - Durability Feature

This document explains Arc's Write-Ahead Log (WAL) feature for providing durability guarantees.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Performance Impact](#performance-impact)
- [Operations](#operations)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Overview

The Write-Ahead Log (WAL) is an optional feature that provides **zero data loss guarantees** on system crashes. When enabled, Arc persists all incoming data to disk **before** acknowledging writes, ensuring that data can be recovered even if the instance crashes.

### When to Enable WAL

Enable WAL if you need:
- ✅ **Zero data loss** on system crashes
- ✅ **Guaranteed durability** for regulatory compliance
- ✅ **Recovery from unexpected failures** (power loss, OOM kills, etc.)

Keep WAL disabled if you:
- ⚡ **Prioritize maximum throughput** (1.89M records/sec)
- 💰 **Can tolerate 0-5 seconds of data loss** on rare crashes
- 🔄 **Have client-side retry logic** or message queue upstream

### Performance vs Durability Tradeoff

```
Without WAL:        1.93M records/sec → 0-5 seconds data loss risk
WAL + fdatasync:    1.56M records/sec → Near-zero data loss risk
WAL + fsync:        1.61M records/sec → Zero data loss risk

Tradeoff: 19% throughput reduction for durability (fdatasync mode)
```

## Architecture

### Data Flow with WAL

```
┌──────────────────────────────────────────────────────────┐
│  HTTP Request (MessagePack or Line Protocol)             │
└──────────────────┬───────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────┐
│  1. WAL.append(records)                                  │
│     - Serialize to MessagePack binary                    │
│     - Calculate CRC32 checksum                           │
│     - Write to disk                                      │
│     - fdatasync() ← Force physical disk sync             │
└──────────────────┬───────────────────────────────────────┘
                   │
                   ▼ Data is DURABLE (on disk)
┌──────────────────────────────────────────────────────────┐
│  2. HTTP 202 Accepted ← Response to client               │
└──────────────────┬───────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────┐
│  3. Buffer.write(records)                                │
│     - Add to in-memory buffer                            │
│     - Flush when 50K records or 5 seconds               │
└──────────────────┬───────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────┐
│  4. Parquet Writer                                       │
│     - Convert to Arrow columnar format                   │
│     - Write Parquet file                                 │
│     - Upload to S3/MinIO                                 │
└──────────────────┬───────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────┐
│  5. WAL.mark_completed() ← Can now delete WAL entry      │
└──────────────────────────────────────────────────────────┘
```

**Key insight:** Once WAL confirms the write (step 1), the data is **guaranteed durable** even if Arc crashes before step 4 completes.

### Per-Worker WAL Files

Arc uses Gunicorn with multiple worker processes. Each worker has its own WAL file to avoid lock contention:

```
./data/wal/
├── worker-1-20251008_140530.wal
├── worker-2-20251008_140530.wal
├── worker-3-20251008_140530.wal
└── worker-4-20251008_140530.wal
```

**Benefits:**
- ✅ Zero lock contention (parallel writes)
- ✅ Simple implementation
- ✅ Natural partitioning
- ✅ Parallel recovery on startup

### Multi-Database Support

WAL records include database information when the `x-arc-database` header is used:

```python
# Write to specific database
POST /write/v2/msgpack
Headers:
  x-arc-database: production

# WAL entry stores database in record metadata
{
  "measurement": "cpu",
  "_database": "production",  # Database override
  "fields": {...}
}

# On recovery, records are routed to correct database
```

**Database Routing:**
- Records written without `x-arc-database` → `default` database
- Records with `x-arc-database: production` → `production` database
- WAL recovery preserves database routing during replay

### WAL File Format

Binary format optimized for fast writes and reliable recovery:

```
WAL File Structure:
┌─────────────────────────────────────────────────────┐
│ Header (7 bytes):                                   │
│   - Magic: "ARCW" (4 bytes)                         │
│   - Version: 0x0001 (2 bytes)                       │
│   - Checksum Type: CRC32 (1 byte)                   │
├─────────────────────────────────────────────────────┤
│ Entry 1:                                            │
│   - Payload Length: 4 bytes (uint32)                │
│   - Timestamp: 8 bytes (uint64, microseconds)       │
│   - Checksum: 4 bytes (CRC32)                       │
│   - MessagePack Payload: N bytes                    │
├─────────────────────────────────────────────────────┤
│ Entry 2: ...                                        │
│ Entry 3: ...                                        │
└─────────────────────────────────────────────────────┘
```

**Features:**
- Binary format (fast serialization)
- CRC32 checksums (detect corruption)
- MessagePack payload (compact, fast)
- Self-describing (magic + version)

## Configuration

### Enable WAL

Edit `.env` file:

```bash
# Enable WAL with balanced durability
WAL_ENABLED=true
WAL_DIR=./data/wal
WAL_SYNC_MODE=fdatasync        # Recommended for production
WAL_MAX_SIZE_MB=100            # Rotate at 100MB
WAL_MAX_AGE_SECONDS=3600       # Rotate after 1 hour
```

### Sync Modes

Arc supports three sync modes with different durability/performance tradeoffs:

| Mode | Throughput | Data Loss Risk | Use Case |
|------|------------|----------------|----------|
| `fsync` | ~1.61M rec/s | **Zero** | Financial, regulated industries |
| `fdatasync` | ~1.56M rec/s | **Near-zero** | Production (recommended) |
| `async` | ~1.60M rec/s | <1 second | High-throughput, can tolerate small loss |

#### fdatasync (Recommended)

```bash
WAL_SYNC_MODE=fdatasync
```

**How it works:**
- Syncs data to disk (file contents)
- Skips metadata sync (file size, modified time)
- 50% faster than `fsync`, nearly same durability

**Guarantees:**
- Data is on physical disk
- Can recover all data on crash
- File metadata may be stale (not critical)

#### fsync (Maximum Safety)

```bash
WAL_SYNC_MODE=fsync
```

**How it works:**
- Syncs both data AND metadata to disk
- Slowest, but absolute guarantee

**Use when:**
- Regulatory compliance requires it
- Zero tolerance for any data loss
- Performance is secondary

#### async (Performance-First)

```bash
WAL_SYNC_MODE=async
```

**How it works:**
- Writes to OS buffer cache
- No explicit sync (OS flushes periodically)
- Very fast, but small risk window

**Use when:**
- Need 90% of original throughput
- Can tolerate ~1 second data loss
- Have upstream retry mechanisms

### Rotation Settings

Control when WAL files rotate:

```bash
# Rotate when file reaches 100MB
WAL_MAX_SIZE_MB=100

# Rotate after 1 hour (even if file is small)
WAL_MAX_AGE_SECONDS=3600
```

**Why rotation matters:**
- Prevents unbounded growth
- Faster recovery (smaller files)
- Automatic cleanup of old WALs

## Performance Impact

### Benchmark Results

Tested on Apple M3 Max (14 cores, 36GB RAM):

| Configuration | Throughput | Data Loss Risk | Notes |
|--------------|-----------|----------------|-------|
| **No WAL (default)** | 1.93M rec/s | 0-5 seconds | Original Arc performance |
| **WAL + async** | 1.60M rec/s (-17%) | <1 second | Good balance, OS buffering |
| **WAL + fdatasync** | 1.56M rec/s (-19%) | Near-zero | **Recommended for production** |
| **WAL + fsync** | 1.61M rec/s (-17%) | Zero | Maximum safety, full metadata sync |

### Performance Tuning

**If WAL is too slow**, try:

1. **Use faster disks** (NVMe SSDs)
   ```bash
   # WAL benefits from fast storage
   WAL_DIR=/mnt/nvme/arc-wal
   ```

2. **Switch to async mode**
   ```bash
   WAL_SYNC_MODE=async  # Faster, small risk
   ```

3. **Increase buffer sizes** (fewer WAL writes)
   ```bash
   WRITE_BUFFER_SIZE=100000  # Batch more records
   ```

**If you need maximum durability**, use:

```bash
WAL_ENABLED=true
WAL_SYNC_MODE=fsync           # Strictest durability
WRITE_BUFFER_SIZE=10000       # Flush frequently
WRITE_BUFFER_AGE=1            # 1 second max buffer age
```

## Operations

### Recovery on Startup

Arc automatically recovers from WAL files on startup:

```
2025-10-08 14:30:00 [INFO] WAL recovery started: 4 files
2025-10-08 14:30:01 [INFO] Recovering WAL: worker-1-20251008_143000.wal
2025-10-08 14:30:01 [INFO] WAL read complete: 1000 entries, 5242880 bytes, 0 corrupted
2025-10-08 14:30:02 [INFO] Recovering WAL: worker-2-20251008_143000.wal
...
2025-10-08 14:30:05 [INFO] WAL recovery complete: 4000 batches, 200000 entries, 0 corrupted
2025-10-08 14:30:05 [INFO] WAL archived: worker-1-20251008_143000.wal.recovered
```

**Process:**
1. Find all `*.wal` files in `WAL_DIR`
2. Read and validate each entry (checksum verification)
3. Replay records into buffer system
4. Archive recovered WAL as `*.wal.recovered`
5. Continue normal operations

**Recovery time:**
- ~5 seconds per 100MB WAL file
- Parallel recovery across workers
- Corrupted entries are skipped (logged)

### Manual Recovery

If automatic recovery fails, manually replay WAL files:

```python
from storage.wal import WALReader, WALRecovery
from pathlib import Path

# Read a single WAL file
wal_file = Path('./data/wal/worker-1-20251008_143000.wal')
reader = WALReader(wal_file)
batches = reader.read_all()

print(f"Recovered {len(batches)} batches")
for batch in batches[:5]:  # First 5 batches
    print(f"  Batch: {len(batch)} records")
```

### Cleanup

Arc automatically cleans up old recovered WAL files after 24 hours:

```python
from storage.wal import WALRecovery

recovery = WALRecovery('./data/wal')
recovery.cleanup_old_wals(max_age_seconds=86400)  # 24 hours
```

Manual cleanup:

```bash
# Remove all recovered WAL files
rm -f ./data/wal/*.wal.recovered

# Archive old WAL files
mkdir -p ./data/wal/archive
mv ./data/wal/*.wal.recovered ./data/wal/archive/
```

## Monitoring

### WAL API Endpoints

Arc provides dedicated WAL monitoring endpoints at `/api/wal/*`:

#### Get WAL Status

```bash
curl http://localhost:8000/api/wal/status
```

Response:

```json
{
  "enabled": true,
  "writer_type": "Direct Arrow (zero-copy)",
  "configuration": {
    "sync_mode": "fdatasync",
    "worker_id": 1,
    "current_file": "./data/wal/worker-1-20251008_143000.wal"
  },
  "stats": {
    "worker_id": 1,
    "current_file": "./data/wal/worker-1-20251008_143000.wal",
    "current_size_mb": 45.2,
    "current_age_seconds": 1850,
    "sync_mode": "fdatasync",
    "total_entries": 5000,
    "total_bytes": 47382528,
    "total_syncs": 5000,
    "total_rotations": 2
  }
}
```

#### Get Detailed Statistics

```bash
curl http://localhost:8000/api/wal/stats
```

#### List WAL Files

```bash
curl http://localhost:8000/api/wal/files
```

Response:

```json
{
  "active": [
    {
      "name": "worker-1-20251008_143000.wal",
      "size_mb": 45.2,
      "modified": 1696775400,
      "path": "./data/wal/worker-1-20251008_143000.wal"
    }
  ],
  "recovered": [
    {
      "name": "worker-1-20251008_120000.wal.recovered",
      "size_mb": 98.5,
      "modified": 1696768800,
      "path": "./data/wal/worker-1-20251008_120000.wal.recovered"
    }
  ],
  "total_size_mb": 143.7,
  "wal_directory": "./data/wal"
}
```

#### WAL Health Check

```bash
curl http://localhost:8000/api/wal/health
```

Response:

```json
{
  "healthy": true,
  "wal_enabled": true,
  "issues": [],
  "recommendations": [],
  "stats_summary": {
    "current_size_mb": 45.2,
    "current_age_seconds": 1850,
    "total_entries": 5000,
    "total_rotations": 2
  }
}
```

#### Cleanup Old WAL Files

```bash
# Cleanup files older than 24 hours (default)
curl -X POST http://localhost:8000/api/wal/cleanup

# Custom age (in hours)
curl -X POST "http://localhost:8000/api/wal/cleanup?max_age_hours=48"
```

Response:

```json
{
  "deleted_count": 5,
  "freed_mb": 492.5,
  "max_age_hours": 24,
  "message": "Cleaned up 5 old WAL files, freed 492.5 MB"
}
```

#### Get Recovery History

```bash
curl http://localhost:8000/api/wal/recovery/history
```

Response:

```json
{
  "recovered_files": [
    {
      "name": "worker-1-20251008_120000.wal.recovered",
      "size_mb": 98.5,
      "recovered_at": 1696768800,
      "age_hours": 2.5
    }
  ],
  "last_recovery": 1696768800,
  "total_recovered": 5
}
```

### Legacy Buffer Stats

You can also get WAL stats from the buffer monitoring endpoint:

```bash
curl http://localhost:8000/api/monitoring/buffer
```

Response includes WAL metrics in the `wal` field.

### Key Metrics to Monitor

1. **current_size_mb** - Watch for files approaching `WAL_MAX_SIZE_MB`
2. **current_age_seconds** - Should rotate before `WAL_MAX_AGE_SECONDS`
3. **total_syncs** - High count indicates good write activity
4. **total_rotations** - Ensure rotation is happening regularly

### Alerts

Set up monitoring alerts for:

```bash
# WAL file growing too large
if wal.current_size_mb > 90:
    alert("WAL file near rotation limit")

# WAL file too old (rotation not happening)
if wal.current_age_seconds > 7200:  # 2 hours
    alert("WAL file not rotating")

# WAL directory disk usage
if disk_usage("./data/wal") > 80%:
    alert("WAL directory disk usage high")
```

## Troubleshooting

### Issue: WAL Recovery Taking Too Long

**Symptoms:**
```
2025-10-08 14:30:00 [INFO] WAL recovery started: 50 files
... (minutes pass) ...
```

**Causes:**
- Many small WAL files (rotation too frequent)
- Very large WAL files (rotation too infrequent)
- Slow disk I/O

**Solutions:**

1. Adjust rotation settings:
   ```bash
   WAL_MAX_SIZE_MB=50          # Smaller files, faster recovery
   WAL_MAX_AGE_SECONDS=1800    # Rotate more frequently
   ```

2. Use faster disks for WAL:
   ```bash
   WAL_DIR=/mnt/nvme/arc-wal   # NVMe SSD
   ```

3. Increase worker count:
   ```bash
   WORKERS=16  # More workers = parallel recovery
   ```

### Issue: WAL Disk Space Growing

**Symptoms:**
```bash
$ du -sh ./data/wal
5.2G    ./data/wal
```

**Causes:**
- Old `.wal.recovered` files not cleaned up
- Active WAL files not rotating
- Too many workers with large WAL files

**Solutions:**

1. Manual cleanup:
   ```bash
   rm -f ./data/wal/*.wal.recovered
   ```

2. Reduce retention:
   ```bash
   WAL_MAX_SIZE_MB=50      # Rotate sooner
   WAL_MAX_AGE_SECONDS=1800  # 30 minutes
   ```

3. Add cron job for cleanup:
   ```bash
   # Cleanup recovered WALs older than 24 hours
   0 2 * * * find /path/to/data/wal -name "*.wal.recovered" -mtime +1 -delete
   ```

### Issue: WAL Write Failures

**Symptoms:**
```
2025-10-08 14:30:00 [ERROR] WAL append failed: [Errno 28] No space left on device
2025-10-08 14:30:00 [ERROR] WAL append failed, records may be lost on crash
```

**Causes:**
- Disk full
- Permission issues
- I/O errors

**Solutions:**

1. Check disk space:
   ```bash
   df -h /path/to/WAL_DIR
   ```

2. Check permissions:
   ```bash
   ls -ld ./data/wal
   chmod 755 ./data/wal
   ```

3. Move WAL to larger disk:
   ```bash
   WAL_DIR=/mnt/large-disk/arc-wal
   ```

### Issue: Corrupted WAL Entries

**Symptoms:**
```
2025-10-08 14:30:01 [ERROR] WAL checksum mismatch at 102400: expected 12345678, got 87654321
```

**Causes:**
- Disk corruption
- System crash during write
- Hardware issues

**Solutions:**

1. Check disk health:
   ```bash
   # Linux
   smartctl -a /dev/sda

   # macOS
   diskutil verifyVolume /
   ```

2. Review logs for crashes:
   ```bash
   journalctl -u arc-core | grep -i crash
   ```

3. If corruption is frequent, consider:
   - Using `fsync` mode (slower but safer)
   - Replacing failing hardware
   - Running file system checks

### Issue: Performance Degradation with WAL

**Symptoms:**
- Throughput dropped from 1.89M to 600K rec/s
- High CPU usage from fsync calls

**Solutions:**

1. Verify sync mode:
   ```bash
   # Should be fdatasync, not fsync
   WAL_SYNC_MODE=fdatasync
   ```

2. Check disk I/O wait:
   ```bash
   iostat -x 1
   # Look for %iowait > 50%
   ```

3. Move WAL to faster disk:
   ```bash
   WAL_DIR=/mnt/nvme/arc-wal
   ```

4. Consider disabling WAL if durability isn't critical:
   ```bash
   WAL_ENABLED=false
   ```

## Best Practices

### Production Deployment

**Recommended configuration:**

```bash
# High durability, good performance
WAL_ENABLED=true
WAL_SYNC_MODE=fdatasync
WAL_DIR=/mnt/fast-ssd/arc-wal
WAL_MAX_SIZE_MB=100
WAL_MAX_AGE_SECONDS=3600
```

**Monitoring setup:**

1. Monitor WAL disk usage
2. Alert on write failures
3. Track recovery time during restarts
4. Log rotation metrics

**Backup strategy:**

- WAL files are ephemeral (deleted after recovery)
- Don't backup WAL files directly
- Backup final Parquet files in S3/MinIO instead

### Development/Testing

**Recommended configuration:**

```bash
# WAL disabled for maximum speed
WAL_ENABLED=false
```

**Or if testing WAL:**

```bash
# Fast iteration, async mode
WAL_ENABLED=true
WAL_SYNC_MODE=async
WAL_MAX_SIZE_MB=10  # Small files for testing
```

---

## Summary

**Enable WAL if:**
- ✅ Zero data loss is required
- ✅ Regulated industry (finance, healthcare)
- ✅ Can accept 37% throughput reduction

**Disable WAL if:**
- ⚡ Maximum throughput is priority
- 💰 Can tolerate 0-5s data loss risk
- 🔄 Have upstream retry/queue mechanisms

**Recommended settings:**
```bash
WAL_ENABLED=true
WAL_SYNC_MODE=fdatasync  # Best balance
WAL_DIR=/mnt/nvme/arc-wal  # Fast disk
```

For questions or issues, open a GitHub issue or join community discussions!
