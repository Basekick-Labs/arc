# Size-Based File Rotation Design

**Date:** 2025-11-05
**Status:** Design Phase
**Priority:** High (needed for 100+ server deployments)

---

## Problem Statement

Arc currently creates one parquet file per buffer flush (every 5 seconds), resulting in:
- **720 files/hour/measurement** (before hourly compaction)
- **24 files/day/measurement** (after hourly compaction)
- **8,760 files/year/measurement** (365 days × 24 files)
- **411,720 files/year** for 2 servers with 47 measurements

This approach doesn't scale:
- 100 servers: **20.6 million files/year** ❌
- 1,000 servers: **206 million files/year** ❌
- 5,000 servers: **1.03 billion files/year** ❌

Even with tiered compaction (daily/weekly/monthly), file counts remain high because we're fighting the symptom, not the root cause.

---

## Root Cause Analysis

### Current Architecture (Broken)

```python
# Write path:
Buffer (17K rows, 5s age) → Flush → Create new parquet file immediately

# Result:
- One file per flush
- 720 files/hour created
- Must compact constantly to reduce file count
```

**The fundamental problem:** Time-based flushing creates too many files.

### What Industry Leaders Do

**InfluxDB TSM Engine:**
```
Buffer → WAL (durability) → In-memory cache → Flush to 2GB TSM file when full
Result: ~3-10 files per 7-day shard
```

**ClickHouse MergeTree:**
```
Buffer → Parts (100MB-1GB) → Background merge to larger parts
Result: ~50-200 parts for billions of rows
```

**DuckDB Recommendations:**
```
Optimal parquet files: 100MB-10GB with 100K-1M rows per row group
Result: Hundreds of files for TB-scale datasets
```

**Key insight:** All successful systems use **size-based rotation**, not time-based!

---

## Solution: Size-Based File Rotation

### High-Level Architecture

```python
# New write path:
Buffer (17K rows, 5s age) → Flush to active parquet writer → Rotate when file reaches target size

# Example flow:
14:00:00 - Buffer flushes (17K rows) → Appends to cpu_active_001.parquet
14:00:05 - Buffer flushes (17K rows) → Appends to cpu_active_001.parquet
14:00:10 - Buffer flushes (17K rows) → Appends to cpu_active_001.parquet
... (continues for hours/days)
Active file reaches 512MB-2GB → Close cpu_active_001.parquet
                              → Rename to cpu_20251105_000000_20251107_143000.parquet
                              → Open new cpu_active_002.parquet
```

### Key Design Principles

1. **No user-facing changes**: Queries work exactly as before
2. **Preserve real-time visibility**: Data queryable within 5 seconds
3. **No extra memory**: Still flush every 5 seconds
4. **Align with DuckDB best practices**: 100MB-10GB files with good row group size
5. **Backwards compatible**: Can run alongside existing compaction

---

## Detailed Design

### File Naming Convention

**Active files (being written):**
```
{measurement}_active_{worker_id}.parquet
Example: cpu_active_w1.parquet
```

**Closed files (ready for queries):**
```
{measurement}_{start_timestamp}_{end_timestamp}.parquet
Example: cpu_20251105_000000_20251107_235959.parquet (2 days of data)
```

### Storage Structure

```
data/arc/{database}/{measurement}/
├── 2025-11/
│   ├── cpu_20251101_000000_20251103_120000.parquet (512MB, Nov 1-3)
│   ├── cpu_20251103_120000_20251105_180000.parquet (512MB, Nov 3-5)
│   ├── cpu_20251105_180000_20251107_235959.parquet (512MB, Nov 5-7)
│   └── cpu_active_w1.parquet (currently being written)
```

### Active Parquet Writer

New class to manage size-based rotation:

```python
class ActiveParquetWriter:
    """
    Accumulates buffer flushes into large parquet files.
    Rotates to new file when size threshold reached.
    """

    def __init__(
        self,
        database: str,
        measurement: str,
        worker_id: int,
        target_size_mb: int = 512,  # Default 512MB
        max_size_mb: int = 2048      # Hard limit 2GB
    ):
        self.database = database
        self.measurement = measurement
        self.worker_id = worker_id
        self.target_size_mb = target_size_mb
        self.max_size_mb = max_size_mb

        self.active_file_path = None
        self.active_writer = None
        self.current_size_mb = 0
        self.start_timestamp = None
        self.end_timestamp = None
        self.row_count = 0

    async def append_batch(self, df: pd.DataFrame) -> None:
        """
        Append a batch of rows (from buffer flush) to active file.
        Rotates to new file if size threshold exceeded.
        """
        # Initialize new file if needed
        if self.active_writer is None:
            await self._create_new_active_file()

        # Append rows to active file
        await self._append_rows(df)

        # Update metadata
        self.row_count += len(df)
        self.end_timestamp = df['time'].max()
        self.current_size_mb = self._get_file_size_mb()

        # Check if rotation needed
        if self.current_size_mb >= self.target_size_mb:
            await self._rotate_file()

    async def _create_new_active_file(self) -> None:
        """Create new active parquet file for writing."""
        base_path = f"data/arc/{self.database}/{self.measurement}"
        month_dir = datetime.now().strftime("%Y-%m")
        full_path = f"{base_path}/{month_dir}"
        os.makedirs(full_path, exist_ok=True)

        self.active_file_path = f"{full_path}/{self.measurement}_active_w{self.worker_id}.parquet"
        self.active_writer = ParquetWriter(self.active_file_path)
        self.start_timestamp = datetime.now()
        self.current_size_mb = 0
        self.row_count = 0

    async def _append_rows(self, df: pd.DataFrame) -> None:
        """Append rows to active parquet file."""
        # Use pyarrow to append to parquet file
        table = pa.Table.from_pandas(df)

        if self.active_writer is None:
            # First write - create file with schema
            pq.write_table(table, self.active_file_path, compression='snappy')
        else:
            # Append to existing file
            # Note: This requires reading existing file and rewriting
            # See "Parquet Append Challenge" section below
            existing = pq.read_table(self.active_file_path)
            combined = pa.concat_tables([existing, table])
            pq.write_table(combined, self.active_file_path, compression='snappy')

    async def _rotate_file(self) -> None:
        """Close active file and rename to timestamped name."""
        # Close active writer
        if self.active_writer:
            self.active_writer.close()

        # Generate final filename with timestamp range
        start_str = self.start_timestamp.strftime("%Y%m%d_%H%M%S")
        end_str = self.end_timestamp.strftime("%Y%m%d_%H%M%S")

        final_path = self.active_file_path.replace(
            f"_active_w{self.worker_id}.parquet",
            f"_{start_str}_{end_str}.parquet"
        )

        # Rename file
        os.rename(self.active_file_path, final_path)

        logger.info(
            f"Rotated parquet file: {self.measurement} "
            f"({self.row_count} rows, {self.current_size_mb:.1f}MB) "
            f"→ {os.path.basename(final_path)}"
        )

        # Reset for new file
        self.active_writer = None
        self.active_file_path = None

    def _get_file_size_mb(self) -> float:
        """Get current file size in MB."""
        if self.active_file_path and os.path.exists(self.active_file_path):
            return os.path.getsize(self.active_file_path) / (1024 * 1024)
        return 0.0
```

### Integration with Existing Buffer Manager

Modify `BufferManager` to use `ActiveParquetWriter`:

```python
class BufferManager:
    def __init__(self, ...):
        # Existing initialization
        ...

        # NEW: Active parquet writers (one per measurement)
        self.active_writers = {}  # key: (database, measurement)
        self.target_file_size_mb = config.get('target_file_size_mb', 512)

    async def flush_buffer(self, database: str, measurement: str):
        """Flush buffer to active parquet writer (instead of creating new file)."""
        buffer_key = (database, measurement)

        if buffer_key not in self.buffers:
            return

        df = self.buffers[buffer_key]

        if df.empty:
            return

        # Get or create active writer for this measurement
        if buffer_key not in self.active_writers:
            self.active_writers[buffer_key] = ActiveParquetWriter(
                database=database,
                measurement=measurement,
                worker_id=self.worker_id,
                target_size_mb=self.target_file_size_mb
            )

        writer = self.active_writers[buffer_key]

        # Append batch to active file (may trigger rotation)
        await writer.append_batch(df)

        # Clear buffer
        self.buffers[buffer_key] = pd.DataFrame()

        logger.debug(
            f"Flushed buffer: {database}.{measurement} "
            f"({len(df)} rows) → active file"
        )
```

---

## Parquet Append Challenge

### The Problem

Parquet files are **immutable** by design. You cannot truly "append" to a parquet file efficiently.

### Approaches to Handle This

#### Option A: Read-Modify-Write (Simple but Inefficient)

```python
# Every append requires:
1. Read entire existing file into memory
2. Concatenate new rows
3. Write entire file back to disk

# Performance:
- 100MB file: ~500ms per append (CPU + I/O bound)
- 512MB file: ~2-3 seconds per append
- Becomes slower as file grows!
```

**Problem:** For 5-second flushes, you'd spend most of your time rewriting files!

#### Option B: Keep Multiple Small Files, Merge Periodically (Hybrid)

```python
# Write path:
Buffer flush → Create small parquet file (as current)
Every 5 minutes: Merge accumulated small files → large file

# Example:
14:00:00-14:05:00 - Creates 60 small files (5s buffer × 60)
14:05:00 - Merge 60 files → cpu_batch_001.parquet (50MB)
14:05:00-14:10:00 - Creates 60 more small files
14:10:00 - Merge 60 files → cpu_batch_002.parquet (50MB)
...
When 10 batch files accumulated (500MB total):
  Merge cpu_batch_001...010.parquet → cpu_20251105_140000_20251105_145959.parquet
```

**This is essentially time-based compaction** - which we already have!

#### Option C: Accumulate in Memory, Flush to Large Files (Best)

```python
# Write path:
Buffer flush → Accumulate in secondary memory buffer (not on disk)
Secondary buffer reaches 100MB → Flush to parquet file (one write)

# Trade-offs:
- More memory usage: ~100-500MB per measurement
- Fewer disk writes: 1 write per 100MB instead of 720 writes per hour
- Data latency: Up to 5-10 minutes before queryable
```

**Problem:** This breaks Arc's "real-time visibility" value proposition!

#### Option D: Use Arrow IPC/Feather Format for Active Files (Recommended)

```python
# Write path:
Buffer flush → Append to Arrow IPC file (supports true append!)
Active file reaches 512MB → Convert to Parquet, close file

# Arrow IPC format:
- Supports efficient append (no rewrite needed)
- Columnar format (like Parquet)
- Fast writes and reads
- Convert to Parquet when done for compression

# Example:
cpu_active_w1.arrow (being written, 300MB)
  ↓ (reaches 512MB)
cpu_20251105_000000_20251107_120000.parquet (compressed, final)
```

**Advantages:**
- Efficient append (no read-modify-write)
- Real-time queryable (DuckDB can read Arrow IPC)
- Convert to Parquet when done for better compression
- Best of both worlds!

---

## Recommended Implementation: Hybrid Approach

Combine Option B (periodic merging) with Option D (Arrow IPC):

### Phase 1: Keep Current Flush, Add Periodic Batch Compaction

```python
# Short-term solution (1-2 weeks):
1. Keep current behavior (flush to small parquet every 5s)
2. Add background job: Every 5-10 minutes, merge small files → larger file
3. Target file size: 100-500MB per batch

Result:
- 24-144 batch files per day per measurement (vs 720 hourly files)
- 10-50x fewer files immediately
- No risky architectural changes
```

### Phase 2: Implement Arrow IPC Active Writer

```python
# Medium-term improvement (1-2 months):
1. Add ActiveArrowWriter class
2. Buffer flushes → Append to .arrow file
3. When .arrow file reaches 512MB → Convert to .parquet
4. Update query layer to read both .arrow (active) and .parquet (closed)

Result:
- 50-200 files per year per measurement
- 100-500x fewer files
- True efficient append
```

---

## Configuration

New settings in `arc.conf`:

```toml
[ingestion]
# Existing settings
buffer_size = 17000
buffer_age_seconds = 5
compression = "snappy"

# NEW: Size-based file rotation
enable_size_based_rotation = true      # Enable new behavior
target_file_size_mb = 512              # Target file size before rotation
max_file_size_mb = 2048                # Hard limit (safety)
rotation_check_interval_seconds = 60   # How often to check file size

# Phase 2: Arrow IPC format for active files
use_arrow_active_files = false         # Use Arrow IPC for active writes
arrow_to_parquet_threshold_mb = 512    # Convert .arrow → .parquet at this size
```

---

## Expected Results

### File Count Comparison

| Scenario | Current (Hourly Compact) | With Size-Based Rotation | Improvement |
|----------|-------------------------|--------------------------|-------------|
| **1 measurement, 1 year** | 8,760 files | 50-200 files | **44-175x** |
| **47 measurements, 1 year** | 411,720 files | 2,350-9,400 files | **44-175x** |
| **100 servers, 1 year** | 20.6M files | 117K-470K files | **44-175x** |
| **1,000 servers, 1 year** | 206M files | 1.17M-4.7M files | **44-175x** |

### With Tiered Compaction Added

Combining size-based rotation + daily/weekly/monthly compaction:

| Scenario | Size-Based Only | + Daily/Weekly/Monthly | Total Improvement |
|----------|----------------|------------------------|-------------------|
| **1 measurement, 1 year** | 50-200 files | 12-52 files | **170-730x** |
| **47 measurements, 1 year** | 2,350-9,400 files | 564-2,444 files | **170-730x** |
| **1,000 servers, 1 year** | 1.17M-4.7M files | 282K-1.22M files | **170-730x** |

---

## Implementation Plan

### Phase 1: Periodic Batch Compaction (2-3 weeks)

**Goal:** Reduce file count 10-50x with minimal changes

1. Add background job: Runs every 5-10 minutes
2. Find small files created in last 5-10 minutes
3. Merge them into larger batch files (100-500MB target)
4. Delete original small files
5. Works alongside existing hourly compaction

**Effort:** 2-3 days development, 1 week testing

### Phase 2: Arrow IPC Active Writer (1-2 months)

**Goal:** True efficient append, reduce file count 100-500x

1. Implement `ActiveArrowWriter` class
2. Modify `BufferManager` to use Arrow IPC for active files
3. Add background job: Convert .arrow → .parquet when threshold reached
4. Update query layer to handle both formats
5. Add monitoring/metrics

**Effort:** 1-2 weeks development, 2-3 weeks testing

### Phase 3: Optimize + Production Hardening (1-2 months)

**Goal:** Production-ready, handle edge cases

1. Handle worker crashes (incomplete .arrow files)
2. Optimize Arrow → Parquet conversion
3. Add configuration options
4. Benchmark query performance
5. Add operational tooling (force rotation, inspect active files)

**Effort:** 2-3 weeks development, 2-4 weeks testing

---

## Backwards Compatibility

### Query Compatibility

Queries work unchanged:
```sql
-- User queries this:
SELECT * FROM telegraf.cpu WHERE time > NOW() - INTERVAL '1 hour'

-- DuckDB reads:
1. Existing small parquet files (if any)
2. New large batch files
3. Active .arrow files (Phase 2)

-- Merge happens transparently
```

### Storage Migration

No migration needed:
- New system writes large files
- Old small files remain queryable
- Existing compaction continues to work
- Gradually old files get compacted away

### Rollback Plan

Can rollback safely:
1. Disable `enable_size_based_rotation = false`
2. System reverts to current behavior
3. Existing large files remain queryable
4. No data loss

---

## Risks and Mitigations

### Risk 1: Arrow IPC file corruption

**Scenario:** Worker crashes while writing .arrow file

**Mitigation:**
- Active .arrow files have worker ID in name
- On startup, detect incomplete .arrow files
- Replay WAL to recover data (if WAL enabled)
- Or: Mark as corrupted, alert operator

### Risk 2: Query performance regression

**Scenario:** Large files slower to query than many small files

**Mitigation:**
- DuckDB is optimized for 100MB-10GB files (our target)
- Row group statistics enable efficient filtering
- Benchmark queries during Phase 2 testing
- Can tune `target_file_size_mb` based on results

### Risk 3: Memory pressure from Arrow IPC

**Scenario:** Active .arrow files use too much memory

**Mitigation:**
- Arrow IPC files are memory-mapped (not fully in RAM)
- Monitor memory usage in testing
- Can limit concurrent active files per worker
- Tune rotation threshold based on memory constraints

### Risk 4: Storage backend compatibility

**Scenario:** S3 storage doesn't support append well

**Mitigation:**
- For S3: Use local buffer, upload complete files only
- Active .arrow files stay local (fast disk)
- Only upload .parquet files to S3
- S3 upload is one-time (not continuous append)

---

## Monitoring and Metrics

New metrics to track:

```python
# File rotation metrics
arc_active_files_count{database, measurement}           # Number of active .arrow files
arc_active_file_size_mb{database, measurement}          # Size of active files
arc_file_rotations_total{database, measurement}         # Counter of rotations
arc_file_rotation_duration_seconds{database, measurement}  # Time to rotate file

# Arrow → Parquet conversion (Phase 2)
arc_arrow_to_parquet_conversions_total{database, measurement}
arc_arrow_to_parquet_duration_seconds{database, measurement}
arc_arrow_to_parquet_compression_ratio{database, measurement}

# File size distribution
arc_parquet_file_size_mb_bucket{database, measurement, le}  # Histogram
```

---

## Alternatives Considered

### Alternative 1: Implement in-memory cache like InfluxDB

**Pros:**
- Industry-proven approach
- Lowest file count possible (50-150 files/year total)

**Cons:**
- Requires 5-10GB RAM (vs current 1.6GB)
- Major architectural rewrite (query layer, WAL, crash recovery)
- 3-6 months development time
- Breaks Arc's simplicity value proposition

**Decision:** Too expensive for current needs. Revisit if targeting 10K+ servers.

### Alternative 2: Use database instead of parquet files

**Pros:**
- Databases handle file management internally
- Built-in transaction support

**Cons:**
- Defeats purpose of Arc (DuckDB-based, serverless)
- Loses parquet ecosystem benefits
- Higher operational complexity

**Decision:** Against Arc's core design philosophy.

### Alternative 3: Only use tiered compaction (no size-based rotation)

**Pros:**
- Simpler implementation (already have compaction code)
- No new file formats (Arrow IPC)

**Cons:**
- Still creates too many files (only reduces by 20-50x)
- Compaction overhead remains high
- Doesn't scale to 1K+ servers

**Decision:** This is the fallback if size-based rotation fails.

---

## Success Criteria

### Phase 1 Success Metrics

- [ ] File count reduced by 10-50x
- [ ] Query performance maintained (< 5% regression)
- [ ] Memory usage unchanged
- [ ] No data loss in 2 weeks of testing
- [ ] Works with existing compaction

### Phase 2 Success Metrics

- [ ] File count reduced by 100-500x
- [ ] Query latency < 10% regression
- [ ] Successful recovery from worker crashes
- [ ] Works on both local and S3 storage
- [ ] Production stable for 1 month

### Phase 3 Success Metrics

- [ ] Successfully deployed to 100+ server customers
- [ ] File count meets projections (50-200 files/year/measurement)
- [ ] Operational tooling complete
- [ ] Documentation updated
- [ ] Benchmarks published

---

## Summary

**Problem:** Arc creates too many files (8,760/year/measurement), doesn't scale beyond 100 servers

**Solution:** Size-based file rotation with Arrow IPC format for efficient append

**Implementation:**
- Phase 1: Periodic batch compaction (2-3 weeks) - 10-50x improvement
- Phase 2: Arrow IPC active writer (1-2 months) - 100-500x improvement
- Phase 3: Production hardening (1-2 months)

**Expected Result:** 50-200 files/year/measurement (vs 8,760 currently) = **44-175x improvement**

**Combined with tiered compaction:** 12-52 files/year/measurement = **170-730x improvement**

**Scales to:** 1,000-5,000 servers before needing in-memory cache architecture

This design serves as the implementation reference for size-based file rotation.
