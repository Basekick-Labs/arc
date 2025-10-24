# Arc Bottleneck Analysis & Profiling Plan

**Date:** October 24, 2025
**Branch:** `profiling/bottleneck-analysis`
**Goal:** Identify performance bottlenecks in Python 3.13 ingestion pipeline and find optimization opportunities

---

## Current Performance Baseline

**Python 3.13 + GIL:**
- **Throughput:** 2.42M RPS (MessagePack columnar)
- **Latency:** p50: 1.74ms, p95: 28.13ms, p99: 45.27ms
- **CPU:** ~100% utilization (14 cores, M3 Max)
- **Workers:** 42 (3x cores)

**Note from Python 3.14t experiment:**
- Startup time was noticeably slower on 3.14t
- This suggests initialization overhead worth investigating

---

## Profiling Strategy

### 1. Startup Time Profiling
**Observation:** Python 3.14t had slower startup
**Goal:** Identify slow imports and initialization

**Tools:**
- `python -X importtime` - Measure import times
- `time` command - Measure total startup
- Line-by-line profiling of `main.py` startup

**What to look for:**
- Heavy imports (pandas, pyarrow, duckdb, etc.)
- Slow initialization (buffers, connections, caches)
- Unnecessary eager loading

### 2. Ingestion Pipeline Profiling
**Goal:** Find hot paths in the request → disk pipeline

**Tools:**
- `cProfile` - Python-level profiling
- `py-spy` - Production profiling (no instrumentation)
- `line_profiler` - Line-by-line profiling
- `memory_profiler` - Memory usage tracking

**What to profile:**
- `msgpack_routes.py:write_msgpack()` - Request handling
- `msgpack_decoder.decode()` - MessagePack decoding
- `arrow_writer.py:write()` - Buffer operations
- `arrow_writer.py:_flush_records()` - Parquet writing
- Storage backend operations

### 3. Lock Contention Analysis
**Goal:** Measure actual lock wait times

**Approach:**
- Add instrumentation around `async with self._lock`
- Track lock acquisition times
- Identify if locks are actually contending

### 4. Memory Profiling
**Goal:** Find memory allocation hotspots

**Tools:**
- `tracemalloc` - Built-in memory tracking
- `memray` - Advanced memory profiler
- `pympler` - Object tracking

**What to look for:**
- Unnecessary copies (especially large arrays)
- Memory leaks
- High allocation rates

### 5. I/O Profiling
**Goal:** Measure disk/storage bottlenecks

**Tools:**
- `iostat` - Disk I/O statistics
- `iotop` - Per-process I/O monitoring
- Async I/O profiling

**What to look for:**
- Disk write latency
- Storage backend performance
- Async I/O blocking

---

## Profiling Tools Setup

### Install Profiling Dependencies
```bash
pip install py-spy line-profiler memory-profiler memray viztracer
```

### Tool Descriptions

**1. py-spy** (Recommended - Low overhead)
- Sampling profiler
- Works on running processes
- No code changes needed
- Generates flamegraphs

**2. cProfile** (Built-in)
- Deterministic profiling
- Higher overhead
- Good for detailed analysis

**3. line_profiler**
- Line-by-line profiling
- Requires `@profile` decorator
- Shows time per line

**4. memray** (Memory)
- Advanced memory profiler
- Tracks allocations
- Native extension support

**5. viztracer** (Visual)
- Timeline visualization
- Shows async behavior
- Chrome trace format

---

## Profiling Experiments

### Experiment 1: Startup Time Analysis

**Measure import times:**
```bash
python -X importtime -c "from api.main import app" 2>&1 | tee startup_imports.txt
```

**Measure total startup:**
```bash
time ./venv/bin/python -c "from api.main import app; print('Loaded')"
```

**Analyze:**
- Which modules take longest to import?
- Can we lazy-load anything?
- Are there unnecessary imports?

---

### Experiment 2: Live Profiling During Benchmark

**Using py-spy (recommended):**
```bash
# Start Arc
./start.sh native &
ARC_PID=$!

# Profile while running benchmark
py-spy record -o profile.svg -d 30 -p $ARC_PID &

# Run benchmark
python scripts/benchmark_ingestion.py

# View flamegraph
open profile.svg
```

**Using cProfile:**
```bash
# Add to main.py:
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()

# ... application runs ...

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(50)
```

---

### Experiment 3: Line-by-Line Profiling

**Profile hot functions:**
```python
# Add to arrow_writer.py
from line_profiler import profile

@profile
async def write(self, records):
    # ... existing code ...

@profile
async def _flush_records(self, measurement, records):
    # ... existing code ...
```

**Run:**
```bash
kernprof -l -v api/main.py
```

---

### Experiment 4: Memory Profiling

**Using memray:**
```bash
memray run -o profile.bin ./start.sh native
memray flamegraph profile.bin
```

**Using tracemalloc (built-in):**
```python
import tracemalloc

tracemalloc.start()
# ... run some operations ...
snapshot = tracemalloc.take_snapshot()
top_stats = snapshot.statistics('lineno')

for stat in top_stats[:10]:
    print(stat)
```

---

### Experiment 5: Lock Contention Measurement

**Instrumented lock timing:**
```python
import time
from collections import defaultdict

class InstrumentedLock:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.wait_times = []
        self.hold_times = []

    async def __aenter__(self):
        start = time.perf_counter()
        await self._lock.acquire()
        wait_time = time.perf_counter() - start
        self.wait_times.append(wait_time)
        self._hold_start = time.perf_counter()

    async def __aexit__(self, *args):
        hold_time = time.perf_counter() - self._hold_start
        self.hold_times.append(hold_time)
        self._lock.release()

# Use InstrumentedLock in ArrowParquetBuffer
# Log statistics periodically
```

---

## Expected Bottlenecks to Investigate

### 1. MessagePack Decoding
**Hypothesis:** Decoding binary to Python dicts is expensive

**Test:**
- Profile `msgpack_decoder.decode()`
- Compare pure MessagePack vs columnar format
- Check if we can optimize deserialization

### 2. Dictionary Operations
**Hypothesis:** Creating/manipulating dicts is slow in Python

**Test:**
- Profile record grouping by measurement
- Profile dict access patterns
- Consider using structs or named tuples

### 3. Async Overhead
**Hypothesis:** asyncio/uvloop overhead

**Test:**
- Profile event loop operations
- Measure context switch overhead
- Check if we're creating too many tasks

### 4. Parquet Writing
**Hypothesis:** PyArrow/Parquet serialization is slow

**Test:**
- Profile `pq.write_table()`
- Test different compression levels
- Measure columnar conversion time

### 5. Buffer Operations
**Hypothesis:** Buffer management overhead

**Test:**
- Profile `buffers[measurement].extend()`
- Measure list growth/reallocation
- Check defaultdict performance

### 6. Storage Backend
**Hypothesis:** I/O is the real bottleneck

**Test:**
- Profile `storage_backend.upload_file()`
- Measure disk write latency
- Check if async I/O is blocking

---

## Optimization Ideas (Based on Profiling)

### Potential Optimizations to Test

1. **Lazy Imports**
   - Defer heavy imports until needed
   - Reduce startup time

2. **Buffer Pre-allocation**
   - Pre-allocate list capacity
   - Reduce reallocation overhead

3. **Struct Instead of Dict**
   - Use dataclasses or named tuples
   - Faster field access

4. **Batch Processing**
   - Process records in larger batches
   - Reduce per-record overhead

5. **Lock-Free Structures**
   - Use asyncio.Queue
   - Reduce lock contention

6. **Compression Tuning**
   - Test snappy vs no compression vs zstd
   - Find optimal tradeoff

7. **Column Caching**
   - Cache Arrow column builders
   - Reduce allocation overhead

8. **Parallel Flushes**
   - Flush multiple measurements in parallel
   - Use asyncio.gather()

---

## Metrics to Track

### Before/After Comparisons

| Metric | Baseline | After Optimization | Improvement |
|--------|----------|-------------------|-------------|
| **Throughput** | 2.42M RPS | TBD | TBD |
| **p50 Latency** | 1.74ms | TBD | TBD |
| **p95 Latency** | 28.13ms | TBD | TBD |
| **p99 Latency** | 45.27ms | TBD | TBD |
| **CPU Usage** | 100% | TBD | TBD |
| **Memory Usage** | TBD | TBD | TBD |
| **Startup Time** | TBD | TBD | TBD |

---

## Success Criteria

**Goal:** Achieve meaningful performance improvement

**Targets:**
- 🎯 **Throughput:** 2.5M+ RPS (3%+ improvement)
- 🎯 **Latency p50:** <1.5ms (14%+ improvement)
- 🎯 **Latency p95:** <25ms (11%+ improvement)
- 🎯 **Startup Time:** <5 seconds (measurable improvement)

---

## Next Steps

1. ✅ Create profiling branch
2. ⏳ Install profiling tools
3. ⏳ Run startup time analysis
4. ⏳ Profile ingestion pipeline
5. ⏳ Identify top 3 bottlenecks
6. ⏳ Implement optimizations
7. ⏳ Benchmark and compare
8. ⏳ Document findings

---

**Status:** Ready to begin profiling 🚀
