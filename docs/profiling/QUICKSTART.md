# Arc Ingestion Profiling - Quick Start

**Goal:** Find bottlenecks in the Python 3.13 ingestion pipeline (2.42M RPS baseline)

---

## 🚀 Quick Profile (Recommended)

**This will show you where CPU time is spent during ingestion:**

### Step 1: Start Arc
```bash
./start.sh native
```

### Step 2: Start Profiler (in another terminal)
```bash
python scripts/profile_arc_ingestion.py 60
```

This will profile for 60 seconds.

### Step 3: Run Benchmark
While the profiler is running, run your benchmark:
```bash
python scripts/benchmark_ingestion.py
```

### Step 4: View Results
```bash
open profile_ingestion_*.svg
```

---

## 📊 Interpreting the Flamegraph

### What to Look For

**Wide bars = hot paths (where time is spent)**

Focus on these functions in the Arc codebase:

1. **`msgpack_decoder.decode()`**
   - MessagePack binary → Python dict conversion
   - If wide: decoding overhead is significant

2. **`arrow_writer.write()`**
   - Buffer management
   - Record grouping by measurement
   - Lock operations
   - If wide: buffer operations are slow

3. **`arrow_writer._flush_records()`**
   - Parquet table construction
   - Compression
   - File writing
   - If wide: Parquet serialization is the bottleneck

4. **`asyncio.Lock.acquire()` / `asyncio.Lock.__aenter__`**
   - Lock contention
   - If wide: threads are waiting for locks

5. **Dictionary operations**
   - `dict.__setitem__`, `dict.__getitem__`, `defaultdict`
   - If wide: Python dict overhead is significant

### External Libraries (Expected to be Wide)

These are **normal** and likely optimized already:
- `pyarrow.Table.from_pandas()` - Columnar conversion
- `pyarrow.parquet.write_table()` - Parquet writing
- `msgpack.unpackb()` - Binary unpacking
- `uvloop` internals - Event loop

**Why?** These are C extensions that already release the GIL. We can't optimize them directly.

---

## 🎯 Expected Bottlenecks to Investigate

Based on the 2.42M RPS baseline, likely bottlenecks are:

### 1. Lock Contention
**Symptom:** Wide bars in `asyncio.Lock` operations

**Hypothesis:** Multiple workers waiting for buffer lock

**Investigation:**
- Check lock wait times in flamegraph
- Look for `asyncio.Lock.acquire` in stack traces
- Wide = contention, narrow = no contention

**Potential Fix:**
- Per-measurement locks (already tried in 3.14t, but might work better in 3.13)
- Lock-free queue structures
- Batch lock acquisitions

### 2. MessagePack Decoding
**Symptom:** Wide bars in `msgpack_decoder.decode()` or `msgpack.unpackb()`

**Hypothesis:** Binary → Python dict conversion is slow

**Investigation:**
- Measure % of time in decode functions
- Compare columnar vs row-based decoding

**Potential Fix:**
- Optimize columnar format parsing
- Pre-allocate dict structures
- Use faster msgpack library (ormsgpack?)

### 3. Dictionary Operations
**Symptom:** Wide bars in `dict` operations, `defaultdict`, or record grouping

**Hypothesis:** Python dict overhead for measurement grouping

**Investigation:**
- Time spent in `by_measurement[measurement].append(record)`
- Dict key lookups

**Potential Fix:**
- Use dataclasses or named tuples instead of dicts
- Pre-allocate dict capacity
- Minimize dict copies

### 4. Buffer Operations
**Symptom:** Wide bars in `arrow_writer.write()` excluding lock operations

**Hypothesis:** List operations (`buffers[measurement].extend()`) are slow

**Investigation:**
- Time in buffer extend operations
- Memory allocation overhead

**Potential Fix:**
- Pre-allocate buffer capacity
- Use deque instead of list
- Batch extend operations

### 5. Parquet Writing
**Symptom:** Wide bars in `arrow_writer._flush_records()` or `pq.write_table()`

**Hypothesis:** Disk I/O or compression is the bottleneck

**Investigation:**
- Time in Parquet write operations
- Compression overhead (snappy)
- File I/O latency

**Potential Fix:**
- Test no compression vs snappy vs zstd
- Parallel flushes for multiple measurements
- Larger buffer sizes to reduce flush frequency
- Async file I/O

---

## 📈 Alternative Profiling Methods

### Method 2: Line-by-Line Timing (More Detailed)

This adds instrumentation to measure specific operations:

1. **Install line_profiler:**
```bash
./venv/bin/pip install line-profiler
```

2. **Add @profile decorator to hot functions:**

Edit `ingest/arrow_writer.py`:
```python
from line_profiler import profile

@profile
async def write(self, records: List[Dict[str, Any]]):
    # ... existing code ...

@profile
async def _flush_records(self, measurement: str, records: List[Dict[str, Any]]):
    # ... existing code ...
```

3. **Run with profiling:**
```bash
kernprof -l -v ./start.sh native
```

### Method 3: Memory Profiling

Find memory allocation hotspots:

```bash
./venv/bin/pip install memray
memray run -o profile.bin ./start.sh native
# Run benchmark in another terminal
# Ctrl+C to stop
memray flamegraph profile.bin
```

---

## ✅ Success Metrics

**Current Baseline (Python 3.13 + GIL):**
- Throughput: **2.42M RPS**
- Latency p50: **1.74ms**
- Latency p95: **28.13ms**
- CPU: **~100% utilization**

**Optimization Targets:**
- 🎯 Throughput: **2.5M+ RPS** (+3%)
- 🎯 Latency p50: **<1.5ms** (-14%)
- 🎯 Latency p95: **<25ms** (-11%)

---

## 🔄 Profiling Workflow

1. **Profile** → Identify bottleneck
2. **Hypothesize** → What's causing it?
3. **Measure** → Add specific timing if needed
4. **Optimize** → Make targeted changes
5. **Benchmark** → Validate improvement
6. **Repeat** → Find next bottleneck

---

## 📝 Next Steps After Profiling

Once you identify the bottleneck from the flamegraph:

1. **Document findings** in `docs/profiling/findings.md`
2. **Create optimization branch** (e.g., `optimize/reduce-lock-contention`)
3. **Implement targeted fix**
4. **Benchmark before/after**
5. **Commit if improved, revert if regressed**

---

**Ready to profile?** Run the steps above! 🚀
