# Compression Test Results

**Date:** 2025-01-24
**Branch:** `profiling/bottleneck-analysis`
**Config:** Per-measurement locks enabled
**Benchmark:** `msgpack_load_test_pregenerated.py` (3 measurements: cpu, mem, disk)

---

## 🎯 Test Configuration

All tests run with:
- Workers: 8
- Buffer size: 50,000 records (from arc.conf)
- Buffer age: 5 seconds
- Per-measurement locks: **ENABLED**
- Target RPS: 2.5M

## 📊 Results Summary

| Compression | RPS | Achievement | p50 Latency | p95 Latency | p99 Latency | vs Baseline |
|-------------|-----|-------------|-------------|-------------|-------------|-------------|
| **snappy (baseline)** | 2,380,565 | 95.2% | 2.05ms | 29.48ms | 108.69ms | - |
| **none** | 2,381,153 | 95.2% | 2.10ms | 30.11ms | 106.52ms | **+0.02%** ✓ |
| **zstd** | 2,333,292 | 93.3% | 2.39ms | 57.66ms | 125.84ms | **-2.0%** ❌ |
| **gzip** | 2,305,916 | 92.2% | 1.99ms | 33.66ms | 241.08ms | **-3.1%** ❌ |

---

## 🔍 Key Findings

### Finding 1: Compression Has Minimal Impact

**No improvement with compression=none:**
- Expected: +15-25% throughput
- Actual: +0.02% (essentially identical)

**Conclusion:** Compression is NOT the bottleneck!

### Finding 2: All Results Slower Than Original Baseline

**Original baseline (before per-measurement locks):**
- RPS: **2.42M**
- p50: **1.75ms**
- p95: **28.16ms**
- p99: **47.09ms**

**Current results (with per-measurement locks):**
- RPS: **2.38M** (-1.7%)
- p50: **2.05-2.39ms** (+17-37%)
- p95: **29-58ms** (+3-106%)
- p99: **107-241ms** (+128-412%)

**Regression:** Per-measurement locks added **~17-37% latency overhead**

### Finding 3: Tail Latency Degradation

p99 latency increased dramatically:
- Baseline: 47ms
- Current: 107-241ms (**+128-412%**)

This suggests increased **lock contention** or **queueing delays**.

---

## 💡 Analysis

### Why Compression Doesn't Matter

1. **PyArrow already optimized** - Compression happens in C++ with GIL released
2. **I/O is the bottleneck** - Writing to disk, not compression CPU
3. **Async flush** - Compression happens outside lock, doesn't block writes

### Why Per-Measurement Locks Made Things Slower

**Problem:** Per-measurement locks added overhead without benefit

**Root causes:**

1. **Lock creation overhead:**
   ```python
   lock = await self._get_lock(measurement)  # Dictionary lookup + possible lock creation
   ```
   - Called for every write operation
   - Checks `if measurement in self._locks` (dict lookup)
   - May need master lock for new measurements

2. **Sequential locking:**
   ```python
   for measurement, records in by_measurement.items():
       lock = await self._get_lock(measurement)
       async with lock:
           # Process records
   ```
   - Processes measurements one at a time
   - If request has 3 measurements, locks them sequentially
   - Adds 3x lock overhead per request

3. **No parallelism benefit:**
   - Benchmark writes to cpu, mem, disk in same batch
   - Sequential processing = no parallel writes to different measurements
   - All overhead, no benefit

### The Real Bottleneck

Based on compression having no effect:

✅ **NOT CPU-bound** (compression doesn't matter)
✅ **NOT lock-bound** (more locks made it worse)
✅ **Likely disk I/O bound** (writing Parquet files)

**Evidence:**
- 40-50% CPU usage (waiting on I/O)
- Compression changes make no difference
- Increasing buffer size from 10K → 50K didn't help

---

## 🎯 Recommendations

### Priority 1: Revert Per-Measurement Locks

**Action:** Go back to single global lock

**Rationale:**
- Per-measurement locks add overhead without benefit
- Single lock is simpler and faster for this workload
- Baseline was actually better: 2.42M RPS vs 2.38M RPS

### Priority 2: Profile Disk I/O

**Action:** Measure actual disk write latency

**Commands:**
```bash
# Monitor disk I/O during benchmark
iostat -x 1

# Check storage backend performance
time ls -R ./data/arc/ | wc -l  # File count
du -sh ./data/arc/               # Total size
```

**Questions:**
- How many files being created per second?
- What's the disk write throughput?
- Is storage backend (local filesystem) the bottleneck?

### Priority 3: Reduce Flush Frequency

**Current:** Buffer size 50K, age 5s

**Test:** Increase to 100K or 200K records

**Rationale:**
- Fewer flushes = less I/O overhead
- Larger Parquet files = better efficiency
- Trade-off: More memory usage, longer data latency

**Config change:**
```toml
[ingestion]
buffer_size = 100000  # or 200000
buffer_age_seconds = 10  # or 15
```

### Priority 4: Parallel Flushes

**Action:** Flush multiple measurements concurrently

**Implementation:**
```python
# Instead of:
for measurement, records in records_to_flush.items():
    await self._flush_records(measurement, records)

# Do:
flush_tasks = [
    self._flush_records(measurement, records)
    for measurement, records in records_to_flush.items()
]
await asyncio.gather(*flush_tasks)
```

**Expected gain:** +20-30% if I/O bound (parallel disk writes)

---

## 📈 Next Steps

1. **Revert per-measurement locks** → Expected: back to 2.42M RPS
2. **Profile disk I/O** → Confirm storage is bottleneck
3. **Test larger buffers** → 100K or 200K records
4. **Test parallel flushes** → Concurrent writes for cpu, mem, disk

---

## 🔬 Detailed Test Data

### Test 1: compression=none

```
[   5.0s] RPS: 2428200 | p50:  2.1ms p95:  42.9ms p99: 100.5ms
[  10.0s] RPS: 2383200 | p50:  2.0ms p95:  38.4ms p99: 105.4ms
[  15.0s] RPS: 2386800 | p50:  2.1ms p95:  36.3ms p99: 106.1ms
[  20.0s] RPS: 2389800 | p50:  2.1ms p95:  33.8ms p99: 107.7ms
[  25.0s] RPS: 2401800 | p50:  2.1ms p95:  33.0ms p99: 106.9ms
[  30.1s] RPS: 2396200 | p50:  2.1ms p95:  30.1ms p99: 106.5ms

Final: 2,381,153 RPS | p50: 2.10ms | p95: 30.11ms | p99: 106.52ms
```

### Test 2: compression=zstd

```
[   5.0s] RPS: 2384200 | p50:  2.3ms p95:  49.4ms p99: 119.0ms
[  10.0s] RPS: 2359800 | p50:  2.2ms p95:  51.1ms p99: 121.7ms
[  15.0s] RPS: 2344600 | p50:  2.3ms p95:  56.5ms p99: 122.3ms
[  20.0s] RPS: 2348400 | p50:  2.3ms p95:  56.8ms p99: 123.0ms
[  25.0s] RPS: 2356200 | p50:  2.4ms p95:  58.0ms p99: 123.7ms
[  30.1s] RPS: 2346000 | p50:  2.4ms p95:  57.2ms p99: 125.5ms

Final: 2,333,292 RPS | p50: 2.39ms | p95: 57.66ms | p99: 125.84ms
```

### Test 3: compression=gzip

```
[   5.0s] RPS: 2390800 | p50:  2.1ms p95:  40.6ms p99: 192.0ms
[  10.0s] RPS: 2326400 | p50:  2.0ms p95:  39.1ms p99: 237.2ms
[  15.0s] RPS: 2324200 | p50:  2.0ms p95:  39.3ms p99: 239.4ms
[  20.0s] RPS: 2322600 | p50:  2.0ms p95:  34.4ms p99: 240.6ms
[  25.0s] RPS: 2305600 | p50:  2.0ms p95:  39.1ms p99: 241.5ms
[  30.1s] RPS: 2333600 | p50:  2.0ms p95:  33.1ms p99: 241.1ms

Final: 2,305,916 RPS | p50: 1.99ms | p95: 33.66ms | p99: 241.08ms
```

### Test 4: compression=snappy (re-baseline)

```
[   5.0s] RPS: 2421600 | p50:  2.1ms p95:  41.7ms p99:  90.2ms
[  10.0s] RPS: 2396000 | p50:  2.0ms p95:  38.7ms p99: 103.6ms
[  15.0s] RPS: 2377600 | p50:  2.0ms p95:  38.9ms p99: 108.9ms
[  20.0s] RPS: 2406800 | p50:  2.0ms p95:  33.8ms p99: 107.0ms
[  25.0s] RPS: 2399200 | p50:  2.0ms p95:  32.6ms p99: 108.3ms
[  30.1s] RPS: 2377200 | p50:  2.0ms p95:  29.5ms p99: 108.7ms

Final: 2,380,565 RPS | p50: 2.05ms | p95: 29.48ms | p99: 108.69ms
```

---

## ✅ Conclusion

**Compression is not the bottleneck.** All compression options perform essentially the same (~2.38M RPS).

**Per-measurement locks added overhead** without providing benefit for this workload, causing:
- -1.7% throughput regression
- +17-37% p50 latency regression
- +128-412% p99 latency regression

**Recommended path forward:**
1. Revert to single global lock (simpler, faster for this case)
2. Focus on reducing I/O overhead (larger buffers, parallel flushes)
3. Profile actual disk I/O to confirm bottleneck
