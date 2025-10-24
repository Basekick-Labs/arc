# Ingestion Bottleneck Analysis - Findings

**Date:** 2025-01-24
**Branch:** `profiling/bottleneck-analysis`
**Baseline Performance:** 2.42M RPS (Python 3.13)

---

## 🔥 Profiling Results

### CPU Time Distribution

```
100% Total Request Processing
├─ 83.7% write_msgpack() (api/msgpack_routes.py:182)
│  ├─ 78.4% arrow_writer.write() (ingest/arrow_writer.py:476)
│  │  └─ Most time in _flush_records() when buffer is full
│  └─ ~5% MessagePack decoding + overhead
└─ 16.3% FastAPI/Starlette middleware overhead
```

**Key Finding:** **93.6% of ingestion time** (78.4 / 83.7) is spent in `arrow_writer.write()`.

---

## 🎯 Identified Bottlenecks

### 1. **Lock Contention in write() - Lines 439-476**

**Problem:**
```python
async with self._lock:  # Single global lock!
    # Group records by measurement
    by_measurement = defaultdict(list)
    for record in records:
        measurement = record.get('measurement', 'unknown')
        by_measurement[measurement].append(record)

    # Add to buffers
    for measurement, measurement_records in by_measurement.items():
        self.buffers[measurement].extend(measurement_records)

        # Check buffer size (complex calculation)
        buffer_size = sum(
            len(r['columns']['time']) if r.get('_columnar') else 1
            for r in self.buffers[measurement]
        )
```

**Issue:** The lock is held during:
- Record grouping by measurement (dictionary operations)
- Buffer size calculations (iterating over buffers)
- Multiple list extend operations

**Impact:** Multiple worker processes wait on this lock during high throughput.

**Severity:** 🔴 HIGH - This is likely the primary bottleneck

---

### 2. **Expensive Buffer Size Calculation - Lines 462-465**

**Problem:**
```python
buffer_size = sum(
    len(r['columns']['time']) if r.get('_columnar') else 1
    for r in self.buffers[measurement]
)
```

**Issue:**
- This runs on EVERY write call for EVERY measurement
- Iterates entire buffer to count records
- For columnar data, accesses nested dict `r['columns']['time']`
- Conditional check `r.get('_columnar')` for every record

**Impact:** O(n) operation where n = current buffer size. As buffer grows, this gets slower.

**Example:**
- Buffer has 5,000 records
- Each write checks all 5,000 records
- With 2.42M RPS, this happens ~2.4M times/second

**Severity:** 🔴 HIGH - Unnecessary computation inside hot path

---

### 3. **Dictionary Operations Under Lock - Lines 441-444**

**Problem:**
```python
async with self._lock:
    # Group records by measurement
    by_measurement = defaultdict(list)
    for record in records:
        measurement = record.get('measurement', 'unknown')
        by_measurement[measurement].append(record)
```

**Issue:**
- Python dict operations (get, append) while holding lock
- Could be done BEFORE acquiring lock
- Lock protects buffers, not input records

**Impact:** Increases lock hold time by ~10-20%

**Severity:** 🟡 MEDIUM - Easy optimization

---

### 4. **Duplicate Record Counting - Lines 454-458**

**Problem:**
```python
num_records = sum(
    len(r['columns']['time']) if r.get('_columnar') else 1
    for r in measurement_records
)
self.total_records_buffered += num_records
```

**Issue:**
- Just grouped records by measurement in previous loop
- Now iterating again to count them
- Same columnar check repeated

**Impact:** Redundant loop iteration

**Severity:** 🟡 MEDIUM - Wasteful but small impact

---

## 📊 Optimization Opportunities (Ranked)

### Priority 1: Maintain Buffer Size Counter (Eliminates Recalculation)

**Current:** O(n) buffer size check on every write
**Proposed:** O(1) counter update

**Implementation:**
```python
# Add to __init__:
self.buffer_sizes: Dict[str, int] = defaultdict(int)  # Track size per measurement

# In write():
self.buffer_sizes[measurement] += num_records

# Check becomes O(1):
if self.buffer_sizes[measurement] >= self.max_buffer_size:
    # Flush
    self.buffer_sizes[measurement] = 0
```

**Expected Gain:** 10-20% reduction in write() time
**Effort:** Low (15 minutes)
**Risk:** Low (simple counter)

---

### Priority 2: Move Record Grouping Outside Lock

**Current:** Group records while holding lock
**Proposed:** Group first, then acquire lock

**Implementation:**
```python
# BEFORE acquiring lock:
by_measurement = defaultdict(list)
for record in records:
    measurement = record.get('measurement', 'unknown')
    by_measurement[measurement].append(record)

# NOW acquire lock:
async with self._lock:
    # Just update buffers
    for measurement, measurement_records in by_measurement.items():
        self.buffers[measurement].extend(measurement_records)
        # ...
```

**Expected Gain:** 5-10% reduction in lock contention
**Effort:** Low (10 minutes)
**Risk:** Low (records don't need protection)

---

### Priority 3: Per-Measurement Locks (Advanced)

**Current:** Single global lock for all measurements
**Proposed:** One lock per measurement

**Rationale:**
- If writing to `cpu_metrics` and `memory_metrics` simultaneously
- Currently they block each other
- With per-measurement locks, they can write in parallel

**Implementation:**
```python
self._locks: Dict[str, asyncio.Lock] = {}
self._master_lock = asyncio.Lock()  # Only for creating new locks

async def _get_lock(self, measurement: str) -> asyncio.Lock:
    if measurement in self._locks:
        return self._locks[measurement]

    async with self._master_lock:
        if measurement not in self._locks:
            self._locks[measurement] = asyncio.Lock()
        return self._locks[measurement]
```

**Expected Gain:** 20-40% if workload has multiple measurements
**Effort:** Medium (30 minutes)
**Risk:** Medium (more complex locking)

**Note:** We tried this in Python 3.14t and it didn't help - BUT that was because reference counting overhead dominated. In Python 3.13, this might actually work!

---

### Priority 4: Batch Buffer Extends

**Current:** Extend buffer for each measurement
**Proposed:** Single extend per measurement

**Issue:** Not actually a problem - already doing single extend per measurement

**Status:** ✅ Already optimized

---

### Priority 5: Pre-allocate Dict Capacity

**Current:** defaultdict() grows dynamically
**Proposed:** Pre-size dict for common case

**Implementation:**
```python
# If typical request has 5-10 measurements:
by_measurement = dict()
by_measurement.setdefault(measurement, []).append(record)
```

**Expected Gain:** <5%
**Effort:** Low
**Risk:** Low
**Priority:** 🟢 LOW - Minor optimization

---

## 🧪 Recommended Testing Order

### Test 1: Buffer Size Counter (Highest ROI)
1. Add `buffer_sizes` counter to track size per measurement
2. Remove expensive `sum()` calculation
3. Benchmark: Should see 10-20% improvement

### Test 2: Move Grouping Outside Lock
1. Move record grouping before lock acquisition
2. Benchmark: Should see 5-10% improvement

### Test 3: Combine Test 1 + Test 2
1. Apply both optimizations together
2. Benchmark: Should see 15-30% combined improvement
3. **Target: 2.8M+ RPS** (from 2.42M baseline)

### Test 4: Per-Measurement Locks (If Still Bottlenecked)
1. Only if lock contention still visible in profiling
2. Implement per-measurement locks
3. Benchmark: Could see 20-40% improvement if multi-measurement workload

---

## 📈 Expected Performance Targets

| Optimization | Baseline RPS | Expected RPS | Gain |
|-------------|--------------|--------------|------|
| Baseline | 2.42M | - | - |
| + Buffer Counter | 2.42M | **2.66M - 2.90M** | +10-20% |
| + Grouping Outside Lock | 2.66M | **2.79M - 2.93M** | +5-10% |
| + Per-Measurement Locks | 2.79M | **3.35M - 3.91M** | +20-40%* |

\* Only if workload uses multiple measurements

**Realistic Target:** **2.8M - 3.0M RPS** (+16-24% improvement)

---

## 🔍 Other Observations

### FastAPI Overhead: 16.3%
- Middleware, routing, exception handling
- This is normal and acceptable
- Not worth optimizing (would require dropping FastAPI)

### MessagePack Decoding: ~5%
- Already using fast msgpack library
- Could try `ormsgpack` (faster Rust-based decoder)
- Low priority - small gains

### Parquet Writing
- Not showing up as bottleneck in profile!
- Flushing happens outside lock (good!)
- pyarrow already optimized (C++ with GIL release)

---

## ✅ Next Steps

1. **Implement Priority 1 optimization** (buffer size counter)
2. **Benchmark** - validate improvement
3. **Implement Priority 2 optimization** (grouping outside lock)
4. **Benchmark** - validate combined improvement
5. **If lock contention persists:** Implement Priority 3 (per-measurement locks)
6. **Document final results**

**Ready to optimize?** Let's start with Priority 1! 🚀
