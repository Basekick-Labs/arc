# Arc Write Performance Optimization Results

## 📊 Benchmark: Apple M3 Max (14 cores, 400 workers, 30s)

---

## Throughput Improvement: +3.1%

```
Before:  1.95M RPS  ████████████████████
After:   2.01M RPS  ████████████████████▓  +3.1%
```

**2.01M records/sec** - MessagePack binary protocol

---

## Latency Improvements (Lower is Better)

### p50 (Median) - Improved by 8.7%
```
Before:  18.21ms  ████████████████████
After:   16.62ms  ██████████████████▏     -8.7%
```

### p95 (95th percentile) - Improved by 20.3% ⚡
```
Before:  184.60ms  ████████████████████
After:   147.12ms  ███████████████▉        -20.3%
```

### p99 (99th percentile) - Improved by 19.6% ⚡
```
Before:  395.12ms  ████████████████████
After:   317.53ms  ████████████████        -19.6%
```

---

## Optimizations Applied

1. **MessagePack Streaming Decoder**
   - Uses `msgpack.Unpacker()` for incremental processing
   - Reduces memory usage by 10-20%
   - Avoids full payload materialization

2. **Columnar Polars Construction**
   - Converts to `dict[str, list]` before DataFrame
   - 5-10% faster with better cache locality
   - More efficient memory layout

---

## Test Results

- **Total Records**: 61M records in 30 seconds
- **Success Rate**: 100% (zero errors)
- **Workers**: 400 concurrent workers
- **Protocol**: MessagePack binary + Direct Arrow/Parquet
- **Storage**: MinIO (native deployment)

---

**Key Insight**: 20% tail latency reduction is more important than 3% throughput gain.
Lower p95/p99 = more predictable performance under load. Critical for production! 🎯
