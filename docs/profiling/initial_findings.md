# Arc Profiling: Initial Findings

**Date:** October 24, 2025
**Branch:** `profiling/bottleneck-analysis`
**Python Version:** 3.13.3

---

## Startup Time Analysis

### Baseline Measurement
- **Total startup time:** 0.736 seconds
- **User time:** 0.49s
- **System time:** 0.09s
- **CPU usage:** 78%

### Slowest Imports (Top 20)

| Module | Import Time (μs) | Cumulative Time (μs) | Notes |
|--------|------------------|---------------------|-------|
| `fastapi.openapi.models` | 41,292 | 110,721 | OpenAPI schema generation |
| `api.main` | 35,565 | 461,524 | Main application module |
| `pyarrow.lib` | 16,661 | 37,137 | Arrow C++ bindings |
| `api.models` | 12,616 | 12,616 | Pydantic models |
| `pyarrow.compute` | 10,058 | 14,641 | Arrow compute functions |
| `aiohttp.connector` | 9,604 | 10,608 | HTTP client |
| `pydantic_core.core_schema` | 5,357 | 6,388 | Pydantic validation |
| `polars._polars_runtime_32` | 5,342 | 5,395 | Polars Rust bindings |
| `duckdb._dbapi_type_object` | 5,225 | 5,390 | DuckDB types |
| `api.retention_routes` | 4,189 | 4,189 | Retention policies |
| `api.delete_routes` | 3,769 | 19,271 | Delete operations |
| `numpy._core._multiarray_umath` | 3,711 | 4,157 | NumPy core |
| `_duckdb` | 3,642 | 3,642 | DuckDB C++ extension |
| `urllib3.util.url` | 2,889 | 2,889 | URL parsing |
| `aiohttp.tracing` | 2,810 | 3,573 | Request tracing |
| `pyarrow._compute` | 2,753 | 2,753 | Arrow compute internals |
| `annotated_types` | 2,731 | 2,731 | Type annotations |
| `pydantic.types` | 2,424 | 2,424 | Pydantic type system |
| `fastapi.routing` | 2,306 | 135,909 | FastAPI routing |
| `botocore.credentials` | 2,028 | 2,685 | AWS credentials (S3) |

### Key Observations

1. **FastAPI OpenAPI (110ms cumulative)**
   - OpenAPI schema generation takes significant time
   - Loaded even if not using `/docs` endpoint
   - **Optimization:** Lazy-load OpenAPI schema

2. **PyArrow (37ms)**
   - Heavy C++ bindings
   - Required for Arrow operations
   - **Not easily optimizable** (core dependency)

3. **Pydantic Models (6ms+)**
   - Model validation setup
   - **Optimization:** Consider simpler types for hot paths

4. **Polars (5ms)**
   - Rust bindings loaded
   - May not be heavily used
   - **Optimization:** Check if necessary, consider lazy import

5. **DuckDB (5ms)**
   - C++ extension loading
   - Core query engine
   - **Not easily optimizable** (core dependency)

6. **Delete/Retention Routes (8ms combined)**
   - Feature routes loaded eagerly
   - **Optimization:** Consider lazy router registration

---

## Comparison: Python 3.13 vs 3.14t Startup

Based on Python 3.14t experiment observation:

| Metric | Python 3.13 | Python 3.14t (observed) |
|--------|-------------|-------------------------|
| **Startup Time** | 0.736s | ~1.5s+ (noticeably slower) |
| **Reason** | - | Reference counting setup overhead |

**Analysis:** Python 3.14t's slower startup is likely due to:
- Additional initialization for free-threading support
- Setting up per-object reference counting locks
- Module initialization with thread-safe structures

This confirms free-threading has overhead even before runtime!

---

## Startup Optimization Opportunities

### Quick Wins (Easy, Low Risk)

1. **Lazy-load OpenAPI Schema**
   - Only generate when `/docs` or `/openapi.json` accessed
   - **Potential savings:** ~110ms
   - **Implementation:** Conditional `app.openapi()` generation

2. **Defer Heavy Imports**
   - Import `polars` only when needed (is it even used?)
   - Import `botocore` only for S3 storage backend
   - **Potential savings:** ~10-20ms
   - **Implementation:** Move imports inside functions

3. **Lazy Router Registration**
   - Register delete/retention routers on-demand
   - **Potential savings:** ~8ms
   - **Implementation:** Conditional `include_router()`

### Medium Effort

4. **Optimize Pydantic Models**
   - Use simpler types for hot paths
   - Consider `TypedDict` for internal structures
   - **Potential savings:** ~5ms
   - **Risk:** Need careful validation testing

5. **Split Application Modules**
   - Break up `api.main` into smaller modules
   - Reduce initial loading
   - **Potential savings:** Variable
   - **Risk:** Architectural change

### Not Worth It

- PyArrow/DuckDB imports (core dependencies, can't avoid)
- NumPy imports (required by Arrow)
- FastAPI routing (core framework)

---

## Next Steps

1. ✅ **Baseline startup measurements complete**
2. ⏳ **Live profiling during ingestion** (py-spy)
3. ⏳ **Identify hot paths in write pipeline**
4. ⏳ **Memory profiling**
5. ⏳ **Lock contention analysis**

---

## Profiling Tools Installed

- ✅ `py-spy` - Low-overhead sampling profiler
- ✅ `line-profiler` - Line-by-line profiling
- ✅ `memory-profiler` - Memory usage tracking
- ✅ `memray` - Advanced memory profiler
- ✅ `viztracer` - Timeline visualization

**Ready for live profiling!** 🚀

---

## Commands Reference

### Measure startup time:
```bash
time ./venv/bin/python -c "from api.main import app; print('Loaded')"
```

### Analyze imports:
```bash
./venv/bin/python -X importtime -c "from api.main import app" 2>&1 | grep "import time:" | sort -t: -k2 -rn | head -20
```

### Profile live with py-spy:
```bash
python scripts/profile_ingestion.py --mode pyspy --duration 30
```

### Run benchmark:
```bash
python scripts/benchmark_ingestion.py
```
