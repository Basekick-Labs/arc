# Arc Roadmap

This document outlines planned features and enhancements for Arc. Timelines are estimates and subject to change based on community feedback and priorities.

---

## 2026.01.1 (Q1 2026)

### Native Azure Blob Storage Support
**Status**: Planned
**Priority**: Medium

Add native Azure Blob Storage backend support for Arc, eliminating the need for s3proxy middleware.

**Features**:
- Direct Azure Blob Storage API integration
- Azure authentication (storage account keys, SAS tokens, managed identity)
- Compatible with existing storage backend interface
- Configuration via `arc.conf`

**Workaround Available**: Use s3proxy as middleware to present Azure Blob Storage as S3-compatible

---

### Partition Discovery API
**Status**: Planned
**Priority**: Medium

Expose Arc's intelligent partition pruning capabilities via REST API endpoints to enable external tools (Polars, Arrow, Spark) to benefit from Arc's time-based partition optimization.

**Endpoints**:

#### GET `/api/v1/partitions/{database}/{measurement}`
Returns optimized partition paths for a given time range.

**Query Parameters**:
- `start_time`: ISO 8601 timestamp (e.g., `2025-11-19T15:00:00`)
- `end_time`: ISO 8601 timestamp

**Response**:
```json
{
  "paths": [
    "/data/arc/default/cpu/2025/11/19/15/*.parquet",
    "/data/arc/default/cpu/2025/11/19/*.parquet"
  ],
  "optimization": {
    "total_possible_partitions": 8760,
    "pruned_to": 2,
    "speedup_estimate": "4380x"
  }
}
```

#### GET `/api/v1/metadata/{database}/{measurement}`
Returns partition structure metadata and schema information.

**Response**:
```json
{
  "database": "default",
  "measurement": "cpu",
  "partition_scheme": "year/month/day/hour",
  "available_partitions": [
    {
      "path": "2025/11/19/15",
      "start": "2025-11-19T15:00:00Z",
      "end": "2025-11-19T15:59:59Z",
      "records": 50000,
      "size_bytes": 10485760
    }
  ],
  "oldest_data": "2024-01-01T00:00:00Z",
  "newest_data": "2025-11-19T16:00:00Z",
  "total_records": 125000000,
  "schema": {
    "time": "timestamp[us]",
    "host": "string",
    "usage_idle": "float64",
    "usage_user": "float64"
  }
}
```

**Use Case**: Polars users can query Arc for optimal file paths:
```python
import polars as pl
import requests

# Get optimized paths from Arc
response = requests.get(
    "http://arc:8000/api/v1/partitions/default/cpu",
    params={"start_time": "2025-11-19T15:00:00", "end_time": "2025-11-19T17:00:00"}
)
paths = response.json()["paths"]

# Polars reads only relevant files
df = pl.scan_parquet(paths).filter(pl.col("temperature") > 25).collect()
```

**Benefits**:
- Arc's partition pruning intelligence available to external tools
- Reduces files scanned by 10-1000x for time-range queries
- Works with any Arrow-compatible tool (Polars, DuckDB, Spark)

---

## Future Considerations

### Vortex Format Support
**Status**: Under evaluation
**Priority**: Low

DuckDB recently added support for the Vortex columnar format, which claims 3-10x faster query performance compared to Parquet. Arc could support Vortex as an optional storage format.

**Approach**:
- Phase 1: Vortex output for compacted files (keep Parquet for ingestion)
- Phase 2: Full Vortex support when Python ecosystem matures

**Considerations**:
- Ecosystem maturity (Polars, Spark support)
- Format stability (breaking changes?)
- Migration path from Parquet

---

### Hive-Style Partitioning
**Status**: Under evaluation
**Priority**: Low

Add option for explicit Hive-style partition columns for better compatibility with external tools.

**Current**: `cpu/2025/11/19/15/*.parquet`
**Proposed**: `cpu/year=2025/month=11/day=19/hour=15/*.parquet`

**Benefits**:
- Automatic partition column extraction in Polars/Spark
- Better tooling compatibility

**Trade-offs**:
- Longer path names
- Migration complexity for existing deployments

---

## Community Contributions Welcome

We welcome community contributions for any of these features! If you're interested in implementing or testing any of these, please:

1. Open a GitHub issue to discuss the approach
2. Share your use case and requirements
3. Submit a PR with your implementation

For questions or to prioritize features for your use case, reach out via GitHub Issues or our community channels.
