# Arc

High-performance time-series database built on DuckDB. Go implementation.

[Documentation](https://docs.basekick.net/arc)

---

## The Problem

Industrial IoT generates massive data at scale:

- **Racing & Motorsport**: 100M+ sensor readings per race
- **Smart Cities**: Billions of infrastructure events daily
- **Mining & Manufacturing**: Equipment telemetry at unprecedented scale
- **Energy & Utilities**: Grid monitoring, smart meters, renewable output
- **Oil & Gas**: Pipeline sensors, drilling telemetry, refinery monitoring
- **Logistics & Fleet**: Vehicle tracking, route optimization, delivery metrics
- **Medical & Healthcare**: Patient monitoring, clinical sleep studies, device telemetry
- **Observability**: Metrics, logs, traces from distributed systems

Traditional time-series databases can't keep up. They're slow, expensive, and lock your data in proprietary formats.

**Arc solves this: 9.47M records/sec ingestion, sub-second queries on billions of rows, portable Parquet files you own.**

```sql
-- Analyze equipment anomalies across facilities
SELECT
  device_id,
  facility_name,
  AVG(temperature) OVER (
    PARTITION BY device_id
    ORDER BY timestamp
    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
  ) as temp_moving_avg,
  MAX(pressure) as peak_pressure,
  STDDEV(vibration) as vibration_variance
FROM iot_sensors
WHERE timestamp > NOW() - INTERVAL '24 hours'
  AND facility_id IN ('mining_site_42', 'plant_7')
GROUP BY device_id, facility_name, timestamp
HAVING MAX(pressure) > 850 OR STDDEV(vibration) > 2.5;
```

**Standard DuckDB SQL. Window functions, CTEs, joins. No proprietary query language.**

---

## Performance

Benchmarked on Apple MacBook Pro M3 Max (14 cores, 36GB RAM, 1TB NVMe).

### Ingestion

| Protocol | Throughput | p50 Latency | p99 Latency |
|----------|------------|-------------|-------------|
| MessagePack Columnar | **9.47M rec/s** | 8.40ms | 42.29ms |
| Line Protocol | 1.92M rec/s | 49.53ms | 108.53ms |

### Query

| Format | Throughput | Response Size (50K rows) |
|--------|------------|--------------------------|
| Arrow IPC | **2.88M rows/s** | 1.71 MB |
| JSON | 2.23M rows/s | 2.41 MB |

### vs Python Implementation

| Metric | Go | Python | Improvement |
|--------|-----|--------|-------------|
| Ingestion | 9.47M rec/s | 4.21M rec/s | **125% faster** |
| Memory | Stable | 372MB leak/500 queries | **No leaks** |
| Deployment | Single binary | Multi-worker processes | **Simpler** |

---

## Why Go

- **Stable memory**: Go's GC returns memory to OS. No leaks.
- **Single binary**: Deploy one executable. No dependencies.
- **Native concurrency**: Goroutines handle thousands of connections efficiently.
- **Production GC**: Sub-millisecond pause times at scale.

---

## Quick Start

```bash
# Build
make build

# Run
./arc

# Verify
curl http://localhost:8000/health
```

---

## Installation

### Pre-built Binaries (Coming Soon)

- Linux (amd64, arm64)
- macOS (amd64, arm64)
- Windows (amd64)

### Container Images

Coming soon.

### Build from Source

```bash
# Prerequisites: Go 1.22+

# Clone and build
git clone https://github.com/basekick-labs/arc-go.git
cd arc-go
make build

# Run
./arc
```

---

## Features

- **Ingestion**: MessagePack columnar (fastest), InfluxDB Line Protocol
- **Query**: DuckDB SQL engine, JSON and Apache Arrow IPC responses
- **Storage**: Local filesystem, S3, MinIO
- **Auth**: Token-based authentication with in-memory caching
- **Durability**: Optional write-ahead log (WAL)
- **Compaction**: Tiered (hourly/daily) automatic file merging
- **Data Management**: Retention policies, continuous queries, GDPR-compliant delete
- **Observability**: Prometheus metrics, structured logging, graceful shutdown
- **Reliability**: Circuit breakers, retry with exponential backoff

---

## Configuration

Arc uses TOML configuration with environment variable overrides.

```toml
[server]
host = "0.0.0.0"
port = 8000

[storage]
backend = "local"        # local, s3, minio
local_path = "./data/arc"

[ingest]
flush_interval = "5s"
max_buffer_size = 50000

[auth]
enabled = true
```

Environment variables use `ARC_` prefix:

```bash
export ARC_SERVER_PORT=8000
export ARC_STORAGE_BACKEND=s3
export ARC_AUTH_ENABLED=true
```

See [arc.toml](./arc.toml) for complete configuration reference.

---

## Project Structure

```
arc-go/
├── cmd/arc/           # Application entry point
├── internal/
│   ├── api/           # HTTP handlers (Fiber)
│   ├── auth/          # Token authentication
│   ├── compaction/    # Tiered file compaction
│   ├── config/        # Configuration management
│   ├── database/      # DuckDB connection pool
│   ├── ingest/        # MessagePack, Line Protocol, Arrow writer
│   ├── logger/        # Structured logging (zerolog)
│   ├── metrics/       # Prometheus metrics
│   ├── pruning/       # Query partition pruning
│   ├── shutdown/      # Graceful shutdown coordinator
│   ├── storage/       # Local, S3, MinIO backends
│   ├── telemetry/     # Usage telemetry
│   ├── circuitbreaker/# Resilience patterns
│   └── wal/           # Write-ahead log
├── test/integration/  # Integration tests
├── arc.toml           # Configuration file
├── Makefile           # Build commands
└── go.mod
```

---

## Development

```bash
make deps           # Install dependencies
make build          # Build binary
make run            # Run without building
make test           # Run tests
make test-coverage  # Run tests with coverage
make bench          # Run benchmarks
make lint           # Run linter
make fmt            # Format code
make clean          # Clean build artifacts
```

---

## License

Arc is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

- Free to use, modify, and distribute
- If you modify Arc and run it as a service, you must share your changes under AGPL-3.0

For commercial licensing, contact: **enterprise@basekick.net**
