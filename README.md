# Arc

[![Ingestion](https://img.shields.io/badge/ingestion-10.1M%20rec%2Fs-brightgreen)](https://github.com/basekick-labs/arc)
[![Query](https://img.shields.io/badge/query-2.2M%20rows%2Fs-blue)](https://github.com/basekick-labs/arc)
[![Go](https://img.shields.io/badge/go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

[![Docs](https://img.shields.io/badge/docs-basekick.net-blue?logo=gitbook)](https://docs.basekick.net/arc)
[![Website](https://img.shields.io/badge/website-basekick.net-orange?logo=firefox)](https://basekick.net)
[![Discord](https://img.shields.io/badge/discord-join-7289da?logo=discord)](https://discord.gg/nxnWfUxsdm)
[![GitHub](https://img.shields.io/github/stars/basekick-labs/arc?style=social)](https://github.com/basekick-labs/arc)

High-performance time-series database built on DuckDB. Go implementation.

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

**Arc solves this: 12M records/sec ingestion, sub-second queries on billions of rows, portable Parquet files you own.**

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
FROM data.iot_sensors
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
| MessagePack Columnar | **11.8M rec/s** | 1.06ms | 9.94ms |
| MessagePack + Zstd | 11.76M rec/s | 1.41ms | 22.36ms |
| MessagePack + GZIP | 10.34M rec/s | 1.85ms | 22.70ms |
| Line Protocol | 2.55M rec/s | 6.77ms | 20.99ms |

### Query

| Format | Throughput | Response Size (500K rows) |
|--------|------------|---------------------------|
| Arrow IPC | **2.2M rows/s** | 21.5 MB |
| JSON | 1.0M rows/s | 47.0 MB |

Full table scan: **~1 billion rows/s** (321M records in 329ms)

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

### Docker

```bash
docker run -d \
  -p 8000:8000 \
  -v arc-data:/app/data \
  ghcr.io/basekick-labs/arc:latest
```

### Debian/Ubuntu

```bash
wget https://github.com/basekick-labs/arc/releases/download/v26.01.2/arc_26.01.2_amd64.deb
sudo dpkg -i arc_26.01.2_amd64.deb
sudo systemctl enable arc && sudo systemctl start arc
```

### RHEL/Fedora

```bash
wget https://github.com/basekick-labs/arc/releases/download/v26.01.2/arc-26.01.2-1.x86_64.rpm
sudo rpm -i arc-26.01.2-1.x86_64.rpm
sudo systemctl enable arc && sudo systemctl start arc
```

### Kubernetes (Helm)

```bash
helm install arc https://github.com/basekick-labs/arc/releases/download/v26.01.2/arc-26.01.2.tgz
```

### Build from Source

```bash
# Prerequisites: Go 1.25+

# Clone and build
git clone https://github.com/basekick-labs/arc.git
cd arc
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
arc/
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

---

## Contributors

Thanks to everyone who has contributed to Arc:

- [@schotime](https://github.com/schotime) (Adam Schroder) - Data-time partitioning, compaction API triggers, UTC fixes
