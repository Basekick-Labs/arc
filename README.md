# Arc

[![Ingestion](https://img.shields.io/badge/ingestion-18.6M%20rec%2Fs-brightgreen)](https://github.com/basekick-labs/arc)
[![Query](https://img.shields.io/badge/query-2.64M%20rows%2Fs-blue)](https://github.com/basekick-labs/arc)
[![Go](https://img.shields.io/badge/go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

[![Docs](https://img.shields.io/badge/docs-basekick.net-blue?logo=gitbook)](https://docs.basekick.net/arc)
[![Website](https://img.shields.io/badge/website-basekick.net-orange?logo=firefox)](https://basekick.net)
[![Discord](https://img.shields.io/badge/discord-join-7289da?logo=discord)](https://discord.gg/nxnWfUxsdm)
[![GitHub](https://img.shields.io/github/stars/basekick-labs/arc?style=social)](https://github.com/basekick-labs/arc)

High-performance time-series database for Aerospace, Defense, and Industrial IoT. 18.6M records/sec. Satellite tracking, launch telemetry, ground stations, manufacturing, energy. DuckDB SQL + Parquet + Arrow. AGPL-3.0

---

## The Problem

Aerospace, defense, and industrial systems generate massive telemetry at scale:

* **Aerospace & Defense**: Satellite constellations, launch vehicles, ground stations, orbital tracking
* **Space Operations**: 14K+ objects in orbit, TLE data, SGP4 propagation, conjunction analysis
* **Industrial IoT**: Manufacturing telemetry, mining sensors, equipment monitoring
* **Energy & Utilities**: Grid monitoring, smart meters, renewable output, pipeline sensors
* **Transportation**: Racing telemetry, fleet tracking, logistics optimization
* **Healthcare**: Patient monitoring, medical devices, clinical studies
* **Observability**: Metrics, logs, traces from distributed systems

Traditional time-series databases weren't built for aerospace workloads:
- ITAR compliance requires self-hosted infrastructure
- Mission-critical systems can't risk vendor lock-in
- Burst ingestion during satellite passes (10M+ metrics/sec → silence → burst)
- Multi-decade retention for space missions
- Sub-second queries for real-time decision making

**Arc solves this: 18.6M records/sec ingestion, sub-second queries on billions of rows, portable Parquet files you own, ITAR-ready self-hosted deployment.**

```sql
-- Track satellite orbital elements over time
SELECT
  satellite_id,
  norad_id,
  epoch,
  inclination,
  eccentricity,
  mean_motion,
  LAG(mean_motion) OVER (PARTITION BY satellite_id ORDER BY epoch) as prev_mean_motion,
  mean_motion - LAG(mean_motion) OVER (PARTITION BY satellite_id ORDER BY epoch) as orbital_decay
FROM tle.satellites
WHERE satellite_id LIKE 'Starlink%'
  AND epoch > NOW() - INTERVAL '30 days'
ORDER BY satellite_id, epoch DESC;

-- Analyze ground station contact windows
SELECT
  ground_station_id,
  satellite_id,
  MAX(signal_strength) as peak_signal,
  AVG(data_rate) as avg_throughput,
  SUM(bytes_received) as total_data
FROM telemetry.contacts
WHERE contact_start > NOW() - INTERVAL '24 hours'
GROUP BY ground_station_id, satellite_id
HAVING AVG(data_rate) > 1000000;  -- 1 Mbps minimum

-- Industrial equipment monitoring
SELECT
  device_id,
  facility_name,
  AVG(temperature) OVER (
    PARTITION BY device_id
    ORDER BY timestamp
    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
  ) as temp_moving_avg,
  MAX(pressure) as peak_pressure
FROM iot.sensors
WHERE timestamp > NOW() - INTERVAL '24 hours'
  AND facility_id IN ('plant_7', 'mining_site_42')
HAVING MAX(pressure) > 850;
```

**Standard DuckDB SQL. Window functions, CTEs, joins. No proprietary query language.**

---

## **Live Demo**
See Arc tracking 14,273 satellites in real-time:
🛰️ [https://basekick.net/demos/satellite-tracking](https://basekick.net/demos/satellite-tracking)

---

## Performance

Benchmarked on Apple MacBook Pro M3 Max (14 cores, 36GB RAM, 1TB NVMe).
Test config: 12 concurrent workers, 1000-record batches, IoT sensor data.

### Ingestion

| Protocol | Throughput | p50 Latency | p99 Latency |
|----------|------------|-------------|-------------|
| MessagePack Columnar | **18.6M rec/s** | 0.46ms | 3.68ms |
| MessagePack + Zstd | 16.8M rec/s | 0.55ms | 3.23ms |
| MessagePack + GZIP | 15.4M rec/s | 0.63ms | 3.17ms |
| Line Protocol | 3.7M rec/s | 2.63ms | 10.63ms |

### Compaction

Automatic background compaction merges small Parquet files into optimized larger files:

| Metric | Before | After | Reduction |
|--------|--------|-------|-----------|
| Files | 43 | 1 | 97.7% |
| Size | 372 MB | 36 MB | **90.4%** |

Benefits:
- **10x storage reduction** via better compression and encoding
- **Faster queries** - scan 1 file vs 43 files
- **Lower cloud costs** - less storage, fewer API calls

### Query (January 2026)

Arrow IPC format provides 2x throughput vs JSON for large result sets:

| Query | Arrow (ms) | JSON (ms) | Speedup |
|-------|------------|-----------|---------|
| COUNT(*) - Full Table | 6.7 | 9.0 | 1.35x |
| SELECT LIMIT 10K | 27 | 31 | 1.14x |
| SELECT LIMIT 100K | 55 | 103 | 1.88x |
| SELECT LIMIT 500K | 201 | 420 | **2.10x** |
| SELECT LIMIT 1M | 379 | 789 | **2.08x** |
| AVG/MIN/MAX Aggregation | 146 | 146 | 1.00x |
| GROUP BY host (Top 10) | 107 | 104 | 0.98x |
| Last 1 hour filter | 12 | 11 | 0.96x |

**Best throughput:**
- Arrow: **2.64M rows/sec** (1M row SELECT)
- JSON: **1.27M rows/sec** (1M row SELECT)
- COUNT(*): **12-19B rows/sec** (134M rows, 7-11ms)

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
sudo dpkg -i arc_26.02.2_amd64.deb
sudo systemctl enable arc && sudo systemctl start arc
```

### RHEL/Fedora

```bash
wget https://github.com/basekick-labs/arc/releases/download/v26.01.2/arc-26.01.2-1.x86_64.rpm
sudo rpm -i arc-26.02.2-1.x86_64.rpm
sudo systemctl enable arc && sudo systemctl start arc
```

### Kubernetes (Helm)

```bash
helm install arc https://github.com/basekick-labs/arc/releases/download/v26.01.2/arc-26.02.2.tgz
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
- [@khalid244](https://github.com/khalid244) - S3 partition pruning improvements, multi-line SQL query support
