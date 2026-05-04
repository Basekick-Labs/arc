# Arc

[![Ingestion](https://img.shields.io/badge/ingestion-19M%2B%20rec%2Fs-brightgreen)](https://github.com/basekick-labs/arc)
[![Query](https://img.shields.io/badge/query-6.29M%20rows%2Fs-blue)](https://github.com/basekick-labs/arc)
[![Go](https://img.shields.io/badge/go-1.26+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

[![Docs](https://img.shields.io/badge/docs-basekick.net-blue?logo=gitbook)](https://docs.basekick.net/arc)
[![Website](https://img.shields.io/badge/website-basekick.net-orange?logo=firefox)](https://basekick.net)
[![Discord](https://img.shields.io/badge/discord-join-7289da?logo=discord)](https://discord.gg/nxnWfUxsdm)
[![GitHub](https://img.shields.io/github/stars/basekick-labs/arc?style=social)](https://github.com/basekick-labs/arc)

High-performance columnar analytical database. 19M+ records/sec ingestion, 6M+ rows/sec queries. Built on DuckDB + Parquet + Arrow. Use for product analytics, observability, AI agents, IoT, logs, or data warehousing. Single binary. No vendor lock-in. AGPL-3.0

---

## The Problem

Modern applications generate massive amounts of data that needs fast ingestion and analytical queries:

* **Product Analytics**: Events, clickstreams, user behavior, A/B testing
* **Observability**: Metrics, logs, traces from distributed systems
* **AI Agent Memory**: Conversation history, context, RAG, embeddings
* **Industrial IoT**: Manufacturing telemetry, sensors, equipment monitoring
* **Security & Compliance**: Audit logs, SIEM, security events
* **Data Warehousing**: Analytics, BI, reporting on time-series or event data

Traditional solutions have problems:
- **Expensive**: Cloud data warehouses cost thousands per month at scale
- **Complex**: ClickHouse/Druid require cluster management expertise
- **Vendor lock-in**: Proprietary formats trap your data
- **Slow ingestion**: Most analytical DBs struggle with high-throughput writes
- **Overkill**: Need simple deployment, not Kubernetes orchestration

**Arc solves this: 19M+ records/sec ingestion, 6M+ rows/sec queries, portable Parquet files you own, single binary deployment.**

```sql
-- Product analytics: user events
SELECT
  user_id,
  event_type,
  COUNT(*) as event_count,
  COUNT(DISTINCT session_id) as sessions
FROM analytics.events
WHERE timestamp > NOW() - INTERVAL '7 days'
  AND event_type IN ('page_view', 'click', 'purchase')
GROUP BY user_id, event_type
HAVING COUNT(*) > 100;

-- Observability: error rate by service
SELECT
  service_name,
  DATE_TRUNC('hour', timestamp) as hour,
  COUNT(*) as total_requests,
  SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) as errors,
  (SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 as error_rate
FROM logs.http_requests
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY service_name, hour
HAVING error_rate > 1.0;

-- AI agent memory: conversation search
SELECT
  agent_id,
  conversation_id,
  user_message,
  assistant_response,
  created_at
FROM ai.conversations
WHERE agent_id = 'support-bot-v2'
  AND created_at > NOW() - INTERVAL '30 days'
  AND user_message ILIKE '%refund%'
ORDER BY created_at DESC
LIMIT 100;
```

**Standard DuckDB SQL. Window functions, CTEs, joins. No proprietary query language.**

---

## **Live Demo**
See Arc in action: [https://basekick.net/demos](https://basekick.net/demos)

---

## Performance

Benchmarked on Apple MacBook Pro M3 Max (14 cores, 36GB RAM, 1TB NVMe).
Test config: 12 concurrent workers, 1000-record batches, columnar data.

### Ingestion

| Protocol | Throughput | p50 Latency | p99 Latency |
|----------|------------|-------------|-------------|
| MessagePack Columnar | **19.9M rec/s** | 0.43ms | 2.95ms |
| MessagePack + Zstd | 16.5M rec/s | 0.60ms | 2.70ms |
| MessagePack + GZIP | 16.5M rec/s | 0.60ms | 2.71ms |
| Line Protocol | 4.1M rec/s | 2.41ms | 8.85ms |

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

### Query (March 2026)

Arrow IPC format provides up to 3.6x throughput vs JSON for large result sets:

| Query | Arrow (ms) | JSON (ms) | Speedup |
|-------|------------|-----------|---------|
| COUNT(*) - 1.88B rows | 1.9 | 1.8 | 0.95x |
| SELECT LIMIT 10K | 70 | 75 | 1.07x |
| SELECT LIMIT 100K | 88 | 106 | 1.20x |
| SELECT LIMIT 500K | 127 | 253 | **1.99x** |
| SELECT LIMIT 1M | 159 | 438 | **2.75x** |
| Time Range (7d) LIMIT 10K | 45 | 51 | 1.13x |
| Time Bucket (1h, 7d) | 986 | 1089 | 1.10x |
| Date Trunc (day, 30d) | 2013 | 2190 | 1.09x |

**Best throughput:**
- Arrow: **6.29M rows/sec** (1M row SELECT)
- JSON: **2.28M rows/sec** (1M row SELECT)
- COUNT(*): **~1.1T rows/sec** (1.88B rows, 1.8ms)

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
wget https://github.com/basekick-labs/arc/releases/download/v26.05.1/arc_26.05.1_amd64.deb
sudo dpkg -i arc_26.05.1_amd64.deb
sudo systemctl enable arc && sudo systemctl start arc
```

### RHEL/Fedora

```bash
wget https://github.com/basekick-labs/arc/releases/download/v26.05.1/arc-26.05.1-1.x86_64.rpm
sudo rpm -i arc-26.05.1-1.x86_64.rpm
sudo systemctl enable arc && sudo systemctl start arc
```

### Kubernetes (Helm)

```bash
helm install arc https://github.com/basekick-labs/arc/releases/download/v26.05.1/arc-26.05.1.tgz
```

### Build from Source

```bash
# Prerequisites: Go 1.26+

# Clone and build
git clone https://github.com/basekick-labs/arc.git
cd arc
make build

# Or build directly with Go (the duckdb_arrow tag is required)
go build -tags=duckdb_arrow ./cmd/arc

# Run
./arc
```

---

## Ecosystem & Integrations

| Tool | Description | Link |
|------|-------------|------|
| **VS Code Extension** | Browse databases, run queries, visualize results | [Marketplace](https://marketplace.visualstudio.com/items?itemName=basekick-labs.arc-db-manager) |
| **Grafana Data Source** | Native Grafana plugin for dashboards and alerting | [GitHub](https://github.com/Basekick-Labs/grafana-arc-datasource) |
| **Telegraf Output Plugin** | Ship data from 300+ Telegraf inputs directly to Arc | [Docs](https://docs.influxdata.com/telegraf/v1/output-plugins/arc/) |
| **Python SDK** | Query and ingest from Python applications | [PyPI](https://pypi.org/project/arc-tsdb-client/) |
| **Superset Dialect (JSON)** | Apache Superset connector using JSON transport | [GitHub](https://github.com/Basekick-Labs/arc-superset-dialect) |
| **Superset Dialect (Arrow)** | Apache Superset connector using Arrow transport | [GitHub](https://github.com/Basekick-Labs/arc-superset-arrow) |

---

## Features

### Core Capabilities
- **Columnar storage**: Parquet format with DuckDB query engine
- **Multi-use-case**: Product analytics, observability, AI, IoT, logs, data warehousing

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
├── cmd/arc/              # Application entry point
├── internal/
│   ├── api/              # HTTP handlers (Fiber) — query, write, import, TLE, admin
│   ├── audit/            # Audit logging for API operations
│   ├── auth/             # Token authentication and RBAC
│   ├── backup/           # Backup and restore (data, metadata, config)
│   ├── circuitbreaker/   # Resilience patterns (retry, backoff)
│   ├── cluster/          # Raft consensus, node roles, WAL replication
│   ├── compaction/       # Tiered hourly/daily Parquet file merging
│   ├── config/           # TOML configuration with env var overrides
│   ├── database/         # DuckDB connection pool
│   ├── governance/       # Per-token query quotas and rate limiting
│   ├── ingest/           # MessagePack, Line Protocol, TLE, Arrow writer
│   ├── license/          # License validation and feature gating
│   ├── logger/           # Structured logging (zerolog)
│   ├── metrics/          # Prometheus metrics
│   ├── mqtt/             # MQTT subscriber — topic-to-measurement ingestion
│   ├── pruning/          # Query-time partition pruning
│   ├── query/            # Parallel partition executor
│   ├── queryregistry/    # Active/completed query tracking
│   ├── scheduler/        # Continuous queries and retention policies
│   ├── shutdown/         # Graceful shutdown coordinator
│   ├── sql/              # SQL parsing utilities
│   ├── storage/          # Local, S3, Azure backends
│   ├── telemetry/        # Usage telemetry
│   ├── tiering/          # Hot/cold storage lifecycle management
│   └── wal/              # Write-ahead log
├── pkg/models/           # Shared data structures (Record, ColumnarRecord)
├── benchmarks/           # Performance benchmarking suites
├── deploy/               # Docker Compose and Kubernetes configs
├── helm/                 # Helm charts
├── scripts/              # Utility scripts (analysis, backfill, debugging)
├── arc.toml              # Configuration file
├── Makefile              # Build commands
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
