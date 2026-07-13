# Arc

[![Ingestion](https://img.shields.io/badge/ingestion-19M%2B%20rec%2Fs-brightgreen)](https://github.com/basekick-labs/arc)
[![Query](https://img.shields.io/badge/query-8.42M%20rows%2Fs-blue)](https://github.com/basekick-labs/arc)
[![Go](https://img.shields.io/badge/go-1.26+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

[![Docs](https://img.shields.io/badge/docs-basekick.net-blue?logo=gitbook)](https://docs.basekick.net/arc)
[![Website](https://img.shields.io/badge/website-basekick.net-orange?logo=firefox)](https://basekick.net)
[![Discord](https://img.shields.io/badge/discord-join-7289da?logo=discord)](https://discord.gg/nxnWfUxsdm)
[![GitHub](https://img.shields.io/github/stars/basekick-labs/arc?style=social)](https://github.com/basekick-labs/arc)

High-performance columnar analytical database. 19M+ records/sec ingestion, 8M+ rows/sec queries. Ingestion, storage, compaction, SQL queries, retention policies, and continuous queries — in one binary. Open Parquet files on your storage. No vendor lock-in. AGPL-3.0.

> **Prefer a UI?** [**Arc Launchpad**](https://github.com/Basekick-Labs/launchpad) is a self-hosted web console for the Arc instances you run — SQL console, schema explorer, logs, monitoring, and management for tokens, retention, alerts, continuous queries, and MQTT ingestion. Deploy it alongside Arc with Docker Compose. [Docs](https://docs.basekick.net/launchpad).

---

## The Problem

Modern applications generate massive amounts of data that needs fast ingestion and analytical queries:

* **Product Analytics**: Events, clickstreams, user behavior, A/B testing
* **Observability**: Metrics, logs, traces from distributed systems
* **AI Agent Memory**: Conversation history, context, RAG, embeddings
* **Edge & Tactical**: Disconnected operations, tactical edge platforms, sensor telemetry, MQTT-native
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

---

## What Arc is (and isn't)

Arc is a complete analytical database: ingestion pipeline, storage engine, compaction system, SQL query layer, retention policy manager, continuous query scheduler, and MQTT subscriber — in one binary. It uses DuckDB as its query engine the same way PostgreSQL uses its own, but Arc adds everything the query engine doesn't: high-throughput ingestion with automatic Parquet flushing, background compaction, scheduled compute, data lifecycle management, authentication, backup and restore, and enterprise clustering.

Arc is **not a wrapper**. You don't bring your own ingestion, compaction, or retention policies. Arc provides the full stack.

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

**Standard SQL. Window functions, CTEs, joins, aggregations. No proprietary query language.**

---

## **Live Demo**
See Arc in action: [https://basekick.net/demos](https://basekick.net/demos)

---

## Performance

Benchmarked on Apple MacBook Pro M3 Max (14 cores, 36GB RAM, 1TB NVMe).
Test config: 12 concurrent workers, 1000-record batches, columnar data.

### Ingestion (June 2026)

| Protocol | Throughput | p50 Latency | p99 Latency |
|----------|------------|-------------|-------------|
| MessagePack Columnar | **20.9M rec/s** | 0.43ms | 2.67ms |
| MessagePack + Zstd | 17.2M rec/s | 0.58ms | 2.49ms |
| MessagePack + GZIP | 16.9M rec/s | 0.59ms | 2.55ms |
| Line Protocol | 5.4M rec/s | 1.83ms | 7.24ms |

All rows measured over a 60-second sustained run. The radix flush-sort and single-hour
allocation fixes in 26.06.2 lifted MessagePack Columnar from 19.9M to 20.9M and flattened
its decay curve; the Zstd/GZIP rows benefit from the same flush-path work.

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

### Query (May 2026)

Arc speaks three wire formats from the same query engine. **Arrow IPC** is the throughput leader for analytical clients (Grafana, pyarrow, polars) that can take an Arrow dependency — zero-copy from the engine's internal columnar buffers. **MessagePack** (experimental, columnar) is the choice for clients that don't speak Arrow but want smaller bytes and faster decode than JSON — same envelope shape as JSON, native binary types for timestamps and binary columns. **JSON** stays the default for ergonomic compatibility.

Benchmark: 393.7M-row `cpu` measurement, 5 iterations per query, M3 Max. Latency is p50 in milliseconds. The five SELECT-LIMIT rows were measured back-to-back in the same session so the three columns are apples-to-apples; the DuckDB-bound rows (Time Bucket, Date Trunc, GROUP BY) are dominated by query execution and converge across wire formats.

| Query | JSON (ms) | MessagePack (ms) | Arrow IPC (ms) | msgpack vs JSON | Arrow vs JSON |
|-------|----------:|-----------------:|---------------:|----------------:|--------------:|
| COUNT(*) — 393.7M rows | 1.03 | 1.03 | 0.86 | 1.00x | 1.20x |
| SELECT LIMIT 10K | 18.4 | 16.6 | 14.7 | 1.11x | 1.25x |
| SELECT LIMIT 100K | 48.1 | 33.2 | 31.0 | **1.45x** | **1.55x** |
| SELECT LIMIT 500K | 173.2 | 81.1 | 61.1 | **2.14x** | **2.84x** |
| SELECT LIMIT 1M | 334.2 | **133.6** | **105.4** | **2.49x** | **3.17x** |
| Time Range (7d) LIMIT 10K | 15.0 | 15.5 | 15.5 | 0.97x | 0.97x |
| Time Bucket (1h, 7d) | 4.7 | 4.8 | 4.7 | 0.98x | 1.00x |
| Date Trunc (day, 30d) | 416 | 415 | 413 | 1.00x | 1.01x |
| GROUP BY host | 452 | 450 | 450 | 1.00x | 1.00x |
| GROUP BY host + hour | 645 | 660 | 672 | 0.98x | 0.96x |

**Best throughput on LIMIT 1M (1M-row payload, single connection):**
- Arrow IPC: **9.49M rows/sec** (105.4ms)
- MessagePack: **7.49M rows/sec** (133.6ms)
- JSON: **2.99M rows/sec** (334.2ms)
- COUNT(*): **~382B rows/sec equivalent** (393.7M rows in 1.03ms — parquet footer reads, not a row scan)

**Notes on the table:** the wire-format speedups manifest on response-heavy queries (≥100k rows) where encoding dominates the per-request wall time. For aggregations (Time Bucket, Date Trunc, GROUP BY) the response is tiny — a few rows — and DuckDB execution is 99%+ of the wall time; all three formats converge. The Arrow IPC win comes from a memcpy of the column buffer; the MessagePack endpoint walks each cell through a typed columnar encoder (one type-switch per column, not per row) and lands at ~78% of Arrow IPC's throughput while remaining decodable by any msgpack client without an Arrow dependency.

The MessagePack endpoint is **experimental** (gated behind the `duckdb_arrow` build tag, no operator-tunable row cap yet) — see the 26.06.1 release notes for the wire-format spec, operational constraints, and the columnar-redesign story.

---

## Single binary. Zero dependencies.

Arc deploys as one statically-linked executable. No JVM, no Python environment, no PostgreSQL cluster to manage, no ZooKeeper ensemble to babysit. Run it on a laptop, a factory edge box, a battlefield server, or a Kubernetes cluster. Same binary, same config surface.

- **Air-gap ready**: No external services required at runtime. No license server, no cloud dependency.
- **Edge to cloud**: Deploy at the tactical edge, in a sovereign cloud, or on-premises.
- **Minimal footprint**: One process. Memory usage proportional to active workload, not fleet size.

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
# Docker Hub
docker run -d \
  -p 8000:8000 \
  -v arc-data:/app/data \
  basekicklabs/arc:latest

# or GitHub Container Registry
docker run -d \
  -p 8000:8000 \
  -v arc-data:/app/data \
  ghcr.io/basekick-labs/arc:latest
```

Multi-arch images (`linux/amd64` + `linux/arm64`) are published to both registries on every release.

### macOS (Homebrew)

```bash
brew install basekick-labs/tap/arc
```

Apple Silicon. DuckDB is statically linked, so there are no runtime dependencies. (Use `brew install --formula arc` if you tap first, to disambiguate from the `arc` browser cask.)

### Debian/Ubuntu

```bash
wget https://github.com/basekick-labs/arc/releases/download/v26.06.3/arc_26.06.3_amd64.deb
sudo dpkg -i arc_26.06.3_amd64.deb
sudo systemctl enable arc && sudo systemctl start arc
```

### RHEL/Fedora

```bash
wget https://github.com/basekick-labs/arc/releases/download/v26.06.3/arc-26.06.3-1.x86_64.rpm
sudo rpm -i arc-26.06.3-1.x86_64.rpm
sudo systemctl enable arc && sudo systemctl start arc
```

### Kubernetes (Helm)

```bash
helm install arc https://github.com/basekick-labs/arc/releases/download/v26.06.3/arc-26.06.3.tgz
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

### FIPS 140-3 Build

For US defense/federal and other regulated environments, Arc ships an optional
**`arc-fips`** build: the same source at the same version, compiled against the
CMVP-certified Go Cryptographic Module and run in FIPS-only mode. Pick the
`-fips` artifact instead of the standard one.

```bash
# Binary — download arc-fips-linux-amd64 (or -arm64) from the release
# Container — same repos, -fips tag suffix:
docker run -d -p 8000:8000 -v arc-data:/app/data ghcr.io/basekick-labs/arc:VERSION-fips
# or basekicklabs/arc:VERSION-fips

# Build from source:
make build-fips      # -> arc-fips (GOFIPS140=v1.0.0, -tags=duckdb_arrow,fips)
```

The FIPS build reports the same version as the standard build and logs
`"fips_mode":true` at startup. **Cutover note:** existing bcrypt-hashed API
tokens must be rotated when moving to the FIPS build (it stores new tokens with
PBKDF2 and fails bcrypt verification closed). The Go Cryptographic Module is
CMVP-certified; Arc itself is not a CMVP-listed module. See the
[FIPS 140-3 mode guide](https://docs.basekick.net/docs/configuration/fips).

---

## Ecosystem & Integrations

| Tool | Description | Link |
|------|-------------|------|
| **Arc Launchpad** | Self-hosted web UI: SQL console, schema explorer, logs, monitoring, and management for tokens, retention, alerts, continuous queries, MQTT ingestion, and teams | [GitHub](https://github.com/Basekick-Labs/launchpad) · [Docs](https://docs.basekick.net/launchpad) |
| **VS Code Extension** | Browse databases, run queries, visualize results | [Marketplace](https://marketplace.visualstudio.com/items?itemName=basekick-labs.arc-db-manager) |
| **Grafana Data Source** | Native Grafana plugin for dashboards and alerting | [GitHub](https://github.com/Basekick-Labs/grafana-arc-datasource) |
| **Telegraf Output Plugin** | Ship data from 300+ Telegraf inputs directly to Arc | [Docs](https://docs.influxdata.com/telegraf/v1/output-plugins/arc/) |
| **Python SDK** | Query and ingest from Python applications | [PyPI](https://pypi.org/project/arc-tsdb-client/) |
| **Superset Dialect (JSON)** | Apache Superset connector using JSON transport | [GitHub](https://github.com/Basekick-Labs/arc-superset-dialect) |
| **Superset Dialect (Arrow)** | Apache Superset connector using Arrow transport | [GitHub](https://github.com/Basekick-Labs/arc-superset-arrow) |

---

## Features

### Core Capabilities
- **Columnar storage**: Parquet format with full analytical SQL engine
- **Multi-use-case**: Product analytics, observability, AI, IoT, edge and tactical, logs, data warehousing

- **Ingestion**: MessagePack columnar (fastest), InfluxDB Line Protocol, MQTT, TLE (satellite telemetry)
- **Query**: Full analytical SQL; JSON, columnar MessagePack (experimental), and Apache Arrow IPC responses
- **Compaction**: Tiered (hourly/daily) automatic Parquet file merging — 10x storage reduction
- **Data Lifecycle**: Retention policies, continuous queries, tiered storage (hot/cold)
- **Durability**: Optional write-ahead log (WAL), backup and restore
- **Storage**: Local filesystem, S3, MinIO
- **Auth**: Token-based authentication with in-memory caching
- **Durability**: Optional write-ahead log (WAL)
- **Data Management**: GDPR-compliant delete operations
- **Observability**: Prometheus metrics, structured logging, graceful shutdown
- **Reliability**: Circuit breakers, retry with exponential backoff
- **Supply chain**: SBOM (SPDX + CycloneDX), Trivy scans, cosign-signed releases, SLSA L3 provenance
- **FIPS 140-3**: Optional `arc-fips` build against the CMVP-certified Go Cryptographic Module — see [Installation](#fips-140-3-build)
- **Edge Sync** (coming 26.09.1): Spoke-to-hub data transport for disconnected operations

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
│   ├── database/         # Query engine and connection management
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

Thanks to everyone who has contributed code to Arc:

- [@schotime](https://github.com/schotime) (Adam Schroder) — Data-time partitioning, compaction API triggers, UTC fixes
- [@khalid244](https://github.com/khalid244) — S3 partition pruning improvements, multi-line SQL query support
- [@SAY-5](https://github.com/SAY-5) (Sai Asish Y) — MQTT nil-guard hardening (handlers + manager) with regression coverage

And a thank-you to community members whose bug reports drove fixes:

- [@bjarneksat](https://github.com/bjarneksat) — reported the line-protocol null-field handling bug fixed in 26.03.1
