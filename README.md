<p align="center">
  <img src="assets/arc_logo.jpg" alt="Arc Logo" width="400"/>
</p>

<h1 align="center">Arc Core</h1>

<p align="center">
  <a href="https://www.gnu.org/licenses/agpl-3.0"><img src="https://img.shields.io/badge/License-AGPL%203.0-blue.svg" alt="License: AGPL-3.0"/></a>
  <a href="https://github.com/basekick-labs/arc-core"><img src="https://img.shields.io/badge/Throughput-1.89M%20RPS-brightgreen.svg" alt="Performance"/></a>
</p>

<p align="center">
  High-performance time-series data warehouse built on DuckDB, Parquet, and MinIO.
</p>

> **⚠️ Alpha Release - Technical Preview**
> Arc Core is currently in active development and evolving rapidly. While the system is stable and functional, it is **not recommended for production workloads** at this time. We are continuously improving performance, adding features, and refining the API. Use in development and testing environments only.

## Features

- **High-Performance Ingestion**: MessagePack binary protocol (recommended), InfluxDB Line Protocol (drop-in replacement), JSON
- **DuckDB Query Engine**: Fast analytical queries with SQL
- **Distributed Storage with MinIO**: S3-compatible object storage for unlimited scale and cost-effective data management (recommended). Also supports local disk, AWS S3, and GCS
- **Data Import**: Import data from InfluxDB, TimescaleDB, HTTP endpoints
- **Query Caching**: Configurable result caching for improved performance
- **Production Ready**: Docker deployment with health checks and monitoring

## Performance Benchmark 🚀

**Arc achieves 1.89M records/sec with MessagePack binary protocol!**

| Metric | Value | Notes |
|--------|-------|-------|
| **Throughput** | **1.89M records/sec** | MessagePack binary protocol |
| **p50 Latency** | **21ms** | Median response time |
| **p95 Latency** | **204ms** | 95th percentile |
| **Success Rate** | **99.9998%** | Production-grade reliability |
| **vs Line Protocol** | **7.9x faster** | 240K → 1.89M RPS |

*Tested on Apple M3 Max (14 cores), native deployment with MinIO*

**🎯 Optimal Configuration:**
- **Workers:** 3x CPU cores (e.g., 14 cores = 42 workers)
- **Deployment:** Native mode (2.4x faster than Docker)
- **Storage:** MinIO native (not containerized)
- **Protocol:** MessagePack binary (`/write/v2/msgpack`)

## Quick Start (Native - Recommended for Maximum Performance)

**Native deployment delivers 1.89M RPS vs 570K RPS in Docker (2.4x faster).**

```bash
# One-command start (auto-installs MinIO, auto-detects CPU cores)
./start.sh native

# Alternative: Manual setup
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# Start MinIO natively (auto-configured by start.sh)
brew install minio/stable/minio minio/stable/mc  # macOS
# OR download from https://min.io/download for Linux

# Start Arc (auto-detects optimal worker count: 3x CPU cores)
./start.sh native
```

Arc API will be available at `http://localhost:8000`
MinIO Console at `http://localhost:9001` (minioadmin/minioadmin)

## Quick Start (Docker)

```bash
# Start Arc Core with MinIO
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f arc-api

# Stop
docker-compose down
```

**Note:** Docker mode achieves ~570K RPS. For maximum performance (1.89M RPS), use native deployment.

## Remote Deployment

Deploy Arc Core to a remote server:

```bash
# Docker deployment
./deploy.sh -h your-server.com -u ubuntu -m docker

# Native deployment
./deploy.sh -h your-server.com -u ubuntu -m native
```

## Configuration

Arc Core uses a centralized `arc.conf` configuration file (TOML format). This provides:
- Clean, organized configuration structure
- Environment variable overrides for Docker/production
- Production-ready defaults
- Comments and documentation inline

### Primary Configuration: arc.conf

Edit the `arc.conf` file for all settings:

```toml
# Server Configuration
[server]
host = "0.0.0.0"
port = 8000
workers = 8  # Adjust based on load: 4=light, 8=medium, 16=high

# Authentication
[auth]
enabled = true
default_token = ""  # Leave empty to auto-generate

# Query Cache
[query_cache]
enabled = true
ttl_seconds = 60

# Storage Backend (MinIO recommended)
[storage]
backend = "minio"

[storage.minio]
endpoint = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin123"
bucket = "arc"
use_ssl = false

# For AWS S3
# [storage]
# backend = "s3"
# [storage.s3]
# bucket = "arc-data"
# region = "us-east-1"

# For Google Cloud Storage
# [storage]
# backend = "gcs"
# [storage.gcs]
# bucket = "arc-data"
# project_id = "my-project"
```

**Configuration Priority** (highest to lowest):
1. Environment variables (e.g., `ARC_WORKERS=16`)
2. `arc.conf` file
3. Built-in defaults

### Environment Variable Overrides

You can override any setting via environment variables:

```bash
# Server
ARC_HOST=0.0.0.0
ARC_PORT=8000
ARC_WORKERS=8

# Storage
STORAGE_BACKEND=minio
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_BUCKET=arc

# Cache
QUERY_CACHE_ENABLED=true
QUERY_CACHE_TTL=60

# Logging
LOG_LEVEL=INFO
```

**Legacy Support**: `.env` files are still supported for backward compatibility, but `arc.conf` is recommended.

## Getting Started

### 1. Get Your Admin Token

After starting Arc Core, create an admin token for API access:

```bash
# Docker deployment
docker exec -it arc-api python3 -c "
from api.auth import AuthManager
auth = AuthManager(db_path='/data/historian.db')
token = auth.create_token('my-admin', description='Admin token')
print(f'Admin Token: {token}')
"

# Native deployment
cd /path/to/arc-core
source venv/bin/activate
python3 -c "
from api.auth import AuthManager
auth = AuthManager()
token = auth.create_token('my-admin', description='Admin token')
print(f'Admin Token: {token}')
"
```

Save this token - you'll need it for all API requests.

### 2. API Endpoints

All endpoints require authentication via Bearer token:

```bash
# Set your token
export ARC_TOKEN="your-token-here"
```

#### Health Check
```bash
curl http://localhost:8000/health
```

#### Ingest Data (MessagePack - Recommended)

**MessagePack binary protocol offers 7.9x faster ingestion** with zero-copy PyArrow processing:

```python
import msgpack
import requests
from datetime import datetime
import os

# Get or create API token
token = os.getenv("ARC_TOKEN")
if not token:
    from api.auth import AuthManager
    auth = AuthManager(db_path='./data/historian.db')
    token = auth.create_token(name='my-app', description='My application')
    print(f"Created token: {token}")
    print(f"Save it: export ARC_TOKEN='{token}'")

# Prepare data in MessagePack binary format
# Uses batch format with measurement-based schema
data = {
    "batch": [
        {
            "m": "cpu",                                      # measurement name
            "t": int(datetime.now().timestamp() * 1000),    # timestamp (milliseconds)
            "h": "server01",                                 # host tag (optional)
            "tags": {                                        # additional tags (optional)
                "region": "us-east",
                "dc": "aws"
            },
            "fields": {                                      # metric values (required)
                "usage_idle": 95.0,
                "usage_user": 3.2,
                "usage_system": 1.8
            }
        },
        {
            "m": "cpu",
            "t": int(datetime.now().timestamp() * 1000),
            "h": "server02",
            "tags": {
                "region": "us-west",
                "dc": "gcp"
            },
            "fields": {
                "usage_idle": 85.0,
                "usage_user": 10.5,
                "usage_system": 4.5
            }
        },
        {
            "m": "mem",
            "t": int(datetime.now().timestamp() * 1000),
            "h": "server01",
            "fields": {
                "used_percent": 45.2,
                "available": 8192
            }
        }
    ]
}

# Send via MessagePack
response = requests.post(
    "http://localhost:8000/write/v2/msgpack",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/msgpack"
    },
    data=msgpack.packb(data)
)

# Check response (returns 204 No Content on success)
if response.status_code == 204:
    print(f"✓ Successfully wrote {len(data['batch'])} measurements!")
else:
    print(f"✗ Error {response.status_code}: {response.text}")
```

**Batch ingestion** (for high throughput - 1.89M RPS):

```python
# Generate 10,000 measurements for high-throughput ingestion
batch = [
    {
        "m": "sensor_data",                             # measurement name
        "t": int(datetime.now().timestamp() * 1000),   # timestamp (milliseconds)
        "h": f"sensor_{i % 100}",                       # host/sensor ID
        "tags": {
            "location": f"zone_{i % 10}",
            "type": "temperature"
        },
        "fields": {
            "temperature": 20 + (i % 10),
            "humidity": 60 + (i % 20),
            "pressure": 1013 + (i % 5)
        }
    }
    for i in range(10000)
]

data = {"batch": batch}

response = requests.post(
    "http://localhost:8000/write/v2/msgpack",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/msgpack"
    },
    data=msgpack.packb(data)
)

if response.status_code == 204:
    print(f"✓ Wrote 10,000 measurements successfully!")
```

#### Ingest Data (Line Protocol - InfluxDB Compatibility)

**For drop-in replacement of InfluxDB** - compatible with Telegraf and InfluxDB clients:

```bash
# InfluxDB 1.x compatible endpoint
curl -X POST "http://localhost:8000/write/line?db=mydb" \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H "Content-Type: text/plain" \
  --data-binary "cpu,host=server01 value=0.64 1633024800000000000"

# Multiple measurements
curl -X POST "http://localhost:8000/write/line?db=metrics" \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H "Content-Type: text/plain" \
  --data-binary "cpu,host=server01,region=us-west value=0.64 1633024800000000000
memory,host=server01,region=us-west used=8.2,total=16.0 1633024800000000000
disk,host=server01,region=us-west used=120.5,total=500.0 1633024800000000000"
```

**Telegraf configuration** (drop-in InfluxDB replacement):

```toml
[[outputs.influxdb]]
  urls = ["http://localhost:8000"]
  database = "telegraf"
  skip_database_creation = true

  # Authentication
  username = ""  # Leave empty
  password = "$ARC_TOKEN"  # Use your Arc token as password

  # Or use HTTP headers
  [outputs.influxdb.headers]
    Authorization = "Bearer $ARC_TOKEN"
```

#### Query Data

**Basic query** (Python):

```python
import requests
import os

token = os.getenv("ARC_TOKEN")  # Your API token

# Simple query
response = requests.post(
    "http://localhost:8000/query",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    },
    json={
        "sql": "SELECT * FROM cpu WHERE host = 'server01' ORDER BY time DESC LIMIT 10",
        "format": "json"
    }
)

data = response.json()
print(f"Rows: {len(data['data'])}")
for row in data['data']:
    print(row)
```

**Using curl**:

```bash
curl -X POST http://localhost:8000/query \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM cpu WHERE host = '\''server01'\'' LIMIT 10",
    "format": "json"
  }'
```

**Advanced queries with DuckDB SQL**:

```python
# Time-series aggregation
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": """
            SELECT
                time_bucket(INTERVAL '5 minutes', time) as bucket,
                host,
                AVG(usage_idle) as avg_idle,
                MAX(usage_user) as max_user
            FROM cpu
            WHERE time > now() - INTERVAL '1 hour'
            GROUP BY bucket, host
            ORDER BY bucket DESC
        """,
        "format": "json"
    }
)

# Window functions
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": """
            SELECT
                timestamp,
                host,
                usage_idle,
                AVG(usage_idle) OVER (
                    PARTITION BY host
                    ORDER BY timestamp
                    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                ) as moving_avg
            FROM cpu
            ORDER BY timestamp DESC
            LIMIT 100
        """,
        "format": "json"
    }
)

# Join multiple measurements
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": """
            SELECT
                c.timestamp,
                c.host,
                c.usage_idle as cpu_idle,
                m.used_percent as mem_used
            FROM cpu c
            JOIN mem m ON c.timestamp = m.timestamp AND c.host = m.host
            WHERE c.timestamp > now() - INTERVAL '10 minutes'
            ORDER BY c.timestamp DESC
        """,
        "format": "json"
    }
)
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                      │
│  (Telegraf, Python, Go, JavaScript, curl, etc.)             │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ HTTP/HTTPS
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                   Arc API Layer (FastAPI)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ Line Protocol│  │  MessagePack │  │  Query Engine    │  │
│  │   Endpoint   │  │   Binary API │  │   (DuckDB)       │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Write Pipeline
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              Buffering & Processing Layer                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  ParquetBuffer (Line Protocol)                       │  │
│  │  - Batches records by measurement                    │  │
│  │  - Polars DataFrame → Parquet                        │  │
│  │  - Snappy compression                                │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  ArrowParquetBuffer (MessagePack Binary)             │  │
│  │  - Zero-copy PyArrow RecordBatch                     │  │
│  │  - Direct Parquet writes (3x faster)                 │  │
│  │  - Columnar from start                               │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Parquet Files
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              Storage Backend (Pluggable)                     │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  MinIO (Recommended - S3-compatible)                   │ │
│  │  ✓ Unlimited scale          ✓ Distributed             │ │
│  │  ✓ Cost-effective           ✓ Self-hosted             │ │
│  │  ✓ High availability        ✓ Erasure coding          │ │
│  │  ✓ Multi-tenant             ✓ Object versioning       │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  Alternative backends: Local Disk, AWS S3, Google Cloud     │
└─────────────────────────────────────────────────────────────┘
                   │
                   │ Query Path (Direct Parquet reads)
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              Query Engine (DuckDB)                           │
│  - Direct Parquet reads from object storage                 │
│  - Columnar execution engine                                │
│  - Query cache for common queries                           │
│  - Full SQL interface (Postgres-compatible)                 │
└─────────────────────────────────────────────────────────────┘
```

### Why MinIO?

Arc Core is designed with **MinIO as the primary storage backend** for several key reasons:

1. **Unlimited Scale**: Store petabytes of time-series data without hitting storage limits
2. **Cost-Effective**: Commodity hardware or cloud storage at fraction of traditional database costs
3. **Distributed Architecture**: Built-in replication and erasure coding for data durability
4. **S3 Compatibility**: Works with any S3-compatible storage (AWS S3, GCS, Wasabi, etc.)
5. **Performance**: Direct Parquet reads from object storage with DuckDB's efficient execution
6. **Separation of Compute & Storage**: Scale storage and compute independently
7. **Self-Hosted Option**: Run on your own infrastructure without cloud vendor lock-in

The MinIO + Parquet + DuckDB combination provides the perfect balance of cost, performance, and scalability for analytical time-series workloads.


## Performance

Arc Core has been benchmarked using [ClickBench](https://github.com/ClickHouse/ClickBench) - the industry-standard analytical database benchmark with 100M row dataset (14GB) and 43 analytical queries.

### ClickBench Results

**Hardware: AWS c6a.4xlarge** (16 vCPU AMD EPYC 7R13, 32GB RAM, 500GB gp2)
- **Cold Run Total**: 35.18s (sum of 43 queries, first execution)
- **Hot Run Average**: 0.81s (average per query after caching)
- **Aggregate Performance**: ~2.8M rows/sec cold, ~123M rows/sec hot (across all queries)
- **Storage**: MinIO (S3-compatible)
- **Success Rate**: 43/43 queries (100%)

**Hardware: Apple M3 Max** (14 cores ARM, 36GB RAM)
- **Cold Run Total**: 23.86s (sum of 43 queries, first execution)
- **Hot Run Average**: 0.52s (average per query after caching)
- **Aggregate Performance**: ~4.2M rows/sec cold, ~192M rows/sec hot (across all queries)
- **Storage**: Local NVMe SSD
- **Success Rate**: 43/43 queries (100%)

### Key Performance Characteristics

- **Columnar Storage**: Parquet format with Snappy compression
- **Query Engine**: DuckDB with default settings (ClickBench compliant)
- **Result Caching**: 60s TTL for repeated queries (production mode)
- **End-to-End**: All timings include HTTP/JSON API overhead

### Fastest Queries (M3 Max)

| Query | Time (avg) | Description |
|-------|-----------|-------------|
| Q1 | 0.021s | Simple aggregation |
| Q8 | 0.034s | String parsing |
| Q27 | 0.086s | Complex grouping |
| Q41 | 0.048s | URL parsing |
| Q42 | 0.044s | Multi-column filter |

### Most Complex Queries

| Query | Time (avg) | Description |
|-------|-----------|-------------|
| Q29 | 7.97s | Heavy string operations |
| Q19 | 1.69s | Multiple joins |
| Q33 | 1.86s | Complex aggregations |

**Benchmark Configuration**:
- Dataset: 100M rows, 14GB Parquet (ClickBench hits.parquet)
- Protocol: HTTP REST API with JSON responses
- Caching: Disabled for benchmark compliance
- Tuning: None (default DuckDB settings)

See full results and methodology at [ClickBench Results](https://github.com/ClickHouse/ClickBench) (Arc submission pending).

## Docker Services

The `docker-compose.yml` includes:

- **arc-api**: Main API server (port 8000)
- **minio**: S3-compatible storage (port 9000, console 9001)
- **minio-init**: Initializes MinIO buckets on startup


## Development

```bash
# Run with auto-reload
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# Run tests (if available in parent repo)
pytest tests/
```

## Monitoring

Health check endpoint:
```bash
curl http://localhost:8000/health
```

Logs:
```bash
# Docker
docker-compose logs -f arc-api

# Native (systemd)
sudo journalctl -u arc-api -f
```

## API Reference

### Public Endpoints (No Authentication Required)

- `GET /` - API information
- `GET /health` - Service health check
- `GET /ready` - Readiness probe
- `GET /docs` - Swagger UI documentation
- `GET /redoc` - ReDoc documentation
- `GET /openapi.json` - OpenAPI specification

**Note**: All other endpoints require Bearer token authentication.

### Data Ingestion Endpoints

**MessagePack Binary Protocol** (Recommended - 3x faster):
- `POST /write/v2/msgpack` - Write data via MessagePack
- `POST /api/v2/msgpack` - Alternative endpoint
- `GET /write/v2/msgpack/stats` - Get ingestion statistics
- `GET /write/v2/msgpack/spec` - Get protocol specification

**Line Protocol** (InfluxDB compatibility):
- `POST /write` - InfluxDB 1.x compatible write
- `POST /api/v1/write` - InfluxDB 1.x API format
- `POST /api/v2/write` - InfluxDB 2.x API format
- `POST /api/v1/query` - InfluxDB 1.x query format
- `GET /write/health` - Write endpoint health check
- `GET /write/stats` - Write statistics
- `POST /write/flush` - Force flush write buffer

### Query Endpoints

- `POST /query` - Execute DuckDB SQL query
- `POST /query/estimate` - Estimate query cost
- `POST /query/stream` - Stream large query results
- `GET /query/{measurement}` - Get measurement data
- `GET /query/{measurement}/csv` - Export measurement as CSV
- `GET /measurements` - List all measurements/tables

### Authentication

- `GET /auth/verify` - Verify token validity
- `GET /auth/tokens` - List all tokens
- `POST /auth/tokens` - Create new token
- `GET /auth/tokens/{id}` - Get token details
- `PATCH /auth/tokens/{id}` - Update token
- `DELETE /auth/tokens/{id}` - Delete token
- `POST /auth/tokens/{id}/rotate` - Rotate token (generate new)

### Health & Monitoring

- `GET /health` - Service health check
- `GET /ready` - Readiness probe
- `GET /metrics` - Prometheus metrics
- `GET /metrics/timeseries/{type}` - Time-series metrics
- `GET /metrics/endpoints` - Endpoint statistics
- `GET /metrics/query-pool` - Query pool status
- `GET /metrics/memory` - Memory profile
- `GET /logs` - Application logs

### Connection Management

**InfluxDB Connections**:
- `GET /connections/influx` - List InfluxDB connections
- `POST /connections/influx` - Create InfluxDB connection
- `PUT /connections/influx/{id}` - Update connection
- `DELETE /connections/{type}/{id}` - Delete connection
- `POST /connections/{type}/{id}/activate` - Activate connection
- `POST /connections/{type}/test` - Test connection

**Storage Connections**:
- `GET /connections/storage` - List storage backends
- `POST /connections/storage` - Create storage connection
- `PUT /connections/storage/{id}` - Update storage connection

### Export Jobs

- `GET /jobs` - List all export jobs
- `POST /jobs` - Create new export job
- `PUT /jobs/{id}` - Update job configuration
- `DELETE /jobs/{id}` - Delete job
- `GET /jobs/{id}/executions` - Get job execution history
- `POST /jobs/{id}/run` - Run job immediately
- `POST /jobs/{id}/cancel` - Cancel running job
- `GET /monitoring/jobs` - Monitor job status

### HTTP/JSON Export

- `POST /api/http-json/connections` - Create HTTP/JSON connection
- `GET /api/http-json/connections` - List connections
- `GET /api/http-json/connections/{id}` - Get connection details
- `PUT /api/http-json/connections/{id}` - Update connection
- `DELETE /api/http-json/connections/{id}` - Delete connection
- `POST /api/http-json/connections/{id}/test` - Test connection
- `POST /api/http-json/connections/{id}/discover-schema` - Discover schema
- `POST /api/http-json/export` - Export data via HTTP

### Cache Management

- `GET /cache/stats` - Cache statistics
- `GET /cache/health` - Cache health status
- `POST /cache/clear` - Clear query cache

### Interactive API Documentation

Arc Core includes auto-generated API documentation:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI JSON**: `http://localhost:8000/openapi.json`

## Roadmap

Arc Core is under active development. Current focus areas:

- **Performance Optimization**: Further improvements to ingestion and query performance
- **API Stability**: Finalizing core API contracts
- **Enhanced Monitoring**: Additional metrics and observability features
- **Documentation**: Expanded guides and tutorials
- **Production Hardening**: Testing and validation for production use cases

We welcome feedback and feature requests as we work toward a stable 1.0 release.

## License

Arc Core is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

This means:
- ✅ **Free to use** - Use Arc Core for any purpose
- ✅ **Free to modify** - Modify the source code as needed
- ✅ **Free to distribute** - Share your modifications with others
- ⚠️ **Share modifications** - If you modify Arc and run it as a service, you must share your changes under AGPL-3.0

### Why AGPL?

AGPL-3.0 ensures that improvements to Arc benefit the entire community, even when run as a cloud service. This prevents the "SaaS loophole" where companies could take the code, improve it, and keep changes proprietary.

### Commercial Licensing

For organizations that require:
- Proprietary modifications without disclosure
- Commercial support and SLAs
- Enterprise features and managed services

Please contact us at: **enterprise[at]basekick[dot]net**

We offer dual licensing and commercial support options.

## Support

- **Community Support**: [GitHub Issues](https://github.com/basekick-labs/arc-core/issues)
- **Enterprise Support**: enterprise[at]basekick[dot]net
- **General Inquiries**: support[at]basekick[dot]net

## Disclaimer

**Arc Core is provided "as-is" in alpha state.** While we use it extensively for development and testing, it is not yet production-ready. Features and APIs may change without notice. Always back up your data and test thoroughly in non-production environments before considering any production deployment.
