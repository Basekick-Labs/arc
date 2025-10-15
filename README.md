<p align="center">
  <img src="assets/arc_logo.jpg" alt="Arc Logo" width="400"/>
</p>

<h1 align="center">Arc Core</h1>

<p align="center">
  <a href="https://www.gnu.org/licenses/agpl-3.0"><img src="https://img.shields.io/badge/License-AGPL%203.0-blue.svg" alt="License: AGPL-3.0"/></a>
  <a href="https://github.com/basekick-labs/arc-core"><img src="https://img.shields.io/badge/Throughput-2.42M%20RPS-brightgreen.svg" alt="Performance"/></a>
  <a href="https://discord.gg/nxnWfUxsdm"><img src="https://img.shields.io/badge/Discord-Join%20Community-5865F2?logo=discord&logoColor=white" alt="Discord"/></a>
</p>

<p align="center">
  High-performance time-series data warehouse built on DuckDB and Parquet with flexible storage options.
</p>

> **Alpha Release - Technical Preview**
> Arc Core is currently in active development and evolving rapidly. While the system is stable and functional, it is **not recommended for production workloads** at this time. We are continuously improving performance, adding features, and refining the API. Use in development and testing environments only.

## Features

- **High-Performance Ingestion**: MessagePack binary protocol (recommended), InfluxDB Line Protocol (drop-in replacement), JSON
- **Multi-Database Architecture**: Organize data by environment, tenant, or application with database namespaces - [Learn More](#multi-database-architecture)
- **Write-Ahead Log (WAL)**: Optional durability feature for zero data loss (disabled by default) - [Learn More](docs/WAL.md)
- **Automatic File Compaction**: Merges small Parquet files into larger ones for 10-50x faster queries (enabled by default) - [Learn More](docs/COMPACTION.md)
- **DuckDB Query Engine**: Fast analytical queries with SQL, cross-database joins, and advanced analytics
- **Flexible Storage Options**: Local filesystem (fastest), MinIO (distributed), AWS S3/R2 (cloud), or Google Cloud Storage
- **Data Import**: Import data from InfluxDB, TimescaleDB, HTTP endpoints
- **Query Caching**: Configurable result caching for improved performance
- **Apache Superset Integration**: Native dialect for BI dashboards with multi-database schema support
- **Production Ready**: Docker deployment with health checks and monitoring

## Performance Benchmark

**Arc achieves 2.42M records/sec with columnar MessagePack format and authentication enabled!**

### Write Performance - Format Comparison

| Wire Format | Throughput | p50 Latency | p95 Latency | p99 Latency | Notes |
|-------------|------------|-------------|-------------|-------------|-------|
| **MessagePack Columnar** | **2.42M RPS** | **1.74ms** | **28.13ms** | **45.27ms** | Zero-copy passthrough + auth cache (RECOMMENDED) |
| **MessagePack Row** | **908K RPS** | **136.86ms** | **851.71ms** | **1542ms** | Legacy format with conversion overhead |
| **Line Protocol** | **240K RPS** | N/A | N/A | N/A | InfluxDB compatibility mode |

**Columnar Format Advantages:**
- **2.66x faster throughput** vs row format (2.42M vs 908K RPS)
- **78x lower p50 latency** (1.74ms vs 136.86ms)
- **30x lower p95 latency** (28.13ms vs 851.71ms)
- **34x lower p99 latency** (45.27ms vs 1542ms)
- **Near-zero authentication overhead** with 30s token cache

*Tested on Apple M3 Max (14 cores), native deployment, 400 workers*
*MessagePack columnar format with zero-copy Arrow passthrough*

### Authentication Performance

Arc includes built-in token-based authentication with minimal performance overhead thanks to intelligent caching:

| Configuration | Throughput | p50 Latency | p95 Latency | p99 Latency | Notes |
|--------------|-----------|-------------|-------------|-------------|-------|
| **Auth Disabled** | 2.42M RPS | 1.64ms | 27.27ms | 41.63ms | No security (not recommended) |
| **Auth + Cache (30s TTL)** | **2.42M RPS** | **1.74ms** | **28.13ms** | **45.27ms** | **Production recommended** |
| **Auth (no cache)** | 2.31M RPS | 6.36ms | 41.41ms | 63.31ms | 5ms SQLite lookup overhead |

**Key Insights:**
- **Token caching** eliminates auth performance penalty (only +0.1ms overhead vs no auth)
- **30-second TTL** provides excellent hit rate at 2.4M RPS workloads
- **Security with speed**: Full authentication with near-zero performance impact
- **Configurable TTL**: Adjust cache duration via `AUTH_CACHE_TTL` (default: 30s)

**Cache Statistics:**
- **Hit rate**: 99.9%+ at sustained high throughput
- **Revocation delay**: Max 30 seconds (cache TTL)
- **Manual invalidation**: `POST /auth/cache/invalidate` for immediate effect
- **Monitoring**: `GET /auth/cache/stats` for cache performance metrics

### Storage Backend Performance

| Storage Backend | Throughput | Notes |
|----------------|------------|-------|
| **Local NVMe** | **2.42M RPS** | Direct filesystem (fastest) |
| **MinIO** | **~2.1M RPS** | S3-compatible object storage |

**Why is columnar format so much faster?**
1. **Zero conversion overhead** - No flatten tags/fields, no row→column conversion
2. **Better batching** - 1000 records in one columnar structure vs 1000 individual dicts
3. **Smaller wire payload** - Field names sent once instead of repeated per-record
4. **More efficient memory** - Arrays are more compact than list of dicts
5. **Less lock contention** - Fewer buffer operations per batch

**Optimal Configuration:**
- **Format:** MessagePack columnar (2.55x faster than row format)
- **Workers:** ~30x CPU cores for I/O-bound workloads (e.g., 14 cores = 400 workers)
- **Deployment:** Native mode (3.5x faster than Docker)
- **Storage:** Local filesystem for maximum performance, MinIO for distributed deployments
- **Protocol:** MessagePack binary columnar (`/write/v2/msgpack`)
- **Performance Stack:**
  - `uvloop`: 2-4x faster event loop (Cython-based C implementation)
  - `httptools`: 40% faster HTTP parser
  - `orjson`: 20-50% faster JSON serialization (Rust + SIMD)
- **Optimizations:**
  - Zero-copy columnar passthrough (no data transformation)
  - Non-blocking flush operations (writes continue during I/O)

## Quick Start (Native - Recommended for Maximum Performance)

**Native deployment delivers 2.32M RPS vs 570K RPS in Docker (4.1x faster).**

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

**Note:** Docker mode achieves ~570K RPS. For maximum performance (2.32M RPS with columnar format), use native deployment.

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

# Storage Backend Configuration
[storage]
backend = "local"  # Options: local, minio, s3, gcs, ceph

# Option 1: Local Filesystem (fastest, single-node)
[storage.local]
base_path = "./data/arc"      # Or "/mnt/nvme/arc-data" for dedicated storage
database = "default"

# Option 2: MinIO (recommended for distributed deployments)
# [storage]
# backend = "minio"
# [storage.minio]
# endpoint = "http://minio:9000"
# access_key = "minioadmin"
# secret_key = "minioadmin123"
# bucket = "arc"
# database = "default"
# use_ssl = false

# Option 3: AWS S3 / Cloudflare R2
# [storage]
# backend = "s3"
# [storage.s3]
# bucket = "arc-data"
# database = "default"
# region = "us-east-1"
# access_key = "YOUR_ACCESS_KEY"
# secret_key = "YOUR_SECRET_KEY"

# Option 4: Google Cloud Storage
# [storage]
# backend = "gcs"
# [storage.gcs]
# bucket = "arc-data"
# database = "default"
# project_id = "my-project"
# credentials_file = "/path/to/service-account.json"
```

**Configuration Priority** (highest to lowest):
1. Environment variables (e.g., `ARC_WORKERS=16`)
2. `arc.conf` file
3. Built-in defaults

### Storage Backend Selection Guide

| Backend | Performance | Use Case | Pros | Cons |
|---------|-------------|----------|------|------|
| **Local** | Fastest (2.32M RPS) | Single-node, development, edge | Direct I/O, no overhead, simple setup | No distribution, single point of failure |
| **MinIO** | Fast (~2.0M RPS) | Distributed, multi-tenant | S3-compatible, scalable, cost-effective | Requires MinIO service, slight overhead |
| **AWS S3** | Cloud-native | Production, unlimited scale | Fully managed, 99.999999999% durability | Network latency, costs |
| **GCS** | Cloud-native | Google Cloud deployments | Integrated with GCP, global CDN | Network latency, costs |

**Recommendation:**
- **Development/Testing**: Local filesystem (`backend = "local"`)
- **Production (single-node)**: Local filesystem with NVMe storage
- **Production (distributed)**: MinIO or AWS S3/R2
- **Cloud deployments**: AWS S3, Cloudflare R2, or Google Cloud Storage

### Environment Variable Overrides

You can override any setting via environment variables:

```bash
# Server
ARC_HOST=0.0.0.0
ARC_PORT=8000
ARC_WORKERS=8

# Storage - Local Filesystem
STORAGE_BACKEND=local
STORAGE_LOCAL_BASE_PATH=/data/arc
STORAGE_LOCAL_DATABASE=default

# Storage - MinIO (alternative)
# STORAGE_BACKEND=minio
# MINIO_ENDPOINT=minio:9000
# MINIO_ACCESS_KEY=minioadmin
# MINIO_SECRET_KEY=minioadmin123
# MINIO_BUCKET=arc

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
auth = AuthManager(db_path='/data/arc.db')
token = auth.create_token('my-admin', description='Admin token')
print(f'Admin Token: {token}')
"

# Native deployment
cd /path/to/arc-core
source venv/bin/activate
python3 -c "
from api.auth import AuthManager
auth = AuthManager(db_path='./data/arc.db')
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

#### Ingest Data (MessagePack - Columnar Format RECOMMENDED)

**Columnar MessagePack format is 2.55x faster than row format** with zero-copy passthrough to Arrow:

```python
import msgpack
import requests
from datetime import datetime
import os

# Get or create API token
token = os.getenv("ARC_TOKEN")
if not token:
    from api.auth import AuthManager
    auth = AuthManager(db_path='./data/arc.db')
    token = auth.create_token(name='my-app', description='My application')
    print(f"Created token: {token}")
    print(f"Save it: export ARC_TOKEN='{token}'")

# COLUMNAR FORMAT (RECOMMENDED - 2.55x faster)
# All data organized as columns (arrays), not rows
data = {
    "m": "cpu",                    # measurement name
    "columns": {                   # columnar data structure
        "time": [
            int(datetime.now().timestamp() * 1000),
            int(datetime.now().timestamp() * 1000) + 1000,
            int(datetime.now().timestamp() * 1000) + 2000
        ],
        "host": ["server01", "server02", "server03"],
        "region": ["us-east", "us-west", "eu-central"],
        "datacenter": ["aws", "gcp", "azure"],
        "usage_idle": [95.0, 85.0, 92.0],
        "usage_user": [3.2, 10.5, 5.8],
        "usage_system": [1.8, 4.5, 2.2]
    }
}

# Send columnar data (2.32M RPS throughput)
response = requests.post(
    "http://localhost:8000/write/v2/msgpack",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/msgpack",
        "x-arc-database": "default"  # Optional: specify database
    },
    data=msgpack.packb(data)
)

# Check response (returns 204 No Content on success)
if response.status_code == 204:
    print(f"Successfully wrote {len(data['columns']['time'])} records!")
else:
    print(f"Error {response.status_code}: {response.text}")
```

**High-throughput batch ingestion** (columnar format - 2.32M RPS):

```python
# Generate 10,000 records in columnar format
num_records = 10000
base_time = int(datetime.now().timestamp() * 1000)

data = {
    "m": "sensor_data",
    "columns": {
        "time": [base_time + i for i in range(num_records)],
        "sensor_id": [f"sensor_{i % 100}" for i in range(num_records)],
        "location": [f"zone_{i % 10}" for i in range(num_records)],
        "type": ["temperature"] * num_records,
        "temperature": [20 + (i % 10) for i in range(num_records)],
        "humidity": [60 + (i % 20) for i in range(num_records)],
        "pressure": [1013 + (i % 5) for i in range(num_records)]
    }
}

response = requests.post(
    "http://localhost:8000/write/v2/msgpack",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/msgpack"
    },
    data=msgpack.packb(data)
)

if response.status_code == 204:
    print(f"Wrote 10,000 records successfully at 2.32M RPS!")
```

<details>
<summary><b>Row Format (Legacy - 2.55x slower, kept for compatibility)</b></summary>

**Only use row format if you cannot generate columnar data client-side:**

```python
# ROW FORMAT (LEGACY - 908K RPS, much slower)
# Each record is a separate dictionary
data = {
    "batch": [
        {
            "m": "cpu",
            "t": int(datetime.now().timestamp() * 1000),
            "h": "server01",
            "tags": {
                "region": "us-east",
                "dc": "aws"
            },
            "fields": {
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
        }
    ]
}

response = requests.post(
    "http://localhost:8000/write/v2/msgpack",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/msgpack"
    },
    data=msgpack.packb(data)
)
```

**Performance Warning**: Row format has 20-26x higher latency and 2.55x lower throughput than columnar format. Use columnar format whenever possible.

</details>

#### Ingest Data (Line Protocol - InfluxDB Compatibility)

**For drop-in replacement of InfluxDB** - compatible with Telegraf and InfluxDB clients:

```bash
# InfluxDB 1.x compatible endpoint
curl -X POST "http://localhost:8000/api/v1/write?db=mydb" \
  -H "Authorization: Bearer $ARC_TOKEN" \
  -H "Content-Type: text/plain" \
  --data-binary "cpu,host=server01 value=0.64 1633024800000000000"

# Multiple measurements
curl -X POST "http://localhost:8000/api/v1/write?db=metrics" \
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

### Apache Arrow Columnar Queries

Arc supports Apache Arrow format for zero-copy columnar data transfer, ideal for analytics workloads and data pipelines.

**Performance Benefits:**
- **7.36x faster** for large result sets (100K+ rows)
- **43% smaller payloads** compared to JSON
- **Zero-copy** for Pandas, Polars, and other Arrow-compatible tools
- **Columnar format** stays efficient from Parquet → DuckDB → Arrow → client

**Python Example with Pandas:**

```python
import requests
import pyarrow as pa
import pandas as pd

# Execute query and get Arrow format
response = requests.post(
    "http://localhost:8000/query/arrow",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": """
            SELECT
                time_bucket(INTERVAL '1 hour', time) as hour,
                host,
                AVG(usage_idle) as avg_cpu_idle,
                COUNT(*) as sample_count
            FROM cpu
            WHERE time > now() - INTERVAL '24 hours'
            GROUP BY hour, host
            ORDER BY hour DESC
        """
    }
)

# Parse Arrow IPC stream
reader = pa.ipc.open_stream(response.content)
arrow_table = reader.read_all()

# Convert to Pandas DataFrame (zero-copy)
df = arrow_table.to_pandas()

print(f"Retrieved {len(df)} rows")
print(df.head())
```

**Polars Example (even faster):**

```python
import requests
import pyarrow as pa
import polars as pl

response = requests.post(
    "http://localhost:8000/query/arrow",
    headers={"Authorization": f"Bearer {token}"},
    json={"sql": "SELECT * FROM cpu WHERE host = 'server01' LIMIT 100000"}
)

# Parse Arrow and convert to Polars (zero-copy)
reader = pa.ipc.open_stream(response.content)
arrow_table = reader.read_all()
df = pl.from_arrow(arrow_table)

print(df.describe())
```

**When to use Arrow format:**
- Large result sets (10K+ rows)
- Wide tables with many columns
- Data pipelines feeding into Pandas/Polars
- Analytics notebooks and dashboards
- ETL processes requiring columnar data

**When to use JSON format:**
- Small result sets (<1K rows)
- Simple API integrations
- Web dashboards
- Quick debugging and testing

## Multi-Database Architecture

Arc supports multiple databases (namespaces) within a single instance, allowing you to organize and isolate data by environment, tenant, or application.

### Storage Structure

Data is organized as: `{bucket}/{database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet`

```
arc/                           # MinIO bucket
├── default/                   # Default database
│   ├── cpu/2025/01/15/14/    # CPU metrics
│   ├── mem/2025/01/15/14/    # Memory metrics
│   └── disk/2025/01/15/14/   # Disk metrics
├── production/                # Production database
│   ├── cpu/2025/01/15/14/
│   └── mem/2025/01/15/14/
└── staging/                   # Staging database
    ├── cpu/2025/01/15/14/
    └── mem/2025/01/15/14/
```

### Configuration

Configure the database in `arc.conf`:

```toml
[storage.minio]
endpoint = "http://localhost:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket = "arc"
database = "default"  # Database namespace
```

Or via environment variable:
```bash
export MINIO_DATABASE="production"
```

### Writing to Specific Databases

**MessagePack Protocol (Columnar - Recommended):**
```python
import msgpack
import requests
from datetime import datetime

token = "your-token-here"

# Columnar format (2.55x faster)
data = {
    "m": "cpu",
    "columns": {
        "time": [int(datetime.now().timestamp() * 1000)],
        "host": ["server01"],
        "usage_idle": [95.0],
        "usage_user": [3.2],
        "usage_system": [1.8]
    }
}

# Write to production database
response = requests.post(
    "http://localhost:8000/write/v2/msgpack",
    headers={
        "x-api-key": token,
        "Content-Type": "application/msgpack",
        "x-arc-database": "production"  # Specify database
    },
    data=msgpack.packb(data)
)

# Write to staging database
response = requests.post(
    "http://localhost:8000/write/v2/msgpack",
    headers={
        "x-api-key": token,
        "Content-Type": "application/msgpack",
        "x-arc-database": "staging"  # Different database
    },
    data=msgpack.packb(data)
)
```

**Line Protocol:**
```bash
# Write to default database (uses configured database)
curl -X POST http://localhost:8000/write \
  -H "x-api-key: $ARC_TOKEN" \
  -d 'cpu,host=server01 usage_idle=95.0'

# Write to specific database
curl -X POST http://localhost:8000/write \
  -H "x-api-key: $ARC_TOKEN" \
  -H "x-arc-database: production" \
  -d 'cpu,host=server01 usage_idle=95.0'
```

### Querying Across Databases

**Show Available Databases:**
```sql
SHOW DATABASES;
-- Output:
-- default
-- production
-- staging
```

**Show Tables in Current Database:**
```sql
SHOW TABLES;
-- Output:
-- database | table_name | storage_path | file_count | total_size_mb
-- default  | cpu        | s3://arc/default/cpu/ | 150 | 75.2
-- default  | mem        | s3://arc/default/mem/ | 120 | 52.1
```

**Query Specific Database:**
```python
# Query production database
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": "SELECT * FROM production.cpu WHERE timestamp > NOW() - INTERVAL 1 HOUR",
        "format": "json"
    }
)

# Query default database (no prefix needed)
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": "SELECT * FROM cpu WHERE timestamp > NOW() - INTERVAL 1 HOUR",
        "format": "json"
    }
)
```

**Cross-Database Queries:**
```python
# Compare production vs staging metrics
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": """
            SELECT
                p.timestamp,
                p.host,
                p.usage_idle as prod_cpu,
                s.usage_idle as staging_cpu,
                (p.usage_idle - s.usage_idle) as diff
            FROM production.cpu p
            JOIN staging.cpu s
                ON p.timestamp = s.timestamp
                AND p.host = s.host
            WHERE p.timestamp > NOW() - INTERVAL 1 HOUR
            ORDER BY p.timestamp DESC
            LIMIT 100
        """,
        "format": "json"
    }
)
```

### Use Cases

**Environment Separation:**
```toml
# Production instance
database = "production"

# Staging instance
database = "staging"

# Development instance
database = "dev"
```

**Multi-Tenant Architecture:**
```python
# Write tenant-specific data
headers = {
    "x-api-key": token,
    "x-arc-database": f"tenant_{tenant_id}"
}
```

**Data Lifecycle Management:**
```python
# Hot data (frequent queries)
database = "hot"

# Warm data (occasional queries)
database = "warm"

# Cold data (archival)
database = "cold"
```

### Apache Superset Integration

In Superset, Arc databases appear as **schemas**:

1. Install the Arc Superset dialect:
   ```bash
   pip install arc-superset-dialect
   ```

2. Connect to Arc:
   ```
   arc://your-token@localhost:8000/default
   ```

3. View databases as schemas in the Superset UI:
   ```
   Schema: default
     ├── cpu
     ├── mem
     └── disk

   Schema: production
     ├── cpu
     └── mem

   Schema: staging
     ├── cpu
     └── mem
   ```

For more details, see the [Multi-Database Migration Plan](DATABASE_MIGRATION_PLAN.md).

## Write-Ahead Log (WAL) - Durability Feature

Arc includes an optional Write-Ahead Log (WAL) for applications requiring **zero data loss** on system crashes. WAL is **disabled by default** to maximize throughput.

### When to Enable WAL

Enable WAL if you need:
- **Zero data loss** on crashes
- **Regulatory compliance** (finance, healthcare)
- **Guaranteed durability** for critical data

Keep WAL disabled if you:
- **Prioritize maximum throughput** (2.01M records/sec)
- **Can tolerate 0-5 seconds data loss** on rare crashes
- **Have upstream retry logic** (Kafka, message queues)

### Performance Impact

| Configuration | Throughput | Data Loss Risk |
|--------------|-----------|----------------|
| **WAL Disabled (default)** | 2.01M rec/s | 0-5 seconds |
| **WAL async** | 1.67M rec/s (-17%) | <1 second |
| **WAL fdatasync** | 1.63M rec/s (-19%) | Near-zero |
| **WAL fsync** | 1.67M rec/s (-17%) | Zero |

### Enable WAL

Edit `.env` file:

```bash
# Enable Write-Ahead Log for durability
WAL_ENABLED=true
WAL_SYNC_MODE=fdatasync     # Recommended: balanced mode
WAL_DIR=./data/wal
WAL_MAX_SIZE_MB=100
WAL_MAX_AGE_SECONDS=3600
```

### Monitor WAL

Check WAL status via API:

```bash
# Get WAL status
curl http://localhost:8000/api/wal/status

# Get detailed statistics
curl http://localhost:8000/api/wal/stats

# List WAL files
curl http://localhost:8000/api/wal/files

# Health check
curl http://localhost:8000/api/wal/health

# Cleanup old recovered files
curl -X POST http://localhost:8000/api/wal/cleanup
```

**For complete WAL documentation, see [docs/WAL.md](docs/WAL.md)**

## File Compaction - Query Optimization

Arc automatically **compacts small Parquet files into larger ones** to dramatically improve query performance. During high-throughput ingestion, Arc creates many small files (50-100MB). Compaction merges these into optimized 512MB files, reducing file count by 100x and improving query speed by 10-50x.

### Why Compaction Matters

**The Small File Problem:**
- High-throughput ingestion creates 100+ small files per hour
- DuckDB must open every file for queries → slow query performance
- Example: 1000 files × 5ms open time = 5 seconds just to start querying

**After Compaction:**
- **2,704 files → 3 files (901x reduction)** - Real production test results
- **80.4% compression ratio** (3.7 GB → 724 MB with ZSTD)
- Query time: 5 seconds → 0.05 seconds (100x faster)
- Better compression (ZSTD vs Snappy during writes)
- Improved DuckDB parallel scanning

### How It Works

Compaction runs automatically on a schedule (default: every hour at :05):

1. **Scans** for completed hourly partitions (e.g., `2025/10/08/14/`)
2. **Locks** partition to prevent concurrent compaction
3. **Downloads** all small files for that partition
4. **Merges** using DuckDB into optimized 512MB files
5. **Uploads** compacted files with `.compacted` suffix
6. **Deletes** old small files from storage
7. **Cleanup** temp files and releases lock

### Configuration

Compaction is **enabled by default** in [arc.conf](arc.conf):

```toml
[compaction]
enabled = true
min_age_hours = 1         # Wait 1 hour before compacting (let hour complete)
min_files = 10            # Only compact if ≥10 files exist
target_file_size_mb = 512 # Target size for compacted files
schedule = "5 * * * *"    # Cron schedule: every hour at :05
max_concurrent_jobs = 2   # Run 2 compactions in parallel
compression = "zstd"      # Better compression than snappy
compression_level = 3     # Balance compression vs speed
```

### Monitoring Compaction

Check compaction status via API:

```bash
# Get current status
curl http://localhost:8000/api/compaction/status

# Get detailed statistics
curl http://localhost:8000/api/compaction/stats

# List eligible partitions
curl http://localhost:8000/api/compaction/candidates

# Manually trigger compaction
curl -X POST http://localhost:8000/api/compaction/trigger

# View active jobs
curl http://localhost:8000/api/compaction/jobs

# View job history
curl http://localhost:8000/api/compaction/history
```

### Reducing File Count at Source

**Best practice**: Reduce file generation by increasing buffer size before they're written:

```toml
[ingestion]
buffer_size = 200000        # Up from 50,000 (4x fewer files)
buffer_age_seconds = 10     # Up from 5 (2x fewer files)
```

**Impact**:
- **Files generated**: 2,000/hour → 250/hour (8x reduction)
- **Compaction time**: 150s → 20s (7x faster)
- **Memory usage**: +300MB per worker (~12GB total on 42 workers)
- **Query freshness**: 5s → 10s delay

This is the **most effective optimization** - fewer files means faster compaction AND faster queries.

### When to Disable Compaction

Compaction should remain enabled for production, but you might disable it:
- **Testing**: When you want to see raw ingestion files
- **Low write volume**: If you write <10 files per hour
- **Development**: When iterating on ingestion code

To disable, edit [arc.conf](arc.conf):

```toml
[compaction]
enabled = false
```

**For complete compaction documentation, see [docs/COMPACTION.md](docs/COMPACTION.md)**

## Architecture Overview

Arc's architecture is optimized for high-throughput time-series ingestion with **MessagePack columnar format** as the recommended ingestion path, delivering 2.32M records/sec with zero-copy passthrough to Parquet.

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                      │
│  (Python, Go, JavaScript, Telegraf, curl, etc.)             │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ HTTP/HTTPS
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                   Arc API Layer (FastAPI)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  MessagePack │  │ Line Protocol│  │  Query Engine    │  │
│  │Columnar (REC)│  │   (Legacy)   │  │   (DuckDB)       │  │
│  │ 2.32M RPS    │  │  240K RPS    │  │                  │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Write Pipeline
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              Buffering & Processing Layer                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  ArrowParquetBuffer (MessagePack Columnar)           │  │
│  │  RECOMMENDED - Zero-copy passthrough                 │  │
│  │  - Client sends columnar data                        │  │
│  │  - Direct PyArrow RecordBatch → Parquet              │  │
│  │  - No row→column conversion (2.55x faster)           │  │
│  │  - Minimal memory overhead                           │  │
│  │  - Throughput: 2.32M RPS                             │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  ParquetBuffer (Line Protocol / MessagePack Row)     │  │
│  │  LEGACY - For compatibility                          │  │
│  │  - Flattens tags/fields                              │  │
│  │  - Row→column conversion                             │  │
│  │  - Polars DataFrame → Parquet                        │  │
│  │  - Throughput: 240K-908K RPS                         │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Parquet Files (columnar format)
                   ▼
┌─────────────────────────────────────────────────────────────┐
│              Storage Backend (Pluggable)                     │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Local NVMe (Fastest - 2.32M RPS)                     │ │
│  │  • Direct I/O, minimal overhead                       │ │
│  │  • Best for single-node, development, edge            │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  MinIO (Recommended for Production - ~2.0M RPS)       │ │
│  │  • S3-compatible, distributed, scalable               │ │
│  │  • High availability, erasure coding                  │ │
│  │  • Multi-tenant, object versioning                    │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  Alternative backends: AWS S3/R2, Google Cloud Storage      │
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
│  - Zero-copy aggregations on columnar data                  │
└─────────────────────────────────────────────────────────────┘
```

### Ingestion Flow (Columnar Format - Recommended)

1. **Client generates columnar data**: `{m: "cpu", columns: {time: [...], host: [...], val: [...]}}`
2. **MessagePack serialization**: Binary encoding (10-30% smaller than JSON)
3. **Arc receives columnar batch**: No parsing overhead, validates array lengths
4. **Zero-copy passthrough**: Direct PyArrow RecordBatch creation
5. **Buffering**: In-memory columnar batches (minimal overhead)
6. **Parquet writes**: Direct columnar → Parquet (no conversion)
7. **Storage**: Write to local NVMe or MinIO (2.32M RPS sustained)

**Key Advantages:**
- **2.55x faster throughput** vs row format (2.32M vs 908K RPS)
- **20-26x lower latency** (p50: 6.75ms vs 136ms)
- **Zero conversion overhead** - No flatten, no row→column conversion
- **Better compression** - Field names sent once, not per-record
- **More efficient memory** - Arrays more compact than list of dicts

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
- **Cold Run Total**: 22.64s (sum of 43 queries, first execution)
- **With Query Cache**: 16.87s (60s TTL caching enabled, 1.34x speedup)
- **Cache Hit Performance**: 3-20ms per query (sub-second for all cached queries)
- **Cache Hit Rate**: 51% of queries benefit from caching (22/43 queries)
- **Aggregate Performance**: ~4.4M rows/sec cold, ~5.9M rows/sec cached
- **Storage**: Local NVMe SSD
- **Success Rate**: 43/43 queries (100%)
- **Optimizations**: DuckDB pool (early connection release), async gzip decompression

### Key Performance Characteristics

- **Columnar Storage**: Parquet format with Snappy compression
- **Query Engine**: DuckDB with default settings (ClickBench compliant)
- **Result Caching**: 60s TTL for repeated queries (production mode)
- **End-to-End**: All timings include HTTP/JSON API overhead

### Fastest Queries (M3 Max)

| Query | Time | Description |
|-------|------|-------------|
| Q1 | 0.043s | Simple COUNT(*) aggregation |
| Q7 | 0.036s | MIN/MAX on date column |
| Q8 | 0.039s | GROUP BY with filter |
| Q20 | 0.047s | Point lookup by UserID |
| Q42 | 0.043s | Multi-column aggregation |

### Most Complex Queries

| Query | Time | Description |
|-------|------|-------------|
| Q29 | 8.09s | REGEXP_REPLACE with heavy string operations |
| Q19 | 1.69s | Timestamp conversion with GROUP BY |
| Q33 | 1.28s | Complex multi-column aggregations |
| Q23 | 1.10s | String matching with LIKE patterns |

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

- `POST /query` - Execute DuckDB SQL query (JSON response)
- `POST /query/arrow` - Execute DuckDB SQL query (Apache Arrow columnar format)
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

### Compaction Management

- `GET /api/compaction/status` - Current compaction status
- `GET /api/compaction/stats` - Detailed statistics
- `GET /api/compaction/candidates` - List eligible partitions
- `POST /api/compaction/trigger` - Manually trigger compaction
- `GET /api/compaction/jobs` - View active jobs
- `GET /api/compaction/history` - View job history

### Interactive API Documentation

Arc Core includes auto-generated API documentation:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI JSON**: `http://localhost:8000/openapi.json`

## Integrations

### Apache Superset - Interactive Dashboards

Create interactive dashboards and visualizations for your Arc data using Apache Superset:

**Quick Start:**
```bash
# Install the Arc dialect
pip install arc-superset-dialect

# Or use Docker with Arc pre-configured
git clone https://github.com/basekick-labs/arc-superset-dialect.git
cd arc-superset-dialect
docker build -t superset-arc .
docker run -d -p 8088:8088 superset-arc
```

**Connect to Arc:**
1. Access Superset at `http://localhost:8088` (admin/admin)
2. Add database connection: `arc://YOUR_API_KEY@localhost:8000/default`
3. Start building dashboards with SQL queries

**Example Dashboard Queries:**
```sql
-- Time-series CPU usage
SELECT
    time_bucket(INTERVAL '5 minutes', timestamp) as time,
    host,
    AVG(usage_idle) as avg_idle
FROM cpu
WHERE timestamp > NOW() - INTERVAL 6 HOUR
GROUP BY time, host
ORDER BY time DESC;

-- Correlate CPU and Memory
SELECT c.timestamp, c.host, c.usage_idle, m.used_percent
FROM cpu c
JOIN mem m ON c.timestamp = m.timestamp AND c.host = m.host
WHERE c.timestamp > NOW() - INTERVAL 1 HOUR
LIMIT 1000;
```

**Learn More:**
- [Arc Superset Dialect Repository](https://github.com/basekick-labs/arc-superset-dialect)
- [Integration Guide](https://github.com/basekick-labs/arc-superset-dialect#readme)
- [PyPI Package](https://pypi.org/project/arc-superset-dialect/) (once published)

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
- **Free to use** - Use Arc Core for any purpose
- **Free to modify** - Modify the source code as needed
- **Free to distribute** - Share your modifications with others
- **Share modifications** - If you modify Arc and run it as a service, you must share your changes under AGPL-3.0

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

- **Discord Community**: [Join our Discord](https://discord.gg/nxnWfUxsdm) - Get help, share feedback, and connect with other Arc users
- **GitHub Issues**: [Report bugs and request features](https://github.com/basekick-labs/arc-core/issues)
- **Enterprise Support**: enterprise[at]basekick[dot]net
- **General Inquiries**: support[at]basekick[dot]net

## Disclaimer

**Arc Core is provided "as-is" in alpha state.** While we use it extensively for development and testing, it is not yet production-ready. Features and APIs may change without notice. Always back up your data and test thoroughly in non-production environments before considering any production deployment.
