# Arc Core - Project Structure

This document describes the arc-core repository structure and what files were migrated from historian_product.

## Directory Structure

```
arc-core/
├── api/                    # Core API components
│   ├── main.py            # FastAPI application (79KB)
│   ├── duckdb_engine.py   # Query engine (26KB)
│   ├── line_protocol_routes.py
│   ├── msgpack_routes.py
│   ├── http_json_routes.py
│   ├── kafka_routes.py
│   ├── auth.py            # Authentication/RBAC
│   ├── database.py        # Connection management
│   ├── query_cache.py     # Query result caching
│   ├── scheduler.py       # Export scheduler
│   ├── models.py          # Pydantic models
│   └── ...
│
├── ingest/                 # Data ingestion
│   ├── line_protocol_parser.py
│   ├── msgpack_decoder.py
│   ├── arrow_writer.py
│   ├── parquet_buffer.py
│   └── parquet_buffer_batched.py
│
├── storage/                # Storage backends
│   ├── local_backend.py
│   ├── minio_backend.py
│   ├── s3_backend.py
│   └── gcs_backend.py
│
├── exporter/               # Data export
│   ├── http_json_exporter.py
│   ├── kafka_exporter.py
│   ├── influx1x_exporter.py
│   ├── influx2x_exporter.py
│   └── timescale_exporter.py
│
├── config.py               # Configuration
├── config_loader.py        # Config loading
├── kafka_consumer.py       # Kafka consumer service
│
├── Dockerfile              # Multi-stage production build
├── docker-compose.yml      # Arc + MinIO stack
├── entrypoint.sh          # Docker entrypoint
├── start.sh               # Quick start script
├── deploy.sh              # Remote deployment script
│
├── requirements.txt        # Minimal production deps
├── .env.example           # Configuration template
├── .dockerignore
├── .gitignore
├── README.md              # Main documentation
└── STRUCTURE.md           # This file
```

## What Was Migrated

### ✅ Included in arc-core (Production Code)

**API Components (20 files, ~385KB)**
- All core API routes and handlers
- Authentication and RBAC
- Query engine and caching
- Monitoring and logging

**Ingestion (5 files, ~56KB)**
- Line protocol parser
- MessagePack decoder
- Arrow/Parquet writers

**Storage Backends (4 files, ~38KB)**
- Local, MinIO, S3, GCS support

**Exporters (6 files, ~69KB)**
- HTTP, Kafka, InfluxDB, TimescaleDB

**Configuration**
- Minimal production requirements
- Docker deployment files
- Deployment scripts

### ❌ Left in historian_product (Development/Testing)

**Documentation**
- All markdown files (except README)
- Architecture docs
- Deployment guides
- ClickBench results

**Benchmarks**
- benchmarks/clickbench/
- Performance test results

**Scripts**
- scripts/ directory (~40 scripts)
- Debug and test utilities
- Migration tools
- Load testing

**UI**
- arc-ui/ (frontend)
- Grafana integration

**Tests**
- All test_*.py files
- Demo scripts
- Example code

**Build Artifacts**
- __pycache__/
- data/
- logs/
- .git/ (separate repos)

## File Size Summary

Total arc-core production code: ~548KB

- API: 385KB (20 files)
- Ingest: 56KB (5 files)
- Storage: 38KB (4 files)
- Exporter: 69KB (6 files)

## Docker Image Layers

1. **Builder stage**: Compile dependencies
2. **Production stage**: Runtime environment only
3. **Non-root user**: Security best practice
4. **Health checks**: Automatic monitoring

## Deployment Options

### 1. Docker (Recommended)
```bash
./start.sh docker
# or
docker-compose up -d
```

### 2. Native Python
```bash
./start.sh native
```

### 3. Remote Deployment
```bash
./deploy.sh -h your-server.com -u ubuntu -m docker
```

## Environment Variables

See `.env.example` for full configuration options.

Key variables:
- `STORAGE_BACKEND`: local, minio, s3, gcs
- `QUERY_CACHE_ENABLED`: Enable query result caching
- `WORKERS`: Number of API workers
- `LOG_LEVEL`: INFO, DEBUG, WARNING, ERROR

## Next Steps

1. **Test locally**: `./start.sh docker`
2. **Customize .env**: Copy from .env.example
3. **Build Docker image**: `docker build -t arc-core:latest .`
4. **Deploy to server**: `./deploy.sh -h <host>`
5. **Push to registry**: Tag and push to Docker Hub/ECR

## Maintenance

- **Update dependencies**: Edit requirements.txt
- **Add features**: Update historian_product first, then sync to arc-core
- **Version releases**: Tag arc-core releases separately

## Support

For development and testing, continue using historian_product.
For production deployment, use arc-core.
