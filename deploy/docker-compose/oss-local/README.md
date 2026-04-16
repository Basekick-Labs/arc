# Arc OSS — Single Node, Local Storage

Simplest Arc deployment: one container, one named volume, port `:8000` exposed. No clustering, no license key, no reverse proxy.

## Architecture

```
┌──────────────────────────┐
│          Arc             │
│     (port 8000)          │
│                          │
│  ┌────────────────────┐  │
│  │  /app/data         │  │
│  │    ├── arc/        │  │
│  │    │   (Parquet)   │  │
│  │    └── wal/        │  │
│  └────────────────────┘  │
└──────────────────────────┘
```

## Usage

```bash
# Optional: pre-set the admin token (otherwise read it from the logs)
export ARC_AUTH_BOOTSTRAP_TOKEN=$(openssl rand -hex 32)

docker compose up -d

# Retrieve the admin token (if you didn't pre-set it)
docker compose logs arc | grep -i "initial admin token"

# Write data (line-protocol)
curl -X POST "http://localhost:8000/write?db=mydb" \
  -H "Authorization: Bearer $ARC_AUTH_BOOTSTRAP_TOKEN" \
  -d 'cpu,host=server01 usage=42.5'

# Query
curl -X POST http://localhost:8000/api/v1/query \
  -H "Authorization: Bearer $ARC_AUTH_BOOTSTRAP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'
```

## When to use this example

- Local development and evaluation
- Single-machine deployments (embedded devices, edge gateways)
- Small workloads where one host is enough

For S3-backed storage see [`../oss-s3/`](../oss-s3/). For a Traefik-fronted version see [`../oss-traefik/`](../oss-traefik/). For multi-node clustering see the Enterprise examples.
