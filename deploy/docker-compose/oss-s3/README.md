# Arc OSS — Single Node, S3-Backed (bundled MinIO)

One Arc container backed by a bundled MinIO for S3-compatible storage. Still a single Arc node — just with object storage instead of a local disk. Swap MinIO for AWS S3 / R2 / Tigris / Azure by removing the `minio` service and pointing at your own endpoint.

## Architecture

```
┌──────────────┐      ┌──────────────┐
│     Arc      │─────▶│    MinIO     │
│  (port 8000) │      │  (port 9000) │
└──────────────┘      └──────────────┘
```

## Usage

```bash
# Optional: override the MinIO root credentials + pre-set the admin token
export MINIO_ROOT_USER=arcminio
export MINIO_ROOT_PASSWORD=$(openssl rand -hex 32)
export ARC_AUTH_BOOTSTRAP_TOKEN=$(openssl rand -hex 32)

docker compose up -d

# Write data
curl -X POST "http://localhost:8000/write?db=mydb" \
  -H "Authorization: Bearer $ARC_AUTH_BOOTSTRAP_TOKEN" \
  -d 'cpu,host=server01 usage=42.5'

# Query
curl -X POST http://localhost:8000/api/v1/query \
  -H "Authorization: Bearer $ARC_AUTH_BOOTSTRAP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'

# MinIO console — browse the bucket contents
open http://localhost:9001
```

## Pointing at external S3 / R2 / Tigris / Azure

1. Remove the `minio` service and the `depends_on.minio` block
2. Replace the `ARC_STORAGE_S3_*` values with your credentials and endpoint:

```yaml
      ARC_STORAGE_S3_ENDPOINT: "https://s3.us-east-1.amazonaws.com"
      ARC_STORAGE_S3_FORCE_PATH_STYLE: "false"
      ARC_STORAGE_S3_USE_SSL: "true"
      ARC_STORAGE_S3_ACCESS_KEY: "${AWS_ACCESS_KEY_ID}"
      ARC_STORAGE_S3_SECRET_KEY: "${AWS_SECRET_ACCESS_KEY}"
```

## When to use this example

- Testing the S3 code path locally before going to production
- Single-host deployments where you want object-storage durability (snapshots, bucket versioning) instead of a local volume
- Stepping stone to Enterprise clustered deployments, which share a single bucket across nodes (see [`../enterprise-shared/`](../enterprise-shared/))

For a local-disk variant see [`../oss-local/`](../oss-local/); for a Traefik-fronted OSS setup see [`../oss-traefik/`](../oss-traefik/).
