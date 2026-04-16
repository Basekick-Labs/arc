# Arc — Docker Compose Examples

Ready-to-run Docker Compose setups for every common Arc topology. Pick the folder that matches your deployment shape.

## Examples

| Folder | Edition | Storage | Proxy | Use case |
|--------|---------|---------|-------|----------|
| [`oss-local/`](./oss-local/) | OSS | Local disk | none | Local dev, evaluation, single-host small workloads |
| [`oss-s3/`](./oss-s3/) | OSS | S3 (bundled MinIO) | none | Object-storage durability on a single host; testing S3 code path |
| [`oss-traefik/`](./oss-traefik/) | OSS | Local disk | Traefik v3.6 | Single host with TLS / access logs / shared reverse proxy |
| [`enterprise-shared/`](./enterprise-shared/) | Enterprise | Shared S3 (MinIO) | Traefik v3.6 | HA cluster sharing one bucket; cloud-native deployments |
| [`enterprise-local/`](./enterprise-local/) | Enterprise | Local disk + peer replication | Traefik v3.6 | HA cluster on bare metal / edge / air-gapped (no shared storage) |

## Quick start

```bash
cd deploy/docker-compose/<example>
docker compose up -d
```

Each folder has its own README with configuration details, environment variables, and sizing notes.

## OSS vs Enterprise

- **OSS** — single-node Arc. Simple to run, no license key. Full ingest + query functionality, no clustering / failover / RBAC / backup / auto-aggregation.
- **Enterprise** — multi-node clustering with role separation (writer / reader / compactor), automatic failover, TLS + shared-secret authentication, tiered storage, RBAC, and more. Requires a license key — request one at `enterprise@basekick.net`.

See the [OSS vs Enterprise comparison](https://docs.basekick.net/arc-enterprise/overview) for the full list, and [Deployment Patterns](https://docs.basekick.net/arc-enterprise/deployment-patterns) for the rationale behind the two Enterprise topologies.
