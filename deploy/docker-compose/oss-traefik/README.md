# Arc OSS — Single Node + Traefik

Arc OSS behind **Traefik v3.6** using the Docker provider. Single Arc container, local storage, Traefik on `:8000` as the entry point and `:8080` for the dashboard. Handy when you want TLS termination, request logs, or a shared proxy for other services on the same host — without the Enterprise cluster setup.

## Architecture

```
┌───────────────────────────────────────┐
│              Traefik v3.6             │
│        (entry :8000, dash :8080)      │
└───────────────────┬───────────────────┘
                    │
                    ▼
           ┌──────────────────┐
           │        Arc        │
           │  (Docker-network) │
           └──────────────────┘
```

## Usage

```bash
export ARC_AUTH_BOOTSTRAP_TOKEN=$(openssl rand -hex 32)
docker compose up -d

# Write and query through Traefik
curl -X POST "http://localhost:8000/write?db=mydb" \
  -H "Authorization: Bearer $ARC_AUTH_BOOTSTRAP_TOKEN" \
  -d 'cpu,host=server01 usage=42.5'

curl -X POST http://localhost:8000/api/v1/query \
  -H "Authorization: Bearer $ARC_AUTH_BOOTSTRAP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'

# Traefik dashboard
open http://localhost:8080
```

## Adding TLS

Traefik's `tls` configuration plugs in via labels or a `cert-resolver` command-line flag. A common pattern with Let's Encrypt:

```yaml
    command:
      - --entrypoints.websecure.address=:443
      - --certificatesresolvers.le.acme.email=you@example.com
      - --certificatesresolvers.le.acme.storage=/letsencrypt/acme.json
      - --certificatesresolvers.le.acme.tlschallenge=true
    labels:
      traefik.http.routers.arc.entrypoints: "websecure"
      traefik.http.routers.arc.tls.certresolver: "le"
```

## Production considerations

- Remove `--api.insecure=true` and the `:8080` port mapping before exposing Traefik publicly
- Put TLS in front (see above) or terminate TLS at an upstream load balancer
- Bind `:8000` to `127.0.0.1` and front with your existing edge LB if you don't want Traefik to be the public face

## When to use this example

- Single-host deployments that need TLS, path-based routing, or access logs
- Shared hosts where Arc runs alongside other services behind one Traefik

For a minimal Arc with no proxy see [`../oss-local/`](../oss-local/). For S3 storage see [`../oss-s3/`](../oss-s3/). For multi-node clustering with failover see the Enterprise examples.
