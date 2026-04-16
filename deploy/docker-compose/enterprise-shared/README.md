# Arc Cluster — Docker Compose (Shared Storage)

A 3-writer + 1-reader Arc cluster with **Traefik v3.6** as the reverse proxy and **MinIO** for S3-compatible shared storage. Traefik uses the Docker provider — routing is defined per container via labels, so the setup load-balances automatically as you add or remove writers/readers.

## Architecture

```
                         ┌─────────────────────────────────────┐
                         │             Traefik v3.6            │
                         │      (entrypoint :8000, dash :8080) │
                         └───────────────────┬─────────────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
               writes only             writes only              queries only
                    │                        │                        │
                    ▼                        ▼                        ▼
         ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
         │     Writer 1     │    │     Writer 2     │    │     Reader 1     │
         │   (Raft Leader)  │    │   (Raft Voter)   │    │   (Query Only)   │
         │                  │    │                  │    │                  │
         │  • Ingestion ✓   │    │  • Ingestion ✓   │    │  • Ingestion ✗   │
         │  • Query ✓       │    │  • Query ✓       │    │  • Query ✓       │
         │  • Raft ✓        │    │  • Raft ✓        │    │  • Raft ✗        │
         └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
                  │                       │                       │
                  │    ┌──────────────────┤                       │
                  │    │                  │                       │
                  │    ▼                  │                       │
         ┌────────────────────┐           │                       │
         │     Writer 3      │            │                       │
         │   (Raft Voter)    │            │                       │
         │                   │            │                       │
         │  • Ingestion ✓    │            │                       │
         │  • Query ✓        │            │                       │
         │  • Raft ✓         │            │                       │
         └────────┬──────────┘            │                       │
                  │                       │                       │
                  └───────────┬───────────┴───────────────────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │    MinIO (S3)         │
                  │   Shared Storage      │
                  │                       │
                  │  All nodes read/write │
                  │  to the same bucket   │
                  └───────────────────────┘
```

## How It Works

### Node Roles

| Role | Ingestion | Query | Raft Consensus | Use Case |
|------|-----------|-------|----------------|----------|
| **Writer** | ✓ | ✓ | ✓ | Accepts writes, participates in leader election |
| **Reader** | ✗ | ✓ | ✗ | Query-only, horizontal read scaling |

### Request Flow

#### Writes (Ingestion)

1. Client sends write request to Traefik (port 8000)
2. Traefik routes write paths (see routing table below) across the writers
3. If the receiving writer is NOT the Raft leader → Arc forwards to the leader
4. Raft leader commits to WAL and replicates to followers
5. Data is written to MinIO (S3)

```
Client → Traefik → Writer2 → (forward) → Writer1 (Leader) → MinIO
```

#### Queries

1. Client sends query to Traefik (port 8000)
2. Traefik routes `/api/v1/query` to readers
3. Reader queries data directly from MinIO (S3)
4. Results returned to client

```
Client → Traefik → Reader1 → MinIO → Reader1 → Client
```

**Note:** Writers can also serve queries. Traefik routes queries to readers to offload traffic from writers, but if a query hits a writer, it will serve it directly — no forwarding needed since all nodes read from the same shared storage.

### Why 3 Writers?

Raft consensus requires a quorum (majority) to elect a leader:

| Writers | Quorum | Can Tolerate |
|---------|--------|--------------|
| 1 | 1 | 0 failures |
| 2 | 2 | 0 failures (split-brain risk) |
| **3** | **2** | **1 failure** |
| 5 | 3 | 2 failures |

With 3 writers, the cluster can survive 1 writer failure and still elect a leader.

### Data Flow: WAL Replication + Shared Storage

Arc uses a two-tier data flow:

1. **WAL Replication (recent data)**
   - Writers replicate WAL entries to readers in real-time
   - Enables low-latency queries on just-ingested data (seconds-old)
   - Configured via `ARC_CLUSTER_REPLICATION_ENABLED=true` on readers

2. **Shared S3 Storage (persisted data)**
   - Writers flush data to Parquet files in S3 (MinIO)
   - All nodes query persisted data directly from S3
   - Data is durable once flushed to S3

**Query consistency:**

| Data Age | Source | Requires WAL Replication? |
|----------|--------|---------------------------|
| < flush interval | Writer's buffer + WAL | Yes (for readers) |
| > flush interval | S3 Parquet files | No |

**Benefits of shared storage:**
- **Stateless compute** - nodes can be replaced without data migration
- **Horizontal read scaling** - add readers without copying data
- **Single source of truth** - S3 is the durable storage layer

## Ports

| Service | Port | Purpose |
|---------|------|---------|
| Nginx | 8000 | Client API (load balanced) |
| MinIO API | 9000 | S3 API |
| MinIO Console | 9001 | Web UI |

Internal ports (not exposed):
- 8000: Arc HTTP API
- 9100: Cluster coordinator (node-to-node)
- 9200: Raft consensus

## Usage

```bash
# Start the cluster
export ARC_LICENSE_KEY="your-enterprise-license-key"
docker-compose up -d

# Check cluster status
curl http://localhost:8000/api/v1/cluster/status

# Write data
curl -X POST "http://localhost:8000/write?db=mydb" \
  -d 'cpu,host=server01 usage=42.5'

# Query data
curl -X POST "http://localhost:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'
```

## Scaling

### Add More Readers

Edit `docker-compose.yml` and add a reader block — no Traefik config change needed, the Docker provider picks it up automatically from the labels inherited via the YAML anchor:

```yaml
arc-reader2:
  <<: *arc-common
  container_name: arc-reader2
  hostname: arc-reader2
  volumes:
    - arc-reader2-data:/app/data
  environment:
    <<: *arc-env
    ARC_CLUSTER_NODE_ID: "arc-reader2"
    ARC_CLUSTER_ROLE: "reader"
    ARC_CLUSTER_ADVERTISE_ADDR: "arc-reader2:9100"
    ARC_CLUSTER_REPLICATION_ENABLED: "true"
  labels:
    <<: *traefik-reader-labels
  depends_on:
    - arc-writer1
```

Run `docker compose up -d arc-reader2` and the new reader appears in Traefik's `arc-readers` service backend pool immediately.

## Production Considerations

1. **Use external S3** — replace MinIO with AWS S3 or other managed object storage
2. **External load balancer** — terminate TLS upstream (AWS ALB, GCP LB, Cloudflare) and forward to Traefik
3. **Disable the Traefik dashboard** — remove `--api.insecure=true` and the `:8080` port mapping in production
4. **Persistent storage** — ensure volumes are backed up
5. **Monitoring** — add Prometheus metrics scraping (Arc exposes `/metrics`; Traefik exposes its own at `:8082` if enabled)
