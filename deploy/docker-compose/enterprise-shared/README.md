# Arc Cluster — Docker Compose (Shared Storage / Pattern 2 multi-writer)

A 3-writer + 1-reader Arc cluster with **Traefik v3.6** as the reverse proxy and **MinIO** for S3-compatible shared storage. Traefik uses the Docker provider — routing is defined per container via labels, so the setup load-balances automatically as you add or remove writers/readers.

This compose runs in **Pattern 2 multi-writer mode** (`ARC_CLUSTER_SHARED_STORAGE_MODE=true`): all three writers accept writes concurrently and PUT to the same MinIO bucket. There is no "primary" writer to elect or fail over to — the load balancer (Traefik) picks any healthy writer for each request, and a writer crash is recovered by the next request landing on a surviving writer.

## Architecture

```
                         ┌─────────────────────────────────────┐
                         │             Traefik v3.6            │
                         │      (entrypoint :8000, dash :8080) │
                         │   round-robins writes across all    │
                         │   healthy writer pods (via /ready)  │
                         └───────────────────┬─────────────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
               writes (any)            writes (any)             queries only
                    │                        │                        │
                    ▼                        ▼                        ▼
         ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
         │     Writer 1     │    │     Writer 2     │    │     Reader 1     │
         │   (all writers   │    │   are equal —    │    │   (Query Only)   │
         │     are peer)    │    │   no primary)    │    │                  │
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
         │  (all writers     │            │                       │
         │    are peer)      │            │                       │
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
                  │  All writers PUT      │
                  │  concurrently; all    │
                  │  nodes read from the  │
                  │  same bucket          │
                  └───────────────────────┘
```

## How It Works

### Node Roles

| Role | Ingestion | Query | Raft Consensus | Use Case |
|------|-----------|-------|----------------|----------|
| **Writer** | ✓ (all writers accept) | ✓ | ✓ | Accepts writes; participates in leader election for singleton tasks |
| **Reader** | ✗ | ✓ | ✗ | Query-only, horizontal read scaling |

### Request Flow

#### Writes (Ingestion)

1. Client sends write request to Traefik (port 8000)
2. Traefik picks any healthy writer (round-robin across writers 1/2/3)
3. **The receiving writer handles the request directly** — no forwarding, no "leader" gate; every writer is equivalent for ingestion in Pattern 2
4. Writer buffers in its local Arrow buffer, then flushes Parquet files to MinIO (S3)
5. Manifest entry registered via Raft (cluster-wide visibility on next snapshot/query)

```
Client → Traefik → Writer{1,2,3} → MinIO (directly; no forwarding)
```

Each writer's flushed Parquet files land in the same MinIO bucket under unique filenames (per-process nanosecond suffix, so concurrent flushes from different writers in the same hour partition cannot collide).

#### Queries

1. Client sends query to Traefik (port 8000)
2. Traefik routes `/api/v1/query` to readers
3. Reader queries data directly from MinIO (S3)
4. Results returned to client

```
Client → Traefik → Reader1 → MinIO → Reader1 → Client
```

**Note:** Writers can also serve queries. Traefik routes queries to readers to offload traffic from writers, but if a query hits a writer it will serve it directly — no forwarding needed since all nodes read from the same shared storage.

### Failover

Pattern 2 multi-writer **delegates failover to the load balancer** rather than running an in-cluster promotion machinery. There is no "primary writer" to fail over to — all writers are peers.

When a writer crashes:
1. The writer pod's `/ready` endpoint stops responding (or returns 503).
2. Traefik's docker-provider health check marks the writer unhealthy and stops routing to it within one poll cycle (~5-10 s).
3. New writes route to the surviving writer(s). **No in-cluster failover action required.**
4. In-flight buffer on the crashed writer — i.e. records that arrived in memory but had not yet been flushed to MinIO — is lost. Records that completed the MinIO PUT before the crash are durable.
5. On writer restart, the local WAL replays any un-flushed entries into the new Arrow buffer before `/ready` flips back to 200 and Traefik resumes routing.

Singleton background tasks (retention, continuous queries, deletes) gate on the cluster Raft leader — if the leader writer crashes, Raft elects a new leader within ~1 s and those tasks resume on the new leader's next scheduler tick.

### Why 3 Writers?

Three writers in Pattern 2 multi-writer mode buy you **two things at once**:

1. **HA via the LB**: Traefik routes around a crashed writer with no in-cluster ceremony. Even with just 2 writers Pattern 2 would be HA on the ingestion side.
2. **Raft quorum tolerance** for the cluster-wide state (RBAC, tokens, manifest, shard assignments). Singleton tasks (retention, CQ, delete) all run on the Raft leader, so we want Raft to elect a leader even on one writer failure.

| Writers | Quorum | Can Tolerate (Raft side) |
|---------|--------|--------------------------|
| 1 | 1 | 0 failures (no HA) |
| 2 | 2 | 0 failures (single failure = no quorum → no leader → singleton tasks pause) |
| **3** | **2** | **1 failure** |
| 5 | 3 | 2 failures |

With 3 writers, the cluster can survive 1 writer failure on both the ingestion side (LB routes around it) AND the singleton-task side (Raft still has quorum).

`writer.replicas=2` is **not recommended** — ingestion stays HA via the LB but Raft cannot elect a leader on a single failure, so singleton tasks (retention, CQ, delete) pause until quorum is restored.

### Data Flow: shared storage + Raft state

Arc's Pattern 2 multi-writer uses two independent data planes:

1. **Data (Parquet) → shared S3**
   - Each writer flushes Parquet files to its own per-process filenames in the same MinIO bucket.
   - All nodes (writers + readers) query directly from the bucket — no replication needed.
   - WAL is local-only and used solely for crash recovery (replay un-flushed records on writer restart). Not replicated peer-to-peer.

2. **Cluster state (manifest, tokens, RBAC, shard map) → cluster Raft**
   - Already replicated cluster-wide via Raft (Phase A token replication and Phase A.1 RBAC replication).
   - Every writer sees the same auth/manifest state.

**Benefits of Pattern 2 shared storage:**
- **Stateless compute** — writer pods can be replaced without data migration; the bucket is the durable layer.
- **Horizontal write scaling** — add more writers behind the LB to scale write throughput linearly (bounded by MinIO bandwidth).
- **Horizontal read scaling** — add more readers; they share the bucket too.
- **Trivial failover** — LB retry is the failover mechanism; no in-cluster promotion daemon.

## Ports

| Service | Port | Purpose |
|---------|------|---------|
| Traefik | 8000 | Client API (load balanced across writers/readers) |
| Traefik dashboard | 8080 | Web UI (insecure; disable in production) |
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
export ARC_CLUSTER_SHARED_SECRET=$(openssl rand -hex 32)
docker compose up -d

# Check cluster status
curl http://localhost:8000/api/v1/cluster

# Write data (Traefik LB round-robins across writers)
curl -X POST "http://localhost:8000/write?db=mydb" \
  -d 'cpu,host=server01 usage=42.5'

# Query data
curl -X POST "http://localhost:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'
```

## Smoke Testing

`smoke.sh` exercises the full Pattern 2 multi-writer flow end-to-end. Three scenarios share the same harness:

| Scenario | What it tests |
|---|---|
| `base` (default) | Ingest N records via Traefik LB, query through reader, assert exact record count. Verifies LB round-robin, writer concurrency, MinIO durability, reader-can-see-writer-flushes. |
| `non-leader-crash` | Same as base, but `docker kill` a non-leader writer mid-ingest. Asserts: every HTTP-success record is queryable (durability invariant). In-flight buffer on the killed writer may be lost. |
| `leader-crash` | Same as base, but kill the current Raft leader mid-ingest. Asserts: Raft elects a new leader within seconds; writes continue via LB to surviving writers; durability invariant holds. |

The smoke uses `docker-compose.override.smoke.yml` to build Arc **from source** rather than pulling `basekick/arc:latest`, so you can run it against branch under review.

```bash
export ARC_LICENSE_KEY="ARC-ENT-..."           # dev key (NOT the customer .env key)
export ARC_CLUSTER_SHARED_SECRET=$(openssl rand -hex 32)

./smoke.sh                                     # base scenario, 1000 records
RECORDS=100000 ./smoke.sh                      # larger run
SCENARIO=non-leader-crash ./smoke.sh           # crash variant
SCENARIO=leader-crash ./smoke.sh               # leader crash variant
```

On failure, the smoke leaves containers running for debugging. Re-running cleans up.

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
