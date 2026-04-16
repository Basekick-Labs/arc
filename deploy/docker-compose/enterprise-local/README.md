# Arc Cluster — Local Storage with Peer Replication

This deployment uses **local filesystem storage** with peer-to-peer file replication between nodes. No shared S3 required. **Traefik v3.6** acts as the reverse proxy via the Docker provider.

## Architecture

```
                         ┌─────────────────────────────────────┐
                         │             Traefik v3.6            │
                         │      (entrypoint :8000, dash :8080) │
                         └───────────────────┬─────────────────┘
                                             │
         ┌───────────────────────────────────┼───────────────────────────────────┐
         │                                   │                                   │
    writes only                         writes only                        queries only
         │                                   │                                   │
         ▼                                   ▼                                   ▼
┌─────────────────┐                ┌─────────────────┐                ┌─────────────────┐
│    Writer 1     │                │    Writer 2     │                │    Reader 1     │
│  (Raft Leader)  │                │  (Raft Voter)   │                │  (Query Only)   │
│                 │                │                 │                │                 │
│ ┌─────────────┐ │   Raft WAL     │ ┌─────────────┐ │   WAL Repl.   │ ┌─────────────┐ │
│ │ Local Vol.  │ │◄──────────────►│ │ Local Vol.  │ │──────────────►│ │ Local Vol.  │ │
│ │ /app/data   │ │                │ │ /app/data   │ │               │ │ /app/data   │ │
│ └─────────────┘ │                │ └─────────────┘ │               │ └─────────────┘ │
└─────────────────┘                └─────────────────┘               └─────────────────┘
         │                                   │
         │              Raft WAL             │
         │◄─────────────────────────────────►│
         │                                   │
         ▼                                   ▼
┌─────────────────┐
│    Writer 3     │
│  (Raft Voter)   │
│                 │
│ ┌─────────────┐ │
│ │ Local Vol.  │ │
│ │ /app/data   │ │
│ └─────────────┘ │
└─────────────────┘
```

## Local vs Shared Backend

| Aspect | Local (this folder) | Shared ([../docker-compose/](../docker-compose/)) |
|--------|---------------------|---------------------------------------------------|
| **Storage** | Each node has its own volume | All nodes share one bucket |
| **Data access** | Via peer-to-peer file replication | Direct from shared bucket |
| **Replication** | **Required** — files fetched with SHA-256 verification | Not required (bucket is shared) |
| **Scaling readers** | Each reader keeps a full replica | Just add nodes (no data copy) |
| **Data durability** | Depends on volume backup | Bucket handles durability |
| **Use case** | Bare-metal, on-prem, edge, air-gapped | Cloud, managed Kubernetes |

## Data Flow

### With Local Backend

```
Write Request
     │
     ▼
┌─────────────┐     Raft Log      ┌─────────────┐
│   Writer 1  │◄─────────────────►│   Writer 2  │
│   (Leader)  │                   │  (Follower) │
└──────┬──────┘                   └─────────────┘
       │
       │ Raft announces new file (manifest)
       │ Peers pull bytes with SHA-256 verification
       ▼
┌─────────────┐
│   Reader 1  │
└─────────────┘
```

1. Write lands on leader (Writer 1)
2. Leader commits to Raft and flushes a Parquet file to its local volume
3. Raft announces the file in the cluster manifest
4. Followers and readers pull the bytes from a peer, verifying SHA-256
5. Each node ends up with its **own full replica** on local disk

### Query Path

```
Query Request
     │
     ▼
┌─────────────┐
│   Reader 1  │ ──► Reads from /app/data (local replica)
└─────────────┘     Bytes fetched peer-to-peer from writers
```

**Important:** Readers can only query data they've already pulled. Set `ARC_CLUSTER_REPLICATION_ENABLED=true` on every node (already set in this compose file).

## When to Use Local Backend

**Good for:**
- Development and testing
- Edge deployments (no cloud access)
- Single-machine setups
- Low-latency requirements (no network to S3)

**Not ideal for:**
- Large-scale production (volume management is harder)
- Multi-region deployments
- When you need instant reader scaling

## Volume Contents

Each node's `/app/data` volume contains:

```
/app/data/
├── raft/           # Raft consensus state (writers only)
│   ├── logs/       # Raft log entries
│   └── snapshots/  # Raft snapshots
├── wal/            # Write-ahead log
├── storage/        # Parquet data files
│   └── {database}/
│       └── {measurement}/
│           └── YYYY/MM/DD/HH/*.parquet
└── auth.db         # SQLite (auth, audit, etc.)
```

## Usage

```bash
# Start the cluster
export ARC_LICENSE_KEY="your-enterprise-license-key"
docker compose up -d

# Check cluster status
curl http://localhost:8000/api/v1/cluster/status

# Write data (goes to leader, replicated to followers, peer-replicated to reader)
curl -X POST "http://localhost:8000/write?db=mydb" \
  -d 'cpu,host=server01 usage=42.5'

# Query (reader serves from its local replica)
curl -X POST "http://localhost:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'

# Traefik dashboard (live routers and backends)
open http://localhost:8080
```

## Ports

| Service | Port | Purpose |
|---------|------|---------|
| Traefik | 8000 | Client API (load balanced) |
| Traefik | 8080 | Traefik dashboard (disable in production) |

Internal ports (not exposed):
- 8000: Arc HTTP API
- 9100: Cluster coordinator (node-to-node)
- 9200: Raft consensus

## Comparison with Shared-Storage Pattern

See [../docker-compose/](../docker-compose/) for the shared-storage version with MinIO.

| Folder | Backend | Shared Storage |
|--------|---------|----------------|
| `docker-compose/` | S3 (MinIO) | Yes — all nodes read from the same bucket |
| `docker-compose-local/` | Local filesystem | No — each node holds a replica, files are fetched peer-to-peer |
