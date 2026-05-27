# Arc Enterprise Helm Chart

Multi-node Arc Enterprise deployment for Kubernetes with role separation (writer / reader / compactor), high availability, and a choice of storage pattern (shared object storage with Pattern 2 multi-writer, or per-node local storage with Pattern 1 single-writer + writer-failover).

## Prerequisites

- Kubernetes 1.24+
- Helm 3.10+
- An Arc Enterprise license key (request at enterprise@basekick.net)
- A cluster shared secret (any strong random string)

## Quick Start

### Pattern A — Shared Object Storage (bundled MinIO)

```bash
helm install arc-ent helm/arc-enterprise \
  -f helm/arc-enterprise/values-shared-storage.yaml \
  --set license.key=ARC-ENT-XXXX-XXXX-XXXX-XXXX \
  --set cluster.sharedSecret.value=$(openssl rand -hex 32) \
  --set minio.credentials.rootUser=arcminio \
  --set minio.credentials.rootPassword=$(openssl rand -hex 32) \
  --namespace arc --create-namespace
```

This deploys:
- 1 MinIO pod (bundled S3-compatible storage)
- 1 writer, 2 readers, 1 compactor
- Each Arc pod points at the MinIO bucket as shared storage

The chart refuses to install if any of the required credentials are missing
— there are no weak defaults.

### Pattern B — Local Storage with Peer Replication

```bash
helm install arc-ent helm/arc-enterprise \
  -f helm/arc-enterprise/values-local-storage.yaml \
  --set license.key=ARC-ENT-XXXX-XXXX-XXXX-XXXX \
  --set cluster.sharedSecret.value=$(openssl rand -hex 32) \
  --namespace arc --create-namespace
```

This deploys:
- No MinIO
- 1 writer, 2 readers, 1 compactor, each with its own PersistentVolume
- Peer-to-peer file replication keeps nodes in sync

## Choosing a Pattern

| Use case | Recommended pattern |
|----------|--------------------|
| Cloud-native deployment on managed Kubernetes (EKS/GKE/AKS) | Shared |
| Already running S3, MinIO, or Azure Blob in production | Shared |
| Bare metal or on-prem Kubernetes | Local |
| Air-gapped / edge / defense deployments | Local |
| Need lowest possible query latency (local NVMe) | Local |

See [Deployment Patterns](https://docs.basekick.net/arc-enterprise/deployment-patterns) for the full comparison.

## Common Configuration

### External S3 (AWS / Azure / Tigris / R2)

```yaml
# values-override.yaml
storage:
  mode: shared
  shared:
    external: true
    bucket: my-arc-bucket
    region: us-east-1
    endpoint: https://s3.us-east-1.amazonaws.com
    useSSL: true
    usePathStyle: false
    credentials:
      existingSecret: arc-s3-credentials   # must contain access-key, secret-key

minio:
  enabled: false                           # don't deploy bundled MinIO
```

### Authentication (admin token + cluster replication)

```yaml
# values-override.yaml
auth:
  bootstrapToken:
    existingSecret: arc-admin-token          # must contain key "bootstrap-token", 32+ chars
```

Or let Arc generate the admin token at first boot and read it from logs:

```bash
# Exactly one pod (the Raft leader) prints the banner. Search all writers:
kubectl -n arc logs -l app.kubernetes.io/component=writer --prefix=true --tail=-1 | \
  grep -B1 -A2 'Admin API token:'
```

Since Arc Enterprise v26.06.1, tokens (creation, update, revocation, deletion,
rotation) replicate cluster-wide automatically via Raft — a token created on
any node is valid on every node, and revoking on any node invalidates it
cluster-wide within ~50 ms. RBAC tables (organisations, teams, roles,
measurement permissions, token memberships) follow the same pattern in
v26.07.1. See the [Authentication docs](https://docs.basekick.net/docs/configuration/authentication#cluster-auth-replication-enterprise)
for the full semantics, including:

- The leader-only bootstrap banner behaviour (relevant for first install + log scraping).
- Pre-26.06.1 tokens do **not** auto-migrate — re-issue them via the API after upgrade.
- Divergence-detection error log and remediation if a pre-existing local
  AUTOINCREMENT row collides with a new cluster-stamped ID.

### Cluster TLS

```yaml
cluster:
  tls:
    enabled: true
    existingSecret: arc-cluster-tls        # must contain tls.crt, tls.key
```

Create the TLS secret first:

```bash
kubectl create secret tls arc-cluster-tls --cert=server.crt --key=server.key -n arc
```

### High Availability

Arc Enterprise offers two HA models depending on which storage pattern you chose.

#### Shared storage (Pattern 2 multi-writer) — `storage.mode: shared`

All writer pods accept writes concurrently behind a Kubernetes Service load
balancer. There is no "primary writer" to fail over to — every writer is a
peer. A writer crash is recovered by the Service stopping routing to the
unready pod (via the `/ready` probe) within one poll cycle.

```yaml
writer:
  replicas: 3                              # see "Why 3?" below
  # cluster.failover.enabled is NOT required in shared-storage mode;
  # failover is delegated to the Kubernetes Service + /ready probe.
```

In shared-storage mode, `cluster.shared_storage_mode=true` is set
automatically by the chart (see `templates/_helpers.tpl`). This:

- Suppresses Arc's in-cluster writer-failover health-check loop (the
  Kubernetes Service does the work).
- Switches singleton background tasks (retention, continuous queries,
  deletes) to gate on the cluster Raft leader instead of on a singleton
  "primary writer." Leader change is sub-second on healthy clusters.
- Requires the Enterprise license to include the
  `shared_storage_multi_writer` feature; startup refuses otherwise.

#### Local storage (Pattern 1) — `storage.mode: local`

Each writer pod owns its own PersistentVolume. In current Arc Enterprise
v26.06.x this pattern still runs in **single-writer + multi-reader** mode:
one writer takes ingest, readers replicate the WAL and can be promoted on
writer failure via Arc's writer-failover controller. (Pattern 1 multi-writer
— per-shard writer ownership — is a future initiative; see the multi-writer
plan doc.)

```yaml
writer:
  replicas: 1                              # one active writer
reader:
  replicas: 3                              # readers double as failover pool
cluster:
  failover:
    enabled: true                          # automatic writer + compactor failover
```

#### Why 3 writers? (shared-storage Pattern 2)

Three writers buy you **two** HA properties at once:

1. **LB-side HA**: a writer crash is routed around by the Service; with 3
   writers you tolerate one failure on the ingestion side and still have
   2 healthy writers serving traffic.
2. **Raft quorum**: cluster-wide state (manifest, tokens, RBAC) and
   singleton-task ownership (retention/CQ/delete) gate on the cluster Raft
   leader. With 3 writers Raft tolerates 1 failure and still elects a leader.

| writer.replicas | Quorum | Ingest HA | Raft HA |
|-----------------|--------|-----------|---------|
| 1 | 1 | none | none |
| 2 | 2 | yes (LB) | **no** (no failure tolerance — chart rejects this) |
| **3** | **2** | **yes** | **yes (tolerates 1 failure)** |
| 5 | 3 | yes | yes (tolerates 2 failures) |

The chart rejects `writer.replicas=2` (see `templates/_validation.tpl`)
because a Raft quorum of 2 requires both pods to be reachable — a single
failure leaves the cluster without a leader, so singleton tasks pause.

Only the pod with ordinal `-0` bootstraps Raft on first install; the other
writers join via the seed list.

## Operations

### Check cluster status

```bash
kubectl -n arc port-forward svc/arc-ent-arc-enterprise-reader 8000:8000
curl http://localhost:8000/api/v1/cluster | jq
```

### Scale readers

```bash
helm upgrade arc-ent helm/arc-enterprise --reuse-values --set reader.replicas=4
```

### Upgrade Arc version

```bash
helm upgrade arc-ent helm/arc-enterprise --reuse-values --set image.tag=26.05.1
```

## Uninstall

```bash
helm uninstall arc-ent -n arc
kubectl delete pvc -l app.kubernetes.io/instance=arc-ent -n arc   # removes data
```

## Full Values Reference

See [values.yaml](./values.yaml) for every configurable option with inline documentation.
