# Arc Enterprise - Product Definition

**Date**: January 8, 2026
**Status**: Planning
**Authors**: Product & Engineering Team

---

## Executive Summary

Arc Enterprise extends the high-performance Arc OSS time-series database with enterprise-grade features for production deployments at scale. This document defines the feature set, differentiation strategy, and licensing model.

---

## 1. Feature Matrix

### 1.1 Clustering & High Availability

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Single-node deployment | ✅ | ✅ |
| **Clustering (3+ nodes)** | ❌ | ✅ |
| **Automatic failover** | ❌ | ✅ |
| **Split architecture (Writer/Reader/Compactor roles)** | ❌ | ✅ |
| **Cross-datacenter replication** | ❌ | ✅ |
| Edge-to-cloud sync | ✅ (manual) | ✅ (automatic) |

**Enterprise Implementation Details:**

```
┌─────────────────────────────────────────────────────────────────┐
│                     Arc Enterprise Cluster                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Writer 1   │    │   Writer 2   │    │   Writer 3   │       │
│  │  (Primary)   │◄──►│  (Standby)   │◄──►│  (Standby)   │       │
│  └──────┬───────┘    └──────────────┘    └──────────────┘       │
│         │                                                        │
│         │ WAL Replication                                        │
│         ▼                                                        │
│  ┌──────────────────────────────────────────────────────┐       │
│  │              Shared Storage Layer                     │       │
│  │         (S3 / Azure Blob / MinIO / NFS)              │       │
│  └──────────────────────────────────────────────────────┘       │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Reader 1   │    │   Reader 2   │    │   Reader N   │       │
│  │  (Query)     │    │  (Query)     │    │  (Query)     │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐                           │
│  │ Compactor 1  │    │ Compactor 2  │    (Background workers)   │
│  └──────────────┘    └──────────────┘                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Capabilities:**
- **Consensus**: Raft-based leader election for writers
- **Failover**: Automatic promotion of standby writer (< 30 second RTO)
- **Read scaling**: Horizontal read replica scaling
- **Role separation**: Isolate write, read, and compaction workloads

---

### 1.2 Role-Based Access Control (RBAC)

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Token-based auth | ✅ | ✅ |
| Token permissions (read/write/delete/admin) | ✅ | ✅ |
| **Organizations & Teams** | ❌ | ✅ |
| **Granular database permissions** | ❌ | ✅ |
| **Measurement-level permissions** | ❌ | ✅ |
| **Row-level security (tag-based)** | ❌ | ✅ |
| **LDAP/SAML/OIDC integration** | ❌ | ✅ |
| **Audit logging** | ❌ | ✅ (implemented) |

**Enterprise Permission Model:**

```yaml
# Example RBAC Configuration
organizations:
  - name: "Acme Corp"
    teams:
      - name: "Data Engineering"
        roles:
          - database: "production"
            permissions: [read, write, delete]
          - database: "analytics"
            permissions: [read, write]

      - name: "Analytics"
        roles:
          - database: "production"
            permissions: [read]
            measurements: ["metrics_*", "events_*"]
          - database: "analytics"
            permissions: [read, write]

      - name: "Operations"
        roles:
          - database: "*"
            permissions: [read]
            tag_filter: "env = 'production'"  # Row-level security
```

**Audit Log Events (implemented):**
- Authentication attempts (success/failure) — `auth.failed` (401/403 responses)
- Token management — `token.created`, `token.deleted`, `token.rotated`
- RBAC changes — `rbac.{org,team,role,membership}.{created,updated,deleted}`
- Data operations — `data.query`, `data.write`, `data.import`, `data.delete`
- Database management — `database.created`, `database.deleted`
- Infrastructure — `mqtt.*`, `compaction.triggered`, `tiering.*`
- Default catch-all — `api.{method}` for uncategorized endpoints

**Configuration:**
```toml
[audit_log]
enabled = false              # Requires enterprise license with audit_logging feature
retention_days = 90          # Auto-cleanup of old entries
include_reads = false        # Log GET/query requests (high volume, disabled by default)
```

**Query API (admin-only, license-gated):**
- `GET /api/v1/audit/logs` — Query with filters: event_type, actor, database, since, until, limit, offset
- `GET /api/v1/audit/stats` — Aggregate counts by event_type over time window

**Architecture:** Async buffered channel (non-blocking) → batch SQLite inserts (100 events or 1s flush) → configurable retention cleanup. Fiber middleware captures all auditable requests after response.

---

### 1.3 Tiered Storage

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Local storage | ✅ | ✅ |
| S3/Azure Blob/MinIO | ✅ | ✅ |
| **Hot/Cold tiering** | ❌ | ✅ |
| **Automatic tier migration** | ❌ | ✅ |
| **Per-database policies** | ❌ | ✅ |
| **Query-time tier routing** | ❌ | ✅ |
| **Migration scheduling** | ❌ | ✅ |

**Status: ✅ Implemented**

**Tiering Architecture (2-Tier System):**

```
┌─────────────────────────────────────────────────────────────────┐
│                    Arc Tiered Storage                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  HOT TIER (Local / Existing Storage Backend)                    │
│  ├── Recent data (default: 30 days, configurable)               │
│  ├── Optimized for: Low latency queries                         │
│  ├── Uses ingestion compression settings                        │
│  └── Cost: $$$                                                   │
│                                                                  │
│           │ Automatic migration based on:                        │
│           │ - Partition age (file creation time)                 │
│           │ - Per-database policy thresholds                     │
│           ▼                                                      │
│                                                                  │
│  COLD TIER (S3 Glacier / Azure Archive)                         │
│  ├── Historical data (default: 30+ days)                        │
│  ├── Optimized for: Cost efficiency                             │
│  ├── S3 Storage Class: GLACIER (configurable)                   │
│  └── Cost: $                                                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Features:**
- **Simple 2-tier model**: Hot (fast local) → Cold (archive storage)
- **Single threshold**: Data older than `default_hot_max_age_days` moves to cold
- **Age-based migration**: Files moved based on partition time vs configured threshold
- **Per-database policies**: Override global defaults per database (via API)
- **Hot-only databases**: Exclude specific databases from tiering entirely
- **Scheduled migrations**: Cron-based scheduler runs migrations automatically (default: 2am daily)
- **Manual migrations**: Trigger migrations on-demand via API
- **Zero recompression**: Files move as-is, no re-encoding overhead

**Configuration Example:**
```toml
[tiered_storage]
enabled = true
migration_schedule = "0 2 * * *"     # 2am daily
migration_max_concurrent = 4
migration_batch_size = 100

# Single threshold: data older than this moves from hot to cold
default_hot_max_age_days = 30

[tiered_storage.cold]
enabled = true
backend = "s3"
s3_bucket = "arc-archive"
s3_region = "us-east-1"
# s3_access_key = ""                 # Use env: ARC_TIERED_STORAGE_COLD_S3_ACCESS_KEY
# s3_secret_key = ""                 # Use env: ARC_TIERED_STORAGE_COLD_S3_SECRET_KEY
s3_use_ssl = true
s3_path_style = false
s3_storage_class = "GLACIER"         # GLACIER, DEEP_ARCHIVE, GLACIER_IR, STANDARD_IA
retrieval_mode = "standard"          # standard, expedited, bulk
```

---

### 1.4 Automatic Aggregation & Retention

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Continuous queries (manual trigger) | ✅ | ✅ |
| Retention policies (manual trigger) | ✅ | ✅ |
| **Scheduled continuous queries** | ❌ | ✅ |
| **Automatic retention enforcement** | ❌ | ✅ |
| **Cascading aggregations** | ❌ | ✅ |
| **Aggregation templates** | ❌ | ✅ |
| **Backfill support** | ❌ | ✅ |

**Aggregation Pipeline:**

```
Raw Data (1s resolution)
    │
    │ CQ: 1-minute aggregation (runs every minute)
    ▼
1-Minute Data
    │
    │ CQ: 1-hour aggregation (runs every hour)
    ▼
1-Hour Data
    │
    │ CQ: 1-day aggregation (runs daily)
    ▼
1-Day Data
    │
    │ Retention: Delete raw after 7 days
    │ Retention: Delete 1-min after 30 days
    │ Retention: Delete 1-hour after 365 days
    │ Retention: Keep 1-day forever
    ▼
Optimized Long-term Storage
```

**Configuration Example:**
```toml
[[continuous_queries]]
name = "downsample_1min"
source_measurement = "metrics_raw"
destination_measurement = "metrics_1min"
query = """
  SELECT
    time_bucket('1 minute', timestamp) as timestamp,
    host,
    avg(cpu_percent) as cpu_percent,
    max(memory_used) as memory_used,
    sum(requests) as requests
  FROM metrics_raw
  WHERE timestamp >= NOW() - INTERVAL '5 minutes'
  GROUP BY 1, 2
"""
schedule = "* * * * *"  # Every minute
enabled = true

[[retention_policies]]
name = "raw_retention"
database = "telemetry"
measurement = "metrics_raw"
retention_days = 7
schedule = "0 2 * * *"  # Daily at 2 AM
enabled = true
```

---

### 1.5 Additional Enterprise Features

#### 1.5.1 Query Governance

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Basic query execution | ✅ | ✅ |
| **Query quotas (per user/team)** | ❌ | ✅ |
| **Rate limiting** | ❌ | ✅ |
| **Query result caching** | ❌ | ✅ |
| **Query cost estimation** | ❌ | ✅ |
| **Long-running query management** | ❌ | ✅ |
| **Query audit trail** | ❌ | ✅ |

#### 1.5.2 Backup & Disaster Recovery

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Storage backend replication | ✅ (manual) | ✅ |
| **Managed backup & restore** | ❌ | ✅ |
| **Point-in-time recovery** | ❌ | ✅ |
| **Cross-region replication** | ❌ | ✅ |
| **Backup encryption** | ❌ | ✅ |
| **Backup retention policies** | ❌ | ✅ |

#### 1.5.3 Observability & Operations

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Prometheus metrics | ✅ | ✅ |
| Health checks | ✅ | ✅ |
| **Distributed tracing** | ❌ | ✅ |
| **Advanced dashboards** | ❌ | ✅ |
| **Alerting rules** | ❌ | ✅ |
| **Capacity planning tools** | ❌ | ✅ |
| **Performance advisor** | ❌ | ✅ |

#### 1.5.4 Data Management

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Schema inference | ✅ | ✅ |
| **Schema versioning** | ❌ | ✅ |
| **Schema registry** | ❌ | ✅ |
| **Data lineage tracking** | ❌ | ✅ |
| **Data quality rules** | ❌ | ✅ |
| **PII detection & masking** | ❌ | ✅ |

#### 1.5.5 Multi-Tenancy

| Feature | OSS | Enterprise |
|---------|-----|------------|
| Single tenant | ✅ | ✅ |
| **Logical multi-tenancy** | ❌ | ✅ |
| **Resource isolation** | ❌ | ✅ |
| **Tenant-level quotas** | ❌ | ✅ |
| **Cross-tenant queries** | ❌ | ✅ |

---

## 2. Migration Incentives (OSS → Enterprise)

### 2.1 Pain Points Addressed

| OSS Limitation | Enterprise Solution | Business Impact |
|----------------|---------------------|-----------------|
| Manual CQ execution via API | Scheduled execution + monitoring | Reduced operational overhead |
| Manual retention via API | Automatic enforcement | Storage cost savings |
| Single-node only | Clustering + HA | Production reliability |
| Basic token auth | RBAC + SSO | Enterprise compliance |
| Single storage tier | Hot/Cold 2-tier storage | 60-80% storage cost reduction |
| No query governance | Quotas + caching | Resource predictability |
| No audit trail | Comprehensive logging | Compliance (SOC2, HIPAA) |

### 2.2 Migration Path

```
OSS Deployment
    │
    │ 1. Install Enterprise license
    │ 2. Enable clustering (add nodes)
    │ 3. Configure RBAC
    │ 4. Enable tiered storage
    │ 5. Activate automatic CQs
    ▼
Enterprise Deployment (zero data migration required)
```

**Key Migration Benefits:**
- Drop-in upgrade (same binary, license key enables features)
- No data migration required
- Existing configurations preserved
- Gradual feature adoption

---

## 3. Licensing Model

### 3.1 Licensing Dimensions

After analyzing industry standards (TimescaleDB, InfluxDB, ClickHouse Cloud, Datadog), we recommend a **hybrid model**:

#### Option A: Core-Based Licensing (Recommended)

| Tier | Cluster-Wide Cores | Annual Price | Features |
|------|-------------------|--------------|----------|
| Starter | Up to 8 | $5,000/year | All Enterprise features, single cluster |
| Professional | Up to 32 | $15,000/year | + Multi-cluster, priority support |
| Enterprise | Up to 128 | $40,000/year | + Dedicated support, SLA |
| Unlimited | Unlimited | Custom | + White-glove onboarding, custom SLA |

*Multi-year discounts available. Contact sales.*

**Core Counting:**
- Core limits apply to the **total cores across all nodes** in the cluster
- Each node reports its core count (`GOMAXPROCS`) when joining the cluster
- The cluster leader validates that adding a new node won't exceed the license limit
- Nodes that would cause the cluster to exceed the limit are rejected with a clear error

**Example with Enterprise license (128 cores):**
| Configuration | Total Cores | Allowed? |
|---------------|-------------|----------|
| 4 nodes × 32 cores | 128 | Yes |
| 2 nodes × 64 cores | 128 | Yes |
| 4 nodes × 64 cores | 256 | No (exceeds limit) |

**Rationale:**
- Cores correlate with workload capacity
- Simple to understand and audit
- Common in database industry (Oracle, SQL Server model)
- Allows growth within tier before upgrade
- Cluster-wide counting prevents license arbitrage via many small nodes

#### Option B: Node-Based Licensing

| Tier | Nodes | Price/Node/Month | Features |
|------|-------|------------------|----------|
| Starter | 1-3 | $300 | All Enterprise features |
| Professional | 4-10 | $250 | + Volume discount |
| Enterprise | 11+ | $200 | + Dedicated support |

**Rationale:**
- Simple counting mechanism
- Natural fit for clustered deployment
- Easy to enforce

#### Option C: Ingestion-Based Licensing

| Tier | Events/Day | Price/Month |
|------|------------|-------------|
| Starter | Up to 100M | $500 |
| Growth | Up to 1B | $2,000 |
| Scale | Up to 10B | $8,000 |
| Enterprise | Unlimited | Custom |

**Rationale:**
- Pay for what you use
- Aligns cost with value (more data = more value)
- Industry standard (Datadog, Splunk model)

### 3.2 Recommended Approach: Core-Based with Add-Ons

**Base License (Core-Based):**
```
Arc Enterprise License
├── Base: $500/month for 8 cores (cluster-wide total)
├── Additional cores: $50/core/month (cluster-wide total)
└── Includes: All Enterprise features
```

**Optional Add-Ons:**
| Add-On | Price/Month | Description |
|--------|-------------|-------------|
| Premium Support | +$500 | 4-hour response SLA, dedicated Slack |
| Multi-Region | +$1,000 | Cross-region replication & failover |
| Compliance Pack | +$500 | SOC2, HIPAA audit reports |
| Professional Services | Custom | Implementation, optimization |

### 3.3 Competitive Positioning

| Product | Model | Comparable Arc Price |
|---------|-------|---------------------|
| TimescaleDB Cloud | Usage-based | Arc ~30-50% cheaper |
| InfluxDB Cloud | Usage-based | Arc ~40-60% cheaper |
| ClickHouse Cloud | Usage-based | Arc comparable |
| QuestDB Enterprise | Core-based | Arc comparable |
| Datadog | Per-host + ingestion | Arc ~50-70% cheaper |

**Value Proposition:**
- **10x performance** vs InfluxDB at equivalent resources
- **Simpler architecture** than distributed alternatives
- **Multi-cloud** support included (no vendor lock-in)
- **Predictable pricing** vs consumption-based competitors

---

## 4. Go-to-Market Strategy

### 4.1 Target Customers

**Primary:**
- Mid-market companies (100-1000 employees)
- High data volume (>100M events/day)
- Existing InfluxDB/TimescaleDB users (migration play)
- IoT/Manufacturing companies
- DevOps/Observability teams

**Secondary:**
- Startups scaling beyond OSS
- Enterprises seeking multi-cloud portability
- Edge computing deployments

### 4.2 Sales Motion

```
OSS User Journey:
Download → Evaluate → Production → Scale Issues → Enterprise

Enterprise Entry Points:
1. "We need HA" → Clustering
2. "Storage costs are killing us" → Tiering
3. "Compliance audit" → RBAC + Audit logs
4. "Too much manual work" → Automatic aggregation
```

### 4.3 Success Metrics

| Metric | Target (Year 1) |
|--------|-----------------|
| Enterprise customers | 50 |
| ARR | $500K |
| OSS → Enterprise conversion | 5% |
| Enterprise churn | <5% |
| NPS | >50 |

---

## 5. Implementation Roadmap

### Phase 1: Foundation (Q1 2026)
- [x] License key infrastructure
  - [x] Enterprise Activation Server deployed (https://enterprise.basekick.net)
  - [x] RSA-2048 license signing/verification
  - [x] Online activation flow
  - [x] Offline activation (air-gapped support)
  - [x] Machine fingerprint binding
  - [x] License heartbeat & verification endpoints
  - [x] Admin UI for license management
  - [x] Security hardening (rate limiting, CORS, input validation)
- [x] Automatic CQ scheduling (Enterprise-only)
  - [x] CQ Scheduler with cron-based execution
  - [x] Per-CQ interval configuration
  - [x] License-gated feature check
  - [x] Scheduler reload API endpoint
  - [x] S3 and Azure storage backend support
- [x] Automatic retention enforcement (Enterprise-only)
  - [x] Retention Scheduler with configurable cron schedule
  - [x] Per-policy `is_active` control via API
  - [x] License-gated feature check
  - [x] Manual trigger API endpoint
- [x] RBAC foundation (orgs, teams, granular permissions)
  - [x] Organizations/Teams/Roles hierarchy
  - [x] Database pattern matching with wildcards
  - [x] Measurement-level permissions
  - [x] Token-to-team membership
  - [x] License gating (rbac feature)
  - [x] Backward compatible with OSS tokens
  - [x] Security hardening (input validation, cache security, generic errors)

### Phase 2: Scale (Q2 2026)
- [x] Clustering foundation (shared storage model)
  - [x] Node role system (standalone/writer/reader/compactor)
  - [x] Role capabilities (CanIngest, CanQuery, CanCompact, CanCoordinate)
  - [x] Cluster coordinator with lifecycle management
  - [x] Node registry with health tracking
  - [x] Health checker with configurable thresholds
  - [x] Cluster status API endpoints
  - [x] License gating (clustering feature)
  - [x] Configuration via environment variables and arc.toml
- [x] Tiered storage (hot/cold 2-tier system)
  - [x] TieringManager with hot/cold tier backends
  - [x] MetadataStore for tier file tracking (SQLite)
  - [x] PolicyStore for per-database policies (with API)
  - [x] Migrator with streaming file copy and integrity verification
  - [x] Scheduler with cron-based migration scheduling
  - [x] Query-time tier routing (TierRouter)
  - [x] Tiering API endpoints (status, files, migrate, stats)
  - [x] Policy management API (CRUD for per-database overrides)
  - [x] License gating (tiered_storage feature)
- [ ] Query governance (quotas, rate limiting)
- [x] Audit logging
  - [x] AuditLogger with buffered async writes to SQLite
  - [x] Fiber middleware for automatic event capture
  - [x] Event classification (auth, token, RBAC, data, database, mqtt, compaction, tiering)
  - [x] Query API with filtering (event_type, actor, database, time range)
  - [x] Stats API with aggregate counts by event_type
  - [x] Configurable retention cleanup (default 90 days)
  - [x] License gating (audit_logging feature)
  - [x] Prometheus metrics (audit_events_total, audit_write_errors)

### Phase 3: Enterprise (Q3 2026)
- [x] Raft consensus for writer leader election
  - [x] hashicorp/raft integration with ClusterFSM
  - [x] Leader election and state replication
  - [x] Snapshot/restore for state persistence
- [x] Request routing (HTTP forwarding)
  - [x] RouteWrite forwards to writer nodes
  - [x] RouteQuery forwards to readers (with writer fallback)
  - [x] Load balancing: round-robin, least-connections
- [x] Peer discovery protocol
  - [x] TCP-based protocol with JoinRequest/JoinResponse/LeaderInfo
  - [x] Automatic seed discovery via ARC_CLUSTER_SEEDS
  - [x] Leader redirect for non-leader join requests
- [x] Inter-node WAL replication (Phase 3.3)
  - [x] Replication protocol (ReplicateEntry, Sync, Ack messages)
  - [x] Sender: writer-side WAL streaming to readers
  - [x] Receiver: reader-side reception and local apply
  - [x] Integrated into coordinator lifecycle
- [x] Automatic failover (< 30s RTO)
  - [x] WriterFailoverManager with health-based detection
  - [x] WriterState tracking (primary/standby) on Node
  - [x] Raft FSM commands for promote/demote writer
  - [x] Standby selection (healthiest replica)
  - [x] Cooldown to prevent flapping
  - [x] Router prefers primary writer for write routing
  - [x] Manual failover trigger support
- [ ] SSO integration (LDAP/SAML/OIDC)
- [ ] Backup & restore

### Phase 4: Advanced (Q4 2026)
- [x] Multi-writer sharding
  - [x] Database-level sharding (`hash(database) % num_shards`)
  - [x] Shard map with dynamic assignment via meta cluster
  - [x] Shard router with automatic forwarding to shard primary
  - [x] Meta cluster for shard coordination (Raft-based)
- [x] Per-shard replication
  - [x] ShardReplicationManager for primary-side WAL streaming
  - [x] ShardReceiverManager for replica-side reception
  - [x] Independent WAL streams per shard
  - [x] Configurable buffer sizes and batch settings
- [x] Automatic shard failover
  - [x] FailoverManager with node/shard health monitoring
  - [x] Per-shard Raft (lightweight mode for leadership)
  - [x] Automatic primary promotion on failure
  - [x] Configurable health check intervals and thresholds
- [x] Cross-shard queries (scatter-gather)
  - [x] ScatterGather coordinator for multi-shard queries
  - [x] Parallel shard queries with concurrency limiting
  - [x] Result merging with custom merge functions
  - [x] Local shard optimization
- [ ] Cross-region replication (future)
- [ ] Data temperature inference (future)
- [ ] Advanced observability (future)
- [ ] Multi-tenancy (future)

---

## 6. Technical Considerations

### 6.1 License Enforcement

```go
// License validation approach
type License struct {
    CustomerID    string
    MaxCores      int
    Features      []Feature
    ExpiresAt     time.Time
    Signature     []byte  // RSA signature
}

// Check on startup + periodic validation
func (l *License) Validate() error {
    // Verify signature
    // Check expiration
    // Validate feature access
    // Count cores vs limit
}
```

**Enforcement Strategy:**
- Offline-capable (signature-based validation)
- Graceful degradation (warns before hard block)
- Phone-home optional (telemetry for usage insights)
- Enterprise features disabled if license invalid

### 6.2 Feature Flags

```toml
# Enterprise features gated by license
[enterprise]
enabled = true
license_key = "arc-ent-xxxx-xxxx-xxxx"

[enterprise.clustering]
enabled = true  # Requires valid enterprise license

[enterprise.rbac]
enabled = true

[enterprise.tiering]
enabled = true

[enterprise.auto_aggregation]
enabled = true
```

---

## 7. Decisions Made

1. **Pricing validation**: No. OSS users are not the right audience for enterprise pricing validation - for them, any price feels expensive. Enterprise pricing will be validated with companies actively seeking enterprise features.

2. **Free trial**: 14-day trial. Can be extended after:
   - Setting success criteria with the prospect
   - Engaging with sales team
   - Demonstrating active evaluation

3. **Startup program**: No startup program at launch. Re-evaluate if there's demand.

4. **Arc Cloud vs Enterprise**: Focus on Arc Enterprise (self-hosted) first. Market demand is for enterprise features, not managed hosting. Arc Cloud is a separate initiative for later.

## 8. Additional Decisions

5. **Partner program**: No reseller/MSP program at launch.

6. **Volume discounts**: No volume discounts. Discounts only available for **multi-year contracts**.

7. **Billing cycle**: **Annual only**. Rationale:
   - Self-hosted deployment = no reason for monthly flexibility
   - Reduces procurement and collection overhead
   - Enables predictable revenue projection
   - Aligns with enterprise software buying patterns

8. **Public pricing, sales-assisted purchase**:
   - **Pricing is public** - no "Contact Sales" to see costs
   - **All purchases require sales engagement** - to understand use case, set success criteria, support POC
   - **Goal**: Transparency without friction, human connection without gatekeeping

   | What | Public | Requires Sales |
   |------|--------|----------------|
   | Pricing & features | ✅ | |
   | Trial request | | ✅ |
   | Purchase | | ✅ |
   | POC support | | ✅ |

9. **Repository strategy**: **Monorepo with hardened protection**
   - Single `arc/` repository
   - Enterprise code gated with `//go:build enterprise`
   - **Hardened license protection:**
     - Code obfuscation (e.g., [garble](https://github.com/burrowers/garble) or [go-obfuscate](https://github.com/unixpickle/gobfuscate))
     - Tamper detection (binary integrity checks)
     - License validation at multiple code points (not single bypass)
     - Runtime checks dispersed throughout enterprise code paths
   - Makes bypass effort high enough to deter all but the most determined (who weren't paying anyway)

10. **License enforcement**: **Offline-first, machine fingerprint binding**
    - **Air-gapped support**: File exchange activation (no internet required on server)
      1. Server generates `request.json` with machine fingerprint
      2. Customer uploads to license portal from any machine
      3. Portal returns RSA-signed `license.json`
      4. Customer installs license file on server
      5. Arc validates signature locally (no network needed)
    - **Machine fingerprint**: SHA256 hash of MAC + CPU serial + disk serial
    - **Machine replacement**: Support ticket required (forces conversation, relationship building)
    - **Starter tier**: Single machine only (fingerprint locked)
    - **Professional+**: Multiple machines tracked via license server heartbeat (if internet available) or multiple license files (if air-gapped)

---

## 9. Support & SLA

### 9.1 Support Tiers

| Aspect | Starter ($5K) | Professional ($15K) | Enterprise ($40K) | Unlimited (Custom) |
|--------|---------------|---------------------|-------------------|-------------------|
| **Support channels** | Email | Email + Slack | Email + Slack + Phone | Dedicated Slack channel |
| **Response time (critical)** | 24 hours | 8 hours | 4 hours | 1 hour |
| **Response time (high)** | 48 hours | 24 hours | 8 hours | 4 hours |
| **Response time (normal)** | 5 business days | 2 business days | 1 business day | 8 hours |
| **Support hours** | Business hours (9-5 PT) | Business hours (9-5 PT) | 24/5 | 24/7 |
| **Named contacts** | 2 | 5 | 10 | Unlimited |
| **Escalation path** | Standard | Priority | Direct to engineering | Dedicated engineer |
| **Quarterly reviews** | ❌ | ❌ | ✅ | ✅ (monthly) |

**Severity Definitions:**
- **Critical**: Production down, no workaround
- **High**: Production impaired, workaround available
- **Normal**: Non-production issue or feature question

### 9.2 SLA (Enterprise & Unlimited Tiers Only)

| Commitment | Target |
|------------|--------|
| Software availability | 99.9% (when properly deployed) |
| Critical bug fix | Patch within 72 hours |
| Security vulnerability | Patch within 24 hours (critical), 72 hours (high) |
| Response time adherence | 95% of tickets meet SLA |

**SLA Credits (Enterprise/Unlimited):**
- Miss 3+ critical response SLAs in a quarter → 10% credit
- Miss 5+ critical response SLAs in a quarter → 25% credit

*Note: SLA applies to Arc software and support response, not customer infrastructure.*

---

## 10. Contract Terms

### 10.1 License Expiration Behavior

| Phase | Timing | Behavior |
|-------|--------|----------|
| **Active** | License valid | Full functionality |
| **Warning** | 30 days before expiry | Dashboard warning, email notifications |
| **Grace period** | 0-14 days after expiry | Full functionality, persistent warnings |
| **Expired** | 15+ days after expiry | **Read-only mode** - queries work, writes blocked |
| **Hard block** | 30+ days after expiry | Enterprise features disabled, reverts to OSS behavior |

**Rationale for read-only grace:**
- Doesn't destroy customer's production
- Creates urgency without catastrophe
- Allows time for procurement/renewal process
- Prevents abuse (can't just let it expire and keep writing)

### 10.2 Upgrade/Downgrade Policy

| Action | Policy |
|--------|--------|
| **Upgrade mid-term** | Prorated difference for remaining term |
| **Downgrade mid-term** | Not allowed until renewal |
| **Add cores mid-term** | Prorated for remaining term |
| **Reduce cores mid-term** | Not allowed until renewal |

### 10.3 Renewal Terms

| Term | Policy |
|------|--------|
| **Notice period** | 60 days before expiry for non-renewal |
| **Auto-renewal** | Yes, unless notice given |
| **Price lock** | Year 1 price locked; Years 2+ subject to max 5% increase |
| **Multi-year discount** | 2-year: 10% off; 3-year: 15% off |

### 10.4 Payment Terms

| Term | Policy |
|------|--------|
| **Payment timing** | Annual upfront |
| **Net terms** | Net 30 (invoiced customers) |
| **Payment methods** | Wire transfer, ACH, credit card (+3% fee) |
| **Late payment** | 1.5%/month interest after 30 days |

---

## 11. Security & Compliance

### 11.1 Current Certifications

| Certification | Status | Timeline |
|---------------|--------|----------|
| SOC 2 Type I | Planned | Q3 2026 |
| SOC 2 Type II | Planned | Q1 2027 |
| ISO 27001 | Not planned | On request |
| HIPAA BAA | Available on request | Enterprise tier only |
| GDPR DPA | Available | All tiers |

### 11.2 Security Commitments

- Annual penetration testing (report available under NDA)
- Vulnerability disclosure program
- Security advisories for critical issues
- Encrypted license files (RSA-2048)
- No customer data transmitted to Basekick (license validation only)

---

## 12. Documentation & Training

| Resource | Starter | Professional | Enterprise | Unlimited |
|----------|---------|--------------|------------|-----------|
| Online documentation | ✅ | ✅ | ✅ | ✅ |
| Video tutorials | ✅ | ✅ | ✅ | ✅ |
| Architecture guides | ✅ | ✅ | ✅ | ✅ |
| Live onboarding session | 1 hour | 2 hours | 4 hours | Custom |
| Custom training | ❌ | On request ($) | 1 day included | Custom |
| Architecture review | ❌ | ❌ | 1 included | Quarterly |

---

## 13. Professional Services (Optional)

| Service | Price | Description |
|---------|-------|-------------|
| Implementation package | $5,000 | Guided deployment, config review, go-live support |
| Migration assistance | $2,500/day | Help migrating from InfluxDB, TimescaleDB, etc. |
| Performance optimization | $2,500/day | Query tuning, schema design, infrastructure review |
| Custom integration | $2,500/day | Build custom integrations, connectors |
| Training workshop | $3,000/day | On-site or remote team training |

*Professional services available for all tiers.*

---

## 14. Arc Enterprise Configuration Guide

This section documents how to configure Arc with an Enterprise license to enable automatic CQ and retention scheduling.

### 14.1 Feature Overview

Arc Enterprise adds automatic scheduling features that require a valid license:

| Feature | Description | License Required |
|---------|-------------|------------------|
| **CQ Scheduler** | Automatically executes continuous queries on their configured interval | Yes |
| **Retention Scheduler** | Automatically enforces retention policies on a cron schedule | Yes |
| **Manual CQ/Retention** | Create, list, update, delete, and manually execute CQs and retention policies | No |

Without a license, you can still use CQs and retention policies manually - only automatic scheduling requires Enterprise.

### 14.2 Configuration

#### Environment Variables

```bash
# License Configuration
ARC_LICENSE_KEY=ARC-ENT-XXXX-XXXX-XXXX-XXXX  # Your enterprise license key

# Scheduler Configuration (requires valid license)
ARC_SCHEDULER_RETENTION_SCHEDULE="0 3 * * *" # Cron schedule for retention (default: 3am daily)
```

**Note**: The license server URL (`https://enterprise.basekick.net`) and validation interval (4 hours) are hardcoded and cannot be changed.

CQ and retention scheduling is controlled per-item via `is_active` field when creating/updating CQs and retention policies through the API.

#### arc.toml Configuration

```toml
[license]
enabled = true
key = "ARC-ENT-XXXX-XXXX-XXXX-XXXX"

[scheduler]
retention_schedule = "0 3 * * *" # Cron schedule for retention (default: 3am daily)
```

**Note:** CQ and retention scheduling is controlled per-item via `is_active` field in the database, managed through the API. There are no global `cq_enabled` or `retention_enabled` config options.

### 14.3 License Tiers

| Tier | Features |
|------|----------|
| **starter** | Basic features, limited cores |
| **professional** | CQ scheduler, retention scheduler |
| **enterprise** | All features, RBAC, clustering |
| **unlimited** | All features, unlimited cores/machines |

Features are gated by the `features` array in the license:
- `cq_scheduler` - Enables automatic CQ scheduling
- `retention_scheduler` - Enables automatic retention scheduling
- `rbac` - Role-based access control
- `clustering` - Multi-node clustering (future)

### 14.4 API Endpoints

#### Scheduler Status

```bash
# Get scheduler status
curl http://localhost:8000/api/v1/schedulers
```

Response (without license):
```json
{
  "cq_scheduler": {
    "enabled": false,
    "reason": "CQ scheduler not configured or no valid license"
  },
  "retention_scheduler": {
    "enabled": false,
    "reason": "Retention scheduler not configured or no valid license"
  },
  "license": {
    "valid": false,
    "reason": "License client not configured"
  }
}
```

Response (with valid license):
```json
{
  "cq_scheduler": {
    "running": true,
    "job_count": 5,
    "license_valid": true
  },
  "retention_scheduler": {
    "running": true,
    "schedule": "0 3 * * *",
    "next_run": "2026-01-09T03:00:00Z",
    "license_valid": true
  },
  "license": {
    "valid": true,
    "tier": "professional",
    "status": "active",
    "days_remaining": 365,
    "expires_at": "2027-01-08T00:00:00Z",
    "features": ["cq_scheduler", "retention_scheduler"]
  }
}
```

#### Reload CQ Scheduler (Enterprise)

```bash
# Reload all CQ schedules from database
curl -X POST http://localhost:8000/api/v1/schedulers/cq/reload
```

#### Trigger Retention Now (Enterprise)

```bash
# Trigger retention execution immediately
curl -X POST http://localhost:8000/api/v1/schedulers/retention/trigger
```

### 14.5 Startup Behavior

#### Without License

At startup, Arc logs:
```
WRN Enterprise license not configured - enterprise features disabled
```

When scheduler endpoints are called:
```
WRN Scheduler status requested - enterprise license required for automatic scheduling
WRN CQ scheduler reload requested but scheduler not running - enterprise license required
WRN Retention trigger requested but scheduler not running - enterprise license required
```

#### With Valid License

At startup, Arc logs:
```
INF Enterprise license verified tier=professional status=active days_remaining=365 expires_at=2027-01-08T00:00:00Z
INF CQ scheduler started job_count=5
INF Retention scheduler started schedule="0 3 * * *"
```

#### License Verification Failure

If license verification fails at startup:
```
WRN License verification failed - enterprise features disabled error="connection refused"
```

Arc continues to run with enterprise features disabled.

### 14.6 CQ Interval Configuration

When creating a CQ, the `interval` field specifies how often it should run automatically:

```bash
curl -X POST http://localhost:8000/api/v1/continuous_queries \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cpu_1m_rollup",
    "database": "production",
    "source_measurement": "cpu",
    "destination_measurement": "cpu_1m",
    "query": "SELECT time_bucket('\''1m'\'', timestamp) as timestamp, host, avg(usage_percent) as avg_usage FROM production.cpu WHERE timestamp >= {start_time} AND timestamp < {end_time} GROUP BY time_bucket('\''1m'\'', timestamp), host",
    "interval": "30s",
    "is_active": true
  }'
```

Supported interval formats (Go duration):
- `10s` - 10 seconds (minimum)
- `1m` - 1 minute
- `5m` - 5 minutes
- `1h` - 1 hour
- `24h` - 24 hours

**Note**: The CQ will only execute automatically if:
1. Enterprise license is valid
2. `ARC_SCHEDULER_CQ_ENABLED=true`
3. CQ `is_active=true`
4. Interval is at least 10 seconds

### 14.7 Retention Schedule Configuration

The retention scheduler uses cron syntax for scheduling. Configure via:

```bash
ARC_SCHEDULER_RETENTION_SCHEDULE="0 3 * * *"  # 3am daily (default)
```

Cron format: `minute hour day-of-month month day-of-week`

Examples:
- `0 3 * * *` - Daily at 3:00 AM
- `0 */6 * * *` - Every 6 hours
- `0 0 * * 0` - Weekly on Sunday at midnight
- `0 12 1 * *` - First of every month at noon

### 14.8 Obtaining a License

Contact Basekick to obtain an Arc Enterprise license:
- Email: sales@basekick.net
- Website: https://basekick.net/arc-enterprise

License keys follow the format: `ARC-ENT-XXXX-XXXX-XXXX-XXXX`

### 14.9 Troubleshooting

#### License validation fails

1. Check network connectivity to `enterprise.basekick.net`
2. Verify license key is correct
3. Check if license has expired
4. Review Arc logs for specific error messages

#### Scheduler not starting

1. Verify license is configured: `ARC_LICENSE_KEY` or `[license] key` in config
2. Verify license is valid and includes required features (`cq_scheduler`, `retention_scheduler`)
3. Check that CQ/Retention feature is enabled in config (`continuous_query.enabled=true`, `retention.enabled=true`)
4. Ensure at least one CQ or retention policy exists with `is_active=true`

#### CQ not executing

1. Check CQ is active: `curl http://localhost:8000/api/v1/continuous_queries`
2. Verify interval is at least 10 seconds
3. Check scheduler status: `curl http://localhost:8000/api/v1/schedulers`
4. Review Arc logs for execution errors

---

## 15. RBAC Configuration Guide (Enterprise)

This section documents how to configure and use Role-Based Access Control (RBAC) in Arc Enterprise.

### 15.1 Overview

RBAC provides granular access control with a hierarchy:

```
Organizations
    └── Teams
            └── Roles (database patterns + permissions)
                    └── Measurement Permissions (optional)
```

**License Requirement:** RBAC requires an enterprise license with the `rbac` feature enabled.

### 15.2 Concepts

| Concept | Description |
|---------|-------------|
| **Organization** | Top-level tenant container |
| **Team** | Group within an organization that can have multiple roles |
| **Role** | Permission set for a database pattern (e.g., `production`, `*`, `analytics_*`) |
| **Measurement Permission** | Optional fine-grained permission for specific measurements within a role |
| **Token Membership** | Links an API token to one or more teams |

### 15.3 Permission Types

| Permission | Description |
|------------|-------------|
| `read` | Query/read data from databases |
| `write` | Write/ingest data |
| `delete` | Delete data |
| `admin` | Administrative access (grants all permissions) |

### 15.4 Pattern Matching

Database and measurement patterns support wildcards:

| Pattern | Matches | Example |
|---------|---------|---------|
| `production` | Exact match | Only "production" |
| `*` | Any value | All databases/measurements |
| `prod_*` | Prefix with underscore | "prod_us", "prod_eu" |
| `*_metrics` | Suffix with underscore | "cpu_metrics", "memory_metrics" |
| `prod*` | General prefix | "production", "prod_v2" |

### 15.5 API Reference

All RBAC endpoints require `admin` permission and a valid enterprise license with `rbac` feature.

#### Organizations

```bash
# List organizations
curl http://localhost:8000/api/v1/rbac/organizations \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Create organization
curl -X POST http://localhost:8000/api/v1/rbac/organizations \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme Corp",
    "description": "Main organization"
  }'

# Get organization (includes teams)
curl http://localhost:8000/api/v1/rbac/organizations/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Update organization
curl -X PATCH http://localhost:8000/api/v1/rbac/organizations/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Updated description",
    "enabled": true
  }'

# Delete organization (cascades to teams, roles, memberships)
curl -X DELETE http://localhost:8000/api/v1/rbac/organizations/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

#### Teams

```bash
# List teams in organization
curl http://localhost:8000/api/v1/rbac/organizations/1/teams \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Create team
curl -X POST http://localhost:8000/api/v1/rbac/organizations/1/teams \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Data Engineering",
    "description": "Data platform team"
  }'

# Get team (includes roles)
curl http://localhost:8000/api/v1/rbac/teams/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Update team
curl -X PATCH http://localhost:8000/api/v1/rbac/teams/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}'

# Delete team (cascades to roles and memberships)
curl -X DELETE http://localhost:8000/api/v1/rbac/teams/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

#### Roles

```bash
# List roles for team
curl http://localhost:8000/api/v1/rbac/teams/1/roles \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Create role with database pattern
curl -X POST http://localhost:8000/api/v1/rbac/teams/1/roles \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "database_pattern": "production",
    "permissions": ["read", "write"]
  }'

# Create role with wildcard
curl -X POST http://localhost:8000/api/v1/rbac/teams/1/roles \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "database_pattern": "staging_*",
    "permissions": ["read", "write", "delete"]
  }'

# Get role (includes measurement permissions)
curl http://localhost:8000/api/v1/rbac/roles/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Update role
curl -X PATCH http://localhost:8000/api/v1/rbac/roles/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "permissions": ["read"]
  }'

# Delete role
curl -X DELETE http://localhost:8000/api/v1/rbac/roles/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

#### Measurement Permissions (Optional Granular Access)

```bash
# List measurement permissions for role
curl http://localhost:8000/api/v1/rbac/roles/1/measurements \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Add measurement-level permission
curl -X POST http://localhost:8000/api/v1/rbac/roles/1/measurements \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "measurement_pattern": "metrics_*",
    "permissions": ["read"]
  }'

# Delete measurement permission
curl -X DELETE http://localhost:8000/api/v1/rbac/measurement-permissions/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

#### Token Membership

```bash
# List teams a token belongs to
curl http://localhost:8000/api/v1/auth/tokens/1/teams \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Add token to team
curl -X POST http://localhost:8000/api/v1/auth/tokens/1/teams \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"team_id": 1}'

# Remove token from team
curl -X DELETE http://localhost:8000/api/v1/auth/tokens/1/teams/1 \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Get effective permissions for token
curl http://localhost:8000/api/v1/auth/tokens/1/permissions \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### 15.6 Complete Example: Setting Up RBAC

```bash
# Set your admin token
export ADMIN_TOKEN="your-admin-token"
export ARC_URL="http://localhost:8000"

# 1. Create an organization
curl -X POST $ARC_URL/api/v1/rbac/organizations \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "Acme Corp", "description": "Main organization"}'
# Returns: {"success": true, "organization": {"id": 1, ...}}

# 2. Create teams
curl -X POST $ARC_URL/api/v1/rbac/organizations/1/teams \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "Data Engineering", "description": "Full access to production"}'
# Returns: {"success": true, "team": {"id": 1, ...}}

curl -X POST $ARC_URL/api/v1/rbac/organizations/1/teams \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "Analytics", "description": "Read-only access"}'
# Returns: {"success": true, "team": {"id": 2, ...}}

# 3. Create roles for Data Engineering team (full access to production)
curl -X POST $ARC_URL/api/v1/rbac/teams/1/roles \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"database_pattern": "production", "permissions": ["read", "write", "delete"]}'

# 4. Create roles for Analytics team (read-only, specific measurements)
curl -X POST $ARC_URL/api/v1/rbac/teams/2/roles \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"database_pattern": "production", "permissions": ["read"]}'

# Add measurement restriction to Analytics role
curl -X POST $ARC_URL/api/v1/rbac/roles/2/measurements \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"measurement_pattern": "metrics_*", "permissions": ["read"]}'

curl -X POST $ARC_URL/api/v1/rbac/roles/2/measurements \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"measurement_pattern": "events_*", "permissions": ["read"]}'

# 5. Create API tokens and assign to teams

# Create a Data Engineering token
curl -X POST $ARC_URL/api/v1/auth/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "data-eng-token", "description": "Data Engineering team token", "permissions": []}'
# Returns: {"success": true, "token": "xyz123..."}

# Add to Data Engineering team
curl -X POST $ARC_URL/api/v1/auth/tokens/2/teams \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"team_id": 1}'

# Create an Analytics token
curl -X POST $ARC_URL/api/v1/auth/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "analytics-token", "description": "Analytics team token", "permissions": []}'

# Add to Analytics team
curl -X POST $ARC_URL/api/v1/auth/tokens/3/teams \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"team_id": 2}'

# 6. Verify effective permissions
curl $ARC_URL/api/v1/auth/tokens/2/permissions \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# Data Engineering token should have read/write/delete on production

curl $ARC_URL/api/v1/auth/tokens/3/permissions \
  -H "Authorization: Bearer $ADMIN_TOKEN"
# Analytics token should have read-only on metrics_* and events_* measurements
```

### 15.7 Permission Resolution

When a request is made with a token:

1. **If RBAC is disabled** (no license): Use OSS token permissions only
2. **If RBAC is enabled but token has no team memberships**: Use OSS token permissions (backward compatible)
3. **If RBAC is enabled and token has team memberships**:
   - Check each team's roles for matching database pattern
   - If role has measurement permissions, check for matching measurement pattern
   - Aggregate permissions from all matching roles
   - If RBAC grants permission: ALLOW
   - If RBAC denies: Fallback to OSS token permissions
   - If neither grants: DENY

### 15.8 Security Hardening

The RBAC implementation includes comprehensive security hardening to protect against common vulnerabilities:

#### License Enforcement
- **Machine fingerprint binding**: Licenses are bound to hardware (SHA256 of hostname + MAC + CPU + OS)
- **Server-side validation**: Every license must be verified with the central enterprise server
- **Periodic re-validation**: Licenses are re-verified every 4 hours
- **Feature gating**: RBAC requires the `rbac` feature flag in the license
- **Multiple enforcement points**: License checked at API routes, permission checks, and manager level

#### Input Validation
- **Pattern validation**: Database/measurement patterns are validated against strict regex:
  - Only alphanumeric characters, underscores, hyphens allowed
  - Single wildcard (`*`) allowed at start or end only
  - No SQL injection characters permitted
- **Name validation**: Organization/team names must:
  - Start with a letter
  - Contain only alphanumeric characters, underscores, hyphens
  - Be at most 64 characters

#### Cache Security
- **Immediate invalidation**: Permission changes take effect immediately (cache invalidated on mutations)
- **Disabled token check**: Disabled tokens are denied immediately, not cached for 30 seconds
- **TOCTOU protection**: Double-checked locking prevents race conditions in cache loading

#### Error Handling
- **Generic error messages**: Database errors return "Internal server error" to clients
- **Server-side logging**: Detailed errors are logged for debugging but not exposed

#### Access Control
- **SHOW command protection**: `SHOW DATABASES` and `SHOW TABLES` require read permission
- **Per-measurement checks**: Write operations validate permissions for each measurement being written

### 15.9 Troubleshooting RBAC

#### RBAC endpoints return 403

1. Verify license includes `rbac` feature: `curl $ARC_URL/api/v1/schedulers` (check license section)
2. Verify you're using an admin token

#### Token doesn't have expected permissions

1. Check token's team memberships: `curl $ARC_URL/api/v1/auth/tokens/{id}/teams`
2. Check effective permissions: `curl $ARC_URL/api/v1/auth/tokens/{id}/permissions`
3. Verify role patterns match your database/measurement names
4. Check if measurement permissions are restricting access

#### OSS tokens stopped working

They should still work! RBAC is additive. If token has OSS permissions, those are checked as fallback. Verify:
1. Token is enabled
2. Token has the required permission in its permissions list
3. Token hasn't expired

---

## 16. Clustering Configuration Guide (Enterprise)

This section documents how to configure and use Arc Enterprise clustering for horizontal scaling with role-based node separation.

### 16.1 Overview

Arc Enterprise clustering enables horizontal scaling through a **shared storage model** with role-based node separation:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Arc Enterprise Cluster                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Writer 1   │    │   Writer 2   │    │   Writer 3   │       │
│  │  (Primary)   │◄──►│  (Standby)   │◄──►│  (Standby)   │       │
│  └──────┬───────┘    └──────────────┘    └──────────────┘       │
│         │                                                        │
│         │ Writes to shared storage                               │
│         ▼                                                        │
│  ┌──────────────────────────────────────────────────────┐       │
│  │              Shared Storage Layer                     │       │
│  │         (S3 / Azure Blob / MinIO / NFS)              │       │
│  └──────────────────────────────────────────────────────┘       │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Reader 1   │    │   Reader 2   │    │   Reader N   │       │
│  │  (Query)     │    │  (Query)     │    │  (Query)     │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐                           │
│  │ Compactor 1  │    │ Compactor 2  │    (Background workers)   │
│  └──────────────┘    └──────────────┘                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**License Requirement:** Clustering requires an enterprise license with the `clustering` feature enabled.

### 16.2 Node Roles

| Role | Description | Capabilities |
|------|-------------|--------------|
| `standalone` | Single-node deployment (default, OSS-compatible) | Ingest, Query, Compact |
| `writer` | Handles data ingestion, writes to shared storage | Ingest, Query, Coordinate |
| `reader` | Query-only node, reads from shared storage | Query only |
| `compactor` | Background compaction and maintenance | Compact only |

**Role Capabilities Matrix:**

| Role | CanIngest | CanQuery | CanCompact | CanCoordinate |
|------|-----------|----------|------------|---------------|
| standalone | ✅ | ✅ | ✅ | ❌ |
| writer | ✅ | ✅ | ❌ | ✅ |
| reader | ❌ | ✅ | ❌ | ❌ |
| compactor | ❌ | ❌ | ✅ | ❌ |

### 16.3 Configuration

#### Environment Variables

```bash
# Enable clustering (requires enterprise license with clustering feature)
ARC_CLUSTER_ENABLED=true

# Node identity
ARC_CLUSTER_NODE_ID=arc-writer-1          # Unique node ID (auto-generated if empty)
ARC_CLUSTER_ROLE=writer                    # standalone, writer, reader, compactor
ARC_CLUSTER_NAME=arc-production            # Cluster name for identification

# Discovery (for multi-node clusters)
ARC_CLUSTER_SEEDS=10.0.0.1:9100,10.0.0.2:9100  # Seed nodes for discovery

# Network
ARC_CLUSTER_COORDINATOR_ADDR=:9100         # Coordinator bind address
ARC_CLUSTER_ADVERTISE_ADDR=10.0.0.1:9100   # Address advertised to peers

# Health checking
ARC_CLUSTER_HEALTH_CHECK_INTERVAL=5        # Seconds between health checks
ARC_CLUSTER_HEALTH_CHECK_TIMEOUT=3         # Seconds before check times out
ARC_CLUSTER_UNHEALTHY_THRESHOLD=3          # Failed checks before marking unhealthy

# Heartbeat
ARC_CLUSTER_HEARTBEAT_INTERVAL=1           # Seconds between heartbeats
ARC_CLUSTER_HEARTBEAT_TIMEOUT=5            # Seconds before node considered dead
```

#### arc.toml Configuration

```toml
[cluster]
enabled = true
node_id = "arc-writer-1"
role = "writer"
cluster_name = "arc-production"
seeds = ["10.0.0.1:9100", "10.0.0.2:9100"]
coordinator_addr = ":9100"
advertise_addr = "10.0.0.1:9100"
health_check_interval = 5
health_check_timeout = 3
unhealthy_threshold = 3
heartbeat_interval = 1
heartbeat_timeout = 5
```

### 16.4 API Endpoints

#### Get Cluster Status

```bash
curl http://localhost:8000/api/v1/cluster
```

**Response (clustering enabled):**
```json
{
  "enabled": true,
  "mode": "cluster",
  "running": true,
  "cluster_name": "arc-production",
  "local_node_id": "arc-writer-1",
  "local_role": "writer",
  "node_count": 5,
  "healthy_count": 5,
  "writers": 1,
  "readers": 3,
  "compactors": 1,
  "nodes": [
    {
      "id": "arc-writer-1",
      "role": "writer",
      "state": "healthy",
      "address": ":9100",
      "api_address": ":8000"
    }
  ],
  "license": {
    "valid": true,
    "tier": "enterprise",
    "features": ["clustering", "cq_scheduler", "retention_scheduler", "rbac"]
  }
}
```

**Response (clustering disabled):**
```json
{
  "enabled": false,
  "mode": "standalone",
  "reason": "Enterprise license with clustering feature required"
}
```

#### List Cluster Nodes

```bash
# Get all nodes
curl http://localhost:8000/api/v1/cluster/nodes

# Filter by role
curl http://localhost:8000/api/v1/cluster/nodes?role=writer

# Filter by state
curl http://localhost:8000/api/v1/cluster/nodes?state=healthy
```

#### Get Specific Node

```bash
curl http://localhost:8000/api/v1/cluster/nodes/arc-writer-1
```

#### Get Local Node Info

```bash
curl http://localhost:8000/api/v1/cluster/local
```

**Response:**
```json
{
  "id": "arc-writer-1",
  "name": "arc-writer-1",
  "role": "writer",
  "state": "healthy",
  "address": ":9100",
  "api_address": ":8000",
  "cluster_name": "arc-production",
  "version": "1.0.0",
  "started_at": "2026-01-13T10:00:00Z",
  "joined_at": "2026-01-13T10:00:01Z",
  "capabilities": {
    "can_ingest": true,
    "can_query": true,
    "can_compact": false,
    "can_coordinate": true
  },
  "is_local": true
}
```

#### Get Cluster Health

```bash
curl http://localhost:8000/api/v1/cluster/health
```

**Response:**
```json
{
  "healthy": 5,
  "unhealthy": 0,
  "total": 5,
  "health_checker": {
    "running": true,
    "check_interval_ms": 5000,
    "check_timeout_ms": 3000,
    "unhealthy_threshold": 3
  }
}
```

### 16.5 Deployment Examples

#### Single Writer + Multiple Readers

```bash
# Writer node
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_ROLE=writer \
ARC_CLUSTER_NODE_ID=writer-1 \
ARC_CLUSTER_NAME=prod \
ARC_STORAGE_BACKEND=s3 \
ARC_STORAGE_S3_BUCKET=arc-data \
./arc

# Reader node 1
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_ROLE=reader \
ARC_CLUSTER_NODE_ID=reader-1 \
ARC_CLUSTER_NAME=prod \
ARC_CLUSTER_SEEDS=writer-1:9100 \
ARC_STORAGE_BACKEND=s3 \
ARC_STORAGE_S3_BUCKET=arc-data \
./arc

# Reader node 2
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_ROLE=reader \
ARC_CLUSTER_NODE_ID=reader-2 \
ARC_CLUSTER_NAME=prod \
ARC_CLUSTER_SEEDS=writer-1:9100 \
ARC_STORAGE_BACKEND=s3 \
ARC_STORAGE_S3_BUCKET=arc-data \
./arc
```

#### Dedicated Compactor

```bash
# Compactor node (no API traffic, just background work)
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_ROLE=compactor \
ARC_CLUSTER_NODE_ID=compactor-1 \
ARC_CLUSTER_NAME=prod \
ARC_STORAGE_BACKEND=s3 \
ARC_STORAGE_S3_BUCKET=arc-data \
./arc
```

### 16.6 Node States

| State | Description |
|-------|-------------|
| `unknown` | Initial state before health is determined |
| `joining` | Node is joining the cluster |
| `healthy` | Node is operating normally |
| `unhealthy` | Node has failed health checks but may recover |
| `dead` | Node has been unreachable for extended period |
| `leaving` | Node is gracefully leaving the cluster |

### 16.7 Implementation Status

The clustering implementation is complete through Phase 3, with Phase 4 (sharding) planned:

| Feature | Status | Phase |
|---------|--------|-------|
| Node role system | ✅ Implemented | Phase 2 |
| Cluster coordinator | ✅ Implemented | Phase 2 |
| Node registry | ✅ Implemented | Phase 2 |
| Health checking | ✅ Implemented | Phase 2 |
| Cluster status API | ✅ Implemented | Phase 2 |
| License gating | ✅ Implemented | Phase 2 |
| Raft consensus (leader election) | ✅ Implemented | Phase 3.1 |
| Request routing (HTTP forwarding) | ✅ Implemented | Phase 3.2 |
| Router integration (API handlers) | ✅ Implemented | Phase 3.2b |
| Inter-node WAL replication | ✅ Implemented | Phase 3.3 |
| Peer discovery protocol | ✅ Implemented | Phase 3.4 |
| **Automatic writer failover** | ✅ Implemented | Phase 3.5 |
| **Multi-writer sharding** | ✅ Implemented | Phase 4.1 |
| **Meta cluster (dynamic assignment)** | ✅ Implemented | Phase 4.1b |
| **Per-shard replication** | ✅ Implemented | Phase 4.2 |
| **Per-shard Raft & failover** | ✅ Implemented | Phase 4.3 |
| **Cross-shard queries (scatter-gather)** | ✅ Implemented | Phase 4.4 |

#### Current Throughput (Phase 3)

| Configuration | Write Throughput | Notes |
|---------------|------------------|-------|
| Single writer, direct | 11.4M rps | Maximum single-node performance |
| Reader → Writer (forwarded) | 8.8M rps | ~23% overhead for routing hop |

#### Phase 4 Throughput (Planned)

| Configuration | Write Throughput | Fault Tolerance |
|---------------|------------------|-----------------|
| 3 shards, RF=3 | 33M rps | Survives 2 failures per shard |
| 10 shards, RF=3 | 110M rps | Survives 2 failures per shard |

### 16.8 Raft Configuration (Phase 3)

Phase 3 adds Raft-based leader election and peer discovery:

```bash
# Raft consensus configuration
ARC_CLUSTER_RAFT_DATA_DIR=./data/raft           # Raft state directory
ARC_CLUSTER_RAFT_BIND_ADDR=:9200                # Raft transport bind address
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=10.0.0.1:9200   # Full address advertised to peers
ARC_CLUSTER_RAFT_BOOTSTRAP=true                 # Bootstrap new cluster (first node only)

# Request routing configuration
ARC_CLUSTER_ROUTE_TIMEOUT=5000                  # Forward timeout in milliseconds
ARC_CLUSTER_ROUTE_RETRIES=3                     # Retry count for forwards
```

**Important:** `ARC_CLUSTER_RAFT_ADVERTISE_ADDR` must be a full address (e.g., `localhost:9200` or `10.0.0.1:9200`), not just a port binding like `:9200`.

### 16.9 Multi-Node Cluster Setup

Example 3-node cluster with writer and readers:

```bash
# Terminal 1: Writer (will become Raft leader)
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_ROLE=writer \
ARC_CLUSTER_NODE_ID=writer-1 \
ARC_CLUSTER_NAME=prod \
ARC_SERVER_PORT=8000 \
ARC_CLUSTER_COORDINATOR_ADDR=:9100 \
ARC_CLUSTER_ADVERTISE_ADDR=localhost:9100 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9200 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=localhost:9200 \
ARC_CLUSTER_RAFT_BOOTSTRAP=true \
./arc

# Terminal 2: Reader (joins via seed)
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_ROLE=reader \
ARC_CLUSTER_NODE_ID=reader-1 \
ARC_CLUSTER_NAME=prod \
ARC_SERVER_PORT=8001 \
ARC_CLUSTER_COORDINATOR_ADDR=:9101 \
ARC_CLUSTER_ADVERTISE_ADDR=localhost:9101 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9201 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=localhost:9201 \
ARC_CLUSTER_SEEDS=localhost:9100 \
./arc

# Terminal 3: Reader (joins via seed)
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_ROLE=reader \
ARC_CLUSTER_NODE_ID=reader-2 \
ARC_CLUSTER_NAME=prod \
ARC_SERVER_PORT=8002 \
ARC_CLUSTER_COORDINATOR_ADDR=:9102 \
ARC_CLUSTER_ADVERTISE_ADDR=localhost:9102 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9202 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=localhost:9202 \
ARC_CLUSTER_SEEDS=localhost:9100 \
./arc
```

### 16.10 Peer Discovery Protocol

New nodes join the cluster by connecting to seed nodes:

1. **Bootstrap**: First node starts with `ARC_CLUSTER_RAFT_BOOTSTRAP=true` and becomes leader
2. **Discovery**: New nodes connect to seeds via `ARC_CLUSTER_SEEDS`
3. **Join Request**: New node sends JoinRequest with its ID, role, and addresses
4. **Leader Redirect**: If seed is not leader, it redirects to the current leader
5. **Raft AddVoter**: Leader adds the new node to Raft cluster
6. **FSM Update**: Leader adds node to cluster state via FSM
7. **Response**: Leader sends JoinResponse with cluster info

Protocol message types:
- `JoinRequest` - New node wants to join
- `JoinResponse` - Leader responds with success/failure and cluster info
- `LeaderInfo` - Non-leader redirects to current leader
- `Heartbeat` / `HeartbeatAck` - Periodic keepalive
- `LeaveNotify` - Node gracefully leaving

### 16.11 Multi-Writer Sharding (Phase 4 - Implemented)

Phase 4 enables horizontal write scaling through database-level sharding:

```
                         ┌─────────────────────┐
                         │   Client / LB       │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
             ┌──────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
             │  Shard 0    │ │  Shard 1    │ │  Shard 2    │
             ├─────────────┤ ├─────────────┤ ├─────────────┤
             │ Writer-0    │ │ Writer-1    │ │ Writer-2    │
             │   (primary) │ │   (primary) │ │   (primary) │
             │ Replica-0a  │ │ Replica-1a  │ │ Replica-2a  │
             │ Replica-0b  │ │ Replica-1b  │ │ Replica-2b  │
             └─────────────┘ └─────────────┘ └─────────────┘
```

**Key Features:**
- **Database-level sharding**: `shard_id = hash(database_name) % num_shards` using FNV-1a
- **Dynamic assignment**: Meta cluster automatically assigns shards to nodes via Raft consensus
- **Per-shard replication**: Each shard has independent WAL replication streams (configurable RF)
- **Automatic failover**: FailoverManager monitors health and promotes replicas automatically
- **Cross-shard queries**: ScatterGather coordinator parallelizes queries across shards

**Configuration:**

```bash
# Enable sharding
ARC_CLUSTER_SHARDING_ENABLED=true
ARC_CLUSTER_SHARDING_NUM_SHARDS=3              # Number of shards
ARC_CLUSTER_SHARDING_REPLICATION_FACTOR=3      # Copies per shard (default: 3)

# Per-shard Raft
ARC_CLUSTER_SHARD_RAFT_BASE_PORT=9300          # Base port for shard Raft
ARC_CLUSTER_SHARD_RAFT_DATA_DIR=./data/shard-raft

# Failover configuration
ARC_CLUSTER_SHARDING_HEALTH_CHECK_INTERVAL=5s  # Health check frequency
ARC_CLUSTER_SHARDING_UNHEALTHY_THRESHOLD=3     # Failed checks before unhealthy
ARC_CLUSTER_SHARDING_FAILOVER_TIMEOUT=30s      # Failover operation timeout

# Per-shard replication
ARC_CLUSTER_SHARDING_REPLICATION_BUFFER=10000  # Entry buffer size
ARC_CLUSTER_SHARDING_REPLICATION_BATCH=100     # Batch size for sends
ARC_CLUSTER_SHARDING_REPLICATION_FLUSH=100ms   # Flush interval
```

**Write Path:**
1. Client sends write to any node
2. Node computes: `shard_id = hash(database) % num_shards`
3. Node forwards to shard primary (or processes locally if primary)
4. Primary writes to WAL + ArrowBuffer
5. Primary replicates to shard replicas asynchronously

**Query Path:**
1. Client sends query to any node
2. Node determines affected databases from query
3. Single-shard query: Route to shard primary or replica
4. Multi-shard query: ScatterGather fans out to all shards in parallel
5. Results merged and returned

**Failover:**
1. FailoverManager detects unhealthy primary (missed health checks)
2. Meta cluster leader selects best replica (healthy, lowest lag)
3. New primary promoted via `AssignShard` command
4. Shard map updated and propagated to all nodes
5. Writes resume to new primary (target: ~1-3 second RTO)

### 16.12 Troubleshooting

#### Cluster endpoints return "standalone" mode

1. Verify license key is configured: `ARC_LICENSE_KEY=ARC-ENT-...`
2. Verify license includes `clustering` feature: check `/api/v1/schedulers` for license info
3. Verify clustering is enabled: `ARC_CLUSTER_ENABLED=true`

#### Node shows as unhealthy

1. Check node logs for errors
2. Verify network connectivity between nodes
3. Check health check interval is appropriate for your network latency
4. Increase `unhealthy_threshold` if experiencing transient failures

#### Nodes not discovering each other

1. Verify `seeds` configuration points to running nodes
2. Check `coordinator_addr` is accessible from other nodes
3. Verify `advertise_addr` is reachable (not 0.0.0.0)
4. Check firewall rules allow traffic on coordinator port (default: 9100)

### 16.13 Sharding Configuration Guide (Phase 4)

This section provides detailed configuration and deployment guidance for multi-writer sharding.

#### 16.13.1 Architecture Components

| Component | Description | File |
|-----------|-------------|------|
| **ShardMap** | Tracks shard-to-node assignments | `internal/cluster/sharding/shardmap.go` |
| **ShardRouter** | Routes requests to correct shard primary | `internal/cluster/sharding/router.go` |
| **MetaCluster** | Raft-based coordinator for shard assignments | `internal/cluster/sharding/meta.go` |
| **ShardReplicationManager** | Per-shard WAL replication (primary side) | `internal/cluster/sharding/shard_replication.go` |
| **ShardReceiverManager** | Per-shard WAL reception (replica side) | `internal/cluster/sharding/shard_receiver.go` |
| **FailoverManager** | Health monitoring and automatic failover | `internal/cluster/sharding/failover.go` |
| **ScatterGather** | Cross-shard query coordination | `internal/cluster/sharding/scatter_gather.go` |

#### 16.13.2 Deployment Example: 3-Node Cluster

This setup provides fault tolerance with Raft consensus (survives 1 node failure).

**Production Deployment (separate machines):**

```bash
# Node 1 (10.0.0.1) - Bootstrap node (ONLY node-1 should bootstrap)
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_NODE_ID=node-1 \
ARC_CLUSTER_ROLE=writer \
ARC_SERVER_PORT=8000 \
ARC_CLUSTER_COORDINATOR_ADDR=:9100 \
ARC_CLUSTER_ADVERTISE_ADDR=10.0.0.1:9100 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9200 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=10.0.0.1:9200 \
ARC_CLUSTER_RAFT_BOOTSTRAP=true \
ARC_DATA_DIR=/var/lib/arc/data \
ARC_CLUSTER_RAFT_DATA_DIR=/var/lib/arc/raft \
./arc

# Node 2 (10.0.0.2) - Joins via seeds (NO bootstrap)
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_NODE_ID=node-2 \
ARC_CLUSTER_ROLE=writer \
ARC_SERVER_PORT=8000 \
ARC_CLUSTER_COORDINATOR_ADDR=:9100 \
ARC_CLUSTER_ADVERTISE_ADDR=10.0.0.2:9100 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9200 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=10.0.0.2:9200 \
ARC_CLUSTER_SEEDS=10.0.0.1:9100 \
ARC_DATA_DIR=/var/lib/arc/data \
ARC_CLUSTER_RAFT_DATA_DIR=/var/lib/arc/raft \
./arc

# Node 3 (10.0.0.3) - Joins via seeds (NO bootstrap)
ARC_LICENSE_KEY=ARC-ENT-... \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_NODE_ID=node-3 \
ARC_CLUSTER_ROLE=writer \
ARC_SERVER_PORT=8000 \
ARC_CLUSTER_COORDINATOR_ADDR=:9100 \
ARC_CLUSTER_ADVERTISE_ADDR=10.0.0.3:9100 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9200 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=10.0.0.3:9200 \
ARC_CLUSTER_SEEDS=10.0.0.1:9100 \
ARC_DATA_DIR=/var/lib/arc/data \
ARC_CLUSTER_RAFT_DATA_DIR=/var/lib/arc/raft \
./arc
```

**Local Development (same machine, different ports):**

```bash
# Clean data directories first
rm -rf ./data/node1 ./data/node2 ./data/node3

# Terminal 1: Node 1 (bootstrap)
ARC_LICENSE_KEY=$ARC_LICENSE_KEY \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_NODE_ID=node-1 \
ARC_CLUSTER_ROLE=writer \
ARC_SERVER_PORT=8000 \
ARC_CLUSTER_COORDINATOR_ADDR=:9100 \
ARC_CLUSTER_ADVERTISE_ADDR=localhost:9100 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9200 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=localhost:9200 \
ARC_CLUSTER_RAFT_BOOTSTRAP=true \
ARC_DATA_DIR=./data/node1 \
ARC_CLUSTER_RAFT_DATA_DIR=./data/node1/raft \
./arc

# Terminal 2: Node 2 (wait for node-1 to start first)
ARC_LICENSE_KEY=$ARC_LICENSE_KEY \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_NODE_ID=node-2 \
ARC_CLUSTER_ROLE=writer \
ARC_SERVER_PORT=8001 \
ARC_CLUSTER_COORDINATOR_ADDR=:9101 \
ARC_CLUSTER_ADVERTISE_ADDR=localhost:9101 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9201 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=localhost:9201 \
ARC_CLUSTER_SEEDS=localhost:9100 \
ARC_DATA_DIR=./data/node2 \
ARC_CLUSTER_RAFT_DATA_DIR=./data/node2/raft \
./arc

# Terminal 3: Node 3 (after node-2 joins)
ARC_LICENSE_KEY=$ARC_LICENSE_KEY \
ARC_CLUSTER_ENABLED=true \
ARC_CLUSTER_NODE_ID=node-3 \
ARC_CLUSTER_ROLE=writer \
ARC_SERVER_PORT=8002 \
ARC_CLUSTER_COORDINATOR_ADDR=:9102 \
ARC_CLUSTER_ADVERTISE_ADDR=localhost:9102 \
ARC_CLUSTER_RAFT_BIND_ADDR=:9202 \
ARC_CLUSTER_RAFT_ADVERTISE_ADDR=localhost:9202 \
ARC_CLUSTER_SEEDS=localhost:9100 \
ARC_DATA_DIR=./data/node3 \
ARC_CLUSTER_RAFT_DATA_DIR=./data/node3/raft \
./arc
```

**Important Notes:**
- Only the first node (`node-1`) should have `ARC_CLUSTER_RAFT_BOOTSTRAP=true`
- Other nodes join via `ARC_CLUSTER_SEEDS` pointing to an existing cluster member
- Each node must have its own `ARC_DATA_DIR` and `ARC_CLUSTER_RAFT_DATA_DIR`
- Raft requires a quorum (majority) for leader election - with 3 nodes, 2 must be running

**Verify cluster status:**
```bash
curl -s http://localhost:8000/api/v1/cluster | jq '.raft'
# Should show all 3 nodes as voters in latest_configuration
```

#### 16.13.3 Throughput Planning

| Shards | RF | Nodes | Write Throughput | Fault Tolerance |
|--------|----|----|------------------|-----------------|
| 1 | 1 | 1 | 11M rps | None |
| 1 | 3 | 3 | 11M rps | Survives 2 failures |
| 3 | 1 | 3 | 33M rps | None |
| 3 | 3 | 9 | 33M rps | Survives 2 failures/shard |
| 10 | 3 | 30 | 110M rps | Survives 2 failures/shard |

**Formula:** `Write Throughput = num_shards × 11M rps`

#### 16.13.4 API Endpoints

```bash
# Get shard map status
curl http://localhost:8000/api/v1/cluster/shards

# Response:
{
  "num_shards": 3,
  "version": 5,
  "shards": [
    {
      "shard_id": 0,
      "primary": {"id": "node-1", "address": "10.0.0.1:9100", "state": "healthy"},
      "replicas": [
        {"id": "node-2", "address": "10.0.0.2:9100", "state": "healthy"},
        {"id": "node-3", "address": "10.0.0.3:9100", "state": "healthy"}
      ]
    },
    ...
  ]
}

# Get specific shard details
curl http://localhost:8000/api/v1/cluster/shards/0

# Trigger manual failover for a shard
curl -X POST http://localhost:8000/api/v1/cluster/shards/0/failover

# Get failover manager stats
curl http://localhost:8000/api/v1/cluster/failover/stats
```

#### 16.13.5 Testing Failover

```bash
# 1. Start 3-node sharded cluster (see deployment example above)

# 2. Write data to a database that routes to shard 0
curl -X POST http://localhost:8000/api/v1/write \
  -H "Content-Type: application/json" \
  -H "X-Arc-Database: testdb" \
  -d '{"measurement": "cpu", "tags": {"host": "server1"}, "fields": {"usage": 45.5}, "timestamp": 1704067200000000000}'

# 3. Verify which shard "testdb" routes to
curl http://localhost:8000/api/v1/cluster/shards?database=testdb

# 4. Kill the primary node for that shard (e.g., node-1)
# Wait 1-3 seconds for automatic failover

# 5. Verify new primary elected
curl http://localhost:8000/api/v1/cluster/shards/0

# 6. Verify writes still work
curl -X POST http://localhost:8000/api/v1/write \
  -H "Content-Type: application/json" \
  -H "X-Arc-Database: testdb" \
  -d '{"measurement": "cpu", "tags": {"host": "server1"}, "fields": {"usage": 52.1}, "timestamp": 1704067260000000000}'
```

#### 16.13.6 Troubleshooting Sharding

##### Writes failing with "no shard primary"

1. Check shard map: `curl http://localhost:8000/api/v1/cluster/shards`
2. Verify all nodes are healthy
3. Check if meta cluster leader is up
4. Review Arc logs for failover events

##### Cross-shard queries timing out

1. Increase scatter-gather timeout
2. Check network connectivity between nodes
3. Reduce `MaxConcurrentShards` if overwhelming nodes

##### Replication lag increasing

1. Check network bandwidth between primary and replicas
2. Increase replication buffer size
3. Adjust batch size for higher throughput
4. Monitor replica node resource usage

##### Failover not triggering

1. Verify `HealthCheckInterval` is appropriate
2. Check `UnhealthyThreshold` setting
3. Ensure meta cluster has a leader
4. Review failover manager stats

---

## 17. Tiered Storage Configuration Guide (Enterprise)

This section documents how to configure and use tiered storage for automatic data lifecycle management with a simple 2-tier system: Hot (local) → Cold (S3/Azure archive).

### 17.1 Overview

Tiered storage enables automatic data lifecycle management with two tiers:

```
HOT (Local/Main Storage)  →  COLD (S3 Glacier / Azure Archive)
     Recent data                Historical data
     Fast queries               Lowest cost
```

**Design Philosophy:** Simple and effective. One threshold (`default_hot_max_age_days`) determines when data moves from hot to cold. No intermediate tiers to configure.

**License Requirement:** Tiered storage requires an enterprise license with the `tiered_storage` feature enabled.

### 17.2 How It Works

1. **File Tracking**: All data files are tracked in SQLite with their tier, partition time, and metadata
2. **Age-Based Migration**: Files migrate based on partition age vs configured threshold
3. **Scheduled Execution**: A cron scheduler runs migrations automatically (default: 2am daily)
4. **Zero Recompression**: Files move as-is between tiers - no re-encoding overhead
5. **Per-Database Policies**: Override global threshold per database via API
6. **Query Routing**: Queries automatically route to the correct tier(s) based on time range

### 17.3 Configuration

Cold tier has its own S3/Azure credentials configured directly in the `[tiered_storage.cold]` section.

#### Environment Variables

**Complete Reference:**

```bash
# =============================================================================
# TIERED STORAGE - MAIN SETTINGS
# =============================================================================
ARC_TIERED_STORAGE_ENABLED=true                      # Enable tiered storage (requires license)
ARC_TIERED_STORAGE_MIGRATION_SCHEDULE="0 2 * * *"    # Cron schedule (default: 2am daily)
ARC_TIERED_STORAGE_MIGRATION_MAX_CONCURRENT=4        # Max concurrent file migrations
ARC_TIERED_STORAGE_MIGRATION_BATCH_SIZE=100          # Files per migration batch

# Single threshold: data older than this moves from hot to cold
ARC_TIERED_STORAGE_DEFAULT_HOT_MAX_AGE_DAYS=30       # Hot → Cold after N days

# =============================================================================
# COLD TIER CONFIGURATION (S3/Azure archive storage)
# =============================================================================
ARC_TIERED_STORAGE_COLD_ENABLED=true
ARC_TIERED_STORAGE_COLD_BACKEND=s3                   # "s3" or "azure"

# S3 Configuration
ARC_TIERED_STORAGE_COLD_S3_BUCKET=arc-archive        # Required for S3
ARC_TIERED_STORAGE_COLD_S3_REGION=us-east-1
ARC_TIERED_STORAGE_COLD_S3_ENDPOINT=                 # For MinIO (leave empty for AWS)
ARC_TIERED_STORAGE_COLD_S3_ACCESS_KEY=               # Use env var for security
ARC_TIERED_STORAGE_COLD_S3_SECRET_KEY=               # Use env var for security
ARC_TIERED_STORAGE_COLD_S3_USE_SSL=true              # Use HTTPS (recommended)
ARC_TIERED_STORAGE_COLD_S3_PATH_STYLE=false          # Set true for MinIO
ARC_TIERED_STORAGE_COLD_S3_STORAGE_CLASS=GLACIER     # GLACIER, DEEP_ARCHIVE, GLACIER_IR, STANDARD_IA
ARC_TIERED_STORAGE_COLD_RETRIEVAL_MODE=standard      # standard, expedited, bulk

# Azure Configuration (alternative to S3)
ARC_TIERED_STORAGE_COLD_AZURE_CONTAINER=arc-archive
ARC_TIERED_STORAGE_COLD_AZURE_CONNECTION_STRING=     # Use env var for security
ARC_TIERED_STORAGE_COLD_AZURE_ACCOUNT_NAME=
ARC_TIERED_STORAGE_COLD_AZURE_ACCOUNT_KEY=
ARC_TIERED_STORAGE_COLD_AZURE_SAS_TOKEN=
ARC_TIERED_STORAGE_COLD_AZURE_ENDPOINT=              # For Azurite (leave empty for Azure)
ARC_TIERED_STORAGE_COLD_AZURE_USE_MANAGED_IDENTITY=false
ARC_TIERED_STORAGE_COLD_AZURE_ACCESS_TIER=Archive    # Hot, Cool, Archive
```

**Minimal Configuration Examples:**

```bash
# Example 1: Local hot, S3 Glacier cold
ARC_TIERED_STORAGE_ENABLED=true
ARC_TIERED_STORAGE_DEFAULT_HOT_MAX_AGE_DAYS=30
ARC_TIERED_STORAGE_COLD_ENABLED=true
ARC_TIERED_STORAGE_COLD_BACKEND=s3
ARC_TIERED_STORAGE_COLD_S3_BUCKET=arc-archive
ARC_TIERED_STORAGE_COLD_S3_REGION=us-east-1
ARC_TIERED_STORAGE_COLD_S3_ACCESS_KEY=AKIA...
ARC_TIERED_STORAGE_COLD_S3_SECRET_KEY=secret...

# Example 2: Local hot, Azure Archive cold
ARC_TIERED_STORAGE_ENABLED=true
ARC_TIERED_STORAGE_DEFAULT_HOT_MAX_AGE_DAYS=30
ARC_TIERED_STORAGE_COLD_ENABLED=true
ARC_TIERED_STORAGE_COLD_BACKEND=azure
ARC_TIERED_STORAGE_COLD_AZURE_CONTAINER=arc-archive
ARC_TIERED_STORAGE_COLD_AZURE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...

# Example 3: Local hot, MinIO cold (development)
ARC_TIERED_STORAGE_ENABLED=true
ARC_TIERED_STORAGE_DEFAULT_HOT_MAX_AGE_DAYS=1
ARC_TIERED_STORAGE_COLD_ENABLED=true
ARC_TIERED_STORAGE_COLD_BACKEND=s3
ARC_TIERED_STORAGE_COLD_S3_BUCKET=arc-archive
ARC_TIERED_STORAGE_COLD_S3_ENDPOINT=localhost:9000
ARC_TIERED_STORAGE_COLD_S3_ACCESS_KEY=minioadmin
ARC_TIERED_STORAGE_COLD_S3_SECRET_KEY=minioadmin
ARC_TIERED_STORAGE_COLD_S3_USE_SSL=false
ARC_TIERED_STORAGE_COLD_S3_PATH_STYLE=true
```

#### arc.toml Configuration

```toml
[tiered_storage]
enabled = true
migration_schedule = "0 2 * * *"        # 2am daily
migration_max_concurrent = 4
migration_batch_size = 100

# Single threshold: data older than this moves from hot to cold
default_hot_max_age_days = 30

[tiered_storage.cold]
enabled = true
backend = "s3"

# S3 Configuration
s3_bucket = "arc-archive"
s3_region = "us-east-1"
# s3_endpoint = ""                      # For MinIO
# s3_access_key = ""                    # Use env: ARC_TIERED_STORAGE_COLD_S3_ACCESS_KEY
# s3_secret_key = ""                    # Use env: ARC_TIERED_STORAGE_COLD_S3_SECRET_KEY
s3_use_ssl = true
s3_path_style = false
s3_storage_class = "GLACIER"            # GLACIER, DEEP_ARCHIVE, GLACIER_IR, STANDARD_IA

# Azure Configuration (alternative to S3)
# azure_container = "arc-archive"
# azure_connection_string = ""          # Use env: ARC_TIERED_STORAGE_COLD_AZURE_CONNECTION_STRING
# azure_account_name = ""
# azure_account_key = ""
# azure_use_managed_identity = false
azure_access_tier = "Archive"           # Hot, Cool, Archive

# Retrieval settings (for Glacier/Archive)
retrieval_mode = "standard"             # standard, expedited, bulk
```

### 17.4 API Endpoints

All tiering endpoints are under `/api/v1/tiering/` and require a valid enterprise license.

#### Get Tiering Status

```bash
curl http://localhost:8000/api/v1/tiering/status
```

**Response:**
```json
{
  "enabled": true,
  "scheduler": {
    "running": true,
    "schedule": "0 2 * * *",
    "next_run": "2026-01-24T02:00:00Z",
    "last_run": "2026-01-23T02:00:00Z"
  },
  "tiers": {
    "hot": {"enabled": true, "backend": "local"},
    "cold": {"enabled": true, "backend": "s3", "bucket": "arc-archive"}
  }
}
```

#### List Files by Tier

```bash
# All files
curl http://localhost:8000/api/v1/tiering/files

# Filter by tier (hot or cold)
curl "http://localhost:8000/api/v1/tiering/files?tier=cold"

# Filter by database
curl "http://localhost:8000/api/v1/tiering/files?database=production"

# With limit
curl "http://localhost:8000/api/v1/tiering/files?limit=50"
```

**Response:**
```json
{
  "files": [
    {
      "id": 1,
      "path": "mydb/cpu/2026/01/15/data.parquet",
      "database": "mydb",
      "measurement": "cpu",
      "partition_time": "2026-01-15T00:00:00Z",
      "tier": "hot",
      "size_bytes": 1048576,
      "created_at": "2026-01-15T10:30:00Z",
      "migrated_at": null
    }
  ],
  "count": 1
}
```

#### Trigger Manual Migration

```bash
# Run full migration cycle (hot → cold)
curl -X POST http://localhost:8000/api/v1/tiering/migrate
```

**Response:**
```json
{
  "message": "Migration completed successfully",
  "status": "completed"
}
```

#### Get Migration Statistics

```bash
curl http://localhost:8000/api/v1/tiering/stats
```

**Response:**
```json
{
  "tier_stats": {
    "hot": {"tier": "hot", "file_count": 150, "total_size_mb": 2048},
    "cold": {"tier": "cold", "file_count": 2000, "total_size_mb": 32768}
  },
  "recent_migrations": [
    {
      "id": 1,
      "file_path": "mydb/cpu/2026/01/10/data.parquet",
      "database": "mydb",
      "from_tier": "hot",
      "to_tier": "cold",
      "size_bytes": 1048576,
      "started_at": "2026-01-23T02:00:15Z",
      "completed_at": "2026-01-23T02:00:18Z",
      "error": ""
    }
  ]
}
```

### 17.5 Per-Database Policies API

Override global threshold per database or exclude databases from tiering entirely.

#### List All Policies

```bash
curl http://localhost:8000/api/v1/tiering/policies
```

**Response:**
```json
{
  "policies": [
    {
      "id": 1,
      "database": "realtime",
      "hot_only": true,
      "hot_max_age_days": null,
      "created_at": "2026-01-20T10:00:00Z",
      "updated_at": "2026-01-20T10:00:00Z"
    },
    {
      "id": 2,
      "database": "metrics",
      "hot_only": false,
      "hot_max_age_days": 7,
      "created_at": "2026-01-21T14:30:00Z",
      "updated_at": "2026-01-21T14:30:00Z"
    }
  ],
  "count": 2
}
```

#### Get Policy for Database

```bash
curl http://localhost:8000/api/v1/tiering/policies/metrics
```

#### Set/Update Policy

```bash
# Set custom threshold for a database (move to cold after 7 days)
curl -X PUT http://localhost:8000/api/v1/tiering/policies/metrics \
  -H "Content-Type: application/json" \
  -d '{"hot_max_age_days": 7}'

# Exclude database from tiering (keep all data in hot tier)
curl -X PUT http://localhost:8000/api/v1/tiering/policies/realtime \
  -H "Content-Type: application/json" \
  -d '{"hot_only": true}'
```

**Response:**
```json
{
  "message": "Policy updated successfully",
  "database": "metrics",
  "policy": {
    "database": "metrics",
    "hot_only": false,
    "hot_max_age_days": 7
  }
}
```

#### Delete Policy (Revert to Global Defaults)

```bash
curl -X DELETE http://localhost:8000/api/v1/tiering/policies/metrics
```

#### Get Effective Policy

Shows the resolved policy (custom value or global default):

```bash
curl http://localhost:8000/api/v1/tiering/policies/metrics/effective
```

**Response:**
```json
{
  "database": "metrics",
  "hot_only": false,
  "hot_max_age_days": 7,
  "source": "custom"
}
```

### 17.6 Common Use Cases

#### Use Case 1: Aggressive Tiering for Cost Savings

Move data to cold tier quickly:

```bash
# Set aggressive global default (archive after 7 days)
ARC_TIERED_STORAGE_DEFAULT_HOT_MAX_AGE_DAYS=7
```

#### Use Case 2: Keep Critical Data Hot

Exclude real-time dashboards from tiering:

```bash
curl -X PUT http://localhost:8000/api/v1/tiering/policies/realtime_metrics \
  -H "Content-Type: application/json" \
  -d '{"hot_only": true}'
```

#### Use Case 3: Different Retention per Database

```bash
# Short retention for debug logs (cold after 1 day)
curl -X PUT http://localhost:8000/api/v1/tiering/policies/debug_logs \
  -H "Content-Type: application/json" \
  -d '{"hot_max_age_days": 1}'

# Long retention for audit data (cold after 90 days)
curl -X PUT http://localhost:8000/api/v1/tiering/policies/audit_logs \
  -H "Content-Type: application/json" \
  -d '{"hot_max_age_days": 90}'
```

#### Use Case 4: Hot-Only Mode (Disable Cold Tier)

Disable cold tier to keep all data in hot storage:

```toml
[tiered_storage.cold]
enabled = false
```

### 17.7 Startup Behavior

#### Without License

```
WRN Tiered storage requires enterprise license - disabled
```

Tiering API endpoints will return:
```json
{
  "enabled": false,
  "reason": "Enterprise license with tiered_storage feature required"
}
```

#### With Valid License

```
INF Tiered storage enabled
INF Tiering scheduler started schedule="0 2 * * *"
INF Loaded 3 tiering policies into cache
```

### 17.8 Troubleshooting

#### Migration not running

1. Verify license: `curl http://localhost:8000/api/v1/tiering/status`
2. Check scheduler is running and next_run time
3. Review logs for migration errors
4. Verify cold tier credentials are valid

#### Files not migrating

1. Check if database has `hot_only=true` policy
2. Verify file age exceeds threshold (`default_hot_max_age_days`)
3. Check cold tier backend is accessible
4. Review migration history in stats endpoint

#### Query returning incomplete results

1. Verify query time range covers correct tiers
2. Check if data exists in expected tier: `curl /api/v1/tiering/files?database=X`
3. For cold tier (Glacier), ensure retrieval is complete (Glacier has delays)

#### Credential errors

Verify cold tier credentials:
1. Check `tiered_storage.cold.s3_*` credentials are valid
2. IAM permissions allow read/write to the cold tier bucket
3. For Azure, connection string or managed identity works for the container

### 17.9 Cost Optimization Tips

1. **Start with generous thresholds**: Use 30-day default, then optimize based on access patterns
2. **Use GLACIER_IR for cold tier**: Instant retrieval Glacier balances cost and access speed
3. **Monitor access patterns**: Adjust threshold based on actual query patterns
4. **Exclude frequently queried databases**: Use `hot_only=true` for dashboards
5. **Consider S3 lifecycle rules**: Combine with S3 lifecycle for additional cost optimization

---

## 18. Appendix: Competitive Analysis

### 18.1 InfluxDB
- **Model**: Cloud (usage) or Enterprise (node-based)
- **Enterprise pricing**: ~$1,500/node/month
- **Strengths**: Market leader, ecosystem
- **Weaknesses**: Performance, complexity, cost

### 18.2 TimescaleDB
- **Model**: Cloud (usage) or Self-hosted (free)
- **Enterprise pricing**: N/A (cloud only for enterprise features)
- **Strengths**: PostgreSQL compatibility
- **Weaknesses**: Performance at scale, limited enterprise features

### 18.3 ClickHouse Cloud
- **Model**: Usage-based (compute + storage)
- **Pricing**: ~$0.30/GB storage, $0.06/vCPU-hour
- **Strengths**: Performance, scale
- **Weaknesses**: Complexity, not time-series optimized

### 18.4 QuestDB Enterprise
- **Model**: Core-based
- **Pricing**: ~$500/core/month (estimated)
- **Strengths**: Performance
- **Weaknesses**: Limited ecosystem, small company

---

*Document Version: 1.7*
*Last Updated: January 23, 2026*
*Phase 2 Complete - Tiered storage (hot/cold 2-tier system) with per-database policies implemented*
