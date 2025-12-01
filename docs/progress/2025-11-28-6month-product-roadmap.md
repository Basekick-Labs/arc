# Arc Product Roadmap - 6 Month Plan (Jan 2026 - June 2026)

## Executive Summary

Arc is a high-performance time-series database targeting IoT/Industrial, Smart Cities, Observability, and Financial use cases. v25.12.1 shipped the Go rewrite (from Python) with 125% faster ingestion. This roadmap prepares for **Cloud Beta in Q1 2026** while continuing to strengthen the Core (OSS) product.

**Current Version**: v25.12.1 (Go rewrite, production-ready)
**Release Cadence**: Monthly releases (v26.01 → v26.06)
**Cloud Beta Target**: Q1 2026 (stable beta, not GA)

---

## Strategic Priorities

1. **Cloud Beta Readiness** - S3 completion, deployment architecture
2. **IoT/Industrial Focus** - MQTT ingestion for edge devices
3. **Developer Experience** - Official SDKs (Python, Go, JS)
4. **Cloud Control Center** - Web dashboard for instance management
5. **Scale for Cloud** - Read/write replicas for H2 enterprise readiness

---

## Product Tiers

| Feature | OSS (Free) | Cloud | Enterprise |
|---------|------------|-------|------------|
| Core ingestion (MessagePack, Line Protocol) | ✓ | ✓ | ✓ |
| MQTT ingestion | ✓ | ✓ | ✓ |
| S3/MinIO backend | ✓ | ✓ | ✓ |
| Azure Blob Storage backend | ✓ | ✓ | ✓ |
| Basic token auth | ✓ | ✓ | ✓ |
| SDKs (Python, Go, JS) | ✓ | ✓ | ✓ |
| Bulk import | ✓ | ✓ | ✓ |
| RBAC (reader/writer roles) | - | ✓ | ✓ |
| Multi-tenancy / quotas | - | ✓ | ✓ |
| Encryption at rest | - | ✓ | ✓ |
| Audit logging | - | ✓ | ✓ |
| Control Center | - | ✓ | ✓ |
| Read/Write replicas | - | ✓ | ✓ |
| On-premise deployment | ✓ | - | ✓ |
| Managed infrastructure | - | ✓ | - |

**Note**: Cloud and Enterprise share the same feature set. Cloud = managed by Basekick. Enterprise = self-hosted with commercial license (if customers need it).

---

## Cloud Architecture (Decided)

**Deployment Model**: Dedicated containers per customer
- Stronger isolation (no noisy neighbors)
- Simpler billing per instance
- Supports dev/prod separation per customer
- Resource bounds per container

**Storage Backend**: MinIO on your metal (S3-compatible)
- No external dependencies
- Full S3 API compatibility
- Add GCS/R2 only when customers explicitly request

**Control Center Stack**: Admin template (React/Vue)
- Accelerates UI/UX development
- Reduces Month 3 scope risk

---

## Month 1: January 2026 (v26.01)
### Theme: **Cloud Beta Foundation**

**OSS Features:** ✅ **COMPLETED December 1, 2025** (ahead of schedule!)
- [x] Complete S3 delete operations (currently skipped with warning)
- [x] Implement S3-compatible SHOW/LIST commands (pagination-based)
- [x] S3 retention policy execution support
- [x] S3 continuous query support
- [x] Azure Blob Storage backend (customer requested)
  - [x] Connection string / SAS token / Managed Identity authentication
  - [x] Delete, SHOW/LIST, retention, CQ support (parity with S3)
- [x] Arrow writer: handle row-to-columnar conversion edge case

**Cloud/Enterprise Features:**
- [ ] Basic RBAC: reader/writer roles (beyond current token auth)
- [ ] Define container deployment architecture (Kubernetes manifests)
- [ ] Per-instance resource limits configuration
- [ ] Instance health/metrics endpoint for control center

**Why Now**: S3 completion for OSS + RBAC/deployment for Cloud Beta.

**Estimated Effort**: ~~High~~ OSS features done early, Cloud features remain

---

## Month 2: February 2026 (v26.02)
### Theme: **MQTT Ingestion & Python SDK**

**OSS Features:**
- [ ] MQTT subscriber for IoT edge devices
  - Configurable broker connection
  - Topic-to-measurement mapping
  - QoS support (0, 1, 2)
  - TLS/authentication support
- [ ] Official Python SDK (pip installable)
  - MessagePack columnar ingestion
  - Query with Arrow response support
  - Async support for high-throughput scenarios
  - Pandas/Polars DataFrame integration

**Why Now**: MQTT is critical for IoT/Industrial customers (your #1 vertical). Python SDK enables data engineers and IoT developers.

**Estimated Effort**: High

---

## Month 3: March 2026 (v26.03)
### Theme: **Cloud Control Center MVP & Bulk Import**

**OSS Features:**
- [ ] Bulk import API (`/api/v1/import/csv`, `/api/v1/import/parquet`)
- [ ] Migration guides from InfluxDB, QuestDB, TimescaleDB, ClickHouse

**Cloud/Enterprise Features:**
- [ ] Web Dashboard (Control Center) - MVP
  - Instance list with health status
  - Resource utilization metrics (CPU, memory, disk, ingestion rate)
  - Basic query interface (not full Grafana replacement)
  - Token/user management UI
  - Alert configuration (threshold-based)

**Why Now**: Control Center is essential for Cloud operations. Bulk import enables customer migrations from competing TSDBs.

**Estimated Effort**: High

---

## Month 4: April 2026 (v26.04)
### Theme: **Go & JavaScript SDKs**

**OSS Features:**
- [ ] Official Go SDK (go get installable)
  - Zero-copy Arrow support
  - Connection pooling
  - Retry with exponential backoff
- [ ] Official JavaScript/TypeScript SDK (npm installable)
  - Browser and Node.js support
  - Streaming query results
  - TypeScript types
- [ ] Slow query logging with configurable threshold

**Cloud/Enterprise Features:**
- [ ] Query history/audit endpoint (`/api/v1/queries/history`)

**Why Now**: Go for backend integrations (IoT gateways), JS for Control Center and web apps.

**Estimated Effort**: High

---

## Month 5: May 2026 (v26.05)
### Theme: **Quotas, Billing & Read Replicas (Foundation)**

**Cloud/Enterprise Features:**
- [ ] Per-instance quotas (ingestion rate, storage, query concurrency)
- [ ] Rate limiting middleware with per-token limits
- [ ] Billing metrics export (usage tracking)
- [ ] Admin API for instance management
- [ ] Read replicas - foundation architecture
  - Replication protocol design
  - Leader election / failover strategy
  - Consistency model (eventual vs strong)

**Why Now**: Required for Cloud scale and billing. Read replicas foundation for H2 enterprise readiness.

**Estimated Effort**: High

---

## Month 6: June 2026 (v26.06)
### Theme: **Read/Write Replicas & Encryption**

**Cloud/Enterprise Features:**
- [ ] Read replicas - implementation
  - Streaming replication from leader
  - Automatic failover
  - Load balancing for read queries
- [ ] Write replicas (multi-leader) - foundation
  - Conflict resolution strategy
  - Geo-distribution considerations
- [ ] Encryption at rest (Parquet encryption support) - optional per-instance
- [ ] Key management API (rotation, per-instance keys)
- [ ] Audit logging for all administrative actions
- [ ] `/api/v1/admin/audit-log` endpoint with filtering

**Why Now**: Read/write replicas enable Cloud scale and prepare for H2 enterprise. Encryption enables medical/healthcare customers.

**Estimated Effort**: High

---

## H2 2026: Enterprise Readiness

If customer demand exists, H2 2026 focuses on:
- [ ] Enterprise licensing and packaging
- [ ] Write replicas - full implementation (multi-leader)
- [ ] Horizontal sharding strategy
- [ ] GCS / Cloudflare R2 backends (on-demand)
- [ ] Compliance certifications (SOC2, HIPAA)
- [ ] Enterprise support SLAs

---

## Backlog (Future Releases)

### Performance & Scale
- Tiered storage (hot/warm/cold with automatic archival)
- Query result caching (configurable TTL)
- `/api/v1/query/explain` endpoint for query plans

### Integrations
- Kafka consumer for streaming ingestion
- OpenTelemetry instrumentation
- Terraform provider
- Kubernetes operator

### Advanced Analytics
- Time-series specific UDFs (FILL, INTERPOLATE, MOVING_AVERAGE)
- Anomaly detection algorithms
- Forecasting capabilities

---

## Cloud Beta Checklist (Q1 2026)

Must-have for beta launch (January):
- [x] S3 backend complete (delete, SHOW, retention, CQ) ✅ **Done Dec 1, 2025**
- [x] Azure Blob Storage backend ✅ **Done Dec 1, 2025** (bonus!)
- [ ] Basic RBAC (reader/writer roles)
- [ ] Container deployment architecture
- [ ] Instance health metrics

Nice-to-have for beta (Feb-March):
- [ ] MQTT ingestion
- [ ] Python SDK
- [ ] Control Center MVP

---

## Technical Debt to Address Incrementally

| Item | Priority | Notes |
|------|----------|-------|
| ~~S3 backend gaps~~ | ~~High~~ | ✅ **Resolved Dec 1, 2025** |
| ~~Arrow writer row-to-columnar~~ | ~~High~~ | ✅ **Resolved Dec 1, 2025** |
| Large file refactoring (arrow_writer.go) | Medium | As features touch these files |
| Additional integration tests | Medium | Add with each new feature |

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Cloud Beta Users | 5-10 by end of Q1 2026 |
| SDK Downloads (PyPI, npm, Go) | 1,000+ monthly by June 2026 |
| GitHub Stars | 500+ by Cloud GA |
| Production Deployments (OSS) | 50+ tracked installations |

---

## Resource Considerations

- **MQTT**: May require IoT domain expertise for proper QoS/reliability
- **Control Center**: Frontend development (React/Vue) or consider existing admin templates
- **SDKs**: Consider hiring/contracting SDK specialists or engaging community
- **Security (Encryption)**: May need security review before enabling for medical customers

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Cloud Beta delays | Core features (S3, RBAC) in Month 1; Control Center can slip to Month 4 |
| MQTT complexity | Start with basic broker support, add advanced features post-launch |
| SDK scope creep | Define MVP feature set per SDK, iterate post-launch |
| Encryption performance | Make optional, benchmark before GA |
| Control Center scope | MVP = instance list + metrics + tokens; full features in later releases |
