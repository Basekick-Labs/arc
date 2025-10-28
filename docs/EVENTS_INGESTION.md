# Events Ingestion with Arc

Arc completes the observability suite with high-throughput events ingestion. Events represent **state changes** and **significant actions** in your infrastructure, applications, and business operations.

This document describes Arc's events ingestion capabilities, performance benchmarks, and how to use Arc for unified observability with metrics, logs, traces, AND events.

## Table of Contents

- [Overview](#overview)
- [Why Use Arc for Events?](#why-use-arc-for-events)
- [Performance Benchmarks](#performance-benchmarks)
- [Understanding Events](#understanding-events)
- [Protocol Specification](#protocol-specification)
- [Sending Events to Arc](#sending-events-to-arc)
- [Event Types and Categories](#event-types-and-categories)
- [Querying Events](#querying-events)
- [Correlating Observability Data](#correlating-observability-data)
- [Load Testing](#load-testing)

---

## Overview

Arc provides **938,000+ events/second** ingestion using the MessagePack columnar protocol. This completes the observability suite:

- ✅ **Metrics**: 2.45M/sec - What is the state?
- ✅ **Logs**: 955K/sec - What is happening?
- ✅ **Traces**: 944K/sec - How are requests flowing?
- ✅ **Events**: 938K/sec - What changed?

**One endpoint. One protocol. Complete unified observability.**

---

## Why Use Arc for Events?

### The Four Pillars of Observability

Events are the **missing piece** that explains WHY your system behaves the way it does:

```
Traditional Observability (3 Pillars):
├── Metrics  → "CPU spiked to 95% at 14:32"
├── Logs     → "500 errors logged at 14:32"
└── Traces   → "Request latency increased at 14:32"

❓ Question: WHY did all these happen at 14:32?

Complete Observability (4 Pillars):
├── Metrics  → "CPU spiked to 95% at 14:32"
├── Logs     → "500 errors logged at 14:32"
├── Traces   → "Request latency increased at 14:32"
└── Events   → "Deployment started at 14:32" ✅ ROOT CAUSE!
```

### Key Benefits

1. **Root Cause Analysis**: Events explain anomalies in metrics, logs, and traces
2. **Unified Storage**: All four pillars in the same Parquet files
3. **Correlated Queries**: JOIN events with metrics/logs/traces by timestamp
4. **Business Intelligence**: Track business events alongside technical telemetry
5. **Audit Trail**: Complete record of changes and actions

---

## Performance Benchmarks

### Throughput Performance

Arc achieves **938,177 events/second** on standard hardware (14-core MacBook Pro):

| Batch Size | Throughput | p50 Latency | p99 Latency | Use Case |
|------------|------------|-------------|-------------|----------|
| **20,000 events** | **938K events/sec** | 154.8ms | 2010.5ms | Maximum throughput |
| **10,000 events** | **489K events/sec** | 26.0ms | 413.1ms | Balanced |
| **5,000 events** | ~250K events/sec* | ~15ms* | ~200ms* | Low latency |
| **1,000 events** | ~50K events/sec* | ~8ms* | ~50ms* | Real-time |

*Estimated based on batch size scaling

### Hardware Utilization

During the 938K events/sec test:
- **CPU**: 85-90% peak utilization
- **Memory**: 16-18 GB
- **Duration**: 60 seconds
- **Total Events**: 60 million
- **Errors**: 0 (100% success rate)

### Test Configuration

- **Hardware**: 14-core Apple Silicon, 36 GB RAM
- **Arc Workers**: 42 Gunicorn workers
- **Test Workers**: 600 concurrent async workers
- **Protocol**: MessagePack columnar format + gzip compression
- **Storage**: Local Parquet files

---

## Understanding Events

### What are Events?

**Events** represent discrete occurrences that signify **state changes** or **significant actions**:

| Category | Examples |
|----------|----------|
| **Infrastructure** | Deployments, scaling, failovers, restarts, config changes |
| **Business** | User signups, payments, subscriptions, orders, feature flags |
| **Security** | Auth attempts, rate limits, violations, suspicious activity |
| **Application** | Job completions, circuit breakers, health checks |

### Events vs Logs

Many people confuse events and logs:

**Logs** are verbose, text-based records:
```
2025-10-28 14:32:15 INFO Starting deployment of version v1.2.3
2025-10-28 14:32:16 INFO Pulling Docker image: myapp:v1.2.3
2025-10-28 14:32:20 INFO Creating 5 new containers
2025-10-28 14:32:25 INFO Waiting for health checks...
2025-10-28 14:35:10 INFO Deployment completed successfully
```

**Events** are structured, high-level state changes:
```python
{
    "time": 1730134935000,
    "event_type": "deployment_started",
    "version": "v1.2.3",
    "replicas": 5,
    "environment": "production"
}
{
    "time": 1730135110000,
    "event_type": "deployment_completed",
    "version": "v1.2.3",
    "duration_ms": 175000,
    "success": true
}
```

**Events are queryable. Logs are searchable.**

---

## Protocol Specification

### MessagePack Columnar Format

Arc uses a columnar data format for maximum compression and write throughput:

```python
{
    "m": "system_events",  # measurement/table name
    "columns": {
        "time": [timestamp1, timestamp2, ...],              # millisecond timestamps
        "event_type": ["deployment_started", "user_signup", ...],
        "event_category": ["infrastructure", "business", ...],
        "severity": ["info", "warning", "error", "critical"],
        "source": ["kubernetes", "app-backend", ...],
        "environment": ["production", "staging", ...],
        "user_id": ["user-123", "", ...],                   # Optional, empty if not applicable
        "resource_id": ["deployment-456", "order-789", ...],
        "metadata": ['{"version":"v1.2.3"}', '{"plan":"premium"}', ...],  # JSON string
        "duration_ms": [0, 5000, ...],                      # 0 for instantaneous events
        "success": [true, false, ...],
        "amount": [0.0, 29.99, ...]                         # For business events with monetary values
    }
}
```

### Field Types

- **time**: int64 millisecond timestamps (required)
- **event_type**: string (deployment_started, user_signup, etc.)
- **event_category**: string (infrastructure, business, security, application)
- **severity**: string (info, warning, error, critical)
- **source**: string (service or system that generated the event)
- **environment**: string (production, staging, development)
- **user_id**: string (optional, for user-related events)
- **resource_id**: string (optional, deployment ID, order ID, etc.)
- **metadata**: string (JSON-encoded additional context)
- **duration_ms**: int64 (event duration, 0 for instant events)
- **success**: boolean (whether the event succeeded)
- **amount**: float (for business events with monetary values)

### Compression

Events compress well due to repetitive event types and categories:

- **Uncompressed**: ~80-100 bytes/event
- **Compressed**: ~16-32 bytes/event (with gzip)
- **Compression ratio**: 3-5x
- **Batch size**: 10,000 events = ~162 KB compressed

---

## Sending Events to Arc

### Python Example

```python
import msgpack
import gzip
import requests
import time
import json

def send_events_to_arc(events, arc_url="http://localhost:8000",
                       database="events", token="your-api-token"):
    """
    Send events to Arc using MessagePack columnar format

    Args:
        events: List of event dictionaries
        arc_url: Arc server URL
        database: Database name
        token: API authentication token
    """
    # Convert to columnar format
    payload = {
        "m": "system_events",
        "columns": {
            "time": [event["timestamp"] for event in events],
            "event_type": [event["event_type"] for event in events],
            "event_category": [event["category"] for event in events],
            "severity": [event.get("severity", "info") for event in events],
            "source": [event["source"] for event in events],
            "environment": [event.get("environment", "production") for event in events],
            "user_id": [event.get("user_id", "") for event in events],
            "resource_id": [event.get("resource_id", "") for event in events],
            "metadata": [json.dumps(event.get("metadata", {})) for event in events],
            "duration_ms": [event.get("duration_ms", 0) for event in events],
            "success": [event.get("success", True) for event in events],
            "amount": [event.get("amount", 0.0) for event in events],
        }
    }

    # Serialize and compress
    payload_binary = msgpack.packb(payload)
    payload_compressed = gzip.compress(payload_binary)

    # Send to Arc
    response = requests.post(
        f"{arc_url}/api/v1/write/msgpack",
        data=payload_compressed,
        headers={
            "Content-Type": "application/msgpack",
            "Content-Encoding": "gzip",
            "Authorization": f"Bearer {token}",
            "X-Arc-Database": database
        }
    )

    return response


# Example: Deployment events
events = [
    # Deployment started
    {
        "timestamp": int(time.time() * 1000),
        "event_type": "deployment_started",
        "category": "infrastructure",
        "severity": "info",
        "source": "kubernetes",
        "environment": "production",
        "resource_id": "deployment-abc123",
        "metadata": {
            "version": "v1.2.3",
            "replicas": 5,
            "strategy": "rolling"
        },
        "success": True
    },
    # Deployment completed (3 minutes later)
    {
        "timestamp": int(time.time() * 1000) + 180000,
        "event_type": "deployment_completed",
        "category": "infrastructure",
        "severity": "info",
        "source": "kubernetes",
        "environment": "production",
        "resource_id": "deployment-abc123",
        "metadata": {
            "version": "v1.2.3",
            "replicas": 5
        },
        "duration_ms": 180000,  # 3 minutes
        "success": True
    }
]

response = send_events_to_arc(events)
print(f"Status: {response.status_code}")
```

### Business Events Example

```python
# User signup event
signup_event = {
    "timestamp": int(time.time() * 1000),
    "event_type": "user_signup",
    "category": "business",
    "severity": "info",
    "source": "app-backend",
    "user_id": "user-12345",
    "metadata": {
        "source": "web",
        "referral": "google"
    },
    "success": True
}

# Payment processed event
payment_event = {
    "timestamp": int(time.time() * 1000),
    "event_type": "payment_processed",
    "category": "business",
    "severity": "info",
    "source": "payment-service",
    "user_id": "user-12345",
    "resource_id": "payment-xyz789",
    "metadata": {
        "payment_method": "card",
        "currency": "USD"
    },
    "amount": 29.99,
    "success": True
}

send_events_to_arc([signup_event, payment_event])
```

### Security Events Example

```python
# Failed authentication
auth_failed = {
    "timestamp": int(time.time() * 1000),
    "event_type": "authentication_failed",
    "category": "security",
    "severity": "warning",
    "source": "auth-service",
    "user_id": "user-99999",
    "metadata": {
        "source_ip": "192.168.1.100",
        "user_agent": "curl/7.68.0",
        "reason": "invalid_password"
    },
    "success": False
}

# Rate limit exceeded
rate_limit = {
    "timestamp": int(time.time() * 1000),
    "event_type": "rate_limit_exceeded",
    "category": "security",
    "severity": "error",
    "source": "api-gateway",
    "user_id": "user-88888",
    "metadata": {
        "source_ip": "203.0.113.42",
        "requests_per_minute": 1000,
        "limit": 100
    },
    "success": False
}

send_events_to_arc([auth_failed, rate_limit])
```

---

## Event Types and Categories

### Infrastructure Events

Track system-level changes and operations:

- `deployment_started`, `deployment_completed`, `deployment_failed`
- `auto_scale_up`, `auto_scale_down`
- `server_restart`, `server_shutdown`
- `config_change`, `certificate_renewed`
- `database_failover`, `cache_invalidated`

### Business Events

Track user actions and business operations:

- `user_signup`, `user_login`, `user_logout`
- `subscription_created`, `subscription_upgraded`, `subscription_cancelled`
- `payment_processed`, `payment_failed`, `refund_issued`
- `order_placed`, `order_completed`, `order_cancelled`
- `feature_flag_toggled`, `ab_test_started`

### Security Events

Track security-related activities:

- `authentication_success`, `authentication_failed`
- `authorization_denied`, `privilege_escalation`
- `rate_limit_exceeded`, `ddos_detected`
- `api_key_created`, `api_key_rotated`, `api_key_revoked`
- `suspicious_activity`, `account_locked`

### Application Events

Track application-level operations:

- `background_job_started`, `background_job_completed`, `background_job_failed`
- `circuit_breaker_opened`, `circuit_breaker_closed`
- `health_check_failed`, `health_check_recovered`
- `cache_cleared`, `queue_purged`

---

## Querying Events

### Find Recent Events

```sql
SELECT *
FROM system_events
WHERE time > NOW() - INTERVAL '1 hour'
ORDER BY time DESC
LIMIT 100;
```

### Events by Type

```sql
SELECT
    event_type,
    COUNT(*) as event_count,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful,
    AVG(duration_ms) as avg_duration_ms
FROM system_events
WHERE time > NOW() - INTERVAL '24 hours'
GROUP BY event_type
ORDER BY event_count DESC;
```

### Failed Events

```sql
SELECT
    time,
    event_type,
    severity,
    source,
    metadata
FROM system_events
WHERE success = false
    AND time > NOW() - INTERVAL '1 hour'
ORDER BY time DESC;
```

### Business Metrics from Events

```sql
-- Daily revenue from payment events
SELECT
    DATE_TRUNC('day', time) as day,
    COUNT(*) as payments,
    SUM(amount) as revenue,
    AVG(amount) as avg_payment
FROM system_events
WHERE event_type = 'payment_processed'
    AND success = true
    AND time > NOW() - INTERVAL '30 days'
GROUP BY day
ORDER BY day DESC;
```

---

## Correlating Observability Data

### Events + Metrics: Root Cause Analysis

"Why did CPU spike?"

```sql
-- Find deployments during CPU spikes
SELECT
    e.time as deployment_time,
    e.event_type,
    e.metadata,
    m.time as metric_time,
    m.value as cpu_value
FROM system_events e
JOIN cpu_metrics m
    ON m.time BETWEEN e.time AND e.time + e.duration_ms
    AND m.value > 80  -- High CPU
WHERE e.event_category = 'infrastructure'
    AND e.time > NOW() - INTERVAL '1 hour'
ORDER BY e.time DESC;
```

### Events + Logs: Error Context

"What errors happened during this deployment?"

```sql
SELECT
    e.time as event_time,
    e.event_type,
    e.resource_id,
    l.time as log_time,
    l.level,
    l.message
FROM system_events e
JOIN application_logs l
    ON l.time BETWEEN e.time AND e.time + e.duration_ms
    AND l.level IN ('ERROR', 'FATAL')
WHERE e.event_type = 'deployment_started'
    AND e.time > NOW() - INTERVAL '1 hour'
ORDER BY e.time DESC;
```

### Events + Traces: Latency Impact

"How did this deployment affect request latency?"

```sql
SELECT
    CASE
        WHEN t.time < e.time THEN 'before_deployment'
        WHEN t.time BETWEEN e.time AND e.time + e.duration_ms THEN 'during_deployment'
        ELSE 'after_deployment'
    END as phase,
    COUNT(*) as request_count,
    AVG(t.duration_ns / 1000000.0) as avg_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.duration_ns / 1000000.0) as p95_latency_ms
FROM distributed_traces t
CROSS JOIN system_events e
WHERE e.event_type = 'deployment_started'
    AND e.time > NOW() - INTERVAL '2 hours'
    AND t.time BETWEEN e.time - INTERVAL '30 minutes' AND e.time + INTERVAL '1 hour'
GROUP BY phase
ORDER BY phase;
```

### Complete Correlation: All Four Pillars

"Show me everything that happened around this incident"

```sql
WITH incident AS (
    SELECT time, time + INTERVAL '30 minutes' as end_time
    FROM system_events
    WHERE event_type = 'health_check_failed'
        AND source = 'api-gateway'
    ORDER BY time DESC
    LIMIT 1
)
SELECT
    'event' as data_type,
    e.time,
    e.event_type as detail,
    e.severity,
    NULL as value
FROM system_events e, incident i
WHERE e.time BETWEEN i.time - INTERVAL '10 minutes' AND i.end_time

UNION ALL

SELECT
    'metric' as data_type,
    m.time,
    'cpu_usage' as detail,
    CASE WHEN m.value > 80 THEN 'critical' ELSE 'normal' END as severity,
    m.value
FROM cpu_metrics m, incident i
WHERE m.time BETWEEN i.time - INTERVAL '10 minutes' AND i.end_time
    AND m.value > 50

UNION ALL

SELECT
    'log' as data_type,
    l.time,
    l.message as detail,
    l.level as severity,
    NULL as value
FROM application_logs l, incident i
WHERE l.time BETWEEN i.time - INTERVAL '10 minutes' AND i.end_time
    AND l.level IN ('ERROR', 'FATAL')

UNION ALL

SELECT
    'trace' as data_type,
    t.time,
    t.operation_name as detail,
    CASE WHEN t.error THEN 'error' ELSE 'normal' END as severity,
    t.duration_ns / 1000000.0 as value
FROM distributed_traces t, incident i
WHERE t.time BETWEEN i.time - INTERVAL '10 minutes' AND i.end_time
    AND (t.error = true OR t.duration_ns > 500000000)

ORDER BY time DESC;
```

---

## Load Testing

Arc includes a comprehensive events load testing script: [`scripts/synthetic_events_load_test.py`](../scripts/synthetic_events_load_test.py)

### Features

- Pre-generates realistic infrastructure, business, security, and application events
- Rich metadata with JSON-encoded context
- Realistic duration values for long-running events
- Success/failure flags and severity levels
- Monetary amounts for business events

### Usage

```bash
# Basic test - 500K events/sec
./scripts/synthetic_events_load_test.py \
    --token YOUR_API_TOKEN \
    --database events \
    --rps 500000 \
    --duration 30

# High-throughput test - 938K events/sec
./scripts/synthetic_events_load_test.py \
    --token YOUR_API_TOKEN \
    --database events \
    --rps 1000000 \
    --duration 60 \
    --batch-size 20000 \
    --workers 600
```

### Example Output

```
================================================================================
Arc Synthetic Events Load Test - MessagePack Columnar Format
================================================================================
Target RPS:      1,000,000 events/sec
Batch Size:      20,000 events/batch
Duration:        60s
Database:        events
Event Types:     Infrastructure, Business, Security, Application
Pre-generated:   500 batches in memory
Protocol:        MessagePack + Direct Arrow/Parquet
Arc URL:         http://localhost:8000/api/v1/write/msgpack
================================================================================

================================================================================
Test Complete
================================================================================
Duration:        64.0s
Total Sent:      60,000,000 events
Total Errors:    0
Success Rate:    100.00%
Actual RPS:      938,177 events/sec
Target RPS:      1,000,000 events/sec
Achievement:     93.8%

Latency Percentiles (ms):
  p50:  154.76
  p95:  1776.68
  p99:  2010.52
  p999: 2022.02
================================================================================
```

---

## Best Practices

### 1. Use Descriptive Event Types

```python
# Good - clear and specific
event_type = "deployment_started"
event_type = "payment_processed"
event_type = "user_signup"

# Bad - vague
event_type = "event"
event_type = "action"
event_type = "thing_happened"
```

### 2. Include Rich Metadata

```python
# Good - detailed context
metadata = {
    "version": "v1.2.3",
    "replicas": 5,
    "strategy": "rolling",
    "triggered_by": "nacho@example.com",
    "commit_sha": "abc123def456"
}

# Bad - minimal context
metadata = {
    "version": "v1.2.3"
}
```

### 3. Set Appropriate Severity

```python
# Deployment started - informational
severity = "info"

# Deployment taking longer than expected - warning
severity = "warning"

# Deployment failed - error
severity = "error"

# System-wide outage - critical
severity = "critical"
```

### 4. Track Event Duration

```python
# For long-running events
start_event = {
    "event_type": "deployment_started",
    "duration_ms": 0  # Just started
}

end_event = {
    "event_type": "deployment_completed",
    "duration_ms": 180000  # Took 3 minutes
}
```

### 5. Correlate with Other Telemetry

Always include fields that enable correlation:

```python
{
    "time": timestamp,  # For time-based correlation
    "source": "api-gateway",  # Matches service_name in logs/traces
    "environment": "production",  # Matches environment in all telemetry
    "resource_id": "deployment-123",  # Links related events
}
```

---

## Real-World Use Cases

### 1. Incident Response

```sql
-- Timeline of events during incident
SELECT time, event_type, severity, source, metadata
FROM system_events
WHERE time BETWEEN '2025-10-28 14:00:00' AND '2025-10-28 15:00:00'
    AND severity IN ('error', 'critical')
ORDER BY time;
```

### 2. Deployment Analytics

```sql
-- Deployment success rate and duration
SELECT
    DATE_TRUNC('day', time) as day,
    COUNT(*) as total_deployments,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful,
    AVG(duration_ms / 1000.0) as avg_duration_seconds,
    MAX(duration_ms / 1000.0) as max_duration_seconds
FROM system_events
WHERE event_type = 'deployment_completed'
    AND time > NOW() - INTERVAL '30 days'
GROUP BY day
ORDER BY day DESC;
```

### 3. Security Monitoring

```sql
-- Failed authentication attempts by user
SELECT
    user_id,
    COUNT(*) as failed_attempts,
    MAX(time) as last_attempt,
    ARRAY_AGG(DISTINCT metadata::json->>'source_ip') as ip_addresses
FROM system_events
WHERE event_type = 'authentication_failed'
    AND time > NOW() - INTERVAL '1 hour'
GROUP BY user_id
HAVING COUNT(*) >= 5  -- 5+ failed attempts
ORDER BY failed_attempts DESC;
```

### 4. Business Analytics

```sql
-- User growth and conversion funnel
WITH signups AS (
    SELECT DATE_TRUNC('day', time) as day, COUNT(*) as signup_count
    FROM system_events
    WHERE event_type = 'user_signup'
    GROUP BY day
),
subscriptions AS (
    SELECT DATE_TRUNC('day', time) as day, COUNT(*) as subscription_count
    FROM system_events
    WHERE event_type = 'subscription_created'
    GROUP BY day
)
SELECT
    s.day,
    s.signup_count,
    COALESCE(sub.subscription_count, 0) as subscriptions,
    ROUND(100.0 * COALESCE(sub.subscription_count, 0) / s.signup_count, 2) as conversion_rate
FROM signups s
LEFT JOIN subscriptions sub ON s.day = sub.day
WHERE s.day > NOW() - INTERVAL '30 days'
ORDER BY s.day DESC;
```

---

## Conclusion

Events are the **fourth pillar** of observability, completing Arc's unified observability platform. With **938K events/second**, Arc provides:

- ✅ **Root cause analysis** through event correlation
- ✅ **Business intelligence** alongside technical telemetry
- ✅ **Unified storage** with metrics, logs, and traces
- ✅ **Simple operations** with one protocol and one endpoint

Events answer the critical question: **"What changed?"** - enabling you to understand not just what happened, but **why** it happened.

---

## References

- [Synthetic Events Load Test Script](../scripts/synthetic_events_load_test.py)
- [Log Ingestion Documentation](./LOG_INGESTION.md)
- [Traces Ingestion Documentation](./TRACES_INGESTION.md)
- [Load Testing Guide](./LOAD_TESTING.md)
- [Arc Architecture](./ARCHITECTURE.md)

---

**Last Updated**: October 28, 2025
**Test Environment**: 14-core Apple Silicon, 36 GB RAM, macOS
**Arc Version**: Latest main branch
