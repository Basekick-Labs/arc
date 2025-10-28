# Distributed Traces Ingestion with Arc

Arc supports high-throughput distributed tracing using the same MessagePack columnar protocol used for metrics and logs. This enables complete observability through a unified system.

This document describes Arc's trace ingestion capabilities, performance benchmarks, and how to use Arc as a high-performance trace storage backend.

## Table of Contents

- [Overview](#overview)
- [Why Use Arc for Traces?](#why-use-arc-for-traces)
- [Performance Benchmarks](#performance-benchmarks)
- [Understanding Distributed Tracing](#understanding-distributed-tracing)
- [Protocol Specification](#protocol-specification)
- [Sending Traces to Arc](#sending-traces-to-arc)
- [Load Testing](#load-testing)
- [Querying Traces](#querying-traces)
- [Comparison with Other Systems](#comparison-with-other-systems)

---

## Overview

Arc provides **944,000+ spans/second** trace ingestion using the MessagePack columnar protocol. This completes the observability triangle:

- ✅ **Metrics**: 2.45M/sec
- ✅ **Logs**: 955K/sec
- ✅ **Traces**: 944K/sec

**One endpoint. One protocol. Complete observability.**

---

## Why Use Arc for Traces?

### Complete Observability Stack

Traditional observability requires multiple specialized systems:

```
Traditional Stack:
├── Prometheus/VictoriaMetrics → metrics
├── Loki/Elasticsearch → logs
├── Jaeger/Tempo → traces
└── Complex correlation, multiple storage backends

Arc Unified Stack:
└── Arc → metrics + logs + traces + events
    └── Single storage backend, unified queries
```

### Key Benefits

1. **Unified Storage**: Metrics, logs, and traces in the same Parquet files
2. **Correlated Queries**: Join traces with metrics and logs by timestamp
3. **High Performance**: 944K spans/sec with zero errors
4. **Simple Operations**: One system to deploy, monitor, and scale
5. **Cost Efficiency**: Shared infrastructure and columnar compression

---

## Performance Benchmarks

### Throughput Performance

Arc achieves **944,253 spans/second** on standard hardware (14-core MacBook Pro):

| Batch Size | Throughput | p50 Latency | p99 Latency | Use Case |
|------------|------------|-------------|-------------|----------|
| **20,000 spans** | **944K spans/sec** | 162.2ms | 2093.7ms | Maximum throughput |
| **10,000 spans** | ~450K spans/sec* | ~80ms* | ~1000ms* | Balanced |
| **5,000 spans** | **99K spans/sec** | 32.8ms | 105.9ms | Low latency |
| **1,000 spans** | ~20K spans/sec* | ~10ms* | ~50ms* | Real-time tracing |

*Estimated based on batch size scaling

### Hardware Utilization

During the 944K spans/sec test:
- **CPU**: 85-90% peak utilization
- **Memory**: 15-18 GB
- **Duration**: 60 seconds
- **Total Spans**: 60 million
- **Errors**: 0 (100% success rate)

### Test Configuration

- **Hardware**: 14-core Apple Silicon, 36 GB RAM
- **Arc Workers**: 42 Gunicorn workers
- **Test Workers**: 600 concurrent async workers
- **Protocol**: MessagePack columnar format + gzip compression
- **Storage**: Local Parquet files

---

## Understanding Distributed Tracing

### What are Distributed Traces?

A **trace** represents a single request's journey through a distributed system. Each trace consists of multiple **spans** that represent operations within services.

**Key Concepts:**

1. **Trace ID**: Unique identifier linking all spans for a single request
2. **Span ID**: Unique identifier for a single operation
3. **Parent Span ID**: Links spans into a hierarchy (parent-child relationships)
4. **Duration**: How long the operation took
5. **Service Name**: Which service performed this operation

### Example Trace Flow

```
Request: User clicks "Place Order"

api-gateway (trace_id: abc123)
├── span_id: span-001, duration: 250ms, operation: HTTP POST /orders
    ├── auth-service (trace_id: abc123)
    │   └── span_id: span-002, parent: span-001, duration: 15ms, operation: verify_token
    ├── order-service (trace_id: abc123)
    │   └── span_id: span-003, parent: span-001, duration: 120ms, operation: create_order
    │       ├── inventory-service (trace_id: abc123)
    │       │   └── span_id: span-004, parent: span-003, duration: 45ms, operation: check_stock
    │       └── payment-service (trace_id: abc123)
    │           └── span_id: span-005, parent: span-003, duration: 60ms, operation: charge_card
    └── notification-service (trace_id: abc123)
        └── span_id: span-006, parent: span-001, duration: 30ms, operation: send_email
```

All spans share the same `trace_id` (abc123), allowing you to reconstruct the entire request flow.

---

## Protocol Specification

### MessagePack Columnar Format

Arc uses a columnar data format for maximum compression and write throughput:

```python
{
    "m": "distributed_traces",  # measurement/table name
    "columns": {
        "time": [timestamp1, timestamp2, ...],              # millisecond timestamps
        "trace_id": ["abc123", "abc123", ...],              # groups related spans
        "span_id": ["span-001", "span-002", ...],           # unique span IDs
        "parent_span_id": ["", "span-001", ...],            # parent span (empty = root)
        "service_name": ["api-gateway", "auth-service", ...],
        "operation_name": ["HTTP POST", "verify_token", ...],
        "span_kind": ["server", "client", ...],             # OpenTelemetry standard
        "duration_ns": [250000000, 15000000, ...],          # duration in nanoseconds
        "status_code": [200, 200, ...],                     # HTTP status codes
        "http_method": ["POST", "GET", ...],                # HTTP methods
        "environment": ["production", "staging", ...],
        "region": ["us-east-1", "eu-west-1", ...],
        "error": [false, false, ...]                        # error flag
    }
}
```

### Field Types

- **time**: int64 millisecond timestamps (required)
- **trace_id**: string (hex or UUID format)
- **span_id**: string (hex or UUID format)
- **parent_span_id**: string (empty string for root spans)
- **service_name**: string
- **operation_name**: string
- **span_kind**: string (server, client, internal, producer, consumer)
- **duration_ns**: int64 (nanoseconds)
- **status_code**: int (HTTP status codes)
- **http_method**: string
- **error**: boolean

### Span Kinds (OpenTelemetry Standard)

- **server**: Span covers server-side handling of a synchronous RPC or HTTP request
- **client**: Span describes a request to a remote service
- **internal**: Span represents an internal operation within an application
- **producer**: Span represents a message producer sending to a message broker
- **consumer**: Span represents a message consumer receiving from a broker

### Compression

Traces compress well due to repetitive trace IDs and service names:

- **Uncompressed**: ~100 bytes/span
- **Compressed**: ~26-27 bytes/span (with gzip)
- **Compression ratio**: 3.7x
- **Batch size**: 5,000 spans = ~134 KB compressed

---

## Sending Traces to Arc

### Python Example (OpenTelemetry-style)

```python
import msgpack
import gzip
import requests
import time
import uuid

def send_traces_to_arc(spans, arc_url="http://localhost:8000",
                       database="traces", token="your-api-token"):
    """
    Send distributed traces to Arc using MessagePack columnar format

    Args:
        spans: List of span dictionaries
        arc_url: Arc server URL
        database: Database name
        token: API authentication token
    """
    # Convert to columnar format
    payload = {
        "m": "distributed_traces",
        "columns": {
            "time": [span["timestamp"] for span in spans],
            "trace_id": [span["trace_id"] for span in spans],
            "span_id": [span["span_id"] for span in spans],
            "parent_span_id": [span.get("parent_span_id", "") for span in spans],
            "service_name": [span["service_name"] for span in spans],
            "operation_name": [span["operation_name"] for span in spans],
            "span_kind": [span.get("span_kind", "internal") for span in spans],
            "duration_ns": [span["duration_ns"] for span in spans],
            "status_code": [span.get("status_code", 0) for span in spans],
            "error": [span.get("error", False) for span in spans],
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

# Example: Trace a request through multiple services
trace_id = uuid.uuid4().hex
base_time = int(time.time() * 1000)

spans = [
    # Root span - API Gateway
    {
        "timestamp": base_time,
        "trace_id": trace_id,
        "span_id": "span-001",
        "parent_span_id": "",  # Root span
        "service_name": "api-gateway",
        "operation_name": "POST /orders",
        "span_kind": "server",
        "duration_ns": 250_000_000,  # 250ms
        "status_code": 200,
        "error": False
    },
    # Child span - Auth Service
    {
        "timestamp": base_time + 5,
        "trace_id": trace_id,
        "span_id": "span-002",
        "parent_span_id": "span-001",
        "service_name": "auth-service",
        "operation_name": "verify_token",
        "span_kind": "client",
        "duration_ns": 15_000_000,  # 15ms
        "status_code": 200,
        "error": False
    },
    # Child span - Order Service
    {
        "timestamp": base_time + 25,
        "trace_id": trace_id,
        "span_id": "span-003",
        "parent_span_id": "span-001",
        "service_name": "order-service",
        "operation_name": "create_order",
        "span_kind": "internal",
        "duration_ns": 120_000_000,  # 120ms
        "status_code": 0,
        "error": False
    }
]

response = send_traces_to_arc(spans)
print(f"Status: {response.status_code}")
```

### Go Example

```go
package main

import (
    "bytes"
    "compress/gzip"
    "net/http"
    "time"

    "github.com/google/uuid"
    "github.com/vmihailenco/msgpack/v5"
)

type TracePayload struct {
    M       string                 `msgpack:"m"`
    Columns map[string]interface{} `msgpack:"columns"`
}

type Span struct {
    Timestamp     int64
    TraceID       string
    SpanID        string
    ParentSpanID  string
    ServiceName   string
    OperationName string
    SpanKind      string
    DurationNS    int64
    StatusCode    int
    Error         bool
}

func sendTraces(spans []Span, arcURL, database, token string) error {
    // Extract columns
    times := make([]int64, len(spans))
    traceIDs := make([]string, len(spans))
    spanIDs := make([]string, len(spans))
    parentSpanIDs := make([]string, len(spans))
    serviceNames := make([]string, len(spans))
    operationNames := make([]string, len(spans))
    spanKinds := make([]string, len(spans))
    durations := make([]int64, len(spans))
    statusCodes := make([]int, len(spans))
    errors := make([]bool, len(spans))

    for i, span := range spans {
        times[i] = span.Timestamp
        traceIDs[i] = span.TraceID
        spanIDs[i] = span.SpanID
        parentSpanIDs[i] = span.ParentSpanID
        serviceNames[i] = span.ServiceName
        operationNames[i] = span.OperationName
        spanKinds[i] = span.SpanKind
        durations[i] = span.DurationNS
        statusCodes[i] = span.StatusCode
        errors[i] = span.Error
    }

    // Create payload
    payload := TracePayload{
        M: "distributed_traces",
        Columns: map[string]interface{}{
            "time":            times,
            "trace_id":        traceIDs,
            "span_id":         spanIDs,
            "parent_span_id":  parentSpanIDs,
            "service_name":    serviceNames,
            "operation_name":  operationNames,
            "span_kind":       spanKinds,
            "duration_ns":     durations,
            "status_code":     statusCodes,
            "error":           errors,
        },
    }

    // Serialize to MessagePack
    data, err := msgpack.Marshal(payload)
    if err != nil {
        return err
    }

    // Compress
    var buf bytes.Buffer
    gz := gzip.NewWriter(&buf)
    gz.Write(data)
    gz.Close()

    // Send HTTP request
    req, _ := http.NewRequest("POST", arcURL+"/api/v1/write/msgpack", &buf)
    req.Header.Set("Content-Type", "application/msgpack")
    req.Header.Set("Content-Encoding", "gzip")
    req.Header.Set("X-Arc-Database", database)
    req.Header.Set("Authorization", "Bearer "+token)

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    return nil
}

func main() {
    traceID := uuid.New().String()
    baseTime := time.Now().UnixMilli()

    spans := []Span{
        {
            Timestamp:     baseTime,
            TraceID:       traceID,
            SpanID:        "span-001",
            ParentSpanID:  "",
            ServiceName:   "api-gateway",
            OperationName: "POST /orders",
            SpanKind:      "server",
            DurationNS:    250_000_000,
            StatusCode:    200,
            Error:         false,
        },
    }

    sendTraces(spans, "http://localhost:8000", "traces", "your-token")
}
```

---

## Load Testing

Arc includes a comprehensive traces load testing script: [`scripts/synthetic_traces_load_test.py`](../scripts/synthetic_traces_load_test.py)

### Features

- Pre-generates realistic distributed traces with span hierarchies
- 20 simulated microservices with realistic operation names
- Parent-child span relationships (30% of spans have parents)
- OpenTelemetry-compliant span kinds
- Realistic duration distribution (1ms-500ms based on operation type)
- HTTP metadata for server/client spans
- 5% error rate with increased latency for failures

### Usage

```bash
# Basic test - 100K spans/sec
./scripts/synthetic_traces_load_test.py \
    --token YOUR_API_TOKEN \
    --database traces \
    --rps 100000 \
    --duration 30

# High-throughput test - 944K spans/sec
./scripts/synthetic_traces_load_test.py \
    --token YOUR_API_TOKEN \
    --database traces \
    --rps 1000000 \
    --duration 60 \
    --batch-size 20000 \
    --workers 600

# Low-latency test
./scripts/synthetic_traces_load_test.py \
    --token YOUR_API_TOKEN \
    --database traces \
    --rps 100000 \
    --duration 30 \
    --batch-size 5000 \
    --workers 100
```

### Example Output

```
================================================================================
Arc Synthetic Traces Load Test - MessagePack Columnar Format
================================================================================
Target RPS:      1,000,000 spans/sec
Batch Size:      20,000 spans/batch
Duration:        60s
Database:        traces
Services:        20
Pre-generated:   500 batches in memory
Protocol:        MessagePack + Direct Arrow/Parquet
Arc URL:         http://localhost:8000/api/v1/write/msgpack
================================================================================

[   5.0s] RPS:  1200000 (target: 1000000) | Total:    6,000,000 | Errors:      0
[  10.0s] RPS:   800000 (target: 1000000) | Total:   10,000,000 | Errors:      0
...
[  60.0s] RPS:  1000000 (target: 1000000) | Total:   60,000,000 | Errors:      0

================================================================================
Test Complete
================================================================================
Duration:        63.5s
Total Sent:      60,000,000 spans
Total Errors:    0
Success Rate:    100.00%
Actual RPS:      944,253 spans/sec
Target RPS:      1,000,000 spans/sec
Achievement:     94.4%

Latency Percentiles (ms):
  p50:  162.15
  p95:  1861.69
  p99:  2093.71
  p999: 2103.43
================================================================================
```

---

## Querying Traces

### Find All Spans for a Trace

```sql
SELECT *
FROM distributed_traces
WHERE trace_id = 'abc123'
ORDER BY time;
```

### Find Slow Traces

```sql
SELECT
    trace_id,
    service_name,
    operation_name,
    duration_ns / 1000000.0 as duration_ms
FROM distributed_traces
WHERE duration_ns > 1000000000  -- > 1 second
ORDER BY duration_ns DESC
LIMIT 100;
```

### Traces with Errors

```sql
SELECT
    trace_id,
    service_name,
    operation_name,
    status_code,
    error
FROM distributed_traces
WHERE error = true
    AND time > NOW() - INTERVAL '1 hour'
ORDER BY time DESC;
```

### Service Dependency Map

```sql
-- Find parent-child service relationships
SELECT DISTINCT
    parent.service_name as parent_service,
    child.service_name as child_service,
    COUNT(*) as call_count,
    AVG(child.duration_ns) / 1000000.0 as avg_duration_ms
FROM distributed_traces child
JOIN distributed_traces parent
    ON child.parent_span_id = parent.span_id
    AND child.trace_id = parent.trace_id
WHERE child.time > NOW() - INTERVAL '1 hour'
GROUP BY parent.service_name, child.service_name
ORDER BY call_count DESC;
```

### P95 Latency by Service

```sql
SELECT
    service_name,
    operation_name,
    COUNT(*) as total_spans,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ns) / 1000000.0 as p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ns) / 1000000.0 as p95_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ns) / 1000000.0 as p99_ms
FROM distributed_traces
WHERE time > NOW() - INTERVAL '1 hour'
GROUP BY service_name, operation_name
ORDER BY p95_ms DESC
LIMIT 20;
```

### Correlate Traces with Logs

```sql
-- Find logs for a specific trace
SELECT
    l.time,
    l.level,
    l.service,
    l.message,
    t.operation_name,
    t.duration_ns / 1000000.0 as span_duration_ms
FROM application_logs l
JOIN distributed_traces t
    ON l.service = t.service_name
    AND l.time BETWEEN t.time AND t.time + (t.duration_ns / 1000000)
WHERE t.trace_id = 'abc123'
ORDER BY l.time;
```

---

## Comparison with Other Systems

### Ingestion Throughput

| System | Spans/Second | Notes |
|--------|--------------|-------|
| **Arc** | **944,000** | Unified metrics + logs + traces |
| **Jaeger** | ~100K-200K* | Cassandra/Elasticsearch backend |
| **Tempo** | ~50K-100K* | Object storage backend |
| **Zipkin** | ~50K-100K* | Multiple storage backends |
| **SigNoz** | ~100K* | ClickHouse backend |

*Estimates based on community reports and documentation

### Key Differentiators

**Arc vs Jaeger:**
- Arc: 4-9x faster ingestion, unified with metrics/logs
- Jaeger: Mature ecosystem, native OpenTelemetry support
- **Use Arc if**: You want unified observability storage
- **Use Jaeger if**: You need mature tracing UI and ecosystem

**Arc vs Tempo:**
- Arc: 9-18x faster ingestion, queryable by SQL
- Tempo: Optimized for object storage, TraceQL queries
- **Use Arc if**: You need high throughput and SQL queries
- **Use Tempo if**: You're Grafana-centric and want TraceQL

**Arc vs SigNoz:**
- Arc: 9x faster ingestion, simpler deployment
- SigNoz: Full observability UI, ClickHouse-based
- **Use Arc if**: You want embedded database, API-first
- **Use SigNoz if**: You need complete observability UI

### Arc's Unique Position

Arc is the **only** system that provides:
- ✅ **2.45M metrics/sec** (best-in-class)
- ✅ **955K logs/sec** (45x faster than Loki/Elasticsearch)
- ✅ **944K spans/sec** (9x faster than Jaeger/Tempo)
- ✅ **Unified protocol** (same endpoint for all data types)
- ✅ **Simple deployment** (17K lines Python, single binary)
- ✅ **Correlated queries** (join metrics, logs, traces by timestamp)

---

## Real-World Capacity

### What can 944K spans/second handle?

**Deployment Size Estimates:**

Assuming 10 spans per trace on average:

- **Small** (10 services, 100 req/s): 1,000 spans/s → **944x headroom**
- **Medium** (50 services, 1,000 req/s): 10,000 spans/s → **94x headroom**
- **Large** (200 services, 5,000 req/s): 50,000 spans/s → **19x headroom**
- **Very Large** (500 services, 10,000 req/s): 100,000 spans/s → **9x headroom**

### Daily Volume

At 944K spans/second:
- **Per minute**: 56.6 million spans
- **Per hour**: 3.4 billion spans
- **Per day**: 81.6 billion spans
- **Per month**: 2.45 trillion spans

This is sufficient for most production deployments without sharding.

---

## Best Practices

### 1. Batch Spans by Trace

Send all spans for a trace together when possible:

```python
# Good - batch related spans
spans_for_trace = collect_spans_for_trace(trace_id)
send_traces_to_arc(spans_for_trace)

# Acceptable - batch by time window
spans_last_second = collect_spans_in_window(1000)
send_traces_to_arc(spans_last_second)
```

### 2. Include Parent-Child Relationships

Always set `parent_span_id` to enable trace visualization:

```python
root_span = {
    "span_id": "span-001",
    "parent_span_id": "",  # Empty for root
    ...
}

child_span = {
    "span_id": "span-002",
    "parent_span_id": "span-001",  # Links to parent
    ...
}
```

### 3. Use Consistent Service Names

Keep service names consistent across metrics, logs, and traces:

```python
# Consistent naming enables correlation
service_name = "payment-service"

# Metrics
send_metrics({"service": service_name, "cpu": 45.2})

# Logs
send_logs({"service": service_name, "message": "Processing payment"})

# Traces
send_traces([{"service_name": service_name, "operation": "charge_card"}])
```

### 4. Set Realistic Duration Values

Use nanoseconds for duration (even though timestamps are milliseconds):

```python
# Correct - duration in nanoseconds
duration_ns = 45_000_000  # 45ms

# Calculate from start/end
start_ns = time.time_ns()
# ... do work ...
end_ns = time.time_ns()
duration_ns = end_ns - start_ns
```

### 5. Sample High-Volume Traces

For very high RPS services, implement sampling:

```python
import random

# Sample 10% of traces
if random.random() < 0.1:
    send_traces_to_arc(spans)
```

---

## Limitations and Considerations

### Current Limitations

1. **No trace visualization UI**: Arc focuses on storage, not UI (use Grafana/custom UI)
2. **No automatic sampling**: You must implement sampling client-side
3. **No span filtering**: All spans for a trace should be stored together
4. **Single-node only**: Arc doesn't support distributed deployments (yet)

### When NOT to Use Arc for Traces

- **Need trace UI**: Use Jaeger or SigNoz for built-in visualization
- **OpenTelemetry native**: Use Tempo for native OTLP protocol support
- **Extreme scale**: Use distributed Jaeger/Tempo for multi-region deployments
- **Complex sampling**: Use systems with built-in sampling strategies

### When to Use Arc for Traces

- ✅ High-volume trace ingestion (100K+ spans/sec)
- ✅ Unified observability (correlate metrics, logs, traces)
- ✅ SQL-based trace analysis
- ✅ Simple deployment and operations
- ✅ Cost-conscious workloads

---

## Future Improvements

Potential enhancements for trace ingestion:

1. **Trace visualization UI**: Built-in or integrated trace viewer
2. **Automatic sampling**: Server-side adaptive sampling
3. **Span links**: Support for span-to-span references
4. **Trace retention**: Separate retention policies for traces
5. **OpenTelemetry protocol**: Native OTLP support

---

## Conclusion

Arc provides **competitive distributed tracing performance** (944K spans/s) while maintaining its core strength as a **unified observability database**.

The ability to ingest metrics, logs, and traces through **one protocol** and **one endpoint** dramatically simplifies observability infrastructure.

For most production deployments, Arc's throughput (944K spans/s) provides an excellent balance of performance, simplicity, and unified storage.

---

## References

- [Synthetic Traces Load Test Script](../scripts/synthetic_traces_load_test.py)
- [Log Ingestion Documentation](./LOG_INGESTION.md)
- [Load Testing Guide](./LOAD_TESTING.md)
- [Arc Architecture](./ARCHITECTURE.md)
- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)

---

**Last Updated**: October 28, 2025
**Test Environment**: 14-core Apple Silicon, 36 GB RAM, macOS
**Arc Version**: Latest main branch
