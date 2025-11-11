# Load Testing Arc

Arc includes comprehensive load testing tools to benchmark ingestion performance for both metrics and logs. This document covers how to use the load testing scripts and interpret results.

## Table of Contents

- [Overview](#overview)
- [Metrics Load Testing](#metrics-load-testing)
- [Logs Load Testing](#logs-load-testing)
- [Traces Load Testing](#traces-load-testing)
- [Understanding Results](#understanding-results)
- [Performance Tuning](#performance-tuning)
- [Hardware Monitoring](#hardware-monitoring)
- [Troubleshooting](#troubleshooting)

---

## Overview

Arc provides load testing scripts for all four observability data types, plus a unified demo scenario.

### Performance Philosophy

**Arc prioritizes production-ready latency over maximum throughput.** All benchmarks use `batch_size=1000` to ensure sub-100ms p99 latency across all data types. While larger batches (10K-20K) can achieve 15-20% higher throughput, they result in 10-30x worse latency, making them unsuitable for real-time observability workloads.

**Recommended Configuration:**
- **batch_size=1000** for all production workloads
- Sub-100ms p99 latency for real-time dashboards, alerting, and debugging
- Consistent configuration across metrics, logs, traces, and events

### Performance Testing Scripts

1. **[`scripts/metrics_load_test.py`](../scripts/metrics_load_test.py)** - Metrics ingestion (2.91M metrics/sec, 29ms p99)
2. **[`scripts/synthetic_logs_load_test.py`](../scripts/synthetic_logs_load_test.py)** - Log ingestion (968K logs/sec, 58ms p99)
3. **[`scripts/synthetic_traces_load_test.py`](../scripts/synthetic_traces_load_test.py)** - Distributed traces (784K spans/sec, 64ms p99)
4. **[`scripts/synthetic_events_load_test.py`](../scripts/synthetic_events_load_test.py)** - Events ingestion (981K events/sec, 55ms p99)

### Demo Scenario

5. **[`scripts/demo_scenario_marketing_campaign.py`](../scripts/demo_scenario_marketing_campaign.py)** - Unified demo with all four data types showing a realistic marketing campaign impact scenario

All scripts:
- Pre-generate and compress data in memory for minimal overhead
- Use async workers with connection pooling
- Provide real-time RPS and latency monitoring
- Support configurable batch sizes and concurrency
- Report detailed statistics (p50, p95, p99, p999 latencies)

---

## Metrics Load Testing

### Script Location

[`scripts/metrics_load_test.py`](../scripts/metrics_load_test.py)

### Features

- Pre-generates realistic metrics data (CPU, memory, disk, network, temperature, etc.)
- Supports columnar MessagePack format (recommended) and row-based format
- Simulates 100 nodes sending system metrics
- Configurable batch size, workers, and target RPS
- Zero-copy compression with gzip

### Basic Usage

```bash
# Test with 500K metrics/sec for 30 seconds
./scripts/metrics_load_test.py \
    --token YOUR_API_TOKEN \
    --rps 500000 \
    --duration 30

# High-throughput test - 2.91M metrics/sec
./scripts/metrics_load_test.py \
    --token YOUR_API_TOKEN \
    --rps 3000000 \
    --duration 60 \
    --batch-size 1000 \
    --workers 500 \
    --columnar
```

### Command-Line Options

```
Required:
  --token TOKEN          API authentication token

Optional:
  --url URL              Arc server URL (default: http://localhost:8000)
  --database NAME        Database name (default: default)
  --rps RPS              Target records per second (default: 500000)
  --duration SECONDS     Test duration in seconds (default: 30)
  --batch-size SIZE      Records per batch (default: 1000)
  --workers NUM          Concurrent workers (default: auto-calculated)
  --columnar             Use columnar format (recommended for max performance)
```

### Example Output

```
🔄 Pre-generating 500 batches (500,000 records)
  Progress: 500/500 (182 batches/sec)
✅ Pre-generation complete!
   Time taken: 2.7s
   Total batches: 500
   Total records: 500,000
   Avg compressed size: 8.2 KB
   Total size: 4.0 MB

================================================================================
Arc MessagePack Load Test - Columnar Format
================================================================================
Target RPS:      500,000 metrics/sec
Batch Size:      1,000 metrics/batch
Duration:        30s
Database:        default
Nodes:           100
Pre-generated:   500 batches in memory
Protocol:        MessagePack + Direct Arrow/Parquet
Arc URL:         http://localhost:8000
================================================================================

Starting 50 workers...
Connection pool: 100 connections for 50 workers
Warming up connection pool...
✅ Connection pool warmed up

[   5.0s] RPS:  500000 (target: 500000) | Total:    2,500,000 | Errors:      0 | Latency (ms) - p50:    4.2 p95:   12.3 p99:   18.7
[  10.0s] RPS:  500000 (target: 500000) | Total:    5,000,000 | Errors:      0 | Latency (ms) - p50:    4.1 p95:   12.1 p99:   18.5
[  15.0s] RPS:  500000 (target: 500000) | Total:    7,500,000 | Errors:      0 | Latency (ms) - p50:    4.0 p95:   12.0 p99:   18.3
[  20.0s] RPS:  500000 (target: 500000) | Total:   10,000,000 | Errors:      0 | Latency (ms) - p50:    3.9 p95:   11.8 p99:   18.1
[  25.0s] RPS:  500000 (target: 500000) | Total:   12,500,000 | Errors:      0 | Latency (ms) - p50:    3.8 p95:   11.7 p99:   17.9
[  30.0s] RPS:  500000 (target: 500000) | Total:   15,000,000 | Errors:      0 | Latency (ms) - p50:    3.7 p95:   11.5 p99:   17.8

================================================================================
Test Complete
================================================================================
Duration:        30.2s
Total Sent:      15,000,000 metrics
Total Errors:    0
Success Rate:    100.00%
Actual RPS:      496,689 metrics/sec
Target RPS:      500,000 metrics/sec
Achievement:     99.3%

Latency Percentiles (ms):
  p50:  3.72
  p95:  11.53
  p99:  17.82
  p999: 24.31
================================================================================
```

### Metrics Data Schema

The script generates realistic metrics with these fields:

```python
{
    "m": "system_metrics",  # measurement name
    "columns": {
        "time": [timestamp1, timestamp2, ...],      # nanosecond timestamps
        "node": ["node-1", "node-2", ...],          # 100 different nodes
        "metric_name": ["cpu", "memory", ...],      # metric type
        "value": [45.2, 67.8, ...],                 # metric value (float)
        "region": ["us-east-1", ...],               # AWS region
        "environment": ["prod", "staging", ...]     # environment
    }
}
```

**Metrics types generated:**
- `cpu` (0-100%)
- `memory` (0-100%)
- `disk` (0-100%)
- `network_in` (0-1000 Mbps)
- `network_out` (0-1000 Mbps)
- `temperature` (20-80°C)
- `power` (100-500W)

### World Record Performance

Arc achieved **2.91 million metrics/second** using this script:

```bash
./scripts/metrics_load_test.py \
    --token YOUR_TOKEN \
    --rps 3000000 \
    --duration 60 \
    --batch-size 1000 \
    --workers 500 \
    --columnar
```

**Results:**
- **2.91M metrics/sec** sustained (97.1% of 3M target)
- **p50 latency**: 1.76ms
- **p95 latency**: 13.28ms
- **p99 latency**: 29.03ms
- **Zero errors** over 60 seconds
- **Hardware**: Apple M3 Max (14 cores), 36GB RAM
- **Configuration**: batch_size=1000, workers=500, gzip compression level 1

---

## Logs Load Testing

### Script Location

[`scripts/synthetic_logs_load_test.py`](../scripts/synthetic_logs_load_test.py)

### Features

- Pre-generates realistic application logs with structured fields
- Weighted log level distribution (30% DEBUG, 50% INFO, 15% WARN, 4% ERROR, 1% FATAL)
- 50 simulated services across multiple environments and regions
- Realistic messages, status codes, response times, and request IDs
- Columnar MessagePack format with gzip compression

### Basic Usage

```bash
# Test with 50K logs/sec for 30 seconds
./scripts/synthetic_logs_load_test.py \
    --token YOUR_API_TOKEN \
    --database logs \
    --rps 50000 \
    --duration 30

# High-throughput test - 968K logs/sec
./scripts/synthetic_logs_load_test.py \
    --token YOUR_API_TOKEN \
    --database logs \
    --rps 1000000 \
    --duration 60 \
    --batch-size 1000 \
    --workers 400
```

### Command-Line Options

```
Required:
  --token TOKEN          API authentication token

Optional:
  --url URL              Arc server URL (default: http://localhost:8000)
  --database NAME        Database name (default: logs)
  --rps RPS              Target logs per second (default: 50000)
  --duration SECONDS     Test duration in seconds (default: 30)
  --batch-size SIZE      Logs per batch (default: 1000)
  --workers NUM          Concurrent workers (default: auto-calculated)
  --num-services NUM     Number of simulated services (default: 50)
```

### Example Output

```
🔄 Pre-generating 500 log batches (500,000 log entries)
✅ Pre-generation complete!
   Time taken: 5.2s
   Total batches: 500
   Total log entries: 500,000
   Avg compressed size: 47.8 KB
   Total size: 23.4 MB

================================================================================
Arc Synthetic Logs Load Test - MessagePack Columnar Format
================================================================================
Target RPS:      1,000,000 logs/sec
Batch Size:      1,000 logs/batch
Duration:        60s
Database:        logs
Services:        50
Pre-generated:   500 batches in memory
Protocol:        MessagePack + Direct Arrow/Parquet
Arc URL:         http://localhost:8000
================================================================================

Starting 400 workers...

[   5.0s] RPS: 1000000 (target: 1000000) | Total:    5,000,000 | Errors:      0 | Latency (ms) - p50:   14.5 p95:   53.6 p99:   64.3
[  10.0s] RPS:  994800 (target: 1000000) | Total:    9,974,000 | Errors:      0 | Latency (ms) - p50:   14.3 p95:   49.2 p99:   61.9
[  15.0s] RPS:  906800 (target: 1000000) | Total:   14,508,000 | Errors:      0 | Latency (ms) - p50:   12.7 p95:   46.6 p99:   62.3
[  20.0s] RPS:  998400 (target: 1000000) | Total:   19,500,000 | Errors:      0 | Latency (ms) - p50:   11.9 p95:   46.7 p99:   64.8
[  25.0s] RPS: 1000000 (target: 1000000) | Total:   24,500,000 | Errors:      0 | Latency (ms) - p50:    9.4 p95:   42.7 p99:   61.6
[  30.0s] RPS:  973800 (target: 1000000) | Total:   29,369,000 | Errors:      0 | Latency (ms) - p50:    9.2 p95:   41.6 p99:   61.2

================================================================================
Test Complete
================================================================================
Duration:        60.5s
Total Sent:      58,548,000 log entries
Total Errors:    0
Success Rate:    100.00%
Actual RPS:      967,989 logs/sec
Target RPS:      1,000,000 logs/sec
Achievement:     96.8%

Latency Percentiles (ms):
  p50:  7.68
  p95:  37.74
  p99:  58.35
  p999: 88.11
================================================================================
```

### Log Data Schema

The script generates realistic logs with these fields:

```python
{
    "m": "application_logs",  # measurement name
    "columns": {
        "time": [timestamp1, timestamp2, ...],                    # nanosecond timestamps
        "level": ["INFO", "ERROR", ...],                          # log level
        "service": ["service-1", "service-2", ...],              # 50 services
        "environment": ["prod", "staging", "dev"],               # environment
        "region": ["us-east-1", "us-west-2", ...],              # AWS region
        "message": ["Request processed successfully", ...],      # log message
        "response_time_ms": [145, 203, ...],                     # response time
        "status_code": [200, 500, ...],                          # HTTP status
        "request_id": ["abc123def456", ...]                      # unique request ID
    }
}
```

**Log level distribution:**
- DEBUG: 30%
- INFO: 50%
- WARN: 15%
- ERROR: 4%
- FATAL: 1%

### Peak Performance

Arc achieved **955,179 logs/second** using this script:

```bash
./scripts/synthetic_logs_load_test.py \
    --token YOUR_TOKEN \
    --database logs \
    --rps 1000000 \
    --duration 60 \
    --batch-size 20000 \
    --workers 600
```

**Results:**
- **968K logs/sec** sustained (96.8% of 1M target)
- **58.5 million logs** in 60 seconds
- **p50 latency**: 7.68ms
- **p99 latency**: 58.35ms
- **Zero errors** over 60 seconds
- **Configuration**: batch_size=1000, workers=400

---

## Traces Load Testing

### Script Location

[`scripts/synthetic_traces_load_test.py`](../scripts/synthetic_traces_load_test.py)

### Features

- Pre-generates realistic distributed traces with span hierarchies
- 20 simulated microservices with realistic operation names
- Parent-child span relationships (30% of spans have parents)
- OpenTelemetry-compliant span kinds (server, client, internal, producer, consumer)
- Realistic duration distribution (1ms-500ms based on operation type)
- HTTP metadata for server/client spans
- 5% error rate with increased latency for failures

### Basic Usage

```bash
# Test with 100K spans/sec for 30 seconds
./scripts/synthetic_traces_load_test.py \
    --token YOUR_API_TOKEN \
    --database traces \
    --rps 100000 \
    --duration 30

# High-throughput test - 784K spans/sec
./scripts/synthetic_traces_load_test.py \
    --token YOUR_API_TOKEN \
    --database traces \
    --rps 800000 \
    --duration 60 \
    --batch-size 1000 \
    --workers 500
```

### Command-Line Options

```
Required:
  --token TOKEN          API authentication token

Optional:
  --database NAME        Database name (default: traces)
  --rps RPS              Target spans per second (default: 100000)
  --duration SECONDS     Test duration in seconds (default: 30)
  --batch-size SIZE      Spans per batch (default: 1000)
  --workers NUM          Concurrent workers (default: auto-calculated)
  --num-services NUM     Number of simulated services (default: 20)
```

### Example Output

```
🔄 Pre-generating 500 trace batches (500,000 spans)
  Progress: 100/500 (144 batches/sec)
  Progress: 200/500 (145 batches/sec)
  Progress: 300/500 (146 batches/sec)
  Progress: 400/500 (146 batches/sec)
  Progress: 500/500 (147 batches/sec)
✅ Pre-generation complete!
   Time taken: 3.4s
   Total batches: 500
   Total spans: 500,000
   Avg compressed size: 28.8 KB
   Total size: 14.1 MB

Starting 500 workers...

================================================================================
Arc Synthetic Traces Load Test - MessagePack Columnar Format
================================================================================
Target RPS:      800,000 spans/sec
Batch Size:      1,000 spans/batch
Duration:        60s
Database:        traces
Services:        20
Pre-generated:   500 batches in memory
Protocol:        MessagePack + Direct Arrow/Parquet
Arc URL:         http://localhost:8000/api/v1/write/msgpack
================================================================================

[   5.0s] RPS:   800000 (target: 800000) | Total:    4,000,000 | Errors:      0 | Latency (ms) - p50:   27.9 p95:   68.7 p99:   92.5
[  10.0s] RPS:   800000 (target: 800000) | Total:    8,000,000 | Errors:      0 | Latency (ms) - p50:   17.2 p95:   63.6 p99:   84.0
[  15.0s] RPS:   798000 (target: 800000) | Total:   11,990,000 | Errors:      0 | Latency (ms) - p50:    9.4 p95:   58.0 p99:   75.7
[  20.0s] RPS:   791000 (target: 800000) | Total:   15,945,000 | Errors:      0 | Latency (ms) - p50:    6.9 p95:   54.2 p99:   73.8
[  25.0s] RPS:   748400 (target: 800000) | Total:   19,687,000 | Errors:      0 | Latency (ms) - p50:    6.1 p95:   50.4 p99:   71.3
[  30.0s] RPS:   791800 (target: 800000) | Total:   23,646,000 | Errors:      0 | Latency (ms) - p50:    5.2 p95:   48.4 p99:   70.1

================================================================================
Test Complete
================================================================================
Duration:        60.6s
Total Sent:      47,505,000 spans
Total Errors:    0
Success Rate:    100.00%
Actual RPS:      783,762 spans/sec
Target RPS:      800,000 spans/sec
Achievement:     98.0%

Latency Percentiles (ms):
  p50:  2.61
  p95:  39.86
  p99:  63.56
  p999: 92.48
================================================================================
```

### Performance Trade-offs

Arc's trace benchmarks prioritize **production-ready latency** over maximum throughput:

| Configuration | Throughput | p50 Latency | p99 Latency | Production Ready? |
|---------------|------------|-------------|-------------|-------------------|
| **batch_size=1000** (recommended) | **784K spans/sec** | **2.61ms** | **63.56ms** | ✅ Yes - sub-100ms p99 |
| batch_size=5000 | 99K spans/sec | 32.8ms | 105.9ms | ⚠️ Marginal |
| batch_size=20000 | 944K spans/sec | 162ms | 2093ms | ❌ No - 2+ second p99 |

**Why 1K batches?** Real-time tracing requires low latency. While 20K batches achieve 20% higher throughput (944K), they have:
- **62x worse p50 latency** (162ms vs 2.61ms)
- **33x worse p99 latency** (2093ms vs 63.56ms)

For production observability, sub-100ms latency is critical for debugging, alerting, and real-time dashboards.

### Trace Data Schema

The script generates OpenTelemetry-compatible traces:

```python
{
    "m": "distributed_traces",
    "columns": {
        "time": [timestamp],                          # millisecond timestamps
        "trace_id": ["abc123"],                       # groups related spans
        "span_id": ["span-001"],                      # unique span ID
        "parent_span_id": [""],                       # parent span (empty = root)
        "service_name": ["api-gateway"],
        "operation_name": ["POST /orders"],
        "span_kind": ["server"],                      # server, client, internal, producer, consumer
        "duration_ns": [250000000],                   # duration in nanoseconds
        "status_code": [200],
        "http_method": ["POST"],
        "environment": ["production"],
        "region": ["us-east-1"],
        "error": [false]
    }
}
```

**Simulated Services:**
- api-gateway, auth-service, user-service
- product-service, order-service, payment-service
- inventory-service, notification-service, email-service
- database-service, cache-service, queue-service
- And more...

**Span Hierarchy Example:**
30% of spans have a parent, creating realistic trace trees:
```
api-gateway (root span)
├── auth-service (child of root)
├── order-service (child of root)
│   ├── inventory-service (child of order-service)
│   └── payment-service (child of order-service)
└── notification-service (child of root)
```

### Peak Performance

Arc achieved **944,253 spans/second** using this script:

```bash
./scripts/synthetic_traces_load_test.py \
    --token YOUR_TOKEN \
    --database traces \
    --rps 1000000 \
    --duration 60 \
    --batch-size 20000 \
    --workers 600
```

**Results:**
- **944K spans/sec** sustained
- **60 million spans** in 60 seconds
- **p99 latency**: 2093ms
- **Zero errors**

---

## Understanding Results

### RPS (Records Per Second)

The primary throughput metric. Shows:
- **Current RPS**: Actual ingestion rate in the last 5 seconds
- **Target RPS**: Your configured target
- **Total**: Cumulative records sent

**Note**: For large batches, you may see bursty patterns (e.g., 2.4M then 0, alternating) because Arc processes batches then waits for next batch. This is normal.

### Latency Percentiles

Understanding latency metrics:

- **p50 (median)**: 50% of requests complete faster than this
- **p95**: 95% of requests complete faster than this (important for SLAs)
- **p99**: 99% of requests complete faster than this (tail latency)
- **p999**: 99.9% of requests complete faster than this (extreme tail)

**What's good latency?**

| Use Case | Target p99 | Batch Size |
|----------|-----------|------------|
| Real-time alerts | < 50ms | 1,000 |
| Interactive dashboards | < 200ms | 5,000 |
| Production monitoring | < 500ms | 10,000 |
| Batch processing | < 2s | 20,000 |

### Achievement Percentage

`Actual RPS / Target RPS × 100`

- **> 95%**: Excellent - Arc is keeping up
- **80-95%**: Good - Minor bottleneck
- **< 80%**: Investigate - Check CPU, disk, network, or Arc configuration

### Error Rate

Should always be **0%** for valid tests. If you see errors:
- Check Arc logs for issues
- Verify authentication token
- Check network connectivity
- Ensure Arc has sufficient resources

---

## Performance Tuning

### Batch Size Recommendations

**For Metrics:**

| Target RPS | Batch Size | Workers | Expected p99 |
|-----------|------------|---------|--------------|
| 100K | 500 | 10 | < 10ms |
| 500K | 1,000 | 50 | < 20ms |
| 1M | 1,000 | 100 | < 30ms |
| 2.91M | 1,000 | 500 | < 30ms |

**For Logs:**

| Target RPS | Batch Size | Workers | Expected p99 |
|-----------|------------|---------|--------------|
| 50K | 1,000 | 5 | < 50ms |
| 100K | 5,000 | 10 | < 200ms |
| 200K | 10,000 | 20 | < 400ms |
| 955K | 20,000 | 600 | < 2s |

### Worker Count Tuning

The scripts auto-calculate workers based on target RPS:

```python
# Metrics: 1 worker per 10K RPS
workers = min(int(target_rps / 10000), 30)

# Logs: 1 worker per 10K RPS
workers = min(max(int(target_rps / 10000), 1), 30)
```

**Manual override:**
```bash
--workers 600  # Force 600 workers for extreme throughput
```

### Arc Server Tuning

For maximum throughput, configure Arc with more workers:

```bash
# In start.sh or gunicorn command
gunicorn -w 42 -k uvicorn.workers.UvicornWorker api.main:app

# Formula: workers = (2-3) × CPU_cores
# For 14 cores: 28-42 workers
```

---

## Hardware Monitoring

### Monitor During Load Test

Run this while your load test is running to see real-time resource usage:

```bash
# macOS
while true; do
    echo "=== $(date) ==="
    top -l 1 -n 0 | grep "CPU usage"
    ps aux | grep gunicorn | grep -v grep | awk '{sum_cpu+=$3; sum_mem+=$4} END {
        printf "Arc CPU: %.1f%% | MEM: %.1f%%\n", sum_cpu, sum_mem
    }'
    iostat -w 1 -c 2 disk0 | tail -1 | awk '{printf "Disk: R=%.1f MB/s W=%.1f MB/s\n", $3, $4}'
    echo ""
    sleep 5
done
```

```bash
# Linux
while true; do
    echo "=== $(date) ==="
    top -bn1 | grep "Cpu(s)"
    ps aux | grep gunicorn | grep -v grep | awk '{sum_cpu+=$3; sum_mem+=$4} END {
        printf "Arc CPU: %.1f%% | MEM: %.1f%%\n", sum_cpu, sum_mem
    }'
    iostat -x 1 2 | tail -1 | awk '{printf "Disk: R=%.1f MB/s W=%.1f MB/s\n", $6, $7}'
    echo ""
    sleep 5
done
```

### Expected Resource Usage

**At 2.45M metrics/sec:**
- CPU: 60-80% (across all cores)
- Memory: 8-12 GB
- Disk Write: 50-100 MB/s
- Network: Depends on batch size and compression

**At 968K logs/sec:**
- CPU: 35-45% (across all cores)
- Memory: 12-16 GB
- Disk Write: 40-80 MB/s
- Network: 150-300 MB/s (steady)

**At 981K events/sec:**
- CPU: 30-40% (across all cores)
- Memory: 10-14 GB
- Disk Write: 30-60 MB/s
- Network: 100-200 MB/s (steady)

**At 784K traces/sec:**
- CPU: 35-45% (across all cores)
- Memory: 12-16 GB
- Disk Write: 35-70 MB/s
- Network: 120-250 MB/s (steady)

---

## Troubleshooting

### Test Runs Too Slow

**Symptoms**: Actual RPS < 50% of target

**Solutions:**
1. **Increase Arc workers**: Edit `start.sh` to use more Gunicorn workers
2. **Check CPU saturation**: If Arc is at 100% CPU, you've hit the limit
3. **Reduce batch size**: Smaller batches = less processing per request
4. **Add more client workers**: Use `--workers 600` for higher concurrency
5. **Check disk I/O**: SSD required for high throughput

### High Error Rate

**Symptoms**: Errors > 0

**Solutions:**
1. **Check Arc logs**: `tail -f logs/arc.log`
2. **Verify token**: Ensure `--token` matches Arc configuration
3. **Check database exists**: Create database first if needed
4. **Network issues**: Test connectivity with `curl`
5. **Resource exhaustion**: Arc may be out of memory or disk space

### High Latency

**Symptoms**: p99 > 1 second for small batches

**Solutions:**
1. **Reduce batch size**: Trade throughput for latency
2. **Check Arc compaction**: May be blocking writes
3. **Monitor disk I/O**: Slow disk = high latency
4. **Reduce workers**: Too many workers can cause contention
5. **Check network latency**: Add `--url` with local IP

### Pre-generation Takes Forever

**Symptoms**: Pre-generation > 5 minutes

**Solutions:**
1. **Reduce batch size**: Less data to generate
2. **Reduce `--batch-size`**: Fewer records per batch
3. **Use faster CPU**: Pre-gen is CPU-bound (compression)

**Normal timings:**
- Metrics 1K batch × 500: ~3 seconds
- Logs 1K batch × 500: ~30 seconds
- Logs 20K batch × 500: ~150 seconds

### Memory Issues

**Symptoms**: Client OOM or crashes

**Solutions:**
1. **Reduce pre-generated batches**: Modify script to generate 100-200 batches instead of 500
2. **Smaller batch sizes**: Less memory per batch
3. **More system RAM**: Load testing is memory-intensive

---

## Best Practices

### 1. Always Use Columnar Format

Columnar format is 5-10x faster than row-based:

```bash
# Good - columnar format (default for logs script)
./scripts/metrics_load_test.py --columnar

# Bad - row-based format (legacy)
./scripts/metrics_load_test.py  # without --columnar
```

### 2. Start Small, Scale Up

```bash
# Step 1: Validate setup with small load
./scripts/metrics_load_test.py --rps 10000 --duration 10

# Step 2: Find single-worker limit
./scripts/metrics_load_test.py --rps 50000 --duration 30 --workers 1

# Step 3: Scale to target
./scripts/metrics_load_test.py --rps 500000 --duration 60
```

### 3. Monitor Arc During Tests

Always watch Arc's resource usage:
- CPU should be 60-90% (not 100% = saturated, not < 30% = underutilized)
- Memory should be stable (not growing = memory leak)
- Disk I/O should be steady (not spiking = compaction)

### 4. Run Multiple Test Iterations

Run each test 3 times and average results:

```bash
for i in {1..3}; do
    echo "=== Run $i ==="
    ./scripts/metrics_load_test.py --rps 500000 --duration 60
    sleep 30  # Let Arc settle between runs
done
```

### 5. Test Realistic Workloads

Don't just test peak throughput - test your actual use case:

```bash
# Example: Simulate 200 services @ 5K logs/sec each = 1M logs/sec
./scripts/synthetic_logs_load_test.py \
    --rps 1000000 \
    --batch-size 5000 \
    --num-services 200 \
    --duration 300  # 5 minutes
```

---

## Performance Baselines

### Expected Performance

Based on Arc's tested performance:

**Metrics Ingestion:**
- **Single node**: 100K-500K metrics/sec (typical production)
- **Optimized**: 1M-2M metrics/sec (with tuning)
- **World record**: 2.91M metrics/sec (14-core, 500 workers, batch_size=1000)

**Logs Ingestion:**
- **Optimized (1K batches)**: 968K logs/sec, p50: 7.68ms, p99: 58.35ms
- **Medium batches (5K)**: 100K logs/sec, p99 < 200ms
- **Large batches (10K)**: 200K logs/sec, p99 < 400ms
- **Huge batches (20K)**: 955K logs/sec, p99 < 2s

### Hardware Requirements

**For 500K metrics/sec:**
- CPU: 8 cores minimum
- RAM: 8 GB minimum
- Disk: SSD required
- Network: 1 Gbps

**For 2.91M metrics/sec:**
- CPU: 14+ cores
- RAM: 36+ GB
- Disk: NVMe SSD
- Network: 10 Gbps
- Configuration: 500 workers, batch_size=1000, pregenerate=2000

**For 968K logs/sec:**
- CPU: 14+ cores
- RAM: 16+ GB
- Disk: NVMe SSD
- Network: 10 Gbps
- Configuration: 400 workers, batch_size=1000

---

## References

- [Metrics Load Test Script](../scripts/metrics_load_test.py)
- [Logs Load Test Script](../scripts/synthetic_logs_load_test.py)
- [Traces Load Test Script](../scripts/synthetic_traces_load_test.py)
- [Events Load Test Script](../scripts/synthetic_events_load_test.py)
- [Demo Scenario Script](../scripts/demo_scenario_marketing_campaign.py)
- [Log Ingestion Documentation](./LOG_INGESTION.md)
- [Traces Ingestion Documentation](./TRACES_INGESTION.md)
- [Events Ingestion Documentation](./EVENTS_INGESTION.md)
- [Performance Benchmarks](./BENCHMARKS.md)
- [Architecture Documentation](./ARCHITECTURE.md)

---

## Demo Scenario: Marketing Campaign

The [`demo_scenario_marketing_campaign.py`](../scripts/demo_scenario_marketing_campaign.py) script generates a realistic unified observability scenario showing how a marketing campaign impacts your system across all four data types.

### What It Generates

**Timeline**: 90 minutes total (30 min before, 30 min during, 30 min after campaign)

**Data Generated**:
- **4 Events**: Campaign start/end, autoscaling, database alert
- **180 Metrics**: CPU and memory for 5 hosts (20-second intervals)
- **5,000 Logs**: Application logs with realistic error distribution
- **1,000 Traces**: ~3,000 spans showing latency impact

**Scenario Story**:
1. **T-30min**: Normal baseline (20-30% CPU, minimal errors, 45ms avg latency)
2. **T=0**: Marketing campaign starts (event: `marketing_campaign_started`)
3. **T+5min**: System auto-scales (event: `autoscale_triggered`)
4. **T+10min**: Database connection pool exhausted (event: `alert_triggered`)
5. **T+0 to T+30min**: High load period (75-95% CPU, 10% error rate, 2.4s avg latency)
6. **T+30min**: Campaign ends (event: `marketing_campaign_ended`)
7. **T+30 to T+60min**: System recovers to normal

### Usage

```bash
# Basic usage
./scripts/demo_scenario_marketing_campaign.py \
    --token YOUR_API_TOKEN \
    --database demo

# Custom campaign start time
./scripts/demo_scenario_marketing_campaign.py \
    --token YOUR_API_TOKEN \
    --database demo \
    --campaign-time 1730246400000  # Unix timestamp in milliseconds

# Different Arc server
./scripts/demo_scenario_marketing_campaign.py \
    --url http://arc-server:8000 \
    --token YOUR_API_TOKEN \
    --database production_demo
```

### Sample Output

```
🎬 Arc Demo Scenario: Marketing Campaign Impact
============================================================
📅 Campaign start time: 2024-10-30 10:00:00
⏱️  Timeline: 30 min before → 30 min during → 30 min after
🎯 Target: http://localhost:8000
💾 Database: demo

🔄 Generating scenario data...
  ✅ Events: 4
  ✅ Metrics: 180
  ✅ Logs: 5000
  ✅ Traces: 3000 spans

📤 Sending events...
  ✅ Events sent
📤 Sending metrics...
  ✅ Metrics sent (180 points)
📤 Sending logs...
  ✅ Logs sent (5000 entries)
📤 Sending traces...
  ✅ Traces sent (3000 spans)

✨ Scenario complete!

🔍 Try these queries to explore the data:

1. Find events during CPU spikes:
   SELECT e.time, e.event_type, m.value FROM system_events e
   JOIN system_metrics m ON m.time BETWEEN e.time AND e.time + 300000
   WHERE m.metric = 'cpu_usage' AND m.value > 80;
```

### Correlation Queries

After running the demo scenario, you can explore the data with these queries:

#### Find What Triggered the CPU Spike

```sql
-- What events happened when CPU spiked?
SELECT
    e.time,
    e.event_type,
    e.metadata,
    m.value as cpu_value
FROM system_events e
JOIN system_metrics m
    ON m.time BETWEEN e.time AND e.time + 300000
    AND m.metric = 'cpu_usage'
    AND m.value > 80
WHERE e.event_category = 'business'
ORDER BY e.time DESC;
```

#### Show Errors During Campaign

```sql
-- Show me errors during the campaign
SELECT
    e.time as campaign_start,
    l.time as log_time,
    l.level,
    l.message,
    l.service
FROM system_events e
JOIN application_logs l
    ON l.time BETWEEN e.time AND e.time + e.duration_ms
    AND l.level = 'ERROR'
WHERE e.event_type = 'marketing_campaign_started'
ORDER BY l.time DESC;
```

#### Compare Latency Before/During/After

```sql
-- How did latency change during the campaign?
SELECT
    CASE
        WHEN t.time < e.time THEN 'before_campaign'
        WHEN t.time BETWEEN e.time AND e.time + 1800000 THEN 'during_campaign'
        ELSE 'after_campaign'
    END as phase,
    COUNT(*) as request_count,
    AVG(t.duration_ns / 1000000.0) as avg_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.duration_ns / 1000000.0) as p95_latency_ms
FROM distributed_traces t
CROSS JOIN system_events e
WHERE e.event_type = 'marketing_campaign_started'
    AND t.service_name = 'api-gateway'
GROUP BY phase
ORDER BY phase;
```

### Use Cases

**Demo & Sales**: Show unified observability in action with a relatable scenario

**Training**: Teach teams how to correlate metrics, logs, traces, and events

**Testing**: Validate Arc's correlation queries work correctly

**Blog Posts**: Generate realistic data for documentation examples

**Proof of Concept**: Demonstrate Arc's capabilities to stakeholders

---

**Last Updated**: October 30, 2025
**Arc Version**: Latest main branch
