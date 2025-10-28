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

Arc provides three specialized load testing scripts for complete observability testing:

1. **[`scripts/metrics_load_test.py`](../scripts/metrics_load_test.py)** - For testing metrics ingestion (2.45M+ metrics/sec)
2. **[`scripts/synthetic_logs_load_test.py`](../scripts/synthetic_logs_load_test.py)** - For testing log ingestion (955K+ logs/sec)
3. **[`scripts/synthetic_traces_load_test.py`](../scripts/synthetic_traces_load_test.py)** - For testing distributed traces (944K+ spans/sec)

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

# High-throughput test - 2.45M metrics/sec (world record!)
./scripts/metrics_load_test.py \
    --token YOUR_API_TOKEN \
    --rps 2500000 \
    --duration 60 \
    --batch-size 1000 \
    --workers 600 \
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

Arc achieved **2.45 million metrics/second** using this script:

```bash
./scripts/metrics_load_test.py \
    --token YOUR_TOKEN \
    --rps 2500000 \
    --duration 60 \
    --batch-size 1000 \
    --workers 600 \
    --columnar
```

**Results:**
- **2.45M metrics/sec** sustained
- **p99 latency**: ~18ms
- **Zero errors**
- **Hardware**: 14-core system, 36GB RAM

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

# High-throughput test - 955K logs/sec
./scripts/synthetic_logs_load_test.py \
    --token YOUR_API_TOKEN \
    --database logs \
    --rps 1000000 \
    --duration 60 \
    --batch-size 20000 \
    --workers 600
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
🔄 Pre-generating 500 log batches (10,000,000 log entries)
  Progress: 100/500 (3 batches/sec)
  Progress: 200/500 (3 batches/sec)
  Progress: 300/500 (3 batches/sec)
  Progress: 400/500 (3 batches/sec)
  Progress: 500/500 (3 batches/sec)
✅ Pre-generation complete!
   Time taken: 154.4s
   Total batches: 500
   Total log entries: 10,000,000
   Avg compressed size: 478.0 KB
   Total size: 233.4 MB

================================================================================
Arc Synthetic Logs Load Test - MessagePack Columnar Format
================================================================================
Target RPS:      1,000,000 logs/sec
Batch Size:      20,000 logs/batch
Duration:        60s
Database:        logs
Services:        50
Pre-generated:   500 batches in memory
Protocol:        MessagePack + Direct Arrow/Parquet
Arc URL:         http://localhost:8000
================================================================================

Starting 600 workers...
Connection pool: 256 connections for 600 workers
Warming up connection pool...
✅ Connection pool warmed up

[   5.0s] RPS: 2400000 (target: 1000000) | Total:   12,000,000 | Errors:      0 | Latency (ms) - p50: 1162.7 p95: 1827.4 p99: 1833.7
[  10.0s] RPS:       0 (target: 1000000) | Total:   12,000,000 | Errors:      0 | Latency (ms) - p50: 1162.7 p95: 1827.4 p99: 1833.7
[  15.0s] RPS: 2400000 (target: 1000000) | Total:   24,000,000 | Errors:      0 | Latency (ms) - p50:  451.3 p95: 1515.7 p99: 1832.6
[  20.0s] RPS:       0 (target: 1000000) | Total:   24,000,000 | Errors:      0 | Latency (ms) - p50:  451.3 p95: 1515.7 p99: 1832.6
[  25.0s] RPS:  692000 (target: 1000000) | Total:   27,460,000 | Errors:      0 | Latency (ms) - p50:  347.2 p95: 1502.4 p99: 1832.4
[  30.0s] RPS: 1708000 (target: 1000000) | Total:   36,000,000 | Errors:      0 | Latency (ms) - p50:  254.8 p95: 1490.9 p99: 1831.2

================================================================================
Test Complete
================================================================================
Duration:        62.8s
Total Sent:      60,000,000 log entries
Total Errors:    0
Success Rate:    100.00%
Actual RPS:      955,179 logs/sec
Target RPS:      1,000,000 logs/sec
Achievement:     95.5%

Latency Percentiles (ms):
  p50:  113.90
  p95:  1406.35
  p99:  1827.42
  p999: 1836.07
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
- **955K logs/sec** sustained
- **60 million logs** in 60 seconds
- **p99 latency**: 1827ms
- **Zero errors**

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

# High-throughput test - 944K spans/sec
./scripts/synthetic_traces_load_test.py \
    --token YOUR_API_TOKEN \
    --database traces \
    --rps 1000000 \
    --duration 60 \
    --batch-size 20000 \
    --workers 600
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
🔄 Pre-generating 500 trace batches (10,000,000 spans)
  Progress: 500/500 (3 batches/sec)
✅ Pre-generation complete!
   Time taken: 146.1s
   Total batches: 500
   Total spans: 10,000,000
   Avg compressed size: 506.9 KB
   Total size: 247.5 MB

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

[   5.0s] RPS: 1200000 (target: 1000000) | Total:    6,000,000 | Errors:      0
[  10.0s] RPS:  800000 (target: 1000000) | Total:   10,000,000 | Errors:      0
[  15.0s] RPS: 1000000 (target: 1000000) | Total:   15,000,000 | Errors:      0
...
[  60.0s] RPS: 1000000 (target: 1000000) | Total:   60,000,000 | Errors:      0

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
| 2.45M | 1,000 | 600 | < 40ms |

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

**At 955K logs/sec:**
- CPU: 80-90% (peak bursts)
- Memory: 12-15 GB
- Disk Write: 40-80 MB/s
- Network: 200-500 MB/s (bursty)

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
- **World record**: 2.45M metrics/sec (14-core, 600 workers)

**Logs Ingestion:**
- **Small batches (1K)**: 45K logs/sec, p99 < 50ms
- **Medium batches (5K)**: 100K logs/sec, p99 < 200ms
- **Large batches (10K)**: 200K logs/sec, p99 < 400ms
- **Huge batches (20K)**: 955K logs/sec, p99 < 2s

### Hardware Requirements

**For 500K metrics/sec:**
- CPU: 8 cores minimum
- RAM: 8 GB minimum
- Disk: SSD required
- Network: 1 Gbps

**For 2.45M metrics/sec:**
- CPU: 14+ cores
- RAM: 16+ GB
- Disk: NVMe SSD
- Network: 10 Gbps

**For 955K logs/sec:**
- CPU: 14+ cores
- RAM: 16+ GB
- Disk: NVMe SSD
- Network: 10 Gbps

---

## References

- [Metrics Load Test Script](../scripts/metrics_load_test.py)
- [Logs Load Test Script](../scripts/synthetic_logs_load_test.py)
- [Log Ingestion Documentation](./LOG_INGESTION.md)
- [Performance Benchmarks](./BENCHMARKS.md)
- [Architecture Documentation](./ARCHITECTURE.md)

---

**Last Updated**: October 28, 2025
**Arc Version**: Latest main branch
