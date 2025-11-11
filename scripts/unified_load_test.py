#!/usr/bin/env python3
"""
Arc Unified Load Test - All Four Data Types Simultaneously

Tests Arc's performance under realistic mixed workloads with metrics, logs,
traces, and events running concurrently.

Usage:
    ./scripts/unified_load_test.py --token YOUR_TOKEN --duration 60

This simulates a production environment where all four observability signals
are being ingested at the same time.
"""

import argparse
import asyncio
import gzip
import msgpack
import numpy as np
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict
import aiohttp


@dataclass
class WorkloadConfig:
    """Configuration for each data type workload"""
    name: str
    target_rps: int
    batch_size: int
    workers: int
    database: str
    color: str


@dataclass
class WorkloadResults:
    """Results from a single workload"""
    name: str
    total_sent: int
    total_errors: int
    duration: float
    latencies: List[float]

    @property
    def actual_rps(self) -> float:
        return self.total_sent / self.duration if self.duration > 0 else 0

    @property
    def success_rate(self) -> float:
        total = self.total_sent + self.total_errors
        return (self.total_sent / total * 100) if total > 0 else 0

    def percentile(self, p: float) -> float:
        if not self.latencies:
            return 0
        return float(np.percentile(self.latencies, p))


# ============================================================================
# Data Pool Classes
# ============================================================================

class DataPool:
    """Base class for pre-generated data pools"""
    def __init__(self, num_batches: int, batch_size: int):
        self.batches = []
        self.num_batches = num_batches
        self.batch_size = batch_size

    def get_batch(self) -> bytes:
        """Get a random pre-compressed batch"""
        return random.choice(self.batches)


class MetricsDataPool(DataPool):
    """Pre-generated metrics data pool"""
    def __init__(self, num_batches: int = 500, batch_size: int = 1000):
        super().__init__(num_batches, batch_size)
        print(f"🔄 Pre-generating {num_batches} metrics batches ({num_batches * batch_size:,} metrics)")
        start = time.perf_counter()

        for _ in range(num_batches):
            timestamp = int(datetime.now().timestamp() * 1_000_000)
            times = [timestamp + i * 1000 for i in range(batch_size)]

            data = {
                "m": "system_metrics",
                "columns": {
                    "time": times,
                    "cpu": np.random.uniform(10, 90, batch_size).tolist(),
                    "memory": np.random.uniform(20, 80, batch_size).tolist(),
                    "disk": np.random.uniform(0, 100, batch_size).tolist(),
                    "network_in": np.random.uniform(0, 1000, batch_size).tolist(),
                    "network_out": np.random.uniform(0, 1000, batch_size).tolist(),
                }
            }
            packed = msgpack.packb(data)
            compressed = gzip.compress(packed, compresslevel=1)
            self.batches.append(compressed)

        elapsed = time.perf_counter() - start
        avg_size = sum(len(b) for b in self.batches) / len(self.batches) / 1024
        print(f"✅ Metrics: {elapsed:.1f}s, {len(self.batches)} batches, avg {avg_size:.1f} KB")


class LogsDataPool(DataPool):
    """Pre-generated logs data pool"""
    def __init__(self, num_batches: int = 500, batch_size: int = 1000):
        super().__init__(num_batches, batch_size)
        print(f"🔄 Pre-generating {num_batches} log batches ({num_batches * batch_size:,} logs)")
        start = time.perf_counter()

        services = ["api-gateway", "auth-service", "user-service", "payment-service"]
        levels = ["INFO", "WARN", "ERROR", "DEBUG"]
        messages = ["Request processed", "Query executed", "Cache hit", "Auth success"]

        for _ in range(num_batches):
            timestamp = int(datetime.now().timestamp() * 1_000_000)
            times = [timestamp + i * 1000 for i in range(batch_size)]

            data = {
                "m": "application_logs",
                "columns": {
                    "time": times,
                    "level": [random.choice(levels) for _ in range(batch_size)],
                    "service": [random.choice(services) for _ in range(batch_size)],
                    "message": [random.choice(messages) for _ in range(batch_size)],
                    "request_id": [f"req-{random.randint(100000, 999999)}" for _ in range(batch_size)],
                }
            }
            packed = msgpack.packb(data)
            compressed = gzip.compress(packed, compresslevel=1)
            self.batches.append(compressed)

        elapsed = time.perf_counter() - start
        avg_size = sum(len(b) for b in self.batches) / len(self.batches) / 1024
        print(f"✅ Logs: {elapsed:.1f}s, {len(self.batches)} batches, avg {avg_size:.1f} KB")


class TracesDataPool(DataPool):
    """Pre-generated traces data pool"""
    def __init__(self, num_batches: int = 500, batch_size: int = 1000):
        super().__init__(num_batches, batch_size)
        print(f"🔄 Pre-generating {num_batches} trace batches ({num_batches * batch_size:,} spans)")
        start = time.perf_counter()

        services = ["frontend", "api-gateway", "auth-service", "database"]
        operations = ["http.request", "db.query", "cache.get", "grpc.call"]

        for _ in range(num_batches):
            timestamp = int(datetime.now().timestamp() * 1_000_000)
            times = [timestamp + i * 1000 for i in range(batch_size)]

            data = {
                "m": "distributed_traces",
                "columns": {
                    "time": times,
                    "trace_id": [f"trace-{random.randint(100000, 999999)}" for _ in range(batch_size)],
                    "span_id": [f"span-{i}" for i in range(batch_size)],
                    "parent_span_id": [f"span-{i-1}" if i > 0 and random.random() > 0.3 else "" for i in range(batch_size)],
                    "service_name": [random.choice(services) for _ in range(batch_size)],
                    "operation_name": [random.choice(operations) for _ in range(batch_size)],
                    "duration_ns": [random.randint(1_000_000, 500_000_000) for _ in range(batch_size)],
                    "status_code": [random.choice([200, 200, 200, 201, 400]) for _ in range(batch_size)],
                }
            }
            packed = msgpack.packb(data)
            compressed = gzip.compress(packed, compresslevel=1)
            self.batches.append(compressed)

        elapsed = time.perf_counter() - start
        avg_size = sum(len(b) for b in self.batches) / len(self.batches) / 1024
        print(f"✅ Traces: {elapsed:.1f}s, {len(self.batches)} batches, avg {avg_size:.1f} KB")


class EventsDataPool(DataPool):
    """Pre-generated events data pool"""
    def __init__(self, num_batches: int = 500, batch_size: int = 1000):
        super().__init__(num_batches, batch_size)
        print(f"🔄 Pre-generating {num_batches} event batches ({num_batches * batch_size:,} events)")
        start = time.perf_counter()

        event_types = ["deployment", "alert", "incident", "config_change"]
        severities = ["info", "warning", "critical"]
        sources = ["kubernetes", "prometheus", "grafana"]

        for _ in range(num_batches):
            timestamp = int(datetime.now().timestamp() * 1_000_000)
            times = [timestamp + i * 1000 for i in range(batch_size)]

            data = {
                "m": "system_events",
                "columns": {
                    "time": times,
                    "event_type": [random.choice(event_types) for _ in range(batch_size)],
                    "severity": [random.choice(severities) for _ in range(batch_size)],
                    "source": [random.choice(sources) for _ in range(batch_size)],
                    "description": [f"Event-{random.randint(1000, 9999)}" for _ in range(batch_size)],
                }
            }
            packed = msgpack.packb(data)
            compressed = gzip.compress(packed, compresslevel=1)
            self.batches.append(compressed)

        elapsed = time.perf_counter() - start
        avg_size = sum(len(b) for b in self.batches) / len(self.batches) / 1024
        print(f"✅ Events: {elapsed:.1f}s, {len(self.batches)} batches, avg {avg_size:.1f} KB")


# ============================================================================
# Worker Functions
# ============================================================================

async def send_batch(
    session: aiohttp.ClientSession,
    url: str,
    token: str,
    database: str,
    data: bytes,
    latencies: List[float]
) -> bool:
    """Send a pre-compressed batch"""
    headers = {
        "x-api-key": token,
        "Content-Type": "application/msgpack",
        "Content-Encoding": "gzip",
        "X-Arc-Database": database,
    }

    start = time.perf_counter()
    try:
        async with session.post(url, data=data, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
            latency_ms = (time.perf_counter() - start) * 1000
            latencies.append(latency_ms)
            return response.status in (200, 204)  # Arc returns 204 No Content on success
    except Exception:
        return False


async def worker(
    worker_id: int,
    config: WorkloadConfig,
    url: str,
    token: str,
    duration: int,
    stats: Dict,
    data_pool: DataPool
):
    """Worker that sends pre-generated batches"""
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=100)
    async with aiohttp.ClientSession(connector=connector) as session:

        batches_per_second = config.target_rps / config.batch_size
        interval = 1.0 / batches_per_second if batches_per_second > 0 else 0.1

        start_time = time.time()
        batches_sent = 0
        errors = 0
        latencies = []

        # Initialize stats
        stats[f"{config.name}_{worker_id}"] = {
            "batches": 0,
            "errors": 0,
            "latencies": []
        }

        while time.time() - start_time < duration:
            data = data_pool.get_batch()
            success = await send_batch(session, url, token, config.database, data, latencies)

            if success:
                batches_sent += 1
            else:
                errors += 1

            # Update stats
            stats[f"{config.name}_{worker_id}"] = {
                "batches": batches_sent,
                "errors": errors,
                "latencies": latencies.copy()
            }

            await asyncio.sleep(interval)


async def monitor(stats: Dict, configs: List[WorkloadConfig], start_time: float, duration: int):
    """Monitor and print combined statistics"""
    last_totals = {config.name: 0 for config in configs}

    while time.time() - start_time < duration:
        await asyncio.sleep(5)

        elapsed = time.time() - start_time
        workload_stats = {}

        for config in configs:
            total_records = 0
            total_errors = 0
            all_latencies = []

            for key, worker_stats in stats.items():
                if key.startswith(f"{config.name}_"):
                    total_records += worker_stats["batches"] * config.batch_size
                    total_errors += worker_stats["errors"]
                    all_latencies.extend(worker_stats["latencies"])

            current_rps = (total_records - last_totals[config.name]) / 5.0
            last_totals[config.name] = total_records

            p50 = float(np.percentile(all_latencies, 50)) if all_latencies else 0
            p99 = float(np.percentile(all_latencies, 99)) if all_latencies else 0

            workload_stats[config.name] = {
                "total": total_records,
                "errors": total_errors,
                "rps": current_rps,
                "p50": p50,
                "p99": p99,
            }

        grand_total = sum(ws["total"] for ws in workload_stats.values())
        grand_errors = sum(ws["errors"] for ws in workload_stats.values())
        total_rps = sum(ws["rps"] for ws in workload_stats.values())

        print(f"\n[{elapsed:6.1f}s] Combined: {total_rps:10,.0f} RPS | Total: {grand_total:15,} | Errors: {grand_errors:6}", flush=True)

        for config in configs:
            ws = workload_stats[config.name]
            print(f"  {config.color}{config.name:8s}\033[0m: {ws['rps']:8,.0f} RPS | "
                  f"{ws['total']:12,} recs | {ws['errors']:4} err | "
                  f"p50: {ws['p50']:5.1f}ms p99: {ws['p99']:6.1f}ms", flush=True)


async def run_unified_test(
    url: str,
    token: str,
    duration: int,
    configs: List[WorkloadConfig]
):
    """Run all workloads simultaneously"""

    print("\n" + "=" * 80)
    print("Arc Unified Load Test - All Four Data Types")
    print("=" * 80)

    # Pre-generate data pools
    data_pools = {
        "metrics": MetricsDataPool(num_batches=500, batch_size=configs[0].batch_size),
        "logs": LogsDataPool(num_batches=500, batch_size=configs[1].batch_size),
        "traces": TracesDataPool(num_batches=500, batch_size=configs[2].batch_size),
        "events": EventsDataPool(num_batches=500, batch_size=configs[3].batch_size),
    }

    print("\n" + "=" * 80)
    print(f"Duration:        {duration}s")
    print(f"Arc URL:         {url}")
    print()

    for config in configs:
        print(f"{config.color}{config.name:8s}\033[0m: {config.target_rps:10,} {config.name}/sec | "
              f"Batch: {config.batch_size:5,} | Workers: {config.workers:3} | DB: {config.database}")

    print("=" * 80)
    print()

    stats = {}
    start_time = time.time()

    # Create all workers and monitor
    tasks = []

    for config in configs:
        data_pool = data_pools[config.name]
        for i in range(config.workers):
            task = asyncio.create_task(
                worker(i, config, url, token, duration, stats, data_pool)
            )
            tasks.append(task)

    # Add monitor
    monitor_task = asyncio.create_task(monitor(stats, configs, start_time, duration))
    tasks.append(monitor_task)

    # Wait for completion
    await asyncio.gather(*tasks, return_exceptions=True)

    end_time = time.time()
    actual_duration = end_time - start_time

    # Collect final results
    results = []
    for config in configs:
        total_records = 0
        total_errors = 0
        all_latencies = []

        for key, worker_stats in stats.items():
            if key.startswith(f"{config.name}_"):
                total_records += worker_stats["batches"] * config.batch_size
                total_errors += worker_stats["errors"]
                all_latencies.extend(worker_stats["latencies"])

        results.append(WorkloadResults(
            name=config.name,
            total_sent=total_records,
            total_errors=total_errors,
            duration=actual_duration,
            latencies=all_latencies
        ))

    # Print final summary
    print("\n" + "=" * 80)
    print("Test Complete - Final Results")
    print("=" * 80)
    print(f"Total Duration:  {actual_duration:.1f}s\n")

    grand_total_records = 0
    grand_total_errors = 0
    grand_total_rps = 0

    for config, result in zip(configs, results):
        print(f"{config.color}{'=' * 40}")
        print(f"{config.name.upper()}")
        print(f"{'=' * 40}\033[0m")
        print(f"Total Sent:      {result.total_sent:,} {config.name}")
        print(f"Total Errors:    {result.total_errors}")
        print(f"Success Rate:    {result.success_rate:.2f}%")
        print(f"Actual RPS:      {result.actual_rps:,.0f} {config.name}/sec")
        print(f"Target RPS:      {config.target_rps:,} {config.name}/sec")
        print(f"Achievement:     {(result.actual_rps / config.target_rps * 100):.1f}%")
        print(f"\nLatency (ms): p50: {result.percentile(50):.2f} | "
              f"p95: {result.percentile(95):.2f} | p99: {result.percentile(99):.2f}\n")

        grand_total_records += result.total_sent
        grand_total_errors += result.total_errors
        grand_total_rps += result.actual_rps

    print("=" * 80)
    print("COMBINED TOTALS")
    print("=" * 80)
    print(f"Total Records:   {grand_total_records:,}")
    print(f"Total Errors:    {grand_total_errors}")
    print(f"Combined RPS:    {grand_total_rps:,.0f} records/sec")
    success_rate = (grand_total_records / (grand_total_records + grand_total_errors) * 100) if (grand_total_records + grand_total_errors) > 0 else 0
    print(f"Success Rate:    {success_rate:.2f}%")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Arc Unified Load Test - All Four Data Types Simultaneously",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--url", default="http://localhost:8000/api/v1/write/msgpack",
                       help="Arc API endpoint")
    parser.add_argument("--token", required=True,
                       help="API authentication token")
    parser.add_argument("--duration", type=int, default=60,
                       help="Test duration in seconds")

    # Metrics config
    parser.add_argument("--metrics-rps", type=int, default=1000000,
                       help="Target metrics per second")
    parser.add_argument("--metrics-batch", type=int, default=1000,
                       help="Metrics per batch")
    parser.add_argument("--metrics-workers", type=int, default=200,
                       help="Metrics workers")
    parser.add_argument("--metrics-database", type=str, default="default",
                       help="Metrics database")

    # Logs config
    parser.add_argument("--logs-rps", type=int, default=500000,
                       help="Target logs per second")
    parser.add_argument("--logs-batch", type=int, default=1000,
                       help="Logs per batch")
    parser.add_argument("--logs-workers", type=int, default=200,
                       help="Logs workers")
    parser.add_argument("--logs-database", type=str, default="logs",
                       help="Logs database")

    # Traces config
    parser.add_argument("--traces-rps", type=int, default=400000,
                       help="Target spans per second")
    parser.add_argument("--traces-batch", type=int, default=1000,
                       help="Spans per batch")
    parser.add_argument("--traces-workers", type=int, default=200,
                       help="Traces workers")
    parser.add_argument("--traces-database", type=str, default="traces",
                       help="Traces database")

    # Events config
    parser.add_argument("--events-rps", type=int, default=500000,
                       help="Target events per second")
    parser.add_argument("--events-batch", type=int, default=1000,
                       help="Events per batch")
    parser.add_argument("--events-workers", type=int, default=200,
                       help="Events workers")
    parser.add_argument("--events-database", type=str, default="events",
                       help="Events database")

    args = parser.parse_args()

    # Build configs
    configs = [
        WorkloadConfig(
            name="metrics",
            target_rps=args.metrics_rps,
            batch_size=args.metrics_batch,
            workers=args.metrics_workers,
            database=args.metrics_database,
            color="\033[92m"  # Green
        ),
        WorkloadConfig(
            name="logs",
            target_rps=args.logs_rps,
            batch_size=args.logs_batch,
            workers=args.logs_workers,
            database=args.logs_database,
            color="\033[94m"  # Blue
        ),
        WorkloadConfig(
            name="traces",
            target_rps=args.traces_rps,
            batch_size=args.traces_batch,
            workers=args.traces_workers,
            database=args.traces_database,
            color="\033[95m"  # Magenta
        ),
        WorkloadConfig(
            name="events",
            target_rps=args.events_rps,
            batch_size=args.events_batch,
            workers=args.events_workers,
            database=args.events_database,
            color="\033[93m"  # Yellow
        ),
    ]

    asyncio.run(run_unified_test(args.url, args.token, args.duration, configs))


if __name__ == "__main__":
    main()
