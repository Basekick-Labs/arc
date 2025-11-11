#!/usr/bin/env python3
"""
Synthetic Traces Load Test for Arc

Generates realistic distributed trace data using MessagePack columnar format.
Simulates microservices traces with spans, parent-child relationships, and realistic timings.

Expected: 500K-1M spans/sec with columnar format
"""

import msgpack
import aiohttp
import asyncio
import argparse
import gzip
import random
import time
from datetime import datetime
from typing import List
import hashlib
import uuid


class SyntheticTracesDataPool:
    """Pool of pre-generated and pre-compressed synthetic trace batches"""

    def __init__(self, num_batches: int, batch_size: int, num_services: int = 20):
        self.batches = []

        print(f"🔄 Pre-generating {num_batches} trace batches ({num_batches * batch_size:,} spans)")

        # Service topology - realistic microservices
        services = [
            "api-gateway",
            "auth-service",
            "user-service",
            "product-service",
            "order-service",
            "payment-service",
            "inventory-service",
            "notification-service",
            "email-service",
            "sms-service",
            "analytics-service",
            "recommendation-service",
            "search-service",
            "cache-service",
            "database-service",
            "queue-service",
            "storage-service",
            "cdn-service",
            "logging-service",
            "monitoring-service"
        ][:num_services]

        # Span operation names per service
        operations = {
            "api-gateway": ["HTTP GET", "HTTP POST", "HTTP PUT", "HTTP DELETE", "route_request", "authenticate"],
            "auth-service": ["verify_token", "login", "logout", "refresh_token", "validate_session"],
            "user-service": ["get_user", "create_user", "update_user", "delete_user", "list_users"],
            "product-service": ["get_product", "search_products", "update_inventory", "check_availability"],
            "order-service": ["create_order", "get_order", "update_order", "cancel_order", "process_payment"],
            "payment-service": ["charge_card", "refund", "validate_payment", "process_transaction"],
            "inventory-service": ["check_stock", "reserve_items", "release_items", "update_stock"],
            "notification-service": ["send_notification", "queue_message", "deliver_message"],
            "email-service": ["send_email", "render_template", "queue_email"],
            "database-service": ["SELECT", "INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT"],
            "cache-service": ["get", "set", "delete", "flush"],
        }

        # Default operations for services not in the map
        default_operations = ["process", "execute", "handle_request", "query"]

        environments = ["production", "staging", "development"]
        regions = ["us-east-1", "us-west-2", "eu-central-1", "ap-south-1"]

        # HTTP methods and status codes
        http_methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]
        success_codes = [200, 201, 204]
        error_codes = [400, 401, 403, 404, 500, 502, 503, 504]

        # Span kinds (OpenTelemetry standard)
        span_kinds = ["server", "client", "internal", "producer", "consumer"]

        start_time = time.perf_counter()

        for i in range(num_batches):
            base_timestamp = int(time.time() * 1000)  # milliseconds (Arc expects milliseconds)

            # COLUMNAR FORMAT for maximum performance
            timestamps = []
            trace_ids = []
            span_ids = []
            parent_span_ids = []
            service_names = []
            operation_names = []
            span_kinds_batch = []
            durations = []  # nanoseconds
            status_codes_batch = []
            http_methods_batch = []
            environments_batch = []
            regions_batch = []
            error_flags = []

            for j in range(batch_size):
                # Generate trace ID (unique per ~10 spans to simulate related spans)
                if j % 10 == 0:
                    current_trace_id = uuid.uuid4().hex
                    current_parent_span = None

                trace_id = current_trace_id
                span_id = uuid.uuid4().hex[:16]

                # 30% of spans have a parent (create span hierarchy)
                if random.random() < 0.3 and current_parent_span:
                    parent_span_id = current_parent_span
                else:
                    parent_span_id = ""
                    current_parent_span = span_id

                # Select service and operation
                service = random.choice(services)
                operation = random.choice(operations.get(service, default_operations))

                # Span kind (mostly server and client)
                span_kind = random.choices(
                    span_kinds,
                    weights=[40, 30, 20, 5, 5]
                )[0]

                # Duration varies by operation type
                # Database and cache ops are fast, API calls are slower
                if "database" in service.lower() or "cache" in service.lower():
                    duration = random.randint(1_000_000, 50_000_000)  # 1-50ms
                elif "gateway" in service.lower() or "api" in operation.lower():
                    duration = random.randint(10_000_000, 500_000_000)  # 10-500ms
                else:
                    duration = random.randint(5_000_000, 200_000_000)  # 5-200ms

                # HTTP metadata (for server/client spans)
                if span_kind in ["server", "client"]:
                    http_method = random.choice(http_methods)
                    # 5% error rate
                    if random.random() < 0.05:
                        status_code = random.choice(error_codes)
                        error = True
                        # Errors take longer
                        duration = int(duration * random.uniform(1.5, 3.0))
                    else:
                        status_code = random.choice(success_codes)
                        error = False
                else:
                    http_method = ""
                    status_code = 0
                    error = random.random() < 0.02  # 2% internal errors

                env = random.choice(environments)
                region = random.choice(regions)

                # Timestamp with slight offset to show progression
                timestamp = base_timestamp + j  # 1ms apart

                # Append to columnar arrays
                timestamps.append(timestamp)
                trace_ids.append(trace_id)
                span_ids.append(span_id)
                parent_span_ids.append(parent_span_id)
                service_names.append(service)
                operation_names.append(operation)
                span_kinds_batch.append(span_kind)
                durations.append(duration)
                status_codes_batch.append(status_code)
                http_methods_batch.append(http_method)
                environments_batch.append(env)
                regions_batch.append(region)
                error_flags.append(error)

            # Create columnar payload
            payload_dict = {
                "m": "distributed_traces",  # measurement
                "columns": {
                    "time": timestamps,
                    "trace_id": trace_ids,
                    "span_id": span_ids,
                    "parent_span_id": parent_span_ids,
                    "service_name": service_names,
                    "operation_name": operation_names,
                    "span_kind": span_kinds_batch,
                    "duration_ns": durations,
                    "status_code": status_codes_batch,
                    "http_method": http_methods_batch,
                    "environment": environments_batch,
                    "region": regions_batch,
                    "error": error_flags
                }
            }

            # Serialize and compress
            payload_binary = msgpack.packb(payload_dict)
            payload_compressed = gzip.compress(payload_binary)

            self.batches.append(payload_compressed)

            # Progress indicator
            if (i + 1) % 100 == 0:
                elapsed = time.perf_counter() - start_time
                rate = (i + 1) / elapsed
                print(f"  Progress: {i+1}/{num_batches} ({rate:.0f} batches/sec)")

        elapsed = time.perf_counter() - start_time
        avg_size = sum(len(b) for b in self.batches) / len(self.batches)
        total_size = sum(len(b) for b in self.batches)

        print(f"✅ Pre-generation complete!")
        print(f"   Time taken: {elapsed:.1f}s")
        print(f"   Total batches: {len(self.batches)}")
        print(f"   Total spans: {len(self.batches) * batch_size:,}")
        print(f"   Avg compressed size: {avg_size/1024:.1f} KB")
        print(f"   Total size: {total_size/1024/1024:.1f} MB")
        print()

    def get_batch(self) -> bytes:
        """Get a random pre-generated batch"""
        return random.choice(self.batches)


async def send_batch(
    session: aiohttp.ClientSession,
    url: str,
    token: str,
    database: str,
    data_pool: SyntheticTracesDataPool,
    latencies: List[float]
):
    """Send a single batch to Arc"""
    start = time.perf_counter()

    try:
        batch = data_pool.get_batch()

        async with session.post(
            url,
            data=batch,
            headers={
                "Content-Type": "application/msgpack",
                "Content-Encoding": "gzip",
                "Authorization": f"Bearer {token}",
                "X-Arc-Database": database
            }
        ) as response:
            await response.read()

            latency = (time.perf_counter() - start) * 1000  # ms
            latencies.append(latency)

            return response.status == 204

    except Exception as e:
        print(f"Error: {e}")
        return False


async def worker(
    session: aiohttp.ClientSession,
    url: str,
    token: str,
    database: str,
    data_pool: SyntheticTracesDataPool,
    batch_size: int,
    target_rps: int,
    duration: int,
    worker_id: int,
    stats: dict
):
    """Worker that sends batches at target rate"""
    batches_per_second = target_rps / batch_size
    interval = 1.0 / batches_per_second if batches_per_second > 0 else 0.1

    start_time = time.time()
    batches_sent = 0
    errors = 0
    latencies = []

    # Initialize stats immediately so monitor can see it
    stats[worker_id] = {
        "batches": 0,
        "errors": 0,
        "latencies": []
    }

    while time.time() - start_time < duration:
        success = await send_batch(session, url, token, database, data_pool, latencies)

        if success:
            batches_sent += 1
        else:
            errors += 1

        # Update stats continuously for monitoring
        stats[worker_id] = {
            "batches": batches_sent,
            "errors": errors,
            "latencies": latencies.copy()
        }

        await asyncio.sleep(interval)


async def monitor(stats: dict, batch_size: int, target_rps: int, start_time: float, duration: int):
    """Monitor and print statistics"""
    last_total = 0

    while time.time() - start_time < duration:
        await asyncio.sleep(5)

        elapsed = time.time() - start_time
        total_batches = sum(s.get("batches", 0) for s in stats.values())
        total_errors = sum(s.get("errors", 0) for s in stats.values())
        total_records = total_batches * batch_size

        # Calculate current RPS (last 5 seconds)
        current_batches = total_batches - last_total
        current_rps = (current_batches * batch_size) / 5
        last_total = total_batches

        # Collect all latencies
        all_latencies = []
        for s in stats.values():
            all_latencies.extend(s.get("latencies", []))

        if all_latencies:
            all_latencies.sort()
            p50 = all_latencies[len(all_latencies) // 2]
            p95 = all_latencies[int(len(all_latencies) * 0.95)]
            p99 = all_latencies[int(len(all_latencies) * 0.99)]

            print(f"[{elapsed:6.1f}s] RPS: {current_rps:8.0f} (target: {target_rps}) | "
                  f"Total: {total_records:12,} | Errors: {total_errors:6} | "
                  f"Latency (ms) - p50: {p50:6.1f} p95: {p95:6.1f} p99: {p99:6.1f}", flush=True)


async def run_load_test(
    url: str,
    token: str,
    target_rps: int,
    duration: int,
    batch_size: int,
    workers: int = None,
    database: str = "default"
):
    """Run synthetic traces load test"""

    # Pre-generate data
    num_batches = 500
    data_pool = SyntheticTracesDataPool(num_batches, batch_size)

    # Calculate workers
    if workers is None:
        num_workers = min(max(int(target_rps / 10000), 1), 30)
    else:
        num_workers = workers
    print(f"Starting {num_workers} workers...")
    print()

    # Create session and workers
    if num_workers <= 100:
        connector_limit = num_workers + 50
    elif num_workers <= 300:
        connector_limit = 200
    else:
        connector_limit = 256

    print(f"Connection pool: {connector_limit} connections for {num_workers} workers")
    print()

    connector = aiohttp.TCPConnector(limit=connector_limit, limit_per_host=connector_limit)
    timeout = aiohttp.ClientTimeout(total=30)

    print("Warming up connection pool...")
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Warmup - send a few batches to establish connections
        warmup_tasks = [
            send_batch(session, url, token, database, data_pool, [])
            for _ in range(min(num_workers, 10))
        ]
        await asyncio.gather(*warmup_tasks)
        print("✅ Connection pool warmed up")
        print()

    # Print test configuration
    print("=" * 80)
    print("Arc Synthetic Traces Load Test - MessagePack Columnar Format")
    print("=" * 80)
    print(f"Target RPS:      {target_rps:,} spans/sec")
    print(f"Batch Size:      {batch_size:,} spans/batch")
    print(f"Duration:        {duration}s")
    print(f"Database:        {database}")
    print(f"Services:        {20}")
    print(f"Pre-generated:   {num_batches} batches in memory")
    print(f"Protocol:        MessagePack + Direct Arrow/Parquet")
    print(f"Arc URL:         {url}")
    print("=" * 80)
    print()

    # Run load test
    connector = aiohttp.TCPConnector(limit=connector_limit, limit_per_host=connector_limit)
    stats = {}
    start_time = time.time()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            worker(session, url, token, database, data_pool, batch_size,
                   target_rps // num_workers, duration, i, stats)
            for i in range(num_workers)
        ]

        # Add monitor task
        tasks.append(monitor(stats, batch_size, target_rps, start_time, duration))

        await asyncio.gather(*tasks)

    # Final statistics
    elapsed = time.time() - start_time
    total_batches = sum(s.get("batches", 0) for s in stats.values())
    total_errors = sum(s.get("errors", 0) for s in stats.values())
    total_records = total_batches * batch_size

    # Collect all latencies
    all_latencies = []
    for s in stats.values():
        all_latencies.extend(s.get("latencies", []))

    all_latencies.sort()
    p50 = all_latencies[len(all_latencies) // 2] if all_latencies else 0
    p95 = all_latencies[int(len(all_latencies) * 0.95)] if all_latencies else 0
    p99 = all_latencies[int(len(all_latencies) * 0.99)] if all_latencies else 0
    p999 = all_latencies[int(len(all_latencies) * 0.999)] if all_latencies else 0

    actual_rps = total_records / elapsed
    achievement = (actual_rps / target_rps) * 100

    print()
    print("=" * 80)
    print("Test Complete")
    print("=" * 80)
    print(f"Duration:        {elapsed:.1f}s")
    print(f"Total Sent:      {total_records:,} spans")
    print(f"Total Errors:    {total_errors}")
    print(f"Success Rate:    {((total_records - total_errors) / total_records * 100) if total_records else 0:.2f}%")
    print(f"Actual RPS:      {actual_rps:,.0f} spans/sec")
    print(f"Target RPS:      {target_rps:,} spans/sec")
    print(f"Achievement:     {achievement:.1f}%")
    print()
    print(f"Latency Percentiles (ms):")
    print(f"  p50:  {p50:.2f}")
    print(f"  p95:  {p95:.2f}")
    print(f"  p99:  {p99:.2f}")
    print(f"  p999: {p999:.2f}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Arc Synthetic Traces Load Test")
    parser.add_argument("--url", default="http://localhost:8000/api/v1/write/msgpack",
                        help="Arc write endpoint URL")
    parser.add_argument("--token", required=True, help="API authentication token")
    parser.add_argument("--database", default="traces", help="Database name")
    parser.add_argument("--rps", type=int, default=100000, help="Target spans per second")
    parser.add_argument("--duration", type=int, default=30, help="Test duration in seconds")
    parser.add_argument("--batch-size", type=int, default=1000, help="Spans per batch")
    parser.add_argument("--workers", type=int, default=None, help="Number of concurrent workers")
    parser.add_argument("--num-services", type=int, default=20, help="Number of simulated services")

    args = parser.parse_args()

    asyncio.run(run_load_test(
        url=args.url,
        token=args.token,
        target_rps=args.rps,
        duration=args.duration,
        batch_size=args.batch_size,
        workers=args.workers,
        database=args.database
    ))
