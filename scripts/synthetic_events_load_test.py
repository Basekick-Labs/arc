#!/usr/bin/env python3
"""
Synthetic Events Load Test for Arc

Generates realistic system and business events using MessagePack columnar format.
Simulates infrastructure events (deployments, scaling), business events (signups, payments),
and security events (auth attempts, violations).

Expected: 1M+ events/sec with columnar format
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
import uuid
import json


class SyntheticEventsDataPool:
    """Pool of pre-generated and pre-compressed synthetic event batches"""

    def __init__(self, num_batches: int, batch_size: int):
        self.batches = []

        print(f"🔄 Pre-generating {num_batches} event batches ({num_batches * batch_size:,} events)")

        # Event types and categories
        event_types = {
            # Infrastructure events
            "deployment_started": "infrastructure",
            "deployment_completed": "infrastructure",
            "deployment_failed": "infrastructure",
            "auto_scale_up": "infrastructure",
            "auto_scale_down": "infrastructure",
            "server_restart": "infrastructure",
            "config_change": "infrastructure",
            "certificate_renewed": "infrastructure",
            "database_failover": "infrastructure",
            "cache_invalidated": "infrastructure",

            # Business events
            "user_signup": "business",
            "user_login": "business",
            "subscription_created": "business",
            "subscription_upgraded": "business",
            "subscription_cancelled": "business",
            "payment_processed": "business",
            "payment_failed": "business",
            "order_placed": "business",
            "order_completed": "business",
            "order_cancelled": "business",
            "feature_flag_toggled": "business",

            # Security events
            "authentication_success": "security",
            "authentication_failed": "security",
            "authorization_denied": "security",
            "rate_limit_exceeded": "security",
            "api_key_rotated": "security",
            "suspicious_activity": "security",
            "password_reset": "security",
            "mfa_enabled": "security",

            # Application events
            "background_job_started": "application",
            "background_job_completed": "application",
            "background_job_failed": "application",
            "circuit_breaker_opened": "application",
            "circuit_breaker_closed": "application",
            "health_check_failed": "application",
            "health_check_recovered": "application",
        }

        severities = ["info", "warning", "error", "critical"]
        environments = ["production", "staging", "development"]
        services = [
            "api-gateway", "auth-service", "user-service",
            "payment-service", "order-service", "notification-service",
            "analytics-service", "search-service", "recommendation-service"
        ]

        # Metadata templates for different event types
        versions = ["v1.2.3", "v1.2.4", "v1.3.0", "v2.0.0", "v2.1.0"]
        plans = ["free", "starter", "professional", "enterprise"]
        regions = ["us-east-1", "us-west-2", "eu-central-1", "ap-south-1"]

        start_time = time.perf_counter()

        for i in range(num_batches):
            base_timestamp = int(time.time() * 1000)

            # COLUMNAR FORMAT for maximum performance
            timestamps = []
            event_type_batch = []
            event_category_batch = []
            severity_batch = []
            source_batch = []
            environment_batch = []
            user_ids = []
            resource_ids = []
            metadata_batch = []
            duration_ms_batch = []
            success_batch = []
            amounts = []  # For business events with monetary values

            for j in range(batch_size):
                # Select random event type
                event_type = random.choice(list(event_types.keys()))
                event_category = event_types[event_type]

                # Determine severity based on event type
                if "failed" in event_type or "denied" in event_type or "exceeded" in event_type:
                    severity = random.choice(["error", "critical"])
                    success = False
                elif "completed" in event_type or "success" in event_type or "recovered" in event_type:
                    severity = "info"
                    success = True
                elif "started" in event_type or "opened" in event_type:
                    severity = random.choice(["info", "warning"])
                    success = True
                else:
                    severity = random.choice(severities)
                    success = random.random() > 0.1  # 90% success rate

                source = random.choice(services)
                environment = random.choice(environments)

                # Generate metadata based on event type
                metadata = {}
                user_id = ""
                resource_id = ""
                duration_ms = 0
                amount = 0.0

                if "deployment" in event_type:
                    version = random.choice(versions)
                    metadata = {
                        "version": version,
                        "replicas": random.randint(2, 10),
                        "strategy": random.choice(["rolling", "blue-green", "canary"])
                    }
                    resource_id = f"deployment-{uuid.uuid4().hex[:8]}"
                    if "completed" in event_type or "failed" in event_type:
                        duration_ms = random.randint(60000, 600000)  # 1-10 minutes

                elif "scale" in event_type:
                    old_count = random.randint(2, 10)
                    new_count = old_count + random.randint(1, 5) if "up" in event_type else old_count - random.randint(1, 3)
                    metadata = {
                        "old_count": old_count,
                        "new_count": max(1, new_count),
                        "reason": random.choice(["cpu_threshold", "memory_threshold", "request_rate"])
                    }
                    resource_id = f"autoscale-{uuid.uuid4().hex[:8]}"

                elif event_category == "business":
                    user_id = f"user-{random.randint(1000, 9999)}"

                    if "signup" in event_type:
                        metadata = {
                            "source": random.choice(["web", "mobile", "api"]),
                            "referral": random.choice(["", "google", "friend", "ad"])
                        }

                    elif "subscription" in event_type:
                        plan = random.choice(plans)
                        metadata = {
                            "plan": plan,
                            "billing_cycle": random.choice(["monthly", "yearly"])
                        }
                        if "upgraded" in event_type:
                            amount = random.choice([9.99, 29.99, 99.99, 299.99])

                    elif "payment" in event_type:
                        amount = random.uniform(10.0, 500.0)
                        metadata = {
                            "payment_method": random.choice(["card", "paypal", "bank_transfer"]),
                            "currency": "USD"
                        }
                        resource_id = f"payment-{uuid.uuid4().hex[:8]}"

                    elif "order" in event_type:
                        amount = random.uniform(20.0, 1000.0)
                        metadata = {
                            "item_count": random.randint(1, 10),
                            "shipping": random.choice(["standard", "express", "overnight"])
                        }
                        resource_id = f"order-{uuid.uuid4().hex[:8]}"
                        if "completed" in event_type:
                            duration_ms = random.randint(300000, 7200000)  # 5 mins - 2 hours

                elif event_category == "security":
                    user_id = f"user-{random.randint(1000, 9999)}"
                    metadata = {
                        "source_ip": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
                        "user_agent": random.choice(["Mozilla/5.0", "curl/7.68.0", "PostmanRuntime/7.26.8"])
                    }

                    if "rate_limit" in event_type:
                        metadata["requests_per_minute"] = random.randint(100, 1000)
                        metadata["limit"] = 100

                elif event_category == "application":
                    if "job" in event_type:
                        job_types = ["email_batch", "report_generation", "data_sync", "cleanup"]
                        metadata = {
                            "job_type": random.choice(job_types),
                            "priority": random.choice(["low", "medium", "high"])
                        }
                        resource_id = f"job-{uuid.uuid4().hex[:8]}"
                        if "completed" in event_type or "failed" in event_type:
                            duration_ms = random.randint(5000, 300000)  # 5s - 5 minutes

                    elif "circuit_breaker" in event_type:
                        metadata = {
                            "downstream_service": random.choice(services),
                            "failure_count": random.randint(5, 20) if "opened" in event_type else 0
                        }

                # Timestamp with slight offset
                timestamp = base_timestamp + j

                # Append to columnar arrays
                timestamps.append(timestamp)
                event_type_batch.append(event_type)
                event_category_batch.append(event_category)
                severity_batch.append(severity)
                source_batch.append(source)
                environment_batch.append(environment)
                user_ids.append(user_id)
                resource_ids.append(resource_id)
                metadata_batch.append(json.dumps(metadata) if metadata else "")
                duration_ms_batch.append(duration_ms)
                success_batch.append(success)
                amounts.append(round(amount, 2))

            # Create columnar payload
            payload_dict = {
                "m": "system_events",
                "columns": {
                    "time": timestamps,
                    "event_type": event_type_batch,
                    "event_category": event_category_batch,
                    "severity": severity_batch,
                    "source": source_batch,
                    "environment": environment_batch,
                    "user_id": user_ids,
                    "resource_id": resource_ids,
                    "metadata": metadata_batch,
                    "duration_ms": duration_ms_batch,
                    "success": success_batch,
                    "amount": amounts
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
        print(f"   Total events: {len(self.batches) * batch_size:,}")
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
    data_pool: SyntheticEventsDataPool,
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
    data_pool: SyntheticEventsDataPool,
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

    while time.time() - start_time < duration:
        success = await send_batch(session, url, token, database, data_pool, latencies)

        if success:
            batches_sent += 1
        else:
            errors += 1

        await asyncio.sleep(interval)

    stats[worker_id] = {
        "batches": batches_sent,
        "errors": errors,
        "latencies": latencies
    }


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
                  f"Latency (ms) - p50: {p50:6.1f} p95: {p95:6.1f} p99: {p99:6.1f}")


async def run_load_test(
    url: str,
    token: str,
    target_rps: int,
    duration: int,
    batch_size: int,
    workers: int = None,
    database: str = "default"
):
    """Run synthetic events load test"""

    # Pre-generate data
    num_batches = 500
    data_pool = SyntheticEventsDataPool(num_batches, batch_size)

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
    print("Arc Synthetic Events Load Test - MessagePack Columnar Format")
    print("=" * 80)
    print(f"Target RPS:      {target_rps:,} events/sec")
    print(f"Batch Size:      {batch_size:,} events/batch")
    print(f"Duration:        {duration}s")
    print(f"Database:        {database}")
    print(f"Event Types:     Infrastructure, Business, Security, Application")
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
    print(f"Total Sent:      {total_records:,} events")
    print(f"Total Errors:    {total_errors}")
    print(f"Success Rate:    {((total_records - total_errors) / total_records * 100) if total_records else 0:.2f}%")
    print(f"Actual RPS:      {actual_rps:,.0f} events/sec")
    print(f"Target RPS:      {target_rps:,} events/sec")
    print(f"Achievement:     {achievement:.1f}%")
    print()
    print(f"Latency Percentiles (ms):")
    print(f"  p50:  {p50:.2f}")
    print(f"  p95:  {p95:.2f}")
    print(f"  p99:  {p99:.2f}")
    print(f"  p999: {p999:.2f}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Arc Synthetic Events Load Test")
    parser.add_argument("--url", default="http://localhost:8000/api/v1/write/msgpack",
                        help="Arc write endpoint URL")
    parser.add_argument("--token", required=True, help="API authentication token")
    parser.add_argument("--database", default="events", help="Database name")
    parser.add_argument("--rps", type=int, default=100000, help="Target events per second")
    parser.add_argument("--duration", type=int, default=30, help="Test duration in seconds")
    parser.add_argument("--batch-size", type=int, default=1000, help="Events per batch")
    parser.add_argument("--workers", type=int, default=None, help="Number of concurrent workers")

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
