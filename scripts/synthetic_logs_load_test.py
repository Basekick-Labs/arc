#!/usr/bin/env python3
"""
Synthetic Logs Load Test for Arc

Generates realistic log data using MessagePack columnar format for maximum throughput.
Simulates application logs, system logs, and access logs.

Expected: 500K-1M logs/sec with columnar format
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


class SyntheticLogsDataPool:
    """Pool of pre-generated and pre-compressed synthetic log batches"""

    def __init__(self, num_batches: int, batch_size: int, num_services: int = 50):
        self.batches = []

        print(f"🔄 Pre-generating {num_batches} log batches ({num_batches * batch_size:,} log entries)")

        # Log templates
        log_levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
        services = [f"service-{i}" for i in range(1, num_services + 1)]
        environments = ["prod", "staging", "dev"]
        regions = ["us-east-1", "us-west-2", "eu-central-1", "ap-south-1"]

        # Common log messages
        messages = [
            "Request processed successfully",
            "Database query executed",
            "Cache hit",
            "Cache miss",
            "Authentication successful",
            "API request received",
            "Background job started",
            "Background job completed",
            "Configuration reloaded",
            "Health check passed",
            "Connection established",
            "Connection closed",
            "Retry attempt",
            "Timeout occurred",
            "Resource not found",
        ]

        error_messages = [
            "Connection timeout",
            "Database connection failed",
            "Invalid request payload",
            "Authentication failed",
            "Rate limit exceeded",
            "Internal server error",
            "Service unavailable",
            "Deadlock detected",
        ]

        start_time = time.perf_counter()

        for i in range(num_batches):
            timestamp = int(datetime.now().timestamp() * 1000)

            # COLUMNAR FORMAT for maximum performance
            log_levels_batch = []
            services_batch = []
            environments_batch = []
            regions_batch = []
            messages_batch = []
            response_times = []
            status_codes = []
            request_ids = []
            timestamps = []

            for j in range(batch_size):
                # Generate realistic log entry
                level = random.choices(
                    log_levels,
                    weights=[30, 50, 15, 4, 1]  # Weighted distribution
                )[0]

                service = random.choice(services)
                env = random.choice(environments)
                region = random.choice(regions)

                # Generate message based on log level
                if level in ["ERROR", "FATAL"]:
                    message = random.choice(error_messages)
                    status_code = random.choice([500, 502, 503, 504, 400, 401, 403, 404])
                    response_time = random.uniform(500, 5000)  # Slower for errors
                else:
                    message = random.choice(messages)
                    status_code = random.choices(
                        [200, 201, 204, 304, 400, 404, 500],
                        weights=[70, 10, 5, 10, 2, 2, 1]
                    )[0]
                    response_time = random.uniform(10, 500)  # Faster for success

                # Generate unique request ID
                request_id = hashlib.md5(
                    f"{service}{timestamp+j}{random.random()}".encode()
                ).hexdigest()[:16]

                log_levels_batch.append(level)
                services_batch.append(service)
                environments_batch.append(env)
                regions_batch.append(region)
                messages_batch.append(message)
                response_times.append(response_time)
                status_codes.append(status_code)
                request_ids.append(request_id)
                timestamps.append(timestamp + j)

            payload_dict = {
                "m": "application_logs",  # measurement
                "columns": {
                    "time": timestamps,
                    "level": log_levels_batch,
                    "service": services_batch,
                    "environment": environments_batch,
                    "region": regions_batch,
                    "message": messages_batch,
                    "response_time_ms": response_times,
                    "status_code": status_codes,
                    "request_id": request_ids
                }
            }

            # Serialize with MessagePack (binary)
            payload_binary = msgpack.packb(payload_dict)

            # Pre-compress with gzip
            payload_compressed = gzip.compress(payload_binary)

            self.batches.append(payload_compressed)

            # Progress
            if (i + 1) % 100 == 0:
                elapsed = time.perf_counter() - start_time
                batches_per_sec = (i + 1) / elapsed if elapsed > 0 else 0
                print(f"  Progress: {i+1}/{num_batches} ({int(batches_per_sec)} batches/sec)")

        avg_size = sum(len(b) for b in self.batches) / len(self.batches)
        total_size = sum(len(b) for b in self.batches) / (1024 * 1024)
        elapsed = time.perf_counter() - start_time

        print(f"✅ Pre-generation complete!")
        print(f"   Time taken: {elapsed:.1f}s")
        print(f"   Total batches: {len(self.batches):,}")
        print(f"   Total log entries: {len(self.batches) * batch_size:,}")
        print(f"   Avg compressed size: {avg_size/1024:.1f} KB")
        print(f"   Total size: {total_size:.1f} MB")
        print()

    def get_batch(self) -> bytes:
        """Get a random pre-compressed log batch"""
        return random.choice(self.batches)


class SyntheticLogsLoadTester:
    """High-performance synthetic logs load tester"""

    def __init__(self, url: str, token: str, data_pool: SyntheticLogsDataPool, database: str = "default"):
        self.url = f"{url}/api/v1/write/msgpack"
        self.token = token
        self.data_pool = data_pool
        self.database = database

        # Stats
        self.total_sent = 0
        self.total_errors = 0
        self.latencies = []
        self.lock = asyncio.Lock()

    async def send_batch(self, session: aiohttp.ClientSession, batch_size: int) -> bool:
        """Send pre-compressed log batch"""
        start = time.perf_counter()

        try:
            # Get pre-compressed payload (near-zero overhead)
            payload = self.data_pool.get_batch()

            # Send
            headers = {
                "x-api-key": self.token,
                "Content-Type": "application/msgpack",
                "Content-Encoding": "gzip",
                "x-arc-database": self.database
            }

            async with session.post(self.url, data=payload, headers=headers) as response:
                latency = (time.perf_counter() - start) * 1000

                async with self.lock:
                    if response.status == 204:
                        self.total_sent += batch_size
                        self.latencies.append(latency)
                        return True
                    else:
                        self.total_errors += 1
                        # Log first few errors to see what's wrong
                        if self.total_errors <= 5:
                            error_body = await response.text()
                            print(f"\n❌ Error {self.total_errors}: Status {response.status}")
                            print(f"   Response: {error_body[:200]}")
                        return False

        except Exception as e:
            async with self.lock:
                self.total_errors += 1
                # Log first few exceptions to see what's wrong
                if self.total_errors <= 5:
                    print(f"\n❌ Exception {self.total_errors}: {type(e).__name__}: {e}")
            return False

    async def worker(self, session: aiohttp.ClientSession, target_rps: int, duration: int, batch_size: int):
        """Worker coroutine that sends batches"""
        batches_per_second = target_rps / batch_size
        delay = 1.0 / batches_per_second if batches_per_second > 0 else 0

        start_time = time.time()

        while time.time() - start_time < duration:
            await self.send_batch(session, batch_size)
            if delay > 0:
                await asyncio.sleep(delay)

    def get_percentile(self, percentile: float) -> float:
        """Get latency percentile"""
        if not self.latencies:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        index = int(len(sorted_latencies) * percentile)
        return sorted_latencies[min(index, len(sorted_latencies) - 1)]


async def run_load_test(
    url: str,
    token: str,
    target_rps: int,
    duration: int,
    pregenerate: int,
    batch_size: int,
    num_services: int,
    workers: int = None,
    database: str = "default"
):
    """Run synthetic logs load test"""

    # Pre-generate data
    data_pool = SyntheticLogsDataPool(pregenerate, batch_size, num_services)

    # Initialize tester
    tester = SyntheticLogsLoadTester(url, token, data_pool, database)

    print("=" * 80)
    print("Arc Synthetic Logs Load Test - MessagePack Columnar Format")
    print("=" * 80)
    print(f"Target RPS:      {target_rps:,} logs/sec")
    print(f"Batch Size:      {batch_size:,} logs/batch")
    print(f"Duration:        {duration}s")
    print(f"Database:        {database}")
    print(f"Services:        {num_services}")
    print(f"Pre-generated:   {pregenerate} batches in memory")
    print(f"Protocol:        MessagePack + Direct Arrow/Parquet")
    print(f"Arc URL:         {url}")
    print("=" * 80)
    print()

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

    connector = aiohttp.TCPConnector(
        limit=connector_limit,
        limit_per_host=connector_limit,
        ttl_dns_cache=300,
        force_close=False,
        enable_cleanup_closed=True,
        keepalive_timeout=30
    )
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)

    print(f"Connection pool: {connector_limit} connections for {num_workers} workers")
    print()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Warm up connection pool
        print("Warming up connection pool...")
        warmup_tasks = []
        for _ in range(min(10, num_workers)):
            async def warmup():
                try:
                    async with session.get(f"{url}/health") as resp:
                        await resp.text()
                except:
                    pass
            warmup_tasks.append(warmup())
        await asyncio.gather(*warmup_tasks, return_exceptions=True)
        print("✅ Connection pool warmed up\n")

        # Start workers
        start_time = time.time()
        worker_tasks = [
            asyncio.create_task(tester.worker(session, target_rps // num_workers, duration, batch_size))
            for _ in range(num_workers)
        ]

        # Monitor progress
        last_sent = 0
        while time.time() - start_time < duration:
            await asyncio.sleep(5)

            elapsed = time.time() - start_time
            current_rps = (tester.total_sent - last_sent) / 5

            p50 = tester.get_percentile(0.50)
            p95 = tester.get_percentile(0.95)
            p99 = tester.get_percentile(0.99)

            print(
                f"[{elapsed:6.1f}s] RPS: {int(current_rps):>7} (target: {target_rps}) | "
                f"Total: {tester.total_sent:>12,} | Errors: {tester.total_errors:>6} | "
                f"Latency (ms) - p50: {p50:>6.1f} p95: {p95:>6.1f} p99: {p99:>6.1f}",
                flush=True
            )

            last_sent = tester.total_sent

        # Wait for workers to finish
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Final stats
    print()
    print("=" * 80)
    print("Test Complete")
    print("=" * 80)

    elapsed = time.time() - start_time
    actual_rps = tester.total_sent / elapsed if elapsed > 0 else 0

    p50 = tester.get_percentile(0.50)
    p95 = tester.get_percentile(0.95)
    p99 = tester.get_percentile(0.99)
    p999 = tester.get_percentile(0.999)

    print(f"Duration:        {elapsed:.1f}s")
    print(f"Total Sent:      {tester.total_sent:,} log entries")
    print(f"Total Errors:    {tester.total_errors}")
    if tester.total_sent + tester.total_errors > 0:
        success_rate = (tester.total_sent / (tester.total_sent + tester.total_errors) * 100)
        print(f"Success Rate:    {success_rate:.2f}%")
    print(f"Actual RPS:      {int(actual_rps):,} logs/sec")
    print(f"Target RPS:      {target_rps:,} logs/sec")
    if target_rps > 0:
        print(f"Achievement:     {(actual_rps/target_rps*100):.1f}%")
    print()
    print(f"Latency Percentiles (ms):")
    print(f"  p50:  {p50:.2f}")
    print(f"  p95:  {p95:.2f}")
    print(f"  p99:  {p99:.2f}")
    print(f"  p999: {p999:.2f}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Synthetic Logs Load Test for Arc"
    )
    parser.add_argument("--url", default="http://localhost:8000", help="Arc API URL")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--database", default="default", help="Target database (default: default)")
    parser.add_argument("--rps", type=int, default=50000, help="Target RPS (logs/sec)")
    parser.add_argument("--duration", type=int, default=30, help="Test duration (seconds)")
    parser.add_argument("--pregenerate", type=int, default=500, help="Number of batches to pre-generate")
    parser.add_argument("--batch-size", type=int, default=1000, help="Logs per batch")
    parser.add_argument("--services", type=int, default=50, help="Number of unique services")
    parser.add_argument("--workers", type=int, default=None, help="Number of concurrent workers (default: auto)")

    args = parser.parse_args()

    asyncio.run(run_load_test(
        url=args.url,
        token=args.token,
        target_rps=args.rps,
        duration=args.duration,
        pregenerate=args.pregenerate,
        batch_size=args.batch_size,
        num_services=args.services,
        workers=args.workers,
        database=args.database
    ))
