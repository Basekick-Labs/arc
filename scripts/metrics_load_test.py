#!/usr/bin/env python3

import msgpack
import aiohttp
import asyncio
import argparse
import gzip
import random
import time
from datetime import datetime
from typing import List
import numpy as np


class PreGeneratedMessagePackDataPool:
    """Pool of pre-generated and pre-compressed MessagePack batches"""

    def __init__(self, num_batches: int, batch_size: int, num_hosts: int = 1000, columnar: bool = False):
        self.batches = []
        self.columnar = columnar

        format_type = "COLUMNAR (zero-copy)" if columnar else "ROW (legacy)"
        print(f"🔄 Pre-generating {num_batches} MessagePack batches ({num_batches * batch_size:,} records) - {format_type}")

        measurements = ["cpu", "mem", "disk"]
        hosts = [f"server{i:03d}" for i in range(1, num_hosts + 1)]
        regions = ["us-east", "us-west", "eu-central"]
        envs = ["prod", "staging", "dev"]

        for i in range(num_batches):
            timestamp = int(datetime.now().timestamp() * 1000)

            if columnar:
                # COLUMNAR FORMAT (FAST PATH - zero conversion)
                # Group by measurement for better batching
                measurement = random.choice(measurements)

                payload_dict = {
                    "m": measurement,
                    "columns": {
                        "time": [timestamp + j for j in range(batch_size)],
                        "host": [random.choice(hosts) for _ in range(batch_size)],
                        "region": [random.choice(regions) for _ in range(batch_size)],
                        "env": [random.choice(envs) for _ in range(batch_size)],
                        "usage_idle": [random.uniform(0, 100) for _ in range(batch_size)],
                        "usage_user": [random.uniform(0, 100) for _ in range(batch_size)],
                        "usage_system": [random.uniform(0, 100) for _ in range(batch_size)]
                    }
                }
            else:
                # ROW FORMAT (LEGACY - with conversion overhead)
                batch = []

                for _ in range(batch_size):
                    measurement = random.choice(measurements)
                    host = random.choice(hosts)

                    record = {
                        "m": measurement,
                        "t": timestamp + random.randint(0, 1000),
                        "h": host,
                        "fields": {
                            "usage_idle": random.uniform(0, 100),
                            "usage_user": random.uniform(0, 100),
                            "usage_system": random.uniform(0, 100)
                        }
                    }

                    # Add tags occasionally
                    if random.random() > 0.7:
                        record["tags"] = {
                            "region": random.choice(regions),
                            "env": random.choice(envs)
                        }

                    batch.append(record)

                # Create MessagePack batch format
                payload_dict = {"batch": batch}

            # Serialize with MessagePack (binary)
            payload_binary = msgpack.packb(payload_dict)

            # Pre-compress with gzip
            payload_compressed = gzip.compress(payload_binary)

            self.batches.append(payload_compressed)

            # Progress
            if (i + 1) % 100 == 0:
                batches_per_sec = (i + 1) / (time.perf_counter() - start_time) if i > 0 else 0
                print(f"  Progress: {i+1}/{num_batches} ({int(batches_per_sec)} batches/sec)")

        avg_size = sum(len(b) for b in self.batches) / len(self.batches)
        total_size = sum(len(b) for b in self.batches) / (1024 * 1024)

        print(f"✅ Pre-generation complete!")
        print(f"   Time taken: {time.perf_counter() - start_time:.1f}s")
        print(f"   Total batches: {len(self.batches):,}")
        print(f"   Total records: {len(self.batches) * batch_size:,}")
        print(f"   Avg compressed size: {avg_size/1024:.1f} KB")
        print(f"   Total size: {total_size:.1f} MB")
        print()

    def get_batch(self) -> bytes:
        """Get a random pre-compressed MessagePack batch"""
        return random.choice(self.batches)


class MessagePackLoadTester:
    """High-performance MessagePack load tester"""

    def __init__(self, url: str, token: str, data_pool: PreGeneratedMessagePackDataPool, database: str = "default"):
        self.url = f"{url}/api/v1/write/msgpack"
        self.token = token
        self.data_pool = data_pool
        self.database = database

        # Stats
        self.total_sent = 0
        self.total_errors = 0
        self.latencies = []
        self.lock = asyncio.Lock()

    async def send_batch(self, session: aiohttp.ClientSession) -> bool:
        """Send pre-compressed MessagePack batch"""
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
                        self.total_sent += 1000  # batch_size
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

    async def worker(self, session: aiohttp.ClientSession, target_rps: int, duration: int):
        """Worker coroutine that sends batches"""
        batch_size = 1000
        batches_per_second = target_rps / batch_size
        delay = 1.0 / batches_per_second

        start_time = time.time()

        while time.time() - start_time < duration:
            await self.send_batch(session)
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
    num_hosts: int,
    workers: int = None,
    database: str = "default",
    columnar: bool = False
):
    """Run pre-generated MessagePack load test"""

    # Pre-generate data
    global start_time
    start_time = time.perf_counter()
    data_pool = PreGeneratedMessagePackDataPool(pregenerate, batch_size, num_hosts, columnar=columnar)

    # Initialize tester
    tester = MessagePackLoadTester(url, token, data_pool, database)

    format_type = "COLUMNAR (zero-copy passthrough)" if columnar else "ROW (legacy with conversion)"

    print("=" * 80)
    print("Arc MessagePack Binary Protocol - Maximum Throughput Test")
    print("=" * 80)
    print(f"Format:          {format_type}")
    print(f"Target RPS:      {target_rps:,}")
    print(f"Batch Size:      {batch_size:,}")
    print(f"Duration:        {duration}s")
    print(f"Database:        {database}")
    print(f"Pre-generated:   Yes (batches in memory)")
    print(f"Protocol:        MessagePack + Direct Arrow/Parquet")
    print(f"Arc URL:         {url}")
    print("=" * 80)
    print()

    # Calculate workers
    if workers is None:
        num_workers = min(int(target_rps / 10000), 30)
    else:
        num_workers = workers
    print(f"Starting {num_workers} workers...")
    print()

    # Create session and workers
    # Set connector limits intelligently based on worker count
    # Use fewer connections but aggressive reuse for high worker counts
    if num_workers <= 100:
        connector_limit = num_workers + 50
    elif num_workers <= 300:
        connector_limit = 200  # Share connections among workers
    else:
        connector_limit = 256  # Cap at 256 for very high worker counts

    connector = aiohttp.TCPConnector(
        limit=connector_limit,
        limit_per_host=connector_limit,
        ttl_dns_cache=300,
        force_close=False,  # Reuse connections aggressively
        enable_cleanup_closed=True,
        keepalive_timeout=30  # Keep connections alive for reuse
    )
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)

    print(f"Connection pool: {connector_limit} connections for {num_workers} workers")
    print()
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Warm up connection pool by making a few health check requests
        print("Warming up connection pool...")
        warmup_tasks = []
        for _ in range(min(10, num_workers)):
            async def warmup():
                try:
                    async with session.get(f"{tester.url.replace('/api/v1/write/msgpack', '/health')}") as resp:
                        await resp.text()
                except:
                    pass
            warmup_tasks.append(warmup())
        await asyncio.gather(*warmup_tasks, return_exceptions=True)
        print("✅ Connection pool warmed up\n")

        # Start workers
        start_time = time.time()
        workers = [
            asyncio.create_task(tester.worker(session, target_rps // num_workers, duration))
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
                f"[{elapsed:6.1f}s] RPS: {int(current_rps):>6} (target: {target_rps}) | "
                f"Total: {tester.total_sent:>12,} | Errors: {tester.total_errors:>6} | "
                f"Latency (ms) - p50: {p50:>6.1f} p95: {p95:>6.1f} p99: {p99:>6.1f}"
            )

            last_sent = tester.total_sent

        # Wait for workers to finish
        await asyncio.gather(*workers, return_exceptions=True)

    # Final stats
    print()
    print("=" * 80)
    print("Test Complete")
    print("=" * 80)

    elapsed = time.time() - start_time
    actual_rps = tester.total_sent / elapsed

    p50 = tester.get_percentile(0.50)
    p95 = tester.get_percentile(0.95)
    p99 = tester.get_percentile(0.99)
    p999 = tester.get_percentile(0.999)

    print(f"Duration:        {elapsed:.1f}s")
    print(f"Total Sent:      {tester.total_sent:,} records")
    print(f"Total Errors:    {tester.total_errors}")
    print(f"Success Rate:    {(tester.total_sent/(tester.total_sent+tester.total_errors)*100):.2f}%")
    print(f"Actual RPS:      {int(actual_rps):,} records/sec")
    print(f"Target RPS:      {target_rps:,} records/sec")
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
        description="Pre-Generated MessagePack Load Test for Arc Binary Protocol"
    )
    parser.add_argument("--url", default="http://localhost:8000", help="Arc API URL")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--database", default="default", help="Target database (default: default)")
    parser.add_argument("--rps", type=int, default=500000, help="Target RPS")
    parser.add_argument("--duration", type=int, default=30, help="Test duration (seconds)")
    parser.add_argument("--pregenerate", type=int, default=1000, help="Number of batches to pre-generate")
    parser.add_argument("--batch-size", type=int, default=1000, help="Records per batch")
    parser.add_argument("--hosts", type=int, default=1000, help="Number of unique hosts")
    parser.add_argument("--workers", type=int, default=None, help="Number of concurrent workers (default: auto)")
    parser.add_argument("--columnar", action="store_true", help="Use columnar format (25-35%% faster, RECOMMENDED)")

    args = parser.parse_args()

    asyncio.run(run_load_test(
        url=args.url,
        token=args.token,
        target_rps=args.rps,
        duration=args.duration,
        pregenerate=args.pregenerate,
        batch_size=args.batch_size,
        num_hosts=args.hosts,
        workers=args.workers,
        database=args.database,
        columnar=args.columnar
    ))
