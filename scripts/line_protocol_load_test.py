#!/usr/bin/env python3

import aiohttp
import asyncio
import argparse
import gzip
import random
import time
from datetime import datetime
from typing import List


class PreGeneratedLineProtocolDataPool:
    """Pool of pre-generated and pre-compressed Line Protocol batches"""

    def __init__(self, num_batches: int, batch_size: int, num_hosts: int = 1000):
        self.batches = []

        print(f"🔄 Pre-generating {num_batches} Line Protocol batches ({num_batches * batch_size:,} records)")

        measurements = ["cpu", "mem", "disk", "net"]
        hosts = [f"server{i:03d}" for i in range(1, num_hosts + 1)]
        regions = ["us-east", "us-west", "eu-central"]
        envs = ["prod", "staging", "dev"]

        for i in range(num_batches):
            # Generate timestamp in nanoseconds (Line Protocol standard)
            timestamp_ns = int(time.time() * 1_000_000_000)

            lines = []
            for j in range(batch_size):
                measurement = random.choice(measurements)
                host = random.choice(hosts)
                region = random.choice(regions)
                env = random.choice(envs)

                # Line Protocol format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
                if measurement == "cpu":
                    line = (
                        f"cpu,host={host},region={region},env={env} "
                        f"usage_idle={random.uniform(0, 100):.2f},"
                        f"usage_user={random.uniform(0, 50):.2f},"
                        f"usage_system={random.uniform(0, 30):.2f} "
                        f"{timestamp_ns + j * 1000}"  # Increment timestamp by 1us per record
                    )
                elif measurement == "mem":
                    line = (
                        f"mem,host={host},region={region},env={env} "
                        f"used_percent={random.uniform(20, 90):.2f},"
                        f"available_percent={random.uniform(10, 80):.2f},"
                        f"used_bytes={random.randint(1000000000, 8000000000)}i "
                        f"{timestamp_ns + j * 1000}"
                    )
                elif measurement == "disk":
                    device = random.choice(["sda1", "sda2", "nvme0n1"])
                    line = (
                        f"disk,host={host},region={region},device={device} "
                        f"used_percent={random.uniform(30, 95):.2f},"
                        f"free_bytes={random.randint(10000000000, 500000000000)}i,"
                        f"inodes_used={random.randint(100000, 1000000)}i "
                        f"{timestamp_ns + j * 1000}"
                    )
                else:  # net
                    interface = random.choice(["eth0", "eth1", "lo"])
                    line = (
                        f"net,host={host},interface={interface} "
                        f"bytes_sent={random.randint(1000000, 10000000)}i,"
                        f"bytes_recv={random.randint(1000000, 10000000)}i,"
                        f"packets_sent={random.randint(1000, 100000)}i,"
                        f"packets_recv={random.randint(1000, 100000)}i "
                        f"{timestamp_ns + j * 1000}"
                    )

                lines.append(line)

            # Join all lines with newlines
            payload_text = "\n".join(lines)

            # Pre-compress with gzip (Telegraf sends gzipped by default)
            payload_compressed = gzip.compress(payload_text.encode('utf-8'))

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
        """Get a random pre-compressed Line Protocol batch"""
        return random.choice(self.batches)


class LineProtocolLoadTester:
    """High-performance Line Protocol load tester"""

    def __init__(self, url: str, token: str, data_pool: PreGeneratedLineProtocolDataPool, database: str = "default"):
        self.url = f"{url}/api/v1/write"
        self.token = token
        self.data_pool = data_pool
        self.database = database

        # Stats
        self.total_sent = 0
        self.total_errors = 0
        self.latencies = []
        self.lock = asyncio.Lock()

    async def send_batch(self, session: aiohttp.ClientSession, batch_size: int) -> bool:
        """Send pre-compressed Line Protocol batch"""
        start = time.perf_counter()

        try:
            # Get pre-compressed payload (near-zero overhead)
            payload = self.data_pool.get_batch()

            # Send (InfluxDB 1.x compatible endpoint)
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "text/plain",
                "Content-Encoding": "gzip",
                "x-arc-database": self.database
            }

            # Add db query parameter (InfluxDB 1.x style)
            url = f"{self.url}?db={self.database}"

            async with session.post(url, data=payload, headers=headers) as response:
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
        delay = 1.0 / batches_per_second

        start_time = time.time()

        while time.time() - start_time < duration:
            await self.send_batch(session, batch_size)
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
    database: str = "telegraf"
):
    """Run pre-generated Line Protocol load test"""

    # Pre-generate data
    global start_time
    start_time = time.perf_counter()
    data_pool = PreGeneratedLineProtocolDataPool(pregenerate, batch_size, num_hosts)

    # Initialize tester
    tester = LineProtocolLoadTester(url, token, data_pool, database)

    print("=" * 80)
    print("Arc Line Protocol - InfluxDB/Telegraf Compatible Load Test")
    print("=" * 80)
    print(f"Format:          Line Protocol (InfluxDB 1.x/2.x compatible)")
    print(f"Target RPS:      {target_rps:,}")
    print(f"Batch Size:      {batch_size:,}")
    print(f"Duration:        {duration}s")
    print(f"Database:        {database}")
    print(f"Pre-generated:   Yes (batches in memory)")
    print(f"Protocol:        Line Protocol → ArrowParquetBuffer")
    print(f"Optimizations:   Integer timestamps, Schema caching, LZ4")
    print(f"Arc URL:         {url}")
    print("=" * 80)
    print()

    # Calculate workers
    if workers is None:
        # Line Protocol needs fewer workers than MsgPack (text parsing is slower)
        num_workers = min(int(target_rps / 5000), 50)
        num_workers = max(num_workers, 10)  # At least 10 workers
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
                f"[{elapsed:6.1f}s] RPS: {int(current_rps):>6} (target: {target_rps}) | "
                f"Total: {tester.total_sent:>12,} | Errors: {tester.total_errors:>6} | "
                f"Latency (ms) - p50: {p50:>6.1f} p95: {p95:>6.1f} p99: {p99:>6.1f}"
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
    print()
    print("💡 Note: Line Protocol is text-based and will be slower than binary MsgPack.")
    print("   Expected: 60-80% of MsgPack throughput (~1.5-2M RPS ceiling)")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pre-Generated Line Protocol Load Test for Arc (InfluxDB/Telegraf Compatible)"
    )
    parser.add_argument("--url", default="http://localhost:8000", help="Arc API URL")
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--database", default="telegraf", help="Target database (default: telegraf)")
    parser.add_argument("--rps", type=int, default=100000, help="Target RPS (default: 100k)")
    parser.add_argument("--duration", type=int, default=30, help="Test duration (seconds)")
    parser.add_argument("--pregenerate", type=int, default=1000, help="Number of batches to pre-generate")
    parser.add_argument("--batch-size", type=int, default=1000, help="Records per batch")
    parser.add_argument("--hosts", type=int, default=1000, help="Number of unique hosts")
    parser.add_argument("--workers", type=int, default=None, help="Number of concurrent workers (default: auto)")

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
        database=args.database
    ))
