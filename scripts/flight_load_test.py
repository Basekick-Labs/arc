#!/usr/bin/env python3
"""
Arrow Flight Load Test

High-performance load testing for Arc's Arrow Flight RPC server.
Based on metrics_load_test.py but uses Arrow Flight instead of HTTP+MessagePack.
"""

import argparse
import asyncio
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import random
import time
from datetime import datetime
from typing import List


class PreGeneratedArrowDataPool:
    """Pool of pre-generated Arrow RecordBatches"""

    def __init__(self, num_batches: int, batch_size: int, num_hosts: int = 1000):
        self.batches = []
        self.batch_size = batch_size

        print(f"🔄 Pre-generating {num_batches} Arrow RecordBatches ({num_batches * batch_size:,} records)")
        start = time.perf_counter()

        measurements = ["cpu", "mem", "disk"]
        hosts = [f"server{i:03d}" for i in range(1, num_hosts + 1)]

        # Define Arrow schema
        schema = pa.schema([
            ('time', pa.timestamp('us')),
            ('host', pa.string()),
            ('measurement', pa.string()),
            ('usage_idle', pa.float64()),
            ('usage_user', pa.float64()),
            ('usage_system', pa.float64()),
        ])

        for i in range(num_batches):
            timestamp_us = int(datetime.now().timestamp() * 1_000_000)
            times = [timestamp_us + j * 1000 for j in range(batch_size)]

            # Generate data
            batch_hosts = [random.choice(hosts) for _ in range(batch_size)]
            batch_measurements = [random.choice(measurements) for _ in range(batch_size)]
            usage_idle = np.random.uniform(0, 100, batch_size).tolist()
            usage_user = np.random.uniform(0, 100, batch_size).tolist()
            usage_system = np.random.uniform(0, 100, batch_size).tolist()

            # Create RecordBatch
            batch = pa.RecordBatch.from_arrays([
                pa.array(times, type=pa.timestamp('us')),
                pa.array(batch_hosts),
                pa.array(batch_measurements),
                pa.array(usage_idle),
                pa.array(usage_user),
                pa.array(usage_system),
            ], schema=schema)

            self.batches.append(batch)

            if (i + 1) % 100 == 0:
                batches_per_sec = (i + 1) / (time.perf_counter() - start) if i > 0 else 0
                print(f"  Progress: {i+1}/{num_batches} ({int(batches_per_sec)} batches/sec)")

        elapsed = time.perf_counter() - start
        avg_size = sum(b.nbytes for b in self.batches) / len(self.batches) / 1024
        total_size = sum(b.nbytes for b in self.batches) / (1024 * 1024)

        print(f"✅ Pre-generation complete!")
        print(f"   Time taken: {elapsed:.1f}s")
        print(f"   Total batches: {len(self.batches):,}")
        print(f"   Total records: {len(self.batches) * batch_size:,}")
        print(f"   Avg batch size: {avg_size:.1f} KB")
        print(f"   Total size: {total_size:.1f} MB")
        print()

    def get_batch(self) -> pa.RecordBatch:
        """Get a random pre-generated Arrow RecordBatch"""
        return random.choice(self.batches)


class FlightLoadTester:
    """High-performance Arrow Flight load tester"""

    def __init__(self, host: str, ports: List[int], data_pool: PreGeneratedArrowDataPool, database: str = "default"):
        self.host = host
        self.ports = ports  # List of all Flight server ports
        self.data_pool = data_pool
        self.database = database
        self._port_index = 0  # Round-robin index

        # Stats
        self.total_sent = 0
        self.total_errors = 0
        self.latencies = []
        self.lock = asyncio.Lock()

    def get_next_port(self) -> int:
        """Get next port in round-robin fashion"""
        port = self.ports[self._port_index % len(self.ports)]
        self._port_index += 1
        return port

    async def send_batch(self) -> bool:
        """Send Arrow RecordBatch via Flight"""
        start = time.perf_counter()

        try:
            # Get pre-generated batch
            batch = self.data_pool.get_batch()

            # Get next port in round-robin
            port = self.get_next_port()

            # Run Flight call in thread pool (Flight client is synchronous)
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                None,
                self._send_batch_sync,
                batch,
                port
            )

            latency = (time.perf_counter() - start) * 1000

            async with self.lock:
                if success:
                    self.total_sent += self.data_pool.batch_size
                    self.latencies.append(latency)
                    return True
                else:
                    self.total_errors += 1
                    return False

        except Exception as e:
            async with self.lock:
                self.total_errors += 1
                if self.total_errors <= 5:
                    print(f"\n❌ Exception {self.total_errors}: {type(e).__name__}: {e}")
            return False

    def _send_batch_sync(self, batch: pa.RecordBatch, port: int) -> bool:
        """Synchronous Flight write (runs in thread pool)"""
        try:
            # Create new client for this thread, connecting to specific port
            location = f"grpc://{self.host}:{port}"
            client = flight.FlightClient(location)

            # Create descriptor with database and measurement
            # Use first measurement in batch (they're all the same type in this test)
            measurement = batch.column(2)[0].as_py()  # measurement column
            descriptor = flight.FlightDescriptor.for_path(self.database, measurement)

            # Send batch
            writer, _ = client.do_put(descriptor, batch.schema)
            writer.write_batch(batch)
            writer.close()

            return True
        except Exception:
            return False

    async def worker(self, target_rps: int, duration: int):
        """Worker coroutine that sends batches"""
        batch_size = self.data_pool.batch_size
        batches_per_second = target_rps / batch_size
        delay = 1.0 / batches_per_second

        start_time = time.time()

        while time.time() - start_time < duration:
            await self.send_batch()
            await asyncio.sleep(delay)

    def get_percentile(self, percentile: float) -> float:
        """Get latency percentile"""
        if not self.latencies:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        index = int(len(sorted_latencies) * percentile)
        return sorted_latencies[min(index, len(sorted_latencies) - 1)]


async def run_load_test(
    host: str,
    base_port: int,
    target_rps: int,
    duration: int,
    pregenerate: int,
    batch_size: int,
    num_hosts: int,
    num_flight_servers: int = 42,
    workers: int = None,
    database: str = "default",
    throttle: bool = False
):
    """Run Arrow Flight load test"""

    # Pre-generate data
    global start_time
    start_time = time.perf_counter()
    data_pool = PreGeneratedArrowDataPool(pregenerate, batch_size, num_hosts)

    # Discover available Flight server ports
    # Arc starts each worker with Flight on port base_port + (PID % 1000)
    # For simplicity, try sequential ports from base_port
    print(f"🔍 Discovering Flight servers starting at port {base_port}...")
    available_ports = []
    for offset in range(num_flight_servers):
        port = base_port + offset
        try:
            # Try to connect to see if server is running
            flight.FlightClient(f"grpc://{host}:{port}")
            # If we can create client, port is available
            available_ports.append(port)
            if len(available_ports) <= 5:  # Only print first few
                print(f"  ✓ Found Flight server on port {port}")
        except:
            # Port not available, skip
            pass

    if not available_ports:
        print(f"❌ No Flight servers found! Is Arc running with Flight enabled?")
        return

    print(f"✅ Found {len(available_ports)} Flight servers (ports: {min(available_ports)}-{max(available_ports)})")
    print()

    # Initialize tester with all available ports
    tester = FlightLoadTester(host, available_ports, data_pool, database)

    print("=" * 80)
    print("Arc Arrow Flight - Maximum Throughput Test")
    print("=" * 80)
    print(f"Protocol:        Arrow Flight RPC (gRPC/HTTP2)")
    print(f"Target RPS:      {target_rps:,}")
    print(f"Batch Size:      {batch_size:,}")
    print(f"Duration:        {duration}s")
    print(f"Database:        {database}")
    print(f"Pre-generated:   Yes (batches in memory)")
    print(f"Flight Servers:  {len(available_ports)} servers (ports {min(available_ports)}-{max(available_ports)})")
    print(f"Load Balancing:  Round-robin across all servers")
    print("=" * 80)
    print()

    # Calculate workers
    if workers is None:
        num_workers = min(int(target_rps / 10000), 30)
    else:
        num_workers = workers
    print(f"Starting {num_workers} workers...")
    print()

    # Start workers
    if throttle:
        # THROTTLED: Divide target RPS among workers for controlled sustained load
        worker_rps = target_rps // num_workers
        mode_desc = f"THROTTLED (each worker: {worker_rps:,} RPS)"
    else:
        # WIDE-OPEN: Each worker tries full target RPS (max throughput discovery)
        worker_rps = target_rps
        mode_desc = f"WIDE-OPEN (each worker: {worker_rps:,} RPS - max throughput test)"

    print(f"Mode: {mode_desc}\n")

    start_time = time.time()
    worker_tasks = [
        asyncio.create_task(tester.worker(worker_rps, duration))
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
    print(f"Success Rate:    {(tester.total_sent/(tester.total_sent+tester.total_errors)*100) if (tester.total_sent+tester.total_errors) > 0 else 0:.2f}%")
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
        description="Arrow Flight Load Test for Arc"
    )
    parser.add_argument("--host", default="localhost", help="Flight server host")
    parser.add_argument("--port", type=int, default=8815, help="Base Flight server port")
    parser.add_argument("--num-servers", type=int, default=100, help="Number of Flight servers to discover")
    parser.add_argument("--database", default="default", help="Target database")
    parser.add_argument("--rps", type=int, default=500000, help="Target RPS")
    parser.add_argument("--duration", type=int, default=30, help="Test duration (seconds)")
    parser.add_argument("--pregenerate", type=int, default=1000, help="Number of batches to pre-generate")
    parser.add_argument("--batch-size", type=int, default=1000, help="Records per batch")
    parser.add_argument("--hosts", type=int, default=1000, help="Number of unique hosts")
    parser.add_argument("--workers", type=int, default=None, help="Number of concurrent workers (default: auto)")
    parser.add_argument("--throttle", action="store_true", help="Divide target RPS among workers (controlled test)")

    args = parser.parse_args()

    asyncio.run(run_load_test(
        host=args.host,
        base_port=args.port,
        target_rps=args.rps,
        duration=args.duration,
        pregenerate=args.pregenerate,
        batch_size=args.batch_size,
        num_hosts=args.hosts,
        num_flight_servers=args.num_servers,
        workers=args.workers,
        database=args.database,
        throttle=args.throttle
    ))
