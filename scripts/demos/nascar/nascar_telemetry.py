#!/usr/bin/env python3
"""
Arc Demo Scenario: NASCAR Race Telemetry Load Test

This script generates realistic NASCAR race telemetry data to demonstrate Arc's capability
to handle high-volume real-time time-series data ingestion and querying.

NASCAR generates approximately 612,000 messages per second across all cars during a race:
- 40 cars on track
- Over 60 different data points per car (engine RPM, brake pressure, speed, tire temp, etc.)
- Sampled hundreds of times per second
- ~15,300 messages per second per car (612,000 ÷ 40)
- Message size: ~0.15KB per message
- Total throughput: ~92 MB/s sustained for up to 4 hours

This demo simulates a full NASCAR race with all 40 cars and can test Arc up to 2.4M msg/sec.

Use cases demonstrated:
- High-frequency time-series data ingestion (612k msg/sec baseline, up to 2.4M msg/sec)
- Real-time race monitoring across all teams
- Performance analytics and car comparison
- Race strategy optimization with live telemetry
- Sustained high-volume writes for 4+ hour race duration
"""

import msgpack
import aiohttp
import asyncio
import argparse
import gzip
import random
import time
from datetime import datetime
import math
import numpy as np
from typing import List


class NASCARTelemetryDataPool:
    """Pool of pre-generated and pre-compressed NASCAR telemetry batches"""

    def __init__(self, num_batches: int, batch_size: int, num_cars: int = 40):
        """
        Args:
            num_batches: Number of batches to pre-generate
            batch_size: Messages per batch (will be split across 3 telemetry types)
            num_cars: Number of cars in the race (default: 40)
        """
        self.batches = {
            "engine_telemetry": [],
            "chassis_telemetry": [],
            "tire_telemetry": []
        }
        self.num_cars = num_cars
        self.batch_size = batch_size

        # Race simulation state
        self.race_start_time = int(time.time() * 1000)
        self.laps_completed = [0] * num_cars

        # Track configuration (simulating a 1.5 mile oval like Charlotte Motor Speedway)
        self.track_length_miles = 1.5
        self.track_length_meters = self.track_length_miles * 1609.34

        # Car state (position on track in meters)
        self.car_positions = [i * (self.track_length_meters / num_cars) for i in range(num_cars)]

        # Multiple teams and manufacturers for full race simulation
        self.teams = [
            "Hendrick Motorsports", "Joe Gibbs Racing", "Team Penske", "Stewart-Haas Racing",
            "Richard Childress Racing", "Trackhouse Racing", "Kaulig Racing", "JTG Daugherty Racing",
            "Petty GMS Motorsports", "Front Row Motorsports", "Live Fast Motorsports", "Spire Motorsports"
        ]
        self.manufacturers = ["Chevrolet", "Ford", "Toyota"]

        # Generate 40 drivers with realistic car numbers and distribution
        self.drivers = []
        for i in range(num_cars):
            self.drivers.append({
                "car_number": i + 1,
                "driver": f"Driver {i + 1}",
                "team": self.teams[i % len(self.teams)],
                "manufacturer": self.manufacturers[i % len(self.manufacturers)]
            })

        print(f"🔄 Pre-generating {num_batches} NASCAR telemetry batches ({num_batches * batch_size:,} messages) - COLUMNAR")
        print(f"   🏎️  Cars: {num_cars}")
        print(f"   📦 Batch size: {batch_size:,} messages")
        print()

        start_time = time.perf_counter()

        # Generate batches for each telemetry type
        for i in range(num_batches):
            # Each batch gets 1/3 of messages for each type
            engine_batch_size = batch_size // 3
            chassis_batch_size = batch_size // 3
            tire_batch_size = batch_size - engine_batch_size - chassis_batch_size

            current_time_ms = self.race_start_time + (i * 1000)  # 1 second per batch

            # Generate engine telemetry batch
            if engine_batch_size > 0:
                engine_payload = self._generate_engine_batch(current_time_ms, engine_batch_size)
                engine_compressed = gzip.compress(msgpack.packb(engine_payload))
                self.batches["engine_telemetry"].append(engine_compressed)

            # Generate chassis telemetry batch
            if chassis_batch_size > 0:
                chassis_payload = self._generate_chassis_batch(current_time_ms, chassis_batch_size)
                chassis_compressed = gzip.compress(msgpack.packb(chassis_payload))
                self.batches["chassis_telemetry"].append(chassis_compressed)

            # Generate tire telemetry batch
            if tire_batch_size > 0:
                tire_payload = self._generate_tire_batch(current_time_ms, tire_batch_size)
                tire_compressed = gzip.compress(msgpack.packb(tire_payload))
                self.batches["tire_telemetry"].append(tire_compressed)

            # Update car positions for next batch
            self._update_car_positions(1.0)

            # Progress indicator
            if (i + 1) % 100 == 0:
                elapsed = time.perf_counter() - start_time
                rate = (i + 1) / elapsed
                print(f"  Progress: {i+1}/{num_batches} ({int(rate)} batches/sec)")

        elapsed = time.perf_counter() - start_time

        # Calculate stats
        for telem_type, batches in self.batches.items():
            if batches:
                avg_size = sum(len(b) for b in batches) / len(batches)
                total_size = sum(len(b) for b in batches)
                print(f"  ✅ {telem_type}: {len(batches)} batches, {total_size/1024/1024:.1f} MB (avg: {avg_size/1024:.1f} KB)")

        total_messages = num_batches * batch_size
        print()
        print(f"✅ Pre-generation complete!")
        print(f"   Time taken: {elapsed:.1f}s")
        print(f"   Total messages: {total_messages:,}")
        print(f"   Generation rate: {int(total_messages / elapsed):,} messages/sec")
        print()

    def _update_car_positions(self, time_delta_seconds: float):
        """Update car positions based on elapsed time"""
        for i in range(self.num_cars):
            # Base speed varies by car performance
            base_speed_mph = random.uniform(180, 195)
            speed_mps = base_speed_mph * 0.44704

            # Update position
            self.car_positions[i] += speed_mps * time_delta_seconds

            # Handle lap completion
            if self.car_positions[i] >= self.track_length_meters:
                self.car_positions[i] -= self.track_length_meters
                self.laps_completed[i] += 1

    def _generate_engine_batch(self, current_time_ms: int, batch_size: int) -> dict:
        """Generate engine telemetry batch using NumPy for speed"""
        car_indices = np.random.randint(0, self.num_cars, size=batch_size)

        # Vectorized position calculation
        positions_pct = np.array([self.car_positions[i] / self.track_length_meters for i in car_indices])
        in_turn = np.sin(positions_pct * 4 * np.pi) < 0

        # Vectorized RPM/speed generation
        rpm = np.where(in_turn,
                      np.random.uniform(7000, 8500, size=batch_size),
                      np.random.uniform(8500, 9500, size=batch_size)).astype(int)
        throttle = np.where(in_turn,
                           np.random.uniform(70, 90, size=batch_size),
                           np.random.uniform(90, 100, size=batch_size))
        speed_mph = np.where(in_turn,
                            np.random.uniform(160, 180, size=batch_size),
                            np.random.uniform(185, 200, size=batch_size))

        # Build payload directly from arrays
        return {
            "m": "engine_telemetry",
            "columns": {
                "time": [current_time_ms] * batch_size,
                "car_number": [self.drivers[i]["car_number"] for i in car_indices],
                "driver": [self.drivers[i]["driver"] for i in car_indices],
                "team": [self.drivers[i]["team"] for i in car_indices],
                "manufacturer": [self.drivers[i]["manufacturer"] for i in car_indices],
                "rpm": rpm.tolist(),
                "throttle_position": np.round(throttle, 1).tolist(),
                "speed_mph": np.round(speed_mph, 1).tolist(),
                "engine_temp": np.random.randint(190, 220, size=batch_size).tolist(),
                "oil_pressure": np.random.randint(40, 65, size=batch_size).tolist(),
                "oil_temp": np.random.randint(240, 280, size=batch_size).tolist(),
                "fuel_level": np.round(np.random.uniform(2, 18, size=batch_size), 2).tolist(),
                "gear": np.where(in_turn, np.random.randint(3, 5, size=batch_size), 4).tolist(),
                "lap": [self.laps_completed[i] for i in car_indices]
            }
        }

    def _generate_chassis_batch(self, current_time_ms: int, batch_size: int) -> dict:
        """Generate chassis telemetry batch using NumPy"""
        car_indices = np.random.randint(0, self.num_cars, size=batch_size)
        positions_pct = np.array([self.car_positions[i] / self.track_length_meters for i in car_indices])
        in_turn = np.sin(positions_pct * 4 * np.pi) < 0

        lateral_g = np.where(in_turn, np.random.uniform(2.0, 3.5, size=batch_size),
                            np.random.uniform(-0.2, 0.2, size=batch_size))
        brake_pressure = np.where(in_turn, np.random.randint(500, 1500, size=batch_size), 0)

        return {
            "m": "chassis_telemetry",
            "columns": {
                "time": [current_time_ms] * batch_size,
                "car_number": [self.drivers[i]["car_number"] for i in car_indices],
                "driver": [self.drivers[i]["driver"] for i in car_indices],
                "lateral_g": np.round(lateral_g, 2).tolist(),
                "longitudinal_g": np.round(np.where(in_turn, np.random.uniform(-0.5, 0.2, size=batch_size),
                                                     np.random.uniform(0.5, 1.2, size=batch_size)), 2).tolist(),
                "vertical_g": np.round(np.random.uniform(0.8, 1.2, size=batch_size), 2).tolist(),
                "brake_pressure_psi": brake_pressure.tolist(),
                "steering_angle_deg": np.round(np.where(in_turn,
                                                        np.random.uniform(15, 25, size=batch_size) * np.random.choice([-1, 1], size=batch_size),
                                                        np.random.uniform(-2, 2, size=batch_size)), 1).tolist(),
                "ride_height_front_in": np.round(np.random.uniform(3.0, 3.5, size=batch_size), 2).tolist(),
                "ride_height_rear_in": np.round(np.random.uniform(4.0, 4.5, size=batch_size), 2).tolist(),
                "lap": [self.laps_completed[i] for i in car_indices]
            }
        }

    def _generate_tire_batch(self, current_time_ms: int, batch_size: int) -> dict:
        """Generate tire telemetry batch using NumPy"""
        car_indices = np.random.randint(0, self.num_cars, size=batch_size)
        wear_factors = np.array([(self.laps_completed[i] % 50) / 50.0 for i in car_indices])

        # Generate pressure for all 4 tires
        base_pressure = np.random.uniform(30, 35, size=(batch_size, 4))
        pressure_increase = wear_factors.reshape(-1, 1) * 2
        pressures = base_pressure + pressure_increase

        # Generate temperature for all 4 tires
        base_temp = 180
        temp_increase = (wear_factors * 40).astype(int).reshape(-1, 1)
        temps = base_temp + np.tile(temp_increase, (1, 4))

        return {
            "m": "tire_telemetry",
            "columns": {
                "time": [current_time_ms] * batch_size,
                "car_number": [self.drivers[i]["car_number"] for i in car_indices],
                "driver": [self.drivers[i]["driver"] for i in car_indices],
                "lf_pressure_psi": np.round(pressures[:, 0], 1).tolist(),
                "rf_pressure_psi": np.round(pressures[:, 1], 1).tolist(),
                "lr_pressure_psi": np.round(pressures[:, 2], 1).tolist(),
                "rr_pressure_psi": np.round(pressures[:, 3], 1).tolist(),
                "lf_temp_f": temps[:, 0].tolist(),
                "rf_temp_f": temps[:, 1].tolist(),
                "lr_temp_f": temps[:, 2].tolist(),
                "rr_temp_f": temps[:, 3].tolist(),
                "lap": [self.laps_completed[i] for i in car_indices]
            }
        }

    def get_batch(self, telemetry_type: str) -> bytes:
        """Get a random pre-generated batch for the specified telemetry type"""
        return random.choice(self.batches[telemetry_type])


class NASCARLoadTester:
    """High-performance NASCAR telemetry load tester"""

    def __init__(self, url: str, token: str, data_pool: NASCARTelemetryDataPool, database: str):
        self.url = f"{url}/api/v1/write/msgpack"
        self.token = token
        self.data_pool = data_pool
        self.database = database

        # Stats (thread-safe with lock)
        self.total_sent = 0
        self.total_errors = 0
        self.latencies = []
        self.lock = asyncio.Lock()

    async def send_batch(self, session: aiohttp.ClientSession, telemetry_type: str) -> bool:
        """Send pre-compressed MessagePack batch"""
        start = time.perf_counter()

        try:
            # Get pre-compressed payload (near-zero overhead)
            payload = self.data_pool.get_batch(telemetry_type)

            # Send
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/msgpack",
                "Content-Encoding": "gzip",
                "X-Arc-Database": self.database
            }

            async with session.post(self.url, data=payload, headers=headers) as response:
                latency = (time.perf_counter() - start) * 1000

                async with self.lock:
                    if response.status in (200, 204):
                        # Each batch contains batch_size/3 messages for this telemetry type
                        self.total_sent += self.data_pool.batch_size // 3
                        self.latencies.append(latency)
                        return True
                    else:
                        self.total_errors += 1
                        # Log first few errors
                        if self.total_errors <= 3:
                            error_body = await response.text()
                            print(f"\n❌ Error {self.total_errors}: Status {response.status} - {telemetry_type}")
                            print(f"   Response: {error_body[:200]}")
                        return False

        except Exception as e:
            async with self.lock:
                self.total_errors += 1
                # Log first few exceptions
                if self.total_errors <= 3:
                    print(f"\n❌ Exception {self.total_errors}: {type(e).__name__}: {e}")
            return False

    async def worker(self, session: aiohttp.ClientSession, telemetry_type: str, target_rps: int, duration: int):
        """Worker coroutine that sends batches"""
        messages_per_batch = self.data_pool.batch_size // 3
        batches_per_second = target_rps / messages_per_batch
        delay = 1.0 / batches_per_second if batches_per_second > 0 else 0.1

        start_time = time.time()

        while time.time() - start_time < duration:
            await self.send_batch(session, telemetry_type)
            await asyncio.sleep(delay)

    def get_percentile(self, percentile: float) -> float:
        """Get latency percentile"""
        if not self.latencies:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        index = int(len(sorted_latencies) * percentile)
        return sorted_latencies[min(index, len(sorted_latencies) - 1)]


async def run_load_test(
    arc_url: str,
    token: str,
    database: str,
    target_msg_per_sec: int,
    duration: int,
    batch_size: int,
    num_batches_pregenerate: int,
    num_cars: int,
    num_workers: int
):
    """Run the NASCAR telemetry load test"""
    print("=" * 80)
    print("Arc NASCAR Telemetry Load Test")
    print("=" * 80)
    print(f"🎯 Target: {arc_url}")
    print(f"🗄️  Database: {database}")
    print(f"🏎️  Cars: {num_cars}")
    print(f"📊 Target rate: {target_msg_per_sec:,} messages/sec")
    print(f"📦 Batch size: {batch_size:,} messages")
    print(f"⏱️  Duration: {duration} seconds")
    print(f"💾 Pre-generating: {num_batches_pregenerate} batches")
    print(f"👷 Workers: {num_workers} concurrent workers")
    print("=" * 80)
    print()

    # Pre-generate data pool
    data_pool = NASCARTelemetryDataPool(
        num_batches=num_batches_pregenerate,
        batch_size=batch_size,
        num_cars=num_cars
    )

    # Initialize tester
    tester = NASCARLoadTester(arc_url, token, data_pool, database)

    # Calculate intelligent connection pool size
    if num_workers <= 100:
        connector_limit = num_workers + 50
    elif num_workers <= 300:
        connector_limit = 200
    else:
        connector_limit = 256

    print(f"Connection pool: {connector_limit} connections for {num_workers} workers")
    print()

    # Create session with optimized connection pooling
    connector = aiohttp.TCPConnector(
        limit=connector_limit,
        limit_per_host=connector_limit,
        ttl_dns_cache=300,
        force_close=False,  # Reuse connections aggressively
        enable_cleanup_closed=True,
        keepalive_timeout=30  # Keep connections alive for reuse
    )
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Warm up connection pool
        print("Warming up connection pool...")
        warmup_tasks = []
        for _ in range(min(10, num_workers)):
            async def warmup():
                try:
                    async with session.get(f"{arc_url}/health") as resp:
                        await resp.text()
                except:
                    pass
            warmup_tasks.append(warmup())
        await asyncio.gather(*warmup_tasks, return_exceptions=True)
        print("✅ Connection pool warmed up\n")

        # Distribute workers across 3 telemetry types
        workers_per_type = num_workers // 3
        rate_per_type = target_msg_per_sec // 3
        rate_per_worker = rate_per_type // workers_per_type if workers_per_type > 0 else rate_per_type

        # Start workers
        start_time = time.time()
        workers = []

        for telemetry_type in ["engine_telemetry", "chassis_telemetry", "tire_telemetry"]:
            for _ in range(workers_per_type):
                workers.append(
                    asyncio.create_task(
                        tester.worker(session, telemetry_type, rate_per_worker, duration)
                    )
                )

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
                f"[{elapsed:6.1f}s] RPS: {int(current_rps):>7} (target: {target_msg_per_sec:,}) | "
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
    actual_rps = tester.total_sent / elapsed if elapsed > 0 else 0

    p50 = tester.get_percentile(0.50)
    p95 = tester.get_percentile(0.95)
    p99 = tester.get_percentile(0.99)
    p999 = tester.get_percentile(0.999)

    total_attempts = tester.total_sent + tester.total_errors
    success_rate = (tester.total_sent / total_attempts * 100) if total_attempts > 0 else 0

    print(f"Duration:        {elapsed:.1f}s")
    print(f"Total Sent:      {tester.total_sent:,} messages")
    print(f"Total Errors:    {tester.total_errors}")
    print(f"Success Rate:    {success_rate:.2f}%")
    print(f"Actual RPS:      {int(actual_rps):,} messages/sec")
    print(f"Target RPS:      {target_msg_per_sec:,} messages/sec")
    achievement = (actual_rps / target_msg_per_sec * 100) if target_msg_per_sec > 0 else 0
    print(f"Achievement:     {achievement:.1f}%")
    print()
    print(f"Latency Percentiles (ms):")
    print(f"  p50:  {p50:.2f}")
    print(f"  p95:  {p95:.2f}")
    print(f"  p99:  {p99:.2f}")
    print(f"  p999: {p999:.2f}")
    print("=" * 80)
    print()
    print("🔍 Example queries for Grafana dashboards:")
    print()
    print("1. Real-time speed comparison (top 5 cars):")
    print(f"   SELECT car_number, driver, AVG(speed_mph) as avg_speed")
    print(f"   FROM {database}.engine_telemetry")
    print("   WHERE time > now() - INTERVAL '30 seconds'")
    print("   GROUP BY car_number, driver")
    print("   ORDER BY avg_speed DESC")
    print("   LIMIT 5;")


async def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic NASCAR race telemetry for Arc load test"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Arc server URL (default: http://localhost:8000)"
    )
    parser.add_argument(
        "--token",
        required=True,
        help="Arc API token"
    )
    parser.add_argument(
        "--database",
        default="nascar_telemetry",
        help="Database name (default: nascar_telemetry)"
    )
    parser.add_argument(
        "--cars",
        type=int,
        default=40,
        help="Number of cars in the race (default: 40)"
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=612000,
        help="Target total messages per second (default: 612000 = NASCAR baseline, max: 2400000)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds (default: 60)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Messages per batch (default: 10000)"
    )
    parser.add_argument(
        "--pregenerate-batches",
        type=int,
        default=500,
        help="Number of batches to pre-generate (default: 500)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=300,
        help="Number of concurrent workers (default: 300, use 500+ for maximum throughput)"
    )
    parser.add_argument(
        "--full-race",
        action="store_true",
        help="Generate and stream a full 3-hour NASCAR race (auto-calculates duration and batches)"
    )

    args = parser.parse_args()

    # If full race mode, calculate parameters for a 3-hour race
    if args.full_race:
        race_duration_hours = 3
        race_duration_seconds = race_duration_hours * 3600  # 10,800 seconds

        # Calculate batches needed: one batch per second
        args.pregenerate_batches = race_duration_seconds
        args.duration = race_duration_seconds

        print("=" * 80)
        print("🏁 FULL NASCAR RACE MODE")
        print("=" * 80)
        print(f"Race Duration: {race_duration_hours} hours ({race_duration_seconds:,} seconds)")
        print(f"Pre-generating: {args.pregenerate_batches:,} batches (1 per second)")
        print(f"Total Messages: {args.pregenerate_batches * args.batch_size:,}")
        print(f"Target Rate: {args.rate:,} messages/sec")
        print("=" * 80)
        print()

    await run_load_test(
        arc_url=args.url,
        token=args.token,
        database=args.database,
        target_msg_per_sec=args.rate,
        duration=args.duration,
        batch_size=args.batch_size,
        num_batches_pregenerate=args.pregenerate_batches,
        num_cars=args.cars,
        num_workers=args.workers
    )


if __name__ == "__main__":
    asyncio.run(main())
