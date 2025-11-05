#!/usr/bin/env python3
"""
Arc Demo Scenario: NASCAR Race Telemetry

This script generates realistic NASCAR race telemetry data to demonstrate Arc's capability
to handle high-volume real-time time-series data ingestion and querying.

NASCAR generates approximately 612,000 messages per second across all cars during a race:
- 40 cars on track
- Over 60 different data points per car (engine RPM, brake pressure, speed, tire temp, etc.)
- Sampled hundreds of times per second
- ~15,300 messages per second per car (612,000 ÷ 40)
- Message size: ~0.15KB per message
- Total throughput: ~92 MB/s sustained for up to 4 hours

This demo simulates a full NASCAR race with all 40 cars:
- Real-time telemetry streaming from 40 race cars
- **612,000 messages per second total** (matching real NASCAR scale)
- **~92 MB/s sustained throughput**
- Multiple sensor readings: engine, chassis, tire telemetry
- Realistic race dynamics including position changes and lap tracking
- Multiple teams and manufacturers

Use cases demonstrated:
- High-frequency time-series data ingestion (612k msg/sec baseline)
- Real-time race monitoring across all teams
- Performance analytics and car comparison
- Race strategy optimization with live telemetry
- Sustained high-volume writes for 4+ hour race duration
- Can scale to test Arc's 2.4M msg/sec capacity
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


class NASCARTelemetryScenario:
    """Generates realistic NASCAR race telemetry data"""

    def __init__(self, arc_url: str, token: str, database: str, num_cars: int = 40,
                 messages_per_second: int = 15300, duration_seconds: int = None):
        """
        Args:
            arc_url: Arc server URL
            token: API token
            database: Database name for telemetry data
            num_cars: Number of cars in the race (default: 40, full NASCAR field)
            messages_per_second: Target message rate PER CAR (default: 15,300 per car = 612k total)
            duration_seconds: How long to run (None = continuous)
        """
        self.arc_url = arc_url
        self.token = token
        self.database = database
        self.num_cars = num_cars
        self.messages_per_second = messages_per_second
        self.duration_seconds = duration_seconds
        self.session = None

        # Race simulation state
        self.race_start_time = int(time.time() * 1000)
        self.race_elapsed_ms = 0
        self.race_state = "green"  # green, yellow (caution), red (stopped)
        self.laps_completed = [0] * num_cars

        # Track configuration (simulating a 1.5 mile oval like Charlotte Motor Speedway)
        self.track_length_miles = 1.5
        self.track_length_meters = self.track_length_miles * 1609.34
        self.ideal_lap_time_seconds = 28.0  # ~190 mph average

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

        # Stats
        self.total_messages_sent = 0
        self.total_bytes_sent = 0
        self.start_time = None

    async def setup(self):
        """Setup HTTP session"""
        self.session = aiohttp.ClientSession()
        self.start_time = time.time()

    async def cleanup(self):
        """Cleanup HTTP session"""
        if self.session:
            await self.session.close()

    async def send_batch(self, measurement: str, payload: dict):
        """Send a batch of telemetry to Arc

        Args:
            measurement: Measurement name
            payload: MessagePack payload with columnar data
        """
        # Serialize to MessagePack
        data = msgpack.packb(payload)

        # Compress with gzip
        compressed = gzip.compress(data)

        headers = {
            "Content-Type": "application/msgpack",
            "Content-Encoding": "gzip",
            "X-Arc-Database": self.database,
            "Authorization": f"Bearer {self.token}"
        }

        try:
            async with self.session.post(
                f"{self.arc_url}/api/v1/write/msgpack",
                data=compressed,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status in (200, 204):
                    self.total_messages_sent += len(payload["columns"]["time"])
                    self.total_bytes_sent += len(compressed)
                    return True
                else:
                    text = await response.text()
                    print(f"❌ Error sending {measurement}: {response.status} - {text}")
                    return False
        except Exception as e:
            print(f"❌ Exception sending data: {e}")
            return False

    def update_car_positions(self, time_delta_seconds: float):
        """Update car positions based on elapsed time

        Args:
            time_delta_seconds: Time elapsed since last update
        """
        for i in range(self.num_cars):
            # Base speed varies by car performance
            base_speed_mph = random.uniform(180, 195)

            # During caution, all cars slow down
            if self.race_state == "yellow":
                base_speed_mph = 60
            elif self.race_state == "red":
                base_speed_mph = 0

            # Convert to meters per second
            speed_mps = base_speed_mph * 0.44704

            # Update position
            self.car_positions[i] += speed_mps * time_delta_seconds

            # Handle lap completion
            if self.car_positions[i] >= self.track_length_meters:
                self.car_positions[i] -= self.track_length_meters
                self.laps_completed[i] += 1

    def generate_engine_telemetry(self, current_time_ms: int, car_idx: int):
        """Generate engine telemetry for one car

        Returns dict with engine metrics
        """
        driver = self.drivers[car_idx]

        # Simulate engine parameters
        # RPM varies based on position on track (higher in straights, lower in turns)
        track_position_pct = self.car_positions[car_idx] / self.track_length_meters
        in_turn = math.sin(track_position_pct * 4 * math.pi) < 0  # 4 turns per lap

        if self.race_state == "green":
            if in_turn:
                rpm = random.uniform(7000, 8500)
                throttle = random.uniform(70, 90)
                speed_mph = random.uniform(160, 180)
            else:
                rpm = random.uniform(8500, 9500)
                throttle = random.uniform(90, 100)
                speed_mph = random.uniform(185, 200)
        else:
            # Under caution
            rpm = random.uniform(3000, 4000)
            throttle = random.uniform(20, 30)
            speed_mph = random.uniform(55, 65)

        return {
            "car_number": driver["car_number"],
            "driver": driver["driver"],
            "team": driver["team"],
            "manufacturer": driver["manufacturer"],
            "rpm": int(rpm),
            "throttle_position": round(throttle, 1),
            "speed_mph": round(speed_mph, 1),
            "engine_temp": random.randint(190, 220),
            "oil_pressure": random.randint(40, 65),
            "oil_temp": random.randint(240, 280),
            "fuel_level": round(random.uniform(2, 18), 2),  # gallons
            "gear": random.randint(3, 4) if in_turn else 4,
            "lap": self.laps_completed[car_idx]
        }

    def generate_chassis_telemetry(self, current_time_ms: int, car_idx: int):
        """Generate chassis/suspension telemetry for one car"""
        driver = self.drivers[car_idx]

        track_position_pct = self.car_positions[car_idx] / self.track_length_meters
        in_turn = math.sin(track_position_pct * 4 * math.pi) < 0

        if in_turn:
            # Higher lateral G-forces in turns
            lateral_g = random.uniform(2.0, 3.5)
            longitudinal_g = random.uniform(-0.5, 0.2)
            brake_pressure = random.randint(500, 1500) if random.random() < 0.3 else 0
            steering_angle = random.uniform(-25, -15) if random.random() < 0.5 else random.uniform(15, 25)
        else:
            # Straights
            lateral_g = random.uniform(-0.2, 0.2)
            longitudinal_g = random.uniform(0.5, 1.2)
            brake_pressure = 0
            steering_angle = random.uniform(-2, 2)

        return {
            "car_number": driver["car_number"],
            "driver": driver["driver"],
            "lateral_g": round(lateral_g, 2),
            "longitudinal_g": round(longitudinal_g, 2),
            "vertical_g": round(random.uniform(0.8, 1.2), 2),
            "brake_pressure_psi": brake_pressure,
            "steering_angle_deg": round(steering_angle, 1),
            "ride_height_front_in": round(random.uniform(3.0, 3.5), 2),
            "ride_height_rear_in": round(random.uniform(4.0, 4.5), 2),
            "lap": self.laps_completed[car_idx]
        }

    def generate_tire_telemetry(self, current_time_ms: int, car_idx: int):
        """Generate tire telemetry for one car"""
        driver = self.drivers[car_idx]

        # Tire temps and pressures increase over time/laps
        laps_since_pit = self.laps_completed[car_idx] % 50  # Assume pit every 50 laps
        wear_factor = laps_since_pit / 50.0

        return {
            "car_number": driver["car_number"],
            "driver": driver["driver"],
            "lf_pressure_psi": round(random.uniform(30, 35) + wear_factor * 2, 1),
            "rf_pressure_psi": round(random.uniform(30, 35) + wear_factor * 2, 1),
            "lr_pressure_psi": round(random.uniform(30, 35) + wear_factor * 2, 1),
            "rr_pressure_psi": round(random.uniform(30, 35) + wear_factor * 2, 1),
            "lf_temp_f": int(180 + wear_factor * 40),
            "rf_temp_f": int(180 + wear_factor * 40),
            "lr_temp_f": int(180 + wear_factor * 40),
            "rr_temp_f": int(180 + wear_factor * 40),
            "lap": self.laps_completed[car_idx]
        }

    async def generate_and_send_batch(self, current_time_ms: int, batch_size: int):
        """Generate and send a batch of telemetry data

        Args:
            current_time_ms: Current timestamp in milliseconds
            batch_size: Number of messages to generate
        """
        # Distribute messages across the three telemetry types
        engine_count = batch_size // 3
        chassis_count = batch_size // 3
        tire_count = batch_size - engine_count - chassis_count

        # Generate engine telemetry
        engine_data = []
        for _ in range(engine_count):
            car_idx = random.randint(0, self.num_cars - 1)
            data = self.generate_engine_telemetry(current_time_ms, car_idx)
            data["time"] = current_time_ms
            engine_data.append(data)

        if engine_data:
            payload = {
                "m": "engine_telemetry",
                "columns": {
                    "time": [d["time"] for d in engine_data],
                    "car_number": [d["car_number"] for d in engine_data],
                    "driver": [d["driver"] for d in engine_data],
                    "team": [d["team"] for d in engine_data],
                    "manufacturer": [d["manufacturer"] for d in engine_data],
                    "rpm": [d["rpm"] for d in engine_data],
                    "throttle_position": [d["throttle_position"] for d in engine_data],
                    "speed_mph": [d["speed_mph"] for d in engine_data],
                    "engine_temp": [d["engine_temp"] for d in engine_data],
                    "oil_pressure": [d["oil_pressure"] for d in engine_data],
                    "oil_temp": [d["oil_temp"] for d in engine_data],
                    "fuel_level": [d["fuel_level"] for d in engine_data],
                    "gear": [d["gear"] for d in engine_data],
                    "lap": [d["lap"] for d in engine_data]
                }
            }
            await self.send_batch("engine_telemetry", payload)

        # Generate chassis telemetry
        chassis_data = []
        for _ in range(chassis_count):
            car_idx = random.randint(0, self.num_cars - 1)
            data = self.generate_chassis_telemetry(current_time_ms, car_idx)
            data["time"] = current_time_ms
            chassis_data.append(data)

        if chassis_data:
            payload = {
                "m": "chassis_telemetry",
                "columns": {
                    "time": [d["time"] for d in chassis_data],
                    "car_number": [d["car_number"] for d in chassis_data],
                    "driver": [d["driver"] for d in chassis_data],
                    "lateral_g": [d["lateral_g"] for d in chassis_data],
                    "longitudinal_g": [d["longitudinal_g"] for d in chassis_data],
                    "vertical_g": [d["vertical_g"] for d in chassis_data],
                    "brake_pressure_psi": [d["brake_pressure_psi"] for d in chassis_data],
                    "steering_angle_deg": [d["steering_angle_deg"] for d in chassis_data],
                    "ride_height_front_in": [d["ride_height_front_in"] for d in chassis_data],
                    "ride_height_rear_in": [d["ride_height_rear_in"] for d in chassis_data],
                    "lap": [d["lap"] for d in chassis_data]
                }
            }
            await self.send_batch("chassis_telemetry", payload)

        # Generate tire telemetry
        tire_data = []
        for _ in range(tire_count):
            car_idx = random.randint(0, self.num_cars - 1)
            data = self.generate_tire_telemetry(current_time_ms, car_idx)
            data["time"] = current_time_ms
            tire_data.append(data)

        if tire_data:
            payload = {
                "m": "tire_telemetry",
                "columns": {
                    "time": [d["time"] for d in tire_data],
                    "car_number": [d["car_number"] for d in tire_data],
                    "driver": [d["driver"] for d in tire_data],
                    "lf_pressure_psi": [d["lf_pressure_psi"] for d in tire_data],
                    "rf_pressure_psi": [d["rf_pressure_psi"] for d in tire_data],
                    "lr_pressure_psi": [d["lr_pressure_psi"] for d in tire_data],
                    "rr_pressure_psi": [d["rr_pressure_psi"] for d in tire_data],
                    "lf_temp_f": [d["lf_temp_f"] for d in tire_data],
                    "rf_temp_f": [d["rf_temp_f"] for d in tire_data],
                    "lr_temp_f": [d["lr_temp_f"] for d in tire_data],
                    "rr_temp_f": [d["rr_temp_f"] for d in tire_data],
                    "lap": [d["lap"] for d in tire_data]
                }
            }
            await self.send_batch("tire_telemetry", payload)

    async def run_scenario(self):
        """Execute the telemetry streaming scenario"""
        print("🏁 Arc Demo Scenario: NASCAR Race Telemetry")
        print("=" * 70)
        print(f"📅 Race start time: {datetime.fromtimestamp(self.race_start_time / 1000)}")
        print(f"🏎️  Cars on track: {self.num_cars}")
        print(f"🏁 Teams: {len(set(d['team'] for d in self.drivers))} teams")
        print(f"🏭 Manufacturers: {', '.join(set(d['manufacturer'] for d in self.drivers))}")
        print(f"📊 Target rate: {self.messages_per_second:,} msg/sec per car × {self.num_cars} cars = {self.messages_per_second * self.num_cars:,} total msg/sec")
        expected_throughput_mb = (self.messages_per_second * self.num_cars * 0.15) / 1024
        print(f"💾 Expected throughput: ~{expected_throughput_mb:.1f} MB/s (@ 0.15KB per message)")
        print(f"🏟️  Track: {self.track_length_miles} mile oval")
        print(f"⏱️  Ideal lap time: {self.ideal_lap_time_seconds}s (~{3600 * self.track_length_miles / self.ideal_lap_time_seconds:.0f} mph avg)")
        print(f"🎯 Target: {self.arc_url}")
        print(f"🗄️  Database: {self.database}")
        if self.duration_seconds:
            print(f"⏳ Duration: {self.duration_seconds} seconds")
        else:
            print(f"⏳ Duration: Continuous (Ctrl+C to stop)")
        print()
        print("📡 Telemetry types: Engine (RPM, speed, throttle) | Chassis (G-forces) | Tires (pressure, temp)")
        print("Press Ctrl+C to stop...")
        print()

        # Calculate batch parameters
        # Send batches every 100ms for smooth throughput
        batch_interval_seconds = 0.1
        messages_per_batch = int(self.messages_per_second * batch_interval_seconds)

        print(f"📦 Batch config: {messages_per_batch} messages every {batch_interval_seconds}s")
        print()

        last_stats_time = time.time()
        last_update_time = time.time()

        try:
            while True:
                loop_start = time.time()
                current_time_ms = int(loop_start * 1000)

                # Update race simulation
                time_delta = loop_start - last_update_time
                self.update_car_positions(time_delta)
                last_update_time = loop_start

                # Generate and send batch
                await self.generate_and_send_batch(current_time_ms, messages_per_batch)

                # Print stats every 5 seconds
                if loop_start - last_stats_time >= 5.0:
                    elapsed = loop_start - self.start_time
                    msg_rate = self.total_messages_sent / elapsed if elapsed > 0 else 0
                    bytes_rate = self.total_bytes_sent / elapsed if elapsed > 0 else 0

                    # Find leader
                    leader_idx = max(range(self.num_cars), key=lambda i: self.laps_completed[i])
                    leader = self.drivers[leader_idx]

                    print(f"📈 [{int(elapsed)}s] Rate: {msg_rate:,.0f} msg/s | "
                          f"Throughput: {bytes_rate/1024/1024:.2f} MB/s | "
                          f"Total: {self.total_messages_sent:,} msgs | "
                          f"Leader: #{leader['car_number']} ({leader['driver']}) Lap {self.laps_completed[leader_idx]}")

                    last_stats_time = loop_start

                # Check duration
                if self.duration_seconds and (loop_start - self.start_time) >= self.duration_seconds:
                    break

                # Sleep to maintain target rate
                elapsed_in_loop = time.time() - loop_start
                sleep_time = max(0, batch_interval_seconds - elapsed_in_loop)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            print("\n🛑 Stopped by user")

        # Final stats
        total_elapsed = time.time() - self.start_time
        print()
        print("🏁 Race telemetry streaming complete!")
        print("=" * 70)
        print(f"⏱️  Total duration: {total_elapsed:.1f} seconds")
        print(f"📊 Total messages: {self.total_messages_sent:,}")
        print(f"📈 Average rate: {self.total_messages_sent / total_elapsed:,.0f} messages/second")
        print(f"💾 Total data sent: {self.total_bytes_sent / 1024 / 1024:.2f} MB (compressed)")
        print(f"🏎️  Laps completed: Leader completed {max(self.laps_completed)} laps")
        print()
        print("🔍 Example queries for Grafana dashboards:")
        print()
        print("1. Real-time speed comparison (top 5 cars):")
        print(f"   SELECT car_number, driver, AVG(speed_mph) as avg_speed")
        print(f"   FROM {self.database}.engine_telemetry")
        print("   WHERE time > now() - INTERVAL '30 seconds'")
        print("   GROUP BY car_number, driver")
        print("   ORDER BY avg_speed DESC")
        print("   LIMIT 5;")
        print()
        print("2. Engine performance by manufacturer:")
        print(f"   SELECT manufacturer, AVG(rpm) as avg_rpm, AVG(throttle_position) as avg_throttle")
        print(f"   FROM {self.database}.engine_telemetry")
        print("   WHERE time > now() - INTERVAL '1 minute'")
        print("   GROUP BY manufacturer;")
        print()
        print("3. Tire degradation analysis:")
        print(f"   SELECT car_number, lap,")
        print("     AVG(lf_temp_f + rf_temp_f + lr_temp_f + rr_temp_f) / 4 as avg_tire_temp,")
        print("     AVG(lf_pressure_psi + rf_pressure_psi + lr_pressure_psi + rr_pressure_psi) / 4 as avg_tire_pressure")
        print(f"   FROM {self.database}.tire_telemetry")
        print("   WHERE car_number = 24")
        print("   GROUP BY car_number, lap")
        print("   ORDER BY lap DESC")
        print("   LIMIT 50;")
        print()
        print("4. G-force distribution (cornering performance):")
        print(f"   SELECT car_number, driver,")
        print("     MAX(lateral_g) as max_lateral_g,")
        print("     AVG(ABS(lateral_g)) as avg_lateral_g")
        print(f"   FROM {self.database}.chassis_telemetry")
        print("   WHERE time > now() - INTERVAL '5 minutes'")
        print("   GROUP BY car_number, driver")
        print("   ORDER BY max_lateral_g DESC;")


async def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic NASCAR race telemetry for Arc demo"
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
        help="Number of cars in the race (default: 40, full NASCAR field)"
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=15300,
        help="Target messages per second PER CAR (default: 15300, 40 cars = 612k total)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Duration in seconds (default: continuous, use Ctrl+C to stop)"
    )

    args = parser.parse_args()

    scenario = NASCARTelemetryScenario(
        arc_url=args.url,
        token=args.token,
        database=args.database,
        num_cars=args.cars,
        messages_per_second=args.rate,
        duration_seconds=args.duration
    )

    await scenario.setup()
    try:
        await scenario.run_scenario()
    finally:
        await scenario.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
