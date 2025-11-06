# NASCAR Race Telemetry Demo

Simulates real NASCAR race telemetry based on AWS's published NASCAR telemetry architecture.

## Real NASCAR Stats

- **40 cars** on track during a race
- **Over 60 data points** per car (engine RPM, brake pressure, speed, tire temp, fuel, g-forces, etc.)
- **Sampled hundreds of times per second**
- **612,000 messages per second total** across all cars
- **~15,300 messages per second per car**
- **Message size:** ~0.15KB per message
- **Sustained throughput:** ~92 MB/s for up to 4 hours

## What This Demo Shows

This scenario demonstrates Arc's ability to:

1. **High-frequency time-series ingestion** - Sustain 612k+ messages/second
2. **Real-time analytics** - Query live telemetry data as it's being ingested
3. **Multi-entity tracking** - Monitor 40 cars simultaneously across multiple teams
4. **Performance comparison** - Compare telemetry across manufacturers, teams, and drivers
5. **Long-duration workloads** - Maintain performance over 4+ hour race duration

## Usage

This script pre-generates NASCAR telemetry data and then sends it to Arc at a controlled rate to test ingestion performance.

### Basic Usage (NASCAR Baseline - 612k msg/sec)

```bash
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --url http://localhost:8000 \
  --database nascar_telemetry \
  --rate 612000 \
  --duration 60
```

This will generate **612,000 messages/second** - the real NASCAR baseline.

### Full Race Simulation (3 Hours)

```bash
# Generate and stream an entire 3-hour NASCAR race
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --full-race \
  --rate 612000 \
  --workers 300
```

This will:
- Pre-generate **10,800 batches** (1 per second for 3 hours)
- Stream **~108 million messages** total
- Simulate a complete NASCAR race with realistic telemetry
- Take approximately **3 hours** to stream at 612k msg/sec

### Test Arc's Maximum Capacity (2.4M msg/sec)

```bash
# Test at 2.4M messages/second (Arc's rated capacity)
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --rate 2400000 \
  --duration 300 \
  --workers 500
```

### Custom Configuration

```bash
# Smaller test (300k msg/sec, 30 seconds)
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --rate 300000 \
  --duration 30 \
  --batch-size 5000

# Large batch size for maximum throughput
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --rate 1200000 \
  --batch-size 20000 \
  --pregenerate-batches 1000
```

### Command-Line Arguments

- `--url` - Arc server URL (default: `http://localhost:8000`)
- `--token` - Arc API token (required)
- `--database` - Database name (default: `nascar_telemetry`)
- `--cars` - Number of cars in the race (default: `40`)
- `--rate` - **Total messages per second** across all cars (default: `612000`)
- `--duration` - Duration in seconds (default: `60`)
- `--batch-size` - Messages per batch (default: `10000`)
- `--pregenerate-batches` - Number of batches to pre-generate (default: `500`)
- `--workers` - Number of concurrent workers (default: `300`, use `500+` for max throughput)
- `--full-race` - Generate and stream a full 3-hour NASCAR race (auto-calculates duration and batches)

## Data Schema

The demo generates three types of telemetry data:

### 1. Engine Telemetry (`engine_telemetry`)

| Column | Type | Description |
|--------|------|-------------|
| `time` | int64 | Timestamp in milliseconds |
| `car_number` | int | Car number (1-40) |
| `driver` | string | Driver name |
| `team` | string | Team name |
| `manufacturer` | string | Chevrolet, Ford, or Toyota |
| `rpm` | int | Engine RPM (3000-9500) |
| `throttle_position` | float | Throttle % (0-100) |
| `speed_mph` | float | Speed in MPH (55-200) |
| `engine_temp` | int | Engine temperature (°F) |
| `oil_pressure` | int | Oil pressure (PSI) |
| `oil_temp` | int | Oil temperature (°F) |
| `fuel_level` | float | Fuel level (gallons) |
| `gear` | int | Current gear (3-4) |
| `lap` | int | Current lap number |

### 2. Chassis Telemetry (`chassis_telemetry`)

| Column | Type | Description |
|--------|------|-------------|
| `time` | int64 | Timestamp in milliseconds |
| `car_number` | int | Car number (1-40) |
| `driver` | string | Driver name |
| `lateral_g` | float | Lateral G-force (-3.5 to 3.5) |
| `longitudinal_g` | float | Longitudinal G-force |
| `vertical_g` | float | Vertical G-force |
| `brake_pressure_psi` | int | Brake pressure (PSI) |
| `steering_angle_deg` | float | Steering angle (degrees) |
| `ride_height_front_in` | float | Front ride height (inches) |
| `ride_height_rear_in` | float | Rear ride height (inches) |
| `lap` | int | Current lap number |

### 3. Tire Telemetry (`tire_telemetry`)

| Column | Type | Description |
|--------|------|-------------|
| `time` | int64 | Timestamp in milliseconds |
| `car_number` | int | Car number (1-40) |
| `driver` | string | Driver name |
| `lf_pressure_psi` | float | Left front tire pressure (PSI) |
| `rf_pressure_psi` | float | Right front tire pressure (PSI) |
| `lr_pressure_psi` | float | Left rear tire pressure (PSI) |
| `rr_pressure_psi` | float | Right rear tire pressure (PSI) |
| `lf_temp_f` | int | Left front tire temperature (°F) |
| `rf_temp_f` | int | Right front tire temperature (°F) |
| `lr_temp_f` | int | Left rear tire temperature (°F) |
| `rr_temp_f` | int | Right rear tire temperature (°F) |
| `lap` | int | Current lap number |

## Example Queries for Grafana Dashboards

### 1. Real-Time Leaderboard (Top 10 by Speed)

```sql
SELECT
    car_number,
    driver,
    team,
    manufacturer,
    AVG(speed_mph) as avg_speed
FROM nascar_telemetry.engine_telemetry
WHERE time > now() - INTERVAL '10 seconds'
GROUP BY car_number, driver, team, manufacturer
ORDER BY avg_speed DESC
LIMIT 10;
```

### 2. Speed Over Time (Specific Car)

```sql
SELECT
    time,
    speed_mph,
    rpm,
    throttle_position,
    fuel_level,
    lap
FROM nascar_telemetry.engine_telemetry
WHERE car_number = 24
  AND time >= now() - INTERVAL '5 minutes'
ORDER BY time DESC
LIMIT 1000;
```

### 3. Manufacturer Performance Comparison

```sql
SELECT
    manufacturer,
    COUNT(*) as samples,
    AVG(rpm) as avg_rpm,
    AVG(speed_mph) as avg_speed,
    AVG(throttle_position) as avg_throttle,
    MAX(speed_mph) as max_speed
FROM nascar_telemetry.engine_telemetry
WHERE time >= now() - INTERVAL '1 minute'
GROUP BY manufacturer
ORDER BY avg_speed DESC;
```

### 4. Tire Degradation Analysis

```sql
SELECT
    car_number,
    ANY_VALUE(driver) as driver,
    lap,
    ROUND(AVG((lf_temp_f + rf_temp_f + lr_temp_f + rr_temp_f) / 4.0), 1) as avg_tire_temp,
    ROUND(AVG((lf_pressure_psi + rf_pressure_psi + lr_pressure_psi + rr_pressure_psi) / 4.0), 1) as avg_tire_pressure,
    MAX(lf_temp_f) as hottest_tire_temp
FROM nascar_telemetry.tire_telemetry
WHERE car_number = 24
  AND time >= now() - INTERVAL '10 minutes'
GROUP BY car_number, lap
ORDER BY lap DESC
LIMIT 50;
```

### 5. G-Force Distribution (Cornering Performance)

```sql
SELECT
    car_number,
    driver,
    MAX(lateral_g) as max_lateral_g,
    ROUND(AVG(ABS(lateral_g)), 2) as avg_lateral_g,
    MAX(longitudinal_g) as max_longitudinal_g,
    MAX(brake_pressure_psi) as max_brake_pressure
FROM nascar_telemetry.chassis_telemetry
WHERE time >= now() - INTERVAL '5 minutes'
GROUP BY car_number, driver
ORDER BY max_lateral_g DESC
LIMIT 10;
```

### 6. Fuel Strategy Monitor

```sql
SELECT
    car_number,
    ANY_VALUE(driver) as driver,
    ANY_VALUE(team) as team,
    MAX(lap) as current_lap,
    ROUND(AVG(fuel_level), 2) as avg_fuel_level,
    ROUND(MIN(fuel_level), 2) as min_fuel_level,
    ROUND(MAX(fuel_level), 2) as max_fuel_level,
    COUNT(*) as samples
FROM nascar_telemetry.engine_telemetry
WHERE time >= now() - INTERVAL '2 minutes'
GROUP BY car_number
ORDER BY avg_fuel_level ASC
LIMIT 10;
```

### 7. Team Comparison Dashboard

```sql
SELECT
    team,
    COUNT(DISTINCT car_number) as cars,
    ROUND(AVG(speed_mph), 1) as avg_speed,
    ROUND(MAX(speed_mph), 1) as max_speed,
    ROUND(AVG(rpm), 0) as avg_rpm,
    COUNT(*) as samples
FROM nascar_telemetry.engine_telemetry
WHERE time >= now() - INTERVAL '30 seconds'
GROUP BY team
ORDER BY avg_speed DESC;
```

### 8. Brake Usage Analysis (Heavy Braking Events)

```sql
SELECT
    car_number,
    driver,
    COUNT(*) as heavy_brake_events,
    ROUND(AVG(brake_pressure_psi), 0) as avg_brake_pressure,
    MAX(brake_pressure_psi) as max_brake_pressure,
    ROUND(AVG(ABS(steering_angle_deg)), 1) as avg_steering_angle
FROM nascar_telemetry.chassis_telemetry
WHERE time >= now() - INTERVAL '1 minute'
  AND brake_pressure_psi > 500
GROUP BY car_number, driver
ORDER BY heavy_brake_events DESC
LIMIT 10;
```

## Grafana Dashboard Ideas

### 1. Race Overview
- Real-time leaderboard
- Current lap leaders
- Manufacturer standings
- Average speeds by team

### 2. Car Performance Detail
- Speed, RPM, throttle over time
- G-force visualization
- Tire temperature heat map
- Fuel level gauge

### 3. Team Strategy View
- All team cars comparison
- Lap time trends
- Tire degradation tracking
- Fuel consumption rates

### 4. Technical Analysis
- Cornering G-forces
- Brake usage patterns
- Engine performance metrics
- Tire pressure vs temperature correlation

## Performance Benchmarking

Use this demo to benchmark Arc's performance at different load levels:

```bash
# Baseline NASCAR load (612k msg/sec)
python3 nascar_telemetry.py --token $ARC_TOKEN --rate 612000 --duration 300 --workers 300

# 2x NASCAR load (1.2M msg/sec)
python3 nascar_telemetry.py --token $ARC_TOKEN --rate 1200000 --duration 300 --workers 400

# 4x NASCAR load (2.4M msg/sec - Arc's rated capacity)
python3 nascar_telemetry.py --token $ARC_TOKEN --rate 2400000 --duration 300 --workers 500

# Stress test beyond rated capacity (3M msg/sec)
python3 nascar_telemetry.py --token $ARC_TOKEN --rate 3000000 --duration 60 --workers 600
```

Monitor Arc's performance metrics during the test:
- Message ingestion rate (actual vs. target)
- Write latency (P50, P95, P99)
- Query response time
- Memory usage
- Disk I/O
- CPU utilization

## Use Case: Blog Post Demo

This scenario is perfect for demonstrating Arc in a blog post about real-time telemetry:

1. **Start the telemetry stream** - Shows sustained high-volume ingestion
2. **Build Grafana dashboards** - Demonstrates real-time query performance
3. **Show race insights** - Highlights analytical capabilities
4. **Compare Arc vs alternatives** - Benchmark against InfluxDB, TimescaleDB, etc.

### Key Talking Points
- Arc handles NASCAR-scale telemetry (612k msg/sec, 92 MB/s) on commodity hardware
- Sub-second query response times on live data
- Simple deployment (single binary, no complex clustering)
- SQL compatibility makes it easy for teams to build dashboards

## Reference

Based on: [AWS Blog - Accelerating Motorsports: How NASCAR delivers real-time racing data](https://aws.amazon.com/blogs/media/accelerating-motorsports-how-nascar-delivers-real-time-racing-data-to-broadcasters-racing-teams-and-fans/)
