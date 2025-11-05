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

### Basic Usage (Full Race - 40 Cars)

```bash
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --url http://localhost:8000 \
  --database nascar_telemetry
```

This will generate **612,000 messages/second** (15,300 per car × 40 cars).

### Custom Configuration

```bash
# Test with fewer cars (e.g., 10 cars = 153k msg/sec)
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --cars 10 \
  --rate 15300

# Run for a specific duration (5 minutes)
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --duration 300

# Increase message rate to test Arc's limits (2.4M msg/sec)
python3 nascar_telemetry.py \
  --token YOUR_ARC_TOKEN \
  --cars 40 \
  --rate 60000
```

### Command-Line Arguments

- `--url` - Arc server URL (default: `http://localhost:8000`)
- `--token` - Arc API token (required)
- `--database` - Database name (default: `nascar_telemetry`)
- `--cars` - Number of cars in the race (default: `40`)
- `--rate` - Messages per second per car (default: `15300`)
- `--duration` - Duration in seconds (default: continuous, use Ctrl+C to stop)

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
    throttle_position
FROM nascar_telemetry.engine_telemetry
WHERE car_number = 24
  AND time > now() - INTERVAL '5 minutes'
ORDER BY time DESC;
```

### 3. Manufacturer Performance Comparison

```sql
SELECT
    manufacturer,
    AVG(rpm) as avg_rpm,
    AVG(speed_mph) as avg_speed,
    AVG(throttle_position) as avg_throttle
FROM nascar_telemetry.engine_telemetry
WHERE time > now() - INTERVAL '1 minute'
GROUP BY manufacturer
ORDER BY avg_speed DESC;
```

### 4. Tire Degradation Analysis

```sql
SELECT
    car_number,
    lap,
    (lf_temp_f + rf_temp_f + lr_temp_f + rr_temp_f) / 4 as avg_tire_temp,
    (lf_pressure_psi + rf_pressure_psi + lr_pressure_psi + rr_pressure_psi) / 4 as avg_tire_pressure
FROM nascar_telemetry.tire_telemetry
WHERE car_number = 24
  AND time > now() - INTERVAL '10 minutes'
ORDER BY lap DESC
LIMIT 50;
```

### 5. G-Force Distribution (Cornering Performance)

```sql
SELECT
    car_number,
    driver,
    team,
    MAX(lateral_g) as max_lateral_g,
    AVG(ABS(lateral_g)) as avg_lateral_g,
    MAX(longitudinal_g) as max_longitudinal_g
FROM nascar_telemetry.chassis_telemetry
WHERE time > now() - INTERVAL '5 minutes'
GROUP BY car_number, driver, team
ORDER BY max_lateral_g DESC
LIMIT 10;
```

### 6. Fuel Strategy Monitor

```sql
SELECT
    car_number,
    driver,
    team,
    lap,
    AVG(fuel_level) as avg_fuel_level,
    MIN(fuel_level) as min_fuel_level
FROM nascar_telemetry.engine_telemetry
WHERE time > now() - INTERVAL '2 minutes'
GROUP BY car_number, driver, team, lap
HAVING avg_fuel_level < 5.0
ORDER BY avg_fuel_level ASC;
```

### 7. Team Comparison Dashboard

```sql
SELECT
    team,
    COUNT(DISTINCT car_number) as cars,
    AVG(speed_mph) as avg_speed,
    MAX(speed_mph) as max_speed,
    AVG(rpm) as avg_rpm
FROM nascar_telemetry.engine_telemetry
WHERE time > now() - INTERVAL '30 seconds'
GROUP BY team
ORDER BY avg_speed DESC;
```

### 8. Brake Usage Analysis

```sql
SELECT
    car_number,
    driver,
    COUNT(*) as brake_applications,
    AVG(brake_pressure_psi) as avg_brake_pressure,
    MAX(brake_pressure_psi) as max_brake_pressure
FROM nascar_telemetry.chassis_telemetry
WHERE time > now() - INTERVAL '1 minute'
  AND brake_pressure_psi > 100
GROUP BY car_number, driver
ORDER BY brake_applications DESC
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

Use this demo to benchmark Arc's performance:

```bash
# Baseline NASCAR load (612k msg/sec)
python3 nascar_telemetry.py --token $TOKEN --duration 300

# 2x NASCAR load (1.2M msg/sec)
python3 nascar_telemetry.py --token $TOKEN --cars 40 --rate 30600

# 4x NASCAR load (2.4M msg/sec - Arc's rated capacity)
python3 nascar_telemetry.py --token $TOKEN --cars 40 --rate 60000
```

Monitor Arc's performance metrics during the test:
- Message ingestion rate
- Write latency
- Query response time
- Memory usage
- Disk I/O

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
