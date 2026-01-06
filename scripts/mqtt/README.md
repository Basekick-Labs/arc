# MQTT Test Scripts

Test utilities for MQTT ingestion development and debugging.

## Prerequisites

Start an MQTT broker (e.g., Mosquitto):

```bash
# macOS
brew install mosquitto
brew services start mosquitto

# Docker
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto
```

## Producer

Sends test sensor data to an MQTT broker.

```bash
# Basic usage - 10 msg/s to sensors/temperature
go run scripts/mqtt/producer.go

# High throughput test - 1000 msg/s for 60 seconds
go run scripts/mqtt/producer.go -rate 1000 -duration 60s

# Send 1000 messages then stop
go run scripts/mqtt/producer.go -count 1000

# JSON format instead of MessagePack
go run scripts/mqtt/producer.go -format json

# Batch multiple records per message
go run scripts/mqtt/producer.go -batch 100 -rate 100

# Custom broker and topic
go run scripts/mqtt/producer.go -broker tcp://broker.example.com:1883 -topic iot/sensors

# With authentication
go run scripts/mqtt/producer.go -username user -password secret
```

### Producer Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-broker` | `tcp://localhost:1883` | MQTT broker URL |
| `-client` | `arc-test-producer` | Client ID |
| `-topic` | `sensors/temperature` | Topic to publish to |
| `-qos` | `1` | QoS level (0, 1, 2) |
| `-count` | `0` | Messages to send (0 = unlimited) |
| `-rate` | `10` | Messages per second |
| `-duration` | `0` | Run duration (0 = until count/Ctrl+C) |
| `-format` | `msgpack` | Format: `json` or `msgpack` |
| `-batch` | `1` | Records per message |
| `-username` | | MQTT username |
| `-password` | | MQTT password |
| `-verbose` | `false` | Show each message sent |

## Consumer

Receives and displays MQTT messages (for debugging).

```bash
# Subscribe to all sensor topics
go run scripts/mqtt/consumer.go -topic "sensors/#"

# Show message contents
go run scripts/mqtt/consumer.go -topic "sensors/#" -verbose

# Custom stats interval
go run scripts/mqtt/consumer.go -stats 10s
```

### Consumer Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-broker` | `tcp://localhost:1883` | MQTT broker URL |
| `-client` | `arc-test-consumer` | Client ID |
| `-topic` | `sensors/#` | Topic to subscribe (wildcards OK) |
| `-qos` | `1` | QoS level (0, 1, 2) |
| `-username` | | MQTT username |
| `-password` | | MQTT password |
| `-verbose` | `false` | Show message contents |
| `-stats` | `5s` | Stats reporting interval |

## Test Scenarios

### Basic Integration Test

1. Start Arc with MQTT enabled
2. Create a subscription via API:
   ```bash
   curl -X POST http://localhost:8080/api/v1/mqtt/subscriptions \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer $TOKEN" \
     -d '{
       "name": "test-sensors",
       "broker": "tcp://localhost:1883",
       "client_id": "arc-test",
       "topics": ["sensors/#"],
       "database": "mqtt_test"
     }'
   ```
3. Start the subscription:
   ```bash
   curl -X POST http://localhost:8080/api/v1/mqtt/subscriptions/{id}/start \
     -H "Authorization: Bearer $TOKEN"
   ```
4. Run the producer:
   ```bash
   go run scripts/mqtt/producer.go -count 100
   ```
5. Query the data:
   ```bash
   curl "http://localhost:8080/api/v1/query?db=mqtt_test&q=SELECT%20*%20FROM%20mqtt_test%20LIMIT%2010"
   ```

### Throughput Test

```bash
# Terminal 1: Monitor Arc stats
watch -n1 'curl -s http://localhost:8080/api/v1/mqtt/stats | jq'

# Terminal 2: High-rate producer
go run scripts/mqtt/producer.go -rate 10000 -batch 100 -duration 60s
```

### Format Compatibility Test

```bash
# Test JSON
go run scripts/mqtt/producer.go -format json -count 10 -verbose

# Test MessagePack
go run scripts/mqtt/producer.go -format msgpack -count 10 -verbose
```
