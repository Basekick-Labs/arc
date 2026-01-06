// MQTT Test Producer - sends test messages to an MQTT broker
//
// Usage:
//   go run scripts/mqtt/producer.go [flags]
//
// Examples:
//   go run scripts/mqtt/producer.go -broker tcp://localhost:1883 -topic sensors/temp -count 100
//   go run scripts/mqtt/producer.go -broker tcp://localhost:1883 -topic sensors/# -rate 1000 -duration 60s
//   go run scripts/mqtt/producer.go -format json -topic test/json
//   go run scripts/mqtt/producer.go -format msgpack -topic test/msgpack

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	broker   = flag.String("broker", "tcp://localhost:1883", "MQTT broker URL")
	clientID = flag.String("client", "arc-test-producer", "MQTT client ID")
	topic    = flag.String("topic", "sensors/temperature", "Topic to publish to")
	qos      = flag.Int("qos", 1, "QoS level (0, 1, or 2)")
	count    = flag.Int("count", 0, "Number of messages to send (0 = unlimited)")
	rate     = flag.Int("rate", 10, "Messages per second")
	duration = flag.Duration("duration", 0, "Duration to run (0 = until count or Ctrl+C)")
	format   = flag.String("format", "msgpack", "Message format: json or msgpack")
	batch    = flag.Int("batch", 1, "Number of records per message")
	username = flag.String("username", "", "MQTT username")
	password = flag.String("password", "", "MQTT password")
	verbose  = flag.Bool("verbose", false, "Verbose output")
)

type SensorReading struct {
	Timestamp   int64   `json:"timestamp" msgpack:"timestamp"`
	SensorID    string  `json:"sensor_id" msgpack:"sensor_id"`
	Temperature float64 `json:"temperature" msgpack:"temperature"`
	Humidity    float64 `json:"humidity" msgpack:"humidity"`
	Location    string  `json:"location" msgpack:"location"`
}

func main() {
	flag.Parse()

	fmt.Printf("MQTT Test Producer\n")
	fmt.Printf("==================\n")
	fmt.Printf("Broker:   %s\n", *broker)
	fmt.Printf("Topic:    %s\n", *topic)
	fmt.Printf("Format:   %s\n", *format)
	fmt.Printf("Rate:     %d msg/s\n", *rate)
	fmt.Printf("Batch:    %d records/msg\n", *batch)
	if *count > 0 {
		fmt.Printf("Count:    %d messages\n", *count)
	}
	if *duration > 0 {
		fmt.Printf("Duration: %s\n", *duration)
	}
	fmt.Println()

	// Create MQTT client options
	opts := pahomqtt.NewClientOptions()
	opts.AddBroker(*broker)
	opts.SetClientID(*clientID)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)
	opts.SetConnectTimeout(10 * time.Second)

	if *username != "" {
		opts.SetUsername(*username)
		opts.SetPassword(*password)
	}

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		fmt.Println("Connected to broker")
	})

	opts.SetConnectionLostHandler(func(c pahomqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)
	})

	// Connect
	client := pahomqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(10 * time.Second) {
		fmt.Fprintf(os.Stderr, "Connection timeout\n")
		os.Exit(1)
	}
	if err := token.Error(); err != nil {
		fmt.Fprintf(os.Stderr, "Connection error: %v\n", err)
		os.Exit(1)
	}

	defer client.Disconnect(1000)

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Stats
	var sent, failed int64
	startTime := time.Now()

	// Setup rate limiting
	interval := time.Second / time.Duration(*rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Setup duration timer if specified
	var durationTimer <-chan time.Time
	if *duration > 0 {
		durationTimer = time.After(*duration)
	}

	// Locations for test data
	locations := []string{"warehouse-a", "warehouse-b", "office-1", "office-2", "lab", "datacenter"}
	sensorIDs := []string{"temp-001", "temp-002", "temp-003", "hum-001", "hum-002", "combo-001"}

	fmt.Println("Sending messages... (Ctrl+C to stop)")

	messageNum := 0
	running := true

	for running {
		select {
		case <-sigCh:
			fmt.Println("\nReceived shutdown signal")
			running = false

		case <-durationTimer:
			fmt.Println("\nDuration reached")
			running = false

		case <-ticker.C:
			// Generate batch of records
			records := make([]SensorReading, *batch)
			for i := 0; i < *batch; i++ {
				records[i] = SensorReading{
					Timestamp:   time.Now().UnixMilli(),
					SensorID:    sensorIDs[rand.Intn(len(sensorIDs))],
					Temperature: 20.0 + rand.Float64()*15.0, // 20-35°C
					Humidity:    40.0 + rand.Float64()*40.0, // 40-80%
					Location:    locations[rand.Intn(len(locations))],
				}
			}

			// Encode message
			var payload []byte
			var err error

			if *batch == 1 {
				// Single record
				if *format == "json" {
					payload, err = json.Marshal(records[0])
				} else {
					payload, err = msgpack.Marshal(records[0])
				}
			} else {
				// Array of records
				if *format == "json" {
					payload, err = json.Marshal(records)
				} else {
					payload, err = msgpack.Marshal(records)
				}
			}

			if err != nil {
				fmt.Fprintf(os.Stderr, "Encode error: %v\n", err)
				atomic.AddInt64(&failed, 1)
				continue
			}

			// Publish
			token := client.Publish(*topic, byte(*qos), false, payload)
			if token.WaitTimeout(5 * time.Second) && token.Error() == nil {
				atomic.AddInt64(&sent, 1)
				if *verbose {
					fmt.Printf("Sent message %d (%d bytes)\n", messageNum+1, len(payload))
				}
			} else {
				atomic.AddInt64(&failed, 1)
				if *verbose {
					fmt.Printf("Failed to send message %d: %v\n", messageNum+1, token.Error())
				}
			}

			messageNum++

			// Check count limit
			if *count > 0 && messageNum >= *count {
				fmt.Println("\nMessage count reached")
				running = false
			}
		}
	}

	// Print stats
	elapsed := time.Since(startTime)
	totalSent := atomic.LoadInt64(&sent)
	totalFailed := atomic.LoadInt64(&failed)
	totalRecords := totalSent * int64(*batch)

	fmt.Printf("\n")
	fmt.Printf("Summary\n")
	fmt.Printf("=======\n")
	fmt.Printf("Duration:       %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Messages sent:  %d\n", totalSent)
	fmt.Printf("Messages failed: %d\n", totalFailed)
	fmt.Printf("Records sent:   %d\n", totalRecords)
	fmt.Printf("Throughput:     %.1f msg/s\n", float64(totalSent)/elapsed.Seconds())
	fmt.Printf("Throughput:     %.1f records/s\n", float64(totalRecords)/elapsed.Seconds())
}
