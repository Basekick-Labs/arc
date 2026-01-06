// MQTT Test Consumer - receives messages from an MQTT broker (for debugging)
//
// Usage:
//   go run scripts/mqtt/consumer.go [flags]
//
// Examples:
//   go run scripts/mqtt/consumer.go -broker tcp://localhost:1883 -topic sensors/#
//   go run scripts/mqtt/consumer.go -broker tcp://localhost:1883 -topic test/+ -verbose

package main

import (
	"encoding/json"
	"flag"
	"fmt"
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
	clientID = flag.String("client", "arc-test-consumer", "MQTT client ID")
	topic    = flag.String("topic", "sensors/#", "Topic to subscribe to (supports wildcards)")
	qos      = flag.Int("qos", 1, "QoS level (0, 1, or 2)")
	username = flag.String("username", "", "MQTT username")
	password = flag.String("password", "", "MQTT password")
	verbose  = flag.Bool("verbose", false, "Show message contents")
	stats    = flag.Duration("stats", 5*time.Second, "Stats reporting interval")
)

var (
	messagesReceived int64
	bytesReceived    int64
	lastCount        int64
	lastBytes        int64
	startTime        time.Time
)

func main() {
	flag.Parse()

	fmt.Printf("MQTT Test Consumer\n")
	fmt.Printf("==================\n")
	fmt.Printf("Broker:  %s\n", *broker)
	fmt.Printf("Topic:   %s\n", *topic)
	fmt.Printf("QoS:     %d\n", *qos)
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
		// Subscribe on connect (and reconnect)
		token := c.Subscribe(*topic, byte(*qos), messageHandler)
		if token.WaitTimeout(10 * time.Second) && token.Error() == nil {
			fmt.Printf("Subscribed to: %s\n", *topic)
		} else {
			fmt.Printf("Subscribe error: %v\n", token.Error())
		}
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

	startTime = time.Now()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Stats ticker
	statsTicker := time.NewTicker(*stats)
	defer statsTicker.Stop()

	fmt.Println("\nWaiting for messages... (Ctrl+C to stop)")

	for {
		select {
		case <-sigCh:
			fmt.Println("\nReceived shutdown signal")
			printFinalStats()
			return

		case <-statsTicker.C:
			printStats()
		}
	}
}

func messageHandler(client pahomqtt.Client, msg pahomqtt.Message) {
	atomic.AddInt64(&messagesReceived, 1)
	atomic.AddInt64(&bytesReceived, int64(len(msg.Payload())))

	if *verbose {
		fmt.Printf("\n--- Message on %s ---\n", msg.Topic())
		fmt.Printf("Size: %d bytes\n", len(msg.Payload()))

		// Try to decode and pretty print
		var data interface{}

		// Try msgpack first
		if err := msgpack.Unmarshal(msg.Payload(), &data); err == nil {
			pretty, _ := json.MarshalIndent(data, "", "  ")
			fmt.Printf("Format: MessagePack\n")
			fmt.Printf("Content:\n%s\n", string(pretty))
			return
		}

		// Try JSON
		if err := json.Unmarshal(msg.Payload(), &data); err == nil {
			pretty, _ := json.MarshalIndent(data, "", "  ")
			fmt.Printf("Format: JSON\n")
			fmt.Printf("Content:\n%s\n", string(pretty))
			return
		}

		// Raw bytes
		fmt.Printf("Format: Binary/Unknown\n")
		if len(msg.Payload()) <= 100 {
			fmt.Printf("Content: %x\n", msg.Payload())
		} else {
			fmt.Printf("Content: %x... (truncated)\n", msg.Payload()[:100])
		}
	}
}

func printStats() {
	count := atomic.LoadInt64(&messagesReceived)
	bytes := atomic.LoadInt64(&bytesReceived)

	deltaCount := count - lastCount
	deltaBytes := bytes - lastBytes

	rate := float64(deltaCount) / stats.Seconds()
	bytesRate := float64(deltaBytes) / stats.Seconds()

	fmt.Printf("[%s] Messages: %d (+%d) | Rate: %.1f msg/s | %.1f KB/s\n",
		time.Now().Format("15:04:05"),
		count,
		deltaCount,
		rate,
		bytesRate/1024)

	lastCount = count
	lastBytes = bytes
}

func printFinalStats() {
	elapsed := time.Since(startTime)
	count := atomic.LoadInt64(&messagesReceived)
	bytes := atomic.LoadInt64(&bytesReceived)

	fmt.Printf("\n")
	fmt.Printf("Summary\n")
	fmt.Printf("=======\n")
	fmt.Printf("Duration:   %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Messages:   %d\n", count)
	fmt.Printf("Bytes:      %d (%.2f MB)\n", bytes, float64(bytes)/(1024*1024))
	fmt.Printf("Throughput: %.1f msg/s\n", float64(count)/elapsed.Seconds())
	fmt.Printf("Throughput: %.1f KB/s\n", float64(bytes)/1024/elapsed.Seconds())
}
