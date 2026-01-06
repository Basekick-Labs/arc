package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/pkg/models"
)

// Subscriber handles MQTT connection and message processing for a single subscription
type Subscriber struct {
	id           string
	config       *Subscription
	client       pahomqtt.Client
	arrowBuffer  *ingest.ArrowBuffer
	logger       zerolog.Logger
	encryptor    PasswordEncryptor
	onStatusChange func(id string, status SubscriptionStatus, errMsg string)

	// Runtime state
	mu             sync.RWMutex
	running        bool
	connectedSince time.Time
	lastMessageAt  time.Time

	// Statistics
	messagesReceived atomic.Int64
	messagesFailed   atomic.Int64
	bytesReceived    atomic.Int64
	reconnects       atomic.Int64

	// Shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSubscriber creates a new MQTT subscriber
func NewSubscriber(
	config *Subscription,
	arrowBuffer *ingest.ArrowBuffer,
	encryptor PasswordEncryptor,
	onStatusChange func(id string, status SubscriptionStatus, errMsg string),
	logger zerolog.Logger,
) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())

	return &Subscriber{
		id:             config.ID,
		config:         config,
		arrowBuffer:    arrowBuffer,
		encryptor:      encryptor,
		onStatusChange: onStatusChange,
		logger:         logger.With().Str("subscription_id", config.ID).Str("subscription_name", config.Name).Logger(),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start connects to the MQTT broker and begins message processing
func (s *Subscriber) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("subscriber already running")
	}
	s.mu.Unlock()

	// Build MQTT client options
	opts, err := s.buildClientOptions()
	if err != nil {
		return fmt.Errorf("failed to build client options: %w", err)
	}

	// Create client
	s.client = pahomqtt.NewClient(opts)

	// Connect
	s.logger.Info().Str("broker", s.config.Broker).Msg("Connecting to MQTT broker")

	token := s.client.Connect()
	if !token.WaitTimeout(time.Duration(s.config.ConnectTimeoutSeconds) * time.Second) {
		return fmt.Errorf("connection timeout after %d seconds", s.config.ConnectTimeoutSeconds)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}

	s.mu.Lock()
	s.running = true
	s.connectedSince = time.Now()
	s.mu.Unlock()

	s.logger.Info().Msg("Connected to MQTT broker")
	s.updateStatus(StatusRunning, "")

	return nil
}

// Stop disconnects from the MQTT broker
func (s *Subscriber) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.cancel()

	if s.client != nil && s.client.IsConnected() {
		// Unsubscribe from topics
		for _, topic := range s.config.Topics {
			s.client.Unsubscribe(topic)
		}

		// Disconnect with 1 second timeout
		s.client.Disconnect(1000)
	}

	s.logger.Info().Msg("Disconnected from MQTT broker")
	s.updateStatus(StatusStopped, "")

	return nil
}

// IsRunning returns whether the subscriber is running
func (s *Subscriber) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// GetStats returns current statistics
func (s *Subscriber) GetStats() *SubscriptionStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := "stopped"
	if s.running {
		status = "running"
	}

	return &SubscriptionStats{
		ID:               s.id,
		Name:             s.config.Name,
		Status:           status,
		MessagesReceived: s.messagesReceived.Load(),
		MessagesFailed:   s.messagesFailed.Load(),
		BytesReceived:    s.bytesReceived.Load(),
		LastMessageAt:    s.lastMessageAt,
		ConnectedSince:   s.connectedSince,
		Reconnects:       s.reconnects.Load(),
	}
}

// buildClientOptions creates MQTT client options from subscription config
func (s *Subscriber) buildClientOptions() (*pahomqtt.ClientOptions, error) {
	opts := pahomqtt.NewClientOptions()

	// Broker URL
	opts.AddBroker(s.config.Broker)
	opts.SetClientID(s.config.ClientID)

	// Keep alive
	opts.SetKeepAlive(time.Duration(s.config.KeepAliveSeconds) * time.Second)
	opts.SetConnectTimeout(time.Duration(s.config.ConnectTimeoutSeconds) * time.Second)

	// Auto reconnect
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(time.Duration(s.config.ReconnectMaxSeconds) * time.Second)

	// Authentication
	if s.config.Username != "" {
		opts.SetUsername(s.config.Username)
	}
	if s.config.PasswordEncrypted != "" {
		password, err := s.encryptor.Decrypt(s.config.PasswordEncrypted)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt password: %w", err)
		}
		opts.SetPassword(password)
	}

	// TLS
	if s.config.TLSEnabled {
		tlsConfig, err := s.buildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
	}

	// Connection handlers
	opts.SetOnConnectHandler(s.onConnect)
	opts.SetConnectionLostHandler(s.onConnectionLost)
	opts.SetReconnectingHandler(s.onReconnecting)

	// Clean session
	opts.SetCleanSession(true)

	return opts, nil
}

// buildTLSConfig creates TLS configuration
func (s *Subscriber) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: s.config.TLSInsecureSkipVerify,
	}

	// Load CA certificate if provided
	if s.config.TLSCAPath != "" {
		caCert, err := os.ReadFile(s.config.TLSCAPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate and key if provided
	if s.config.TLSCertPath != "" && s.config.TLSKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(s.config.TLSCertPath, s.config.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// onConnect is called when connection is established
func (s *Subscriber) onConnect(client pahomqtt.Client) {
	s.logger.Info().Msg("MQTT connection established, subscribing to topics")

	// Subscribe to all topics
	for _, topic := range s.config.Topics {
		token := client.Subscribe(topic, byte(s.config.QoS), s.onMessage)
		token.Wait()
		if err := token.Error(); err != nil {
			s.logger.Error().Err(err).Str("topic", topic).Msg("Failed to subscribe to topic")
			continue
		}
		s.logger.Info().Str("topic", topic).Int("qos", s.config.QoS).Msg("Subscribed to topic")
	}

	s.mu.Lock()
	s.connectedSince = time.Now()
	s.mu.Unlock()

	// Update metrics
	metrics.Get().SetMQTTConnected(true)
}

// onConnectionLost is called when connection is lost
func (s *Subscriber) onConnectionLost(client pahomqtt.Client, err error) {
	s.logger.Warn().Err(err).Msg("MQTT connection lost")
	metrics.Get().SetMQTTConnected(false)
}

// onReconnecting is called before reconnection attempt
func (s *Subscriber) onReconnecting(client pahomqtt.Client, opts *pahomqtt.ClientOptions) {
	s.reconnects.Add(1)
	metrics.Get().IncMQTTReconnects()
	s.logger.Info().Int64("reconnect_count", s.reconnects.Load()).Msg("Attempting to reconnect to MQTT broker")
}

// onMessage handles incoming MQTT messages
func (s *Subscriber) onMessage(client pahomqtt.Client, msg pahomqtt.Message) {
	s.messagesReceived.Add(1)
	s.bytesReceived.Add(int64(len(msg.Payload())))

	s.mu.Lock()
	s.lastMessageAt = time.Now()
	s.mu.Unlock()

	// Update metrics
	metrics.Get().IncMQTTMessagesReceived()
	metrics.Get().IncMQTTBytesReceived(int64(len(msg.Payload())))

	// Determine database from topic mapping or use default
	database := s.config.Database
	if s.config.TopicMapping != nil {
		if mapped, ok := s.config.TopicMapping[msg.Topic()]; ok {
			database = mapped
		}
	}

	// Decode and write message
	if err := s.processMessage(msg.Topic(), msg.Payload(), database); err != nil {
		s.messagesFailed.Add(1)
		metrics.Get().IncMQTTMessagesFailed()
		s.logger.Error().
			Err(err).
			Str("topic", msg.Topic()).
			Int("payload_size", len(msg.Payload())).
			Msg("Failed to process MQTT message")
	} else {
		metrics.Get().IncMQTTDecodeSuccess()
	}
}

// processMessage decodes and writes an MQTT message to the ArrowBuffer
func (s *Subscriber) processMessage(topic string, payload []byte, database string) error {
	// Try to decode as MessagePack first (check magic bytes)
	if len(payload) > 0 {
		records, err := s.decodePayload(topic, payload)
		if err != nil {
			return fmt.Errorf("failed to decode payload: %w", err)
		}

		// Write to ArrowBuffer
		if err := s.arrowBuffer.Write(s.ctx, database, records); err != nil {
			return fmt.Errorf("failed to write to buffer: %w", err)
		}
	}

	return nil
}

// decodePayload attempts to decode the message payload
func (s *Subscriber) decodePayload(topic string, payload []byte) ([]interface{}, error) {
	// Try MessagePack first (more efficient for IoT)
	if records, err := s.tryDecodeMsgPack(payload); err == nil {
		return records, nil
	}

	// Fall back to JSON
	if records, err := s.tryDecodeJSON(topic, payload); err == nil {
		return records, nil
	}

	return nil, fmt.Errorf("failed to decode payload as MessagePack or JSON")
}

// tryDecodeMsgPack attempts to decode payload as MessagePack
func (s *Subscriber) tryDecodeMsgPack(payload []byte) ([]interface{}, error) {
	var data interface{}
	if err := msgpack.Unmarshal(payload, &data); err != nil {
		return nil, err
	}

	return s.convertToRecords(data)
}

// tryDecodeJSON attempts to decode payload as JSON
func (s *Subscriber) tryDecodeJSON(_ string, payload []byte) ([]interface{}, error) {
	var data interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, err
	}

	return s.convertToRecords(data)
}

// convertToRecords converts decoded data to records for ArrowBuffer
func (s *Subscriber) convertToRecords(data interface{}) ([]interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Single record
		record, err := s.mapToRecord(v)
		if err != nil {
			return nil, err
		}
		return []interface{}{record}, nil

	case []interface{}:
		// Array of records
		records := make([]interface{}, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				record, err := s.mapToRecord(m)
				if err != nil {
					return nil, err
				}
				records = append(records, record)
			}
		}
		return records, nil

	default:
		return nil, fmt.Errorf("unexpected data type: %T", data)
	}
}

// mapToRecord converts a map to a models.Record
func (s *Subscriber) mapToRecord(m map[string]interface{}) (*models.Record, error) {
	record := &models.Record{
		Fields: make(map[string]interface{}),
		Tags:   make(map[string]string),
	}

	// Extract measurement
	if meas, ok := m["m"].(string); ok {
		record.Measurement = meas
	} else if meas, ok := m["measurement"].(string); ok {
		record.Measurement = meas
	} else {
		record.Measurement = "mqtt" // Default measurement
	}

	// Extract timestamp - Arc uses microseconds internally
	record.Timestamp = s.extractTimestamp(m)

	// Extract tags
	if tags, ok := m["tags"].(map[string]interface{}); ok {
		for k, v := range tags {
			if str, ok := v.(string); ok {
				record.Tags[k] = str
			}
		}
	}

	// Extract fields (everything else)
	if fields, ok := m["fields"].(map[string]interface{}); ok {
		record.Fields = fields
	} else {
		// Treat remaining keys as fields
		for k, v := range m {
			if k != "m" && k != "measurement" && k != "t" && k != "time" && k != "timestamp" && k != "tags" && k != "fields" {
				record.Fields[k] = v
			}
		}
	}

	return record, nil
}

// extractTimestamp extracts and normalizes timestamp to microseconds
// Handles various numeric types from MessagePack/JSON decoding
// Detects milliseconds vs microseconds vs nanoseconds based on magnitude
func (s *Subscriber) extractTimestamp(m map[string]interface{}) int64 {
	var ts int64

	// Try common timestamp field names
	for _, key := range []string{"t", "time", "timestamp"} {
		if v, ok := m[key]; ok {
			ts = toInt64(v)
			if ts > 0 {
				break
			}
		}
	}

	if ts == 0 {
		// No timestamp found, use current time
		return time.Now().UnixMicro()
	}

	// Normalize to microseconds based on magnitude
	// Seconds:      ~1.7e9  (10 digits, e.g., 1704499200)
	// Milliseconds: ~1.7e12 (13 digits, e.g., 1704499200000)
	// Microseconds: ~1.7e15 (16 digits, e.g., 1704499200000000)
	// Nanoseconds:  ~1.7e18 (19 digits, e.g., 1704499200000000000)

	switch {
	case ts > 1e18: // Nanoseconds
		return ts / 1000
	case ts > 1e15: // Microseconds (already correct)
		return ts
	case ts > 1e12: // Milliseconds
		return ts * 1000
	default: // Seconds
		return ts * 1_000_000
	}
}

// toInt64 converts various numeric types to int64
func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case uint64:
		return int64(n)
	case uint32:
		return int64(n)
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	default:
		return 0
	}
}

// updateStatus updates the subscription status via callback
func (s *Subscriber) updateStatus(status SubscriptionStatus, errMsg string) {
	if s.onStatusChange != nil {
		s.onStatusChange(s.id, status, errMsg)
	}
}

// ID returns the subscriber ID
func (s *Subscriber) ID() string {
	return s.id
}

// Config returns the subscription configuration
func (s *Subscriber) Config() *Subscription {
	return s.config
}
