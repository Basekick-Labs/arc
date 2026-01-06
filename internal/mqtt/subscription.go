package mqtt

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Validation limits
const (
	MaxTopics       = 100
	MaxTopicLength  = 1024
	MaxClientIDLen  = 255
	MaxBrokerURLLen = 2048
	MaxNameLen      = 255
)

// SubscriptionStatus represents the current state of a subscription
type SubscriptionStatus string

const (
	StatusStopped SubscriptionStatus = "stopped"
	StatusRunning SubscriptionStatus = "running"
	StatusError   SubscriptionStatus = "error"
	StatusPaused  SubscriptionStatus = "paused"
)

// Subscription represents an MQTT subscription configuration
type Subscription struct {
	ID                    string             `json:"id"`
	Name                  string             `json:"name"`
	Broker                string             `json:"broker"`
	ClientID              string             `json:"client_id"`
	Topics                []string           `json:"topics"`
	QoS                   int                `json:"qos"`
	Database              string             `json:"database"`
	Username              string             `json:"username,omitempty"`
	PasswordEncrypted     string             `json:"-"` // Never expose in JSON
	HasPassword           bool               `json:"has_password"`
	TLSEnabled            bool               `json:"tls_enabled"`
	TLSCertPath           string             `json:"tls_cert_path,omitempty"`
	TLSKeyPath            string             `json:"tls_key_path,omitempty"`
	TLSCAPath             string             `json:"tls_ca_path,omitempty"`
	TLSInsecureSkipVerify bool               `json:"tls_insecure_skip_verify"`
	AutoStart             bool               `json:"auto_start"`
	Status                SubscriptionStatus `json:"status"`
	ErrorMessage          string             `json:"error_message,omitempty"`
	TopicMapping          map[string]string  `json:"topic_mapping,omitempty"`
	KeepAliveSeconds      int                `json:"keep_alive_seconds"`
	ConnectTimeoutSeconds int                `json:"connect_timeout_seconds"`
	ReconnectMinSeconds   int                `json:"reconnect_min_seconds"`
	ReconnectMaxSeconds   int                `json:"reconnect_max_seconds"`
	CreatedAt             time.Time          `json:"created_at"`
	UpdatedAt             time.Time          `json:"updated_at"`
}

// CreateSubscriptionRequest is the request body for creating a subscription
type CreateSubscriptionRequest struct {
	Name                  string            `json:"name"`
	Broker                string            `json:"broker"`
	ClientID              string            `json:"client_id"`
	Topics                []string          `json:"topics"`
	QoS                   int               `json:"qos"`
	Database              string            `json:"database"`
	Username              string            `json:"username,omitempty"`
	Password              string            `json:"password,omitempty"`
	TLSEnabled            bool              `json:"tls_enabled"`
	TLSCertPath           string            `json:"tls_cert_path,omitempty"`
	TLSKeyPath            string            `json:"tls_key_path,omitempty"`
	TLSCAPath             string            `json:"tls_ca_path,omitempty"`
	TLSInsecureSkipVerify bool              `json:"tls_insecure_skip_verify"`
	AutoStart             bool              `json:"auto_start"`
	TopicMapping          map[string]string `json:"topic_mapping,omitempty"`
	KeepAliveSeconds      int               `json:"keep_alive_seconds"`
	ConnectTimeoutSeconds int               `json:"connect_timeout_seconds"`
	ReconnectMinSeconds   int               `json:"reconnect_min_seconds"`
	ReconnectMaxSeconds   int               `json:"reconnect_max_seconds"`
}

// UpdateSubscriptionRequest is the request body for updating a subscription
type UpdateSubscriptionRequest struct {
	Name                  *string            `json:"name,omitempty"`
	Broker                *string            `json:"broker,omitempty"`
	ClientID              *string            `json:"client_id,omitempty"`
	Topics                []string           `json:"topics,omitempty"`
	QoS                   *int               `json:"qos,omitempty"`
	Database              *string            `json:"database,omitempty"`
	Username              *string            `json:"username,omitempty"`
	Password              *string            `json:"password,omitempty"`
	TLSEnabled            *bool              `json:"tls_enabled,omitempty"`
	TLSCertPath           *string            `json:"tls_cert_path,omitempty"`
	TLSKeyPath            *string            `json:"tls_key_path,omitempty"`
	TLSCAPath             *string            `json:"tls_ca_path,omitempty"`
	TLSInsecureSkipVerify *bool              `json:"tls_insecure_skip_verify,omitempty"`
	AutoStart             *bool              `json:"auto_start,omitempty"`
	TopicMapping          *map[string]string `json:"topic_mapping,omitempty"`
	KeepAliveSeconds      *int               `json:"keep_alive_seconds,omitempty"`
	ConnectTimeoutSeconds *int               `json:"connect_timeout_seconds,omitempty"`
	ReconnectMinSeconds   *int               `json:"reconnect_min_seconds,omitempty"`
	ReconnectMaxSeconds   *int               `json:"reconnect_max_seconds,omitempty"`
}

// SubscriptionStats contains runtime statistics for a subscription
type SubscriptionStats struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Status           string    `json:"status"`
	MessagesReceived int64     `json:"messages_received"`
	MessagesFailed   int64     `json:"messages_failed"`
	BytesReceived    int64     `json:"bytes_received"`
	LastMessageAt    time.Time `json:"last_message_at,omitempty"`
	ConnectedSince   time.Time `json:"connected_since,omitempty"`
	Reconnects       int64     `json:"reconnects"`
}

// Validate validates the subscription configuration
func (s *Subscription) Validate() error {
	if s.Name == "" {
		return errors.New("name is required")
	}
	if len(s.Name) > MaxNameLen {
		return fmt.Errorf("name exceeds %d characters", MaxNameLen)
	}

	if s.Broker == "" {
		return errors.New("broker is required")
	}
	if len(s.Broker) > MaxBrokerURLLen {
		return fmt.Errorf("broker URL exceeds %d characters", MaxBrokerURLLen)
	}

	// Validate broker URL format
	if err := validateBrokerURL(s.Broker); err != nil {
		return fmt.Errorf("invalid broker URL: %w", err)
	}

	if s.ClientID == "" {
		return errors.New("client_id is required")
	}
	if len(s.ClientID) > MaxClientIDLen {
		return fmt.Errorf("client_id exceeds %d characters", MaxClientIDLen)
	}

	if len(s.Topics) == 0 {
		return errors.New("at least one topic is required")
	}
	if len(s.Topics) > MaxTopics {
		return fmt.Errorf("maximum %d topics allowed", MaxTopics)
	}

	for _, topic := range s.Topics {
		if topic == "" {
			return errors.New("empty topic not allowed")
		}
		if len(topic) > MaxTopicLength {
			return fmt.Errorf("topic pattern exceeds %d characters", MaxTopicLength)
		}
	}

	if s.QoS < 0 || s.QoS > 2 {
		return errors.New("qos must be 0, 1, or 2")
	}

	if s.Database == "" {
		return errors.New("database is required")
	}

	// Path traversal check for TLS certificate paths
	for _, path := range []string{s.TLSCertPath, s.TLSKeyPath, s.TLSCAPath} {
		if path != "" && strings.Contains(path, "..") {
			return errors.New("path traversal not allowed in certificate paths")
		}
	}

	// Validate timeouts
	if s.KeepAliveSeconds < 0 {
		return errors.New("keep_alive_seconds cannot be negative")
	}
	if s.ConnectTimeoutSeconds < 0 {
		return errors.New("connect_timeout_seconds cannot be negative")
	}
	if s.ReconnectMinSeconds < 0 {
		return errors.New("reconnect_min_seconds cannot be negative")
	}
	if s.ReconnectMaxSeconds < 0 {
		return errors.New("reconnect_max_seconds cannot be negative")
	}
	if s.ReconnectMinSeconds > s.ReconnectMaxSeconds && s.ReconnectMaxSeconds > 0 {
		return errors.New("reconnect_min_seconds cannot exceed reconnect_max_seconds")
	}

	return nil
}

// SetDefaults sets default values for optional fields
func (s *Subscription) SetDefaults() {
	if s.ClientID == "" {
		s.ClientID = generateClientID()
	}
	if s.QoS == 0 {
		s.QoS = 1 // Default to at-least-once
	}
	if s.KeepAliveSeconds == 0 {
		s.KeepAliveSeconds = 60
	}
	if s.ConnectTimeoutSeconds == 0 {
		s.ConnectTimeoutSeconds = 30
	}
	if s.ReconnectMinSeconds == 0 {
		s.ReconnectMinSeconds = 1
	}
	if s.ReconnectMaxSeconds == 0 {
		s.ReconnectMaxSeconds = 60
	}
	if s.Status == "" {
		s.Status = StatusStopped
	}
}

// generateClientID creates a unique client ID for MQTT connections
func generateClientID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return "arc-" + hex.EncodeToString(b)
}

// validateBrokerURL validates the MQTT broker URL format
func validateBrokerURL(brokerURL string) error {
	// MQTT URLs can be: tcp://, ssl://, ws://, wss://
	validSchemes := []string{"tcp://", "ssl://", "ws://", "wss://", "mqtt://", "mqtts://"}

	hasValidScheme := false
	for _, scheme := range validSchemes {
		if strings.HasPrefix(brokerURL, scheme) {
			hasValidScheme = true
			break
		}
	}

	if !hasValidScheme {
		return fmt.Errorf("must start with one of: %v", validSchemes)
	}

	// Parse as URL to validate structure
	parsed, err := url.Parse(brokerURL)
	if err != nil {
		return err
	}

	if parsed.Host == "" {
		return errors.New("host is required")
	}

	return nil
}

// ValidateCreateRequest validates a create subscription request
func ValidateCreateRequest(req *CreateSubscriptionRequest) error {
	s := &Subscription{
		Name:                  req.Name,
		Broker:                req.Broker,
		ClientID:              req.ClientID,
		Topics:                req.Topics,
		QoS:                   req.QoS,
		Database:              req.Database,
		TLSCertPath:           req.TLSCertPath,
		TLSKeyPath:            req.TLSKeyPath,
		TLSCAPath:             req.TLSCAPath,
		KeepAliveSeconds:      req.KeepAliveSeconds,
		ConnectTimeoutSeconds: req.ConnectTimeoutSeconds,
		ReconnectMinSeconds:   req.ReconnectMinSeconds,
		ReconnectMaxSeconds:   req.ReconnectMaxSeconds,
	}
	s.SetDefaults()
	return s.Validate()
}
