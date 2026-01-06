package mqtt

import (
	"testing"
)

func TestSubscription_Validate(t *testing.T) {
	validSub := func() *Subscription {
		s := &Subscription{
			Name:     "test-sub",
			Broker:   "tcp://localhost:1883",
			ClientID: "test-client",
			Topics:   []string{"sensors/#"},
			QoS:      1,
			Database: "iot",
		}
		s.SetDefaults()
		return s
	}

	tests := []struct {
		name    string
		modify  func(*Subscription)
		wantErr bool
	}{
		{
			name:    "valid",
			modify:  func(s *Subscription) {},
			wantErr: false,
		},
		{
			name:    "empty_name",
			modify:  func(s *Subscription) { s.Name = "" },
			wantErr: true,
		},
		{
			name:    "name_too_long",
			modify:  func(s *Subscription) { s.Name = string(make([]byte, MaxNameLen+1)) },
			wantErr: true,
		},
		{
			name:    "empty_broker",
			modify:  func(s *Subscription) { s.Broker = "" },
			wantErr: true,
		},
		{
			name:    "invalid_broker_scheme",
			modify:  func(s *Subscription) { s.Broker = "http://localhost:1883" },
			wantErr: true,
		},
		{
			name:    "valid_ssl_broker",
			modify:  func(s *Subscription) { s.Broker = "ssl://localhost:8883" },
			wantErr: false,
		},
		{
			name:    "valid_ws_broker",
			modify:  func(s *Subscription) { s.Broker = "ws://localhost:9001" },
			wantErr: false,
		},
		{
			name:    "empty_client_id",
			modify:  func(s *Subscription) { s.ClientID = "" },
			wantErr: true,
		},
		{
			name:    "no_topics",
			modify:  func(s *Subscription) { s.Topics = nil },
			wantErr: true,
		},
		{
			name:    "empty_topic",
			modify:  func(s *Subscription) { s.Topics = []string{""} },
			wantErr: true,
		},
		{
			name:    "too_many_topics",
			modify:  func(s *Subscription) { s.Topics = make([]string, MaxTopics+1); for i := range s.Topics { s.Topics[i] = "topic" } },
			wantErr: true,
		},
		{
			name:    "invalid_qos_negative",
			modify:  func(s *Subscription) { s.QoS = -1 },
			wantErr: true,
		},
		{
			name:    "invalid_qos_high",
			modify:  func(s *Subscription) { s.QoS = 3 },
			wantErr: true,
		},
		{
			name:    "valid_qos_0",
			modify:  func(s *Subscription) { s.QoS = 0 },
			wantErr: false,
		},
		{
			name:    "valid_qos_2",
			modify:  func(s *Subscription) { s.QoS = 2 },
			wantErr: false,
		},
		{
			name:    "empty_database",
			modify:  func(s *Subscription) { s.Database = "" },
			wantErr: true,
		},
		{
			name:    "path_traversal_cert",
			modify:  func(s *Subscription) { s.TLSCertPath = "../etc/passwd" },
			wantErr: true,
		},
		{
			name:    "path_traversal_key",
			modify:  func(s *Subscription) { s.TLSKeyPath = "/foo/../bar" },
			wantErr: true,
		},
		{
			name:    "path_traversal_ca",
			modify:  func(s *Subscription) { s.TLSCAPath = "..\\windows\\system32" },
			wantErr: true,
		},
		{
			name:    "valid_tls_paths",
			modify:  func(s *Subscription) { s.TLSCertPath = "/etc/certs/client.crt"; s.TLSKeyPath = "/etc/certs/client.key" },
			wantErr: false,
		},
		{
			name:    "negative_keep_alive",
			modify:  func(s *Subscription) { s.KeepAliveSeconds = -1 },
			wantErr: true,
		},
		{
			name:    "reconnect_min_exceeds_max",
			modify:  func(s *Subscription) { s.ReconnectMinSeconds = 60; s.ReconnectMaxSeconds = 30 },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := validSub()
			tt.modify(sub)

			err := sub.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSubscription_SetDefaults(t *testing.T) {
	sub := &Subscription{}
	sub.SetDefaults()

	if sub.QoS != 1 {
		t.Errorf("Default QoS = %d, want 1", sub.QoS)
	}
	if sub.KeepAliveSeconds != 60 {
		t.Errorf("Default KeepAliveSeconds = %d, want 60", sub.KeepAliveSeconds)
	}
	if sub.ConnectTimeoutSeconds != 30 {
		t.Errorf("Default ConnectTimeoutSeconds = %d, want 30", sub.ConnectTimeoutSeconds)
	}
	if sub.ReconnectMinSeconds != 1 {
		t.Errorf("Default ReconnectMinSeconds = %d, want 1", sub.ReconnectMinSeconds)
	}
	if sub.ReconnectMaxSeconds != 60 {
		t.Errorf("Default ReconnectMaxSeconds = %d, want 60", sub.ReconnectMaxSeconds)
	}
	if sub.Status != StatusStopped {
		t.Errorf("Default Status = %s, want %s", sub.Status, StatusStopped)
	}
}

func TestValidateBrokerURL(t *testing.T) {
	tests := []struct {
		url     string
		wantErr bool
	}{
		{"tcp://localhost:1883", false},
		{"ssl://broker.example.com:8883", false},
		{"ws://localhost:9001/mqtt", false},
		{"wss://broker.example.com/mqtt", false},
		{"mqtt://localhost:1883", false},
		{"mqtts://localhost:8883", false},
		{"http://localhost:1883", true},
		{"https://localhost:8883", true},
		{"localhost:1883", true},
		{"tcp://", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			err := validateBrokerURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateBrokerURL(%q) error = %v, wantErr %v", tt.url, err, tt.wantErr)
			}
		})
	}
}
