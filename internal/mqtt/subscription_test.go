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
			name: "too_many_topics",
			modify: func(s *Subscription) {
				s.Topics = make([]string, MaxTopics+1)
				for i := range s.Topics {
					s.Topics[i] = "topic"
				}
			},
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
			name:    "negative_reconnect_max",
			modify:  func(s *Subscription) { s.ReconnectMaxSeconds = -1 },
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

	// SetDefaults intentionally does NOT default QoS (#326) — an empty struct
	// keeps QoS at its zero value here. QoS defaulting happens earlier, when a
	// create request is mapped to a Subscription (see resolveQoS / the create
	// tests below), so an explicit "qos": 0 is never rewritten.
	if sub.QoS != 0 {
		t.Errorf("SetDefaults should not touch QoS; got %d, want 0 (unchanged)", sub.QoS)
	}
	if sub.KeepAliveSeconds != 60 {
		t.Errorf("Default KeepAliveSeconds = %d, want 60", sub.KeepAliveSeconds)
	}
	if sub.ConnectTimeoutSeconds != 30 {
		t.Errorf("Default ConnectTimeoutSeconds = %d, want 30", sub.ConnectTimeoutSeconds)
	}
	if sub.ReconnectMaxSeconds != 60 {
		t.Errorf("Default ReconnectMaxSeconds = %d, want 60", sub.ReconnectMaxSeconds)
	}
	if sub.Status != StatusStopped {
		t.Errorf("Default Status = %s, want %s", sub.Status, StatusStopped)
	}
}

// TestResolveQoS is the core #326 regression: an omitted QoS defaults to 1, but
// an explicit value — including 0 — is preserved.
func TestResolveQoS(t *testing.T) {
	zero, one, two := 0, 1, 2
	tests := []struct {
		name string
		in   *int
		want int
	}{
		{"omitted defaults to at-least-once", nil, defaultQoS},
		{"explicit 0 is preserved (the bug)", &zero, 0},
		{"explicit 1 is preserved", &one, 1},
		{"explicit 2 is preserved", &two, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveQoS(tt.in); got != tt.want {
				t.Errorf("resolveQoS(%v) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestValidateCreateRequest_QoSZeroPreserved verifies the full create-request
// mapping keeps an explicit QoS 0 (#326): the request maps to a Subscription
// whose QoS is 0, not silently rewritten to 1, and still validates.
func TestValidateCreateRequest_QoSZeroPreserved(t *testing.T) {
	base := func(qos *int) *CreateSubscriptionRequest {
		return &CreateSubscriptionRequest{
			Name:     "s",
			Broker:   "tcp://localhost:1883",
			ClientID: "c",
			Topics:   []string{"sensors/#"},
			QoS:      qos,
			Database: "iot",
		}
	}

	zero := 0
	// Explicit 0 must validate (it's a legal QoS).
	if err := ValidateCreateRequest(base(&zero)); err != nil {
		t.Fatalf("explicit QoS 0 should be valid: %v", err)
	}

	// And it must map to a Subscription with QoS 0, not 1.
	sub := &Subscription{
		Name:     "s",
		Broker:   "tcp://localhost:1883",
		ClientID: "c",
		Topics:   []string{"sensors/#"},
		QoS:      resolveQoS(base(&zero).QoS),
		Database: "iot",
	}
	sub.SetDefaults()
	if sub.QoS != 0 {
		t.Errorf("explicit QoS 0 was overwritten to %d (#326 regression)", sub.QoS)
	}

	// Omitted QoS defaults to 1.
	omitted := resolveQoS(base(nil).QoS)
	if omitted != 1 {
		t.Errorf("omitted QoS = %d, want default 1", omitted)
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
