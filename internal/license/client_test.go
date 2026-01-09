package license

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestNewClient(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name      string
		cfg       *ClientConfig
		wantErr   bool
		errSubstr string
	}{
		{
			name: "valid config",
			cfg: &ClientConfig{
				LicenseKey: "ARC-ENT-1234-5678-90AB-CDEF",
				Logger:     logger,
			},
			wantErr: false,
		},
		{
			name: "missing license key",
			cfg: &ClientConfig{
				Logger: logger,
			},
			wantErr:   true,
			errSubstr: "license key is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if client == nil {
				t.Error("expected client, got nil")
			}
			if client.fingerprint == "" {
				t.Error("fingerprint should be set")
			}
		})
	}
}

func TestClient_GetFingerprint(t *testing.T) {
	client, err := NewClient(&ClientConfig{
		LicenseKey: "ARC-ENT-1234-5678-90AB-CDEF",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	fp := client.GetFingerprint()
	if fp == "" {
		t.Error("fingerprint should not be empty")
	}
	if !ValidateFingerprint(fp) {
		t.Error("fingerprint should be valid")
	}
}

func TestClient_GetLicense_BeforeVerify(t *testing.T) {
	client, err := NewClient(&ClientConfig{
		LicenseKey: "ARC-ENT-1234-5678-90AB-CDEF",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	license := client.GetLicense()
	if license != nil {
		t.Error("license should be nil before verify")
	}

	if client.IsValid() {
		t.Error("client should not be valid before verify")
	}
	if client.IsEnterprise() {
		t.Error("client should not be enterprise before verify")
	}
}

// Note: Tests that require a mock HTTP server (TestClient_Verify_*) have been removed
// because the license server URL is hardcoded to enterprise.basekick.net.
// Integration tests against the real server should be run separately.
