package config

import "testing"

// server.max_payload_size = 0 cannot work (the HTTP server reads it as its
// own 4MB default, MessagePack refuses every body), but a node configured so
// keeps starting: the default is used and a warning says so (#1383).
func TestLoad_MaxPayloadSizeZeroUsesTheDefault1383(t *testing.T) {
	def, err := ParseSize(DefaultMaxPayloadSize)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value    string
		want     int64
		wantWarn bool
	}{
		{"0B", def, true},
		{"0", def, true},
		{"10MB", 10 << 20, false},
		{"", def, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.value != "" {
				t.Setenv("ARC_SERVER_MAX_PAYLOAD_SIZE", tc.value)
			}
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() = %v", err)
			}
			if cfg.Server.MaxPayloadSize != tc.want {
				t.Fatalf("MaxPayloadSize = %d, want %d", cfg.Server.MaxPayloadSize, tc.want)
			}
			warned := false
			for _, w := range cfg.Warnings {
				if w.Key == "server.max_payload_size" {
					warned = true
					if w.Value != tc.value {
						t.Fatalf("warning value %q, want %q", w.Value, tc.value)
					}
				}
			}
			if warned != tc.wantWarn {
				t.Fatalf("warned = %v, want %v (%+v)", warned, tc.wantWarn, cfg.Warnings)
			}
		})
	}
}
