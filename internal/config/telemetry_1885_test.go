package config

import (
	"strings"
	"testing"
)

// #1885: telemetry.instance_id must be a UUID, stored canonical; checked only
// with telemetry on. telemetry.data_dir defaults to ./data, also when empty.
func TestLoad_TelemetryInstanceID1885(t *testing.T) {
	for _, tc := range []struct {
		name, enabled, id, dir string
		wantID, wantDir        string
		wantErr                bool
	}{
		{name: "defaults", enabled: "true", wantDir: "./data"},
		{name: "uuid upper case normalised", enabled: "true", id: "4F8A2C1E-9B3D-4E5F-8A7B-1C2D3E4F5A6B", wantID: "4f8a2c1e-9b3d-4e5f-8a7b-1c2d3e4f5a6b", wantDir: "./data"},
		{name: "braced uuid normalised", enabled: "true", id: "{4f8a2c1e-9b3d-4e5f-8a7b-1c2d3e4f5a6b}", wantID: "4f8a2c1e-9b3d-4e5f-8a7b-1c2d3e4f5a6b", wantDir: "./data"},
		{name: "free-form refused", enabled: "true", id: "acme-prod-eu1", wantErr: true},
		{name: "nil uuid refused", enabled: "true", id: "00000000-0000-0000-0000-000000000000", wantErr: true},
		{name: "textbook example refused", enabled: "true", id: "550e8400-e29b-41d4-a716-446655440000", wantErr: true},
		{name: "telemetry off: not checked", enabled: "false", id: "acme-prod-eu1", wantID: "acme-prod-eu1", wantDir: "./data"},
		{name: "data_dir set", enabled: "true", dir: "/var/lib/arc", wantDir: "/var/lib/arc"},
		{name: "data_dir empty falls back", enabled: "true", dir: " ", wantDir: "./data"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_TELEMETRY_ENABLED", tc.enabled)
			t.Setenv("ARC_TELEMETRY_INSTANCE_ID", tc.id)
			if tc.dir != "" {
				t.Setenv("ARC_TELEMETRY_DATA_DIR", tc.dir)
			}
			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "UUID") {
					t.Fatalf("Load() error = %v, want a UUID error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Telemetry.InstanceID != tc.wantID || cfg.Telemetry.DataDir != tc.wantDir {
				t.Fatalf("instance_id %q data_dir %q; want %q %q", cfg.Telemetry.InstanceID, cfg.Telemetry.DataDir, tc.wantID, tc.wantDir)
			}
		})
	}
}
