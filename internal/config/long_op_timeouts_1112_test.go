package config

import (
	"strings"
	"testing"
	"time"
)

// The four long-operation bounds that were hardcoded (#1112), read the way
// tiered_storage.scan_timeout and backup.operation_timeout are: a Go duration
// string, positive only, a load-time error naming the key and the value.
// Validated and populated with every feature off, which is the default.
func TestLongOperationTimeouts(t *testing.T) {
	keys := []struct {
		key, env string
		def      time.Duration
		get      func(*Config) time.Duration
	}{
		{"tiered_storage.migration_timeout", "ARC_TIERED_STORAGE_MIGRATION_TIMEOUT", 2 * time.Hour,
			func(c *Config) time.Duration { return c.TieredStorage.MigrationTimeout }},
		{"edge_sync.spoke.sync_timeout", "ARC_EDGE_SYNC_SPOKE_SYNC_TIMEOUT", 2 * time.Hour,
			func(c *Config) time.Duration { return c.EdgeSync.Spoke.SyncTimeout }},
		{"edge_sync.spoke.bundle.timeout", "ARC_EDGE_SYNC_SPOKE_BUNDLE_TIMEOUT", 2 * time.Hour,
			func(c *Config) time.Duration { return c.EdgeSync.Spoke.Bundle.Timeout }},
		{"edge_sync.import.timeout", "ARC_EDGE_SYNC_IMPORT_TIMEOUT", 4 * time.Hour,
			func(c *Config) time.Duration { return c.EdgeSync.Import.Timeout }},
	}
	for _, k := range keys {
		for _, tc := range []struct {
			name    string
			value   string
			want    time.Duration
			wantErr bool
		}{
			{"default", "", k.def, false},
			{"nondefault", "45m", 45 * time.Minute, false},
			{"seconds", "90s", 90 * time.Second, false},
			{"zero", "0s", 0, true},
			{"negative", "-5m", 0, true},
			{"malformed", "banana", 0, true},
		} {
			t.Run(k.key+"/"+tc.name, func(t *testing.T) {
				t.Chdir(t.TempDir())
				t.Setenv(k.env, tc.value)
				cfg, err := Load()
				if tc.wantErr {
					if err == nil || !strings.Contains(err.Error(), k.key) || !strings.Contains(err.Error(), tc.value) {
						t.Fatalf("got %v, want an error naming %s and %q", err, k.key, tc.value)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := k.get(cfg); got != tc.want {
					t.Fatalf("%s = %s, want %s", k.key, got, tc.want)
				}
			})
		}
	}
}
