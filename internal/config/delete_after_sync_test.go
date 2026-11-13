package config

import (
	"strings"
	"testing"
	"time"
)

func TestDeleteAfterSyncConfig(t *testing.T) {
	base := map[string]string{
		"ARC_EDGE_SYNC_SPOKE_ENABLED":  "true",
		"ARC_EDGE_SYNC_SPOKE_HUB_URL":  "https://hub.example:8000",
		"ARC_EDGE_SYNC_SPOKE_SPOKE_ID": "rocket01",
		"ARC_EDGE_SYNC_SPOKE_HUB_ID":   "hub-1",
		"ARC_EDGE_SYNC_SPOKE_SECRET":   "0123456789abcdef0123456789abcdef",
	}
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr string
		check   func(*testing.T, *Config)
	}{
		{"default off", nil, "", func(t *testing.T, c *Config) {
			if c.EdgeSync.Spoke.DeleteAfterSync || c.EdgeSync.Spoke.DeleteAfterSyncDelay != 0 {
				t.Fatalf("defaults = %v %s", c.EdgeSync.Spoke.DeleteAfterSync, c.EdgeSync.Spoke.DeleteAfterSyncDelay)
			}
		}},
		{"on, https, non-default delay", map[string]string{"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC": "true", "ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC_DELAY": "36h"}, "", func(t *testing.T, c *Config) {
			if !c.EdgeSync.Spoke.DeleteAfterSync || c.EdgeSync.Spoke.DeleteAfterSyncDelay != 36*time.Hour {
				t.Fatalf("got %v %s", c.EdgeSync.Spoke.DeleteAfterSync, c.EdgeSync.Spoke.DeleteAfterSyncDelay)
			}
		}},
		{"loopback http allowed", map[string]string{"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC": "true", "ARC_EDGE_SYNC_SPOKE_HUB_URL": "http://127.0.0.1:8000"}, "", nil},
		{"plain http refused", map[string]string{"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC": "true", "ARC_EDGE_SYNC_SPOKE_HUB_URL": "http://hub.example:8000"}, "https", nil},
		{"cluster refused", map[string]string{"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC": "true", "ARC_CLUSTER_ENABLED": "true"}, "cluster", nil},
		{"needs spoke.enabled", map[string]string{"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC": "true", "ARC_EDGE_SYNC_SPOKE_ENABLED": "false"}, "edge_sync.spoke.enabled", nil},
		{"negative delay", map[string]string{"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC_DELAY": "-1s"}, "delete_after_sync_delay", nil},
		{"delay beyond retention", map[string]string{"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC": "true", "ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC_DELAY": "2400h", "ARC_EDGE_SYNC_SPOKE_LEDGER_RETENTION_DAYS": "90"}, "ledger_retention_days", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range base {
				t.Setenv(k, v)
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one mentioning %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.check != nil {
				tc.check(t, cfg)
			}
		})
	}
}
