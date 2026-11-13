package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEdgeSyncRelayConfig(t *testing.T) {
	relayDir := t.TempDir()
	spoke := map[string]string{
		"ARC_EDGE_SYNC_SPOKE_SPOKE_ID":         "rocket01",
		"ARC_EDGE_SYNC_SPOKE_HUB_ID":           "hub-1",
		"ARC_EDGE_SYNC_SPOKE_SECRET":           "0123456789abcdef0123456789abcdef",
		"ARC_EDGE_SYNC_SPOKE_RELAY_ENABLED":    "true",
		"ARC_EDGE_SYNC_SPOKE_RELAY_TYPE":       "local",
		"ARC_EDGE_SYNC_SPOKE_RELAY_LOCAL_PATH": relayDir,
	}
	hub := map[string]string{
		"ARC_EDGE_SYNC_HUB_ID":                  "hub-1",
		"ARC_EDGE_SYNC_IMPORT_RELAY_ENABLED":    "true",
		"ARC_EDGE_SYNC_IMPORT_RELAY_TYPE":       "s3",
		"ARC_EDGE_SYNC_IMPORT_RELAY_S3_BUCKET":  "relay",
		"ARC_EDGE_SYNC_IMPORT_RELAY_S3_PREFIX":  "fleet-a/",
		"ARC_EDGE_SYNC_IMPORT_RELAY_S3_REGION":  "eu-west-1",
		"ARC_EDGE_SYNC_IMPORT_RELAY_S3_USE_SSL": "false",
	}
	merge := func(maps ...map[string]string) map[string]string {
		out := map[string]string{}
		for _, m := range maps {
			for k, v := range m {
				out[k] = v
			}
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr string
		check   func(*testing.T, *Config)
	}{
		{"off by default", nil, "", func(t *testing.T, c *Config) {
			if c.EdgeSync.Spoke.Relay.Enabled || c.EdgeSync.Import.Relay.Enabled || c.EdgeSync.Spoke.Active() || c.EdgeSync.Import.Active() {
				t.Fatal("relay on by default")
			}
		}},
		{"spoke defaults", spoke, "", func(t *testing.T, c *Config) {
			r := c.EdgeSync.Spoke.Relay
			if r.Interval != 5*time.Minute || r.AckTimeout != 72*time.Hour || r.Target.LocalPath != relayDir {
				t.Fatalf("spoke relay = %+v", r)
			}
			if !c.EdgeSync.Spoke.Active() || !c.EdgeSync.Spoke.ExportsBundles() {
				t.Fatal("a relay-only spoke must count as an active, bundle-exporting spoke")
			}
		}},
		{"spoke non-default interval and timeout", merge(spoke, map[string]string{
			"ARC_EDGE_SYNC_SPOKE_RELAY_INTERVAL": "90s", "ARC_EDGE_SYNC_SPOKE_RELAY_ACK_TIMEOUT": "6h"}), "", func(t *testing.T, c *Config) {
			if r := c.EdgeSync.Spoke.Relay; r.Interval != 90*time.Second || r.AckTimeout != 6*time.Hour {
				t.Fatalf("got %s %s", r.Interval, r.AckTimeout)
			}
		}},
		{"hub, s3 with a prefix (non-default)", hub, "", func(t *testing.T, c *Config) {
			r := c.EdgeSync.Import.Relay
			if r.Interval != time.Minute || r.Target.S3Prefix != "fleet-a/" || r.Target.S3Region != "eu-west-1" || r.Target.S3UseSSL {
				t.Fatalf("hub relay = %+v", r)
			}
			if !c.EdgeSync.Import.Active() || c.EdgeSync.Import.Enabled {
				t.Fatal("relay-only hub: Active without the drive route")
			}
			if r.StagingDir != "./data/edge_sync_relay_staging" {
				t.Fatalf("staging dir default = %q", r.StagingDir)
			}
		}},
		{"spoke needs a secret", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_SECRET": ""}), "ARC_EDGE_SYNC_SPOKE_SECRET", nil},
		{"spoke needs a type", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_RELAY_TYPE": ""}), "relay.type", nil},
		{"gcs needs a bucket", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_RELAY_TYPE": "gcs"}), "gcs_bucket", nil},
		{"gcs with a prefix", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_RELAY_TYPE": "gcs",
			"ARC_EDGE_SYNC_SPOKE_RELAY_GCS_BUCKET": "relay", "ARC_EDGE_SYNC_SPOKE_RELAY_GCS_PREFIX": "fleet-c/"}), "", func(t *testing.T, c *Config) {
			if tg := c.EdgeSync.Spoke.Relay.Target; tg.GCSBucket != "relay" || tg.GCSPrefix != "fleet-c/" {
				t.Fatalf("target = %+v", tg)
			}
		}},
		{"gcs http endpoint refused", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_RELAY_TYPE": "gcs",
			"ARC_EDGE_SYNC_SPOKE_RELAY_GCS_BUCKET": "relay", "ARC_EDGE_SYNC_SPOKE_RELAY_GCS_ENDPOINT": "http://example.com"}), "https", nil},
		{"relative local path refused", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_RELAY_LOCAL_PATH": "relay"}), "absolute", nil},
		{"ack timeout not past interval", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_RELAY_ACK_TIMEOUT": "5m"}), "ack_timeout", nil},
		{"bad interval", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_RELAY_INTERVAL": "0s"}), "relay.interval", nil},
		{"bad interval ignored while off", map[string]string{"ARC_EDGE_SYNC_SPOKE_RELAY_INTERVAL": "nonsense"}, "", nil},
		{"hub needs hub_id", merge(hub, map[string]string{"ARC_EDGE_SYNC_HUB_ID": ""}), "edge_sync.hub_id", nil},
		{"hub s3 needs a bucket", merge(hub, map[string]string{"ARC_EDGE_SYNC_IMPORT_RELAY_S3_BUCKET": ""}), "s3_bucket", nil},
		{"relay inside primary storage refused", merge(spoke, map[string]string{
			"ARC_STORAGE_BACKEND": "local", "ARC_STORAGE_LOCAL_PATH": relayDir,
			"ARC_EDGE_SYNC_SPOKE_RELAY_LOCAL_PATH": filepath.Join(relayDir, "relay")}), "overlaps primary storage", nil},
		{"relay sibling of primary storage allowed", merge(spoke, map[string]string{
			"ARC_STORAGE_BACKEND": "local", "ARC_STORAGE_LOCAL_PATH": filepath.Join(relayDir, "data"),
			"ARC_EDGE_SYNC_SPOKE_RELAY_LOCAL_PATH": filepath.Join(relayDir, "data-relay")}), "", nil},
		{"hub staging inside storage refused", merge(hub, map[string]string{
			"ARC_STORAGE_BACKEND": "local", "ARC_STORAGE_LOCAL_PATH": relayDir,
			"ARC_EDGE_SYNC_IMPORT_RELAY_STAGING_DIR": filepath.Join(relayDir, "staging")}), "staging_dir", nil},
		{"hub staging inside a local relay refused", merge(hub, map[string]string{
			"ARC_EDGE_SYNC_IMPORT_RELAY_TYPE": "local", "ARC_EDGE_SYNC_IMPORT_RELAY_LOCAL_PATH": relayDir,
			"ARC_EDGE_SYNC_IMPORT_RELAY_STAGING_DIR": filepath.Join(relayDir, "staging")}), "overlaps", nil},
		{"negative bundle cap refused on a relay-only spoke", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_BUNDLE_MAX_BYTES": "-1"}), "max_bytes", nil},
		{"delete_after_sync on a relay-only spoke", merge(spoke, map[string]string{"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC": "true"}), "", nil},
		{"delete_after_sync delay checked against ack max age on a relay spoke", merge(spoke, map[string]string{
			"ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC": "true", "ARC_EDGE_SYNC_SPOKE_DELETE_AFTER_SYNC_DELAY": "200h"}), "delete_after_sync_ack_max_age", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			// The FIPS build has no native GCS backend: every configured
			// gcs relay is refused there, before its other checks.
			if fipsBuild && tc.env["ARC_EDGE_SYNC_SPOKE_RELAY_GCS_BUCKET"] != "" {
				tc.wantErr, tc.check = "FIPS build", nil
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

// A file-configured relay: the target keys are read as a table, exactly as a
// backup target's.
func TestEdgeSyncRelayConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", "0123456789abcdef0123456789abcdef")
	toml := `
[edge_sync.spoke]
spoke_id = "rocket01"
hub_id = "hub-1"

[edge_sync.spoke.relay]
enabled = true
type = "azure"
azure_container = "relay"
azure_prefix = "fleet-a"
azure_account_name = "acct"
interval = "2m"
`
	if err := os.WriteFile(filepath.Join(dir, "arc.toml"), []byte(toml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.EdgeSync.Spoke.Relay
	if r.Target.Type != "azure" || r.Target.AzureContainer != "relay" || r.Target.AzurePrefix != "fleet-a" || r.Interval != 2*time.Minute {
		t.Fatalf("relay = %+v", r)
	}
}
