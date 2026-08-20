package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// loadWith installs body as arc.toml in a temp working directory and loads it.
// Load discovers its config file from the working directory, so the chdir is
// how a test controls what it reads.
func loadWith(t *testing.T, body string) (*Config, error) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "arc.toml"), []byte(body), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(oldWd) })
	return Load()
}

const validSpokeConfig = `
[edge_sync.spoke]
enabled = true
hub_url = "https://hub.example.com"
spoke_id = "rocket-01"
hub_id = "ground-station"
`

// The secret's only sanctioned home is the environment, so a config that
// supplies it there must load. Guarding this with viper's IsSet rejects it —
// IsSet consults the environment under AutomaticEnv — which left the feature
// unusable in exactly the setup it requires.
func TestSpokeConfig_SecretFromEnvironmentIsAccepted(t *testing.T) {
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", strings.Repeat("a", 64))

	cfg, err := loadWith(t, validSpokeConfig)
	if err != nil {
		t.Fatalf("a spoke supplying its secret via the environment failed to load: %v", err)
	}
	if cfg.EdgeSync.Spoke.Secret == "" {
		t.Error("the secret from the environment did not reach the config")
	}
}

// A secret in the file is refused rather than ignored: one that is ignored
// still leaks, and leaving it makes the committed copy look load-bearing.
func TestSpokeConfig_SecretInFileIsRefused(t *testing.T) {
	for _, key := range []string{"secret", "shared_secret"} {
		t.Run(key, func(t *testing.T) {
			_, err := loadWith(t, validSpokeConfig+key+` = "hunter2"`+"\n")
			if err == nil {
				t.Fatalf("a config file carrying %s loaded without complaint", key)
			}
			if !strings.Contains(err.Error(), "ARC_EDGE_SYNC_SPOKE_SECRET") {
				t.Errorf("the error does not say where the secret belongs: %v", err)
			}
		})
	}
}

// The hub API token follows the secret's rules exactly: environment only,
// refused in the file, and reachable from the config when supplied via env.
func TestSpokeConfig_HubTokenEnvOnlyAndRefusedInFile(t *testing.T) {
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", strings.Repeat("a", 64))
	t.Setenv("ARC_EDGE_SYNC_HUB_TOKEN", "hub-write-token")

	cfg, err := loadWith(t, validSpokeConfig)
	if err != nil {
		t.Fatalf("a spoke supplying its hub token via the environment failed to load: %v", err)
	}
	if cfg.EdgeSync.Spoke.HubToken != "hub-write-token" {
		t.Errorf("hub token from the environment did not reach the config: %q", cfg.EdgeSync.Spoke.HubToken)
	}

	_, err = loadWith(t, validSpokeConfig+`hub_token = "hunter2"`+"\n")
	if err == nil {
		t.Fatal("a config file carrying hub_token loaded without complaint")
	}
	if !strings.Contains(err.Error(), "ARC_EDGE_SYNC_HUB_TOKEN") {
		t.Errorf("the error does not say where the token belongs: %v", err)
	}
}

// A mismatched or missing hub_id fails every request with a 400. Catching it
// at load turns a silent total failure into a startup message.
func TestSpokeConfig_RequiresIdentitiesAndURL(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"no hub_url", "[edge_sync.spoke]\nenabled = true\nspoke_id = \"r1\"\nhub_id = \"g\"\n", "hub_url"},
		{"no spoke_id", "[edge_sync.spoke]\nenabled = true\nhub_url = \"https://h\"\nhub_id = \"g\"\n", "spoke_id"},
		{"no hub_id", "[edge_sync.spoke]\nenabled = true\nhub_url = \"https://h\"\nspoke_id = \"r1\"\n", "hub_id"},
		{"scheme-less hub_url", "[edge_sync.spoke]\nenabled = true\nhub_url = \"hub.example.com\"\nspoke_id = \"r1\"\nhub_id = \"g\"\n", "http"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := loadWith(t, tc.body); err == nil {
				t.Fatalf("config loaded despite %s", tc.name)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// A disabled spoke is the default for every Arc deployment, so an incomplete
// block must not block startup.
func TestSpokeConfig_DisabledSpokeSkipsValidation(t *testing.T) {
	cfg, err := loadWith(t, "[edge_sync.spoke]\nenabled = false\n")
	if err != nil {
		t.Fatalf("a disabled spoke blocked startup: %v", err)
	}
	if cfg.EdgeSync.Spoke.Enabled {
		t.Error("spoke reported enabled")
	}
}

// Defaults are declared with v.SetDefault rather than left to the agent's
// zero-value fallbacks, so an operator reading the config sees them and the
// documented value cannot drift from the code.
func TestSpokeConfig_DefaultsAreDeclared(t *testing.T) {
	cfg, err := loadWith(t, "[server]\nport = 8000\n")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.EdgeSync.Spoke.Enabled {
		t.Error("the spoke is enabled by default")
	}
	if got := cfg.EdgeSync.Spoke.MaxAttempts; got != 5 {
		t.Errorf("max_attempts = %d, want 5", got)
	}
	if got := cfg.EdgeSync.Spoke.MaxConcurrent; got != 2 {
		t.Errorf("max_concurrent = %d, want 2", got)
	}
	// 1000 pages under the hub's default max_reconcile_entries (10000) and
	// bounds per-page spoke memory. 0 ("whole backlog in one reconcile") is
	// an explicit opt-in, not the default — a default-config spoke with a
	// large backlog must not depend on 413 splitting to make progress.
	if got := cfg.EdgeSync.Spoke.BatchSize; got != 1000 {
		t.Errorf("batch_size = %d, want 1000", got)
	}
	// Default TRUE: a syncing spoke must not silently double-count on the
	// hub; opting out is the explicit choice (issue #610).
	if !cfg.EdgeSync.Spoke.DeferCompactionUntilSynced {
		t.Error("defer_compaction_until_synced should default to true")
	}
	// Default TRUE: a hub compacts what it received; keeping the raw
	// per-file layout is the explicit choice (issue #619).
	if !cfg.EdgeSync.CompactReceivedNamespaces {
		t.Error("compact_received_namespaces should default to true")
	}
}

// A fully air-gapped spoke exports bundles and never runs the network path, so
// bundle validation must not be nested inside the spoke's own enabled check.
func TestBundleConfig_ValidatesIndependentlyOfTheSpokeAgent(t *testing.T) {
	// The secret signs the manifest, so bundle export requires it too.
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", strings.Repeat("a", 64))

	body := `
[edge_sync.spoke]
spoke_id = "rocket-01"
hub_id = "ground-station"

[edge_sync.spoke.bundle]
enabled = true
allowed_dirs = ["/mnt/usb"]
`
	cfg, err := loadWith(t, body)
	if err != nil {
		t.Fatalf("an air-gap-only spoke failed to load: %v", err)
	}
	if cfg.EdgeSync.Spoke.Enabled {
		t.Error("the sync agent was enabled without being asked for")
	}
	if !cfg.EdgeSync.Spoke.Bundle.Enabled {
		t.Error("bundle export did not survive load")
	}
	// Defaults must apply even though the agent is off.
	if cfg.EdgeSync.Spoke.Bundle.MaxFiles != 10000 {
		t.Errorf("max_files = %d, want 10000", cfg.EdgeSync.Spoke.Bundle.MaxFiles)
	}
	if cfg.EdgeSync.Spoke.Bundle.MaxBytes != int64(64)<<30 {
		t.Errorf("max_bytes = %d, want 64GiB", cfg.EdgeSync.Spoke.Bundle.MaxBytes)
	}
}

// Every other Arc write path is confined to the storage root by its backend. A
// bundle is not, so the permitted roots must be stated rather than guessed.
func TestBundleConfig_RequiresAllowedDirsAndIdentities(t *testing.T) {
	// Present so each case fails for the reason it is testing, not for a
	// missing secret.
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", strings.Repeat("a", 64))

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			"no allowed_dirs",
			"[edge_sync.spoke]\nspoke_id = \"r1\"\nhub_id = \"g\"\n[edge_sync.spoke.bundle]\nenabled = true\n",
			"allowed_dirs",
		},
		{
			"no spoke_id",
			"[edge_sync.spoke]\nhub_id = \"g\"\n[edge_sync.spoke.bundle]\nenabled = true\nallowed_dirs = [\"/mnt/usb\"]\n",
			"spoke_id",
		},
		{
			"no hub_id",
			"[edge_sync.spoke]\nspoke_id = \"r1\"\n[edge_sync.spoke.bundle]\nenabled = true\nallowed_dirs = [\"/mnt/usb\"]\n",
			"hub_id",
		},
		{
			"negative max_files",
			"[edge_sync.spoke]\nspoke_id = \"r1\"\nhub_id = \"g\"\n[edge_sync.spoke.bundle]\nenabled = true\nallowed_dirs = [\"/mnt/usb\"]\nmax_files = -1\n",
			"max_files",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := loadWith(t, tc.body); err == nil {
				t.Fatalf("config loaded despite %s", tc.name)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// Disabled bundle export is the default for every Arc deployment.
func TestBundleConfig_DisabledSkipsValidation(t *testing.T) {
	cfg, err := loadWith(t, "[edge_sync.spoke.bundle]\nenabled = false\n")
	if err != nil {
		t.Fatalf("a disabled bundle blocked startup: %v", err)
	}
	if cfg.EdgeSync.Spoke.Bundle.Enabled {
		t.Error("bundle export reported enabled")
	}
}

// An air-gap-only spoke never enters the Spoke.Enabled block, so without its
// own check the missing secret surfaced as a late fatal from an internal
// constructor after a full startup — naming a Go type rather than the
// environment variable, to the operator least able to iterate.
func TestBundleConfig_RequiresTheSecretOnTheAirGapPath(t *testing.T) {
	body := `
[edge_sync.spoke]
spoke_id = "rocket-01"
hub_id = "ground-station"

[edge_sync.spoke.bundle]
enabled = true
allowed_dirs = ["/mnt/usb"]
`
	_, err := loadWith(t, body)
	if err == nil {
		t.Fatal("an air-gap spoke loaded with no signing secret")
	}
	if !strings.Contains(err.Error(), "ARC_EDGE_SYNC_SPOKE_SECRET") {
		t.Errorf("the error does not name the environment variable: %v", err)
	}

	// With the secret present it must load.
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", strings.Repeat("a", 64))
	if _, err := loadWith(t, body); err != nil {
		t.Errorf("an air-gap spoke with a secret failed to load: %v", err)
	}
}
