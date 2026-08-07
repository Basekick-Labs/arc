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
	// 0 means "defer to the hub's cap", which is a real value here, not an
	// unset field.
	if got := cfg.EdgeSync.Spoke.BatchSize; got != 0 {
		t.Errorf("batch_size = %d, want 0", got)
	}
}
