package config

import (
	"os"
	"strings"
	"testing"
)

// tok is a 40-character placeholder; it is not a real credential.
const bootstrapTok1713 = "abcdefghijklmnopqrstuvwxyz0123456789ABCD"

func bootstrapWarnings(cfg *Config) []LoadWarning {
	var out []LoadWarning
	for _, w := range cfg.Warnings {
		if w.Key == "auth.bootstrap_token" {
			out = append(out, w)
		}
	}
	return out
}

// TestLoad_BootstrapTokenTrimmed1713 pins #1713: surrounding whitespace (the
// trailing newline of a file the value was read from) is removed before the
// value reaches the bootstrap, inner content is kept, the warning never
// carries the value, and a value that is only whitespace is refused.
func TestLoad_BootstrapTokenTrimmed1713(t *testing.T) {
	for _, tc := range []struct {
		name     string
		value    string
		auth     string
		force    bool
		want     string
		wantWarn bool
		wantErr  string
	}{
		{name: "trailing newline", value: bootstrapTok1713 + "\n", auth: "true", want: bootstrapTok1713, wantWarn: true},
		{name: "CRLF", value: bootstrapTok1713 + "\r\n", auth: "true", want: bootstrapTok1713, wantWarn: true},
		{name: "spaces", value: "  " + bootstrapTok1713 + "  ", auth: "true", want: bootstrapTok1713, wantWarn: true},
		{name: "tabs", value: "\t" + bootstrapTok1713 + "\t", auth: "true", want: bootstrapTok1713, wantWarn: true},
		{name: "clean value, no warning", value: bootstrapTok1713, auth: "true", want: bootstrapTok1713},
		{name: "inner whitespace kept", value: " abc def\tghi\n", auth: "true", want: "abc def\tghi", wantWarn: true},
		{name: "force_bootstrap trimmed too", value: bootstrapTok1713 + "\n", auth: "true", force: true, want: bootstrapTok1713, wantWarn: true},
		{name: "only whitespace refused", value: " \n\t\r\n", auth: "true", wantErr: "contains only whitespace"},
		{name: "only whitespace with force_bootstrap refused", value: "\n", auth: "true", force: true, wantErr: "contains only whitespace"},
		{name: "only whitespace, auth disabled: not read, not refused", value: "\n", auth: "false", want: ""},
		{name: "auth disabled: trimmed, no warning", value: bootstrapTok1713 + "\n", auth: "false", want: bootstrapTok1713},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_AUTH_ENABLED", tc.auth)
			t.Setenv("ARC_AUTH_BOOTSTRAP_TOKEN", tc.value)
			if tc.force {
				t.Setenv("ARC_AUTH_FORCE_BOOTSTRAP", "true")
			}
			cfg, err := Load()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load() error = %v, want it to contain %q", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), tc.value) {
					t.Fatalf("Load() error carries the configured value")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Auth.BootstrapToken != tc.want {
				t.Fatalf("BootstrapToken = %q, want %q", cfg.Auth.BootstrapToken, tc.want)
			}
			ws := bootstrapWarnings(cfg)
			if got := len(ws) > 0; got != tc.wantWarn {
				t.Fatalf("bootstrap warning present = %v, want %v", got, tc.wantWarn)
			}
			for _, w := range ws {
				if strings.Contains(w.Value, tc.want) || strings.Contains(w.Message, tc.want) {
					t.Fatalf("warning carries the token value: %+v", w)
				}
			}
		})
	}
}

// TestLoad_BootstrapTokenTrimmedFromConfigFile1713 covers the arc.toml path:
// a TOML string with an escaped newline is trimmed like the env var.
func TestLoad_BootstrapTokenTrimmedFromConfigFile1713(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	toml := "[auth]\nenabled = true\nbootstrap_token = \"" + bootstrapTok1713 + "\\n\"\n"
	if err := os.WriteFile("arc.toml", []byte(toml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ConfigFile == "" {
		t.Fatal("arc.toml was not read")
	}
	if cfg.Auth.BootstrapToken != bootstrapTok1713 {
		t.Fatalf("BootstrapToken = %q, want the trimmed value", cfg.Auth.BootstrapToken)
	}
	if len(bootstrapWarnings(cfg)) != 1 {
		t.Fatalf("want one bootstrap warning, got %+v", cfg.Warnings)
	}
}
