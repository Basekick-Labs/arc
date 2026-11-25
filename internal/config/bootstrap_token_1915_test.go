package config

import (
	"strings"
	"testing"
)

// #1915: a bootstrap token the bootstrap would reject stops Arc at load,
// instead of leaving it running with auth on and no admin token. The length
// is the trimmed value's, and the error never carries the value.
func TestLoad_BootstrapTokenTooShort1915(t *testing.T) {
	long := strings.Repeat("a", MinBootstrapTokenLength)
	for _, tc := range []struct {
		name    string
		value   string
		auth    string
		force   bool
		wantErr string
		want    string
	}{
		{name: "31 bytes refused", value: long[:31], auth: "true", wantErr: "31 bytes after surrounding whitespace is removed"},
		{name: "31 bytes plus a newline refused, counted trimmed", value: long[:31] + "\n", auth: "true", wantErr: "31 bytes after surrounding whitespace is removed"},
		{name: "with force_bootstrap refused too", value: long[:10], auth: "true", force: true, wantErr: "must be at least 32"},
		{name: "exactly 32 accepted", value: long, auth: "true", want: long},
		{name: "auth disabled: not read, not refused", value: "short", auth: "false", want: "short"},
		{name: "unset: Arc generates one", value: "", auth: "true", want: ""},
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
				if v := strings.TrimSpace(tc.value); v != "" && strings.Contains(err.Error(), v) {
					t.Fatal("Load() error carries the configured value")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Auth.BootstrapToken != tc.want {
				t.Fatalf("BootstrapToken = %q, want %q", cfg.Auth.BootstrapToken, tc.want)
			}
		})
	}
}

// auth.force_bootstrap without a token warns: it acts only with one.
func TestLoad_ForceBootstrapWithoutToken1915(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_AUTH_ENABLED", "true")
	t.Setenv("ARC_AUTH_BOOTSTRAP_TOKEN", "")
	t.Setenv("ARC_AUTH_FORCE_BOOTSTRAP", "true")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	var found bool
	for _, w := range cfg.Warnings {
		if w.Key == "auth.force_bootstrap" {
			found = true
			if !strings.Contains(w.Message, "only together with auth.bootstrap_token") {
				t.Fatalf("warning message = %q", w.Message)
			}
		}
	}
	if !found {
		t.Fatal("no auth.force_bootstrap warning")
	}
}
