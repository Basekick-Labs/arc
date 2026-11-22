package config

import "testing"

// auth.metrics_public: off by default (metrics need a token), on when set.
func TestAuthMetricsPublic(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.MetricsPublic {
		t.Error("auth.metrics_public defaults to true, want false")
	}
	t.Setenv("ARC_AUTH_METRICS_PUBLIC", "true")
	if cfg, err = Load(); err != nil || !cfg.Auth.MetricsPublic {
		t.Errorf("ARC_AUTH_METRICS_PUBLIC=true: %v, %v", cfg.Auth.MetricsPublic, err)
	}
}
