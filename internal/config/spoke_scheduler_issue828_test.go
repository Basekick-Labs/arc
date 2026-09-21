package config

import (
	"strings"
	"testing"
	"time"
)

func TestSpokeSchedulerDefaultsIssue828(t *testing.T) {
	cfg, err := loadWith(t, "[server]\nport = 8000\n")
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}

	if got := cfg.EdgeSync.Spoke.SyncInterval; got != 5*time.Minute {
		t.Fatalf("sync interval = %s, want 5m", got)
	}
	if got := cfg.EdgeSync.Spoke.RetryInterval; got != 30*time.Second {
		t.Fatalf("retry interval = %s, want 30s", got)
	}
	if cfg.EdgeSync.Spoke.Enabled {
		t.Fatal("network spoke unexpectedly enabled")
	}
}

func TestSpokeSchedulerFileAndEnvironmentOverridesIssue828(t *testing.T) {
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", strings.Repeat("a", 64))
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SYNC_INTERVAL", "17s")
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SYNC_RETRY_INTERVAL", "3s")

	cfg, err := loadWith(
		t,
		validSpokeConfig+
			"sync_interval = \"11s\"\n"+
			"sync_retry_interval = \"2s\"\n",
	)
	if err != nil {
		t.Fatalf("load overrides: %v", err)
	}

	if got := cfg.EdgeSync.Spoke.SyncInterval; got != 17*time.Second {
		t.Fatalf("environment sync interval = %s, want 17s", got)
	}
	if got := cfg.EdgeSync.Spoke.RetryInterval; got != 3*time.Second {
		t.Fatalf("environment retry interval = %s, want 3s", got)
	}
}

func TestSpokeSchedulerFileOverridesIssue828(t *testing.T) {
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", strings.Repeat("a", 64))

	cfg, err := loadWith(
		t,
		validSpokeConfig+
			"sync_interval = \"11s\"\n"+
			"sync_retry_interval = \"2s\"\n",
	)
	if err != nil {
		t.Fatalf("load file overrides: %v", err)
	}

	if cfg.EdgeSync.Spoke.SyncInterval != 11*time.Second ||
		cfg.EdgeSync.Spoke.RetryInterval != 2*time.Second {
		t.Fatalf("file interval overrides were lost: %+v", cfg.EdgeSync.Spoke)
	}
}

func TestSpokeSchedulerRejectsInvalidDurationsIssue828(t *testing.T) {
	t.Setenv("ARC_EDGE_SYNC_SPOKE_SECRET", strings.Repeat("a", 64))

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "invalid interval",
			body: "sync_interval = \"not-a-duration\"\n",
			want: "sync_interval",
		},
		{
			name: "zero interval",
			body: "sync_interval = \"0s\"\n",
			want: "sync_interval",
		},
		{
			name: "negative interval",
			body: "sync_interval = \"-1s\"\n",
			want: "sync_interval",
		},
		{
			name: "invalid retry",
			body: "sync_retry_interval = \"bad\"\n",
			want: "sync_retry_interval",
		},
		{
			name: "zero retry",
			body: "sync_retry_interval = \"0s\"\n",
			want: "sync_retry_interval",
		},
		{
			name: "equal intervals",
			body: "sync_interval = \"5m\"\nsync_retry_interval = \"5m\"\n",
			want: "sync_retry_interval",
		},
		{
			name: "retry longer",
			body: "sync_interval = \"5m\"\nsync_retry_interval = \"6m\"\n",
			want: "sync_retry_interval",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadWith(t, validSpokeConfig+tc.body)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("load error = %v, want %q", err, tc.want)
			}
		})
	}
}
