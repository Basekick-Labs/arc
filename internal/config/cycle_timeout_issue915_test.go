package config

import (
	"strings"
	"testing"
	"time"
)

func TestCycleTimeoutConfigIssue915(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"default", "", 30 * time.Minute, false},
		{"nondefault", "2h15m", 135 * time.Minute, false},
		{"seconds", "90s", 90 * time.Second, false},
		{"zero", "0s", 0, true},
		{"negative", "-5m", 0, true},
		{"malformed", "thirty minutes", 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_COMPACTION_CYCLE_TIMEOUT", tc.value)

			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(
					err.Error(), "compaction.cycle_timeout",
				) {
					t.Fatalf("got %v, want cycle timeout validation error", err)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}
			if cfg.Compaction.CycleTimeout != tc.want {
				t.Fatalf(
					"timeout = %s, want %s",
					cfg.Compaction.CycleTimeout,
					tc.want,
				)
			}
		})
	}
}
