package config

import (
	"strings"
	"testing"
)

// #1938: a WAL recovery interval <= 0 with the WAL on used to reach
// time.NewTicker and panic at startup; Load refuses it instead.
func TestLoad_WALRecoveryIntervalMustBePositive1938(t *testing.T) {
	for _, tc := range []struct {
		name, enabled, interval string
		wantErr                 bool
	}{
		{"wal on, 0", "true", "0", true},
		{"wal on, negative", "true", "-5", true},
		{"wal on, 30", "true", "30", false},
		{"wal off, 0", "false", "0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_WAL_ENABLED", tc.enabled)
			t.Setenv("ARC_WAL_RECOVERY_INTERVAL_SECONDS", tc.interval)
			_, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "wal.recovery_interval_seconds") {
					t.Fatalf("Load() error = %v, want a wal.recovery_interval_seconds error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
		})
	}
}
