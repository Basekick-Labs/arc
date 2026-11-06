package config

import (
	"strings"
	"testing"
	"time"
)

// #1501: wal.ack_timeout bounds a durable write's wait, so a value that does not
// is refused at load rather than turned into an unbounded or expired wait.
func TestWALAckTimeout(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"default", "", 30 * time.Second, false},
		{"nondefault", "2m", 2 * time.Minute, false},
		{"zero", "0s", 0, true},
		{"negative", "-5s", 0, true},
		{"malformed", "banana", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.value != "" {
				t.Setenv("ARC_WAL_ACK_TIMEOUT", tc.value)
			}
			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "wal.ack_timeout") || !strings.Contains(err.Error(), tc.value) {
					t.Fatalf("got %v, want an error naming wal.ack_timeout and %q", err, tc.value)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.WAL.AckTimeout != tc.want {
				t.Fatalf("AckTimeout = %s, want %s", cfg.WAL.AckTimeout, tc.want)
			}
		})
	}
}
