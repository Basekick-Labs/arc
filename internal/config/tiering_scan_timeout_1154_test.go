package config

import (
	"strings"
	"testing"
	"time"
)

// tiered_storage.scan_timeout replaces the 30 minutes hardcoded at the startup
// scan and POST /api/v1/tiering/scan, and bounds the pre-migration scan inside
// a cycle as well (#1154). Read with time.ParseDuration over GetString, the way
// compaction.cycle_timeout and backup.operation_timeout are; the shape of this
// test follows theirs.
//
// The default case matters most: it is 2h rather than the 30m it replaces, so
// that all three paths agree on one number. 30 minutes had no rationale — it
// arrived with #953, a cluster-manifest change.
func TestTieredStorageScanTimeout(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"default", "", 2 * time.Hour, false},
		{"nondefault", "45m", 45 * time.Minute, false},
		{"long", "6h", 6 * time.Hour, false},
		{"seconds", "5s", 5 * time.Second, false},
		// Zero is a load-time error rather than "no timeout": a context with
		// a zero timeout is already expired, so every scan would truncate on
		// its first object and no stale hot row would ever be retired.
		{"zero", "0s", 0, true},
		{"negative", "-5m", 0, true},
		{"malformed", "banana", 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_TIERED_STORAGE_SCAN_TIMEOUT", tc.value)

			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "tiered_storage.scan_timeout") {
					t.Fatalf("got %v, want a tiered_storage.scan_timeout validation error naming the key", err)
				}
				if tc.value != "" && !strings.Contains(err.Error(), tc.value) {
					t.Errorf("error %v does not name the rejected value %q", err, tc.value)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}
			if cfg.TieredStorage.ScanTimeout != tc.want {
				t.Fatalf("ScanTimeout = %s, want %s", cfg.TieredStorage.ScanTimeout, tc.want)
			}
		})
	}
}

// The key is validated and assigned even when tiering is disabled: the struct
// literal that populates TieredStorage runs unconditionally, and a zero here
// would reach any Manager a later config reload or a test hands it.
func TestScanTimeoutIsPopulatedWithTieringDisabled(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TieredStorage.Enabled {
		t.Fatal("precondition: tiering is disabled by default, which is the state under test")
	}
	if cfg.TieredStorage.ScanTimeout != 2*time.Hour {
		t.Fatalf("ScanTimeout = %s with tiering disabled, want 2h; an unassigned field is a dead scan context", cfg.TieredStorage.ScanTimeout)
	}
}
