package config

import "testing"

// The default must match the floor NewHourlyTier enforces (#278); a default
// below it made every start without the key log the override warning.
func TestHourlyMinAgeHoursDefault(t *testing.T) {
	t.Chdir(t.TempDir())

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Compaction.HourlyMinAgeHours; got != 1 {
		t.Fatalf("compaction.hourly_min_age_hours default = %d, want 1", got)
	}
}

// An explicit value is passed through unchanged; the clamp to 1 lives in
// compaction.NewHourlyTier, not in config loading.
func TestHourlyMinAgeHoursExplicitZeroPassesThrough(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_COMPACTION_HOURLY_MIN_AGE_HOURS", "0")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Compaction.HourlyMinAgeHours; got != 0 {
		t.Fatalf("explicit hourly_min_age_hours = %d, want 0", got)
	}
}
