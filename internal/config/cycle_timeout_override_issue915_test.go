package config

import (
	"os"
	"testing"
	"time"
)

func TestCycleTimeoutEnvOverridesFileIssue915(t *testing.T) {
	t.Chdir(t.TempDir())

	err := os.WriteFile(
		"arc.toml",
		[]byte("[compaction]\ncycle_timeout = \"2h\"\n"),
		0600,
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("ARC_COMPACTION_CYCLE_TIMEOUT", "75m")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Compaction.CycleTimeout != 75*time.Minute {
		t.Fatalf(
			"cycle timeout = %s, want 75m",
			cfg.Compaction.CycleTimeout,
		)
	}
}
