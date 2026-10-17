package config

import (
	"strings"
	"testing"
)

func TestLoad_WALDiskPressureDefaults(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.WAL.DiskPressureEnabled || cfg.WAL.DiskMinFreeMB != 0 || cfg.WAL.MaxDirectoryMB != 0 {
		t.Fatalf("defaults = %v / %d / %d, want true / 0 / 0", cfg.WAL.DiskPressureEnabled, cfg.WAL.DiskMinFreeMB, cfg.WAL.MaxDirectoryMB)
	}
}

func TestLoad_WALDiskPressureFromEnv(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_WAL_DISK_PRESSURE_ENABLED", "false")
	t.Setenv("ARC_WAL_DISK_MIN_FREE_MB", "2048")
	t.Setenv("ARC_WAL_MAX_DIRECTORY_MB", "4000")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WAL.DiskPressureEnabled || cfg.WAL.DiskMinFreeMB != 2048 || cfg.WAL.MaxDirectoryMB != 4000 {
		t.Fatalf("got %v / %d / %d", cfg.WAL.DiskPressureEnabled, cfg.WAL.DiskMinFreeMB, cfg.WAL.MaxDirectoryMB)
	}
}

func TestLoad_WALDiskNegativeRejected(t *testing.T) {
	for _, key := range []string{"ARC_WAL_DISK_MIN_FREE_MB", "ARC_WAL_MAX_DIRECTORY_MB"} {
		t.Run(key, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv(key, "-1")
			_, err := Load()
			want := strings.ToLower(strings.TrimPrefix(key, "ARC_WAL_"))
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want a rejection naming %s", err, want)
			}
		})
	}
}

// A cap too small to hold the active file and the clearing band would keep
// the gate on for good: raised to four WAL files, with a warning.
func TestLoad_WALMaxDirectoryRaisedToFourFiles(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_WAL_MAX_SIZE_MB", "100")
	t.Setenv("ARC_WAL_MAX_DIRECTORY_MB", "399")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WAL.MaxDirectoryMB != 400 {
		t.Fatalf("max_directory_mb = %d, want 400", cfg.WAL.MaxDirectoryMB)
	}
	found := false
	for _, w := range cfg.Warnings {
		if w.Key == "wal.max_directory_mb" && w.Value == "399" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no warning: %+v", cfg.Warnings)
	}
	t.Setenv("ARC_WAL_MAX_DIRECTORY_MB", "400")
	cfg, err = Load()
	if err != nil || cfg.WAL.MaxDirectoryMB != 400 || len(cfg.Warnings) != 0 && cfg.Warnings[len(cfg.Warnings)-1].Key == "wal.max_directory_mb" {
		t.Fatalf("a cap of four files was changed or warned about: %v %d %+v", err, cfg.WAL.MaxDirectoryMB, cfg.Warnings)
	}
}
