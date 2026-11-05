package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// findWarning returns the load warning for key, or nil.
func findWarning(cfg *Config, key string) *LoadWarning {
	for i := range cfg.Warnings {
		if cfg.Warnings[i].Key == key {
			return &cfg.Warnings[i]
		}
	}
	return nil
}

// TestPackagedLayoutBootsWithUnitBackupPath reproduces #1463 and its fix: the
// packaged unit's storage root is a parent of arc.toml's "./data/backups"
// resolved from the unit's working directory, which Load refuses, and the
// unit's ARC_BACKUP_LOCAL_PATH moves the destination outside it. The stock
// arc.toml line is in the file, as it is on every packaged install, and must
// not draw the override warning.
func TestPackagedLayoutBootsWithUnitBackupPath(t *testing.T) {
	writeArcToml(t, "[backup]\nlocal_path = \"./data/backups\"\n")
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARC_STORAGE_LOCAL_PATH", filepath.Join(wd, "data"))

	mustLoadError(t, "overlaps primary storage")

	t.Setenv("ARC_BACKUP_LOCAL_PATH", filepath.Join(wd, "backups"))
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with the unit's backup path: %v", err)
	}
	if cfg.Backup.LocalPath != filepath.Join(wd, "backups") {
		t.Fatalf("backup.local_path = %q, want the environment's", cfg.Backup.LocalPath)
	}
	if w := findWarning(cfg, "backup.local_path"); w != nil {
		t.Fatalf("stock arc.toml value drew an override warning: %+v", w)
	}
}

func TestBackupLocalPathOverrideWarning(t *testing.T) {
	cases := []struct {
		name     string
		toml     string
		env      string // relative to the working directory; "" = unset
		wantWarn bool
	}{
		{"customised file value overridden", "[backup]\nlocal_path = \"/mnt/backupdisk\"\n", "backups", true},
		{"no env", "[backup]\nlocal_path = \"/mnt/backupdisk\"\n", "", false},
		{"key not in file", "[server]\nport = 8000\n", "backups", false},
		{"file at default", "[backup]\nlocal_path = \"data/backups/\"\n", "backups", false},
		{"same directory spelled differently", "[backup]\nlocal_path = \"./backups\"\n", "backups", false},
		{"backup disabled", "[backup]\nenabled = false\nlocal_path = \"/mnt/backupdisk\"\n", "backups", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeArcToml(t, tc.toml)
			wd, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			if tc.env != "" {
				t.Setenv("ARC_BACKUP_LOCAL_PATH", filepath.Join(wd, tc.env))
			}
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load(): %v", err)
			}
			w := findWarning(cfg, "backup.local_path")
			if (w != nil) != tc.wantWarn {
				t.Fatalf("warning = %+v, want present=%v", w, tc.wantWarn)
			}
			if w != nil {
				if w.Value != "/mnt/backupdisk" {
					t.Errorf("warning value = %q, want the file's value", w.Value)
				}
				for _, want := range []string{"ARC_BACKUP_LOCAL_PATH=" + filepath.Join(wd, "backups"), "systemctl edit arc"} {
					if !strings.Contains(w.Message, want) {
						t.Errorf("warning message %q lacks %q", w.Message, want)
					}
				}
			}
		})
	}
}

func TestLegacyBackupsInStorageRootWarning(t *testing.T) {
	cases := []struct {
		name     string
		layout   map[string]string // path under the storage root -> file content
		wantWarn bool
	}{
		{"old packaged backups", map[string]string{"backups/20260901_000000_ab12/manifest.json": "{}"}, true},
		{"database named backups", map[string]string{"backups/cpu/2026/09/01/00/x.parquet": ""}, false},
		{"nothing there", map[string]string{"mydb/cpu/2026/09/01/00/x.parquet": ""}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeArcToml(t, "[server]\nport = 8000\n")
			wd, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(wd, "data")
			for p, body := range tc.layout {
				full := filepath.Join(root, p)
				if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("ARC_STORAGE_LOCAL_PATH", root)
			t.Setenv("ARC_BACKUP_LOCAL_PATH", filepath.Join(wd, "backups"))
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load(): %v", err)
			}
			w := findWarning(cfg, "storage.local_path")
			if (w != nil) != tc.wantWarn {
				t.Fatalf("warning = %+v, want present=%v", w, tc.wantWarn)
			}
			if w != nil && !strings.Contains(w.Message, filepath.Join(root, "backups")) {
				t.Errorf("warning message %q does not name the directory", w.Message)
			}
		})
	}
}
