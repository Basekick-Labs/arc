package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// findWarning returns the load warning whose message contains substr, or nil.
// By message, not key: both #1463 warnings are about backup.local_path.
func findWarning(cfg *Config, substr string) *LoadWarning {
	for i := range cfg.Warnings {
		if strings.Contains(cfg.Warnings[i].Message, substr) {
			return &cfg.Warnings[i]
		}
	}
	return nil
}

const (
	overrideMsg = "is overridden by"
	legacyMsg   = "holds Arc backups left by an earlier release"
)

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
	if w := findWarning(cfg, overrideMsg); w != nil {
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
			w := findWarning(cfg, overrideMsg)
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

func TestLegacyBackupsWarning(t *testing.T) {
	cases := []struct {
		name        string
		backend     string            // storage backend; "" = local
		layout      map[string]string // path under the working directory -> file content
		wantWarn    bool
		wantInsides bool // the warning says they are inside the storage root
	}{
		{"old packaged backups, local primary", "", map[string]string{"data/backups/20260901_000000_ab12/manifest.json": "{}"}, true, true},
		// The S3-primary packaged install booted before #1463 and wrote here;
		// the storage-root scan cannot see it, the file-path scan must.
		{"old packaged backups, s3 primary", "s3", map[string]string{"data/backups/20260901_000000_ab12/manifest.json": "{}"}, true, false},
		{"database named backups", "", map[string]string{"data/backups/cpu/2026/09/01/00/x.parquet": ""}, false, false},
		{"nothing there", "", map[string]string{"data/mydb/cpu/2026/09/01/00/x.parquet": ""}, false, false},
		{"backups already in the current directory", "", map[string]string{"backups/20260901_000000_ab12/manifest.json": "{}"}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeArcToml(t, "[backup]\nlocal_path = \"./data/backups\"\n")
			wd, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			for p, body := range tc.layout {
				full := filepath.Join(wd, p)
				if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.backend == "s3" {
				t.Setenv("ARC_STORAGE_BACKEND", "s3")
				t.Setenv("ARC_STORAGE_S3_BUCKET", "acme-arc")
			} else {
				t.Setenv("ARC_STORAGE_LOCAL_PATH", filepath.Join(wd, "data"))
			}
			t.Setenv("ARC_BACKUP_LOCAL_PATH", filepath.Join(wd, "backups"))
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load(): %v", err)
			}
			w := findWarning(cfg, legacyMsg)
			if (w != nil) != tc.wantWarn {
				t.Fatalf("warning = %+v, want present=%v", w, tc.wantWarn)
			}
			if w == nil {
				return
			}
			for _, want := range []string{filepath.Join(wd, "data", "backups"), "move the backup directories into " + filepath.Join(wd, "backups")} {
				if !strings.Contains(w.Message, want) {
					t.Errorf("warning message %q lacks %q", w.Message, want)
				}
			}
			if got := strings.Contains(w.Message, "inside the storage root"); got != tc.wantInsides {
				t.Errorf("message says inside the storage root = %v, want %v: %q", got, tc.wantInsides, w.Message)
			}
		})
	}
}

// TestShippedUnitBootsWithShippedArcToml loads the repo arc.toml the way the
// packaged unit runs Arc: from the unit's WorkingDirectory, with the unit's
// Environment lines, both read from deploy/systemd/arc.service. It is the
// PR-time stand-in for the release pipeline's package smoke tests, which run
// only on a release: remove the unit's ARC_BACKUP_LOCAL_PATH line and this
// fails with the #1463 refusal.
func TestShippedUnitBootsWithShippedArcToml(t *testing.T) {
	unit, err := os.ReadFile(filepath.Join("..", "..", "deploy", "systemd", "arc.service"))
	if err != nil {
		t.Fatal(err)
	}
	toml, err := os.ReadFile(filepath.Join("..", "..", "arc.toml"))
	if err != nil {
		t.Fatal(err)
	}

	// The unit's absolute paths are re-rooted under a temp dir standing in
	// for /var/lib/arc.
	var workDir string
	var env [][2]string
	for _, line := range strings.Split(string(unit), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "WorkingDirectory="):
			workDir = strings.TrimPrefix(line, "WorkingDirectory=")
		case strings.HasPrefix(line, "Environment="):
			kv := strings.Trim(strings.TrimPrefix(line, "Environment="), `"`)
			k, val, ok := strings.Cut(kv, "=")
			if !ok {
				t.Fatalf("unparseable unit line %q", line)
			}
			env = append(env, [2]string{k, val})
		}
	}
	if workDir == "" || len(env) == 0 {
		t.Fatalf("unit has no WorkingDirectory or Environment: %q", unit)
	}

	root := t.TempDir()
	reroot := func(p string) string {
		if p == workDir || strings.HasPrefix(p, workDir+"/") {
			return filepath.Join(root, strings.TrimPrefix(p, workDir))
		}
		return p
	}
	// Load searches "." before /etc/arc; the stand-in WorkingDirectory holds
	// the packaged arc.toml.
	if err := os.WriteFile(filepath.Join(root, "arc.toml"), toml, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	for _, kv := range env {
		t.Setenv(kv[0], reroot(kv[1]))
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with the shipped unit and arc.toml: %v", err)
	}
	if !cfg.Backup.Enabled {
		t.Fatal("shipped arc.toml has backup disabled; this test would prove nothing")
	}
	if storage.PathWithin(storage.ResolveExistingPath(cfg.Backup.LocalPath), storage.ResolveExistingPath(cfg.Storage.LocalPath)) {
		t.Fatalf("backup %q is inside storage %q", cfg.Backup.LocalPath, cfg.Storage.LocalPath)
	}
	if len(cfg.Warnings) != 0 {
		t.Fatalf("a fresh packaged install warns: %+v", cfg.Warnings)
	}
}
