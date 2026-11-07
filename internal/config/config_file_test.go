package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// findShadowWarning returns the #1466 shadowing warning, or nil.
func findShadowWarning(cfg *Config) *LoadWarning {
	for i := range cfg.Warnings {
		if cfg.Warnings[i].Key == "config file" {
			return &cfg.Warnings[i]
		}
	}
	return nil
}

// skipIfEtcArc skips when the machine has a packaged install: /etc/arc is in
// the search path and these tests cannot control it.
func skipIfEtcArc(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/etc/arc"); err == nil {
		t.Skip("/etc/arc exists on this machine")
	}
}

// homeConfig points $HOME at a temp dir holding .arc/arc.toml and runs from
// an empty working directory, so the file is found through the search path,
// as /etc/arc/arc.toml is on a packaged install.
func homeConfig(t *testing.T) (configPath, workDir string) {
	t.Helper()
	home := t.TempDir()
	configPath = filepath.Join(home, ".arc", "arc.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("[server]\nport = 8000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	workDir = t.TempDir()
	t.Chdir(workDir)
	return configPath, workDir
}

// TestConfigFileIsTheFileLoadRead (#1466): backup used "arc.toml" relative to
// the working directory, which is not the file when it was found elsewhere in
// the search path.
func TestConfigFileIsTheFileLoadRead(t *testing.T) {
	skipIfEtcArc(t)
	configPath, _ := homeConfig(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.EvalSymlinks(configPath)
	got, _ := filepath.EvalSymlinks(cfg.ConfigFile)
	if got != want || !filepath.IsAbs(cfg.ConfigFile) {
		t.Fatalf("ConfigFile = %q, want the absolute path of %q", cfg.ConfigFile, configPath)
	}
	if w := findShadowWarning(cfg); w != nil {
		t.Errorf("one config file drew a shadowing warning: %+v", w)
	}
}

func TestConfigFileEmptyWithoutAConfigFile(t *testing.T) {
	skipIfEtcArc(t)
	t.Setenv("HOME", t.TempDir())
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigFile != "" {
		t.Fatalf("ConfigFile = %q, want empty", cfg.ConfigFile)
	}
}

// TestShadowedConfigFileWarns: an arc.toml in the working directory wins over
// the one further down the search path, which is what restore_config before
// 27.01.1 left on packaged installs (/var/lib/arc/arc.toml over
// /etc/arc/arc.toml).
func TestShadowedConfigFileWarns(t *testing.T) {
	skipIfEtcArc(t)
	homePath, workDir := homeConfig(t)
	shadow := filepath.Join(workDir, "arc.toml")
	if err := os.WriteFile(shadow, []byte("[server]\nport = 8000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	w := findShadowWarning(cfg)
	if w == nil {
		t.Fatal("no shadowing warning")
	}
	if !strings.Contains(w.Message, homePath) {
		t.Errorf("warning %q does not name the shadowed %q", w.Message, homePath)
	}
	if got, _ := filepath.EvalSymlinks(cfg.ConfigFile); got != mustEval(t, shadow) {
		t.Errorf("loaded %q, want the working directory's %q", cfg.ConfigFile, shadow)
	}
}

// TestSymlinkedWorkDirDoesNotShadowItself: a working directory that is a
// symlink to the config's own directory finds the same file twice under two
// names; os.SameFile, not path comparison, keeps that quiet.
func TestSymlinkedWorkDirDoesNotShadowItself(t *testing.T) {
	skipIfEtcArc(t)
	homePath, _ := homeConfig(t)
	link := filepath.Join(t.TempDir(), "cwd")
	if err := os.Symlink(filepath.Dir(homePath), link); err != nil {
		t.Fatal(err)
	}
	t.Chdir(link)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if w := findShadowWarning(cfg); w != nil {
		t.Errorf("the same file under two names drew a warning: %+v", w)
	}
}

func mustEval(t *testing.T, p string) string {
	t.Helper()
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
