package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// configRig is a standalone node with one data file, a local backup
// directory, and its config file in a directory of its own, away from the
// working directory: the packaged layout, where the config is
// /etc/arc/arc.toml and the working directory /var/lib/arc (#1466).
type configRig struct {
	m          *Manager
	dataDir    string
	configPath string
	workDir    string
}

const dataFile = "prod/cpu/2026/10/07/00/a.parquet"

func newConfigRig(t *testing.T, configPath string) *configRig {
	t.Helper()
	return newConfigRigAt(t, configPath, t.TempDir())
}

// newConfigRigAt is newConfigRig with the backup directory given, so a backup
// taken by one rig can be restored by another.
func newConfigRigAt(t *testing.T, configPath, backupPath string) *configRig {
	t.Helper()
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, filepath.Dir(dataFile)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, dataFile), []byte("PAR1"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := storage.NewLocalBackend(dataDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { data.Close() })

	workDir := t.TempDir()
	t.Chdir(workDir)

	m, err := NewManager(&ManagerConfig{
		DataStorage: data,
		BackupPath:  backupPath,
		ConfigPath:  configPath,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &configRig{m: m, dataDir: dataDir, configPath: configPath, workDir: workDir}
}

// writeConfig creates a config file in a fresh directory and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "arc.toml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustRead(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func (r *configRig) backup(t *testing.T, includeConfig bool) string {
	t.Helper()
	res, err := r.m.CreateBackup(context.Background(), BackupOptions{IncludeConfig: includeConfig})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	return res.Manifest.BackupID
}

// TestConfigBackupAndRestoreUseTheLoadedFile is the #1466 bug: both ends used
// "arc.toml" relative to the working directory, so a node whose config was
// found through the search path backed up no config and restored one into
// the working directory, where it took precedence over the real file.
func TestConfigBackupAndRestoreUseTheLoadedFile(t *testing.T) {
	r := newConfigRig(t, writeConfig(t, "# original\n"))

	id := r.backup(t, true)
	got, err := r.m.GetBackup(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !got.HasConfig {
		t.Fatal("the backup holds no config: include_config did not read the loaded file")
	}

	if err := os.WriteFile(r.configPath, []byte("# edited after the backup\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := r.m.RestoreBackup(context.Background(), RestoreOptions{BackupID: id, RestoreConfig: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	if s := mustRead(t, r.configPath); s != "# original\n" {
		t.Errorf("config after restore = %q, want the backup's", s)
	}
	if s := mustRead(t, configSafetyCopyPath(r.configPath, id)); s != "# edited after the backup\n" {
		t.Errorf("safety copy = %q, want the config the restore replaced", s)
	}
	if _, err := os.Stat(filepath.Join(r.workDir, "arc.toml")); !os.IsNotExist(err) {
		t.Errorf("restore wrote arc.toml into the working directory (stat err %v)", err)
	}
}

// TestSecondConfigRestoreKeepsTheFirstSafetyCopy: one fixed safety-copy name
// meant a second restore overwrote the copy of the operator's original config
// with the first restore's result.
func TestSecondConfigRestoreKeepsTheFirstSafetyCopy(t *testing.T) {
	r := newConfigRig(t, writeConfig(t, "# A\n"))
	idA := r.backup(t, true)
	if err := os.WriteFile(r.configPath, []byte("# B\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	idB := r.backup(t, true)
	if idA == idB {
		t.Skip("two backups in one second share an id")
	}
	if err := os.WriteFile(r.configPath, []byte("# operator original\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{idA, idB} {
		if _, err := r.m.RestoreBackup(context.Background(), RestoreOptions{BackupID: id, RestoreConfig: true}); err != nil {
			t.Fatalf("RestoreBackup(%s): %v", id, err)
		}
	}
	if s := mustRead(t, configSafetyCopyPath(r.configPath, idA)); s != "# operator original\n" {
		t.Errorf("first safety copy = %q, want the operator's original", s)
	}
	if s := mustRead(t, r.configPath); s != "# B\n" {
		t.Errorf("config = %q, want the second backup's", s)
	}
}

// TestConfigRestoreRefusedBeforeAnyDataIsRestored: the config step runs after
// data and metadata, so a config that cannot be written used to fail the
// restore after the data was already back. Refused up front now, and only when
// the backup holds a config.
func TestConfigRestoreRefusedBeforeAnyDataIsRestored(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(t *testing.T) string // returns the config path
		reason string
	}{
		{"no config file loaded", func(t *testing.T) string { return "" }, "without a config file"},
		{"directory refuses the safety copy", func(t *testing.T) string {
			if os.Geteuid() == 0 {
				t.Skip("root passes access(2) on a read-only directory")
			}
			p := writeConfig(t, "# current\n")
			dir := filepath.Dir(p)
			if err := os.Chmod(dir, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.Chmod(dir, 0o700) })
			return p
		}, "keeps a copy of the current config"},
		{"file not writable", func(t *testing.T) string {
			if os.Geteuid() == 0 {
				t.Skip("root passes access(2) on a read-only file")
			}
			p := writeConfig(t, "# current\n")
			if err := os.Chmod(p, 0o444); err != nil {
				t.Fatal(err)
			}
			return p
		}, "is not writable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The backup is taken with a config the rig can read; the
			// restore then runs on a node in the case's state.
			backupPath := t.TempDir()
			src := newConfigRigAt(t, writeConfig(t, "# backed up\n"), backupPath)
			id := src.backup(t, true)

			r := newConfigRigAt(t, tc.setup(t), backupPath)
			if err := os.Remove(filepath.Join(r.dataDir, dataFile)); err != nil {
				t.Fatal(err)
			}

			_, err := r.m.RestoreBackup(context.Background(), RestoreOptions{BackupID: id, RestoreData: true, RestoreConfig: true})
			if !errors.Is(err, ErrConfigRestoreUnavailable) || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("RestoreBackup error = %v, want ErrConfigRestoreUnavailable mentioning %q", err, tc.reason)
			}
			if _, err := os.Stat(filepath.Join(r.dataDir, dataFile)); !os.IsNotExist(err) {
				t.Errorf("data was restored before the refusal (stat err %v)", err)
			}
		})
	}
}

// TestConfigRestoreOfABackupWithoutConfigIsNotRefused: restore_config on a
// backup that holds no config skips the step, as it always did, even on a
// node that could not write one.
func TestConfigRestoreOfABackupWithoutConfigIsNotRefused(t *testing.T) {
	r := newConfigRig(t, "")
	id := r.backup(t, false)
	if _, err := r.m.RestoreBackup(context.Background(), RestoreOptions{BackupID: id, RestoreConfig: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
}
