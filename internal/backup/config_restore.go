package backup

import (
	"errors"
	"fmt"
	"path/filepath"
)

// ErrConfigRestoreUnavailable is wrapped by every ConfigRestoreBlocker refusal,
// so a caller can tell "this node cannot restore its config" from other
// restore failures.
var ErrConfigRestoreUnavailable = errors.New("restore_config is not available on this node")

// configSafetyCopyPath is where restoreConfig keeps the config it is about to
// overwrite: beside it, named for the restore, so a second restore cannot
// overwrite the copy of the original.
func configSafetyCopyPath(configPath, backupID string) string {
	return configPath + ".before-restore-" + backupID
}

// ConfigRestoreBlocker reports why restoring the config of backupID would fail
// on this node, or nil. Checked before a restore writes anything (#1466): the
// config step runs after the data and metadata steps, so a failure there used
// to leave the data restored and the config not, with the operator told only
// that the restore failed.
//
// configPath is the file config.Load read (viper's ConfigFileUsed), not a
// path relative to the working directory. It is "" when Arc started without
// a config file, and then there is nothing to restore over: writing one into
// the working directory would make it the file every later start loads.
//
// The checks are access(2), with no side effects: the file must be writable,
// and its directory must accept the safety copy. On the .deb/.rpm/Arch
// packages /etc/arc is root's, so a config restore is refused there.
func (m *Manager) ConfigRestoreBlocker(backupID string) error {
	if m.configPath == "" {
		return fmt.Errorf("%w: Arc started without a config file, so there is no file to restore the backup's config over", ErrConfigRestoreUnavailable)
	}
	if err := checkWritable(m.configPath); err != nil {
		return fmt.Errorf("%w: the config file %s is not writable by this process (%v); restore it by hand from %s/config/arc.toml in the backup", ErrConfigRestoreUnavailable, m.configPath, err, backupID)
	}
	// The safety copy is created next to the config path itself, so a
	// symlinked config needs the link's directory, not the target's.
	dir := filepath.Dir(m.configPath)
	if err := checkDirAcceptsFile(dir); err != nil {
		return fmt.Errorf("%w: Arc keeps a copy of the current config before overwriting it, and %s does not accept %s (%v); restore it by hand from %s/config/arc.toml in the backup", ErrConfigRestoreUnavailable, dir, filepath.Base(configSafetyCopyPath(m.configPath, backupID)), err, backupID)
	}
	return nil
}
