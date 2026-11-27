package config

// Load-time advisories for the packaged (.deb/.rpm/Arch) backup layout, #1463.
//
// The packages' systemd unit runs Arc from /var/lib/arc with
// ARC_STORAGE_LOCAL_PATH=/var/lib/arc/data. arc.toml's relative
// "./data/backups" resolved from there is /var/lib/arc/data/backups, inside the
// storage root, which checkBackupDestinationOverlap refuses. The unit now also
// sets ARC_BACKUP_LOCAL_PATH=/var/lib/arc/backups. Two consequences are
// invisible without these warnings:
//
//   - an operator who changed backup.local_path in /etc/arc/arc.toml is
//     overridden by the unit, because the environment beats the file; and
//   - backups an earlier release wrote where the file pointed are no longer
//     in the backup directory. That is true with ANY primary backend: an
//     S3-primary packaged install booted before this fix, wrote its backups
//     to /var/lib/arc/data/backups, and loses them from the listing now.
//
// Both are warnings, never refusals: neither stops Arc doing what it was told.

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"

	"github.com/basekick-labs/arc/internal/storage"
)

const (
	// defaultBackupLocalPath is backup.local_path's default and the value the
	// stock arc.toml ships.
	defaultBackupLocalPath = "./data/backups"
	// backupLocalPathEnv is the variable the packaged systemd unit sets.
	backupLocalPathEnv = "ARC_BACKUP_LOCAL_PATH"
)

// fileBackupLocalPath returns backup.local_path as the config file alone sets
// it: the file's value, or the default when the file does not set it or there
// is no file. "" when the file cannot be re-read.
//
// A second, environment-free viper is needed because with AutomaticEnv
// v.GetString returns the environment's value, and viper has no accessor for
// the file's.
func fileBackupLocalPath(v *viper.Viper) string {
	if v.ConfigFileUsed() == "" || !v.InConfig("backup.local_path") {
		return defaultBackupLocalPath
	}
	fv := viper.New()
	fv.SetConfigFile(v.ConfigFileUsed())
	fv.SetConfigType("toml")
	if err := fv.ReadInConfig(); err != nil {
		return ""
	}
	return strings.TrimSpace(fv.GetString("backup.local_path"))
}

// backupEnvOverride reports the environment's backup.local_path and the
// file's, when the environment overrides the file with a different directory.
// viper ignores an empty variable (AllowEmptyEnv is off), so an empty value
// overrides nothing.
func backupEnvOverride(v *viper.Viper) (env, file string, ok bool) {
	env = strings.TrimSpace(os.Getenv(backupLocalPathEnv))
	if env == "" {
		return "", "", false
	}
	file = fileBackupLocalPath(v)
	if file == "" || storage.ResolveExistingPath(file) == storage.ResolveExistingPath(env) {
		return "", "", false
	}
	return env, file, true
}

// inLocalStorageRoot reports whether dir is inside a local primary storage
// root, where the overlap check would refuse it as a backup destination.
func inLocalStorageRoot(cfg *Config, dir string) bool {
	if cfg.Storage.Backend != "local" || strings.TrimSpace(cfg.Storage.LocalPath) == "" {
		return false
	}
	return storage.PathWithin(storage.ResolveExistingPath(dir), storage.ResolveExistingPath(cfg.Storage.LocalPath))
}

// backupLocalPathOverrideWarning reports a backup.local_path the operator wrote
// in the config file that ARC_BACKUP_LOCAL_PATH overrides.
//
// Silent when the file holds the shipped default: every packaged install has
// that line, so warning on it would fire on every boot of every package (and
// backups already written there are legacyBackupsWarning's job). Silent when
// the file's value is inside the storage root: keeping it is refused at boot,
// so "keep your directory" would be wrong advice. Silent when backup is off,
// and when default_target is set, because local_path is then never read.
func backupLocalPathOverrideWarning(v *viper.Viper, cfg *Config) *LoadWarning {
	if !cfg.Backup.Enabled || strings.TrimSpace(cfg.Backup.DefaultTarget) != "" {
		return nil
	}
	env, file, ok := backupEnvOverride(v)
	if !ok || filepath.Clean(file) == filepath.Clean(defaultBackupLocalPath) || inLocalStorageRoot(cfg, file) {
		return nil
	}
	return &LoadWarning{
		Key:   "backup.local_path",
		Value: file,
		Message: "backup.local_path in " + v.ConfigFileUsed() + " is overridden by " +
			backupLocalPathEnv + "=" + env + ", so backups are written there and backups in the configured directory are not listed. " +
			"The .deb/.rpm/Arch systemd unit sets this variable (#1463); to keep your directory, set it in a drop-in with `systemctl edit arc`",
	}
}

// legacyBackupsWarning reports Arc backups in a directory this boot does not
// use as its backup destination, in either of two places an earlier release
// wrote them:
//
//   - where the config file points, when ARC_BACKUP_LOCAL_PATH overrides it:
//     the packaged install's /var/lib/arc/data/backups, whatever the primary
//     backend; and
//   - <storage.local_path>/backups, for a local primary, where those backups
//     are also inside the storage root.
//
// A directory counts only if a child holds a manifest.json, the file every
// backup writes, so a database that happens to be named "backups" does not
// trigger it. Checked whatever backup.enabled says: inside the storage root
// the hazard is the data listing and the reconciliation sweep, which run
// either way.
func legacyBackupsWarning(v *viper.Viper, cfg *Config) *LoadWarning {
	current := ""
	if cfg.Backup.Enabled && strings.TrimSpace(cfg.Backup.DefaultTarget) == "" && cfg.Backup.LocalPath != "" {
		current = storage.ResolveExistingPath(cfg.Backup.LocalPath)
	}

	var candidates []string
	if _, file, ok := backupEnvOverride(v); ok {
		candidates = append(candidates, storage.ResolveExistingPath(file))
	}
	if cfg.Storage.Backend == "local" && strings.TrimSpace(cfg.Storage.LocalPath) != "" {
		candidates = append(candidates, filepath.Join(storage.ResolveExistingPath(cfg.Storage.LocalPath), "backups"))
	}

	for _, dir := range candidates {
		if dir == current || !holdsBackups(dir) {
			continue
		}
		// The operator's spelling, not the resolved path: it is what they
		// will recognise and type.
		dest := "the backup destination"
		if current != "" {
			dest = cfg.Backup.LocalPath
		}
		msg := dir + " holds Arc backups left by an earlier release (#1463), outside this boot's backup directory, so the backup API does not list them. "
		if inLocalStorageRoot(cfg, dir) {
			msg += "They are also inside the storage root: the next backup copies their data files as data, and on a cluster with reconciliation enabled and manifest_only_dry_run off the sweep deletes their data files. "
		}
		return &LoadWarning{
			Key:     "backup.local_path",
			Value:   dir,
			Message: msg + "Stop Arc and move the backup directories into " + dest,
		}
	}
	return nil
}

// holdsBackups reports whether some child directory of dir holds a
// manifest.json. Stat, not DirEntry.IsDir, so a symlinked backup directory
// counts.
func holdsBackups(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		st, err := os.Stat(filepath.Join(dir, e.Name(), "manifest.json"))
		if err == nil && st.Mode().IsRegular() {
			return true
		}
	}
	return false
}
