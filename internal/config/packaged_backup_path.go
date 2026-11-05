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
//   - backups an earlier release wrote to /var/lib/arc/data/backups stay in
//     the storage root, unlisted, where the data listing picks them up.
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

// backupLocalPathOverrideWarning reports a backup.local_path written in the
// config file that ARC_BACKUP_LOCAL_PATH overrides.
//
// Silent when the file still holds the shipped default: every packaged install
// has that line, so warning on it would fire on every boot of every package.
// Silent when the two resolve to the same directory, when backup is off, and
// when default_target is set, because local_path is then never read.
//
// The file's own value has to be read from a second, environment-free viper:
// with AutomaticEnv, v.GetString returns the environment's value, and viper has
// no accessor for the file's.
func backupLocalPathOverrideWarning(v *viper.Viper, cfg *Config) *LoadWarning {
	if !cfg.Backup.Enabled || strings.TrimSpace(cfg.Backup.DefaultTarget) != "" {
		return nil
	}
	if !v.InConfig("backup.local_path") || v.ConfigFileUsed() == "" {
		return nil
	}
	// viper ignores an empty variable (AllowEmptyEnv is off), so an empty
	// value overrides nothing.
	env := strings.TrimSpace(os.Getenv(backupLocalPathEnv))
	if env == "" {
		return nil
	}
	fv := viper.New()
	fv.SetConfigFile(v.ConfigFileUsed())
	fv.SetConfigType("toml")
	if err := fv.ReadInConfig(); err != nil {
		return nil
	}
	file := strings.TrimSpace(fv.GetString("backup.local_path"))
	if file == "" || filepath.Clean(file) == filepath.Clean(defaultBackupLocalPath) {
		return nil
	}
	if storage.ResolveExistingPath(file) == storage.ResolveExistingPath(env) {
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

// legacyBackupsInStorageRootWarning reports Arc backups inside the storage
// root at <storage.local_path>/backups — where a packaged install of an earlier
// release wrote them. A directory counts only if a child holds a manifest.json,
// the file every backup writes, so a database that happens to be named
// "backups" does not trigger it.
//
// Checked whatever backup.enabled says: the hazard is the data listing and the
// reconciliation sweep, which run either way.
func legacyBackupsInStorageRootWarning(cfg *Config) *LoadWarning {
	if cfg.Storage.Backend != "local" || strings.TrimSpace(cfg.Storage.LocalPath) == "" {
		return nil
	}
	dir := filepath.Join(storage.ResolveExistingPath(cfg.Storage.LocalPath), "backups")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		st, err := os.Stat(filepath.Join(dir, e.Name(), "manifest.json"))
		if err != nil || !st.Mode().IsRegular() {
			continue
		}
		return &LoadWarning{
			Key:   "storage.local_path",
			Value: cfg.Storage.LocalPath,
			Message: dir + " holds Arc backups inside the storage root, left by an earlier release (#1463). " +
				"The backup API does not list them, the next backup copies them as data, and on a cluster with reconciliation enabled and manifest_only_dry_run off the sweep deletes them. " +
				"Stop Arc and move the backup directories into backup.local_path",
		}
	}
	return nil
}
