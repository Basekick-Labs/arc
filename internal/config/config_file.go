package config

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// configSearchPaths are the directories Load searches for arc.<ext>, in
// viper's order: the first directory holding one wins. Shared by Load and
// shadowedConfigFilesWarning so the warning cannot drift from the search.
var configSearchPaths = []string{".", "/etc/arc/", "$HOME/.arc/"}

// configSearchDir expands a configSearchPaths entry the way viper does: $VARs
// expanded, then made absolute against the working directory.
func configSearchDir(p string) string {
	abs, err := filepath.Abs(os.ExpandEnv(p))
	if err != nil {
		return filepath.Clean(p)
	}
	return abs
}

// shadowedConfigFilesWarning reports config files that exist in the search
// path but lose to the one Load read.
//
// The case it exists for (#1466): before 27.01.1 restore_config wrote
// arc.toml into the working directory instead of over the loaded file. On a
// packaged install that is /var/lib/arc/arc.toml, and since "." is searched
// first it has been the config ever since, with /etc/arc/arc.toml silently
// ignored. Files are compared with os.SameFile, so a working directory that is
// a symlink to /etc/arc does not report its own config.
func shadowedConfigFilesWarning(used string) *LoadWarning {
	if used == "" {
		return nil
	}
	usedInfo, err := os.Stat(used)
	if err != nil {
		return nil
	}
	var shadowed []string
	seen := map[string]bool{}
	for _, p := range configSearchPaths {
		dir := configSearchDir(p)
		if seen[dir] {
			continue
		}
		seen[dir] = true
		for _, ext := range viper.SupportedExts {
			candidate := filepath.Join(dir, "arc."+ext)
			info, err := os.Stat(candidate)
			if err != nil || !info.Mode().IsRegular() || os.SameFile(info, usedInfo) {
				continue
			}
			shadowed = append(shadowed, candidate)
		}
	}
	if len(shadowed) == 0 {
		return nil
	}
	verb := " also exists and is ignored. "
	if len(shadowed) > 1 {
		verb = " also exist and are ignored. "
	}
	return &LoadWarning{
		Key:   "config file",
		Value: used,
		Message: "Arc loaded " + used + "; " + strings.Join(shadowed, ", ") + verb +
			"A restore_config before 27.01.1 wrote arc.toml into the working directory, where it takes precedence (#1466): " +
			"if that is how " + used + " got there, merge what you need into the file you maintain and remove " + used,
	}
}
