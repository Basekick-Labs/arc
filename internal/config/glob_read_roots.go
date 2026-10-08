package config

import (
	"fmt"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// checkParquetReadRootsGlobSafe refuses a storage root or compaction temp
// directory whose path contains a DuckDB glob metacharacter.
//
// DuckDB treats a read_parquet() path as a PATTERN, not a literal name, and
// compaction interpolates a full filesystem path into it: since #969 the local
// backend hands DuckDB the input's real path, and the remote backend hands it a
// path under compaction.temp_directory. Go's os.Open is literal, so
// validateParquetFile and os.Stat agree a file exists while DuckDB resolves the
// same string to a DIFFERENT file - a sibling that matches the pattern - and
// compaction then deletes inputs it never read.
//
// internal/compaction guards the interpolation itself (storage.ValidateGlobSafe
// before the read), but a per-file guard is the wrong place to DISCOVER this:
// every input fails it, on every cycle, so compaction silently never progresses
// on a directory the operator chose deliberately. The metacharacters are
// */?[]{}, all legal in a POSIX directory name, so "/data/arc[prod]" is a
// plausible root rather than a pathological one. Refusing at startup turns a
// permanently degraded node into one that says why it will not run.
//
// Only the roots that reach a LOCAL read_parquet are checked. The storage root
// is checked for the local backend only: an S3 or Azure key is interpolated by
// the query path, which applies its own ValidateGlobSafe to the key, and the
// bucket prefix never reaches a local glob. compaction.temp_directory is
// checked whenever compaction is enabled, because the remote-backend download
// path streams inputs into it and then reads them from there.
func (c *Config) checkParquetReadRootsGlobSafe() error {
	if c.Storage.Backend == "local" {
		if err := storage.ValidateGlobSafe(c.Storage.LocalPath); err != nil {
			return fmt.Errorf("invalid storage.local_path %q: %w; DuckDB reads compaction inputs by "+
				"interpolating this path into read_parquet(), where a glob metacharacter can match a "+
				"different file than the one named. Rename the directory to remove it",
				c.Storage.LocalPath, err)
		}
	}
	if c.Compaction.Enabled && strings.TrimSpace(c.Compaction.TempDirectory) != "" {
		if err := storage.ValidateGlobSafe(c.Compaction.TempDirectory); err != nil {
			return fmt.Errorf("invalid compaction.temp_directory %q: %w; compaction interpolates paths "+
				"under this directory into read_parquet(), where a glob metacharacter can match a "+
				"different file than the one named. Rename the directory to remove it",
				c.Compaction.TempDirectory, err)
		}
	}
	return nil
}
