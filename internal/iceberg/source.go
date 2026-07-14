package iceberg

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// Measurement identifies one Arc table to export.
type Measurement struct {
	Database    string
	Measurement string
}

// FileSetSource enumerates the Arc data files the Iceberg tables should mirror. It is the
// reconciler's view of Arc's durable state. The default implementation walks the storage
// backend directly (works in OSS and cluster, with or without tiering), which is why the
// exporter has no dependency on the tiering metadata store or the Raft manifest being enabled.
type FileSetSource interface {
	// Measurements returns every (database, measurement) that currently has data files.
	Measurements(ctx context.Context) ([]Measurement, error)
	// Files returns the current data files for one measurement, as iceberg-readable URIs.
	Files(ctx context.Context, m Measurement) ([]FileRef, error)
}

// StorageWalkSource lists files straight from the storage backend. Arc's layout is
// {database}/{measurement}/{year}/{month}/{day}/{hour}/{file}.parquet, so databases are the
// top-level directories and measurements are the second level.
type StorageWalkSource struct {
	backend  storage.Backend
	resolver *PathResolver
	nsPrefix string // Iceberg namespace prefix; warehouse dirs "<nsPrefix>_*.db" are excluded
}

// NewStorageWalkSource builds a storage-walking file-set source. nsPrefix is the exporter's
// namespace prefix (e.g. "arc"); it must match so the walk skips the Iceberg warehouse
// directories the exporter writes under the same storage root ("<nsPrefix>_<db>.db/…") — those
// are Arc's own table metadata, NOT user databases, and must never be enumerated as tables.
func NewStorageWalkSource(backend storage.Backend, nsPrefix string) *StorageWalkSource {
	if nsPrefix == "" {
		nsPrefix = "arc"
	}
	return &StorageWalkSource{backend: backend, resolver: NewPathResolver(backend), nsPrefix: nsPrefix}
}

// isWarehouseDir reports whether a top-level directory is an Iceberg warehouse namespace
// directory ("<nsPrefix>_<db>.db") written by the exporter, which must be excluded from the
// data-file walk. Iceberg's SQL catalog names namespace dirs "<namespace>.db".
func (s *StorageWalkSource) isWarehouseDir(name string) bool {
	return strings.HasPrefix(name, s.nsPrefix+"_") && strings.HasSuffix(name, ".db")
}

// dirLister is the subset of storage backends that can enumerate immediate subdirectories.
type dirLister interface {
	ListDirectories(ctx context.Context, prefix string) ([]string, error)
}

// Measurements enumerates (database, measurement) pairs from the top two directory levels.
func (s *StorageWalkSource) Measurements(ctx context.Context) ([]Measurement, error) {
	dl, ok := s.backend.(dirLister)
	if !ok {
		return nil, fmt.Errorf("storage backend does not support directory listing")
	}
	dbs, err := dl.ListDirectories(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	// ListDirectories returns base names (not prefixed paths) and already skips hidden dirs.
	var out []Measurement
	for _, db := range dbs {
		db = strings.Trim(db, "/")
		if db == "" || s.isWarehouseDir(db) {
			continue // skip empty + the exporter's own warehouse namespace dirs
		}
		measurements, err := dl.ListDirectories(ctx, db+"/")
		if err != nil {
			return nil, fmt.Errorf("list measurements for %q: %w", db, err)
		}
		for _, m := range measurements {
			m = strings.Trim(m, "/")
			if m == "" {
				continue
			}
			out = append(out, Measurement{Database: db, Measurement: m})
		}
	}
	return out, nil
}

// Files lists the current .parquet files for a measurement and resolves them to URIs.
func (s *StorageWalkSource) Files(ctx context.Context, m Measurement) ([]FileRef, error) {
	files, _, err := s.FilesAndLocal(ctx, m)
	return files, err
}

// LocalFiles returns on-disk paths to ALL of the measurement's Parquet files for schema
// derivation (the reconciler unions their schemas — Arc's per-measurement schema can evolve,
// so one sample file is not enough; see UnionSchema). Returns empty if the backend is not
// local or the measurement has no local files. Cold-only measurements yield no local files
// and are skipped for schema derivation in v1 (documented limitation).
func (s *StorageWalkSource) LocalFiles(ctx context.Context, m Measurement) ([]string, error) {
	_, local, err := s.FilesAndLocal(ctx, m)
	return local, err
}

// FilesAndLocal lists the measurement's Parquet files ONCE and returns both the iceberg-readable
// URIs and the on-disk local paths, so the reconciler doesn't pay for two identical backend
// List() calls per measurement per pass. The two slices are aligned only in the sense that every
// local path corresponds to a data file also present in `files`; local is empty when the backend
// is non-local. Files() and LocalFiles() delegate here for callers that need just one view.
func (s *StorageWalkSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	prefix := m.Database + "/" + m.Measurement + "/"
	paths, err := s.backend.List(ctx, prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("list files for %s/%s: %w", m.Database, m.Measurement, err)
	}
	var files []FileRef
	var local []string
	for _, p := range paths {
		if !isDataFile(p) {
			continue
		}
		files = append(files, FileRef{PhysicalPath: s.resolver.Resolve(p)})
		if lp := s.resolver.LocalPath(p); lp != "" {
			local = append(local, lp)
		}
	}
	return files, local, nil
}

// isDataFile reports whether a storage key is an Arc data file to export. Only .parquet is
// exported; vortex is a separate (shelved) format and Iceberg's data files are Parquet.
func isDataFile(p string) bool {
	// filepath.Base (not path.Base): storage keys from a local backend on Windows are
	// backslash-separated (LocalBackend.List uses filepath.Rel without ToSlash), and Iceberg
	// export is local-only. filepath.Base handles both separators; path.Base would treat a
	// backslash key as one component.
	base := filepath.Base(p)
	// Skip dotfiles. This covers Arc's in-flight ".tmp.*" writes (a separate ".tmp." check would
	// be redundant — it already starts with "."), so a partially-written file is never registered.
	if strings.HasPrefix(base, ".") {
		return false
	}
	return strings.HasSuffix(base, ".parquet")
}
