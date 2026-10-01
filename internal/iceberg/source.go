package iceberg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"

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
	// pendingOutputs, when set, names compaction outputs whose sources still
	// exist; they are excluded from the file set (#638).
	pendingOutputs func(ctx context.Context, prefix string) (map[string]struct{}, error)
	logger         zerolog.Logger

	// namespaceExpander, when set, returns the top-level directories that are
	// edge-sync spoke namespaces rather than databases (#634). A hub stores
	// received data at {spoke_id}/{db}/{meas}/…, one level deeper than local
	// data, so without expansion the walk reads {spoke}/{db} as
	// (database, measurement) and unions every measurement under it into a
	// single franken-table. Mirrors the compaction manager's expander (#619).
	namespaceExpander func(ctx context.Context) (map[string]struct{}, error)
}

// SetPendingOutputs installs the lookup for compaction outputs that are not
// yet committed under a "{database}/{measurement}/" prefix (see
// compaction.ManifestManager.PendingOutputsUnder). Those keys are left out of
// the file set so a reconcile pass that lands between a compaction's upload
// and its source deletion does not register the compacted file next to the
// raws it replaces (#638). nil means no compaction state to consult.
func (s *StorageWalkSource) SetPendingOutputs(fn func(ctx context.Context, prefix string) (map[string]struct{}, error)) {
	s.pendingOutputs = fn
}

// SetNamespaceExpander installs edge-sync spoke-namespace expansion (#634).
// Wired on a hub so received data is exported as the tables it actually is
// instead of one garbage table per spoke namespace.
func (s *StorageWalkSource) SetNamespaceExpander(fn func(ctx context.Context) (map[string]struct{}, error)) {
	s.namespaceExpander = fn
}

// NewStorageWalkSource builds a storage-walking file-set source. nsPrefix is the exporter's
// namespace prefix (e.g. "arc"); it must match so the walk skips the Iceberg warehouse
// directories the exporter writes under the same storage root ("<nsPrefix>_<db>.db/…") — those
// are Arc's own table metadata, NOT user databases, and must never be enumerated as tables.
func NewStorageWalkSource(backend storage.Backend, nsPrefix string, logger zerolog.Logger) *StorageWalkSource {
	if nsPrefix == "" {
		nsPrefix = "arc"
	}
	return &StorageWalkSource{
		backend:  backend,
		resolver: NewPathResolver(backend),
		nsPrefix: nsPrefix,
		logger:   logger,
	}
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
	// Expand edge-sync spoke namespaces into {spoke}/{db} pseudo-databases
	// before walking, so their measurements are discovered at the right depth
	// (#634).
	dbs = s.expandNamespaces(ctx, dl, dbs)

	var out []Measurement
	for _, db := range dbs {
		db = strings.Trim(db, "/")
		if db == "" || s.isWarehouseDir(db) || db == compactionStateDir || storage.IsReservedRootDir(db) {
			continue // skip empty, the exporter's own warehouse namespace dirs, and Arc's reserved state dirs (_compaction_state, _schema)
		}
		measurements, err := dl.ListDirectories(ctx, db+"/")
		if err != nil {
			s.logger.Error().Err(err).Str("database", db).
				Msg("Iceberg reconcile: failed to enumerate database measurements; skipping database")
			continue
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

// expandNamespaces replaces registered edge-sync spoke namespaces in a
// top-level directory list with {spoke}/{child} pseudo-databases (#634),
// mirroring what the compaction manager does for the same layout (#619).
//
// A hub stores received data one level deeper than local data:
// {spoke_id}/{db}/{meas}/{y}/{m}/{d}/{h}/*.parquet. Walked flat, {spoke}/{db}
// reads as (database, measurement) and every measurement underneath is unioned
// into one table with a franken-schema — or fails schema union on a
// cross-measurement type collision and logs an error every pass forever.
//
// A bare spoke directory yields nothing on its own once expanded, so it is
// dropped. Expander errors are fail-safe: spoke directories are SKIPPED for
// this pass rather than exported unexpanded, because exporting them wrong
// mints catalog tables that then have to be cleaned up by hand.
func (s *StorageWalkSource) expandNamespaces(ctx context.Context, dl dirLister, dirs []string) []string {
	if s.namespaceExpander == nil {
		return dirs
	}
	spokes, err := s.namespaceExpander(ctx)
	if err != nil {
		s.logger.Warn().Err(err).
			Msg("Iceberg reconcile: spoke-namespace lookup failed; skipping received namespaces this pass (fail-safe)")
		spokes = nil
	}
	if len(spokes) == 0 {
		return dirs
	}

	out := make([]string, 0, len(dirs))
	for _, d := range dirs {
		name := strings.Trim(d, "/")
		if _, isSpoke := spokes[name]; !isSpoke {
			out = append(out, d)
			continue
		}
		children, err := dl.ListDirectories(ctx, name+"/")
		if err != nil {
			s.logger.Warn().Err(err).Str("spoke", name).
				Msg("Iceberg reconcile: could not list a spoke namespace; skipping it this pass")
			continue
		}
		for _, child := range children {
			child = strings.Trim(child, "/")
			if child == "" || storage.IsReservedRootDir(child) {
				continue
			}
			out = append(out, name+"/"+child)
		}
	}
	return out
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

const unusableSampleCap = 32

// FilesAndLocal lists the measurement's Parquet files ONCE and returns both the iceberg-readable
// URIs and the on-disk local paths, so the reconciler doesn't pay for two identical backend
// List() calls per measurement per pass. The two slices are aligned only in the sense that every
// local path corresponds to a data file also present in `files`; local is empty when the backend
// is non-local. Files() and LocalFiles() delegate here for callers that need just one view.
func (s *StorageWalkSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	prefix := m.Database + "/" + m.Measurement + "/"
	if ul, ok := s.backend.(storage.UnusableLister); ok {
		// Enumerate hidden files first: a hidden-to-usable rename between the
		// two passes may cause a spurious refusal, but never a silent omission.
		unusable, err := ul.ListUnusable(ctx, prefix)
		if err != nil {
			return nil, nil, fmt.Errorf("check hidden files for %s/%s: %w", m.Database, m.Measurement, err)
		}
		var hiddenData []storage.UnusableObject
		for _, obj := range unusable {
			if isDataFile(obj.Path) {
				hiddenData = append(hiddenData, obj)
			}
		}
		if len(hiddenData) > 0 {
			sampleCount := len(hiddenData)
			if sampleCount > unusableSampleCap {
				sampleCount = unusableSampleCap
			}
			samples := make([]string, 0, sampleCount)
			for i, obj := range hiddenData {
				if i >= unusableSampleCap {
					break
				}
				reason := "unknown reason"
				if obj.Err != nil {
					reason = obj.Err.Error()
				}
				samples = append(samples, fmt.Sprintf("%s: %s", obj.Path, reason))
			}
			return nil, nil, fmt.Errorf(
				"refusing Iceberg export for %s/%s: %d data file(s) are hidden from the normal storage listing, so publishing the table would be incomplete; rename the files, and the next reconcile pass will publish. Sample(s): %s",
				m.Database, m.Measurement, len(hiddenData), strings.Join(samples, "; "),
			)
		}
	}
	paths, err := s.backend.List(ctx, prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("list files for %s/%s: %w", m.Database, m.Measurement, err)
	}
	// Read the compaction state AFTER the listing and BEFORE the stat loop.
	// The order is what makes this race-free: a compaction deletes its
	// manifest only after every source file is gone, so a manifest absent here
	// means the sources were deleted before the stats below, which drop them;
	// a manifest present here means its output is excluded. Reading before the
	// listing would miss an upload that lands in between; reading after the
	// stats would let a compaction that finishes in between leave both the
	// (already registered) raws and its output in the set (#638).
	var pending map[string]struct{}
	if s.pendingOutputs != nil {
		pending, err = s.pendingOutputs(ctx, prefix)
		if err != nil {
			return nil, nil, fmt.Errorf("pending compaction outputs for %s/%s: %w", m.Database, m.Measurement, err)
		}
	}
	var files []FileRef
	var local []string
	excluded := 0
	for _, p := range paths {
		if !isDataFile(p) {
			continue
		}
		if _, skip := pending[filepath.ToSlash(p)]; skip {
			excluded++
			continue
		}
		uri, err := s.resolver.Resolve(p)
		if err != nil {
			// The listing produced a key this backend would itself refuse.
			// Fail rather than skip: a skipped file is missing from the
			// exported table with no other signal.
			return nil, nil, fmt.Errorf("listed file %q has no usable storage path: %w", p, err)
		}
		lp := s.resolver.LocalPath(p)
		size, present, err := s.statSize(ctx, p, lp)
		if err != nil {
			return nil, nil, fmt.Errorf("stat listed file %q: %w", p, err)
		}
		if !present {
			// Removed between the listing and the stat (retention, compaction, the
			// delete API). It is not on storage, so it is not in this pass's set;
			// leaving it out of BOTH slices keeps the schema derivation from
			// opening a path that no longer exists. The next pass converges.
			s.logger.Debug().Str("file", p).Msg("Iceberg reconcile: file vanished after listing; skipping it this pass")
			continue
		}
		files = append(files, FileRef{PhysicalPath: uri, SizeBytes: size})
		if lp != "" {
			local = append(local, lp)
		}
	}
	if excluded > 0 {
		s.logger.Debug().Str("database", m.Database).Str("measurement", m.Measurement).Int("excluded", excluded).
			Msg("Iceberg reconcile: compaction outputs still replacing their sources are left out of this pass")
	}
	return files, local, nil
}

// statSize returns the file's current byte size, which the reconciler needs
// because a file rewritten in place keeps its path (#633): the delete API's
// partial-match branch renames a smaller file over the original, and only the
// size tells that content apart from what the Iceberg manifest describes.
// present=false means the file is gone. Local files are stat'ed directly; any
// other backend answers through StatFile (Iceberg export is local-only, so
// that branch only serves wrapped backends in tests — note LocalBackend.StatFile
// reports a ".part" staging file's size when the final file is absent, which
// os.Stat on the final path does not).
func (s *StorageWalkSource) statSize(ctx context.Context, key, localPath string) (size int64, present bool, err error) {
	if localPath != "" {
		st, err := os.Stat(localPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return 0, false, nil
			}
			return 0, false, err
		}
		return st.Size(), true, nil
	}
	size, err = s.backend.StatFile(ctx, key)
	if err != nil {
		return 0, false, err
	}
	if size < 0 {
		return 0, false, nil
	}
	return size, true, nil
}

// compactionStateDir is compaction.ManifestBasePath, spelled here so this
// package does not import internal/compaction: it is a top-level directory of
// the storage root that holds crash-recovery manifests, not a database.
const compactionStateDir = "_compaction_state"

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
