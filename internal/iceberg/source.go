package iceberg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

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
	// layout names the warehouse's directories in the storage root, which the walk skips. Never
	// nil: NewStorageWalkSource starts with the namespace-directory rule alone, and
	// SetWarehouseLayout adds a subdirectory or nested warehouse (#1626).
	layout *WarehouseLayout
	// pendingOutputs, when set, names compaction outputs whose sources still
	// exist; they are excluded from the file set (#638).
	pendingOutputs func(ctx context.Context, prefix string) (map[string]struct{}, error)
	// deletedHere, when set, reports a listed file the cluster has deleted
	// while this node's copy is still on disk; it is excluded too.
	deletedHere func(path string) bool
	// exclusions, when set, names the files Arc's own reads leave out under a
	// prefix (see SetExclusions).
	exclusions Exclusions
	// manifestKnows and manifestSync, when set (shared-storage clusters), are the replicated
	// manifest's view of a listed file and its read barrier (see SetManifestBarrier).
	manifestKnows func(path string) bool
	manifestSync  func(ctx context.Context) error
	// unmanagedMu guards unmanaged: per measurement, the listed files still
	// unknown to the manifest after a barrier. They are objects the manifest
	// never learns of (written before the cluster existed, say), and a
	// barrier per pass for them would be a replicated log entry per pass
	// (#1609). Replaced on every barrier, so it holds only what is still
	// listed; a file the manifest later learns of is dropped from it.
	unmanagedMu sync.Mutex
	unmanaged   map[string]map[string]struct{}
	// cold, when set, is the cold tier the source lists next to the primary (cold.go).
	cold   *coldTier
	logger zerolog.Logger

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

// SetDeletedHere installs the cluster's view of local copies it has deleted
// (cluster.Coordinator.LocalCopyDeleted). On a local-storage cluster a delete
// reaches this node's disk after the manifest: a staged compaction moves its
// output into place before the inputs are unlinked (#1207), a DELETE rewrite's
// new path is pulled before the old one is unlinked, and an unlink can fail or
// be lost to a crash until the stale-local sweep removes the copy. Without
// this, a pass in between publishes the deleted file next to the one holding
// its rows, and external readers see them twice. The check runs per key after
// the listing, the #638 ordering: an output in the listing was moved into
// place after this node applied its commit, so its inputs already read as
// deleted. nil (standalone) excludes nothing, and so does the lookup on a
// cluster node that does not track its local copies.
func (s *StorageWalkSource) SetDeletedHere(fn func(path string) bool) {
	s.deletedHere = fn
}

// Exclusions is the set of stored files Arc's own reads leave out because
// another file under the same prefix holds their rows: the inputs of a
// compaction whose output is in place, a file a row DELETE replaced, a local
// copy the cluster deleted. It is the query layer's set
// (api.CombineOpenCompactions), asked strictly: where a read would guess, an
// export must not, so a source that cannot tell returns an error.
type Exclusions interface {
	ExcludedForStrict(ctx context.Context, prefix string) ([]string, error)
}

// SetExclusions installs the files Arc's reads leave out (#939 A0). Without
// it the exporter publishes, during a DELETE rewrite or a compaction, a file
// next to the one that replaced it, and external readers count its rows twice
// while Arc's own queries count them once. nil excludes nothing.
func (s *StorageWalkSource) SetExclusions(ex Exclusions) {
	s.exclusions = ex
}

// SetManifestBarrier installs the shared-storage read barrier (#939 A2). In shared storage every
// data object is registered in the replicated manifest, and the files Arc's reads leave out
// (SetExclusions) come from this node's copy of it. A listed file that copy does not know is the
// sign it lags a commit — a compaction whose output this listing already sees, say, while its
// inputs are not yet left out. Then the source waits for the manifest to catch up (sync) and
// lists once more. Only then: a barrier is a replicated log entry on a follower, so one per
// measurement per pass would be thousands of entries per pass.
func (s *StorageWalkSource) SetManifestBarrier(knows func(path string) bool, sync func(ctx context.Context) error) {
	s.manifestKnows, s.manifestSync = knows, sync
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
		layout:   NewWarehouseLayout("", "", nsPrefix),
		logger:   logger,
	}
}

// SetWarehouseLayout makes the walk skip the directories the exporter's warehouse occupies in
// the storage root, by the rule the database listings apply (#1626): a subdirectory warehouse
// ("<root>/lv_wh") is not walked as a database, nor a nested one as a measurement. The
// namespace-directory rule holds either way. Call during wiring, before the scheduler starts.
func (s *StorageWalkSource) SetWarehouseLayout(l *WarehouseLayout) {
	if l != nil {
		s.layout = l
	}
}

// isWarehouseDir reports whether a top-level directory belongs to the Iceberg warehouse the
// exporter writes ("<nsPrefix>_<db>.db" namespace directories, and a subdirectory warehouse once
// SetWarehouseLayout names it), which must be excluded from the data-file walk.
func (s *StorageWalkSource) isWarehouseDir(name string) bool {
	return s.layout.IsWarehouseRootDir(name)
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
	out, err := s.measurementsOn(ctx, dl)
	if err != nil {
		return nil, err
	}
	cold, err := s.coldMeasurements(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[Measurement]bool, len(out))
	for _, m := range out {
		seen[m] = true
	}
	for _, m := range cold {
		if !seen[m] {
			seen[m] = true
			out = append(out, m)
		}
	}
	return out, nil
}

// measurementsOn enumerates the (database, measurement) directories of one backend.
func (s *StorageWalkSource) measurementsOn(ctx context.Context, dl dirLister) ([]Measurement, error) {
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
			if m == "" || s.layout.IsWarehouseMeasurement(db, m) {
				continue // a nested warehouse ("<root>/db/m/…") is not a measurement
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

// FilesAndLocal lists the measurement's Parquet files and returns both the iceberg-readable
// URIs and the on-disk local paths from that one listing, so the reconciler doesn't pay for two
// identical backend List() calls per measurement per pass. It lists again only when files
// changed under the listing (see listOnce). The two slices are aligned only in the sense that every
// local path corresponds to a data file also present in `files`; local is empty when the backend
// is non-local. Files() and LocalFiles() delegate here for callers that need just one view.
func (s *StorageWalkSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	var prev []string
	synced := false
	for attempt := 1; ; attempt++ {
		files, local, vanished, err := s.listOnce(ctx, m)
		moved := errors.Is(err, errExclusionsMoved)
		if moved {
			err = nil
		}
		if err == nil && len(vanished) == 0 && !moved && synced && s.manifestKnows != nil {
			s.recordUnmanaged(m, files)
		}
		if err == nil && len(vanished) == 0 && !moved && !synced && s.manifestKnows != nil && s.unknownToManifest(m, files) {
			// At most once per call: a file the manifest never learns of (an unmanaged
			// object) must not turn every pass into a barrier per listing.
			synced = true
			if err := s.manifestSync(ctx); err != nil {
				return nil, nil, fmt.Errorf("wait for the cluster manifest before exporting %s/%s: %w", m.Database, m.Measurement, err)
			}
			attempt--
			continue
		}
		if err != nil || (len(vanished) == 0 && !moved) {
			return files, local, err
		}
		// The same keys listed and missing twice running are not a removal
		// racing the listing (that key leaves the next listing) but an entry
		// that never resolves, such as a dangling symlink: it holds no rows.
		if !moved && slices.Equal(vanished, prev) {
			return files, local, nil
		}
		if attempt == vanishRetries {
			return nil, nil, fmt.Errorf(
				"file set of %s/%s kept changing under the listing (%d file(s) removed while it was read, %d attempts); keeping the previous snapshot this pass",
				m.Database, m.Measurement, len(vanished), attempt)
		}
		prev = vanished
		s.logger.Debug().Str("database", m.Database).Str("measurement", m.Measurement).Int("vanished", len(vanished)).
			Msg("Iceberg reconcile: files removed while the measurement was listed; listing again")
	}
}

// FileSchema derives one listed file's schema from its footer: from disk when
// the file is local, otherwise from the tail of the object (SchemaFromBackend).
func (s *StorageWalkSource) FileSchema(ctx context.Context, f FileRef) (ArcSchema, error) {
	if f.Cold && s.cold != nil {
		return s.coldSchema(ctx, f)
	}
	if f.LocalPath != "" {
		return SchemaFromParquetContext(ctx, f.LocalPath)
	}
	return SchemaFromBackend(ctx, s.backend, f.Key, f.SizeBytes)
}

// objectLister is the backend's sized listing when the backend is an object
// store; nil for local storage, which keeps its per-file stat (List plus
// os.Stat is cheaper there than a walk that stats every file anyway).
func (s *StorageWalkSource) objectLister() storage.ObjectLister {
	if s.backend.Type() == "local" {
		return nil
	}
	ol, _ := s.backend.(storage.ObjectLister)
	return ol
}

// listSizes lists prefix with each object's size, keyed as List keys it.
func listSizes(ctx context.Context, lister storage.ObjectLister, prefix string) (map[string]int64, error) {
	objs, err := lister.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(objs))
	for _, o := range objs {
		out[o.Path] = o.Size
	}
	return out, nil
}

// unknownToManifest reports whether any listed file is unknown to the replicated manifest.
// unknownToManifest reports whether a listed file is unknown to the manifest
// and not already known to stay so after an earlier barrier (unmanaged).
func (s *StorageWalkSource) unknownToManifest(m Measurement, files []FileRef) bool {
	s.unmanagedMu.Lock()
	defer s.unmanagedMu.Unlock()
	known := s.unmanaged[measurementKey(m.Database, m.Measurement)]
	for _, f := range files {
		if s.manifestKnows(f.Key) {
			delete(known, f.Key)
			continue
		}
		if _, ok := known[f.Key]; !ok {
			return true
		}
	}
	return false
}

// recordUnmanaged remembers the listed files a barrier did not make known.
func (s *StorageWalkSource) recordUnmanaged(m Measurement, files []FileRef) {
	still := make(map[string]struct{})
	for _, f := range files {
		if !s.manifestKnows(f.Key) {
			still[f.Key] = struct{}{}
		}
	}
	s.unmanagedMu.Lock()
	defer s.unmanagedMu.Unlock()
	if s.unmanaged == nil {
		s.unmanaged = make(map[string]map[string]struct{})
	}
	if len(still) == 0 {
		delete(s.unmanaged, measurementKey(m.Database, m.Measurement))
		return
	}
	s.unmanaged[measurementKey(m.Database, m.Measurement)] = still
}

// vanishRetries bounds FilesAndLocal's attempts at a stable listing.
const vanishRetries = 3

// errExclusionsMoved: a listed file started being left out of Arc's reads
// while the listing ran, so the listing has no usable set and is taken again.
var errExclusionsMoved = errors.New("files left out of reads changed during the listing")

// listOnce is one listing of the measurement. vanished names, sorted, the
// listed files that changed under it: gone by the time they were stat'ed, or
// left out of Arc's reads only once the listing ran. Whatever did that (a
// compaction, a DELETE rewrite, retention) may have put their rows in a file
// this listing missed, so the set is not exported.
func (s *StorageWalkSource) listOnce(ctx context.Context, m Measurement) (files []FileRef, local []string, vanished []string, err error) {
	prefix := m.Database + "/" + m.Measurement + "/"
	if ul, ok := s.backend.(storage.UnusableLister); ok {
		// Enumerate hidden files first: a hidden-to-usable rename between the
		// two passes may cause a spurious refusal, but never a silent omission.
		unusable, err := ul.ListUnusable(ctx, prefix)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("check hidden files for %s/%s: %w", m.Database, m.Measurement, err)
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
			return nil, nil, nil, fmt.Errorf(
				"refusing Iceberg export for %s/%s: %d data file(s) are hidden from the normal storage listing, so publishing the table would be incomplete; rename the files, and the next reconcile pass will publish. Sample(s): %s",
				m.Database, m.Measurement, len(hiddenData), strings.Join(samples, "; "),
			)
		}
	}
	// The files Arc's reads leave out, read BEFORE the listing: each is left
	// out only once the file holding its rows is in storage, so every
	// replacement of a key read here is in the listing below. Read after it,
	// a key whose replacement landed in between would drop that key's rows
	// from this pass.
	var leaveOut map[string]struct{}
	if s.exclusions != nil {
		keys, err := s.exclusions.ExcludedForStrict(ctx, prefix)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("files Arc's reads leave out under %s/%s: %w", m.Database, m.Measurement, err)
		}
		leaveOut = make(map[string]struct{}, len(keys))
		for _, k := range keys {
			leaveOut[filepath.ToSlash(k)] = struct{}{}
		}
	}
	// On an object store the listing carries each object's size, so no file is
	// HEADed; a second listing below stands in for the per-file stat.
	lister := s.objectLister()
	var paths []string
	var listedSize map[string]int64
	if lister != nil {
		listedSize, err = listSizes(ctx, lister, prefix)
		if err == nil {
			paths = make([]string, 0, len(listedSize))
			for p := range listedSize {
				paths = append(paths, p)
			}
			sort.Strings(paths)
		}
	} else {
		paths, err = s.backend.List(ctx, prefix)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list files for %s/%s: %w", m.Database, m.Measurement, err)
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
			return nil, nil, nil, fmt.Errorf("pending compaction outputs for %s/%s: %w", m.Database, m.Measurement, err)
		}
	}
	// A key that started being left out while this listing ran: its
	// replacement may have landed after the listing, so leaving it out could
	// drop its rows, and keeping it could count them twice.
	if s.exclusions != nil {
		after, err := s.exclusions.ExcludedForStrict(ctx, prefix)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("files Arc's reads leave out under %s/%s: %w", m.Database, m.Measurement, err)
		}
		listed := make(map[string]struct{}, len(paths))
		for _, p := range paths {
			listed[filepath.ToSlash(p)] = struct{}{}
		}
		for _, k := range after {
			k = filepath.ToSlash(k)
			if _, before := leaveOut[k]; before {
				continue
			}
			if _, ok := listed[k]; ok {
				vanished = append(vanished, k)
			}
		}
		if len(vanished) > 0 {
			sort.Strings(vanished)
			return nil, nil, vanished, errExclusionsMoved
		}
	}
	if lister != nil {
		// The object-store stat: a data object gone, or a different size, by
		// this second listing changed under the first.
		again, err := listSizes(ctx, lister, prefix)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("list files for %s/%s: %w", m.Database, m.Measurement, err)
		}
		for _, p := range paths {
			if !isDataFile(p) {
				continue
			}
			if size, ok := again[p]; !ok || size != listedSize[p] {
				vanished = append(vanished, p)
			}
		}
		if len(vanished) > 0 {
			sort.Strings(vanished)
			return nil, nil, vanished, nil
		}
	}
	excluded, deleted, replaced := 0, 0, 0
	for _, p := range paths {
		if !isDataFile(p) {
			continue
		}
		if _, skip := pending[filepath.ToSlash(p)]; skip {
			excluded++
			continue
		}
		if _, skip := leaveOut[filepath.ToSlash(p)]; skip {
			replaced++
			continue
		}
		if s.deletedHere != nil && s.deletedHere(filepath.ToSlash(p)) {
			deleted++
			continue
		}
		uri, err := s.resolver.Resolve(p)
		if err != nil {
			// The listing produced a key this backend would itself refuse.
			// Fail rather than skip: a skipped file is missing from the
			// exported table with no other signal.
			return nil, nil, nil, fmt.Errorf("listed file %q has no usable storage path: %w", p, err)
		}
		lp := s.resolver.LocalPath(p)
		size, present := listedSize[p], true
		if lister == nil {
			size, present, err = s.statSize(ctx, p, lp)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("stat listed file %q: %w", p, err)
			}
		}
		if !present {
			// Removed between the listing and the stat (retention, compaction, the
			// delete API). Its rows may now sit in a file this listing predates, so
			// the caller lists again rather than export a set missing them.
			s.logger.Debug().Str("file", p).Msg("Iceberg reconcile: file vanished after listing")
			vanished = append(vanished, filepath.ToSlash(p))
			continue
		}
		files = append(files, FileRef{PhysicalPath: uri, SizeBytes: size, Key: filepath.ToSlash(p), LocalPath: lp})
		if lp != "" {
			local = append(local, lp)
		}
	}
	if excluded > 0 {
		s.logger.Debug().Str("database", m.Database).Str("measurement", m.Measurement).Int("excluded", excluded).
			Msg("Iceberg reconcile: compaction outputs still replacing their sources are left out of this pass")
	}
	if deleted > 0 {
		s.logger.Debug().Str("database", m.Database).Str("measurement", m.Measurement).Int("deleted", deleted).
			Msg("Iceberg reconcile: files the cluster deleted, still on this node's disk, are left out of this pass")
	}
	if replaced > 0 {
		s.logger.Debug().Str("database", m.Database).Str("measurement", m.Measurement).Int("replaced", replaced).
			Msg("Iceberg reconcile: files Arc's reads leave out (replaced by a compaction or DELETE) are left out of this pass")
	}
	sort.Strings(vanished)
	if s.cold != nil {
		skip := func(key string) bool {
			if _, ok := pending[key]; ok {
				return true
			}
			if _, ok := leaveOut[key]; ok {
				return true
			}
			return s.deletedHere != nil && s.deletedHere(key)
		}
		if files, local, err = s.mergeCold(ctx, prefix, files, local, skip); err != nil {
			return nil, nil, nil, err
		}
	}
	return files, local, vanished, nil
}

// statSize returns the file's current byte size, which the reconciler needs
// for a file rewritten at its own path (#633). Since #1073 the delete API's
// partial-match branch publishes a fresh path instead, but restores and
// edge-sync still write a known path, and only the size tells that
// content apart from what the Iceberg manifest describes.
// present=false means the file is gone. Local files are stat'ed directly; a
// backend that is not an object store (objectLister) but has no local path
// answers through StatFile. Object stores never get here: their size comes
// from the listing.
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
	// backslash-separated (LocalBackend.List uses filepath.Rel without ToSlash). filepath.Base
	// handles both separators; path.Base would treat a backslash key as one component. An
	// object-store key never holds a backslash (the backends refuse one), so it is unaffected.
	base := filepath.Base(p)
	// Skip dotfiles. This covers Arc's in-flight ".tmp.*" writes (a separate ".tmp." check would
	// be redundant — it already starts with "."), so a partially-written file is never registered.
	if strings.HasPrefix(base, ".") {
		return false
	}
	return strings.HasSuffix(base, ".parquet")
}
