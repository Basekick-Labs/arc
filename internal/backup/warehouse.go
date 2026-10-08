package backup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/storage"
)

// icebergBackupPrefix is the directory under <backupID>/ that holds Iceberg
// warehouse metadata copied from a warehouse OUTSIDE the data storage root.
// A warehouse under the root travels with the data listing under data/ instead.
const icebergBackupPrefix = "iceberg"

// warehouseFile is one metadata file the warehouse walk found.
type warehouseFile struct {
	rel  string // slash-separated path relative to the warehouse root
	abs  string // on-disk path
	size int64
}

// configureIcebergWarehouse decides, once, whether the Iceberg warehouse needs
// its own copy pass. The data-storage listing already covers a warehouse under
// the storage root (the default and the subdirectory layout of #534); a
// warehouse anywhere else is invisible to that listing, and until #637 a backup
// silently omitted it while still carrying the catalog rows that point into it.
//
// Both sides are compared with symlinks resolved and at a path boundary, and
// the RESOLVED warehouse path is what the walk later uses: filepath.WalkDir does
// not follow a symlinked root, so walking the configured spelling of a
// symlinked warehouse would back up nothing and report success.
func (m *Manager) configureIcebergWarehouse(cfg *ManagerConfig) {
	if cfg.IcebergWarehousePath == "" {
		return
	}
	m.icebergEnabled = true
	m.icebergNSPrefix = cfg.IcebergNamespacePrefix
	if m.icebergNSPrefix == "" {
		m.icebergNSPrefix = "arc"
	}
	wh := resolveExistingPath(cfg.IcebergWarehousePath)
	if lb, ok := cfg.DataStorage.(*storage.LocalBackend); ok {
		root := resolveExistingPath(lb.GetBasePath())
		if pathWithin(wh, root) {
			// Remember where under the root it sits: "" for the root itself,
			// "<sub>/" for the #534 subdirectory layout. A scoped backup
			// finds the namespace directories it leaves out through it.
			if rel, err := filepath.Rel(root, wh); err == nil && rel != "." {
				m.icebergWarehouseKeyPrefix = filepath.ToSlash(rel) + "/"
			}
			m.logger.Debug().Str("warehouse", wh).Str("storage_root", root).
				Msg("Iceberg warehouse is under the storage root; the data listing covers its metadata")
			return
		}
		if pathWithin(root, wh) {
			m.logger.Warn().Str("warehouse", wh).Str("storage_root", root).
				Msg("Iceberg warehouse contains the storage root; only its " + m.icebergNSPrefix + "_*.db namespace directories are backed up from it")
		}
	}
	// Containment against the backup destination is a question only a LOCAL
	// destination has (#1085 stage B2b-1): an object store cannot contain a
	// directory on this machine. Asking it of cfg.BackupPath while a remote
	// target is configured would be worse than useless — BackupPath keeps its
	// "./data/backups" default and is not the destination, so the warning
	// would name a directory nothing writes to, and resolveExistingPath("")
	// resolves to the WORKING DIRECTORY, which contains almost everything.
	if bp := localDestinationPath(cfg); bp != "" && pathWithin(bp, wh) {
		m.logger.Warn().Str("warehouse", wh).Str("backup_path", bp).
			Msg("Iceberg warehouse contains the backup directory; only its " + m.icebergNSPrefix + "_*.db namespace directories are backed up from it")
	}
	m.icebergWarehouse = wh
	m.icebergWarehouseConfigured = filepath.Clean(cfg.IcebergWarehousePath)
	m.logger.Info().Str("warehouse", wh).
		Msg("Iceberg warehouse is outside the storage root; backups copy its table metadata separately under " + icebergBackupPrefix + "/")
}

// localDestinationPath returns the resolved local directory the DEFAULT
// backup target will be written to, or "" when that destination is an object
// store or no local path is configured at all.
//
// The default target alone, because this answers one question — can the
// Iceberg warehouse CONTAIN the backup directory — and a warehouse on this
// machine can only contain a directory on this machine. A routed target is
// checked by config.checkBackupDestinationOverlap, which refuses an overlap
// with primary storage for every target; the warning here is the one thing
// that needs a path rather than a Destination.
func localDestinationPath(cfg *ManagerConfig) string {
	path := cfg.BackupPath
	if def := defaultConfiguredTarget(cfg); def != nil {
		if def.Remote {
			return ""
		}
		path = def.Spec.LocalPath
	}
	if path == "" {
		return ""
	}
	return resolveExistingPath(path)
}

// defaultConfiguredTarget is the member of cfg.Targets that cfg.DefaultTarget
// names, or nil when no target is configured. A DefaultTarget naming none of
// them is refused by NewManager, so nil here means "no targets".
func defaultConfiguredTarget(cfg *ManagerConfig) *Target {
	for i := range cfg.Targets {
		if cfg.Targets[i].Name == cfg.DefaultTarget {
			return &cfg.Targets[i]
		}
	}
	return nil
}

// resolveExistingPath and pathWithin are storage.ResolveExistingPath and
// storage.PathWithin.
//
// The implementations moved to internal/storage when the backup-destination
// overlap refusal needed the same two rules (#1085 stage B2b): the refusal
// runs in config.Load, which cannot import this package, and two
// independently-maintained copies of a boundary match is exactly how #534's
// own fix shipped a mid-segment HasPrefix bug. Kept as package-local names so
// the six call sites in this file and the table tests that pin their
// behaviour continue to read as they did, and so those tests now exercise the
// shared implementation rather than a second copy of it.
func resolveExistingPath(p string) string { return storage.ResolveExistingPath(p) }

func pathWithin(p, dir string) bool { return storage.PathWithin(p, dir) }

// listIcebergWarehouseFiles walks the outside-root warehouse and returns the
// exporter's table metadata files. The walk is scoped to the exporter's own
// layout — <prefix>_<db>.db/<table>/metadata/<file> — and nothing else: a
// warehouse that contains the storage root or the backup directory would
// otherwise sweep every earlier backup's SQLite snapshot (backups/<id>/metadata/)
// and any measurement that happens to be named "metadata" into the copy.
//
// Dot-prefixed names are skipped like the storage listing skips them (a
// .DS_Store under metadata/ would be copied and then hidden on restore).
// Symlink entries are skipped. A warehouse that does not exist yet is empty. Any
// other walk error is fatal: a silently partial warehouse is the bug this fixes.
func (m *Manager) listIcebergWarehouseFiles() ([]warehouseFile, error) {
	return m.walkIcebergWarehouse(nil)
}

// walkIcebergWarehouse is listIcebergWarehouseFiles with an optional filter on
// the namespace directory: when keep is non-nil, a namespace it declines is
// skipped whole. A scoped backup uses it to count only its own databases'
// namespaces (#1084); the unscoped walk passes nil.
func (m *Manager) walkIcebergWarehouse(keep func(nsDir string) bool) ([]warehouseFile, error) {
	root := m.icebergWarehouse
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// A scoped count pass (keep != nil) backs nothing up from the
			// warehouse either way; only the unscoped copy pass says so loud.
			ev := m.logger.Info()
			if keep != nil {
				ev = m.logger.Debug()
			}
			ev.Str("warehouse", root).Msg("Iceberg warehouse does not exist yet; nothing to back up from it")
			return nil, nil
		}
		return nil, fmt.Errorf("stat iceberg warehouse %s: %w", root, err)
	}
	var out []warehouseFile
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if p == root {
			return nil
		}
		name := d.Name()
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		depth := strings.Count(rel, "/") + 1
		if strings.HasPrefix(name, ".") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			// Not followed. Below the table level that is a file the exporter
			// never writes; above it, it is a whole namespace or table the
			// backup will not carry, which the operator must hear about.
			if depth <= 3 {
				m.logger.Warn().Str("path", p).
					Msg("Iceberg warehouse entry is a symlink and is not followed; its tables are not in this backup")
			}
			return nil
		}
		switch depth {
		case 1: // namespace directory: <prefix>_<db>.db
			if !d.IsDir() {
				return nil
			}
			if !strings.HasPrefix(name, m.icebergNSPrefix+"_") || !strings.HasSuffix(name, ".db") {
				return fs.SkipDir
			}
			if keep != nil && !keep(name) {
				return fs.SkipDir
			}
		case 2: // table directory
			if !d.IsDir() {
				return nil
			}
		case 3: // metadata directory
			if !d.IsDir() {
				return nil
			}
			if name != "metadata" {
				return fs.SkipDir
			}
		case 4: // metadata files
			if d.IsDir() {
				return fs.SkipDir
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			out = append(out, warehouseFile{rel: rel, abs: p, size: info.Size()})
		default:
			if d.IsDir() {
				return fs.SkipDir
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk iceberg warehouse %s: %w", root, err)
	}
	return out, nil
}

// countExcludedIcebergNamespaces records, for a scoped backup (#1084), how
// much Iceberg table metadata of the scoped databases it left out and where it
// is: the namespace directory <prefix>_<db>.db of each scoped database, found
// under the storage root (at the root itself or under the configured
// subdirectory, through icebergWarehouseKeyPrefix) with one prefix listing
// per database, or in the outside-root warehouse with the scoped walk. The
// in-root listing is of the data store and fails the run like the data
// listing would; the outside-root walk only counts, so a warehouse that
// cannot be read is logged and the count left at zero, consistent with the
// downgraded preflight. Only called when Iceberg export is on.
func (m *Manager) countExcludedIcebergNamespaces(ctx context.Context, lister storage.ObjectLister, sc *scope, manifest *Manifest) error {
	if m.icebergWarehouse == "" {
		for _, name := range sc.names {
			dir := m.icebergWarehouseKeyPrefix + icebergNamespaceDir(m.icebergNSPrefix, name)
			objs, err := lister.ListObjects(ctx, dir+"/")
			if err != nil {
				return fmt.Errorf("failed to list the Iceberg namespace directory %s: %w", dir, err)
			}
			if len(objs) == 0 {
				continue
			}
			manifest.IcebergNamespaceFilesExcluded += int64(len(objs))
			manifest.IcebergNamespacesExcluded = append(manifest.IcebergNamespacesExcluded, dir)
		}
	} else {
		files, err := m.walkIcebergWarehouse(func(dir string) bool { return sc.ownsIcebergNamespaceDir(dir, m.icebergNSPrefix) })
		if err != nil {
			m.logger.Warn().Err(err).Str("warehouse", m.icebergWarehouse).
				Msg("Could not count the Iceberg namespace metadata this scoped backup leaves out; a scoped backup does not copy the warehouse")
			return nil
		}
		perDir := make(map[string]int64)
		for _, f := range files {
			dir, _, _ := strings.Cut(f.rel, "/")
			perDir[dir]++
		}
		dirs := make([]string, 0, len(perDir))
		for dir := range perDir {
			dirs = append(dirs, dir)
		}
		sort.Strings(dirs)
		for _, dir := range dirs {
			manifest.IcebergNamespaceFilesExcluded += perDir[dir]
			manifest.IcebergNamespacesExcluded = append(manifest.IcebergNamespacesExcluded, dir)
		}
	}
	if manifest.IcebergNamespaceFilesExcluded > 0 {
		m.logger.Info().
			Int64("files", manifest.IcebergNamespaceFilesExcluded).
			Strs("namespaces", manifest.IcebergNamespacesExcluded).
			Msg("Iceberg table metadata of the scoped databases is not in this backup: the Iceberg catalog is instance-wide and travels with include_metadata, which a scoped backup cannot carry; take an unscoped backup for the Iceberg tables")
	}
	return nil
}

// preflightIcebergWarehouse fails a backup before any data is copied when the
// outside-root warehouse exists but cannot be read, so a permission problem
// surfaces in seconds rather than after the whole data set was copied. A
// warehouse that does not exist yet is fine (fresh node).
func (m *Manager) preflightIcebergWarehouse() error {
	if m.icebergWarehouse == "" {
		return nil
	}
	if _, err := os.ReadDir(m.icebergWarehouse); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("iceberg warehouse %s is not readable: %w", m.icebergWarehouse, err)
	}
	return nil
}

// copyIcebergWarehouse copies the walked files under <backupID>/iceberg/<rel>.
// A source that cannot be opened is skipped and counted, like a data file that
// vanished between listing and copy; every other failure aborts the backup.
//
// Always the DEFAULT leg (#1085 stage B2b-2), made explicit by taking it as a
// parameter rather than being incidental: an outside-root warehouse belongs to
// no database — it is the instance's Iceberg metadata, only useful with the
// instance-wide SQL catalog, which rides with include_metadata to the same
// place.
func (m *Manager) copyIcebergWarehouse(ctx context.Context, leg *backupLeg, files []warehouseFile) (int64, error) {
	run := leg.run
	backupID := run.id
	progress := run.progress
	var skipped int64
	for _, f := range files {
		select {
		case <-ctx.Done():
			return skipped, ctx.Err()
		default:
		}
		destPath := backupID + "/" + icebergBackupPrefix + "/" + f.rel
		// Failing the whole run here is deliberate (#1100), for the same
		// reason copyStateFiles fails on an unstorable key: absence would not
		// be detectable. A warehouse is not routed per database and cannot be
		// partially useful — Iceberg metadata is a graph of cross-references,
		// so a backup holding all of it but one manifest list restores a
		// catalog whose tables do not resolve, discovered at read time rather
		// than at backup time. Only the data path skips, because a skipped
		// data file is counted, named and still held by the source.
		if err := storage.ValidateKey(destPath); err != nil {
			return skipped, fmt.Errorf("iceberg warehouse file %q cannot be stored under a valid backup key: %w", f.rel, err)
		}
		written, err := m.streamLocalFileToBackup(ctx, leg.target, f.abs, destPath)
		if err != nil {
			if !isSourceReadError(err) {
				return skipped, fmt.Errorf("failed to back up iceberg warehouse file %s: %w", f.rel, err)
			}
			skipped++
			m.logger.Warn().Str("path", f.abs).Err(err).Msg("Failed to read Iceberg warehouse file, skipping")
			continue
		}
		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, written)
		atomic.AddInt64(&leg.files, 1)
		atomic.AddInt64(&leg.bytes, written)
		run.publishTargets()
	}
	atomic.AddInt64(&progress.SkippedFiles, skipped)
	atomic.AddInt64(&leg.skipped, skipped)
	run.publishTargets()
	return skipped, nil
}

// streamLocalFileToBackup copies one local file into backup storage. The file
// is opened once and its size taken from that handle, so the declared length
// matches the bytes streamed. Open and stat failures are source-read errors
// (skippable); a backup-storage write failure is fatal.
func (m *Manager) streamLocalFileToBackup(ctx context.Context, dest backupTarget, srcAbs, destPath string) (int64, error) {
	f, err := os.Open(srcAbs)
	if err != nil {
		return 0, fmt.Errorf("%w: open %s: %v", errBackupRead, srcAbs, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("%w: stat %s: %v", errBackupRead, srcAbs, err)
	}
	if err := dest.backend.WriteReader(ctx, destPath, f, info.Size()); err != nil {
		m.cleanupPartialBackupWrite(ctx, dest, destPath)
		return 0, fmt.Errorf("failed to write to %s: %w", dest.describe(), err)
	}
	return info.Size(), nil
}

// restoreIcebergWarehouse writes the objects under <backupID>/iceberg/ into
// this node's configured outside-root warehouse. The destination is always the
// LOCAL configuration, never a path named by the manifest: the manifest is data
// read from backup storage. The catalog's metadata locations are absolute, so
// the restore can only work when this node's warehouse is at the source's path.
//
// src is the run's ANCHOR leg — the leg whose manifest declared the warehouse —
// and not whatever target is the default now. A zero-object listing returns nil
// below, so reading the wrong leg does not fail: it reports a completed restore
// that wrote no warehouse back, which is #637's failure mode. See
// runRead.anchor.
func (m *Manager) restoreIcebergWarehouse(ctx context.Context, src backupTarget, backupID string, manifest *Manifest, progress *Progress, catalogRestored bool) error {
	prefix := backupID + "/" + icebergBackupPrefix + "/"
	files, err := src.backend.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("failed to list the backup iceberg warehouse files in %s: %w", src.describe(), err)
	}
	if len(files) == 0 {
		return nil
	}
	// The spelling the catalog rows were built from. Backups written before
	// configured_path existed only carry the resolved directory.
	sourcePath := ""
	if manifest.IcebergWarehouse != nil {
		sourcePath = manifest.IcebergWarehouse.ConfiguredPath
		if sourcePath == "" {
			sourcePath = manifest.IcebergWarehouse.Path
		}
	}
	if m.icebergWarehouse == "" {
		progress.IcebergWarehouseFilesSkipped = int64(len(files))
		m.setProgress(progress)
		ev := m.logger.Warn().Int("files", len(files)).Str("backup_warehouse", sourcePath)
		if catalogRestored {
			ev.Msg("Backup holds Iceberg warehouse metadata from a warehouse outside the storage root, but this node has no such warehouse; skipping it. The restored catalog points at absolute paths under the backup's warehouse: set iceberg.warehouse to that path (a symlink to it works) and run the restore again")
		} else {
			ev.Msg("Backup holds Iceberg warehouse metadata from a warehouse outside the storage root, but this node has no such warehouse; skipping it")
		}
		return nil
	}
	// Does the source's configured spelling, evaluated on THIS host, land in the
	// directory the files are written to? That is the condition under which the
	// restored catalog's absolute metadata locations resolve; comparing resolved
	// directories would warn falsely for the symlink workaround and stay silent
	// when the spellings differ but the real directories coincide.
	if sourcePath != "" && resolveExistingPath(sourcePath) != m.icebergWarehouse {
		m.logger.Warn().
			Str("backup_warehouse", sourcePath).
			Str("local_warehouse", m.icebergWarehouseConfigured).
			Msg("Restoring Iceberg warehouse metadata into a directory the backup's catalog does not point at; its metadata locations are absolute, so they will not resolve unless iceberg.warehouse is the backup's path (a symlink from that path to this directory works)")
	}
	progress.TotalFiles += int64(len(files))
	m.setProgress(progress)

	var skipped int64
	var sample []string
	for _, srcPath := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rel := strings.TrimPrefix(srcPath, prefix)
		dest, err := warehouseRestorePath(m.icebergWarehouse, rel)
		if err != nil {
			return fmt.Errorf("refusing to restore backup object %s: %w", srcPath, err)
		}
		written, err := m.streamRestoreToLocalFile(ctx, src, srcPath, dest)
		if err != nil {
			if !isRestoreReadError(err) {
				return fmt.Errorf("failed to restore %s: %w", srcPath, err)
			}
			skipped++
			if len(sample) < unaddressableSampleCap {
				sample = append(sample, srcPath)
			}
			m.logger.Warn().Str("path", srcPath).Err(err).Msg("Failed to read backup file, skipping")
			continue
		}
		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, written)
		m.setProgress(progress)
	}
	if skipped > 0 {
		atomic.AddInt64(&progress.SkippedFiles, skipped)
		merged := append(append([]string(nil), progress.SkippedSample...), sample...)
		progress.SkippedSample = merged
		m.setProgress(progress)
	}
	m.logger.Info().Int("files", len(files)).Str("warehouse", m.icebergWarehouse).Msg("Iceberg warehouse metadata restored")
	return nil
}

// warehouseRestorePath joins a backup-relative key under the warehouse root,
// refusing anything that is not a plain relative path of clean segments. The
// backup backend's key contract already rejects such keys at write and hides
// them at list time; this is the second guard on the one write that happens
// outside a storage backend.
func warehouseRestorePath(root, rel string) (string, error) {
	rel = filepath.ToSlash(rel)
	if rel == "" || strings.HasPrefix(rel, "/") {
		return "", errors.New("empty or absolute key")
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("invalid path segment %q", seg)
		}
	}
	dest := filepath.Join(root, filepath.FromSlash(rel))
	if !pathWithin(dest, root) {
		return "", errors.New("key escapes the warehouse")
	}
	return dest, nil
}

// streamRestoreToLocalFile streams one backup object to a local file via a
// temp file in the destination directory and a rename, with Arc's 0600/0700
// modes. Only a backup-storage read failure is skippable.
func (m *Manager) streamRestoreToLocalFile(ctx context.Context, src backupTarget, srcPath, dest string) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return 0, fmt.Errorf("failed to create warehouse directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".restore-*")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	tw := &trackingWriter{w: tmp}
	if err := src.backend.ReadTo(ctx, srcPath, tw); err != nil {
		tmp.Close()
		return 0, classifyReadTo(srcPath, err, tw.err)
	}
	info, err := tmp.Stat()
	if err != nil {
		tmp.Close()
		return 0, fmt.Errorf("failed to stat temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return 0, fmt.Errorf("failed to sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("failed to close temp file: %w", err)
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return 0, fmt.Errorf("failed to set file mode: %w", err)
	}
	if err := os.Rename(tmpPath, dest); err != nil {
		return 0, fmt.Errorf("failed to place restored file: %w", err)
	}
	return info.Size(), nil
}
