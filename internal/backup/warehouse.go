package backup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
			m.logger.Debug().Str("warehouse", wh).Str("storage_root", root).
				Msg("Iceberg warehouse is under the storage root; the data listing covers its metadata")
			return
		}
		if pathWithin(root, wh) {
			m.logger.Warn().Str("warehouse", wh).Str("storage_root", root).
				Msg("Iceberg warehouse contains the storage root; only its " + m.icebergNSPrefix + "_*.db namespace directories are backed up from it")
		}
	}
	if bp := resolveExistingPath(cfg.BackupPath); pathWithin(bp, wh) {
		m.logger.Warn().Str("warehouse", wh).Str("backup_path", bp).
			Msg("Iceberg warehouse contains the backup directory; only its " + m.icebergNSPrefix + "_*.db namespace directories are backed up from it")
	}
	m.icebergWarehouse = wh
	m.icebergWarehouseConfigured = filepath.Clean(cfg.IcebergWarehousePath)
	m.logger.Info().Str("warehouse", wh).
		Msg("Iceberg warehouse is outside the storage root; backups copy its table metadata separately under " + icebergBackupPrefix + "/")
}

// resolveExistingPath returns p as an absolute, cleaned path with symlinks
// resolved. A path that does not exist yet is resolved through its deepest
// existing ancestor and the remainder is re-joined, so a fresh node whose
// warehouse sits under a symlinked parent still classifies correctly.
func resolveExistingPath(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return filepath.Clean(p)
	}
	abs = filepath.Clean(abs)
	var tail []string
	cur := abs
	for {
		if resolved, err := filepath.EvalSymlinks(cur); err == nil {
			for i := len(tail) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, tail[i])
			}
			return resolved
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return abs
		}
		tail = append(tail, filepath.Base(cur))
		cur = parent
	}
}

// pathWithin reports whether p is dir itself or lies beneath it, matching only
// at a path boundary: "/data/wh-other" is not within "/data/wh" (#534).
func pathWithin(p, dir string) bool {
	sep := string(filepath.Separator)
	dir = strings.TrimSuffix(filepath.Clean(dir), sep)
	p = filepath.Clean(p)
	return p == dir || strings.HasPrefix(p, dir+sep)
}

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
	root := m.icebergWarehouse
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			m.logger.Info().Str("warehouse", root).Msg("Iceberg warehouse does not exist yet; nothing to back up from it")
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
func (m *Manager) copyIcebergWarehouse(ctx context.Context, backupID string, files []warehouseFile, progress *Progress) (int64, error) {
	var skipped int64
	for _, f := range files {
		select {
		case <-ctx.Done():
			return skipped, ctx.Err()
		default:
		}
		destPath := backupID + "/" + icebergBackupPrefix + "/" + f.rel
		if err := storage.ValidateKey(destPath); err != nil {
			return skipped, fmt.Errorf("iceberg warehouse file %q cannot be stored under a valid backup key: %w", f.rel, err)
		}
		written, err := m.streamLocalFileToBackup(ctx, f.abs, destPath)
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
		m.setProgress(progress)
	}
	atomic.AddInt64(&progress.SkippedFiles, skipped)
	m.setProgress(progress)
	return skipped, nil
}

// streamLocalFileToBackup copies one local file into backup storage. The file
// is opened once and its size taken from that handle, so the declared length
// matches the bytes streamed. Open and stat failures are source-read errors
// (skippable); a backup-storage write failure is fatal.
func (m *Manager) streamLocalFileToBackup(ctx context.Context, srcAbs, destPath string) (int64, error) {
	f, err := os.Open(srcAbs)
	if err != nil {
		return 0, fmt.Errorf("%w: open %s: %v", errBackupRead, srcAbs, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("%w: stat %s: %v", errBackupRead, srcAbs, err)
	}
	if err := m.backupStorage.WriteReader(ctx, destPath, f, info.Size()); err != nil {
		m.cleanupPartialWrite(ctx, m.backupStorage, destPath)
		return 0, fmt.Errorf("failed to write to backup storage: %w", err)
	}
	return info.Size(), nil
}

// restoreIcebergWarehouse writes the objects under <backupID>/iceberg/ into
// this node's configured outside-root warehouse. The destination is always the
// LOCAL configuration, never a path named by the manifest: the manifest is data
// read from backup storage. The catalog's metadata locations are absolute, so
// the restore can only work when this node's warehouse is at the source's path.
func (m *Manager) restoreIcebergWarehouse(ctx context.Context, backupID string, manifest *Manifest, progress *Progress, catalogRestored bool) error {
	prefix := backupID + "/" + icebergBackupPrefix + "/"
	files, err := m.backupStorage.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("failed to list backup iceberg warehouse files: %w", err)
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
		written, err := m.streamRestoreToLocalFile(ctx, srcPath, dest)
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
func (m *Manager) streamRestoreToLocalFile(ctx context.Context, srcPath, dest string) (int64, error) {
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
	if err := m.backupStorage.ReadTo(ctx, srcPath, tw); err != nil {
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
