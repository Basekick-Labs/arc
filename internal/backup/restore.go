package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// RestoreOptions controls what gets restored and from where.
type RestoreOptions struct {
	BackupID        string
	RestoreData     bool // restore parquet files
	RestoreMetadata bool // restore SQLite database
	RestoreConfig   bool // restore arc.toml (requires restart)
}

// RestoreResult is returned when a restore completes.
type RestoreResult struct {
	Manifest *Manifest
	Duration time.Duration
}

// errRestoreRead marks a failure reading a backup object from backup storage.
//
// It is the only per-file failure a restore tolerates mid-run: the object is
// counted and sampled and the restore moves on, so an operator recovering from
// a damaged backup still gets every file that can be read. The restore does
// not end "completed" because of it (see RestoreBackup): unlike backup, where
// a source read can fail because compaction or retention removed the file
// between listing and copy, nothing removes objects under a backup while it is
// being restored, so every such failure is damage or a degraded store.
//
// Temp-file and data-storage failures are not wrapped and abort the restore:
// they mean the environment underneath it is broken, and continuing past them
// is how a restore silently drops files.
var errRestoreRead = errors.New("restore source read failed")

func isRestoreReadError(err error) bool {
	return errors.Is(err, errRestoreRead)
}

// trackingWriter records the first error the destination returned.
//
// Every backend's ReadTo is an io.Copy into the caller's writer, so a full temp
// filesystem surfaces as a ReadTo error that is indistinguishable, by the error
// alone, from the source being unreadable. The recorded error lets
// classifyReadTo attribute the failure to the right side.
//
// Wrapping the temp file costs the backup→temp hop io.Copy's zero-copy fast
// path (copy_file_range needs an *os.File destination), so that hop runs
// through a buffered loop. Deliberate: a restore is rare and disk-bound, and
// the alternative is not knowing which side failed. The temp→data hop is
// unaffected.
type trackingWriter struct {
	w   io.Writer
	err error
}

func (t *trackingWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if err != nil && t.err == nil {
		t.err = err
	}
	return n, err
}

// createRestoreTemp creates the per-file staging temp file. A variable so tests
// can hand streamRestoreFile a file that refuses writes, which is the only
// portable way to drive the destination-side ReadTo failure end to end.
var createRestoreTemp = func() (*os.File, error) {
	return os.CreateTemp("", "arc-restore-*.parquet")
}

// classifyReadTo turns a ReadTo failure into the restore's two error classes: a
// destination error recorded by the tracking writer is fatal; anything else is
// a source read, wrapped with errRestoreRead so the caller can skip it.
func classifyReadTo(srcPath string, readErr, writeErr error) error {
	if writeErr != nil {
		return fmt.Errorf("failed to write temp file while reading %s from backup: %w", srcPath, writeErr)
	}
	return fmt.Errorf("failed to read from backup: %w: %w", errRestoreRead, readErr)
}

// RestoreBackup restores data from a backup. It runs synchronously; the API
// layer launches it in a goroutine and exposes progress via GetProgress().
//
// A restore that could not restore every data file ends with Status "failed"
// and an error naming the counts, even though every readable file was written
// (#762). "completed" is what automation checks; reporting it over a gap turns
// the gap into a surprise at query time, which is the failure this exists to
// prevent. The files that were restored stay in place.
func (m *Manager) RestoreBackup(ctx context.Context, opts RestoreOptions) (*RestoreResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	startTime := time.Now()

	progress := &Progress{
		Operation: "restore",
		BackupID:  opts.BackupID,
		Status:    "running",
		StartedAt: startTime,
	}
	m.setProgress(progress)
	defer func() {
		now := time.Now()
		progress.CompletedAt = &now
		m.setProgress(progress)
	}()

	m.logger.Info().Str("backup_id", opts.BackupID).Msg("Starting restore")

	// ── 1. Read and validate manifest ───────────────────────────────────
	manifest, err := m.GetBackup(ctx, opts.BackupID)
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, fmt.Errorf("failed to read backup manifest: %w", err)
	}

	// A backup that was incomplete when it was taken restores exactly what it
	// holds. Surface that up front so a gap that predates the restore is not
	// mistaken for one the restore caused, and so an operator who never read
	// the manifest learns about it now.
	progress.BackupSkippedFiles = manifest.SkippedFiles
	progress.BackupUnaddressableFiles = manifest.UnaddressableFiles
	m.setProgress(progress)
	if manifest.SkippedFiles > 0 || manifest.UnaddressableFiles > 0 {
		m.logger.Warn().
			Str("backup_id", opts.BackupID).
			Int64("backup_skipped_files", manifest.SkippedFiles).
			Int64("backup_unaddressable_files", manifest.UnaddressableFiles).
			Strs("unaddressable_sample", manifest.UnaddressableSample).
			Msg("Restoring a backup that was incomplete when it was taken")
	}

	// ── 2. Restore data files ───────────────────────────────────────────
	if opts.RestoreData {
		if err := m.restoreDataFiles(ctx, opts.BackupID, manifest, progress); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
	}

	// ── 3. Restore SQLite metadata ──────────────────────────────────────
	if opts.RestoreMetadata && manifest.HasMetadata {
		if err := m.restoreSQLite(ctx, opts.BackupID); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, fmt.Errorf("failed to restore SQLite database: %w", err)
		}
	}

	// ── 4. Restore config ───────────────────────────────────────────────
	if opts.RestoreConfig && manifest.HasConfig {
		if err := m.restoreConfig(ctx, opts.BackupID); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, fmt.Errorf("failed to restore config: %w", err)
		}
	}

	// ── 5. Decide the outcome ───────────────────────────────────────────
	// Decided after the metadata and config steps so those are staged even
	// when the data set has a gap: an operator recovering a node wants both.
	duration := time.Since(startTime)
	skipped := atomic.LoadInt64(&progress.SkippedFiles)
	missing := progress.MissingFiles
	unaddressable := progress.UnaddressableFiles
	if skipped+missing+unaddressable > 0 {
		var parts []string
		if skipped > 0 {
			parts = append(parts, fmt.Sprintf("%d objects could not be read from backup storage (skipped_sample)", skipped))
		}
		if unaddressable > 0 {
			parts = append(parts, fmt.Sprintf("%d data files are in backup storage under names no listing returns (unaddressable_sample; rename them and re-run)", unaddressable))
		}
		if missing > 0 {
			parts = append(parts, fmt.Sprintf("%d data files the backup inventoried are absent from backup storage (missing_files)", missing))
		}
		err := fmt.Errorf("restore incomplete: %s; the files that could be restored are in place", strings.Join(parts, "; "))
		progress.Status = "failed"
		progress.Error = err.Error()
		m.logger.Error().
			Str("backup_id", opts.BackupID).
			Int64("files_restored", atomic.LoadInt64(&progress.ProcessedFiles)).
			Int64("skipped", skipped).
			Int64("unaddressable", unaddressable).
			Int64("missing", missing).
			Strs("skipped_sample", progress.SkippedSample).
			Strs("unaddressable_sample", progress.UnaddressableSample).
			Dur("duration", duration).
			Msg("Restore incomplete")
		return nil, err
	}

	progress.Status = "completed"

	m.logger.Info().
		Str("backup_id", opts.BackupID).
		Int64("files_restored", atomic.LoadInt64(&progress.ProcessedFiles)).
		Dur("duration", duration).
		Msg("Restore completed")

	return &RestoreResult{Manifest: manifest, Duration: duration}, nil
}

// restoreDataFiles copies parquet files from the backup back into data storage.
//
// Backup objects that cannot be read are skipped, counted, and sampled; every
// other failure aborts (see errRestoreRead). Data files the listing cannot
// return, and files the manifest inventoried that are gone, are counted too.
// The caller turns any non-zero count into a failed restore.
func (m *Manager) restoreDataFiles(ctx context.Context, backupID string, manifest *Manifest, progress *Progress) error {
	dataPrefix := backupID + "/data/"

	files, err := m.backupStorage.List(ctx, dataPrefix)
	if err != nil {
		return fmt.Errorf("failed to list backup data files: %w", err)
	}

	progress.TotalFiles = int64(len(files))
	progress.TotalBytes = manifest.TotalSizeBytes

	// Three ways the listing can under-represent what the manifest promised,
	// and nothing in the copy loop can notice any of them: every listed file
	// restores fine.
	//
	// 1. The listing hides it. An object store returns dot-prefixed keys and
	//    the backup copied them, but the local backup store's listing hides
	//    dot-prefixed names (and any key an older Arc wrote that the contract
	//    now refuses). ListUnusable returns exactly what List dropped, so those
	//    are counted and named: renaming them in the backup recovers the data.
	// 2. The object is gone: a partial sync, a truncated copy, an operator's
	//    rm. Counted against the manifest inventory as missing.
	// 3. The backup itself never wrote it (manifest.SkippedFiles); not missing.
	//
	// Only .parquet entries are counted: the listing also holds Iceberg
	// warehouse metadata copied under data/, which is not part of TotalFiles.
	var present int64
	for _, f := range files {
		if strings.HasSuffix(f, ".parquet") {
			present++
		}
	}
	if ul, ok := m.backupStorage.(storage.UnusableLister); ok {
		hidden, err := ul.ListUnusable(ctx, dataPrefix)
		if err != nil {
			return fmt.Errorf("failed to inventory unlistable backup objects: %w", err)
		}
		var sample []string
		var n int64
		for _, o := range hidden {
			if !strings.HasSuffix(o.Path, ".parquet") {
				continue
			}
			n++
			if len(sample) < unaddressableSampleCap {
				sample = append(sample, o.Path)
			}
		}
		if n > 0 {
			progress.UnaddressableFiles = n
			progress.UnaddressableSample = sample
			m.logger.Warn().
				Int64("unaddressable", n).
				Strs("sample", sample).
				Msg("Backup storage holds data files no listing returns; they cannot be restored until renamed")
		}
	}
	if expected := manifest.TotalFiles - manifest.SkippedFiles; expected > present+progress.UnaddressableFiles {
		progress.MissingFiles = expected - present - progress.UnaddressableFiles
		m.logger.Warn().
			Int64("inventoried", expected).
			Int64("present", present).
			Int64("unaddressable", progress.UnaddressableFiles).
			Int64("missing", progress.MissingFiles).
			Msg("Backup storage holds fewer data files than the manifest inventoried; the restore will be incomplete")
	}
	m.setProgress(progress)

	// Skips are published on every exit, including a fatal abort part-way
	// through, so the status shows which objects were unreadable even when
	// something else ended the restore. The sample is assigned once, here, and
	// never appended to again: published snapshots copy the slice header, so
	// readers only ever see a finished slice.
	var skipped int64
	var sample []string
	defer func() {
		if skipped == 0 {
			return
		}
		atomic.AddInt64(&progress.SkippedFiles, skipped)
		progress.SkippedSample = sample
		m.setProgress(progress)
	}()

	for _, srcPath := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Strip the backup prefix to get the original storage path
		destPath := strings.TrimPrefix(srcPath, dataPrefix)
		if destPath == "" || destPath == srcPath {
			continue
		}

		// Stream via temp file to avoid loading entire Parquet file into memory
		bytesWritten, err := m.streamRestoreFile(ctx, srcPath, destPath)
		if err != nil {
			// Only a backup-storage read failure is skippable. A temp-file or
			// data-storage failure means the environment underneath the
			// restore is broken, and continuing would drop files silently.
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
		atomic.AddInt64(&progress.ProcessedBytes, bytesWritten)
		// Republish so /status polling sees live counters — published Progress
		// values are immutable snapshots, not the struct being mutated here.
		m.setProgress(progress)

		if atomic.LoadInt64(&progress.ProcessedFiles)%100 == 0 {
			m.logger.Info().
				Int64("processed", atomic.LoadInt64(&progress.ProcessedFiles)).
				Int64("total", progress.TotalFiles).
				Msg("Restore progress")
		}
	}

	return nil
}

// streamRestoreFile streams a file from backup storage to data storage via a temp file,
// avoiding loading the entire file into memory (important for large Parquet files).
//
// Only a backup-storage read failure is wrapped with errRestoreRead (making it
// skippable by the caller); temp file, seek, and data-storage write failures are
// returned unwrapped and are fatal to the restore. A ReadTo failure caused by
// the temp file itself (see trackingWriter) is fatal, not a read.
func (m *Manager) streamRestoreFile(ctx context.Context, srcPath, destPath string) (int64, error) {
	tmpFile, err := createRestoreTemp()
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	// Stream from backup storage to temp file
	tw := &trackingWriter{w: tmpFile}
	if err := m.backupStorage.ReadTo(ctx, srcPath, tw); err != nil {
		return 0, classifyReadTo(srcPath, err, tw.err)
	}

	// Get size and rewind for upload
	info, err := tmpFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat temp file: %w", err)
	}
	size := info.Size()

	if _, err := tmpFile.Seek(0, 0); err != nil {
		return 0, fmt.Errorf("failed to seek temp file: %w", err)
	}

	// Stream from temp file to data storage
	if err := m.dataStorage.WriteReader(ctx, destPath, tmpFile, size); err != nil {
		m.cleanupPartialWrite(ctx, m.dataStorage, destPath)
		return 0, fmt.Errorf("failed to write to data storage: %w", err)
	}

	return size, nil
}

// restoreSQLite restores the SQLite database from the backup.
// It creates a .before-restore backup of the current database first.
func (m *Manager) restoreSQLite(ctx context.Context, backupID string) error {
	if err := m.restoreSQLiteFile(ctx, backupID, "arc.db", m.sqliteDBPath); err != nil {
		return err
	}

	// Restore the Iceberg SQL catalog when it was backed up as a separate
	// database. Absent for a backup taken before the catalog was split out, or
	// one where the catalog lived in the shared database (already restored
	// above) — neither is an error.
	//
	// Presence is tested with Exists rather than by classifying the read error:
	// every backend implements Exists with an explicit (bool, error), whereas
	// not-found error text differs per backend ("file not found" locally,
	// wrapped SDK errors for S3/Azure). Matching on text would silently invert
	// — reporting "no catalog in this backup" for a real read failure — if the
	// backup destination ever stops being local.
	if m.icebergCatalogDBPath != "" {
		srcPath := fmt.Sprintf("%s/metadata/%s", backupID, icebergCatalogDBName)
		exists, err := m.backupStorage.Exists(ctx, srcPath)
		if err != nil {
			return fmt.Errorf("failed to check for Iceberg catalog in backup: %w", err)
		}
		if !exists {
			m.logger.Info().Str("backup_id", backupID).
				Msg("Backup contains no separate Iceberg catalog; skipping")
			return nil
		}
		if err := m.restoreSQLiteFile(ctx, backupID, icebergCatalogDBName, m.icebergCatalogDBPath); err != nil {
			return fmt.Errorf("failed to restore Iceberg catalog: %w", err)
		}
		m.logger.Info().Str("backup_id", backupID).Msg("Iceberg catalog database restored")
	}

	return nil
}

// restoreSQLiteFile STAGES one SQLite database from metadata/<srcName> in the
// backup as <destPath>.pending-restore. The live database is never touched:
// applying a restore over a running server's database — even by atomic
// rename — leaves existing connections on the old inode while new pool
// connections open the restored file, splitting state across both (#635).
// ApplyPendingRestores applies the staged file at the next boot, before any
// subsystem opens the database, and takes the .before-restore safety copy at
// that point (when it can be made complete and cheaply). Restaging overwrites
// a previous staging: the last restore before restart wins. An operator can
// cancel by deleting the .pending-restore file before restarting.
func (m *Manager) restoreSQLiteFile(ctx context.Context, backupID, srcName, destPath string) error {
	srcPath := fmt.Sprintf("%s/metadata/%s", backupID, srcName)

	// Stream from backup storage into the staging file (#639 item 8): the
	// shared database can be multi-GB on audit-heavy deployments, and
	// buffering it in memory violates the streaming rule everywhere else in
	// this package. CreateTemp creates 0600 before any byte lands, and the
	// staging file is renamed into the pending path only on full success.
	staging, err := os.CreateTemp(filepath.Dir(destPath), ".restore-staging-*")
	if err != nil {
		return fmt.Errorf("failed to create restore staging file: %w", err)
	}
	stagingPath := staging.Name()
	if err := m.backupStorage.ReadTo(ctx, srcPath, staging); err != nil {
		staging.Close()
		os.Remove(stagingPath)
		return fmt.Errorf("failed to stream SQLite backup into staging: %w", err)
	}
	if err := staging.Close(); err != nil {
		os.Remove(stagingPath)
		return fmt.Errorf("failed to close staged restore: %w", err)
	}
	if err := os.Chmod(stagingPath, 0600); err != nil {
		os.Remove(stagingPath)
		return fmt.Errorf("failed to restrict staged restore: %w", err)
	}
	pendingPath := StagePath(destPath)
	if err := os.Rename(stagingPath, pendingPath); err != nil {
		os.Remove(stagingPath)
		return fmt.Errorf("failed to stage restored SQLite database: %w", err)
	}

	m.logger.Warn().
		Str("backup_id", backupID).
		Str("database", srcName).
		Str("staged_at", pendingPath).
		Msg("SQLite restore staged; it is applied at the next server start")
	return nil
}

// restoreConfig restores the arc.toml config file from the backup.
// It creates a .before-restore backup of the current config first.
func (m *Manager) restoreConfig(ctx context.Context, backupID string) error {
	srcPath := fmt.Sprintf("%s/config/arc.toml", backupID)
	data, err := m.backupStorage.Read(ctx, srcPath)
	if err != nil {
		return fmt.Errorf("failed to read config backup: %w", err)
	}

	// Safety: backup the current config before overwriting
	if _, statErr := os.Stat(m.configPath); statErr == nil {
		preRestorePath := m.configPath + ".before-restore"
		currentData, err := os.ReadFile(m.configPath)
		if err == nil {
			if err := os.WriteFile(preRestorePath, currentData, 0600); err != nil {
				m.logger.Warn().Err(err).Msg("Failed to create pre-restore backup of config")
			} else {
				m.logger.Info().Str("path", preRestorePath).Msg("Created pre-restore backup of config")
			}
		}
	}

	if err := os.WriteFile(m.configPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	m.logger.Info().Str("backup_id", backupID).Msg("Config file restored (restart required)")
	return nil
}
