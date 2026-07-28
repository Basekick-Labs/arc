package backup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/basekick-labs/arc/internal/storage"
)

// errBackupRead marks a failure reading the SOURCE file from data storage.
//
// Only source-read failures are skippable: the file may legitimately have been
// deleted by compaction or retention between the listing and the copy, and
// aborting the whole backup for that benign race would be wrong.
//
// Every other failure — temp file creation, seek, backup-storage write — is
// fatal. Those indicate a broken environment (no temp space, unwritable or
// unreachable backup storage), not a race, and continuing would produce a
// backup that silently omits files while reporting success.
//
// Classification is deliberately positive rather than by exclusion: a failure
// mode added here later is fatal by default until someone marks it skippable.
var errBackupRead = errors.New("backup source read failed")

func isSourceReadError(err error) bool {
	return errors.Is(err, errBackupRead)
}

// BackupOptions controls what gets backed up and where.
type BackupOptions struct {
	IncludeMetadata bool // back up the SQLite database
	IncludeConfig   bool // back up arc.toml
}

// BackupResult is returned when a backup completes.
type BackupResult struct {
	Manifest *Manifest
	Duration time.Duration
}

// CreateBackup performs a full backup. It runs synchronously; the API layer
// launches it in a goroutine and exposes progress via GetProgress().
func (m *Manager) CreateBackup(ctx context.Context, opts BackupOptions) (*BackupResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	backupID := generateBackupID()
	startTime := time.Now()

	progress := &Progress{
		Operation: "backup",
		BackupID:  backupID,
		Status:    "running",
		StartedAt: startTime,
	}
	m.setProgress(progress)
	defer func() {
		now := time.Now()
		progress.CompletedAt = &now
		m.setProgress(progress)
	}()

	m.logger.Info().Str("backup_id", backupID).Msg("Starting backup")

	// ── 1. Discover data files ──────────────────────────────────────────
	objectLister, ok := m.dataStorage.(storage.ObjectLister)
	if !ok {
		progress.Status = "failed"
		progress.Error = "storage backend does not support listing objects"
		return nil, fmt.Errorf("storage backend does not support ListObjects")
	}

	objects, err := objectLister.ListObjects(ctx, "")
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, fmt.Errorf("failed to list data files: %w", err)
	}

	// Filter to .parquet files only
	var parquetFiles []storage.ObjectInfo
	for _, obj := range objects {
		if strings.HasSuffix(obj.Path, ".parquet") {
			parquetFiles = append(parquetFiles, obj)
		}
	}

	// Build manifest inventory
	manifest := &Manifest{
		Version:    "dev",
		BackupID:   backupID,
		CreatedAt:  startTime.UTC(),
		BackupType: "full",
	}

	dbMap := make(map[string]*DatabaseInfo)
	for _, obj := range parquetFiles {
		manifest.TotalFiles++
		manifest.TotalSizeBytes += obj.Size

		db, meas := parseDBMeasurement(obj.Path)
		di, exists := dbMap[db]
		if !exists {
			di = &DatabaseInfo{Name: db}
			dbMap[db] = di
		}
		di.FileCount++
		di.SizeBytes += obj.Size

		// Find or create measurement entry
		found := false
		for i := range di.Measurements {
			if di.Measurements[i].Name == meas {
				di.Measurements[i].FileCount++
				di.Measurements[i].SizeBytes += obj.Size
				found = true
				break
			}
		}
		if !found {
			di.Measurements = append(di.Measurements, MeasurementInfo{
				Name:      meas,
				FileCount: 1,
				SizeBytes: obj.Size,
			})
		}
	}
	for _, di := range dbMap {
		manifest.Databases = append(manifest.Databases, *di)
	}

	progress.TotalFiles = manifest.TotalFiles
	progress.TotalBytes = manifest.TotalSizeBytes

	// ── 2. Copy parquet files ───────────────────────────────────────────
	if err := m.copyDataFiles(ctx, backupID, parquetFiles, progress); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// ── 3. Copy SQLite metadata ─────────────────────────────────────────
	if opts.IncludeMetadata && m.sqliteDBPath != "" {
		if err := m.backupSQLite(ctx, backupID); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to backup SQLite database")
			// Non-fatal: continue with backup
		} else {
			manifest.HasMetadata = true
		}
	}

	// ── 4. Copy config ──────────────────────────────────────────────────
	if opts.IncludeConfig && m.configPath != "" {
		if err := m.backupConfig(ctx, backupID); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to backup config file")
		} else {
			manifest.HasConfig = true
		}
	}

	// ── 5. Write manifest ───────────────────────────────────────────────
	// Record files that were inventoried but proved unreadable, so the manifest
	// does not claim contents the backup does not actually hold.
	manifest.SkippedFiles = atomic.LoadInt64(&progress.SkippedFiles)

	manifestData, err := MarshalManifest(manifest)
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}
	manifestPath := fmt.Sprintf("%s/manifest.json", backupID)
	if err := m.backupStorage.Write(ctx, manifestPath, manifestData); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, fmt.Errorf("failed to write manifest: %w", err)
	}

	progress.Status = "completed"
	duration := time.Since(startTime)

	m.logger.Info().
		Str("backup_id", backupID).
		Int64("files", manifest.TotalFiles).
		Int64("bytes", manifest.TotalSizeBytes).
		Int64("skipped", manifest.SkippedFiles).
		Dur("duration", duration).
		Msg("Backup completed")

	return &BackupResult{Manifest: manifest, Duration: duration}, nil
}

// copyDataFiles copies parquet files from data storage to backup storage.
//
// Files whose source cannot be read are skipped (see errBackupRead) and counted
// in progress.SkippedFiles. Every other failure aborts the backup. If every file
// was skipped, the backup fails rather than reporting success over an empty set.
func (m *Manager) copyDataFiles(ctx context.Context, backupID string, files []storage.ObjectInfo, progress *Progress) error {
	var skipped int64

	for _, obj := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		destPath := fmt.Sprintf("%s/data/%s", backupID, obj.Path)
		written, err := m.streamBackupFile(ctx, obj.Path, destPath)
		if err != nil {
			// Only a source-read failure is skippable — the file may have been
			// deleted by compaction/retention between listing and copy. Anything
			// else (temp file, seek, backup write) means the environment is broken
			// and continuing would silently drop files from the backup.
			if !isSourceReadError(err) {
				return fmt.Errorf("failed to back up %s: %w", obj.Path, err)
			}
			skipped++
			m.logger.Warn().Str("path", obj.Path).Err(err).Msg("Failed to read data file, skipping")
			continue
		}

		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, written)

		if atomic.LoadInt64(&progress.ProcessedFiles)%100 == 0 {
			m.logger.Info().
				Int64("processed", atomic.LoadInt64(&progress.ProcessedFiles)).
				Int64("total", progress.TotalFiles).
				Msg("Backup progress")
		}
	}

	atomic.StoreInt64(&progress.SkippedFiles, skipped)

	// Every single file failed to read. That is an unreadable source storage,
	// not the benign delete-during-backup race that skipping exists to tolerate.
	// Reporting success here would produce an empty backup that looks valid.
	if skipped > 0 && atomic.LoadInt64(&progress.ProcessedFiles) == 0 {
		return fmt.Errorf("backup failed: all %d data files were unreadable", skipped)
	}

	if skipped > 0 {
		m.logger.Warn().
			Int64("skipped", skipped).
			Int64("copied", atomic.LoadInt64(&progress.ProcessedFiles)).
			Msg("Backup completed with skipped files — backup is incomplete")
	}

	return nil
}

// streamBackupFile streams a file from data storage to backup storage via a temp file,
// avoiding loading the entire file into memory (important for large Parquet files).
// It returns the number of bytes actually copied.
//
// Only a source-read failure is wrapped with errBackupRead (making it skippable by
// the caller); temp file, seek, and write failures are returned unwrapped and are
// fatal to the backup.
func (m *Manager) streamBackupFile(ctx context.Context, srcPath, destPath string) (int64, error) {
	tmpFile, err := os.CreateTemp("", "arc-backup-*.parquet")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	// Stream from data storage to temp file
	if err := m.dataStorage.ReadTo(ctx, srcPath, tmpFile); err != nil {
		return 0, fmt.Errorf("failed to read from data storage: %w: %w", errBackupRead, err)
	}

	// Size the upload from the temp file rather than the listing: the listing is a
	// point-in-time snapshot that compaction or retention may have invalidated, and
	// a declared size that disagrees with the reader can truncate the upload on
	// backends that send it as Content-Length. Matches streamRestoreFile.
	info, err := tmpFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat temp file: %w", err)
	}
	size := info.Size()

	// Rewind for upload
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return 0, fmt.Errorf("failed to seek temp file: %w", err)
	}

	// Stream from temp file to backup storage
	if err := m.backupStorage.WriteReader(ctx, destPath, tmpFile, size); err != nil {
		return 0, fmt.Errorf("failed to write to backup storage: %w", err)
	}

	return size, nil
}

// backupSQLite copies the SQLite database file into the backup.
// It performs a WAL checkpoint first to ensure a consistent copy.
// The file is streamed via a temp file to avoid loading the entire database into memory.
func (m *Manager) backupSQLite(ctx context.Context, backupID string) error {
	// Checkpoint WAL to ensure all data is flushed to the main DB file.
	db, err := sql.Open("sqlite3", m.sqliteDBPath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite for checkpoint: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		db.Close()
		return fmt.Errorf("WAL checkpoint failed: %w", err)
	}
	db.Close()

	// Get file size for WriteReader
	info, err := os.Stat(m.sqliteDBPath)
	if err != nil {
		return fmt.Errorf("failed to stat SQLite database: %w", err)
	}
	size := info.Size()

	// Stream via temp file to avoid loading entire DB into memory
	f, err := os.Open(m.sqliteDBPath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite database: %w", err)
	}
	defer f.Close()

	destPath := fmt.Sprintf("%s/metadata/arc.db", backupID)
	if err := m.backupStorage.WriteReader(ctx, destPath, f, size); err != nil {
		return fmt.Errorf("failed to write SQLite backup: %w", err)
	}

	m.logger.Info().Str("backup_id", backupID).Int64("bytes", size).Msg("SQLite database backed up")
	return nil
}

// backupConfig copies the arc.toml config file into the backup.
//
// SECURITY: arc.toml typically contains plaintext credentials (S3 secret key,
// Azure account key, cluster shared secret). The backup storage must be treated
// as secret material with the same access controls as the live config.
// Operators who cannot secure backup storage should set backup.include_config
// to false in arc.toml.
func (m *Manager) backupConfig(ctx context.Context, backupID string) error {
	data, err := os.ReadFile(m.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	destPath := fmt.Sprintf("%s/config/arc.toml", backupID)
	if err := m.backupStorage.Write(ctx, destPath, data); err != nil {
		return fmt.Errorf("failed to write config backup: %w", err)
	}

	m.logger.Info().Str("backup_id", backupID).Msg("Config file backed up")
	return nil
}

// parseDBMeasurement extracts the database and measurement from a storage path.
// Path format: {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{file}.parquet
func parseDBMeasurement(path string) (database, measurement string) {
	path = filepath.ToSlash(path)
	parts := strings.SplitN(path, "/", 3)
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	if len(parts) == 1 {
		return parts[0], "unknown"
	}
	return "unknown", "unknown"
}
