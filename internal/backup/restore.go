package backup

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
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

// RestoreBackup restores data from a backup. It runs synchronously; the API
// layer launches it in a goroutine and exposes progress via GetProgress().
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

	progress.Status = "completed"
	duration := time.Since(startTime)

	m.logger.Info().
		Str("backup_id", opts.BackupID).
		Int64("files_restored", progress.ProcessedFiles).
		Dur("duration", duration).
		Msg("Restore completed")

	return &RestoreResult{Manifest: manifest, Duration: duration}, nil
}

// restoreDataFiles copies parquet files from the backup back into data storage.
func (m *Manager) restoreDataFiles(ctx context.Context, backupID string, manifest *Manifest, progress *Progress) error {
	dataPrefix := backupID + "/data/"

	files, err := m.backupStorage.List(ctx, dataPrefix)
	if err != nil {
		return fmt.Errorf("failed to list backup data files: %w", err)
	}

	progress.TotalFiles = int64(len(files))
	progress.TotalBytes = manifest.TotalSizeBytes

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
			m.logger.Warn().Str("path", srcPath).Err(err).Msg("Failed to restore backup file, skipping")
			continue
		}

		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, bytesWritten)

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
func (m *Manager) streamRestoreFile(ctx context.Context, srcPath, destPath string) (int64, error) {
	tmpFile, err := os.CreateTemp("", "arc-restore-*.parquet")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	if err := os.Chmod(tmpFile.Name(), 0600); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return 0, fmt.Errorf("failed to secure temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	// Stream from backup storage to temp file
	if err := m.backupStorage.ReadTo(ctx, srcPath, tmpFile); err != nil {
		return 0, fmt.Errorf("failed to read from backup: %w", err)
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
		return 0, fmt.Errorf("failed to write to data storage: %w", err)
	}

	return size, nil
}

// restoreSQLite restores the SQLite database from the backup.
// It creates a .before-restore backup of the current database first.
func (m *Manager) restoreSQLite(ctx context.Context, backupID string) error {
	srcPath := fmt.Sprintf("%s/metadata/arc.db", backupID)
	data, err := m.backupStorage.Read(ctx, srcPath)
	if err != nil {
		return fmt.Errorf("failed to read SQLite backup: %w", err)
	}

	// Safety: backup the current database before overwriting
	if _, statErr := os.Stat(m.sqliteDBPath); statErr == nil {
		preRestorePath := m.sqliteDBPath + ".before-restore"
		currentData, err := os.ReadFile(m.sqliteDBPath)
		if err == nil {
			if err := os.WriteFile(preRestorePath, currentData, 0600); err != nil {
				m.logger.Warn().Err(err).Msg("Failed to create pre-restore backup of SQLite database")
			} else {
				m.logger.Info().Str("path", preRestorePath).Msg("Created pre-restore backup of SQLite database")
			}
		}
	}

	if err := os.WriteFile(m.sqliteDBPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write SQLite database: %w", err)
	}

	m.logger.Info().Str("backup_id", backupID).Msg("SQLite database restored")
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
