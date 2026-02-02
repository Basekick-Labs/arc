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

		data, err := m.backupStorage.Read(ctx, srcPath)
		if err != nil {
			m.logger.Warn().Str("path", srcPath).Err(err).Msg("Failed to read backup file, skipping")
			continue
		}

		if err := m.dataStorage.Write(ctx, destPath, data); err != nil {
			return fmt.Errorf("failed to restore file %s: %w", destPath, err)
		}

		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, int64(len(data)))

		if atomic.LoadInt64(&progress.ProcessedFiles)%100 == 0 {
			m.logger.Info().
				Int64("processed", atomic.LoadInt64(&progress.ProcessedFiles)).
				Int64("total", progress.TotalFiles).
				Msg("Restore progress")
		}
	}

	return nil
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
			if err := os.WriteFile(preRestorePath, currentData, 0644); err != nil {
				m.logger.Warn().Err(err).Msg("Failed to create pre-restore backup of config")
			} else {
				m.logger.Info().Str("path", preRestorePath).Msg("Created pre-restore backup of config")
			}
		}
	}

	if err := os.WriteFile(m.configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	m.logger.Info().Str("backup_id", backupID).Msg("Config file restored (restart required)")
	return nil
}
