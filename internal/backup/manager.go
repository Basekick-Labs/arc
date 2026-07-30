package backup

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Manager orchestrates backup and restore operations.
type Manager struct {
	dataStorage   storage.Backend // primary data storage
	backupStorage storage.Backend // default local backup destination
	sqliteDBPath  string          // path to shared SQLite database
	configPath    string          // path to arc.toml
	// icebergCatalogDBPath is the Iceberg SQL catalog, backed up separately only
	// when iceberg.catalog_db_path points somewhere other than the shared DB.
	// The catalog holds every Iceberg table's schema and snapshot pointers, so a
	// backup without it restores data whose tables no longer resolve.
	icebergCatalogDBPath string

	logger zerolog.Logger
	mu     sync.Mutex // serializes backup/restore operations
	active atomic.Pointer[Progress]
}

// ManagerConfig holds configuration for creating a backup manager.
type ManagerConfig struct {
	DataStorage  storage.Backend
	BackupPath   string // local directory for backups
	SQLiteDBPath string
	// IcebergCatalogDBPath is the Iceberg SQL catalog path. Leave empty, or set
	// equal to SQLiteDBPath, when the catalog lives in the shared database —
	// it is then already covered by the shared-database backup.
	IcebergCatalogDBPath string
	ConfigPath           string
	Logger               zerolog.Logger
}

// NewManager creates a new backup manager.
func NewManager(cfg *ManagerConfig) (*Manager, error) {
	if cfg.DataStorage == nil {
		return nil, fmt.Errorf("data storage backend is required")
	}
	if cfg.BackupPath == "" {
		return nil, fmt.Errorf("backup path is required")
	}

	backupBackend, err := storage.NewLocalBackend(cfg.BackupPath, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup storage: %w", err)
	}

	// Only treat the Iceberg catalog as a separate database when it really is a
	// different file. Both paths default to the same value, and a relative vs
	// absolute spelling of one file must not produce a redundant second copy.
	icebergCatalog := cfg.IcebergCatalogDBPath
	if icebergCatalog != "" && sameFilePath(icebergCatalog, cfg.SQLiteDBPath) {
		icebergCatalog = ""
	}

	return &Manager{
		dataStorage:          cfg.DataStorage,
		backupStorage:        backupBackend,
		sqliteDBPath:         cfg.SQLiteDBPath,
		icebergCatalogDBPath: icebergCatalog,
		configPath:           cfg.ConfigPath,
		logger:               cfg.Logger.With().Str("component", "backup-manager").Logger(),
	}, nil
}

// sameFilePath reports whether two configured paths refer to the same file,
// comparing cleaned absolute paths with symlinks resolved when they exist.
func sameFilePath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	resolve := func(p string) string {
		abs, err := filepath.Abs(p)
		if err != nil {
			return filepath.Clean(p)
		}
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			return resolved
		}
		return abs
	}
	return resolve(a) == resolve(b)
}

// generateBackupID creates a unique backup identifier.
func generateBackupID() string {
	now := time.Now().UTC()
	short := uuid.New().String()[:8]
	return fmt.Sprintf("backup-%s-%s", now.Format("20060102-150405"), short)
}

// GetProgress returns the current active operation progress, or nil if idle.
func (m *Manager) GetProgress() *Progress {
	return m.active.Load()
}

// setProgress stores the current operation progress.
func (m *Manager) setProgress(p *Progress) {
	m.active.Store(p)
}

// ListBackups returns summaries of all available backups in the default backup storage.
func (m *Manager) ListBackups(ctx context.Context) ([]BackupSummary, error) {
	// Each backup is a directory with a manifest.json inside
	files, err := m.backupStorage.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to list backup storage: %w", err)
	}

	// Find all manifest.json files
	manifests := make(map[string]bool)
	for _, f := range files {
		if strings.HasSuffix(f, "/manifest.json") {
			manifests[f] = true
		}
	}

	var summaries []BackupSummary
	for manifestPath := range manifests {
		data, err := m.backupStorage.Read(ctx, manifestPath)
		if err != nil {
			m.logger.Warn().Str("path", manifestPath).Err(err).Msg("Failed to read manifest, skipping")
			continue
		}
		manifest, err := UnmarshalManifest(data)
		if err != nil {
			m.logger.Warn().Str("path", manifestPath).Err(err).Msg("Failed to parse manifest, skipping")
			continue
		}
		summaries = append(summaries, SummaryFromManifest(manifest))
	}

	return summaries, nil
}

// GetBackup reads and returns the manifest for a specific backup.
func (m *Manager) GetBackup(ctx context.Context, backupID string) (*Manifest, error) {
	manifestPath := fmt.Sprintf("%s/manifest.json", backupID)
	data, err := m.backupStorage.Read(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("backup not found: %w", err)
	}
	return UnmarshalManifest(data)
}

// DeleteBackup removes all files for a backup from the default backup storage.
func (m *Manager) DeleteBackup(ctx context.Context, backupID string) error {
	// List all files under this backup ID
	prefix := backupID + "/"
	files, err := m.backupStorage.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("failed to list backup files: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("backup not found: %s", backupID)
	}

	// Use batch delete if available
	if bd, ok := m.backupStorage.(storage.BatchDeleter); ok {
		if err := bd.DeleteBatch(ctx, files); err != nil {
			return fmt.Errorf("failed to delete backup: %w", err)
		}
	} else {
		for _, f := range files {
			if err := m.backupStorage.Delete(ctx, f); err != nil {
				m.logger.Warn().Str("path", f).Err(err).Msg("Failed to delete backup file")
			}
		}
	}

	m.logger.Info().Str("backup_id", backupID).Int("files_deleted", len(files)).Msg("Backup deleted")
	return nil
}
