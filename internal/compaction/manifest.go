package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// ManifestStatus represents the state of a compaction manifest
type ManifestStatus string

const (
	// ManifestStatusPending indicates the compaction is in progress
	ManifestStatusPending ManifestStatus = "pending"
)

// ManifestBasePath is the base directory for storing compaction manifests
const ManifestBasePath = "_compaction_state"

// Manifest tracks the state of a compaction operation for crash recovery.
// If a pod crashes after uploading the compacted file but before deleting
// source files, the manifest allows recovery to complete the deletion.
type Manifest struct {
	// Output file information
	OutputPath string `json:"output_path"` // Full storage path of compacted file
	OutputSize int64  `json:"output_size"` // Expected size of output file (for validation)

	// Input files that were compacted
	InputFiles []string `json:"input_files"`

	// Metadata
	Database      string         `json:"database"`
	Measurement   string         `json:"measurement"`
	PartitionPath string         `json:"partition_path"`
	Tier          string         `json:"tier"`
	Status        ManifestStatus `json:"status"`
	CreatedAt     time.Time      `json:"created_at"`
	JobID         string         `json:"job_id"`
}

// ManifestManager handles reading, writing, and recovering from compaction manifests
type ManifestManager struct {
	backend storage.Backend
	logger  zerolog.Logger
	mu      sync.Mutex

	// Cache of manifest paths to input files for quick lookup during candidate filtering
	// Key: manifest path, Value: set of input file paths
	manifestCache     map[string]map[string]struct{}
	manifestCacheMu   sync.RWMutex
	manifestCacheTime time.Time
	cacheTTL          time.Duration
}

// NewManifestManager creates a new manifest manager
func NewManifestManager(backend storage.Backend, logger zerolog.Logger) *ManifestManager {
	return &ManifestManager{
		backend:       backend,
		logger:        logger.With().Str("component", "manifest-manager").Logger(),
		manifestCache: make(map[string]map[string]struct{}),
		cacheTTL:      30 * time.Second,
	}
}

// GenerateManifestPath generates a unique manifest path for a compaction job
func (m *ManifestManager) GenerateManifestPath(tier, database, partitionPath, jobID string) string {
	// Path format: _compaction_state/{tier}/{database}/{partition_path_sanitized}_{jobID}.json
	sanitizedPartition := strings.ReplaceAll(partitionPath, "/", "_")
	return filepath.Join(ManifestBasePath, tier, database, fmt.Sprintf("%s_%s.json", sanitizedPartition, jobID))
}

// WriteManifest writes a manifest to storage
func (m *ManifestManager) WriteManifest(ctx context.Context, manifest *Manifest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	manifestPath := m.GenerateManifestPath(manifest.Tier, manifest.Database, manifest.PartitionPath, manifest.JobID)

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal manifest: %w", err)
	}

	if err := m.backend.Write(ctx, manifestPath, data); err != nil {
		return "", fmt.Errorf("failed to write manifest to %s: %w", manifestPath, err)
	}

	m.logger.Debug().
		Str("path", manifestPath).
		Str("output", manifest.OutputPath).
		Int("input_count", len(manifest.InputFiles)).
		Msg("Wrote compaction manifest")

	// Invalidate cache
	m.invalidateCache()

	return manifestPath, nil
}

// DeleteManifest removes a manifest from storage
func (m *ManifestManager) DeleteManifest(ctx context.Context, manifestPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.backend.Delete(ctx, manifestPath); err != nil {
		return fmt.Errorf("failed to delete manifest %s: %w", manifestPath, err)
	}

	m.logger.Debug().Str("path", manifestPath).Msg("Deleted compaction manifest")

	// Invalidate cache
	m.invalidateCache()

	return nil
}

// ReadManifest reads a manifest from storage
func (m *ManifestManager) ReadManifest(ctx context.Context, manifestPath string) (*Manifest, error) {
	data, err := m.backend.Read(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest %s: %w", manifestPath, err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest %s: %w", manifestPath, err)
	}

	return &manifest, nil
}

// ListManifests lists all manifest files in storage
func (m *ManifestManager) ListManifests(ctx context.Context) ([]string, error) {
	objects, err := m.backend.List(ctx, ManifestBasePath+"/")
	if err != nil {
		// If the directory doesn't exist, return empty list
		return []string{}, nil
	}

	var manifests []string
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".json") {
			manifests = append(manifests, obj)
		}
	}

	return manifests, nil
}

// RecoverOrphanedManifests finds and processes orphaned manifests from interrupted compactions.
// Returns the number of manifests recovered and any error encountered.
func (m *ManifestManager) RecoverOrphanedManifests(ctx context.Context) (int, error) {
	manifests, err := m.ListManifests(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to list manifests: %w", err)
	}

	if len(manifests) == 0 {
		return 0, nil
	}

	m.logger.Info().Int("count", len(manifests)).Msg("Found orphaned manifests, starting recovery")

	var recovered int
	for _, manifestPath := range manifests {
		select {
		case <-ctx.Done():
			return recovered, ctx.Err()
		default:
		}

		if err := m.recoverManifest(ctx, manifestPath); err != nil {
			m.logger.Error().Err(err).Str("manifest", manifestPath).Msg("Failed to recover manifest")
			continue
		}
		recovered++
	}

	m.logger.Info().Int("recovered", recovered).Int("total", len(manifests)).Msg("Manifest recovery complete")
	return recovered, nil
}

// recoverManifest processes a single orphaned manifest
func (m *ManifestManager) recoverManifest(ctx context.Context, manifestPath string) error {
	manifest, err := m.ReadManifest(ctx, manifestPath)
	if err != nil {
		// If we can't read the manifest, delete it and let compaction retry
		m.logger.Warn().Err(err).Str("manifest", manifestPath).Msg("Cannot read manifest, deleting")
		return m.DeleteManifest(ctx, manifestPath)
	}

	m.logger.Info().
		Str("manifest", manifestPath).
		Str("output", manifest.OutputPath).
		Int("inputs", len(manifest.InputFiles)).
		Msg("Processing orphaned manifest")

	// Check if output file exists
	exists, err := m.backend.Exists(ctx, manifest.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to check output file existence: %w", err)
	}

	if !exists {
		// Output file doesn't exist - compaction was interrupted before upload completed
		// Delete manifest and let compaction retry
		m.logger.Info().
			Str("manifest", manifestPath).
			Str("output", manifest.OutputPath).
			Msg("Output file missing, deleting manifest for retry")
		return m.DeleteManifest(ctx, manifestPath)
	}

	// Output file exists - verify size if we have ObjectLister
	if objectLister, ok := m.backend.(storage.ObjectLister); ok {
		objects, err := objectLister.ListObjects(ctx, manifest.OutputPath)
		if err == nil && len(objects) > 0 {
			actualSize := objects[0].Size
			if actualSize != manifest.OutputSize {
				// Size mismatch - partial upload, delete output and manifest for retry
				m.logger.Warn().
					Str("manifest", manifestPath).
					Int64("expected_size", manifest.OutputSize).
					Int64("actual_size", actualSize).
					Msg("Output file size mismatch, deleting for retry")

				if err := m.backend.Delete(ctx, manifest.OutputPath); err != nil {
					m.logger.Warn().Err(err).Str("output", manifest.OutputPath).Msg("Failed to delete partial output")
				}
				return m.DeleteManifest(ctx, manifestPath)
			}
		}
	}

	// Output file exists and is valid - complete the deletion of input files
	m.logger.Info().
		Str("manifest", manifestPath).
		Int("inputs", len(manifest.InputFiles)).
		Msg("Output file valid, completing input file deletion")

	var deleteErrors int
	for _, inputFile := range manifest.InputFiles {
		if err := m.backend.Delete(ctx, inputFile); err != nil {
			// Check if file already deleted
			exists, checkErr := m.backend.Exists(ctx, inputFile)
			if checkErr == nil && !exists {
				// File already deleted, continue
				continue
			}
			m.logger.Warn().Err(err).Str("file", inputFile).Msg("Failed to delete input file during recovery")
			deleteErrors++
		}
	}

	if deleteErrors > 0 {
		m.logger.Warn().
			Int("errors", deleteErrors).
			Int("total", len(manifest.InputFiles)).
			Msg("Some input files could not be deleted during recovery")
	}

	// Delete the manifest to complete recovery
	return m.DeleteManifest(ctx, manifestPath)
}

// GetFilesInManifests returns a set of all input files currently tracked by manifests.
// This is used to exclude files from compaction candidate scans.
func (m *ManifestManager) GetFilesInManifests(ctx context.Context) (map[string]struct{}, error) {
	m.manifestCacheMu.RLock()
	if time.Since(m.manifestCacheTime) < m.cacheTTL && len(m.manifestCache) > 0 {
		// Return cached result
		result := make(map[string]struct{})
		for _, files := range m.manifestCache {
			for f := range files {
				result[f] = struct{}{}
			}
		}
		m.manifestCacheMu.RUnlock()
		return result, nil
	}
	m.manifestCacheMu.RUnlock()

	// Rebuild cache
	m.manifestCacheMu.Lock()
	defer m.manifestCacheMu.Unlock()

	// Double-check after acquiring write lock
	if time.Since(m.manifestCacheTime) < m.cacheTTL && len(m.manifestCache) > 0 {
		result := make(map[string]struct{})
		for _, files := range m.manifestCache {
			for f := range files {
				result[f] = struct{}{}
			}
		}
		return result, nil
	}

	manifests, err := m.ListManifests(ctx)
	if err != nil {
		return nil, err
	}

	newCache := make(map[string]map[string]struct{})
	result := make(map[string]struct{})

	for _, manifestPath := range manifests {
		manifest, err := m.ReadManifest(ctx, manifestPath)
		if err != nil {
			m.logger.Warn().Err(err).Str("manifest", manifestPath).Msg("Failed to read manifest for cache")
			continue
		}

		files := make(map[string]struct{})
		for _, f := range manifest.InputFiles {
			files[f] = struct{}{}
			result[f] = struct{}{}
		}
		// Also add output file to prevent re-compaction
		result[manifest.OutputPath] = struct{}{}
		newCache[manifestPath] = files
	}

	m.manifestCache = newCache
	m.manifestCacheTime = time.Now()

	return result, nil
}

// invalidateCache clears the manifest cache
func (m *ManifestManager) invalidateCache() {
	m.manifestCacheMu.Lock()
	defer m.manifestCacheMu.Unlock()
	m.manifestCache = make(map[string]map[string]struct{})
	m.manifestCacheTime = time.Time{}
}

// IsFileInManifest checks if a file is tracked by any manifest
func (m *ManifestManager) IsFileInManifest(ctx context.Context, filePath string) (bool, error) {
	files, err := m.GetFilesInManifests(ctx)
	if err != nil {
		return false, err
	}
	_, exists := files[filePath]
	return exists, nil
}
