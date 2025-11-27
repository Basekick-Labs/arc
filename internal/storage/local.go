package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog"
)

// LocalBackend implements the Backend interface for local filesystem storage
type LocalBackend struct {
	basePath string
	logger   zerolog.Logger

	// OPTIMIZATION: Directory cache to avoid redundant os.MkdirAll calls
	// Under sustained load, hundreds of goroutines would call MkdirAll for same dirs
	// causing filesystem lock contention. This cache eliminates that.
	dirCache map[string]bool
	dirMu    sync.RWMutex
}

// NewLocalBackend creates a new local filesystem storage backend
func NewLocalBackend(basePath string, logger zerolog.Logger) (*LocalBackend, error) {
	// Convert to absolute path to avoid issues with filepath.Rel during List operations
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve absolute path: %w", err)
	}

	// Ensure base path exists with owner-only permissions for security
	// Files inside are created with 0600 via os.CreateTemp
	if err := os.MkdirAll(absPath, 0700); err != nil {
		return nil, fmt.Errorf("failed to create base path: %w", err)
	}

	return &LocalBackend{
		basePath: absPath,
		logger:   logger.With().Str("component", "local-storage").Logger(),
		dirCache: make(map[string]bool),
	}, nil
}

// Write writes data to the specified path with atomic write (write to temp, then rename)
func (b *LocalBackend) Write(ctx context.Context, path string, data []byte) error {
	// Validate and sanitize the path to prevent path traversal
	fullPath, err := b.validatePath(path)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	// OPTIMIZATION: Check directory cache first (avoid filesystem lock contention)
	dir := filepath.Dir(fullPath)

	// Fast path: check if directory already exists in cache (RLock)
	b.dirMu.RLock()
	exists := b.dirCache[dir]
	b.dirMu.RUnlock()

	if !exists {
		// Slow path: create directory and update cache (Lock)
		b.dirMu.Lock()
		// Double-check after acquiring write lock
		if !b.dirCache[dir] {
			// Use 0700 for owner-only access (security best practice)
			if err := os.MkdirAll(dir, 0700); err != nil {
				b.dirMu.Unlock()
				return fmt.Errorf("failed to create directory: %w", err)
			}
			b.dirCache[dir] = true
		}
		b.dirMu.Unlock()
	}

	// Write to temporary file with cryptographically random name (prevents TOCTOU attacks)
	tmpFile, err := os.CreateTemp(dir, ".arc-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Write data and close the file
	_, writeErr := tmpFile.Write(data)
	closeErr := tmpFile.Close()
	if writeErr != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to write temp file: %w", writeErr)
	}
	if closeErr != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to close temp file: %w", closeErr)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, fullPath); err != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	b.logger.Debug().
		Str("path", path).
		Int("size", len(data)).
		Msg("Wrote file")

	return nil
}

// WriteReader writes data from a reader to the specified path (for large files)
func (b *LocalBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	// Validate and sanitize the path to prevent path traversal
	fullPath, err := b.validatePath(path)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	// Create parent directory if it doesn't exist (owner-only permissions)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to temporary file with cryptographically random name (prevents TOCTOU attacks)
	tmpFile, err := os.CreateTemp(dir, ".arc-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Copy data
	written, err := io.Copy(tmpFile, reader)
	closeErr := tmpFile.Close()

	if err != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to write data: %w", err)
	}
	if closeErr != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to close temp file: %w", closeErr)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, fullPath); err != nil {
		os.Remove(tmpPath) // Clean up temp file on error
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	b.logger.Debug().
		Str("path", path).
		Int64("size", written).
		Msg("Wrote file from reader")

	return nil
}

// Read reads data from the specified path
func (b *LocalBackend) Read(ctx context.Context, path string) ([]byte, error) {
	// Validate and sanitize the path to prevent path traversal
	fullPath, err := b.validatePath(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", path)
		}
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

// ReadTo reads data from the specified path and writes it to the writer
func (b *LocalBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	// Validate and sanitize the path to prevent path traversal
	fullPath, err := b.validatePath(path)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file not found: %s", path)
		}
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(writer, file)
	if err != nil {
		return fmt.Errorf("failed to copy file data: %w", err)
	}

	return nil
}

// List lists all objects with the given prefix
func (b *LocalBackend) List(ctx context.Context, prefix string) ([]string, error) {
	// Validate and sanitize the prefix to prevent path traversal
	searchPath, err := b.validatePath(prefix)
	if err != nil {
		return nil, fmt.Errorf("invalid prefix: %w", err)
	}
	var results []string

	// Use filepath.Walk to recursively list files
	err = filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Skip directories that don't exist
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Skip hidden files (e.g., .DS_Store on macOS)
		if strings.HasPrefix(info.Name(), ".") {
			return nil
		}

		// Get relative path from base
		relPath, err := filepath.Rel(b.basePath, path)
		if err != nil {
			return err
		}

		results = append(results, relPath)
		return nil
	})

	if err != nil {
		// If the directory doesn't exist, return empty list
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	return results, nil
}

// Delete deletes the object at the specified path
func (b *LocalBackend) Delete(ctx context.Context, path string) error {
	// Validate and sanitize the path to prevent path traversal
	fullPath, err := b.validatePath(path)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil // Already deleted, not an error
		}
		return fmt.Errorf("failed to delete file: %w", err)
	}

	b.logger.Debug().
		Str("path", path).
		Msg("Deleted file")

	return nil
}

// Exists checks if an object exists at the specified path
func (b *LocalBackend) Exists(ctx context.Context, path string) (bool, error) {
	// Validate and sanitize the path to prevent path traversal
	fullPath, err := b.validatePath(path)
	if err != nil {
		return false, fmt.Errorf("invalid path: %w", err)
	}

	_, err = os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check file existence: %w", err)
	}

	return true, nil
}

// Close closes any resources held by the backend (no-op for local storage)
func (b *LocalBackend) Close() error {
	return nil
}

// GetFullPath returns the full filesystem path for a given storage path
// Useful for debugging and direct file access
func (b *LocalBackend) GetFullPath(path string) string {
	// Validate and sanitize the path to prevent path traversal
	fullPath, err := b.validatePath(path)
	if err != nil {
		// Return empty string for invalid paths
		return ""
	}
	return fullPath
}

// GetBasePath returns the base path for the local storage
func (b *LocalBackend) GetBasePath() string {
	return b.basePath
}

// Type returns the storage type identifier
func (b *LocalBackend) Type() string {
	return "local"
}

// ConfigJSON returns the configuration as JSON for subprocess recreation
func (b *LocalBackend) ConfigJSON() string {
	config := map[string]string{"base_path": b.basePath}
	data, _ := json.Marshal(config)
	return string(data)
}

// sanitizePath removes any potentially dangerous path components
func sanitizePath(path string) string {
	// Remove leading slashes
	path = strings.TrimPrefix(path, "/")

	// Replace .. with _ to prevent directory traversal
	path = strings.ReplaceAll(path, "..", "_")

	// Remove any null bytes (can bypass some checks)
	path = strings.ReplaceAll(path, "\x00", "")

	return path
}

// validatePath ensures the resolved path stays within the base path (prevents path traversal)
func (b *LocalBackend) validatePath(path string) (string, error) {
	// First sanitize the path
	sanitized := sanitizePath(path)

	// Join with base path and get the absolute path
	fullPath := filepath.Join(b.basePath, sanitized)
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve path: %w", err)
	}

	// Get absolute base path for comparison
	absBasePath, err := filepath.Abs(b.basePath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve base path: %w", err)
	}

	// Ensure the resolved path is within the base path
	// Use filepath.Rel to check if the path is under basePath
	relPath, err := filepath.Rel(absBasePath, absPath)
	if err != nil {
		return "", fmt.Errorf("path traversal detected")
	}

	// If the relative path starts with "..", it's outside the base path
	if strings.HasPrefix(relPath, "..") {
		return "", fmt.Errorf("path traversal detected: path escapes base directory")
	}

	return absPath, nil
}
