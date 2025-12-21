package storage

import (
	"context"
	"io"
	"time"
)

// Backend defines the interface for storage backends (local, S3, MinIO)
type Backend interface {
	// Write writes data to the specified path
	Write(ctx context.Context, path string, data []byte) error

	// WriteReader writes data from a reader to the specified path (for large files)
	WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error

	// Read reads data from the specified path
	Read(ctx context.Context, path string) ([]byte, error)

	// ReadTo reads data from the specified path and writes it to the writer
	ReadTo(ctx context.Context, path string, writer io.Writer) error

	// List lists all objects with the given prefix
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete deletes the object at the specified path
	Delete(ctx context.Context, path string) error

	// Exists checks if an object exists at the specified path
	Exists(ctx context.Context, path string) (bool, error)

	// Close closes any resources held by the backend
	Close() error

	// Type returns the storage type identifier ("local", "s3", etc.)
	// Used for subprocess serialization
	Type() string

	// ConfigJSON returns the configuration as JSON for subprocess recreation
	// Used for subprocess serialization
	ConfigJSON() string
}

// DirectoryLister lists immediate subdirectories at a prefix.
// This is useful for SHOW DATABASES/TABLES commands.
type DirectoryLister interface {
	ListDirectories(ctx context.Context, prefix string) ([]string, error)
}

// BatchDeleter supports efficient batch deletion of multiple objects.
// Implementations should handle batching internally (e.g., S3 supports up to 1000 objects per batch).
type BatchDeleter interface {
	DeleteBatch(ctx context.Context, paths []string) error
}

// ObjectInfo provides metadata about a storage object.
type ObjectInfo struct {
	Path         string
	Size         int64
	LastModified time.Time
}

// ObjectLister lists objects with their metadata.
// This is useful for retention policies that need to check file ages.
type ObjectLister interface {
	ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// DirectoryRemover removes an empty directory.
// This is used to clean up database directories after all files are deleted.
// For object storage (S3, Azure), this is typically a no-op since directories don't exist as objects.
type DirectoryRemover interface {
	RemoveDirectory(ctx context.Context, path string) error
}
