package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrResumeNotSupported is returned by AppendingBackend.AppendReader on
// backends that do not support append writes (S3, Azure Blob Storage).
// Callers should delete any partial file and retry from byte zero.
var ErrResumeNotSupported = errors.New("storage: resume not supported by this backend")

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

	// ReadToAt reads data from path starting at the given byte offset and writes
	// to writer. offset=0 starts at the beginning (equivalent to ReadTo).
	// Returns an error if offset is negative or >= file size.
	ReadToAt(ctx context.Context, path string, writer io.Writer, offset int64) error

	// StatFile returns the byte size of the file at path.
	// Returns -1 (and nil error) if the file does not exist.
	// Returns a non-nil error only for unexpected backend failures.
	StatFile(ctx context.Context, path string) (int64, error)

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

// AppendingBackend is an optional extension of Backend for backends that
// support appending bytes to an existing file. Callers type-assert Backend to
// AppendingBackend before calling AppendReader; if the assertion fails, the
// backend does not support resumable writes and the caller should fall back to
// a full re-fetch.
//
// Only local-SSD backends implement this. S3 and Azure Blob Storage do not
// support append on block objects and return ErrResumeNotSupported instead.
type AppendingBackend interface {
	Backend
	// AppendReader appends bytes from reader to the existing file at path.
	// appendSize is the number of bytes expected from reader (informational;
	// implementations may ignore it). Returns ErrResumeNotSupported if the
	// backend cannot append.
	AppendReader(ctx context.Context, path string, reader io.Reader, appendSize int64) error
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
