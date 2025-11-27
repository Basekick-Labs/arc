package storage

import (
	"context"
	"io"
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
