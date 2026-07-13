package iceberg

import (
	"path/filepath"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// PathResolver turns Arc's storage-relative file keys (e.g.
// "mydb/cpu/2026/07/13/14/cpu_....parquet") into the fully-qualified URIs iceberg-go reads:
// "file://<abs>" for local, "s3://bucket/prefix/<key>" for S3, "azure://container/<key>" for
// Azure. Mirrors the backend type-switch in storage.GetStoragePath / retention.buildParquetPath
// so the exporter resolves paths identically to the rest of Arc.
type PathResolver struct {
	backend storage.Backend
}

// NewPathResolver builds a resolver for the given backend.
func NewPathResolver(backend storage.Backend) *PathResolver {
	return &PathResolver{backend: backend}
}

// DefaultWarehouse returns the Iceberg warehouse root for a backend when none is configured:
// the storage root, so table metadata lands alongside the data (file:// local, s3://bucket/
// prefix for object storage). Iceberg writes {warehouse}/{namespace}.db/{table}/metadata/...
func DefaultWarehouse(backend storage.Backend) string {
	switch b := backend.(type) {
	case *storage.S3Backend:
		return strings.TrimSuffix("s3://"+b.GetBucket()+"/"+b.GetPrefix(), "/")
	case *storage.AzureBlobBackend:
		return "azure://" + b.GetContainer()
	case *storage.LocalBackend:
		return "file://" + b.GetBasePath()
	default:
		return "file://./data"
	}
}

// Resolve returns the iceberg-readable URI for a storage-relative key.
func (r *PathResolver) Resolve(relativeKey string) string {
	key := strings.TrimPrefix(relativeKey, "/")
	switch b := r.backend.(type) {
	case *storage.S3Backend:
		return "s3://" + b.GetBucket() + "/" + b.GetPrefix() + key
	case *storage.AzureBlobBackend:
		return "azure://" + b.GetContainer() + "/" + key
	case *storage.LocalBackend:
		return "file://" + filepath.Join(b.GetBasePath(), key)
	default:
		return "file://" + filepath.Join("./data", key)
	}
}

// LocalPath returns an on-disk path for a relative key when the backend is local, or ""
// otherwise. The reconciler uses this to sample a hot-tier Parquet file for schema derivation
// (SchemaFromParquet needs a local file).
func (r *PathResolver) LocalPath(relativeKey string) string {
	if b, ok := r.backend.(*storage.LocalBackend); ok {
		return filepath.Join(b.GetBasePath(), strings.TrimPrefix(relativeKey, "/"))
	}
	return ""
}
