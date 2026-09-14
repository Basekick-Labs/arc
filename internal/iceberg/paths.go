package iceberg

import (
	"path/filepath"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// PathResolver turns Arc's storage-relative file keys (e.g.
// "mydb/cpu/2026/07/13/14/cpu_....parquet") into the fully-qualified URIs iceberg-go reads:
// "file://<abs>" for local, "s3://bucket/prefix/<key>" for S3, "azure://container/<key>" for
// Azure. Resolution goes through storage.ObjectURI, the one validated key-to-location
// builder (#746), so the exporter resolves paths identically to the rest of Arc by
// construction rather than by keeping a copy of the backend type-switch in step.
type PathResolver struct {
	backend storage.Backend
}

// NewPathResolver builds a resolver for the given backend.
func NewPathResolver(backend storage.Backend) *PathResolver {
	return &PathResolver{backend: backend}
}

// localFileURI builds a valid file:// URI from an on-disk path. It resolves to an absolute path
// (a relative path like ./data would be misread as the URI host/authority by Spark/Trino/DuckDB)
// and normalizes separators to forward slashes (backslashes on Windows produce malformed URIs).
// Result is file:///abs/path — the empty authority (three slashes) is the correct local form.
func localFileURI(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		abs = p
	}
	slashed := filepath.ToSlash(abs)
	if !strings.HasPrefix(slashed, "/") {
		// Windows absolute paths (C:/…) need a leading slash after file:// so the drive letter
		// is the path, not the authority: file:///C:/…
		slashed = "/" + slashed
	}
	return "file://" + slashed
}

// DefaultWarehouse returns the Iceberg warehouse root for a backend when none is configured:
// the storage root, so table metadata lands alongside the data (file:// local, s3://bucket/
// prefix for object storage). Iceberg writes {warehouse}/{namespace}.db/{table}/metadata/...
//
// This deliberately does NOT go through storage.ObjectURI, unlike Resolve. The
// warehouse root is the empty key, which ValidateKey rejects because a key must
// name an object, and the result must NOT carry a trailing separator because
// exporter.warehouseRelKey trims this value off a location and then trims the
// separator itself. It is a root, not a key.
func DefaultWarehouse(backend storage.Backend) string {
	switch b := backend.(type) {
	case *storage.S3Backend:
		return strings.TrimSuffix("s3://"+b.GetBucket()+"/"+b.GetPrefix(), "/")
	case *storage.AzureBlobBackend:
		return "azure://" + b.GetContainer()
	case *storage.LocalBackend:
		return localFileURI(b.GetBasePath())
	default:
		return localFileURI("./data")
	}
}

// Resolve returns the iceberg-readable URI for a storage-relative key.
//
// The backend type-switch this used to carry is now storage.ObjectURI, the one
// validated builder every direct object-store reader shares (#746). Two things
// changed as a result. The key is validated rather than repaired: the old
// TrimPrefix(relativeKey, "/") silently mapped "/db/x" and "db/x" onto one URI,
// which is the rewrite-instead-of-reject shape #741 removed from LocalBackend.
// And the local case reuses the backend's own key-to-path mapping instead of
// re-joining the base path, so the URI names the file the backend reads and
// writes by construction.
//
// The only caller is StorageWalkSource.FilesAndLocal, whose input is
// Backend.List output, so a rejection means the listing produced a key the same
// backend would refuse. That is propagated rather than skipped: skipping would
// drop a data file from the exported Iceberg table, which is silent data loss
// in the export, while failing names the problem.
func (r *PathResolver) Resolve(relativeKey string) (string, error) {
	uri, err := storage.ObjectURI(r.backend, relativeKey)
	if err != nil {
		return "", err
	}
	// Object stores are already URIs; a local path needs the file:// form that
	// Spark, Trino and DuckDB expect.
	switch r.backend.(type) {
	case *storage.S3Backend, *storage.AzureBlobBackend:
		return uri, nil
	default:
		return localFileURI(uri), nil
	}
}

// LocalPath returns an on-disk path for a relative key when the backend is local, or ""
// otherwise. The reconciler uses this to sample a hot-tier Parquet file for schema derivation
// (SchemaFromParquet needs a local file).
//
// An unusable key yields "" rather than a repaired path: the caller treats ""
// as "no local sample available", which is the correct outcome for a key that
// names nothing.
func (r *PathResolver) LocalPath(relativeKey string) string {
	b, ok := r.backend.(*storage.LocalBackend)
	if !ok {
		return ""
	}
	path, err := storage.ObjectURI(b, relativeKey)
	if err != nil {
		return ""
	}
	return path
}
