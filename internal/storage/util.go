package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// recordStorageError increments the storage-error counter unless the
// operation failed because the caller cancelled or timed out its context —
// caller-side lifecycle events (client disconnects, shutdown), not
// storage-backend failures. Matches the treatment of context.Canceled in
// the query and puller paths.
//
// Both checks are needed: errors.Is catches SDK errors that wrap the
// context error, while ctx.Err() catches failures that surface as plain
// network errors (e.g. EPIPE / connection reset when a disconnecting
// client's writer kills an io.Copy) without wrapping the context error.
func recordStorageError(ctx context.Context, err error) {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	metrics.Get().IncStorageErrors()
}

// GetLocalBasePath returns the base filesystem path for local storage backends.
// For cloud backends (S3, Azure), it logs a warning and returns empty string.
// For unknown backends, it returns the provided fallback path.
//
// Parameters:
//   - backend: The storage backend to check
//   - logger: Logger for warnings about unsupported backends (can be nil)
//   - feature: Feature name for warning messages (e.g., "Continuous queries", "Retention")
//   - fallback: Default path to return for unknown backend types (use "" to disable)
func GetLocalBasePath(backend Backend, logger *zerolog.Logger, feature string, fallback string) string {
	switch b := backend.(type) {
	case *LocalBackend:
		return b.GetBasePath()
	case *S3Backend:
		if logger != nil {
			logger.Warn().Msgf("%s not fully supported for S3 backend yet", feature)
		}
		return ""
	case *AzureBlobBackend:
		if logger != nil {
			logger.Warn().Msgf("%s not fully supported for Azure backend yet", feature)
		}
		return ""
	default:
		return fallback
	}
}

// defaultBackendRoot is the location assumed for a Backend this package does
// not recognise. It matches the default data directory.
const defaultBackendRoot = "./data"

// backendRoot returns the location every key in backend hangs off, with a
// trailing separator, so root+key is that key's fully-qualified location.
//
// This is the ONE place the backend type-switch for direct-reader URIs lives.
// Before #746 it was written out longhand in ten places (storage.GetStoragePath,
// three dead S3 helpers, retention.buildParquetPath, delete.getQueryPath and
// two in iceberg), and they disagreed: #258 added the S3 prefix to some copies
// and missed others, which left retention reading a location nothing was ever
// written to.
//
// The matching INVERSE (URL back to a backend-relative key) deliberately does
// not live here. Its one consumer is partition pruning, which must invert a
// URL for a tier whose backend is not the one it holds, so it trims the root
// parsed out of that tier's own glob instead of type-switching on a backend.
// See PartitionPruner.extractStoragePrefix.
//
// The local case reuses LocalBackend's own pathPrefix rather than joining the
// base path again. The two differ when basePath is "/", and the backend's own
// mapping is by definition the one that names the file it reads and writes.
func backendRoot(backend Backend) string {
	switch b := backend.(type) {
	case *S3Backend:
		return "s3://" + b.bucket + "/" + b.prefix
	case *AzureBlobBackend:
		// Azure has no prefix concept: the key IS the blob name.
		return "azure://" + b.containerName + "/"
	case *LocalBackend:
		return b.pathPrefix
	default:
		return defaultBackendRoot + "/"
	}
}

// ObjectURI returns the fully-qualified location of key for engines that read
// the object store DIRECTLY rather than through Backend: DuckDB's read_parquet
// over httpfs, and iceberg-go's FileIO.
//
// Those engines are the reason #743's contract was not enough on its own. A
// Backend method validates its key, but these callers never call one: they hand
// a URI to another process. This is the second chokepoint, and it enforces the
// same contract.
//
// It does NOT apply ValidateGlobSafe, and that is deliberate. Glob-safety is a
// property of the SINK, not of the location: iceberg-go opens this path as a
// literal object and os.Open takes it verbatim, so a key containing "*" is
// perfectly readable there and rejecting it would fail an export over a file
// that resolves fine. Only a caller interpolating the result into a DuckDB
// path needs the extra rule, and those callers apply it themselves.
//
// The returned local path is the on-disk path DuckDB wants. Iceberg wraps it in
// a file:// URI itself.
func ObjectURI(backend Backend, key string) (string, error) {
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	return backendRoot(backend) + key, nil
}

// GetStoragePath returns the read_parquet glob covering every Parquet file
// written for one database and measurement.
//
// database and measurement are validated as single path SEGMENTS, not as a
// joined key: ValidateKey(database+"/"+measurement) would accept a measurement
// of "a/b" and silently produce a glob over a different directory. They are
// also checked for glob metacharacters, which the key contract deliberately
// permits and a read path cannot.
//
// Returning an error is the point of the change: before #746 this built a URI
// from whatever it was handed, and DuckDB read it.
func GetStoragePath(backend Backend, database, measurement string) (string, error) {
	if err := validateReadSegment("database", database); err != nil {
		return "", err
	}
	if err := validateReadSegment("measurement", measurement); err != nil {
		return "", err
	}
	return backendRoot(backend) + database + "/" + measurement + "/**/*.parquet", nil
}

// validateReadSegment applies both segment rules a read path needs and names
// which component failed, since the caller passes two and the error is
// otherwise ambiguous.
func validateReadSegment(what, seg string) error {
	if err := ValidateKeySegment(seg); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if err := ValidateGlobSafe(seg); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}
