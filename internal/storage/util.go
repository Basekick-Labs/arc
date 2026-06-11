package storage

import (
	"context"
	"errors"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// recordStorageError increments the storage-error counter unless the
// operation failed because the caller cancelled or timed out its context —
// caller-side lifecycle events (client disconnects, shutdown), not
// storage-backend failures. Matches the treatment of context.Canceled in
// the query and puller paths. Used by the cloud backends, whose SDK calls
// honour ctx; local file ops don't fail on cancellation, so the local
// backend increments the counter directly.
func recordStorageError(err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
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

// GetStoragePath returns the full storage path for a database/measurement with glob pattern.
// Supports all storage backends: local, S3, and Azure.
func GetStoragePath(backend Backend, database, measurement string) string {
	switch b := backend.(type) {
	case *S3Backend:
		return "s3://" + b.GetBucket() + "/" + b.GetPrefix() + database + "/" + measurement + "/**/*.parquet"
	case *AzureBlobBackend:
		return "azure://" + b.GetContainer() + "/" + database + "/" + measurement + "/**/*.parquet"
	case *LocalBackend:
		return b.GetBasePath() + "/" + database + "/" + measurement + "/**/*.parquet"
	default:
		return "./data/" + database + "/" + measurement + "/**/*.parquet"
	}
}
