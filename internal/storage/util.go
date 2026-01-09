package storage

import "github.com/rs/zerolog"

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
		return "s3://" + b.GetBucket() + "/" + database + "/" + measurement + "/**/*.parquet"
	case *AzureBlobBackend:
		return "azure://" + b.GetContainer() + "/" + database + "/" + measurement + "/**/*.parquet"
	case *LocalBackend:
		return b.GetBasePath() + "/" + database + "/" + measurement + "/**/*.parquet"
	default:
		return "./data/" + database + "/" + measurement + "/**/*.parquet"
	}
}
