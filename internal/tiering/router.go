package tiering

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

// Router routes queries to the correct storage tier(s)
type Router struct {
	manager *Manager
	logger  zerolog.Logger
}

// NewRouter creates a new tier router
func NewRouter(manager *Manager, logger zerolog.Logger) *Router {
	return &Router{
		manager: manager,
		logger:  logger.With().Str("component", "tiering-router").Logger(),
	}
}

// GetStoragePathsForQuery returns storage paths across all tiers for a query
// This is the primary integration point with the query handler
func (r *Router) GetStoragePathsForQuery(ctx context.Context, database, measurement string, startTime, endTime *time.Time) ([]TieredPath, error) {
	var paths []TieredPath

	// Get all files for this database/measurement from metadata
	files, err := r.manager.metadata.GetFilesByDatabase(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("failed to get files: %w", err)
	}

	// Filter by measurement and time range, group by tier
	tierFiles := make(map[Tier][]string)

	for _, file := range files {
		// Filter by measurement if specified
		if measurement != "" && file.Measurement != measurement {
			continue
		}

		// Filter by time range if specified
		if startTime != nil && file.PartitionTime.Before(*startTime) {
			continue
		}
		if endTime != nil && file.PartitionTime.After(*endTime) {
			continue
		}

		tierFiles[file.Tier] = append(tierFiles[file.Tier], file.Path)
	}

	// Convert to TieredPath list
	for tier, filePaths := range tierFiles {
		backend := r.getBackendName(tier)
		for _, path := range filePaths {
			paths = append(paths, TieredPath{
				Path:    path,
				Tier:    tier,
				Backend: backend,
			})
		}
	}

	return paths, nil
}

// GetGlobPathsForQuery returns glob patterns for each tier
// This is useful when we don't have metadata and need to scan all files
func (r *Router) GetGlobPathsForQuery(database, measurement string) map[Tier]string {
	paths := make(map[Tier]string)

	// Hot tier (local)
	paths[TierHot] = fmt.Sprintf("%s/%s/**/*.parquet", database, measurement)

	// Cold tier (S3/Azure archive) - if enabled
	if r.manager.coldBackend != nil && r.manager.config.Cold.Enabled {
		paths[TierCold] = fmt.Sprintf("%s/%s/**/*.parquet", database, measurement)
	}

	return paths
}

// helper functions

func (r *Router) getBackendName(tier Tier) string {
	switch tier {
	case TierHot:
		return "local"
	case TierCold:
		return r.manager.config.Cold.Backend
	default:
		return "local"
	}
}
