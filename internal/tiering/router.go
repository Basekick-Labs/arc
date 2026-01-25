package tiering

import (
	"context"
	"fmt"
	"strings"
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

// BuildReadParquetExpr builds a DuckDB read_parquet expression for tiered paths
// This generates a UNION ALL query to read from multiple tiers
func (r *Router) BuildReadParquetExpr(paths []TieredPath) string {
	if len(paths) == 0 {
		return ""
	}

	// Group paths by tier
	tierPaths := make(map[Tier][]string)
	for _, p := range paths {
		tierPaths[p.Tier] = append(tierPaths[p.Tier], p.Path)
	}

	// If only one tier, return simple expression
	if len(tierPaths) == 1 {
		for _, filePaths := range tierPaths {
			return r.buildReadParquet(filePaths)
		}
	}

	// Multiple tiers: build UNION ALL expression
	var parts []string

	// Process tiers in order: hot, cold (2-tier system)
	for _, tier := range []Tier{TierHot, TierCold} {
		if filePaths, ok := tierPaths[tier]; ok && len(filePaths) > 0 {
			parts = append(parts, fmt.Sprintf("SELECT * FROM %s", r.buildReadParquet(filePaths)))
		}
	}

	if len(parts) == 1 {
		return r.buildReadParquet(tierPaths[TierHot])
	}

	return fmt.Sprintf("(%s)", strings.Join(parts, " UNION ALL "))
}

// BuildMultiTierQuery builds a complete DuckDB query that reads from all tiers
func (r *Router) BuildMultiTierQuery(database, measurement string, whereClause string) string {
	globPaths := r.GetGlobPathsForQuery(database, measurement)

	var parts []string

	// Hot tier
	if hotPath, ok := globPaths[TierHot]; ok {
		hotBackend := r.manager.hotBackend
		fullPath := r.buildFullPath(hotBackend.Type(), hotPath)
		parts = append(parts, fmt.Sprintf("SELECT * FROM read_parquet('%s')", fullPath))
	}

	// Cold tier
	if coldPath, ok := globPaths[TierCold]; ok && r.manager.coldBackend != nil {
		fullPath := r.buildFullPath(r.manager.coldBackend.Type(), coldPath)
		parts = append(parts, fmt.Sprintf("SELECT * FROM read_parquet('%s')", fullPath))
	}

	if len(parts) == 0 {
		return ""
	}

	query := strings.Join(parts, " UNION ALL ")

	if whereClause != "" {
		query = fmt.Sprintf("SELECT * FROM (%s) WHERE %s", query, whereClause)
	}

	return query
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

func (r *Router) buildReadParquet(paths []string) string {
	if len(paths) == 0 {
		return ""
	}

	if len(paths) == 1 {
		return fmt.Sprintf("read_parquet('%s')", paths[0])
	}

	// Multiple paths: use list
	quotedPaths := make([]string, len(paths))
	for i, p := range paths {
		quotedPaths[i] = fmt.Sprintf("'%s'", p)
	}
	return fmt.Sprintf("read_parquet([%s], union_by_name=true)", strings.Join(quotedPaths, ", "))
}

func (r *Router) buildFullPath(backendType, path string) string {
	switch backendType {
	case "s3":
		bucket := r.manager.config.Cold.S3Bucket
		return fmt.Sprintf("s3://%s/%s", bucket, path)
	case "azure":
		container := r.manager.config.Cold.AzureContainer
		return fmt.Sprintf("azure://%s/%s", container, path)
	case "local":
		// For local, the path is already relative to storage root
		return path
	default:
		return path
	}
}
