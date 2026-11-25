package main

import (
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/config"
)

// waitVisibleConfig is x-arc-wait: visible's view of the configuration
// (#1759). It reads only cfg, never the cluster router, which is nil without
// a coordinator: a cluster is refused on cfg.Cluster.Enabled alone. The
// file-time pruning margin is the value the query pruner gets
// (queryHandler.SetFileTimePruning).
func waitVisibleConfig(cfg *config.Config) api.WaitVisibleConfig {
	return api.WaitVisibleConfig{
		Enabled:               cfg.Ingest.WaitVisibleEnabled,
		Clustered:             cfg.Cluster.Enabled,
		StorageBackend:        cfg.Storage.Backend,
		FileTimePruning:       cfg.Query.FileTimePruning,
		FileTimePruningMargin: time.Duration(cfg.Query.FileTimePruningMarginSeconds) * time.Second,
	}
}
