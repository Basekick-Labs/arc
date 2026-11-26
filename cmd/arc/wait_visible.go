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

// writeForcingEnabled says whether a visible write may flush the measurements
// it wrote early (#1759): the key on, standalone, local storage. On a cluster
// the header is refused; on object storage a waited write ends 202
// storage_not_confirmable, so an early flush would only add files. The
// handlers' forcer keys off it, and the buffer's ledger is on whenever it is
// (keyFlushLedgerEnabled), so a forcer never meets a ledger that is off. The
// ledger can also be on without it, for fresh queries (#1760); the forcer is
// then not set.
func writeForcingEnabled(cfg *config.Config) bool {
	return cfg.Ingest.WaitVisibleEnabled && !cfg.Cluster.Enabled && cfg.Storage.Backend == "local"
}

// waitVisibleForceInterval is ingest.wait_visible_min_interval_ms, which
// config.Load bounds to [1 s, 1 h].
func waitVisibleForceInterval(cfg *config.Config) time.Duration {
	return time.Duration(cfg.Ingest.WaitVisibleMinIntervalMS) * time.Millisecond
}
