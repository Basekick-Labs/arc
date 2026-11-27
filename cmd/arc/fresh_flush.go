package main

import (
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/config"
)

// freshFlushEnabled says whether a query may flush and wait for the
// measurements it reads (x-arc-freshness: flush, #1760): the key on,
// standalone, local storage. The buffer's task tracking keys off it, and the
// handler refuses the header on the same terms (freshFlushConfig), so a
// handler that waits always has a buffer that can.
func freshFlushEnabled(cfg *config.Config) bool {
	return cfg.Query.FreshFlushEnabled && !cfg.Cluster.Enabled && cfg.Storage.Backend == "local"
}

// keyFlushLedgerEnabled: the buffer's per-measurement flush ledger, which
// both visible writes (#1759) and fresh queries force through. One ledger,
// one forced-flush budget, shared by the two.
func keyFlushLedgerEnabled(cfg *config.Config) bool {
	return writeForcingEnabled(cfg) || freshFlushEnabled(cfg)
}

// freshFlushConfig is x-arc-freshness's view of the configuration. Like
// waitVisibleConfig it reads only cfg, never the cluster router.
func freshFlushConfig(cfg *config.Config) api.FreshFlushConfig {
	return api.FreshFlushConfig{
		Enabled:        cfg.Query.FreshFlushEnabled,
		Clustered:      cfg.Cluster.Enabled,
		StorageBackend: cfg.Storage.Backend,
		MinInterval:    time.Duration(cfg.Query.FreshFlushMinIntervalMS) * time.Millisecond,
		MaxWaiters:     cfg.Query.FreshFlushMaxWaiters,
		RetryAfter:     time.Duration(cfg.WAL.RecoveryIntervalSeconds) * time.Second,
	}
}
