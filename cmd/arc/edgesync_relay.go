package main

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/logger"
	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// openRelayBackend builds the relay bucket's backend (#1685) through the
// same factory as primary storage, and closes it after the relay's
// schedulers have stopped (a storage-priority hook runs after the
// scheduler-priority one).
func openRelayBackend(t config.BackupTargetConfig, key string, sc *shutdown.Coordinator) storage.Backend {
	b, err := storage.NewBackend(t.BackendSpec(), logger.Get("edgesync-relay-storage"))
	if err != nil {
		log.Fatal().Err(err).Str("key", key).Msg("Failed to open the edge sync relay bucket; refusing to start")
	}
	sc.RegisterHook(key+"-backend", func(context.Context) error { return b.Close() }, shutdown.PriorityStorage)
	return b
}

// scheduleRelay runs a relay pass on a timer through the edge sync scheduler,
// which re-checks the licence and (on a cluster) the primary-writer role on
// every tick. Returns whether it is running: without a valid licence it is
// not, and the manual route stays available.
func scheduleRelay(name string, runner edgesync.SyncRunner, interval time.Duration,
	licenseClient *license.Client, clusterCoordinator *cluster.Coordinator,
	sc *shutdown.Coordinator, lg zerolog.Logger) bool {
	if licenseClient == nil || !licenseClient.CanUseEdgeSyncScheduler() {
		lg.Info().Msg("Scheduled relay passes require a valid paid license; the manual relay route remains available")
		return false
	}
	var gate edgesync.WriterGate
	if clusterCoordinator != nil {
		gate = newWriterClusterGate(clusterCoordinator)
	}
	// Retry at the interval itself: a bundle that keeps failing must not
	// make the relay poll (and re-fetch) faster than configured.
	retry := interval
	s, err := edgesync.NewScheduler(edgesync.SchedulerConfig{
		Agent:         runner,
		LicenseClient: licenseClient,
		SyncInterval:  interval,
		RetryInterval: retry,
		Enabled:       true,
		ClusterGate:   gate,
		// Warn and up: a 1m hub poll would otherwise log every quiet pass.
		// Passes that did something log themselves (RelayPoller.Runner).
		Logger: lg.Level(zerolog.WarnLevel),
	})
	if err != nil {
		log.Fatal().Err(err).Str("relay", name).Msg("Failed to configure the relay scheduler; refusing to start")
	}
	if err := s.Start(); err != nil {
		log.Fatal().Err(err).Str("relay", name).Msg("Failed to start the relay scheduler; refusing to start")
	}
	sc.RegisterHook(name+"-scheduler", func(context.Context) error {
		s.Stop()
		return nil
	}, shutdown.PriorityScheduler)
	return s.IsRunning()
}

// startHubRelay wires the hub's relay poller (edge_sync.import.relay.*).
func startHubRelay(cfg *config.Config, importer *edgesync.Importer, authManager *auth.AuthManager,
	licenseClient *license.Client, clusterCoordinator *cluster.Coordinator, server *api.Server,
	sc *shutdown.Coordinator, lg zerolog.Logger) {
	rc := cfg.EdgeSync.Import.Relay
	relay := openRelayBackend(rc.Target, "edgesync-relay-hub", sc)
	poller, err := edgesync.NewRelayPoller(edgesync.RelayPollerConfig{
		Relay: relay, Importer: importer, StagingDir: rc.StagingDir, HubID: cfg.EdgeSync.HubID, Logger: lg,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create the edge sync relay poller; refusing to start")
	}
	h, err := api.NewEdgeSyncRelayHubHandler(poller, authManager, cfg.EdgeSync.Import.Timeout, lg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create the edge sync relay handler; refusing to start")
	}
	h.RegisterRoutes(server.GetApp())
	scheduled := scheduleRelay("edgesync-relay-hub", poller.Runner(), rc.Interval, licenseClient, clusterCoordinator, sc, lg)
	lg.Info().Str("type", rc.Target.Type).Dur("interval", rc.Interval).Bool("scheduled", scheduled).
		Msg("Edge sync relay enabled on the hub; poll manually with POST /api/v1/bundle-import/relay/run")
}

// startSpokeRelay wires the spoke's relay pass (edge_sync.spoke.relay.*).
func startSpokeRelay(cfg *config.Config, exporter *edgesync.Exporter, licenseClient *license.Client,
	clusterCoordinator *cluster.Coordinator, sc *shutdown.Coordinator, lg zerolog.Logger) *edgesync.RelaySpoke {
	rc := cfg.EdgeSync.Spoke.Relay
	relay := openRelayBackend(rc.Target, "edgesync-relay-spoke", sc)
	r, err := edgesync.NewRelaySpoke(edgesync.RelaySpokeConfig{
		Exporter: exporter, Relay: relay, AckTimeout: rc.AckTimeout, Logger: lg,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create the edge sync relay; refusing to start")
	}
	scheduled := scheduleRelay("edgesync-relay-spoke", r.Runner(), rc.Interval, licenseClient, clusterCoordinator, sc, lg)
	lg.Info().Str("type", rc.Target.Type).Dur("interval", rc.Interval).Dur("ack_timeout", rc.AckTimeout).
		Bool("scheduled", scheduled).
		Msg("Edge sync relay enabled on the spoke; run a pass manually with POST /api/v1/spoke-sync/relay/run")
	return r
}
