package main

import (
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/iceberg"
	"github.com/basekick-labs/arc/internal/logger"
	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

// icebergCatalogNotHere is the 503 a node answers when it does not hold the
// Iceberg warehouse.
const icebergCatalogNotHere = "this node does not hold the Iceberg export: with node-local storage only the node that runs the export " +
	"has the table metadata; point the engine's catalog URI at that node"

// icebergCatalogServable decides, per request, whether this node can answer
// the Iceberg REST catalog (#1690). A warehouse every node sees (a shared
// bucket, or shared_storage_mode) can be served anywhere, and a node with no
// export gate exports itself. Otherwise only the node the export's gate lets
// run holds the metadata, and the others answer 503 rather than an empty
// catalog. That is keyed on the gate, not on the coordinator: cluster.enabled
// without one (no clustering licence, or a coordinator that failed to start)
// still gates the export on the static compactor role. The gate is asked per
// request because the role moves without a restart.
func icebergCatalogServable(sharedWarehouse bool, gate iceberg.WriterGate) func() (bool, string) {
	if sharedWarehouse || gate == nil {
		return nil
	}
	return func() (bool, string) {
		if gate.CanRun() {
			return true, ""
		}
		return false, icebergCatalogNotHere
	}
}

// registerIcebergRESTCatalog mounts the read-only Iceberg REST catalog. It is
// called after the global auth and audit middleware are on the app, so both
// cover it. rbacManager and spokeRegistry may be nil (no RBAC wired, not a
// hub); authManager nil means auth is off.
func registerIcebergRESTCatalog(app *fiber.App, exporter *iceberg.Exporter, authManager *auth.AuthManager, rbacManager *auth.RBACManager,
	spokeRegistry *edgesync.Registry, backend storage.Backend, servable func() (bool, string)) {
	// A local warehouse outside the storage root is never published
	// (writeVersionHint), so the catalog over it would be empty forever.
	if !exporter.PublishedAddressable() {
		log.Fatal().Msg("iceberg.rest_catalog_enabled=true needs iceberg.warehouse inside the storage root: the catalog serves the " +
			"published table versions, and a warehouse outside the storage root is never published")
	}
	h := api.NewIcebergCatalogHandler(exporter, authManager, logger.Get("iceberg"))
	if rbacManager != nil {
		h.SetRBAC(rbacManager)
	}
	if spokeRegistry != nil {
		h.SetSpokeNamespaces(receivedNamespaces(spokeRegistry))
	}
	h.SetServable(servable)
	// Client-overridable defaults. The region only: an S3 endpoint is often
	// an internal hostname, and credentials are never sent.
	if s3, ok := backend.(*storage.S3Backend); ok && s3.GetRegion() != "" {
		h.SetDefaults(map[string]string{"s3.region": s3.GetRegion()})
	}
	h.RegisterRoutes(app)
	log.Info().Str("path", api.IcebergCatalogBasePath).Bool("auth", authManager != nil).Bool("rbac", rbacManager != nil).
		Msg("Iceberg REST catalog enabled (read-only): clients use uri=<arc base URL>" + api.IcebergCatalogBasePath)
}

// wireIcebergGlue builds the Glue sync (#1690 option B) and returns the
// export's start function extended to start it: the sync runs only where and
// when the export runs, on the export's own gate (nil: standalone, always).
// The exporter reports each publish to it. At shutdown it stops after the
// export's final drain and syncs what that drain published (#1886).
func wireIcebergGlue(exporter *iceberg.Exporter, backend storage.Backend, gate iceberg.WriterGate, gcfg config.IcebergGlueConfig,
	layout string, sc *shutdown.Coordinator, shutdownTimeout time.Duration, start func()) func() {
	region := iceberg.GlueRegion(backend, gcfg.Region)
	client, err := iceberg.NewGlueClient(backend, region)
	if err != nil {
		log.Fatal().Err(err).Msg("iceberg.glue.enabled=true: cannot build the Glue client")
	}
	gs := iceberg.NewGlueSync(iceberg.GlueSyncConfig{
		Exporter:  exporter,
		API:       client,
		CatalogID: gcfg.CatalogID,
		Interval:  time.Duration(gcfg.SyncInterval) * time.Second,
		Gate:      gate,
		Logger:    logger.Get("iceberg"),
	})
	exporter.SetOnPublished(gs.Notify)
	catalog := gcfg.CatalogID
	if catalog == "" {
		catalog = "(the credentials' account)"
	}
	log.Info().Str("region", region).Str("catalog_id", catalog).Int("retry_interval_s", gcfg.SyncInterval).
		Msg("Iceberg tables are registered in AWS Glue (iceberg.glue.enabled): one Glue table per published table, updated on each publish")
	// Glue, unlike Arc's REST catalog, accepts writes: an engine that drops
	// or purges a table through it deletes the data files the metadata names,
	// which are Arc's own in either layout. gc.enabled=false would stop that
	// in Spark and Java, but iceberg-go then refuses Arc's own snapshot
	// expiry, so the control is the consumers' S3 permissions.
	where := "the data files the table's metadata names are Arc's own"
	if layout == config.IcebergLayoutColocated {
		where += ", and each Glue table's location is the measurement's data directory"
	}
	log.Warn().Msg("Iceberg Glue sync: Glue accepts writes, and " + where + ". Give Athena, Spark and every other Glue consumer read-only " +
		"S3 access to Arc's bucket and leave Glue table optimizers off: a DROP TABLE ... PURGE, VACUUM or orphan-file cleanup run " +
		"through Glue would delete Arc's data")
	registerIcebergGlueShutdown(sc, &icebergGlueShutdown{flush: gs.Flush, timeout: shutdownTimeout})
	return func() {
		if start != nil {
			start()
		}
		gs.Start()
	}
}
