package main

import (
	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/iceberg"
	"github.com/basekick-labs/arc/internal/logger"
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
