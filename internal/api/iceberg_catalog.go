package api

// Arc's read-only Iceberg REST catalog (#1690).
//
// It serves the read side of the Iceberg REST catalog spec over the tables the
// Iceberg export publishes, so engines that find tables through a catalog
// (Snowflake ICEBERG_REST with auto-refresh, Trino, Spark, PyIceberg, DuckDB,
// Polaris federation) can list and load them. Clients use the base URI
// http(s)://<arc>/iceberg and Arc tokens as bearer tokens.
//
// What is served is the published version (iceberg.Exporter.LoadPublished):
// the v<N>.metadata.json that version-hint.text names, the same file on every
// node that sees the warehouse. Arc is the only writer of its tables, so every
// write operation is refused (406) and none is advertised in /v1/config.
//
// Absence is reported only when confirmed. A storage or permission-store
// failure is a 503, never a 404 or a shorter listing, and is never cached: a
// remote catalog that sees a table vanish from one poll can drop it on its
// side.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/iceberg"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

// IcebergCatalogBasePath is where the catalog is mounted; clients append the
// spec's /v1/... paths to it.
const IcebergCatalogBasePath = "/iceberg"

// icebergListingTTL is how long a database or table listing is reused. Loads
// are never cached, so a new publish is visible on the next load; a listing
// learns of a new table within this. Snowflake's catalog sync defaults to 30 s.
const icebergListingTTL = 30 * time.Second

// icebergCacheSweepAt is the cache size at which expired entries are swept.
const icebergCacheSweepAt = 256

// icebergNamespaceScanParallel bounds how many databases the namespace
// listing checks at once.
const icebergNamespaceScanParallel = 8

// icebergRequestTimeout bounds one request's storage reads.
const icebergRequestTimeout = 30 * time.Second

// icebergEndpoints are the spec's endpoint templates Arc serves, exactly as
// clients compare them ({prefix} literal). Advertising them tells a spec-1.x
// client not to attempt anything else, writes included.
var icebergEndpoints = []string{
	"GET /v1/{prefix}/namespaces",
	"GET /v1/{prefix}/namespaces/{namespace}",
	"HEAD /v1/{prefix}/namespaces/{namespace}",
	"GET /v1/{prefix}/namespaces/{namespace}/tables",
	"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"GET /v1/{prefix}/namespaces/{namespace}/views",
}

// IcebergPublishedSource is what the catalog reads: *iceberg.Exporter.
type IcebergPublishedSource interface {
	NamespacePrefix() string
	PublishedDatabases(ctx context.Context) ([]string, error)
	PublishedTables(ctx context.Context, database string, limit int) ([]string, error)
	LoadPublished(ctx context.Context, database, measurement string) (iceberg.PublishedTable, bool, error)
}

// IcebergCatalogRBAC is the subset of *auth.RBACManager the catalog asks.
// Every method reports a store failure distinctly from a denial.
type IcebergCatalogRBAC interface {
	CheckPermission(req *auth.PermissionCheckRequest) *auth.PermissionCheckResult
	CanAccessAnythingInErr(tokenInfo *auth.TokenInfo, database, permission string) (bool, error)
	HasPermissionAnywhere(tokenInfo *auth.TokenInfo, permission string) (bool, error)
}

// IcebergCatalogHandler serves the catalog.
type IcebergCatalogHandler struct {
	src         IcebergPublishedSource
	authManager *auth.AuthManager
	rbac        IcebergCatalogRBAC
	// servable, when set, reports whether this node can answer; a node that
	// does not hold the warehouse (node-local storage, not the exporter)
	// answers 503 with the reason rather than an empty catalog.
	servable func() (bool, string)
	// awaitFresh, when set, waits briefly for the files reported for a
	// table before its load (#1752).
	awaitFresh func(ctx context.Context, database, measurement string)
	// spokes, on an edge-sync hub, names the storage-root directories that
	// are spokes, not databases; the namespace listing does not scan them.
	spokes   func(ctx context.Context) (map[string]struct{}, error)
	defaults map[string]string
	logger   zerolog.Logger

	ttl   time.Duration
	now   func() time.Time
	sf    singleflight.Group
	mu    sync.Mutex
	cache map[string]icebergCachedList
}

type icebergCachedList struct {
	names []string
	at    time.Time
}

// NewIcebergCatalogHandler builds the handler. authManager nil means auth is
// off: every table is served.
func NewIcebergCatalogHandler(src IcebergPublishedSource, authManager *auth.AuthManager, logger zerolog.Logger) *IcebergCatalogHandler {
	return &IcebergCatalogHandler{
		src:         src,
		authManager: authManager,
		defaults:    map[string]string{},
		logger:      logger.With().Str("component", "iceberg-catalog").Logger(),
		ttl:         icebergListingTTL,
		now:         time.Now,
		cache:       map[string]icebergCachedList{},
	}
}

// SetRBAC wires the permission checker. Leave it unset when RBAC is not
// wired; the token's coarse permissions then decide.
func (h *IcebergCatalogHandler) SetRBAC(r IcebergCatalogRBAC) { h.rbac = r }

// SetServable sets the per-request "can this node answer" check.
func (h *IcebergCatalogHandler) SetServable(f func() (bool, string)) { h.servable = f }

// SetAwaitFresh makes a table load first wait, briefly, for the files
// ingest reported for the table before it (iceberg.load_export_min_interval,
// #1752). nil, the default, reads the published version at once.
func (h *IcebergCatalogHandler) SetAwaitFresh(f func(ctx context.Context, database, measurement string)) {
	h.awaitFresh = f
}

// SetSpokeNamespaces names an edge-sync hub's spoke directories, which the
// namespace listing skips: they hold no table of a single-level namespace, so
// scanning them is pure cost. A spoke named directly resolves to no table.
func (h *IcebergCatalogHandler) SetSpokeNamespaces(f func(ctx context.Context) (map[string]struct{}, error)) {
	h.spokes = f
}

// SetDefaults sets the client-overridable properties /v1/config returns.
// Never credentials.
func (h *IcebergCatalogHandler) SetDefaults(d map[string]string) {
	h.defaults = map[string]string{}
	for k, v := range d {
		h.defaults[k] = v
	}
}

// RegisterRoutes mounts the catalog under IcebergCatalogBasePath. The app's
// global auth middleware authenticates first (the path is not public); the
// group's door then requires read authority somewhere and answers in the
// spec's error shape, and each handler checks the database and table.
func (h *IcebergCatalogHandler) RegisterRoutes(app fiber.Router) {
	g := app.Group(IcebergCatalogBasePath, h.door)
	// Fiber registers HEAD alongside every GET, so the exists-checks are
	// answered inside the GET handlers.
	g.Get("/v1/config", h.handleConfig)
	g.Get("/v1/namespaces", h.handleListNamespaces)
	g.Get("/v1/namespaces/:namespace", h.handleGetNamespace)
	g.Get("/v1/namespaces/:namespace/tables", h.handleListTables)
	g.Get("/v1/namespaces/:namespace/tables/:table", h.handleLoadTable)
	g.Get("/v1/namespaces/:namespace/views", h.handleListViews)
	g.Get("/v1/namespaces/:namespace/views/:view", func(c *fiber.Ctx) error {
		return icebergError(c, fiber.StatusNotFound, "NoSuchViewException", "Arc's Iceberg catalog has no views")
	})
	g.All("/*", h.handleUnsupported)
}

// icebergError writes the spec's ErrorModel.
func icebergError(c *fiber.Ctx, status int, typ, msg string) error {
	return c.Status(status).JSON(fiber.Map{"error": fiber.Map{"message": msg, "type": typ, "code": status}})
}

func icebergUnavailable(c *fiber.Ctx, msg string) error {
	return icebergError(c, fiber.StatusServiceUnavailable, "ServiceUnavailableException", msg)
}

// door admits a caller holding read authority on anything, the same question
// the query routes ask at theirs (#1559), and refuses on a node that cannot
// answer. Per-database and per-table checks follow in the handlers.
func (h *IcebergCatalogHandler) door(c *fiber.Ctx) error {
	if h.servable != nil {
		if ok, why := h.servable(); !ok {
			return icebergUnavailable(c, why)
		}
	}
	if h.authManager == nil {
		return c.Next()
	}
	token := auth.GetTokenInfo(c)
	if token == nil {
		return icebergError(c, fiber.StatusUnauthorized, "NotAuthorizedException", "Authentication required")
	}
	var allowed bool
	if h.rbac == nil {
		allowed = h.authManager.HasPermission(token, "read")
	} else {
		var err error
		if allowed, err = h.rbac.HasPermissionAnywhere(token, "read"); err != nil {
			h.logger.Error().Err(err).Int64("token_id", token.ID).Msg("RBAC grants could not be read; refusing the Iceberg catalog request (fail closed)")
			return icebergUnavailable(c, "Permission store unavailable; retry later")
		}
	}
	if !allowed {
		return icebergError(c, fiber.StatusForbidden, "ForbiddenException", "Permission denied: read required")
	}
	return c.Next()
}

// canSeeDatabase answers "may this caller enumerate inside database".
func (h *IcebergCatalogHandler) canSeeDatabase(c *fiber.Ctx, database string) (bool, error) {
	if h.authManager == nil {
		return true, nil
	}
	token := auth.GetTokenInfo(c)
	if token == nil {
		return false, nil
	}
	if h.rbac == nil {
		return h.authManager.HasPermission(token, "read"), nil
	}
	return h.rbac.CanAccessAnythingInErr(token, database, "read")
}

// canReadTable answers "may this caller read measurement of database".
func (h *IcebergCatalogHandler) canReadTable(c *fiber.Ctx, database, measurement string) (bool, error) {
	if h.authManager == nil {
		return true, nil
	}
	token := auth.GetTokenInfo(c)
	if token == nil {
		return false, nil
	}
	if h.rbac == nil {
		return h.authManager.HasPermission(token, "read"), nil
	}
	res := h.rbac.CheckPermission(&auth.PermissionCheckRequest{
		TokenInfo: token, Database: database, Measurement: measurement, Permission: "read",
	})
	if res.Source == auth.SourceUnavailable {
		return false, errIcebergPermissionStore
	}
	return res.Allowed, nil
}

var errIcebergPermissionStore = errors.New("permission store unavailable")

// pathSegment returns a route parameter decoded, as an owned string. Clients
// escape a space, '#' or '?' in a name, so the segment is decoded once; the
// decoded name then goes through SafeCatalogName, which refuses a separator,
// a control character (a multi-level namespace's %1F) and a second '%'.
func pathSegment(c *fiber.Ctx, name string) (string, bool) {
	v, err := url.PathUnescape(c.Params(name))
	if err != nil || v == "" {
		return "", false
	}
	return strings.Clone(v), true
}

// databaseOf maps a namespace segment to the database it serves.
func (h *IcebergCatalogHandler) databaseOf(ns string) (string, bool) {
	db, ok := strings.CutPrefix(ns, h.src.NamespacePrefix()+"_")
	if !ok || !iceberg.SafeCatalogName(db) {
		return "", false
	}
	return db, true
}

func (h *IcebergCatalogHandler) namespaceOf(db string) []string {
	return []string{h.src.NamespacePrefix() + "_" + db}
}

// cachedList returns a listing from the cache, or computes it once for every
// concurrent caller on a context detached from any one request, so a client
// that disconnects does not fail the others. Errors are not cached.
func (h *IcebergCatalogHandler) cachedList(key string, compute func(ctx context.Context) ([]string, error)) ([]string, error) {
	h.mu.Lock()
	if e, ok := h.cache[key]; ok && h.now().Sub(e.at) < h.ttl {
		h.mu.Unlock()
		return e.names, nil
	}
	h.mu.Unlock()
	v, err, _ := h.sf.Do(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*icebergRequestTimeout)
		defer cancel()
		names, err := compute(ctx)
		if err != nil {
			return nil, err
		}
		// An empty listing is not cached: keys come from caller-supplied
		// namespaces, and caching every name a caller tries would grow
		// without bound. Expired entries are swept as the map grows.
		if len(names) > 0 {
			h.mu.Lock()
			now := h.now()
			if len(h.cache) >= icebergCacheSweepAt {
				for k, e := range h.cache {
					if now.Sub(e.at) >= h.ttl {
						delete(h.cache, k)
					}
				}
			}
			h.cache[key] = icebergCachedList{names: names, at: now}
			h.mu.Unlock()
		}
		return names, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]string), nil
}

func (h *IcebergCatalogHandler) tables(database string) ([]string, error) {
	return h.cachedList("tables\x00"+database, func(ctx context.Context) ([]string, error) {
		return h.src.PublishedTables(ctx, database, 0)
	})
}

// databases are the databases holding at least one published table.
func (h *IcebergCatalogHandler) databases() ([]string, error) {
	return h.cachedList("databases", func(ctx context.Context) ([]string, error) {
		candidates, err := h.src.PublishedDatabases(ctx)
		if err != nil {
			return nil, err
		}
		var spokes map[string]struct{}
		if h.spokes != nil {
			if spokes, err = h.spokes(ctx); err != nil {
				return nil, err
			}
		}
		has := make([]bool, len(candidates))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(icebergNamespaceScanParallel)
		for i, db := range candidates {
			if _, isSpoke := spokes[db]; isSpoke {
				continue
			}
			g.Go(func() error {
				one, err := h.src.PublishedTables(gctx, db, 1)
				has[i] = len(one) > 0
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		out := make([]string, 0, len(candidates))
		for i, db := range candidates {
			if has[i] {
				out = append(out, db)
			}
		}
		return out, nil
	})
}

// storageUnavailable answers 503 for a failure. The detail goes to the log
// only: it can name other databases (a listing reads every database before
// any per-caller filter) and, on local storage, absolute paths.
func (h *IcebergCatalogHandler) storageUnavailable(c *fiber.Ctx, err error, what string) error {
	if errors.Is(err, errIcebergPermissionStore) {
		return icebergUnavailable(c, "Permission store unavailable; retry later")
	}
	h.logger.Warn().Err(err).Str("path", c.Path()).Msg("Iceberg catalog could not read " + what)
	return icebergUnavailable(c, "Arc could not read "+what+" right now; retry later (the Arc log has the cause)")
}

func (h *IcebergCatalogHandler) handleConfig(c *fiber.Ctx) error {
	// The warehouse query parameter (Snowflake's CATALOG_NAME, PyIceberg's
	// warehouse) is accepted and ignored: Arc serves one catalog.
	return c.JSON(fiber.Map{
		"defaults":  h.defaults,
		"overrides": map[string]string{},
		"endpoints": icebergEndpoints,
	})
}

func (h *IcebergCatalogHandler) handleListNamespaces(c *fiber.Ctx) error {
	// Arc's namespaces are one level: nothing lives under a parent.
	if c.Query("parent") != "" {
		return c.JSON(fiber.Map{"namespaces": [][]string{}})
	}
	dbs, err := h.databases()
	if err != nil {
		return h.storageUnavailable(c, err, "the Iceberg warehouse")
	}
	out := make([][]string, 0, len(dbs))
	for _, db := range dbs {
		ok, err := h.canSeeDatabase(c, db)
		if err != nil {
			return h.storageUnavailable(c, errIcebergPermissionStore, "")
		}
		if ok {
			out = append(out, h.namespaceOf(db))
		}
	}
	return c.JSON(fiber.Map{"namespaces": out})
}

// namespace resolves the :namespace parameter to a database the caller may
// see and that holds a published table. done is set when a response has been
// written.
func (h *IcebergCatalogHandler) namespace(c *fiber.Ctx) (db string, done bool, err error) {
	notFound := func() (string, bool, error) {
		return "", true, icebergError(c, fiber.StatusNotFound, "NoSuchNamespaceException", "Namespace does not exist: "+c.Params("namespace"))
	}
	ns, ok := pathSegment(c, "namespace")
	if !ok {
		return notFound()
	}
	db, ok = h.databaseOf(ns)
	if !ok {
		return notFound()
	}
	visible, err := h.canSeeDatabase(c, db)
	if err != nil {
		return "", true, h.storageUnavailable(c, errIcebergPermissionStore, "")
	}
	if !visible {
		return notFound()
	}
	tables, err := h.tables(db)
	if err != nil {
		return "", true, h.storageUnavailable(c, err, "the Iceberg warehouse")
	}
	if len(tables) == 0 {
		return notFound()
	}
	return db, false, nil
}

func (h *IcebergCatalogHandler) handleGetNamespace(c *fiber.Ctx) error {
	db, done, err := h.namespace(c)
	if done {
		return err
	}
	if c.Method() == fiber.MethodHead {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.JSON(fiber.Map{"namespace": h.namespaceOf(db), "properties": map[string]string{}})
}

func (h *IcebergCatalogHandler) handleListTables(c *fiber.Ctx) error {
	db, done, err := h.namespace(c)
	if done {
		return err
	}
	tables, err := h.tables(db)
	if err != nil {
		return h.storageUnavailable(c, err, "the Iceberg warehouse")
	}
	type ident struct {
		Namespace []string `json:"namespace"`
		Name      string   `json:"name"`
	}
	out := make([]ident, 0, len(tables))
	for _, m := range tables {
		ok, err := h.canReadTable(c, db, m)
		if err != nil {
			return h.storageUnavailable(c, err, "")
		}
		if ok {
			out = append(out, ident{Namespace: h.namespaceOf(db), Name: m})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return c.JSON(fiber.Map{"identifiers": out})
}

func (h *IcebergCatalogHandler) handleListViews(c *fiber.Ctx) error {
	if _, done, err := h.namespace(c); done {
		return err
	}
	return c.JSON(fiber.Map{"identifiers": []any{}})
}

type icebergLoadTableResult struct {
	MetadataLocation string            `json:"metadata-location"`
	Metadata         json.RawMessage   `json:"metadata"`
	Config           map[string]string `json:"config"`
}

func (h *IcebergCatalogHandler) handleLoadTable(c *fiber.Ctx) error {
	notFound := func() error {
		return icebergError(c, fiber.StatusNotFound, "NoSuchTableException",
			"Table does not exist: "+c.Params("namespace")+"."+c.Params("table"))
	}
	ns, ok := pathSegment(c, "namespace")
	if !ok {
		return notFound()
	}
	m, ok := pathSegment(c, "table")
	if !ok || !iceberg.SafeCatalogName(m) {
		return notFound()
	}
	db, ok := h.databaseOf(ns)
	if !ok {
		return notFound()
	}
	// A table the caller may not read is a 404, as if it were not there: a
	// 403 would confirm it exists, and clients map only 404 to "does not
	// exist" in their exists-checks.
	allowed, err := h.canReadTable(c, db, m)
	if err != nil {
		return h.storageUnavailable(c, err, "")
	}
	if !allowed {
		return notFound()
	}
	ctx, cancel := context.WithTimeout(c.Context(), icebergRequestTimeout)
	defer cancel()
	// Only a GET, and only for a table the caller may read: the wait can move
	// the table's next export earlier (bounded per table by the load
	// interval), never commit from here.
	if h.awaitFresh != nil && c.Method() != fiber.MethodHead {
		h.awaitFresh(ctx, db, m)
	}
	pt, found, err := h.src.LoadPublished(ctx, db, m)
	if err != nil {
		return h.storageUnavailable(c, err, "the table's published Iceberg metadata")
	}
	if !found {
		return notFound()
	}
	if c.Method() == fiber.MethodHead {
		return c.SendStatus(fiber.StatusNoContent)
	}
	// No credential vending: engines read storage with their own credentials
	// (Snowflake's external volume, the client's S3 settings).
	return c.JSON(icebergLoadTableResult{MetadataLocation: pt.MetadataLocation, Metadata: pt.Metadata, Config: map[string]string{}})
}

// handleUnsupported answers every path and method the catalog does not
// serve: a write is refused as unsupported (Arc is the only writer of its
// tables), an unknown read path is a 404.
func (h *IcebergCatalogHandler) handleUnsupported(c *fiber.Ctx) error {
	switch c.Method() {
	case fiber.MethodGet, fiber.MethodHead:
		return icebergError(c, fiber.StatusNotFound, "NotFoundException",
			"Unknown Iceberg REST path "+c.Path()+": Arc's catalog is at "+IcebergCatalogBasePath+"/v1/... and takes no prefix")
	}
	return icebergError(c, http.StatusNotAcceptable, "UnsupportedOperationException",
		"Arc's Iceberg catalog is read-only: Arc is the only writer of its tables")
}
