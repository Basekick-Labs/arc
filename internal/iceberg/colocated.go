package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// Co-located table metadata (#1676).
//
// The default ("warehouse") layout puts a table's metadata under
// <warehouse>/<prefix>_<db>.db/<measurement>/metadata, apart from the data,
// and its manifests reference Arc's Parquet by absolute path. Readers that
// reach a table only through its folder cannot follow those paths: a
// OneLake shortcut converts the metadata but Fabric's Spark fails on the
// first data file.
//
// The "colocated" layout puts a NEW table's location at the measurement's own
// directory, <storage root>/<db>/<measurement>, so its metadata lands in
// <db>/<measurement>/metadata and every data file is under the table folder.
// The layout only decides where tables are created. An existing table keeps
// its location, whichever layout is set: every path that reads or publishes
// a table works from its catalog row, so switching the key, either way, is
// safe and moves nothing. With no catalog row (a node that never exported
// the table) adoption looks in the configured layout's directory first and
// then in the other one, so a table created under the other layout is
// adopted rather than created a second time.

// Layouts accepted by SetLayout; the same strings as config.IcebergLayout*.
const (
	layoutWarehouse = "warehouse"
	layoutColocated = "colocated"
)

// SetLayout selects where new tables are created. Colocated needs the
// warehouse to be the storage root, compared after both are resolved (a
// trailing slash, ./data, a bare path or a symlink spell the same directory
// differently, #639, #1616), and a backend whose root can be named as a
// table location (local or S3, which is what the export supports).
//
// Call it during wiring, before the reconcile scheduler starts.
func (e *Exporter) SetLayout(layout string) error {
	switch layout {
	case "", layoutWarehouse:
		e.layout = layoutWarehouse
		return nil
	case layoutColocated:
	default:
		return fmt.Errorf("unknown Iceberg layout %q", layout)
	}
	switch e.backend.(type) {
	case *storage.LocalBackend, *storage.S3Backend:
	default:
		return fmt.Errorf("iceberg.layout=%q needs local or S3 storage", layoutColocated)
	}
	e.resolveBases()
	if e.resolvedWarehouse != e.resolvedRoot {
		return fmt.Errorf("iceberg.layout=%q puts table metadata in each measurement's directory, so the warehouse "+
			"must be the storage root; iceberg.warehouse is %q and the storage root is %q: remove iceberg.warehouse "+
			"or set it to the storage root", layoutColocated, e.warehouse, DefaultWarehouse(e.backend))
	}
	e.layout = layoutColocated
	return nil
}

func (e *Exporter) colocatedLayout() bool { return e.layout == layoutColocated }

// colocatedTableLocation is the location a colocated table is created at:
// the measurement's directory, as a URI iceberg-go parses (it joins
// "metadata" onto it with url.JoinPath). Built with url.URL so a name with a
// space, %, # or ? is escaped rather than read as a fragment or a query.
func (e *Exporter) colocatedTableLocation(database, measurement string) (string, error) {
	if err := storage.ValidateKey(database + "/" + measurement); err != nil {
		return "", fmt.Errorf("table location for %s/%s: %w", database, measurement, err)
	}
	switch b := e.backend.(type) {
	case *storage.LocalBackend:
		return localFileURI(filepath.Join(b.GetBasePath(), filepath.FromSlash(database), measurement)), nil
	case *storage.S3Backend:
		return (&url.URL{Scheme: "s3", Host: b.GetBucket(), Path: "/" + path.Join(b.GetPrefix(), database, measurement)}).String(), nil
	default:
		return "", fmt.Errorf("no colocated table location for a %T backend", e.backend)
	}
}

// warehouseTableDirLoc is the directory the warehouse layout puts a table's
// metadata in, as iceberg-go names it from the catalog's namespace key.
func (e *Exporter) warehouseTableDirLoc(nsKey, measurement string) string {
	return strings.TrimSuffix(e.warehouse, "/") + "/" + nsKey + ".db/" + measurement + "/metadata"
}

// noRowMetaDirLoc picks the metadata directory to adopt from when this node
// has no catalog row for the table: the configured layout's first, the other
// layout's when only it holds a published version. With nothing published in
// either, the configured layout's (EnsureTable creates the table there).
func (e *Exporter) noRowMetaDirLoc(ctx context.Context, nsKey, database, measurement string) (string, error) {
	warehouseDir := e.warehouseTableDirLoc(nsKey, measurement)
	colocDir := ""
	if loc, err := e.colocatedTableLocation(database, measurement); err == nil {
		colocDir = loc + "/metadata"
	}
	order := []string{warehouseDir, colocDir}
	if e.colocatedLayout() {
		order = []string{colocDir, warehouseDir}
	}
	for _, dir := range order {
		if dir == "" {
			continue
		}
		published, err := e.hasPublishedVersion(ctx, dir)
		if err != nil {
			return "", err
		}
		if published {
			return dir, nil
		}
	}
	if order[0] == "" {
		return order[1], nil
	}
	return order[0], nil
}

// hasPublishedVersion reports whether a metadata directory holds a
// version-hint or a v<N>.metadata.json.
func (e *Exporter) hasPublishedVersion(ctx context.Context, dirLoc string) (bool, error) {
	key, ok := e.warehouseRelKey(dirLoc + "/version-hint.text")
	if !ok {
		return false, nil
	}
	dirKey := path.Dir(key)
	if n, err := e.readHint(ctx, dirKey); err != nil || n >= 0 {
		return n >= 0, err
	}
	n, err := e.newestPublished(ctx, dirKey)
	return n >= 0, err
}

// checkAdoptedLocation refuses to adopt metadata whose own location is not
// the directory it was found in, for a colocated table. A backup restored
// onto another storage root (another bucket, prefix or path) carries
// metadata naming the source's location, and iceberg-go would write the next
// commit there. Warehouse-layout tables are left as they were before the
// colocated layout existed.
func (e *Exporter) checkAdoptedLocation(database, measurement, dirLoc string, body []byte) error {
	want, err := e.colocatedTableLocation(database, measurement)
	if err != nil || !sameLocation(strings.TrimSuffix(dirLoc, "/metadata"), want) {
		return nil // not a colocated table directory
	}
	var meta struct {
		Location string `json:"location"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		return fmt.Errorf("published Iceberg metadata in %s: %w", dirLoc, err)
	}
	if !sameLocation(meta.Location, want) {
		return fmt.Errorf("the Iceberg metadata in %s names location %q, not this table's directory %q: it was "+
			"probably restored from another storage root; delete %s/metadata/ and the export recreates the table",
			dirLoc, meta.Location, want, database+"/"+measurement)
	}
	return nil
}

// databaseOfIdent inverts namespaceIdentifier: {<prefix>_<db>, table} is
// database <db>, {<prefix>_<spoke>, <db>, table} is "<spoke>/<db>". An
// identifier this exporter did not build returns "".
func databaseOfIdent(nsPrefix string, ident []string) string {
	if len(ident) < 2 {
		return ""
	}
	first, ok := strings.CutPrefix(ident[0], nsPrefix+"_")
	if !ok || first == "" {
		return ""
	}
	switch len(ident) {
	case 2:
		return first
	case 3:
		return first + "/" + ident[1]
	}
	return ""
}

// sameLocation compares two table locations in the spelling warehouseRelKey
// compares them in: percent escapes decoded, local symlinks resolved.
func sameLocation(a, b string) bool {
	norm := func(s string) string { return decodedURIPath(resolveFileURI(strings.TrimSuffix(s, "/"))) }
	return norm(a) == norm(b)
}

// IsColocatedMetadataKey reports whether a storage key has the shape of a
// colocated table's metadata: <...>/<measurement>/metadata/<file>, with at
// least a database and a measurement above it, and a file that is not Parquet
// (Arc writes only Parquet in its data tree).
func IsColocatedMetadataKey(key string) bool {
	parts := strings.Split(key, "/")
	if len(parts) < 4 || parts[len(parts)-2] != "metadata" {
		return false
	}
	base := parts[len(parts)-1]
	return base != "" && !strings.HasSuffix(base, ".parquet")
}

// OwnsMetadataKey reports whether a database DELETE must leave a key to
// DropDatabase (#1676): a colocated table's metadata. Deleting it with the
// data, before DropDatabase writes the drop markers, reopens #1611: another
// node's catalog row would point at metadata that is gone with no marker to
// say why. Implements api.IcebergCatalogDropper.
func (e *Exporter) OwnsMetadataKey(key string) bool { return IsColocatedMetadataKey(key) }

// colocatedMetadataByTable lists a database's colocated table metadata,
// keyed by the table's directory relative to the database: "<measurement>"
// for <database>/<measurement>/metadata/<file>, and "<db>/<measurement>" for
// a hub's spoke tables under a spoke namespace,
// <spoke>/<db>/<measurement>/metadata/<file>. Those are exactly the keys
// IsColocatedMetadataKey claims under the database, so everything the
// database DELETE leaves to DropDatabase is collected here.
func (e *Exporter) colocatedMetadataByTable(ctx context.Context, database string) (map[string][]string, error) {
	keys, err := e.backend.List(ctx, database+"/")
	if err != nil {
		return nil, fmt.Errorf("list colocated Iceberg metadata for %s: %w", database, err)
	}
	out := map[string][]string{}
	for _, k := range keys {
		k = filepath.ToSlash(k)
		rest, ok := strings.CutPrefix(k, database+"/")
		if !ok {
			continue
		}
		parts := strings.Split(rest, "/")
		if (len(parts) != 3 && len(parts) != 4) || parts[len(parts)-2] != "metadata" || !IsColocatedMetadataKey(k) {
			continue
		}
		dir := strings.Join(parts[:len(parts)-2], "/")
		out[dir] = append(out[dir], k)
	}
	return out, nil
}
