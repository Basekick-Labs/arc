package iceberg

import (
	"context"
	"path"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// WarehouseLayout names the directories the Iceberg warehouse occupies inside the storage root,
// so that every walker that enumerates the root as databases can leave them out (#1626). Two
// layouts put the warehouse there:
//
//   - the default, iceberg.warehouse = the storage root: the namespace directories
//     "<prefix>_<db>.db" sit at the root, next to the databases;
//   - iceberg.warehouse = a subdirectory of the root (#534, #1616): "<root>/lv_wh" holds the
//     namespace directories, and "lv_wh" sits at the root looking like a database.
//
// A warehouse nested two or more levels deep ("<root>/a/b") leaves "a" a database (it may be a
// real one) and hides only "b" inside it. A warehouse outside the root, or in another bucket,
// occupies nothing here.
//
// A nil *WarehouseLayout answers false to everything: Iceberg export is off.
type WarehouseLayout struct {
	nsPrefix string
	// rootName is the warehouse directory directly under the storage root ("lv_wh"), or "".
	rootName string
	// nestedDB, nestedMeasurement are the first two segments of a warehouse nested at least
	// two levels under the root, or ""; nestedRel is its whole path relative to the root.
	nestedDB, nestedMeasurement, nestedRel string
}

// NewWarehouseLayout computes the layout from the storage root and the warehouse, both already
// resolved to one spelling (resolveFileURI + decodedURIPath, as warehouseRelKey compares them).
// root "" means no backend is known: only the namespace-directory rule applies.
func NewWarehouseLayout(root, warehouse, nsPrefix string) *WarehouseLayout {
	if nsPrefix == "" {
		nsPrefix = "arc"
	}
	l := &WarehouseLayout{nsPrefix: nsPrefix}
	// The root's trailing slash must go before the trim below; the warehouse's is
	// removed by path.Clean.
	root = strings.TrimSuffix(root, "/")
	if root == "" || warehouse == "" || !isUnderDir(warehouse, root) {
		return l
	}
	// At a path boundary (isUnderDir above), never a bare prefix trim: "/data/wh-other" must not
	// read as "/data/wh" plus "-other" (#534).
	rel := strings.TrimPrefix(strings.TrimPrefix(warehouse, root), "/")
	if rel == "" {
		return l // the warehouse is the root: only the namespace directories
	}
	rel = path.Clean(rel)
	if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return l
	}
	segs := strings.Split(rel, "/")
	if len(segs) == 1 {
		l.rootName = segs[0]
		return l
	}
	l.nestedDB, l.nestedMeasurement, l.nestedRel = segs[0], segs[1], rel
	return l
}

// IsWarehouseRootDir reports whether a top-level directory of the storage root belongs to the
// Iceberg warehouse rather than being a database: a namespace directory of this prefix
// (wherever the warehouse sits, as the export walk has always skipped them), or the
// subdirectory warehouse itself.
func (l *WarehouseLayout) IsWarehouseRootDir(name string) bool {
	if l == nil {
		return false
	}
	name = strings.Trim(name, "/")
	if name == "" {
		return false
	}
	if l.rootName != "" && name == l.rootName {
		return true
	}
	return isWarehouseNamespaceDir(name, l.nsPrefix)
}

// IsWarehouseMeasurement reports whether measurement m of database db is the directory a nested
// warehouse ("<root>/db/m/…") occupies.
func (l *WarehouseLayout) IsWarehouseMeasurement(db, m string) bool {
	if l == nil || l.nestedDB == "" {
		return false
	}
	return strings.Trim(db, "/") == l.nestedDB && strings.Trim(m, "/") == l.nestedMeasurement
}

// ContainsWarehouse reports whether database db holds a nested warehouse ("<root>/db/m/…"),
// and its path relative to the storage root. Deleting db would delete the warehouse with it,
// the metadata of every exported table included (#1626). db is a single path segment, so the
// exact compare is a boundary match ("a-other" is not "a").
func (l *WarehouseLayout) ContainsWarehouse(db string) (string, bool) {
	if l == nil || l.nestedDB == "" || strings.Trim(db, "/") != l.nestedDB {
		return "", false
	}
	return l.nestedRel, true
}

// RootName returns the subdirectory warehouse's name directly under the storage root, or "".
// It is the one warehouse name a database could be created or ingested under: a namespace
// directory's name has a dot, which no database name may.
func (l *WarehouseLayout) RootName() string {
	if l == nil {
		return ""
	}
	return l.rootName
}

// Nested returns the database and measurement a warehouse nested two or more levels deep sits
// under, or "", "".
func (l *WarehouseLayout) Nested() (db, measurement string) {
	if l == nil {
		return "", ""
	}
	return l.nestedDB, l.nestedMeasurement
}

// isWarehouseNamespaceDir reports whether a top-level directory is an Iceberg warehouse namespace
// directory ("<nsPrefix>_<db>.db") written by the exporter. Iceberg's SQL catalog names namespace
// dirs "<namespace>.db".
func isWarehouseNamespaceDir(name, nsPrefix string) bool {
	if strings.HasPrefix(name, nsPrefix+"_") && strings.HasSuffix(name, ".db") {
		return true
	}
	// iceberg-go v0.7.0 JSON-encodes a namespace whose component contains a dot, and an edge-sync
	// spoke ID may contain one (#1129), so that directory does not start with "<prefix>_" at all.
	// Decoding through catalogNamespaceKey's own inverse is deliberate: a second, local copy of
	// the encoding rule is how the exporter would come to disagree with the catalog about which
	// directories are its own, and a directory it fails to recognise here is walked back in as a
	// user database -- the harm the whole namespace scheme exists to prevent (#634).
	if !strings.HasSuffix(name, ".db") {
		return false
	}
	namespace, ok := decodeCatalogNamespace(strings.TrimSuffix(name, ".db"))
	return ok && namespaceBelongsToPrefix(namespace, nsPrefix)
}

// IsWarehouseNamespaceDir is isWarehouseNamespaceDir for callers outside this package, so they
// recognise the exporter's namespace directories -- the JSON-encoded spelling included -- by the
// exporter's own rule rather than a copy of it (#1202).
func IsWarehouseNamespaceDir(name, nsPrefix string) bool {
	return isWarehouseNamespaceDir(name, nsPrefix)
}

// NamespaceDirOwnedBy reports whether a warehouse namespace directory ("<namespace>.db", one path
// segment) holds tables of root, a storage-root segment: a plain database or an edge-sync spoke.
//
// A plain database's namespace is one component, "<nsPrefix>_<db>". A spoke's is two,
// "<nsPrefix>_<spoke>" and the dot-free inner database (namespaceIdentifier), stored either as the
// dot-joined key or, when a component contains a dot (#1129), as the JSON-encoded key; a legacy
// table the #1129 migration rekeyed keeps its dot-joined directory, because the migration moves
// nothing on disk. So root owns:
//
//   - "<nsPrefix>_<root>.db"                      its plain database namespace
//   - "<nsPrefix>_<root>.<inner>.db", inner dot-free  a spoke database, dot-joined
//   - an encoded key whose first component is "<nsPrefix>_<root>"
//
// The dot-free rule on inner is what keeps root "rocket" from claiming spoke "rocket.01"'s
// "arc_rocket.01.telemetry.db". What it cannot resolve is "arc_a.b.db", which is both root "a"'s
// spoke database "b" and, read literally, root "a.b"'s plain namespace (#750): both claim it. The
// caller this exists for reports what a scoped backup left out, where over-reporting is the safe
// direction.
func NamespaceDirOwnedBy(dir, nsPrefix, root string) bool {
	key, ok := strings.CutSuffix(dir, ".db")
	if !ok || root == "" {
		return false
	}
	first := nsPrefix + "_" + root
	if parts, ok := decodeCatalogNamespace(key); ok {
		return parts[0] == first
	}
	if key == first {
		return true
	}
	inner, ok := strings.CutPrefix(key, first+".")
	return ok && inner != "" && !strings.Contains(inner, ".")
}

// WarehouseLayout returns where this exporter's warehouse sits in the storage root. It resolves
// the two bases itself rather than through resolveBases, so calling it at startup does not
// freeze warehouseRelKey's lazily cached resolution earlier than before.
func (e *Exporter) WarehouseLayout() *WarehouseLayout {
	return LayoutFor(e.backend, e.warehouse, e.nsPrefix)
}

// LayoutFor is the layout an exporter on backend with this warehouse URI (as main.go passes it
// to NewExporter) reports, computed without one: main.go needs the warehouse's name before the
// exporter exists, ahead of the first writer (MQTT subscriptions start early).
func LayoutFor(backend storage.Backend, warehouse, nsPrefix string) *WarehouseLayout {
	root := ""
	if backend != nil {
		root = decodedURIPath(resolveFileURI(DefaultWarehouse(backend)))
	}
	return NewWarehouseLayout(root, decodedURIPath(resolveFileURI(strings.TrimSuffix(warehouse, "/"))), nsPrefix)
}

// WarnWarehouseShadowing logs, once at startup, a warehouse placed where a database could be: a
// subdirectory warehouse whose directory also holds something other than namespace directories
// (a database of that name written before the warehouse was pointed there, now hidden with it),
// and a nested warehouse, which hides one measurement name of a database. Best-effort: a failed
// listing is logged at Debug and never fails startup.
func WarnWarehouseShadowing(ctx context.Context, backend storage.Backend, l *WarehouseLayout, logger zerolog.Logger) {
	if l == nil {
		return
	}
	if db, m := l.Nested(); db != "" {
		logger.Warn().Str("database", db).Str("measurement", m).
			Msg("The Iceberg warehouse is nested inside the storage root, under a database and measurement: writes to that measurement of that database are not refused and land inside the warehouse directory, where they are hidden from the listings and not exported; do not write there. The database itself stays listed, and deleting it is refused while it holds the warehouse")
	}
	name := l.RootName()
	if name == "" || backend == nil {
		return
	}
	dl, ok := backend.(dirLister)
	if !ok {
		return
	}
	dirs, err := dl.ListDirectories(ctx, name+"/")
	if err != nil {
		logger.Debug().Err(err).Str("warehouse_dir", name).Msg("Could not list the Iceberg warehouse directory to check it for database data")
		return
	}
	for _, d := range dirs {
		d = strings.Trim(d, "/")
		if d == "" || isWarehouseNamespaceDir(d, l.nsPrefix) || storage.IsReservedRootDir(d) {
			continue
		}
		logger.Warn().Str("warehouse_dir", name).Str("entry", d).
			Msg("The Iceberg warehouse directory also holds non-Iceberg data, likely a database of the same name written before iceberg.warehouse pointed there; it is no longer listed, and new writes to that name are refused. Move that data out, or point iceberg.warehouse elsewhere")
		return
	}
}
