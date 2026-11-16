package iceberg

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/basekick-labs/arc/internal/storage"
)

// A whole spoke dropped on an edge-sync hub (#1730).
//
// DELETE /databases/{spoke} on a hub drops every database the spoke synced.
// Each is the namespace {<prefix>_<spoke>, <db>} (namespaceIdentifier, #1129),
// not the spoke's own {<prefix>_<spoke>}, so DropDatabase's single-namespace
// drop missed them in the warehouse layout: the tables stayed in the catalog
// and their metadata in the warehouse, naming deleted data files.
//
// A sub-namespace can be stored three ways, and each is matched exactly:
//
//   - current: catalog identifier {<prefix>_<spoke>, <db>}; warehouse directory
//     catalogNamespaceKey(...) + ".db" (plain "<prefix>_<spoke>.<db>.db", or
//     the encoded key when the spoke ID has a dot);
//   - written by an Arc before #1129 and not migrated (the namespace migration
//     defaults to dry run): catalog key "<prefix>_<spoke>.<db>", which the
//     catalog parses into one component per dot, and the same plain warehouse
//     directory. For a dot-free spoke ID this is the current form; it differs
//     only for a dotted one;
//   - migrated: the catalog row moved to the current key, the warehouse
//     directory stayed at the plain name (the migration moves no files).
//
// Drop markers go under the current key only: every reader (recoverDeadRow,
// dropInProgress, clearDropMarker) builds it from tableIdent, and an
// unmigrated row is never loaded under it.
//
// With the warehouse at the storage root, the listing also returns the data
// directories of every spoke and database, and a spoke ID may be shaped like
// a warehouse directory name. Only Iceberg metadata keys,
// <dir>/<table>/metadata/<file> and never a Parquet file, are claimed or
// deleted, so a drop can never take another spoke's data.
//
// Matching is on the whole namespace, never a prefix of a component: spoke
// "edge.x" must not claim spoke "edge"'s database "x", whose plain key is the
// same string "<prefix>_edge.x". NamespaceDirOwnedBy deliberately over-claims
// that case (#750) and is not used here.

// spokeSub is one database of the spoke being dropped.
type spokeSub struct {
	namespaces    []icetable.Identifier // catalog namespaces holding it, as the catalog returns them
	catalogTables []icetable.Identifier // their tables
	dirs          map[string]bool       // warehouse namespace directories (storage keys, no trailing "/")
	tables        map[string]bool       // table names, catalog and warehouse, for drop markers
	markerKey     string                // the namespace key readers look markers up by
}

// spokeSubNamespaces finds the databases of spoke database (a hub's top-level
// directory) in this node's catalog and in the warehouse. The warehouse is
// listed too because on a shared bucket the node running the drop has often
// never exported the database (#1611). A database without spoke databases
// returns none.
func (e *Exporter) spokeSubNamespaces(ctx context.Context, database string) (map[string]*spokeSub, error) {
	prefix := e.nsPrefix + "_" + database
	subs := map[string]*spokeSub{}
	sub := func(db string) *spokeSub {
		s := subs[db]
		if s == nil {
			s = &spokeSub{dirs: map[string]bool{}, tables: map[string]bool{},
				markerKey: catalogNamespaceKey(icetable.Identifier{prefix, db})}
			subs[db] = s
		}
		return s
	}

	// This node's catalog.
	namespaces, err := e.catalog.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list Iceberg namespaces for drop: %w", err)
	}
	for _, ns := range namespaces {
		db, ok := spokeDatabaseOf(ns, prefix)
		if !ok {
			continue
		}
		s := sub(db)
		s.namespaces = append(s.namespaces, ns)
		for ident, err := range e.catalog.ListTables(ctx, ns) {
			if err != nil {
				if errors.Is(err, icecatalog.ErrNoSuchNamespace) {
					break
				}
				return nil, fmt.Errorf("list tables for drop: %w", err)
			}
			s.catalogTables = append(s.catalogTables, ident)
			s.tables[ident[len(ident)-1]] = true
		}
	}

	// The warehouse. ok=false: outside the storage root, which this exporter
	// cannot address; "" with ok: the warehouse is the storage root.
	if e.backend == nil {
		return subs, nil
	}
	rel, ok := e.warehouseRelKey(e.warehouse)
	lister, canList := e.backend.(storage.DirectoryLister)
	if !ok || !canList {
		return subs, nil
	}
	listAt := rel
	if listAt != "" {
		listAt += "/"
	}
	names, err := lister.ListDirectories(ctx, listAt)
	if err != nil {
		return nil, fmt.Errorf("list the Iceberg warehouse for drop: %w", err)
	}
	for _, name := range names {
		key, isNS := strings.CutSuffix(name, ".db")
		if !isNS {
			continue
		}
		db, ok := spokeDatabaseOfKey(key, prefix)
		if !ok {
			continue
		}
		dir := name
		if rel != "" {
			dir = rel + "/" + name
		}
		keys, err := e.backend.List(ctx, dir+"/")
		if err != nil {
			return nil, fmt.Errorf("list warehouse metadata: %w", err)
		}
		tables := map[string]bool{}
		for _, k := range keys {
			if table, ok := warehouseMetadataTable(dir, k); ok {
				tables[table] = true
			}
		}
		if len(tables) == 0 {
			continue // nothing of a table here: not claimed
		}
		s := sub(db)
		s.dirs[dir] = true
		for table := range tables {
			s.tables[table] = true
		}
	}
	return subs, nil
}

// warehouseMetadataTable reports the table of an Iceberg metadata key in a
// warehouse namespace directory: exactly <dir>/<table>/metadata/<file>, the
// file not a Parquet file. Anything else is not the exporter's to delete.
func warehouseMetadataTable(dir, key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, dir+"/")
	if !ok {
		return "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] != "metadata" || parts[2] == "" ||
		strings.EqualFold(path.Ext(parts[2]), ".parquet") {
		return "", false
	}
	return parts[0], true
}

// warehouseMetadataKeys keeps the keys of dir that warehouseMetadataTable
// accepts: what DropDatabase may delete from a warehouse namespace directory.
func warehouseMetadataKeys(dir string, keys []string) []string {
	out := keys[:0:0]
	for _, k := range keys {
		if _, ok := warehouseMetadataTable(dir, k); ok {
			out = append(out, k)
		}
	}
	return out
}

// spokeDatabaseOf reports the spoke database a catalog namespace holds, if it
// is one of prefix's: {prefix, db} (current or migrated), or the components of
// the unmigrated key prefix+"."+db. db is dot-free (namespaceIdentifier).
func spokeDatabaseOf(ns icetable.Identifier, prefix string) (string, bool) {
	if len(ns) == 2 && ns[0] == prefix && ns[1] != "" && !strings.Contains(ns[1], ".") {
		return ns[1], true
	}
	if len(ns) > 2 {
		return spokeDatabaseOfKey(strings.Join(ns, "."), prefix)
	}
	return "", false
}

// spokeDatabaseOfKey is spokeDatabaseOf for a namespace key (a warehouse
// directory name without ".db"): encoded {prefix, db}, or plain
// prefix+"."+db.
func spokeDatabaseOfKey(key, prefix string) (string, bool) {
	if ns, ok := decodeCatalogNamespace(key); ok {
		if len(ns) == 2 && ns[0] == prefix && ns[1] != "" && !strings.Contains(ns[1], ".") {
			return ns[1], true
		}
		return "", false
	}
	db, ok := strings.CutPrefix(key, prefix+".")
	if !ok || db == "" || strings.Contains(db, ".") {
		return "", false
	}
	return db, true
}
