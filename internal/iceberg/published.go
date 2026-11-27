package iceberg

// The published state of the exported tables, read for Arc's Iceberg REST
// catalog (#1690).
//
// What the catalog serves is what every directory reader already reads: the
// v<N>.metadata.json that version-hint.text names in the table's metadata
// directory. That is the export's commit point (writeVersionHint writes the
// fenced copy first and the hint last), it is the same file on every node that
// sees the warehouse, and it is what adoption already treats as authoritative
// (#1609). This node's SQLite catalog row is NOT consulted: on a shared bucket
// only the exporting node's rows are current, and a catalog answer that
// depended on which node a load balancer picked would make tables appear and
// disappear between requests.
//
// Nothing here writes. The exporter's own lookups (TableExists, adoptPublished,
// EnsureTable) move catalog rows, so the catalog must never call them.
//
// Only a version version-hint.text names is served, never a newer v<N>
// without one: that is what directory readers see, and the hint is gone first
// or last in every DROP (a colocated DROP deletes it first), so "no hint" is
// either a table not yet published or one being dropped.
//
// Every read failure is returned as an error, never as "not there": an
// external catalog that lists a table on one poll and not the next can drop it
// on its side (a Snowflake catalog-linked database), so absence is reported
// only when it is confirmed.
//
// A table being dropped is not always detectable. A database DELETE removes
// the data files before the table metadata, and the metadata in between still
// names them: the table is served until its hint is deleted (a colocated DROP
// deletes it first, the warehouse layout last), and reads of it fail
// meanwhile, as they do for directory readers. Drop markers are deliberately
// NOT consulted: the exporter clears a table's marker at most once per
// process (clearDropMarker), and the DROP runs on another node, so after a
// drop and recreate a marker can outlive the new table's publish. Refusing
// on it would hide a live table indefinitely.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// publishedScanParallel bounds the concurrent storage reads of one table
// listing. Each candidate costs one small read (two with no hint), so a
// database of hundreds of measurements on S3 lists in a few round trips.
const publishedScanParallel = 16

// PublishedTable is the current published version of one table.
type PublishedTable struct {
	// MetadataLocation is the full URI of the v<N>.metadata.json served.
	MetadataLocation string
	// Metadata is that file's bytes, checked to be valid JSON.
	Metadata []byte
}

// NamespacePrefix is the prefix of every namespace this exporter writes
// ("<prefix>_<database>").
func (e *Exporter) NamespacePrefix() string { return e.nsPrefix }

// PublishedAddressable reports whether the storage backend can read the
// warehouse, which is what the catalog reads from. A local iceberg.warehouse
// outside the storage root is written by iceberg-go but never published
// (writeVersionHint warns once and skips), so a catalog over it would be
// permanently empty.
func (e *Exporter) PublishedAddressable() bool {
	_, ok := e.warehouseRelKey(e.warehouse + "/" + e.nsPrefix + "_probe.db/t/metadata/version-hint.text")
	return ok
}

// SafeCatalogName reports whether a database or measurement name can name a
// published table in the catalog: one path segment that cannot climb out of
// its directory. A '%' is refused so a decoded URL segment cannot carry a
// second escape. It is deliberately looser than the ingest name rules: the
// export walk exports whatever directory names storage holds, and a client
// escapes the rest (a space, '#', '?') in the URL, which the catalog decodes.
func SafeCatalogName(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 255 {
		return false
	}
	if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
		return false // hidden and reserved directories (_schema, _iceberg_dropped, ...)
	}
	if strings.ContainsAny(name, "/\\%") {
		return false
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return storage.ValidateKey(name) == nil
}

// publishedDir is one candidate metadata directory of a table.
type publishedDir struct {
	loc       string // URI, as table metadata names it
	key       string // storage key relative to the storage root
	colocated bool
}

// servedLayout is the warehouse layout, resolved once.
func (e *Exporter) servedLayout() *WarehouseLayout {
	e.servedLayoutOnce.Do(func() { e.servedLayoutVal = e.WarehouseLayout() })
	return e.servedLayoutVal
}

// servedDatabase reports whether a name can be a database of the catalog: a
// safe segment that is not one of the warehouse's own directories. Without
// the second half a warehouse namespace directory ("arc_mydb.db") would be
// addressable as a database whose "colocated" tables are the warehouse
// tables, checked against the permissions of the wrong name.
func (e *Exporter) servedDatabase(database string) bool {
	return SafeCatalogName(database) && !e.servedLayout().IsWarehouseRootDir(database)
}

// servedTable is servedDatabase for a table: neither name unsafe, and the
// measurement not the directory a nested warehouse occupies.
func (e *Exporter) servedTable(database, measurement string) bool {
	return e.servedDatabase(database) && SafeCatalogName(measurement) &&
		!e.servedLayout().IsWarehouseMeasurement(database, measurement)
}

// publishedDirs returns the two standard metadata directories of a table, the
// configured layout's first: the same order noRowMetaDirLoc adopts in, so a
// table created under the other layout is still found. A directory the
// backend cannot address is left out.
func (e *Exporter) publishedDirs(database, measurement string) (nsKey string, dirs []publishedDir, err error) {
	if !e.servedTable(database, measurement) {
		return "", nil, nil
	}
	ns, err := namespaceIdentifier(e.nsPrefix, database)
	if err != nil {
		return "", nil, nil // not a database this catalog serves
	}
	nsKey = catalogNamespaceKey(ns)
	add := func(loc string, colocated bool) {
		key, ok := e.warehouseRelKey(loc + "/version-hint.text")
		if !ok {
			return
		}
		dirs = append(dirs, publishedDir{loc: loc, key: path.Dir(key), colocated: colocated})
	}
	warehouseLoc := e.warehouseTableDirLoc(nsKey, measurement)
	colocLoc := ""
	if loc, err := e.colocatedTableLocation(database, measurement); err == nil {
		colocLoc = loc + "/metadata"
	}
	if e.colocatedLayout() {
		if colocLoc != "" {
			add(colocLoc, true)
		}
		add(warehouseLoc, false)
	} else {
		add(warehouseLoc, false)
		if colocLoc != "" {
			add(colocLoc, true)
		}
	}
	return nsKey, dirs, nil
}

// LoadPublished returns the current published version of a table: the
// v<N>.metadata.json version-hint.text names. found is false only when
// neither standard directory has a hint. A state that cannot be served safely
// (a hint naming a missing version, a current snapshot whose manifest list is
// gone, metadata naming another location) is an error, never "not found".
func (e *Exporter) LoadPublished(ctx context.Context, database, measurement string) (PublishedTable, bool, error) {
	nsKey, dirs, err := e.publishedDirs(database, measurement)
	if err != nil {
		return PublishedTable{}, false, err
	}
	for _, d := range dirs {
		n, err := e.readHint(ctx, d.key)
		if err != nil {
			return PublishedTable{}, false, err
		}
		if n < 0 {
			continue
		}
		name := "v" + strconv.Itoa(n) + ".metadata.json"
		body, err := e.backend.Read(ctx, path.Join(d.key, name))
		if err != nil {
			if storage.IsNotFound(err) {
				return PublishedTable{}, false, fmt.Errorf("Iceberg table %s.%s: version-hint.text names v%d, which is missing "+
					"(a database DROP in progress, or a damaged table)", nsKey, measurement, n)
			}
			return PublishedTable{}, false, fmt.Errorf("read published Iceberg metadata %s: %w", path.Join(d.key, name), err)
		}
		if err := e.checkServable(ctx, database, measurement, d, body); err != nil {
			return PublishedTable{}, false, fmt.Errorf("Iceberg table %s.%s v%d: %w", nsKey, measurement, n, err)
		}
		return PublishedTable{MetadataLocation: d.loc + "/" + name, Metadata: body}, true, nil
	}
	return PublishedTable{}, false, nil
}

// checkServable refuses metadata a reader could not use: not JSON, a
// colocated table's metadata naming another storage root (a backup restored
// elsewhere, checkAdoptedLocation), or a current snapshot whose manifest list
// is gone (a DROP deletes manifests before the v<N> files and the hint).
func (e *Exporter) checkServable(ctx context.Context, database, measurement string, d publishedDir, body []byte) error {
	var meta struct {
		CurrentSnapshotID *int64 `json:"current-snapshot-id"`
		Snapshots         []struct {
			SnapshotID   int64  `json:"snapshot-id"`
			ManifestList string `json:"manifest-list"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		return fmt.Errorf("published metadata is not valid JSON: %w", err)
	}
	if d.colocated {
		if err := e.checkAdoptedLocation(database, measurement, d.loc, body); err != nil {
			return err
		}
	}
	if meta.CurrentSnapshotID == nil || *meta.CurrentSnapshotID == -1 {
		return nil // a table with no snapshot yet: nothing to read
	}
	for _, s := range meta.Snapshots {
		if s.SnapshotID != *meta.CurrentSnapshotID {
			continue
		}
		key, ok := e.warehouseRelKey(s.ManifestList)
		if !ok {
			return fmt.Errorf("the current snapshot's manifest list %s is outside the warehouse", s.ManifestList)
		}
		size, err := e.backend.StatFile(ctx, key)
		if err != nil {
			return fmt.Errorf("check the current snapshot's manifest list: %w", err)
		}
		if size < 0 {
			return fmt.Errorf("the current snapshot's manifest list %s is missing (a database DROP in progress, or a damaged table)", key)
		}
		return nil
	}
	return errors.New("the current snapshot is not in the metadata's snapshot list")
}

// listDirs lists the immediate subdirectories under prefix. A missing store is
// an error here: the catalog does not read it as "no tables" (see the package
// comment).
func (e *Exporter) listDirs(ctx context.Context, prefix string) ([]string, error) {
	lister, ok := e.backend.(storage.DirectoryLister)
	if !ok {
		return nil, fmt.Errorf("the %s storage backend cannot list directories", e.backend.Type())
	}
	dirs, err := lister.ListDirectories(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", prefix, err)
	}
	return dirs, nil
}

// warehouseRelDir is the storage key of the warehouse root ("" when it is the
// storage root).
func (e *Exporter) warehouseRelDir() (string, bool) {
	key, ok := e.warehouseRelKey(e.warehouse + "/x")
	if !ok {
		return "", false
	}
	if dir := path.Dir(key); dir != "." {
		return dir, true
	}
	return "", true
}

// PublishedDatabases returns the databases that may hold published tables:
// the storage root's databases (the filters of the database listing) and the
// databases named by warehouse namespace directories (a database whose hot
// data is gone still has its table). Whether each holds a published table is
// PublishedTables' question. Edge-sync spoke namespaces (two components) are
// not served.
func (e *Exporter) PublishedDatabases(ctx context.Context) ([]string, error) {
	seen := map[string]struct{}{}
	root, err := e.listDirs(ctx, "")
	if err != nil {
		return nil, err
	}
	for _, d := range root {
		if !e.servedDatabase(d) {
			continue
		}
		seen[d] = struct{}{}
	}
	whDir, ok := e.warehouseRelDir()
	if ok {
		nsDirs := root
		if whDir != "" {
			if nsDirs, err = e.listDirs(ctx, whDir+"/"); err != nil {
				return nil, err
			}
		}
		for _, d := range nsDirs {
			if db, ok := e.databaseOfNamespaceDir(d); ok {
				seen[db] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for db := range seen {
		out = append(out, db)
	}
	sort.Strings(out)
	return out, nil
}

// databaseOfNamespaceDir maps a warehouse namespace directory back to its
// single-component database. A dotted plain directory may be a spoke's
// ("arc_<spoke>.<db>.db", #750); it is returned as the dotted name, and its
// tables resolve under the encoded key, where a spoke's never are, so it
// lists nothing.
func (e *Exporter) databaseOfNamespaceDir(dir string) (string, bool) {
	if !isWarehouseNamespaceDir(dir, e.nsPrefix) {
		return "", false
	}
	key := strings.TrimSuffix(dir, ".db")
	if parts, ok := decodeCatalogNamespace(key); ok {
		if len(parts) != 1 {
			return "", false
		}
		key = parts[0]
	}
	db, ok := strings.CutPrefix(key, e.nsPrefix+"_")
	if !ok || !e.servedDatabase(db) {
		return "", false
	}
	return db, true
}

// PublishedTables returns the tables of a database that publish a version,
// sorted. limit > 0 stops once that many are found (a namespace listing needs
// only one). Every read error is returned.
func (e *Exporter) PublishedTables(ctx context.Context, database string, limit int) ([]string, error) {
	ns, err := namespaceIdentifier(e.nsPrefix, database)
	if !e.servedDatabase(database) || err != nil {
		return nil, nil
	}
	nsKey := catalogNamespaceKey(ns)
	candidates := map[string]struct{}{}
	hot, err := e.listDirs(ctx, database+"/")
	if err != nil {
		return nil, err
	}
	for _, m := range hot {
		if e.servedTable(database, m) {
			candidates[m] = struct{}{}
		}
	}
	if whDir, ok := e.warehouseRelDir(); ok {
		nsDir := path.Join(whDir, nsKey+".db") + "/"
		tables, err := e.listDirs(ctx, nsDir)
		if err != nil {
			return nil, err
		}
		for _, m := range tables {
			if e.servedTable(database, m) {
				candidates[m] = struct{}{}
			}
		}
	}
	names := make([]string, 0, len(candidates))
	for m := range candidates {
		names = append(names, m)
	}
	sort.Strings(names)

	var out []string
	for start := 0; start < len(names); start += publishedScanParallel {
		end := min(start+publishedScanParallel, len(names))
		batch := names[start:end]
		published := make([]bool, len(batch))
		errs := make([]error, len(batch))
		done := make(chan struct{}, len(batch))
		for i, m := range batch {
			go func() {
				defer func() { done <- struct{}{} }()
				published[i], errs[i] = e.isPublished(ctx, database, m)
			}()
		}
		for range batch {
			<-done
		}
		for i, m := range batch {
			if errs[i] != nil {
				return nil, errs[i]
			}
			if published[i] {
				out = append(out, m)
			}
		}
		if limit > 0 && len(out) >= limit {
			return out[:limit], nil
		}
	}
	return out, nil
}

// isPublished reports whether either standard directory of a table has a
// version-hint.text. Existence only: whether that version can be served is
// LoadPublished's question, so one damaged table (an unreadable hint) is a
// 503 on its own load and does not fail every listing.
func (e *Exporter) isPublished(ctx context.Context, database, measurement string) (bool, error) {
	_, dirs, err := e.publishedDirs(database, measurement)
	if err != nil {
		return false, err
	}
	for _, d := range dirs {
		ok, err := e.backend.Exists(ctx, path.Join(d.key, "version-hint.text"))
		if err != nil {
			return false, fmt.Errorf("check the Iceberg version-hint of %s/%s: %w", database, measurement, err)
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}
