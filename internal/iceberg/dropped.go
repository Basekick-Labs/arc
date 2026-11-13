package iceberg

// Catalog rows whose metadata is gone (#1611).
//
// The SQLite catalog is node-local; the warehouse is not, on a shared bucket.
// A DROP of a database runs on one node (the primary writer) and removes the
// namespace's warehouse objects for every node, but only that node's catalog
// rows. Another node that once exported the table keeps a row pointing at
// deleted metadata. Its load then fails with fs.ErrNotExist, and the #637
// gate refuses to create over a row ("restore the warehouse, or delete the
// row") on every pass, even after the database is recreated and another node
// published the new table.
//
// recoverDeadRow runs when a load fails that way:
//
//   - Anything published for the table (the newer of version-hint.text and the
//     newest v<N>.metadata.json, or the newest v<N> when the hint names one that
//     is missing): the row is moved onto it whatever its version number
//     relative to the row's. The warehouse is the authority; a recreated table
//     starts again at v0, below the dead row. Safe in every topology, and it
//     also heals a row a backup restored without its file, where the backup
//     holds a hint newer than its v<N> files (#1613).
//   - Nothing published: the row is forgotten (and the table re-adopted or
//     created) only where forgetDroppedRows is set (a shared bucket with a
//     cluster coordinator) and the table carries a drop marker. Elsewhere, a
//     standalone node above all, the #637 error stands: no other node can drop
//     its rows, and its whole-instance backup holds the warehouse, so "restore
//     it" is the right answer.
//
// The drop marker is one object per dropped table under a reserved root
// directory (droppedMarkerRoot): reserved roots are not databases to any
// listing, and backups do not copy them, so a restore never brings one back.
// DropDatabase writes the markers before it deletes anything, so a drop that
// fails part way, re-run, writes them again; and only where forgetDroppedRows
// is set. A table's marker is removed by its next successful publish: from
// then on a lagging node follows the published version and needs no marker,
// and a later loss of that table (a wiped prefix) is the #637 error again.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"strconv"
	"strings"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/basekick-labs/arc/internal/storage"
)

// droppedMarkerRoot is the reserved root directory holding drop markers,
// <root>/<escaped namespace key>/<escaped table>.
const droppedMarkerRoot = "_iceberg_dropped"

// droppedMarkerKey is the storage key of a table's drop marker.
func droppedMarkerKey(nsKey, measurement string) string {
	return droppedMarkerRoot + "/" + url.PathEscape(nsKey) + "/" + url.PathEscape(measurement)
}

// clearDropMarker removes a table's drop marker after a successful publish,
// at most once per table per process.
func (e *Exporter) clearDropMarker(ctx context.Context, ident icetable.Identifier) {
	if !e.forgetDroppedRows || e.backend == nil {
		return
	}
	key := strings.Join(ident, "\x00")
	e.publishedMu.Lock()
	if e.markerCleared == nil {
		e.markerCleared = make(map[string]struct{})
	}
	_, done := e.markerCleared[key]
	e.markerCleared[key] = struct{}{}
	e.publishedMu.Unlock()
	if done {
		return
	}
	marker := droppedMarkerKey(catalogNamespaceKey(ident[:len(ident)-1]), ident[len(ident)-1])
	if err := e.backend.Delete(ctx, marker); err != nil && !storage.IsNotFound(err) {
		e.logger.Debug().Err(err).Str("key", marker).Msg("Iceberg: could not remove a drop marker after publishing; retried next process")
	}
}

// SetForgetDroppedRows lets a dead catalog row with nothing published be
// forgotten when the namespace carries DropDatabase's marker (a shared bucket
// with a cluster coordinator).
func (e *Exporter) SetForgetDroppedRows(on bool) {
	e.forgetDroppedRows = on
}

// loadTableRecovering is catalog.LoadTable, recovering once from a row whose
// metadata file is gone (see the file comment).
func (e *Exporter) loadTableRecovering(ctx context.Context, ident icetable.Identifier) (*icetable.Table, error) {
	tbl, err := e.catalog.LoadTable(ctx, ident)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return tbl, err
	}
	recovered, rerr := e.recoverDeadRow(ctx, ident)
	if rerr != nil {
		return nil, rerr
	}
	if !recovered {
		return nil, err
	}
	return e.catalog.LoadTable(ctx, ident)
}

// recoverDeadRow moves or forgets ident's catalog row, whose metadata file is
// gone. It reports whether it changed the row.
func (e *Exporter) recoverDeadRow(ctx context.Context, ident icetable.Identifier) (bool, error) {
	if e.backend == nil {
		return false, nil
	}
	nsKey, measurement := catalogNamespaceKey(ident[:len(ident)-1]), ident[len(ident)-1]
	rowLoc, rowExists, err := e.catalogRow(ctx, nsKey, measurement)
	if err != nil || !rowExists {
		return false, err
	}
	// The table's lineage changed under this node: its next publish must try
	// the drop marker again, even if it cleared (none) before the drop.
	e.publishedMu.Lock()
	delete(e.markerCleared, strings.Join(ident, "\x00"))
	e.publishedMu.Unlock()
	slash := strings.LastIndex(rowLoc, "/")
	if slash < 0 {
		return false, nil // not a location this exporter wrote
	}
	dirLoc := rowLoc[:slash]
	dirKey, ok := e.warehouseRelKey(dirLoc + "/version-hint.text")
	if !ok {
		return false, nil // a warehouse outside the storage root: nothing to read
	}
	dirKey = path.Dir(dirKey)

	// The newer of the hint and the newest v<N>: a writer that published
	// v<N> and failed to write the hint left the hint behind.
	hint, err := e.readHint(ctx, dirKey)
	if err != nil {
		return false, err
	}
	newest, err := e.newestPublished(ctx, dirKey)
	if err != nil {
		return false, err
	}
	target := max(hint, newest)
	var body []byte
	var vKey string
	if target >= 0 {
		vKey = path.Join(dirKey, "v"+strconv.Itoa(target)+".metadata.json")
		body, err = e.backend.Read(ctx, vKey)
		if err != nil && storage.IsNotFound(err) && target > newest && !e.dropInProgress(ctx, nsKey, measurement) {
			// The hint names a version that is not there: a backup copies
			// the hint as it is at copy time, which can be newer than the
			// v<N> files it listed (#1613). The newest present one, even
			// below the dead row (a recreated table starts again at v0).
			target, err = newest, nil
			if target >= 0 {
				vKey = path.Join(dirKey, "v"+strconv.Itoa(target)+".metadata.json")
				body, err = e.backend.Read(ctx, vKey)
			}
		}
		if err != nil {
			return false, fmt.Errorf("read published Iceberg metadata %s: %w", vKey, err)
		}
	}
	if target >= 0 {
		if !json.Valid(body) {
			return false, fmt.Errorf("published Iceberg metadata %s is not valid JSON", vKey)
		}
		newLoc := dirLoc + "/v" + strconv.Itoa(target) + ".metadata.json"
		if err := e.pointCatalogRow(ctx, nsKey, measurement, rowLoc, true, newLoc); err != nil {
			return false, err
		}
		e.markPublished(ident, newLoc)
		e.logger.Warn().Str("table", strings.Join(ident, ".")).Str("missing", rowLoc).Int("to_version", target).
			Msg("Iceberg: this node's catalog row pointed at metadata that is gone; moved it onto the version published in the warehouse (the table was dropped and recreated by another node, or this catalog was restored ahead of its warehouse files)")
		return true, nil
	}

	if !e.forgetDroppedRows {
		return false, nil
	}
	marker := droppedMarkerKey(nsKey, measurement)
	dropped, err := e.backend.Exists(ctx, marker)
	if err != nil {
		return false, fmt.Errorf("check Iceberg drop marker %s: %w", marker, err)
	}
	if !dropped {
		return false, nil
	}
	if err := e.forgetCatalogRow(ctx, nsKey, measurement, rowLoc); err != nil {
		return false, err
	}
	e.logger.Warn().Str("table", strings.Join(ident, ".")).Str("missing", rowLoc).
		Msg("Iceberg: forgot this node's catalog row for a table whose database another node dropped; the table is exported again as a new table")
	// With no row, a table published at the canonical directory since is
	// adopted before a load creates one.
	if _, err := e.adoptPublished(ctx, ident, nsKey, databaseOfIdent(e.nsPrefix, ident), measurement); err != nil {
		return true, err
	}
	return true, nil
}

// dropInProgress reports whether ident carries a drop marker, where markers
// are written (forgetDroppedRows). A database DROP deletes the v<N> files in
// lexical order and the hint last, so while it runs a hint can name a missing
// version and the newest v<N> left is arbitrary: the missing-hint fallbacks
// must not follow it. A marker that cannot be read counts as set: the pass
// fails as it did before the fallback existed and retries.
func (e *Exporter) dropInProgress(ctx context.Context, nsKey, measurement string) bool {
	if !e.forgetDroppedRows || e.backend == nil {
		return false
	}
	set, err := e.backend.Exists(ctx, droppedMarkerKey(nsKey, measurement))
	return err != nil || set
}

// forgetCatalogRow deletes the row only while it still points at loc.
func (e *Exporter) forgetCatalogRow(ctx context.Context, nsKey, measurement, loc string) error {
	res, err := e.db.ExecContext(ctx,
		`DELETE FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? AND metadata_location = ?`,
		e.catalogName, nsKey, measurement, loc)
	if err != nil {
		return fmt.Errorf("forget the Iceberg catalog row of a dropped table: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("Iceberg catalog row for %s.%s changed while forgetting it (%d rows deleted, want 1)", nsKey, measurement, n)
	}
	return nil
}
