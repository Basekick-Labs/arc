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
//   - Anything published for the table (version-hint.text, else the newest
//     v<N>.metadata.json): the row is moved onto it whatever its version number
//     relative to the row's. The warehouse is the authority; a recreated table
//     starts again at v0, below the dead row. Safe in every topology, and it
//     also heals a row a backup restored without its file (#1613).
//   - Nothing published: the row is forgotten (and the table re-adopted or
//     created) only where forgetDroppedRows is set (a shared bucket with a
//     cluster coordinator) and DropDatabase left its marker in the namespace
//     directory. Elsewhere, a standalone node above all, the #637 error stands:
//     no other node can drop its rows, and its whole-instance backup holds the
//     warehouse, so "restore it" is the right answer.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"

	icetable "github.com/apache/iceberg-go/table"
)

// droppedMarker is written by DropDatabase into the namespace directory once
// it has deleted the namespace's warehouse objects. The data walk skips the
// namespace directory and backups copy only metadata/ directories, so a
// restore never brings one back.
const droppedMarker = "_arc_dropped"

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
	dirLoc := rowLoc[:strings.LastIndex(rowLoc, "/")]
	dirKey, ok := e.warehouseRelKey(dirLoc + "/version-hint.text")
	if !ok {
		return false, nil // a warehouse outside the storage root: nothing to read
	}
	dirKey = path.Dir(dirKey)

	target, err := e.readHint(ctx, dirKey)
	if err != nil {
		return false, err
	}
	if target < 0 {
		if target, err = e.newestPublished(ctx, dirKey); err != nil {
			return false, err
		}
	}
	if target >= 0 {
		vKey := path.Join(dirKey, "v"+strconv.Itoa(target)+".metadata.json")
		body, err := e.backend.Read(ctx, vKey)
		if err != nil {
			return false, fmt.Errorf("read published Iceberg metadata %s: %w", vKey, err)
		}
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
	// The namespace directory: <ns>.db/<measurement>/metadata.
	marker := path.Join(path.Dir(path.Dir(dirKey)), droppedMarker)
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
	// A migrated (#1129) row lived in a legacy directory: look for a table
	// published at the canonical one before a load creates one.
	if _, err := e.adoptPublished(ctx, ident, nsKey, measurement); err != nil {
		return true, err
	}
	return true, nil
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
