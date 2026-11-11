package iceberg

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/basekick-labs/arc/internal/storage"
)

// The warehouse, not this node's catalog row, is the authority on a table's current version
// (#939 A2).
//
// The SQLite catalog is node-local. On a shared bucket (with or without shared_storage_mode,
// #1609) the tables live in the one bucket every node sees, and the export moves with the
// compactor lease, so a node can come back to a table with a catalog row that is versions
// behind what another node published since — or with no row at all. Committing from that row forks the table's history: iceberg-go writes the next
// version from the stale metadata, Arc's version-hint goes backwards, and the orphan sweep then
// deletes the other lineage's manifests as unreachable.
//
// Two mechanisms close that:
//
//   - Fence: Arc publishes each version's v<N>.metadata.json copy with a create-only-if-absent
//     write (storage.ConditionalWriter). Only one writer can ever publish version N; a writer
//     that finds a different v<N> already there stops the pass before pruning or sweeping.
//   - Adoption: before loading a table, the exporter reads the published version-hint and moves
//     its catalog row onto the newest published version when the row is behind it, or is the
//     same version number with different content (the losing side of a fence). Done every pass
//     where the warehouse is authoritative (SetWarehouseAuthoritative, set on every node since
//     #1613), including the scheduler's first loads after a restart; otherwise only when the
//     row is missing.

// SetWarehouseAuthoritative makes every pass check the published version before loading a
// table. Arc sets it on every node: a shared bucket's published version is current (#1609),
// and any catalog can be restored from a backup behind its warehouse, S3 or local (#1613),
// where committing from the stale row would roll the version-hint back. Without it the check
// runs only for a table with no catalog row.
func (e *Exporter) SetWarehouseAuthoritative(on bool) {
	e.warehouseAuthoritative = on
}

// errFenced reports that another writer published this version of the table first.
var errFenced = errors.New("another writer published this version of the Iceberg table first")

// publishMetadataCopy writes body as vKey with the fence when the backend supports it. A copy
// already there with the same bytes is this writer's own earlier publish (a retried pass).
func (e *Exporter) publishMetadataCopy(ctx context.Context, vKey string, body []byte) error {
	cw, ok := e.backend.(storage.ConditionalWriter)
	if !ok {
		return e.backend.Write(ctx, vKey, body)
	}
	created, err := cw.WriteIfAbsent(ctx, vKey, body)
	if err != nil || created {
		return err
	}
	existing, err := e.backend.Read(ctx, vKey)
	if err != nil {
		return fmt.Errorf("read the existing %s: %w", vKey, err)
	}
	if !bytes.Equal(existing, body) {
		return fmt.Errorf("%w: %s holds different metadata", errFenced, vKey)
	}
	return nil
}

// adoptPublished moves the table's catalog row onto the newest version published in the
// warehouse when the row is missing, behind, or on the losing side of a fence. It returns
// whether it moved the row. A table the warehouse holds no published version of is left alone
// (EnsureTable creates it). Read errors fail the pass: guessing would risk the fork this exists
// to prevent.
func (e *Exporter) adoptPublished(ctx context.Context, ident icetable.Identifier, nsKey, measurement string) (bool, error) {
	if e.backend == nil {
		return false, nil
	}
	rowLoc, rowExists, err := e.catalogRow(ctx, nsKey, measurement)
	if err != nil {
		return false, err
	}
	if rowExists && !e.warehouseAuthoritative {
		return false, nil
	}
	dirLoc := strings.TrimSuffix(e.warehouse, "/") + "/" + nsKey + ".db/" + measurement + "/metadata"
	if rowExists {
		// Not path.Dir: it cleans "file:///x" to "file:/x" and "s3://b/x" to "s3:/b/x".
		slash := strings.LastIndex(rowLoc, "/")
		if slash < 0 {
			return false, nil // not a location this exporter wrote
		}
		dirLoc = rowLoc[:slash]
	}
	dirKey, ok := e.warehouseRelKey(dirLoc + "/version-hint.text")
	if !ok {
		return false, nil // warehouse outside the storage root: nothing published to read
	}
	dirKey = path.Dir(dirKey)

	hintN, err := e.readHint(ctx, dirKey)
	if err != nil {
		return false, err
	}
	rowN := -1
	if rowExists {
		if v, _, ok := e.parseVersionAndMetaDir(rowLoc); ok {
			rowN, _ = strconv.Atoi(v)
		}
	}

	// decide picks the version to adopt for a published hint, or -1.
	decide := func(hintN int) (int, error) {
		switch {
		case !rowExists:
			if hintN >= 0 {
				return hintN, nil
			}
			return e.newestPublished(ctx, dirKey)
		case hintN > rowN:
			return hintN, nil
		case e.isPublishedHere(ident, rowLoc):
			// This node published the row's version itself: nothing newer, nothing to compare.
			return -1, nil
		}
		// Same or older hint: is the row's own version published, and is it this row's?
		same, exists, err := e.publishedMatchesRow(ctx, dirKey, rowN, rowLoc)
		if err != nil {
			return -1, err
		}
		switch {
		case exists && same:
			e.markPublished(ident, rowLoc)
		case exists:
			return max(hintN, rowN), nil // the fence's losing side: take the published lineage
		}
		return -1, nil
	}
	target, err := decide(hintN)
	if err != nil {
		return false, err
	}
	if target < 0 {
		return false, nil
	}

	vKey := path.Join(dirKey, "v"+strconv.Itoa(target)+".metadata.json")
	body, err := e.backend.Read(ctx, vKey)
	if err != nil && storage.IsNotFound(err) && rowExists && target == hintN {
		// The hint names a version that is not there: a backup copies the
		// hint as it is at copy time, which can be newer than the v<N> files
		// it listed (#1613). Decide again, once, from the newest v<N>
		// present; one below the row adopts nothing. Not without a row:
		// DropDatabase deletes v<N> files before the hint, and a pass in that
		// window would insert a row for the dropped table.
		newest, nerr := e.newestPublished(ctx, dirKey)
		if nerr != nil {
			return false, nerr
		}
		if newest < hintN {
			if target, err = decide(newest); err != nil {
				return false, err
			}
			if target < 0 {
				return false, nil
			}
			vKey = path.Join(dirKey, "v"+strconv.Itoa(target)+".metadata.json")
			body, err = e.backend.Read(ctx, vKey)
		}
	}
	if err != nil {
		return false, fmt.Errorf("read published Iceberg metadata %s: %w", vKey, err)
	}
	if !json.Valid(body) {
		return false, fmt.Errorf("published Iceberg metadata %s is not valid JSON", vKey)
	}
	newLoc := dirLoc + "/v" + strconv.Itoa(target) + ".metadata.json"
	if err := e.pointCatalogRow(ctx, nsKey, measurement, rowLoc, rowExists, newLoc); err != nil {
		return false, err
	}
	e.markPublished(ident, newLoc)
	e.logger.Warn().Str("table", strings.Join(ident, ".")).Int("from_version", rowN).Int("to_version", target).
		Bool("had_row", rowExists).
		Msg("Iceberg: adopted the table version published in the warehouse (another node advanced the table, or this node's catalog row was missing)")
	return true, nil
}

// readHint returns the version version-hint.text names, or -1 when there is none.
func (e *Exporter) readHint(ctx context.Context, dirKey string) (int, error) {
	data, err := e.backend.Read(ctx, path.Join(dirKey, "version-hint.text"))
	if err != nil {
		if storage.IsNotFound(err) {
			return -1, nil
		}
		return -1, fmt.Errorf("read Iceberg version-hint: %w", err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || n < 0 {
		return -1, fmt.Errorf("Iceberg version-hint %q in %s is not a version number", data, dirKey)
	}
	return n, nil
}

// newestPublished returns the highest N with a v<N>.metadata.json in dirKey, or -1. It covers a
// writer that published v<N> and stopped before writing the hint.
func (e *Exporter) newestPublished(ctx context.Context, dirKey string) (int, error) {
	keys, err := e.backend.List(ctx, dirKey+"/")
	if err != nil {
		return -1, fmt.Errorf("list Iceberg metadata in %s: %w", dirKey, err)
	}
	newest := -1
	for _, k := range keys {
		base := path.Base(k)
		if !strings.HasPrefix(base, "v") || !strings.HasSuffix(base, ".metadata.json") {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(base, "v"), ".metadata.json")); err == nil && n > newest {
			newest = n
		}
	}
	return newest, nil
}

// publishedMatchesRow reports whether v<rowN> exists in the warehouse and holds exactly the
// metadata the catalog row points at.
func (e *Exporter) publishedMatchesRow(ctx context.Context, dirKey string, rowN int, rowLoc string) (same, exists bool, err error) {
	if rowN < 0 {
		return false, false, nil
	}
	published, err := e.backend.Read(ctx, path.Join(dirKey, "v"+strconv.Itoa(rowN)+".metadata.json"))
	if err != nil {
		if storage.IsNotFound(err) {
			return false, false, nil // not published yet: this pass's hint write will
		}
		return false, false, fmt.Errorf("read published Iceberg metadata v%d: %w", rowN, err)
	}
	rowKey, ok := e.warehouseRelKey(rowLoc)
	if !ok {
		return false, true, nil
	}
	own, err := e.backend.Read(ctx, rowKey)
	if err != nil {
		if storage.IsNotFound(err) {
			// The row's own file is gone (#1611: the table was dropped and recreated by
			// another node): it is not the published version, so the caller takes that.
			return false, true, nil
		}
		return false, true, fmt.Errorf("read the catalog's Iceberg metadata %s: %w", rowKey, err)
	}
	return bytes.Equal(own, published), true, nil
}

// catalogRow returns the table's metadata location from this node's catalog.
func (e *Exporter) catalogRow(ctx context.Context, nsKey, measurement string) (string, bool, error) {
	var loc sql.NullString
	err := e.db.QueryRowContext(ctx,
		`SELECT metadata_location FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		e.catalogName, nsKey, measurement).Scan(&loc)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read Iceberg catalog row: %w", err)
	}
	return loc.String, true, nil
}

// pointCatalogRow moves (or inserts) the table's catalog row onto newLoc. An existing row is
// compare-and-set on the location read before, so a concurrent change is refused rather than
// overwritten; the catalog has no RegisterTable (iceberg-go v0.7.0 SQL), hence the raw SQL, in
// the style of the namespace migration (#1129).
func (e *Exporter) pointCatalogRow(ctx context.Context, nsKey, measurement, oldLoc string, exists bool, newLoc string) error {
	if exists {
		res, err := e.db.ExecContext(ctx,
			`UPDATE iceberg_tables SET metadata_location = ?, previous_metadata_location = ?
			 WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? AND metadata_location = ?`,
			newLoc, oldLoc, e.catalogName, nsKey, measurement, oldLoc)
		if err != nil {
			return fmt.Errorf("move Iceberg catalog row to the published version: %w", err)
		}
		if n, err := res.RowsAffected(); err != nil || n != 1 {
			return fmt.Errorf("Iceberg catalog row for %s.%s changed while adopting the published version (%d rows updated, want 1)", nsKey, measurement, n)
		}
		return nil
	}
	_, err := e.db.ExecContext(ctx,
		`INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, iceberg_type, metadata_location, previous_metadata_location)
		 VALUES (?, ?, ?, 'TABLE', ?, NULL)`,
		e.catalogName, nsKey, measurement, newLoc)
	if err != nil && strings.Contains(err.Error(), "iceberg_type") {
		// A catalog created with iceberg-go's v0 schema has no iceberg_type column.
		_, err = e.db.ExecContext(ctx,
			`INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)
			 VALUES (?, ?, ?, ?, NULL)`,
			e.catalogName, nsKey, measurement, newLoc)
	}
	if err != nil {
		return fmt.Errorf("register the published Iceberg table in the catalog: %w", err)
	}
	return nil
}

func (e *Exporter) isPublishedHere(ident icetable.Identifier, loc string) bool {
	e.publishedMu.Lock()
	defer e.publishedMu.Unlock()
	return e.published[strings.Join(ident, "\x00")] == loc
}

func (e *Exporter) markPublished(ident icetable.Identifier, loc string) {
	e.publishedMu.Lock()
	defer e.publishedMu.Unlock()
	if e.published == nil {
		e.published = make(map[string]string)
	}
	e.published[strings.Join(ident, "\x00")] = loc
}
