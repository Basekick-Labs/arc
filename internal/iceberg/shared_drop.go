package iceberg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/basekick-labs/arc/internal/storage"
)

// A database dropped on one node of a shared bucket while another node
// exports it (#1792).
//
// The node that handles a database DELETE (the primary writer) deletes the
// warehouse's objects for every node; the node that exports (the compactor
// lease holder) is usually another one, and the database lock that keeps a
// drop and an export apart (database_lock.go) is one process's. A reconcile
// pass on the exporting node that had loaded a table before the drop
// committed and published after the drop had listed what it would delete:
// what the pass wrote stayed, naming manifests that were gone, under a
// catalog row that still loaded. Every later pass of the table failed, also
// once the database was written again.
//
// Each drop on a shared bucket (forgetDroppedRows) therefore leaves a record
// per database, in the reserved marker root, that every node reads:
//
//   - The dropping node writes it before it lists anything, with a new id
//     and "not done", and rewrites it "done" when the drop has finished.
//   - The exporting node reads it at the start of each pass of a table of
//     that database. While a drop is running it gives way, as it does for a
//     drop in its own process: it must neither commit into a table that is
//     being deleted nor create one the drop's listings would then remove.
//   - A table carries the id of the drop record its database had when it was
//     created (tableDropIDProp). A table whose id is not the record's was
//     created before the latest drop: it is what that drop meant to remove,
//     whatever a pass has written into it since. The exporting node removes
//     it (its metadata objects and its catalog row) when it finds one: at
//     the end of the pass that overlapped the drop, or at the start of the
//     next pass, whichever sees it first, on any node and after a restart.
//
// A record that cannot be read is not evidence of anything: the pass gives
// way and removes nothing. A database with no record was never dropped on a
// shared bucket (or the record did not survive a restore, which does not
// copy the reserved root), and its tables are left alone whatever id they
// carry.
//
// The per-table drop markers (dropped.go) stay what they were: what lets a
// node forget a catalog row whose metadata is gone.

// tableDropIDProp is the table property holding the id of the database's
// drop record when the table was created; absent when there was none.
const tableDropIDProp = "arc.drop-id"

// dropRecordStale is how long a record that says a drop is running is
// believed. A drop is bounded by its request; one that died leaves its
// record "not done", and the database's export must not wait for it for
// good. Repeating the DELETE runs the drop again and finishes the record.
// A var for tests.
var dropRecordStale = 15 * time.Minute

// dropRecord is a database's drop record.
type dropRecord struct {
	ID      string    `json:"id"`
	Started time.Time `json:"started"`
	Done    bool      `json:"done"`
}

// dropRecordKey is the storage key of a database's drop record. "%drop" is
// not the escaped form of any namespace key (PathEscape escapes "%"), so it
// cannot be a table marker's directory. The database is the one the lock is
// kept under: for an edge-sync hub's "{spoke}/{db}", the spoke.
func dropRecordKey(database string) string {
	return droppedMarkerRoot + "/%drop/" + url.PathEscape(databaseLockKey(database))
}

// readDropRecord returns the database's drop record; exists is false when
// there is none. Only where records are written (forgetDroppedRows).
func (e *Exporter) readDropRecord(ctx context.Context, database string) (rec dropRecord, exists bool, err error) {
	if !e.forgetDroppedRows || e.backend == nil {
		return dropRecord{}, false, nil
	}
	data, err := e.backend.Read(ctx, dropRecordKey(database))
	if err != nil {
		if storage.IsNotFound(err) {
			return dropRecord{}, false, nil
		}
		return dropRecord{}, false, fmt.Errorf("read the Iceberg drop record of database %q: %w", database, err)
	}
	if err := json.Unmarshal(data, &rec); err != nil || rec.ID == "" {
		return dropRecord{}, false, fmt.Errorf("the Iceberg drop record of database %q is not readable: %q", database, data)
	}
	return rec, true, nil
}

func (e *Exporter) writeDropRecord(ctx context.Context, database string, rec dropRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := e.backend.Write(ctx, dropRecordKey(database), data); err != nil {
		return fmt.Errorf("write the Iceberg drop record of database %q: %w", database, err)
	}
	return nil
}

// beginDropRecord records that this node is dropping the database, before it
// lists or deletes anything, and returns the record. Only on a shared bucket.
func (e *Exporter) beginDropRecord(ctx context.Context, database string) (dropRecord, bool, error) {
	if !e.forgetDroppedRows || e.backend == nil {
		return dropRecord{}, false, nil
	}
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return dropRecord{}, false, fmt.Errorf("new Iceberg drop id: %w", err)
	}
	rec := dropRecord{ID: hex.EncodeToString(raw[:]), Started: time.Now().UTC()}
	if err := e.writeDropRecord(ctx, database, rec); err != nil {
		return dropRecord{}, false, err
	}
	return rec, true, nil
}

// removeDropRecord removes the record this drop wrote, when the drop found
// nothing to remove, and the directories that held it when they are empty.
// A record another drop has written meanwhile is left.
func (e *Exporter) removeDropRecord(ctx context.Context, database string, mine dropRecord) error {
	if cur, exists, err := e.readDropRecord(ctx, database); err != nil || !exists || cur.ID != mine.ID {
		return err
	}
	if err := e.backend.Delete(ctx, dropRecordKey(database)); err != nil && !storage.IsNotFound(err) {
		return fmt.Errorf("remove the Iceberg drop record of database %q: %w", database, err)
	}
	if remover, ok := e.backend.(storage.DirectoryRemover); ok {
		_ = remover.RemoveDirectory(ctx, droppedMarkerRoot+"/%drop") // fails when not empty: other databases' records
		_ = remover.RemoveDirectory(ctx, droppedMarkerRoot)
	}
	return nil
}

// settleDroppedTable is what a pass does about its database's drop record
// before it touches the table: give way to a drop that is running
// (errDatabaseBeingDropped, also when the record cannot be read), and remove
// a table that predates the latest drop. It records the record's id for the
// pass (a table the pass creates carries it, and the pass's end compares
// with it). A no-op where there are no records.
func (e *Exporter) settleDroppedTable(ctx context.Context, database, measurement string) error {
	e.dropSettled = true
	e.dropID = ""
	if !e.forgetDroppedRows || e.backend == nil {
		return nil
	}
	ctx = e.withAWS(ctx)
	rec, exists, err := e.readDropRecord(ctx, database)
	if err != nil {
		return fmt.Errorf("%w: %v", errDatabaseBeingDropped, err)
	}
	if !exists {
		return nil
	}
	if !rec.Done && time.Since(rec.Started) < dropRecordStale {
		return fmt.Errorf("database %q, on another node: %w", database, errDatabaseBeingDropped)
	}
	e.dropID = rec.ID
	return e.removeTableOfAnEarlierDrop(ctx, database, measurement, rec.ID)
}

// removeTableOfAnEarlierDrop removes this node's table when it was created
// before the drop whose record has id: its metadata objects and its catalog
// row. A table with no row here, one that does not load, and one created
// under id are left to the pass.
func (e *Exporter) removeTableOfAnEarlierDrop(ctx context.Context, database, measurement, id string) error {
	ident := e.tableIdent(database, measurement)
	nsKey := catalogNamespaceKey(ident[:len(ident)-1])
	loc, rowExists, err := e.catalogRow(ctx, nsKey, measurement)
	if err != nil || !rowExists {
		return err
	}
	tbl, err := e.catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil // a row whose metadata is gone: dropped.go's recovery
	}
	if tbl.Properties()[tableDropIDProp] == id {
		return nil
	}
	e.logger.Info().Str("database", database).Str("measurement", measurement).
		Msg("Iceberg: this table was created before its database was last dropped (on another node); removing what is left of it")
	if _, dirKey, ok := e.parseVersionAndMetaDir(tbl.MetadataLocation()); ok && dirKey != "" && dirKey != "." {
		keys, err := e.backend.List(ctx, dirKey+"/")
		if err != nil {
			return fmt.Errorf("list what is left of dropped table %s.%s: %w", database, measurement, err)
		}
		// The hint first, so a reader of the directory stops resolving the
		// table before its versions go.
		hint := path.Join(dirKey, "version-hint.text")
		if err := e.backend.Delete(ctx, hint); err != nil && !storage.IsNotFound(err) {
			return fmt.Errorf("delete %s: %w", hint, err)
		}
		for _, k := range keys {
			if k == hint || !strings.HasPrefix(k, dirKey+"/") {
				continue
			}
			if err := e.backend.Delete(ctx, k); err != nil && !storage.IsNotFound(err) {
				return fmt.Errorf("delete %s: %w", k, err)
			}
		}
	}
	if err := e.forgetCatalogRow(ctx, nsKey, measurement, loc); err != nil {
		return err
	}
	// What this node remembered of the table, and of its database's tables
	// for the scheduler: as after a drop in this process.
	e.countDatabaseDrop(database)
	e.forgetDatabaseMemory(database)
	if e.onDatabaseDropped != nil {
		e.onDatabaseDropped(database)
	}
	return nil
}

// errDroppedUnderPass is a pass's error when its database was dropped on
// another node while it ran: what it wrote has been removed.
var errDroppedUnderPass = errors.New("iceberg: the database was dropped on another node during this pass")

// droppedUnderPass reports, at the end of a pass that wrote something,
// whether the database's drop record is another one than at its start, and
// then removes the table. A record that cannot be read removes nothing: the
// next pass looks again.
func (e *Exporter) droppedUnderPass(ctx context.Context, database, measurement string, canCommit func() bool) bool {
	if !e.forgetDroppedRows || e.backend == nil {
		return false
	}
	rec, exists, err := e.readDropRecord(ctx, database)
	if err != nil || !exists || rec.ID == e.dropID {
		return false
	}
	if canCommit != nil && !canCommit() {
		return false // no longer the exporter: the node that is decides
	}
	if err := e.removeTableOfAnEarlierDrop(ctx, database, measurement, rec.ID); err != nil {
		e.logger.Warn().Err(err).Str("database", database).Str("measurement", measurement).
			Msg("Iceberg: could not remove a table whose database was dropped on another node during its pass; the next pass tries again")
	}
	return true
}

// dropIDOf is the table's drop id, for tests and logs.
func dropIDOf(tbl *icetable.Table) string { return tbl.Properties()[tableDropIDProp] }
