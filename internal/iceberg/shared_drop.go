package iceberg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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
//     and "not done", and rewrites it "done" when it has stopped deleting:
//     at the drop's end, or when the drop failed with its context still live
//     (an expired or cancelled one can leave deletes in flight, and keeps the
//     record "running" until it is stale). The record stays from then on,
//     also when the drop found nothing to remove: a first export may be
//     about to create the table. A restore does not remove it either
//     (#1796): what it brought back is judged against it like anything else.
//   - The exporting node reads it in each pass that is about to read or
//     write a table of that database (not in one that finds the table's
//     files unchanged). While a drop is running it gives way, as it does for
//     a drop in its own process: it must neither commit into a table that is
//     being deleted nor create one the drop's listings would then remove.
//   - A table carries the id of the drop record its database had when it was
//     created (tableDropIDProp). A table whose id is not the record's was
//     created before the latest drop: it is what that drop meant to remove,
//     whatever a pass has written into it since. The exporting node removes
//     it (its metadata objects and its catalog row) when it finds one: at
//     the end of the pass that overlapped the drop, or in a later pass of
//     the table, whichever sees it first, on any node and after a restart.
//     Its data is exported again as a new table, without the old one's
//     snapshot history.
//
// That last rule also takes a table that is healthy but older than the
// record: one restored from a backup made before the database was dropped
// (the record is in the bucket, not in the backup), and one an exporter still
// running a version without this created after a newer writer's drop. Each is
// removed once and exported again from its data.
//
// A record that cannot be read is not evidence of anything: the pass gives
// way and removes nothing. A database with no record was never dropped on a
// shared bucket, and its tables are left alone whatever id they carry.
//
// The clocks: a record that says "running" is believed for dropRecordStale
// by the exporting node's clock, counted from the dropping node's; the drop
// stops deleting at half of that. Nodes whose clocks differ by more than the
// other half could export into a drop that is still deleting.
//
// Ingest appends are not part of this: where records are kept there is a
// coordinator, and with one the ingest-triggered export is off
// (icebergIngestInterval, cmd/arc/cluster_topology.go); the exporting node
// runs timed passes only.
//
// The per-table drop markers (dropped.go) are what lets a node forget a
// catalog row whose metadata is gone. While one is set the #1613 fallbacks
// are off, as during a drop; the record says when that drop is over
// (dropInProgress, #1799).

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
	// Done: the dropping node has stopped deleting, at Finished: the drop
	// ended, or failed in a way that leaves no delete in flight (DropDatabase).
	Done     bool      `json:"done"`
	Finished time.Time `json:"finished,omitzero"`
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

// stopped is when the drop of rec stopped deleting, and whether it has: at
// Finished when it is done, at its deadline when it is stale (a dropping node
// that died), not yet when it is running.
func (rec dropRecord) stopped() (time.Time, bool) {
	switch {
	case rec.Done:
		if rec.Finished.IsZero() {
			// Written by a build without Finished: the drop's markers are
			// stamped before its deadline, which it never ran past.
			return dropDeadline(rec.Started), true
		}
		return rec.Finished, true
	case time.Since(rec.Started) >= dropRecordStale:
		return dropDeadline(rec.Started), true
	}
	return time.Time{}, false
}

// dropDeadline is when a drop whose record began at started stops deleting:
// half of what the record is believed for, the other half being what the
// nodes' clocks may differ by.
func dropDeadline(started time.Time) time.Time { return started.Add(dropRecordStale / 2) }

// finishDropRecord marks this drop's record done. A record another drop has
// written since (two writers around a failover) is that drop's to finish.
// A failure is logged and not returned: the tables are dropped, and all the
// record still does is hold the database's export back until it is stale.
func (e *Exporter) finishDropRecord(ctx context.Context, database string, mine dropRecord) {
	// Also when the drop's own context has ended (it failed on it).
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dropEndCheckTimeout)
	defer cancel()
	cur, exists, err := e.readDropRecord(ctx, database)
	if err == nil && exists && cur.ID != mine.ID {
		return
	}
	if err == nil {
		mine.Done, mine.Finished = true, time.Now().UTC()
		err = e.writeDropRecord(ctx, database, mine)
	}
	if err != nil {
		e.logger.Warn().Err(err).Str("database", database).Dur("paused_for_up_to", dropRecordStale).
			Msg("Iceberg: the drop's record could not be marked done; the export of a database written again under this name waits until the record is stale")
	}
}

// settleDroppedTable is what a pass does about its database's drop record
// before it touches the table: give way to a drop that is running
// (errDatabaseBeingDropped, also when the record cannot be read), and remove
// a table that predates the latest drop. It records the record's id for the
// pass (a table the pass creates carries it, and the pass's end compares
// with it). A no-op where there are no records.
func (e *Exporter) settleDroppedTable(ctx context.Context, database, measurement string, canCommit func() bool) error {
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
	// Known to be of this drop already: nothing to load.
	tkey := database + "\x00" + measurement
	e.dropSeenMu.Lock()
	seen := e.dropSeen[tkey] == rec.ID
	e.dropSeenMu.Unlock()
	if seen {
		return nil
	}
	// Removing is the exporter's to do: a node that is not it (any more)
	// leaves the table to the node that is.
	if canCommit != nil && !canCommit() {
		return nil
	}
	return e.removeTableOfAnEarlierDrop(ctx, database, measurement, rec)
}

// noteDropSeen remembers that the table carries the drop record id.
func (e *Exporter) noteDropSeen(database, measurement, id string) {
	e.dropSeenMu.Lock()
	defer e.dropSeenMu.Unlock()
	if e.dropSeen == nil {
		e.dropSeen = make(map[string]string)
	}
	e.dropSeen[database+"\x00"+measurement] = id
}

// removeTableOfAnEarlierDrop removes this node's table when it was created
// before the drop whose record has id: its metadata objects and its catalog
// row. A table with no row here, one that does not load, and one created
// under id are left to the pass.
func (e *Exporter) removeTableOfAnEarlierDrop(ctx context.Context, database, measurement string, rec dropRecord) error {
	id := rec.ID
	// The table as the warehouse has it, where another node published it
	// since this node's row was written (or this node has no row): what is
	// judged, and removed, is what the pass would load.
	if err := e.adoptBeforeLoad(ctx, database, measurement); err != nil {
		return err
	}
	ident := e.tableIdent(database, measurement)
	nsKey := catalogNamespaceKey(ident[:len(ident)-1])
	loc, rowExists, err := e.catalogRow(ctx, nsKey, measurement)
	if err != nil || !rowExists {
		return err
	}
	tbl, err := e.catalog.LoadTable(ctx, ident)
	if errors.Is(err, fs.ErrNotExist) {
		// A row whose metadata is gone, which the drop deleted: recovered
		// here (dropped.go), before the pass reads anything of the table,
		// so that what was cached of it is let go of in time.
		recovered, rerr := e.recoverDeadRow(ctx, ident)
		if rerr != nil || !recovered {
			return rerr
		}
		// Moved onto a published version (a restore, another node's
		// table): judged like any other below. Forgotten: nothing left.
		if loc, rowExists, err = e.catalogRow(ctx, nsKey, measurement); err != nil || !rowExists {
			return err
		}
		if tbl, err = e.catalog.LoadTable(ctx, ident); err != nil {
			return nil // the pass's own load reports it
		}
	}
	if err != nil {
		return nil // the pass's own load reports it
	}
	if tbl.Properties()[tableDropIDProp] == id {
		e.noteDropSeen(database, measurement, id)
		return nil
	}
	// The table's drop marker first, and this process's memory of having
	// cleared it: if the row cannot be forgotten below, it is a row whose
	// metadata is gone, and only the marker lets a later pass (here or on a
	// node that exported the table before) forget it (dropped.go). The
	// overlapped pass's own publish may have cleared the drop's marker.
	// Stamped with when the drop stopped, not now: the marker is that
	// drop's, and must not read as one of a drop still running
	// (dropInProgress). A record still running gives the zero time, which
	// reads as running anyway.
	at, _ := rec.stopped()
	if err := e.backend.Write(ctx, droppedMarkerKey(nsKey, measurement), []byte(at.UTC().Format(time.RFC3339)+"\n")); err != nil {
		return fmt.Errorf("write the drop marker of %s.%s: %w", database, measurement, err)
	}
	e.publishedMu.Lock()
	delete(e.markerCleared, strings.Join(ident, "\x00"))
	e.publishedMu.Unlock()
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
	e.removeEmptiedTableDirs(ctx, database, tbl.MetadataLocation())
	// What this node remembered of the table, and the stamp the scheduler
	// compares its cache with: of this table only, the database's other
	// tables are not touched. The scheduler's own per-table bookkeeping
	// for ingest appends is not told: there are no appends where drop
	// records are kept (shared_drop.go's header).
	e.countTableRemoval(database, measurement)
	e.forgetTableMemory(database, measurement)
	return nil
}

// removeEmptiedTableDirs removes the directories a removed table leaves
// empty on a filesystem (object stores have none): its metadata directory,
// its own, and the namespace's or, colocated, the database's (and the
// spoke's). An empty measurement directory would keep a dropped database
// listed (#1676). Each removal fails, and is meant to, when the directory
// still holds something: another table, or the data written since.
func (e *Exporter) removeEmptiedTableDirs(ctx context.Context, database, metaLoc string) {
	remover, ok := e.backend.(storage.DirectoryRemover)
	if !ok {
		return
	}
	_, dir, ok := e.parseVersionAndMetaDir(metaLoc)
	if !ok {
		return
	}
	levels := 3 // metadata, the table, the namespace or database
	if e.colocatedLayout() && strings.Contains(database, "/") {
		levels = 4 // and the spoke
	}
	for i := 0; i < levels && dir != "" && dir != "." && dir != "/"; i++ {
		if err := remover.RemoveDirectory(ctx, dir); err != nil {
			return
		}
		dir = path.Dir(dir)
	}
}

// dropEndCheckTimeout bounds a pass's look at the drop record at its end,
// and the removal that can follow.
const dropEndCheckTimeout = time.Minute

// errDroppedUnderPass is a pass's error when its database was dropped on
// another node while it ran: what it wrote has been removed.
var errDroppedUnderPass = errors.New("iceberg: the database was dropped on another node during this pass")

// droppedUnderPass reports, at the end of a pass that reached the table,
// whether the database's drop record is another one than at its start, and
// then removes the table. A record that cannot be read removes nothing: a
// later pass of the table, if its database is written again, looks again.
// On its own short context: a pass that ran out of time after a commit still
// has to look.
func (e *Exporter) droppedUnderPass(ctx context.Context, database, measurement string, canCommit func() bool) bool {
	if !e.forgetDroppedRows || e.backend == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dropEndCheckTimeout)
	defer cancel()
	rec, exists, err := e.readDropRecord(ctx, database)
	if err != nil || !exists || rec.ID == e.dropID {
		return false
	}
	if canCommit != nil && !canCommit() {
		return false // no longer the exporter: the node that is decides
	}
	if err := e.removeTableOfAnEarlierDrop(ctx, database, measurement, rec); err != nil {
		e.logger.Warn().Err(err).Str("database", database).Str("measurement", measurement).
			Msg("Iceberg: could not remove a table whose database was dropped on another node during its pass; the next pass tries again")
	}
	return true
}

// dropIDOf is the table's drop id, for tests and logs.
func dropIDOf(tbl *icetable.Table) string { return tbl.Properties()[tableDropIDProp] }
