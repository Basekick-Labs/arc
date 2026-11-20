package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// One database's tables are not exported and dropped at once (#1789).
//
// A database DELETE drops its Iceberg tables from the request's goroutine
// (DropDatabase), and until this lock a reconcile pass or an append of one of
// those tables could be anywhere in its work at that moment. A pass that had
// loaded its table before the drop committed or published after it: the
// catalog got its row back (from the pass's commit, or adopted from a
// version file the drop had not reached yet) next to metadata naming
// manifests the drop then deleted. Such a table loads, so nothing treats it
// as gone, and every later pass of it fails; a database written again under
// the same name was exported into it.
//
// Every export of a table holds its database's lock shared, from before it
// lists the measurement to its end (holdDatabase: the scheduler's passes and
// appends). DropDatabase holds it alone. So a drop waits for the exports of
// that database in flight and no new one starts until it is done; an export
// that starts after it lists the storage the DELETE left.
//
// An export does not wait for a drop. It runs on the scheduler's loop, or in
// a batch the loop is waiting for, and while it waited no table of any other
// database would be exported; a drop deletes every metadata object of every
// table of its database, which on object storage is not quick. An export
// that finds its database being dropped (or a drop waiting for it) gives way
// at once (errDatabaseBeingDropped) and its table is tried again later, as
// one whose files are changing under the export is.
//
// The lock is this process's. On a shared bucket the node that handles the
// DELETE is usually not the node that exports, and that node's passes are
// not held by it: the database's drop record is what they go by
// (shared_drop.go, #1792).

// databaseLockShares is how many exports of one database may hold its lock
// together: more than can ever run at once. A drop takes all of them.
const databaseLockShares = 1 << 20

// dropLockWait is the longest DropDatabase waits for the exports of the
// database in flight, whatever time its caller gives it: a pass has two
// minutes, and a DELETE that waited that long has something wedged before
// it. A var for tests.
var dropLockWait = 150 * time.Second

type databaseLocks struct {
	mu sync.Mutex
	m  map[string]*semaphore.Weighted
	// drops counts the drops of each database (by lock key) in this
	// process: what was learnt of a table under one count is not true under
	// the next (databaseDrops).
	drops map[string]uint64
}

// databaseDrops is how many times the database's tables have been dropped in
// this process, a table removed because another node dropped its database
// included (shared_drop.go). A pass reads it while it holds the database, so
// no drop in this process changes it under the pass (the pass itself can,
// by removing such a table, and then it ends with an error and caches
// nothing); what the scheduler cached for a table under another number
// describes a table that is gone, whatever now has its name.
func (e *Exporter) databaseDrops(database string) uint64 {
	e.dbLocks.mu.Lock()
	defer e.dbLocks.mu.Unlock()
	return e.dbLocks.drops[databaseLockKey(database)]
}

func (e *Exporter) countDatabaseDrop(database string) {
	e.dbLocks.mu.Lock()
	defer e.dbLocks.mu.Unlock()
	if e.dbLocks.drops == nil {
		e.dbLocks.drops = make(map[string]uint64)
	}
	e.dbLocks.drops[databaseLockKey(database)]++
}

// databaseLockKey is what a database's lock is kept under: the database, or
// for an edge-sync hub's "{spoke}/{db}" the spoke, which is what a DELETE of
// the spoke drops.
func databaseLockKey(database string) string {
	if top, _, nested := strings.Cut(database, "/"); nested {
		return top
	}
	return database
}

func (l *databaseLocks) of(database string) *semaphore.Weighted {
	key := databaseLockKey(database)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		l.m = make(map[string]*semaphore.Weighted)
	}
	sem, ok := l.m[key]
	if !ok {
		sem = semaphore.NewWeighted(databaseLockShares)
		l.m[key] = sem
	}
	return sem
}

// errDatabaseBeingDropped is holdDatabase's refusal: the database's tables
// are being dropped, or a drop is waiting for their exports to end.
var errDatabaseBeingDropped = errors.New("iceberg: the database is being dropped")

// holdDatabase holds the database's lock for one export of one of its
// tables and returns the release. It never waits: only a drop, running or
// waiting its turn, keeps an export out, and then it returns
// errDatabaseBeingDropped.
func (e *Exporter) holdDatabase(database string) (release func(), err error) {
	sem := e.dbLocks.of(database)
	if !sem.TryAcquire(1) {
		return nil, fmt.Errorf("database %q: %w", database, errDatabaseBeingDropped)
	}
	return func() { sem.Release(1) }, nil
}

// lockDatabaseForDrop holds the database's lock alone, once no export of
// its tables is running, and returns the release. It gives up after
// dropLockWait or when ctx ends, whichever is first.
func (e *Exporter) lockDatabaseForDrop(ctx context.Context, database string) (release func(), err error) {
	sem := e.dbLocks.of(database)
	if sem.TryAcquire(databaseLockShares) {
		return func() { sem.Release(databaseLockShares) }, nil
	}
	wctx, cancel := context.WithTimeout(ctx, dropLockWait)
	defer cancel()
	if err := sem.Acquire(wctx, databaseLockShares); err != nil {
		return nil, fmt.Errorf("an Iceberg export of database %q was still running and its tables were not dropped; "+
			"repeat the DELETE to drop them: %w", database, err)
	}
	return func() { sem.Release(databaseLockShares) }, nil
}

// forgetDatabaseMemory drops what the exporter remembers of a dropped
// database's tables, so a database written again under the same name starts
// from what is in the warehouse and nothing else.
func (e *Exporter) forgetDatabaseMemory(database string) {
	// By table identifier: "<prefix>_<db>\x00<table>".
	if ns, err := namespaceIdentifier(e.nsPrefix, database); err == nil {
		prefix := strings.Join(ns, "\x00") + "\x00"
		e.publishedMu.Lock()
		for k := range e.published {
			if strings.HasPrefix(k, prefix) {
				delete(e.published, k)
			}
		}
		for k := range e.markerCleared {
			if strings.HasPrefix(k, prefix) {
				delete(e.markerCleared, k)
			}
		}
		e.publishedMu.Unlock()
	}
	e.dropSeenMu.Lock()
	for k := range e.dropSeen {
		if strings.HasPrefix(k, database+"\x00") || strings.HasPrefix(k, database+"/") {
			delete(e.dropSeen, k)
		}
	}
	e.dropSeenMu.Unlock()
	// By "<db>\x00<measurement>", and "<spoke>/<db>\x00<measurement>" under a spoke.
	match := func(k string) bool {
		return strings.HasPrefix(k, database+"\x00") || strings.HasPrefix(k, database+"/")
	}
	e.chunkMu.Lock()
	for k := range e.appendSmall {
		if match(k) {
			delete(e.appendSmall, k)
		}
	}
	for k := range e.chunkSize {
		if match(k) {
			delete(e.chunkSize, k)
		}
	}
	for k := range e.floorDue {
		if match(k) {
			delete(e.floorDue, k)
		}
	}
	for k := range e.leftAt {
		if match(k) {
			delete(e.leftAt, k)
		}
	}
	e.chunkMu.Unlock()
}

// SetOnDatabaseDropped sets what is told after a database's tables were
// dropped (the scheduler, which forgets what it remembers of them). Call it
// before the scheduler starts. It is called from DropDatabase's goroutine
// and must not block.
func (e *Exporter) SetOnDatabaseDropped(f func(database string)) {
	e.onDatabaseDropped = f
}
