package iceberg

// Database drops on node-local warehouses (#1731).
//
// With node-local storage every node has its own Iceberg warehouse and
// catalog. A database DELETE runs on the primary writer and drops only the
// writer's tables; the node that exports, and any node that exported before
// a lease move, keeps tables naming the deleted files. Two mechanisms remove
// them, both wired only on such clusters (cmd/arc):
//
//   - The writer's replicated notice: each node's coordinator asks for the
//     drop once its delete workers have unlinked the database's files
//     (RequestDropDatabase).
//   - A backstop for a node that missed the notice (offline through the
//     DELETE, caught up by snapshot, restarted before its drop ran): every
//     dropBackstopEvery, each database in this node's catalog that the
//     cluster has no files under and this node holds no data file of is
//     dropped. A database emptied by retention loses its (empty) table the
//     same way; new data recreates it.
//
// These drops run on the loop goroutine and regardless of the gate. What
// keeps any drop, these and the API's own DELETE on its request's goroutine,
// from overlapping an export of the same tables is the exporter's database
// lock (database_lock.go, #1789). Regardless of the gate: a node that does not export is exactly the one
// whose stale tables nothing else removes. A drop is skipped while the
// cluster's manifest has files under the database (SetDropGuard, read after
// a Raft barrier): an old notice replayed at restart after the database was
// recreated, or a database recreated moments after its delete; the exporting
// node reconciles that table. A failed drop is retried with a growing delay.

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"
)

// dropRetry is a failed drop's retry state.
type dropRetry struct {
	attempts int
	next     time.Time
}

const (
	dropMaxAttempts   = 10
	dropRetryBase     = 5 * time.Second
	dropRetryMax      = 10 * time.Minute
	dropBackstopEvery = 10 * time.Minute
)

// SetDropGuard sets the predicate that skips a drop: the cluster manifest
// still registers files under the database. An error counts as a failed
// attempt. Setting it also enables the backstop. Call during wiring.
func (s *Scheduler) SetDropGuard(f func(ctx context.Context, database string) (bool, error)) {
	s.dropGuard = f
}

// RequestDropDatabase asks for this node's Iceberg tables of a deleted
// database to be dropped. Safe from any goroutine and never blocks; requests
// coalesce and wait for the loop, before it starts too.
func (s *Scheduler) RequestDropDatabase(database string) {
	if s == nil || database == "" || strings.ContainsAny(database, "/\\") {
		return
	}
	s.reqMu.Lock()
	s.drops[database] = struct{}{}
	s.reqMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// droppedThisPass reports whether the running pass dropped database (or the
// spoke above a hub's "{spoke}/{db}" database).
func (s *Scheduler) droppedThisPass(database string) bool {
	if len(s.passDrops) == 0 {
		return false
	}
	if _, ok := s.passDrops[database]; ok {
		return true
	}
	if spoke, _, found := strings.Cut(database, "/"); found {
		_, ok := s.passDrops[spoke]
		return ok
	}
	return false
}

// backstopDrops requests a drop for each database in this node's catalog that
// the cluster has no files under and this node holds no data file of. Loop
// goroutine only; at most every dropBackstopEvery.
func (s *Scheduler) backstopDrops(ctx context.Context) {
	if s.dropGuard == nil || s.exporter == nil || time.Now().Before(s.backstopDue) {
		return
	}
	s.backstopDue = time.Now().Add(dropBackstopEvery)
	dbs, err := s.exporter.CatalogDatabases(ctx)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Iceberg: could not list this node's catalog for tables of deleted databases; retried later")
		return
	}
	for _, db := range dbs {
		if ctx.Err() != nil {
			return
		}
		gctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		has, err := s.dropGuard(gctx, db)
		cancel()
		if err != nil || has {
			continue
		}
		lctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		local, err := s.exporter.HasLocalData(lctx, db)
		cancel()
		if err != nil || local {
			continue
		}
		s.logger.Info().Str("database", db).
			Msg("Iceberg: this node holds tables of a database the cluster has no files under; dropping them")
		s.RequestDropDatabase(db)
	}
}

// noteDatabaseDropped is told by the exporter that a database's tables were
// dropped, from whichever goroutine dropped them (the API's DELETE, or this
// scheduler's own loop). The loop forgets what it remembers of the database
// at its next service of drops: before its next batch of exports. Until then
// what it remembers is stale, and a pass does not trust it: the cached state
// carries the exporter's count of the database's drops, and a pass that finds
// another count starts from the table (passRunner.reconcile). An append to a
// table that is gone falls back to a pass. Never blocks.
func (s *Scheduler) noteDatabaseDropped(database string) {
	if s == nil || database == "" {
		return
	}
	s.reqMu.Lock()
	s.dropped[database] = struct{}{}
	s.reqMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// forgetDropped forgets the databases dropped since the last call. Loop
// goroutine only.
func (s *Scheduler) forgetDropped() {
	s.reqMu.Lock()
	if len(s.dropped) == 0 {
		s.reqMu.Unlock()
		return
	}
	dbs := make([]string, 0, len(s.dropped))
	for db := range s.dropped {
		dbs = append(dbs, db)
	}
	clear(s.dropped)
	s.reqMu.Unlock()
	for _, db := range dbs {
		s.forgetDatabase(db)
	}
}

// serviceDrops runs the requested drops that are due. Loop goroutine only.
func (s *Scheduler) serviceDrops(ctx context.Context) {
	if s.exporter == nil {
		return
	}
	// What a DELETE dropped since the last service (#1789), first.
	s.forgetDropped()
	s.backstopDrops(ctx)
	s.reqMu.Lock()
	if len(s.drops) == 0 {
		s.reqMu.Unlock()
		return
	}
	now := time.Now()
	var due []string
	for db := range s.drops {
		if r, ok := s.dropRetry[db]; ok && now.Before(r.next) {
			continue
		}
		due = append(due, db)
	}
	for _, db := range due {
		delete(s.drops, db)
	}
	s.reqMu.Unlock()

	for i, db := range due {
		if ctx.Err() != nil {
			for _, rest := range due[i:] {
				s.requeueDrop(rest)
			}
			return
		}
		var err error
		if s.dropGuard != nil {
			gctx, cancel := context.WithTimeout(ctx, measurementTimeout)
			var has bool
			has, err = s.dropGuard(gctx, db)
			cancel()
			if err == nil && has {
				delete(s.dropRetry, db)
				s.logger.Info().Str("database", db).
					Msg("Iceberg: not dropping the tables of a deleted database: the cluster has files under it again (recreated, or an old notice replayed); the exporting node reconciles them")
				continue
			}
		}
		if err == nil {
			dctx, cancel := context.WithTimeout(ctx, measurementTimeout)
			err = s.dropRecovered(dctx, db)
			cancel()
		}
		if err != nil {
			s.dropFailed(db, err)
			continue
		}
		delete(s.dropRetry, db)
		s.forgetDatabase(db)
		if s.passDrops == nil {
			s.passDrops = map[string]struct{}{}
		}
		s.passDrops[db] = struct{}{}
		s.logger.Info().Str("database", db).
			Msg("Iceberg: dropped this node's tables of a database deleted on the cluster")
	}
}

// dropRecovered is DropDatabase with a panic turned into an error: the
// trigger is replicated, so a panic would otherwise take every node down.
func (s *Scheduler) dropRecovered(ctx context.Context, db string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().Str("database", db).Str("panic", fmt.Sprint(r)).Str("stack", string(debug.Stack())).
				Msg("Iceberg: recovered a panic while dropping a deleted database's tables")
			err = fmt.Errorf("drop panicked: %v", r)
		}
	}()
	return s.exporter.DropDatabase(ctx, db)
}

func (s *Scheduler) dropFailed(db string, err error) {
	r := s.dropRetry[db]
	r.attempts++
	if r.attempts >= dropMaxAttempts {
		delete(s.dropRetry, db)
		s.logger.Error().Err(err).Str("database", db).Int("attempts", r.attempts).
			Msg("Iceberg: could not drop this node's tables of a deleted database; giving up until the periodic check finds them again")
		return
	}
	delay := dropRetryBase << (r.attempts - 1)
	if delay > dropRetryMax {
		delay = dropRetryMax
	}
	r.next = time.Now().Add(delay)
	s.dropRetry[db] = r
	s.logger.Warn().Err(err).Str("database", db).Int("attempt", r.attempts).Dur("retry_in", delay).
		Msg("Iceberg: could not drop this node's tables of a deleted database; retrying")
	s.requeueDrop(db)
	time.AfterFunc(delay, func() {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	})
}

func (s *Scheduler) requeueDrop(db string) {
	s.reqMu.Lock()
	s.drops[db] = struct{}{}
	s.reqMu.Unlock()
}

// forgetDatabase clears what the scheduler remembers about a dropped
// database's measurements, so a database recreated under the same name is
// reconciled from scratch rather than skipped on a stale fingerprint, and
// drops its cached cold listing.
func (s *Scheduler) forgetDatabase(db string) {
	match := func(key string) bool {
		return strings.HasPrefix(key, db+"\x00") || strings.HasPrefix(key, db+"/")
	}
	// Its tables' waiting reports too: a dropped table is not waiting for an
	// export (the per-table pending series, #1772).
	s.reqMu.Lock()
	dropped := false
	for k := range s.ingestPending {
		if match(k) {
			delete(s.ingestPending, k)
			dropped = true
		}
	}
	if dropped {
		s.publishIngestPendingLocked()
	}
	s.reqMu.Unlock()
	for k := range s.state {
		if match(k) {
			delete(s.state, k)
		}
	}
	for k := range s.lastReconciled {
		if match(k) {
			delete(s.lastReconciled, k)
		}
	}
	for k := range s.busySince {
		if match(k) {
			delete(s.busySince, k)
		}
	}
	for k := range s.lastBlocked {
		if match(k) {
			delete(s.lastBlocked, k)
		}
	}
	// The append path too (#1751): a dropped table must not take an append
	// (there is nothing to append to), and its files are gone.
	var appendKeys []string
	s.reqMu.Lock()
	for k := range s.appendPending {
		if match(k) {
			appendKeys = append(appendKeys, k)
		}
	}
	s.reqMu.Unlock()
	for k := range s.appendOK {
		if match(k) {
			appendKeys = append(appendKeys, k)
		}
	}
	for k := range s.recentAdds {
		if match(k) {
			appendKeys = append(appendKeys, k)
		}
	}
	for _, k := range appendKeys {
		s.forgetAppend(k)
	}
	if ci, ok := s.source.(coldInvalidator); ok {
		ci.InvalidateCold(db)
	}
}
