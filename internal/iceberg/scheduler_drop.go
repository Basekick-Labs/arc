package iceberg

// Database drops requested by the cluster (#1731).
//
// With node-local storage every node has its own Iceberg warehouse and
// catalog. A database DELETE runs on the primary writer and drops only the
// writer's tables; the node that exports, and any node that exported before
// a lease move, keeps tables naming the deleted files. The writer then
// proposes a replicated database-drop notice, and each node's coordinator
// asks its scheduler to drop the database once its delete workers have
// unlinked the database's files (cluster.SetIcebergDatabaseDropper).
//
// The drop runs on the loop goroutine, so it never overlaps a reconcile, and
// regardless of the gate: a node that does not export is exactly the one
// whose stale tables nothing else removes. It is skipped while the cluster's
// manifest still holds files under the database (SetDropGuard): a Raft log
// replayed at restart can re-apply an old notice of a database recreated
// since, and a database recreated within moments of its delete still has its
// table, which the reconcile carries forward. A failed drop is retried with a
// growing delay.

import (
	"context"
	"strings"
	"time"
)

// dropRetry is a failed drop's retry state.
type dropRetry struct {
	attempts int
	next     time.Time
}

const (
	dropMaxAttempts = 10
	dropRetryBase   = 5 * time.Second
	dropRetryMax    = 10 * time.Minute
)

// SetDropGuard sets the predicate that skips a requested drop: the cluster
// manifest still registers files under the database. Call during wiring.
func (s *Scheduler) SetDropGuard(f func(database string) bool) { s.dropGuard = f }

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
	spoke, _, found := strings.Cut(database, "/")
	if found {
		_, ok := s.passDrops[spoke]
		return ok
	}
	return false
}

// serviceDrops runs the requested drops that are due. Loop goroutine only.
func (s *Scheduler) serviceDrops(ctx context.Context) {
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

	for _, db := range due {
		if ctx.Err() != nil {
			s.requeueDrop(db)
			return
		}
		if s.dropGuard != nil && s.dropGuard(db) {
			delete(s.dropRetry, db)
			s.logger.Info().Str("database", db).
				Msg("Iceberg: not dropping the tables of a deleted database: the cluster has files under it again (recreated, or an old notice replayed); the export keeps its tables current")
			continue
		}
		dctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		err := s.exporter.DropDatabase(dctx, db)
		cancel()
		if err != nil {
			r := s.dropRetry[db]
			r.attempts++
			if r.attempts >= dropMaxAttempts {
				delete(s.dropRetry, db)
				s.logger.Error().Err(err).Str("database", db).Int("attempts", r.attempts).
					Msg("Iceberg: could not drop this node's tables of a deleted database; giving up. Repeating the database DELETE (it answers 404) sends the drop again")
				continue
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
	if ci, ok := s.source.(coldInvalidator); ok {
		ci.InvalidateCold(db)
	}
}
