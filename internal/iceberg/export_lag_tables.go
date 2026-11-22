package iceberg

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// Iceberg export lag per table (#1772): the Scheduler is the metrics
// package's IcebergTableLagSource, read at scrape time.

// noteLastLag records a table's last publish that covered ingest files. An
// identifier this exporter did not build has no database to label it with
// and is left out.
func (e *Exporter) noteLastLag(key string, ident []string, lag time.Duration, at time.Time) {
	db := databaseOfIdent(e.nsPrefix, ident)
	if db == "" {
		return
	}
	e.lag.mu.Lock()
	if e.lag.last == nil {
		e.lag.last = make(map[string]lastLag)
	}
	e.lag.last[key] = lastLag{database: db, measurement: ident[len(ident)-1], lag: lag, at: at}
	e.lag.mu.Unlock()
}

// clearLastLag forgets every table's last lag: this node stopped exporting
// (the gate closed, a restore paused it), so another node's publishes are
// the ones that count now, and two nodes must not report the same series.
func (e *Exporter) clearLastLag() {
	e.lag.mu.Lock()
	clear(e.lag.last)
	e.lag.mu.Unlock()
}

// lastLagWindow is how long a table's last lag is reported after its
// publish: at least an hour, and three timed passes, so a table exported
// only by the timer keeps its series between passes. A table missing from
// the series is therefore not known to be healthy, only not recently slow.
func (s *Scheduler) lastLagWindow() time.Duration {
	return max(time.Hour, 3*s.interval)
}

// pendingFloor is how long a table's oldest file must have waited before it
// counts as waiting: twice the interval its export runs at, so a healthy
// table between two exports is not one.
func (s *Scheduler) pendingFloor() time.Duration {
	if s.ingestInterval > 0 {
		return 2 * s.ingestInterval
	}
	return 2 * s.interval
}

// PendingTables implements metrics.IcebergTableLagSource: per table, how
// long its oldest file waiting for an export has waited, from the earlier of
// its first report since the last export that covered it (the ingest
// trigger, standalone) and the write time of its oldest committed file not
// yet published (every mode). Only tables past pendingFloor.
func (s *Scheduler) PendingTables(n int, now time.Time) ([]metrics.IcebergTableLag, int) {
	since := map[string]time.Time{} // "<db>\x00<m>" -> oldest
	s.reqMu.Lock()
	for key, r := range s.ingestPending {
		since[key] = r.first
	}
	s.reqMu.Unlock()
	if e := s.exporter; e != nil {
		e.lag.mu.Lock()
		for key, times := range e.lag.pending {
			ident := strings.Split(key, "\x00")
			db := databaseOfIdent(e.nsPrefix, ident)
			if db == "" || len(times) == 0 {
				continue
			}
			oldest := slices.MinFunc(times, func(a, b time.Time) int { return a.Compare(b) })
			k := db + "\x00" + ident[len(ident)-1]
			if cur, ok := since[k]; !ok || oldest.Before(cur) {
				since[k] = oldest
			}
		}
		e.lag.mu.Unlock()
	}
	floor := s.pendingFloor()
	var out []metrics.IcebergTableLag
	for key, t := range since {
		wait := now.Sub(t)
		if wait < floor {
			continue
		}
		db, m, _ := strings.Cut(key, "\x00")
		out = append(out, metrics.IcebergTableLag{Database: db, Measurement: m, Seconds: wait.Seconds()})
	}
	return topTables(out, n), len(out)
}

// LastLagTables implements metrics.IcebergTableLagSource: per table, the
// largest lag of its last publish that covered ingest files, within
// lastLagWindow. Older entries are deleted here, so the map holds only
// tables published recently.
func (s *Scheduler) LastLagTables(n int, now time.Time) []metrics.IcebergTableLag {
	e := s.exporter
	if e == nil {
		return nil
	}
	window := s.lastLagWindow()
	var out []metrics.IcebergTableLag
	e.lag.mu.Lock()
	for key, l := range e.lag.last {
		if now.Sub(l.at) > window {
			delete(e.lag.last, key)
			continue
		}
		out = append(out, metrics.IcebergTableLag{Database: l.database, Measurement: l.measurement, Seconds: l.lag.Seconds()})
	}
	e.lag.mu.Unlock()
	return topTables(out, n)
}

// topTables sorts worst first, ties by name so the series set is the same
// from one scrape to the next, and keeps n.
func topTables(t []metrics.IcebergTableLag, n int) []metrics.IcebergTableLag {
	slices.SortFunc(t, func(a, b metrics.IcebergTableLag) int {
		if c := cmp.Compare(b.Seconds, a.Seconds); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Database, b.Database); c != 0 {
			return c
		}
		return cmp.Compare(a.Measurement, b.Measurement)
	})
	if len(t) > n {
		t = t[:n]
	}
	return t
}
