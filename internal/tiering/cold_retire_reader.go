package tiering

// Cold rows a cluster reader retires (#1563).
//
// A cold DELETE, a retention run and a database delete run only on the
// primary writer. Each removes cold objects and retires the rows in the
// primary's own SQLite. So does the migrator when it drops a cold copy.
// Nothing carries that to readers: tier events are node-local (the
// replication puller and the local delete worker) and never retire a cold
// row. The reader's cold sync finds such a row, because it loaded the row and
// its listing did not return the object, and until #1563 it left the row in
// place forever. Every reader then logged the row as missing on every cycle,
// and a reader promoted by failover reported it as lost data (#1551).
//
// A reader therefore retires the row itself, on the same evidence the primary
// acts on in delete_cold.go and retention_cold.go, checked at the moment it
// acts: the cold object is absent, the row is still cold and not quarantined,
// and there is no hot copy. A row with a hot copy is left alone here; the
// cold sync points it back at hot (see the revert above the call).
//
// Why a row deleted here cannot be one whose object exists: every writer of a
// cold row writes the object first. The migrator copies, then flips the row;
// a restore writes under GuardColdWrite, then records; a tier event probes
// the cold object, then marks the row; the sync lists, then records. So after
// each delete the object is stat'ed again. If it is there now, a writer raced
// the check, and the row is written back. The pass is skipped while a restore
// runs on this node, or when a marking or restore started since the sync
// began.
//
// The reader cannot tell the primary's removal from an object deleted outside
// Arc (a bucket lifecycle rule). Both are retired here. The primary keeps
// reporting the second as lost for as long as it leads; after a failover, the
// new primary no longer does.

import (
	"context"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// ColdStoreIdentity is implemented by the cluster manifest adapter: the cold
// store the cluster's cold mark (#1552) was seeded from. A reader whose own
// cold store differs retires nothing. A misconfigured bucket or prefix would
// otherwise read every row as missing.
type ColdStoreIdentity interface {
	ColdMarkBackend() (backend string, seeded, conflict bool)
}

// readerColdRetireWorkers bounds the concurrent stats a retirement pass makes.
const readerColdRetireWorkers = 8

// readerColdRetireBudget bounds the pass's time out of the scan's budget. A
// reader syncs once per migration cycle (daily by default), and a day of
// retention over hourly partitions retires thousands of rows, so the pass is
// bounded by time, not by count.
const readerColdRetireBudget = 5 * time.Minute

// readerColdRetireResult is what one pass did.
type readerColdRetireResult struct {
	retired   int
	kept      int // still cold: a hot copy, an object written meanwhile, a failed check
	unchecked int // left when the budget ran out
	sample    []string
	byScope   map[string]int
}

// readerRetireAllowed reports whether this sync may retire cold rows, and why
// not. listedObjects is how many objects the sync's listing returned.
func (m *Manager) readerRetireAllowed(authGen uint64, listedObjects int) (bool, string) {
	if !m.roleGated() {
		return false, "not a cluster reader"
	}
	if m.coldAuth.restoring.Load() > 0 || m.coldAuth.gen.Load() != authGen {
		return false, "a restore or marking ran during the sync"
	}
	if id, ok := m.manifest.(ColdStoreIdentity); ok {
		backend, seeded, conflict := id.ColdMarkBackend()
		switch {
		case conflict:
			return false, "the cluster's cold mark was seeded from two different cold stores"
		case seeded && backend != "" && backend != m.ColdBackendIdentity():
			return false, "this node's cold store (" + m.ColdBackendIdentity() + ") is not the one the cluster seeded from (" + backend + ")"
		case seeded && backend != "":
			return true, ""
		}
	}
	// Identity unknown (mark not seeded, or an older cluster). An empty
	// listing is the one shape a wrong bucket or prefix certainly produces.
	if listedObjects == 0 {
		return false, "the cold listing returned no objects and the cluster's cold store identity is unknown"
	}
	return true, ""
}

// retireUnseenColdRows retires the rows in unseen whose cold object and hot
// copy are both gone, and removes them from unseen. Called by the cold sync
// on a cluster reader only.
func (m *Manager) retireUnseenColdRows(ctx context.Context, unseen map[string]int64) readerColdRetireResult {
	res := readerColdRetireResult{byScope: map[string]int{}}
	if len(unseen) == 0 {
		return res
	}
	ctx, cancel := context.WithTimeout(ctx, readerColdRetireBudget)
	defer cancel()

	paths := make(chan string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range min(readerColdRetireWorkers, len(unseen)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range paths {
				retired, checked := m.retireOneUnseenColdRow(ctx, path)
				mu.Lock()
				switch {
				case retired:
					res.retired++
					if len(res.sample) < 5 {
						res.sample = append(res.sample, path)
					}
					if scope, ok := measurementScope(path); ok {
						res.byScope[scope]++
					}
					delete(unseen, path)
				case !checked:
					res.unchecked++
				default:
					res.kept++
				}
				mu.Unlock()
			}
		}()
	}
	// The map is read here and written by the workers under mu: take the
	// keys first.
	keys := make([]string, 0, len(unseen))
	for p := range unseen {
		keys = append(keys, p)
	}
	for _, p := range keys {
		if ctx.Err() != nil {
			mu.Lock()
			res.unchecked++
			mu.Unlock()
			continue
		}
		paths <- p
	}
	close(paths)
	wg.Wait()
	if res.retired > 0 {
		metrics.Get().AddTieringColdRowsRetiredByReader(int64(res.retired))
	}
	return res
}

// retireOneUnseenColdRow checks path now and retires its row when the cold
// object and the hot copy are both gone. checked is false when the check
// could not be made (a failed stat, the budget).
func (m *Manager) retireOneUnseenColdRow(ctx context.Context, path string) (retired, checked bool) {
	if ctx.Err() != nil {
		return false, false
	}
	row, lost, _, ok := m.recheckColdRowFull(ctx, path)
	if !ok {
		return false, false
	}
	if !lost {
		return false, true
	}
	if m.beforeReaderColdRetire != nil {
		m.beforeReaderColdRetire(path)
	}
	deleted, err := m.metadata.RetireColdRowIfUnquarantined(context.WithoutCancel(ctx), path)
	if err != nil {
		m.logger.Warn().Err(err).Str("path", path).Msg("Could not retire a cold row whose object and hot copy are gone")
		return false, false
	}
	if !deleted {
		return false, true // changed since the check
	}
	// Every cold writer writes the object before the row: an object that is
	// there now was written after the check, and its row must stay.
	inCold, err := m.coldBackend.Exists(context.WithoutCancel(ctx), path)
	if err == nil && !inCold {
		return true, true
	}
	migratedAt := time.Time{}
	if row.MigratedAt != nil {
		migratedAt = *row.MigratedAt
	}
	if _, werr := m.metadata.RecordColdFilesBatch(context.WithoutCancel(ctx), []FileMetadata{*row}, migratedAt); werr != nil {
		m.logger.Error().Err(werr).AnErr("stat_error", err).Str("path", path).
			Msg("Retired a cold row, then found its object may exist and could not write the row back; the next cold sync records it again from the listing")
	}
	return false, true
}
