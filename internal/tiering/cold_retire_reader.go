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

// readerColdRetireBudget bounds the pass's checks. A reader syncs once per
// migration cycle (daily by default), and a day of retention over hourly
// partitions retires thousands of rows, so the pass is bounded by time, not
// by count. It never takes more than half of what is left of the scan's own
// budget: the hot scan runs after it on the same context.
const readerColdRetireBudget = 5 * time.Minute

// readerColdStatTimeout bounds each stat or write made after the check
// budget, which runs detached: the rows are deleted by then, and leaving the
// pass part way would drop rows whose object a writer just put back.
const readerColdStatTimeout = 30 * time.Second

// readerColdRetireResult is what one pass did.
type readerColdRetireResult struct {
	retired   int
	kept      int // still cold: a hot copy, an object written meanwhile
	unchecked int // a failed check, or left when the budget ran out
	failed    int // retirable, but the row delete failed
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
			return false, "this node's cold store (" + m.ColdBackendIdentity() + ") is not the one the cluster seeded from (" + backend + "); every node's tiered_storage.cold settings must be spelled identically"
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

// readerRetireBudget is the time the checks may take out of ctx.
func readerRetireBudget(ctx context.Context) time.Duration {
	budget := readerColdRetireBudget
	if dl, ok := ctx.Deadline(); ok {
		if half := time.Until(dl) / 2; half < budget {
			budget = half
		}
	}
	return budget
}

// retireUnseenColdRows retires the rows in unseen whose cold object and hot
// copy are both gone, and removes them from unseen. Called by the cold sync
// on a cluster reader only.
//
// Three steps: concurrent checks under the time budget; one DELETE per chunk
// for every row that qualified (the tier cache invalidated once per
// measurement, not once per row); then a stat of each retired object, and a
// write-back, in one batch, of any that a writer put back meanwhile.
func (m *Manager) retireUnseenColdRows(ctx context.Context, unseen map[string]int64) readerColdRetireResult {
	res := readerColdRetireResult{byScope: map[string]int{}}
	if len(unseen) == 0 {
		return res
	}
	checkCtx, cancel := context.WithTimeout(ctx, readerRetireBudget(ctx))
	defer cancel()

	// 1. Checks.
	keys := make([]string, 0, len(unseen))
	for p := range unseen {
		keys = append(keys, p)
	}
	var mu sync.Mutex
	candidates := make(map[string]*FileMetadata)
	m.forEachConcurrently(checkCtx, keys, func(path string) {
		row, lost, _, ok := m.recheckColdRowFull(checkCtx, path)
		mu.Lock()
		defer mu.Unlock()
		switch {
		case !ok:
			res.unchecked++
		case lost:
			candidates[path] = row
		default:
			res.kept++
		}
	}, func() {
		mu.Lock()
		res.unchecked++
		mu.Unlock()
	})
	if len(candidates) == 0 {
		return res
	}

	// 2. One DELETE per chunk. Detached: these rows qualified, and each is
	// re-checked in step 3 whatever happens to the caller.
	paths := make([]string, 0, len(candidates))
	for p := range candidates {
		if m.beforeReaderColdRetire != nil {
			m.beforeReaderColdRetire(p)
		}
		paths = append(paths, p)
	}
	delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), readerColdStatTimeout)
	deleted, err := m.metadata.RetireColdRowsIfUnquarantined(delCtx, paths)
	delCancel()
	if err != nil {
		res.failed = len(paths) - len(deleted)
		m.logger.Warn().Err(err).Int("rows", res.failed).
			Msg("Could not retire cold rows whose object and hot copy are gone; the next cold sync tries again")
	}
	res.kept += len(paths) - len(deleted) - res.failed // changed since the check

	// 3. Every cold writer writes the object before the row: an object that
	// is there now was written after the check, and its row goes back, with
	// the size the store reports. A stat that fails writes the row back too;
	// the next sync decides again.
	var writeBack []FileMetadata
	m.forEachConcurrently(context.WithoutCancel(ctx), deleted, func(path string) {
		sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), readerColdStatTimeout)
		size, serr := m.coldBackend.StatFile(sctx, path)
		scancel()
		mu.Lock()
		defer mu.Unlock()
		if serr == nil && size < 0 {
			res.retired++
			if len(res.sample) < 5 {
				res.sample = append(res.sample, path)
			}
			if scope, ok := measurementScope(path); ok {
				res.byScope[scope]++
			}
			delete(unseen, path)
			return
		}
		row := *candidates[path]
		if serr == nil {
			row.SizeBytes = size
		}
		writeBack = append(writeBack, row)
		res.kept++
	}, nil)
	if len(writeBack) > 0 {
		wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), readerColdStatTimeout)
		if _, werr := m.metadata.RecordColdFilesBatch(wctx, writeBack, time.Now()); werr != nil {
			m.logger.Error().Err(werr).Int("rows", len(writeBack)).
				Msg("Retired cold rows whose object then reappeared or could not be checked, and could not write them back; the next cold sync records them again from its listing")
		}
		wcancel()
	}
	if res.retired > 0 {
		metrics.Get().AddTieringColdRowsRetiredByReader(int64(res.retired))
	}
	return res
}

// forEachConcurrently calls fn for each item on readerColdRetireWorkers
// goroutines. Items left when ctx ends are passed to skipped (if set)
// instead.
func (m *Manager) forEachConcurrently(ctx context.Context, items []string, fn func(string), skipped func()) {
	ch := make(chan string)
	var wg sync.WaitGroup
	for range min(readerColdRetireWorkers, len(items)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range ch {
				fn(it)
			}
		}()
	}
	for _, it := range items {
		if ctx.Err() != nil {
			if skipped != nil {
				skipped()
			}
			continue
		}
		ch <- it
	}
	close(ch)
	wg.Wait()
}
