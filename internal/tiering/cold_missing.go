package tiering

// Cold tier rows whose object is gone from cold storage (#1551).
//
// The cold-tier metadata sync already finds them: rows it loaded that its
// listing did not return. That set is not reliable on its own, because the
// rows are loaded after the listing: an object written (and its row recorded)
// between the two, by the migrator, a restore or a replication event, looks
// missing for one sync. So each candidate is checked again, at the moment of
// the check: the cold object is stat'ed, the row re-read, and the hot copy
// stat'ed. Only a row that is still cold, whose object is still absent, is
// counted, split by whether a hot copy still serves it.
//
// Counted only where this node's rows follow the store: a standalone node or
// the cluster's primary writer. A reader cannot tell the primary's cold
// DELETE or retention from a loss: it retires such rows instead, once both
// copies are confirmed gone (#1563, cold_retire_reader.go).
//
// A cold DELETE, retention run or database delete removes each object
// before its row, and so does the migrator when a compaction took a file
// over (dropTakenOver), so a check that overlaps one would count rows about
// to be retired. Each brackets itself with BeginColdRemoval/EndColdRemoval,
// and a check that overlapped one keeps the previous result.
//
// A row whose retirement failed after Arc deleted its object (logged by that
// DELETE or retention run) is counted as lost until a later run retires it.
//
// A check that could not examine every candidate (time or count budget, a
// failed stat) never lowers the published lost count: it keeps the previous
// result instead, so an alert is cleared only by a check that saw it gone.

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// coldMissingCheckLimit bounds the stats one sync spends on candidates: a
// lifecycle rule that emptied the bucket leaves every row a candidate.
const coldMissingCheckLimit = 1000

// coldMissingCheckBudget bounds the time the check takes out of the scan's
// budget: the hot scan runs after it on the same context, and a diagnostic
// must not truncate it. Candidates left when it runs out are unchecked.
const coldMissingCheckBudget = 30 * time.Second

// ColdMissingObjectsStatus is the last check of cold rows whose object is gone.
type ColdMissingObjectsStatus struct {
	// Lost counts cold rows whose object is gone from cold storage and that
	// have no hot copy: data the tier metadata says exists and no read returns.
	Lost int `json:"lost"`
	// HotCopy counts cold rows whose cold object is gone but whose hot copy is
	// still read through the hot tier: no data is missing.
	HotCopy int `json:"hot_copy"`
	// Unchecked counts candidates not checked: over the per-sync limit or
	// time budget, or a stat or row read that failed.
	Unchecked int `json:"unchecked,omitempty"`
	// Sample names up to five lost rows.
	Sample    []string  `json:"sample,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
}

// BeginColdRemoval is called by a cold DELETE or retention run before it
// deletes cold objects; EndColdRemoval when it has retired their rows.
// Nil-receiver safe.
func (m *Manager) BeginColdRemoval() {
	if m == nil {
		return
	}
	m.coldRemovals.Add(1)
	m.coldRemovalGen.Add(1)
}

// EndColdRemoval ends a BeginColdRemoval.
func (m *Manager) EndColdRemoval() {
	if m == nil {
		return
	}
	m.coldRemovalGen.Add(1)
	m.coldRemovals.Add(-1)
}

// ColdMissingObjects returns the last check, or nil when this node has not
// checked or does not report: no usable cold tier, no sync yet, or a cluster
// reader (including a primary demoted since its last check).
// Nil-receiver safe.
func (m *Manager) ColdMissingObjects() *ColdMissingObjectsStatus {
	if m == nil || !m.coldTierUsable() || m.roleGated() {
		return nil
	}
	return m.coldMissing.Load()
}

// checkMissingColdObjects checks the rows a completed sync found no object
// for, and publishes the result. unseen is that set (path -> size).
func (m *Manager) checkMissingColdObjects(ctx context.Context, unseen map[string]int64) {
	if m.roleGated() {
		m.coldMissing.Store(nil)
		metrics.Get().SetTieringColdRowsMissingObjects(0, 0)
		return
	}
	gen := m.coldRemovalGen.Load()
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, coldMissingCheckBudget)
	defer cancel()
	st := &ColdMissingObjectsStatus{}
	checked := 0
	for path := range unseen {
		if checked == coldMissingCheckLimit || ctx.Err() != nil {
			st.Unchecked++
			continue
		}
		checked++
		lost, hotCopy, ok := m.recheckColdRow(ctx, path)
		switch {
		case !ok:
			st.Unchecked++
		case lost:
			st.Lost++
			if len(st.Sample) < 5 {
				st.Sample = append(st.Sample, path)
			}
		case hotCopy:
			st.HotCopy++
		}
	}
	if m.coldRemovals.Load() > 0 || m.coldRemovalGen.Load() != gen {
		// A cold removal overlapped: it may have deleted objects whose rows
		// it had not retired yet. Keep the previous result.
		return
	}
	st.CheckedAt = time.Now().UTC()
	if !m.publishColdMissing(st, started) {
		return
	}
	if st.Lost > 0 {
		m.logger.Error().Int("count", st.Lost).Strs("sample", st.Sample).
			Msg("Cold tier rows have no object in cold storage and no hot copy: these files cannot be read and nothing in Arc restores them. Check for bucket lifecycle/expiry rules or deletes outside Arc on the cold prefix, or a DELETE or retention run that logged a failure to retire cold rows; restore the objects from a backup, or delete the data through Arc to retire the rows")
	}
}

// publishColdMissing stores st unless a check that started later already
// published, or st is incomplete and would lower the lost count. Concurrent
// syncs (ScanTiers and the cycle) can each run a check.
func (m *Manager) publishColdMissing(st *ColdMissingObjectsStatus, started time.Time) bool {
	m.coldMissingMu.Lock()
	defer m.coldMissingMu.Unlock()
	if started.Before(m.coldMissingStarted) {
		return false
	}
	if prev := m.coldMissing.Load(); st.Unchecked > 0 && prev != nil && st.Lost < prev.Lost {
		return false
	}
	m.coldMissingStarted = started
	m.coldMissing.Store(st)
	metrics.Get().SetTieringColdRowsMissingObjects(int64(st.Lost), int64(st.HotCopy))
	return true
}

// recheckColdRow checks one candidate now. lost: the row is still cold and
// neither copy exists. hotCopy: the row is still cold, its cold object is
// gone and the hot copy exists. ok is false when a check failed.
func (m *Manager) recheckColdRow(ctx context.Context, path string) (lost, hotCopy, ok bool) {
	_, lost, hotCopy, ok = m.recheckColdRowFull(ctx, path)
	return lost, hotCopy, ok
}

// recheckColdRowFull is recheckColdRow, also returning the row it read when
// that row is still a cold, unquarantined one.
func (m *Manager) recheckColdRowFull(ctx context.Context, path string) (row *FileMetadata, lost, hotCopy, ok bool) {
	inCold, err := m.coldBackend.Exists(ctx, path)
	if err != nil {
		return nil, false, false, false
	}
	if inCold {
		return nil, false, false, true // written after the listing
	}
	row, err = m.metadata.GetFile(ctx, path)
	if err != nil {
		return nil, false, false, false
	}
	if row == nil || row.Tier != TierCold || row.QuarantinedAt != nil {
		return nil, false, false, true // retired, reverted or quarantined since
	}
	inHot, err := m.hotBackend.Exists(ctx, path)
	if err != nil {
		return nil, false, false, false
	}
	return row, !inHot, inHot, true
}
