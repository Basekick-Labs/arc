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
// the cluster's primary writer. A reader never removes a cold row the primary
// deleted (#1563), so on a reader every normal cold DELETE or retention would
// read as data loss.
//
// A cold DELETE or retention run removes each object before its row, so a
// check that overlaps one would count rows it is about to retire. Such runs
// bracket themselves with BeginColdRemoval/EndColdRemoval, and a check that
// overlapped one keeps the previous result.

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// coldMissingCheckLimit bounds the stats one sync spends on candidates: a
// lifecycle rule that emptied the bucket leaves every row a candidate.
const coldMissingCheckLimit = 1000

// ColdMissingObjectsStatus is the last check of cold rows whose object is gone.
type ColdMissingObjectsStatus struct {
	// Lost counts cold rows whose object is gone from cold storage and that
	// have no hot copy: data the tier metadata says exists and no read returns.
	Lost int `json:"lost"`
	// HotCopy counts cold rows whose cold object is gone but whose hot copy is
	// still read through the hot tier: no data is missing.
	HotCopy int `json:"hot_copy"`
	// Unchecked counts candidates not checked: over the per-sync limit, or a
	// stat or row read that failed.
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
// checked: no usable cold tier, no sync yet, or a cluster reader.
// Nil-receiver safe.
func (m *Manager) ColdMissingObjects() *ColdMissingObjectsStatus {
	if m == nil || !m.coldTierUsable() {
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
		// A cold DELETE or retention overlapped: it may have deleted objects
		// whose rows it had not retired yet. Keep the previous result.
		return
	}
	st.CheckedAt = time.Now().UTC()
	m.coldMissing.Store(st)
	metrics.Get().SetTieringColdRowsMissingObjects(int64(st.Lost), int64(st.HotCopy))
	if st.Lost > 0 {
		m.logger.Error().Int("count", st.Lost).Strs("sample", st.Sample).
			Msg("Cold tier rows have no object in cold storage and no hot copy: these files cannot be read and nothing in Arc restores them. Check for bucket lifecycle/expiry rules or deletes outside Arc on the cold prefix; restore the objects from a backup, or delete the data through Arc to retire the rows")
	}
}

// recheckColdRow checks one candidate now. lost: the row is still cold and
// neither copy exists. hotCopy: the row is still cold, its cold object is
// gone and the hot copy exists. ok is false when a check failed.
func (m *Manager) recheckColdRow(ctx context.Context, path string) (lost, hotCopy, ok bool) {
	inCold, err := m.coldBackend.Exists(ctx, path)
	if err != nil {
		return false, false, false
	}
	if inCold {
		return false, false, true // written after the listing
	}
	row, err := m.metadata.GetFile(ctx, path)
	if err != nil {
		return false, false, false
	}
	if row == nil || row.Tier != TierCold || row.QuarantinedAt != nil {
		return false, false, true // retired, reverted or quarantined since
	}
	inHot, err := m.hotBackend.Exists(ctx, path)
	if err != nil {
		return false, false, false
	}
	return !inHot, inHot, true
}
