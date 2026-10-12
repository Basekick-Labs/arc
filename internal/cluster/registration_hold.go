package cluster

import (
	"sort"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/wal"
)

// The registration hold (#1207 S4c): the WAL keeps a covered batch's entries
// until the file it was flushed into is registered, across restarts too. This
// outbox is the authority on what is still waiting — its journal survives a
// crash — so the WAL asks it through CoverageHold.
//
// A batch is held while an entry carrying its coverage is pending (loaded
// from the journal included, and after Stop or Abandon: the next start
// registers it), and after its acknowledgement until that ack is durable: a
// crash before the ack is synced re-registers the file on the next start, and
// a refusal then needs the entries.

type holdKey struct {
	instance uint64
	key      string
}

// coverageHold is an immutable snapshot: merged intervals per (instance, key).
type coverageHold map[holdKey][][2]uint64

func (h coverageHold) HoldsAny(instance uint64, key string, lo, hi uint64) bool {
	v := h[holdKey{instance, key}]
	i := sort.Search(len(v), func(i int) bool { return v[i][1] >= lo })
	return i < len(v) && v[i][0] <= hi
}

func (h coverageHold) add(instance uint64, key string, lo, hi uint64) {
	k := holdKey{instance, key}
	h[k] = append(h[k], [2]uint64{lo, hi})
}

func (h coverageHold) merge() {
	for k, v := range h {
		sort.Slice(v, func(i, j int) bool { return v[i][0] < v[j][0] })
		out := v[:1]
		for _, iv := range v[1:] {
			last := &out[len(out)-1]
			if iv[0] <= last[1]+1 {
				if iv[1] > last[1] {
					last[1] = iv[1]
				}
				continue
			}
			out = append(out, iv)
		}
		h[k] = out
	}
}

// CoverageHold returns the current hold. Cheap when nothing changed since the
// last call; rebuilt from the pending entries otherwise.
func (r *RegistrationOutbox) CoverageHold() wal.CoverageHold {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hold != nil && r.holdBuiltGen == r.holdGen && r.holdBuiltSynced == r.synced {
		return r.hold
	}
	h := coverageHold{}
	seen := map[*ingest.GroupCoverage]bool{} // a group's files share one coverage
	addEntry := func(e *outboxEntry) {
		if seen[e.rec.Cov] {
			return
		}
		seen[e.rec.Cov] = true
		spans, _ := e.rec.Cov.HeldSpans()
		for _, s := range spans {
			h.add(s.Instance, e.rec.Cov.Key, s.Lo, s.Hi)
		}
	}
	for _, e := range r.byPath {
		if !e.done && e.rec.Cov != nil {
			addEntry(e)
		}
	}
	r.pruneUnsyncedAcksLocked()
	for _, e := range r.unsyncedAcks {
		addEntry(e)
	}
	h.merge()
	r.hold, r.holdBuiltGen, r.holdBuiltSynced = h, r.holdGen, r.synced
	return h
}

// noteAckLocked records that e's ack record was just appended: its batches
// stay held until that record is durable. Caller holds r.mu.
func (r *RegistrationOutbox) noteAckLocked(e *outboxEntry) {
	if e.rec.Cov == nil {
		return
	}
	r.holdGen++
	e.ackAt = r.written
	r.unsyncedAcks = append(r.unsyncedAcks, e)
}

// pruneUnsyncedAcksLocked drops acknowledgements that are durable now. ackAt
// grows with the journal, so they are a prefix. Caller holds r.mu.
func (r *RegistrationOutbox) pruneUnsyncedAcksLocked() {
	i := 0
	for i < len(r.unsyncedAcks) && r.unsyncedAcks[i].ackAt <= r.synced {
		r.unsyncedAcks[i] = nil
		i++
	}
	if i > 0 {
		r.unsyncedAcks = append(r.unsyncedAcks[:0], r.unsyncedAcks[i:]...)
	}
}

// unknownHolds counts loaded entries whose coverage was truncated by a
// journal written before S4c, which has no bounds: the WAL entries they need
// are unknown, and nothing holds them. Caller holds r.mu.
func (r *RegistrationOutbox) unknownHoldsLocked() int {
	n := 0
	for _, e := range r.byPath {
		if !e.done && e.rec.Cov != nil {
			if _, ok := e.rec.Cov.HeldSpans(); !ok {
				n++
			}
		}
	}
	return n
}

// HoldsCoverage reports whether any WAL entry is held for a pending file.
func (r *RegistrationOutbox) HoldsCoverage() bool {
	return len(r.CoverageHold().(coverageHold)) > 0
}
