package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Stream takeover state (#1207 S4b).
//
// When a primary writer dies, a standby that received its batches over the
// origin stream claims them and publishes the ones no registered file holds.
// Two pieces of replicated state make that exactly once:
//
//   - the settled index: per writer instance and buffer key, which batches
//     already have their rows in a registered file, per partition hour. A
//     covered registration overlapping it is refused as a duplicate. Files
//     carry their exact coverage (one file per partition hour of a flush), so
//     the index is exact per (batch, hour).
//   - claims: the batches a standby took over. The origin's own later
//     registrations of claimed batches are refused, the claimant's accepted.
//
// Every rule here is a pure function of the log. It is all inert until the
// StreamClaims flag is applied, which needs every node to advertise
// CapabilityStreamClaims; until then registrations behave as before and the
// commands below are refused.

// CapabilityStreamClaims is advertised at join by a node that applies the
// takeover rules and runs with cluster.stream_takeover (writers: and
// cluster.wal_coverage). Earlier builds advertised stream_claims_v1 (S4a) and
// stream_claims_v2 (S4b1a/b, no claimant, no exit on a claim) and
// stream_claims_v3 (S4b1c1: a leave still orphaned the leaving writer's
// instances) and never enabled the flag; they block it.
const CapabilityStreamClaims = "stream_claims_v4"

// Instance status, from its origin's announcements.
const (
	InstanceCurrent  = "current"  // the origin's live process
	InstanceAdopted  = "adopted"  // a previous process; the origin still holds its unflushed batches
	InstanceClosed   = "closed"   // a previous process the origin, on the same disks, holds nothing of
	InstanceOrphaned = "orphaned" // a previous process the origin cannot vouch for (new disks, removed node)
)

// Refusal kinds for a covered registration, in the order they are checked.
const (
	RefusedClaimed   = "claimed"   // a batch another writer claimed
	RefusedStale     = "stale"     // a claimant file for a claim that is gone or does not include it
	RefusedDuplicate = "duplicate" // a batch whose rows for this hour are already registered
	// RefusedFull: a claimant file the settled index has no room for. Taking
	// it would stop the index tracking the instance, and nothing could then
	// arbitrate the origin's copies against the claimant's.
	RefusedFull = "full"
)

// Bounds on the replicated takeover state, which every node keeps and
// snapshots. A per-instance bound marks that instance untracked rather than
// refusing anything on the registration path: failover then falls back to
// today's behaviour for it, loudly.
const (
	maxTakeoverInstances       = 4096
	maxIndexEntries            = 1 << 20 // whole intervals + partial pieces + lost intervals, all instances
	maxKeyIntervals            = 4096
	maxKeyPartial              = 2048
	maxKeyLost                 = 1024
	maxInstanceKeys            = 4096
	maxTakeoverClaims          = 1024
	maxClaimIntervalsPerKey    = 4096
	MaxTakeoverPayloadSize     = 256 << 10
	maxRefusedOpsReported      = 64
	maxTakeoverKeyLen          = 512
	maxAnnounceHolds           = 256
	maxTakeoverReportedWriters = 256
	maxCoverageEntries         = 8192 // ranges + partial pieces of one instance in one file
	maxCoverageInstances       = 16   // instances in one file's coverage
)

// CoveragePiece is part of one batch: a chunk of an oversized one and/or a
// row range of it, as WAL recovery replays them. Whole batches travel as
// ranges instead.
type CoveragePiece struct {
	CSeq     uint64 `json:"c"`
	Chunk    uint16 `json:"k,omitempty"`
	Chunks   uint16 `json:"n,omitempty"`
	RowLo    int    `json:"rl,omitempty"`
	RowHi    int    `json:"rh,omitempty"`
	RowTotal int    `json:"rt,omitempty"`
}

func (p CoveragePiece) whole() bool { return p.Chunks == 0 && p.RowTotal == 0 }

// piecesOverlap reports whether two pieces of one batch share a row. Pieces
// whose row coordinates differ (a chunk against an unchunked row range)
// overlap conservatively.
func piecesOverlap(a, b CoveragePiece) bool {
	if a.CSeq != b.CSeq {
		return false
	}
	if a.whole() || b.whole() {
		return true
	}
	if a.Chunks > 0 && b.Chunks > 0 && a.Chunk != b.Chunk {
		return false
	}
	if (a.Chunks > 0) != (b.Chunks > 0) || a.RowTotal == 0 || b.RowTotal == 0 {
		return true
	}
	return a.RowLo < b.RowHi && b.RowLo < a.RowHi
}

// InstanceCoverage is a file's coverage for one writer instance: the batches
// of buffer Key with at least one row in the file. Truncated is set when the
// lists were capped. ClaimEpoch is set on files a claimant registers for
// its claim.
type InstanceCoverage struct {
	Instance   uint64          `json:"i"`
	Key        string          `json:"k"`
	Ranges     [][2]uint64     `json:"r,omitempty"`
	Partial    []CoveragePiece `json:"p,omitempty"`
	Truncated  bool            `json:"t,omitempty"`
	ClaimEpoch uint64          `json:"e,omitempty"`
}

// cseqs returns the batches of c as intervals.
func (c InstanceCoverage) cseqs() Intervals {
	v := normalized(c.Ranges)
	for _, p := range c.Partial {
		v = v.addRange(p.CSeq, p.CSeq)
	}
	return v
}

// StreamInstance is one writer process the cluster knows of.
type StreamInstance struct {
	Instance    uint64   `json:"instance"`
	Origin      string   `json:"origin"`
	AnnouncedAt uint64   `json:"announced_at"`
	Status      string   `json:"status"`
	AdoptedBy   uint64   `json:"adopted_by,omitempty"`
	Untracked   bool     `json:"untracked,omitempty"`
	OriginGone  bool     `json:"origin_gone,omitempty"`
	ReportedBy  []string `json:"reported_by,omitempty"` // writers that found nothing to claim of it
}

// keyIndex is one (instance, key)'s settled batches: every batch at or below
// Floor is settled in every hour except those in Lost; above it, per
// partition hour, the whole batches and pieces with rows registered.
type keyIndex struct {
	Floor uint64               `json:"f,omitempty"`
	Lost  Intervals            `json:"l,omitempty"`
	Hours map[int64]*hourIndex `json:"h,omitempty"`
	// LostHours are, per partition hour, the pieces at or below Floor of
	// files that vanished before any node pulled them (#1207 S4b1c2): their
	// rows in that hour are in no file, so they are not settled there, while
	// the batches' rows in other hours, and other rows of the hour, still are.
	// A piece leaves once Hours covers it again (republished).
	LostHours map[int64]*hourIndex `json:"lh,omitempty"`
}

type hourIndex struct {
	Whole   Intervals       `json:"w,omitempty"`
	Partial []CoveragePiece `json:"p,omitempty"`
}

func (k *keyIndex) entries() int {
	n := len(k.Lost)
	for _, h := range k.Hours {
		n += len(h.Whole) + len(h.Partial)
	}
	for _, h := range k.LostHours {
		n += len(h.Whole) + len(h.Partial)
	}
	return n
}

// floorSettled reports whether batch c is settled by the floor: at or below
// it and not lost. Per hour, lostPieces may still take rows of it back.
func (k *keyIndex) floorSettled(c uint64) bool { return c <= k.Floor && !k.Lost.contains(c) }

// lostPieces returns the pieces of batch c whose rows in hour vanished; whole
// when the whole batch did.
func (k *keyIndex) lostPieces(c uint64, hour int64) (whole bool, ps []CoveragePiece) {
	lh := k.LostHours[hour]
	if lh == nil {
		return false, nil
	}
	if lh.Whole.contains(c) {
		return true, nil
	}
	for _, q := range lh.Partial {
		if q.CSeq == c {
			ps = append(ps, q)
		}
	}
	return false, ps
}

// lostBatches returns the batches with a vanished piece in hour.
func (k *keyIndex) lostBatches(hour int64) Intervals {
	lh := k.LostHours[hour]
	if lh == nil {
		return nil
	}
	out := append(Intervals(nil), lh.Whole...)
	for _, q := range lh.Partial {
		out = out.addRange(q.CSeq, q.CSeq)
	}
	return out
}

// floorSettledSet is the batches settled by the floor in every hour: at or
// below it, not lost, with no vanished piece in any hour. What a claim never
// takes.
func (k *keyIndex) floorSettledSet() Intervals {
	if k == nil || k.Floor == 0 {
		return nil
	}
	s := Intervals{{0, k.Floor}}.minus(k.Lost)
	for h := range k.LostHours {
		s = s.minus(k.lostBatches(h))
	}
	return s
}

// pruneLost drops the vanished pieces of hour that Hours covers again.
func (k *keyIndex) pruneLost(hour int64) {
	lh, h := k.LostHours[hour], k.Hours[hour]
	if lh == nil || h == nil {
		return
	}
	lh.Whole = lh.Whole.minus(h.Whole)
	// A whole vanished batch republished in row or chunk pieces: only batches
	// with pieces in this hour can be.
	for _, q := range h.Partial {
		if lh.Whole.contains(q.CSeq) && piecesCover(h.samePieces(q.CSeq), CoveragePiece{CSeq: q.CSeq}) {
			lh.Whole = lh.Whole.removeRange(q.CSeq, q.CSeq)
		}
	}
	kept := lh.Partial[:0]
	for _, q := range lh.Partial {
		// Re-covered whole, or every row of the batch in pieces (in any row
		// coordinates), or the vanished rows themselves in the same ones.
		same := h.samePieces(q.CSeq)
		if h.Whole.contains(q.CSeq) || piecesCover(same, CoveragePiece{CSeq: q.CSeq}) || piecesCover(same, q) {
			continue
		}
		kept = append(kept, q)
	}
	lh.Partial = kept
	if len(lh.Whole) == 0 && len(lh.Partial) == 0 {
		delete(k.LostHours, hour)
	}
}

func (h *hourIndex) samePieces(c uint64) []CoveragePiece {
	if h == nil {
		return nil
	}
	var out []CoveragePiece
	for _, q := range h.Partial {
		if q.CSeq == c {
			out = append(out, q)
		}
	}
	return out
}

// settledWhole returns the batches of [lo, hi] whose rows in hour are, at
// least in part, in a registered file.
func (k *keyIndex) settledWhole(lo, hi uint64, hour int64) Intervals {
	if k == nil {
		return nil
	}
	var out Intervals
	if k.Floor >= lo {
		// A batch whose rows of this hour all vanished is not settled here;
		// one with only some rows vanished still has rows in a file.
		out = Intervals{{lo, minU64(hi, k.Floor)}}.minus(k.Lost)
		if lh := k.LostHours[hour]; lh != nil {
			out = out.minus(lh.Whole)
		}
	}
	if h := k.Hours[hour]; h != nil {
		out = out.union(h.Whole.clip(lo, hi))
		for _, q := range h.Partial {
			if q.CSeq >= lo && q.CSeq <= hi {
				out = out.addRange(q.CSeq, q.CSeq)
			}
		}
	}
	return out
}

// settledPiece reports whether rows of piece p in hour are in a registered
// file.
func (k *keyIndex) settledPiece(p CoveragePiece, hour int64) bool {
	if k == nil {
		return false
	}
	if k.floorSettled(p.CSeq) {
		if whole, lost := k.lostPieces(p.CSeq, hour); !whole && !piecesCover(lost, p) {
			return true // some rows of p are in a file
		}
	}
	h := k.Hours[hour]
	if h == nil {
		return false
	}
	if h.Whole.contains(p.CSeq) {
		return true
	}
	for _, q := range h.Partial {
		if piecesOverlap(p, q) {
			return true
		}
	}
	return false
}

// StreamClaim is the batches of one instance a writer took over.
type StreamClaim struct {
	Instance     uint64               `json:"instance"`
	Origin       string               `json:"origin"`
	By           string               `json:"by"`
	Epoch        uint64               `json:"epoch"` // the log index of the claim
	Set          map[string]Intervals `json:"set"`
	Done         bool                 `json:"done,omitempty"`
	Released     bool                 `json:"released,omitempty"`
	Unreplayable map[string]Intervals `json:"unreplayable,omitempty"`
	At           time.Time            `json:"at"`
	// ExtendedAt is the log index of the last ExtendStreamClaim: a claimant
	// computes what to replay only from a manifest at or past it.
	ExtendedAt uint64 `json:"extended_at,omitempty"`
}

func (c *StreamClaim) active() bool { return c != nil && !c.Released }

// takeoverState is the FSM's takeover state, under ClusterFSM.mu.
type takeoverState struct {
	instances map[uint64]*StreamInstance
	current   map[string]uint64
	index     map[uint64]map[string]*keyIndex
	claims    map[uint64]*StreamClaim
	// departed are writers that left gracefully (a restart, usually): their
	// instances and claims are kept until they re-join, or until the leader
	// expires the departure (RemoveReasonDepartedExpired). Keyed by node id,
	// valued by the departure time the leave carried.
	departed map[string]time.Time
	entries  int    // index entries across every instance (bounded by maxIndexEntries)
	gen      uint64 // bumped on every change; not replicated (a reader's cache key)
}

func newTakeoverState() *takeoverState {
	return &takeoverState{
		instances: map[uint64]*StreamInstance{},
		current:   map[string]uint64{},
		index:     map[uint64]map[string]*keyIndex{},
		claims:    map[uint64]*StreamClaim{},
		departed:  map[string]time.Time{},
	}
}

// takeoverSnapshot is the snapshot form; nil in older snapshots.
type takeoverSnapshot struct {
	Instances []StreamInstance                `json:"instances,omitempty"`
	Index     map[string]map[string]*keyIndex `json:"index,omitempty"` // instance (hex) -> key
	Claims    []StreamClaim                   `json:"claims,omitempty"`
	Departed  map[string]time.Time            `json:"departed,omitempty"`
}

func instKey(i uint64) string { return fmt.Sprintf("%016x", i) }

func (t *takeoverState) snapshot() *takeoverSnapshot {
	if len(t.instances) == 0 && len(t.claims) == 0 && len(t.departed) == 0 {
		return nil
	}
	s := &takeoverSnapshot{Index: map[string]map[string]*keyIndex{}}
	for _, id := range sortedInstanceIDs(t.instances) {
		s.Instances = append(s.Instances, *t.instances[id])
	}
	for id, keys := range t.index {
		cp := make(map[string]*keyIndex, len(keys))
		for k, v := range keys {
			cp[k] = v.clone()
		}
		s.Index[instKey(id)] = cp
	}
	ids := make([]uint64, 0, len(t.claims))
	for id := range t.claims {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		s.Claims = append(s.Claims, t.claims[id].clone())
	}
	if len(t.departed) > 0 {
		s.Departed = make(map[string]time.Time, len(t.departed))
		for id, at := range t.departed {
			s.Departed[id] = at
		}
	}
	return s
}

func restoreTakeover(s *takeoverSnapshot) *takeoverState {
	t := newTakeoverState()
	if s == nil {
		return t
	}
	for i := range s.Instances {
		in := s.Instances[i]
		t.instances[in.Instance] = &in
		if in.Status == InstanceCurrent {
			t.current[in.Origin] = in.Instance
		}
	}
	for hex, keys := range s.Index {
		var id uint64
		if _, err := fmt.Sscanf(hex, "%016x", &id); err != nil {
			continue
		}
		t.index[id] = keys
		for _, k := range keys {
			if k.Hours == nil {
				k.Hours = map[int64]*hourIndex{}
			}
			t.entries += k.entries()
		}
	}
	for i := range s.Claims {
		c := s.Claims[i]
		t.claims[c.Instance] = &c
	}
	for id, at := range s.Departed {
		t.departed[id] = at
	}
	return t
}

func (k *keyIndex) clone() *keyIndex {
	c := &keyIndex{Floor: k.Floor, Lost: append(Intervals(nil), k.Lost...), Hours: make(map[int64]*hourIndex, len(k.Hours))}
	for h, v := range k.Hours {
		c.Hours[h] = &hourIndex{Whole: append(Intervals(nil), v.Whole...), Partial: append([]CoveragePiece(nil), v.Partial...)}
	}
	if len(k.LostHours) > 0 {
		c.LostHours = make(map[int64]*hourIndex, len(k.LostHours))
		for h, v := range k.LostHours {
			c.LostHours[h] = &hourIndex{Whole: append(Intervals(nil), v.Whole...), Partial: append([]CoveragePiece(nil), v.Partial...)}
		}
	}
	return c
}

func (c *StreamClaim) clone() StreamClaim {
	cc := *c
	cc.Set = cloneIntervalMap(c.Set)
	cc.Unreplayable = cloneIntervalMap(c.Unreplayable)
	return cc
}

func cloneIntervalMap(m map[string]Intervals) map[string]Intervals {
	if m == nil {
		return nil
	}
	out := make(map[string]Intervals, len(m))
	for k, v := range m {
		out[k] = append(Intervals(nil), v...)
	}
	return out
}

func sortedInstanceIDs(m map[uint64]*StreamInstance) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// ---- refusals ----

// RefusedOp names one refused registration of a batch.
type RefusedOp struct {
	Op       int    `json:"op"`
	Kind     string `json:"kind"`
	Instance uint64 `json:"instance,omitempty"`
	Epoch    uint64 `json:"epoch,omitempty"`
}

const streamClaimConflictText = "stream claim conflict"

// StreamClaimConflictError refuses covered registrations that would register
// rows twice, or rows another writer claimed. Terminal: retrying the same
// registration cannot succeed. Ops are the refused operations of the batch
// (op 0 for a single registration); the claimed batches themselves are read
// from the FSM, not carried here.
type StreamClaimConflictError struct {
	Ops []RefusedOp
}

func (e *StreamClaimConflictError) Error() string {
	ops, _ := json.Marshal(e.Ops)
	return fmt.Sprintf("%s: ops %s", streamClaimConflictText, ops)
}

// IsStreamClaimConflict reports a takeover refusal, from the FSM error itself
// or from its text as a forwarded refusal carries it.
func IsStreamClaimConflict(err error) bool {
	var ce *StreamClaimConflictError
	return errors.As(err, &ce) || (err != nil && strings.Contains(err.Error(), streamClaimConflictText))
}

// StreamClaimConflictOps returns the refused ops of a takeover refusal,
// typed or as text.
func StreamClaimConflictOps(err error) []RefusedOp {
	var ce *StreamClaimConflictError
	if errors.As(err, &ce) {
		return ce.Ops
	}
	if err == nil {
		return nil
	}
	text := err.Error()
	marker := streamClaimConflictText + ": ops "
	i := strings.Index(text, marker)
	if i < 0 {
		return nil
	}
	var ops []RefusedOp
	dec := json.NewDecoder(strings.NewReader(text[i+len(marker):]))
	if dec.Decode(&ops) != nil {
		return nil
	}
	return ops
}

// ---- registration rules ----

// covSet is coverage collected per (instance, key, hour): what earlier
// register ops of one batch added (so two copies of a batch in one Raft entry
// cannot both register), or what a superseded file held.
type covSet map[uint64]map[string]map[int64]*hourIndex

func (s covSet) add(c InstanceCoverage, hour int64) {
	if s[c.Instance] == nil {
		s[c.Instance] = map[string]map[int64]*hourIndex{}
	}
	if s[c.Instance][c.Key] == nil {
		s[c.Instance][c.Key] = map[int64]*hourIndex{}
	}
	h := s[c.Instance][c.Key][hour]
	if h == nil {
		h = &hourIndex{}
		s[c.Instance][c.Key][hour] = h
	}
	h.Whole = h.Whole.union(normalized(c.Ranges))
	h.Partial = append(h.Partial, c.Partial...)
}

func (s covSet) get(inst uint64, key string, hour int64) *hourIndex {
	return s[inst][key][hour]
}

// wholeOf returns the batches of h touched at all: whole ones and those of
// partial pieces.
// wholeBatches returns the batches of [lo, hi] h holds whole.
func (h *hourIndex) wholeBatches(lo, hi uint64) Intervals {
	if h == nil {
		return nil
	}
	return h.Whole.clip(lo, hi)
}

func (h *hourIndex) wholeOf(lo, hi uint64) Intervals {
	if h == nil {
		return nil
	}
	out := h.Whole.clip(lo, hi)
	for _, q := range h.Partial {
		if q.CSeq >= lo && q.CSeq <= hi {
			out = out.addRange(q.CSeq, q.CSeq)
		}
	}
	return out
}

func (h *hourIndex) hasPiece(p CoveragePiece) bool {
	if h == nil {
		return false
	}
	if h.Whole.contains(p.CSeq) {
		return true
	}
	for _, q := range h.Partial {
		if piecesOverlap(p, q) {
			return true
		}
	}
	return false
}

// fileHour is the partition hour a file's coverage is indexed under.
func fileHour(e FileEntry) int64 {
	return e.PartitionTime.UTC().Truncate(time.Hour).Unix()
}

// takeoverRefusalLocked applies the registration rules to one register op,
// in order: claimed, stale, duplicate. scratch is what earlier accepted ops
// of the batch registered; replaced is what the file this one supersedes
// held, when that file is deleted in the same batch (a rewrite moves rows,
// it does not duplicate them). Caller holds f.mu.
func (f *ClusterFSM) takeoverRefusalLocked(p RegisterFilePayload, scratch, replaced covSet) (RefusedOp, bool) {
	if !f.streamClaims || len(p.Cov2) == 0 || p.InheritFrom != "" {
		return RefusedOp{}, false
	}
	// A retry of a registration that already committed is the same file.
	if e, ok := f.files[p.File.Path]; ok && e.SHA256 == p.File.SHA256 && e.SizeBytes == p.File.SizeBytes && e.OriginNodeID == p.File.OriginNodeID {
		return RefusedOp{}, false
	}
	t := f.takeover
	hour := fileHour(p.File)
	for _, c := range p.Cov2 {
		// Claims first: they do not depend on the index, so neither a
		// truncated coverage nor an untracked instance gets past them.
		if k := t.claims[c.Instance]; k.active() && c.ClaimEpoch != k.Epoch {
			if c.Truncated || overlapsSet(c, k.Set[c.Key]) {
				return RefusedOp{Kind: RefusedClaimed, Instance: c.Instance, Epoch: k.Epoch}, true
			}
		}
		if c.ClaimEpoch != 0 {
			// A claimant file is published only where the index can arbitrate
			// it: an untracked instance would take it unchecked (duplicates).
			k := t.claims[c.Instance]
			if in := t.instances[c.Instance]; in == nil || in.Untracked ||
				!k.active() || k.Epoch != c.ClaimEpoch || c.Truncated || !insideSet(c, k.Set[c.Key]) {
				return RefusedOp{Kind: RefusedStale, Instance: c.Instance, Epoch: c.ClaimEpoch}, true
			}
		}
		in := t.instances[c.Instance]
		if in == nil || in.Untracked || c.Truncated {
			continue
		}
		if f.duplicateLocked(c, hour, scratch, replaced) {
			return RefusedOp{Kind: RefusedDuplicate, Instance: c.Instance}, true
		}
		if c.ClaimEpoch != 0 && f.indexWouldOverflowLocked(c, hour, scratch) {
			return RefusedOp{Kind: RefusedFull, Instance: c.Instance, Epoch: c.ClaimEpoch}, true
		}
	}
	return RefusedOp{}, false
}

// duplicateLocked reports whether some batch of c already has rows of this
// hour in a registered file, other than the file being superseded.
func (f *ClusterFSM) duplicateLocked(c InstanceCoverage, hour int64, scratch, replaced covSet) bool {
	ki := f.takeover.index[c.Instance][c.Key]
	rep := replaced.get(c.Instance, c.Key, hour)
	scr := scratch.get(c.Instance, c.Key, hour)
	for _, r := range normalized(c.Ranges) {
		hit := ki.settledWhole(r[0], r[1], hour)
		if rep != nil {
			// Only batches the superseded file held whole: one it held part
			// of may have other rows of this hour in another file.
			hit = hit.minus(rep.wholeBatches(r[0], r[1]))
		}
		if len(hit) > 0 || len(scr.wholeOf(r[0], r[1])) > 0 {
			return true
		}
	}
	for _, p := range c.Partial {
		if (ki.settledPiece(p, hour) && !rep.hasPiece(p)) || scr.hasPiece(p) {
			return true
		}
	}
	return false
}

// takeoverRegisteredLocked records an accepted registration: the file keeps
// its coverage, and the index gains its batches for the file's hour. old is
// the entry the path had before, if any. Caller holds f.mu.
func (f *ClusterFSM) takeoverRegisteredLocked(p RegisterFilePayload, entry *FileEntry, inherited []InstanceCoverage) {
	entry.Coverage = nil
	if !f.streamClaims {
		return
	}
	if p.InheritFrom != "" {
		// A DELETE rewrite: the same rows minus the deleted ones, so the
		// coverage moves with them and the index is unchanged.
		entry.Coverage = inherited
		return
	}
	if len(p.Cov2) == 0 {
		// A re-registration of the same path without coverage (a restore)
		// keeps what the FSM knew of it.
		if old, ok := f.files[entry.Path]; ok {
			entry.Coverage = old.Coverage
		}
		return
	}
	entry.Coverage = append([]InstanceCoverage(nil), p.Cov2...)
	hour := fileHour(*entry)
	for _, c := range p.Cov2 {
		f.indexAddLocked(c, hour)
	}
}

// indexAddLocked adds c's batches for hour. An instance the cluster does not
// know is not indexed: it cannot be claimed either.
func (f *ClusterFSM) indexAddLocked(c InstanceCoverage, hour int64) {
	t := f.takeover
	t.gen++
	in := t.instances[c.Instance]
	if in == nil || in.Untracked {
		return
	}
	if c.Truncated {
		f.untrackLocked(in, "a registration carried a truncated coverage")
		return
	}
	keys := t.index[c.Instance]
	if keys == nil {
		keys = map[string]*keyIndex{}
		t.index[c.Instance] = keys
	}
	ki := keys[c.Key]
	if ki == nil {
		if len(keys) >= maxInstanceKeys {
			f.untrackLocked(in, "too many buffer keys")
			return
		}
		ki = &keyIndex{Hours: map[int64]*hourIndex{}}
		keys[c.Key] = ki
	}
	before := ki.entries()
	h := ki.Hours[hour]
	if h == nil {
		h = &hourIndex{}
		ki.Hours[hour] = h
	}
	var add Intervals
	lost := ki.lostBatches(hour)
	for _, r := range normalized(c.Ranges) {
		if r[1] <= ki.Floor && !ki.Lost.overlaps(r[0], r[1]) {
			// Settled by the floor, except batches with a vanished piece here.
			add = append(add, lost.clip(r[0], r[1])...)
			continue
		}
		add = append(add, r)
	}
	h.Whole = h.Whole.union(normalized(add))
	for _, p := range c.Partial {
		if ki.floorSettled(p.CSeq) && !lost.contains(p.CSeq) {
			continue
		}
		h.Partial = append(h.Partial, p)
	}
	if len(h.Whole) == 0 && len(h.Partial) == 0 {
		delete(ki.Hours, hour)
	}
	ki.pruneLost(hour)
	t.entries += ki.entries() - before
	whole, partial := 0, 0
	for _, hh := range ki.Hours {
		whole += len(hh.Whole)
		partial += len(hh.Partial)
	}
	if whole > maxKeyIntervals || partial > maxKeyPartial {
		f.untrackLocked(in, "the settled index of a buffer key is over its bound")
		return
	}
	if t.entries > maxIndexEntries {
		f.untrackLocked(in, "the settled index is over its cluster-wide bound")
	}
}

// indexWouldOverflowLocked reports whether indexing c at hour, after what
// earlier ops of the batch (scratch) add, would put the instance's index over
// a bound that untracks it. Computed as indexAddLocked would grow it: whole
// intervals merge per hour, pieces at or below the floor are dropped. Only
// the cluster-wide bound counts other keys' scratch without merging.
func (f *ClusterFSM) indexWouldOverflowLocked(c InstanceCoverage, hour int64, scratch covSet) bool {
	t := f.takeover
	keys := t.index[c.Instance]
	ki := keys[c.Key]
	if ki == nil {
		n := len(keys)
		for k := range scratch[c.Instance] {
			if _, ok := keys[k]; !ok && k != c.Key {
				n++
			}
		}
		if n >= maxInstanceKeys {
			return true
		}
		ki = &keyIndex{}
	}
	lost := ki.lostBatches(hour)
	above := func(rs [][2]uint64) Intervals {
		var out Intervals
		for _, r := range normalized(rs) {
			if r[1] <= ki.Floor && !ki.Lost.overlaps(r[0], r[1]) {
				out = append(out, lost.clip(r[0], r[1])...)
				continue
			}
			out = append(out, r)
		}
		return normalized(out)
	}
	partialsAbove := func(ps []CoveragePiece) int {
		n := 0
		for _, p := range ps {
			if !ki.floorSettled(p.CSeq) || lost.contains(p.CSeq) {
				n++
			}
		}
		return n
	}
	hours := map[int64]bool{hour: true}
	for h := range ki.Hours {
		hours[h] = true
	}
	for h := range scratch[c.Instance][c.Key] {
		hours[h] = true
	}
	var whole, partial, before int
	for h := range hours {
		var w Intervals
		if hi := ki.Hours[h]; hi != nil {
			w = hi.Whole
			partial += len(hi.Partial)
			before += len(hi.Whole) + len(hi.Partial)
		}
		if sh := scratch[c.Instance][c.Key][h]; sh != nil {
			w = w.union(above(sh.Whole))
			partial += partialsAbove(sh.Partial)
		}
		if h == hour {
			w = w.union(above(c.Ranges))
			partial += partialsAbove(c.Partial)
		}
		whole += len(w)
	}
	if whole > maxKeyIntervals || partial > maxKeyPartial {
		return true
	}
	pending := 0
	for inst, byKey := range scratch {
		for k, byHour := range byKey {
			if inst == c.Instance && k == c.Key {
				continue
			}
			for _, h := range byHour {
				pending += len(h.Whole) + len(h.Partial)
			}
		}
	}
	return t.entries+pending+whole+partial-before > maxIndexEntries
}

// takeoverVanishedLocked takes back the batches of a file that vanished
// before any node pulled it: above the floor from the hour's index, at or
// below it as vanished pieces of that hour. An unfinished claim on the
// instance grows to take them, so the claimant republishes them and the
// origin's copies are refused. Caller holds f.mu.
func (f *ClusterFSM) takeoverVanishedLocked(c InstanceCoverage, hour int64, logIndex uint64) {
	f.indexSubtractLocked(c, hour)
	t := f.takeover
	in := t.instances[c.Instance]
	ki := t.index[c.Instance][c.Key]
	if in == nil || in.Untracked || ki == nil || c.Truncated {
		return
	}
	var whole Intervals
	for _, r := range normalized(c.Ranges) {
		if r[0] > ki.Floor {
			continue
		}
		whole = whole.union(Intervals{{r[0], minU64(r[1], ki.Floor)}}.minus(ki.Lost))
	}
	var partial []CoveragePiece
	for _, p := range c.Partial {
		if ki.floorSettled(p.CSeq) {
			partial = append(partial, p)
		}
	}
	if len(whole) == 0 && len(partial) == 0 {
		return
	}
	before := ki.entries()
	if ki.LostHours == nil {
		ki.LostHours = map[int64]*hourIndex{}
	}
	lh := ki.LostHours[hour]
	if lh == nil {
		lh = &hourIndex{}
		ki.LostHours[hour] = lh
	}
	lh.Whole = lh.Whole.union(whole)
	lh.Partial = append(lh.Partial, partial...)
	t.entries += ki.entries() - before
	t.gen++
	n := 0
	for _, h := range ki.LostHours {
		n += len(h.Whole) + len(h.Partial)
	}
	if n > maxKeyLost || t.entries > maxIndexEntries {
		f.untrackLocked(in, "the vanished pieces of a buffer key are over their bound")
		return
	}
	if k := t.claims[c.Instance]; k.active() && !k.Done {
		set := k.Set[c.Key].union(whole)
		for _, p := range partial {
			set = set.addRange(p.CSeq, p.CSeq)
		}
		if len(set) > maxClaimIntervalsPerKey {
			f.untrackLocked(in, "a claim grew past its bound with vanished batches")
			return
		}
		k.Set[c.Key] = set
		k.ExtendedAt = logIndex
	}
}

// ClaimableSet returns the batches of ivs a claim of (inst, key) would take:
// all but those settled by the floor in every hour.
func (f *ClusterFSM) ClaimableSet(inst uint64, key string, ivs Intervals) Intervals {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := normalized(ivs)
	if ki := f.takeover.index[inst][key]; ki != nil {
		s = s.minus(ki.floorSettledSet())
	}
	return s
}

// indexSubtractLocked removes c's batches for hour: a file that vanished
// before any node could pull it holds nothing.
func (f *ClusterFSM) indexSubtractLocked(c InstanceCoverage, hour int64) {
	t := f.takeover
	t.gen++
	ki := t.index[c.Instance][c.Key]
	if ki == nil {
		return
	}
	h := ki.Hours[hour]
	if h == nil {
		return
	}
	before := ki.entries()
	for _, r := range c.Ranges {
		h.Whole = h.Whole.removeRange(r[0], r[1])
	}
	if len(c.Partial) > 0 {
		kept := h.Partial[:0]
		for _, q := range h.Partial {
			drop := false
			for _, p := range c.Partial {
				if p == q {
					drop = true
					break
				}
			}
			if !drop {
				kept = append(kept, q)
			}
		}
		h.Partial = kept
	}
	if len(h.Whole) == 0 && len(h.Partial) == 0 {
		delete(ki.Hours, hour)
	}
	t.entries += ki.entries() - before
}

// untrackLocked stops indexing an instance and frees its index. Its
// registrations are accepted unchecked from now on and it cannot be claimed.
func (f *ClusterFSM) untrackLocked(in *StreamInstance, why string) {
	f.takeover.gen++
	if in.Untracked {
		return
	}
	in.Untracked = true
	f.dropIndexLocked(in.Instance)
	f.logger.Error().Str("instance", instKey(in.Instance)).Str("origin", in.Origin).Str("reason", why).
		Msg("Stream takeover stopped tracking a writer instance; a failover from it cannot take over its unflushed batches")
}

func (f *ClusterFSM) dropIndexLocked(inst uint64) {
	t := f.takeover
	for _, k := range t.index[inst] {
		t.entries -= k.entries()
	}
	delete(t.index, inst)
}

func (f *ClusterFSM) removeInstanceLocked(inst uint64) {
	t := f.takeover
	t.gen++
	f.dropIndexLocked(inst)
	if in := t.instances[inst]; in != nil && t.current[in.Origin] == inst {
		delete(t.current, in.Origin)
	}
	delete(t.instances, inst)
	delete(t.claims, inst)
}

// ---- commands ----

// Continuity is the last instance recorded on each volume a writer keeps
// its unflushed batches on. All three equal to the origin's current instance
// means the same disks: anything not in Holds is registered.
type Continuity struct {
	WAL     uint64 `json:"wal"`
	Journal uint64 `json:"journal"`
	Storage uint64 `json:"storage"`
}

// AnnounceInstancePayload is a writer starting a new process.
type AnnounceInstancePayload struct {
	Origin     string     `json:"origin"`
	Instance   uint64     `json:"instance"`
	Continuity Continuity `json:"continuity"`
	// Holds are previous instances of this origin with batches still
	// pending, parked or in its WAL.
	Holds []uint64 `json:"holds,omitempty"`
	// FreshStart is false when the instance registered files before this
	// announcement (takeover was enabled mid-run): it is never trusted.
	FreshStart bool `json:"fresh_start"`
}

func (f *ClusterFSM) applyAnnounceInstance(payload []byte, logIndex uint64) interface{} {
	var p AnnounceInstancePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal announce instance payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.takeover
	switch {
	case !f.streamClaims:
		return errors.New("announce instance: stream takeover is not enabled")
	case p.Instance == 0 || p.Origin == "" || len(p.Holds) > maxAnnounceHolds:
		return errors.New("announce instance: origin and instance are required, holds bounded")
	}
	if n, ok := f.nodes[p.Origin]; !ok || n.Role != "writer" {
		return fmt.Errorf("announce instance: %s is not a writer in the cluster", p.Origin)
	}
	if in := t.instances[p.Instance]; in != nil {
		if in.Origin == p.Origin && in.AnnouncedAt != 0 {
			return nil // a retry of an announcement that committed
		}
		return fmt.Errorf("announce instance: %s is already known", instKey(p.Instance))
	}
	holds := map[uint64]bool{}
	for _, h := range p.Holds {
		holds[h] = true
	}
	prev, hadPrev := t.current[p.Origin]
	continuity := hadPrev && p.Continuity.WAL == prev && p.Continuity.Journal == prev && p.Continuity.Storage == prev
	for _, id := range sortedInstanceIDs(t.instances) {
		in := t.instances[id]
		if in.Origin != p.Origin {
			continue
		}
		in.OriginGone = false
		switch {
		case holds[id]:
			// Held: adopted by the new process. One that is claimed stays
			// claimed: the origin's copies of its claimed batches are
			// refused. Either way it is no longer current: an origin has
			// one current instance.
			in.Status, in.AdoptedBy = InstanceAdopted, p.Instance
		case in.Status == InstanceAdopted || in.Status == InstanceCurrent:
			if continuity {
				in.Status = InstanceClosed
			} else {
				in.Status = InstanceOrphaned
			}
			in.AdoptedBy = 0
			if c := t.claims[id]; c != nil && c.Done && !c.Released {
				// Published by its claimant, and its origin holds nothing of
				// it: nothing will ever register it again (as on ClaimDone).
				f.removeInstanceLocked(id)
			}
		}
	}
	delete(t.current, p.Origin)
	if len(t.instances) >= maxTakeoverInstances {
		// Not tracked: its registrations go unindexed, and it cannot be
		// claimed.
		f.logger.Error().Str("origin", p.Origin).Str("instance", instKey(p.Instance)).
			Msg("Stream takeover instance table is full; a failover from this writer instance cannot take over its unflushed batches")
		return nil
	}
	t.instances[p.Instance] = &StreamInstance{Instance: p.Instance, Origin: p.Origin, AnnouncedAt: logIndex, Status: InstanceCurrent, Untracked: !p.FreshStart}
	t.current[p.Origin] = p.Instance
	if !p.FreshStart {
		f.logger.Error().Str("origin", p.Origin).Str("instance", instKey(p.Instance)).
			Msg("Writer instance registered files before stream takeover was enabled; its batches can be taken over only after its next restart")
	}
	return nil
}

// KeySettlement is one key's progress for SettleCoverage: every batch at or
// below Floor is accounted for, and those in Lost settled without a file.
type KeySettlement struct {
	Floor uint64    `json:"f"`
	Lost  Intervals `json:"l,omitempty"`
}

// SettleCoveragePayload raises the floors of the origin's current instance.
type SettleCoveragePayload struct {
	Origin   string                   `json:"origin"`
	Instance uint64                   `json:"instance"`
	Keys     map[string]KeySettlement `json:"keys"`
}

func (f *ClusterFSM) applySettleCoverage(payload []byte) interface{} {
	if len(payload) > MaxTakeoverPayloadSize {
		return fmt.Errorf("settle coverage: payload of %d bytes exceeds %d", len(payload), MaxTakeoverPayloadSize)
	}
	var p SettleCoveragePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal settle coverage payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.takeover
	if !f.streamClaims {
		return errors.New("settle coverage: stream takeover is not enabled")
	}
	in := t.instances[p.Instance]
	if in == nil || in.Origin != p.Origin || t.current[p.Origin] != p.Instance {
		return fmt.Errorf("settle coverage: %s is not the current instance of %s", instKey(p.Instance), p.Origin)
	}
	if in.Untracked {
		return nil
	}
	keys := make([]string, 0, len(p.Keys))
	for k := range p.Keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		s := p.Keys[key]
		if len(key) == 0 || len(key) > maxTakeoverKeyLen {
			continue
		}
		idx := t.index[p.Instance]
		if idx == nil {
			idx = map[string]*keyIndex{}
			t.index[p.Instance] = idx
		}
		ki := idx[key]
		if ki == nil {
			if len(idx) >= maxInstanceKeys {
				f.untrackLocked(in, "too many buffer keys")
				return nil
			}
			ki = &keyIndex{Hours: map[int64]*hourIndex{}}
			idx[key] = ki
		}
		if s.Floor <= ki.Floor {
			continue
		}
		before := ki.entries()
		for _, iv := range s.Lost {
			if iv[1] > ki.Floor && iv[0] <= s.Floor {
				lo := iv[0]
				if lo <= ki.Floor {
					lo = ki.Floor + 1
				}
				hi := iv[1]
				if hi > s.Floor {
					hi = s.Floor
				}
				ki.Lost = ki.Lost.addRange(lo, hi)
			}
		}
		ki.Floor = s.Floor
		for hr, h := range ki.Hours {
			var whole Intervals
			lostHere := ki.lostBatches(hr)
			for _, iv := range h.Whole {
				if iv[1] > ki.Floor {
					whole = whole.addRange(maxU64(iv[0], ki.Floor+1), iv[1])
				}
				// At or below the floor: kept only where lost (in every hour,
				// or a vanished piece of this one).
				for _, l := range ki.Lost.union(lostHere).clip(iv[0], minU64(iv[1], ki.Floor)) {
					whole = whole.addRange(l[0], l[1])
				}
			}
			h.Whole = whole
			kept := h.Partial[:0]
			for _, q := range h.Partial {
				if q.CSeq > ki.Floor || ki.Lost.contains(q.CSeq) || lostHere.contains(q.CSeq) {
					kept = append(kept, q)
				}
			}
			h.Partial = kept
			if len(h.Whole) == 0 && len(h.Partial) == 0 {
				delete(ki.Hours, hr)
			}
		}
		t.entries += ki.entries() - before
		if len(ki.Lost) > maxKeyLost || t.entries > maxIndexEntries {
			f.untrackLocked(in, "the settled index is over its bound")
			return nil
		}
	}
	return nil
}

// ClaimSet is the batches of one instance a takeover claims, per key.
type ClaimSet struct {
	Instance uint64               `json:"instance"`
	Keys     map[string]Intervals `json:"keys,omitempty"`
}

// TakeOverStreamV2Payload claims batches of one origin's instances. An
// instance with no keys reports that the proposer holds nothing of it.
type TakeOverStreamV2Payload struct {
	Origin string     `json:"origin"`
	By     string     `json:"by"`
	Claims []ClaimSet `json:"claims"`
	Now    time.Time  `json:"now"`
}

func (f *ClusterFSM) applyTakeOverStreamV2(payload []byte, logIndex uint64) interface{} {
	if len(payload) > MaxTakeoverPayloadSize {
		return fmt.Errorf("take over stream: payload of %d bytes exceeds %d", len(payload), MaxTakeoverPayloadSize)
	}
	var p TakeOverStreamV2Payload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal take over stream payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.takeover
	switch {
	case !f.streamClaims:
		return errors.New("take over stream: stream takeover is not enabled")
	case p.Origin == "" || p.By == "" || p.Now.IsZero() || len(p.Claims) == 0:
		return errors.New("take over stream: origin, by, now and claims are required")
	case p.By == p.Origin:
		return errors.New("take over stream: a node cannot claim its own instance")
	}
	by, ok := f.nodes[p.By]
	if !ok || by.Role != "writer" || by.State != "healthy" {
		return fmt.Errorf("take over stream: %s is not a healthy writer in the cluster", p.By)
	}
	inCmd := map[uint64]bool{}
	for _, c := range p.Claims {
		if inCmd[c.Instance] {
			return fmt.Errorf("take over stream: %s is listed twice", instKey(c.Instance))
		}
		inCmd[c.Instance] = true
	}
	// Validate every instance before changing anything.
	newClaims := 0
	for _, c := range p.Claims {
		in := t.instances[c.Instance]
		switch {
		case in == nil || in.Origin != p.Origin:
			return fmt.Errorf("take over stream: %s is not a known instance of %s", instKey(c.Instance), p.Origin)
		case in.Untracked:
			return fmt.Errorf("take over stream: %s is not tracked", instKey(c.Instance))
		case t.claims[c.Instance].active():
			return fmt.Errorf("take over stream: %s is already claimed", instKey(c.Instance))
		case len(c.Keys) > maxInstanceKeys:
			return fmt.Errorf("take over stream: %d keys for one instance", len(c.Keys))
		}
		switch in.Status {
		case InstanceCurrent:
			if f.primaryWriterID != p.By {
				return fmt.Errorf("take over stream: only the designated primary takes over %s", instKey(c.Instance))
			}
		case InstanceAdopted:
			if f.primaryWriterID != p.By {
				return fmt.Errorf("take over stream: only the designated primary takes over %s", instKey(c.Instance))
			}
			if !inCmd[in.AdoptedBy] && !t.claims[in.AdoptedBy].active() {
				return fmt.Errorf("take over stream: %s is held by the live instance %s", instKey(c.Instance), instKey(in.AdoptedBy))
			}
		}
		for k, v := range c.Keys {
			if len(k) == 0 || len(k) > maxTakeoverKeyLen || len(v) > maxClaimIntervalsPerKey {
				return errors.New("take over stream: a claimed key is malformed or over its bound")
			}
		}
		if len(c.Keys) > 0 {
			newClaims++
		}
	}
	if len(t.claims)+newClaims > maxTakeoverClaims {
		return fmt.Errorf("take over stream: %d claims already held", len(t.claims))
	}
	for _, c := range p.Claims {
		in := t.instances[c.Instance]
		set := map[string]Intervals{}
		for k, v := range c.Keys {
			s := normalized(v)
			// Batches settled in every hour (at or below the floor, not
			// lost) are not claimable. Hours settled above the floor are
			// filtered by the claimant when it replays.
			if ki := t.index[c.Instance][k]; ki != nil {
				s = s.minus(ki.floorSettledSet())
			}
			if len(s) > 0 {
				set[k] = s
			}
		}
		if len(set) == 0 {
			// Only an instance listed with no keys at all reports holding
			// nothing; a set the floors trimmed to nothing claims nothing and
			// reports nothing (later keys may still be claimed).
			if in != nil && len(c.Keys) == 0 {
				f.reportedEmptyLocked(in, p.By)
			}
			continue
		}
		t.claims[c.Instance] = &StreamClaim{Instance: c.Instance, Origin: p.Origin, By: p.By, Epoch: logIndex, Set: set, At: p.Now}
		f.logger.Info().Str("origin", p.Origin).Str("by", p.By).Str("instance", instKey(c.Instance)).
			Uint64("epoch", logIndex).Int("keys", len(set)).
			Msg("Stream claim applied: the claimant now owns the claimed batches of this instance")
	}
	return nil
}

// reportedEmptyLocked records that a writer holds nothing to claim of a
// closed or orphaned instance; once every other writer has said so, nobody
// can ever publish anything of it and it is removed.
func (f *ClusterFSM) reportedEmptyLocked(in *StreamInstance, by string) {
	if in.Status != InstanceClosed && in.Status != InstanceOrphaned {
		return
	}
	for _, r := range in.ReportedBy {
		if r == by {
			return
		}
	}
	if len(in.ReportedBy) < maxTakeoverReportedWriters {
		in.ReportedBy = append(in.ReportedBy, by)
	}
	f.removeIfAllReportedLocked(in)
}

// removeIfAllReportedLocked removes a closed or orphaned instance once every
// writer other than its origin has reported holding nothing of it.
func (f *ClusterFSM) removeIfAllReportedLocked(in *StreamInstance) {
	if len(in.ReportedBy) == 0 || (in.Status != InstanceClosed && in.Status != InstanceOrphaned) {
		return
	}
	if c := f.takeover.claims[in.Instance]; c.active() && !c.Done {
		return // another writer is still publishing it
	}
	for id, n := range f.nodes {
		if n.Role != "writer" || id == in.Origin {
			continue
		}
		found := false
		for _, r := range in.ReportedBy {
			if r == id {
				found = true
				break
			}
		}
		if !found {
			return
		}
	}
	// A writer that left gracefully may hold the only copy: it must report
	// too (it can once it re-joins; its departure expires otherwise).
	for id := range f.takeover.departed {
		if id == in.Origin {
			continue
		}
		found := false
		for _, r := range in.ReportedBy {
			if r == id {
				found = true
				break
			}
		}
		if !found {
			return
		}
	}
	f.removeInstanceLocked(in.Instance)
}

// ClaimDonePayload ends a claimant's replay. Unreplayable are claimed
// batches it could not publish: they stop being claimed, so the origin's own
// copy can still register.
type ClaimDonePayload struct {
	Instance     uint64               `json:"instance"`
	By           string               `json:"by"`
	Epoch        uint64               `json:"epoch"`
	Unreplayable map[string]Intervals `json:"unreplayable,omitempty"`
}

func (f *ClusterFSM) applyClaimDone(payload []byte) interface{} {
	if len(payload) > MaxTakeoverPayloadSize {
		return fmt.Errorf("claim done: payload of %d bytes exceeds %d", len(payload), MaxTakeoverPayloadSize)
	}
	var p ClaimDonePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal claim done payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.takeover
	c := t.claims[p.Instance]
	if !f.streamClaims || !c.active() || c.By != p.By || c.Epoch != p.Epoch {
		return fmt.Errorf("claim done: no active claim %d of %s by %s", p.Epoch, instKey(p.Instance), p.By)
	}
	if c.Done {
		return nil
	}
	c.Done = true
	if len(p.Unreplayable) > 0 {
		c.Unreplayable = map[string]Intervals{}
		n := 0
		for k, v := range p.Unreplayable {
			for _, iv := range v {
				c.Set[k] = c.Set[k].removeRange(iv[0], iv[1])
				c.Unreplayable[k] = c.Unreplayable[k].addRange(iv[0], iv[1])
				n++
			}
			if len(c.Set[k]) == 0 {
				delete(c.Set, k)
			}
		}
		f.logger.Error().Str("instance", instKey(p.Instance)).Str("by", p.By).Int("intervals", n).
			Msg("Stream claim finished with batches the claimant could not publish; they are left to their origin")
	}
	if in := t.instances[p.Instance]; in != nil && (in.OriginGone || in.Status == InstanceClosed || in.Status == InstanceOrphaned) {
		// Nothing will ever register this instance again.
		f.removeInstanceLocked(p.Instance)
	}
	return nil
}

// ReleaseStreamClaimPayload releases an unfinished claim whose claimant is
// gone for good (operator), so another standby can claim the instance.
type ReleaseStreamClaimPayload struct {
	Instance uint64 `json:"instance"`
	Epoch    uint64 `json:"epoch"`
	// By is set when the claimant releases its own claim (its store copy is
	// gone); unset is an operator release proposed on the leader.
	By string `json:"by,omitempty"`
}

func (f *ClusterFSM) applyReleaseStreamClaim(payload []byte) interface{} {
	var p ReleaseStreamClaimPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal release stream claim payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.takeover.claims[p.Instance]
	if !f.streamClaims || !c.active() || c.Epoch != p.Epoch || (p.By != "" && p.By != c.By) {
		return fmt.Errorf("release stream claim: no active claim %d of %s", p.Epoch, instKey(p.Instance))
	}
	if c.Done {
		return fmt.Errorf("release stream claim: claim %d of %s is finished", p.Epoch, instKey(p.Instance))
	}
	c.Released = true
	f.logger.Warn().Str("instance", instKey(p.Instance)).Str("by", c.By).Msg("Stream claim released; another writer may take the instance over")
	return nil
}

// ExtendStreamClaimPayload adds keys to an unfinished claim: one
// TakeOverStreamV2 is bounded by MaxTakeoverPayloadSize, so a claimant
// opens its claim with the first keys and adds the rest.
type ExtendStreamClaimPayload struct {
	Instance uint64               `json:"instance"`
	By       string               `json:"by"`
	Epoch    uint64               `json:"epoch"`
	Keys     map[string]Intervals `json:"keys"`
}

func (f *ClusterFSM) applyExtendStreamClaim(payload []byte, logIndex uint64) interface{} {
	if len(payload) > MaxTakeoverPayloadSize {
		return fmt.Errorf("extend stream claim: payload of %d bytes exceeds %d", len(payload), MaxTakeoverPayloadSize)
	}
	var p ExtendStreamClaimPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal extend stream claim payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.takeover
	c := t.claims[p.Instance]
	if !f.streamClaims || !c.active() || c.Done || c.By != p.By || c.Epoch != p.Epoch {
		return fmt.Errorf("extend stream claim: no unfinished claim %d of %s by %s", p.Epoch, instKey(p.Instance), p.By)
	}
	if in := t.instances[p.Instance]; in == nil || in.Untracked {
		return fmt.Errorf("extend stream claim: %s is not tracked", instKey(p.Instance))
	}
	add := map[string]Intervals{}
	for k, v := range p.Keys {
		if len(k) == 0 || len(k) > maxTakeoverKeyLen || len(v) > maxClaimIntervalsPerKey {
			return errors.New("extend stream claim: a claimed key is malformed or over its bound")
		}
		s := normalized(v)
		if ki := t.index[p.Instance][k]; ki != nil {
			s = s.minus(ki.floorSettledSet())
		}
		if len(s) == 0 {
			continue
		}
		if u := c.Set[k].union(s); len(u) > maxClaimIntervalsPerKey {
			return fmt.Errorf("extend stream claim: key %q over %d intervals", k, maxClaimIntervalsPerKey)
		}
		add[k] = s
	}
	keys := len(c.Set)
	for k := range add {
		if _, ok := c.Set[k]; !ok {
			keys++
		}
	}
	if keys > maxInstanceKeys {
		return fmt.Errorf("extend stream claim: %d keys for one instance", keys)
	}
	for k, s := range add {
		c.Set[k] = c.Set[k].union(s)
	}
	c.ExtendedAt = logIndex
	return nil
}

// InstanceSettledPayload is an origin reporting that nothing of one of its
// previous instances is pending, parked or in its WAL any more.
type InstanceSettledPayload struct {
	Origin   string `json:"origin"`
	Instance uint64 `json:"instance"`
}

func (f *ClusterFSM) applyInstanceSettled(payload []byte) interface{} {
	var p InstanceSettledPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal instance settled payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.takeover
	in := t.instances[p.Instance]
	switch {
	case !f.streamClaims:
		return errors.New("instance settled: stream takeover is not enabled")
	case in == nil:
		return nil // already removed
	case in.Origin != p.Origin || t.current[p.Origin] == p.Instance:
		return fmt.Errorf("instance settled: %s is not a previous instance of %s", instKey(p.Instance), p.Origin)
	case in.Status != InstanceAdopted:
		// Closed and orphaned instances may still be held by a standby
		// only: they go when claimed to the end, or reported empty.
		return fmt.Errorf("instance settled: %s is not held by its origin", instKey(p.Instance))
	}
	if c := t.claims[p.Instance]; c.active() && !c.Done {
		return fmt.Errorf("instance settled: %s has an unfinished claim", instKey(p.Instance))
	}
	f.removeInstanceLocked(p.Instance)
	return nil
}

// takeoverNodeRemovedLocked: a claimant removed from the cluster releases
// its unfinished claims; a removed origin's instances become orphaned (it
// will never register them), and keep their claims. Caller holds f.mu.
func (f *ClusterFSM) takeoverNodeRemovedLocked(id string) {
	t := f.takeover
	t.gen++
	delete(t.departed, id) // before the report sweep below: it no longer waits for this node
	for _, inst := range sortedInstanceIDs(t.instances) {
		in := t.instances[inst]
		if in.Origin == id {
			in.OriginGone = true
			in.Status, in.AdoptedBy = InstanceOrphaned, 0
			if c := t.claims[inst]; c != nil && c.Done {
				f.removeInstanceLocked(inst)
			}
		}
	}
	delete(t.current, id)
	// A removed writer can no longer report: instances it was the last to
	// hear from may now be complete.
	for _, inst := range sortedInstanceIDs(t.instances) {
		if in := t.instances[inst]; in != nil {
			f.removeIfAllReportedLocked(in)
		}
	}
	for _, c := range t.claims {
		if c.By == id && !c.Done && !c.Released {
			c.Released = true
			f.logger.Warn().Str("instance", instKey(c.Instance)).Str("by", id).
				Msg("Stream claim released: its claimant was removed from the cluster")
		}
	}
}

// maxDepartedWriters bounds the departures kept; past it a leave is applied
// as an operator removal (instances orphaned, claims released).
const maxDepartedWriters = 256

// takeoverNodeDepartedLocked records a graceful leave of a writer: its
// instances and claims stay as they are (a restart re-joins and adopts its
// instances first), so it is not treated as gone. Caller holds f.mu.
func (f *ClusterFSM) takeoverNodeDepartedLocked(id string, at time.Time) {
	t := f.takeover
	if _, ok := t.departed[id]; !ok && len(t.departed) >= maxDepartedWriters {
		// Full: the departure most likely never to return goes, the oldest
		// (ties by id, so every node picks the same).
		oldest := ""
		for d, at := range t.departed {
			if oldest == "" || at.Before(t.departed[oldest]) || (at.Equal(t.departed[oldest]) && d < oldest) {
				oldest = d
			}
		}
		f.logger.Error().Str("node_id", oldest).Int("departed", len(t.departed)).
			Msg("Too many writers have left the cluster without coming back; the oldest departure is removed from stream takeover as an operator removal would")
		f.takeoverNodeRemovedLocked(oldest)
	}
	t.gen++
	t.departed[id] = at
}

// takeoverNodeBackLocked forgets a departure: the node re-joined. Back in
// another role than writer it will never announce again: its instances are
// given up as on an operator removal.
func (f *ClusterFSM) takeoverNodeBackLocked(id, role string) {
	t := f.takeover
	if t == nil {
		return
	}
	if _, ok := t.departed[id]; ok {
		delete(t.departed, id)
		t.gen++
	}
	if role == "writer" || !f.streamClaims {
		return
	}
	for _, in := range t.instances {
		if in.Origin == id && !in.OriginGone {
			f.takeoverNodeRemovedLocked(id)
			return
		}
	}
}

// CurrentInstanceClaimed reports whether origin's current instance has an
// active claim, finished or not: that process exits for a restart (its next
// instance is not claimed).
func (f *ClusterFSM) CurrentInstanceClaimed(origin string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	inst, ok := f.takeover.current[origin]
	if !ok {
		return false
	}
	return f.takeover.claims[inst].active()
}

// DepartedWriters returns the writers that left gracefully and have not
// re-joined, with their departure time.
func (f *ClusterFSM) DepartedWriters() map[string]time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]time.Time, len(f.takeover.departed))
	for id, at := range f.takeover.departed {
		out[id] = at
	}
	return out
}

// ---- read access ----

// StreamInstances returns the instances the cluster knows, ordered.
func (f *ClusterFSM) StreamInstances() []StreamInstance {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []StreamInstance
	for _, id := range sortedInstanceIDs(f.takeover.instances) {
		out = append(out, *f.takeover.instances[id])
	}
	return out
}

// StreamClaims returns a copy of the claims, ordered by instance.
func (f *ClusterFSM) StreamClaims() []StreamClaim {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ids := make([]uint64, 0, len(f.takeover.claims))
	for id := range f.takeover.claims {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]StreamClaim, 0, len(ids))
	for _, id := range ids {
		out = append(out, f.takeover.claims[id].clone())
	}
	return out
}

// InstanceState is the part of an instance and its claim a standby store's
// eviction needs; read cheaply, without cloning claim sets.
type InstanceState struct {
	Origin       string
	Status       string
	Untracked    bool
	Claimed      bool // a claim exists (any state)
	ClaimDone    bool
	ClaimRelease bool
}

// TakeoverInstanceStates returns the state of every known instance.
func (f *ClusterFSM) TakeoverInstanceStates() map[uint64]InstanceState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[uint64]InstanceState, len(f.takeover.instances))
	for id, in := range f.takeover.instances {
		st := InstanceState{Origin: in.Origin, Status: in.Status, Untracked: in.Untracked}
		if c := f.takeover.claims[id]; c != nil {
			st.Claimed, st.ClaimDone, st.ClaimRelease = true, c.Done, c.Released
		}
		out[id] = st
	}
	return out
}

// ClaimsBy returns the claims node holds as claimant, released ones
// included (the claimant stops working on them).
func (f *ClusterFSM) ClaimsBy(node string) []StreamClaim {
	var out []StreamClaim
	for _, c := range f.StreamClaims() {
		if c.By == node {
			out = append(out, c)
		}
	}
	return out
}

// CoveredState is what the index says about the rows of one stored batch
// record in one partition hour.
type CoveredState int

const (
	// CoveredUntracked: the instance is unknown or untracked; nothing can be
	// arbitrated, so nothing of it may be published by a claimant.
	CoveredUntracked CoveredState = iota
	// CoveredAll: every row of the record in this hour is in a registered file.
	CoveredAll
	// CoveredRanges: the rows of the record registered in this hour are the
	// returned row ranges (none: nothing is).
	CoveredRanges
)

// CoveredRows reports which rows of one stored record (cseq, chunk of chunks;
// chunks <= 1 is an unsplit batch) are in registered files of hour, in the
// record's row coordinates. total is the row count the ranges were
// registered against; an error is a registration in other coordinates than
// the record (it cannot be subtracted row by row).
func (f *ClusterFSM) CoveredRows(inst uint64, key string, cseq uint64, chunk, chunks uint16, hour time.Time) (CoveredState, [][2]int, int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t := f.takeover
	if in := t.instances[inst]; in == nil || in.Untracked {
		return CoveredUntracked, nil, 0, nil
	}
	ki := t.index[inst][key]
	if ki == nil {
		return CoveredRanges, nil, 0, nil
	}
	hr := hour.UTC().Truncate(time.Hour).Unix()
	h := ki.Hours[hr]
	if h != nil && h.Whole.contains(cseq) {
		return CoveredAll, nil, 0, nil
	}
	chunked := chunks > 1
	if !chunked {
		chunk = 0
	}
	var ranges [][2]int
	total := -1
	if ki.floorSettled(cseq) {
		// Every row is in a file, except the rows of vanished pieces here.
		whole, lost := ki.lostPieces(cseq, hr)
		if !whole {
			var gone [][2]int
			for _, q := range lost {
				if (q.Chunks > 0) != chunked {
					return CoveredRanges, nil, 0, fmt.Errorf("batch %d of %s vanished in other row coordinates", cseq, key)
				}
				if chunked && q.Chunk != chunk {
					continue
				}
				if q.RowTotal == 0 {
					whole = true // the whole chunk
					break
				}
				if total >= 0 && q.RowTotal != total {
					return CoveredRanges, nil, 0, fmt.Errorf("batch %d of %s vanished with differing row totals", cseq, key)
				}
				total = q.RowTotal
				gone = append(gone, [2]int{q.RowLo, q.RowHi})
			}
			if !whole {
				if len(gone) == 0 {
					return CoveredAll, nil, 0, nil
				}
				sort.Slice(gone, func(i, j int) bool { return gone[i][0] < gone[j][0] })
				ranges = complementRanges(gone, total)
			}
		}
	}
	if h == nil {
		if total < 0 {
			return CoveredRanges, nil, 0, nil
		}
		return CoveredRanges, ranges, total, nil
	}
	for _, q := range h.Partial {
		if q.CSeq != cseq {
			continue
		}
		if (q.Chunks > 0) != chunked {
			return CoveredRanges, nil, 0, fmt.Errorf("batch %d of %s is registered in other row coordinates", cseq, key)
		}
		if chunked && q.Chunk != chunk {
			continue
		}
		if q.RowTotal == 0 {
			return CoveredAll, nil, 0, nil // the whole chunk
		}
		if total >= 0 && q.RowTotal != total {
			return CoveredRanges, nil, 0, fmt.Errorf("batch %d of %s is registered with differing row totals", cseq, key)
		}
		total = q.RowTotal
		ranges = append(ranges, [2]int{q.RowLo, q.RowHi})
	}
	if total < 0 {
		return CoveredRanges, nil, 0, nil
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
	return CoveredRanges, ranges, total, nil
}

// StreamClaimCount reports how many claims are held.
func (f *ClusterFSM) StreamClaimCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.takeover.claims)
}

// CoverageSettled reports whether the rows of piece p of (inst, key) for
// partition hour are in a registered file.
func (f *ClusterFSM) CoverageSettled(inst uint64, key string, p CoveragePiece, hour time.Time) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.takeover.index[inst][key].settledPiece(p, hour.UTC().Truncate(time.Hour).Unix())
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// DeleteReasonVanished is the delete reason of a file that was gone from its
// origin's storage when its registration committed.
const DeleteReasonVanished = "vanished-before-registration"

// inheritedCoverageLocked returns the coverage a DELETE rewrite takes over
// from the file it rewrites. Caller holds f.mu.
func (f *ClusterFSM) inheritedCoverageLocked(p RegisterFilePayload) []InstanceCoverage {
	if p.InheritFrom == "" {
		return nil
	}
	// Only from a file of the same measurement and partition hour, and only
	// for a registration that brings no coverage of its own.
	if old, ok := f.files[p.InheritFrom]; ok && len(p.Cov2) == 0 && old.Database == p.File.Database &&
		old.Measurement == p.File.Measurement && fileHour(*old) == fileHour(p.File) {
		return append([]InstanceCoverage(nil), old.Coverage...)
	}
	return nil
}

// batchTakeoverRefusalsLocked applies the registration rules to every
// register op of a batch, against the manifest before the batch, the
// coverage earlier accepted ops of the batch register, and, per op, the
// coverage of the file it supersedes when that file is deleted anywhere in
// the batch. It fills inherited for DELETE rewrites. Caller holds f.mu
// (read).
func (f *ClusterFSM) batchTakeoverRefusalsLocked(ops []BatchFileOp, decoded []any, inherited [][]InstanceCoverage) []RefusedOp {
	deleted := map[string]bool{}
	for i, op := range ops {
		if op.Type == CommandDeleteFile {
			deleted[decoded[i].(DeleteFilePayload).Path] = true
		}
	}
	scratch := covSet{}
	var refused []RefusedOp
	for i, op := range ops {
		if op.Type != CommandRegisterFile {
			continue
		}
		rp := decoded[i].(RegisterFilePayload)
		inherited[i] = f.inheritedCoverageLocked(rp)
		var replaced covSet
		if rp.Supersedes != "" && deleted[rp.Supersedes] {
			if old, ok := f.files[rp.Supersedes]; ok && len(old.Coverage) > 0 {
				replaced = covSet{}
				for _, c := range old.Coverage {
					replaced.add(c, fileHour(*old))
				}
			}
		}
		if r, ok := f.takeoverRefusalLocked(rp, scratch, replaced); ok {
			r.Op = i
			if len(refused) < maxRefusedOpsReported {
				refused = append(refused, r)
			}
			continue
		}
		if f.streamClaims && rp.InheritFrom == "" {
			hour := fileHour(rp.File)
			for _, c := range rp.Cov2 {
				scratch.add(c, hour)
			}
		}
	}
	return refused
}

func overlapsSet(c InstanceCoverage, set Intervals) bool {
	for _, iv := range c.cseqs() {
		if set.overlaps(iv[0], iv[1]) {
			return true
		}
	}
	return false
}

func insideSet(c InstanceCoverage, set Intervals) bool {
	for _, iv := range c.cseqs() {
		if !set.covers(iv[0], iv[1]) {
			return false
		}
	}
	return true
}

// sanitizeCoverage bounds a registration's coverage before any rule reads
// it: Apply runs on every node, so its cost must not depend on what one
// writer chose to send. An entry over the bounds is treated as truncated:
// it is never indexed, and it untracks its instance.
func sanitizeCoverage(p *RegisterFilePayload) {
	tooMany := len(p.Cov2) > maxCoverageInstances
	for i := range p.Cov2 {
		c := &p.Cov2[i]
		if tooMany || len(c.Ranges)+len(c.Partial) > maxCoverageEntries || len(c.Key) == 0 || len(c.Key) > maxTakeoverKeyLen {
			c.Ranges, c.Partial, c.Truncated = nil, nil, true
		}
	}
}

// TakeoverGeneration changes whenever the takeover state may have changed:
// a cache key for readers. Local to this node, not replicated.
func (f *ClusterFSM) TakeoverGeneration() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.takeover.gen
}

// CoverageCovered reports whether every row of piece p of (inst, key) in
// partition hour is in a registered file: the batch is settled in every hour
// (at or below the floor, not lost), held whole for the hour, or its pieces
// registered for the hour cover p. Unlike the duplicate rule, which refuses
// on any overlap, this is what a node may delete or stop holding on (#1207).
func (f *ClusterFSM) CoverageCovered(inst uint64, key string, p CoveragePiece, hour time.Time) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ki := f.takeover.index[inst][key]
	if ki == nil {
		return false
	}
	hr := hour.UTC().Truncate(time.Hour).Unix()
	h := ki.Hours[hr]
	if ki.floorSettled(p.CSeq) {
		whole, lost := ki.lostPieces(p.CSeq, hr)
		if !whole {
			// Covered unless a vanished piece overlapping p is not
			// republished (conservative: the whole vanished piece).
			for _, q := range lost {
				if piecesOverlap(p, q) && !(h != nil && (h.Whole.contains(q.CSeq) || piecesCover(h.samePieces(q.CSeq), q))) {
					return false
				}
			}
			return true
		}
	}
	if h == nil {
		return false
	}
	if h.Whole.contains(p.CSeq) {
		return true
	}
	var same []CoveragePiece
	for _, q := range h.Partial {
		if q.CSeq == p.CSeq {
			same = append(same, q)
		}
	}
	return piecesCover(same, p)
}

// piecesCover reports whether the pieces qs of one batch cover every row of
// p. Pieces in other row coordinates (chunked against unchunked) cover
// nothing of each other.
func piecesCover(qs []CoveragePiece, p CoveragePiece) bool {
	if len(qs) == 0 {
		return false
	}
	// chunkCovered: rows [lo, hi) of chunk c (chunked == the pieces use
	// chunk coordinates), or all of it when total is 0.
	chunkCovered := func(chunked bool, c uint16, lo, hi int) bool {
		var spans [][2]int
		total := -1
		for _, q := range qs {
			if (q.Chunks > 0) != chunked || (chunked && q.Chunk != c) {
				continue
			}
			if q.RowTotal == 0 {
				return true // all rows of this chunk
			}
			spans = append(spans, [2]int{q.RowLo, q.RowHi})
			total = q.RowTotal
		}
		if total < 0 {
			return false
		}
		if hi == 0 { // the whole chunk
			lo, hi = 0, total
		}
		sort.Slice(spans, func(i, j int) bool { return spans[i][0] < spans[j][0] })
		cur := lo
		for _, sp := range spans {
			if sp[0] > cur {
				break
			}
			if sp[1] > cur {
				cur = sp[1]
			}
		}
		return cur >= hi
	}
	switch {
	case p.whole():
		// Either every row of the unchunked batch, or every chunk whole.
		if chunkCovered(false, 0, 0, 0) {
			return true
		}
		n := uint16(0)
		for _, q := range qs {
			if q.Chunks > 0 {
				n = q.Chunks
				break
			}
		}
		if n == 0 {
			return false
		}
		for c := uint16(0); c < n; c++ {
			if !chunkCovered(true, c, 0, 0) {
				return false
			}
		}
		return true
	case p.RowTotal == 0:
		return chunkCovered(true, p.Chunk, 0, 0)
	default:
		return chunkCovered(p.Chunks > 0, p.Chunk, p.RowLo, p.RowHi)
	}
}

// CoveringFiles returns the files of database/measurement in partition hour
// whose coverage holds rows of piece p of (inst, key).
func (f *ClusterFSM) CoveringFiles(database, measurement string, hour time.Time, inst uint64, key string, p CoveragePiece) []FileEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	h := hour.UTC().Truncate(time.Hour).Unix()
	var out []FileEntry
	for path := range f.filesByDB[database] {
		e := f.files[path]
		if e == nil || e.Measurement != measurement || fileHour(*e) != h {
			continue
		}
		for _, c := range e.Coverage {
			if c.Instance != inst || c.Key != key {
				continue
			}
			hit := normalized(c.Ranges).contains(p.CSeq)
			for _, q := range c.Partial {
				if !hit && piecesOverlap(p, q) {
					hit = true
				}
			}
			if hit {
				out = append(out, *e)
				break
			}
		}
	}
	return out
}

func (f *ClusterFSM) bumpTakeoverGen() {
	f.mu.Lock()
	f.takeover.gen++
	f.mu.Unlock()
}

// UncoveredBatches returns the batches of [lo, hi] of (inst, key) whose rows
// in partition hour are not all in registered files, under one read of the
// state: settled in every hour (at or below the floor, not lost), or held
// whole for the hour, count as covered. A batch registered only in pieces is
// reported uncovered (conservative: callers keep it).
func (f *ClusterFSM) UncoveredBatches(inst uint64, key string, lo, hi uint64, hour time.Time) Intervals {
	f.mu.RLock()
	defer f.mu.RUnlock()
	all := Intervals{{lo, hi}}
	if lo > hi {
		return nil
	}
	ki := f.takeover.index[inst][key]
	if ki == nil {
		return all
	}
	hr := hour.UTC().Truncate(time.Hour).Unix()
	if ki.Floor >= lo {
		// A batch with a vanished piece here counts as uncovered.
		all = all.minus(Intervals{{lo, minU64(hi, ki.Floor)}}.minus(ki.Lost).minus(ki.lostBatches(hr)))
	}
	if h := ki.Hours[hr]; h != nil {
		all = all.minus(h.Whole.clip(lo, hi))
	}
	return all
}

// CoveringFilesOf returns the files of database/measurement in partition hour
// whose coverage of (inst, key) shares a batch with set, once per file.
func (f *ClusterFSM) CoveringFilesOf(database, measurement string, hour time.Time, inst uint64, key string, set Intervals) []FileEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	h := hour.UTC().Truncate(time.Hour).Unix()
	var out []FileEntry
	for path := range f.filesByDB[database] {
		e := f.files[path]
		if e == nil || e.Measurement != measurement || fileHour(*e) != h {
			continue
		}
		for _, c := range e.Coverage {
			if c.Instance != inst || c.Key != key {
				continue
			}
			hit := false
			for _, iv := range c.cseqs() {
				if set.overlaps(iv[0], iv[1]) {
					hit = true
					break
				}
			}
			if hit {
				out = append(out, *e)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}
