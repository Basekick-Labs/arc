package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// DELETE records (#1207 PR d1).
//
// A row DELETE runs on the primary over the files it has. Rows acknowledged
// before it may still be outside any registered file: in a standby's stream
// store (a claimant replays them after a failover), in a writer's WAL (an
// origin back with its volume republishes them, or recovers them at start),
// or pending in a writer's outbox. A DELETE record keeps the request in the
// replicated state until no such copy can be published any more, and every
// file holding a batch in its scope is rewritten without the matching rows
// before it registers (the outbox applies the records), or refused.
//
// A batch is in a record's scope when it was acknowledged before the
// DELETE: a batch of the deleting primary's instance at or below the mark it
// read, a batch of any other instance known at the time at or below the mark
// that instance's writer acknowledged (a live writer is fenced and stops
// accepting writes until it has), or any batch of an instance that never
// acknowledged (gone writers mint nothing more).

// maxDeleteRecords bounds the live records. Full, the oldest one pinned only
// by retired instances or by lost batches of live ones is dropped (see
// evictDeleteRecordLocked); otherwise a new DELETE is refused (retryable).
const maxDeleteRecords = 256

// maxDeleteWhereLen bounds a record's WHERE clause.
const maxDeleteWhereLen = 16 << 10

// RecordScope is one instance's part of a record's scope.
type RecordScope struct {
	Instance uint64 `json:"i"`
	// Acked: the instance's writer reported the batches it had acknowledged
	// when it applied the record (Marks); before that, all of its batches are
	// in scope.
	Acked bool              `json:"a,omitempty"`
	Marks map[string]uint64 `json:"m,omitempty"`
}

// DeleteRecord is one row DELETE (or DROP DATABASE) kept for the stream
// takeover.
type DeleteRecord struct {
	ID          uint64 `json:"id"` // the log index it was applied at
	Origin      string `json:"origin"`
	Database    string `json:"db"`
	Measurement string `json:"m,omitempty"` // "" = every measurement of the database
	Where       string `json:"w,omitempty"` // "" = every row
	// Own is the deleting writer's instance; OwnMarks its acknowledged
	// batches per buffer key when it ran the DELETE.
	Own      uint64            `json:"own,omitempty"`
	OwnMarks map[string]uint64 `json:"own_marks,omitempty"`
	// Before is every other instance the cluster knew of, by instance.
	Before []RecordScope `json:"before,omitempty"`
	At     time.Time     `json:"at"`
}

// MatchesKey reports whether buffer key ("database/measurement") is one the
// record deletes rows of.
func (r *DeleteRecord) MatchesKey(key string) bool {
	db, m, ok := strings.Cut(key, "/")
	return ok && db == r.Database && (r.Measurement == "" || m == r.Measurement)
}

func (r *DeleteRecord) scopeOf(inst uint64) *RecordScope {
	i := sort.Search(len(r.Before), func(i int) bool { return r.Before[i].Instance >= inst })
	if i < len(r.Before) && r.Before[i].Instance == inst {
		return &r.Before[i]
	}
	return nil
}

// InScope reports whether batch cseq of (inst, key) was acknowledged before
// the DELETE, so its rows matching Where are deleted.
func (r *DeleteRecord) InScope(inst uint64, key string, cseq uint64) bool {
	if !r.MatchesKey(key) {
		return false
	}
	if inst == r.Own && r.Own != 0 {
		return cseq <= r.OwnMarks[key]
	}
	s := r.scopeOf(inst)
	if s == nil {
		return false
	}
	return !s.Acked || cseq <= s.Marks[key]
}

// coverageScope reports how c's batches stand against r: every one in scope,
// none, or some (a file that straddles a mark).
func (r *DeleteRecord) coverageScope(c InstanceCoverage) (any, all bool) {
	if !r.MatchesKey(c.Key) {
		return false, false
	}
	if c.Truncated {
		// Its batches are not listed: in scope if any batch of the instance
		// could be. Applying the record to such a file is safe either way.
		if c.Instance == r.Own && r.Own != 0 {
			return r.OwnMarks[c.Key] > 0, false
		}
		if s := r.scopeOf(c.Instance); s != nil {
			return !s.Acked || s.Marks[c.Key] > 0, false
		}
		return false, false
	}
	all = true
	for _, iv := range c.cseqs() {
		for _, x := range []uint64{iv[0], iv[1]} {
			if r.InScope(c.Instance, c.Key, x) {
				any = true
			} else {
				all = false
			}
		}
	}
	return any, all && any
}

func (r *DeleteRecord) clone() DeleteRecord {
	c := *r
	c.OwnMarks = cloneMarks(r.OwnMarks)
	c.Before = make([]RecordScope, len(r.Before))
	for i, s := range r.Before {
		c.Before[i] = RecordScope{Instance: s.Instance, Acked: s.Acked, Marks: cloneMarks(s.Marks)}
	}
	return c
}

func cloneMarks(m map[string]uint64) map[string]uint64 {
	if m == nil {
		return nil
	}
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// RecordDeletePayload is proposed by the primary before it runs a DELETE.
// Marks are its instance's acknowledged batches per buffer key of the
// DELETE's scope.
type RecordDeletePayload struct {
	Origin      string            `json:"origin"`
	Instance    uint64            `json:"instance"`
	Database    string            `json:"db"`
	Measurement string            `json:"m,omitempty"`
	Where       string            `json:"w,omitempty"`
	Marks       map[string]uint64 `json:"marks,omitempty"`
	Now         time.Time         `json:"now"`
}

// RecordDeleteAckPayload is a writer reporting, for a record naming its
// current instance, the batches that instance had acknowledged.
type RecordDeleteAckPayload struct {
	Origin   string            `json:"origin"`
	Record   uint64            `json:"record"`
	Instance uint64            `json:"instance"`
	Marks    map[string]uint64 `json:"marks,omitempty"`
}

// SettleDeletedPayload settles batch pieces whose rows in Hour every listed
// record deleted: the file that would have held them has none left, so no
// file registers them. Proposed by a claimant for its claim, or by an
// origin for an earlier instance of its own.
type SettleDeletedPayload struct {
	By       string           `json:"by"`
	Hour     int64            `json:"hour"` // unix seconds of the partition hour
	Coverage InstanceCoverage `json:"cov"`
	Records  []uint64         `json:"records"`
}

func validMarks(m map[string]uint64) error {
	if len(m) > maxInstanceKeys {
		return fmt.Errorf("%d buffer keys exceed %d", len(m), maxInstanceKeys)
	}
	for k := range m {
		if len(k) == 0 || len(k) > maxTakeoverKeyLen {
			return fmt.Errorf("bad buffer key %q", k)
		}
	}
	return nil
}

func (f *ClusterFSM) applyRecordDelete(payload []byte, logIndex uint64) interface{} {
	if len(payload) > MaxTakeoverPayloadSize {
		return fmt.Errorf("record delete: payload of %d bytes exceeds %d", len(payload), MaxTakeoverPayloadSize)
	}
	var p RecordDeletePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal record delete payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.streamClaims {
		return errors.New("record delete: stream takeover is not enabled")
	}
	switch {
	case p.Origin == "" || p.Database == "" || p.Now.IsZero():
		return errors.New("record delete: origin, database and time are required")
	case len(p.Where) > maxDeleteWhereLen:
		return fmt.Errorf("record delete: WHERE of %d bytes exceeds %d", len(p.Where), maxDeleteWhereLen)
	case strings.Contains(p.Database, "/") || strings.Contains(p.Measurement, "/"):
		return errors.New("record delete: bad database or measurement")
	}
	if err := validMarks(p.Marks); err != nil {
		return fmt.Errorf("record delete: %w", err)
	}
	if n, ok := f.nodes[p.Origin]; !ok || n.Role != "writer" {
		return fmt.Errorf("record delete: %s is not a writer in the cluster", p.Origin)
	}
	if f.primaryWriterID != "" && f.primaryWriterID != p.Origin {
		return fmt.Errorf("record delete: %s is not the primary writer", p.Origin)
	}
	t := f.takeover
	// An instance the table had no room for is not current; its writer
	// still records its DELETEs under it.
	if cur := t.current[p.Origin]; cur != p.Instance && (cur != 0 || t.instances[p.Instance] != nil) {
		return fmt.Errorf("record delete: %s is not the current instance of %s", instKey(p.Instance), p.Origin)
	}
	f.gcDeleteRecordsLocked()
	if len(t.records) >= maxDeleteRecords && !f.evictDeleteRecordLocked() {
		return ErrDeleteRecordsFull
	}
	r := &DeleteRecord{
		ID: logIndex, Origin: p.Origin, Database: p.Database, Measurement: p.Measurement, Where: p.Where,
		Own: p.Instance, At: p.Now,
	}
	for k, v := range p.Marks {
		if v > 0 && r.MatchesKey(k) {
			if r.OwnMarks == nil {
				r.OwnMarks = map[string]uint64{}
			}
			r.OwnMarks[k] = v
		}
	}
	for _, id := range sortedInstanceIDs(t.instances) {
		if id != p.Instance {
			r.Before = append(r.Before, RecordScope{Instance: id})
		}
	}
	t.records = append(t.records, r)
	t.gen++
	f.refreshUnackedDeletesLocked()
	return nil
}

// ErrDeleteRecordsFull refuses a DELETE while maxDeleteRecords records are
// live and none can be dropped. Retryable: records drop as writers settle.
var ErrDeleteRecordsFull = errors.New("record delete: too many DELETEs are waiting for writers to settle their batches; retry later")

func (f *ClusterFSM) applyRecordDeleteAck(payload []byte) interface{} {
	if len(payload) > MaxTakeoverPayloadSize {
		return fmt.Errorf("record delete ack: payload of %d bytes exceeds %d", len(payload), MaxTakeoverPayloadSize)
	}
	var p RecordDeleteAckPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal record delete ack payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.streamClaims {
		return errors.New("record delete ack: stream takeover is not enabled")
	}
	if err := validMarks(p.Marks); err != nil {
		return fmt.Errorf("record delete ack: %w", err)
	}
	t := f.takeover
	r := f.deleteRecordLocked(p.Record)
	if r == nil {
		return fmt.Errorf("record delete ack: no record %d", p.Record)
	}
	if p.Origin == "" || t.current[p.Origin] != p.Instance {
		return fmt.Errorf("record delete ack: %s is not the current instance of %s", instKey(p.Instance), p.Origin)
	}
	s := r.scopeOf(p.Instance)
	if s == nil || s.Acked {
		return nil // not named, or already acknowledged: nothing to change
	}
	s.Acked = true
	s.Marks = nil
	for k, v := range p.Marks {
		if v > 0 && r.MatchesKey(k) {
			if s.Marks == nil {
				s.Marks = map[string]uint64{}
			}
			s.Marks[k] = v
		}
	}
	t.gen++
	f.gcDeleteRecordsLocked()
	return nil
}

func (f *ClusterFSM) applySettleDeleted(payload []byte) interface{} {
	if len(payload) > MaxTakeoverPayloadSize {
		return fmt.Errorf("settle deleted: payload of %d bytes exceeds %d", len(payload), MaxTakeoverPayloadSize)
	}
	var p SettleDeletedPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal settle deleted payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.streamClaims {
		return errors.New("settle deleted: stream takeover is not enabled")
	}
	t := f.takeover
	c := p.Coverage
	in := t.instances[c.Instance]
	switch {
	case p.By == "" || len(p.Records) == 0:
		return errors.New("settle deleted: proposer and records are required")
	case c.Truncated || len(c.Key) == 0 || len(c.Key) > maxTakeoverKeyLen:
		return errors.New("settle deleted: the coverage must list its batches")
	case len(c.Ranges)+len(c.Partial) > maxCoverageEntries:
		return errors.New("settle deleted: coverage over its bound")
	case in == nil || in.Untracked:
		// Nothing tracks its batches (removed, or untracked): nothing to
		// settle, and the writer may drop the file.
		return nil
	}
	// Only the writer publishing these batches may settle them: the claimant
	// of an active, unfinished claim holding them, or their origin for one of
	// its earlier instances (its current one settles by its floors).
	k := t.claims[c.Instance]
	switch {
	case k.active() && !k.Done && k.By == p.By && c.ClaimEpoch == k.Epoch && insideSet(c, k.Set[c.Key]):
	case c.ClaimEpoch == 0 && in.Origin == p.By && t.current[p.By] != c.Instance && !(k.active() && !k.Done):
	default:
		return fmt.Errorf("settle deleted: %s may not settle batches of %s", p.By, instKey(c.Instance))
	}
	var recs []*DeleteRecord
	for _, id := range p.Records {
		r := f.deleteRecordLocked(id)
		if r == nil {
			return fmt.Errorf("settle deleted: no record %d", id)
		}
		recs = append(recs, r)
	}
	// A record's scope in one (instance, key) is every batch up to a mark, or
	// all of them: an interval is in scope when its last batch is.
	for _, iv := range c.cseqs() {
		ok := false
		for _, r := range recs {
			if r.InScope(c.Instance, c.Key, iv[1]) {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("settle deleted: batch %d of %s is in no listed record", iv[1], c.Key)
		}
	}
	f.indexAddLocked(c, p.Hour)
	return nil
}

func (f *ClusterFSM) deleteRecordLocked(id uint64) *DeleteRecord {
	recs := f.takeover.records
	i := sort.Search(len(recs), func(i int) bool { return recs[i].ID >= id })
	if i < len(recs) && recs[i].ID == id {
		return recs[i]
	}
	return nil
}

// recordPinsLocked returns the instances that still keep r: an instance with
// a batch in scope that may still be published outside the files the DELETE
// rewrote. retired and lost say why each pins it. Caller holds f.mu.
func (f *ClusterFSM) recordPinsLocked(r *DeleteRecord) (pins []uint64, onlyRetiredOrLost bool) {
	t := f.takeover
	onlyRetiredOrLost = true
	settled := func(inst uint64, marks map[string]uint64) (ok, lostOnly bool) {
		lostOnly = true
		ok = true
		for key, mark := range marks {
			if mark == 0 {
				continue
			}
			floor, lost := uint64(0), Intervals(nil)
			if ki := t.index[inst][key]; ki != nil {
				floor, lost = ki.Floor, ki.Lost
			} else if u, found := t.untrackedFloors[inst][key]; found {
				floor, lost = u.Floor, u.Lost
			}
			if floor < mark {
				ok, lostOnly = false, false
				continue
			}
			if lost.overlaps(1, mark) {
				ok = false
			}
		}
		return ok, lostOnly && !ok
	}
	check := func(inst uint64, acked bool, marks map[string]uint64) {
		in := t.instances[inst]
		if in == nil {
			return // removed: refused as stale wherever it turns up
		}
		if !acked {
			pins = append(pins, inst)
			if in.Status != InstanceRetired {
				onlyRetiredOrLost = false
			}
			return
		}
		ok, lostOnly := settled(inst, marks)
		if ok {
			return
		}
		pins = append(pins, inst)
		if !lostOnly && in.Status != InstanceRetired {
			onlyRetiredOrLost = false
		}
	}
	if r.Own != 0 {
		check(r.Own, true, r.OwnMarks)
	}
	for _, s := range r.Before {
		check(s.Instance, s.Acked, s.Marks)
	}
	return pins, onlyRetiredOrLost
}

// gcDeleteRecordsLocked drops the records nothing can publish a batch of any
// more. Caller holds f.mu.
func (f *ClusterFSM) gcDeleteRecordsLocked() {
	t := f.takeover
	kept := t.records[:0]
	for _, r := range t.records {
		if pins, _ := f.recordPinsLocked(r); len(pins) > 0 {
			kept = append(kept, r)
			continue
		}
		t.gen++
	}
	for i := len(kept); i < len(t.records); i++ {
		t.records[i] = nil
	}
	t.records = kept
	f.refreshUnackedDeletesLocked()
}

// refreshUnackedDeletesLocked republishes the instances that owe a DELETE
// record an acknowledgement. Caller holds f.mu.
func (f *ClusterFSM) refreshUnackedDeletesLocked() {
	m := map[uint64]bool{}
	for _, r := range f.takeover.records {
		for _, s := range r.Before {
			if !s.Acked {
				m[s.Instance] = true
			}
		}
	}
	f.unackedDeletes.Store(&m)
}

// OwesDeleteAck reports, without taking the FSM lock, whether a live DELETE
// record names inst and its writer has not acknowledged it: that writer
// accepts no write until it has, or a row acknowledged after the DELETE
// would be in its scope.
func (f *ClusterFSM) OwesDeleteAck(inst uint64) bool {
	m := f.unackedDeletes.Load()
	return m != nil && (*m)[inst]
}

// evictDeleteRecordLocked makes room for a record: the oldest record kept
// only by retired instances (writers the cluster wrote off) and by lost
// batches of live ones is dropped, and those retired instances are removed,
// so copies of them that come back are refused and stay parked rather than
// publish rows the DELETE removed. Lost batches of a live instance, should
// they ever be claimed, are published without the dropped record (logged).
// Reports whether a record was dropped. Caller holds f.mu.
func (f *ClusterFSM) evictDeleteRecordLocked() bool {
	t := f.takeover
	var r *DeleteRecord
	var pins []uint64
	for _, c := range t.records {
		if p, ok := f.recordPinsLocked(c); ok {
			r, pins = c, p
			break
		}
	}
	if r == nil {
		return false
	}
	for _, inst := range pins {
		in := t.instances[inst]
		if in == nil {
			continue
		}
		if in.Status == InstanceRetired {
			f.logger.Warn().Uint64("record", r.ID).Str("instance", instKey(inst)).Str("origin", in.Origin).
				Msg("Too many DELETE records; a retired writer instance holding one back is removed, and copies of it that come back are parked")
			f.removeInstanceLocked(inst)
			continue
		}
		f.logger.Error().Uint64("record", r.ID).Str("instance", instKey(inst)).Str("origin", in.Origin).
			Msg("Too many DELETE records; one is dropped while lost batches of a writer instance are in its scope: if those batches are ever replayed, rows it deleted come back")
	}
	// Removing an instance collects records already; drop r if it is left.
	for i, c := range t.records {
		if c == r {
			t.records = append(t.records[:i], t.records[i+1:]...)
			t.gen++
			break
		}
	}
	f.refreshUnackedDeletesLocked()
	return true
}

// deletedRefusalLocked applies the deleted rule to one coverage entry of a
// registration: a file holding a batch of a live record's scope must have
// been rewritten with every such record (DeleteSeen at or past its ID).
// Caller holds f.mu.
func (f *ClusterFSM) deletedRefusalLocked(c InstanceCoverage, seen uint64) (RefusedOp, bool) {
	recs := f.takeover.records
	i := sort.Search(len(recs), func(i int) bool { return recs[i].ID > seen })
	for _, r := range recs[i:] {
		if any, _ := r.coverageScope(c); any {
			return RefusedOp{Kind: RefusedDeleted, Instance: c.Instance, Epoch: c.ClaimEpoch}, true
		}
	}
	return RefusedOp{}, false
}

// DeleteRecords returns the live DELETE records, oldest first.
func (f *ClusterFSM) DeleteRecords() []DeleteRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]DeleteRecord, 0, len(f.takeover.records))
	for _, r := range f.takeover.records {
		out = append(out, r.clone())
	}
	return out
}

// DeleteRecordsFor returns the live records with an ID above after that
// hold a batch of cov in their scope, oldest first. straddle is set when
// one of them holds some of a coverage entry's batches but not all: such a
// file cannot be rewritten with it as a whole.
func (f *ClusterFSM) DeleteRecordsFor(cov []InstanceCoverage, after uint64) (out []DeleteRecord, straddle bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	recs := f.takeover.records
	i := sort.Search(len(recs), func(i int) bool { return recs[i].ID > after })
	for _, r := range recs[i:] {
		hit := false
		for _, c := range cov {
			any, all := r.coverageScope(c)
			if any {
				hit = true
				if !all && !c.Truncated {
					straddle = true
				}
			}
		}
		if hit {
			out = append(out, r.clone())
		}
	}
	return out, straddle
}

// DeleteScopeIDs returns the IDs of the live records holding batch cseq of
// (inst, key) in their scope: two batches with different answers must not
// share a file.
func (f *ClusterFSM) DeleteScopeIDs(inst uint64, key string, cseq uint64) []uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []uint64
	for _, r := range f.takeover.records {
		if r.InScope(inst, key, cseq) {
			out = append(out, r.ID)
		}
	}
	return out
}

// UnackedDeleteRecords returns the live records naming inst that its writer
// has not acknowledged yet, oldest first.
func (f *ClusterFSM) UnackedDeleteRecords(inst uint64) []DeleteRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []DeleteRecord
	for _, r := range f.takeover.records {
		if s := r.scopeOf(inst); s != nil && !s.Acked {
			out = append(out, r.clone())
		}
	}
	return out
}

// LatestDeleteRecord returns the ID of the newest live record, 0 if none.
func (f *ClusterFSM) LatestDeleteRecord() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if n := len(f.takeover.records); n > 0 {
		return f.takeover.records[n-1].ID
	}
	return 0
}

// untrackedSettleLocked keeps the floors an untracked instance settles (it
// has no index): the DELETE records naming it are collected by them.
// Caller holds f.mu.
func (f *ClusterFSM) untrackedSettleLocked(p SettleCoveragePayload) {
	t := f.takeover
	keys := t.untrackedFloors[p.Instance]
	names := make([]string, 0, len(p.Keys))
	for k := range p.Keys {
		names = append(names, k)
	}
	sort.Strings(names) // the key bound must cut the same keys everywhere
	for _, key := range names {
		s := p.Keys[key]
		if len(key) == 0 || len(key) > maxTakeoverKeyLen {
			continue
		}
		cur, ok := keys[key]
		if !ok && len(keys) >= maxInstanceKeys {
			continue
		}
		if s.Floor <= cur.Floor {
			continue
		}
		for _, iv := range s.Lost {
			if iv[1] > cur.Floor && iv[0] <= s.Floor && len(cur.Lost) < maxKeyLost {
				cur.Lost = cur.Lost.addRange(maxU64(iv[0], cur.Floor+1), minU64(iv[1], s.Floor))
			}
		}
		cur.Floor = s.Floor
		if keys == nil {
			keys = map[string]KeySettlement{}
			t.untrackedFloors[p.Instance] = keys
		}
		keys[key] = cur
	}
	f.gcDeleteRecordsLocked()
}
