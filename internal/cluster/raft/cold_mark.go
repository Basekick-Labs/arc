package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Cold mark (#1552).
//
// For each "database/measurement" scope, the latest UTC day that may hold an
// object in the cold tier. A cluster reader whose cold listing fails can use
// it to tell that a range starting after the mark needs no cold data, which
// its own tier rows cannot tell it (they lag the cold store in several ways).
//
// Invariant: the mark is at or after the day of every cold object of its
// scope. It holds because
//   - every cold writer (the migrator, a restore) raises the mark, through a
//     committed Add, before it writes the object;
//   - nothing ever lowers it (retention, DELETE and policy changes leave it
//     high, which only costs what a read costs without it);
//   - objects written before the mark existed are covered by a seed: a cold
//     listing that started after the fence below, folded in with Seed.
//
// MarkUnknownDay is the mark of a scope holding an object whose day cannot be
// read from its key: such a scope is never hot-only.
//
// State machine, the same pure function of the log on every node:
//
//	off ──Enable──▶ enabled ──acks of every node at enable──▶ fenced ──Seed(final)──▶ seeded
//
//   - Enable is proposed by the leader once every node advertises
//     CapabilityColdMark; it records the node table as AckWanted.
//   - A node acks once it applied Enable and no cold write it began without a
//     committed Add can still be running (cluster.coldWriteMu): before Enable,
//     a writer in a cluster that still has older builds writes unmarked.
//   - Fenced latches when every AckWanted node still in the table acked; a
//     node removal rechecks it.
//   - Seed is refused before the fence; its final chunk sets Seeded and the
//     cold backend identity it listed. Seeds from two different identities
//     set BackendConflict, and the mark is never trusted.
//
// Add and Seed apply in any state: both only raise marks.
//
// Liveness (rolled-back members): a node on an older build is refused at
// join but can stay in the Raft configuration and the node table, and such a
// node writes cold objects without raising the mark. Every node on this build
// therefore proposes Alive every ColdMarkAliveEvery once the mark is enabled,
// and the mark is trusted (ColdMarkTrustedFor) only while every node in the
// table and every Raft server has an Alive no older than ColdMarkAliveMaxAge.
// A member that stops proposing it, whatever the reason, makes the mark
// untrusted within ColdMarkAliveMaxAge (a rolled-back member can write
// unmarked for that long before readers stop trusting it); a removed node
// stops counting, and its Alive and Ack are forgotten. Alive carries the
// proposer's time and Apply keeps the latest in log order (Apply never reads
// the clock); an Alive more than ColdMarkAliveSkew in a reader's future
// counts as not alive. The join gate refuses older builds only while the
// flag is known on (on the proposing leader, also while its proposal may
// still commit); the liveness check covers every other case.
//
// A reader also checks that its own cold store is the one the mark was
// seeded from (ColdMarkTrustedFor's localIdentity). BackendConflict stays
// until an admin resets the seed (CommandColdMarkReset): that clears Seeded,
// the identity and the conflict, keeps every mark (they stay a superset), and
// the next cold listing after the fence seeds again.

// CapabilityColdMark is advertised at join by every build that maintains the
// cold mark.
const CapabilityColdMark = "cold_mark_v1"

// Liveness timing: each node proposes Alive every ColdMarkAliveEvery; the
// mark is trusted while every member's last Alive is within
// ColdMarkAliveMaxAge.
// An Alive more than ColdMarkAliveSkew in the reader's future counts as not
// alive: a node whose clock runs ahead must not keep the mark trusted after
// it stops confirming.
const (
	ColdMarkAliveEvery  = 5 * time.Minute
	ColdMarkAliveMaxAge = 15 * time.Minute
	ColdMarkAliveSkew   = time.Minute
)

// maxColdMarkSegmentLen bounds each scope segment: a query refuses database
// and measurement names longer than 128 bytes, so a longer scope is read by
// no query.
const maxColdMarkSegmentLen = 128

// MarkUnknownDay marks a scope whose cold objects' days are not all known.
const MarkUnknownDay = int32(math.MaxInt32)

// maxColdMarkEntries bounds one Add or Seed command; maxColdMarkScopes the
// replicated state.
const (
	maxColdMarkEntries = 1000
	maxColdMarkScopes  = 1 << 20
)

// EnableColdMarkPayload turns the cold mark on. Nodes lists the node ids the
// proposer saw advertising the capability; Apply refuses unless every node in
// this FSM's table is among them.
type EnableColdMarkPayload struct {
	Nodes []string `json:"nodes"`
}

// ColdMarkAddPayload raises marks: scope -> UTC day (days since the epoch).
// At is the proposer's time, recorded as the scope's last raise (Apply
// never reads the clock); zero from an older proposer.
type ColdMarkAddPayload struct {
	Marks map[string]int32 `json:"marks"`
	At    time.Time        `json:"at,omitempty"`
}

// ColdMarkAckPayload is a node reporting that it applied Enable and drained
// its unmarked cold writes.
type ColdMarkAckPayload struct {
	NodeID string `json:"node_id"`
}

// ColdMarkAlivePayload is a node on a build that maintains the mark saying
// so, at At (the proposer's clock).
type ColdMarkAlivePayload struct {
	NodeID string    `json:"node_id"`
	At     time.Time `json:"at"`
}

// ColdMarkResetPayload is an admin resetting the seed, with who and why for
// the log.
type ColdMarkResetPayload struct {
	By     string `json:"by,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// ColdMarkSeedPayload folds a cold listing into the marks. Final sets Seeded,
// with Backend the identity of the cold store listed.
type ColdMarkSeedPayload struct {
	Marks   map[string]int32 `json:"marks"`
	At      time.Time        `json:"at,omitempty"`
	Final   bool             `json:"final,omitempty"`
	Backend string           `json:"backend,omitempty"`
}

// coldMarkState is the replicated state and its snapshot form.
type coldMarkState struct {
	Enabled         bool             `json:"enabled,omitempty"`
	AckWanted       []string         `json:"ack_wanted,omitempty"`
	Acked           map[string]bool  `json:"acked,omitempty"`
	Fenced          bool             `json:"fenced,omitempty"`
	Seeded          bool             `json:"seeded,omitempty"`
	Backend         string           `json:"backend,omitempty"`
	BackendConflict bool             `json:"backend_conflict,omitempty"`
	Marks           map[string]int32 `json:"marks,omitempty"`
	// Alive: each node's latest Alive time.
	Alive map[string]time.Time `json:"alive,omitempty"`
	// Raised: each scope's last raise, the proposer's time from the Add or
	// Seed that set or raised it. A reader uses it to tell a recent cold
	// write its own tier rows may not know of yet from an old one.
	Raised map[string]time.Time `json:"raised,omitempty"`
}

// ColdMarkStatus is a copy of the flags, for status and the coordinator.
type ColdMarkStatus struct {
	Enabled         bool     `json:"enabled"`
	Fenced          bool     `json:"fenced"`
	Seeded          bool     `json:"seeded"`
	BackendConflict bool     `json:"backend_conflict,omitempty"`
	Scopes          int      `json:"scopes"`
	AckPending      []string `json:"ack_pending,omitempty"`
}

func (f *ClusterFSM) coldMarkLocked() *coldMarkState {
	if f.coldMark == nil {
		f.coldMark = &coldMarkState{}
	}
	return f.coldMark
}

// validColdMarkScope accepts "database/measurement" with two non-empty
// segments and no traversal.
// ValidColdMarkScope is validColdMarkScope for proposers, which drop such
// keys before proposing rather than have one refuse a whole seed.
func ValidColdMarkScope(scope string) bool { return validColdMarkScope(scope) }

func validColdMarkScope(scope string) bool {
	db, meas, ok := strings.Cut(scope, "/")
	if !ok || db == "" || meas == "" || strings.Contains(meas, "/") || len(db) > maxColdMarkSegmentLen || len(meas) > maxColdMarkSegmentLen {
		return false
	}
	return !strings.Contains(scope, "..") && !strings.ContainsAny(scope, "\\\x00")
}

// raiseColdMarksLocked raises the marks in m. Validates everything first so a
// refused command changes nothing.
func (f *ClusterFSM) raiseColdMarksLocked(m map[string]int32, at time.Time) error {
	if len(m) > maxColdMarkEntries {
		return fmt.Errorf("cold mark: %d entries, at most %d", len(m), maxColdMarkEntries)
	}
	st := f.coldMarkLocked()
	added := 0
	for scope, day := range m {
		if !validColdMarkScope(scope) {
			return fmt.Errorf("cold mark: invalid scope %q", scope)
		}
		if day < 0 {
			return fmt.Errorf("cold mark: invalid day %d for %q", day, scope)
		}
		if _, ok := st.Marks[scope]; !ok {
			added++
		}
	}
	if len(st.Marks)+added > maxColdMarkScopes {
		return fmt.Errorf("cold mark: more than %d scopes", maxColdMarkScopes)
	}
	if st.Marks == nil {
		st.Marks = make(map[string]int32, len(m))
	}
	for scope, day := range m {
		if cur, ok := st.Marks[scope]; !ok || day > cur {
			st.Marks[scope] = day
			if !at.IsZero() {
				if st.Raised == nil {
					st.Raised = map[string]time.Time{}
				}
				st.Raised[scope] = at.UTC()
			}
		}
	}
	return nil
}

func (f *ClusterFSM) applyEnableColdMark(payload []byte) interface{} {
	var p EnableColdMarkPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal enable cold mark payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.coldMarkLocked()
	if st.Enabled {
		return nil
	}
	listed := make(map[string]bool, len(p.Nodes))
	for _, n := range p.Nodes {
		listed[n] = true
	}
	wanted := make([]string, 0, len(f.nodes))
	for id := range f.nodes {
		if !listed[id] {
			return fmt.Errorf("enable cold mark: node %s is not known to support it", id)
		}
		wanted = append(wanted, id)
	}
	sort.Strings(wanted)
	st.Enabled = true
	st.AckWanted = wanted
	st.Acked = map[string]bool{}
	f.updateColdMarkFenceLocked()
	f.logger.Info().Int("nodes", len(wanted)).Msg("Cold mark enabled: every node supports it")
	return nil
}

func (f *ClusterFSM) applyColdMarkAdd(payload []byte) interface{} {
	var p ColdMarkAddPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal cold mark add payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.raiseColdMarksLocked(p.Marks, p.At)
}

func (f *ClusterFSM) applyColdMarkAck(payload []byte) interface{} {
	var p ColdMarkAckPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal cold mark ack payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.coldMarkLocked()
	if !st.Enabled {
		return errors.New("cold mark ack: not enabled")
	}
	if _, ok := f.nodes[p.NodeID]; !ok {
		return fmt.Errorf("cold mark ack: unknown node %q", p.NodeID)
	}
	st.Acked[p.NodeID] = true
	f.updateColdMarkFenceLocked()
	return nil
}

func (f *ClusterFSM) applyColdMarkAlive(payload []byte) interface{} {
	var p ColdMarkAlivePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal cold mark alive payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.coldMarkLocked()
	if !st.Enabled {
		return errors.New("cold mark alive: not enabled")
	}
	if _, ok := f.nodes[p.NodeID]; !ok {
		return fmt.Errorf("cold mark alive: unknown node %q", p.NodeID)
	}
	if p.At.IsZero() {
		return errors.New("cold mark alive: no time")
	}
	if st.Alive == nil {
		st.Alive = map[string]time.Time{}
	}
	// Latest in log order, not the maximum: a node can only speak for
	// itself, and a clock that ran ahead and was corrected must be able to
	// bring its Alive back (a maximum would hold the future time forever).
	st.Alive[p.NodeID] = p.At.UTC()
	return nil
}

// ColdMarkLastAlive returns nodeID's latest Alive time (zero if none).
func (f *ClusterFSM) ColdMarkLastAlive(nodeID string) time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.coldMark == nil {
		return time.Time{}
	}
	return f.coldMark.Alive[nodeID]
}

// ColdMarkTrustedFor reports whether readers may use the mark at now: enabled,
// fenced, seeded from one cold store, and every node in the table and every
// id in servers (the Raft configuration) has an Alive within maxAge of now.
// why names the first reason it is not.
//
// localIdentity is the reader's own cold store identity: a mark seeded from
// another store says nothing about the one this node reads.
func (f *ClusterFSM) ColdMarkTrustedFor(now time.Time, maxAge time.Duration, servers []string, localIdentity string) (trusted bool, why string) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	st := f.coldMark
	switch {
	case st == nil || !st.Enabled:
		return false, "the cold mark is not enabled"
	case !st.Fenced:
		return false, "the cold mark is not fenced yet"
	case !st.Seeded:
		return false, "the cold mark is not seeded yet"
	case st.BackendConflict:
		return false, "the cold mark was seeded from two different cold stores"
	case localIdentity == "" || localIdentity != st.Backend:
		return false, "this node's cold tier is not the cold store the mark was seeded from; every node must use the same cold tier"
	}
	oldest, newest := now.Add(-maxAge), now.Add(ColdMarkAliveSkew)
	ids := make([]string, 0, len(f.nodes))
	for id := range f.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		switch at := st.Alive[id]; {
		case at.After(newest):
			return false, "node " + id + " confirmed the cold mark at " + at.Format(time.RFC3339) + ", in this node's future: check clock skew"
		case at.Before(oldest):
			return false, "node " + id + " has not confirmed it maintains the cold mark within " + maxAge.String()
		}
	}
	for _, id := range servers {
		if _, member := f.nodes[id]; !member {
			return false, "Raft server " + id + " is not a cluster node; remove the voter"
		}
	}
	return true, ""
}

// coldMarkNodeRemovedLocked forgets a removed node: its Alive and Ack stop
// counting, and a node rejoining with the id must confirm and ack again.
func (f *ClusterFSM) coldMarkNodeRemovedLocked(id string) {
	if st := f.coldMark; st != nil {
		delete(st.Alive, id)
		delete(st.Acked, id)
	}
	f.updateColdMarkFenceLocked()
}

// updateColdMarkFenceLocked latches Fenced once every AckWanted node still
// in the node table acked.
func (f *ClusterFSM) updateColdMarkFenceLocked() {
	st := f.coldMark
	if st == nil || !st.Enabled || st.Fenced {
		return
	}
	for _, id := range st.AckWanted {
		if _, member := f.nodes[id]; member && !st.Acked[id] {
			return
		}
	}
	st.Fenced = true
	f.logger.Info().Msg("Cold mark fenced: no node can still be writing to the cold tier without raising it")
}

func (f *ClusterFSM) applyColdMarkSeed(payload []byte) interface{} {
	var p ColdMarkSeedPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal cold mark seed payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.coldMarkLocked()
	if !st.Fenced {
		return errors.New("cold mark seed: not fenced")
	}
	if p.Final && p.Backend == "" {
		return errors.New("cold mark seed: final chunk without a backend identity")
	}
	if err := f.raiseColdMarksLocked(p.Marks, p.At); err != nil {
		return err
	}
	if !p.Final {
		return nil
	}
	switch {
	case st.Backend == "":
		st.Backend = p.Backend
	case st.Backend != p.Backend && !st.BackendConflict:
		st.BackendConflict = true
		f.logger.Error().Str("seeded", st.Backend).Str("listed", p.Backend).
			Msg("Cold mark seeded from two different cold stores; it is not trusted. Every node must use the same cold tier bucket and prefix")
	}
	if !st.Seeded {
		st.Seeded = true
		f.logger.Info().Int("scopes", len(st.Marks)).Msg("Cold mark seeded")
	}
	return nil
}

func (f *ClusterFSM) applyColdMarkReset(payload []byte) interface{} {
	var p ColdMarkResetPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal cold mark reset payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.coldMarkLocked()
	if !st.Enabled {
		return errors.New("cold mark reset: the cold mark is not enabled")
	}
	st.Seeded, st.Backend, st.BackendConflict = false, "", false
	f.logger.Warn().Str("by", p.By).Str("reason", p.Reason).
		Msg("Cold mark seed reset by an admin: readers stop using it until the next cold listing seeds it again")
	return nil
}

// ColdMark returns the mark of scope ("database/measurement"): has is false
// when no cold object of the scope has been marked, trusted when the mark is
// enabled, fenced, seeded and from one cold store. A mark of MarkUnknownDay
// means the scope's days are not known.
func (f *ClusterFSM) ColdMark(scope string) (day int32, has, trusted bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	st := f.coldMark
	if st == nil {
		return 0, false, false
	}
	day, has = st.Marks[scope]
	return day, has, st.Enabled && st.Fenced && st.Seeded && !st.BackendConflict
}

// ColdMarkRaisedAt is scope's last raise (zero when unknown).
func (f *ClusterFSM) ColdMarkRaisedAt(scope string) time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.coldMark == nil {
		return time.Time{}
	}
	return f.coldMark.Raised[scope]
}

// ColdMarkCovers reports whether scope's mark is already at or after day.
func (f *ClusterFSM) ColdMarkCovers(scope string, day int32) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.coldMark == nil {
		return false
	}
	cur, ok := f.coldMark.Marks[scope]
	return ok && cur >= day
}

// ColdMarkBackend returns the cold store identity the mark was seeded from,
// whether a final seed applied, and whether seeds from two different stores
// conflicted (#1563: a reader retires a cold row only when its own cold store
// is the one the cluster seeded from).
func (f *ClusterFSM) ColdMarkBackend() (backend string, seeded, conflict bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.coldMark == nil {
		return "", false, false
	}
	return f.coldMark.Backend, f.coldMark.Seeded, f.coldMark.BackendConflict
}

// ColdMarkStatus returns the flags and the nodes the fence waits for.
func (f *ClusterFSM) ColdMarkStatus() ColdMarkStatus {
	f.mu.RLock()
	defer f.mu.RUnlock()
	st := f.coldMark
	if st == nil {
		return ColdMarkStatus{}
	}
	out := ColdMarkStatus{
		Enabled: st.Enabled, Fenced: st.Fenced, Seeded: st.Seeded,
		BackendConflict: st.BackendConflict, Scopes: len(st.Marks),
	}
	if st.Enabled && !st.Fenced {
		for _, id := range st.AckWanted {
			if _, member := f.nodes[id]; member && !st.Acked[id] {
				out.AckPending = append(out.AckPending, id)
			}
		}
	}
	return out
}

// ColdMarkAcked reports whether nodeID acked.
func (f *ClusterFSM) ColdMarkAcked(nodeID string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.coldMark != nil && f.coldMark.Acked[nodeID]
}

func (f *ClusterFSM) snapshotColdMarkLocked() *coldMarkState {
	st := f.coldMark
	if st == nil || (!st.Enabled && len(st.Marks) == 0) {
		return nil
	}
	cp := *st
	cp.AckWanted = append([]string(nil), st.AckWanted...)
	cp.Acked = make(map[string]bool, len(st.Acked))
	for k, v := range st.Acked {
		cp.Acked[k] = v
	}
	cp.Marks = make(map[string]int32, len(st.Marks))
	for k, v := range st.Marks {
		cp.Marks[k] = v
	}
	cp.Alive = make(map[string]time.Time, len(st.Alive))
	for k, v := range st.Alive {
		cp.Alive[k] = v
	}
	cp.Raised = make(map[string]time.Time, len(st.Raised))
	for k, v := range st.Raised {
		cp.Raised[k] = v
	}
	return &cp
}

// restoreColdMarkLocked installs a snapshot's state, dropping invalid scopes.
func (f *ClusterFSM) restoreColdMarkLocked(st *coldMarkState) {
	if st == nil {
		f.coldMark = nil
		return
	}
	cp := *st
	if cp.Acked == nil {
		cp.Acked = map[string]bool{}
	}
	cp.Marks = make(map[string]int32, len(st.Marks))
	for scope, day := range st.Marks {
		if !validColdMarkScope(scope) || day < 0 {
			f.logger.Error().Str("scope", scope).Msg("Invalid cold mark scope in snapshot; refused")
			// A dropped scope could hide a cold object: stop trusting.
			cp.BackendConflict = true
			continue
		}
		cp.Marks[scope] = day
	}
	f.coldMark = &cp
}
