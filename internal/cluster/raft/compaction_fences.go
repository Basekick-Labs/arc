package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Read fences for staged compaction on a shared object store (#1852).
//
// Every node reads the bucket, so a committed object job must not change
// what any node's reads see until every node has finished the reads that
// started under the old view, and dropped the caches that may remember it.
// A node's read fence W says exactly that for every log entry at or below W:
// every read the node started before its FSM had applied W has ended, and it
// dropped its caches after applying every phase at or below W. One fence
// covers every job.
//
// A job committed while the flag is on is Acked and moves through phases;
// each step needs every node's fence at or above the current phase's index:
//
//	committed  reads: the inputs; the output left out by name. Then the
//	           finisher copies the output to its final key.
//	published  reads unchanged; caches dropped. Then reads switch.
//	switched   reads: the output; every input the manifest does not list is
//	           left out. Then the finisher deletes the input objects.
//	removed    reads unchanged; caches dropped. Then Finalize and Cleanup.
//	reverting  from committed or published, once the finisher deleted the
//	           output object (no staged object to copy): reads unchanged;
//	           caches dropped. Then RevertCompaction re-registers the inputs.
//
// While an Acked job is before removed, the FSM refuses every other manifest
// change to its output and inputs. A node the leader has not heard from for a
// while gets a waived fence from the leader: it lost its read lease, which
// cancels its reads, and it re-syncs (and drops its caches) before it reads
// again.
//
// Jobs committed while the flag was off (legacy) keep the earlier rule to
// their end: inputs left out once the output is seen at its final key.

// CapabilityCompactionFences is advertised by every build that applies the
// rules above and keeps read fences.
const CapabilityCompactionFences = "compaction_fences_v1"

// Phases of an Acked object job.
const (
	PhaseCommitted = "committed"
	PhasePublished = "published"
	PhaseSwitched  = "switched"
	PhaseRemoved   = "removed"
	PhaseReverting = "reverting"
)

// Conflict reasons of the fence commands.
const (
	CompactionFencesBehind = "fences-behind"
	CompactionWrongPhase   = "wrong-phase"
	CompactionNotAcked     = "not-acked"
)

// compactionPhaseNext lists the phases each phase may advance to.
var compactionPhaseNext = map[string][]string{
	PhaseCommitted: {PhasePublished, PhaseReverting},
	PhasePublished: {PhaseSwitched, PhaseReverting},
	PhaseSwitched:  {PhaseRemoved},
}

// EnableCompactionFencesPayload turns the flag on; see
// EnableStreamClaimsPayload for the Nodes rule.
type EnableCompactionFencesPayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

// ReadFencePayload raises Node's fence to Index. Waived: proposed by the
// leader for a node it has not heard from (never forwarded).
type ReadFencePayload struct {
	Node   string `json:"node"`
	Index  uint64 `json:"index"`
	Waived bool   `json:"waived,omitempty"`
}

// AdvanceCompactionPayload moves an Acked job from From to To. By must hold
// the job's finisher lease.
type AdvanceCompactionPayload struct {
	Job  string `json:"job"`
	By   string `json:"by"`
	From string `json:"from"`
	To   string `json:"to"`
}

// compactionFencesState is the snapshot form; nil until the flag was applied.
type compactionFencesState struct {
	Enabled bool              `json:"enabled"`
	Fences  map[string]uint64 `json:"fences,omitempty"`
}

func (f *ClusterFSM) applyEnableCompactionFences(payload []byte) interface{} {
	var p EnableCompactionFencesPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal enable compaction fences payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.compactionFences {
		return nil
	}
	if !f.compactionCommit {
		return errors.New("enable compaction fences: compaction commit is not enabled")
	}
	listed := map[string]bool{}
	for _, n := range p.Nodes {
		listed[n] = true
	}
	var missing []string
	for id := range f.nodes {
		if !listed[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("enable compaction fences: nodes %v are not known to support them", missing)
	}
	f.compactionFences = true
	f.logger.Info().Int("nodes", len(f.nodes)).Msg("Compaction read fences enabled")
	return nil
}

func (f *ClusterFSM) applyReadFence(payload []byte, logIndex uint64) interface{} {
	var p ReadFencePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal read fence payload: %w", err)
	}
	if p.Node == "" || p.Index == 0 || p.Index > logIndex {
		return fmt.Errorf("read fence: node %q index %d (log index %d)", p.Node, p.Index, logIndex)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.compactionFences {
		return errors.New("read fence: compaction fences are not enabled")
	}
	if _, known := f.nodes[p.Node]; !known {
		return fmt.Errorf("read fence: %q is not a member", p.Node)
	}
	if p.Index <= f.readFences[p.Node] {
		return nil // monotonic: an older fence changes nothing
	}
	f.readFences[p.Node] = p.Index
	if p.Waived {
		f.lastWaiver = logIndex
		f.logger.Warn().Str("node", p.Node).Uint64("index", p.Index).
			Msg("Read fence waived for a node the leader has not heard from: it cannot be reading")
		// The waived node itself acts as if it had lost its read lease
		// (a waiver of a node that was reading after all costs a read
		// blip, not wrong rows).
		if cb := f.onReadFenceWaived; cb != nil {
			node := p.Node
			f.pendingEvents = append(f.pendingEvents, func() { cb(node) })
		}
	}
	return nil
}

// fencesAtLocked reports whether every node in the table has a fence at or
// above index.
func (f *ClusterFSM) fencesAtLocked(index uint64) bool {
	for id := range f.nodes {
		if f.readFences[id] < index {
			return false
		}
	}
	return true
}

func (f *ClusterFSM) applyAdvanceCompaction(payload []byte, logIndex uint64) interface{} {
	var p AdvanceCompactionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal advance compaction payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.compactionFences {
		return errors.New("advance compaction: compaction fences are not enabled")
	}
	s, ok := f.staged[p.Job]
	if !ok {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionUnknownJob}
	}
	if !s.Acked || !s.Object || !s.Finalizing {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionNotAcked}
	}
	if s.Phase == p.To {
		return nil // a retry after it applied
	}
	if s.FinisherNode != p.By || p.By == "" {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionNotFinisher}
	}
	if s.Phase != p.From {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionWrongPhase}
	}
	allowed := false
	for _, to := range compactionPhaseNext[s.Phase] {
		allowed = allowed || to == p.To
	}
	if !allowed {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionWrongPhase}
	}
	// Into reverting changes nothing for reads (the inputs are read and the
	// output left out in committed and published): no fence needed.
	if p.To != PhaseReverting && !f.fencesAtLocked(s.PhaseIndex) {
		return &CompactionConflictError{Job: p.Job, Reason: CompactionFencesBehind}
	}
	s.Phase, s.PhaseIndex = p.To, logIndex
	return nil
}

// compactionFencedOwnsLocked reports whether an Acked job before removed owns
// path (its output or an input): every other manifest change to it is
// refused, so what reads leave out depends on the phase alone.
func (f *ClusterFSM) compactionFencedOwnsLocked(path string) bool {
	if !f.compactionFences {
		return false
	}
	job, ok := f.stagedPaths[path]
	if !ok {
		return false
	}
	s := f.staged[job]
	return s != nil && s.Acked && s.Finalizing && s.Phase != PhaseRemoved
}

// SetReadFenceWaivedCallback registers what runs after a waived fence
// applies (on every node; the callback checks whether it names itself). It
// must not block.
func (f *ClusterFSM) SetReadFenceWaivedCallback(cb func(node string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onReadFenceWaived = cb
}

// CompactionFencedOwns reports whether a fenced job before removed owns path.
func (f *ClusterFSM) CompactionFencedOwns(path string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.compactionFencedOwnsLocked(path)
}

// CompactionFencedOwnsUnder reports whether a fenced job before removed owns
// a path under prefix.
func (f *ClusterFSM) CompactionFencedOwnsUnder(prefix string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.compactionFences {
		return false
	}
	for _, s := range f.staged {
		if !s.Acked || !s.Finalizing || s.Phase == PhaseRemoved {
			continue
		}
		if strings.HasPrefix(s.Output.Path, prefix) {
			return true
		}
		for _, in := range s.Inputs {
			if strings.HasPrefix(in.Path, prefix) {
				return true
			}
		}
	}
	return false
}

// WaivedSince reports whether a fence was waived at or after index: the
// waived node may still be reading until it applies the waiver (it then
// drops its read lease), so a step after one waits a lease period first.
// Not in the snapshot: after a restore the wait is skipped once.
func (f *ClusterFSM) WaivedSince(index uint64) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastWaiver >= index && f.lastWaiver > 0
}

// FencesAt reports whether every node's read fence is at or above index.
func (f *ClusterFSM) FencesAt(index uint64) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.fencesAtLocked(index)
}

// ReadFence returns a node's fence.
func (f *ClusterFSM) ReadFence(node string) uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.readFences[node]
}

// FencesBehind returns the nodes whose fence is below index.
func (f *ClusterFSM) FencesBehind(index uint64) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []string
	for id := range f.nodes {
		if f.readFences[id] < index {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// CompactionFencesEnabled reports whether the flag is applied.
func (f *ClusterFSM) CompactionFencesEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.compactionFences
}

// FenceTop returns the highest phase index of an Acked finalizing object
// job, 0 when none: a node whose fence is below it has work.
func (f *ClusterFSM) FenceTop() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var top uint64
	for _, s := range f.staged {
		if s.Acked && s.Finalizing && s.PhaseIndex > top {
			top = s.PhaseIndex
		}
	}
	return top
}

func (f *ClusterFSM) snapshotCompactionFencesLocked() *compactionFencesState {
	if !f.compactionFences && len(f.readFences) == 0 {
		return nil
	}
	st := &compactionFencesState{Enabled: f.compactionFences}
	if len(f.readFences) > 0 {
		st.Fences = make(map[string]uint64, len(f.readFences))
		for k, v := range f.readFences {
			st.Fences[k] = v
		}
	}
	return st
}

func (f *ClusterFSM) restoreCompactionFencesLocked(st *compactionFencesState) {
	f.compactionFences, f.readFences = false, map[string]uint64{}
	if st == nil {
		return
	}
	f.compactionFences = st.Enabled
	for k, v := range st.Fences {
		f.readFences[k] = v
	}
}

// FenceCacheOutputs returns the outputs of Acked jobs whose phase, set at an
// index in (since, upto], needs a cache round on every node.
func (f *ClusterFSM) FenceCacheOutputs(since, upto uint64) []FileEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []FileEntry
	for _, s := range f.staged {
		if !s.Acked || !s.Finalizing || s.PhaseIndex <= since || s.PhaseIndex > upto {
			continue
		}
		switch s.Phase {
		case PhasePublished, PhaseRemoved, PhaseReverting:
			out = append(out, s.Output)
		}
	}
	return out
}
