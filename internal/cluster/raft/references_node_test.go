package raft

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1619: ReferencesNode is the FSM half of "is this a member" for the operator
// removal. Each case puts the ID in exactly one structure; a structure
// applyRemoveNode acts on must count, history a removal leaves alone must not.
func TestReferencesNodeSources(t *testing.T) {
	const id = "n-1"
	cases := []struct {
		name string
		set  func(f *ClusterFSM)
		want bool
	}{
		{"nothing", func(f *ClusterFSM) {}, false},
		{"node table", func(f *ClusterFSM) { f.nodes[id] = &NodeInfo{ID: id} }, true},
		{"primary writer", func(f *ClusterFSM) { f.primaryWriterID = id }, true},
		{"compactor lease", func(f *ClusterFSM) { f.activeCompactorID = id }, true},
		{"pause ack", func(f *ClusterFSM) { f.compactionPause.Acks = map[string]uint64{id: 1} }, true},
		{"staged origin", func(f *ClusterFSM) { f.staged = map[string]*StagedCompaction{"j": {Job: "j", Origin: id}} }, true},
		{"staged holder", func(f *ClusterFSM) {
			f.staged = map[string]*StagedCompaction{"j": {Job: "j", Origin: "other", Holders: []string{"x", id}}}
		}, true},
		{"cold mark alive", func(f *ClusterFSM) { f.coldMark = &coldMarkState{Alive: map[string]time.Time{id: time.Now()}} }, true},
		{"cold mark acked", func(f *ClusterFSM) { f.coldMark = &coldMarkState{Acked: map[string]bool{id: true}} }, true},
		{"departed", func(f *ClusterFSM) { f.takeover.departed[id] = time.Now() }, true},
		{"current instance", func(f *ClusterFSM) { f.takeover.current[id] = 7 }, true},
		{"live instance origin", func(f *ClusterFSM) {
			f.takeover.instances[7] = &StreamInstance{Instance: 7, Origin: id, Status: InstanceOrphaned}
		}, true},
		{"active claim", func(f *ClusterFSM) { f.takeover.claims[7] = &StreamClaim{Instance: 7, By: id} }, true},
		// An instance an earlier removal already orphaned: the removal acted.
		{"orphaned instance of a removed origin", func(f *ClusterFSM) {
			f.takeover.instances[7] = &StreamInstance{Instance: 7, Origin: id, Status: InstanceOrphaned, OriginGone: true}
		}, false},
		// History a removal does not touch.
		{"retired instance origin", func(f *ClusterFSM) {
			f.takeover.instances[7] = &StreamInstance{Instance: 7, Origin: id, Status: InstanceRetired}
		}, false},
		{"finished claim", func(f *ClusterFSM) { f.takeover.claims[7] = &StreamClaim{Instance: 7, By: id, Done: true} }, false},
		{"released claim", func(f *ClusterFSM) { f.takeover.claims[7] = &StreamClaim{Instance: 7, By: id, Released: true} }, false},
		{"file origin", func(f *ClusterFSM) { f.files["p"] = &FileEntry{Path: "p", OriginNodeID: id} }, false},
		{"cold mark ack wanted", func(f *ClusterFSM) { f.coldMark = &coldMarkState{AckWanted: []string{id}} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewClusterFSM(zerolog.Nop())
			tc.set(f)
			if got := f.ReferencesNode(id); got != tc.want {
				t.Errorf("ReferencesNode = %v, want %v", got, tc.want)
			}
		})
	}
}

// The other half of the contract: after an operator removal of id, nothing
// ReferencesNode counts still names it, so a retry answers 404 instead of
// re-proposing. Every source that applyRemoveNode clears is set for id.
//
// Deliberate exceptions, which removal does not clear and ReferencesNode still
// counts (conservative: a retry is a no-op 200): the compactor lease and the
// pause acks. They are set to another node here and checked untouched.
func TestReferencesNodeFalseAfterRemoval(t *testing.T) {
	const id, other = "n-1", "n-2"
	f := NewClusterFSM(zerolog.Nop())
	f.streamClaims = true
	f.nodes[id] = &NodeInfo{ID: id, Role: "writer"}
	f.nodes[other] = &NodeInfo{ID: other, Role: "writer"}
	f.primaryWriterID = id
	f.activeCompactorID = other
	f.compactionPause.Acks = map[string]uint64{other: 1}
	f.staged["j1"] = &StagedCompaction{Job: "j1", Origin: id}
	f.staged["j2"] = &StagedCompaction{Job: "j2", Origin: other, Holders: []string{id, other}}
	f.coldMark = &coldMarkState{Alive: map[string]time.Time{id: time.Now()}, Acked: map[string]bool{id: true}}
	f.takeover.departed[id] = time.Now()
	f.takeover.current[id] = 7
	f.takeover.instances[7] = &StreamInstance{Instance: 7, Origin: id, Status: InstanceCurrent}
	f.takeover.instances[8] = &StreamInstance{Instance: 8, Origin: id, Status: InstanceClosed}
	f.takeover.claims[9] = &StreamClaim{Instance: 9, Origin: other, By: id}

	if !f.ReferencesNode(id) {
		t.Fatal("ReferencesNode = false before the removal")
	}
	raw, _ := json.Marshal(RemoveNodePayload{NodeID: id})
	if err, _ := f.applyRemoveNode(raw).(error); err != nil {
		t.Fatalf("applyRemoveNode: %v", err)
	}
	if f.ReferencesNode(id) {
		t.Errorf("ReferencesNode = true after the operator removal: a retry would re-propose instead of answering 404")
	}
	if f.activeCompactorID != other || f.compactionPause.Acks[other] != 1 {
		t.Errorf("the removal touched another node's lease or pause ack")
	}
}
