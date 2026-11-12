package raft

import (
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
