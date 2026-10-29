package raft

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// #1207 S3: the applied-index pair counts commands only and survives a
// snapshot, so a standby can compare its own against a primary's.
func TestFSMAppliedIndexCountsCommandsAndSurvivesSnapshot(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	if fsm.AppliedDone() != 0 || fsm.AppliedStarted() != 0 {
		t.Fatal("fresh FSM has an applied index")
	}
	applyBarrier(t, fsm, "tok-1", 7)
	if fsm.AppliedDone() != 7 || fsm.AppliedStarted() != 7 {
		t.Fatalf("after command 7: done=%d started=%d", fsm.AppliedDone(), fsm.AppliedStarted())
	}
	// A command that fails still counts: it was applied, with an error.
	fsm.Apply(&raft.Log{Index: 9, Data: []byte("not json")})
	if fsm.AppliedDone() != 9 {
		t.Fatalf("after a failed command 9: done=%d", fsm.AppliedDone())
	}

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snapshot.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	fsm2 := NewClusterFSM(zerolog.Nop())
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if fsm2.AppliedDone() != 9 || fsm2.AppliedStarted() != 9 {
		t.Fatalf("restored: done=%d started=%d, want 9", fsm2.AppliedDone(), fsm2.AppliedStarted())
	}
}
