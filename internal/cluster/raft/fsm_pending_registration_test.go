package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func TestPendingRegistrationIsReplicatedAndClearedAtomically(t *testing.T) {
	const path = "db/measurement/partition.parquet"
	intent := PendingRegistrationPayload{Path: path, OwnerID: "writer-1", RegistrationID: "registration-1"}
	set := makeCommand(t, CommandSetPendingRegistration, intent)
	writer, peer := newTestFSM(), newTestFSM()
	for i, fsm := range []*ClusterFSM{writer, peer} {
		if result := fsm.Apply(&raft.Log{Index: uint64(i + 1), Data: set}); result != nil {
			t.Fatalf("set intent on FSM %d: %v", i, result)
		}
	}
	if !peer.HasPendingRegistration(path) {
		t.Fatal("replicated intent is not visible to peer")
	}

	// A stale clear from another owner must not remove the writer's intent.
	wrongOwner := makeCommand(t, CommandClearPendingRegistration, PendingRegistrationPayload{
		Path: path, OwnerID: "writer-2", RegistrationID: intent.RegistrationID,
	})
	if result := peer.Apply(&raft.Log{Index: 3, Data: wrongOwner}); result != nil {
		t.Fatalf("apply wrong-owner clear: %v", result)
	}
	if !peer.HasPendingRegistration(path) {
		t.Fatal("clear from another owner removed the intent")
	}

	file := makeFileEntry(path, "db", "measurement", 12)
	register, err := json.Marshal(RegisterFilePayload{File: file})
	if err != nil {
		t.Fatal(err)
	}
	clear, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	batch := makeCommand(t, CommandBatchFileOps, BatchFileOpsPayload{Ops: []BatchFileOp{
		{Type: CommandRegisterFile, Payload: register},
		{Type: CommandClearPendingRegistration, Payload: clear},
	}})
	if result := peer.Apply(&raft.Log{Index: 4, Data: batch}); result != nil {
		t.Fatalf("apply register-and-clear batch: %v", result)
	}
	if _, ok := peer.GetFile(path); !ok {
		t.Fatal("registered file is missing")
	}
	if peer.HasPendingRegistration(path) {
		t.Fatal("intent remains after registration was applied")
	}
}

func TestPendingRegistrationsSurviveSnapshotRestore(t *testing.T) {
	const path = "db/measurement/pending.parquet"
	fsm := newTestFSM()
	data := makeCommand(t, CommandSetPendingRegistration, PendingRegistrationPayload{
		Path: path, OwnerID: "writer-1", RegistrationID: "registration-1",
	})
	if result := fsm.Apply(&raft.Log{Index: 1, Data: data}); result != nil {
		t.Fatal(result)
	}
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snapshot.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	restored := newTestFSM()
	if err := restored.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if !restored.HasPendingRegistration(path) {
		t.Fatal("pending intent was lost during snapshot restore")
	}
}

func TestPendingRegistrationIdentityCanBeReusedOnlyByOwner(t *testing.T) {
	fsm := newTestFSM()
	path := "db/measurement/pending.parquet"
	first := PendingRegistrationPayload{Path: path, OwnerID: "writer-1", RegistrationID: "same-id"}
	for i, p := range []PendingRegistrationPayload{
		first,
		{Path: path, OwnerID: "writer-2", RegistrationID: "same-id"},
	} {
		data := makeCommand(t, CommandSetPendingRegistration, p)
		if result := fsm.Apply(&raft.Log{Index: uint64(i + 1), Data: data}); result != nil {
			t.Fatal(result)
		}
	}
	clear := makeCommand(t, CommandClearPendingRegistration, first)
	if result := fsm.Apply(&raft.Log{Index: 3, Data: clear}); result != nil {
		t.Fatal(result)
	}
	if !fsm.HasPendingRegistration(path) {
		t.Fatal("clearing one owner's intent removed another owner's intent")
	}
	if got := fsm.GetPendingRegistrations(path); len(got) != 1 || got[0].OwnerID != "writer-2" {
		t.Fatalf("remaining intents = %#v, want only writer-2", got)
	}
}

func TestBatchRejectsInvalidPendingIntentBeforeRegisteringFiles(t *testing.T) {
	fsm := newTestFSM()
	path := "db/measurement/file.parquet"
	file, err := json.Marshal(RegisterFilePayload{File: makeFileEntry(path, "db", "measurement", 12)})
	if err != nil {
		t.Fatal(err)
	}
	invalidIntent, err := json.Marshal(PendingRegistrationPayload{Path: path, RegistrationID: "missing-owner"})
	if err != nil {
		t.Fatal(err)
	}
	data := makeCommand(t, CommandBatchFileOps, BatchFileOpsPayload{Ops: []BatchFileOp{
		{Type: CommandRegisterFile, Payload: file},
		{Type: CommandSetPendingRegistration, Payload: invalidIntent},
	}})
	if result := fsm.Apply(&raft.Log{Index: 1, Data: data}); result == nil {
		t.Fatal("batch with an invalid pending intent was accepted")
	}
	if _, exists := fsm.GetFile(path); exists {
		t.Fatal("file registration was applied before the invalid intent was rejected")
	}
}
