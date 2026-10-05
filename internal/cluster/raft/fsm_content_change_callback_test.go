package raft

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The puller's #798 fix depends on the FSM firing the content-change callback
// AFTER the registration callback, for the same apply, and ONLY when the
// checksum or size changed. The puller tests simulate that pair by calling
// their own enqueue twice; this pins the producer, so a reorder or a dropped
// contentChanged in the FSM cannot pass CI with the fix silently off.
func TestFSMContentChangeCallbackFiresAfterRegisterAndOnlyOnContent(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	var events []string
	fsm.SetFileContentChangedCallback(func(e *FileEntry) { events = append(events, "content:"+e.SHA256) })
	fsm.SetFileCallbacks(func(e *FileEntry) { events = append(events, "register:"+e.SHA256) }, nil)

	createdAt := time.Now().UTC()
	file := func(sha string, size int64) FileEntry {
		return FileEntry{Path: "db/cpu/2026/10/05/12/a.parquet", Database: "db", Measurement: "cpu",
			CreatedAt: createdAt, SizeBytes: size, SHA256: sha}
	}
	check := func(step string, want ...string) {
		t.Helper()
		if len(events) != len(want) {
			t.Fatalf("%s: callbacks = %v, want %v", step, events, want)
		}
		for i := range want {
			if events[i] != want[i] {
				t.Fatalf("%s: callbacks = %v, want %v", step, events, want)
			}
		}
		events = nil
	}

	if r := fsm.applyRegisterFileStruct(RegisterFilePayload{File: file("aaa", 10)}, 1); r != nil {
		t.Fatal(r)
	}
	check("first register", "register:aaa")

	if r := fsm.applyRegisterFileStruct(RegisterFilePayload{File: file("aaa", 10)}, 2); r != nil {
		t.Fatal(r)
	}
	check("identical re-register", "register:aaa")

	if r := fsm.applyUpdateFileStruct(UpdateFilePayload{File: file("aaa", 10)}, 3); r != nil {
		t.Fatal(r)
	}
	check("update with unchanged content", "register:aaa")

	if r := fsm.applyUpdateFileStruct(UpdateFilePayload{File: file("bbb", 10)}, 4); r != nil {
		t.Fatal(r)
	}
	check("update with a new checksum, same size", "register:bbb", "content:bbb")

	if r := fsm.applyRegisterFileStruct(RegisterFilePayload{File: file("bbb", 11)}, 5); r != nil {
		t.Fatal(r)
	}
	check("re-register with a new size", "register:bbb", "content:bbb")

	// The batch path delivers the same pair, after the whole batch is applied.
	payload, err := json.Marshal(UpdateFilePayload{File: file("ccc", 11)})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := json.Marshal(BatchFileOpsPayload{Ops: []BatchFileOp{{Type: CommandUpdateFile, Payload: payload}}})
	if err != nil {
		t.Fatal(err)
	}
	if r := fsm.applyBatchFileOps(batch, 6); r != nil {
		t.Fatal(r)
	}
	check("batch update with a new checksum", "register:ccc", "content:ccc")

	// With the content callback unwired (shutdown), a content change still
	// registers and does not panic.
	fsm.SetFileContentChangedCallback(nil)
	if r := fsm.applyUpdateFileStruct(UpdateFilePayload{File: file("ddd", 11)}, 7); r != nil {
		t.Fatal(r)
	}
	check("content change with no content callback", "register:ddd")
}
