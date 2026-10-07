package raft

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A batch is one Raft operation. Callbacks must observe the completed
// manifest, not an intermediate state between its individual file ops.
func TestFSMBatchFileOpsAtomicVisibilityIssue447(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	createdAt := time.Now().UTC()

	mustJSON := func(value interface{}) []byte {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	inputs := make([]string, 3)
	for i := range inputs {
		inputs[i] = fmt.Sprintf(
			"db/cpu/2026/09/19/03/input-%d.parquet", i,
		)

		file := FileEntry{
			Path:        inputs[i],
			Database:    "db",
			Measurement: "cpu",
			CreatedAt:   createdAt,
		}

		if result := fsm.applyRegisterFileStruct(
			RegisterFilePayload{File: file},
			uint64(i+1),
		); result != nil {
			t.Fatalf("seed input %d: %v", i, result)
		}
	}

	if got := len(fsm.GetFilesByDatabase("db")); got != 3 {
		t.Fatalf("initial file count = %d, want 3", got)
	}

	callbacks := 0

	// Installed AFTER seeding so only the batch invokes them. Both callbacks
	// read the FSM: that is how the compaction bridge's consumers behave, and
	// it also proves the callbacks run outside the manifest write lock.
	observe := func(what string) {
		callbacks++
		files := fsm.GetFilesByDatabase("db")
		if len(files) != 1 ||
			files[0].Path != "db/cpu/2026/09/19/03/output.parquet" {
			t.Errorf(
				"observed intermediate manifest in callback %d (%s): files=%+v",
				callbacks, what, files,
			)
		}
	}
	fsm.SetFileCallbacks(
		func(file *FileEntry) { observe("registered " + file.Path) },
		func(path, reason string) { observe("deleted " + path) },
	)

	// The compaction bridge's order: the output is registered first, then
	// the inputs are deleted (internal/cluster/compaction_bridge.go). On the
	// unfixed code the register callback therefore saw four files and each
	// delete callback one fewer.
	output := FileEntry{
		Path:        "db/cpu/2026/09/19/03/output.parquet",
		Database:    "db",
		Measurement: "cpu",
		CreatedAt:   createdAt,
	}
	ops := make([]BatchFileOp, 0, 4)
	ops = append(ops, BatchFileOp{
		Type: CommandRegisterFile,
		Payload: mustJSON(RegisterFilePayload{
			File: output,
		}),
	})
	for _, path := range inputs {
		ops = append(ops, BatchFileOp{
			Type: CommandDeleteFile,
			Payload: mustJSON(DeleteFilePayload{
				Path:   path,
				Reason: "compaction",
			}),
		})
	}

	if result := fsm.applyBatchFileOps(
		mustJSON(BatchFileOpsPayload{Ops: ops}),
		100,
	); result != nil {
		t.Fatalf("apply batch: %v", result)
	}

	if callbacks != 4 {
		t.Fatalf("callbacks = %d, want 4 (one register, three deletes)", callbacks)
	}

	files := fsm.GetFilesByDatabase("db")
	if len(files) != 1 || files[0].Path != output.Path {
		t.Fatalf("final manifest is incorrect: %+v", files)
	}
}
