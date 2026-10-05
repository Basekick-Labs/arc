package raft

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestFSMBatchFileOpsReadersNeverSeeAPartialBatch is #447's own test shape: a
// reader spins on GetFilesByDatabase while compaction-shaped batches (the
// output registered first, then the inputs deleted, as the bridge orders them)
// are applied. Only reads that begin AND end inside an apply are judged; the
// two states they may legitimately see are the pre-batch manifest (all inputs)
// and the post-batch one (the output alone). Anything else is a half-applied
// batch. On the unfixed code roughly four in five such reads saw one.
//
// The callbacks are judged too, and they are deterministic: each must find the
// completed manifest. That half of the test fails on the unfixed code on every
// run, whatever the scheduler does to the reader.
func TestFSMBatchFileOpsReadersNeverSeeAPartialBatch(t *testing.T) {
	const inputs = 50
	const rounds = 20

	fsm := NewClusterFSM(zerolog.Nop())
	createdAt := time.Now().UTC()
	mustJSON := func(v interface{}) []byte {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	var inBatch, stop atomic.Bool
	var inWindow, partialReads atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			before := inBatch.Load()
			n := len(fsm.GetFilesByDatabase("db"))
			after := inBatch.Load()
			if !(before && after) {
				continue
			}
			inWindow.Add(1)
			if n != inputs && n != 1 {
				partialReads.Add(1)
			}
		}
	}()

	var partialCallbacks atomic.Int64
	observe := func() {
		if len(fsm.GetFilesByDatabase("db")) != 1 {
			partialCallbacks.Add(1)
		}
	}

	for r := 0; r < rounds; r++ {
		paths := make([]string, inputs)
		for i := range paths {
			paths[i] = fmt.Sprintf("db/cpu/2026/09/19/03/r%d-in-%d.parquet", r, i)
			res := fsm.applyRegisterFileStruct(RegisterFilePayload{File: FileEntry{
				Path: paths[i], Database: "db", Measurement: "cpu", CreatedAt: createdAt,
			}}, uint64(r*1000+i+1))
			if res != nil {
				t.Fatalf("seed: %v", res)
			}
		}
		out := fmt.Sprintf("db/cpu/2026/09/19/03/r%d-out.parquet", r)
		ops := []BatchFileOp{{Type: CommandRegisterFile, Payload: mustJSON(RegisterFilePayload{File: FileEntry{
			Path: out, Database: "db", Measurement: "cpu", CreatedAt: createdAt,
		}})}}
		for _, p := range paths {
			ops = append(ops, BatchFileOp{Type: CommandDeleteFile, Payload: mustJSON(DeleteFilePayload{Path: p, Reason: "compaction"})})
		}
		payload := mustJSON(BatchFileOpsPayload{Ops: ops})

		// Callbacks only around the batch: the seeding and the reset below
		// would otherwise be judged against a manifest that is not "one file".
		fsm.SetFileCallbacks(func(*FileEntry) { observe() }, func(string, string) { observe() })
		inBatch.Store(true)
		res := fsm.applyBatchFileOps(payload, uint64(r*1000+999))
		inBatch.Store(false)
		fsm.SetFileCallbacks(nil, nil)
		if res != nil {
			t.Fatalf("apply batch: %v", res)
		}
		if res := fsm.applyDeleteFileStruct(DeleteFilePayload{Path: out, Reason: "test-reset"}); res != nil {
			t.Fatalf("reset: %v", res)
		}
	}
	stop.Store(true)
	wg.Wait()

	if n := partialCallbacks.Load(); n != 0 {
		t.Errorf("%d callbacks observed a half-applied batch", n)
	}
	t.Logf("reads inside an apply: %d", inWindow.Load())
	if n := partialReads.Load(); n != 0 {
		t.Errorf("%d of %d reads inside an apply observed a half-applied batch", n, inWindow.Load())
	}
}
