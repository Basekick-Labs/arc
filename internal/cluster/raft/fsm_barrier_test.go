package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func applyBarrier(t *testing.T, fsm *ClusterFSM, token string, index uint64) interface{} {
	t.Helper()
	payload, err := json.Marshal(BarrierPayload{Token: token, NodeID: "reader-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	data, err := json.Marshal(Command{Type: CommandBarrier, Payload: payload})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	return fsm.Apply(&raft.Log{Index: index, Data: data})
}

func TestFSMBarrier_RecordsTokenAtAppliedIndex(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	if _, ok := fsm.BarrierApplied("t1"); ok {
		t.Fatal("unknown token must not be reported as applied")
	}
	if res := applyBarrier(t, fsm, "t1", 41); res != nil {
		t.Fatalf("apply: %v", res)
	}
	idx, ok := fsm.BarrierApplied("t1")
	if !ok || idx != 41 {
		t.Fatalf("BarrierApplied(t1) = (%d, %v), want (41, true)", idx, ok)
	}
	if got := len(fsm.GetAllFiles()); got != 0 {
		t.Fatalf("a barrier must not touch the manifest, got %d files", got)
	}
}

func TestFSMBarrier_RejectsBadTokens(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	if res := applyBarrier(t, fsm, "", 1); res == nil {
		t.Fatal("empty token must be rejected")
	}
	if res := applyBarrier(t, fsm, strings.Repeat("x", MaxBarrierTokenLen+1), 2); res == nil {
		t.Fatal("over-long token must be rejected")
	}
	if res := applyBarrier(t, fsm, strings.Repeat("x", MaxBarrierTokenLen), 3); res != nil {
		t.Fatalf("token at the limit must be accepted: %v", res)
	}
}

func TestFSMBarrier_EvictsOldest(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	for i := 1; i <= maxBarriers+1; i++ {
		if res := applyBarrier(t, fsm, fmt.Sprintf("tok-%d", i), uint64(i)); res != nil {
			t.Fatalf("apply %d: %v", i, res)
		}
	}
	if _, ok := fsm.BarrierApplied("tok-1"); ok {
		t.Error("oldest barrier must be evicted past the cap")
	}
	if _, ok := fsm.BarrierApplied("tok-2"); !ok {
		t.Error("second barrier must survive")
	}
	if idx, ok := fsm.BarrierApplied(fmt.Sprintf("tok-%d", maxBarriers+1)); !ok || idx != uint64(maxBarriers+1) {
		t.Errorf("newest barrier: got (%d, %v)", idx, ok)
	}
}

// A follower that catches up by snapshot install never applies the barrier
// entry itself; the token has to travel inside the snapshot.
func TestFSMBarrier_SurvivesSnapshotRestore(t *testing.T) {
	fsm := NewClusterFSM(zerolog.Nop())
	applyBarrier(t, fsm, "before-snapshot", 9)
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snapshot.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	fsm2 := NewClusterFSM(zerolog.Nop())
	applyBarrier(t, fsm2, "stale-local", 3) // must be replaced, not merged
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if idx, ok := fsm2.BarrierApplied("before-snapshot"); !ok || idx != 9 {
		t.Fatalf("restored barrier: got (%d, %v), want (9, true)", idx, ok)
	}
	if _, ok := fsm2.BarrierApplied("stale-local"); ok {
		t.Fatal("Restore must replace the barrier map, not merge into it")
	}
	// Eviction order is rebuilt by index after a restore.
	for i := 0; i < maxBarriers; i++ {
		applyBarrier(t, fsm2, fmt.Sprintf("after-%d", i), uint64(100+i))
	}
	if _, ok := fsm2.BarrierApplied("before-snapshot"); ok {
		t.Fatal("the restored (oldest) barrier must be the first evicted")
	}
}
