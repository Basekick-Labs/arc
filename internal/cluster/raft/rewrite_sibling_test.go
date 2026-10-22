package raft

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestHasRewriteSibling(t *testing.T) {
	fsm := newTestFSM()
	const (
		orig = "db/cpu/2026/01/01/00/cpu_1.parquet"
		rw1  = "db/cpu/2026/01/01/00/cpu_1_rewrite_5.parquet"
		rw2  = "db/cpu/2026/01/01/00/cpu_1_rewrite_9.parquet"
		far  = "db/cpu/2026/01/01/01/cpu_1_rewrite_5.parquet"
	)
	reg := func(p string) {
		if r := fsm.Apply(&raft.Log{Data: makeCommand(t, CommandRegisterFile, RegisterFilePayload{File: FileEntry{Path: p, Database: "db", Measurement: "cpu", SHA256: "s", SizeBytes: 1, OriginNodeID: "w1", Tier: "hot", CreatedAt: time.Now()}})}); r != nil {
			t.Fatalf("register %s: %v", p, r)
		}
	}
	del := func(p string) {
		fsm.Apply(&raft.Log{Data: makeCommand(t, CommandDeleteFile, DeleteFilePayload{Path: p, Reason: "test"})})
	}
	reg(far)
	if fsm.HasRewriteSibling(orig) {
		t.Fatal("a rewrite in another hour is a sibling")
	}
	reg(rw1)
	if !fsm.HasRewriteSibling(orig) || fsm.HasRewriteSibling(rw1) || !fsm.HasRewriteSibling(rw2) {
		t.Fatal("rewrite registered: the original and another rewrite have a sibling, the rewrite alone does not")
	}
	del(rw1)
	if fsm.HasRewriteSibling(orig) {
		t.Fatal("a deleted rewrite still counts")
	}
	reg(orig)
	if !fsm.HasRewriteSibling(rw2) || fsm.HasRewriteSibling(orig) {
		t.Fatal("the registered original is a rewrite's sibling, not its own")
	}
	reg(rw2)

	// The index survives a snapshot install.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	fsm2 := newTestFSM()
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if !fsm2.HasRewriteSibling(rw1) {
		t.Fatal("restored: a registered rewrite is not found")
	}
}
