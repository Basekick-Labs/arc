package cluster

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// #676: the WAL is purged as soon as it is flushed, so a WAL file the
// republisher listed is routinely gone by the time it reads it. That is a
// skip: not a warning, and not an incomplete scan (which would withhold the
// verdict on batches with no WAL copy).
func TestRepublishSkipsAWALFilePurgedBeforeItIsRead(t *testing.T) {
	for _, at := range []string{"summary", "read"} {
		t.Run(at, func(t *testing.T) {
			r, inst, rp := republishRig(t)
			var logs bytes.Buffer
			rp.logger = zerolog.New(&logs)
			src := r.wal.CurrentFile()
			_ = r.wal.Rotate()
			raw, err := os.ReadFile(src)
			if err != nil {
				t.Fatal(err)
			}
			victim := src + ".copy.wal"
			if err := os.WriteFile(victim, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			hour := time.Now().UTC().Truncate(time.Hour)
			cands := map[uint64]map[string]map[uint64][]republishCand{inst: {"db/cpu": {
				2:  {{hour: hour, chunk: -1}},
				99: {{hour: hour, chunk: -1}}, // in no WAL file
			}}}
			if at == "read" {
				// Summarised while it existed, so the pass goes straight to
				// the full read.
				if !rp.fileMayHold(victim, cands) {
					t.Fatal("the copy holds batch 2")
				}
			}

			prev := republishScanFile
			t.Cleanup(func() { republishScanFile = prev })
			removed, victimScans := false, 0
			republishScanFile = func(path string, l zerolog.Logger, match func(wal.Coverage) bool, fn func(*wal.Entry) error) error {
				if path == victim {
					victimScans++
				}
				if path == victim && !removed {
					removed = true
					if err := os.Remove(victim); err != nil {
						t.Error(err)
					}
				}
				return prev(path, l, match, fn)
			}

			rp.republish(context.Background(), r.c.raftNode.FSM(), cands)
			if !removed {
				t.Fatal("the file was never read")
			}
			if victimScans != 1 {
				t.Fatalf("the purged file was opened %d times, want 1: a file gone at its summary is not read in full", victimScans)
			}
			if strings.Contains(logs.String(), "republish parked batches failed") {
				t.Fatalf("a purged WAL file was reported as a read failure: %s", logs.String())
			}
			if !rp.missing[republishBatch{inst, "db/cpu", 99}] {
				t.Fatal("the scan was treated as incomplete: a batch with no WAL copy got no verdict")
			}
			if rp.missing[republishBatch{inst, "db/cpu", 2}] {
				t.Fatal("batch 2 is in the remaining WAL file")
			}
		})
	}
}

// #676 L1: a batch registered by another writer during the pass has its WAL
// file purged, and no copy is found; that is not a missing copy, and no
// Error says it is. A batch still uncovered is.
func TestRepublishDoesNotCallARegisteredBatchMissing(t *testing.T) {
	r, inst, rp := republishRig(t)
	var logs bytes.Buffer
	rp.logger = zerolog.New(&logs)
	hour := time.Now().UTC().Truncate(time.Hour)
	// 96 and 97 are partial: chunk 0 of a batch split in two.
	p96 := raft.CoveragePiece{CSeq: 96, Chunk: 0, Chunks: 2}
	p97 := raft.CoveragePiece{CSeq: 97, Chunk: 0, Chunks: 2}
	cands := map[uint64]map[string]map[uint64][]republishCand{inst: {"db/cpu": {
		96: {{hour: hour, chunk: 0, piece: &p96}},
		97: {{hour: hour, chunk: 0, piece: &p97}},
		98: {{hour: hour, chunk: -1}},
		99: {{hour: hour, chunk: -1}},
	}}}
	// Batch 99 lands in a file another writer registered meanwhile.
	propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/other.parquet", SHA256: "o", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: hour, OriginNodeID: "writer-2", CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: inst, Key: "db/cpu", Ranges: [][2]uint64{{99, 99}}, Partial: []raft.CoveragePiece{p97}}}})
	waitFSM(t, func() bool { return len(r.c.raftNode.FSM().UncoveredBatches(inst, "db/cpu", 99, 99, hour)) == 0 })
	if !r.c.raftNode.FSM().CoverageCovered(inst, "db/cpu", p97, hour) {
		t.Fatal("the registered partial piece is not covered")
	}

	rp.republish(context.Background(), r.c.raftNode.FSM(), cands)
	if rp.missing[republishBatch{inst, "db/cpu", 99}] {
		t.Fatal("a batch registered elsewhere was marked as having no WAL copy")
	}
	if rp.missing[republishBatch{inst, "db/cpu", 97}] {
		t.Fatal("a partial piece registered elsewhere was marked as having no WAL copy")
	}
	for _, c := range []uint64{96, 98} {
		if !rp.missing[republishBatch{inst, "db/cpu", c}] {
			t.Fatalf("uncovered batch %d with no WAL copy got no verdict", c)
		}
	}
	if n := strings.Count(logs.String(), "no complete WAL copy"); n != 2 {
		t.Fatalf("%d missing-copy Errors, want 2 (batches 96 and 98)", n)
	}
}
