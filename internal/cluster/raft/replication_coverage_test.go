package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/replicaview"
	"github.com/rs/zerolog"
)

func TestReplicationRetirementSurvivesSnapshotAndCompresses(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	original := FileEntry{Path: "db/cpu/2026/10/01/01/file.parquet", Database: "db", Measurement: "cpu", CreatedAt: time.Now().UTC(), WALCoverage: []replicaview.PartitionCoverage{{Hour: 1, Coverage: replicaview.Coverage{{Instance: 9, First: 1, Last: 10}}}, {Hour: 2, Coverage: replicaview.Coverage{{Instance: 9, First: 1, Last: 10}}}}}
	if result := f.applyRegisterFileStruct(RegisterFilePayload{File: original}, 1); result != nil {
		t.Fatal(result)
	}
	// Caller-owned input and getter copies cannot mutate committed coverage.
	original.WALCoverage[0].Coverage[0].Last = 100
	copied, _ := f.GetFile(original.Path)
	copied.WALCoverage[0].Coverage[0].Last = 200
	if result := f.applyDeleteFileStruct(DeleteFilePayload{Path: original.Path, Reason: "retention:expired"}); result != nil {
		t.Fatal(result)
	}
	retirements := f.ReplicationRetirements()
	if len(retirements) != 2 || retirements[0].Coverage[0].Last != 10 {
		t.Fatalf("incorrect committed retirements: %+v", retirements)
	}
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s := snapshot.(*fsmSnapshot)
	encoded, err := json.Marshal(FSMSnapshot{ReplicationRetired: s.replicationRetired})
	if err != nil {
		t.Fatal(err)
	}
	restored := NewClusterFSM(zerolog.Nop())
	if err := restored.Restore(io.NopCloser(bytes.NewReader(encoded))); err != nil {
		t.Fatal(err)
	}
	if got := restored.ReplicationRetirements(); !reflect.DeepEqual(got, retirements) {
		t.Fatalf("snapshot lost deletion evidence: %v != %v", got, retirements)
	}
}

func TestCompactionAndTieringDoNotRetireStreamedData(t *testing.T) {
	for _, reason := range []string{"compaction", "compaction:job-1", "tiering:migrated"} {
		f := NewClusterFSM(zerolog.Nop())
		entry := FileEntry{Path: "db/cpu/2026/10/01/01/file.parquet", Database: "db", Measurement: "cpu", CreatedAt: time.Now().UTC(), WALCoverage: []replicaview.PartitionCoverage{{Hour: 1, Coverage: replicaview.Coverage{{Instance: 9, First: 1, Last: 10}}}}}
		if result := f.applyRegisterFileStruct(RegisterFilePayload{File: entry}, 1); result != nil {
			t.Fatal(result)
		}
		if result := f.applyDeleteFileStruct(DeleteFilePayload{Path: entry.Path, Reason: reason}); result != nil {
			t.Fatal(result)
		}
		if got := f.ReplicationRetirements(); len(got) != 0 {
			t.Fatalf("%s withdrew streamed rows before replacement arrived: %v", reason, got)
		}
	}
}
