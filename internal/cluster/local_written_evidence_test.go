package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
)

// The outbox tells the coordinator about each file this node flushed, before
// it is announced: the evidence of what the local file holds (#1182).
func TestOutboxReportsFlushedFiles(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	var got [][2]string
	r.SetLocalWritten(func(path, sha string) { got = append(got, [2]string{path, sha}) })
	r.RegisterFile("db", "m", "db/m/2026/10/15/00/f.parquet", time.Now(), 10, "abc")
	if len(got) != 1 || got[0] != [2]string{"db/m/2026/10/15/00/f.parquet", "abc"} {
		t.Fatalf("reported %v", got)
	}
}

type evidenceBridgeCoordinator struct {
	stubBridgeCoordinator
	written []string
}

func (e *evidenceBridgeCoordinator) NoteLocalWritten(path, _ string) {
	e.written = append(e.written, path)
}

// A compaction output registered through the bridge is reported first, on
// both registration paths.
func TestCompactionBridgeReportsOutputs(t *testing.T) {
	coord := &evidenceBridgeCoordinator{stubBridgeCoordinator: stubBridgeCoordinator{nodeID: "c-1"}}
	b := NewCompactionBridge(coord)
	file := compaction.CompactedFile{Path: "db/m/2026/10/15/00/out.parquet", SHA256: "abc", SizeBytes: 1}
	if err := b.RegisterCompactedFile(context.Background(), file); err != nil {
		t.Fatal(err)
	}
	if err := b.BatchFileOps(context.Background(), []compaction.CompactedFile{file}, nil); err != nil {
		t.Fatal(err)
	}
	if len(coord.written) != 2 {
		t.Fatalf("reported %v", coord.written)
	}
}
