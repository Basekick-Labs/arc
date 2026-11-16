package cluster

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #1728: the primary writer's cold-removal notice for the Iceberg exporter.

func noticeRig(t *testing.T, readerCaps []string) (*Coordinator, *pathRecorder) {
	t.Helper()
	c := originRig(t, "writer-1", RoleWriter)
	all := []string{raft.CapabilityIcebergNotice}
	capNodes(t, c, map[string][]string{"raft-1": all, "writer-1": all, "writer-2": all, "reader-1": readerCaps})
	rec := &pathRecorder{}
	rec.wire(c)
	c.raftNode.FSM().SetIcebergPathsCallback(c.onIcebergNotice)
	return c, rec
}

func TestMeasurementDirOf(t *testing.T) {
	for in, want := range map[string]string{
		"db/cpu/2026/11/16/14/a.parquet":       "db/cpu/",
		"spoke/db/cpu/2026/11/16/14/a.parquet": "spoke/db/cpu/",
		"/db/cpu/2026/11/16/14/a.parquet":      "db/cpu/",
		"db/cpu/a.parquet":                     "db/cpu/",
		"a.parquet":                            "",
		// Daily files, the common cold shape: one key per measurement.
		"db/cpu/2026/11/16/cpu_daily.parquet":       "db/cpu/",
		"spoke/db/cpu/2026/11/16/cpu_daily.parquet": "spoke/db/cpu/",
		// A measurement named like a year is not cut.
		"db/2026/2026/11/16/14/a.parquet": "db/2026/",
		// Already a measurement directory.
		"db/cpu/":       "db/cpu/",
		"spoke/db/cpu/": "spoke/db/cpu/",
	} {
		if got := measurementDirOf(in); got != want {
			t.Errorf("measurementDirOf(%q) = %q, want %q", in, got, want)
		}
	}
}

// Proposed once every node applies the notice; the exporter's cold callback
// gets one key per measurement directory.
func TestColdRemovalNoticeReachesTheExport(t *testing.T) {
	c, rec := noticeRig(t, []string{raft.CapabilityIcebergNotice})
	paths := []string{
		"db/cpu/2026/11/16/14/a.parquet", "db/cpu/2026/11/16/15/b.parquet",
		"db/mem/2026/11/16/14/c.parquet",
	}
	if err := c.ProposeIcebergColdRemoved(context.Background(), paths); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return rec.gotCold() == "db/cpu/,db/mem/" })
	if now, late := rec.got(); now != "" || late != "" {
		t.Fatalf("a cold notice went to the hot callbacks: now %q later %q", now, late)
	}
}

// A burst of removals is one notice, proposed after the delay.
func TestColdRemovalsShareOneNotice(t *testing.T) {
	old := coldNoticeDelay
	coldNoticeDelay = 150 * time.Millisecond
	t.Cleanup(func() { coldNoticeDelay = old })
	c, rec := noticeRig(t, []string{raft.CapabilityIcebergNotice})
	var mu sync.Mutex
	applied := 0
	c.raftNode.FSM().SetIcebergPathsCallback(func(k []string) { mu.Lock(); applied++; mu.Unlock(); c.onIcebergNotice(k) })
	c.NoteIcebergColdRemoved([]string{"db/cpu/2026/11/16/14/a.parquet"})
	time.Sleep(50 * time.Millisecond) // inside the delay
	if rec.gotCold() != "" {
		t.Fatal("proposed before the delay")
	}
	c.NoteIcebergColdRemoved([]string{"db/cpu/2026/11/16/15/b.parquet", "db/mem/2026/11/16/cpu_daily.parquet"})
	waitFSM(t, func() bool { return rec.gotCold() != "" })
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	n := applied
	mu.Unlock()
	if got := rec.gotCold(); got != "db/cpu/,db/mem/" || n != 1 {
		t.Fatalf("notices %d, keys %q; want one notice naming both measurements", n, got)
	}
}

// A node on an older build blocks the notice: nothing is proposed.
func TestColdRemovalNoticeWaitsForEveryNode(t *testing.T) {
	c, rec := noticeRig(t, nil)
	if err := c.ProposeIcebergColdRemoved(context.Background(), []string{"db/cpu/2026/11/16/14/a.parquet"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := rec.gotCold(); got != "" {
		t.Fatalf("proposed with a node that cannot apply it: %q", got)
	}
}

// Forwarded from an ingest role only.
func TestForwardApply_IcebergNotice(t *testing.T) {
	c, _ := noticeRig(t, []string{raft.CapabilityIcebergNotice})
	raw, _ := json.Marshal(raft.IcebergPathsChangedPayload{Keys: []string{"db/cpu/"}})
	cmd := &raft.Command{Type: raft.CommandIcebergPathsChanged, Payload: raw}
	if ack := forwardOnce(t, c, "writer-2", cmd); ack.Status != "ok" {
		t.Fatalf("from a writer: %+v", ack)
	}
	if ack := forwardOnce(t, c, "reader-1", cmd); ack.Status == "ok" {
		t.Fatalf("accepted from a reader: %+v", ack)
	}
	// Refused at the forward, before the log.
	bad, _ := json.Marshal(raft.IcebergPathsChangedPayload{Keys: []string{"../etc/"}})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandIcebergPathsChanged, Payload: bad}); ack.Status == "ok" {
		t.Fatalf("an invalid notice was accepted: %+v", ack)
	}
}

// The FSM observer is installed with the Raft node (every topology) and
// cleared at stop. Source-level: no test builds a coordinator through
// NewCoordinator; the cluster binary run exercises the path.
func TestIcebergNoticeObserverIsWired(t *testing.T) {
	src, err := os.ReadFile("coordinator.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"c.raftFSM.SetIcebergPathsCallback(c.onIcebergNotice)", "fsm.SetIcebergPathsCallback(nil)"} {
		if !strings.Contains(string(src), line) {
			t.Errorf("coordinator.go lacks %q", line)
		}
	}
}
