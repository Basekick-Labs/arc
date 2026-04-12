package cluster

// End-to-end integration test for Phase 4:
//
//   subprocess writes completion manifest to disk →
//   CompletionWatcher picks it up →
//   CompactionBridge translates to Raft command →
//   ClusterFSM applies the command →
//   onFileRegistered callback fires on the reader side
//
// This test does NOT boot a real Raft cluster — it applies commands
// directly to a ClusterFSM via the bridgeCoordinator stub, which is
// enough to exercise the real serialization, unmarshal, and FSM handler
// path. The unit tests for the watcher and bridge individually cover
// error cases; this test verifies the glue holds under the happy path
// and under ErrNotLeader retry.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/compaction"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// fsmCoordinator is a test-only bridgeCoordinator that forwards
// RegisterFileInManifest / DeleteFileFromManifest directly to a real
// raft.ClusterFSM via its Apply method. Using the real FSM lets us
// observe that onFileRegistered / onFileDeleted callbacks fire and the
// files map ends up in the expected state.
//
// This intentionally mirrors what a real leader coordinator does without
// needing to stand up the full Raft cluster. The FSM.Apply path is the
// same one exercised in production after Raft commits.
type fsmCoordinator struct {
	leader   bool
	nodeID   string
	fsm      *raft.ClusterFSM
	logIndex uint64 // incremented per Apply to mimic Raft's monotonic index
}

func (c *fsmCoordinator) IsLeader() bool      { return c.leader }
func (c *fsmCoordinator) LocalNodeID() string { return c.nodeID }

func (c *fsmCoordinator) RegisterFileInManifest(file raft.FileEntry) error {
	payload, err := json.Marshal(raft.RegisterFilePayload{File: file})
	if err != nil {
		return fmt.Errorf("marshal register payload: %w", err)
	}
	cmd := raft.Command{Type: raft.CommandRegisterFile, Payload: payload}
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	c.logIndex++
	if res := c.fsm.Apply(&hraft.Log{Index: c.logIndex, Data: data}); res != nil {
		if e, ok := res.(error); ok && e != nil {
			return e
		}
	}
	return nil
}

func (c *fsmCoordinator) DeleteFileFromManifest(path, reason string) error {
	payload, err := json.Marshal(raft.DeleteFilePayload{Path: path, Reason: reason})
	if err != nil {
		return fmt.Errorf("marshal delete payload: %w", err)
	}
	cmd := raft.Command{Type: raft.CommandDeleteFile, Payload: payload}
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	c.logIndex++
	if res := c.fsm.Apply(&hraft.Log{Index: c.logIndex, Data: data}); res != nil {
		if e, ok := res.(error); ok && e != nil {
			return e
		}
	}
	return nil
}

// waitFSMHasFile polls the FSM until it contains the given path or the
// deadline elapses. Reports failure with the final file list for debug.
func waitFSMHasFile(t *testing.T, fsm *raft.ClusterFSM, path string, timeout time.Duration) *raft.FileEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entry, ok := fsm.GetFile(path)
		if ok {
			return entry
		}
		time.Sleep(5 * time.Millisecond)
	}
	files := fsm.GetAllFiles()
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Path)
	}
	t.Fatalf("FSM did not see file %q within %v; manifest contents: %v", path, timeout, names)
	return nil
}

// TestPhase4_CompletionManifestToFSM is the happy-path integration test:
// a subprocess writes a completion manifest in state sources_deleted, the
// watcher picks it up via the real CompactionBridge, and the FSM ends up
// with the compacted file registered and the sources deleted.
func TestPhase4_CompletionManifestToFSM(t *testing.T) {
	dir := t.TempDir()
	fsm := raft.NewClusterFSM(zerolog.Nop())
	coord := &fsmCoordinator{leader: true, nodeID: "compactor-1", fsm: fsm}
	bridge := NewCompactionBridge(coord)

	// Pre-seed the sources in the manifest so the DeleteFile path has
	// something to remove. This mirrors what would happen in production:
	// the compactor previously registered the source files (via the flush
	// path) and is now replacing them with a compacted output.
	seedSource := func(path string) {
		payload, _ := json.Marshal(raft.RegisterFilePayload{File: raft.FileEntry{
			Path:          path,
			SHA256:        "src-" + path,
			SizeBytes:     100,
			Database:      "db",
			Measurement:   "cpu",
			PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
			OriginNodeID:  "writer-1",
			Tier:          "hot",
			CreatedAt:     time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		}})
		cmd, _ := json.Marshal(raft.Command{Type: raft.CommandRegisterFile, Payload: payload})
		coord.logIndex++
		fsm.Apply(&hraft.Log{Index: coord.logIndex, Data: cmd})
	}
	seedSource("db/cpu/2026/04/11/14/src_a.parquet")
	seedSource("db/cpu/2026/04/11/14/src_b.parquet")

	// Drop a completion manifest on disk — simulates what the subprocess
	// would have written after a successful compaction job.
	manifest := &compaction.CompletionManifest{
		JobID:         "p4_happy_1",
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: "db/cpu/2026/04/11/14",
		Tier:          "hot",
		State:         compaction.CompletionStateSourcesDeleted,
		Outputs: []compaction.CompactedOutput{
			{
				Path:          "db/cpu/2026/04/11/14/compacted_out.parquet",
				SHA256:        "deadbeef",
				SizeBytes:     5000,
				Database:      "db",
				Measurement:   "cpu",
				PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
				Tier:          "hot",
				CreatedAt:     time.Date(2026, 4, 11, 14, 30, 0, 0, time.UTC),
			},
		},
		DeletedSources: []string{
			"db/cpu/2026/04/11/14/src_a.parquet",
			"db/cpu/2026/04/11/14/src_b.parquet",
		},
		CreatedAt: time.Date(2026, 4, 11, 14, 30, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 4, 11, 14, 30, 0, 0, time.UTC),
	}
	writeManifest(t, dir, manifest)

	// Start the watcher against the real bridge.
	watcher, err := compaction.NewCompletionWatcher(compaction.CompletionWatcherConfig{
		Dir:          dir,
		Bridge:       bridge,
		PollInterval: 10 * time.Millisecond,
		ApplyTimeout: 500 * time.Millisecond,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCompletionWatcher: %v", err)
	}
	watcher.Start(context.Background())
	defer watcher.Stop()

	// Assert the compacted file landed in the FSM.
	registered := waitFSMHasFile(t, fsm, "db/cpu/2026/04/11/14/compacted_out.parquet", 2*time.Second)
	if registered.SHA256 != "deadbeef" {
		t.Errorf("registered SHA256: got %q, want deadbeef", registered.SHA256)
	}
	if registered.OriginNodeID != "compactor-1" {
		t.Errorf("OriginNodeID: got %q, want compactor-1 (bridge must stamp local node ID)", registered.OriginNodeID)
	}
	if registered.SizeBytes != 5000 {
		t.Errorf("SizeBytes: got %d, want 5000", registered.SizeBytes)
	}

	// Assert the sources were removed from the FSM.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, hasA := fsm.GetFile("db/cpu/2026/04/11/14/src_a.parquet")
		_, hasB := fsm.GetFile("db/cpu/2026/04/11/14/src_b.parquet")
		if !hasA && !hasB {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := fsm.GetFile("db/cpu/2026/04/11/14/src_a.parquet"); ok {
		t.Errorf("src_a should have been deleted")
	}
	if _, ok := fsm.GetFile("db/cpu/2026/04/11/14/src_b.parquet"); ok {
		t.Errorf("src_b should have been deleted")
	}

	// Completion manifest should have been removed from disk.
	entries, _ := listJSONPaths(dir)
	if len(entries) != 0 {
		t.Errorf("completion manifest should be removed after successful apply, found: %v", entries)
	}
}

// TestPhase4_NotLeaderKeepsManifest drives the bridge against a
// non-leader fsmCoordinator and asserts the watcher retries until the
// coordinator becomes leader.
func TestPhase4_NotLeaderKeepsManifestUntilLeaderFlap(t *testing.T) {
	dir := t.TempDir()
	fsm := raft.NewClusterFSM(zerolog.Nop())
	// Start as NOT leader — watcher should keep the manifest.
	coord := &fsmCoordinator{leader: false, nodeID: "compactor-1", fsm: fsm}
	bridge := NewCompactionBridge(coord)

	manifest := &compaction.CompletionManifest{
		JobID:         "p4_flap_1",
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: "db/cpu/2026/04/11/14",
		Tier:          "hot",
		State:         compaction.CompletionStateSourcesDeleted,
		Outputs: []compaction.CompactedOutput{
			{
				Path:          "db/cpu/2026/04/11/14/compacted_flap.parquet",
				SHA256:        "flappy",
				SizeBytes:     1000,
				Database:      "db",
				Measurement:   "cpu",
				PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
				Tier:          "hot",
				CreatedAt:     time.Date(2026, 4, 11, 14, 30, 0, 0, time.UTC),
			},
		},
		DeletedSources: []string{},
		CreatedAt:      time.Date(2026, 4, 11, 14, 30, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2026, 4, 11, 14, 30, 0, 0, time.UTC),
	}
	writeManifest(t, dir, manifest)

	watcher, err := compaction.NewCompletionWatcher(compaction.CompletionWatcherConfig{
		Dir:          dir,
		Bridge:       bridge,
		PollInterval: 10 * time.Millisecond,
		ApplyTimeout: 500 * time.Millisecond,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewCompletionWatcher: %v", err)
	}
	watcher.Start(context.Background())
	defer watcher.Stop()

	// Give the watcher several ticks to observe the manifest under the
	// not-leader condition. The FSM must NOT see the file during this
	// window because the bridge short-circuits with ErrNotLeader.
	time.Sleep(100 * time.Millisecond)
	if _, ok := fsm.GetFile("db/cpu/2026/04/11/14/compacted_flap.parquet"); ok {
		t.Fatal("file must not appear in FSM while bridge reports not-leader")
	}
	// Manifest must still exist on disk.
	entries, _ := listJSONPaths(dir)
	if len(entries) != 1 {
		t.Fatalf("manifest should still exist during leader-flap, got %d entries", len(entries))
	}

	// Flip to leader and expect the retry to succeed within a few ticks.
	coord.leader = true
	waitFSMHasFile(t, fsm, "db/cpu/2026/04/11/14/compacted_flap.parquet", 2*time.Second)

	// Manifest should have been cleaned up after the successful apply.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := listJSONPaths(dir)
		if len(entries) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	entries, _ = listJSONPaths(dir)
	if len(entries) != 0 {
		t.Errorf("completion manifest should be removed after successful post-flap apply, found %d", len(entries))
	}
}

// --- helpers ---

// writeManifest serializes m and writes it atomically to
// dir/<JobID>.json. Uses the same tmp-file-plus-rename pattern as the
// production writeCompletionManifest (which is unexported and cannot be
// called from outside the compaction package).
func writeManifest(t *testing.T, dir string, m *compaction.CompletionManifest) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	final := filepath.Join(dir, m.JobID+".json")
	tmp := final + ".tmp"
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

// listJSONPaths returns all *.json files in dir (no .tmp) for assertions.
func listJSONPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".json") {
			out = append(out, filepath.Join(dir, name))
		}
	}
	return out, nil
}
