package cluster

// End-to-end integration tests for Phase 3 peer file replication catch-up.
//
// These tests wire the real handleFetchFile server (via startOriginServer)
// to the real filereplication.FetchClient + Puller + RunCatchUp path. They
// reuse the helpers from filereplication_integration_test.go — memBackend,
// seedFileInFSM, originServer — so a change to the Phase 2 integration
// surface is a compile-time break here.
//
// What Phase 3 catch-up must do:
//   1. On startup (simulated by calling Puller.RunCatchUp with the full
//      FSM snapshot), enqueue every manifest entry whose local backend
//      copy is missing.
//   2. Try the multi-peer fallback list in order — origin first, then
//      healthy peers — until one responds with the file bytes.
//   3. Stop trying other peers on checksum mismatch (data integrity
//      short-circuit).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// seedMultipleFilesInFSM applies N register-file commands to a fresh FSM.
// Unlike seedFileInFSM (which hardcodes index=1), this uses distinct indices
// so the FSM accepts all of them. Returns the assembled entries for use as
// the catch-up walker input.
func seedMultipleFilesInFSM(t *testing.T, fsm *raft.ClusterFSM, entries []raft.FileEntry) []*raft.FileEntry {
	t.Helper()
	out := make([]*raft.FileEntry, 0, len(entries))
	for i, entry := range entries {
		payload, err := json.Marshal(raft.RegisterFilePayload{File: entry})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		cmd := raft.Command{Type: raft.CommandRegisterFile, Payload: payload}
		data, err := json.Marshal(cmd)
		if err != nil {
			t.Fatalf("marshal command: %v", err)
		}
		result := fsm.Apply(&hraft.Log{Index: uint64(i + 1), Data: data})
		if err, ok := result.(error); ok && err != nil {
			t.Fatalf("fsm.Apply entry %d: %v", i, err)
		}
		entryCopy := entry
		out = append(out, &entryCopy)
	}
	return out
}

// makeFileEntry builds a FileEntry whose SHA256 is the actual hash of the
// body bytes — required for integration tests because the real fetch client
// verifies the body against the entry's SHA256 field. The unit-level helper
// makeEntry in filereplication/puller_test.go stubs the hash as "deadbeef"
// because its fake fetcher skips verification; keeping these helpers
// separate avoids dragging body-hashing into every puller unit test.
func makeFileEntry(path string, body []byte, originID string) raft.FileEntry {
	sum := sha256.Sum256(body)
	return raft.FileEntry{
		Path:          path,
		SHA256:        hex.EncodeToString(sum[:]),
		SizeBytes:     int64(len(body)),
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		OriginNodeID:  originID,
		Tier:          "hot",
		CreatedAt:     time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC),
	}
}

// buildCatchUpPuller constructs a puller wired to a fresh backend and a
// resolver that returns the given peer addresses in order. Returns the
// puller, its backend, and a stop function.
func buildCatchUpPuller(t *testing.T, selfID string, peerAddrs []string) (*filereplication.Puller, *memBackend, func()) {
	t.Helper()
	backend := newMemBackend()
	fetchClient, err := filereplication.NewFetchClient(filereplication.FetchClient{
		SelfNodeID:   selfID,
		ClusterName:  "test-cluster",
		SharedSecret: "catchup-secret",
		DialTimeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFetchClient: %v", err)
	}

	resolver := filereplication.NewRegistryResolver(func(_, _ string) []string {
		return peerAddrs
	})

	p, err := filereplication.New(filereplication.Config{
		SelfNodeID:            selfID,
		Backend:               backend,
		Fetcher:               fetchClient,
		PeerResolver:          resolver,
		Workers:               2,
		QueueSize:             32,
		RetryMaxAttempts:      2,
		RetryInitialBackoff:   10 * time.Millisecond,
		FetchTimeout:          5 * time.Second,
		CatchUpQueueHighWater: 0.8,
		Logger:                zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New puller: %v", err)
	}
	p.Start(context.Background())
	return p, backend, p.Stop
}

// waitForCatchUp polls puller stats until pulled + failed reaches target,
// or the deadline expires. Returns the final stats.
func waitForCatchUp(t *testing.T, p *filereplication.Puller, target int64) map[string]int64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := p.Stats()
		if s["pulled"]+s["failed"] >= target {
			return s
		}
		time.Sleep(20 * time.Millisecond)
	}
	return p.Stats()
}

// --- Test 1: happy path — catch-up pulls all missing files from origin ---

func TestPhase3CatchUpHappyPath(t *testing.T) {
	const (
		originID = "writer-1"
		readerID = "reader-1"
	)

	// Seed 10 files into the origin's backend and FSM.
	originBackend := newMemBackend()
	originFSM := raft.NewClusterFSM(zerolog.Nop())
	rawEntries := make([]raft.FileEntry, 10)
	for i := range rawEntries {
		body := []byte(fmt.Sprintf("file-%02d body bytes here", i))
		path := fmt.Sprintf("testdb/cpu/2026/04/11/18/catchup-%02d.parquet", i)
		if err := originBackend.Write(context.Background(), path, body); err != nil {
			t.Fatalf("seed backend: %v", err)
		}
		rawEntries[i] = makeFileEntry(path, body, originID)
	}
	entries := seedMultipleFilesInFSM(t, originFSM, rawEntries)

	origin := startOriginServer(t, originBackend, originFSM, "catchup-secret", "test-cluster", originID)
	defer origin.stop()

	// Build the reader's puller pointing at the origin.
	puller, readerBackend, stop := buildCatchUpPuller(t, readerID, []string{origin.addr()})
	defer stop()

	// Run catch-up with the full manifest — every entry is missing locally.
	puller.RunCatchUp(context.Background(), entries)

	stats := waitForCatchUp(t, puller, 10)
	if stats["pulled"] != 10 {
		t.Errorf("pulled: got %d, want 10 (stats=%+v)", stats["pulled"], stats)
	}
	if stats["failed"] != 0 {
		t.Errorf("failed: got %d, want 0", stats["failed"])
	}
	if stats["catchup_entries_walked"] != 10 {
		t.Errorf("catchup_entries_walked: got %d, want 10", stats["catchup_entries_walked"])
	}
	if stats["catchup_enqueued"] != 10 {
		t.Errorf("catchup_enqueued: got %d, want 10", stats["catchup_enqueued"])
	}
	if stats["catchup_completed_at"] == 0 {
		t.Errorf("catchup_completed_at should be non-zero")
	}

	// Every file should now exist in the reader's backend with correct bytes.
	for _, e := range rawEntries {
		got, err := readerBackend.Read(context.Background(), e.Path)
		if err != nil {
			t.Errorf("reader backend missing %s: %v", e.Path, err)
			continue
		}
		sum := sha256.Sum256(got)
		if hex.EncodeToString(sum[:]) != e.SHA256 {
			t.Errorf("reader hash mismatch for %s", e.Path)
		}
	}
}

// --- Test 2: origin absent, fallback succeeds via any-peer ---
//
// This is the Kubernetes rotation case: the original writer pod is gone,
// but a sibling peer still holds the files. Catch-up must succeed via the
// fallback resolver. Simulated here by leaving the "origin" slot empty and
// only passing a non-origin peer address to the resolver.

func TestPhase3CatchUpOriginAbsentFallbackSucceeds(t *testing.T) {
	const (
		originID = "writer-1" // the long-gone original writer
		peerID   = "peer-2"   // the fallback peer that still has the files
		readerID = "reader-1"
	)

	// Seed the files only on peer-2. Origin (writer-1) has no backend here —
	// it was scheduled to a different pod and is gone.
	peerBackend := newMemBackend()
	peerFSM := raft.NewClusterFSM(zerolog.Nop())
	rawEntries := []raft.FileEntry{
		makeFileEntry("testdb/cpu/rotate-a.parquet", []byte("rotate-a body"), originID),
		makeFileEntry("testdb/cpu/rotate-b.parquet", []byte("rotate-b body bytes"), originID),
	}
	for _, e := range rawEntries {
		body := []byte(fmt.Sprintf("unused")) // filled below
		switch e.Path {
		case "testdb/cpu/rotate-a.parquet":
			body = []byte("rotate-a body")
		case "testdb/cpu/rotate-b.parquet":
			body = []byte("rotate-b body bytes")
		}
		if err := peerBackend.Write(context.Background(), e.Path, body); err != nil {
			t.Fatalf("seed peer backend: %v", err)
		}
	}
	entries := seedMultipleFilesInFSM(t, peerFSM, rawEntries)

	// peer-2 serves fetches. The resolver won't even mention origin — the
	// coordinator's registry-backed resolver would have already filtered it
	// out as unhealthy.
	peer := startOriginServer(t, peerBackend, peerFSM, "catchup-secret", "test-cluster", peerID)
	defer peer.stop()

	puller, readerBackend, stop := buildCatchUpPuller(t, readerID, []string{peer.addr()})
	defer stop()

	puller.RunCatchUp(context.Background(), entries)

	stats := waitForCatchUp(t, puller, 2)
	if stats["pulled"] != 2 {
		t.Errorf("pulled: got %d, want 2 — fallback from absent origin to peer-2 should succeed (stats=%+v)", stats["pulled"], stats)
	}
	for _, e := range rawEntries {
		if _, err := readerBackend.Read(context.Background(), e.Path); err != nil {
			t.Errorf("reader backend missing %s after fallback: %v", e.Path, err)
		}
	}
}

// --- Test 3: no peer has the file ---
//
// Both candidate peers respond "file not found on local backend". The
// puller should try every peer per attempt, exhaust retries, and count
// the entry as failed. Reader backend stays empty.

func TestPhase3CatchUpNoPeerHasFile(t *testing.T) {
	const (
		originID = "writer-1"
		readerID = "reader-1"
	)

	// Empty backends — neither peer has the files, even though the FSM
	// manifest knows about them.
	backend1 := newMemBackend()
	backend2 := newMemBackend()

	// Both peers share the same FSM manifest contents (so the path is
	// known), but the files don't exist on disk, so handleFetchFile
	// responds with Code="not_found" via the existence check.
	rawEntries := []raft.FileEntry{
		makeFileEntry("testdb/cpu/missing.parquet", []byte("will never be served"), originID),
	}

	fsm1 := raft.NewClusterFSM(zerolog.Nop())
	_ = seedMultipleFilesInFSM(t, fsm1, rawEntries)
	fsm2 := raft.NewClusterFSM(zerolog.Nop())
	_ = seedMultipleFilesInFSM(t, fsm2, rawEntries)

	peer1 := startOriginServer(t, backend1, fsm1, "catchup-secret", "test-cluster", "peer-1")
	defer peer1.stop()
	peer2 := startOriginServer(t, backend2, fsm2, "catchup-secret", "test-cluster", "peer-2")
	defer peer2.stop()

	// Rebuild entries as pointers for RunCatchUp.
	entries := make([]*raft.FileEntry, len(rawEntries))
	for i := range rawEntries {
		e := rawEntries[i]
		entries[i] = &e
	}

	puller, readerBackend, stop := buildCatchUpPuller(t, readerID, []string{peer1.addr(), peer2.addr()})
	defer stop()

	puller.RunCatchUp(context.Background(), entries)

	stats := waitForCatchUp(t, puller, 1)
	if stats["failed"] != 1 {
		t.Errorf("failed: got %d, want 1 (no peer has the file; catch-up should give up cleanly) — stats=%+v", stats["failed"], stats)
	}
	if stats["pulled"] != 0 {
		t.Errorf("pulled: got %d, want 0", stats["pulled"])
	}
	// Reader backend must remain empty.
	if exists, _ := readerBackend.Exists(context.Background(), "testdb/cpu/missing.parquet"); exists {
		t.Errorf("reader backend should be empty after failed catch-up")
	}
}

// Ensure io.Discard is used elsewhere so this file's imports compile even
// in the absence of other references. (Static unused-var protection.)
var _ = io.Discard
