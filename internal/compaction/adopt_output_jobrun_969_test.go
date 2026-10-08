//go:build duckdb_arrow

package compaction

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// TestJobRunClusterModeHashesTheOutputBeforePublishingIt drives the real
// Job.Run in cluster mode on a local backend, which is the only configuration
// where both halves of the #969 output change are live at once: the upload
// MOVES the compacted file into the storage root, and the completion manifest
// needs that file's SHA-256.
//
// Hashing after the upload -- where the code did it before this change --
// fails here with ENOENT, because the file the hash wants has been moved. The
// seam test TestClusterOutputManifestHashSurvivesAdopt performs the ordering
// itself and so cannot catch a regression inside Job.Run; this one can.
func TestJobRunClusterModeHashesTheOutputBeforePublishingIt(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	// The temp directory is a sibling of the storage root on the same
	// filesystem, which is the default shape (./data/compaction next to
	// ./data/arc) and the precondition for the move. Assert it below.
	tempDir := filepath.Join(filepath.Dir(root), filepath.Base(root)+"-compaction")
	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	const partition = "testdb/cpu/2026/04/11/14"
	keys := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		fixture := filepath.Join(t.TempDir(), "in.parquet")
		writeFixtureParquet(t, ctx, db, fixture,
			"TIMESTAMPTZ '2026-04-11 14:00:0"+string(rune('0'+i))+"Z' AS time, 'h"+string(rune('0'+i))+"' AS host, 1.0 AS value")
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		key := partition + "/in" + string(rune('0'+i)) + ".parquet"
		if err := backend.Write(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}

	completionDir := filepath.Join(t.TempDir(), "completion")
	job := NewJob(&JobConfig{
		Measurement: "cpu", PartitionPath: partition, Files: keys,
		StorageBackend: backend, Database: "testdb", Tier: "hourly",
		TempDirectory: tempDir, Logger: zerolog.Nop(), DB: db,
		ManifestManager: NewManifestManager(backend, zerolog.Nop()),
		JobID:           "adopt-cluster-969",
		CompletionDir:   completionDir,
	})
	if !job.clusterMode() {
		t.Fatal("precondition: CompletionDir must put the job in cluster mode, else no manifest is written and this test proves nothing")
	}

	if err := job.Run(ctx); err != nil {
		t.Fatalf("Job.Run in cluster mode: %v", err)
	}
	if job.Status != JobStatusCompleted {
		t.Fatalf("job status = %s, want completed", job.Status)
	}
	if job.OutputStorageKey == "" {
		t.Fatal("no output key recorded")
	}

	// Precondition for the whole point of the test: the upload must have MOVED
	// the output, not copied it. If a future change puts the two paths on
	// different filesystems, or drops the adopt path, this fires and the
	// assertion below stops meaning anything.
	if entries, _ := os.ReadDir(tempDir); len(entries) != 0 {
		t.Fatalf("temp directory is not empty after the job: %v", entries)
	}

	published := filepath.Join(root, job.OutputStorageKey)
	bytesOnDisk, err := os.ReadFile(published)
	if err != nil {
		t.Fatalf("read published output: %v", err)
	}
	want := sha256.Sum256(bytesOnDisk)

	m, err := readCompletionManifest(filepath.Join(completionDir, job.JobID+".json"))
	if err != nil {
		t.Fatalf("read completion manifest: %v", err)
	}
	if m.State != CompletionStateSourcesDeleted && m.State != CompletionStateOutputWritten {
		t.Fatalf("manifest state = %s, want output_written or sources_deleted", m.State)
	}
	if len(m.Outputs) != 1 {
		t.Fatalf("manifest carries %d outputs, want 1", len(m.Outputs))
	}
	if got := m.Outputs[0].SHA256; got != hex.EncodeToString(want[:]) {
		t.Fatalf("manifest SHA256 = %q, want %q -- the digest peers verify against does not match the bytes that were published",
			got, hex.EncodeToString(want[:]))
	}
	if m.Outputs[0].SizeBytes != int64(len(bytesOnDisk)) {
		t.Errorf("manifest SizeBytes = %d, want %d", m.Outputs[0].SizeBytes, len(bytesOnDisk))
	}
	// The published object carries the same restrictive mode every other local
	// object gets, even though a rename preserves the source mode.
	info, err := os.Stat(published)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("published object mode = %#o, want 0600", perm)
	}
}
