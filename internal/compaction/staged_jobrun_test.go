//go:build duckdb_arrow

package compaction

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// #1207 d2a2a: a staged job writes its output to the staged key, durable,
// keeps its inputs, writes no crash-recovery storage manifest, and hands the
// watcher a "staged" completion manifest naming exactly what it read.
func TestJobRunStaged(t *testing.T) { testJobRunStaged(t, false) }

// #1207 d2a2b: on an object store the staged output is written with its
// checksum recorded on the object.
func TestJobRunStagedObject(t *testing.T) { testJobRunStaged(t, true) }

// objectOnly is an object store over a backend, hiding its local-only
// interfaces (moves, adoption).
type objectOnly struct {
	storage.Backend
	meta map[string]string
}

func (o *objectOnly) WriteReaderWithSHA(ctx context.Context, p string, r io.Reader, size int64, sha string) error {
	o.meta[p] = sha
	return o.Backend.WriteReader(ctx, p, r, size)
}
func (o *objectOnly) CopyObject(context.Context, string, string, int64, string) error { return nil }
func (o *objectOnly) ObjectSHA(ctx context.Context, p string) (int64, string, error) {
	size, err := o.Backend.StatFile(ctx, p)
	return size, o.meta[p], err
}

func testJobRunStaged(t *testing.T, object bool) {
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
	tempDir := filepath.Join(filepath.Dir(root), filepath.Base(root)+"-compaction")
	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempDir) })

	const partition = "testdb/cpu/2026/04/11/14"
	keys := make([]string, 0, 3)
	inputs := map[string]StagedInput{}
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
		sum := sha256.Sum256(data)
		inputs[key] = StagedInput{Path: key, SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	}
	var jobBackend storage.Backend = backend
	obj := &objectOnly{Backend: backend, meta: map[string]string{}}
	if object {
		jobBackend = obj
	}
	run := func(job string, ins map[string]StagedInput) (*Job, string, error) {
		completionDir := filepath.Join(t.TempDir(), "completion")
		j := NewJob(&JobConfig{
			Measurement: "cpu", PartitionPath: partition, Files: keys,
			StorageBackend: jobBackend, Database: "testdb", Tier: "hourly",
			TempDirectory: tempDir, Logger: zerolog.Nop(), DB: db,
			ManifestManager: NewManifestManager(backend, zerolog.Nop()),
			JobID:           job, CompletionDir: completionDir,
			Staged: true, Inputs: ins,
		})
		return j, completionDir, j.Run(ctx)
	}

	// An input the cluster manifest does not describe: no staged manifest.
	partial := map[string]StagedInput{keys[0]: inputs[keys[0]]}
	if _, dir, err := run("staged-partial", partial); err == nil {
		t.Fatal("ran with an undescribed input")
	} else if m, rerr := readCompletionManifest(filepath.Join(dir, "staged-partial.json")); rerr == nil && m.State == CompletionStateStaged {
		t.Fatal("staged manifest written for a failed job")
	}

	job, dir, err := run("staged-job", inputs)
	if err != nil {
		t.Fatalf("Job.Run staged: %v", err)
	}
	for _, k := range keys {
		if _, err := os.Stat(filepath.Join(root, k)); err != nil {
			t.Fatalf("input %s gone: %v", k, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, job.OutputStorageKey)); !os.IsNotExist(err) {
		t.Fatalf("output at its final path before any commit: %v", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(root, "_compaction_state")); len(entries) != 0 {
		t.Fatalf("a crash-recovery manifest was written: %v", entries)
	}
	m, err := readCompletionManifest(filepath.Join(dir, "staged-job.json"))
	if err != nil {
		t.Fatal(err)
	}
	if m.State != CompletionStateStaged || len(m.Outputs) != 0 || m.StagedOutput == nil || len(m.Inputs) != 3 ||
		m.StagedKey != StagedKey("staged-job", job.OutputStorageKey) || m.StagedOutput.Path != job.OutputStorageKey {
		t.Fatalf("manifest %+v", m)
	}
	staged, err := os.ReadFile(filepath.Join(root, m.StagedKey))
	if err != nil {
		t.Fatalf("staged output: %v", err)
	}
	sum := sha256.Sum256(staged)
	if m.StagedOutput.SHA256 != hex.EncodeToString(sum[:]) || m.StagedOutput.SizeBytes != int64(len(staged)) {
		t.Fatal("staged output does not match the manifest")
	}
	if m.StagedObject != object {
		t.Fatalf("staged object %v", m.StagedObject)
	}
	if object && obj.meta[m.StagedKey] != m.StagedOutput.SHA256 {
		t.Fatalf("checksum recorded on the staged object: %q", obj.meta[m.StagedKey])
	}
	for _, in := range m.Inputs {
		if in != inputs[in.Path] {
			t.Fatalf("input %+v, want %+v", in, inputs[in.Path])
		}
	}
}
