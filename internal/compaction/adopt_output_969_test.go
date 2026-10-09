package compaction

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// adoptJob builds the minimum Job that uploadFile needs: a backend, the output
// size uploadFile hands to AdoptFile, and a logger whose output a test can read.
func adoptJob(t *testing.T, backend storage.Backend, size int64, out io.Writer) *Job {
	t.Helper()
	logger := zerolog.Nop()
	if out != nil {
		logger = zerolog.New(out)
	}
	return &Job{
		JobID:          "job_adopt_969",
		Database:       "testdb",
		Measurement:    "cpu",
		PartitionPath:  "testdb/cpu/2026/10/08/14",
		Tier:           "hourly",
		StorageBackend: backend,
		BytesAfter:     size,
		logger:         logger,
	}
}

func compactedTempOutput(t *testing.T) (string, []byte) {
	t.Helper()
	// Its own directory, as compaction.temp_directory is its own path.
	path := filepath.Join(t.TempDir(), "20261008_140000_compacted.parquet")
	payload := []byte("PAR1-this-is-the-compacted-output")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write temp output: %v", err)
	}
	return path, payload
}

// TestUploadFileAdoptsLocalOutput is the #969 output-side win: on a local
// backend the output is moved into storage, so the bytes are not copied.
func TestUploadFileAdoptsLocalOutput(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	local, payload := compactedTempOutput(t)
	j := adoptJob(t, backend, int64(len(payload)), nil)
	key := filepath.Join(j.PartitionPath, filepath.Base(local))

	if err := j.uploadFile(context.Background(), local, key); err != nil {
		t.Fatalf("uploadFile: %v", err)
	}

	if _, err := os.Stat(local); !os.IsNotExist(err) {
		t.Fatalf("temp output still present after upload (err=%v); it was copied, not moved", err)
	}
	got, err := os.ReadFile(filepath.Join(root, key))
	if err != nil {
		t.Fatalf("read published object: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("published bytes = %q, want %q", got, payload)
	}
}

// refusingAdopter wraps a real local backend but refuses to adopt, which is
// what LocalBackend does when compaction.temp_directory is on a different
// filesystem from storage.local_path.
type refusingAdopter struct {
	*storage.LocalBackend
	reason     string
	adoptCalls int
	writeCalls int
}

func (r *refusingAdopter) AdoptFile(ctx context.Context, path, localPath string) error {
	r.adoptCalls++
	return fmt.Errorf("%w: %s", storage.ErrAdoptUnsupported, r.reason)
}

func (r *refusingAdopter) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	r.writeCalls++
	return r.LocalBackend.WriteReader(ctx, path, reader, size)
}

// TestUploadFileFallsBackToCopyOnCrossDevice covers the configuration the
// compaction.temp_directory key exists for. Skipping the fallback here would
// fail every compaction on a split-volume deployment.
func TestUploadFileFallsBackToCopyOnCrossDevice(t *testing.T) {
	root := t.TempDir()
	base, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	backend := &refusingAdopter{LocalBackend: base, reason: "different filesystems"}

	local, payload := compactedTempOutput(t)
	j := adoptJob(t, backend, int64(len(payload)), nil)
	key := filepath.Join(j.PartitionPath, filepath.Base(local))

	if err := j.uploadFile(context.Background(), local, key); err != nil {
		t.Fatalf("uploadFile must copy when the output cannot be adopted: %v", err)
	}
	if backend.adoptCalls != 1 {
		t.Errorf("AdoptFile called %d times, want 1", backend.adoptCalls)
	}
	if backend.writeCalls != 1 {
		t.Errorf("WriteReader called %d times, want 1 (the fallback copy)", backend.writeCalls)
	}

	// A copy leaves the source alone, which is exactly why the digest taken
	// before the upload stays valid on this path too.
	if _, err := os.Stat(local); err != nil {
		t.Errorf("fallback must not consume the temp output: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, key))
	if err != nil {
		t.Fatalf("read published object: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("fallback published %q, want %q", got, payload)
	}
}

// TestUploadFileFallbackLogsAboveDebug: the fallback costs the operator a full
// extra pass over the output on every compaction, and Debug from the compaction
// subprocess clears neither the subprocess nor the parent level filter at the
// default log level, so the line has to be at least Warn to reach anyone.
func TestUploadFileFallbackLogsAboveDebug(t *testing.T) {
	root := t.TempDir()
	base, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	backend := &refusingAdopter{LocalBackend: base, reason: "different filesystems"}

	var logs bytes.Buffer
	local, payload := compactedTempOutput(t)
	j := adoptJob(t, backend, int64(len(payload)), &logs)

	if err := j.uploadFile(context.Background(), local, filepath.Join(j.PartitionPath, filepath.Base(local))); err != nil {
		t.Fatalf("uploadFile: %v", err)
	}

	var found bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if !strings.Contains(fmt.Sprint(entry["message"]), "copied instead") {
			continue
		}
		found = true
		if lvl := fmt.Sprint(entry["level"]); lvl != "warn" && lvl != "error" {
			t.Errorf("fallback logged at %q; Debug and Info do not reach an operator from the compaction subprocess", lvl)
		}
		if entry["local_path"] == nil || entry["path"] == nil {
			t.Errorf("fallback line must name both paths, got %v", entry)
		}
	}
	if !found {
		t.Errorf("no fallback line emitted; the extra copy is invisible. logs=%q", logs.String())
	}
}

// brokenAdopter fails for a reason that is NOT "copy instead". uploadFile must
// surface it rather than quietly doing the copy: an adopt that fails on a full
// disk or an unwritable destination is a real failure, and a silent fallback
// would spend a full pass over the output before failing anyway.
type brokenAdopter struct {
	*storage.LocalBackend
	writeCalls int
}

func (b *brokenAdopter) AdoptFile(ctx context.Context, path, localPath string) error {
	return errors.New("disk is on fire")
}

func (b *brokenAdopter) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	b.writeCalls++
	return b.LocalBackend.WriteReader(ctx, path, reader, size)
}

func TestUploadFileFailsOnANonSentinelAdoptError(t *testing.T) {
	base, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	backend := &brokenAdopter{LocalBackend: base}

	local, payload := compactedTempOutput(t)
	j := adoptJob(t, backend, int64(len(payload)), nil)

	err = j.uploadFile(context.Background(), local, filepath.Join(j.PartitionPath, filepath.Base(local)))
	if err == nil {
		t.Fatal("uploadFile swallowed an adopt error that is not ErrAdoptUnsupported")
	}
	if !strings.Contains(err.Error(), "disk is on fire") {
		t.Errorf("error does not carry the cause: %v", err)
	}
	if backend.writeCalls != 0 {
		t.Errorf("WriteReader called %d times; only ErrAdoptUnsupported may fall back to the copy", backend.writeCalls)
	}
}

// TestClusterOutputManifestHashSurvivesAdopt is the trap test. The completion
// manifest's digest is the value peers verify pulled bytes against, and the
// output it describes no longer exists locally once the upload has adopted it.
// Hashing after the upload fails here with ENOENT -- and only in cluster mode,
// which no OSS single-node path exercises.
func TestClusterOutputManifestHashSurvivesAdopt(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	local, payload := compactedTempOutput(t)
	want := sha256.Sum256(payload)

	j := adoptJob(t, backend, int64(len(payload)), nil)
	j.CompletionDir = filepath.Join(t.TempDir(), "completion")
	if !j.clusterMode() {
		t.Fatal("precondition: the job must be in cluster mode or the manifest is never written")
	}

	// The order Job.Run uses: digest, then publish, then manifest.
	outputSHA, err := sha256File(local)
	if err != nil {
		t.Fatalf("sha256File: %v", err)
	}
	key := filepath.Join(j.PartitionPath, filepath.Base(local))
	if err := j.uploadFile(context.Background(), local, key); err != nil {
		t.Fatalf("uploadFile: %v", err)
	}
	if _, statErr := os.Stat(local); !os.IsNotExist(statErr) {
		t.Fatalf("precondition: the upload must have consumed the temp output, else this test proves nothing (err=%v)", statErr)
	}
	if err := j.writeOutputWrittenManifest(context.Background(), outputSHA, key); err != nil {
		t.Fatalf("writeOutputWrittenManifest after an adopted upload: %v", err)
	}

	m, err := readCompletionManifest(filepath.Join(j.CompletionDir, j.JobID+".json"))
	if err != nil {
		t.Fatalf("read completion manifest: %v", err)
	}
	if len(m.Outputs) != 1 {
		t.Fatalf("manifest carries %d outputs, want 1", len(m.Outputs))
	}
	if got := m.Outputs[0].SHA256; got != hex.EncodeToString(want[:]) {
		t.Fatalf("manifest SHA256 = %q, want %q (the digest of the published bytes)", got, hex.EncodeToString(want[:]))
	}
	if m.Outputs[0].SizeBytes != int64(len(payload)) {
		t.Errorf("manifest SizeBytes = %d, want %d", m.Outputs[0].SizeBytes, len(payload))
	}
}

// TestJobWithoutCompletionDirIsNotClusterMode pins the predicate the digest is
// gated on, which is the only reason an OSS single node does not pay a full
// extra read of the output: it writes no completion manifest, so it needs no
// digest. This asserts the gate's input, not the gate -- see
// TestClusterOutputManifestHashSurvivesAdopt for the cluster side.
func TestJobWithoutCompletionDirIsNotClusterMode(t *testing.T) {
	j := adoptJob(t, nil, 0, nil)
	if j.clusterMode() {
		t.Fatal("a Job with no CompletionDir must not be in cluster mode")
	}
}
