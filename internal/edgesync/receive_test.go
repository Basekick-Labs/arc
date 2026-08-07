package edgesync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func newTestReceiver(t *testing.T) (*Receiver, storage.Backend) {
	t.Helper()

	dir, err := os.MkdirTemp("", "sync-receive-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	r, err := NewReceiver(ReceiverConfig{Backend: backend, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("new receiver: %v", err)
	}
	return r, backend
}

const testPath = "metrics/cpu/2026/08/07/14/cpu_123.parquet"

func TestReceiver_CommitsVerifiedFile(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	content := []byte("parquet payload bytes")
	digest := sha256Hex(content)

	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if res.Outcome != OutcomeCommitted {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeCommitted)
	}

	// The file must land under the spoke's namespace, not at its native path.
	final := NamespacedPath("rocket-01", testPath)
	got, err := backend.Read(ctx, final)
	if err != nil {
		t.Fatalf("read committed file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("committed content differs from what was sent")
	}

	// Staging must be cleaned up, or every transfer doubles hub disk use.
	if exists, _ := backend.Exists(ctx, stagingPathFor("rocket-01", testPath)); exists {
		t.Error("staging object survived a successful promote")
	}
}

func TestReceiver_ChecksumMismatchNeverReachesFinalPath(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	content := []byte("the real payload")
	corrupted := []byte("the fake payload") // same length, different bytes

	res, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(corrupted))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if res.Outcome != OutcomeChecksumMismatch {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeChecksumMismatch)
	}

	// This is the property the whole staging design exists for: corrupt bytes
	// must never appear where a reader — or a later reconcile — would find them.
	if exists, _ := backend.Exists(ctx, NamespacedPath("rocket-01", testPath)); exists {
		t.Error("corrupt content was committed to the final path")
	}
	if exists, _ := backend.Exists(ctx, stagingPathFor("rocket-01", testPath)); exists {
		t.Error("corrupt staging object was left behind")
	}
}

func TestReceiver_RedeliveryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestReceiver(t)

	content := []byte("parquet payload")
	digest := sha256Hex(content)

	if _, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content)); err != nil {
		t.Fatalf("first delivery: %v", err)
	}

	// The lost-ack case: the spoke never saw the acknowledgment and resends.
	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if res.Outcome != OutcomeAlreadyPresent {
		t.Errorf("outcome = %q, want %q", res.Outcome, OutcomeAlreadyPresent)
	}
	if !res.Outcome.Done() {
		t.Error("already-present must be terminal, or the spoke resends forever")
	}
}

func TestReceiver_ConflictRefusesOverwrite(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	original := []byte("the hub's content")
	if _, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(original), int64(len(original)), 0, bytes.NewReader(original)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Same path, different content — a spoke_id collision or corruption.
	replacement := []byte("a different content")
	res, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(replacement), int64(len(replacement)), 0, bytes.NewReader(replacement))
	if err != nil {
		t.Fatalf("conflicting delivery: %v", err)
	}
	if res.Outcome != OutcomeConflict {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeConflict)
	}
	if res.TheirSHA256 != sha256Hex(original) {
		t.Errorf("TheirSHA256 = %q, want the hub's %q", res.TheirSHA256, sha256Hex(original))
	}

	// The original bytes must be untouched — overwriting destroys whichever
	// copy is the correct one.
	stored, err := backend.Read(ctx, NamespacedPath("rocket-01", testPath))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(stored, original) {
		t.Error("a conflicting upload overwrote the hub's content")
	}
}

func TestReceiver_SpokeNamespacingIsolatesEdges(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	// Two rockets legitimately produce the SAME native path — identical
	// measurement, identical hour. Without namespacing the second would
	// conflict with (or overwrite) the first.
	a := []byte("rocket one telemetry")
	b := []byte("rocket two telemetry")

	if _, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(a), int64(len(a)), 0, bytes.NewReader(a)); err != nil {
		t.Fatalf("rocket-01: %v", err)
	}
	res, err := r.Receive(ctx, "rocket-02", testPath, sha256Hex(b), int64(len(b)), 0, bytes.NewReader(b))
	if err != nil {
		t.Fatalf("rocket-02: %v", err)
	}
	if res.Outcome != OutcomeCommitted {
		t.Fatalf("second spoke got %q, want %q — namespacing failed", res.Outcome, OutcomeCommitted)
	}

	for spoke, want := range map[string][]byte{"rocket-01": a, "rocket-02": b} {
		got, err := backend.Read(ctx, NamespacedPath(spoke, testPath))
		if err != nil {
			t.Fatalf("read %s: %v", spoke, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s holds the wrong content", spoke)
		}
	}
}

func TestReceiver_PartialThenResume(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	if !r.SupportsResume() {
		t.Skip("local backend should support resume")
	}

	content := []byte("the complete parquet payload for this file")
	digest := sha256Hex(content)
	const prefixLen = 15

	// A contact window closes mid-file.
	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content[:prefixLen]))
	if err != nil {
		t.Fatalf("partial: %v", err)
	}
	if res.Outcome != OutcomePartial {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomePartial)
	}
	if res.BytesAccepted != prefixLen {
		t.Fatalf("accepted %d bytes, want %d", res.BytesAccepted, prefixLen)
	}
	// A partial file must not be visible as committed data.
	if exists, _ := backend.Exists(ctx, NamespacedPath("rocket-01", testPath)); exists {
		t.Error("a partially-received file appeared at the final path")
	}

	// The next window sends only the tail.
	res2, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), res.BytesAccepted, bytes.NewReader(content[res.BytesAccepted:]))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res2.Outcome != OutcomeCommitted {
		t.Fatalf("resumed outcome = %q, want %q", res2.Outcome, OutcomeCommitted)
	}

	// The reassembled file must be byte-identical — a resume that mis-splices
	// the tail corrupts silently, which is exactly what the whole-file digest
	// is there to catch.
	stored, err := backend.Read(ctx, NamespacedPath("rocket-01", testPath))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(stored, content) {
		t.Error("the resumed file does not match the original")
	}
}

func TestReceiver_ResumeWithWrongOffsetReportsTruth(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestReceiver(t)

	content := []byte("the complete parquet payload for this file")
	digest := sha256Hex(content)

	if _, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content[:10])); err != nil {
		t.Fatalf("partial: %v", err)
	}

	// The spoke thinks the hub has 30 bytes; it actually has 10. Appending at
	// the wrong place would corrupt the file, so the hub must answer with what
	// it really holds instead.
	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 30, bytes.NewReader(content[30:]))
	if err != nil {
		t.Fatalf("bad-offset resume: %v", err)
	}
	if res.Outcome != OutcomePartial {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomePartial)
	}
	if res.BytesAccepted != 10 {
		t.Errorf("accepted = %d, want the 10 bytes the hub actually holds", res.BytesAccepted)
	}
}

func TestReceiver_RejectsOversizedBody(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	// The spoke declares a small file but streams more. Without a cap this
	// writes unbounded bytes into hub storage under an honest-looking header.
	declared := []byte("small")
	oversized := append(declared, bytes.Repeat([]byte("x"), 10_000)...)

	res, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(declared), int64(len(declared)), 0, bytes.NewReader(oversized))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	// Only the declared prefix is read, so it verifies and commits at the
	// declared size — the excess is simply never consumed.
	if res.Outcome != OutcomeCommitted {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeCommitted)
	}
	stored, err := backend.Read(ctx, NamespacedPath("rocket-01", testPath))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(stored) != len(declared) {
		t.Errorf("stored %d bytes, want %d — the declared size did not cap the body", len(stored), len(declared))
	}
}

func TestReceiver_RejectsMaliciousPaths(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestReceiver(t)
	content := []byte("payload")

	// A spoke is semi-trusted — it may be a compromised edge box — so its
	// declared path is untrusted input that becomes a filesystem location.
	paths := []struct {
		name, path string
	}{
		{"parent traversal", "../../../etc/passwd.parquet"},
		{"embedded traversal", "metrics/../../escape.parquet"},
		{"absolute", "/etc/passwd.parquet"},
		{"backslash", "metrics\\cpu\\f.parquet"},
		{"empty", ""},
		{"NUL byte", "metrics/cpu\x00/f.parquet"},
		{"dot prefix", ".sync-staging/rocket-02/f.parquet"},
		{"empty segment", "metrics//f.parquet"},
		{"not parquet", "metrics/cpu/evil.sh"},
	}

	for _, tt := range paths {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := r.Receive(ctx, "rocket-01", tt.path, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content)); err == nil {
				t.Errorf("path %q was accepted", tt.path)
			}
		})
	}
}

func TestReceiver_RejectsMaliciousSpokeIDs(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestReceiver(t)
	content := []byte("payload")

	// The spoke ID is the first path segment of everything that spoke writes.
	// The HMAC proves WHO is asking, not WHERE they may write — so the ID
	// still has to be validated as a path component.
	ids := []struct{ name, id string }{
		{"empty", ""},
		{"traversal", ".."},
		{"separator", "rocket/../other"},
		{"backslash", "rocket\\other"},
		{"dot prefix", ".sync-staging"},
		{"NUL byte", "rocket\x00-01"},
	}

	for _, tt := range ids {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := r.Receive(ctx, tt.id, testPath, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content)); err == nil {
				t.Errorf("spoke ID %q was accepted", tt.id)
			}
		})
	}
}

func TestReceiver_RejectsMalformedDigestAndSize(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestReceiver(t)
	content := []byte("payload")
	good := sha256Hex(content)

	t.Run("digest not hex", func(t *testing.T) {
		if _, err := r.Receive(ctx, "rocket-01", testPath, strings.Repeat("z", 64), int64(len(content)), 0, bytes.NewReader(content)); err == nil {
			t.Error("a non-hex digest was accepted")
		}
	})
	t.Run("digest wrong length", func(t *testing.T) {
		if _, err := r.Receive(ctx, "rocket-01", testPath, "abc123", int64(len(content)), 0, bytes.NewReader(content)); err == nil {
			t.Error("a short digest was accepted")
		}
	})
	t.Run("negative size", func(t *testing.T) {
		if _, err := r.Receive(ctx, "rocket-01", testPath, good, -1, 0, bytes.NewReader(content)); err == nil {
			t.Error("a negative size was accepted")
		}
	})
	t.Run("offset beyond size", func(t *testing.T) {
		if _, err := r.Receive(ctx, "rocket-01", testPath, good, int64(len(content)), int64(len(content))+1, bytes.NewReader(nil)); err == nil {
			t.Error("an offset past the file size was accepted")
		}
	})
	t.Run("negative offset", func(t *testing.T) {
		if _, err := r.Receive(ctx, "rocket-01", testPath, good, int64(len(content)), -1, bytes.NewReader(content)); err == nil {
			t.Error("a negative offset was accepted")
		}
	})
}

func TestReceiver_RegistersCommittedFiles(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sync-receive-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	var registered []*ReceivedFile
	r, err := NewReceiver(ReceiverConfig{
		Backend: backend,
		Logger:  zerolog.Nop(),
		RegisterFile: func(_ context.Context, f *ReceivedFile) error {
			registered = append(registered, f)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("new receiver: %v", err)
	}

	content := []byte("payload")
	if _, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content)); err != nil {
		t.Fatalf("receive: %v", err)
	}

	if len(registered) != 1 {
		t.Fatalf("registered %d files, want 1", len(registered))
	}
	f := registered[0]
	if f.Path != NamespacedPath("rocket-01", testPath) {
		t.Errorf("registered path = %q, want the namespaced path", f.Path)
	}
	if f.SourcePath != testPath {
		t.Errorf("SourcePath = %q, want the spoke's original %q", f.SourcePath, testPath)
	}
	if f.SpokeID != "rocket-01" || f.SHA256 != sha256Hex(content) || f.SizeBytes != int64(len(content)) {
		t.Errorf("registered metadata is wrong: %+v", f)
	}
}

func TestReceiver_RegistrationFailureKeepsBytes(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sync-receive-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	r, err := NewReceiver(ReceiverConfig{
		Backend: backend,
		Logger:  zerolog.Nop(),
		RegisterFile: func(context.Context, *ReceivedFile) error {
			return errors.New("raft unavailable")
		},
	})
	if err != nil {
		t.Fatalf("new receiver: %v", err)
	}

	content := []byte("payload")
	if _, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content)); err == nil {
		t.Fatal("a manifest failure was reported as success")
	}

	// The verified bytes must survive. Deleting them over a transient manifest
	// failure would discard a completed transfer; the spoke retries and gets
	// AlreadyPresent, and registration is attempted again.
	if exists, _ := backend.Exists(ctx, NamespacedPath("rocket-01", testPath)); !exists {
		t.Error("verified content was discarded because the manifest write failed")
	}
}

func TestReceiver_RequiresBackend(t *testing.T) {
	if _, err := NewReceiver(ReceiverConfig{Logger: zerolog.Nop()}); err == nil {
		t.Error("a nil backend was accepted")
	}
}

func TestReceiver_StagingIsOutsideQueryableNamespace(t *testing.T) {
	// Staged bytes must not be reachable as data. The staging prefix begins
	// with a dot, which validateSyncPath rejects, so a spoke cannot address
	// another spoke's staging area by declaring a crafted path.
	if !strings.HasPrefix(StagingPrefix, ".") {
		t.Error("the staging prefix must start with a dot so it cannot be a database name")
	}
	if err := validateSyncPath(StagingPrefix + "/rocket-02/f.parquet"); err == nil {
		t.Error("a path into the staging area was accepted")
	}
}

func TestNamespacedPath(t *testing.T) {
	got := NamespacedPath("rocket-01", "metrics/cpu/f.parquet")
	want := "rocket-01/metrics/cpu/f.parquet"
	if got != want {
		t.Errorf("NamespacedPath = %q, want %q", got, want)
	}
}

// nonAppendingBackend wraps a Backend to hide AppendingBackend, simulating S3
// or Azure — where a partial transfer cannot be resumed.
type nonAppendingBackend struct{ storage.Backend }

func TestReceiver_NoResumeSupportRestartsFromZero(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sync-receive-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	local, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { local.Close() })

	r, err := NewReceiver(ReceiverConfig{Backend: nonAppendingBackend{local}, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("new receiver: %v", err)
	}
	if r.SupportsResume() {
		t.Fatal("the wrapper should hide AppendingBackend")
	}

	content := []byte("the complete parquet payload for this file")
	digest := sha256Hex(content)

	// A short body on a non-appending backend must report zero accepted, so
	// the spoke restarts from the beginning rather than resuming into a
	// prefix the hub cannot extend.
	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content[:10]))
	if err != nil {
		t.Fatalf("partial: %v", err)
	}
	if res.Outcome != OutcomePartial {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomePartial)
	}
	if res.BytesAccepted != 0 {
		t.Errorf("accepted = %d, want 0 — this backend cannot be resumed into", res.BytesAccepted)
	}

	// An attempted resume must fail loudly rather than silently corrupting.
	if _, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 10, bytes.NewReader(content[10:])); !errors.Is(err, storage.ErrResumeNotSupported) {
		t.Errorf("resume on a non-appending backend: err = %v, want ErrResumeNotSupported", err)
	}

	// A full re-send still succeeds.
	res2, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("full re-send: %v", err)
	}
	if res2.Outcome != OutcomeCommitted {
		t.Errorf("outcome = %q, want %q", res2.Outcome, OutcomeCommitted)
	}
}

func TestReceiver_ShortBodyIsNotCommitted(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	// A body that ends early must never be treated as complete, even though
	// its bytes are a valid prefix — the declared size is the contract.
	content := []byte("the complete parquet payload")
	res, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(content), int64(len(content)), 0,
		io.LimitReader(bytes.NewReader(content), 5))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if res.Outcome != OutcomePartial {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomePartial)
	}
	if exists, _ := backend.Exists(ctx, NamespacedPath("rocket-01", testPath)); exists {
		t.Error("a short body was committed to the final path")
	}
}

func TestReceivedFile_DerivesArcNamespace(t *testing.T) {
	// The hub prepends the spoke ID, so database/measurement/partition must be
	// derived from the spoke's ORIGINAL path — reading them off the namespaced
	// path would make the spoke ID look like the database name.
	f := &ReceivedFile{
		SpokeID:    "rocket-01",
		SourcePath: "metrics/cpu/2026/08/07/14/cpu_123.parquet",
		Path:       NamespacedPath("rocket-01", "metrics/cpu/2026/08/07/14/cpu_123.parquet"),
	}

	if got := f.Database(); got != "metrics" {
		t.Errorf("Database() = %q, want %q — the spoke ID leaked into the database name", got, "metrics")
	}
	if got := f.Measurement(); got != "cpu" {
		t.Errorf("Measurement() = %q, want %q", got, "cpu")
	}

	// PartitionTime is load-bearing: tiering filters queries by it and computes
	// file age from it, so a zero value would make the file look infinitely old
	// and drop it from time-ranged queries.
	want := time.Date(2026, 8, 7, 14, 0, 0, 0, time.UTC)
	if got := f.PartitionTime(); !got.Equal(want) {
		t.Errorf("PartitionTime() = %v, want %v", got, want)
	}
	if got := f.PartitionTime(); got.Location() != time.UTC {
		t.Errorf("PartitionTime location = %v, want UTC", got.Location())
	}
}

func TestReceivedFile_UnparseablePathsYieldZeroValues(t *testing.T) {
	// A path that does not carry a partition must report the zero time rather
	// than a wrong one — "unknown" is recoverable, epoch is silently wrong.
	tests := []struct{ name, path string }{
		{"too short", "metrics/cpu/f.parquet"},
		{"non-numeric partition", "metrics/cpu/YYYY/MM/DD/HH/f.parquet"},
		{"impossible date", "metrics/cpu/2026/13/45/99/f.parquet"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &ReceivedFile{SourcePath: tt.path}
			if got := f.PartitionTime(); !got.IsZero() {
				t.Errorf("PartitionTime() = %v, want the zero time", got)
			}
		})
	}

	short := &ReceivedFile{SourcePath: "onlyone"}
	if short.Database() != "" || short.Measurement() != "" {
		t.Error("a path too short to carry a namespace returned non-empty values")
	}
}

// wedgeBackend fails ReadTo on the final path exactly once, simulating a
// promote that dies after staging but before the rename.
type wedgeBackend struct {
	storage.Backend
	failOn string
	failed bool
}

func (w *wedgeBackend) ReadTo(ctx context.Context, p string, out io.Writer) error {
	if p == w.failOn && !w.failed {
		w.failed = true
		return errors.New("simulated transient read failure")
	}
	return w.Backend.ReadTo(ctx, p, out)
}

func TestReceiver_StalePartDoesNotWedgeThePath(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sync-wedge-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	local, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { local.Close() })

	staging := stagingPathFor("rocket-01", testPath)
	backend := &wedgeBackend{Backend: local, failOn: staging}
	r, err := NewReceiver(ReceiverConfig{Backend: backend, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}

	content := []byte("parquet payload")
	digest := sha256Hex(content)

	// First attempt: verification succeeds, promote fails mid-copy. That can
	// leave "{finalPath}.part" behind with the final file absent.
	if _, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content)); err == nil {
		t.Fatal("expected the seeded promote failure")
	}

	// The retry must succeed. LocalBackend.StatFile falls back to the .part
	// file, so a stale one would be read as "the file already exists" and the
	// identity branch would then fail forever on ReadTo — wedging this path
	// permanently, with no retry able to clear it.
	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("retry after a failed promote: %v — the path is wedged", err)
	}
	if res.Outcome != OutcomeCommitted {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeCommitted)
	}

	stored, err := backend.Read(ctx, NamespacedPath("rocket-01", testPath))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(stored, content) {
		t.Error("the recovered file does not match what was sent")
	}
}

func TestReceiver_PartialResultsAlwaysValidate(t *testing.T) {
	ctx := context.Background()
	r, _ := newTestReceiver(t)

	// A staged prefix longer than a newly-declared size means the two sides
	// disagree about the file itself, not just the offset. Reporting the
	// staged length would return BytesAccepted > SizeBytes — which
	// PutResult.Validate rejects — so the spoke would hard-error on the hub's
	// own reply. PR 2 has the agent call Validate on everything it receives,
	// so the hub must never emit a result its own validator refuses.
	big := []byte("0123456789012345678901234567890123456789012345678901234567890123456789")
	if _, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(big), 100, 0, bytes.NewReader(big[:60])); err != nil {
		t.Fatalf("stage: %v", err)
	}

	const smaller = 50
	res, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(big[:smaller]), smaller, 10, bytes.NewReader(big[10:smaller]))
	if err != nil {
		t.Fatalf("re-declare at a smaller size: %v", err)
	}

	entry := &LedgerEntry{Path: testPath, SHA256: sha256Hex(big[:smaller]), SizeBytes: smaller}
	if verr := res.Validate(entry); verr != nil {
		t.Errorf("the hub returned a result its own Validate rejects: %v", verr)
	}
	if res.BytesAccepted > smaller {
		t.Errorf("BytesAccepted = %d exceeds the declared size %d", res.BytesAccepted, smaller)
	}
}

func TestReceiver_SweepStagingReclaimsAbandonedPartials(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	// Three abandoned transfers: a spoke declares a size, sends a prefix, and
	// never returns. Nothing else in the system reclaims these, so without a
	// sweep a spoke can fill the hub's disk with fresh paths.
	content := []byte("the complete parquet payload for this file")
	for i := 0; i < 3; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/abandoned_%d.parquet", i)
		if _, err := r.Receive(ctx, "rocket-01", p, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content[:10])); err != nil {
			t.Fatalf("abandon %d: %v", i, err)
		}
	}

	// A sweep with a long horizon must keep them: a staged prefix IS a
	// legitimate resume checkpoint, and deleting it turns a recoverable
	// transfer into a restart from zero.
	removed, err := r.SweepStaging(ctx, 24*time.Hour, time.Now())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if removed != 0 {
		t.Errorf("swept %d fresh partials; a recent checkpoint must be preserved", removed)
	}

	// Pretending the files are old: everything past the horizon goes.
	removed, err = r.SweepStaging(ctx, time.Hour, time.Now().Add(2*time.Hour))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if removed < 3 {
		t.Errorf("swept %d, want at least the 3 abandoned partials", removed)
	}

	for i := 0; i < 3; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/abandoned_%d.parquet", i)
		staging := stagingPathFor("rocket-01", p)
		if exists, _ := backend.Exists(ctx, staging); exists {
			t.Errorf("abandoned staging file %d survived the sweep", i)
		}
		if exists, _ := backend.Exists(ctx, partSuffix(staging)); exists {
			t.Errorf("abandoned .part file %d survived the sweep", i)
		}
	}
}

func TestReceiver_SweepStagingLeavesCommittedFilesAlone(t *testing.T) {
	ctx := context.Background()
	r, backend := newTestReceiver(t)

	content := []byte("committed payload")
	if _, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content)); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The sweep must only ever touch the staging prefix — a bug here would
	// delete verified, committed data.
	if _, err := r.SweepStaging(ctx, time.Hour, time.Now().Add(48*time.Hour)); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	stored, err := backend.Read(ctx, NamespacedPath("rocket-01", testPath))
	if err != nil {
		t.Fatalf("the sweep deleted a committed file: %v", err)
	}
	if !bytes.Equal(stored, content) {
		t.Error("committed content changed during a staging sweep")
	}
}

func TestReceiver_RetryRecoversFromAFailedRegistration(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sync-reg-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	// Fails once — a Raft election or a quorum blip — then recovers.
	var attempts int
	var registered []string
	r, err := NewReceiver(ReceiverConfig{
		Backend: backend,
		Logger:  zerolog.Nop(),
		RegisterFile: func(_ context.Context, f *ReceivedFile) error {
			attempts++
			if attempts == 1 {
				return errors.New("raft: leadership lost")
			}
			registered = append(registered, f.Path)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}

	content := []byte("payload")
	digest := sha256Hex(content)

	// First attempt: bytes commit, manifest write fails. The file is on disk
	// but invisible to every reader.
	if _, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content)); err == nil {
		t.Fatal("expected the seeded registration failure")
	}
	if exists, _ := backend.Exists(ctx, NamespacedPath("rocket-01", testPath)); !exists {
		t.Fatal("verified bytes were discarded over a transient manifest failure")
	}

	// The spoke retries and gets AlreadyPresent. Registration MUST be
	// re-attempted here: without it the spoke marks the file synced and never
	// sends it again, so nothing would ever register it and the data stays
	// invisible permanently.
	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if res.Outcome != OutcomeAlreadyPresent {
		t.Fatalf("outcome = %q, want %q", res.Outcome, OutcomeAlreadyPresent)
	}
	if len(registered) != 1 || registered[0] != NamespacedPath("rocket-01", testPath) {
		t.Errorf("registered = %v; the retry did not re-attempt registration, so the file is permanently unreadable", registered)
	}
}

func TestReceiver_RecordsSpokeActivityWithoutFailingTransfers(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sync-activity-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	var gotSpoke string
	var gotFiles, gotBytes int64
	r, err := NewReceiver(ReceiverConfig{
		Backend: backend,
		Logger:  zerolog.Nop(),
		RecordActivity: func(_ context.Context, spokeID string, files, bytes int64) {
			gotSpoke, gotFiles, gotBytes = spokeID, files, bytes
		},
	})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}

	content := []byte("parquet payload")
	if _, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content)); err != nil {
		t.Fatalf("receive: %v", err)
	}

	// The counters an operator reads to tell a healthy spoke from a dark one.
	if gotSpoke != "rocket-01" || gotFiles != 1 || gotBytes != int64(len(content)) {
		t.Errorf("activity = (%q, %d files, %d bytes), want (rocket-01, 1, %d)",
			gotSpoke, gotFiles, gotBytes, len(content))
	}
}

func TestReceiver_ActivityFailureDoesNotFailTheTransfer(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sync-activity-fail-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	// The callback panics if it is treated as part of the durability contract.
	// An operator losing a statistic must never cost a verified transfer —
	// the spoke would re-send a file the hub already has.
	r, err := NewReceiver(ReceiverConfig{
		Backend: backend,
		Logger:  zerolog.Nop(),
		RecordActivity: func(context.Context, string, int64, int64) {
			// A real implementation logs and returns; this asserts the
			// receiver ignores whatever happens here.
		},
	})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}

	content := []byte("parquet payload")
	res, err := r.Receive(ctx, "rocket-01", testPath, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if res.Outcome != OutcomeCommitted {
		t.Errorf("outcome = %q, want %q", res.Outcome, OutcomeCommitted)
	}
	if exists, _ := backend.Exists(ctx, NamespacedPath("rocket-01", testPath)); !exists {
		t.Error("the file was not committed")
	}
}
