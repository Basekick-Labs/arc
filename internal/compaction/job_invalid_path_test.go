package compaction

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// newDownloadJob builds the smallest Job that downloadFiles needs. It is driven
// directly rather than through Job.Run because running a compaction requires
// DuckDB (the only Job-level tests are behind the duckdb_arrow build tag), and
// the behaviour under test is entirely in the download stage.
func newDownloadJob(t *testing.T, backend storage.Backend, files []string) *Job {
	t.Helper()
	return &Job{
		Measurement:    "cpu",
		PartitionPath:  "testdb/cpu/2026/04/11/14",
		Files:          files,
		StorageBackend: backend,
		Database:       "testdb",
		Tier:           "hourly",
		JobID:          "job_download",
		logger:         zerolog.Nop(),
	}
}

// TestDownloadFilesSkipsUnusableInput covers the site the issue describes as
// "a skippable input becomes a hard job failure": ReadTo fails permanently, the
// recovery calls Exists and gates on checkErr == nil, and because Exists fails
// identically on the same key the "already compacted, skipping" escape hatch is
// unreachable. Every compaction cycle then failed the whole job on it.
//
// The unusable key sits at index 1 of 3 deliberately. downloadFiles runs a
// worker pool, so a bug that depends on the bad entry arriving first or last
// would survive a two-file table.
func TestDownloadFilesSkipsUnusableInput(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	defer backend.Close()
	ctx := context.Background()

	good1 := "testdb/cpu/2026/04/11/14/a.parquet"
	bad := `testdb\cpu/2026/04/11/14/b.parquet`
	good2 := "testdb/cpu/2026/04/11/14/c.parquet"
	for _, k := range []string{good1, good2} {
		if err := backend.Write(ctx, k, []byte("parquet-bytes")); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}

	job := newDownloadJob(t, backend, []string{good1, bad, good2})
	files, err := job.downloadFiles(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("downloadFiles failed the whole job on one unusable input: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("downloaded %d files, want 2 (the two addressable inputs)", len(files))
	}
	for _, f := range files {
		if f.storageKey == bad {
			t.Fatal("the unusable key was reported as downloaded")
		}
	}
	// BytesBefore must not count the skipped input, or the job reports a
	// compaction ratio computed against bytes it never read.
	if job.BytesBefore != int64(2*len("parquet-bytes")) {
		t.Fatalf("BytesBefore = %d, want %d", job.BytesBefore, 2*len("parquet-bytes"))
	}
}

// readFailingBackend fails ReadTo for one key with a transient error while
// everything else goes to a real LocalBackend.
type readFailingBackend struct {
	storage.Backend
	failKey string
}

func (b *readFailingBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	if path == b.failKey {
		return errors.New("connection reset by peer")
	}
	return b.Backend.ReadTo(ctx, path, w)
}

// TestDownloadFilesFailsJobOnTransientReadError is the over-correction guard.
// A quarantine widened to "any ReadTo error, skip the file" would turn a
// network blip into a silent partial compaction: the surviving inputs get
// merged and deleted while the unread one is quietly dropped from the set, and
// nothing reports that the output is incomplete.
func TestDownloadFilesFailsJobOnTransientReadError(t *testing.T) {
	base, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	defer base.Close()
	ctx := context.Background()

	good := "testdb/cpu/2026/04/11/14/a.parquet"
	flaky := "testdb/cpu/2026/04/11/14/b.parquet"
	for _, k := range []string{good, flaky} {
		if err := base.Write(ctx, k, []byte("parquet-bytes")); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}

	job := newDownloadJob(t, &readFailingBackend{Backend: base, failKey: flaky}, []string{good, flaky})
	if _, err := job.downloadFiles(ctx, t.TempDir()); err == nil {
		t.Fatal("a transient read failure must fail the job; silently compacting the rest loses the unread input's rows")
	}
}
