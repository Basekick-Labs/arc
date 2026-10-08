package backup

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// recordingBackend is a Backend that fails every write and records the keys it
// was asked to Delete. It deliberately does NOT implement StagingInspector, so
// it stands in for S3 and Azure, the two destinations a backup target can name
// (#1101).
type recordingBackend struct {
	storage.Backend // nil: every unimplemented method panics if reached

	writeErr  error
	deleteErr error
	deleted   []string
}

var errFakeWrite = errors.New("fake write failure")

func (b *recordingBackend) WriteReader(context.Context, string, io.Reader, int64) error {
	if b.writeErr != nil {
		return b.writeErr
	}
	return errFakeWrite
}

func (b *recordingBackend) Delete(_ context.Context, path string) error {
	b.deleted = append(b.deleted, path)
	return b.deleteErr
}

// stagingBackend is recordingBackend plus StagingInspector, standing in for
// LocalBackend: it must take the DeleteStaged path and must NOT be sent a
// Delete of the destination key.
type stagingBackend struct {
	recordingBackend
	staged    []string
	stagedErr error
}

func (b *stagingBackend) DeleteStaged(_ context.Context, path string) error {
	b.staged = append(b.staged, path)
	return b.stagedErr
}

func (b *stagingBackend) StagedSize(context.Context, string) (int64, error) {
	return -1, nil
}

func (b *stagingBackend) ReadStaged(context.Context, string, io.Writer) error {
	return nil
}

func (b *stagingBackend) ListStaged(context.Context, string) ([]storage.ObjectInfo, error) {
	return nil, nil
}

// Compile-time proof that the fakes model the shapes the code type-asserts
// on: one destination that stages and one that does not. A fake that silently
// stopped satisfying StagingInspector would make the staging test pass for
// the wrong reason.
var (
	_ storage.Backend          = (*recordingBackend)(nil)
	_ storage.StagingInspector = (*stagingBackend)(nil)
)

func newTestManagerWithBackupStorage(b storage.Backend, logOut io.Writer) *Manager {
	logger := zerolog.New(logOut).Level(zerolog.DebugLevel)
	return &Manager{backupStorage: b, logger: logger}
}

// A failed write to a non-staging backup destination is compensated with a
// Delete of the destination key.
func TestCleanupPartialBackupWriteDeletesOnANonStagingDestination(t *testing.T) {
	var out bytes.Buffer
	fake := &recordingBackend{}
	m := newTestManagerWithBackupStorage(fake, &out)

	m.cleanupPartialBackupWrite(context.Background(), m.defaultDestination(), "backup-20260101-000000-abcdef12/data/db/cpu/x.parquet")

	if len(fake.deleted) != 1 {
		t.Fatalf("Delete called %d times, want 1: a failed write to a remote backup destination must be compensated", len(fake.deleted))
	}
	if want := "backup-20260101-000000-abcdef12/data/db/cpu/x.parquet"; fake.deleted[0] != want {
		t.Errorf("Delete(%q), want Delete(%q)", fake.deleted[0], want)
	}
}

// A staging destination still takes the DeleteStaged path, and must not also
// be sent a Delete: the committed object at the key is not this run's.
func TestCleanupPartialBackupWriteUsesStagingWhenAvailable(t *testing.T) {
	var out bytes.Buffer
	fake := &stagingBackend{}
	m := newTestManagerWithBackupStorage(fake, &out)

	m.cleanupPartialBackupWrite(context.Background(), m.defaultDestination(), "backup-x/data/db/cpu/x.parquet")

	if len(fake.staged) != 1 || fake.staged[0] != "backup-x/data/db/cpu/x.parquet" {
		t.Errorf("DeleteStaged calls = %v, want exactly the destination key", fake.staged)
	}
	if len(fake.deleted) != 0 {
		t.Errorf("Delete calls = %v; a staging backend leaves a .part file, not a committed object", fake.deleted)
	}
}

// A compensation that itself fails is logged and swallowed. The write error is
// the one the caller must surface; masking it with a cleanup failure is how
// the real cause gets lost.
func TestCleanupPartialBackupWriteSwallowsItsOwnFailure(t *testing.T) {
	var out bytes.Buffer
	fake := &recordingBackend{deleteErr: errors.New("storage unreachable")}
	m := newTestManagerWithBackupStorage(fake, &out)

	m.cleanupPartialBackupWrite(context.Background(), m.defaultDestination(), "backup-x/data/db/cpu/x.parquet")

	if len(fake.deleted) != 1 {
		t.Fatalf("Delete called %d times, want 1", len(fake.deleted))
	}
	logged := out.String()
	if !strings.Contains(logged, "Could not remove the backup destination key") {
		t.Errorf("the failed compensation was not logged; log was %q", logged)
	}
	if !strings.Contains(logged, `"level":"debug"`) {
		t.Errorf("the failed compensation must be logged at debug, not louder; log was %q", logged)
	}
}

// The restore path must NOT delete its destination key on a non-staging
// backend. This is the guard that keeps #1101's compensation from becoming
// data loss: a failed S3 PutObject or Azure block-blob commit leaves the
// PREVIOUS object at the key intact, and that object is a live, manifest-
// registered data file the restore was overwriting. Deleting it turns a failed
// write into a lost file with a manifest entry nothing re-registers.
func TestCleanupPartialWriteNeverDeletesALiveDataKey(t *testing.T) {
	var out bytes.Buffer
	fake := &recordingBackend{}
	m := newTestManagerWithBackupStorage(fake, &out)

	m.cleanupPartialWrite(context.Background(), fake, "db/cpu/2026/10/06/14/live.parquet")

	if len(fake.deleted) != 0 {
		t.Fatalf("Delete called with %v; the restore destination may hold a live data file that the failed write did not replace", fake.deleted)
	}

	// The staging variant is still compensated there, because a ".part" file
	// is unambiguously this run's.
	staging := &stagingBackend{}
	m2 := newTestManagerWithBackupStorage(staging, &out)
	m2.cleanupPartialWrite(context.Background(), staging, "db/cpu/2026/10/06/14/live.parquet")
	if len(staging.staged) != 1 {
		t.Errorf("DeleteStaged calls = %v, want the destination key", staging.staged)
	}
	if len(staging.deleted) != 0 {
		t.Errorf("Delete calls = %v, want none", staging.deleted)
	}
}

// The SQLite copy had no compensation at all before #1101. Driven through the
// real call site rather than through the helper, so removing the call fails
// here.
func TestBackupSQLiteFileCompensatesAFailedWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (a INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	fake := &recordingBackend{}
	m := newTestManagerWithBackupStorage(fake, &out)

	err = m.backupSQLiteFile(ctx, m.defaultDestination(), "backup-20260101-000000-abcdef12", dbPath, "arc.db")
	if err == nil {
		t.Fatal("backupSQLiteFile must return the write failure")
	}
	if len(fake.deleted) != 1 {
		t.Fatalf("Delete called %d times, want 1: the SQLite copy must compensate its failed write too", len(fake.deleted))
	}
	if want := "backup-20260101-000000-abcdef12/metadata/arc.db"; fake.deleted[0] != want {
		t.Errorf("Delete(%q), want Delete(%q)", fake.deleted[0], want)
	}
}
