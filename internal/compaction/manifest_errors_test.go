package compaction

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// listFailingBackend fails every List call, standing in for S3 throttling,
// expired credentials, or a network blip.
type listFailingBackend struct {
	storage.Backend
	err error
}

func (b *listFailingBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, b.err
}

func newListFailingManager(t *testing.T, err error) *ManifestManager {
	t.Helper()
	local, lerr := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if lerr != nil {
		t.Fatalf("NewLocalBackend: %v", lerr)
	}
	return NewManifestManager(&listFailingBackend{Backend: local, err: err}, zerolog.Nop())
}

// A storage failure must surface as an error, not as "there are no manifests".
//
// The two are opposite instructions to the caller: an empty manifest set means
// "nothing is being compacted, proceed", while a failed lookup means "we cannot
// tell, so do not touch these files".
func TestListManifests_PropagatesStorageErrors(t *testing.T) {
	sentinel := errors.New("SlowDown: please reduce your request rate")
	m := newListFailingManager(t, sentinel)

	manifests, err := m.ListManifests(context.Background())
	if err == nil {
		t.Fatalf("expected the storage error to propagate; got nil with %d manifests", len(manifests))
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("underlying storage error should be wrapped, got: %v", err)
	}
	if !strings.Contains(err.Error(), "list manifests") {
		t.Errorf("error should name the operation, got: %v", err)
	}
}

// GetFilesInManifests feeds filterCandidateFiles, which has an explicit guard
// that skips a partition when this lookup fails. Swallowing the error made that
// guard unreachable and allowed re-compaction of in-flight files.
func TestGetFilesInManifests_PropagatesStorageErrors(t *testing.T) {
	m := newListFailingManager(t, errors.New("connection reset by peer"))

	files, err := m.GetFilesInManifests(context.Background())
	if err == nil {
		t.Fatalf("expected the storage error to propagate; got nil with %d files — "+
			"filterCandidateFiles would read this as 'nothing is being compacted' and proceed", len(files))
	}
}

// Recovery must not report success over a failed listing: returning (0, nil)
// says "there was nothing to recover", which is a different claim from "we
// could not find out".
func TestRecoverOrphanedManifests_PropagatesStorageErrors(t *testing.T) {
	m := newListFailingManager(t, errors.New("AccessDenied"))

	recovered, err := m.RecoverOrphanedManifests(context.Background())
	if err == nil {
		t.Fatalf("expected the storage error to propagate; got nil with recovered=%d", recovered)
	}
	if recovered != 0 {
		t.Errorf("recovered = %d, want 0 on failure", recovered)
	}
	// The message should not stutter now that ListManifests describes the failure.
	if strings.Count(err.Error(), "failed to list manifests") > 1 {
		t.Errorf("error message is double-wrapped: %v", err)
	}
}

// A missing manifest directory is normal on a fresh install and must stay a
// non-error: the backends already report an absent prefix as an empty listing.
func TestListManifests_EmptyStorageIsNotAnError(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	m := NewManifestManager(local, zerolog.Nop())

	manifests, err := m.ListManifests(context.Background())
	if err != nil {
		t.Fatalf("a fresh install with no manifest directory must not error: %v", err)
	}
	if len(manifests) != 0 {
		t.Errorf("expected no manifests, got %d", len(manifests))
	}
}
