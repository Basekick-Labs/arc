package api

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// syncSpyBackend is the local backend with its ObjectMover.Sync observed (and
// optionally failed), to see that the DELETE rewrite makes its output durable
// before the caller goes on to remove the old file (#1618).
type syncSpyBackend struct {
	*storage.LocalBackend
	root    string
	synced  []string
	existed []bool
	err     error
}

func (s *syncSpyBackend) Sync(ctx context.Context, key string) error {
	s.synced = append(s.synced, key)
	_, statErr := os.Stat(filepath.Join(s.root, key))
	s.existed = append(s.existed, statErr == nil)
	if s.err != nil {
		return s.err
	}
	return s.LocalBackend.Sync(ctx, key)
}

func spyOnRewriteSync(r *wideRig, err error) *syncSpyBackend {
	spy := &syncSpyBackend{LocalBackend: r.h.storage.(*storage.LocalBackend), root: r.root, err: err}
	r.h.storage = spy
	return spy
}

func TestDeleteRewriteSyncsTheNewFileBeforeReturning(t *testing.T) {
	single := false
	r := newWideRig(t, &single)
	spy := spyOnRewriteSync(r, nil)
	const rows = 2000
	out, err := r.rewrite(r.file(rows, 2), rows)
	if err != nil {
		t.Fatal(err)
	}
	rel, _ := filepath.Rel(r.root, out)
	if len(spy.synced) != 1 || spy.synced[0] != filepath.ToSlash(rel) {
		t.Fatalf("synced %v, want exactly the rewritten file %s", spy.synced, rel)
	}
	if !spy.existed[0] {
		t.Fatal("synced before the rewritten file had its final name")
	}
}

// A rewrite that cannot be made durable fails and publishes nothing, so the
// caller keeps the old file.
func TestDeleteRewriteThatCannotSyncFailsAndKeepsTheOldFile(t *testing.T) {
	single := false
	r := newWideRig(t, &single)
	injected := errors.New("injected sync failure")
	spy := spyOnRewriteSync(r, injected)
	const rows = 2000
	full := r.file(rows, 2)
	if _, err := r.rewrite(full, rows); !errors.Is(err, injected) {
		t.Fatalf("rewrite = %v, want the sync failure", err)
	}
	if len(spy.synced) != 1 {
		t.Fatalf("synced %v", spy.synced)
	}
	if _, err := os.Stat(filepath.Join(r.root, spy.synced[0])); !os.IsNotExist(err) {
		t.Fatalf("the unsynced rewrite is still published: %v", err)
	}
	if _, err := os.Stat(full); err != nil {
		t.Fatalf("the old file must stay: %v", err)
	}
}
