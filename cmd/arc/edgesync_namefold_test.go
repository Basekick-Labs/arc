package main

import (
	"testing"

	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// typeOnlyBackend is a non-local backend for storageFoldsNames: only Type is
// consulted, so the embedded nil interface is never called.
type typeOnlyBackend struct {
	storage.Backend
	kind string
}

func (b typeOnlyBackend) Type() string { return b.kind }

// Object stores compare keys byte for byte: an existing pair of spoke IDs
// that differ only in case stays two namespaces and keeps syncing (#740). A
// local root is probed, so the answer is the filesystem's.
func TestStorageFoldsNames(t *testing.T) {
	for _, kind := range []string{"s3", "azure", "gcs"} {
		if got := storageFoldsNames(typeOnlyBackend{kind: kind}, zerolog.Nop()); got.Any() {
			t.Errorf("%s: storageFoldsNames = %+v, want no folds", kind, got)
		}
	}
	// A local backend whose root cannot be probed (a wrapper hiding
	// GetBasePath) assumes both folds: the collision may be real.
	if got := storageFoldsNames(typeOnlyBackend{kind: "local"}, zerolog.Nop()); got != edgesync.AllNameFolds {
		t.Errorf("unprobeable local: storageFoldsNames = %+v, want AllNameFolds", got)
	}
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	want, err := edgesync.ProbeNameFolding(dir)
	if err != nil {
		t.Fatalf("ProbeNameFolding: %v", err)
	}
	if got := storageFoldsNames(local, zerolog.Nop()); got != want {
		t.Errorf("local: storageFoldsNames = %v, want the probe's %v", got, want)
	}
}
