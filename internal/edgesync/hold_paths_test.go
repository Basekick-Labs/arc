package edgesync

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

type holdRecorder struct {
	held     map[string]int
	acquired []string
}

func (h *holdRecorder) hold(paths []string) func() {
	if h.held == nil {
		h.held = map[string]int{}
	}
	for _, p := range paths {
		h.held[p]++
		h.acquired = append(h.acquired, p)
	}
	return func() {
		for _, p := range paths {
			if h.held[p]--; h.held[p] == 0 {
				delete(h.held, p)
			}
		}
	}
}

// A redelivery re-registers the file it finds at the path without writing it,
// so the receive holds the final path until the registration returns
// (#1071).
func TestReceiver_HoldsThePathThroughTheRegistration(t *testing.T) {
	r, _ := newTestReceiver(t)
	h := &holdRecorder{}
	r.holdPath = func(p string) func() { return h.hold([]string{p}) }
	final := NamespacedPath(testSpokeID, testPath)
	registered := 0
	r.registerFile = func(_ context.Context, f *ReceivedFile) error {
		if h.held[final] == 0 {
			t.Fatal("registered without holding the path")
		}
		registered++
		return nil
	}
	ctx := context.Background()
	content := "parquet bytes"
	sum, _ := sha256Of(content)
	for i := 0; i < 2; i++ { // the second is the redelivery
		if _, err := r.Receive(ctx, testSpokeID, testPath, sum, int64(len(content)), 0, strings.NewReader(content)); err != nil {
			t.Fatal(err)
		}
	}
	if registered != 2 || len(h.held) != 0 || len(h.acquired) != 2 || h.acquired[0] != final {
		t.Fatalf("registered %d held %v acquired %v", registered, h.held, h.acquired)
	}
}

// An import holds every path of the bundle until its manifest batches are
// flushed, and releases them after.
func TestImporter_HoldsPathsUntilTheFlush(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, true)
	h := &holdRecorder{}
	rig.importer.holdPaths = h.hold
	flushed := 0
	rig.importer.flushManifest = func(_ context.Context, files []*ReceivedFile) error {
		for _, f := range files {
			if h.held[f.Path] == 0 {
				t.Fatalf("%s flushed without a hold", f.Path)
			}
		}
		flushed += len(files)
		return nil
	}
	dir := exportBundle(t, rig.secret, 3, testHubID)
	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if flushed != 3 || len(h.held) != 0 {
		t.Fatalf("flushed %d, still held %v", flushed, h.held)
	}
	want := NamespacedPath(testSpokeID, fmt.Sprintf("default/cpu/2026/08/07/%02d/f_%04d.parquet", 0, 0))
	if h.acquired[0] != want {
		t.Fatalf("held %v", h.acquired)
	}
}
