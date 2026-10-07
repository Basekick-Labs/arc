package main

// The quiescer main.go hands the coordinator for the cluster-wide compaction
// pause (#1087), with each of its optional inputs absent, and with the
// watcher built but not yet started, as it is when a pause lands during
// Coordinator.Start.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/rs/zerolog"
)

type quiescerTestBridge struct{}

func (quiescerTestBridge) RegisterCompactedFile(context.Context, compaction.CompactedFile) error {
	return nil
}
func (quiescerTestBridge) DeleteCompactedSource(context.Context, string, string) error { return nil }
func (quiescerTestBridge) BatchFileOps(context.Context, []compaction.CompactedFile, []compaction.DeleteSourceOp) error {
	return nil
}

func writePendingManifest(t *testing.T, dir, jobID, state string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]interface{}{"job_id": jobID, "state": state})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, jobID+".json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCompactionQuiescerIssue1087(t *testing.T) {
	t.Run("nothing to quiesce acks at once", func(t *testing.T) {
		// No compaction on this node, and no watcher expected: idle.
		q := newCompactionQuiescer(nil, nil, nil, false, zerolog.Nop())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := q(ctx); err != nil {
			t.Fatalf("no manager, no watcher, none expected: %v", err)
		}
	})

	t.Run("an expected watcher that is missing never reports idle", func(t *testing.T) {
		// This node compacts with replication, so its commits go through a
		// watcher, but the watcher could not be built: nothing can say
		// whether a phase-2 commit is pending, so the node must not ack.
		q := newCompactionQuiescer(nil, nil, nil, true, zerolog.Nop())
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		start := time.Now()
		err := q(ctx)
		if err == nil || !strings.Contains(err.Error(), "watcher not available") {
			t.Fatalf("expected watcher missing: err = %v, want a refusal naming the watcher", err)
		}
		if ctx.Err() != nil {
			t.Fatal("the refusal must be immediate, not a deadline")
		}
		_ = start
	})

	t.Run("a built but unstarted watcher holds the quiesce on a pending sources_deleted commit", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "pending")
		// The pause can land during Coordinator.Start, before the watcher
		// is started: the manifest is on disk and the quiesce must see it.
		pending := writePendingManifest(t, dir, "job-a", "sources_deleted")
		w, err := compaction.NewCompletionWatcher(compaction.CompletionWatcherConfig{Dir: dir, Bridge: quiescerTestBridge{}, Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		// Deliberately NOT started.
		q := newCompactionQuiescer(nil, nil, w, true, zerolog.Nop())

		ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
		defer cancel()
		err = q(ctx)
		if err == nil || !strings.Contains(err.Error(), "sources_deleted") {
			t.Fatalf("a pending phase-2 commit did not hold the quiesce: err = %v", err)
		}

		// Once the commit is gone (the watcher applied and removed it) the
		// node is quiesced; a stuck output_written manifest does not hold it.
		if err := os.Remove(pending); err != nil {
			t.Fatal(err)
		}
		writePendingManifest(t, dir, "job-b", "output_written")
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel2()
		if err := q(ctx2); err != nil {
			t.Fatalf("quiesce with only a stuck output_written manifest: %v", err)
		}
	})
}
