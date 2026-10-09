package cluster

// The tier-recorder seam: the local-delete workers report what they unlinked
// so this node's tier rows follow its own disk, and the recorder can be wired on
// an already-running coordinator (the tiering manager is built long after
// Start) and deliberately survives into the shutdown drain.

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type unlinkReport struct {
	path   string
	reason string
	size   int64
}

type fakeTierRecorder struct {
	mu       sync.Mutex
	pulled   []string
	unlinked []unlinkReport
}

func (r *fakeTierRecorder) RecordReplicatedFile(path string, sizeBytes int64) {
	r.mu.Lock()
	r.pulled = append(r.pulled, path)
	r.mu.Unlock()
}

func (r *fakeTierRecorder) RecordUnlinkedFile(path, reason string, sizeBytes int64) {
	r.mu.Lock()
	r.unlinked = append(r.unlinked, unlinkReport{path: path, reason: reason, size: sizeBytes})
	r.mu.Unlock()
}

func (r *fakeTierRecorder) unlinkReports() []unlinkReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]unlinkReport, len(r.unlinked))
	copy(out, r.unlinked)
	return out
}

func waitUnlinkReports(t *testing.T, r *fakeTierRecorder, n int) []unlinkReport {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := r.unlinkReports(); len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return r.unlinkReports()
}

func TestUnlinkOne_ReportsReasonAndMeasuredSize(t *testing.T) {
	backend := newMemBackend()
	const path = "db1/cpu/2026/10/03/14/a.parquet"
	body := []byte("twelve bytes")
	if err := backend.Write(nil, path, body); err != nil { //nolint:staticcheck // memBackend ignores ctx
		t.Fatalf("seed backend: %v", err)
	}

	rec := &fakeTierRecorder{}
	c := newDeleteRig(t, backend, false, nil)
	c.SetTierRecorder(rec)

	c.enqueueLocalDelete(path, "tiering:migrated")

	reports := waitUnlinkReports(t, rec, 1)
	if len(reports) != 1 {
		t.Fatalf("got %d unlink reports, want 1: %+v", len(reports), reports)
	}
	if reports[0].path != path {
		t.Fatalf("path = %q, want %q", reports[0].path, path)
	}
	// The reason decides whether the tiering side spends a cold existence
	// check, so it has to arrive as enqueued.
	if reports[0].reason != "tiering:migrated" {
		t.Fatalf("reason = %q, want tiering:migrated", reports[0].reason)
	}
	// Measured before the delete: the manifest entry is already gone by the
	// time a delete reaches a worker, and a tier row's size_bytes is NOT NULL.
	if reports[0].size != int64(len(body)) {
		t.Fatalf("size = %d, want %d", reports[0].size, len(body))
	}
}

func TestUnlinkOne_ReportsSizeForStagedFile(t *testing.T) {
	const path = "db1/cpu/2026/10/03/14/partial.parquet"
	prefix := []byte("partial")
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	pr, pw := io.Pipe()
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- backend.WriteReader(context.Background(), path, pr, int64(len(prefix)+1))
	}()
	if _, err := pw.Write(prefix); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	if err := pw.CloseWithError(errors.New("simulated interrupted transfer")); err != nil {
		t.Fatalf("close partial transfer: %v", err)
	}
	if err := <-writeErr; err == nil {
		t.Fatal("WriteReader succeeded after an interrupted transfer")
	}

	rec := &fakeTierRecorder{}
	c := newDeleteRig(t, backend, false, nil)
	c.SetTierRecorder(rec)
	c.enqueueLocalDelete(path, "tiering:migrated")

	reports := waitUnlinkReports(t, rec, 1)
	if len(reports) != 1 {
		t.Fatalf("got %d unlink reports, want 1: %+v", len(reports), reports)
	}
	if reports[0].size != int64(len(prefix)) {
		t.Fatalf("size = %d, want staged size %d", reports[0].size, len(prefix))
	}
	if size, err := backend.StagedSize(context.Background(), path); err != nil || size != -1 {
		t.Fatalf("staged size after delete = (%d, %v), want (-1, nil)", size, err)
	}
}

// Delete reports success for a path that was never here — LocalBackend treats
// a missing file as already deleted — so the delete alone is not evidence this
// node held the file. Every node in a cluster sees every manifest delete; only
// the ones that actually had the file may touch their rows.
func TestUnlinkOne_DoesNotReportAPathThisNodeNeverHeld(t *testing.T) {
	backend := newMemBackend()
	rec := &fakeTierRecorder{}
	c := newDeleteRig(t, backend, false, nil)
	c.SetTierRecorder(rec)

	c.enqueueLocalDelete("db1/cpu/2026/10/03/14/never-here.parquet", "tiering:migrated")

	// Give the worker its grace period and then some.
	time.Sleep(900 * time.Millisecond)
	if reports := rec.unlinkReports(); len(reports) != 0 {
		t.Fatalf("reported a path this node never held: %+v", reports)
	}
}

// The tiering manager is constructed long after the coordinator starts, so the
// recorder has to be attachable to a running coordinator.
func TestSetTierRecorder_TakesEffectOnARunningCoordinator(t *testing.T) {
	backend := newMemBackend()
	const before = "db1/cpu/2026/10/03/14/before.parquet"
	const after = "db1/cpu/2026/10/03/14/after.parquet"
	if err := backend.Write(nil, before, []byte("x")); err != nil { //nolint:staticcheck
		t.Fatalf("seed: %v", err)
	}
	if err := backend.Write(nil, after, []byte("y")); err != nil { //nolint:staticcheck
		t.Fatalf("seed: %v", err)
	}

	rec := &fakeTierRecorder{}
	c := newDeleteRig(t, backend, false, nil)

	// No recorder yet: this unlink is covered by the startup tier scan, not by
	// a report.
	c.enqueueLocalDelete(before, "tiering:migrated")
	time.Sleep(900 * time.Millisecond)
	if reports := rec.unlinkReports(); len(reports) != 0 {
		t.Fatalf("reported before the recorder was wired: %+v", reports)
	}

	c.SetTierRecorder(rec)
	c.enqueueLocalDelete(after, "tiering:migrated")

	reports := waitUnlinkReports(t, rec, 1)
	if len(reports) != 1 || reports[0].path != after {
		t.Fatalf("got %+v, want one report for %q", reports, after)
	}
}

// The recorder is deliberately NOT cleared by Stop: the tiering manager
// registers its shutdown hook after the coordinator's and equal-priority
// hooks run in registration order, so the drainer is still alive while the
// delete workers drain — and those unlinks are real, so their reports should
// land. An earlier revision cleared it here and threw that work away on every
// graceful shutdown.
func TestStop_LeavesTheRecorderWiredForTheDeleteDrain(t *testing.T) {
	backend := newMemBackend()
	const path = "db1/cpu/2026/10/03/14/a.parquet"
	if err := backend.Write(nil, path, []byte("x")); err != nil { //nolint:staticcheck
		t.Fatalf("seed: %v", err)
	}

	rec := &fakeTierRecorder{}
	c := newDeleteRig(t, backend, false, nil)
	c.SetTierRecorder(rec)

	// Queue an unlink, then stop the workers the way Stop does: the drain
	// must still report it.
	c.enqueueLocalDelete(path, "tiering:migrated")
	c.stopDeleteWorkers(c.deleteStop, c.deleteWg)
	c.deleteStop = nil

	if reports := rec.unlinkReports(); len(reports) != 1 {
		t.Fatalf("got %d reports from the shutdown drain, want 1: %+v", len(reports), reports)
	}
}

// Nil recorder is every OSS and every tiering-disabled deployment.
func TestTierRecorderWrappers_NilRecorderIsANoOp(t *testing.T) {
	c := &Coordinator{logger: zerolog.Nop()}
	c.recordPulledFileInTiering("db1/cpu/2026/10/03/14/a.parquet", 1)
	c.recordUnlinkedFileInTiering("db1/cpu/2026/10/03/14/a.parquet", "compaction:7", 1)
}
