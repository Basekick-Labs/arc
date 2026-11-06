package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// The headline of #1141: a restore of N cold files writes their tier rows in
// ONE call, not N. Each row used to be its own implicit transaction, and so
// its own fsync, on the single shared SQLite connection — so a restore
// serialised auth, audit, MQTT and ingest tier registration behind one write
// per file for its whole duration.
func TestColdRestoreWritesTierRowsInOneBatchCall(t *testing.T) {
	ctx := context.Background()
	rig := newColdRig(t, nil)
	cold := []string{
		"prod/cpu/2026/01/01/00/a.parquet",
		"prod/cpu/2026/01/01/01/b.parquet",
		"prod/mem/2026/01/01/00/c.parquet",
	}
	for _, p := range cold {
		rig.writeCold(t, p, "PAR1"+p)
	}

	result, err := rig.m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
		t.Fatal(err)
	}

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	if n := rig.cold.records.Load(); n != 1 {
		t.Errorf("tier rows were written in %d calls for %d files, want 1: the batch is the whole point of #1141", n, len(cold))
	}
	for _, p := range cold {
		if rig.cold.recorded[p] == 0 {
			t.Errorf("no cold tier row recorded for %s; without it the query path cannot route to the file", p)
		}
	}
	if p := rig.m.GetProgress(); p == nil || p.ColdFilesRestoredToCold != int64(len(cold)) {
		t.Errorf("cold_files_restored_to_cold = %v, want %d", p, len(cold))
	}
}

// The cap is the only flush trigger besides the end of the run, so a batch
// larger than it flushes mid-loop and again at the end — and the first flush
// is exactly coldRowBatchSize rows, not the whole lot.
func TestColdRowBatchFlushesAtTheCap(t *testing.T) {
	ctx := context.Background()
	src := &fakeColdSource{}
	m := &Manager{coldSource: src, logger: zerolog.Nop()}
	batch := newColdRowBatch()
	progress := &Progress{}

	const files = coldRowBatchSize + 1
	for i := 0; i < files; i++ {
		m.queueColdRow(ctx, batch, fmt.Sprintf("prod/cpu/2026/01/01/00/f%d.parquet", i), 10, progress)
	}
	if n := src.records.Load(); n != 1 {
		t.Fatalf("%d calls after queueing %d rows, want 1 at the cap", n, files)
	}
	m.flushColdRows(ctx, batch, progress)
	if n := src.records.Load(); n != 2 {
		t.Fatalf("%d calls after the final flush, want 2", n)
	}
	want := []int{coldRowBatchSize, 1}
	if len(src.callSizes) != 2 || src.callSizes[0] != want[0] || src.callSizes[1] != want[1] {
		t.Errorf("call sizes = %v, want %v: the cap must bound the write lock hold, so a flush may not exceed it", src.callSizes, want)
	}
	if progress.ColdFilesRestoredToCold != files {
		t.Errorf("cold_files_restored_to_cold = %d, want %d", progress.ColdFilesRestoredToCold, files)
	}
}

// Batching must not collapse the per-file outcome. "41 of 50 written, 9
// quarantined" is what an operator can act on; a batch that reported one
// aggregate failure would be strictly worse than the unbatched version.
func TestColdRowBatchCountsQuarantinedAndFailedSeparately(t *testing.T) {
	ctx := context.Background()
	const (
		good        = "prod/cpu/2026/01/01/00/good.parquet"
		quarantined = "prod/cpu/2026/01/01/00/quarantined.parquet"
		unparseable = "not-a-data-path.parquet"
	)
	src := &fakeColdSource{
		quarantined: map[string]bool{quarantined: true},
		unparseable: map[string]bool{unparseable: true},
	}
	m := &Manager{coldSource: src, logger: zerolog.Nop()}
	batch := newColdRowBatch()
	progress := &Progress{}

	for _, p := range []string{good, quarantined, unparseable} {
		m.queueColdRow(ctx, batch, p, 10, progress)
	}
	m.flushColdRows(ctx, batch, progress)

	if progress.ColdFilesRestoredToCold != 1 {
		t.Errorf("cold_files_restored_to_cold = %d, want 1", progress.ColdFilesRestoredToCold)
	}
	if progress.ColdRestoreQuarantineSkipped != 1 {
		t.Errorf("cold_restore_quarantine_skipped = %d, want 1: a quarantined row is a fact tiering established, not a failure", progress.ColdRestoreQuarantineSkipped)
	}
	if progress.ColdRowsNotRecorded != 1 {
		t.Errorf("cold_rows_not_recorded = %d, want 1 for the path no row could be attempted for", progress.ColdRowsNotRecorded)
	}
	if src.recorded[good] == 0 {
		t.Error("the good path in a batch with a quarantined and an unparseable one was not written")
	}
}

// A transaction-level failure means NOTHING in the chunk was written, so the
// whole chunk is unrecorded — and the restore still completes, because the
// bytes are in the store and the rest of the restore is sound.
func TestColdRowBatchChunkFailureCountsTheWholeChunk(t *testing.T) {
	ctx := context.Background()
	src := &fakeColdSource{recordErr: fmt.Errorf("database is locked")}
	m := &Manager{coldSource: src, logger: zerolog.Nop()}
	batch := newColdRowBatch()
	progress := &Progress{}

	const files = 5
	for i := 0; i < files; i++ {
		m.queueColdRow(ctx, batch, fmt.Sprintf("prod/cpu/2026/01/01/00/f%d.parquet", i), 10, progress)
	}
	m.flushColdRows(ctx, batch, progress)

	if progress.ColdRowsNotRecorded != files {
		t.Errorf("cold_rows_not_recorded = %d, want %d: one transaction per call means a failure loses the whole chunk", progress.ColdRowsNotRecorded, files)
	}
	if progress.ColdFilesRestoredToCold != 0 {
		t.Errorf("cold_files_restored_to_cold = %d, want 0", progress.ColdFilesRestoredToCold)
	}
	if progress.ColdRestoreQuarantineSkipped != 0 {
		t.Errorf("cold_restore_quarantine_skipped = %d, want 0: a failed transaction is not a quarantine", progress.ColdRestoreQuarantineSkipped)
	}
}

// The hot fallback batches the same way, and its own counter is NOT the row's:
// ColdFilesRestoredToHot counts the routing decision, taken before the bytes
// are written, so the flush must not touch it.
func TestHotFallbackBatchMovesRowsInOneCall(t *testing.T) {
	ctx := context.Background()
	src := &fakeColdSource{}
	m := &Manager{coldSource: src, logger: zerolog.Nop()}
	batch := newColdRowBatch()
	progress := &Progress{}

	paths := []string{"prod/cpu/2026/01/01/00/a.parquet", "prod/cpu/2026/01/01/01/b.parquet"}
	for _, p := range paths {
		m.queueHotRow(ctx, batch, p, 20, progress)
	}
	m.flushColdRows(ctx, batch, progress)

	if n := src.hotRecords.Load(); n != 1 {
		t.Errorf("%d hot calls for %d files, want 1", n, len(paths))
	}
	for _, p := range paths {
		if src.recordedHot[p] != 20 {
			t.Errorf("no forced hot row for %s; the ordinary report cannot move a cold row, which is why this exists", p)
		}
	}
	if progress.ColdFilesRestoredToHot != 0 {
		t.Errorf("cold_files_restored_to_hot = %d, want 0 from the flush: it counts the routing decision, not the row", progress.ColdFilesRestoredToHot)
	}
}

// cancellingColdBackend cancels the restore after its first successful write,
// so the copy loop takes its ctx.Done() exit with a tier row already pending.
type cancellingColdBackend struct {
	storage.Backend
	cancel func()
	writes int
}

func (b *cancellingColdBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	err := b.Backend.WriteReader(ctx, path, r, size)
	b.writes++
	if err == nil && b.cancel != nil {
		b.cancel()
	}
	return err
}

func (b *cancellingColdBackend) Write(ctx context.Context, path string, data []byte) error {
	err := b.Backend.Write(ctx, path, data)
	b.writes++
	if err == nil && b.cancel != nil {
		b.cancel()
	}
	return err
}

// Decision 5 of #1141, and the case it most needs to cover: the restore
// returns because its context died, and the rows for bytes ALREADY in the
// cold store are still written. The final flush therefore runs on a context
// detached from the restore's — on the restore's own it would fail to begin a
// transaction and write nothing, in exactly the situation the deferred flush
// exists for. A later cold-metadata scan can repair missing rows, including
// on a standalone node, but the files remain unreadable until that scan
// succeeds.
func TestColdRestoreFlushesTierRowsAfterACancelledRestore(t *testing.T) {
	rig := newColdRig(t, nil)
	first := "prod/cpu/2026/01/01/00/a.parquet"
	second := "prod/cpu/2026/01/01/01/b.parquet"
	rig.writeCold(t, first, "PAR1a")
	rig.writeCold(t, second, "PAR1b")

	result, err := rig.m.CreateBackup(context.Background(), BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(rig.coldDir, "prod")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wrapped := &cancellingColdBackend{Backend: rig.cold.backend, cancel: cancel}
	rig.cold.backend = wrapped

	if _, err := rig.m.RestoreBackup(ctx, RestoreOptions{BackupID: result.Manifest.BackupID, RestoreData: true}); err == nil {
		t.Fatal("the restore reported success after its context was cancelled")
	}
	if wrapped.writes == 0 {
		t.Fatal("no cold write happened, so there is no pending row and the test proves nothing")
	}
	if len(rig.cold.recorded) == 0 {
		t.Errorf("no tier row was written for the %d cold file(s) already in the store; those bytes are unreadable and nothing else will record them", wrapped.writes)
	}
	if p := rig.m.GetProgress(); p == nil || p.ColdFilesRestoredToCold == 0 {
		t.Errorf("cold_files_restored_to_cold = %v, want the rows the detached flush wrote to be published", p)
	}
}

// The cap flush must survive a dead context just as the final one does, and
// this is the case that proves it: the restore runs under a fixed
// operation_timeout deadline, so that deadline can land anywhere — including
// inside a mid-loop flush of a full chunk on the single shared connection,
// which is the longest-running thing the restore does to SQLite.
//
// There is nothing to retry from if it is lost: the flush takes the map before
// it calls the recorder, so the rows are already out of the batch and the
// deferred flush finds nothing. A thousand files would be left with bytes in
// the cold store and no tier row, unqueryable, behind a single warning.
func TestColdRowBatchFlushesAtTheCapOnADeadContext(t *testing.T) {
	src := &fakeColdSource{}
	m := &Manager{coldSource: src, logger: zerolog.Nop()}
	batch := newColdRowBatch()
	progress := &Progress{}

	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < coldRowBatchSize-1; i++ {
		m.queueColdRow(ctx, batch, fmt.Sprintf("prod/cpu/2026/01/01/00/f%d.parquet", i), 10, progress)
	}
	if n := src.records.Load(); n != 0 {
		t.Fatalf("%d calls before the cap, want 0", n)
	}
	// The deadline lands between the last queue and the one that trips the cap.
	cancel()
	m.queueColdRow(ctx, batch, "prod/cpu/2026/01/01/00/last.parquet", 10, progress)

	if n := src.records.Load(); n != 1 {
		t.Fatalf("%d calls after tripping the cap, want 1", n)
	}
	if progress.ColdRowsNotRecorded != 0 {
		t.Errorf("cold_rows_not_recorded = %d, want 0: the cap flush must detach from the restore context like the final one does, or a deadline inside it loses the whole chunk", progress.ColdRowsNotRecorded)
	}
	if progress.ColdFilesRestoredToCold != coldRowBatchSize {
		t.Errorf("cold_files_restored_to_cold = %d, want %d", progress.ColdFilesRestoredToCold, coldRowBatchSize)
	}
	if len(src.recorded) != coldRowBatchSize {
		t.Errorf("%d rows recorded, want %d", len(src.recorded), coldRowBatchSize)
	}
}
