package iceberg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// A backup restore that stages a catalog pauses the export until the restart
// that applies it (#1813).
//
// The staged catalog replaces this process's at the restart, so whatever the
// export commits in between is thrown away with it, and some of it does harm
// first: on a warehouse outside the storage root, where nothing is
// published, each commit's delete-after-commit can remove the very metadata
// file the restored row points at, and after the restart every pass of that
// table fails on the missing file with nothing to recover it from. So once
// the restore has decided to stage a catalog, no export or drop starts, and
// the ones in flight are waited for, before it writes anything.
//
// Every export holds one share of the pause (holdDatabase), and so does a
// drop (lockDatabaseForDrop); the pause takes them all. It is this process's
// and is never lifted except by the restore that took it failing before it
// staged the catalog; the restart ends it.

// errExportPaused is holdDatabase's and lockDatabaseForDrop's refusal while a
// restore holds the pause.
var errExportPaused = errors.New("iceberg: export is paused until the restart that applies a restored catalog")

// restorePauseWait is the longest a pause waits for the exports in flight.
// A var for tests.
var restorePauseWait = 150 * time.Second

type exportPause struct {
	once sync.Once
	sem  *semaphore.Weighted
	// held is set while a restore holds the pause. A second restore before
	// the restart finds it held and goes on (the last restore before the
	// restart wins, restore.go) instead of waiting for a pause that is
	// never lifted.
	held atomic.Bool
}

func (p *exportPause) get() *semaphore.Weighted {
	p.once.Do(func() { p.sem = semaphore.NewWeighted(databaseLockShares) })
	return p.sem
}

// holdExport holds one share of the pause, or refuses while it is taken.
func (e *Exporter) holdExport() (release func(), err error) {
	if e.restorePause.held.Load() {
		return nil, errExportPaused
	}
	sem := e.restorePause.get()
	if !sem.TryAcquire(1) {
		return nil, errExportPaused
	}
	return func() { sem.Release(1) }, nil
}

// PauseExportForRestore stops every export and drop of this process: it
// waits for the ones in flight (at most restorePauseWait, or until ctx ends)
// and keeps new ones out. resume lifts the pause; a restore calls it only
// when it fails before staging its catalog, and otherwise the pause lasts
// until the process restarts.
func (e *Exporter) PauseExportForRestore(ctx context.Context, reason string) (resume func(), err error) {
	p := &e.restorePause
	sem := p.get()
	if p.held.Load() {
		// An earlier restore holds it until the restart; this one restages.
		// Its resume must not lift the earlier one's pause.
		return func() {}, nil
	}
	wctx, cancel := context.WithTimeout(ctx, restorePauseWait)
	defer cancel()
	if err := sem.Acquire(wctx, databaseLockShares); err != nil {
		return nil, fmt.Errorf("an Iceberg export or drop was still running (or the restore was cancelled), so the export could not be paused for the restore: %w", err)
	}
	p.held.Store(true)
	e.logger.Warn().Str("reason", reason).
		Msg("Iceberg export paused: a restore is staging a catalog, which replaces this one at the next start; no table is exported or dropped until the server restarts")
	var once sync.Once
	return func() {
		once.Do(func() {
			p.held.Store(false)
			sem.Release(databaseLockShares)
			e.logger.Info().Str("reason", reason).Msg("Iceberg export resumed: the restore did not stage a catalog")
		})
	}, nil
}

// exportPausedForRestore reports that a restore holds the pause.
func (e *Exporter) exportPausedForRestore() bool {
	return e.restorePause.held.Load()
}
