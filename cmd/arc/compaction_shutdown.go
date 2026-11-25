package main

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/rs/zerolog"
)

// Compaction at shutdown (#1887), in two steps:
//
//   - a hook, first of all (priorityCompactionStop), stops compaction at its
//     next boundary: no new cycle is claimed, scheduled or triggered, and a
//     running cycle stops before its next batch, measurement or split half.
//     Batches already running keep going through the HTTP drain and the
//     buffer's flush.
//   - a component after the WAL's close (priorityCompactionWait) waits for the
//     running cycle to release its claim. At its deadline it kills the cycle
//     (its subprocess gets SIGKILL through exec.CommandContext) and waits for
//     the release again, so the parent's after-child steps run while storage
//     and the databases are still open, and no child outlives the parent to
//     race a replacement process. A kill is a crash for the job, which the
//     next start recovers from its manifest.
//
// The wait is a component, not a hook: every hook runs before every
// component, and a hook that waited would hold the buffer's final flush
// behind a compaction batch.
//
// The Iceberg export's drain, Glue and its catalog handle come after the
// wait, before the cluster coordinator (shutdown.PriorityCompaction).
const (
	priorityCompactionStop = 1 // before ready-flag-off and the HTTP drain
	priorityCompactionWait = shutdown.PriorityWAL + 1
	// The cluster's completion watcher drains the records the last batches
	// left after they ended, and before Raft stops: a component after the
	// wait. As a hook it ran before the wait, and a batch ending after its
	// final drain left its record unapplied.
	priorityCompactionWatcher = shutdown.PriorityWAL + 2
	priorityIcebergExport     = shutdown.PriorityWAL + 3
	priorityIcebergGlue       = shutdown.PriorityWAL + 4
	priorityIcebergCatalogDB  = shutdown.PriorityWAL + 5
)

// compactionWaitMargin is the part of the shutdown budget the wait leaves,
// before the kill, for the Iceberg drain and the closes after it.
func compactionWaitMargin(timeout time.Duration) time.Duration { return timeout / 5 }

// compactionKillGrace bounds the wait for the claim's release after the
// kill, and for the schedulers' stop; never past the shutdown deadline
// (graceWithin).
const compactionKillGrace = 5 * time.Second

// compactionExitGrace bounds the final kill's wait (killLeftoverCycles).
const compactionExitGrace = 2 * time.Second

// graceWithin is compactionKillGrace, cut to what is left of ctx's deadline,
// and at least 100ms: the release after a kill takes milliseconds.
func graceWithin(ctx context.Context) time.Duration {
	g := compactionKillGrace
	if dl, ok := ctx.Deadline(); ok {
		g = min(g, time.Until(dl))
	}
	return max(g, 100*time.Millisecond)
}

// compactionStopper is the part of *compaction.Manager shutdown uses.
type compactionStopper interface {
	BeginShutdown()
	WaitIdle(ctx context.Context) bool
	KillCycles()
	RunningCycleID() int64
}

// compactionShutdown is the wait component. Built only with compaction on.
type compactionShutdown struct {
	manager    compactionStopper
	schedulers []*compaction.Scheduler
	timeout    time.Duration
	logger     zerolog.Logger
}

// begin is the early hook.
func (s *compactionShutdown) begin(context.Context) error {
	s.manager.BeginShutdown()
	compaction.StopDispatchAll(s.schedulers...)
	return nil
}

// CloseContext waits for the running cycle, kills it at the deadline, and
// waits for its release.
func (s *compactionShutdown) CloseContext(ctx context.Context) error {
	wctx, cancel := endingBefore(ctx, compactionWaitMargin(s.timeout), s.timeout)
	idle := s.manager.WaitIdle(wctx)
	cancel()
	if !idle {
		id := s.manager.RunningCycleID()
		s.logger.Warn().Int64("cycle_id", id).
			Msg("Compaction cycle still running at the shutdown deadline: killing it; the next start recovers its job from the manifest")
		s.manager.KillCycles()
		kctx, kcancel := context.WithTimeout(context.Background(), graceWithin(ctx))
		idle = s.manager.WaitIdle(kctx)
		kcancel()
		if !idle {
			s.logger.Error().Int64("cycle_id", id).
				Msg("Compaction cycle did not release its claim after the kill; shutdown continues")
			return nil
		}
	}
	// The cron jobs end right after the claim's release.
	done := make(chan struct{})
	go func() {
		compaction.StopAll(s.schedulers...)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(graceWithin(ctx)):
		s.logger.Warn().Msg("Compaction schedulers did not stop in time; shutdown continues")
	}
	return nil
}

func (s *compactionShutdown) Close() error { return s.CloseContext(context.Background()) }

// killLeftoverCycles is the last step before the process exits: the
// coordinator skips the remaining components once its deadline passes, and
// the wait may never have run. A child that outlived the parent would race
// a replacement process (#1887), so kill whatever still runs, and give its
// claim a moment to release.
func killLeftoverCycles(m compactionStopper, logger zerolog.Logger) {
	if m == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	idle := m.WaitIdle(ctx)
	cancel()
	if idle {
		return
	}
	logger.Warn().Int64("cycle_id", m.RunningCycleID()).
		Msg("Compaction cycle still running as Arc exits: killing it; the next start recovers its job from the manifest")
	m.KillCycles()
	ctx, cancel = context.WithTimeout(context.Background(), compactionExitGrace)
	defer cancel()
	m.WaitIdle(ctx)
}

// registerCompactionShutdown registers both steps.
func registerCompactionShutdown(c *shutdown.Coordinator, s *compactionShutdown) {
	c.RegisterHook("compaction-stop", s.begin, priorityCompactionStop)
	c.Register("compaction", s, priorityCompactionWait)
}
