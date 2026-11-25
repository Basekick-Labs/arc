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
	priorityCompactionStop   = 1 // before ready-flag-off and the HTTP drain
	priorityCompactionWait   = shutdown.PriorityWAL + 1
	priorityIcebergExport    = shutdown.PriorityWAL + 2
	priorityIcebergGlue      = shutdown.PriorityWAL + 3
	priorityIcebergCatalogDB = shutdown.PriorityWAL + 4
)

// compactionWaitMargin is the part of the shutdown budget the wait leaves,
// before the kill, for the Iceberg drain and the closes after it.
func compactionWaitMargin(timeout time.Duration) time.Duration { return timeout / 5 }

// compactionKillGrace bounds the wait for the claim's release after the
// kill, when the shutdown context has no deadline left.
const compactionKillGrace = 5 * time.Second

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
		kctx, kcancel := context.WithTimeout(context.Background(), compactionKillGrace)
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
	case <-time.After(compactionKillGrace):
		s.logger.Warn().Msg("Compaction schedulers did not stop in time; shutdown continues")
	}
	return nil
}

func (s *compactionShutdown) Close() error { return s.CloseContext(context.Background()) }

// registerCompactionShutdown registers both steps.
func registerCompactionShutdown(c *shutdown.Coordinator, s *compactionShutdown) {
	c.RegisterHook("compaction-stop", s.begin, priorityCompactionStop)
	c.Register("compaction", s, priorityCompactionWait)
}
