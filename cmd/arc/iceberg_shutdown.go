package main

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/shutdown"
)

// Iceberg export at shutdown (#1886).
//
// Every shutdown hook runs before every component. The export's loop used to
// stop in a hook, so the files the ingest buffer's final flush writes (a
// component, PriorityBuffer) asked an export that had already stopped, and
// their tables stayed behind the bucket until the next start; Glue, stopped
// in a hook too, never heard of a version published that late.
//
// Now:
//   - the hook still stops the loop (so no timed pass competes with the final
//     flush), and remembers whether it ended in time;
//   - a component after the WAL's close (priorityIcebergExport) serves what is
//     queued, the final flush's files included (Scheduler.DrainStopped);
//   - Glue flushes after it (priorityIcebergGlue);
//   - the export's own SQLite handle, when it has one, closes last
//     (priorityIcebergCatalogDB), after Glue, which reads through the exporter.
//
// After the WAL, not before: the coordinator checks its deadline only between
// components, and a skipped WAL close loses its newest entries for good
// (main.go, arrow-buffer's budget). Before the cluster coordinator
// (PriorityCompaction): the export's gate reads it. Before auth
// (PriorityAuth), which closes a borrowed SQLite handle, and storage.
//
// The drain uses what is left of the shutdown budget, less a tenth for the
// closes after it and, with Glue on, a twentieth for Glue's flush: nothing is
// taken from the hooks' share. The compaction wait before it
// (priorityCompactionWait, #1887) may use the budget up to a fifth before the
// deadline when a batch overruns, which leaves the drain a tenth (a
// twentieth with Glue). The trade is deliberate: a batch the wait kills is a
// crash the next start recovers, and a child left running would race the
// replacement process. The drain and Glue's flush do network I/O
// among the components, each bounded by its own deadline.
//
// The hook's wait for the loop is bounded. When the loop does not end in
// time (a pass ignoring cancellation), the drain and the catalog handle's
// close are skipped, and that pass may outlive the components after them
// (cluster coordinator, auth, storage): the trade the old unbounded Stop made
// the other way, holding the buffer's final flush behind such a pass.

const (
	// icebergStopWaitMax bounds the hook's wait for the loop, whose pass in
	// flight is cancelled with it; icebergStopWaitMin is the least it waits,
	// even when the hooks have overrun their share.
	icebergStopWaitMax = 10 * time.Second
	icebergStopWaitMin = time.Second
)

// icebergCloseMargin is the part of the shutdown budget the Iceberg steps
// leave for the components after them (cluster coordinator, tiering, auth,
// storage, database): closes, each quick.
func icebergCloseMargin(timeout time.Duration) time.Duration { return timeout / 10 }

// icebergGlueShare is Glue's flush's part of the shutdown budget.
func icebergGlueShare(timeout time.Duration) time.Duration { return timeout / 20 }

// icebergDrainMargin is how long before the shutdown deadline the export's
// drain stops: the closes' margin, plus Glue's share when Glue is on.
func icebergDrainMargin(timeout time.Duration, glue bool) time.Duration {
	m := icebergCloseMargin(timeout)
	if glue {
		m += icebergGlueShare(timeout)
	}
	return m
}

// endingBefore is ctx ending margin before its deadline. Without a deadline
// (a direct Close, outside the coordinator) it ends after fallback.
func endingBefore(ctx context.Context, margin, fallback time.Duration) (context.Context, context.CancelFunc) {
	if dl, ok := ctx.Deadline(); ok {
		return context.WithDeadline(ctx, dl.Add(-margin))
	}
	return context.WithTimeout(ctx, fallback)
}

// icebergManifestSyncer is the part of the cluster coordinator the drain's
// manifest catch-up uses.
type icebergManifestSyncer interface {
	SyncManifestWithin(ctx context.Context, timeout time.Duration) error
}

// icebergManifestCatchUpMax bounds the catch-up: a whole-cluster stop can
// leave a follower with no leader to sync from.
const icebergManifestCatchUpMax = 5 * time.Second

// icebergManifestCatchUp is the drain's `before` on a cluster with a shared
// bucket: a follower exporter learns of the final flush's files (its own
// included) from its FSM apply, so it catches up first. Only where this node
// exports (gate), and for at most icebergManifestCatchUpMax.
func icebergManifestCatchUp(c icebergManifestSyncer, gate interface{ CanRun() bool }, logger zerolog.Logger) func(context.Context) {
	return func(ctx context.Context) {
		if gate != nil && !gate.CanRun() {
			return
		}
		wait := icebergManifestCatchUpMax
		if dl, ok := ctx.Deadline(); ok {
			wait = min(wait, time.Until(dl)/2)
		}
		if err := c.SyncManifestWithin(ctx, wait); err != nil {
			logger.Warn().Err(err).Msg("Iceberg export: the manifest did not catch up before the final export; exporting what is queued, and the next start reconciles the rest")
		}
	}
}

// icebergDrainer is the part of *iceberg.Scheduler the shutdown steps use.
type icebergDrainer interface {
	StopWithin(time.Duration) bool
	DrainStopped(context.Context) int
}

// icebergShutdown carries the export's shutdown steps: the hook that stops
// the loop, and the component that drains it.
type icebergShutdown struct {
	sched   icebergDrainer
	timeout time.Duration // server.shutdown_timeout, validated
	glue    bool          // Glue sync on: its flush after the drain needs a share
	// before runs ahead of the drain, within its deadline: on a cluster with a
	// shared bucket, a manifest sync so the final registrations' arrivals are
	// queued (they reach a follower exporter through its own FSM apply). Nil
	// elsewhere.
	before func(ctx context.Context)
	logger zerolog.Logger

	stopped bool // the hook saw the loop end: the state is the drain's
}

// stopLoop is the hook. Its wait is bounded by what is left of the hooks'
// deadline, within [icebergStopWaitMin, icebergStopWaitMax].
func (s *icebergShutdown) stopLoop(ctx context.Context) error {
	d := icebergStopWaitMax
	if dl, ok := ctx.Deadline(); ok {
		if left := time.Until(dl); left < d {
			d = left
		}
	}
	d = max(d, icebergStopWaitMin)
	s.stopped = s.sched.StopWithin(d)
	return nil
}

// CloseContext is the component: the drain.
func (s *icebergShutdown) CloseContext(ctx context.Context) error {
	if !s.stopped {
		return nil // StopWithin logged it; the loop may still hold the state
	}
	dctx, cancel := endingBefore(ctx, icebergDrainMargin(s.timeout, s.glue), s.timeout/10)
	defer cancel()
	if dctx.Err() != nil {
		s.logger.Warn().Msg("Iceberg export: no shutdown time left for the final export; the next start reconciles the tables")
		return nil
	}
	if s.before != nil {
		s.before(dctx)
	}
	began := time.Now()
	n := s.sched.DrainStopped(dctx)
	ev := s.logger.Debug()
	switch {
	case dctx.Err() != nil:
		ev = s.logger.Warn().Bool("deadline_reached", true)
	case n > 0:
		ev = s.logger.Info()
	}
	ev.Int("tables_changed", n).Dur("took", time.Since(began)).
		Msg("Iceberg export: final export at shutdown done; anything left is reconciled at the next start")
	return nil
}

func (s *icebergShutdown) Close() error { return s.CloseContext(context.Background()) }

// icebergGlueShutdown is Glue's flush, after the drain.
type icebergGlueShutdown struct {
	flush   func(context.Context)
	timeout time.Duration
}

func (g *icebergGlueShutdown) CloseContext(ctx context.Context) error {
	gctx, cancel := endingBefore(ctx, icebergCloseMargin(g.timeout), g.timeout/20)
	defer cancel()
	g.flush(gctx)
	return nil
}

func (g *icebergGlueShutdown) Close() error { return g.CloseContext(context.Background()) }

// registerIcebergShutdown registers the export's shutdown steps. closeDB, when
// set, closes the export's own SQLite handle after Glue; it is skipped when
// the loop did not stop in time, since it may still be using the handle.
func registerIcebergShutdown(c *shutdown.Coordinator, s *icebergShutdown, closeDB func() error) {
	c.RegisterHook("iceberg", s.stopLoop, shutdown.PriorityDatabase)
	c.Register("iceberg-export", s, priorityIcebergExport)
	if closeDB != nil {
		c.Register("iceberg-catalog-db", shutdownFunc(func() error {
			if !s.stopped {
				return nil
			}
			return closeDB()
		}), priorityIcebergCatalogDB)
	}
}

// registerIcebergGlueShutdown registers Glue's flush between the drain and
// the catalog handle's close.
func registerIcebergGlueShutdown(c *shutdown.Coordinator, g *icebergGlueShutdown) {
	c.Register("iceberg-glue", g, priorityIcebergGlue)
}
