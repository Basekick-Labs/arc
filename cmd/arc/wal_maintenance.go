package main

import (
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// walPromptPurgeInterval is the least time between two purges driven by
// wal.Writer.PurgeHint (#676). The periodic pass alone purged every
// wal.recovery_interval_seconds (300 s by default), long enough at a high
// ingest rate for the WAL to fill its volume; on every rotation and floor
// release instead, the WAL holds about what is still buffered.
const walPromptPurgeInterval = time.Second

// walPurgeLimiter runs a purge on a hint at most once per interval, and never
// loses one: a hint inside the interval schedules a single trailing purge for
// when it ends. It is driven from the WAL maintenance goroutine — hint() on
// each PurgeHint, fire() when trailing() delivers — so a purge never runs
// beside the recovery pass, which deletes WAL files too. Not safe for
// concurrent use.
type walPurgeLimiter struct {
	interval time.Duration
	purge    func()
	last     time.Time
	timer    *time.Timer
}

func newWALPurgeLimiter(interval time.Duration, purge func()) *walPurgeLimiter {
	return &walPurgeLimiter{interval: interval, purge: purge}
}

// hint records a purge hint: purges now if the last purge ended at least an
// interval ago, otherwise makes sure one purge follows when it has.
func (l *walPurgeLimiter) hint() {
	if l.timer != nil {
		return // a trailing purge is already due and covers this hint
	}
	if wait := time.Until(l.last.Add(l.interval)); wait > 0 {
		l.timer = time.NewTimer(wait)
		return
	}
	l.run()
}

// trailing delivers when a scheduled trailing purge is due; nil (never ready
// in a select) when none is.
func (l *walPurgeLimiter) trailing() <-chan time.Time {
	if l.timer == nil {
		return nil
	}
	return l.timer.C
}

// fire runs the trailing purge trailing() announced.
func (l *walPurgeLimiter) fire() {
	l.timer = nil
	l.run()
}

func (l *walPurgeLimiter) run() {
	l.purge()
	// From the END of the pass: a slow pass (a large pending set during a
	// flush outage) still leaves the interval free.
	l.last = time.Now()
}

func (l *walPurgeLimiter) stop() {
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
}

// walPurgePass is the purge the WAL maintenance goroutine runs, both on a
// purge hint and from the periodic pass: rotated files below the unflushed
// floor, then the files recovery retained for the registration hold (#1207
// S4c) once their batches are registered. Neither is gated on a flush
// failure: the floor keeps every entry no checkpoint covers, and a failed
// flush leaves its entries pending.
type walPurgePass struct {
	writer *wal.Writer
	logger zerolog.Logger

	lastHeldWarn time.Time
	lastErrLog   time.Time
}

// prompt is the hint-driven purge. Its logging is sampled: it runs up to once
// a second, so a persistent failure (a file that cannot be removed) would
// otherwise log every second.
func (p *walPurgePass) prompt() {
	if _, err := p.writer.PurgeFlushed(p.writer.MinUnflushedSequence()); err != nil {
		p.sampledError(err, "Prompt WAL purge failed; retried on the next rotation or flush")
	}
	p.releaseRetained()
}

// releaseRetained deletes the retained files whose batches are registered and
// reports how many files the registration hold keeps.
func (p *walPurgePass) releaseRetained() {
	if _, err := p.writer.ReleaseRetained(); err != nil {
		p.sampledError(err, "Releasing retained WAL files failed")
	}
	held := p.writer.HeldFiles()
	metrics.Get().SetWALHeldFiles(int64(held))
	if held > 0 && time.Since(p.lastHeldWarn) >= 10*time.Minute {
		p.lastHeldWarn = time.Now()
		p.logger.Warn().Int("files", held).
			Msg("WAL files are kept until the files their batches were flushed into are registered; the WAL grows while registrations cannot land (see arc_cluster_file_registration_oldest_pending_seconds)")
	}
}

func (p *walPurgePass) sampledError(err error, msg string) {
	if time.Since(p.lastErrLog) < time.Minute {
		return
	}
	p.lastErrLog = time.Now()
	p.logger.Error().Err(err).Msg(msg + " (sampled: at most one line a minute)")
}
