package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
)

// cronParser is the single shared cron parser used to validate the
// schedule string at construction and to compute the next run time
// for status reporting. Sharing it across NewScheduler / Start /
// nextRun avoids re-allocating an identical parser on every Status()
// call (an API hot path).
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// Scheduler wraps a Reconciler with cron + lifecycle + status, mirroring
// internal/scheduler/retention_scheduler.go almost exactly. The split
// (Reconciler is the engine; Scheduler is the cron host) keeps the core
// algorithm unit-testable without dragging cron timing into every test.
//
// The act-mode policy (Druid-style: opt-in flag, then auto-act once
// enabled, with grace window + blast cap as guard rails) is enforced
// here. Cron ticks call Reconcile with dryRun=cfg.ManifestOnlyDryRun so
// operators with the safety bridge enabled get report-only runs even on
// the cron path.
type Scheduler struct {
	reconciler  *Reconciler
	schedule    string
	scheduleObj cron.Schedule // parsed once in NewScheduler; reused by Start + nextRun
	cron        *cron.Cron
	logger      zerolog.Logger

	mu      sync.Mutex
	running bool
}

// SchedulerConfig is constructed by main.go after the Reconciler.
type SchedulerConfig struct {
	Reconciler *Reconciler
	// Schedule is a 5-field cron expression; empty defaults to
	// "17 4 * * *" (daily 04:17, offset from retention 03:* and
	// compaction :05).
	Schedule string
	Logger   zerolog.Logger
}

// NewScheduler constructs a scheduler. The reconciler is required; a
// default cron schedule is filled in if the caller passes empty. The
// parsed cron.Schedule is cached on the struct so Start and nextRun
// don't re-parse on every call (Status() hits nextRun on every API
// request).
func NewScheduler(cfg SchedulerConfig) (*Scheduler, error) {
	if cfg.Reconciler == nil {
		return nil, fmt.Errorf("reconciliation: reconciler is required")
	}
	schedule := cfg.Schedule
	if schedule == "" {
		schedule = "17 4 * * *"
	}
	parsed, err := cronParser.Parse(schedule)
	if err != nil {
		return nil, fmt.Errorf("reconciliation: invalid cron schedule %q: %w", schedule, err)
	}
	return &Scheduler{
		reconciler:  cfg.Reconciler,
		schedule:    schedule,
		scheduleObj: parsed,
		logger:      cfg.Logger.With().Str("component", "reconciliation-scheduler").Logger(),
	}, nil
}

// Start arms the cron job. Returns nil and logs Warn if reconciliation
// is disabled (Enabled=false in the reconciler's Config), matching the
// pattern at internal/scheduler/retention_scheduler.go:96.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		s.logger.Warn().Msg("Reconciliation scheduler already running")
		return nil
	}
	if !s.reconciler.cfg.Enabled {
		s.logger.Warn().Msg("Reconciliation feature disabled — scheduler not starting")
		return nil
	}

	s.cron = cron.New(cron.WithParser(cronParser))
	if _, err := s.cron.AddFunc(s.schedule, s.tick); err != nil {
		return fmt.Errorf("reconciliation: add cron func: %w", err)
	}
	s.cron.Start()
	s.running = true

	s.logger.Info().
		Str("schedule", s.schedule).
		Time("next_run", s.nextRun()).
		Bool("dry_run_only", s.reconciler.cfg.ManifestOnlyDryRun).
		Str("backend_kind", string(s.reconciler.cfg.BackendKind)).
		Msg("Reconciliation scheduler started")
	return nil
}

// Stop halts the cron and waits for any in-flight tick to finish.
// Idempotent — calling Stop on a non-running scheduler is a no-op.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	if s.cron != nil {
		ctx := s.cron.Stop()
		<-ctx.Done()
	}
	s.running = false
	s.logger.Info().Msg("Reconciliation scheduler stopped")
}

// IsRunning returns true while the cron is armed.
func (s *Scheduler) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// tick is the cron callback. The reconciler's own runState mutex
// prevents overlap, but we still log the skip case loudly so an
// operator looking at logs can correlate "the cron fired but nothing
// happened" to "the previous run was still going".
func (s *Scheduler) tick() {
	// Use a context bounded by MaxRunDuration so a stuck Reconcile
	// can't block the next tick. Reconcile applies its own
	// MaxRunDuration cap internally, but we belt-and-braces here.
	timeout := s.reconciler.cfg.MaxRunDuration
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	dryRun := s.reconciler.cfg.ManifestOnlyDryRun
	_, err := s.reconciler.Reconcile(ctx, dryRun)
	if err != nil {
		// ErrAlreadyRunning is expected during long runs; ErrGated is
		// expected on non-eligible nodes. Both are debug-level. Use
		// errors.Is so a future code path that wraps these sentinels
		// still classifies correctly — direct == comparison breaks
		// silently the moment anyone wraps the sentinel.
		if errors.Is(err, ErrAlreadyRunning) || errors.Is(err, ErrGated) || errors.Is(err, ErrDisabled) {
			s.logger.Debug().Err(err).Msg("Reconciliation tick skipped")
			return
		}
		s.logger.Error().Err(err).Msg("Reconciliation tick failed")
	}
}

// TriggerNow runs a reconcile cycle immediately. The dry-run choice is
// the caller's: API handlers default to dry_run=true, the cron path
// honors cfg.ManifestOnlyDryRun.
//
// Returns the Run summary alongside any error so API callers can
// surface the run ID even when the run aborted mid-cycle.
func (s *Scheduler) TriggerNow(ctx context.Context, dryRun bool) (*Run, error) {
	if !s.reconciler.cfg.Enabled {
		return nil, ErrDisabled
	}
	return s.reconciler.Reconcile(ctx, dryRun)
}

// Status returns a JSON-serializable snapshot for the API.
func (s *Scheduler) Status() map[string]interface{} {
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()

	cfg := s.reconciler.cfg
	out := map[string]interface{}{
		"enabled":              cfg.Enabled,
		"running":              running,
		"schedule":             s.schedule,
		"backend_kind":         string(cfg.BackendKind),
		"grace_window":         cfg.GraceWindow.String(),
		"clock_skew_allowance": cfg.ClockSkewAllowance.String(),
		"max_run_duration":     cfg.MaxRunDuration.String(),
		"max_manifest_size":    cfg.MaxManifestSize,
		"max_deletes_per_run":  cfg.MaxDeletesPerRun,
		"manifest_only_dry_run": cfg.ManifestOnlyDryRun,
		"in_flight":            s.reconciler.IsRunning(),
	}
	if running {
		out["next_run"] = s.nextRun().UTC().Format(time.RFC3339)
	}
	if last := s.reconciler.LastRun(); last != nil {
		out["last_run"] = last
	}
	if recent := s.reconciler.RecentRuns(); len(recent) > 0 {
		out["recent_runs"] = recent
	}
	return out
}

// FindRun proxies to the underlying reconciler's ring buffer for the
// API GET /api/v1/reconciliation/runs/{id} endpoint.
func (s *Scheduler) FindRun(id string) (*Run, bool) {
	return s.reconciler.FindRun(id)
}

// Schedule returns the cron expression. Helpful for tests.
func (s *Scheduler) Schedule() string { return s.schedule }

// Reconciler exposes the underlying engine. Used by the API handler
// to surface lower-level state where the scheduler doesn't add value.
func (s *Scheduler) Reconciler() *Reconciler { return s.reconciler }

// nextRun returns the time of the next scheduled tick. Uses the
// cached cron.Schedule parsed in NewScheduler — the previous
// implementation re-parsed the cron expression on every call, which
// was a hot-path waste because Status() (an authenticated API
// endpoint) calls nextRun on every request.
func (s *Scheduler) nextRun() time.Time {
	if s.scheduleObj == nil {
		return time.Time{}
	}
	return s.scheduleObj.Next(time.Now())
}
