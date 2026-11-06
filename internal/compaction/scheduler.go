package compaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/metrics"
)

// ClusterGate is the minimal interface the compaction scheduler needs to
// decide whether the local node may run compaction. Phase 4 wires this to
// a closure that checks RoleCompactor against cfg.Cluster.Role; OSS and
// standalone nodes pass a nil gate (or construct without one) and are
// allowed unconditionally.
//
// The interface lives here rather than in the cluster package so the
// scheduler has no compile-time dependency on the cluster package — the
// gate is a function pointer plus a tiny interface, not an import.
type ClusterGate interface {
	// CanCompact reports whether the local node may execute compaction.
	// Gates with temporary fences may additionally implement
	// CanScheduleCompaction() bool to keep cron armed while execution waits.
	CanCompact() bool
	// Role returns the current node's role string for log messages. The
	// scheduler only uses this for human-readable output, never for
	// decision-making — CanCompact is authoritative.
	Role() string
}

// Scheduler schedules compaction jobs using cron-style schedules
type Scheduler struct {
	manager   *Manager
	schedule  string
	tierNames []string // Specific tiers to process (must be non-empty and enabled)
	enabled   bool
	// clusterGate is the Phase 4 role check. Every tick (including every
	// deferred attempt) and TriggerNow consult CanCompact. Start may use the
	// optional CanScheduleCompaction method to arm cron during temporary fences.
	// Assigned only in NewScheduler, so a tick reads it without s.mu, which
	// Stop holds while it waits for running ticks. nil gate means "no check,
	// allow" for OSS / standalone paths and existing tests.
	clusterGate ClusterGate

	cron      *cron.Cron
	running   bool
	roleGated bool          // true when Start found CanCompact=false; used by Status
	stopCh    chan struct{} // closed by Stop, so a deferred tick stops waiting; one per Start

	// nextTickForTest replaces getNextRun when non-nil.
	nextTickForTest func() time.Time
	// afterClaimRefusedForTest, when non-nil, runs in a deferred tick between
	// a refused claim and the wait for its release: the window the release
	// signal, taken before the attempt, must cover.
	afterClaimRefusedForTest func()
	// beforeStopWaitForTest, when non-nil, runs in Stop after this scheduler
	// is signalled and before it waits for running jobs.
	beforeStopWaitForTest func()

	logger zerolog.Logger
	mu     sync.Mutex
}

// SchedulerConfig holds configuration for creating a compaction scheduler
type SchedulerConfig struct {
	Manager     *Manager
	Schedule    string      // Cron schedule string (e.g., "5 * * * *" for every hour at :05)
	TierNames   []string    // Specific tiers to process (must be non-empty and enabled)
	Enabled     bool        // Enable automatic scheduling
	ClusterGate ClusterGate // Optional Phase 4 role check; nil means "no gate, allow"
	Logger      zerolog.Logger
}

// NewScheduler creates a new compaction scheduler
func NewScheduler(cfg *SchedulerConfig) (*Scheduler, error) {
	// Default schedule: every hour at :05
	if cfg.Schedule == "" {
		cfg.Schedule = "5 * * * *"
	}

	// Validate cron schedule
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	if _, err := parser.Parse(cfg.Schedule); err != nil {
		return nil, err
	}

	s := &Scheduler{
		manager:     cfg.Manager,
		schedule:    cfg.Schedule,
		tierNames:   cfg.TierNames,
		enabled:     cfg.Enabled,
		clusterGate: cfg.ClusterGate,
		logger:      cfg.Logger.With().Str("component", "compaction-scheduler").Logger(),
	}

	if cfg.Enabled {
		tierInfo := "all tiers"
		if len(cfg.TierNames) > 0 {
			tierInfo = fmt.Sprintf("tiers: %v", cfg.TierNames)
		}
		s.logger.Info().
			Str("schedule", cfg.Schedule).
			Str("tiers", tierInfo).
			Msg("Compaction scheduler initialized")
	} else {
		s.logger.Debug().
			Str("schedule", cfg.Schedule).
			Msg("Compaction scheduler disabled")
	}

	return s, nil
}

// Start starts the compaction scheduler
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.enabled {
		s.logger.Debug().Msg("Compaction scheduler disabled, not starting")
		return nil
	}

	if s.running {
		s.logger.Warn().Msg("Compaction scheduler already running")
		return nil
	}

	// Phase 4: role gate. When a cluster gate is wired and reports that
	// the local node cannot compact, we stay idle (NOT running, NOT an
	// error). This is the "only compactor nodes compact in cluster mode"
	// enforcement, placed at the single chokepoint so scattered goroutines
	// don't need individual checks. OSS/standalone pass no gate and
	// short-circuit past this check.
	allowed := s.clusterGate == nil || s.clusterGate.CanCompact()
	if gate, ok := s.clusterGate.(interface{ CanScheduleCompaction() bool }); ok {
		// A temporary license fence must not permanently disable cron. The
		// execution and manual-trigger paths still check CanCompact every time.
		allowed = gate.CanScheduleCompaction()
	}
	if !allowed {
		s.roleGated = true
		s.logger.Info().
			Str("role", s.clusterGate.Role()).
			Str("expected_role", "compactor").
			Msg("Compaction scheduler gated: node role does not have CanCompact capability. Scheduler idle.")
		return nil
	}
	s.roleGated = false

	// Create cron instance
	s.cron = cron.New(cron.WithParser(cron.NewParser(
		cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
	)))

	// Add compaction job
	stop := make(chan struct{})
	_, err := s.cron.AddFunc(s.schedule, func() {
		s.tick(stop)
	})
	if err != nil {
		return err
	}
	s.stopCh = stop

	// Start cron scheduler
	s.cron.Start()
	s.running = true

	// Log next run time
	nextRun := s.getNextRun()
	s.logger.Info().
		Str("schedule", s.schedule).
		Time("next_run", nextRun).
		Msg("Compaction scheduler started")

	return nil
}

// Stop stops the compaction scheduler
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	// A deferred tick must stop waiting, or the wait below lasts until the next
	// tick.
	s.signalStopLocked()
	if s.beforeStopWaitForTest != nil {
		s.beforeStopWaitForTest()
	}

	// Stop cron scheduler
	if s.cron != nil {
		ctx := s.cron.Stop()
		<-ctx.Done() // Wait for running jobs to complete
	}

	s.running = false
	s.logger.Info().Msg("Compaction scheduler stopped")
}

// signalStop tells this scheduler's deferred ticks to stop waiting, without
// waiting for anything: StopAll signals every scheduler before stopping any.
func (s *Scheduler) signalStop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signalStopLocked()
}

func (s *Scheduler) signalStopLocked() {
	if s.running && s.stopCh != nil {
		close(s.stopCh)
		s.stopCh = nil
	}
}

// StopAll stops schedulers that share a Manager. Every one is told to stop
// before any is waited for: Stop waits for a running cycle, and a deferred
// tick of a scheduler not yet stopped would wake when that cycle releases the
// claim and start a new cycle during shutdown (#220). nil entries are skipped.
func StopAll(schedulers ...*Scheduler) {
	for _, s := range schedulers {
		if s != nil {
			s.signalStop()
		}
	}
	for _, s := range schedulers {
		if s != nil {
			s.Stop()
		}
	}
}

// tick is what a cron tick runs. stop ends a tick still waiting for the claim.
func (s *Scheduler) tick(stop <-chan struct{}) {
	if s.runCompaction() {
		s.runDeferredTick(stop)
	}
}

// runCompaction runs one compaction cycle for a tick, and reports whether the
// tick found another cycle holding the claim, so nothing ran.
func (s *Scheduler) runCompaction() (claimHeld bool) {
	if !s.tickAllowed() {
		return false
	}
	return errors.Is(s.runScheduledCycle(), ErrCycleAlreadyRunning)
}

// tickAllowed reports whether this node may run a scheduled cycle now, and logs
// why not.
func (s *Scheduler) tickAllowed() bool {
	// Re-check the gate on EVERY tick, not only in Start().
	//
	// Start() runs before the cluster coordinator exists (main.go starts the
	// schedulers, then builds the coordinator, then wires it into the gate),
	// so at that moment CanCompact falls back to the static role capability
	// and a compactor node arms its cron unconditionally. If the compactor
	// lease then sits on another node — which is the normal state on a
	// cluster where a writer took it while the compactor was still joining —
	// nothing stopped this cron, because the lose-the-lease callback only
	// fires on the node that HELD it. Two nodes then compacted the same
	// partitions into two outputs, both registered in the manifest: the
	// duplicate-output hazard the single-compactor lease exists to prevent.
	//
	// This is also what CLAUDE.md's cluster-ops rule requires — the gate
	// belongs in the execution path so a lease change takes effect without a
	// restart — and this scheduler is the file that rule cites as the
	// reference pattern.
	//
	// Read without s.mu: the gate never changes after NewScheduler, and Stop
	// holds s.mu while it waits for this job to return.
	gate := s.clusterGate
	if gate != nil && !gate.CanCompact() {
		s.logger.Debug().
			Str("role", gate.Role()).
			Msg("Scheduled compaction skipped: this node does not hold the compactor lease")
		return false
	}
	if reason := pauseReasonOf(gate); reason != "" {
		s.logger.Info().
			Str("pause", reason).
			Msg("Scheduled compaction skipped: compaction is paused cluster-wide")
		return false
	}
	return true
}

// runScheduledCycle claims a cycle and runs it, logging its outcome. It returns
// ErrCycleAlreadyRunning, unlogged, when another cycle holds the claim: whether
// that costs the tick is for the caller to say (#220).
func (s *Scheduler) runScheduledCycle() error {
	claim, err := s.manager.ClaimCycle()
	if err != nil {
		return err
	}
	defer claim.Release()

	startTime := time.Now()
	s.logger.Info().
		Dur("cycle_timeout", s.manager.CycleTimeout).
		Msg("Triggering scheduled compaction")

	ctx, cancel := context.WithTimeout(context.Background(), s.manager.CycleTimeout)
	defer cancel()

	err = s.manager.runClaimed(ctx, claim.ID, cycleSourceScheduler, nil, s.tierNames, "")
	if err != nil {
		switch {
		case errors.Is(err, ErrCompactionPaused):
			s.logger.Info().Int64("cycle_id", claim.ID).Dur("duration", time.Since(startTime)).Msg("Scheduled compaction stopped at a batch boundary: compaction is paused cluster-wide")
		case ctx.Err() != nil:
			s.logger.Info().Err(ctx.Err()).Int64("cycle_id", claim.ID).Dur("duration", time.Since(startTime)).Msg("Scheduled compaction interrupted")
		default:
			s.logger.Error().Err(err).Int64("cycle_id", claim.ID).Dur("duration", time.Since(startTime)).Msg("Scheduled compaction failed")
		}
		return err
	}

	duration := time.Since(startTime)
	s.logger.Info().
		Int64("cycle_id", claim.ID).
		Dur("duration", duration).
		Msg("Scheduled compaction completed")
	return nil
}

// runDeferredTick gives a tick that found another cycle holding the claim its
// one run, as soon as the claim is free (#220): the hourly and daily schedulers
// and the manual trigger share one claim, and without this a collision costs
// the tier a whole period, or every day's run if it repeats. It gives up at the
// scheduler's next tick, which then stands for itself, and on Stop. Each attempt
// passes the same gate as a tick, so losing the compactor lease or a pause ends
// the wait like any tick those would have stopped.
func (s *Scheduler) runDeferredTick(stop <-chan struct{}) {
	next := s.nextTick()
	s.logger.Info().
		Strs("tiers", s.tierNames).
		Time("next_run", next).
		Msg("Scheduled compaction deferred: a cycle is already running; it runs when that cycle ends")

	expired := time.NewTimer(time.Until(next))
	defer expired.Stop()
	skipped := func() {
		for _, tier := range s.tierNames {
			metrics.Get().IncCompactionScheduledSkips(tier)
		}
		s.logger.Warn().
			Strs("tiers", s.tierNames).
			Time("next_run", next).
			Msg("Scheduled compaction skipped: a cycle held the claim until the next tick (see arc_compaction_scheduled_skips_total)")
	}
	for {
		// Taken before the attempt: a release after the attempt fails closes
		// this signal, so it cannot be missed. Another scheduler can win the
		// claim first; then this waits for that cycle too.
		released := s.manager.cycleReleaseSignal()
		select {
		case <-stop:
			return
		default:
		}
		// The next tick stands for itself from here: a release and the
		// deadline can be ready together, and the wait below picks either.
		select {
		case <-expired.C:
			skipped()
			return
		default:
		}
		if !s.tickAllowed() {
			return
		}
		if !errors.Is(s.runScheduledCycle(), ErrCycleAlreadyRunning) {
			return
		}
		if s.afterClaimRefusedForTest != nil {
			s.afterClaimRefusedForTest()
		}
		select {
		case <-released:
		case <-expired.C:
			skipped()
			return
		case <-stop:
			return
		}
	}
}

// nextTick is when the next tick of this schedule fires.
func (s *Scheduler) nextTick() time.Time {
	if s.nextTickForTest != nil {
		return s.nextTickForTest()
	}
	return s.getNextRun()
}

// ErrCompactionRoleGated is returned by TriggerNow on a node whose role
// does not have CanCompact (Phase 4). It lets API handlers surface a
// clear "this node is not the compactor" message instead of a generic
// 500 or a silent no-op.
var ErrCompactionRoleGated = fmt.Errorf("compaction: node role is not compactor")

// pauseReporter is the optional half of a ClusterGate (#1087): a gate that
// can also report the cluster-wide compaction pause in force, as a
// human-readable reason, or "" when compaction is not paused. It is NOT part
// of ClusterGate on purpose: Start treats a false CanCompact as permanent role
// gating and never arms cron, so a node that acquired the compactor lease
// during a pause, or a dedicated compactor rebooting mid-pause, would stay
// idle after the resume. The pause is therefore checked per tick and per
// manual trigger, and Start still arms cron while paused.
type pauseReporter interface {
	CompactionPauseReason() string
}

// pauseReasonOf returns the pause in force according to gate, or "" when the
// gate is nil, cannot report one, or compaction is not paused.
func pauseReasonOf(gate ClusterGate) string {
	if gate == nil {
		return ""
	}
	if pr, ok := gate.(pauseReporter); ok {
		return pr.CompactionPauseReason()
	}
	return ""
}

// TriggerNow triggers compaction immediately (manual trigger)
// Returns the cycle ID and any error that occurred
func (s *Scheduler) TriggerNow(ctx context.Context) (int64, error) {
	// Phase 4: the same role gate applies to manual triggers as to the
	// cron path — otherwise an operator could accidentally force a
	// non-compactor node to run compaction and trip the shared-storage
	// duplicate-output bug we just designed this to prevent.
	s.mu.Lock()
	gate := s.clusterGate
	s.mu.Unlock()
	if gate != nil && !gate.CanCompact() {
		s.logger.Warn().
			Str("role", gate.Role()).
			Msg("Manual compaction trigger rejected: node role is not compactor")
		return 0, ErrCompactionRoleGated
	}
	if reason := pauseReasonOf(gate); reason != "" {
		s.logger.Info().
			Str("pause", reason).
			Msg("Manual compaction trigger rejected: compaction is paused cluster-wide")
		return 0, fmt.Errorf("%w: %s", ErrCompactionPaused, reason)
	}

	s.logger.Info().Msg("Manual compaction trigger")

	// cycleSourceUnspecified, not cycleSourceScheduler: this is the manual
	// path, as the name and the log line above both say. It has no production
	// caller today, and labelling it "scheduler" would misattribute whichever
	// caller is added next -- most plausibly an API route, which is the one
	// thing "scheduler" must not mean (#1162).

	startTime := time.Now()
	cycleID, err := s.manager.runCycleInternal(ctx, cycleSourceUnspecified, nil, s.tierNames)
	if err != nil {
		return cycleID, err
	}

	duration := time.Since(startTime)
	s.logger.Info().
		Int64("cycle_id", cycleID).
		Dur("duration", duration).
		Msg("Manual compaction completed")

	return cycleID, nil
}

// getNextRun returns the next scheduled run time
func (s *Scheduler) getNextRun() time.Time {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(s.schedule)
	if err != nil {
		return time.Time{}
	}
	return schedule.Next(time.Now())
}

// Status returns scheduler status
func (s *Scheduler) Status() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := map[string]interface{}{
		"enabled":  s.enabled,
		"running":  s.running,
		"schedule": s.schedule,
	}

	if s.running {
		status["next_run"] = s.getNextRun().Format(time.RFC3339)
	}

	// Phase 4: expose role-gate state so operators hitting
	// /api/v1/cluster/status can see why a node isn't compacting.
	if s.clusterGate != nil {
		status["role_gated"] = s.roleGated
		status["gate_role"] = s.clusterGate.Role()
	}

	return status
}

// IsEnabled returns whether the scheduler is enabled
func (s *Scheduler) IsEnabled() bool {
	return s.enabled
}

// IsRunning returns whether the scheduler is running
func (s *Scheduler) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}
