package compaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
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
	// CanCompact reports whether the local node may run compaction. When
	// the scheduler sees false, it logs a clear "gated" message and stays
	// idle (NOT running, NOT an error).
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
	// clusterGate is the Phase 4 role check. When non-nil, Start() and
	// TriggerNow() both consult it and refuse to run when CanCompact is
	// false. nil gate means "no check, allow" — used by OSS / standalone
	// paths and by existing tests that predate Phase 4.
	clusterGate ClusterGate

	cron       *cron.Cron
	running    bool
	roleGated  bool // true when Start found CanCompact=false; used by Status
	stopCh     chan struct{}

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
		stopCh:      make(chan struct{}),
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
	if s.clusterGate != nil && !s.clusterGate.CanCompact() {
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
	_, err := s.cron.AddFunc(s.schedule, func() {
		s.runCompaction()
	})
	if err != nil {
		return err
	}

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

	// Stop cron scheduler
	if s.cron != nil {
		ctx := s.cron.Stop()
		<-ctx.Done() // Wait for running jobs to complete
	}

	s.running = false
	s.logger.Info().Msg("Compaction scheduler stopped")
}

// runCompaction runs one compaction cycle
func (s *Scheduler) runCompaction() {
	startTime := time.Now()
	s.logger.Info().Msg("Triggering scheduled compaction")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	cycleID, err := s.manager.RunCompactionCycleForTiers(ctx, s.tierNames)
	if err != nil {
		s.logger.Error().Err(err).Int64("cycle_id", cycleID).Msg("Scheduled compaction failed")
		return
	}

	duration := time.Since(startTime)
	s.logger.Info().
		Int64("cycle_id", cycleID).
		Dur("duration", duration).
		Msg("Scheduled compaction completed")
}

// ErrCompactionRoleGated is returned by TriggerNow on a node whose role
// does not have CanCompact (Phase 4). It lets API handlers surface a
// clear "this node is not the compactor" message instead of a generic
// 500 or a silent no-op.
var ErrCompactionRoleGated = fmt.Errorf("compaction: node role is not compactor")

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

	s.logger.Info().Msg("Manual compaction trigger")

	startTime := time.Now()
	cycleID, err := s.manager.RunCompactionCycleForTiers(ctx, s.tierNames)
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
