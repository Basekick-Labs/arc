package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
)

// RetentionClusterGate is the minimal interface the retention scheduler needs
// to decide whether this node may run retention. A nil gate means no check
// (standalone/OSS mode — retention always runs).
type RetentionClusterGate interface {
	// CanRunRetention reports whether the local node may execute retention.
	// When false, the scheduler stays idle — not running, not an error.
	CanRunRetention() bool
	// Role returns a human-readable role string for log messages only.
	Role() string
}

// ErrRetentionRoleGated is returned by TriggerNow when the node's role does
// not permit running retention (e.g. it is a reader, not the primary writer).
var ErrRetentionRoleGated = fmt.Errorf("retention: node role is not primary writer")

// RetentionScheduler manages automatic execution of retention policies on a schedule
type RetentionScheduler struct {
	retentionHandler *api.RetentionHandler
	licenseClient    *license.Client
	clusterGate      RetentionClusterGate
	schedule         string // Cron schedule (e.g., "0 3 * * *" = 3am daily)
	cron             *cron.Cron
	running    bool
	runningJob bool // true while a retention cycle is in progress; prevents overlap
	mu         sync.Mutex
	logger     zerolog.Logger
}

// RetentionSchedulerConfig holds configuration for the retention scheduler
type RetentionSchedulerConfig struct {
	RetentionHandler *api.RetentionHandler
	LicenseClient    *license.Client
	ClusterGate      RetentionClusterGate // nil = standalone, no gate
	Schedule         string               // Cron schedule string (e.g., "0 3 * * *")
	Logger           zerolog.Logger
}

// NewRetentionScheduler creates a new retention scheduler
func NewRetentionScheduler(cfg *RetentionSchedulerConfig) (*RetentionScheduler, error) {
	// Default schedule: daily at 3am
	schedule := cfg.Schedule
	if schedule == "" {
		schedule = "0 3 * * *"
	}

	// Validate cron schedule
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	if _, err := parser.Parse(schedule); err != nil {
		return nil, err
	}

	s := &RetentionScheduler{
		retentionHandler: cfg.RetentionHandler,
		licenseClient:    cfg.LicenseClient,
		clusterGate:      cfg.ClusterGate,
		schedule:         schedule,
		logger:           cfg.Logger.With().Str("component", "retention-scheduler").Logger(),
	}

	s.logger.Info().
		Str("schedule", schedule).
		Msg("Retention scheduler initialized")

	return s, nil
}

// Start starts the retention scheduler
func (s *RetentionScheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		s.logger.Warn().Msg("Retention scheduler already running")
		return nil
	}

	// Check license - require valid license for retention scheduler
	if s.licenseClient == nil || !s.licenseClient.CanUseRetentionScheduler() {
		s.logger.Warn().Msg("Valid enterprise license required for retention scheduler - not starting")
		return nil
	}

	// Create cron instance
	s.cron = cron.New(cron.WithParser(cron.NewParser(
		cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
	)))

	// Add retention job
	_, err := s.cron.AddFunc(s.schedule, func() {
		s.runRetention()
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
		Msg("Retention scheduler started")

	return nil
}

// Stop stops the retention scheduler
func (s *RetentionScheduler) Stop() {
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
	s.logger.Info().Msg("Retention scheduler stopped")
}

// runRetention runs one retention cycle for all active policies
func (s *RetentionScheduler) runRetention() {
	// Check license before execution
	if s.licenseClient == nil || !s.licenseClient.CanUseRetentionScheduler() {
		s.logger.Warn().Msg("Valid enterprise license required, skipping retention execution")
		return
	}

	// Cluster gate: checked on every tick so role transitions (failover,
	// demotion) take effect without a restart. clusterGate is immutable
	// after construction so no lock is needed here.
	if s.clusterGate != nil && !s.clusterGate.CanRunRetention() {
		s.logger.Debug().Str("role", s.clusterGate.Role()).Msg("Retention tick skipped: node is not primary writer")
		return
	}

	// Prevent concurrent execution: if a previous cycle is still running
	// (e.g. slow storage or large dataset), skip this tick entirely.
	s.mu.Lock()
	if s.runningJob {
		s.mu.Unlock()
		s.logger.Warn().Msg("Retention tick skipped: previous cycle still running")
		return
	}
	s.runningJob = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.runningJob = false
		s.mu.Unlock()
	}()

	startTime := time.Now()
	s.logger.Info().Msg("Triggering scheduled retention")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Get all active policies
	policies, err := s.retentionHandler.GetActivePolicies()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to get active retention policies")
		return
	}

	if len(policies) == 0 {
		s.logger.Info().Msg("No active retention policies to execute")
		return
	}

	s.logger.Info().
		Int("policy_count", len(policies)).
		Msg("Executing retention policies")

	// Execute each policy
	var totalDeleted int64
	var successCount int
	var errorCount int

	for _, policy := range policies {
		resp, err := s.retentionHandler.ExecutePolicy(ctx, policy.ID)
		if err != nil {
			s.logger.Error().
				Err(err).
				Int64("policy_id", policy.ID).
				Str("policy_name", policy.Name).
				Msg("Retention policy execution failed")
			errorCount++
			continue
		}

		totalDeleted += resp.DeletedCount
		successCount++

		s.logger.Info().
			Int64("policy_id", policy.ID).
			Str("policy_name", policy.Name).
			Int64("deleted_count", resp.DeletedCount).
			Int("files_deleted", resp.FilesDeleted).
			Msg("Retention policy executed")
	}

	duration := time.Since(startTime)
	s.logger.Info().
		Int("success_count", successCount).
		Int("error_count", errorCount).
		Int64("total_deleted", totalDeleted).
		Dur("duration", duration).
		Msg("Scheduled retention completed")
}

// TriggerNow triggers retention execution immediately
func (s *RetentionScheduler) TriggerNow(ctx context.Context) error {
	// The same gate applies to manual triggers — a reader node must not
	// run retention on demand either. clusterGate is immutable after
	// construction so no lock is needed.
	if s.clusterGate != nil && !s.clusterGate.CanRunRetention() {
		s.logger.Warn().
			Str("role", s.clusterGate.Role()).
			Msg("Manual retention trigger rejected: node is not primary writer")
		return ErrRetentionRoleGated
	}

	// Check license - require valid license for retention scheduler
	if s.licenseClient == nil || !s.licenseClient.CanUseRetentionScheduler() {
		s.logger.Warn().Msg("Valid enterprise license required for retention trigger")
		return nil
	}

	s.mu.Lock()
	if s.runningJob {
		s.mu.Unlock()
		s.logger.Warn().Msg("Manual retention trigger rejected: a cycle is already running")
		return fmt.Errorf("retention cycle already in progress")
	}
	s.runningJob = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.runningJob = false
		s.mu.Unlock()
	}()

	s.logger.Info().Msg("Manual retention trigger")

	startTime := time.Now()

	// Get all active policies
	policies, err := s.retentionHandler.GetActivePolicies()
	if err != nil {
		return err
	}

	if len(policies) == 0 {
		s.logger.Info().Msg("No active retention policies to execute")
		return nil
	}

	// Execute each policy
	var totalDeleted int64
	var lastError error

	for _, policy := range policies {
		resp, err := s.retentionHandler.ExecutePolicy(ctx, policy.ID)
		if err != nil {
			s.logger.Error().
				Err(err).
				Int64("policy_id", policy.ID).
				Str("policy_name", policy.Name).
				Msg("Retention policy execution failed")
			lastError = err
			continue
		}

		totalDeleted += resp.DeletedCount
	}

	duration := time.Since(startTime)
	s.logger.Info().
		Int64("total_deleted", totalDeleted).
		Dur("duration", duration).
		Msg("Manual retention completed")

	return lastError
}

// getNextRun returns the next scheduled run time
func (s *RetentionScheduler) getNextRun() time.Time {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(s.schedule)
	if err != nil {
		return time.Time{}
	}
	return schedule.Next(time.Now())
}

// Status returns scheduler status
func (s *RetentionScheduler) Status() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	licenseValid := false
	if s.licenseClient != nil {
		licenseValid = s.licenseClient.CanUseRetentionScheduler()
	}

	status := map[string]interface{}{
		"running":       s.running,
		"schedule":      s.schedule,
		"license_valid": licenseValid,
	}

	if s.running {
		status["next_run"] = s.getNextRun().Format(time.RFC3339)
	}

	if s.clusterGate != nil {
		status["can_run"] = s.clusterGate.CanRunRetention()
		status["gate_role"] = s.clusterGate.Role()
	}

	return status
}

// IsRunning returns whether the scheduler is running
func (s *RetentionScheduler) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// GetSchedule returns the cron schedule string
func (s *RetentionScheduler) GetSchedule() string {
	return s.schedule
}
