package tiering

import (
	"context"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
)

// Scheduler manages scheduled tier migrations
type Scheduler struct {
	manager  *Manager
	schedule string
	cron     *cron.Cron
	entryID  cron.EntryID
	running  bool
	lastRun  *time.Time
	logger   zerolog.Logger
	mu       sync.RWMutex
}

// SchedulerConfig holds configuration for creating a scheduler
type SchedulerConfig struct {
	Manager  *Manager
	Schedule string // Cron schedule (default: "0 2 * * *" = 2am daily)
	Logger   zerolog.Logger
}

// NewScheduler creates a new migration scheduler
func NewScheduler(cfg *SchedulerConfig) *Scheduler {
	schedule := cfg.Schedule
	if schedule == "" {
		schedule = "0 2 * * *" // Default: 2am daily
	}

	return &Scheduler{
		manager:  cfg.Manager,
		schedule: schedule,
		logger:   cfg.Logger.With().Str("component", "tiering-scheduler").Logger(),
	}
}

// Start starts the scheduler
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	// Create cron scheduler with seconds support
	s.cron = cron.New(cron.WithLocation(time.UTC))

	// Add the migration job
	entryID, err := s.cron.AddFunc(s.schedule, func() {
		s.runMigration()
	})
	if err != nil {
		return err
	}

	s.entryID = entryID
	s.cron.Start()
	s.running = true

	logEvent := s.logger.Info().Str("schedule", s.schedule)
	if nextRun := s.getNextRun(); nextRun != nil {
		logEvent = logEvent.Time("next_run", *nextRun)
	}
	logEvent.Msg("Tiering scheduler started")

	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	if s.cron != nil {
		ctx := s.cron.Stop()
		<-ctx.Done()
		s.cron = nil
	}

	s.running = false
	s.logger.Info().Msg("Tiering scheduler stopped")
}

// IsRunning returns true if the scheduler is running
func (s *Scheduler) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// Status returns the current scheduler status
func (s *Scheduler) Status() *SchedulerStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := &SchedulerStatus{
		Running:  s.running,
		Schedule: s.schedule,
		LastRun:  s.lastRun,
	}

	if s.running {
		status.NextRun = s.getNextRun()
	}

	return status
}

// TriggerNow triggers an immediate migration cycle
func (s *Scheduler) TriggerNow(ctx context.Context) error {
	s.logger.Info().Msg("Manual migration triggered")
	return s.manager.RunMigrationCycle(ctx)
}

// runMigration runs a migration cycle (called by cron)
func (s *Scheduler) runMigration() {
	s.mu.Lock()
	now := time.Now().UTC()
	s.lastRun = &now
	s.mu.Unlock()

	s.logger.Info().Msg("Scheduled migration cycle starting")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	if err := s.manager.RunMigrationCycle(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Scheduled migration cycle failed")
	}
}

// getNextRun returns the next scheduled run time
func (s *Scheduler) getNextRun() *time.Time {
	if s.cron == nil {
		return nil
	}

	entry := s.cron.Entry(s.entryID)
	if entry.ID == 0 {
		return nil
	}

	next := entry.Next
	return &next
}
