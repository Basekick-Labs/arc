package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/rs/zerolog"
)

// CQScheduler manages automatic execution of continuous queries based on their intervals
type CQScheduler struct {
	cqHandler     *api.ContinuousQueryHandler
	licenseClient *license.Client
	jobs          map[int64]*cqJob // CQ ID → job info
	mu            sync.RWMutex
	wg            sync.WaitGroup
	stopCh        chan struct{}
	running       bool
	logger        zerolog.Logger
}

// cqJob represents a scheduled CQ job
type cqJob struct {
	cqID     int64
	cqName   string
	interval time.Duration
	ticker   *time.Ticker
	stopCh   chan struct{}
}

// CQSchedulerConfig holds configuration for the CQ scheduler
type CQSchedulerConfig struct {
	CQHandler     *api.ContinuousQueryHandler
	LicenseClient *license.Client
	Logger        zerolog.Logger
}

// NewCQScheduler creates a new CQ scheduler
func NewCQScheduler(cfg *CQSchedulerConfig) (*CQScheduler, error) {
	s := &CQScheduler{
		cqHandler:     cfg.CQHandler,
		licenseClient: cfg.LicenseClient,
		jobs:          make(map[int64]*cqJob),
		stopCh:        make(chan struct{}),
		logger:        cfg.Logger.With().Str("component", "cq-scheduler").Logger(),
	}

	s.logger.Info().Msg("CQ scheduler initialized")
	return s, nil
}

// Start starts the CQ scheduler by loading active CQs and starting their jobs
func (s *CQScheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		s.logger.Warn().Msg("CQ scheduler already running")
		return nil
	}

	// Check license - require valid license for CQ scheduler
	if s.licenseClient == nil || !s.licenseClient.CanUseCQScheduler() {
		s.logger.Warn().Msg("Valid enterprise license required for CQ scheduler - not starting")
		return nil
	}

	// Load and start all active CQs
	cqs, err := s.cqHandler.GetActiveCQs()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to load active CQs")
		return err
	}

	for _, cq := range cqs {
		if err := s.startJob(cq.ID, cq.Name, cq.Interval); err != nil {
			s.logger.Warn().
				Err(err).
				Int64("cq_id", cq.ID).
				Str("cq_name", cq.Name).
				Msg("Failed to start CQ job")
			continue
		}
	}

	s.running = true
	s.logger.Info().
		Int("active_cqs", len(s.jobs)).
		Msg("CQ scheduler started")

	return nil
}

// Stop stops all CQ jobs and the scheduler
func (s *CQScheduler) Stop() {
	s.mu.Lock()

	if !s.running {
		s.mu.Unlock()
		return
	}

	// Stop all jobs
	for id, job := range s.jobs {
		close(job.stopCh)
		job.ticker.Stop()
		s.logger.Debug().
			Int64("cq_id", id).
			Str("cq_name", job.cqName).
			Msg("Stopped CQ job")
	}
	s.jobs = make(map[int64]*cqJob)
	s.running = false
	s.mu.Unlock()

	// Wait for all goroutines to finish outside the lock to avoid deadlock
	s.wg.Wait()

	s.logger.Info().Msg("CQ scheduler stopped")
}

// ReloadCQ reloads a specific CQ (call after CQ update)
func (s *CQScheduler) ReloadCQ(cqID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop existing job if any
	if job, exists := s.jobs[cqID]; exists {
		close(job.stopCh)
		job.ticker.Stop()
		delete(s.jobs, cqID)
		s.logger.Debug().Int64("cq_id", cqID).Msg("Stopped existing CQ job for reload")
	}

	// Get updated CQ
	cq, err := s.cqHandler.GetCQ(cqID)
	if err != nil {
		return err
	}

	// Only restart if active
	if !cq.IsActive {
		s.logger.Debug().Int64("cq_id", cqID).Msg("CQ is not active, not restarting job")
		return nil
	}

	return s.startJob(cq.ID, cq.Name, cq.Interval)
}

// ReloadAll reloads all CQ schedules from the database
func (s *CQScheduler) ReloadAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop all existing jobs
	for id, job := range s.jobs {
		close(job.stopCh)
		job.ticker.Stop()
		s.logger.Debug().Int64("cq_id", id).Msg("Stopped CQ job for full reload")
	}
	s.jobs = make(map[int64]*cqJob)

	// Check license - require valid license for CQ scheduler
	if s.licenseClient == nil || !s.licenseClient.CanUseCQScheduler() {
		s.logger.Warn().Msg("Valid enterprise license required for CQ scheduler")
		return nil
	}

	// Load and start all active CQs
	cqs, err := s.cqHandler.GetActiveCQs()
	if err != nil {
		return err
	}

	for _, cq := range cqs {
		if err := s.startJob(cq.ID, cq.Name, cq.Interval); err != nil {
			s.logger.Warn().
				Err(err).
				Int64("cq_id", cq.ID).
				Str("cq_name", cq.Name).
				Msg("Failed to start CQ job")
			continue
		}
	}

	s.logger.Info().
		Int("active_cqs", len(s.jobs)).
		Msg("CQ scheduler reloaded")

	return nil
}

// startJob starts a job for a specific CQ (must be called with lock held)
func (s *CQScheduler) startJob(cqID int64, cqName, intervalStr string) error {
	// Parse interval using Go duration format
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return err
	}

	// Minimum interval of 10 seconds to prevent abuse
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}

	job := &cqJob{
		cqID:     cqID,
		cqName:   cqName,
		interval: interval,
		ticker:   time.NewTicker(interval),
		stopCh:   make(chan struct{}),
	}

	s.jobs[cqID] = job

	// Start the job goroutine
	s.wg.Add(1)
	go s.runJob(job)

	s.logger.Info().
		Int64("cq_id", cqID).
		Str("cq_name", cqName).
		Dur("interval", interval).
		Msg("Started CQ job")

	return nil
}

// runJob runs a single CQ job on its interval
func (s *CQScheduler) runJob(job *cqJob) {
	defer s.wg.Done()
	for {
		select {
		case <-job.ticker.C:
			// Check license before each execution
			if s.licenseClient == nil || !s.licenseClient.CanUseCQScheduler() {
				s.logger.Warn().
					Int64("cq_id", job.cqID).
					Str("cq_name", job.cqName).
					Msg("Valid enterprise license required, skipping CQ execution")
				continue
			}

			s.executeJob(job)
		case <-job.stopCh:
			return
		}
	}
}

// executeJob executes a single CQ
func (s *CQScheduler) executeJob(job *cqJob) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s.logger.Debug().
		Int64("cq_id", job.cqID).
		Str("cq_name", job.cqName).
		Msg("Executing scheduled CQ")

	resp, err := s.cqHandler.ExecuteCQ(ctx, job.cqID)
	if err != nil {
		s.logger.Error().
			Err(err).
			Int64("cq_id", job.cqID).
			Str("cq_name", job.cqName).
			Msg("Scheduled CQ execution failed")
		return
	}

	s.logger.Info().
		Int64("cq_id", job.cqID).
		Str("cq_name", job.cqName).
		Int64("records_written", resp.RecordsWritten).
		Float64("duration_seconds", resp.ExecutionTimeSeconds).
		Msg("Scheduled CQ execution completed")
}

// Status returns the current status of the scheduler
func (s *CQScheduler) Status() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]map[string]interface{}, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, map[string]interface{}{
			"cq_id":    job.cqID,
			"cq_name":  job.cqName,
			"interval": job.interval.String(),
		})
	}

	licenseValid := false
	if s.licenseClient != nil {
		licenseValid = s.licenseClient.CanUseCQScheduler()
	}

	return map[string]interface{}{
		"running":       s.running,
		"license_valid": licenseValid,
		"job_count":     len(s.jobs),
		"jobs":          jobs,
	}
}

// IsRunning returns whether the scheduler is running
func (s *CQScheduler) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// JobCount returns the number of active jobs
func (s *CQScheduler) JobCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.jobs)
}
