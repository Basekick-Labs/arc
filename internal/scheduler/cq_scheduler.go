package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/rs/zerolog"
)

// CQClusterGate is an alias for WriterGate. Both schedulers share the same
// interface; this alias lets existing code compile without changes.
type CQClusterGate = WriterGate

// CQScheduler manages automatic execution of continuous queries based on their intervals
type CQScheduler struct {
	cqHandler     *api.ContinuousQueryHandler
	licenseClient *license.Client
	clusterGate   WriterGate
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
	done     chan struct{} // closed by runJob when the goroutine exits
}

// CQSchedulerConfig holds configuration for the CQ scheduler
type CQSchedulerConfig struct {
	CQHandler     *api.ContinuousQueryHandler
	LicenseClient *license.Client
	ClusterGate   WriterGate // nil = standalone, no gate
	Logger        zerolog.Logger
}

// NewCQScheduler creates a new CQ scheduler
func NewCQScheduler(cfg *CQSchedulerConfig) (*CQScheduler, error) {
	s := &CQScheduler{
		cqHandler:     cfg.CQHandler,
		licenseClient: cfg.LicenseClient,
		clusterGate:   cfg.ClusterGate,
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
	for id := range s.jobs {
		s.stopJobLocked(id)
	}
	s.running = false
	s.mu.Unlock()

	// Wait for all goroutines to finish outside the lock to avoid deadlock
	s.wg.Wait()

	s.logger.Info().Msg("CQ scheduler stopped")
}

// stopJobLocked signals the job goroutine to stop and removes it from the map.
// It returns the job's done channel so callers that need to wait for the
// goroutine to fully exit (e.g. before starting a replacement) can do so
// outside the lock. Returns nil when no job was running for cqID.
// Caller must hold s.mu.
func (s *CQScheduler) stopJobLocked(cqID int64) chan struct{} {
	if job, exists := s.jobs[cqID]; exists {
		close(job.stopCh)
		job.ticker.Stop()
		delete(s.jobs, cqID)
		s.logger.Debug().Int64("cq_id", cqID).Str("cq_name", job.cqName).Msg("Stopped CQ job")
		return job.done
	}
	return nil
}

// ReloadCQ reloads a specific CQ (call after CQ update)
func (s *CQScheduler) ReloadCQ(cqID int64) error {
	s.mu.Lock()

	if !s.running {
		s.mu.Unlock()
		return nil
	}

	done := s.stopJobLocked(cqID)

	// Get updated CQ while still holding the lock
	cq, err := s.cqHandler.GetCQ(cqID)
	s.mu.Unlock()

	if err != nil {
		return err
	}

	// Wait for the old goroutine to exit before starting a replacement so that
	// a long-running executeJob cannot overlap with the new job.
	if done != nil {
		<-done
	}

	if !cq.IsActive {
		s.logger.Debug().Int64("cq_id", cqID).Msg("CQ is not active, not restarting job")
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startJob(cq.ID, cq.Name, cq.Interval)
}

// StartJobDirect schedules a job using caller-supplied data, avoiding a
// redundant SQLite read. Used by handleCreate immediately after INSERT so the
// scheduler never races against an uncommitted or not-yet-visible row.
func (s *CQScheduler) StartJobDirect(cqID int64, name, interval string, isActive bool) error {
	s.mu.Lock()

	if !s.running {
		s.mu.Unlock()
		return nil
	}

	done := s.stopJobLocked(cqID)
	s.mu.Unlock()

	// Wait for any in-flight execution to finish before starting the new job.
	if done != nil {
		<-done
	}

	if !isActive {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startJob(cqID, name, interval)
}

// ReloadAll reloads all CQ schedules from the database
func (s *CQScheduler) ReloadAll() error {
	s.mu.Lock()

	if !s.running {
		s.mu.Unlock()
		return nil
	}

	// Collect done channels before releasing the lock so we can wait for
	// in-flight executions to finish without holding the lock.
	var doneChans []chan struct{}
	for id := range s.jobs {
		if ch := s.stopJobLocked(id); ch != nil {
			doneChans = append(doneChans, ch)
		}
	}
	s.mu.Unlock()

	for _, ch := range doneChans {
		<-ch
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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
		done:     make(chan struct{}),
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
	defer close(job.done)
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

			// Cluster gate: checked on every tick so role transitions (failover,
			// demotion) take effect without a restart. clusterGate is immutable
			// after construction so no lock is needed here.
			if s.clusterGate != nil && !s.clusterGate.IsPrimaryWriter() {
				s.logger.Debug().
					Str("role", s.clusterGate.Role()).
					Int64("cq_id", job.cqID).
					Str("cq_name", job.cqName).
					Msg("CQ tick skipped: node is not primary writer")
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
	// Use a context that cancels on both timeout and stop signal
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Cancel context if stop signal is received during execution
	go func() {
		select {
		case <-job.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

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
