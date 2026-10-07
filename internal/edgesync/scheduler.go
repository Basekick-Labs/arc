package edgesync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// SyncRunner is the part of Agent used by the automatic spoke scheduler.
type SyncRunner interface {
	Run(context.Context) (*RunResult, error)
}

// WriterGate keeps scheduled sync on the current primary writer in a cluster.
// A nil gate allows a standalone spoke.
type WriterGate interface {
	IsPrimaryWriter() bool
	Role() string
}

// SchedulerMetrics receives the two operator-facing outcomes required to
// monitor scheduled sync: last success time and failed pass count.
type SchedulerMetrics interface {
	RecordEdgeSyncSpokeSuccess(time.Time)
	IncEdgeSyncSpokePassFailures()
}

// SchedulerLicense is checked at startup and before every scheduled attempt.
// Manual Agent.Run deliberately has no license requirement.
type SchedulerLicense interface {
	CanUseEdgeSyncScheduler() bool
}

// SchedulerConfig configures automatic network sync for one spoke agent.
type SchedulerConfig struct {
	Agent         SyncRunner
	SyncInterval  time.Duration
	RetryInterval time.Duration
	Enabled       bool
	LicenseClient SchedulerLicense
	ClusterGate   WriterGate
	Metrics       SchedulerMetrics
	Logger        zerolog.Logger
}

// Scheduler runs a spoke pass periodically. The next timer is reset after a
// pass finishes, so time spent syncing cannot create a backlog of missed ticks.
type Scheduler struct {
	agent         SyncRunner
	syncInterval  time.Duration
	retryInterval time.Duration
	enabled       bool
	licenseClient SchedulerLicense
	clusterGate   WriterGate
	metrics       SchedulerMetrics
	logger        zerolog.Logger

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewScheduler validates configuration and returns a ready scheduler.
func NewScheduler(cfg SchedulerConfig) (*Scheduler, error) {
	if cfg.Enabled && cfg.Agent == nil {
		return nil, errors.New("edgesync: enabled scheduler requires a sync agent")
	}
	if cfg.SyncInterval <= 0 {
		return nil, fmt.Errorf("edgesync: sync interval must be greater than 0 (got %s)", cfg.SyncInterval)
	}
	if cfg.RetryInterval <= 0 {
		return nil, fmt.Errorf("edgesync: retry interval must be greater than 0 (got %s)", cfg.RetryInterval)
	}
	if cfg.RetryInterval > cfg.SyncInterval {
		return nil, errors.New("edgesync: retry interval must not exceed sync interval")
	}
	return &Scheduler{
		agent:         cfg.Agent,
		syncInterval:  cfg.SyncInterval,
		retryInterval: cfg.RetryInterval,
		enabled:       cfg.Enabled,
		licenseClient: cfg.LicenseClient,
		clusterGate:   cfg.ClusterGate,
		metrics:       cfg.Metrics,
		logger:        cfg.Logger.With().Str("component", "edgesync-scheduler").Logger(),
	}, nil
}

// Start launches the scheduler after the caller installs the compaction gate.
// Its first pass runs after SyncInterval.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		s.logger.Debug().Msg("Edge sync scheduler disabled")
		return nil
	}
	if s.running {
		s.logger.Warn().Msg("Edge sync scheduler already running")
		return nil
	}
	if s.licenseClient == nil || !s.licenseClient.CanUseEdgeSyncScheduler() {
		s.logger.Info().Msg("Valid paid license required for automatic edge sync; manual sync remains available")
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.running = true
	done := s.done
	go func() {
		defer close(done)
		s.runLoop(ctx)
	}()
	s.logger.Info().Dur("sync_interval", s.syncInterval).Dur("retry_interval", s.retryInterval).
		Msg("Edge sync scheduler started")
	return nil
}

// Stop cancels an in-flight scheduled pass and waits for the loop to exit.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	cancel, done := s.cancel, s.done
	s.mu.Unlock()

	cancel()
	<-done

	s.mu.Lock()
	if s.done == done {
		s.running = false
		s.cancel = nil
		s.done = nil
	}
	s.mu.Unlock()
}

// IsRunning reports whether the scheduler loop is active.
func (s *Scheduler) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

type scheduledRun struct {
	result *RunResult
	err    error
}

func (s *Scheduler) runLoop(ctx context.Context) {
	timer := time.NewTimer(s.syncInterval)
	defer timer.Stop()

	var runDone <-chan scheduledRun
	var runCancel context.CancelFunc
	var canceledByGate bool
	failures := 0
	for {
		select {
		case <-ctx.Done():
			if runDone != nil {
				<-runDone
			}
			return

		case <-timer.C:
			if s.licenseClient == nil || !s.licenseClient.CanUseEdgeSyncScheduler() {
				if runDone != nil && runCancel != nil {
					runCancel()
					canceledByGate = true
				}
				s.logger.Debug().Msg("Scheduled edge sync skipped: valid paid license required")
				resetSchedulerTimer(timer, s.syncInterval)
				continue
			}
			if s.clusterGate != nil && !s.clusterGate.IsPrimaryWriter() {
				if runDone != nil && runCancel != nil {
					runCancel()
					canceledByGate = true
					s.logger.Info().Str("role", s.clusterGate.Role()).
						Msg("Scheduled edge sync pass canceled after node lost primary-writer role")
				} else {
					s.logger.Debug().Str("role", s.clusterGate.Role()).
						Msg("Scheduled edge sync tick skipped: node is not the primary writer")
				}
				resetSchedulerTimer(timer, s.syncInterval)
				continue
			}
			if runDone != nil {
				s.logger.Info().Msg("Scheduled edge sync tick skipped: a pass is still in progress")
				resetSchedulerTimer(timer, s.syncInterval)
				continue
			}

			finished := make(chan scheduledRun, 1)
			runDone = finished
			runCtx, cancel := context.WithCancel(ctx)
			runCancel = cancel
			go func() {
				result, err := s.agent.Run(runCtx)
				finished <- scheduledRun{result: result, err: err}
			}()
			resetSchedulerTimer(timer, s.syncInterval)

		case outcome := <-runDone:
			if ctx.Err() != nil {
				return
			}
			runDone = nil
			if runCancel != nil {
				runCancel()
				runCancel = nil
			}
			if canceledByGate {
				canceledByGate = false
				s.logger.Info().Msg("Scheduled edge sync pass ended after losing scheduling eligibility")
				resetSchedulerTimer(timer, s.syncInterval)
				continue
			}
			if errors.Is(outcome.err, ErrAgentRunInProgress) {
				// A manual request owns the same Agent guard. This is a skipped
				// tick, not a hub failure and must not increment failure metrics.
				s.logger.Info().Msg("Scheduled edge sync tick skipped: a manual pass is in progress")
				resetSchedulerTimer(timer, s.syncInterval)
				continue
			}
			passErr := outcome.err
			if passErr == nil && outcome.result == nil {
				passErr = errors.New("sync pass returned a nil result")
			}
			if passErr == nil && (outcome.result.Failed > 0 || outcome.result.Partial > 0 || len(outcome.result.Conflicts) > 0) {
				// Agent.Run reports per-file transfer errors in RunResult while
				// keeping the pass-level error nil so other files can finish.
				passErr = fmt.Errorf("sync pass incomplete: failed=%d partial=%d conflicts=%d", outcome.result.Failed, outcome.result.Partial, len(outcome.result.Conflicts))
			}
			if passErr != nil {
				if s.metrics != nil {
					s.metrics.IncEdgeSyncSpokePassFailures()
				}
				if failures < 64 {
					failures++
				}
				delay := s.retryDelay(failures)
				s.logger.Warn().Err(passErr).Int("consecutive_failures", failures).Dur("next_attempt_in", delay).Msg("Scheduled edge sync pass failed; retrying with backoff")
				resetSchedulerTimer(timer, delay)
				continue
			}

			failures = 0
			completedAt := time.Now().UTC()
			// An empty backlog makes no network request and cannot establish hub health.
			if s.metrics != nil && outcome.result.HubContacted {
				s.metrics.RecordEdgeSyncSpokeSuccess(completedAt)
			}
			event := s.logger.Info().Time("completed_at", completedAt)
			if outcome.result != nil {
				event.Int("sent", outcome.result.Sent).
					Int("failed", outcome.result.Failed).
					Int("partial", outcome.result.Partial).
					Int("conflicts", len(outcome.result.Conflicts)).
					Dur("duration", outcome.result.Duration)
			}
			event.Msg("Scheduled edge sync pass completed")
			resetSchedulerTimer(timer, s.syncInterval)
		}
	}
}

// retryDelay grows exponentially from the retry interval, capped at the normal
// interval. Compare before doubling so even very large durations cannot overflow.
func (s *Scheduler) retryDelay(failures int) time.Duration {
	delay := s.retryInterval
	for attempt := 1; attempt < failures && delay < s.syncInterval; attempt++ {
		if delay > s.syncInterval/2 {
			return s.syncInterval
		}
		delay *= 2
	}
	if delay > s.syncInterval {
		return s.syncInterval
	}
	return delay
}

func resetSchedulerTimer(timer *time.Timer, after time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(after)
}
