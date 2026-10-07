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
	RecordEdgeSyncSuccess(time.Time)
	IncEdgeSyncFailure()
}

// SchedulerConfig configures automatic network sync for one spoke agent.
type SchedulerConfig struct {
	Agent         SyncRunner
	SyncInterval  time.Duration
	RetryInterval time.Duration
	Enabled       bool
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
	return &Scheduler{
		agent:         cfg.Agent,
		syncInterval:  cfg.SyncInterval,
		retryInterval: cfg.RetryInterval,
		enabled:       cfg.Enabled,
		clusterGate:   cfg.ClusterGate,
		metrics:       cfg.Metrics,
		logger:        cfg.Logger.With().Str("component", "edgesync-scheduler").Logger(),
	}, nil
}

// Start launches the scheduler. Its first pass runs after SyncInterval, which
// lets the rest of Arc finish startup and install the real compaction gate.
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
	var canceledByDemotion bool
	for {
		select {
		case <-ctx.Done():
			if runDone != nil {
				<-runDone
			}
			return

		case <-timer.C:
			if s.clusterGate != nil && !s.clusterGate.IsPrimaryWriter() {
				if runDone != nil && runCancel != nil {
					runCancel()
					canceledByDemotion = true
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
			if canceledByDemotion {
				canceledByDemotion = false
				s.logger.Info().Msg("Scheduled edge sync pass ended after primary-writer demotion")
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
			if outcome.err != nil {
				if s.metrics != nil {
					s.metrics.IncEdgeSyncFailure()
				}
				s.logger.Warn().Err(outcome.err).Msg("Scheduled edge sync pass failed; retrying after the configured backoff")
				resetSchedulerTimer(timer, s.retryInterval)
				continue
			}

			completedAt := time.Now().UTC()
			if s.metrics != nil {
				s.metrics.RecordEdgeSyncSuccess(completedAt)
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

func resetSchedulerTimer(timer *time.Timer, after time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(after)
}
