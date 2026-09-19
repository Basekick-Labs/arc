package edgesync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

// SpokeSchedulerConfig configures automatic network synchronization.
// A nil CanRun gate permits standalone operation.
type SpokeSchedulerConfig struct {
	Agent         *Agent
	Interval      time.Duration
	RetryInterval time.Duration
	CanRun        func() bool
	Logger        zerolog.Logger
	OnSuccess     func(time.Time)
	OnFailure     func()
}

// SpokeScheduler runs network sync passes without overlapping manual passes.
// Run is owned by one goroutine; its backoff state must not be accessed
// concurrently.
type SpokeScheduler struct {
	agent         *Agent
	interval      time.Duration
	retryInterval time.Duration
	canRun        func() bool
	logger        zerolog.Logger
	OnSuccess     func(time.Time)
	OnFailure     func()
	run           func(context.Context) (*RunResult, error)
	failures      int
}

// NewSpokeScheduler validates the scheduling intervals.
func NewSpokeScheduler(cfg SpokeSchedulerConfig) (*SpokeScheduler, error) {
	if cfg.Agent == nil {
		return nil, errors.New("edgesync: scheduler requires an agent")
	}
	if cfg.Interval < time.Second {
		return nil, fmt.Errorf(
			"edgesync: sync interval must be at least 1s, got %s",
			cfg.Interval,
		)
	}
	if cfg.RetryInterval < time.Second ||
		cfg.RetryInterval >= cfg.Interval {
		return nil, fmt.Errorf(
			"edgesync: retry interval must be at least 1s and shorter than sync interval",
		)
	}

	return &SpokeScheduler{
		agent:         cfg.Agent,
		interval:      cfg.Interval,
		retryInterval: cfg.RetryInterval,
		canRun:        cfg.CanRun,
		logger:        cfg.Logger.With().Str("component", "edgesync-scheduler").Logger(),
		run:           cfg.Agent.Run,
		OnSuccess:     cfg.OnSuccess,
		OnFailure:     cfg.OnFailure,
	}, nil
}

// Run blocks until cancellation. The caller owns the goroutine and must
// cancel and join it before closing the agent's ledger or storage backend.
func (s *SpokeScheduler) Run(ctx context.Context) {
	timer := time.NewTimer(s.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		delay := s.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		timer.Reset(delay)
	}
}

// runOnce checks eligibility on every attempt, not only at startup.
// The Agent's own guard coordinates with manual HTTP requests.
func (s *SpokeScheduler) runOnce(ctx context.Context) time.Duration {
	if ctx.Err() != nil {
		return s.interval
	}
	if s.canRun != nil && !s.canRun() {
		s.logger.Debug().Msg("Scheduled sync skipped: node is not the primary writer")
		return s.interval
	}

	result, err := s.run(ctx)
	if errors.Is(err, ErrSyncAlreadyRunning) {
		s.logger.Debug().Msg("Scheduled sync skipped: another pass is running")
		return s.interval
	}

	// Shutdown cancellation is not a failed contact attempt.
	if ctx.Err() != nil {
		return s.interval
	}

	if err == nil && result == nil {
		err = errors.New("sync pass returned a nil result")
	}
	if err == nil && (result.Failed > 0 ||
		result.Partial > 0 || len(result.Conflicts) > 0) {
		err = fmt.Errorf(
			"sync pass incomplete: failed=%d partial=%d conflicts=%d",
			result.Failed,
			result.Partial,
			len(result.Conflicts),
		)
	}

	if err != nil {
		if s.OnFailure != nil {
			s.OnFailure()
		}
		if s.failures < 64 {
			s.failures++
		}

		delay := s.retryInterval
		for attempt := 1; attempt < s.failures && delay < s.interval; attempt++ {
			if delay > s.interval/2 {
				delay = s.interval
				break
			}
			delay *= 2
		}
		if delay > s.interval {
			delay = s.interval
		}

		s.logger.Warn().Err(err).
			Int("consecutive_failures", s.failures).
			Dur("next_attempt_in", delay).
			Msg("Scheduled sync did not complete successfully")
		return delay
	}

	s.failures = 0
	// A successful empty-backlog pass does not establish hub health.
	if result.HubContacted && s.OnSuccess != nil {
		s.OnSuccess(time.Now())
	}
	s.logger.Info().
		Int("sent", result.Sent).
		Int("already_present", result.AlreadyPresent).
		Dur("next_attempt_in", s.interval).
		Msg("Scheduled sync pass completed")
	return s.interval
}
