package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// State represents the circuit breaker state
type State int

const (
	StateClosed   State = iota // Normal operation
	StateOpen                  // Failing, rejecting requests
	StateHalfOpen              // Testing if service recovered
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

var (
	// ErrCircuitOpen is returned when the circuit breaker is open
	ErrCircuitOpen = errors.New("circuit breaker is open")

	// ErrTooManyRequests is returned when too many requests in half-open state
	ErrTooManyRequests = errors.New("too many requests in half-open state")
)

// Config holds circuit breaker configuration
type Config struct {
	// Name for logging
	Name string

	// MaxFailures is the maximum number of failures before opening the circuit
	MaxFailures int

	// Timeout is how long to wait before attempting to close the circuit
	Timeout time.Duration

	// HalfOpenMaxRequests is the max requests allowed in half-open state
	HalfOpenMaxRequests int

	// OnStateChange is called when state changes
	OnStateChange func(name string, from, to State)
}

// DefaultConfig returns default circuit breaker configuration
func DefaultConfig(name string) *Config {
	return &Config{
		Name:                name,
		MaxFailures:         5,
		Timeout:             30 * time.Second,
		HalfOpenMaxRequests: 3,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config *Config
	logger zerolog.Logger

	mu              sync.RWMutex
	state           State
	failures        int
	successes       int
	lastFailureTime time.Time
	halfOpenCount   int32 // atomic counter for half-open requests
}

// New creates a new circuit breaker
func New(cfg *Config, logger zerolog.Logger) *CircuitBreaker {
	if cfg == nil {
		cfg = DefaultConfig("default")
	}

	return &CircuitBreaker{
		config: cfg,
		logger: logger.With().Str("component", "circuit-breaker").Str("name", cfg.Name).Logger(),
		state:  StateClosed,
	}
}

// Execute runs the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.allowRequest() {
		cb.logger.Warn().
			Str("state", cb.State().String()).
			Msg("Request rejected by circuit breaker")
		return ErrCircuitOpen
	}

	// Execute the function
	err := fn()

	// Record the result
	cb.recordResult(err)

	return err
}

// allowRequest checks if a request should be allowed
func (cb *CircuitBreaker) allowRequest() bool {
	cb.mu.RLock()
	state := cb.state
	lastFailure := cb.lastFailureTime
	cb.mu.RUnlock()

	switch state {
	case StateClosed:
		return true

	case StateOpen:
		// Check if timeout has passed
		if time.Since(lastFailure) > cb.config.Timeout {
			// Transition to half-open
			cb.mu.Lock()
			if cb.state == StateOpen { // Double-check after acquiring write lock
				cb.setState(StateHalfOpen)
				atomic.StoreInt32(&cb.halfOpenCount, 0)
			}
			cb.mu.Unlock()
			return true
		}
		return false

	case StateHalfOpen:
		// Allow limited requests in half-open state
		count := atomic.AddInt32(&cb.halfOpenCount, 1)
		return count <= int32(cb.config.HalfOpenMaxRequests)

	default:
		return true
	}
}

// recordResult records the result of an operation
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.recordFailure()
	} else {
		cb.recordSuccess()
	}
}

func (cb *CircuitBreaker) recordFailure() {
	cb.failures++
	cb.successes = 0
	cb.lastFailureTime = time.Now()

	cb.logger.Debug().
		Int("failures", cb.failures).
		Int("max_failures", cb.config.MaxFailures).
		Str("state", cb.state.String()).
		Msg("Recorded failure")

	switch cb.state {
	case StateClosed:
		if cb.failures >= cb.config.MaxFailures {
			cb.setState(StateOpen)
		}

	case StateHalfOpen:
		// Any failure in half-open returns to open
		cb.setState(StateOpen)
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.successes++

	cb.logger.Debug().
		Int("successes", cb.successes).
		Str("state", cb.state.String()).
		Msg("Recorded success")

	switch cb.state {
	case StateClosed:
		// Reset failures on success
		cb.failures = 0

	case StateHalfOpen:
		// After enough successes in half-open, close the circuit
		if cb.successes >= cb.config.HalfOpenMaxRequests {
			cb.setState(StateClosed)
		}
	}
}

func (cb *CircuitBreaker) setState(newState State) {
	if cb.state == newState {
		return
	}

	oldState := cb.state
	cb.state = newState
	cb.failures = 0
	cb.successes = 0

	cb.logger.Info().
		Str("from", oldState.String()).
		Str("to", newState.String()).
		Msg("Circuit breaker state changed")

	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(cb.config.Name, oldState, newState)
	}
}

// State returns the current state
func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Stats returns circuit breaker statistics
func (cb *CircuitBreaker) Stats() map[string]interface{} {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return map[string]interface{}{
		"name":              cb.config.Name,
		"state":             cb.state.String(),
		"failures":          cb.failures,
		"successes":         cb.successes,
		"max_failures":      cb.config.MaxFailures,
		"timeout_seconds":   cb.config.Timeout.Seconds(),
		"last_failure_time": cb.lastFailureTime,
	}
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateClosed)
	cb.failures = 0
	cb.successes = 0

	cb.logger.Info().Msg("Circuit breaker reset")
}

// IsOpen returns true if the circuit is open
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state == StateOpen
}
