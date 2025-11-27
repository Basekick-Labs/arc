package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/basekick-labs/arc/internal/circuitbreaker"
	"github.com/rs/zerolog"
)

// ResilientBackend wraps a storage backend with circuit breaker and retry logic
type ResilientBackend struct {
	backend Backend
	cb      *circuitbreaker.CircuitBreaker
	logger  zerolog.Logger

	// Retry configuration
	maxRetries    int
	retryDelay    time.Duration
	retryMaxDelay time.Duration
}

// ResilientConfig holds configuration for the resilient backend
type ResilientConfig struct {
	// Circuit breaker settings
	MaxFailures         int
	Timeout             time.Duration
	HalfOpenMaxRequests int

	// Retry settings
	MaxRetries    int
	RetryDelay    time.Duration
	RetryMaxDelay time.Duration
}

// DefaultResilientConfig returns default resilient backend configuration
func DefaultResilientConfig() *ResilientConfig {
	return &ResilientConfig{
		MaxFailures:         5,
		Timeout:             30 * time.Second,
		HalfOpenMaxRequests: 3,
		MaxRetries:          3,
		RetryDelay:          100 * time.Millisecond,
		RetryMaxDelay:       5 * time.Second,
	}
}

// NewResilientBackend creates a new resilient storage backend
func NewResilientBackend(backend Backend, cfg *ResilientConfig, logger zerolog.Logger) *ResilientBackend {
	if cfg == nil {
		cfg = DefaultResilientConfig()
	}

	cbConfig := &circuitbreaker.Config{
		Name:                "storage",
		MaxFailures:         cfg.MaxFailures,
		Timeout:             cfg.Timeout,
		HalfOpenMaxRequests: cfg.HalfOpenMaxRequests,
	}

	return &ResilientBackend{
		backend:       backend,
		cb:            circuitbreaker.New(cbConfig, logger),
		logger:        logger.With().Str("component", "resilient-storage").Logger(),
		maxRetries:    cfg.MaxRetries,
		retryDelay:    cfg.RetryDelay,
		retryMaxDelay: cfg.RetryMaxDelay,
	}
}

// Write writes data to the storage backend with resilience
func (r *ResilientBackend) Write(ctx context.Context, path string, data []byte) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := r.cb.Execute(func() error {
			return r.backend.Write(ctx, path, data)
		})

		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry if circuit is open
		if err == circuitbreaker.ErrCircuitOpen {
			r.logger.Warn().
				Str("path", path).
				Msg("Storage write rejected - circuit breaker open")
			return err
		}

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Calculate retry delay with exponential backoff
		delay := r.retryDelay * time.Duration(1<<uint(attempt))
		if delay > r.retryMaxDelay {
			delay = r.retryMaxDelay
		}

		r.logger.Warn().
			Err(err).
			Str("path", path).
			Int("attempt", attempt+1).
			Int("max_retries", r.maxRetries).
			Dur("retry_delay", delay).
			Msg("Storage write failed, retrying")

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("storage write failed after %d retries: %w", r.maxRetries, lastErr)
}

// WriteReader writes data from a reader to the storage backend
func (r *ResilientBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := r.cb.Execute(func() error {
			return r.backend.WriteReader(ctx, path, reader, size)
		})

		if err == nil {
			return nil
		}

		lastErr = err

		if err == circuitbreaker.ErrCircuitOpen {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		delay := r.retryDelay * time.Duration(1<<uint(attempt))
		if delay > r.retryMaxDelay {
			delay = r.retryMaxDelay
		}

		r.logger.Warn().
			Err(err).
			Str("path", path).
			Int("attempt", attempt+1).
			Dur("retry_delay", delay).
			Msg("Storage write (reader) failed, retrying")

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("storage write failed after %d retries: %w", r.maxRetries, lastErr)
}

// Read reads data from the storage backend with resilience
func (r *ResilientBackend) Read(ctx context.Context, path string) ([]byte, error) {
	var lastErr error
	var data []byte

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := r.cb.Execute(func() error {
			var readErr error
			data, readErr = r.backend.Read(ctx, path)
			return readErr
		})

		if err == nil {
			return data, nil
		}

		lastErr = err

		if err == circuitbreaker.ErrCircuitOpen {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		delay := r.retryDelay * time.Duration(1<<uint(attempt))
		if delay > r.retryMaxDelay {
			delay = r.retryMaxDelay
		}

		r.logger.Warn().
			Err(err).
			Str("path", path).
			Int("attempt", attempt+1).
			Dur("retry_delay", delay).
			Msg("Storage read failed, retrying")

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("storage read failed after %d retries: %w", r.maxRetries, lastErr)
}

// ReadTo reads data to a writer from the storage backend
func (r *ResilientBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := r.cb.Execute(func() error {
			return r.backend.ReadTo(ctx, path, writer)
		})

		if err == nil {
			return nil
		}

		lastErr = err

		if err == circuitbreaker.ErrCircuitOpen {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		delay := r.retryDelay * time.Duration(1<<uint(attempt))
		if delay > r.retryMaxDelay {
			delay = r.retryMaxDelay
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("storage read failed after %d retries: %w", r.maxRetries, lastErr)
}

// List lists files in the storage backend
func (r *ResilientBackend) List(ctx context.Context, prefix string) ([]string, error) {
	var lastErr error
	var files []string

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := r.cb.Execute(func() error {
			var listErr error
			files, listErr = r.backend.List(ctx, prefix)
			return listErr
		})

		if err == nil {
			return files, nil
		}

		lastErr = err

		if err == circuitbreaker.ErrCircuitOpen {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		delay := r.retryDelay * time.Duration(1<<uint(attempt))
		if delay > r.retryMaxDelay {
			delay = r.retryMaxDelay
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("storage list failed after %d retries: %w", r.maxRetries, lastErr)
}

// Delete deletes a file from the storage backend
func (r *ResilientBackend) Delete(ctx context.Context, path string) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := r.cb.Execute(func() error {
			return r.backend.Delete(ctx, path)
		})

		if err == nil {
			return nil
		}

		lastErr = err

		if err == circuitbreaker.ErrCircuitOpen {
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		delay := r.retryDelay * time.Duration(1<<uint(attempt))
		if delay > r.retryMaxDelay {
			delay = r.retryMaxDelay
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("storage delete failed after %d retries: %w", r.maxRetries, lastErr)
}

// Exists checks if a file exists in the storage backend
func (r *ResilientBackend) Exists(ctx context.Context, path string) (bool, error) {
	var lastErr error
	var exists bool

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := r.cb.Execute(func() error {
			var existsErr error
			exists, existsErr = r.backend.Exists(ctx, path)
			return existsErr
		})

		if err == nil {
			return exists, nil
		}

		lastErr = err

		if err == circuitbreaker.ErrCircuitOpen {
			return false, err
		}

		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		delay := r.retryDelay * time.Duration(1<<uint(attempt))
		if delay > r.retryMaxDelay {
			delay = r.retryMaxDelay
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	return false, fmt.Errorf("storage exists check failed after %d retries: %w", r.maxRetries, lastErr)
}

// Close closes the underlying storage backend
func (r *ResilientBackend) Close() error {
	return r.backend.Close()
}

// CircuitBreakerStats returns circuit breaker statistics
func (r *ResilientBackend) CircuitBreakerStats() map[string]interface{} {
	return r.cb.Stats()
}

// IsCircuitOpen returns true if the circuit breaker is open
func (r *ResilientBackend) IsCircuitOpen() bool {
	return r.cb.IsOpen()
}

// ResetCircuitBreaker resets the circuit breaker
func (r *ResilientBackend) ResetCircuitBreaker() {
	r.cb.Reset()
}
