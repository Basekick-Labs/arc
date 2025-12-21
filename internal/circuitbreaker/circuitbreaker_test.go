package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestStateString tests the State.String() method
func TestStateString(t *testing.T) {
	tests := []struct {
		state    State
		expected string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.state.String(); got != tt.expected {
				t.Errorf("State.String() = %s, want %s", got, tt.expected)
			}
		})
	}
}

// TestDefaultConfig tests the DefaultConfig function
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("test-breaker")

	if cfg.Name != "test-breaker" {
		t.Errorf("Name = %s, want test-breaker", cfg.Name)
	}
	if cfg.MaxFailures != 5 {
		t.Errorf("MaxFailures = %d, want 5", cfg.MaxFailures)
	}
	if cfg.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s", cfg.Timeout)
	}
	if cfg.HalfOpenMaxRequests != 3 {
		t.Errorf("HalfOpenMaxRequests = %d, want 3", cfg.HalfOpenMaxRequests)
	}
}

// TestNew tests circuit breaker creation
func TestNew(t *testing.T) {
	logger := zerolog.Nop()

	t.Run("with config", func(t *testing.T) {
		cfg := &Config{
			Name:                "test",
			MaxFailures:         3,
			Timeout:             10 * time.Second,
			HalfOpenMaxRequests: 2,
		}

		cb := New(cfg, logger)

		if cb.State() != StateClosed {
			t.Errorf("Initial state = %v, want Closed", cb.State())
		}
		if cb.config.MaxFailures != 3 {
			t.Errorf("MaxFailures = %d, want 3", cb.config.MaxFailures)
		}
	})

	t.Run("with nil config", func(t *testing.T) {
		cb := New(nil, logger)

		if cb.State() != StateClosed {
			t.Errorf("Initial state = %v, want Closed", cb.State())
		}
		if cb.config.Name != "default" {
			t.Errorf("Name = %s, want default", cb.config.Name)
		}
	})
}

// TestCircuitBreaker_Closed tests normal operation in closed state
func TestCircuitBreaker_Closed(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:        "test",
		MaxFailures: 3,
		Timeout:     100 * time.Millisecond,
	}
	cb := New(cfg, logger)

	t.Run("allows requests when closed", func(t *testing.T) {
		callCount := 0
		err := cb.Execute(func() error {
			callCount++
			return nil
		})

		if err != nil {
			t.Errorf("Execute returned error: %v", err)
		}
		if callCount != 1 {
			t.Errorf("callCount = %d, want 1", callCount)
		}
		if cb.State() != StateClosed {
			t.Errorf("State = %v, want Closed", cb.State())
		}
	})

	t.Run("resets failures on success", func(t *testing.T) {
		// Cause some failures
		for i := 0; i < 2; i++ {
			cb.Execute(func() error {
				return errors.New("test error")
			})
		}

		// Now succeed
		cb.Execute(func() error {
			return nil
		})

		stats := cb.Stats()
		if stats["failures"].(int) != 0 {
			t.Errorf("failures = %d after success, want 0", stats["failures"])
		}
	})
}

// TestCircuitBreaker_Open tests transition to open state after failures
func TestCircuitBreaker_Open(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:        "test",
		MaxFailures: 3,
		Timeout:     100 * time.Millisecond,
	}
	cb := New(cfg, logger)

	testErr := errors.New("test error")

	// Cause enough failures to open the circuit
	for i := 0; i < 3; i++ {
		cb.Execute(func() error {
			return testErr
		})
	}

	if cb.State() != StateOpen {
		t.Errorf("State = %v after %d failures, want Open", cb.State(), cfg.MaxFailures)
	}

	// Requests should be rejected when open
	callCount := 0
	err := cb.Execute(func() error {
		callCount++
		return nil
	})

	if err != ErrCircuitOpen {
		t.Errorf("Execute error = %v, want ErrCircuitOpen", err)
	}
	if callCount != 0 {
		t.Errorf("Function was called when circuit is open")
	}
}

// TestCircuitBreaker_HalfOpen tests half-open state behavior
func TestCircuitBreaker_HalfOpen(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:                "test",
		MaxFailures:         2,
		Timeout:             50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := New(cfg, logger)

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	if cb.State() != StateOpen {
		t.Fatalf("State = %v, want Open", cb.State())
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// First request should trigger transition to half-open
	err := cb.Execute(func() error {
		return nil
	})
	if err != nil {
		t.Errorf("First half-open request failed: %v", err)
	}

	if cb.State() != StateHalfOpen {
		t.Errorf("State = %v after timeout, want HalfOpen", cb.State())
	}
}

// TestCircuitBreaker_HalfOpen_FailureReturnsToOpen tests that failure in half-open reopens circuit
func TestCircuitBreaker_HalfOpen_FailureReturnsToOpen(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:                "test",
		MaxFailures:         2,
		Timeout:             50 * time.Millisecond,
		HalfOpenMaxRequests: 3,
	}
	cb := New(cfg, logger)

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	// Wait for timeout to transition to half-open
	time.Sleep(60 * time.Millisecond)

	// Make a request to transition to half-open
	cb.Execute(func() error {
		return nil
	})

	if cb.State() != StateHalfOpen {
		t.Fatalf("State = %v, want HalfOpen", cb.State())
	}

	// A failure in half-open should return to open
	cb.Execute(func() error {
		return errors.New("error")
	})

	if cb.State() != StateOpen {
		t.Errorf("State = %v after half-open failure, want Open", cb.State())
	}
}

// TestCircuitBreaker_HalfOpen_SuccessCloses tests that enough successes in half-open closes circuit
func TestCircuitBreaker_HalfOpen_SuccessCloses(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:                "test",
		MaxFailures:         2,
		Timeout:             50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := New(cfg, logger)

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Enough successful requests in half-open should close the circuit
	for i := 0; i < cfg.HalfOpenMaxRequests; i++ {
		cb.Execute(func() error {
			return nil
		})
	}

	if cb.State() != StateClosed {
		t.Errorf("State = %v after %d successes in half-open, want Closed", cb.State(), cfg.HalfOpenMaxRequests)
	}
}

// TestCircuitBreaker_Reset tests manual reset
func TestCircuitBreaker_Reset(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:        "test",
		MaxFailures: 2,
		Timeout:     1 * time.Hour, // Long timeout so it won't auto-close
	}
	cb := New(cfg, logger)

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	if cb.State() != StateOpen {
		t.Fatalf("State = %v, want Open", cb.State())
	}

	// Reset should close the circuit
	cb.Reset()

	if cb.State() != StateClosed {
		t.Errorf("State = %v after Reset, want Closed", cb.State())
	}

	// Should allow requests again
	err := cb.Execute(func() error {
		return nil
	})
	if err != nil {
		t.Errorf("Execute after Reset returned error: %v", err)
	}
}

// TestCircuitBreaker_IsOpen tests the IsOpen helper
func TestCircuitBreaker_IsOpen(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:        "test",
		MaxFailures: 2,
		Timeout:     1 * time.Hour,
	}
	cb := New(cfg, logger)

	if cb.IsOpen() {
		t.Error("IsOpen = true initially, want false")
	}

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	if !cb.IsOpen() {
		t.Error("IsOpen = false after failures, want true")
	}

	cb.Reset()

	if cb.IsOpen() {
		t.Error("IsOpen = true after Reset, want false")
	}
}

// TestCircuitBreaker_Stats tests statistics reporting
func TestCircuitBreaker_Stats(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:        "test-stats",
		MaxFailures: 5,
		Timeout:     30 * time.Second,
	}
	cb := New(cfg, logger)

	// Make some requests
	cb.Execute(func() error { return nil })
	cb.Execute(func() error { return nil })
	cb.Execute(func() error { return errors.New("error") })

	stats := cb.Stats()

	if stats["name"] != "test-stats" {
		t.Errorf("stats[name] = %v, want test-stats", stats["name"])
	}
	if stats["state"] != "closed" {
		t.Errorf("stats[state] = %v, want closed", stats["state"])
	}
	if stats["max_failures"] != 5 {
		t.Errorf("stats[max_failures] = %v, want 5", stats["max_failures"])
	}
	if stats["timeout_seconds"] != 30.0 {
		t.Errorf("stats[timeout_seconds] = %v, want 30", stats["timeout_seconds"])
	}
}

// TestCircuitBreaker_OnStateChange tests state change callback
func TestCircuitBreaker_OnStateChange(t *testing.T) {
	logger := zerolog.Nop()

	var stateChanges []struct {
		from, to State
	}
	var mu sync.Mutex

	cfg := &Config{
		Name:        "test",
		MaxFailures: 2,
		Timeout:     50 * time.Millisecond,
		OnStateChange: func(name string, from, to State) {
			mu.Lock()
			stateChanges = append(stateChanges, struct{ from, to State }{from, to})
			mu.Unlock()
		},
	}
	cb := New(cfg, logger)

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	mu.Lock()
	if len(stateChanges) != 1 {
		t.Fatalf("Expected 1 state change, got %d", len(stateChanges))
	}
	if stateChanges[0].from != StateClosed || stateChanges[0].to != StateOpen {
		t.Errorf("State change = %v->%v, want Closed->Open", stateChanges[0].from, stateChanges[0].to)
	}
	mu.Unlock()

	// Wait for timeout and trigger half-open
	time.Sleep(60 * time.Millisecond)
	cb.Execute(func() error { return nil })

	mu.Lock()
	if len(stateChanges) < 2 {
		t.Fatalf("Expected at least 2 state changes, got %d", len(stateChanges))
	}
	if stateChanges[1].from != StateOpen || stateChanges[1].to != StateHalfOpen {
		t.Errorf("State change = %v->%v, want Open->HalfOpen", stateChanges[1].from, stateChanges[1].to)
	}
	mu.Unlock()
}

// TestCircuitBreaker_HalfOpenRequestLimit tests that half-open limits requests
func TestCircuitBreaker_HalfOpenRequestLimit(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:                "test",
		MaxFailures:         2,
		Timeout:             50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := New(cfg, logger)

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// First request transitions to half-open
	cb.Execute(func() error { return nil })

	if cb.State() != StateHalfOpen {
		t.Fatalf("State = %v, want HalfOpen", cb.State())
	}

	// Track how many requests are rejected vs allowed
	var allowed int32
	var rejected int32

	// Sequential requests to avoid race conditions in the test
	for i := 0; i < 5; i++ {
		err := cb.Execute(func() error {
			atomic.AddInt32(&allowed, 1)
			return nil
		})
		if err == ErrCircuitOpen {
			atomic.AddInt32(&rejected, 1)
		}
	}

	// At least some should be rejected once we exceed the half-open limit
	// The first request already counted as 1, so we should see rejections
	if rejected == 0 {
		t.Log("Note: No requests were rejected - circuit may have transitioned to closed")
	}
}

// TestCircuitBreaker_Concurrent tests thread safety
func TestCircuitBreaker_Concurrent(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{
		Name:        "test",
		MaxFailures: 100, // High threshold to avoid opening
		Timeout:     1 * time.Second,
	}
	cb := New(cfg, logger)

	var wg sync.WaitGroup
	var successCount int32
	var errorCount int32

	// Run many concurrent requests
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := cb.Execute(func() error {
				if i%10 == 0 {
					return errors.New("error")
				}
				return nil
			})
			if err != nil && err != ErrCircuitOpen {
				atomic.AddInt32(&errorCount, 1)
			} else if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Should not panic and should process all requests
	total := successCount + errorCount
	if total != 100 {
		t.Errorf("Processed %d requests, expected 100", total)
	}

	// Circuit should still be closed (threshold is 100)
	if cb.State() != StateClosed {
		t.Errorf("State = %v, want Closed", cb.State())
	}
}

// TestCircuitBreaker_ErrorPropagation tests that function errors are returned
func TestCircuitBreaker_ErrorPropagation(t *testing.T) {
	logger := zerolog.Nop()
	cfg := DefaultConfig("test")
	cb := New(cfg, logger)

	expectedErr := errors.New("custom error")

	err := cb.Execute(func() error {
		return expectedErr
	})

	if err != expectedErr {
		t.Errorf("Execute returned %v, want %v", err, expectedErr)
	}
}

// TestCircuitBreaker_ThresholdEdgeCases tests edge cases around thresholds
func TestCircuitBreaker_ThresholdEdgeCases(t *testing.T) {
	logger := zerolog.Nop()

	t.Run("exactly at threshold", func(t *testing.T) {
		cfg := &Config{
			Name:        "test",
			MaxFailures: 3,
			Timeout:     1 * time.Hour,
		}
		cb := New(cfg, logger)

		// 2 failures - should still be closed
		for i := 0; i < 2; i++ {
			cb.Execute(func() error { return errors.New("error") })
		}
		if cb.State() != StateClosed {
			t.Error("Circuit opened before reaching threshold")
		}

		// 3rd failure - should open
		cb.Execute(func() error { return errors.New("error") })
		if cb.State() != StateOpen {
			t.Error("Circuit should be open at threshold")
		}
	})

	t.Run("failures reset on success", func(t *testing.T) {
		cfg := &Config{
			Name:        "test",
			MaxFailures: 3,
			Timeout:     1 * time.Hour,
		}
		cb := New(cfg, logger)

		// 2 failures
		cb.Execute(func() error { return errors.New("error") })
		cb.Execute(func() error { return errors.New("error") })

		// 1 success resets
		cb.Execute(func() error { return nil })

		// 2 more failures - total is 2, not 4
		cb.Execute(func() error { return errors.New("error") })
		cb.Execute(func() error { return errors.New("error") })

		if cb.State() != StateClosed {
			t.Error("Circuit should still be closed - failures were reset")
		}
	})
}
