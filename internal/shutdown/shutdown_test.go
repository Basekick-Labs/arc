package shutdown

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// mockShutdownable is a test implementation of Shutdownable
type mockShutdownable struct {
	closeCalled bool
	closeErr    error
	closeDelay  time.Duration
}

func (m *mockShutdownable) Close() error {
	if m.closeDelay > 0 {
		time.Sleep(m.closeDelay)
	}
	m.closeCalled = true
	return m.closeErr
}

func newTestCoordinator() *Coordinator {
	logger := zerolog.Nop()
	return New(5*time.Second, logger)
}

func TestNew(t *testing.T) {
	logger := zerolog.Nop()
	c := New(10*time.Second, logger)

	if c == nil {
		t.Fatal("expected non-nil coordinator")
	}
	if c.timeout != 10*time.Second {
		t.Errorf("expected timeout 10s, got %v", c.timeout)
	}
	if c.shutdownCh == nil {
		t.Error("expected shutdownCh to be initialized")
	}
}

func TestRegister(t *testing.T) {
	c := newTestCoordinator()
	comp := &mockShutdownable{}

	c.Register("test-component", comp, PriorityHTTPServer)

	if len(c.components) != 1 {
		t.Errorf("expected 1 component, got %d", len(c.components))
	}
	if c.components[0].name != "test-component" {
		t.Errorf("expected name 'test-component', got '%s'", c.components[0].name)
	}
	if c.components[0].priority != PriorityHTTPServer {
		t.Errorf("expected priority %d, got %d", PriorityHTTPServer, c.components[0].priority)
	}
}

func TestRegisterHook(t *testing.T) {
	c := newTestCoordinator()

	c.RegisterHook("test-hook", func(ctx context.Context) error {
		return nil
	}, PriorityBuffer)

	if len(c.hooks) != 1 {
		t.Errorf("expected 1 hook, got %d", len(c.hooks))
	}
	if c.hooks[0].name != "test-hook" {
		t.Errorf("expected name 'test-hook', got '%s'", c.hooks[0].name)
	}
}

func TestShutdown(t *testing.T) {
	c := newTestCoordinator()
	comp := &mockShutdownable{}
	hookCalled := false

	c.Register("test-component", comp, PriorityHTTPServer)
	c.RegisterHook("test-hook", func(ctx context.Context) error {
		hookCalled = true
		return nil
	}, PriorityBuffer)

	err := c.Shutdown()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !comp.closeCalled {
		t.Error("expected component Close() to be called")
	}
	if !hookCalled {
		t.Error("expected hook to be called")
	}
}

func TestShutdownOnce(t *testing.T) {
	c := newTestCoordinator()
	callCount := 0
	comp := &mockShutdownable{}

	c.Register("test-component", comp, PriorityHTTPServer)
	c.RegisterHook("test-hook", func(ctx context.Context) error {
		callCount++
		return nil
	}, PriorityBuffer)

	// Call Shutdown multiple times
	c.Shutdown()
	c.Shutdown()
	c.Shutdown()

	// Hook should only be called once
	if callCount != 1 {
		t.Errorf("expected hook to be called once, got %d times", callCount)
	}
}

func TestShutdownPriority(t *testing.T) {
	c := newTestCoordinator()
	order := []string{}
	var mu sync.Mutex

	// Register components in non-priority order
	comp1 := &mockShutdownable{}
	comp2 := &mockShutdownable{}
	comp3 := &mockShutdownable{}

	c.Register("database", comp1, PriorityDatabase) // 90 - last
	c.Register("http", comp2, PriorityHTTPServer)   // 10 - first
	c.Register("buffer", comp3, PriorityBuffer)     // 30 - middle

	// Register hooks to track order
	c.RegisterHook("hook-db", func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "hook-db")
		mu.Unlock()
		return nil
	}, PriorityDatabase)
	c.RegisterHook("hook-http", func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "hook-http")
		mu.Unlock()
		return nil
	}, PriorityHTTPServer)

	c.Shutdown()

	// Verify hooks are called in priority order (lower first)
	if len(order) < 2 {
		t.Fatalf("expected at least 2 hooks called, got %d", len(order))
	}
	if order[0] != "hook-http" {
		t.Errorf("expected 'hook-http' first, got '%s'", order[0])
	}
	if order[1] != "hook-db" {
		t.Errorf("expected 'hook-db' second, got '%s'", order[1])
	}
}

func TestShutdownWithError(t *testing.T) {
	c := newTestCoordinator()
	expectedErr := errors.New("component error")
	comp := &mockShutdownable{closeErr: expectedErr}

	c.Register("failing-component", comp, PriorityHTTPServer)

	err := c.Shutdown()
	if err == nil {
		t.Error("expected error from shutdown")
	}
	if err != expectedErr {
		t.Errorf("expected error '%v', got '%v'", expectedErr, err)
	}
}

func TestTriggerShutdown(t *testing.T) {
	c := newTestCoordinator()

	// Channel should not be closed initially
	select {
	case <-c.shutdownCh:
		t.Fatal("shutdownCh should not be closed initially")
	default:
		// expected
	}

	c.TriggerShutdown()

	// Channel should now be closed
	select {
	case <-c.shutdownCh:
		// expected
	default:
		t.Fatal("shutdownCh should be closed after TriggerShutdown")
	}
}

func TestTriggerShutdownConcurrent(t *testing.T) {
	// This test verifies that TriggerShutdown is safe to call concurrently
	// Previously this could panic due to double-close of channel
	c := newTestCoordinator()

	var wg sync.WaitGroup
	numGoroutines := 100
	panicCount := atomic.Int32{}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCount.Add(1)
				}
			}()
			c.TriggerShutdown()
		}()
	}

	wg.Wait()

	if panicCount.Load() > 0 {
		t.Errorf("TriggerShutdown panicked %d times", panicCount.Load())
	}

	// Verify channel is closed
	select {
	case <-c.shutdownCh:
		// expected
	default:
		t.Fatal("shutdownCh should be closed")
	}
}

func TestTriggerShutdownThenShutdown(t *testing.T) {
	// Test that TriggerShutdown followed by Shutdown works correctly
	c := newTestCoordinator()
	comp := &mockShutdownable{}

	c.Register("test-component", comp, PriorityHTTPServer)

	// Trigger shutdown first
	c.TriggerShutdown()

	// Then call Shutdown - should not panic
	err := c.Shutdown()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !comp.closeCalled {
		t.Error("expected component Close() to be called")
	}
}

func TestShutdownTimeout(t *testing.T) {
	logger := zerolog.Nop()
	c := New(100*time.Millisecond, logger) // Short timeout

	slowComp := &mockShutdownable{closeDelay: 500 * time.Millisecond}
	c.Register("slow-component", slowComp, PriorityHTTPServer)

	// Add a second component that should be skipped due to timeout
	secondComp := &mockShutdownable{}
	c.Register("second-component", secondComp, PriorityDatabase)

	err := c.Shutdown()

	// Should get context deadline exceeded error
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestWaitForSignalWithTrigger(t *testing.T) {
	c := newTestCoordinator()

	done := make(chan struct{})
	go func() {
		c.WaitForSignal()
		close(done)
	}()

	// Give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Trigger shutdown
	c.TriggerShutdown()

	// Should return within reasonable time
	select {
	case <-done:
		// expected
	case <-time.After(time.Second):
		t.Fatal("WaitForSignal did not return after TriggerShutdown")
	}
}

func TestSortComponentsByPriority(t *testing.T) {
	components := []namedComponent{
		{name: "c", priority: 30},
		{name: "a", priority: 10},
		{name: "b", priority: 20},
	}

	sortComponentsByPriority(components)

	if components[0].name != "a" || components[0].priority != 10 {
		t.Errorf("expected first component to be 'a' with priority 10, got '%s' with priority %d",
			components[0].name, components[0].priority)
	}
	if components[1].name != "b" || components[1].priority != 20 {
		t.Errorf("expected second component to be 'b' with priority 20, got '%s' with priority %d",
			components[1].name, components[1].priority)
	}
	if components[2].name != "c" || components[2].priority != 30 {
		t.Errorf("expected third component to be 'c' with priority 30, got '%s' with priority %d",
			components[2].name, components[2].priority)
	}
}

func TestSortHooksByPriority(t *testing.T) {
	hooks := []namedHook{
		{name: "c", priority: 30},
		{name: "a", priority: 10},
		{name: "b", priority: 20},
	}

	sortHooksByPriority(hooks)

	if hooks[0].name != "a" || hooks[0].priority != 10 {
		t.Errorf("expected first hook to be 'a' with priority 10")
	}
	if hooks[1].name != "b" || hooks[1].priority != 20 {
		t.Errorf("expected second hook to be 'b' with priority 20")
	}
	if hooks[2].name != "c" || hooks[2].priority != 30 {
		t.Errorf("expected third hook to be 'c' with priority 30")
	}
}
