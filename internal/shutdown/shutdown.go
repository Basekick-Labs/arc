package shutdown

import (
	"context"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

// Shutdownable is an interface for components that can be shut down gracefully
type Shutdownable interface {
	Close() error
}

// ShutdownFunc is a function that performs cleanup during shutdown
type ShutdownFunc func(ctx context.Context) error

// Coordinator manages graceful shutdown of all components
type Coordinator struct {
	timeout time.Duration
	logger  zerolog.Logger

	mu         sync.Mutex
	components []namedComponent
	hooks      []namedHook

	// componentReserve is the part of timeout kept for the components
	// (SetComponentReserve). Zero: hooks and components share one deadline.
	componentReserve time.Duration

	// Shutdown state
	shutdownOnce sync.Once
	triggerOnce  sync.Once // Separate Once for TriggerShutdown to prevent race condition
	shutdownCh   chan struct{}
}

type namedComponent struct {
	name      string
	component Shutdownable
	priority  int // Lower = shutdown first
}

type namedHook struct {
	name     string
	hook     ShutdownFunc
	priority int
}

// New creates a new shutdown coordinator
func New(timeout time.Duration, logger zerolog.Logger) *Coordinator {
	return &Coordinator{
		timeout:    timeout,
		logger:     logger.With().Str("component", "shutdown").Logger(),
		shutdownCh: make(chan struct{}),
	}
}

// Register registers a component for graceful shutdown
// Priority determines shutdown order (lower = shutdown first)
func (c *Coordinator) Register(name string, component Shutdownable, priority int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.components = append(c.components, namedComponent{
		name:      name,
		component: component,
		priority:  priority,
	})

	c.logger.Debug().
		Str("name", name).
		Int("priority", priority).
		Msg("Registered component for shutdown")
}

// RegisterHook registers a shutdown hook function
func (c *Coordinator) RegisterHook(name string, hook ShutdownFunc, priority int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.hooks = append(c.hooks, namedHook{
		name:     name,
		hook:     hook,
		priority: priority,
	})

	c.logger.Debug().
		Str("name", name).
		Int("priority", priority).
		Msg("Registered shutdown hook")
}

// SetComponentReserve keeps reserve of the budget for the components: the
// hooks run on a context whose deadline is the shutdown deadline minus
// reserve, and a hook that overruns it no longer costs the components their
// turn (#1644). A hook that starts after that deadline still runs, on an
// expired context, so a hook that stops a scheduler still stops it before the
// components it uses close; one that waits on its context returns at once.
// The components keep the full deadline and are skipped only when it passes.
// A reserve of zero, or one not below the timeout, keeps the single deadline.
func (c *Coordinator) SetComponentReserve(reserve time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reserve <= 0 || reserve >= c.timeout {
		c.componentReserve = 0
		return
	}
	c.componentReserve = reserve
}

// WaitForSignal blocks until a shutdown signal is received
func (c *Coordinator) WaitForSignal() os.Signal {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case sig := <-quit:
		c.logger.Info().
			Str("signal", sig.String()).
			Msg("Received shutdown signal")
		return sig
	case <-c.shutdownCh:
		return syscall.SIGTERM
	}
}

// Shutdown performs graceful shutdown of all registered components
func (c *Coordinator) Shutdown() error {
	var shutdownErr error

	c.shutdownOnce.Do(func() {
		// Use triggerOnce to safely close the channel (may have been closed by TriggerShutdown)
		c.triggerOnce.Do(func() {
			close(c.shutdownCh)
		})

		c.logger.Info().
			Dur("timeout", c.timeout).
			Int("components", len(c.components)).
			Int("hooks", len(c.hooks)).
			Msg("Starting graceful shutdown")

		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()

		start := time.Now()

		// Sort components by priority (lowest first)
		c.mu.Lock()
		reserve := c.componentReserve
		components := make([]namedComponent, len(c.components))
		copy(components, c.components)
		hooks := make([]namedHook, len(c.hooks))
		copy(hooks, c.hooks)
		c.mu.Unlock()

		// Sort by priority
		sortComponentsByPriority(components)
		sortHooksByPriority(hooks)

		// The hooks' deadline: the shutdown deadline, less the components'
		// reserve when one is set.
		hookCtx := ctx
		if reserve > 0 {
			var hookCancel context.CancelFunc
			hookCtx, hookCancel = context.WithTimeout(ctx, c.timeout-reserve)
			defer hookCancel()
		}
		hookWindowLogged := false

		// Execute hooks first
		for _, h := range hooks {
			select {
			case <-ctx.Done():
				c.logger.Warn().
					Str("hook", h.name).
					Msg("Shutdown timeout reached, skipping remaining hooks")
				shutdownErr = ctx.Err()
				return
			case <-hookCtx.Done():
				if !hookWindowLogged {
					hookWindowLogged = true
					c.logger.Warn().
						Str("hook", h.name).
						Dur("components_reserve", reserve).
						Msg("Shutdown hooks overran their share of the budget; the remaining hooks run on an expired context")
				}
			default:
			}

			c.logger.Debug().
				Str("hook", h.name).
				Int("priority", h.priority).
				Msg("Executing shutdown hook")

			if err := h.hook(hookCtx); err != nil {
				c.logger.Error().
					Err(err).
					Str("hook", h.name).
					Msg("Shutdown hook failed")
				if shutdownErr == nil {
					shutdownErr = err
				}
			}
		}

		// Shutdown components
		for _, comp := range components {
			select {
			case <-ctx.Done():
				c.logger.Warn().
					Str("component", comp.name).
					Msg("Shutdown timeout reached, skipping remaining components")
				shutdownErr = ctx.Err()
				return
			default:
			}

			c.logger.Debug().
				Str("component", comp.name).
				Int("priority", comp.priority).
				Msg("Shutting down component")

			if err := comp.component.Close(); err != nil {
				c.logger.Error().
					Err(err).
					Str("component", comp.name).
					Msg("Component shutdown failed")
				if shutdownErr == nil {
					shutdownErr = err
				}
			} else {
				c.logger.Debug().
					Str("component", comp.name).
					Msg("Component shutdown complete")
			}
		}

		duration := time.Since(start)
		c.logger.Info().
			Dur("duration", duration).
			Msg("Graceful shutdown complete")
	})

	return shutdownErr
}

// TriggerShutdown triggers a shutdown programmatically
// This is safe to call from multiple goroutines concurrently
func (c *Coordinator) TriggerShutdown() {
	c.triggerOnce.Do(func() {
		c.logger.Info().Msg("Programmatic shutdown triggered")
		close(c.shutdownCh)
	})
}

// Simple bubble sort for small slices
// sortComponentsByPriority is sortHooksByPriority for components, and had the
// same defect.
func sortComponentsByPriority(components []namedComponent) {
	sort.SliceStable(components, func(i, j int) bool {
		return components[i].priority < components[j].priority
	})
}

// sortHooksByPriority orders hooks by ascending priority, and keeps hooks of
// EQUAL priority in the order they were registered. The previous selection
// sort reordered equal keys, so the relative order of the ten hooks that share
// PriorityCompaction depended on how many lower-priority hooks happened to be
// registered — which varies with configuration, so the same binary shut down
// in a different order in OSS and in a cluster (#854).
func sortHooksByPriority(hooks []namedHook) {
	sort.SliceStable(hooks, func(i, j int) bool {
		return hooks[i].priority < hooks[j].priority
	})
}

// Priorities for common components (use these as guidelines)
const (
	PriorityHTTPServer = 10 // Stop accepting new requests first
	PriorityIngest     = 20 // Stop ingestion
	PriorityBuffer     = 30 // Flush buffers
	PriorityWAL        = 40 // Flush WAL
	// PriorityScheduler stops the tick-driven schedulers that ask the cluster
	// coordinator whether they may run — compaction, continuous queries,
	// retention, reconciliation. They quiesce BEFORE the coordinator they
	// depend on, so a tick in flight never finds it gone (#854).
	PriorityScheduler  = 45 // Stop cluster-gated schedulers
	PriorityCompaction = 50 // Stop compaction and the cluster coordinator
	PriorityTelemetry  = 60 // Send final telemetry
	PriorityAuth       = 70 // Auth manager
	PriorityStorage    = 80 // Storage backends
	PriorityDatabase   = 90 // Database connections last
)
