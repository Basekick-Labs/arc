package main

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/rs/zerolog"
)

// fakeStopper is a cycle that holds the claim until it is released or
// killed; a kill releases it after killDelay, as a dying subprocess does.
type fakeStopper struct {
	mu        sync.Mutex
	busy      bool
	released  chan struct{}
	killDelay time.Duration
	began     bool
	killed    bool
	log       *[]string
}

func newFakeStopper(busy bool, log *[]string) *fakeStopper {
	return &fakeStopper{busy: busy, released: make(chan struct{}), log: log}
}

func (f *fakeStopper) BeginShutdown() {
	f.mu.Lock()
	f.began = true
	f.mu.Unlock()
	*f.log = append(*f.log, "compaction-stop")
}

func (f *fakeStopper) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.busy {
		f.busy = false
		close(f.released)
	}
}

func (f *fakeStopper) WaitIdle(ctx context.Context) bool {
	f.mu.Lock()
	busy := f.busy
	f.mu.Unlock()
	if !busy {
		return true
	}
	select {
	case <-f.released:
		return true
	case <-ctx.Done():
		return false
	}
}

func (f *fakeStopper) KillCycles() {
	f.mu.Lock()
	f.killed = true
	f.mu.Unlock()
	time.AfterFunc(f.killDelay, f.release)
}

func (f *fakeStopper) RunningCycleID() int64 { return 7 }

// The stop comes first of all, before the readiness flag and the HTTP drain;
// the wait comes after the buffer's flush and the WAL, before the Iceberg
// drain and the cluster coordinator.
func TestCompactionShutdownOrder1887(t *testing.T) {
	var order []string
	c := shutdown.New(5*time.Second, zerolog.Nop())
	f := newFakeStopper(false, &order)
	c.RegisterHook("http-server", func(context.Context) error { order = append(order, "http-server"); return nil }, shutdown.PriorityHTTPServer)
	c.RegisterHook("ready-flag-off", func(context.Context) error { order = append(order, "ready-flag-off"); return nil }, 5)
	c.Register("arrow-buffer", shutdownFunc(func() error { order = append(order, "arrow-buffer"); return nil }), shutdown.PriorityBuffer)
	c.Register("wal", shutdownFunc(func() error { order = append(order, "wal"); return nil }), shutdown.PriorityWAL)
	c.Register("iceberg-export", shutdownFunc(func() error { order = append(order, "iceberg-export"); return nil }), priorityIcebergExport)
	c.Register("compaction-completion-watcher", shutdownFunc(func() error { order = append(order, "completion-watcher"); return nil }), priorityCompactionWatcher)
	registerClusterCoordinatorShutdown(c, func() error { order = append(order, "cluster-coordinator"); return nil })
	registerCompactionShutdown(c, &compactionShutdown{manager: shutdownLogged{f, &order}, timeout: 5 * time.Second, logger: zerolog.Nop()})
	if err := c.Shutdown(); err != nil {
		t.Fatal(err)
	}
	want := []string{"compaction-stop", "ready-flag-off", "http-server", "arrow-buffer", "wal", "compaction-wait", "completion-watcher", "iceberg-export", "cluster-coordinator"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v\nwant    %v", order, want)
	}
	if !(priorityCompactionWait < priorityCompactionWatcher && priorityCompactionWatcher < priorityIcebergExport && priorityIcebergExport < priorityIcebergGlue &&
		priorityIcebergGlue < priorityIcebergCatalogDB && priorityIcebergCatalogDB < shutdown.PriorityCompaction) {
		t.Fatal("the compaction wait and the Iceberg steps must run in that order, before the cluster coordinator")
	}
}

// shutdownLogged records the wait's start in the order.
type shutdownLogged struct {
	*fakeStopper
	order *[]string
}

func (s shutdownLogged) WaitIdle(ctx context.Context) bool {
	if len(*s.order) == 0 || (*s.order)[len(*s.order)-1] != "compaction-wait" {
		*s.order = append(*s.order, "compaction-wait")
	}
	return s.fakeStopper.WaitIdle(ctx)
}

// A cycle that outlives the wait's deadline is killed, and the next
// component runs only after its claim was released: no child outlives the
// steps that close storage under it.
func TestCompactionShutdownKillsAtTheDeadlineAndWaitsForRelease1887(t *testing.T) {
	var order []string
	timeout := time.Second
	c := shutdown.New(timeout, zerolog.Nop())
	f := newFakeStopper(true, &order)
	f.killDelay = 100 * time.Millisecond
	registerCompactionShutdown(c, &compactionShutdown{manager: f, timeout: timeout, logger: zerolog.Nop()})
	var releasedBeforeNext bool
	c.Register("storage", shutdownFunc(func() error {
		f.mu.Lock()
		releasedBeforeNext = !f.busy
		f.mu.Unlock()
		return nil
	}), shutdown.PriorityStorage)
	start := time.Now()
	_ = c.Shutdown()
	took := time.Since(start)
	if !f.killed {
		t.Fatal("the cycle was not killed at the deadline")
	}
	if !releasedBeforeNext {
		t.Fatal("storage closed while the killed cycle still held its claim")
	}
	// The kill comes a fifth of the budget before the deadline.
	if took < timeout-compactionWaitMargin(timeout)-50*time.Millisecond || took > timeout {
		t.Fatalf("shutdown took %v; the kill should come at %v", took, timeout-compactionWaitMargin(timeout))
	}
}

// A cycle that ends during the wait is not killed.
func TestCompactionShutdownWaitsForACycleThatEnds1887(t *testing.T) {
	var order []string
	c := shutdown.New(5*time.Second, zerolog.Nop())
	f := newFakeStopper(true, &order)
	registerCompactionShutdown(c, &compactionShutdown{manager: f, timeout: 5 * time.Second, logger: zerolog.Nop()})
	time.AfterFunc(200*time.Millisecond, f.release)
	_ = c.Shutdown()
	if f.killed {
		t.Fatal("a cycle that ended in time was killed")
	}
	if !f.began {
		t.Fatal("BeginShutdown was not called")
	}
}

// The coordinator skipped the wait (its deadline passed): the last step
// before exit kills the cycle and waits for its release.
func TestKillLeftoverCyclesAtExit1887(t *testing.T) {
	var order []string
	f := newFakeStopper(true, &order)
	f.killDelay = 50 * time.Millisecond
	killLeftoverCycles(f, zerolog.Nop())
	if !f.killed {
		t.Fatal("a cycle still running at exit was not killed")
	}
	if !f.WaitIdle(context.Background()) {
		t.Fatal("exit did not wait for the release")
	}
	idle := newFakeStopper(false, &order)
	killLeftoverCycles(idle, zerolog.Nop())
	if idle.killed {
		t.Fatal("an idle manager was killed")
	}
}

// The kill grace never runs past the shutdown deadline, and has a floor.
func TestGraceWithinTheDeadline1887(t *testing.T) {
	if g := graceWithin(context.Background()); g != compactionKillGrace {
		t.Fatalf("no deadline: %v, want %v", g, compactionKillGrace)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if g := graceWithin(ctx); g > time.Second || g < 900*time.Millisecond {
		t.Fatalf("1s left: %v", g)
	}
	past, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	if g := graceWithin(past); g != 100*time.Millisecond {
		t.Fatalf("deadline passed: %v, want the 100ms floor", g)
	}
}
