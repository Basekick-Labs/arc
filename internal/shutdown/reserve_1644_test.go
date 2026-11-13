package shutdown

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1644: a hook that holds its context until the deadline (an HTTP drain
// behind a long query) leaves the components their reserve; the hooks after
// it still run, on an expired context; and the shutdown reports no error.
func TestComponentReserveSurvivesAHookThatUsesItsWholeShare(t *testing.T) {
	c := New(600*time.Millisecond, zerolog.Nop())
	c.SetComponentReserve(400 * time.Millisecond)

	var hookDeadline time.Time
	c.RegisterHook("drain", func(ctx context.Context) error {
		hookDeadline, _ = ctx.Deadline()
		<-ctx.Done()
		return nil
	}, PriorityHTTPServer)
	var laterRan bool
	var laterCtxErr error
	c.RegisterHook("scheduler-stop", func(ctx context.Context) error {
		laterRan = true
		laterCtxErr = ctx.Err()
		return nil
	}, PriorityHTTPServer+5)
	buffer := &mockShutdownable{closeDelay: 150 * time.Millisecond}
	wal := &mockShutdownable{}
	c.Register("buffer", buffer, PriorityBuffer)
	c.Register("wal", wal, PriorityWAL)

	start := time.Now()
	if err := c.Shutdown(); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if got := hookDeadline.Sub(start); got > 250*time.Millisecond {
		t.Fatalf("hook deadline %v after start, want about 200ms (timeout - reserve)", got)
	}
	if !laterRan || laterCtxErr == nil {
		t.Fatalf("later hook ran=%v ctxErr=%v, want it run on an expired context", laterRan, laterCtxErr)
	}
	if !buffer.closeCalled || !wal.closeCalled {
		t.Fatalf("components skipped: buffer=%v wal=%v", buffer.closeCalled, wal.closeCalled)
	}
}

// Without a reserve the old single deadline holds: a hook that uses it all
// costs every component its turn.
func TestNoComponentReserveKeepsTheSingleDeadline(t *testing.T) {
	c := New(200*time.Millisecond, zerolog.Nop())
	c.RegisterHook("drain", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}, PriorityHTTPServer)
	comp := &mockShutdownable{}
	c.Register("buffer", comp, PriorityBuffer)
	if err := c.Shutdown(); err == nil {
		t.Fatal("want the timeout error")
	}
	if comp.closeCalled {
		t.Fatal("component ran after the single deadline passed")
	}
}

func TestSetComponentReserveIgnoresOutOfRangeValues(t *testing.T) {
	c := New(time.Second, zerolog.Nop())
	for _, r := range []time.Duration{-time.Second, 0, time.Second, 2 * time.Second} {
		c.SetComponentReserve(r)
		if c.componentReserve != 0 {
			t.Fatalf("reserve %v kept as %v", r, c.componentReserve)
		}
	}
	c.SetComponentReserve(300 * time.Millisecond)
	if c.componentReserve != 300*time.Millisecond {
		t.Fatalf("reserve %v", c.componentReserve)
	}
}

// Once the full deadline has passed, the remaining hooks are skipped every
// time, not only when a select over both deadlines happens to pick it.
func TestHooksPastTheFullDeadlineAreAlwaysSkipped(t *testing.T) {
	for i := 0; i < 20; i++ {
		c := New(100*time.Millisecond, zerolog.Nop())
		c.SetComponentReserve(50 * time.Millisecond)
		c.RegisterHook("overrun", func(ctx context.Context) error {
			time.Sleep(150 * time.Millisecond)
			return nil
		}, PriorityHTTPServer)
		var later bool
		c.RegisterHook("later", func(context.Context) error {
			later = true
			return nil
		}, PriorityHTTPServer+1)
		if err := c.Shutdown(); err == nil {
			t.Fatal("want the timeout error")
		}
		if later {
			t.Fatalf("run %d: a hook ran after the full deadline", i)
		}
	}
}
