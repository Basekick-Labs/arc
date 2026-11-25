package shutdown

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type contextCloser struct {
	deadline  time.Time
	hadCtx    bool
	closeUsed bool
}

func (c *contextCloser) Close() error { c.closeUsed = true; return nil }

func (c *contextCloser) CloseContext(ctx context.Context) error {
	c.deadline, c.hadCtx = ctx.Deadline()
	return nil
}

// #1886: a ContextCloser gets the whole shutdown's deadline through
// CloseContext, and Close is not called; a plain component still gets Close.
func TestContextCloserGetsTheShutdownDeadline(t *testing.T) {
	c := New(2*time.Second, zerolog.Nop())
	cc := &contextCloser{}
	plain := &mockShutdownable{}
	c.Register("iceberg", cc, PriorityWAL+1)
	c.Register("wal", plain, PriorityWAL)

	start := time.Now()
	if err := c.Shutdown(); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if cc.closeUsed {
		t.Fatal("Close was called on a ContextCloser; want CloseContext only")
	}
	if !cc.hadCtx {
		t.Fatal("CloseContext got a context without a deadline")
	}
	if got := cc.deadline.Sub(start); got < 1900*time.Millisecond || got > 2100*time.Millisecond {
		t.Fatalf("deadline %v after start, want the 2s shutdown timeout", got)
	}
	if !plain.closeCalled {
		t.Fatal("plain component not closed")
	}
}
