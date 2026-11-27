package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// cqLeaseCoordinator is the primary writer, with or without a read lease.
type cqLeaseCoordinator struct{ ready bool }

func (c *cqLeaseCoordinator) IsPrimaryWriter() bool { return true }
func (c *cqLeaseCoordinator) Role() string          { return "writer" }
func (c *cqLeaseCoordinator) ReadReady() (bool, string) {
	return c.ready, "this node has not yet synced"
}

// #1825, #1852: a continuous query run reads, and writes what it read back
// for good. On a node that may not serve reads it is refused before it
// reads.
func TestCQRunRefusedWithoutAReadLease1825(t *testing.T) {
	h := &ContinuousQueryHandler{logger: zerolog.Nop(), coordinator: &cqLeaseCoordinator{}}
	ran := false
	h.aggregateHook = func(context.Context, *ContinuousQuery, string, time.Time) (int64, error) {
		ran = true
		return 0, nil
	}
	end := time.Now()
	_, err := h.aggregate(context.Background(), &ContinuousQuery{Name: "cq1"}, "SELECT 1", end.Add(-time.Minute), end)
	if !errors.Is(err, ErrNoReadLease) || ran {
		t.Fatalf("err = %v, ran %v; want the run refused before it reads", err, ran)
	}
}

// A refused run is skipped, not failed: no execution row, the window kept,
// and a manual run answers 503, retryable.
func TestCQRefusedRunIsSkipped1825(t *testing.T) {
	cqClockAt(t)
	r := newStandaloneCQRig(t)
	if code, body := r.do(t, "POST", "/cq", cqBody); code != fiber.StatusCreated {
		t.Fatalf("create: %d %v", code, body)
	}
	lease := &cqLeaseCoordinator{}
	r.h.coordinator = lease

	if _, err := r.h.ExecuteCQ(context.Background(), 1); !errors.Is(err, ErrNoReadLease) {
		t.Fatalf("scheduled run: err = %v, want it skipped for want of a read lease", err)
	}
	code, raw := r.rawExecute(t, "1", `{}`)
	if code != fiber.StatusServiceUnavailable {
		t.Fatalf("manual run: %d %s, want 503", code, raw)
	}
	if n := r.executionRows(t, 1); n != 0 || len(r.runs) != 0 || r.localWatermark(t, 1) != nil {
		t.Fatalf("a refused run left %d execution row(s), ran %d time(s), or moved the window", n, len(r.runs))
	}

	lease.ready = true
	if code, raw := r.rawExecute(t, "1", `{}`); code != fiber.StatusOK {
		t.Fatalf("with the lease: %d %s", code, raw)
	}
	if len(r.runs) != 1 {
		t.Fatalf("ran %d time(s) with the lease", len(r.runs))
	}
}
