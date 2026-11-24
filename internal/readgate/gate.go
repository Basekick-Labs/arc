// Package readgate counts the reads of data files in flight, so that a job
// that swaps one set of files for another can wait until no read planned
// before its switch is still on its way to the files (#1825).
//
// A query's file exclusions are fixed when its SQL is built, and the files it
// reads are fixed when the statement is bound, some time later: the request
// may wait for a pooled connection in between, for as long as its own
// deadline allows, and a continuous query for longer. A job that changes what
// reads must exclude, and then removes a file, has to know that every
// statement built under the old exclusions has been bound. Time cannot tell
// it: a governance policy can give a token a longer deadline than the global
// one.
//
// A read enters before its SQL is built and leaves no earlier than its
// statement is bound. In practice it leaves once the statement has executed:
// the DuckDB driver runs a query to completion inside the call that starts
// it, and the callers leave when their handler returns. So a wait lasts as
// long as the longest read in flight, which is bounded by that read's own
// deadline (the query timeout, a governance policy's, ten minutes for a
// continuous query). A job that waits needs its own deadline and a way to
// give up.
//
// The job changes its state, then calls Flip and waits for every read that
// entered before the flip to leave.
//
// A read also binds the cancel function of the context its statement runs
// under (Bind), and CancelAll cancels every read bound so far and closes the
// gate: until Reopen, a read that binds is cancelled at once. A cluster node
// that loses its read lease stops its reads, those that had not bound yet
// included, so a job waiting for such a node knows within a fixed time that
// it no longer reads (#1825).
//
// What is not counted, and a job must handle otherwise: the Iceberg export's
// listing, DELETE's and retention's own reads of files, compaction, reads on
// other nodes of a cluster (each node has its own gate), and files the arcx
// router opens while it streams or shadows a query after the handler returned.
package readgate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Gate counts reads in flight, by the epoch they entered in. The zero value
// is ready to use. A nil *Gate counts nothing and never waits.
type Gate struct {
	mu     sync.Mutex
	epoch  uint64
	active map[*readRec]struct{}
	// moved is closed, and made anew by the next waiter, whenever a read
	// leaves.
	moved chan struct{}
	// cancelAll is cancelled by CancelAll, which then makes a new one; every
	// bound read's cancel function runs when it is (Bind).
	cancelAll    context.Context
	cancelAllNow context.CancelFunc
	// closed: CancelAll was called and Reopen was not since.
	closed bool
	// cancelled: CancelAll or Reopen was called; cancelledThrough is the last
	// epoch it ended. A read that entered in it or earlier is cancelled at
	// Bind, even after Reopen: its exclusions may be from before what the
	// node missed.
	cancelled        bool
	cancelledThrough uint64
}

// readRec is one read in flight.
type readRec struct {
	epoch uint64
	// doneAt: when the context its statement runs under ended (Bind), in
	// Unix nanoseconds; 0 while it has not.
	doneAt atomic.Int64
}

// HungAfter: a read whose context ended this long ago and that has still not
// left is hung (the engine did not stop it); Wait no longer waits for it. A
// read's context ends at its own deadline, so a read that runs as long as it
// may is always waited for. A variable for tests.
var HungAfter = 30 * time.Second

// Entry is a read's place in the gate, for Bind.
type Entry struct {
	Epoch uint64
	rec   *readRec
}

// Bind ties a read's cancel function to CancelAll: it runs when CancelAll is
// called before the read's own cancel. ctx is the context the read's
// statement runs under: once it ends, the read may be counted as hung
// (HungAfter). The returned function replaces cancel: it unbinds, then
// cancels. A nil cancel is allowed (nothing to cancel, nothing bound).
func (g *Gate) Bind(e Entry, ctx context.Context, cancel context.CancelFunc) context.CancelFunc {
	if g == nil || cancel == nil {
		return cancel
	}
	g.mu.Lock()
	if g.closed || (g.cancelled && e.Epoch <= g.cancelledThrough) {
		g.mu.Unlock()
		cancel()
		return cancel
	}
	if g.cancelAll == nil {
		g.cancelAll, g.cancelAllNow = context.WithCancel(context.Background())
	}
	all := g.cancelAll
	g.mu.Unlock()
	stop := context.AfterFunc(all, cancel)
	stopDone := func() bool { return true }
	if e.rec != nil && ctx != nil {
		rec := e.rec
		stopDone = context.AfterFunc(ctx, func() { rec.doneAt.Store(time.Now().UnixNano()) })
	}
	return func() {
		stop()
		stopDone()
		cancel()
	}
}

// CancelAll cancels every read bound so far, and every read that binds until
// Reopen.
func (g *Gate) CancelAll() {
	if g == nil {
		return
	}
	g.mu.Lock()
	now := g.cancelAllNow
	g.cancelAll, g.cancelAllNow = nil, nil
	g.closed = true
	g.cancelled, g.cancelledThrough = true, g.epoch
	g.epoch++
	g.mu.Unlock()
	if now != nil {
		now()
	}
}

// Reopen lets reads that enter from now on run. A read that entered while
// the gate was closed stays cancelled at its Bind: it passed the node's
// readiness check just before the node lost its lease.
func (g *Gate) Reopen() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.closed {
		g.cancelled, g.cancelledThrough = true, g.epoch
		g.epoch++
	}
	g.closed = false
	g.mu.Unlock()
}

// Enter records a read about to build its SQL and returns the function that
// records it as bound. The returned function is safe to call more than once
// and from any goroutine.
func (g *Gate) Enter() (leave func()) {
	_, leave = g.EnterAt()
	return leave
}

// EnterAt is Enter, with the read's entry, for Bind.
func (g *Gate) EnterAt() (entry Entry, leave func()) {
	if g == nil {
		return Entry{}, func() {}
	}
	g.mu.Lock()
	rec := &readRec{epoch: g.epoch}
	if g.active == nil {
		g.active = map[*readRec]struct{}{}
	}
	g.active[rec] = struct{}{}
	g.mu.Unlock()
	var once sync.Once
	return Entry{Epoch: rec.epoch, rec: rec}, func() {
		once.Do(func() {
			g.mu.Lock()
			delete(g.active, rec)
			// A waiter looks again.
			if g.moved != nil {
				close(g.moved)
				g.moved = nil
			}
			g.mu.Unlock()
		})
	}
}

// Epoch is the epoch a read entering now would enter in.
func (g *Gate) Epoch() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.epoch
}

// Flip starts a new epoch and returns the one that ended: reads that enter
// from now on are not waited for by Wait(ended).
func (g *Gate) Flip() (ended uint64) {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	ended = g.epoch
	g.epoch++
	return ended
}

// Wait returns once no read that entered in epoch ended or earlier is still
// in flight, but for hung ones (HungAfter), or with ctx's error. ended must be
// a value Flip returned: reads keep entering the current epoch, so waiting on
// it may never end.
func (g *Gate) Wait(ctx context.Context, ended uint64) error {
	if g == nil {
		return nil
	}
	for {
		g.mu.Lock()
		busy := false
		now := time.Now().UnixNano()
		for r := range g.active {
			if r.epoch > ended {
				continue
			}
			if d := r.doneAt.Load(); d != 0 && time.Duration(now-d) >= HungAfter {
				continue
			}
			busy = true
			break
		}
		if !busy {
			g.mu.Unlock()
			return nil
		}
		if g.moved == nil {
			g.moved = make(chan struct{})
		}
		moved := g.moved
		g.mu.Unlock()
		// A read whose context ended becomes hung without leaving: look
		// again now and then.
		select {
		case <-moved:
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Hung is how many reads in flight are hung: their context ended HungAfter
// ago and they have not left.
func (g *Gate) Hung() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	now := time.Now().UnixNano()
	for r := range g.active {
		if d := r.doneAt.Load(); d != 0 && time.Duration(now-d) >= HungAfter {
			n++
		}
	}
	return n
}

// InFlight is how many reads have entered and not left.
func (g *Gate) InFlight() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.active)
}
