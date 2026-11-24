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
)

// Gate counts reads in flight, by the epoch they entered in. The zero value
// is ready to use. A nil *Gate counts nothing and never waits.
type Gate struct {
	mu     sync.Mutex
	epoch  uint64
	active map[uint64]int
	// moved is closed, and made anew by the next waiter, whenever the last
	// read of an epoch leaves.
	moved chan struct{}
	// cancelAll is cancelled by CancelAll, which then makes a new one; every
	// bound read's cancel function runs when it is (Bind).
	cancelAll    context.Context
	cancelAllNow context.CancelFunc
	// closed: CancelAll was called and Reopen was not since.
	closed bool
}

// Bind ties a read's cancel function to CancelAll: it runs when CancelAll is
// called before the read's own cancel. The returned function replaces
// cancel: it unbinds, then cancels. A nil cancel is allowed (nothing to
// cancel, nothing bound).
func (g *Gate) Bind(cancel context.CancelFunc) context.CancelFunc {
	if g == nil || cancel == nil {
		return cancel
	}
	g.mu.Lock()
	if g.closed {
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
	return func() {
		stop()
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
	g.mu.Unlock()
	if now != nil {
		now()
	}
}

// Reopen lets reads that bind from now on run.
func (g *Gate) Reopen() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closed = false
	g.mu.Unlock()
}

// Enter records a read about to build its SQL and returns the function that
// records it as bound. The returned function is safe to call more than once
// and from any goroutine.
func (g *Gate) Enter() (leave func()) {
	if g == nil {
		return func() {}
	}
	g.mu.Lock()
	e := g.epoch
	if g.active == nil {
		g.active = map[uint64]int{}
	}
	g.active[e]++
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if g.active[e]--; g.active[e] <= 0 {
				delete(g.active, e)
				// The last read of an epoch left: a waiter looks again.
				if g.moved != nil {
					close(g.moved)
					g.moved = nil
				}
			}
			g.mu.Unlock()
		})
	}
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
// in flight, or with ctx's error. ended must be a value Flip returned: reads
// keep entering the current epoch, so waiting on it may never end.
func (g *Gate) Wait(ctx context.Context, ended uint64) error {
	if g == nil {
		return nil
	}
	for {
		g.mu.Lock()
		busy := false
		for e := range g.active {
			if e <= ended {
				busy = true
				break
			}
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
		select {
		case <-moved:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// InFlight is how many reads have entered and not left.
func (g *Gate) InFlight() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, c := range g.active {
		n += c
	}
	return n
}
