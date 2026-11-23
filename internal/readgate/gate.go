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
// A read enters before its SQL is built and leaves once its statement is
// bound. The job changes its state, then calls Flip and waits for every read
// that entered before the flip to leave.
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
	// moved is closed and replaced whenever a read leaves.
	moved chan struct{}
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
			}
			if g.moved != nil {
				close(g.moved)
				g.moved = nil
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
// in flight, or with ctx's error.
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
