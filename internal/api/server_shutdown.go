package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

// The force stages of ShutdownContext: each is a fifth of the drain budget,
// within these bounds. The floor lets a cancelled query or a closed
// connection unwind even when the budget is already spent; the two stages
// can then overrun the hooks' share by up to 200 ms, which the components'
// tenth (cmd/arc shutdownComponentReserve) covers from a
// server.shutdown_timeout of 2 s up.
const (
	shutdownForceMin = 100 * time.Millisecond
	shutdownForceMax = 2 * time.Second
)

// httpShutdownStages are the waits of one HTTP drain: the grace in-flight
// requests get to finish, then two force waits — after the queries are
// cancelled, and after the connections still open are closed.
type httpShutdownStages struct {
	grace, force time.Duration
}

func httpShutdownStagesFor(budget time.Duration) httpShutdownStages {
	if budget < 0 {
		budget = 0
	}
	force := budget / 5
	if force < shutdownForceMin {
		force = shutdownForceMin
	}
	if force > shutdownForceMax {
		force = shutdownForceMax
	}
	grace := budget - 2*force
	if grace < 0 {
		grace = 0
	}
	return httpShutdownStages{grace: grace, force: force}
}

// shutdownNewConnAllowance is how long a drain waits on a connection that has
// never sent a request, counted from when it was accepted: its first request,
// or its TLS handshake, may still be on the way, and closing it would reset a
// write its client cannot safely repeat. 5 s is the rule net/http uses.
// fasthttp has it too, but counts from after the TLS handshake, so with TLS
// it never closed a connection that did not handshake (#1840).
//
// shutdownSweepInterval is how often the drain looks for such connections.
// Variables so a test can shorten them.
var (
	shutdownNewConnAllowance = 5 * time.Second
	shutdownSweepInterval    = 50 * time.Millisecond
)

// connTracker records the server's open connections through fasthttp's
// ConnState hook, so shutdown can close the ones a graceful drain would wait
// on: a response stream blocked on a client that stopped reading sees no
// context, only a closed connection; and a connection that never sent a
// request is not one fasthttp's own drain reliably closes.
type connTracker struct {
	mu    sync.Mutex
	conns map[net.Conn]*trackedConn
}

type trackedConn struct {
	accepted time.Time
	used     bool // a request has started on it
	closing  bool // shutdown has closed it; fasthttp has not reported it yet
}

func newConnTracker() *connTracker {
	return &connTracker{conns: map[net.Conn]*trackedConn{}}
}

func (t *connTracker) connState(c net.Conn, state fasthttp.ConnState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch state {
	case fasthttp.StateNew:
		t.conns[c] = &trackedConn{accepted: time.Now()}
	case fasthttp.StateActive:
		// The first byte of a request has arrived.
		if tc := t.conns[c]; tc != nil {
			tc.used = true
		}
	case fasthttp.StateClosed, fasthttp.StateHijacked:
		delete(t.conns, c)
	}
}

// closeAll closes every tracked connection and returns how many it had not
// closed before. fasthttp closes them again when their serve loops end; a
// second Close is harmless.
func (t *connTracker) closeAll() int {
	return t.closeWhere(func(*trackedConn) bool { return true })
}

// closeUnused closes the connections that have never sent a request and were
// accepted at least minAge ago, and returns how many.
func (t *connTracker) closeUnused(minAge time.Duration) int {
	now := time.Now()
	return t.closeWhere(func(tc *trackedConn) bool {
		return !tc.used && now.Sub(tc.accepted) >= minAge
	})
}

func (t *connTracker) closeWhere(match func(*trackedConn) bool) int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	var conns []net.Conn
	for c, tc := range t.conns {
		// An entry stays until fasthttp reports the close, so one closed
		// by an earlier call is still here: not counted or closed twice.
		if tc.closing || !match(tc) {
			continue
		}
		tc.closing = true
		conns = append(conns, c)
	}
	t.mu.Unlock()
	// Concurrently and without waiting: a tls.Conn's Close sends a
	// close-notify alert, which can block up to 5 s on a client whose
	// receive window is full.
	for _, c := range conns {
		go func(c net.Conn) { _ = c.Close() }(c)
	}
	return len(conns)
}

// usedOpen is the number of connections a request has started on that
// shutdown has not closed: the ones a query can still be running on.
func (t *connTracker) usedOpen() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, tc := range t.conns {
		if tc.used && !tc.closing {
			n++
		}
	}
	return n
}

// QueryLifetime is the context the query handler runs its reads on
// (QueryHandler.SetLifetime). ShutdownContext cancels it once the drain's
// grace is over, so a query still running then ends instead of holding the
// shutdown (#1644).
func (s *Server) QueryLifetime() context.Context {
	if s.queryLifetime == nil {
		return context.Background()
	}
	return s.queryLifetime
}

// endQueries cancels QueryLifetime. A Server not built by NewServer has none.
func (s *Server) endQueries() {
	if s.cancelQueries != nil {
		s.cancelQueries()
	}
}

// ShutdownContext drains the HTTP server within ctx's deadline (#1644):
//
//  1. stop accepting, close idle connections, and let in-flight requests
//     finish for the grace; a connection that has never sent a request is
//     closed once it is shutdownNewConnAllowance old, or when the grace ends;
//  2. cancel QueryLifetime, which ends the queries still running and their
//     response streams, and wait one force stage;
//  3. close the connections still open, and wait one more.
//
// The grace is the budget less the two force stages, so the drain returns
// close to the deadline instead of holding the shutdown's whole budget, and
// the coordinator's later hooks and components still run. Without a deadline
// the budget is fallback.
//
// It returns nil once the server has stopped, even when it had to cancel
// queries or close connections (a warning says so). It returns an error only
// when requests are still running after both force stages: a handler that
// waits on neither its context nor its connection.
func (s *Server) ShutdownContext(ctx context.Context, fallback time.Duration) error {
	s.logger.Info().Msg("Shutting down server gracefully...")

	s.onShutdownStartMu.Lock()
	fns := s.onShutdownStart
	s.onShutdownStartMu.Unlock()
	for _, fn := range fns {
		fn()
	}
	// Whatever happens below, no query outlives the server.
	defer s.endQueries()

	budget := fallback
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}
	st := httpShutdownStagesFor(budget)

	// One drain for the whole sequence. fasthttp's ShutdownWithContext holds
	// the server's lock while it waits and cannot be restarted, so it gets a
	// context that outlives every stage, and this function stops waiting on
	// it at the last one.
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()
	stopped := make(chan error, 1)
	go func() { stopped <- s.app.ShutdownWithContext(drainCtx) }()

	finish := func(err error) error {
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("server shutdown failed: %w", err)
		}
		s.logger.Info().Msg("Server stopped")
		return nil
	}

	grace := time.NewTimer(st.grace)
	defer grace.Stop()
	sweep := time.NewTicker(shutdownSweepInterval)
	defer sweep.Stop()
	closedUnused := func(n int) {
		if n > 0 {
			s.logger.Info().Int("connections", n).Msg("HTTP shutdown: closed connections that never sent a request")
		}
	}
graceStage:
	for {
		select {
		case err := <-stopped:
			return finish(err)
		case <-grace.C:
			break graceStage
		case <-sweep.C:
			// A connection that has sent nothing since it was accepted:
			// fasthttp's drain waits on it, without limit when it has not
			// finished a TLS handshake.
			closedUnused(s.conns.closeUnused(shutdownNewConnAllowance))
		}
	}

	// The grace is over. A connection that never sent a request has had all
	// the time there is; no query runs on it.
	closedUnused(s.conns.closeUnused(0))
	s.endQueries()
	if n := s.conns.usedOpen(); n > 0 {
		s.logger.Warn().Dur("grace", st.grace).Int("connections", n).Msg("HTTP shutdown: cancelled the queries still running after the grace period")
	}
	force := time.NewTimer(st.force)
	defer force.Stop()
	select {
	case err := <-stopped:
		return finish(err)
	case <-force.C:
	}

	if n := s.conns.closeAll(); n > 0 {
		s.logger.Warn().Int("connections", n).Msg("HTTP shutdown: closed connections still open after in-flight queries were cancelled")
	}
	force.Reset(st.force)
	select {
	case err := <-stopped:
		return finish(err)
	case <-force.C:
	}
	return errors.New("server shutdown failed: requests still running after their queries were cancelled and their connections closed")
}
