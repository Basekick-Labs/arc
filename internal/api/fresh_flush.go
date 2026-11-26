package api

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
)

// A query carrying "x-arc-freshness: flush" asks for an answer that includes
// every row this node acknowledged before the request arrived (#1760). On
// /api/v1/query and /api/v1/query/msgpack, with query.fresh_flush_enabled on
// a standalone node with local storage and no active tiered storage, Arc
// flushes the measurements the query reads (each at most once per
// query.fresh_flush_min_interval_ms, inside the forced-flush budget visible
// writes share), waits until every such row is in a stored file, then builds
// and runs the query as usual: 200 with X-Arc-Freshness: flushed and
// X-Arc-Freshness-Wait-Ms. Everything else is refused before the query runs,
// with X-Arc-Freshness: false and X-Arc-Freshness-Reason (the label of
// arc_query_fresh_flush_total as well):
//
//   - invalid_value (400): any value other than "flush" (case-insensitive,
//     trimmed); every value of a repeated header is read, empty ones count
//     as absent;
//   - disabled (403): the key off, on every query route. A client written
//     against the feature off keeps getting exactly this;
//   - unsupported_route (400): the key on, a route other than the two above;
//   - cluster, storage_not_supported (object storage, or a local volume that
//     folds names), tiering_not_supported (400), before any forward and
//     before the request is counted in arc_query_requests_total.
//
// The check runs in the handler, so the route's auth middleware has already
// admitted the request. The exits after it are freshExit's; the table is in
// the PR and the release notes.

const (
	// FreshnessHeader is the request header; its one value is "flush".
	FreshnessHeader = "x-arc-freshness"
	// FreshnessAnswerHeader, FreshnessReasonHeader and FreshnessWaitHeader
	// carry the answer. The absence of FreshnessAnswerHeader never means
	// fresh.
	FreshnessAnswerHeader = "X-Arc-Freshness"
	FreshnessReasonHeader = "X-Arc-Freshness-Reason"
	FreshnessWaitHeader   = "X-Arc-Freshness-Wait-Ms"

	freshnessReasonInvalidValue     = "invalid_value"
	freshnessReasonDisabled         = "disabled"
	freshnessReasonUnsupportedRoute = "unsupported_route"
	freshnessReasonCluster          = "cluster"
	freshnessReasonStorage          = "storage_not_supported"
	freshnessReasonTiering          = "tiering_not_supported"
	freshnessReasonShow             = "show_not_supported"
	freshnessReasonForbidden        = "forbidden"
	freshnessReasonTooMany          = "too_many_measurements"
	freshnessReasonBusy             = "busy"
	freshnessReasonQueryError       = "query_error"
	freshnessReasonWALReplay        = "flush_failed_wal_replay"
	freshnessReasonRecoveryPending  = "recovery_pending"
	freshnessReasonFlushFailed      = "flush_failed"
	freshnessReasonNotConfirmed     = "not_confirmed"
	freshnessReasonTimeout          = "timeout"
	freshnessReasonShutdown         = "shutdown"
	freshnessReasonCancelled        = "cancelled"
	freshnessReasonUnavailable      = "fresh_flush_unavailable"
	freshnessFlushed                = "flushed"

	// freshWaitedLocal marks a request that waited for flushes: the request
	// logger leaves it out of the HTTP latency, arc_query_fresh_flush_seconds
	// records the wait.
	freshWaitedLocal = "arc.fresh_waited"
)

var (
	// freshFlushMaxKeys bounds the measurements one fresh query may name.
	freshFlushMaxKeys = 64
	// freshFlushBudget bounds a fresh query's wait, below its own deadline.
	freshFlushBudget = 30 * time.Second
	// freshDrainDone is the channel the HTTP drain closes (fasthttp's
	// RequestCtx.Done; nil under app.Test). A var so a test can close it.
	freshDrainDone = func(c *fiber.Ctx) <-chan struct{} { return c.Context().Done() }
)

// FreshFlushConfig is what the node's configuration says about fresh
// queries.
type FreshFlushConfig struct {
	Enabled        bool          // query.fresh_flush_enabled
	Clustered      bool          // cluster.enabled: refused, never forwarded
	StorageBackend string        // storage.backend: only "local" is supported
	MinInterval    time.Duration // query.fresh_flush_min_interval_ms
	MaxWaiters     int           // query.fresh_flush_max_waiters
	// RetryAfter answers a refusal while WAL recovery is pending:
	// wal.recovery_interval_seconds, at most an hour.
	RetryAfter time.Duration
}

// FreshFlushWaiter is the buffer's side of a fresh query
// (ingest.ArrowBuffer.WaitKeysFlushed).
type FreshFlushWaiter interface {
	WaitKeysFlushed(ctx context.Context, keys []string, interval time.Duration) error
}

type freshFlush struct {
	cfg     FreshFlushConfig
	waiter  FreshFlushWaiter
	waiters atomic.Int64
}

// SetFreshFlush configures x-arc-freshness. Call before serving. With
// cfg.Enabled the waiter must be the buffer whose key flush ledger and task
// tracking are on; without them every fresh query is answered 503
// fresh_flush_unavailable.
func (h *QueryHandler) SetFreshFlush(cfg FreshFlushConfig, w FreshFlushWaiter) {
	if cfg.RetryAfter > time.Hour {
		cfg.RetryAfter = time.Hour
	}
	h.fresh = &freshFlush{cfg: cfg, waiter: w}
}

func (h *QueryHandler) freshEnabled() bool { return h.fresh != nil && h.fresh.cfg.Enabled }

// readFreshnessHeader: present is false without the header or with only
// empty values; invalid when any value is not "flush". One Peek without it.
func readFreshnessHeader(c *fiber.Ctx) (present, invalid bool) {
	hd := &c.Request().Header
	if hd.Peek(FreshnessHeader) == nil {
		return false, false
	}
	for _, v := range hd.PeekAll(FreshnessHeader) {
		v = bytes.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		present = true
		if !bytes.EqualFold(v, []byte("flush")) {
			invalid = true
		}
	}
	return present, invalid
}

// refuseFresh sets the answer headers of a refusal and counts it.
func refuseFresh(c *fiber.Ctx, status int, reason, msg string) (int, string) {
	c.Set(FreshnessAnswerHeader, "false")
	c.Set(FreshnessReasonHeader, reason)
	metrics.Get().IncQueryFreshFlush(reason)
	return status, msg
}

// freshnessCommon refuses what every query route refuses alike: an invalid
// value and the key off.
func (h *QueryHandler) freshnessCommon(c *fiber.Ctx) (present bool, status int, msg string) {
	present, invalid := readFreshnessHeader(c)
	switch {
	case !present:
		return false, 0, ""
	case invalid:
		status, msg = refuseFresh(c, fiber.StatusBadRequest, freshnessReasonInvalidValue, `invalid x-arc-freshness value: the only value is "flush"; the query was not run`)
	case !h.freshEnabled():
		status, msg = refuseFresh(c, fiber.StatusForbidden, freshnessReasonDisabled, "x-arc-freshness is not available on this server; the query was not run")
	}
	return true, status, msg
}

// freshnessRefusal is the check of the routes that do not support the
// header (arrow, estimate, :measurement): status 0 without it; otherwise a
// refusal the caller writes in its route's error shape.
func (h *QueryHandler) freshnessRefusal(c *fiber.Ctx) (status int, msg string) {
	present, status, msg := h.freshnessCommon(c)
	if !present || status != 0 {
		return status, msg
	}
	return refuseFresh(c, fiber.StatusBadRequest, freshnessReasonUnsupportedRoute, "x-arc-freshness is supported only on /api/v1/query and /api/v1/query/msgpack; the query was not run")
}

// freshBegin is executeQuery's check, before the forward and the request
// count: fresh when the request is to wait, or a refusal.
func (h *QueryHandler) freshBegin(c *fiber.Ctx) (fresh bool, status int, msg string) {
	present, status, msg := h.freshnessCommon(c)
	if !present || status != 0 {
		return false, status, msg
	}
	cfg := &h.fresh.cfg
	switch {
	case cfg.Clustered || h.router != nil:
		status, msg = refuseFresh(c, fiber.StatusBadRequest, freshnessReasonCluster, "x-arc-freshness is not supported on a cluster; the query was not run")
	case cfg.StorageBackend != "local" || storageFoldsNames(h.storage):
		status, msg = refuseFresh(c, fiber.StatusBadRequest, freshnessReasonStorage, "x-arc-freshness is supported only on local storage that does not fold names; the query was not run")
	case h.tieringManager != nil:
		// WaitKeysFlushed cannot see a file stored but left unregistered.
		status, msg = refuseFresh(c, fiber.StatusBadRequest, freshnessReasonTiering, "x-arc-freshness is not supported while tiered storage is active; the query was not run")
	default:
		return true, 0, ""
	}
	return false, status, msg
}

// freshExit is deferred by executeQuery for a request it waits for, right
// after freshBegin. It runs before Fiber writes the answer, so it covers
// every writer of executeQuery, which writes each error itself and never
// returns one to Fiber's error handler: keep it that way. A non-200 answer
// that set no reason of its own (any error before the wait, or any error
// after a wait that succeeded) becomes false/query_error; an error inside a
// stream that started with 200 stays the query's own. A panic, before or
// after the wait, is answered 500 by the recover middleware with
// false/query_error, never flushed; it is re-raised. (An emitted-refs refusal
// after the wait, possible only if the two transforms disagree, is
// query_error too, since it comes after flushed.) Every such request is
// counted once in arc_query_fresh_flush_total.
func freshExit(c *fiber.Ctx, flushed *bool) {
	p := recover()
	reason := string(c.Response().Header.Peek(FreshnessReasonHeader))
	if p != nil || (c.Response().StatusCode() != fiber.StatusOK && (*flushed || reason == "")) {
		reason = freshnessReasonQueryError
		c.Set(FreshnessAnswerHeader, "false")
		c.Set(FreshnessReasonHeader, reason)
	} else if *flushed {
		reason = freshnessFlushed
	}
	metrics.Get().IncQueryFreshFlush(reason)
	if p != nil {
		panic(p)
	}
}

// freshReason sets the reason of an exit before the wait that has one of its
// own (SHOW, RBAC); a no-op for a request that does not wait.
func freshReason(c *fiber.Ctx, fresh bool, reason string) {
	if fresh {
		c.Set(FreshnessAnswerHeader, "false")
		c.Set(FreshnessReasonHeader, reason)
	}
}

// freshKeys turns the transform's emitted references into buffer keys,
// deduplicated: the references are the (database, measurement) the query's
// storage paths were built from, and ingest keys its buffers by the same
// pair.
func freshKeys(refs []TableReference) []string {
	keys := make([]string, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, r := range refs {
		k := r.Database + "/" + r.Measurement
		if _, dup := seen[k]; !dup {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	return keys
}

// freshWait runs a fresh query's wait: a transform of its SQL to learn the
// measurements it reads (in its own read-gate entry, left before the wait),
// the RBAC check of those, the key and waiter limits, and the wait itself.
// status 0: the wait succeeded and the answer headers say flushed; the
// caller builds and runs the query as usual. Otherwise the refusal, with
// its headers and counters done.
func (h *QueryHandler) freshWait(c *fiber.Ctx, sql, headerDB string, timeout time.Duration, deadline time.Time) (status int, msg string) {
	m := metrics.Get()
	refs, err := func() ([]TableReference, error) {
		// Not holdRead: it parks its entry on the request for the stream
		// writer, and the real transform's entry would overwrite it. Left
		// before the wait, and on a panic.
		_, leave := h.reads.EnterAt()
		defer leave()
		transformCtx, cancelTransform := transformContext(c.Context(), deadline)
		defer cancelTransform()
		_, _, refs, err := h.getTransformedSQLForQueryWithRefs(transformCtx, sql, headerDB)
		return refs, err
	}()
	if err != nil {
		m.IncQueryErrors()
		if errors.Is(err, errQueryTimedOut) {
			m.IncQueryTimeouts()
			h.logTransformTimeout(sql, timeout)
		}
		return transformErrorStatus(err), err.Error()
	}
	// Before anything is flushed: a token cannot flush, or time, what it
	// cannot read.
	if err := h.checkEmittedQueryPermissions(c, "read", refs); err != nil {
		m.IncQueryErrors()
		freshReason(c, true, freshnessReasonForbidden)
		return fiber.StatusForbidden, err.Error()
	}
	keys := freshKeys(refs)
	if len(keys) > freshFlushMaxKeys {
		m.IncQueryErrors()
		c.Set(FreshnessAnswerHeader, "false")
		c.Set(FreshnessReasonHeader, freshnessReasonTooMany)
		return fiber.StatusBadRequest, "x-arc-freshness: the query reads more than " + strconv.Itoa(freshFlushMaxKeys) + " measurements; the query was not run"
	}
	if len(keys) == 0 { // reads no measurement: nothing to wait for
		c.Set(FreshnessAnswerHeader, freshnessFlushed)
		c.Set(FreshnessWaitHeader, "0")
		return 0, ""
	}
	f := h.fresh
	if f.waiters.Add(1) > int64(f.cfg.MaxWaiters) {
		f.waiters.Add(-1)
		// Counted as an error, as governance counts its own 429
		// (checkQueryGovernance).
		m.IncQueryErrors()
		c.Set(FreshnessAnswerHeader, "false")
		c.Set(FreshnessReasonHeader, freshnessReasonBusy)
		c.Set(fiber.HeaderRetryAfter, "5")
		return fiber.StatusServiceUnavailable, "x-arc-freshness: too many fresh queries waiting on this node; retry later; the query was not run"
	}
	m.AddQueryFreshFlushWaiters(1)
	defer func() { // the slot and the gauge, on every return and on a panic
		f.waiters.Add(-1)
		m.AddQueryFreshFlushWaiters(-1)
	}()
	c.Locals(freshWaitedLocal, true)

	budget := time.Now().Add(freshFlushBudget)
	queryDeadlineFirst := !deadline.IsZero() && deadline.Before(budget)
	if queryDeadlineFirst {
		budget = deadline
	}
	ctx, cancel := context.WithDeadline(c.UserContext(), budget)
	defer cancel() // also called right after the wait, below
	drained := freshDrainDone(c)
	if drained != nil {
		go func() {
			select {
			case <-drained:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	h.watchQueryClientDisconnect(ctx, c.Context().Conn(), "", cancel, metrics.DisconnectPathSQLJSON)
	t0 := time.Now()
	err = f.waiter.WaitKeysFlushed(ctx, keys, f.cfg.MinInterval)
	waited := time.Since(t0)
	ctxErr := ctx.Err()
	// At once: it stops the disconnect watcher, so execution's own watcher
	// is the only one (a disconnect is counted once).
	cancel()
	m.ObserveQueryFreshFlushWait(waited)
	c.Set(FreshnessWaitHeader, strconv.FormatInt(waited.Milliseconds(), 10))

	if err == nil {
		c.Set(FreshnessAnswerHeader, freshnessFlushed)
		return 0, ""
	}
	var reason string
	status, reason, msg = h.freshWaitError(err, ctxErr, drained, queryDeadlineFirst)
	m.IncQueryErrors()
	if reason == freshnessReasonTimeout && queryDeadlineFirst {
		m.IncQueryTimeouts()
	}
	c.Set(FreshnessAnswerHeader, "false")
	c.Set(FreshnessReasonHeader, reason)
	if reason == freshnessReasonWALReplay || reason == freshnessReasonRecoveryPending {
		c.Set(fiber.HeaderRetryAfter, strconv.Itoa(max(1, int(f.cfg.RetryAfter/time.Second))))
	}
	return status, msg
}

// freshWaitError maps WaitKeysFlushed's error to the answer: the one table
// from the buffer's errors to (status, reason). The buffer's own texts are
// never sent.
func (h *QueryHandler) freshWaitError(err, ctxErr error, drained <-chan struct{}, queryDeadlineFirst bool) (status int, reason, msg string) {
	const notRun = "; the query was not run"
	var rp *ingest.RecoveryPendingError
	switch {
	case ctxErr != nil && errors.Is(err, ctxErr):
		select {
		case <-drained: // nil: never ready
			return fiber.StatusServiceUnavailable, freshnessReasonShutdown, "x-arc-freshness: the server is shutting down" + notRun
		default:
		}
		if h.shuttingDown() {
			return fiber.StatusServiceUnavailable, freshnessReasonShutdown, "x-arc-freshness: the server is shutting down" + notRun
		}
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			if queryDeadlineFirst {
				return fiber.StatusGatewayTimeout, freshnessReasonTimeout, "x-arc-freshness: query.timeout passed while the query waited for its measurements' flushes" + notRun
			}
			return fiber.StatusGatewayTimeout, freshnessReasonTimeout, "x-arc-freshness: the measurements' flushes took longer than " + freshFlushBudget.String() + notRun
		}
		return fiber.StatusServiceUnavailable, freshnessReasonCancelled, "x-arc-freshness: the query was cancelled while it waited for its measurements' flushes" + notRun
	case errors.Is(err, ingest.ErrBufferClosing):
		return fiber.StatusServiceUnavailable, freshnessReasonShutdown, "x-arc-freshness: the server is shutting down" + notRun
	case errors.As(err, &rp):
		if rp.Reason == ingest.RecoveryReasonRecoveryPending {
			return fiber.StatusServiceUnavailable, freshnessReasonRecoveryPending, "x-arc-freshness: a WAL recovery pass left rows to replay; retry after the next pass" + notRun
		}
		return fiber.StatusServiceUnavailable, freshnessReasonWALReplay, "x-arc-freshness: a flush failed and its rows wait for WAL recovery; retry after the next pass" + notRun
	case errors.Is(err, ingest.ErrKeyFlushFailed):
		return fiber.StatusServiceUnavailable, freshnessReasonFlushFailed, "x-arc-freshness: a flush of a measurement the query reads failed" + notRun
	case errors.Is(err, ingest.ErrKeyFlushUnconfirmed):
		return fiber.StatusServiceUnavailable, freshnessReasonNotConfirmed, "x-arc-freshness: a flush of a measurement the query reads ended without a confirmed store" + notRun
	}
	// ErrLedgerOff, or an error this table does not know: a wiring fault.
	h.logger.Error().Err(err).Msg("x-arc-freshness: the buffer cannot wait for flushes although query.fresh_flush_enabled is on")
	return fiber.StatusServiceUnavailable, freshnessReasonUnavailable, "x-arc-freshness is unavailable on this server right now" + notRun
}
