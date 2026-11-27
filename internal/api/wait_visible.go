package api

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
)

// A MessagePack or line-protocol write carrying "x-arc-wait: visible" is
// answered once its rows are visible to a query on this node that starts
// after the answer (#1759), waiting at most waitVisibleBudget. With a forcer
// set (SetForcer) the measurements it wrote are flushed early: at most once
// per ingest.wait_visible_min_interval_ms per measurement, and only inside
// the forced-flush budget (spare flush capacity), which fresh queries share
// (#1760). Otherwise it waits for its rows' normal age or size flush.
//
//   - 204, X-Arc-Visible: true: every row is in a stored (and, with tiering,
//     registered) file a query lists.
//   - 202, X-Arc-Visible: false, X-Arc-Visible-Reason: the rows were
//     accepted, as a 204 without the header says, but are not known to be
//     visible (ingest.VisibleReason*).
//   - 500, reason flush_failed_rows_at_risk: a flush holding rows without a
//     WAL copy failed; they may still be stored.
//
// Refused before anything is read or forwarded: any other value (400), the
// feature off (403), a cluster (400), other routes (400). Over
// waitVisibleMaxWaiters at once: 503 wait_visible_busy, before admission.

const (
	// WaitVisibleHeader is the request header; its one value is "visible".
	WaitVisibleHeader = "x-arc-wait"
	// VisibleHeader and VisibleReasonHeader carry the answer. The absence of
	// VisibleHeader never means visible.
	VisibleHeader       = "X-Arc-Visible"
	VisibleReasonHeader = "X-Arc-Visible-Reason"

	// The reasons of answers that are not waited on: a partial write (400)
	// and a write stopped part-way (500), each with its usual status.
	visibleReasonPartial = "partial_write"
	visibleReasonStopped = "write_stopped"

	// waitVisibleLocal marks a request that waited: the request logger
	// leaves it out of the HTTP latency, arc_ingest_wait_visible_seconds
	// records it.
	waitVisibleLocal = "arc.wait_visible"
)

var (
	// waitVisibleBudget bounds a write's wait. A var for tests.
	waitVisibleBudget = 20 * time.Second
	// waitVisibleMaxWaiters bounds the visible writes in progress on this
	// node, from just before admission to the answer. A var for tests.
	waitVisibleMaxWaiters int64 = 1024
)

// WaitVisibleConfig is what the node's configuration says about visible
// writes.
type WaitVisibleConfig struct {
	Enabled   bool // ingest.wait_visible_enabled
	Clustered bool // cluster.enabled: refused, never forwarded
	// StorageBackend is storage.backend. Only "local" is confirmable in this
	// version: an object store's listing is not known to show a new file to
	// the next query, so its writes answer 202 storage_not_confirmable.
	StorageBackend string
	// FileTimePruning and FileTimePruningMargin are query.file_time_pruning
	// and its margin, as the pruner gets them: rows stamped past the margin,
	// in the current hour, can be hidden until the hour closes.
	FileTimePruning       bool
	FileTimePruningMargin time.Duration
}

// WaitVisible serves x-arc-wait: visible for the write handlers. A nil
// *WaitVisible is the feature off.
type WaitVisible struct {
	enabled, clustered, storageConfirmable, futureCheck bool
	margin                                              time.Duration
	waiters                                             atomic.Int64
	// forcer and forceInterval shorten a wait by flushing its measurements
	// early; nil forcer: never (SetForcer).
	forcer        ingest.KeyForcer
	forceInterval time.Duration
}

// SetForcer lets a visible write flush the measurements it wrote early, at
// most once per interval per measurement (ingest.wait_visible_min_interval_ms).
// Set at startup, before the routes serve, and only where the buffer's ledger
// is enabled; without it a wait never forces. A nil wv is a no-op.
func (wv *WaitVisible) SetForcer(f ingest.KeyForcer, interval time.Duration) {
	if wv == nil {
		return
	}
	wv.forcer, wv.forceInterval = f, interval
}

// NewWaitVisible builds the node's WaitVisible; one shared by every handler,
// so the waiter limit is the node's.
func NewWaitVisible(cfg WaitVisibleConfig) *WaitVisible {
	local := cfg.StorageBackend == "local"
	return &WaitVisible{
		enabled:            cfg.Enabled,
		clustered:          cfg.Clustered,
		storageConfirmable: local,
		// The pruner never prunes object-store paths by file time.
		futureCheck: cfg.FileTimePruning && local,
		margin:      cfg.FileTimePruningMargin,
	}
}

// visibleWrite is one request's wait; nil when the request does not wait.
type visibleWrite struct {
	wv      *WaitVisible
	held    bool
	tAdm    time.Time
	outcome *ingest.FlushOutcome
}

// begin reads the header. handled: the request is refused and answered.
// Before anything is decoded or forwarded.
func (wv *WaitVisible) begin(c *fiber.Ctx) (vw *visibleWrite, handled bool, err error) {
	v := c.Request().Header.Peek(WaitVisibleHeader)
	if len(v) == 0 {
		return nil, false, nil
	}
	switch {
	case !bytes.EqualFold(bytes.TrimSpace(v), []byte("visible")):
		return nil, true, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid x-arc-wait value: the only value is \"visible\"; nothing was written",
		})
	case wv == nil || !wv.enabled:
		return nil, true, c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "x-arc-wait is disabled. Set ingest.wait_visible_enabled=true in arc.toml to enable it; nothing was written",
		})
	case wv.clustered:
		return nil, true, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "x-arc-wait is not supported on a cluster; nothing was written",
		})
	}
	return &visibleWrite{wv: wv}, false, nil
}

// refuseWaitVisible answers 400 for x-arc-wait on a route that does not
// support it, before anything is decoded or forwarded; handled false
// without the header.
func refuseWaitVisible(c *fiber.Ctx) (handled bool, err error) {
	if len(c.Request().Header.Peek(WaitVisibleHeader)) == 0 {
		return false, nil
	}
	return true, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
		"error": "x-arc-wait is supported only on /api/v1/write/msgpack and /api/v1/write/line-protocol; nothing was written",
	})
}

// acquire takes a waiter slot, immediately before admission; false when
// the node is at its limit (answer ingest.ErrWaitVisibleBusy). Always true
// on a nil vw. Release with release, on every return.
func (vw *visibleWrite) acquire() bool {
	if vw == nil {
		return true
	}
	m := metrics.Get()
	if vw.wv.waiters.Add(1) > waitVisibleMaxWaiters {
		vw.wv.waiters.Add(-1)
		m.IncIngestWaitVisible(metrics.WaitVisibleBusy)
		return false
	}
	vw.held = true
	m.AddIngestWaitVisibleWaiters(1)
	// Before admission: an earlier time can only flag more rows as stamped
	// in the future, never fewer.
	vw.tAdm = time.Now()
	return true
}

func (vw *visibleWrite) release() {
	if vw == nil || !vw.held {
		return
	}
	vw.held = false
	vw.wv.waiters.Add(-1)
	metrics.Get().AddIngestWaitVisibleWaiters(-1)
}

// context is the admitted write context with the request's visible outcome;
// ctx itself on a nil vw.
func (vw *visibleWrite) context(ctx context.Context) context.Context {
	if vw == nil {
		return ctx
	}
	vw.outcome = ingest.NewVisibleOutcome(vw.tAdm, vw.wv.futureCheck, vw.wv.margin)
	return ingest.WithFlushOutcome(ctx, vw.outcome)
}

// notWaited marks an answer given without waiting (reason
// visibleReasonPartial or visibleReasonStopped); its status is unchanged.
func (vw *visibleWrite) notWaited(c *fiber.Ctx, reason string) {
	if vw == nil {
		return
	}
	c.Set(VisibleHeader, "false")
	c.Set(VisibleReasonHeader, reason)
	metrics.Get().IncIngestWaitVisible(metrics.WaitVisiblePartial)
}

// answer waits for the outcome of a write that returned without error and
// answers. rows is what the handler counted written; rowsKnown false when it
// could not count them. The caller has released the admission and the
// request body: nothing the wait holds is still counted by admission.
func (vw *visibleWrite) answer(c *fiber.Ctx, rows int64, rowsKnown bool) error {
	if waitVisibleBeforeWait != nil {
		waitVisibleBeforeWait(c)
	}
	c.Locals(waitVisibleLocal, true)
	// Read once: fasthttp closes it when the HTTP drain begins (nil under
	// app.Test, where the budget still ends the wait).
	done := c.Context().Done()
	verdict := ingest.WaitVisible(context.Background(), vw.outcome, ingest.VisibleWait{
		WrittenRows:        rows,
		WrittenRowsKnown:   rowsKnown,
		Budget:             waitVisibleBudget,
		Done:               done,
		StorageConfirmable: vw.wv.storageConfirmable,
		Forcer:             vw.wv.forcer,
		ForceInterval:      vw.wv.forceInterval,
	})
	m := metrics.Get()
	m.ObserveIngestWaitVisible(verdict.Waited)
	m.IncIngestWaitVisible(waitVisibleOutcome(verdict))
	return respondVisible(c, verdict, rows)
}

// waitVisibleBeforeWait, when set (tests only), runs as a visible write
// starts waiting: what the request still holds then is what it holds for the
// whole wait.
var waitVisibleBeforeWait func(c *fiber.Ctx)

// respondVisible answers a verdict: 204 only for visible, 500 only for rows
// at risk, 202 for every other reason, a future one included.
func respondVisible(c *fiber.Ctx, v ingest.VisibleVerdict, rows int64) error {
	if v.Visible {
		c.Set(VisibleHeader, "true")
		return c.SendStatus(fiber.StatusNoContent)
	}
	reason := v.Reason
	if reason == ingest.VisibleReasonNone {
		reason = ingest.VisibleReasonNotConfirmed
	}
	c.Set(VisibleHeader, "false")
	c.Set(VisibleReasonHeader, string(reason))
	if reason == ingest.VisibleReasonRowsAtRisk {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":        "the write was accepted, but a flush holding its rows failed and they had no WAL copy; they may still be stored, check before resending",
			"visible":      false,
			"reason":       reason,
			"rows_written": rows,
			"rows_at_risk": v.RowsAtRisk,
		})
	}
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"visible":      false,
		"reason":       reason,
		"rows_written": rows,
	})
}

func waitVisibleOutcome(v ingest.VisibleVerdict) int {
	switch {
	case v.Visible && v.Empty:
		return metrics.WaitVisibleEmpty
	case v.Visible:
		return metrics.WaitVisibleVisible
	}
	switch v.Reason {
	case ingest.VisibleReasonRowsAtRisk:
		return metrics.WaitVisibleRowsAtRisk
	case ingest.VisibleReasonWALReplay:
		return metrics.WaitVisibleWALReplay
	case ingest.VisibleReasonShutdown:
		return metrics.WaitVisibleShutdown
	case ingest.VisibleReasonTimeout:
		return metrics.WaitVisibleTimeout
	case ingest.VisibleReasonStorageNotConfirmable:
		return metrics.WaitVisibleStorageNotConfirmable
	case ingest.VisibleReasonFutureStamped:
		return metrics.WaitVisibleFutureStamped
	}
	return metrics.WaitVisibleNotConfirmed
}
