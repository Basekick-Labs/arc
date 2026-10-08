package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"sync"

	"github.com/basekick-labs/arc/internal/replicaview"
)

type replicationLeaseKey struct{}
type replicationQueryLease struct {
	mu        sync.Mutex
	snapshots map[string]*replicaview.Snapshot
}

func (l *replicationQueryLease) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, snapshot := range l.snapshots {
		_ = snapshot.Close()
		delete(l.snapshots, key)
	}
	return nil
}

// SetReplicationView is configured before serving requests. resolve must name
// immutable files whose lifetime follows the query snapshot, including while
// a streamed response is still being consumed.
func (h *QueryHandler) SetReplicationView(view *replicaview.View, resolve func(string) string) {
	h.replicationView = view
	h.replicationResolve = resolve
}

func (h *QueryHandler) SetReplicationSync(sync func(context.Context) error) { h.replicationSync = sync }

func (h *QueryHandler) attachReplicationLease(ctx context.Context) (context.Context, error) {
	if h.replicationView == nil {
		return ctx, nil
	}
	if _, ok := ctx.Value(replicationLeaseKey{}).(*replicationQueryLease); ok {
		return ctx, nil
	}
	if h.replicationSync != nil {
		if err := h.replicationSync(ctx); err != nil {
			return ctx, errors.Join(replicaview.ErrUnavailable, err)
		}
	}
	owner, ok := ctx.(interface {
		SetUserValue(interface{}, interface{})
	})
	if !ok {
		return ctx, fmt.Errorf("replication query requires a request-owned snapshot lease")
	}
	lease := &replicationQueryLease{snapshots: make(map[string]*replicaview.Snapshot)}
	// fasthttp closes io.Closer user values on request reset, after the response
	// stream ends. A handler defer would release files too early for streaming.
	owner.SetUserValue(replicationLeaseKey{}, lease)
	return context.WithValue(ctx, replicationLeaseKey{}, lease), nil
}

func (h *QueryHandler) replicationReadExpr(ctx context.Context, database, measurement, keyword string) string {
	lease, ok := ctx.Value(replicationLeaseKey{}).(*replicationQueryLease)
	if !ok || h.replicationResolve == nil {
		recordStoragePathFailure(ctx, fmt.Errorf("replication query has no source lease"))
		return keyword + " (SELECT NULL)"
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	key := database + "\x00" + measurement
	snapshot, ok := lease.snapshots[key]
	if !ok {
		snapshot = h.replicationView.Snapshot(database, measurement)
		lease.snapshots[key] = snapshot
	}
	relation, err := snapshot.SQL(h.replicationResolve, h.anchorFor(ctx, database, measurement), buildReadParquetOptions())
	if err != nil {
		recordStoragePathFailure(ctx, err)
		return keyword + " (SELECT NULL)"
	}
	return keyword + " " + relation
}

// Catch-up delays are retryable availability failures, not malformed SQL.
func queryTransformErrorStatus(err error) int {
	if errors.Is(err, replicaview.ErrUnavailable) {
		return fiber.StatusServiceUnavailable
	}
	return fiber.StatusBadRequest
}
