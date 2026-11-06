package ingest

import (
	"context"
	"sync"
)

type liveRequestAdmission struct {
	mu     sync.Mutex
	closed bool
	active int
	idle   chan struct{}
}

// TrackLiveAdmission opts a cluster runtime into lifecycle request tracking.
// Wire once at startup, before accepting HTTP or MQTT; standalone stays zero-allocation.
func (b *ArrowBuffer) TrackLiveAdmission() {
	b.liveAdmission.CompareAndSwap(nil, &liveRequestAdmission{})
}

func (a *liveRequestAdmission) enter() (func(), bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, false
	}
	a.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.active--
			if a.active == 0 && a.idle != nil {
				close(a.idle)
				a.idle = nil
			}
		})
	}, true
}

// CloseLiveAdmission joins whole live requests, including MQTT callbacks that
// passed the ingest gate before licensing fenced the node. WAL replay uses its
// separate recovery path and remains available to settle earlier failures.
func (b *ArrowBuffer) CloseLiveAdmission(ctx context.Context) error {
	a := b.liveAdmission.Load()
	if a == nil {
		return nil
	}
	a.mu.Lock()
	a.closed = true
	if a.active == 0 {
		a.mu.Unlock()
		return nil
	}
	if a.idle == nil {
		a.idle = make(chan struct{})
	}
	idle := a.idle
	a.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
