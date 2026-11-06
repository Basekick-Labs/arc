package cluster

import (
	"context"
	"sync"
)

// ServiceAdmission reserves whole requests before entering any data handler.
// Closing admission and observing zero reservations is one atomic barrier.
type ServiceAdmission struct {
	mu      sync.Mutex
	closed  bool
	active  int
	idle    chan struct{}
	allowed func() bool
}

func NewServiceAdmission(allowed func() bool) *ServiceAdmission {
	return &ServiceAdmission{allowed: allowed}
}
func (a *ServiceAdmission) Enter() (func(), bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || (a.allowed != nil && !a.allowed()) {
		return nil, false
	}
	a.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.active--
			if a.closed && a.active == 0 && a.idle != nil {
				close(a.idle)
				a.idle = nil
			}
		})
	}, true
}
func (a *ServiceAdmission) CloseAndWait(ctx context.Context) error {
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
	case <-ctx.Done():
		return ctx.Err()
	case <-idle:
		return nil
	}
}
