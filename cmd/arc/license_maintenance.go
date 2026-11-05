package main

import (
	"context"
	"sync"
)

// A pause is acknowledged only between complete maintenance passes. Cancellation
// or a failed drain releases it, so recovery remains available for the next try.
type licenseMaintenancePause struct {
	ready   chan struct{}
	release chan struct{}
	ctx     context.Context
}

func pauseLicenseMaintenance(ctx context.Context, requests chan<- licenseMaintenancePause) (func(), error) {
	p := licenseMaintenancePause{ready: make(chan struct{}), release: make(chan struct{}), ctx: ctx}
	var once sync.Once
	resume := func() { once.Do(func() { close(p.release) }) }
	select {
	case requests <- p:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-p.ready:
		return resume, nil
	case <-ctx.Done():
		resume()
		return nil, ctx.Err()
	}
}

func (p licenseMaintenancePause) wait(runtime context.Context) {
	close(p.ready)
	select {
	case <-p.release:
	case <-p.ctx.Done():
	case <-runtime.Done():
	}
}
