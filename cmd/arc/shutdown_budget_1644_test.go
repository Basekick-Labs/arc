package main

import (
	"context"
	"testing"
	"time"
)

// #1644: the cluster ready drain takes at most half of the hooks' remaining
// share, so the HTTP drain after it keeps a grace.
func TestReadyDrainGraceWithin(t *testing.T) {
	if got := readyDrainGraceWithin(context.Background(), 10*time.Second); got != 10*time.Second {
		t.Fatalf("no deadline: %v", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if got := readyDrainGraceWithin(ctx, 10*time.Second); got > 4*time.Second || got < 3900*time.Millisecond {
		t.Fatalf("8s left: %v, want about 4s", got)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Minute)
	defer cancel2()
	if got := readyDrainGraceWithin(ctx2, 10*time.Second); got != 10*time.Second {
		t.Fatalf("a minute left: %v", got)
	}
	ctx3, cancel3 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel3()
	if got := readyDrainGraceWithin(ctx3, 10*time.Second); got != 0 {
		t.Fatalf("spent: %v", got)
	}
}

// The components keep the buffer's half plus a tenth: at the default 30 s
// the hooks get 12 s, which leaves the cluster ready drain 6 s and the HTTP
// drain at least 6 s.
func TestShutdownComponentReserve(t *testing.T) {
	if got := shutdownComponentReserve(30 * time.Second); got != 18*time.Second {
		t.Fatalf("30s: %v", got)
	}
	if got := shutdownComponentReserve(5 * time.Second); got != 3*time.Second {
		t.Fatalf("5s: %v", got)
	}
}
