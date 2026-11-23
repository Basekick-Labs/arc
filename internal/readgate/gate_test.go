package readgate

import (
	"context"
	"sync"
	"testing"
	"time"
)

func waits(t *testing.T, g *Gate, ended uint64) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	return g.Wait(ctx, ended) != nil
}

// Wait waits for the reads that entered before the flip, and only for those.
func TestWaitIsForReadsThatEnteredBeforeTheFlip(t *testing.T) {
	var g Gate
	before := g.Enter()
	ended := g.Flip()
	after := g.Enter() // entered after the flip: not waited for
	if !waits(t, &g, ended) {
		t.Fatal("Wait returned while a read from before the flip was in flight")
	}
	before()
	before() // twice: counted once
	if waits(t, &g, ended) {
		t.Fatal("Wait still waits after the read from before the flip left, for one that entered after")
	}
	if g.InFlight() != 1 {
		t.Fatalf("in flight = %d, want the read that entered after the flip", g.InFlight())
	}
	// That read belongs to the next flip.
	ended2 := g.Flip()
	if !waits(t, &g, ended2) {
		t.Fatal("the second flip did not wait for the read from its own epoch")
	}
	after()
	if waits(t, &g, ended2) || g.InFlight() != 0 {
		t.Fatal("not idle after every read left")
	}
}

// A read from an earlier epoch still in flight is waited for by a later flip.
func TestWaitCoversEarlierEpochs(t *testing.T) {
	var g Gate
	old := g.Enter()
	g.Flip()
	ended := g.Flip()
	if !waits(t, &g, ended) {
		t.Fatal("a read from two epochs back was not waited for")
	}
	old()
	if waits(t, &g, ended) {
		t.Fatal("still waiting")
	}
}

// Wait wakes when the last read leaves, and gives up with the context.
func TestWaitWakesAndGivesUp(t *testing.T) {
	var g Gate
	leave := g.Enter()
	ended := g.Flip()
	done := make(chan error, 1)
	go func() { done <- g.Wait(context.Background(), ended) }()
	select {
	case <-done:
		t.Fatal("Wait returned with a read in flight")
	case <-time.After(30 * time.Millisecond):
	}
	leave()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not wake when the read left")
	}
	g.Enter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Wait(ctx, g.Flip()); err == nil {
		t.Fatal("Wait ignored a cancelled context")
	}
}

func TestNilGate(t *testing.T) {
	var g *Gate
	g.Enter()()
	if g.Wait(context.Background(), g.Flip()) != nil || g.InFlight() != 0 {
		t.Fatal("a nil gate must count nothing and never wait")
	}
}

func TestConcurrentReads(t *testing.T) {
	var g Gate
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			leave := g.Enter()
			time.Sleep(time.Millisecond)
			leave()
		}()
	}
	for i := 0; i < 20; i++ {
		if err := g.Wait(context.Background(), g.Flip()); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if err := g.Wait(context.Background(), g.Flip()); err != nil || g.InFlight() != 0 {
		t.Fatalf("in flight = %d, err = %v", g.InFlight(), err)
	}
}
