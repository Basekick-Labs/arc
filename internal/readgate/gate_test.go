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

func TestCancelAllCancelsBoundReads(t *testing.T) {
	var g Gate
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := g.Bind(0, cancel1)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := g.Bind(0, cancel2)
	done2() // finished before the cancel: unbound
	g.CancelAll()
	select {
	case <-ctx1.Done():
	case <-time.After(time.Second):
		t.Fatal("a bound read was not cancelled")
	}
	done1()
	if ctx2.Err() == nil {
		t.Fatal("unreachable: done2 cancels its own context")
	}
	// Bound while closed: cancelled at once.
	ctxC, cancelC := context.WithCancel(context.Background())
	doneC := g.Bind(1, cancelC)
	if ctxC.Err() == nil {
		t.Fatal("a read bound after CancelAll and before Reopen ran")
	}
	doneC()
	g.Reopen()
	// Bound after the reopen: not cancelled by the earlier call.
	ctx3, cancel3 := context.WithCancel(context.Background())
	done3 := g.Bind(1, cancel3)
	defer done3()
	time.Sleep(10 * time.Millisecond)
	if ctx3.Err() != nil {
		t.Fatal("a read bound after CancelAll was cancelled")
	}
	g.CancelAll()
	select {
	case <-ctx3.Done():
	case <-time.After(time.Second):
		t.Fatal("a second CancelAll did not cancel")
	}
	var nilGate *Gate
	if nilGate.Bind(0, nil) != nil {
		t.Fatal("nil cancel bound")
	}
	nilGate.CancelAll()
}

// A read that entered before CancelAll stays cancelled at its Bind, even
// after Reopen: its exclusions are from before what the node missed.
func TestReadFromBeforeCancelAllStaysCancelled(t *testing.T) {
	var g Gate
	old, leave := g.EnterAt()
	defer leave()
	g.CancelAll()
	g.Reopen()
	ctx, cancel := context.WithCancel(context.Background())
	done := g.Bind(old, cancel)
	defer done()
	if ctx.Err() == nil {
		t.Fatal("a read from before CancelAll ran after Reopen")
	}
	fresh, leave2 := g.EnterAt()
	defer leave2()
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := g.Bind(fresh, cancel2)
	defer done2()
	if ctx2.Err() != nil {
		t.Fatal("a read from after the reopen was cancelled")
	}
}
