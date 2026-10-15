package originstream

import (
	"context"
	"testing"
)

// #1266: the capped state clears after a trim frees room, without waiting for
// the primary's next batch.
func TestCappedClearsAfterTrim(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.segBytes = 1 // one record per segment
	for c := uint64(1); c <= 4; c++ {
		if err := s.Append("p", ent(1, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	s.maxBytes = s.bytes // at its cap
	s.capped = true
	s.mu.Unlock()
	if err := s.AcceptFloors("p", &Floors{Instance: 1, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Floor: 4}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil { // floors apply once durable
		t.Fatal(err)
	}
	if _, err := s.Trim(context.Background(), allReady); err != nil {
		t.Fatal(err)
	}
	if s.Stats().Capped {
		t.Fatal("still capped after the trim emptied the store")
	}
}

// Crossing half the cap asks for a trim pass; staying above does not repeat.
func TestTrimWantedAtHalfTheCap(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	one := s.Stats().Bytes
	s.mu.Lock()
	s.maxBytes = 3 * one // half = 1.5 records
	s.mu.Unlock()
	select {
	case <-s.TrimWanted():
		t.Fatal("asked below half the cap")
	default:
	}
	if err := s.Append("p", ent(1, "db/a", 2)); err != nil { // crosses half
		t.Fatal(err)
	}
	select {
	case <-s.TrimWanted():
	default:
		t.Fatal("no trim asked when crossing half the cap")
	}
}

// Below the free-space reserve new batches are dropped (recorded as gaps);
// they are kept again once space returns.
func TestLowSpaceDropsNewBatches(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	before := s.Stats().Bytes
	s.SetLowSpace(true, 1, 2)
	if err := s.Append("p", ent(1, "db/a", 2)); err != nil {
		t.Fatal(err)
	}
	if s.Stats().Bytes != before {
		t.Fatal("a batch was stored below the reserve")
	}
	s.SetLowSpace(false, 10, 2)
	if err := s.Append("p", ent(1, "db/a", 3)); err != nil {
		t.Fatal(err)
	}
	if s.Stats().Bytes == before {
		t.Fatal("not storing again after space returned")
	}
}

// Entering low space first evicts what the policy allows, and never an
// instance the policy keeps (a claim in progress, the live primary's).
func TestLowSpaceEvictsOnlyWhatThePolicyAllows(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	mustAppend(t, s, "a", ent(1, "k", 1)) // kept (EvictNever)
	mustAppend(t, s, "b", ent(2, "k", 1)) // evictable (EvictFirst)
	tier := map[uint64]int{1: EvictNever, 2: EvictFirst}
	s.SetEvictable(func(in InstanceInfo) int { return tier[in.Instance] }, func() bool { return true })
	s.SetLowSpace(true, 1, 2)
	if len(s.Held("a", 1)["k"]) == 0 {
		t.Fatal("low space evicted an instance the policy keeps")
	}
	if len(s.Held("b", 2)["k"]) != 0 {
		t.Fatal("low space did not evict what the policy allows")
	}
}
