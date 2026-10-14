package originstream

import (
	"context"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
)

// #1266: a trim asks once per (segment, key), with the segment's batches of
// the key and the instance, so a segment whose files are here goes while a
// later one waits; floors arrive unsorted and still count.
func TestTrimAsksPerSegmentAndKey(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.segBytes = 1 // one record per segment
	for c := uint64(1); c <= 4; c++ {
		if err := s.Append("p", ent(7, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	// Settled out of order: sorted when accepted.
	f := &Floors{Instance: 7, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Settled: [][2]uint64{{3, 4}, {1, 2}}}}}
	if err := s.AcceptFloors("p", f); err != nil {
		t.Fatal(err)
	}
	var asked []TrimRequest
	ready := func(_ context.Context, reqs []TrimRequest) []bool {
		asked = reqs
		out := make([]bool, len(reqs))
		for i, r := range reqs {
			// The files holding batches 1 and 2 are here; 3 and 4 are not.
			out[i] = r.CSeqs[0][1] <= 2
		}
		return out
	}
	if _, err := s.Trim(context.Background(), ready); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 4 {
		t.Fatalf("asked %d requests, want one per segment: %+v", len(asked), asked)
	}
	for _, r := range asked {
		if r.Instance != 7 || r.Origin != "p" || r.Key != "db/a" || r.AppliedIndex != 9 || len(r.CSeqs) != 1 || r.CSeqs[0][0] != r.CSeqs[0][1] {
			t.Fatalf("request %+v", r)
		}
	}
	if got := s.Stats().Segments; got != 2 {
		t.Fatalf("segments = %d, want 2 (batches 3 and 4 wait for their files)", got)
	}
	// Two segments wait, on one key.
	if got := metrics.Get().Snapshot()["standby_store_blocked_keys"].(int64); got != 1 {
		t.Fatalf("blocked keys = %d, want 1", got)
	}
	if held := s.Held("p", 7)["db/a"]; len(held) != 1 || held[0] != [2]uint64{3, 4} {
		t.Fatalf("held %v, want [[3 4]]", held)
	}
}

// The blocked gauge counts measurements per origin, not per instance: two
// instances of one origin waiting on one key are one blocked key.
func TestTrimBlockedKeysPerOriginAndKey(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	for _, inst := range []uint64{7, 8} {
		if err := s.Append("p", ent(inst, "db/a", 1)); err != nil {
			t.Fatal(err)
		}
		if err := s.AcceptFloors("p", &Floors{Instance: inst, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Floor: 1}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Trim(context.Background(), noneReady); err != nil {
		t.Fatal(err)
	}
	if got := metrics.Get().Snapshot()["standby_store_blocked_keys"].(int64); got != 1 {
		t.Fatalf("blocked keys = %d, want 1", got)
	}
}

// A readiness answer of the wrong length trims nothing.
func TestTrimMalformedAnswerTrimsNothing(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.segBytes = 1
	for c := uint64(1); c <= 2; c++ {
		if err := s.Append("p", ent(7, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AcceptFloors("p", &Floors{Instance: 7, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Floor: 2}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Trim(context.Background(), func(context.Context, []TrimRequest) []bool { return []bool{true} }); err != nil {
		t.Fatal(err)
	}
	if got := s.Stats().Segments; got != 2 {
		t.Fatalf("segments = %d, want 2", got)
	}
}

// Floors whose settled intervals arrive out of order still cover a segment
// spanning them: they are sorted when accepted, and when loaded at open (the
// file keeps them as received).
func TestTrimUnsortedSettledFloors(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		dir := t.TempDir()
		s := openTestStore(t, dir, 0)
		for c := uint64(1); c <= 4; c++ {
			if err := s.Append("p", ent(7, "db/a", c)); err != nil {
				t.Fatal(err)
			}
		}
		f := &Floors{Instance: 7, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Settled: [][2]uint64{{3, 4}, {1, 2}}}}}
		if err := s.AcceptFloors("p", f); err != nil {
			t.Fatal(err)
		}
		if reopen {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openTestStore(t, dir, 0)
		}
		if _, err := s.Trim(context.Background(), allReady); err != nil {
			t.Fatal(err)
		}
		if got := s.Stats().Segments; got != 0 {
			t.Fatalf("reopen=%v: segments = %d, want 0: unsorted settled floors did not cover the segment", reopen, got)
		}
	}
}
