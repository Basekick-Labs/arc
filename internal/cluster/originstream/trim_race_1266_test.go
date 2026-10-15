package originstream

import (
	"context"
	"testing"
)

// #1266 review B1: Trim asks readiness without the store lock. A batch the
// primary streams into the open segment meanwhile was never vetted (it is not
// registered yet); the trim must not take it with the segment.
func TestTrimKeepsBatchesAppendedDuringReadiness(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	for c := uint64(1); c <= 2; c++ {
		if err := s.Append("p", ent(1, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	// Both batches registered: the open segment is a trim candidate.
	if err := s.AcceptFloors("p", &Floors{Instance: 1, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Floor: 2}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil { // floors apply once durable
		t.Fatal(err)
	}
	if _, err := s.Trim(context.Background(), func(ctx context.Context, reqs []TrimRequest) []bool {
		// The primary streams batch 3 while readiness runs.
		if err := s.Append("p", ent(1, "db/a", 3)); err != nil {
			t.Error(err)
		}
		return allReady(ctx, reqs)
	}); err != nil {
		t.Fatal(err)
	}
	held := s.Held("p", 1)["db/a"]
	found := false
	for _, iv := range held {
		if iv[0] <= 3 && 3 <= iv[1] {
			found = true
		}
	}
	if !found {
		t.Fatalf("batch 3, appended during readiness and never registered, was trimmed: held %v", held)
	}
}
