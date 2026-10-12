package originstream

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

func chunkEnt(inst uint64, key string, cseq uint64, chunk, chunks uint16) *Entry {
	return &Entry{Instance: inst, Key: key, CSeq: cseq, Chunk: chunk, Chunks: chunks, Payload: []byte(fmt.Sprintf("%s-%d-%d", key, cseq, chunk))}
}

func mustAppend(t *testing.T, s *Store, origin string, es ...*Entry) {
	t.Helper()
	for _, e := range es {
		if err := s.Append(origin, e); err != nil {
			t.Fatal(err)
		}
	}
}

// Received: whole batches only (gaps and the batch still arriving are not),
// minus what the floors say is registered; it survives a reopen.
func TestReceived(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	mustAppend(t, s, "o", ent(7, "db/cpu", 1), ent(7, "db/cpu", 2), ent(7, "db/cpu", 4), // 3 is a gap
		chunkEnt(7, "db/cpu", 5, 0, 2), chunkEnt(7, "db/cpu", 5, 1, 2), // whole in two chunks
		chunkEnt(7, "db/cpu", 6, 0, 3), chunkEnt(7, "db/cpu", 6, 2, 3), // chunk 1 missing
		chunkEnt(7, "db/cpu", 7, 0, 2), // still arriving
		ent(7, "db/mem", 1))
	want := "map[db/cpu:[[1 2] [4 5]] db/mem:[[1 1]]]"
	if got := fmt.Sprint(s.Received("o", 7)); got != want {
		t.Fatalf("received %s, want %s", got, want)
	}
	if err := s.AcceptFloors("o", &Floors{Instance: 7, AppliedIndex: 3, Keys: map[string]KeyFloor{
		"db/cpu": {Floor: 1, Settled: [][2]uint64{{4, 4}}}}}); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(s.Received("o", 7)); got != "map[db/cpu:[[2 2] [5 5]] db/mem:[[1 1]]]" {
		t.Fatalf("after floors: %s", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openTestStore(t, dir, 0)
	if got := fmt.Sprint(s2.Received("o", 7)); got != "map[db/cpu:[[2 2] [5 5]] db/mem:[[1 1]]]" {
		t.Fatalf("after reopen: %s", got)
	}
	if s2.Received("o", 8) != nil {
		t.Fatal("unknown instance")
	}
}

// Iterate yields every stored record once, in order, across calls and a
// retransmission, and resumes from its cursor.
func TestIterate(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.segBytes = 64 // a few records per segment
	mustAppend(t, s, "o", ent(7, "db/cpu", 1), ent(7, "db/cpu", 2), ent(7, "db/mem", 1),
		ent(7, "db/cpu", 2), // retransmitted
		chunkEnt(7, "db/cpu", 3, 0, 2), chunkEnt(7, "db/cpu", 3, 1, 2), ent(7, "db/cpu", 4))
	var got []string
	limit := 2
	collect := func(r Record) bool {
		if limit >= 0 && len(got) == limit {
			return false // refused: the next call starts with it
		}
		got = append(got, fmt.Sprintf("%s/%d.%d=%s", r.Key, r.CSeq, r.Chunk, r.Payload))
		return true
	}
	cur, damaged, err := s.Iterate("o", 7, Cursor{}, collect)
	if err != nil || damaged != 0 || len(got) != 2 {
		t.Fatalf("first call: %v %d %v", got, damaged, err)
	}
	saved := cur
	limit = -1
	cur, _, err = s.Iterate("o", 7, cur, collect)
	if err != nil {
		t.Fatal(err)
	}
	want := "[db/cpu/1.0=db/cpu-1 db/cpu/2.0=db/cpu-2 db/mem/1.0=db/mem-1 db/cpu/3.0=db/cpu-3-0 db/cpu/3.1=db/cpu-3-1 db/cpu/4.0=db/cpu-4]"
	if fmt.Sprint(got) != want {
		t.Fatalf("records %v\nwant    %s", got, want)
	}
	// A retry from a cursor kept earlier sees the same records again.
	got = nil
	if _, _, err := s.Iterate("o", 7, saved, collect); err != nil || len(got) != 4 || got[0] != "db/mem/1.0=db/mem-1" {
		t.Fatalf("retry from a kept cursor: %v %v", got, err)
	}
	// Records appended later are read by the next call from the cursor.
	mustAppend(t, s, "o", ent(7, "db/cpu", 5))
	got = nil
	if _, _, err := s.Iterate("o", 7, cur, collect); err != nil || fmt.Sprint(got) != "[db/cpu/5.0=db/cpu-5]" {
		t.Fatalf("resumed: %v %v", got, err)
	}
	if _, _, err := s.Iterate("o", 9, Cursor{}, collect); err != ErrNoInstance {
		t.Fatalf("unknown instance: %v", err)
	}
}

// A segment trimmed while an iteration runs is skipped; a damaged record
// ends its segment and is counted.
func TestIterateTrimmedAndDamaged(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.segBytes = 20 // one record per segment
	mustAppend(t, s, "o", ent(7, "k", 1), ent(7, "k", 2), ent(7, "k", 3), ent(7, "k", 4))
	s.mu.Lock()
	d := s.instances[instanceID{"o", 7}]
	if len(d.segs) < 3 {
		s.mu.Unlock()
		t.Fatalf("segments %d", len(d.segs))
	}
	first, second := d.segs[0].path, d.segs[1].path
	s.mu.Unlock()
	var got []uint64
	_, damaged, err := s.Iterate("o", 7, Cursor{}, func(r Record) bool {
		got = append(got, r.CSeq)
		if r.CSeq == 1 {
			_ = os.Remove(second) // trimmed under the reader
		}
		return true
	})
	if err != nil || damaged != 0 || fmt.Sprint(got) != "[1 3 4]" {
		t.Fatalf("trimmed: %v %d %v", got, damaged, err)
	}
	raw, _ := os.ReadFile(first)
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(first, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	got = nil
	_, damaged, err = s.Iterate("o", 7, Cursor{}, func(r Record) bool { got = append(got, r.CSeq); return true })
	if err != nil || damaged != 1 || fmt.Sprint(got) != "[3 4]" {
		t.Fatalf("damaged: %v %d %v", got, damaged, err)
	}
}

// With a policy, the cap evicts what it allows, then what it allows under
// pressure, and keeps the rest (dropping new batches instead).
func TestEvictPolicy(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	mustAppend(t, s, "a", ent(1, "k", 1), ent(1, "k", 2))
	mustAppend(t, s, "b", ent(2, "k", 1), ent(2, "k", 2))
	mustAppend(t, s, "c", ent(3, "k", 1), ent(3, "k", 2))
	size := s.Stats().Bytes
	var seen []InstanceInfo
	tier := map[uint64]int{1: EvictNever, 2: EvictUnderPressure, 3: EvictFirst}
	s.SetEvictable(func(in InstanceInfo) int { seen = append(seen, in); return tier[in.Instance] }, func() bool { return true })
	s.maxBytes = size // the next append is over the cap
	mustAppend(t, s, "a", ent(1, "k", 3))
	has := func(origin string, inst uint64) bool { return len(s.Received(origin, inst)) > 0 }
	if !has("a", 1) || !has("b", 2) || has("c", 3) {
		t.Fatalf("after one eviction: a %v b %v c %v", has("a", 1), has("b", 2), has("c", 3))
	}
	if len(seen) == 0 || seen[0].Floors {
		t.Fatalf("policy input %+v", seen)
	}
	// Still over: the under-pressure tier goes next; the pinned one stays.
	s.maxBytes = s.Stats().Bytes
	mustAppend(t, s, "a", ent(1, "k", 4))
	if !has("a", 1) || has("b", 2) {
		t.Fatalf("after pressure: a %v b %v", has("a", 1), has("b", 2))
	}
	// Only pinned data left: new batches are dropped (gaps), nothing evicted.
	s.maxBytes = s.Stats().Bytes
	mustAppend(t, s, "a", ent(1, "k", 5))
	if fmt.Sprint(s.Received("a", 1)) != "map[k:[[1 4]]]" || len(keyGaps(s, "a", 1, "k")) == 0 {
		t.Fatalf("pinned: %v gaps %v", s.Received("a", 1), keyGaps(s, "a", 1, "k"))
	}
	if got := metrics.Get().Snapshot()["standby_store_pinned_bytes"].(int64); got != 0 {
		t.Fatalf("pinned bytes %d for an instance the store keeps anyway", got)
	}
	// Its origin moved on: the store alone would evict it, the policy keeps
	// it. That is what the gauge counts.
	s.SetCurrent("a", 99)
	s.lastPinCalc = time.Time{}
	mustAppend(t, s, "c", ent(5, "k", 1))
	if got := metrics.Get().Snapshot()["standby_store_pinned_bytes"].(int64); got == 0 {
		t.Fatal("pinned bytes not reported")
	}
}

// Without a policy the cap evicts replaced instances only (unchanged).
func TestEvictWithoutPolicyKeepsTheOldRule(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.SetCurrent("a", 1)
	mustAppend(t, s, "a", ent(1, "k", 1))
	s.SetCurrent("a", 2)
	mustAppend(t, s, "a", ent(2, "k", 1))
	s.maxBytes = s.Stats().Bytes
	mustAppend(t, s, "a", ent(2, "k", 2))
	if len(s.Received("a", 1)) != 0 || len(s.Received("a", 2)) == 0 {
		t.Fatal("the replaced instance was not the one evicted")
	}
}

func TestDropInstance(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	mustAppend(t, s, "a", ent(1, "k", 1), ent(2, "k", 1))
	before := s.Stats().Bytes
	if err := s.DropInstance("a", 1); err != nil {
		t.Fatal(err)
	}
	if s.Received("a", 1) != nil || s.Stats().Bytes >= before || len(s.Instances()) != 1 {
		t.Fatalf("not dropped: %+v", s.Instances())
	}
	if _, err := s.Trim(context.Background(), allReady); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s2 := openTestStore(t, dir, 0); len(s2.Instances()) != 1 {
		t.Fatal("dropped instance came back on reopen")
	}
}

// A drop while a trim pass waits for readiness does not free the bytes twice.
func TestDropInstanceDuringTrim(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	mustAppend(t, s, "o", ent(7, "k", 1), ent(8, "k", 1))
	if err := s.AcceptFloors("o", &Floors{Instance: 7, Keys: map[string]KeyFloor{"k": {Floor: 1}}}); err != nil {
		t.Fatal(err)
	}
	ready := func(ctx context.Context, keys []TrimKey) map[TrimKey]bool {
		if err := s.DropInstance("o", 7); err != nil {
			t.Error(err)
		}
		return allReady(ctx, keys)
	}
	if _, err := s.Trim(context.Background(), ready); err != nil {
		t.Fatal(err)
	}
	var held int64
	for _, in := range s.Instances() {
		held += in.Bytes
	}
	if got := s.Stats().Bytes; got != held || held == 0 {
		t.Fatalf("store bytes %d, instances hold %d", got, held)
	}
}

// With the policy installed but not active (takeover off in the cluster),
// the cap keeps the store's own rule.
func TestEvictPolicyInactive(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.SetCurrent("a", 1)
	mustAppend(t, s, "a", ent(1, "k", 1))
	s.SetCurrent("a", 2)
	mustAppend(t, s, "a", ent(2, "k", 1))
	called := false
	s.SetEvictable(func(InstanceInfo) int { called = true; return EvictNever }, func() bool { return false })
	s.maxBytes = s.Stats().Bytes
	mustAppend(t, s, "a", ent(2, "k", 2))
	if called || len(s.Received("a", 1)) != 0 {
		t.Fatalf("inactive policy consulted %v, replaced kept %v", called, s.Received("a", 1))
	}
}
