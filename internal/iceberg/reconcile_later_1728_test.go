package iceberg

import (
	"context"
	"strings"
	"testing"
	"time"
)

func withLaterDelays(t *testing.T, a, b, c time.Duration) {
	t.Helper()
	old := laterDelays
	laterDelays = [3]time.Duration{a, b, c}
	t.Cleanup(func() { laterDelays = old })
}

func hotKeys(s *Scheduler) string {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	var out []string
	for k := range s.requestedHot {
		out = append(out, k)
	}
	return strings.Join(out, ",")
}

// A shared bucket's manifest delete is asked about after each delay, without
// re-listing the cold tier, and file keys collapse to their directory
// (#1728).
func TestRequestReconcilePathsLaterAsksAfterTheDelayWithoutCold(t *testing.T) {
	withLaterDelays(t, 150*time.Millisecond, time.Hour, time.Hour)
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	r.write(t, winIn1, 20)
	r.write(t, winIn2, 30)
	r.sched.runPass(ctx)
	src := &coldRecorder{StorageWalkSource: r.src}
	r.sched.source = src
	if err := r.be.Delete(ctx, winIn1); err != nil {
		t.Fatal(err)
	}
	r.sched.RequestReconcilePathsLater([]string{winIn1, winIn2})
	r.sched.serviceRequests(ctx)
	if len(src.events) != 0 {
		t.Fatalf("served before the delay: %v", src.events)
	}
	deadline := time.Now().Add(5 * time.Second)
	for hotKeys(r.sched) == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := hotKeys(r.sched); got != hourDir {
		t.Fatalf("queued %q, want the files' directory once", got)
	}
	r.sched.serviceRequests(ctx)
	if got := strings.Join(src.events, ","); got != "list mydb/cpu" {
		t.Fatalf("events %s, want one listing and no cold invalidation", got)
	}
	if got := r.live(t); strings.Join(got, ",") != "cpu_in2.parquet" {
		t.Fatalf("live = %v", got)
	}
}

// A key that arrives while a bucket's timer runs still waits that bucket's
// full delay: it gets a timer of its own after the current one fires.
func TestRequestReconcilePathsLaterKeepsEachKeysDelay(t *testing.T) {
	withLaterDelays(t, 300*time.Millisecond, time.Hour, time.Hour)
	s := NewScheduler(SchedulerConfig{})
	s.RequestReconcilePathsLater([]string{"db/a/"})
	time.Sleep(250 * time.Millisecond)
	s.RequestReconcilePathsLater([]string{"db/b/"}) // 50 ms before the first timer fires
	time.Sleep(150 * time.Millisecond)              // the first fired; b has waited 150 ms
	if got := hotKeys(s); got != "db/a/" {
		t.Fatalf("after the first timer: %q, want db/a/ only", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(hotKeys(s), "db/b/") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(hotKeys(s), "db/b/") {
		t.Fatal("the later key never fired")
	}
}

// A hot-only change is served at once, without dropping the cold listing.
func TestRequestReconcilePathsHotServesNowWithoutCold(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	r.write(t, winIn1, 20)
	r.write(t, winIn2, 30)
	r.sched.runPass(ctx)
	src := &coldRecorder{StorageWalkSource: r.src}
	r.sched.source = src
	if err := r.be.Delete(ctx, winIn1); err != nil {
		t.Fatal(err)
	}
	r.sched.RequestReconcilePathsHot([]string{winIn1})
	r.sched.serviceRequests(ctx)
	if got := strings.Join(src.events, ","); got != "list mydb/cpu" {
		t.Fatalf("events %s, want a listing without cold invalidation", got)
	}
}
