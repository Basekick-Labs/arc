package iceberg

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A shared bucket's manifest delete is asked about later, once per delay
// bucket however many keys arrive, and without re-listing the cold tier
// (#1728).
func TestRequestReconcilePathsLaterCoalescesAndSkipsCold(t *testing.T) {
	old := laterDelays
	laterDelays = [3]time.Duration{30 * time.Millisecond, 60 * time.Millisecond, 90 * time.Millisecond}
	t.Cleanup(func() { laterDelays = old })

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
	r.sched.RequestReconcilePathsLater([]string{winIn1})
	r.sched.RequestReconcilePathsLater([]string{winIn1}) // same bucket timers
	r.sched.serviceRequests(ctx)
	if len(src.events) != 0 {
		t.Fatalf("served before the delay: %v", src.events)
	}
	time.Sleep(50 * time.Millisecond)
	r.sched.serviceRequests(ctx)
	if got := strings.Join(src.events, ","); got != "list mydb/cpu" {
		t.Fatalf("after the first delay: %s, want one listing and no cold invalidation", got)
	}
	if got := r.live(t); strings.Join(got, ",") != "cpu_in2.parquet" {
		t.Fatalf("live = %v", got)
	}
	time.Sleep(80 * time.Millisecond)
	r.sched.serviceRequests(ctx)
	// The later buckets ask again (the object may have gone later); the
	// fingerprint is unchanged, so nothing is committed.
	if got := r.live(t); strings.Join(got, ",") != "cpu_in2.parquet" {
		t.Fatalf("live after follow-ups = %v", got)
	}
}
