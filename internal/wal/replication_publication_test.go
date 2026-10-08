package wal

import (
	"sync/atomic"
	"testing"
)

func TestDeferredReplicationPublishesOnlyAdmittedEntries(t *testing.T) {
	w := newReceivedTestWriter(t)
	w.DeferTrackedReplication()
	var published atomic.Int64
	var gotIdentity string
	w.SetReplicationHook(func(entry *ReplicationEntry) {
		published.Add(1)
		var err error
		gotIdentity, _, err = TrackedPayload(entry.TrackedPayload)
		if err != nil {
			t.Error(err)
		}
	})
	accepted, err := w.AppendTracked([]map[string]interface{}{{"m": "cpu", "time": int64(1700000000000000), "v": int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := w.AppendTracked([]map[string]interface{}{{"m": "cpu", "time": int64(1700000000000000), "v": int64(2)}})
	if err != nil {
		t.Fatal(err)
	}
	if published.Load() != 0 {
		t.Fatal("replication ran before buffer admission")
	}
	w.ForgetTracked(rejected)
	w.PublishTracked(rejected)
	if published.Load() != 0 {
		t.Fatal("rejected write reached replica")
	}
	// A fast flush can checkpoint before PublishTracked gets scheduled.
	if err := w.MarkFlushed(accepted); err != nil {
		t.Fatal(err)
	}
	w.PublishTracked(accepted)
	w.PublishTracked(accepted)
	if published.Load() != 1 || gotIdentity != accepted[0] {
		t.Fatalf("published %d, identity=%s", published.Load(), gotIdentity)
	}
	w.pendingMu.Lock()
	pending := len(w.stagedReplication)
	w.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("retained %d serialized entries after admission", pending)
	}
}
