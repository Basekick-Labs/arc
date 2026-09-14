package audit

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

func auditSnapshotInt(t *testing.T, key string) int64 {
	t.Helper()
	v, ok := metrics.Get().Snapshot()[key]
	if !ok {
		t.Fatalf("metric %q missing from snapshot", key)
	}
	n, ok := v.(int64)
	if !ok {
		t.Fatalf("metric %q is %T, want int64", key, v)
	}
	return n
}

// TestAudit_DroppedEventsAreCounted pins that an audit event discarded because
// the queue is full is visible in metrics.
//
// Regression test for #802: internal/audit did not import internal/metrics at
// all. LogEvent dropped events on a full channel with only a Warn log, so an
// audit trail could develop silent gaps under load — with both audit metrics
// reading 0 the whole time. For a compliance-driven feature, undetectable loss
// is the worst failure mode.
func TestAudit_DroppedEventsAreCounted(t *testing.T) {
	before := auditSnapshotInt(t, "audit_events_dropped")

	// A logger with a full channel and no writer draining it: every LogEvent
	// after the buffer fills must take the drop path.
	l := &Logger{
		eventCh: make(chan *AuditEvent, 1),
	}

	const attempts = 5
	for i := 0; i < attempts; i++ {
		l.LogEvent(&AuditEvent{EventType: "test", Timestamp: time.Now().UTC()})
	}

	after := auditSnapshotInt(t, "audit_events_dropped")
	dropped := after - before

	// One event fits in the buffer; the rest are dropped.
	if want := int64(attempts - 1); dropped != want {
		t.Fatalf("audit_events_dropped advanced by %d, want %d — dropped audit events are not counted (#802)", dropped, want)
	}
}
