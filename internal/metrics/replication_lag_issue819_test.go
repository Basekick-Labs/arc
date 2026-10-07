package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestReplicationLagProviderOwnershipIssue819(t *testing.T) {
	m := &Metrics{startTime: time.Now()}

	unregisterOld := m.RegisterReplicationLagProvider(
		func() []ReplicationLagSample {
			return []ReplicationLagSample{
				{Peer: "old-reader", Entries: 4, HasSeconds: true, Seconds: 10},
			}
		},
	)

	unregisterNew := m.RegisterReplicationLagProvider(
		func() []ReplicationLagSample {
			return []ReplicationLagSample{
				{Peer: "new-reader", Entries: 2, HasSeconds: true, Seconds: 5},
			}
		},
	)

	// Cleanup of an old sender must not remove a newer sender's gauges.
	unregisterOld()

	output := m.PrometheusFormat()
	if !strings.Contains(
		output,
		`arc_replication_lag_entries{peer="new-reader"} 2`,
	) {
		t.Fatal("old cleanup removed the newer provider")
	}

	if strings.Contains(output, `peer="old-reader"`) {
		t.Fatal("old provider left historical peer series")
	}

	unregisterNew()

	output = m.PrometheusFormat()
	if strings.Contains(output, "arc_replication_lag_entries{peer=") ||
		strings.Contains(output, "arc_replication_lag_seconds{peer=") {
		t.Fatal("unregistered provider left stale peer series")
	}
}

func TestReplicationLagPeerLabelEscapingIssue819(t *testing.T) {
	m := &Metrics{startTime: time.Now()}

	peer := "reader\"one\\two\nthree"

	unregister := m.RegisterReplicationLagProvider(
		func() []ReplicationLagSample {
			return []ReplicationLagSample{
				{
					Peer:       peer,
					Entries:    7,
					Seconds:    2.5,
					HasSeconds: true,
				},
			}
		},
	)
	defer unregister()

	output := m.PrometheusFormat()

	for _, want := range []string{
		`arc_replication_lag_entries{peer="reader\"one\\two\nthree"} 7`,
		`arc_replication_lag_seconds{peer="reader\"one\\two\nthree"} 2.500000`,
	} {
		if !strings.Contains(output, want) {
			t.Errorf("missing or malformed escaped sample: %q", want)
		}
	}
}
