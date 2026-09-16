package cluster

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #856 writer-redundancy warning tests.
//
// Readers are never promotion candidates (WriterFailoverManager.selectPrimary
// filters on Role == RoleWriter), so a cluster's writer-role node count IS its
// redundancy. These tests drive checkWriterRedundancy directly, the same way
// the compactor-election tests drive checkCompactorElected.

// registerWriters adds n healthy writer-role nodes to the registry.
func registerWriters(t *testing.T, registry *Registry, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("writer-%d", i)
		node := NewNode(id, id, RoleWriter, "test-cluster")
		node.State = StateHealthy
		if err := registry.Register(node); err != nil {
			t.Fatalf("registry.Register %s: %v", id, err)
		}
	}
}

// pastGracePeriod moves the checker's birth time back so the startup
// suppression window has already elapsed.
func pastGracePeriod(h *HealthChecker) {
	h.startedAt = time.Now().Add(-2 * writerRedundancyGracePeriod)
}

func TestWriterRedundancy_WarnsBelowThreeInBothPatterns(t *testing.T) {
	tests := []struct {
		name     string
		mode     writerRedundancyMode
		writers  int
		wantWarn bool
		wantText string
	}{
		{"failover/zero writers", writerRedundancyFailover, 0, true, "fewer than two"},
		{"failover/one writer", writerRedundancyFailover, 1, true, "fewer than two"},
		{"failover/two writers", writerRedundancyFailover, 2, true, "only two"},
		{"failover/three writers", writerRedundancyFailover, 3, false, ""},
		{"failover/four writers", writerRedundancyFailover, 4, false, ""},
		{"shared-storage/zero writers", writerRedundancyLoadBalanced, 0, true, "fewer than two"},
		{"shared-storage/one writer", writerRedundancyLoadBalanced, 1, true, "fewer than two"},
		{"shared-storage/two writers", writerRedundancyLoadBalanced, 2, true, "only two"},
		{"shared-storage/three writers", writerRedundancyLoadBalanced, 3, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, registry, logs := newTestHealthChecker(t, false)
			pastGracePeriod(h)
			// A second reader so even the zero-writer rows are a cluster
			// rather than a single-node install, which is suppressed.
			peer := NewNode("reader-2", "reader-2", RoleReader, "test-cluster")
			peer.State = StateHealthy
			if err := registry.Register(peer); err != nil {
				t.Fatalf("registry.Register: %v", err)
			}
			registerWriters(t, registry, tt.writers)

			h.checkWriterRedundancy(tt.mode)

			got := logs.String()
			warned := strings.Contains(got, "ARC_CLUSTER_ROLE=writer")
			if warned != tt.wantWarn {
				t.Fatalf("warned=%v, want %v for %d writers. Logs:\n%s", warned, tt.wantWarn, tt.writers, got)
			}
			if !tt.wantWarn {
				return
			}
			if !strings.Contains(got, tt.wantText) {
				t.Errorf("expected message to contain %q, got:\n%s", tt.wantText, got)
			}
			if !strings.Contains(got, fmt.Sprintf(`"writer_nodes":%d`, tt.writers)) {
				t.Errorf("expected writer_nodes=%d in structured output, got:\n%s", tt.writers, got)
			}
		})
	}
}

// The two patterns lose writer availability for different reasons, so an
// operator must not be told to wait for a promotion that shared-storage mode
// deliberately never issues.
func TestWriterRedundancy_MessageIsPatternSpecific(t *testing.T) {
	failover := writerRedundancyMessage(writerRedundancyFailover, 1)
	shared := writerRedundancyMessage(writerRedundancyLoadBalanced, 1)

	if !strings.Contains(failover, "readers are never promotion candidates") {
		t.Errorf("Pattern 1 message should explain readers are not candidates, got: %s", failover)
	}
	if !strings.Contains(shared, "suppressed") {
		t.Errorf("Pattern 2 message should say promotion is suppressed, got: %s", shared)
	}
	if failover == shared {
		t.Error("the two patterns should not share one message")
	}
}

// Regression guard for a claim that was in the first draft of this warning and
// is NOT true of the shipped code: every node that joins becomes a Raft voter
// regardless of role (coordinator.go AddVoter), so a two-writer cluster does
// not lose quorum when one writer dies — readers carry the quorum. If #862
// ever narrows the voter set, this test is the place to revisit the wording.
func TestWriterRedundancy_MessageDoesNotClaimQuorumLoss(t *testing.T) {
	for _, mode := range []writerRedundancyMode{writerRedundancyFailover, writerRedundancyLoadBalanced} {
		for writers := 0; writers < writersForHA; writers++ {
			msg := strings.ToLower(writerRedundancyMessage(mode, writers))
			if strings.Contains(msg, "quorum") {
				t.Errorf("mode=%d writers=%d message claims quorum loss, which the current voter set does not support: %s", mode, writers, msg)
			}
		}
	}
}

func TestWriterRedundancy_SilentDuringGracePeriod(t *testing.T) {
	// A freshly constructed checker is inside the window: peers have not had
	// time to join yet, so a one-writer registry is not yet evidence of
	// anything.
	h, _, logs := newTestHealthChecker(t, false)
	registerWriters(t, h.registry, 1)

	h.checkWriterRedundancy(writerRedundancyFailover)

	if logs.Len() != 0 {
		t.Errorf("expected silence inside the grace period, got:\n%s", logs.String())
	}

	// Once the window elapses the same registry shape warns.
	pastGracePeriod(h)
	h.checkWriterRedundancy(writerRedundancyFailover)
	if !strings.Contains(logs.String(), "ARC_CLUSTER_ROLE=writer") {
		t.Errorf("expected a warning after the grace period, got:\n%s", logs.String())
	}
}

func TestWriterRedundancy_RateLimited(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, false)
	pastGracePeriod(h)
	registerWriters(t, registry, 1)

	for i := 0; i < 3; i++ {
		h.checkWriterRedundancy(writerRedundancyFailover)
	}

	if count := strings.Count(logs.String(), "ARC_CLUSTER_ROLE=writer"); count != 1 {
		t.Errorf("expected exactly 1 warning across 3 back-to-back checks, got %d. Logs:\n%s", count, logs.String())
	}
}

func TestWriterRedundancy_RecoveryResetsTheCooldown(t *testing.T) {
	// broken → fixed → broken must warn again immediately rather than sit out
	// the remainder of a stale cooldown.
	h, registry, logs := newTestHealthChecker(t, false)
	pastGracePeriod(h)
	registerWriters(t, registry, 2)

	h.checkWriterRedundancy(writerRedundancyFailover)
	if count := strings.Count(logs.String(), "ARC_CLUSTER_ROLE=writer"); count != 1 {
		t.Fatalf("expected the first warning, got %d. Logs:\n%s", count, logs.String())
	}

	// A third writer joins: quiet, and the cooldown is released.
	third := NewNode("writer-late", "writer-late", RoleWriter, "test-cluster")
	third.State = StateHealthy
	if err := registry.Register(third); err != nil {
		t.Fatalf("registry.Register: %v", err)
	}
	h.checkWriterRedundancy(writerRedundancyFailover)
	if count := strings.Count(logs.String(), "ARC_CLUSTER_ROLE=writer"); count != 1 {
		t.Fatalf("a healthy three-writer cluster must be silent, got %d warnings. Logs:\n%s", count, logs.String())
	}
	if h.lastWriterRedundancyWarnAt.Load() != 0 {
		t.Error("recovery should clear the warn timestamp so the next regression warns immediately")
	}

	// It leaves again.
	registry.Unregister("writer-late")
	h.checkWriterRedundancy(writerRedundancyFailover)
	if count := strings.Count(logs.String(), "ARC_CLUSTER_ROLE=writer"); count != 2 {
		t.Errorf("expected an immediate second warning after the cluster regressed, got %d. Logs:\n%s", count, logs.String())
	}
}

// The count is by role, not by health: a topology warning that vanished the
// moment a writer went unhealthy would be silent exactly when it matters, and
// an unhealthy writer already has its own logging.
func TestWriterRedundancy_CountsByRoleNotHealth(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, false)
	pastGracePeriod(h)
	registerWriters(t, registry, 3)

	for _, id := range []string{"writer-0", "writer-1"} {
		node, ok := registry.Get(id)
		if !ok {
			t.Fatalf("registry.Get %s: not found", id)
		}
		registry.UpdateNodeState(node.ID, StateDead)
	}

	h.checkWriterRedundancy(writerRedundancyFailover)

	if logs.Len() != 0 {
		t.Errorf("three writer-role nodes is the right topology even when two are dead; expected silence, got:\n%s", logs.String())
	}
}

// A single-node install has no redundancy of any kind and its operator knows
// it. The topology this warning exists for is the one that looks highly
// available: several nodes, only one of which is a writer.
func TestWriterRedundancy_SilentOnASingleNodeCluster(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf).With().Timestamp().Logger()

	local := NewNode("solo", "solo", RoleWriter, "test-cluster")
	local.State = StateHealthy
	registry := NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	h := NewHealthChecker(&HealthCheckerConfig{Registry: registry, Logger: logger})
	pastGracePeriod(h)

	h.checkWriterRedundancy(writerRedundancyFailover)
	if buf.Len() != 0 {
		t.Fatalf("a one-node cluster should be silent, got:\n%s", buf.String())
	}

	// A second node joins: now the cluster looks like a cluster, and one
	// writer out of two nodes is exactly the shape #856 is about.
	reader := NewNode("reader-1", "reader-1", RoleReader, "test-cluster")
	reader.State = StateHealthy
	if err := registry.Register(reader); err != nil {
		t.Fatalf("registry.Register: %v", err)
	}
	h.checkWriterRedundancy(writerRedundancyFailover)
	if !strings.Contains(buf.String(), "ARC_CLUSTER_ROLE=writer") {
		t.Errorf("expected a warning once the cluster has more than one node, got:\n%s", buf.String())
	}
}

// The zero value must be off, so OSS and any cluster mode the coordinator does
// not explicitly arm never runs the check.
func TestWriterRedundancy_DisabledByDefault(t *testing.T) {
	h, _, _ := newTestHealthChecker(t, false)
	if mode := writerRedundancyMode(h.writerRedundancy.Load()); mode != writerRedundancyOff {
		t.Fatalf("a freshly constructed checker should be off, got mode %d", mode)
	}

	h.EnableWriterRedundancyWarning(writerRedundancyLoadBalanced)
	if mode := writerRedundancyMode(h.writerRedundancy.Load()); mode != writerRedundancyLoadBalanced {
		t.Fatalf("arming did not take, got mode %d", mode)
	}
}
