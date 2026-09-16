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

// registerPeer adds one healthy reader so the registry is a cluster rather
// than a single-node install, which is suppressed.
func registerPeer(t *testing.T, registry *Registry, id string) {
	t.Helper()
	node := NewNode(id, id, RoleReader, "test-cluster")
	node.State = StateHealthy
	if err := registry.Register(node); err != nil {
		t.Fatalf("registry.Register %s: %v", id, err)
	}
}

// sustainedDeficit backdates the deficit clock so the caller's next check is
// past writerRedundancySustainPeriod and may warn.
func sustainedDeficit(h *HealthChecker) {
	h.writerDeficitSince = time.Now().Add(-2 * writerRedundancySustainPeriod)
}

// The arming decision is the one thing the configuration matrix is about, so
// it is a pure function and this table is the matrix.
func TestWriterRedundancyModeFor(t *testing.T) {
	tests := []struct {
		sharedStorage  bool
		failoverActive bool
		want           writerRedundancyMode
	}{
		{false, true, writerRedundancyFailover},     // Pattern 1, failover built
		{false, false, writerRedundancyNoFailover},  // Pattern 1, flag off / no Raft / unlicensed
		{true, false, writerRedundancyLoadBalanced}, // Pattern 2, the normal case
		{true, true, writerRedundancyLoadBalanced},  // Pattern 2 wins: promotion is suppressed there
	}
	for _, tt := range tests {
		got := writerRedundancyModeFor(tt.sharedStorage, tt.failoverActive)
		if got != tt.want {
			t.Errorf("writerRedundancyModeFor(shared=%v, failover=%v) = %d, want %d",
				tt.sharedStorage, tt.failoverActive, got, tt.want)
		}
	}

	// Never off: a coordinator exists only in cluster mode, and every cluster
	// mode wants the check. Off is reserved for a checker nobody armed.
	for _, shared := range []bool{true, false} {
		for _, failover := range []bool{true, false} {
			if writerRedundancyModeFor(shared, failover) == writerRedundancyOff {
				t.Errorf("shared=%v failover=%v disarmed the check", shared, failover)
			}
		}
	}
}

func TestWriterRedundancy_WarnsBelowThreeInEveryMode(t *testing.T) {
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
		{"no-failover/one writer", writerRedundancyNoFailover, 1, true, "no automatic writer failover"},
		{"no-failover/two writers", writerRedundancyNoFailover, 2, true, "no automatic writer failover"},
		{"no-failover/three writers", writerRedundancyNoFailover, 3, false, ""},
		{"shared-storage/zero writers", writerRedundancyLoadBalanced, 0, true, "fewer than two"},
		{"shared-storage/one writer", writerRedundancyLoadBalanced, 1, true, "fewer than two"},
		{"shared-storage/two writers", writerRedundancyLoadBalanced, 2, true, "only two"},
		{"shared-storage/three writers", writerRedundancyLoadBalanced, 3, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, registry, logs := newTestHealthChecker(t, false)
			sustainedDeficit(h)
			registerPeer(t, registry, "reader-2")
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

// Each shape loses writer availability for a different reason, so an operator
// must not be told to wait for a promotion that will never be issued.
func TestWriterRedundancy_MessageIsModeSpecific(t *testing.T) {
	failover := writerRedundancyMessage(writerRedundancyFailover, 1, 1)
	noFailover := writerRedundancyMessage(writerRedundancyNoFailover, 1, 1)
	shared := writerRedundancyMessage(writerRedundancyLoadBalanced, 1, 1)

	if !strings.Contains(failover, "readers are never promotion candidates") {
		t.Errorf("Pattern 1 message should explain readers are not candidates, got: %s", failover)
	}
	if !strings.Contains(shared, "suppressed") {
		t.Errorf("Pattern 2 message should say promotion is suppressed, got: %s", shared)
	}
	if !strings.Contains(noFailover, "cluster.failover_enabled=true") {
		t.Errorf("the no-failover message should name the flag to set, got: %s", noFailover)
	}
	// This message used to warn that every writer-role node treats itself as
	// primary, which was true until #872 elected one regardless of the flag.
	// What it must say now is the opposite: a primary exists, and nothing will
	// replace it. Asserting the old wording is absent keeps the two from
	// drifting back together.
	if strings.Contains(noFailover, "treats itself as") {
		t.Errorf("the no-failover message still warns about multiple self-declared primaries, which #872 fixed: %s", noFailover)
	}
	if !strings.Contains(noFailover, "one primary is elected") {
		t.Errorf("the no-failover message should say a primary IS elected, got: %s", noFailover)
	}
	if !strings.Contains(noFailover, "nothing will choose a replacement") {
		t.Errorf("the no-failover message should say no replacement is coming, got: %s", noFailover)
	}
	if !strings.Contains(noFailover, "demote") {
		t.Errorf("the no-failover message should name the manual hand-over, got: %s", noFailover)
	}
	if failover == shared || failover == noFailover || shared == noFailover {
		t.Error("the three modes should not share one message")
	}
}

// The quorum sentence is conditional, and these pin both directions.
//
// It is appended only when the cluster's ingest-capable node count is thin,
// because those are the nodes that vote (#862). A two-writer cluster that also
// runs a standalone node has three voters and a real quorum margin, and telling
// its operator otherwise would be false.
//
// The previous version of this test asserted the messages never contain
// "quorum", because before #862 readers were voters and the claim was wrong in
// the other direction. Keeping it inverted rather than deleting it means a
// change that re-enfranchises a non-ingesting role has something to fail.
func TestWriterRedundancy_QuorumSentenceMatchesTheVoterCount(t *testing.T) {
	modes := []writerRedundancyMode{
		writerRedundancyFailover,
		writerRedundancyNoFailover,
		writerRedundancyLoadBalanced,
	}
	for _, mode := range modes {
		// Two writers and nothing else that ingests: two voters, no margin.
		thin := strings.ToLower(writerRedundancyMessage(mode, 2, 2))
		if !strings.Contains(thin, "quorum") {
			t.Errorf("mode=%d with %d voters should explain the quorum loss: %s", mode, 2, thin)
		}

		// Two writers plus a standalone node: three voters, quorum survives a
		// writer loss, so the sentence must not appear.
		withSpare := strings.ToLower(writerRedundancyMessage(mode, 2, writersForHA))
		if strings.Contains(withSpare, "quorum") {
			t.Errorf("mode=%d with %d voters must NOT claim a quorum loss: %s", mode, writersForHA, withSpare)
		}
	}
}

// The quorum sentence is only sound while the voter set is the ingest-capable
// set. This is the guard the previous version of the test above was reaching
// for and did not achieve: it compared VotesInElections against its own
// definition, which is true by construction.
func TestWriterRedundancy_QuorumClaimRestsOnTheVoterRule(t *testing.T) {
	for _, r := range AllRoles() {
		votes := r.VotesInElections()
		ingests := r.GetCapabilities().CanIngest
		if votes != ingests {
			t.Errorf("role %q votes=%v but ingests=%v; the quorum sentence assumes the two sets are identical", r, votes, ingests)
		}
	}
	// Spelled out so a change to the capabilities table, not just to
	// VotesInElections, fails here too.
	if RoleReader.VotesInElections() || RoleCompactor.VotesInElections() {
		t.Error("a non-ingesting role votes; the quorum sentence no longer follows from the writer count")
	}
	if !RoleWriter.VotesInElections() || !RoleStandalone.VotesInElections() {
		t.Error("an ingesting role does not vote; a cluster could be left unable to elect")
	}
}

// A deficit that clears before the sustain window never warns. This is the
// rolling-upgrade case: a leaving node broadcasts its departure and peers
// unregister it, so cycling one writer pod at a time walks a correct
// three-writer cluster through two writers on every node, every time.
func TestWriterRedundancy_SilentWhileTheDeficitIsBrief(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, false)
	registerPeer(t, registry, "reader-2")
	registerWriters(t, registry, 3)

	// A writer leaves for an upgrade. Several ticks pass, all inside the
	// sustain window.
	registry.Unregister("writer-2")
	for i := 0; i < 5; i++ {
		h.checkWriterRedundancy(writerRedundancyFailover)
	}
	if logs.Len() != 0 {
		t.Fatalf("expected silence during a brief deficit, got:\n%s", logs.String())
	}
	if h.writerDeficitSince.IsZero() {
		t.Error("the deficit clock should be running while the cluster is short")
	}

	// It comes back. The clock resets, so the next cycle gets its own window
	// rather than inheriting this one.
	registerWriters(t, registry, 3)
	h.checkWriterRedundancy(writerRedundancyFailover)
	if !h.writerDeficitSince.IsZero() {
		t.Error("recovery should clear the deficit clock")
	}
	if logs.Len() != 0 {
		t.Errorf("a recovered cluster must be silent, got:\n%s", logs.String())
	}
}

// A deficit that persists past the window does warn.
func TestWriterRedundancy_WarnsOnceTheDeficitPersists(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, false)
	registerPeer(t, registry, "reader-2")
	registerWriters(t, registry, 1)

	h.checkWriterRedundancy(writerRedundancyFailover)
	if logs.Len() != 0 {
		t.Fatalf("the first deficient tick only starts the clock, got:\n%s", logs.String())
	}

	// Backdate the clock rather than sleeping two minutes.
	h.writerDeficitSince = time.Now().Add(-writerRedundancySustainPeriod - time.Second)
	h.checkWriterRedundancy(writerRedundancyFailover)
	if !strings.Contains(logs.String(), "ARC_CLUSTER_ROLE=writer") {
		t.Errorf("expected a warning once the deficit outlived the window, got:\n%s", logs.String())
	}
}

func TestWriterRedundancy_RateLimited(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, false)
	sustainedDeficit(h)
	registerPeer(t, registry, "reader-2")
	registerWriters(t, registry, 1)

	for i := 0; i < 3; i++ {
		h.checkWriterRedundancy(writerRedundancyFailover)
	}

	if count := strings.Count(logs.String(), "ARC_CLUSTER_ROLE=writer"); count != 1 {
		t.Errorf("expected exactly 1 warning across 3 back-to-back checks, got %d. Logs:\n%s", count, logs.String())
	}
}

func TestWriterRedundancy_RecoveryReleasesTheCooldown(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, false)
	sustainedDeficit(h)
	registerPeer(t, registry, "reader-2")
	registerWriters(t, registry, 2)

	h.checkWriterRedundancy(writerRedundancyFailover)
	if count := strings.Count(logs.String(), "ARC_CLUSTER_ROLE=writer"); count != 1 {
		t.Fatalf("expected the first warning, got %d. Logs:\n%s", count, logs.String())
	}
	if h.lastWriterRedundancyWarnAt.Load() == 0 {
		t.Fatal("warning should have armed the cooldown")
	}

	// A third writer joins: quiet, and the cooldown is released so a later
	// regression is not muted by a stale timestamp.
	registerPeer(t, registry, "unused") // keep Count() honest if writers move
	registerWriters(t, registry, 3)
	h.checkWriterRedundancy(writerRedundancyFailover)
	if count := strings.Count(logs.String(), "ARC_CLUSTER_ROLE=writer"); count != 1 {
		t.Fatalf("a healthy three-writer cluster must be silent, got %d warnings. Logs:\n%s", count, logs.String())
	}
	if h.lastWriterRedundancyWarnAt.Load() != 0 {
		t.Error("recovery should clear the warn timestamp")
	}
}

// The count is by role, not by health: a topology warning that vanished the
// moment a writer went unhealthy would be silent exactly when it matters, and
// an unhealthy writer already has its own logging.
func TestWriterRedundancy_CountsByRoleNotHealth(t *testing.T) {
	h, registry, logs := newTestHealthChecker(t, false)
	sustainedDeficit(h)
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
	sustainedDeficit(h)

	h.checkWriterRedundancy(writerRedundancyFailover)
	if buf.Len() != 0 {
		t.Fatalf("a one-node cluster should be silent, got:\n%s", buf.String())
	}
	if !h.writerDeficitSince.IsZero() {
		t.Error("a suppressed single-node cluster should not accrue a deficit")
	}

	// A second node joins: now the cluster looks like a cluster, and one
	// writer out of two nodes is exactly the shape #856 is about.
	registerPeer(t, registry, "reader-1")
	sustainedDeficit(h)
	h.checkWriterRedundancy(writerRedundancyFailover)
	if !strings.Contains(buf.String(), "ARC_CLUSTER_ROLE=writer") {
		t.Errorf("expected a warning once the cluster has more than one node, got:\n%s", buf.String())
	}
}

// The zero value must be off, so OSS and any checker the coordinator does not
// arm never runs the check.
func TestWriterRedundancy_DisabledByDefault(t *testing.T) {
	h, _, _ := newTestHealthChecker(t, false)
	if mode := writerRedundancyMode(h.writerRedundancy.Load()); mode != writerRedundancyOff {
		t.Fatalf("a freshly constructed checker should be off, got mode %d", mode)
	}

	h.enableWriterRedundancyWarning(writerRedundancyLoadBalanced)
	if mode := writerRedundancyMode(h.writerRedundancy.Load()); mode != writerRedundancyLoadBalanced {
		t.Fatalf("arming did not take, got mode %d", mode)
	}
}
