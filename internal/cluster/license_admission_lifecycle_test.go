package cluster

import (
	"encoding/json"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/license"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func TestLicenseMissingGrantCannotAuthorizeOSSHandoff(t *testing.T) {
	c := &Coordinator{cfg: &config.ClusterConfig{}, lifecycleRecovery: true}
	c.lifecycle.started.Store(true)
	p := c.licensePhase()
	if p.Phase != license.PhaseAuthorityUnavailable || terminalLicensePhase(p) || c.ServingAllowed() || c.LicenseWritesAllowed() {
		t.Fatalf("missing grant authorized data service or terminal handoff: %+v", p)
	}
	for _, p := range []license.ExpiryState{
		{Phase: license.PhaseAdmissionBlocked, Reason: "installation_not_admitted"},
		{Phase: license.PhaseAuthorityUnavailable},
		{Phase: license.PhaseOSS},
		{Phase: license.PhaseOSS, Reason: "unknown_server_error"},
	} {
		if terminalLicensePhase(p) {
			t.Fatalf("unconfirmed cause authorized terminal transition: %+v", p)
		}
	}
	for _, reason := range []string{"signed_cutoff", "key_rejected", "clustering_not_licensed"} {
		if !terminalLicensePhase(license.ExpiryState{Phase: license.PhaseOSS, Reason: reason}) {
			t.Fatalf("confirmed terminal cause lost: %s", reason)
		}
	}
}

func TestLicensePreparationCannotBypassAdmissionAndUnknownAuthority(t *testing.T) {
	fsm := raft.NewClusterFSM(zerolog.Nop())
	apply := func(kind raft.CommandType, payload any) {
		t.Helper()
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(raft.Command{Type: kind, Payload: raw})
		if err != nil {
			t.Fatal(err)
		}
		if result := fsm.Apply(&hraft.Log{Data: data}); result != nil {
			t.Fatal(result)
		}
	}
	apply(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: "w1", Role: "writer", CoreCount: 1}})
	apply(raft.CommandLicenseLifecycle, raft.LicenseLifecycleState{Generation: 1, Mode: "preparing", Controller: "w1", Owner: "w1", Roles: map[string]string{"writer": "w1"}, Participants: []string{"w1"}})
	c := &Coordinator{cfg: &config.ClusterConfig{}, localNode: NewNode("w1", "w1", RoleWriter, "test"), raftFSM: fsm, lifecycleRecovery: true}
	c.lifecycle.started.Store(true)
	c.lifecycle.ready.Store(true)
	for _, p := range []license.ExpiryState{
		{Phase: license.PhaseAdmissionBlocked, Reason: "installation_not_admitted"},
		{Phase: license.PhaseAuthorityUnavailable},
		{Phase: license.PhaseOSS},
	} {
		c.lifecycle.phase = func() license.ExpiryState { return p }
		if c.ServingAllowed() || c.ReadServingAllowed() || c.LicenseWritesAllowed() {
			t.Fatalf("preparing bypassed refusal: %+v", p)
		}
		if got := fsm.LicenseLifecycle(); got.Generation != 1 || got.Mode != "preparing" {
			t.Fatal("gate read changed durable lifecycle state")
		}
	}
	c.lifecycle.phase = func() license.ExpiryState {
		return license.ExpiryState{Phase: license.PhaseOSS, Reason: "signed_cutoff"}
	}
	if !c.ReadServingAllowed() || c.ServingAllowed() || c.LicenseWritesAllowed() {
		t.Fatal("confirmed cutoff lost read-only inventory access or enabled writes")
	}
}
