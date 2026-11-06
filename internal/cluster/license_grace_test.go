package cluster

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/rs/zerolog"
)

func TestGraceWritersStopWithoutStoppingReadsOrCoordination(t *testing.T) {
	for _, role := range []NodeRole{RoleWriter, RoleStandalone} {
		c := &Coordinator{cfg: &config.ClusterConfig{SharedStorageMode: true}, localNode: NewNode("writer", "writer", role, "test"), raftFSM: raft.NewClusterFSM(zerolog.Nop())}
		c.lifecycle.started.Store(true)
		state := license.ExpiryState{Valid: true, Phase: license.PhaseGraceRestarts, GraceDay: 6}
		c.lifecycle.phase = func() license.ExpiryState { return state }
		for day := 6; day <= 14; day++ {
			state.GraceDay, state.WritersStopped = day, day >= 7
			if !c.ServingAllowed() || !c.ReadServingAllowed() {
				t.Errorf("day %d: reads/coordination stopped", day)
			}
			if c.MayAcceptIngest() != (day < 7) || errors.Is(c.ingestRefusal(), ingest.ErrIngestFenced) != (day >= 7) {
				t.Errorf("day %d: live ingest did not respect writer stop", day)
			}
		}
		state = license.ExpiryState{Valid: true, Phase: license.PhaseActive}
		if !c.MayAcceptIngest() || c.ingestRefusal() != nil {
			t.Error("replacement license did not reopen ingestion")
		}
	}
}

func TestGraceBlocksAllWriteForwardingButKeepsLocalQueries(t *testing.T) {
	r := NewRouter(&RouterConfig{LocalNode: NewNode("writer", "writer", RoleStandalone, "test"), WriteAllowed: func() bool { return false }, Logger: zerolog.Nop()})
	req, _ := http.NewRequest(http.MethodPost, "http://example.invalid/api/v1/write", nil)
	for _, forward := range []func(context.Context, *http.Request) (*http.Response, error){r.RouteWrite, r.RouteImport} {
		if _, err := forward(context.Background(), req); !errors.Is(err, ErrWriteForwardUnavailable) {
			t.Fatalf("write route bypassed stop: %v", err)
		}
	}
	for _, forward := range []func(context.Context, *Node, *http.Request) (*http.Response, error){r.ForwardWriteTo, r.ForwardImportTo} {
		if _, err := forward(context.Background(), nil, req); !errors.Is(err, ErrWriteForwardUnavailable) {
			t.Fatalf("direct forwarding bypassed stop: %v", err)
		}
	}
	if _, err := r.RouteQuery(context.Background(), req); !errors.Is(err, ErrLocalNodeCanHandle) {
		t.Fatalf("writer stop disabled reads: %v", err)
	}
}
