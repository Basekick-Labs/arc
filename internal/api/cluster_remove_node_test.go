package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1619: the status mapping of DELETE /api/v1/cluster/nodes/:id. Membership
// itself is tested against a real Raft node in internal/cluster
// (remove_node_unknown_test.go); these pin what the handler answers for each
// coordinator outcome.

type fakeNodeRemover struct {
	local   *cluster.Node
	err     error
	removed []string
}

func (f *fakeNodeRemover) GetLocalNode() *cluster.Node { return f.local }

func (f *fakeNodeRemover) RemoveNodeViaRaft(id string) error {
	f.removed = append(f.removed, id)
	return f.err
}

func removeNode(t *testing.T, r *fakeNodeRemover, id string) (int, map[string]any) {
	t.Helper()
	h := NewClusterHandler(nil, nil, nil, zerolog.Nop())
	h.nodeRemover = r
	app := fiber.New()
	h.RegisterRoutes(app)
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/cluster/nodes/"+id, nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return resp.StatusCode, body
}

func TestRemoveNodeUnknownIsNotFound(t *testing.T) {
	r := &fakeNodeRemover{local: cluster.NewNode("node-1", "node-1", cluster.RoleWriter, "c"),
		err: fmt.Errorf("%w: %q", cluster.ErrNodeNotFound, "ghost")}
	code, body := removeNode(t, r, "ghost")
	if code != fiber.StatusNotFound {
		t.Fatalf("status %d, want 404 (body %v)", code, body)
	}
	msg, _ := body["error"].(string)
	if msg != "node ghost is not a member of the cluster" {
		t.Errorf("error %q", msg)
	}
	if strings.Contains(msg, "not the leader") {
		t.Errorf("the 404 text %q contains \"not the leader\"; arcli would retry it on the leader", msg)
	}
	if ok, _ := body["success"].(bool); ok {
		t.Error("success=true on a 404")
	}
}

// arcli retries on the leader by matching "not the leader" in the error text;
// the status and text stay as they were.
func TestRemoveNodeOnNonLeaderKeepsTheNotLeaderAnswer(t *testing.T) {
	r := &fakeNodeRemover{err: errors.New("not the leader")}
	code, body := removeNode(t, r, "node-2")
	if code != fiber.StatusInternalServerError {
		t.Errorf("status %d, want 500", code)
	}
	if msg, _ := body["error"].(string); !strings.Contains(msg, "not the leader") {
		t.Errorf("error %q does not say \"not the leader\"", msg)
	}
}

func TestRemoveNodeSelfIsBadRequest(t *testing.T) {
	r := &fakeNodeRemover{local: cluster.NewNode("node-1", "node-1", cluster.RoleWriter, "c")}
	code, body := removeNode(t, r, "node-1")
	if code != fiber.StatusBadRequest {
		t.Errorf("status %d, want 400 (body %v)", code, body)
	}
	if len(r.removed) != 0 {
		t.Errorf("self-removal reached the coordinator: %v", r.removed)
	}
}

func TestRemoveNodeMemberSucceeds(t *testing.T) {
	r := &fakeNodeRemover{local: cluster.NewNode("node-1", "node-1", cluster.RoleWriter, "c")}
	code, body := removeNode(t, r, "node-2")
	if code != fiber.StatusOK {
		t.Errorf("status %d, want 200 (body %v)", code, body)
	}
	if len(r.removed) != 1 || r.removed[0] != "node-2" {
		t.Errorf("removed %v, want [node-2]", r.removed)
	}
}
