package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
)

// #1630: on a cluster node, the token routes for a token the cluster does not
// manage (created on this node before v26.06.1) answered 200 and changed
// nothing. These tests pin that a 200 now means the change took effect here.

func doTokenJSON(t *testing.T, app *fiber.App, method, path, body string) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestClusterRevokeOfNodeLocalTokenTakesEffect fails on main: there the
// revoke answers 200 and the token keeps authenticating. It uses only what
// main has, and follows the node_local_token answer the way an operator does.
func TestClusterRevokeOfNodeLocalTokenTakesEffect(t *testing.T) {
	app, am := setupTokenStatusTest(t)
	// Created before the node runs with its cluster, like a pre-v26.06.1
	// token: the row exists only in this node's database.
	secret, err := am.CreateToken(context.Background(), "leaked", "", "read", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	id := tokenIDNamed(t, am, "leaked")
	if am.VerifyToken(secret) == nil {
		t.Fatal("the token does not authenticate before the revoke")
	}
	am.SetRaftProposer(&droppingTokenProposer{})

	path := fmt.Sprintf("/tokens/%d/revoke", id)
	status, body := doTokenJSON(t, app, "POST", path, "")
	if status == http.StatusConflict && body["code"] == "node_local_token" {
		node, _ := body["node_id"].(string)
		if node == "" {
			node = "this"
		}
		status, body = doTokenJSON(t, app, "POST", path+"?node="+node, "")
	}
	if status != http.StatusOK {
		t.Fatalf("revoke: %d %v", status, body)
	}
	if am.VerifyToken(secret) != nil {
		t.Fatal("revoke answered 200 but the token still authenticates")
	}
}

// viewTokenProposer is droppingTokenProposer with the cluster state's read
// side: an FSM that holds the given tokens, on node "n2".
type viewTokenProposer struct {
	droppingTokenProposer
	tokens map[int64]auth.ClusterTokenEntry
}

func (p *viewTokenProposer) ClusterToken(id int64) (auth.ClusterTokenEntry, bool) {
	e, ok := p.tokens[id]
	return e, ok
}

func (p *viewTokenProposer) ClusterTokenIDByName(name string) (int64, bool) {
	for id, e := range p.tokens {
		if e.Name == name {
			return id, true
		}
	}
	return 0, false
}

func (p *viewTokenProposer) AppliedDone() uint64 { return 1 << 40 }
func (p *viewTokenProposer) LocalNodeID() string { return "n2" }

func TestNodeLocalTokenRouteBodies(t *testing.T) {
	app, am := setupTokenStatusTest(t)
	ctx := context.Background()
	secret, err := am.CreateToken(ctx, "legacy", "", "read", nil)
	if err != nil {
		t.Fatal(err)
	}
	id := tokenIDNamed(t, am, "legacy")
	const clusterID = 900
	if err := am.ApplyCreateToken(auth.ClusterTokenEntry{ID: clusterID, Name: "svc", Permissions: "read",
		TokenHash: "$2a$10$svcfakebcrypthash", TokenPrefix: "svcprefix", CreatedAtUnixNano: 1_700_000_000_000_000_000, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	prop := &viewTokenProposer{tokens: map[int64]auth.ClusterTokenEntry{
		clusterID: {ID: clusterID, Name: "svc", TokenHash: "$2a$10$svcfakebcrypthash", CreatedAtUnixNano: 1_700_000_000_000_000_000},
	}}
	am.SetRaftProposer(prop)
	legacy := fmt.Sprintf("/tokens/%d", id)
	cluster := fmt.Sprintf("/tokens/%d", clusterID)

	for _, tc := range []struct {
		method, path, body string
		status             int
		code               string
	}{
		{"POST", legacy + "/revoke", "", 409, "node_local_token"},
		{"POST", legacy + "/rotate", "", 409, "node_local_token"},
		{"PATCH", legacy, `{"description":"x"}`, 409, "node_local_token"},
		{"DELETE", legacy, "", 409, "node_local_token"},
		{"POST", legacy + "/revoke?node=n1", "", 409, "wrong_node"},
		{"POST", "/tokens/4242/revoke?node=n1", "", 409, "wrong_node"},
		{"POST", legacy + "/revoke?node=", "", 400, "invalid_node"},
		{"POST", cluster + "/revoke?node=n2", "", 409, "cluster_token"},
	} {
		status, body := doTokenJSON(t, app, tc.method, tc.path, tc.body)
		if status != tc.status || body["code"] != tc.code || body["success"] != false {
			t.Errorf("%s %s: %d %v, want %d code %s", tc.method, tc.path, status, body, tc.status, tc.code)
		}
		if tc.code == "node_local_token" && (body["node_id"] != "n2" || !strings.Contains(fmt.Sprint(body["error"]), "?node=n2")) {
			t.Errorf("%s %s: the refusal does not name the node: %v", tc.method, tc.path, body)
		}
	}
	if am.VerifyToken(secret) == nil {
		t.Fatal("a refused request changed the token")
	}
	if n := prop.proposals.Load(); n != 0 {
		t.Fatalf("%d proposals from refused requests", n)
	}

	// The list and the get mark the token.
	if _, body := doTokenJSON(t, app, "GET", legacy, ""); body["token"].(map[string]any)["node_local"] != true {
		t.Errorf("get: %v, want node_local", body)
	}

	// With ?node of this node: done, the shipped message kept, scope added.
	status, body := doTokenJSON(t, app, "POST", legacy+"/revoke?node=n2", "")
	if status != 200 || body["message"] != "Token revoked successfully" || body["scope"] != "node" || body["node_id"] != "n2" {
		t.Fatalf("revoke: %d %v", status, body)
	}
	tok, _ := body["token"].(map[string]any)
	if tok["name"] != "legacy" || tok["fingerprint"] == "" || !strings.Contains(fmt.Sprint(body["warning"]), "node n2 only") {
		t.Errorf("revoke body does not name what changed: %v", body)
	}
	if am.VerifyToken(secret) != nil {
		t.Error("still authenticates")
	}

	// A cluster token's body is as before: no scope fields.
	status, body = doTokenJSON(t, app, "POST", cluster+"/revoke", "")
	if status != 200 || len(body) != 2 || body["message"] != "Token revoked successfully" {
		t.Errorf("cluster revoke body changed: %d %v", status, body)
	}
}

// Standalone: ?node is ignored and the bodies are as before.
func TestStandaloneTokenRoutesIgnoreNode(t *testing.T) {
	app, am := setupTokenStatusTest(t)
	if _, err := am.CreateToken(context.Background(), "solo", "", "read", nil); err != nil {
		t.Fatal(err)
	}
	id := tokenIDNamed(t, am, "solo")
	status, body := doTokenJSON(t, app, "POST", fmt.Sprintf("/tokens/%d/revoke?node=", id), "")
	if status != 200 || len(body) != 2 {
		t.Errorf("standalone revoke with empty node: %d %v", status, body)
	}
	if _, body := doTokenJSON(t, app, "GET", fmt.Sprintf("/tokens/%d", id), ""); body["token"].(map[string]any)["node_local"] != nil {
		t.Errorf("standalone get marks node_local: %v", body)
	}
}
