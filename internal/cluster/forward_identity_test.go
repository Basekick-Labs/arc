package cluster

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

const fwdCluster = "fwd-cluster"
const fwdSecret = "test-cluster-secret-32-bytes-long!"

// seen is what the receiving handler observed of one forwarded request.
type seen struct {
	tokenID                       int64
	authorization, apiKey, cookie string
	rawQuery                      string
	headers                       http.Header
}

type fwdRig struct {
	t        *testing.T
	coord    *Coordinator // the receiver, writer-W
	router   *Router      // the forwarder, reader-A
	target   *Node        // writer-W as the router sees it
	am       *auth.AuthManager
	writeTok string
	writeID  int64
	last     *seen
	url      string
}

// newFwdRig starts a one-node Raft whose FSM knows reader-A (forwarder),
// writer-W (receiver, capable) and writer-old (an older build), a receiver
// with the auth middleware on a real listener, and a router on reader-A.
func newFwdRig(t *testing.T) *fwdRig {
	t.Helper()
	rn := startRaftNodeInDir(t, "writer-W", freeTCPAddr(t), t.TempDir(), true)
	t.Cleanup(func() { _ = rn.Stop() })
	waitLeader(t, rn)
	for _, n := range []*raft.NodeInfo{
		{ID: "reader-A", Role: string(RoleReader), ClusterName: fwdCluster, Capabilities: []string{CapabilityForwardIdentity}},
		{ID: "writer-W", Role: string(RoleWriter), ClusterName: fwdCluster, Capabilities: []string{CapabilityForwardIdentity}},
		{ID: "writer-old", Role: string(RoleWriter), ClusterName: fwdCluster},
	} {
		if err := rn.AddNode(n, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	local := NewNode("writer-W", "writer-W", RoleWriter, fwdCluster)
	coord := &Coordinator{
		cfg:           &config.ClusterConfig{ClusterName: fwdCluster, SharedSecret: fwdSecret},
		localNode:     local,
		raftNode:      rn,
		raftFSM:       rn.FSM(),
		logger:        zerolog.Nop(),
		ctx:           context.Background(),
		forwardNonces: security.NewForwardNonceCache(security.ForwardIdentityTolerance),
	}

	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = am.Close() })
	tok, err := am.CreateToken(context.Background(), "writer", "", "write", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := &fwdRig{t: t, coord: coord, am: am, writeTok: tok, writeID: am.VerifyToken(tok).ID}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(auth.NewMiddleware(auth.MiddlewareConfig{
		AuthManager: am,
		ForwardIdentity: func(c *fiber.Ctx) (bool, int64, error) {
			return coord.VerifyForwardIdentity(func(name string) []string {
				var out []string
				for _, v := range c.Request().Header.PeekAll(name) {
					out = append(out, string(v))
				}
				return out
			}, c.Method(), string(c.Request().RequestURI()))
		},
	}))
	record := func(c *fiber.Ctx) error {
		s := &seen{
			authorization: c.Get("Authorization"),
			apiKey:        c.Get("X-Api-Key"),
			cookie:        c.Get("Cookie"),
			rawQuery:      string(c.Request().URI().QueryString()),
			headers:       http.Header{},
		}
		if info := auth.GetTokenInfo(c); info != nil {
			s.tokenID = info.ID
		}
		c.Request().Header.VisitAll(func(k, v []byte) { s.headers.Add(string(k), string(v)) })
		r.last = s
		return c.SendStatus(fiber.StatusNoContent)
	}
	app.Post("/api/v1/write/msgpack", record)
	app.Post("/api/v1/query", record)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	r.url = "http://" + ln.Addr().String()

	reader := NewNode("reader-A", "reader-A", RoleReader, fwdCluster)
	r.router = NewRouter(&RouterConfig{
		Timeout:             5 * time.Second,
		LocalNode:           reader,
		Registry:            NewRegistry(&RegistryConfig{LocalNode: reader, MaxNodes: 8, Logger: zerolog.Nop()}),
		Logger:              zerolog.Nop(),
		SharedSecret:        fwdSecret,
		ClusterName:         fwdCluster,
		PeerForwardIdentity: coord.peerForwardIdentity,
	})
	r.target = NewNode("writer-W", "writer-W", RoleWriter, fwdCluster)
	r.target.APIAddress = ln.Addr().String()
	r.coord.cfg.ClusterName = fwdCluster
	return r
}

func waitLeader(t *testing.T, rn *raft.Node) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !rn.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// clientRequest is a write as a client sends it to reader-A, every
// credential carrier filled in, plus a spoofed assertion header.
func (r *fwdRig) clientRequest(tokenID int64) *http.Request {
	req, _ := http.NewRequest("POST", "http://reader-a/api/v1/write/msgpack?db=a&p="+r.writeTok+"&z=1&u=me&b=2", bytes.NewReader([]byte("body")))
	req.Header.Set("Authorization", "Bearer "+r.writeTok)
	req.Header.Set("X-Api-Key", r.writeTok)
	req.Header.Set("Cookie", "session="+r.writeTok)
	req.Header.Set("X-Arc-Database", "a")
	req.Header.Set("Content-Type", "application/msgpack")
	req.Header.Set(security.HeaderForwardTokenID, "1")
	return req.WithContext(WithForwardTokenID(context.Background(), tokenID))
}

// A forward to a capable peer carries the signed identity and no credential:
// the peer authenticates it as the caller's token, and never sees the token.
func TestForwardIdentity_PeerSeesIdentityNotCredential(t *testing.T) {
	r := newFwdRig(t)
	resp, err := r.router.ForwardWriteTo(context.Background(), r.target, r.clientRequest(r.writeID))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 || r.last == nil {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if r.last.tokenID != r.writeID {
		t.Fatalf("authenticated as %d, want %d", r.last.tokenID, r.writeID)
	}
	if r.last.authorization != "" || r.last.apiKey != "" || r.last.cookie != "" {
		t.Fatalf("credential reached the peer: %+v", r.last)
	}
	if r.last.rawQuery != "db=a&z=1&b=2" {
		t.Fatalf("query %q: credential parameters must go, the rest keep their order", r.last.rawQuery)
	}
	for k, v := range r.last.headers {
		if strings.Contains(strings.Join(v, ","), r.writeTok) {
			t.Fatalf("token in header %s", k)
		}
	}
}

// A peer that predates the identity still gets the credential, so a rolling
// upgrade keeps forwarding; it gets no assertion headers either.
func TestForwardIdentity_OlderPeerGetsCredential(t *testing.T) {
	r := newFwdRig(t)
	req := r.clientRequest(r.writeID)
	fwd, _ := http.NewRequest("POST", "http://writer-old/api/v1/write/msgpack?"+req.URL.RawQuery, nil)
	fwd.Header = req.Header.Clone()
	if err := r.router.signForward(fwd, &Node{ID: "writer-old"}, r.writeID); err != nil {
		t.Fatal(err)
	}
	if fwd.Header.Get("Authorization") == "" || !strings.Contains(fwd.URL.RawQuery, "p=") {
		t.Fatal("an older peer must still receive the credential")
	}
	for k := range fwd.Header {
		if strings.HasPrefix(k, security.ForwardIdentityHeaderPfx) {
			t.Fatalf("assertion header %s sent to an older peer (or a client copy kept)", k)
		}
	}
}

// Each attempt is signed afresh: retries never reuse a nonce the peer has
// already seen.
func TestForwardIdentity_EachAttemptSignedAfresh(t *testing.T) {
	r := newFwdRig(t)
	nonces := map[string]bool{}
	for i := 0; i < 3; i++ {
		fwd, _ := http.NewRequest("POST", r.url+"/api/v1/write/msgpack", nil)
		if err := r.router.signForward(fwd, r.target, r.writeID); err != nil {
			t.Fatal(err)
		}
		nonces[fwd.Header.Get(security.HeaderForwardNonce)] = true
	}
	if len(nonces) != 3 {
		t.Fatalf("nonces reused: %v", nonces)
	}
}

// signed returns a request to the receiver signed as from with the given
// identity fields, ready to send.
func (r *fwdRig) signed(from, target string, tokenID int64, mutate func(*http.Request)) *http.Request {
	req, _ := http.NewRequest("POST", r.url+"/api/v1/write/msgpack?db=a", bytes.NewReader([]byte("x")))
	req.Header.Set("X-Arc-Database", "a")
	rt := NewRouter(&RouterConfig{LocalNode: NewNode(from, from, RoleReader, fwdCluster), SharedSecret: fwdSecret, ClusterName: fwdCluster,
		PeerForwardIdentity: func(string) bool { return true }, Registry: NewRegistry(&RegistryConfig{MaxNodes: 8, Logger: zerolog.Nop()}), Logger: zerolog.Nop()})
	if err := rt.signForward(req, &Node{ID: target}, tokenID); err != nil {
		r.t.Fatal(err)
	}
	if mutate != nil {
		mutate(req)
	}
	return req
}

func send(t *testing.T, req *http.Request) int {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// Every way an assertion can be wrong is a 401, with no fallback to a
// credential sent alongside it.
func TestForwardIdentity_Rejections(t *testing.T) {
	r := newFwdRig(t)
	other, err := r.am.CreateToken(context.Background(), "off", "", "write", nil)
	if err != nil {
		t.Fatal(err)
	}
	offID := r.am.VerifyToken(other).ID
	if err := r.am.RevokeToken(context.Background(), offID); err != nil {
		t.Fatal(err)
	}
	if code := send(t, r.signed("reader-A", "writer-W", r.writeID, nil)); code != 204 {
		t.Fatalf("baseline: %d", code)
	}
	cases := map[string]*http.Request{
		"database changed":  r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) { q.Header.Set("X-Arc-Database", "b") }),
		"measurement added": r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) { q.Header.Set("X-Arc-Measurement", "m") }),
		"encoding added":    r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) { q.Header.Set("Content-Encoding", "gzip") }),
		"query changed":     r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) { q.URL.RawQuery = "db=b" }),
		"query reordered":   r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) { q.URL.RawQuery = "db=a&x=1" }),
		"token changed":     r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) { q.Header.Set(security.HeaderForwardTokenID, strconv.FormatInt(r.writeID+1, 10)) }),
		"duplicate mac": r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) {
			q.Header.Add(security.HeaderForwardHMAC, q.Header.Get(security.HeaderForwardHMAC))
		}),
		"missing nonce":   r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) { q.Header.Del(security.HeaderForwardNonce) }),
		"other target":    r.signed("reader-A", "writer-old", r.writeID, nil),
		"self as sender":  r.signed("writer-W", "writer-W", r.writeID, nil),
		"unknown sender":  r.signed("ghost", "writer-W", r.writeID, nil),
		"revoked token":   r.signed("reader-A", "writer-W", offID, nil),
		"unknown token":   r.signed("reader-A", "writer-W", 999999, nil),
		"no caller token": r.signed("reader-A", "writer-W", 0, nil),
		"wrong secret": r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) {
			q.Header.Set(security.HeaderForwardHMAC, strings.Repeat("ab", 32))
		}),
		"stale": r.signed("reader-A", "writer-W", r.writeID, func(q *http.Request) {
			q.Header.Set(security.HeaderForwardTimestamp, "1")
		}),
		"lone header with a valid credential": func() *http.Request {
			q, _ := http.NewRequest("POST", r.url+"/api/v1/write/msgpack", nil)
			q.Header.Set(security.HeaderForwardNode, "reader-A")
			q.Header.Set("Authorization", "Bearer "+r.writeTok)
			return q
		}(),
	}
	for name, req := range cases {
		if code := send(t, req); code != 401 {
			t.Errorf("%s: %d, want 401", name, code)
		}
	}
	// Replay of an accepted request.
	req := r.signed("reader-A", "writer-W", r.writeID, nil)
	h := req.Header.Clone()
	if code := send(t, req); code != 204 {
		t.Fatalf("first send: %d", code)
	}
	again, _ := http.NewRequest("POST", req.URL.String(), bytes.NewReader([]byte("x")))
	again.Header = h
	if code := send(t, again); code != 401 {
		t.Fatalf("replay: %d, want 401", code)
	}
	// Without any assertion header the ordinary credential still works.
	plain, _ := http.NewRequest("POST", r.url+"/api/v1/write/msgpack", nil)
	plain.Header.Set("Authorization", "Bearer "+r.writeTok)
	if code := send(t, plain); code != 204 {
		t.Fatalf("plain client request: %d", code)
	}
}

// A peer this node has no record of is sent the identity, not the
// credential: an older build answers 401 instead of receiving the token.
func TestForwardIdentity_UnknownPeerGetsIdentity(t *testing.T) {
	r := newFwdRig(t)
	if !r.coord.peerForwardIdentity("not-in-fsm") {
		t.Fatal("unknown peer must get the identity")
	}
	if r.coord.peerForwardIdentity("writer-old") {
		t.Fatal("a peer without the capability must get the credential")
	}
	noRaft := &Coordinator{cfg: &config.ClusterConfig{SharedSecret: fwdSecret}}
	if noRaft.peerForwardIdentity("writer-W") {
		t.Fatal("without Raft token IDs differ per node: forward the credential")
	}
}

// A forwarded query (the retrying path) is signed too.
func TestForwardIdentity_QueryForwardSigned(t *testing.T) {
	r := newFwdRig(t)
	req, _ := http.NewRequest("POST", "http://compactor/api/v1/query", bytes.NewReader([]byte(`{"sql":"SELECT 1"}`)))
	req.Header.Set("Authorization", "Bearer "+r.writeTok)
	req = req.WithContext(WithForwardTokenID(context.Background(), r.writeID))
	resp, err := r.router.forwardRequest(context.Background(), r.target, req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 || r.last.tokenID != r.writeID || r.last.authorization != "" {
		t.Fatalf("status %d seen %+v", resp.StatusCode, r.last)
	}
}

// An expired token and a forward from a node without auth are refused, the
// latter saying why.
func TestForwardIdentity_ExpiredTokenAndNoCaller(t *testing.T) {
	r := newFwdRig(t)
	past := time.Now().Add(-time.Hour)
	if err := r.am.UpdateToken(context.Background(), r.writeID, nil, nil, nil, &past); err != nil {
		t.Fatal(err)
	}
	if code := send(t, r.signed("reader-A", "writer-W", r.writeID, nil)); code != 401 {
		t.Fatalf("expired token: %d", code)
	}
	resp, err := http.DefaultClient.Do(r.signed("reader-A", "writer-W", 0, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 401 || !strings.Contains(string(body), "authentication is disabled on the forwarding node") {
		t.Fatalf("no caller: %d %s", resp.StatusCode, body)
	}
}

// Every build advertises that it authenticates forwarded requests by
// identity, so its peers stop sending it credentials.
func TestForwardIdentity_CapabilityAdvertised(t *testing.T) {
	c := &Coordinator{cfg: &config.ClusterConfig{}, localNode: NewNode("n", "n", RoleReader, "c"), logger: zerolog.Nop()}
	if !hasCapability(c.localCapabilities(), CapabilityForwardIdentity) {
		t.Fatalf("capabilities %v", c.localCapabilities())
	}
}
