package cluster

// #1270: writes are forwarded once (never retried after being sent), to the
// designated primary; a standby writer forwards only when it can trust its
// view of the designation.

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

func writerNode(id, addr string) *Node {
	n := NewNode(id, id, RoleWriter, "c")
	n.UpdateState(StateHealthy)
	n.SetAddresses("", addr)
	return n
}

func writeReq(t *testing.T) *http.Request {
	t.Helper()
	r, err := http.NewRequest("POST", "http://x/api/v1/write/msgpack", strings.NewReader("body"))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestForwardWriteToIsSentOnce(t *testing.T) {
	var hits atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(204)
	}))
	defer slow.Close()
	reader := NewNode("r", "r", RoleReader, "c")
	r := NewRouter(&RouterConfig{Registry: NewRegistry(&RegistryConfig{Logger: zerolog.Nop()}), LocalNode: reader, Logger: zerolog.Nop(), WriteTimeout: 100 * time.Millisecond})
	_, err := r.ForwardWriteTo(context.Background(), writerNode("w", strings.TrimPrefix(slow.URL, "http://")), writeReq(t))
	if !errors.Is(err, ErrWriteForwardAmbiguous) {
		t.Fatalf("timeout after send: %v, want ErrWriteForwardAmbiguous", err)
	}
	time.Sleep(400 * time.Millisecond)
	if n := hits.Load(); n != 1 {
		t.Fatalf("the write reached the writer %d times; a forwarded write must never be retried once sent", n)
	}

	// Nothing listening: not reached, nothing written.
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := l.Addr().String()
	_ = l.Close()
	_, err = r.ForwardWriteTo(context.Background(), writerNode("w", dead), writeReq(t))
	if !errors.Is(err, ErrWriteForwardUnavailable) {
		t.Fatalf("refused connection: %v, want ErrWriteForwardUnavailable", err)
	}

	// A host that never answers the connection (dropped packets): nothing
	// was sent, so "unavailable", within the dial timeout, not the write
	// timeout. TEST-NET-1 is never routed.
	r2 := NewRouter(&RouterConfig{Registry: NewRegistry(&RegistryConfig{Logger: zerolog.Nop()}), LocalNode: reader, Logger: zerolog.Nop(), WriteTimeout: time.Minute})
	start := time.Now()
	_, err = r2.ForwardWriteTo(context.Background(), writerNode("w", "192.0.2.1:9"), writeReq(t))
	if !errors.Is(err, ErrWriteForwardUnavailable) {
		t.Fatalf("unanswered dial: %v, want ErrWriteForwardUnavailable (nothing was sent)", err)
	}
	if took := time.Since(start); took > writeForwardDialTimeout+3*time.Second {
		t.Fatalf("unanswered dial took %v", took)
	}
}

// The forwarded-writes counter tells the writer's refusals from successes.
func TestWritesForwardedResults(t *testing.T) {
	refuse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(IngestFencedResponseHeader, "1")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer refuse.Close()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer ok.Close()
	m := metrics.Get()
	r := NewRouter(&RouterConfig{Registry: NewRegistry(&RegistryConfig{Logger: zerolog.Nop()}), LocalNode: NewNode("r", "r", RoleReader, "c"), Logger: zerolog.Nop()})
	beforeOK, beforeRefused := m.WritesForwardedFor("ok"), m.WritesForwardedFor("refused")
	for _, srv := range []*httptest.Server{refuse, ok} {
		resp, err := r.ForwardWriteTo(context.Background(), writerNode("w", strings.TrimPrefix(srv.URL, "http://")), writeReq(t))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if m.WritesForwardedFor("ok")-beforeOK != 1 || m.WritesForwardedFor("refused")-beforeRefused != 1 {
		t.Fatalf("ok +%d refused +%d, want 1 and 1", m.WritesForwardedFor("ok")-beforeOK, m.WritesForwardedFor("refused")-beforeRefused)
	}
}

// A reader forwards to the designated primary even when its registry carries
// no writer state (a join can leave it empty, #1267), not round robin.
func TestRouteWritePrefersTheDesignatedPrimary(t *testing.T) {
	var gotA, gotB atomic.Int32
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { gotA.Add(1); w.WriteHeader(204) }))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { gotB.Add(1); w.WriteHeader(204) }))
	defer b.Close()
	reg := NewRegistry(&RegistryConfig{Logger: zerolog.Nop()})
	_ = reg.Register(writerNode("w-a", strings.TrimPrefix(a.URL, "http://")))
	_ = reg.Register(writerNode("w-b", strings.TrimPrefix(b.URL, "http://")))
	r := NewRouter(&RouterConfig{Registry: reg, LocalNode: NewNode("r", "r", RoleReader, "c"), Logger: zerolog.Nop(), Designated: func() string { return "w-b" }})
	for i := 0; i < 4; i++ {
		resp, err := r.RouteWrite(context.Background(), writeReq(t))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if gotA.Load() != 0 || gotB.Load() != 4 {
		t.Fatalf("writes: a=%d b=%d, want all to the designated b", gotA.Load(), gotB.Load())
	}
}

// WriteForwardTarget answers only on a trusted view.
func TestWriteForwardTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	rn := startSingleNodeRaft(t, "w-5")
	c := newPauseForwardingCoordinator(t, "w-5", RoleWriter, rn)
	for _, id := range []string{"w-1", "w-5"} {
		if err := rn.AddNode(&raft.NodeInfo{ID: id, Name: id, Role: string(RoleWriter), ClusterName: "c", Address: "127.0.0.1:1", State: string(StateHealthy)}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := rn.PromoteWriter("w-1", "", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for rn.FSM().PrimaryWriterIDFast() != "w-1" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	_ = c.registry.Register(writerNode("w-1", "127.0.0.1:9"))
	fr := newFenceRig()
	fr.f.localID, fr.primary = "w-5", "w-1"
	fr.f.evaluate()
	c.ingestFence.Store(fr.f)

	if n, ok := c.WriteForwardTarget(); !ok || n.ID != "w-1" {
		t.Fatalf("steady standby: %v %v", n, ok)
	}
	// Lost contact: its view of the designation is not trusted.
	fr.now = fr.now.Add(8 * time.Second)
	fr.f.evaluate()
	if _, ok := c.WriteForwardTarget(); ok {
		t.Fatal("forwarded while out of contact with the leader")
	}
	fr = newFenceRig()
	fr.f.localID, fr.primary = "w-5", "w-1"
	fr.f.evaluate()
	c.ingestFence.Store(fr.f)
	// Takeover gates keep their 503.
	c.claimedExit.Store(true)
	if _, ok := c.WriteForwardTarget(); ok {
		t.Fatal("forwarded from a claimed process")
	}
	c.claimedExit.Store(false)
	// Primary unhealthy: no target.
	dead := writerNode("w-1", "127.0.0.1:9")
	dead.UpdateState(StateUnhealthy)
	_ = c.registry.Register(dead)
	if _, ok := c.WriteForwardTarget(); ok {
		t.Fatal("forwarded to an unhealthy primary")
	}
	_ = c.registry.Register(writerNode("w-1", "127.0.0.1:9"))
	// Shared storage: never.
	c.cfg.SharedStorageMode = true
	if _, ok := c.WriteForwardTarget(); ok {
		t.Fatal("forwarded in shared-storage mode")
	}
	c.cfg.SharedStorageMode = false
	// This node is the primary: no target.
	if err := rn.PromoteWriter("w-5", "w-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for rn.FSM().PrimaryWriterIDFast() != "w-5" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	// The local node is a healthy, addressable writer: only the self check
	// stops it.
	self := writerNode("w-5", "127.0.0.1:9")
	_ = c.registry.Register(self)
	fr = newFenceRig()
	fr.f.localID, fr.primary = "w-5", "w-5"
	fr.f.evaluate()
	c.ingestFence.Store(fr.f)
	if _, ok := c.WriteForwardTarget(); ok {
		t.Fatal("the primary forwarded its own writes")
	}
}

func TestAPIHostPort(t *testing.T) {
	for _, c := range []struct{ api, coord, want string }{
		{":8000", "writer-0.writer-headless.ns.svc.cluster.local:9100", "writer-0.writer-headless.ns.svc.cluster.local:8000"},
		{"0.0.0.0:8000", "10.0.0.5:9100", "10.0.0.5:8000"},
		{"10.0.0.7:8000", "10.0.0.5:9100", "10.0.0.7:8000"},
		{"[::1]:8000", "x:9100", "[::1]:8000"},
		{":8000", "", ":8000"},
	} {
		n := NewNode("w", "w", RoleWriter, "c")
		n.SetAddresses(c.coord, c.api)
		if got := apiHostPort(n); got != c.want {
			t.Errorf("api %q coord %q -> %q, want %q", c.api, c.coord, got, c.want)
		}
	}
}
