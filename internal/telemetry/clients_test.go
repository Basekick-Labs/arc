package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const goodID = "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f"

func TestClientRegistry_RecordSnapshotClear(t *testing.T) {
	r := newClientRegistry()
	if r.snapshot() != nil {
		t.Fatal("empty registry must snapshot to nil so the payload omits clients")
	}
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	r.record(goodID, "26.09.1", now)
	r.record(goodID, "26.09.2", now.Add(time.Minute)) // refresh keeps one entry, latest version
	r.record("1f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", "", now)
	s := r.snapshot()
	if s == nil || s.Arcli.Installations != 2 || s.Arcli.Truncated || len(s.Arcli.List) != 2 {
		t.Fatalf("snapshot: %+v", s)
	}
	if !r.pending() {
		t.Error("pending must be true before the window is taken")
	}
	if s.Arcli.List[0].ID != goodID || s.Arcli.List[0].Version != "26.09.2" || s.Arcli.List[0].LastSeen != "2026-09-07T12:01:00Z" {
		t.Errorf("entry: %+v", s.Arcli.List[0])
	}
	if s.Arcli.List[1].Version != "unknown" {
		t.Errorf("empty version must read unknown: %+v", s.Arcli.List[1])
	}
	b, _ := json.Marshal(s)
	if !strings.Contains(string(b), `"arcli":{"installations":2,"truncated":false,"list":[{"id":"`) {
		t.Errorf("json: %s", b)
	}
	// take() starts a new window; restore() merges back after a failed send.
	info, win := r.take()
	if info == nil || len(info.Arcli.List) != 2 || r.snapshot() != nil || r.pending() {
		t.Fatalf("take: info=%+v after=%+v", info, r.snapshot())
	}
	r.record("2f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", "26.09.3", now) // arrives mid-send
	r.restore(win)
	if s := r.snapshot(); s == nil || len(s.Arcli.List) != 3 {
		t.Errorf("restore must merge: %+v", s)
	}
	if _, w := r.take(); w == nil {
		t.Error("window expected")
	}
	r.restore(nil) // no-op
}

// snapshot renders without starting a new window (test helper).
func (r *clientRegistry) snapshot() *ClientsInfo {
	info, win := r.take()
	r.restore(win)
	return info
}

func TestClientRegistry_RejectsGarbage(t *testing.T) {
	r := newClientRegistry()
	now := time.Now()
	for _, bad := range []string{"", "abc", strings.Repeat("a", 36), "0F0F0F0F-0F0F-4F0F-8F0F-0F0F0F0F0F0F", goodID + "x", "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0\n", strings.Repeat("0", 10000)} {
		r.record(bad, "26.09.1", now)
	}
	if r.snapshot() != nil {
		t.Errorf("garbage ids must be dropped: %+v", r.snapshot())
	}
	r.record(goodID, strings.Repeat("v", 33), now)
	r.record("2f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", "26.09.1\x1b[31m", now)
	s := r.snapshot()
	for _, e := range s.Arcli.List {
		if e.Version != "unknown" {
			t.Errorf("bad version must read unknown: %+v", e)
		}
	}
}

func TestClientRegistry_Bounded(t *testing.T) {
	r := newClientRegistry()
	now := time.Now()
	for i := 0; i < maxClients+1000; i++ {
		r.record(fmt.Sprintf("%08x-0000-4000-8000-000000000000", i), "26.09.1", now)
	}
	s := r.snapshot()
	if len(s.Arcli.List) != maxClients || !s.Arcli.Truncated || s.Arcli.Installations != maxClients {
		t.Errorf("bounded: listed=%d truncated=%v installations=%d", len(s.Arcli.List), s.Arcli.Truncated, s.Arcli.Installations)
	}
	// Known ids still refresh when the map is full.
	r.record("00000000-0000-4000-8000-000000000000", "26.09.9", now)
	if r.snapshot().Arcli.List[0].Version != "26.09.9" {
		t.Error("existing entry must refresh when full")
	}
}

func TestRecordClient_NilCollector(t *testing.T) {
	var c *Collector
	c.RecordClient(goodID, "x") // must not panic
	c.flushClients(time.Second)
	c = &Collector{}
	c.RecordClient(goodID, "x") // no registry: no-op
	c.flushClients(time.Second)
	if info, w := c.clients.take(); info != nil || w != nil || c.clients.pending() {
		t.Error("nil registry must be inert")
	}
	c.clients.restore(nil)
}

func TestSendTelemetry_WindowSemantics(t *testing.T) {
	var got []string
	var status = 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, string(b))
		w.WriteHeader(status)
	}))
	defer srv.Close()
	dir := t.TempDir()
	c, err := New(&Config{Enabled: true, Endpoint: srv.URL, Interval: time.Hour, DataDir: dir}, "test", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c.RecordClient(goodID, "26.09.1")
	c.sendTelemetryCtx(context.Background()) // 500: window restored
	if !strings.Contains(got[0], `"clients":{"arcli":{"installations":1`) || !c.clients.pending() {
		t.Fatalf("failed send: body=%s pending=%v", got[0], c.clients.pending())
	}
	status = 200
	c.sendTelemetryCtx(context.Background()) // 200: window cleared
	if !strings.Contains(got[1], goodID) || c.clients.pending() {
		t.Fatalf("ok send: body=%s pending=%v", got[1], c.clients.pending())
	}
	c.sendTelemetryCtx(context.Background()) // nothing pending: no clients key
	if strings.Contains(got[2], `"clients"`) {
		t.Errorf("empty window must omit clients: %s", got[2])
	}
	// Shutdown flush only when something is pending.
	c.flushClients(time.Second)
	if len(got) != 3 {
		t.Errorf("flush with nothing pending must not send (got %d)", len(got))
	}
	c.RecordClient(goodID, "26.09.1")
	c.flushClients(time.Second)
	if len(got) != 4 {
		t.Errorf("flush with pending must send (got %d)", len(got))
	}
}
