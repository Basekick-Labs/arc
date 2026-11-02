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
	r.record(ClientKindArcli, goodID, "26.09.1", "", "", now)
	r.record(ClientKindArcli, goodID, "26.09.2", "", "", now.Add(time.Minute)) // refresh keeps one entry, latest version
	r.record(ClientKindArcli, "1f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", "", "", "", now)
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
	r.record(ClientKindArcli, "2f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", "26.09.3", "", "", now) // arrives mid-send
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
		r.record(ClientKindArcli, bad, "26.09.1", "", "", now)
	}
	if r.snapshot() != nil {
		t.Errorf("garbage ids must be dropped: %+v", r.snapshot())
	}
	r.record(ClientKindArcli, goodID, strings.Repeat("v", 33), "", "", now)
	r.record(ClientKindArcli, "2f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", "26.09.1\x1b[31m", "", "", now)
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
	for i := 0; i < maxClientsPerKind+1000; i++ {
		r.record(ClientKindArcli, fmt.Sprintf("%08x-0000-4000-8000-000000000000", i), "26.09.1", "", "", now)
	}
	s := r.snapshot()
	if len(s.Arcli.List) != maxClientsPerKind || !s.Arcli.Truncated || s.Arcli.Installations != maxClientsPerKind {
		t.Errorf("bounded: listed=%d truncated=%v installations=%d", len(s.Arcli.List), s.Arcli.Truncated, s.Arcli.Installations)
	}
	// Known ids still refresh when the map is full.
	r.record(ClientKindArcli, "00000000-0000-4000-8000-000000000000", "26.09.9", "", "", now)
	if r.snapshot().Arcli.List[0].Version != "26.09.9" {
		t.Error("existing entry must refresh when full")
	}
}

func TestRecordClient_NilCollector(t *testing.T) {
	var c *Collector
	c.RecordClient(ClientKindArcli, goodID, "x", "", "") // must not panic
	c.flushClients(time.Second)
	c = &Collector{}
	c.RecordClient(ClientKindArcli, goodID, "x", "", "") // no registry: no-op
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
	c.RecordClient(ClientKindArcli, goodID, "26.09.1", "", "")
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
	c.RecordClient(ClientKindArcli, goodID, "26.09.1", "", "")
	c.flushClients(time.Second)
	if len(got) != 4 {
		t.Errorf("flush with pending must send (got %d)", len(got))
	}
}

const sdkID = "5f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f"

// The arcli section is byte-for-byte what Arc sent before SDK support:
// same keys, same order, no runtime/platform even when the recorder is
// handed them, and no python_sdk key when no SDK was seen.
func TestClientsPayload_ArcliUnchanged(t *testing.T) {
	r := newClientRegistry()
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	r.record(ClientKindArcli, goodID, "26.09.1", "3.12.4", "linux/amd64", now)
	b, err := json.Marshal(r.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"arcli":{"installations":1,"truncated":false,"list":[{"id":"0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f","version":"26.09.1","last_seen":"2026-09-07T12:00:00Z"}]}}`
	if string(b) != want {
		t.Errorf("arcli payload changed:\n got %s\nwant %s", b, want)
	}
}

func TestClientsPayload_PythonSDKShape(t *testing.T) {
	r := newClientRegistry()
	now := time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)
	r.record(ClientKindPythonSDK, sdkID, "0.2.0", "3.12.4", "linux/amd64", now)
	r.record(ClientKindPythonSDK, "6f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", "0.1.0", "", "", now) // older SDK: no UA comment
	b, _ := json.Marshal(r.snapshot())
	const want = `{"python_sdk":{"installations":2,"truncated":false,"list":[` +
		`{"id":"5f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f","version":"0.2.0","runtime":"3.12.4","platform":"linux/amd64","last_seen":"2026-11-02T09:30:00Z"},` +
		`{"id":"6f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f","version":"0.1.0","runtime":"unknown","platform":"unknown","last_seen":"2026-11-02T09:30:00Z"}]}}`
	if string(b) != want {
		t.Errorf("python_sdk payload:\n got %s\nwant %s", b, want)
	}
	// Both kinds: arcli first, python_sdk beside it, arcli bytes unchanged.
	r.record(ClientKindArcli, goodID, "26.09.1", "", "", time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC))
	b, _ = json.Marshal(r.snapshot())
	if !strings.HasPrefix(string(b), `{"arcli":{"installations":1,"truncated":false,"list":[{"id":"0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f","version":"26.09.1","last_seen":"2026-09-07T12:00:00Z"}]},"python_sdk":{"installations":2,`) {
		t.Errorf("both kinds: %s", b)
	}
}

func TestClientRegistry_SDKCleaningAndUnknownKind(t *testing.T) {
	r := newClientRegistry()
	now := time.Now()
	for _, kind := range []string{"", "go_sdk", "ARCLI", "python", "python_sdk "} {
		r.record(kind, sdkID, "0.2.0", "3.12.4", "linux/amd64", now)
	}
	if r.snapshot() != nil || r.pending() {
		t.Fatalf("unknown kinds must be ignored: %+v", r.snapshot())
	}
	r.record(ClientKindPythonSDK, "0F0F0F0F-0F0F-4F0F-8F0F-0F0F0F0F0F0F", "0.2.0", "3.12.4", "linux/amd64", now)
	if r.snapshot() != nil {
		t.Fatal("uppercase id must be dropped")
	}
	r.record(ClientKindPythonSDK, sdkID, "0.2.0", strings.Repeat("3", 33), "linux amd64", now)
	r.record(ClientKindPythonSDK, goodID, "0.2.0", "3.12.4\x00", "linux/amd64é", now)
	r.record(ClientKindPythonSDK, "7f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f", strings.Repeat("v", 32), strings.Repeat("r", 32), strings.Repeat("p", 32), now)
	s := r.snapshot()
	if s.Arcli != nil || s.PythonSDK == nil || len(s.PythonSDK.List) != 3 {
		t.Fatalf("snapshot: %+v", s)
	}
	for _, e := range s.PythonSDK.List[:2] { // goodID, sdkID sort first
		if e.Runtime != "unknown" || e.Platform != "unknown" || e.Version != "0.2.0" {
			t.Errorf("bad runtime/platform must read unknown: %+v", e)
		}
	}
	if e := s.PythonSDK.List[2]; e.Runtime != strings.Repeat("r", 32) || e.Platform != strings.Repeat("p", 32) {
		t.Errorf("32 chars is within the limit: %+v", e)
	}
}

// SDK churn (a fresh id per container start) cannot crowd out arcli:
// each kind has its own bound and its own truncated flag, through take
// and restore as well.
func TestClientRegistry_BoundedPerKind(t *testing.T) {
	r := newClientRegistry()
	now := time.Now()
	id := func(prefix, i int) string { return fmt.Sprintf("%02x%06x-0000-4000-8000-000000000000", prefix, i) }
	for i := 0; i < maxClientsPerKind+500; i++ {
		r.record(ClientKindPythonSDK, id(1, i), "0.2.0", "3.12.4", "linux/amd64", now)
	}
	for i := 0; i < maxClientsPerKind; i++ {
		r.record(ClientKindArcli, id(2, i), "26.09.1", "", "", now)
	}
	s := r.snapshot()
	if len(s.PythonSDK.List) != maxClientsPerKind || !s.PythonSDK.Truncated {
		t.Errorf("python_sdk: listed=%d truncated=%v", len(s.PythonSDK.List), s.PythonSDK.Truncated)
	}
	if len(s.Arcli.List) != maxClientsPerKind || s.Arcli.Truncated {
		t.Errorf("arcli must be unaffected by SDK churn: listed=%d truncated=%v", len(s.Arcli.List), s.Arcli.Truncated)
	}
	r.record(ClientKindArcli, id(2, maxClientsPerKind), "26.09.1", "", "", now) // arcli's own bound
	if s := r.snapshot(); !s.Arcli.Truncated || len(s.Arcli.List) != maxClientsPerKind {
		t.Errorf("arcli bound: %+v", s.Arcli.Truncated)
	}

	// take/restore keep kinds apart: a full SDK window restored over
	// newer SDK ids truncates the SDK kind only.
	info, win := r.take()
	if info == nil || r.pending() {
		t.Fatal("take must start an empty window")
	}
	r.record(ClientKindPythonSDK, id(3, 0), "0.2.1", "3.13.0", "darwin/arm64", now)
	r.restore(win)
	s = r.snapshot()
	if len(s.PythonSDK.List) != maxClientsPerKind || !s.PythonSDK.Truncated || len(s.Arcli.List) != maxClientsPerKind {
		t.Errorf("restore: sdk=%d/%v arcli=%d", len(s.PythonSDK.List), s.PythonSDK.Truncated, len(s.Arcli.List))
	}
	found := false
	for _, e := range s.PythonSDK.List {
		found = found || e.ID == id(3, 0)
	}
	if !found {
		t.Error("the id recorded during the send must survive restore")
	}
}

func TestClientRegistry_PendingSDKOnly(t *testing.T) {
	r := newClientRegistry()
	r.record(ClientKindPythonSDK, sdkID, "0.2.0", "3.12.4", "linux/amd64", time.Now())
	if !r.pending() {
		t.Error("an SDK-only window is pending")
	}
	info, win := r.take()
	if info == nil || info.Arcli != nil || info.PythonSDK == nil || win == nil || r.pending() {
		t.Errorf("take: %+v", info)
	}
}

func TestSendTelemetry_PythonSDK(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, string(b))
	}))
	defer srv.Close()
	c, err := New(&Config{Enabled: true, Endpoint: srv.URL, Interval: time.Hour, DataDir: t.TempDir()}, "test", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c.RecordClient(ClientKindPythonSDK, sdkID, "0.2.0", "3.12.4", "linux/amd64")
	c.sendTelemetryCtx(context.Background())
	if len(got) != 1 || !strings.Contains(got[0], `"clients":{"python_sdk":{"installations":1,"truncated":false,"list":[{"id":"`+sdkID+`","version":"0.2.0","runtime":"3.12.4","platform":"linux/amd64","last_seen":"`) {
		t.Fatalf("body: %v", got)
	}
	if c.clients.pending() {
		t.Error("a 200 must clear the SDK window")
	}
	c.RecordClient(ClientKindPythonSDK, sdkID, "0.2.0", "3.12.4", "linux/amd64")
	c.flushClients(time.Second)
	if len(got) != 2 {
		t.Errorf("an SDK-only pending window must flush at shutdown (got %d)", len(got))
	}
}

func TestCleanRuntimeField(t *testing.T) {
	for in, want := range map[string]string{
		"3.12.4":                "3.12.4",
		"3.13.0rc1":             "3.13.0rc1",
		"linux/amd64":           "linux/amd64",
		"darwin/arm64":          "darwin/arm64",
		"":                      "unknown",
		`"},{"x":1`:             "unknown",
		"<script>":              "unknown",
		"linux amd64":           "unknown",
		"3.12.4\x00":            "unknown",
		strings.Repeat("a", 33): "unknown",
	} {
		if got := cleanRuntimeField(in); got != want {
			t.Errorf("cleanRuntimeField(%q) = %q, want %q", in, got, want)
		}
	}
}
