package telemetry

import (
	"context"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Client-side identity, as reported by arcli and the Python SDK.
//
// Clients send a random per-installation UUID on every request
// (Arc-Client-Installation-Id, or arcli's legacy Arcli-Installation-Id)
// together with their product and version in User-Agent. The API server
// hands both to RecordClient for requests that were served (status <
// 400, so unauthenticated probes cannot fill the set); the collector
// includes the distinct installations seen since the last successful
// report in its payload, so the telemetry database can correlate client
// installations with Arc instances. Nothing about the request itself
// (path, database, token) is recorded.

// Client kinds; must match api.ClientKindArcli / api.ClientKindPythonSDK
// (internal/api/client_identity.go). Any other kind is ignored.
const (
	clientKindArcli     = "arcli"
	clientKindPythonSDK = "python_sdk"
)

// maxClientsPerKind bounds the registry, per client kind, so a churn of
// SDK ids (one per container start, say) cannot crowd out arcli ids.
// Beyond it new ids of that kind are dropped and the report says
// Truncated for that kind, so a flood of random ids cannot grow memory
// (2 × 256 entries ≈ 60 KB).
const maxClientsPerKind = 256

var installationIDRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ClientsInfo is the "clients" object in the telemetry payload.
type ClientsInfo struct {
	Arcli     *ArcliClients `json:"arcli,omitempty"`
	PythonSDK *SDKClients   `json:"python_sdk,omitempty"`
}

// ArcliClients lists the arcli installations seen since the last report.
type ArcliClients struct {
	Installations int           `json:"installations"` // == len(List)
	Truncated     bool          `json:"truncated"`     // at least one more distinct id was seen and dropped
	List          []ArcliClient `json:"list"`
}

// ArcliClient is one installation.
type ArcliClient struct {
	ID       string `json:"id"`
	Version  string `json:"version"`
	LastSeen string `json:"last_seen"` // RFC 3339, UTC
}

// SDKClients lists the installations of one SDK seen since the last
// report. Generic so later SDKs (Go, JS) reuse it under their own key.
type SDKClients struct {
	Installations int         `json:"installations"` // == len(List)
	Truncated     bool        `json:"truncated"`     // at least one more distinct id was seen and dropped
	List          []SDKClient `json:"list"`
}

// SDKClient is one SDK installation. Runtime is the language runtime
// version ("3.12.4"), Platform the os/arch pair ("linux/amd64");
// either is "unknown" when the User-Agent did not say.
type SDKClient struct {
	ID       string `json:"id"`
	Version  string `json:"version"`
	Runtime  string `json:"runtime"`
	Platform string `json:"platform"`
	LastSeen string `json:"last_seen"` // RFC 3339, UTC
}

type clientEntry struct {
	version, runtime, platform string
	lastSeen                   time.Time
}

// kindWindow is one client kind's set within a reporting window.
type kindWindow struct {
	seen      map[string]clientEntry
	truncated bool
}

func newKindWindows() map[string]*kindWindow {
	return map[string]*kindWindow{
		clientKindArcli:     {seen: make(map[string]clientEntry)},
		clientKindPythonSDK: {seen: make(map[string]clientEntry)},
	}
}

// clientRegistry is the bounded, mutex-protected set behind
// Collector.RecordClient. One lock per recorded request; requests
// without the header never touch it.
type clientRegistry struct {
	mu    sync.Mutex
	kinds map[string]*kindWindow // fixed key set: newKindWindows
}

func newClientRegistry() *clientRegistry {
	return &clientRegistry{kinds: newKindWindows()}
}

// record stores or refreshes an installation. Unknown kinds, invalid
// ids and over-long fields are rejected before the lock is taken; the
// strings must already be owned by the caller (not fasthttp buffer
// views).
func (r *clientRegistry) record(kind, id, version, runtime, platform string, now time.Time) {
	if kind != clientKindArcli && kind != clientKindPythonSDK {
		return
	}
	if len(id) != 36 || !installationIDRe.MatchString(id) {
		return
	}
	e := clientEntry{version: cleanVersion(version), lastSeen: now}
	if kind != clientKindArcli {
		e.runtime, e.platform = cleanVersion(runtime), cleanVersion(platform)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.kinds[kind]
	if _, ok := w.seen[id]; !ok && len(w.seen) >= maxClientsPerKind {
		w.truncated = true
		return
	}
	w.seen[id] = e
}

// take returns the current sets rendered for the payload and starts a
// fresh window, so records arriving during the send are not lost. The
// returned window is handed back to restore() if the send fails; nil
// means nothing was seen for any kind (the payload omits "clients").
func (r *clientRegistry) take() (*ClientsInfo, *clientWindow) {
	if r == nil {
		return nil, nil
	}
	r.mu.Lock()
	kinds := r.kinds
	r.kinds = newKindWindows()
	r.mu.Unlock()
	info := &ClientsInfo{}
	if w := kinds[clientKindArcli]; len(w.seen) > 0 {
		list := make([]ArcliClient, 0, len(w.seen))
		for id, e := range w.seen {
			list = append(list, ArcliClient{ID: id, Version: e.version, LastSeen: e.lastSeen.UTC().Format(time.RFC3339)})
		}
		sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
		info.Arcli = &ArcliClients{Installations: len(list), Truncated: w.truncated, List: list}
	}
	if w := kinds[clientKindPythonSDK]; len(w.seen) > 0 {
		info.PythonSDK = renderSDK(w)
	}
	if info.Arcli == nil && info.PythonSDK == nil {
		return nil, nil
	}
	return info, &clientWindow{kinds: kinds}
}

func renderSDK(w *kindWindow) *SDKClients {
	list := make([]SDKClient, 0, len(w.seen))
	for id, e := range w.seen {
		list = append(list, SDKClient{ID: id, Version: e.version, Runtime: e.runtime, Platform: e.platform, LastSeen: e.lastSeen.UTC().Format(time.RFC3339)})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return &SDKClients{Installations: len(list), Truncated: w.truncated, List: list}
}

type clientWindow struct {
	kinds map[string]*kindWindow
}

// restore merges a window back after a failed send, per kind. Entries
// recorded since (newer) win; each kind's bound still applies.
func (r *clientRegistry) restore(cw *clientWindow) {
	if r == nil || cw == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for kind, old := range cw.kinds {
		cur := r.kinds[kind]
		if cur == nil {
			continue
		}
		for id, e := range old.seen {
			if _, ok := cur.seen[id]; ok {
				continue
			}
			if len(cur.seen) >= maxClientsPerKind {
				cur.truncated = true
				break
			}
			cur.seen[id] = e
		}
		cur.truncated = cur.truncated || old.truncated
	}
}

// pending reports whether anything is waiting to be sent.
func (r *clientRegistry) pending() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.kinds {
		if len(w.seen) > 0 {
			return true
		}
	}
	return false
}

// cleanVersion keeps a short printable-ASCII string (version, runtime,
// platform); anything else becomes "unknown" rather than a payload full
// of junk.
func cleanVersion(v string) string {
	if v == "" || len(v) > 32 {
		return "unknown"
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 0x21 || v[i] > 0x7e {
			return "unknown"
		}
	}
	return v
}

// RecordClient notes that a client installation (arcli, Python SDK)
// was served by this node. Safe on a nil collector (telemetry
// disabled): it does nothing. Implements api.ClientRecorder.
func (c *Collector) RecordClient(kind, id, version, runtime, platform string) {
	if c == nil || c.clients == nil {
		return
	}
	c.clients.record(kind, id, version, runtime, platform, time.Now())
}

// flushClients sends one last report at shutdown when clients are
// waiting, so nodes restarted more often than the interval still report
// the client installations they served. Best effort, bounded by timeout.
func (c *Collector) flushClients(timeout time.Duration) {
	if c == nil || c.clients == nil || !c.clients.pending() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c.logger.Info().Msg("Sending final telemetry beacon (pending clients)")
	c.sendTelemetryCtx(ctx)
}
