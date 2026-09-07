package telemetry

import (
	"context"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Client-side identity, as reported by arcli.
//
// arcli sends a random per-installation UUID on every request
// (Arcli-Installation-Id) together with its version in User-Agent.
// The API server hands both to RecordClient for requests that were
// served (status < 400, so unauthenticated probes cannot fill the set);
// the collector includes the distinct installations seen since the
// last successful report in its payload, so the telemetry database can
// correlate CLI installations with Arc instances. Nothing about the
// request itself (path, database, token) is recorded.

// maxClients bounds the registry. Beyond it new ids are dropped and
// the report says Truncated, so a flood of random ids cannot grow
// memory (256 entries ≈ 25 KB).
const maxClients = 256

var installationIDRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ClientsInfo is the "clients" object in the telemetry payload.
type ClientsInfo struct {
	Arcli *ArcliClients `json:"arcli,omitempty"`
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

type clientEntry struct {
	version  string
	lastSeen time.Time
}

// clientRegistry is the bounded, mutex-protected set behind
// Collector.RecordClient. One lock per recorded request; requests
// without the header never touch it.
type clientRegistry struct {
	mu        sync.Mutex
	seen      map[string]clientEntry
	truncated bool
}

func newClientRegistry() *clientRegistry {
	return &clientRegistry{seen: make(map[string]clientEntry)}
}

// record stores or refreshes an installation. Invalid ids and
// over-long versions are rejected before the lock is taken; the strings
// must already be owned by the caller (not fasthttp buffer views).
func (r *clientRegistry) record(id, version string, now time.Time) {
	if len(id) != 36 || !installationIDRe.MatchString(id) {
		return
	}
	version = cleanVersion(version)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.seen[id]; !ok && len(r.seen) >= maxClients {
		r.truncated = true
		return
	}
	r.seen[id] = clientEntry{version: version, lastSeen: now}
}

// take returns the current set rendered for the payload and starts a
// fresh window, so records arriving during the send are not lost. The
// returned window is handed back to restore() if the send fails; nil
// means nothing was seen (the payload omits "clients").
func (r *clientRegistry) take() (*ClientsInfo, *clientWindow) {
	if r == nil {
		return nil, nil
	}
	r.mu.Lock()
	seen, truncated := r.seen, r.truncated
	r.seen, r.truncated = make(map[string]clientEntry), false
	r.mu.Unlock()
	if len(seen) == 0 {
		return nil, nil
	}
	list := make([]ArcliClient, 0, len(seen))
	for id, e := range seen {
		list = append(list, ArcliClient{ID: id, Version: e.version, LastSeen: e.lastSeen.UTC().Format(time.RFC3339)})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return &ClientsInfo{Arcli: &ArcliClients{Installations: len(list), Truncated: truncated, List: list}},
		&clientWindow{seen: seen, truncated: truncated}
}

type clientWindow struct {
	seen      map[string]clientEntry
	truncated bool
}

// restore merges a window back after a failed send. Entries recorded
// since (newer) win; the bound still applies.
func (r *clientRegistry) restore(w *clientWindow) {
	if r == nil || w == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, e := range w.seen {
		if _, ok := r.seen[id]; ok {
			continue
		}
		if len(r.seen) >= maxClients {
			r.truncated = true
			break
		}
		r.seen[id] = e
	}
	r.truncated = r.truncated || w.truncated
}

// pending reports whether anything is waiting to be sent.
func (r *clientRegistry) pending() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen) > 0
}

// cleanVersion keeps a short printable-ASCII version string; anything
// else becomes "unknown" rather than a payload full of junk.
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

// RecordClient notes that an arcli installation was served by this
// node. Safe on a nil collector (telemetry disabled): it does nothing.
func (c *Collector) RecordClient(id, version string) {
	if c == nil || c.clients == nil {
		return
	}
	c.clients.record(id, version, time.Now())
}

// flushClients sends one last report at shutdown when clients are
// waiting, so nodes restarted more often than the interval still report
// the CLI installations they served. Best effort, bounded by timeout.
func (c *Collector) flushClients(timeout time.Duration) {
	if c == nil || c.clients == nil || !c.clients.pending() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c.logger.Info().Msg("Sending final telemetry beacon (pending CLI clients)")
	c.sendTelemetryCtx(ctx)
}
