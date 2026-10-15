package filereplication

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// verdictRig is a puller over a real local backend whose walk sees a scripted
// ObserveListed verdict and reports what RecordContent was told (#1182).
type verdictRig struct {
	p       *Puller
	local   *storage.LocalBackend
	fetcher *fakeFetcher
	mu      sync.Mutex
	content map[string]string // path -> sha RecordContent was told
}

func newVerdictRig(t *testing.T, peerBody []byte, verdict ListedVerdict, mutate func(*Config)) *verdictRig {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	r := &verdictRig{local: local, content: map[string]string{}}
	if peerBody != nil {
		r.fetcher = newRepeatingFetcher(peerBody)
	} else {
		r.fetcher = &fakeFetcher{results: []fakeFetchResult{{err: errors.New("no peer holds it")}}, repeat: true}
	}
	cfg := Config{
		SelfNodeID:              "reader-1",
		Backend:                 local,
		Fetcher:                 r.fetcher,
		PeerResolver:            staticResolver{nodeID: "writer-1", addrs: []string{"peer:1"}, ok: true},
		Workers:                 1,
		QueueSize:               8,
		RetryMaxAttempts:        1,
		RetryInitialBackoff:     time.Millisecond,
		FetchTimeout:            2 * time.Second,
		ForceContentRefresh:     true,
		RepullMissingSelfOrigin: true,
		ObserveListed:           func(*raft.FileEntry) ListedVerdict { return verdict },
		LocalStamp: func(path string) (string, bool) {
			if _, err := local.StatFile(context.Background(), path); err != nil {
				return "", false
			}
			return "stamp", true
		},
		RecordContent: func(e *raft.FileEntry, _, sha string) {
			r.mu.Lock()
			r.content[e.Path] = sha
			r.mu.Unlock()
		},
		Logger: zerolog.Nop(),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.Start(context.Background())
	t.Cleanup(p.Stop)
	r.p = p
	return r
}

func (r *verdictRig) recorded(path string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sha, ok := r.content[path]
	return sha, ok
}

// catchUp runs the startup walk over entries and waits for the batch to settle.
func (r *verdictRig) catchUp(t *testing.T, entries ...*raft.FileEntry) map[string]int64 {
	t.Helper()
	r.p.RunCatchUp(context.Background(), sliceFetcher(entries))
	return waitStats(t, r.p, func(s map[string]int64) bool {
		return s["inflight_count"] == 0 && s["queue_depth"] == 0
	})
}

const verdictPath = "db/m/2026/10/15/00/v.parquet"

var (
	staleBody   = []byte("old same-size payload")
	currentBody = []byte("new same-size payload")
)

// The repro of #1182 at the puller: a copy of the manifest's size the node
// cannot vouch for is no longer trusted by its size. Unverified: hashed; a
// copy that already is the manifest's version is kept, and its checksum
// recorded, with no download.
func TestCatchUpHashesAnUnverifiedCopy(t *testing.T) {
	r := newVerdictRig(t, currentBody, ListedUnverified, nil)
	if err := r.local.Write(context.Background(), verdictPath, currentBody); err != nil {
		t.Fatal(err)
	}
	e := entryFor(verdictPath, currentBody)
	stats := r.catchUp(t, e)
	if r.fetcher.calls.Load() != 0 || stats["refresh_verified"] != 1 || stats["listed_unvouched"] != 1 {
		t.Fatalf("fetches=%d stats=%v, want a hash and no download", r.fetcher.calls.Load(), stats)
	}
	if sha, _ := r.recorded(verdictPath); sha != e.SHA256 {
		t.Fatalf("recorded %q, want the hashed checksum", sha)
	}
	if !r.p.FullyCaughtUp() {
		t.Fatalf("a verified copy left the gate red: %v", r.p.CatchUpStatus())
	}
}

// Unverified and stale: the hash finds other bytes, records what it found,
// and the manifest's version is downloaded over it.
func TestCatchUpReplacesAStaleUnverifiedCopy(t *testing.T) {
	r := newVerdictRig(t, currentBody, ListedUnverified, nil)
	if err := r.local.Write(context.Background(), verdictPath, staleBody); err != nil {
		t.Fatal(err)
	}
	e := entryFor(verdictPath, currentBody)
	r.catchUp(t, e)
	body, err := r.local.Read(context.Background(), verdictPath)
	if err != nil || string(body) != string(currentBody) {
		t.Fatalf("local = %q (%v), want the manifest version", body, err)
	}
	if sha, _ := r.recorded(verdictPath); sha != e.SHA256 {
		t.Fatalf("recorded %q after the pull, want the manifest checksum", sha)
	}
	if !r.p.FullyCaughtUp() {
		t.Fatalf("gate red after the refresh: %v", r.p.CatchUpStatus())
	}
}

// Known stale: downloaded without hashing first.
func TestCatchUpDownloadsAKnownStaleCopy(t *testing.T) {
	r := newVerdictRig(t, currentBody, ListedStale, nil)
	if err := r.local.Write(context.Background(), verdictPath, staleBody); err != nil {
		t.Fatal(err)
	}
	stats := r.catchUp(t, entryFor(verdictPath, currentBody))
	if r.fetcher.calls.Load() != 1 || stats["refresh_verified"] != 0 || stats["forced_refresh"] != 1 {
		t.Fatalf("fetches=%d stats=%v, want one forced download", r.fetcher.calls.Load(), stats)
	}
}

// No peer holds the manifest's bytes: the gate stays red (truthful), the
// stale copy stays where it is (it may be the only copy of anything), and a
// trusted verdict keeps the old size-only behaviour.
func TestCatchUpGateStaysRedWhileAStaleCopyCannotBeRefreshed(t *testing.T) {
	r := newVerdictRig(t, nil, ListedStale, nil)
	if err := r.local.Write(context.Background(), verdictPath, staleBody); err != nil {
		t.Fatal(err)
	}
	r.catchUp(t, entryFor(verdictPath, currentBody))
	if r.p.FullyCaughtUp() {
		t.Fatalf("gate green over a stale copy: %v", r.p.CatchUpStatus())
	}
	body, err := r.local.Read(context.Background(), verdictPath)
	if err != nil || string(body) != string(staleBody) {
		t.Fatalf("local = %q (%v), want the stale copy kept", body, err)
	}

	trusted := newVerdictRig(t, nil, ListedTrusted, nil)
	if err := trusted.local.Write(context.Background(), verdictPath, staleBody); err != nil {
		t.Fatal(err)
	}
	trusted.catchUp(t, entryFor(verdictPath, currentBody))
	if !trusted.p.FullyCaughtUp() || trusted.fetcher.calls.Load() != 0 {
		t.Fatalf("a trusted copy was refreshed: fetches=%d %v", trusted.fetcher.calls.Load(), trusted.p.CatchUpStatus())
	}
}

// A self-origin copy is not skipped by the walk when unvouched: a merge
// restore that wrote other bytes and failed to register leaves exactly that.
// With no peer to serve the manifest's version, the node's own copy is kept.
func TestCatchUpChecksAnUnvouchedSelfOriginCopy(t *testing.T) {
	r := newVerdictRig(t, nil, ListedUnverified, nil)
	if err := r.local.Write(context.Background(), verdictPath, staleBody); err != nil {
		t.Fatal(err)
	}
	e := entryFor(verdictPath, currentBody)
	e.OriginNodeID = "reader-1"
	stats := r.catchUp(t, e)
	if stats["listed_unvouched"] != 1 || r.p.FullyCaughtUp() {
		t.Fatalf("self-origin unvouched copy skipped: stats=%v", stats)
	}
	if body, _ := r.local.Read(context.Background(), verdictPath); string(body) != string(staleBody) {
		t.Fatalf("the node's own copy was touched: %q", body)
	}
}

// A hash the walk asked for that cannot be read is not answered with a full
// download (#1182): the gate waits, the next walk hashes again.
func TestCatchUpUnreadableHashDoesNotDownload(t *testing.T) {
	r := newVerdictRig(t, currentBody, ListedUnverified, func(c *Config) {
		c.Backend = unreadableBackend{c.Backend}
	})
	if err := r.local.Write(context.Background(), verdictPath, currentBody); err != nil {
		t.Fatal(err)
	}
	stats := r.catchUp(t, entryFor(verdictPath, currentBody))
	if r.fetcher.calls.Load() != 0 || stats["verify_unreadable"] != 1 || r.p.FullyCaughtUp() {
		t.Fatalf("fetches=%d stats=%v, want no download, gate red", r.fetcher.calls.Load(), stats)
	}
}

type unreadableBackend struct{ storage.Backend }

func (unreadableBackend) ReadTo(context.Context, string, io.Writer) error {
	return errors.New("disk too slow")
}

// slowBackend reads the local file in small chunks, pausing between them, or
// stalls after the first chunk.
type slowBackend struct {
	storage.Backend
	pause time.Duration
	stall bool
}

func (b slowBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	body, err := b.Backend.Read(ctx, path)
	if err != nil {
		return err
	}
	for i := 0; i < len(body); i += 4 {
		if _, err := w.Write(body[i:min(i+4, len(body))]); err != nil {
			return err
		}
		if b.stall {
			<-ctx.Done()
			return ctx.Err()
		}
		time.Sleep(b.pause)
	}
	return nil
}

// A hash is bounded by a stall, not a deadline (#1182, review H1): a copy read
// slowly but steadily is hashed to the end however long it takes; one that
// stops reading is given up after the stall.
func TestVerifyLocalCopyIsBoundedByAStall(t *testing.T) {
	old := localHashStall
	localHashStall = 150 * time.Millisecond
	t.Cleanup(func() { localHashStall = old })
	ctx := context.Background()
	slow := newVerdictRig(t, currentBody, ListedUnverified, func(c *Config) {
		c.Backend = slowBackend{Backend: c.Backend, pause: 40 * time.Millisecond} // ~240 ms in all
	})
	if err := slow.local.Write(ctx, verdictPath, currentBody); err != nil {
		t.Fatal(err)
	}
	e := entryFor(verdictPath, currentBody)
	if got := slow.p.verifyLocalCopy(zerolog.Nop(), e); got != localCopyMatched {
		t.Fatalf("a slow steady hash = %v, want matched", got)
	}
	stalled := newVerdictRig(t, currentBody, ListedUnverified, func(c *Config) {
		c.Backend = slowBackend{Backend: c.Backend, stall: true}
	})
	if err := stalled.local.Write(ctx, verdictPath, currentBody); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if got := stalled.p.verifyLocalCopy(zerolog.Nop(), e); got != localCopyUnreadable {
		t.Fatalf("a stalled hash = %v, want unreadable", got)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("a stalled hash took %v", d)
	}
}

// A walk's hash that could not be read does not leave the path pending a
// forced refresh, which would make the next plain request download it in full
// (#1182, review M1).
func TestUnreadableHashDoesNotMarkTheRefreshPending(t *testing.T) {
	r := newVerdictRig(t, currentBody, ListedUnverified, func(c *Config) {
		c.Backend = unreadableBackend{c.Backend}
	})
	if err := r.local.Write(context.Background(), verdictPath, currentBody); err != nil {
		t.Fatal(err)
	}
	stats := r.catchUp(t, entryFor(verdictPath, currentBody))
	if stats["refresh_pending"] != 0 || stats["verify_unreadable"] != 1 {
		t.Fatalf("stats=%v, want nothing pending", stats)
	}
	r.p.Enqueue(entryFor(verdictPath, currentBody)) // a later plain arrival
	waitStats(t, r.p, func(s map[string]int64) bool { return s["inflight_count"] == 0 && s["skipped_local"] >= 1 })
	if r.fetcher.calls.Load() != 0 {
		t.Fatal("a plain arrival after an unreadable hash downloaded the file")
	}
}

// A self-origin copy is never refreshed by the walk where self-origin pulls
// are off (a shared backend): the request would be skipped and its gate tag
// never cleared (#1182, review L5).
func TestCatchUpLeavesSelfOriginAloneWithoutRepull(t *testing.T) {
	r := newVerdictRig(t, currentBody, ListedStale, func(c *Config) { c.RepullMissingSelfOrigin = false })
	if err := r.local.Write(context.Background(), verdictPath, staleBody); err != nil {
		t.Fatal(err)
	}
	e := entryFor(verdictPath, currentBody)
	e.OriginNodeID = "reader-1"
	stats := r.catchUp(t, e)
	if stats["listed_unvouched"] != 0 || !r.p.FullyCaughtUp() {
		t.Fatalf("stats=%v gate=%v", stats, r.p.CatchUpStatus())
	}
}

// A pull that finds no candidate peer in any attempt is a failure: the
// startup gate does not go green over a file it never fetched.
func TestNoCandidatePeerCountsAsFailed(t *testing.T) {
	r := newVerdictRig(t, currentBody, ListedTrusted, func(c *Config) {
		c.PeerResolver = staticResolver{ok: false}
	})
	stats := r.catchUp(t, entryFor(verdictPath, currentBody))
	if stats["no_peer_exhausted"] != 1 || stats["failed"] != 1 || r.p.FullyCaughtUp() {
		t.Fatalf("stats=%v gate=%v", stats, r.p.CatchUpStatus())
	}
}

// A forced refresh waiting behind a running request keeps its force when a
// plain request (a tier update: newer LSN, same checksum) replaces it, and the
// hand-off keeps it too (#1182, adversarial H1).
func TestForceSurvivesAReplacingPlainRequest(t *testing.T) {
	p := &Puller{
		inflight:       map[string]*pullRequest{},
		pending:        map[string]*pullRequest{},
		refreshPending: map[string]struct{}{},
		queue:          make(chan *pullRequest, 4),
		cfg:            Config{SelfNodeID: "reader-1", ForceContentRefresh: true},
		logger:         zerolog.Nop(),
	}
	running := entryFor(verdictPath, currentBody)
	running.LSN = 1
	p.inflight[verdictPath] = &pullRequest{entry: running}
	forced := *running
	forced.LSN = 2
	p.enqueueRequest(&forced, enqueueSourceCatchUp, true, true)
	tier := forced
	tier.LSN = 3
	p.enqueueRequest(&tier, enqueueSourceReactive, false, false)
	w := p.pending[verdictPath]
	if w == nil || !w.force || !w.verifyLocal || w.entry.LSN != 3 {
		t.Fatalf("waiting = %+v, want LSN 3 with force and verify", w)
	}

	// Hand-off from a running forced request that did not land.
	p.inflight[verdictPath] = &pullRequest{entry: running, force: true, verifyLocal: true}
	p.pending[verdictPath] = &pullRequest{entry: &tier}
	next := p.finishEntry(verdictPath, enqueueSourceReactive, false, false, true)
	if next == nil || !next.force || !next.verifyLocal {
		t.Fatalf("hand-off = %+v, want force and verify kept", next)
	}
	// One that landed hands off as is.
	p.inflight[verdictPath] = &pullRequest{entry: running, force: true}
	p.pending[verdictPath] = &pullRequest{entry: &tier}
	if next := p.finishEntry(verdictPath, enqueueSourceReactive, false, true, true); next == nil || next.force {
		t.Fatalf("hand-off after a pull = %+v, want plain", next)
	}
}
