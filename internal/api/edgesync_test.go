package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

const (
	testHubID   = "ground-station"
	testSpokeID = "rocket-01"
	testSecret  = "per-spoke-shared-secret"
	testSyncPth = "metrics/cpu/2026/08/07/14/cpu_123.parquet"
)

func testDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// syncTestRig is a hub with the receive endpoint mounted and no API-token auth
// (authManager nil), so tests exercise the sync-specific HMAC layer directly.
type syncTestRig struct {
	app     *fiber.App
	backend storage.Backend
}

func newSyncTestRig(t *testing.T) *syncTestRig {
	t.Helper()

	dir, err := os.MkdirTemp("", "api-sync-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	idx := newTestAPIHubIndex(t)
	recv, err := edgesync.NewReceiver(edgesync.ReceiverConfig{Backend: backend, Index: idx, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	rec, err := edgesync.NewReconciler(edgesync.ReconcilerConfig{Index: idx, Backend: backend, MaxEntries: 100})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}

	h, err := NewEdgeSyncHandler(EdgeSyncHandlerConfig{
		Receiver:     recv,
		Reconciler:   rec,
		SpokeSecrets: StaticSpokeSecrets(map[string]string{testSpokeID: testSecret}),
		Replay:       security.NewNonceCache(security.HMACTimestampTolerance),
		HubID:        testHubID,
		MaxFileBytes: 8 << 20,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	// A generous global limit so tests exercise the HANDLER's per-upload cap
	// rather than fasthttp's default 4MB, which would mask it.
	app := fiber.New(fiber.Config{DisableStartupMessage: true, BodyLimit: 32 << 20})
	h.RegisterRoutes(app)

	return &syncTestRig{app: app, backend: backend}
}

// req builds a signed upload request. Fields are overridable so a test can
// tamper with exactly one thing.
type syncReq struct {
	spokeID, hubID, path, sha string
	size                      int64
	offset                    int64
	nonce                     string
	ts                        int64
	body                      []byte
	macOverride               string
	secret                    string
}

func defaultReq(body []byte) syncReq {
	nonce, _ := security.GenerateNonce()
	return syncReq{
		spokeID: testSpokeID,
		hubID:   testHubID,
		path:    testSyncPth,
		sha:     testDigest(body),
		size:    int64(len(body)),
		nonce:   nonce,
		ts:      time.Now().Unix(),
		body:    body,
		secret:  testSecret,
	}
}

func (r syncReq) do(t *testing.T, rig *syncTestRig) *http.Response {
	t.Helper()

	mac := r.macOverride
	if mac == "" {
		var err error
		mac, err = security.ComputeSyncFileHMAC(r.secret, r.nonce, r.spokeID, r.hubID, r.path, r.sha, r.ts)
		if err != nil {
			t.Fatalf("compute MAC: %v", err)
		}
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/sync/file", bytes.NewReader(r.body))
	httpReq.Header.Set(headerSpokeID, r.spokeID)
	httpReq.Header.Set(headerHubID, r.hubID)
	httpReq.Header.Set(headerPath, r.path)
	httpReq.Header.Set(headerSHA256, r.sha)
	httpReq.Header.Set(headerSize, strconv.FormatInt(r.size, 10))
	httpReq.Header.Set(headerNonce, r.nonce)
	httpReq.Header.Set(headerTS, strconv.FormatInt(r.ts, 10))
	httpReq.Header.Set(headerMAC, mac)
	if r.offset > 0 {
		httpReq.Header.Set(headerOffset, strconv.FormatInt(r.offset, 10))
	}

	resp, err := rig.app.Test(httpReq, 10_000)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return resp
}

func decodeBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return out
}

func TestEdgeSyncHandler_CommitsAuthenticatedUpload(t *testing.T) {
	rig := newSyncTestRig(t)
	body := []byte("parquet payload bytes")

	resp := defaultReq(body).do(t, rig)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200; body=%v", resp.StatusCode, decodeBody(t, resp))
	}
	got := decodeBody(t, resp)
	if got["outcome"] != string(edgesync.OutcomeCommitted) {
		t.Errorf("outcome = %v, want %q", got["outcome"], edgesync.OutcomeCommitted)
	}

	stored, err := rig.backend.Read(context.Background(), edgesync.NamespacedPath(testSpokeID, testSyncPth))
	if err != nil {
		t.Fatalf("read stored file: %v", err)
	}
	if !bytes.Equal(stored, body) {
		t.Error("stored content differs from the upload")
	}
}

func TestEdgeSyncHandler_RejectsUnauthenticated(t *testing.T) {
	rig := newSyncTestRig(t)
	body := []byte("payload")

	// Each case is a distinct way of failing authentication. All must be
	// refused — an endpoint that accepts any of these is remotely writable.
	tests := []struct {
		name   string
		mutate func(*syncReq)
	}{
		{"wrong secret", func(r *syncReq) { r.secret = "not-the-spokes-secret" }},
		{"unknown spoke", func(r *syncReq) {
			// The MAC is computed with a secret this spoke does not have
			// registered, so ONLY the registry lookup can reject it — if the
			// lookup were skipped, the request would still be well-formed and
			// internally consistent.
			r.spokeID = "rocket-99"
			r.secret = "rocket-99-own-secret"
		}},
		{"forged MAC", func(r *syncReq) { r.macOverride = hex.EncodeToString(bytes.Repeat([]byte{0}, 32)) }},
		{"tampered path after signing", func(r *syncReq) {
			mac, _ := security.ComputeSyncFileHMAC(r.secret, r.nonce, r.spokeID, r.hubID, r.path, r.sha, r.ts)
			r.macOverride = mac
			r.path = "metrics/cpu/2026/08/07/14/other.parquet"
		}},
		{"tampered digest after signing", func(r *syncReq) {
			mac, _ := security.ComputeSyncFileHMAC(r.secret, r.nonce, r.spokeID, r.hubID, r.path, r.sha, r.ts)
			r.macOverride = mac
			r.sha = testDigest([]byte("different"))
		}},
		{"expired timestamp", func(r *syncReq) {
			r.ts = time.Now().Add(-2 * security.HMACTimestampTolerance).Unix()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := defaultReq(body)
			tt.mutate(&r)
			resp := r.do(t, rig)
			if resp.StatusCode != fiber.StatusUnauthorized {
				t.Errorf("status = %d, want 401", resp.StatusCode)
			}
			// Nothing may be written for a request that failed auth.
			exists, _ := rig.backend.Exists(context.Background(), edgesync.NamespacedPath(r.spokeID, r.path))
			if exists {
				t.Error("an unauthenticated upload was written to storage")
			}
		})
	}
}

func TestEdgeSyncHandler_RejectsReplay(t *testing.T) {
	rig := newSyncTestRig(t)
	body := []byte("payload")
	r := defaultReq(body)

	if resp := r.do(t, rig); resp.StatusCode != fiber.StatusOK {
		t.Fatalf("first delivery: status = %d", resp.StatusCode)
	}

	// Replaying the identical signed request must be refused. The MAC still
	// verifies — every bound field is unchanged — so only the nonce cache
	// stops it.
	resp := r.do(t, rig)
	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("replay: status = %d, want 401", resp.StatusCode)
	}
	if got := decodeBody(t, resp); got["reason"] != "replay" {
		t.Errorf("reason = %v, want %q", got["reason"], "replay")
	}
}

func TestEdgeSyncHandler_RejectsWrongHub(t *testing.T) {
	rig := newSyncTestRig(t)
	r := defaultReq([]byte("payload"))
	r.hubID = "a-different-hub"

	// A request minted for another hub — even correctly signed with this
	// spoke's secret — must not be accepted here.
	resp := r.do(t, rig)
	if resp.StatusCode != fiber.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestEdgeSyncHandler_MapsOutcomesToStatusCodes(t *testing.T) {
	ctx := context.Background()

	t.Run("already present is 200", func(t *testing.T) {
		rig := newSyncTestRig(t)
		body := []byte("payload")
		if resp := defaultReq(body).do(t, rig); resp.StatusCode != fiber.StatusOK {
			t.Fatalf("seed: status = %d", resp.StatusCode)
		}
		// Fresh nonce, same content — the lost-ack case.
		resp := defaultReq(body).do(t, rig)
		if resp.StatusCode != fiber.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if got := decodeBody(t, resp); got["outcome"] != string(edgesync.OutcomeAlreadyPresent) {
			t.Errorf("outcome = %v, want %q", got["outcome"], edgesync.OutcomeAlreadyPresent)
		}
	})

	t.Run("conflict is 409 with the hub digest", func(t *testing.T) {
		rig := newSyncTestRig(t)
		original := []byte("the hub's content")
		if resp := defaultReq(original).do(t, rig); resp.StatusCode != fiber.StatusOK {
			t.Fatalf("seed: status = %d", resp.StatusCode)
		}

		resp := defaultReq([]byte("different content")).do(t, rig)
		if resp.StatusCode != fiber.StatusConflict {
			t.Fatalf("status = %d, want 409", resp.StatusCode)
		}
		got := decodeBody(t, resp)
		if got["their_sha256"] != testDigest(original) {
			t.Errorf("their_sha256 = %v, want the hub's digest", got["their_sha256"])
		}

		// The original must survive — a conflict never overwrites.
		stored, err := rig.backend.Read(ctx, edgesync.NamespacedPath(testSpokeID, testSyncPth))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(stored, original) {
			t.Error("a conflicting upload overwrote the hub's content")
		}
	})

	t.Run("checksum mismatch is 422 and stores nothing", func(t *testing.T) {
		rig := newSyncTestRig(t)
		declared := []byte("the declared content")
		r := defaultReq(declared)
		r.body = []byte("the actual content!!") // same length, different bytes

		resp := r.do(t, rig)
		if resp.StatusCode != fiber.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want 422", resp.StatusCode)
		}
		if exists, _ := rig.backend.Exists(ctx, edgesync.NamespacedPath(testSpokeID, testSyncPth)); exists {
			t.Error("content that failed verification was stored")
		}
	})

	t.Run("partial is 206 with the accepted offset", func(t *testing.T) {
		rig := newSyncTestRig(t)
		full := []byte("the complete parquet payload for this file")
		r := defaultReq(full)
		r.body = full[:12] // link dropped mid-stream

		resp := r.do(t, rig)
		if resp.StatusCode != fiber.StatusPartialContent {
			t.Fatalf("status = %d, want 206", resp.StatusCode)
		}
		got := decodeBody(t, resp)
		if got["bytes_accepted"] != float64(12) {
			t.Errorf("bytes_accepted = %v, want 12", got["bytes_accepted"])
		}
	})
}

func TestEdgeSyncHandler_ResumesFromOffset(t *testing.T) {
	rig := newSyncTestRig(t)
	full := []byte("the complete parquet payload for this file")

	first := defaultReq(full)
	first.body = full[:12]
	resp := first.do(t, rig)
	if resp.StatusCode != fiber.StatusPartialContent {
		t.Fatalf("partial: status = %d, want 206", resp.StatusCode)
	}

	// The spoke resumes, sending only the tail.
	second := defaultReq(full)
	second.body = full[12:]
	second.offset = 12
	resp2 := second.do(t, rig)
	if resp2.StatusCode != fiber.StatusOK {
		t.Fatalf("resume: status = %d, want 200; body=%v", resp2.StatusCode, decodeBody(t, resp2))
	}

	stored, err := rig.backend.Read(context.Background(), edgesync.NamespacedPath(testSpokeID, testSyncPth))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(stored, full) {
		t.Error("the resumed file does not match the original")
	}
}

func TestEdgeSyncHandler_RejectsMalformedRequests(t *testing.T) {
	rig := newSyncTestRig(t)
	body := []byte("payload")

	tests := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{"no spoke ID", func(r *http.Request) { r.Header.Del(headerSpokeID) }},
		{"no path", func(r *http.Request) { r.Header.Del(headerPath) }},
		{"no digest", func(r *http.Request) { r.Header.Del(headerSHA256) }},
		{"no nonce", func(r *http.Request) { r.Header.Del(headerNonce) }},
		{"no MAC", func(r *http.Request) { r.Header.Del(headerMAC) }},
		{"no size", func(r *http.Request) { r.Header.Del(headerSize) }},
		{"non-numeric size", func(r *http.Request) { r.Header.Set(headerSize, "big") }},
		{"negative size", func(r *http.Request) { r.Header.Set(headerSize, "-1") }},
		{"non-numeric offset", func(r *http.Request) { r.Header.Set(headerOffset, "start") }},
		{"no timestamp", func(r *http.Request) { r.Header.Del(headerTS) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := defaultReq(body)
			mac, _ := security.ComputeSyncFileHMAC(r.secret, r.nonce, r.spokeID, r.hubID, r.path, r.sha, r.ts)

			httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/sync/file", bytes.NewReader(body))
			httpReq.Header.Set(headerSpokeID, r.spokeID)
			httpReq.Header.Set(headerHubID, r.hubID)
			httpReq.Header.Set(headerPath, r.path)
			httpReq.Header.Set(headerSHA256, r.sha)
			httpReq.Header.Set(headerSize, strconv.FormatInt(r.size, 10))
			httpReq.Header.Set(headerNonce, r.nonce)
			httpReq.Header.Set(headerTS, strconv.FormatInt(r.ts, 10))
			httpReq.Header.Set(headerMAC, mac)
			tt.mutate(httpReq)

			resp, err := rig.app.Test(httpReq, 10_000)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if resp.StatusCode == fiber.StatusOK {
				t.Errorf("a malformed request was accepted (status 200)")
			}
		})
	}
}

func TestEdgeSyncHandler_RejectsPathTraversal(t *testing.T) {
	rig := newSyncTestRig(t)
	body := []byte("payload")

	// The path is signed, so this models a COMPROMISED spoke — one holding a
	// valid secret and deliberately trying to write outside its namespace.
	// The HMAC proves who is asking, not where they may write.
	for _, p := range []string{
		"../../../etc/passwd.parquet",
		"metrics/../../escape.parquet",
		"/absolute/path.parquet",
		".sync-staging/rocket-02/steal.parquet",
	} {
		t.Run(p, func(t *testing.T) {
			r := defaultReq(body)
			r.path = p
			resp := r.do(t, rig)
			if resp.StatusCode == fiber.StatusOK {
				t.Errorf("path %q was accepted", p)
			}
		})
	}
}

func TestNewEdgeSyncHandler_RequiresSecurityDependencies(t *testing.T) {
	dir, _ := os.MkdirTemp("", "api-sync-*")
	t.Cleanup(func() { os.RemoveAll(dir) })
	backend, _ := storage.NewLocalBackend(dir, zerolog.Nop())
	t.Cleanup(func() { backend.Close() })
	recv, _ := edgesync.NewReceiver(edgesync.ReceiverConfig{Backend: backend, Logger: zerolog.Nop()})

	base := EdgeSyncHandlerConfig{
		Receiver:     recv,
		Reconciler:   mustReconciler(t),
		SpokeSecrets: StaticSpokeSecrets(map[string]string{testSpokeID: testSecret}),
		Replay:       security.NewNonceCache(security.HMACTimestampTolerance),
		HubID:        testHubID,
		MaxFileBytes: 8 << 20,
		Logger:       zerolog.Nop(),
	}

	// A missing security dependency must be a hard startup error. Defaulting
	// any of these to a no-op would silently disable authentication.
	tests := []struct {
		name   string
		mutate func(*EdgeSyncHandlerConfig)
	}{
		{"no receiver", func(c *EdgeSyncHandlerConfig) { c.Receiver = nil }},
		{"no reconciler", func(c *EdgeSyncHandlerConfig) { c.Reconciler = nil }},
		{"no spoke secrets", func(c *EdgeSyncHandlerConfig) { c.SpokeSecrets = nil }},
		{"no replay guard", func(c *EdgeSyncHandlerConfig) { c.Replay = nil }},
		{"no hub ID", func(c *EdgeSyncHandlerConfig) { c.HubID = "" }},
		{"no upload cap", func(c *EdgeSyncHandlerConfig) { c.MaxFileBytes = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			if _, err := NewEdgeSyncHandler(cfg); err == nil {
				t.Error("handler was constructed without a required dependency")
			}
		})
	}
}

func TestStaticSpokeSecrets(t *testing.T) {
	ctx := context.Background()
	source := map[string]string{"rocket-01": "secret-one", "rocket-02": ""}
	lookup := StaticSpokeSecrets(source)

	if s, ok := lookup(ctx, "rocket-01"); !ok || s != "secret-one" {
		t.Errorf("known spoke: (%q, %v), want (secret-one, true)", s, ok)
	}
	if _, ok := lookup(ctx, "rocket-99"); ok {
		t.Error("an unregistered spoke resolved to a secret")
	}
	// An empty secret must not authenticate — it would make an unconfigured
	// spoke interoperable with anyone computing a MAC over an empty key.
	if _, ok := lookup(ctx, "rocket-02"); ok {
		t.Error("a spoke with an empty secret was accepted")
	}

	// Mutating the caller's map must not change which spokes authenticate.
	source["rocket-99"] = "injected"
	if _, ok := lookup(ctx, "rocket-99"); ok {
		t.Error("mutating the source map added a spoke after construction")
	}
}

func TestEdgeSyncHandler_ConcurrentUploadsAreIsolated(t *testing.T) {
	rig := newSyncTestRig(t)

	// Distinct spokes and files, uploaded concurrently — the hub must keep
	// them apart. Run under -race for this to mean anything.
	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			body := []byte(fmt.Sprintf("payload for file %d", i))
			r := defaultReq(body)
			r.path = fmt.Sprintf("metrics/cpu/2026/08/07/14/cpu_%d.parquet", i)
			mac, err := security.ComputeSyncFileHMAC(r.secret, r.nonce, r.spokeID, r.hubID, r.path, r.sha, r.ts)
			if err != nil {
				errs <- err
				return
			}
			r.macOverride = mac

			httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/sync/file", bytes.NewReader(r.body))
			httpReq.Header.Set(headerSpokeID, r.spokeID)
			httpReq.Header.Set(headerHubID, r.hubID)
			httpReq.Header.Set(headerPath, r.path)
			httpReq.Header.Set(headerSHA256, r.sha)
			httpReq.Header.Set(headerSize, strconv.FormatInt(r.size, 10))
			httpReq.Header.Set(headerNonce, r.nonce)
			httpReq.Header.Set(headerTS, strconv.FormatInt(r.ts, 10))
			httpReq.Header.Set(headerMAC, mac)

			resp, err := rig.app.Test(httpReq, 10_000)
			if err != nil {
				errs <- err
				return
			}
			if resp.StatusCode != fiber.StatusOK {
				errs <- fmt.Errorf("file %d: status %d", i, resp.StatusCode)
				return
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent upload: %v", err)
		}
	}

	for i := 0; i < n; i++ {
		p := edgesync.NamespacedPath(testSpokeID, fmt.Sprintf("metrics/cpu/2026/08/07/14/cpu_%d.parquet", i))
		got, err := rig.backend.Read(context.Background(), p)
		if err != nil {
			t.Errorf("read file %d: %v", i, err)
			continue
		}
		if want := fmt.Sprintf("payload for file %d", i); string(got) != want {
			t.Errorf("file %d holds %q, want %q", i, got, want)
		}
	}
}

func TestEdgeSyncHandler_UnregisteredSpokeIsRejectedByTheRegistry(t *testing.T) {
	rig := newSyncTestRig(t)
	body := []byte("payload")

	// Isolates the registry lookup from the MAC check. An unregistered spoke
	// signing with the EMPTY secret produces a MAC that would verify if the
	// handler fell back to an empty secret when the lookup fails — so only a
	// genuine registry rejection stops this request.
	r := defaultReq(body)
	r.spokeID = "rocket-99"
	r.secret = ""

	resp := r.do(t, rig)
	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 — an unregistered spoke authenticated", resp.StatusCode)
	}

	// The rejection must not disclose whether the spoke is unknown versus
	// merely mis-signed; either would let an attacker enumerate spoke IDs.
	if got := decodeBody(t, resp); got["reason"] != nil {
		t.Errorf("reason = %v, want none — an unknown spoke must be indistinguishable from a bad MAC", got["reason"])
	}

	if exists, _ := rig.backend.Exists(context.Background(), edgesync.NamespacedPath(r.spokeID, r.path)); exists {
		t.Error("an unregistered spoke wrote to storage")
	}
}

func TestEdgeSyncHandler_RejectsOversizedUploads(t *testing.T) {
	rig := newSyncTestRig(t)

	// The rig caps uploads at 8MiB. Because StreamRequestBody=false buffers the
	// whole body before routing, an unbounded cap would let anyone who can
	// reach the port pin memory without holding a token or a spoke secret.
	big := bytes.Repeat([]byte("x"), 9<<20) // above the 8MiB handler cap, below the 32MiB app limit
	r := defaultReq(big)

	resp := r.do(t, rig)
	if resp.StatusCode != fiber.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	if exists, _ := rig.backend.Exists(context.Background(), edgesync.NamespacedPath(testSpokeID, testSyncPth)); exists {
		t.Error("an oversized upload was written to storage")
	}
}

func TestEdgeSyncHandler_RejectsOversizedDeclaredSize(t *testing.T) {
	rig := newSyncTestRig(t)

	// A small body with a huge declared size must also be refused: the
	// declared size is what the receiver uses to bound its reads and to size
	// the staging write, so an unbounded value is a resource claim regardless
	// of how many bytes actually arrive.
	small := []byte("tiny")
	r := defaultReq(small)
	r.size = 100 << 20

	resp := r.do(t, rig)
	if resp.StatusCode != fiber.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
}

// failingRegistrarBackend is fine; the manifest write is what fails.
func TestEdgeSyncHandler_HubSideFailureIs503NotBadRequest(t *testing.T) {
	dir, err := os.MkdirTemp("", "api-sync-503-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	// Models a Raft election or quorum loss: registration fails transiently.
	recv, err := edgesync.NewReceiver(edgesync.ReceiverConfig{
		Backend: backend,
		Logger:  zerolog.Nop(),
		RegisterFile: func(context.Context, *edgesync.ReceivedFile) error {
			return errors.New("raft: leadership lost")
		},
	})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}

	rec2, err := edgesync.NewReconciler(edgesync.ReconcilerConfig{Index: newTestAPIHubIndex(t), Backend: backend, MaxEntries: 100})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}
	h, err := NewEdgeSyncHandler(EdgeSyncHandlerConfig{
		Receiver:     recv,
		Reconciler:   rec2,
		SpokeSecrets: StaticSpokeSecrets(map[string]string{testSpokeID: testSecret}),
		Replay:       security.NewNonceCache(security.HMACTimestampTolerance),
		HubID:        testHubID,
		MaxFileBytes: 8 << 20,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	app := fiber.New(fiber.Config{DisableStartupMessage: true, BodyLimit: 32 << 20})
	h.RegisterRoutes(app)
	rig := &syncTestRig{app: app, backend: backend}

	resp := defaultReq([]byte("payload")).do(t, rig)

	// 400 would tell the spoke its request was malformed, so it would either
	// give up or keep retrying something it believes is broken. A transient
	// hub-side failure is exactly what it SHOULD retry.
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	got := decodeBody(t, resp)
	if got["reason"] != "hub_unavailable" {
		t.Errorf("reason = %v, want %q", got["reason"], "hub_unavailable")
	}
	// The message must not leak hub internals to a spoke.
	if msg, _ := got["error"].(string); strings.Contains(msg, "raft") {
		t.Errorf("error message leaks hub internals: %q", msg)
	}
}

// newTestAPIHubIndex builds a hub index on a temp SQLite file.
func newTestAPIHubIndex(t *testing.T) *edgesync.HubIndex {
	t.Helper()
	f, err := os.CreateTemp("", "api-hub-index-*.db")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	f.Close()
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatalf("open sqlite: %v", err)
	}
	idx, err := edgesync.NewHubIndex(db, zerolog.Nop())
	if err != nil {
		db.Close()
		os.Remove(f.Name())
		t.Fatalf("hub index: %v", err)
	}
	t.Cleanup(func() { db.Close(); os.Remove(f.Name()) })
	return idx
}

func mustReconciler(t *testing.T) *edgesync.Reconciler {
	t.Helper()
	dir, err := os.MkdirTemp("", "api-recon-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	b, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	r, err := edgesync.NewReconciler(edgesync.ReconcilerConfig{Index: newTestAPIHubIndex(t), Backend: b, MaxEntries: 100})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}
	return r
}

// reconcileReq builds a signed reconcile request.
type reconcileReqSpec struct {
	spokeID, hubID string
	nonce          string
	ts             int64
	body           []byte
	macOverride    string
	secret         string
}

func defaultReconcileReq(entries []edgesync.ReconcileEntry) reconcileReqSpec {
	nonce, _ := security.GenerateNonce()
	body, _ := json.Marshal(map[string]any{"entries": entries})
	return reconcileReqSpec{
		spokeID: testSpokeID,
		hubID:   testHubID,
		nonce:   nonce,
		ts:      time.Now().Unix(),
		body:    body,
		secret:  testSecret,
	}
}

func (r reconcileReqSpec) do(t *testing.T, rig *syncTestRig) *http.Response {
	t.Helper()
	mac := r.macOverride
	if mac == "" {
		var err error
		mac, err = security.ComputeSyncReconcileHMAC(r.secret, r.nonce, r.spokeID, r.hubID, r.body, r.ts)
		if err != nil {
			t.Fatalf("compute MAC: %v", err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sync/reconcile", bytes.NewReader(r.body))
	req.Header.Set(headerSpokeID, r.spokeID)
	req.Header.Set(headerHubID, r.hubID)
	req.Header.Set(headerNonce, r.nonce)
	req.Header.Set(headerTS, strconv.FormatInt(r.ts, 10))
	req.Header.Set(headerMAC, mac)
	req.Header.Set("Content-Type", "application/json")

	resp, err := rig.app.Test(req, 10_000)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return resp
}

func TestEdgeSyncHandler_ReconcileRoundTrip(t *testing.T) {
	rig := newSyncTestRig(t)

	// Upload one file so the hub has something to report as present.
	body := []byte("parquet payload")
	if resp := defaultReq(body).do(t, rig); resp.StatusCode != fiber.StatusOK {
		t.Fatalf("seed upload: status %d", resp.StatusCode)
	}

	entries := []edgesync.ReconcileEntry{
		{Path: testSyncPth, SHA256: testDigest(body)},
		{Path: "metrics/cpu/2026/08/07/14/never_sent.parquet", SHA256: testDigest([]byte("other"))},
	}
	resp := defaultReconcileReq(entries).do(t, rig)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200; body=%v", resp.StatusCode, decodeBody(t, resp))
	}

	got := decodeBody(t, resp)
	present, _ := got["present"].([]any)
	missing, _ := got["missing"].([]any)
	if len(present) != 1 || present[0] != testSyncPth {
		t.Errorf("present = %v, want the uploaded file", present)
	}
	if len(missing) != 1 {
		t.Errorf("missing = %v, want the file never sent", missing)
	}
	// Empty slices rather than null, so a spoke need not special-case the field.
	if _, ok := got["conflicts"].([]any); !ok {
		t.Errorf("conflicts = %v, want an empty array not null", got["conflicts"])
	}
}

func TestEdgeSyncHandler_ReconcileRejectsUnauthenticated(t *testing.T) {
	rig := newSyncTestRig(t)
	entries := []edgesync.ReconcileEntry{{Path: testSyncPth, SHA256: testDigest([]byte("x"))}}

	tests := []struct {
		name   string
		mutate func(*reconcileReqSpec)
	}{
		{"wrong secret", func(r *reconcileReqSpec) { r.secret = "not-the-secret" }},
		{"unknown spoke", func(r *reconcileReqSpec) { r.spokeID = "rocket-99"; r.secret = "its-own-secret" }},
		{"expired timestamp", func(r *reconcileReqSpec) {
			r.ts = time.Now().Add(-2 * security.HMACTimestampTolerance).Unix()
		}},
		{"body swapped after signing", func(r *reconcileReqSpec) {
			// The MAC binds a digest of the body precisely so a replayed
			// request cannot substitute a different path list and use the hub
			// as an oracle for what data exists.
			mac, _ := security.ComputeSyncReconcileHMAC(r.secret, r.nonce, r.spokeID, r.hubID, r.body, r.ts)
			r.macOverride = mac
			r.body, _ = json.Marshal(map[string]any{"entries": []edgesync.ReconcileEntry{
				{Path: "metrics/secret/2026/08/07/14/probe.parquet", SHA256: testDigest([]byte("probe"))},
			}})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := defaultReconcileReq(entries)
			tt.mutate(&r)
			if resp := r.do(t, rig); resp.StatusCode != fiber.StatusUnauthorized {
				t.Errorf("status = %d, want 401", resp.StatusCode)
			}
		})
	}
}

func TestEdgeSyncHandler_ReconcileRejectsReplay(t *testing.T) {
	rig := newSyncTestRig(t)
	r := defaultReconcileReq([]edgesync.ReconcileEntry{{Path: testSyncPth, SHA256: testDigest([]byte("x"))}})

	if resp := r.do(t, rig); resp.StatusCode != fiber.StatusOK {
		t.Fatalf("first: status %d", resp.StatusCode)
	}
	resp := r.do(t, rig)
	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("replay: status = %d, want 401", resp.StatusCode)
	}
	if got := decodeBody(t, resp); got["reason"] != "replay" {
		t.Errorf("reason = %v, want replay", got["reason"])
	}
}

func TestEdgeSyncHandler_ReconcileRejectsOversizedBatch(t *testing.T) {
	rig := newSyncTestRig(t)

	// The rig caps at 100 entries. An unbounded batch is a pre-auth memory
	// claim, since the body is buffered before routing.
	entries := make([]edgesync.ReconcileEntry, 101)
	for i := range entries {
		entries[i] = edgesync.ReconcileEntry{
			Path:   fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%d.parquet", i),
			SHA256: testDigest([]byte(fmt.Sprint(i))),
		}
	}

	resp := defaultReconcileReq(entries).do(t, rig)
	if resp.StatusCode != fiber.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	if got := decodeBody(t, resp); got["max_entries"] == nil {
		t.Error("the 413 response does not tell the spoke the limit it must page under")
	}
}

func TestEdgeSyncHandler_ReconcileRejectsMalformedBody(t *testing.T) {
	rig := newSyncTestRig(t)

	r := defaultReconcileReq(nil)
	r.body = []byte("{not json")
	// The MAC must still be computed over the malformed bytes, so this
	// exercises body parsing rather than authentication.
	mac, err := security.ComputeSyncReconcileHMAC(r.secret, r.nonce, r.spokeID, r.hubID, r.body, r.ts)
	if err != nil {
		t.Fatalf("mac: %v", err)
	}
	r.macOverride = mac

	if resp := r.do(t, rig); resp.StatusCode != fiber.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestEdgeSyncHandler_ReconcileRejectsMaliciousPaths(t *testing.T) {
	rig := newSyncTestRig(t)

	// Signed by a legitimate spoke, so this models a COMPROMISED edge trying
	// to probe outside its namespace. The MAC proves who is asking, not that
	// what they ask about is theirs.
	for _, p := range []string{
		"../../../etc/passwd.parquet",
		"/absolute/path.parquet",
		".sync-staging/rocket-02/steal.parquet",
	} {
		t.Run(p, func(t *testing.T) {
			entries := []edgesync.ReconcileEntry{{Path: p, SHA256: testDigest([]byte("x"))}}
			if resp := defaultReconcileReq(entries).do(t, rig); resp.StatusCode == fiber.StatusOK {
				t.Errorf("path %q was accepted", p)
			}
		})
	}
}

func TestEdgeSyncHandler_ReconcileRejectsOversizedBodyBeforeAuth(t *testing.T) {
	rig := newSyncTestRig(t)

	// The Content-Length guard is the ONLY bound applied before
	// authentication, so it must be exercised by a body that actually exceeds
	// it. An earlier version of this suite could not: with a 100-entry cap the
	// byte threshold was 25,600 while a 101-entry body was only ~12KB, so the
	// guard was unreachable and deleting it left every test green.
	//
	// Padding the paths pushes the body past the byte threshold while keeping
	// the entry count under the cap, isolating the pre-auth guard.
	pad := strings.Repeat("p", 400)
	entries := make([]edgesync.ReconcileEntry, 90)
	for i := range entries {
		entries[i] = edgesync.ReconcileEntry{
			Path:   fmt.Sprintf("metrics/cpu/2026/08/07/14/%s_%d.parquet", pad, i),
			SHA256: testDigest([]byte(fmt.Sprint(i))),
		}
	}

	r := defaultReconcileReq(entries)
	if len(r.body) <= 90*256 {
		t.Fatalf("test body is %d bytes, not above the %d-byte guard — the guard would not be exercised",
			len(r.body), 90*256)
	}

	resp := r.do(t, rig)
	if resp.StatusCode != fiber.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 — an oversized body was accepted before auth", resp.StatusCode)
	}
}
