package edgesync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
)

// HTTPTransport pushes files to a hub over HTTPS.
//
// The v1 transport. Everything protocol-shaped — the identity rule, the
// outcome taxonomy, resume — lives above this in the interface, so an S3-relay
// or sneakernet transport reuses it unchanged and only the wire format differs.
type HTTPTransport struct {
	baseURL  string
	spokeID  string
	secret   string
	apiToken string
	client   *http.Client
}

// HTTPTransportConfig configures an HTTPTransport.
type HTTPTransportConfig struct {
	// BaseURL is the hub's root, e.g. https://ground-station.example.com.
	BaseURL string

	// SpokeID is this spoke's registered identity, bound into every MAC.
	SpokeID string

	// Secret is the hub-issued shared secret. Never logged, never persisted
	// by this package — it arrives from the environment and stays in memory.
	Secret string

	// APIToken is an Arc API token for the hub's token middleware, which
	// gates /api/v1/sync at write level ahead of the per-spoke HMAC. Optional:
	// empty means no Authorization header, which only works against a hub
	// running with auth disabled. Same handling discipline as Secret.
	APIToken string

	// Timeout bounds a single request. Zero means 30 minutes, matching the
	// hub's receive timeout: a large Parquet file over a constrained link is
	// the expected case, not an anomaly.
	Timeout time.Duration

	// Client overrides the HTTP client, for tests and for callers that need a
	// custom TLS config.
	Client *http.Client
}

// NewHTTPTransport validates configuration and returns a ready transport.
func NewHTTPTransport(cfg HTTPTransportConfig) (*HTTPTransport, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("edgesync: HTTP transport requires a hub URL")
	}
	if err := validateSpokeID(cfg.SpokeID); err != nil {
		return nil, fmt.Errorf("edgesync: HTTP transport spoke ID: %w", err)
	}
	if cfg.Secret == "" {
		// Without a secret every request would be rejected, and the failure
		// would look like a hub problem rather than a missing credential.
		return nil, errors.New("edgesync: HTTP transport requires a spoke secret")
	}

	client := cfg.Client
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Minute
		}
		client = &http.Client{Timeout: timeout}
	}

	return &HTTPTransport{
		baseURL:  strings.TrimRight(cfg.BaseURL, "/"),
		spokeID:  cfg.SpokeID,
		secret:   cfg.Secret,
		apiToken: cfg.APIToken,
		client:   client,
	}, nil
}

// ReconcileTooLargeError reports that the hub refused a reconcile batch with
// 413. MaxEntries is the hub's advertised entry cap, or 0 when the refusal
// came from a byte limit that advertises none (the route-level body cap).
// The agent reacts by splitting the page and retrying, so this error never
// fails a pass on its own.
type ReconcileTooLargeError struct {
	MaxEntries int
}

func (e *ReconcileTooLargeError) Error() string {
	if e.MaxEntries > 0 {
		return fmt.Sprintf("edgesync: hub refused reconcile batch as too large (max_entries=%d)", e.MaxEntries)
	}
	return "edgesync: hub refused reconcile batch as too large"
}

// Reconcile asks the hub which of the pending files it already holds.
func (t *HTTPTransport) Reconcile(ctx context.Context, hubID string, pending []*LedgerEntry) (*ReconcileResult, error) {
	entries := make([]ReconcileEntry, 0, len(pending))
	for _, e := range pending {
		entries = append(entries, ReconcileEntry{
			Path:      e.Path,
			SHA256:    e.SHA256,
			SizeBytes: e.SizeBytes,
		})
	}

	body, err := json.Marshal(map[string]any{"entries": entries})
	if err != nil {
		return nil, fmt.Errorf("edgesync: encode reconcile body: %w", err)
	}

	nonce, err := security.GenerateNonce()
	if err != nil {
		return nil, fmt.Errorf("edgesync: generate nonce: %w", err)
	}
	ts := time.Now().Unix()

	// The MAC binds a digest of the body, so a captured request cannot be
	// replayed with a substituted path list to probe what the hub holds.
	mac, err := security.ComputeSyncReconcileHMAC(t.secret, nonce, t.spokeID, hubID, body, ts)
	if err != nil {
		return nil, fmt.Errorf("edgesync: sign reconcile: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+"/api/v1/sync/reconcile", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("edgesync: build reconcile request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	t.setAuthHeaders(req, hubID, nonce, ts, mac)

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("edgesync: reconcile request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		// Two producers on the hub: the reconciler's entry cap (body carries
		// max_entries) and the route-level byte limit (no max_entries). Both
		// are typed so the agent splits the page instead of failing the pass.
		var refusal struct {
			MaxEntries int `json:"max_entries"`
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = json.Unmarshal(body, &refusal)
		return nil, &ReconcileTooLargeError{MaxEntries: refusal.MaxEntries}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, t.statusError(resp, "reconcile")
	}

	var out struct {
		Missing   []string   `json:"missing"`
		Present   []string   `json:"present"`
		Conflicts []Conflict `json:"conflicts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("edgesync: decode reconcile response: %w", err)
	}

	return &ReconcileResult{
		Missing:   out.Missing,
		Present:   out.Present,
		Conflicts: out.Conflicts,
	}, nil
}

// PutFile streams one file to the hub, resuming from offset.
func (t *HTTPTransport) PutFile(ctx context.Context, hubID string, entry *LedgerEntry, body io.Reader, offset int64) (*PutResult, error) {
	if entry == nil {
		return nil, errors.New("edgesync: PutFile requires an entry")
	}

	nonce, err := security.GenerateNonce()
	if err != nil {
		return nil, fmt.Errorf("edgesync: generate nonce: %w", err)
	}
	ts := time.Now().Unix()

	mac, err := security.ComputeSyncFileHMAC(t.secret, nonce, t.spokeID, hubID, entry.Path, entry.SHA256, ts)
	if err != nil {
		return nil, fmt.Errorf("edgesync: sign file transfer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+"/api/v1/sync/file", body)
	if err != nil {
		return nil, fmt.Errorf("edgesync: build file request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(headerSyncPath, entry.Path)
	req.Header.Set(headerSyncSHA256, entry.SHA256)
	req.Header.Set(headerSyncSize, strconv.FormatInt(entry.SizeBytes, 10))
	if offset > 0 {
		req.Header.Set(headerSyncOffset, strconv.FormatInt(offset, 10))
	}
	t.setAuthHeaders(req, hubID, nonce, ts, mac)

	// Content-Length so the hub's pre-auth size check can reject an oversized
	// upload before buffering it. Without this a resumed transfer of unknown
	// length would look unbounded.
	req.ContentLength = entry.SizeBytes - offset

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("edgesync: file request: %w", err)
	}
	defer resp.Body.Close()

	return t.decodePutResponse(resp, entry)
}

// decodePutResponse maps the hub's status code to a PutResult.
func (t *HTTPTransport) decodePutResponse(resp *http.Response, entry *LedgerEntry) (*PutResult, error) {
	var out struct {
		Outcome       string `json:"outcome"`
		BytesAccepted int64  `json:"bytes_accepted"`
		TheirSHA256   string `json:"their_sha256"`
		Reason        string `json:"reason"`
		Error         string `json:"error"`
	}
	// A body is expected on every documented status, but a proxy or a
	// truncated response can produce an empty one — decode failures are
	// tolerated here so the status code still decides the outcome.
	_ = json.NewDecoder(resp.Body).Decode(&out)

	switch resp.StatusCode {
	case http.StatusOK:
		outcome := OutcomeCommitted
		if out.Outcome == string(OutcomeAlreadyPresent) {
			outcome = OutcomeAlreadyPresent
		}
		accepted := out.BytesAccepted
		if accepted == 0 {
			// An older or terser hub may omit it; the file is committed, so
			// the whole size is what was accepted.
			accepted = entry.SizeBytes
		}
		return &PutResult{Outcome: outcome, BytesAccepted: accepted}, nil

	case http.StatusPartialContent:
		return &PutResult{Outcome: OutcomePartial, BytesAccepted: out.BytesAccepted}, nil

	case http.StatusConflict:
		// 409 covers two different things. A content disagreement carries the
		// hub's digest and is terminal; a refused resume is recoverable by
		// restarting from zero, so it must not be mistaken for the former.
		if out.Reason == "resume_unsupported" {
			return nil, fmt.Errorf("edgesync: hub cannot resume: %s", out.Error)
		}
		return &PutResult{Outcome: OutcomeConflict, TheirSHA256: out.TheirSHA256}, nil

	case http.StatusUnprocessableEntity:
		return &PutResult{Outcome: OutcomeChecksumMismatch}, nil

	case http.StatusTooManyRequests:
		return &PutResult{
			Outcome:    OutcomeBackpressure,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}, nil

	default:
		return nil, t.statusError(resp, "file transfer")
	}
}

// setAuthHeaders applies the headers every sync request carries.
func (t *HTTPTransport) setAuthHeaders(req *http.Request, hubID, nonce string, ts int64, mac string) {
	// Two layers, matching the hub's mount order: the Arc API token satisfies
	// the token middleware ahead of the routes, then the HMAC headers bind
	// this request to a spoke identity and its content.
	if t.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+t.apiToken)
	}
	req.Header.Set(headerSyncSpokeID, t.spokeID)
	req.Header.Set(headerSyncHubID, hubID)
	req.Header.Set(headerSyncNonce, nonce)
	req.Header.Set(headerSyncTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(headerSyncMAC, mac)
}

// statusError builds an error for an unexpected status.
//
// The response body is read but bounded: a hub returning an unexpected status
// is already misbehaving, and an unbounded read here would let it exhaust the
// spoke — the machine least able to absorb it.
func (t *HTTPTransport) statusError(resp *http.Response, op string) error {
	const maxErrorBody = 4 << 10
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = resp.Status
	}
	// The two failures whose remedy is on THIS box, not the hub: the hub's
	// token middleware rejected the request before the sync handler ran.
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		if t.apiToken == "" {
			msg += " (hub requires an Arc API token; set ARC_EDGE_SYNC_HUB_TOKEN on this spoke)"
		} else {
			msg += " (the ARC_EDGE_SYNC_HUB_TOKEN on this spoke was rejected by the hub, or the request MAC failed; check the token, then the spoke secret and hub_id)"
		}
	case http.StatusForbidden:
		msg += " (the ARC_EDGE_SYNC_HUB_TOKEN on this spoke lacks write permission on the hub)"
	}
	return fmt.Errorf("edgesync: %s failed with %d: %s", op, resp.StatusCode, msg)
}

// parseRetryAfter reads a Retry-After header, in seconds.
//
// Falls back to a second rather than zero: a zero delay would busy-loop
// against a hub that has just said it is overloaded.
func parseRetryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Second
}

// Sync request headers. Duplicated from the api package rather than imported:
// this package must not depend on the HTTP layer, and the api package already
// imports this one.
const (
	headerSyncSpokeID   = "X-Arc-Spoke-ID"
	headerSyncHubID     = "X-Arc-Sync-HubID"
	headerSyncPath      = "X-Arc-Sync-Path"
	headerSyncSHA256    = "X-Arc-Sync-SHA256"
	headerSyncSize      = "X-Arc-Sync-Size"
	headerSyncOffset    = "X-Arc-Sync-Offset"
	headerSyncNonce     = "X-Arc-Sync-Nonce"
	headerSyncTimestamp = "X-Arc-Sync-Timestamp"
	headerSyncMAC       = "X-Arc-Sync-MAC"
)

// Compile-time check that HTTPTransport satisfies the interface.
var _ SyncTransport = (*HTTPTransport)(nil)
