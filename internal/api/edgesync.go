package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Sync request headers. The spoke sends its native path; the hub namespaces it.
const (
	headerSpokeID = "X-Arc-Spoke-ID"
	headerHubID   = "X-Arc-Sync-HubID"
	headerPath    = "X-Arc-Sync-Path"
	headerSHA256  = "X-Arc-Sync-SHA256"
	headerSize    = "X-Arc-Sync-Size"
	headerOffset  = "X-Arc-Sync-Offset"
	headerNonce   = "X-Arc-Sync-Nonce"
	headerTS      = "X-Arc-Sync-Timestamp"
	headerMAC     = "X-Arc-Sync-MAC"
)

// receiveTimeout bounds a single file transfer. Generous because a large
// Parquet file over a constrained link is the expected case, not an anomaly.
const receiveTimeout = 30 * time.Minute

// reconcileTimeout bounds a discovery request. Much shorter than a transfer:
// reconcile is a bounded SQLite lookup, so anything slow is a symptom rather
// than a large file.
const reconcileTimeout = 2 * time.Minute

// SpokeSecretLookup returns the shared secret registered for a spoke.
//
// Per-spoke secrets are the point: revoking one edge must not re-key the
// fleet. Returning ok=false rejects the request without disclosing whether the
// spoke is unknown or merely disabled.
type SpokeSecretLookup func(ctx context.Context, spokeID string) (secret string, ok bool)

// EdgeSyncHandler serves the hub side of edge-to-cloud sync.
type EdgeSyncHandler struct {
	receiver     *edgesync.Receiver
	reconciler   *edgesync.Reconciler
	lookup       SpokeSecretLookup
	replay       security.ReplayGuard
	authManager  *auth.AuthManager
	hubID        string
	maxFileBytes int64
	logger       zerolog.Logger
}

// EdgeSyncHandlerConfig configures the handler.
type EdgeSyncHandlerConfig struct {
	Receiver *edgesync.Receiver

	// Reconciler answers batch discovery. Required — without it a spoke
	// returning from a long outage would have to probe file by file.
	Reconciler *edgesync.Reconciler

	// SpokeSecrets resolves a spoke's shared secret. Required — without it
	// every request would be unauthenticated.
	SpokeSecrets SpokeSecretLookup

	// Replay consumes nonces so a captured request cannot be replayed inside
	// the freshness window. Required.
	Replay security.ReplayGuard

	// HubID identifies this hub. It is bound into the request MAC, so a
	// request minted for another hub sharing the spoke's secret is rejected.
	HubID string

	// MaxFileBytes caps a single upload. Required and must be > 0.
	MaxFileBytes int64

	AuthManager *auth.AuthManager
	Logger      zerolog.Logger
}

// NewEdgeSyncHandler validates configuration and returns a ready handler.
//
// Every dependency that carries a security property is required rather than
// optional: a nil secret lookup or replay guard would silently downgrade
// authentication, and a handler that half-works is worse than one that refuses
// to start.
func NewEdgeSyncHandler(cfg EdgeSyncHandlerConfig) (*EdgeSyncHandler, error) {
	if cfg.Receiver == nil {
		return nil, errors.New("edgesync handler: receiver is required")
	}
	if cfg.Reconciler == nil {
		return nil, errors.New("edgesync handler: reconciler is required")
	}
	if cfg.SpokeSecrets == nil {
		return nil, errors.New("edgesync handler: spoke secret lookup is required")
	}
	if cfg.Replay == nil {
		return nil, errors.New("edgesync handler: replay guard is required")
	}
	if cfg.HubID == "" {
		return nil, errors.New("edgesync handler: hub ID is required")
	}
	if cfg.MaxFileBytes <= 0 {
		return nil, errors.New("edgesync handler: max file bytes must be > 0")
	}
	return &EdgeSyncHandler{
		receiver:     cfg.Receiver,
		reconciler:   cfg.Reconciler,
		lookup:       cfg.SpokeSecrets,
		replay:       cfg.Replay,
		authManager:  cfg.AuthManager,
		hubID:        cfg.HubID,
		maxFileBytes: cfg.MaxFileBytes,
		// logger.Get already sets component; adding it again duplicates the key.
		logger: cfg.Logger,
	}, nil
}

// RegisterRoutes mounts the hub receive endpoints.
//
// Two independent authentication layers, both mandatory:
//
//   - Arc's API token auth, so the endpoint is not reachable by an anonymous
//     caller even before sync-specific checks run.
//   - Per-spoke HMAC inside the handler, which is what actually binds the
//     request to a spoke identity and its content.
//
// The token layer alone is insufficient (it proves a caller has *an* Arc
// token, not that it is a given spoke), and the HMAC alone would leave the
// endpoint reachable by anyone who can guess a spoke ID. Both, always.
func (h *EdgeSyncHandler) RegisterRoutes(app fiber.Router) {
	group := app.Group("/api/v1/sync")

	// A route-level body limit, ahead of everything else. The Content-Length
	// pre-checks below cannot bound a chunked request (there is no declared
	// length to check), and the body is buffered before routing — so without
	// this an unauthenticated caller could stream past both caps. Sized to the
	// larger of the two per-route limits, since one group serves both.
	bodyLimit := h.maxFileBytes
	if rb := h.maxReconcileBytes(); rb > bodyLimit {
		bodyLimit = rb
	}
	group.Use(func(c *fiber.Ctx) error {
		if int64(len(c.Body())) > bodyLimit {
			return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
				"error": "request body exceeds the edge sync limit",
			})
		}
		return c.Next()
	})

	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}

	group.Post("/file", h.receiveFile)
	group.Post("/reconcile", h.reconcile)

	h.logger.Info().
		Str("hub_id", h.hubID).
		Bool("resume_supported", h.receiver.SupportsResume()).
		Msg("Edge sync receive routes registered")
}

// receiveFile handles POST /api/v1/sync/file.
func (h *EdgeSyncHandler) receiveFile(c *fiber.Ctx) error {
	// Size check FIRST, before anything else touches the request.
	//
	// The Fiber app runs with StreamRequestBody=false, so fasthttp has already
	// buffered the body by the time this handler is entered — but rejecting
	// here still bounds what an attacker can make the hub hold, because the
	// route-level limit is what an operator can tune down from the global
	// server.max_payload_size (1GB by default). Without it, any party who can
	// reach the port could pin up to 1GB per connection with no token and no
	// spoke secret.
	if cl := c.Request().Header.ContentLength(); cl > 0 && int64(cl) > h.maxFileBytes {
		h.logger.Warn().
			Int("content_length", cl).
			Int64("limit", h.maxFileBytes).
			Msg("Sync upload rejected: exceeds edge_sync.max_file_bytes")
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": "upload exceeds edge_sync.max_file_bytes",
			"limit": h.maxFileBytes,
		})
	}

	// Fiber's header values alias a reusable buffer, so anything retained past
	// this handler must be copied. These are all consumed synchronously, but
	// copying keeps that from becoming a latent bug if the receive path ever
	// goes async.
	spokeID := string(append([]byte(nil), c.Request().Header.Peek(headerSpokeID)...))
	hubID := string(append([]byte(nil), c.Request().Header.Peek(headerHubID)...))
	filePath := string(append([]byte(nil), c.Request().Header.Peek(headerPath)...))
	sha := string(append([]byte(nil), c.Request().Header.Peek(headerSHA256)...))
	nonce := string(append([]byte(nil), c.Request().Header.Peek(headerNonce)...))
	mac := string(append([]byte(nil), c.Request().Header.Peek(headerMAC)...))

	if spokeID == "" || filePath == "" || sha == "" || nonce == "" || mac == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "missing required sync headers",
		})
	}

	// The hub ID must match this hub. A MAC minted for a different hub that
	// shares the spoke's secret is not valid here.
	if hubID != h.hubID {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "hub ID mismatch",
		})
	}

	size, err := strconv.ParseInt(c.Get(headerSize), 10, 64)
	if err != nil || size < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid or missing " + headerSize,
		})
	}
	// A declared size above the cap is refused even when Content-Length is
	// absent or understated — the declared size is what the receiver uses to
	// bound its reads and to size the staging write.
	if size > h.maxFileBytes {
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": "declared size exceeds edge_sync.max_file_bytes",
			"limit": h.maxFileBytes,
		})
	}

	var offset int64
	if raw := c.Get(headerOffset); raw != "" {
		offset, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || offset < 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "invalid " + headerOffset,
			})
		}
	}

	ts, err := strconv.ParseInt(c.Get(headerTS), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid or missing " + headerTS,
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), receiveTimeout)
	defer cancel()

	secret, ok := h.lookup(ctx, spokeID)
	if !ok {
		// Deliberately identical to a MAC failure: distinguishing "unknown
		// spoke" from "bad signature" would let an attacker enumerate
		// registered spoke IDs.
		h.logger.Warn().Str("spoke_id", spokeID).Msg("Sync rejected: unknown or disabled spoke")
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "authentication failed"})
	}

	// Validates the MAC and consumes the nonce, in that order — a forged
	// request must not burn a nonce slot and lock out the legitimate one.
	if err := security.ValidateSyncFileHMACWithReplay(
		h.replay, secret, nonce, spokeID, hubID, filePath, sha, ts, mac,
		security.HMACTimestampTolerance,
	); err != nil {
		return h.authFailure(c, spokeID, err)
	}

	// The body is fully buffered, not streamed: the Fiber app is constructed
	// with StreamRequestBody=false (api/server.go) so the gzip-compressed
	// ingest path can handle bodies itself, and RequestBodyStream() returns
	// nothing usable under that setting. A sync upload is therefore bounded by
	// the server's BodyLimit (server.max_payload_size), which an operator
	// running a hub must size above their largest compacted Parquet file.
	//
	// This is a real deviation from §5.2's "stream the body". Flipping
	// StreamRequestBody globally would change behavior for every existing
	// endpoint, so it is deliberately out of scope here; the receiver itself
	// takes an io.Reader and streams to storage, so only this seam buffers.
	res, err := h.receiver.Receive(ctx, spokeID, filePath, sha, size, offset, bytes.NewReader(c.Body()))
	if err != nil {
		if errors.Is(err, storage.ErrResumeNotSupported) {
			// Not a failure the spoke can fix by retrying the same request:
			// it must restart from zero. 409 with an explicit reason so the
			// agent branches without parsing prose.
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error":  "resume not supported by this hub's storage backend",
				"reason": "resume_unsupported",
				"resume": false,
			})
		}
		// A hub-side failure — storage I/O, or a manifest write during a Raft
		// election — is transient and the spoke SHOULD retry. Telling it 400
		// would say its request was malformed, so it would either give up or
		// retry a request it believes is broken. 503 is the honest answer, and
		// the error text is withheld because it describes hub internals.
		if errors.Is(err, edgesync.ErrReceiveInternal) {
			h.logger.Error().Err(err).
				Str("spoke_id", spokeID).
				Str("path", filePath).
				Msg("Sync receive failed for a hub-side reason")
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error":  "hub temporarily unable to accept this file",
				"reason": "hub_unavailable",
			})
		}

		// Everything else is the spoke's fault — a bad path, a malformed
		// digest, an out-of-range offset — and the message tells it what to fix.
		h.logger.Warn().Err(err).
			Str("spoke_id", spokeID).
			Str("path", filePath).
			Msg("Sync receive rejected an invalid request")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	return h.writeOutcome(c, res)
}

// reconcile handles POST /api/v1/sync/reconcile.
//
// One round-trip tells a spoke which of its pending files the hub already
// holds, which is what makes a long disconnection survivable: 5,000 pending
// files cost one request rather than 5,000.
func (h *EdgeSyncHandler) reconcile(c *fiber.Ctx) error {
	// Size check first, before anything reads the body. The body is buffered
	// before routing (StreamRequestBody=false), so this is the only bound on
	// what an unauthenticated caller can make the hub hold.
	if cl := c.Request().Header.ContentLength(); cl > 0 && int64(cl) > h.maxReconcileBytes() {
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error":       "reconcile batch too large",
			"max_entries": h.reconciler.MaxEntries(),
		})
	}

	spokeID := string(append([]byte(nil), c.Request().Header.Peek(headerSpokeID)...))
	hubID := string(append([]byte(nil), c.Request().Header.Peek(headerHubID)...))
	nonce := string(append([]byte(nil), c.Request().Header.Peek(headerNonce)...))
	mac := string(append([]byte(nil), c.Request().Header.Peek(headerMAC)...))

	if spokeID == "" || nonce == "" || mac == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "missing required sync headers",
		})
	}
	if hubID != h.hubID {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "hub ID mismatch"})
	}

	ts, err := strconv.ParseInt(c.Get(headerTS), 10, 64)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid or missing " + headerTS,
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), reconcileTimeout)
	defer cancel()

	secret, ok := h.lookup(ctx, spokeID)
	if !ok {
		h.logger.Warn().Str("spoke_id", spokeID).Msg("Reconcile rejected: unknown or disabled spoke")
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "authentication failed"})
	}

	// The MAC binds a digest of the body, so a replayed request cannot
	// substitute a different path list and use the hub as an oracle for what
	// data exists. Validated against the RAW bytes, before parsing — hashing a
	// re-serialized form would compare a different byte sequence.
	body := c.Body()
	if err := security.ValidateSyncReconcileHMACWithReplay(
		h.replay, secret, nonce, spokeID, hubID, body, ts, mac,
		security.HMACTimestampTolerance,
	); err != nil {
		return h.authFailure(c, spokeID, err)
	}

	var req reconcileRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "malformed reconcile body",
		})
	}

	res, err := h.reconciler.Reconcile(ctx, spokeID, req.Entries)
	if err != nil {
		if errors.Is(err, edgesync.ErrReconcileTooLarge) {
			return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
				"error":       err.Error(),
				"max_entries": h.reconciler.MaxEntries(),
			})
		}
		if errors.Is(err, edgesync.ErrReceiveInternal) {
			h.logger.Error().Err(err).Str("spoke_id", spokeID).Msg("Reconcile failed for a hub-side reason")
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error":  "hub temporarily unable to reconcile",
				"reason": "hub_unavailable",
			})
		}
		h.logger.Warn().Err(err).Str("spoke_id", spokeID).Msg("Reconcile rejected an invalid request")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Empty slices rather than null: a spoke iterating the response should not
	// have to special-case a missing field.
	return c.Status(fiber.StatusOK).JSON(reconcileResponse{
		Missing:   orEmpty(res.Missing),
		Present:   orEmpty(res.Present),
		Conflicts: orEmptyConflicts(res.Conflicts),
	})
}

// maxReconcileBytes bounds the request body from the entry cap.
//
// Derived rather than configured separately so the two cannot drift. The
// figure is measured, not guessed: a minimal legal entry JSON-encodes to 96
// bytes (a one-character path plus a 64-character digest) and a realistic Arc
// entry — a full compacted path with a size — to 189. An earlier version used
// 512, which sounded "generous" but let the byte check admit roughly five
// times the entry cap, so the only bound applied BEFORE authentication was
// five times looser than intended.
//
// 256 covers a realistic entry with room to spare while keeping the pre-auth
// bound close to the entry cap. A batch of legitimately long paths that
// overshoots is still answered correctly — it is refused with a 413 naming the
// entry limit, which is what a spoke needs to page under.
func (h *EdgeSyncHandler) maxReconcileBytes() int64 {
	const bytesPerEntry = 256
	return int64(h.reconciler.MaxEntries()) * bytesPerEntry
}

type reconcileRequest struct {
	Entries []edgesync.ReconcileEntry `json:"entries"`
}

type reconcileResponse struct {
	Missing   []string            `json:"missing"`
	Present   []string            `json:"present"`
	Conflicts []edgesync.Conflict `json:"conflicts"`
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func orEmptyConflicts(c []edgesync.Conflict) []edgesync.Conflict {
	if c == nil {
		return []edgesync.Conflict{}
	}
	return c
}

// authFailure reports an authentication problem without leaking which check
// failed, beyond the skew/forgery distinction an operator genuinely needs.
func (h *EdgeSyncHandler) authFailure(c *fiber.Ctx, spokeID string, err error) error {
	log := h.logger.Warn().Str("spoke_id", spokeID)

	switch {
	case errors.Is(err, security.ErrSyncAuthExpired):
		// Worth distinguishing: an operator seeing this fixes NTP on the edge
		// box, whereas a MAC failure means investigating a forgery.
		log.Msg("Sync rejected: timestamp outside the freshness window")
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error":  "authentication failed",
			"reason": "timestamp_expired",
		})
	case errors.Is(err, security.ErrSyncAuthReplay):
		log.Msg("Sync rejected: nonce already used")
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error":  "authentication failed",
			"reason": "replay",
		})
	default:
		log.Msg("Sync rejected: HMAC validation failed")
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "authentication failed"})
	}
}

// writeOutcome maps a receive outcome to the status codes §5.2 defines.
func (h *EdgeSyncHandler) writeOutcome(c *fiber.Ctx, res *edgesync.PutResult) error {
	switch res.Outcome {
	case edgesync.OutcomeCommitted, edgesync.OutcomeAlreadyPresent:
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"outcome":        string(res.Outcome),
			"bytes_accepted": res.BytesAccepted,
		})

	case edgesync.OutcomePartial:
		// 206 carries the hub's true offset, which may differ from what the
		// spoke sent — that is how a diverged checkpoint gets corrected.
		return c.Status(fiber.StatusPartialContent).JSON(fiber.Map{
			"outcome":        string(res.Outcome),
			"bytes_accepted": res.BytesAccepted,
		})

	case edgesync.OutcomeConflict:
		// The hub's digest is the evidence an operator needs to tell a
		// spoke-ID collision from corruption.
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"outcome":      string(res.Outcome),
			"their_sha256": res.TheirSHA256,
		})

	case edgesync.OutcomeChecksumMismatch:
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"outcome": string(res.Outcome),
			"error":   "checksum mismatch; the upload was discarded",
		})

	case edgesync.OutcomeBackpressure:
		c.Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())))
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"outcome": string(res.Outcome),
		})

	default:
		h.logger.Error().Str("outcome", string(res.Outcome)).Msg("Unhandled sync outcome")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "unhandled sync outcome",
		})
	}
}

// StaticSpokeSecrets builds a lookup over an in-memory map.
//
// A stand-in until the spoke registry lands (#569 PR 7), which will store
// per-spoke secrets in SQLite. Exported so the hub can be wired and exercised
// end to end before that PR.
func StaticSpokeSecrets(secrets map[string]string) SpokeSecretLookup {
	// Copy so a later mutation of the caller's map cannot silently change
	// which spokes authenticate.
	own := make(map[string]string, len(secrets))
	for k, v := range secrets {
		own[k] = v
	}
	return func(_ context.Context, spokeID string) (string, bool) {
		s, ok := own[spokeID]
		if !ok || s == "" {
			return "", false
		}
		return s, true
	}
}
