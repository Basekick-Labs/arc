package api

import (
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// CacheInvalidatePath is the single source of truth for the
// /api/v1/internal/cache/invalidate route. The receiver registers it
// in CacheInvalidateHandler.Register; the cluster post-compaction
// sender (cmd/arc/main.go) and the auth middleware's PublicRoutes
// config both reference this constant so a future typo cannot
// silently break fan-out.
const CacheInvalidatePath = "/api/v1/internal/cache/invalidate"

// CacheInvalidateHandler serves the post-compaction cluster
// cache-invalidate endpoint. It exists only when cluster mode is
// active AND cluster.shared_secret is configured — callers in
// cmd/arc/main.go construct it conditionally and skip Register
// entirely otherwise.
//
// Closes audit finding #3 from 2026-05-19: the previous gate was a
// static header (`X-Arc-Internal: cache-invalidate`) which any
// network-reachable caller could replay, forcing DuckDB's
// cache_httpfs glob results to repopulate from S3 — cost
// amplification + p99 spikes with no auth.
//
// The "unconfigured = no route" shape (rather than a runtime
// secret-string check inside the handler) means an unconfigured Arc
// instance returns 404 for this path — there is nothing to probe,
// not even a 403. Configured-but-misauthed callers still get a
// uniform 403 across every rejection branch (see handle).
//
// Per-decision: the audit deliberately assigned NO CVE to this
// finding because OSS does not reach the endpoint (post-compaction
// invalidation is in-process via db.ClearHTTPCache) and the endpoint
// carries no secrets — it is only a cluster-mode "clear your caches
// now" signal. Do NOT retroactively file a CVE without re-evaluating
// the threat model.
type CacheInvalidateHandler struct {
	sharedSecret string
	clusterName  string
	localNodeID  string
	nonceCache   *security.NonceCache
	tolerance    time.Duration
	onInvalidate func()
	logger       zerolog.Logger
}

// NewCacheInvalidateHandler constructs the handler. All arguments are
// required; the constructor panics on missing pieces so a
// misconfigured caller is loudly broken at startup instead of producing
// a silently half-armed endpoint. By construction, every field on the
// returned value is non-zero — handle() needs no nil-guards.
func NewCacheInvalidateHandler(
	sharedSecret, clusterName, localNodeID string,
	nonceCache *security.NonceCache,
	tolerance time.Duration,
	onInvalidate func(),
	logger zerolog.Logger,
) *CacheInvalidateHandler {
	if sharedSecret == "" {
		panic("NewCacheInvalidateHandler: sharedSecret must be non-empty (call only when cluster.shared_secret is configured)")
	}
	if clusterName == "" {
		panic("NewCacheInvalidateHandler: clusterName must be non-empty")
	}
	if localNodeID == "" {
		panic("NewCacheInvalidateHandler: localNodeID must be non-empty")
	}
	if nonceCache == nil {
		panic("NewCacheInvalidateHandler: nonceCache must be non-nil (replay protection cannot be disabled)")
	}
	if tolerance <= 0 {
		panic("NewCacheInvalidateHandler: tolerance must be positive")
	}
	if onInvalidate == nil {
		panic("NewCacheInvalidateHandler: onInvalidate must be non-nil")
	}
	return &CacheInvalidateHandler{
		sharedSecret: sharedSecret,
		clusterName:  clusterName,
		localNodeID:  localNodeID,
		nonceCache:   nonceCache,
		tolerance:    tolerance,
		onInvalidate: onInvalidate,
		logger:       logger,
	}
}

// Register wires POST CacheInvalidatePath. Callers MUST add
// CacheInvalidatePath to the auth middleware's PublicRoutes — the
// endpoint authenticates via HMAC, not via the user-token auth
// middleware (cluster peers do not carry user auth).
func (h *CacheInvalidateHandler) Register(app fiber.Router) {
	app.Post(CacheInvalidatePath, h.handle)
}

// handle validates the HMAC-authenticated cluster cache-invalidate
// request and, on success, calls onInvalidate.
//
// Every rejection path returns fiber.StatusForbidden with no body so
// an attacker cannot probe to distinguish "missing headers" from
// "bad MAC" from "replay". Rejection logging uses Debug level on
// purpose: at Warn the endpoint would amplify a flooder into a
// log-DoS, while operators chasing a misconfiguration can flip the
// level on the api package logger.
//
// Validates HMAC over (nonce, nodeID, clusterName, timestamp) using
// the cluster shared secret. Uses the same shared secret and
// 5-minute freshness window as the leader-forward and peer-fetch
// endpoints, with a "cache-invalidate:" label prefix as the endpoint
// discriminator (no payload or path to bind, so the label is what
// prevents cross-endpoint MAC replay).
//
// Replay-protects via the cluster nonce cache (TTL matches the HMAC
// freshness tolerance, so a nonce expires from the cache at the same
// instant its MAC would be rejected as stale). The Track call runs
// AFTER HMAC validation so an attacker who can't forge a MAC cannot
// burn nonce-cache slots that a legitimate peer might present.
//
// Rejects a request that claims to come from this node's own ID —
// local invalidation happens in-process, never over HTTP.
func (h *CacheInvalidateHandler) handle(c *fiber.Ctx) error {
	nodeID := c.Get("X-Arc-Node-ID")
	clusterName := c.Get("X-Arc-Cluster")
	nonce := c.Get("X-Arc-Nonce")
	mac := c.Get("X-Arc-HMAC")
	tsStr := c.Get("X-Arc-Timestamp")
	if nodeID == "" || clusterName == "" || nonce == "" || mac == "" || tsStr == "" {
		h.logger.Debug().Str("remote_ip", c.IP()).
			Msg("cache-invalidate rejected: missing one or more required headers")
		return c.SendStatus(fiber.StatusForbidden)
	}

	// Reject requests claiming the local node ID: local cache
	// invalidation runs in-process (the compaction callback calls
	// the invalidation hooks directly), so a self-addressed HTTP
	// request is either a misconfiguration or a confused attacker.
	// localNodeID is not secret — constant-time compare not required.
	if nodeID == h.localNodeID {
		h.logger.Debug().Str("remote_ip", c.IP()).Str("node_id", nodeID).
			Msg("cache-invalidate rejected: self-addressed request")
		return c.SendStatus(fiber.StatusForbidden)
	}

	// Fast-path reject before HMAC computation. The HMAC itself also
	// binds clusterName (tamper-proof), but checking the header first
	// avoids a useless hash compute for cross-cluster requests.
	if clusterName != h.clusterName {
		h.logger.Debug().Str("remote_ip", c.IP()).Str("cluster_name", clusterName).
			Msg("cache-invalidate rejected: cluster name mismatch")
		return c.SendStatus(fiber.StatusForbidden)
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		h.logger.Debug().Err(err).Str("remote_ip", c.IP()).Str("timestamp", tsStr).
			Msg("cache-invalidate rejected: non-numeric timestamp")
		return c.SendStatus(fiber.StatusForbidden)
	}

	if err := security.ValidateCacheInvalidateHMAC(
		h.sharedSecret, nonce, nodeID, clusterName, ts, mac, h.tolerance,
	); err != nil {
		h.logger.Debug().Err(err).Str("remote_ip", c.IP()).Str("node_id", nodeID).
			Msg("cache-invalidate rejected: HMAC validation failed")
		return c.SendStatus(fiber.StatusForbidden)
	}

	// Replay check AFTER HMAC validation: an attacker who can't even
	// produce a valid MAC cannot burn nonce-cache slots that a real
	// peer might present.
	if !h.nonceCache.Track(nodeID, nonce) {
		h.logger.Debug().Str("remote_ip", c.IP()).Str("node_id", nodeID).
			Msg("cache-invalidate rejected: replay (nonce already seen)")
		return c.SendStatus(fiber.StatusForbidden)
	}

	h.onInvalidate()
	return c.SendStatus(fiber.StatusNoContent)
}
