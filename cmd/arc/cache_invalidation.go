package main

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
)

// invalidatePeerQueryCaches sends one authenticated invalidation to a peer.
// The caller runs it asynchronously after the local caches have been cleared.
func invalidatePeerQueryCaches(client *http.Client, scheme string, n *cluster.Node, sharedSecret, senderID, clusterName string, logger zerolog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Compute the HMAC once per request: a fresh nonce
	// (32 random bytes, hex-encoded) binds the MAC to a
	// single attempt; the timestamp binds it to a
	// 5-minute freshness window. Receiver replay-checks
	// (nonce, sender nodeID) against its NonceCache.
	nonce, err := security.GenerateNonce()
	if err != nil {
		logger.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to generate nonce for cache invalidation")
		return
	}
	ts := time.Now().Unix()
	mac := security.ComputeCacheInvalidateHMAC(
		sharedSecret, nonce, senderID, clusterName, ts,
	)

	// Build via *url.URL so an IPv6 literal in
	// n.APIAddress (e.g. "[::1]:8000") survives
	// unmangled. CacheInvalidatePath is a known-safe
	// constant path, so we don't need to escape it.
	peerURL := (&url.URL{
		Scheme: scheme,
		Host:   cluster.APIHostPort(n),
		Path:   api.CacheInvalidatePath,
	}).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peerURL, nil)
	if err != nil {
		logger.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to create cache invalidation request")
		return
	}
	req.Header.Set("X-Arc-Node-ID", senderID)
	req.Header.Set("X-Arc-Cluster", clusterName)
	req.Header.Set("X-Arc-Nonce", nonce)
	req.Header.Set("X-Arc-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Arc-HMAC", mac)

	resp, err := client.Do(req)
	if err != nil {
		logger.Warn().Err(err).Str("node_id", n.ID).Str("address", n.APIAddress).
			Msg("Failed to invalidate cache on remote node")
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		logger.Warn().Int("status", resp.StatusCode).Str("node_id", n.ID).
			Msg("Unexpected status from cache invalidation")
	} else {
		logger.Debug().Str("node_id", n.ID).Msg("Remote query caches invalidated")
	}
}
