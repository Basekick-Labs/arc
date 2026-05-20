package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// ComputeHMAC computes HMAC-SHA256 over the join parameters.
// The message format is: nonce:nodeID:clusterName:timestamp
func ComputeHMAC(sharedSecret, nonce, nodeID, clusterName string, timestamp int64) string {
	message := fmt.Sprintf("%s:%s:%s:%d", nonce, nodeID, clusterName, timestamp)
	h := hmac.New(sha256.New, []byte(sharedSecret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

// GenerateNonce creates a cryptographically random 32-byte nonce as hex string.
func GenerateNonce() (string, error) {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}
	return hex.EncodeToString(nonce), nil
}

// ValidateHMAC validates the HMAC and checks timestamp freshness to prevent replay attacks.
func ValidateHMAC(sharedSecret, nonce, nodeID, clusterName string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	now := time.Now().Unix()
	drift := now - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		return fmt.Errorf("auth timestamp expired (drift: %ds, tolerance: %ds)", drift, int64(tolerance.Seconds()))
	}

	expected := ComputeHMAC(sharedSecret, nonce, nodeID, clusterName, timestamp)
	if !hmac.Equal([]byte(expected), []byte(receivedMAC)) {
		return fmt.Errorf("HMAC validation failed: shared secret mismatch")
	}
	return nil
}

// ComputeFetchHMAC computes HMAC-SHA256 for a peer file-fetch request. The
// message format binds the requested path into the signed payload so a stolen
// MAC for file A cannot be replayed within the freshness window to fetch a
// different file B. Format: nonce:nodeID:clusterName:path:timestamp
func ComputeFetchHMAC(sharedSecret, nonce, nodeID, clusterName, path string, timestamp int64) string {
	message := fmt.Sprintf("%s:%s:%s:%s:%d", nonce, nodeID, clusterName, path, timestamp)
	h := hmac.New(sha256.New, []byte(sharedSecret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

// ValidateFetchHMAC validates a peer fetch-request HMAC and checks freshness.
// The path is included in the signed payload — see ComputeFetchHMAC.
func ValidateFetchHMAC(sharedSecret, nonce, nodeID, clusterName, path string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	now := time.Now().Unix()
	drift := now - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		return fmt.Errorf("fetch auth timestamp expired (drift: %ds, tolerance: %ds)", drift, int64(tolerance.Seconds()))
	}

	expected := ComputeFetchHMAC(sharedSecret, nonce, nodeID, clusterName, path, timestamp)
	if !hmac.Equal([]byte(expected), []byte(receivedMAC)) {
		return fmt.Errorf("fetch HMAC validation failed: shared secret mismatch or path tampered")
	}
	return nil
}

// ComputeForwardHMAC computes HMAC-SHA256 for a leader-forwarding request.
// The message format binds the command payload into the signed material so
// an attacker on the network cannot swap the command body while keeping the
// same HMAC. Format: nonce:nodeID:clusterName:payloadSHA256:timestamp
//
// We hash the payload with SHA-256 before feeding it into the HMAC message
// rather than including the raw bytes, so the HMAC input remains a
// fixed-length string regardless of command size.
func ComputeForwardHMAC(sharedSecret, nonce, nodeID, clusterName string, payload []byte, timestamp int64) string {
	payloadHash := sha256.Sum256(payload)
	payloadHex := hex.EncodeToString(payloadHash[:])
	message := fmt.Sprintf("%s:%s:%s:%s:%d", nonce, nodeID, clusterName, payloadHex, timestamp)
	h := hmac.New(sha256.New, []byte(sharedSecret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

// ValidateForwardHMAC validates a leader-forwarding HMAC and checks freshness.
// The command payload is included in the signed material — see ComputeForwardHMAC.
func ValidateForwardHMAC(sharedSecret, nonce, nodeID, clusterName string, payload []byte, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	now := time.Now().Unix()
	drift := now - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		return fmt.Errorf("forward auth timestamp expired (drift: %ds, tolerance: %ds)", drift, int64(tolerance.Seconds()))
	}

	expected := ComputeForwardHMAC(sharedSecret, nonce, nodeID, clusterName, payload, timestamp)
	if !hmac.Equal([]byte(expected), []byte(receivedMAC)) {
		return fmt.Errorf("forward HMAC validation failed: shared secret mismatch or payload tampered")
	}
	return nil
}

// ComputeCacheInvalidateHMAC computes HMAC-SHA256 for the post-compaction
// cluster cache-invalidation broadcast (POST /api/v1/internal/cache/invalidate).
// The request body is empty by design — the only state the endpoint needs is
// "clear your caches now" — so the signed material binds sender identity and
// freshness rather than payload bytes. Format: nonce:nodeID:clusterName:timestamp
//
// Distinct label from Forward/Fetch/JoinHMAC so a leaked MAC for one endpoint
// cannot be replayed against another, even within the freshness window.
func ComputeCacheInvalidateHMAC(sharedSecret, nonce, nodeID, clusterName string, timestamp int64) string {
	message := fmt.Sprintf("cache-invalidate:%s:%s:%s:%d", nonce, nodeID, clusterName, timestamp)
	h := hmac.New(sha256.New, []byte(sharedSecret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

// ValidateCacheInvalidateHMAC validates a cache-invalidate HMAC and checks
// freshness. The message format is intentionally label-distinct from
// ComputeForwardHMAC / ComputeFetchHMAC / ComputeHMAC so cross-endpoint
// replay is impossible even if a MAC leaks within the freshness window.
func ValidateCacheInvalidateHMAC(sharedSecret, nonce, nodeID, clusterName string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	now := time.Now().Unix()
	drift := now - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		// Symmetric: covers both stale (past) and future-dated drift.
		// Existing tests still match on "timestamp expired" — do not
		// rename without updating the test substring match.
		return fmt.Errorf("cache-invalidate auth timestamp expired or out of tolerance (drift: %ds, tolerance: %ds)", drift, int64(tolerance.Seconds()))
	}

	expected := ComputeCacheInvalidateHMAC(sharedSecret, nonce, nodeID, clusterName, timestamp)
	if !hmac.Equal([]byte(expected), []byte(receivedMAC)) {
		return fmt.Errorf("cache-invalidate HMAC validation failed: shared secret mismatch")
	}
	return nil
}
