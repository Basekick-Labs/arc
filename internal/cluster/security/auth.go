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
// Fields are delimited by NUL (\x00) so a field containing a colon
// cannot be smuggled to collide with a different (nonce, nodeID,
// clusterName, timestamp) arrangement. NUL is forbidden in HTTP header
// values by net/http (httpguts), so the sender side cannot produce a
// NUL-containing field even via malicious config.
// Message format: nonce \x00 nodeID \x00 clusterName \x00 timestamp
func ComputeHMAC(sharedSecret, nonce, nodeID, clusterName string, timestamp int64) string {
	return hex.EncodeToString(computeJoinHMACRaw(sharedSecret, nonce, nodeID, clusterName, timestamp))
}

func computeJoinHMACRaw(sharedSecret, nonce, nodeID, clusterName string, timestamp int64) []byte {
	message := fmt.Sprintf("%s\x00%s\x00%s\x00%d", nonce, nodeID, clusterName, timestamp)
	return computeRawHMAC(sharedSecret, message)
}

// constantTimeHexEqual reports whether two hex-encoded MAC strings
// represent the same byte sequence, in constant time over the
// decoded length. The two callers (every Validate*HMAC) decode the
// received MAC from hex and compare raw bytes against the recomputed
// expected MAC — more idiomatic than comparing hex strings (the
// underlying hash is what matters; the hex envelope is just transport).
// Returns false on any hex-decode error so a malformed receivedMAC
// fails closed.
func constantTimeHexEqual(expectedRaw []byte, receivedHex string) bool {
	receivedRaw, err := hex.DecodeString(receivedHex)
	if err != nil {
		return false
	}
	return hmac.Equal(expectedRaw, receivedRaw)
}

// computeRawHMAC is the shared compute path for all the Compute*HMAC
// public functions. They format their domain-specific canonical input,
// then call this to do the SHA-256 HMAC + return the raw bytes.
// Public callers receive the hex encoding for transport; validators
// compare against the raw bytes directly via constantTimeHexEqual.
func computeRawHMAC(sharedSecret, message string) []byte {
	h := hmac.New(sha256.New, []byte(sharedSecret))
	h.Write([]byte(message))
	return h.Sum(nil)
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

	expected := computeJoinHMACRaw(sharedSecret, nonce, nodeID, clusterName, timestamp)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}

// ComputeFetchHMAC computes HMAC-SHA256 for a peer file-fetch request. The
// message format binds the requested path into the signed payload so a stolen
// MAC for file A cannot be replayed within the freshness window to fetch a
// different file B.
// Fields are NUL-delimited (see ComputeHMAC for rationale).
// Format: nonce \x00 nodeID \x00 clusterName \x00 path \x00 timestamp
func ComputeFetchHMAC(sharedSecret, nonce, nodeID, clusterName, path string, timestamp int64) string {
	return hex.EncodeToString(computeFetchHMACRaw(sharedSecret, nonce, nodeID, clusterName, path, timestamp))
}

func computeFetchHMACRaw(sharedSecret, nonce, nodeID, clusterName, path string, timestamp int64) []byte {
	message := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d", nonce, nodeID, clusterName, path, timestamp)
	return computeRawHMAC(sharedSecret, message)
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

	expected := computeFetchHMACRaw(sharedSecret, nonce, nodeID, clusterName, path, timestamp)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("fetch HMAC validation failed: shared secret mismatch, path tampered, or malformed MAC")
	}
	return nil
}

// ComputeForwardHMAC computes HMAC-SHA256 for a leader-forwarding request.
// The message format binds the command payload into the signed material so
// an attacker on the network cannot swap the command body while keeping the
// same HMAC.
//
// We hash the payload with SHA-256 before feeding it into the HMAC message
// rather than including the raw bytes, so the HMAC input remains a
// fixed-length string regardless of command size.
//
// Fields are NUL-delimited (see ComputeHMAC for rationale).
// Format: nonce \x00 nodeID \x00 clusterName \x00 payloadSHA256 \x00 timestamp
func ComputeForwardHMAC(sharedSecret, nonce, nodeID, clusterName string, payload []byte, timestamp int64) string {
	return hex.EncodeToString(computeForwardHMACRaw(sharedSecret, nonce, nodeID, clusterName, payload, timestamp))
}

func computeForwardHMACRaw(sharedSecret, nonce, nodeID, clusterName string, payload []byte, timestamp int64) []byte {
	payloadHash := sha256.Sum256(payload)
	payloadHex := hex.EncodeToString(payloadHash[:])
	message := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d", nonce, nodeID, clusterName, payloadHex, timestamp)
	return computeRawHMAC(sharedSecret, message)
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

	expected := computeForwardHMACRaw(sharedSecret, nonce, nodeID, clusterName, payload, timestamp)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("forward HMAC validation failed: shared secret mismatch, payload tampered, or malformed MAC")
	}
	return nil
}

// ComputeCacheInvalidateHMAC computes HMAC-SHA256 for the post-compaction
// cluster cache-invalidation broadcast (POST /api/v1/internal/cache/invalidate).
// The request body is empty by design — the only state the endpoint needs is
// "clear your caches now" — so the signed material binds sender identity and
// freshness rather than payload bytes.
//
// The "cache-invalidate" label is the first field of the canonical input,
// distinct from Forward/Fetch/Join so a leaked MAC for one endpoint cannot
// be replayed against another even within the freshness window. Fields are
// NUL-delimited (see ComputeHMAC for rationale).
// Format: "cache-invalidate" \x00 nonce \x00 nodeID \x00 clusterName \x00 timestamp
func ComputeCacheInvalidateHMAC(sharedSecret, nonce, nodeID, clusterName string, timestamp int64) string {
	return hex.EncodeToString(computeCacheInvalidateHMACRaw(sharedSecret, nonce, nodeID, clusterName, timestamp))
}

func computeCacheInvalidateHMACRaw(sharedSecret, nonce, nodeID, clusterName string, timestamp int64) []byte {
	message := fmt.Sprintf("cache-invalidate\x00%s\x00%s\x00%s\x00%d", nonce, nodeID, clusterName, timestamp)
	return computeRawHMAC(sharedSecret, message)
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

	expected := computeCacheInvalidateHMACRaw(sharedSecret, nonce, nodeID, clusterName, timestamp)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("cache-invalidate HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}
