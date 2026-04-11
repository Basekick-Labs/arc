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
