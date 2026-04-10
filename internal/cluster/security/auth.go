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
