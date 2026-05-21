package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"golang.org/x/crypto/hkdf"
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

// ----------------------------------------------------------------------
// Replication protocol HMAC + session key derivation
// ----------------------------------------------------------------------
//
// Closes GHSA-wfgr-8x84-22q7 / CVE-2026-48106 (audit X1, 2026-05-19).
//
// The replication protocol has three trust boundaries:
//
//   1. Handshake (MsgReplicateSync): reader announces itself to the
//      writer, requests stream-from-position. Authenticated with a
//      full HMAC over {nonce, readerID, clusterName, lastKnownSeq,
//      timestamp} keyed by the cluster shared secret. Same shape as
//      ComputeFetchHMAC / ComputeForwardHMAC / ComputeCacheInvalidateHMAC,
//      with the "replicate-sync" label prefix to prevent cross-endpoint
//      MAC replay.
//
//   2. Per-entry stream (MsgReplicateEntry): hundreds-to-thousands per
//      second under load. We can't afford a full HMAC + nonce-cache
//      lookup per entry. Instead, on successful handshake both sides
//      derive a session key from the handshake nonce + cluster shared
//      secret (HKDF). Each entry carries an 8-byte truncated MAC tag
//      computed over the session key + sequence + payload hash. Forging
//      requires knowing the session key, which requires having
//      witnessed the handshake — which requires having the cluster
//      shared secret.
//
//   3. Periodic checkpoint (MsgReplicateCheckpoint): every N entries
//      the sender emits a full HMAC over the cumulative payload hash
//      + last seq + last ts + nonce, keyed by the cluster shared
//      secret (NOT the session key). Defense-in-depth against session
//      key compromise mid-stream and against any catastrophic tag
//      collision sequence.

// ComputeReplicateSyncHMAC computes the handshake HMAC for the
// replication MsgReplicateSync message. The "replicate-sync" label
// distinguishes this MAC from other cluster HMACs so a leaked MAC for
// one endpoint cannot be replayed against another.
// Format: "replicate-sync" \x00 nonce \x00 readerID \x00 clusterName \x00 lastKnownSeq \x00 timestamp
func ComputeReplicateSyncHMAC(sharedSecret, nonce, readerID, clusterName string, lastKnownSeq uint64, timestamp int64) string {
	return hex.EncodeToString(computeReplicateSyncHMACRaw(sharedSecret, nonce, readerID, clusterName, lastKnownSeq, timestamp))
}

func computeReplicateSyncHMACRaw(sharedSecret, nonce, readerID, clusterName string, lastKnownSeq uint64, timestamp int64) []byte {
	message := fmt.Sprintf("replicate-sync\x00%s\x00%s\x00%s\x00%d\x00%d", nonce, readerID, clusterName, lastKnownSeq, timestamp)
	return computeRawHMAC(sharedSecret, message)
}

// ValidateReplicateSyncHMAC validates the handshake HMAC and checks
// freshness. Same symmetric-drift semantics as the other validators —
// both stale-past and future-dated timestamps are refused.
func ValidateReplicateSyncHMAC(sharedSecret, nonce, readerID, clusterName string, lastKnownSeq uint64, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	now := time.Now().Unix()
	drift := now - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		return fmt.Errorf("replicate-sync auth timestamp expired or out of tolerance (drift: %ds, tolerance: %ds)", drift, int64(tolerance.Seconds()))
	}

	expected := computeReplicateSyncHMACRaw(sharedSecret, nonce, readerID, clusterName, lastKnownSeq, timestamp)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("replicate-sync HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}

// DeriveReplicationSessionKey derives a 32-byte per-connection session
// key from the cluster shared secret + handshake nonce via HKDF-SHA256.
// Both sides see the handshake nonce, so both derive the same key.
//
// The session key is ephemeral — it lives only for the duration of
// the TCP connection. Reconnect requires a fresh handshake with a
// fresh nonce, which derives a fresh key. An attacker who didn't
// witness the handshake (didn't see the nonce on the wire) can't
// derive the session key from the cluster shared secret alone.
//
// The info string "arc-replication-session:" namespaces this KDF
// output from any other future use of HKDF with the same cluster
// shared secret + nonce inputs.
func DeriveReplicationSessionKey(sharedSecret string, handshakeNonce string) ([]byte, error) {
	// HKDF-Extract uses the nonce as salt and the shared secret as IKM.
	// HKDF-Expand pulls 32 bytes of output keyed with the namespace
	// info string.
	r := hkdf.New(sha256.New, []byte(sharedSecret), []byte(handshakeNonce), []byte("arc-replication-session:"))
	key := make([]byte, 32)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, fmt.Errorf("derive replication session key: %w", err)
	}
	return key, nil
}

// ReplicationEntryTagLen is the byte length of the per-entry MAC tag
// on the wire. 8 bytes = 64-bit MAC. Forgery probability per attempt
// is 2^-64 — effectively unforgeable for the threat model. Smaller
// than the full 32-byte HMAC because the data stream is high-rate
// and the security argument is "attacker must have witnessed the
// handshake" rather than "attacker is trying random forgeries."
const ReplicationEntryTagLen = 8

// HMACTimestampTolerance is the symmetric window (past and future) the
// cluster HMAC validators accept on a signed timestamp. Five minutes
// is the same window enforced by every Compute*HMAC consumer in this
// package (join, leave, fetch, forward-apply, cache-invalidate,
// replicate-sync, replicate-checkpoint) AND the NonceCache TTL the
// coordinator constructs at startup — keep them aligned so a replayed
// HMAC can never outlive its nonce-cache slot. Centralized here so a
// future operator can tune all sites in one place; Gemini round 1 on
// PR #449 flagged the hardcoded 5*time.Minute scattered across
// coordinator.go.
const HMACTimestampTolerance = 5 * time.Minute

// ComputeReplicationEntryTag returns the 8-byte per-entry MAC tag
// computed over the session key + sequence + first 8 bytes of the
// payload SHA-256. The payload binding prevents an attacker from
// swapping the payload bytes of an in-flight entry while keeping the
// tag. The sequence binding prevents replay of a previously-seen tag
// against a different entry.
//
// We use the first 8 bytes of the payload SHA-256 (not the full 32)
// because (a) the tag itself is only 8 bytes, so a full payload hash
// would be wasted entropy, and (b) the goal is binding, not collision
// resistance — an attacker who can find a SHA-256 prefix collision is
// past the point where this matters.
func ComputeReplicationEntryTag(sessionKey []byte, sequence uint64, payload []byte) []byte {
	payloadHash := sha256.Sum256(payload)
	return ComputeReplicationEntryTagFromHash(sessionKey, sequence, payloadHash)
}

// ComputeReplicationEntryTagFromHash is the broadcast-friendly variant
// of ComputeReplicationEntryTag: it takes a pre-computed SHA-256 over
// the payload instead of computing it internally. The broadcaster hashes
// the payload once (SHA-256 is the dominant cost in
// ComputeReplicationEntryTag) and passes the same hash to every
// reader's per-entry tag computation — at N readers, this drops the
// per-broadcast hash cost from N × sha256(payload) to 1 ×
// sha256(payload). Flagged by Gemini round 1 on PR #449 as a hot-path
// optimization for multi-reader deployments.
func ComputeReplicationEntryTagFromHash(sessionKey []byte, sequence uint64, payloadHash [sha256.Size]byte) []byte {
	mac := hmac.New(sha256.New, sessionKey)
	// Sequence as big-endian uint64, then first 8 bytes of payload hash.
	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], sequence)
	mac.Write(entryFrameLabel)
	mac.Write(seqBuf[:])
	mac.Write(payloadHash[:8])
	full := mac.Sum(nil)
	return full[:ReplicationEntryTagLen]
}

// entryFrameLabel is the domain-separation label baked into every
// per-entry MAC tag. Hoisted to a package-level var so each
// ComputeReplicationEntryTag call avoids a per-call []byte conversion.
var entryFrameLabel = []byte("entry-frame:")

// ValidateReplicationEntryTag recomputes the tag and compares
// constant-time. Returns nil on match, an error on mismatch.
// Caller (the receiver) must drop the connection on any error —
// a tag mismatch means either the session key is wrong or the
// payload was tampered. Either way the session is poisoned.
func ValidateReplicationEntryTag(sessionKey []byte, sequence uint64, payload []byte, receivedTag []byte) error {
	if len(receivedTag) != ReplicationEntryTagLen {
		return fmt.Errorf("replication entry tag: wrong length (got %d, want %d)", len(receivedTag), ReplicationEntryTagLen)
	}
	expected := ComputeReplicationEntryTag(sessionKey, sequence, payload)
	if !hmac.Equal(expected, receivedTag) {
		return fmt.Errorf("replication entry tag mismatch: session key wrong or payload tampered")
	}
	return nil
}

// ComputeReplicationCheckpointHMAC returns the full HMAC for a
// periodic checkpoint message. Keyed by the cluster shared secret
// (NOT the session key) so a session-key compromise mid-stream can't
// forge checkpoints. Same NUL-delimited format as the other Compute*HMAC
// functions, with the "replicate-checkpoint" label.
// Format: "replicate-checkpoint" \x00 nonce \x00 senderNodeID \x00 clusterName \x00 cumulativePayloadHashHex \x00 lastSeq \x00 timestamp
func ComputeReplicationCheckpointHMAC(sharedSecret, nonce, senderNodeID, clusterName string, cumulativePayloadHash [32]byte, lastSeq uint64, timestamp int64) string {
	return hex.EncodeToString(computeReplicationCheckpointHMACRaw(sharedSecret, nonce, senderNodeID, clusterName, cumulativePayloadHash, lastSeq, timestamp))
}

func computeReplicationCheckpointHMACRaw(sharedSecret, nonce, senderNodeID, clusterName string, cumulativePayloadHash [32]byte, lastSeq uint64, timestamp int64) []byte {
	hashHex := hex.EncodeToString(cumulativePayloadHash[:])
	message := fmt.Sprintf("replicate-checkpoint\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d", nonce, senderNodeID, clusterName, hashHex, lastSeq, timestamp)
	return computeRawHMAC(sharedSecret, message)
}

// ValidateReplicationCheckpointHMAC validates a checkpoint MAC + checks
// freshness. Caller drops the connection on any error.
func ValidateReplicationCheckpointHMAC(sharedSecret, nonce, senderNodeID, clusterName string, cumulativePayloadHash [32]byte, lastSeq uint64, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	now := time.Now().Unix()
	drift := now - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		return fmt.Errorf("replicate-checkpoint timestamp expired or out of tolerance (drift: %ds, tolerance: %ds)", drift, int64(tolerance.Seconds()))
	}

	expected := computeReplicationCheckpointHMACRaw(sharedSecret, nonce, senderNodeID, clusterName, cumulativePayloadHash, lastSeq, timestamp)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("replicate-checkpoint HMAC validation failed: shared secret mismatch, payload tampered, or malformed MAC")
	}
	return nil
}
