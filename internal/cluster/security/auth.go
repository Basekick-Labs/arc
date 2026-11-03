package security

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// MsgType is a per-message-type label bound into the join-family HMAC for
// domain separation (#504). Defining a dedicated type — rather than using
// untyped string constants — prevents an arbitrary string from being passed
// where a message-type label is expected, making sign/validate mismatches
// a compile-time error.
type MsgType string

// Message-type labels bound into the coordinator-handshake HMACs
// (handshake_auth.go) for domain separation (#504). Using shared typed constants —
// rather than bare string literals at each call site — guarantees the sign and
// validate sides agree. Using a bare string literal instead of these constants
// at a single call site would reject every message of that message type
// (sign and validate would compute different MACs), so this must not be
// stringly-typed.
const (
	MsgTypeJoin      MsgType = "join"
	MsgTypeLeave     MsgType = "leave"
	MsgTypeHeartbeat MsgType = "heartbeat"
	// Raft transport connection-handshake labels (GHSA-wwfh-qrfq-6f8g). The
	// two directions carry distinct labels so a response captured in one
	// direction cannot be replayed as the other: the server proves the secret
	// with MsgTypeRaftAuthResp over the client's nonce, the client with
	// MsgTypeRaftAuth over the server's nonce.
	MsgTypeRaftAuth     MsgType = "raft-auth"
	MsgTypeRaftAuthResp MsgType = "raft-auth-resp"
)

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

// ComputeFetchHMAC computes HMAC-SHA256 for a peer file-fetch request. The
// message format binds the requested path into the signed payload so a stolen
// MAC for file A cannot be replayed within the freshness window to fetch a
// different file B.
// Fields are NUL-delimited. (The coordinator handshake family moved to
// length-prefixed canonicalization in 26.09.2 — see handshake_auth.go and
// canonicalSyncInput. These families keep the NUL format: their field sets
// are fixed-arity and their trailing timestamp makes re-partitioning
// inert. Do not add a variable-length field to one without moving it.)
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
// Fields are NUL-delimited. (The coordinator handshake family moved to
// length-prefixed canonicalization in 26.09.2 — see handshake_auth.go and
// canonicalSyncInput. These families keep the NUL format: their field sets
// are fixed-arity and their trailing timestamp makes re-partitioning
// inert. Do not add a variable-length field to one without moving it.)
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
// NUL-delimited (see ComputeFetchHMAC for the caveat on that format).
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
// ComputeForwardHMAC / ComputeFetchHMAC / the handshake family so cross-endpoint
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

// HMACTimestampTolerance is the symmetric window (past and future) the
// cluster HMAC validators accept on a signed timestamp. Five minutes
// is the same window enforced by every Compute*HMAC consumer in this
// package (join, leave, heartbeat, the handshake responses, fetch,
// forward-apply, cache-invalidate, the origin stream, pending
// registrations, flush scope). Centralized here so a future operator can tune
// all sites in one place; Gemini round 1 on PR #449 flagged the
// hardcoded 5*time.Minute scattered across coordinator.go.
//
// NOTE: a NonceCache built for this tolerance does NOT use it as its own
// TTL — it must outlive the window by more than the window, because a
// message may be stamped up to one tolerance in the FUTURE and is still
// accepted. NewNonceCache derives the right lifetime itself; pass it the
// tolerance, not a TTL.
const HMACTimestampTolerance = 5 * time.Minute

// ComputeOriginStreamHelloHMAC authenticates a standby writer opening the
// origin stream (#1207). Its own label, so no other cluster MAC replays here.
//
// Format: "origin-stream-hello" \x00 version \x00 nonce \x00 nodeID \x00 clusterName \x00 timestamp
func ComputeOriginStreamHelloHMAC(sharedSecret string, version int, nonce, nodeID, clusterName string, timestamp int64) string {
	return hex.EncodeToString(computeRawHMAC(sharedSecret, originHelloMessage(version, nonce, nodeID, clusterName, timestamp)))
}

func originHelloMessage(version int, nonce, nodeID, clusterName string, timestamp int64) string {
	return fmt.Sprintf("origin-stream-hello\x00%d\x00%s\x00%s\x00%s\x00%d", version, nonce, nodeID, clusterName, timestamp)
}

// ValidateOriginStreamHelloHMAC checks a hello's MAC and freshness.
func ValidateOriginStreamHelloHMAC(sharedSecret string, version int, nonce, nodeID, clusterName string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if err := checkHMACTimestamp("origin-stream-hello", timestamp, tolerance); err != nil {
		return err
	}
	expected := computeRawHMAC(sharedSecret, originHelloMessage(version, nonce, nodeID, clusterName, timestamp))
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("origin-stream-hello HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}

// ComputeOriginStreamHelloAckHMAC authenticates the primary's answer, bound
// to the hello's nonce so a recorded answer cannot be replayed to a new hello.
//
// Format: "origin-stream-hello-ack" \x00 helloNonce \x00 originNodeID \x00 instance \x00 error \x00 timestamp
func ComputeOriginStreamHelloAckHMAC(sharedSecret, helloNonce, originNodeID, instance, errText string, timestamp int64) string {
	return hex.EncodeToString(computeRawHMAC(sharedSecret, fmt.Sprintf("origin-stream-hello-ack\x00%s\x00%s\x00%s\x00%s\x00%d", helloNonce, originNodeID, instance, errText, timestamp)))
}

// ValidateOriginStreamHelloAckHMAC checks the primary's answer.
func ValidateOriginStreamHelloAckHMAC(sharedSecret, helloNonce, originNodeID, instance, errText string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if err := checkHMACTimestamp("origin-stream-hello-ack", timestamp, tolerance); err != nil {
		return err
	}
	expected := computeRawHMAC(sharedSecret, fmt.Sprintf("origin-stream-hello-ack\x00%s\x00%s\x00%s\x00%s\x00%d", helloNonce, originNodeID, instance, errText, timestamp))
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("origin-stream-hello-ack HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}

// ComputePendingRegistrationsQueryHMAC authenticates a reconciler asking a
// node for its pending registrations (#1214).
//
// Format: "pending-registrations-query" \x00 nonce \x00 nodeID \x00 clusterName \x00 timestamp
func ComputePendingRegistrationsQueryHMAC(sharedSecret, nonce, nodeID, clusterName string, timestamp int64) string {
	return hex.EncodeToString(computeRawHMAC(sharedSecret, pendingQueryMessage(nonce, nodeID, clusterName, timestamp)))
}

func pendingQueryMessage(nonce, nodeID, clusterName string, timestamp int64) string {
	return fmt.Sprintf("pending-registrations-query\x00%s\x00%s\x00%s\x00%d", nonce, nodeID, clusterName, timestamp)
}

// ValidatePendingRegistrationsQueryHMAC checks a query's MAC and freshness.
func ValidatePendingRegistrationsQueryHMAC(sharedSecret, nonce, nodeID, clusterName string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if err := checkHMACTimestamp("pending-registrations-query", timestamp, tolerance); err != nil {
		return err
	}
	expected := computeRawHMAC(sharedSecret, pendingQueryMessage(nonce, nodeID, clusterName, timestamp))
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("pending-registrations-query HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}

// ComputePendingRegistrationsReplyHMAC authenticates a node's answer, bound to
// the query's nonce so a recorded answer cannot be replayed to a new query,
// and over a SHA-256 digest of the paths so none can be stripped on the way.
//
// Format: "pending-registrations-reply" \x00 queryNonce \x00 nodeID \x00 clusterName \x00 status \x00 sha256(paths, NUL-separated) \x00 timestamp
func ComputePendingRegistrationsReplyHMAC(sharedSecret, queryNonce, nodeID, clusterName, status string, paths []string, timestamp int64) string {
	return hex.EncodeToString(computeRawHMAC(sharedSecret, pendingReplyMessage(queryNonce, nodeID, clusterName, status, paths, timestamp)))
}

func pendingReplyMessage(queryNonce, nodeID, clusterName, status string, paths []string, timestamp int64) string {
	h := sha256.New()
	for _, p := range paths {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return fmt.Sprintf("pending-registrations-reply\x00%s\x00%s\x00%s\x00%s\x00%x\x00%d", queryNonce, nodeID, clusterName, status, h.Sum(nil), timestamp)
}

// ValidatePendingRegistrationsReplyHMAC checks a node's answer.
func ValidatePendingRegistrationsReplyHMAC(sharedSecret, queryNonce, nodeID, clusterName, status string, paths []string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if err := checkHMACTimestamp("pending-registrations-reply", timestamp, tolerance); err != nil {
		return err
	}
	expected := computeRawHMAC(sharedSecret, pendingReplyMessage(queryNonce, nodeID, clusterName, status, paths, timestamp))
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("pending-registrations-reply HMAC validation failed: shared secret mismatch, paths tampered, or malformed MAC")
	}
	return nil
}

// ComputeFlushScopeRequestHMAC authenticates a flush scope request (#1265).
//
// Format: "flush-scope-request" \x00 nonce \x00 nodeID \x00 clusterName \x00 database \x00 measurement \x00 timestamp
func ComputeFlushScopeRequestHMAC(sharedSecret, nonce, nodeID, clusterName, database, measurement string, timestamp int64) string {
	return hex.EncodeToString(computeRawHMAC(sharedSecret, flushScopeRequestMessage(nonce, nodeID, clusterName, database, measurement, timestamp)))
}

func flushScopeRequestMessage(nonce, nodeID, clusterName, database, measurement string, timestamp int64) string {
	return fmt.Sprintf("flush-scope-request\x00%s\x00%s\x00%s\x00%s\x00%s\x00%d", nonce, nodeID, clusterName, database, measurement, timestamp)
}

// ValidateFlushScopeRequestHMAC checks a request's MAC and freshness.
func ValidateFlushScopeRequestHMAC(sharedSecret, nonce, nodeID, clusterName, database, measurement string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if err := checkHMACTimestamp("flush-scope-request", timestamp, tolerance); err != nil {
		return err
	}
	expected := computeRawHMAC(sharedSecret, flushScopeRequestMessage(nonce, nodeID, clusterName, database, measurement, timestamp))
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("flush-scope-request HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}

// ComputeFlushScopeReplyHMAC authenticates a node's answer, bound to the
// request's nonce and scope, over a SHA-256 digest of the paths.
//
// Format: "flush-scope-reply" \x00 requestNonce \x00 nodeID \x00 clusterName \x00 database \x00 measurement \x00 status \x00 sha256(paths, NUL-separated) \x00 timestamp
func ComputeFlushScopeReplyHMAC(sharedSecret, requestNonce, nodeID, clusterName, database, measurement, status string, paths []string, timestamp int64) string {
	return hex.EncodeToString(computeRawHMAC(sharedSecret, flushScopeReplyMessage(requestNonce, nodeID, clusterName, database, measurement, status, paths, timestamp)))
}

func flushScopeReplyMessage(requestNonce, nodeID, clusterName, database, measurement, status string, paths []string, timestamp int64) string {
	h := sha256.New()
	for _, p := range paths {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return fmt.Sprintf("flush-scope-reply\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%x\x00%d", requestNonce, nodeID, clusterName, database, measurement, status, h.Sum(nil), timestamp)
}

// ValidateFlushScopeReplyHMAC checks a node's answer.
func ValidateFlushScopeReplyHMAC(sharedSecret, requestNonce, nodeID, clusterName, database, measurement, status string, paths []string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if err := checkHMACTimestamp("flush-scope-reply", timestamp, tolerance); err != nil {
		return err
	}
	expected := computeRawHMAC(sharedSecret, flushScopeReplyMessage(requestNonce, nodeID, clusterName, database, measurement, status, paths, timestamp))
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("flush-scope-reply HMAC validation failed: shared secret mismatch, paths tampered, or malformed MAC")
	}
	return nil
}

// DeriveOriginStreamSessionKey derives the origin stream's per-connection key
// from the shared secret and the hello nonce, under its own HKDF info string.
func DeriveOriginStreamSessionKey(sharedSecret, helloNonce string) ([]byte, error) {
	key, err := hkdf.Key(sha256.New, []byte(sharedSecret), []byte(helloNonce), "arc-origin-stream-session:", 32)
	if err != nil {
		return nil, fmt.Errorf("derive origin stream session key: %w", err)
	}
	return key, nil
}

func checkHMACTimestamp(label string, timestamp int64, tolerance time.Duration) error {
	drift := time.Now().Unix() - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		return fmt.Errorf("%s auth timestamp expired or out of tolerance (drift: %ds, tolerance: %ds)", label, drift, int64(tolerance.Seconds()))
	}
	return nil
}

// ComputeLocalPoliciesQueryHMAC authenticates a local policies query (#1324).
//
// Format: "local-policies-query" \x00 nonce \x00 nodeID \x00 clusterName \x00 kind \x00 epoch \x00 timestamp
func ComputeLocalPoliciesQueryHMAC(sharedSecret, nonce, nodeID, clusterName, kind string, epoch uint64, timestamp int64) string {
	return hex.EncodeToString(computeRawHMAC(sharedSecret, localPoliciesQueryMessage(nonce, nodeID, clusterName, kind, epoch, timestamp)))
}

func localPoliciesQueryMessage(nonce, nodeID, clusterName, kind string, epoch uint64, timestamp int64) string {
	return fmt.Sprintf("local-policies-query\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d", nonce, nodeID, clusterName, kind, epoch, timestamp)
}

// ValidateLocalPoliciesQueryHMAC checks a query's MAC and freshness.
func ValidateLocalPoliciesQueryHMAC(sharedSecret, nonce, nodeID, clusterName, kind string, epoch uint64, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if err := checkHMACTimestamp("local-policies-query", timestamp, tolerance); err != nil {
		return err
	}
	expected := computeRawHMAC(sharedSecret, localPoliciesQueryMessage(nonce, nodeID, clusterName, kind, epoch, timestamp))
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("local-policies-query HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}

// ComputeLocalPoliciesReplyHMAC authenticates a node's rows, bound to the
// query's nonce, kind and epoch, over a SHA-256 digest of the rows.
//
// Format: "local-policies-reply" \x00 queryNonce \x00 nodeID \x00 clusterName \x00 kind \x00 epoch \x00 status \x00 sha256(policies) \x00 timestamp
func ComputeLocalPoliciesReplyHMAC(sharedSecret, queryNonce, nodeID, clusterName, kind string, epoch uint64, status string, policies []byte, timestamp int64) string {
	return hex.EncodeToString(computeRawHMAC(sharedSecret, localPoliciesReplyMessage(queryNonce, nodeID, clusterName, kind, epoch, status, policies, timestamp)))
}

func localPoliciesReplyMessage(queryNonce, nodeID, clusterName, kind string, epoch uint64, status string, policies []byte, timestamp int64) string {
	sum := sha256.Sum256(policies)
	return fmt.Sprintf("local-policies-reply\x00%s\x00%s\x00%s\x00%s\x00%d\x00%s\x00%x\x00%d", queryNonce, nodeID, clusterName, kind, epoch, status, sum[:], timestamp)
}

// ValidateLocalPoliciesReplyHMAC checks a node's answer.
func ValidateLocalPoliciesReplyHMAC(sharedSecret, queryNonce, nodeID, clusterName, kind string, epoch uint64, status string, policies []byte, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if err := checkHMACTimestamp("local-policies-reply", timestamp, tolerance); err != nil {
		return err
	}
	expected := computeRawHMAC(sharedSecret, localPoliciesReplyMessage(queryNonce, nodeID, clusterName, kind, epoch, status, policies, timestamp))
	if !constantTimeHexEqual(expected, receivedMAC) {
		return fmt.Errorf("local-policies-reply HMAC validation failed: shared secret mismatch, rows tampered, or malformed MAC")
	}
	return nil
}
