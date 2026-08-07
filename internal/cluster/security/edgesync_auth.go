package security

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Edge-sync HMAC domain labels.
//
// Every edge-sync MAC begins with one of these, so a MAC minted for one sync
// operation cannot be replayed as another within the freshness window — the
// same label-based domain separation ComputeCacheInvalidateHMAC and the
// join-family MACs use (#504).
//
// The label alone is NOT sufficient to separate these from the unlabeled
// cluster MACs in auth.go, and it is worth being precise about why.
// ComputeFetchHMAC's canonical input is
// `nonce \x00 nodeID \x00 clusterName \x00 path \x00 timestamp` — five
// NUL-delimited fields with no label. An attacker who sets the fetch nonce to
// the literal string "sync-file" and embeds NUL bytes inside nodeID and path
// manufactures the two extra delimiters that turn five fields into seven, and
// the resulting canonical input is byte-identical to a sync file MAC over
// attacker-chosen (spokeID, hubID, path, contentSHA256). Since spokeID is what
// enforces §6.3 namespacing, that is a cross-spoke namespace write.
//
// This is not hypothetical on the wire: ComputeFetchHMAC travels over the raw
// TCP coordinator protocol (filereplication/fetch_client.go dials via
// security.Dial and JSON-encodes the message), and JSON carries NUL happily.
// It is canonicalSyncInput's length prefixing — not the label, and not the
// NUL guard — that closes the forgery. The label remains for separation
// BETWEEN the two sync families, which is what it is good for.
const (
	// syncLabelReconcile marks a batch reconcile request (§5.1).
	syncLabelReconcile = "sync-reconcile"

	// syncLabelFile marks a single-file transfer (§5.2), covering both the
	// streaming PUT and the HEAD probe — they address the same file with the
	// same content hash, so a MAC valid for one is legitimately valid for the
	// other.
	syncLabelFile = "sync-file"
)

// ErrSyncAuthMalformedField is returned when a field carries a NUL byte.
//
// This is defense in depth, not the mechanism that makes the encoding
// unambiguous — canonicalSyncInput's length prefixing does that, and it holds
// regardless of field contents. The guard exists because a NUL in a spoke ID,
// hub ID, path, or hex digest is malformed input in its own right: every one
// of those is an operator-configured identifier or a storage key, so a NUL
// means something upstream is wrong and the request should be refused rather
// than authenticated.
var ErrSyncAuthMalformedField = errors.New("security: sync auth field contains a NUL byte")

// ErrSyncAuthExpired is returned when a request's timestamp falls outside the
// freshness window. Distinguished from a MAC mismatch so an operator can tell
// clock skew (fix NTP) from a wrong secret or a forgery attempt.
var ErrSyncAuthExpired = errors.New("security: sync auth timestamp expired")

// ErrSyncAuthInvalid is returned when the MAC does not match. Deliberately
// says nothing about why: the secret being wrong, the path being tampered
// with, and the MAC being malformed are indistinguishable to a caller, which
// is what keeps this from becoming an oracle.
var ErrSyncAuthInvalid = errors.New("security: sync HMAC validation failed")

// ComputeSyncFileHMAC computes the MAC for a single-file sync transfer.
//
// It binds five things, each closing a specific hole:
//
//   - spokeID — the sender's identity. The hub rewrites incoming paths under
//     {spoke_id}/, so binding it here is what stops one spoke from writing
//     into another's namespace (§6.3).
//   - hubID — the intended recipient. Without it, a MAC captured en route to
//     one hub could be replayed at another that shares the spoke's secret.
//   - targetPath — the file being written. Prevents a captured MAC for file A
//     from being reused to write file B (the same reasoning as
//     ComputeFetchHMAC's path binding).
//   - contentSHA256 — the spoke's DECLARED digest for the bytes. Be precise
//     about what this buys: it authenticates the declaration, so a network
//     attacker cannot alter bytes in flight without failing the hub's own
//     hash check, and cannot replay a captured MAC with substituted content.
//     It says nothing about a legitimate spoke, which picks both the digest
//     and the bytes. The binding is only load-bearing BECAUSE the hub
//     independently recomputes the digest over the received stream and
//     refuses to commit on a mismatch (§5.2, verify-before-commit) — that
//     receiver-side check is a hard requirement on the handler, not optional.
//   - timestamp — freshness, checked against HMACTimestampTolerance.
//
// Fields are NUL-delimited so that no two distinct field arrangements can
// produce the same signed input — a value cannot absorb its neighbour across
// the boundary.
//
// Over HTTPS this is belt-and-braces: net/http rejects a NUL in a header value
// outright ("invalid header field value"), so an HTTP sender cannot even
// transmit one. The delimiting is not allowed to DEPEND on that, though — the
// planned S3-relay and sneakernet transports (§10) carry these fields as
// object metadata and bundle entries, where no such rule applies. The
// construction has to be unambiguous on its own.
//
// Format: "sync-file" \x00 nonce \x00 spokeID \x00 hubID \x00 targetPath \x00 contentSHA256 \x00 timestamp
func ComputeSyncFileHMAC(sharedSecret, nonce, spokeID, hubID, targetPath, contentSHA256 string, timestamp int64) (string, error) {
	if err := rejectSyncNUL(nonce, spokeID, hubID, targetPath, contentSHA256); err != nil {
		return "", err
	}
	return hex.EncodeToString(
		computeSyncFileHMACRaw(sharedSecret, nonce, spokeID, hubID, targetPath, contentSHA256, timestamp)), nil
}

func computeSyncFileHMACRaw(sharedSecret, nonce, spokeID, hubID, targetPath, contentSHA256 string, timestamp int64) []byte {
	return computeRawHMAC(sharedSecret, canonicalSyncInput(
		syncLabelFile, nonce, spokeID, hubID, targetPath, contentSHA256,
		strconv.FormatInt(timestamp, 10)))
}

// ValidateSyncFileHMAC checks a single-file transfer MAC and its freshness.
//
// Freshness is checked FIRST and separately from the MAC comparison so an
// expired-but-authentic request reports clock skew rather than a generic
// failure — the difference between "NTP is broken on the rocket" and "someone
// is forging requests", which an operator in the field needs to tell apart.
//
// This does NOT check the nonce cache; the caller must do that after a
// successful validation (see the note on SyncNonceKey). Splitting them keeps a
// forged request from burning a nonce-cache slot.
func ValidateSyncFileHMAC(sharedSecret, nonce, spokeID, hubID, targetPath, contentSHA256 string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	// Malformed-input check. The canonical encoding is already unambiguous
	// (see canonicalSyncInput), so this is not load-bearing for forgery
	// resistance — it refuses input that should never have been constructed.
	if err := rejectSyncNUL(nonce, spokeID, hubID, targetPath, contentSHA256); err != nil {
		return err
	}
	if err := checkSyncFreshness(timestamp, tolerance); err != nil {
		return err
	}
	expected := computeSyncFileHMACRaw(sharedSecret, nonce, spokeID, hubID, targetPath, contentSHA256, timestamp)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return ErrSyncAuthInvalid
	}
	return nil
}

// ComputeSyncReconcileHMAC computes the MAC for a batch reconcile request.
//
// The reconcile body is a list of (path, sha256, size) triples that can run to
// tens of megabytes, so — like ComputeForwardHMAC — the MAC binds a SHA-256 of
// the body rather than the bytes themselves, keeping the HMAC input a fixed
// length regardless of backlog size.
//
// Binding the body digest is not optional. Reconcile tells the spoke what the
// hub does and does not hold, so an unbound MAC would let an attacker replay a
// captured request with a substituted path list and learn which arbitrary
// paths exist on the hub — a data-inventory oracle. §7 calls this out: even
// reconcile leaks "what data exists on the hub" if unauthenticated.
//
// Format: "sync-reconcile" \x00 nonce \x00 spokeID \x00 hubID \x00 bodySHA256 \x00 timestamp
func ComputeSyncReconcileHMAC(sharedSecret, nonce, spokeID, hubID string, body []byte, timestamp int64) (string, error) {
	if err := rejectSyncNUL(nonce, spokeID, hubID); err != nil {
		return "", err
	}
	return hex.EncodeToString(
		computeSyncReconcileHMACRaw(sharedSecret, nonce, spokeID, hubID, body, timestamp)), nil
}

func computeSyncReconcileHMACRaw(sharedSecret, nonce, spokeID, hubID string, body []byte, timestamp int64) []byte {
	sum := sha256.Sum256(body)
	return computeRawHMAC(sharedSecret, canonicalSyncInput(
		syncLabelReconcile, nonce, spokeID, hubID, hex.EncodeToString(sum[:]),
		strconv.FormatInt(timestamp, 10)))
}

// ValidateSyncReconcileHMAC checks a reconcile MAC and its freshness.
//
// body must be the exact bytes the hub received — hash it before parsing, not
// after re-serializing, or a semantically-equivalent-but-differently-encoded
// body will fail to match.
func ValidateSyncReconcileHMAC(sharedSecret, nonce, spokeID, hubID string, body []byte, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	// See ValidateSyncFileHMAC. body is exempt: it is hashed to a fixed-length
	// hex digest before entering the canonical input, so a NUL in the payload
	// is both harmless and legitimate.
	if err := rejectSyncNUL(nonce, spokeID, hubID); err != nil {
		return err
	}
	if err := checkSyncFreshness(timestamp, tolerance); err != nil {
		return err
	}
	expected := computeSyncReconcileHMACRaw(sharedSecret, nonce, spokeID, hubID, body, timestamp)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return ErrSyncAuthInvalid
	}
	return nil
}

// canonicalSyncInput serializes fields so that no two distinct field tuples
// can produce the same string — including tuples of DIFFERENT arity, and
// including fields whose contents are attacker-chosen.
//
// Each field is written as its byte length, a colon, then the field:
//
//	7:sync-file5:nonce5:spoke3:hub4:path3:sha10:1750000000
//
// A NUL-delimited encoding is not sufficient here, and this is the concrete
// reason. The unlabeled cluster MAC families in auth.go build their input as
// `nonce \x00 nodeID \x00 clusterName \x00 path \x00 timestamp` — five
// fields. An attacker who sets the fetch nonce to the literal "sync-file" and
// embeds NUL bytes inside nodeID and path manufactures the extra delimiters
// that turn five fields into seven, producing a byte-identical canonical
// input to a sync file MAC over attacker-chosen (spokeID, hubID, path,
// contentSHA256). Since spokeID enforces §6.3 namespacing, that is a
// cross-spoke namespace write. Rejecting NUL in the SYNC fields does not
// close it: the NULs live on the fetch side, which this package's sync
// validators never see.
//
// Length prefixing removes the ambiguity at the source. A field cannot absorb
// its neighbour, because its length is declared before its bytes are read, so
// no arrangement of contents can re-partition the string. This is why the
// construction does not depend on the transport filtering anything — which
// matters, since ComputeFetchHMAC travels over the raw TCP coordinator
// protocol as JSON (NUL passes through fine), and the planned S3-relay and
// sneakernet transports (§10) have no header rules at all.
func canonicalSyncInput(fields ...string) string {
	var b strings.Builder
	for _, f := range fields {
		b.WriteString(strconv.Itoa(len(f)))
		b.WriteByte(':')
		b.WriteString(f)
	}
	return b.String()
}

// rejectSyncNUL reports an error if any field carries a NUL byte.
//
// Applied on both the compute and validate paths. Note this is NOT what closes
// the cross-family forgery — length prefixing is. Guarding only the sync
// fields could not close it anyway: in that attack the NULs live in the
// unlabeled cluster MAC's fields, which these validators never see.
func rejectSyncNUL(fields ...string) error {
	for _, f := range fields {
		if strings.ContainsRune(f, 0) {
			return ErrSyncAuthMalformedField
		}
	}
	return nil
}

// checkSyncFreshness enforces the symmetric timestamp window.
//
// Symmetric because a request from the future is as suspicious as one from the
// past: an edge box with a badly-skewed clock produces both, and accepting
// arbitrarily-future timestamps would let a captured MAC stay valid
// indefinitely.
func checkSyncFreshness(timestamp int64, tolerance time.Duration) error {
	// Timestamps are second-granularity, so a sub-second tolerance truncates
	// to zero and rejects everything — fail-closed, but a confusing footgun
	// for a future caller who passes 500ms. Refuse it explicitly instead.
	if tolerance < time.Second {
		return fmt.Errorf("security: sync auth tolerance %v is below the one-second timestamp granularity", tolerance)
	}

	drift := time.Now().Unix() - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		return fmt.Errorf("%w (drift: %ds, tolerance: %ds)",
			ErrSyncAuthExpired, drift, int64(tolerance.Seconds()))
	}
	return nil
}

// ReplayGuard is the subset of NonceCache the sync validators need. Defined as
// an interface so a handler can pass the coordinator's cache, and a test can
// pass a stub, without this package depending on either.
type ReplayGuard interface {
	// Track records a nonce and reports true if it is new. False means the
	// (id, nonce) pair was already seen inside the TTL window.
	Track(id, nonce string) bool
}

// ErrSyncAuthReplay is returned when a request's nonce has already been used
// inside the freshness window.
//
// Distinguished from a MAC failure because it is not a forgery: the request is
// authentic, which is exactly what makes it worth alerting on. A spoke does
// not resend the same nonce, so seeing this means either a broken client or
// someone replaying captured traffic.
var ErrSyncAuthReplay = errors.New("security: sync auth nonce already used")

// ValidateSyncFileHMACWithReplay validates the MAC and then consumes the nonce.
//
// Prefer this over ValidateSyncFileHMAC. Freshness alone is NOT replay
// protection: inside the tolerance window an attacker can resend a captured
// request byte for byte and the MAC still verifies, because every bound field
// is unchanged. The nonce cache is what makes a request single-use, and
// splitting that into a step the handler must remember is how it gets
// forgotten — a handler that validates and returns nil is fully replayable for
// five minutes, with every test still passing.
//
// The nonce is consumed only AFTER the MAC verifies, so a forged request
// cannot burn a cache slot (which would otherwise let an attacker lock out a
// legitimate nonce).
func ValidateSyncFileHMACWithReplay(guard ReplayGuard, sharedSecret, nonce, spokeID, hubID, targetPath, contentSHA256 string, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if guard == nil {
		return errors.New("security: sync replay guard is required")
	}
	if err := ValidateSyncFileHMAC(sharedSecret, nonce, spokeID, hubID, targetPath, contentSHA256, timestamp, receivedMAC, tolerance); err != nil {
		return err
	}
	key, err := SyncNonceKey(spokeID)
	if err != nil {
		return err
	}
	if !guard.Track(key, nonce) {
		return ErrSyncAuthReplay
	}
	return nil
}

// ValidateSyncReconcileHMACWithReplay validates the MAC and then consumes the
// nonce. See ValidateSyncFileHMACWithReplay.
func ValidateSyncReconcileHMACWithReplay(guard ReplayGuard, sharedSecret, nonce, spokeID, hubID string, body []byte, timestamp int64, receivedMAC string, tolerance time.Duration) error {
	if guard == nil {
		return errors.New("security: sync replay guard is required")
	}
	if err := ValidateSyncReconcileHMAC(sharedSecret, nonce, spokeID, hubID, body, timestamp, receivedMAC, tolerance); err != nil {
		return err
	}
	key, err := SyncNonceKey(spokeID)
	if err != nil {
		return err
	}
	if !guard.Track(key, nonce) {
		return ErrSyncAuthReplay
	}
	return nil
}

// SyncNonceKey returns the identity a sync request's nonce must be tracked
// under in a NonceCache.
//
// NonceCache.Track keys on (id, nonce), so the id must be the SPOKE — two
// spokes independently generating the same nonce is astronomically unlikely
// but must not be able to cause one to reject the other's request. Passing
// something coarser (a hub ID, a constant) would make one spoke's traffic
// evict or block another's.
//
// Freshness alone does not stop replay: within the tolerance window an
// attacker can resend a captured request verbatim, and the MAC still verifies
// because every bound field is unchanged. The nonce cache is what makes each
// request single-use. Callers MUST track after validating — see §7.
func SyncNonceKey(spokeID string) (string, error) {
	// A NUL in spokeID would let two different (spokeID, nonce) pairs produce
	// the same cache key, because NonceCache.Track builds `id \x00 nonce`:
	//   ("spoke\x00extra", "nonce") and ("spoke", "extra\x00nonce")
	// both yield "sync:spoke\x00extra\x00nonce" — so one spoke could consume
	// another's replay slot.
	//
	// Rejected rather than escaped. Percent-encoding looked tempting but is
	// not injective without also escaping the escape character: "a\x00b" and a
	// literal "a%00b" would collide, trading one collision for another. A NUL
	// in a spoke ID is malformed input — spoke IDs come from the hub's
	// registry — so refusing it is both simpler and stronger.
	if strings.ContainsRune(spokeID, 0) {
		return "", ErrSyncAuthMalformedField
	}
	return "sync:" + spokeID, nil
}

// Compile-time check that the concrete cache satisfies the interface, so a
// change to NonceCache.Track cannot silently break sync's replay protection.
var _ ReplayGuard = (*NonceCache)(nil)
