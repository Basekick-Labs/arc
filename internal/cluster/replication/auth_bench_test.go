// Benchmarks for the GHSA-wfgr-8x84-22q7 stream-authentication path.
// Cost target was set by the user requirement: replication must not
// regress measurably at Arc's ingest profile (19.9M records/sec). The
// fast path is the per-entry tag — checkpoints fire 1/1024 of the time
// so their full-HMAC cost amortises away.
package replication

import (
	"crypto/sha256"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/security"
)

// BenchmarkComputeReplicationEntryTag measures the per-entry sender
// stamping cost. Run with:
//
//	go test -bench=BenchmarkComputeReplicationEntryTag -benchmem \
//	    ./internal/cluster/replication/...
func BenchmarkComputeReplicationEntryTag(b *testing.B) {
	sessionKey, err := security.DeriveReplicationSessionKey(
		"test-shared-secret", "test-handshake-nonce-aaaaaaaaaaaa",
	)
	if err != nil {
		b.Fatal(err)
	}

	// 256-byte payload is roughly representative of a typical Arc
	// replication entry (a small batch of records msgpack-encoded).
	// Smaller and larger payloads are covered separately below.
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = security.ComputeReplicationEntryTag(sessionKey, uint64(i), payload)
	}
}

// BenchmarkComputeReplicationEntryTagFromHash measures the hot-path
// variant used by broadcastEntry: SHA-256 is computed once per
// broadcast and reused across every reader's tag. At N readers this
// drops the per-broadcast hash cost from N × sha256(payload) to 1 ×
// sha256(payload). Expect ~half the per-call cost of
// ComputeReplicationEntryTag (skips the SHA-256 over payload).
func BenchmarkComputeReplicationEntryTagFromHash(b *testing.B) {
	sessionKey, err := security.DeriveReplicationSessionKey(
		"test-shared-secret", "test-handshake-nonce-aaaaaaaaaaaa",
	)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	payloadHash := sha256.Sum256(payload)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = security.ComputeReplicationEntryTagFromHash(sessionKey, uint64(i), payloadHash)
	}
}

// BenchmarkComputeReplicationEntryTagWithMAC measures the production
// hot-path variant used by sendToReader: a single reusable hmac.Hash
// per reader, Reset() between entries. Expected to be markedly faster
// than ComputeReplicationEntryTagFromHash (no per-call hmac.New
// allocation) and produces measurably fewer allocations per op.
func BenchmarkComputeReplicationEntryTagWithMAC(b *testing.B) {
	sessionKey, err := security.DeriveReplicationSessionKey(
		"test-shared-secret", "test-handshake-nonce-aaaaaaaaaaaa",
	)
	if err != nil {
		b.Fatal(err)
	}
	mac := security.NewReplicationEntryHMAC(sessionKey)
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	payloadHash := sha256.Sum256(payload)
	var tag [security.ReplicationEntryTagLen]byte

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		security.ComputeReplicationEntryTagWithMAC(mac, uint64(i), payloadHash, tag[:])
	}
}

// BenchmarkValidateReplicationEntryTagWithMAC measures the production
// hot-path variant used by receiveLoop: per-connection reusable mac +
// pre-computed payload hash (the receiver already computes the hash
// for cumulativeHash so this is free). Expected to be in the same
// ballpark as ComputeReplicationEntryTagWithMAC.
func BenchmarkValidateReplicationEntryTagWithMAC(b *testing.B) {
	sessionKey, err := security.DeriveReplicationSessionKey(
		"test-shared-secret", "test-handshake-nonce-aaaaaaaaaaaa",
	)
	if err != nil {
		b.Fatal(err)
	}
	mac := security.NewReplicationEntryHMAC(sessionKey)
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	payloadHash := sha256.Sum256(payload)
	var tag [security.ReplicationEntryTagLen]byte
	security.ComputeReplicationEntryTagWithMAC(mac, 0, payloadHash, tag[:])

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := security.ValidateReplicationEntryTagWithMAC(mac, 0, payloadHash, tag[:]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkValidateReplicationEntryTag measures the per-entry receiver
// verification cost. Expected to be ~2x ComputeReplicationEntryTag
// (compute + constant-time compare).
func BenchmarkValidateReplicationEntryTag(b *testing.B) {
	sessionKey, err := security.DeriveReplicationSessionKey(
		"test-shared-secret", "test-handshake-nonce-aaaaaaaaaaaa",
	)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	tag := security.ComputeReplicationEntryTag(sessionKey, 0, payload)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Reuse the same sequence + payload so we measure the
		// verify cost, not memory pressure from regenerating the
		// tag every iteration.
		if err := security.ValidateReplicationEntryTag(sessionKey, 0, payload, tag); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkComputeReplicationEntryTag_64B(b *testing.B) {
	benchmarkComputeEntryTagPayload(b, 64)
}
func BenchmarkComputeReplicationEntryTag_1KB(b *testing.B) {
	benchmarkComputeEntryTagPayload(b, 1024)
}
func BenchmarkComputeReplicationEntryTag_8KB(b *testing.B) {
	benchmarkComputeEntryTagPayload(b, 8192)
}

func benchmarkComputeEntryTagPayload(b *testing.B, size int) {
	sessionKey, err := security.DeriveReplicationSessionKey(
		"test-shared-secret", "test-handshake-nonce-aaaaaaaaaaaa",
	)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = security.ComputeReplicationEntryTag(sessionKey, uint64(i), payload)
	}
}
