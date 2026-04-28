package api

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
)

// TestDecompressZstdPooled_BombRejected_WithoutOOM is a regression
// test for the round-4 gemini finding: the original implementation
// used zstd.Decoder.DecodeAll, which grows its output buffer to fit
// the entire decompressed stream regardless of the WithDecoderMaxMemory
// setting (that option only bounds the decoder's per-frame window,
// not the output buffer). A high-ratio compressed payload could OOM
// the process before any post-hoc len(buf) check could fire.
//
// The fix uses streaming Reset + io.LimitReader + io.ReadAll so the
// decoded byte count is hard-bounded during decoding. This test
// constructs a small compressed input that decompresses to ~256 MB
// of zeros — well above the 100 MB default cap. The helper must
// reject it with a "exceeds limit" error rather than ballooning
// memory.
func TestDecompressZstdPooled_BombRejected_WithoutOOM(t *testing.T) {
	// 256 MB of zeros compresses to a few hundred bytes with zstd.
	const decompressedSize = 256 * 1024 * 1024
	plaintext := bytes.Repeat([]byte{0}, decompressedSize)

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	bomb := encoder.EncodeAll(plaintext, nil)
	if err := encoder.Close(); err != nil {
		t.Fatalf("encoder.Close: %v", err)
	}

	t.Logf("bomb compression: input=%d bytes -> compressed=%d bytes (ratio %.0fx)",
		decompressedSize, len(bomb), float64(decompressedSize)/float64(len(bomb)))

	// 100 MB cap (the LP/TLE default). The bomb expands to 256 MB,
	// so we expect a clean error and zero process death.
	out, err := decompressZstdPooled(bomb, 100*1024*1024)
	if err == nil {
		t.Fatalf("decompressZstdPooled: expected error rejecting 256MB bomb at 100MB cap, got nil err with %d bytes output", len(out))
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("decompressZstdPooled: expected 'exceeds %%MB limit' error, got: %v", err)
	}
}

// TestDecompressZstdPooled_RejectsBenignOversizedPayload covers the
// non-adversarial cap-exceeded path: a regular (low-ratio) compressed
// payload whose decoded size is just over the limit. Must hit the
// same error path as the bomb without spurious decode failures.
func TestDecompressZstdPooled_RejectsBenignOversizedPayload(t *testing.T) {
	// Use random-ish bytes so the compressed size is close to the
	// uncompressed size — this exercises the LimitReader path on a
	// reasonable input rather than the bomb shape.
	plaintext := make([]byte, 200)
	for i := range plaintext {
		plaintext[i] = byte(i % 251)
	}
	// Tile to ~10 KB.
	plaintext = bytes.Repeat(plaintext, 50)

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	compressed := encoder.EncodeAll(plaintext, nil)
	if err := encoder.Close(); err != nil {
		t.Fatalf("encoder.Close: %v", err)
	}

	// Set the cap to half the plaintext size so we know we cross it.
	cap := len(plaintext) / 2
	out, err := decompressZstdPooled(compressed, cap)
	if err == nil {
		t.Fatalf("decompressZstdPooled: expected over-cap error, got %d bytes output", len(out))
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("decompressZstdPooled: expected 'exceeds' error, got: %v", err)
	}
}

// TestDecompressRequest_UncompressedRejectsOverCap is a regression
// test for the round-6 gemini finding: the uncompressed branch of
// decompressRequest had no maxSize check, so a multi-GB raw body
// would be both accepted AND defensively copied, doubling the
// allocation in the uncompressed-OOM vector. The fix applies the
// same maxSize cap to all three branches.
func TestDecompressRequest_UncompressedRejectsOverCap(t *testing.T) {
	const cap = 1024
	rawBody := bytes.Repeat([]byte{'x'}, cap+1)
	body, codec, err := decompressRequest(rawBody, cap)
	if err == nil {
		t.Fatalf("decompressRequest: expected over-cap error, got %d bytes (codec=%q)", len(body), codec)
	}
	if codec != "" {
		t.Fatalf("decompressRequest: expected codec=\"\" for uncompressed-rejection, got %q", codec)
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("decompressRequest: expected 'exceeds' in error, got: %v", err)
	}
}

// TestDecompressRequest_UncompressedAcceptsAtCap pins the boundary:
// a body exactly at maxSize is accepted. Catches off-by-one.
func TestDecompressRequest_UncompressedAcceptsAtCap(t *testing.T) {
	const capSize = 1024
	rawBody := bytes.Repeat([]byte{'y'}, capSize)
	body, codec, err := decompressRequest(rawBody, capSize)
	if err != nil {
		t.Fatalf("decompressRequest: unexpected error at boundary: %v", err)
	}
	if codec != "" {
		t.Fatalf("decompressRequest: expected codec=\"\", got %q", codec)
	}
	if len(body) != capSize {
		t.Fatalf("decompressRequest: expected %d bytes, got %d", capSize, len(body))
	}
	// Defensive copy: body must not alias rawBody.
	body[0] = 'Z'
	if rawBody[0] != 'y' {
		t.Fatalf("decompressRequest: body aliased rawBody — defensive copy regressed")
	}
}

// TestDecompressZstdPooled_HappyPath_RoundTrip pins the non-adversarial
// success path: small payload below the cap decodes cleanly to the
// original bytes. Catches regressions where the streaming-Reset fix
// breaks normal-sized requests.
func TestDecompressZstdPooled_HappyPath_RoundTrip(t *testing.T) {
	plaintext := []byte("metric,host=server01 value=42 1700000000000000000\n" +
		"metric,host=server02 value=43 1700000000000000001\n")

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	compressed := encoder.EncodeAll(plaintext, nil)
	if err := encoder.Close(); err != nil {
		t.Fatalf("encoder.Close: %v", err)
	}

	out, err := decompressZstdPooled(compressed, 0) // 0 → 100 MB default
	if err != nil {
		t.Fatalf("decompressZstdPooled: unexpected error: %v", err)
	}
	if !bytes.Equal(out, plaintext) {
		t.Fatalf("decompressZstdPooled: round-trip mismatch\n want: %q\n  got: %q", plaintext, out)
	}
}

// TestMessagePackDecoder_CopiesStringFields_NoBodyAliasing pins the
// invariant that lets writeMsgPack hand the raw fasthttp body directly
// to the synchronous decoder without a defensive copy on the
// uncompressed path. If a future fork update introduces zero-copy
// string aliasing, this test catches it before production sees
// silent data corruption.
//
// The Basekick-Labs/msgpack/v6 fork uses `string(b)` and io.ReadFull
// internally, both of which always allocate fresh backing storage.
// We verify by mutating the source bytes after Decode returns and
// asserting the decoded string values are unchanged.
//
// The msgpack-columnar hot path benchmarks at 19M+ rec/s; the
// defensive memcpy that gemini r2/r6 wanted everywhere costs ~5%
// throughput on this path, so we keep this invariant load-bearing.
func TestMessagePackDecoder_CopiesStringFields_NoBodyAliasing(t *testing.T) {
	// Build a minimal valid msgpack payload (row format) matching the
	// decoder's expected shape. Strings go through the fork's
	// decode_string.go path (`return string(b)`), which always copies.
	encoded, err := msgpack.Marshal(map[string]interface{}{
		"m":      "cpu",
		"t":      int64(1700000000000),
		"h":      "server01",
		"fields": map[string]interface{}{"value": float64(42)},
		"tags":   map[string]interface{}{"region": "us-east"},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoder := ingest.NewMessagePackDecoder(zerolog.Nop())
	rawBody := append([]byte(nil), encoded...) // separate slice we can mutate

	decoded, err := decoder.Decode(rawBody)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// Decode returns []interface{} of length 1 for a single-record
	// payload. Each element is a *models.Record (row format) with
	// string Measurement and Tags map.
	results, ok := decoded.([]interface{})
	if !ok || len(results) != 1 {
		t.Fatalf("Decode: expected []interface{} of length 1, got %T (len=%d)", decoded, len(results))
	}
	rec, ok := results[0].(*models.Record)
	if !ok {
		t.Fatalf("Decode: expected *models.Record, got %T", results[0])
	}
	if rec.Measurement != "cpu" {
		t.Fatalf("Decode pre-mutation: expected measurement \"cpu\", got %q", rec.Measurement)
	}
	if rec.Tags["region"] != "us-east" {
		t.Fatalf("Decode pre-mutation: expected tag region=us-east, got %q", rec.Tags["region"])
	}

	// Now mutate the source bytes. If the decoder aliased rawBody, the
	// strings already extracted into rec.Measurement / rec.Tags would
	// reflect the mutation.
	for i := range rawBody {
		rawBody[i] = 0xFF
	}

	if rec.Measurement != "cpu" {
		t.Fatalf("decoder is aliasing the input buffer — Measurement was %q after rawBody mutation, want %q. The msgpack-uncompressed fast path requires the decoder to copy strings; if this fails, restore the defensive copy in writeMsgPack and re-evaluate the perf tradeoff.", rec.Measurement, "cpu")
	}
	if rec.Tags["region"] != "us-east" {
		t.Fatalf("decoder is aliasing the input buffer — Tags[region] was %q after rawBody mutation, want %q.", rec.Tags["region"], "us-east")
	}
}
