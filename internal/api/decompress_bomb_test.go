package api

import (
	"bytes"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
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
