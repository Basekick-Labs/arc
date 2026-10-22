package api

import (
	"bytes"
	"errors"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// ghsaFpmhFrame is the proof of concept from GHSA-fpmh-frq8-7rj8: a crafted
// two-block zstd frame on which klauspost/compress zstd <= 1.20.1 panics
// (index out of range in fse.nextFast) instead of returning an error.
var ghsaFpmhFrame = slices.Concat(
	[]byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38, 0x74, 0x06, 0x00, 0x42, 0xb8, 0x32, 0x0d, 0x1a, 0x00, 0x00, 0x00, 0x90, 0xaf, 0xfa, 0xff, 0x03, 0x00, 0x80, 0xff, 0x7f},
	bytes.Repeat([]byte{0xa5, 0x94, 0x52, 0x4a, 0x29}, 37),
	[]byte{0xa5, 0x94, 0x12, 0x00, 0xad, 0x56, 0x03, 0xee, 0xff, 0xdf, 0xb3, 0x1a, 0x1c, 0x1a, 0x00, 0xe0, 0xff, 0x1f, 0x0f, 0xb1, 0x9c, 0x5a, 0xcf, 0x4d, 0xe4, 0x1d, 0x7b, 0xab, 0x99, 0x5e, 0x01, 0x51, 0xa4, 0x73, 0xd8, 0xb1, 0xf3, 0x39, 0x4b, 0x67, 0xf6, 0xab, 0x1a, 0xab, 0x1a, 0xab, 0x1a},
	bytes.Repeat([]byte{0x8c, 0x31, 0xc6, 0x18, 0x63}, 1365),
	[]byte{0x8c, 0x31},
	bytes.Repeat([]byte{0x63, 0x8c, 0x31, 0xc6, 0x18}, 1365),
	[]byte{0x63, 0x2c},
	bytes.Repeat([]byte{0x31, 0xc6, 0x18, 0x63, 0x8c}, 1365),
	[]byte{0x31, 0x46},
	bytes.Repeat([]byte{0x31, 0xc6, 0x18, 0x63, 0x8c}, 1365),
	[]byte{0x31, 0x06, 0x00},
)

// A crafted zstd request body is rejected with an error. Before, the
// decoder panicked on a goroutine of its own and the process exited; such a
// panic would kill this test binary too, so a pass means it no longer can.
func TestDecompressZstdCraftedFrameIsAnError(t *testing.T) {
	// Whether the bug panics depends on what the decoder's scratch tables
	// held before, so decode a valid payload first, then the frame repeatedly.
	if _, err := decompressZstdPooled(zstdEncodeForTest(t, []byte(strings.Repeat("warm ", 5000))), 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := decompressZstdPooled(ghsaFpmhFrame, 0); err == nil {
			t.Fatal("the crafted frame decoded without an error")
		}
		if _, codec, err := decompressRequest(ghsaFpmhFrame, 0); err == nil || codec != "zstd" {
			t.Fatalf("decompressRequest: codec %q err %v", codec, err)
		}
	}
	// The pool still decodes valid payloads afterwards.
	want := strings.Repeat("cpu,host=a v=1 1\n", 1000)
	out, err := decompressZstdPooled(zstdEncodeForTest(t, []byte(want)), 0)
	if err != nil || string(out) != want {
		t.Fatalf("valid payload after the crafted one: %v", err)
	}
}

func zstdEncodeForTest(t *testing.T, b []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return enc.EncodeAll(b, nil)
}

// Control: the frame does make this module's zstd decoder panic, so
// TestDecompressZstdCraftedFrameIsAnError exercises the recover rather than
// passing because the frame is rejected earlier. This fails, by design, once
// klauspost/compress is upgraded past 1.20.1 (which fixed the bug): the
// recover then needs an injected panic to stay tested.
func TestZstdCraftedFramePanicsTheDecoder(t *testing.T) {
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		_, _ = d.DecodeAll(ghsaFpmhFrame, nil)
		return false
	}()
	if !panicked {
		t.Fatal("the crafted frame no longer panics this decoder: replace the frame with an injected panic")
	}
}

// A frame may not make the decoder allocate a window larger than
// zstdMaxWindow: a 10-byte frame declaring a 512 MB window allocated it
// (and the pooled decoder kept it), at the msgpack 1 GB cap.
func TestDecompressZstdRejectsAHugeWindow(t *testing.T) {
	const exp = 19 // window 2^(10+19) = 512 MB
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, exp << 3, 0x09, 0x00, 0x00, 'x'}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := decompressZstdPooled(frame, 1<<30)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, errZstdWindow) {
		t.Fatalf("a frame declaring a 512 MB window: %v, want the window error", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 64<<20 {
		t.Fatalf("the frame made the decoder allocate %d MB", grew>>20)
	}
}

// A pooled decoder keeps the memory cap it was created with: one made for a
// smaller cap must not be handed to a caller with a larger one, whose valid
// payload it would reject. The msgpack and line-protocol handlers pass
// different caps.
func TestDecompressZstdPooled_CallersWithDifferentCaps(t *testing.T) {
	small := zstdEncodeForTest(t, bytes.Repeat([]byte("a"), 1000))
	big := zstdEncodeForTest(t, bytes.Repeat([]byte("abcdefgh"), (2<<20)/8)) // 2 MiB
	for i := 0; i < 5; i++ {
		if _, err := decompressZstdPooled(small, 1<<20); err != nil {
			t.Fatal(err)
		}
		out, err := decompressZstdPooled(big, 8<<20)
		if err != nil || len(out) != 2<<20 {
			t.Fatalf("round %d: a 2 MiB payload under an 8 MiB cap, after a 1 MiB-cap caller: %d %v", i, len(out), err)
		}
		if _, err := decompressZstdPooled(big, 1<<20); err == nil {
			t.Fatalf("round %d: a 2 MiB payload passed a 1 MiB cap", i)
		}
	}
}

// DecodeAll stops at the size limit on the bombs that have no declared size
// to check up front: a stream of frames without a content size, and many
// concatenated frames each under the limit.
func TestDecompressZstdBombsStopAtTheLimit(t *testing.T) {
	const limit = 10 << 20
	var stream bytes.Buffer
	w, err := zstd.NewWriter(&stream)
	if err != nil {
		t.Fatal(err)
	}
	zero := make([]byte, 1<<20)
	for i := 0; i < 256; i++ {
		_, _ = w.Write(zero)
	}
	_ = w.Close()
	one := zstdEncodeForTest(t, make([]byte, 4<<20))
	var many []byte
	for i := 0; i < 64; i++ {
		many = append(many, one...)
	}
	for name, bomb := range map[string][]byte{"no declared size": stream.Bytes(), "64 frames": many} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := decompressZstdPooled(bomb, limit)
		runtime.ReadMemStats(&after)
		if err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("%s: %v, want the size limit error", name, err)
		}
		if grew := after.TotalAlloc - before.TotalAlloc; grew > 8*limit {
			t.Fatalf("%s: allocated %d MB for a %d MB limit", name, grew>>20, limit>>20)
		}
	}
}
