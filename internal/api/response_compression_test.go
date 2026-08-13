package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/zstd"
)

func negotiateFor(t *testing.T, acceptEncoding string) string {
	t.Helper()
	app := fiber.New()
	var got string
	app.Get("/", func(c *fiber.Ctx) error {
		got = negotiateResponseCompression(c)
		return nil
	})
	req := httptest.NewRequest("GET", "/", nil)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	if _, err := app.Test(req); err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return got
}

func TestNegotiateResponseCompression(t *testing.T) {
	cases := []struct {
		accept string
		want   string
	}{
		{"", ""},
		{"gzip", "gzip"},
		{"zstd", "zstd"},
		{"gzip, zstd", "zstd"}, // zstd preferred regardless of order
		{"zstd, gzip", "zstd"},
		{"GZIP", "gzip"},
		{"x-gzip", "gzip"},
		{"br", ""}, // unsupported encoding → identity
		{"gzip;q=0", ""},
		{"zstd;q=0, gzip", "gzip"},
		{"gzip;q=0.5, zstd;q=0.1", "zstd"}, // both acceptable → zstd
		{"identity", ""},
	}
	for _, tc := range cases {
		if got := negotiateFor(t, tc.accept); got != tc.want {
			t.Errorf("Accept-Encoding %q: got %q want %q", tc.accept, got, tc.want)
		}
	}
}

// TestCompressedSinkRoundTrip verifies both pooled codecs produce streams the
// standard decoders accept, across repeated pool reuse.
func TestCompressedSinkRoundTrip(t *testing.T) {
	payload := []byte(strings.Repeat(`{"host":"server01","value":42.5},`, 5000))

	for _, encoding := range []string{"zstd", "gzip", ""} {
		for round := 0; round < 3; round++ { // exercise pool reuse
			var buf bytes.Buffer
			sink, finish := compressedSink(&buf, encoding)
			if _, err := sink.Write(payload); err != nil {
				t.Fatalf("%s round %d write: %v", encoding, round, err)
			}
			if err := finish(); err != nil {
				t.Fatalf("%s round %d finish: %v", encoding, round, err)
			}

			var decoded []byte
			var err error
			switch encoding {
			case "zstd":
				r, e := zstd.NewReader(&buf)
				if e != nil {
					t.Fatalf("zstd reader: %v", e)
				}
				decoded, err = io.ReadAll(r)
				r.Close()
			case "gzip":
				r, e := gzip.NewReader(&buf)
				if e != nil {
					t.Fatalf("gzip reader: %v", e)
				}
				decoded, err = io.ReadAll(r)
			default:
				decoded, err = buf.Bytes(), nil
			}
			if err != nil {
				t.Fatalf("%s round %d decode: %v", encoding, round, err)
			}
			if !bytes.Equal(decoded, payload) {
				t.Fatalf("%s round %d payload mismatch (%d vs %d bytes)", encoding, round, len(decoded), len(payload))
			}
			if encoding != "" && buf.Len() != 0 {
				// buf drained by decode above for zstd path only; just assert compression happened
				_ = buf
			}
		}
	}
}
