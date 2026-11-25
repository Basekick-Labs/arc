package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Under a server.max_payload_size below zstdMaxWindow, the decoder refuses a
// frame whose window is over the limit before decoding it. zstd -3 (and
// Node's zstdCompressSync, which node-red-arc uses) gives a large batch a
// 2 MB window, so at a 1 MB limit that batch must answer 413, which a client
// splits on, not the 400 for a frame compressed with --ultra or --long.
func TestMsgPackZstdWindowOverALowLimitReturns413(t *testing.T) {
	const limit = 1 << 20
	rig := newLPWriteRig(t, nil)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	NewMsgPackHandler(zerolog.Nop(), rig.buf, limit).RegisterRoutes(app)

	enc, err := zstd.NewWriter(nil, zstd.WithWindowSize(2<<20))
	require.NoError(t, err)
	defer enc.Close()
	body := enc.EncodeAll(bytes.Repeat([]byte("x"), 3<<20), nil)
	require.Less(t, len(body), limit)
	require.Equal(t, uint64(2<<20), zstdFirstFrameWindow(body), "the frame must declare the 2 MB window")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader(body))
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode, string(out))
	require.Contains(t, string(out), "window exceeds the 1MB limit")
}

// A window over zstdMaxWindow stays the 400 that names --ultra/--long, under
// a low limit too: splitting would not shrink it.
func TestDecompressZstdHugeWindowUnderALowLimitStays400(t *testing.T) {
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 19 << 3, 0x09, 0x00, 0x00, 'x'} // 512 MB window
	_, err := decompressZstdPooled(frame, 1<<20)
	require.True(t, errors.Is(err, errZstdWindow), "got %v", err)
}
