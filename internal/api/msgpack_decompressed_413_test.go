package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestMsgPackDecompressedPayloadLimitReturns413(t *testing.T) {
	const maxPayloadSize = 2 << 10
	rig := newLPWriteRig(t, nil)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	NewMsgPackHandler(zerolog.Nop(), rig.buf, maxPayloadSize).RegisterRoutes(app)

	plain := bytes.Repeat([]byte("x"), 4<<10)
	compressors := map[string]func([]byte) ([]byte, error){
		"gzip": func(src []byte) ([]byte, error) {
			var compressed bytes.Buffer
			writer := gzip.NewWriter(&compressed)
			if _, err := writer.Write(src); err != nil {
				return nil, err
			}
			if err := writer.Close(); err != nil {
				return nil, err
			}
			return compressed.Bytes(), nil
		},
		"zstd": func(src []byte) ([]byte, error) {
			writer, err := zstd.NewWriter(nil, zstd.WithWindowSize(1<<10))
			if err != nil {
				return nil, err
			}
			defer writer.Close()
			return writer.EncodeAll(src, nil), nil
		},
	}

	for name, compress := range compressors {
		t.Run(name, func(t *testing.T) {
			body, err := compress(plain)
			require.NoError(t, err)
			require.Less(t, len(body), maxPayloadSize, "compressed request must fit the raw-body limit")

			req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/msgpack")
			resp, err := app.Test(req, -1)
			require.NoError(t, err)
			defer resp.Body.Close()
			responseBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode, string(responseBody))
		})
	}
}

func TestMsgPackInvalidCompressionRemains400(t *testing.T) {
	rig := newLPWriteRig(t, nil)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	NewMsgPackHandler(zerolog.Nop(), rig.buf, 2<<10).RegisterRoutes(app)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader([]byte{0x1f, 0x8b, 0x08}))
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(responseBody))
}
