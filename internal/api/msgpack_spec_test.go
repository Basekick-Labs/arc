package api

// GET /api/v1/write/msgpack/spec documents the write endpoint. It said
// authentication was "not yet implemented" long after the route itself
// required a token (#1434), so these tests hold the document to the code.

import (
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// The examples must be payloads the handler accepts, stored the way the
// document says: tag_keys columns as tags, the other columns as fields.
func TestMsgPackSpecExamplesAreAccepted(t *testing.T) {
	doc := msgPackSpecDocument()
	wantRows := map[string]int64{"example_columnar": 3, "example_row": 1}
	for _, name := range []string{"example_columnar", "example_row"} {
		t.Run(name, func(t *testing.T) {
			r, post := newMsgPackRowsRig(t)
			status, out := post(doc[name])
			require.Equal(t, http.StatusNoContent, status, out)
			rows, types := r.stored(t)
			require.Equal(t, map[string]int64{"cpu": wantRows[name]}, rows)
			require.Equal(t, "float64", types["cpu"]["usage_idle"])
			tags := storedTags(t, r.dir, "cpu")
			switch name {
			case "example_columnar":
				require.Equal(t, "host,region", tags)
			case "example_row":
				require.Equal(t, "datacenter,host,region", tags)
			}
		})
	}
}

// storedTags returns the arc:tags metadata of the measurement's files.
func storedTags(t *testing.T, dir, measurement string) string {
	t.Helper()
	var tags string
	require.NoError(t, filepath.WalkDir(filepath.Join(dir, "d", measurement), func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()
		pf, err := file.NewParquetReader(f)
		require.NoError(t, err)
		defer pf.Close()
		if v := pf.MetaData().KeyValueMetadata().FindValue("arc:tags"); v != nil {
			tags = *v
		}
		return nil
	}))
	return tags
}

func TestMsgPackSpecDescribesAuthentication(t *testing.T) {
	r := newLPWriteRig(t, nil)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	NewMsgPackHandler(zerolog.Nop(), r.buf, 100<<20).RegisterRoutes(app)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/write/msgpack/spec", nil), -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotContains(t, string(body), "not yet implemented")

	var spec struct {
		Authentication struct {
			Header string `json:"header"`
			Scheme string `json:"scheme"`
		} `json:"authentication"`
		Database struct {
			Header string `json:"header"`
		} `json:"database"`
	}
	require.NoError(t, json.Unmarshal(body, &spec))
	require.Equal(t, "Authorization", spec.Authentication.Header)
	require.Equal(t, "Bearer <token>", spec.Authentication.Scheme)
	require.Equal(t, "x-arc-database", spec.Database.Header)
}
