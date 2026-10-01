//go:build duckdb_arrow

package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/fieldschema"
)

// The msgpack wire path streams Arrow batches; an anchor-only column is an
// all-NULL typed column it has never had to encode before #914.
func TestFieldSchema_MsgPackTypedNullColumnIssue914(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	seedCustomerScenario(e)
	body, _ := json.Marshal(QueryRequest{SQL: rangeSQL("source, weeks_later", jan1, jan1.Add(24*time.Hour), "")})
	req := httptest.NewRequest("POST", "/api/v1/query/msgpack", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.app.Test(req, 30000)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("msgpack status %d: %s", resp.StatusCode, raw)
	}
	out := decodeMsgpack(t, raw)
	cols := colsOf(t, out)
	if len(cols) != 2 || len(cols[1]) != 1 || cols[1][0] != nil {
		t.Fatalf("msgpack typed NULL column: %+v", out)
	}
	_ = time.Second
}
