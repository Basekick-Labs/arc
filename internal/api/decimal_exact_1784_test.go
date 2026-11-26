//go:build duckdb_arrow

package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Follow-ups to #1922 (x-arc-decimals: exact, #1784): the HTTP routes.

// Exact returns the digits: the issue's sum, a negative value, a NULL and a
// value past the first batch, where default mode cuts the stream.
func TestExactDecimalsReturnTheDigits1784(t *testing.T) {
	r := newOverflowRig(t)
	code, msg, typ, vals := r.arrowWithDecimals(t, overflowRepro, []string{decimalModeExact})
	require.Equal(t, http.StatusOK, code, msg)
	require.Equal(t, "decimal(38, 0)", typ.String())
	require.Equal(t, []string{"10770278400000000000"}, vals)

	const sql = "SELECT CASE WHEN i = 5000 THEN -170141183460469231731687303715884105727::HUGEINT " +
		"WHEN i = 5001 THEN NULL WHEN i = 5002 THEN 100000000000000000000::HUGEINT ELSE i::HUGEINT END AS v FROM range(6000) t(i)"
	code, msg, _, vals = r.arrowWithDecimals(t, sql, []string{decimalModeExact})
	require.Equal(t, http.StatusOK, code, msg)
	require.Len(t, vals, 6000)
	require.Equal(t, "4999", vals[4999])
	require.Equal(t, "-170141183460469231731687303715884105727", vals[5000])
	require.Equal(t, "NULL", vals[5001])
	require.Equal(t, "100000000000000000000", vals[5002])
	require.Equal(t, "5999", vals[5999])
}

// Canary for a DuckDB quirk the release note documents: DuckDB exports its
// unsigned UHUGEINT as decimal(38,0) by reinterpreting the bits, and Arc
// cannot tell it from a HUGEINT once exported, so a value at or above 2^127
// arrives as value - 2^128. If DuckDB changes that, this fails: update the
// release note's caveat with it. A HUGEINT with 39 digits arrives right, in a
// decimal(38,0) field.
func TestDecimalsUHUGEINTCanary1784(t *testing.T) {
	r := newOverflowRig(t)
	for _, c := range []struct {
		expr, exact, deflt string // deflt "400": #1803's refusal
	}{
		{"340282366920938463463374607431768211455::UHUGEINT", "-1", "-1"},
		{"170141183460469231731687303715884105728::UHUGEINT", "-170141183460469231731687303715884105728", "400"},
		{"170141183460469231731687303715884105727::HUGEINT", "170141183460469231731687303715884105727", "400"},
	} {
		sql := "SELECT " + c.expr + " AS v"
		code, msg, typ, vals := r.arrowWithDecimals(t, sql, []string{decimalModeExact})
		require.Equal(t, http.StatusOK, code, "%s: %s", c.expr, msg)
		require.Equal(t, "decimal(38, 0)", typ.String(), c.expr)
		require.Equal(t, []string{c.exact}, vals, "exact %s", c.expr)

		code, msg, _, vals = r.arrowWithDecimals(t, sql, nil)
		if c.deflt == "400" {
			require.Equal(t, http.StatusBadRequest, code, c.expr)
			require.True(t, strings.HasPrefix(msg, decimalOverflowPrefix), msg)
			continue
		}
		require.Equal(t, http.StatusOK, code, "%s: %s", c.expr, msg)
		require.Equal(t, []string{c.deflt}, vals, "default %s", c.expr)
	}
}

// The governance row cap applies under exact as it does by default, with no
// overflow peek: a wide value inside the cap is returned, rows past it are
// not sent.
func TestExactDecimalsUnderTheGovernanceCap1784(t *testing.T) {
	const capRows = 3000 // inside the second 2,048-row batch
	_, app, _ := newArcxArrowHandlerRig(t, &governance.Policy{TokenID: 42, MaxRowsPerQuery: capRows}, 0)
	body, _ := json.Marshal(map[string]string{"sql": hugeAt(5000, 2500)})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/query/arrow", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(decimalsHeader, decimalModeExact)
	resp, err := app.Test(req, 30000)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	reader, err := ipc.NewReader(resp.Body)
	require.NoError(t, err)
	defer reader.Release()
	require.Equal(t, "decimal(38, 0)", reader.Schema().Field(0).Type.String())
	vals := columnStrings(t, reader)
	require.Len(t, vals, capRows)
	require.Equal(t, "100000000000000000000", vals[2500])
	require.Equal(t, "2999", vals[capRows-1])
}

// A browser preflight names x-arc-decimals and the Arrow route's encoding
// headers; Arc must allow them.
func TestDecimalsHeaderCORS1784(t *testing.T) {
	app := NewServer(DefaultServerConfig(), zerolog.Nop()).GetApp()
	app.Post("/q", func(c *fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	for _, header := range []string{decimalsHeader, "x-arc-arrow-dictionary", "x-arc-arrow-compression"} {
		req := httptest.NewRequest(http.MethodOptions, "/q", nil)
		req.Header.Set("Origin", "https://app.example")
		req.Header.Set("Access-Control-Request-Method", "POST")
		req.Header.Set("Access-Control-Request-Headers", header)
		resp, err := app.Test(req, -1)
		require.NoError(t, err)
		allowed := strings.Split(strings.ToLower(resp.Header.Get("Access-Control-Allow-Headers")), ",")
		require.Contains(t, allowed, header)
	}
}

// /api/v1/query/estimate checks the header like the query routes, before the
// forward, and accepts exact and default as a no-op: an estimate returns no
// column values.
func TestEstimateAndTheDecimalsHeader1784(t *testing.T) {
	const body = `{"sql":"SELECT * FROM sample.cpu"}`
	r := newFreshRig(t)
	for _, c := range []struct {
		values []string
		status int
	}{
		{nil, http.StatusOK},
		{[]string{decimalModeExact}, http.StatusOK},
		{[]string{decimalModeDefault}, http.StatusOK},
		{[]string{"bogus"}, http.StatusBadRequest},
		{[]string{decimalModeExact, decimalModeExact}, http.StatusBadRequest},
	} {
		a := r.sendDecimals(t, "POST", "/api/v1/query/estimate", body, c.values)
		require.Equal(t, c.status, a.status, "%v: %s", c.values, a.body)
		if c.status == http.StatusBadRequest {
			var doc EstimateResponse
			require.NoError(t, json.Unmarshal([]byte(a.body), &doc))
			require.False(t, doc.Success)
			require.Equal(t, "error", doc.WarningLevel)
			require.Contains(t, doc.Error, "x-arc-decimals")
		}
	}
}

// On a node that forwards its reads (a compactor), a value the route refuses
// is refused before the forward: the peer never sees it. exact on estimate is
// forwarded, as an estimate accepts it.
func TestDecimalsHeaderRefusedBeforeTheForward1784(t *testing.T) {
	for _, c := range []struct {
		method, path, body string
		values             []string
		forwarded          bool
	}{
		{"GET", "/api/v1/query/cpu?database=sample", "", []string{decimalModeExact}, false},
		{"GET", "/api/v1/query/cpu?database=sample", "", []string{"bogus"}, false},
		{"POST", "/api/v1/query/estimate", `{"sql":"SELECT * FROM sample.cpu"}`, []string{"bogus"}, false},
		{"POST", "/api/v1/query/estimate", `{"sql":"SELECT * FROM sample.cpu"}`, []string{decimalModeExact}, true},
		{"POST", "/api/v1/query", `{"sql":"SELECT * FROM sample.cpu"}`, []string{decimalModeExact}, false},
		{"POST", "/api/v1/query/arrow", `{"sql":"SELECT * FROM sample.cpu"}`, []string{decimalModeExact, decimalModeDefault}, false},
	} {
		t.Run(c.method+" "+c.path+" "+strings.Join(c.values, "+"), func(t *testing.T) {
			r := newFreshRig(t)
			peer := &readPeer{}
			srv := httptest.NewServer(peer)
			defer srv.Close()
			r.qh.SetRouter(readRouter(t, srv.URL, true, true))

			a := r.sendDecimals(t, c.method, c.path, c.body, c.values)
			if c.forwarded {
				require.Equal(t, int32(1), peer.hits.Load(), "not forwarded (%d %s)", a.status, a.body)
				return
			}
			require.Equal(t, http.StatusBadRequest, a.status, a.body)
			require.Contains(t, a.body, "x-arc-decimals")
			require.Zero(t, peer.hits.Load(), "a refused x-arc-decimals was forwarded")

			// Without the header the rig does reach the forward.
			r.sendDecimals(t, c.method, c.path, c.body, nil)
			require.Equal(t, int32(1), peer.hits.Load(), "control: not forwarded")
		})
	}
}

// sendDecimals sends one x-arc-decimals line per value with the rig's read
// token.
func (r *freshRig) sendDecimals(t *testing.T, method, path, body string, values []string) freshAnswer {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+r.read)
	if values != nil {
		req.Header[http.CanonicalHeaderKey(decimalsHeader)] = values
	}
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return freshAnswer{status: resp.StatusCode, body: string(b)}
}
