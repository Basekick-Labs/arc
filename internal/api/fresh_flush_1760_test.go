//go:build duckdb_arrow

package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1760: this build recognises x-arc-freshness and refuses it on every query
// route, after auth and before anything is forwarded, parsed or run.

// freshRig: the real auth middleware (global, as main wires it) and the query
// handler's own RegisterRoutes over a stored sample.cpu, with rows of
// sample.fresh left in the buffer.
type freshRig struct {
	app         *fiber.App
	qh          *QueryHandler
	dataDir     string
	read, write string
}

func newFreshRig(t *testing.T) *freshRig {
	t.Helper()
	ctx := context.Background()
	logger := zerolog.Nop()
	metrics.Init(logger)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	t.Cleanup(func() { buffer.Close() })
	row := func(meas string) {
		require.NoError(t, buffer.WriteTypedColumnarDirect(ctx, "sample", meas, &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{time.Now().UTC().UnixMicro()}, "value": []int64{1}},
		}, 1))
	}
	row("cpu")
	require.NoError(t, buffer.FlushAllAndWait(ctx))
	row("fresh") // stays buffered: no request below may flush it

	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	am, err := auth.NewAuthManager(filepath.Join(root, "auth.db"), time.Minute, 100, logger)
	require.NoError(t, err)
	t.Cleanup(func() { am.Close() })
	read, err := am.CreateToken(ctx, "fresh-read", "", "read", nil)
	require.NoError(t, err)
	writ, err := am.CreateToken(ctx, "fresh-write", "", "write", nil)
	require.NoError(t, err)

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	mw := auth.DefaultMiddlewareConfig()
	mw.AuthManager = am
	app.Use(auth.NewMiddleware(mw))
	qh := NewQueryHandler(duck, backend, logger, 0, 0)
	qh.SetAuthAndRBAC(am, nil)
	qh.RegisterRoutes(app)
	return &freshRig{app: app, qh: qh, dataDir: dataDir, read: read, write: writ}
}

// freshRoutes are every query route; a compactor forwards each of them.
var freshRoutes = []struct{ method, path, body string }{
	{"POST", "/api/v1/query", `{"sql":"SELECT * FROM sample.cpu"}`},
	{"POST", "/api/v1/query/msgpack", `{"sql":"SELECT * FROM sample.cpu"}`},
	{"POST", "/api/v1/query/arrow", `{"sql":"SELECT * FROM sample.cpu"}`},
	{"POST", "/api/v1/query/estimate", `{"sql":"SELECT * FROM sample.cpu"}`},
	{"GET", "/api/v1/query/cpu?database=sample", ""},
}

type freshAnswer struct {
	status         int
	fresh, reason  string
	body           string
	contentPresent bool
}

func (r *freshRig) send(t *testing.T, method, path, body, token, header string, withHeader bool) freshAnswer {
	t.Helper()
	var values []string
	if withHeader {
		values = []string{header}
	}
	return r.sendValues(t, method, path, body, token, values)
}

// sendValues sends one x-arc-freshness header line per value (none for nil).
func (r *freshRig) sendValues(t *testing.T, method, path, body, token string, values []string) freshAnswer {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if values != nil {
		req.Header[http.CanonicalHeaderKey(FreshnessHeader)] = values
	}
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	_, present := resp.Header[http.CanonicalHeaderKey(FreshnessAnswerHeader)]
	return freshAnswer{
		status: resp.StatusCode, fresh: resp.Header.Get(FreshnessAnswerHeader),
		reason: resp.Header.Get(FreshnessReasonHeader), body: string(b), contentPresent: present,
	}
}

// storedFiles counts the Parquet files under sample/<meas>.
func (r *freshRig) storedFiles(t *testing.T, meas string) int {
	t.Helper()
	n := 0
	_ = filepath.Walk(filepath.Join(r.dataDir, "sample", meas), func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".parquet") {
			n++
		}
		return nil
	})
	return n
}

func TestFreshness_RefusedOnEveryQueryRoute(t *testing.T) {
	r := newFreshRig(t)
	for _, rt := range freshRoutes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			// Without the header: the route answers as before, with no
			// freshness answer header.
			a := r.send(t, rt.method, rt.path, rt.body, r.read, "", false)
			require.Equal(t, http.StatusOK, a.status, a.body)
			require.False(t, a.contentPresent, "X-Arc-Freshness on a request without the header")
			// An empty value counts as absent.
			a = r.send(t, rt.method, rt.path, rt.body, r.read, "", true)
			require.Equal(t, http.StatusOK, a.status, a.body)
			require.False(t, a.contentPresent, "X-Arc-Freshness on a request with an empty header")

			// "flush", in any case and spacing: 403 disabled.
			for _, v := range []string{"flush", " FLUSH ", "Flush"} {
				a = r.send(t, rt.method, rt.path, rt.body, r.read, v, true)
				require.Equal(t, http.StatusForbidden, a.status, a.body)
				require.Equal(t, "false", a.fresh)
				require.Equal(t, "disabled", a.reason)
				if rt.path != "/api/v1/query/msgpack" {
					require.Contains(t, a.body, "x-arc-freshness is not available")
				}
			}

			// A repeated header: every value counts, empty ones as absent.
			for _, tc := range []struct {
				values []string
				status int
				reason string
			}{
				{[]string{"", "flush"}, http.StatusForbidden, "disabled"},
				{[]string{"flush", ""}, http.StatusForbidden, "disabled"},
				{[]string{"x", "flush"}, http.StatusBadRequest, "invalid_value"},
				{[]string{"flush", "x"}, http.StatusBadRequest, "invalid_value"},
				{[]string{"", " "}, http.StatusOK, ""},
			} {
				a = r.sendValues(t, rt.method, rt.path, rt.body, r.read, tc.values)
				require.Equal(t, tc.status, a.status, "%q: %s", tc.values, a.body)
				require.Equal(t, tc.reason, a.reason, "%q", tc.values)
			}

			// Any other value: 400 invalid_value.
			a = r.send(t, rt.method, rt.path, rt.body, r.read, "yes", true)
			require.Equal(t, http.StatusBadRequest, a.status, a.body)
			require.Equal(t, "false", a.fresh)
			require.Equal(t, "invalid_value", a.reason)

			// Auth comes first: no token is 401, a token without read is the
			// door's refusal; neither says anything about the feature.
			a = r.send(t, rt.method, rt.path, rt.body, "", "flush", true)
			require.Equal(t, http.StatusUnauthorized, a.status, a.body)
			require.False(t, a.contentPresent, "an unauthenticated request learned about x-arc-freshness")
			a = r.send(t, rt.method, rt.path, rt.body, r.write, "flush", true)
			require.Equal(t, http.StatusForbidden, a.status, a.body)
			require.False(t, a.contentPresent, "a token without read learned about x-arc-freshness")
			require.Contains(t, a.body, "read required")
		})
	}
	// Nothing was flushed by any of it.
	require.Zero(t, r.storedFiles(t, "fresh"), "a refused fresh query flushed the buffer")
	require.Equal(t, 1, r.storedFiles(t, "cpu"))
}

// A refused request is not a query: it is not counted in the query
// counters, on every route that counts.
func TestFreshness_RefusedRequestsAreNotCounted(t *testing.T) {
	r := newFreshRig(t)
	for _, rt := range freshRoutes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			requests := queryMetricInt(t, "query_requests_total")
			errs := queryMetricInt(t, "query_errors_total")
			for _, v := range []string{"flush", "yes"} {
				a := r.send(t, rt.method, rt.path, rt.body, r.read, v, true)
				require.Equal(t, "false", a.fresh, a.body)
			}
			require.Equal(t, requests, queryMetricInt(t, "query_requests_total"), "a refused request was counted as a query")
			require.Equal(t, errs, queryMetricInt(t, "query_errors_total"), "a refused request was counted as a query error")
		})
	}
}

// The refusal comes before governance: refused requests use none of a
// token's rate limit.
func TestFreshness_RefusedBeforeGovernance(t *testing.T) {
	m := newGovernanceTestManager(t, &governance.Policy{TokenID: 42, RateLimitPerMinute: 1})
	h := newGovernanceTestHandler(m, 0)
	app := newGovernanceTestApp(t, h, &auth.TokenInfo{ID: 42, Name: "limited"}, nil, nil)
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil)
		req.Header.Set(FreshnessHeader, "flush")
		resp, err := app.Test(req, -1)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusForbidden, resp.StatusCode, "refused request %d", i)
	}
	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/query/cpu?database=default", nil), -1)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "the refused requests used the token's rate limit")
}

// Browsers may send x-arc-freshness and read the two answer headers (CORS),
// as for x-arc-wait (#1759).
func TestFreshness_CORS(t *testing.T) {
	app := NewServer(DefaultServerConfig(), zerolog.Nop()).GetApp()
	app.Post("/q", func(c *fiber.Ctx) error { return c.SendStatus(http.StatusOK) })
	req := httptest.NewRequest(http.MethodOptions, "/q", nil)
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", FreshnessHeader)
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	require.Contains(t, strings.ToLower(resp.Header.Get("Access-Control-Allow-Headers")), FreshnessHeader)
	req = httptest.NewRequest(http.MethodPost, "/q", nil)
	req.Header.Set("Origin", "https://app.example")
	resp, err = app.Test(req, -1)
	require.NoError(t, err)
	exposed := resp.Header.Get("Access-Control-Expose-Headers")
	require.Contains(t, exposed, FreshnessAnswerHeader)
	require.Contains(t, exposed, FreshnessReasonHeader)
	require.Contains(t, exposed, FreshnessWaitHeader)
}

// On a node that forwards its reads (a compactor), the refusal comes before
// the forward: the peer never sees a fresh query.
func TestFreshness_RefusedBeforeTheForward(t *testing.T) {
	for _, rt := range freshRoutes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			r := newFreshRig(t)
			peer := &readPeer{}
			srv := httptest.NewServer(peer)
			defer srv.Close()
			r.qh.SetRouter(readRouter(t, srv.URL, true, true))

			a := r.send(t, rt.method, rt.path, rt.body, r.read, "flush", true)
			require.Equal(t, http.StatusForbidden, a.status, a.body)
			require.Equal(t, "disabled", a.reason)
			require.Zero(t, peer.hits.Load(), "a fresh query was forwarded")

			// The same request without the header is forwarded: the rig does
			// reach the forward.
			a = r.send(t, rt.method, rt.path, rt.body, r.read, "", false)
			require.Equal(t, int32(1), peer.hits.Load(), "control: not forwarded (%d %s)", a.status, a.body)
		})
	}
}
