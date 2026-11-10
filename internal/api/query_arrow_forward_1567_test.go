//go:build duckdb_arrow

package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1567: a node that cannot serve queries (a compactor) forwards
// POST /api/v1/query/arrow, as it does /api/v1/query, streaming the answer
// so the Arrow trailers reach the client; with no peer that accepts a
// forwarded Arrow query it runs the query locally, as before.

// arrowUpstream is the peer: it answers like the Arrow endpoint, chunked, with
// the three trailers, or fails partway through the body.
type arrowUpstream struct {
	hits    atomic.Int32
	status  int
	failMid bool
	body    []byte
}

func (u *arrowUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.hits.Add(1)
	if r.URL.Path != "/api/v1/query/arrow" {
		http.Error(w, "unexpected path "+r.URL.Path, http.StatusTeapot)
		return
	}
	if u.status != 0 && u.status != http.StatusOK {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(u.status)
		_, _ = w.Write([]byte(`{"success":false,"error":"bad sql"}`))
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Trailer", strings.Join(arrowTrailerNames, ", "))
	w.WriteHeader(http.StatusOK)
	half := len(u.body) / 2
	_, _ = w.Write(u.body[:half])
	w.(http.Flusher).Flush()
	if u.failMid {
		// Cut the connection mid-body, mid-message.
		panic(http.ErrAbortHandler)
	}
	_, _ = w.Write(u.body[half:])
	w.Header().Set(arrowStreamTruncatedTrailer, "row cap reached")
	w.Header().Set(arrowRowsCappedTrailer, "1000")
	w.Header().Set(arrowExecutionTimeTrailer, "42")
}

// arrowForwardRouter routes from a compactor to one writer at peerURL;
// capable says whether that writer advertises the forwarded-Arrow capability.
func arrowForwardRouter(t *testing.T, peerURL string, role cluster.NodeRole, capable bool) *cluster.Router {
	t.Helper()
	local := cluster.NewNode("local", "Local", role, "test-cluster")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	writer := cluster.NewNode("writer", "Writer", cluster.RoleWriter, "test-cluster")
	writer.UpdateState(cluster.StateHealthy)
	writer.APIAddress = strings.TrimPrefix(peerURL, "http://")
	require.NoError(t, registry.Register(writer))
	return cluster.NewRouter(&cluster.RouterConfig{
		// The whole-response route timeout must not cut a streamed answer.
		Timeout:        50 * time.Millisecond,
		Registry:       registry,
		LocalNode:      local,
		Logger:         zerolog.Nop(),
		PeerArrowQuery: func(string) bool { return capable },
	})
}

func (r *doorRig) arrowQuery(t *testing.T) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT 1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.tokens["coarse-no-team"])
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	return resp
}

func TestForwardedArrowQueryCarriesItsTrailers(t *testing.T) {
	r := newDoorRig(t)
	body := bytes.Repeat([]byte("arrow-ipc-bytes-"), 64<<10) // 1 MiB, several chunks
	up := &arrowUpstream{body: body}
	// The upstream sleeps past the router's route timeout before ending, to
	// prove the streaming client has no whole-response timeout.
	slow := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		up.ServeHTTP(&slowWriter{ResponseWriter: w, delay: 120 * time.Millisecond}, req)
	})
	peer := httptest.NewServer(slow)
	defer peer.Close()
	r.qh.SetRouter(arrowForwardRouter(t, peer.URL, cluster.RoleCompactor, true))

	resp := r.arrowQuery(t)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, bytes.Equal(body, got), "relayed body differs (%d vs %d bytes)", len(got), len(body))
	require.Equal(t, "row cap reached", resp.Trailer.Get(arrowStreamTruncatedTrailer))
	require.Equal(t, "1000", resp.Trailer.Get(arrowRowsCappedTrailer))
	require.Equal(t, "42", resp.Trailer.Get(arrowExecutionTimeTrailer))
	require.Equal(t, int32(1), up.hits.Load())
}

// slowWriter delays the second write, so the response outlives a short
// whole-response timeout.
type slowWriter struct {
	http.ResponseWriter
	delay  time.Duration
	writes int
}

func (s *slowWriter) Write(p []byte) (int, error) {
	s.writes++
	if s.writes == 2 {
		time.Sleep(s.delay)
	}
	return s.ResponseWriter.Write(p)
}

func (s *slowWriter) Flush() { s.ResponseWriter.(http.Flusher).Flush() }

// An upstream that fails partway ends the forwarded response as a failed
// transfer: the client cannot read it to a clean end.
func TestForwardedArrowQueryUpstreamFailureIsABrokenTransfer(t *testing.T) {
	r := newDoorRig(t)
	up := &arrowUpstream{body: bytes.Repeat([]byte("x"), 256<<10), failMid: true}
	peer := httptest.NewServer(up)
	defer peer.Close()
	r.qh.SetRouter(arrowForwardRouter(t, peer.URL, cluster.RoleCompactor, true))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT 1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.tokens["coarse-no-team"])
	resp, err := r.app.Test(req, -1)
	if err == nil {
		// app.Test reads the whole response; if it got one, its body must
		// still fail to read to a clean end.
		_, err = io.ReadAll(resp.Body)
	}
	require.Error(t, err, "a cut upstream must not read as a complete response")
	require.Contains(t, err.Error(), "EOF")
}

// An error answer is relayed as is (buffered, like every other forward).
func TestForwardedArrowQueryRelaysAnErrorAnswer(t *testing.T) {
	r := newDoorRig(t)
	peer := httptest.NewServer(&arrowUpstream{status: http.StatusBadRequest})
	defer peer.Close()
	r.qh.SetRouter(arrowForwardRouter(t, peer.URL, cluster.RoleCompactor, true))

	resp := r.arrowQuery(t)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(got), "bad sql")
}

// No peer advertises the capability (an older reader in a rolling upgrade),
// or this node can serve queries: the query runs here, as before.
func TestArrowQueryIsNotForwardedWithoutACapablePeer(t *testing.T) {
	for _, c := range []struct {
		name    string
		role    cluster.NodeRole
		capable bool
	}{
		{"compactor, peer lacks the capability", cluster.RoleCompactor, false},
		{"reader serves locally", cluster.RoleReader, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newDoorRig(t)
			up := &arrowUpstream{body: []byte("never")}
			peer := httptest.NewServer(up)
			defer peer.Close()
			r.qh.SetRouter(arrowForwardRouter(t, peer.URL, c.role, c.capable))

			resp := r.arrowQuery(t)
			_, _ = io.ReadAll(resp.Body)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "application/vnd.apache.arrow.stream", resp.Header.Get("Content-Type"))
			require.Zero(t, up.hits.Load(), "forwarded although it should run locally")
		})
	}
}

// A forwarded identity is accepted on the Arrow route (#1305's list).
func TestArrowQueryRouteAcceptsAForwardedIdentity(t *testing.T) {
	require.True(t, forwardedRoutes["POST /api/v1/query/arrow"])
}
