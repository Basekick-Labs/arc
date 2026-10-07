package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func newImportForwardingRouter(t *testing.T, peerURL string, role cluster.NodeRole) *cluster.Router {
	t.Helper()

	localNode := cluster.NewNode("local", "Local", role, "test-cluster")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})
	writer := cluster.NewNode("writer", "Writer", cluster.RoleWriter, "test-cluster")
	writer.UpdateState(cluster.StateHealthy)
	writer.APIAddress = strings.TrimPrefix(peerURL, "http://")
	require.NoError(t, registry.Register(writer))

	return cluster.NewRouter(&cluster.RouterConfig{
		Timeout:   time.Second,
		Retries:   0,
		Registry:  registry,
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})
}

func TestImportRoutesForwardWritesFromNonIngestNodes(t *testing.T) {
	type forwardedRequest struct {
		path      string
		body      string
		forwarded string
	}

	requests := make(chan forwardedRequest, 8)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		requests <- forwardedRequest{
			path:      r.URL.RequestURI(),
			body:      string(body),
			forwarded: r.Header.Get(ForwardedByHeader),
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("forwarded"))
	}))
	defer peer.Close()

	paths := []string{
		"/api/v1/import/csv",
		"/api/v1/import/parquet",
		"/api/v1/import/lp",
		"/api/v1/import/tle",
	}
	for _, role := range []cluster.NodeRole{cluster.RoleReader, cluster.RoleCompactor} {
		app := fiber.New()
		handler := NewImportHandler(zerolog.Nop())
		handler.SetRouter(newImportForwardingRouter(t, peer.URL, role))
		handler.RegisterRoutes(app)

		for _, path := range paths {
			t.Run(string(role)+path, func(t *testing.T) {
				body := "import body that is not parsed on this node"
				req := httptest.NewRequest(http.MethodPost, path+"?db=sample", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/octet-stream")
				resp, err := app.Test(req, testRequestTimeoutMS)
				require.NoError(t, err)
				defer resp.Body.Close()
				responseBody, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, http.StatusAccepted, resp.StatusCode)
				require.Equal(t, "forwarded", string(responseBody))

				got := <-requests
				require.Equal(t, path+"?db=sample", got.path)
				require.Equal(t, body, got.body)
				require.Equal(t, "local", got.forwarded)
			})
		}
	}
}

func TestImportRoutesRejectAlreadyForwardedWrites(t *testing.T) {
	var peerCalls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer peer.Close()

	app := fiber.New()
	handler := NewImportHandler(zerolog.Nop())
	handler.SetRouter(newImportForwardingRouter(t, peer.URL, cluster.RoleCompactor))
	handler.RegisterRoutes(app)

	for _, path := range []string{
		"/api/v1/import/csv",
		"/api/v1/import/parquet",
		"/api/v1/import/lp",
		"/api/v1/import/tle",
	} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("payload"))
			req.Header.Set(ForwardedByHeader, "another-node")
			resp, err := app.Test(req, testRequestTimeoutMS)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusLoopDetected, resp.StatusCode)
		})
	}
	require.Zero(t, peerCalls.Load(), "loop-marked imports must not be forwarded again")
}
