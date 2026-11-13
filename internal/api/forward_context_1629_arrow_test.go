//go:build duckdb_arrow

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1629: a forwarded Arrow query (#1567) dials on a context that does not
// reach the pooled fasthttp request's values. Its answer is streamed after
// the handler returns, so this is the forward whose context outlives the
// handler the longest.
func TestForwardedArrowQueryDoesNotReachTheRequestContext(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			peer := &probePeer{status: status}
			srv := httptest.NewServer(peer)
			defer srv.Close()
			probe := &dialProbe{}
			qh := NewQueryHandler(nil, nil, zerolog.Nop(), 0, 0)
			qh.SetRouter(probeRouter(t, cluster.RoleCompactor, cluster.RoleReader, srv, probe, false))
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			app.Use(probeMiddleware)
			qh.RegisterRoutes(app)

			resp, err := app.Test(mustProbeRequest(t, "/api/v1/query/arrow", `{"sql":"SELECT 1"}`), 10000)
			require.NoError(t, err)
			_ = resp.Body.Close()
			require.Equal(t, status, resp.StatusCode)
			probe.requireClean(t)
		})
	}
}
