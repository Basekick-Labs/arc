package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func TestCacheInvalidationSenderReachesPeer(t *testing.T) {
	for _, tc := range []struct{ name, api, coordinator, want string }{
		{"hostless", ":8000", "w2:9100", "w2:8000"},
		{"IPv4 wildcard", "0.0.0.0:8000", "w2:9100", "w2:8000"},
		{"IPv6 wildcard", "[::]:8443", "[2001:db8::2]:9100", "[2001:db8::2]:8443"},
		{"explicit API host", "api-w2:8443", "w2:9100", "api-w2:8443"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const secret, clusterName = "test-shared-secret", "test-cluster"
			var invalidations, dials atomic.Int32
			app := fiber.New(fiber.Config{DisableStartupMessage: true})
			api.NewCacheInvalidateHandler(secret, clusterName, "w2",
				security.NewNonceCache(cacheInvalidateHMACTolerance), cacheInvalidateHMACTolerance,
				func() { invalidations.Add(1) }, zerolog.Nop()).Register(app)
			t.Cleanup(func() { _ = app.Shutdown() })

			// Use the real receiver to check the sender's method, path and HMAC.
			// Remap only the TCP dial so this needs neither DNS nor a fixed port.
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Host != tc.want || r.Method != http.MethodPost || r.URL.Path != api.CacheInvalidatePath {
					t.Errorf("request = %s %s host %q, want POST %s host %q",
						r.Method, r.URL.Path, r.Host, api.CacheInvalidatePath, tc.want)
				}
				resp, err := app.Test(r, 5000)
				if err != nil {
					t.Errorf("peer receiver: %v", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusNoContent {
					t.Errorf("peer status = %d, want 204", resp.StatusCode)
				}
				w.WriteHeader(resp.StatusCode)
			}))
			defer peer.Close()
			transport := security.NewClusterHTTPTransport(nil)
			transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				dials.Add(1)
				if address != tc.want {
					return nil, fmt.Errorf("dial target = %q, want %q", address, tc.want)
				}
				return (&net.Dialer{}).DialContext(ctx, network, peer.Listener.Addr().String())
			}
			defer transport.CloseIdleConnections()
			node := cluster.NewNode("w2", "w2", cluster.RoleWriter, clusterName)
			node.SetAddresses(tc.coordinator, tc.api)
			invalidatePeerQueryCaches(&http.Client{Transport: transport}, "http", node,
				secret, "w1", clusterName, zerolog.Nop())
			if got := dials.Load(); got != 1 {
				t.Errorf("dials = %d, want 1", got)
			}
			if got := invalidations.Load(); got != 1 {
				t.Fatalf("peer invalidations = %d, want 1 (expected dial target %s)", got, tc.want)
			}
		})
	}
}
