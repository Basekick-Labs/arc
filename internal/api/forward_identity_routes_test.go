package api

import (
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/gofiber/fiber/v2"
)

type acceptingChecker struct{}

func (acceptingChecker) VerifyForwardIdentity(values cluster.ForwardIdentityHeaderValues, _, _ string) (bool, int64, error) {
	if len(values("X-Arc-Forward-Hmac")) == 0 {
		return false, 0, nil
	}
	return true, 7, nil
}

// A verified forwarded identity is accepted only on the routes a peer
// forwards, so it cannot stand in for a token anywhere else.
func TestForwardIdentityVerifier_OnlyOnForwardedRoutes(t *testing.T) {
	verify := ForwardIdentityVerifier(acceptingChecker{})
	app := fiber.New()
	app.All("/*", func(c *fiber.Ctx) error {
		present, id, err := verify(c)
		switch {
		case !present:
			return c.SendString("absent")
		case err != nil:
			return c.SendString("refused")
		default:
			_ = id
			return c.SendString("accepted")
		}
	})
	for _, tc := range []struct {
		method, path, want string
		withHeader         bool
	}{
		{"POST", "/api/v1/write/msgpack", "accepted", true},
		{"POST", "/write", "accepted", true},
		{"POST", "/api/v2/write", "accepted", true},
		{"POST", "/api/v1/write/line-protocol", "accepted", true},
		{"POST", "/api/v1/write/tle", "accepted", true},
		{"POST", "/api/v1/import/csv", "accepted", true},
		{"POST", "/api/v1/import/parquet", "accepted", true},
		{"POST", "/api/v1/import/lp", "accepted", true},
		{"POST", "/api/v1/import/tle", "accepted", true},
		{"POST", "/api/v1/query", "accepted", true},
		{"POST", "/api/v1/query/msgpack", "accepted", true},
		{"POST", "/api/v1/query/arrow", "refused", true},
		{"POST", "/api/v1/auth/tokens", "refused", true},
		{"DELETE", "/api/v1/databases/x", "refused", true},
		{"GET", "/api/v1/query", "refused", true},
		{"POST", "/api/v1/write/msgpack/../../auth/tokens", "refused", true},
		{"POST", "/api/v1/write/msgpack", "absent", false},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		if tc.withHeader {
			req.Header.Set("X-Arc-Forward-Hmac", "x")
		}
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 16)
		n, _ := resp.Body.Read(buf)
		if got := string(buf[:n]); got != tc.want {
			t.Errorf("%s %s: %s, want %s", tc.method, tc.path, got, tc.want)
		}
	}
}

// A client cannot carry an assertion header across a forward hop.
func TestBuildHTTPRequest_StripsForwardIdentityHeaders(t *testing.T) {
	for _, h := range []string{"X-Arc-Forward-Hmac", "X-Arc-Forward-Token-Id", "X-Arc-Forward-Node"} {
		if !isClientForwardingHeader(h) {
			t.Errorf("%s is not stripped", h)
		}
	}
}

// The request handed to the router carries the ID of the token this node
// verified, which the router asserts to the peer.
func TestBuildHTTPRequest_CarriesVerifiedTokenID(t *testing.T) {
	app := fiber.New()
	var got int64 = -1
	app.Post("/w", func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{ID: 42})
		req, err := BuildHTTPRequest(c)
		if err != nil {
			return err
		}
		got = cluster.ForwardTokenID(req.Context())
		return nil
	})
	if _, err := app.Test(httptest.NewRequest("POST", "/w", nil), -1); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("token id %d, want 42", got)
	}
}
