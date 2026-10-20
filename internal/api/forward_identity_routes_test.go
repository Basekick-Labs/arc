package api

import (
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/gofiber/fiber/v2"
)

type acceptingChecker struct{}

func (acceptingChecker) VerifyForwardIdentity(values cluster.ForwardIdentityHeaderValues, _, _ string) (bool, cluster.ForwardToken, error) {
	if len(values("X-Arc-Forward-Hmac")) == 0 {
		return false, cluster.ForwardToken{}, nil
	}
	return true, cluster.ForwardToken{ID: 7, Fingerprint: "f"}, nil
}

// A verified forwarded identity is accepted only on the routes a peer
// forwards, so it cannot stand in for a token anywhere else.
func TestForwardIdentityVerifier_OnlyOnForwardedRoutes(t *testing.T) {
	verify := ForwardIdentityVerifier(acceptingChecker{})
	app := fiber.New()
	app.All("/*", func(c *fiber.Ctx) error {
		present, id, _, err := verify(c)
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
	var got cluster.ForwardToken
	app.Post("/w", func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{ID: 42, Fingerprint: "fp"})
		req, err := BuildHTTPRequest(c)
		if err != nil {
			return err
		}
		got = cluster.ForwardTokenOf(req.Context())
		return nil
	})
	if _, err := app.Test(httptest.NewRequest("POST", "/w", nil), -1); err != nil {
		t.Fatal(err)
	}
	if got != (cluster.ForwardToken{ID: 42, Fingerprint: "fp"}) {
		t.Fatalf("token %+v, want 42/fp", got)
	}
}

type uriRecorder struct{ uri string }

func (u *uriRecorder) VerifyForwardIdentity(_ cluster.ForwardIdentityHeaderValues, _, uri string) (bool, cluster.ForwardToken, error) {
	u.uri = uri
	return false, cluster.ForwardToken{}, nil
}

// The identity is checked against the request URI as received, which is what
// the peer signed, even after earlier middleware parsed the query (fasthttp
// then rebuilds RequestURI() from the parsed form).
func TestForwardIdentityVerifier_ChecksTheRawRequestURI(t *testing.T) {
	rec := &uriRecorder{}
	verify := ForwardIdentityVerifier(rec)
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error { _ = c.Query("db"); return c.Next() })
	app.Post("/api/v1/write/msgpack", func(c *fiber.Ctx) error {
		_, _, _, _ = verify(c)
		return nil
	})
	const uri = "/api/v1/write/msgpack?db=a&s=a%20b&&c=%2B&e=%zz"
	if _, err := app.Test(httptest.NewRequest("POST", uri, nil), -1); err != nil {
		t.Fatal(err)
	}
	if rec.uri != uri {
		t.Fatalf("checked %q, the peer signed %q", rec.uri, uri)
	}
}
