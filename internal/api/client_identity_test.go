package api

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/auth"
)

type recSpy struct {
	id, version         string
	first, firstVersion string // the third recorded pair, kept to detect buffer reuse
	calls               int
}

func (r *recSpy) RecordClient(id, version string) {
	r.id, r.version, r.calls = id, version, r.calls+1
	if r.calls == 3 {
		r.first, r.firstVersion = id, version
	}
}

func TestClientIdentityMiddleware(t *testing.T) {
	spy := &recSpy{}
	app := fiber.New()
	app.Use(clientIdentity(spy, false))
	app.Get("/x", func(c *fiber.Ctx) error { return c.SendString("ok") })
	app.Get("/denied", func(c *fiber.Ctx) error { return c.SendStatus(401) })
	for _, p := range []string{"/health", "/ready", "/metrics", "/api/v1/metrics", "/api/v1/auth/verify"} {
		app.Get(p, func(c *fiber.Ctx) error { return c.SendString("public") })
	}

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(HeaderArcliInstallationID, "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
	req.Header.Set("User-Agent", "arcli/26.09.1 (darwin/arm64)")
	if resp, err := app.Test(req); err != nil || resp.StatusCode != 200 {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	if spy.calls != 1 || spy.id != "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f" || spy.version != "26.09.1" {
		t.Errorf("spy: %+v", spy)
	}
	// No header: recorder untouched.
	_, _ = app.Test(httptest.NewRequest("GET", "/x", nil))
	if spy.calls != 1 {
		t.Errorf("no header must not record: %+v", spy)
	}
	// Foreign user agent: id still recorded, version empty (→ unknown).
	req = httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(HeaderArcliInstallationID, "1f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
	req.Header.Set("User-Agent", "curl/8.0")
	_, _ = app.Test(req)
	if spy.calls != 2 || spy.version != "" {
		t.Errorf("foreign UA: %+v", spy)
	}
	// Handlers that return an error (Fiber renders the status after the
	// stack unwinds) and unknown routes are not "served".
	app.Get("/errs", func(c *fiber.Ctx) error { return fiber.NewError(401, "no") })
	app.Get("/boom", func(c *fiber.Ctx) error { return fmt.Errorf("boom") })
	for _, p := range []string{"/errs", "/boom", "/no-such-route"} {
		req = httptest.NewRequest("GET", p, nil)
		req.Header.Set(HeaderArcliInstallationID, "7f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
		_, _ = app.Test(req)
		if spy.calls != 2 {
			t.Errorf("%s must not record: %+v", p, spy)
		}
	}
	// Rejected requests (401/403/…) never reach the recorder.
	req = httptest.NewRequest("GET", "/denied", nil)
	req.Header.Set(HeaderArcliInstallationID, "2f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
	_, _ = app.Test(req)
	if spy.calls != 2 {
		t.Errorf("401 must not record: %+v", spy)
	}
	// Public (token-less) routes answer 200 but are never counted, in
	// any of the spellings Fiber's case-insensitive, slash-tolerant
	// router accepts.
	for _, p := range []string{"/health", "/health/", "/HEALTH", "/Health/", "/ready", "/metrics", "/api/v1/metrics", "/API/v1/metrics/", "/api/v1/auth/verify"} {
		req = httptest.NewRequest("GET", p, nil)
		req.Header.Set(HeaderArcliInstallationID, "5f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
		if resp, _ := app.Test(req); resp.StatusCode != 200 || spy.calls != 2 {
			t.Errorf("%s: status=%d calls=%d", p, resp.StatusCode, spy.calls)
		}
	}
	// Wrong length is dropped before any copy.
	req = httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(HeaderArcliInstallationID, "short")
	_, _ = app.Test(req)
	if spy.calls != 2 {
		t.Errorf("short id must not record: %+v", spy)
	}
	// The recorded strings survive the request buffer being reused.
	req = httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(HeaderArcliInstallationID, "3f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
	req.Header.Set("User-Agent", "arcli/1.2.3")
	_, _ = app.Test(req)
	for i := 0; i < 50; i++ {
		r2 := httptest.NewRequest("GET", "/x", nil)
		r2.Header.Set("User-Agent", strings.Repeat("z", 64))
		r2.Header.Set(HeaderArcliInstallationID, "4f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
		_, _ = app.Test(r2)
	}
	if spy.first != "3f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f" || spy.firstVersion != "1.2.3" {
		t.Errorf("stored strings mutated: %q %q", spy.first, spy.firstVersion)
	}
}

func TestArcliVersion(t *testing.T) {
	for in, want := range map[string]string{
		"arcli/26.09.1 (darwin/arm64)": "26.09.1",
		"arcli/dev":                    "dev",
		"arcli":                        "",
		"Arc/26.09.1":                  "",
		"":                             "",
	} {
		if got := arcliVersion(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestClientIdentityRequiresValidatedToken(t *testing.T) {
	spy := &recSpy{}
	app := fiber.New()
	app.Use(clientIdentity(spy, true))
	// A route the auth middleware would guard: without token_info set the
	// request is not counted even though it answers 200.
	app.Get("/open200", func(c *fiber.Ctx) error { return c.SendString("ok") })
	app.Get("/authed", func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{Name: "t"})
		return c.SendString("ok")
	})
	for _, tc := range []struct {
		path  string
		calls int
	}{{"/open200", 0}, {"/authed", 1}, {"/AUTHED/", 2}} {
		req := httptest.NewRequest("GET", tc.path, nil)
		req.Header.Set(HeaderArcliInstallationID, "6f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
		if resp, _ := app.Test(req); resp.StatusCode != 200 || spy.calls != tc.calls {
			t.Errorf("%s: status=%d calls=%d want %d", tc.path, resp.StatusCode, spy.calls, tc.calls)
		}
	}
}
