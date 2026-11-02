package api

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/telemetry"
)

type recSpy struct {
	kind, id, version, runtime, platform string
	first, firstVersion, firstPlatform   string // the third recorded call, kept to detect buffer reuse
	calls                                int
}

func (r *recSpy) RecordClient(kind, id, version, runtime, platform string) {
	r.kind, r.id, r.version, r.runtime, r.platform, r.calls = kind, id, version, runtime, platform, r.calls+1
	if r.calls == 3 {
		r.first, r.firstVersion, r.firstPlatform = id, version, platform
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
	if spy.calls != 1 || spy.kind != ClientKindArcli || spy.id != "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f" || spy.version != "26.09.1" || spy.runtime != "" || spy.platform != "" {
		t.Errorf("spy: %+v", spy)
	}
	// No header: recorder untouched.
	_, _ = app.Test(httptest.NewRequest("GET", "/x", nil))
	if spy.calls != 1 {
		t.Errorf("no header must not record: %+v", spy)
	}
	// Foreign or missing user agent: not a known client, nothing recorded,
	// with either header.
	for _, h := range []string{HeaderArcliInstallationID, HeaderClientInstallationID} {
		for _, ua := range []string{"curl/8.0", "", "Arc/26.09.1", "arcli", "python-requests/2.32.0", "ARCLI/26.09.1"} {
			req = httptest.NewRequest("GET", "/x", nil)
			req.Header.Set(h, "1f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
			if ua != "" {
				req.Header.Set("User-Agent", ua)
			}
			_, _ = app.Test(req)
			if spy.calls != 1 {
				t.Errorf("%s with UA %q must not record: %+v", h, ua, spy)
			}
		}
	}
	// The Python SDK on the generic header: kind, runtime and platform.
	req = httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(HeaderClientInstallationID, "1f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
	req.Header.Set("User-Agent", "arc-client-python/0.2.0 (python/3.12.4; linux/amd64)")
	_, _ = app.Test(req)
	if spy.calls != 2 || spy.kind != ClientKindPythonSDK || spy.id != "1f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f" ||
		spy.version != "0.2.0" || spy.runtime != "3.12.4" || spy.platform != "linux/amd64" {
		t.Errorf("python sdk: %+v", spy)
	}
	// Handlers that return an error (Fiber renders the status after the
	// stack unwinds) and unknown routes are not "served".
	app.Get("/errs", func(c *fiber.Ctx) error { return fiber.NewError(401, "no") })
	app.Get("/boom", func(c *fiber.Ctx) error { return fmt.Errorf("boom") })
	for _, p := range []string{"/errs", "/boom", "/no-such-route"} {
		req = httptest.NewRequest("GET", p, nil)
		req.Header.Set(HeaderArcliInstallationID, "7f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
		req.Header.Set("User-Agent", "arcli/26.09.1")
		_, _ = app.Test(req)
		if spy.calls != 2 {
			t.Errorf("%s must not record: %+v", p, spy)
		}
	}
	// Rejected requests (401/403/…) never reach the recorder.
	req = httptest.NewRequest("GET", "/denied", nil)
	req.Header.Set(HeaderArcliInstallationID, "2f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
	req.Header.Set("User-Agent", "arcli/26.09.1")
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
		req.Header.Set("User-Agent", "arcli/26.09.1")
		if resp, _ := app.Test(req); resp.StatusCode != 200 || spy.calls != 2 {
			t.Errorf("%s: status=%d calls=%d", p, resp.StatusCode, spy.calls)
		}
	}
	// Wrong length is dropped before any copy.
	req = httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(HeaderArcliInstallationID, "short")
	req.Header.Set("User-Agent", "arcli/26.09.1")
	_, _ = app.Test(req)
	if spy.calls != 2 {
		t.Errorf("short id must not record: %+v", spy)
	}
	// The recorded strings survive the request buffer being reused.
	req = httptest.NewRequest("GET", "/x", nil)
	req.Header.Set(HeaderClientInstallationID, "3f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
	req.Header.Set("User-Agent", "arc-client-python/1.2.3 (python/3.11.9; darwin/arm64)")
	_, _ = app.Test(req)
	for i := 0; i < 50; i++ {
		r2 := httptest.NewRequest("GET", "/x", nil)
		r2.Header.Set("User-Agent", "arc-client-python/"+strings.Repeat("z", 64)+" (python/"+strings.Repeat("y", 8)+"; "+strings.Repeat("x", 16)+")")
		r2.Header.Set(HeaderClientInstallationID, "4f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
		_, _ = app.Test(r2)
	}
	if spy.calls != 53 || spy.first != "3f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f" || spy.firstVersion != "1.2.3" || spy.firstPlatform != "darwin/arm64" {
		t.Errorf("stored strings mutated: calls=%d %q %q %q", spy.calls, spy.first, spy.firstVersion, spy.firstPlatform)
	}
}

func TestParseClientUserAgent(t *testing.T) {
	for in, want := range map[string]clientUA{
		"arcli/26.09.1 (darwin/arm64)": {kind: ClientKindArcli, version: "26.09.1"},
		"arcli/dev":                    {kind: ClientKindArcli, version: "dev"},
		"arcli/":                       {kind: ClientKindArcli},
		"arc-client-python/0.2.0 (python/3.12.4; linux/amd64)":     {kind: ClientKindPythonSDK, version: "0.2.0", runtime: "3.12.4", platform: "linux/amd64"},
		"arc-client-python/0.2.0 (python/3.13.0rc1;windows/arm64)": {kind: ClientKindPythonSDK, version: "0.2.0", runtime: "3.13.0rc1", platform: "windows/arm64"},
		// Older SDKs: no comment, runtime and platform unknown.
		"arc-client-python/0.1.0": {kind: ClientKindPythonSDK, version: "0.1.0"},
		// Malformed comments keep the version, report neither runtime nor platform.
		"arc-client-python/0.2.0 (linux/amd64)":                       {kind: ClientKindPythonSDK, version: "0.2.0"},
		"arc-client-python/0.2.0 (pypy/7.3; linux/amd64)":             {kind: ClientKindPythonSDK, version: "0.2.0"},
		"arc-client-python/0.2.0 (python/3.12.4; linux/amd64; extra)": {kind: ClientKindPythonSDK, version: "0.2.0"},
		"arc-client-python/0.2.0 (python/3.12.4; linux/amd64":         {kind: ClientKindPythonSDK, version: "0.2.0"},
		"arc-client-python/0.2.0 garbage":                             {kind: ClientKindPythonSDK, version: "0.2.0"},
		// Values the recorder cleans (spaces, length) still parse here.
		"arc-client-python/0.2.0 (python/3.12.4; linux amd64)": {kind: ClientKindPythonSDK, version: "0.2.0", runtime: "3.12.4", platform: "linux amd64"},
		// Not a known client.
		"arcli":                   {},
		"Arc/26.09.1":             {},
		"arc-client-python":       {},
		"Arc-Client-Python/0.2.0": {},
		"arc-client-go/0.1.0":     {},
		"python-requests/2.32.0":  {},
		"":                        {},
	} {
		if got := parseClientUserAgent(in); got != want {
			t.Errorf("%q: got %+v want %+v", in, got, want)
		}
	}
}

// Arc-Client-Installation-Id is accepted, the legacy arcli header still
// is, and the generic header wins when a client sends both.
func TestClientIdentityHeaders(t *testing.T) {
	spy := &recSpy{}
	app := fiber.New()
	app.Use(clientIdentity(spy, false))
	app.Get("/x", func(c *fiber.Ctx) error { return c.SendString("ok") })
	const legacy, generic = "aaaaaaaa-0f0f-4f0f-8f0f-0f0f0f0f0f0f", "bbbbbbbb-0f0f-4f0f-8f0f-0f0f0f0f0f0f"
	for _, tc := range []struct {
		name, ua         string
		headers          map[string]string
		wantKind, wantID string
	}{
		{"arcli legacy header", "arcli/26.09.1 (darwin/arm64)", map[string]string{HeaderArcliInstallationID: legacy}, ClientKindArcli, legacy},
		{"arcli generic header", "arcli/26.09.1 (darwin/arm64)", map[string]string{HeaderClientInstallationID: generic}, ClientKindArcli, generic},
		{"sdk generic header", "arc-client-python/0.2.0 (python/3.12.4; linux/amd64)", map[string]string{HeaderClientInstallationID: generic}, ClientKindPythonSDK, generic},
		{"sdk legacy header", "arc-client-python/0.2.0", map[string]string{HeaderArcliInstallationID: legacy}, ClientKindPythonSDK, legacy},
		{"both: generic wins", "arcli/26.09.1", map[string]string{HeaderArcliInstallationID: legacy, HeaderClientInstallationID: generic}, ClientKindArcli, generic},
	} {
		before := spy.calls
		req := httptest.NewRequest("GET", "/x", nil)
		req.Header.Set("User-Agent", tc.ua)
		for k, v := range tc.headers {
			req.Header.Set(k, v)
		}
		if resp, err := app.Test(req); err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s: resp=%v err=%v", tc.name, resp, err)
		}
		if spy.calls != before+1 || spy.kind != tc.wantKind || spy.id != tc.wantID {
			t.Errorf("%s: %+v", tc.name, spy)
		}
	}
	// A malformed generic header is not rescued by the legacy one: the
	// generic header was sent, so it is the one judged.
	before := spy.calls
	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("User-Agent", "arcli/26.09.1")
	req.Header.Set(HeaderClientInstallationID, "short")
	req.Header.Set(HeaderArcliInstallationID, legacy)
	_, _ = app.Test(req)
	if spy.calls != before {
		t.Errorf("short generic id must not record: %+v", spy)
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
		req.Header.Set(HeaderClientInstallationID, "6f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f")
		req.Header.Set("User-Agent", "arc-client-python/0.2.0 (python/3.12.4; linux/amd64)")
		if resp, _ := app.Test(req); resp.StatusCode != 200 || spy.calls != tc.calls {
			t.Errorf("%s: status=%d calls=%d want %d", tc.path, resp.StatusCode, spy.calls, tc.calls)
		}
	}
}

// The registry drops any kind it does not know, so a rename on one side
// would silently stop counting that client. Neither package imports the
// other in production; this test is where the two are tied together.
func TestClientKindsMatchTelemetry(t *testing.T) {
	if ClientKindArcli != telemetry.ClientKindArcli || ClientKindPythonSDK != telemetry.ClientKindPythonSDK {
		t.Fatalf("api kinds (%q, %q) != telemetry kinds (%q, %q)",
			ClientKindArcli, ClientKindPythonSDK, telemetry.ClientKindArcli, telemetry.ClientKindPythonSDK)
	}
}

func TestParseClientUserAgent_OverlongVersionDropped(t *testing.T) {
	ua := parseClientUserAgent("arc-client-python/" + strings.Repeat("9", 4096) + " (python/3.12.4; linux/amd64)")
	if ua.kind != ClientKindPythonSDK || ua.version != "" || ua.runtime != "3.12.4" || ua.platform != "linux/amd64" {
		t.Fatalf("got %+v", ua)
	}
	if got := parseClientUserAgent("arc-client-python/0.2.0 (python/3.12.4; linux/amd64; extra)"); got.runtime != "" || got.platform != "" {
		t.Fatalf("three-element comment must not parse: %+v", got)
	}
}
