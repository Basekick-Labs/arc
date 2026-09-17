package audit

import (
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/valyala/fasthttp"
)

func TestMiddlewareOwnsRequestDataBeforeEnqueue(t *testing.T) {
	for _, tc := range []struct {
		name, route, path, query, database, measurement, userAgent string
		headers, detail                                            bool
	}{
		{"headers take precedence", "/api/v1/databases/:database/:measurement", "/api/v1/databases/param-db/param-measurement", "db=query-db", "header-db", "header-measurement", strings.Repeat("A", 257), true, true},
		{"parameter fallback", "/api/v1/databases/:database/:measurement", "/api/v1/databases/param-db/param-measurement", "db=query-db", "param-db", "param-measurement", strings.Repeat("B", 256), false, false},
		{"query fallback", "/api/v1/databases", "/api/v1/databases", "db=query-db", "query-db", "", "client", false, false},
		{"empty metadata", "/api/v1/databases", "/api/v1/databases", "", "", "", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := &Logger{eventCh: make(chan *AuditEvent, 1)}
			app := fiber.New(fiber.Config{ProxyHeader: fiber.HeaderXForwardedFor})
			app.Use(Middleware(logger, true))
			var buffers [][]byte
			var detail map[string]string
			app.Post(tc.route, func(c *fiber.Ctx) error {
				// Capture the actual borrowed storage, not a separately copied fixture.
				for _, value := range []string{c.Method(), c.Path(), c.IP(), c.Get("User-Agent"), c.Get("x-arc-database"), c.Get("x-arc-measurement"), c.Params("database"), c.Params("measurement"), c.Query("db")} {
					buffers = append(buffers, utils.UnsafeBytes(value))
				}
				if tc.detail {
					key, value := c.Query("detail-key"), c.Query("detail-value")
					detail = map[string]string{key: value}
					buffers = append(buffers, utils.UnsafeBytes(key), utils.UnsafeBytes(value))
				} else if tc.name == "empty metadata" {
					detail = map[string]string{}
				}
				// Literal key also lets this regression compile against the original middleware.
				c.Locals("audit_detail", detail)
				c.Locals("token_info", &auth.TokenInfo{Name: "service-token"})
				return c.SendStatus(fiber.StatusCreated)
			})

			ctx := &fasthttp.RequestCtx{}
			ctx.Request.Header.SetMethod(fiber.MethodPost)
			ctx.Request.SetRequestURI(tc.path + "?" + tc.query + "&detail-key=target&detail-value=first-value")
			ctx.Request.Header.Set(fiber.HeaderXForwardedFor, "192.0.2.1")
			ctx.Request.Header.SetUserAgent(tc.userAgent)
			if tc.headers {
				ctx.Request.Header.Set("x-arc-database", "header-db")
				ctx.Request.Header.Set("x-arc-measurement", "header-measurement")
			}
			app.Handler()(ctx)
			var event *AuditEvent
			select {
			case event = <-logger.eventCh:
			default:
				t.Fatal("middleware did not enqueue an event")
			}

			if tc.detail {
				detail["target"] = "handler changed the map"
				detail["new-key"] = "new-value"
			}
			// Simulate reuse only after the handler and middleware have returned.
			// There is no concurrent access and no dependency on pool scheduling.
			for _, buffer := range buffers {
				for i := range buffer {
					buffer[i] = 'x'
				}
			}
			for _, field := range []struct{ name, got, want string }{
				{"path", event.Path, tc.path},
				{"IP", event.IPAddress, "192.0.2.1"},
				{"database", event.Database, tc.database},
				{"measurement", event.Measurement, tc.measurement},
				{"user agent", event.UserAgent, tc.userAgent[:min(len(tc.userAgent), 256)]},
				{"actor", event.Actor, "service-token"},
				{"event type", event.EventType, "database.created"},
				{"method", event.Method, fiber.MethodPost},
			} {
				if field.got != field.want {
					t.Errorf("%s after buffer reuse: got %q, want %q", field.name, field.got, field.want)
				}
			}
			if event.StatusCode != fiber.StatusCreated {
				t.Errorf("status = %d, want 201", event.StatusCode)
			}
			if tc.detail {
				if len(event.Detail) != 1 || event.Detail["target"] != "first-value" {
					t.Errorf("detail aliases request storage or the handler map: %#v", event.Detail)
				}
			} else if len(event.Detail) != 0 {
				t.Errorf("unexpected details: %#v", event.Detail)
			}
		})
	}
}

func TestMiddlewareSkipsExcludedAndReadPaths(t *testing.T) {
	logger := &Logger{eventCh: make(chan *AuditEvent, 1)}
	app := fiber.New()
	app.Use(Middleware(logger, false))
	app.Get("/read", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })
	app.Post("/health", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })
	for _, request := range []struct{ method, uri string }{{fiber.MethodGet, "/read"}, {fiber.MethodPost, "/health"}} {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod(request.method)
		ctx.Request.SetRequestURI(request.uri)
		app.Handler()(ctx)
	}
	select {
	case event := <-logger.eventCh:
		t.Fatalf("unexpected event for ignored request: %#v", event)
	default:
	}
}
