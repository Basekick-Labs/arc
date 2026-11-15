package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1559: the query and ingest routes gated on the token's coarse read/write
// bit, so a token whose access comes only from RBAC (created with no
// permissions, added to a team) was refused before RBAC was asked. The door
// now admits a token holding the permission on anything; the handlers decide
// per table and per measurement.
//
// Everything here is real: AuthManager, RBACManager, the global auth
// middleware and each handler's own RegisterRoutes. A rig that injects
// token_info or leaves the auth manager nil would pass the door by
// construction and test nothing.

type doorRig struct {
	app    *fiber.App
	tokens map[string]string
	am     *auth.AuthManager
	qh     *QueryHandler
	dbs    *DatabasesHandler
}

// doorDenied is the door's refusal body; a handler's RBAC refusal reads
// differently, which is how the tests tell the two apart.
func doorDenied(permission string) string { return "Permission denied: " + permission + " required" }

func newDoorRig(t *testing.T) *doorRig {
	t.Helper()
	ctx := context.Background()
	logger := zerolog.Nop()
	metrics.Init(logger)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	backend, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	t.Cleanup(func() { buffer.Close() })
	for _, dm := range [][2]string{{"tenanta", "cpu"}, {"tenanta", "mem"}, {"tenantb", "cpu"}} {
		db, meas := dm[0], dm[1]
		if err := buffer.WriteTypedColumnarDirect(ctx, db, meas, &ingest.TypedColumnBatch{
			Data: map[string]interface{}{
				"time":  []int64{time.Now().UTC().UnixMicro()},
				"value": []int64{1},
			},
		}, 1); err != nil {
			t.Fatalf("write %s fixture: %v", db, err)
		}
	}
	if err := buffer.FlushAllAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })

	am, err := auth.NewAuthManager(filepath.Join(root, "auth.db"), 5*time.Minute, 100, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	rm := auth.NewRBACManager(&auth.RBACManagerConfig{DB: am.GetDB(), Logger: logger})
	org, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "door-org"})
	if err != nil {
		t.Fatal(err)
	}
	// team creates a team with one role on tenanta; measPerms, when set, makes
	// the role measurement-scoped to cpu with those permissions.
	team := func(name string, rolePerms, measPerms []string) int64 {
		t.Helper()
		tm, err := rm.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		role, err := rm.CreateRole(ctx, tm.ID, &auth.CreateRoleRequest{DatabasePattern: "tenanta", Permissions: rolePerms})
		if err != nil {
			t.Fatal(err)
		}
		if measPerms != nil {
			if _, err := rm.CreateMeasurementPermission(ctx, role.ID, &auth.CreateMeasurementPermissionRequest{
				MeasurementPattern: "cpu", Permissions: measPerms,
			}); err != nil {
				t.Fatal(err)
			}
		}
		return tm.ID
	}
	tokens := map[string]string{}
	token := func(name, perms string, teamID int64) {
		t.Helper()
		tok, err := am.CreateToken(ctx, name, name, perms, nil)
		if err != nil {
			t.Fatal(err)
		}
		if teamID != 0 {
			info := am.VerifyToken(tok)
			if info == nil {
				t.Fatal("verify token")
			}
			if _, err := rm.AddTokenToTeam(ctx, info.ID, teamID); err != nil {
				t.Fatal(err)
			}
		}
		tokens[name] = tok
	}
	// RBAC-only, read+write on tenanta: the issue's token.
	token("rbac-only", auth.PermissionsNone, team("rw", []string{"read", "write"}, nil))
	// RBAC-only, measurement-scoped: read on tenanta.cpu only. The role-level
	// write does not count once the role has measurement grants.
	token("rbac-meas-read", auth.PermissionsNone, team("meas", []string{"read", "write"}, []string{"read"}))
	// No permissions and no team: refused everywhere, as before.
	token("none", auth.PermissionsNone, 0)
	// Coarse read,write whose team grants neither: RBAC is authoritative for a
	// team member, so the door refuses it (the deliberate narrowing).
	token("coarse-team-no-grant", "read,write", team("del", []string{"delete"}, nil))
	// Coarse read,write and no team: the OSS model, unchanged.
	token("coarse-no-team", "read,write", 0)
	rm.InvalidateAllCache()

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	mw := auth.DefaultMiddlewareConfig()
	mw.AuthManager = am
	app.Use(auth.NewMiddleware(mw))

	qh := NewQueryHandler(duck, backend, logger, 0, 0)
	qh.SetAuthAndRBAC(am, rm)
	qh.RegisterRoutes(app)
	lp := NewLineProtocolHandler(buffer, logger)
	lp.SetAuthAndRBAC(am, rm)
	lp.RegisterRoutes(app)
	mp := NewMsgPackHandler(logger, buffer, 10<<20)
	mp.SetAuthAndRBAC(am, rm)
	mp.RegisterRoutes(app)
	tle := NewTLEHandler(buffer, logger)
	tle.SetAuthAndRBAC(am, rm)
	tle.RegisterRoutes(app)
	dbs := NewDatabasesHandler(backend, &config.DeleteConfig{}, am, logger)
	dbs.SetRBACManager(rm)
	dbs.RegisterRoutes(app)
	return &doorRig{app: app, tokens: tokens, am: am, qh: qh, dbs: dbs}
}

func (r *doorRig) do(t *testing.T, token string, req *http.Request) (int, string) {
	t.Helper()
	req.Header.Set("Authorization", "Bearer "+r.tokens[token])
	resp, err := r.app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (r *doorRig) query(t *testing.T, token, path, sql string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(QueryRequest{SQL: sql})
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return r.do(t, token, req)
}

func TestRBACOnlyTokenCanQuery(t *testing.T) {
	r := newDoorRig(t)
	for _, c := range []struct {
		token, path, sql string
		status           int
		doorRefusal      bool
	}{
		// The issue's reproduction, on both query routes sharing the door.
		{"rbac-only", "/api/v1/query", "SELECT 1", 200, false},
		{"rbac-only", "/api/v1/query/msgpack", "SELECT 1", 200, false},
		{"rbac-only", "/api/v1/query/arrow", "SELECT 1", 200, false},
		{"none", "/api/v1/query/arrow", "SELECT 1", 403, true},
		{"rbac-only", "/api/v1/query", "SELECT count(*) FROM tenanta.cpu", 200, false},
		{"rbac-meas-read", "/api/v1/query", "SELECT count(*) FROM tenanta.cpu", 200, false},
		// Past the door, the handler still scopes it.
		{"rbac-only", "/api/v1/query", "SELECT count(*) FROM tenantb.cpu", 403, false},
		// Refused at the door.
		{"none", "/api/v1/query", "SELECT 1", 403, true},
		{"coarse-team-no-grant", "/api/v1/query", "SELECT 1", 403, true},
		// OSS model unchanged.
		{"coarse-no-team", "/api/v1/query", "SELECT count(*) FROM tenantb.cpu", 200, false},
	} {
		t.Run(fmt.Sprintf("%s %s %s", c.token, c.path, c.sql), func(t *testing.T) {
			status, body := r.query(t, c.token, c.path, c.sql)
			if status != c.status {
				t.Fatalf("status %d, want %d: %s", status, c.status, body)
			}
			if got := strings.Contains(body, doorDenied("read")); got != c.doorRefusal {
				t.Fatalf("refused at the door = %v, want %v: %s", got, c.doorRefusal, body)
			}
		})
	}
}

// DESCRIBE and SUMMARIZE name a table without FROM, so the reference
// extractor finds none; nothing rewrites db.m for them either, so DuckDB
// answers from its own (empty) catalog. An RBAC-only token past the door
// must get no data from another tenant's table this way.
func TestRBACOnlyTokenGetsNoDataFromDescribeOrSummarize(t *testing.T) {
	r := newDoorRig(t)
	for _, sql := range []string{"DESCRIBE tenantb.cpu", "SUMMARIZE tenantb.cpu"} {
		status, body := r.query(t, "rbac-only", "/api/v1/query", sql)
		if status == 200 || strings.Contains(body, `"value"`) {
			t.Fatalf("%s: status %d, %s; want a refusal with no column data", sql, status, body)
		}
	}
}

func TestRBACOnlyTokenCanIngest(t *testing.T) {
	r := newDoorRig(t)
	lp := func(path, db string) *http.Request {
		target := path
		switch path {
		case "/write":
			target += "?db=" + db
		case "/api/v2/write":
			target += "?bucket=" + db
		}
		req := httptest.NewRequest(http.MethodPost, target, strings.NewReader("cpu value=1\n"))
		if path == "/api/v1/write/line-protocol" {
			req.Header.Set("x-arc-database", db)
		}
		return req
	}
	mp := func(db string) *http.Request {
		b, err := msgpack.Marshal([]interface{}{map[string]interface{}{
			"m": "cpu", "t": time.Now().UnixMilli(), "fields": map[string]interface{}{"value": 1.0},
		}})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/msgpack")
		req.Header.Set("x-arc-database", db)
		return req
	}
	rw := func(db string) *http.Request {
		body := remoteWriteTestBody(remoteWriteTestRequest(remoteWriteTestSeries(
			[]remoteWriteTestLabel{{name: "__name__", value: "cpu"}, {name: "instance", value: "host-01:9100"}},
			[]remoteWriteTestSample{{value: 1, time: time.Now().UnixMilli()}},
			false, false,
		)))
		req := httptest.NewRequest(http.MethodPost, prometheusRemoteWritePath+"?db="+db, bytes.NewReader(body))
		for name, value := range remoteWriteTestHeaders() {
			if name != "x-arc-database" {
				req.Header.Set(name, value)
			}
		}
		return req
	}
	type tc struct {
		name, token string
		req         func() *http.Request
		ok          bool // 2xx
		doorRefusal bool
	}
	var cases []tc
	for _, path := range []string{"/write", "/api/v2/write", "/api/v1/write/line-protocol"} {
		path := path
		cases = append(cases,
			tc{path + " granted db", "rbac-only", func() *http.Request { return lp(path, "tenanta") }, true, false},
			tc{path + " other db", "rbac-only", func() *http.Request { return lp(path, "tenantb") }, false, false},
			tc{path + " no grants", "none", func() *http.Request { return lp(path, "tenanta") }, false, true},
			tc{path + " read-only measurement grant", "rbac-meas-read", func() *http.Request { return lp(path, "tenanta") }, false, true},
			tc{path + " coarse write, team grants none", "coarse-team-no-grant", func() *http.Request { return lp(path, "tenanta") }, false, true},
			tc{path + " coarse write, no team", "coarse-no-team", func() *http.Request { return lp(path, "tenantb") }, true, false},
		)
	}
	cases = append(cases,
		tc{"msgpack granted db", "rbac-only", func() *http.Request { return mp("tenanta") }, true, false},
		tc{"msgpack other db", "rbac-only", func() *http.Request { return mp("tenantb") }, false, false},
		tc{"msgpack no grants", "none", func() *http.Request { return mp("tenanta") }, false, true},
		tc{"remote write granted db", "rbac-only", func() *http.Request { return rw("tenanta") }, true, false},
		tc{"remote write other db", "rbac-only", func() *http.Request { return rw("tenantb") }, false, false},
		tc{"remote write no grants", "none", func() *http.Request { return rw("tenanta") }, false, true},
		tc{"remote write coarse write, team grants none", "coarse-team-no-grant", func() *http.Request { return rw("tenanta") }, false, true},
	)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, body := r.do(t, c.token, c.req())
			if ok := status >= 200 && status < 300; ok != c.ok {
				t.Fatalf("status %d, want 2xx = %v: %s", status, c.ok, body)
			}
			if got := strings.Contains(body, doorDenied("write")); got != c.doorRefusal {
				t.Fatalf("refused at the door = %v, want %v (status %d): %s", got, c.doorRefusal, status, body)
			}
		})
	}
}

// TLE's door: the body is not a valid TLE set, so a token past the door gets
// the handler's answer, never the door's.
func TestRBACOnlyTokenPassesTheTLEDoor(t *testing.T) {
	r := newDoorRig(t)
	tle := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/tle", strings.NewReader("not a tle\n"))
		req.Header.Set("x-arc-database", "tenanta")
		return req
	}
	if _, body := r.do(t, "rbac-only", tle()); strings.Contains(body, doorDenied("write")) {
		t.Fatalf("RBAC-only token refused at the TLE door: %s", body)
	}
	if status, body := r.do(t, "none", tle()); status != 403 || !strings.Contains(body, doorDenied("write")) {
		t.Fatalf("token with no grants: status %d, %s; want the door's 403", status, body)
	}
}

type anyChecker struct {
	allow bool
	err   error
}

func (a anyChecker) HasPermissionAnywhere(*auth.TokenInfo, string) (bool, error) {
	return a.allow, a.err
}

// The door middleware on its own. Here, not in package auth: the first fiber
// response a test binary serves starts fasthttp's server-date goroutine, which
// calls time.Now every second and races auth's TestOSS_RBACWrites_StampUTC,
// which swaps time.Local.
func TestRequireAnyResourcePermission(t *testing.T) {
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	run := func(rm auth.AnyResourcePermissionChecker, info *auth.TokenInfo) int {
		t.Helper()
		app := fiber.New()
		app.Use(func(c *fiber.Ctx) error {
			if info != nil {
				c.Locals("token_info", info)
			}
			return c.Next()
		})
		app.Get("/", auth.RequireAnyResourcePermission(am, rm, "read"), func(c *fiber.Ctx) error { return c.SendStatus(204) })
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode
	}
	rbacOnly := &auth.TokenInfo{ID: 1, Enabled: true, Permissions: []string{}}
	coarse := &auth.TokenInfo{ID: 2, Enabled: true, Permissions: []string{"read"}}
	for _, c := range []struct {
		name string
		rm   auth.AnyResourcePermissionChecker
		info *auth.TokenInfo
		want int
	}{
		{"no token", anyChecker{allow: true}, nil, 401},
		{"checker allows an RBAC-only token", anyChecker{allow: true}, rbacOnly, 204},
		{"checker refuses, coarse bit not consulted", anyChecker{}, coarse, 403},
		// Grants unreadable: refused, as unavailable rather than denied, and
		// the coarse bit is not a fallback.
		{"grants unreadable", anyChecker{err: errors.New("store down")}, coarse, 503},
		{"no checker, coarse read", nil, coarse, 204},
		{"no checker, no coarse read", nil, rbacOnly, 403},
	} {
		if got := run(c.rm, c.info); got != c.want {
			t.Errorf("%s: status %d, want %d", c.name, got, c.want)
		}
	}
}
