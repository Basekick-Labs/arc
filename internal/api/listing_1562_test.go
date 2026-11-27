package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1562: listing every database (GET /api/v1/databases, SHOW DATABASES, the
// unscoped GET /api/v1/measurements) used to require a read grant covering
// every database, so a tenant token could not discover its own. Each listing
// now shows the databases the caller can read something in.
//
// newDoorRig: real AuthManager, RBACManager and middleware; data in
// tenanta.cpu, tenanta.mem and tenantb.cpu. Tokens: rbac-only (role on
// tenanta), rbac-meas-read (tenanta.cpu only), coarse-no-team (no team, coarse
// read,write), none, coarse-team-no-grant (team granting only delete).

func (r *doorRig) databasesList(t *testing.T, token string) (int, DatabaseListResponse) {
	t.Helper()
	status, body := r.do(t, token, httptest.NewRequest(http.MethodGet, "/api/v1/databases", nil))
	var out DatabaseListResponse
	if status == 200 {
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
	}
	return status, out
}

func TestDatabasesListShowsTheCallersDatabases(t *testing.T) {
	r := newDoorRig(t)
	for _, c := range []struct {
		token  string
		status int
		want   map[string]int // database -> measurement count
	}{
		{"rbac-only", 200, map[string]int{"tenanta": 2}},
		// Counted by what the caller can read, as GET /databases/:name does.
		{"rbac-meas-read", 200, map[string]int{"tenanta": 1}},
		{"coarse-no-team", 200, map[string]int{"tenanta": 2, "tenantb": 1}},
		{"none", 403, nil},
		{"coarse-team-no-grant", 403, nil},
	} {
		t.Run(c.token, func(t *testing.T) {
			status, list := r.databasesList(t, c.token)
			if status != c.status {
				t.Fatalf("status %d, want %d", status, c.status)
			}
			if c.want == nil {
				return
			}
			got := map[string]int{}
			for _, d := range list.Databases {
				got[d.Name] = d.MeasurementCount
			}
			if len(got) != len(c.want) || list.Count != len(c.want) {
				t.Fatalf("databases %v (count %d), want %v", got, list.Count, c.want)
			}
			for db, n := range c.want {
				if got[db] != n {
					t.Fatalf("databases %v, want %v", got, c.want)
				}
			}
			// The list's count agrees with GET /databases/:name.
			for db, n := range c.want {
				status, body := r.do(t, c.token, httptest.NewRequest(http.MethodGet, "/api/v1/databases/"+db, nil))
				var one DatabaseInfo
				_ = json.Unmarshal([]byte(body), &one)
				if status != 200 || one.MeasurementCount != n {
					t.Fatalf("GET /databases/%s: %d %s; want count %d", db, status, body, n)
				}
			}
		})
	}
}

// SHOW DATABASES is answered by Arc, filtered, in every spelling, including a
// comment hiding the command from the SHOW match. If a form ever escaped the
// match, DuckDB would answer it from its own catalog, unfiltered, in a
// different shape; the exact columns and rows catch that.
func TestShowDatabasesCommentFormsAreFiltered(t *testing.T) {
	r := newDoorRig(t)
	forms := []string{
		"SHOW DATABASES",
		"/* sneaky */ SHOW DATABASES",
		"-- sneaky\nSHOW DATABASES",
		"SHOW DATABASES -- trailing",
	}
	for _, c := range []struct {
		token string
		want  []string
	}{
		{"rbac-only", []string{"tenanta"}},
		{"coarse-no-team", []string{"tenanta", "tenantb"}},
	} {
		for _, sql := range forms {
			status, body := r.query(t, c.token, "/api/v1/query", sql)
			var resp struct {
				Columns []string        `json:"columns"`
				Data    [][]interface{} `json:"data"`
			}
			if err := json.Unmarshal([]byte(body), &resp); err != nil || status != 200 {
				t.Fatalf("%s %q: %d %s", c.token, sql, status, body)
			}
			var got []string
			for _, row := range resp.Data {
				got = append(got, row[0].(string))
			}
			sort.Strings(got)
			if len(resp.Columns) != 1 || resp.Columns[0] != "database" || len(got) != len(c.want) {
				t.Fatalf("%s %q: columns %v rows %v; want Arc's [database] answer %v", c.token, sql, resp.Columns, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("%s %q: rows %v, want %v", c.token, sql, got, c.want)
				}
			}
		}
	}
}

func TestUnscopedMeasurementsListShowsTheCallersDatabases(t *testing.T) {
	r := newDoorRig(t)
	for _, c := range []struct {
		token string
		want  []string
	}{
		{"rbac-only", []string{"tenanta.cpu", "tenanta.mem"}},
		{"rbac-meas-read", []string{"tenanta.cpu"}},
		{"coarse-no-team", []string{"tenanta.cpu", "tenanta.mem", "tenantb.cpu"}},
	} {
		status, body := r.do(t, c.token, httptest.NewRequest(http.MethodGet, "/api/v1/measurements", nil))
		var resp struct {
			Measurements []MeasurementInfo `json:"measurements"`
		}
		if err := json.Unmarshal([]byte(body), &resp); err != nil || status != 200 {
			t.Fatalf("%s: %d %s", c.token, status, body)
		}
		var got []string
		for _, m := range resp.Measurements {
			got = append(got, m.Database+"."+m.Measurement)
		}
		sort.Strings(got)
		if len(got) != len(c.want) {
			t.Fatalf("%s: %v, want %v", c.token, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("%s: %v, want %v", c.token, got, c.want)
			}
		}
	}
}

// listingStoreDown answers every grant question as "store unreadable".
type listingStoreDown struct{ *mockRBACChecker }

func (listingStoreDown) CheckPermission(*auth.PermissionCheckRequest) *auth.PermissionCheckResult {
	return &auth.PermissionCheckResult{Allowed: false, Source: auth.SourceUnavailable}
}

// Grants that cannot be read: the listing says so, rather than answering an
// empty list that reads as "you can see nothing".
func TestDatabaseListingFilterReportsAnUnreadableStore(t *testing.T) {
	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{ID: 1, Enabled: true})
		visible, unavailable := databaseListingFilter(c, listingStoreDown{&mockRBACChecker{}})
		if !unavailable || visible != nil {
			t.Errorf("visible %v, unavailable %v; want unavailable", visible != nil, unavailable)
		}
		return c.SendStatus(204)
	})
	if _, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil)); err != nil {
		t.Fatal(err)
	}
}

// coldTiering is a database tiering manager whose metadata is a real store,
// so a cold-only database (a tier row, no hot file) reaches the listing.
type coldTiering struct {
	fakeDatabaseTieringManager
	store *tiering.MetadataStore
}

func (c *coldTiering) GetMetadata() *tiering.MetadataStore { return c.store }

// A database known only from tier metadata (cold-only) is filtered like any
// other, in both listings: a tenant token must not learn its name.
func TestColdOnlyDatabasesAreFiltered(t *testing.T) {
	r := newDoorRig(t)
	ctx := context.Background()

	// SHOW DATABASES: the query handler's cold-database list.
	r.qh.coldDatabases = func(context.Context) ([]string, error) { return []string{"coldhidden"}, nil }
	show := func(token string) []string {
		t.Helper()
		status, body := r.query(t, token, "/api/v1/query", "SHOW DATABASES")
		var resp struct {
			Data [][]interface{} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &resp); err != nil || status != 200 {
			t.Fatalf("%s: %d %s", token, status, body)
		}
		var got []string
		for _, row := range resp.Data {
			got = append(got, row[0].(string))
		}
		sort.Strings(got)
		return got
	}
	if got := show("rbac-only"); len(got) != 1 || got[0] != "tenanta" {
		t.Errorf("SHOW DATABASES, tenant token: %v, want [tenanta]", got)
	}
	if got := show("coarse-no-team"); len(got) != 3 || got[0] != "coldhidden" {
		t.Errorf("SHOW DATABASES, full access: %v, want coldhidden among 3", got)
	}

	// GET /api/v1/databases: a real tier-metadata row in a cold-only database.
	store, err := tiering.NewMetadataStore(r.am.GetDB(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordColdFile(ctx, &tiering.FileMetadata{
		Path: "coldhidden/cpu/2026/01/01/cpu_daily.parquet", Database: "coldhidden", Measurement: "cpu",
		PartitionTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Tier: tiering.TierCold, SizeBytes: 1,
		CreatedAt: time.Now(),
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	r.dbs.SetTieringManager(&coldTiering{store: store})
	for _, c := range []struct {
		token string
		want  bool
	}{{"rbac-only", false}, {"coarse-no-team", true}} {
		_, list := r.databasesList(t, c.token)
		found := false
		for _, d := range list.Databases {
			found = found || d.Name == "coldhidden"
		}
		if found != c.want {
			t.Errorf("GET /databases, %s: coldhidden listed = %v, want %v (%+v)", c.token, found, c.want, list.Databases)
		}
	}
}
