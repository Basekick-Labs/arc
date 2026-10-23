package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Through the route: the retried DELETE removes the leftover, whether or not
// it has anything else to rewrite.
func TestRetriedDeleteThroughTheRoute(t *testing.T) {
	for _, alsoMatching := range []bool{false, true} {
		r := newLeftoverRig(t)
		h := NewDeleteHandler(r.db, r.backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
		r.h = h
		app := fiber.New()
		h.RegisterRoutes(app)
		del := func(where string) (int, DeleteResponse) {
			t.Helper()
			req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(`{"database":"db","measurement":"cpu","where":"`+where+`","confirm":true}`))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req, 20000)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(resp.Body)
			var out DeleteResponse
			_ = json.Unmarshal(raw, &out)
			return resp.StatusCode, out
		}
		old := leftoverHour + "cpu_20261007_204851_558252000.parquet"
		r.parquet(old, "h0", "h1", "h2")
		failReplacedDeletes(t, old)
		if code, _ := del("host = 'h0'"); code == 200 {
			t.Fatal("the DELETE whose old file stayed answered 200")
		}
		deleteReplaced = storage.DeleteWithRetry
		where := "host = 'h0'"
		if alsoMatching {
			// Another file matches too: the leftover goes under the lock,
			// before the rewrites.
			r.parquet(leftoverHour+"cpu_20261007_205000_000000000.parquet", "h0", "h5")
		}
		code, out := del(where)
		if code != 200 {
			t.Fatalf("retry: %d %+v", code, out)
		}
		want := int64(0)
		if alsoMatching {
			want = 1
		}
		if out.DeletedCount != want {
			t.Fatalf("retry deleted %d rows, want %d (the leftover's rows are not counted)", out.DeletedCount, want)
		}
		if r.exists(old) {
			t.Fatalf("alsoMatching=%v: the leftover is still in storage: %v", alsoMatching, r.files())
		}
		if got := r.retired(); len(got) != 0 {
			t.Fatalf("journal %v", got)
		}
		// A dry run never removes anything.
		r.parquet(old, "h0", "h1", "h2")
		if err := r.h.journalRetired(t.Context(), old, old+"x"); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(`{"database":"db","measurement":"cpu","where":"host = 'h0'","dry_run":true}`))
		req.Header.Set("Content-Type", "application/json")
		if _, err := app.Test(req, 20000); err != nil {
			t.Fatal(err)
		}
		if !r.exists(old) {
			t.Fatal("a dry run removed a leftover")
		}
	}
}

// Through the route: a DELETE that reaches a file the tiering migrator is
// moving answers 207 naming why (a retry cannot reach it once it is in cold
// storage), one whose cold copy could not be checked 503, and the other
// file's rows are deleted either way.
func TestDeleteOfAMovingFileThroughTheRoute(t *testing.T) {
	for _, tc := range []struct {
		name     string
		claims   func(path string) *fakeMoveClaims
		code     int
		contains string
	}{
		{"moving", func(p string) *fakeMoveClaims { return &fakeMoveClaims{moving: map[string]bool{p: true}} }, 207, "being moved to cold storage"},
		{"unchecked", func(p string) *fakeMoveClaims { return &fakeMoveClaims{unchecked: map[string]bool{p: true}} }, 503, "cold storage could not be checked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newLeftoverRig(t)
			h := NewDeleteHandler(r.db, r.backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
			r.h = h
			moving := leftoverHour + "cpu_20261007_204851_558252000.parquet"
			other := leftoverHour + "cpu_20261007_205000_000000000.parquet"
			r.parquet(moving, "h0", "h1")
			r.parquet(other, "h0", "h5")
			h.moveClaims = tc.claims(moving)
			app := fiber.New()
			h.RegisterRoutes(app)

			req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req, 20000)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(resp.Body)
			var out DeleteResponse
			_ = json.Unmarshal(raw, &out)
			if resp.StatusCode != tc.code || !strings.Contains(out.Error, tc.contains) || strings.Contains(out.Error, "cluster manifest") {
				t.Fatalf("got %d %q, want %d naming %q", resp.StatusCode, out.Error, tc.code, tc.contains)
			}
			if out.DeletedCount != 1 {
				t.Fatalf("deleted %d rows, want 1 (the other file)", out.DeletedCount)
			}
			if !r.exists(moving) {
				t.Fatal("the moving file was touched")
			}
		})
	}
}
