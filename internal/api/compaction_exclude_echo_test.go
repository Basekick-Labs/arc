package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// The trigger response echoes compaction.exclude_databases on UNSCOPED
// triggers only: a scoped trigger bypasses the list, so echoing it there
// would imply a filter that does not apply. The manager has no tiers, so
// the async cycle each trigger starts returns before touching storage.
//
// Each trigger gets its own manager (#1126). The two assertions are
// independent, and sharing one manager made the second trigger depend on the
// first one's background cycle having released its claim; the test then had
// to poll IsCycleRunning() between them, which is exactly the shape that was
// flaky before #1161 made the handler claim the cycle synchronously. With no
// previous cycle there is nothing to wait for.
func TestCompactionTriggerEchoesExclusions(t *testing.T) {
	trigger := func(path string) map[string]interface{} {
		t.Helper()
		manager := compaction.NewManager(&compaction.ManagerConfig{
			ExcludeDatabases: []string{"staging"},
			CycleTimeout:     time.Minute,
			Logger:           zerolog.Nop(),
		})
		handler := NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop())
		app := fiber.New()
		handler.RegisterRoutes(app)

		req := httptest.NewRequest(http.MethodPost, path, nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]interface{}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	unscoped := trigger("/api/v1/compaction/trigger")
	echoed, ok := unscoped["exclude_databases"].([]interface{})
	if !ok || len(echoed) != 1 || echoed[0] != "staging" {
		t.Fatalf(
			"unscoped echo = %v, want [staging]",
			unscoped["exclude_databases"],
		)
	}

	scoped := trigger("/api/v1/compaction/trigger?database=staging")
	if _, present := scoped["exclude_databases"]; present {
		t.Fatalf("scoped response leaked exclude_databases: %v", scoped)
	}
}
