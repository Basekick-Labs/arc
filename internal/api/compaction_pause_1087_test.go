package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// The trigger endpoint answers 409 while compaction is paused cluster-wide
// (#1087) and starts no cycle; once the pause ends the same request runs.
func TestCompactionTriggerAnswers409WhilePausedIssue1087(t *testing.T) {
	manager := compaction.NewManager(&compaction.ManagerConfig{CycleTimeout: time.Minute, Logger: zerolog.Nop()})
	var paused atomic.Bool
	manager.SetPauseGate(paused.Load)
	handler := NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)

	trigger := func() (int, map[string]interface{}) {
		t.Helper()
		resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/api/v1/compaction/trigger", nil))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]interface{}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("body %s: %v", body, err)
		}
		return resp.StatusCode, out
	}

	paused.Store(true)
	status, body := trigger()
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409", status)
	}
	if body["error"] != "compaction is paused cluster-wide" || body["paused"] != true {
		t.Fatalf("body = %v", body)
	}
	time.Sleep(50 * time.Millisecond)
	if manager.GetCurrentCycleID() != 0 || manager.IsCycleRunning() {
		t.Fatal("a cycle was started under the pause")
	}

	paused.Store(false)
	if status, body := trigger(); status != http.StatusOK {
		t.Fatalf("status after the resume = %d (%v), want 200", status, body)
	}
}
