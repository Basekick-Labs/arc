package scheduler

import (
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func sendCQRequest(t *testing.T, app *fiber.App, method, path, body string) int {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// Deleting a continuous query stops its job. The job used to keep ticking at
// the CQ's interval for the life of the process, every run failing on the
// missing row and logged at Error, and the scheduler status kept listing it.
func TestDeletingACQStopsItsJob(t *testing.T) {
	h, err := api.NewContinuousQueryHandler(nil, nil, nil,
		&config.ContinuousQueryConfig{Enabled: true, DBPath: filepath.Join(t.TempDir(), "cq.db")}, nil, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	s := runningCQScheduler(t)
	h.SetScheduler(s)
	app := fiber.New()
	h.RegisterRoutes(app)

	for _, name := range []string{"a", "b"} {
		body := fmt.Sprintf(`{"name":%q,"database":"db","source_measurement":"src","destination_measurement":"dst_%s",`+
			`"query":"SELECT * FROM src WHERE time >= {start_time} AND time < {end_time}","interval":"1h"}`, name, name)
		if status := sendCQRequest(t, app, "POST", "/api/v1/continuous_queries", body); status != fiber.StatusCreated {
			t.Fatalf("create %s: status %d", name, status)
		}
	}
	first, second := s.jobFor(1), s.jobFor(2)
	if first == nil || second == nil {
		t.Fatalf("jobs after create: %+v, %+v; want one per CQ", first, second)
	}

	if status := sendCQRequest(t, app, "DELETE", "/api/v1/continuous_queries/1", ""); status != fiber.StatusOK {
		t.Fatalf("delete: status %d", status)
	}
	if j := s.jobFor(1); j != nil {
		t.Fatalf("the deleted CQ still has a job: %+v", j)
	}
	select {
	case <-first.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the deleted CQ's job goroutine kept running")
	}
	if s.jobFor(2) != second {
		t.Fatal("deleting one CQ touched another CQ's job")
	}

	// An ID with no stored CQ is a 404 and stops nothing.
	if status := sendCQRequest(t, app, "DELETE", "/api/v1/continuous_queries/1", ""); status != fiber.StatusNotFound {
		t.Fatalf("second delete: status %d, want 404", status)
	}
	if s.JobCount() != 1 || s.jobFor(2) != second {
		t.Fatalf("%d jobs after a 404 delete; want only the other CQ's, unchanged", s.JobCount())
	}
}
