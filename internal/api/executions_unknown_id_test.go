package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

// The execution-history routes of continuous queries and retention policies
// answer 404 for an ID with nothing behind it, as GET /:id does, and list a
// definition that has not run yet as [] rather than null. Before, both answered
// 200 {"executions": null} for any ID, so a typo or a deleted definition looked
// like one that had simply never run.

// getExecutions returns the status and the raw "executions" value of a GET.
func getExecutions(t *testing.T, app *fiber.App, path string) (int, string) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest("GET", path, nil), -1)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Executions json.RawMessage `json:"executions"`
		Error      string          `json:"error"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("GET %s: %v (%s)", path, err, raw)
	}
	if resp.StatusCode != fiber.StatusOK {
		return resp.StatusCode, body.Error
	}
	return resp.StatusCode, string(body.Executions)
}

func TestCQExecutionsOfUnknownIDIsNotFound(t *testing.T) {
	for _, clustered := range []bool{false, true} {
		name := "standalone"
		if clustered {
			name = "cluster"
		}
		t.Run(name, func(t *testing.T) {
			r := newCQRig(t)
			if clustered {
				r.src.seedCQ()
			}
			r.app.Get("/cq/:id/executions", r.h.handleGetExecutions)
			if code, body := r.do(t, "POST", "/cq", cqBody); code != fiber.StatusCreated {
				t.Fatalf("create: %d %v", code, body)
			}

			if code, got := getExecutions(t, r.app, "/cq/1/executions"); code != fiber.StatusOK || got != "[]" {
				t.Errorf("a CQ that has not run: %d %s, want 200 []", code, got)
			}
			if code, got := getExecutions(t, r.app, "/cq/9/executions"); code != fiber.StatusNotFound || got != "Continuous query not found" {
				t.Errorf("an unknown ID: %d %q, want 404", code, got)
			}

			if code, _ := r.do(t, "POST", "/cq/1/execute", `{}`); code != fiber.StatusOK {
				t.Fatalf("execute: %d", code)
			}
			var runs []CQExecution
			code, got := getExecutions(t, r.app, "/cq/1/executions")
			if code != fiber.StatusOK || json.Unmarshal([]byte(got), &runs) != nil || len(runs) != 1 || runs[0].QueryID != 1 {
				t.Errorf("after one run: %d %s, want that run", code, got)
			}

			if code, _ := r.do(t, "DELETE", "/cq/1", ""); code != fiber.StatusOK {
				t.Fatalf("delete: %d", code)
			}
			if code, got := getExecutions(t, r.app, "/cq/1/executions"); code != fiber.StatusNotFound {
				t.Errorf("a deleted CQ: %d %s, want 404", code, got)
			}
		})
	}
}

func TestRetentionExecutionsOfUnknownIDIsNotFound(t *testing.T) {
	for _, clustered := range []bool{false, true} {
		name := "standalone"
		if clustered {
			name = "cluster"
		}
		t.Run(name, func(t *testing.T) {
			r := newClusterRetentionRig(t)
			// A local policy takes the table's first ID; the cluster's starts
			// at the floor its seed set.
			pid := int64(1)
			if clustered {
				r.src.seed(false, 50)
				pid = 50
			}
			id := strconv.FormatInt(pid, 10)
			r.app.Get("/retention/:id/executions", r.h.handleGetExecutions)
			code, body := r.do(t, "POST", "/retention", `{"name":"p","database":"db","retention_days":30}`)
			if code != fiber.StatusCreated || body["id"].(float64) != float64(pid) {
				t.Fatalf("create: %d %v, want ID %d", code, body, pid)
			}

			if code, got := getExecutions(t, r.app, "/retention/"+id+"/executions"); code != fiber.StatusOK || got != "[]" {
				t.Errorf("a policy that has not run: %d %s, want 200 []", code, got)
			}
			if code, got := getExecutions(t, r.app, "/retention/7/executions"); code != fiber.StatusNotFound || got != "Retention policy not found" {
				t.Errorf("an unknown ID: %d %q, want 404", code, got)
			}

			// The history is this node's own, for the cluster's policy too.
			if r.h.recordExecutionStart(pid, time.Now()) == 0 {
				t.Fatal("execution not recorded")
			}
			var runs []RetentionExecution
			code, got := getExecutions(t, r.app, "/retention/"+id+"/executions")
			if code != fiber.StatusOK || json.Unmarshal([]byte(got), &runs) != nil || len(runs) != 1 || runs[0].PolicyID != pid {
				t.Errorf("after one run: %d %s, want that run", code, got)
			}

			if code, _ := r.do(t, "DELETE", "/retention/"+id, ""); code != fiber.StatusOK {
				t.Fatalf("delete: %d", code)
			}
			if code, got := getExecutions(t, r.app, "/retention/"+id+"/executions"); code != fiber.StatusNotFound {
				t.Errorf("a deleted policy: %d %s, want 404", code, got)
			}
		})
	}
}
