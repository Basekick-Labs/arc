package api

import (
	"net/http"
	"testing"
)

// A trigger after shutdown began answers 503 and claims nothing (#1887); a
// trigger while a cycle runs keeps its 409.
func TestTriggerRefusedDuringShutdown1887(t *testing.T) {
	manager, app := cyclesRig(t)
	before := manager.RunningCycleID()
	manager.BeginShutdown()
	status, body := postJSON1884(t, app, "/api/v1/compaction/trigger")
	if status != http.StatusServiceUnavailable || body["stopping"] != true {
		t.Fatalf("status %d body %v, want 503 stopping", status, body)
	}
	if manager.RunningCycleID() != before || manager.IsCycleRunning() {
		t.Fatal("a refused trigger claimed a cycle")
	}
}
