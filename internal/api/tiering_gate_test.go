package api

import (
	"testing"

	"github.com/gofiber/fiber/v2"
)

// A manual migration on a node the cluster gate excludes is a 409 that
// names the role, so an operator hitting a reader or standby writer learns
// where to retry instead of reading a 500.
func TestMigrationRoleGatedResponse(t *testing.T) {
	status, body := migrationRoleGatedResponse("reader")
	if status != fiber.StatusConflict {
		t.Fatalf("status = %d, want 409", status)
	}
	if body["role"] != "reader" {
		t.Fatalf("role = %v, want reader", body["role"])
	}
	for _, key := range []string{"error", "message"} {
		if s, _ := body[key].(string); s == "" {
			t.Fatalf("%s missing from the 409 body", key)
		}
	}
}
