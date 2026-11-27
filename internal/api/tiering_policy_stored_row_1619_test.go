package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

// #1619: the PUT answered the request object, id 0 and zero timestamps. It
// now answers the stored row.
func TestSetPolicyAnswersTheStoredRow(t *testing.T) {
	app, _ := newTieringPoliciesTestApp(t, 0, nil)

	var ids []int64
	for _, body := range []string{`{"hot_max_age_days":3}`, `{"hot_max_age_days":5}`} {
		request := httptest.NewRequest(http.MethodPut, "/api/v1/tiering/policies/db1", strings.NewReader(body))
		request.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
		response, err := app.Test(request, 1000)
		if err != nil {
			t.Fatalf("app.Test: %v", err)
		}
		var got struct {
			Policy struct {
				ID            int64     `json:"id"`
				HotMaxAgeDays *int      `json:"hot_max_age_days"`
				CreatedAt     time.Time `json:"created_at"`
				UpdatedAt     time.Time `json:"updated_at"`
			} `json:"policy"`
		}
		err = json.NewDecoder(response.Body).Decode(&got)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != fiber.StatusOK {
			t.Fatalf("PUT %s: status %d, decode %v", body, response.StatusCode, err)
		}
		if got.Policy.ID == 0 || got.Policy.CreatedAt.IsZero() || got.Policy.UpdatedAt.IsZero() {
			t.Fatalf("PUT %s answered %+v, want the stored id and timestamps", body, got.Policy)
		}
		ids = append(ids, got.Policy.ID)
	}
	if ids[0] != ids[1] {
		t.Fatalf("PUT ids = %v, want the same row updated", ids)
	}
}
