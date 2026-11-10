package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/rs/zerolog"
)

// #1551: /health carries the counts of cold rows whose object is gone, and no
// paths: it is unauthenticated.
func TestHealthColdMissingObjectsField(t *testing.T) {
	get := func(s *Server) map[string]any {
		t.Helper()
		s.RegisterRoutes()
		resp, err := s.app.Test(httptest.NewRequest("GET", "/health", nil), 2000)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return body
	}

	s := NewServer(DefaultServerConfig(), zerolog.Nop())
	if _, ok := get(s)["tiering"]; ok {
		t.Fatal("tiering field present without tiering wired")
	}

	s = NewServer(DefaultServerConfig(), zerolog.Nop())
	s.SetColdMissingObjects(func() *tiering.ColdMissingObjectsStatus { return nil })
	if _, ok := get(s)["tiering"]; ok {
		t.Fatal("tiering field present before any check")
	}

	s = NewServer(DefaultServerConfig(), zerolog.Nop())
	s.SetColdMissingObjects(func() *tiering.ColdMissingObjectsStatus {
		return &tiering.ColdMissingObjectsStatus{Lost: 2, HotCopy: 1, Sample: []string{"db/cpu/x.parquet"}, CheckedAt: time.Now()}
	})
	tier := get(s)["tiering"].(map[string]any)
	if tier["cold_rows_missing_objects"] != float64(2) || tier["cold_rows_missing_objects_hot_copy"] != float64(1) || tier["cold_missing_checked_at"] == nil {
		t.Fatalf("tiering = %v", tier)
	}
	if len(tier) != 3 {
		t.Fatalf("tiering = %v: only counts and the check time belong on /health", tier)
	}
}
