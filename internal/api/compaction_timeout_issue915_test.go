package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type manualDeadlineObservationIssue915 struct {
	database    string
	measurement string
	deadline    time.Time
	hasDeadline bool
}

type manualDeadlineTierIssue915 struct {
	compaction.Tier
	observations chan manualDeadlineObservationIssue915
}

func (manualDeadlineTierIssue915) GetTierName() string { return "hourly" }
func (manualDeadlineTierIssue915) IsEnabled() bool     { return true }

func (manualDeadlineTierIssue915) GetStats() map[string]interface{} {
	return map[string]interface{}{}
}

func (tier manualDeadlineTierIssue915) FindCandidates(
	ctx context.Context, database, measurement string,
) ([]compaction.Candidate, error) {
	deadline, ok := ctx.Deadline()

	tier.observations <- manualDeadlineObservationIssue915{
		database:    database,
		measurement: measurement,
		deadline:    deadline,
		hasDeadline: ok,
	}

	return nil, nil
}

func TestManualCompactionDeadlineAndScopeIssue915(t *testing.T) {
	backend, err := storage.NewLocalBackend(
		t.TempDir(), zerolog.Nop(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	const timeout = 90 * time.Minute

	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend,
		LockManager:    compaction.NewLockManager(),
		MaxConcurrent:  1,
		CycleTimeout:   timeout,
		TempDirectory:  t.TempDir(),
		Logger:         zerolog.Nop(),
	})

	// No real subprocess is necessary: the test inspects the exact
	// context delivered to candidate discovery.
	manager.ManifestManager = nil

	observations := make(
		chan manualDeadlineObservationIssue915, 2,
	)
	manager.Tiers = []compaction.Tier{
		manualDeadlineTierIssue915{
			observations: observations,
		},
	}

	handler := NewCompactionHandler(
		manager, nil, nil, nil, zerolog.Nop(),
	)

	app := fiber.New()
	handler.RegisterRoutes(app)

	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/compaction/trigger?database=db&measurement=cpu&tier=hourly",
		nil,
	)

	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf(
			"HTTP status = %d, want 200",
			response.StatusCode,
		)
	}

	var payload struct {
		Database    string `json:"database"`
		Measurement string `json:"measurement"`
	}

	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}

	if payload.Database != "db" || payload.Measurement != "cpu" {
		t.Fatalf("unexpected response scope: %+v", payload)
	}

	select {
	case observed := <-observations:
		if observed.database != "db" ||
			observed.measurement != "cpu" {
			t.Fatalf(
				"discovered %q/%q, want db/cpu",
				observed.database,
				observed.measurement,
			)
		}

		if !observed.hasDeadline {
			t.Fatal("manual cycle has no context deadline")
		}

		remaining := time.Until(observed.deadline)
		if remaining < 89*time.Minute ||
			remaining > timeout {
			t.Fatalf(
				"manual deadline = %s, want approximately %s",
				remaining, timeout,
			)
		}

	case <-time.After(10 * time.Second):
		t.Fatal("manual trigger never reached discovery")
	}

	// Wait for asynchronous completion before closing its backend.
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		stats := manager.Stats()

		outcome, ok := stats["last_cycle"].(map[string]interface{})
		if ok && outcome["status"] == "completed" {
			break
		}

		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("manual cycle did not complete: %#v", stats)
		}
	}

	select {
	case extra := <-observations:
		t.Fatalf(
			"unexpected additional measurement: %+v",
			extra,
		)
	default:
	}
}
