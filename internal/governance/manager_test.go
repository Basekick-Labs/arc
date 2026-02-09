package governance

import (
	"context"
	"database/sql"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func setupTestManager(t *testing.T, cfg *config.GovernanceConfig) *Manager {
	t.Helper()

	// Initialize metrics singleton (required by manager)
	metrics.Init(zerolog.Nop())

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if cfg == nil {
		cfg = &config.GovernanceConfig{}
	}

	m, err := NewManager(&ManagerConfig{
		DB:     db,
		Config: cfg,
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	t.Cleanup(func() { m.Stop() })

	return m
}

func TestManager_CreateAndGetPolicy(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	policy, err := m.CreatePolicy(ctx, &Policy{
		TokenID:            1,
		RateLimitPerMinute: 60,
		RateLimitPerHour:   1000,
		MaxQueriesPerHour:  500,
		MaxQueriesPerDay:   5000,
		MaxRowsPerQuery:    100000,
		MaxScanDurationSec: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if policy.ID == 0 {
		t.Fatal("expected non-zero policy ID")
	}
	if policy.TokenID != 1 {
		t.Fatalf("expected TokenID=1, got %d", policy.TokenID)
	}

	// Get it back
	got, err := m.GetPolicy(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected policy, got nil")
	}
	if got.RateLimitPerMinute != 60 {
		t.Fatalf("expected RateLimitPerMinute=60, got %d", got.RateLimitPerMinute)
	}
	if got.MaxRowsPerQuery != 100000 {
		t.Fatalf("expected MaxRowsPerQuery=100000, got %d", got.MaxRowsPerQuery)
	}
}

func TestManager_GetPolicy_NotFound(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	got, err := m.GetPolicy(ctx, 999)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("expected nil for non-existent policy")
	}
}

func TestManager_UpdatePolicy(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	m.CreatePolicy(ctx, &Policy{
		TokenID:            1,
		RateLimitPerMinute: 10,
	})

	updated, err := m.UpdatePolicy(ctx, &Policy{
		TokenID:            1,
		RateLimitPerMinute: 100,
		MaxRowsPerQuery:    50000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.RateLimitPerMinute != 100 {
		t.Fatalf("expected updated RateLimitPerMinute=100, got %d", updated.RateLimitPerMinute)
	}

	got, _ := m.GetPolicy(ctx, 1)
	if got.RateLimitPerMinute != 100 {
		t.Fatalf("expected persisted RateLimitPerMinute=100, got %d", got.RateLimitPerMinute)
	}
}

func TestManager_DeletePolicy(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	m.CreatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 10})

	if err := m.DeletePolicy(ctx, 1); err != nil {
		t.Fatal(err)
	}

	got, _ := m.GetPolicy(ctx, 1)
	if got != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestManager_ListPolicies(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	m.CreatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 10})
	m.CreatePolicy(ctx, &Policy{TokenID: 2, RateLimitPerMinute: 20})

	policies, err := m.ListPolicies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(policies))
	}
}

func TestManager_CheckRateLimit_NoPolicy(t *testing.T) {
	m := setupTestManager(t, nil)

	result := m.CheckRateLimit(999)
	if !result.Allowed {
		t.Fatal("expected Allowed=true with no policy and no defaults")
	}
}

func TestManager_CheckRateLimit_WithPolicy(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	m.CreatePolicy(ctx, &Policy{
		TokenID:            1,
		RateLimitPerMinute: 3,
	})

	for i := 0; i < 3; i++ {
		result := m.CheckRateLimit(1)
		if !result.Allowed {
			t.Fatalf("expected Allowed=true on request %d", i+1)
		}
	}

	result := m.CheckRateLimit(1)
	if result.Allowed {
		t.Fatal("expected Allowed=false after exceeding rate limit")
	}
	if result.RetryAfterSec < 1 {
		t.Fatalf("expected RetryAfterSec >= 1, got %d", result.RetryAfterSec)
	}
}

func TestManager_CheckRateLimit_WithDefaults(t *testing.T) {
	m := setupTestManager(t, &config.GovernanceConfig{
		DefaultRateLimitPerMin: 2,
	})

	// No per-token policy — should use config defaults
	m.CheckRateLimit(1)
	m.CheckRateLimit(1)

	result := m.CheckRateLimit(1)
	if result.Allowed {
		t.Fatal("expected Allowed=false after exceeding default rate limit")
	}
}

func TestManager_CheckQuota_NoPolicy(t *testing.T) {
	m := setupTestManager(t, nil)

	result := m.CheckQuota(999)
	if !result.Allowed {
		t.Fatal("expected Allowed=true with no policy and no defaults")
	}
}

func TestManager_CheckQuota_WithPolicy(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	m.CreatePolicy(ctx, &Policy{
		TokenID:           1,
		MaxQueriesPerHour: 3,
		MaxRowsPerQuery:   50000,
		MaxScanDurationSec: 10,
	})

	for i := 0; i < 3; i++ {
		result := m.CheckQuota(1)
		if !result.Allowed {
			t.Fatalf("expected Allowed=true on query %d", i+1)
		}
	}

	result := m.CheckQuota(1)
	if result.Allowed {
		t.Fatal("expected Allowed=false after exceeding hourly quota")
	}
}

func TestManager_CheckQuota_ReturnsMaxRows(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	m.CreatePolicy(ctx, &Policy{
		TokenID:         1,
		MaxRowsPerQuery: 50000,
	})

	result := m.CheckQuota(1)
	if !result.Allowed {
		t.Fatal("expected Allowed=true")
	}
	if result.MaxRows != 50000 {
		t.Fatalf("expected MaxRows=50000, got %d", result.MaxRows)
	}
}

func TestManager_CheckQuota_ReturnsMaxDuration(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	m.CreatePolicy(ctx, &Policy{
		TokenID:            1,
		MaxScanDurationSec: 15,
	})

	result := m.CheckQuota(1)
	if !result.Allowed {
		t.Fatal("expected Allowed=true")
	}
	if result.MaxDuration.Seconds() != 15 {
		t.Fatalf("expected MaxDuration=15s, got %v", result.MaxDuration)
	}
}

func TestManager_GetTokenUsage(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	m.CreatePolicy(ctx, &Policy{
		TokenID:            1,
		RateLimitPerMinute: 60,
		RateLimitPerHour:   1000,
		MaxQueriesPerHour:  100,
		MaxQueriesPerDay:   1000,
	})

	// Make some requests to populate usage
	m.CheckRateLimit(1)
	m.CheckRateLimit(1)
	m.CheckQuota(1)
	m.CheckQuota(1)
	m.CheckQuota(1)

	usage := m.GetTokenUsage(1)
	if usage.TokenID != 1 {
		t.Fatalf("expected TokenID=1, got %d", usage.TokenID)
	}
	if usage.QueriesThisHour != 3 {
		t.Fatalf("expected QueriesThisHour=3, got %d", usage.QueriesThisHour)
	}
	if usage.RateLimitRemainingPerMin != 58 { // 60 - 2
		t.Fatalf("expected RateLimitRemainingPerMin=58, got %d", usage.RateLimitRemainingPerMin)
	}
}

func TestManager_DuplicateTokenID(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()

	_, err := m.CreatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 10})
	if err != nil {
		t.Fatal(err)
	}

	_, err = m.CreatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 20})
	if err == nil {
		t.Fatal("expected error on duplicate token_id")
	}
}

func TestManager_NilDB(t *testing.T) {
	metrics.Init(zerolog.Nop())
	_, err := NewManager(&ManagerConfig{
		DB:     nil,
		Config: &config.GovernanceConfig{},
		Logger: zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error with nil DB")
	}
}

func TestManager_PolicyPersistsAfterReload(t *testing.T) {
	metrics.Init(zerolog.Nop())

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := &config.GovernanceConfig{}

	// Create first manager and add a policy
	m1, err := NewManager(&ManagerConfig{DB: db, Config: cfg, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m1.CreatePolicy(ctx, &Policy{TokenID: 42, RateLimitPerMinute: 99})
	m1.Stop()

	// Create second manager using same DB — policy should be loaded from SQLite
	m2, err := NewManager(&ManagerConfig{DB: db, Config: cfg, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}

	got, _ := m2.GetPolicy(ctx, 42)
	if got == nil {
		t.Fatal("expected policy to persist after reload")
	}
	if got.RateLimitPerMinute != 99 {
		t.Fatalf("expected RateLimitPerMinute=99, got %d", got.RateLimitPerMinute)
	}
	m2.Stop()
}
