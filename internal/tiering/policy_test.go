package tiering

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func setupTestPolicyStore(t *testing.T) (*PolicyStore, func()) {
	t.Helper()

	// Create temp file for SQLite
	tmpFile, err := os.CreateTemp("", "tiering_policy_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to open SQLite: %v", err)
	}

	// 2-tier system: only hot max age is needed
	defaults := &config.TieredStorageConfig{
		DefaultHotMaxAgeDays: 7,
	}

	logger := zerolog.Nop()
	store, err := NewPolicyStore(db, defaults, logger)
	if err != nil {
		db.Close()
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to create policy store: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(tmpFile.Name())
	}

	return store, cleanup
}

func TestPolicyStore_SetAndGet(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	// 2-tier system: only hot max age is needed
	hotDays := 3

	policy := &DatabasePolicy{
		Database:      "testdb",
		HotOnly:       false,
		HotMaxAgeDays: &hotDays,
	}

	// Set policy
	err := store.Set(ctx, policy)
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Get policy
	retrieved, err := store.Get(ctx, "testdb")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if retrieved == nil {
		t.Fatal("Get() returned nil")
	}

	if retrieved.Database != "testdb" {
		t.Errorf("Database = %v, want testdb", retrieved.Database)
	}
	if *retrieved.HotMaxAgeDays != 3 {
		t.Errorf("HotMaxAgeDays = %v, want 3", *retrieved.HotMaxAgeDays)
	}
}

func TestPolicyStore_HotOnly(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	policy := &DatabasePolicy{
		Database: "realtime",
		HotOnly:  true,
	}

	// Set policy
	err := store.Set(ctx, policy)
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Check IsHotOnly
	if !store.IsHotOnly(ctx, "realtime") {
		t.Error("IsHotOnly() = false, want true")
	}

	// Non-existent database should not be hot-only
	if store.IsHotOnly(ctx, "nonexistent") {
		t.Error("IsHotOnly() for nonexistent = true, want false")
	}
}

func TestPolicyStore_GetEffective(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	// Test 1: No custom policy - should use global defaults (2-tier system)
	effective := store.GetEffective(ctx, "db_without_policy")
	if effective.Source != "global" {
		t.Errorf("Source = %v, want global", effective.Source)
	}
	if effective.HotMaxAgeDays != 7 {
		t.Errorf("HotMaxAgeDays = %v, want 7 (global default)", effective.HotMaxAgeDays)
	}

	// Test 2: Custom policy with overrides
	hotDays := 3
	policy := &DatabasePolicy{
		Database:      "custom_db",
		HotMaxAgeDays: &hotDays,
	}
	err := store.Set(ctx, policy)
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	effective = store.GetEffective(ctx, "custom_db")
	if effective.Source != "custom" {
		t.Errorf("Source = %v, want custom", effective.Source)
	}
	if effective.HotMaxAgeDays != 3 {
		t.Errorf("HotMaxAgeDays = %v, want 3 (custom)", effective.HotMaxAgeDays)
	}
}

func TestPolicyStore_Delete(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	hotDays := 3
	policy := &DatabasePolicy{
		Database:      "testdb",
		HotMaxAgeDays: &hotDays,
	}

	// Set policy
	err := store.Set(ctx, policy)
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Delete policy
	err = store.Delete(ctx, "testdb")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Get should return nil
	retrieved, err := store.Get(ctx, "testdb")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if retrieved != nil {
		t.Error("Get() should return nil after delete")
	}

	// Effective policy should now be global
	effective := store.GetEffective(ctx, "testdb")
	if effective.Source != "global" {
		t.Errorf("Source = %v, want global after delete", effective.Source)
	}
}

func TestPolicyStore_List(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	// Add multiple policies
	policies := []DatabasePolicy{
		{Database: "db1", HotOnly: true},
		{Database: "db2", HotOnly: false},
		{Database: "db3", HotOnly: false},
	}

	for _, p := range policies {
		policy := p
		err := store.Set(ctx, &policy)
		if err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}

	// List policies
	listed, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(listed) != 3 {
		t.Errorf("List() returned %d policies, want 3", len(listed))
	}
}

func TestPolicyStore_Update(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	hotDays := 3
	policy := &DatabasePolicy{
		Database:      "testdb",
		HotOnly:       false,
		HotMaxAgeDays: &hotDays,
	}

	// Set initial policy
	err := store.Set(ctx, policy)
	if err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Update policy
	newHotDays := 5
	policy.HotMaxAgeDays = &newHotDays
	policy.HotOnly = true

	err = store.Set(ctx, policy)
	if err != nil {
		t.Fatalf("Set() (update) error = %v", err)
	}

	// Verify update
	retrieved, err := store.Get(ctx, "testdb")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if *retrieved.HotMaxAgeDays != 5 {
		t.Errorf("HotMaxAgeDays = %v, want 5", *retrieved.HotMaxAgeDays)
	}
	if !retrieved.HotOnly {
		t.Error("HotOnly = false, want true")
	}
}

// TestPolicyStore_NegativeLookupCached verifies that a not-found lookup is
// cached (#345). The proof is behavioral: after the first Get caches the
// negative, a row inserted directly via SQL (bypassing Set, so the cache
// never learns) must stay invisible to subsequent Gets.
func TestPolicyStore_NegativeLookupCached(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	policy, err := store.Get(ctx, "nocustom")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if policy != nil {
		t.Fatalf("Get() for nonexistent policy = %+v, want nil", policy)
	}

	// Insert behind the store's back. If the negative lookup was cached,
	// the store must keep returning nil; if it re-queries SQLite, this row
	// leaks through and the test fails.
	_, err = store.db.Exec(
		`INSERT INTO tiering_policies (database, hot_only, updated_at) VALUES (?, 1, CURRENT_TIMESTAMP)`,
		"nocustom",
	)
	if err != nil {
		t.Fatalf("direct insert failed: %v", err)
	}

	policy, err = store.Get(ctx, "nocustom")
	if err != nil {
		t.Fatalf("Get() after direct insert error = %v", err)
	}
	if policy != nil {
		t.Errorf("Get() after direct insert = %+v, want nil (negative lookup should be served from cache, not SQLite)", policy)
	}
}

// TestPolicyStore_SetOverridesCachedNegative verifies that Set invalidates a
// previously cached not-found entry (#345).
func TestPolicyStore_SetOverridesCachedNegative(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	policy, err := store.Get(ctx, "latecomer")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if policy != nil {
		t.Fatalf("Get() for nonexistent policy = %+v, want nil", policy)
	}

	hotDays := 3
	if err := store.Set(ctx, &DatabasePolicy{
		Database:      "latecomer",
		HotMaxAgeDays: &hotDays,
	}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	policy, err = store.Get(ctx, "latecomer")
	if err != nil {
		t.Fatalf("Get() after Set error = %v", err)
	}
	if policy == nil {
		t.Fatal("Get() after Set = nil, want policy (Set must overwrite the cached negative)")
	}
	if policy.HotMaxAgeDays == nil || *policy.HotMaxAgeDays != hotDays {
		t.Errorf("Get() after Set HotMaxAgeDays = %v, want %d", policy.HotMaxAgeDays, hotDays)
	}
}

// TestPolicyStore_ListSkipsCachedNegatives is the regression test for the
// review blocker on #345: List() dereferences every cache value, and a
// cached negative (nil) must not panic it or appear in the listing.
func TestPolicyStore_ListSkipsCachedNegatives(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	// Cache a negative, then a real policy.
	if policy, err := store.Get(ctx, "defaultsonly"); err != nil || policy != nil {
		t.Fatalf("Get() = (%+v, %v), want (nil, nil)", policy, err)
	}
	hotDays := 5
	if err := store.Set(ctx, &DatabasePolicy{Database: "custom", HotMaxAgeDays: &hotDays}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	policies, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("List() returned %d policies, want 1 (cached negative must be skipped)", len(policies))
	}
	if policies[0].Database != "custom" {
		t.Errorf("List()[0].Database = %q, want %q", policies[0].Database, "custom")
	}

	// Derived lookups must treat a cached negative as "use defaults".
	if store.IsHotOnly(ctx, "defaultsonly") {
		t.Error("IsHotOnly() after cached negative = true, want false")
	}
	effective := store.GetEffective(ctx, "defaultsonly")
	if effective == nil || effective.Source != "global" {
		t.Errorf("GetEffective() after cached negative = %+v, want global defaults", effective)
	}
}

// TestPolicyStore_CacheIfFreshSkipsAfterDelete is the deterministic
// regression test for #499. It replays the racing interleaving: a Get read
// the policy row from SQLite, then Delete removed the row and the cache
// entry before the Get committed its (now stale) result. The generation
// check must refuse the cache write — key-absence alone cannot distinguish
// "just deleted" from "never cached".
func TestPolicyStore_CacheIfFreshSkipsAfterDelete(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()

	hotDays := 3
	policy := &DatabasePolicy{Database: "racer", HotMaxAgeDays: &hotDays}
	if err := store.Set(ctx, policy); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// A concurrent Get snapshots gen, then reads the row from SQLite —
	// at this point the policy still exists.
	genAtRead := store.gen
	staleRead := policy

	// Delete wins the race: row and cache entry are gone, gen bumped.
	if err := store.Delete(ctx, "racer"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// The Get now tries to commit its stale read. The generation check
	// must reject it (returning the stale value uncached is fine — the
	// next Get re-queries).
	store.cacheIfFresh("racer", staleRead, genAtRead)

	got, err := store.Get(ctx, "racer")
	if err != nil {
		t.Fatalf("Get() after delete error = %v", err)
	}
	if got != nil {
		t.Errorf("Get() after Delete = %+v, want nil (stale positive must not be re-cached, #499)", got)
	}
}

// TestPolicyStore_ConcurrentGetSetDelete hammers the racing interleaving
// end-to-end under -race: after each Set+Delete pair completes, a Get must
// observe the deletion regardless of how an in-flight Get interleaved.
func TestPolicyStore_ConcurrentGetSetDelete(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()

	ctx := context.Background()
	hotDays := 3

	for i := 0; i < 200; i++ {
		if err := store.Set(ctx, &DatabasePolicy{Database: "churn", HotMaxAgeDays: &hotDays}); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
		// Evict so the racing Get takes the DB-read path.
		store.mu.Lock()
		delete(store.cache, "churn")
		store.mu.Unlock()

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = store.Get(ctx, "churn")
		}()

		if err := store.Delete(ctx, "churn"); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		<-done

		got, err := store.Get(ctx, "churn")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got != nil {
			t.Fatalf("iteration %d: Get() after Delete = %+v, want nil (stale positive re-cached, #499)", i, got)
		}
	}
}
