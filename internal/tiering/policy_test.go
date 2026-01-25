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
