package tiering

import (
	"context"
	"testing"
	"time"
)

// #1619: Set cached the request object, so a Get after a Set answered id 0
// and zero timestamps until a restart re-read the table. The cache and the
// caller's policy now carry the stored row.
func TestPolicyStore_SetReportsTheStoredRow(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	ctx := context.Background()

	days := 3
	policy := &DatabasePolicy{Database: "db1", HotMaxAgeDays: &days}
	if err := store.Set(ctx, policy); err != nil {
		t.Fatal(err)
	}
	if policy.ID == 0 || policy.CreatedAt.IsZero() || policy.UpdatedAt.IsZero() {
		t.Fatalf("Set left the caller's policy without the stored row: %+v", policy)
	}

	got, err := store.Get(ctx, "db1")
	if err != nil || got == nil {
		t.Fatalf("Get = %v, %v", got, err)
	}
	// What the table holds, read by a store with an empty cache.
	fresh, err := NewPolicyStore(store.db, store.defaults, store.logger)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := fresh.Get(ctx, "db1")
	if err != nil || stored == nil {
		t.Fatalf("fresh Get = %v, %v", stored, err)
	}
	for name, p := range map[string]*DatabasePolicy{"cached Get": got, "Set's policy": policy} {
		if p.ID != stored.ID || !p.CreatedAt.Equal(stored.CreatedAt) || !p.UpdatedAt.Equal(stored.UpdatedAt) {
			t.Errorf("%s = id %d created %v updated %v, stored id %d created %v updated %v",
				name, p.ID, p.CreatedAt, p.UpdatedAt, stored.ID, stored.CreatedAt, stored.UpdatedAt)
		}
	}

	// A second Set keeps the row: same id and created_at. CURRENT_TIMESTAMP
	// has one-second resolution, so updated_at may not move; it is not before
	// created_at.
	first := *policy
	again := &DatabasePolicy{Database: "db1", HotOnly: true}
	if err := store.Set(ctx, again); err != nil {
		t.Fatal(err)
	}
	if again.ID != first.ID || !again.CreatedAt.Equal(first.CreatedAt) || again.UpdatedAt.Before(again.CreatedAt) {
		t.Fatalf("second Set = id %d created %v updated %v; want id %d, created %v, updated not before created",
			again.ID, again.CreatedAt, again.UpdatedAt, first.ID, first.CreatedAt)
	}
	if got, _ := store.Get(ctx, "db1"); got == nil || got.ID != first.ID || !got.HotOnly {
		t.Fatalf("Get after the second Set = %+v", got)
	}
}

// The cache holds its own copy: a caller that reuses its policy after Set
// does not change what Get returns.
func TestPolicyStore_SetCachesACopy(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	ctx := context.Background()

	days := 3
	policy := &DatabasePolicy{Database: "db1", HotMaxAgeDays: &days}
	if err := store.Set(ctx, policy); err != nil {
		t.Fatal(err)
	}
	policy.HotOnly = true
	days = 30

	got, err := store.Get(ctx, "db1")
	if err != nil || got == nil {
		t.Fatalf("Get = %v, %v", got, err)
	}
	if got.HotOnly || got.HotMaxAgeDays == nil || *got.HotMaxAgeDays != 3 {
		t.Fatalf("Get = hot_only %v hot_max_age_days %v, want the policy as set (false, 3)", got.HotOnly, got.HotMaxAgeDays)
	}
}

// storedRowSource is a seeded cluster copy that holds one stored row, or
// none when row is nil.
type storedRowSource struct {
	row *DatabasePolicy
}

func (storedRowSource) Seeded() bool                               { return true }
func (storedRowSource) Collecting() bool                           { return false }
func (storedRowSource) List() []DatabasePolicy                     { return nil }
func (storedRowSource) Set(context.Context, *DatabasePolicy) error { return nil }
func (storedRowSource) Delete(context.Context, string) error       { return nil }
func (s storedRowSource) Get(database string) (*DatabasePolicy, bool) {
	if s.row == nil || s.row.Database != database {
		return nil, false
	}
	cp := *s.row
	return &cp, true
}

// On a cluster the stored row is the cluster's: Set reports its id and
// timestamps. A row the read does not find (a Delete landed meanwhile) leaves
// the policy as it was, and the write still succeeded.
func TestPolicyStore_SetInClusterReportsTheStoredRow(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	ctx := context.Background()

	created := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	updated := created.Add(time.Hour)
	store.SetClusterSource(storedRowSource{row: &DatabasePolicy{ID: 7, Database: "db1", CreatedAt: created, UpdatedAt: updated}})
	policy := &DatabasePolicy{Database: "db1", HotOnly: true}
	if err := store.Set(ctx, policy); err != nil {
		t.Fatal(err)
	}
	if policy.ID != 7 || !policy.CreatedAt.Equal(created) || !policy.UpdatedAt.Equal(updated) || !policy.HotOnly {
		t.Fatalf("Set on a cluster = %+v, want id 7 with the cluster's timestamps", policy)
	}

	store.SetClusterSource(storedRowSource{})
	missed := &DatabasePolicy{Database: "db2"}
	if err := store.Set(ctx, missed); err != nil {
		t.Fatalf("Set whose report read found nothing = %v, want success", err)
	}
	if missed.ID != 0 {
		t.Fatalf("Set whose report read found nothing = %+v, want it unchanged", missed)
	}
}

// Reporting the stored row must not fail a write that happened: RETURNING
// steps the upsert before its values are converted. A row whose timestamp
// does not convert (NULL here) is updated, Set succeeds, and Get serves the
// new policy rather than the one cached before.
func TestPolicyStore_SetSucceedsWhenTheStoredRowDoesNotConvert(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	ctx := context.Background()

	if err := store.Set(ctx, &DatabasePolicy{Database: "db1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE tiering_policies SET created_at = NULL WHERE database = 'db1'`); err != nil {
		t.Fatal(err)
	}
	if err := store.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatalf("Set = %v, want success: the row was written", err)
	}
	var stored bool
	if err := store.db.QueryRow(`SELECT hot_only FROM tiering_policies WHERE database = 'db1'`).Scan(&stored); err != nil || !stored {
		t.Fatalf("stored hot_only = %v, %v", stored, err)
	}
	if got, err := store.Get(ctx, "db1"); err != nil || got == nil || !got.HotOnly {
		t.Fatalf("Get = %+v, %v; want the policy as written", got, err)
	}
}

// A Set whose statement fails drops the cached entry, so Get does not keep
// serving it as if the write had not been tried.
func TestPolicyStore_FailedSetDropsTheCachedEntry(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	ctx := context.Background()

	if err := store.Set(ctx, &DatabasePolicy{Database: "db1"}); err != nil {
		t.Fatal(err)
	}
	_ = store.db.Close()
	if err := store.Set(ctx, &DatabasePolicy{Database: "db1", HotOnly: true}); err == nil {
		t.Fatal("Set on a closed database succeeded")
	}
	if got, err := store.Get(ctx, "db1"); err == nil {
		t.Fatalf("Get = %+v from the cache, want a read of the table", got)
	}
}

// A row the cluster mirrored into the table (Go time.Time timestamps, not
// CURRENT_TIMESTAMP) is reported the same way once the node writes locally
// again, as after a restore onto a standalone node.
func TestPolicyStore_SetOverAMirroredRow(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	ctx := context.Background()

	created := time.Date(2026, 10, 1, 8, 30, 0, 0, time.UTC)
	if err := store.MirrorClusterPolicies(ctx, []DatabasePolicy{{ID: 4, Database: "db1", CreatedAt: created, UpdatedAt: created}}); err != nil {
		t.Fatal(err)
	}
	policy := &DatabasePolicy{Database: "db1", HotOnly: true}
	if err := store.Set(ctx, policy); err != nil {
		t.Fatal(err)
	}
	if policy.ID == 0 || !policy.CreatedAt.Equal(created) || policy.UpdatedAt.IsZero() {
		t.Fatalf("Set over a mirrored row = %+v, want its id and created_at %v", policy, created)
	}
}
