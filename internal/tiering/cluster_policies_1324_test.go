package tiering

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSource is a cluster copy of the policies whose state a test drives.
type fakeSource struct {
	seeded     atomic.Bool
	collecting func() bool
	mu         sync.Mutex
	sets       []string
}

func (f *fakeSource) Seeded() bool { return f.seeded.Load() }
func (f *fakeSource) Collecting() bool {
	if f.collecting != nil {
		return f.collecting()
	}
	return false
}
func (f *fakeSource) Get(string) (*DatabasePolicy, bool) { return nil, false }
func (f *fakeSource) List() []DatabasePolicy             { return nil }
func (f *fakeSource) Set(_ context.Context, p *DatabasePolicy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets = append(f.sets, p.Database)
	return nil
}
func (f *fakeSource) Delete(context.Context, string) error { return nil }

// #1324: a write that decided on the local table, then waited for the store's
// lock while the cluster seeded the policies, goes to the cluster: written to
// the local table it would be ignored from then on.
func TestPolicyStore_SeedWhileWaitingForTheLockGoesToTheCluster(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	src := &fakeSource{}
	// Collecting is consulted under the lock; the cluster seeds just before.
	src.collecting = func() bool { return false }
	store.SetClusterSource(src)

	store.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- store.Set(context.Background(), &DatabasePolicy{Database: "db1", HotOnly: true}) }()
	time.Sleep(50 * time.Millisecond) // the write passed the unlocked check and waits for the lock
	src.seeded.Store(true)
	store.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	src.mu.Lock()
	sets := append([]string(nil), src.sets...)
	src.mu.Unlock()
	if len(sets) != 1 || sets[0] != "db1" {
		t.Fatalf("cluster sets = %v, want the write sent to the cluster", sets)
	}
	store.SetClusterSource(nil)
	if p, _ := store.Get(context.Background(), "db1"); p != nil {
		t.Fatalf("the write also landed in the local table: %+v", p)
	}
}

// The read a collect takes holds the lock local writes take: a write in
// progress finishes first, so it is in the rows the cluster adopts.
func TestPolicyStore_LocalPoliciesWaitsForAWriteInProgress(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	src := &fakeSource{collecting: func() bool {
		// Called with the store's lock held, in the middle of the write.
		once.Do(func() { close(entered) })
		<-release
		return false
	}}
	store.SetClusterSource(src)
	setDone := make(chan error, 1)
	go func() { setDone <- store.Set(context.Background(), &DatabasePolicy{Database: "db1", HotOnly: true}) }()
	<-entered

	listed := make(chan []DatabasePolicy, 1)
	go func() {
		rows, _ := store.LocalPolicies(context.Background())
		listed <- rows
	}()
	select {
	case rows := <-listed:
		t.Fatalf("the collect read the table (%d rows) while a write held its lock", len(rows))
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-setDone; err != nil {
		t.Fatal(err)
	}
	if rows := <-listed; len(rows) != 1 || rows[0].Database != "db1" {
		t.Fatalf("collect read %+v, want the write that was in progress", rows)
	}
}
