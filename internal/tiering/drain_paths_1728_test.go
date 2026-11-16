package tiering

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// The replicated tier drain reports paths that left this node's hot tier (an
// unlink: a migration or a removal), never pulled files, which in steady
// state are every ingest batch (#1728).
func TestTierDrainReportsUnlinkedNotPulled(t *testing.T) {
	m, hot := withHotRow(t, newMockBackend("s3"), true, "db/cpu/2026/05/01/10/cpu_pulled.parquet")
	var mu sync.Mutex
	var got []string
	m.SetOnPathsChanged(func(keys []string) {
		mu.Lock()
		got = append(got, keys...)
		mu.Unlock()
	})
	const pulled = "db/cpu/2026/05/01/10/cpu_pulled.parquet"
	// withHotRow recorded the pull before the callback was wired; another
	// pulled file now must not be reported either.
	hot.seedRaw("db/cpu/2026/05/01/10/cpu_other.parquet", []byte("123456789"))
	m.RecordReplicatedFile("db/cpu/2026/05/01/10/cpu_other.parquet", 9)
	waitTierEvents(t, m, 2)
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if len(got) != 0 {
		mu.Unlock()
		t.Fatalf("a pulled file was reported: %v", got)
	}
	mu.Unlock()
	m.RecordUnlinkedFile(pulled, "tiering:migrated", 9)
	waitTierEvents(t, m, 3)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		s := strings.Join(got, ",")
		mu.Unlock()
		if s == pulled {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("reported %v, want the unlinked path once", got)
}

// A database delete's unlink is not reported: the drop removes the
// database's tables itself, and a reconcile racing it could recreate one.
func TestTierDrainSkipsDatabaseDeletes(t *testing.T) {
	const p = "db/cpu/2026/05/01/10/cpu_pulled.parquet"
	m, _ := withHotRow(t, newMockBackend("s3"), true, p)
	var got []string
	m.SetOnPathsChanged(func(keys []string) { got = append(got, keys...) })
	m.RecordUnlinkedFile(p, "database:delete", 9)
	waitTierEvents(t, m, 2)
	time.Sleep(50 * time.Millisecond)
	if len(got) != 0 {
		t.Fatalf("a database delete was reported: %v", got)
	}
}
