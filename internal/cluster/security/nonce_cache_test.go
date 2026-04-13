package security

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNonceCache_NewNonceAccepted(t *testing.T) {
	nc := NewNonceCache(5 * time.Minute)
	if !nc.Track("node-1", "abc123") {
		t.Fatal("first Track should return true (new nonce)")
	}
}

func TestNonceCache_DuplicateRejected(t *testing.T) {
	nc := NewNonceCache(5 * time.Minute)
	nc.Track("node-1", "abc123")
	if nc.Track("node-1", "abc123") {
		t.Fatal("duplicate nonce should return false")
	}
}

func TestNonceCache_SameNonceDifferentNode(t *testing.T) {
	nc := NewNonceCache(5 * time.Minute)
	nc.Track("node-1", "abc123")
	if !nc.Track("node-2", "abc123") {
		t.Fatal("same nonce from different node should be accepted")
	}
}

func TestNonceCache_ExpiredNonceReusable(t *testing.T) {
	nc := NewNonceCache(1 * time.Millisecond)
	nc.Track("node-1", "abc123")
	time.Sleep(5 * time.Millisecond)
	if !nc.Track("node-1", "abc123") {
		t.Fatal("expired nonce should be reusable")
	}
}

func TestNonceCache_Len(t *testing.T) {
	nc := NewNonceCache(5 * time.Minute)
	nc.Track("n1", "a")
	nc.Track("n1", "b")
	nc.Track("n2", "a")
	if nc.Len() != 3 {
		t.Errorf("Len: got %d, want 3", nc.Len())
	}
}

func TestNonceCache_ConcurrentAccess(t *testing.T) {
	nc := NewNonceCache(5 * time.Minute)
	var wg sync.WaitGroup
	accepted := make([]int, 10)
	// 10 goroutines each try to track the same nonce — exactly 1 should win.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if nc.Track("node-1", "race-nonce") {
				accepted[idx] = 1
			}
		}(i)
	}
	wg.Wait()
	total := 0
	for _, v := range accepted {
		total += v
	}
	if total != 1 {
		t.Errorf("exactly 1 goroutine should win the race, got %d", total)
	}
}

func TestNonceCache_ManyNonces(t *testing.T) {
	nc := NewNonceCache(5 * time.Minute)
	for i := 0; i < 10000; i++ {
		nonce := fmt.Sprintf("nonce-%d", i)
		if !nc.Track("node-1", nonce) {
			t.Fatalf("unique nonce %d should be accepted", i)
		}
	}
	if nc.Len() != 10000 {
		t.Errorf("Len: got %d, want 10000", nc.Len())
	}
}
