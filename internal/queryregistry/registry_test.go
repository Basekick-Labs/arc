package queryregistry

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestRegistry(historySize int) *Registry {
	return NewRegistry(&RegistryConfig{HistorySize: historySize}, zerolog.Nop())
}

func TestRegistry_RegisterAndGetActive(t *testing.T) {
	r := newTestRegistry(10)

	queryID, ctx := r.Register(context.Background(), "SELECT 1", 1, "test-token", "127.0.0.1", false, 0)
	if queryID == "" {
		t.Fatal("expected non-empty query ID")
	}
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	active := r.GetActive()
	if len(active) != 1 {
		t.Fatalf("expected 1 active query, got %d", len(active))
	}
	if active[0].ID != queryID {
		t.Fatalf("expected query ID %s, got %s", queryID, active[0].ID)
	}
	if active[0].Status != StatusRunning {
		t.Fatalf("expected status running, got %s", active[0].Status)
	}
	if active[0].SQL != "SELECT 1" {
		t.Fatalf("expected SQL 'SELECT 1', got %s", active[0].SQL)
	}

	if r.ActiveCount() != 1 {
		t.Fatalf("expected ActiveCount=1, got %d", r.ActiveCount())
	}
}

func TestRegistry_Complete(t *testing.T) {
	r := newTestRegistry(10)

	queryID, _ := r.Register(context.Background(), "SELECT 1", 1, "test-token", "127.0.0.1", false, 0)
	r.Complete(queryID, 42)

	if r.ActiveCount() != 0 {
		t.Fatalf("expected 0 active queries after complete, got %d", r.ActiveCount())
	}

	history := r.GetHistory(0)
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}
	if history[0].Status != StatusCompleted {
		t.Fatalf("expected status completed, got %s", history[0].Status)
	}
	if history[0].RowCount != 42 {
		t.Fatalf("expected RowCount=42, got %d", history[0].RowCount)
	}
	if history[0].EndTime == nil {
		t.Fatal("expected non-nil EndTime")
	}
}

func TestRegistry_Fail(t *testing.T) {
	r := newTestRegistry(10)

	queryID, _ := r.Register(context.Background(), "SELECT bad", 1, "test-token", "127.0.0.1", false, 0)
	r.Fail(queryID, "syntax error")

	if r.ActiveCount() != 0 {
		t.Fatalf("expected 0 active queries after fail, got %d", r.ActiveCount())
	}

	history := r.GetHistory(0)
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}
	if history[0].Status != StatusFailed {
		t.Fatalf("expected status failed, got %s", history[0].Status)
	}
	if history[0].Error != "syntax error" {
		t.Fatalf("expected error 'syntax error', got %s", history[0].Error)
	}
}

func TestRegistry_TimedOut(t *testing.T) {
	r := newTestRegistry(10)

	queryID, _ := r.Register(context.Background(), "SELECT slow()", 1, "test-token", "127.0.0.1", false, 0)
	r.TimedOut(queryID)

	history := r.GetHistory(0)
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}
	if history[0].Status != StatusTimedOut {
		t.Fatalf("expected status timed_out, got %s", history[0].Status)
	}
}

func TestRegistry_Cancel(t *testing.T) {
	r := newTestRegistry(10)

	queryID, ctx := r.Register(context.Background(), "SELECT slow()", 1, "test-token", "127.0.0.1", false, 0)

	// Cancel the query
	ok := r.Cancel(queryID)
	if !ok {
		t.Fatal("expected Cancel to return true")
	}

	// Context should be cancelled
	select {
	case <-ctx.Done():
		// expected
	default:
		t.Fatal("expected context to be cancelled")
	}

	if r.ActiveCount() != 0 {
		t.Fatalf("expected 0 active after cancel, got %d", r.ActiveCount())
	}

	history := r.GetHistory(0)
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}
	if history[0].Status != StatusCancelled {
		t.Fatalf("expected status cancelled, got %s", history[0].Status)
	}
}

func TestRegistry_Cancel_NotFound(t *testing.T) {
	r := newTestRegistry(10)

	ok := r.Cancel("nonexistent")
	if ok {
		t.Fatal("expected Cancel to return false for nonexistent query")
	}
}

func TestRegistry_GetQuery_Active(t *testing.T) {
	r := newTestRegistry(10)

	queryID, _ := r.Register(context.Background(), "SELECT 1", 1, "test-token", "127.0.0.1", false, 0)

	q := r.GetQuery(queryID)
	if q == nil {
		t.Fatal("expected to find active query")
	}
	if q.Status != StatusRunning {
		t.Fatalf("expected status running, got %s", q.Status)
	}
}

func TestRegistry_GetQuery_History(t *testing.T) {
	r := newTestRegistry(10)

	queryID, _ := r.Register(context.Background(), "SELECT 1", 1, "test-token", "127.0.0.1", false, 0)
	r.Complete(queryID, 5)

	q := r.GetQuery(queryID)
	if q == nil {
		t.Fatal("expected to find query in history")
	}
	if q.Status != StatusCompleted {
		t.Fatalf("expected status completed, got %s", q.Status)
	}
}

func TestRegistry_GetQuery_NotFound(t *testing.T) {
	r := newTestRegistry(10)

	q := r.GetQuery("nonexistent")
	if q != nil {
		t.Fatal("expected nil for nonexistent query")
	}
}

func TestRegistry_HistoryRingBuffer_Overflow(t *testing.T) {
	r := newTestRegistry(3) // Small buffer

	// Register and complete 5 queries
	for i := 0; i < 5; i++ {
		id, _ := r.Register(context.Background(), "SELECT 1", 1, "tok", "127.0.0.1", false, 0)
		r.Complete(id, i)
	}

	// Should only keep the last 3
	if r.HistoryLen() != 3 {
		t.Fatalf("expected HistoryLen=3, got %d", r.HistoryLen())
	}

	history := r.GetHistory(0)
	if len(history) != 3 {
		t.Fatalf("expected 3 history entries, got %d", len(history))
	}

	// Should be newest first: row counts 4, 3, 2
	if history[0].RowCount != 4 {
		t.Fatalf("expected newest entry RowCount=4, got %d", history[0].RowCount)
	}
	if history[2].RowCount != 2 {
		t.Fatalf("expected oldest entry RowCount=2, got %d", history[2].RowCount)
	}
}

func TestRegistry_HistoryLimit(t *testing.T) {
	r := newTestRegistry(10)

	for i := 0; i < 5; i++ {
		id, _ := r.Register(context.Background(), "SELECT 1", 1, "tok", "127.0.0.1", false, 0)
		r.Complete(id, i)
	}

	// Request only 2
	history := r.GetHistory(2)
	if len(history) != 2 {
		t.Fatalf("expected 2 history entries with limit, got %d", len(history))
	}
}

func TestRegistry_ParallelQuery(t *testing.T) {
	r := newTestRegistry(10)

	queryID, _ := r.Register(context.Background(), "SELECT * FROM db.measurement", 1, "tok", "127.0.0.1", true, 24)

	q := r.GetQuery(queryID)
	if !q.IsParallel {
		t.Fatal("expected IsParallel=true")
	}
	if q.PartitionCount != 24 {
		t.Fatalf("expected PartitionCount=24, got %d", q.PartitionCount)
	}
}

func TestRegistry_DefaultHistorySize(t *testing.T) {
	r := NewRegistry(nil, zerolog.Nop())
	if r.histSize != 100 {
		t.Fatalf("expected default histSize=100, got %d", r.histSize)
	}
}

func TestRegistry_ActiveDurationMs(t *testing.T) {
	r := newTestRegistry(10)

	r.Register(context.Background(), "SELECT 1", 1, "tok", "127.0.0.1", false, 0)
	time.Sleep(10 * time.Millisecond)

	active := r.GetActive()
	if len(active) != 1 {
		t.Fatalf("expected 1 active query, got %d", len(active))
	}
	if active[0].DurationMs < 10 {
		t.Fatalf("expected DurationMs >= 10, got %f", active[0].DurationMs)
	}
}

func TestRegistry_Concurrent(t *testing.T) {
	r := newTestRegistry(100)
	var wg sync.WaitGroup

	// Concurrent registers and completes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, _ := r.Register(context.Background(), "SELECT 1", 1, "tok", "127.0.0.1", false, 0)
			r.Complete(id, 1)
		}()
	}

	// Concurrent cancels
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, _ := r.Register(context.Background(), "SELECT 1", 1, "tok", "127.0.0.1", false, 0)
			r.Cancel(id)
		}()
	}

	// Concurrent reads
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.GetActive()
			r.GetHistory(0)
			r.ActiveCount()
		}()
	}

	wg.Wait()

	// All queries should be completed or cancelled
	if r.ActiveCount() != 0 {
		t.Fatalf("expected 0 active queries after concurrent test, got %d", r.ActiveCount())
	}
}

func TestRegistry_CompleteNonexistent(t *testing.T) {
	r := newTestRegistry(10)

	// Should not panic
	r.Complete("nonexistent", 0)
	r.Fail("nonexistent", "error")
	r.TimedOut("nonexistent")

	if r.HistoryLen() != 0 {
		t.Fatalf("expected 0 history entries, got %d", r.HistoryLen())
	}
}
