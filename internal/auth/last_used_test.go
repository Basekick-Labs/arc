package auth

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// syncBuffer is a concurrency-safe io.Writer for capturing log output while
// background goroutines are still running.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// setupLoggingAuthManager builds an AuthManager whose logs are captured, so a
// test can assert on what shutdown did (or did not) emit.
func setupLoggingAuthManager(t *testing.T) (*AuthManager, *syncBuffer) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "arc-auth-lastused-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	logs := &syncBuffer{}
	logger := zerolog.New(logs).Level(zerolog.DebugLevel)

	am, err := NewAuthManager(filepath.Join(tmpDir, "auth.db"), 5*time.Minute, 100, logger)
	if err != nil {
		t.Fatalf("failed to create AuthManager: %v", err)
	}
	return am, logs
}

// Regression guard for #325: the last_used_at update must not outlive Close().
//
// Previously VerifyToken spawned a fire-and-forget goroutine running
// db.Exec(UPDATE ...). A Close() that landed between the spawn and the Exec
// closed the database out from under it, and the goroutine logged
// "sql: database is closed" at Error level on an otherwise clean shutdown.
func TestLastUsed_NoErrorLoggedWhenClosedImmediatelyAfterVerify(t *testing.T) {
	// Repeat: the original race was timing-dependent, so a single iteration
	// proves little. Kept small because each iteration pays PBKDF2 token
	// creation — the pre-fix code fails on the first iteration in practice.
	iterations := 10
	if testing.Short() {
		iterations = 2
	}
	for i := 0; i < iterations; i++ {
		am, logs := setupLoggingAuthManager(t)

		token, err := am.CreateToken(context.Background(), "tok", "", "admin", nil)
		if err != nil {
			t.Fatalf("failed to create token: %v", err)
		}

		// Force a cache miss so the last_used_at path actually runs: a cache
		// hit returns before the update is ever queued.
		am.InvalidateCache()
		if info := am.VerifyToken(token); info == nil {
			t.Fatal("expected token to verify")
		}

		// Close immediately — this is the race window.
		if err := am.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		if out := logs.String(); strings.Contains(out, "database is closed") {
			t.Fatalf("iteration %d: shutdown logged a closed-database error:\n%s", i, out)
		}
	}
}

// The update must actually be applied, not merely not-crash: a fix that
// silently dropped every update would pass the race test above.
func TestLastUsed_IsPersistedAfterClose(t *testing.T) {
	am, _ := setupLoggingAuthManager(t)

	token, err := am.CreateToken(context.Background(), "tok", "", "admin", nil)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	// A freshly created token has never been used.
	tokens, err := am.ListTokens()
	if err != nil {
		t.Fatalf("ListTokens failed: %v", err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].LastUsedAt != nil {
		t.Fatalf("expected LastUsedAt to be unset before use, got %v", tokens[0].LastUsedAt)
	}

	am.InvalidateCache()
	if info := am.VerifyToken(token); info == nil {
		t.Fatal("expected token to verify")
	}

	// Close drains the queue, so the update must be durable afterwards.
	if err := am.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := NewAuthManager(am.dbPath, 5*time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("failed to reopen AuthManager: %v", err)
	}
	defer reopened.Close()

	tokens, err = reopened.ListTokens()
	if err != nil {
		t.Fatalf("ListTokens after reopen failed: %v", err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token after reopen, got %d", len(tokens))
	}
	if tokens[0].LastUsedAt == nil {
		t.Fatal("last_used_at was not persisted — the queued update was dropped on shutdown")
	}
}

// A full queue must never block the authentication hot path.
func TestRecordLastUsed_DoesNotBlockWhenQueueFull(t *testing.T) {
	am := &AuthManager{
		lastUsedCh: make(chan lastUsedUpdate, 2),
		logger:     zerolog.Nop(),
	}

	// Fill the buffer, then overshoot it substantially. No reader is draining,
	// so any blocking send would deadlock this test.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			am.recordLastUsed(int64(i), time.Now())
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recordLastUsed blocked when the queue was full")
	}

	if got := len(am.lastUsedCh); got != 2 {
		t.Errorf("expected queue to stay at capacity 2, got %d", got)
	}
}

// Close must be safe when updates are still being produced concurrently.
func TestLastUsed_ConcurrentVerifyDuringClose(t *testing.T) {
	am, logs := setupLoggingAuthManager(t)

	token, err := am.CreateToken(context.Background(), "tok", "", "admin", nil)
	if err != nil {
		t.Fatalf("failed to create token: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					am.InvalidateCache()
					am.VerifyToken(token)
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	closeErr := am.Close()
	close(stop)
	wg.Wait()

	if closeErr != nil {
		t.Fatalf("Close failed: %v", closeErr)
	}
	// Verifications racing Close may legitimately fail (the DB is closing), but
	// the drain itself must not report a closed database.
	if out := logs.String(); strings.Contains(out, "Failed to update last_used_at") &&
		strings.Contains(out, "database is closed") {
		t.Errorf("last_used_at writer ran against a closed database:\n%s", out)
	}
}
