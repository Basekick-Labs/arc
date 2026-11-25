package auth

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func newLoggedAuth1915(t *testing.T) (*AuthManager, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	am, err := NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, zerolog.New(&buf))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	return am, &buf
}

const (
	valueA1915 = "bootstrap-value-a-0123456789abcdef0123456789"
	valueB1915 = "bootstrap-value-b-0123456789abcdef0123456789"
)

// The bootstrap's length rule is the one config refuses at load.
func TestBootstrapLengthMatchesConfig1915(t *testing.T) {
	short := strings.Repeat("x", config.MinBootstrapTokenLength-1)
	if _, err := (&AuthManager{}).EnsureInitialTokenWithValue(context.Background(), short); err == nil {
		t.Fatalf("auth accepted a %d-byte value that config refuses", len(short))
	}
}

// A configured value that is not among the tokens of a populated database
// is reported at Warn (it used to be a debug line), the value is not
// logged, existing tokens still work, and startup does not mark the admin
// token as used.
func TestSkippedBootstrapIsExplained1915(t *testing.T) {
	am, buf := newLoggedAuth1915(t)
	ctx := context.Background()
	if _, err := am.EnsureInitialTokenWithValue(ctx, valueA1915); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if got, err := am.EnsureInitialTokenWithValue(ctx, valueB1915); err != nil || got != "" {
		t.Fatalf("second bootstrap = %q, %v; want a no-op", got, err)
	}
	out := buf.String()
	if !strings.Contains(out, `"level":"warn"`) || !strings.Contains(out, "was NOT installed") || !strings.Contains(out, "auth.force_bootstrap=true") {
		t.Fatalf("no warning naming force_bootstrap:\n%s", out)
	}
	if strings.Contains(out, valueB1915) || strings.Contains(out, valueA1915) {
		t.Fatal("a token value was logged")
	}
	var lastUsed sql.NullTime
	if err := am.db.QueryRow(`SELECT last_used_at FROM api_tokens WHERE name = 'admin'`).Scan(&lastUsed); err != nil {
		t.Fatal(err)
	}
	if lastUsed.Valid {
		t.Fatal("startup stamped last_used_at on the admin token")
	}
	if am.VerifyToken(valueA1915) == nil {
		t.Fatal("the existing token stopped authenticating")
	}
}

// The normal restart, with the configured value already installed, stays
// quiet.
func TestBootstrapAlreadyInstalledIsQuiet1915(t *testing.T) {
	am, buf := newLoggedAuth1915(t)
	ctx := context.Background()
	if _, err := am.EnsureInitialTokenWithValue(ctx, valueA1915); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if _, err := am.EnsureInitialTokenWithValue(ctx, valueA1915); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), `"level":"warn"`) {
		t.Fatalf("warned on a normal restart:\n%s", buf.String())
	}
}

// A value matching a disabled token is reported as such, not as one to
// force-add.
func TestBootstrapMatchesDisabledToken1915(t *testing.T) {
	am, buf := newLoggedAuth1915(t)
	ctx := context.Background()
	if _, err := am.EnsureInitialTokenWithValue(ctx, valueA1915); err != nil {
		t.Fatal(err)
	}
	if _, err := am.db.Exec(`UPDATE api_tokens SET enabled = 0 WHERE name = 'admin'`); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if _, err := am.EnsureInitialTokenWithValue(ctx, valueA1915); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "matches a token that is disabled") || strings.Contains(out, "was NOT installed") {
		t.Fatalf("want the disabled-token warning:\n%s", out)
	}
}

// A forced start with a new value, while an arc-recovery token from an
// earlier recovery exists, says the value was not added and what to do,
// instead of claiming the caller can use it; the skipped bootstrap points
// at the existing arc-recovery too.
func TestForcedStartWithAnExistingRecoveryToken1915(t *testing.T) {
	am, buf := newLoggedAuth1915(t)
	ctx := context.Background()
	if _, err := am.EnsureInitialToken(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := am.ForceAddRecoveryToken(ctx, valueA1915); err != nil || got == "" {
		t.Fatalf("first recovery = %q, %v", got, err)
	}
	buf.Reset()
	if got, err := am.ForceAddRecoveryToken(ctx, valueB1915); err != nil || got != "" {
		t.Fatalf("second recovery = %q, %v; want a no-op", got, err)
	}
	if out := buf.String(); !strings.Contains(out, "already exists with a different value") || !strings.Contains(out, "was NOT added") {
		t.Fatalf("want the different-value warning:\n%s", out)
	}
	if am.VerifyToken(valueB1915) != nil {
		t.Fatal("the second value authenticates; the test does not exercise the no-op")
	}
	buf.Reset()
	if _, err := am.EnsureInitialTokenWithValue(ctx, valueB1915); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, "arc-recovery from an earlier recovery already exists") {
		t.Fatalf("skipped bootstrap does not mention the existing arc-recovery:\n%s", out)
	}
	// The same value again is the restart of a recovery that worked: no
	// different-value warning.
	buf.Reset()
	if _, err := am.ForceAddRecoveryToken(ctx, valueA1915); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "different value") {
		t.Fatalf("warned about a different value for the same value:\n%s", buf.String())
	}
}
