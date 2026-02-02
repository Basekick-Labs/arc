package audit

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func newTestLogger(t *testing.T) *Logger {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	l, err := NewLogger(&LoggerConfig{
		DB:     db,
		Config: &config.AuditLogConfig{Enabled: true, RetentionDays: 90},
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestNewLogger(t *testing.T) {
	l := newTestLogger(t)
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewLogger_NilDB(t *testing.T) {
	_, err := NewLogger(&LoggerConfig{
		Config: &config.AuditLogConfig{},
		Logger: zerolog.Nop(),
	})
	if err == nil {
		t.Fatal("expected error for nil DB")
	}
}

func TestLogEventAndQuery(t *testing.T) {
	l := newTestLogger(t)
	l.Start()
	defer l.Stop()

	l.LogEvent(&AuditEvent{
		Timestamp:  time.Now().UTC(),
		EventType:  "token.created",
		Actor:      "admin",
		Method:     "POST",
		Path:       "/api/v1/auth/tokens",
		StatusCode: 201,
		IPAddress:  "127.0.0.1",
		DurationMs: 5,
	})

	l.LogEvent(&AuditEvent{
		Timestamp:  time.Now().UTC(),
		EventType:  "data.write",
		Actor:      "writer-token",
		Method:     "POST",
		Path:       "/api/v1/write/mydb",
		Database:   "mydb",
		StatusCode: 204,
		IPAddress:  "10.0.0.1",
		DurationMs: 12,
	})

	// Wait for flush
	time.Sleep(1500 * time.Millisecond)

	ctx := context.Background()

	// Query all
	entries, err := l.Query(ctx, &QueryFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Query by event type
	entries, err = l.Query(ctx, &QueryFilter{EventType: "token.created"})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Actor != "admin" {
		t.Fatalf("expected actor 'admin', got '%s'", entries[0].Actor)
	}

	// Query by database
	entries, err = l.Query(ctx, &QueryFilter{Database: "mydb"})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry for mydb, got %d", len(entries))
	}
}

func TestStats(t *testing.T) {
	l := newTestLogger(t)
	l.Start()
	defer l.Stop()

	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		l.LogEvent(&AuditEvent{Timestamp: now, EventType: "data.write", Actor: "a", Method: "POST", Path: "/write", StatusCode: 204})
	}
	l.LogEvent(&AuditEvent{Timestamp: now, EventType: "token.created", Actor: "a", Method: "POST", Path: "/tokens", StatusCode: 201})

	time.Sleep(1500 * time.Millisecond)

	stats, err := l.Stats(context.Background(), now.Add(-1*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if stats["data.write"] != 3 {
		t.Fatalf("expected 3 data.write, got %d", stats["data.write"])
	}
	if stats["token.created"] != 1 {
		t.Fatalf("expected 1 token.created, got %d", stats["token.created"])
	}
}

func TestRetentionCleanup(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	l, err := NewLogger(&LoggerConfig{
		DB:     db,
		Config: &config.AuditLogConfig{Enabled: true, RetentionDays: 1},
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert an old entry directly
	oldTime := time.Now().UTC().AddDate(0, 0, -5)
	_, err = db.Exec("INSERT INTO audit_logs (timestamp, event_type, actor, method, path, status_code) VALUES (?, ?, ?, ?, ?, ?)",
		oldTime, "old.event", "test", "GET", "/old", 200)
	if err != nil {
		t.Fatal(err)
	}

	// Insert a recent entry
	_, err = db.Exec("INSERT INTO audit_logs (timestamp, event_type, actor, method, path, status_code) VALUES (?, ?, ?, ?, ?, ?)",
		time.Now().UTC(), "new.event", "test", "GET", "/new", 200)
	if err != nil {
		t.Fatal(err)
	}

	l.cleanupOldEntries()

	entries, err := l.Query(context.Background(), &QueryFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after cleanup, got %d", len(entries))
	}
	if entries[0].EventType != "new.event" {
		t.Fatalf("expected new.event to survive, got %s", entries[0].EventType)
	}
}

func TestClassifyEvent(t *testing.T) {
	tests := []struct {
		method, path string
		status       int
		expected     string
	}{
		{"POST", "/api/v1/auth/tokens", 201, "token.created"},
		{"DELETE", "/api/v1/auth/tokens/abc", 200, "token.deleted"},
		{"POST", "/api/v1/auth/tokens/abc/rotate", 200, "token.rotated"},
		{"POST", "/api/v1/query", 200, "data.query"},
		{"POST", "/write", 204, "data.write"},
		{"POST", "/api/v2/write", 204, "data.write"},
		{"POST", "/api/v1/delete", 200, "data.delete"},
		{"POST", "/api/v1/databases", 201, "database.created"},
		{"DELETE", "/api/v1/databases/mydb", 200, "database.deleted"},
		{"POST", "/api/v1/rbac/organizations", 201, "rbac.org.created"},
		{"PUT", "/api/v1/rbac/roles/123", 200, "rbac.role.updated"},
		{"DELETE", "/api/v1/rbac/teams/abc", 200, "rbac.team.deleted"},
		{"GET", "/api/v1/rbac/memberships", 200, "rbac.membership.read"},
		{"GET", "/anything", 401, "auth.failed"},
		{"GET", "/anything", 403, "auth.failed"},
		{"POST", "/api/v1/import", 200, "data.import"},
		{"PUT", "/api/v1/mqtt/config", 200, "mqtt.put"},
		{"POST", "/api/v1/compaction/trigger", 200, "compaction.triggered"},
		{"PUT", "/api/v1/tiering/policy", 200, "tiering.put"},
		{"GET", "/unknown", 200, "api.get"},
	}

	for _, tt := range tests {
		got := classifyEvent(tt.method, tt.path, tt.status)
		if got != tt.expected {
			t.Errorf("classifyEvent(%s, %s, %d) = %s, want %s", tt.method, tt.path, tt.status, got, tt.expected)
		}
	}
}

func TestQueryWithLimitAndOffset(t *testing.T) {
	l := newTestLogger(t)
	l.Start()
	defer l.Stop()

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		l.LogEvent(&AuditEvent{Timestamp: now, EventType: "data.write", Actor: "a", Method: "POST", Path: "/write", StatusCode: 204})
	}

	time.Sleep(1500 * time.Millisecond)

	entries, err := l.Query(context.Background(), &QueryFilter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries with limit, got %d", len(entries))
	}

	entries, err = l.Query(context.Background(), &QueryFilter{Limit: 2, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries with offset, got %d", len(entries))
	}
}
