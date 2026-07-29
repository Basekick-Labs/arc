package api

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// Retention and CQ have their own *_db_path keys that default to the auth
// database. When they resolve to the same file the handle is borrowed, and
// Close must not close it: these handlers shut down before the auth manager
// that owns it, so closing here would take the database out from under
// everything still shutting down.
func TestRetentionHandler_BorrowsAndDoesNotCloseSharedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	am, err := auth.NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	h, err := NewRetentionHandler(nil, nil, &config.RetentionConfig{DBPath: dbPath}, nil, am, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRetentionHandler: %v", err)
	}

	if h.ownsDB {
		t.Error("handler must borrow the auth handle when the paths match")
	}
	if h.db != am.GetDB() {
		t.Error("handler must use the auth manager's handle, not a new one")
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close on a borrowed handle should be a no-op: %v", err)
	}
	if err := am.GetDB().Ping(); err != nil {
		t.Fatalf("borrowed handle was closed by the handler: %v", err)
	}
}

// An operator who points retention.db_path at a different file expects a
// genuinely separate database — borrowing there would silently ignore the key.
func TestRetentionHandler_OwnsSeparateDB(t *testing.T) {
	dir := t.TempDir()
	authPath := filepath.Join(dir, "arc.db")
	retentionPath := filepath.Join(dir, "retention.db")

	am, err := auth.NewAuthManager(authPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	h, err := NewRetentionHandler(nil, nil, &config.RetentionConfig{DBPath: retentionPath}, nil, am, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRetentionHandler: %v", err)
	}

	if !h.ownsDB {
		t.Fatal("a separate db_path must produce a handle the handler owns")
	}
	if h.db == am.GetDB() {
		t.Error("a separate db_path must not reuse the auth handle")
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.db.Ping(); err == nil {
		t.Error("an owned handle must actually be closed")
	}
	// The auth database must be untouched.
	if err := am.GetDB().Ping(); err != nil {
		t.Errorf("closing the retention handle affected the auth database: %v", err)
	}
}

// With auth disabled there is no handle to borrow; the handler opens its own,
// exactly as before.
func TestRetentionHandler_NilAuthManagerOpensOwnDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")

	h, err := NewRetentionHandler(nil, nil, &config.RetentionConfig{DBPath: dbPath}, nil, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRetentionHandler with a nil auth manager: %v", err)
	}
	defer h.Close()

	if !h.ownsDB {
		t.Error("with no auth manager the handler must own its handle")
	}
	if _, err := h.db.Exec("SELECT 1"); err != nil {
		t.Fatalf("database unusable: %v", err)
	}
}

func TestContinuousQueryHandler_BorrowsAndDoesNotCloseSharedDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	am, err := auth.NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	h, err := NewContinuousQueryHandler(nil, nil, nil, &config.ContinuousQueryConfig{DBPath: dbPath}, am, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewContinuousQueryHandler: %v", err)
	}

	if h.ownsDB {
		t.Error("handler must borrow the auth handle when the paths match")
	}
	if h.sqliteDB != am.GetDB() {
		t.Error("handler must use the auth manager's handle, not a new one")
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close on a borrowed handle should be a no-op: %v", err)
	}
	if err := am.GetDB().Ping(); err != nil {
		t.Fatalf("borrowed handle was closed by the handler: %v", err)
	}
}

func TestContinuousQueryHandler_OwnsSeparateDB(t *testing.T) {
	dir := t.TempDir()
	am, err := auth.NewAuthManager(filepath.Join(dir, "arc.db"), time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	h, err := NewContinuousQueryHandler(nil, nil, nil,
		&config.ContinuousQueryConfig{DBPath: filepath.Join(dir, "cq.db")}, am, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewContinuousQueryHandler: %v", err)
	}

	if !h.ownsDB {
		t.Fatal("a separate db_path must produce a handle the handler owns")
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := am.GetDB().Ping(); err != nil {
		t.Errorf("closing the CQ handle affected the auth database: %v", err)
	}
}

// Retention's schema declares a foreign key from retention_executions to
// retention_policies. The auth DSN enables foreign key enforcement while
// retention's own DSN did not, so borrowing changes enforcement — verify the
// handler's actual delete order still works under it.
func TestRetentionHandler_DeleteOrderWorksWithForeignKeysEnabled(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	am, err := auth.NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	h, err := NewRetentionHandler(nil, nil, &config.RetentionConfig{DBPath: dbPath}, nil, am, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRetentionHandler: %v", err)
	}
	defer h.Close()

	res, err := h.db.Exec(
		`INSERT INTO retention_policies (name, database, retention_days) VALUES ('p', 'db', 30)`)
	if err != nil {
		t.Fatalf("insert policy: %v", err)
	}
	policyID, _ := res.LastInsertId()

	if _, err := h.db.Exec(
		`INSERT INTO retention_executions (policy_id, status) VALUES (?, 'ok')`, policyID); err != nil {
		t.Fatalf("insert execution: %v", err)
	}

	// Children first, then the parent — the order deletePolicy uses.
	if _, err := h.db.Exec("DELETE FROM retention_executions WHERE policy_id = ?", policyID); err != nil {
		t.Fatalf("delete executions: %v", err)
	}
	if _, err := h.db.Exec("DELETE FROM retention_policies WHERE id = ?", policyID); err != nil {
		t.Fatalf("delete policy with foreign keys enabled: %v", err)
	}
}
