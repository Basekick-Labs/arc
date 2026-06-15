package mqtt

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// TestNewRepository_LocksDBPerms is the M4 regression test (issue #509): on a
// custom (non-shared) MQTT DB path, the DB and its WAL/SHM siblings must be
// locked to 0600. The MQTT DB holds encrypted broker credentials.
func TestNewRepository_LocksDBPerms(t *testing.T) {
	// Place the DB under a not-yet-existing subdirectory so we also exercise the
	// 0700 parent-dir creation.
	dbPath := filepath.Join(t.TempDir(), "sub", "mqtt.db")

	repo, err := NewRepository(dbPath, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	for _, p := range []string{dbPath, dbPath + "-wal", dbPath + "-shm", dbPath + "-journal"} {
		info, err := os.Lstat(p)
		if os.IsNotExist(err) {
			continue // sidecars may not exist depending on journal mode/state
		}
		if err != nil {
			t.Fatalf("lstat %s: %v", p, err)
		}
		if perm := info.Mode().Perm(); perm != 0600 {
			t.Errorf("%s perm = %o, want 0600", filepath.Base(p), perm)
		}
	}
}
