package telemetry

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A configured id is used and nothing is written to the data directory.
func TestConfiguredInstanceIDWritesNothing1885(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	c, err := New(&Config{Endpoint: "http://127.0.0.1:1", Interval: time.Hour, DataDir: dir, InstanceID: "4f8a2c1e-9b3d-4e5f-8a7b-1c2d3e4f5a6b"}, "test", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if got := c.GetInstanceID(); got != "4f8a2c1e-9b3d-4e5f-8a7b-1c2d3e4f5a6b" {
		t.Fatalf("instance id %q", got)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("data dir was created (%v)", err)
	}
}

// Without one, the id file in the data directory is created once and reused;
// a file holding only whitespace is replaced, not returned as "".
func TestInstanceIDFile1885(t *testing.T) {
	dir := t.TempDir()
	first, err := loadOrCreateInstanceID(dir)
	if err != nil || first == "" {
		t.Fatalf("first = %q, %v", first, err)
	}
	if again, _ := loadOrCreateInstanceID(dir); again != first {
		t.Fatalf("second load %q, want %q", again, first)
	}
	info, err := os.Stat(filepath.Join(dir, instanceIDFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("id file mode %v (%v), want 0600", info.Mode().Perm(), err)
	}
	if err := os.WriteFile(filepath.Join(dir, instanceIDFile), []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := loadOrCreateInstanceID(dir); err != nil || got == "" {
		t.Fatalf("whitespace file: %q, %v; want a fresh id", got, err)
	}
}

// A read error other than a missing file is reported, not papered over with
// a new id.
func TestInstanceIDReadErrorIsReported1885(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, instanceIDFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if id, err := loadOrCreateInstanceID(dir); err == nil {
		t.Fatalf("got id %q for an unreadable id file, want an error", id)
	}
}
