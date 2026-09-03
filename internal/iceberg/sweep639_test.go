package iceberg

// Unit tests for the #639 sweep-completion items that live in this package:
// item 2 (once-per-process gated warning), item 5 (symlink-resolved
// warehouseRelKey), item 7 (tighten-only metadata permission hardening).

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestWarehouseRelKey_SymlinkedWarehouseResolves(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real-wh")
	if err := os.MkdirAll(filepath.Join(real, "arc_db.db", "cpu", "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link-wh")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	// Warehouse configured through the SYMLINK; metadata location rendered
	// through the REAL path (how a resolved writer would emit it). Raw string
	// comparison never matches these; resolution must.
	e := &Exporter{warehouse: "file://" + filepath.ToSlash(link)}
	metaLoc := "file://" + filepath.ToSlash(filepath.Join(real, "arc_db.db", "cpu", "metadata", "v3.metadata.json"))

	rel, ok := e.warehouseRelKey(metaLoc)
	if !ok {
		t.Fatal("symlinked warehouse spelling did not resolve to the same root (#639 item 5)")
	}
	if want := "arc_db.db/cpu/metadata/v3.metadata.json"; rel != want {
		t.Fatalf("rel = %q, want %q", rel, want)
	}
}

func TestHardenLocalMetadataPerms_TightenOnly(t *testing.T) {
	base := t.TempDir()
	metaDir := filepath.Join(base, "wh", "arc_db.db", "cpu", "metadata")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	wide := filepath.Join(metaDir, "00001-x.metadata.json")
	if err := os.WriteFile(wide, []byte("m"), 0o644); err != nil {
		t.Fatal(err)
	}
	restrictive := filepath.Join(metaDir, "keep.json")
	if err := os.WriteFile(restrictive, []byte("m"), 0o400); err != nil {
		t.Fatal(err)
	}

	e := &Exporter{logger: zerolog.Nop()}
	e.hardenLocalMetadataPerms("file://" + filepath.ToSlash(wide))

	if info, _ := os.Stat(wide); info.Mode().Perm() != 0o600 {
		t.Fatalf("umask-wide file = %v, want 0600", info.Mode().Perm())
	}
	if info, _ := os.Stat(restrictive); info.Mode().Perm() != 0o400 {
		t.Fatalf("restrictive file = %v, want UNCHANGED 0400 (tighten only)", info.Mode().Perm())
	}
	if info, _ := os.Stat(metaDir); info.Mode().Perm() != 0o700 {
		t.Fatalf("metadata dir = %v, want 0700", info.Mode().Perm())
	}
}

func TestRunPass_GatedWarnsOncePerProcess(t *testing.T) {
	var logs bytes.Buffer
	s := &Scheduler{
		gate:   blockedGate{},
		logger: zerolog.New(&logs),
		state:  map[string]measurementState{},
	}
	s.runPass(context.Background())
	s.runPass(context.Background())
	s.runPass(context.Background())

	if got := strings.Count(logs.String(), "gated off on this node"); got != 1 {
		t.Fatalf("gated WARN fired %d times over 3 passes, want exactly 1 (#639 item 2)", got)
	}
}

type blockedGate struct{}

func (blockedGate) CanRun() bool { return false }
