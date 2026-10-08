package replicaview

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestPinnedSourceSurvivesExternalUnlinkAndReplacement(t *testing.T) {
	root := t.TempDir()
	key := "db/cpu/file.parquet"
	if err := os.MkdirAll(filepath.Join(root, "db/cpu"), 0700); err != nil {
		t.Fatal(err)
	}
	content := []byte("immutable original file bytes")
	if err := os.WriteFile(filepath.Join(root, key), content, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	pins, err := OpenPinnedFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	defer pins.Close()
	pin, err := pins.Pin(key, hash)
	if err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Stat(filepath.Join(root, key))
	if err != nil {
		t.Fatal(err)
	}
	pinnedInfo, err := os.Stat(filepath.Join(root, pin))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(originalInfo, pinnedInfo) {
		t.Fatal("pin copied the file rather than linking its inode")
	}
	// Models another process deleting the source, then reusing its key.
	if err := os.Remove(filepath.Join(root, key)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, key), []byte("replacement bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, pin))
	if err != nil || string(got) != string(content) {
		t.Fatalf("query's old file changed: %q %v", got, err)
	}
	if err := pins.Remove(key, hash); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, key)); err != nil {
		t.Fatal("pin cleanup deleted the canonical replacement")
	}
}

func TestPinRejectsWrongChecksumAndEscapingSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "data"), []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	pins, err := OpenPinnedFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	defer pins.Close()
	sum := sha256.Sum256([]byte("different"))
	hash := hex.EncodeToString(sum[:])
	if _, err := pins.Pin("data", hash); err == nil {
		t.Fatal("published wrong file version")
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := pins.Pin("escape/data", hash); err == nil {
		t.Fatal("pin escaped storage root through a symlink")
	}
	for _, key := range []string{"../data", "/data", ".replica-pins/anything"} {
		if _, err := pins.Pin(key, hash); err == nil {
			t.Fatalf("accepted escaping key %q", key)
		}
	}
}
