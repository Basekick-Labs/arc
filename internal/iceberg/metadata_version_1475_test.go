package iceberg

// #1475: parseVersionAndMetaDir turns a metadata file name into the integer
// version-hint.text holds and v<N>.metadata.json is named after. Directory
// readers (DuckDB, Spark) depend on that integer, so the leading zeros of
// iceberg-go's 00004-<uuid> names are dropped on purpose.

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseVersionAndMetaDir(t *testing.T) {
	wh := t.TempDir()
	// resolveFileURI resolves only paths that exist; on macOS t.TempDir is
	// under a symlinked /var, so the metadata dir must exist for both sides
	// of the warehouse comparison to resolve alike.
	metaDir := filepath.Join(wh, "arc_db.db", "cpu", "metadata")
	if err := os.MkdirAll(metaDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sibling := wh + "-other"
	if err := os.MkdirAll(filepath.Join(sibling, "metadata"), 0o700); err != nil {
		t.Fatal(err)
	}
	e := &Exporter{warehouse: "file://" + wh}
	in := func(name string) string { return "file://" + filepath.Join(metaDir, name) }

	for _, tc := range []struct {
		loc  string
		want string // "" = not ok
	}{
		{in("00004-1f6e1c62-2a37-4f4b-9b3e-1d2c3b4a5f60.metadata.json"), "4"},
		{in("00010-abc.metadata.json"), "10"},
		{in("00000-abc.metadata.json"), "0"},
		{in("v4.metadata.json"), "4"},     // Arc's adopted copy (adopt.go)
		{in("v04.metadata.json"), "4"},    // the same version
		{in("+4-abc.metadata.json"), "4"}, // Atoi accepts a sign; iceberg-go never writes one
		{in("x4.metadata.json"), ""},
		{in("v.metadata.json"), ""},
		{in("-abc.metadata.json"), ""},
		{in("abcde-abc.metadata.json"), ""},
		{in("99999999999999999999-abc.metadata.json"), ""}, // overflows int
		{in("00004-abc.json"), ""},
		{"file://" + filepath.Join(sibling, "metadata", "00004-abc.metadata.json"), ""}, // a sibling warehouse
		{"file://" + wh, ""},                                                            // the warehouse root itself
	} {
		v, dir, ok := e.parseVersionAndMetaDir(tc.loc)
		if tc.want == "" {
			if ok {
				t.Errorf("%s: got version %q, want not ok", filepath.Base(tc.loc), v)
			}
			continue
		}
		if !ok || v != tc.want {
			t.Errorf("%s: got %q ok=%v, want %q", filepath.Base(tc.loc), v, ok, tc.want)
			continue
		}
		if dir != "arc_db.db/cpu/metadata" {
			t.Errorf("%s: dirKey %q, want arc_db.db/cpu/metadata", filepath.Base(tc.loc), dir)
		}
	}
}
