package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// The property: ListObjects and ListUnusable partition the store between them.
//
// This is the shape that matters, rather than a list of known-bad spellings.
// #756 exists because the hidden set was derived from ValidateKey while the
// listings dropped entries for another reason too, so the two definitions
// disagreed and a contract-VALID data file went missing from every backup with
// nothing reporting it. Asserting the partition directly is what stops that
// from happening again the next time a listing grows a filter.
func TestListingsPartitionTheStore(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	ctx := context.Background()

	// Every file below is a real file on disk holding bytes.
	onDisk := []string{
		"db/cpu/2026/09/12/13/good.parquet",        // ordinary
		"db/cpu/2026/09/12/13/ba\\d.parquet",       // backslash: refused by the contract
		"db/cpu/2026/09/12/13/.hidden.parquet",     // dot-prefixed: ACCEPTED by the contract
		"db/cpu/2026/09/12/13/legacy.part",         // reserved suffix, no committed base
		"db/cpu/2026/09/12/13/.arc-123456.tmp",     // a write in progress
		"db/cpu/2026/09/12/13/staged.parquet",      // committed…
		"db/cpu/2026/09/12/13/staged.parquet.part", // …with its ordinary staging partial
	}
	for _, rel := range onDisk {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, []byte("ROWS:"+rel), 0o600); err != nil {
			t.Skipf("filesystem will not hold %q: %v", rel, err)
		}
	}

	objs, err := b.ListObjects(ctx, "")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	hidden, err := b.ListUnusable(ctx, "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}

	seen := map[string]string{}
	for _, o := range objs {
		seen[o.Path] = "listed"
	}
	for _, o := range hidden {
		if prev, dup := seen[o.Path]; dup {
			t.Errorf("%q is both %s and reported unusable; the two must partition", o.Path, prev)
		}
		seen[o.Path] = "unusable"
	}

	want := map[string]string{
		"db/cpu/2026/09/12/13/good.parquet":        "listed",
		"db/cpu/2026/09/12/13/staged.parquet":      "listed",
		"db/cpu/2026/09/12/13/ba\\d.parquet":       "unusable",
		"db/cpu/2026/09/12/13/.hidden.parquet":     "unusable",
		"db/cpu/2026/09/12/13/legacy.part":         "", // base key "legacy" is valid: staging, not loss
		"db/cpu/2026/09/12/13/.arc-123456.tmp":     "", // in-flight: neither
		"db/cpu/2026/09/12/13/staged.parquet.part": "", // ordinary staging: neither
	}
	for path, expect := range want {
		got := seen[path]
		if got != expect {
			t.Errorf("%q: got %q, want %q", path, orNone(got), orNone(expect))
		}
	}
}

func orNone(s string) string {
	if s == "" {
		return "(neither)"
	}
	return s
}

// A dot-prefixed data file passes the key contract, is writable and readable
// through Backend, and is still absent from every listing. Deriving the hidden
// set from ValidateKey would therefore never report it, which is the specific
// hole this test pins.
func TestContractValidFileHiddenByListingIsReported(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	ctx := context.Background()
	const key = "db/cpu/2026/09/12/13/.hidden.parquet"

	if err := ValidateKey(key); err != nil {
		t.Fatalf("premise: the key must PASS the contract, got %v", err)
	}
	if err := b.Write(ctx, key, []byte("HIDDEN-ROWS")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := b.Read(ctx, key); err != nil {
		t.Fatalf("Read: %v", err)
	}

	objs, _ := b.ListObjects(ctx, "")
	if len(objs) != 0 {
		t.Fatalf("premise: the listing must hide it, got %v", objs)
	}

	hidden, err := b.ListUnusable(ctx, "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}
	if len(hidden) != 1 || hidden[0].Path != key {
		t.Fatalf("ListUnusable = %v, want exactly %q", hidden, key)
	}
	if hidden[0].Size != int64(len("HIDDEN-ROWS")) {
		t.Errorf("Size = %d, want %d", hidden[0].Size, len("HIDDEN-ROWS"))
	}
}

// Keys the contract refuses are reported with an error callers can classify,
// matching how the reconciliation sweeps branch on ErrInvalidPath.
func TestUnusableReportsClassifiableError(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}

	dir := filepath.Join(root, "db", "cpu")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ba\\d.parquet"), []byte("x"), 0o600); err != nil {
		t.Skipf("no backslash filenames here: %v", err)
	}

	hidden, err := b.ListUnusable(context.Background(), "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}
	if len(hidden) != 1 {
		t.Fatalf("want 1 entry, got %v", hidden)
	}
	if !errors.Is(hidden[0].Err, ErrInvalidPath) {
		t.Errorf("Err = %v, want it to wrap ErrInvalidPath", hidden[0].Err)
	}
}

// An over-long key is producible by nesting (a >255-byte SEGMENT is not
// creatable on ext4 at all), and it must be reported rather than vanish.
func TestOverLongKeyIsReported(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	seg := strings.Repeat("d", 200)
	rel := filepath.Join("db", seg, seg, seg, seg, seg, seg, "f.parquet")
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Skipf("cannot nest that deep: %v", err)
	}
	if err := os.WriteFile(full, []byte("LONG"), 0o600); err != nil {
		t.Skipf("cannot create: %v", err)
	}

	ctx := context.Background()
	objs, _ := b.ListObjects(ctx, "")
	if len(objs) != 0 {
		t.Fatalf("premise: the listing must hide it, got %v", objs)
	}
	hidden, err := b.ListUnusable(ctx, "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}
	if len(hidden) != 1 {
		t.Fatalf("want the over-long key reported, got %v", hidden)
	}
	if !errors.Is(hidden[0].Err, ErrInvalidPath) {
		t.Errorf("Err = %v, want ErrInvalidPath", hidden[0].Err)
	}
}

// Directories are never reported. Reporting the walk root would give Path ""
// or ".", which names the data directory itself.
func TestUnusableNeverReportsADirectory(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	// A directory whose own name the contract refuses.
	if err := os.MkdirAll(filepath.Join(root, "db", "me\\as"), 0o700); err != nil {
		t.Skipf("no backslash names here: %v", err)
	}
	hidden, err := b.ListUnusable(context.Background(), "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}
	for _, o := range hidden {
		if o.Path == "" || o.Path == "." {
			t.Fatalf("reported a directory as an object: %+v", o)
		}
	}
	if len(hidden) != 0 {
		t.Errorf("an empty bad directory holds no objects, got %v", hidden)
	}
}

// A file UNDER a directory whose name the contract refuses is still real data
// and must be reported: the directory is not enumerable, so this is the only
// way it can ever be found.
func TestUnusableSeesFilesUnderABadDirectory(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	dir := filepath.Join(root, "db", "me\\as", "2026")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Skipf("no backslash names here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f.parquet"), []byte("ROWS"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	ctx := context.Background()
	if objs, _ := b.ListObjects(ctx, ""); len(objs) != 0 {
		t.Fatalf("premise: the listing must hide it, got %v", objs)
	}
	hidden, err := b.ListUnusable(ctx, "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}
	if len(hidden) != 1 || !strings.HasSuffix(hidden[0].Path, "f.parquet") {
		t.Fatalf("want the file under the bad directory reported, got %v", hidden)
	}
}

// The .part exclusion must key off the base KEY, not off whether something
// exists at that name. Testing existence with os.Stat let three real files fall
// into neither listing, which breaks the partition this design rests on.
func TestPartExclusionDoesNotSwallowRealObjects(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	ctx := context.Background()

	mk := func(rel, body string) {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatalf("mkdir %q: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
			t.Fatalf("write %q: %v", rel, err)
		}
	}

	// 1. Base name is occupied by a DIRECTORY, so no write to key "db/cpu"
	//    could ever stage here. An existence test would see the directory and
	//    call this an ordinary partial.
	mk("db/cpu/data.parquet", "PAR1")
	mk("db/cpu.part", "PAR1-real-rows")

	// 2. Base still carries the reserved suffix, so it is not a key either.
	mk("db/mem/legacy.part", "PAR1-legacy")
	mk("db/mem/legacy.part.part", "PAR1-legacy-partial")

	// 3. Base is a directory prefix, not a key at all.
	mk("db/disk/.part", "PAR1-dotpart")

	// And the shapes that MUST stay excluded.
	mk("db/net/good.parquet", "PAR1-good")
	mk("db/net/good.parquet.part", "ordinary staging partial")
	mk("db/net/inflight.parquet.part", "a WriteReader or compaction output in progress")

	objs, err := b.ListObjects(ctx, "")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	hidden, err := b.ListUnusable(ctx, "")
	if err != nil {
		t.Fatalf("ListUnusable: %v", err)
	}

	where := map[string]string{}
	for _, o := range objs {
		where[o.Path] = "listed"
	}
	for _, o := range hidden {
		if prev, dup := where[o.Path]; dup {
			t.Errorf("%q is both %s and unusable", o.Path, prev)
		}
		where[o.Path] = "unusable"
	}

	for path, want := range map[string]string{
		"db/cpu/data.parquet":          "listed",
		"db/net/good.parquet":          "listed",
		"db/cpu.part":                  "unusable",
		"db/mem/legacy.part":           "", // base key "legacy" is valid: indistinguishable from a partial
		"db/mem/legacy.part.part":      "unusable",
		"db/disk/.part":                "unusable",
		"db/net/good.parquet.part":     "", // ordinary staging
		"db/net/inflight.parquet.part": "", // in flight, no committed base yet
	} {
		if got := where[path]; got != want {
			t.Errorf("%q: got %q, want %q", path, orNone(got), orNone(want))
		}
	}
}
